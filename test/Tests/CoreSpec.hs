{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Tests.CoreSpec (coreTests) where

import Control.Concurrent.Async (async, wait)
import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (liftIO)
import Data.List (nub)
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import qualified Database.PostgreSQL.Simple as PG
import PGQueuer.Backend.Simple
import PGQueuer.Core.Monad
import PGQueuer.Schema (install, uninstall)
import PGQueuer.Settings (defaultDBSettings)
import PGQueuer.Types
import Test.Tasty
import Test.Tasty.HUnit

-- | Run an action with a fresh schema and SimpleDb environment
withFreshSimpleDb :: (SimpleDbEnv -> IO a) -> IO a
withFreshSimpleDb action = do
    conn <- connectPostgreSQL "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    let settings = defaultDBSettings
    -- Clean slate
    uninstall conn settings
    install conn settings
    let env = SimpleDbEnv conn settings
    result <- action env
    close conn
    return result

-- | Core integration tests for the MonadPGQueuer abstraction via SimpleDb
coreTests :: TestTree
coreTests =
    testGroup
        "Phase 1 Foundation - SimpleDb Integration"
        [ testEnqueueCapturesJobId
        , testConcurrentDequeueSkipLocked
        , testLogJobsTransitionsStatus
        , testWithTransactionRollback
        ]

{- | Test: Call enqueue with an entrypoint and payload
Expectation: Return value contains a valid JobId; row exists in 'pgqueuer' table
-}
testEnqueueCapturesJobId :: TestTree
testEnqueueCapturesJobId =
    testCase "performs basic job enqueuing and captures a distinct JobId" $ do
        withFreshSimpleDb $ \env -> do
            jobIds <- runSimpleDb env $ do
                enqueue (Entrypoint "test_basic") Nothing 0 Nothing Nothing Nothing
            assertEqual "enqueue should return exactly one JobId" 1 (length jobIds)
            case jobIds of
                [JobId jid] -> assertBool "JobId should be positive" (jid > 0)
                _ -> assertFailure "Expected exactly one JobId"

            -- Verify the row exists by dequeuing it
            queueMgrId <- nextRandom
            let params = [EntrypointExecutionParameter (Entrypoint "test_basic") 0]
            jobs <- runSimpleDb env $ do
                dequeue 10 params queueMgrId Nothing 300
            case (jobIds, jobs) of
                ([expectedId], [job]) -> do
                    assertEqual "Dequeued job should have the same id" expectedId (jobId job)
                    assertEqual "Dequeued job status should be Picked" Picked (jobStatus job)
                _ -> assertFailure "Expected exactly one job dequeued"

{- | Test: Enqueue 3 matching jobs. Simulate two concurrent workers dequeuing with batch size 1.
Expectation: Worker 1 and Worker 2 must receive completely distinct jobs.
No thread should block or wait on the other worker's lock.
-}
testConcurrentDequeueSkipLocked :: TestTree
testConcurrentDequeueSkipLocked =
    testCase "executes non-contending dequeue using FOR UPDATE SKIP LOCKED" $ do
        withFreshSimpleDb $ \env -> do
            -- Enqueue 3 jobs
            _ <- runSimpleDb env $ do
                _ <- enqueue (Entrypoint "concurrent_test") Nothing 1 Nothing Nothing Nothing
                _ <- enqueue (Entrypoint "concurrent_test") Nothing 1 Nothing Nothing Nothing
                enqueue (Entrypoint "concurrent_test") Nothing 1 Nothing Nothing Nothing

            let params = [EntrypointExecutionParameter (Entrypoint "concurrent_test") 0]

            -- Two concurrent workers each dequeue batch=1 using separate connections
            conn1 <- connectPostgreSQL "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
            conn2 <- connectPostgreSQL "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
            let env1 = SimpleDbEnv conn1 (sdbSettings env)
            let env2 = SimpleDbEnv conn2 (sdbSettings env)

            qmId1 <- nextRandom
            qmId2 <- nextRandom

            a1 <- async $ runSimpleDb env1 (dequeue 1 params qmId1 Nothing 300)
            a2 <- async $ runSimpleDb env2 (dequeue 1 params qmId2 Nothing 300)

            jobs1 <- wait a1
            jobs2 <- wait a2

            close conn1
            close conn2

            assertEqual "Worker 1 should get exactly 1 job" 1 (length jobs1)
            assertEqual "Worker 2 should get exactly 1 job" 1 (length jobs2)

            let allIds = map jobId jobs1 ++ map jobId jobs2
            assertEqual
                "Workers must receive completely distinct jobs"
                (length allIds)
                (length (nub allIds))

{- | Test: Execute logJobs to mark a job as Failed.
Expectation: Active job row status updates to 'failed' and an append-only log
record is created in the 'pgqueuer_log' table.
-}
testLogJobsTransitionsStatus :: TestTree
testLogJobsTransitionsStatus =
    testCase "atomically transitions job status and records to the transaction log" $ do
        withFreshSimpleDb $ \env -> do
            -- Enqueue and pick a job
            queueMgrId <- nextRandom
            let ep = Entrypoint "log_test"
            let params = [EntrypointExecutionParameter ep 0]

            _ <- runSimpleDb env $ enqueue ep Nothing 0 Nothing Nothing Nothing
            jobs <- runSimpleDb env $ dequeue 1 params queueMgrId Nothing 300
            assertEqual "Should dequeue 1 job" 1 (length jobs)

            case jobs of
                [theJob] -> do
                    -- Mark as Failed via logJobs
                    runSimpleDb env $ logJobs [(jobId theJob, Failed, Nothing)]

                    -- Verify: job status should be 'failed' (logJobs keeps failed jobs via UPDATE)
                    let conn = sdbConnection env
                    statusRows <-
                        PG.query
                            conn
                            "SELECT status::text FROM pgqueuer WHERE id = ?"
                            (PG.Only (jobId theJob)) ::
                            IO [PG.Only String]
                    case statusRows of
                        [PG.Only statusText] ->
                            assertEqual "Status should be 'failed'" "failed" statusText
                        _ -> assertFailure "Expected exactly one status row"

                    -- Verify: log entry exists
                    logRows <-
                        PG.query
                            conn
                            "SELECT status::text FROM pgqueuer_log WHERE job_id = ? AND status = 'failed'"
                            (PG.Only (jobId theJob)) ::
                            IO [PG.Only String]
                    assertBool "Should have at least one 'failed' log entry" (not (null logRows))
                _ -> assertFailure "Expected exactly one dequeued job"

{- | Test: Open a withTransaction boundary. Enqueue a job, then intentionally
throw an exception.
Expectation: The outer boundary catches the exception. A subsequent check
verifies that the job was never inserted into the database.
-}
testWithTransactionRollback :: TestTree
testWithTransactionRollback =
    testCase "strictly rolls back state modifications when a transaction block fails" $ do
        withFreshSimpleDb $ \env -> do
            let conn = sdbConnection env

            -- Count rows before
            [PG.Only (beforeCount :: Int)] <-
                PG.query_ conn "SELECT COUNT(*) FROM pgqueuer"

            -- Attempt a transaction that will fail
            result <- try $ runSimpleDb env $ do
                withTransaction $ do
                    _ <- enqueue (Entrypoint "rollback_test") Nothing 0 Nothing Nothing Nothing
                    -- Force an exception inside the transaction
                    liftIO $ ioError (userError "intentional failure to trigger rollback")

            -- Verify the exception was caught
            case result of
                Left (_ :: SomeException) -> return ()
                Right _ -> assertFailure "Expected an exception but transaction succeeded"

            -- Verify: no new rows were inserted (transaction was rolled back)
            [PG.Only (afterCount :: Int)] <-
                PG.query_ conn "SELECT COUNT(*) FROM pgqueuer"
            assertEqual
                "Row count should be unchanged after rollback"
                beforeCount
                afterCount
