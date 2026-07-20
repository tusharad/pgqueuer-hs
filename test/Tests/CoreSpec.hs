{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Tests.CoreSpec (
    coreTests,
    Runner (..),
    connStr,
)
where

import Control.Concurrent.Async (async, wait)
import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (Value (String))
import Data.ByteString (ByteString)
import Data.List (nub)
import Data.Maybe (listToMaybe)
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (Connection)
import qualified Database.PostgreSQL.Simple as PG
import PGQueuer.Core.Monad
import PGQueuer.Schema (install, uninstall)
import PGQueuer.Settings (DBSettings, defaultDBSettings)
import PGQueuer.Types
import PGQueuer.Workflow (JobNode (..), insertJobTree, (<~~))
import Test.Tasty
import Test.Tasty.HUnit

-- | Connection string used for testing.
connStr :: ByteString
connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"

{- | A wrapper for a polymorphic backend runner.

Wrapping in a newtype avoids the need for ImpredicativeTypes when
passing runners inside tuples or data structures.
-}
newtype Runner = Runner
    { runWith :: forall a. (forall m. (MonadPGQueuer m, MonadIO m) => m a) -> IO a
    }

{- | Core integration tests parameterized over any 'MonadPGQueuer' backend.

Accepts:
 1. A label string (e.g., "SimpleDb", "HasqlDb")
 2. A function that creates a Runner + cleanup from a fresh schema connection
 3. A function that creates two independent Runners for concurrent tests
-}
coreTests ::
    -- | Backend label
    String ->
    -- | (conn -> settings -> IO (runner, cleanup))
    (Connection -> DBSettings -> IO (Runner, IO ())) ->
    -- | (conn -> settings -> IO (runner1, runner2, cleanup))
    (Connection -> DBSettings -> IO (Runner, Runner, IO ())) ->
    TestTree
coreTests label mkRunner mkDualRunners =
    testGroup
        (label <> " - Core Integration")
        [ testEnqueueCapturesJobId mkRunner
        , testConcurrentDequeueSkipLocked mkRunner mkDualRunners
        , testLogJobsTransitionsStatus mkRunner
        , testWithTransactionRollback mkRunner
        , testJobTreeRollup mkRunner
        ]

-- | Set up a fresh schema and run an action.
withFreshSchema :: (Connection -> DBSettings -> IO a) -> IO a
withFreshSchema action = do
    conn <- PG.connectPostgreSQL connStr
    let settings = defaultDBSettings
    uninstall conn settings
    install conn settings
    result <- action conn settings
    PG.close conn
    return result

{- | Test: Call enqueue with an entrypoint and payload
Expectation: Return value contains a valid JobId; row exists in 'pgqueuer' table
-}
testEnqueueCapturesJobId ::
    (Connection -> DBSettings -> IO (Runner, IO ())) ->
    TestTree
testEnqueueCapturesJobId mkRunner =
    testCase "performs basic job enqueuing and captures a distinct JobId" $ do
        withFreshSchema $ \conn settings -> do
            (runner, cleanup) <- mkRunner conn settings

            jobIds <-
                runWith runner $
                    enqueue (Entrypoint "test_basic") Nothing 0 Nothing Nothing Nothing Nothing Nothing Queued

            assertEqual "enqueue should return exactly one JobId" 1 (length jobIds)
            case jobIds of
                [JobId jid] -> assertBool "JobId should be positive" (jid > 0)
                _ -> assertFailure "Expected exactly one JobId"

            -- Verify the row exists by dequeuing it
            queueMgrId <- nextRandom
            let params = [EntrypointExecutionParameter (Entrypoint "test_basic") 0 5 (Exponential 5 60) FullJitter]
            jobs <-
                runWith runner $
                    dequeue 10 params queueMgrId Nothing 300

            case (jobIds, jobs) of
                ([expectedId], [job]) -> do
                    assertEqual "Dequeued job should have the same id" expectedId (jobId job)
                    assertEqual "Dequeued job status should be Picked" Picked (jobStatus job)
                _ -> assertFailure "Expected exactly one job dequeued"

            cleanup

{- | Test: Enqueue 3 matching jobs. Simulate two concurrent workers dequeuing with batch size 1.
Expectation: Worker 1 and Worker 2 must receive completely distinct jobs.
No thread should block or wait on the other worker's lock.
-}
testConcurrentDequeueSkipLocked ::
    (Connection -> DBSettings -> IO (Runner, IO ())) ->
    (Connection -> DBSettings -> IO (Runner, Runner, IO ())) ->
    TestTree
testConcurrentDequeueSkipLocked mkRunner mkDualRunners =
    testCase "executes non-contending dequeue using FOR UPDATE SKIP LOCKED" $ do
        withFreshSchema $ \conn settings -> do
            -- Use a single runner for enqueueing
            (enqRunner, enqCleanup) <- mkRunner conn settings

            -- Enqueue 3 jobs
            _ <- runWith enqRunner $ do
                _ <- enqueue (Entrypoint "concurrent_test") Nothing 1 Nothing Nothing Nothing Nothing Nothing Queued
                _ <- enqueue (Entrypoint "concurrent_test") Nothing 1 Nothing Nothing Nothing Nothing Nothing Queued
                enqueue (Entrypoint "concurrent_test") Nothing 1 Nothing Nothing Nothing Nothing Nothing Queued

            enqCleanup

            -- Create two independent runners for concurrent dequeue
            (runner1, runner2, dualCleanup) <- mkDualRunners conn settings

            let params = [EntrypointExecutionParameter (Entrypoint "concurrent_test") 0 5 (Exponential 5 60) FullJitter]

            qmId1 <- nextRandom
            qmId2 <- nextRandom

            a1 <- async $ runWith runner1 (dequeue 1 params qmId1 Nothing 300)
            a2 <- async $ runWith runner2 (dequeue 1 params qmId2 Nothing 300)

            jobs1 <- wait a1
            jobs2 <- wait a2

            dualCleanup

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
testLogJobsTransitionsStatus ::
    (Connection -> DBSettings -> IO (Runner, IO ())) ->
    TestTree
testLogJobsTransitionsStatus mkRunner =
    testCase "atomically transitions job status and records to the transaction log" $ do
        withFreshSchema $ \conn settings -> do
            (runner, cleanup) <- mkRunner conn settings

            -- Enqueue and pick a job
            queueMgrId <- nextRandom
            let ep = Entrypoint "log_test"
            let params = [EntrypointExecutionParameter ep 0 5 (Exponential 5 60) FullJitter]

            _ <- runWith runner $ enqueue ep Nothing 0 Nothing Nothing Nothing Nothing Nothing Queued
            jobs <- runWith runner $ dequeue 1 params queueMgrId Nothing 300
            assertEqual "Should dequeue 1 job" 1 (length jobs)

            case jobs of
                [theJob] -> do
                    -- Mark as Failed via logJobs
                    runWith runner $ logJobs [(jobId theJob, Failed, Nothing)]

                    -- Verify: job status should be 'failed' (via postgresql-simple for verification)
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

            cleanup

{- | Test: Open a withTransaction boundary. Enqueue a job, then intentionally
throw an exception.
Expectation: The outer boundary catches the exception. A subsequent check
verifies that the job was never inserted into the database.
-}
testWithTransactionRollback ::
    (Connection -> DBSettings -> IO (Runner, IO ())) ->
    TestTree
testWithTransactionRollback mkRunner =
    testCase "strictly rolls back state modifications when a transaction block fails" $ do
        withFreshSchema $ \conn settings -> do
            (runner, cleanup) <- mkRunner conn settings

            -- Count rows before
            [PG.Only (beforeCount :: Int)] <-
                PG.query_ conn "SELECT COUNT(*) FROM pgqueuer"

            -- Attempt a transaction that will fail
            result <- try $ runWith runner $ do
                withTransaction $ do
                    _ <- enqueue (Entrypoint "rollback_test") Nothing 0 Nothing Nothing Nothing Nothing Nothing Queued
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

            cleanup

{- | Test: Insert a JobTree and verify that parent is only dequeued after children complete.
Expectation: Children are dequeued first, parent remains 'held'.
Upon completion of all children, parent is dequeued.
-}
testJobTreeRollup ::
    (Connection -> DBSettings -> IO (Runner, IO ())) ->
    TestTree
testJobTreeRollup mkRunner =
    testCase "atomically rolls up child results and unholds parent" $ do
        withFreshSchema $ \conn settings -> do
            (runner, cleanup) <- mkRunner conn settings

            let parentNode = JobNode (Entrypoint "parent") Nothing 0 Nothing Nothing Nothing Nothing
                childNode1 = JobNode (Entrypoint "child") (Just "c1") 0 Nothing Nothing Nothing Nothing
                childNode2 = JobNode (Entrypoint "child") (Just "c2") 0 Nothing Nothing Nothing Nothing
                tree = parentNode <~~ [childNode1 <~~ [], childNode2 <~~ []]

            runWith runner $ insertJobTree tree

            queueMgrId <- nextRandom
            let params =
                    [ EntrypointExecutionParameter (Entrypoint "child") 0 5 (Exponential 5 60) FullJitter
                    , EntrypointExecutionParameter (Entrypoint "parent") 0 5 (Exponential 5 60) FullJitter
                    ]

            -- First dequeue: should only get children
            jobs1 <- runWith runner $ dequeue 10 params queueMgrId Nothing 300
            assertEqual "Should dequeue 2 children" 2 (length jobs1)
            assertBool "All dequeued jobs should be 'child'" (all (\j -> jobEntrypoint j == Entrypoint "child") jobs1)

            -- Mark first child as success
            case listToMaybe jobs1 of
                Nothing -> pure ()
                Just c1 -> runWith runner $ logJobs [(jobId c1, Successful, Just (String "result1"))]

            -- Second dequeue: should get 0 jobs (parent is still held)
            jobs2 <- runWith runner $ dequeue 10 params queueMgrId Nothing 300
            assertEqual "Should dequeue 0 jobs because 1 child is still pending" 0 (length jobs2)

            -- Mark second child as success
            case drop 1 jobs1 of
                (c2 : _) -> runWith runner $ logJobs [(jobId c2, Successful, Just (String "result2"))]
                [] -> pure ()

            -- Third dequeue: should now get the parent job
            jobs3 <- runWith runner $ dequeue 10 params queueMgrId Nothing 300
            assertEqual "Should dequeue 1 parent job" 1 (length jobs3)
            case listToMaybe jobs3 of
                Nothing -> pure ()
                Just parentJob -> do
                    assertEqual "Dequeued job should be 'parent'" (Entrypoint "parent") (jobEntrypoint parentJob)

                    -- Fetch merged child results
                    results <- runWith runner $ mergedChildResults (jobId parentJob)
                    assertEqual "Should have 2 child results" 2 (length results)
                    let resStrs = [s | String s <- results]
                    assertBool "Results should contain both child outputs" ("result1" `elem` resStrs && "result2" `elem` resStrs)

            cleanup
