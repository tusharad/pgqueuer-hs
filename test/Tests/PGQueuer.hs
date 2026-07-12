{-# LANGUAGE OverloadedStrings #-}

module Tests.PGQueuer (runTests) where

import Test.Tasty
import Test.Tasty.HUnit

import Data.Either (isLeft, isRight)
import qualified Data.Map.Strict as Map
import Data.Maybe (isJust, listToMaybe)
import Data.UUID.V4 (nextRandom)
import PGQueuer

{-
Each test happens within a transaction where a DB gets created and at the end gets killed.

1. Schema installation:
        installSchema installs schema [x]
        uninstallSchema uninstall schema [x]
        installing schema twice throws error TODO: later
2. QueueManager creates queue # Already is covered by other cases
3. Registering an entrypoint, registers and entrypoint. Registering again updates [x]
4. Enqueuing single and multiple works [x]
5. Dequeuing works. Updates entry in table [x]
6. Log works [x]
7. GetQueueSize and clearQueue works
8. List failed jobs, mark job as cancelled, requeue, retry jobs
9. Pass large json payload
10. Multiple entries are enqueued and dequeued seamlessly
11. MarkJobAsCancelled, UpdateHeartBeat
-}

runTests :: IO ()
runTests =
    defaultMain $
        dependentTestGroup
            "All tests"
            AllFinish
            [ schemaTests
            , entrypointTests
            , enqueueDequeueJobs
            , enqueueDequeueMultipleJobs
            , logAndListJobs
            ]

schemaTests :: TestTree
schemaTests = do
    testGroup "Schema related tests" [checkInstallUninstallSchema]

checkInstallUninstallSchema :: TestTree
checkInstallUninstallSchema = testCase "installed schema should should be deleted safely" $ do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    let dbSetting = defaultDBSettings
    queueMgrId <- nextRandom
    withQueueManager conStr dbSetting queueMgrId $ \qm -> do
        uninstallSchema qm -- Uninstall to make it idomatic
        _ <- installSchema qm
        schemaInstalled <- verifyStructure qm
        assertBool "Schema is installed and verified" (isRight schemaInstalled)
        uninstallSchema qm
        schemaInstalled2 <- verifyStructure qm
        assertBool "Schema is removed and verified" (isLeft schemaInstalled2)

entrypointTests :: TestTree
entrypointTests = testCase "Adding an entrypoint in qm adds entrypoint" $ do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    let dbSetting = defaultDBSettings
    queueMgrId <- nextRandom
    withQueueManager conStr dbSetting queueMgrId $ \qm -> do
        let ep = Entrypoint "hello"
        qm1 <- registerEntrypoint qm ep (\_ -> pure ())
        let entrypoints = qmEntrypoints qm1
        assertBool "qm should contain entrypoint hello" $ do
            isJust (Map.lookup "hello" entrypoints)

enqueueDequeueJobs :: TestTree
enqueueDequeueJobs =
    testCase "Enqueue jobs should have entry in PGQ table" $ do
        let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
        let dbSetting = defaultDBSettings
        queueMgrId <- nextRandom
        withQueueManager conStr dbSetting queueMgrId $ \qm -> do
            uninstallSchema qm
            _ <- installSchema qm
            let ep = Entrypoint "hello"
            let params = [EntrypointExecutionParameter ep 0]
            qm1 <- registerEntrypoint qm ep (\_ -> pure ())
            _ <- enqueue qm1 ep Nothing 0 Nothing Nothing Nothing
            stats <- getQueueSize qm1
            case listToMaybe stats of
                Nothing -> assertFailure "List is empty"
                Just stat -> assertBool "Should contain exactly one entry" $ statsCount stat == 1

            _ <- dequeue qm1 20 params Nothing 3
            stats1 <- getQueueSize qm1
            case listToMaybe stats1 of
                Nothing -> assertFailure "statistics are empty"
                Just stat -> assertBool "Should get picked up" $ statsStatus stat == Picked

enqueueDequeueMultipleJobs :: TestTree
enqueueDequeueMultipleJobs =
    testCase "Enqueue multiple jobs dequeue them" $ do
        let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
        let dbSetting = defaultDBSettings
        queueMgrId <- nextRandom
        withQueueManager conStr dbSetting queueMgrId $ \qm -> do
            uninstallSchema qm
            _ <- installSchema qm
            let ep = Entrypoint "hello"
            let params = [EntrypointExecutionParameter ep 0]
            qm1 <- registerEntrypoint qm ep (\_ -> pure ())
            _ <-
                enqueueMultiple
                    qm1
                    (replicate 10 ep)
                    (replicate 10 Nothing)
                    (replicate 10 0)
                    (replicate 10 Nothing)
                    (replicate 10 Nothing)
                    (replicate 10 Nothing)
            stats <- getQueueSize qm1
            case listToMaybe stats of
                Nothing -> assertFailure "statistics are empty"
                Just stat -> assertEqual "Should contain exactly 10 entries" (statsCount stat) 10

            _ <- dequeue qm1 20 params Nothing 3
            stats1 <- getQueueSize qm1
            case listToMaybe stats1 of
                Nothing -> assertFailure "statistics are empty"
                Just stat -> assertEqual "Should contain exactly zero entry" (statsStatus stat) Picked

logAndListJobs :: TestTree
logAndListJobs =
    testCase "Update job status to failed and cancelled" $ do
        let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
        let dbSetting = defaultDBSettings
        queueMgrId <- nextRandom
        withQueueManager conStr dbSetting queueMgrId $ \qm -> do
            uninstallSchema qm
            _ <- installSchema qm
            let ep = Entrypoint "hello"
            qm1 <- registerEntrypoint qm ep (\_ -> pure ())
            jobIds <-
                enqueueMultiple
                    qm1
                    (replicate 2 ep)
                    (replicate 2 Nothing)
                    (replicate 2 0)
                    (replicate 2 Nothing)
                    (replicate 2 Nothing)
                    (replicate 2 Nothing)
            case jobIds of
                (job1 : job2 : _) -> do
                    logJobs qm1 [(job1, Failed, Nothing)]
                    failedJobs <- listFailedJobs qm1 1
                    case listToMaybe failedJobs of
                        Nothing -> assertFailure "Need exactly one failed job"
                        Just failedJob ->
                            assertEqual
                                "should be update jobid"
                                job1
                                (jobId failedJob)
                    markJobAsCancelled qm1 [job2]
                    cancelledJobStats <- listJobStatusById qm1 [job2]
                    case listToMaybe cancelledJobStats of
                        Nothing -> assertFailure "Need exactly one cancelled job"
                        Just (jId, jStatus) -> do
                            assertEqual "Round trup for JobStatusById" jId job2
                            assertEqual "Status should be cancelled" jStatus Canceled
                _ -> assertFailure "There should be at least two jobs"
