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

withFreshQueue :: (QueueManager -> IO a) -> IO a
withFreshQueue action = do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    let dbSetting = defaultDBSettings
    queueMgrId <- nextRandom
    withQueueManager conStr dbSetting queueMgrId $ \qm -> do
        uninstallSchema qm
        installSchema qm
        action qm

expectOne :: String -> [a] -> (a -> Assertion) -> Assertion
expectOne message values assertion =
    case listToMaybe values of
        Nothing -> assertFailure message
        Just value -> assertion value

schemaTests :: TestTree
schemaTests = do
    testGroup "Schema related tests" [checkInstallUninstallSchema]

checkInstallUninstallSchema :: TestTree
checkInstallUninstallSchema = testCase "installed schema should should be deleted safely" $ do
    withFreshQueue $ \qm -> do
        schemaInstalled <- verifyStructure qm
        assertBool "Schema is installed and verified" (isRight schemaInstalled)
        uninstallSchema qm
        schemaInstalled2 <- verifyStructure qm
        assertBool "Schema is removed and verified" (isLeft schemaInstalled2)

entrypointTests :: TestTree
entrypointTests = testCase "Adding an entrypoint in qm adds entrypoint" $ do
    withFreshQueue $ \qm -> do
        let ep = Entrypoint "hello"
        qm1 <- registerEntrypoint qm ep (\_ -> pure ())
        let entrypoints = qmEntrypoints qm1
        assertBool "qm should contain entrypoint hello" $ isJust (Map.lookup "hello" entrypoints)

enqueueDequeueJobs :: TestTree
enqueueDequeueJobs =
    testCase "Enqueue jobs should have entry in PGQ table" $ do
        withFreshQueue $ \qm -> do
            let ep = Entrypoint "hello"
            let params = [EntrypointExecutionParameter ep 0]
            qm1 <- registerEntrypoint qm ep (\_ -> pure ())
            _ <- enqueue qm1 ep Nothing 0 Nothing Nothing Nothing
            stats <- getQueueSize qm1
            expectOne "List is empty" stats $ \stat ->
                assertBool "Should contain exactly one entry" $ statsCount stat == 1

            pickedJobs <- dequeue qm1 20 params Nothing 3
            expectOne "dequeue returned no jobs" pickedJobs $ \job -> do
                assertEqual "dequeued job should be picked" Picked (jobStatus job)
                status <- listJobStatusById qm1 [jobId job]
                expectOne "job status lookup failed" status $ \(_, jobStatus') ->
                    assertEqual "job should be picked" Picked jobStatus'

enqueueDequeueMultipleJobs :: TestTree
enqueueDequeueMultipleJobs =
    testCase "Enqueue multiple jobs dequeue them" $ do
        withFreshQueue $ \qm -> do
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
            expectOne "statistics are empty" stats $ \stat ->
                assertEqual "Should contain exactly 10 entries" 10 (statsCount stat)

            pickedJobs <- dequeue qm1 20 params Nothing 3
            assertEqual "Should dequeue 10 jobs" 10 (length pickedJobs)
            assertBool "All jobs should be picked" (all ((== Picked) . jobStatus) pickedJobs)
            stats1 <- getQueueSize qm1
            expectOne "statistics are empty" stats1 $ \stat ->
                assertEqual "picked jobs should be tracked" Picked (statsStatus stat)

logAndListJobs :: TestTree
logAndListJobs =
    testCase "Update job status, retry, requeue and clear queue" $ do
        withFreshQueue $ \qm -> do
            let ep = Entrypoint "hello"
            let params = [EntrypointExecutionParameter ep 0]
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
            assertEqual "enqueueMultiple should return two job ids" 2 (length jobIds)
            pickedJobs <- dequeue qm1 2 params Nothing 3
            case pickedJobs of
                [job1, job2] -> do
                    updateHeartbeat qm1 [jobId job1]
                    heartbeatStatus <- listJobStatusById qm1 [jobId job1]
                    expectOne "heartbeat status lookup failed" heartbeatStatus $ \(_, status) ->
                        assertEqual "picked job should stay picked" Picked status

                    logJobs qm1 [(jobId job1, Failed, Nothing)]
                    failedJobs <- listFailedJobs qm1 10
                    expectOne "Need exactly one failed job" failedJobs $ \failedJob -> do
                        assertEqual "failed job id mismatch" (jobId job1) (jobId failedJob)
                        retryJob qm1 failedJob 0 Nothing

                    retryStatus <- listJobStatusById qm1 [jobId job1]
                    expectOne "retry status lookup failed" retryStatus $ \(_, status) ->
                        assertEqual "retried job should be queued" Queued status

                    requeueJobs qm1 [jobId job2]
                    requeueStatus <- listJobStatusById qm1 [jobId job2]
                    expectOne "requeue status lookup failed" requeueStatus $ \(_, status) ->
                        assertEqual "requeued job should be queued" Queued status

                    markJobAsCancelled qm1 [jobId job2]
                    cancelledStatus <- listJobStatusById qm1 [jobId job2]
                    expectOne "cancelled status lookup failed" cancelledStatus $ \(_, status) ->
                        assertEqual "cancelled job should be cancelled" Canceled status

                    clearQueue qm1 (Just [ep])
                    remaining <- getQueueSize qm1
                    assertBool "queue should be empty after clearQueue" (null remaining)
                _ -> assertFailure "There should be exactly two picked jobs"
