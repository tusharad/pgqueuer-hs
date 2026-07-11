{-# LANGUAGE OverloadedStrings #-}

module Tests.PGQueuer (runTests) where

import Test.Tasty
import Test.Tasty.HUnit

import Control.Concurrent (threadDelay)
import PGQueuer
import Data.Either (isRight, isLeft)
import Data.Maybe (isJust, listToMaybe)
import Data.UUID.V4 (nextRandom)
import qualified Data.Map.Strict as Map

{-

Each test happens within a transaction where a DB gets created and at the end gets killed.

1. Schema installation:
        installSchema installs schema [x]
        uninstallSchema uninstall schema [x]
        installing schema twice throws error TODO: later

2. QueueManager creates queue # Already is covered by other cases
3. Registering an entrypoint, registers and entrypoint. Registering again updates [x]
4. Enqueuing single and multiple works [x]
5. Dequeuing works. Removes/Updates entry in table
6. Log works
7. GetQueueSize and clearQueue works
8. List failed jobs, mark job as cancelled, requeue, retry jobs
9. Pass large json payload
-}

runTests :: IO ()
runTests = defaultMain $ dependentTestGroup "All tests" AllFinish [
            schemaTests,
            entrypointTests,
            enqueueDequeueJobs
        ]

schemaTests :: TestTree
schemaTests = do
    testGroup "Schema related tests"  [checkInstallUninstallSchema]

checkInstallUninstallSchema :: TestTree
checkInstallUninstallSchema = testCase "installed schema should should be deleted safely" $ do
        let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
        let dbSetting = defaultDBSettings 
        queueMgrId <- nextRandom
        withQueueManager conStr dbSetting queueMgrId $ \qm -> do
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
            assertBool "Should contain exactly one entry" $ length stats == 1

            _ <- dequeue qm1 20 params Nothing 3
            threadDelay 5000000
            stats1 <- getQueueSize qm1
            print ("stats1 " :: String , stats1)
            case listToMaybe stats1 of
              Nothing -> assertFailure "statistics are empty"
              Just stat -> assertBool "Should contain exactly zero entry" $ statsStatus stat == Picked

