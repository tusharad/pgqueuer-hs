{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module HPCScheduler (runApp) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad (forever, replicateM_, when)
import Data.Aeson
import Data.Either (isLeft)
import Data.Maybe (fromMaybe)
import Data.Time.Clock (secondsToNominalDiffTime)
import Data.UUID.V4 (nextRandom)
import GHC.Generics
import PGQueuer

data UserType = Student | PostDoc | Professor
    deriving (Show, Eq)

userPriority :: UserType -> Int
userPriority Student = 1
userPriority PostDoc = 2
userPriority Professor = 3

data CalculationPayload = CalculationPayload
    { userID :: Int
    , dept :: String
    , nums :: [Int]
    }
    deriving (Show, Eq, Generic, FromJSON, ToJSON)

submitJob :: QueueManager -> UserType -> CalculationPayload -> IO ()
submitJob qm uType payload = do
    let priority = userPriority uType
        payloadBytes = encode payload
        ep = Entrypoint "calc"

    -- Enqueue the job with the specific priority[cite: 1]
    jobIds <- enqueue qm ep (Just payloadBytes) priority Nothing Nothing Nothing
    putStrLn $ "Submitted " ++ show uType ++ " job (Priority " ++ show priority ++ ") -> Job IDs: " ++ show jobIds

workerLoop :: QueueManager -> IO ()
workerLoop qm = forever $ do
    let epParams = [EntrypointExecutionParameter (Entrypoint "calc") 0]

    jobs <- dequeue qm 5 epParams Nothing 300

    if null jobs
        then threadDelay 1000000 -- Sleep for 1 second if queue is empty
        else mapM_ (processJob qm) jobs

processJob :: QueueManager -> Job -> IO ()
processJob qm job = do
    let maxAttempts = 3

    eRes <- try (doCalculation job) :: IO (Either SomeException ())
    case eRes of
        Right _ -> do
            putStrLn $ "[SUCCESS] Job " ++ show (jobId job) ++ " completed."
            logJobs qm [(jobId job, Successful, Nothing)]
        Left err -> do
            if jobAttempts job < maxAttempts
                then do
                    putStrLn $ "[RETRY] Job " ++ show (jobId job) ++ " failed. Retrying in 2 seconds..."
                    retryJob qm job (secondsToNominalDiffTime 2) Nothing
                else do
                    putStrLn $ "[DLQ] Job " ++ show (jobId job) ++ " failed " ++ show maxAttempts ++ " times. Marking as FAILED."
                    let errorTraceback = Just (toJSON (show err))
                    logJobs qm [(jobId job, Failed, errorTraceback)]

-- Simulate the heavy calculation
doCalculation :: Job -> IO ()
doCalculation j = do
    let content = fromMaybe "{}" (jobPayload j)
    case decode content :: Maybe CalculationPayload of
        Nothing -> error "Failed to decode JSON payload"
        Just payload -> do
            threadDelay 500000 -- Simulate some computational time (0.5s)

            -- Deliberately cause a division-by-zero error if the list is empty
            -- (This simulates a bad payload/poison pill)
            let avg = sum (nums payload) `div` length (nums payload)
            putStrLn $ "Result calculated: " ++ show avg

runApp :: IO ()
runApp = do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    queueMgrId <- nextRandom

    withQueueManager conStr defaultDBSettings queueMgrId $ \qm -> do
        eInstalled <- verifyStructure qm
        when (isLeft eInstalled) (installSchema qm)
        -- Ensure the database tables exist
        replicateM_ 2 (forkIO $ workerLoop qm)
        submitJob qm Student (CalculationPayload 1 "CS" [10, 20, 30])
        submitJob qm Student (CalculationPayload 2 "CS" [])
        submitJob qm Professor (CalculationPayload 3 "Physics" [100, 200, 300])

        threadDelay 15000000
        putStrLn "--- System Shutdown ---"
