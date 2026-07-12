{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module HPCScheduler (runApp) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (replicateM_, when)
import Data.Aeson
import Data.Either (isLeft)
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

runApp :: IO ()
runApp = do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
        epParams = [EntrypointExecutionParameter (Entrypoint "calc") 0]
    queueMgrId <- nextRandom

    withQueueManager conStr defaultDBSettings queueMgrId $ \qm -> do
        eInstalled <- verifyStructure qm
        when (isLeft eInstalled) (installSchema qm)
        -- Ensure the database tables exist
        replicateM_ 2 (forkIO $ workerLoop qm epParams)
        submitJob qm Student (CalculationPayload 1 "CS" [10, 20, 30])
        submitJob qm Student (CalculationPayload 2 "CS" [])
        submitJob qm Professor (CalculationPayload 3 "Physics" [100, 200, 300])

        threadDelay 15000000
        putStrLn "--- System Shutdown ---"
