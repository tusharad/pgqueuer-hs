{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module HPCScheduler (runApp) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (replicateM_, when)
import Data.Aeson
import Data.Either (isLeft)
import Data.Maybe (fromMaybe)
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

    jobIds <- enqueue qm ep (Just payloadBytes) priority Nothing Nothing Nothing
    putStrLn $ "Submitted " ++ show uType ++ " job (Priority " ++ show priority ++ ") -> Job IDs: " ++ show jobIds

submitMapReduce :: QueueManager -> IO ()
submitMapReduce qm = do
    let parent = JobNode (Entrypoint "reduce") Nothing 1 Nothing Nothing Nothing Nothing
        child1 = JobNode (Entrypoint "map") (Just "10") 1 Nothing Nothing Nothing Nothing
        child2 = JobNode (Entrypoint "map") (Just "20") 1 Nothing Nothing Nothing Nothing
        tree = parent <~~ [child1 <~~ [], child2 <~~ []]
    insertJobTree qm tree
    putStrLn "Submitted MapReduce workflow."

runApp :: IO ()
runApp = do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
        epParams =
            [ EntrypointExecutionParameter (Entrypoint "calc") 0 5 (Exponential 5 60) FullJitter
            , EntrypointExecutionParameter (Entrypoint "map") 0 5 (Exponential 5 60) FullJitter
            , EntrypointExecutionParameter (Entrypoint "reduce") 0 5 (Exponential 5 60) FullJitter
            ]
    queueMgrId <- nextRandom

    withQueueManager conStr defaultDBSettings queueMgrId $ \qm -> do
        eInstalled <- verifyStructure qm
        when (isLeft eInstalled) (installSchema qm)
        -- Ensure the database tables exist
        -- Register a cron schedule for a routine job
        registerSchedule qm (CronExpression "*/5 * * * *") (Entrypoint "calc")

        replicateM_ 2 (forkIO $ workerLoop qm defaultBufferConfig epParams)

        -- Register Map/Reduce handlers
        qm' <- registerEntrypoint qm (Entrypoint "map") $ \job -> do
            let payloadStr = fromMaybe "" (jobPayload job)
            putStrLn $ "Map job running: " ++ show payloadStr

        qm'' <- registerEntrypoint qm' (Entrypoint "reduce") $ \_job -> do
            putStrLn "Reduce job running! Awaiting merged results..."
            -- To fetch children results, we can use mergedChildResults but we need to do it inside PGQueuer monad,
            -- or just simulate it here since we don't have direct access in simple handler.
            putStrLn "Reduce job complete!"

        submitJob qm'' Student (CalculationPayload 1 "CS" [10, 20, 30])
        submitJob qm'' Professor (CalculationPayload 3 "Physics" [100, 200, 300])
        submitMapReduce qm''

        threadDelay 15000000
        putStrLn "--- System Shutdown ---"
