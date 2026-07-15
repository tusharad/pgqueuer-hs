{-# LANGUAGE OverloadedStrings #-}

module Python (runApp) where

import Control.Monad (when)
import Data.ByteString.Lazy.Char8 (pack)
import Data.Either (isLeft)
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (ConnectInfo (..), connect, defaultConnectInfo)

import PGQueuer

runApp :: IO ()
runApp = do
    -- Connect to PostgreSQL
    conn <-
        connect
            defaultConnectInfo
                { connectHost = "localhost"
                , connectPort = 5432
                , connectUser = "queue_user"
                , connectPassword = "queue_pass"
                , connectDatabase = "queue_db"
                }
    let settings = defaultDBSettings

    -- Install schema
    queueMgrId1 <- nextRandom
    qm1 <- createQueueManager conn settings queueMgrId1
    putStrLn "Installing schema..."
    eInstalled <- verifyStructure qm1
    when (isLeft eInstalled) (installSchema qm1)
    putStrLn "Schema installed"
    producer qm1

{-
-- Consumer thread
queueMgrId3 <- nextRandom
_consumerThreadId <- forkIO $ do
    qm <- createQueueManager conn settings queueMgrId3
    processedCount <- processJobsWithTimeout 5 qm
    putStrLn $ "Consumer: Processed " ++ show processedCount ++ " jobs"

-- Wait for threads to complete
threadDelay 15000000 -- 15 seconds
putStrLn "Combined example completed"
-}

producer :: QueueManager -> IO ()
producer qm = do
    jobIds <-
        enqueueMultiple
            qm
            (replicate 10 (Entrypoint "fetch"))
            [Just (pack $ "Job " ++ show n) | n <- [1 .. 10] :: [Int]]
            (replicate 10 0)
            []
            []
            []
    putStrLn $ "Producer: Enqueued " ++ show (length jobIds) ++ " jobs"
