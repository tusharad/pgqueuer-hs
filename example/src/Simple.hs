{-# LANGUAGE OverloadedStrings #-}

module Simple (runApp) where

import Control.Concurrent (forkIO, threadDelay)
import Data.ByteString.Lazy.Char8 (pack)
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (ConnectInfo (..), connect, defaultConnectInfo)

import PGQueuer

-- | Handle a single job with error handling
handleJob :: Job -> IO ()
handleJob job = do
    putStrLn $ "Processing: " ++ show (jobId job)
    -- Simulate work
    threadDelay 100000 -- 0.1 second
    -- Log completion
    putStrLn $ "Completed: " ++ show (jobId job)

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
    installSchema qm1
    putStrLn "Schema installed"

    -- Producer thread
    queueMgrId2 <- nextRandom
    _producerThreadId <- forkIO $ do
        conn' <-
            connect
                defaultConnectInfo
                    { connectHost = "localhost"
                    , connectPort = 5432
                    , connectUser = "queue_user"
                    , connectPassword = "queue_pass"
                    , connectDatabase = "queue_db"
                    }
        qm <- createQueueManager conn' settings queueMgrId2
        threadDelay 2000000 -- Wait 2 seconds
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

    -- Consumer thread
    queueMgrId3 <- nextRandom
    _consumerThreadId <- forkIO $ do
        conn' <-
            connect
                defaultConnectInfo
                    { connectHost = "localhost"
                    , connectPort = 5432
                    , connectUser = "queue_user"
                    , connectPassword = "queue_pass"
                    , connectDatabase = "queue_db"
                    }
        qm <- createQueueManager conn' settings queueMgrId3
        processedCount <- processJobsWithTimeout 5 qm
        putStrLn $ "Consumer: Processed " ++ show processedCount ++ " jobs"

    -- Wait for threads to complete
    threadDelay 15000000 -- 15 seconds
    putStrLn "Combined example completed"

-- | Process jobs with a timeout (in seconds)
processJobsWithTimeout :: Int -> QueueManager -> IO Int
processJobsWithTimeout timeoutSecs qm = do
    go 0 0
  where
    maxTime = timeoutSecs * 1000000 -- Convert to microseconds
    go count elapsedTime
        | elapsedTime > maxTime = return count
        | otherwise = do
            jobs <-
                dequeue
                    qm
                    10 -- batch size
                    [EntrypointExecutionParameter (Entrypoint "fetch") 0]
                    Nothing
                    300

            if null jobs
                then do
                    threadDelay 100000
                    go count (elapsedTime + 100000)
                else do
                    mapM_ handleJob jobs
                    let newCount = count + length jobs
                    threadDelay 100000
                    go newCount (elapsedTime + 100000)
