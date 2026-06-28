{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (catch, SomeException)
import Data.ByteString.Char8 (pack)
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (connectPostgreSQL)
import System.Environment (getArgs)
import System.Exit (exitFailure)

import PGQueuer

-- ============================================================================
-- Producer: Enqueue jobs
-- ============================================================================

producerExample :: IO ()
producerExample = do
  -- Connect to PostgreSQL
  conn <- connectPostgreSQL ""
  
  -- Create settings and queue manager
  let settings = defaultDBSettings { dbPrefix = "" }
  queueMgrId <- nextRandom
  qm <- createQueueManager conn settings queueMgrId
  
  -- Enqueue 100 jobs
  jobIds <- enqueueMultiple qm
    (replicate 100 (Entrypoint "fetch"))
    [Just (pack $ "Message " ++ show n) | n <- [1..100]]
    (replicate 100 0)  -- default priority
    []  -- no execute_after
    []  -- no dedup keys
    []  -- no headers
  
  putStrLn $ "Enqueued " ++ show (length jobIds) ++ " jobs"

-- ============================================================================
-- Consumer: Process jobs
-- ============================================================================

consumerExample :: IO ()
consumerExample = do
  -- Connect to PostgreSQL
  conn <- connectPostgreSQL ""
  
  -- Create settings and queue manager
  let settings = defaultDBSettings { dbPrefix = "" }
  queueMgrId <- nextRandom
  qm <- createQueueManager conn settings queueMgrId
  
  -- Process jobs in a loop
  let processLoop = do
        jobs <- dequeue qm
          100  -- batch size
          [EntrypointExecutionParameter (Entrypoint "fetch") 0]  -- unlimited concurrency
          Nothing  -- no global limit
          300  -- 5 minute heartbeat timeout
        
        case jobs of
          [] -> do
            putStrLn "No jobs available, sleeping..."
            threadDelay (1000000)  -- 1 second
            processLoop
          _ -> do
            putStrLn $ "Dequeued " ++ show (length jobs) ++ " jobs"
            -- Process jobs
            mapM_ handleJob jobs
            processLoop
  
  processLoop

-- | Handle a single job with error handling
handleJob :: Job -> IO ()
handleJob job = do
  putStrLn $ "Processing: " ++ show (jobId job)
  -- Simulate work
  threadDelay (100000)  -- 0.1 second
  -- Log completion
  putStrLn $ "Completed: " ++ show (jobId job)

-- ============================================================================
-- Setup: Install schema
-- ============================================================================

setupExample :: IO ()
setupExample = do
  -- Connect to PostgreSQL
  conn <- connectPostgreSQL ""
  
  let settings = defaultDBSettings
  queueMgrId <- nextRandom
  qm <- createQueueManager conn settings queueMgrId
  
  -- Install schema
  installSchema qm
  putStrLn "Schema installed successfully"
  
  -- Verify structure
  result <- verifyStructure qm
  case result of
    Left err -> putStrLn $ "Error: " ++ err
    Right _ -> putStrLn "Schema verification successful"

-- ============================================================================
-- Combined example with producer and consumer in separate threads
-- ============================================================================

combinedExample :: IO ()
combinedExample = do
  -- Connect to PostgreSQL
  conn <- connectPostgreSQL ""
  let settings = defaultDBSettings
  
  -- Install schema
  queueMgrId1 <- nextRandom
  qm1 <- createQueueManager conn settings queueMgrId1
  putStrLn "Installing schema..."
  installSchema qm1
  putStrLn "Schema installed"
  
  -- Producer thread
  queueMgrId2 <- nextRandom
  producerThreadId <- forkIO $ do
    conn' <- connectPostgreSQL ""
    qm <- createQueueManager conn' settings queueMgrId2
    threadDelay (2000000)  -- Wait 2 seconds
    jobIds <- enqueueMultiple qm
      (replicate 10 (Entrypoint "fetch"))
      [Just (pack $ "Job " ++ show n) | n <- [1..10]]
      (replicate 10 0)
      []
      []
      []
    putStrLn $ "Producer: Enqueued " ++ show (length jobIds) ++ " jobs"
  
  -- Consumer thread
  queueMgrId3 <- nextRandom
  consumerThreadId <- forkIO $ do
    conn' <- connectPostgreSQL ""
    qm <- createQueueManager conn' settings queueMgrId3
    processedCount <- processJobsWithTimeout 5 qm
    putStrLn $ "Consumer: Processed " ++ show processedCount ++ " jobs"
  
  -- Wait for threads to complete
  threadDelay (15000000)  -- 15 seconds
  putStrLn "Combined example completed"

-- | Process jobs with a timeout (in seconds)
processJobsWithTimeout :: Int -> QueueManager -> IO Int
processJobsWithTimeout timeoutSecs qm = do
  let maxTime = timeoutSecs * 1000000  -- Convert to microseconds
  go 0 0
  where
    go count elapsedTime
      | elapsedTime > maxTime = return count
      | otherwise = do
        jobs <- dequeue qm
          10  -- batch size
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

-- ============================================================================
-- Main
-- ============================================================================

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["setup"] -> setupExample
    ["producer"] -> producerExample
    ["consumer"] -> consumerExample
    ["combined"] -> combinedExample
    _ -> do
      putStrLn "Usage: pgqueuer-hs [setup|producer|consumer|combined]"
      exitFailure
