{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (throwIO)
import Control.Monad (forever)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.UUID.V4 (nextRandom)
import PGQueuer

main :: IO ()
main = do
  let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
  uuid <- nextRandom

  withQueueManager connStr defaultDBSettings uuid $ \qm -> do
    setupSchema qm
    _ <- verifyStructure qm

    -- 1. Register a Cron Schedule to scrape every minute for demonstration (Cron expressions depend on the cron library, assuming * * * * * is every minute)
    registerSchedule qm (CronExpression "* * * * *") (Entrypoint "ScrapeCurrency")

    -- 2. Register the Entrypoint that simulates the flaky API
    qm1 <- registerEntrypoint qm (Entrypoint "ScrapeCurrency") $ \_payload -> do
      now <- getPOSIXTime
      -- Fail pseudo-randomly based on timestamp (simulate 502 Bad Gateway)
      let isFlaky = (round now :: Int) `mod` 5 /= 0
      if isFlaky
        then do
          putStrLn "[Scraper] API returned 502 Bad Gateway. Throwing JobRetryableException..."
          throwIO $ JobRetryableException "Flaky API 502"
        else putStrLn "[Scraper] Currency data scraped successfully!"

    let execParams =
          [ EntrypointExecutionParameter
              { paramEntrypoint = Entrypoint "ScrapeCurrency"
              , paramConcurrencyLimit = 5
              , paramMaxAttempts = 5
              , paramBackoffStrategy = Exponential 2 60 -- Base 2s, Cap 60s
              , paramJitter = EqualJitter
              }
          ]

    -- 3. Launch an admin thread that finds dead jobs and forcefully retries them
    _ <- forkIO $ forever $ do
      threadDelay (30 * 1000 * 1000) -- every 30 seconds
      putStrLn "[Admin] Checking for quarantined jobs..."
      failedJobs <- listFailedJobs qm1 10
      if null failedJobs
        then putStrLn "[Admin] No dead jobs found."
        else do
          putStrLn $ "[Admin] Found " ++ show (length failedJobs) ++ " dead jobs. Force retrying..."
          now <- getCurrentTime
          retryJobs qm1 [(jobId j, now, fromIntegral (jobAttempts j + 1)) | j <- failedJobs]
          putStrLn $ "[Admin] Successfully requeued " ++ show (length failedJobs) ++ " jobs."

    -- 4. Start worker loop
    putStrLn "Worker started with EqualJitter backoff. Processing jobs..."
    workerLoop qm1 defaultBufferConfig execParams
