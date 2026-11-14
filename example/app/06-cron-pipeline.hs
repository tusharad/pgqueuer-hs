{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Main
Description : Distributed Cron Job Scheduling and Worker Pipeline Example.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Demonstrates registering 5-field cron schedules ('CronExpression'), inspecting
scheduled jobs in PostgreSQL, and processing periodic recurring executions.
-}
module Main (main) where

import Control.Concurrent (threadDelay)
import Data.Time.Clock (getCurrentTime)
import Data.UUID.V4 (nextRandom)
import PGQueuer
import PGQueuer.Worker.Cron (calculateNextRunTime)

main :: IO ()
main = do
  let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
  uuid <- nextRandom

  withQueueManager connStr defaultDBSettings uuid $ \qm -> do
    setupSchema qm
    _ <- verifyStructure qm

    putStrLn "Registering periodic cron tasks..."
    -- 1. Schedule a task every minute
    let cronExpr = CronExpression "* * * * *"
    registerSchedule qm cronExpr (Entrypoint "GenerateHourlyMetrics")

    now <- getCurrentTime
    case calculateNextRunTime cronExpr now of
      Right nextRun -> putStrLn $ "Next execution scheduled for: " ++ show nextRun
      Left err -> putStrLn $ "Cron calculation error: " ++ err

    -- 2. Register handler for the cron entrypoint
    qm1 <- registerEntrypoint qm (Entrypoint "GenerateHourlyMetrics") $ \job -> do
      putStrLn $
        "[Cron Worker] Running periodic metric aggregation for job #"
          ++ show (let JobId jid = jobId job in jid)
      threadDelay 20000 -- simulate calculation
      putStrLn "[Cron Worker] Metric aggregation completed successfully."

    -- 3. Also enqueue an immediate manual trigger job
    putStrLn "Enqueuing an immediate manual trigger job..."
    _ <- enqueue qm1 (Entrypoint "GenerateHourlyMetrics") Nothing 0 Nothing Nothing Nothing

    -- 4. Start worker loop
    putStrLn "Worker started. Processing jobs... (Press Ctrl+C to stop)"
    let execParams =
          [ EntrypointExecutionParameter
              { paramEntrypoint = Entrypoint "GenerateHourlyMetrics"
              , paramConcurrencyLimit = 2
              , paramMaxAttempts = 3
              , paramBackoffStrategy = Exponential 2 30
              , paramJitter = FullJitter
              }
          ]
    workerLoop qm1 defaultBufferConfig execParams
