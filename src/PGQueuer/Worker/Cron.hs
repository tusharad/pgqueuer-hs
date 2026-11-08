{-# LANGUAGE ScopedTypeVariables #-}

{- |
Module      : PGQueuer.Worker.Cron
Description : SchedulerManager for distributed Cron Jobs.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

This module provides the logic for the distributed cron scheduler loop.
-}
module PGQueuer.Worker.Cron
  ( runCronScheduler
  , calculateNextRunTime
  ) where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, catch)
import Control.Monad (forM_, forever)
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import Database.PostgreSQL.Simple (Connection)
import System.Cron (nextMatch, parseCronSchedule)

import qualified PGQueuer.Query as Q
import PGQueuer.Settings (DBSettings)
import PGQueuer.Types

-- | Calculate the next run time for a given cron expression and reference time.
calculateNextRunTime :: CronExpression -> UTCTime -> Either String UTCTime
calculateNextRunTime (CronExpression exprText) refTime =
  case parseCronSchedule exprText of
    Left err -> Left err
    Right schedule ->
      case nextMatch schedule refTime of
        Just nextTime -> Right nextTime
        Nothing -> Left "No matching future run time found for cron schedule"

-- | Run the cron scheduler loop.
runCronScheduler :: Connection -> DBSettings -> IO ()
runCronScheduler conn settings = forever $ do
  -- 1. Fetch schedules that are due (status = 'queued' AND next_run <= NOW())
  -- This uses FOR UPDATE SKIP LOCKED to prevent other workers from picking them up.
  schedules <- Q.fetchSchedules conn settings `catch` (\(_ :: SomeException) -> return [])

  now <- getCurrentTime

  -- 2. For each picked schedule, dispatch a job to the queue and reset its state
  forM_ schedules $ \schedule -> do
    let ep = scheduleEntrypoint schedule
        sid = scheduleId schedule

    -- Enqueue the job for the cron task
    _ <-
      Q.enqueueMultiple
        conn
        settings
        [ep]
        [Nothing]
        [0]
        [Nothing]
        [Nothing]
        [Nothing]
        [Nothing]
        [Nothing]
        [Queued]
        `catch` (\(_ :: SomeException) -> return [])

    -- Calculate next run using the cron expression
    let nextRun = case calculateNextRunTime (scheduleExpression schedule) now of
          Right nextTime -> nextTime
          Left _ -> now

    -- Reset schedule state back to queued
    Q.setScheduleQueued conn settings sid nextRun
      `catch` (\(_ :: SomeException) -> return ())

  -- 3. Calculate sleep time
  -- Get the earliest next_run across all queued schedules
  mbEarliest <- Q.getEarliestNextRun conn settings `catch` (\(_ :: SomeException) -> return Nothing)
  sleepTime <-
    getCurrentTime >>= \newNow ->
      case mbEarliest of
        Just earliest -> do
          let diff = diffUTCTime earliest newNow
          if diff <= 0
            then return 1 -- Immediate retry if something is due
            else return (min (ceiling diff * 1000000) 60000000) -- Sleep up to 60s
        Nothing -> return 60000000 -- Default to 60s if no schedules exist
  threadDelay sleepTime
