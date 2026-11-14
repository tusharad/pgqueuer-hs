{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : PGQueuer.Types
Description : Core domain models, data types, and parameters for PGQueuer.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

This module defines all primary domain representations for PGQueuer: jobs,
schedules, statuses, failure strategies, jitter algorithms, retry backoffs,
and event payloads received from PostgreSQL @LISTEN@ / @NOTIFY@ channels.
-}
module PGQueuer.Types
  ( -- * Identifiers & Labels
    JobId (..)
  , ScheduleId (..)
  , Entrypoint (..)
  , CronExpression (..)
  , Channel (..)

    -- * Job Representation & Lifecycle
  , Job (..)
  , JobStatus (..)
  , jobStatusToText
  , textToJobStatus

    -- * Execution & Worker Configuration
  , EntrypointExecutionParameter (..)
  , BackoffStrategy (..)
  , Jitter (..)
  , OnFailure (..)
  , onFailureToText
  , textToOnFailure

    -- * Scheduling
  , Schedule (..)

    -- * Logging & Statistics
  , QueueStatistics (..)
  , LogEntry (..)
  , LogStatistics (..)
  , TracebackRecord (..)

    -- * Events & Notifications
  , Operation (..)
  , Event (..)

    -- * Exceptions
  , JobRetryableException (..)
  , JobPermanentException (..)

    -- * Defaults
  , defaultChannel
  , defaultBatchSize
  , defaultHeartbeatTimeout
  ) where

import Data.Aeson (FromJSON, ToJSON, Value)
import qualified Data.ByteString.Lazy as BL
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import Data.UUID (UUID)
import Database.PostgreSQL.Simple.FromField (FromField (..))
import Database.PostgreSQL.Simple.FromRow (FromRow (..), field)
import Database.PostgreSQL.Simple.ToField (ToField (..))
import GHC.Generics (Generic)
import UnliftIO.Exception (Exception)

-- | Unique integer identifier for an enqueued job.
newtype JobId = JobId Int
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (FromField, ToField, FromJSON, ToJSON)

-- | Unique integer identifier for a recurring cron schedule.
newtype ScheduleId = ScheduleId Int
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (FromField, ToField, FromJSON, ToJSON)

-- | Named destination / routing key that associates a job with its handler.
newtype Entrypoint = Entrypoint Text
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (FromField, ToField, FromJSON, ToJSON)

-- | 5-field cron expression string (e.g. @"* * * * *"@ or @"0 0 * * *"@).
newtype CronExpression = CronExpression Text
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (FromField, ToField, FromJSON, ToJSON)

-- | PostgreSQL pub/sub channel name for @LISTEN@ / @NOTIFY@ signals.
newtype Channel = Channel Text
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (FromField, ToField, FromJSON, ToJSON)

-- | Lifecycle states of a job in the queue.
data JobStatus
  = -- | Job is waiting in the database to be claimed by an active worker.
    Queued
  | -- | Job has been claimed by a worker using @FOR UPDATE SKIP LOCKED@.
    Picked
  | -- | Job processing completed successfully.
    Successful
  | -- | Job failed and has exhausted its retry attempts.
    Failed
  | -- | Job execution resulted in an unhandled exception or crash.
    Exception
  | -- | Job was explicitly cancelled via 'PGQueuer.markJobAsCancelled'.
    Canceled
  | -- | Job was deleted or purged from the queue ledger.
    Deleted
  | -- | Job is held waiting for upstream dependency jobs (DAG workflow).
    Held
  deriving stock (Show, Eq, Ord, Generic, Bounded, Enum)
  deriving anyclass (FromJSON, ToJSON)

-- | Convert a 'JobStatus' to its lowercase PostgreSQL enum text representation.
jobStatusToText :: JobStatus -> Text
jobStatusToText Queued = "queued"
jobStatusToText Picked = "picked"
jobStatusToText Successful = "successful"
jobStatusToText Failed = "failed"
jobStatusToText Exception = "exception"
jobStatusToText Canceled = "canceled"
jobStatusToText Deleted = "deleted"
jobStatusToText Held = "held"

-- | Parse a 'JobStatus' from its PostgreSQL text enum representation.
textToJobStatus :: Text -> Maybe JobStatus
textToJobStatus "queued" = Just Queued
textToJobStatus "picked" = Just Picked
textToJobStatus "successful" = Just Successful
textToJobStatus "failed" = Just Failed
textToJobStatus "exception" = Just Exception
textToJobStatus "canceled" = Just Canceled
textToJobStatus "deleted" = Just Deleted
textToJobStatus "held" = Just Held
textToJobStatus _ = Nothing

instance FromField JobStatus where
  fromField f v = fromField f v >>= \t -> return (fromMaybe Queued (textToJobStatus t))

instance ToField JobStatus where
  toField = toField . jobStatusToText

-- | Database table change operations emitted by trigger functions.
data Operation
  = -- | An INSERT statement occurred.
    Insert
  | -- | An UPDATE statement occurred.
    Update
  | -- | A DELETE statement occurred.
    Delete
  | -- | A TRUNCATE statement occurred.
    Truncate
  deriving stock (Show, Eq, Ord, Generic, Bounded, Enum)
  deriving anyclass (FromJSON, ToJSON)

-- | Asynchronous event notification payload broadcast over PostgreSQL pub/sub channels.
data Event
  = -- | Trigger notification indicating queue table mutations.
    TableChangedEvent
      { eventChannel :: Channel
      -- ^ Channel the notification was broadcast over.
      , eventSentAt :: UTCTime
      -- ^ Timestamp when the notification was generated.
      , eventOperation :: Operation
      -- ^ Specific database DML operation performed.
      , eventTable :: Text
      -- ^ Target table name modified.
      }
  | -- | Explicit cancellation signal for a list of job IDs.
    CancellationEvent
      { eventChannel :: Channel
      -- ^ Channel the cancellation was sent over.
      , eventSentAt :: UTCTime
      -- ^ Timestamp of cancellation dispatch.
      , eventIds :: [JobId]
      -- ^ Target job IDs marked for cancellation.
      }
  | -- | Keepalive or heartbeat ping signal from an active manager.
    HealthCheckEvent
      { eventChannel :: Channel
      -- ^ Channel carrying the health check.
      , eventSentAt :: UTCTime
      -- ^ Ping timestamp.
      , eventId :: UUID
      -- ^ Originating queue manager UUID.
      }
  deriving stock (Show, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Full database record representing an active or historical job.
data Job = Job
  { jobId :: JobId
  -- ^ Unique primary key of the job.
  , jobPriority :: Int
  -- ^ Integer priority (lower numerical values dequeue first).
  , jobCreated :: UTCTime
  -- ^ Timestamp when the job was initially inserted.
  , jobUpdated :: UTCTime
  -- ^ Timestamp of the most recent state or field modification.
  , jobHeartbeat :: UTCTime
  -- ^ Worker keepalive timestamp (used to detect dead/stalled workers).
  , jobExecuteAfter :: UTCTime
  -- ^ Earliest time this job may be claimed for execution.
  , jobStatus :: JobStatus
  -- ^ Current lifecycle status of the job.
  , jobEntrypoint :: Entrypoint
  -- ^ Destination entrypoint that handles this job.
  , jobPayload :: Maybe BL.ByteString
  -- ^ Raw binary or serialized JSON payload.
  , jobAttempts :: Int
  -- ^ Number of execution attempts completed so far.
  , jobQueueManagerId :: Maybe UUID
  -- ^ UUID of the worker node currently holding this job.
  , jobHeaders :: Maybe Value
  -- ^ Optional metadata headers (e.g. OpenTelemetry trace contexts).
  , jobParentId :: Maybe JobId
  -- ^ Parent job ID for DAG workflow dependencies.
  , jobParentState :: Maybe Value
  -- ^ State passed to or collected by parent workflow nodes.
  }
  deriving stock (Show, Eq, Generic)

-- | Full database record representing a scheduled recurring cron job.
data Schedule = Schedule
  { scheduleId :: ScheduleId
  -- ^ Unique primary key of the recurring schedule.
  , scheduleExpression :: CronExpression
  -- ^ Cron expression defining recurring trigger intervals.
  , scheduleEntrypoint :: Entrypoint
  -- ^ Target entrypoint to dispatch jobs to when triggered.
  , scheduleHeartbeat :: UTCTime
  -- ^ Scheduler keepalive timestamp.
  , scheduleCreated :: UTCTime
  -- ^ Timestamp when this schedule was registered.
  , scheduleUpdated :: UTCTime
  -- ^ Timestamp of the latest update.
  , scheduleNextRun :: UTCTime
  -- ^ Target timestamp for the next job emission.
  , scheduleLastRun :: Maybe UTCTime
  -- ^ Timestamp when the last job was dispatched, if any.
  , scheduleStatus :: JobStatus
  -- ^ Current status of the schedule record.
  }
  deriving stock (Show, Eq, Generic)

-- | Entry logged to the unlogged @pgqueuer_log@ table for high-throughput audit tracking.
data LogEntry = LogEntry
  { logCreated :: UTCTime
  -- ^ Creation timestamp of the log event.
  , logJobId :: JobId
  -- ^ Job ID associated with this log record.
  , logStatus :: JobStatus
  -- ^ Status recorded in this transition.
  , logPriority :: Int
  -- ^ Priority of the associated job.
  , logEntrypoint :: Entrypoint
  -- ^ Entrypoint of the associated job.
  , logTraceback :: Maybe Value
  -- ^ Stack trace or error payload captured on failure.
  , logAggregated :: Bool
  -- ^ Whether this entry has been folded into statistics.
  , logParentId :: Maybe JobId
  -- ^ Parent job ID if part of a workflow.
  }
  deriving stock (Show, Eq, Generic)

-- | Aggregated queue throughput and depth metrics.
data QueueStatistics = QueueStatistics
  { statsCount :: Int
  -- ^ Number of jobs matching this grouping.
  , statsEntrypoint :: Entrypoint
  -- ^ Target entrypoint.
  , statsPriority :: Int
  -- ^ Priority level.
  , statsStatus :: JobStatus
  -- ^ Job lifecycle status.
  }
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Aggregated historical log entry metrics.
data LogStatistics = LogStatistics
  { logStatsCount :: Int
  -- ^ Aggregated count of events.
  , logStatsCreated :: UTCTime
  -- ^ Aggregation window bucket timestamp.
  , logStatsEntrypoint :: Entrypoint
  -- ^ Entrypoint category.
  , logStatsPriority :: Int
  -- ^ Priority level.
  , logStatsStatus :: JobStatus
  -- ^ Status recorded.
  }
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Captured exception type, error message, and formatted callstack.
data TracebackRecord = TracebackRecord
  { traceJobId :: JobId
  -- ^ Job ID that failed.
  , traceTimestamp :: UTCTime
  -- ^ Time exception occurred.
  , traceExceptionType :: Text
  -- ^ Stringified exception type / constructor.
  , traceExceptionMessage :: Text
  -- ^ Detailed exception message.
  , traceTraceback :: Text
  -- ^ Formatted call stack or traceback.
  }
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Action to take when a job exceeds its maximum retry threshold.
data OnFailure
  = -- | Permanently remove the failed job from the queue ledger.
    OnDelete
  | -- | Retain the job in the dead-letter 'Held' state for manual triage.
    OnHold
  deriving stock (Show, Eq, Ord, Generic, Bounded, Enum)
  deriving anyclass (FromJSON, ToJSON)

-- | Convert an 'OnFailure' action to its text representation.
onFailureToText :: OnFailure -> Text
onFailureToText OnDelete = "delete"
onFailureToText OnHold = "hold"

-- | Parse an 'OnFailure' action from its text representation.
textToOnFailure :: Text -> Maybe OnFailure
textToOnFailure "delete" = Just OnDelete
textToOnFailure "hold" = Just OnHold
textToOnFailure _ = Nothing

-- | Configuration parameters controlling execution limits and retry resilience per entrypoint.
data EntrypointExecutionParameter = EntrypointExecutionParameter
  { paramEntrypoint :: Entrypoint
  -- ^ Target entrypoint to configure.
  , paramConcurrencyLimit :: Int
  -- ^ Maximum simultaneous active jobs for this entrypoint across workers (0 = unlimited).
  , paramMaxAttempts :: Int
  -- ^ Maximum allowed retry attempts before declaring failure.
  , paramBackoffStrategy :: BackoffStrategy
  -- ^ Mathematical formula calculating retry delays.
  , paramJitter :: Jitter
  -- ^ Random jitter formula applied to delay calculations.
  }
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Strategy used for calculating backoff delays on job retries.
data BackoffStrategy
  = -- | Delay scales exponentially: @delay = min cap (base * 2^(attempts - 1))@.
    Exponential
      { backoffBase :: NominalDiffTime
      -- ^ Base retry delay.
      , backoffCap :: NominalDiffTime
      -- ^ Maximum delay cap.
      }
  | -- | Delay scales linearly: @delay = min cap (base * attempts)@.
    Linear
      { backoffBase :: NominalDiffTime
      -- ^ Base retry multiplier.
      , backoffCap :: NominalDiffTime
      -- ^ Maximum delay cap.
      }
  | -- | Constant delay regardless of attempt count.
    Constant
      { backoffDelay :: NominalDiffTime
      -- ^ Fixed retry delay.
      }
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Randomization strategy applied to retry delays to prevent thundering herd problems.
data Jitter
  = -- | Apply no randomization; use exact calculated delay.
    NoJitter
  | -- | Uniform random delay between 0 and calculated delay: @[0, delay)@.
    FullJitter
  | -- | Half deterministic, half uniform random: @delay / 2 + random([0, delay / 2))@.
    EqualJitter
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Exception indicating a transient, recoverable failure. Causes the job to be rescheduled with backoff.
newtype JobRetryableException = JobRetryableException Text
  deriving stock (Show, Eq)
  deriving anyclass (Exception)

-- | Exception indicating a fatal, non-recoverable failure. Immediately marks the job as permanently failed or held.
newtype JobPermanentException = JobPermanentException Text
  deriving stock (Show, Eq)
  deriving anyclass (Exception)

-- | Default PostgreSQL pub/sub channel name (@ch_pgqueuer@).
defaultChannel :: Channel
defaultChannel = Channel "ch_pgqueuer"

-- | Default batch size for fetching jobs (100).
defaultBatchSize :: Int
defaultBatchSize = 100

-- | Default worker heartbeat timeout in seconds (300 seconds / 5 minutes).
defaultHeartbeatTimeout :: Int
defaultHeartbeatTimeout = 300

instance FromRow Job where
  fromRow =
    Job
      <$> field -- jobId
      <*> field -- jobPriority
      <*> field -- jobCreated
      <*> field -- jobUpdated
      <*> field -- jobHeartbeat
      <*> field -- jobExecuteAfter
      <*> field -- jobStatus
      <*> field -- jobEntrypoint
      <*> field -- jobPayload
      <*> field -- jobAttempts
      <*> field -- jobQueueManagerId
      <*> field -- jobHeaders
      <*> field -- jobParentId
      <*> field -- jobParentState

instance FromRow LogEntry where
  fromRow =
    LogEntry
      <$> field -- logCreated
      <*> field -- logJobId
      <*> field -- logStatus
      <*> field -- logPriority
      <*> field -- logEntrypoint
      <*> field -- logTraceback
      <*> field -- logAggregated
      <*> field -- logParentId

instance FromRow QueueStatistics where
  fromRow =
    QueueStatistics
      <$> field -- statsCount
      <*> field -- statsEntrypoint
      <*> field -- statsPriority
      <*> field -- statsStatus

instance FromRow LogStatistics where
  fromRow =
    LogStatistics
      <$> field -- logStatsCount
      <*> field -- logStatsCreated
      <*> field -- logStatsEntrypoint
      <*> field -- logStatsPriority
      <*> field -- logStatsStatus

instance FromRow Schedule where
  fromRow =
    Schedule
      <$> field -- scheduleId
      <*> field -- scheduleExpression
      <*> field -- scheduleEntrypoint
      <*> field -- scheduleHeartbeat
      <*> field -- scheduleCreated
      <*> field -- scheduleUpdated
      <*> field -- scheduleNextRun
      <*> field -- scheduleLastRun
      <*> field -- scheduleStatus
