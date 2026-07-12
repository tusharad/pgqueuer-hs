{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module PGQueuer.Types (
    JobId (..),
    ScheduleId (..),
    Entrypoint (..),
    Channel (..),
    EntrypointExecutionParameter (..),
    Job (..),
    JobStatus (..),
    QueueStatistics (..),
    OnFailure (..),
    onFailureToText,
    textToOnFailure,
    defaultHeartbeatTimeout,
    defaultBatchSize,
    jobStatusToText,
    defaultChannel,
) where

import Data.Aeson (Value)
import Data.ByteString (ByteString)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID (UUID)
import Database.PostgreSQL.Simple.FromField (FromField (..))
import Database.PostgreSQL.Simple.FromRow (FromRow (..), field)
import Database.PostgreSQL.Simple.ToField (ToField (..))
import GHC.Generics (Generic)

-- ============================================================================
-- Type aliases and newtypes for safety
-- ============================================================================
newtype JobId = JobId Int
    deriving stock (Show, Eq, Ord, Generic)
    deriving newtype (FromField, ToField)

newtype ScheduleId = ScheduleId Int
    deriving stock (Show, Eq, Ord, Generic)
    deriving newtype (FromField, ToField)

newtype Entrypoint = Entrypoint Text
    deriving stock (Show, Eq, Ord, Generic)
    deriving newtype (FromField, ToField)

newtype CronExpression = CronExpression Text
    deriving stock (Show, Eq, Ord, Generic)
    deriving newtype (FromField, ToField)

newtype Channel = Channel Text
    deriving stock (Show, Eq, Ord, Generic)
    deriving newtype (FromField, ToField)

-- ============================================================================
-- Job Status type
-- ============================================================================

data JobStatus
    = Queued
    | Picked
    | Successful
    | Failed
    | Exception
    | Canceled
    | Deleted
    deriving stock (Show, Eq, Ord, Generic, Bounded, Enum)

jobStatusToText :: JobStatus -> Text
jobStatusToText Queued = "queued"
jobStatusToText Picked = "picked"
jobStatusToText Successful = "successful"
jobStatusToText Failed = "failed"
jobStatusToText Exception = "exception"
jobStatusToText Canceled = "canceled"
jobStatusToText Deleted = "deleted"

textToJobStatus :: Text -> Maybe JobStatus
textToJobStatus "queued" = Just Queued
textToJobStatus "picked" = Just Picked
textToJobStatus "successful" = Just Successful
textToJobStatus "failed" = Just Failed
textToJobStatus "exception" = Just Exception
textToJobStatus "canceled" = Just Canceled
textToJobStatus "deleted" = Just Deleted
textToJobStatus _ = Nothing

instance FromField JobStatus where
    fromField f v = fromField f v >>= \t -> return (fromMaybe Queued (textToJobStatus t))

instance ToField JobStatus where
    toField = toField . jobStatusToText

-- ============================================================================
-- Database Operation type
-- ============================================================================

data Operation
    = Insert
    | Update
    | Delete
    | Truncate
    deriving stock (Show, Eq, Ord, Generic, Bounded, Enum)

{-
operationToText :: Operation -> Text
operationToText Insert = "insert"
operationToText Update = "update"
operationToText Delete = "delete"
operationToText Truncate = "truncate"

textToOperation :: Text -> Maybe Operation
textToOperation "insert" = Just Insert
textToOperation "update" = Just Update
textToOperation "delete" = Just Delete
textToOperation "truncate" = Just Truncate
textToOperation _ = Nothing
-}
-- ============================================================================
-- Event types
-- ============================================================================

data Event
    = TableChangedEvent
        { eventChannel :: Channel
        , eventSentAt :: UTCTime
        , eventOperation :: Operation
        , eventTable :: Text
        }
    | CancellationEvent
        { eventChannel :: Channel
        , eventSentAt :: UTCTime
        , eventIds :: [JobId]
        }
    | HealthCheckEvent
        { eventChannel :: Channel
        , eventSentAt :: UTCTime
        , eventId :: UUID
        }
    deriving stock (Show, Generic)

-- ============================================================================
-- Job type
-- ============================================================================

data Job = Job
    { jobId :: JobId
    , jobPriority :: Int
    , jobCreated :: UTCTime
    , jobUpdated :: UTCTime
    , jobHeartbeat :: UTCTime
    , jobExecuteAfter :: UTCTime
    , jobStatus :: JobStatus
    , jobEntrypoint :: Entrypoint
    , jobPayload :: Maybe ByteString
    , jobAttempts :: Int
    , jobQueueManagerId :: Maybe UUID
    , jobHeaders :: Maybe Value
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- Log entry type
-- ============================================================================

data LogEntry = LogEntry
    { logCreated :: UTCTime
    , logJobId :: JobId
    , logStatus :: JobStatus
    , logPriority :: Int
    , logEntrypoint :: Entrypoint
    , logTraceback :: Maybe Value
    , logAggregated :: Bool
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- Queue statistics type
-- ============================================================================

data QueueStatistics = QueueStatistics
    { statsCount :: Int
    , statsEntrypoint :: Entrypoint
    , statsPriority :: Int
    , statsStatus :: JobStatus
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- Log statistics type
-- ============================================================================

data LogStatistics = LogStatistics
    { logStatsCount :: Int
    , logStatsCreated :: UTCTime
    , logStatsEntrypoint :: Entrypoint
    , logStatsPriority :: Int
    , logStatsStatus :: JobStatus
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- Schedule type
-- ============================================================================

data Schedule = Schedule
    { scheduleId :: ScheduleId
    , scheduleExpression :: CronExpression
    , scheduleEntrypoint :: Entrypoint
    , scheduleHeartbeat :: UTCTime
    , scheduleCreated :: UTCTime
    , scheduleUpdated :: UTCTime
    , scheduleNextRun :: UTCTime
    , scheduleLastRun :: Maybe UTCTime
    , scheduleStatus :: JobStatus
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- Traceback record
-- ============================================================================

data TracebackRecord = TracebackRecord
    { traceJobId :: JobId
    , traceTimestamp :: UTCTime
    , traceExceptionType :: Text
    , traceExceptionMessage :: Text
    , traceTraceback :: Text
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- On failure policy
-- ============================================================================

data OnFailure
    = OnDelete
    | OnHold
    deriving stock (Show, Eq, Ord, Generic, Bounded, Enum)

onFailureToText :: OnFailure -> Text
onFailureToText OnDelete = "delete"
onFailureToText OnHold = "hold"

textToOnFailure :: Text -> Maybe OnFailure
textToOnFailure "delete" = Just OnDelete
textToOnFailure "hold" = Just OnHold
textToOnFailure _ = Nothing

-- ============================================================================
-- Entrypoint execution parameter
-- ============================================================================

data EntrypointExecutionParameter = EntrypointExecutionParameter
    { paramEntrypoint :: Entrypoint
    , paramConcurrencyLimit :: Int
    }
    deriving stock (Show, Eq, Generic)

-- ============================================================================
-- Default values
-- ============================================================================

defaultChannel :: Channel
defaultChannel = Channel "pgqueuer"

defaultBatchSize :: Int
defaultBatchSize = 100

defaultHeartbeatTimeout :: Int
defaultHeartbeatTimeout = 300 -- 5 minutes in seconds

-- ============================================================================
-- FromRow instances for database records
-- ============================================================================

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
