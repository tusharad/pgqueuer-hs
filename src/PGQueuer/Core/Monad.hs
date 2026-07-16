{- |
Module      : PGQueuer.Core.Monad
Description : Abstract typeclass for database queue operations.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Defines the 'MonadPGQueuer' typeclass which serves as the contract
for any database engine backend. This abstraction enables swapping
out low-level database clients without modifying the concurrency
or worker loop infrastructure.
-}
module PGQueuer.Core.Monad (
    MonadPGQueuer (..),
)
where

import Data.Aeson (Value)
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int32)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import Data.UUID (UUID)
import PGQueuer.Types

{- | Abstract interface for database-backed queue operations.

Any backend that implements this typeclass provides the full
lifecycle management for jobs: enqueueing, dequeueing with
lock-free concurrency via @FOR UPDATE SKIP LOCKED@, status
logging, heartbeat maintenance, retry logic, and transactional
boundaries.
-}
class (Monad m) => MonadPGQueuer m where
    {- | Enqueue a single job with optional payload, priority, delay,
    deduplication key, and headers. Returns the assigned 'JobId's.
    -}
    enqueue ::
        Entrypoint ->
        Maybe BL.ByteString ->
        Int ->
        Maybe NominalDiffTime ->
        Maybe Text ->
        Maybe Value ->
        m [JobId]

    {- | Dequeue a batch of jobs matching the given entrypoint parameters.
    Uses @FOR UPDATE SKIP LOCKED@ internally to avoid contention.
    -}
    dequeue ::
        -- | Batch size
        Int ->
        -- | Per-entrypoint execution parameters
        [EntrypointExecutionParameter] ->
        -- | Queue manager identifier
        UUID ->
        -- | Global concurrency limit
        Maybe Int ->
        -- | Heartbeat timeout in seconds
        Int ->
        m [Job]

    {- | Atomically update job statuses and append entries to the
    audit log table.
    -}
    logJobs :: [(JobId, JobStatus, Maybe Value)] -> m ()

    {- | Refresh heartbeat timestamps for the given active jobs.
    Only updates the @heartbeat@ column (HOT-friendly).
    -}
    updateHeartbeat :: [JobId] -> m ()

    {- | Retry failed jobs in bulk.
    Provides a list of (JobId, newExecuteAfter, newAttempts).
    -}
    retryJobs :: [(JobId, UTCTime, Int32)] -> m ()

    {- | Execute an action within a database transaction boundary.
    Provides @BEGIN@, @COMMIT@ on success, and @ROLLBACK@ on exception.
    -}
    withTransaction :: m a -> m a

    {- | Insert a cron schedule if it does not already exist.
    Matches the Python implementation's `insert_schedule` with ON CONFLICT DO NOTHING.
    -}
    insertSchedule :: CronExpression -> Entrypoint -> m ()

    {- | Fetch due schedules safely using FOR UPDATE SKIP LOCKED.
    Atomically transitions them to 'picked' status and returns the Schedule records.
    -}
    fetchSchedules :: m [Schedule]

    {- | Reset a schedule's status back to 'queued' after its job has been dispatched.
    Sets last_run = NOW() and updates next_run to the provided UTCTime.
    -}
    setScheduleQueued :: ScheduleId -> UTCTime -> m ()

    {- | Get the earliest next_run among queued schedules.
    Used by the cron scheduler to determine how long to sleep.
    -}
    getEarliestNextRun :: m (Maybe UTCTime)
