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
import Data.Text (Text)
import Data.Time (NominalDiffTime)
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

    {- | Retry a failed job after a specified delay, incrementing
    the attempt counter.
    -}
    retryJob :: Job -> NominalDiffTime -> Maybe Value -> m ()

    {- | Execute an action within a database transaction boundary.
    Provides @BEGIN@, @COMMIT@ on success, and @ROLLBACK@ on exception.
    -}
    withTransaction :: m a -> m a
