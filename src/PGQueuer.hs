{- |
Module      : PGQueuer
Description : Main interface for the PGQueuer job queue ecosystem.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

This module provides the high-level API for interacting with PGQueuer.
It re-exports essential types and settings required to configure,
manage, and interact with the database-backed queue.
-}
module PGQueuer (
    -- * Queue Manager
    QueueManager (..),
    createQueueManager,
    withQueueManager,

    -- * Entrypoint Management
    registerEntrypoint,

    -- * Job Operations
    workerLoop,
    enqueue,
    enqueueMultiple,
    dequeue,
    markJobAsCancelled,
    requeueJobs,
    retryJob,
    updateHeartbeat,
    logJobs,

    -- * Queue Statistics and Management
    getQueueSize,
    clearQueue,
    listFailedJobs,
    listJobStatusById,

    -- * Schema management
    verifyStructure,
    installSchema,
    uninstallSchema,

    -- * Re-exported modules
    module PGQueuer.Types,
    module PGQueuer.Settings,
) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import Control.Monad (forever)
import Data.Aeson (Value)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.UUID (UUID)
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)

import qualified PGQueuer.Query as Q
import PGQueuer.Schema (install, uninstall, verifyStructure_)
import PGQueuer.Settings
import PGQueuer.Types

-- | Core state context for managing Queue.
data QueueManager = QueueManager
    { qmConnection :: Connection
    -- ^ Database connection
    , qmSettings :: DBSettings
    -- ^ Database settings
    , qmEntrypoints :: Map Text EntrypointHandler
    -- ^ Registered entrypoint handlers
    , qmQueueManagerId :: UUID
    -- ^ Unique identifier for this QueueManager instance
    }

-- | Entrypoint handler type
type EntrypointHandler = Job -> IO ()

-- | Create a new QueueManager from a connection
createQueueManager ::
    -- | Database connection
    Connection ->
    -- | Database settings
    DBSettings ->
    -- | Unique identifier for this QueueManager instance
    UUID ->
    IO QueueManager
createQueueManager conn settings queueMgrId = do
    return $
        QueueManager
            { qmConnection = conn
            , qmSettings = settings
            , qmEntrypoints = Map.empty
            , qmQueueManagerId = queueMgrId
            }

-- | Register an entrypoint handler
registerEntrypoint ::
    -- | QueueManager instance
    QueueManager ->
    -- | Entrypoint to register
    Entrypoint ->
    -- | Handler function for the entrypoint
    EntrypointHandler ->
    IO QueueManager
registerEntrypoint qm (Entrypoint ep) handler = do
    return
        qm
            { qmEntrypoints = Map.insert ep handler (qmEntrypoints qm)
            }

-- | Continuously dequeue jobs and dispatch them to registered handlers.
workerLoop :: QueueManager -> [EntrypointExecutionParameter] -> IO ()
workerLoop qm params = forever $ do
    jobs <- dequeue qm defaultBatchSize params Nothing defaultHeartbeatTimeout
    if null jobs
        then threadDelay 1000000
        else mapM_ (dispatchJob qm) jobs

dispatchJob :: QueueManager -> Job -> IO ()
dispatchJob qm job =
    case Map.lookup entrypointName (qmEntrypoints qm) of
        Nothing -> fail $ "No handler registered for entrypoint: " ++ show entrypointName
        Just handler -> handler job
  where
    Entrypoint entrypointName = jobEntrypoint job

-- ============================================================================
-- Queue operations (wrappers around Query module)
-- ============================================================================

-- | Enqueue a single job
enqueue ::
    -- | QueueManager instance
    QueueManager ->
    -- | Entrypoint for the job
    Entrypoint ->
    -- | Optional payload for the job
    Maybe BL.ByteString ->
    -- | Priority of the job
    Int ->
    -- | Optional delay before the job can be executed
    Maybe NominalDiffTime ->
    -- | Optional deduplication key for the job
    Maybe Text ->
    -- | Optional headers for the job
    Maybe Value ->
    IO [JobId]
enqueue qm =
    Q.enqueueSingle
        (qmConnection qm)
        (qmSettings qm)

-- | Enqueue multiple jobs
enqueueMultiple ::
    -- | QueueManager instance
    QueueManager ->
    -- | List of entrypoints for the jobs
    [Entrypoint] ->
    -- | List of optional payloads for the jobs
    [Maybe BL.ByteString] ->
    -- | List of priorities for the jobs
    [Int] ->
    -- | List of optional delays before the jobs can be executed
    [Maybe NominalDiffTime] ->
    -- | List of optional deduplication keys for the jobs
    [Maybe Text] ->
    -- | List of optional headers for the jobs
    [Maybe Value] ->
    IO [JobId]
enqueueMultiple qm =
    Q.enqueueMultiple
        (qmConnection qm)
        (qmSettings qm)

-- | Dequeue jobs
dequeue ::
    -- | QueueManager instance
    QueueManager ->
    -- | Batch size for dequeuing jobs
    Int ->
    -- | List of entrypoint execution parameters
    [EntrypointExecutionParameter] ->
    -- | Optional maximum number of jobs to dequeue
    Maybe Int ->
    -- | Heartbeat timeout in seconds
    Int ->
    IO [Job]
dequeue qm batchSize params =
    Q.dequeue
        (qmConnection qm)
        (qmSettings qm)
        batchSize
        params
        (qmQueueManagerId qm)

-- | Log job status changes
logJobs ::
    -- | QueueManager instance
    QueueManager ->
    -- | List of job status changes with optional traceback
    [(JobId, JobStatus, Maybe Value)] ->
    IO ()
logJobs qm =
    Q.logJobs (qmConnection qm) (qmSettings qm)

-- | Get queue size statistics
getQueueSize :: QueueManager -> IO [QueueStatistics]
getQueueSize qm = Q.queueSize (qmConnection qm) (qmSettings qm)

-- | Clear the queue
clearQueue ::
    -- | QueueManager instance
    QueueManager ->
    -- | Optional list of entrypoints to clear; if Nothing, clears all
    Maybe [Entrypoint] ->
    IO ()
clearQueue qm = Q.clearQueue (qmConnection qm) (qmSettings qm)

-- | List failed jobs
listFailedJobs :: QueueManager -> Int -> IO [Job]
listFailedJobs qm = Q.listFailedJobs (qmConnection qm) (qmSettings qm)

-- | List Job status by Id
listJobStatusById :: QueueManager -> [JobId] -> IO [(JobId, JobStatus)]
listJobStatusById qm = Q.jobStatusById (qmConnection qm) (qmSettings qm)

-- | Mark jobs as cancelled
markJobAsCancelled :: QueueManager -> [JobId] -> IO ()
markJobAsCancelled qm = Q.markJobAsCancelled (qmConnection qm) (qmSettings qm)

-- | Update heartbeat
updateHeartbeat :: QueueManager -> [JobId] -> IO ()
updateHeartbeat qm = Q.updateHeartbeat (qmConnection qm) (qmSettings qm)

-- | Requeue jobs
requeueJobs :: QueueManager -> [JobId] -> IO ()
requeueJobs qm = Q.requeueJobs (qmConnection qm) (qmSettings qm)

-- | Retry a job
retryJob :: QueueManager -> Job -> NominalDiffTime -> Maybe Value -> IO ()
retryJob qm = Q.retryJob (qmConnection qm) (qmSettings qm)

-- ============================================================================
-- Schema management
-- ============================================================================

-- | Verify schema structure
verifyStructure :: QueueManager -> IO (Either String ())
verifyStructure qm = PGQueuer.Schema.verifyStructure_ (qmConnection qm) (qmSettings qm)

-- | Install schema
installSchema :: QueueManager -> IO ()
installSchema qm = install (qmConnection qm) (qmSettings qm)

-- | Uninstall schema
uninstallSchema :: QueueManager -> IO ()
uninstallSchema qm = uninstall (qmConnection qm) (qmSettings qm)

-- | Run a QueueManager within a bracket (for resource safety)
withQueueManager ::
    -- | Connection string for PostgreSQL
    ByteString ->
    -- | Database settings
    DBSettings ->
    -- | Unique identifier for this QueueManager instance
    UUID ->
    -- | Action to run with the QueueManager
    (QueueManager -> IO a) ->
    IO a
withQueueManager connStr settings queueMgrId action = do
    bracket
        (connectPostgreSQL connStr >>= \conn -> createQueueManager conn settings queueMgrId)
        (close . qmConnection)
        action
