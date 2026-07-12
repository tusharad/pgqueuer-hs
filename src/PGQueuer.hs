{-# LANGUAGE OverloadedStrings #-}

module PGQueuer 
  ( QueueManager (..)
  , createQueueManager
  , registerEntrypoint
  , enqueue
  , enqueueMultiple
  , dequeue
  , logJobs
  , getQueueSize
  , clearQueue
  , listFailedJobs
  , listJobStatusById
  , markJobAsCancelled
  , updateHeartbeat
  , requeueJobs
  , retryJob
  , verifyStructure
  , installSchema
  , uninstallSchema
  , withQueueManager
  , module PGQueuer.Types
  , module PGQueuer.Settings
  ) where

import Control.Exception (bracket)
import Data.Aeson (Value)
import Data.ByteString (ByteString)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.UUID (UUID)
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)
import qualified Data.Map.Strict as Map

import PGQueuer.Types
import PGQueuer.Settings
import PGQueuer.Schema (install, uninstall, verifyStructure_)
import qualified PGQueuer.Query as Q

-- ============================================================================
-- Queue Manager
-- ============================================================================

data QueueManager = QueueManager
  { qmConnection :: Connection
  , qmSettings :: DBSettings
  , qmEntrypoints :: Map Text EntrypointHandler
  , qmQueueManagerId :: UUID
  }

-- | Entrypoint handler type
type EntrypointHandler = Job -> IO ()

-- | Create a new QueueManager from a connection
createQueueManager
  :: Connection
  -> DBSettings
  -> UUID
  -> IO QueueManager
createQueueManager conn settings queueMgrId = do
  return $ QueueManager
    { qmConnection = conn
    , qmSettings = settings
    , qmEntrypoints = Map.empty
    , qmQueueManagerId = queueMgrId
    }

-- | Register an entrypoint handler
registerEntrypoint
  :: QueueManager
  -> Entrypoint
  -> EntrypointHandler
  -> IO QueueManager
registerEntrypoint qm (Entrypoint ep) handler = do
  return qm
    { qmEntrypoints = Map.insert ep handler (qmEntrypoints qm)
    }

-- ============================================================================
-- Queue operations (wrappers around Query module)
-- ============================================================================

-- | Enqueue a single job
enqueue
  :: QueueManager
  -> Entrypoint
  -> Maybe ByteString
  -> Int
  -> Maybe NominalDiffTime
  -> Maybe Text
  -> Maybe Value
  -> IO [JobId]
enqueue qm entrypoint payload priority executeAfter dedupeKey headers =
  Q.enqueueSingle 
    (qmConnection qm) 
    (qmSettings qm) 
    entrypoint 
    payload 
    priority 
    executeAfter 
    dedupeKey 
    headers

-- | Enqueue multiple jobs
enqueueMultiple
  :: QueueManager
  -> [Entrypoint]
  -> [Maybe ByteString]
  -> [Int]
  -> [Maybe NominalDiffTime]
  -> [Maybe Text]
  -> [Maybe Value]
  -> IO [JobId]
enqueueMultiple qm entrypoints payloads priorities executeAfters dedupeKeys headers =
  Q.enqueueMultiple
    (qmConnection qm)
    (qmSettings qm)
    entrypoints
    payloads
    priorities
    executeAfters
    dedupeKeys
    headers

-- | Dequeue jobs
dequeue
  :: QueueManager
  -> Int
  -> [EntrypointExecutionParameter]
  -> Maybe Int
  -> Int
  -> IO [Job]
dequeue qm batchSize params globalLimit heartbeatTimeout =
  Q.dequeue
    (qmConnection qm)
    (qmSettings qm)
    batchSize
    params
    (qmQueueManagerId qm)
    globalLimit
    heartbeatTimeout

-- | Log job status changes
logJobs
  :: QueueManager
  -> [(JobId, JobStatus, Maybe Value)]
  -> IO ()
logJobs qm statuses =
  Q.logJobs (qmConnection qm) (qmSettings qm) statuses

-- | Get queue size statistics
getQueueSize :: QueueManager -> IO [QueueStatistics]
getQueueSize qm = Q.queueSize (qmConnection qm) (qmSettings qm)

-- | Clear the queue
clearQueue
  :: QueueManager
  -> Maybe [Entrypoint]
  -> IO ()
clearQueue qm eps = Q.clearQueue (qmConnection qm) (qmSettings qm) eps

-- | List failed jobs
listFailedJobs :: QueueManager -> Int -> IO [Job]
listFailedJobs qm limit = Q.listFailedJobs (qmConnection qm) (qmSettings qm) limit

-- | List Job status by Id
listJobStatusById :: QueueManager -> [JobId] -> IO [(JobId, JobStatus)]
listJobStatusById qm jobIds = Q.jobStatusById (qmConnection qm) (qmSettings qm) jobIds

-- | Mark jobs as cancelled
markJobAsCancelled :: QueueManager -> [JobId] -> IO ()
markJobAsCancelled qm ids = Q.markJobAsCancelled (qmConnection qm) (qmSettings qm) ids

-- | Update heartbeat
updateHeartbeat :: QueueManager -> [JobId] -> IO ()
updateHeartbeat qm ids = Q.updateHeartbeat (qmConnection qm) (qmSettings qm) ids

-- | Requeue jobs
requeueJobs :: QueueManager -> [JobId] -> IO ()
requeueJobs qm ids = Q.requeueJobs (qmConnection qm) (qmSettings qm) ids

-- | Retry a job
retryJob :: QueueManager -> Job -> NominalDiffTime -> Maybe Value -> IO ()
retryJob qm job delay tb = Q.retryJob (qmConnection qm) (qmSettings qm) job delay tb

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
withQueueManager
  :: ByteString
  -> DBSettings
  -> UUID
  -> (QueueManager -> IO a)
  -> IO a
withQueueManager connStr settings queueMgrId action = do
  bracket
    (connectPostgreSQL connStr >>= \conn -> createQueueManager conn settings queueMgrId)
    (\qm -> close (qmConnection qm))
    action
