module PGQueuer (
    QueueManager (..),
    createQueueManager,
    registerEntrypoint,
    enqueue,
    enqueueMultiple,
    dequeue,
    logJobs,
    getQueueSize,
    clearQueue,
    listFailedJobs,
    listJobStatusById,
    markJobAsCancelled,
    updateHeartbeat,
    requeueJobs,
    retryJob,
    verifyStructure,
    installSchema,
    uninstallSchema,
    withQueueManager,
    module PGQueuer.Types,
    module PGQueuer.Settings,
) where

import Control.Exception (bracket)
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
createQueueManager ::
    Connection ->
    DBSettings ->
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
    QueueManager ->
    Entrypoint ->
    EntrypointHandler ->
    IO QueueManager
registerEntrypoint qm (Entrypoint ep) handler = do
    return
        qm
            { qmEntrypoints = Map.insert ep handler (qmEntrypoints qm)
            }

-- ============================================================================
-- Queue operations (wrappers around Query module)
-- ============================================================================

-- | Enqueue a single job
enqueue ::
    QueueManager ->
    Entrypoint ->
    Maybe BL.ByteString ->
    Int ->
    Maybe NominalDiffTime ->
    Maybe Text ->
    Maybe Value ->
    IO [JobId]
enqueue qm =
    Q.enqueueSingle
        (qmConnection qm)
        (qmSettings qm)

-- | Enqueue multiple jobs
enqueueMultiple ::
    QueueManager ->
    [Entrypoint] ->
    [Maybe BL.ByteString] ->
    [Int] ->
    [Maybe NominalDiffTime] ->
    [Maybe Text] ->
    [Maybe Value] ->
    IO [JobId]
enqueueMultiple qm =
    Q.enqueueMultiple
        (qmConnection qm)
        (qmSettings qm)

-- | Dequeue jobs
dequeue ::
    QueueManager ->
    Int ->
    [EntrypointExecutionParameter] ->
    Maybe Int ->
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
    QueueManager ->
    [(JobId, JobStatus, Maybe Value)] ->
    IO ()
logJobs qm =
    Q.logJobs (qmConnection qm) (qmSettings qm)

-- | Get queue size statistics
getQueueSize :: QueueManager -> IO [QueueStatistics]
getQueueSize qm = Q.queueSize (qmConnection qm) (qmSettings qm)

-- | Clear the queue
clearQueue ::
    QueueManager ->
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
    ByteString ->
    DBSettings ->
    UUID ->
    (QueueManager -> IO a) ->
    IO a
withQueueManager connStr settings queueMgrId action = do
    bracket
        (connectPostgreSQL connStr >>= \conn -> createQueueManager conn settings queueMgrId)
        (close . qmConnection)
        action
