{-# LANGUAGE OverloadedStrings #-}

module PGQueuer.Query
  ( enqueueSingle
  , enqueueMultiple
  , dequeue
  , logJobs
  , queueSize
  , queuedWork
  , retryJob
  , requeueJobs
  , markJobAsCancelled
  , updateHeartbeat
  , jobStatus
  , clearQueue
  , listFailedJobs
  ) where

import Data.Aeson (Value)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Time (UTCTime, NominalDiffTime, addUTCTime)
import Data.UUID (UUID)
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromRow (FromRow (..), field)
import Database.PostgreSQL.Simple.Types (Only (..), Query (..))
import qualified Data.Text.Encoding as TE
import PGQueuer.Settings
import PGQueuer.Types

import qualified Data.Text as T

-- ============================================================================
-- Helper functions
-- ============================================================================

textToQuery :: Text -> Query
textToQuery = Query . TE.encodeUtf8

-- ============================================================================
-- FromRow instances for database records
-- ============================================================================

instance FromRow Job where
  fromRow = Job
    <$> field  -- jobId
    <*> field  -- jobPriority
    <*> field  -- jobCreated
    <*> field  -- jobUpdated
    <*> field  -- jobHeartbeat
    <*> field  -- jobExecuteAfter
    <*> field  -- jobStatus
    <*> field  -- jobEntrypoint
    <*> field  -- jobPayload
    <*> field  -- jobAttempts
    <*> field  -- jobQueueManagerId
    <*> field  -- jobHeaders

instance FromRow LogEntry where
  fromRow = LogEntry
    <$> field  -- logCreated
    <*> field  -- logJobId
    <*> field  -- logStatus
    <*> field  -- logPriority
    <*> field  -- logEntrypoint
    <*> field  -- logTraceback
    <*> field  -- logAggregated

instance FromRow QueueStatistics where
  fromRow = QueueStatistics
    <$> field  -- statsCount
    <*> field  -- statsEntrypoint
    <*> field  -- statsPriority
    <*> field  -- statsStatus

instance FromRow LogStatistics where
  fromRow = LogStatistics
    <$> field  -- logStatsCount
    <*> field  -- logStatsCreated
    <*> field  -- logStatsEntrypoint
    <*> field  -- logStatsPriority
    <*> field  -- logStatsStatus

instance FromRow Schedule where
  fromRow = Schedule
    <$> field  -- scheduleId
    <*> field  -- scheduleExpression
    <*> field  -- scheduleEntrypoint
    <*> field  -- scheduleHeartbeat
    <*> field  -- scheduleCreated
    <*> field  -- scheduleUpdated
    <*> field  -- scheduleNextRun
    <*> field  -- scheduleLastRun
    <*> field  -- scheduleStatus

-- ============================================================================
-- Enqueue operations
-- ============================================================================

-- | Enqueue a single job
enqueueSingle
  :: Connection
  -> DBSettings
  -> Entrypoint
  -> Maybe ByteString
  -> Int
  -> Maybe NominalDiffTime
  -> Maybe Text
  -> Maybe Value
  -> IO [JobId]
enqueueSingle conn settings entrypoint payload priority executeAfter dedupeKey headers = do
  enqueueMultiple conn settings [entrypoint] (maybeToList payload) [priority] 
    (maybeToList executeAfter) 
    (maybeToList dedupeKey)
    (maybeToList headers)
  where
    maybeToList Nothing = []
    maybeToList (Just x) = [Just x]

-- | Enqueue multiple jobs
enqueueMultiple
  :: Connection
  -> DBSettings
  -> [Entrypoint]
  -> [Maybe ByteString]
  -> [Int]
  -> [Maybe NominalDiffTime]
  -> [Maybe Text]
  -> [Maybe Value]
  -> IO [JobId]
enqueueMultiple conn settings entrypoints payloads priorities executeAfters dedupeKeys headersList = do
  let q = T.unlines
        [ "WITH inserted AS ("
        , "    INSERT INTO " <> queueTable settings
        , "    (priority, entrypoint, payload, execute_after, dedupe_key, headers, status)"
        , "    VALUES ("
        , "        UNNEST($1::int[]),"
        , "        UNNEST($2::text[]),"
        , "        UNNEST($3::bytea[]),"
        , "        UNNEST($4::interval[]) + NOW(),"
        , "        UNNEST($5::text[]),"
        , "        UNNEST($6::jsonb[]),"
        , "        'queued'"
        , "    )"
        , "    RETURNING id, entrypoint, status, priority"
        , ")"
        , "INSERT INTO " <> queueTableLog settings
        , "(job_id, status, entrypoint, priority)"
        , "SELECT id, 'queued', entrypoint, priority"
        , "FROM inserted"
        , "RETURNING job_id AS id"
        ]
  result <- query conn (textToQuery q)
    ( priorities
    , map (\(Entrypoint e) -> e) entrypoints
    , payloads
    , executeAfters
    , dedupeKeys
    , headersList
    ) :: IO [Only JobId]
  return $ map fromOnly result

-- ============================================================================
-- Dequeue operations
-- ============================================================================

-- | Dequeue jobs respecting concurrency limits
dequeue
  :: Connection
  -> DBSettings
  -> Int  -- batch size
  -> [EntrypointExecutionParameter]  -- per-entrypoint parameters
  -> UUID  -- queue manager id
  -> Maybe Int  -- global concurrency limit
  -> Int  -- heartbeat timeout in seconds
  -> IO [Job]
dequeue conn settings batchSize params queueMgrId globalLimit heartbeatTimeoutSecs = do
  let entrypoints = map paramEntrypoint params
      concurrencyLimits = map paramConcurrencyLimit params
      
      q = T.unlines
        [ "WITH"
        , "params AS ("
        , "    SELECT"
        , "        UNNEST($2::text[])   AS entrypoint,"
        , "        UNNEST($3::bigint[]) AS concurrency_limit"
        , "),"
        , "picked AS ("
        , "    SELECT entrypoint, COUNT(*) AS total"
        , "    FROM " <> queueTable settings
        , "    WHERE queue_manager_id IS NOT NULL"
        , "      AND entrypoint = ANY($2::text[])"
        , "    GROUP BY entrypoint"
        , "),"
        , "worker_load AS ("
        , "    SELECT COUNT(*) AS total"
        , "    FROM " <> queueTable settings
        , "    WHERE queue_manager_id = $4"
        , "      AND entrypoint = ANY($2::text[])"
        , "),"
        , "available AS ("
        , "    SELECT p.entrypoint"
        , "    FROM params p"
        , "    LEFT JOIN picked pk ON pk.entrypoint = p.entrypoint"
        , "    WHERE p.concurrency_limit <= 0"
        , "       OR COALESCE(pk.total, 0) < p.concurrency_limit"
        , "),"
        , "next_queued_src AS ("
        , "    SELECT q.id, q.priority"
        , "    FROM available a"
        , "    CROSS JOIN LATERAL ("
        , "        SELECT q2.id, q2.priority"
        , "        FROM " <> queueTable settings <> " q2"
        , "        WHERE q2.entrypoint = a.entrypoint"
        , "          AND q2.status = 'queued'"
        , "          AND q2.execute_after < NOW()"
        , "        ORDER BY q2.priority DESC, q2.id ASC"
        , "        LIMIT $1"
        , "        FOR UPDATE SKIP LOCKED"
        , "    ) q"
        , "    WHERE ($5::BIGINT IS NULL"
        , "           OR (SELECT total FROM worker_load) < $5)"
        , "    ORDER BY q.priority DESC, q.id ASC"
        , "    LIMIT $1"
        , "),"
        , "next_queued AS ("
        , "    SELECT id FROM next_queued_src"
        , "),"
        , "next_stale AS ("
        , "    SELECT q.id"
        , "    FROM " <> queueTable settings <> " q"
        , "    JOIN params p ON p.entrypoint = q.entrypoint"
        , "    WHERE q.status = 'picked'"
        , "      AND q.heartbeat < NOW() - INTERVAL '" <> T.pack (show heartbeatTimeoutSecs) <> " seconds'"
        , "      AND q.execute_after < NOW()"
        , "      AND ($5::BIGINT IS NULL"
        , "           OR (SELECT total FROM worker_load) < $5)"
        , "    ORDER BY q.priority DESC, q.id ASC"
        , "    FOR UPDATE SKIP LOCKED"
        , "    LIMIT $1"
        , "),"
        , "eligible AS ("
        , "    SELECT id FROM ("
        , "        SELECT id, 0 AS src FROM next_queued"
        , "        UNION ALL"
        , "        SELECT id, 1 AS src FROM next_stale"
        , "    ) combined"
        , "    ORDER BY src, id"
        , "    LIMIT $1"
        , "),"
        , "claimed AS ("
        , "    UPDATE " <> queueTable settings
        , "    SET status = 'picked',"
        , "        updated   = NOW(),"
        , "        heartbeat = NOW(),"
        , "        queue_manager_id = $4"
        , "    WHERE id IN (SELECT id FROM eligible)"
        , "    RETURNING *"
        , "),"
        , "log_pick AS ("
        , "    INSERT INTO " <> queueTableLog settings <> " (job_id, status, entrypoint, priority)"
        , "    SELECT id, status, entrypoint, priority FROM claimed"
        , ")"
        , "SELECT * FROM claimed ORDER BY priority DESC, id ASC"
        ]
  query conn (textToQuery q)
    ( batchSize
    , map (\(Entrypoint e) -> e) entrypoints
    , concurrencyLimits
    , queueMgrId
    , globalLimit
    )

-- ============================================================================
-- Log operations
-- ============================================================================

-- | Log job completions and status changes
logJobs
  :: Connection
  -> DBSettings
  -> [(JobId, JobStatus, Maybe Value)]  -- (job_id, status, traceback)
  -> IO ()
logJobs conn settings jobStatuses = do
  let jobIds = map (\(JobId id, _, _) -> id) jobStatuses
      statuses = map (\(_, s, _) -> jobStatusToText s) jobStatuses
      tracebacks = map (\(_, _, tb) -> tb) jobStatuses
      
      q = T.unlines
        [ "WITH job_status AS ("
        , "    SELECT"
        , "        UNNEST($1::integer[])   AS id,"
        , "        UNNEST($2::" <> queueStatusType settings <> "[]) AS status,"
        , "        UNNEST($3::JSONB[])     AS traceback"
        , "), deleted AS ("
        , "    DELETE FROM " <> queueTable settings
        , "    WHERE id = ANY(SELECT js.id FROM job_status js WHERE js.status != 'failed')"
        , "    RETURNING id, entrypoint, priority"
        , "), held AS ("
        , "    UPDATE " <> queueTable settings
        , "    SET status = 'failed', updated = NOW(), queue_manager_id = NULL"
        , "    WHERE id = ANY(SELECT js.id FROM job_status js WHERE js.status = 'failed')"
        , "    RETURNING id, entrypoint, priority"
        , "), all_resolved AS ("
        , "    SELECT id, entrypoint, priority FROM deleted"
        , "    UNION ALL"
        , "    SELECT id, entrypoint, priority FROM held"
        , "), merged AS ("
        , "    SELECT"
        , "        job_status.id           AS id,"
        , "        job_status.status       AS status,"
        , "        job_status.traceback    AS traceback,"
        , "        all_resolved.entrypoint AS entrypoint,"
        , "        all_resolved.priority   AS priority"
        , "    FROM job_status"
        , "    INNER JOIN all_resolved"
        , "        ON all_resolved.id = job_status.id"
        , ")"
        , "INSERT INTO " <> queueTableLog settings <> " ("
        , "    job_id,"
        , "    status,"
        , "    entrypoint,"
        , "    priority,"
        , "    traceback"
        , ")"
        , "SELECT id, status, entrypoint, priority, traceback FROM merged"
        ]
  _ <- execute conn (textToQuery q) (jobIds, statuses, tracebacks)
  return ()

-- ============================================================================
-- Queue status operations
-- ============================================================================

-- | Get queue size statistics
queueSize :: Connection -> DBSettings -> IO [QueueStatistics]
queueSize conn settings = do
  let q = T.unlines
        [ "SELECT"
        , "    count(*) AS count,"
        , "    priority,"
        , "    entrypoint,"
        , "    status"
        , "FROM " <> queueTable settings
        , "GROUP BY entrypoint, priority, status"
        , "ORDER BY count, entrypoint, priority, status"
        ]
  query_ conn (textToQuery q)

-- | Get queued work count for specific entrypoints
queuedWork :: Connection -> DBSettings -> [Entrypoint] -> IO Int
queuedWork conn settings entrypoints = do
  let eps = map (\(Entrypoint e) -> e) entrypoints
      q = T.unlines
        [ "SELECT COUNT(*)"
        , "FROM " <> queueTable settings
        , "WHERE entrypoint = ANY($1::text[])"
        , "  AND status = 'queued'"
        ]
  result <- query conn (textToQuery q) (Only eps) :: IO [Only Int]
  case result of
    [(Only count)] -> return count
    _ -> return 0

-- ============================================================================
-- Retry operations
-- ============================================================================

-- | Retry a failed job
retryJob
  :: Connection
  -> DBSettings
  -> Job
  -> NominalDiffTime  -- delay
  -> Maybe Value  -- traceback
  -> IO ()
retryJob conn settings job delay _traceback = do
  let newExecuteAfter = addUTCTime delay (jobExecuteAfter job)
      newAttempts = jobAttempts job + 1
      q = T.unlines
        [ "UPDATE " <> queueTable settings
        , "SET status = 'queued',"
        , "    execute_after = $1,"
        , "    attempts = $2,"
        , "    queue_manager_id = NULL,"
        , "    updated = NOW()"
        , "WHERE id = $3"
        ]
  _ <- execute conn (textToQuery q) (newExecuteAfter, newAttempts, jobId job)
  return ()

-- | Requeue multiple jobs
requeueJobs :: Connection -> DBSettings -> [JobId] -> IO ()
requeueJobs conn settings jobIds = do
  let q = T.unlines
        [ "UPDATE " <> queueTable settings
        , "SET status = 'queued',"
        , "    queue_manager_id = NULL,"
        , "    updated = NOW()"
        , "WHERE id = ANY($1::integer[])"
        ]
  _ <- execute conn (textToQuery q) (Only jobIds)
  return ()

-- ============================================================================
-- Job status operations
-- ============================================================================

-- | Mark jobs as cancelled
markJobAsCancelled :: Connection -> DBSettings -> [JobId] -> IO ()
markJobAsCancelled conn settings jobIds = do
  let q = T.unlines
        [ "UPDATE " <> queueTable settings
        , "SET status = 'canceled', updated = NOW()"
        , "WHERE id = ANY($1::integer[])"
        ]
  _ <- execute conn (textToQuery q) (Only jobIds)
  return ()

-- | Update heartbeat for active jobs
updateHeartbeat :: Connection -> DBSettings -> [JobId] -> IO ()
updateHeartbeat conn settings jobIds = do
  let q = T.unlines
        [ "UPDATE " <> queueTable settings
        , "SET heartbeat = NOW()"
        , "WHERE id = ANY($1::integer[]) AND status = 'picked'"
        ]
  _ <- execute conn (textToQuery q) (Only jobIds)
  return ()

-- | Get job status by IDs
jobStatus :: Connection -> DBSettings -> [JobId] -> IO [(JobId, JobStatus)]
jobStatus conn settings jobIds = do
  let q = T.unlines
        [ "SELECT id, status"
        , "FROM " <> queueTable settings
        , "WHERE id = ANY($1::integer[])"
        ]
  query conn (textToQuery q) (Only jobIds) :: IO [(JobId, JobStatus)]

-- ============================================================================
-- Cleanup operations
-- ============================================================================

-- | Clear entire queue or by entrypoint
clearQueue :: Connection -> DBSettings -> Maybe [Entrypoint] -> IO ()
clearQueue conn settings mbEntrypoints = do
  case mbEntrypoints of
    Nothing -> do
      let q = "DELETE FROM " <> queueTable settings
      _ <- execute_ conn (textToQuery q)
      return ()
    Just entrypoints -> do
      let eps = map (\(Entrypoint e) -> e) entrypoints
          q = T.unlines
            [ "DELETE FROM " <> queueTable settings
            , "WHERE entrypoint = ANY($1::text[])"
            ]
      _ <- execute conn (textToQuery q) (Only eps)
      return ()

-- | List failed jobs
listFailedJobs :: Connection -> DBSettings -> Int -> IO [Job]
listFailedJobs conn settings limit = do
  let q = T.unlines
        [ "SELECT *"
        , "FROM " <> queueTable settings
        , "WHERE status = 'failed'"
        , "ORDER BY updated DESC"
        , "LIMIT " <> T.pack (show limit)
        ]
  query_ conn (textToQuery q)
