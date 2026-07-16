{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : PGQueuer.Backend.Hasql.Statements
Description : Statically compiled Hasql statements for queue operations.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Defines all SQL operations as @Hasql.Statement.Statement@ objects with
binary encoders and decoders. Every statement is marked as prepared
(@True@) to force PostgreSQL to skip query planning in hot paths.

The @dequeue@ statement is especially critical: it uses
@FOR UPDATE SKIP LOCKED@ and MUST be prepared to avoid repeated
planning inside the inner worker loop.
-}
module PGQueuer.Backend.Hasql.Statements (
    enqueueStmt,
    dequeueStmt,
    logJobsStmt,
    updateHeartbeatStmt,
    retryJobsStmt,
    walLsnStmt,
    tableStatsStmt,
    insertScheduleStmt,
    fetchSchedulesStmt,
    setScheduleQueuedStmt,
    getEarliestNextRunStmt,
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import Data.Functor.Contravariant ((>$<))
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (UTCTime)
import Data.UUID (UUID)
import Data.Vector (Vector)
import qualified Hasql.Decoders as D
import qualified Hasql.Encoders as E
import Hasql.Statement (Statement (..))

import PGQueuer.Settings (DBSettings, queueStatusType, queueTable, queueTableLog)
import PGQueuer.Types

-- ============================================================================
-- Decoders for domain types
-- ============================================================================

{- | Decode a 'JobStatus' from a text column.
Uses 'D.custom' which has signature @(Bool -> ByteString -> Either Text a)@.
The Bool indicates text vs binary format; ByteString is the raw non-null value.
-}
jobStatusDecoder :: D.Value JobStatus
jobStatusDecoder = D.custom $ \_isBinary bs ->
    let t = TE.decodeUtf8 bs
     in case textToJobStatus_ t of
            Just s -> Right s
            Nothing -> Left ("Unknown job status: " <> t)

textToJobStatus_ :: Text -> Maybe JobStatus
textToJobStatus_ "queued" = Just Queued
textToJobStatus_ "picked" = Just Picked
textToJobStatus_ "successful" = Just Successful
textToJobStatus_ "failed" = Just Failed
textToJobStatus_ "exception" = Just Exception
textToJobStatus_ "canceled" = Just Canceled
textToJobStatus_ "deleted" = Just Deleted
textToJobStatus_ _ = Nothing

-- | Decode a 'Job' from a row.
jobRow :: D.Row Job
jobRow =
    (Job . JobId . fromIntegral <$> D.column (D.nonNullable D.int4)) -- id
        <*> (fromIntegral <$> D.column (D.nonNullable D.int4)) -- priority
        <*> D.column (D.nonNullable D.timestamptz) -- created
        <*> D.column (D.nonNullable D.timestamptz) -- updated
        <*> D.column (D.nonNullable D.timestamptz) -- heartbeat
        <*> D.column (D.nonNullable D.timestamptz) -- execute_after
        <*> D.column (D.nonNullable jobStatusDecoder) -- status
        <*> (Entrypoint <$> D.column (D.nonNullable D.text)) -- entrypoint
        <*> (fmap BL.fromStrict <$> D.column (D.nullable D.bytea)) -- payload
        <*> (fromIntegral <$> D.column (D.nonNullable D.int4)) -- attempts
        <*> D.column (D.nullable D.uuid) -- queue_manager_id
        <*> D.column (D.nullable D.jsonb) -- headers

-- ============================================================================
-- Enqueue Statement
-- ============================================================================

{- | Enqueue multiple jobs in a single round-trip.

Parameters: (priorities, entrypoints, payloads, intervals_text, dedupe_keys, headers_json)

Uses raw SQL with @UNNEST@ arrays to batch-insert jobs.
-}
enqueueStmt :: DBSettings -> Statement (Vector Int32, Vector Text, Vector (Maybe ByteString), Vector (Maybe Text), Vector (Maybe Text), Vector (Maybe ByteString)) (Vector Int32)
enqueueStmt settings =
    Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "WITH inserted AS ("
                , "    INSERT INTO " <> queueTable settings
                , "    (priority, entrypoint, payload, execute_after, dedupe_key, headers, status)"
                , "    SELECT"
                , "        p, e, pay, COALESCE(NOW() + ea::interval, NOW()), d, h, 'queued'"
                , "    FROM UNNEST("
                , "        $1::int[],"
                , "        $2::text[],"
                , "        $3::bytea[],"
                , "        $4::text[],"
                , "        $5::text[],"
                , "        $6::jsonb[]"
                , "    ) AS t(p, e, pay, ea, d, h)"
                , "    RETURNING id, entrypoint, status, priority"
                , ")"
                , "INSERT INTO " <> queueTableLog settings
                , "(job_id, status, entrypoint, priority)"
                , "SELECT id, 'queued', entrypoint, priority"
                , "FROM inserted"
                , "RETURNING job_id::int AS id"
                ]
    encoder =
        ((\(a, _, _, _, _, _) -> a) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.int4))))
            <> ((\(_, b, _, _, _, _) -> b) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.text))))
            <> ((\(_, _, c, _, _, _) -> c) >$< E.param (E.nonNullable (E.foldableArray (E.nullable E.bytea))))
            <> ((\(_, _, _, d, _, _) -> d) >$< E.param (E.nonNullable (E.foldableArray (E.nullable E.text))))
            <> ((\(_, _, _, _, e, _) -> e) >$< E.param (E.nonNullable (E.foldableArray (E.nullable E.text))))
            <> ((\(_, _, _, _, _, f) -> f) >$< E.param (E.nonNullable (E.foldableArray (E.nullable E.jsonbBytes))))
    decoder = D.rowVector (D.column (D.nonNullable D.int4))

-- ============================================================================
-- Dequeue Statement
-- ============================================================================

{- | Dequeue jobs using @FOR UPDATE SKIP LOCKED@.

This statement is marked as prepared (@True@) to force PostgreSQL to
cache the query plan. This is critical for CPU efficiency in the inner
worker loop.

Parameters: (batch_size, queue_manager_id, global_limit, entrypoints, concurrency_limits, heartbeat_timeout_secs)
-}
dequeueStmt :: DBSettings -> Int -> Statement (Int32, UUID, Maybe Int64, Vector Text, Vector Int64) (Vector Job)
dequeueStmt settings heartbeatTimeoutSecs =
    Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "WITH"
                , "runtime AS ("
                , "    SELECT"
                , "        $1::int    AS batch_size,"
                , "        $2::uuid   AS queue_manager_id,"
                , "        $3::bigint AS global_limit"
                , "),"
                , "params AS ("
                , "    SELECT"
                , "        UNNEST($4::text[])   AS entrypoint,"
                , "        UNNEST($5::bigint[]) AS concurrency_limit"
                , "),"
                , "picked AS ("
                , "    SELECT entrypoint, COUNT(*) AS total"
                , "    FROM " <> queueTable settings <> " q"
                , "    WHERE q.queue_manager_id IS NOT NULL"
                , "      AND EXISTS (SELECT 1 FROM params p WHERE p.entrypoint = q.entrypoint)"
                , "    GROUP BY q.entrypoint"
                , "),"
                , "worker_load AS ("
                , "    SELECT COUNT(*) AS total"
                , "    FROM " <> queueTable settings <> " q"
                , "    WHERE q.queue_manager_id = (SELECT queue_manager_id FROM runtime)"
                , "      AND EXISTS (SELECT 1 FROM params p WHERE p.entrypoint = q.entrypoint)"
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
                , "        LIMIT (SELECT batch_size FROM runtime)"
                , "        FOR UPDATE SKIP LOCKED"
                , "    ) q"
                , "    WHERE ((SELECT global_limit FROM runtime) IS NULL"
                , "           OR (SELECT total FROM worker_load) < (SELECT global_limit FROM runtime))"
                , "    ORDER BY q.priority DESC, q.id ASC"
                , "    LIMIT (SELECT batch_size FROM runtime)"
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
                , "      AND ((SELECT global_limit FROM runtime) IS NULL"
                , "           OR (SELECT total FROM worker_load) < (SELECT global_limit FROM runtime))"
                , "    ORDER BY q.priority DESC, q.id ASC"
                , "    FOR UPDATE SKIP LOCKED"
                , "    LIMIT (SELECT batch_size FROM runtime)"
                , "),"
                , "eligible AS ("
                , "    SELECT id FROM ("
                , "        SELECT id, 0 AS src FROM next_queued"
                , "        UNION ALL"
                , "        SELECT id, 1 AS src FROM next_stale"
                , "    ) combined"
                , "    ORDER BY src, id"
                , "    LIMIT (SELECT batch_size FROM runtime)"
                , "),"
                , "claimed AS ("
                , "    UPDATE " <> queueTable settings
                , "    SET status = 'picked',"
                , "        updated   = NOW(),"
                , "        heartbeat = NOW(),"
                , "        queue_manager_id = (SELECT queue_manager_id FROM runtime)"
                , "    WHERE id IN (SELECT id FROM eligible)"
                , "    RETURNING *"
                , "),"
                , "log_pick AS ("
                , "    INSERT INTO " <> queueTableLog settings <> " (job_id, status, entrypoint, priority)"
                , "    SELECT id, status, entrypoint, priority FROM claimed"
                , ")"
                , "SELECT id, priority, created, updated, heartbeat, execute_after, status::text AS status, entrypoint, payload, attempts, queue_manager_id, headers"
                , "FROM claimed"
                , "ORDER BY priority DESC, id ASC"
                ]
    encoder =
        ((\(a, _, _, _, _) -> a) >$< E.param (E.nonNullable E.int4))
            <> ((\(_, b, _, _, _) -> b) >$< E.param (E.nonNullable E.uuid))
            <> ((\(_, _, c, _, _) -> c) >$< E.param (E.nullable E.int8))
            <> ((\(_, _, _, d, _) -> d) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.text))))
            <> ((\(_, _, _, _, e) -> e) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.int8))))
    decoder = D.rowVector jobRow

-- | Get earliest next_run.
getEarliestNextRunStmt :: Statement () (Maybe UTCTime)
getEarliestNextRunStmt = Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "SELECT MIN(next_run)"
                , "FROM pgqueuer_schedules"
                , "WHERE status = 'queued'"
                ]
    encoder = E.noParams
    decoder = D.singleRow (D.column (D.nullable D.timestamptz))

-- ============================================================================
-- LogJobs Statement
-- ============================================================================

{- | Log job status transitions.

Parameters: (job_ids, statuses, tracebacks)
-}
logJobsStmt :: DBSettings -> Statement (Vector Int32, Vector Text, Vector (Maybe ByteString)) ()
logJobsStmt settings =
    Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "WITH job_status AS ("
                , "    SELECT"
                , "        UNNEST($1::integer[])   AS id,"
                , "        UNNEST($2::" <> queueStatusType settings <> "[]) AS status,"
                , "        UNNEST($3::jsonb[])     AS traceback"
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
    encoder =
        ((\(a, _, _) -> a) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.int4))))
            <> ((\(_, b, _) -> b) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.text))))
            <> ((\(_, _, c) -> c) >$< E.param (E.nonNullable (E.foldableArray (E.nullable E.jsonbBytes))))
    decoder = D.noResult

-- ============================================================================
-- UpdateHeartbeat Statement
-- ============================================================================

{- | Refresh heartbeat timestamps for active jobs.

Parameters: job_ids as int array
-}
updateHeartbeatStmt :: DBSettings -> Statement (Vector Int32) ()
updateHeartbeatStmt settings =
    Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "UPDATE " <> queueTable settings
                , "SET heartbeat = NOW()"
                , "WHERE id = ANY($1::integer[]) AND status = 'picked'"
                ]
    encoder = E.param (E.nonNullable (E.foldableArray (E.nonNullable E.int4)))
    decoder = D.noResult

-- ============================================================================
-- RetryJobs Statement
-- ============================================================================

{- | Retry failed jobs after a specified delay.

Parameters: (execute_afters, attempts, job_ids)
-}
retryJobsStmt :: DBSettings -> Statement (Vector UTCTime, Vector Int32, Vector Int32) ()
retryJobsStmt settings =
    Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "WITH updates AS ("
                , "    SELECT"
                , "        UNNEST($1::timestamptz[]) AS execute_after,"
                , "        UNNEST($2::integer[])     AS attempts,"
                , "        UNNEST($3::integer[])     AS id"
                , ")"
                , "UPDATE " <> queueTable settings <> " q"
                , "SET status = 'queued',"
                , "    execute_after = u.execute_after,"
                , "    attempts = u.attempts,"
                , "    queue_manager_id = NULL,"
                , "    updated = NOW()"
                , "FROM updates u"
                , "WHERE q.id = u.id"
                ]
    encoder =
        ((\(a, _, _) -> a) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.timestamptz))))
            <> ((\(_, b, _) -> b) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.int4))))
            <> ((\(_, _, c) -> c) >$< E.param (E.nonNullable (E.foldableArray (E.nonNullable E.int4))))
    decoder = D.noResult

-- ============================================================================
-- Telemetry Statements (for benchmarking)
-- ============================================================================

{- | Query current WAL LSN position as a bigint (bytes).

Returns the WAL position for computing write-amplification.
-}
walLsnStmt :: Statement () Int64
walLsnStmt =
    Statement sql encoder decoder True
  where
    sql = "SELECT pg_current_wal_lsn() - '0/0'::pg_lsn"
    encoder = E.noParams
    decoder = D.singleRow (D.column (D.nonNullable D.int8))

{- | Query pg_stat_user_tables metrics for a given table.

Returns: (n_tup_upd, n_tup_hot_upd, n_dead_tup)
-}
tableStatsStmt :: Statement Text (Int64, Int64, Int64)
tableStatsStmt =
    Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "SELECT"
                , "    COALESCE(n_tup_upd, 0)::bigint,"
                , "    COALESCE(n_tup_hot_upd, 0)::bigint,"
                , "    COALESCE(n_dead_tup, 0)::bigint"
                , "FROM pg_stat_user_tables"
                , "WHERE relname = $1::text"
                ]
    encoder = E.param (E.nonNullable E.text)
    decoder =
        D.singleRow $
            (,,)
                <$> D.column (D.nonNullable D.int8)
                <*> D.column (D.nonNullable D.int8)
                <*> D.column (D.nonNullable D.int8)

-- ============================================================================
-- Schedule Statements
-- ============================================================================

-- | Insert a new cron schedule.
insertScheduleStmt :: Statement (Text, Text) ()
insertScheduleStmt = Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "INSERT INTO pgqueuer_schedules (expression, entrypoint)"
                , "VALUES ($1, $2)"
                , "ON CONFLICT (expression, entrypoint) DO NOTHING"
                ]
    encoder =
        (fst >$< E.param (E.nonNullable E.text))
            <> (snd >$< E.param (E.nonNullable E.text))
    decoder = D.noResult

-- | Fetch due schedules.
fetchSchedulesStmt :: Statement () (Vector (Int32, Text, Text, UTCTime, UTCTime, UTCTime, UTCTime, Maybe UTCTime, Text))
fetchSchedulesStmt = Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "UPDATE pgqueuer_schedules"
                , "SET status = 'picked',"
                , "    updated = NOW(),"
                , "    heartbeat = NOW()"
                , "WHERE id IN ("
                , "    SELECT id"
                , "    FROM pgqueuer_schedules"
                , "    WHERE status = 'queued'"
                , "      AND next_run <= NOW()"
                , "    ORDER BY id ASC"
                , "    FOR UPDATE SKIP LOCKED"
                , ")"
                , "RETURNING id, expression, entrypoint, heartbeat, created, updated, next_run, last_run, status::text"
                ]
    encoder = E.noParams
    decoder =
        D.rowVector $
            (,,,,,,,,)
                <$> D.column (D.nonNullable D.int4)
                <*> D.column (D.nonNullable D.text)
                <*> D.column (D.nonNullable D.text)
                <*> D.column (D.nonNullable D.timestamptz)
                <*> D.column (D.nonNullable D.timestamptz)
                <*> D.column (D.nonNullable D.timestamptz)
                <*> D.column (D.nonNullable D.timestamptz)
                <*> D.column (D.nullable D.timestamptz)
                <*> D.column (D.nonNullable D.text)

-- | Reset schedule to queued and update next run.
setScheduleQueuedStmt :: Statement (UTCTime, Int32) ()
setScheduleQueuedStmt = Statement sql encoder decoder True
  where
    sql =
        TE.encodeUtf8 $
            T.unlines
                [ "UPDATE pgqueuer_schedules"
                , "SET status = 'queued',"
                , "    updated = NOW(),"
                , "    last_run = NOW(),"
                , "    next_run = $1"
                , "WHERE id = $2"
                ]
    encoder =
        (fst >$< E.param (E.nonNullable E.timestamptz))
            <> (snd >$< E.param (E.nonNullable E.int4))
    decoder = D.noResult
