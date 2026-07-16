{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Module      : Main
Description : Telemetry benchmarking harness for PGQueuer hasql backend.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Benchmarks the hasql engine against specific workloads, automatically
capturing WAL write-amplification, HOT update ratios, and dead-tuple
accumulation from a local PostgreSQL instance.
-}
module Main (main) where

import Control.Monad (forM_, replicateM_, unless, void)
import Data.ByteString (ByteString)
import Data.IORef
import Data.Int (Int64)
import qualified Data.Text as T
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.UUID.V4 (nextRandom)
import qualified Database.PostgreSQL.Simple as PG
import qualified Hasql.Pool as Pool
import qualified Hasql.Session as Session
import PGQueuer.Backend.Hasql
import PGQueuer.Backend.Hasql.Statements (tableStatsStmt, walLsnStmt)
import PGQueuer.Core.Monad
import PGQueuer.Schema (install, uninstall)
import PGQueuer.Settings (defaultDBSettings)
import PGQueuer.Types
import PGQueuer.Worker.Buffer
import System.IO (hFlush, stdout)

-- ============================================================================
-- Telemetry snapshot
-- ============================================================================

-- | Snapshot of database telemetry metrics.
data TelemetrySnapshot = TelemetrySnapshot
    { tsWalLsn :: Int64
    -- ^ WAL position in bytes
    , tsQueueUpdates :: Int64
    -- ^ n_tup_upd for pgqueuer table
    , tsQueueHotUpdates :: Int64
    -- ^ n_tup_hot_upd for pgqueuer table
    , tsQueueDeadTuples :: Int64
    -- ^ n_dead_tup for pgqueuer table
    , tsLogUpdates :: Int64
    -- ^ n_tup_upd for pgqueuer_log table
    , tsLogHotUpdates :: Int64
    -- ^ n_tup_hot_upd for pgqueuer_log table
    , tsLogDeadTuples :: Int64
    -- ^ n_dead_tup for pgqueuer_log table
    }
    deriving (Show)

-- | Connection string for benchmarks.
benchConnStr :: ByteString
benchConnStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"

-- | Capture a telemetry snapshot from the database.
captureSnapshot :: HasqlDbEnv -> IO TelemetrySnapshot
captureSnapshot dbEnv = do
    walLsn <- runPoolSession' dbEnv $ Session.statement () walLsnStmt
    (qUpd, qHot, qDead) <- runPoolSession' dbEnv $ Session.statement "pgqueuer" tableStatsStmt
    (lUpd, lHot, lDead) <- runPoolSession' dbEnv $ Session.statement "pgqueuer_log" tableStatsStmt
    return
        TelemetrySnapshot
            { tsWalLsn = walLsn
            , tsQueueUpdates = qUpd
            , tsQueueHotUpdates = qHot
            , tsQueueDeadTuples = qDead
            , tsLogUpdates = lUpd
            , tsLogHotUpdates = lHot
            , tsLogDeadTuples = lDead
            }

-- | Run a session, throwing on error.
runPoolSession' :: HasqlDbEnv -> Session.Session a -> IO a
runPoolSession' dbEnv session = do
    result <- Pool.use (hdbPool dbEnv) session
    case result of
        Left err -> error $ "Benchmark session error: " <> show err
        Right val -> return val

-- ============================================================================
-- Benchmark workload profiles
-- ============================================================================

-- | Run the ungrouped single-mode workload: batch_size=1, 1000 jobs.
benchUngroupedSingle :: HasqlDbEnv -> IO (IO ())
benchUngroupedSingle dbEnv = do
    let ep = Entrypoint "bench_single"
        params = [EntrypointExecutionParameter ep 0 5 (Exponential 5 60) FullJitter]
        jobCount = 1000 :: Int

    -- Enqueue all jobs
    forM_ [1 .. jobCount] $ \_ ->
        runHasqlDb dbEnv $
            enqueue ep Nothing 0 Nothing Nothing Nothing

    -- Dequeue one at a time
    qmId <- nextRandom
    dequeued <- newIORef (0 :: Int)
    let loop = do
            jobs <- runHasqlDb dbEnv $ dequeue 1 params qmId Nothing 30
            unless (null jobs) $ do
                -- Simulate heartbeats to prove HOT updates
                replicateM_ 2 $
                    runHasqlDb dbEnv $
                        updateHeartbeat [jobId j | j <- jobs]

                -- Log as successful
                runHasqlDb dbEnv $
                    logJobs [(jobId j, Successful, Nothing) | j <- jobs]
                modifyIORef' dequeued (+ length jobs)
                loop
    return loop

-- | Run the ungrouped batched-mode workload: batch_size=10, 1000 jobs.
benchUngroupedBatched :: HasqlDbEnv -> IO (IO ())
benchUngroupedBatched dbEnv = do
    let ep = Entrypoint "bench_batch"
        params = [EntrypointExecutionParameter ep 0 5 (Exponential 5 60) FullJitter]
        jobCount = 1000 :: Int

    -- Enqueue all jobs
    forM_ [1 .. jobCount] $ \_ ->
        runHasqlDb dbEnv $
            enqueue ep Nothing 0 Nothing Nothing Nothing

    -- Dequeue in batches of 10
    qmId <- nextRandom
    let loop = do
            jobs <- runHasqlDb dbEnv $ dequeue 10 params qmId Nothing 30
            unless (null jobs) $ do
                -- Simulate heartbeat
                replicateM_ 2 $
                    runHasqlDb dbEnv $
                        updateHeartbeat [jobId j | j <- jobs]

                runHasqlDb dbEnv $
                    logJobs [(jobId j, Successful, Nothing) | j <- jobs]
                loop
    return loop

-- | Run the high-cardinality grouped workload: 2500 distinct groups.
benchHighCardinalityGrouped :: HasqlDbEnv -> IO (IO ())
benchHighCardinalityGrouped dbEnv = do
    let jobCount = 2500 :: Int
        batchSize = 10

    -- Enqueue jobs across distinct entrypoints
    forM_ [1 .. jobCount] $ \(i :: Int) -> do
        let ep = Entrypoint (T.pack $ "group_" <> show i)
        void $
            runHasqlDb dbEnv $
                enqueue ep Nothing 0 Nothing Nothing Nothing

    -- Build params for all groups
    let allParams =
            [ EntrypointExecutionParameter (Entrypoint (T.pack $ "group_" <> show i)) 0 5 (Exponential 5 60) FullJitter
            | i <- [1 .. jobCount]
            ]

    -- Dequeue in batches
    qmId <- nextRandom
    let loop = do
            jobs <- runHasqlDb dbEnv $ dequeue batchSize allParams qmId Nothing 30
            unless (null jobs) $ do
                -- Simulate heartbeat
                replicateM_ 2 $
                    runHasqlDb dbEnv $
                        updateHeartbeat [jobId j | j <- jobs]

                runHasqlDb dbEnv $
                    logJobs [(jobId j, Successful, Nothing) | j <- jobs]
                loop
    return loop

{- | Run the buffered batched-mode workload: batch_size=10, 1000 jobs.
Uses STM buffers for both job status logging and heartbeats.
-}
benchBufferedBatched :: HasqlDbEnv -> IO (IO ())
benchBufferedBatched dbEnv = do
    let ep = Entrypoint "bench_buffered"
        params = [EntrypointExecutionParameter ep 0 5 (Exponential 5 60) FullJitter]
        jobCount = 1000 :: Int

    -- Enqueue all jobs
    forM_ [1 .. jobCount] $ \_ ->
        runHasqlDb dbEnv $
            enqueue ep Nothing 0 Nothing Nothing Nothing

    -- Build the dequeue + buffered-ACK loop
    qmId <- nextRandom
    let loop = do
            let logSink items = runHasqlDb dbEnv $ logJobs items
                hbSink ids = runHasqlDb dbEnv $ updateHeartbeat ids
            withBuffer defaultBufferConfig logSink $ \logBuf ->
                withBuffer defaultBufferConfig hbSink $ \hbBuf -> do
                    let innerLoop = do
                            jobs <- runHasqlDb dbEnv $ dequeue 10 params qmId Nothing 30
                            unless (null jobs) $ do
                                -- Buffer heartbeats
                                mapM_ (add hbBuf . jobId) jobs
                                mapM_ (add hbBuf . jobId) jobs
                                -- Buffer ACKs
                                mapM_ (\j -> add logBuf (jobId j, Successful, Nothing)) jobs
                                innerLoop
                    innerLoop
    return loop

-- ============================================================================
-- Telemetry reporting
-- ============================================================================

-- | Print telemetry report for a workload.
printTelemetryReport :: String -> Int -> TelemetrySnapshot -> TelemetrySnapshot -> Double -> IO ()
printTelemetryReport name jobCount pre post elapsedSecs = do
    let walDelta = tsWalLsn post - tsWalLsn pre
        walPerJob = if jobCount > 0 then walDelta `div` fromIntegral jobCount else 0
        queueUpdDelta = tsQueueUpdates post - tsQueueUpdates pre
        queueHotDelta = tsQueueHotUpdates post - tsQueueHotUpdates pre
        hotPct :: Double
        hotPct =
            if queueUpdDelta > 0
                then fromIntegral queueHotDelta / fromIntegral queueUpdDelta * 100.0
                else 0.0
        updPerJob :: Double
        updPerJob =
            if jobCount > 0
                then fromIntegral queueUpdDelta / fromIntegral jobCount
                else 0.0
        deadDelta = tsQueueDeadTuples post - tsQueueDeadTuples pre
        throughput :: Double
        throughput =
            if elapsedSecs > 0
                then fromIntegral jobCount / elapsedSecs
                else 0.0

    putStrLn $ "  " <> name
    putStrLn $ "    " <> show (round throughput :: Int) <> " jobs/sec"
    putStrLn $
        "    write-amp: "
            <> show walPerJob
            <> " B WAL/job | queue HOT "
            <> showPct hotPct
            <> " ("
            <> showFrac updPerJob
            <> " upd/job)"
    putStrLn $
        "    autovacuum: queue "
            <> show deadDelta
            <> " dead ("
            <> show deadDelta
            <> "/"
            <> show jobCount
            <> " jobs)"
    hFlush stdout

showPct :: Double -> String
showPct d = show (round d :: Int) <> "%"

showFrac :: Double -> String
showFrac d =
    let s = show d
     in take 4 s

-- ============================================================================
-- Main
-- ============================================================================

main :: IO ()
main = do
    putStrLn "============================================"
    putStrLn "PGQueuer Telemetry Benchmark (hasql backend)"
    putStrLn "============================================"
    putStrLn ""

    -- Set up schema via postgresql-simple
    schemaConn <- PG.connectPostgreSQL benchConnStr
    let settings = defaultDBSettings

    -- Analyze tables to update pg_stat counters
    let resetSchema = do
            uninstall schemaConn settings
            install schemaConn settings
            void $ PG.execute_ schemaConn "ANALYZE pgqueuer"
            void $ PG.execute_ schemaConn "ANALYZE pgqueuer_log"

    -- Create hasql pool
    dbEnv <- mkHasqlDbEnv benchConnStr settings

    putStrLn "Worker Throughput (hasql) - 4 pool connections"
    putStrLn ""

    let quietBenchDb = do
            void $ PG.execute_ schemaConn "ALTER TABLE pgqueuer SET (autovacuum_enabled = false);"
            void $ PG.execute_ schemaConn "VACUUM ANALYZE pgqueuer;"
            void $ PG.execute_ schemaConn "CHECKPOINT;"

    -- Profile 1: Ungrouped Single
    do
        resetSchema
        runBench <- benchUngroupedSingle dbEnv
        quietBenchDb
        preSnap <- captureSnapshot dbEnv
        startTime <- getCurrentTime
        runBench
        endTime <- getCurrentTime
        postSnap <- captureSnapshot dbEnv
        let elapsed = realToFrac (diffUTCTime endTime startTime) :: Double
        printTelemetryReport "ungrouped single (batch_size=1)" 1000 preSnap postSnap elapsed

    putStrLn ""

    -- Profile 2: Ungrouped Batched
    do
        resetSchema
        runBench <- benchUngroupedBatched dbEnv
        quietBenchDb
        preSnap <- captureSnapshot dbEnv
        startTime <- getCurrentTime
        runBench
        endTime <- getCurrentTime
        postSnap <- captureSnapshot dbEnv
        let elapsed = realToFrac (diffUTCTime endTime startTime) :: Double
        printTelemetryReport "ungrouped batched (batch_size=10)" 1000 preSnap postSnap elapsed

    putStrLn ""

    -- Profile 3: High-Cardinality Grouped
    do
        resetSchema
        runBench <- benchHighCardinalityGrouped dbEnv
        quietBenchDb
        preSnap <- captureSnapshot dbEnv
        startTime <- getCurrentTime
        runBench
        endTime <- getCurrentTime
        postSnap <- captureSnapshot dbEnv
        let elapsed = realToFrac (diffUTCTime endTime startTime) :: Double
        printTelemetryReport "high-cardinality grouped (2.5k groups)" 2500 preSnap postSnap elapsed

    putStrLn ""

    -- Profile 4: Buffered Batched
    do
        resetSchema
        runBench <- benchBufferedBatched dbEnv
        quietBenchDb
        preSnap <- captureSnapshot dbEnv
        startTime <- getCurrentTime
        runBench
        endTime <- getCurrentTime
        postSnap <- captureSnapshot dbEnv
        let elapsed = realToFrac (diffUTCTime endTime startTime) :: Double
        printTelemetryReport "buffered batched (batch_size=10, STM)" 1000 preSnap postSnap elapsed

    putStrLn ""
    putStrLn "============================================"
    putStrLn "Benchmark complete."
    putStrLn "============================================"

    -- Cleanup
    releaseHasqlDbEnv dbEnv
    PG.close schemaConn
