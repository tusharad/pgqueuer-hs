{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Module      : PGQueuer.Backend.Hasql
Description : High-performance backend using hasql binary protocol.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Provides 'HasqlDb', a concrete monad transformer that implements
'MonadPGQueuer' using @hasql@'s binary protocol for zero-allocation
data transfers. All SQL operations are statically compiled as
@Hasql.Statement.Statement@ objects and executed through a
@hasql-pool@ connection pool.

This is the high-performance driver. It uses server-side prepared
statements (@True@) for all queries, enabling PostgreSQL to skip
the query planning phase in the inner worker loops.
-}
module PGQueuer.Backend.Hasql (
    HasqlDb (..),
    HasqlDbEnv (..),
    runHasqlDb,
    mkHasqlDbEnv,
    releaseHasqlDbEnv,
) where

import Control.Exception (SomeException, catch, throwIO)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader, ReaderT (..))
import qualified Data.Aeson as Aeson
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (NominalDiffTime)
import Data.UUID ()
import qualified Data.Vector as V
import qualified Hasql.Connection.Setting as HCS
import qualified Hasql.Connection.Setting.Connection as HCSC
import qualified Hasql.Pool as Pool
import qualified Hasql.Pool.Config as PoolConfig
import Hasql.Session (Session)
import qualified Hasql.Session as Session
import PGQueuer.Backend.Hasql.Statements
import PGQueuer.Core.Monad (MonadPGQueuer (..))
import PGQueuer.Settings (DBSettings)
import PGQueuer.Types

-- | Environment carrying the connection pool and database settings.
data HasqlDbEnv = HasqlDbEnv
    { hdbPool :: Pool.Pool
    -- ^ Connection pool
    , hdbSettings :: DBSettings
    -- ^ Table/schema naming settings
    }

{- | Create a 'HasqlDbEnv' with a connection pool.

The pool is created with 4 connections and a 10-second acquisition
timeout, which is appropriate for both testing and benchmarking.
-}
mkHasqlDbEnv :: ByteString -> DBSettings -> IO HasqlDbEnv
mkHasqlDbEnv connStr dbSettings = do
    let poolCfg =
            PoolConfig.settings
                [ PoolConfig.size 4
                , PoolConfig.acquisitionTimeout 10
                , PoolConfig.staticConnectionSettings
                    [HCS.connection (HCSC.string (TE.decodeUtf8 connStr))]
                ]
    pool <- Pool.acquire poolCfg
    return
        HasqlDbEnv
            { hdbPool = pool
            , hdbSettings = dbSettings
            }

-- | Release all connections in the pool.
releaseHasqlDbEnv :: HasqlDbEnv -> IO ()
releaseHasqlDbEnv = Pool.release . hdbPool

{- | A concrete monad transformer wrapping @ReaderT HasqlDbEnv IO@.

This is the high-performance 'MonadPGQueuer' implementation backed by
@hasql@'s binary protocol.
-}
newtype HasqlDb a = HasqlDb
    { unHasqlDb :: ReaderT HasqlDbEnv IO a
    }
    deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader HasqlDbEnv)

-- | Run a 'HasqlDb' computation with the given environment.
runHasqlDb :: HasqlDbEnv -> HasqlDb a -> IO a
runHasqlDb env (HasqlDb action) = runReaderT action env

-- | Run a session against the pool, throwing on error.
runPoolSession :: HasqlDbEnv -> Session a -> IO a
runPoolSession env session = do
    result <- Pool.use (hdbPool env) session
    case result of
        Left err -> throwIO (userError $ "HasqlDb session error: " <> show err)
        Right val -> return val

instance MonadPGQueuer HasqlDb where
    enqueue ep payload priority executeAfter dedupeKey headers parentId parentState status = do
        env <- HasqlDb (ReaderT return)
        let settings = hdbSettings env
        liftIO $ do
            let priorities = V.singleton (fromIntegral priority :: Int32)
                entrypoints = V.singleton (let Entrypoint e = ep in e)
                payloads = V.singleton (BL.toStrict <$> payload)
                intervals = V.singleton (fmap formatInterval executeAfter)
                dedupeKeys = V.singleton dedupeKey
                headersBs = V.singleton (BL.toStrict . Aeson.encode <$> headers)
                parentIds = V.singleton ((\(JobId i) -> fromIntegral i :: Int32) <$> parentId)
                parentStates = V.singleton (BL.toStrict . Aeson.encode <$> parentState)
                statuses = V.singleton (jobStatusToText status)
            result <-
                runPoolSession env $
                    Session.statement
                        (priorities, entrypoints, payloads, intervals, dedupeKeys, headersBs, parentIds, parentStates, statuses)
                        (enqueueStmt settings)
            return $ map (JobId . fromIntegral) (V.toList result)

    dequeue batchSize params queueMgrId globalLimit heartbeatTimeoutSecs = do
        env <- HasqlDb (ReaderT return)
        let settings = hdbSettings env
            entrypoints = V.fromList $ map (\p -> let Entrypoint e = paramEntrypoint p in e) params
            concurrencyLimits = V.fromList $ map (fromIntegral . paramConcurrencyLimit) params
            batchSize' = fromIntegral batchSize :: Int32
            globalLimit' = fromIntegral <$> globalLimit :: Maybe Int64
        liftIO $ do
            result <-
                runPoolSession env $
                    Session.statement
                        (batchSize', queueMgrId, globalLimit', entrypoints, concurrencyLimits)
                        (dequeueStmt settings heartbeatTimeoutSecs)
            return $ V.toList result

    logJobs jobStatuses = do
        env <- HasqlDb (ReaderT return)
        let settings = hdbSettings env
            jobIds = V.fromList $ map (\(JobId i, _, _) -> fromIntegral i :: Int32) jobStatuses
            statuses = V.fromList $ map (\(_, s, _) -> jobStatusToText s) jobStatuses
            tracebacks = V.fromList $ map (\(_, _, tb) -> BL.toStrict . Aeson.encode <$> tb) jobStatuses
        liftIO $ runPoolSession env $ Session.statement (jobIds, statuses, tracebacks) (logJobsStmt settings)

    updateHeartbeat jobIds = do
        env <- HasqlDb (ReaderT return)
        let settings = hdbSettings env
            ids = V.fromList $ map (\(JobId i) -> fromIntegral i :: Int32) jobIds
        liftIO $ runPoolSession env $ Session.statement ids (updateHeartbeatStmt settings)

    retryJobs updates = do
        env <- HasqlDb (ReaderT return)
        let settings = hdbSettings env
            executeAfters = V.fromList [ea | (_, ea, _) <- updates]
            attempts = V.fromList [a | (_, _, a) <- updates]
            ids = V.fromList [fromIntegral i | (JobId i, _, _) <- updates]
        liftIO $
            runPoolSession env $
                Session.statement
                    (executeAfters, attempts, ids)
                    (retryJobsStmt settings)

    mergedChildResults (JobId jid) = do
        env <- HasqlDb (ReaderT return)
        let settings = hdbSettings env
        liftIO $ do
            results <- runPoolSession env $ Session.statement (fromIntegral jid) (mergedChildResultsStmt settings)
            return (V.toList results)

    withTransaction action = do
        env <- HasqlDb (ReaderT return)
        liftIO $ do
            -- Manually manage BEGIN/COMMIT/ROLLBACK via raw SQL sessions
            let doBegin = runPoolSession env $ Session.sql "BEGIN"
                doCommit = runPoolSession env $ Session.sql "COMMIT"
                doRollback = runPoolSession env $ Session.sql "ROLLBACK"
            doBegin
            ( do
                    result <- runHasqlDb env action
                    doCommit
                    return result
                )
                `catch` ( \(e :: SomeException) -> do
                            _ <- doRollback `catch` (\(_ :: SomeException) -> return ())
                            throwIO e
                        )

    insertSchedule (CronExpression expr) (Entrypoint ep) = do
        env <- HasqlDb (ReaderT return)
        liftIO $ runPoolSession env $ Session.statement (expr, ep) insertScheduleStmt

    fetchSchedules = do
        env <- HasqlDb (ReaderT return)
        rows <- liftIO $ runPoolSession env $ Session.statement () fetchSchedulesStmt
        return $ V.toList $ V.map mapScheduleRow rows
      where
        mapScheduleRow (i, expr, ep, hb, cr, upd, nr, lr, st) =
            Schedule
                { scheduleId = ScheduleId (fromIntegral i)
                , scheduleExpression = CronExpression expr
                , scheduleEntrypoint = Entrypoint ep
                , scheduleHeartbeat = hb
                , scheduleCreated = cr
                , scheduleUpdated = upd
                , scheduleNextRun = nr
                , scheduleLastRun = lr
                , scheduleStatus = fromMaybe Queued (textToJobStatus st)
                }

    setScheduleQueued (ScheduleId sid) nextRun = do
        env <- HasqlDb (ReaderT return)
        liftIO $ runPoolSession env $ Session.statement (nextRun, fromIntegral sid) setScheduleQueuedStmt

    getEarliestNextRun = do
        env <- HasqlDb (ReaderT return)
        liftIO $ runPoolSession env $ Session.statement () getEarliestNextRunStmt

-- | Format a NominalDiffTime as a PostgreSQL interval string.
formatInterval :: NominalDiffTime -> Text
formatInterval dt =
    let secs = realToFrac dt :: Double
     in T.pack (show secs) <> " seconds"
