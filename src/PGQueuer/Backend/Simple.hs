{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

{- |
Module      : PGQueuer.Backend.Simple
Description : Reference backend using postgresql-simple.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Provides 'SimpleDb', a concrete monad transformer that implements
'MonadPGQueuer' by delegating to the existing "PGQueuer.Query" functions
through a 'ReaderT' over a single 'Connection'.

This is the baseline reference driver. It uses @FOR UPDATE SKIP LOCKED@
for lock-free dequeue and wraps transactions via @postgresql-simple@'s
'PG.withTransaction'.
-}
module PGQueuer.Backend.Simple (
    SimpleDb (..),
    SimpleDbEnv (..),
    runSimpleDb,
    mkSimpleDbEnv,
)
where

import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader, ReaderT (..), asks)
import Database.PostgreSQL.Simple (Connection)
import qualified Database.PostgreSQL.Simple as PG
import PGQueuer.Core.Monad (MonadPGQueuer (..))
import qualified PGQueuer.Query as Q
import PGQueuer.Settings (DBSettings, defaultDBSettings)

{- | Environment carrying the connection handle and database settings
required by the 'SimpleDb' monad.
-}
data SimpleDbEnv = SimpleDbEnv
    { sdbConnection :: Connection
    -- ^ Database connection handle
    , sdbSettings :: DBSettings
    -- ^ Table/schema naming settings
    }

-- | Convenience constructor using 'defaultDBSettings'.
mkSimpleDbEnv :: Connection -> SimpleDbEnv
mkSimpleDbEnv conn =
    SimpleDbEnv
        { sdbConnection = conn
        , sdbSettings = defaultDBSettings
        }

{- | A concrete monad transformer wrapping @ReaderT SimpleDbEnv IO@.

This is the reference 'MonadPGQueuer' implementation backed by
@postgresql-simple@.
-}
newtype SimpleDb a = SimpleDb
    { unSimpleDb :: ReaderT SimpleDbEnv IO a
    }
    deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader SimpleDbEnv)

-- | Run a 'SimpleDb' computation with the given environment.
runSimpleDb :: SimpleDbEnv -> SimpleDb a -> IO a
runSimpleDb env (SimpleDb action) = runReaderT action env

instance MonadPGQueuer SimpleDb where
    enqueue ep payload priority executeAfter dedupeKey headers = do
        conn <- asks sdbConnection
        settings <- asks sdbSettings
        liftIO $
            Q.enqueueSingle
                conn
                settings
                ep
                payload
                priority
                executeAfter
                dedupeKey
                headers

    dequeue batchSize params queueMgrId globalLimit heartbeatTimeoutSecs = do
        conn <- asks sdbConnection
        settings <- asks sdbSettings
        liftIO $
            Q.dequeue
                conn
                settings
                batchSize
                params
                queueMgrId
                globalLimit
                heartbeatTimeoutSecs

    logJobs jobStatuses = do
        conn <- asks sdbConnection
        settings <- asks sdbSettings
        liftIO $ Q.logJobs conn settings jobStatuses

    updateHeartbeat jobIds = do
        conn <- asks sdbConnection
        settings <- asks sdbSettings
        liftIO $ Q.updateHeartbeat conn settings jobIds

    retryJob job delay traceback = do
        conn <- asks sdbConnection
        settings <- asks sdbSettings
        liftIO $ Q.retryJob conn settings job delay traceback

    withTransaction action = do
        env <- SimpleDb (ReaderT return)
        let conn = sdbConnection env
        liftIO $ PG.withTransaction conn (runSimpleDb env action)
