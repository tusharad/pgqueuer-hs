{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : PGQueuer.Settings
Description : Configuration settings and table name resolvers for PGQueuer.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Provides 't:DBSettings' for configuring database table prefixes and
pub/sub channel names, along with helper functions resolving table names.
-}
module PGQueuer.Settings
  ( DBSettings (..)
  , defaultDBSettings
  , queueTable
  , queueTableLog
  , statisticsTable
  , schedulesTable
  , queueStatusType
  , function
  , trigger
  ) where

import Data.Text (Text)
import PGQueuer.Types (Channel, defaultChannel)

-- | Database schema configuration options.
data DBSettings = DBSettings
  { dbPrefix :: Text
  -- ^ Optional prefix prepended to all table, function, and trigger names.
  , dbChannel :: Channel
  -- ^ PostgreSQL channel used for @LISTEN@ / @NOTIFY@ signals.
  }
  deriving (Show, Eq)

-- | Default settings with no prefix and standard channel @"ch_pgqueuer"@.
defaultDBSettings :: DBSettings
defaultDBSettings =
  DBSettings
    { dbPrefix = ""
    , dbChannel = defaultChannel
    }

-- | Resolved name of the primary active jobs table.
queueTable :: DBSettings -> Text
queueTable settings = dbPrefix settings <> "pgqueuer"

-- | Resolved name of the unlogged audit log table.
queueTableLog :: DBSettings -> Text
queueTableLog settings = dbPrefix settings <> "pgqueuer_log"

-- | Resolved name of the queue throughput statistics table.
statisticsTable :: DBSettings -> Text
statisticsTable settings = dbPrefix settings <> "pgqueuer_statistics"

-- | Resolved name of the recurring cron schedules table.
schedulesTable :: DBSettings -> Text
schedulesTable settings = dbPrefix settings <> "pgqueuer_schedules"

-- | Resolved name of the PostgreSQL status enum type.
queueStatusType :: DBSettings -> Text
queueStatusType settings = dbPrefix settings <> "pgqueuer_status"

-- | Resolved name of the database change trigger function.
function :: DBSettings -> Text
function settings = dbPrefix settings <> "fn_pgqueuer_changed"

-- | Resolved name of the database change trigger.
trigger :: DBSettings -> Text
trigger settings = dbPrefix settings <> "tg_pgqueuer_changed"
