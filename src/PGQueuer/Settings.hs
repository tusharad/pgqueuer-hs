{-# LANGUAGE OverloadedStrings #-}

module PGQueuer.Settings (
    DBSettings (..),
    defaultDBSettings,
    queueTable,
    queueTableLog,
    statisticsTable,
    schedulesTable,
    queueStatusType,
    function,
    trigger,
) where

import Data.Text (Text)
import PGQueuer.Types (Channel, defaultChannel)

-- ============================================================================
-- Database settings
-- ============================================================================

data DBSettings = DBSettings
    { dbPrefix :: Text
    , dbChannel :: Channel
    }

-- ============================================================================
-- Default settings
-- ============================================================================

defaultDBSettings :: DBSettings
defaultDBSettings =
    DBSettings
        { dbPrefix = ""
        , dbChannel = defaultChannel
        }

-- ============================================================================
-- Table and other object names (with prefix support)
-- ============================================================================

queueTable :: DBSettings -> Text
queueTable settings = dbPrefix settings <> "pgqueuer_queue"

queueTableLog :: DBSettings -> Text
queueTableLog settings = dbPrefix settings <> "pgqueuer_queue_log"

statisticsTable :: DBSettings -> Text
statisticsTable settings = dbPrefix settings <> "pgqueuer_statistics"

schedulesTable :: DBSettings -> Text
schedulesTable settings = dbPrefix settings <> "pgqueuer_schedules"

queueStatusType :: DBSettings -> Text
queueStatusType settings = dbPrefix settings <> "pgqueuer_job_status"

function :: DBSettings -> Text
function settings = dbPrefix settings <> "pgqueuer_notify_fn"

trigger :: DBSettings -> Text
trigger settings = dbPrefix settings <> "pgqueuer_trigger"
