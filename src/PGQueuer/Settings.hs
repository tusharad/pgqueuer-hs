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

data DBSettings = DBSettings
    { dbPrefix :: Text
    , dbChannel :: Channel
    }

defaultDBSettings :: DBSettings
defaultDBSettings =
    DBSettings
        { dbPrefix = ""
        , dbChannel = defaultChannel
        }

queueTable :: DBSettings -> Text
queueTable settings = dbPrefix settings <> "pgqueuer"

queueTableLog :: DBSettings -> Text
queueTableLog settings = dbPrefix settings <> "pgqueuer_log"

statisticsTable :: DBSettings -> Text
statisticsTable settings = dbPrefix settings <> "pgqueuer_statistics"

schedulesTable :: DBSettings -> Text
schedulesTable settings = dbPrefix settings <> "pgqueuer_schedules"

queueStatusType :: DBSettings -> Text
queueStatusType settings = dbPrefix settings <> "pgqueuer_status"

function :: DBSettings -> Text
function settings = dbPrefix settings <> "fn_pgqueuer_changed"

trigger :: DBSettings -> Text
trigger settings = dbPrefix settings <> "tg_pgqueuer_changed"
