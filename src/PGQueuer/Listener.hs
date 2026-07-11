{-# LANGUAGE OverloadedStrings #-}

module PGQueuer.Listener
  ( listen
  , unlisten
  , unlistenAll
  , getNextNotification
  , notifyJobCancellation
  , notifyHealthCheck
  , parseEventPayload
  ) where

import Control.Monad (void)
import qualified Data.Aeson as Aeson
import Data.Aeson ((.=))
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import Data.Time (getCurrentTime)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (Connection, Only (..), execute, execute_)
import Database.PostgreSQL.Simple.Notification (Notification, getNotification)
import Database.PostgreSQL.Simple.Types (Query (..))
import PGQueuer.Types (Channel (..), defaultChannel)

-- | Listen for notifications on a specific channel
listen :: Connection -> T.Text -> IO ()
listen conn channel =
  void $ execute_ conn (Query $ TE.encodeUtf8 ("LISTEN " <> channel))

-- | Stop listening on a specific channel
unlisten :: Connection -> T.Text -> IO ()
unlisten conn channel =
  void $ execute_ conn (Query $ TE.encodeUtf8 ("UNLISTEN " <> channel))

-- | Stop listening on all channels
unlistenAll :: Connection -> IO ()
unlistenAll conn =
  void $ execute_ conn (Query "UNLISTEN *;")

-- | Get the next notification, blocking until available
getNextNotification :: Connection -> IO (Maybe Notification)
getNextNotification conn = Just <$> getNotification conn

sendNotification :: Connection -> Aeson.Value -> IO ()
sendNotification conn payload =
  void $ execute conn "NOTIFY pgqueuer, ?" (Only payloadText)
  where
    payloadText = TE.decodeUtf8 . BL.toStrict $ Aeson.encode payload

notificationChannel :: T.Text
notificationChannel = channelText
  where
    Channel channelText = defaultChannel

-- | Send a job cancellation notification
notifyJobCancellation :: Connection -> Int -> IO ()
notifyJobCancellation conn jobId = do
  sentAt <- getCurrentTime
  sendNotification
    conn
    (Aeson.object
      [ "channel" .= notificationChannel
      , "sent_at" .= sentAt
      , "type" .= ("cancellation_event" :: T.Text)
      , "ids" .= ([jobId] :: [Int])
      ])

-- | Send a health check notification
notifyHealthCheck :: Connection -> IO ()
notifyHealthCheck conn = do
  sentAt <- getCurrentTime
  healthCheckId <- nextRandom
  sendNotification
    conn
    (Aeson.object
      [ "channel" .= notificationChannel
      , "sent_at" .= sentAt
      , "type" .= ("health_check_event" :: T.Text)
      , "id" .= healthCheckId
      ])

-- | Parse event payload from notification
parseEventPayload :: BS.ByteString -> Either String String
parseEventPayload payload =
  case Aeson.eitherDecodeStrict' payload :: Either String Aeson.Value of
    Left err -> Left err
    Right _ -> Right (BS.unpack payload)
