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
import qualified Data.ByteString.Char8 as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Database.PostgreSQL.Simple (Connection, execute_)
import Database.PostgreSQL.Simple.Notification (Notification)
import Database.PostgreSQL.Simple.Types (Query (..))

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
-- Currently a stub implementation
getNextNotification :: Connection -> IO (Maybe Notification)
getNextNotification _conn = do
  -- TODO: Implement using postgresql-simple's notification support
  return Nothing

-- | Send a job cancellation notification
-- Currently a stub implementation
notifyJobCancellation :: Connection -> Int -> IO ()
notifyJobCancellation _conn _jobId = do
  -- TODO: Implement notification sending
  return ()

-- | Send a health check notification
-- Currently a stub implementation
notifyHealthCheck :: Connection -> IO ()
notifyHealthCheck _conn = do
  -- TODO: Implement notification sending
  return ()

-- | Parse event payload from notification
-- Currently a stub implementation
parseEventPayload :: BS.ByteString -> Either String String
parseEventPayload _payload = do
  -- TODO: Parse JSON event payloads
  Right "event_parsed"
