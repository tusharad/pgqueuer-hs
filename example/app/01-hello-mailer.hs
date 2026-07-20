{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Aeson
import qualified Data.Text as T
import Data.UUID.V4 (nextRandom)
import GHC.Generics
import PGQueuer

data UserPayload = UserPayload {email :: T.Text, name :: T.Text}
    deriving (Show, Generic, ToJSON, FromJSON)

main :: IO ()
main = do
    let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    uuid <- nextRandom

    -- Boot Queue Manager
    withQueueManager connStr defaultDBSettings uuid $ \qm -> do
        -- 1. Initialize Schema
        setupSchema qm
        _ <- verifyStructure qm

        -- 3. Register Entrypoint
        _ <- registerEntrypoint qm (Entrypoint "SendEmail") $ \job -> do
            case jobPayload job of
                Just p -> case decode p of
                    Just user -> putStrLn $ "Email sent to " ++ T.unpack (name user) ++ " <" ++ T.unpack (email user) ++ ">"
                    Nothing -> putStrLn "Failed to parse payload"
                Nothing -> putStrLn "No payload found"
            return ()

        -- 4. Enqueue a Job
        putStrLn "Enqueuing SendEmail job..."
        _ <- enqueue qm (Entrypoint "SendEmail") (Just $ encode (UserPayload "user@example.com" "Alice")) 0 Nothing Nothing Nothing

        -- 5. Start worker loop (will block, so we'll just let it run for a bit if we want or just block forever)
        putStrLn "Worker started. Processing jobs... (Press Ctrl+C to quit)"
        workerLoop qm defaultBufferConfig []
