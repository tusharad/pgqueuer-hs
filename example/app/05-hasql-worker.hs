{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Main
Description : High-performance Hasql Connection Pool Worker Example.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Demonstrates running worker workflows with the compiled binary Hasql backend
('HasqlDb'), connection pool acquisition, transactional enqueue, and batch dequeue
using @FOR UPDATE SKIP LOCKED@.
-}
module Main (main) where

import Control.Monad (forM_)
import Data.Aeson (encode)
import qualified Data.ByteString.Char8 as BS8
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import PGQueuer.Backend.Hasql
import PGQueuer.Core.Monad
import PGQueuer.Schema (install)
import PGQueuer.Settings (defaultDBSettings)
import PGQueuer.Types

main :: IO ()
main = do
  let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
  putStrLn "Initializing schema via setup connection..."
  setupConn <- connectPostgreSQL (BS8.pack connStr)
  install setupConn defaultDBSettings
  close setupConn

  putStrLn "Acquiring Hasql connection pool..."
  hasqlEnv <- mkHasqlDbEnv (BS8.pack connStr) defaultDBSettings

  qmId <- nextRandom
  let ep = Entrypoint "OrderNotification"
      execParams =
        [ EntrypointExecutionParameter
            { paramEntrypoint = ep
            , paramConcurrencyLimit = 10
            , paramMaxAttempts = 3
            , paramBackoffStrategy = Exponential 1 10
            , paramJitter = FullJitter
            }
        ]

  -- 1. Enqueue a batch of jobs inside a database transaction
  putStrLn "Enqueuing 20 notification jobs via Hasql binary protocol..."
  runHasqlDb hasqlEnv $ withTransaction $ do
    forM_ [1 .. 20 :: Int] $ \idx -> do
      let payload = encode (objectText [("orderId", show idx), ("customer", "Alice")])
      enqueue ep (Just payload) 0 Nothing Nothing Nothing Nothing Nothing Queued

  -- 2. Dequeue jobs using FOR UPDATE SKIP LOCKED
  putStrLn "Worker claiming jobs using FOR UPDATE SKIP LOCKED..."
  jobs <- runHasqlDb hasqlEnv $ dequeue 10 execParams qmId Nothing 300
  putStrLn $ "Claimed " ++ show (length jobs) ++ " jobs for processing."

  -- 3. Simulate processing, keepalives, and status completion
  forM_ jobs $ \job -> do
    putStrLn $ "Processing job #" ++ show (let JobId jid = jobId job in jid)
    -- Send heartbeat to prevent timeout
    runHasqlDb hasqlEnv $ updateHeartbeat [jobId job]
    -- Mark as successfully completed
    runHasqlDb hasqlEnv $ logJobs [(jobId job, Successful, Nothing)]

  putStrLn "Successfully processed all claimed jobs."
  releaseHasqlDbEnv hasqlEnv
  putStrLn "Hasql connection pool released cleanly."

objectText :: [(String, String)] -> String
objectText kvs =
  "{"
    ++ foldr
      (\(k, v) acc -> "\"" ++ k ++ "\":\"" ++ v ++ "\"" ++ if null acc then "" else "," ++ acc)
      ""
      kvs
    ++ "}"
