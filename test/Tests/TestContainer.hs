{-# LANGUAGE OverloadedStrings #-}

module Tests.TestContainer
  ( postgresContainer
  , withPostgresTestContainer
  , getTestConnStr
  , resolveConnStr
  ) where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import Data.Function ((&))
import qualified Data.Text.Lazy as TL
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)
import System.Environment (lookupEnv)
import Test.Tasty (TestTree)
import TestContainers
  ( Container
  , ContainerRequest
  , Pipe (Stderr)
  , containerPort
  , containerRequest
  , fromTag
  , run
  , setEnv
  , setExpose
  , setWaitingFor
  , waitForLogLine
  , waitUntilTimeout
  , withoutReaper
  )
import TestContainers.Tasty (withContainers)

{- | PostgreSQL 16 container definition.
PostgreSQL logs its operational messages (including startup readiness) to Stderr.
-}
postgresContainer :: ContainerRequest
postgresContainer =
  containerRequest (fromTag "postgres:16-alpine")
    & setExpose [5432]
    & setEnv
      [ ("POSTGRES_USER", "queue_user")
      , ("POSTGRES_PASSWORD", "queue_pass")
      , ("POSTGRES_DB", "queue_db")
      ]
    & withoutReaper
    & setWaitingFor
      ( waitUntilTimeout 60 $
          waitForLogLine Stderr (TL.isInfixOf "database system is ready to accept connections")
      )

{- | Resolve connection host for mapped container ports.
Honors @TESTCONTAINERS_HOST@ if explicitly set, defaulting to @"127.0.0.1"@.
-}
getTestHost :: IO String
getTestHost = do
  mHost <- lookupEnv "TESTCONTAINERS_HOST"
  pure $ case mHost of
    Just h | not (null h) -> h
    _ -> "127.0.0.1"

-- | Retry connecting until PostgreSQL is actually responsive.
ensurePostgresReady :: ByteString -> IO ()
ensurePostgresReady connStr = go (30 :: Int)
  where
    go 0 = pure ()
    go n = do
      result <- try (connectPostgreSQL connStr) :: IO (Either SomeException Connection)
      case result of
        Right conn -> close conn
        Left _ -> do
          threadDelay 200000
          go (n - 1)

{- | Resolve connection string dynamically:
1. Uses PG_CONN_STRING if set.
2. Uses DATABASE_URL if set.
3. Falls back to the given dynamic container.
-}
getTestConnStr :: Container -> IO ByteString
getTestConnStr container = do
  mEnv <- lookupEnv "PG_CONN_STRING"
  case mEnv of
    Just str | not (null str) -> pure (BS8.pack str)
    _ -> do
      mDbUrl <- lookupEnv "DATABASE_URL"
      case mDbUrl of
        Just str | not (null str) -> pure (BS8.pack str)
        _ -> do
          host <- getTestHost
          let port = containerPort container 5432
          pure $
            "postgresql://queue_user:queue_pass@"
              <> BS8.pack host
              <> ":"
              <> BS8.pack (show port)
              <> "/queue_db"

-- | Static fallback connection resolver for local environments.
resolveConnStr :: IO ByteString
resolveConnStr = do
  mEnv <- lookupEnv "PG_CONN_STRING"
  case mEnv of
    Just str | not (null str) -> pure (BS8.pack str)
    _ -> do
      mDbUrl <- lookupEnv "DATABASE_URL"
      case mDbUrl of
        Just str | not (null str) -> pure (BS8.pack str)
        _ -> pure "postgresql://queue_user:queue_pass@localhost:5432/queue_db"

-- | Run a Tasty test tree with a dynamically provisioned PostgreSQL container.
withPostgresTestContainer :: (IO ByteString -> TestTree) -> TestTree
withPostgresTestContainer testFactory =
  withContainers (run postgresContainer) $ \getContainer ->
    testFactory $ do
      container <- getContainer
      connStr <- getTestConnStr container
      ensurePostgresReady connStr
      pure connStr
