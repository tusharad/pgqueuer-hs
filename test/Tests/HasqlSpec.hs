{-# LANGUAGE OverloadedStrings #-}

module Tests.HasqlSpec (hasqlCoreTests) where

import Database.PostgreSQL.Simple (Connection)
import PGQueuer.Backend.Hasql
import PGQueuer.Settings (DBSettings)
import Test.Tasty
import Tests.CoreSpec (Runner (..), connStr, coreTests)

{- | Core integration tests running on the 'HasqlDb' backend.

Uses the same 4 tests as SimpleDb to prove 100% behavioral parity.
-}
hasqlCoreTests :: TestTree
hasqlCoreTests = coreTests "HasqlDb" mkHasqlRunner mkDualHasqlRunners

-- | Create a single HasqlDb runner from a schema-management connection.
mkHasqlRunner :: Connection -> DBSettings -> IO (Runner, IO ())
mkHasqlRunner _conn settings = do
    env <- mkHasqlDbEnv connStr settings
    let runner = Runner $ \action -> runHasqlDb env action
    let cleanup = releaseHasqlDbEnv env
    return (runner, cleanup)

-- | Create two independent HasqlDb runners for concurrent testing.
mkDualHasqlRunners :: Connection -> DBSettings -> IO (Runner, Runner, IO ())
mkDualHasqlRunners _conn settings = do
    env1 <- mkHasqlDbEnv connStr settings
    env2 <- mkHasqlDbEnv connStr settings
    let runner1 = Runner $ \action -> runHasqlDb env1 action
    let runner2 = Runner $ \action -> runHasqlDb env2 action
    let cleanup = do
            releaseHasqlDbEnv env1
            releaseHasqlDbEnv env2
    return (runner1, runner2, cleanup)
