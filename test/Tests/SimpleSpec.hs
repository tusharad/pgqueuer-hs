{-# LANGUAGE OverloadedStrings #-}

module Tests.SimpleSpec (simpleCoreTests) where

import Database.PostgreSQL.Simple (Connection)
import qualified Database.PostgreSQL.Simple as PG
import PGQueuer.Backend.Simple
import PGQueuer.Settings (DBSettings)
import Test.Tasty
import Tests.CoreSpec (Runner (..), connStr, coreTests)

{- | Core integration tests running on the 'SimpleDb' backend.

Uses the same 4 tests as HasqlDb to prove behavioral parity.
-}
simpleCoreTests :: TestTree
simpleCoreTests = coreTests "SimpleDb" mkSimpleRunner mkDualSimpleRunners

-- | Create a single SimpleDb runner from a schema-management connection.
mkSimpleRunner :: Connection -> DBSettings -> IO (Runner, IO ())
mkSimpleRunner conn settings = do
    let env = SimpleDbEnv conn settings
    let runner = Runner $ \action -> runSimpleDb env action
    return (runner, return ())

-- | Create two independent SimpleDb runners for concurrent testing.
mkDualSimpleRunners :: Connection -> DBSettings -> IO (Runner, Runner, IO ())
mkDualSimpleRunners _conn settings = do
    conn1 <- PG.connectPostgreSQL connStr
    conn2 <- PG.connectPostgreSQL connStr
    let env1 = SimpleDbEnv conn1 settings
    let env2 = SimpleDbEnv conn2 settings
    let runner1 = Runner $ \action -> runSimpleDb env1 action
    let runner2 = Runner $ \action -> runSimpleDb env2 action
    let cleanup = do
            PG.close conn1
            PG.close conn2
    return (runner1, runner2, cleanup)
