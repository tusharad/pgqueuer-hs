{-# LANGUAGE OverloadedStrings #-}

module Tests.SimpleSpec
  ( simpleCoreTests
  , simpleCoreTestsWith
  )
where

import Data.ByteString (ByteString)
import qualified Database.PostgreSQL.Simple as PG
import PGQueuer.Backend.Simple
import Test.Tasty
import Tests.CoreSpec (Runner (..), connStr, coreTestsWith)

{- | Core integration tests running on the 'SimpleDb' backend.

Uses the same 5 tests as HasqlDb to prove behavioral parity.
-}
simpleCoreTests :: TestTree
simpleCoreTests = simpleCoreTestsWith (pure connStr)

simpleCoreTestsWith :: IO ByteString -> TestTree
simpleCoreTestsWith getConnStr = coreTestsWith getConnStr "SimpleDb" mkSimpleRunner mkDualSimpleRunners
  where
    mkSimpleRunner _cStr conn settings = do
      let env = SimpleDbEnv conn settings
      let runner = Runner $ \action -> runSimpleDb env action
      return (runner, return ())

    mkDualSimpleRunners cStr _conn settings = do
      conn1 <- PG.connectPostgreSQL cStr
      conn2 <- PG.connectPostgreSQL cStr
      let env1 = SimpleDbEnv conn1 settings
      let env2 = SimpleDbEnv conn2 settings
      let runner1 = Runner $ \action -> runSimpleDb env1 action
      let runner2 = Runner $ \action -> runSimpleDb env2 action
      let cleanup = do
            PG.close conn1
            PG.close conn2
      return (runner1, runner2, cleanup)
