{-# LANGUAGE OverloadedStrings #-}

module Tests.HasqlSpec
  ( hasqlCoreTests
  , hasqlCoreTestsWith
  )
where

import Data.ByteString (ByteString)
import PGQueuer.Backend.Hasql
import Test.Tasty
import Tests.CoreSpec (Runner (..), connStr, coreTestsWith)

{- | Core integration tests running on the 'HasqlDb' backend.

Uses the same 5 tests as SimpleDb to prove 100% behavioral parity.
-}
hasqlCoreTests :: TestTree
hasqlCoreTests = hasqlCoreTestsWith (pure connStr)

hasqlCoreTestsWith :: IO ByteString -> TestTree
hasqlCoreTestsWith getConnStr = coreTestsWith getConnStr "HasqlDb" mkHasqlRunner mkDualHasqlRunners
  where
    mkHasqlRunner cStr _conn settings = do
      env <- mkHasqlDbEnv cStr settings
      let runner = Runner $ \action -> runHasqlDb env action
      let cleanup = releaseHasqlDbEnv env
      return (runner, cleanup)

    mkDualHasqlRunners cStr _conn settings = do
      env1 <- mkHasqlDbEnv cStr settings
      env2 <- mkHasqlDbEnv cStr settings
      let runner1 = Runner $ \action -> runHasqlDb env1 action
      let runner2 = Runner $ \action -> runHasqlDb env2 action
      let cleanup = do
            releaseHasqlDbEnv env1
            releaseHasqlDbEnv env2
      return (runner1, runner2, cleanup)
