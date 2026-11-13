{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import System.Environment (lookupEnv)
import Test.Tasty
import Test.Tasty.Runners (NumThreads (..))
import Tests.BackoffSpec (backoffTests)
import Tests.BufferSpec (bufferTests)
import Tests.CronSpec (cronTests)
import Tests.HasqlSpec (hasqlCoreTests, hasqlCoreTestsWith)
import Tests.PGQueuer (pgqueuerTests, pgqueuerTestsWith)
import Tests.SimpleSpec (simpleCoreTests, simpleCoreTestsWith)
import Tests.TestContainer (withPostgresTestContainer)
import Tests.TypesSpec (typesTests)
import Tests.WorkflowSpec (workflowTests)

main :: IO ()
main = do
  useTC <- lookupEnv "USE_TESTCONTAINERS"
  let runTestContainers = case useTC of
        Just v | v `elem` ["1", "true", "TRUE", "yes", "YES"] -> True
        _ -> False

  defaultMain
    $ localOption (NumThreads 1)
    $ testGroup
      "PGQueuer"
      [ typesTests
      , cronTests
      , bufferTests
      , workflowTests
      , backoffTests
      , if runTestContainers
          then withPostgresTestContainer $ \getConnStr ->
            testGroup
              "End-to-End Tests (Testcontainers PostgreSQL 16)"
              [ pgqueuerTestsWith getConnStr
              , testGroup
                  "Core - Backend Parity"
                  [ simpleCoreTestsWith getConnStr
                  , hasqlCoreTestsWith getConnStr
                  ]
              ]
          else
            testGroup
              "End-to-End Tests"
              [ pgqueuerTests
              , testGroup
                  "Core - Backend Parity"
                  [ simpleCoreTests
                  , hasqlCoreTests
                  ]
              ]
      ]
