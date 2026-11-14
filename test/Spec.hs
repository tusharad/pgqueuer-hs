{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import System.Environment (lookupEnv)
import Test.Tasty
import Test.Tasty.Runners (NumThreads (..))
import Tests.BackoffSpec (backoffTests)
import Tests.BufferSpec (bufferTests)
import Tests.CronSpec (cronTests)
import Tests.HasqlSpec (hasqlCoreTestsWith)
import Tests.PGQueuer (pgqueuerTestsWith)
import Tests.SimpleSpec (simpleCoreTestsWith)
import Tests.TestContainer (resolveConnStr, withPostgresTestContainer)
import Tests.TypesSpec (typesTests)
import Tests.WorkflowSpec (workflowTests)

main :: IO ()
main = do
  useTC <- lookupEnv "USE_TESTCONTAINERS"
  let runTestContainers = case useTC of
        Just v | v `elem` ["1", "true", "TRUE", "yes", "YES"] -> True
        _ -> False

  let e2eGroup label getConnStr =
        testGroup
          label
          [ pgqueuerTestsWith getConnStr
          , testGroup
              "Core - Backend Parity"
              [ simpleCoreTestsWith getConnStr
              , hasqlCoreTestsWith getConnStr
              ]
          ]

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
          then withPostgresTestContainer (e2eGroup "End-to-End Tests (Testcontainers PostgreSQL 16)")
          else e2eGroup "End-to-End Tests" resolveConnStr
      ]
