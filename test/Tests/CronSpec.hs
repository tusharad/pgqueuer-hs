{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Tests.CronSpec
Description : Unit tests for Cron scheduling calculations and expression validation.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental
-}
module Tests.CronSpec (cronTests) where

import Data.Either (isLeft, isRight)
import Data.Time (UTCTime (..), fromGregorian, secondsToDiffTime)
import PGQueuer.Types (CronExpression (..))
import PGQueuer.Worker.Cron (calculateNextRunTime)
import Test.Tasty
import Test.Tasty.HUnit

cronTests :: TestTree
cronTests =
  testGroup
    "Cron Scheduler"
    [ testCase "Valid every-minute expression '* * * * *' produces future time" $ do
        let refTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 3600)
            res = calculateNextRunTime (CronExpression "* * * * *") refTime
        assertBool "Calculation succeeds" (isRight res)
        case res of
          Right nextTime -> assertBool "Next time is strictly after refTime" (nextTime > refTime)
          Left err -> assertFailure ("Unexpected error: " ++ err)
    , testCase "Valid daily midnight expression '0 0 * * *' produces midnight time" $ do
        let refTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 3600)
            res = calculateNextRunTime (CronExpression "0 0 * * *") refTime
        assertBool "Calculation succeeds" (isRight res)
        case res of
          Right nextTime -> do
            assertBool "Next time is after refTime" (nextTime > refTime)
            assertEqual "Midnight seconds" (secondsToDiffTime 0) (utctDayTime nextTime)
          Left err -> assertFailure ("Unexpected error: " ++ err)
    , testCase "Invalid cron expression returns Left with error message" $ do
        let refTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 3600)
            res = calculateNextRunTime (CronExpression "not-a-valid-cron-string") refTime
        assertBool "Returns Left on malformed cron" (isLeft res)
    ]
