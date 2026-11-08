{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Tests.TypesSpec
Description : Unit tests for PGQueuer domain types, JSON instances, and conversions.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental
-}
module Tests.TypesSpec (typesTests) where

import Data.Aeson (decode, encode)
import PGQueuer.Types
import Test.Tasty
import Test.Tasty.HUnit

typesTests :: TestTree
typesTests =
  testGroup
    "Types & Serialization"
    [ jobStatusConversions
    , onFailureConversions
    , jsonRoundtrips
    , defaultsTests
    ]

jobStatusConversions :: TestTree
jobStatusConversions =
  testGroup
    "JobStatus Conversions"
    [ testCase "All JobStatus constructors roundtrip through textToJobStatus" $ do
        let allStatuses = [minBound .. maxBound] :: [JobStatus]
        mapM_
          (\st -> assertEqual (show st) (Just st) (textToJobStatus $ jobStatusToText st))
          allStatuses
    , testCase "Invalid status text returns Nothing" $ do
        assertEqual "invalid text" Nothing (textToJobStatus "unknown_status")
        assertEqual "empty text" Nothing (textToJobStatus "")
    ]

onFailureConversions :: TestTree
onFailureConversions =
  testGroup
    "OnFailure Conversions"
    [ testCase "All OnFailure constructors roundtrip through textToOnFailure" $ do
        let allFailures = [minBound .. maxBound] :: [OnFailure]
        mapM_
          (\f -> assertEqual (show f) (Just f) (textToOnFailure $ onFailureToText f))
          allFailures
    , testCase "Invalid failure text returns Nothing" $ do
        assertEqual "invalid text" Nothing (textToOnFailure "ignore")
        assertEqual "empty text" Nothing (textToOnFailure "")
    ]

jsonRoundtrips :: TestTree
jsonRoundtrips =
  testGroup
    "JSON Roundtrips"
    [ testCase "JobId JSON roundtrip" $ do
        let jid = JobId 42
        assertEqual "JobId" (Just jid) (decode $ encode jid)
    , testCase "ScheduleId JSON roundtrip" $ do
        let sid = ScheduleId 101
        assertEqual "ScheduleId" (Just sid) (decode $ encode sid)
    , testCase "Entrypoint JSON roundtrip" $ do
        let ep = Entrypoint "email_processor"
        assertEqual "Entrypoint" (Just ep) (decode $ encode ep)
    , testCase "CronExpression JSON roundtrip" $ do
        let expr = CronExpression "*/5 * * * *"
        assertEqual "CronExpression" (Just expr) (decode $ encode expr)
    , testCase "Channel JSON roundtrip" $ do
        let ch = Channel "my_custom_channel"
        assertEqual "Channel" (Just ch) (decode $ encode ch)
    , testCase "JobStatus JSON roundtrip for all constructors" $ do
        let allStatuses = [minBound .. maxBound] :: [JobStatus]
        mapM_
          (\st -> assertEqual (show st) (Just st) (decode $ encode st))
          allStatuses
    , testCase "OnFailure JSON roundtrip" $ do
        let allFailures = [minBound .. maxBound] :: [OnFailure]
        mapM_
          (\f -> assertEqual (show f) (Just f) (decode $ encode f))
          allFailures
    , testCase "BackoffStrategy JSON roundtrip" $ do
        let s1 = Exponential 2 60
            s2 = Linear 1 30
            s3 = Constant 5
        assertEqual "Exponential" (Just s1) (decode $ encode s1)
        assertEqual "Linear" (Just s2) (decode $ encode s2)
        assertEqual "Constant" (Just s3) (decode $ encode s3)
    , testCase "Jitter JSON roundtrip" $ do
        let j1 = NoJitter
            j2 = FullJitter
            j3 = EqualJitter
        assertEqual "NoJitter" (Just j1) (decode $ encode j1)
        assertEqual "FullJitter" (Just j2) (decode $ encode j2)
        assertEqual "EqualJitter" (Just j3) (decode $ encode j3)
    ]

defaultsTests :: TestTree
defaultsTests =
  testGroup
    "Default Parameters"
    [ testCase "defaultChannel is ch_pgqueuer" $ do
        assertEqual "defaultChannel" (Channel "ch_pgqueuer") defaultChannel
    , testCase "defaultBatchSize is 100" $ do
        assertEqual "defaultBatchSize" 100 defaultBatchSize
    , testCase "defaultHeartbeatTimeout is 300" $ do
        assertEqual "defaultHeartbeatTimeout" 300 defaultHeartbeatTimeout
    ]
