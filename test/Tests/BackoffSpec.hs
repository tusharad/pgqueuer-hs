{-# LANGUAGE OverloadedStrings #-}

module Tests.BackoffSpec (backoffTests) where

import PGQueuer.Types
import PGQueuer.Worker.Backoff
import Test.Tasty
import Test.Tasty.HUnit

backoffTests :: TestTree
backoffTests =
    testGroup
        "Backoff & Jitter Tests"
        [ testGroup
            "calculateBackoff"
            [ testCase "Constant strategy returns the constant delay" $ do
                let strategy = Constant 10
                calculateBackoff strategy 1 @?= 10
                calculateBackoff strategy 5 @?= 10
            , testCase "Linear strategy calculates base * attempts and respects cap" $ do
                let strategy = Linear 5 20
                calculateBackoff strategy 1 @?= 5
                calculateBackoff strategy 3 @?= 15
                calculateBackoff strategy 5 @?= 20 -- Capped at 20 (5 * 5 = 25)
            , testCase "Exponential strategy calculates base * 2^(attempts-1) and respects cap" $ do
                let strategy = Exponential 2 30
                calculateBackoff strategy 1 @?= 2
                calculateBackoff strategy 3 @?= 8 -- 2 * 2^2
                calculateBackoff strategy 5 @?= 30 -- Capped at 30 (2 * 2^4 = 32)
            ]
        , testGroup
            "applyJitter"
            [ testCase "NoJitter returns the exact delay" $ do
                res <- applyJitter NoJitter 10
                res @?= 10
            , testCase "FullJitter returns a value between 0 and delay" $ do
                res <- applyJitter FullJitter 10
                assertBool "FullJitter should be >= 0" (res >= 0)
                assertBool "FullJitter should be <= 10" (res <= 10)
            , testCase "EqualJitter returns a value between delay/2 and delay" $ do
                res <- applyJitter EqualJitter 10
                assertBool "EqualJitter should be >= 5" (res >= 5)
                assertBool "EqualJitter should be <= 10" (res <= 10)
            ]
        ]
