{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Tests.BufferSpec
Description : Unit tests for TimedOverflowBuffer concurrency and flush policies.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental
-}
module Tests.BufferSpec (bufferTests) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (replicateConcurrently_)
import Control.Concurrent.MVar (modifyMVar_, newMVar, readMVar)
import PGQueuer.Worker.Buffer
import Test.Tasty
import Test.Tasty.HUnit

bufferTests :: TestTree
bufferTests =
  testGroup
    "TimedOverflowBuffer"
    [ testCase "Manual flush drains all elements" $ do
        flushed <- newMVar ([] :: [Int])
        let config = BufferConfig {bcMaxSize = 100, bcFlushInterval = 1000000}
        withBuffer config (\items -> modifyMVar_ flushed (\acc -> pure (acc ++ items))) $ \buf -> do
          add buf 1
          add buf 2
          add buf 3
          flushBuffer buf
        res <- readMVar flushed
        assertEqual "Flushed items" [1, 2, 3] res
    , testCase "Overflow threshold automatically triggers flush" $ do
        flushed <- newMVar ([] :: [Int])
        -- Max size 5, long timer interval
        let config = BufferConfig {bcMaxSize = 5, bcFlushInterval = 5000000}
        withBuffer config (\items -> modifyMVar_ flushed (\acc -> pure (acc ++ items))) $ \buf -> do
          mapM_ (add buf) [1 .. 5]
          -- Allow background flusher thread to wake from overflow signal
          threadDelay 100000
          res <- readMVar flushed
          assertEqual "Automatically flushed on overflow" [1 .. 5] res
    , testCase "Timer expiration automatically triggers flush" $ do
        flushed <- newMVar ([] :: [Int])
        -- 50ms flush interval
        let config = BufferConfig {bcMaxSize = 100, bcFlushInterval = 50000}
        withBuffer config (\items -> modifyMVar_ flushed (\acc -> pure (acc ++ items))) $ \buf -> do
          add buf 999
          -- Wait for timer to fire
          threadDelay 150000
          res <- readMVar flushed
          assertEqual "Automatically flushed on timer" [999] res
    , testCase "withBuffer drains remaining items on exit" $ do
        flushed <- newMVar ([] :: [Int])
        let config = BufferConfig {bcMaxSize = 100, bcFlushInterval = 10000000}
        withBuffer config (\items -> modifyMVar_ flushed (\acc -> pure (acc ++ items))) $ \buf -> do
          add buf 42
          add buf 43
        -- After exiting withBuffer, flusher thread drains buffer
        res <- readMVar flushed
        assertEqual "Clean shutdown drains remaining" [42, 43] res
    , testCase "Concurrent pushes preserve all items" $ do
        flushed <- newMVar ([] :: [Int])
        let totalItems = 200 :: Int
            config = BufferConfig {bcMaxSize = 50, bcFlushInterval = 50000}
        withBuffer config (\items -> modifyMVar_ flushed (\acc -> pure (acc ++ items))) $ \buf -> do
          replicateConcurrently_ 4 $ do
            mapM_ (add buf) [1 .. 50]
          threadDelay 200000
          flushBuffer buf
        res <- readMVar flushed
        assertEqual "All concurrent items received" totalItems (length res)
    ]
