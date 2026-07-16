{- |
Module      : PGQueuer.Worker.Backoff
Description : Retry backoff and jitter calculations.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

This module provides data types and functions to calculate backoff delays
and apply jitter for retrying failed jobs.
-}
module PGQueuer.Worker.Backoff (
    BackoffStrategy (..),
    Jitter (..),
    calculateBackoff,
    applyJitter,
) where

import Data.Time.Clock (NominalDiffTime)
import System.Random (randomRIO)

import PGQueuer.Types (BackoffStrategy (..), Jitter (..))

{- | Calculate the base backoff delay before applying jitter.
The attempt parameter should be the number of attempts already made (e.g., if failing on the first attempt, attempts=1).
-}
calculateBackoff :: BackoffStrategy -> Int -> NominalDiffTime
calculateBackoff (Constant delay) _ = delay
calculateBackoff (Linear base cap) attempts =
    let calculated = base * fromIntegral attempts
     in min cap calculated
calculateBackoff (Exponential base cap) attempts =
    -- For exponential, commonly delay = base * 2^(attempts - 1)
    let calculated = base * (2 ^ (attempts - 1))
     in min cap calculated

-- | Apply jitter to a calculated delay.
applyJitter :: Jitter -> NominalDiffTime -> IO NominalDiffTime
applyJitter NoJitter delay = return delay
applyJitter FullJitter delay = do
    let maxMs = round ((realToFrac delay :: Double) * 1000) :: Int
    if maxMs <= 0
        then return delay
        else do
            jitterMs <- randomRIO (0, maxMs)
            return $ realToFrac (fromIntegral jitterMs :: Double) / 1000
applyJitter EqualJitter delay = do
    let halfDelay = delay / 2
        maxMs = round ((realToFrac halfDelay :: Double) * 1000) :: Int
    if maxMs <= 0
        then return delay
        else do
            jitterMs <- randomRIO (0, maxMs)
            return $ halfDelay + (realToFrac (fromIntegral jitterMs :: Double) / 1000)
