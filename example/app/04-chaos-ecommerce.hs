{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (throwIO)
import Control.Monad (forever)
import Data.UUID.V4 (nextRandom)
import PGQueuer
import System.Random (randomRIO)

main :: IO ()
main = do
    let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    uuid <- nextRandom

    withQueueManager connStr defaultDBSettings uuid $ \qm -> do
        setupSchema qm
        _ <- verifyStructure qm

        -- 1. Register handlers for ChargeCard, DeductInventory, ShipOrder
        _ <- registerEntrypoint qm (Entrypoint "ChargeCard") $ \_payload -> do
            panic <- randomRIO (1, 100 :: Int)
            if panic > 95
                then throwIO $ JobRetryableException "Random Thread Panic in ChargeCard!"
                else threadDelay 10000 -- simulate 10ms work
        _ <- registerEntrypoint qm (Entrypoint "DeductInventory") $ \_payload -> do
            delay <- randomRIO (5000, 20000)
            threadDelay delay -- variable delay
        _ <- registerEntrypoint qm (Entrypoint "ShipOrder") $ \_payload -> do
            threadDelay 5000

        -- 2. Generator Thread: Floods the queue with 5000 orders/sec in batches
        _ <- forkIO $ forever $ do
            let batchSize = 500
            let entrypoints = replicate batchSize (Entrypoint "ChargeCard")
            let payloads = replicate batchSize (Just "{}")
            let priorities = replicate batchSize 0
            let nothingList = replicate batchSize Nothing
            _ <- enqueueMultiple qm entrypoints payloads priorities nothingList nothingList nothingList
            threadDelay 100000 -- 100ms sleep, approx 5000/sec
            return ()

        -- 3. Watchdog Dashboard Thread
        _ <- forkIO $ forever $ do
            threadDelay 1000000 -- every 1 second
            putStrLn "\n--- [LIVE DASHBOARD] ---"
            stats <- getQueueSize qm
            if null stats
                then putStrLn "Queue is empty."
                else mapM_ print stats

            -- Simulate randomly cancelling a few jobs if queue is large
            -- To really do this we need JobIds, but we'll just demonstrate the API exists
            -- markJobAsCancelled qm [someId]
            -- For chaos, we might clearQueue if it goes completely haywire
            -- clearQueue qm (Just ["ChargeCard"])

            putStrLn "------------------------\n"

        -- 4. Start Worker Pool with aggressive buffer
        let execParams =
                [ EntrypointExecutionParameter (Entrypoint "ChargeCard") 20 3 (Constant 1) FullJitter
                , EntrypointExecutionParameter (Entrypoint "DeductInventory") 20 3 (Constant 1) FullJitter
                , EntrypointExecutionParameter (Entrypoint "ShipOrder") 20 3 (Constant 1) FullJitter
                ]

        let chaosBuffer = BufferConfig{bcMaxSize = 1000, bcFlushInterval = 500000}

        putStrLn "[Chaos E-Commerce] Starting workers with aggressive 1000-job buffer..."
        workerLoop qm chaosBuffer execParams
