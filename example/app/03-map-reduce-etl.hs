{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (modifyMVar_, newMVar, readMVar, threadDelay)
import Data.Aeson (encode)
import Data.UUID.V4 (nextRandom)
import PGQueuer

main :: IO ()
main = do
    let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    uuid <- nextRandom

    withQueueManager connStr defaultDBSettings uuid $ \qm -> do
        setupSchema qm
        _ <- verifyStructure qm

        -- Counter to track active branch workers (just for visualization)
        activeWorkers <- newMVar (0 :: Int)

        -- Register ProcessBranch entrypoint
        _ <- registerEntrypoint qm (Entrypoint "ProcessBranch") $ \_payload -> do
            -- Increment active workers
            modifyMVar_ activeWorkers (return . (+ 1))
            currentActive <- readMVar activeWorkers
            putStrLn $ "[Branch Worker] Processing branch... (Active workers: " ++ show currentActive ++ ")"

            -- Simulate some work
            threadDelay 50000

            -- Decrement
            modifyMVar_ activeWorkers (return . subtract 1)

        -- Register MasterEOD entrypoint
        _ <- registerEntrypoint qm (Entrypoint "MasterEOD") $ \_payload -> do
            putStrLn "\n\n======================================="
            putStrLn "MASTER EOD REPORT RUNNING"
            putStrLn "=======================================\n\n"

        let execParams =
                [ EntrypointExecutionParameter
                    { paramEntrypoint = Entrypoint "ProcessBranch"
                    , paramConcurrencyLimit = 10 -- EXACTLY 10 concurrency limit
                    , paramMaxAttempts = 1
                    , paramBackoffStrategy = Constant 0
                    , paramJitter = NoJitter
                    }
                , EntrypointExecutionParameter
                    { paramEntrypoint = Entrypoint "MasterEOD"
                    , paramConcurrencyLimit = 1
                    , paramMaxAttempts = 1
                    , paramBackoffStrategy = Constant 0
                    , paramJitter = NoJitter
                    }
                ]

        -- Build JobTree: Master -> 500 Branches
        let mkBranch i = JobNode (Entrypoint "ProcessBranch") (Just $ encode i) 0 Nothing Nothing Nothing Nothing <~~ []
            masterNode = JobNode (Entrypoint "MasterEOD") Nothing 0 Nothing Nothing Nothing Nothing
            tree = masterNode <~~ map mkBranch [(1 :: Int) .. 500]

        putStrLn "Inserting Job Tree with 1 Master and 500 Branches..."
        insertJobTree qm tree

        -- Start worker loop
        putStrLn "Workers started. Watch the concurrency limit cap at 10..."
        workerLoop qm defaultBufferConfig execParams
