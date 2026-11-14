# pgqueuer-hs

[![Hackage](https://img.shields.io/hackage/v/pgqueuer-hs.svg)](https://hackage.haskell.org/package/pgqueuer-hs)
[![GHC](https://img.shields.io/badge/GHC-9.4%20%7C%209.6%20%7C%209.8%20%7C%209.10-purple.svg)](https://www.haskell.org/ghc/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Haskell CI](https://github.com/tusharad/pgqueuer-hs/actions/workflows/haskell.yaml/badge.svg)](https://github.com/tusharad/pgqueuer-hs/actions/workflows/haskell.yaml)

A high-performance, transactional PostgreSQL-backed background job queue for Haskell, fully compatible with [pgqueuer](https://github.com/janbjorge/pgqueuer).

---

## Overview

**Your PostgreSQL database is already a distributed job queue.**

`pgqueuer-hs` turns PostgreSQL into a fast, resilient, horizontally scalable background job processor. Jobs reside directly inside your PostgreSQL database with full ACID guarantees.

- **No separate message broker**: Eliminate the operational overhead of running RabbitMQ, Redis, or Kafka for background tasks.
- **Transactional enqueuing**: Enqueue jobs in the exact same database transaction as your business domain data. If your transaction rolls back, the job is never scheduled.
- **Zero-allocation binary protocol**: Support for both `postgresql-simple` and compiled `hasql` prepared statements with connection pooling.
- **Cross-language compatible**: Process jobs enqueued by Python `pgqueuer` services in Haskell, or vice versa.

---

## Architecture

```mermaid
flowchart TD
    subgraph Producers ["Producers / Applications"]
        P1["App Transaction (PostgreSQL)"]
        P2["Cron Scheduler (PGQueuer)"]
        P3["DAG Workflow Root"]
    end

    subgraph Postgres ["PostgreSQL 14+"]
        Q["pgqueuer (Active Ledger)\nFOR UPDATE SKIP LOCKED"]
        L["pgqueuer_log (Unlogged Table)\nAudit & Telemetry"]
        S["pgqueuer_schedules\nDistributed Cron"]
        T["Triggers & LISTEN/NOTIFY\nInstant Dispatch"]
    end

    subgraph Workers ["Horizontal Worker Cluster"]
        W1["Worker Instance 1"]
        W2["Worker Instance 2"]
        W3["Worker Instance N"]
    end

    subgraph Buffers ["STM Decoupled Sinks"]
        B1["TimedOverflowBuffer (Logs)"]
        B2["TimedOverflowBuffer (Heartbeats)"]
    end

    P1 -->|INSERT INTO pgqueuer| Q
    P2 -->|Dispatch Due Jobs| Q
    P3 -->|insertJobTree| Q

    Q -.->|Notify Mutation| T
    T -.->|Instant Wakeup| Workers

    Workers -->|Batch Claim| Q
    Workers -->|Push Logs| B1
    Workers -->|Push Keepalives| B2

    B1 -->|Bulk Flush| L
    B2 -->|HOT Update| Q
```

---

## Key Features

- **Lock-Free Concurrency**: Leverages `FOR UPDATE SKIP LOCKED` and partial indexes to eliminate row lock contention across horizontal worker pools.
- **Dual Backend Architecture (`MonadPGQueuer`)**:
  - `SimpleDb`: Reference driver built on `postgresql-simple` for lightweight setups.
  - `HasqlDb`: High-performance binary driver using server-side prepared statements and `hasql-pool`.
- **Instant Event Dispatch**: Emits PostgreSQL `LISTEN` / `NOTIFY` signals when jobs arrive, waking workers immediately without wasteful high-frequency polling.
- **STM Timed Overflow Buffering**: Worker threads push status transitions and heartbeats to bounded STM queues (`TimedOverflowBuffer`), flushed in bulk to eliminate WAL write amplification.
- **Resilient Backoff & Jitter**: Configurable mathematical backoff strategies (`Exponential`, `Linear`, `Constant`) with anti-thundering-herd jitter (`FullJitter`, `EqualJitter`, `NoJitter`).
- **DAG Workflow Trees**: Build complex job dependency pipelines (`JobTree`, `<~~`). Parent jobs remain held in PostgreSQL until all child tasks finish successfully.
- **Distributed Cron Scheduling**: Native 5-field cron recurring jobs (`CronExpression`) stored and coordinated directly in PostgreSQL.
- **Distributed Tracing (OpenTelemetry)**: Built-in W3C trace context injection and span extraction in job headers (`-f otel`).

---

## Installation

Add `pgqueuer-hs` to your `package.yaml`:

```yaml
dependencies:
  - pgqueuer-hs >= 0.1.0.0
  - postgresql-simple
  - aeson
  - text
  - time
  - uuid
```

Or in your `.cabal` file:

```cabal
build-depends:
    base >= 4.7 && < 5
  , pgqueuer-hs >= 0.1.0.0
```

---

## Quick Start (15 Lines)

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Data.Aeson (encode)
import Data.UUID.V4 (nextRandom)
import PGQueuer

main :: IO ()
main = do
  let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
  queueMgrId <- nextRandom

  withQueueManager connStr defaultDBSettings queueMgrId $ \qm -> do
    -- 1. Setup schema automatically
    setupSchema qm

    -- 2. Register entrypoint handler
    let ep = Entrypoint "SendWelcomeEmail"
    qm1 <- registerEntrypoint qm ep $ \job -> do
      putStrLn $ "Processing job #" ++ show (let JobId jid = jobId job in jid)

    -- 3. Enqueue a job
    _ <- enqueue qm1 ep (Just $ encode ("user@example.com" :: String)) 0 Nothing Nothing Nothing

    -- 4. Start worker loop (blocks and processes jobs)
    putStrLn "Worker listening for incoming jobs..."
    let execParams = [EntrypointExecutionParameter ep 5 3 (Exponential 2 60) FullJitter]
    workerLoop qm1 defaultBufferConfig execParams
```

---

## Backend Comparison

`pgqueuer-hs` provides a unified `MonadPGQueuer` typeclass that abstracts low-level database operations:

| Feature | `SimpleDb` (postgresql-simple) | `HasqlDb` (hasql) |
|---|---|---|
| **Protocol** | Text/Parametrized | Compiled Binary Protocol |
| **Connection Pooling** | Single Connection or external pool | Native `hasql-pool` (built-in) |
| **Query Planning** | Client-side query preparation | Server-side prepared statements |
| **Best For** | Simple CLI scripts, prototypes, testing | High-throughput production microservices |

### Running with Hasql Connection Pool

```haskell
import Database.PostgreSQL.Simple (connectPostgreSQL)
import PGQueuer.Backend.Hasql
import PGQueuer.Core.Monad
import PGQueuer.Schema (install)
import PGQueuer.Settings (defaultDBSettings)
import PGQueuer.Types

main :: IO ()
main = do
  let connStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
  env <- mkHasqlDbEnv connStr defaultDBSettings

  -- Execute transactional job operations
  runHasqlDb env $ withTransaction $ do
    _ <- enqueue (Entrypoint "ImageResize") Nothing 0 Nothing Nothing Nothing Nothing Nothing Queued
    pure ()

  releaseHasqlDbEnv env
```

---

## Core Capabilities

### 1. Resilient Retries with Jitter

Prevent cascading failures and thundering herd stampedes by applying backoff and jitter algorithms:

```haskell
let params =
      [ EntrypointExecutionParameter
          { paramEntrypoint = Entrypoint "ExternalPaymentAPI"
          , paramConcurrencyLimit = 5       -- At most 5 concurrent jobs
          , paramMaxAttempts = 5            -- Retry up to 5 times
          , paramBackoffStrategy = Exponential 2 60 -- Base 2s, Cap 60s
          , paramJitter = EqualJitter       -- Half deterministic, half random
          }
      ]
```

### 2. DAG Workflows & Dependency Trees

Define workflows where parent tasks wait for child tasks to complete:

```haskell
-- Define child task nodes
let childA = JobNode (Entrypoint "ExtractData") (Just "chunk-1") 0 Nothing Nothing Nothing Nothing <~~ []
    childB = JobNode (Entrypoint "ExtractData") (Just "chunk-2") 0 Nothing Nothing Nothing Nothing <~~ []

-- Parent MasterReport task depends on childA and childB
let workflow = JobNode (Entrypoint "MasterReport") Nothing 0 Nothing Nothing Nothing Nothing <~~ [childA, childB]

-- Atomically inserts tree: children are Queued, parent is Held until children complete
insertJobTree workflow
```

### 3. Distributed Cron Scheduling

Coordinate periodic jobs across your entire cluster without external daemons:

```haskell
-- Register a schedule running every 15 minutes
registerSchedule qm (CronExpression "*/15 * * * *") (Entrypoint "SyncCatalog")
```

---

## Examples

Fully working examples are located in the [`example`](example/) directory:

- [**01-hello-mailer**](example/app/01-hello-mailer.hs): Basic queue management, JSON payload handling, worker loop.
- [**02-resilient-scraper**](example/app/02-resilient-scraper.hs): Flaky API handling with `JobRetryableException`, exponential backoff, equal jitter, and admin retry watchdog.
- [**03-map-reduce-etl**](example/app/03-map-reduce-etl.hs): Hierarchical DAG workflows (`JobTree`) with 500 child tasks and parent state aggregation.
- [**04-chaos-ecommerce**](example/app/04-chaos-ecommerce.hs): High-throughput stress test with 5,000+ jobs/sec and a live queue status dashboard.
- [**05-hasql-worker**](example/app/05-hasql-worker.hs): Zero-allocation binary protocol backend with connection pooling and `FOR UPDATE SKIP LOCKED`.
- [**06-cron-pipeline**](example/app/06-cron-pipeline.hs): Distributed cron schedule management and periodic worker dispatch.

To run the examples:

```bash
# 1. Start local PostgreSQL
make db-up

# 2. Run any example
cd example
stack run hello-mailer
stack run hasql-worker
stack run cron-pipeline
```

---

## Development & Makefile

```bash
make help          # List all available targets
make db-up         # Start local PostgreSQL container (Podman or Docker)
make test          # Run test suite against PostgreSQL
make bench         # Compile and execute benchmarks
make lint          # Run HLint static analysis
make format        # Format all Haskell files with Fourmolu
make format-check  # Verify formatting compliance (CI check)
make check         # Run cabal check for Hackage packaging
make docs          # Build Haddock documentation
make clean         # Clean build artifacts
make db-down       # Stop and remove local database container
```

---

## License

This project is licensed under the [MIT License](LICENSE) - see the LICENSE file for details.

---

## See Also

- [Python pgqueuer](https://github.com/janbjorge/pgqueuer) - The original Python implementation.
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) - `FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY`.
- [hasql](https://hackage.haskell.org/package/hasql) - High-performance PostgreSQL driver.
- [postgresql-simple](https://hackage.haskell.org/package/postgresql-simple) - Mid-level client library.
