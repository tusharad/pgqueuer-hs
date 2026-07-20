# pgqueuer-hs: PostgreSQL-powered job queues for Haskell

A PostgreSQL-powered job queue library. fully compatible Haskell implementation of [pgqueuer](https://github.com/janbjorge/pgqueuer).

## Overview

Your PostgreSQL database is already a job queue.

`pgqueuer-hs` turns PostgreSQL into a fast, reliable background job processor. Jobs live in the same database as your application data. One stack, full ACID guarantees, and no separate message broker to run.

### Key Features

- **PostgreSQL-Native**: Jobs stored directly in PostgreSQL with ACID guarantees
- **Transactional Enqueue**: Enqueue jobs in the same transaction as your application data
- **Safe Concurrency**: `FOR UPDATE SKIP LOCKED` prevents duplicate processing
- **Per-Entrypoint Limits**: Configure concurrency limits per job type
- **Global Limits**: Optional global concurrency control across all entrypoints
- **Instant Dispatch**: `LISTEN/NOTIFY` wakes workers immediately when jobs arrive
- **Deferred Jobs**: Schedule job execution with `execute_after`
- **Deduplication**: Prevent duplicate jobs with `dedupe_key`
- **Cross-Language Compatible**: Jobs created in Python can be processed by Haskell (and vice versa)
- **Job Tracking**: Complete logging of job status transitions
- **Error Handling**: Failed jobs can be retried or held for manual inspection

## Database Schema

The Haskell port uses the **exact same schema** as Python pgqueuer:

## Examples

Fully working examples are available in the `./example` directory.

To run the examples, first spin up the PostgreSQL database (requires Docker or Podman):
```bash
make db-up
```

Then, navigate to the example directory and use `cabal run` to execute the applications. For example:
```bash
cd example
cabal run hello-mailer
cabal run resilient-scraper
cabal run map-reduce-etl
cabal run chaos-ecommerce
```

*(Note: The applications will automatically verify and install the required database schema on startup).*

## Installation

Add `pgqueuer-hs` to your `package.yaml` or `project.cabal` dependencies:

```yaml
dependencies:
  - pgqueuer-hs
  - postgresql-simple
  - uuid
  - time
  - text
  - aeson

## Quick start

Install schema if not installed.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Data.UUID.V4 (nextRandom)
import PGQueuer
import Data.Either (isLeft)
import Data.UUID.V4 (nextRandom)

main :: IO ()
main = do
    let conStr = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    queueMgrId <- nextRandom
    withQueueManager conStr defaultDBSettings queueMgrId $ \qm -> do
        -- setup schema
        setupSchema qm

        -- Enqueue a job
        let ep = Entrypoint "hello"
        let params = [EntrypointExecutionParameter ep 0 5 (Exponential 5 60) FullJitter]
        qm1 <- registerEntrypoint qm ep (\_ -> pure ())
        _ <- enqueue qm1 ep Nothing 0 Nothing Nothing Nothing

        -- Dequeue jobs
        pickedJobs <- dequeue qm1 20 params Nothing 3
        mapM_ (\job -> jobStatus job) pickedJobs
```

## Core Concepts
### Architecture

PGQueuer creates a self-contained ecosystem within your PostgreSQL database:
  1. pgqueuer table: The primary ledger for active jobs (status: queued, picked).
  2. pgqueuer_log table: An unlogged table used for fast, high-volume event logging of job state transitions.
  3. pgqueuer_statistics table: Aggregates queue throughput and metrics.
  4. pgqueuer_schedules table: Manages cron-like recurring jobs and future executions.
  
  Database Triggers: Automatically emit pub/sub notifications via fn_pgqueuer_changed when queue states mutate.

### Status Lifecycle

- Jobs transition through various states defined by JobStatus:
- Queued - Waiting for a worker.
- Picked - Claimed by a worker (protected by a heartbeat timeout).
- Successful - Completed without errors.
- Failed / Exception - Encountered an error (eligible for retry).
- Canceled / Deleted - Terminated or scrubbed.

> [!IMPORTANT]  
> Codebase is currently highly unstable.

## Notes

- Postgresql-simple is currently being used as the primary database driver. In the future, adapter drivers will be implemented.
- Scheduling is not supported right now.

## Development

For local development, testing, and benchmarking, a `Makefile` is provided to manage the database and schema.

1. **Start the Database** (requires `podman` or `docker`):
   ```bash
   make db-up
   ```
   *(Override the engine using `make ENGINE=docker db-up` if necessary)*

2. **Install Schema**:
   ```bash
   make install-schema
   ```

3. **Run Tests & Benchmarks**:
   ```bash
   make test
   make bench
   ```

4. **Stop Database**:
   ```bash
   make db-down
   ```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## See Also

- [Python pgqueuer](https://github.com/janbjorge/pgqueuer)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [postgresql-simple](http://hackage.haskell.org/package/postgresql-simple)
