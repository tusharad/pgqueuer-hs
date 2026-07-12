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

### Tables

- **pgqueuer**: Main job table
  - `id`: Job ID (SERIAL PRIMARY KEY)
  - `priority`: Job priority (INT)
  - `status`: Job status (ENUM: queued, picked, successful, exception, canceled, deleted, failed)
  - `entrypoint`: Job handler name (TEXT)
  - `payload`: Job data (BYTEA)
  - `execute_after`: Scheduled execution time (TIMESTAMP WITH TIME ZONE)
  - `dedupe_key`: Deduplication key (TEXT, optional)
  - `headers`: JSON headers (JSONB)
  - `attempts`: Attempt count (INT)
  - `queue_manager_id`: Worker ID (UUID)
  - `created`, `updated`, `heartbeat`: Timestamps

- **pgqueuer_log**: Job event log
  - `job_id`: Reference to queue.id
  - `status`: Status at the time of log entry
  - `traceback`: Exception details (JSONB)
  - `aggregated`: For log compaction (BOOLEAN)

- **pgqueuer_statistics**: Processing statistics
- **pgqueuer_schedules**: Cron-based job scheduling

### Indexes

Optimized indexes on:
- `(priority DESC, id ASC)` - For priority-based dequeue
- `(entrypoint, priority DESC, id ASC)` - Per-entrypoint selection
- `(entrypoint, execute_after)` - For deferred jobs
- `(queue_manager_id)` - For worker heartbeats
- Unique index on `(dedupe_key)` for active jobs only

## Examples

Fully working examples are available in `./example` directory

## Installation

### Prerequisites

- PostgreSQL 12+
- GHC 9.2+
- Cabal 3.4+ or Stack 2.7+
- `libpq-dev` (PostgreSQL client library)

### Build from Source

```bash
cd pgqueuer-hs
stack build
```

## License


## See Also

- [Python pgqueuer](https://github.com/janbjorge/pgqueuer)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [postgresql-simple](http://hackage.haskell.org/package/postgresql-simple)
