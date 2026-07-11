# pgqueuer-hs: Haskell port of PgQueuer

A fully compatible Haskell implementation of [pgqueuer](https://github.com/janbjorge/pgqueuer), a PostgreSQL-powered job queue library.

## Overview

`pgqueuer-hs` brings PostgreSQL-based job queue capabilities to Haskell applications. It shares the exact same database schema with the Python version, enabling seamless interoperability: jobs enqueued from Python can be processed by Haskell workers, and vice versa.

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

- **pgqueuer_queue**: Main job table
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

- **pgqueuer_queue_log**: Job event log
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

Or with Cabal:

```bash
cabal build all
```

## Quick Start

### 1. Set Up Database Schema

```haskell
{-# LANGUAGE OverloadedStrings #-}
import PGQueuer
import Database.PostgreSQL.Simple (connectPostgreSQL)
import Data.UUID.V4 (nextRandom)

main :: IO ()
main = do
  conn <- connectPostgreSQL ""  -- Uses PGHOST, PGUSER, PGDATABASE env vars
  let settings = defaultDBSettings
  queueMgrId <- nextRandom
  qm <- createQueueManager conn settings queueMgrId
  
  -- Install schema
  installSchema qm
  putStrLn "Schema installed!"
```

### 2. Enqueue Jobs

```haskell
import Data.ByteString.Char8 (pack)

enqueueExample :: QueueManager -> IO ()
enqueueExample qm = do
  -- Single job
  jobIds <- enqueue qm
    (Entrypoint "send_email")
    (Just (pack "user@example.com"))
    0  -- priority
    Nothing  -- execute_after (immediate)
    Nothing  -- no dedup key
    Nothing  -- no headers
  
  putStrLn $ "Enqueued: " ++ show jobIds
  
  -- Batch enqueue
  jobIds <- enqueueMultiple qm
    (replicate 100 (Entrypoint "process_task"))
    [Just (pack $ "Task " ++ show i) | i <- [1..100]]
    (replicate 100 0)  -- all priority 0
    []  -- no deferred execution
    []  -- no dedup keys
    []  -- no headers
  
  putStrLn $ "Enqueued " ++ show (length jobIds) ++ " jobs"
```

### 3. Dequeue and Process

```haskell
import Control.Concurrent (threadDelay)

processorExample :: QueueManager -> IO ()
processorExample qm = do
  let loop = do
        jobs <- dequeue qm
          100  -- batch size
          [EntrypointExecutionParameter (Entrypoint "send_email") 0]  -- 0 = unlimited
          Nothing  -- no global limit
          300  -- 5 min heartbeat timeout
        
        case jobs of
          [] -> threadDelay 1000000 >> loop  -- Wait 1 second
          _ -> do
            -- Process each job
            mapM_ processJob jobs
            
            -- Log results as successful
            logJobs qm [(jobId job, Successful, Nothing) | job <- jobs]
            
            loop
  
  loop

-- Process a single job
processJob :: Job -> IO ()
processJob job = do
  putStrLn $ "Processing job: " ++ show (jobId job)
  -- Your business logic here
```

## Cross-Language Compatibility

### Python Produces, Haskell Consumes

Python code:
```python
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver

pgq = PgQueuer(driver)

@pgq.entrypoint("fetch_data")
async def handle_fetch(job):
    # Process job.payload
    pass

# Enqueue from Python
await pgq.queries.enqueue("fetch_data", b"user_id=123")
```

Haskell consumer:
```haskell
import PGQueuer
import Data.ByteString (ByteString)

main = do
  conn <- connectPostgreSQL ""
  let settings = defaultDBSettings
  queueMgrId <- nextRandom
  qm <- createQueueManager conn settings queueMgrId
  
  -- Haskell will see the same jobs Python enqueued
  jobs <- dequeue qm 100 
    [EntrypointExecutionParameter (Entrypoint "fetch_data") 0]
    Nothing 300
  
  mapM_ processJob jobs
  
processJob :: Job -> IO ()
processJob job = do
  let payload = jobPayload job
  putStrLn $ "Got job: " ++ show (jobId job)
  -- Process the payload created by Python
```

## API Reference

### Core Types

```haskell
-- Job identifier
newtype JobId = JobId Int

-- Entrypoint handler name
newtype Entrypoint = Entrypoint Text

-- Job status
data JobStatus 
  = Queued
  | Picked
  | Successful
  | Failed
  | Exception
  | Canceled
  | Deleted

-- Main job record
data Job = Job
  { jobId :: JobId
  , jobPriority :: Int
  , jobStatus :: JobStatus
  , jobEntrypoint :: Entrypoint
  , jobPayload :: Maybe ByteString
  , jobExecuteAfter :: UTCTime
  , jobAttempts :: Int
  , ... }
```

### Queue Manager Functions

```haskell
-- Create queue manager
createQueueManager :: Connection -> DBSettings -> UUID -> IO QueueManager

-- Enqueue single job
enqueue :: QueueManager -> Entrypoint -> Maybe ByteString 
  -> Int -> Maybe NominalDiffTime -> Maybe Text -> Maybe Value
  -> IO [JobId]

-- Enqueue batch
enqueueMultiple :: QueueManager -> [Entrypoint] -> [Maybe ByteString]
  -> [Int] -> [Maybe NominalDiffTime] -> [Maybe Text] -> [Maybe Value]
  -> IO [JobId]

-- Dequeue jobs
dequeue :: QueueManager -> Int -> [EntrypointExecutionParameter]
  -> Maybe Int -> Int -> IO [Job]

-- Log job completion
logJobs :: QueueManager -> [(JobId, JobStatus, Maybe Value)] -> IO ()

-- Retry job
retryJob :: QueueManager -> Job -> NominalDiffTime -> Maybe Value -> IO ()

-- Requeue jobs
requeueJobs :: QueueManager -> [JobId] -> IO ()

-- Get queue statistics
getQueueSize :: QueueManager -> IO [QueueStatistics]

-- Cancel jobs
markJobAsCancelled :: QueueManager -> [JobId] -> IO ()

-- Update heartbeat (keep job alive)
updateHeartbeat :: QueueManager -> [JobId] -> IO ()
```

### Schema Management

```haskell
-- Install schema
installSchema :: QueueManager -> IO ()

-- Uninstall schema
uninstallSchema :: QueueManager -> IO ()

-- Verify structure
verifyStructure :: QueueManager -> IO (Either String ())
```

## Configuration

### Database Settings

```haskell
data DBSettings = DBSettings
  { dbPrefix :: Text    -- Table prefix (e.g., "myapp_")
  , dbChannel :: Channel -- LISTEN/NOTIFY channel name
  }

-- Use defaults
let settings = defaultDBSettings

-- Or customize
let settings = DBSettings 
  { dbPrefix = "myapp_"
  , dbChannel = Channel "myapp_pgqueuer"
  }
```

### Environment Variables

The PostgreSQL connection uses standard libpq environment variables:

- `PGHOST`: PostgreSQL server hostname (default: localhost)
- `PGPORT`: PostgreSQL server port (default: 5432)
- `PGUSER`: PostgreSQL user (default: postgres)
- `PGPASSWORD`: PostgreSQL password
- `PGDATABASE`: Database name

## Concurrency

### Per-Entrypoint Limits

Concurrency is enforced per entrypoint at the database level:

```haskell
-- Limit to 5 concurrent jobs for "send_email"
let params = [EntrypointExecutionParameter (Entrypoint "send_email") 5]
jobs <- dequeue qm 100 params Nothing 300
```

### Global Concurrency Limit

Limit total concurrent jobs across all entrypoints:

```haskell
-- Max 50 concurrent jobs globally
jobs <- dequeue qm 100 params (Just 50) 300
```

## Heartbeats

Long-running jobs must update their heartbeat to avoid being reclaimed:

```haskell
processLongJob :: QueueManager -> Job -> IO ()
processLongJob qm job = do
  -- Do work in chunks
  forM_ [1..10] $ \chunk -> do
    putStrLn $ "Chunk " ++ show chunk
    threadDelay 10000000  -- 10 seconds
    
    -- Update heartbeat every chunk
    updateHeartbeat qm [jobId job]
```

## Error Handling

### On Failure Policy

When a job fails, choose what happens:

1. **Delete** (default): Job is removed
2. **Hold**: Job status changes to 'failed' and stays in DB for inspection

```haskell
-- Log job as failed (hold for inspection)
logJobs qm [(jobId job, Failed, Just traceback)]

-- Later, manually inspect and requeue
failedJobs <- listFailedJobs qm 100
requeueJobs qm (map jobId failedJobs)
```

## Performance Considerations

1. **Batch Size**: Use appropriate batch size (50-200 typical)
2. **Heartbeat Interval**: Update frequently for long jobs
3. **Dequeue Poll Interval**: Sleep when no jobs available
4. **Index Coverage**: Schema includes optimal indexes
5. **UNLOGGED Tables**: Log table is unlogged for performance

## Limitations and Future Work

Currently supported:
- ✅ Basic job enqueueing and dequeueing
- ✅ Per-entrypoint concurrency limits
- ✅ Job retry and error handling
- ✅ Job cancellation
- ✅ Queue statistics
- ✅ Python-Haskell interoperability

Not yet implemented:
- ⚠️ Cron-based job scheduling (schedules table)
- ⚠️ LISTEN/NOTIFY real-time workers
- ⚠️ Tracing support (Logfire/Sentry)
- ⚠️ Health checks
- ⚠️ Dashboard/monitoring

## License


## See Also

- [Python pgqueuer](https://github.com/janbjorge/pgqueuer)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [postgresql-simple](http://hackage.haskell.org/package/postgresql-simple)
