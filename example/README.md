# PGQueuer-hs Examples

Comprehensive, runnable examples demonstrating key features and production design patterns of `pgqueuer-hs`.

---

## Prerequisites

Start the local PostgreSQL container (supports both `podman` and `docker`):

```bash
./example/start-db.sh
```

Or from the repository root:

```bash
make db-up
```

---

## Runnable Applications

### 1. Hello Mailer (`hello-mailer`)
Basic queue manager setup, schema initialization, registration of entrypoints, payload serialization with Aeson, and worker loop dispatch.

```bash
cd example
stack run hello-mailer
```

### 2. Resilient Scraper (`resilient-scraper`)
Simulates flaky external HTTP APIs throwing `JobRetryableException`, configured with exponential backoff and jitter (`EqualJitter`), plus an administrative watchdog thread detecting and retrying failed/dead jobs via `listFailedJobs` and `retryJobs`.

```bash
cd example
stack run resilient-scraper
```

### 3. Map-Reduce ETL (`map-reduce-etl`)
Demonstrates Directed Acyclic Graph (DAG) workflows using `JobTree` and `<~~`. Enqueues a parent `MasterEOD` report held until 500 child branch jobs complete, enforcing exact concurrency limits and rolling up child outputs into the parent state.

```bash
cd example
stack run map-reduce-etl
```

### 4. Chaos E-Commerce (`chaos-ecommerce`)
High-throughput concurrency stress test: multi-entrypoint workflows (`ChargeCard`, `DeductInventory`, `ShipOrder`), batch enqueuing at 5,000+ jobs/sec, and a live watchdog dashboard monitoring queue depth and status transitions.

```bash
cd example
stack run chaos-ecommerce
```

### 5. High-Performance Hasql Worker (`hasql-worker`)
Zero-allocation database processing using the `HasqlDb` binary protocol backend, `hasql-pool` connection pooling, transactional batch enqueuing, and lock-free claiming with `FOR UPDATE SKIP LOCKED`.

```bash
cd example
stack run hasql-worker
```

### 6. Distributed Cron Pipeline (`cron-pipeline`)
Recurring job scheduling with 5-field cron expressions (`* * * * *`), computing next run timestamps via `calculateNextRunTime`, registering schedules in PostgreSQL, and executing periodic background jobs.

```bash
cd example
stack run cron-pipeline
```

---

## Building All Examples

```bash
cd example
stack build
```
