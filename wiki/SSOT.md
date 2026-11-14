# PGQueuer-hs: Engineering Master Plan & Architecture Specification

This document serves as the Single Source of Truth (SSOT) for the `PGQueuer-hs` project. It outlines the architectural vision, engineering standards, and execution roadmap required to build a battle-tested, production-grade PostgreSQL job queue in Haskell.

As the project evolves, this document will be updated to reflect shifting requirements and architectural decisions.

---

## I. Project Vision & Targets

The goal is to build a high-throughput, resilient, and multi-backend PostgreSQL job queue in Haskell. While maintaining loose conceptual compatibility with Python's PGQueuer, `PGQueuer-hs` is unconstrained by cross-language rigidities and will leverage Haskell's unique concurrency and type-safety strengths to compete directly with elite systems like Arbiter.

### Key Deliverables

* **Multi-Driver Architecture:** Native support for `postgresql-simple` (for simplicity), `hasql` (for zero-allocation extreme throughput), and `orville-postgresql` (for ORM integrations).


* **Zero-Contention Dispatch:** Database interactions must use `FOR UPDATE SKIP LOCKED` and partial indexes to eliminate row lock contention across horizontal worker pools.


* **STM-Powered Concurrency:** Utilizing Haskell's Software Transactional Memory (`TBQueue`, `TVar`) to batch database writes (logs, heartbeats) and decouple the DB from the execution threads.


* **Strict Observability:** Built-in benchmarking that tracks PostgreSQL-specific metrics: WAL write-amplification, Heap-Only Tuple (HOT) update efficiency, and dead-tuple accumulation.



---

## II. High-Level Architecture & Code Design

### 1. The Multi-Backend Driver Abstraction

To support multiple database drivers (`postgresql-simple`, `hasql`, `orville`), we will utilize the Repository Pattern via a typeclass (e.g., `MonadQueue` or `QueueRepository`). This mirrors the strategy used by Arbiter to wrap DB connections in different monad transformers like `HasqlDb`, `SimpleDb`, and `OrvilleM`.

* **Typeclass Definition:** The class will expose core operations: `enqueue`, `dequeue`, `logJobs`, `updateHeartbeat`, and `moveToDLQ`.


* **Backend Implementations:**
* `PGQueuer.Backend.Simple`: Uses `postgresql-simple` for easy setup and broad compatibility.


* `PGQueuer.Backend.Hasql`: Statically compiled prepared statements with binary encoding/decoding to bypass garbage collection overhead.


* `PGQueuer.Backend.Orville`: Integrates seamlessly for users utilizing the Orville ORM.





### 2. Event-Driven Dispatch via `LISTEN`/`NOTIFY`

To avoid idle busy-polling, the system will rely on PostgreSQL's asynchronous notifications.

* **Multi-Channel Listener:** A dedicated background thread will issue a `LISTEN` command on the queue channel (e.g., `ch_pgqueuer`).


* **Wake-up Signals:** When a `table_changed_event` arrives, the listener writes to an STM `TMVar`, which instantly wakes the sleeping dispatcher threads to execute a `dequeue` query.



### 3. Worker Pool & Buffer Subsystems

Workers will operate asynchronously from the database persistence layers.

* **Dispatch Loop:** The dispatcher pulls a batch of jobs from the DB and pushes them into an STM `TBQueue`.


* **Worker Threads:** Lightweight Haskell threads pull from the `TBQueue`, process the payload, and return the result.


* **Timed Overflow Buffers:** Successful job completions, failures, and heartbeats are pushed to internal buffers (`JobStatusLogBuffer`, `HeartbeatBuffer`). A background thread flushes these to the database via bulk `UPDATE`/`INSERT` arrays either when the buffer hits a size limit or a time threshold expires.



### 4. Exception Hierarchy & Safety

Errors will be explicitly modeled to dictate queue behavior:

* `JobRetryableException`: Job failed transiently. Calculate exponential backoff and update `execute_after`.


* `JobPermanentException`: Poison pill. Route immediately to the DLQ.


* `JobStolenException`: A concurrency race where a heartbeat detects another worker claimed the job.


* All worker loops must be wrapped in `tryAny` and `bracket` to guarantee resource cleanup and prevent thread death.



---

## III. Database Design & Optimization

The database schema must be meticulously tuned to minimize write amplification.

### Core Tables

1. **`pgqueuer` (Active Queue):** Holds only `queued` and `picked` jobs.


2. **`pgqueuer_log` (Archive/Audit):** Append-only table for completed, failed, or canceled jobs.


3. **`pgqueuer_statistics` (Metrics):** Aggregated throughput statistics.


4. **`pgqueuer_schedules` (Cron):** Manages recurring cron jobs.



### Performance Tuning Rules

* **Heap-Only Tuples (HOT):** The `pgqueuer` table will be configured with a `FILLFACTOR` of 70 to reserve page space. Updates to a job's `status` or `heartbeat` will happen in-place, drastically reducing disk IO.


* **Partial Indexes:** Indexes will be strictly scoped using `WHERE status = 'queued'` or `WHERE status = 'picked'`. Completed jobs are deleted from the active table and moved to the log table, keeping active indexes entirely memory-resident.



---

## IV. Project Phasing & Dependencies

### Phase 1: Foundation & Driver Abstraction

* **Objective:** Establish the core types, domain models, and the database abstraction layer.
* **Tasks:**
* Define `Job`, `JobId`, `JobStatus` (using `newtype` wrappers for safety).


* Define the `QueueRepository` typeclass.


* Implement the default `postgresql-simple` backend to achieve parity with the existing prototype.


* Implement the schema migration and validation logic (`verifyStructure`).





### Phase 2: The Performance Baseline (Hasql & Benchmarking)

* **Objective:** Implement the high-throughput `hasql` engine and prove its speed.
* **Tasks:**
* Implement the `hasql` backend utilizing prepared statements.


* Build the benchmarking suite (`Arbiter-Bench` equivalent) to track Throughput, WAL usage, HOT percentage, and Dead Tuples.


* Validate the `FOR UPDATE SKIP LOCKED` dequeue logic under heavy synthetic load.





### Phase 3: Event-Driven Dispatch & Buffering

* **Objective:** Introduce non-blocking concurrent workers.
* **Tasks:**
* Build the `NotificationConsumer` using `LISTEN` / `NOTIFY`.


* Implement `TimedOverflowBuffer` for batched heartbeats and log flushes.


* Implement graceful shutdown logic, ensuring in-flight jobs finish before process exit.





### Phase 4: Advanced Workflows & Orville Integration

* **Objective:** Complete feature sets for enterprise capabilities.
* **Tasks:**
* Implement the `Orville` database driver backend.


* Build the Scheduler Manager for Cron-based jobs.


* Build Dead Letter Queue (DLQ) automated routing for exhausted retries.


