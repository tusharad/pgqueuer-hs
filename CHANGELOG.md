# Changelog

All notable changes to `pgqueuer-hs` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the [Haskell Package Versioning Policy (PVP)](https://pvp.haskell.org/).

## [Unreleased]

### Added
- **Multi-Snapshot CI Matrix**:
  - Added support for multiple Stackage LTS resolvers (`stack-lts-24.52.yaml`, `stack-lts-23.28.yaml`, `stack-lts-22.44.yaml`, `stack-lts-21.25.yaml`).
  - Added dynamic matrix discovery using `freckle/stack-action/generate-matrix@v5`.
- **Comprehensive Unit & Integration Test Suites**:
  - `Tests.TypesSpec`: Bidirectional roundtrips for `JobStatus`, `OnFailure`, `BackoffStrategy`, `Jitter`, and JSON serialization.
  - `Tests.BufferSpec`: Concurrency, overflow triggering, timer flush, and graceful shutdown tests for `TimedOverflowBuffer`.
  - `Tests.CronSpec`: Cron expression parsing and next run calculation tests.
  - `Tests.WorkflowSpec`: Hierarchical DAG tree construction and invariant verification.
- **Runnable Example Executables**:
  - Added `hasql-worker` (`example/app/05-hasql-worker.hs`) showcasing the high-performance compiled binary Hasql backend with connection pooling.
  - Added `cron-pipeline` (`example/app/06-cron-pipeline.hs`) demonstrating recurring cron job scheduling and worker dispatch.
- **Workflow Automation & Developer Tooling**:
  - Added `Makefile` with targets for `build`, `test`, `bench`, `lint`, `format`, `format-check`, `check`, `docs`, `clean`, `db-up`, `db-down`.
  - Added `.hlint.yaml` configuration preventing dangerous partial functions (`head`, `tail`, `fromJust`, `read`, `undefined`, etc.).
  - Added automated Hackage publication pipeline (`.github/workflows/publish-hackage.yml`).
  - Added `cabal.project` linking root and example packages.

### Fixed
- **Haddock Documentation Warnings**:
  - Achieved 100% Haddock documentation coverage across all library modules.
  - Disambiguated identifier type/value links (`'t:JobId'`, `'t:Job'`, `'t:SimpleDb'`, `'t:HasqlDb'`, `'t:TimedOverflowBuffer'`, `'t:DBSettings'`).
  - Fixed out-of-scope identifier link warnings.
- **Fourmolu Formatting**:
  - Cleaned and formatted all source trees (`src`, `test`, `bench`, `example`) conforming strictly to `fourmolu.yaml`.
- **Database Engine Compatibility**:
  - Made `start-db.sh` and Makefile auto-detect both `podman` and `docker` with health-check readiness polling.
  - Fixed resilient scraper example's admin retry loop with real timestamp and attempt tracking.

---

## [0.0.1.0] - 2026-11-08

### Added
- **Core Architecture (`PGQueuer`)**:
  - High-level `QueueManager` API (`createQueueManager`, `withQueueManager`) for lifecycle and connection management.
  - Dual backend abstraction via `MonadPGQueuer`:
    - `SimpleDb`: Reference driver built on `postgresql-simple`.
    - `HasqlDb`: Zero-allocation high-performance driver using compiled binary prepared statements and `hasql-pool`.
- **Lock-Free Concurrency**:
  - Dequeue engine utilizing PostgreSQL `FOR UPDATE SKIP LOCKED` for zero-contention job claims across horizontal worker instances.
  - Per-entrypoint concurrency limits and optional global concurrency limits.
- **STM Timed Overflow Buffering (`PGQueuer.Worker.Buffer`)**:
  - Decoupled database write logging and heartbeat updates via `TimedOverflowBuffer`.
  - Bulk flushing triggered by buffer capacity overflow or timer expiration.
- **Retry Policies & Jitter Algorithms (`PGQueuer.Worker.Backoff`)**:
  - Mathematical backoff strategies: `Exponential`, `Linear`, `Constant`.
  - Anti-thundering-herd jitter implementations: `NoJitter`, `FullJitter`, `EqualJitter`.
  - Transient exception hierarchy: `JobRetryableException` and `JobPermanentException`.
- **DAG Workflow Dependencies (`PGQueuer.Workflow`)**:
  - Tree-structured job execution (`JobTree`, `JobNode`, `<~~`).
  - Automatic atomic hold on parent jobs until all child jobs complete with rollup of child outputs.
- **Distributed Cron Scheduling (`PGQueuer.Worker.Cron`)**:
  - 5-field cron recurring job registration (`CronExpression`) stored directly in PostgreSQL.
  - Lock-free schedule acquisition with `calculateNextRunTime` dispatch.
- **OpenTelemetry Observability (`PGQueuer.Tracing`)**:
  - Conditional OpenTelemetry integration (`-f otel`) supporting W3C trace context propagation in job headers.
- **Database Schema Management (`PGQueuer.Schema`)**:
  - Full automated DDL installation (`installSchema`, `uninstallSchema`, `verifyStructure`).
  - Triggers and functions emitting asynchronous pub/sub events via `LISTEN`/`NOTIFY`.
