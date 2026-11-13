# AGENTS.md — pgqueuer-hs

This file is the primary orientation document for AI coding agents working on this codebase.
Read this fully before making any change. Update it when architectural decisions are made.

---

## What This Project Is

`pgqueuer-hs` is a high-performance, type-safe PostgreSQL background job queue for Haskell applications.
Inspired by `pgqueuer` in Python, `pgqueuer-hs` leverages PostgreSQL's `LISTEN` / `NOTIFY` and `FOR UPDATE SKIP LOCKED` for reliable, non-contending, at-least-once distributed job processing.

**Key capabilities**:
* **Zero-polling real-time dispatch**: Immediate worker wakeup via PostgreSQL `LISTEN` / `NOTIFY`.
* **Non-contending job claims**: Safe concurrent workers with `FOR UPDATE SKIP LOCKED`.
* **Dual Backend**:
  * `SimpleDb`: Backed by `postgresql-simple` for simplicity and compatibility.
  * `HasqlDb`: Backed by `hasql` and `hasql-pool` utilizing PostgreSQL's binary protocol for maximum throughput.
* **Declarative Workflow DAGs**: Hierarchical orchestration using `JobTree` and the `<~~` dependency operator.
* **Native Cron Scheduling**: Periodic cron job evaluation and automatic dispatch.
* **Pluggable Retries & Backoff**: Constant, linear, and exponential backoff strategies with full or equal jitter.
* **Buffered WAL Writes**: `TimedOverflowBuffer` for write batching and rate limiting to prevent PostgreSQL WAL explosion under heavy load.
* **Distributed Tracing**: Structured logging and OpenTelemetry tracing integration.

---

## Formal Verification Requirement

**Every new feature or bug fix MUST follow the `formal-spec-verifier` skill protocol:**

1. Read `.agents/skills/formal-spec-verifier/SKILL.md`
2. Fill a System Law Spec from `.agents/skills/formal-spec-verifier/resources/system-law-template.yaml`
3. Write Tasty / HUnit / QuickCheck properties FIRST (leave implementation body as `undefined` until contract and tests are in place)
4. Implement minimum code to satisfy the oracle
5. Run: `./.agents/skills/formal-spec-verifier/scripts/verify_gate.sh --haskell`

---

## Branching Strategy & Git Protocol

All agents and contributors working on this codebase MUST follow this branching model without exception:

### 1. Branch Roles & Hierarchy
* **`main`**: Protected production branch. Represents verified, stable releases tagged with SemVer (`vX.Y.Z.W`). **NEVER commit or push directly to `main`.**
* **`develop`**: Primary integration branch and default development target. All feature and bugfix branches originate here and merge back here.
* **Supporting Branches**:
  * `feat/<feature-name>`: Cut from `develop`, merged back to `develop` via PR.
  * `fix/<bug-description>`: Cut from `develop`, merged back to `develop`.
  * `release/vX.Y.Z.W`: Release stabilization branches cut from `develop` when preparing milestone releases; merged into `main` (with release tag) and back into `develop`.
  * `hotfix/<description>`: Emergency production fixes cut from `main`, merged into both `main` and `develop`.
  * `chore/<task>`: Documentation, toolchain, or CI/CD updates cut from `develop`.

### 2. Mandatory Agent Workflow
1. **Always base work on up-to-date `develop`**:
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feat/<feature-name>
   ```
2. **Never commit directly to `main`** or leave feature work unmerged on detached/ad-hoc branches.
3. **Execute Formatting, Linting & Verification Gate before committing/merging**:
   ```bash
   make format-check   # fourmolu -m check across src, test, bench, example
   make lint           # hlint across src, test, bench, example
   make test           # run all 49+ unit and integration tests
   ./.agents/skills/formal-spec-verifier/scripts/verify_gate.sh --haskell
   ```
4. **Use Conventional Commits**:
   * `feat:` for new capabilities
   * `fix:` for bug fixes
   * `refactor:` for code restructuring without behavior change
   * `test:` for test additions or updates
   * `docs:` for documentation updates
   * `chore:` for build/dependency/tool updates

---

## Repository Architecture

```
stack.yaml / cabal.project     -- Workspace configuration (resolver: lts-24.52 / GHC 9.10.3)
stack-lts-*.yaml               -- Multi-LTS resolvers (lts-23.28, lts-24.52)
fourmolu.yaml                  -- Repository formatting rules (2-space indent, leading commas)
.hlint.yaml                    -- Static analysis rules (bans partial functions: head, tail, fromJust, error)
Makefile                       -- Unified targets: build, test, lint, format, check, sdist, docs-hackage, release-check

src/
  PGQueuer.hs                  -- Top-level public API, queue lifecycle, workerLoop, registration
  PGQueuer/
    Types.hs                   -- Domain types (Job, JobId, Channel, JobStatus, BackoffStrategy, Jitter)
    Settings.hs                -- DBSettings configuration and table naming
    Schema.hs                  -- Database schema installation and teardown
    Listener.hs                -- PostgreSQL LISTEN / NOTIFY real-time listener loop
    Query.hs                   -- Low-level database queries (postgresql-simple)
    Tracing.hs                 -- Tracing context and structured logging
    Core/
      Monad.hs                 -- Abstract backend typeclasses: MonadPgQueuer, MonadQueue, MonadJob
    Backend/
      Simple.hs                -- postgresql-simple backend implementation (SimpleDb)
      Hasql.hs                 -- Hasql binary protocol backend implementation (HasqlDb)
      Hasql/Statements.hs      -- Compiled Hasql Statements and Encoders/Decoders
    Worker/
      Backoff.hs               -- Retry delay calculation (Constant, Linear, Exponential) & Jitter
      Buffer.hs                -- TimedOverflowBuffer (write batching to protect PostgreSQL WAL)
      Cron.hs                  -- Periodic cron task evaluation and scheduling
    Workflow.hs                -- DAG workflow primitives (JobTree, JobNode, <~~)

test/
  Spec.hs                      -- Test suite entrypoint (Tasty test runner)
  Tests/
    TypesSpec.hs               -- Serialization, JSON roundtrips, and enum conversions
    CronSpec.hs                -- Cron parser validation and next-run-time calculations
    BufferSpec.hs              -- TimedOverflowBuffer concurrent drains, timer flush, and capacity limits
    WorkflowSpec.hs            -- Workflow DAG tree hierarchy and composition
    BackoffSpec.hs             -- Backoff strategies and jitter variance
    CoreSpec.hs                -- Core integration testing against live PostgreSQL
    SimpleSpec.hs              -- postgresql-simple backend integration
    HasqlSpec.hs               -- Hasql backend integration

bench/
  Main.hs                      -- Microbenchmarks and throughput benchmarks (tasty-bench)

example/
  package.yaml / stack.yaml    -- Standalone examples package
  start-db.sh                  -- Docker / Podman PostgreSQL 16 container launcher
  app/
    01-hello-mailer.hs         -- SimpleDb worker & producer
    02-resilient-scraper.hs    -- Retry policies and exponential backoff
    03-map-reduce-etl.hs       -- DAG workflow coordination with (<~~)
    04-chaos-ecommerce.hs      -- WAL write-batching via TimedOverflowBuffer
    05-hasql-worker.hs         -- High-performance Hasql binary protocol worker
    06-cron-pipeline.hs        -- Periodic cron task scheduling

.agents/
  skills/
    formal-spec-verifier/      -- Protocol and scripts for black-box formal verification
```

---

## Core Backend Architecture

### MonadPgQueuer & Backend Parity
All database operations are parameterized through the `MonadPgQueuer` typeclasses in `PGQueuer.Core.Monad`:
* **`SimpleDb`**: Wraps a `Database.PostgreSQL.Simple.Connection`.
* **`HasqlDb`**: Wraps a `Hasql.Pool.Pool`.

Any change affecting database queries, transactions, or state transitions MUST maintain parity between `SimpleDb` and `HasqlDb`.

### Write Batching with `TimedOverflowBuffer`
To avoid saturating PostgreSQL with transaction log entries during heavy workloads, log entries and state flushes use `withBuffer`:
* Automatically flushes when buffer reaches `bcMaxSize`.
* Automatically flushes when `bcTimeoutSeconds` elapses.
* Drains all remaining items cleanly on application exit.

---

## Code Quality & Formatting Rules

1. **Compiler Strictness**: Code must compile with `-Wall`, `-Wcompat`, `-Widentities`, `-Wincomplete-record-updates`, `-Wincomplete-uni-patterns`, `-Wmissing-export-lists`, `-Wmissing-home-modules`, `-Wredundant-constraints`.
2. **No Partial Functions**: Never use `head`, `tail`, `init`, `fromJust`, `read`, or `error` in library code. Use safe alternatives (`uncons`, pattern matching, `readMaybe`, monadic `fail`).
3. **Fourmolu Formatting**: Always run `make format` or verify with `make format-check`.
4. **HLint Cleanliness**: Always run `make lint` and resolve any hints.
5. **Cabal & Stack Parity**: Both `stack test` and `cabal check` must pass cleanly.
6. **Haddock Documentation**: Maintain 100% documentation coverage across all exported types and functions. Use unambiguous identifiers (`'t:JobId'`, `'t:SimpleDb'`).

---

## Testing & Quality Verification

Before committing:
```bash
make release-check
```

This automates:
1. Fourmolu formatting validation (`make format-check`)
2. HLint linting check (`make lint`)
3. Cabal package check (`cabal check`)
4. Test suite against PostgreSQL (`make test`)
5. Source distribution generation (`cabal sdist`)
6. Hackage documentation archive build (`cabal haddock --haddock-for-hackage --enable-doc`)

---

## File-by-File Change Guide

When implementing any feature or fix:
1. Check the issue description or requirements.
2. Read the affected source and test files.
3. Write HUnit / QuickCheck test cases FIRST.
4. Implement the minimum change required.
5. Verify with `make release-check` and `./.agents/skills/formal-spec-verifier/scripts/verify_gate.sh --haskell`.

