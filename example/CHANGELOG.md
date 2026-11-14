# Changelog for `example`

All notable changes to the `pgqueuer-hs` examples will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the [Haskell Package Versioning Policy (PVP)](https://pvp.haskell.org/).

## [0.1.0.0] - 2026-11-08

### Added
- `hello-mailer`: Basic queue manager setup, JSON payload serialization, and worker loop.
- `resilient-scraper`: Simulated transient 502 failures, exponential backoff, equal jitter, and admin retry watchdog.
- `map-reduce-etl`: DAG job dependency tree with 500 child tasks and parent state aggregation.
- `chaos-ecommerce`: High-frequency stress test with live status dashboard.
- `hasql-worker`: High-performance binary protocol driver with `hasql-pool` connection pooling.
- `cron-pipeline`: Recurring cron job scheduling and worker processing.
