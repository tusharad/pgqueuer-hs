# Contributing to pgqueuer-hs

Thank you for your interest in contributing to `pgqueuer-hs`! We welcome bug reports, feature proposals, documentation improvements, and pull requests.

---

## Development Setup

### Prerequisites
* **GHC**: 9.4, 9.6, 9.8, or 9.10+
* **Stack**: 2.15+ or **Cabal**: 3.10+
* **PostgreSQL**: 14+ (or Docker / Podman to run the local container)
* **Fourmolu**: 0.14+
* **HLint**: 3.8+

### Building the Project

Clone the repository and build:

```bash
git clone https://github.com/tusharad/pgqueuer-hs.git
cd pgqueuer-hs

# Using Stack
make build

# Or using Cabal
cabal build
```

---

## Local Database Setup

Integration tests and example applications connect to a PostgreSQL instance. You can launch a containerized test instance using:

```bash
make db-up
```

This starts a PostgreSQL 16 container named `queue-db` with healthcheck polling. To stop and clean it up:

```bash
make db-down
```

---

## Running Tests

Run the full test suite (including unit tests, cron tests, buffer flusher tests, and database integration tests):

```bash
make test
```

Or with Cabal:

```bash
cabal test
```

---

## Code Style & Formatting

We maintain strict consistency using Fourmolu and HLint:

* **Format code**:
  ```bash
  make format
  ```
* **Verify formatting**:
  ```bash
  make format-check
  ```
* **Static analysis**:
  ```bash
  make lint
  ```

---

## Pre-Release Validation

Before submitting a pull request or cutting a new release, execute the pre-release validation suite:

```bash
make release-check
```

This verifies:
1. Fourmolu formatting compliance (`make format-check`)
2. HLint linting with zero warnings (`make lint`)
3. Cabal package metadata validity (`cabal check`)
4. Full test suite execution (`make test`)
5. Hackage source distribution tarball generation (`cabal sdist`)
6. Hackage documentation tarball creation (`cabal haddock --haddock-for-hackage --enable-doc`)

---

## Publishing to Hackage

This repository contains a GitHub Actions workflow (`.github/workflows/publish-hackage.yml`) for automated Hackage releases.

### Automated Release via GitHub Actions
1. Ensure `CHANGELOG.md` and `package.yaml` have the updated version number according to the [Haskell Package Versioning Policy (PVP)](https://pvp.haskell.org/).
2. Run `make release-check` to verify all builds, tests, and tarballs succeed locally.
3. Commit and push changes to `main`.
4. Create and push a git tag prefixed with `v` (e.g. `v0.1.0.0`):
   ```bash
   git tag -a v0.1.0.0 -m "Release v0.1.0.0"
   git push origin v0.1.0.0
   ```
5. The GitHub Actions runner will automatically build the source tarball and Haddock archive and upload them to Hackage using repository secret `HACKAGE_TOKEN` (or `HACKAGE_USERNAME` and `HACKAGE_PASSWORD`).

### Manual Release (Fallback)
If publishing manually via Cabal:

```bash
# 1. Build distribution tarball and documentation
make sdist
make docs-hackage

# 2. Upload package and documentation to Hackage
cabal upload --publish -t "${HACKAGE_TOKEN}" dist-newstyle/sdist/pgqueuer-hs-*.tar.gz
cabal upload --publish -d -t "${HACKAGE_TOKEN}" dist-newstyle/pgqueuer-hs-*-docs.tar.gz
```

