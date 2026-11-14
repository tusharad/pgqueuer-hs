.PHONY: help build test bench lint format format-check check docs docs-hackage sdist release-check clean db-up db-down install-schema uninstall-schema build-example

.DEFAULT_GOAL := help

DB_NAME ?= queue-db
DB_USER ?= queue_user
DB_PASS ?= queue_pass
DB_DB ?= queue_db
DB_PORT ?= 5432
CONN_STR ?= "postgresql://$(DB_USER):$(DB_PASS)@localhost:$(DB_PORT)/$(DB_DB)"

# Engine auto-detection (podman or docker)
ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

## Display this help screen
help:
	@echo "Available Makefile targets for pgqueuer-hs:"
	@echo "  build           Compile library and executables"
	@echo "  test            Run test suite against PostgreSQL"
	@echo "  bench           Compile and execute benchmark suite"
	@echo "  lint            Run HLint static analysis on all source trees"
	@echo "  format          Format Haskell source files with Fourmolu"
	@echo "  format-check    Verify formatting with Fourmolu (CI check)"
	@echo "  check           Run cabal check for Hackage packaging validity"
	@echo "  docs            Build Haddock documentation"
	@echo "  docs-hackage    Build Hackage-compliant documentation archive"
	@echo "  sdist           Generate Hackage source distribution tarball"
	@echo "  release-check   Run complete pre-release validation suite"
	@echo "  clean           Clean build artifacts"
	@echo "  db-up           Start local PostgreSQL container"
	@echo "  db-down         Stop and remove local PostgreSQL container"
	@echo "  install-schema  Install PGQueuer schema into local database"
	@echo "  uninstall-schema Uninstall PGQueuer schema from local database"
	@echo "  build-example   Compile all example applications"

## Compile library
build:
	stack build

## Run test suite
test:
	stack test

## Run benchmark suite
bench:
	stack bench

## Run HLint static analysis
lint:
	hlint src test bench example

## Format source code with Fourmolu
format:
	fourmolu -i src test bench example

## Verify formatting with Fourmolu
format-check:
	fourmolu -m check src test bench example

## Validate package metadata
check:
	cabal check

## Build Haddock documentation
docs:
	stack haddock --no-haddock-deps

## Build Hackage-compliant documentation archive
docs-hackage:
	cabal haddock --haddock-for-hackage --enable-doc

## Build Hackage source distribution tarball
sdist:
	cabal sdist

## Run full pre-release validation
release-check: format-check lint check test sdist docs-hackage

## Clean build artifacts
clean:
	stack clean
	cd example && stack clean

## Start PostgreSQL container
db-up:
	./example/start-db.sh

## Stop PostgreSQL container
db-down:
	@echo "Stopping $(DB_NAME)..."
	-$(ENGINE) stop $(DB_NAME) >/dev/null 2>&1 || true
	-$(ENGINE) rm $(DB_NAME) >/dev/null 2>&1 || true

## Install database schema
install-schema:
	@echo "Installing schema..."
	stack exec -- ghc -e 'import PGQueuer.Schema' -e 'import Database.PostgreSQL.Simple' -e 'import PGQueuer.Settings' -e 'conn <- connectPostgreSQL $(CONN_STR)' -e 'install conn defaultDBSettings'

## Uninstall database schema
uninstall-schema:
	@echo "Uninstalling schema..."
	stack exec -- ghc -e 'import PGQueuer.Schema' -e 'import Database.PostgreSQL.Simple' -e 'import PGQueuer.Settings' -e 'conn <- connectPostgreSQL $(CONN_STR)' -e 'uninstall conn defaultDBSettings'

## Build all examples
build-example:
	cd example && stack build
