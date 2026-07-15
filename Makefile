.PHONY: db-up db-down install-schema uninstall-schema test bench build-example

DB_NAME=queue-db
DB_USER=queue_user
DB_PASS=queue_pass
DB_DB=queue_db
DB_PORT=5432
CONN_STR="postgresql://$(DB_USER):$(DB_PASS)@localhost:$(DB_PORT)/$(DB_DB)"

# Engine can be overridden with make ENGINE=docker db-up
ENGINE ?= podman

db-up:
	@echo "Starting Postgres via $(ENGINE)..."
	$(ENGINE) run --name $(DB_NAME) -d \
		-e POSTGRES_USER=$(DB_USER) \
		-e POSTGRES_PASSWORD=$(DB_PASS) \
		-e POSTGRES_DB=$(DB_DB) \
		-p $(DB_PORT):5432 \
		postgres:16 || echo "Database container already exists or failed to start."

db-down:
	@echo "Stopping Postgres..."
	-$(ENGINE) stop $(DB_NAME)
	-$(ENGINE) rm $(DB_NAME)

install-schema:
	@echo "Installing schema..."
	stack exec -- ghc -e 'import PGQueuer.Schema' -e 'import Database.PostgreSQL.Simple' -e 'import PGQueuer.Settings' -e 'conn <- connectPostgreSQL $(CONN_STR)' -e 'install conn defaultDBSettings'

uninstall-schema:
	@echo "Uninstalling schema..."
	stack exec -- ghc -e 'import PGQueuer.Schema' -e 'import Database.PostgreSQL.Simple' -e 'import PGQueuer.Settings' -e 'conn <- connectPostgreSQL $(CONN_STR)' -e 'uninstall conn defaultDBSettings'

test:
	stack test

bench:
	stack bench

build-example:
	cd example && stack build
