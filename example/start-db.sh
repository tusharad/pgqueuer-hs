#!/usr/bin/env bash
set -euo pipefail

ENGINE="${CONTAINER_ENGINE:-}"
if [ -z "$ENGINE" ]; then
  if command -v podman >/dev/null 2>&1; then
    ENGINE=podman
  elif command -v docker >/dev/null 2>&1; then
    ENGINE=docker
  else
    echo "Error: Neither podman nor docker found in PATH." >&2
    exit 1
  fi
fi

CONTAINER_NAME="queue-db"
DB_USER="queue_user"
DB_PASS="queue_pass"
DB_NAME="queue_db"
DB_PORT="5432"

echo "Using container engine: $ENGINE"
$ENGINE stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
$ENGINE rm "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "Starting PostgreSQL container ($CONTAINER_NAME) on port $DB_PORT..."
$ENGINE run \
  --name "$CONTAINER_NAME" -d \
  -e POSTGRES_USER="$DB_USER" \
  -e POSTGRES_PASSWORD="$DB_PASS" \
  -e POSTGRES_DB="$DB_NAME" \
  -p "$DB_PORT:5432" \
  postgres:16

echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
  if $ENGINE exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; then
    echo "PostgreSQL is ready and accepting connections."
    exit 0
  fi
  sleep 1
done

echo "Error: PostgreSQL failed to become ready within 30 seconds." >&2
exit 1
