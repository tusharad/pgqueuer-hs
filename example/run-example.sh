#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"

case "$mode" in
  simple|hpcscheduler|python)
    stack run example-exe -- "$mode"
    ;;
  py-consumer)
    uv run --with asyncpg pgq run consumer:main
    ;;
  py-producer)
    uv run --with asyncpg,pgqueuer,asyncio producer.py
    ;;
  h2p)
    uv run --with asyncpg pgq run consumer:main &
    consumer_pid=$!
    trap 'kill "$consumer_pid" 2>/dev/null || true' EXIT
    sleep 2
    stack run example-exe -- python
    wait "$consumer_pid"
    ;;
  p2h)
    uv run --with asyncpg,pgqueuer,asyncio producer.py
    stack run example-exe -- simple
    ;;
  *)
    cat <<'EOF'
usage: ./run-example.sh [simple|hpcscheduler|python|py-consumer|py-producer|h2p|p2h]
EOF
    exit 1
    ;;
esac