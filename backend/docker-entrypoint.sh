#!/bin/sh
# Simple entrypoint that waits for the Postgres host:port to accept TCP
# connections before exec'ing the app. Designed to be tiny and POSIX-compatible
# so it works on Alpine's /bin/sh.

set -eu

# Allow override via env vars (compose sets DB_HOST/DB_PORT)
DB_HOST=${DB_HOST:-sumo-db}
DB_PORT=${DB_PORT:-5432}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-60}

echo "Waiting for Postgres at ${DB_HOST}:${DB_PORT} (timeout=${WAIT_TIMEOUT}s)"
count=0
while ! nc -z ${DB_HOST} ${DB_PORT} >/dev/null 2>&1; do
  count=$((count+1))
  if [ "$count" -ge "$WAIT_TIMEOUT" ]; then
    echo "Timed out waiting for Postgres after ${WAIT_TIMEOUT}s" >&2
    break
  fi
  sleep 1
done

echo "Starting sumo-backend"
exec /app/sumo-backend
