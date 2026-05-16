#!/usr/bin/env sh
set -e

wait_for() {
  host="$1"
  port="$2"
  name="$3"
  retries=60
  while [ $retries -gt 0 ]; do
    if python -c "import socket,sys
s=socket.socket(); s.settimeout(2)
try:
    s.connect(('$host', int('$port')))
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
      echo "[entrypoint] $name is up at $host:$port"
      return 0
    fi
    retries=$((retries-1))
    sleep 1
  done
  echo "[entrypoint] timeout waiting for $name at $host:$port" >&2
  exit 1
}

PG_HOST="${POSTGRES_HOST:-postgres}"
PG_PORT="${POSTGRES_PORT:-5432}"
MQ_HOST="${RABBITMQ_HOST:-rabbitmq}"
MQ_PORT="${RABBITMQ_PORT:-5672}"
REDIS_H="${REDIS_HOST:-redis}"
REDIS_P="${REDIS_PORT:-6379}"

wait_for "$PG_HOST" "$PG_PORT" "postgres"
wait_for "$MQ_HOST" "$MQ_PORT" "rabbitmq"
wait_for "$REDIS_H" "$REDIS_P" "redis"

case "$1" in
  api)
    echo "[entrypoint] running migrations..."
    alembic upgrade head
    echo "[entrypoint] starting API..."
    exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --proxy-headers
    ;;
  worker)
    echo "[entrypoint] starting worker..."
    exec python -m app.worker
    ;;
  migrate)
    exec alembic upgrade head
    ;;
  test)
    shift
    exec pytest "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
