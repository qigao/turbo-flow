#!/bin/sh
set -eu

: "${FLOWIE_HEALTH_HOST:=127.0.0.1}"
: "${FLOWIE_HEALTH_PORT:=18883}"

case "$FLOWIE_HEALTH_PORT" in
  ''|*[!0-9]*)
    echo "flowie healthcheck: FLOWIE_HEALTH_PORT must be numeric" >&2
    exit 2
    ;;
esac

if [ "$FLOWIE_HEALTH_PORT" -lt 1 ] || [ "$FLOWIE_HEALTH_PORT" -gt 65535 ]; then
  echo "flowie healthcheck: FLOWIE_HEALTH_PORT is outside 1..65535" >&2
  exit 2
fi

kill -0 1
nc -z -w 3 "$FLOWIE_HEALTH_HOST" "$FLOWIE_HEALTH_PORT"
