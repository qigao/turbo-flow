#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

: "${FLOWIE_CONFIG:=/etc/flowie/flowie.yml}"
: "${FLOWIE_GRAPH:=/etc/flowie/flowie.flow}"
: "${FLOWIE_CONTROL_CONFIG:=/etc/flowie/control.yml}"
: "${FLOWIE_PROTOCOL_STORE_PATH:=:memory:}"
: "${FLOWIE_PROFILE:=flowie}"

for required_file in "$FLOWIE_CONFIG" "$FLOWIE_GRAPH" "$FLOWIE_CONTROL_CONFIG"; do
  if [ ! -r "$required_file" ]; then
    echo "flowie entrypoint: required file is not readable: $required_file" >&2
    exit 66
  fi
done

if [ "$FLOWIE_PROTOCOL_STORE_PATH" != ":memory:" ]; then
  echo "flowie entrypoint: standalone MQTT protocol store must be :memory:" >&2
  exit 64
fi

set -- \
  flowie_server \
  --require-security \
  --profile "$FLOWIE_PROFILE" \
  --control-config "$FLOWIE_CONTROL_CONFIG" \
  --protocol-store-path "$FLOWIE_PROTOCOL_STORE_PATH"

if [ -n "${FLOWIE_STORAGE_BACKEND_PLUGINS:-}" ]; then
  old_ifs=$IFS
  IFS=:
  for plugin_path in $FLOWIE_STORAGE_BACKEND_PLUGINS; do
    if [ -z "$plugin_path" ] || [ ! -r "$plugin_path" ]; then
      echo "flowie entrypoint: storage backend plugin is not readable: $plugin_path" >&2
      exit 66
    fi
    set -- "$@" --storage-backend-plugin "$plugin_path"
  done
  IFS=$old_ifs
fi

set -- "$@" "$FLOWIE_CONFIG" "$FLOWIE_GRAPH"
exec "$@"
