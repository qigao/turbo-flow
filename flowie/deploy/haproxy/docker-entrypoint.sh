#!/bin/sh
set -eu

fail() {
    echo "flowie-haproxy: $*" >&2
    exit 1
}

require_uint_range() {
    name=$1
    value=$2
    minimum=$3
    maximum=$4
    case "$value" in
        ''|*[!0-9]*) fail "$name must be an unsigned integer" ;;
    esac
    if [ "$value" -lt "$minimum" ] || [ "$value" -gt "$maximum" ]; then
        fail "$name must be in [$minimum, $maximum]"
    fi
}

: "${FLOWIE_PUBLIC_HOST:?set FLOWIE_PUBLIC_HOST}"
: "${FLOWIE_MQTT_BACKENDS:?set FLOWIE_MQTT_BACKENDS}"

case "$FLOWIE_PUBLIC_HOST" in
    ''|*[!A-Za-z0-9.-]*) fail "FLOWIE_PUBLIC_HOST contains invalid characters" ;;
esac

FLOWIE_MQTT_PUBLIC_PORT=${FLOWIE_MQTT_PUBLIC_PORT:-8883}
FLOWIE_MQTT_INSPECT_DELAY_MS=${FLOWIE_MQTT_INSPECT_DELAY_MS:-5000}
FLOWIE_MQTT_INSPECTION_BUFFER_BYTES=${FLOWIE_MQTT_INSPECTION_BUFFER_BYTES:-32768}
FLOWIE_MQTT_CONNECT_TIMEOUT_MS=${FLOWIE_MQTT_CONNECT_TIMEOUT_MS:-5000}
FLOWIE_MQTT_IDLE_TIMEOUT_SECONDS=${FLOWIE_MQTT_IDLE_TIMEOUT_SECONDS:-3600}
FLOWIE_HAPROXY_CERT_POLL_SECONDS=${FLOWIE_HAPROXY_CERT_POLL_SECONDS:-300}

require_uint_range FLOWIE_MQTT_PUBLIC_PORT "$FLOWIE_MQTT_PUBLIC_PORT" 1 65535
require_uint_range FLOWIE_MQTT_INSPECT_DELAY_MS "$FLOWIE_MQTT_INSPECT_DELAY_MS" 1 60000
require_uint_range FLOWIE_MQTT_INSPECTION_BUFFER_BYTES \
    "$FLOWIE_MQTT_INSPECTION_BUFFER_BYTES" 16384 262144
require_uint_range FLOWIE_MQTT_CONNECT_TIMEOUT_MS "$FLOWIE_MQTT_CONNECT_TIMEOUT_MS" 1 60000
require_uint_range FLOWIE_MQTT_IDLE_TIMEOUT_SECONDS \
    "$FLOWIE_MQTT_IDLE_TIMEOUT_SECONDS" 1 604800
require_uint_range FLOWIE_HAPROXY_CERT_POLL_SECONDS \
    "$FLOWIE_HAPROXY_CERT_POLL_SECONDS" 10 86400

certificate_directory=/etc/letsencrypt/live/$FLOWIE_PUBLIC_HOST
certificate_file=$certificate_directory/fullchain.pem
key_file=$certificate_directory/privkey.pem
[ -s "$certificate_file" ] || fail "public certificate is missing: $certificate_file"
[ -s "$key_file" ] || fail "public private key is missing: $key_file"

runtime_directory=/run/flowie-haproxy
generated_config=$runtime_directory/haproxy.cfg
mkdir -p "$runtime_directory"
substitutions='${FLOWIE_PUBLIC_HOST} ${FLOWIE_MQTT_PUBLIC_PORT} ${FLOWIE_MQTT_INSPECT_DELAY_MS} ${FLOWIE_MQTT_INSPECTION_BUFFER_BYTES} ${FLOWIE_MQTT_CONNECT_TIMEOUT_MS} ${FLOWIE_MQTT_IDLE_TIMEOUT_SECONDS}'
envsubst "$substitutions" \
    < /etc/flowie-haproxy/haproxy.cfg.template \
    > "$generated_config"

backend_count=0
previous_ifs=$IFS
IFS=,
for backend in $FLOWIE_MQTT_BACKENDS; do
    backend_count=$((backend_count + 1))
    [ "$backend_count" -le 32 ] || fail "FLOWIE_MQTT_BACKENDS exceeds 32 nodes"
    if ! printf '%s\n' "$backend" | grep -Eq '^([A-Za-z0-9_.-]+:[0-9]+|\[[0-9A-Fa-f:.]+\]:[0-9]+)$'; then
        fail "invalid MQTT backend address: $backend"
    fi
    printf '    server flowie-%02d %s check send-proxy\n' "$backend_count" "$backend" \
        >> "$generated_config"
done
IFS=$previous_ifs
[ "$backend_count" -gt 0 ] || fail "FLOWIE_MQTT_BACKENDS contains no nodes"

haproxy -c -f "$generated_config"

certificate_fingerprint() {
    sha256sum "$certificate_file" "$key_file" | sha256sum | awk '{print $1}'
}

watch_certificates() {
    previous_fingerprint=$(certificate_fingerprint)
    while sleep "$FLOWIE_HAPROXY_CERT_POLL_SECONDS"; do
        current_fingerprint=$(certificate_fingerprint) || continue
        [ "$current_fingerprint" != "$previous_fingerprint" ] || continue
        if haproxy -c -f "$generated_config" && kill -USR2 1; then
            previous_fingerprint=$current_fingerprint
            echo "flowie-haproxy: reloaded renewed MQTT certificate" >&2
        else
            echo "flowie-haproxy: renewed certificate validation or reload failed" >&2
        fi
    done
}

watch_certificates &
exec "$@"
