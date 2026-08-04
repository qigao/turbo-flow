#!/bin/sh
set -eu

deploy_dir=/root/dev/flowie-eu/server-docker
device_env=/root/code/picimpact/picimpact-e2e-device.env
credentials_env="$deploy_dir/control-admin-credentials.env"
ca_file="$deploy_dir/certs/control-ca.crt"
origin=https://127.0.0.1:8443
cookie_file=$(mktemp "$deploy_dir/.device-token-cookie.XXXXXX")
response_file=$(mktemp "$deploy_dir/.device-token-response.XXXXXX")
token_file=$(mktemp "$deploy_dir/.device-token-value.XXXXXX")
next_env=$(mktemp /root/code/picimpact/.device-env.XXXXXX)

cleanup() {
  if command -v shred >/dev/null 2>&1; then
    shred -u "$cookie_file" "$response_file" "$token_file" "$next_env" 2>/dev/null || true
  else
    rm -f "$cookie_file" "$response_file" "$token_file" "$next_env"
  fi
}
trap cleanup EXIT HUP INT TERM
umask 077

set -a
. "$credentials_env"
set +a
: "${FLOWIE_BOOTH_ADMIN_PASSWORD:?missing booth administrator password}"

login_status=$(curl --silent --show-error --cacert "$ca_file" \
  --cookie-jar "$cookie_file" --output /dev/null --write-out '%{http_code}' \
  --request POST "$origin/v2/control/login" \
  --header "Origin: $origin" \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'domain=booth' \
  --data-urlencode 'principal=booth' \
  --data-urlencode "password=$FLOWIE_BOOTH_ADMIN_PASSWORD")
[ "$login_status" = 303 ] || {
  echo "device token rotation: management login failed with HTTP $login_status" >&2
  exit 1
}

session_token=$(awk '$6 == "flowie_session" { print $7 }' "$cookie_file")
[ -n "$session_token" ] || {
  echo 'device token rotation: management session cookie is missing' >&2
  exit 1
}

rpc_status=$(curl --silent --show-error --cacert "$ca_file" \
  --output "$response_file" --write-out '%{http_code}' \
  --request POST "$origin/v2/control/rpc" \
  --header "Origin: $origin" \
  --header 'Content-Type: application/json' \
  --header "Authorization: Bearer $session_token" \
  --data '{"jsonrpc":"2.0","method":"control.credential.rotate","params":{"principal_id":"picimpact-e2e-device","request_id":"device-token-v1-20260804"},"id":"device-token-v1-20260804"}')
[ "$rpc_status" = 200 ] || {
  echo "device token rotation: RPC failed with HTTP $rpc_status" >&2
  exit 1
}

python3 -c 'import json,re,sys
document=json.load(open(sys.argv[1], encoding="utf-8"))
if document.get("error") is not None:
    raise SystemExit("device token rotation: RPC returned an error")
token=document.get("result", {}).get("token")
if not isinstance(token, str) or re.fullmatch(r"flw_mqtt_v1_[A-Za-z0-9_-]{43}", token) is None:
    raise SystemExit("device token rotation: RPC returned an invalid token")
with open(sys.argv[2], "w", encoding="ascii", newline="") as output:
    output.write(token)
' "$response_file" "$token_file"

username_line=$(sed -n '/^FLOWIE_MQTT_USERNAME=/p' "$device_env")
[ -n "$username_line" ] || {
  echo 'device token rotation: MQTT username is missing' >&2
  exit 1
}
[ "$(printf '%s\n' "$username_line" | wc -l)" -eq 1 ] || {
  echo 'device token rotation: duplicate MQTT username entries' >&2
  exit 1
}

{
  printf '%s\n' "$username_line"
  printf 'FLOWIE_DEVICE_TOKEN='
  tr -d '\r\n' < "$token_file"
  printf '\n'
} > "$next_env"
chmod 0600 "$next_env"
mv "$next_env" "$device_env"

echo 'device token rotation: completed'
