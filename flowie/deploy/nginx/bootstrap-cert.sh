#!/bin/sh
set -eu

: "${FLOWIE_PUBLIC_HOST:?set FLOWIE_PUBLIC_HOST}"
: "${FLOWIE_CERTBOT_EMAIL:?set FLOWIE_CERTBOT_EMAIL}"

lets_encrypt_dir=${FLOWIE_LETSENCRYPT_DIR:-./state/letsencrypt}
mkdir -p "$lets_encrypt_dir"
lets_encrypt_dir=$(cd "$lets_encrypt_dir" && pwd)

docker compose -f compose.yml build flowie-nginx
docker run --rm --network host \
    --entrypoint certbot \
    -v "$lets_encrypt_dir:/etc/letsencrypt" \
    flowie-nginx:local certonly --standalone --non-interactive --agree-tos \
    --email "$FLOWIE_CERTBOT_EMAIL" -d "$FLOWIE_PUBLIC_HOST"
