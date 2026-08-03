#!/bin/sh
set -eu

required_vars="FLOWIE_PUBLIC_HOST FLOWIE_CONTROL_UPSTREAM FLOWIE_CONTROL_TLS_NAME FLOWIE_CERTBOT_EMAIL"
for variable in $required_vars; do
    eval "value=\${$variable:-}"
    if [ -z "$value" ]; then
        echo "flowie-nginx: required environment variable is unset: $variable" >&2
        exit 1
    fi
done

certificate_directory="/etc/letsencrypt/live/$FLOWIE_PUBLIC_HOST"
if [ ! -s "$certificate_directory/fullchain.pem" ] ||
   [ ! -s "$certificate_directory/privkey.pem" ]; then
    echo "flowie-nginx: public certificate is missing for $FLOWIE_PUBLIC_HOST" >&2
    echo "flowie-nginx: run bootstrap-cert.sh before starting the service" >&2
    exit 1
fi
if [ ! -s /etc/flowie/internal-ca.pem ]; then
    echo "flowie-nginx: internal CA file is missing" >&2
    exit 1
fi

mkdir -p /etc/flowie-nginx/generated /etc/flowie-nginx/http.d /var/www/certbot
substitutions='${FLOWIE_PUBLIC_HOST} ${FLOWIE_CONTROL_UPSTREAM} ${FLOWIE_CONTROL_TLS_NAME}'
envsubst "$substitutions" \
    < /etc/flowie-nginx/templates/http.conf.template \
    > /etc/flowie-nginx/generated/http.conf

nginx -t

renew_interval=${FLOWIE_CERTBOT_RENEW_INTERVAL:-12h}
(
    while sleep "$renew_interval"; do
        certbot renew --quiet --cert-name "$FLOWIE_PUBLIC_HOST" \
            --webroot -w /var/www/certbot \
            --deploy-hook 'nginx -s reload' ||
            echo "flowie-nginx: certificate renewal failed" >&2
    done
) &

exec "$@"
