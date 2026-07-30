FROM nginx:1.27-alpine

RUN apk add --no-cache certbot

COPY nginx.conf /etc/nginx/nginx.conf
COPY templates /etc/flowie-nginx/templates
COPY docker-entrypoint.sh /usr/local/bin/flowie-nginx-entrypoint

RUN chmod 0755 /usr/local/bin/flowie-nginx-entrypoint

ENTRYPOINT ["/usr/local/bin/flowie-nginx-entrypoint"]
CMD ["nginx", "-g", "daemon off;"]
