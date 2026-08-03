FROM haproxy:3.2-alpine

USER root

RUN apk add --no-cache gettext

COPY haproxy.cfg.template /etc/flowie-haproxy/haproxy.cfg.template
COPY docker-entrypoint.sh /usr/local/bin/flowie-haproxy-entrypoint

RUN chmod 0755 /usr/local/bin/flowie-haproxy-entrypoint

ENTRYPOINT ["/usr/local/bin/flowie-haproxy-entrypoint"]
CMD ["haproxy", "-W", "-db", "-f", "/run/flowie-haproxy/haproxy.cfg"]
