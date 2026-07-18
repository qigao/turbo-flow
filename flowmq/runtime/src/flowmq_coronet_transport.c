#include "flowmq_coronet_transport.h"

#include "turbo_error.h"

int flowmq_coronet_transport_validate(flowmq_coronet_transport_t transport) {
  return transport >= FLOWMQ_TRANSPORT_TCP && transport <= FLOWMQ_TRANSPORT_WSS ? TURBO_OK
                                                                                : TURBO_EINVAL;
}

tf_coronet_transport_t flowmq_coronet_transport_coronet(flowmq_coronet_transport_t transport) {
  switch (transport) {
  case FLOWMQ_TRANSPORT_TCP:
    return TF_CORONET_TRANSPORT_TCP;
  case FLOWMQ_TRANSPORT_TLS:
    return TF_CORONET_TRANSPORT_TLS;
  case FLOWMQ_TRANSPORT_UDP:
    return TF_CORONET_TRANSPORT_UDP;
  case FLOWMQ_TRANSPORT_KCP:
    return TF_CORONET_TRANSPORT_KCP;
  case FLOWMQ_TRANSPORT_PIPE:
    return TF_CORONET_TRANSPORT_PIPE;
  case FLOWMQ_TRANSPORT_WS:
    return TF_CORONET_TRANSPORT_WS;
  case FLOWMQ_TRANSPORT_WSS:
    return TF_CORONET_TRANSPORT_WSS;
  default:
    return TF_CORONET_TRANSPORT_COUNT;
  }
}

int flowmq_coronet_transport_kcp_fec_resolve(flowmq_coronet_transport_t transport,
                                             const tf_coronet_kcp_fec_options_t *options,
                                             turbo_kcp_fec_config_t *config, int *configured) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  if (coronet == TF_CORONET_TRANSPORT_COUNT) return TURBO_EINVAL;
  return tf_coronet_kcp_fec_options_resolve(coronet, options, config, configured);
}

coro_socket_t *flowmq_coronet_transport_create(coro_context_t *ctx,
                                               flowmq_coronet_transport_t transport, int listener) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  if (!ctx || (listener != 0 && listener != 1) || coronet == TF_CORONET_TRANSPORT_COUNT)
    return NULL;
  return listener ? tf_coronet_create_server_socket(ctx, coronet)
                  : tf_coronet_create_client_socket(ctx, coronet);
}

int flowmq_coronet_transport_apply(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const turbo_kcp_fec_config_t *kcp_fec, int kcp_fec_configured,
                                   const tf_coronet_socket_options_t *socket_options) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  int rc;
  if (!socket || !socket_options || coronet == TF_CORONET_TRANSPORT_COUNT) return TURBO_EINVAL;
  rc = tf_coronet_apply_kcp_fec(socket, coronet, kcp_fec, kcp_fec_configured);
  if (rc != TURBO_OK) return rc;
  return tf_coronet_apply_socket_options(socket, coronet, socket_options);
}

int flowmq_coronet_transport_connect(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                     const char *host, int port, const char *path,
                                     const tf_coronet_socket_timeout_config_t *timeouts,
                                     const tf_coronet_udp_options_t *udp_options) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  uint64_t timeout_ms = 0u;
  int rc;
  if (!socket || !timeouts || !udp_options || coronet == TF_CORONET_TRANSPORT_COUNT)
    return TURBO_EINVAL;
  rc = tf_coronet_connection_timeout_resolve(coronet, timeouts, &timeout_ms);
  if (rc != TURBO_OK) return rc;
  (void)coro_socket_set_timeout(socket, timeout_ms);
  rc = tf_coronet_connect_socket(socket, coronet, host, port, path);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_apply_udp_options(socket, coronet, udp_options);
  if (rc != TURBO_OK) return rc;
  (void)tf_coronet_apply_socket_timeout(socket, timeouts, TF_CORONET_TIMEOUT_RECV);
  return TURBO_OK;
}

int flowmq_coronet_transport_listen(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                    const char *host, int port, const char *path,
                                    const tf_coronet_socket_timeout_config_t *timeouts,
                                    const tf_coronet_udp_options_t *udp_options, int reuse_port,
                                    coro_handler_fn handler, void *ctx) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  int rc;
  if (!socket || !timeouts || !udp_options || !handler || coronet == TF_CORONET_TRANSPORT_COUNT)
    return TURBO_EINVAL;
  if (reuse_port) coro_socket_set_reuse_port(socket, 1);
  (void)tf_coronet_apply_socket_timeout(socket, timeouts, TF_CORONET_TIMEOUT_RECV);
  rc = tf_coronet_listen_socket(socket, coronet, host, port, path, handler, ctx);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_apply_udp_options(socket, coronet, udp_options);
  if (rc != TURBO_OK) return rc;
  return tf_coronet_join_multicast(socket, coronet, udp_options);
}

int flowmq_coronet_transport_send(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                  const tf_coronet_socket_timeout_config_t *timeouts,
                                  const char *data, size_t len) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  if (!socket || !timeouts || !data || len == 0u || coronet == TF_CORONET_TRANSPORT_COUNT)
    return TURBO_EINVAL;
  (void)tf_coronet_apply_socket_timeout(socket, timeouts, TF_CORONET_TIMEOUT_SEND);
  return tf_coronet_send_socket(socket, coronet, data, len);
}

int flowmq_coronet_transport_sendv(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const tf_coronet_socket_timeout_config_t *timeouts,
                                   const turbo_iovec_t *iov, size_t iovcnt) {
  if (!socket || !timeouts || !iov || iovcnt == 0u) return TURBO_EINVAL;
  if (transport != FLOWMQ_TRANSPORT_TCP) return TURBO_ENOTSUP;
  (void)tf_coronet_apply_socket_timeout(socket, timeouts, TF_CORONET_TIMEOUT_SEND);
  return coro_socket_sendv(socket, iov, iovcnt);
}

int flowmq_coronet_transport_leave_multicast(coro_socket_t *socket,
                                             flowmq_coronet_transport_t transport,
                                             const tf_coronet_udp_options_t *udp_options) {
  tf_coronet_transport_t coronet = flowmq_coronet_transport_coronet(transport);
  if (!socket || !udp_options || coronet == TF_CORONET_TRANSPORT_COUNT) return TURBO_EINVAL;
  return tf_coronet_leave_multicast(socket, coronet, udp_options);
}
