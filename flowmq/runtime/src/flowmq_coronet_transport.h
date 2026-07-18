#ifndef FLOWMQ_CORONET_TRANSPORT_H
#define FLOWMQ_CORONET_TRANSPORT_H

#include "CoroNet/turbo_coro_socket.h"
#include "flow_coronet_runtime.h"

typedef enum flowmq_coronet_transport_e {
  FLOWMQ_TRANSPORT_TCP = 1,
  FLOWMQ_TRANSPORT_TLS,
  FLOWMQ_TRANSPORT_UDP,
  FLOWMQ_TRANSPORT_KCP,
  FLOWMQ_TRANSPORT_PIPE,
  FLOWMQ_TRANSPORT_WS,
  FLOWMQ_TRANSPORT_WSS
} flowmq_coronet_transport_t;

int flowmq_coronet_transport_validate(flowmq_coronet_transport_t transport);
tf_coronet_transport_t flowmq_coronet_transport_coronet(flowmq_coronet_transport_t transport);
int flowmq_coronet_transport_kcp_fec_resolve(flowmq_coronet_transport_t transport,
                                             const tf_coronet_kcp_fec_options_t *options,
                                             turbo_kcp_fec_config_t *config, int *configured);
coro_socket_t *flowmq_coronet_transport_create(coro_context_t *ctx,
                                               flowmq_coronet_transport_t transport, int listener);
int flowmq_coronet_transport_apply(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const turbo_kcp_fec_config_t *kcp_fec, int kcp_fec_configured,
                                   const tf_coronet_socket_options_t *socket_options);
int flowmq_coronet_transport_connect(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                     const char *host, int port, const char *path,
                                     const tf_coronet_socket_timeout_config_t *timeouts,
                                     const tf_coronet_udp_options_t *udp_options);
int flowmq_coronet_transport_listen(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                    const char *host, int port, const char *path,
                                    const tf_coronet_socket_timeout_config_t *timeouts,
                                    const tf_coronet_udp_options_t *udp_options, int reuse_port,
                                    coro_handler_fn handler, void *ctx);
int flowmq_coronet_transport_send(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                  const tf_coronet_socket_timeout_config_t *timeouts,
                                  const char *data, size_t len);
int flowmq_coronet_transport_sendv(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const tf_coronet_socket_timeout_config_t *timeouts,
                                   const turbo_iovec_t *iov, size_t iovcnt);
int flowmq_coronet_transport_leave_multicast(coro_socket_t *socket,
                                             flowmq_coronet_transport_t transport,
                                             const tf_coronet_udp_options_t *udp_options);

#endif /* FLOWMQ_CORONET_TRANSPORT_H */
