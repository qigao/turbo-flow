#ifndef FLOW_CORONET_RUNTIME_H
#define FLOW_CORONET_RUNTIME_H

#include "CoroNet/turbo_coro_socket.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum tf_coronet_transport_e {
  TF_CORONET_TRANSPORT_TCP = 0,
  TF_CORONET_TRANSPORT_UDP,
  TF_CORONET_TRANSPORT_KCP,
  TF_CORONET_TRANSPORT_TLS,
  TF_CORONET_TRANSPORT_WS,
  TF_CORONET_TRANSPORT_WSS,
  TF_CORONET_TRANSPORT_PIPE,
  TF_CORONET_TRANSPORT_COUNT
} tf_coronet_transport_t;

#define TF_CORONET_TRANSPORT_VALUE_COUNT 7u

#define TF_CORONET_OPTION_TIMEOUT_MS "timeout_ms"
#define TF_CORONET_OPTION_CONNECT_TIMEOUT_MS "connect_timeout_ms"
#define TF_CORONET_OPTION_SEND_TIMEOUT_MS "send_timeout_ms"
#define TF_CORONET_OPTION_RECV_TIMEOUT_MS "recv_timeout_ms"
#define TF_CORONET_OPTION_HANDSHAKE_TIMEOUT_MS "handshake_timeout_ms"

extern const char *const TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_VALUE_COUNT];

typedef struct tf_coronet_socket_timeout_config_s {
  uint64_t timeout_ms;
  uint64_t connect_timeout_ms;
  uint64_t send_timeout_ms;
  uint64_t recv_timeout_ms;
  uint64_t handshake_timeout_ms;
  uint32_t set_flags;
  uint32_t explicit_flags;
} tf_coronet_socket_timeout_config_t;

typedef enum tf_coronet_timeout_set_flag_e {
  TF_CORONET_TIMEOUT_SET_DEFAULT = 1u << 0,
  TF_CORONET_TIMEOUT_SET_CONNECT = 1u << 1,
  TF_CORONET_TIMEOUT_SET_SEND = 1u << 2,
  TF_CORONET_TIMEOUT_SET_RECV = 1u << 3,
  TF_CORONET_TIMEOUT_SET_HANDSHAKE = 1u << 4
} tf_coronet_timeout_set_flag_t;

#define TF_CORONET_TIMEOUT_SET_ALL ((1u << 5) - 1u)

typedef struct tf_coronet_kcp_options_s {
  uint8_t pre_shared_key[TURBO_KCP_PSK_SIZE];
  uint32_t mtu;
  uint32_t send_window;
  uint32_t receive_window;
  uint32_t interval_ms;
  uint32_t handshake_retry_ms;
  uint32_t fast_resend;
  int no_congestion_window;
  uint32_t data_shards;
  uint32_t parity_shards;
  uint32_t max_payload_size;
  uint32_t receive_group_count;
} tf_coronet_kcp_options_t;

typedef struct tf_coronet_socket_options_s {
  int tcp_keepalive;
  uint64_t tcp_keepalive_idle_ms;
  uint64_t tcp_keepalive_interval_ms;
  uint32_t tcp_keepalive_count;
  int linger;
  uint64_t linger_ms;
  size_t send_hwm_bytes;
  size_t socket_recv_buffer_bytes;
  size_t socket_send_buffer_bytes;
} tf_coronet_socket_options_t;

typedef enum tf_coronet_udp_option_flag_e {
  TF_CORONET_UDP_OPTION_MULTICAST_LOOP = 1u << 0,
  TF_CORONET_UDP_OPTION_MULTICAST_TTL = 1u << 1,
  TF_CORONET_UDP_OPTION_BROADCAST = 1u << 2
} tf_coronet_udp_option_flag_t;

#define TF_CORONET_UDP_OPTION_ALL ((1u << 3) - 1u)

typedef struct tf_coronet_udp_options_s {
  const char *multicast_group;
  const char *multicast_interface;
  uint32_t option_flags;
  int multicast_loop;
  uint32_t multicast_ttl;
  int broadcast;
} tf_coronet_udp_options_t;

typedef enum tf_coronet_timeout_kind_e {
  TF_CORONET_TIMEOUT_CONNECT,
  TF_CORONET_TIMEOUT_SEND,
  TF_CORONET_TIMEOUT_RECV,
  TF_CORONET_TIMEOUT_HANDSHAKE
} tf_coronet_timeout_kind_t;

int tf_coronet_transport_valid(tf_coronet_transport_t transport);
int tf_coronet_transport_is_ws(tf_coronet_transport_t transport);
int tf_coronet_transport_is_pipe(tf_coronet_transport_t transport);
int tf_coronet_endpoint_config_validate(tf_coronet_transport_t transport, const char *host,
                                        int port, const char *path);
int tf_coronet_kcp_pre_shared_key_parse(const char *hex,
                                        uint8_t out[TURBO_KCP_PSK_SIZE]);
int tf_coronet_kcp_options_resolve(tf_coronet_transport_t transport,
                                   const tf_coronet_kcp_options_t *options,
                                   turbo_kcp_config_t *config, int *configured);
int tf_coronet_socket_options_validate(tf_coronet_transport_t transport,
                                       const tf_coronet_socket_options_t *options);
int tf_coronet_reuse_port_validate(tf_coronet_transport_t transport, int reuse_port,
                                   int listener);
int tf_coronet_udp_options_validate(tf_coronet_transport_t transport,
                                    const tf_coronet_udp_options_t *options, int allow_join);
const char *tf_coronet_endpoint(const char *host, const char *path);
const char *tf_coronet_ws_path(const char *path);
coro_socket_t *tf_coronet_apply_socket_timeout(coro_socket_t *socket,
                                               const tf_coronet_socket_timeout_config_t *timeouts,
                                               tf_coronet_timeout_kind_t timeout_kind);
int tf_coronet_apply_kcp_config(coro_socket_t *socket,
                                tf_coronet_transport_t transport,
                                const turbo_kcp_config_t *config,
                                int configured);
int tf_coronet_apply_socket_options(coro_socket_t *socket, tf_coronet_transport_t transport,
                                    const tf_coronet_socket_options_t *options);
int tf_coronet_apply_udp_options(coro_socket_t *socket, tf_coronet_transport_t transport,
                                 const tf_coronet_udp_options_t *options);
int tf_coronet_join_multicast(coro_socket_t *socket, tf_coronet_transport_t transport,
                              const tf_coronet_udp_options_t *options);
int tf_coronet_leave_multicast(coro_socket_t *socket, tf_coronet_transport_t transport,
                               const tf_coronet_udp_options_t *options);
coro_socket_t *tf_coronet_create_client_socket(coro_context_t *ctx,
                                               tf_coronet_transport_t transport);
coro_socket_t *tf_coronet_create_server_socket(coro_context_t *ctx,
                                               tf_coronet_transport_t transport);
int tf_coronet_connect_socket(coro_socket_t *socket, tf_coronet_transport_t transport,
                              const char *host, int port, const char *path);
int tf_coronet_connect_socket_ex(coro_socket_t *socket, tf_coronet_transport_t transport,
                                 const char *connect_host, int port, const char *path,
                                 const char *request_host);
int tf_coronet_listen_socket(coro_socket_t *socket, tf_coronet_transport_t transport,
                             const char *host, int port, const char *path, coro_handler_fn handler,
                             void *arg);
int tf_coronet_send_socket(coro_socket_t *socket, tf_coronet_transport_t transport,
                           const char *data, size_t len);
void tf_coronet_socket_timeouts_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                        uint64_t fallback_timeout_ms);
int tf_coronet_connection_timeout_resolve(tf_coronet_transport_t transport,
                                          const tf_coronet_socket_timeout_config_t *timeouts,
                                          uint64_t *timeout_ms);

#ifdef __cplusplus
}
#endif

#endif
