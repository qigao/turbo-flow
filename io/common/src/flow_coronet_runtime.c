#include "flow_coronet_runtime.h"

#include "turbo_error.h"

#include <limits.h>
#include <string.h>

const char *const TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_VALUE_COUNT] = {
    "tcp", "udp", "kcp", "tls", "ws", "wss", "pipe"};

int tf_coronet_transport_valid(tf_coronet_transport_t transport) {
  return transport >= TF_CORONET_TRANSPORT_TCP && transport < TF_CORONET_TRANSPORT_COUNT;
}

int tf_coronet_transport_is_ws(tf_coronet_transport_t transport) {
  return transport == TF_CORONET_TRANSPORT_WS || transport == TF_CORONET_TRANSPORT_WSS;
}

int tf_coronet_transport_is_pipe(tf_coronet_transport_t transport) {
  return transport == TF_CORONET_TRANSPORT_PIPE;
}

static int tf_coronet_transport_is_tcp_backed(tf_coronet_transport_t transport) {
  return transport == TF_CORONET_TRANSPORT_TCP || transport == TF_CORONET_TRANSPORT_TLS ||
         transport == TF_CORONET_TRANSPORT_WS || transport == TF_CORONET_TRANSPORT_WSS;
}

static int tf_coronet_transport_supports_send_hwm(tf_coronet_transport_t transport) {
  return tf_coronet_transport_is_tcp_backed(transport) || transport == TF_CORONET_TRANSPORT_PIPE;
}

int tf_coronet_endpoint_config_validate(tf_coronet_transport_t transport, const char *host,
                                        int port, const char *path) {
  if (!tf_coronet_transport_valid(transport)) return TURBO_EINVAL;
  if (tf_coronet_transport_is_pipe(transport)) {
    if ((!path || path[0] == '\0') && (!host || host[0] == '\0')) return TURBO_EINVAL;
    return TURBO_OK;
  }
  if (!host || host[0] == '\0' || port < 1 || port > 65535) return TURBO_EINVAL;
  return TURBO_OK;
}

int tf_coronet_socket_options_validate(tf_coronet_transport_t transport,
                                       const tf_coronet_socket_options_t *options) {
  if (!tf_coronet_transport_valid(transport) || !options) return TURBO_EINVAL;
  if (options->tcp_keepalive) {
    if (!tf_coronet_transport_is_tcp_backed(transport)) return TURBO_EINVAL;
    if (options->tcp_keepalive_idle_ms > UINT32_MAX ||
        options->tcp_keepalive_interval_ms > UINT32_MAX) {
      return TURBO_ERANGE;
    }
  } else if (options->tcp_keepalive_idle_ms != 0 || options->tcp_keepalive_interval_ms != 0 ||
             options->tcp_keepalive_count != 0) {
    return TURBO_EINVAL;
  }
  if (options->linger) {
    if (!tf_coronet_transport_is_tcp_backed(transport)) return TURBO_EINVAL;
    if (options->linger_ms > UINT32_MAX) return TURBO_ERANGE;
  } else if (options->linger_ms != 0) {
    return TURBO_EINVAL;
  }
  if (options->send_hwm_bytes != 0 && !tf_coronet_transport_supports_send_hwm(transport)) {
    return TURBO_EINVAL;
  }
  if ((options->socket_recv_buffer_bytes != 0 || options->socket_send_buffer_bytes != 0) &&
      !tf_coronet_transport_is_tcp_backed(transport)) {
    return TURBO_EINVAL;
  }
  if (options->socket_recv_buffer_bytes > (size_t)INT_MAX ||
      options->socket_send_buffer_bytes > (size_t)INT_MAX) {
    return TURBO_ERANGE;
  }
  return TURBO_OK;
}

int tf_coronet_reuse_port_validate(tf_coronet_transport_t transport, int reuse_port,
                                   int listener) {
  if (!tf_coronet_transport_valid(transport)) return TURBO_EINVAL;
  if (!reuse_port) return TURBO_OK;
  if (!listener || transport == TF_CORONET_TRANSPORT_PIPE) return TURBO_EINVAL;
  return TURBO_OK;
}

int tf_coronet_udp_options_validate(tf_coronet_transport_t transport,
                                    const tf_coronet_udp_options_t *options, int allow_join) {
  int has_group;
  int has_interface;
  if (!tf_coronet_transport_valid(transport) || !options) return TURBO_EINVAL;
  has_group = options->multicast_group && options->multicast_group[0] != '\0';
  has_interface = options->multicast_interface && options->multicast_interface[0] != '\0';
  if (options->option_flags & ~TF_CORONET_UDP_OPTION_ALL) return TURBO_EINVAL;
  if ((has_group || has_interface || options->option_flags != 0u) &&
      transport != TF_CORONET_TRANSPORT_UDP) {
    return TURBO_EINVAL;
  }
  if (has_interface && !has_group) return TURBO_EINVAL;
  if (has_group && !allow_join) return TURBO_EINVAL;
  if ((options->option_flags & TF_CORONET_UDP_OPTION_MULTICAST_LOOP) == 0u &&
      options->multicast_loop != 0) {
    return TURBO_EINVAL;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_MULTICAST_TTL) == 0u &&
      options->multicast_ttl != 0u) {
    return TURBO_EINVAL;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_BROADCAST) == 0u &&
      options->broadcast != 0) {
    return TURBO_EINVAL;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_MULTICAST_LOOP) != 0u &&
      options->multicast_loop != 0 && options->multicast_loop != 1) {
    return TURBO_EINVAL;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_MULTICAST_TTL) != 0u &&
      options->multicast_ttl > 255u) {
    return TURBO_ERANGE;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_BROADCAST) != 0u &&
      options->broadcast != 0 && options->broadcast != 1) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int tf_coronet_hex_nibble(char value, uint8_t *out) {
  if (!out) return TURBO_EINVAL;
  if (value >= '0' && value <= '9')
    *out = (uint8_t)(value - '0');
  else if (value >= 'a' && value <= 'f')
    *out = (uint8_t)(value - 'a' + 10);
  else if (value >= 'A' && value <= 'F')
    *out = (uint8_t)(value - 'A' + 10);
  else
    return TURBO_EINVAL;
  return TURBO_OK;
}

int tf_coronet_kcp_pre_shared_key_parse(const char *hex,
                                        uint8_t out[TURBO_KCP_PSK_SIZE]) {
  size_t i;
  if (!hex || !out || strlen(hex) != TURBO_KCP_PSK_SIZE * 2U) return TURBO_EINVAL;
  for (i = 0u; i < TURBO_KCP_PSK_SIZE; ++i) {
    uint8_t high;
    uint8_t low;
    if (tf_coronet_hex_nibble(hex[i * 2u], &high) != TURBO_OK ||
        tf_coronet_hex_nibble(hex[i * 2u + 1u], &low) != TURBO_OK) {
      memset(out, 0, TURBO_KCP_PSK_SIZE);
      return TURBO_EINVAL;
    }
    out[i] = (uint8_t)((high << 4u) | low);
  }
  return TURBO_OK;
}

int tf_coronet_kcp_options_resolve(tf_coronet_transport_t transport,
                                   const tf_coronet_kcp_options_t *options,
                                   turbo_kcp_config_t *config, int *configured) {
  static const uint64_t KCP_MAX_FEC_STATE_BYTES = UINT64_C(64) * 1024u * 1024u;
  uint8_t key_bits = 0u;
  uint64_t fec_state_bytes;
  uint32_t total_shards;
  int has_options;
  size_t i;
  if (!options || !config || !configured) return TURBO_EINVAL;
  turbo_kcp_config_default(config);
  *configured = 0;
  for (i = 0u; i < TURBO_KCP_PSK_SIZE; ++i)
    key_bits |= options->pre_shared_key[i];
  has_options = key_bits != 0u || options->mtu != 0u ||
                options->send_window != 0u || options->receive_window != 0u ||
                options->interval_ms != 0u || options->handshake_retry_ms != 0u ||
                options->fast_resend != 0u || options->no_congestion_window != 0 ||
                options->data_shards != 0u || options->parity_shards != 0u ||
                options->max_payload_size != 0u ||
                options->receive_group_count != 0u;
  if (transport != TF_CORONET_TRANSPORT_KCP)
    return has_options ? TURBO_EINVAL : TURBO_OK;
  if (key_bits == 0u || options->mtu < 576u || options->mtu > UINT16_MAX ||
      options->send_window == 0u || options->send_window > UINT16_MAX ||
      options->receive_window == 0u || options->receive_window > UINT16_MAX ||
      options->interval_ms == 0u || options->interval_ms > 100u ||
      options->handshake_retry_ms == 0u ||
      options->handshake_retry_ms > UINT16_MAX ||
      options->fast_resend > UINT8_MAX ||
      (options->no_congestion_window != 0 &&
       options->no_congestion_window != 1) ||
      options->data_shards == 0 || options->data_shards > 255u ||
      options->parity_shards == 0 || options->parity_shards > 255u ||
      options->max_payload_size < options->mtu + TURBO_KCP_SECURE_RECORD_OVERHEAD ||
      options->max_payload_size > UINT16_MAX ||
      options->receive_group_count == 0 ||
      options->receive_group_count > 64u) {
    return TURBO_EINVAL;
  }
  total_shards = options->data_shards + options->parity_shards;
  if (total_shards > 255u) return TURBO_EINVAL;
  fec_state_bytes = (uint64_t)total_shards *
                    ((uint64_t)options->max_payload_size + 2u) *
                    options->receive_group_count;
  if (fec_state_bytes > KCP_MAX_FEC_STATE_BYTES) return TURBO_ERANGE;
  memcpy(config->pre_shared_key, options->pre_shared_key,
         TURBO_KCP_PSK_SIZE);
  config->mtu = (uint16_t)options->mtu;
  config->send_window = (uint16_t)options->send_window;
  config->receive_window = (uint16_t)options->receive_window;
  config->interval_ms = (uint16_t)options->interval_ms;
  config->handshake_retry_ms = (uint16_t)options->handshake_retry_ms;
  config->fast_resend = (uint8_t)options->fast_resend;
  config->no_congestion_window =
      (uint8_t)options->no_congestion_window;
  config->fec.backend = TURBO_KCP_FEC_BACKEND_REED_SOLOMON;
  config->fec.data_shards = (uint16_t)options->data_shards;
  config->fec.parity_shards = (uint16_t)options->parity_shards;
  config->fec.max_payload_size = (uint16_t)options->max_payload_size;
  config->fec.receive_group_count =
      (uint16_t)options->receive_group_count;
  *configured = 1;
  return TURBO_OK;
}

const char *tf_coronet_endpoint(const char *host, const char *path) {
  return path && path[0] != '\0' ? path : host;
}

const char *tf_coronet_ws_path(const char *path) { return path && path[0] != '\0' ? path : "/"; }

void tf_coronet_socket_timeouts_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                        uint64_t fallback_timeout_ms) {
  uint64_t default_timeout_ms;
  if (!timeouts) return;
  timeouts->explicit_flags = timeouts->set_flags;
  default_timeout_ms =
      (timeouts->set_flags & TF_CORONET_TIMEOUT_SET_DEFAULT)
          ? timeouts->timeout_ms
          : (timeouts->timeout_ms != 0 ? timeouts->timeout_ms : fallback_timeout_ms);
  timeouts->timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & TF_CORONET_TIMEOUT_SET_CONNECT) &&
      timeouts->connect_timeout_ms == 0) {
    timeouts->connect_timeout_ms = default_timeout_ms;
  }
  if (!(timeouts->set_flags & TF_CORONET_TIMEOUT_SET_SEND) && timeouts->send_timeout_ms == 0) {
    timeouts->send_timeout_ms = default_timeout_ms;
  }
  if (!(timeouts->set_flags & TF_CORONET_TIMEOUT_SET_RECV) && timeouts->recv_timeout_ms == 0) {
    timeouts->recv_timeout_ms = default_timeout_ms;
  }
  if (!(timeouts->set_flags & TF_CORONET_TIMEOUT_SET_HANDSHAKE) &&
      timeouts->handshake_timeout_ms == 0) {
    timeouts->handshake_timeout_ms = default_timeout_ms;
  }
  timeouts->set_flags = TF_CORONET_TIMEOUT_SET_ALL;
}

int tf_coronet_connection_timeout_resolve(tf_coronet_transport_t transport,
                                          const tf_coronet_socket_timeout_config_t *timeouts,
                                          uint64_t *timeout_ms) {
  int connect_explicit;
  int handshake_explicit;
  if (!tf_coronet_transport_valid(transport) || !timeouts || !timeout_ms) return TURBO_EINVAL;
  *timeout_ms = timeouts->connect_timeout_ms;
  connect_explicit = (timeouts->explicit_flags & TF_CORONET_TIMEOUT_SET_CONNECT) != 0;
  handshake_explicit = (timeouts->explicit_flags & TF_CORONET_TIMEOUT_SET_HANDSHAKE) != 0;
  if (transport != TF_CORONET_TRANSPORT_TLS && transport != TF_CORONET_TRANSPORT_WS &&
      transport != TF_CORONET_TRANSPORT_WSS) {
    return handshake_explicit ? TURBO_EINVAL : TURBO_OK;
  }
  if (connect_explicit && handshake_explicit &&
      timeouts->connect_timeout_ms != timeouts->handshake_timeout_ms) {
    return TURBO_EINVAL;
  }
  if (handshake_explicit) *timeout_ms = timeouts->handshake_timeout_ms;
  return TURBO_OK;
}

coro_socket_t *tf_coronet_apply_socket_timeout(coro_socket_t *socket,
                                               const tf_coronet_socket_timeout_config_t *timeouts,
                                               tf_coronet_timeout_kind_t timeout_kind) {
  uint64_t timeout_ms = 0;
  if (!socket || !timeouts) return socket;

  switch (timeout_kind) {
  case TF_CORONET_TIMEOUT_CONNECT:
    timeout_ms = timeouts->connect_timeout_ms;
    break;
  case TF_CORONET_TIMEOUT_SEND:
    timeout_ms = timeouts->send_timeout_ms;
    break;
  case TF_CORONET_TIMEOUT_RECV:
    timeout_ms = timeouts->recv_timeout_ms;
    break;
  case TF_CORONET_TIMEOUT_HANDSHAKE:
    timeout_ms = timeouts->handshake_timeout_ms;
    break;
  default:
    timeout_ms = timeouts->timeout_ms;
    break;
  }

  (void)coro_socket_set_timeout(socket, timeout_ms);
  return socket;
}

int tf_coronet_apply_kcp_config(coro_socket_t *socket,
                                tf_coronet_transport_t transport,
                                const turbo_kcp_config_t *config,
                                int configured) {
  if (!configured) return TURBO_OK;
  if (!socket || !config) return TURBO_EINVAL;
  if (transport != TF_CORONET_TRANSPORT_KCP) return TURBO_EINVAL;
  return coro_socket_set_kcp_config(socket, config);
}

int tf_coronet_apply_socket_options(coro_socket_t *socket, tf_coronet_transport_t transport,
                                    const tf_coronet_socket_options_t *options) {
  int rc;
  if (!socket || !options) return TURBO_EINVAL;
  rc = tf_coronet_socket_options_validate(transport, options);
  if (rc != TURBO_OK) return rc;
  if (options->tcp_keepalive) {
    turbo_tcp_keepalive_config_t keepalive;
    keepalive.enabled = 1;
    keepalive.idle_ms = (uint32_t)options->tcp_keepalive_idle_ms;
    keepalive.interval_ms = (uint32_t)options->tcp_keepalive_interval_ms;
    keepalive.count = options->tcp_keepalive_count;
    rc = coro_socket_set_tcp_keepalive(socket, &keepalive);
    if (rc != TURBO_OK) return rc;
  }
  if (options->linger) {
    turbo_socket_linger_config_t linger;
    linger.enabled = 1;
    linger.timeout_ms = (uint32_t)options->linger_ms;
    rc = coro_socket_set_linger(socket, &linger);
    if (rc != TURBO_OK) return rc;
  }
  if (options->send_hwm_bytes != 0) {
    rc = coro_socket_set_send_hwm(socket, options->send_hwm_bytes);
    if (rc != TURBO_OK) return rc;
  }
  if (options->socket_recv_buffer_bytes != 0) {
    rc = coro_socket_set_recv_buffer_size(socket, options->socket_recv_buffer_bytes);
    if (rc != TURBO_OK) return rc;
  }
  if (options->socket_send_buffer_bytes != 0) {
    rc = coro_socket_set_send_buffer_size(socket, options->socket_send_buffer_bytes);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

int tf_coronet_apply_udp_options(coro_socket_t *socket, tf_coronet_transport_t transport,
                                 const tf_coronet_udp_options_t *options) {
  int rc;
  if (!socket || !options) return TURBO_EINVAL;
  rc = tf_coronet_udp_options_validate(transport, options, 1);
  if (rc != TURBO_OK) return rc;
  if ((options->option_flags & TF_CORONET_UDP_OPTION_MULTICAST_LOOP) != 0u) {
    rc = coro_socket_set_multicast_loop(socket, options->multicast_loop);
    if (rc != TURBO_OK) return rc;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_MULTICAST_TTL) != 0u) {
    rc = coro_socket_set_multicast_ttl(socket, (int)options->multicast_ttl);
    if (rc != TURBO_OK) return rc;
  }
  if ((options->option_flags & TF_CORONET_UDP_OPTION_BROADCAST) != 0u) {
    rc = coro_socket_set_broadcast(socket, options->broadcast);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

int tf_coronet_join_multicast(coro_socket_t *socket, tf_coronet_transport_t transport,
                              const tf_coronet_udp_options_t *options) {
  if (!socket || !options) return TURBO_EINVAL;
  if (!options->multicast_group || options->multicast_group[0] == '\0') return TURBO_OK;
  if (transport != TF_CORONET_TRANSPORT_UDP) return TURBO_EINVAL;
  return coro_socket_join_multicast(socket, options->multicast_group,
                                    options->multicast_interface);
}

int tf_coronet_leave_multicast(coro_socket_t *socket, tf_coronet_transport_t transport,
                               const tf_coronet_udp_options_t *options) {
  if (!socket || !options) return TURBO_EINVAL;
  if (!options->multicast_group || options->multicast_group[0] == '\0') return TURBO_OK;
  if (transport != TF_CORONET_TRANSPORT_UDP) return TURBO_EINVAL;
  return coro_socket_leave_multicast(socket, options->multicast_group,
                                     options->multicast_interface);
}

coro_socket_t *tf_coronet_create_client_socket(coro_context_t *ctx,
                                               tf_coronet_transport_t transport) {
  if (!ctx) return NULL;
  switch (transport) {
  case TF_CORONET_TRANSPORT_TCP:
  case TF_CORONET_TRANSPORT_WS:
    return coro_socket_create_tcpv4(ctx);
  case TF_CORONET_TRANSPORT_UDP:
    return coro_socket_create_udpv4(ctx);
  case TF_CORONET_TRANSPORT_KCP:
    return coro_socket_create_kcp(ctx);
  case TF_CORONET_TRANSPORT_TLS:
  case TF_CORONET_TRANSPORT_WSS:
    return coro_socket_create(ctx, CORO_SOCKET_TLS);
  case TF_CORONET_TRANSPORT_PIPE:
    return coro_socket_create_pipe(ctx);
  default:
    return NULL;
  }
}

coro_socket_t *tf_coronet_create_server_socket(coro_context_t *ctx,
                                               tf_coronet_transport_t transport) {
  if (!ctx) return NULL;
  switch (transport) {
  case TF_CORONET_TRANSPORT_TCP:
  case TF_CORONET_TRANSPORT_WS:
  case TF_CORONET_TRANSPORT_WSS:
    return coro_socket_create_tcpv4(ctx);
  case TF_CORONET_TRANSPORT_UDP:
    return coro_socket_create_udpv4(ctx);
  case TF_CORONET_TRANSPORT_KCP:
    return coro_socket_create_kcp(ctx);
  case TF_CORONET_TRANSPORT_TLS:
    return coro_socket_create(ctx, CORO_SOCKET_TLS);
  case TF_CORONET_TRANSPORT_PIPE:
    return coro_socket_create_pipe(ctx);
  default:
    return NULL;
  }
}

int tf_coronet_connect_socket(coro_socket_t *socket, tf_coronet_transport_t transport,
                              const char *host, int port, const char *path) {
  if (!socket) return TURBO_EINVAL;
  switch (transport) {
  case TF_CORONET_TRANSPORT_TCP:
  case TF_CORONET_TRANSPORT_UDP:
  case TF_CORONET_TRANSPORT_KCP:
  case TF_CORONET_TRANSPORT_TLS:
    return coro_socket_connect(socket, host, port);
  case TF_CORONET_TRANSPORT_PIPE:
    return coro_socket_connect_pipe(socket, tf_coronet_endpoint(host, path));
  case TF_CORONET_TRANSPORT_WS:
    return coro_socket_connect_ws(socket, host, port, tf_coronet_ws_path(path), 0);
  case TF_CORONET_TRANSPORT_WSS:
    return coro_socket_connect_ws(socket, host, port, tf_coronet_ws_path(path), 1);
  default:
    return TURBO_ENOTSUP;
  }
}

int tf_coronet_listen_socket(coro_socket_t *socket, tf_coronet_transport_t transport,
                             const char *host, int port, const char *path, coro_handler_fn handler,
                             void *arg) {
  if (!socket || !handler) return TURBO_EINVAL;
  if (tf_coronet_transport_is_ws(transport)) {
    return coro_socket_listen_ws(socket, host, port, transport == TF_CORONET_TRANSPORT_WSS, handler,
                                 arg);
  }
  if (transport == TF_CORONET_TRANSPORT_PIPE) {
    return coro_socket_listen_on(socket, tf_coronet_endpoint(host, path), 0, handler, arg);
  }
  if (!tf_coronet_transport_valid(transport)) return TURBO_ENOTSUP;
  return coro_socket_listen_on(socket, host, port, handler, arg);
}

int tf_coronet_send_socket(coro_socket_t *socket, tf_coronet_transport_t transport,
                           const char *data, size_t len) {
  if (!socket || !data || len == 0) return TURBO_EINVAL;
  if (tf_coronet_transport_is_ws(transport)) return coro_socket_send_ws_text(socket, data, len);
  if (!tf_coronet_transport_valid(transport)) return TURBO_ENOTSUP;
  return coro_socket_send(socket, data, len);
}
