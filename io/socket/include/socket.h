#ifndef TURBO_FLOW_CORONET_H
#define TURBO_FLOW_CORONET_H

#include "CoroNet/turbo_coro_context.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_coronet_execution.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SOCKET_MODULE_VERSION 1u
#define TURBO_FLOW_SOCKET_MODULE "io.socket"
#define TURBO_FLOW_SOCKET_RECEIVE_OPERATION "socket.receive"
#define TURBO_FLOW_SOCKET_SEND_OPERATION "socket.send"
#define TURBO_FLOW_SOCKET_PRIMITIVE_TYPE "SocketEndpoint"

typedef struct turbo_flow_coronet_socket_config_s {
  /** SOURCE listens and publishes received bytes into the flow; SINK connects and sends payloads.
   */
  int role;
  /** TCP, UDP, KCP, TLS, WS, WSS, and Pipe are CoroNet transports, not SOCKS5 proxy modes. */
  int transport;
  /** Bind address for SOURCE, remote address for SINK. Not used by Pipe when path is set. */
  const char *host;
  /** Bind port for SOURCE, remote port for SINK. Ignored by Pipe. */
  int port;
  /** WebSocket request path for WS/WSS sinks; Pipe endpoint for Pipe transports. */
  const char *path;
  /** Per-socket operation timeout in milliseconds; 0 uses the adapter default. */
  uint64_t timeout_ms;
  /** Optional per-operation connect timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t connect_timeout_ms;
  /** Optional per-operation send timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t send_timeout_ms;
  /** Optional per-operation receive timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t recv_timeout_ms;
  /**
   * Optional TLS/WS/WSS connect+handshake deadline in milliseconds; 0 falls back to timeout_ms.
   * CoroNet currently exposes one combined client wait, so an explicitly different
   * connect_timeout_ms is rejected. Explicit server-side handshake timeout is unsupported.
   */
  uint64_t handshake_timeout_ms;
  /** @deprecated Retained for source compatibility; lane-safe sends use send_timeout_ms. */
  uint32_t max_pump_iterations;
  /**
   * CoroNet event-loop context used by this adapter.
   *
   * When NULL, the adapter creates, owns, and drives a fresh context for
   * adapters. When non-NULL and `take_context_ownership` is 0, the caller owns
   * and drives the context. When non-NULL and ownership is transferred, the
   * adapter stops and destroys it during shutdown.
   */
  coro_context_t *context;
  int take_context_ownership;
  /** Required 64-character hexadecimal PSK when transport is KCP. */
  const char *kcp_pre_shared_key;
  uint32_t kcp_mtu;
  uint32_t kcp_send_window;
  uint32_t kcp_receive_window;
  uint32_t kcp_interval_ms;
  uint32_t kcp_handshake_retry_ms;
  uint32_t kcp_fast_resend;
  /** Enable KCP congestion-window control; disabled by default for low latency. */
  int kcp_congestion_control;
  uint32_t kcp_fec_data_shards;
  uint32_t kcp_fec_parity_shards;
  uint32_t kcp_fec_max_payload_size;
  uint32_t kcp_fec_receive_groups;
  /** Enable listener reuse-port binding where CoroNet supports it. SOURCE only. */
  int reuse_port;
  /** Enable OS TCP keepalive on TCP-backed transports. */
  int tcp_keepalive;
  uint64_t tcp_keepalive_idle_ms;
  uint64_t tcp_keepalive_interval_ms;
  uint32_t tcp_keepalive_count;
  /** Enable OS SO_LINGER on TCP-backed transports. linger_ms may be 0 for abortive close. */
  int linger;
  uint64_t linger_ms;
  /** CoroNet socket send high-water mark in bytes; 0 disables the socket-level cap. */
  size_t send_hwm_bytes;
  /** SOURCE only: multicast group joined after the UDP listener binds. */
  const char *udp_multicast_group;
  /** IPv4 local address or IPv6 decimal interface index used for multicast membership. */
  const char *udp_multicast_interface;
  /** Presence bits for loop/TTL/broadcast so an all-zero legacy config preserves OS defaults. */
  uint32_t udp_option_flags;
  int udp_multicast_loop;
  uint32_t udp_multicast_ttl;
  int udp_broadcast;
} turbo_flow_coronet_socket_config_t;

#define TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_LOOP (1u << 0)
#define TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_TTL (1u << 1)
#define TURBO_FLOW_CORONET_UDP_OPTION_BROADCAST (1u << 2)

/** Explicitly disable a timeout while preserving 0 as the legacy fallback value. */
#define TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED UINT64_MAX

typedef enum turbo_flow_coronet_socket_role_e {
  TURBO_FLOW_CORONET_SOCKET_UNCONFIGURED = 0,
  TURBO_FLOW_CORONET_SOCKET_SOURCE,
  TURBO_FLOW_CORONET_SOCKET_SINK
} turbo_flow_coronet_socket_role_t;

typedef enum turbo_flow_coronet_transport_e {
  TURBO_FLOW_CORONET_TRANSPORT_TCP = 0,
  TURBO_FLOW_CORONET_TRANSPORT_UDP,
  TURBO_FLOW_CORONET_TRANSPORT_KCP,
  TURBO_FLOW_CORONET_TRANSPORT_TLS,
  TURBO_FLOW_CORONET_TRANSPORT_WS,
  TURBO_FLOW_CORONET_TRANSPORT_WSS,
  TURBO_FLOW_CORONET_TRANSPORT_PIPE
} turbo_flow_coronet_transport_t;

/**
 * Register a generic CoroNet socket adapter for DSL `adapter "<name>"`.
 *
 * This binds turbo_flow source/sink lifecycle to a CoroNet context. The adapter
 * moves raw bytes or WebSocket frame payloads between sockets and flow messages;
 * it does not implement SOCKS5 proxying, peer discovery, retry policy, routing,
 * message broker semantics, or product-level protocol ownership.
 *
 * SOURCE adapters listen on `host:port` and call `turbo_flow_publish()` for each
 * received payload. SINK adapters connect to `host:port` and synchronously send
 * the current flow message payload. Register a distinct adapter name for each
 * endpoint binding that needs independent configuration.
 */
TURBO_FLOW_C_API int
turbo_flow_coronet_register_socket_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_coronet_socket_config_t *config);

/**
 * Register with an explicit immutable CoroNet context or pool-lane binding.
 * config's legacy context fields must be zero. Returns TURBO_OK or a concrete
 * validation, allocation, lane-resolution, or adapter-registration error.
 */
TURBO_FLOW_C_API int turbo_flow_coronet_register_socket_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_coronet_socket_config_t *config,
    const turbo_flow_coronet_execution_binding_t *execution);

/**
 * Register `adapter_name` from one immutable resolved YAML snapshot.
 *
 * The adapter kind must be `socket`. Host objects and context ownership are
 * deliberately excluded from YAML; the event-loop binding remains an explicit
 * host responsibility. Unknown or mistyped fields fail before registration.
 */
TURBO_FLOW_C_API int turbo_flow_coronet_register_socket_resolved_adapter_ex(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_coronet_execution_binding_t *execution);

/** Register a resolved socket adapter with a private CoroNet execution context. */
TURBO_FLOW_C_API int turbo_flow_coronet_register_socket_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

/** Validate a concrete socket adapter endpoint config before registration. */
TURBO_FLOW_C_API int
turbo_flow_coronet_socket_config_validate(const turbo_flow_coronet_socket_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CORONET_H */
