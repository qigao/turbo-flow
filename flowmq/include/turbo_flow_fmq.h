#ifndef TURBO_FLOW_FMQ_H
#define TURBO_FLOW_FMQ_H

#include "CoroNet/turbo_coro_context.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_coronet_execution.h"
#include "turbo_flow_protocol.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE (8u * 1024u * 1024u)
#define TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS 1024u
#define TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT 65535u
#define TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE 255u
#define TURBO_FLOW_FMQ_MAX_TOPIC_SIZE 1024u
#define TURBO_FLOW_FMQ_API_VERSION 1u
#define TURBO_FLOW_FMQ_WIRE_VERSION 2u
#define TURBO_FLOW_FMQ_APP_API_VERSION 1u
#define TURBO_FLOW_FMQ_MODULE "io.fmq"
#define TURBO_FLOW_FMQ_PUB_SEND_OPERATION "fmq.pub.send"
#define TURBO_FLOW_FMQ_SUB_RECEIVE_OPERATION "fmq.sub.receive"
#define TURBO_FLOW_FMQ_PUSH_SEND_OPERATION "fmq.push.send"
#define TURBO_FLOW_FMQ_PULL_RECEIVE_OPERATION "fmq.pull.receive"
#define TURBO_FLOW_FMQ_ROUTER_RECEIVE_OPERATION "fmq.router.receive"
#define TURBO_FLOW_FMQ_ROUTER_SEND_OPERATION "fmq.router.send"
#define TURBO_FLOW_FMQ_DEALER_RECEIVE_OPERATION "fmq.dealer.receive"
#define TURBO_FLOW_FMQ_DEALER_SEND_OPERATION "fmq.dealer.send"
#define TURBO_FLOW_FMQ_PAIR_RECEIVE_OPERATION "fmq.pair.receive"
#define TURBO_FLOW_FMQ_PAIR_SEND_OPERATION "fmq.pair.send"
#define TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION "fmq.req.request"
#define TURBO_FLOW_FMQ_REQ_REPLY_OPERATION "fmq.req.reply"
#define TURBO_FLOW_FMQ_REP_REQUEST_OPERATION "fmq.rep.request"
#define TURBO_FLOW_FMQ_REP_REPLY_OPERATION "fmq.rep.reply"
#define TURBO_FLOW_FMQ_XPUB_RECEIVE_OPERATION "fmq.xpub.receive"
#define TURBO_FLOW_FMQ_XPUB_SEND_OPERATION "fmq.xpub.send"
#define TURBO_FLOW_FMQ_XSUB_RECEIVE_OPERATION "fmq.xsub.receive"
#define TURBO_FLOW_FMQ_XSUB_SEND_OPERATION "fmq.xsub.send"

typedef enum turbo_flow_fmq_endpoint_mode_e {
  TURBO_FLOW_FMQ_BIND = 1,
  TURBO_FLOW_FMQ_CONNECT
} turbo_flow_fmq_endpoint_mode_t;

typedef enum turbo_flow_fmq_transport_e {
  TURBO_FLOW_FMQ_TCP = 1,
  TURBO_FLOW_FMQ_TLS,
  TURBO_FLOW_FMQ_UDP,
  TURBO_FLOW_FMQ_KCP,
  TURBO_FLOW_FMQ_PIPE,
  TURBO_FLOW_FMQ_WS,
  TURBO_FLOW_FMQ_WSS
} turbo_flow_fmq_transport_t;

typedef enum turbo_flow_fmq_metadata_policy_e {
  TURBO_FLOW_FMQ_METADATA_STATIC = 1,
  TURBO_FLOW_FMQ_METADATA_INHERIT,
  /** Read pointer-free metadata from turbo_flow_content_descriptor_t::identity. */
  TURBO_FLOW_FMQ_METADATA_CONTENT
} turbo_flow_fmq_metadata_policy_t;

typedef enum turbo_flow_fmq_frame_admission_policy_e {
  TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL = 0,
  TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK,
  TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST
} turbo_flow_fmq_frame_admission_policy_t;

typedef enum turbo_flow_fmq_event_kind_e {
  TURBO_FLOW_FMQ_EVENT_PEER_CONNECTED = 1,
  TURBO_FLOW_FMQ_EVENT_PEER_DISCONNECTED,
  TURBO_FLOW_FMQ_EVENT_RECONNECT_SCHEDULED,
  TURBO_FLOW_FMQ_EVENT_RECONNECT_SUCCEEDED,
  TURBO_FLOW_FMQ_EVENT_RECONNECT_FAILED,
  TURBO_FLOW_FMQ_EVENT_HEARTBEAT_TIMEOUT,
  TURBO_FLOW_FMQ_EVENT_FRAME_SENT,
  TURBO_FLOW_FMQ_EVENT_HWM_REACHED,
  TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED,
  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM,
  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED,
  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DISCONNECTED
} turbo_flow_fmq_event_kind_t;

typedef struct turbo_flow_fmq_event_s {
  size_t size;
  uint32_t version;
  turbo_flow_fmq_event_kind_t kind;
  int status;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  tstr_v peer_identity;
  tstr_v peer_topic;
  uint64_t reconnect_delay_ms;
  uint64_t frame_bytes;
} turbo_flow_fmq_event_t;

#define TURBO_FLOW_FMQ_EVENT_INIT {sizeof(turbo_flow_fmq_event_t), TURBO_FLOW_FMQ_API_VERSION}

/**
 * Host-owned monitoring callback. Values are borrowed for the call only, and
 * callback code must not re-enter the same flow or adapter.
 */
typedef void (*turbo_flow_fmq_event_fn)(void *ctx, const turbo_flow_fmq_event_t *event);

#define TURBO_FLOW_FMQ_TIMEOUT_DISABLED UINT64_MAX
#define TURBO_FLOW_FMQ_RECONNECT_DISABLED UINT64_MAX
#define TURBO_FLOW_FMQ_RECONNECT_MAX_UNBOUNDED UINT64_MAX

typedef struct turbo_flow_fmq_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  /** Bind address or remote host. */
  const char *host;
  int port;
  /**
   * PUB/XPUB output topic or SUB/XSUB prefix. Empty SUB/XSUB prefix subscribes to all topics.
   * A NULL XSUB topic sends no initial subscription so graph input can drive proxy controls.
   */
  const char *topic;
  /** Optional application payload media type; unknown or NULL remains opaque. */
  const char *content_type;
  /** Optional host registry and trusted application schema selector. */
  const turbo_flow_content_binding_t *content_binding;
  /** Required and unique for DEALER; optional diagnostic identity for PAIR. */
  const char *identity;
  size_t max_frame_size;
  uint32_t max_connections;
  /** Backward-compatible default timeout in milliseconds for all operations when not specialized.
   */
  uint64_t timeout_ms;
  /** Per-phase connect timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t connect_timeout_ms;
  /** Per-phase send timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t send_timeout_ms;
  /** Per-phase receive timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t recv_timeout_ms;
  /**
   * TLS/WS/WSS connect+handshake deadline in milliseconds; 0 falls back to timeout_ms.
   * CoroNet currently exposes one combined client wait, so an explicitly different
   * connect_timeout_ms is rejected. Explicit BIND-side handshake timeout is unsupported.
   */
  uint64_t handshake_timeout_ms;
  /**
   * Reconnect timing. 0 means keep the default profile:
   * - reconnect_initial_ms default: 1000 ms
   * - reconnect_max_ms default: 30000 ms
   */
  uint64_t reconnect_initial_ms;
  uint64_t reconnect_max_ms;
  /** Heartbeat interval and timeout in milliseconds. Both 0 disables protocol heartbeat. */
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  turbo_flow_fmq_event_fn event_callback;
  void *event_ctx;
  /** Optional CoroNet context. A supplied context must transfer ownership. */
  coro_context_t *context;
  int take_context_ownership;
  /**
   * Outbound metadata policy. STATIC uses topic/identity above. INHERIT copies
   * the current FMQ message metadata and requires synchronous FMQ input.
   * CONTENT uses the message-owned content descriptor identity.
   */
  turbo_flow_fmq_metadata_policy_t topic_policy;
  turbo_flow_fmq_metadata_policy_t identity_policy;
  /** Pipe endpoint for PIPE; WebSocket request path for WS/WSS, default "/". */
  const char *path;
  /** Enable KCP packet-erasure FEC. Valid only when transport is KCP. */
  int kcp_fec;
  /** KCP FEC backend enum: 0 = none, 1 = wirehair. */
  int kcp_fec_backend;
  uint32_t kcp_fec_data_shards;
  uint32_t kcp_fec_parity_shards;
  uint32_t kcp_fec_max_payload_size;
  /** Enable listener reuse-port binding where CoroNet supports it. BIND only. */
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
  /** BIND only: multicast group joined after the UDP listener binds. */
  const char *udp_multicast_group;
  /** IPv4 local address or IPv6 decimal interface index used for multicast membership. */
  const char *udp_multicast_interface;
  /** Presence bits for loop/TTL/broadcast so an all-zero legacy config preserves OS defaults. */
  uint32_t udp_option_flags;
  int udp_multicast_loop;
  uint32_t udp_multicast_ttl;
  int udp_broadcast;
  /** FMQ in-flight frame count high-water mark; 0 disables the FMQ-level cap. */
  size_t frame_hwm_messages;
  /** FMQ in-flight encoded-frame byte high-water mark; 0 disables the FMQ-level cap. */
  size_t frame_hwm_bytes;
  /** Capacity behavior. FAIL is the backward-compatible default. */
  turbo_flow_fmq_frame_admission_policy_t frame_admission_policy;
  /** BLOCK only: zero checks immediately; UINT64_MAX waits until capacity or stop. */
  uint64_t frame_admission_timeout_ms;
  /** FMQ stop drain timeout for in-flight frames; 0 closes immediately. */
  uint64_t frame_linger_ms;
} turbo_flow_fmq_config_t;

#define TURBO_FLOW_FMQ_CONFIG_INIT {sizeof(turbo_flow_fmq_config_t), TURBO_FLOW_FMQ_API_VERSION}

typedef enum turbo_flow_fmq_slow_peer_policy_e {
  TURBO_FLOW_FMQ_SLOW_PEER_FAIL = 1,
  TURBO_FLOW_FMQ_SLOW_PEER_DROP_OLDEST,
  TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT
} turbo_flow_fmq_slow_peer_policy_t;

/**
 * Optional bounded per-peer queue policy for PUB/XPUB fan-out.
 *
 * This separate versioned structure preserves the frozen FMQ API-v1 endpoint config layout.
 * At least one HWM must be non-zero. A remote SUB/XSUB must send a non-empty identity that is
 * unique among live peers. A successful publish is a volatile queue-admission ACK; actual peer
 * writes and policy actions are reported through turbo_flow_fmq_event_fn.
 */
typedef struct turbo_flow_fmq_fanout_config_s {
  size_t size;
  uint32_t version;
  size_t peer_hwm_messages;
  size_t peer_hwm_bytes;
  turbo_flow_fmq_slow_peer_policy_t slow_peer_policy;
} turbo_flow_fmq_fanout_config_t;

#define TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT                                                          \
  {sizeof(turbo_flow_fmq_fanout_config_t), TURBO_FLOW_FMQ_API_VERSION, 0u, 0u,                     \
   TURBO_FLOW_FMQ_SLOW_PEER_FAIL}

#define TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_LOOP (1u << 0)
#define TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_TTL (1u << 1)
#define TURBO_FLOW_FMQ_UDP_OPTION_BROADCAST (1u << 2)

/**
 * Register one ZeroMQ-like FMQ endpoint under a dotted DSL adapter binding.
 *
 * FMQ has its own bounded, versioned framing protocol over CoroNet transports.
 * It provides messaging-pattern semantics but is not ZeroMQ wire compatible.
 * The supported pairings are PUB(bind)/SUB(connect), PUSH(bind)/PULL(connect),
 * REQ(connect)/REP(bind), ROUTER(bind)/DEALER(connect), and
 * PAIR(bind)/PAIR(connect).
 *
 * `config` must be initialized with TURBO_FLOW_FMQ_CONFIG_INIT. FMQ API v1
 * accepts structures whose size is at least the v1 size and rejects unknown
 * API versions. The wire protocol remains v2-only and is independent from the
 * local C API version.
 */
CXX_C_API int turbo_flow_fmq_register_adapter(turbo_flow_t *flow, const char *name,
                                              const turbo_flow_fmq_config_t *config);

/**
 * Register an FMQ adapter with an explicit CoroNet execution binding.
 *
 * The legacy context fields in config must be zero. Borrowed contexts and pool
 * lanes are driven by their owner, which must keep them running until adapter
 * shutdown completes. Returns TURBO_OK or a concrete config, lane-resolution,
 * allocation, or adapter-registration error.
 */
CXX_C_API int
turbo_flow_fmq_register_adapter_ex(turbo_flow_t *flow, const char *name,
                                   const turbo_flow_fmq_config_t *config,
                                   const turbo_flow_coronet_execution_binding_t *execution);

/** Register PUB/XPUB with an explicit bounded per-peer slow-subscriber policy. */
CXX_C_API int turbo_flow_fmq_register_fanout_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_fmq_config_t *config,
                                                     const turbo_flow_fmq_fanout_config_t *fanout);

/** Register bounded per-peer fan-out with an explicit CoroNet execution binding. */
CXX_C_API int
turbo_flow_fmq_register_fanout_adapter_ex(turbo_flow_t *flow, const char *name,
                                          const turbo_flow_fmq_config_t *config,
                                          const turbo_flow_fmq_fanout_config_t *fanout,
                                          const turbo_flow_coronet_execution_binding_t *execution);

/**
 * Register one FMQ adapter from an immutable resolved YAML snapshot.
 *
 * `name` selects the adapter entry and is also used as the TurboFlow binding
 * name. Only serializable FMQ fields are accepted; host callbacks, schema
 * pointers, and CoroNet execution ownership remain explicit host API inputs.
 */
CXX_C_API int turbo_flow_fmq_register_resolved_adapter(turbo_flow_t *flow, const char *name,
                                                       const turbo_flow_resolved_config_t *resolved,
                                                       turbo_flow_config_error_t *error);

CXX_C_API int turbo_flow_fmq_register_resolved_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution, turbo_flow_config_error_t *error);

/** Message-backed topic view, valid until the message is mutated or cleaned up. */
CXX_C_API int turbo_flow_fmq_message_topic(const turbo_flow_msg_t *msg, tstr_v *topic);

/** Message-backed peer identity view, valid until the message is mutated or cleaned up. */
CXX_C_API int turbo_flow_fmq_message_identity(const turbo_flow_msg_t *msg, tstr_v *identity);

/** Read the message-owned wire correlation ID. */
CXX_C_API int turbo_flow_fmq_message_correlation_id(const turbo_flow_msg_t *msg,
                                                    uint64_t *correlation_id);

/**
 * Detach a ROUTER request from its synchronous receive callback.
 *
 * ROUTER ingress already attaches a message-owned, generation-fenced route.
 * This function validates that capability, ensures the content descriptor is
 * owned, and clears FMQ metadata that the delayed reply no longer needs. The
 * message may then cross worker/fan-out/memory-queue boundaries and later enter
 * the same ROUTER sink. A disconnected, reconnected, or restarted session fails
 * with TURBO_ENOTCONN. REP remains deliberately synchronous.
 */
CXX_C_API int turbo_flow_fmq_message_detach_router_route(turbo_flow_msg_t *msg);

/**
 * Read a message-backed XPUB subscription event.
 * Returns TURBO_ENOENT for ordinary DATA messages.
 */
CXX_C_API int turbo_flow_fmq_message_subscription(const turbo_flow_msg_t *msg, int *subscribe,
                                                  tstr_v *topic);

typedef struct turbo_flow_fmq_app_s turbo_flow_fmq_app_t;

/**
 * Borrowed-message application callback.
 *
 * The message and its payload/topic/identity views remain valid only for this
 * call unless explicitly cloned. Returning an error fails the current graph
 * attempt. REP callbacks may replace the payload and return TURBO_OK to send
 * the synchronous reply. Do not re-enter lifecycle or send APIs on the same
 * application from this callback.
 */
typedef int (*turbo_flow_fmq_app_message_fn)(turbo_flow_fmq_app_t *app,
                                             turbo_flow_msg_t *message, void *ctx);

typedef struct turbo_flow_fmq_app_options_s {
  size_t size;
  uint32_t version;
  turbo_flow_fmq_app_message_fn on_message;
  void *message_ctx;
} turbo_flow_fmq_app_options_t;

#define TURBO_FLOW_FMQ_APP_OPTIONS_INIT                                                             \
  {sizeof(turbo_flow_fmq_app_options_t), TURBO_FLOW_FMQ_APP_API_VERSION, NULL, NULL}

/**
 * Create one ZeroMQ-like application facade over one graph-native FMQ endpoint.
 *
 * The returned application owns its TurboFlow generation and FMQ adapter but
 * creates no additional socket, receive queue, protocol state, or worker
 * thread. Receive-only patterns require on_message. Bidirectional patterns may
 * omit it to discard ingress. Send-only patterns reject a callback.
 */
CXX_C_API int turbo_flow_fmq_app_create(const turbo_flow_fmq_config_t *endpoint,
                                        const turbo_flow_fmq_app_options_t *options,
                                        turbo_flow_fmq_app_t **out);

/** Create the same facade from one resolved YAML adapter entry. */
CXX_C_API int turbo_flow_fmq_app_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error);

CXX_C_API int turbo_flow_fmq_app_start(turbo_flow_fmq_app_t *app);
CXX_C_API int turbo_flow_fmq_app_stop(turbo_flow_fmq_app_t *app);

/** Send copied bytes through the facade's graph input source. */
CXX_C_API int turbo_flow_fmq_app_send(turbo_flow_fmq_app_t *app, const void *data,
                                      size_t data_size);

typedef struct turbo_flow_fmq_app_send_item_s {
  const void *data;
  size_t data_size;
} turbo_flow_fmq_app_send_item_t;

#define TURBO_FLOW_FMQ_APP_SEND_ITEM_INIT {NULL, 0u}
#define TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS 1024u
#define TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES (64u * 1024u * 1024u)

/**
 * Send a batch of copied application messages and wait for every submitted
 * frame to reach the same delivery boundary as turbo_flow_fmq_app_send().
 *
 * PUB, PUSH, and DEALER facades are supported. TCP connect endpoints coalesce
 * the encoded frames into one stream write; other valid endpoint layouts keep
 * the same batch admission/completion semantics without coalescing. On error,
 * frames before the failing item remain submitted and @p submitted reports
 * their count. A batch is bounded by
 * TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS and
 * TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES.
 */
CXX_C_API int turbo_flow_fmq_app_send_batch(turbo_flow_fmq_app_t *app,
                                            const turbo_flow_fmq_app_send_item_t *items,
                                            size_t item_count, size_t *submitted);

/**
 * Send one existing message through the facade.
 *
 * The Flow clones/retains the message for the current publish. This form
 * preserves a detached ROUTER route and is therefore the delayed-reply entry.
 */
CXX_C_API int turbo_flow_fmq_app_send_message(turbo_flow_fmq_app_t *app,
                                              const turbo_flow_msg_t *message);

/** Replace application payload while retaining FMQ route/session context. */
CXX_C_API int turbo_flow_fmq_app_message_set_payload_copy(turbo_flow_msg_t *message,
                                                          const void *data, size_t data_size);

/** Stop when needed and release the facade-owned Flow generation. */
CXX_C_API void turbo_flow_fmq_app_destroy(turbo_flow_fmq_app_t *app);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_H */
