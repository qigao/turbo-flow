#ifndef FLOWIE_H
#define FLOWIE_H

#include "CoroNet/turbo_coro_context.h"
#include "flowie_mqtt_protocol.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_policy.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_security.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_MQTT_SERVER_MODULE "protocol.mqtt.server"
#define FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION "mqtt.publish.ingress"
#define FLOWIE_MQTT_PACKET_EGRESS_OPERATION "mqtt.packet.egress"

typedef struct turbo_flow_coronet_execution_binding_s turbo_flow_coronet_execution_binding_t;
typedef struct flowie_endpoint_s flowie_endpoint_core_t;

#define FLOWIE_DEFAULT_MAX_PACKET_SIZE (1024u * 1024u)
#define FLOWIE_DEFAULT_MAX_CONNECTIONS 1024u
#define FLOWIE_DEFAULT_SEND_HWM_BYTES (1024u * 1024u)
#define FLOWIE_DEFAULT_RECV_BUFFER_SIZE (4u * 1024u)
#define FLOWIE_DEFAULT_MAX_SUBSCRIPTIONS_PER_SESSION 1024u
#define FLOWIE_DEFAULT_MAX_INFLIGHT_PER_SESSION 64u
#define FLOWIE_MAX_CONNECTIONS_LIMIT UINT32_MAX
#define FLOWIE_MIN_COROUTINE_STACK_SIZE (64u * 1024u)
#define FLOWIE_MAX_COROUTINE_STACK_SIZE (8u * 1024u * 1024u)
#define FLOWIE_MIN_RECV_BUFFER_SIZE 1024u
#define FLOWIE_MAX_RECV_BUFFER_SIZE (1024u * 1024u)

typedef enum flowie_transport_e {
  FLOWIE_TRANSPORT_TCP = 1,
  FLOWIE_TRANSPORT_TLS,
  FLOWIE_TRANSPORT_WS,
  FLOWIE_TRANSPORT_WSS,
  FLOWIE_TRANSPORT_PIPE
} flowie_transport_t;

/** Per-subscriber overflow behavior for MQTT fan-out. */
typedef enum flowie_slow_subscriber_policy_e {
  FLOWIE_SLOW_SUBSCRIBER_POLICY_UNSPECIFIED = 0,
  /** Close only the saturated subscriber and continue the current fan-out batch. */
  FLOWIE_SLOW_SUBSCRIBER_DISCONNECT = 1
} flowie_slow_subscriber_policy_t;

typedef struct flowie_endpoint_config_s {
  size_t size;
  flowie_transport_t transport;
  /** Managed-session QoS ACK gates. All four typed settlement points are implemented. */
  turbo_flow_protocol_settlement_policy_t settlement;
  const char *host;
  int port;
  /** Pipe endpoint or WS/WSS request path. */
  const char *path;
  size_t max_packet_size;
  uint32_t max_connections;
  uint64_t timeout_ms;
  uint64_t recv_timeout_ms;
  int reuse_port;
  int tcp_keepalive;
  uint64_t tcp_keepalive_idle_ms;
  uint64_t tcp_keepalive_interval_ms;
  uint32_t tcp_keepalive_count;
  int linger;
  uint64_t linger_ms;
  /** Per-connection pending-send byte HWM; exhaustion disconnects only that connection. */
  size_t send_hwm_bytes;
  /** Non-zero enables endpoint-owned CONNECT/session/CONNACK processing. */
  int manage_sessions;
  size_t max_sessions;
  size_t max_subscriptions_per_session;
  size_t max_inflight_per_session;
  /** Independent endpoint-owned retained-message capacity. Zero selects max_sessions. */
  size_t max_retained_messages;
  /** Zero selects DISCONNECT; no other slow-subscriber policy is currently supported. */
  flowie_slow_subscriber_policy_t slow_subscriber_policy;
  /** Private-context coroutine stack size. Zero selects the CoroNet default. */
  size_t coroutine_stack_size;
  /** Maximum inbound MQTT 5 Topic Alias accepted per connection. Zero disables aliases. */
  uint16_t topic_alias_maximum;
  /**
   * Capacity of each private-context CoroNet user-space receive buffer.
   * 0 selects the 4 KiB component default.
   */
  size_t stream_recv_buffer_bytes;
  /** Requested OS SO_RCVBUF bytes for TCP/TLS/WS/WSS; 0 preserves the OS default. */
  size_t socket_recv_buffer_bytes;
  /** Requested OS SO_SNDBUF bytes for TCP/TLS/WS/WSS; 0 preserves the OS default. */
  size_t socket_send_buffer_bytes;
  /**
   * Client CA bundle for TLS/WSS. A non-empty value enables required client
   * certificate authentication; absent preserves server-auth-only TLS.
   */
  const char *tls_client_ca_file;
} flowie_endpoint_config_t;

#define FLOWIE_ENDPOINT_CONFIG_INIT                                                                \
  {sizeof(flowie_endpoint_config_t), FLOWIE_TRANSPORT_TCP,                                         \
   TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT}

/**
 * Borrowed security capabilities for one managed endpoint.
 *
 * The provider context and realm must outlive the registered endpoint. Credentials
 * remain borrowed only during CONNECT authentication; Flowie copies only the
 * validated principal. `realm_channel` and `auth_method` are copied at registration.
 */
typedef struct flowie_endpoint_security_binding_s {
  size_t size;
  const char *realm_channel;
  const char *auth_method;
  const turbo_flow_security_auth_provider_t *auth_provider;
  /** Required when MQTT 5 CONNECT carries Authentication Method. No basic-auth fallback occurs. */
  const turbo_flow_security_enhanced_auth_provider_t *enhanced_auth_provider;
  turbo_flow_security_realm_t *realm;
} flowie_endpoint_security_binding_t;

#define FLOWIE_ENDPOINT_SECURITY_BINDING_INIT                                                      \
  {sizeof(flowie_endpoint_security_binding_t), NULL, NULL, NULL, NULL, NULL}

/**
 * Borrowed Record backend for managed MQTT protocol facts and retained publications.
 * Flowie adapts it into its internal ProtocolStore facade at registration; endpoint code never
 * calls backend callbacks directly. This binding is independent of business FlowStore channels.
 */
typedef struct flowie_endpoint_persistence_binding_s {
  size_t size;
  /** Resolved YAML channel name, or FLOWIE_IMPLICIT_PROTOCOL_STORE_CHANNEL; copied at
   * registration. */
  const char *store_channel;
  /** Provider remains caller-owned and must outlive the registered endpoint. */
  turbo_flow_record_store_t *store;
} flowie_endpoint_persistence_binding_t;

#define FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT                                                   \
  {sizeof(flowie_endpoint_persistence_binding_t), NULL, NULL}

/** Maximum trusted proxy networks accepted by one endpoint binding. */
#define FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS 64u

/**
 * Trusted PROXY protocol v1/v2 admission for a TCP/TLS/WSS endpoint.
 *
 * Every direct peer must match one of the numeric IPv4/IPv6 CIDRs. Once this
 * binding is enabled, a valid PROXY v1 or v2 header is mandatory and is consumed
 * before plaintext MQTT framing or the TLS handshake. TCP is intended for a
 * protected listener behind a TLS-terminating proxy. Missing, malformed,
 * oversized, timed-out, or untrusted input is rejected before MQTT
 * authentication. Text v1 carries only addresses and ports; opaque TLVs are
 * available only with binary v2. Strings are copied at registration.
 */
typedef struct flowie_endpoint_proxy_binding_s {
  size_t size;
  const char *const *trusted_peer_cidrs;
  size_t trusted_peer_count;
  size_t max_header_bytes;
  uint64_t header_timeout_ms;
} flowie_endpoint_proxy_binding_t;

#define FLOWIE_ENDPOINT_PROXY_BINDING_INIT                                                         \
  {sizeof(flowie_endpoint_proxy_binding_t), NULL, 0u, 0u, 0u}

#define FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_V1 1u
#define FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_V2 2u
#define FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_V3 3u
#define FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_V3
#define FLOWIE_ENDPOINT_CLUSTER_DEFAULT_REQUEST_TIMEOUT_MS 5000u

/** Cluster-neutral post-CONNECT operation delegated to the MQTT session owner. */
typedef enum flowie_endpoint_cluster_command_e {
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_NONE = 0,
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH,
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_SUBSCRIBE,
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_UNSUBSCRIBE,
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_ACK,
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_DISCONNECT,
  /** Internal completion lane marker; command() must reject this value. */
  FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH_SETTLE
} flowie_endpoint_cluster_command_t;

/**
 * Borrowed owner-produced socket action. `packet` is copied by the endpoint
 * before the callback returns. An empty packet is an explicit no-op or close.
 * For a PUBLISH command, DURABLE means a new publish event was committed and
 * must enter the graph; zero means duplicate/no graph admission. A later
 * PUBLISH_SETTLE completion uses DURABLE only to report its fact commit.
 */
typedef struct flowie_endpoint_cluster_action_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t mqtt_version;
  flowie_mqtt_span_t packet;
  uint8_t close_after_send;
  turbo_flow_protocol_settlement_point_t settlement_point;
} flowie_endpoint_cluster_action_t;

#define FLOWIE_ENDPOINT_CLUSTER_ACTION_INIT                                                        \
  {sizeof(flowie_endpoint_cluster_action_t),                                                       \
   FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT,                                                    \
   FLOWIE_MQTT_VERSION_UNSPECIFIED,                                                                \
   {NULL, 0u},                                                                                     \
   0u,                                                                                             \
   (turbo_flow_protocol_settlement_point_t)0}

/** Runs at most once on the endpoint CoroNet owner lane after an accepted request. */
typedef void (*flowie_endpoint_cluster_complete_fn)(void *complete_ctx, int status,
                                                    const flowie_endpoint_cluster_action_t *action);

/**
 * Borrowed edge-observation metadata for CONNECT. Addresses are endpoint-derived;
 * proxy_tlvs contains the structurally validated opaque PROXY v2 TLV bytes. These
 * values are advisory and must never authorize ownership or fencing decisions.
 */
typedef struct flowie_endpoint_cluster_ingress_s {
  size_t size;
  uint32_t abi_version;
  const char *remote_address;
  const char *transport_peer_address;
  flowie_mqtt_span_t proxy_tlvs;
} flowie_endpoint_cluster_ingress_t;

#define FLOWIE_ENDPOINT_CLUSTER_INGRESS_INIT                                                       \
  {sizeof(flowie_endpoint_cluster_ingress_t),                                                      \
   FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT,                                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   {NULL, 0u}}

/** Endpoint-owned socket port retained by the cluster adapter until detach(). */
typedef struct flowie_endpoint_cluster_socket_port_s {
  size_t size;
  void *ctx;
  /** Idempotently fence this exact socket generation with Session Taken Over semantics. */
  int (*takeover_close)(void *ctx);
  /** Atomically copy/admit one independently ordered owner action. */
  int (*apply_action)(void *ctx, const flowie_endpoint_cluster_action_t *action);
} flowie_endpoint_cluster_socket_port_t;

#define FLOWIE_ENDPOINT_CLUSTER_SOCKET_PORT_INIT                                                   \
  {sizeof(flowie_endpoint_cluster_socket_port_t), NULL, NULL, NULL}

/**
 * External MQTT session-owner boundary for one managed endpoint.
 *
 * Request inputs are borrowed only for the call; an implementation returning
 * TURBO_OK must copy them and, after the request callback returns, invoke
 * completion exactly once on the endpoint's CoroNet owner lane. A non-OK return
 * must not invoke completion. `detach()` is
 * synchronous: after it returns, the adapter must never invoke a completion or
 * socket-port callback for that connection generation. The binding and its ctx
 * remain caller-owned and must outlive the registered endpoint. A successful
 * connect completion must contain exactly one CONNACK. For MQTT 5 success, the
 * endpoint preserves owner properties and appends its socket-limit, assigned
 * Client ID, and enhanced-auth properties; the owner must not emit duplicates
 * of those endpoint-owned properties. A PUBLISH command has connection-local
 * Topic Aliases expanded by the endpoint. A SUBSCRIBE command may contain only
 * the ACL-authorized subset; its SUBACK must contain one reason per submitted
 * entry in the same order, and the endpoint merges locally denied reasons.
 */
typedef struct flowie_endpoint_cluster_binding_s {
  size_t size;
  uint32_t abi_version;
  void *ctx;
  uint64_t request_timeout_ms;
  int (*connect)(void *ctx, uint64_t connection_id, uint64_t connection_generation,
                 const flowie_mqtt_connect_view_t *connect,
                 /** NULL when this endpoint has no security binding. */
                 const turbo_flow_security_principal_t *principal,
                 const flowie_endpoint_cluster_ingress_t *ingress,
                 const flowie_endpoint_cluster_socket_port_t *socket_port,
                 flowie_endpoint_cluster_complete_fn complete, void *complete_ctx);
  int (*command)(void *ctx, uint64_t connection_id, uint64_t connection_generation,
                 flowie_endpoint_cluster_command_t command, flowie_mqtt_version_t mqtt_version,
                 flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
                 flowie_endpoint_cluster_complete_fn complete, void *complete_ctx);
  /**
   * Forward one graph settlement to the CONNECT-bound session owner. The
   * request carries the endpoint-local route generation; the adapter must map
   * it to the authoritative owner generation captured from CONNECT_BIND before
   * serializing it. A successful return accepts exactly one later completion.
   */
  int (*settle)(void *ctx, uint64_t connection_id, uint64_t connection_generation,
                flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id,
                const turbo_flow_protocol_settlement_request_t *settlement,
                flowie_endpoint_cluster_complete_fn complete, void *complete_ctx);
  /** Fire-and-forget abnormal loss; inputs must be copied before returning TURBO_OK. */
  int (*connection_lost)(void *ctx, uint64_t connection_id, uint64_t connection_generation,
                         flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id);
  void (*detach)(void *ctx, uint64_t connection_id, uint64_t connection_generation);
} flowie_endpoint_cluster_binding_t;

#define FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT                                                       \
  {sizeof(flowie_endpoint_cluster_binding_t),                                                      \
   FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT,                                                    \
   NULL,                                                                                           \
   FLOWIE_ENDPOINT_CLUSTER_DEFAULT_REQUEST_TIMEOUT_MS,                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/** Reserved channel name used for the composition root's standalone protocol store. */
#define FLOWIE_IMPLICIT_PROTOCOL_STORE_CHANNEL "__flowie_protocol_store"
/** Source-compatible name for integrations compiled against the former session-store contract. */
#define FLOWIE_IMPLICIT_LOCAL_SESSION_STORE_CHANNEL FLOWIE_IMPLICIT_PROTOCOL_STORE_CHANNEL

/** Optional capabilities injected into one endpoint without extending its config ABI. */
typedef struct flowie_endpoint_bindings_s {
  size_t size;
  const flowie_endpoint_security_binding_t *security;
  const flowie_endpoint_persistence_binding_t *persistence;
  const flowie_endpoint_proxy_binding_t *proxy;
  const flowie_endpoint_cluster_binding_t *cluster;
} flowie_endpoint_bindings_t;

#define FLOWIE_ENDPOINT_BINDINGS_V1_SIZE                                                           \
  (offsetof(flowie_endpoint_bindings_t, persistence) +                                             \
   sizeof(((flowie_endpoint_bindings_t *)0)->persistence))

#define FLOWIE_ENDPOINT_BINDINGS_V2_SIZE                                                           \
  (offsetof(flowie_endpoint_bindings_t, proxy) + sizeof(((flowie_endpoint_bindings_t *)0)->proxy))

#define FLOWIE_ENDPOINT_BINDINGS_V3_SIZE                                                           \
  (offsetof(flowie_endpoint_bindings_t, cluster) +                                                 \
   sizeof(((flowie_endpoint_bindings_t *)0)->cluster))

#define FLOWIE_ENDPOINT_BINDINGS_INIT {sizeof(flowie_endpoint_bindings_t), NULL, NULL, NULL, NULL}

/**
 * Direct application dispatch for one graph-neutral MQTT endpoint Core.
 *
 * The message is borrowed for this same-lane call. Clone it before retaining it.
 * Set result->status to the processing result and, when the configured MQTT
 * settlement policy requires ACCEPTED or DURABLE, set result->protocol_settlement
 * to the boundary actually completed. The callback must not stop or destroy the
 * same Core.
 */
typedef int (*flowie_endpoint_core_message_fn)(flowie_endpoint_core_t *endpoint,
                                               turbo_flow_msg_t *message,
                                               turbo_flow_publish_result_t *result, void *ctx);

typedef struct flowie_endpoint_core_options_s {
  size_t size;
  flowie_endpoint_core_message_fn on_message;
  void *message_ctx;
} flowie_endpoint_core_options_t;

#define FLOWIE_ENDPOINT_CORE_OPTIONS_INIT {sizeof(flowie_endpoint_core_options_t), NULL, NULL}

/**
 * Create one graph-neutral MQTT broker endpoint with a private CoroNet context.
 *
 * The Core owns listener, sessions, subscriptions, retained state and bounded
 * send queues. It does not create, compile or start a TurboFlow graph.
 */
CXX_C_API int flowie_endpoint_core_create(const char *name, const flowie_endpoint_config_t *config,
                                          const flowie_endpoint_core_options_t *options,
                                          flowie_endpoint_core_t **out);

/**
 * Create a direct Core with explicit CoroNet placement and optional bindings.
 * OWNED_CONTEXT is rejected; borrowed resources remain caller-owned through
 * destruction. Security and persistence bindings are copied/retained according
 * to their individual contracts.
 */
CXX_C_API int
flowie_endpoint_core_create_ex(const char *name, const flowie_endpoint_config_t *config,
                               const flowie_endpoint_core_options_t *options,
                               const turbo_flow_coronet_execution_binding_t *execution,
                               const flowie_endpoint_bindings_t *bindings,
                               flowie_endpoint_core_t **out);

CXX_C_API int flowie_endpoint_core_start(flowie_endpoint_core_t *endpoint);
CXX_C_API int flowie_endpoint_core_stop(flowie_endpoint_core_t *endpoint);

/**
 * Submit one complete owned/borrowed MQTT packet to the endpoint egress path.
 * The Core retains or copies backing storage before returning when asynchronous
 * owner-lane work is required.
 */
CXX_C_API int flowie_endpoint_core_send_message(flowie_endpoint_core_t *endpoint,
                                                turbo_flow_msg_t *message);
CXX_C_API void flowie_endpoint_core_destroy(flowie_endpoint_core_t *endpoint);

/**
 * Immutable MQTT PUBLISH facts schema for TurboFlow Policy.
 * MQTT 5 optional properties materialize as NULL when absent; binary correlation data is exposed
 * as a length-delimited string view and may contain NUL bytes.
 */
CXX_C_API const turbo_flow_expr_schema_t *flowie_mqtt_rule_schema(void);

/** Materialize one complete owned MQTT PUBLISH for a `rules.apply` operation. */
CXX_C_API int flowie_mqtt_rule_facts_provider(const turbo_flow_msg_t *message,
                                              const turbo_flow_expr_schema_t *schema,
                                              const turbo_flow_expr_value_t **values_out,
                                              size_t *value_count_out, void *ctx);

/**
 * Return the MQTT PUBLISH application payload rather than the encoded packet.
 *
 * The returned view is borrowed from the message buffer and remains valid for
 * the message lifetime. `ctx` is ignored so this function can be used directly
 * as a `turbo_flow_rulesforge_payload_view_fn`.
 */
CXX_C_API int flowie_mqtt_payload_view(const turbo_flow_msg_t *message, tstr_v *payload_out,
                                       void *ctx);

/**
 * Register one Flowie MQTT server primitive as a bidirectional TurboFlow adapter.
 * The source owns listener/receive/framing; a sink binding accepts an encoded
 * MQTT control packet carrying a message-owned MQTT protocol route.
 * ACCEPTED requires an explicit graph admission stage to complete the settlement.
 * DURABLE requires an explicit durable store commit boundary. It proves
 * message persistence, not restoration of Flowie's process-local session state.
 */
CXX_C_API int flowie_register_endpoint(turbo_flow_t *flow, const char *name,
                                       const flowie_endpoint_config_t *config);

/** Register a Flowie endpoint with an explicit immutable CoroNet lane binding. */
CXX_C_API int flowie_register_endpoint_ex(turbo_flow_t *flow, const char *name,
                                          const flowie_endpoint_config_t *config,
                                          const turbo_flow_coronet_execution_binding_t *execution);

/** Register a managed endpoint with explicit borrowed authentication and authorization owners. */
CXX_C_API int
flowie_register_secure_endpoint_ex(turbo_flow_t *flow, const char *name,
                                   const flowie_endpoint_config_t *config,
                                   const turbo_flow_coronet_execution_binding_t *execution,
                                   const flowie_endpoint_security_binding_t *security);

/** Register with any supported combination of security and durable-session bindings. */
CXX_C_API int
flowie_register_bound_endpoint_ex(turbo_flow_t *flow, const char *name,
                                  const flowie_endpoint_config_t *config,
                                  const turbo_flow_coronet_execution_binding_t *execution,
                                  const flowie_endpoint_bindings_t *bindings);

/** Secure counterpart of flowie_register_endpoint() using a private execution context. */
CXX_C_API int flowie_register_secure_endpoint(turbo_flow_t *flow, const char *name,
                                              const flowie_endpoint_config_t *config,
                                              const flowie_endpoint_security_binding_t *security);

CXX_C_API int flowie_register_bound_endpoint(turbo_flow_t *flow, const char *name,
                                             const flowie_endpoint_config_t *config,
                                             const flowie_endpoint_bindings_t *bindings);

/** Register one Flowie endpoint from an immutable resolved YAML adapter entry. */
CXX_C_API int flowie_register_resolved_endpoint(turbo_flow_t *flow, const char *name,
                                                const turbo_flow_resolved_config_t *resolved,
                                                turbo_flow_config_error_t *error);

/**
 * Register one MQTT 5 client source from a strict resolved `flowie_client`
 * adapter. The source subscribes one explicit topic filter and publishes an
 * owned MQTT PUBLISH message into the graph. Network reconnect is bounded by
 * the configured initial and maximum delays; durable processing remains the
 * responsibility of a downstream outbox sink.
 */
CXX_C_API int flowie_register_resolved_client_source(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_resolved_config_t *resolved,
                                                     turbo_flow_config_error_t *error);

CXX_C_API int flowie_register_resolved_endpoint_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution, turbo_flow_config_error_t *error);

/**
 * Register a secure endpoint whose YAML names the injected realm and authentication method.
 * The adapter must contain `security_realm` and `auth_method`; both are matched against
 * the binding so a stale or miswired composition fails before registration.
 */
CXX_C_API int flowie_register_resolved_secure_endpoint_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const flowie_endpoint_security_binding_t *security, turbo_flow_config_error_t *error);

CXX_C_API int flowie_register_resolved_secure_endpoint(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const flowie_endpoint_security_binding_t *security, turbo_flow_config_error_t *error);

/** Resolved-YAML counterpart; `protocol_store` (or legacy `session_store`) must match the binding.
 */
CXX_C_API int flowie_register_resolved_bound_endpoint_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const flowie_endpoint_bindings_t *bindings, turbo_flow_config_error_t *error);

CXX_C_API int flowie_register_resolved_bound_endpoint(turbo_flow_t *flow, const char *name,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      const flowie_endpoint_bindings_t *bindings,
                                                      turbo_flow_config_error_t *error);

/**
 * Borrowed application ingress view produced at the protocol/data bridge.
 * `metadata` and `route` are copied values; topic, properties, and payload keep
 * the lifetime of the connection owner's receive buffer.
 */
typedef struct flowie_publish_message_view_s {
  size_t size;
  turbo_flow_protocol_message_t metadata;
  turbo_flow_protocol_route_t route;
  flowie_mqtt_span_t topic;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t payload;
} flowie_publish_message_view_t;

#define FLOWIE_PUBLISH_MESSAGE_VIEW_INIT                                                           \
  {sizeof(flowie_publish_message_view_t), TURBO_FLOW_PROTOCOL_MESSAGE_INIT,                        \
   TURBO_FLOW_PROTOCOL_ROUTE_INIT}

/**
 * Build the MQTT PUBLISH protocol/data bridge value without allocation.
 * The caller supplies the protocol-owner route and current session generation.
 * Returns TURBO_OK, TURBO_EINVAL for ABI/route errors, or TURBO_EPROTO for an
 * invalid MQTT publish contract. Output is modified only on success.
 */
CXX_C_API int flowie_publish_message_map(const flowie_mqtt_publish_view_t *publish,
                                         flowie_mqtt_version_t version, uint64_t owner_instance_id,
                                         uint64_t session_id, uint64_t session_generation,
                                         flowie_publish_message_view_t *out);

typedef enum flowie_mqtt_security_resource_kind_e {
  FLOWIE_MQTT_SECURITY_TOPIC = 1,
  FLOWIE_MQTT_SECURITY_TOPIC_FILTER
} flowie_mqtt_security_resource_kind_t;

/** Optional matcher context distinguishing a concrete PUBLISH topic from a SUBSCRIBE filter. */
typedef struct flowie_mqtt_security_context_s {
  size_t size;
  flowie_mqtt_security_resource_kind_t kind;
} flowie_mqtt_security_context_t;

#define FLOWIE_MQTT_SECURITY_CONTEXT_INIT                                                          \
  {sizeof(flowie_mqtt_security_context_t), FLOWIE_MQTT_SECURITY_TOPIC}

/**
 * Initialize a SecurityRealm adapter matcher for MQTT Topic Filters.
 * The matcher is stateless and may be shared when the realm itself is
 * externally synchronized according to its owner contract.
 */
CXX_C_API int flowie_mqtt_security_matcher_init(turbo_flow_security_matcher_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_H */
