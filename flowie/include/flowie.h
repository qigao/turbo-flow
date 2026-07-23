#ifndef FLOWIE_H
#define FLOWIE_H

#include "CoroNet/turbo_coro_context.h"
#include "flowie_mqtt_protocol.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_policy.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_security.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_MQTT_SERVER_MODULE "protocol.mqtt.server"
#define FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION "mqtt.publish.ingress"
#define FLOWIE_MQTT_PACKET_EGRESS_OPERATION "mqtt.packet.egress"

typedef struct turbo_flow_coronet_execution_binding_s turbo_flow_coronet_execution_binding_t;

#define FLOWIE_ABI_V1 1u
#define FLOWIE_ENDPOINT_ABI_V2 2u
#define FLOWIE_ENDPOINT_ABI_V3 3u
#define FLOWIE_ENDPOINT_ABI_V4 4u
#define FLOWIE_ENDPOINT_ABI_V5 5u
#define FLOWIE_ENDPOINT_ABI_V6 6u
#define FLOWIE_ENDPOINT_ABI_V7 7u
#define FLOWIE_ENDPOINT_ABI_V8 8u
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
  uint32_t abi_version;
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
  /** Legacy convenience ownership. Prefer the explicit execution-binding registration API. */
  coro_context_t *context;
  int take_context_ownership;
  /** Independent endpoint-owned retained-message capacity. Zero selects max_sessions. */
  size_t max_retained_messages;
  /** Zero selects DISCONNECT; no other slow-subscriber policy is currently supported. */
  flowie_slow_subscriber_policy_t slow_subscriber_policy;
  /** Private-context coroutine stack size. Zero selects the CoroNet default. */
  size_t coroutine_stack_size;
  /** Capacity of each private-context CoroNet ping-pong receive buffer. Zero selects 4 KiB. */
  size_t recv_buffer_size;
  /** Maximum inbound MQTT 5 Topic Alias accepted per connection. Zero disables aliases. */
  uint16_t topic_alias_maximum;
} flowie_endpoint_config_t;

#define FLOWIE_ENDPOINT_CONFIG_INIT                                                                \
  {sizeof(flowie_endpoint_config_t), FLOWIE_ENDPOINT_ABI_V8, FLOWIE_TRANSPORT_TCP,                 \
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
 * Borrowed FlowStore Record owner for managed MQTT facts and retained publications.
 * Flowie adapts it into an internal `turbo_flow_mqtt_store_t` facade at registration;
 * endpoint code never calls this backend's callbacks directly.
 */
typedef struct flowie_endpoint_persistence_binding_s {
  size_t size;
  /** Resolved YAML channel name, or FLOWIE_IMPLICIT_LOCAL_SESSION_STORE_CHANNEL; copied at registration. */
  const char *store_channel;
  /** Provider remains caller-owned and must outlive the registered endpoint. */
  turbo_flow_record_store_t *store;
} flowie_endpoint_persistence_binding_t;

#define FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT                                                   \
  {sizeof(flowie_endpoint_persistence_binding_t), NULL, NULL}

/** Reserved channel name used when managed MQTT endpoints use the default local Record backend. */
#define FLOWIE_IMPLICIT_LOCAL_SESSION_STORE_CHANNEL "__flowie_local_record"

/** Optional capabilities injected into one endpoint without extending its config ABI. */
typedef struct flowie_endpoint_bindings_s {
  size_t size;
  const flowie_endpoint_security_binding_t *security;
  const flowie_endpoint_persistence_binding_t *persistence;
} flowie_endpoint_bindings_t;

#define FLOWIE_ENDPOINT_BINDINGS_INIT {sizeof(flowie_endpoint_bindings_t), NULL, NULL}

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

/** Secure counterpart of flowie_register_endpoint(), including legacy context ownership. */
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

/** Resolved-YAML counterpart; `session_store` must match the injected store channel. */
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
  uint32_t abi_version;
  turbo_flow_protocol_message_t metadata;
  turbo_flow_protocol_route_t route;
  flowie_mqtt_span_t topic;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t payload;
} flowie_publish_message_view_t;

#define FLOWIE_PUBLISH_MESSAGE_VIEW_INIT                                                           \
  {sizeof(flowie_publish_message_view_t), FLOWIE_ABI_V1, TURBO_FLOW_PROTOCOL_MESSAGE_INIT,         \
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
