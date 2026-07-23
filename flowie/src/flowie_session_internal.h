#ifndef FLOWIE_SESSION_INTERNAL_H
#define FLOWIE_SESSION_INTERNAL_H

#include "flowie.h"
#include "turbo_flow.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_SESSION_INTERNAL_ABI_V1 1u
#define FLOWIE_SESSION_INTERNAL_MAX_SUBSCRIPTIONS 65535u

/**
 * @internal @incomplete
 * Single-CoroNet-lane session configuration. The owner is deliberately not
 * thread-safe: the endpoint coroutine serializes every mutation. This private
 * slice owns CONNECT state, subscriptions, and inbound QoS state; will state
 * stays unexposed until its state machine is complete.
 */
typedef struct flowie_session_config_s {
  size_t size;
  uint32_t abi_version;
  uint64_t owner_instance_id;
  uint64_t session_id;
  size_t max_subscriptions;
  size_t max_inflight;
  turbo_flow_protocol_settlement_policy_t settlement;
} flowie_session_config_t;

#define FLOWIE_SESSION_CONFIG_INIT                                                                 \
  {sizeof(flowie_session_config_t),           FLOWIE_SESSION_INTERNAL_ABI_V1, 0u, 0u, 0u, 0u,      \
   TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT}

typedef struct flowie_session_snapshot_s {
  size_t size;
  uint32_t abi_version;
  uint8_t active;
  uint8_t clean_start;
  flowie_mqtt_version_t version;
  uint16_t keep_alive;
  uint32_t session_expiry_interval;
  uint64_t owner_instance_id;
  uint64_t session_id;
  uint64_t session_generation;
  uint64_t resource_generation;
  size_t subscription_count;
  size_t subscription_capacity;
  size_t inflight_count;
  size_t inflight_capacity;
  flowie_mqtt_span_t client_id;
  uint8_t has_will;
  uint8_t will_pending;
  uint8_t will_qos;
  uint8_t will_retain;
  uint32_t will_delay_interval;
  flowie_mqtt_span_t will_topic;
  flowie_mqtt_span_t will_properties;
  flowie_mqtt_span_t will_payload;
} flowie_session_snapshot_t;

#define FLOWIE_SESSION_SNAPSHOT_INIT                                                               \
  {sizeof(flowie_session_snapshot_t), FLOWIE_SESSION_INTERNAL_ABI_V1}

typedef struct flowie_session_subscription_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_span_t filter;
  uint8_t qos;
  uint8_t no_local;
  uint8_t retain_as_published;
  uint8_t retain_handling;
  uint32_t subscription_identifier;
} flowie_session_subscription_t;

#define FLOWIE_SESSION_SUBSCRIPTION_INIT                                                           \
  {sizeof(flowie_session_subscription_t), FLOWIE_SESSION_INTERNAL_ABI_V1}

typedef struct flowie_session_subscribe_result_s {
  size_t size;
  uint32_t abi_version;
  uint16_t packet_id;
  size_t accepted_count;
  uint8_t changed;
} flowie_session_subscribe_result_t;

#define FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT                                                       \
  {sizeof(flowie_session_subscribe_result_t), FLOWIE_SESSION_INTERNAL_ABI_V1}

typedef struct flowie_session_unsubscribe_result_s {
  size_t size;
  uint32_t abi_version;
  uint16_t packet_id;
  size_t filter_count;
  size_t removed_count;
  uint8_t changed;
  flowie_mqtt_span_t reason_codes;
} flowie_session_unsubscribe_result_t;

#define FLOWIE_SESSION_UNSUBSCRIBE_RESULT_INIT                                                     \
  {sizeof(flowie_session_unsubscribe_result_t), FLOWIE_SESSION_INTERNAL_ABI_V1}

typedef struct flowie_session_owner_s flowie_session_owner_t;

typedef struct flowie_session_connect_result_s {
  size_t size;
  uint32_t abi_version;
  uint8_t accepted;
  uint8_t close_after_reply;
  uint8_t session_present;
  turbo_flow_protocol_route_t route;
  flowie_mqtt_control_packet_t reply;
} flowie_session_connect_result_t;

#define FLOWIE_SESSION_CONNECT_RESULT_INIT                                                        \
  {sizeof(flowie_session_connect_result_t),                                                       \
   FLOWIE_SESSION_INTERNAL_ABI_V1,                                                               \
   0u,                                                                                           \
   0u,                                                                                           \
   0u,                                                                                           \
   TURBO_FLOW_PROTOCOL_ROUTE_INIT,                                                               \
   FLOWIE_MQTT_CONTROL_PACKET_INIT}

typedef enum flowie_session_ack_kind_e {
  FLOWIE_SESSION_ACK_NONE = 0,
  FLOWIE_SESSION_ACK_PUBACK,
  FLOWIE_SESSION_ACK_PUBREC,
  FLOWIE_SESSION_ACK_PUBREL,
  FLOWIE_SESSION_ACK_PUBCOMP
} flowie_session_ack_kind_t;

typedef struct flowie_session_ack_intent_s {
  size_t size;
  uint32_t abi_version;
  flowie_session_ack_kind_t kind;
  uint16_t packet_id;
  uint8_t reason_code;
} flowie_session_ack_intent_t;

#define FLOWIE_SESSION_ACK_INTENT_INIT                                                             \
  {sizeof(flowie_session_ack_intent_t), FLOWIE_SESSION_INTERNAL_ABI_V1, FLOWIE_SESSION_ACK_NONE,   \
   0u, 0u}

/** Convert a committed session ACK intent into a pure protocol encoder command. */
CXX_C_API int flowie_session_ack_control_packet(const flowie_session_ack_intent_t *ack,
                                                flowie_mqtt_version_t version,
                                                flowie_mqtt_control_packet_t *out);

typedef struct flowie_session_publish_begin_result_s {
  size_t size;
  uint32_t abi_version;
  uint8_t admit_graph;
  uint8_t has_ack;
  flowie_publish_message_view_t message;
  flowie_session_ack_intent_t ack;
} flowie_session_publish_begin_result_t;

#define FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT                                                   \
  {sizeof(flowie_session_publish_begin_result_t),                                                  \
   FLOWIE_SESSION_INTERNAL_ABI_V1,                                                                 \
   0u,                                                                                             \
   0u,                                                                                             \
   FLOWIE_PUBLISH_MESSAGE_VIEW_INIT,                                                               \
   FLOWIE_SESSION_ACK_INTENT_INIT}

CXX_C_API flowie_session_owner_t *
flowie_session_owner_create(const flowie_session_config_t *config);
CXX_C_API flowie_session_owner_t *
flowie_session_owner_clone(const flowie_session_owner_t *owner);
CXX_C_API int flowie_session_owner_touch(flowie_session_owner_t *owner);
CXX_C_API void flowie_session_owner_destroy(flowie_session_owner_t *owner);

/** Encode only durable MQTT session state into a canonical versioned LTV record. */
CXX_C_API int flowie_session_owner_record_encode(const flowie_session_owner_t *owner, uint8_t *out,
                                                 size_t capacity, size_t *out_size);

/** Restore one inactive owner; routes and unsettled graph attempts are never restored. */
CXX_C_API int flowie_session_owner_record_restore(
    const flowie_session_config_t *config, flowie_mqtt_span_t client_id, uint64_t revision,
    const uint8_t *data, size_t data_size, flowie_session_owner_t **out);

/**
 * Copy CONNECT-owned state into the session. Empty client IDs are rejected
 * until the endpoint implements Assigned Client Identifier replies.
 */
CXX_C_API int flowie_session_owner_open(flowie_session_owner_t *owner,
                                        const flowie_mqtt_connect_view_t *connect);

/**
 * Apply one parsed CONNECT and produce the only CONNACK policy decision.
 * A protocol-level rejection returns TURBO_OK with accepted=0 and
 * close_after_reply=1; internal/state-owner failures are returned directly.
 */
CXX_C_API int flowie_session_owner_connect(flowie_session_owner_t *owner,
                                            const flowie_mqtt_connect_view_t *connect,
                                            flowie_session_connect_result_t *out);
/** Reconnect an already-active owner after MQTT Client ID takeover without arming its Will. */
CXX_C_API int flowie_session_owner_connect_takeover(flowie_session_owner_t *owner,
                                                     const flowie_mqtt_connect_view_t *connect,
                                                     flowie_session_connect_result_t *out);
CXX_C_API int flowie_session_owner_close(flowie_session_owner_t *owner);
CXX_C_API int flowie_session_owner_snapshot(const flowie_session_owner_t *owner,
                                            flowie_session_snapshot_t *out);
CXX_C_API int flowie_session_owner_route(const flowie_session_owner_t *owner,
                                         turbo_flow_protocol_route_t *out);
/** Apply MQTT 5 DISCONNECT session-expiry override before the connection closes. */
CXX_C_API int flowie_session_owner_disconnect(
    flowie_session_owner_t *owner, const flowie_mqtt_control_packet_view_t *disconnect);

/** Clear a pending/configured Will after graph admission and fan-out complete. */
CXX_C_API int flowie_session_owner_will_complete(flowie_session_owner_t *owner);

/** Apply one complete SUBSCRIBE atomically; no partial subscription set is visible on error. */
CXX_C_API int flowie_session_owner_subscribe(flowie_session_owner_t *owner,
                                             const flowie_mqtt_packet_view_t *packet,
                                             const flowie_mqtt_subscribe_view_t *subscribe,
                                             flowie_session_subscribe_result_t *out);
CXX_C_API int flowie_session_owner_subscription_at(const flowie_session_owner_t *owner,
                                                   size_t index,
                                                   flowie_session_subscription_t *out);

/**
 * Reserve one broker-owned outbound packet identifier. The reservation and
 * every following delivery operation run only on the endpoint owner lane.
 */
CXX_C_API int flowie_session_owner_delivery_reserve(flowie_session_owner_t *owner, uint8_t qos,
                                                    uint16_t *packet_id);
/** Commit an encoded outbound PUBLISH so a persistent reconnect can retransmit it. */
CXX_C_API int flowie_session_owner_delivery_commit(flowie_session_owner_t *owner,
                                                   uint16_t packet_id, flowie_mqtt_span_t packet,
                                                   uint64_t expiry_at_epoch_seconds);
/** Commit a delivery accepted while its persistent subscriber is offline. */
CXX_C_API int flowie_session_owner_delivery_commit_queued(flowie_session_owner_t *owner,
                                                          uint16_t packet_id,
                                                          flowie_mqtt_span_t packet,
                                                          uint64_t expiry_at_epoch_seconds);
/** Roll back a reservation or a committed delivery that was not admitted to the send Queue. */
CXX_C_API int flowie_session_owner_delivery_cancel(flowie_session_owner_t *owner,
                                                   uint16_t packet_id);
/** Remove every expired outbound PUBLISH and advance the durable resource generation once. */
CXX_C_API int flowie_session_owner_delivery_expire(flowie_session_owner_t *owner,
                                                   uint64_t now_epoch_seconds,
                                                   size_t *removed_count);
/** Remove one expired outbound PUBLISH identified by its broker packet identifier. */
CXX_C_API int flowie_session_owner_delivery_expire_packet(flowie_session_owner_t *owner,
                                                          uint16_t packet_id,
                                                          uint64_t now_epoch_seconds, int *removed);
/** Return a borrowed pending packet; only retransmissions, not first queued sends, set DUP. */
CXX_C_API int flowie_session_owner_delivery_pending_at(flowie_session_owner_t *owner, size_t index,
                                                       flowie_mqtt_span_t *packet);
/**
 * Return one pending packet with Message Expiry derived from the absolute delivery deadline.
 * The caller must prune expired deliveries before iterating.
 */
CXX_C_API int flowie_session_owner_delivery_pending_at_ex(flowie_session_owner_t *owner,
                                                          size_t index, uint64_t now_epoch_seconds,
                                                          flowie_mqtt_span_t *packet,
                                                          uint16_t *packet_id,
                                                          uint64_t *expiry_at_epoch_seconds);
/** Rewrite the fixed-width MQTT 5 Message Expiry value without changing packet ownership/size. */
CXX_C_API int flowie_session_delivery_packet_expiry_refresh(flowie_mqtt_version_t version,
                                                            uint8_t *packet, size_t packet_size,
                                                            uint64_t expiry_at_epoch_seconds,
                                                            uint64_t now_epoch_seconds);
/** Consume PUBACK/PUBREC/PUBCOMP for one broker-owned outbound delivery. */
CXX_C_API int flowie_session_owner_delivery_ack(flowie_session_owner_t *owner,
                                                const flowie_mqtt_packet_view_t *packet,
                                                flowie_session_ack_intent_t *reply);
/** Apply one complete UNSUBSCRIBE atomically and emit MQTT 5 per-filter reasons on success. */
CXX_C_API int flowie_session_owner_unsubscribe(
    flowie_session_owner_t *owner, const flowie_mqtt_packet_view_t *packet,
    const flowie_mqtt_unsubscribe_view_t *unsubscribe, uint8_t *reason_codes,
    size_t reason_code_capacity, flowie_session_unsubscribe_result_t *out);

/**
 * Begin one inbound PUBLISH. A duplicate pending packet is consumed with
 * admit_graph=0; a completed QoS 2 first phase also returns a PUBREC intent.
 */
CXX_C_API int flowie_session_owner_publish_begin(flowie_session_owner_t *owner,
                                                 const flowie_mqtt_publish_view_t *publish,
                                                 flowie_session_publish_begin_result_t *out);

/** Advance QoS only when the configured graph/storage settlement point succeeds. */
CXX_C_API int flowie_session_owner_publish_settle(
    flowie_session_owner_t *owner, const turbo_flow_protocol_route_t *route,
    const turbo_flow_protocol_settlement_request_t *request, flowie_session_ack_intent_t *out);

/** Consume PUBREL for a QoS 2 record that has already produced PUBREC. */
CXX_C_API int flowie_session_owner_qos2_release(flowie_session_owner_t *owner,
                                                const turbo_flow_protocol_route_t *route,
                                                uint16_t packet_id,
                                                flowie_session_ack_intent_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_SESSION_INTERNAL_H */
