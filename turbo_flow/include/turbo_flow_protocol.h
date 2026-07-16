#ifndef TURBO_FLOW_PROTOCOL_H
#define TURBO_FLOW_PROTOCOL_H

#include "platform.h"
#include "turbo_error.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_CONTRACT_VERSION 1u

typedef enum turbo_flow_protocol_id_e {
  TURBO_FLOW_PROTOCOL_FMQ = 1,
  TURBO_FLOW_PROTOCOL_MQTT
} turbo_flow_protocol_id_t;

/** FMQ wire-level message patterns shared by protocol owners and adapters. */
typedef enum turbo_flow_fmq_pattern_e {
  TURBO_FLOW_FMQ_PUB = 1,
  TURBO_FLOW_FMQ_SUB,
  TURBO_FLOW_FMQ_PUSH,
  TURBO_FLOW_FMQ_PULL,
  TURBO_FLOW_FMQ_ROUTER,
  TURBO_FLOW_FMQ_DEALER,
  TURBO_FLOW_FMQ_PAIR,
  TURBO_FLOW_FMQ_REQ,
  TURBO_FLOW_FMQ_REP,
  TURBO_FLOW_FMQ_XPUB,
  TURBO_FLOW_FMQ_XSUB
} turbo_flow_fmq_pattern_t;

/**
 * Protocol-neutral communication roles.
 *
 * A protocol adapter maps its wire-level roles onto this enum. The pattern
 * core owns only compatibility, candidate ordering, route fencing, and
 * synchronous correlation state; it never owns peers, payloads, queues, I/O,
 * topic matching, or acknowledgements.
 */
typedef enum turbo_flow_pattern_role_e {
  TURBO_FLOW_PATTERN_PUBLISH = 1,
  TURBO_FLOW_PATTERN_SUBSCRIBE,
  TURBO_FLOW_PATTERN_PUSH,
  TURBO_FLOW_PATTERN_PULL,
  TURBO_FLOW_PATTERN_ROUTE,
  TURBO_FLOW_PATTERN_DEAL,
  TURBO_FLOW_PATTERN_PAIR,
  TURBO_FLOW_PATTERN_REQUEST,
  TURBO_FLOW_PATTERN_REPLY,
  TURBO_FLOW_PATTERN_EXTENDED_PUBLISH,
  TURBO_FLOW_PATTERN_EXTENDED_SUBSCRIBE
} turbo_flow_pattern_role_t;

typedef enum turbo_flow_pattern_selection_e {
  TURBO_FLOW_PATTERN_SELECT_FAN_OUT = 1,
  TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN
} turbo_flow_pattern_selection_t;

/** Host-owned selector cursor; it never owns the candidate set. */
typedef struct turbo_flow_pattern_selector_s {
  size_t size;
  uint32_t contract_version;
  atomic_uint_fast64_t cursor;
} turbo_flow_pattern_selector_t;

typedef struct turbo_flow_pattern_selection_iterator_s {
  size_t size;
  uint32_t contract_version;
  turbo_flow_pattern_selection_t selection;
  size_t candidate_count;
  size_t next_offset;
  size_t remaining;
} turbo_flow_pattern_selection_iterator_t;

#define TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT                                               \
  {sizeof(turbo_flow_pattern_selection_iterator_t), TURBO_FLOW_PROTOCOL_CONTRACT_VERSION,        \
   (turbo_flow_pattern_selection_t)0, 0u, 0u, 0u}

/** Pointer-free route association copied independently from a protocol wire frame. */
typedef struct turbo_flow_pattern_route_s {
  size_t size;
  uint32_t contract_version;
  uint64_t route_id;
  uint64_t generation;
} turbo_flow_pattern_route_t;

#define TURBO_FLOW_PATTERN_ROUTE_INIT                                                            \
  {sizeof(turbo_flow_pattern_route_t), TURBO_FLOW_PROTOCOL_CONTRACT_VERSION, 0u, 0u}

typedef enum turbo_flow_pattern_exchange_state_e {
  TURBO_FLOW_PATTERN_EXCHANGE_READY = 0,
  TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY,
  TURBO_FLOW_PATTERN_EXCHANGE_PROCESSING_REQUEST,
  TURBO_FLOW_PATTERN_EXCHANGE_RESETTING
} turbo_flow_pattern_exchange_state_t;

/**
 * Host-owned synchronous request/reply association.
 *
 * The correlation is installed before the state becomes active and cleared
 * before READY is published. RESETTING deliberately retains the correlation
 * for diagnostics until the owning session calls reset.
 */
typedef struct turbo_flow_pattern_exchange_s {
  size_t size;
  uint32_t contract_version;
  atomic_int state;
  atomic_uint_fast64_t correlation_id;
} turbo_flow_pattern_exchange_t;

typedef enum turbo_flow_mqtt_protocol_version_e {
  TURBO_FLOW_MQTT_PROTOCOL_3_1 = 3,
  TURBO_FLOW_MQTT_PROTOCOL_3_1_1 = 4,
  TURBO_FLOW_MQTT_PROTOCOL_5_0 = 5
} turbo_flow_mqtt_protocol_version_t;

typedef enum turbo_flow_protocol_qos_e {
  TURBO_FLOW_PROTOCOL_QOS_0 = 0,
  TURBO_FLOW_PROTOCOL_QOS_1,
  TURBO_FLOW_PROTOCOL_QOS_2
} turbo_flow_protocol_qos_t;

typedef enum turbo_flow_protocol_message_kind_e {
  TURBO_FLOW_PROTOCOL_MESSAGE_DATA = 1,
  TURBO_FLOW_PROTOCOL_MESSAGE_CONTROL,
  TURBO_FLOW_PROTOCOL_MESSAGE_ACK
} turbo_flow_protocol_message_kind_t;

typedef enum turbo_flow_protocol_settlement_point_e {
  /** Protocol owner accepted the decoded packet before graph admission. */
  TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED = 1,
  /** Graph admission accepted ownership of the message. */
  TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED,
  /** The selected graph path completed processing. */
  TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED,
  /** An explicit durable owner committed the message. */
  TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
} turbo_flow_protocol_settlement_point_t;

/** Pointer-free metadata copied between a protocol owner and TurboFlow. */
typedef struct turbo_flow_protocol_message_s {
  size_t size;
  uint32_t contract_version;
  turbo_flow_protocol_id_t protocol;
  uint32_t protocol_version;
  turbo_flow_protocol_message_kind_t kind;
  uint32_t qos;
  uint32_t packet_id;
  uint64_t session_generation;
  uint8_t duplicate;
  uint8_t retain;
} turbo_flow_protocol_message_t;

#define TURBO_FLOW_PROTOCOL_MESSAGE_INIT                                                          \
  {sizeof(turbo_flow_protocol_message_t), TURBO_FLOW_PROTOCOL_CONTRACT_VERSION, 0, 0u, 0, 0u,    \
   0u, 0u, 0u, 0u}

/**
 * Process-local, pointer-free route copied with an owned flow message.
 *
 * A route identifies one live protocol-owner instance and one of its sessions.
 * It is intentionally not a durable or wire-format identity: serialization and
 * process restart must reject or discard it explicitly instead of replaying it.
 */
typedef struct turbo_flow_protocol_route_s {
  size_t size;
  uint32_t contract_version;
  turbo_flow_protocol_id_t protocol;
  uint32_t reserved;
  uint64_t owner_instance_id;
  uint64_t session_id;
  uint64_t session_generation;
} turbo_flow_protocol_route_t;

#define TURBO_FLOW_PROTOCOL_ROUTE_INIT                                                            \
  {sizeof(turbo_flow_protocol_route_t), TURBO_FLOW_PROTOCOL_CONTRACT_VERSION, 0, 0u, 0u, 0u, 0u}

typedef struct turbo_flow_protocol_settlement_policy_s {
  size_t size;
  turbo_flow_protocol_settlement_point_t qos0;
  turbo_flow_protocol_settlement_point_t qos1;
  turbo_flow_protocol_settlement_point_t qos2;
} turbo_flow_protocol_settlement_policy_t;

#define TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT                                                \
  {sizeof(turbo_flow_protocol_settlement_policy_t), TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED,          \
   TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED}

typedef struct turbo_flow_protocol_settlement_request_s {
  size_t size;
  turbo_flow_protocol_message_t message;
  turbo_flow_protocol_settlement_point_t point;
  int status;
  uint64_t message_id;
  uint32_t attempt;
} turbo_flow_protocol_settlement_request_t;

#define TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT                                               \
  {sizeof(turbo_flow_protocol_settlement_request_t), TURBO_FLOW_PROTOCOL_MESSAGE_INIT,            \
   TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, TURBO_OK, 0u, 0u}

/**
 * Message-owned request for one later primitive settlement boundary.
 *
 * The envelope is pointer-free and process-local. `settled_point` is zero
 * until exactly one primitive accepts the requested responsibility.
 */
typedef struct turbo_flow_protocol_settlement_envelope_s {
  size_t size;
  uint32_t contract_version;
  turbo_flow_protocol_message_t message;
  turbo_flow_protocol_settlement_point_t requested_point;
  turbo_flow_protocol_settlement_point_t settled_point;
} turbo_flow_protocol_settlement_envelope_t;

#define TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT                                              \
  {sizeof(turbo_flow_protocol_settlement_envelope_t), TURBO_FLOW_PROTOCOL_CONTRACT_VERSION,       \
   TURBO_FLOW_PROTOCOL_MESSAGE_INIT, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED,                         \
   (turbo_flow_protocol_settlement_point_t)0}

typedef int (*turbo_flow_protocol_settle_fn)(
    void *ctx, const turbo_flow_protocol_settlement_request_t *request);

typedef struct turbo_flow_protocol_owner_ops_s {
  size_t size;
  turbo_flow_protocol_settle_fn settle;
} turbo_flow_protocol_owner_ops_t;

#define TURBO_FLOW_PROTOCOL_OWNER_OPS_INIT {sizeof(turbo_flow_protocol_owner_ops_t), NULL}

CXX_C_API int turbo_flow_protocol_message_validate(
    const turbo_flow_protocol_message_t *message);
CXX_C_API int turbo_flow_pattern_role_validate(turbo_flow_pattern_role_t role);
CXX_C_API int turbo_flow_pattern_roles_compatible(turbo_flow_pattern_role_t local,
                                                   turbo_flow_pattern_role_t remote);
CXX_C_API int turbo_flow_pattern_selector_init(turbo_flow_pattern_selector_t *selector);
CXX_C_API int turbo_flow_pattern_selection_begin(
    turbo_flow_pattern_selector_t *selector, turbo_flow_pattern_selection_t selection,
    size_t candidate_count, turbo_flow_pattern_selection_iterator_t *iterator);
/** Returns TURBO_ENOENT after every candidate has been produced exactly once. */
CXX_C_API int turbo_flow_pattern_selection_next(
    turbo_flow_pattern_selection_iterator_t *iterator, size_t *candidate_index);
CXX_C_API int turbo_flow_pattern_route_validate(const turbo_flow_pattern_route_t *route);
CXX_C_API int turbo_flow_pattern_routes_match(const turbo_flow_pattern_route_t *requested,
                                               const turbo_flow_pattern_route_t *candidate);
CXX_C_API int turbo_flow_pattern_exchange_init(turbo_flow_pattern_exchange_t *exchange);
CXX_C_API int turbo_flow_pattern_exchange_snapshot(
    const turbo_flow_pattern_exchange_t *exchange, turbo_flow_pattern_exchange_state_t *state,
    uint64_t *correlation_id);
CXX_C_API int turbo_flow_pattern_exchange_begin(
    turbo_flow_pattern_exchange_t *exchange, turbo_flow_pattern_exchange_state_t active_state,
    uint64_t correlation_id);
CXX_C_API int turbo_flow_pattern_exchange_match(
    const turbo_flow_pattern_exchange_t *exchange,
    turbo_flow_pattern_exchange_state_t expected_state, uint64_t correlation_id);
CXX_C_API int turbo_flow_pattern_exchange_finish(
    turbo_flow_pattern_exchange_t *exchange,
    turbo_flow_pattern_exchange_state_t expected_state, uint64_t correlation_id,
    turbo_flow_pattern_exchange_state_t terminal_state);
CXX_C_API int turbo_flow_pattern_exchange_mark_resetting(
    turbo_flow_pattern_exchange_t *exchange);
CXX_C_API int turbo_flow_pattern_exchange_reset(turbo_flow_pattern_exchange_t *exchange);
CXX_C_API int turbo_flow_fmq_pattern_validate(turbo_flow_fmq_pattern_t pattern);
CXX_C_API int turbo_flow_fmq_patterns_compatible(turbo_flow_fmq_pattern_t local,
                                                  turbo_flow_fmq_pattern_t remote);
CXX_C_API int turbo_flow_protocol_settlement_policy_validate(
    const turbo_flow_protocol_settlement_policy_t *policy);
/** Dispatch exactly one decision to the protocol owner; this function never generates an ACK. */
CXX_C_API int turbo_flow_protocol_owner_settle(
    const turbo_flow_protocol_owner_ops_t *owner, void *ctx,
    const turbo_flow_protocol_settlement_policy_t *policy,
    const turbo_flow_protocol_settlement_request_t *request);

#ifdef __cplusplus
}
#endif

#endif
