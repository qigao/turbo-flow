#include "flowie.h"

#include "flow_connection.h"
#include "flow_coronet_execution.h"
#include "flow_coronet_runtime.h"
#include "flow_io_policy.h"
#include "flowie_ingress_internal.h"
#include "flowie_rule_internal.h"
#include "flowie_session_internal.h"
#include "flowie_topic_index_internal.h"
#include "fmt.h"
#include "roaring.h"
#include "turbo_deque.h"
#include "turbo_error.h"
#include "turbo_flow_coronet_execution.h"
#include "turbo_hash.h"
#include "turbo_parser.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define FLOWIE_ENDPOINT_DEFAULT_TIMEOUT_MS 1000u
#define FLOWIE_RETAINED_KEY_PREFIX_SIZE 3u
#define FLOWIE_RETAINED_RECORD_VERSION 1u
#define FLOWIE_RETAINED_RECORD_HEADER_SIZE 8u
#define FLOWIE_RETAINED_RECORD_METADATA_SIZE 17u
#define FLOWIE_PRIVATE_COROUTINE_HEADROOM 8u

static const uint8_t FLOWIE_RETAINED_KEY_PREFIX[FLOWIE_RETAINED_KEY_PREFIX_SIZE] = {
    0u, 'R', FLOWIE_RETAINED_RECORD_VERSION};

static const char *const FLOWIE_TRANSPORT_VALUES[] = {"tcp", "tls", "ws", "wss", "pipe"};
static const char *const FLOWIE_SETTLEMENT_VALUES[] = {"received", "accepted", "processed",
                                                       "durable"};
static const char *const FLOWIE_SLOW_SUBSCRIBER_POLICY_VALUES[] = {"disconnect"};

static atomic_uint_fast64_t flowie_next_endpoint_instance_id = 0u;

static const char FLOWIE_CONNECTION_RESOURCE_SCHEMA_TEXT[] =
    "schema FlowieMqttConnectionResource [id(13102), version(1)];\n"
    "message MqttConnectionStatus {\n"
    "  uint32 state;\n"
    "  int32 last_status;\n"
    "  string connections_current;\n"
    "  string connection_limit;\n"
    "  string in_flight_messages;\n"
    "  string in_flight_bytes;\n"
    "  bool started;\n"
    "  bool accepting;\n"
    "}\n";
static const char FLOWIE_QUEUE_RESOURCE_SCHEMA_TEXT[] =
    "schema FlowieMqttQueueResource [id(13103), version(2)];\n"
    "message MqttQueueStatus {\n"
    "  string load;\n"
    "  string capacity;\n"
    "  string messages;\n"
    "  string bytes;\n"
    "  string connection_hwm_bytes;\n"
    "  uint32 slow_subscriber_policy;\n"
    "  string slow_subscriber_disconnects;\n"
    "  bool saturated;\n"
    "  bool accepting;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOWIE_CONNECTION_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_IO_TRANSPORT,
    TURBO_FLOW_RESOURCE_CONNECTION,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "FlowieMqttConnectionResource",
    "MqttConnectionStatus",
    13102u,
    1u,
    FLOWIE_CONNECTION_RESOURCE_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOWIE_QUEUE_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "FlowieMqttQueueResource",
    "MqttQueueStatus",
    13103u,
    2u,
    FLOWIE_QUEUE_RESOURCE_SCHEMA_TEXT};

static const char FLOWIE_PROTOCOL_RESOURCE_SCHEMA_TEXT[] =
    "schema FlowieMqttProtocolResource [id(13101), version(2)];\n"
    "message MqttProtocolStatus {\n"
    "  uint32 transport;\n"
    "  string sessions;\n"
    "  string session_capacity;\n"
    "  string retained_messages;\n"
    "  string retained_capacity;\n"
    "  bool manage_sessions;\n"
    "  bool security_enabled;\n"
    "  bool persistence_enabled;\n"
    "  bool started;\n"
    "  bool accepting;\n"
    "}\n"
    "message MqttProtocolConditions {\n"
    "  uint32 ready_status;\n"
    "  uint32 ready_reason;\n"
    "  uint32 accepting_status;\n"
    "  uint32 accepting_reason;\n"
    "  uint32 drained_status;\n"
    "  uint32 drained_reason;\n"
    "  uint32 saturated_status;\n"
    "  uint32 saturated_reason;\n"
    "}\n"
    "message MqttProtocolEvent {\n"
    "  string sequence;\n"
    "  string generation;\n"
    "  string observed_generation;\n"
    "  bool gap;\n"
    "  int32 last_status;\n"
    "  uint32 reason;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOWIE_PROTOCOL_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "FlowieMqttProtocolResource",
    "MqttProtocolStatus",
    13101u,
    2u,
    FLOWIE_PROTOCOL_RESOURCE_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOWIE_PROTOCOL_CONDITIONS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
    TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "FlowieMqttProtocolResource",
    "MqttProtocolConditions",
    13101u,
    2u,
    FLOWIE_PROTOCOL_RESOURCE_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOWIE_PROTOCOL_EVENT_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
    TURBO_FLOW_RESOURCE_DOCUMENT_EVENT,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "FlowieMqttProtocolResource",
    "MqttProtocolEvent",
    13101u,
    2u,
    FLOWIE_PROTOCOL_RESOURCE_SCHEMA_TEXT};

static const turbo_flow_option_field_t FLOWIE_ENDPOINT_OPTION_FIELDS[] = {
    {"transport", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u,
     FLOWIE_TRANSPORT_VALUES, sizeof(FLOWIE_TRANSPORT_VALUES) / sizeof(FLOWIE_TRANSPORT_VALUES[0])},
    {"settlement_qos1", TURBO_FLOW_OPTION_ENUM, 0u, 0u, 0u, FLOWIE_SETTLEMENT_VALUES,
     sizeof(FLOWIE_SETTLEMENT_VALUES) / sizeof(FLOWIE_SETTLEMENT_VALUES[0])},
    {"settlement_qos2", TURBO_FLOW_OPTION_ENUM, 0u, 0u, 0u, FLOWIE_SETTLEMENT_VALUES,
     sizeof(FLOWIE_SETTLEMENT_VALUES) / sizeof(FLOWIE_SETTLEMENT_VALUES[0])},
    {"host", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"port", TURBO_FLOW_OPTION_U32, 0u, 0u, 65535u, NULL, 0u},
    {"path", TURBO_FLOW_OPTION_PATH, 0u, 0u, 0u, NULL, 0u},
    {"max_packet_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 2u, FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE,
     NULL, 0u},
    {"max_connections", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u, FLOWIE_MAX_CONNECTIONS_LIMIT, NULL,
     0u},
    {"coroutine_stack_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, FLOWIE_MIN_COROUTINE_STACK_SIZE,
     FLOWIE_MAX_COROUTINE_STACK_SIZE, NULL, 0u},
    {"recv_buffer_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, FLOWIE_MIN_RECV_BUFFER_SIZE,
     FLOWIE_MAX_RECV_BUFFER_SIZE, NULL, 0u},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0u, 0u, 0u, NULL, 0u},
    {"recv_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0u, 0u, 0u, NULL, 0u},
    {"reuse_port", TURBO_FLOW_OPTION_BOOL, 0u, 0u, 0u, NULL, 0u},
    {"tcp_keepalive", TURBO_FLOW_OPTION_BOOL, 0u, 0u, 0u, NULL, 0u},
    {"tcp_keepalive_idle_ms", TURBO_FLOW_OPTION_DURATION_MS, 0u, 0u, 0u, NULL, 0u},
    {"tcp_keepalive_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0u, 0u, 0u, NULL, 0u},
    {"tcp_keepalive_count", TURBO_FLOW_OPTION_U32, 0u, 0u, 0u, NULL, 0u},
    {"linger", TURBO_FLOW_OPTION_BOOL, 0u, 0u, 0u, NULL, 0u},
    {"linger_ms", TURBO_FLOW_OPTION_DURATION_MS, 0u, 0u, 0u, NULL, 0u},
    {"send_hwm_bytes", TURBO_FLOW_OPTION_SIZE, 0u, 0u, 0u, NULL, 0u},
    {"slow_subscriber_policy", TURBO_FLOW_OPTION_ENUM, 0u, 0u, 0u,
     FLOWIE_SLOW_SUBSCRIBER_POLICY_VALUES,
     sizeof(FLOWIE_SLOW_SUBSCRIBER_POLICY_VALUES) /
         sizeof(FLOWIE_SLOW_SUBSCRIBER_POLICY_VALUES[0])},
    {"manage_sessions", TURBO_FLOW_OPTION_BOOL, 0u, 0u, 0u, NULL, 0u},
    {"max_sessions", TURBO_FLOW_OPTION_SIZE, 0u, 0u, 0u, NULL, 0u},
    {"max_subscriptions_per_session", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u, UINT16_MAX, NULL, 0u},
    {"max_inflight_per_session", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u, UINT16_MAX, NULL, 0u},
    {"max_retained_messages", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MIN, 1u, 0u, NULL, 0u},
    {"session_store", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"security_realm", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"auth_method", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"context", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0u, 0u, NULL,
     0u},
    {"take_context_ownership", TURBO_FLOW_OPTION_BOOL, 0u, 0u, 0u, NULL, 0u}};

typedef struct flowie_endpoint_s flowie_endpoint_t;
typedef struct flowie_endpoint_session_s flowie_endpoint_session_t;
typedef struct flowie_endpoint_connection_s flowie_endpoint_connection_t;

typedef enum flowie_reply_request_kind_e {
  FLOWIE_REPLY_PACKET = 1,
  FLOWIE_REPLY_PROTOCOL_SETTLEMENT,
  FLOWIE_REPLY_PUBLISH_FANOUT
} flowie_reply_request_kind_t;

typedef struct flowie_reply_request_s {
  flowie_reply_request_kind_t kind;
  turbo_flow_protocol_route_t route;
  turbo_flow_protocol_settlement_request_t settlement;
  tstr_t packet;
  struct flowie_reply_request_s *next;
  size_t reserved_bytes;
  size_t connection_reserved_bytes;
  int close_after_send;
  int preserve_on_terminal_error;
  int subscriber_delivery;
  int broker_will;
  flowie_mqtt_version_t protocol_version;
} flowie_reply_request_t;

TURBO_DEQUE_DEFINE(flowie_reply_queue_t, flowie_reply_request_t *)

struct flowie_endpoint_connection_s {
  flowie_endpoint_t *endpoint;
  coro_socket_t *socket;
  turbo_flow_protocol_route_t route;
  flowie_mqtt_version_t version;
  flowie_endpoint_session_t *session;
  flowie_reply_queue_t send_queue;
  tf_io_budget_t send_budget;
  int send_queue_initialized;
  int send_budget_initialized;
  int send_drain_active;
  int connack_admitted;
  int connack_sent;
  int closing;
  int close_when_replies_drain;
  int close_when_replies_drain_status;
  size_t terminal_reply_count;
  int settlement_pending;
  turbo_flow_protocol_settlement_request_t pending_settlement;
};

TURBO_HASH_MAP_DEFINE(flowie_route_map_t, uint64_t, flowie_endpoint_connection_t *)

typedef struct flowie_endpoint_resource_s {
  flowie_endpoint_t *endpoint;
  turbo_flow_resource_kind_t kind;
} flowie_endpoint_resource_t;

typedef struct flowie_subscription_member_s {
  flowie_endpoint_session_t *session;
  uint64_t session_id;
  uint8_t qos;
  uint8_t no_local;
  uint8_t retain_as_published;
} flowie_subscription_member_t;

typedef struct flowie_subscription_entry_s {
  tstr_t filter;
  turbo_vec_t members;
  turbo_hash_map_t member_index;
  roaring64_bitmap_t *session_ids;
  turbo_flow_pattern_selector_t selector;
  flowie_topic_index_binding_t topic_binding;
  uint8_t shared;
  uint8_t active;
} flowie_subscription_entry_t;

typedef struct flowie_fanout_target_s {
  flowie_endpoint_session_t *session;
  uint8_t qos;
  uint8_t retain_as_published;
} flowie_fanout_target_t;

typedef struct flowie_fanout_delivery_s {
  flowie_endpoint_session_t *session;
  flowie_reply_request_t *request;
  uint16_t packet_id;
} flowie_fanout_delivery_t;

typedef struct flowie_retained_message_s {
  tstr_t topic;
  tstr_t packet;
  flowie_mqtt_version_t version;
  uint64_t publisher_session_id;
  uint64_t expiry_at_epoch_seconds;
  uint64_t revision;
} flowie_retained_message_t;

struct flowie_endpoint_session_s {
  flowie_session_owner_t *owner;
  tstr_t client_id_owned;
  tstr_v client_id;
  turbo_flow_security_principal_t principal;
  tstr_t security_resource;
  uint64_t expiry_deadline_ns;
  uint64_t expiry_at_epoch_seconds;
  uint64_t expiry_session_generation;
  uint64_t will_deadline_ns;
  uint64_t will_at_epoch_seconds;
  uint64_t will_session_generation;
};

struct flowie_endpoint_s {
  turbo_flow_t *flow;
  tf_coronet_execution_t execution;
  coro_context_t *ctx;
  coro_socket_t *server;
  turbo_vec_t clients;
  flowie_route_map_t routes;
  turbo_vec_t sessions;
  turbo_hash_map_t session_index;
  turbo_vec_t subscription_index;
  turbo_vec_t subscription_free_slots;
  turbo_hash_map_t subscription_filter_index;
  flowie_topic_index_t subscription_topics;
  turbo_vec_t retained_messages;
  turbo_hash_map_t retained_index;
  tstr_t host;
  tstr_t path;
  tstr_t source_name;
  tstr_t security_realm_channel;
  tstr_t security_auth_method;
  tstr_t session_store_channel;
  char owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char connection_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char queue_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char protocol_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  flowie_transport_t transport;
  turbo_flow_protocol_settlement_policy_t settlement;
  int port;
  size_t max_packet_size;
  size_t send_hwm_bytes;
  uint32_t max_connections;
  size_t max_sessions;
  size_t max_retained_messages;
  size_t max_subscriptions_per_session;
  size_t max_inflight_per_session;
  flowie_slow_subscriber_policy_t slow_subscriber_policy;
  uint64_t instance_id;
  uint64_t next_route_id;
  tf_coronet_socket_timeout_config_t timeouts;
  tf_coronet_socket_options_t socket_options;
  int reuse_port;
  int manage_sessions;
  int security_enabled;
  int persistence_enabled;
  turbo_flow_security_auth_provider_t auth_provider;
  turbo_flow_security_realm_t *security_realm;
  turbo_flow_record_store_t *session_store;
  int start_refs;
  int reply_refs;
  atomic_int started;
  atomic_int quiesced;
  atomic_int management_command_active;
  atomic_int last_management_status;
  atomic_uint_fast64_t generation;
  atomic_size_t sessions_current;
  atomic_size_t retained_current;
  atomic_uint_fast64_t slow_subscriber_disconnects;
  tf_connection_state_t connection;
  turbo_mutex_t task_mutex;
  turbo_cond_t task_drained;
  size_t task_count;
  int task_sync_initialized;
  flowie_reply_queue_t send_queue;
  turbo_mutex_t send_queue_mutex;
  tf_io_budget_t send_budget;
  int send_queue_initialized;
  int send_budget_initialized;
  int send_drain_active;
  int routes_initialized;
  int sessions_initialized;
  int subscription_index_initialized;
  int subscription_index_valid;
  int retained_initialized;
  coro_wait_t *expiry_wait;
  int expiry_task_active;
  int route_owner_registered;
  flowie_endpoint_resource_t resources[3];
};

static int flowie_reply_enqueue(flowie_endpoint_t *endpoint, flowie_reply_request_t *request);
static int flowie_connection_reply_enqueue(flowie_endpoint_connection_t *connection,
                                           flowie_reply_request_t *request);
static int flowie_session_will_publish(flowie_endpoint_t *endpoint,
                                       flowie_endpoint_session_t *session);

static int flowie_allocate_endpoint_instance_id(uint64_t *out) {
  uint64_t current;
  if (!out) return TURBO_EINVAL;
  current = atomic_load_explicit(&flowie_next_endpoint_instance_id, memory_order_relaxed);
  for (;;) {
    if (current == UINT64_MAX) return TURBO_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&flowie_next_endpoint_instance_id, &current,
                                              current + 1u, memory_order_relaxed,
                                              memory_order_relaxed)) {
      *out = current + 1u;
      return TURBO_OK;
    }
  }
}

static size_t flowie_session_key_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *client_id = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(client_id->data, client_id->len, NULL);
}

static bool flowie_session_key_equal(const void *left, const void *right, size_t key_size,
                                     void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static flowie_endpoint_session_t *flowie_session_find(flowie_endpoint_t *endpoint,
                                                      flowie_mqtt_span_t client_id) {
  tstr_v key = tstr_v_from_buf((const char *)client_id.data, client_id.size);
  flowie_endpoint_session_t *const *found;
  if (!endpoint || !endpoint->sessions_initialized) return NULL;
  found =
      (flowie_endpoint_session_t *const *)turbo_hash_map_get_const(&endpoint->session_index, &key);
  return found ? *found : NULL;
}

static int flowie_subscription_is_shared(flowie_mqtt_span_t filter) {
  static const uint8_t prefix[] = "$share/";
  return filter.size > sizeof(prefix) - 1u && memcmp(filter.data, prefix, sizeof(prefix) - 1u) == 0;
}

static void flowie_subscription_entry_destroy(flowie_subscription_entry_t *entry) {
  if (!entry) return;
  roaring64_bitmap_free(entry->session_ids);
  turbo_hash_map_destroy(&entry->member_index);
  turbo_vec_destroy(&entry->members);
  tstr_freep(&entry->filter);
  memset(entry, 0, sizeof(*entry));
}

static void flowie_subscription_entries_destroy(turbo_vec_t *entries) {
  if (!entries) return;
  for (size_t i = 0u; i < turbo_vec_size(entries); ++i) {
    flowie_subscription_entry_t *entry = (flowie_subscription_entry_t *)turbo_vec_at(entries, i);
    flowie_subscription_entry_destroy(entry);
  }
  turbo_vec_destroy(entries);
}

static flowie_subscription_entry_t *flowie_subscription_entry_lookup(turbo_vec_t *entries,
                                                                     turbo_hash_map_t *filter_index,
                                                                     flowie_mqtt_span_t filter,
                                                                     size_t *entry_index_out) {
  const tstr_v key = tstr_v_from_buf((const char *)filter.data, filter.size);
  const size_t *entry_index;
  flowie_subscription_entry_t *entry;
  if (entry_index_out) *entry_index_out = FLOWIE_TOPIC_INDEX_NO_ENTRY;
  if (!entries || !filter_index || !filter.data) return NULL;
  entry_index = (const size_t *)turbo_hash_map_get_const(filter_index, &key);
  if (!entry_index) return NULL;
  entry = (flowie_subscription_entry_t *)turbo_vec_at(entries, *entry_index);
  if (!entry || !entry->active || !entry->filter) return NULL;
  if (entry_index_out) *entry_index_out = *entry_index;
  return entry;
}

/** Rebuild the derived selector atomically; session owners remain the only subscription truth. */
static int flowie_subscription_index_rebuild(flowie_endpoint_t *endpoint) {
  turbo_vec_t staged;
  turbo_vec_t staged_free_slots;
  turbo_hash_map_t staged_filter_index;
  flowie_topic_index_t staged_topics;
  int rc;
  if (!endpoint || !endpoint->manage_sessions || !endpoint->subscription_index_initialized)
    return TURBO_EINVAL;
  rc = turbo_vec_init(&staged, sizeof(flowie_subscription_entry_t));
  if (rc != TURBO_OK) return rc;
  rc = turbo_vec_init(&staged_free_slots, sizeof(size_t));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&staged);
    return rc;
  }
  rc = turbo_hash_map_init(&staged_filter_index, sizeof(tstr_v), sizeof(size_t),
                           flowie_session_key_hash, flowie_session_key_equal, NULL);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&staged_free_slots);
    turbo_vec_destroy(&staged);
    return rc;
  }
  rc = flowie_topic_index_init(&staged_topics);
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&staged_filter_index);
    turbo_vec_destroy(&staged_free_slots);
    turbo_vec_destroy(&staged);
    return rc;
  }
  for (size_t session_index = 0u; session_index < turbo_vec_size(&endpoint->sessions);
       ++session_index) {
    flowie_endpoint_session_t *const *slot =
        (flowie_endpoint_session_t *const *)turbo_vec_at_const(&endpoint->sessions, session_index);
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    if (!slot || !*slot) {
      rc = TURBO_EPROTO;
      goto fail;
    }
    rc = flowie_session_owner_snapshot((*slot)->owner, &snapshot);
    if (rc != TURBO_OK) goto fail;
    for (size_t subscription_index = 0u; subscription_index < snapshot.subscription_count;
         ++subscription_index) {
      flowie_session_subscription_t subscription = FLOWIE_SESSION_SUBSCRIPTION_INIT;
      flowie_subscription_member_t member;
      flowie_subscription_entry_t *entry;
      rc = flowie_session_owner_subscription_at((*slot)->owner, subscription_index, &subscription);
      if (rc != TURBO_OK) goto fail;
      entry = flowie_subscription_entry_lookup(&staged, &staged_filter_index, subscription.filter,
                                               NULL);
      if (!entry) {
        flowie_subscription_entry_t created;
        size_t created_index;
        tstr_v created_key;
        memset(&created, 0, sizeof(created));
        created.filter = tstr_new_len(subscription.filter.data, subscription.filter.size);
        if (!created.filter) {
          rc = TURBO_ENOMEM;
          goto fail;
        }
        rc = turbo_vec_init(&created.members, sizeof(flowie_subscription_member_t));
        if (rc != TURBO_OK) {
          tstr_free(created.filter);
          goto fail;
        }
        rc = turbo_hash_map_init(&created.member_index, sizeof(uint64_t), sizeof(size_t), NULL,
                                 NULL, NULL);
        if (rc != TURBO_OK) {
          turbo_vec_destroy(&created.members);
          tstr_free(created.filter);
          goto fail;
        }
        created.session_ids = roaring64_bitmap_create();
        if (!created.session_ids) {
          turbo_hash_map_destroy(&created.member_index);
          turbo_vec_destroy(&created.members);
          tstr_free(created.filter);
          rc = TURBO_ENOMEM;
          goto fail;
        }
        created.shared = (uint8_t)flowie_subscription_is_shared(subscription.filter);
        created.active = 1u;
        rc = turbo_flow_pattern_selector_init(&created.selector);
        if (rc != TURBO_OK) {
          flowie_subscription_entry_destroy(&created);
          goto fail;
        }
        {
          flowie_subscription_entry_t *old_entry = flowie_subscription_entry_lookup(
              &endpoint->subscription_index, &endpoint->subscription_filter_index,
              subscription.filter, NULL);
          if (old_entry) {
            const uint_fast64_t cursor =
                atomic_load_explicit(&old_entry->selector.cursor, memory_order_relaxed);
            atomic_store_explicit(&created.selector.cursor, cursor, memory_order_relaxed);
          }
        }
        rc = turbo_vec_push(&staged, &created);
        if (rc != TURBO_OK) {
          roaring64_bitmap_free(created.session_ids);
          turbo_hash_map_destroy(&created.member_index);
          turbo_vec_destroy(&created.members);
          tstr_free(created.filter);
          goto fail;
        }
        created_index = turbo_vec_size(&staged) - 1u;
        entry = (flowie_subscription_entry_t *)turbo_vec_at(&staged, created_index);
        if (!entry) {
          rc = TURBO_EPROTO;
          goto fail;
        }
        created_key = tstr_to_v(entry->filter);
        rc = turbo_hash_map_put(&staged_filter_index, &created_key, &created_index);
        if (rc != TURBO_OK) goto fail;
      }
      memset(&member, 0, sizeof(member));
      member.session = *slot;
      member.session_id = snapshot.session_id;
      member.qos = subscription.qos;
      member.no_local = subscription.no_local;
      member.retain_as_published = subscription.retain_as_published;
      rc = turbo_vec_push(&entry->members, &member);
      if (rc != TURBO_OK) goto fail;
      {
        size_t member_index = turbo_vec_size(&entry->members) - 1u;
        rc = turbo_hash_map_put(&entry->member_index, &member.session_id, &member_index);
        if (rc != TURBO_OK) {
          (void)turbo_vec_resize(&entry->members, member_index);
          goto fail;
        }
      }
      roaring64_bitmap_add(entry->session_ids, member.session_id);
    }
  }
  for (size_t entry_index = 0u; entry_index < turbo_vec_size(&staged); ++entry_index) {
    flowie_subscription_entry_t *entry =
        (flowie_subscription_entry_t *)turbo_vec_at(&staged, entry_index);
    flowie_mqtt_span_t filter;
    if (!entry || !entry->filter) {
      rc = TURBO_EPROTO;
      goto fail;
    }
    filter.data = (const uint8_t *)entry->filter;
    filter.size = tstr_len(entry->filter);
    rc =
        flowie_topic_index_insert_bound(&staged_topics, filter, entry_index, &entry->topic_binding);
    if (rc != TURBO_OK) goto fail;
  }
  flowie_topic_index_destroy(&endpoint->subscription_topics);
  turbo_hash_map_destroy(&endpoint->subscription_filter_index);
  turbo_vec_destroy(&endpoint->subscription_free_slots);
  flowie_subscription_entries_destroy(&endpoint->subscription_index);
  endpoint->subscription_topics = staged_topics;
  endpoint->subscription_filter_index = staged_filter_index;
  endpoint->subscription_free_slots = staged_free_slots;
  endpoint->subscription_index = staged;
  endpoint->subscription_index_initialized = 1;
  endpoint->subscription_index_valid = 1;
  return TURBO_OK;

fail:
  flowie_topic_index_destroy(&staged_topics);
  turbo_hash_map_destroy(&staged_filter_index);
  turbo_vec_destroy(&staged_free_slots);
  flowie_subscription_entries_destroy(&staged);
  endpoint->subscription_index_valid = 0;
  return rc;
}

static int flowie_subscription_member_add(flowie_subscription_entry_t *entry,
                                          flowie_endpoint_session_t *session, uint64_t session_id,
                                          const flowie_session_subscription_t *subscription) {
  flowie_subscription_member_t member;
  size_t member_index;
  int rc;
  if (!entry || !entry->active || !session || !subscription) return TURBO_EINVAL;
  memset(&member, 0, sizeof(member));
  member.session = session;
  member.session_id = session_id;
  member.qos = subscription->qos;
  member.no_local = subscription->no_local;
  member.retain_as_published = subscription->retain_as_published;
  rc = turbo_vec_push(&entry->members, &member);
  if (rc != TURBO_OK) return rc;
  member_index = turbo_vec_size(&entry->members) - 1u;
  rc = turbo_hash_map_put(&entry->member_index, &session_id, &member_index);
  if (rc != TURBO_OK) {
    (void)turbo_vec_resize(&entry->members, member_index);
    return rc;
  }
  roaring64_bitmap_add(entry->session_ids, session_id);
  return TURBO_OK;
}

static int flowie_subscription_entry_init(flowie_subscription_entry_t *entry,
                                          flowie_mqtt_span_t filter) {
  int rc;
  if (!entry || !filter.data || filter.size == 0u) return TURBO_EINVAL;
  memset(entry, 0, sizeof(*entry));
  entry->filter = tstr_new_len(filter.data, filter.size);
  if (!entry->filter) return TURBO_ENOMEM;
  rc = turbo_vec_init(&entry->members, sizeof(flowie_subscription_member_t));
  if (rc != TURBO_OK) goto fail;
  rc =
      turbo_hash_map_init(&entry->member_index, sizeof(uint64_t), sizeof(size_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) goto fail;
  entry->session_ids = roaring64_bitmap_create();
  if (!entry->session_ids) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = turbo_flow_pattern_selector_init(&entry->selector);
  if (rc != TURBO_OK) goto fail;
  entry->shared = (uint8_t)flowie_subscription_is_shared(filter);
  entry->active = 1u;
  return TURBO_OK;

fail:
  flowie_subscription_entry_destroy(entry);
  return rc;
}

static int flowie_subscription_member_upsert(flowie_endpoint_t *endpoint,
                                             flowie_endpoint_session_t *session,
                                             const flowie_session_subscription_t *subscription) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_subscription_entry_t *entry;
  size_t entry_index = FLOWIE_TOPIC_INDEX_NO_ENTRY;
  size_t *member_index;
  int rc;
  if (!endpoint || !session || !subscription) return TURBO_EINVAL;
  if (!endpoint->subscription_index_valid) return TURBO_OK;
  rc = flowie_session_owner_snapshot(session->owner, &snapshot);
  if (rc != TURBO_OK) return rc;
  entry = flowie_subscription_entry_lookup(&endpoint->subscription_index,
                                           &endpoint->subscription_filter_index,
                                           subscription->filter, &entry_index);
  if (entry) {
    member_index = (size_t *)turbo_hash_map_get(&entry->member_index, &snapshot.session_id);
    if (member_index) {
      flowie_subscription_member_t *member =
          (flowie_subscription_member_t *)turbo_vec_at(&entry->members, *member_index);
      if (!member || member->session_id != snapshot.session_id) return TURBO_EPROTO;
      member->session = session;
      member->qos = subscription->qos;
      member->no_local = subscription->no_local;
      member->retain_as_published = subscription->retain_as_published;
      return TURBO_OK;
    }
    return flowie_subscription_member_add(entry, session, snapshot.session_id, subscription);
  }
  {
    flowie_subscription_entry_t created;
    flowie_subscription_entry_t *stored;
    tstr_v key;
    size_t free_count = turbo_vec_size(&endpoint->subscription_free_slots);
    int reused = free_count != 0u;
    rc = flowie_subscription_entry_init(&created, subscription->filter);
    if (rc != TURBO_OK) return rc;
    rc = flowie_subscription_member_add(&created, session, snapshot.session_id, subscription);
    if (rc != TURBO_OK) {
      flowie_subscription_entry_destroy(&created);
      return rc;
    }
    if (reused) {
      const size_t *free_slot =
          (const size_t *)turbo_vec_at_const(&endpoint->subscription_free_slots, free_count - 1u);
      if (!free_slot) {
        flowie_subscription_entry_destroy(&created);
        return TURBO_EPROTO;
      }
      entry_index = *free_slot;
      stored =
          (flowie_subscription_entry_t *)turbo_vec_at(&endpoint->subscription_index, entry_index);
      if (!stored || stored->active) {
        flowie_subscription_entry_destroy(&created);
        return TURBO_EPROTO;
      }
      *stored = created;
      memset(&created, 0, sizeof(created));
      rc = turbo_vec_pop(&endpoint->subscription_free_slots, NULL);
      if (rc != TURBO_OK) return rc;
    } else {
      rc = turbo_vec_push(&endpoint->subscription_index, &created);
      if (rc != TURBO_OK) {
        flowie_subscription_entry_destroy(&created);
        return rc;
      }
      entry_index = turbo_vec_size(&endpoint->subscription_index) - 1u;
      stored =
          (flowie_subscription_entry_t *)turbo_vec_at(&endpoint->subscription_index, entry_index);
      memset(&created, 0, sizeof(created));
    }
    if (!stored) return TURBO_EPROTO;
    rc = flowie_topic_index_insert_bound(&endpoint->subscription_topics, subscription->filter,
                                         entry_index, &stored->topic_binding);
    if (rc != TURBO_OK) goto rollback_slot;
    key = tstr_to_v(stored->filter);
    rc = turbo_hash_map_put(&endpoint->subscription_filter_index, &key, &entry_index);
    if (rc != TURBO_OK) {
      size_t moved = FLOWIE_TOPIC_INDEX_NO_ENTRY;
      size_t removed_position = stored->topic_binding.position;
      int remove_rc = flowie_topic_index_remove(&endpoint->subscription_topics,
                                                &stored->topic_binding, entry_index, &moved);
      if (remove_rc == TURBO_OK && moved != FLOWIE_TOPIC_INDEX_NO_ENTRY) {
        flowie_subscription_entry_t *moved_entry =
            (flowie_subscription_entry_t *)turbo_vec_at(&endpoint->subscription_index, moved);
        if (moved_entry) moved_entry->topic_binding.position = removed_position;
      }
      goto rollback_slot;
    }
    return TURBO_OK;

  rollback_slot:
    flowie_subscription_entry_destroy(stored);
    if (reused) {
      int restore_rc = turbo_vec_push(&endpoint->subscription_free_slots, &entry_index);
      if (restore_rc != TURBO_OK) return restore_rc;
    } else {
      (void)turbo_vec_resize(&endpoint->subscription_index, entry_index);
    }
    return rc;
  }
}

static int flowie_subscription_member_remove(flowie_endpoint_t *endpoint,
                                             flowie_endpoint_session_t *session,
                                             flowie_mqtt_span_t filter) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_subscription_entry_t *entry;
  size_t entry_index = FLOWIE_TOPIC_INDEX_NO_ENTRY;
  size_t *member_index;
  size_t member_position;
  int rc;
  if (!endpoint || !session || !filter.data) return TURBO_EINVAL;
  if (!endpoint->subscription_index_valid) return TURBO_OK;
  rc = flowie_session_owner_snapshot(session->owner, &snapshot);
  if (rc != TURBO_OK) return rc;
  entry = flowie_subscription_entry_lookup(
      &endpoint->subscription_index, &endpoint->subscription_filter_index, filter, &entry_index);
  if (!entry) return TURBO_OK;
  member_index = (size_t *)turbo_hash_map_get(&entry->member_index, &snapshot.session_id);
  if (!member_index) return TURBO_OK;
  member_position = *member_index;
  if (turbo_vec_size(&entry->members) > 1u) {
    rc = turbo_hash_map_remove(&entry->member_index, &snapshot.session_id, NULL);
    if (rc != TURBO_OK) return TURBO_EPROTO;
    rc = turbo_vec_swap_remove(&entry->members, member_position, NULL);
    if (rc != TURBO_OK) return rc;
    if (member_position < turbo_vec_size(&entry->members)) {
      flowie_subscription_member_t *moved_member =
          (flowie_subscription_member_t *)turbo_vec_at(&entry->members, member_position);
      size_t *moved_index;
      if (!moved_member) return TURBO_EPROTO;
      moved_index = (size_t *)turbo_hash_map_get(&entry->member_index, &moved_member->session_id);
      if (!moved_index) return TURBO_EPROTO;
      *moved_index = member_position;
    }
    roaring64_bitmap_remove(entry->session_ids, snapshot.session_id);
    return TURBO_OK;
  }
  rc = turbo_vec_push(&endpoint->subscription_free_slots, &entry_index);
  if (rc != TURBO_OK) return rc;
  {
    size_t moved_entry = FLOWIE_TOPIC_INDEX_NO_ENTRY;
    size_t removed_position = entry->topic_binding.position;
    tstr_v key = tstr_to_v(entry->filter);
    rc = flowie_topic_index_remove(&endpoint->subscription_topics, &entry->topic_binding,
                                   entry_index, &moved_entry);
    if (rc != TURBO_OK) return rc;
    if (moved_entry != FLOWIE_TOPIC_INDEX_NO_ENTRY) {
      flowie_subscription_entry_t *moved =
          (flowie_subscription_entry_t *)turbo_vec_at(&endpoint->subscription_index, moved_entry);
      if (!moved || !moved->active) return TURBO_EPROTO;
      moved->topic_binding.position = removed_position;
    }
    rc = turbo_hash_map_remove(&endpoint->subscription_filter_index, &key, NULL);
    if (rc != TURBO_OK) return TURBO_EPROTO;
  }
  flowie_subscription_entry_destroy(entry);
  return TURBO_OK;
}

static int flowie_subscription_session_remove(flowie_endpoint_t *endpoint,
                                              flowie_endpoint_session_t *session) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  int rc;
  if (!endpoint || !session) return TURBO_EINVAL;
  if (!endpoint->subscription_index_valid) return TURBO_OK;
  rc = flowie_session_owner_snapshot(session->owner, &snapshot);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < turbo_vec_size(&endpoint->subscription_index); ++i) {
    flowie_subscription_entry_t *entry =
        (flowie_subscription_entry_t *)turbo_vec_at(&endpoint->subscription_index, i);
    flowie_mqtt_span_t filter;
    if (!entry || !entry->active ||
        !turbo_hash_map_contains(&entry->member_index, &snapshot.session_id))
      continue;
    filter.data = (const uint8_t *)entry->filter;
    filter.size = tstr_len(entry->filter);
    rc = flowie_subscription_member_remove(endpoint, session, filter);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static void flowie_session_destroy(flowie_endpoint_session_t *session) {
  if (!session) return;
  tstr_freep(&session->security_resource);
  tstr_freep(&session->client_id_owned);
  flowie_session_owner_destroy(session->owner);
  free(session);
}

static void flowie_session_remove_owned(flowie_endpoint_t *endpoint,
                                        flowie_endpoint_session_t *session) {
  size_t count;
  if (!endpoint || !session || !endpoint->sessions_initialized) return;
  if (flowie_subscription_session_remove(endpoint, session) != TURBO_OK)
    endpoint->subscription_index_valid = 0;
  (void)turbo_hash_map_remove(&endpoint->session_index, &session->client_id, NULL);
  count = turbo_vec_size(&endpoint->sessions);
  for (size_t i = 0u; i < count; ++i) {
    flowie_endpoint_session_t **slot =
        (flowie_endpoint_session_t **)turbo_vec_at(&endpoint->sessions, i);
    if (!slot || *slot != session) continue;
    if (i + 1u < count) {
      flowie_endpoint_session_t *const *last =
          (flowie_endpoint_session_t *const *)turbo_vec_at_const(&endpoint->sessions, count - 1u);
      *slot = last ? *last : NULL;
    }
    (void)turbo_vec_resize(&endpoint->sessions, count - 1u);
    break;
  }
  atomic_store_explicit(&endpoint->sessions_current, turbo_vec_size(&endpoint->sessions),
                        memory_order_release);
  flowie_session_destroy(session);
}

static void flowie_session_remove(flowie_endpoint_t *endpoint, flowie_endpoint_session_t *session) {
  flowie_session_remove_owned(endpoint, session);
}

static uint64_t flowie_security_now_epoch_seconds(void) {
  time_t now = time(NULL);
  return now < 0 ? 0u : (uint64_t)now;
}

static void flowie_retained_messages_destroy(flowie_endpoint_t *endpoint) {
  if (!endpoint || !endpoint->retained_initialized) return;
  for (size_t i = 0u; i < turbo_vec_size(&endpoint->retained_messages); ++i) {
    flowie_retained_message_t *message =
        (flowie_retained_message_t *)turbo_vec_at(&endpoint->retained_messages, i);
    if (!message) continue;
    tstr_freep(&message->topic);
    tstr_freep(&message->packet);
  }
  turbo_hash_map_destroy(&endpoint->retained_index);
  turbo_vec_destroy(&endpoint->retained_messages);
  atomic_store_explicit(&endpoint->retained_current, 0u, memory_order_release);
  endpoint->retained_initialized = 0;
}

static flowie_retained_message_t *flowie_retained_message_find(flowie_endpoint_t *endpoint,
                                                               flowie_mqtt_span_t topic,
                                                               size_t *index_out) {
  tstr_v key;
  size_t *index;
  if (!endpoint || !endpoint->retained_initialized || !topic.data || topic.size == 0u) return NULL;
  key.data = (const char *)topic.data;
  key.len = topic.size;
  index = (size_t *)turbo_hash_map_get(&endpoint->retained_index, &key);
  if (!index) return NULL;
  if (index_out) *index_out = *index;
  return (flowie_retained_message_t *)turbo_vec_at(&endpoint->retained_messages, *index);
}

static int flowie_retained_message_remove_memory(flowie_endpoint_t *endpoint,
                                                 flowie_mqtt_span_t topic) {
  flowie_retained_message_t removed;
  flowie_retained_message_t *moved;
  size_t index;
  size_t count;
  tstr_v key;
  int rc;
  if (!endpoint || !endpoint->retained_initialized) return TURBO_EINVAL;
  if (!flowie_retained_message_find(endpoint, topic, &index)) return TURBO_OK;
  key.data = (const char *)topic.data;
  key.len = topic.size;
  rc = turbo_hash_map_remove(&endpoint->retained_index, &key, NULL);
  if (rc != TURBO_OK) return rc;
  count = turbo_vec_size(&endpoint->retained_messages);
  memset(&removed, 0, sizeof(removed));
  rc = turbo_vec_swap_remove(&endpoint->retained_messages, index, &removed);
  if (rc != TURBO_OK) return rc;
  atomic_store_explicit(&endpoint->retained_current, turbo_vec_size(&endpoint->retained_messages),
                        memory_order_release);
  tstr_freep(&removed.topic);
  tstr_freep(&removed.packet);
  if (index + 1u >= count) return TURBO_OK;
  moved = (flowie_retained_message_t *)turbo_vec_at(&endpoint->retained_messages, index);
  if (!moved || !moved->topic) return TURBO_EPROTO;
  key = tstr_to_v(moved->topic);
  {
    size_t *moved_index = (size_t *)turbo_hash_map_get(&endpoint->retained_index, &key);
    if (!moved_index) return TURBO_EPROTO;
    *moved_index = index;
  }
  return TURBO_OK;
}

static int flowie_publish_expiry_at(const flowie_mqtt_publish_view_t *publish,
                                    uint64_t *expiry_at_epoch_seconds) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  int rc;
  if (!publish || !expiry_at_epoch_seconds) return TURBO_EINVAL;
  *expiry_at_epoch_seconds = 0u;
  if (publish->properties.values.size == 0u) return TURBO_OK;
  rc = flowie_mqtt_property_iterator_init(&publish->properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    if (property.identifier == FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL) {
      uint64_t now = flowie_security_now_epoch_seconds();
      if (now == 0u) return TURBO_EIO;
      *expiry_at_epoch_seconds =
          now > UINT64_MAX - property.integer ? UINT64_MAX : now + property.integer;
    }
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_retained_store_put(flowie_endpoint_t *endpoint,
                                     const flowie_retained_message_t *retained,
                                     uint64_t expected_revision);
static int flowie_retained_store_delete(flowie_endpoint_t *endpoint,
                                        const flowie_retained_message_t *retained);

static int flowie_retained_message_remove(flowie_endpoint_t *endpoint, flowie_mqtt_span_t topic) {
  flowie_retained_message_t *existing;
  int rc;
  if (!endpoint) return TURBO_EINVAL;
  existing = flowie_retained_message_find(endpoint, topic, NULL);
  if (!existing) return TURBO_OK;
  rc = flowie_retained_store_delete(endpoint, existing);
  if (rc != TURBO_OK) return rc;
  return flowie_retained_message_remove_memory(endpoint, topic);
}

static int flowie_retained_message_apply(flowie_endpoint_t *endpoint, uint64_t publisher_session_id,
                                         const flowie_mqtt_packet_view_t *packet,
                                         const flowie_mqtt_publish_view_t *publish) {
  flowie_retained_message_t *existing;
  flowie_retained_message_t added;
  flowie_retained_message_t staged;
  tstr_t replacement;
  uint64_t expiry_at = 0u;
  size_t index;
  tstr_v key;
  int rc;
  if (!endpoint || publisher_session_id == 0u || !packet || !publish || !publish->retain)
    return TURBO_EINVAL;
  if (publish->payload.size == 0u) return flowie_retained_message_remove(endpoint, publish->topic);
  rc = flowie_publish_expiry_at(publish, &expiry_at);
  if (rc != TURBO_OK) return rc;
  replacement = tstr_new_len(packet->packet.data, packet->packet.size);
  if (!replacement) return TURBO_ENOMEM;
  existing = flowie_retained_message_find(endpoint, publish->topic, &index);
  if (existing) {
    staged = *existing;
    staged.packet = replacement;
    staged.version = packet->version;
    staged.publisher_session_id = publisher_session_id;
    staged.expiry_at_epoch_seconds = expiry_at;
    if (endpoint->persistence_enabled) {
      if (existing->revision == 0u ||
          existing->revision >= (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX) {
        tstr_free(replacement);
        return TURBO_ERANGE;
      }
      staged.revision = existing->revision + 1u;
    }
    rc = flowie_retained_store_put(endpoint, &staged, existing->revision);
    if (rc != TURBO_OK) {
      tstr_free(replacement);
      return rc;
    }
    tstr_freep(&existing->packet);
    existing->packet = staged.packet;
    existing->version = staged.version;
    existing->publisher_session_id = staged.publisher_session_id;
    existing->expiry_at_epoch_seconds = staged.expiry_at_epoch_seconds;
    existing->revision = staged.revision;
    return TURBO_OK;
  }
  if (turbo_vec_size(&endpoint->retained_messages) >= endpoint->max_retained_messages) {
    tstr_free(replacement);
    return TURBO_ENOSPC;
  }
  memset(&added, 0, sizeof(added));
  added.topic = tstr_new_len(publish->topic.data, publish->topic.size);
  added.packet = replacement;
  added.version = packet->version;
  added.expiry_at_epoch_seconds = expiry_at;
  added.revision = endpoint->persistence_enabled ? 1u : 0u;
  if (!added.topic) {
    tstr_free(added.packet);
    return TURBO_ENOMEM;
  }
  added.publisher_session_id = publisher_session_id;
  rc = turbo_vec_push(&endpoint->retained_messages, &added);
  if (rc != TURBO_OK) goto fail;
  index = turbo_vec_size(&endpoint->retained_messages) - 1u;
  key = tstr_to_v(added.topic);
  rc = turbo_hash_map_put(&endpoint->retained_index, &key, &index);
  if (rc == TURBO_OK) {
    flowie_retained_message_t *inserted =
        (flowie_retained_message_t *)turbo_vec_at(&endpoint->retained_messages, index);
    rc = inserted ? flowie_retained_store_put(endpoint, inserted, 0u) : TURBO_EPROTO;
    if (rc != TURBO_OK) {
      (void)flowie_retained_message_remove_memory(endpoint, publish->topic);
      return rc;
    }
    atomic_store_explicit(&endpoint->retained_current, turbo_vec_size(&endpoint->retained_messages),
                          memory_order_release);
    return TURBO_OK;
  }
  (void)turbo_vec_resize(&endpoint->retained_messages, index);
fail:
  tstr_freep(&added.topic);
  tstr_freep(&added.packet);
  return rc;
}

static int flowie_security_principal_same_owner(const turbo_flow_security_principal_t *left,
                                                const turbo_flow_security_principal_t *right) {
  return left && right && strcmp(left->principal_id, right->principal_id) == 0 &&
         strcmp(left->principal_type, right->principal_type) == 0 &&
         strcmp(left->tenant_id, right->tenant_id) == 0;
}

#define FLOWIE_ENDPOINT_RECORD_VERSION 2u
#define FLOWIE_ENDPOINT_RECORD_HEADER_SIZE 8u
#define FLOWIE_ENDPOINT_RECORD_METADATA_SIZE 17u
#define FLOWIE_ENDPOINT_RECORD_METADATA_V1_SIZE 9u
#define FLOWIE_ENDPOINT_RECORD_PRINCIPAL_SIZE 24u

static void flowie_endpoint_record_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flowie_endpoint_record_write_u32(uint8_t *out, uint32_t value) {
  for (size_t i = 0u; i < 4u; ++i)
    out[i] = (uint8_t)(value >> (24u - i * 8u));
}

static void flowie_endpoint_record_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - i * 8u));
}

static uint16_t flowie_endpoint_record_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flowie_endpoint_record_read_u32(const uint8_t *data) {
  uint32_t value = 0u;
  for (size_t i = 0u; i < 4u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static uint64_t flowie_endpoint_record_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static int flowie_endpoint_record_cstr_size(const char *value, size_t capacity, int required,
                                            size_t *out) {
  const char *end;
  if (!value || capacity == 0u || !out) return TURBO_EINVAL;
  end = (const char *)memchr(value, '\0', capacity);
  if (!end || (required && end == value)) return TURBO_EPROTO;
  *out = (size_t)(end - value);
  return TURBO_OK;
}

static int flowie_endpoint_principal_validate(const turbo_flow_security_principal_t *principal) {
  size_t ignored;
  if (!principal || principal->size < sizeof(*principal) ||
      principal->abi_version != TURBO_FLOW_SECURITY_ABI_V1 ||
      principal->scope < TURBO_FLOW_SECURITY_SCOPE_SELF ||
      principal->scope > TURBO_FLOW_SECURITY_SCOPE_SYSTEM ||
      principal->role_count > TURBO_FLOW_SECURITY_MAX_ROLES ||
      principal->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS || principal->policy_version == 0u ||
      flowie_endpoint_record_cstr_size(principal->principal_id, sizeof(principal->principal_id), 1,
                                       &ignored) != TURBO_OK ||
      flowie_endpoint_record_cstr_size(principal->principal_type, sizeof(principal->principal_type),
                                       1, &ignored) != TURBO_OK ||
      flowie_endpoint_record_cstr_size(principal->tenant_id, sizeof(principal->tenant_id),
                                       principal->scope != TURBO_FLOW_SECURITY_SCOPE_SYSTEM,
                                       &ignored) != TURBO_OK ||
      flowie_endpoint_record_cstr_size(principal->auth_method, sizeof(principal->auth_method), 1,
                                       &ignored) != TURBO_OK ||
      (principal->scope == TURBO_FLOW_SECURITY_SCOPE_GROUP && principal->group_count == 0u))
    return TURBO_EPROTO;
  for (uint32_t i = 0u; i < principal->role_count; ++i)
    if (flowie_endpoint_record_cstr_size(principal->roles[i], sizeof(principal->roles[i]), 1,
                                         &ignored) != TURBO_OK)
      return TURBO_EPROTO;
  for (uint32_t i = 0u; i < principal->group_count; ++i)
    if (flowie_endpoint_record_cstr_size(principal->groups[i], sizeof(principal->groups[i]), 1,
                                         &ignored) != TURBO_OK)
      return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_endpoint_record_size_add(size_t *total, size_t value_size) {
  size_t wire_size;
  if (!total) return TURBO_EINVAL;
  wire_size = turbo_ltv_wire_size(value_size);
  if (wire_size == 0u || *total > SIZE_MAX - wire_size) return TURBO_ERANGE;
  *total += wire_size;
  return TURBO_OK;
}

static int flowie_endpoint_record_append(uint8_t *out, size_t capacity, size_t *offset,
                                         uint8_t type, const uint8_t *value, size_t value_size) {
  size_t wire_size;
  size_t written;
  if (!out || !offset || type == 0u || (!value && value_size != 0u) || *offset > capacity)
    return TURBO_EINVAL;
  wire_size = turbo_ltv_wire_size(value_size);
  if (wire_size == 0u || wire_size > capacity - *offset) return TURBO_ENOSPC;
  written = turbo_ltv_build(type, value, value_size, out + *offset, capacity - *offset);
  if (written != wire_size) return TURBO_EPROTO;
  *offset += written;
  return TURBO_OK;
}

static int flowie_retained_key_build(flowie_mqtt_span_t topic, uint8_t **key_out,
                                     size_t *key_size_out) {
  uint8_t *key;
  size_t key_size;
  if (key_out) *key_out = NULL;
  if (key_size_out) *key_size_out = 0u;
  if (!key_out || !key_size_out || !topic.data || topic.size == 0u) return TURBO_EINVAL;
  if (!flowie_mqtt_topic_name_validate(topic)) return TURBO_EPROTO;
  if (topic.size > SIZE_MAX - FLOWIE_RETAINED_KEY_PREFIX_SIZE) return TURBO_ERANGE;
  key_size = FLOWIE_RETAINED_KEY_PREFIX_SIZE + topic.size;
  key = (uint8_t *)malloc(key_size);
  if (!key) return TURBO_ENOMEM;
  memcpy(key, FLOWIE_RETAINED_KEY_PREFIX, FLOWIE_RETAINED_KEY_PREFIX_SIZE);
  memcpy(key + FLOWIE_RETAINED_KEY_PREFIX_SIZE, topic.data, topic.size);
  *key_out = key;
  *key_size_out = key_size;
  return TURBO_OK;
}

static int flowie_retained_record_encode(const flowie_endpoint_t *endpoint,
                                         const flowie_retained_message_t *retained, uint8_t **out,
                                         size_t *out_size) {
  const uint8_t header[FLOWIE_RETAINED_RECORD_HEADER_SIZE] = {
      'F', 'R', 'E', 'T', 0u, FLOWIE_RETAINED_RECORD_VERSION, 0u, 0u};
  uint8_t metadata[FLOWIE_RETAINED_RECORD_METADATA_SIZE] = {0};
  uint8_t *record = NULL;
  size_t required = 0u;
  size_t offset = 0u;
  int rc;
  if (out) *out = NULL;
  if (out_size) *out_size = 0u;
  if (!endpoint || !retained || !retained->topic || !retained->packet || !out || !out_size)
    return TURBO_EINVAL;
  if (retained->version != FLOWIE_MQTT_VERSION_3_1_1 && retained->version != FLOWIE_MQTT_VERSION_5)
    return TURBO_EPROTO;
  rc = flowie_endpoint_record_size_add(&required, sizeof(header));
  if (rc == TURBO_OK) rc = flowie_endpoint_record_size_add(&required, sizeof(metadata));
  if (rc == TURBO_OK) rc = flowie_endpoint_record_size_add(&required, tstr_len(retained->packet));
  if (rc != TURBO_OK) return rc;
  if (!endpoint->session_store || required > endpoint->session_store->max_value_size)
    return TURBO_EMSGSIZE;
  record = (uint8_t *)malloc(required);
  if (!record) return TURBO_ENOMEM;
  metadata[0] = (uint8_t)retained->version;
  flowie_endpoint_record_write_u64(metadata + 1u, retained->publisher_session_id);
  flowie_endpoint_record_write_u64(metadata + 9u, retained->expiry_at_epoch_seconds);
  rc = flowie_endpoint_record_append(record, required, &offset, 1u, header, sizeof(header));
  if (rc == TURBO_OK)
    rc = flowie_endpoint_record_append(record, required, &offset, 2u, metadata, sizeof(metadata));
  if (rc == TURBO_OK)
    rc = flowie_endpoint_record_append(record, required, &offset, 3u,
                                       (const uint8_t *)retained->packet,
                                       tstr_len(retained->packet));
  if (rc != TURBO_OK || offset != required) {
    free(record);
    return rc == TURBO_OK ? TURBO_EPROTO : rc;
  }
  *out = record;
  *out_size = required;
  return TURBO_OK;
}

static int flowie_retained_store_put(flowie_endpoint_t *endpoint,
                                     const flowie_retained_message_t *retained,
                                     uint64_t expected_revision) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  flowie_mqtt_span_t topic;
  uint8_t *key = NULL;
  uint8_t *value = NULL;
  size_t key_size = 0u;
  size_t value_size = 0u;
  int rc;
  if (!endpoint || !retained || !retained->topic || !retained->packet) return TURBO_EINVAL;
  if (!endpoint->persistence_enabled) return TURBO_OK;
  if (!endpoint->session_store || retained->revision <= expected_revision ||
      retained->revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EPROTO;
  topic = (flowie_mqtt_span_t){(const uint8_t *)retained->topic, tstr_len(retained->topic)};
  rc = flowie_retained_key_build(topic, &key, &key_size);
  if (rc != TURBO_OK) return rc;
  if (key_size > endpoint->session_store->max_key_size) {
    rc = TURBO_EMSGSIZE;
    goto done;
  }
  rc = flowie_retained_record_encode(endpoint, retained, &value, &value_size);
  if (rc != TURBO_OK) goto done;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.expected_revision = expected_revision;
  mutation.next_revision = retained->revision;
  mutation.value = value;
  mutation.value_size = value_size;
  rc = endpoint->session_store->commit(endpoint->session_store->ctx, &mutation, 1u);

done:
  free(value);
  free(key);
  return rc;
}

static int flowie_retained_store_delete(flowie_endpoint_t *endpoint,
                                        const flowie_retained_message_t *retained) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  flowie_mqtt_span_t topic;
  uint8_t *key = NULL;
  size_t key_size = 0u;
  int rc;
  if (!endpoint || !retained || !retained->topic) return TURBO_EINVAL;
  if (!endpoint->persistence_enabled) return TURBO_OK;
  if (!endpoint->session_store || retained->revision == 0u ||
      retained->revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EPROTO;
  topic = (flowie_mqtt_span_t){(const uint8_t *)retained->topic, tstr_len(retained->topic)};
  rc = flowie_retained_key_build(topic, &key, &key_size);
  if (rc != TURBO_OK) return rc;
  if (key_size > endpoint->session_store->max_key_size) {
    free(key);
    return TURBO_EMSGSIZE;
  }
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.expected_revision = retained->revision;
  rc = endpoint->session_store->commit(endpoint->session_store->ctx, &mutation, 1u);
  free(key);
  return rc;
}

static int flowie_endpoint_record_encode(const flowie_endpoint_t *endpoint,
                                         const flowie_session_owner_t *owner,
                                         const turbo_flow_security_principal_t *principal,
                                         uint64_t expiry_at_epoch_seconds,
                                         uint64_t will_at_epoch_seconds, uint8_t **out,
                                         size_t *out_size) {
  const uint8_t header[FLOWIE_ENDPOINT_RECORD_HEADER_SIZE] = {
      'F', 'S', 'E', 'P', 0u, FLOWIE_ENDPOINT_RECORD_VERSION, 0u, 0u};
  uint8_t metadata[FLOWIE_ENDPOINT_RECORD_METADATA_SIZE] = {0};
  uint8_t principal_metadata[FLOWIE_ENDPOINT_RECORD_PRINCIPAL_SIZE] = {0};
  uint8_t *owner_record = NULL;
  uint8_t *record = NULL;
  size_t owner_size = 0u;
  size_t required = 0u;
  size_t offset = 0u;
  int rc;
  if (out) *out = NULL;
  if (out_size) *out_size = 0u;
  if (!endpoint || !owner || !out || !out_size) return TURBO_EINVAL;
  rc = flowie_session_owner_record_encode(owner, NULL, 0u, &owner_size);
  if (rc != TURBO_ENOSPC || owner_size == 0u) return rc == TURBO_OK ? TURBO_EPROTO : rc;
  owner_record = (uint8_t *)malloc(owner_size);
  if (!owner_record) return TURBO_ENOMEM;
  rc = flowie_session_owner_record_encode(owner, owner_record, owner_size, &owner_size);
  if (rc != TURBO_OK) goto done;
  rc = flowie_endpoint_record_size_add(&required, sizeof(header));
  if (rc == TURBO_OK) rc = flowie_endpoint_record_size_add(&required, sizeof(metadata));
  if (endpoint->security_enabled) {
    size_t length;
    rc = flowie_endpoint_principal_validate(principal);
    if (rc == TURBO_OK) rc = flowie_endpoint_record_size_add(&required, sizeof(principal_metadata));
    if (rc == TURBO_OK) {
      (void)flowie_endpoint_record_cstr_size(principal->principal_id,
                                             sizeof(principal->principal_id), 1, &length);
      rc = flowie_endpoint_record_size_add(&required, length);
    }
    if (rc == TURBO_OK) {
      (void)flowie_endpoint_record_cstr_size(principal->principal_type,
                                             sizeof(principal->principal_type), 1, &length);
      rc = flowie_endpoint_record_size_add(&required, length);
    }
    if (rc == TURBO_OK) {
      (void)flowie_endpoint_record_cstr_size(principal->tenant_id, sizeof(principal->tenant_id), 0,
                                             &length);
      rc = flowie_endpoint_record_size_add(&required, length);
    }
    if (rc == TURBO_OK) {
      (void)flowie_endpoint_record_cstr_size(principal->auth_method, sizeof(principal->auth_method),
                                             1, &length);
      rc = flowie_endpoint_record_size_add(&required, length);
    }
    for (uint32_t i = 0u; rc == TURBO_OK && i < principal->role_count; ++i) {
      (void)flowie_endpoint_record_cstr_size(principal->roles[i], sizeof(principal->roles[i]), 1,
                                             &length);
      rc = flowie_endpoint_record_size_add(&required, length);
    }
    for (uint32_t i = 0u; rc == TURBO_OK && i < principal->group_count; ++i) {
      (void)flowie_endpoint_record_cstr_size(principal->groups[i], sizeof(principal->groups[i]), 1,
                                             &length);
      rc = flowie_endpoint_record_size_add(&required, length);
    }
  }
  if (rc == TURBO_OK) rc = flowie_endpoint_record_size_add(&required, owner_size);
  if (rc != TURBO_OK) goto done;
  if (endpoint->session_store && required > endpoint->session_store->max_value_size) {
    rc = TURBO_EMSGSIZE;
    goto done;
  }
  record = (uint8_t *)malloc(required);
  if (!record) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  flowie_endpoint_record_write_u64(metadata, expiry_at_epoch_seconds);
  metadata[8] = endpoint->security_enabled ? 1u : 0u;
  flowie_endpoint_record_write_u64(metadata + 9u, will_at_epoch_seconds);
  rc = flowie_endpoint_record_append(record, required, &offset, 1u, header, sizeof(header));
  if (rc == TURBO_OK)
    rc = flowie_endpoint_record_append(record, required, &offset, 2u, metadata, sizeof(metadata));
  if (rc == TURBO_OK && endpoint->security_enabled) {
    flowie_endpoint_record_write_u32(principal_metadata, (uint32_t)principal->scope);
    flowie_endpoint_record_write_u64(principal_metadata + 4u, principal->expires_at);
    flowie_endpoint_record_write_u64(principal_metadata + 12u, principal->policy_version);
    flowie_endpoint_record_write_u16(principal_metadata + 20u, (uint16_t)principal->role_count);
    flowie_endpoint_record_write_u16(principal_metadata + 22u, (uint16_t)principal->group_count);
    rc = flowie_endpoint_record_append(record, required, &offset, 3u, principal_metadata,
                                       sizeof(principal_metadata));
#define FLOWIE_APPEND_PRINCIPAL_FIELD(type_value, member_value)                                    \
  do {                                                                                             \
    if (rc == TURBO_OK)                                                                            \
      rc = flowie_endpoint_record_append(record, required, &offset, type_value,                    \
                                         (const uint8_t *)(member_value), strlen(member_value));   \
  } while (0)
    FLOWIE_APPEND_PRINCIPAL_FIELD(4u, principal->principal_id);
    FLOWIE_APPEND_PRINCIPAL_FIELD(5u, principal->principal_type);
    FLOWIE_APPEND_PRINCIPAL_FIELD(6u, principal->tenant_id);
    FLOWIE_APPEND_PRINCIPAL_FIELD(7u, principal->auth_method);
    for (uint32_t i = 0u; rc == TURBO_OK && i < principal->role_count; ++i)
      FLOWIE_APPEND_PRINCIPAL_FIELD(8u, principal->roles[i]);
    for (uint32_t i = 0u; rc == TURBO_OK && i < principal->group_count; ++i)
      FLOWIE_APPEND_PRINCIPAL_FIELD(9u, principal->groups[i]);
#undef FLOWIE_APPEND_PRINCIPAL_FIELD
  }
  if (rc == TURBO_OK)
    rc = flowie_endpoint_record_append(record, required, &offset, 10u, owner_record, owner_size);
  if (rc != TURBO_OK || offset != required) {
    if (rc == TURBO_OK) rc = TURBO_EPROTO;
    goto done;
  }
  *out = record;
  *out_size = required;
  record = NULL;

done:
  free(record);
  free(owner_record);
  return rc;
}

static int flowie_endpoint_record_copy_cstr(char *out, size_t capacity, const uint8_t *value,
                                            size_t value_size, int required) {
  if (!out || capacity == 0u || (!value && value_size != 0u) || value_size >= capacity ||
      (required && value_size == 0u) || (value_size != 0u && memchr(value, '\0', value_size)))
    return TURBO_EPROTO;
  if (value_size != 0u) memcpy(out, value, value_size);
  out[value_size] = '\0';
  return TURBO_OK;
}

static int flowie_endpoint_record_decode(const flowie_endpoint_t *endpoint,
                                         const turbo_flow_record_view_t *record,
                                         flowie_session_owner_t **owner_out,
                                         turbo_flow_security_principal_t *principal_out,
                                         uint64_t *expiry_at_out, uint64_t *will_at_out) {
  flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  flowie_session_owner_t *owner = NULL;
  size_t offset = 0u;
  uint8_t previous_type = 0u;
  uint16_t expected_roles = 0u;
  uint16_t expected_groups = 0u;
  uint16_t roles = 0u;
  uint16_t groups = 0u;
  uint64_t expiry_at = 0u;
  uint64_t will_at = 0u;
  uint8_t record_version = 0u;
  int header_seen = 0;
  int metadata_seen = 0;
  int principal_metadata_seen = 0;
  int owner_seen = 0;
  int rc = TURBO_EPROTO;
  if (owner_out) *owner_out = NULL;
  if (!endpoint || !record || record->size < sizeof(*record) || !record->key ||
      record->key_size == 0u || !record->value || record->value_size == 0u ||
      record->revision == 0u || record->revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
      !owner_out || !principal_out || !expiry_at_out || !will_at_out)
    return TURBO_EINVAL;
  config.owner_instance_id = endpoint->instance_id;
  config.session_id = 1u;
  config.max_subscriptions = endpoint->max_subscriptions_per_session;
  config.max_inflight = endpoint->max_inflight_per_session;
  config.settlement = endpoint->settlement;
  while (offset < record->value_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    const uint8_t *value;
    size_t value_size;
    rc = turbo_ltv_peek_size(record->value + offset, record->value_size - offset, &payload_size,
                             &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > record->value_size - offset ||
        payload_size > record->value_size - offset - header_size) {
      rc = TURBO_EPROTO;
      goto fail;
    }
    wire_size = header_size + payload_size;
    if (turbo_parse_ltv(record->value + offset, wire_size, &message) != TURBO_OK || !message) {
      turbo_free_ltv(&message);
      rc = TURBO_EPROTO;
      goto fail;
    }
    type = turbo_ltv_type(message);
    value = turbo_ltv_value(message);
    value_size = turbo_ltv_value_len(message);
    if (type == 0u || type > 10u || type < previous_type ||
        (type == previous_type && type != 8u && type != 9u)) {
      turbo_free_ltv(&message);
      rc = TURBO_EPROTO;
      goto fail;
    }
    if (type == 1u) {
      if (offset != 0u || value_size != FLOWIE_ENDPOINT_RECORD_HEADER_SIZE ||
          memcmp(value, "FSEP", 4u) != 0 || value[4] != 0u ||
          (value[5] != 1u && value[5] != FLOWIE_ENDPOINT_RECORD_VERSION) || value[6] != 0u ||
          value[7] != 0u) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      record_version = value[5];
      header_seen = 1;
    } else if (type == 2u) {
      size_t expected_size = record_version == 1u ? FLOWIE_ENDPOINT_RECORD_METADATA_V1_SIZE
                                                  : FLOWIE_ENDPOINT_RECORD_METADATA_SIZE;
      if (!header_seen || value_size != expected_size || value[8] > 1u ||
          value[8] != (uint8_t)endpoint->security_enabled) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      expiry_at = flowie_endpoint_record_read_u64(value);
      if (record_version >= 2u) will_at = flowie_endpoint_record_read_u64(value + 9u);
      metadata_seen = 1;
    } else if (type == 3u) {
      uint32_t scope;
      if (!metadata_seen || !endpoint->security_enabled ||
          value_size != FLOWIE_ENDPOINT_RECORD_PRINCIPAL_SIZE) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      scope = flowie_endpoint_record_read_u32(value);
      expected_roles = flowie_endpoint_record_read_u16(value + 20u);
      expected_groups = flowie_endpoint_record_read_u16(value + 22u);
      if (scope < TURBO_FLOW_SECURITY_SCOPE_SELF || scope > TURBO_FLOW_SECURITY_SCOPE_SYSTEM ||
          expected_roles > TURBO_FLOW_SECURITY_MAX_ROLES ||
          expected_groups > TURBO_FLOW_SECURITY_MAX_GROUPS) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      principal.scope = (turbo_flow_security_scope_t)scope;
      principal.expires_at = flowie_endpoint_record_read_u64(value + 4u);
      principal.policy_version = flowie_endpoint_record_read_u64(value + 12u);
      principal.role_count = expected_roles;
      principal.group_count = expected_groups;
      principal_metadata_seen = 1;
    } else if (type >= 4u && type <= 7u) {
      char *target = type == 4u   ? principal.principal_id
                     : type == 5u ? principal.principal_type
                     : type == 6u ? principal.tenant_id
                                  : principal.auth_method;
      size_t capacity = (type == 4u || type == 6u) ? TURBO_FLOW_SECURITY_ID_MAX + 1u
                                                   : TURBO_FLOW_SECURITY_TYPE_MAX + 1u;
      if (!principal_metadata_seen ||
          flowie_endpoint_record_copy_cstr(target, capacity, value, value_size, type != 6u) !=
              TURBO_OK) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
    } else if (type == 8u) {
      if (!principal_metadata_seen || roles >= expected_roles ||
          flowie_endpoint_record_copy_cstr(principal.roles[roles], sizeof(principal.roles[roles]),
                                           value, value_size, 1) != TURBO_OK) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      ++roles;
    } else if (type == 9u) {
      if (!principal_metadata_seen || roles != expected_roles || groups >= expected_groups ||
          flowie_endpoint_record_copy_cstr(principal.groups[groups],
                                           sizeof(principal.groups[groups]), value, value_size,
                                           1) != TURBO_OK) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      ++groups;
    } else {
      flowie_mqtt_span_t client_id = {record->key, record->key_size};
      if (!metadata_seen || owner_seen ||
          (endpoint->security_enabled &&
           (!principal_metadata_seen || roles != expected_roles || groups != expected_groups)) ||
          offset + wire_size != record->value_size) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      rc = flowie_session_owner_record_restore(&config, client_id, record->revision, value,
                                               value_size, &owner);
      if (rc != TURBO_OK) {
        turbo_free_ltv(&message);
        goto fail;
      }
      owner_seen = 1;
    }
    previous_type = type;
    turbo_free_ltv(&message);
    offset += wire_size;
  }
  if (!header_seen || !metadata_seen || !owner_seen ||
      (endpoint->security_enabled && (flowie_endpoint_principal_validate(&principal) != TURBO_OK ||
                                      roles != expected_roles || groups != expected_groups))) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  *owner_out = owner;
  *principal_out = principal;
  *expiry_at_out = expiry_at;
  *will_at_out = will_at;
  return TURBO_OK;

fail:
  flowie_session_owner_destroy(owner);
  return rc;
}

static int flowie_session_record_put(flowie_endpoint_t *endpoint,
                                     const flowie_session_owner_t *current,
                                     const flowie_session_owner_t *staged,
                                     const turbo_flow_security_principal_t *principal,
                                     uint64_t expiry_at_epoch_seconds,
                                     uint64_t will_at_epoch_seconds) {
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  uint8_t *value = NULL;
  size_t value_size = 0u;
  int rc;
  if (!endpoint || !staged) return TURBO_EINVAL;
  if (!endpoint->persistence_enabled) return TURBO_OK;
  if (!endpoint->session_store) return TURBO_EINVAL;
  if (current) {
    rc = flowie_session_owner_snapshot(current, &before);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_session_owner_snapshot(staged, &after);
  if (rc != TURBO_OK) return rc;
  if (!after.client_id.data || after.client_id.size == 0u ||
      after.client_id.size > endpoint->session_store->max_key_size ||
      after.resource_generation <= before.resource_generation ||
      after.resource_generation > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EPROTO;
  rc = flowie_endpoint_record_encode(endpoint, staged, principal, expiry_at_epoch_seconds,
                                     will_at_epoch_seconds, &value, &value_size);
  if (rc != TURBO_OK) return rc;
  mutation.key = after.client_id.data;
  mutation.key_size = after.client_id.size;
  mutation.expected_revision = before.resource_generation;
  mutation.next_revision = after.resource_generation;
  mutation.value = value;
  mutation.value_size = value_size;
  rc = endpoint->session_store->commit(endpoint->session_store->ctx, &mutation, 1u);
  free(value);
  return rc;
}

static int flowie_session_record_delete(flowie_endpoint_t *endpoint,
                                        const flowie_endpoint_session_t *session) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  int rc;
  if (!endpoint || !session) return TURBO_EINVAL;
  if (!endpoint->persistence_enabled) return TURBO_OK;
  rc = flowie_session_owner_snapshot(session->owner, &snapshot);
  if (rc != TURBO_OK || snapshot.resource_generation == 0u) return rc;
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = (const uint8_t *)session->client_id.data;
  mutation.key_size = session->client_id.len;
  mutation.expected_revision = snapshot.resource_generation;
  return endpoint->session_store->commit(endpoint->session_store->ctx, &mutation, 1u);
}

static int flowie_session_commit_staged(flowie_endpoint_t *endpoint,
                                        flowie_endpoint_session_t *session,
                                        flowie_session_owner_t *staged,
                                        const turbo_flow_security_principal_t *principal,
                                        uint64_t expiry_at_epoch_seconds,
                                        uint64_t will_at_epoch_seconds) {
  flowie_session_owner_t *previous;
  int rc;
  if (!endpoint || !session || !staged) return TURBO_EINVAL;
  rc = flowie_session_record_put(endpoint, session->owner, staged,
                                 principal ? principal : &session->principal,
                                 expiry_at_epoch_seconds, will_at_epoch_seconds);
  if (rc != TURBO_OK) return rc;
  previous = session->owner;
  session->owner = staged;
  if (principal) session->principal = *principal;
  session->expiry_at_epoch_seconds = expiry_at_epoch_seconds;
  session->will_at_epoch_seconds = will_at_epoch_seconds;
  flowie_session_owner_destroy(previous);
  return TURBO_OK;
}

static int flowie_security_authorize(flowie_endpoint_t *endpoint,
                                     const turbo_flow_security_principal_t *principal,
                                     uint32_t action,
                                     turbo_flow_security_resource_type_t resource_type,
                                     const char *resource, const void *protocol_context) {
  turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
  turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
  uint64_t now;
  if (!endpoint || !endpoint->security_enabled) return TURBO_OK;
  if (!principal || !resource || !resource[0] || !endpoint->security_realm) return TURBO_EINVAL;
  now = flowie_security_now_epoch_seconds();
  if (principal->expires_at != 0u && now == 0u) return TURBO_EIO;
  request.principal = principal;
  request.tenant_id = principal->tenant_id;
  request.action = action;
  request.resource_type = resource_type;
  request.resource = resource;
  request.protocol_context = protocol_context;
  return turbo_flow_security_realm_authorize(endpoint->security_realm, &request, now, &decision);
}

static int flowie_security_authenticate_connect(flowie_endpoint_t *endpoint,
                                                const flowie_mqtt_connect_view_t *connect,
                                                turbo_flow_security_principal_t *principal_out,
                                                uint8_t *reason_code_out) {
  turbo_flow_security_auth_request_t request = TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  tstr_t identity = NULL;
  tstr_t client_id = NULL;
  int rc;
  if (!endpoint || !connect || !principal_out || !reason_code_out || !endpoint->security_enabled)
    return TURBO_EINVAL;
  *reason_code_out = connect->version == FLOWIE_MQTT_VERSION_5 ? UINT8_C(0x86) : UINT8_C(0x04);
  if (!connect->username.data || connect->username.size == 0u) return TURBO_EPERM;
  identity = tstr_new_len(connect->username.data, connect->username.size);
  client_id = tstr_new_len(connect->client_id.data, connect->client_id.size);
  if (!identity || !client_id) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  request.identity = identity;
  request.method = endpoint->security_auth_method;
  request.secret = connect->password.data;
  request.secret_size = connect->password.size;
  request.protocol = connect->version == FLOWIE_MQTT_VERSION_5 ? "mqtt5" : "mqtt3.1.1";
  rc = turbo_flow_security_authenticate(&endpoint->auth_provider, &request, &principal);
  if (rc != TURBO_OK) goto done;
  *reason_code_out = connect->version == FLOWIE_MQTT_VERSION_5 ? UINT8_C(0x87) : UINT8_C(0x05);
  rc = client_id[0] == '\0'
           ? TURBO_EPERM
           : flowie_security_authorize(endpoint, &principal, TURBO_FLOW_SECURITY_ACTION_CONNECT,
                                       TURBO_FLOW_SECURITY_RESOURCE_GENERIC, client_id, NULL);
  if (rc == TURBO_OK) *principal_out = principal;

done:
  tstr_free(identity);
  tstr_free(client_id);
  return rc;
}

static int flowie_security_authorize_span(flowie_endpoint_connection_t *connection, uint32_t action,
                                          flowie_mqtt_span_t resource,
                                          flowie_mqtt_security_resource_kind_t kind) {
  flowie_mqtt_security_context_t context = FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
  tstr_t copied;
  if (!connection || !connection->endpoint || !connection->session || !resource.data ||
      resource.size == 0u)
    return TURBO_EINVAL;
  if (!connection->endpoint->security_enabled) return TURBO_OK;
  copied = tstr_cpy_len(connection->session->security_resource, (const char *)resource.data,
                        resource.size);
  if (!copied) return TURBO_ENOMEM;
  connection->session->security_resource = copied;
  context.kind = kind;
  return flowie_security_authorize(connection->endpoint, &connection->session->principal, action,
                                   TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC, copied, &context);
}

static int flowie_session_create(flowie_endpoint_t *endpoint,
                                 const flowie_mqtt_connect_view_t *connect,
                                 const turbo_flow_security_principal_t *principal,
                                 flowie_session_connect_result_t *decision,
                                 flowie_endpoint_session_t **out) {
  flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_endpoint_session_t *session;
  int rc;
  if (!endpoint || !connect || !decision || !out) return TURBO_EINVAL;
  *out = NULL;
  if (turbo_vec_size(&endpoint->sessions) >= endpoint->max_sessions) return TURBO_ENOSPC;
  if (endpoint->next_route_id == UINT64_MAX) return TURBO_ERANGE;
  session = (flowie_endpoint_session_t *)calloc(1u, sizeof(*session));
  if (!session) return TURBO_ENOMEM;
  session->principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  session->security_resource = tstr_new_len("", 0u);
  if (!session->security_resource) {
    free(session);
    return TURBO_ENOMEM;
  }
  config.owner_instance_id = endpoint->instance_id;
  config.session_id = ++endpoint->next_route_id;
  config.max_subscriptions = endpoint->max_subscriptions_per_session;
  config.max_inflight = endpoint->max_inflight_per_session;
  config.settlement = endpoint->settlement;
  session->owner = flowie_session_owner_create(&config);
  if (!session->owner) {
    free(session);
    return TURBO_ENOMEM;
  }
  rc = flowie_session_owner_connect(session->owner, connect, decision);
  if (rc != TURBO_OK || !decision->accepted) {
    flowie_session_destroy(session);
    return rc;
  }
  rc = flowie_session_owner_snapshot(session->owner, &snapshot);
  if (rc != TURBO_OK) {
    flowie_session_destroy(session);
    return rc;
  }
  if (endpoint->security_enabled) {
    if (!principal) {
      flowie_session_destroy(session);
      return TURBO_EINVAL;
    }
    session->principal = *principal;
  }
  rc = flowie_session_record_put(endpoint, NULL, session->owner, &session->principal, 0u, 0u);
  if (rc != TURBO_OK) {
    flowie_session_destroy(session);
    return rc;
  }
  session->client_id_owned = tstr_new_len(snapshot.client_id.data, snapshot.client_id.size);
  if (!session->client_id_owned) {
    flowie_session_destroy(session);
    return TURBO_ENOMEM;
  }
  session->client_id =
      tstr_v_from_buf(session->client_id_owned, tstr_len(session->client_id_owned));
  rc = turbo_vec_push(&endpoint->sessions, &session);
  if (rc == TURBO_OK)
    rc = turbo_hash_map_put(&endpoint->session_index, &session->client_id, &session);
  if (rc != TURBO_OK) {
    if (turbo_vec_size(&endpoint->sessions) != 0u) {
      flowie_endpoint_session_t *const *last =
          (flowie_endpoint_session_t *const *)turbo_vec_at_const(
              &endpoint->sessions, turbo_vec_size(&endpoint->sessions) - 1u);
      if (last && *last == session)
        (void)turbo_vec_resize(&endpoint->sessions, turbo_vec_size(&endpoint->sessions) - 1u);
    }
    flowie_session_destroy(session);
    return rc;
  }
  *out = session;
  atomic_store_explicit(&endpoint->sessions_current, turbo_vec_size(&endpoint->sessions),
                        memory_order_release);
  return TURBO_OK;
}

static tf_coronet_transport_t flowie_coronet_transport(flowie_transport_t transport) {
  switch (transport) {
  case FLOWIE_TRANSPORT_TCP:
    return TF_CORONET_TRANSPORT_TCP;
  case FLOWIE_TRANSPORT_TLS:
    return TF_CORONET_TRANSPORT_TLS;
  case FLOWIE_TRANSPORT_WS:
    return TF_CORONET_TRANSPORT_WS;
  case FLOWIE_TRANSPORT_WSS:
    return TF_CORONET_TRANSPORT_WSS;
  case FLOWIE_TRANSPORT_PIPE:
    return TF_CORONET_TRANSPORT_PIPE;
  default:
    return TF_CORONET_TRANSPORT_COUNT;
  }
}

static const char *flowie_transport_scheme(flowie_transport_t transport) {
  switch (transport) {
  case FLOWIE_TRANSPORT_TCP:
    return "tcp";
  case FLOWIE_TRANSPORT_TLS:
    return "tls";
  case FLOWIE_TRANSPORT_WS:
    return "ws";
  case FLOWIE_TRANSPORT_WSS:
    return "wss";
  case FLOWIE_TRANSPORT_PIPE:
    return "pipe";
  default:
    return NULL;
  }
}

static uint64_t flowie_timeout_ns(const flowie_endpoint_t *endpoint) {
  uint64_t timeout_ms = endpoint && endpoint->timeouts.timeout_ms
                            ? endpoint->timeouts.timeout_ms
                            : FLOWIE_ENDPOINT_DEFAULT_TIMEOUT_MS;
  return timeout_ms > UINT64_MAX / UINT64_C(1000000) ? UINT64_MAX : timeout_ms * UINT64_C(1000000);
}

static int flowie_endpoint_config_validate(const flowie_endpoint_config_t *config) {
  tf_coronet_transport_t transport;
  tf_coronet_socket_options_t options;
  int rc;
  if (!config || config->size < sizeof(*config) || config->abi_version != FLOWIE_ENDPOINT_ABI_V7) {
    return TURBO_EINVAL;
  }
  rc = turbo_flow_protocol_settlement_policy_validate(&config->settlement);
  if (rc != TURBO_OK) return rc;
  if (config->settlement.qos0 != TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED ||
      (config->settlement.qos1 != TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED &&
       config->settlement.qos1 != TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED &&
       config->settlement.qos1 != TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED &&
       config->settlement.qos1 != TURBO_FLOW_PROTOCOL_SETTLE_DURABLE) ||
      (config->settlement.qos2 != TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED &&
       config->settlement.qos2 != TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED &&
       config->settlement.qos2 != TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED &&
       config->settlement.qos2 != TURBO_FLOW_PROTOCOL_SETTLE_DURABLE))
    return TURBO_ENOTSUP;
  transport = flowie_coronet_transport(config->transport);
  if (transport == TF_CORONET_TRANSPORT_COUNT) return TURBO_ENOTSUP;
  if (config->transport == FLOWIE_TRANSPORT_PIPE && (!config->path || config->path[0] == '\0')) {
    return TURBO_EINVAL;
  }
  rc = tf_coronet_endpoint_config_validate(transport, config->host, config->port, config->path);
  if (rc != TURBO_OK) return rc;
  if (config->max_packet_size != 0u && config->max_packet_size < 2u) return TURBO_ERANGE;
  if (config->max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE) return TURBO_ERANGE;
  if (config->max_connections > FLOWIE_MAX_CONNECTIONS_LIMIT) return TURBO_ERANGE;
  if ((config->coroutine_stack_size != 0u &&
       config->coroutine_stack_size < FLOWIE_MIN_COROUTINE_STACK_SIZE) ||
      config->coroutine_stack_size > FLOWIE_MAX_COROUTINE_STACK_SIZE)
    return TURBO_ERANGE;
  if ((config->recv_buffer_size != 0u && config->recv_buffer_size < FLOWIE_MIN_RECV_BUFFER_SIZE) ||
      config->recv_buffer_size > FLOWIE_MAX_RECV_BUFFER_SIZE)
    return TURBO_ERANGE;
  if ((config->max_connections ? config->max_connections : FLOWIE_DEFAULT_MAX_CONNECTIONS) >
      SIZE_MAX - FLOWIE_PRIVATE_COROUTINE_HEADROOM)
    return TURBO_ERANGE;
  if (config->slow_subscriber_policy != FLOWIE_SLOW_SUBSCRIBER_POLICY_UNSPECIFIED &&
      config->slow_subscriber_policy != FLOWIE_SLOW_SUBSCRIBER_DISCONNECT)
    return TURBO_ENOTSUP;
  if (!config->manage_sessions &&
      (config->max_sessions != 0u || config->max_subscriptions_per_session != 0u ||
       config->max_inflight_per_session != 0u || config->max_retained_messages != 0u ||
       config->slow_subscriber_policy != FLOWIE_SLOW_SUBSCRIBER_POLICY_UNSPECIFIED ||
       config->settlement.qos1 != TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED ||
       config->settlement.qos2 != TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED))
    return TURBO_EINVAL;
  if (config->manage_sessions &&
      (config->max_subscriptions_per_session > FLOWIE_SESSION_INTERNAL_MAX_SUBSCRIPTIONS ||
       config->max_inflight_per_session > UINT16_MAX))
    return TURBO_ERANGE;
  if (config->take_context_ownership && !config->context) return TURBO_EINVAL;
  memset(&options, 0, sizeof(options));
  options.tcp_keepalive = config->tcp_keepalive;
  options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
  options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
  options.tcp_keepalive_count = config->tcp_keepalive_count;
  options.linger = config->linger;
  options.linger_ms = config->linger_ms;
  options.send_hwm_bytes = config->send_hwm_bytes;
  rc = tf_coronet_socket_options_validate(transport, &options);
  if (rc != TURBO_OK) return rc;
  return tf_coronet_reuse_port_validate(transport, config->reuse_port, 1);
}

static int
flowie_endpoint_security_binding_validate(const flowie_endpoint_config_t *config,
                                          const flowie_endpoint_security_binding_t *security) {
  if (!security || security->size < sizeof(*security) || !security->realm_channel ||
      !security->realm_channel[0] || !security->auth_method || !security->auth_method[0] ||
      !security->auth_provider ||
      security->auth_provider->size < sizeof(*security->auth_provider) ||
      !security->auth_provider->authenticate || !security->realm) {
    return TURBO_EINVAL;
  }
  return config && config->manage_sessions ? TURBO_OK : TURBO_ENOTSUP;
}

static int flowie_endpoint_persistence_binding_validate(
    const flowie_endpoint_config_t *config,
    const flowie_endpoint_persistence_binding_t *persistence) {
  const turbo_flow_record_store_t *store;
  size_t max_sessions;
  size_t max_retained_messages;
  size_t required_records;
  size_t required_value_size;
  size_t max_packet_size;
  const uint32_t required = TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  if (!config || !persistence || persistence->size < sizeof(*persistence) ||
      !persistence->store_channel || !persistence->store_channel[0] ||
      !(store = persistence->store) || store->size < sizeof(*store) ||
      store->api_version != TURBO_FLOW_RECORD_STORE_API_VERSION ||
      (store->capabilities & required) != required || !store->ctx || !store->scan ||
      !store->commit || store->max_key_size == 0u || store->max_value_size == 0u ||
      store->max_batch_size == 0u || store->max_records == 0u)
    return TURBO_EINVAL;
  if (!config->manage_sessions) return TURBO_ENOTSUP;
  max_sessions = config->max_sessions ? config->max_sessions
                                      : (config->max_connections ? config->max_connections
                                                                 : FLOWIE_DEFAULT_MAX_CONNECTIONS);
  max_retained_messages =
      config->max_retained_messages ? config->max_retained_messages : max_sessions;
  if (max_sessions > SIZE_MAX - max_retained_messages) return TURBO_ERANGE;
  required_records = max_sessions + max_retained_messages;
  max_packet_size =
      config->max_packet_size ? config->max_packet_size : FLOWIE_DEFAULT_MAX_PACKET_SIZE;
  required_value_size = turbo_ltv_wire_size(FLOWIE_RETAINED_RECORD_HEADER_SIZE);
  if (required_value_size == 0u ||
      required_value_size > SIZE_MAX - turbo_ltv_wire_size(FLOWIE_RETAINED_RECORD_METADATA_SIZE))
    return TURBO_ERANGE;
  required_value_size += turbo_ltv_wire_size(FLOWIE_RETAINED_RECORD_METADATA_SIZE);
  if (turbo_ltv_wire_size(max_packet_size) == 0u ||
      required_value_size > SIZE_MAX - turbo_ltv_wire_size(max_packet_size))
    return TURBO_ERANGE;
  required_value_size += turbo_ltv_wire_size(max_packet_size);
  if (store->max_records < required_records ||
      store->max_key_size < FLOWIE_RETAINED_KEY_PREFIX_SIZE + FLOWIE_MQTT_MAX_UTF8_SIZE ||
      store->max_value_size < required_value_size)
    return TURBO_ENOSPC;
  return TURBO_OK;
}

static void flowie_task_begin(flowie_endpoint_t *endpoint) {
  turbo_mutex_lock(&endpoint->task_mutex);
  endpoint->task_count += 1u;
  turbo_mutex_unlock(&endpoint->task_mutex);
}

static void flowie_task_end(flowie_endpoint_t *endpoint) {
  turbo_mutex_lock(&endpoint->task_mutex);
  if (endpoint->task_count > 0u) endpoint->task_count -= 1u;
  if (endpoint->task_count == 0u) turbo_cond_broadcast(&endpoint->task_drained);
  turbo_mutex_unlock(&endpoint->task_mutex);
}

static void flowie_wait_tasks(flowie_endpoint_t *endpoint) {
  turbo_mutex_lock(&endpoint->task_mutex);
  while (endpoint->task_count != 0u)
    turbo_cond_wait(&endpoint->task_drained, &endpoint->task_mutex);
  turbo_mutex_unlock(&endpoint->task_mutex);
}

static void flowie_expiry_task(coro_t *co, void *arg) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)arg;
  (void)co;
  while (atomic_load_explicit(&endpoint->started, memory_order_acquire)) {
    uint64_t earliest = UINT64_MAX;
    uint64_t now = turbo_hrtime();
    size_t index = 0u;
    while (index < turbo_vec_size(&endpoint->sessions)) {
      flowie_endpoint_session_t **slot =
          (flowie_endpoint_session_t **)turbo_vec_at(&endpoint->sessions, index);
      flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
      flowie_endpoint_session_t *session;
      if (!slot || !(session = *slot)) {
        ++index;
        continue;
      }
      if (flowie_session_owner_snapshot(session->owner, &snapshot) != TURBO_OK || snapshot.active) {
        session->expiry_deadline_ns = 0u;
        session->expiry_session_generation = 0u;
        session->will_deadline_ns = 0u;
        session->will_session_generation = 0u;
        ++index;
        continue;
      }
      if ((session->expiry_deadline_ns != 0u &&
           snapshot.session_generation != session->expiry_session_generation) ||
          (session->will_deadline_ns != 0u &&
           snapshot.session_generation != session->will_session_generation)) {
        session->expiry_deadline_ns = 0u;
        session->expiry_session_generation = 0u;
        session->will_deadline_ns = 0u;
        session->will_session_generation = 0u;
        ++index;
        continue;
      }
      if (session->will_deadline_ns != 0u) {
        if (session->will_deadline_ns <= now) {
          int will_rc = flowie_session_will_publish(endpoint, session);
          if (will_rc == TURBO_ENOENT) {
            session->will_deadline_ns = 0u;
            session->will_session_generation = 0u;
          } else if (will_rc != TURBO_OK) {
            session->will_deadline_ns =
                now > UINT64_MAX - UINT64_C(1000000000) ? UINT64_MAX : now + UINT64_C(1000000000);
          }
        }
        if (session->will_deadline_ns != 0u && session->will_deadline_ns < earliest)
          earliest = session->will_deadline_ns;
      }
      if (session->expiry_deadline_ns == 0u) {
        ++index;
        continue;
      }
      if (session->expiry_deadline_ns <= now) {
        /* MQTT requires a pending Will to be published before an earlier/equal
         * session end removes its state. A failed graph/store attempt therefore
         * keeps the session until the scheduled Will retry succeeds. */
        if (session->will_deadline_ns != 0u) {
          ++index;
          continue;
        }
        if (endpoint->persistence_enabled &&
            flowie_session_record_delete(endpoint, session) != TURBO_OK) {
          session->expiry_deadline_ns =
              now > UINT64_MAX - UINT64_C(1000000000) ? UINT64_MAX : now + UINT64_C(1000000000);
          if (session->expiry_deadline_ns < earliest) earliest = session->expiry_deadline_ns;
          ++index;
          continue;
        }
        flowie_session_remove_owned(endpoint, session);
        continue;
      }
      if (session->expiry_deadline_ns < earliest) earliest = session->expiry_deadline_ns;
      ++index;
    }
    if (earliest == UINT64_MAX) {
      (void)coro_wait_for(endpoint->expiry_wait, UINT64_MAX);
      continue;
    }
    {
      uint64_t remaining = earliest > now ? earliest - now : 0u;
      uint64_t wait_ms =
          remaining / UINT64_C(1000000) + (remaining % UINT64_C(1000000) != 0u ? 1u : 0u);
      if (wait_ms == 0u) wait_ms = 1u;
      (void)coro_wait_for(endpoint->expiry_wait, wait_ms);
    }
  }
  endpoint->expiry_task_active = 0;
  flowie_task_end(endpoint);
}

static int flowie_expiry_schedule(flowie_endpoint_t *endpoint) {
  int rc;
  if (!endpoint || !endpoint->expiry_wait) return TURBO_EINVAL;
  if (endpoint->expiry_task_active) {
    (void)coro_wait_interrupt(endpoint->expiry_wait, TURBO_EINTR);
    return TURBO_OK;
  }
  endpoint->expiry_task_active = 1;
  flowie_task_begin(endpoint);
  rc = coro_context_spawn(endpoint->ctx, flowie_expiry_task, endpoint);
  if (rc == TURBO_OK) return TURBO_OK;
  endpoint->expiry_task_active = 0;
  flowie_task_end(endpoint);
  return rc;
}

static int flowie_session_expiry_at_compute(const flowie_session_snapshot_t *snapshot,
                                            uint64_t *out) {
  uint64_t now;
  if (!snapshot || !out || snapshot->active || snapshot->session_expiry_interval == 0u)
    return TURBO_EINVAL;
  if (snapshot->session_expiry_interval == UINT32_MAX) {
    *out = 0u;
    return TURBO_OK;
  }
  now = flowie_security_now_epoch_seconds();
  if (now == 0u) return TURBO_EIO;
  *out = now > UINT64_MAX - snapshot->session_expiry_interval
             ? UINT64_MAX
             : now + snapshot->session_expiry_interval;
  return TURBO_OK;
}

static int flowie_session_expiry_arm(flowie_endpoint_t *endpoint,
                                     flowie_endpoint_session_t *session,
                                     const flowie_session_snapshot_t *snapshot) {
  uint64_t duration_ns;
  uint64_t now;
  uint64_t epoch_now;
  if (!endpoint || !session || !snapshot || snapshot->active) return TURBO_EINVAL;
  if (snapshot->session_expiry_interval == 0u || snapshot->session_expiry_interval == UINT32_MAX)
    return TURBO_EINVAL;
  duration_ns = (uint64_t)snapshot->session_expiry_interval * UINT64_C(1000000000);
  now = turbo_hrtime();
  epoch_now = flowie_security_now_epoch_seconds();
  if (epoch_now == 0u) return TURBO_EIO;
  session->expiry_deadline_ns = now > UINT64_MAX - duration_ns ? UINT64_MAX : now + duration_ns;
  if (session->expiry_at_epoch_seconds == 0u)
    session->expiry_at_epoch_seconds = epoch_now > UINT64_MAX - snapshot->session_expiry_interval
                                           ? UINT64_MAX
                                           : epoch_now + snapshot->session_expiry_interval;
  session->expiry_session_generation = snapshot->session_generation;
  return flowie_expiry_schedule(endpoint);
}

static int flowie_session_will_at_compute(const flowie_session_snapshot_t *snapshot,
                                          uint64_t *out) {
  uint32_t delay;
  uint64_t now;
  if (!snapshot || !out || snapshot->active || !snapshot->has_will || !snapshot->will_pending)
    return TURBO_EINVAL;
  delay = snapshot->will_delay_interval;
  if (snapshot->session_expiry_interval != UINT32_MAX && snapshot->session_expiry_interval < delay)
    delay = snapshot->session_expiry_interval;
  now = flowie_security_now_epoch_seconds();
  if (now == 0u) return TURBO_EIO;
  *out = now > UINT64_MAX - delay ? UINT64_MAX : now + delay;
  return TURBO_OK;
}

static int flowie_session_will_arm(flowie_endpoint_t *endpoint, flowie_endpoint_session_t *session,
                                   const flowie_session_snapshot_t *snapshot) {
  uint64_t epoch_now;
  uint64_t now;
  uint64_t remaining;
  uint64_t duration_ns;
  int rc;
  if (!endpoint || !session || !snapshot || snapshot->active || !snapshot->has_will ||
      !snapshot->will_pending)
    return TURBO_EINVAL;
  if (session->will_at_epoch_seconds == 0u) {
    rc = flowie_session_will_at_compute(snapshot, &session->will_at_epoch_seconds);
    if (rc != TURBO_OK) return rc;
  }
  epoch_now = flowie_security_now_epoch_seconds();
  if (epoch_now == 0u) return TURBO_EIO;
  remaining =
      session->will_at_epoch_seconds > epoch_now ? session->will_at_epoch_seconds - epoch_now : 0u;
  duration_ns =
      remaining > UINT64_MAX / UINT64_C(1000000000) ? UINT64_MAX : remaining * UINT64_C(1000000000);
  now = turbo_hrtime();
  session->will_deadline_ns = now > UINT64_MAX - duration_ns ? UINT64_MAX : now + duration_ns;
  if (session->will_deadline_ns == 0u) session->will_deadline_ns = 1u;
  session->will_session_generation = snapshot->session_generation;
  return flowie_expiry_schedule(endpoint);
}

static int flowie_session_close_schedule(flowie_endpoint_t *endpoint,
                                         flowie_endpoint_session_t *session) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_owner_t *closing_owner;
  flowie_session_owner_t *staged = NULL;
  uint64_t expiry_at = 0u;
  uint64_t will_at = 0u;
  uint64_t now_ns;
  int rc;
  if (!endpoint || !session || !session->owner) return TURBO_EINVAL;
  closing_owner = session->owner;
  if (endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(session->owner);
    if (!staged) return TURBO_ENOMEM;
    closing_owner = staged;
  }
  rc = flowie_session_owner_close(closing_owner);
  if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(closing_owner, &snapshot);
  if (rc != TURBO_OK) goto done;

  if (snapshot.session_expiry_interval == 0u && !snapshot.will_pending) {
    rc = flowie_session_record_delete(endpoint, session);
    if (rc == TURBO_OK) flowie_session_remove(endpoint, session);
    goto done;
  }
  if (snapshot.session_expiry_interval == 0u) {
    expiry_at = flowie_security_now_epoch_seconds();
    if (expiry_at == 0u) {
      rc = TURBO_EIO;
      goto done;
    }
  } else if (snapshot.session_expiry_interval != UINT32_MAX) {
    rc = flowie_session_expiry_at_compute(&snapshot, &expiry_at);
    if (rc != TURBO_OK) goto done;
  }
  if (snapshot.will_pending) {
    rc = flowie_session_will_at_compute(&snapshot, &will_at);
    if (rc != TURBO_OK) goto done;
    if (expiry_at != 0u && will_at > expiry_at) will_at = expiry_at;
  }
  if (staged) {
    rc = flowie_session_commit_staged(endpoint, session, staged, NULL, expiry_at, will_at);
    if (rc != TURBO_OK) goto done;
    staged = NULL;
  } else {
    session->expiry_at_epoch_seconds = expiry_at;
    session->will_at_epoch_seconds = will_at;
  }

  if (snapshot.session_expiry_interval == 0u) {
    now_ns = turbo_hrtime();
    session->expiry_deadline_ns = now_ns == 0u ? 1u : now_ns;
    session->expiry_session_generation = snapshot.session_generation;
  } else if (snapshot.session_expiry_interval != UINT32_MAX) {
    rc = flowie_session_expiry_arm(endpoint, session, &snapshot);
    if (rc != TURBO_OK) goto done;
  }
  if (snapshot.will_pending) {
    rc = flowie_session_will_arm(endpoint, session, &snapshot);
    if (rc != TURBO_OK) goto done;
  } else if (snapshot.session_expiry_interval == 0u) {
    rc = flowie_expiry_schedule(endpoint);
  }

done:
  flowie_session_owner_destroy(staged);
  return rc;
}

static void flowie_connection_usage(flowie_endpoint_t *endpoint) {
  tf_io_budget_snapshot_t budget = {0};
  (void)tf_io_budget_snapshot(&endpoint->send_budget, &budget);
  tf_connection_set_usage(&endpoint->connection, turbo_vec_size(&endpoint->clients),
                          budget.messages, budget.bytes);
}

static int flowie_client_add(flowie_endpoint_t *endpoint, coro_socket_t *socket,
                             flowie_endpoint_connection_t **out) {
  flowie_endpoint_connection_t *connection;
  tf_io_budget_config_t budget_config;
  uint64_t generation;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (turbo_vec_size(&endpoint->clients) >= endpoint->max_connections) return TURBO_ENOBUFS;
  if (endpoint->next_route_id == UINT64_MAX) return TURBO_ERANGE;
  connection = (flowie_endpoint_connection_t *)calloc(1, sizeof(*connection));
  if (!connection) return TURBO_ENOMEM;
  connection->endpoint = endpoint;
  connection->socket = socket;
  connection->route = (turbo_flow_protocol_route_t)TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  connection->route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  connection->route.owner_instance_id = endpoint->instance_id;
  connection->route.session_id = ++endpoint->next_route_id;
  generation = atomic_load_explicit(&endpoint->generation, memory_order_acquire);
  connection->route.session_generation = generation ? generation : 1u;
  if (flowie_reply_queue_t_init(&connection->send_queue) != TURBO_OK) {
    free(connection);
    return TURBO_ENOMEM;
  }
  connection->send_queue_initialized = 1;
  memset(&budget_config, 0, sizeof(budget_config));
  budget_config.max_bytes = endpoint->send_hwm_bytes;
  budget_config.admission = TF_IO_ADMISSION_FAIL;
  rc = tf_io_budget_init(&connection->send_budget, &budget_config);
  if (rc != TURBO_OK) {
    flowie_reply_queue_t_destroy(&connection->send_queue);
    free(connection);
    return rc;
  }
  connection->send_budget_initialized = 1;
  rc = tf_io_budget_open(&connection->send_budget);
  if (rc != TURBO_OK) {
    tf_io_budget_destroy(&connection->send_budget);
    flowie_reply_queue_t_destroy(&connection->send_queue);
    free(connection);
    return rc;
  }
  rc = turbo_vec_push(&endpoint->clients, &connection);
  if (rc != TURBO_OK) {
    tf_io_budget_destroy(&connection->send_budget);
    flowie_reply_queue_t_destroy(&connection->send_queue);
    free(connection);
    return rc;
  }
  rc = flowie_route_map_t_put(&endpoint->routes, connection->route.session_id, connection);
  if (rc != TURBO_OK) {
    (void)turbo_vec_resize(&endpoint->clients, turbo_vec_size(&endpoint->clients) - 1u);
    tf_io_budget_destroy(&connection->send_budget);
    flowie_reply_queue_t_destroy(&connection->send_queue);
    free(connection);
    return rc;
  }
  flowie_connection_usage(endpoint);
  *out = connection;
  return rc;
}

static void flowie_client_remove(flowie_endpoint_t *endpoint,
                                 flowie_endpoint_connection_t *connection) {
  size_t count = turbo_vec_size(&endpoint->clients);
  for (size_t i = 0u; i < count; ++i) {
    flowie_endpoint_connection_t **slot =
        (flowie_endpoint_connection_t **)turbo_vec_at(&endpoint->clients, i);
    if (!slot || *slot != connection) continue;
    (void)flowie_route_map_t_remove(&endpoint->routes, connection->route.session_id, NULL);
    if (i + 1u < count) {
      flowie_endpoint_connection_t *const *last =
          (flowie_endpoint_connection_t *const *)turbo_vec_at_const(&endpoint->clients, count - 1u);
      *slot = last ? *last : NULL;
    }
    (void)turbo_vec_resize(&endpoint->clients, count - 1u);
    flowie_connection_usage(endpoint);
    return;
  }
}

static flowie_endpoint_connection_t *
flowie_connection_find(flowie_endpoint_t *endpoint, const turbo_flow_protocol_route_t *route) {
  flowie_endpoint_connection_t **slot;
  flowie_endpoint_connection_t *connection;
  turbo_flow_pattern_route_t requested = TURBO_FLOW_PATTERN_ROUTE_INIT;
  turbo_flow_pattern_route_t candidate = TURBO_FLOW_PATTERN_ROUTE_INIT;
  if (!endpoint || !route || route->protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      route->owner_instance_id != endpoint->instance_id)
    return NULL;
  slot = flowie_route_map_t_get(&endpoint->routes, route->session_id);
  connection = slot ? *slot : NULL;
  if (!connection || connection->closing) return NULL;
  requested.route_id = route->session_id;
  requested.generation = route->session_generation;
  candidate.route_id = connection->route.session_id;
  candidate.generation = connection->route.session_generation;
  return turbo_flow_pattern_routes_match(&requested, &candidate) ? connection : NULL;
}

static void flowie_reply_request_release(flowie_endpoint_t *endpoint,
                                         flowie_reply_request_t *request) {
  if (!request) return;
  tstr_freep(&request->packet);
  if (request->reserved_bytes != 0u)
    (void)tf_io_budget_release(&endpoint->send_budget, request->reserved_bytes);
  free(request);
}

static void flowie_connection_reply_request_release(flowie_endpoint_connection_t *connection,
                                                    flowie_reply_request_t *request) {
  if (!connection || !request) return;
  if (request->preserve_on_terminal_error && connection->terminal_reply_count != 0u)
    connection->terminal_reply_count -= 1u;
  if (request->connection_reserved_bytes != 0u) {
    (void)tf_io_budget_release(&connection->send_budget, request->connection_reserved_bytes);
    request->connection_reserved_bytes = 0u;
  }
  flowie_reply_request_release(connection->endpoint, request);
}

static void flowie_connection_fail_reply_queue(flowie_endpoint_connection_t *connection) {
  flowie_reply_request_t *request = NULL;
  if (!connection || !connection->send_queue_initialized) return;
  while (flowie_reply_queue_t_pop_front(&connection->send_queue, &request))
    flowie_connection_reply_request_release(connection, request);
}

static void flowie_connection_close(flowie_endpoint_connection_t *connection, int status) {
  if (!connection) return;
  connection->closing = 1;
  if (connection->send_budget_initialized) tf_io_budget_close(&connection->send_budget);
  if (connection->socket) (void)coro_socket_interrupt_wait(connection->socket, status);
  flowie_connection_fail_reply_queue(connection);
}

static void flowie_connection_close_after_terminal_replies(flowie_endpoint_connection_t *connection,
                                                           int status) {
  if (!connection) return;
  if (connection->terminal_reply_count == 0u) {
    flowie_connection_close(connection, status);
    return;
  }
  connection->close_when_replies_drain = 1;
  connection->close_when_replies_drain_status = status;
}

static void flowie_slow_subscriber_disconnect(flowie_endpoint_connection_t *connection,
                                              int status) {
  uint_fast64_t current;
  if (!connection || connection->closing) return;
  current = atomic_load_explicit(&connection->endpoint->slow_subscriber_disconnects,
                                 memory_order_relaxed);
  while (current != UINT_FAST64_MAX &&
         !atomic_compare_exchange_weak_explicit(&connection->endpoint->slow_subscriber_disconnects,
                                                &current, current + 1u, memory_order_relaxed,
                                                memory_order_relaxed)) {
  }
  flowie_connection_close(connection, status);
}

static int flowie_reply_packet_validate(flowie_endpoint_connection_t *connection, tstr_t packet) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t envelope = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  size_t consumed = 0u;
  int rc;
  if (!connection || connection->version == FLOWIE_MQTT_VERSION_UNSPECIFIED || !packet)
    return TURBO_EBUSY;
  options.version = connection->version;
  options.max_packet_size = connection->endpoint->max_packet_size;
  rc = flowie_mqtt_packet_parse((const uint8_t *)packet, tstr_len(packet), &options, &envelope,
                                &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != tstr_len(packet)) return TURBO_EPROTO;
  if (envelope.type == FLOWIE_MQTT_PACKET_PUBLISH) {
    if (!connection->connack_admitted) return TURBO_EBUSY;
    return flowie_mqtt_publish_parse(&envelope, &publish) == FLOWIE_MQTT_PARSE_OK ? TURBO_OK
                                                                                  : TURBO_EPROTO;
  }
  rc = flowie_mqtt_control_packet_parse(&envelope, &control);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  if (control.type == FLOWIE_MQTT_PACKET_CONNACK) {
    if (connection->connack_admitted) return TURBO_EPROTO;
  } else if (control.type != FLOWIE_MQTT_PACKET_AUTH && !connection->connack_admitted) {
    return TURBO_EBUSY;
  }
  return TURBO_OK;
}

static int flowie_fanout_target_add(turbo_vec_t *targets, roaring64_bitmap_t *selected,
                                    turbo_hash_map_t *target_index,
                                    const flowie_subscription_member_t *member, int merge) {
  flowie_fanout_target_t *existing;
  flowie_fanout_target_t target;
  size_t index;
  int rc;
  if (!targets || !member || !member->session) return TURBO_EINVAL;
  existing = NULL;
  if (merge && selected && roaring64_bitmap_contains(selected, member->session_id)) {
    const size_t *found =
        (const size_t *)turbo_hash_map_get_const(target_index, &member->session_id);
    if (!found) return TURBO_EPROTO;
    existing = (flowie_fanout_target_t *)turbo_vec_at(targets, *found);
    if (!existing || existing->session != member->session) return TURBO_EPROTO;
  }
  if (existing) {
    if (member->qos > existing->qos) existing->qos = member->qos;
    if (member->retain_as_published) existing->retain_as_published = 1u;
    return TURBO_OK;
  }
  memset(&target, 0, sizeof(target));
  target.session = member->session;
  target.qos = member->qos;
  target.retain_as_published = member->retain_as_published;
  rc = turbo_vec_push(targets, &target);
  if (rc != TURBO_OK) return rc;
  if (!merge) return TURBO_OK;
  index = turbo_vec_size(targets) - 1u;
  rc = turbo_hash_map_put(target_index, &member->session_id, &index);
  if (rc != TURBO_OK) {
    (void)turbo_vec_resize(targets, index);
    return rc;
  }
  roaring64_bitmap_add(selected, member->session_id);
  return TURBO_OK;
}

static int flowie_fanout_select(flowie_endpoint_t *endpoint, uint64_t publisher_session_id,
                                flowie_mqtt_span_t topic, turbo_vec_t *targets) {
  int rc;
  roaring64_bitmap_t *normal_selected;
  turbo_hash_map_t target_index;
  turbo_vec_t matched_entries;
  if (!endpoint || !topic.data || topic.size == 0u || !targets) return TURBO_EINVAL;
  if (!endpoint->subscription_index_valid) {
    rc = flowie_subscription_index_rebuild(endpoint);
    if (rc != TURBO_OK) return rc;
  }
  normal_selected = roaring64_bitmap_create();
  if (!normal_selected) return TURBO_ENOMEM;
  rc = turbo_hash_map_init(&target_index, sizeof(uint64_t), sizeof(size_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) {
    roaring64_bitmap_free(normal_selected);
    return rc;
  }
  rc = turbo_vec_init(&matched_entries, sizeof(size_t));
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&target_index);
    roaring64_bitmap_free(normal_selected);
    return rc;
  }
  rc = flowie_topic_index_match(&endpoint->subscription_topics, topic, &matched_entries);
  if (rc != TURBO_OK) goto done;
  for (size_t match_index = 0u; match_index < turbo_vec_size(&matched_entries); ++match_index) {
    const size_t *entry_index = (const size_t *)turbo_vec_at_const(&matched_entries, match_index);
    flowie_subscription_entry_t *entry =
        entry_index ? (flowie_subscription_entry_t *)turbo_vec_at(&endpoint->subscription_index,
                                                                  *entry_index)
                    : NULL;
    flowie_mqtt_span_t filter;
    int matched = 0;
    if (!entry || !entry->filter || !entry->session_ids) {
      rc = TURBO_EPROTO;
      goto done;
    }
    filter.data = (const uint8_t *)entry->filter;
    filter.size = tstr_len(entry->filter);
    rc = flowie_mqtt_topic_matches(filter, topic, &matched);
    if (rc != FLOWIE_MQTT_PARSE_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (!matched) continue;
    if (!entry->shared) {
      turbo_flow_pattern_selection_iterator_t selection =
          TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
      size_t member_index;
      rc = turbo_flow_pattern_selection_begin(&entry->selector, TURBO_FLOW_PATTERN_SELECT_FAN_OUT,
                                              turbo_vec_size(&entry->members), &selection);
      if (rc == TURBO_ENOENT) continue;
      if (rc != TURBO_OK) goto done;
      while ((rc = turbo_flow_pattern_selection_next(&selection, &member_index)) == TURBO_OK) {
        const flowie_subscription_member_t *member =
            (const flowie_subscription_member_t *)turbo_vec_at_const(&entry->members, member_index);
        turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
        if (!member || !member->session ||
            !roaring64_bitmap_contains(entry->session_ids, member->session_id)) {
          rc = TURBO_EPROTO;
          goto done;
        }
        if (member->no_local && member->session_id == publisher_session_id) continue;
        if (flowie_session_owner_route(member->session->owner, &route) != TURBO_OK) continue;
        rc = flowie_fanout_target_add(targets, normal_selected, &target_index, member, 1);
        if (rc != TURBO_OK) goto done;
      }
      if (rc != TURBO_ENOENT) goto done;
      rc = TURBO_OK;
    } else {
      uint64_t member_count = roaring64_bitmap_get_cardinality(entry->session_ids);
      turbo_flow_pattern_selection_iterator_t selection =
          TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
      size_t candidate_index;
      if (member_count == 0u) continue;
      if (member_count > SIZE_MAX) {
        rc = TURBO_ERANGE;
        goto done;
      }
      rc = turbo_flow_pattern_selection_begin(&entry->selector,
                                              TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN,
                                              (size_t)member_count, &selection);
      if (rc != TURBO_OK) goto done;
      while ((rc = turbo_flow_pattern_selection_next(&selection, &candidate_index)) == TURBO_OK) {
        uint64_t session_id;
        const size_t *member_index;
        const flowie_subscription_member_t *member = NULL;
        turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
        if (!roaring64_bitmap_select(entry->session_ids, (uint64_t)candidate_index, &session_id)) {
          rc = TURBO_EPROTO;
          goto done;
        }
        member_index = (const size_t *)turbo_hash_map_get_const(&entry->member_index, &session_id);
        if (member_index)
          member = (const flowie_subscription_member_t *)turbo_vec_at_const(&entry->members,
                                                                            *member_index);
        if (!member || !member->session || member->session_id != session_id) {
          rc = TURBO_EPROTO;
          goto done;
        }
        if (member->no_local && member->session_id == publisher_session_id) continue;
        if (flowie_session_owner_route(member->session->owner, &route) != TURBO_OK) continue;
        rc = flowie_fanout_target_add(targets, NULL, NULL, member, 0);
        if (rc != TURBO_OK) goto done;
        break;
      }
      if (rc == TURBO_ENOENT) rc = TURBO_OK;
      if (rc != TURBO_OK) goto done;
    }
  }
  rc = roaring64_bitmap_get_cardinality(normal_selected) ==
               (uint64_t)turbo_hash_map_size(&target_index)
           ? TURBO_OK
           : TURBO_EPROTO;

done:
  turbo_vec_destroy(&matched_entries);
  turbo_hash_map_destroy(&target_index);
  roaring64_bitmap_free(normal_selected);
  return rc;
}

static int flowie_publish_forward_properties(const flowie_mqtt_publish_view_t *publish,
                                             int override_expiry, uint32_t expiry_interval,
                                             tstr_t *out) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  tstr_t filtered;
  size_t written = 0u;
  int rc;
  if (!publish || !out) return TURBO_EINVAL;
  *out = NULL;
  filtered = tstr_new_len(NULL, publish->properties.values.size);
  if (!filtered) return TURBO_ENOMEM;
  rc = flowie_mqtt_property_iterator_init(&publish->properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    tstr_free(filtered);
    return TURBO_EPROTO;
  }
  for (;;) {
    const uint8_t *begin = iterator.cursor;
    rc = flowie_mqtt_property_iterator_next(&iterator, &property);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK || !begin || iterator.cursor < begin) {
      tstr_free(filtered);
      return TURBO_EPROTO;
    }
    if (property.identifier == FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS ||
        property.identifier == FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER)
      continue;
    if (override_expiry && property.identifier == FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL) {
      size_t property_size = (size_t)(iterator.cursor - begin);
      if (property_size < sizeof(uint32_t)) {
        tstr_free(filtered);
        return TURBO_EPROTO;
      }
      memcpy(filtered + written, begin, property_size - sizeof(uint32_t));
      filtered[written + property_size - 4u] = (char)(expiry_interval >> 24u);
      filtered[written + property_size - 3u] = (char)(expiry_interval >> 16u);
      filtered[written + property_size - 2u] = (char)(expiry_interval >> 8u);
      filtered[written + property_size - 1u] = (char)expiry_interval;
      written += property_size;
    } else {
      memcpy(filtered + written, begin, (size_t)(iterator.cursor - begin));
      written += (size_t)(iterator.cursor - begin);
    }
  }
  if (!tstr_set_len_checked(filtered, written)) {
    tstr_free(filtered);
    return TURBO_ERANGE;
  }
  *out = filtered;
  return TURBO_OK;
}

static void flowie_fanout_deliveries_release(flowie_endpoint_t *endpoint, turbo_vec_t *deliveries,
                                             int cancel) {
  if (!deliveries) return;
  for (size_t i = 0u; i < turbo_vec_size(deliveries); ++i) {
    flowie_fanout_delivery_t *delivery = (flowie_fanout_delivery_t *)turbo_vec_at(deliveries, i);
    if (!delivery) continue;
    if (cancel && delivery->request && !endpoint->persistence_enabled &&
        delivery->packet_id != 0u && delivery->session)
      (void)flowie_session_owner_delivery_cancel(delivery->session->owner, delivery->packet_id);
    if (delivery->request) flowie_reply_request_release(endpoint, delivery->request);
  }
  turbo_vec_destroy(deliveries);
}

static int flowie_fanout_delivery_build(flowie_endpoint_t *endpoint,
                                        const flowie_fanout_target_t *target,
                                        const flowie_mqtt_packet_view_t *packet,
                                        const flowie_mqtt_publish_view_t *publish,
                                        tstr_t forwarded_properties, int retained_replay,
                                        flowie_fanout_delivery_t *delivery) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  flowie_mqtt_publish_packet_t outbound = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  flowie_reply_request_t *request;
  size_t capacity;
  size_t written = 0u;
  uint16_t packet_id = 0u;
  uint8_t qos;
  int rc;
  if (!endpoint || !target || !packet || !publish || !delivery) return TURBO_EINVAL;
  owner = target->session->owner;
  if (endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc != TURBO_OK || !snapshot.active) {
    if (rc == TURBO_OK) rc = TURBO_ENOTCONN;
    goto fail;
  }
  rc = flowie_session_owner_route(owner, &route);
  if (rc != TURBO_OK) goto fail;
  qos = publish->qos < target->qos ? publish->qos : target->qos;
  if (qos != 0u) {
    rc = flowie_session_owner_delivery_reserve(owner, qos, &packet_id);
    if (rc != TURBO_OK) goto fail;
  }
  if (packet->packet.size > SIZE_MAX - 2u) {
    rc = TURBO_ERANGE;
    goto fail;
  }
  capacity = packet->packet.size + 2u;
  request = (flowie_reply_request_t *)calloc(1u, sizeof(*request));
  if (!request) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  request->packet = tstr_new_len(NULL, capacity);
  if (!request->packet) {
    free(request);
    rc = TURBO_ENOMEM;
    goto fail;
  }
  outbound.version = snapshot.version;
  outbound.qos = qos;
  outbound.retain = (uint8_t)(retained_replay || (publish->retain && target->retain_as_published));
  outbound.packet_id = packet_id;
  outbound.topic = publish->topic;
  outbound.payload = publish->payload;
  if (snapshot.version == FLOWIE_MQTT_VERSION_5 && packet->version == FLOWIE_MQTT_VERSION_5) {
    outbound.properties.data = (const uint8_t *)forwarded_properties;
    outbound.properties.size = tstr_len(forwarded_properties);
  }
  rc = flowie_mqtt_publish_packet_encode(&outbound, (uint8_t *)request->packet, capacity, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK || !tstr_set_len_checked(request->packet, written)) {
    tstr_freep(&request->packet);
    free(request);
    rc = TURBO_EPROTO;
    goto fail;
  }
  if (packet_id != 0u) {
    rc = flowie_session_owner_delivery_commit(
        owner, packet_id, (flowie_mqtt_span_t){(const uint8_t *)request->packet, written});
    if (rc != TURBO_OK) {
      tstr_freep(&request->packet);
      free(request);
      goto fail;
    }
    if (staged) {
      rc = flowie_session_commit_staged(endpoint, target->session, staged, NULL,
                                        target->session->expiry_at_epoch_seconds,
                                        target->session->will_at_epoch_seconds);
      if (rc != TURBO_OK) {
        tstr_freep(&request->packet);
        free(request);
        goto fail;
      }
      staged = NULL;
    }
  }
  request->kind = FLOWIE_REPLY_PACKET;
  request->subscriber_delivery = 1;
  request->route = route;
  request->route.size = sizeof(request->route);
  delivery->session = target->session;
  delivery->request = request;
  delivery->packet_id = packet_id;
  flowie_session_owner_destroy(staged);
  return TURBO_OK;

fail:
  if (packet_id != 0u && owner) (void)flowie_session_owner_delivery_cancel(owner, packet_id);
  flowie_session_owner_destroy(staged);
  return rc;
}

static int flowie_session_subscription_exists(const flowie_session_owner_t *owner,
                                              flowie_mqtt_span_t filter, int *exists) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  int rc;
  if (!owner || !exists) return TURBO_EINVAL;
  *exists = 0;
  rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < snapshot.subscription_count; ++i) {
    flowie_session_subscription_t subscription = FLOWIE_SESSION_SUBSCRIPTION_INIT;
    rc = flowie_session_owner_subscription_at(owner, i, &subscription);
    if (rc != TURBO_OK) return rc;
    if (subscription.filter.size == filter.size &&
        memcmp(subscription.filter.data, filter.data, filter.size) == 0) {
      *exists = 1;
      break;
    }
  }
  return TURBO_OK;
}

static int flowie_retained_delivery_enqueue(flowie_endpoint_t *endpoint,
                                            flowie_fanout_delivery_t *delivery) {
  size_t bytes;
  int rc;
  if (!endpoint || !delivery || !delivery->request || !delivery->request->packet)
    return TURBO_EINVAL;
  bytes = tstr_len(delivery->request->packet);
  rc = tf_io_budget_acquire(&endpoint->send_budget, bytes);
  if (rc != TURBO_OK) goto fail;
  delivery->request->reserved_bytes = bytes;
  rc = flowie_reply_enqueue(endpoint, delivery->request);
  if (rc == TURBO_OK) {
    delivery->request = NULL;
    return TURBO_OK;
  }
  delivery->request = NULL;
fail:
  if (!endpoint->persistence_enabled && delivery->packet_id != 0u && delivery->session)
    (void)flowie_session_owner_delivery_cancel(delivery->session->owner, delivery->packet_id);
  if (delivery->request) flowie_reply_request_release(endpoint, delivery->request);
  delivery->request = NULL;
  return rc;
}

static int flowie_retained_replay_subscription(flowie_endpoint_connection_t *connection,
                                               const flowie_mqtt_subscription_view_t *subscription,
                                               int existed) {
  flowie_endpoint_t *endpoint;
  uint64_t now;
  size_t index = 0u;
  int rc = TURBO_OK;
  if (!connection || !connection->session || !subscription) return TURBO_EINVAL;
  endpoint = connection->endpoint;
  if (subscription->filter.size >= sizeof("$share/") - 1u &&
      memcmp(subscription->filter.data, "$share/", sizeof("$share/") - 1u) == 0)
    return TURBO_OK;
  if (subscription->retain_handling == 2u || (subscription->retain_handling == 1u && existed))
    return TURBO_OK;
  now = flowie_security_now_epoch_seconds();
  if (now == 0u) return TURBO_EIO;
  while (index < turbo_vec_size(&endpoint->retained_messages)) {
    flowie_retained_message_t *retained =
        (flowie_retained_message_t *)turbo_vec_at(&endpoint->retained_messages, index);
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_fanout_target_t target;
    flowie_fanout_delivery_t delivery;
    flowie_mqtt_span_t retained_topic;
    tstr_t properties = NULL;
    size_t consumed = 0u;
    int matched = 0;
    if (!retained || !retained->topic || !retained->packet) return TURBO_EPROTO;
    retained_topic =
        (flowie_mqtt_span_t){(const uint8_t *)retained->topic, tstr_len(retained->topic)};
    if (retained->expiry_at_epoch_seconds != 0u && retained->expiry_at_epoch_seconds <= now) {
      rc = flowie_retained_message_remove(endpoint, retained_topic);
      if (rc != TURBO_OK) return rc;
      continue;
    }
    rc = flowie_mqtt_topic_matches(subscription->filter, retained_topic, &matched);
    if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    if (!matched || (subscription->no_local &&
                     retained->publisher_session_id == connection->route.session_id)) {
      ++index;
      continue;
    }
    options.version = retained->version;
    options.max_packet_size = endpoint->max_packet_size;
    rc = flowie_mqtt_packet_parse((const uint8_t *)retained->packet, tstr_len(retained->packet),
                                  &options, &packet, &consumed, NULL);
    if (rc != FLOWIE_MQTT_PARSE_OK || consumed != tstr_len(retained->packet) ||
        flowie_mqtt_publish_parse(&packet, &publish) != FLOWIE_MQTT_PARSE_OK)
      return TURBO_EPROTO;
    {
      uint64_t remaining =
          retained->expiry_at_epoch_seconds == 0u ? 0u : retained->expiry_at_epoch_seconds - now;
      rc = flowie_publish_forward_properties(
          &publish, retained->expiry_at_epoch_seconds != 0u,
          remaining > UINT32_MAX ? UINT32_MAX : (uint32_t)remaining, &properties);
    }
    if (rc != TURBO_OK) return rc;
    memset(&target, 0, sizeof(target));
    target.session = connection->session;
    target.qos = subscription->qos;
    memset(&delivery, 0, sizeof(delivery));
    rc = flowie_fanout_delivery_build(endpoint, &target, &packet, &publish, properties, 1,
                                      &delivery);
    tstr_freep(&properties);
    if (rc == TURBO_ENOSPC) {
      flowie_slow_subscriber_disconnect(connection, rc);
      return TURBO_OK;
    }
    if (rc != TURBO_OK) return rc;
    rc = flowie_retained_delivery_enqueue(endpoint, &delivery);
    if (rc != TURBO_OK) return rc;
    ++index;
  }
  return TURBO_OK;
}

static int flowie_fanout_batch_admit(flowie_endpoint_t *endpoint, flowie_reply_request_t *fanout,
                                     turbo_vec_t *deliveries) {
  size_t count = turbo_vec_size(deliveries);
  int first_error = TURBO_OK;
  if (!endpoint || !fanout || !deliveries) return TURBO_EINVAL;
  (void)tf_io_budget_release(&endpoint->send_budget, fanout->reserved_bytes);
  fanout->reserved_bytes = 0u;

  /* The endpoint reservation is atomic for the batch. Peer HWM decisions are
   * deliberately local and happen only after every delivery owns its aggregate slot. */
  for (size_t i = 0u; i < count; ++i) {
    flowie_fanout_delivery_t *delivery = (flowie_fanout_delivery_t *)turbo_vec_at(deliveries, i);
    size_t bytes;
    if (!delivery || !delivery->request || !delivery->request->packet) return TURBO_EPROTO;
    bytes = tstr_len(delivery->request->packet);
    first_error = tf_io_budget_acquire(&endpoint->send_budget, bytes);
    if (first_error != TURBO_OK) return first_error;
    delivery->request->reserved_bytes = bytes;
  }

  for (size_t i = 0u; i < count; ++i) {
    flowie_fanout_delivery_t *delivery = (flowie_fanout_delivery_t *)turbo_vec_at(deliveries, i);
    flowie_endpoint_connection_t *connection =
        flowie_connection_find(endpoint, &delivery->request->route);
    int rc = connection ? flowie_connection_reply_enqueue(connection, delivery->request)
                        : TURBO_ENOTCONN;
    if (!connection) flowie_reply_request_release(endpoint, delivery->request);
    delivery->request = NULL;
    if (rc == TURBO_OK) continue;
    if (!endpoint->persistence_enabled && delivery->packet_id != 0u && delivery->session)
      (void)flowie_session_owner_delivery_cancel(delivery->session->owner, delivery->packet_id);
    if (rc != TURBO_ENOSPC && rc != TURBO_ENOTCONN && rc != TURBO_ESHUTDOWN &&
        first_error == TURBO_OK)
      first_error = rc;
  }
  return first_error;
}

static int flowie_fanout_apply(flowie_endpoint_t *endpoint, flowie_reply_request_t *request,
                               uint64_t publisher_session_id, flowie_mqtt_version_t version) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  turbo_vec_t targets;
  turbo_vec_t deliveries;
  tstr_t properties = NULL;
  size_t consumed = 0u;
  int rc;
  if (!endpoint || !request || publisher_session_id == 0u ||
      (version != FLOWIE_MQTT_VERSION_3_1_1 && version != FLOWIE_MQTT_VERSION_5))
    return TURBO_EINVAL;
  options.version = version;
  options.max_packet_size = endpoint->max_packet_size;
  rc = flowie_mqtt_packet_parse((const uint8_t *)request->packet, tstr_len(request->packet),
                                &options, &packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != tstr_len(request->packet) ||
      packet.type != FLOWIE_MQTT_PACKET_PUBLISH)
    return TURBO_EPROTO;
  rc = flowie_mqtt_publish_parse(&packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK || publish.topic.size == 0u) return TURBO_ENOTSUP;
  if (publish.retain) {
    rc = flowie_retained_message_apply(endpoint, publisher_session_id, &packet, &publish);
    if (rc != TURBO_OK) return rc;
  }
  rc = turbo_vec_init(&targets, sizeof(flowie_fanout_target_t));
  if (rc != TURBO_OK) return rc;
  rc = turbo_vec_init(&deliveries, sizeof(flowie_fanout_delivery_t));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&targets);
    return rc;
  }
  rc = flowie_fanout_select(endpoint, publisher_session_id, publish.topic, &targets);
  if (rc != TURBO_OK || turbo_vec_size(&targets) == 0u) goto done;
  rc = flowie_publish_forward_properties(&publish, 0, 0u, &properties);
  if (rc != TURBO_OK) goto done;
  for (size_t i = 0u; i < turbo_vec_size(&targets); ++i) {
    const flowie_fanout_target_t *target =
        (const flowie_fanout_target_t *)turbo_vec_at_const(&targets, i);
    flowie_fanout_delivery_t delivery;
    memset(&delivery, 0, sizeof(delivery));
    rc =
        flowie_fanout_delivery_build(endpoint, target, &packet, &publish, properties, 0, &delivery);
    if (rc == TURBO_ENOSPC || rc == TURBO_ENOTCONN) {
      turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
      flowie_endpoint_connection_t *slow = NULL;
      if (flowie_session_owner_route(target->session->owner, &route) == TURBO_OK)
        slow = flowie_connection_find(endpoint, &route);
      if (slow && rc == TURBO_ENOSPC) flowie_slow_subscriber_disconnect(slow, TURBO_ENOSPC);
      rc = TURBO_OK;
      continue;
    }
    if (rc != TURBO_OK) goto done;
    rc = turbo_vec_push(&deliveries, &delivery);
    if (rc != TURBO_OK) {
      if (!endpoint->persistence_enabled && delivery.packet_id != 0u)
        (void)flowie_session_owner_delivery_cancel(delivery.session->owner, delivery.packet_id);
      flowie_reply_request_release(endpoint, delivery.request);
      goto done;
    }
  }
  rc = flowie_fanout_batch_admit(endpoint, request, &deliveries);

done:
  tstr_freep(&properties);
  turbo_vec_destroy(&targets);
  flowie_fanout_deliveries_release(endpoint, &deliveries, rc != TURBO_OK);
  return rc;
}

static int flowie_will_publish_properties(const flowie_session_snapshot_t *snapshot, tstr_t *out) {
  flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  tstr_t filtered;
  size_t written = 0u;
  int rc;
  if (out) *out = NULL;
  if (!snapshot || !out) return TURBO_EINVAL;
  filtered = tstr_new_len(NULL, snapshot->will_properties.size);
  if (!filtered) return TURBO_ENOMEM;
  block.values = snapshot->will_properties;
  rc = flowie_mqtt_property_iterator_init(&block, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    tstr_free(filtered);
    return TURBO_EPROTO;
  }
  for (;;) {
    const uint8_t *begin = iterator.cursor;
    rc = flowie_mqtt_property_iterator_next(&iterator, &property);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK || !begin || iterator.cursor < begin) {
      tstr_free(filtered);
      return TURBO_EPROTO;
    }
    if (property.identifier == FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL) continue;
    memcpy(filtered + written, begin, (size_t)(iterator.cursor - begin));
    written += (size_t)(iterator.cursor - begin);
  }
  if (!tstr_set_len_checked(filtered, written)) {
    tstr_free(filtered);
    return TURBO_ERANGE;
  }
  *out = filtered;
  return TURBO_OK;
}

static int flowie_session_will_publish(flowie_endpoint_t *endpoint,
                                       flowie_endpoint_session_t *session) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
  turbo_flow_msg_t message;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  tstr_t properties = NULL;
  tstr_t packet = NULL;
  size_t capacity;
  size_t written = 0u;
  int rc;
  if (!endpoint || !session || !endpoint->flow || !endpoint->source_name) return TURBO_EINVAL;
  turbo_flow_msg_init(&message);
  rc = flowie_session_owner_snapshot(session->owner, &snapshot);
  if (rc != TURBO_OK) goto done;
  if (snapshot.active || !snapshot.has_will || !snapshot.will_pending) {
    rc = TURBO_ENOENT;
    goto done;
  }
  rc = flowie_will_publish_properties(&snapshot, &properties);
  if (rc != TURBO_OK) goto done;
  capacity = 16u;
  if (snapshot.will_topic.size > SIZE_MAX - capacity) {
    rc = TURBO_ERANGE;
    goto done;
  }
  capacity += snapshot.will_topic.size;
  if (snapshot.will_payload.size > SIZE_MAX - capacity) {
    rc = TURBO_ERANGE;
    goto done;
  }
  capacity += snapshot.will_payload.size;
  if (tstr_len(properties) > SIZE_MAX - capacity) {
    rc = TURBO_ERANGE;
    goto done;
  }
  capacity += tstr_len(properties);
  if (capacity > endpoint->max_packet_size) {
    rc = TURBO_EMSGSIZE;
    goto done;
  }
  packet = tstr_new_len(NULL, capacity);
  if (!packet) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  publish.version = snapshot.version;
  publish.qos = snapshot.will_qos;
  publish.retain = snapshot.will_retain;
  publish.packet_id = snapshot.will_qos == 0u ? 0u : 1u;
  publish.topic = snapshot.will_topic;
  publish.payload = snapshot.will_payload;
  publish.properties = (flowie_mqtt_span_t){(const uint8_t *)properties, tstr_len(properties)};
  rc = flowie_mqtt_publish_packet_encode(&publish, (uint8_t *)packet, capacity, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK || !tstr_set_len_checked(packet, written)) {
    rc = TURBO_EPROTO;
    goto done;
  }
  route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  route.owner_instance_id = snapshot.owner_instance_id;
  route.session_id = snapshot.session_id;
  route.session_generation = snapshot.session_generation;
  message.type = FLOWIE_MQTT_PACKET_PUBLISH;
  rc = flowie_mqtt_message_flags_encode(snapshot.version, (uint8_t)(packet[0] & 0x0fu),
                                        &message.flags);
  if (rc != TURBO_OK) goto done;
  message.owned_payload = tstr_clone(packet);
  if (!message.owned_payload) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  message.payload = tstr_to_v(message.owned_payload);
  message.flags |= FLOWIE_MQTT_MESSAGE_BROKER_WILL;
  rc = turbo_flow_msg_set_protocol_route(&message, &route);
  if (rc == TURBO_OK)
    rc = turbo_flow_publish_ex(endpoint->flow, endpoint->source_name, &message, &result);
  if (rc == TURBO_OK) rc = result.status;
  if (rc == TURBO_OK) {
    owner = session->owner;
    if (endpoint->persistence_enabled) {
      staged = flowie_session_owner_clone(owner);
      if (!staged) rc = TURBO_ENOMEM;
      else owner = staged;
    }
    if (rc == TURBO_OK) rc = flowie_session_owner_will_complete(owner);
    if (rc == TURBO_OK && staged) {
      rc = flowie_session_commit_staged(endpoint, session, staged, NULL,
                                        session->expiry_at_epoch_seconds, 0u);
      if (rc == TURBO_OK) staged = NULL;
    }
    if (rc == TURBO_OK) {
      session->will_at_epoch_seconds = 0u;
      session->will_deadline_ns = 0u;
      session->will_session_generation = 0u;
    }
  }
  flowie_session_owner_destroy(staged);

done:
  turbo_flow_msg_cleanup(&message);
  tstr_freep(&packet);
  tstr_freep(&properties);
  return rc;
}

static void flowie_reply_request_list_release(flowie_endpoint_t *endpoint,
                                              flowie_reply_request_t *head) {
  while (head) {
    flowie_reply_request_t *next = head->next;
    flowie_reply_request_release(endpoint, head);
    head = next;
  }
}

static void flowie_fail_reply_queue(flowie_endpoint_t *endpoint) {
  flowie_reply_request_t *head = NULL;
  flowie_reply_request_t *tail = NULL;
  flowie_reply_request_t *request = NULL;
  turbo_mutex_lock(&endpoint->send_queue_mutex);
  while (flowie_reply_queue_t_pop_front(&endpoint->send_queue, &request)) {
    request->next = NULL;
    if (tail) tail->next = request;
    else head = request;
    tail = request;
  }
  turbo_mutex_unlock(&endpoint->send_queue_mutex);
  flowie_reply_request_list_release(endpoint, head);
}

static int flowie_reply_settlement_apply(flowie_endpoint_connection_t *connection,
                                         const flowie_reply_request_t *request) {
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  uint8_t encoded[32];
  size_t written = 0u;
  int rc;
  if (!connection || !connection->session || !request ||
      request->kind != FLOWIE_REPLY_PROTOCOL_SETTLEMENT)
    return TURBO_EINVAL;
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_publish_settle(owner, &request->route, &request->settlement, &ack);
  if (rc != TURBO_OK) {
    flowie_session_owner_destroy(staged);
    return rc;
  }
  rc = flowie_session_ack_control_packet(
      &ack, (flowie_mqtt_version_t)request->settlement.message.protocol_version, &control);
  if (rc != TURBO_OK) {
    flowie_session_owner_destroy(staged);
    return rc;
  }
  rc = flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK || written != tstr_len(request->packet) ||
      memcmp(encoded, request->packet, written) != 0) {
    flowie_session_owner_destroy(staged);
    return TURBO_EPROTO;
  }
  if (staged) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc != TURBO_OK) {
      flowie_session_owner_destroy(staged);
      return rc;
    }
  }
  return TURBO_OK;
}

static int flowie_connection_reply_drain(flowie_endpoint_connection_t *connection) {
  int result = TURBO_OK;
  if (!connection || !connection->endpoint) return TURBO_EINVAL;
  for (;;) {
    flowie_reply_request_t *request = NULL;
    int rc;
    if (connection->closing || !flowie_reply_queue_t_pop_front(&connection->send_queue, &request)) {
      if (connection->closing) flowie_connection_fail_reply_queue(connection);
      connection->send_drain_active = 0;
      flowie_connection_usage(connection->endpoint);
      return result;
    }
    rc = coro_socket_send(connection->socket, request->packet, tstr_len(request->packet));
    if (rc == TURBO_OK && ((uint8_t)request->packet[0] >> 4u) == FLOWIE_MQTT_PACKET_CONNACK)
      connection->connack_sent = 1;
    if (rc == TURBO_OK && (request->close_after_send ||
                           ((uint8_t)request->packet[0] >> 4u) == FLOWIE_MQTT_PACKET_DISCONNECT)) {
      flowie_connection_reply_request_release(connection, request);
      flowie_connection_close(connection, TURBO_ENOTCONN);
      continue;
    }
    flowie_connection_reply_request_release(connection, request);
    if (rc != TURBO_OK) {
      result = rc;
      flowie_connection_close(connection, rc);
      continue;
    }
    if (connection->close_when_replies_drain && connection->terminal_reply_count == 0u) {
      flowie_connection_close(connection, connection->close_when_replies_drain_status);
      continue;
    }
    flowie_connection_usage(connection->endpoint);
  }
}

static int flowie_connection_schedule_reply_drain(flowie_endpoint_connection_t *connection) {
  int rc;
  if (!connection || !connection->endpoint || !connection->send_queue_initialized)
    return TURBO_EINVAL;
  if (connection->send_drain_active) return TURBO_OK;
  connection->send_drain_active = 1;
  rc = coro_socket_interrupt_wait(connection->socket, TURBO_EINTR);
  if (rc == TURBO_ENOMEM && coro_context_current() == connection->endpoint->ctx && coro_running()) {
    /* CoroNet intentionally defers socket waiter resumption through its bounded
     * post queue. Yield one owner-lane tick to drain that queue, preserving the
     * ordering contract while allowing fan-out larger than one queue window. */
    (void)coro_yield();
    rc = coro_socket_interrupt_wait(connection->socket, TURBO_EINTR);
  }
  if (rc == TURBO_OK) return TURBO_OK;
  connection->send_drain_active = 0;
  return rc;
}

/* Owner-lane handoff. This function always consumes request, including failures. */
static int flowie_connection_reply_enqueue(flowie_endpoint_connection_t *connection,
                                           flowie_reply_request_t *request) {
  size_t queue_size;
  size_t bytes;
  int rc;
  if (!connection || !request || !request->packet) return TURBO_EINVAL;
  if (connection->closing || connection->close_when_replies_drain ||
      !atomic_load_explicit(&connection->endpoint->started, memory_order_acquire)) {
    flowie_reply_request_release(connection->endpoint, request);
    return connection->closing || connection->close_when_replies_drain ? TURBO_ENOTCONN
                                                                       : TURBO_ESHUTDOWN;
  }
  bytes = tstr_len(request->packet);
  rc = tf_io_budget_acquire(&connection->send_budget, bytes);
  if (rc != TURBO_OK) {
    if (rc == TURBO_ENOSPC && request->subscriber_delivery)
      flowie_slow_subscriber_disconnect(connection, rc);
    else if (rc == TURBO_ENOSPC) flowie_connection_close(connection, rc);
    flowie_reply_request_release(connection->endpoint, request);
    return rc;
  }
  request->connection_reserved_bytes = bytes;
  queue_size = flowie_reply_queue_t_size(&connection->send_queue);
  rc = queue_size == SIZE_MAX
           ? TURBO_ERANGE
           : flowie_reply_queue_t_reserve(&connection->send_queue, queue_size + 1u);
  if (rc == TURBO_OK) rc = flowie_reply_queue_t_push_back(&connection->send_queue, request);
  if (rc != TURBO_OK) {
    flowie_connection_reply_request_release(connection, request);
    flowie_connection_close(connection, rc);
    return rc;
  }
  if (request->preserve_on_terminal_error) connection->terminal_reply_count += 1u;
  if (((uint8_t)request->packet[0] >> 4u) == FLOWIE_MQTT_PACKET_CONNACK)
    connection->connack_admitted = 1;
  rc = flowie_connection_schedule_reply_drain(connection);
  if (rc == TURBO_OK) return TURBO_OK;
  flowie_connection_close(connection, rc);
  return rc;
}

static void flowie_reply_drain_task(coro_t *co, void *arg) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)arg;
  (void)co;
  for (;;) {
    flowie_reply_request_t *request = NULL;
    flowie_endpoint_connection_t *connection;
    int rc;
    turbo_mutex_lock(&endpoint->send_queue_mutex);
    if (!flowie_reply_queue_t_pop_front(&endpoint->send_queue, &request)) {
      endpoint->send_drain_active = 0;
      turbo_mutex_unlock(&endpoint->send_queue_mutex);
      flowie_connection_usage(endpoint);
      flowie_task_end(endpoint);
      return;
    }
    turbo_mutex_unlock(&endpoint->send_queue_mutex);
    connection = flowie_connection_find(endpoint, &request->route);
    if (request->kind == FLOWIE_REPLY_PUBLISH_FANOUT) {
      flowie_mqtt_version_t version = request->broker_will ? request->protocol_version
                                      : connection         ? connection->version
                                                           : FLOWIE_MQTT_VERSION_UNSPECIFIED;
      if (request->broker_will || connection) {
        rc = flowie_fanout_apply(endpoint, request, request->route.session_id, version);
      } else {
        rc = TURBO_ENOTCONN;
      }
      if (rc != TURBO_OK && connection && !request->broker_will)
        (void)coro_socket_interrupt_wait(connection->socket, rc);
      flowie_reply_request_release(endpoint, request);
      flowie_connection_usage(endpoint);
      continue;
    }
    rc = connection ? TURBO_OK : TURBO_ENOTCONN;
    if (rc == TURBO_OK && request->kind == FLOWIE_REPLY_PROTOCOL_SETTLEMENT)
      rc = flowie_reply_settlement_apply(connection, request);
    if (rc == TURBO_OK) rc = flowie_reply_packet_validate(connection, request->packet);
    if (rc == TURBO_OK) {
      rc = flowie_connection_reply_enqueue(connection, request);
      request = NULL;
    }
    if (rc != TURBO_OK && connection && request) flowie_connection_close(connection, rc);
    if (request) flowie_reply_request_release(endpoint, request);
    flowie_connection_usage(endpoint);
  }
}

static void flowie_reply_drain_post(void *arg1, void *arg2) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)arg1;
  int rc;
  (void)arg2;
  rc = coro_context_spawn(endpoint->ctx, flowie_reply_drain_task, endpoint);
  if (rc == TURBO_OK) return;
  flowie_fail_reply_queue(endpoint);
  turbo_mutex_lock(&endpoint->send_queue_mutex);
  endpoint->send_drain_active = 0;
  turbo_mutex_unlock(&endpoint->send_queue_mutex);
  flowie_task_end(endpoint);
}

static int flowie_reply_enqueue(flowie_endpoint_t *endpoint, flowie_reply_request_t *request) {
  int schedule = 0;
  int rc;
  turbo_mutex_lock(&endpoint->send_queue_mutex);
  if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) {
    turbo_mutex_unlock(&endpoint->send_queue_mutex);
    flowie_reply_request_release(endpoint, request);
    return TURBO_ESHUTDOWN;
  }
  rc = flowie_reply_queue_t_push_back(&endpoint->send_queue, request);
  if (rc == TURBO_OK && !endpoint->send_drain_active) {
    endpoint->send_drain_active = 1;
    flowie_task_begin(endpoint);
    schedule = 1;
  }
  turbo_mutex_unlock(&endpoint->send_queue_mutex);
  if (rc != TURBO_OK) {
    flowie_reply_request_release(endpoint, request);
    return rc;
  }
  if (!schedule) return TURBO_OK;
  rc = tf_coronet_execution_post(&endpoint->execution, flowie_reply_drain_post, endpoint, NULL);
  if (rc == TURBO_OK) return TURBO_OK;
  flowie_fail_reply_queue(endpoint);
  turbo_mutex_lock(&endpoint->send_queue_mutex);
  endpoint->send_drain_active = 0;
  turbo_mutex_unlock(&endpoint->send_queue_mutex);
  flowie_task_end(endpoint);
  return rc;
}

static int flowie_reply_control_request_create(flowie_endpoint_t *endpoint,
                                               const turbo_flow_protocol_route_t *route,
                                               const flowie_mqtt_control_packet_t *control,
                                               int close_after_send, int preserve_on_terminal_error,
                                               flowie_reply_request_t **out) {
  uint8_t encoded[32];
  flowie_reply_request_t *request;
  size_t written = 0u;
  int rc;
  if (!endpoint || !route || !control || !out) return TURBO_EINVAL;
  *out = NULL;
  rc = flowie_mqtt_control_packet_encode(control, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  rc = tf_io_budget_acquire(&endpoint->send_budget, written);
  if (rc != TURBO_OK) return rc;
  request = (flowie_reply_request_t *)calloc(1u, sizeof(*request));
  if (!request) {
    (void)tf_io_budget_release(&endpoint->send_budget, written);
    return TURBO_ENOMEM;
  }
  request->route = *route;
  request->route.size = sizeof(request->route);
  request->kind = FLOWIE_REPLY_PACKET;
  request->packet = tstr_new_len(encoded, written);
  request->reserved_bytes = written;
  request->close_after_send = close_after_send;
  request->preserve_on_terminal_error = preserve_on_terminal_error;
  if (!request->packet) {
    free(request);
    (void)tf_io_budget_release(&endpoint->send_budget, written);
    return TURBO_ENOMEM;
  }
  *out = request;
  return TURBO_OK;
}

static int flowie_reply_control_enqueue(flowie_endpoint_t *endpoint,
                                        const turbo_flow_protocol_route_t *route,
                                        const flowie_mqtt_control_packet_t *control,
                                        int close_after_send) {
  flowie_reply_request_t *request = NULL;
  int rc =
      flowie_reply_control_request_create(endpoint, route, control, close_after_send, 0, &request);
  if (rc != TURBO_OK) return rc;
  return flowie_reply_enqueue(endpoint, request);
}

static int flowie_reply_wire_enqueue(flowie_endpoint_t *endpoint,
                                     const turbo_flow_protocol_route_t *route,
                                     flowie_mqtt_span_t packet) {
  flowie_reply_request_t *request;
  int rc;
  if (!endpoint || !route || !packet.data || packet.size == 0u) return TURBO_EINVAL;
  rc = tf_io_budget_acquire(&endpoint->send_budget, packet.size);
  if (rc != TURBO_OK) return rc;
  request = (flowie_reply_request_t *)calloc(1u, sizeof(*request));
  if (!request) {
    (void)tf_io_budget_release(&endpoint->send_budget, packet.size);
    return TURBO_ENOMEM;
  }
  request->route = *route;
  request->route.size = sizeof(request->route);
  request->kind = FLOWIE_REPLY_PACKET;
  request->subscriber_delivery = 1;
  request->packet = tstr_new_len(packet.data, packet.size);
  request->reserved_bytes = packet.size;
  if (!request->packet) {
    free(request);
    (void)tf_io_budget_release(&endpoint->send_budget, packet.size);
    return TURBO_ENOMEM;
  }
  return flowie_reply_enqueue(endpoint, request);
}

static int
flowie_reply_settlement_enqueue(flowie_endpoint_t *endpoint,
                                const turbo_flow_protocol_route_t *route,
                                const turbo_flow_protocol_settlement_request_t *settlement) {
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  flowie_reply_request_t *request;
  uint8_t encoded[32];
  size_t written = 0u;
  int rc;
  if (!endpoint || !route || !settlement || settlement->size < sizeof(*settlement) ||
      (settlement->point != TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED &&
       settlement->point != TURBO_FLOW_PROTOCOL_SETTLE_DURABLE) ||
      settlement->status != TURBO_OK || settlement->message.protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      (settlement->message.qos != 1u && settlement->message.qos != 2u) ||
      settlement->message.packet_id == 0u || settlement->message.packet_id > UINT16_MAX)
    return TURBO_EINVAL;
  ack.kind = settlement->message.qos == 1u ? FLOWIE_SESSION_ACK_PUBACK : FLOWIE_SESSION_ACK_PUBREC;
  ack.packet_id = (uint16_t)settlement->message.packet_id;
  rc = flowie_session_ack_control_packet(
      &ack, (flowie_mqtt_version_t)settlement->message.protocol_version, &control);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  rc = tf_io_budget_acquire(&endpoint->send_budget, written);
  if (rc != TURBO_OK) return rc;
  request = (flowie_reply_request_t *)calloc(1u, sizeof(*request));
  if (!request) {
    (void)tf_io_budget_release(&endpoint->send_budget, written);
    return TURBO_ENOMEM;
  }
  request->kind = FLOWIE_REPLY_PROTOCOL_SETTLEMENT;
  request->route = *route;
  request->route.size = sizeof(request->route);
  request->settlement = *settlement;
  request->settlement.size = sizeof(request->settlement);
  request->packet = tstr_new_len(encoded, written);
  request->reserved_bytes = written;
  if (!request->packet) {
    free(request);
    (void)tf_io_budget_release(&endpoint->send_budget, written);
    return TURBO_ENOMEM;
  }
  return flowie_reply_enqueue(endpoint, request);
}

static int
flowie_endpoint_protocol_route_settle(void *ctx, const turbo_flow_protocol_route_t *route,
                                      const turbo_flow_protocol_settlement_request_t *request) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  if (!endpoint || !route || route->protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      route->owner_instance_id != endpoint->instance_id)
    return TURBO_EINVAL;
  return flowie_reply_settlement_enqueue(endpoint, route, request);
}

static int flowie_connection_bind_session(flowie_endpoint_connection_t *connection,
                                          flowie_ingress_t *ingress,
                                          flowie_endpoint_session_t *session,
                                          const turbo_flow_protocol_route_t *route) {
  flowie_endpoint_t *endpoint;
  turbo_flow_protocol_route_t previous;
  int rc;
  if (!connection || !ingress || !session || !route) return TURBO_EINVAL;
  endpoint = connection->endpoint;
  previous = connection->route;
  (void)flowie_route_map_t_remove(&endpoint->routes, previous.session_id, NULL);
  rc = flowie_route_map_t_put(&endpoint->routes, route->session_id, connection);
  if (rc != TURBO_OK) {
    (void)flowie_route_map_t_put(&endpoint->routes, previous.session_id, connection);
    return rc;
  }
  rc = flowie_ingress_set_route(ingress, route);
  if (rc != TURBO_OK) {
    (void)flowie_route_map_t_remove(&endpoint->routes, route->session_id, NULL);
    (void)flowie_route_map_t_put(&endpoint->routes, previous.session_id, connection);
    return rc;
  }
  connection->route = *route;
  connection->route.size = sizeof(connection->route);
  connection->session = session;
  session->expiry_deadline_ns = 0u;
  session->expiry_at_epoch_seconds = 0u;
  session->expiry_session_generation = 0u;
  session->will_deadline_ns = 0u;
  session->will_at_epoch_seconds = 0u;
  session->will_session_generation = 0u;
  if (endpoint->expiry_wait) (void)coro_wait_interrupt(endpoint->expiry_wait, TURBO_EINTR);
  return TURBO_OK;
}

static int flowie_endpoint_ack_enqueue(flowie_endpoint_connection_t *connection,
                                       flowie_mqtt_version_t version,
                                       const flowie_session_ack_intent_t *ack) {
  flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  flowie_reply_request_t *request = NULL;
  int rc;
  if (!connection || !ack) return TURBO_EINVAL;
  rc = flowie_session_ack_control_packet(ack, version, &control);
  if (rc != TURBO_OK) return rc;
  rc = flowie_reply_control_request_create(connection->endpoint, &connection->route, &control, 0, 1,
                                           &request);
  if (rc != TURBO_OK) return rc;
  return flowie_connection_reply_enqueue(connection, request);
}

static int flowie_endpoint_delivery_replay_enqueue(flowie_endpoint_connection_t *connection) {
  size_t index = 0u;
  if (!connection || !connection->session) return TURBO_EINVAL;
  for (;;) {
    flowie_mqtt_span_t packet = {0};
    int rc = flowie_session_owner_delivery_pending_at(connection->session->owner, index, &packet);
    if (rc == TURBO_ENOENT) return TURBO_OK;
    if (rc != TURBO_OK) return rc;
    rc = flowie_reply_wire_enqueue(connection->endpoint, &connection->route, packet);
    if (rc != TURBO_OK) return rc;
    ++index;
  }
}

static int flowie_endpoint_prepare_delivery_ack(flowie_endpoint_connection_t *connection,
                                                const flowie_mqtt_packet_view_t *packet) {
  flowie_session_ack_intent_t reply = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  int rc;
  if (!connection || !connection->session || !packet) return TURBO_EINVAL;
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_delivery_ack(owner, packet, &reply);
  if (rc == TURBO_ENOENT) {
    flowie_session_owner_destroy(staged);
    return TURBO_EPROTO;
  }
  if (rc != TURBO_OK) {
    flowie_session_owner_destroy(staged);
    return rc;
  }
  if (staged) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc != TURBO_OK) {
      flowie_session_owner_destroy(staged);
      return rc;
    }
  }
  if (reply.kind == FLOWIE_SESSION_ACK_NONE) return TURBO_OK;
  return flowie_endpoint_ack_enqueue(connection, packet->version, &reply);
}

static int
flowie_subscription_index_apply_subscribe(flowie_endpoint_connection_t *connection,
                                          const flowie_mqtt_packet_view_t *packet,
                                          const flowie_mqtt_subscribe_view_t *subscribe) {
  flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
  flowie_mqtt_subscription_view_t view;
  size_t count = 0u;
  int rc;
  if (!connection || !connection->session || !packet || !subscribe) return TURBO_EINVAL;
  rc = flowie_mqtt_subscription_iterator_init(packet, subscribe, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_subscription_iterator_next(&iterator, &view)) == FLOWIE_MQTT_PARSE_OK) {
    flowie_session_subscription_t subscription = FLOWIE_SESSION_SUBSCRIPTION_INIT;
    subscription.filter = view.filter;
    subscription.qos = view.qos;
    subscription.no_local = view.no_local;
    subscription.retain_as_published = view.retain_as_published;
    subscription.retain_handling = view.retain_handling;
    rc =
        flowie_subscription_member_upsert(connection->endpoint, connection->session, &subscription);
    if (rc != TURBO_OK) return rc;
    ++count;
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE && count == subscribe->entry_count ? TURBO_OK
                                                                              : TURBO_EPROTO;
}

static int
flowie_subscription_index_apply_unsubscribe(flowie_endpoint_connection_t *connection,
                                            const flowie_mqtt_unsubscribe_view_t *unsubscribe) {
  flowie_mqtt_topic_filter_iterator_t iterator = FLOWIE_MQTT_TOPIC_FILTER_ITERATOR_INIT;
  flowie_mqtt_span_t filter;
  size_t count = 0u;
  int rc;
  if (!connection || !connection->session || !unsubscribe) return TURBO_EINVAL;
  rc = flowie_mqtt_topic_filter_iterator_init(unsubscribe, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_topic_filter_iterator_next(&iterator, &filter)) ==
         FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_subscription_member_remove(connection->endpoint, connection->session, filter);
    if (rc != TURBO_OK) return rc;
    ++count;
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE && count == unsubscribe->filter_count ? TURBO_OK
                                                                                 : TURBO_EPROTO;
}

static int flowie_endpoint_prepare_subscribe(flowie_endpoint_connection_t *connection,
                                             const flowie_mqtt_packet_view_t *packet) {
  flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
  flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
  flowie_mqtt_subscription_view_t entry;
  flowie_session_subscribe_result_t result = FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  tstr_t reasons = NULL;
  tstr_t existed = NULL;
  flowie_session_owner_t *staged = NULL;
  size_t index = 0u;
  int subscribed = 0;
  int rc;
  rc = flowie_mqtt_subscribe_parse(packet, &subscribe);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  reasons = tstr_new_len(NULL, subscribe.entry_count);
  existed = tstr_new_len(NULL, subscribe.entry_count);
  if (!reasons || !existed) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  if (connection->endpoint->security_enabled) {
    rc = flowie_mqtt_subscription_iterator_init(packet, &subscribe, &iterator);
    if (rc != FLOWIE_MQTT_PARSE_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    while ((rc = flowie_mqtt_subscription_iterator_next(&iterator, &entry)) ==
           FLOWIE_MQTT_PARSE_OK) {
      rc = flowie_security_authorize_span(connection, TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE,
                                          entry.filter, FLOWIE_MQTT_SECURITY_TOPIC_FILTER);
      if (rc == TURBO_EPERM) {
        memset(reasons, packet->version == FLOWIE_MQTT_VERSION_5 ? 0x87 : 0x80,
               subscribe.entry_count);
        goto encode_reply;
      }
      if (rc != TURBO_OK) goto done;
    }
    if (rc != FLOWIE_MQTT_PARSE_NEED_MORE) {
      rc = TURBO_EPROTO;
      goto done;
    }
  }
  rc = flowie_mqtt_subscription_iterator_init(packet, &subscribe, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = TURBO_EPROTO;
    goto done;
  }
  index = 0u;
  while ((rc = flowie_mqtt_subscription_iterator_next(&iterator, &entry)) == FLOWIE_MQTT_PARSE_OK) {
    int was_present = 0;
    if (index >= subscribe.entry_count) {
      rc = TURBO_EPROTO;
      goto done;
    }
    rc = flowie_session_subscription_exists(connection->session->owner, entry.filter, &was_present);
    if (rc != TURBO_OK) goto done;
    existed[index++] = (char)(was_present != 0);
  }
  if (rc != FLOWIE_MQTT_PARSE_NEED_MORE || index != subscribe.entry_count) {
    rc = TURBO_EPROTO;
    goto done;
  }
  staged = flowie_session_owner_clone(connection->session->owner);
  if (!staged) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flowie_session_owner_subscribe(staged, packet, &subscribe, &result);
  if (rc == TURBO_OK) {
    if (result.changed) {
      rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                        connection->session->expiry_at_epoch_seconds,
                                        connection->session->will_at_epoch_seconds);
      if (rc != TURBO_OK) goto done;
      staged = NULL;
    }
    flowie_session_owner_destroy(staged);
    staged = NULL;
    if (result.changed &&
        flowie_subscription_index_apply_subscribe(connection, packet, &subscribe) != TURBO_OK)
      connection->endpoint->subscription_index_valid = 0;
    rc = flowie_mqtt_subscription_iterator_init(packet, &subscribe, &iterator);
    if (rc != FLOWIE_MQTT_PARSE_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    index = 0u;
    while ((rc = flowie_mqtt_subscription_iterator_next(&iterator, &entry)) ==
           FLOWIE_MQTT_PARSE_OK) {
      if (index >= subscribe.entry_count) {
        rc = TURBO_EPROTO;
        goto done;
      }
      reasons[index++] = entry.qos;
    }
    if (rc != FLOWIE_MQTT_PARSE_NEED_MORE || index != subscribe.entry_count) {
      rc = TURBO_EPROTO;
      goto done;
    }
    subscribed = 1;
  } else if (rc == TURBO_ENOSPC) {
    flowie_session_owner_destroy(staged);
    staged = NULL;
    memset(reasons, packet->version == FLOWIE_MQTT_VERSION_5 ? 0x97 : 0x80, subscribe.entry_count);
  } else {
    goto done;
  }
encode_reply:
  reply.version = packet->version;
  reply.type = FLOWIE_MQTT_PACKET_SUBACK;
  reply.packet_id = subscribe.packet_id;
  reply.reason_codes = (flowie_mqtt_span_t){(const uint8_t *)reasons, subscribe.entry_count};
  rc = flowie_reply_control_enqueue(connection->endpoint, &connection->route, &reply, 0);
  if (rc != TURBO_OK || !subscribed) goto done;
  rc = flowie_mqtt_subscription_iterator_init(packet, &subscribe, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = TURBO_EPROTO;
    goto done;
  }
  index = 0u;
  while ((rc = flowie_mqtt_subscription_iterator_next(&iterator, &entry)) == FLOWIE_MQTT_PARSE_OK) {
    if (index >= subscribe.entry_count) {
      rc = TURBO_EPROTO;
      goto done;
    }
    rc = flowie_retained_replay_subscription(connection, &entry, existed[index] != 0);
    if (rc != TURBO_OK) goto done;
    ++index;
  }
  if (rc == FLOWIE_MQTT_PARSE_NEED_MORE && index == subscribe.entry_count) rc = TURBO_OK;
  else rc = TURBO_EPROTO;

done:
  flowie_session_owner_destroy(staged);
  tstr_freep(&existed);
  tstr_freep(&reasons);
  return rc;
}

static int flowie_endpoint_prepare_unsubscribe(flowie_endpoint_connection_t *connection,
                                               const flowie_mqtt_packet_view_t *packet) {
  flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
  flowie_session_unsubscribe_result_t result = FLOWIE_SESSION_UNSUBSCRIBE_RESULT_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  tstr_t reasons = NULL;
  flowie_session_owner_t *staged = NULL;
  int rc;
  rc = flowie_mqtt_unsubscribe_parse(packet, &unsubscribe);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    reasons = tstr_new_len(NULL, unsubscribe.filter_count);
    if (!reasons) return TURBO_ENOMEM;
  }
  staged = flowie_session_owner_clone(connection->session->owner);
  if (!staged) {
    tstr_free(reasons);
    return TURBO_ENOMEM;
  }
  rc = flowie_session_owner_unsubscribe(staged, packet, &unsubscribe, (uint8_t *)reasons,
                                        reasons ? unsubscribe.filter_count : 0u, &result);
  if (rc != TURBO_OK) {
    flowie_session_owner_destroy(staged);
    tstr_free(reasons);
    return rc;
  }
  if (result.changed) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc != TURBO_OK) {
      flowie_session_owner_destroy(staged);
      tstr_free(reasons);
      return rc;
    }
    staged = NULL;
  }
  flowie_session_owner_destroy(staged);
  if (result.changed &&
      flowie_subscription_index_apply_unsubscribe(connection, &unsubscribe) != TURBO_OK)
    connection->endpoint->subscription_index_valid = 0;
  reply.version = packet->version;
  reply.type = FLOWIE_MQTT_PACKET_UNSUBACK;
  reply.packet_id = unsubscribe.packet_id;
  reply.reason_codes = result.reason_codes;
  rc = flowie_reply_control_enqueue(connection->endpoint, &connection->route, &reply, 0);
  tstr_free(reasons);
  return rc;
}

static int flowie_endpoint_prepare_publish(flowie_endpoint_connection_t *connection,
                                           flowie_ingress_t *ingress,
                                           const flowie_mqtt_packet_view_t *packet,
                                           int *publish_packet) {
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_session_publish_begin_result_t begin = FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  turbo_flow_protocol_settlement_request_t settlement = TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  int rc;
  if (!ingress || !publish_packet) return TURBO_EINVAL;
  rc = flowie_mqtt_publish_parse(packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  if (connection->endpoint->security_enabled) {
    rc = flowie_security_authorize_span(connection, TURBO_FLOW_SECURITY_ACTION_PUBLISH,
                                        publish.topic, FLOWIE_MQTT_SECURITY_TOPIC);
    if (rc == TURBO_EPERM && packet->version == FLOWIE_MQTT_VERSION_5 && publish.qos != 0u) {
      *publish_packet = 0;
      ack.kind = publish.qos == 1u ? FLOWIE_SESSION_ACK_PUBACK : FLOWIE_SESSION_ACK_PUBREC;
      ack.packet_id = publish.packet_id;
      ack.reason_code = UINT8_C(0x87);
      return flowie_endpoint_ack_enqueue(connection, packet->version, &ack);
    }
    if (rc != TURBO_OK) return rc;
  }
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_publish_begin(owner, &publish, &begin);
  if (rc == TURBO_OK && staged && begin.admit_graph && publish.qos != 0u) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc == TURBO_OK) staged = NULL;
  }
  flowie_session_owner_destroy(staged);
  if (rc != TURBO_OK) return rc;
  *publish_packet = begin.admit_graph != 0u;
  if (begin.has_ack) return flowie_endpoint_ack_enqueue(connection, packet->version, &begin.ack);
  if (!begin.admit_graph || publish.qos == 0u) return TURBO_OK;
  settlement.message = begin.message.metadata;
  settlement.point = publish.qos == 1u ? connection->endpoint->settlement.qos1
                                       : connection->endpoint->settlement.qos2;
  settlement.status = TURBO_OK;
  if (settlement.point == TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED ||
      settlement.point == TURBO_FLOW_PROTOCOL_SETTLE_DURABLE) {
    turbo_flow_protocol_settlement_envelope_t envelope =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT;
    if (connection->settlement_pending) return TURBO_EBUSY;
    envelope.message = settlement.message;
    envelope.requested_point = settlement.point;
    rc = flowie_ingress_set_protocol_settlement(ingress, &envelope);
    if (rc != TURBO_OK) return rc;
    connection->pending_settlement = settlement;
    connection->settlement_pending = 1;
    return TURBO_OK;
  }
  if (settlement.point == TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED) {
    if (connection->settlement_pending) return TURBO_EBUSY;
    connection->pending_settlement = settlement;
    connection->settlement_pending = 1;
    return TURBO_OK;
  }
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_publish_settle(owner, &connection->route, &settlement, &ack);
  if (rc == TURBO_OK && staged) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc == TURBO_OK) staged = NULL;
  }
  flowie_session_owner_destroy(staged);
  if (rc != TURBO_OK) return rc;
  return flowie_endpoint_ack_enqueue(connection, packet->version, &ack);
}

static int flowie_endpoint_publish_complete(void *ctx, flowie_ingress_t *ingress,
                                            const turbo_flow_msg_t *message,
                                            const turbo_flow_publish_result_t *result) {
  flowie_endpoint_connection_t *connection = (flowie_endpoint_connection_t *)ctx;
  turbo_flow_protocol_settlement_request_t settlement;
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  flowie_mqtt_version_t version;
  int rc;
  (void)ingress;
  (void)message;
  if (!connection || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  if (!connection->settlement_pending) return result->status;
  settlement = connection->pending_settlement;
  connection->pending_settlement =
      (turbo_flow_protocol_settlement_request_t)TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
  connection->settlement_pending = 0;
  if (settlement.point == TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED ||
      settlement.point == TURBO_FLOW_PROTOCOL_SETTLE_DURABLE) {
    if (result->status != TURBO_OK) return result->status;
    return result->protocol_settlement == settlement.point ? TURBO_OK : TURBO_EPROTO;
  }
  version = (flowie_mqtt_version_t)settlement.message.protocol_version;
  settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
  settlement.status = result->status;
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_publish_settle(owner, &connection->route, &settlement, &ack);
  if (rc == TURBO_OK && staged) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc == TURBO_OK) staged = NULL;
  }
  flowie_session_owner_destroy(staged);
  if (rc != TURBO_OK) return rc;
  return flowie_endpoint_ack_enqueue(connection, version, &ack);
}

static int flowie_endpoint_prepare_pubrel(flowie_endpoint_connection_t *connection,
                                          const flowie_mqtt_packet_view_t *packet) {
  flowie_mqtt_control_packet_view_t release = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  int rc = flowie_mqtt_control_packet_parse(packet, &release);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_qos2_release(owner, &connection->route, release.packet_id, &ack);
  if (rc == TURBO_ENOENT) {
    flowie_session_owner_destroy(staged);
    staged = NULL;
    ack.kind = FLOWIE_SESSION_ACK_PUBCOMP;
    ack.packet_id = release.packet_id;
    ack.reason_code = packet->version == FLOWIE_MQTT_VERSION_5 ? 0x92u : 0u;
    rc = TURBO_OK;
  }
  if (rc == TURBO_OK && staged) {
    rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                      connection->session->expiry_at_epoch_seconds,
                                      connection->session->will_at_epoch_seconds);
    if (rc == TURBO_OK) staged = NULL;
  }
  flowie_session_owner_destroy(staged);
  if (rc != TURBO_OK) return rc;
  return flowie_endpoint_ack_enqueue(connection, packet->version, &ack);
}

static int flowie_endpoint_prepare_disconnect(flowie_endpoint_connection_t *connection,
                                              const flowie_mqtt_packet_view_t *packet,
                                              int *stop_pump) {
  flowie_mqtt_control_packet_view_t disconnect = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_owner_t *owner;
  flowie_session_owner_t *staged = NULL;
  int rc;
  if (!stop_pump) return TURBO_EINVAL;
  rc = flowie_mqtt_control_packet_parse(packet, &disconnect);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  owner = connection->session->owner;
  if (connection->endpoint->persistence_enabled) {
    rc = flowie_session_owner_snapshot(owner, &before);
    if (rc != TURBO_OK) return rc;
    staged = flowie_session_owner_clone(owner);
    if (!staged) return TURBO_ENOMEM;
    owner = staged;
  }
  rc = flowie_session_owner_disconnect(owner, &disconnect);
  if (rc == TURBO_OK && staged) {
    rc = flowie_session_owner_snapshot(staged, &after);
    if (rc == TURBO_OK && after.resource_generation != before.resource_generation) {
      rc = flowie_session_commit_staged(connection->endpoint, connection->session, staged, NULL,
                                        connection->session->expiry_at_epoch_seconds,
                                        connection->session->will_at_epoch_seconds);
      if (rc == TURBO_OK) staged = NULL;
    }
  }
  flowie_session_owner_destroy(staged);
  if (rc != TURBO_OK) return rc;
  connection->closing = 1;
  *stop_pump = 1;
  return coro_socket_interrupt_wait(connection->socket, TURBO_ENOTCONN);
}

static int flowie_endpoint_session_prepare(void *ctx, flowie_ingress_t *ingress,
                                           const flowie_mqtt_packet_view_t *packet,
                                           int *publish_packet, int *stop_pump) {
  flowie_endpoint_connection_t *connection = (flowie_endpoint_connection_t *)ctx;
  flowie_endpoint_t *endpoint;
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  flowie_session_connect_result_t decision = FLOWIE_SESSION_CONNECT_RESULT_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  flowie_endpoint_session_t *session;
  flowie_session_owner_t *staged_owner = NULL;
  uint8_t security_reason = 0u;
  int existing_session = 0;
  int rc;
  if (!connection || !ingress || !packet || !publish_packet || !stop_pump) return TURBO_EINVAL;
  *publish_packet = 1;
  *stop_pump = 0;
  endpoint = connection->endpoint;
  if (packet->type == FLOWIE_MQTT_PACKET_CONNECT) {
    rc = flowie_mqtt_connect_parse(packet, &connect);
    if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    if (endpoint->security_enabled) {
      rc = flowie_security_authenticate_connect(endpoint, &connect, &principal, &security_reason);
      if (rc != TURBO_OK) {
        if (rc != TURBO_EPERM) return rc;
        decision.reply.type = FLOWIE_MQTT_PACKET_CONNACK;
        decision.reply.version = connect.version;
        decision.reply.reason_code = security_reason;
        decision.close_after_reply = 1u;
        *publish_packet = 0;
        *stop_pump = 1;
        return flowie_reply_control_enqueue(endpoint, &connection->route, &decision.reply, 1);
      }
    }
    session = flowie_session_find(endpoint, connect.client_id);
    existing_session = session != NULL;
    if (endpoint->security_enabled && session &&
        !flowie_security_principal_same_owner(&session->principal, &principal)) {
      decision.reply.type = FLOWIE_MQTT_PACKET_CONNACK;
      decision.reply.version = connect.version;
      decision.reply.reason_code =
          connect.version == FLOWIE_MQTT_VERSION_5 ? UINT8_C(0x87) : UINT8_C(0x05);
      decision.close_after_reply = 1u;
      *publish_packet = 0;
      *stop_pump = 1;
      return flowie_reply_control_enqueue(endpoint, &connection->route, &decision.reply, 1);
    }
    if (session) {
      if (endpoint->persistence_enabled) {
        staged_owner = flowie_session_owner_clone(session->owner);
        if (!staged_owner) return TURBO_ENOMEM;
        rc = flowie_session_owner_connect(staged_owner, &connect, &decision);
        if (rc == TURBO_OK && decision.accepted) {
          rc = flowie_session_commit_staged(
              endpoint, session, staged_owner,
              endpoint->security_enabled ? &principal : &session->principal, 0u, 0u);
          if (rc == TURBO_OK) staged_owner = NULL;
        }
        flowie_session_owner_destroy(staged_owner);
        staged_owner = NULL;
      } else {
        rc = flowie_session_owner_connect(session->owner, &connect, &decision);
        if (rc == TURBO_OK && decision.accepted && endpoint->security_enabled)
          session->principal = principal;
      }
    } else {
      rc = flowie_session_create(endpoint, &connect, endpoint->security_enabled ? &principal : NULL,
                                 &decision, &session);
      if (rc == TURBO_ENOSPC) {
        decision.reply.type = FLOWIE_MQTT_PACKET_CONNACK;
        decision.reply.version = connect.version;
        decision.reply.reason_code =
            connect.version == FLOWIE_MQTT_VERSION_5 ? UINT8_C(0x97) : UINT8_C(0x03);
        decision.close_after_reply = 1u;
        rc = TURBO_OK;
      }
    }
    if (rc != TURBO_OK) return rc;
    if (decision.accepted) {
      rc = flowie_connection_bind_session(connection, ingress, session, &decision.route);
      if (rc != TURBO_OK) {
        (void)flowie_session_owner_close(session->owner);
        return rc;
      }
      if (existing_session && connect.clean_start) endpoint->subscription_index_valid = 0;
      *publish_packet = 0;
      rc = flowie_reply_control_enqueue(endpoint, &connection->route, &decision.reply, 0);
      if (rc != TURBO_OK) return rc;
      return flowie_endpoint_delivery_replay_enqueue(connection);
    } else {
      *publish_packet = 0;
      *stop_pump = 1;
    }
    return flowie_reply_control_enqueue(endpoint, &connection->route, &decision.reply,
                                        decision.close_after_reply);
  }
  if (!connection->session) return TURBO_EPROTO;
  switch (packet->type) {
  case FLOWIE_MQTT_PACKET_PUBLISH:
    return flowie_endpoint_prepare_publish(connection, ingress, packet, publish_packet);
  case FLOWIE_MQTT_PACKET_SUBSCRIBE:
    *publish_packet = 0;
    return flowie_endpoint_prepare_subscribe(connection, packet);
  case FLOWIE_MQTT_PACKET_UNSUBSCRIBE:
    *publish_packet = 0;
    return flowie_endpoint_prepare_unsubscribe(connection, packet);
  case FLOWIE_MQTT_PACKET_PUBACK:
  case FLOWIE_MQTT_PACKET_PUBREC:
  case FLOWIE_MQTT_PACKET_PUBCOMP:
    *publish_packet = 0;
    return flowie_endpoint_prepare_delivery_ack(connection, packet);
  case FLOWIE_MQTT_PACKET_PUBREL:
    *publish_packet = 0;
    return flowie_endpoint_prepare_pubrel(connection, packet);
  case FLOWIE_MQTT_PACKET_PINGREQ: {
    flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    if (packet->body.size != 0u) return TURBO_EPROTO;
    *publish_packet = 0;
    reply.version = packet->version;
    reply.type = FLOWIE_MQTT_PACKET_PINGRESP;
    return flowie_reply_control_enqueue(endpoint, &connection->route, &reply, 0);
  }
  case FLOWIE_MQTT_PACKET_DISCONNECT:
    *publish_packet = 0;
    return flowie_endpoint_prepare_disconnect(connection, packet, stop_pump);
  case FLOWIE_MQTT_PACKET_AUTH: {
    flowie_mqtt_control_packet_view_t auth = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    if (flowie_mqtt_control_packet_parse(packet, &auth) != FLOWIE_MQTT_PARSE_OK)
      return TURBO_EPROTO;
    *publish_packet = 0;
    *stop_pump = 1;
    reply.version = packet->version;
    reply.type = FLOWIE_MQTT_PACKET_DISCONNECT;
    reply.reason_code = 0x8cu;
    return flowie_reply_control_enqueue(endpoint, &connection->route, &reply, 1);
  }
  default:
    return TURBO_EPROTO;
  }
}

static void flowie_endpoint_client_handler(coro_socket_t *client, void *arg) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)arg;
  flowie_endpoint_connection_t *connection = NULL;
  flowie_ingress_t *ingress = NULL;
  int rc = TURBO_ESHUTDOWN;
  if (!endpoint) return;
  flowie_task_begin(endpoint);
  if (!client || !atomic_load_explicit(&endpoint->started, memory_order_acquire) ||
      atomic_load_explicit(&endpoint->quiesced, memory_order_acquire))
    goto done;
  rc = flowie_client_add(endpoint, client, &connection);
  if (rc != TURBO_OK) goto done;
  {
    flowie_ingress_config_t config = FLOWIE_INGRESS_CONFIG_INIT;
    config.flow = endpoint->flow;
    config.publish_source = endpoint->source_name;
    config.max_packet_size = endpoint->max_packet_size;
    config.route = connection->route;
    if (endpoint->manage_sessions) {
      config.prepare = flowie_endpoint_session_prepare;
      config.publish_complete = flowie_endpoint_publish_complete;
      config.prepare_ctx = connection;
    }
    ingress = flowie_ingress_create(&config);
  }
  if (!ingress) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  (void)tf_coronet_apply_socket_timeout(client, &endpoint->timeouts, TF_CORONET_TIMEOUT_RECV);
  while (atomic_load_explicit(&endpoint->started, memory_order_acquire)) {
    char *chunk = NULL;
    size_t chunk_size = 0u;
    size_t published = 0u;
    if (connection->send_drain_active) {
      rc = flowie_connection_reply_drain(connection);
      if (rc != TURBO_OK) break;
    }
    rc = coro_socket_recv(client, &chunk, &chunk_size);
    if (rc == TURBO_EINTR) {
      rc = TURBO_OK;
      continue;
    }
    if (rc != TURBO_OK) break;
    if (!chunk || chunk_size == 0u) {
      if (chunk) coro_socket_free_recv(chunk);
      rc = TURBO_EOF;
      break;
    }
    rc = flowie_ingress_feed(ingress, chunk, chunk_size, &published);
    coro_socket_free_recv(chunk);
    connection->version = flowie_ingress_version(ingress);
    if (connection->send_drain_active) {
      int send_rc = flowie_connection_reply_drain(connection);
      if (rc == TURBO_OK && send_rc != TURBO_OK) rc = send_rc;
    }
    if (rc != TURBO_OK) break;
  }

done:
  flowie_ingress_destroy(ingress);
  if (connection) {
    flowie_endpoint_session_t *session = connection->session;
    flowie_connection_close_after_terminal_replies(connection,
                                                   rc == TURBO_OK ? TURBO_ENOTCONN : rc);
    flowie_client_remove(endpoint, connection);
    if (session) {
      int close_rc = flowie_session_close_schedule(endpoint, session);
      if (close_rc != TURBO_OK && rc == TURBO_OK) rc = close_rc;
      connection->session = NULL;
    }
  }
  if (client) (void)coro_socket_interrupt_wait(client, rc == TURBO_OK ? TURBO_ENOTCONN : rc);
  if (connection) {
    flowie_connection_fail_reply_queue(connection);
    if (connection->send_budget_initialized) {
      tf_io_budget_destroy(&connection->send_budget);
      connection->send_budget_initialized = 0;
    }
    if (connection->send_queue_initialized) {
      flowie_reply_queue_t_destroy(&connection->send_queue);
      connection->send_queue_initialized = 0;
    }
    free(connection);
  }
  flowie_task_end(endpoint);
}

static int flowie_endpoint_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  const turbo_flow_protocol_route_t *route;
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_version_t protocol_version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
  flowie_reply_request_t *request;
  size_t consumed = 0u;
  size_t bytes;
  int broker_will;
  int rc;
  (void)flow;
  (void)stage;
  if (!endpoint || !msg || !atomic_load_explicit(&endpoint->started, memory_order_acquire))
    return TURBO_ESHUTDOWN;
  route = turbo_flow_msg_protocol_route(msg);
  if (!route || route->protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      route->owner_instance_id != endpoint->instance_id || route->session_id == 0u ||
      route->session_generation == 0u || msg->payload.len == 0u || !msg->payload.data ||
      msg->payload.len > endpoint->max_packet_size)
    return TURBO_EINVAL;
  options.version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
  options.max_packet_size = endpoint->max_packet_size;
  rc = flowie_mqtt_packet_parse((const uint8_t *)msg->payload.data, msg->payload.len, &options,
                                &packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != msg->payload.len ||
      (packet.type != FLOWIE_MQTT_PACKET_PUBLISH && packet.type != FLOWIE_MQTT_PACKET_CONNACK &&
       packet.type != FLOWIE_MQTT_PACKET_PUBACK && packet.type != FLOWIE_MQTT_PACKET_PUBREC &&
       packet.type != FLOWIE_MQTT_PACKET_PUBREL && packet.type != FLOWIE_MQTT_PACKET_PUBCOMP &&
       packet.type != FLOWIE_MQTT_PACKET_SUBACK && packet.type != FLOWIE_MQTT_PACKET_UNSUBACK &&
       packet.type != FLOWIE_MQTT_PACKET_PINGRESP && packet.type != FLOWIE_MQTT_PACKET_DISCONNECT &&
       packet.type != FLOWIE_MQTT_PACKET_AUTH))
    return TURBO_EPROTO;
  if (packet.type == FLOWIE_MQTT_PACKET_PUBLISH && !endpoint->manage_sessions) return TURBO_ENOTSUP;
  broker_will = packet.type == FLOWIE_MQTT_PACKET_PUBLISH &&
                (msg->flags & FLOWIE_MQTT_MESSAGE_BROKER_WILL) != 0u;
  if (broker_will) {
    rc = flowie_mqtt_message_flags_version(msg->flags, &protocol_version);
    if (rc != TURBO_OK) return rc;
  }
  bytes = msg->payload.len;
  rc = tf_io_budget_acquire(&endpoint->send_budget, bytes);
  if (rc != TURBO_OK) return rc;
  request = (flowie_reply_request_t *)calloc(1, sizeof(*request));
  if (!request) {
    (void)tf_io_budget_release(&endpoint->send_budget, bytes);
    return TURBO_ENOMEM;
  }
  request->route = *route;
  request->route.size = sizeof(request->route);
  request->kind =
      packet.type == FLOWIE_MQTT_PACKET_PUBLISH ? FLOWIE_REPLY_PUBLISH_FANOUT : FLOWIE_REPLY_PACKET;
  request->broker_will = broker_will;
  request->protocol_version = protocol_version;
  request->packet = tstr_new_len(msg->payload.data, bytes);
  request->reserved_bytes = bytes;
  if (!request->packet) {
    free(request);
    (void)tf_io_budget_release(&endpoint->send_budget, bytes);
    return TURBO_ENOMEM;
  }
  rc = flowie_reply_enqueue(endpoint, request);
  return rc;
}

static int flowie_listener_start_call(void *arg) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)arg;
  tf_coronet_transport_t transport;
  int rc;
  if (!endpoint || endpoint->server) return endpoint ? TURBO_EALREADY : TURBO_EINVAL;
  transport = flowie_coronet_transport(endpoint->transport);
  endpoint->server = tf_coronet_create_server_socket(endpoint->ctx, transport);
  if (!endpoint->server) return TURBO_ENOMEM;
  if (endpoint->reuse_port) coro_socket_set_reuse_port(endpoint->server, 1);
  rc = tf_coronet_apply_socket_options(endpoint->server, transport, &endpoint->socket_options);
  if (rc == TURBO_OK) {
    (void)tf_coronet_apply_socket_timeout(endpoint->server, &endpoint->timeouts,
                                          TF_CORONET_TIMEOUT_RECV);
    rc = tf_coronet_listen_socket(endpoint->server, transport, endpoint->host, endpoint->port,
                                  endpoint->path, flowie_endpoint_client_handler, endpoint);
    if (rc == TURBO_OK && endpoint->manage_sessions) rc = flowie_expiry_schedule(endpoint);
  }
  if (rc != TURBO_OK) {
    coro_socket_destroy(endpoint->server);
    endpoint->server = NULL;
  }
  return rc;
}

static int flowie_listener_close_call(void *arg) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)arg;
  if (!endpoint) return TURBO_EINVAL;
  if (endpoint->server) {
    coro_socket_destroy(endpoint->server);
    endpoint->server = NULL;
  }
  for (size_t i = 0u; i < turbo_vec_size(&endpoint->clients); ++i) {
    flowie_endpoint_connection_t *const *connection =
        (flowie_endpoint_connection_t *const *)turbo_vec_at_const(&endpoint->clients, i);
    if (connection && *connection && (*connection)->socket)
      (void)coro_socket_interrupt_wait((*connection)->socket, TURBO_ESHUTDOWN);
  }
  return TURBO_OK;
}

typedef struct flowie_management_call_s {
  flowie_endpoint_t *endpoint;
  turbo_flow_resource_command_kind_t kind;
} flowie_management_call_t;

static int flowie_management_call(void *arg) {
  flowie_management_call_t *call = (flowie_management_call_t *)arg;
  flowie_endpoint_t *endpoint;
  int quiesced;
  int rc;
  if (!call || !(endpoint = call->endpoint)) return TURBO_EINVAL;
  if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  quiesced = atomic_load_explicit(&endpoint->quiesced, memory_order_acquire) != 0;
  if ((call->kind == TURBO_FLOW_RESOURCE_COMMAND_QUIESCE && quiesced) ||
      (call->kind == TURBO_FLOW_RESOURCE_COMMAND_RESUME && !quiesced))
    return TURBO_OK;
  if (atomic_load_explicit(&endpoint->generation, memory_order_acquire) == UINT64_MAX)
    return TURBO_ERANGE;
  if (call->kind == TURBO_FLOW_RESOURCE_COMMAND_QUIESCE) {
    atomic_store_explicit(&endpoint->quiesced, 1, memory_order_release);
    rc = TURBO_OK;
  } else if (call->kind == TURBO_FLOW_RESOURCE_COMMAND_RESUME) {
    atomic_store_explicit(&endpoint->quiesced, 0, memory_order_release);
    rc = TURBO_OK;
  } else {
    return TURBO_ENOTSUP;
  }
  if (rc == TURBO_OK)
    (void)atomic_fetch_add_explicit(&endpoint->generation, 1u, memory_order_acq_rel);
  return rc;
}

static int flowie_management_apply(flowie_endpoint_t *endpoint,
                                   turbo_flow_resource_command_kind_t kind, uint64_t timeout_ns) {
  flowie_management_call_t call;
  int expected = 0;
  int rc;
  if (!endpoint || timeout_ns == 0u) return TURBO_EINVAL;
  if (kind != TURBO_FLOW_RESOURCE_COMMAND_QUIESCE && kind != TURBO_FLOW_RESOURCE_COMMAND_RESUME)
    return TURBO_ENOTSUP;
  if (!atomic_compare_exchange_strong_explicit(&endpoint->management_command_active, &expected, 1,
                                               memory_order_acq_rel, memory_order_acquire))
    return TURBO_EBUSY;
  call.endpoint = endpoint;
  call.kind = kind;
  rc = tf_coronet_execution_call(&endpoint->execution, flowie_management_call, &call, timeout_ns);
  atomic_store_explicit(&endpoint->last_management_status, rc, memory_order_release);
  atomic_store_explicit(&endpoint->management_command_active, 0, memory_order_release);
  return rc;
}

static int flowie_endpoint_command(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_adapter_command_t *command) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  turbo_flow_resource_command_kind_t kind;
  (void)flow;
  if (!endpoint || !command) return TURBO_EINVAL;
  if (command->kind == TURBO_FLOW_ADAPTER_QUIESCE) {
    kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
  } else if (command->kind == TURBO_FLOW_ADAPTER_RESUME) {
    kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
  } else {
    return TURBO_ENOTSUP;
  }
  return flowie_management_apply(endpoint, kind, flowie_timeout_ns(endpoint));
}

static int flowie_resource_command(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_resource_command_t *command) {
  flowie_endpoint_resource_t *resource = (flowie_endpoint_resource_t *)ctx;
  uint64_t timeout_ns;
  uint64_t now;
  (void)flow;
  if (!resource || !resource->endpoint || !command) return TURBO_EINVAL;
  if (resource->kind != TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE) return TURBO_ENOTSUP;
  timeout_ns = flowie_timeout_ns(resource->endpoint);
  if (command->deadline_ns != UINT64_MAX) {
    now = turbo_hrtime();
    if (now >= command->deadline_ns) return TURBO_ETIMEDOUT;
    if (command->deadline_ns - now < timeout_ns) timeout_ns = command->deadline_ns - now;
  }
  return flowie_management_apply(resource->endpoint, command->kind, timeout_ns);
}

static int flowie_start_resources(flowie_endpoint_t *endpoint) {
  int rc;
  if (!endpoint || !endpoint->flow || !endpoint->source_name) return TURBO_EINVAL;
  if (atomic_load_explicit(&endpoint->started, memory_order_acquire)) return TURBO_OK;
  if (atomic_load_explicit(&endpoint->generation, memory_order_acquire) == UINT64_MAX)
    return TURBO_ERANGE;
  (void)atomic_fetch_add_explicit(&endpoint->generation, 1u, memory_order_acq_rel);
  if (endpoint->send_budget_initialized) {
    rc = tf_io_budget_open(&endpoint->send_budget);
    if (rc != TURBO_OK) return rc;
  }
  atomic_store_explicit(&endpoint->quiesced, 0, memory_order_release);
  atomic_store_explicit(&endpoint->last_management_status, TURBO_OK, memory_order_release);
  atomic_store_explicit(&endpoint->started, 1, memory_order_release);
  tf_connection_transition(&endpoint->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_ENOTCONN);
  rc = tf_coronet_execution_start(&endpoint->execution);
  if (rc == TURBO_OK) {
    rc = tf_coronet_execution_call(&endpoint->execution, flowie_listener_start_call, endpoint,
                                   flowie_timeout_ns(endpoint));
  }
  if (rc == TURBO_OK) {
    tf_connection_transition(&endpoint->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
    return TURBO_OK;
  }
  atomic_store_explicit(&endpoint->started, 0, memory_order_release);
  atomic_store_explicit(&endpoint->quiesced, 0, memory_order_release);
  if (endpoint->send_budget_initialized) tf_io_budget_close(&endpoint->send_budget);
  flowie_fail_reply_queue(endpoint);
  tf_connection_transition(&endpoint->connection, TURBO_FLOW_CONNECTION_FAILED, rc);
  (void)tf_coronet_execution_call(&endpoint->execution, flowie_listener_close_call, endpoint,
                                  flowie_timeout_ns(endpoint));
  flowie_wait_tasks(endpoint);
  tf_coronet_execution_stop(&endpoint->execution);
  atomic_store_explicit(&endpoint->quiesced, 0, memory_order_release);
  return rc;
}

static void flowie_stop_resources(flowie_endpoint_t *endpoint) {
  if (!endpoint || !atomic_load_explicit(&endpoint->started, memory_order_acquire)) return;
  tf_connection_transition(&endpoint->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_ESHUTDOWN);
  atomic_store_explicit(&endpoint->started, 0, memory_order_release);
  if (endpoint->expiry_wait) (void)coro_wait_interrupt(endpoint->expiry_wait, TURBO_ESHUTDOWN);
  if (endpoint->send_budget_initialized) tf_io_budget_close(&endpoint->send_budget);
  (void)tf_coronet_execution_call(&endpoint->execution, flowie_listener_close_call, endpoint,
                                  flowie_timeout_ns(endpoint));
  flowie_fail_reply_queue(endpoint);
  flowie_wait_tasks(endpoint);
  tf_coronet_execution_stop(&endpoint->execution);
  atomic_store_explicit(&endpoint->quiesced, 0, memory_order_release);
  flowie_connection_usage(endpoint);
  tf_connection_transition(&endpoint->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
}

static int flowie_endpoint_start(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  turbo_flow_protocol_route_owner_ops_t route_owner_ops = TURBO_FLOW_PROTOCOL_ROUTE_OWNER_OPS_INIT;
  tstr_t source_name;
  int rc;
  if (!endpoint || !flow || !stage) return TURBO_EINVAL;
  if (!endpoint->route_owner_registered) {
    route_owner_ops.settle = flowie_endpoint_protocol_route_settle;
    rc = turbo_flow_register_protocol_route_owner(
        flow, TURBO_FLOW_PROTOCOL_MQTT, endpoint->instance_id, &route_owner_ops, endpoint);
    if (rc != TURBO_OK) return rc;
    endpoint->route_owner_registered = 1;
  }
  if (stage->is_source) {
    source_name = tstr_dup(stage->name);
    if (!source_name) return TURBO_ENOMEM;
    if (endpoint->source_name && tstr_cmp(endpoint->source_name, source_name) != 0) {
      tstr_free(source_name);
      return TURBO_EALREADY;
    }
    tstr_freep(&endpoint->source_name);
    endpoint->source_name = source_name;
  } else {
    endpoint->reply_refs += 1;
  }
  endpoint->flow = flow;
  endpoint->start_refs += 1;
  if (atomic_load_explicit(&endpoint->started, memory_order_acquire)) return TURBO_OK;
  if (!endpoint->source_name) return TURBO_OK;
  rc = flowie_start_resources(endpoint);
  if (rc != TURBO_OK) {
    endpoint->start_refs -= 1;
    endpoint->flow = NULL;
  }
  return rc;
}

static void flowie_endpoint_stop(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  (void)flow;
  if (!endpoint || endpoint->start_refs <= 0) return;
  if (stage && !stage->is_source && endpoint->reply_refs > 0) endpoint->reply_refs -= 1;
  endpoint->start_refs -= 1;
  if (endpoint->start_refs > 0) return;
  flowie_stop_resources(endpoint);
  endpoint->flow = NULL;
}

static int flowie_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  tf_io_budget_snapshot_t budget = {0};
  int rc;
  if (!endpoint) return TURBO_EINVAL;
  rc = tf_connection_snapshot(&endpoint->connection, out);
  if (rc != TURBO_OK || !endpoint->send_budget_initialized) return rc;
  rc = tf_io_budget_snapshot(&endpoint->send_budget, &budget);
  if (rc != TURBO_OK) return rc;
  out->in_flight_messages = budget.messages;
  out->in_flight_bytes = budget.bytes;
  return TURBO_OK;
}

static int flowie_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flowie_endpoint_resource_t *resource = (flowie_endpoint_resource_t *)ctx;
  flowie_endpoint_t *endpoint;
  const char *uid;
  int written;
  if (!resource || !resource->endpoint || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  endpoint = resource->endpoint;
  *out = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  out->kind = resource->kind;
  if (resource->kind == TURBO_FLOW_RESOURCE_CONNECTION) {
    out->domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    uid = endpoint->connection_uid;
  } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE) {
    out->domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    uid = endpoint->protocol_uid;
  } else if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
    out->domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    uid = endpoint->queue_uid;
  } else {
    return TURBO_EINVAL;
  }
  out->generation = atomic_load_explicit(&endpoint->generation, memory_order_acquire);
  if (out->generation == 0u) out->generation = 1u;
  out->observed_generation = out->generation;
  written = snprintf(out->uid, sizeof(out->uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(out->uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(out->owner_name, sizeof(out->owner_name), "%s", endpoint->owner_name);
  return written < 0 || (size_t)written >= sizeof(out->owner_name) ? TURBO_ENAMETOOLONG : TURBO_OK;
}

static int flowie_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  flowie_endpoint_resource_t *resource = (flowie_endpoint_resource_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_connection_snapshot_t connection = {0};
  int rc;
  if (!resource || !resource->endpoint || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flowie_resource_metadata(resource, &metadata);
  if (rc != TURBO_OK) return rc;
  rc = tf_connection_snapshot(&resource->endpoint->connection, &connection);
  if (rc != TURBO_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER &&
      resource->endpoint->send_budget_initialized) {
    tf_io_budget_snapshot_t budget = {0};
    if (tf_io_budget_snapshot(&resource->endpoint->send_budget, &budget) != TURBO_OK)
      return TURBO_EINVAL;
    out->load = budget.bytes;
    out->capacity = budget.max_bytes;
  } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE &&
             resource->endpoint->manage_sessions) {
    out->load = atomic_load_explicit(&resource->endpoint->sessions_current, memory_order_acquire);
    out->capacity = resource->endpoint->max_sessions;
  } else {
    out->load = connection.connections_current;
    out->capacity = connection.connection_limit;
  }
  out->saturated = out->capacity != 0u && out->load >= out->capacity;
  out->last_status = connection.last_status;
  return TURBO_OK;
}

static int flowie_resource_document(void *ctx, turbo_flow_resource_document_kind_t document_kind,
                                    turbo_flow_resource_document_t *out) {
  flowie_endpoint_resource_t *resource = (flowie_endpoint_resource_t *)ctx;
  flowie_endpoint_t *endpoint;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_connection_snapshot_t connection = {0};
  tf_io_budget_snapshot_t budget = {0};
  const turbo_flow_resource_schema_t *schema = NULL;
  tstr_t payload = NULL;
  int started;
  int quiesced;
  int rc;
  if (!resource || !resource->endpoint || !out || out->size < sizeof(*out) || out->payload)
    return TURBO_EINVAL;
  endpoint = resource->endpoint;
  rc = flowie_resource_metadata(resource, &metadata);
  if (rc != TURBO_OK) return rc;
  rc = flowie_connection_snapshot(endpoint, &connection);
  if (rc != TURBO_OK) return rc;
  if (endpoint->send_budget_initialized) {
    rc = tf_io_budget_snapshot(&endpoint->send_budget, &budget);
    if (rc != TURBO_OK) return rc;
  }
  started = atomic_load_explicit(&endpoint->started, memory_order_acquire) != 0;
  quiesced = atomic_load_explicit(&endpoint->quiesced, memory_order_acquire) != 0;
  if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) {
    if (resource->kind == TURBO_FLOW_RESOURCE_CONNECTION) {
      schema = &FLOWIE_CONNECTION_STATUS_SCHEMA;
      payload = tstr_format("{\"state\":{},\"last_status\":{},\"connections_current\":\"{}\","
                            "\"connection_limit\":\"{}\",\"in_flight_messages\":\"{}\","
                            "\"in_flight_bytes\":\"{}\",\"started\":{},\"accepting\":{}}",
                            (unsigned)connection.state, connection.last_status,
                            connection.connections_current, connection.connection_limit,
                            connection.in_flight_messages, connection.in_flight_bytes,
                            started ? "true" : "false", started && !quiesced ? "true" : "false");
    } else if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
      tstr_t updated;
      schema = &FLOWIE_QUEUE_STATUS_SCHEMA;
      payload = tstr_format("{\"load\":\"{}\",\"capacity\":\"{}\",\"messages\":\"{}\","
                            "\"bytes\":\"{}\",\"connection_hwm_bytes\":\"{}\","
                            "\"slow_subscriber_policy\":{},",
                            budget.bytes, budget.max_bytes, budget.messages, budget.bytes,
                            endpoint->send_hwm_bytes, (unsigned)endpoint->slow_subscriber_policy);
      if (!payload) return TURBO_ENOMEM;
      updated = tstr_append_format(
          payload, "\"slow_subscriber_disconnects\":\"{}\",\"saturated\":{},\"accepting\":{}}",
          atomic_load_explicit(&endpoint->slow_subscriber_disconnects, memory_order_relaxed),
          budget.max_bytes != 0u && budget.bytes >= budget.max_bytes ? "true" : "false",
          started && !quiesced ? "true" : "false");
      if (!updated) {
        tstr_free(payload);
        return TURBO_ENOMEM;
      }
      payload = updated;
    } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE) {
      size_t sessions = atomic_load_explicit(&endpoint->sessions_current, memory_order_acquire);
      tstr_t updated;
      schema = &FLOWIE_PROTOCOL_STATUS_SCHEMA;
      payload = tstr_format("{\"transport\":{},\"sessions\":\"{}\",\"session_capacity\":\"{}\","
                            "\"retained_messages\":\"{}\",\"retained_capacity\":\"{}\","
                            "\"manage_sessions\":{},",
                            (unsigned)endpoint->transport, sessions, endpoint->max_sessions,
                            atomic_load_explicit(&endpoint->retained_current, memory_order_acquire),
                            endpoint->max_retained_messages,
                            endpoint->manage_sessions ? "true" : "false");
      if (!payload) return TURBO_ENOMEM;
      updated = tstr_append_format(
          payload,
          "\"security_enabled\":{},\"persistence_enabled\":{},\"started\":{},"
          "\"accepting\":{}}",
          endpoint->security_enabled ? "true" : "false",
          endpoint->persistence_enabled ? "true" : "false", started ? "true" : "false",
          started && !quiesced &&
                  (endpoint->max_sessions == 0u || sessions < endpoint->max_sessions)
              ? "true"
              : "false");
      if (!updated) {
        tstr_free(payload);
        return TURBO_ENOMEM;
      }
      payload = updated;
    } else {
      return TURBO_EINVAL;
    }
  } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE &&
             document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS) {
    size_t sessions = atomic_load_explicit(&endpoint->sessions_current, memory_order_acquire);
    int saturated = endpoint->max_sessions != 0u && sessions >= endpoint->max_sessions;
    int accepting = started && !quiesced && !saturated;
    int drained = connection.connections_current == 0u && budget.messages == 0u;
    schema = &FLOWIE_PROTOCOL_CONDITIONS_SCHEMA;
    payload = tstr_format(
        "{\"ready_status\":{},\"ready_reason\":{},"
        "\"accepting_status\":{},\"accepting_reason\":{},"
        "\"drained_status\":{},\"drained_reason\":{},"
        "\"saturated_status\":{},\"saturated_reason\":{}}",
        started ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        started ? TURBO_FLOW_RESOURCE_REASON_RUNNING : TURBO_FLOW_RESOURCE_REASON_NOT_RUNNING,
        accepting ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        accepting ? TURBO_FLOW_RESOURCE_REASON_ACCEPTING : TURBO_FLOW_RESOURCE_REASON_NOT_ACCEPTING,
        drained ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        drained ? TURBO_FLOW_RESOURCE_REASON_DRAINED : TURBO_FLOW_RESOURCE_REASON_WORK_PENDING,
        saturated ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        saturated ? TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED
                  : TURBO_FLOW_RESOURCE_REASON_CAPACITY_AVAILABLE);
  } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE &&
             document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_EVENT) {
    int status = atomic_load_explicit(&endpoint->last_management_status, memory_order_acquire);
    schema = &FLOWIE_PROTOCOL_EVENT_SCHEMA;
    payload =
        tstr_format("{\"sequence\":\"{}\",\"generation\":\"{}\","
                    "\"observed_generation\":\"{}\",\"gap\":false,\"last_status\":{},"
                    "\"reason\":{}}",
                    metadata.generation, metadata.generation, metadata.observed_generation, status,
                    status == TURBO_OK ? TURBO_FLOW_RESOURCE_REASON_OBSERVATION_CURRENT
                                       : TURBO_FLOW_RESOURCE_REASON_OWNER_ERROR);
  } else {
    return TURBO_ENOTSUP;
  }
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, schema, payload,
                                                     tstr_len(payload));
  tstr_free(payload);
  return rc;
}

static void flowie_endpoint_shutdown(void *ctx) {
  flowie_endpoint_t *endpoint = (flowie_endpoint_t *)ctx;
  if (!endpoint) return;
  if (endpoint->start_refs > 0) {
    endpoint->start_refs = 1;
    flowie_endpoint_stop(endpoint, NULL, NULL);
  }
  if (endpoint->expiry_wait) {
    (void)coro_wait_destroy(endpoint->expiry_wait);
    endpoint->expiry_wait = NULL;
  }
  tf_coronet_execution_destroy(&endpoint->execution);
  if (endpoint->task_sync_initialized) {
    turbo_cond_destroy(&endpoint->task_drained);
    turbo_mutex_destroy(&endpoint->task_mutex);
  }
  if (endpoint->send_queue_initialized) {
    flowie_fail_reply_queue(endpoint);
    flowie_reply_queue_t_destroy(&endpoint->send_queue);
    turbo_mutex_destroy(&endpoint->send_queue_mutex);
    endpoint->send_queue_initialized = 0;
  }
  if (endpoint->send_budget_initialized) {
    tf_io_budget_destroy(&endpoint->send_budget);
    endpoint->send_budget_initialized = 0;
  }
  for (size_t i = 0u; i < turbo_vec_size(&endpoint->clients); ++i) {
    flowie_endpoint_connection_t **slot =
        (flowie_endpoint_connection_t **)turbo_vec_at(&endpoint->clients, i);
    if (slot && *slot) {
      flowie_connection_fail_reply_queue(*slot);
      if ((*slot)->send_budget_initialized) tf_io_budget_destroy(&(*slot)->send_budget);
      if ((*slot)->send_queue_initialized) flowie_reply_queue_t_destroy(&(*slot)->send_queue);
      free(*slot);
    }
  }
  turbo_vec_destroy(&endpoint->clients);
  if (endpoint->subscription_index_initialized) {
    flowie_topic_index_destroy(&endpoint->subscription_topics);
    turbo_hash_map_destroy(&endpoint->subscription_filter_index);
    turbo_vec_destroy(&endpoint->subscription_free_slots);
    flowie_subscription_entries_destroy(&endpoint->subscription_index);
    endpoint->subscription_index_initialized = 0;
  }
  flowie_retained_messages_destroy(endpoint);
  if (endpoint->sessions_initialized) {
    for (size_t i = 0u; i < turbo_vec_size(&endpoint->sessions); ++i) {
      flowie_endpoint_session_t **slot =
          (flowie_endpoint_session_t **)turbo_vec_at(&endpoint->sessions, i);
      if (slot && *slot) flowie_session_destroy(*slot);
    }
    turbo_hash_map_destroy(&endpoint->session_index);
    turbo_vec_destroy(&endpoint->sessions);
    endpoint->sessions_initialized = 0;
  }
  if (endpoint->routes_initialized) {
    flowie_route_map_t_destroy(&endpoint->routes);
    endpoint->routes_initialized = 0;
  }
  tstr_freep(&endpoint->host);
  tstr_freep(&endpoint->path);
  tstr_freep(&endpoint->source_name);
  tstr_freep(&endpoint->security_realm_channel);
  tstr_freep(&endpoint->security_auth_method);
  tstr_freep(&endpoint->session_store_channel);
  free(endpoint);
}

static int flowie_endpoint_identity_init(flowie_endpoint_t *endpoint, const char *name) {
  int written;
  written = snprintf(endpoint->owner_name, sizeof(endpoint->owner_name), "%s", name);
  if (written < 0 || (size_t)written >= sizeof(endpoint->owner_name)) return TURBO_ENAMETOOLONG;
  written =
      snprintf(endpoint->connection_uid, sizeof(endpoint->connection_uid), "%s.connection", name);
  if (written < 0 || (size_t)written >= sizeof(endpoint->connection_uid)) {
    return TURBO_ENAMETOOLONG;
  }
  written = snprintf(endpoint->queue_uid, sizeof(endpoint->queue_uid), "%s.queue", name);
  if (written < 0 || (size_t)written >= sizeof(endpoint->queue_uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(endpoint->protocol_uid, sizeof(endpoint->protocol_uid), "%s.protocol", name);
  return written < 0 || (size_t)written >= sizeof(endpoint->protocol_uid) ? TURBO_ENAMETOOLONG
                                                                          : TURBO_OK;
}

static int flowie_endpoint_connection_init(flowie_endpoint_t *endpoint) {
  char address[TURBO_FLOW_ENDPOINT_MAX + 1u];
  const char *scheme = flowie_transport_scheme(endpoint->transport);
  int written;
  if (!scheme) return TURBO_EINVAL;
  if (endpoint->transport == FLOWIE_TRANSPORT_PIPE) {
    const char *path = endpoint->path && tstr_len(endpoint->path) ? endpoint->path : endpoint->host;
    written = strncmp(path, "pipe://", 7u) == 0
                  ? snprintf(address, sizeof(address), "%s", path)
                  : snprintf(address, sizeof(address), "pipe://%s", path);
  } else {
    written =
        snprintf(address, sizeof(address), "%s://%s:%d", scheme, endpoint->host, endpoint->port);
  }
  if (written < 0 || (size_t)written >= sizeof(address)) return TURBO_ENAMETOOLONG;
  return tf_connection_init(&endpoint->connection, address, endpoint->max_connections);
}

static int flowie_retained_record_decode(const flowie_endpoint_t *endpoint,
                                         const turbo_flow_record_view_t *record,
                                         flowie_retained_message_t *out) {
  flowie_retained_message_t retained;
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_mqtt_span_t topic;
  const uint8_t *packet_bytes = NULL;
  size_t packet_size = 0u;
  size_t consumed = 0u;
  size_t offset = 0u;
  uint8_t expected_type = 1u;
  int rc = TURBO_EPROTO;
  memset(&retained, 0, sizeof(retained));
  if (out) memset(out, 0, sizeof(*out));
  if (!endpoint || !record || record->size < sizeof(*record) || !out || !record->key ||
      record->key_size <= FLOWIE_RETAINED_KEY_PREFIX_SIZE || !record->value ||
      record->value_size == 0u || record->revision == 0u ||
      record->revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
      memcmp(record->key, FLOWIE_RETAINED_KEY_PREFIX, FLOWIE_RETAINED_KEY_PREFIX_SIZE) != 0)
    return TURBO_EINVAL;
  topic = (flowie_mqtt_span_t){record->key + FLOWIE_RETAINED_KEY_PREFIX_SIZE,
                               record->key_size - FLOWIE_RETAINED_KEY_PREFIX_SIZE};
  if (!flowie_mqtt_topic_name_validate(topic)) return TURBO_EPROTO;
  while (offset < record->value_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    const uint8_t *value;
    size_t value_size;
    rc = turbo_ltv_peek_size(record->value + offset, record->value_size - offset, &payload_size,
                             &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > record->value_size - offset ||
        payload_size > record->value_size - offset - header_size) {
      rc = TURBO_EPROTO;
      goto fail;
    }
    wire_size = header_size + payload_size;
    if (turbo_parse_ltv(record->value + offset, wire_size, &message) != TURBO_OK || !message) {
      turbo_free_ltv(&message);
      rc = TURBO_EPROTO;
      goto fail;
    }
    type = turbo_ltv_type(message);
    value = turbo_ltv_value(message);
    value_size = turbo_ltv_value_len(message);
    if (type != expected_type) {
      turbo_free_ltv(&message);
      rc = TURBO_EPROTO;
      goto fail;
    }
    if (type == 1u) {
      if (offset != 0u || value_size != FLOWIE_RETAINED_RECORD_HEADER_SIZE ||
          memcmp(value, "FRET", 4u) != 0 || value[4] != 0u ||
          value[5] != FLOWIE_RETAINED_RECORD_VERSION || value[6] != 0u || value[7] != 0u) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
    } else if (type == 2u) {
      if (value_size != FLOWIE_RETAINED_RECORD_METADATA_SIZE ||
          (value[0] != FLOWIE_MQTT_VERSION_3_1_1 && value[0] != FLOWIE_MQTT_VERSION_5)) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      retained.version = (flowie_mqtt_version_t)value[0];
      retained.publisher_session_id = flowie_endpoint_record_read_u64(value + 1u);
      retained.expiry_at_epoch_seconds = flowie_endpoint_record_read_u64(value + 9u);
      if (retained.publisher_session_id == 0u) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
    } else {
      if (offset + wire_size != record->value_size) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        goto fail;
      }
      packet_size = value_size;
      retained.packet = tstr_new_len(value, value_size);
      if (!retained.packet) {
        turbo_free_ltv(&message);
        rc = TURBO_ENOMEM;
        goto fail;
      }
      packet_bytes = (const uint8_t *)retained.packet;
    }
    ++expected_type;
    turbo_free_ltv(&message);
    offset += wire_size;
  }
  if (expected_type != 4u || !packet_bytes || packet_size == 0u) return TURBO_EPROTO;
  options.version = retained.version;
  options.max_packet_size = endpoint->max_packet_size;
  if (flowie_mqtt_packet_parse(packet_bytes, packet_size, &options, &packet, &consumed, NULL) !=
          FLOWIE_MQTT_PARSE_OK ||
      consumed != packet_size || packet.type != FLOWIE_MQTT_PACKET_PUBLISH ||
      flowie_mqtt_publish_parse(&packet, &publish) != FLOWIE_MQTT_PARSE_OK || !publish.retain ||
      publish.payload.size == 0u || publish.topic.size != topic.size ||
      memcmp(publish.topic.data, topic.data, topic.size) != 0)
    return TURBO_EPROTO;
  retained.topic = tstr_new_len(topic.data, topic.size);
  retained.revision = record->revision;
  if (!retained.topic || !retained.packet) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  *out = retained;
  return TURBO_OK;

fail:
  tstr_freep(&retained.topic);
  tstr_freep(&retained.packet);
  return rc;
}

typedef struct flowie_expired_record_s {
  tstr_t key;
  uint64_t revision;
} flowie_expired_record_t;

typedef struct flowie_restore_context_s {
  flowie_endpoint_t *endpoint;
  turbo_vec_t expired;
  uint64_t now_epoch_seconds;
} flowie_restore_context_t;

static void flowie_expired_records_destroy(turbo_vec_t *records) {
  if (!records) return;
  for (size_t i = 0u; i < turbo_vec_size(records); ++i) {
    flowie_expired_record_t *record = (flowie_expired_record_t *)turbo_vec_at(records, i);
    if (record) tstr_freep(&record->key);
  }
  turbo_vec_destroy(records);
}

static int flowie_restore_expired_add(flowie_restore_context_t *context,
                                      const turbo_flow_record_view_t *record) {
  flowie_expired_record_t expired;
  int rc;
  memset(&expired, 0, sizeof(expired));
  expired.key = tstr_new_len(record->key, record->key_size);
  expired.revision = record->revision;
  if (!expired.key) return TURBO_ENOMEM;
  rc = turbo_vec_push(&context->expired, &expired);
  if (rc != TURBO_OK) tstr_freep(&expired.key);
  return rc;
}

static int flowie_endpoint_restore_visit(void *ctx, const turbo_flow_record_view_t *record) {
  flowie_restore_context_t *context = (flowie_restore_context_t *)ctx;
  flowie_endpoint_t *endpoint;
  flowie_endpoint_session_t *session = NULL;
  flowie_session_owner_t *owner = NULL;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  uint64_t expiry_at = 0u;
  uint64_t will_at = 0u;
  uint64_t remaining;
  uint64_t duration_ns;
  uint64_t now_ns;
  int session_ended;
  int rc;
  if (!context || !(endpoint = context->endpoint) || !record || record->size < sizeof(*record))
    return TURBO_EINVAL;
  if (!record->key || record->key_size == 0u) return TURBO_EPROTO;
  if (record->key[0] == 0u) {
    flowie_retained_message_t retained;
    tstr_v key;
    size_t index;
    memset(&retained, 0, sizeof(retained));
    if (record->key_size <= FLOWIE_RETAINED_KEY_PREFIX_SIZE ||
        memcmp(record->key, FLOWIE_RETAINED_KEY_PREFIX, FLOWIE_RETAINED_KEY_PREFIX_SIZE) != 0)
      return TURBO_EPROTO;
    rc = flowie_retained_record_decode(endpoint, record, &retained);
    if (rc != TURBO_OK) return rc;
    if (retained.expiry_at_epoch_seconds != 0u && context->now_epoch_seconds == 0u) {
      tstr_freep(&retained.topic);
      tstr_freep(&retained.packet);
      return TURBO_EIO;
    }
    if (retained.expiry_at_epoch_seconds != 0u &&
        retained.expiry_at_epoch_seconds <= context->now_epoch_seconds) {
      tstr_freep(&retained.topic);
      tstr_freep(&retained.packet);
      return flowie_restore_expired_add(context, record);
    }
    if (turbo_vec_size(&endpoint->retained_messages) >= endpoint->max_retained_messages) {
      rc = TURBO_ENOSPC;
      goto retained_fail;
    }
    rc = turbo_vec_push(&endpoint->retained_messages, &retained);
    if (rc != TURBO_OK) goto retained_fail;
    index = turbo_vec_size(&endpoint->retained_messages) - 1u;
    key = tstr_to_v(retained.topic);
    rc = turbo_hash_map_put(&endpoint->retained_index, &key, &index);
    if (rc != TURBO_OK) {
      (void)turbo_vec_resize(&endpoint->retained_messages, index);
      goto retained_fail;
    }
    return TURBO_OK;

  retained_fail:
    tstr_freep(&retained.topic);
    tstr_freep(&retained.packet);
    return rc;
  }
  rc = flowie_endpoint_record_decode(endpoint, record, &owner, &principal, &expiry_at, &will_at);
  if (rc != TURBO_OK) return rc;
  rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc != TURBO_OK) goto fail;
  session_ended = snapshot.session_expiry_interval == 0u ||
                  (expiry_at != 0u && expiry_at <= context->now_epoch_seconds);
  if (session_ended && !snapshot.will_pending) {
    flowie_session_owner_destroy(owner);
    return flowie_restore_expired_add(context, record);
  }
  if (turbo_vec_size(&endpoint->sessions) >= endpoint->max_sessions) {
    rc = TURBO_ENOSPC;
    goto fail;
  }
  session = (flowie_endpoint_session_t *)calloc(1u, sizeof(*session));
  if (!session) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  session->security_resource = tstr_new_len("", 0u);
  session->client_id_owned = tstr_new_len(record->key, record->key_size);
  if (!session->security_resource || !session->client_id_owned) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  session->client_id =
      tstr_v_from_buf(session->client_id_owned, tstr_len(session->client_id_owned));
  session->owner = owner;
  owner = NULL;
  session->principal = principal;
  now_ns = turbo_hrtime();
  if (session_ended) {
    if (context->now_epoch_seconds == 0u) {
      rc = TURBO_EIO;
      goto fail;
    }
    session->expiry_at_epoch_seconds = expiry_at ? expiry_at : context->now_epoch_seconds;
    session->expiry_deadline_ns = now_ns == 0u ? 1u : now_ns;
    session->expiry_session_generation = snapshot.session_generation;
  } else if (snapshot.session_expiry_interval != UINT32_MAX) {
    if (context->now_epoch_seconds == 0u) {
      rc = TURBO_EIO;
      goto fail;
    }
    remaining =
        expiry_at == 0u ? snapshot.session_expiry_interval : expiry_at - context->now_epoch_seconds;
    session->expiry_at_epoch_seconds = expiry_at == 0u
                                           ? (context->now_epoch_seconds > UINT64_MAX - remaining
                                                  ? UINT64_MAX
                                                  : context->now_epoch_seconds + remaining)
                                           : expiry_at;
    duration_ns = remaining > UINT64_MAX / UINT64_C(1000000000) ? UINT64_MAX
                                                                : remaining * UINT64_C(1000000000);
    session->expiry_deadline_ns =
        now_ns > UINT64_MAX - duration_ns ? UINT64_MAX : now_ns + duration_ns;
    if (session->expiry_deadline_ns == 0u) session->expiry_deadline_ns = 1u;
    session->expiry_session_generation = snapshot.session_generation;
  }
  if (snapshot.will_pending) {
    if (context->now_epoch_seconds == 0u) {
      rc = TURBO_EIO;
      goto fail;
    }
    if (will_at == 0u || session_ended) will_at = context->now_epoch_seconds;
    if (session->expiry_at_epoch_seconds != 0u && will_at > session->expiry_at_epoch_seconds)
      will_at = session->expiry_at_epoch_seconds;
    session->will_at_epoch_seconds = will_at;
    remaining = will_at > context->now_epoch_seconds ? will_at - context->now_epoch_seconds : 0u;
    duration_ns = remaining > UINT64_MAX / UINT64_C(1000000000) ? UINT64_MAX
                                                                : remaining * UINT64_C(1000000000);
    session->will_deadline_ns =
        now_ns > UINT64_MAX - duration_ns ? UINT64_MAX : now_ns + duration_ns;
    if (session->will_deadline_ns == 0u) session->will_deadline_ns = 1u;
    session->will_session_generation = snapshot.session_generation;
  }
  rc = turbo_vec_push(&endpoint->sessions, &session);
  if (rc == TURBO_OK)
    rc = turbo_hash_map_put(&endpoint->session_index, &session->client_id, &session);
  if (rc != TURBO_OK) {
    if (turbo_vec_size(&endpoint->sessions) != 0u) {
      flowie_endpoint_session_t *const *last =
          (flowie_endpoint_session_t *const *)turbo_vec_at_const(
              &endpoint->sessions, turbo_vec_size(&endpoint->sessions) - 1u);
      if (last && *last == session)
        (void)turbo_vec_resize(&endpoint->sessions, turbo_vec_size(&endpoint->sessions) - 1u);
    }
    goto fail;
  }
  if (snapshot.session_id > endpoint->next_route_id) endpoint->next_route_id = snapshot.session_id;
  return TURBO_OK;

fail:
  flowie_session_owner_destroy(owner);
  flowie_session_destroy(session);
  return rc;
}

static int flowie_endpoint_delete_expired(flowie_endpoint_t *endpoint, turbo_vec_t *expired) {
  turbo_flow_record_mutation_t *mutations;
  size_t offset = 0u;
  size_t batch_capacity;
  int rc = TURBO_OK;
  if (!endpoint || !expired || !endpoint->session_store) return TURBO_EINVAL;
  if (turbo_vec_size(expired) == 0u) return TURBO_OK;
  batch_capacity = endpoint->session_store->max_batch_size;
  if (batch_capacity > turbo_vec_size(expired)) batch_capacity = turbo_vec_size(expired);
  mutations = (turbo_flow_record_mutation_t *)calloc(batch_capacity, sizeof(*mutations));
  if (!mutations) return TURBO_ENOMEM;
  while (offset < turbo_vec_size(expired)) {
    size_t count = turbo_vec_size(expired) - offset;
    if (count > batch_capacity) count = batch_capacity;
    for (size_t i = 0u; i < count; ++i) {
      flowie_expired_record_t *record =
          (flowie_expired_record_t *)turbo_vec_at(expired, offset + i);
      mutations[i] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
      mutations[i].kind = TURBO_FLOW_RECORD_DELETE;
      mutations[i].key = (const uint8_t *)record->key;
      mutations[i].key_size = tstr_len(record->key);
      mutations[i].expected_revision = record->revision;
    }
    rc = endpoint->session_store->commit(endpoint->session_store->ctx, mutations, count);
    if (rc != TURBO_OK) break;
    offset += count;
  }
  free(mutations);
  return rc;
}

static int flowie_endpoint_restore_sessions(flowie_endpoint_t *endpoint) {
  flowie_restore_context_t context;
  int rc;
  if (!endpoint || !endpoint->persistence_enabled || !endpoint->session_store) return TURBO_EINVAL;
  memset(&context, 0, sizeof(context));
  context.endpoint = endpoint;
  context.now_epoch_seconds = flowie_security_now_epoch_seconds();
  rc = turbo_vec_init(&context.expired, sizeof(flowie_expired_record_t));
  if (rc != TURBO_OK) return rc;
  rc = endpoint->session_store->scan(endpoint->session_store->ctx, flowie_endpoint_restore_visit,
                                     &context);
  if (rc == TURBO_OK) rc = flowie_endpoint_delete_expired(endpoint, &context.expired);
  if (rc == TURBO_OK) rc = flowie_subscription_index_rebuild(endpoint);
  if (rc == TURBO_OK) {
    atomic_store_explicit(&endpoint->sessions_current, turbo_vec_size(&endpoint->sessions),
                          memory_order_release);
    atomic_store_explicit(&endpoint->retained_current, turbo_vec_size(&endpoint->retained_messages),
                          memory_order_release);
  }
  flowie_expired_records_destroy(&context.expired);
  return rc;
}

static int flowie_register_mqtt_server_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {"MqttConnection", "MqttSendQueue",
                                                "MqttSessionAggregate"};
  static const char *const operation_names[] = {FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION,
                                                FLOWIE_MQTT_PACKET_EGRESS_OPERATION};
  turbo_flow_operation_descriptor_t operations[2];
  turbo_flow_module_descriptor_t module;
  memset(operations, 0, sizeof(operations));
  memset(&module, 0, sizeof(module));
  for (size_t i = 0u; i < 2u; ++i) {
    operations[i].size = sizeof(operations[i]);
    operations[i].name = operation_names[i];
    operations[i].version = 1u;
    operations[i].domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    operations[i].scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operations[i].scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
    operations[i].scope.lifetime = i == 0u ? TURBO_FLOW_LIFETIME_DISPATCH
                                            : TURBO_FLOW_LIFETIME_CALL;
    operations[i].scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    operations[i].scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    operations[i].flags = (i == 0u ? TURBO_FLOW_OPERATION_SOURCE
                                   : TURBO_FLOW_OPERATION_STAGE) |
                          TURBO_FLOW_OPERATION_BRIDGE;
    operations[i].execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  }
  operations[0].output_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[0].output_type = "Message";
  operations[1].input_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[1].input_type = "Message";
  module.size = sizeof(module);
  module.name = FLOWIE_MQTT_SERVER_MODULE;
  module.version = 1u;
  module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                            TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                            TURBO_FLOW_MODULE_NATIVE_API;
  module.primitive_types = primitive_types;
  module.primitive_type_count = 3u;
  module.operation_names = operation_names;
  module.operation_count = 2u;
  return turbo_flow_register_module_contract(flow, &module, operations, 2u);
}

static int
flowie_register_endpoint_internal(turbo_flow_t *flow, const char *name,
                                  const flowie_endpoint_config_t *config,
                                  const turbo_flow_coronet_execution_binding_t *execution,
                                  const flowie_endpoint_security_binding_t *security,
                                  const flowie_endpoint_persistence_binding_t *persistence) {
  flowie_endpoint_t *endpoint;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_resource_provider_registration_t resources[3];
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  static const char *const operation_names[] = {FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION,
                                                FLOWIE_MQTT_PACKET_EGRESS_OPERATION};
  int rc;
  if (!flow || !name || name[0] == '\0' || !execution) return TURBO_EINVAL;
  if (turbo_flow_state(flow) == TURBO_FLOW_STATE_COMPILED ||
      turbo_flow_state(flow) == TURBO_FLOW_STATE_STARTED) {
    return TURBO_EBUSY;
  }
  if (turbo_flow_find_adapter_schema(flow, name)) return TURBO_EALREADY;
  rc = flowie_endpoint_config_validate(config);
  if (rc != TURBO_OK) return rc;
  if (security) {
    rc = flowie_endpoint_security_binding_validate(config, security);
    if (rc != TURBO_OK) return rc;
  }
  if (persistence) {
    rc = flowie_endpoint_persistence_binding_validate(config, persistence);
    if (rc != TURBO_OK) return rc;
  }
  rc = turbo_flow_coronet_execution_binding_validate(execution);
  if (rc != TURBO_OK) return rc;
  if (execution->kind != TURBO_FLOW_CORONET_EXECUTION_PRIVATE &&
      (config->coroutine_stack_size != 0u || config->recv_buffer_size != 0u))
    return TURBO_ENOTSUP;
  rc = flowie_register_mqtt_server_contract(flow);
  if (rc != TURBO_OK) return rc;
  endpoint = (flowie_endpoint_t *)calloc(1, sizeof(*endpoint));
  if (!endpoint) return TURBO_ENOMEM;
  endpoint->transport = config->transport;
  endpoint->settlement = config->settlement;
  endpoint->port = config->port;
  endpoint->max_packet_size =
      config->max_packet_size ? config->max_packet_size : FLOWIE_DEFAULT_MAX_PACKET_SIZE;
  endpoint->max_connections =
      config->max_connections ? config->max_connections : FLOWIE_DEFAULT_MAX_CONNECTIONS;
  endpoint->manage_sessions = config->manage_sessions;
  if (security) {
    endpoint->security_enabled = 1;
    endpoint->auth_provider = *security->auth_provider;
    endpoint->security_realm = security->realm;
    endpoint->security_realm_channel = tstr_dup(security->realm_channel);
    endpoint->security_auth_method = tstr_dup(security->auth_method);
  }
  if (persistence) {
    endpoint->persistence_enabled = 1;
    endpoint->session_store = persistence->store;
    endpoint->session_store_channel = tstr_dup(persistence->store_channel);
  }
  endpoint->max_sessions = config->max_sessions ? config->max_sessions : endpoint->max_connections;
  endpoint->max_retained_messages =
      config->max_retained_messages ? config->max_retained_messages : endpoint->max_sessions;
  endpoint->max_subscriptions_per_session = config->max_subscriptions_per_session
                                                ? config->max_subscriptions_per_session
                                                : FLOWIE_DEFAULT_MAX_SUBSCRIPTIONS_PER_SESSION;
  endpoint->max_inflight_per_session = config->max_inflight_per_session
                                           ? config->max_inflight_per_session
                                           : FLOWIE_DEFAULT_MAX_INFLIGHT_PER_SESSION;
  endpoint->send_hwm_bytes =
      config->send_hwm_bytes ? config->send_hwm_bytes : FLOWIE_DEFAULT_SEND_HWM_BYTES;
  endpoint->slow_subscriber_policy =
      config->slow_subscriber_policy == FLOWIE_SLOW_SUBSCRIBER_POLICY_UNSPECIFIED
          ? FLOWIE_SLOW_SUBSCRIBER_DISCONNECT
          : config->slow_subscriber_policy;
  endpoint->reuse_port = config->reuse_port;
  endpoint->host = config->host ? tstr_dup(config->host) : tstr_new();
  endpoint->path = config->path ? tstr_dup(config->path) : tstr_new();
  endpoint->timeouts.timeout_ms = config->timeout_ms;
  endpoint->timeouts.recv_timeout_ms = config->recv_timeout_ms;
  if (config->timeout_ms != 0u) endpoint->timeouts.set_flags |= TF_CORONET_TIMEOUT_SET_DEFAULT;
  if (config->recv_timeout_ms != 0u) endpoint->timeouts.set_flags |= TF_CORONET_TIMEOUT_SET_RECV;
  tf_coronet_socket_timeouts_resolve(&endpoint->timeouts, 0u);
  endpoint->socket_options.tcp_keepalive = config->tcp_keepalive;
  endpoint->socket_options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
  endpoint->socket_options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
  endpoint->socket_options.tcp_keepalive_count = config->tcp_keepalive_count;
  endpoint->socket_options.linger = config->linger;
  endpoint->socket_options.linger_ms = config->linger_ms;
  endpoint->socket_options.send_hwm_bytes = endpoint->send_hwm_bytes;
  atomic_init(&endpoint->started, 0);
  atomic_init(&endpoint->quiesced, 0);
  atomic_init(&endpoint->management_command_active, 0);
  atomic_init(&endpoint->last_management_status, TURBO_OK);
  atomic_init(&endpoint->generation, 1u);
  atomic_init(&endpoint->sessions_current, 0u);
  atomic_init(&endpoint->retained_current, 0u);
  atomic_init(&endpoint->slow_subscriber_disconnects, 0u);
  rc = flowie_allocate_endpoint_instance_id(&endpoint->instance_id);
  if (rc != TURBO_OK) {
    flowie_endpoint_shutdown(endpoint);
    return rc;
  }
  if (!endpoint->host || !endpoint->path ||
      (endpoint->security_enabled &&
       (!endpoint->security_realm_channel || !endpoint->security_auth_method)) ||
      (endpoint->persistence_enabled && !endpoint->session_store_channel) ||
      turbo_vec_init(&endpoint->clients, sizeof(flowie_endpoint_connection_t *)) != TURBO_OK) {
    flowie_endpoint_shutdown(endpoint);
    return TURBO_ENOMEM;
  }
  if (flowie_route_map_t_init(&endpoint->routes) != TURBO_OK) {
    flowie_endpoint_shutdown(endpoint);
    return TURBO_ENOMEM;
  }
  endpoint->routes_initialized = 1;
  if (endpoint->manage_sessions) {
    if (turbo_vec_init(&endpoint->sessions, sizeof(flowie_endpoint_session_t *)) != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    endpoint->sessions_initialized = 1;
    if (turbo_vec_init(&endpoint->subscription_index, sizeof(flowie_subscription_entry_t)) !=
        TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    endpoint->subscription_index_initialized = 1;
    if (turbo_vec_init(&endpoint->subscription_free_slots, sizeof(size_t)) != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    if (turbo_hash_map_init(&endpoint->subscription_filter_index, sizeof(tstr_v), sizeof(size_t),
                            flowie_session_key_hash, flowie_session_key_equal, NULL) != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    endpoint->subscription_index_valid = 1;
    if (flowie_topic_index_init(&endpoint->subscription_topics) != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    if (turbo_vec_init(&endpoint->retained_messages, sizeof(flowie_retained_message_t)) !=
        TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    if (turbo_hash_map_init(&endpoint->retained_index, sizeof(tstr_v), sizeof(size_t),
                            flowie_session_key_hash, flowie_session_key_equal, NULL) != TURBO_OK) {
      turbo_vec_destroy(&endpoint->retained_messages);
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    endpoint->retained_initialized = 1;
    if (turbo_hash_map_init(&endpoint->session_index, sizeof(tstr_v),
                            sizeof(flowie_endpoint_session_t *), flowie_session_key_hash,
                            flowie_session_key_equal, NULL) != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    if (turbo_vec_reserve(&endpoint->sessions, endpoint->max_sessions) != TURBO_OK ||
        turbo_hash_map_reserve(&endpoint->session_index, endpoint->max_sessions) != TURBO_OK ||
        turbo_vec_reserve(&endpoint->retained_messages, endpoint->max_retained_messages) !=
            TURBO_OK ||
        turbo_hash_map_reserve(&endpoint->retained_index, endpoint->max_retained_messages) !=
            TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
    if (endpoint->persistence_enabled) {
      rc = flowie_endpoint_restore_sessions(endpoint);
      if (rc != TURBO_OK) {
        flowie_endpoint_shutdown(endpoint);
        return rc;
      }
    }
  }
  turbo_mutex_init(&endpoint->task_mutex);
  turbo_cond_init(&endpoint->task_drained);
  endpoint->task_sync_initialized = 1;
  if (flowie_reply_queue_t_init(&endpoint->send_queue) != TURBO_OK) {
    flowie_endpoint_shutdown(endpoint);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&endpoint->send_queue_mutex);
  endpoint->send_queue_initialized = 1;
  {
    const size_t aggregate_hwm =
        endpoint->send_hwm_bytes != 0u &&
                endpoint->max_connections > SIZE_MAX / endpoint->send_hwm_bytes
            ? SIZE_MAX
            : endpoint->send_hwm_bytes * endpoint->max_connections;
    const tf_io_budget_config_t budget_config = {0u, aggregate_hwm, TF_IO_ADMISSION_FAIL, 0u};
    rc = tf_io_budget_init(&endpoint->send_budget, &budget_config);
    if (rc != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return rc;
    }
    endpoint->send_budget_initialized = 1;
  }
  rc = flowie_endpoint_identity_init(endpoint, name);
  if (rc == TURBO_OK) rc = flowie_endpoint_connection_init(endpoint);
  if (rc == TURBO_OK && execution->kind == TURBO_FLOW_CORONET_EXECUTION_PRIVATE) {
    coro_object_pool_config_t pool_config = CORO_OBJECT_POOL_CONFIG_DEFAULT;
    pool_config.max_capacity = endpoint->max_connections + FLOWIE_PRIVATE_COROUTINE_HEADROOM;
    if (pool_config.initial_capacity > pool_config.max_capacity)
      pool_config.initial_capacity = pool_config.max_capacity;
    pool_config.stack_size = config->coroutine_stack_size;
    rc = tf_coronet_execution_init_with_pool(&endpoint->execution, execution, &pool_config);
  } else if (rc == TURBO_OK) {
    rc = tf_coronet_execution_init(&endpoint->execution, execution);
  }
  if (rc != TURBO_OK) {
    flowie_endpoint_shutdown(endpoint);
    return rc;
  }
  endpoint->ctx = endpoint->execution.context;
  if (execution->kind == TURBO_FLOW_CORONET_EXECUTION_PRIVATE) {
    rc = coro_context_set_stream_recv_buffer_size(
        endpoint->ctx,
        config->recv_buffer_size ? config->recv_buffer_size : FLOWIE_DEFAULT_RECV_BUFFER_SIZE);
    if (rc != TURBO_OK) {
      flowie_endpoint_shutdown(endpoint);
      return rc;
    }
  }
  if (endpoint->manage_sessions) {
    endpoint->expiry_wait = coro_wait_create(endpoint->ctx);
    if (!endpoint->expiry_wait) {
      flowie_endpoint_shutdown(endpoint);
      return TURBO_ENOMEM;
    }
  }

  memset(&ops, 0, sizeof(ops));
  ops.start = flowie_endpoint_start;
  ops.consume = flowie_endpoint_consume;
  ops.stop = flowie_endpoint_stop;
  ops.shutdown = flowie_endpoint_shutdown;
  ops.connection_snapshot = flowie_connection_snapshot;
  ops.command = flowie_endpoint_command;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  schema.fields = FLOWIE_ENDPOINT_OPTION_FIELDS;
  schema.field_count =
      sizeof(FLOWIE_ENDPOINT_OPTION_FIELDS) / sizeof(FLOWIE_ENDPOINT_OPTION_FIELDS[0]);
  for (size_t i = 0u; i < 3u; ++i) {
    resources[i] =
        (turbo_flow_resource_provider_registration_t)TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
    resources[i].owner_name = endpoint->owner_name;
    resources[i].ops.metadata = flowie_resource_metadata;
    resources[i].ops.snapshot = flowie_resource_snapshot;
    resources[i].ops.document = flowie_resource_document;
    resources[i].ctx = &endpoint->resources[i];
    endpoint->resources[i].endpoint = endpoint;
  }
  endpoint->resources[0].kind = TURBO_FLOW_RESOURCE_CONNECTION;
  endpoint->resources[1].kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
  endpoint->resources[2].kind = TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE;
  resources[2].ops.command = flowie_resource_command;
  registration.module_name = FLOWIE_MQTT_SERVER_MODULE;
  registration.adapter_name = name;
  registration.ops = &ops;
  registration.ctx = endpoint;
  registration.schema = &schema;
  registration.operation_names = operation_names;
  registration.operation_count = 2u;
  registration.resources = resources;
  registration.resource_count = 3u;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) flowie_endpoint_shutdown(endpoint);
  return rc;
}

int flowie_register_endpoint_ex(turbo_flow_t *flow, const char *name,
                                const flowie_endpoint_config_t *config,
                                const turbo_flow_coronet_execution_binding_t *execution) {
  if (!config || config->context || config->take_context_ownership) return TURBO_EINVAL;
  return flowie_register_endpoint_internal(flow, name, config, execution, NULL, NULL);
}

int flowie_register_secure_endpoint_ex(turbo_flow_t *flow, const char *name,
                                       const flowie_endpoint_config_t *config,
                                       const turbo_flow_coronet_execution_binding_t *execution,
                                       const flowie_endpoint_security_binding_t *security) {
  if (!config || config->context || config->take_context_ownership) return TURBO_EINVAL;
  return flowie_register_endpoint_internal(flow, name, config, execution, security, NULL);
}

int flowie_register_bound_endpoint_ex(turbo_flow_t *flow, const char *name,
                                      const flowie_endpoint_config_t *config,
                                      const turbo_flow_coronet_execution_binding_t *execution,
                                      const flowie_endpoint_bindings_t *bindings) {
  if (!config || config->context || config->take_context_ownership || !bindings ||
      bindings->size < sizeof(*bindings) || (!bindings->security && !bindings->persistence))
    return TURBO_EINVAL;
  return flowie_register_endpoint_internal(flow, name, config, execution, bindings->security,
                                           bindings->persistence);
}

static int
flowie_register_endpoint_with_security(turbo_flow_t *flow, const char *name,
                                       const flowie_endpoint_config_t *config,
                                       const flowie_endpoint_security_binding_t *security) {
  turbo_flow_coronet_execution_binding_t execution;
  flowie_endpoint_config_t normalized;
  int rc = flowie_endpoint_config_validate(config);
  if (rc != TURBO_OK) return rc;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  if (config->context) {
    execution.kind = config->take_context_ownership ? TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT
                                                    : TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    execution.context = config->context;
  } else {
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  }
  normalized = *config;
  normalized.context = NULL;
  normalized.take_context_ownership = 0;
  return security
             ? flowie_register_secure_endpoint_ex(flow, name, &normalized, &execution, security)
             : flowie_register_endpoint_ex(flow, name, &normalized, &execution);
}

int flowie_register_endpoint(turbo_flow_t *flow, const char *name,
                             const flowie_endpoint_config_t *config) {
  return flowie_register_endpoint_with_security(flow, name, config, NULL);
}

int flowie_register_secure_endpoint(turbo_flow_t *flow, const char *name,
                                    const flowie_endpoint_config_t *config,
                                    const flowie_endpoint_security_binding_t *security) {
  return flowie_register_endpoint_with_security(flow, name, config, security);
}

int flowie_register_bound_endpoint(turbo_flow_t *flow, const char *name,
                                   const flowie_endpoint_config_t *config,
                                   const flowie_endpoint_bindings_t *bindings) {
  turbo_flow_coronet_execution_binding_t execution;
  flowie_endpoint_config_t normalized;
  int rc = flowie_endpoint_config_validate(config);
  if (rc != TURBO_OK) return rc;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  if (config->context) {
    execution.kind = config->take_context_ownership ? TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT
                                                    : TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    execution.context = config->context;
  } else {
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  }
  normalized = *config;
  normalized.context = NULL;
  normalized.take_context_ownership = 0;
  return flowie_register_bound_endpoint_ex(flow, name, &normalized, &execution, bindings);
}
