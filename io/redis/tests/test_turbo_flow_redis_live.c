#include "flowie_record_store_contract.h"
#include "flowie_record_store_endurance.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_fmq_broker.h"
#include "turbo_flow_redis.h"
#include "turbo_flow_store_redis.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include "redis_storage_test_helpers.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#ifdef TURBO_FLOW_REDIS_HAS_FLOWIE
  #include "flowie.h"
  #include "flowie_test_socket.h"
#endif

#define REDIS_LIVE_WAIT_ITERATIONS 400
#define REDIS_LIVE_STOP_LIMIT_NS UINT64_C(500000000)
#define REDIS_LIVE_YAML_CAPACITY 4096u

typedef struct redis_live_capture_s {
  atomic_int called;
  atomic_int result;
  unsigned char payload[256];
  size_t payload_len;
} redis_live_capture_t;

typedef struct redis_live_protocol_owner_s {
  int called;
  turbo_flow_protocol_settlement_request_t request;
} redis_live_protocol_owner_t;

typedef struct redis_live_record_capture_s {
  uint8_t keys[4][16];
  size_t key_sizes[4];
  uint8_t values[4][32];
  size_t value_sizes[4];
  uint64_t revisions[4];
  size_t count;
} redis_live_record_capture_t;

typedef struct redis_live_member_capture_s {
  uint8_t members[4][16];
  size_t sizes[4];
  size_t count;
} redis_live_member_capture_t;

static int redis_live_member_visit(void *ctx, turbo_flow_store_bytes_t member) {
  redis_live_member_capture_t *capture = (redis_live_member_capture_t *)ctx;
  if (!capture || !member.data || member.size == 0u || member.size > sizeof(capture->members[0]) ||
      capture->count >= 4u) {
    return TURBO_EPROTO;
  }
  memcpy(capture->members[capture->count], member.data, member.size);
  capture->sizes[capture->count] = member.size;
  capture->count++;
  return TURBO_OK;
}

static int redis_live_record_visit(void *ctx, const turbo_flow_record_view_t *record) {
  redis_live_record_capture_t *capture = (redis_live_record_capture_t *)ctx;
  size_t index;
  if (!capture || !record || record->size < sizeof(*record) || !record->key ||
      record->key_size == 0u || record->key_size > sizeof(capture->keys[0]) ||
      record->value_size > sizeof(capture->values[0]) || record->revision == 0u ||
      capture->count >= 4u)
    return TURBO_EPROTO;
  index = capture->count++;
  memcpy(capture->keys[index], record->key, record->key_size);
  capture->key_sizes[index] = record->key_size;
  if (record->value_size != 0u) memcpy(capture->values[index], record->value, record->value_size);
  capture->value_sizes[index] = record->value_size;
  capture->revisions[index] = record->revision;
  return TURBO_OK;
}

static size_t redis_live_record_find(const redis_live_record_capture_t *capture, const uint8_t *key,
                                     size_t key_size) {
  if (!capture || !key || key_size == 0u) return SIZE_MAX;
  for (size_t i = 0u; i < capture->count; ++i) {
    if (capture->key_sizes[i] == key_size && memcmp(capture->keys[i], key, key_size) == 0) return i;
  }
  return SIZE_MAX;
}

static int redis_live_protocol_settle(void *ctx, const turbo_flow_protocol_route_t *route,
                                      const turbo_flow_protocol_settlement_request_t *request) {
  redis_live_protocol_owner_t *owner = (redis_live_protocol_owner_t *)ctx;
  if (!owner || !route || !request) return TURBO_EINVAL;
  owner->called += 1;
  owner->request = *request;
  return TURBO_OK;
}

static int redis_live_capture(turbo_flow_msg_t *msg, void *ctx) {
  redis_live_capture_t *capture = (redis_live_capture_t *)ctx;
  if (!capture || !msg || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return atomic_load_explicit(&capture->result, memory_order_acquire);
}

static int redis_live_publish(turbo_flow_t *flow, const void *payload, size_t payload_len) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(payload, payload_len);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int redis_live_wait_called(const redis_live_capture_t *capture, int expected) {
  for (int i = 0; i < REDIS_LIVE_WAIT_ITERATIONS; ++i) {
    if (atomic_load_explicit(&capture->called, memory_order_acquire) >= expected) return TURBO_OK;
    turbo_sleep_ms(5);
  }
  return TURBO_ETIMEDOUT;
}

static turbo_flow_redis_stream_config_t redis_live_stream_config(const char *stream) {
  turbo_flow_redis_stream_config_t config;
  memset(&config, 0, sizeof(config));
  config.host = "127.0.0.1";
  config.port = 6379;
  config.timeout_ms = 5000;
  config.stream = stream;
  config.field = "payload";
  config.maxlen = 1u;
  return config;
}

static turbo_flow_protocol_route_t redis_live_route(uint64_t owner, uint64_t session) {
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  route.protocol = TURBO_FLOW_PROTOCOL_FMQ;
  route.owner_instance_id = owner;
  route.session_id = session;
  route.session_generation = 1u;
  return route;
}

static turbo_flow_fmq_broker_logical_address_t
redis_live_address(const char *broker_id, const char *client_id, uint64_t request_id) {
  turbo_flow_fmq_broker_logical_address_t address = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
  memcpy(address.origin_broker_id, broker_id, strlen(broker_id));
  memcpy(address.client_id, client_id, strlen(client_id));
  address.request_id = request_id;
  return address;
}

#ifdef TURBO_FLOW_REDIS_HAS_FLOWIE
static int redis_live_encode_connect(uint8_t *output, size_t capacity, size_t *written,
                                     const char *client_id, uint32_t session_expiry,
                                     const char *will_topic, const char *will_payload,
                                     uint32_t will_delay) {
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  uint8_t properties[5];
  uint8_t will_properties[5];
  if (!output || !written || !client_id) return TURBO_EINVAL;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.keep_alive = 60u;
  connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
  if (session_expiry != 0u) {
    properties[0] = FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL;
    properties[1] = (uint8_t)(session_expiry >> 24u);
    properties[2] = (uint8_t)(session_expiry >> 16u);
    properties[3] = (uint8_t)(session_expiry >> 8u);
    properties[4] = (uint8_t)session_expiry;
    connect.properties = (flowie_mqtt_span_t){properties, sizeof(properties)};
  }
  if (will_topic) {
    connect.has_will = 1u;
    connect.will_topic = (flowie_mqtt_span_t){(const uint8_t *)will_topic, strlen(will_topic)};
    connect.will_payload = (flowie_mqtt_span_t){(const uint8_t *)will_payload,
                                                will_payload ? strlen(will_payload) : 0u};
    if (will_delay != 0u) {
      will_properties[0] = FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL;
      will_properties[1] = (uint8_t)(will_delay >> 24u);
      will_properties[2] = (uint8_t)(will_delay >> 16u);
      will_properties[3] = (uint8_t)(will_delay >> 8u);
      will_properties[4] = (uint8_t)will_delay;
      connect.will_properties = (flowie_mqtt_span_t){will_properties, sizeof(will_properties)};
    }
  }
  return flowie_mqtt_connect_packet_encode(&connect, output, capacity, written) ==
                 FLOWIE_MQTT_PARSE_OK
             ? TURBO_OK
             : TURBO_EPROTO;
}

static int redis_live_start_bound_endpoint(const turbo_flow_resolved_config_t *resolved,
                                           const flowie_endpoint_bindings_t *bindings,
                                           const char *graph, turbo_flow_t **flow_out) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_t *flow;
  int rc;
  if (flow_out) *flow_out = NULL;
  if (!resolved || !bindings || !graph || !flow_out) return TURBO_EINVAL;
  flow = turbo_flow_create();
  if (!flow) return TURBO_ENOMEM;
  rc = flowie_register_resolved_bound_endpoint(flow, "mqtt.endpoint", resolved, bindings, &error);
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(flow, graph, strlen(graph));
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) {
    turbo_flow_destroy(flow);
    return rc;
  }
  *flow_out = flow;
  return TURBO_OK;
}
#endif

spec("turbo_flow_redis_live") {
  it("uses Redis Sets as the bounded binary-safe IndexStore fact source") {
    static const uint8_t online_name[] = {'o', 0u, 'n'};
    static const uint8_t room_name[] = {'r', '1'};
    static const uint8_t first_member[] = {'d', 0u, '1'};
    static const uint8_t second_member[] = {'d', 0u, '2'};
    static const uint8_t third_member[] = {'d', 0u, '3'};
    turbo_flow_redis_index_store_config_t config;
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    turbo_flow_index_store_t *store = NULL;
    turbo_flow_store_stats_t stats = TURBO_FLOW_STORE_STATS_INIT;
    redis_live_member_capture_t capture;
    turbo_flow_store_bytes_t online = {online_name, sizeof(online_name)};
    turbo_flow_store_bytes_t room = {room_name, sizeof(room_name)};
    turbo_flow_store_bytes_t first = {first_member, sizeof(first_member)};
    turbo_flow_store_bytes_t second = {second_member, sizeof(second_member)};
    turbo_flow_store_bytes_t third = {third_member, sizeof(third_member)};
    turbo_flow_store_bytes_t indices[] = {online, room};
    size_t count = 0u;
    int present = 0;
    char redis_key[128];

    (void)snprintf(redis_key, sizeof(redis_key), "turboflow:live:index:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = redis_key;
    config.max_index_name_size = 8u;
    config.max_member_size = 8u;
    limits.max_records = 3u;
    limits.max_bytes = 32u;
    limits.max_item_bytes = 16u;

    check_int_eq(redis_test_index_store_open(&config, &limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, online, first), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, online, second), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, room, second), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, online, first), TURBO_EALREADY);
    check_int_eq(turbo_flow_index_store_add(store, room, third), TURBO_ENOSPC);
    check_int_eq(turbo_flow_index_store_contains(store, room, second, &present), TURBO_OK);
    check_int_eq(present, 1);
    check_int_eq(turbo_flow_index_store_count(store, online, &count), TURBO_OK);
    check_size_eq(count, 2u);
    check_int_eq(turbo_flow_index_store_intersection_count(store, indices, 2u, &count), TURBO_OK);
    check_size_eq(count, 1u);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(turbo_flow_index_store_visit(store, online, redis_live_member_visit, &capture),
                 TURBO_OK);
    check_size_eq(capture.count, 2u);
    check_int_eq(turbo_flow_index_store_stats(store, &stats), TURBO_OK);
    check_size_eq(stats.records, 3u);
    check_size_eq(stats.bytes, sizeof(online_name) + sizeof(room_name) + sizeof(first_member) +
                                   sizeof(second_member) * 2u);
    check_int_eq(turbo_flow_index_store_remove(store, online, first), TURBO_OK);
    check_int_eq(turbo_flow_index_store_remove(store, online, second), TURBO_OK);
    check_int_eq(turbo_flow_index_store_remove(store, room, second), TURBO_OK);
    check_int_eq(turbo_flow_index_store_close(store), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, room, third), TURBO_ESHUTDOWN);
    check_int_eq(redis_test_storage_destroy(store), TURBO_OK);
  }

  it("uses Redis Stream as the bounded cursor-ordered LogStore fact source") {
    static const uint8_t first_payload[] = {'a'};
    static const uint8_t second_payload[] = {'b', 0u};
    static const uint8_t third_payload[] = {'c', 'c', 'c'};
    turbo_flow_redis_log_store_config_t config;
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    turbo_flow_log_store_t *store = NULL;
    turbo_flow_log_record_t records[2] = {TURBO_FLOW_LOG_RECORD_INIT, TURBO_FLOW_LOG_RECORD_INIT};
    turbo_flow_store_stats_t stats = TURBO_FLOW_STORE_STATS_INIT;
    turbo_flow_store_bytes_t first = {first_payload, sizeof(first_payload)};
    turbo_flow_store_bytes_t second = {second_payload, sizeof(second_payload)};
    turbo_flow_store_bytes_t third = {third_payload, sizeof(third_payload)};
    turbo_flow_store_bytes_t empty = TURBO_FLOW_STORE_BYTES_INIT;
    size_t count = 0u;
    size_t trimmed = 0u;
    uint64_t cursor = 0u;
    uint64_t head = 0u;
    uint64_t tail = 0u;
    char redis_key[128];

    (void)snprintf(redis_key, sizeof(redis_key), "turboflow:live:log:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = redis_key;
    config.max_operation_records = 2u;
    limits.max_records = 2u;
    limits.max_bytes = 8u;
    limits.max_item_bytes = 8u;
    limits.full_policy = TURBO_FLOW_STORE_FULL_TRIM_OLDEST;

    check_int_eq(redis_test_log_store_open(&config, &limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 10u, first, &cursor), TURBO_OK);
    check_uint_eq(cursor, 1u);
    check_int_eq(turbo_flow_log_store_append(store, 20u, second, &cursor), TURBO_OK);
    check_uint_eq(cursor, 2u);
    check_int_eq(turbo_flow_log_store_append(store, 30u, third, &cursor), TURBO_OK);
    check_uint_eq(cursor, 3u);
    check_int_eq(turbo_flow_log_store_bounds(store, &head, &tail), TURBO_OK);
    check_uint_eq(head, 2u);
    check_uint_eq(tail, 3u);
    check_int_eq(turbo_flow_log_store_read(store, 1u, records, 2u, &count), TURBO_ERANGE);
    check_size_eq(count, 0u);
    check_int_eq(turbo_flow_log_store_read(store, 0u, records, 2u, &count), TURBO_OK);
    check_size_eq(count, 2u);
    check_uint_eq(records[0].cursor, 2u);
    check_uint_eq(records[0].timestamp_ms, 20u);
    check_mem_eq(mem_buffer_const_data(records[0].payload), second_payload, sizeof(second_payload));
    check_uint_eq(records[1].cursor, 3u);
    check_uint_eq(records[1].timestamp_ms, 30u);
    check_mem_eq(mem_buffer_const_data(records[1].payload), third_payload, sizeof(third_payload));
    turbo_flow_log_record_cleanup(&records[0]);
    turbo_flow_log_record_cleanup(&records[1]);

    check_int_eq(turbo_flow_log_store_trim_before_time(store, 30u, &trimmed), TURBO_OK);
    check_size_eq(trimmed, 1u);
    check_int_eq(turbo_flow_log_store_bounds(store, &head, &tail), TURBO_OK);
    check_uint_eq(head, 3u);
    check_uint_eq(tail, 3u);
    check_int_eq(turbo_flow_log_store_stats(store, &stats), TURBO_OK);
    check_size_eq(stats.records, 1u);
    check_size_eq(stats.bytes, sizeof(third_payload));
    check_size_eq(stats.trims, 2u);
    check_int_eq(turbo_flow_log_store_append(store, 29u, first, &cursor), TURBO_ERANGE);
    check_int_eq(turbo_flow_log_store_trim_before_cursor(store, 4u, &trimmed), TURBO_OK);
    check_size_eq(trimmed, 1u);
    check_int_eq(turbo_flow_log_store_bounds(store, &head, &tail), TURBO_OK);
    check_uint_eq(head, 4u);
    check_uint_eq(tail, 0u);
    check_int_eq(turbo_flow_log_store_close(store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 40u, first, &cursor), TURBO_ESHUTDOWN);
    check_int_eq(redis_test_storage_destroy(store), TURBO_OK);

    store = NULL;
    check_int_eq(redis_test_log_store_open(&config, &limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 40u, empty, &cursor), TURBO_OK);
    check_uint_eq(cursor, 4u);
    check_int_eq(turbo_flow_log_store_read(store, 4u, records, 1u, &count), TURBO_OK);
    check_size_eq(count, 1u);
    check_uint_eq(records[0].cursor, 4u);
    check_uint_eq(records[0].timestamp_ms, 40u);
    check_null(records[0].payload);
    turbo_flow_log_record_cleanup(&records[0]);
    check_int_eq(redis_test_storage_destroy(store), TURBO_OK);

    store = NULL;
    limits.retention_ms = 1u;
    check_int_eq(redis_test_log_store_open(&config, &limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 41u, empty, &cursor), TURBO_EBUSY);
    check_int_eq(redis_test_storage_destroy(store), TURBO_OK);
  }

  it("keeps a rejected append atomic and applies retention before FULL_REJECT admission") {
    static const uint8_t payload[] = {'x'};
    turbo_flow_redis_log_store_config_t config;
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    turbo_flow_log_store_t *store = NULL;
    turbo_flow_store_stats_t stats = TURBO_FLOW_STORE_STATS_INIT;
    turbo_flow_store_bytes_t value = {payload, sizeof(payload)};
    uint64_t cursor = 0u;
    uint64_t head = 0u;
    uint64_t tail = 0u;
    char redis_key[128];

    (void)snprintf(redis_key, sizeof(redis_key), "turboflow:live:log-reject:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = redis_key;
    config.max_operation_records = 2u;
    limits.max_records = 2u;
    limits.max_bytes = 2u;
    limits.max_item_bytes = 1u;
    limits.retention_ms = 15u;
    limits.full_policy = TURBO_FLOW_STORE_FULL_REJECT;

    check_int_eq(redis_test_log_store_open(&config, &limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 10u, value, &cursor), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 20u, value, &cursor), TURBO_OK);
    check_uint_eq(cursor, 2u);
    check_int_eq(turbo_flow_log_store_append(store, 25u, value, &cursor), TURBO_ENOSPC);
    check_uint_eq(cursor, 2u);
    check_int_eq(turbo_flow_log_store_bounds(store, &head, &tail), TURBO_OK);
    check_uint_eq(head, 1u);
    check_uint_eq(tail, 2u);
    check_int_eq(turbo_flow_log_store_append(store, 26u, value, &cursor), TURBO_OK);
    check_uint_eq(cursor, 3u);
    check_int_eq(turbo_flow_log_store_bounds(store, &head, &tail), TURBO_OK);
    check_uint_eq(head, 2u);
    check_uint_eq(tail, 3u);
    check_int_eq(turbo_flow_log_store_stats(store, &stats), TURBO_OK);
    check_size_eq(stats.records, 2u);
    check_size_eq(stats.bytes, 2u);
    check_size_eq(stats.rejects, 1u);
    check_size_eq(stats.trims, 1u);
    check_int_eq(redis_test_storage_destroy(store), TURBO_OK);
  }

  it("uses Redis Hash as the bounded revisioned StateStore fact source") {
    static const uint8_t record_key[] = {'d', 0u, '1'};
    static const uint8_t first_value[] = {'o', 'n'};
    static const uint8_t second_value[] = {'o', 'f', 'f'};
    turbo_flow_redis_record_store_config_t config;
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    turbo_flow_state_store_t *store = NULL;
    turbo_flow_state_record_t record = TURBO_FLOW_STATE_RECORD_INIT;
    turbo_flow_store_stats_t stats = TURBO_FLOW_STORE_STATS_INIT;
    turbo_flow_store_bytes_t key = {record_key, sizeof(record_key)};
    turbo_flow_store_bytes_t first = {first_value, sizeof(first_value)};
    turbo_flow_store_bytes_t second = {second_value, sizeof(second_value)};
    uint64_t revision = 0u;
    char redis_key[128];

    (void)snprintf(redis_key, sizeof(redis_key), "turboflow:live:state:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = redis_key;
    config.max_record_key_size = 8u;
    config.max_value_size = 8u;
    config.max_batch_size = 1u;
    config.max_records = 2u;
    limits.max_records = 2u;
    limits.max_bytes = 16u;
    limits.max_item_bytes = 16u;

    check_int_eq(redis_test_state_store_open(&config, &limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_state_store_put(store, key, first, 0u, &revision), TURBO_OK);
    check_uint_eq(revision, 1u);
    check_int_eq(turbo_flow_state_store_get(store, key, &record), TURBO_OK);
    check_uint_eq(record.revision, 1u);
    check_mem_eq(mem_buffer_const_data(record.value), first_value, sizeof(first_value));
    turbo_flow_state_record_cleanup(&record);
    check_int_eq(turbo_flow_state_store_put(store, key, second, 7u, &revision), TURBO_EBUSY);
    check_int_eq(turbo_flow_state_store_put(store, key, second, 1u, &revision), TURBO_OK);
    check_uint_eq(revision, 2u);
    check_int_eq(turbo_flow_state_store_stats(store, &stats), TURBO_OK);
    check_size_eq(stats.records, 1u);
    check_size_eq(stats.bytes, sizeof(record_key) + sizeof(second_value));
    check_int_eq(turbo_flow_state_store_remove(store, key, 2u), TURBO_OK);
    check_int_eq(turbo_flow_state_store_get(store, key, &record), TURBO_ENOENT);
    check_int_eq(turbo_flow_state_store_close(store), TURBO_OK);
    check_int_eq(turbo_flow_state_store_put(store, key, first, 0u, &revision), TURBO_ESHUTDOWN);
    check_int_eq(redis_test_storage_destroy(store), TURBO_OK);
  }

  it("MQTT-STORE-010 runs the provider-neutral record trace through Redis") {
    turbo_flow_redis_record_store_config_t config;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    flowie_record_store_contract_result_t result = {0};
    char key[128];
    (void)snprintf(key, sizeof(key), "turboflow:live:provider-neutral:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = key;
    config.max_record_key_size = 64u;
    config.max_value_size = 64u;
    config.max_batch_size = 2u;
    config.max_records = 4u;
    check_int_eq(redis_test_record_store_open(&config, &store), TURBO_OK);
    check_int_eq(flowie_record_store_contract_run(&store, &result), TURBO_OK);
    check_size_eq(result.restored_count, 1u);
    check_uint_eq(result.restored_revision, 2u);
    check_int_eq(result.duplicate_create_status, TURBO_EBUSY);
    check_int_eq(result.stale_update_status, TURBO_EBUSY);
    check_int_eq(result.stale_delete_status, TURBO_EBUSY);
    check_true(result.empty_after_delete);
    check_size_eq(result.mqtt_wire_size, 12u);
    check_true(result.mqtt_wire_equal);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);
  }

  it("MQTT-STORE-ENDURANCE-001 runs the shared revision trace through Redis") {
    turbo_flow_redis_record_store_config_t config;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    flowie_record_store_endurance_result_t result = {0};
    char key[128];
    (void)snprintf(key, sizeof(key), "turboflow:live:record-endurance:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = key;
    config.max_record_key_size = FLOWIE_RECORD_ENDURANCE_KEY_SIZE;
    config.max_value_size = FLOWIE_RECORD_ENDURANCE_VALUE_SIZE;
    config.max_batch_size = FLOWIE_RECORD_ENDURANCE_BATCH_SIZE;
    config.max_records = FLOWIE_RECORD_ENDURANCE_RECORDS;
    check_int_eq(redis_test_record_store_open(&config, &store), TURBO_OK);
    check_int_eq(flowie_record_store_endurance_run(&store, &result), TURBO_OK);
    check_size_eq(result.successful_commits, 40u);
    check_size_eq(result.scans, 39u);
    check_size_eq(result.conflicts, 4u);
    check_size_eq(result.final_count, 0u);
    check_true(result.durable);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);
  }

  it("MQTT-SOAK-005 MQTT-STORE-007 resolves Redis timeout and lost commit replies by revision") {
    static const uint8_t unavailable_key[] = {'u'};
    static const uint8_t unavailable_value[] = {'v'};
    turbo_flow_redis_record_store_config_t config;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
    flowie_record_store_recovery_result_t recovery = {0};
    char key[128];
    (void)snprintf(key, sizeof(key), "turboflow:live:recovery:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 1u;
    config.database = 0;
    config.timeout_ms = 100u;
    config.key = key;
    config.max_record_key_size = 16u;
    config.max_value_size = 16u;
    config.max_batch_size = 1u;
    config.max_records = 2u;
    mutation.kind = TURBO_FLOW_RECORD_PUT;
    mutation.key = unavailable_key;
    mutation.key_size = sizeof(unavailable_key);
    mutation.next_revision = 1u;
    mutation.value = unavailable_value;
    mutation.value_size = sizeof(unavailable_value);
    check_int_eq(redis_test_record_store_open(&config, &store), TURBO_OK);
    check_int_ne(store.commit(store.ctx, &mutation, 1u), TURBO_OK);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);

    config.port = 6379u;
    config.timeout_ms = 5000u;
    check_int_eq(redis_test_record_store_open(&config, &store), TURBO_OK);
    check_int_eq(flowie_record_store_recovery_contract_run(&store, &recovery), TURBO_OK);
    check_int_eq(recovery.timeout_status, TURBO_ETIMEDOUT);
    check_int_eq(recovery.lost_reply_status, TURBO_EIO);
    check_int_eq(recovery.retry_status, TURBO_EBUSY);
    check_uint_eq(recovery.revision_after_timeout, 1u);
    check_uint_eq(recovery.revision_after_lost_reply, 2u);
    check_uint_eq(recovery.revision_after_recovery, 3u);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);
  }

  it("uses a replied XADD as one exact routed DURABLE boundary and replays the payload") {
    static const char sink_dsl[] = "source input\n"
                                   "stage append adapter redis.out\n"
                                   "stage main {\n"
                                   "  input -> append\n"
                                   "}\n";
    static const uint8_t payload[] = {'d', 'u', 'r', 'a', 'b', 'l', 'e'};
    char stream[128];
    char group[128];
    char consumer[128];
    turbo_flow_redis_stream_config_t config;
    turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_protocol_route_owner_ops_t owner_ops = TURBO_FLOW_PROTOCOL_ROUTE_OWNER_OPS_INIT;
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_protocol_settlement_envelope_t envelope =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT;
    turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
    turbo_flow_redis_stream_owner_t *stream_owner = NULL;
    redis_live_protocol_owner_t protocol_owner = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t suffix = turbo_hrtime();
    (void)snprintf(stream, sizeof(stream), "turboflow:live:durable:%llu",
                   (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-live-durable-group-%llu",
                   (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-live-durable-consumer-%llu",
                   (unsigned long long)suffix);
    config = redis_live_stream_config(stream);
    owner_ops.settle = redis_live_protocol_settle;
    route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    route.owner_instance_id = 91u;
    route.session_id = 92u;
    route.session_generation = 93u;
    envelope.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    envelope.message.protocol_version = TURBO_FLOW_MQTT_PROTOCOL_5_0;
    envelope.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
    envelope.message.qos = TURBO_FLOW_PROTOCOL_QOS_1;
    envelope.message.packet_id = 94u;
    envelope.message.session_generation = route.session_generation;
    envelope.requested_point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    check_not_null(flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.out", &config), TURBO_OK);
    check_int_eq(turbo_flow_register_protocol_route_owner(
                     flow, route.protocol, route.owner_instance_id, &owner_ops, &protocol_owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, sink_dsl, sizeof(sink_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len(payload, sizeof(payload));
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_msg_set_protocol_settlement(&msg, &envelope), TURBO_OK);
    check_int_eq(turbo_flow_publish_ex(flow, "input", &msg, &result), TURBO_OK);
    check_uint_eq(result.protocol_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(protocol_owner.called, 1);
    check_uint_eq(protocol_owner.request.point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    config.group = group;
    config.consumer = consumer;
    config.group_start_id = "0-0";
    config.read_count = 1u;
    config.block_ms = 100u;
    config.create_group = 1;
    check_int_eq(turbo_flow_redis_stream_owner_create(&config, &stream_owner), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(stream_owner, &claim), TURBO_OK);
    check_mem_eq(claim.payload.data, payload, sizeof(payload));
    check_int_eq(turbo_flow_redis_stream_owner_ack(stream_owner, claim.token), TURBO_OK);
    turbo_flow_redis_stream_owner_destroy(stream_owner);
  }

#ifdef TURBO_FLOW_REDIS_HAS_FLOWIE
  it("ACKs Flowie DURABLE only after the YAML-configured Redis Stream commit") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage persist adapter redis.mqtt\n"
                                "stage main {\n"
                                "  mqtt_in -> persist\n"
                                "}\n";
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'd',   'u',   'r'};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x45u, 0x00u, 'z'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x45u};
    char yaml[REDIS_LIVE_YAML_CAPACITY];
    char stream[128];
    char group[128];
    char consumer[128];
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    uint64_t suffix = turbo_hrtime();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_redis_stream_config_t replay_config;
    turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_redis_stream_owner_t *stream_owner = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    int yaml_size;
    check_int_gt(port, 0);
    check_not_null(flow);
    (void)snprintf(stream, sizeof(stream), "turboflow:live:flowie:%llu",
                   (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-live-flowie-group-%llu",
                   (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-live-flowie-consumer-%llu",
                   (unsigned long long)suffix);
    yaml_size = snprintf(yaml, sizeof(yaml),
                         "version: 1\n"
                         "fragments:\n"
                         "  connection:\n"
                         "    local-redis:\n"
                         "      host: 127.0.0.1\n"
                         "      port: 6379\n"
                         "      database: 0\n"
                         "      timeout_ms: 5000\n"
                         "adapters:\n"
                         "  mqtt.endpoint:\n"
                         "    kind: flowie_endpoint\n"
                         "    config:\n"
                         "      transport: tcp\n"
                         "      host: 127.0.0.1\n"
                         "      port: %hu\n"
                         "      max_connections: 4\n"
                         "      manage_sessions: true\n"
                         "      settlement_qos1: durable\n"
                         "      settlement_qos2: durable\n"
                         "      max_sessions: 4\n"
                         "      max_subscriptions_per_session: 8\n"
                         "      max_inflight_per_session: 8\n"
                         "  redis.mqtt:\n"
                         "    kind: redis\n"
                         "    fragments:\n"
                         "      connection: local-redis\n"
                         "    config:\n"
                         "      pattern: stream\n"
                         "      role: sink\n"
                         "      stream: %s\n"
                         "      field: payload\n"
                         "      maxlen: 1000\n",
                         port, stream);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_register_resolved_adapter(flow, "redis.mqtt", resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 0u, 8u, FLOWIE_DEFAULT_MAX_PACKET_SIZE),
                 TURBO_OK);
    check_int_eq(flowie_test_send(client, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);

    replay_config = redis_live_stream_config(stream);
    replay_config.group = group;
    replay_config.consumer = consumer;
    replay_config.group_start_id = "0-0";
    replay_config.read_count = 1u;
    replay_config.block_ms = 100u;
    replay_config.create_group = 1;
    check_int_eq(turbo_flow_redis_stream_owner_create(&replay_config, &stream_owner), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(stream_owner, &claim), TURBO_OK);
    check_mem_eq(claim.payload.data, publish, sizeof(publish));
    check_int_eq(turbo_flow_redis_stream_owner_ack(stream_owner, claim.token), TURBO_OK);
    turbo_flow_redis_stream_owner_destroy(stream_owner);
  }

  it("restores a pending MQTT Will through Redis and publishes after the remaining delay") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage mqtt_fanout adapter mqtt.endpoint\n"
                                "stage main {\n"
                                "  mqtt_in -> mqtt_fanout\n"
                                "}\n";
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, 'w',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t expected_will[] = {0x30u, 0x05u, 0x00u, 0x01u, 'w', 0x00u, 'x'};
    char key[128];
    char yaml[REDIS_LIVE_YAML_CAPACITY];
    uint8_t subscriber_connect[128];
    uint8_t will_connect[128];
    uint8_t received[32];
    size_t subscriber_connect_size = 0u;
    size_t will_connect_size = 0u;
    unsigned short port = flowie_test_port();
    uint64_t suffix = turbo_hrtime();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    turbo_flow_connection_snapshot_t connections = {0};
    flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t will_client = FLOWIE_TEST_INVALID_SOCKET;
    turbo_flow_t *flow = NULL;
    int will_readable = 0;
    int yaml_size;
    check_int_gt(port, 0);
    (void)snprintf(key, sizeof(key), "turboflow:live:flowie:will:%llu", (unsigned long long)suffix);
    check_int_eq(redis_live_encode_connect(subscriber_connect, sizeof(subscriber_connect),
                                           &subscriber_connect_size, "rsu", 60u, NULL, NULL, 0u),
                 TURBO_OK);
    check_int_eq(redis_live_encode_connect(will_connect, sizeof(will_connect), &will_connect_size,
                                           "rwi", 5u, "w", "x", 2u),
                 TURBO_OK);
    yaml_size = snprintf(yaml, sizeof(yaml),
                         "version: 1\n"
                         "channels:\n"
                         "  mqtt.sessions:\n"
                         "    kind: record_store\n"
                         "    config:\n"
                         "      backend: redis\n"
                         "      host: 127.0.0.1\n"
                         "      port: 6379\n"
                         "      database: 0\n"
                         "      timeout_ms: 5000\n"
                         "      key: '%s'\n"
                         "      max_key_size: 65538\n"
                         "      max_value_size: 1048576\n"
                         "      max_batch_size: 16\n"
                         "      max_records: 8\n"
                         "adapters:\n"
                         "  mqtt.endpoint:\n"
                         "    kind: flowie_endpoint\n"
                         "    config:\n"
                         "      transport: tcp\n"
                         "      host: 127.0.0.1\n"
                         "      port: %hu\n"
                         "      max_packet_size: 4096\n"
                         "      max_connections: 4\n"
                         "      manage_sessions: true\n"
                         "      max_sessions: 4\n"
                         "      max_retained_messages: 4\n"
                         "      max_subscriptions_per_session: 8\n"
                         "      max_inflight_per_session: 8\n"
                         "      session_store: mqtt.sessions\n",
                         key, port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    persistence.store_channel = "mqtt.sessions";
    persistence.store = &store;
    bindings.persistence = &persistence;

    check_int_eq(
        redis_test_record_store_open_resolved_ex(resolved, "mqtt.sessions", &store, &error),
        TURBO_OK);
    check_int_eq(redis_live_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    will_client = flowie_test_connect(port);
    check_true(will_client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(will_client, will_connect, will_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(will_client, 0u, 8u, 4096u), TURBO_OK);
    flowie_test_socket_close(will_client);
    will_client = FLOWIE_TEST_INVALID_SOCKET;
    for (size_t i = 0u; i < REDIS_LIVE_WAIT_ITERATIONS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connections), TURBO_OK);
      if (connections.connections_current == 1u) break;
      turbo_sleep_ms(5u);
    }
    check_size_eq(connections.connections_current, 1u);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    flowie_test_socket_close(subscriber);
    subscriber = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);

    check_int_eq(
        redis_test_record_store_open_resolved_ex(resolved, "mqtt.sessions", &store, &error),
        TURBO_OK);
    check_int_eq(redis_live_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 1u, 8u, 4096u), TURBO_OK);
    for (size_t i = 0u; i < REDIS_LIVE_WAIT_ITERATIONS; ++i) {
      if (flowie_test_socket_readable(subscriber, 10u)) {
        will_readable = 1;
        break;
      }
    }
    check_true(will_readable);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(expected_will)), TURBO_OK);
    check_mem_eq(received, expected_will, sizeof(expected_will));
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
  }
#endif

  it("round trips binary-safe Redis Data through a real server") {
    static const char set_dsl[] = "source input\n"
                                  "stage store adapter redis.data\n"
                                  "stage main {\n"
                                  "  input -> store\n"
                                  "}\n";
    static const char get_dsl[] = "source input\n"
                                  "stage load adapter redis.data\n"
                                  "stage capture\n"
                                  "stage main {\n"
                                  "  input -> load -> capture\n"
                                  "}\n";
    static const unsigned char payload[] = {'f', 'l', 'o', 'w', 0, 'd', 'a', 't', 'a'};
    char key[128];
    turbo_flow_redis_data_config_t config;
    redis_live_capture_t capture;
    turbo_flow_t *set_flow;
    turbo_flow_t *get_flow;
    (void)snprintf(key, sizeof(key), "turboflow:live:data:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379;
    config.timeout_ms = 5000;
    config.key = key;
    config.max_value_size = sizeof(payload);
    config.operation = TURBO_FLOW_REDIS_DATA_SET;
    set_flow = turbo_flow_create();
    check_not_null(set_flow);
    check_int_eq(turbo_flow_redis_register_data_adapter(set_flow, "redis.data", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(set_flow, set_dsl, sizeof(set_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(set_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(set_flow), TURBO_OK);
    check_int_eq(redis_live_publish(set_flow, payload, sizeof(payload)), TURBO_OK);
    check_int_eq(turbo_flow_stop(set_flow), TURBO_OK);
    turbo_flow_destroy(set_flow);

    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    atomic_init(&capture.result, TURBO_OK);
    config.operation = TURBO_FLOW_REDIS_DATA_GET;
    get_flow = turbo_flow_create();
    check_not_null(get_flow);
    check_int_eq(turbo_flow_redis_register_data_adapter(get_flow, "redis.data", &config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(get_flow, "capture", redis_live_capture, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(get_flow, get_dsl, sizeof(get_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(get_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(get_flow), TURBO_OK);
    check_int_eq(redis_live_publish(get_flow, "trigger", 7u), TURBO_OK);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, sizeof(payload));
    check_mem_eq(capture.payload, payload, sizeof(payload));
    check_int_eq(turbo_flow_stop(get_flow), TURBO_OK);
    turbo_flow_destroy(get_flow);
  }

  it("restores an atomic blob snapshot and never falls back when Redis is unavailable") {
    static const uint8_t first[] = {'T', 'F', 'M', 'S', 0u, 1u, 0u, 1u, 0u, 0xffu};
    static const uint8_t oversized[65] = {0};
    turbo_flow_redis_blob_store_config_t config;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    uint8_t loaded[sizeof(first)];
    char key[128];
    size_t loaded_size = 0u;

    (void)snprintf(key, sizeof(key), "turboflow:live:blob:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = key;
    config.max_value_size = 64u;
    check_int_eq(turbo_flow_redis_blob_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.commit(store.ctx, key, first, sizeof(first)), TURBO_OK);
    check_int_eq(store.commit(store.ctx, key, oversized, sizeof(oversized)), TURBO_EMSGSIZE);
    turbo_flow_redis_blob_store_destroy(&store);

    check_int_eq(turbo_flow_redis_blob_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.load(store.ctx, key, loaded, sizeof(loaded), &loaded_size), TURBO_OK);
    check_size_eq(loaded_size, sizeof(first));
    check_mem_eq(loaded, first, sizeof(first));
    turbo_flow_redis_blob_store_destroy(&store);

    config.port = 1u;
    config.timeout_ms = 100u;
    check_int_eq(turbo_flow_redis_blob_store_create(&config, &store), TURBO_OK);
    check_int_ne(store.commit(store.ctx, key, oversized, sizeof(first)), TURBO_OK);
    turbo_flow_redis_blob_store_destroy(&store);

    config.port = 6379u;
    config.timeout_ms = 5000u;
    check_int_eq(turbo_flow_redis_blob_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.load(store.ctx, key, loaded, sizeof(loaded), &loaded_size), TURBO_OK);
    check_mem_eq(loaded, first, sizeof(first));
    turbo_flow_redis_blob_store_destroy(&store);
  }

  it("MQTT-STORE-003/010 commits revision-checked Redis records and restores namespace") {
    static const uint8_t key_a[] = {0u, 'R', 1u, 'a'};
    static const uint8_t key_b[] = {'b'};
    static const uint8_t key_c[] = {'c'};
    static const uint8_t value_one[] = {'o', 'n', 'e'};
    static const uint8_t value_two[] = {'t', 'w', 'o'};
    static const uint8_t value_next[] = {'n', 'e', 'x', 't'};
    turbo_flow_redis_record_store_config_t config;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_mutation_t mutations[2] = {TURBO_FLOW_RECORD_MUTATION_INIT,
                                                 TURBO_FLOW_RECORD_MUTATION_INIT};
    redis_live_record_capture_t capture;
    size_t found;
    char key[128];
    char yaml[1024];
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    (void)snprintf(key, sizeof(key), "turboflow:live:records:%llu",
                   (unsigned long long)turbo_hrtime());
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.database = 0;
    config.timeout_ms = 5000u;
    config.key = key;
    config.max_record_key_size = 16u;
    config.max_value_size = 32u;
    config.max_batch_size = 2u;
    config.max_records = 2u;
    check_true(snprintf(yaml, sizeof(yaml),
                        "version: 1\nchannels:\n  mqtt.sessions:\n    kind: record_store\n"
                        "    config:\n      backend: redis\n      host: 127.0.0.1\n"
                        "      port: 6379\n      database: 0\n      timeout_ms: 5000\n"
                        "      key: '%s'\n      max_key_size: 16\n      max_value_size: 32\n"
                        "      max_batch_size: 2\n      max_records: 2\nadapters: {}\n",
                        key) > 0);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
    check_int_eq(
        redis_test_record_store_open_resolved_ex(resolved, "mqtt.sessions", &store, &error),
        TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, redis_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 0u);

    mutations[0].key = key_a;
    mutations[0].key_size = sizeof(key_a);
    mutations[0].next_revision = 1u;
    mutations[0].value = value_one;
    mutations[0].value_size = sizeof(value_one);
    mutations[1].key = key_b;
    mutations[1].key_size = sizeof(key_b);
    mutations[1].next_revision = 7u;
    mutations[1].value = value_two;
    mutations[1].value_size = sizeof(value_two);
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_OK);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, redis_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 2u);
    found = redis_live_record_find(&capture, key_a, sizeof(key_a));
    check_true(found < capture.count);
    check_uint_eq(capture.revisions[found], 1u);
    check_mem_eq(capture.values[found], value_one, sizeof(value_one));
    found = redis_live_record_find(&capture, key_b, sizeof(key_b));
    check_true(found < capture.count);
    check_uint_eq(capture.revisions[found], 7u);

    mutations[0].expected_revision = 9u;
    mutations[0].next_revision = 10u;
    mutations[0].value = value_next;
    mutations[0].value_size = sizeof(value_next);
    mutations[1].kind = TURBO_FLOW_RECORD_DELETE;
    mutations[1].expected_revision = 7u;
    mutations[1].next_revision = 0u;
    mutations[1].value = NULL;
    mutations[1].value_size = 0u;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_EBUSY);
    mutations[0].expected_revision = 1u;
    mutations[0].next_revision = 2u;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_OK);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);

    check_int_eq(redis_test_record_store_open(&config, &store), TURBO_OK);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, redis_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    found = redis_live_record_find(&capture, key_a, sizeof(key_a));
    check_true(found < capture.count);
    check_uint_eq(capture.revisions[found], 2u);
    check_mem_eq(capture.values[found], value_next, sizeof(value_next));

    mutations[0] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
    mutations[0].key = key_b;
    mutations[0].key_size = sizeof(key_b);
    mutations[0].next_revision = 1u;
    mutations[0].value = value_two;
    mutations[0].value_size = sizeof(value_two);
    mutations[1] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
    mutations[1].key = key_c;
    mutations[1].key_size = sizeof(key_c);
    mutations[1].next_revision = 1u;
    mutations[1].value = value_one;
    mutations[1].value_size = sizeof(value_one);
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_ENOSPC);
    mutations[1].key = key_b;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_EINVAL);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, redis_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    check_int_eq(redis_test_storage_destroy(&store), TURBO_OK);
  }

  it("replays one consumer pending entry and interrupts a real blocked XREADGROUP") {
    static const char sink_dsl[] = "source input\n"
                                   "stage append adapter redis.out\n"
                                   "stage main {\n"
                                   "  input -> append\n"
                                   "}\n";
    static const char source_dsl[] = "source events adapter redis.in\n"
                                     "stage capture\n"
                                     "stage main {\n"
                                     "  events -> capture\n"
                                     "}\n";
    char stream[128];
    char group[128];
    char consumer[128];
    turbo_flow_redis_stream_config_t sink_config;
    turbo_flow_redis_stream_config_t source_config;
    redis_live_capture_t capture;
    turbo_flow_t *sink_flow;
    turbo_flow_t *source_flow;
    uint64_t suffix = turbo_hrtime();
    uint64_t stop_started_ns;
    (void)snprintf(stream, sizeof(stream), "turboflow:live:stream:%llu",
                   (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-live-group-%llu", (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-live-consumer-%llu",
                   (unsigned long long)suffix);

    sink_config = redis_live_stream_config(stream);
    sink_flow = turbo_flow_create();
    check_not_null(sink_flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(sink_flow, "redis.out", &sink_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(sink_flow, sink_dsl, sizeof(sink_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(sink_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(sink_flow), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "replay", 6u), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink_flow), TURBO_OK);
    turbo_flow_destroy(sink_flow);

    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    atomic_init(&capture.result, TURBO_EIO);
    source_config = redis_live_stream_config(stream);
    source_config.poll_interval_ms = 1u;
    source_config.group = group;
    source_config.consumer = consumer;
    source_config.group_start_id = "0";
    source_config.read_count = 1u;
    source_config.block_ms = 60000u;
    source_config.create_group = 1;
    source_flow = turbo_flow_create();
    check_not_null(source_flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(source_flow, "redis.in", &source_config),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(source_flow, "capture", redis_live_capture, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(source_flow, source_dsl, sizeof(source_dsl) - 1u),
                 TURBO_OK);
    check_int_eq(turbo_flow_compile(source_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(source_flow), TURBO_OK);
    check_int_eq(redis_live_wait_called(&capture, 1), TURBO_OK);
    check_int_eq(turbo_flow_stop(source_flow), TURBO_OK);

    atomic_store_explicit(&capture.result, TURBO_OK, memory_order_release);
    check_int_eq(turbo_flow_start(source_flow), TURBO_OK);
    check_int_eq(redis_live_wait_called(&capture, 2), TURBO_OK);
    check_size_eq(capture.payload_len, 6u);
    check_mem_eq(capture.payload, "replay", 6u);
    turbo_sleep_ms(100);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    stop_started_ns = turbo_hrtime();
    check_int_eq(turbo_flow_stop(source_flow), TURBO_OK);
    check_true(turbo_hrtime() - stop_started_ns < REDIS_LIVE_STOP_LIMIT_NS);
    turbo_flow_destroy(source_flow);
  }

  it("holds a Redis Stream claim in the PEL until explicit ack") {
    static const char sink_dsl[] = "source input\n"
                                   "stage append adapter redis.out\n"
                                   "stage main {\n"
                                   "  input -> append\n"
                                   "}\n";
    char stream[128];
    char group[128];
    char consumer[128];
    char first_id[64];
    uint64_t suffix = turbo_hrtime();
    turbo_flow_redis_stream_config_t sink_config;
    turbo_flow_redis_stream_config_t owner_config;
    turbo_flow_redis_stream_owner_t *owner;
    turbo_flow_redis_stream_claim_t first = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_redis_stream_claim_t second = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_t *sink_flow;

    (void)snprintf(stream, sizeof(stream), "turboflow:live:owner:%llu", (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-owner-group-%llu", (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-owner-consumer-%llu",
                   (unsigned long long)suffix);
    sink_config = redis_live_stream_config(stream);
    sink_flow = turbo_flow_create();
    check_not_null(sink_flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(sink_flow, "redis.out", &sink_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(sink_flow, sink_dsl, sizeof(sink_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(sink_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(sink_flow), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "delayed", 7u), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink_flow), TURBO_OK);
    turbo_flow_destroy(sink_flow);

    owner_config = redis_live_stream_config(stream);
    owner_config.group = group;
    owner_config.consumer = consumer;
    owner_config.group_start_id = "0";
    owner_config.block_ms = 10u;
    owner_config.create_group = 1;
    check_int_eq(turbo_flow_redis_stream_owner_create(&owner_config, &owner), TURBO_OK);
    check_not_null(owner);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &first), TURBO_OK);
    check_not_null(first.entry_id);
    check_mem_eq(first.payload.data, "delayed", 7u);
    (void)snprintf(first_id, sizeof(first_id), "%s", first.entry_id);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &second), TURBO_EBUSY);
    check_int_eq(turbo_flow_redis_stream_owner_requeue(owner, first.token), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_ack(owner, first.token), TURBO_EALREADY);

    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &second), TURBO_OK);
    check_str_eq(second.entry_id, first_id);
    check_mem_eq(second.payload.data, "delayed", 7u);
    check_int_eq(turbo_flow_redis_stream_owner_ack(owner, second.token), TURBO_OK);
    first = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &first), TURBO_ENOENT);
    turbo_flow_redis_stream_owner_destroy(owner);
  }

  it("replays bounded Redis multi-claims with stable views and independent settlement") {
    static const char sink_dsl[] = "source input\n"
                                   "stage append adapter redis.out\n"
                                   "stage main {\n"
                                   "  input -> append\n"
                                   "}\n";
    char stream[128];
    char group[128];
    char consumer[128];
    char first_id[64];
    char second_id[64];
    uint64_t suffix = turbo_hrtime();
    turbo_flow_redis_stream_config_t sink_config;
    turbo_flow_redis_stream_config_t owner_config;
    turbo_flow_redis_stream_claim_owner_config_t claim_config =
        TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_CONFIG_INIT;
    turbo_flow_redis_stream_owner_t *owner = NULL;
    turbo_flow_redis_stream_claim_t first = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_redis_stream_claim_t second = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_redis_stream_claim_t third = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_t *sink_flow;
    const char *second_view;

    (void)snprintf(stream, sizeof(stream), "turboflow:live:multi-owner:%llu",
                   (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-multi-owner-group-%llu",
                   (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-multi-owner-consumer-%llu",
                   (unsigned long long)suffix);
    sink_config = redis_live_stream_config(stream);
    sink_flow = turbo_flow_create();
    check_not_null(sink_flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(sink_flow, "redis.out", &sink_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(sink_flow, sink_dsl, sizeof(sink_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(sink_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(sink_flow), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "one", 3u), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "two", 3u), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "three", 5u), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink_flow), TURBO_OK);
    turbo_flow_destroy(sink_flow);

    owner_config = redis_live_stream_config(stream);
    owner_config.group = group;
    owner_config.consumer = consumer;
    owner_config.group_start_id = "0";
    owner_config.block_ms = 10u;
    owner_config.create_group = 1;
    claim_config.max_active_claims = 2u;
    check_int_eq(turbo_flow_redis_stream_owner_create_ex(&owner_config, &claim_config, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &first), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &second), TURBO_OK);
    check_mem_eq(first.payload.data, "one", 3u);
    check_mem_eq(second.payload.data, "two", 3u);
    (void)snprintf(first_id, sizeof(first_id), "%s", first.entry_id);
    (void)snprintf(second_id, sizeof(second_id), "%s", second.entry_id);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &third), TURBO_EBUSY);
    turbo_flow_redis_stream_owner_destroy(owner);

    owner = NULL;
    first = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    second = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_create_ex(&owner_config, &claim_config, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &first), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &second), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_settler(owner, &settler), TURBO_OK);
    check_str_eq(first.entry_id, first_id);
    check_str_eq(second.entry_id, second_id);
    second_view = second.entry_id;
    check_int_eq(settler.requeue(settler.ctx, second.token), TURBO_OK);
    check_int_eq(settler.ack(settler.ctx, first.token), TURBO_OK);
    check_str_eq(second_view, second_id);
    second = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &second), TURBO_OK);
    check_str_eq(second.entry_id, second_id);
    check_mem_eq(second.payload.data, "two", 3u);
    check_int_eq(settler.ack(settler.ctx, second.token), TURBO_OK);
    third = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &third), TURBO_OK);
    check_mem_eq(third.payload.data, "three", 5u);
    check_int_eq(settler.drop(settler.ctx, third.token), TURBO_OK);
    first = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &first), TURBO_ENOENT);
    turbo_flow_redis_stream_owner_destroy(owner);
  }

  it("atomically commits durable state with Redis Stream claim settlement") {
    static const char sink_dsl[] = "source input\n"
                                   "stage append adapter redis.out\n"
                                   "stage main {\n"
                                   "  input -> append\n"
                                   "}\n";
    static const uint8_t dispatched_state[] = {'T', 'F', 'C', 'S', 0, 1, 0, 0};
    static const uint8_t completed_state[] = {'T', 'F', 'C', 'S', 0, 1, 0, 1};
    static const uint8_t confirmed_state[] = {'T', 'F', 'C', 'S', 0, 1, 0, 2};
    char stream[128];
    char group[128];
    char consumer[128];
    char state_key[128];
    uint8_t loaded[32];
    size_t loaded_size = 0u;
    uint64_t suffix = turbo_hrtime();
    turbo_flow_redis_stream_config_t sink_config;
    turbo_flow_redis_stream_config_t owner_config;
    turbo_flow_redis_stream_owner_t *owner = NULL;
    turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_t *sink_flow;

    (void)snprintf(stream, sizeof(stream), "turboflow:live:atomic-owner:%llu",
                   (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-atomic-group-%llu", (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-atomic-consumer-%llu",
                   (unsigned long long)suffix);
    (void)snprintf(state_key, sizeof(state_key), "turboflow:live:atomic-state:%llu",
                   (unsigned long long)suffix);
    sink_config = redis_live_stream_config(stream);
    sink_flow = turbo_flow_create();
    check_not_null(sink_flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(sink_flow, "redis.out", &sink_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(sink_flow, sink_dsl, sizeof(sink_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(sink_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(sink_flow), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "durable", 7u), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink_flow), TURBO_OK);
    turbo_flow_destroy(sink_flow);

    owner_config = redis_live_stream_config(stream);
    owner_config.group = group;
    owner_config.consumer = consumer;
    owner_config.group_start_id = "0";
    owner_config.block_ms = 10u;
    owner_config.create_group = 1;
    check_int_eq(turbo_flow_redis_stream_owner_create(&owner_config, &owner), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &claim), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_settler(owner, &settler), TURBO_OK);
    check_not_null(settler.load_state);
    check_not_null(settler.commit_state);
    check_true(settler.max_state_size >= sizeof(dispatched_state));

    check_int_eq(settler.commit_state(settler.ctx, 0u, TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY,
                                      state_key, dispatched_state, sizeof(dispatched_state)),
                 TURBO_OK);
    check_int_eq(settler.load_state(settler.ctx, state_key, loaded, sizeof(loaded), &loaded_size),
                 TURBO_OK);
    check_size_eq(loaded_size, sizeof(dispatched_state));
    check_mem_eq(loaded, dispatched_state, sizeof(dispatched_state));

    check_int_eq(settler.commit_state(settler.ctx, claim.token, TURBO_FLOW_CLAIM_COMMIT_ACK,
                                      state_key, completed_state, sizeof(completed_state)),
                 TURBO_OK);
    check_int_eq(settler.load_state(settler.ctx, state_key, loaded, sizeof(loaded), &loaded_size),
                 TURBO_OK);
    check_mem_eq(loaded, completed_state, sizeof(completed_state));
    claim = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &claim), TURBO_ENOENT);

    check_int_eq(settler.commit_state(settler.ctx, 0u, TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY,
                                      state_key, confirmed_state, sizeof(confirmed_state)),
                 TURBO_OK);
    check_int_eq(settler.load_state(settler.ctx, state_key, loaded, sizeof(loaded), &loaded_size),
                 TURBO_OK);
    check_mem_eq(loaded, confirmed_state, sizeof(confirmed_state));
    turbo_flow_redis_stream_owner_destroy(owner);
  }

  it("recovers a real Redis-backed FMQ completion outbox after process recreation") {
    static const char sink_dsl[] = "source input\n"
                                   "stage append adapter redis.out\n"
                                   "stage main {\n"
                                   "  input -> append\n"
                                   "}\n";
    char stream[128];
    char group[128];
    char consumer[128];
    char state_key[128];
    uint64_t suffix = turbo_hrtime();
    turbo_flow_redis_stream_config_t sink_config;
    turbo_flow_redis_stream_config_t owner_config;
    turbo_flow_redis_stream_owner_t *owner = NULL;
    turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_fmq_credit_worker_config_t credit_config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_durable_config_t durable_config =
        TURBO_FLOW_FMQ_CREDIT_DURABLE_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *credit = NULL;
    turbo_flow_fmq_credit_durable_t *durable = NULL;
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_protocol_route_t worker = redis_live_route(80u, 1u);
    turbo_flow_protocol_route_t client = redis_live_route(90u, 1u);
    turbo_flow_fmq_broker_logical_address_t address =
        redis_live_address("redis-broker", "redis-client", 9001u);
    turbo_flow_fmq_broker_logical_address_t outbox = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_credit_settlement_result_t settled =
        TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    turbo_flow_t *sink_flow;

    (void)snprintf(stream, sizeof(stream), "turboflow:live:fmq-owner:%llu",
                   (unsigned long long)suffix);
    (void)snprintf(group, sizeof(group), "turboflow-fmq-group-%llu", (unsigned long long)suffix);
    (void)snprintf(consumer, sizeof(consumer), "turboflow-fmq-consumer-%llu",
                   (unsigned long long)suffix);
    (void)snprintf(state_key, sizeof(state_key), "turboflow:live:fmq-state:%llu",
                   (unsigned long long)suffix);

    sink_config = redis_live_stream_config(stream);
    sink_flow = turbo_flow_create();
    check_not_null(sink_flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(sink_flow, "redis.out", &sink_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(sink_flow, sink_dsl, sizeof(sink_dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(sink_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(sink_flow), TURBO_OK);
    check_int_eq(redis_live_publish(sink_flow, "fmq-job", 7u), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink_flow), TURBO_OK);
    turbo_flow_destroy(sink_flow);

    owner_config = redis_live_stream_config(stream);
    owner_config.group = group;
    owner_config.consumer = consumer;
    owner_config.group_start_id = "0";
    owner_config.block_ms = 10u;
    owner_config.create_group = 1;
    check_int_eq(turbo_flow_redis_stream_owner_create(&owner_config, &owner), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &claim), TURBO_OK);
    check_mem_eq(claim.payload.data, "fmq-job", 7u);
    check_int_eq(turbo_flow_redis_stream_owner_settler(owner, &settler), TURBO_OK);

    credit_config.max_workers = 1u;
    credit_config.max_inflight = 1u;
    credit_config.max_credit_messages_per_worker = 1u;
    credit_config.max_credit_bytes_per_worker = 32u;
    credit_config.max_job_bytes = 32u;
    credit_config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    durable_config.capacity = 1u;
    durable_config.state_key = state_key;
    grant.worker_id = "redis-worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 32u;

    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, claim.token, &address, 9101u,
                                                        "jobs", 16u, &client, 1u, &dispatch),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_durable_complete(durable, "redis-worker", &worker, 9101u, 2u,
                                                        &settled),
                 TURBO_OK);
    check_int_eq(settled.action, TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_ACK);

    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
    turbo_flow_redis_stream_owner_destroy(owner);
    durable = NULL;
    credit = NULL;
    owner = NULL;

    check_int_eq(turbo_flow_redis_stream_owner_create(&owner_config, &owner), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_settler(owner, &settler), TURBO_OK);
    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_next(durable, &outbox), TURBO_OK);
    check_str_eq(outbox.origin_broker_id, "redis-broker");
    check_str_eq(outbox.client_id, "redis-client");
    check_uint_eq(outbox.request_id, 9001u);
    claim = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &claim), TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_confirm(durable, &outbox, 3u), TURBO_OK);

    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
    turbo_flow_redis_stream_owner_destroy(owner);
  }
}
