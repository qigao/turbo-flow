#include "turbo_flow_queue.h"
#include "queue_test_paths.h"

#include "sqlite3.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_fs.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define QUEUE_TEST_MESSAGES 8
#define QUEUE_TEST_PAYLOAD 64

typedef struct queue_capture_s {
  char payloads[QUEUE_TEST_MESSAGES][QUEUE_TEST_PAYLOAD];
  size_t lengths[QUEUE_TEST_MESSAGES];
  atomic_int called;
  atomic_int failures_remaining;
  turbo_flow_protocol_route_t route;
  turbo_flow_content_descriptor_t descriptor;
  int has_route;
  int has_descriptor;
} queue_capture_t;

typedef struct queue_publish_thread_s {
  turbo_flow_t *flow;
  atomic_int entered;
  atomic_int result;
} queue_publish_thread_t;

typedef struct queue_protocol_owner_probe_s {
  int called;
  int result;
  turbo_flow_protocol_route_t route;
  turbo_flow_protocol_settlement_request_t request;
} queue_protocol_owner_probe_t;

static int queue_protocol_owner_settle(
    void *ctx, const turbo_flow_protocol_route_t *route,
    const turbo_flow_protocol_settlement_request_t *request) {
  queue_protocol_owner_probe_t *probe = (queue_protocol_owner_probe_t *)ctx;
  probe->called += 1;
  probe->route = *route;
  probe->request = *request;
  return probe->result;
}

typedef struct queue_release_probe_s {
  atomic_int count;
} queue_release_probe_t;

typedef struct queue_record_capture_s {
  uint8_t keys[4][16];
  size_t key_sizes[4];
  uint8_t values[4][32];
  size_t value_sizes[4];
  uint64_t revisions[4];
  size_t count;
} queue_record_capture_t;

static int queue_record_capture_visit(void *ctx, const turbo_flow_record_view_t *record) {
  queue_record_capture_t *capture = (queue_record_capture_t *)ctx;
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

static int queue_publish(turbo_flow_t *flow, char *payload, size_t payload_len,
                         void *transport_context);

static void queue_blocked_publish_thread(void *ctx) {
  queue_publish_thread_t *publish = (queue_publish_thread_t *)ctx;
  char payload[] = "two";
  atomic_store_explicit(&publish->entered, 1, memory_order_release);
  atomic_store_explicit(&publish->result, queue_publish(publish->flow, payload, 3, NULL),
                        memory_order_release);
}

static void queue_buffer_released(void *data, void *ctx) {
  queue_release_probe_t *probe = (queue_release_probe_t *)ctx;
  (void)data;
  atomic_fetch_add_explicit(&probe->count, 1, memory_order_release);
}

static void queue_capture_init(queue_capture_t *capture, int failures) {
  memset(capture, 0, sizeof(*capture));
  atomic_init(&capture->called, 0);
  atomic_init(&capture->failures_remaining, failures);
}

static int queue_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  queue_capture_t *capture = (queue_capture_t *)ctx;
  int failure_count;
  int index;
  if (!capture || !msg || msg->payload.len > QUEUE_TEST_PAYLOAD) return TURBO_EINVAL;
  failure_count = atomic_load_explicit(&capture->failures_remaining, memory_order_acquire);
  if (failure_count > 0 && atomic_compare_exchange_strong_explicit(
                               &capture->failures_remaining, &failure_count, failure_count - 1,
                               memory_order_acq_rel, memory_order_acquire)) {
    return TURBO_EIO;
  }
  index = atomic_load_explicit(&capture->called, memory_order_relaxed);
  if (index >= QUEUE_TEST_MESSAGES) return TURBO_ENOSPC;
  if (msg->payload.len > 0) {
    memcpy(capture->payloads[index], msg->payload.data, msg->payload.len);
  }
  capture->lengths[index] = msg->payload.len;
  {
    const turbo_flow_protocol_route_t *route = turbo_flow_msg_protocol_route(msg);
    const turbo_flow_content_descriptor_t *descriptor = turbo_flow_msg_content_descriptor(msg);
    if (route) {
      capture->route = *route;
      capture->has_route = 1;
    }
    if (descriptor) {
      capture->descriptor = *descriptor;
      capture->has_descriptor = 1;
    }
  }
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static void queue_wait_called(queue_capture_t *capture, int expected) {
  for (int i = 0;
       i < 400 && atomic_load_explicit(&capture->called, memory_order_acquire) < expected; ++i) {
    turbo_sleep_ms(5);
  }
}

static void queue_wait_publish_failure(turbo_flow_queue_t *queue) {
  turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
  for (int i = 0; i < 400; ++i) {
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    if (snapshot.publish_failures > 0) return;
    turbo_sleep_ms(5);
  }
}

static turbo_flow_t *queue_make_sink_flow(turbo_flow_queue_t *queue) {
  static const char *dsl = "source input\n"
                           "stage enqueue adapter queue.sink\n"
                           "stage main {\n"
                           "  input -> enqueue\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_queue_register_sink_adapter(flow, "queue.sink", queue) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *queue_make_source_flow(turbo_flow_queue_t *queue, queue_capture_t *capture) {
  static const char *dsl = "source dequeue adapter queue.source\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  dequeue -> capture\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_queue_register_source_adapter(flow, "queue.source", queue) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", queue_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *queue_make_sink_flow_resolved(turbo_flow_queue_t *queue,
                                                   const turbo_flow_resolved_config_t *resolved,
                                                   turbo_flow_config_error_t *error) {
  static const char *dsl = "source input\n"
                           "stage enqueue adapter queue.sink\n"
                           "stage main {\n"
                           "  input -> enqueue\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow ||
      turbo_flow_queue_register_resolved_adapter(flow, "queue.sink", resolved, queue, error) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *queue_make_source_flow_resolved(turbo_flow_queue_t *queue,
                                                     queue_capture_t *capture,
                                                     const turbo_flow_resolved_config_t *resolved,
                                                     turbo_flow_config_error_t *error) {
  static const char *dsl = "source dequeue adapter queue.source\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  dequeue -> capture\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow ||
      turbo_flow_queue_register_resolved_adapter(flow, "queue.source", resolved, queue, error) !=
          TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", queue_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int queue_publish(turbo_flow_t *flow, char *payload, size_t payload_len,
                         void *transport_context) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(payload, payload_len);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  msg.transport_context = transport_context;
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static turbo_flow_queue_t *queue_create(size_t capacity, size_t max_payload_size,
                                        turbo_flow_queue_full_policy_t policy,
                                        uint64_t timeout_ms) {
  turbo_flow_queue_config_t config;
  memset(&config, 0, sizeof(config));
  config.resource_uid = "queue:test";
  config.owner_name = "test-queue";
  config.capacity = capacity;
  config.max_payload_size = max_payload_size;
  config.full_policy = policy;
  config.enqueue_timeout_ms = timeout_ms;
  return turbo_flow_queue_create(&config);
}

static turbo_flow_queue_t *queue_create_sqlite(const char *path, size_t capacity) {
  turbo_flow_sqlite_queue_config_t config;
  memset(&config, 0, sizeof(config));
  config.queue.resource_uid = "queue:sqlite-test";
  config.queue.owner_name = "sqlite-test-queue";
  config.queue.capacity = capacity;
  config.queue.max_payload_size = 64u;
  config.queue.full_policy = TURBO_FLOW_QUEUE_FULL_FAIL;
  config.database_path = path;
  config.queue_name = "orders";
  config.busy_timeout_ms = 1000;
  return turbo_flow_sqlite_queue_create(&config);
}

static void queue_mark_sqlite_rows_in_flight(const char *path) {
  sqlite3 *database = NULL;
  char *error = NULL;
  check_int_eq(sqlite3_open(path, &database), SQLITE_OK);
  check_not_null(database);
  check_int_eq(sqlite3_exec(database,
                            "UPDATE turbo_flow_queue_messages SET state=1 "
                            "WHERE queue_name='orders'",
                            NULL, NULL, &error),
               SQLITE_OK);
  sqlite3_free(error);
  check_int_eq(sqlite3_close(database), SQLITE_OK);
}

static void queue_create_legacy_sqlite_schema(const char *path) {
  static const char schema[] =
      "CREATE TABLE turbo_flow_queue_messages ("
      "sequence INTEGER PRIMARY KEY AUTOINCREMENT,queue_name TEXT NOT NULL,"
      "message_id INTEGER NOT NULL,timestamp_ns INTEGER NOT NULL,message_type INTEGER NOT NULL,"
      "flags INTEGER NOT NULL,status INTEGER NOT NULL,payload BLOB NOT NULL,"
      "state INTEGER NOT NULL CHECK(state IN (0,1)));"
      "CREATE INDEX turbo_flow_queue_pending "
      "ON turbo_flow_queue_messages(queue_name,state,sequence);";
  sqlite3 *database = NULL;
  check_int_eq(sqlite3_open(path, &database), SQLITE_OK);
  check_not_null(database);
  check_int_eq(sqlite3_exec(database, schema, NULL, NULL, NULL), SQLITE_OK);
  check_int_eq(sqlite3_close(database), SQLITE_OK);
}

static int queue_sqlite_schema_version(const char *path) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  int version = -1;
  check_int_eq(sqlite3_open(path, &database), SQLITE_OK);
  check_not_null(database);
  check_int_eq(
      sqlite3_prepare_v2(database,
                         "SELECT version FROM turbo_flow_queue_schema WHERE schema_name='queue'",
                         -1, &statement, NULL),
      SQLITE_OK);
  check_int_eq(sqlite3_step(statement), SQLITE_ROW);
  version = sqlite3_column_int(statement, 0);
  check_int_eq(sqlite3_finalize(statement), SQLITE_OK);
  check_int_eq(sqlite3_close(database), SQLITE_OK);
  return version;
}

static void queue_set_sqlite_schema_version(const char *path, int version) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  check_int_eq(sqlite3_open(path, &database), SQLITE_OK);
  check_not_null(database);
  check_int_eq(sqlite3_prepare_v2(database,
                                  "UPDATE turbo_flow_queue_schema SET version=?1 "
                                  "WHERE schema_name='queue'",
                                  -1, &statement, NULL),
               SQLITE_OK);
  check_int_eq(sqlite3_bind_int(statement, 1, version), SQLITE_OK);
  check_int_eq(sqlite3_step(statement), SQLITE_DONE);
  check_int_eq(sqlite3_changes(database), 1);
  check_int_eq(sqlite3_finalize(statement), SQLITE_OK);
  check_int_eq(sqlite3_close(database), SQLITE_OK);
}

static void queue_apply_sqlite_ack_without_local_release(const char *path, const char *state_key,
                                                         const uint8_t *state, size_t state_size) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  check_int_eq(sqlite3_open(path, &database), SQLITE_OK);
  check_not_null(database);
  check_int_eq(sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL), SQLITE_OK);
  check_int_eq(
      sqlite3_prepare_v2(
          database,
          "INSERT INTO turbo_flow_queue_claim_state(queue_name,state_key,value) VALUES(?1,?2,?3) "
          "ON CONFLICT(queue_name,state_key) DO UPDATE SET value=excluded.value",
          -1, &statement, NULL),
      SQLITE_OK);
  check_int_eq(sqlite3_bind_text(statement, 1, "orders", -1, SQLITE_STATIC), SQLITE_OK);
  check_int_eq(sqlite3_bind_text(statement, 2, state_key, -1, SQLITE_STATIC), SQLITE_OK);
  check_int_eq(sqlite3_bind_blob(statement, 3, state, (int)state_size, SQLITE_TRANSIENT),
               SQLITE_OK);
  check_int_eq(sqlite3_step(statement), SQLITE_DONE);
  check_int_eq(sqlite3_finalize(statement), SQLITE_OK);
  statement = NULL;
  check_int_eq(
      sqlite3_prepare_v2(
          database,
          "DELETE FROM turbo_flow_queue_messages WHERE sequence=(SELECT sequence FROM "
          "turbo_flow_queue_messages WHERE queue_name=?1 AND state=1 ORDER BY sequence LIMIT 1)",
          -1, &statement, NULL),
      SQLITE_OK);
  check_int_eq(sqlite3_bind_text(statement, 1, "orders", -1, SQLITE_STATIC), SQLITE_OK);
  check_int_eq(sqlite3_step(statement), SQLITE_DONE);
  check_int_eq(sqlite3_changes(database), 1);
  check_int_eq(sqlite3_finalize(statement), SQLITE_OK);
  check_int_eq(sqlite3_exec(database, "COMMIT", NULL, NULL, NULL), SQLITE_OK);
  check_int_eq(sqlite3_close(database), SQLITE_OK);
}

spec("turbo_flow_queue") {
  it("clones FIFO messages across flows and acknowledges after delivery") {
    turbo_flow_queue_t *queue = queue_create(4, 64, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    queue_capture_t capture;
    char first[] = "one";
    char second[] = "two";
    turbo_flow_t *sink;
    turbo_flow_t *source;
    check_not_null(queue);
    queue_capture_init(&capture, 0);
    sink = queue_make_sink_flow(queue);
    source = queue_make_source_flow(queue, &capture);
    check_not_null(sink);
    check_not_null(source);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_EBUSY);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, first, 3, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, second, 3, NULL), TURBO_OK);
    memcpy(first, "bad", 3);
    memcpy(second, "bad", 3);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 2);
    check_int_eq(turbo_flow_start(source), TURBO_OK);
    queue_wait_called(&capture, 2);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_mem_eq(capture.payloads[0], "one", 3);
    check_mem_eq(capture.payloads[1], "two", 3);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0);
    check_size_eq(snapshot.enqueued, 2);
    check_size_eq(snapshot.delivered, 2);
    check_int_eq(turbo_flow_stop(source), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(source);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("preserves owned protocol routes only in a memory queue") {
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    turbo_flow_queue_t *queue = queue_create(2, 64, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    queue_capture_t capture;
    turbo_flow_msg_t msg;
    turbo_flow_t *sink;
    turbo_flow_t *source;
    check_not_null(queue);
    queue_capture_init(&capture, 0);
    sink = queue_make_sink_flow(queue);
    source = queue_make_source_flow(queue, &capture);
    check_not_null(sink);
    check_not_null(source);
    check_int_eq(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                                                    TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA,
                                                    TURBO_FLOW_DATA_ENCODING_JSON,
                                                    "application/json", "jobs"),
                 TURBO_OK);
    route.protocol = TURBO_FLOW_PROTOCOL_FMQ;
    route.owner_instance_id = 11u;
    route.session_id = 22u;
    route.session_generation = 33u;
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("{}", 2u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_copy_content_descriptor(&msg, &descriptor), TURBO_OK);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(turbo_flow_publish(sink, "input", &msg), TURBO_OK);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_start(source), TURBO_OK);
    queue_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_true(capture.has_route);
    check_true(capture.has_descriptor);
    check_uint_eq(capture.route.owner_instance_id, 11u);
    check_uint_eq(capture.route.session_id, 22u);
    check_uint_eq(capture.route.session_generation, 33u);
    check_str_eq(capture.descriptor.media_type, "application/json");
    check_int_eq(turbo_flow_stop(source), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(source);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("uses memory enqueue commit as one exact routed ACCEPTED boundary") {
    static const char dsl[] = "source input\n"
                              "stage enqueue adapter queue.sink\n"
                              "stage main {\n"
                              "  input -> enqueue\n"
                              "}\n";
    turbo_flow_protocol_route_owner_ops_t owner_ops =
        TURBO_FLOW_PROTOCOL_ROUTE_OWNER_OPS_INIT;
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_protocol_settlement_envelope_t envelope =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT;
    turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
    turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_t *queue = queue_create(2u, 64u, TURBO_FLOW_QUEUE_FULL_FAIL, 0u);
    queue_protocol_owner_probe_t owner = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(queue);
    check_not_null(flow);
    owner_ops.settle = queue_protocol_owner_settle;
    route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    route.owner_instance_id = 71u;
    route.session_id = 72u;
    route.session_generation = 73u;
    envelope.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    envelope.message.protocol_version = TURBO_FLOW_MQTT_PROTOCOL_5_0;
    envelope.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
    envelope.message.qos = TURBO_FLOW_PROTOCOL_QOS_1;
    envelope.message.packet_id = 74u;
    envelope.message.session_generation = route.session_generation;
    envelope.requested_point = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    check_int_eq(turbo_flow_queue_register_sink_adapter(flow, "queue.sink", queue), TURBO_OK);
    check_int_eq(turbo_flow_register_protocol_route_owner(
                     flow, route.protocol, route.owner_instance_id, &owner_ops, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("accepted", 8u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_msg_set_protocol_settlement(&msg, &envelope), TURBO_OK);
    check_int_eq(turbo_flow_publish_ex(flow, "input", &msg, &result), TURBO_OK);
    check_uint_eq(result.protocol_settlement, TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED);
    check_int_eq(owner.called, 1);
    check_uint_eq(owner.route.session_id, route.session_id);
    check_uint_eq(owner.request.point, TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_not_null(claim.message);
    check_not_null(turbo_flow_msg_protocol_route(claim.message));
    check_null(turbo_flow_msg_protocol_settlement(claim.message));
    check_int_eq(turbo_flow_queue_claim_ack(queue, claim.token), TURBO_OK);
    turbo_flow_msg_cleanup(&msg);

    owner.result = TURBO_EIO;
    result = (turbo_flow_publish_result_t)TURBO_FLOW_PUBLISH_RESULT_INIT;
    claim = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("committed-no-ack", 16u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_msg_set_protocol_settlement(&msg, &envelope), TURBO_OK);
    check_int_eq(turbo_flow_publish_ex(flow, "input", &msg, &result), TURBO_EIO);
    check_int_eq(result.status, TURBO_EIO);
    check_uint_eq(result.protocol_settlement, 0u);
    check_int_eq(owner.called, 2);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_mem_eq(claim.message->payload.data, "committed-no-ack", 16u);
    check_int_eq(turbo_flow_queue_claim_ack(queue, claim.token), TURBO_OK);
    turbo_flow_msg_cleanup(&msg);

    owner.result = TURBO_OK;
    envelope.requested_point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    result = (turbo_flow_publish_result_t)TURBO_FLOW_PUBLISH_RESULT_INIT;
    claim = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("wrong-boundary", 14u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_msg_set_protocol_settlement(&msg, &envelope), TURBO_OK);
    check_int_eq(turbo_flow_publish_ex(flow, "input", &msg, &result), TURBO_ENOTSUP);
    check_uint_eq(result.protocol_settlement, 0u);
    check_int_eq(owner.called, 2);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_ENOENT);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("uses SQLite COMMIT as one routed DURABLE boundary without persisting the live route") {
    static const char dsl[] = "source input\n"
                              "stage enqueue adapter queue.sink\n"
                              "stage main {\n"
                              "  input -> enqueue\n"
                              "}\n";
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_protocol_route_owner_ops_t owner_ops =
        TURBO_FLOW_PROTOCOL_ROUTE_OWNER_OPS_INIT;
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_protocol_settlement_envelope_t envelope =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT;
    turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
    turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
    queue_protocol_owner_probe_t owner = {0};
    turbo_flow_queue_t *queue;
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    queue_test_database_path(path, sizeof(path));
    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    check_not_null(flow);
    owner_ops.settle = queue_protocol_owner_settle;
    route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    route.owner_instance_id = 81u;
    route.session_id = 82u;
    route.session_generation = 83u;
    envelope.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    envelope.message.protocol_version = TURBO_FLOW_MQTT_PROTOCOL_5_0;
    envelope.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
    envelope.message.qos = TURBO_FLOW_PROTOCOL_QOS_1;
    envelope.message.packet_id = 84u;
    envelope.message.session_generation = route.session_generation;
    envelope.requested_point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    check_int_eq(turbo_flow_queue_register_sink_adapter(flow, "queue.sink", queue), TURBO_OK);
    check_int_eq(turbo_flow_register_protocol_route_owner(
                     flow, route.protocol, route.owner_instance_id, &owner_ops, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("durable", 7u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_msg_set_protocol_settlement(&msg, &envelope), TURBO_OK);
    check_int_eq(turbo_flow_publish_ex(flow, "input", &msg, &result), TURBO_OK);
    check_uint_eq(result.protocol_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(owner.called, 1);
    check_uint_eq(owner.request.point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);

    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_not_null(claim.message);
    check_mem_eq(claim.message->payload.data, "durable", 7u);
    check_null(turbo_flow_msg_protocol_route(claim.message));
    check_null(turbo_flow_msg_protocol_settlement(claim.message));
    check_int_eq(turbo_flow_queue_claim_ack(queue, claim.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    queue_remove_database(path);
  }

  it("rejects a live protocol route at the SQLite serialization boundary") {
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_queue_t *queue;
    turbo_flow_msg_t msg;
    turbo_flow_t *sink;
    queue_test_database_path(path, sizeof(path));
    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    route.protocol = TURBO_FLOW_PROTOCOL_FMQ;
    route.owner_instance_id = 1u;
    route.session_id = 2u;
    route.session_generation = 3u;
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("route", 5u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(turbo_flow_publish(sink, "input", &msg), TURBO_ENOTSUP);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    queue_remove_database(path);
  }

  it("fails explicitly when a fail-policy queue is full") {
    turbo_flow_queue_t *queue = queue_create(1, 8, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    char one[] = "1";
    char two[] = "2";
    turbo_flow_t *sink;
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, one, 1, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, two, 1, NULL), TURBO_ENOSPC);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 1);
    check_size_eq(snapshot.enqueued, 1);
    check_size_eq(snapshot.enqueue_failures, 1);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("drops the oldest queued message only when explicitly configured") {
    turbo_flow_queue_t *queue = queue_create(2, 8, TURBO_FLOW_QUEUE_FULL_DROP_OLDEST, 0);
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    queue_capture_t capture;
    char one[] = "1";
    char two[] = "2";
    char three[] = "3";
    turbo_flow_t *sink;
    turbo_flow_t *source;
    check_not_null(queue);
    queue_capture_init(&capture, 0);
    sink = queue_make_sink_flow(queue);
    source = queue_make_source_flow(queue, &capture);
    check_not_null(sink);
    check_not_null(source);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, one, 1, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, two, 1, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, three, 1, NULL), TURBO_OK);
    check_int_eq(turbo_flow_start(source), TURBO_OK);
    queue_wait_called(&capture, 2);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_mem_eq(capture.payloads[0], "2", 1);
    check_mem_eq(capture.payloads[1], "3", 1);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.dropped_oldest, 1);
    check_size_eq(snapshot.delivered, 2);
    check_int_eq(turbo_flow_stop(source), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(source);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("times out bounded blocking enqueue and rejects unsafe message state") {
    turbo_flow_queue_t *queue = queue_create(1, 3, TURBO_FLOW_QUEUE_FULL_BLOCK, 20);
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    char one[] = "one";
    char two[] = "two";
    char large[] = "four";
    int borrowed_context = 1;
    turbo_flow_t *sink;
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, one, 3, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, two, 3, NULL), TURBO_ETIMEDOUT);
    check_int_eq(queue_publish(sink, large, 4, NULL), TURBO_EMSGSIZE);
    check_int_eq(queue_publish(sink, two, 3, &borrowed_context), TURBO_ENOTSUP);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 1);
    check_size_eq(snapshot.enqueue_failures, 3);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("wakes a blocked enqueue when its sink flow stops") {
    turbo_flow_queue_t *queue = queue_create(1, 8, TURBO_FLOW_QUEUE_FULL_BLOCK, 5000);
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    queue_publish_thread_t publish;
    turbo_thread_t thread;
    char one[] = "one";
    uint64_t stop_started_ns;
    turbo_flow_t *sink;
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, one, 3, NULL), TURBO_OK);
    memset(&publish, 0, sizeof(publish));
    publish.flow = sink;
    atomic_init(&publish.entered, 0);
    atomic_init(&publish.result, TURBO_EBUSY);
    check_int_eq(turbo_thread_create(&thread, queue_blocked_publish_thread, &publish), TURBO_OK);
    while (!atomic_load_explicit(&publish.entered, memory_order_acquire))
      turbo_thread_yield();
    turbo_sleep_ms(20);
    check_int_eq(atomic_load_explicit(&publish.result, memory_order_acquire), TURBO_EBUSY);
    stop_started_ns = turbo_hrtime();
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    check_true(turbo_hrtime() - stop_started_ns < UINT64_C(500000000));
    check_int_eq(turbo_thread_join(&thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&publish.result, memory_order_acquire), TURBO_ESHUTDOWN);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 1);
    check_size_eq(snapshot.enqueue_failures, 1);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("releases buffer ownership for messages still pending at destruction") {
    turbo_flow_queue_t *queue = queue_create(1, 8, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer;
    queue_release_probe_t released;
    char payload[] = "owned";
    turbo_flow_t *sink;
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    atomic_init(&released.count, 0);
    buffer = mem_wrap_external(payload, 5, queue_buffer_released, &released);
    check_not_null(buffer);
    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = tstr_v_from_buf(payload, 5);
    check_int_eq(turbo_flow_publish(sink, "input", &msg), TURBO_OK);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(atomic_load_explicit(&released.count, memory_order_acquire), 0);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(atomic_load_explicit(&released.count, memory_order_acquire), 0);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    check_int_eq(atomic_load_explicit(&released.count, memory_order_acquire), 1);
  }

  it("requeues an unacknowledged message after downstream failure") {
    turbo_flow_queue_t *queue = queue_create(2, 16, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_queue_ack_snapshot_t acknowledgements = TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
    queue_capture_t failing_capture;
    queue_capture_t success_capture;
    char payload[] = "retry";
    turbo_flow_t *sink;
    turbo_flow_t *failing_source;
    turbo_flow_t *success_source;
    check_not_null(queue);
    queue_capture_init(&failing_capture, 1);
    queue_capture_init(&success_capture, 0);
    sink = queue_make_sink_flow(queue);
    failing_source = queue_make_source_flow(queue, &failing_capture);
    check_not_null(sink);
    check_not_null(failing_source);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, 5, NULL), TURBO_OK);
    check_int_eq(turbo_flow_start(failing_source), TURBO_OK);
    queue_wait_publish_failure(queue);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 1);
    check_size_eq(snapshot.delivered, 0);
    check_size_eq(snapshot.publish_failures, 1);
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_size_eq(acknowledgements.accept_acks, 1);
    check_size_eq(acknowledgements.delivery_acks, 0);
    check_size_eq(acknowledgements.delivery_requeues, 1);
    check_size_eq(acknowledgements.delivery_requeue_failures, 0);
    check_int_eq(turbo_flow_stop(failing_source), TURBO_OK);
    turbo_flow_destroy(failing_source);

    success_source = queue_make_source_flow(queue, &success_capture);
    check_not_null(success_source);
    check_int_eq(turbo_flow_start(success_source), TURBO_OK);
    queue_wait_called(&success_capture, 1);
    check_int_eq(atomic_load_explicit(&success_capture.called, memory_order_acquire), 1);
    check_mem_eq(success_capture.payloads[0], "retry", 5);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0);
    check_size_eq(snapshot.delivered, 1);
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_size_eq(acknowledgements.accept_acks, 1);
    check_size_eq(acknowledgements.delivery_acks, 1);
    check_int_eq(turbo_flow_stop(success_source), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(success_source);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("recovers committed and stale in-flight SQLite messages after restart") {
    char path[TURBO_FS_MAX_PATH];
    char payload[] = "durable";
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_queue_ack_snapshot_t acknowledgements = TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
    queue_capture_t capture;
    turbo_flow_queue_t *queue;
    turbo_flow_t *sink;
    turbo_flow_t *source;
    queue_test_database_path(path, sizeof(path));
    queue = queue_create_sqlite(path, 4u);
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, 7u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_size_eq(acknowledgements.accept_acks, 1u);
    check_size_eq(acknowledgements.delivery_acks, 0u);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);

    queue_mark_sqlite_rows_in_flight(path);
    queue = queue_create_sqlite(path, 4u);
    check_not_null(queue);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 1u);
    queue_capture_init(&capture, 0);
    source = queue_make_source_flow(queue, &capture);
    check_not_null(source);
    check_int_eq(turbo_flow_start(source), TURBO_OK);
    queue_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_mem_eq(capture.payloads[0], "durable", 7u);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0u);
    check_size_eq(snapshot.delivered, 1u);
    acknowledgements = (turbo_flow_queue_ack_snapshot_t)TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_size_eq(acknowledgements.accept_acks, 0u);
    check_size_eq(acknowledgements.delivery_acks, 1u);
    check_int_eq(turbo_flow_stop(source), TURBO_OK);
    turbo_flow_destroy(source);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    queue_remove_database(path);
  }

  it("holds a memory request until an asynchronous claim is acked or requeued") {
    char payload[] = "delayed-reply";
    turbo_flow_queue_t *queue = queue_create(2u, 32u, TURBO_FLOW_QUEUE_FULL_FAIL, 0u);
    turbo_flow_queue_claim_t first = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_claim_t second = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_queue_ack_snapshot_t acknowledgements = TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
    queue_capture_t capture;
    turbo_flow_t *sink;
    turbo_flow_t *source;

    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    queue_capture_init(&capture, 0);
    source = queue_make_source_flow(queue, &capture);
    check_not_null(sink);
    check_not_null(source);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, sizeof(payload) - 1u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &first), TURBO_OK);
    check_not_null(first.message);
    check_mem_eq(first.message->payload.data, payload, sizeof(payload) - 1u);
    check_int_eq(turbo_flow_queue_claim(queue, &second), TURBO_EBUSY);
    check_int_eq(turbo_flow_start(source), TURBO_EBUSY);
    check_int_eq(turbo_flow_queue_claim_requeue(queue, first.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim_ack(queue, first.token), TURBO_EALREADY);

    check_int_eq(turbo_flow_queue_claim(queue, &second), TURBO_OK);
    check_true(second.token > first.token);
    check_mem_eq(second.message->payload.data, payload, sizeof(payload) - 1u);
    check_int_eq(turbo_flow_queue_claim_ack(queue, second.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0u);
    check_size_eq(snapshot.delivered, 1u);
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_size_eq(acknowledgements.accept_acks, 1u);
    check_size_eq(acknowledgements.delivery_acks, 1u);
    check_size_eq(acknowledgements.delivery_requeues, 1u);
    turbo_flow_destroy(source);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("persists an asynchronous SQLite requeue across queue recreation") {
    char path[TURBO_FS_MAX_PATH];
    char payload[] = "durable-delayed";
    turbo_flow_queue_t *queue;
    turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_t *sink;

    queue_test_database_path(path, sizeof(path));
    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, sizeof(payload) - 1u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_mem_eq(claim.message->payload.data, payload, sizeof(payload) - 1u);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_EBUSY);
    check_int_eq(turbo_flow_queue_claim_requeue(queue, claim.token), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);

    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    claim = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_mem_eq(claim.message->payload.data, payload, sizeof(payload) - 1u);
    check_int_eq(turbo_flow_queue_claim_ack(queue, claim.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0u);
    check_size_eq(snapshot.delivered, 1u);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    queue_remove_database(path);
  }

  it("atomically restores a fixed-key SQLite blob after recreation") {
    static const uint8_t snapshot[] = {0x54u, 0x46u, 0x4du, 0x53u, 0x00u, 0x01u};
    char path[TURBO_FS_MAX_PATH];
    char yaml[TURBO_FS_MAX_PATH + 512u];
    char resolved_key[TURBO_FLOW_SQLITE_BLOB_STORE_KEY_MAX + 1u];
    uint8_t loaded[sizeof(snapshot)];
    size_t loaded_size = 0u;
    turbo_flow_sqlite_blob_store_config_t config;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    queue_test_database_path(path, sizeof(path));
    memset(&config, 0, sizeof(config));
    config.database_path = path;
    config.key = "fmq:management:test";
    config.busy_timeout_ms = 1000;
    config.max_value_size = 64u;
    check_true(snprintf(yaml, sizeof(yaml),
                        "version: 1\nchannels:\n  operations:\n    kind: blob_store\n"
                        "    config:\n      backend: sqlite\n      database_path: '%s'\n"
                        "      key: fmq:management:test\n      busy_timeout_ms: 1000\n"
                        "      max_value_size: 64\nadapters: {}\n",
                        path) > 0);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_blob_store_create_resolved(
                     resolved, "operations", &store, resolved_key, sizeof(resolved_key), &error),
                 TURBO_OK);
    check_str_eq(resolved_key, config.key);
    turbo_flow_resolved_config_destroy(resolved);
    check_int_eq(store.load(store.ctx, config.key, loaded, sizeof(loaded), &loaded_size),
                 TURBO_ENOENT);
    check_int_eq(store.commit(store.ctx, config.key, snapshot, sizeof(snapshot)), TURBO_OK);
    check_int_eq(store.load(store.ctx, config.key, NULL, 0u, &loaded_size), TURBO_ENOSPC);
    check_size_eq(loaded_size, sizeof(snapshot));
    turbo_flow_sqlite_blob_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_blob_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.load(store.ctx, config.key, loaded, sizeof(loaded), &loaded_size), TURBO_OK);
    check_size_eq(loaded_size, sizeof(snapshot));
    check_mem_eq(loaded, snapshot, sizeof(snapshot));
    check_int_eq(store.load(store.ctx, "wrong-key", loaded, sizeof(loaded), &loaded_size),
                 TURBO_EINVAL);
    turbo_flow_sqlite_blob_store_destroy(&store);
    queue_remove_database(path);
  }

  it("declines a foreign record-store backend before validating provider fields") {
    static const char yaml[] =
        "version: 1\nchannels:\n  mqtt.sessions:\n    kind: record_store\n"
        "    config:\n      backend: redis\n      host: 127.0.0.1\n"
        "      port: 6379\n      key: flowie:sessions\nadapters: {}\n";
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(
                     resolved, "mqtt.sessions", &store, &error),
                 TURBO_ENOTSUP);
    check_str_eq(error.path, "$.channels.mqtt.sessions.config.backend");
    check_null(store.ctx);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("commits revision-checked SQLite record batches and restores a bounded namespace") {
    static const uint8_t key_a[] = {'a'};
    static const uint8_t key_b[] = {'b'};
    static const uint8_t key_c[] = {'c'};
    static const uint8_t value_one[] = {'o', 'n', 'e'};
    static const uint8_t value_two[] = {'t', 'w', 'o'};
    static const uint8_t value_next[] = {'n', 'e', 'x', 't'};
    char path[TURBO_FS_MAX_PATH];
    char yaml[TURBO_FS_MAX_PATH + 768u];
    turbo_flow_sqlite_record_store_config_t config;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_record_mutation_t mutations[2] = {
        TURBO_FLOW_RECORD_MUTATION_INIT, TURBO_FLOW_RECORD_MUTATION_INIT};
    queue_record_capture_t capture;

    queue_test_database_path(path, sizeof(path));
    memset(&config, 0, sizeof(config));
    config.database_path = path;
    config.namespace_name = "mqtt.sessions";
    config.busy_timeout_ms = 1000;
    config.max_key_size = 16u;
    config.max_value_size = 32u;
    config.max_batch_size = 2u;
    config.max_records = 2u;
    check_true(snprintf(yaml, sizeof(yaml),
                        "version: 1\nchannels:\n  mqtt.sessions:\n    kind: record_store\n"
                        "    config:\n      backend: sqlite\n      database_path: '%s'\n"
                        "      namespace_name: mqtt.sessions\n      busy_timeout_ms: 1000\n"
                        "      max_key_size: 16\n      max_value_size: 32\n"
                        "      max_batch_size: 2\n      max_records: 2\nadapters: {}\n",
                        path) > 0);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(
                     resolved, "mqtt.sessions", &store, &error),
                 TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    check_uint_eq(store.capabilities,
                  TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, queue_record_capture_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 0u);

    mutations[0].key = key_b;
    mutations[0].key_size = sizeof(key_b);
    mutations[0].next_revision = 7u;
    mutations[0].value = value_two;
    mutations[0].value_size = sizeof(value_two);
    mutations[1].key = key_a;
    mutations[1].key_size = sizeof(key_a);
    mutations[1].next_revision = 1u;
    mutations[1].value = value_one;
    mutations[1].value_size = sizeof(value_one);
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_OK);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, queue_record_capture_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 2u);
    check_mem_eq(capture.keys[0], key_a, sizeof(key_a));
    check_uint_eq(capture.revisions[0], 1u);
    check_mem_eq(capture.values[0], value_one, sizeof(value_one));
    check_mem_eq(capture.keys[1], key_b, sizeof(key_b));
    check_uint_eq(capture.revisions[1], 7u);

    mutations[0].kind = TURBO_FLOW_RECORD_PUT;
    mutations[0].key = key_a;
    mutations[0].key_size = sizeof(key_a);
    mutations[0].expected_revision = 9u;
    mutations[0].next_revision = 10u;
    mutations[0].value = value_next;
    mutations[0].value_size = sizeof(value_next);
    mutations[1].kind = TURBO_FLOW_RECORD_DELETE;
    mutations[1].key = key_b;
    mutations[1].key_size = sizeof(key_b);
    mutations[1].expected_revision = 7u;
    mutations[1].next_revision = 0u;
    mutations[1].value = NULL;
    mutations[1].value_size = 0u;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_EBUSY);
    mutations[0].expected_revision = 1u;
    mutations[0].next_revision = 2u;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_OK);
    turbo_flow_sqlite_record_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, queue_record_capture_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    check_uint_eq(capture.revisions[0], 2u);
    check_mem_eq(capture.values[0], value_next, sizeof(value_next));

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
    check_int_eq(store.scan(store.ctx, queue_record_capture_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    turbo_flow_sqlite_record_store_destroy(&store);
    queue_remove_database(path);
  }

  it("fails SQLite queue creation for invalid configuration and undersized recovery capacity") {
    char path[TURBO_FS_MAX_PATH];
    char payload[] = "one";
    turbo_flow_sqlite_queue_config_t invalid;
    turbo_flow_queue_t *queue;
    turbo_flow_t *sink;
    queue_test_database_path(path, sizeof(path));
    memset(&invalid, 0, sizeof(invalid));
    invalid.queue.resource_uid = "queue:invalid-sqlite";
    invalid.queue.owner_name = "invalid-sqlite";
    invalid.queue.capacity = 1u;
    invalid.queue.max_payload_size = 8u;
    invalid.database_path = path;
    invalid.queue_name = "orders";
    invalid.busy_timeout_ms = -1;
    check_null(turbo_flow_sqlite_queue_create(&invalid));

    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, 3u, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, 3u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    check_null(queue_create_sqlite(path, 1u));
    queue = queue_create_sqlite(path, 2u);
    check_not_null(queue);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    queue_remove_database(path);
  }

  it("stops an empty source and permits only one active source") {
    turbo_flow_queue_t *queue = queue_create(2, 16, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    queue_capture_t first_capture;
    queue_capture_t second_capture;
    turbo_flow_t *first;
    turbo_flow_t *second;
    check_not_null(queue);
    queue_capture_init(&first_capture, 0);
    queue_capture_init(&second_capture, 0);
    first = queue_make_source_flow(queue, &first_capture);
    second = queue_make_source_flow(queue, &second_capture);
    check_not_null(first);
    check_not_null(second);
    check_int_eq(turbo_flow_start(first), TURBO_OK);
    check_int_eq(turbo_flow_start(second), TURBO_EBUSY);
    turbo_sleep_ms(20);
    check_int_eq(turbo_flow_stop(first), TURBO_OK);
    check_int_eq(atomic_load_explicit(&first_capture.called, memory_order_acquire), 0);
    turbo_flow_destroy(second);
    turbo_flow_destroy(first);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("validates bounded configuration and publishes schema metadata") {
    turbo_flow_queue_config_t config;
    turbo_flow_queue_t *queue;
    turbo_flow_t *flow;
    const turbo_flow_adapter_schema_t *schema;
    memset(&config, 0, sizeof(config));
    config.resource_uid = "queue:invalid";
    config.owner_name = "invalid-queue";
    check_null(turbo_flow_queue_create(&config));
    config.capacity = TURBO_FLOW_QUEUE_MAX_CAPACITY + 1u;
    config.max_payload_size = 1;
    check_null(turbo_flow_queue_create(&config));
    config.capacity = 1;
    config.full_policy = TURBO_FLOW_QUEUE_FULL_BLOCK;
    check_null(turbo_flow_queue_create(&config));
    queue = queue_create(1, 8, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    flow = turbo_flow_create();
    check_not_null(queue);
    check_not_null(flow);
    check_int_eq(turbo_flow_queue_register_sink_adapter(flow, "queue.sink", queue), TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "queue.sink");
    check_not_null(schema);
    check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_QUEUE);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    check_size_eq(schema->field_count, 6);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("projects one shared queue as typed resources without becoming a connection") {
    static const char *dsl = "source dequeue adapter queue.source operation "
                             TURBO_FLOW_QUEUE_DEQUEUE_OPERATION
                             " resource \"test-queue\"\n"
                             "stage enqueue adapter queue.sink operation "
                             TURBO_FLOW_QUEUE_ENQUEUE_OPERATION
                             " resource \"test-queue\"\n"
                             "stage main {\n"
                             "  dequeue -> enqueue\n"
                             "}\n";
    turbo_flow_queue_t *queue = queue_create(4, 32, TURBO_FLOW_QUEUE_FULL_FAIL, 0);
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_resource_snapshot_t resource = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    turbo_flow_connection_snapshot_t connection;
    check_not_null(queue);
    check_not_null(flow);
    check_int_eq(turbo_flow_queue_register_source_adapter(flow, "queue.source", queue), TURBO_OK);
    check_int_eq(turbo_flow_queue_register_sink_adapter(flow, "queue.sink", queue), TURBO_OK);
    check_str_eq(turbo_flow_adapter_operation_module(
                     flow, "queue.source", TURBO_FLOW_QUEUE_DEQUEUE_OPERATION),
                 TURBO_FLOW_QUEUE_MODULE);
    check_str_eq(turbo_flow_adapter_operation_resource(
                     flow, "queue.sink", TURBO_FLOW_QUEUE_ENQUEUE_OPERATION),
                 "test-queue");
    check_not_null(turbo_flow_find_primitive(flow, "test-queue"));
    check_size_eq(turbo_flow_resource_count(flow), 1);
    check_size_eq(turbo_flow_resource_metadata_count(flow), 1);
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 0, &resource), TURBO_OK);
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0, &metadata), TURBO_OK);
    check_int_eq(
        turbo_flow_resource_document_at(flow, 0, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_int_eq(resource.kind, TURBO_FLOW_RESOURCE_QUEUE_BUFFER);
    check_str_eq(resource.uid, "queue:test");
    check_str_eq(resource.owner_name, "test-queue");
    check_str_eq(resource.uid, metadata.uid);
    check_size_eq(resource.load, 0);
    check_size_eq(resource.capacity, 4);
    check_false(resource.saturated);
    check_str_eq(document.schema->type_name, "QueueStatus");
    turbo_flow_resource_document_cleanup(&document);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_ENOTSUP);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 1, &connection), TURBO_ENOTSUP);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("keeps stable multi-claim views and restores original memory enqueue order") {
    turbo_flow_queue_t *queue = queue_create(4u, 32u, TURBO_FLOW_QUEUE_FULL_FAIL, 0u);
    turbo_flow_queue_claim_owner_config_t claims = TURBO_FLOW_QUEUE_CLAIM_OWNER_CONFIG_INIT;
    turbo_flow_queue_claim_t first = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_claim_t second = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_claim_t third = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_claim_t replay = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_t *sink;
    const turbo_flow_msg_t *first_view;
    const turbo_flow_msg_t *third_view;
    check_not_null(queue);
    claims.max_active_claims = 3u;
    check_int_eq(turbo_flow_queue_configure_claims(queue, &claims), TURBO_OK);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, "one", 3u, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, "two", 3u, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, "three", 5u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &first), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &second), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &third), TURBO_OK);
    first_view = first.message;
    third_view = third.message;
    replay = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    check_int_eq(turbo_flow_queue_claim(queue, &replay), TURBO_EBUSY);
    check_int_eq(turbo_flow_queue_claim_ack(queue, second.token), TURBO_OK);
    check_ptr_eq(first.message, first_view);
    check_ptr_eq(third.message, third_view);
    check_mem_eq(first.message->payload.data, "one", 3u);
    check_mem_eq(third.message->payload.data, "three", 5u);
    check_int_eq(queue_publish(sink, "four", 4u, NULL), TURBO_OK);

    check_int_eq(turbo_flow_queue_claim_requeue(queue, third.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim_requeue(queue, first.token), TURBO_OK);
    replay = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    check_int_eq(turbo_flow_queue_claim(queue, &replay), TURBO_OK);
    check_mem_eq(replay.message->payload.data, "one", 3u);
    check_int_eq(turbo_flow_queue_claim_ack(queue, replay.token), TURBO_OK);
    replay = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    check_int_eq(turbo_flow_queue_claim(queue, &replay), TURBO_OK);
    check_mem_eq(replay.message->payload.data, "three", 5u);
    check_int_eq(turbo_flow_queue_claim_ack(queue, replay.token), TURBO_OK);
    replay = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
    check_int_eq(turbo_flow_queue_claim(queue, &replay), TURBO_OK);
    check_mem_eq(replay.message->payload.data, "four", 4u);
    check_int_eq(turbo_flow_queue_claim_ack(queue, replay.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim_ack(queue, first.token), TURBO_EALREADY);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("drops a direct claim without manufacturing a delivery ACK") {
    turbo_flow_queue_t *queue = queue_create(1u, 16u, TURBO_FLOW_QUEUE_FULL_FAIL, 0u);
    turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_queue_ack_snapshot_t acknowledgements = TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_t *sink;
    check_not_null(queue);
    sink = queue_make_sink_flow(queue);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, "drop", 4u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim_settler(queue, &settler), TURBO_OK);
    check_int_eq(settler.drop(settler.ctx, claim.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim_ack(queue, claim.token), TURBO_EALREADY);
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0u);
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_uint_eq(acknowledgements.accept_acks, 1u);
    check_uint_eq(acknowledgements.delivery_acks, 0u);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("migrates legacy SQLite state and settles bounded multi-claims atomically") {
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_queue_t *memory = queue_create(2u, 16u, TURBO_FLOW_QUEUE_FULL_FAIL, 0u);
    turbo_flow_queue_t *sqlite;
    turbo_flow_queue_claim_owner_config_t claims = TURBO_FLOW_QUEUE_CLAIM_OWNER_CONFIG_INIT;
    turbo_flow_queue_claim_t first = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_claim_t second = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_claim_t replay = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    uint8_t loaded[16];
    size_t loaded_size = 0u;
    static const uint8_t accepted[] = {'a', 'c', 'c', 'e', 'p', 't'};
    static const uint8_t completed[] = {'d', 'o', 'n', 'e'};
    static const uint8_t requeued[] = {'r', 'e', 't', 'r', 'y'};
    turbo_flow_t *sink = queue_make_sink_flow(memory);
    check_not_null(memory);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, "a", 1u, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, "b", 1u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(memory, &first), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(memory, &second), TURBO_EBUSY);
    check_int_eq(turbo_flow_queue_claim_requeue(memory, first.token), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(memory), TURBO_OK);

    queue_test_database_path(path, sizeof(path));
    queue_create_legacy_sqlite_schema(path);
    sqlite = queue_create_sqlite(path, 2u);
    check_not_null(sqlite);
    check_int_eq(queue_sqlite_schema_version(path), 2);
    claims.max_active_claims = 2u;
    check_int_eq(turbo_flow_queue_configure_claims(sqlite, &claims), TURBO_OK);
    sink = queue_make_sink_flow(sqlite);
    check_not_null(sink);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, "one", 3u, NULL), TURBO_OK);
    check_int_eq(queue_publish(sink, "two", 3u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(sqlite, &first), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(sqlite, &second), TURBO_OK);
    check_mem_eq(first.message->payload.data, "one", 3u);
    check_mem_eq(second.message->payload.data, "two", 3u);
    check_int_eq(turbo_flow_queue_claim_settler(sqlite, &settler), TURBO_OK);
    check_not_null(settler.load_state);
    check_not_null(settler.commit_state);
    check_true(settler.max_state_size >= sizeof(accepted));
    check_int_eq(settler.commit_state(settler.ctx, 0u, TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY,
                                      "fmq:sqlite:test", accepted, sizeof(accepted)),
                 TURBO_OK);
    check_int_eq(
        settler.load_state(settler.ctx, "fmq:sqlite:test", loaded, sizeof(loaded), &loaded_size),
        TURBO_OK);
    check_size_eq(loaded_size, sizeof(accepted));
    check_mem_eq(loaded, accepted, sizeof(accepted));
    queue_apply_sqlite_ack_without_local_release(path, "fmq:sqlite:test", completed,
                                                 sizeof(completed));
    check_int_eq(settler.commit_state(settler.ctx, first.token, TURBO_FLOW_CLAIM_COMMIT_ACK,
                                      "fmq:sqlite:test", completed, sizeof(completed)),
                 TURBO_OK);
    check_int_eq(settler.commit_state(settler.ctx, second.token, TURBO_FLOW_CLAIM_COMMIT_REQUEUE,
                                      "fmq:sqlite:test", requeued, sizeof(requeued)),
                 TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(sqlite), TURBO_OK);

    sqlite = queue_create_sqlite(path, 2u);
    check_not_null(sqlite);
    claims.max_active_claims = 2u;
    check_int_eq(turbo_flow_queue_configure_claims(sqlite, &claims), TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(sqlite, &replay), TURBO_OK);
    check_mem_eq(replay.message->payload.data, "two", 3u);
    settler = (turbo_flow_claim_settler_t)TURBO_FLOW_CLAIM_SETTLER_INIT;
    check_int_eq(turbo_flow_queue_claim_settler(sqlite, &settler), TURBO_OK);
    check_int_eq(settler.commit_state(settler.ctx, replay.token, TURBO_FLOW_CLAIM_COMMIT_DROP,
                                      "fmq:sqlite:test", completed, sizeof(completed)),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_snapshot(sqlite, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0u);
    check_int_eq(turbo_flow_queue_destroy(sqlite), TURBO_OK);
    queue_set_sqlite_schema_version(path, 3);
    check_null(queue_create_sqlite(path, 2u));
    queue_remove_database(path);
  }

  it("creates and binds a push_pull memory channel entirely from YAML") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  orders:\n"
                               "    kind: queue\n"
                               "    config:\n"
                               "      backend: memory\n"
                               "      pattern: push_pull\n"
                               "      resource_uid: queue:orders\n"
                               "      owner_name: orders-queue\n"
                               "      capacity: 4\n"
                               "      max_active_claims: 3\n"
                               "      max_payload_size: 64\n"
                               "      full_policy: fail\n"
                               "adapters:\n"
                               "  queue.sink:\n"
                               "    kind: queue\n"
                               "    config:\n"
                               "      channel: orders\n"
                               "      role: sink\n"
                               "  queue.source:\n"
                               "    kind: queue\n"
                               "    config:\n"
                               "      channel: orders\n"
                               "      role: source\n";
    static const char unsupported[] =
        "version: 1\nchannels:\n  events:\n    kind: queue\n    config:\n"
        "      backend: memory\n      pattern: pub_sub\n"
        "      resource_uid: queue:events\n      owner_name: events-queue\n"
        "      capacity: 4\n      max_payload_size: 64\n      full_policy: fail\n"
        "adapters: {}\n";
    static const char sqlite_backend[] =
        "version: 1\nchannels:\n  events:\n    kind: queue\n    config:\n"
        "      backend: sqlite\n      pattern: push_pull\n"
        "      resource_uid: queue:events\n      owner_name: events-queue\n"
        "      capacity: 4\n      max_payload_size: 64\n      full_policy: fail\n"
        "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_queue_t *queue = NULL;
    turbo_flow_t *sink;
    turbo_flow_t *source;
    queue_capture_t capture;
    char payload[] = "configured";
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "orders", &queue, &error), TURBO_OK);
    check_not_null(queue);
    queue_capture_init(&capture, 0);
    sink = queue_make_sink_flow_resolved(queue, resolved, &error);
    source = queue_make_source_flow_resolved(queue, &capture, resolved, &error);
    check_not_null(sink);
    check_not_null(source);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(queue_publish(sink, payload, sizeof(payload) - 1u, NULL), TURBO_OK);
    check_int_eq(turbo_flow_start(source), TURBO_OK);
    queue_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_mem_eq(capture.payloads[0], payload, sizeof(payload) - 1u);
    check_int_eq(turbo_flow_stop(source), TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(source);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    queue = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(
        turbo_flow_config_resolve_yaml(unsupported, sizeof(unsupported) - 1u, &resolved, &error),
        TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "events", &queue, &error),
                 TURBO_ENOTSUP);
    check_str_eq(error.path, "$.channels.events.config.pattern");
    check_null(queue);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(sqlite_backend, sizeof(sqlite_backend) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "events", &queue, &error),
                 TURBO_ENOTSUP);
    check_str_eq(error.path, "$.channels.events.config.backend");
    check_null(queue);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("keeps the FlowQueue and Redis Stream YAML example resolvable") {
    char example_path[1024];
    char *yaml;
    size_t yaml_len = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_queue_t *queue = NULL;
    const char *adapter_name = NULL;
    (void)snprintf(example_path, sizeof(example_path), "%s/examples/queue.yml",
                   TURBO_FLOW_QUEUE_SOURCE_DIR);
    yaml = tt_read_file(example_path, &yaml_len);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_len, &resolved, &error), TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "orders", &queue, &error), TURBO_OK);
    check_int_eq(
        turbo_flow_resolved_config_profile_adapter(resolved, "orders", "enqueue", &adapter_name),
        TURBO_OK);
    check_str_eq(adapter_name, "queue.orders.sink");
    check_int_eq(turbo_flow_resolved_config_profile_adapter(
                     resolved, "durable-events", "enqueue", &adapter_name),
                 TURBO_OK);
    check_str_eq(adapter_name, "redis.events.sink");
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    free(yaml);
  }
}
