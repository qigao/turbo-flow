#include "flowie_record_store_contract.h"
#include "flowie_record_store_endurance.h"
#include "libpq-fe.h"
#include "pgsql_storage_test_helpers.h"
#include "tinytest.h"
#include "turbo_flow_pgsql.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PGSQL_LIVE_WAIT_ITERATIONS 400
#define PGSQL_LIVE_MAX_PAYLOAD_SIZE 256u
#define PGSQL_LIVE_MESSAGE_TYPE 3u
#define PGSQL_LIVE_MESSAGE_FLAGS UINT32_C(0x0005000d)

typedef struct pgsql_live_capture_s {
  atomic_int called;
  atomic_int result;
  unsigned char payload[PGSQL_LIVE_MAX_PAYLOAD_SIZE];
  size_t payload_size;
  uint32_t message_type;
  uint32_t message_flags;
  turbo_flow_protocol_origin_t origin;
  int has_origin;
} pgsql_live_capture_t;

typedef struct pgsql_live_protocol_owner_s {
  int called;
  turbo_flow_protocol_settlement_request_t request;
} pgsql_live_protocol_owner_t;

typedef struct pgsql_live_record_capture_s {
  size_t count;
  uint8_t keys[2][16];
  size_t key_sizes[2];
  uint64_t revisions[2];
  uint8_t values[2][16];
  size_t value_sizes[2];
} pgsql_live_record_capture_t;

static int pgsql_live_record_visit(void *ctx, const turbo_flow_record_view_t *record) {
  pgsql_live_record_capture_t *capture = (pgsql_live_record_capture_t *)ctx;
  size_t index;
  if (!capture || !record || capture->count >= 2u || record->key_size > 16u ||
      record->value_size > 16u)
    return TURBO_ENOSPC;
  index = capture->count++;
  memcpy(capture->keys[index], record->key, record->key_size);
  capture->key_sizes[index] = record->key_size;
  capture->revisions[index] = record->revision;
  if (record->value_size != 0u) memcpy(capture->values[index], record->value, record->value_size);
  capture->value_sizes[index] = record->value_size;
  return TURBO_OK;
}

static int pgsql_live_capture(turbo_flow_msg_t *msg, void *ctx) {
  pgsql_live_capture_t *capture = (pgsql_live_capture_t *)ctx;
  if (!capture || !msg || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_size = msg->payload.len;
  capture->message_type = msg->type;
  capture->message_flags = msg->flags;
  if (turbo_flow_msg_protocol_origin(msg)) {
    capture->origin = *turbo_flow_msg_protocol_origin(msg);
    capture->has_origin = 1;
  }
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return atomic_load_explicit(&capture->result, memory_order_acquire);
}

static int pgsql_live_settle(void *ctx, const turbo_flow_protocol_route_t *route,
                             const turbo_flow_protocol_settlement_request_t *request) {
  pgsql_live_protocol_owner_t *owner = (pgsql_live_protocol_owner_t *)ctx;
  if (!owner || !route || !request) return TURBO_EINVAL;
  owner->called += 1;
  owner->request = *request;
  return TURBO_OK;
}

static int pgsql_live_wait_total(const pgsql_live_capture_t *first,
                                 const pgsql_live_capture_t *second, int expected) {
  for (int i = 0; i < PGSQL_LIVE_WAIT_ITERATIONS; ++i) {
    int total = atomic_load_explicit(&first->called, memory_order_acquire);
    if (second) total += atomic_load_explicit(&second->called, memory_order_acquire);
    if (total >= expected) return TURBO_OK;
    turbo_sleep_ms(5u);
  }
  return TURBO_ETIMEDOUT;
}

static turbo_flow_t *pgsql_live_sink_flow(const turbo_flow_pgsql_outbox_config_t *config) {
  static const char graph[] = "source input\n"
                              "stage persist adapter pg.outbox\n"
                              "stage main {\n"
                              "  input -> persist\n"
                              "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_pgsql_register_outbox_adapter(flow, "pg.outbox", config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *pgsql_live_source_flow(const turbo_flow_pgsql_outbox_config_t *config,
                                            const char *adapter_name,
                                            pgsql_live_capture_t *capture) {
  char graph[512];
  turbo_flow_t *flow = turbo_flow_create();
  int written;
  if (!flow) return NULL;
  written = snprintf(graph, sizeof(graph),
                     "source durable adapter %s\n"
                     "stage capture\n"
                     "stage main {\n"
                     "  durable -> capture\n"
                     "}\n",
                     adapter_name);
  if (written < 0 || (size_t)written >= sizeof(graph) ||
      turbo_flow_pgsql_register_outbox_adapter(flow, adapter_name, config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", pgsql_live_capture, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, (size_t)written) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int pgsql_live_publish(turbo_flow_t *flow, const void *payload, size_t payload_size,
                              const turbo_flow_protocol_route_t *route,
                              const turbo_flow_protocol_settlement_envelope_t *settlement,
                              turbo_flow_publish_result_t *result) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.type = PGSQL_LIVE_MESSAGE_TYPE;
  msg.flags = PGSQL_LIVE_MESSAGE_FLAGS;
  msg.owned_payload = tstr_new_len(payload, payload_size);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  if (route) {
    rc = turbo_flow_msg_set_protocol_route(&msg, route);
    if (rc == TURBO_OK) rc = turbo_flow_msg_set_protocol_settlement(&msg, settlement);
  } else rc = TURBO_OK;
  if (rc == TURBO_OK) rc = turbo_flow_publish_ex(flow, "input", &msg, result);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int pgsql_live_count(const char *conninfo, const char *outbox_name, size_t *count) {
  static const char sql[] = "SELECT count(*)::text FROM turbo_flow_outbox WHERE outbox_name=$1";
  const char *values[1] = {outbox_name};
  PGconn *connection = PQconnectdb(conninfo);
  PGresult *result = NULL;
  char *end = NULL;
  unsigned long long value;
  int rc = TURBO_EIO;
  if (!connection || PQstatus(connection) != CONNECTION_OK) goto done;
  result = PQexecParams(connection, sql, 1, NULL, values, NULL, NULL, 0);
  if (!result || PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1 ||
      PQnfields(result) != 1 || PQgetisnull(result, 0, 0))
    goto done;
  value = strtoull(PQgetvalue(result, 0, 0), &end, 10);
  if (!end || *end != '\0' || value > SIZE_MAX) {
    rc = TURBO_EPROTO;
    goto done;
  }
  *count = (size_t)value;
  rc = TURBO_OK;

done:
  if (result) PQclear(result);
  if (connection) PQfinish(connection);
  return rc;
}

spec("turbo_flow_pgsql_live") {
  it("MQTT-STORE-010 runs the provider-neutral record trace through PostgreSQL") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char namespace_name[128];
    turbo_flow_pgsql_record_store_config_t config = TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    pgsql_test_storage_t storage = {0};
    flowie_record_store_contract_result_t result = {0};
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(namespace_name, sizeof(namespace_name), "turboflow_provider_neutral_%llu",
                   (unsigned long long)turbo_hrtime());
    config.conninfo = conninfo;
    config.namespace_name = namespace_name;
    config.max_key_size = 64u;
    config.max_value_size = 64u;
    config.max_batch_size = 2u;
    config.max_records = 4u;
    config.create_table = 1;
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, NULL),
                 TURBO_OK);
    check_int_eq(flowie_record_store_contract_run(&store, &result), TURBO_OK);
    check_size_eq(result.restored_count, 1u);
    check_uint_eq(result.restored_revision, 2u);
    check_int_eq(result.duplicate_create_status, TURBO_EBUSY);
    check_int_eq(result.stale_update_status, TURBO_EBUSY);
    check_int_eq(result.stale_delete_status, TURBO_EBUSY);
    check_true(result.empty_after_delete);
    check_size_eq(result.mqtt_wire_size, 12u);
    check_true(result.mqtt_wire_equal);
    pgsql_test_record_store_close(&storage);
  }

  it("MQTT-STORE-ENDURANCE-001 runs the shared revision trace through PostgreSQL") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char namespace_name[128];
    turbo_flow_pgsql_record_store_config_t config = TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    pgsql_test_storage_t storage = {0};
    flowie_record_store_endurance_result_t result = {0};
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(namespace_name, sizeof(namespace_name), "turboflow_record_endurance_%llu",
                   (unsigned long long)turbo_hrtime());
    config.conninfo = conninfo;
    config.namespace_name = namespace_name;
    config.max_key_size = FLOWIE_RECORD_ENDURANCE_KEY_SIZE;
    config.max_value_size = FLOWIE_RECORD_ENDURANCE_VALUE_SIZE;
    config.max_batch_size = FLOWIE_RECORD_ENDURANCE_BATCH_SIZE;
    config.max_records = FLOWIE_RECORD_ENDURANCE_RECORDS;
    config.create_table = 1;
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, NULL),
                 TURBO_OK);
    check_int_eq(flowie_record_store_endurance_run(&store, &result), TURBO_OK);
    check_size_eq(result.successful_commits, 40u);
    check_size_eq(result.scans, 39u);
    check_size_eq(result.conflicts, 4u);
    check_size_eq(result.final_count, 0u);
    check_true(result.durable);
    pgsql_test_record_store_close(&storage);
  }

  it("MQTT-SOAK-005 MQTT-STORE-007 resolves PostgreSQL outage and lost replies by revision") {
    static const char unavailable_conninfo[] =
        "host=127.0.0.1 port=1 dbname=postgres connect_timeout=1";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char namespace_name[128];
    turbo_flow_pgsql_record_store_config_t config = TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    pgsql_test_storage_t storage = {0};
    flowie_record_store_recovery_result_t recovery = {0};
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(namespace_name, sizeof(namespace_name), "turboflow_recovery_%llu",
                   (unsigned long long)turbo_hrtime());
    config.conninfo = unavailable_conninfo;
    config.namespace_name = namespace_name;
    config.max_key_size = 16u;
    config.max_value_size = 16u;
    config.max_batch_size = 1u;
    config.max_records = 2u;
    config.create_table = 1;
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, NULL),
                 TURBO_EIO);
    check_null(store.ctx);

    config.conninfo = conninfo;
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, NULL),
                 TURBO_OK);
    check_int_eq(flowie_record_store_recovery_contract_run(&store, &recovery), TURBO_OK);
    check_int_eq(recovery.timeout_status, TURBO_ETIMEDOUT);
    check_int_eq(recovery.lost_reply_status, TURBO_EIO);
    check_int_eq(recovery.retry_status, TURBO_EBUSY);
    check_uint_eq(recovery.revision_after_timeout, 1u);
    check_uint_eq(recovery.revision_after_lost_reply, 2u);
    check_uint_eq(recovery.revision_after_recovery, 3u);
    pgsql_test_record_store_close(&storage);
  }

  it("MQTT-STORE-003/010 commits bounded revision-checked PostgreSQL record batches") {
    static const uint8_t key_one[] = {'a', 0u, '1'};
    static const uint8_t key_two[] = {'b', 0u, '2'};
    static const uint8_t key_three[] = {'c', 0u, '3'};
    static const uint8_t value_one[] = {'o', 'n', 'e'};
    static const uint8_t value_next[] = {'n', 'e', 'x', 't'};
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char namespace_name[128];
    turbo_flow_pgsql_record_store_config_t config = TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    pgsql_test_storage_t storage = {0};
    turbo_flow_record_mutation_t mutations[2] = {TURBO_FLOW_RECORD_MUTATION_INIT,
                                                 TURBO_FLOW_RECORD_MUTATION_INIT};
    pgsql_live_record_capture_t capture;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(namespace_name, sizeof(namespace_name), "turboflow_record_live_%llu",
                   (unsigned long long)turbo_hrtime());
    config.conninfo = conninfo;
    config.namespace_name = namespace_name;
    config.max_key_size = 16u;
    config.max_value_size = 16u;
    config.max_batch_size = 2u;
    config.max_records = 2u;
    config.create_table = 1;
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, NULL),
                 TURBO_OK);
    check_bits(store.capabilities, TURBO_FLOW_RECORD_STORE_DURABLE);
    check_bits(store.capabilities, TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, pgsql_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 0u);

    mutations[0].key = key_one;
    mutations[0].key_size = sizeof(key_one);
    mutations[0].next_revision = 1u;
    mutations[0].value = value_one;
    mutations[0].value_size = sizeof(value_one);
    check_int_eq(store.commit(store.ctx, mutations, 1u), TURBO_OK);
    check_int_eq(store.commit(store.ctx, mutations, 1u), TURBO_EBUSY);

    mutations[0].expected_revision = 1u;
    mutations[0].next_revision = 2u;
    mutations[0].value = value_next;
    mutations[0].value_size = sizeof(value_next);
    mutations[1].key = key_two;
    mutations[1].key_size = sizeof(key_two);
    mutations[1].next_revision = 1u;
    mutations[1].value = value_one;
    mutations[1].value_size = sizeof(value_one);
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_OK);

    mutations[0] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
    mutations[0].key = key_three;
    mutations[0].key_size = sizeof(key_three);
    mutations[0].next_revision = 1u;
    mutations[0].value = value_one;
    mutations[0].value_size = sizeof(value_one);
    check_int_eq(store.commit(store.ctx, mutations, 1u), TURBO_ENOSPC);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, pgsql_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 2u);
    check_uint_eq(capture.revisions[0], 2u);
    check_uint_eq(capture.revisions[1], 1u);

    mutations[0] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
    mutations[0].kind = TURBO_FLOW_RECORD_DELETE;
    mutations[0].key = key_one;
    mutations[0].key_size = sizeof(key_one);
    mutations[0].expected_revision = 2u;
    mutations[1] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
    mutations[1].kind = TURBO_FLOW_RECORD_DELETE;
    mutations[1].key = key_two;
    mutations[1].key_size = sizeof(key_two);
    mutations[1].expected_revision = 1u;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_OK);
    pgsql_test_record_store_close(&storage);

    config.create_table = 0;
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, NULL),
                 TURBO_OK);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(store.scan(store.ctx, pgsql_live_record_visit, &capture), TURBO_OK);
    check_size_eq(capture.count, 0u);
    pgsql_test_record_store_close(&storage);
  }

  it("commits DURABLE, preserves failed delivery, recovers, and isolates concurrent sources") {
    static const unsigned char first_payload[] = {'f', 'i', 'r', 's', 't'};
    static const unsigned char second_payload[] = {'s', 'e', 'c', 'o', 'n', 'd'};
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char outbox_name[128];
    turbo_flow_pgsql_outbox_config_t config = TURBO_FLOW_PGSQL_OUTBOX_CONFIG_INIT;
    turbo_flow_protocol_route_owner_ops_t owner_ops = TURBO_FLOW_PROTOCOL_ROUTE_OWNER_OPS_INIT;
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_protocol_settlement_envelope_t envelope =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT;
    turbo_flow_publish_result_t publish_result = TURBO_FLOW_PUBLISH_RESULT_INIT;
    pgsql_live_protocol_owner_t protocol_owner = {0};
    pgsql_live_capture_t failed;
    pgsql_live_capture_t first;
    pgsql_live_capture_t second;
    turbo_flow_t *sink;
    turbo_flow_t *source;
    turbo_flow_t *source_first;
    turbo_flow_t *source_second;
    size_t depth = 0u;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(outbox_name, sizeof(outbox_name), "turboflow_live_%llu",
                   (unsigned long long)turbo_hrtime());
    config.conninfo = conninfo;
    config.outbox_name = outbox_name;
    config.capacity = 1u;
    config.max_payload_size = PGSQL_LIVE_MAX_PAYLOAD_SIZE;
    config.poll_interval_ms = 5u;
    config.claim_scan_limit = 8u;
    config.create_table = 1;
    owner_ops.settle = pgsql_live_settle;
    route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    route.owner_instance_id = 501u;
    route.session_id = 502u;
    route.session_generation = 503u;
    envelope.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    envelope.message.protocol_version = TURBO_FLOW_MQTT_PROTOCOL_5_0;
    envelope.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
    envelope.message.qos = TURBO_FLOW_PROTOCOL_QOS_1;
    envelope.message.packet_id = 504u;
    envelope.message.session_generation = route.session_generation;
    envelope.requested_point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;

    sink = pgsql_live_sink_flow(&config);
    check_not_null(sink);
    check_int_eq(turbo_flow_register_protocol_route_owner(
                     sink, route.protocol, route.owner_instance_id, &owner_ops, &protocol_owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_start(sink), TURBO_OK);
    check_int_eq(pgsql_live_publish(sink, first_payload, sizeof(first_payload), &route, &envelope,
                                    &publish_result),
                 TURBO_OK);
    check_uint_eq(publish_result.protocol_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(protocol_owner.called, 1);
    check_uint_eq(protocol_owner.request.point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(pgsql_live_publish(sink, second_payload, sizeof(second_payload), NULL, NULL,
                                    &publish_result),
                 TURBO_ENOSPC);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);

    memset(&failed, 0, sizeof(failed));
    atomic_init(&failed.called, 0);
    atomic_init(&failed.result, TURBO_EIO);
    config.role = TURBO_FLOW_PGSQL_OUTBOX_SOURCE;
    source = pgsql_live_source_flow(&config, "pg.failed", &failed);
    check_not_null(source);
    check_int_eq(turbo_flow_start(source), TURBO_OK);
    check_int_eq(pgsql_live_wait_total(&failed, NULL, 1), TURBO_OK);
    check_int_eq(turbo_flow_stop(source), TURBO_OK);
    turbo_flow_destroy(source);
    check_int_eq(pgsql_live_count(conninfo, outbox_name, &depth), TURBO_OK);
    check_size_eq(depth, 1u);

    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    atomic_init(&first.called, 0);
    atomic_init(&first.result, TURBO_OK);
    atomic_init(&second.called, 0);
    atomic_init(&second.result, TURBO_OK);
    source_first = pgsql_live_source_flow(&config, "pg.source.first", &first);
    source_second = pgsql_live_source_flow(&config, "pg.source.second", &second);
    check_not_null(source_first);
    check_not_null(source_second);
    check_int_eq(turbo_flow_start(source_first), TURBO_OK);
    check_int_eq(turbo_flow_start(source_second), TURBO_OK);
    check_int_eq(pgsql_live_wait_total(&first, &second, 1), TURBO_OK);
    turbo_sleep_ms(50u);
    check_int_eq(atomic_load_explicit(&first.called, memory_order_acquire) +
                     atomic_load_explicit(&second.called, memory_order_acquire),
                 1);
    if (atomic_load_explicit(&first.called, memory_order_acquire) == 1) {
      check_size_eq(first.payload_size, sizeof(first_payload));
      check_mem_eq(first.payload, first_payload, sizeof(first_payload));
      check_uint_eq(first.message_type, PGSQL_LIVE_MESSAGE_TYPE);
      check_uint_eq(first.message_flags, PGSQL_LIVE_MESSAGE_FLAGS);
      check_true(first.has_origin);
      check_uint_eq(first.origin.protocol, TURBO_FLOW_PROTOCOL_MQTT);
      check_uint_eq(first.origin.protocol_version, TURBO_FLOW_MQTT_PROTOCOL_5_0);
      check_uint_eq(first.origin.session_id, route.session_id);
    } else {
      check_size_eq(second.payload_size, sizeof(first_payload));
      check_mem_eq(second.payload, first_payload, sizeof(first_payload));
      check_uint_eq(second.message_type, PGSQL_LIVE_MESSAGE_TYPE);
      check_uint_eq(second.message_flags, PGSQL_LIVE_MESSAGE_FLAGS);
      check_true(second.has_origin);
      check_uint_eq(second.origin.protocol, TURBO_FLOW_PROTOCOL_MQTT);
      check_uint_eq(second.origin.protocol_version, TURBO_FLOW_MQTT_PROTOCOL_5_0);
      check_uint_eq(second.origin.session_id, route.session_id);
    }
    check_int_eq(turbo_flow_stop(source_second), TURBO_OK);
    check_int_eq(turbo_flow_stop(source_first), TURBO_OK);
    turbo_flow_destroy(source_second);
    turbo_flow_destroy(source_first);
    check_int_eq(pgsql_live_count(conninfo, outbox_name, &depth), TURBO_OK);
    check_size_eq(depth, 0u);
  }
}
