#include "libpq-fe.h"
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

typedef struct pgsql_live_capture_s {
  atomic_int called;
  atomic_int result;
  unsigned char payload[PGSQL_LIVE_MAX_PAYLOAD_SIZE];
  size_t payload_size;
} pgsql_live_capture_t;

typedef struct pgsql_live_protocol_owner_s {
  int called;
  turbo_flow_protocol_settlement_request_t request;
} pgsql_live_protocol_owner_t;

static int pgsql_live_capture(turbo_flow_msg_t *msg, void *ctx) {
  pgsql_live_capture_t *capture = (pgsql_live_capture_t *)ctx;
  if (!capture || !msg || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_size = msg->payload.len;
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
    } else {
      check_size_eq(second.payload_size, sizeof(first_payload));
      check_mem_eq(second.payload, first_payload, sizeof(first_payload));
    }
    check_int_eq(turbo_flow_stop(source_second), TURBO_OK);
    check_int_eq(turbo_flow_stop(source_first), TURBO_OK);
    turbo_flow_destroy(source_second);
    turbo_flow_destroy(source_first);
    check_int_eq(pgsql_live_count(conninfo, outbox_name, &depth), TURBO_OK);
    check_size_eq(depth, 0u);
  }
}
