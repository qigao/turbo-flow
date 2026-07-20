#include "turbo_flow_observe.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <string.h>

typedef struct observe_adapter_state_s {
  int starts;
  int consumes;
  int stops;
  int start_status;
} observe_adapter_state_t;

typedef struct observe_log_state_s {
  int called;
  char stage[64];
  char preview[64];
  size_t payload_size;
  size_t preview_bytes;
  int redacted;
} observe_log_state_t;

typedef struct observe_resource_state_s {
  turbo_flow_domain_t domain;
  turbo_flow_resource_kind_t kind;
  const char *uid;
  const char *owner_name;
  uint64_t load;
  uint64_t capacity;
} observe_resource_state_t;

typedef struct observe_export_state_s {
  tstr_t text;
  size_t metrics;
  int saw_resource;
} observe_export_state_t;

typedef struct observe_batch_state_s {
  uint64_t fail_id;
  int fail_status;
} observe_batch_state_t;

static int observe_export_write(void *ctx, const char *data, size_t len) {
  observe_export_state_t *state = (observe_export_state_t *)ctx;
  if (!state || (len > 0u && !data)) return TURBO_EINVAL;
  state->text = tstr_cat_len(state->text, data, len);
  return state->text ? TURBO_OK : TURBO_ENOMEM;
}

static int observe_export_metric(void *ctx, const turbo_flow_observe_metric_t *metric) {
  observe_export_state_t *state = (observe_export_state_t *)ctx;
  if (!state || !metric || !metric->name) return TURBO_EINVAL;
  ++state->metrics;
  if (metric->resource_uid && strcmp(metric->resource_uid, "queue:export") == 0)
    state->saw_resource = 1;
  return TURBO_OK;
}

static int observe_stage_ok(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static int observe_stage_fail(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_EIO;
}

static int observe_batch_stage(turbo_flow_msg_t *msg, void *ctx) {
  const observe_batch_state_t *state = (const observe_batch_state_t *)ctx;
  if (!msg || !state) return TURBO_EINVAL;
  return msg->id == state->fail_id ? state->fail_status : TURBO_OK;
}

static int observe_batch_prepare(void *ctx, size_t index, turbo_flow_msg_t *message) {
  (void)ctx;
  if (!message) return TURBO_EINVAL;
  message->id = index + 1u;
  return TURBO_OK;
}

static int observe_adapter_start(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  observe_adapter_state_t *state = (observe_adapter_state_t *)ctx;
  (void)flow;
  (void)stage;
  state->starts += 1;
  return state->start_status;
}

static int observe_adapter_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  observe_adapter_state_t *state = (observe_adapter_state_t *)ctx;
  (void)flow;
  (void)stage;
  (void)msg;
  state->consumes += 1;
  return TURBO_OK;
}

static void observe_adapter_stop(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  observe_adapter_state_t *state = (observe_adapter_state_t *)ctx;
  (void)flow;
  (void)stage;
  state->stops += 1;
}

static int observe_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  (void)ctx;
  out->state = TURBO_FLOW_CONNECTION_READY;
  memcpy(out->endpoint, "tcp://127.0.0.1:9000", sizeof("tcp://127.0.0.1:9000"));
  out->connections_current = 3;
  out->connection_limit = 8;
  out->in_flight_messages = 2;
  out->in_flight_bytes = 128;
  out->last_status = TURBO_OK;
  return TURBO_OK;
}

static int observe_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  observe_resource_state_t *state = (observe_resource_state_t *)ctx;
  if (!state || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  out->domain = state->domain;
  out->kind = state->kind;
  memcpy(out->uid, state->uid, strlen(state->uid) + 1u);
  memcpy(out->owner_name, state->owner_name, strlen(state->owner_name) + 1u);
  out->generation = 1u;
  out->observed_generation = 1u;
  return TURBO_OK;
}

static int observe_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  observe_resource_state_t *state = (observe_resource_state_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!state || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (observe_resource_metadata(ctx, &metadata) != TURBO_OK) return TURBO_EINVAL;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  out->load = state->load;
  out->capacity = state->capacity;
  out->last_status = TURBO_OK;
  return TURBO_OK;
}

static void observe_log_write(void *ctx, const turbo_flow_observe_log_summary_t *summary) {
  observe_log_state_t *state = (observe_log_state_t *)ctx;
  size_t stage_len = strlen(summary->stage_name);
  size_t preview_len = strlen(summary->payload_preview);
  state->called += 1;
  if (stage_len >= sizeof(state->stage)) stage_len = sizeof(state->stage) - 1u;
  if (preview_len >= sizeof(state->preview)) preview_len = sizeof(state->preview) - 1u;
  memcpy(state->stage, summary->stage_name, stage_len);
  state->stage[stage_len] = '\0';
  memcpy(state->preview, summary->payload_preview, preview_len);
  state->preview[preview_len] = '\0';
  state->payload_size = summary->payload_size;
  state->preview_bytes = summary->preview_bytes;
  state->redacted = summary->payload_redacted;
}

static int observe_publish(turbo_flow_t *flow, const char *payload) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup(payload);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

spec("turbo_flow_observe") {
  it("collects message stage and adapter lifecycle counters") {
    static const char *dsl = "source input\n"
                             "stage work\n"
                             "stage output adapter target.sink\n"
                             "stage main {\n"
                             "  input -> work -> output\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_t *second_observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_snapshot_t snapshot;
    turbo_flow_observe_graph_snapshot_t graph_snapshot;
    turbo_flow_observe_control_event_t control_event;
    turbo_flow_observe_stage_snapshot_t first;
    turbo_flow_observe_stage_snapshot_t second;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_resource_provider_ops_t resource_ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
    observe_resource_state_t connection_resource = {
        TURBO_FLOW_DOMAIN_IO_TRANSPORT, TURBO_FLOW_RESOURCE_CONNECTION,
        "connection:observe", "target.sink", 3u, 8u};
    observe_resource_state_t queue_resource = {
        TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
        "queue:observe", "target.sink", 2u, 4u};
    observe_adapter_state_t adapter_state;
    check_not_null(flow);
    check_not_null(observe);
    check_not_null(second_observe);
    memset(&adapter_state, 0, sizeof(adapter_state));
    memset(&adapter_ops, 0, sizeof(adapter_ops));
    adapter_ops.start = observe_adapter_start;
    adapter_ops.consume = observe_adapter_consume;
    adapter_ops.stop = observe_adapter_stop;
    adapter_ops.connection_snapshot = observe_connection_snapshot;
    memset(&schema, 0, sizeof(schema));
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_int_eq(
        turbo_flow_register_adapter_ex(flow, "target.sink", &adapter_ops, &adapter_state, &schema),
        TURBO_OK);
    resource_ops.metadata = observe_resource_metadata;
    resource_ops.snapshot = observe_resource_snapshot;
    check_int_eq(turbo_flow_register_resource_provider(
                     flow, connection_resource.owner_name, &resource_ops, &connection_resource),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_resource_provider(
                     flow, queue_resource.owner_name, &resource_ops, &queue_resource),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "work", observe_stage_ok, NULL, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(second_observe, flow), TURBO_EALREADY);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph_snapshot), TURBO_OK);
    check_int_eq(graph_snapshot.runtime.state, TURBO_FLOW_STATE_STARTED);
    check_int_eq(graph_snapshot.runtime.accepting_publishes, 1);
    check_size_eq(graph_snapshot.runtime.active_publishes, 0);
    check_size_eq(graph_snapshot.runtime.stage_count, 3);
    check_size_eq(graph_snapshot.runtime.edge_count, 2);
    check_size_eq(graph_snapshot.runtime.adapter_count, 1);
    check_size_eq(graph_snapshot.connection_providers, 1);
    check_size_eq(graph_snapshot.connections_current, 3);
    check_size_eq(graph_snapshot.resource_providers, 5);
    check_size_eq(graph_snapshot.connection_resources, 1);
    check_size_eq(graph_snapshot.queue_buffer_resources, 1);
    check_size_eq(graph_snapshot.pool_resources, 0);
    check_size_eq(graph_snapshot.runtime_resources, 1);
    check_size_eq(graph_snapshot.segment_resources, 2);
    check_size_eq(graph_snapshot.protocol_resources, 0);
    check_size_eq(graph_snapshot.storage_resources, 0);
    check_size_eq(graph_snapshot.rule_set_resources, 0);
    check_size_eq(graph_snapshot.saturated_resources, 0);
    check_size_eq(graph_snapshot.resource_load, 5);
    check_size_eq(graph_snapshot.resource_capacity, 12);
    check_int_gt(graph_snapshot.system.cpu_cores, 0);
    check_size_gt(graph_snapshot.system.total_memory_bytes, 0);
    memset(&control_event, 0, sizeof(control_event));
    control_event.kind = TURBO_FLOW_OBSERVE_CONTROL_PEER_CONNECTED;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &control_event), TURBO_OK);
    check_int_eq(turbo_flow_observe_record_control_event(observe, &control_event), TURBO_OK);
    control_event.kind = TURBO_FLOW_OBSERVE_CONTROL_PEER_DISCONNECTED;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &control_event), TURBO_OK);
    check_int_eq(turbo_flow_observe_detach(observe), TURBO_EBUSY);
    check_int_eq(observe_publish(flow, "one"), TURBO_OK);
    check_int_eq(observe_publish(flow, "two"), TURBO_OK);
    {
      turbo_flow_observe_control_facts_snapshot_t facts_snapshot;
      turbo_flow_control_facts_t facts = TURBO_FLOW_CONTROL_FACTS_INIT;
      turbo_flow_runtime_snapshot_t runtime;
      static const char rule[] =
          "when traffic.messages == 2 and system.cpu.cores >= 0 and "
          "graph.resource_load == 5 and graph.queue_buffer_resources == 1 then flow pause";
      check_int_eq(turbo_flow_observe_control_facts(observe, &facts_snapshot, &facts), TURBO_OK);
      check_int_eq(turbo_flow_control_ex(flow, rule, sizeof(rule) - 1u, &facts, NULL), TURBO_OK);
      check_int_eq(turbo_flow_runtime_snapshot(flow, &runtime), TURBO_OK);
      check_int_eq(runtime.accepting_publishes, 0);
      check_int_eq(turbo_flow_resume(flow), TURBO_OK);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(observe_publish(flow, "after-stop"), TURBO_EINVAL);
    check_int_eq(turbo_flow_observe_snapshot(observe, &snapshot), TURBO_OK);
    check_size_eq(snapshot.messages, 2);
    check_size_eq(snapshot.payload_bytes, 6);
    check_size_eq(snapshot.message_errors, 0);
    check_size_eq(snapshot.stage_calls, 4);
    check_size_eq(snapshot.stage_errors, 0);
    check_size_eq(snapshot.adapter_starts, 1);
    check_size_eq(snapshot.adapter_stops, 1);
    check_size_eq(snapshot.adapter_errors, 0);
    check_size_eq(turbo_flow_observe_stage_count(observe), 2);
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph_snapshot), TURBO_OK);
    check_int_eq(graph_snapshot.runtime.state, TURBO_FLOW_STATE_STOPPED);
    check_int_eq(graph_snapshot.runtime.accepting_publishes, 0);
    check_size_eq(graph_snapshot.traffic.payload_bytes, 6);
    check_size_eq(graph_snapshot.connections_total, 2);
    check_size_eq(graph_snapshot.disconnections_total, 1);
    check_size_eq(graph_snapshot.connections_current, 3);
    check_size_eq(graph_snapshot.saturated_pools, 0);
    {
      turbo_flow_connection_snapshot_t connection;
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_OK);
      check_str_eq(connection.adapter_name, "target.sink");
      check_int_eq(connection.adapter_kind, TURBO_FLOW_ADAPTER_KIND_CUSTOM);
      check_int_eq(connection.direction, TURBO_FLOW_ADAPTER_OUTPUT);
      check_int_eq(connection.state, TURBO_FLOW_CONNECTION_READY);
      check_str_eq(connection.endpoint, "tcp://127.0.0.1:9000");
      check_size_eq(connection.connection_limit, 8);
      check_size_eq(connection.in_flight_messages, 2);
      check_size_eq(connection.in_flight_bytes, 128);
    }
    check_int_eq(turbo_flow_observe_stage_snapshot_at(observe, 0, &first), TURBO_OK);
    check_int_eq(turbo_flow_observe_stage_snapshot_at(observe, 1, &second), TURBO_OK);
    check_size_eq(first.calls + second.calls, 4);
    check_int_eq(adapter_state.starts, 1);
    check_int_eq(adapter_state.consumes, 2);
    check_int_eq(adapter_state.stops, 1);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_EBUSY);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph_snapshot), TURBO_EINVAL);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
    check_int_eq(turbo_flow_observe_destroy(second_observe), TURBO_OK);
  }

  it("counts stage failures and bounds per-stage series") {
    static const char *dsl = "source input\n"
                             "stage first\n"
                             "stage fail\n"
                             "stage main {\n"
                             "  input -> first -> fail\n"
                             "}\n";
    turbo_flow_observe_config_t config = {.max_stages = 1};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(&config);
    turbo_flow_observe_snapshot_t snapshot;
    check_not_null(flow);
    check_not_null(observe);
    check_int_eq(turbo_flow_register_stage_ex(flow, "first", observe_stage_ok, NULL, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "fail", observe_stage_fail, NULL, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(observe_publish(flow, "bad"), TURBO_EIO);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_snapshot(observe, &snapshot), TURBO_OK);
    check_size_eq(snapshot.messages, 1);
    check_size_eq(snapshot.message_errors, 1);
    check_size_eq(snapshot.stage_calls, 2);
    check_size_eq(snapshot.stage_errors, 1);
    check_size_eq(snapshot.dropped_stage_series, 1);
    check_size_eq(turbo_flow_observe_stage_count(observe), 1);
    check_int_eq(turbo_flow_observe_detach(observe), TURBO_OK);
    {
      turbo_flow_observe_graph_snapshot_t graph_snapshot;
      check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph_snapshot), TURBO_EINVAL);
    }
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("observes each attempted batch message through the first failure") {
    static const char *dsl = "source input\n"
                             "stage work\n"
                             "stage main {\n"
                             "  input -> work\n"
                             "}\n";
    observe_batch_state_t state = {3u, TURBO_EIO};
    turbo_flow_publish_batch_config_t batch = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
    turbo_flow_observe_snapshot_t snapshot;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    size_t published = SIZE_MAX;

    check_not_null(flow);
    check_not_null(observe);
    check_int_eq(turbo_flow_register_stage_ex(flow, "work", observe_batch_stage, &state, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    batch.message_count = 5u;
    batch.prepare = observe_batch_prepare;
    check_int_eq(turbo_flow_publish_batch(flow, "input", &batch, &published), TURBO_EIO);
    check_size_eq(published, 2u);
    check_int_eq(turbo_flow_observe_snapshot(observe, &snapshot), TURBO_OK);
    check_size_eq(snapshot.messages, 3u);
    check_size_eq(snapshot.message_errors, 1u);
    check_size_eq(snapshot.stage_calls, 3u);
    check_size_eq(snapshot.stage_errors, 1u);

    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("counts adapter start failures") {
    static const char *dsl = "source input\n"
                             "stage output adapter target.sink\n"
                             "stage main {\n"
                             "  input -> output\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_snapshot_t snapshot;
    turbo_flow_adapter_ops_t ops;
    turbo_flow_adapter_schema_t schema;
    observe_adapter_state_t state;
    check_not_null(flow);
    check_not_null(observe);
    memset(&state, 0, sizeof(state));
    state.start_status = TURBO_EIO;
    memset(&ops, 0, sizeof(ops));
    ops.start = observe_adapter_start;
    ops.consume = observe_adapter_consume;
    memset(&schema, 0, sizeof(schema));
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_int_eq(turbo_flow_register_adapter_ex(flow, "target.sink", &ops, &state, &schema),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_EIO);
    check_int_eq(turbo_flow_observe_snapshot(observe, &snapshot), TURBO_OK);
    check_size_eq(snapshot.adapter_starts, 1);
    check_size_eq(snapshot.adapter_errors, 1);
    check_int_eq(turbo_flow_observe_detach(observe), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("records external control-plane events") {
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_snapshot_t snapshot;
    turbo_flow_observe_control_event_t event;
    check_not_null(observe);
    memset(&event, 0, sizeof(event));

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_PEER_CONNECTED;
    event.status = TURBO_OK;
    event.value = 0;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_SCHEDULED;
    event.status = TURBO_ETIMEDOUT;
    event.value = 25;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_SUCCEEDED;
    event.status = TURBO_OK;
    event.value = 0;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_HEARTBEAT_TIMEOUT;
    event.status = TURBO_ETIMEDOUT;
    event.value = 100;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_FRAME_SENT;
    event.status = TURBO_OK;
    event.value = 128;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_HWM_REACHED;
    event.status = TURBO_ENOSPC;
    event.value = 256;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    event.kind = TURBO_FLOW_OBSERVE_CONTROL_FRAME_DROPPED;
    event.status = TURBO_ENOTCONN;
    event.value = 512;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);

    check_int_eq(turbo_flow_observe_snapshot(observe, &snapshot), TURBO_OK);
    check_size_eq(snapshot.control_peer_connected, 1);
    check_size_eq(snapshot.control_peer_disconnected, 0);
    check_size_eq(snapshot.control_reconnect_scheduled, 1);
    check_size_eq(snapshot.control_reconnect_succeeded, 1);
    check_size_eq(snapshot.control_reconnect_failed, 0);
    check_size_eq(snapshot.control_heartbeat_timeout, 1);
    check_size_eq(snapshot.control_frame_sent, 1);
    check_size_eq(snapshot.control_hwm_reached, 1);
    check_size_eq(snapshot.control_frame_dropped, 1);
    check_size_eq(snapshot.control_errors, 4);
    check_int_eq(snapshot.control_last_status, TURBO_ENOTCONN);
    check_size_eq(snapshot.control_last_value, 512);

    event.kind = (turbo_flow_observe_control_event_kind_t)0;
    check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_EINVAL);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("queries bounded events command results conditions and exporter-neutral metrics") {
    turbo_flow_observe_config_t config = {16u, 2u};
    turbo_flow_observe_t *observe = turbo_flow_observe_create(&config);
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
    observe_resource_state_t queue_resource = {
        TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
        "queue:export", "export", 2u, 4u};
    turbo_flow_observe_control_event_t event;
    turbo_flow_observe_event_record_t first;
    turbo_flow_observe_event_record_t second;
    turbo_flow_resource_command_result_t command = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    turbo_flow_resource_command_result_t queried = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    turbo_flow_observe_resource_view_t resource;
    char target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
    observe_export_state_t exported;

    memset(&exported, 0, sizeof(exported));
    check_not_null(observe);
    check_not_null(flow);
    ops.metadata = observe_resource_metadata;
    ops.snapshot = observe_resource_snapshot;
    check_int_eq(turbo_flow_register_resource_provider(flow, queue_resource.owner_name, &ops,
                                                       &queue_resource),
                 TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    for (int i = 0; i < 3; ++i) {
      memset(&event, 0, sizeof(event));
      event.kind = TURBO_FLOW_OBSERVE_CONTROL_FRAME_SENT;
      event.value = (uint64_t)i;
      check_int_eq(turbo_flow_observe_record_control_event(observe, &event), TURBO_OK);
    }
    check_size_eq(turbo_flow_observe_event_count(observe), 2u);
    check_size_eq(turbo_flow_observe_dropped_event_count(observe), 1u);
    check_int_eq(turbo_flow_observe_event_at(observe, 0u, &first), TURBO_OK);
    check_int_eq(turbo_flow_observe_event_at(observe, 1u, &second), TURBO_OK);
    check_size_eq(first.sequence, 2u);
    check_size_eq(second.sequence, 3u);
    check_size_eq(first.event.value, 1u);
    check_size_eq(second.event.value, 2u);

    command.status = TURBO_OK;
    command.generation_before = 4u;
    command.generation_after = 5u;
    command.observed_generation = 5u;
    check_int_eq(turbo_flow_observe_record_command_result(observe, queue_resource.uid, &command),
                 TURBO_OK);
    check_int_eq(turbo_flow_observe_last_command_result(observe, target_uid, &queried), TURBO_OK);
    check_str_eq(target_uid, queue_resource.uid);
    check_size_eq(queried.generation_after, 5u);

    check_size_eq(turbo_flow_observe_resource_count(observe), 1u);
    check_int_eq(turbo_flow_observe_resource_at(observe, 0u, &resource), TURBO_OK);
    check_str_eq(resource.snapshot.uid, queue_resource.uid);
    check_uint_eq(resource.condition_count, TURBO_FLOW_RESOURCE_CONDITION_MAX);
    check_int_eq(resource.conditions[2].status, TURBO_FLOW_CONDITION_FALSE);
    check_int_eq(resource.conditions[3].status, TURBO_FLOW_CONDITION_FALSE);
    check_int_eq(turbo_flow_observe_export_prometheus(observe, observe_export_write, &exported),
                 TURBO_OK);
    check_not_null(exported.text);
    check_not_null(strstr(exported.text, "turbo_flow_resource_load"));
    check_not_null(strstr(exported.text, "uid=\"queue:export\""));
    check_null(strstr(exported.text, "payload"));
    check_int_eq(turbo_flow_observe_export_opentelemetry(observe, observe_export_metric,
                                                         &exported),
                 TURBO_OK);
    check_size_gt(exported.metrics, 2u);
    check_true(exported.saw_resource);
    tstr_freep(&exported.text);
    check_int_eq(turbo_flow_observe_detach(observe), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("deduplicates shared typed resources without counting them as connections") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_adapter_ops_t ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_resource_provider_ops_t resource_ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
    turbo_flow_observe_graph_snapshot_t graph;
    observe_resource_state_t queue_resource = {
        TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
        "queue:shared", "shared-queue", 2u, 4u};
    check_not_null(flow);
    check_not_null(observe);
    memset(&ops, 0, sizeof(ops));
    memset(&schema, 0, sizeof(schema));
    schema.kind = TURBO_FLOW_ADAPTER_KIND_QUEUE;
    schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
    check_int_eq(turbo_flow_register_adapter_ex(flow, "queue.source", &ops, NULL, &schema),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_adapter_ex(flow, "queue.sink", &ops, NULL, &schema),
                 TURBO_OK);
    resource_ops.metadata = observe_resource_metadata;
    resource_ops.snapshot = observe_resource_snapshot;
    check_int_eq(turbo_flow_register_resource_provider(
                     flow, queue_resource.owner_name, &resource_ops, &queue_resource),
                 TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
    check_size_eq(graph.connection_providers, 0);
    check_size_eq(graph.connections_current, 0);
    check_size_eq(graph.resource_providers, 1);
    check_size_eq(graph.connection_resources, 0);
    check_size_eq(graph.queue_buffer_resources, 1);
    check_size_eq(graph.pool_resources, 0);
    check_size_eq(graph.resource_load, 2);
    check_size_eq(graph.resource_capacity, 4);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("redacts summaries by default and bounds explicit hex previews") {
    static const char *dsl = "source input\n"
                             "stage log adapter observe.log\n"
                             "stage main {\n"
                             "  input -> log\n"
                             "}\n";
    turbo_flow_observe_log_config_t config;
    observe_log_state_t redacted;
    observe_log_state_t preview;
    turbo_flow_t *redacted_flow = turbo_flow_create();
    turbo_flow_t *preview_flow = turbo_flow_create();
    check_not_null(redacted_flow);
    check_not_null(preview_flow);
    memset(&redacted, 0, sizeof(redacted));
    memset(&preview, 0, sizeof(preview));
    memset(&config, 0, sizeof(config));
    config.write = observe_log_write;
    config.write_ctx = &redacted;
    check_int_eq(turbo_flow_observe_register_log_sink(redacted_flow, "observe.log", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(redacted_flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(redacted_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(redacted_flow), TURBO_OK);
    check_int_eq(observe_publish(redacted_flow, "secret"), TURBO_OK);
    check_int_eq(redacted.called, 1);
    check_int_eq(redacted.redacted, 1);
    check_size_eq(redacted.preview_bytes, 0);
    check_str_eq(redacted.preview, "");
    check_int_eq(turbo_flow_stop(redacted_flow), TURBO_OK);
    turbo_flow_destroy(redacted_flow);

    memset(&config, 0, sizeof(config));
    config.max_preview_bytes = 3;
    config.include_payload_preview = 1;
    config.write = observe_log_write;
    config.write_ctx = &preview;
    check_int_eq(turbo_flow_observe_register_log_sink(preview_flow, "observe.log", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(preview_flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(preview_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(preview_flow), TURBO_OK);
    check_int_eq(observe_publish(preview_flow, "secret"), TURBO_OK);
    check_int_eq(preview.called, 1);
    check_int_eq(preview.redacted, 0);
    check_size_eq(preview.payload_size, 6);
    check_size_eq(preview.preview_bytes, 3);
    check_str_eq(preview.preview, "736563");
    check_str_eq(preview.stage, "log");
    check_int_eq(turbo_flow_stop(preview_flow), TURBO_OK);
    turbo_flow_destroy(preview_flow);
  }

  it("reconciles idle pool load with hysteresis bounds and cooldown") {
    static const char *dsl = "source input\n"
                             "stage work worker 4 capacity 16\n"
                             "stage main {\n"
                             "  input -> work\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_pool_policy_t policy;
    turbo_flow_observe_reconcile_state_t state;
    turbo_flow_observe_reconcile_result_t result;
    turbo_flow_observe_graph_snapshot_t graph;
    turbo_flow_pool_snapshot_t pool;

    check_not_null(flow);
    check_not_null(observe);
    check_int_eq(turbo_flow_register_stage_ex(flow, "work", observe_stage_ok, NULL, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
    check_size_eq(graph.resource_providers, 4);
    check_size_eq(graph.connection_resources, 0);
    check_size_eq(graph.queue_buffer_resources, 0);
    check_size_eq(graph.pool_resources, 1);
    check_size_eq(graph.runtime_resources, 1);
    check_size_eq(graph.segment_resources, 2);
    check_size_eq(graph.resource_load, 0);
    check_size_eq(graph.resource_capacity, 16);
    memset(&policy, 0, sizeof(policy));
    policy.size = sizeof(policy);
    policy.stage_name = "work";
    policy.kind = TURBO_FLOW_POOL_DISRUPTOR;
    policy.min_parallelism = 1;
    policy.max_parallelism = 8;
    policy.scale_up_step = 2;
    policy.scale_down_step = 1;
    policy.high_utilization_bps = 8000;
    policy.low_utilization_bps = 1000;
    policy.high_observations = 2;
    policy.low_observations = 2;
    policy.cooldown_ms = 100;
    policy.drain_timeout_ms = UINT64_MAX;
    memset(&state, 0, sizeof(state));

    check_int_eq(turbo_flow_observe_reconcile_pool(observe, &policy, &state,
                                                   UINT64_C(1000000000), &result),
                 TURBO_OK);
    check_int_eq(result.action, TURBO_FLOW_OBSERVE_RECONCILE_HYSTERESIS);
    check_uint_eq(result.previous_parallelism, 4);
    check_uint_eq(result.utilization_bps, 0);
    check_int_eq(turbo_flow_observe_reconcile_pool(observe, &policy, &state,
                                                   UINT64_C(1001000000), &result),
                 TURBO_OK);
    check_int_eq(result.action, TURBO_FLOW_OBSERVE_RECONCILE_RESIZED);
    check_uint_eq(result.desired_parallelism, 3);
    check_int_eq(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
    check_uint_eq(pool.parallelism, 3);

    check_int_eq(turbo_flow_observe_reconcile_pool(observe, &policy, &state,
                                                   UINT64_C(1010000000), &result),
                 TURBO_OK);
    check_int_eq(result.action, TURBO_FLOW_OBSERVE_RECONCILE_HYSTERESIS);
    check_int_eq(turbo_flow_observe_reconcile_pool(observe, &policy, &state,
                                                   UINT64_C(1020000000), &result),
                 TURBO_OK);
    check_int_eq(result.action, TURBO_FLOW_OBSERVE_RECONCILE_COOLDOWN);
    check_uint_eq(result.desired_parallelism, 2);
    check_int_eq(turbo_flow_observe_reconcile_pool(observe, &policy, &state,
                                                   UINT64_C(1120000000), &result),
                 TURBO_OK);
    check_int_eq(result.action, TURBO_FLOW_OBSERVE_RECONCILE_RESIZED);
    check_uint_eq(result.desired_parallelism, 2);
    check_int_eq(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
    check_uint_eq(pool.parallelism, 2);

    policy.min_parallelism = 5;
    check_int_eq(turbo_flow_observe_reconcile_pool(observe, &policy, &state,
                                                   UINT64_C(1300000000), &result),
                 TURBO_OK);
    check_int_eq(result.action, TURBO_FLOW_OBSERVE_RECONCILE_RESIZED);
    check_uint_eq(result.desired_parallelism, 5);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }
}
