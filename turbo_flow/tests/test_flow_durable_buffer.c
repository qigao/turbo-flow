#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_inbox.h"
#include "../src/flow_internal.h"

#include <string.h>

static turbo_flow_t *durable_buffer_parse(const char *text) {
  turbo_flow_t *flow = turbo_flow_create();
  check_not_null(flow);
  check_equal(turbo_flow_parse_string(flow, text, strlen(text)), SALTS_OK);
  return flow;
}

static int durable_buffer_sink_consume(void *ctx, turbo_flow_t *flow,
                                       const turbo_flow_stage_plan_t *stage,
                                       turbo_flow_msg_t *message) {
  size_t *calls = (size_t *)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  if (calls) *calls += 1u;
  return SALTS_OK;
}

typedef struct durable_buffer_binding_fixture_s {
  turbo_flow_inbox_t inbox;
  turbo_flow_durable_buffer_binding_t *binding;
} durable_buffer_binding_fixture_t;

static int durable_buffer_bind_memory(turbo_flow_t *flow, const char *resource_name,
                                      durable_buffer_binding_fixture_t *fixture) {
  turbo_flow_inbox_memory_config_t inbox_config = turbo_flow_inbox_memory_config_default();
  turbo_flow_durable_buffer_binding_config_t binding_config =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  int rc;

  if (!flow || !resource_name || !fixture) return SALTS_EINVAL;
  fixture->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  fixture->binding = NULL;
  rc = turbo_flow_inbox_memory_create(&inbox_config, &fixture->inbox);
  if (rc != SALTS_OK) return rc;
  binding_config.resource_name = resource_name;
  binding_config.inbox = &fixture->inbox;
  rc = turbo_flow_durable_buffer_bind(flow, &binding_config, &fixture->binding);
  if (rc != SALTS_OK) {
    (void)turbo_flow_inbox_destroy(&fixture->inbox);
    fixture->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  }
  return rc;
}

static void durable_buffer_binding_fixture_cleanup(durable_buffer_binding_fixture_t *fixture) {
  if (!fixture) return;
  if (fixture->binding) {
    (void)turbo_flow_durable_buffer_unbind(fixture->binding);
    fixture->binding = NULL;
  }
  if (fixture->inbox.ops) {
    (void)turbo_flow_inbox_close(&fixture->inbox);
    (void)turbo_flow_inbox_destroy(&fixture->inbox);
  }
  fixture->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
}

static turbo_flow_t *durable_buffer_compile_graph(size_t *sink_calls) {
  static const char graph[] =
      "buffer intake resource intake.store\n"
      "source telemetry\n"
      "stage normalize adapter sink\n"
      "stage main {\n"
      "  telemetry -> intake\n"
      "  intake -> normalize\n"
      "}\n";
  turbo_flow_adapter_ops_t ops;
  turbo_flow_t *flow = durable_buffer_parse(graph);

  memset(&ops, 0, sizeof(ops));
  ops.consume = durable_buffer_sink_consume;
  check_equal(turbo_flow_register_adapter(flow, "sink", &ops, sink_calls), SALTS_OK);
  return flow;
}

spec("Graph durable buffer DSL") {
  it("parses one root buffer with an explicit resource binding") {
    static const char graph[] =
        "buffer intake resource intake.store\n"
        "source telemetry\n"
        "stage normalize\n"
        "stage main {\n"
        "  telemetry -> intake\n"
        "  intake -> normalize\n"
        "}\n";
    turbo_flow_t *flow = durable_buffer_parse(graph);
    int index = turbo_flow_find_stage(flow, "intake");
    const turbo_flow_stage_plan_t *stage;

    check_true(index >= 0);
    stage = turbo_flow_stage_at(flow, (size_t)index);
    check_not_null(stage);
    check_equal(stage->is_buffer, 1);
    check_equal(stage->is_source, 0);
    check_not_null(stage->resource_name);
    check_equal(strcmp(stage->resource_name, "intake.store"), 0);

    turbo_flow_destroy(flow);
  }

  it("rejects a buffer without a resource binding") {
    turbo_flow_t *flow = turbo_flow_create();
    static const char graph[] =
        "buffer intake\n"
        "source telemetry\n"
        "stage main {\n"
        "  telemetry -> intake\n"
        "}\n";

    check_not_null(flow);
    check_not_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects duplicate buffer and stage names") {
    turbo_flow_t *flow = turbo_flow_create();
    static const char graph[] =
        "buffer intake resource intake.store\n"
        "stage intake\n"
        "source telemetry\n"
        "stage main {\n"
        "  telemetry -> intake\n"
        "}\n";

    check_not_null(flow);
    check_not_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects buffer declarations inside reusable stage templates") {
    turbo_flow_t *flow = turbo_flow_create();
    static const char graph[] =
        "stage reusable {\n"
        "  in input\n"
        "  buffer queued resource queue.store\n"
        "  out output\n"
        "  input -> queued -> output\n"
        "}\n"
        "source telemetry\n"
        "stage main {\n"
        "  use branch = reusable\n"
        "  telemetry -> branch\n"
        "}\n";

    check_not_null(flow);
    check_not_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("compiles a bound buffer as a non-executor execution cut") {
    size_t sink_calls = 0u;
    durable_buffer_binding_fixture_t fixture = {0};
    turbo_flow_t *flow = durable_buffer_compile_graph(&sink_calls);
    int source_index = turbo_flow_find_stage(flow, "telemetry");
    int buffer_index = turbo_flow_find_stage(flow, "intake");
    int sink_index = turbo_flow_find_stage(flow, "normalize");
    int rc;

    check_true(source_index >= 0);
    check_true(buffer_index >= 0);
    check_true(sink_index >= 0);
    check_equal(durable_buffer_bind_memory(flow, "intake.store", &fixture), SALTS_OK);
    rc = turbo_flow_compile(flow);
    check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      const flow_runtime_node_plan_t *buffer_node =
          (const flow_runtime_node_plan_t *)vec_at_const(&flow->compiled_plan.nodes,
                                                         (size_t)buffer_index);
      const uint32_t *buffer_executor =
          (const uint32_t *)vec_at_const(&flow->compiled_plan.executor_by_stage,
                                         (size_t)buffer_index);
      const uint32_t *sink_executor =
          (const uint32_t *)vec_at_const(&flow->compiled_plan.executor_by_stage,
                                         (size_t)sink_index);
      const flow_stage_semantic_plan_t *buffer_semantics =
          (const flow_stage_semantic_plan_t *)vec_at_const(&flow->compiled_plan.stage_semantics,
                                                           (size_t)buffer_index);

      check_not_null(buffer_node);
      check_not_null(buffer_executor);
      check_not_null(sink_executor);
      check_not_null(buffer_semantics);
      check_true((buffer_node->flags & FLOW_RUNTIME_NODE_BUFFER) != 0u);
      check_equal(*buffer_executor, FLOW_PLAN_INDEX_NONE);
      check_not_equal(*sink_executor, FLOW_PLAN_INDEX_NONE);
      check_true((buffer_semantics->barriers & FLOW_LOWERING_BARRIER_EXTERNAL_IO) != 0u);
      check_true((buffer_semantics->barriers & FLOW_LOWERING_BARRIER_SETTLEMENT) != 0u);
    }

    durable_buffer_binding_fixture_cleanup(&fixture);
    turbo_flow_destroy(flow);
  }

  it("stops an execution region at a reached bound buffer but can resume from that buffer") {
    size_t sink_calls = 0u;
    durable_buffer_binding_fixture_t fixture = {0};
    turbo_flow_t *flow = durable_buffer_compile_graph(&sink_calls);
    int source_index = turbo_flow_find_stage(flow, "telemetry");
    int buffer_index = turbo_flow_find_stage(flow, "intake");
    int sink_index = turbo_flow_find_stage(flow, "normalize");
    uint8_t reachable[3] = {0};
    uint32_t worklist[3] = {0};
    int rc;

    check_true(source_index >= 0);
    check_true(buffer_index >= 0);
    check_true(sink_index >= 0);
    check_equal(durable_buffer_bind_memory(flow, "intake.store", &fixture), SALTS_OK);
    rc = turbo_flow_compile(flow);
    check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      check_equal(flow_mark_execution_region_from_stage(
                      flow, reachable, worklist, 3u, (uint32_t)source_index),
                  SALTS_OK);
      check_equal(reachable[source_index], 1);
      check_equal(reachable[buffer_index], 1);
      check_equal(reachable[sink_index], 0);

      memset(reachable, 0, sizeof(reachable));
      memset(worklist, 0, sizeof(worklist));
      check_equal(flow_mark_execution_region_from_stage(
                      flow, reachable, worklist, 3u, (uint32_t)buffer_index),
                  SALTS_OK);
      check_equal(reachable[buffer_index], 1);
      check_equal(reachable[sink_index], 1);
    }

    durable_buffer_binding_fixture_cleanup(&fixture);
    turbo_flow_destroy(flow);
  }

  it("rejects compile when a durable buffer resource is unbound") {
    size_t sink_calls = 0u;
    turbo_flow_t *flow = durable_buffer_compile_graph(&sink_calls);

    check_not_equal(turbo_flow_compile(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects one durable binding shared by multiple buffer stages") {
    static const char graph[] =
        "buffer intake resource shared.store\n"
        "buffer archive resource shared.store\n"
        "source telemetry\n"
        "stage main {\n"
        "  telemetry -> intake\n"
        "  telemetry -> archive\n"
        "}\n";
    durable_buffer_binding_fixture_t fixture = {0};
    turbo_flow_t *flow = durable_buffer_parse(graph);

    check_equal(durable_buffer_bind_memory(flow, "shared.store", &fixture), SALTS_OK);
    check_not_equal(turbo_flow_compile(flow), SALTS_OK);

    durable_buffer_binding_fixture_cleanup(&fixture);
    turbo_flow_destroy(flow);
  }

  it("rejects start after its compiled durable binding is removed") {
    size_t sink_calls = 0u;
    durable_buffer_binding_fixture_t fixture = {0};
    turbo_flow_t *flow = durable_buffer_compile_graph(&sink_calls);

    check_equal(durable_buffer_bind_memory(flow, "intake.store", &fixture), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(fixture.binding), SALTS_OK);
    fixture.binding = NULL;
    check_not_equal(turbo_flow_start(flow), SALTS_OK);

    if (flow->state == TURBO_FLOW_STATE_STARTED) check_equal(turbo_flow_stop(flow), SALTS_OK);
    durable_buffer_binding_fixture_cleanup(&fixture);
    turbo_flow_destroy(flow);
  }

  it("admits at a bound durable buffer without activating downstream") {
    static const char graph[] =
        "buffer intake resource intake.store\n"
        "source telemetry\n"
        "stage downstream adapter sink\n"
        "stage main {\n"
        "  telemetry -> intake\n"
        "  intake -> downstream\n"
        "}\n";
    char raw[] = "durable-payload";
    turbo_flow_inbox_memory_config_t inbox_config = turbo_flow_inbox_memory_config_default();
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_config_t binding_config =
        TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_adapter_ops_t ops;
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer;
    size_t sink_calls = 0u;
    turbo_flow_t *flow = durable_buffer_parse(graph);

    memset(&ops, 0, sizeof(ops));
    ops.consume = durable_buffer_sink_consume;
    check_equal(turbo_flow_register_adapter(flow, "sink", &ops, &sink_calls), SALTS_OK);
    check_equal(turbo_flow_inbox_memory_create(&inbox_config, &inbox), SALTS_OK);

    binding_config.resource_name = "intake.store";
    binding_config.inbox = &inbox;
    check_equal(turbo_flow_durable_buffer_bind(flow, &binding_config, &binding), SALTS_OK);
    check_not_null(binding);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    buffer = mem_wrap_external(raw, sizeof(raw) - 1u, NULL, NULL);
    check_not_null(buffer);
    turbo_flow_msg_init(&msg);
    msg.id = 17u;
    msg.ts_ns = 23u;
    msg.type = 5u;
    msg.flags = 7u;
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1u);

    check_equal(turbo_flow_publish(flow, "telemetry", &msg), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.admitted, 1u);
    check_equal(snapshot.pending_records, 1u);
    check_equal(sink_calls, 0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_durable_buffer_unbind(binding), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
