#include "tinytest.h"
#include "turbo_flow.h"
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

  it("compiles a buffer as a non-executor execution cut") {
    size_t sink_calls = 0u;
    turbo_flow_t *flow = durable_buffer_compile_graph(&sink_calls);
    int source_index = turbo_flow_find_stage(flow, "telemetry");
    int buffer_index = turbo_flow_find_stage(flow, "intake");
    int sink_index = turbo_flow_find_stage(flow, "normalize");
    int rc;

    check_true(source_index >= 0);
    check_true(buffer_index >= 0);
    check_true(sink_index >= 0);
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

    turbo_flow_destroy(flow);
  }

  it("stops an execution region at a reached buffer but can resume from that buffer") {
    size_t sink_calls = 0u;
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

    turbo_flow_destroy(flow);
  }
}