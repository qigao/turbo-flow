#include "tinytest.h"
#include "turbo_flow.h"

#include <string.h>

static turbo_flow_t *durable_buffer_parse(const char *text) {
  turbo_flow_t *flow = turbo_flow_create();
  check_not_null(flow);
  check_equal(turbo_flow_parse_string(flow, text, strlen(text)), SALTS_OK);
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
}
