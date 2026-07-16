#include "flow_internal.h"

#include <string.h>

void flow_clear_runtime_plan(turbo_flow_t *flow) {
  if (!flow) return;
  turbo_vec_clear(&flow->runtime_nodes);
  turbo_vec_clear(&flow->runtime_edges);
  turbo_vec_clear(&flow->data_segments);
  turbo_vec_clear(&flow->executor_plans);
}

static int flow_push_data_segment(turbo_flow_t *flow, flow_data_segment_kind_t kind,
                                  uint32_t stage_index, uint32_t edge_index, uint32_t width,
                                  uint32_t capacity,
                                  const turbo_flow_operation_runtime_contract_t *operation) {
  flow_data_segment_plan_t segment;

  memset(&segment, 0, sizeof(segment));
  segment.kind = kind;
  segment.stage_index = stage_index;
  segment.edge_index = edge_index;
  segment.width = width;
  segment.capacity = capacity;
  if (operation) segment.operation = *operation;
  return turbo_vec_push(&flow->data_segments, &segment);
}

static const turbo_flow_operation_runtime_contract_t *
flow_direct_operation_contract(const turbo_flow_t *flow, const flow_stage_plan_impl_t *stage) {
  const turbo_flow_operation_runtime_contract_t *runtime =
      flow_stage_operation_runtime(flow, stage);
  return runtime && runtime->handoff == TURBO_FLOW_HANDOFF_DIRECT ? runtime : NULL;
}

int flow_build_runtime_plan(turbo_flow_t *flow) {
  size_t stage_count;
  int rc;

  if (!flow) return TURBO_EINVAL;

  flow_clear_runtime_plan(flow);
  stage_count = turbo_vec_size(&flow->stages);

  rc = turbo_vec_resize(&flow->runtime_nodes, stage_count);
  if (rc != TURBO_OK) {
    return flow_set_error(flow, rc, 0, 0, "out of memory");
  }
  memset(turbo_vec_data(&flow->runtime_nodes), 0, stage_count * sizeof(flow_runtime_node_plan_t));

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, stage_index);
    flow_runtime_node_plan_t *node =
        (flow_runtime_node_plan_t *)turbo_vec_at(&flow->runtime_nodes, stage_index);

    node->stage_index = (uint32_t)stage_index;
    if (stage->is_source) node->flags |= FLOW_RUNTIME_NODE_SOURCE;
    if (stage->is_port) node->flags |= FLOW_RUNTIME_NODE_PORT;
    if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
      node->flags |= FLOW_RUNTIME_NODE_WORKER_POOL;
    }
    if (!stage->is_source && !stage->is_port) {
      flow_executor_plan_t executor;

      memset(&executor, 0, sizeof(executor));
      executor.stage_index = (uint32_t)stage_index;
      executor.exec = stage->exec;
      executor.fn = stage->fn;
      executor.emit_fn = stage->emit_fn;
      executor.key_selector = stage->key_selector;
      executor.key_ctx = stage->key_ctx;
      executor.keyed_fn = stage->keyed_fn;
      executor.keyed_emit_fn = stage->keyed_emit_fn;
      executor.window_fn = stage->window_fn;
      executor.window_close_fn = stage->window_close_fn;
      executor.keyed_store = stage->keyed_store;
      executor.max_outputs = stage->max_outputs;
      executor.ctx = stage->ctx;
      rc = turbo_vec_push(&flow->executor_plans, &executor);
      if (rc != TURBO_OK) {
        flow_clear_runtime_plan(flow);
        return flow_set_error(flow, rc, 0, 0, "out of memory");
      }
    }
  }

  for (size_t edge_index = 0; edge_index < turbo_vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)turbo_vec_at_const(&flow->edges, edge_index);
    flow_runtime_edge_plan_t runtime_edge;
    flow_runtime_node_plan_t *from_node =
        (flow_runtime_node_plan_t *)turbo_vec_at(&flow->runtime_nodes, edge->from_stage);
    flow_runtime_node_plan_t *to_node =
        (flow_runtime_node_plan_t *)turbo_vec_at(&flow->runtime_nodes, edge->to_stage);
    const flow_stage_plan_impl_t *to =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, edge->to_stage);

    memset(&runtime_edge, 0, sizeof(runtime_edge));
    runtime_edge.from_stage = edge->from_stage;
    runtime_edge.to_stage = edge->to_stage;
    runtime_edge.line = edge->line;
    runtime_edge.column = edge->column;
    runtime_edge.is_stage_internal = edge->is_stage_internal;
    runtime_edge.kind = edge->kind;
    runtime_edge.name = edge->name;
    runtime_edge.predicate = edge->predicate;

    rc = turbo_vec_push(&flow->runtime_edges, &runtime_edge);
    if (rc != TURBO_OK) {
      flow_clear_runtime_plan(flow);
      return flow_set_error(flow, rc, 0, 0, "out of memory");
    }
    rc = flow_push_data_segment(flow, FLOW_DATA_SEGMENT_DIRECT, edge->from_stage,
                                (uint32_t)edge_index, 1u, 0u,
                                flow_direct_operation_contract(flow, to));
    if (rc != TURBO_OK) {
      flow_clear_runtime_plan(flow);
      return flow_set_error(flow, rc, 0, 0, "out of memory");
    }

    from_node->outgoing_count += 1u;
    to_node->incoming_count += 1u;
  }

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    flow_runtime_node_plan_t *node =
        (flow_runtime_node_plan_t *)turbo_vec_at(&flow->runtime_nodes, stage_index);
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, stage_index);
    if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
      rc = flow_push_data_segment(flow, FLOW_DATA_SEGMENT_WORKER_POOL, (uint32_t)stage_index,
                                  UINT32_MAX, stage->data_worker_count, stage->data_pool_capacity,
                                  flow_stage_operation_runtime(flow, stage));
      if (rc != TURBO_OK) {
        flow_clear_runtime_plan(flow);
        return flow_set_error(flow, rc, 0, 0, "out of memory");
      }
    }
    if (node->outgoing_count > 1u) {
      node->flags |= FLOW_RUNTIME_NODE_FANOUT;
      rc = flow_push_data_segment(flow, FLOW_DATA_SEGMENT_BROADCAST_FANOUT, (uint32_t)stage_index,
                                  UINT32_MAX, node->outgoing_count, 0u, NULL);
      if (rc != TURBO_OK) {
        flow_clear_runtime_plan(flow);
        return flow_set_error(flow, rc, 0, 0, "out of memory");
      }
    }
    if (node->incoming_count > 1u) {
      node->flags |= FLOW_RUNTIME_NODE_FANIN;
      rc = flow_push_data_segment(flow, FLOW_DATA_SEGMENT_FANIN_GATE, (uint32_t)stage_index,
                                  UINT32_MAX, node->incoming_count, 0u, NULL);
      if (rc != TURBO_OK) {
        flow_clear_runtime_plan(flow);
        return flow_set_error(flow, rc, 0, 0, "out of memory");
      }
    }
  }

  return TURBO_OK;
}

size_t turbo_flow_segment_count(const turbo_flow_t *flow) {
  return flow && (flow->state == TURBO_FLOW_STATE_COMPILED ||
                  flow->state == TURBO_FLOW_STATE_STARTED ||
                  flow->state == TURBO_FLOW_STATE_STOPPED)
             ? turbo_vec_size(&flow->data_segments)
             : 0u;
}

int turbo_flow_segment_plan_at(const turbo_flow_t *flow, size_t index,
                               turbo_flow_segment_plan_t *out) {
  const flow_data_segment_plan_t *segment;
  if (!flow || !out ||
      (flow->state != TURBO_FLOW_STATE_COMPILED && flow->state != TURBO_FLOW_STATE_STARTED &&
       flow->state != TURBO_FLOW_STATE_STOPPED)) {
    return TURBO_EINVAL;
  }
  segment = (const flow_data_segment_plan_t *)turbo_vec_at_const(&flow->data_segments, index);
  if (!segment) return TURBO_ENOENT;
  memset(out, 0, sizeof(*out));
  out->kind = (turbo_flow_segment_kind_t)segment->kind;
  out->stage_index = segment->stage_index;
  out->edge_index = segment->edge_index;
  out->width = segment->width;
  out->capacity = segment->capacity;
  out->operation = segment->operation;
  return TURBO_OK;
}

const flow_executor_plan_t *flow_executor_plan_for_stage(const turbo_flow_t *flow,
                                                         uint32_t stage_index) {
  if (!flow) return NULL;
  for (size_t i = 0; i < turbo_vec_size(&flow->executor_plans); ++i) {
    const flow_executor_plan_t *executor =
        (const flow_executor_plan_t *)turbo_vec_at_const(&flow->executor_plans, i);
    if (executor->stage_index == stage_index) return executor;
  }
  return NULL;
}

flow_data_segment_plan_t *flow_worker_pool_segment_for_stage(turbo_flow_t *flow,
                                                             uint32_t stage_index) {
  if (!flow) return NULL;
  for (size_t i = 0; i < turbo_vec_size(&flow->data_segments); ++i) {
    flow_data_segment_plan_t *segment =
        (flow_data_segment_plan_t *)turbo_vec_at(&flow->data_segments, i);
    if (segment->kind == FLOW_DATA_SEGMENT_WORKER_POOL && segment->stage_index == stage_index) {
      return segment;
    }
  }
  return NULL;
}
