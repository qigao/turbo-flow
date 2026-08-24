#include "flow_internal.h"

#include <string.h>

/** Time O(V + E), scratch space O(V), with no dependency on the C call stack. */
int flow_mark_reachable_from_stage(const turbo_flow_t *flow, uint8_t *reachable,
                                   uint32_t *worklist, size_t worklist_cap,
                                   uint32_t stage_index) {
  size_t head = 0u;
  size_t tail = 0u;
  size_t stage_count;

  if (!flow || !reachable || !worklist) return TURBO_EINVAL;
  stage_count = vec_size(&flow->runtime_nodes);
  if (stage_index >= stage_count || worklist_cap < stage_count) return TURBO_EINVAL;
  reachable[stage_index] = 1u;
  worklist[tail++] = stage_index;

  while (head < tail) {
    const uint32_t current = worklist[head++];
    const flow_runtime_node_plan_t *node =
        (const flow_runtime_node_plan_t *)vec_at_const(&flow->runtime_nodes, current);
    if (!node || node->outgoing_begin > vec_size(&flow->runtime_edges) ||
        node->outgoing_count > vec_size(&flow->runtime_edges) - node->outgoing_begin) {
      return TURBO_EPROTO;
    }
    for (uint32_t offset = 0u; offset < node->outgoing_count; ++offset) {
      const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
          &flow->runtime_edges, node->outgoing_begin + offset);
      if (!edge || edge->from_stage != current || edge->to_stage >= stage_count) {
        return TURBO_EPROTO;
      }
      if (reachable[edge->to_stage]) continue;
      if (tail >= worklist_cap) return TURBO_ENOSPC;
      reachable[edge->to_stage] = 1u;
      worklist[tail++] = edge->to_stage;
    }
  }
  return TURBO_OK;
}

static int flow_enqueue_ready(uint32_t *queue, size_t queue_cap, size_t *tail, uint32_t stage) {
  if (*tail >= queue_cap) return TURBO_ENOSPC;
  queue[*tail] = stage;
  *tail += 1;
  return TURBO_OK;
}

static const flow_runtime_edge_plan_t *flow_reject_edge_for_stage(const turbo_flow_t *flow,
                                                                  uint32_t stage_index) {
  const flow_runtime_node_plan_t *node =
      (const flow_runtime_node_plan_t *)vec_at_const(&flow->runtime_nodes, stage_index);
  if (!node) return NULL;
  for (uint32_t offset = 0u; offset < node->outgoing_count; ++offset) {
    const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
        &flow->runtime_edges, node->outgoing_begin + offset);
    if (edge && edge->from_stage == stage_index && edge->kind == TURBO_FLOW_EDGE_REJECT) {
      return edge;
    }
  }
  return NULL;
}

static int flow_route_edge_active(turbo_flow_t *flow, const flow_runtime_edge_plan_t *edge,
                                  const turbo_flow_msg_t *msg, int stage_status, int *active) {
  turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
  flow_expr_projection_eval_binding_t projection_binding;
  turbo_flow_expr_value_t value;
  int rc;

  if (!flow || !edge || !msg || !active) return TURBO_EINVAL;
  *active = 0;
  if (stage_status != TURBO_OK) {
    *active = edge->kind == TURBO_FLOW_EDGE_REJECT;
    return TURBO_OK;
  }
  if (edge->kind == TURBO_FLOW_EDGE_REJECT) return TURBO_OK;
  if (msg->data_decision.stage_index == edge->from_stage &&
      msg->data_decision.route[0] != '\0') {
    const flow_stage_plan_impl_t *target =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);
    if (!target) return TURBO_EINVAL;
    *active = strcmp(target->name, msg->data_decision.route) == 0;
    return TURBO_OK;
  }
  if (edge->kind == TURBO_FLOW_EDGE_UNCONDITIONAL) {
    *active = 1;
    return TURBO_OK;
  }
  if (!edge->predicate) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, edge->line, edge->column,
                                     "conditional route predicate is not compiled");
  }
  context.message = msg;
  flow_expr_projection_bind_eval(flow, msg, &projection_binding, &context);
  memset(&value, 0, sizeof(value));
  rc = turbo_flow_expr_evaluate(edge->predicate, &context, &value);
  if (rc != TURBO_OK) {
    return flow_set_error_keep_state(flow, rc, edge->line, edge->column,
                                     "conditional route evaluation failed");
  }
  if (value.type != TURBO_FLOW_EXPR_TYPE_BOOL) {
    return flow_set_error_keep_state(flow, TURBO_EPROTO, edge->line, edge->column,
                                     "conditional route did not return BOOL");
  }
  *active = value.as.boolean != 0;
  return TURBO_OK;
}

static int flow_data_route_exists(const turbo_flow_t *flow, uint32_t stage_index,
                                  const char *route) {
  const flow_runtime_node_plan_t *node;
  if (!flow || !route || route[0] == '\0') return 0;
  node = (const flow_runtime_node_plan_t *)vec_at_const(&flow->runtime_nodes, stage_index);
  if (!node) return 0;
  for (uint32_t offset = 0u; offset < node->outgoing_count; ++offset) {
    const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
        &flow->runtime_edges, node->outgoing_begin + offset);
    const flow_stage_plan_impl_t *target;
    if (!edge || edge->from_stage != stage_index || edge->kind == TURBO_FLOW_EDGE_REJECT) continue;
    target = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);
    if (target && strcmp(target->name, route) == 0) return 1;
  }
  return 0;
}

/** Time O(V + E) over the released subgraph, scratch space O(V), without recursion. */
static int flow_release_downstream(turbo_flow_t *flow, uint32_t stage_index,
                                   const turbo_flow_msg_t *msg, int stage_selected,
                                   int stage_status, uint8_t *done, const uint8_t *reachable,
                                   uint32_t *remaining, uint32_t *activated, uint32_t *queue,
                                   size_t queue_cap, size_t *tail, uint32_t *skipped_queue,
                                   size_t skipped_queue_cap) {
  uint32_t current_stage = stage_index;
  int current_selected = stage_selected;
  int current_status = stage_status;
  size_t skipped_head = 0u;
  size_t skipped_tail = 0u;

  if (!skipped_queue) return TURBO_EINVAL;
  for (;;) {
    const flow_runtime_node_plan_t *node =
        (const flow_runtime_node_plan_t *)vec_at_const(&flow->runtime_nodes, current_stage);
    int has_downstream = 0;
    if (!node) return TURBO_EPROTO;
    for (uint32_t offset = 0u; offset < node->outgoing_count; ++offset) {
      const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
          &flow->runtime_edges, node->outgoing_begin + offset);
      int active = 0;
      int rc;

      if (!edge || edge->from_stage != current_stage) return TURBO_EPROTO;
      if (!reachable[edge->to_stage]) continue;
      has_downstream = 1;
      if (remaining[edge->to_stage] == 0) return TURBO_EINVAL;
      if (current_selected) {
        rc = flow_route_edge_active(flow, edge, msg, current_status, &active);
        {
          const flow_stage_plan_impl_t *from =
              (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->from_stage);
          const flow_stage_plan_impl_t *to =
              (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);
          turbo_flow_observe_event_t event;
          memset(&event, 0, sizeof(event));
          event.kind = TURBO_FLOW_OBSERVE_ROUTE_EVALUATED;
          event.from_name = from ? from->name : NULL;
          event.to_name = to ? to->name : NULL;
          event.route_name = edge->name;
          event.msg = msg;
          event.status = rc == TURBO_OK ? current_status : rc;
          event.selected = rc == TURBO_OK ? active : 0;
          event.edge_kind = edge->kind;
          event.attempt = msg->execution_attempt;
          flow_observer_emit(flow, &event);
        }
        if (rc != TURBO_OK) return rc;
      }
      --remaining[edge->to_stage];
      if (active) ++activated[edge->to_stage];
      if (remaining[edge->to_stage] == 0) {
        if (activated[edge->to_stage] > 0) {
          rc = flow_enqueue_ready(queue, queue_cap, tail, edge->to_stage);
          if (rc != TURBO_OK) return rc;
        } else if (!done[edge->to_stage]) {
          done[edge->to_stage] = 1u;
          rc = flow_enqueue_ready(skipped_queue, skipped_queue_cap, &skipped_tail,
                                  edge->to_stage);
          if (rc != TURBO_OK) return rc;
        }
      }
    }

    if (current_selected && !has_downstream) {
      const flow_stage_plan_impl_t *stage =
          (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, current_stage);
      turbo_flow_observe_event_t event;
      memset(&event, 0, sizeof(event));
      event.kind = TURBO_FLOW_OBSERVE_SINK_COMPLETE;
      event.stage_name = stage ? stage->name : NULL;
      event.adapter_name = stage ? stage->adapter_name : NULL;
      event.operation_name = stage ? stage->operation_name : NULL;
      event.msg = msg;
      event.status = current_status;
      event.selected = -1;
      event.edge_kind = -1;
      event.attempt = msg->execution_attempt;
      flow_observer_emit(flow, &event);
    }

    if (skipped_head >= skipped_tail) return TURBO_OK;
    current_stage = skipped_queue[skipped_head++];
    current_selected = 0;
    current_status = TURBO_OK;
  }
}

int flow_apply_completion(turbo_flow_t *flow, const flow_stage_completion_t *completion,
                          turbo_flow_msg_t *msg, uint8_t *done, const uint8_t *reachable,
                          uint32_t *remaining, uint32_t *activated, uint32_t *queue,
                          size_t queue_cap, size_t *tail, uint32_t *skipped_queue,
                          size_t skipped_queue_cap) {
  if (!flow || !completion || !msg || !done || !reachable || !remaining || !activated || !queue ||
      !tail || !skipped_queue) {
    return TURBO_EINVAL;
  }
  if (completion->entry.stage_index >= vec_size(&flow->runtime_nodes)) return TURBO_EINVAL;
  if (msg->data_decision.stage_index == completion->entry.stage_index &&
      msg->data_decision.route[0] != '\0' &&
      !flow_data_route_exists(flow, completion->entry.stage_index, msg->data_decision.route)) {
    return flow_set_error_keep_state(flow, TURBO_EPROTO, 0, 0,
                                     "data rule selected an unknown downstream route");
  }
  if (msg->data_decision.stage_index == completion->entry.stage_index &&
      (msg->data_decision.dropped || msg->data_decision.dead_letter)) {
    if (msg->data_decision.dead_letter) {
      msg->status = msg->data_decision.dead_letter_status;
    }
    if (done[completion->entry.stage_index]) return TURBO_OK;
    done[completion->entry.stage_index] = 1;
    return flow_release_downstream(flow, completion->entry.stage_index, msg, 0, TURBO_OK, done,
                                   reachable, remaining, activated, queue, queue_cap, tail,
                                   skipped_queue, skipped_queue_cap);
  }
  if (completion->terminal) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages,
                                                           completion->entry.stage_index);
    turbo_flow_observe_event_t event;
    if (done[completion->entry.stage_index]) return TURBO_OK;
    done[completion->entry.stage_index] = 1;
    memset(&event, 0, sizeof(event));
    event.kind = TURBO_FLOW_OBSERVE_SINK_COMPLETE;
    event.stage_name = stage ? stage->name : NULL;
    event.adapter_name = stage ? stage->adapter_name : NULL;
    event.operation_name = stage ? stage->operation_name : NULL;
    event.msg = msg;
    event.status = completion->status;
    event.selected = -1;
    event.edge_kind = -1;
    event.attempt = msg->execution_attempt;
    flow_observer_emit(flow, &event);
    return flow_release_downstream(flow, completion->entry.stage_index, msg, 0, TURBO_OK, done,
                                   reachable, remaining, activated, queue, queue_cap, tail,
                                   skipped_queue, skipped_queue_cap);
  }
  if (completion->status != TURBO_OK) {
    const flow_runtime_edge_plan_t *reject =
        flow_reject_edge_for_stage(flow, completion->entry.stage_index);
    const flow_stage_plan_impl_t *stage;
    int rc;

    if (!reject) return completion->status;
    stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages,
                                                          completion->entry.stage_index);
    if (!stage) return TURBO_EINVAL;
    rc = flow_msg_set_failure(msg, stage->name, stage->adapter_name, reject->name,
                              completion->status,
                              msg->execution_attempt > 0u ? msg->execution_attempt : 1u);
    if (rc != TURBO_OK) {
      return flow_set_error_keep_state(flow, rc, reject->line, reject->column,
                                       "failed to capture reject route metadata");
    }
  }
  if (done[completion->entry.stage_index]) return TURBO_OK;

  done[completion->entry.stage_index] = 1;
  return flow_release_downstream(flow, completion->entry.stage_index, msg, 1,
                                 completion->status, done, reachable, remaining, activated, queue,
                                 queue_cap, tail, skipped_queue, skipped_queue_cap);
}
