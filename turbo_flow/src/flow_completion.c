#include "flow_internal.h"

#include <string.h>

void flow_mark_reachable_from_stage(const turbo_flow_t *flow, uint8_t *reachable,
                                    uint32_t stage_index) {
  if (reachable[stage_index]) return;
  reachable[stage_index] = 1;

  for (size_t i = 0; i < vec_size(&flow->runtime_edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->runtime_edges, i);
    if (edge->from_stage == stage_index) {
      flow_mark_reachable_from_stage(flow, reachable, edge->to_stage);
    }
  }
}

static int flow_enqueue_ready(uint32_t *queue, size_t queue_cap, size_t *tail, uint32_t stage) {
  if (*tail >= queue_cap) return TURBO_ENOSPC;
  queue[*tail] = stage;
  *tail += 1;
  return TURBO_OK;
}

static const flow_runtime_edge_plan_t *flow_reject_edge_for_stage(const turbo_flow_t *flow,
                                                                  uint32_t stage_index) {
  for (size_t i = 0; i < vec_size(&flow->runtime_edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->runtime_edges, i);
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
  if (!flow || !route || route[0] == '\0') return 0;
  for (size_t i = 0; i < vec_size(&flow->runtime_edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->runtime_edges, i);
    const flow_stage_plan_impl_t *target;
    if (!edge || edge->from_stage != stage_index || edge->kind == TURBO_FLOW_EDGE_REJECT) continue;
    target = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);
    if (target && strcmp(target->name, route) == 0) return 1;
  }
  return 0;
}

static int flow_release_downstream(turbo_flow_t *flow, uint32_t stage_index,
                                   const turbo_flow_msg_t *msg, int stage_selected,
                                   int stage_status, uint8_t *done, const uint8_t *reachable,
                                   uint32_t *remaining, uint32_t *activated, uint32_t *queue,
                                   size_t queue_cap, size_t *tail) {
  int has_downstream = 0;
  for (size_t i = 0; i < vec_size(&flow->runtime_edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->runtime_edges, i);
    int active = 0;
    int rc;

    if (edge->from_stage != stage_index || !reachable[edge->to_stage]) continue;
    has_downstream = 1;
    if (remaining[edge->to_stage] == 0) return TURBO_EINVAL;
    if (stage_selected) {
      rc = flow_route_edge_active(flow, edge, msg, stage_status, &active);
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
        event.status = rc == TURBO_OK ? stage_status : rc;
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
        done[edge->to_stage] = 1;
        rc = flow_release_downstream(flow, edge->to_stage, msg, 0, TURBO_OK, done, reachable,
                                     remaining, activated, queue, queue_cap, tail);
        if (rc != TURBO_OK) return rc;
      }
    }
  }

  if (stage_selected && !has_downstream) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    turbo_flow_observe_event_t event;
    memset(&event, 0, sizeof(event));
    event.kind = TURBO_FLOW_OBSERVE_SINK_COMPLETE;
    event.stage_name = stage ? stage->name : NULL;
    event.adapter_name = stage ? stage->adapter_name : NULL;
    event.operation_name = stage ? stage->operation_name : NULL;
    event.msg = msg;
    event.status = stage_status;
    event.selected = -1;
    event.edge_kind = -1;
    event.attempt = msg->execution_attempt;
    flow_observer_emit(flow, &event);
  }

  return TURBO_OK;
}

int flow_apply_completion(turbo_flow_t *flow, const flow_stage_completion_t *completion,
                          turbo_flow_msg_t *msg, uint8_t *done, const uint8_t *reachable,
                          uint32_t *remaining, uint32_t *activated, uint32_t *queue,
                          size_t queue_cap, size_t *tail) {
  if (!flow || !completion || !msg || !done || !reachable || !remaining || !activated || !queue ||
      !tail) {
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
                                   reachable, remaining, activated, queue, queue_cap, tail);
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
                                   reachable, remaining, activated, queue, queue_cap, tail);
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
                                 queue_cap, tail);
}
