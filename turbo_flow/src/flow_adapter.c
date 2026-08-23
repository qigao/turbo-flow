#include "flow_internal.h"

#include <string.h>

void flow_make_stage_view(const flow_stage_plan_impl_t *stage, turbo_flow_stage_plan_t *view) {
  memset(view, 0, sizeof(*view));
  view->name = stage->name;
  view->is_source = stage->is_source;
  view->adapter_name = stage->adapter_name;
  view->operation_name = stage->operation_resolved ? stage->resolved_operation.name
                                                   : stage->operation_name;
  view->resource_name = stage->resource_name;
  view->data_strategy = stage->data_strategy;
  view->data_worker_count = stage->data_worker_count;
  view->exec = stage->exec;
  view->mutability = stage->mutability;
  view->effects = stage->effects;
  view->retry = stage->retry;
  view->reorder = stage->reorder;
}

const flow_adapter_registration_t *flow_adapter_for_stage(const turbo_flow_t *flow,
                                                          const flow_stage_plan_impl_t *stage) {
  int adapter_index;

  if (!flow || !stage || !stage->adapter_name) return NULL;
  adapter_index = flow_find_adapter(flow, stage->adapter_name);
  if (adapter_index < 0) return NULL;
  return (const flow_adapter_registration_t *)vec_at_const(&flow->adapters,
                                                                 (size_t)adapter_index);
}

int flow_adapter_consume_stage(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                               turbo_flow_msg_t *msg) {
  const flow_adapter_registration_t *adapter = flow_adapter_for_stage(flow, stage);
  turbo_flow_stage_plan_t view;

  if (!flow || !stage || !msg) return TURBO_EINVAL;
  if (!adapter || !adapter->ops.consume) {
    return flow_set_error_keep_state(flow, TURBO_ENOTSUP, stage->line, stage->column,
                                     "adapter consume callback is not configured");
  }

  flow_make_stage_view(stage, &view);
  if (stage->retry.max_attempts > 1u) {
    if (!adapter->ops.consume_retry) {
      return flow_set_error_keep_state(flow, TURBO_ENOTSUP, stage->line, stage->column,
                                       "adapter retry callback is not configured");
    }
    return adapter->ops.consume_retry(adapter->ctx, flow, &view, msg, &stage->retry);
  }
  return adapter->ops.consume(adapter->ctx, flow, &view, msg);
}

void flow_stop_adapters(turbo_flow_t *flow) {
  size_t count;

  if (!flow) return;

  count = vec_size(&flow->active_adapters);
  while (count > 0) {
    flow_active_adapter_t *active;
    flow_adapter_registration_t *adapter;
    const flow_stage_plan_impl_t *stage;
    turbo_flow_stage_plan_t view;

    --count;
    active = (flow_active_adapter_t *)vec_at(&flow->active_adapters, count);
    if (!active) continue;

    adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, active->adapter_index);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, active->stage_index);
    if (!adapter || !stage) continue;

    if (adapter->ops.stop) {
      flow_make_stage_view(stage, &view);
      adapter->ops.stop(adapter->ctx, flow, &view);
    }
    if (flow->observer_ops.adapter_event) {
      flow->observer_ops.adapter_event(flow->observer_ctx, stage->name, adapter->name,
                                       TURBO_FLOW_ADAPTER_EVENT_STOP, TURBO_OK);
    }
    {
      turbo_flow_observe_event_t event;
      memset(&event, 0, sizeof(event));
      event.kind = TURBO_FLOW_OBSERVE_ADAPTER_STOP;
      event.stage_name = stage->name;
      event.adapter_name = adapter->name;
      event.operation_name = stage->operation_name;
      event.status = TURBO_OK;
      event.selected = -1;
      event.edge_kind = -1;
      flow_observer_emit(flow, &event);
    }
  }

  turbo_flow_stl_error(vec_clear(&flow->active_adapters));
}

int flow_start_adapters(turbo_flow_t *flow) {
  int rc = TURBO_OK;

  if (!flow) return TURBO_EINVAL;
  turbo_flow_stl_error(vec_clear(&flow->active_adapters));

  for (size_t stage_index = 0; stage_index < vec_size(&flow->stages); ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    int adapter_index;
    flow_adapter_registration_t *adapter;
    flow_active_adapter_t active;
    turbo_flow_stage_plan_t view;

    if (!stage || !stage->adapter_name) continue;

    adapter_index = flow_find_adapter(flow, stage->adapter_name);
    if (adapter_index < 0) {
      rc = flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                     "stage or source adapter is not registered");
      goto fail;
    }

    adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, (size_t)adapter_index);
    if (!adapter) {
      rc = flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                     "adapter registry entry is invalid");
      goto fail;
    }

    if (adapter->ops.start) {
      flow_make_stage_view(stage, &view);
      rc = adapter->ops.start(adapter->ctx, flow, &view);
      if (flow->observer_ops.adapter_event) {
        flow->observer_ops.adapter_event(flow->observer_ctx, stage->name, adapter->name,
                                         TURBO_FLOW_ADAPTER_EVENT_START, rc);
      }
      {
        turbo_flow_observe_event_t event;
        memset(&event, 0, sizeof(event));
        event.kind = TURBO_FLOW_OBSERVE_ADAPTER_START;
        event.stage_name = stage->name;
        event.adapter_name = adapter->name;
        event.operation_name = stage->operation_name;
        event.status = rc;
        event.selected = -1;
        event.edge_kind = -1;
        flow_observer_emit(flow, &event);
      }
      if (rc != TURBO_OK) {
        if (flow->last_error.code == TURBO_OK) {
          (void)flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                          "adapter start failed");
        }
        goto fail;
      }
    }

    if (adapter->ops.start || adapter->ops.stop) {
      memset(&active, 0, sizeof(active));
      active.stage_index = (uint32_t)stage_index;
      active.adapter_index = (size_t)adapter_index;
      if (turbo_flow_stl_error(vec_push(&flow->active_adapters, &active)) != TURBO_OK) {
        rc = flow_set_error_keep_state(flow, TURBO_ENOMEM, stage->line, stage->column,
                                       "out of memory");
        goto fail;
      }
    }
  }

  return TURBO_OK;

fail:
  flow_stop_adapters(flow);
  return rc;
}
