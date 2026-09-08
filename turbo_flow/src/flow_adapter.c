#include "flow_internal.h"

#include <string.h>

static SALTS_THREAD_LOCAL turbo_flow_t *flow_active_adapter_stop_owner;

typedef struct flow_managed_source_start_scope_s {
  turbo_flow_t *flow;
  const turbo_flow_stage_plan_t *stage_view;
  uint32_t stage_index;
  size_t adapter_index;
  size_t active_index;
} flow_managed_source_start_scope_t;

static SALTS_THREAD_LOCAL flow_managed_source_start_scope_t *flow_managed_source_start_scope;

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

const flow_adapter_registration_t *flow_adapter_for_compiled_stage(const turbo_flow_t *flow,
                                                                   uint32_t stage_index) {
  const uint32_t *adapter_index;
  if (!flow || !flow->compiled_plan.sealed) return NULL;
  adapter_index =
      (const uint32_t *)vec_at_const(&flow->compiled_plan.adapter_by_stage, stage_index);
  if (!adapter_index || *adapter_index == FLOW_PLAN_INDEX_NONE) return NULL;
  return (const flow_adapter_registration_t *)vec_at_const(&flow->adapters, *adapter_index);
}

int flow_adapter_consume_stage(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                               const flow_adapter_registration_t *adapter, turbo_flow_msg_t *msg) {
  turbo_flow_stage_plan_t view;

  if (!flow || !stage || !msg) return SALTS_EINVAL;
  if (!adapter || !adapter->ops.consume) {
    return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                     "adapter consume callback is not configured");
  }

  flow_make_stage_view(stage, &view);
  if (stage->retry.max_attempts > 1u) {
    if (!adapter->ops.consume_retry) {
      return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                       "adapter retry callback is not configured");
    }
    return adapter->ops.consume_retry(adapter->ctx, flow, &view, msg, &stage->retry);
  }
  return adapter->ops.consume(adapter->ctx, flow, &view, msg);
}

static int flow_stop_adapter_phase(turbo_flow_t *flow, int source_phase) {
  size_t count;
  int first_status = SALTS_OK;

  if (!flow) return SALTS_EINVAL;

  count = vec_size(&flow->active_adapters);
  while (count > 0) {
    flow_active_adapter_t *active;
    flow_adapter_registration_t *adapter;
    const flow_stage_plan_impl_t *stage;
    turbo_flow_stage_plan_t view;
    int status;

    --count;
    active = (flow_active_adapter_t *)vec_at(&flow->active_adapters, count);
    if (!active || active->stopped) continue;

    adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, active->adapter_index);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, active->stage_index);
    if (!adapter || !stage || !!stage->is_source != !!source_phase) continue;

    if (adapter->ops.stop) {
      turbo_flow_t *previous_stop_owner = flow_active_adapter_stop_owner;
      flow_make_stage_view(stage, &view);
      salts_mutex_lock(&flow->runtime_mutex);
      flow->adapter_stop_callback_active = 1;
      flow->adapter_stop_callback_status = SALTS_OK;
      salts_mutex_unlock(&flow->runtime_mutex);
      flow_active_adapter_stop_owner = flow;
      adapter->ops.stop(adapter->ctx, flow, &view);
      flow_active_adapter_stop_owner = previous_stop_owner;
      salts_mutex_lock(&flow->runtime_mutex);
      status = flow->adapter_stop_callback_status;
      flow->adapter_stop_callback_active = 0;
      salts_mutex_unlock(&flow->runtime_mutex);
    } else {
      status = SALTS_OK;
    }
    if (status == SALTS_OK) active->stopped = 1;
    else if (first_status == SALTS_OK) first_status = status;
    if (flow->observer_ops.adapter_event) {
      flow->observer_ops.adapter_event(flow->observer_ctx, stage->name, adapter->name,
                                       TURBO_FLOW_ADAPTER_EVENT_STOP, status);
    }
    {
      turbo_flow_observe_event_t event;
      memset(&event, 0, sizeof(event));
      event.kind = TURBO_FLOW_OBSERVE_ADAPTER_STOP;
      event.stage_name = stage->name;
      event.adapter_name = adapter->name;
      event.operation_name = stage->operation_name;
      event.status = status;
      event.selected = -1;
      event.edge_kind = -1;
      flow_observer_emit(flow, &event);
    }
  }
  return first_status;
}

void flow_close_managed_source_runs(turbo_flow_t *flow) {
  size_t count;
  if (!flow) return;
  count = vec_size(&flow->active_adapters);
  while (count > 0u) {
    flow_active_adapter_t *active;
    turbo_flow_run_t *run;
    --count;
    active = (flow_active_adapter_t *)vec_at(&flow->active_adapters, count);
    if (!active || !active->managed_source_run) continue;
    run = active->managed_source_run;
    active->managed_source_run = NULL;
    turbo_flow_run_close(run);
  }
}

int flow_stop_non_source_adapters(turbo_flow_t *flow) {
  return flow_stop_adapter_phase(flow, 0);
}

int flow_stop_source_adapters(turbo_flow_t *flow) {
  flow_close_managed_source_runs(flow);
  return flow_stop_adapter_phase(flow, 1);
}

int flow_stop_adapters(turbo_flow_t *flow) {
  int non_source_status;
  int source_status;

  if (!flow) return SALTS_EINVAL;
  non_source_status = flow_stop_non_source_adapters(flow);
  flow_close_managed_source_runs(flow);
  flow_wait_for_publishes(flow);
  source_status = flow_stop_adapter_phase(flow, 1);
  if (non_source_status == SALTS_OK && source_status == SALTS_OK)
    turbo_flow_stl_error(vec_clear(&flow->active_adapters));
  return non_source_status != SALTS_OK ? non_source_status : source_status;
}

int turbo_flow_adapter_report_stop_status(turbo_flow_t *flow, int status) {
  if (!flow || status == SALTS_OK || !flow->runtime_sync_initialized ||
      flow_active_adapter_stop_owner != flow)
    return SALTS_EINVAL;
  salts_mutex_lock(&flow->runtime_mutex);
  if (!flow->adapter_stop_callback_active) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_EINVAL;
  }
  if (flow->adapter_stop_callback_status == SALTS_OK)
    flow->adapter_stop_callback_status = status;
  salts_mutex_unlock(&flow->runtime_mutex);
  return SALTS_OK;
}

int flow_start_adapters(turbo_flow_t *flow) {
  int rc = SALTS_OK;

  if (!flow) return SALTS_EINVAL;
  turbo_flow_stl_error(vec_clear(&flow->active_adapters));
  if (turbo_flow_stl_error(vec_reserve(&flow->active_adapters, vec_size(&flow->stages))) !=
      SALTS_OK) {
    return flow_set_error_keep_state(flow, SALTS_ENOMEM, 0, 0,
                                     "active adapter reservation failed");
  }

  for (size_t stage_index = 0; stage_index < vec_size(&flow->stages); ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    int adapter_index;
    flow_adapter_registration_t *adapter;
    flow_active_adapter_t active;
    turbo_flow_stage_plan_t view;
    int active_registered = 0;

    if (!stage || !stage->adapter_name) continue;

    {
      const uint32_t *compiled_index = (const uint32_t *)vec_at_const(
          &flow->compiled_plan.adapter_by_stage, stage_index);
      adapter_index = compiled_index && *compiled_index != FLOW_PLAN_INDEX_NONE
                          ? (int)*compiled_index
                          : -1;
    }
    if (adapter_index < 0) {
      rc = flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                     "stage or source adapter is not registered");
      goto fail;
    }

    adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, (size_t)adapter_index);
    if (!adapter) {
      rc = flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                     "adapter registry entry is invalid");
      goto fail;
    }

    if (adapter->managed_source) {
      memset(&active, 0, sizeof(active));
      active.stage_index = (uint32_t)stage_index;
      active.adapter_index = (size_t)adapter_index;
      if (turbo_flow_stl_error(vec_push(&flow->active_adapters, &active)) != SALTS_OK) {
        rc = flow_set_error_keep_state(flow, SALTS_ENOMEM, stage->line, stage->column,
                                       "active managed Source registration failed");
        goto fail;
      }
      active_registered = 1;
    }

    if (adapter->ops.start) {
      flow_managed_source_start_scope_t scope;
      flow_managed_source_start_scope_t *previous_scope = flow_managed_source_start_scope;
      flow_make_stage_view(stage, &view);
      if (adapter->managed_source) {
        scope.flow = flow;
        scope.stage_view = &view;
        scope.stage_index = (uint32_t)stage_index;
        scope.adapter_index = (size_t)adapter_index;
        scope.active_index = vec_size(&flow->active_adapters) - 1u;
        flow_managed_source_start_scope = &scope;
      }
      rc = adapter->ops.start(adapter->ctx, flow, &view);
      flow_managed_source_start_scope = previous_scope;
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
      if (rc != SALTS_OK) {
        if (flow->last_error.code == SALTS_OK) {
          (void)flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                          "adapter start failed");
        }
        goto fail;
      }
    }

    if (!active_registered && (adapter->ops.start || adapter->ops.stop)) {
      memset(&active, 0, sizeof(active));
      active.stage_index = (uint32_t)stage_index;
      active.adapter_index = (size_t)adapter_index;
      if (turbo_flow_stl_error(vec_push(&flow->active_adapters, &active)) != SALTS_OK) {
        rc = flow_set_error_keep_state(flow, SALTS_ENOMEM, stage->line, stage->column,
                                       "out of memory");
        goto fail;
      }
    }
  }

  return SALTS_OK;

fail:
  {
    const int rollback_status = flow_stop_adapters(flow);
    if (rollback_status != SALTS_OK) {
      salts_mutex_lock(&flow->runtime_mutex);
      (void)flow_set_error_keep_state(flow, rollback_status, 0, 0,
                                      "adapter stop failed during start rollback");
      flow->state = TURBO_FLOW_STATE_FAILED;
      flow->admission_state = FLOW_ADMISSION_CLOSED;
      flow->adapter_stop_retryable = 1;
      salts_cond_broadcast(&flow->runtime_cond);
      salts_mutex_unlock(&flow->runtime_mutex);
      return rollback_status;
    }
  }
  return rc;
}

int turbo_flow_managed_source_run_open(turbo_flow_t *flow,
                                       const turbo_flow_stage_plan_t *stage,
                                       cflow_publisher *publisher,
                                       const turbo_flow_run_config_t *config,
                                       turbo_flow_run_t **run_out) {
  flow_managed_source_start_scope_t *scope = flow_managed_source_start_scope;
  flow_active_adapter_t *active;
  const flow_adapter_registration_t *adapter;
  turbo_flow_run_t *run = NULL;
  int rc;

  if (run_out) *run_out = NULL;
  if (!flow || !stage || !publisher || !run_out || !scope || scope->flow != flow ||
      scope->stage_view != stage || !stage->is_source || !stage->name || !stage->adapter_name) {
    return SALTS_EINVAL;
  }
  active = (flow_active_adapter_t *)vec_at(&flow->active_adapters, scope->active_index);
  adapter = (const flow_adapter_registration_t *)vec_at_const(&flow->adapters,
                                                               scope->adapter_index);
  if (!active || !adapter || !adapter->managed_source || active->managed_source_run ||
      active->stage_index != scope->stage_index || active->adapter_index != scope->adapter_index ||
      strcmp(stage->adapter_name, adapter->name) != 0) {
    return SALTS_EINVAL;
  }

  rc = flow_run_open_internal(flow, stage->name, publisher, config, 0, 1, &run);
  if (rc != SALTS_OK) return rc;
  active->managed_source_run = run;
  *run_out = run;
  return SALTS_OK;
}
