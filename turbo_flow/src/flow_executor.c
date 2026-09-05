#include "flow_internal.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_threadpool_task_s {
  flow_execution_task_t execution;
  turbo_flow_t *flow;
  flow_pool_record_t *pool_record;
  salts_mutex_t worker_mutex;
  salts_cond_t worker_cond;
  int worker_finished;
} flow_threadpool_task_t;

typedef struct flow_coro_task_s {
  flow_execution_task_t execution;
  turbo_flow_t *flow;
  int pooled;
} flow_coro_task_t;

static int flow_executor_task_header_valid(turbo_flow_t *flow,
                                           const flow_execution_task_t *task) {
  return flow_entry_header_validate(flow, &task->completion.entry, &task->msg) == SALTS_OK &&
         task->completion.entry.completion_handle == &task->completion &&
         task->completion.entry.cancel_handle == &task->cancel_requested;
}

static void flow_threadpool_task_run(void *arg) {
  flow_threadpool_task_t *task = (flow_threadpool_task_t *)arg;
  flow_pool_record_started(task->pool_record);
  if (flow_executor_task_header_valid(task->flow, &task->execution)) {
    flow_execution_task_run(&task->execution);
  } else {
    flow_execution_task_fail(&task->execution, SALTS_EPROTO);
  }
  flow_pool_record_finished(task->pool_record, task->execution.status);
  salts_mutex_lock(&task->worker_mutex);
  task->worker_finished = 1;
  salts_cond_broadcast(&task->worker_cond);
  salts_mutex_unlock(&task->worker_mutex);
}

static void flow_coro_task_run(coro_t *co, void *arg) {
  flow_coro_task_t *task = (flow_coro_task_t *)arg;
  if (flow_executor_task_header_valid(task->flow, &task->execution)) {
    flow_execution_task_run(&task->execution);
  } else {
    flow_execution_task_fail(&task->execution, SALTS_EPROTO);
  }
  if (!task->pooled) coro_set_discard(co, NULL, NULL);
}

static void flow_coro_task_discard(coro_t *co, void *arg) {
  flow_coro_task_t *task = (flow_coro_task_t *)arg;
  (void)co;
  flow_execution_task_discard(&task->execution);
}

static void flow_coro_adapter_destroy(flow_coro_adapter_t *adapter) {
  if (!adapter) return;

  for (uint32_t i = 0; i < adapter->lanes; ++i) {
    if (adapter->schedulers && adapter->schedulers[i]) {
      coro_scheduler_destroy(adapter->schedulers[i]);
      adapter->schedulers[i] = NULL;
    }
    if (adapter->pools && adapter->pools[i]) {
      salts_coro_pool_destroy(adapter->pools[i]);
      adapter->pools[i] = NULL;
    }
    if (adapter->lane_mutexes) salts_mutex_destroy(&adapter->lane_mutexes[i]);
  }
  if (adapter->schedulers) {
    free(adapter->schedulers);
    adapter->schedulers = NULL;
  }
  if (adapter->pools) {
    free(adapter->pools);
    adapter->pools = NULL;
  }
  free(adapter->lane_mutexes);
  adapter->lane_mutexes = NULL;
  adapter->lanes = 0;
}

static int flow_coro_adapter_init(flow_coro_adapter_t *adapter,
                                  const flow_executor_plan_t *executor) {
  uint32_t lanes = executor->exec.lanes ? executor->exec.lanes : 1u;

  memset(adapter, 0, sizeof(*adapter));
  adapter->stage_index = executor->stage_index;
  adapter->lanes = lanes;
  atomic_init(&adapter->next_lane, 0u);
  adapter->schedulers = (coro_scheduler_t **)calloc(lanes, sizeof(*adapter->schedulers));
  adapter->pools = (salts_coro_pool_t **)calloc(lanes, sizeof(*adapter->pools));
  adapter->lane_mutexes = (salts_mutex_t *)calloc(lanes, sizeof(*adapter->lane_mutexes));
  if (!adapter->schedulers || !adapter->pools || !adapter->lane_mutexes) {
    flow_coro_adapter_destroy(adapter);
    return SALTS_ENOMEM;
  }

  for (uint32_t i = 0; i < lanes; ++i) {
    salts_mutex_init(&adapter->lane_mutexes[i]);
    adapter->schedulers[i] = coro_scheduler_create();
    if (!adapter->lane_mutexes[i] || !adapter->schedulers[i]) {
      flow_coro_adapter_destroy(adapter);
      return SALTS_ENOMEM;
    }
    if (executor->exec.pool_capacity > 0u) {
      salts_coro_pool_config_t config = SALTS_CORO_POOL_CONFIG_DEFAULT;
      config.initial_capacity = executor->exec.pool_capacity;
      config.max_capacity = executor->exec.pool_capacity;
      adapter->pools[i] = salts_coro_pool_create(&config);
      if (!adapter->pools[i]) {
        flow_coro_adapter_destroy(adapter);
        return SALTS_ENOMEM;
      }
    }
  }

  return SALTS_OK;
}

static int flow_coro_adapter_reset_lane(flow_coro_adapter_t *adapter, uint32_t lane) {
  coro_scheduler_t *replacement;

  if (!adapter || lane >= adapter->lanes || !adapter->schedulers) return SALTS_EINVAL;

  replacement = coro_scheduler_create();
  if (!replacement) return SALTS_ENOMEM;
  if (adapter->schedulers[lane]) coro_scheduler_destroy(adapter->schedulers[lane]);
  adapter->schedulers[lane] = replacement;
  return SALTS_OK;
}

void flow_stop_runtime_executor_adapters(turbo_flow_t *flow) {
  if (!flow) return;

  for (size_t i = 0; i < vec_size(&flow->threadpool_adapters); ++i) {
    flow_threadpool_adapter_t *adapter =
        (flow_threadpool_adapter_t *)vec_at(&flow->threadpool_adapters, i);
    flow_pool_record_t *record = flow_pool_record_at(flow, adapter->pool_record_index);
    flow_pool_record_set_state(record, TURBO_FLOW_POOL_DRAINING);
    if (adapter->pool) {
      salts_threadpool_destroy(adapter->pool);
      adapter->pool = NULL;
    }
    flow_pool_record_set_state(record, TURBO_FLOW_POOL_STOPPED);
  }
  turbo_flow_stl_error(vec_clear(&flow->threadpool_adapters));

  for (size_t i = 0; i < vec_size(&flow->coro_adapters); ++i) {
    flow_coro_adapter_t *adapter = (flow_coro_adapter_t *)vec_at(&flow->coro_adapters, i);
    flow_pool_record_t *record = flow_pool_record_at(flow, adapter->pool_record_index);
    flow_pool_record_set_state(record, TURBO_FLOW_POOL_DRAINING);
    flow_coro_adapter_destroy(adapter);
    flow_pool_record_set_state(record, TURBO_FLOW_POOL_STOPPED);
  }
  turbo_flow_stl_error(vec_clear(&flow->coro_adapters));
}

void flow_stop_executor_adapters(turbo_flow_t *flow) {
  if (!flow) return;
  flow_stop_runtime_executor_adapters(flow);
}

int flow_start_executor_adapters(turbo_flow_t *flow) {
  if (!flow) return SALTS_EINVAL;

  flow_stop_runtime_executor_adapters(flow);

  for (size_t i = 0; i < vec_size(&flow->executor_plans); ++i) {
    const flow_executor_plan_t *executor =
        (const flow_executor_plan_t *)vec_at_const(&flow->executor_plans, i);
    flow_threadpool_adapter_t adapter;
    flow_coro_adapter_t coro_adapter;

    if (executor->exec.kind == TURBO_FLOW_EXEC_CORO_POOL) {
      uint32_t lanes = executor->exec.lanes ? executor->exec.lanes : 1u;
      uint64_t resource_capacity =
          executor->exec.pool_capacity > 0u ? (uint64_t)lanes * executor->exec.pool_capacity : 0u;
      int rc = flow_coro_adapter_init(&coro_adapter, executor);
      if (rc != SALTS_OK) {
        flow_stop_runtime_executor_adapters(flow);
        return flow_set_error_keep_state(flow, rc, 0, 0, "failed to create coro executor");
      }
      rc = flow_pool_record_add(flow, TURBO_FLOW_POOL_CORO, executor->stage_index, lanes, 0u,
                                resource_capacity, &coro_adapter.pool_record_index);
      if (rc != SALTS_OK) {
        flow_coro_adapter_destroy(&coro_adapter);
        flow_stop_runtime_executor_adapters(flow);
        return flow_set_error_keep_state(flow, rc, 0, 0,
                                         "failed to create coro pool state");
      }
      if (turbo_flow_stl_error(vec_push(&flow->coro_adapters, &coro_adapter)) != SALTS_OK) {
        flow_pool_record_set_state(
            flow_pool_record_at(flow, coro_adapter.pool_record_index), TURBO_FLOW_POOL_FAILED);
        flow_coro_adapter_destroy(&coro_adapter);
        flow_stop_runtime_executor_adapters(flow);
        return flow_set_error_keep_state(flow, SALTS_ENOMEM, 0, 0, "out of memory");
      }
      flow_pool_record_set_state(flow_pool_record_at(flow, coro_adapter.pool_record_index),
                                 TURBO_FLOW_POOL_RUNNING);
      continue;
    }

    if (executor->exec.kind != TURBO_FLOW_EXEC_THREAD_POOL) continue;
    if (executor->exec.workers > (uint32_t)INT_MAX) {
      flow_stop_runtime_executor_adapters(flow);
      return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                       "thread executor worker count is too large");
    }

    adapter.stage_index = executor->stage_index;
    adapter.workers = 0u;
    adapter.pool = salts_threadpool_create((int)executor->exec.workers);
    if (!adapter.pool) {
      flow_stop_runtime_executor_adapters(flow);
      return flow_set_error_keep_state(flow, SALTS_ENOMEM, 0, 0,
                                       "failed to create thread executor");
    }
    adapter.workers = (uint32_t)salts_threadpool_size(adapter.pool);
    {
      int rc = flow_pool_record_add(flow, TURBO_FLOW_POOL_THREAD, executor->stage_index,
                                    adapter.workers, salts_threadpool_capacity(adapter.pool), 0u,
                                    &adapter.pool_record_index);
      if (rc != SALTS_OK) {
        salts_threadpool_destroy(adapter.pool);
        flow_stop_runtime_executor_adapters(flow);
        return flow_set_error_keep_state(flow, rc, 0, 0,
                                         "failed to create thread pool state");
      }
    }
    if (turbo_flow_stl_error(vec_push(&flow->threadpool_adapters, &adapter)) != SALTS_OK) {
      flow_pool_record_set_state(flow_pool_record_at(flow, adapter.pool_record_index),
                                 TURBO_FLOW_POOL_FAILED);
      salts_threadpool_destroy(adapter.pool);
      flow_stop_runtime_executor_adapters(flow);
      return flow_set_error_keep_state(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    }
    flow_pool_record_set_state(flow_pool_record_at(flow, adapter.pool_record_index),
                               TURBO_FLOW_POOL_RUNNING);
  }

  return SALTS_OK;
}

const flow_threadpool_adapter_t *flow_threadpool_adapter_for_stage(const turbo_flow_t *flow,
                                                                   uint32_t stage_index) {
  if (!flow) return NULL;
  for (size_t i = 0; i < vec_size(&flow->threadpool_adapters); ++i) {
    const flow_threadpool_adapter_t *adapter =
        (const flow_threadpool_adapter_t *)vec_at_const(&flow->threadpool_adapters, i);
    if (adapter->stage_index == stage_index) return adapter;
  }
  return NULL;
}

flow_coro_adapter_t *flow_coro_adapter_for_stage(turbo_flow_t *flow, uint32_t stage_index) {
  if (!flow) return NULL;
  for (size_t i = 0; i < vec_size(&flow->coro_adapters); ++i) {
    flow_coro_adapter_t *adapter = (flow_coro_adapter_t *)vec_at(&flow->coro_adapters, i);
    if (adapter->stage_index == stage_index) return adapter;
  }
  return NULL;
}

int flow_execute_threadpool_stage(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                  const flow_executor_plan_t *executor, uint32_t stage_index,
                                  turbo_flow_msg_t *msg, flow_stage_completion_t *completion) {
  const flow_threadpool_adapter_t *adapter = flow_threadpool_adapter_for_stage(flow, stage_index);
  flow_threadpool_task_t task;
  flow_pool_record_t *record;
  const turbo_flow_operation_runtime_contract_t *runtime =
      flow_stage_operation_runtime(flow, stage);
  int rc;

  if (!adapter || !adapter->pool) {
    completion->status = SALTS_EINVAL;
    return flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                     "thread executor is not started");
  }
  record = flow_pool_record_at(flow, adapter->pool_record_index);

  rc = flow_execution_task_init(&task.execution, FLOW_EXECUTION_THREAD, executor->fn, executor->ctx,
                                msg, completion, runtime ? runtime->deadline_ms : 0u);
  if (rc != SALTS_OK) {
    completion->status = rc;
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "thread executor message handoff failed");
  }
  task.pool_record = record;
  task.flow = flow;
  salts_mutex_init(&task.worker_mutex);
  salts_cond_init(&task.worker_cond);
  task.worker_finished = 0;
  flow_pool_record_submitted(record);

  if (salts_threadpool_submit(adapter->pool, flow_threadpool_task_run, &task) != SALTS_OK) {
    flow_pool_record_rejected(record);
    turbo_flow_msg_move(msg, &task.execution.msg);
    flow_execution_task_cleanup(&task.execution);
    salts_cond_destroy(&task.worker_cond);
    salts_mutex_destroy(&task.worker_mutex);
    completion->status = SALTS_ENOSPC;
    return flow_set_error_keep_state(flow, SALTS_ENOSPC, stage->line, stage->column,
                                     "thread executor rejected task");
  }

  rc = flow_execution_task_wait(&task.execution, msg, completion);
  salts_mutex_lock(&task.worker_mutex);
  while (!task.worker_finished) salts_cond_wait(&task.worker_cond, &task.worker_mutex);
  salts_mutex_unlock(&task.worker_mutex);
  flow_execution_task_cleanup(&task.execution);
  salts_cond_destroy(&task.worker_cond);
  salts_mutex_destroy(&task.worker_mutex);
  return rc;
}

static int flow_execute_coro_stage_locked(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                          const flow_executor_plan_t *executor,
                                          flow_coro_adapter_t *adapter, uint32_t lane,
                                          turbo_flow_msg_t *msg,
                                          flow_stage_completion_t *completion) {
  flow_coro_task_t task;
  const turbo_flow_operation_runtime_contract_t *runtime =
      flow_stage_operation_runtime(flow, stage);
  coro_scheduler_t *scheduler;
  coro_t *co;
  int rc;

  scheduler = adapter->schedulers[lane];
  if (!scheduler) {
    completion->status = SALTS_EINVAL;
    return flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                     "coro scheduler lane is not available");
  }

  rc = flow_execution_task_init(&task.execution, FLOW_EXECUTION_CORO, executor->fn, executor->ctx,
                                msg, completion, runtime ? runtime->deadline_ms : 0u);
  if (rc != SALTS_OK) {
    completion->status = rc;
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "coro message handoff failed");
  }
  task.flow = flow;

  if (adapter->pools[lane]) {
    task.pooled = 1;
    co = salts_coro_spawn_pooled(scheduler, adapter->pools[lane], flow_coro_task_run, &task);
    if (!co) {
      rc = turbo_flow_msg_move(msg, &task.execution.msg);
      flow_execution_task_cleanup(&task.execution);
      completion->status = rc == SALTS_OK ? SALTS_ENOSPC : rc;
      return flow_set_error_keep_state(flow, completion->status, stage->line, stage->column,
                                       "coro executor rejected task");
    }
  } else {
    task.pooled = 0;
    co = coro_spawn(scheduler, flow_coro_task_run, &task, NULL);
    if (!co) {
      rc = turbo_flow_msg_move(msg, &task.execution.msg);
      flow_execution_task_cleanup(&task.execution);
      completion->status = rc == SALTS_OK ? SALTS_ENOMEM : rc;
      return flow_set_error_keep_state(flow, completion->status, stage->line, stage->column,
                                       "coro executor rejected task");
    }
  }
  if (!task.pooled) coro_set_discard(co, flow_coro_task_discard, &task);

  while (coro_scheduler_count(scheduler) > 0) {
    if (!coro_scheduler_has_ready(scheduler)) {
      int reset_rc = flow_coro_adapter_reset_lane(adapter, lane);
      flow_execution_task_discard(&task.execution);
      rc = flow_execution_task_wait(&task.execution, msg, completion);
      flow_execution_task_cleanup(&task.execution);
      (void)rc;
      completion->status = reset_rc != SALTS_OK ? reset_rc : SALTS_ENOTSUP;
      return flow_set_error_keep_state(flow, completion->status, stage->line, stage->column,
                                       "coro executor suspended without ready work");
    }
    (void)coro_scheduler_tick(scheduler);
  }

  rc = flow_execution_task_wait(&task.execution, msg, completion);
  flow_execution_task_cleanup(&task.execution);
  return rc;
}

int flow_execute_coro_stage(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                            const flow_executor_plan_t *executor, uint32_t stage_index,
                            turbo_flow_msg_t *msg, flow_stage_completion_t *completion) {
  flow_coro_adapter_t *adapter = flow_coro_adapter_for_stage(flow, stage_index);
  flow_pool_record_t *record;
  uint32_t lane;
  int rc;

  if (!adapter || adapter->lanes == 0u || !adapter->schedulers || !adapter->pools ||
      !adapter->lane_mutexes) {
    completion->status = SALTS_EINVAL;
    return flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                     "coro executor is not started");
  }

  lane = atomic_fetch_add_explicit(&adapter->next_lane, 1u, memory_order_relaxed) % adapter->lanes;
  record = flow_pool_record_at(flow, adapter->pool_record_index);
  flow_pool_record_submitted(record);
  salts_mutex_lock(&adapter->lane_mutexes[lane]);
  flow_pool_record_started(record);
  rc = flow_execute_coro_stage_locked(flow, stage, executor, adapter, lane, msg, completion);
  flow_pool_record_finished(record, rc);
  salts_mutex_unlock(&adapter->lane_mutexes[lane]);
  return rc;
}
