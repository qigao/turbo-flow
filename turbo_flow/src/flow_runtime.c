#include "flow_internal.h"

#include "salts_buffer.h"

#include <limits.h>
#include <string.h>

#define FLOW_RUNTIME_STACK_STAGE_CAPACITY 64u

typedef struct flow_runtime_stack_workspace_s {
  uint8_t reachable[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
  uint8_t done[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
  uint32_t remaining[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
  uint32_t activated[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
  uint32_t queue[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
  uint32_t skipped_queue[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
  uint64_t stage_sequences[FLOW_RUNTIME_STACK_STAGE_CAPACITY];
} flow_runtime_stack_workspace_t;

typedef struct flow_runtime_workspace_s {
  uint8_t *reachable;
  uint8_t *done;
  uint32_t *remaining;
  uint32_t *activated;
  uint32_t *queue;
  uint32_t *skipped_queue;
  uint64_t *stage_sequences;
  void *pooled_storage;
} flow_runtime_workspace_t;

static size_t flow_runtime_align_offset(size_t offset, size_t alignment) {
  return (offset + alignment - 1u) & ~(alignment - 1u);
}

static int flow_runtime_workspace_init(flow_runtime_workspace_t *workspace,
                                       flow_runtime_stack_workspace_t *stack_workspace,
                                       size_t stage_count) {
  const size_t bytes_per_stage = sizeof(uint8_t) * 2u + sizeof(uint32_t) * 4u + sizeof(uint64_t);
  const size_t alignment_slack = sizeof(uint32_t) - 1u + sizeof(uint64_t) - 1u;
  unsigned char *storage;
  size_t offset = 0u;
  size_t allocation_size;

  if (!workspace || !stack_workspace || stage_count == 0u) return SALTS_EINVAL;
  memset(workspace, 0, sizeof(*workspace));
  if (stage_count <= FLOW_RUNTIME_STACK_STAGE_CAPACITY) {
    memset(stack_workspace->reachable, 0, stage_count * sizeof(*stack_workspace->reachable));
    memset(stack_workspace->done, 0, stage_count * sizeof(*stack_workspace->done));
    memset(stack_workspace->remaining, 0, stage_count * sizeof(*stack_workspace->remaining));
    memset(stack_workspace->activated, 0, stage_count * sizeof(*stack_workspace->activated));
    memset(stack_workspace->queue, 0, stage_count * sizeof(*stack_workspace->queue));
    memset(stack_workspace->skipped_queue, 0,
           stage_count * sizeof(*stack_workspace->skipped_queue));
    memset(stack_workspace->stage_sequences, 0,
           stage_count * sizeof(*stack_workspace->stage_sequences));
    workspace->reachable = stack_workspace->reachable;
    workspace->done = stack_workspace->done;
    workspace->remaining = stack_workspace->remaining;
    workspace->activated = stack_workspace->activated;
    workspace->queue = stack_workspace->queue;
    workspace->skipped_queue = stack_workspace->skipped_queue;
    workspace->stage_sequences = stack_workspace->stage_sequences;
    return SALTS_OK;
  }
  if (stage_count > (SIZE_MAX - alignment_slack) / bytes_per_stage) return SALTS_ERANGE;
  allocation_size = stage_count * bytes_per_stage + alignment_slack;
  storage = (unsigned char *)mem_alloc(mem_global(), allocation_size);
  if (!storage) return SALTS_ENOMEM;
  memset(storage, 0, allocation_size);
  workspace->pooled_storage = storage;
  workspace->reachable = storage + offset;
  offset += stage_count * sizeof(*workspace->reachable);
  workspace->done = storage + offset;
  offset += stage_count * sizeof(*workspace->done);
  offset = flow_runtime_align_offset(offset, sizeof(uint32_t));
  workspace->remaining = (uint32_t *)(void *)(storage + offset);
  offset += stage_count * sizeof(*workspace->remaining);
  workspace->activated = (uint32_t *)(void *)(storage + offset);
  offset += stage_count * sizeof(*workspace->activated);
  workspace->queue = (uint32_t *)(void *)(storage + offset);
  offset += stage_count * sizeof(*workspace->queue);
  workspace->skipped_queue = (uint32_t *)(void *)(storage + offset);
  offset += stage_count * sizeof(*workspace->skipped_queue);
  offset = flow_runtime_align_offset(offset, sizeof(uint64_t));
  workspace->stage_sequences = (uint64_t *)(void *)(storage + offset);
  return SALTS_OK;
}

static void flow_runtime_workspace_cleanup(flow_runtime_workspace_t *workspace) {
  if (!workspace || !workspace->pooled_storage) return;
  mem_free(mem_global(), workspace->pooled_storage);
  workspace->pooled_storage = NULL;
}

int flow_publish_enter(turbo_flow_t *flow) {
  int rc = SALTS_EINVAL;

  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state == TURBO_FLOW_STATE_STARTED && flow->admission_state == FLOW_ADMISSION_OPEN) {
    ++flow->active_publishes;
    rc = SALTS_OK;
  } else if (flow->state == TURBO_FLOW_STATE_STARTED) {
    rc = SALTS_ESHUTDOWN;
  }
  salts_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

void flow_publish_leave(turbo_flow_t *flow) {
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->active_publishes > 0u) --flow->active_publishes;
  if (flow->active_publishes == 0u) salts_cond_broadcast(&flow->runtime_cond);
  salts_mutex_unlock(&flow->runtime_mutex);
}

void flow_close_publish_admission(turbo_flow_t *flow) {
  if (!flow || !flow->runtime_sync_initialized) return;

  salts_mutex_lock(&flow->runtime_mutex);
  flow->admission_state = FLOW_ADMISSION_STOPPING;
  salts_cond_broadcast(&flow->runtime_cond);
  salts_mutex_unlock(&flow->runtime_mutex);
}

void flow_wait_for_publishes(turbo_flow_t *flow) {
  if (!flow || !flow->runtime_sync_initialized) return;

  salts_mutex_lock(&flow->runtime_mutex);
  while (flow->active_publishes > 0u) {
    salts_cond_wait(&flow->runtime_cond, &flow->runtime_mutex);
  }
  salts_mutex_unlock(&flow->runtime_mutex);
}

int turbo_flow_start(turbo_flow_t *flow) {
  int reactive_rc;
  if (!flow) return SALTS_EINVAL;
  if (flow->state != TURBO_FLOW_STATE_COMPILED && flow->state != TURBO_FLOW_STATE_STOPPED) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                     "flow must be compiled before start");
  }
  if (!flow->compiled_plan.sealed ||
      vec_size(&flow->compiled_plan.nodes) != vec_size(&flow->stages)) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                     "compiled runtime plan is not available");
  }
  if (vec_empty(&flow->compiled_plan.data_segments) && !vec_empty(&flow->compiled_plan.edges)) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                     "compiled data plan is not available");
  }
  if (flow_runtime_generation_can_advance(flow) != SALTS_OK) {
    return flow_set_error_keep_state(flow, SALTS_ERANGE, 0, 0, "runtime generation is exhausted");
  }
  turbo_flow_stl_error(vec_clear(&flow->pool_records));
  if (flow_start_data_planes(flow) != SALTS_OK) {
    turbo_flow_stl_error(vec_clear(&flow->pool_records));
    return flow->last_error.code;
  }
  if (flow_start_reorder_states(flow) != SALTS_OK) {
    flow_stop_data_planes(flow);
    turbo_flow_stl_error(vec_clear(&flow->pool_records));
    return flow->last_error.code;
  }
  if (flow_start_executor_adapters(flow) != SALTS_OK) {
    flow_stop_reorder_states(flow);
    flow_stop_data_planes(flow);
    turbo_flow_stl_error(vec_clear(&flow->pool_records));
    return flow->last_error.code;
  }
  reactive_rc = flow_reactive_runtime_start(flow);
  if (reactive_rc != SALTS_OK) {
    flow_stop_executor_adapters(flow);
    flow_stop_reorder_states(flow);
    flow_stop_data_planes(flow);
    turbo_flow_stl_error(vec_clear(&flow->pool_records));
    return flow_set_error_keep_state(flow, reactive_rc, 0, 0,
                                     "Reactive Scheduler initialization failed");
  }
  if (flow_start_adapters(flow) != SALTS_OK) {
    flow_reactive_runtime_stop(flow);
    flow_stop_executor_adapters(flow);
    flow_stop_reorder_states(flow);
    flow_stop_data_planes(flow);
    turbo_flow_stl_error(vec_clear(&flow->pool_records));
    return flow->last_error.code;
  }
  flow_runtime_generation_commit(flow);
  salts_mutex_lock(&flow->runtime_mutex);
  flow->state = TURBO_FLOW_STATE_STARTED;
  flow->admission_state = FLOW_ADMISSION_OPEN;
  flow->adapter_stop_retryable = 0;
  salts_mutex_unlock(&flow->runtime_mutex);
  return SALTS_OK;
}

int turbo_flow_pause(turbo_flow_t *flow) {
  int rc = SALTS_OK;
  if (!flow) return SALTS_EINVAL;
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    rc = SALTS_EINVAL;
  } else if (flow->admission_state == FLOW_ADMISSION_OPEN) {
    flow->admission_state = FLOW_ADMISSION_PAUSED;
  } else if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = SALTS_EBUSY;
  } else if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    rc = SALTS_ESHUTDOWN;
  }
  salts_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

int turbo_flow_resume(turbo_flow_t *flow) {
  int rc = SALTS_OK;
  if (!flow) return SALTS_EINVAL;
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    rc = SALTS_EINVAL;
  } else if (flow->admission_state == FLOW_ADMISSION_PAUSED) {
    flow->admission_state = FLOW_ADMISSION_OPEN;
  } else if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = SALTS_EBUSY;
  } else if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    rc = SALTS_ESHUTDOWN;
  }
  salts_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

int turbo_flow_drain(turbo_flow_t *flow, uint64_t timeout_ms) {
  uint64_t started_at;
  uint64_t timeout_ns;
  int rc = SALTS_OK;

  if (!flow) return SALTS_EINVAL;
  started_at = salts_hrtime();
  timeout_ns = timeout_ms == UINT64_MAX || timeout_ms > UINT64_MAX / UINT64_C(1000000)
                   ? UINT64_MAX
                   : timeout_ms * UINT64_C(1000000);

  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    rc = SALTS_EINVAL;
    goto done;
  }
  if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    rc = SALTS_ESHUTDOWN;
    goto done;
  }
  if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = SALTS_EBUSY;
    goto done;
  }
  flow->admission_state = FLOW_ADMISSION_PAUSED;
  while (flow->active_publishes > 0u) {
    uint64_t elapsed;
    if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
      rc = SALTS_ESHUTDOWN;
      break;
    }
    if (timeout_ns == UINT64_MAX) {
      salts_cond_wait(&flow->runtime_cond, &flow->runtime_mutex);
      continue;
    }
    elapsed = salts_hrtime() - started_at;
    if (elapsed >= timeout_ns) {
      rc = SALTS_ETIMEDOUT;
      break;
    }
    if (salts_cond_timedwait(&flow->runtime_cond, &flow->runtime_mutex, timeout_ns - elapsed) !=
            0 &&
        salts_hrtime() - started_at >= timeout_ns) {
      rc = SALTS_ETIMEDOUT;
      break;
    }
  }
done:
  salts_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

typedef struct flow_pool_resize_target_s {
  const flow_stage_plan_impl_t *stage;
  const flow_executor_plan_t *executor;
  const flow_data_segment_plan_t *segment;
  flow_runtime_stage_config_t *runtime_config;
  uint32_t previous_parallelism;
} flow_pool_resize_target_t;

static int flow_find_pool_resize_target(turbo_flow_t *flow,
                                        const turbo_flow_pool_resize_command_t *command,
                                        flow_pool_resize_target_t *target) {
  int stage_index;
  int found = 0;

  memset(target, 0, sizeof(*target));
  stage_index = turbo_flow_find_stage(flow, command->stage_name);
  if (stage_index < 0) return SALTS_ENOENT;
  target->stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, (size_t)stage_index);
  if (!target->stage) return SALTS_EINVAL;
  target->runtime_config = flow_runtime_stage_config_for_stage_mut(flow, (uint32_t)stage_index);
  if (!target->runtime_config) return SALTS_EPROTO;

  for (size_t i = 0; i < vec_size(&flow->pool_records); ++i) {
    const flow_pool_record_t *record =
        (const flow_pool_record_t *)vec_at_const(&flow->pool_records, i);
    if (record && record->stage_index == (uint32_t)stage_index && record->kind == command->kind) {
      found = 1;
      break;
    }
  }
  if (!found) return SALTS_ENOENT;

  if (command->kind == TURBO_FLOW_POOL_DISRUPTOR) {
    target->segment = flow_worker_pool_segment_for_stage(flow, (uint32_t)stage_index);
    if (!target->segment) return SALTS_EINVAL;
    target->previous_parallelism = target->runtime_config->data_workers;
  } else {
    target->executor = flow_executor_plan_for_stage(flow, (uint32_t)stage_index);
    if (!target->executor) return SALTS_EINVAL;
    if (command->kind == TURBO_FLOW_POOL_THREAD &&
        target->executor->exec.kind == TURBO_FLOW_EXEC_THREAD_POOL) {
      target->previous_parallelism = target->runtime_config->thread_workers;
    } else if (command->kind == TURBO_FLOW_POOL_CORO &&
               target->executor->exec.kind == TURBO_FLOW_EXEC_CORO_POOL) {
      target->previous_parallelism = target->runtime_config->coro_lanes;
    } else {
      return SALTS_EINVAL;
    }
  }
  return SALTS_OK;
}

static void flow_apply_pool_parallelism(flow_pool_resize_target_t *target,
                                        turbo_flow_pool_kind_t kind, uint32_t parallelism) {
  if (kind == TURBO_FLOW_POOL_DISRUPTOR) {
    target->runtime_config->data_workers = parallelism;
  } else if (kind == TURBO_FLOW_POOL_THREAD) {
    target->runtime_config->thread_workers = parallelism;
  } else {
    target->runtime_config->coro_lanes = parallelism;
  }
}

static int flow_rebuild_pool_resources(turbo_flow_t *flow) {
  int rc;

  flow_stop_data_planes(flow);
  flow_stop_runtime_executor_adapters(flow);
  turbo_flow_stl_error(vec_clear(&flow->pool_records));
  if (flow->pool_rebuild_fault.before_create != NULL) {
    flow->pool_rebuild_fault.attempts++;
    rc = flow->pool_rebuild_fault.before_create(flow->pool_rebuild_fault.ctx,
                                                flow->pool_rebuild_fault.attempts);
    if (rc != SALTS_OK) {
      return flow_set_error_keep_state(flow, rc, 0, 0, "pool resource rebuild failed");
    }
  }
  if (flow_start_data_planes(flow) != SALTS_OK) return flow->last_error.code;
  if (flow_start_executor_adapters(flow) != SALTS_OK) {
    flow_stop_data_planes(flow);
    return flow->last_error.code;
  }
  return SALTS_OK;
}

int turbo_flow_resize_pool(turbo_flow_t *flow, const turbo_flow_pool_resize_command_t *command) {
  flow_pool_resize_target_t target;
  flow_admission_state_t previous_admission;
  uint64_t started_at;
  uint64_t timeout_ns;
  int rc;
  int resize_rc;
  int adapter_stop_status;

  if (!flow || !command || command->size < sizeof(*command) || !command->stage_name ||
      command->parallelism == 0u || command->expected_generation == 0u ||
      command->kind < TURBO_FLOW_POOL_THREAD || command->kind > TURBO_FLOW_POOL_DISRUPTOR) {
    return SALTS_EINVAL;
  }
  if (command->kind == TURBO_FLOW_POOL_THREAD && command->parallelism > (uint32_t)INT_MAX) {
    return SALTS_ERANGE;
  }
  rc = flow_find_pool_resize_target(flow, command, &target);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, 0, 0, "resize target pool was not found");
  }

  started_at = salts_hrtime();
  timeout_ns = command->drain_timeout_ms == UINT64_MAX ||
                       command->drain_timeout_ms > UINT64_MAX / UINT64_C(1000000)
                   ? UINT64_MAX
                   : command->drain_timeout_ms * UINT64_C(1000000);
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                     "pool resize requires a started flow");
  }
  if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_ESHUTDOWN;
  }
  if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_EBUSY;
  }
  if (command->expected_generation != flow->runtime_generation) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0, "pool resource generation conflict");
  }
  if (target.previous_parallelism == command->parallelism) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_OK;
  }
  if (flow_runtime_generation_can_advance(flow) != SALTS_OK) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_ERANGE;
  }
  previous_admission = flow->admission_state;
  flow->admission_state = FLOW_ADMISSION_RESIZING;
  while (flow->active_publishes > 0u) {
    uint64_t elapsed;
    if (timeout_ns == UINT64_MAX) {
      salts_cond_wait(&flow->runtime_cond, &flow->runtime_mutex);
      continue;
    }
    elapsed = salts_hrtime() - started_at;
    if (elapsed >= timeout_ns || (salts_cond_timedwait(&flow->runtime_cond, &flow->runtime_mutex,
                                                       timeout_ns - elapsed) != 0 &&
                                  salts_hrtime() - started_at >= timeout_ns)) {
      flow->admission_state = FLOW_ADMISSION_PAUSED;
      salts_mutex_unlock(&flow->runtime_mutex);
      return SALTS_ETIMEDOUT;
    }
  }
  salts_mutex_unlock(&flow->runtime_mutex);

  flow_apply_pool_parallelism(&target, command->kind, command->parallelism);
  resize_rc = flow_rebuild_pool_resources(flow);
  if (resize_rc != SALTS_OK) {
    flow_apply_pool_parallelism(&target, command->kind, target.previous_parallelism);
    rc = flow_rebuild_pool_resources(flow);
    if (rc != SALTS_OK) {
      adapter_stop_status = flow_stop_adapters(flow);
      flow_stop_data_planes(flow);
      flow_stop_reorder_states(flow);
      flow_stop_executor_adapters(flow);
      salts_mutex_lock(&flow->runtime_mutex);
      if (adapter_stop_status != SALTS_OK) {
        (void)flow_set_error_keep_state(flow, adapter_stop_status, 0, 0,
                                        "adapter stop failed during pool resize rollback");
      } else {
        (void)flow_set_error_keep_state(flow, rc, 0, 0,
                                        "pool resize and rollback both failed");
      }
      flow->state = TURBO_FLOW_STATE_FAILED;
      flow->admission_state = FLOW_ADMISSION_CLOSED;
      flow->adapter_stop_retryable = adapter_stop_status != SALTS_OK;
      salts_cond_broadcast(&flow->runtime_cond);
      salts_mutex_unlock(&flow->runtime_mutex);
      return adapter_stop_status != SALTS_OK ? adapter_stop_status : rc;
    }
    flow_runtime_generation_commit(flow);
    salts_mutex_lock(&flow->runtime_mutex);
    flow->admission_state = previous_admission;
    salts_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, resize_rc, 0, 0,
                                     "pool resize failed; previous configuration restored");
  }

  flow_runtime_generation_commit(flow);

  salts_mutex_lock(&flow->runtime_mutex);
  flow->admission_state = previous_admission;
  salts_mutex_unlock(&flow->runtime_mutex);
  flow_clear_error(flow);
  return SALTS_OK;
}

int turbo_flow_stop(turbo_flow_t *flow) {
  int non_source_status;
  int source_status;
  int stop_status;
  int retrying;

  if (!flow) return SALTS_EINVAL;
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_EBUSY;
  }
  retrying = flow->state == TURBO_FLOW_STATE_FAILED && flow->adapter_stop_retryable;
  if (flow->state != TURBO_FLOW_STATE_STARTED && !retrying) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0, "flow is not started");
  }
  if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    salts_mutex_unlock(&flow->runtime_mutex);
    return SALTS_EBUSY;
  }
  flow->admission_state = FLOW_ADMISSION_STOPPING;
  flow->adapter_stop_callback_active = 0;
  flow->adapter_stop_callback_status = SALTS_OK;
  salts_cond_broadcast(&flow->runtime_cond);
  salts_mutex_unlock(&flow->runtime_mutex);

  if (!retrying) {
    flow_reactive_runtime_cancel(flow);
  }
  non_source_status = flow_stop_non_source_adapters(flow);
  flow_close_managed_source_runs(flow);
  if (!retrying) {
    flow_stop_async_ingress(flow);
  }
  flow_wait_for_publishes(flow);
  source_status = flow_stop_source_adapters(flow);
  stop_status = non_source_status != SALTS_OK ? non_source_status : source_status;

  if (!retrying) {
    flow_reactive_runtime_stop(flow);
    flow_stop_data_planes(flow);
    flow_stop_reorder_states(flow);
    flow_stop_executor_adapters(flow);
  }

  if (stop_status != SALTS_OK) {
    salts_mutex_lock(&flow->runtime_mutex);
    (void)flow_set_error_keep_state(flow, stop_status, 0, 0, "adapter stop failed");
    flow->state = TURBO_FLOW_STATE_FAILED;
    flow->admission_state = FLOW_ADMISSION_CLOSED;
    flow->adapter_stop_retryable = 1;
    salts_cond_broadcast(&flow->runtime_cond);
    salts_mutex_unlock(&flow->runtime_mutex);
    return stop_status;
  }

  turbo_flow_stl_error(vec_clear(&flow->active_adapters));
  salts_mutex_lock(&flow->runtime_mutex);
  flow->state = TURBO_FLOW_STATE_STOPPED;
  flow->admission_state = FLOW_ADMISSION_CLOSED;
  flow->adapter_stop_retryable = 0;
  salts_mutex_unlock(&flow->runtime_mutex);
  flow_clear_error(flow);
  return SALTS_OK;
}

static int flow_cancel_emission_descendant_reorders(turbo_flow_t *flow, uint32_t stage_index,
                                                    uint64_t *stage_sequences, size_t stage_count) {
  uint8_t stack_reachable[FLOW_RUNTIME_STACK_STAGE_CAPACITY] = {0};
  uint32_t stack_worklist[FLOW_RUNTIME_STACK_STAGE_CAPACITY] = {0};
  uint8_t *reachable = stack_reachable;
  uint32_t *worklist = stack_worklist;
  int pooled = 0;
  int rc = SALTS_OK;

  if (stage_count > FLOW_RUNTIME_STACK_STAGE_CAPACITY) {
    if (stage_count > SIZE_MAX / sizeof(*worklist)) return SALTS_ERANGE;
    reachable = (uint8_t *)mem_alloc(mem_global(), stage_count);
    worklist = (uint32_t *)mem_alloc(mem_global(), stage_count * sizeof(*worklist));
    if (!reachable || !worklist) {
      mem_free(mem_global(), reachable);
      mem_free(mem_global(), worklist);
      return SALTS_ENOMEM;
    }
    memset(reachable, 0, stage_count);
    pooled = 1;
  }
  rc = flow_mark_reachable_from_stage(flow, reachable, worklist, stage_count, stage_index);
  if (rc != SALTS_OK) goto cleanup;
  for (size_t i = 0u; i < stage_count; ++i) {
    int cancel_rc;
    if (i == stage_index || !reachable[i] || stage_sequences[i] == 0u) continue;
    cancel_rc = flow_reorder_cancel(flow, (uint32_t)i, stage_sequences[i]);
    if (rc == SALTS_OK && cancel_rc != SALTS_OK) rc = cancel_rc;
    stage_sequences[i] = 0u;
  }
cleanup:
  if (pooled) {
    mem_free(mem_global(), worklist);
    mem_free(mem_global(), reachable);
  }
  return rc;
}

int flow_run_message_from_stage(turbo_flow_t *flow, uint32_t origin_stage,
                                turbo_flow_msg_t *message) {
  flow_runtime_stack_workspace_t stack_workspace;
  flow_runtime_workspace_t workspace;
  uint8_t *reachable;
  uint8_t *done;
  uint32_t *remaining;
  uint32_t *activated;
  uint32_t *queue;
  uint32_t *skipped_queue;
  uint64_t *stage_sequences;
  size_t head = 0;
  size_t tail = 0;
  size_t stage_count = 0;
  int rc = SALTS_OK;
  flow_stage_completion_t completion = {0};
  uint64_t sequence;

  if (!flow || !message || origin_stage >= vec_size(&flow->stages)) return SALTS_EINVAL;

  stage_count = vec_size(&flow->stages);
  rc = flow_runtime_workspace_init(&workspace, &stack_workspace, stage_count);
  if (rc != SALTS_OK)
    return flow_set_error_keep_state(flow, rc, 0, 0,
                                     rc == SALTS_ERANGE ? "flow graph workspace is too large"
                                                       : "out of memory");
  reachable = workspace.reachable;
  done = workspace.done;
  remaining = workspace.remaining;
  activated = workspace.activated;
  queue = workspace.queue;
  skipped_queue = workspace.skipped_queue;
  stage_sequences = workspace.stage_sequences;

  rc = flow_mark_reachable_from_stage(flow, reachable, queue, stage_count, origin_stage);
  if (rc != SALTS_OK) goto cleanup;
  for (size_t i = 0; i < stage_count; ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (!reachable[i] || i == origin_stage || stage->is_source || stage->is_port) continue;
    rc = flow_dispatch_validate_stage(flow, (uint32_t)i);
    if (rc != SALTS_OK) goto cleanup;
  }

  salts_mutex_lock(&flow->runtime_mutex);
  sequence = atomic_fetch_add_explicit(&flow->next_sequence, 1u, memory_order_relaxed) + 1u;
  for (size_t i = 0; i < stage_count; ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (!reachable[i] || i == origin_stage || stage->is_source || stage->is_port) continue;
    stage_sequences[i] = sequence;
    if (stage->reorder.capacity > 0u) {
      rc = flow_reorder_reserve(flow, (uint32_t)i, &stage_sequences[i]);
      if (rc != SALTS_OK) {
        rc = flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                       "reorder boundary is not accepting publications");
        break;
      }
    }
  }
  salts_mutex_unlock(&flow->runtime_mutex);
  if (rc != SALTS_OK) goto cleanup;

  for (size_t i = 0; i < vec_size(&flow->compiled_plan.edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->compiled_plan.edges, i);
    if (reachable[edge->from_stage] && reachable[edge->to_stage]) {
      remaining[edge->to_stage] += 1;
    }
  }

  memset(&completion, 0, sizeof(completion));
  rc = flow_entry_header_init(flow, &completion.entry, origin_stage, FLOW_DATA_SEGMENT_DIRECT, 0u,
                              sequence, message->id, FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE,
                              &completion);
  if (rc != SALTS_OK) goto cleanup;
  completion.status = SALTS_OK;
  rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                             queue, stage_count, &tail, skipped_queue, stage_count);
  if (rc != SALTS_OK) goto cleanup;

  while (head < tail) {
    uint32_t stage_index = queue[head++];
    flow_stage_plan_impl_t *stage =
        (flow_stage_plan_impl_t *)vec_at(&flow->stages, (size_t)stage_index);

    if (done[stage_index]) continue;
    if (stage->is_port) {
      memset(&completion, 0, sizeof(completion));
      rc = flow_entry_header_init(flow, &completion.entry, stage_index, FLOW_DATA_SEGMENT_DIRECT,
                                  0u, sequence, message->id, FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE,
                                  &completion);
      if (rc != SALTS_OK) goto cleanup;
      completion.status = SALTS_OK;
      rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                                 queue, stage_count, &tail, skipped_queue, stage_count);
      if (rc != SALTS_OK) goto cleanup;
      continue;
    }

    {
      turbo_flow_emitter_t emitter;
      memset(&emitter, 0, sizeof(emitter));
      rc = flow_dispatch_stage(flow, stage_index, message, stage_sequences[stage_index],
                               message->id, &completion, &emitter);
      if (rc == SALTS_OK && (stage->emit_fn || stage->keyed_emit_fn || stage->window_fn)) {
        rc = flow_cancel_emission_descendant_reorders(flow, stage_index, stage_sequences,
                                                      stage_count);
        if (rc == SALTS_OK) {
          for (size_t output_index = 0u; output_index < vec_size(&emitter.outputs);
               ++output_index) {
            turbo_flow_msg_t *output =
                (turbo_flow_msg_t *)vec_at(&emitter.outputs, output_index);
            rc = flow_run_message_from_stage(flow, stage_index, output);
            if (rc != SALTS_OK) break;
          }
        }
        completion.terminal = rc == SALTS_OK;
      }
      if (stage->emit_fn || stage->keyed_emit_fn || stage->window_fn) {
        flow_emitter_cleanup(&emitter);
      }
    }
    if (rc != SALTS_OK && (stage->emit_fn || stage->keyed_emit_fn || stage->window_fn) &&
        completion.status == SALTS_OK) {
      goto cleanup;
    }
    if (rc != SALTS_OK) {
      rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                                 queue, stage_count, &tail, skipped_queue, stage_count);
      if (rc != SALTS_OK) goto cleanup;
      flow_clear_error(flow);
      continue;
    }
    if (completion.async_pending) {
      done[stage_index] = 1u;
      continue;
    }
    rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                               queue, stage_count, &tail, skipped_queue, stage_count);
    if (rc != SALTS_OK) goto cleanup;
  }

cleanup:
  if (stage_sequences) {
    for (size_t i = 0; i < stage_count; ++i) {
      if (stage_sequences[i] != 0u) {
        int cancel_rc = flow_reorder_cancel(flow, (uint32_t)i, stage_sequences[i]);
        if (rc == SALTS_OK && cancel_rc != SALTS_OK) rc = cancel_rc;
      }
    }
  }
  flow_runtime_workspace_cleanup(&workspace);
  return rc;
}

int turbo_flow_advance_event_time_watermark(turbo_flow_t *flow,
                                            turbo_flow_event_time_window_store_t *store,
                                            uint64_t watermark_ns, size_t *closed_windows) {
  const flow_executor_plan_t *window_executor = NULL;
  int entered = 0;
  int rc;

  if (closed_windows) *closed_windows = 0u;
  if (!flow || !store) return SALTS_EINVAL;
  flow_publish_error_context_begin(flow);
  rc = flow_publish_enter(flow);
  if (rc != SALTS_OK) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                   rc == SALTS_ESHUTDOWN
                                       ? "flow is not accepting watermark advancement"
                                       : "flow must be started before watermark advancement");
    goto cleanup_watermark;
  }
  entered = 1;
  flow_clear_error(flow);

  for (size_t index = 0u; index < vec_size(&flow->compiled_plan.executors); ++index) {
    const flow_executor_plan_t *executor =
        (const flow_executor_plan_t *)vec_at_const(&flow->compiled_plan.executors, index);
    if (executor && executor->window_fn && executor->keyed_store == store) {
      window_executor = executor;
      break;
    }
  }
  if (!window_executor) {
    rc = flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                   "event-time window store is not bound to this flow");
    goto cleanup_watermark;
  }

  rc = flow_event_time_window_advance(flow, window_executor->stage_index, store,
                                      window_executor->window_close_fn, window_executor->ctx,
                                      window_executor->max_outputs, watermark_ns, closed_windows);
  if (rc != SALTS_OK && flow_error_code(flow) != rc) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0, "event-time watermark advancement failed");
  }

cleanup_watermark:
  if (entered) flow_publish_leave(flow);
  flow_publish_error_context_end(flow);
  return rc;
}

int flow_publish_local(turbo_flow_t *flow, const char *source_name, uint32_t source_index,
                       turbo_flow_msg_t *local, uint64_t observe_start,
                       turbo_flow_publish_result_t *result,
                       flow_async_publication_t *async_publication) {
  turbo_flow_observe_event_t event;
  int rc;

  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_SOURCE_RECEIVED;
  event.source_name = source_name;
  event.msg = local;
  event.status = SALTS_OK;
  event.selected = -1;
  event.edge_kind = -1;
  flow_observer_emit(flow, &event);

  if (flow->broadcast_ring && !flow_msg_transport_context_is_borrowed(local)) {
    uint64_t sequence;
    salts_mutex_lock(&flow->runtime_mutex);
    sequence = atomic_fetch_add_explicit(&flow->next_sequence, 1u, memory_order_relaxed) + 1u;
    salts_mutex_unlock(&flow->runtime_mutex);
    salts_mutex_lock(&flow->broadcast_mutex);
    rc = flow_publish_broadcast_data_plane(flow, source_index, local, sequence, result);
    salts_mutex_unlock(&flow->broadcast_mutex);
  } else {
    rc = flow_run_message_from_stage(flow, source_index, local);
  }

  if (async_publication) {
    result->status = rc;
    flow_async_publication_seal(async_publication, rc);
    return rc;
  }
  if (flow->observer_ops.message_complete) {
    flow->observer_ops.message_complete(flow->observer_ctx, source_name, local,
                                        salts_hrtime() - observe_start, rc);
  }
  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_FLOW_COMPLETE;
  event.source_name = source_name;
  event.msg = local;
  event.status = rc;
  event.selected = -1;
  event.edge_kind = -1;
  event.duration_ns = observe_start != 0u ? salts_hrtime() - observe_start : 0u;
  flow_observer_emit(flow, &event);
  result->status = rc;
  return rc;
}

static int flow_publish_source_index(turbo_flow_t *flow, const char *source_name,
                                     uint32_t *source_index) {
  int found_index;
  const flow_stage_plan_impl_t *source;

  if (!flow || !source_name || !source_index) return SALTS_EINVAL;
  found_index = turbo_flow_find_stage(flow, source_name);
  if (found_index < 0) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                     "publish source is unknown");
  }
  source = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages,
                                                              (size_t)found_index);
  if (!source->is_source) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                     "publish target must be a source");
  }
  *source_index = (uint32_t)found_index;
  return SALTS_OK;
}

int flow_publish_message_entered(turbo_flow_t *flow, const char *source_name,
                                 int resolved_source_index, const turbo_flow_msg_t *msg,
                                 turbo_flow_publish_result_t *result,
                                 flow_async_publication_t *async_publication) {
  turbo_flow_msg_t local;
  uint32_t source_index = 0u;
  uint64_t observe_start = 0u;
  int local_initialized = 0;
  int async_sealed = 0;
  int rc;

  if (flow_observer_has_handlers(flow)) observe_start = salts_hrtime();
  flow_clear_error(flow);
  if (flow_msg_payload_validate(msg) != SALTS_OK) {
    rc = flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                   "publish payload must be within its backing buffer or owned payload");
    goto cleanup;
  }
  if (resolved_source_index < 0) {
    rc = flow_publish_source_index(flow, source_name, &source_index);
    if (rc != SALTS_OK) goto cleanup;
  } else {
    source_index = (uint32_t)resolved_source_index;
  }

  if (msg->owned_payload || msg->_content_handle) {
    rc = turbo_flow_msg_clone(&local, msg);
  } else {
    rc = turbo_flow_msg_retain_view(&local, msg);
  }
  if (rc != SALTS_OK) {
    const char *error_message = rc == SALTS_ENOTSUP ? "publish cannot clone the schema projection"
                                                    : "publish requires a cloneable payload view";
    rc = flow_set_error_keep_state(flow, rc, 0, 0, error_message);
    goto cleanup;
  }
  local_initialized = 1;
  rc = flow_publish_local(flow, source_name, source_index, &local, observe_start, result,
                          async_publication);
  async_sealed = async_publication != NULL;

cleanup:
  if (async_publication && !async_sealed) flow_async_publication_seal(async_publication, rc);
  if (local_initialized) turbo_flow_msg_cleanup(&local);
  result->status = rc;
  return rc;
}

int turbo_flow_publish_ex(turbo_flow_t *flow, const char *source_name, const turbo_flow_msg_t *msg,
                          turbo_flow_publish_result_t *result) {
  turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  turbo_flow_run_t *run = NULL;
  cflow_scheduler scheduler = {0};
  cflow_publisher publisher = {0};
  int rc;

  if (!flow || !source_name || !msg || !result || result->size < sizeof(*result)) {
    return SALTS_EINVAL;
  }
  *result = (turbo_flow_publish_result_t)TURBO_FLOW_PUBLISH_RESULT_INIT;
  flow_publish_error_context_begin(flow);
  if (flow_msg_payload_validate(msg) != SALTS_OK) {
    rc = flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                   "publish payload must be within its backing buffer or owned payload");
  } else if (!cflow_scheduler_inline_init(&scheduler)) {
    rc = flow_set_error_keep_state(flow, SALTS_ENOMEM, 0, 0,
                                   "inline Reactive Scheduler initialization failed");
  } else if (!cflow_publisher_from_array(&publisher, turbo_flow_message_type(), msg, 1u)) {
    rc = flow_set_error_keep_state(flow, SALTS_ENOMEM, 0, 0,
                                   "single-message Reactive Publisher initialization failed");
  } else {
    config.scheduler = &scheduler;
    rc = flow_run_open_internal(flow, source_name, &publisher, &config, 1, 0, &run);
    if (rc == SALTS_OK) rc = turbo_flow_run_request(run, 1u);
    if (rc == SALTS_OK) rc = turbo_flow_run_wait(run, UINT64_MAX, &run_result);
  }
  turbo_flow_run_close(run);
  if (cflow_publisher_valid(&publisher)) cflow_publisher_destroy(&publisher);
  if (cflow_scheduler_valid(&scheduler)) cflow_scheduler_destroy(&scheduler);
  flow_publish_error_context_end(flow);
  result->status = rc;
  return rc;
}

int turbo_flow_publish(turbo_flow_t *flow, const char *source_name, const turbo_flow_msg_t *msg) {
  turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
  return turbo_flow_publish_ex(flow, source_name, msg, &result);
}

typedef struct flow_publish_batch_next_context_s {
  turbo_flow_t *flow;
  turbo_flow_publish_batch_prepare_fn prepare;
  void *prepare_ctx;
  size_t message_count;
  size_t next_index;
  int failed;
  int protocol_error;
} flow_publish_batch_next_context_t;

static int flow_publish_batch_next(void *ctx, size_t index, turbo_flow_msg_t *message) {
  flow_publish_batch_next_context_t *next = (flow_publish_batch_next_context_t *)ctx;
  turbo_flow_msg_t prepared;
  int rc;

  if (message) turbo_flow_msg_init(message);
  if (!next || !next->flow || !next->prepare || !message) {
    return SALTS_EINVAL;
  }
  if (next->failed || index != next->next_index || index >= next->message_count) {
    next->protocol_error = 1;
    return flow_set_error_keep_state(next->flow, SALTS_EPROTO, 0, 0,
                                     "adapter batch iterator order is invalid");
  }
  turbo_flow_msg_init(&prepared);
  flow_clear_error(next->flow);
  rc = next->prepare(next->prepare_ctx, index, &prepared);
  if (rc != SALTS_OK) {
    next->failed = 1;
    rc = flow_set_error_keep_state(next->flow, rc, 0, 0,
                                   "batch message preparation failed");
    goto cleanup;
  }
  if (flow_msg_payload_validate(&prepared) != SALTS_OK) {
    next->failed = 1;
    rc = flow_set_error_keep_state(
        next->flow, SALTS_EINVAL, 0, 0,
        "publish payload must be within its backing buffer or owned payload");
    goto cleanup;
  }
  rc = prepared.owned_payload || prepared._content_handle
           ? turbo_flow_msg_clone(message, &prepared)
           : turbo_flow_msg_retain_view(message, &prepared);
  if (rc != SALTS_OK) {
    next->failed = 1;
    rc = flow_set_error_keep_state(
        next->flow, rc, 0, 0,
        rc == SALTS_ENOTSUP ? "publish cannot clone the schema projection"
                            : "publish requires a cloneable payload view");
  } else {
    next->next_index += 1u;
  }

cleanup:
  turbo_flow_msg_cleanup(&prepared);
  return rc;
}

static const flow_adapter_registration_t *flow_publish_batch_direct_adapter(
    turbo_flow_t *flow, uint32_t source_index, const flow_stage_plan_impl_t **out_stage) {
  const flow_runtime_edge_plan_t *source_edge = NULL;
  const flow_stage_plan_impl_t *stage;
  const flow_executor_plan_t *executor;
  const flow_adapter_registration_t *adapter;
  const turbo_flow_operation_runtime_contract_t *runtime;

  if (out_stage) *out_stage = NULL;
  if (!flow || !out_stage || flow->broadcast_ring || flow_observer_has_handlers(flow)) {
    return NULL;
  }
  for (size_t i = 0u; i < vec_size(&flow->compiled_plan.edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->compiled_plan.edges, i);
    if (!edge || edge->from_stage != source_index) continue;
    if (source_edge || edge->kind != TURBO_FLOW_EDGE_UNCONDITIONAL || edge->predicate) return NULL;
    source_edge = edge;
  }
  if (!source_edge) return NULL;
  for (size_t i = 0u; i < vec_size(&flow->compiled_plan.edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&flow->compiled_plan.edges, i);
    if (edge && edge->from_stage == source_edge->to_stage) return NULL;
  }
  stage = (const flow_stage_plan_impl_t *)vec_at_const(
      &flow->stages, (size_t)source_edge->to_stage);
  executor = flow_executor_plan_for_stage(flow, source_edge->to_stage);
  if (!stage || !executor || stage->is_source || stage->is_port ||
      stage->effects != TURBO_FLOW_STAGE_EFFECT_NONE ||
      stage->retry.max_attempts > 1u || stage->reorder.capacity != 0u ||
      executor->exec.kind != TURBO_FLOW_EXEC_INLINE || executor->fn || executor->emit_fn ||
      executor->keyed_fn || executor->keyed_emit_fn || executor->window_fn) {
    return NULL;
  }
  runtime = flow_stage_operation_runtime(flow, stage);
  if (runtime &&
      (runtime->handoff != TURBO_FLOW_HANDOFF_DIRECT || runtime->deadline_ms != 0u ||
       runtime->settlement != 0u)) {
    return NULL;
  }
  adapter = flow_adapter_for_compiled_stage(flow, source_edge->to_stage);
  if (!adapter || !adapter->ops.consume || !adapter->consume_batch) return NULL;
  *out_stage = stage;
  return adapter;
}

int turbo_flow_publish_batch(turbo_flow_t *flow, const char *source_name,
                             const turbo_flow_publish_batch_config_t *config,
                             size_t *published) {
  turbo_flow_publish_batch_prepare_fn prepare;
  size_t message_count;
  void *prepare_ctx;
  uint32_t source_index = 0u;
  int publish_entered = 0;
  int rc;

  if (published) *published = 0u;
  if (!flow || !source_name || !config || config->size < sizeof(*config) ||
      config->message_count == 0u || !config->prepare) {
    return SALTS_EINVAL;
  }
  message_count = config->message_count;
  prepare = config->prepare;
  prepare_ctx = config->ctx;
  flow_publish_error_context_begin(flow);
  rc = flow_publish_enter(flow);
  if (rc != SALTS_OK) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                   rc == SALTS_ESHUTDOWN
                                       ? "flow is not accepting publications"
                                       : "flow must be started before publish");
    goto cleanup;
  }
  publish_entered = 1;
  flow_clear_error(flow);
  rc = flow_publish_source_index(flow, source_name, &source_index);
  if (rc != SALTS_OK) goto cleanup;

  {
    const flow_stage_plan_impl_t *batch_stage = NULL;
    const flow_adapter_registration_t *batch_adapter =
        flow_publish_batch_direct_adapter(flow, source_index, &batch_stage);
    if (batch_adapter) {
      flow_publish_batch_next_context_t next_context = {
          flow, prepare, prepare_ctx, message_count, 0u, 0, 0};
      turbo_flow_adapter_batch_t adapter_batch = TURBO_FLOW_ADAPTER_BATCH_INIT;
      turbo_flow_stage_plan_t stage_view;
      size_t consumed = 0u;
      adapter_batch.message_count = message_count;
      adapter_batch.next = flow_publish_batch_next;
      adapter_batch.ctx = &next_context;
      flow_make_stage_view(batch_stage, &stage_view);
      rc = batch_adapter->consume_batch(batch_adapter->ctx, flow, &stage_view, &adapter_batch,
                                        &consumed);
      if (next_context.protocol_error || consumed > message_count ||
          consumed > next_context.next_index ||
          (rc == SALTS_OK &&
           (next_context.failed || consumed != message_count ||
            next_context.next_index != message_count)) ||
          (rc != SALTS_OK &&
           ((next_context.failed && consumed != next_context.next_index) ||
            (!next_context.failed &&
             next_context.next_index - consumed > 1u)))) {
        rc = flow_set_error_keep_state(flow, SALTS_EPROTO, batch_stage->line,
                                       batch_stage->column,
                                       "adapter batch completion count is invalid");
      } else if (rc != SALTS_OK && flow_error_code(flow) == SALTS_OK) {
        rc = flow_set_error_keep_state(flow, rc, batch_stage->line, batch_stage->column,
                                       "adapter batch consume failed");
      }
      if (published) *published = consumed <= message_count ? consumed : 0u;
      goto cleanup;
    }
  }

  for (size_t index = 0u; index < message_count; ++index) {
    turbo_flow_msg_t message;
    turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
    turbo_flow_msg_init(&message);
    rc = prepare(prepare_ctx, index, &message);
    if (rc != SALTS_OK) {
      rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                     "batch message preparation failed");
      turbo_flow_msg_cleanup(&message);
      break;
    }
    rc =
        flow_publish_message_entered(flow, source_name, (int)source_index, &message, &result, NULL);
    turbo_flow_msg_cleanup(&message);
    if (rc != SALTS_OK) break;
    if (published) *published = index + 1u;
  }

cleanup:
  if (publish_entered) flow_publish_leave(flow);
  flow_publish_error_context_end(flow);
  return rc;
}
