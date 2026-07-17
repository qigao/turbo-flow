#include "flow_internal.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

int flow_publish_enter(turbo_flow_t *flow) {
  int rc = TURBO_EINVAL;

  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->state == TURBO_FLOW_STATE_STARTED && flow->admission_state == FLOW_ADMISSION_OPEN) {
    ++flow->active_publishes;
    rc = TURBO_OK;
  } else if (flow->state == TURBO_FLOW_STATE_STARTED) {
    rc = TURBO_ESHUTDOWN;
  }
  turbo_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

void flow_publish_leave(turbo_flow_t *flow) {
  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->active_publishes > 0u) --flow->active_publishes;
  if (flow->active_publishes == 0u) turbo_cond_broadcast(&flow->runtime_cond);
  turbo_mutex_unlock(&flow->runtime_mutex);
}

void flow_close_publish_admission(turbo_flow_t *flow) {
  if (!flow || !flow->runtime_sync_initialized) return;

  turbo_mutex_lock(&flow->runtime_mutex);
  flow->admission_state = FLOW_ADMISSION_STOPPING;
  turbo_cond_broadcast(&flow->runtime_cond);
  turbo_mutex_unlock(&flow->runtime_mutex);
}

void flow_wait_for_publishes(turbo_flow_t *flow) {
  if (!flow || !flow->runtime_sync_initialized) return;

  turbo_mutex_lock(&flow->runtime_mutex);
  while (flow->active_publishes > 0u) {
    turbo_cond_wait(&flow->runtime_cond, &flow->runtime_mutex);
  }
  turbo_mutex_unlock(&flow->runtime_mutex);
}

int turbo_flow_start(turbo_flow_t *flow) {
  if (!flow) return TURBO_EINVAL;
  if (flow->state != TURBO_FLOW_STATE_COMPILED && flow->state != TURBO_FLOW_STATE_STOPPED) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "flow must be compiled before start");
  }
  if (turbo_vec_size(&flow->runtime_nodes) != turbo_vec_size(&flow->stages)) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "compiled runtime plan is not available");
  }
  if (turbo_vec_empty(&flow->data_segments) && !turbo_vec_empty(&flow->runtime_edges)) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "compiled data plan is not available");
  }
  if (flow_runtime_generation_can_advance(flow) != TURBO_OK) {
    return flow_set_error_keep_state(flow, TURBO_ERANGE, 0, 0, "runtime generation is exhausted");
  }
  turbo_vec_clear(&flow->pool_records);
  if (flow_start_data_planes(flow) != TURBO_OK) {
    turbo_vec_clear(&flow->pool_records);
    return flow->last_error.code;
  }
  if (flow_start_reorder_states(flow) != TURBO_OK) {
    flow_stop_data_planes(flow);
    turbo_vec_clear(&flow->pool_records);
    return flow->last_error.code;
  }
  if (flow_start_executor_adapters(flow) != TURBO_OK) {
    flow_stop_reorder_states(flow);
    flow_stop_data_planes(flow);
    turbo_vec_clear(&flow->pool_records);
    return flow->last_error.code;
  }
  if (flow_start_adapters(flow) != TURBO_OK) {
    flow_stop_executor_adapters(flow);
    flow_stop_reorder_states(flow);
    flow_stop_data_planes(flow);
    turbo_vec_clear(&flow->pool_records);
    return flow->last_error.code;
  }
  flow_runtime_generation_commit(flow);
  turbo_mutex_lock(&flow->runtime_mutex);
  flow->state = TURBO_FLOW_STATE_STARTED;
  flow->admission_state = FLOW_ADMISSION_OPEN;
  turbo_mutex_unlock(&flow->runtime_mutex);
  return TURBO_OK;
}

int turbo_flow_pause(turbo_flow_t *flow) {
  int rc = TURBO_OK;
  if (!flow) return TURBO_EINVAL;
  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    rc = TURBO_EINVAL;
  } else if (flow->admission_state == FLOW_ADMISSION_OPEN) {
    flow->admission_state = FLOW_ADMISSION_PAUSED;
  } else if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = TURBO_EBUSY;
  } else if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    rc = TURBO_ESHUTDOWN;
  }
  turbo_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

int turbo_flow_resume(turbo_flow_t *flow) {
  int rc = TURBO_OK;
  if (!flow) return TURBO_EINVAL;
  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    rc = TURBO_EINVAL;
  } else if (flow->admission_state == FLOW_ADMISSION_PAUSED) {
    flow->admission_state = FLOW_ADMISSION_OPEN;
  } else if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = TURBO_EBUSY;
  } else if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    rc = TURBO_ESHUTDOWN;
  }
  turbo_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

int turbo_flow_drain(turbo_flow_t *flow, uint64_t timeout_ms) {
  uint64_t started_at;
  uint64_t timeout_ns;
  int rc = TURBO_OK;

  if (!flow) return TURBO_EINVAL;
  started_at = turbo_hrtime();
  timeout_ns = timeout_ms == UINT64_MAX || timeout_ms > UINT64_MAX / UINT64_C(1000000)
                   ? UINT64_MAX
                   : timeout_ms * UINT64_C(1000000);

  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    rc = TURBO_EINVAL;
    goto done;
  }
  if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    rc = TURBO_ESHUTDOWN;
    goto done;
  }
  if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = TURBO_EBUSY;
    goto done;
  }
  flow->admission_state = FLOW_ADMISSION_PAUSED;
  while (flow->active_publishes > 0u) {
    uint64_t elapsed;
    if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    if (timeout_ns == UINT64_MAX) {
      turbo_cond_wait(&flow->runtime_cond, &flow->runtime_mutex);
      continue;
    }
    elapsed = turbo_hrtime() - started_at;
    if (elapsed >= timeout_ns) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
    if (turbo_cond_timedwait(&flow->runtime_cond, &flow->runtime_mutex, timeout_ns - elapsed) !=
            0 &&
        turbo_hrtime() - started_at >= timeout_ns) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
  }
done:
  turbo_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

typedef struct flow_pool_resize_target_s {
  flow_stage_plan_impl_t *stage;
  flow_executor_plan_t *executor;
  flow_data_segment_plan_t *segment;
  uint32_t previous_parallelism;
} flow_pool_resize_target_t;

static int flow_find_pool_resize_target(turbo_flow_t *flow,
                                        const turbo_flow_pool_resize_command_t *command,
                                        flow_pool_resize_target_t *target) {
  int stage_index;
  int found = 0;

  memset(target, 0, sizeof(*target));
  stage_index = turbo_flow_find_stage(flow, command->stage_name);
  if (stage_index < 0) return TURBO_ENOENT;
  target->stage = (flow_stage_plan_impl_t *)turbo_vec_at(&flow->stages, (size_t)stage_index);
  if (!target->stage) return TURBO_EINVAL;

  for (size_t i = 0; i < turbo_vec_size(&flow->pool_records); ++i) {
    const flow_pool_record_t *record =
        (const flow_pool_record_t *)turbo_vec_at_const(&flow->pool_records, i);
    if (record && record->stage_index == (uint32_t)stage_index && record->kind == command->kind) {
      found = 1;
      break;
    }
  }
  if (!found) return TURBO_ENOENT;

  if (command->kind == TURBO_FLOW_POOL_DISRUPTOR) {
    target->segment = flow_worker_pool_segment_for_stage(flow, (uint32_t)stage_index);
    if (!target->segment) return TURBO_EINVAL;
    target->previous_parallelism = target->stage->data_worker_count;
  } else {
    target->executor =
        (flow_executor_plan_t *)flow_executor_plan_for_stage(flow, (uint32_t)stage_index);
    if (!target->executor) return TURBO_EINVAL;
    if (command->kind == TURBO_FLOW_POOL_THREAD &&
        target->executor->exec.kind == TURBO_FLOW_EXEC_THREAD_POOL) {
      target->previous_parallelism = target->executor->exec.workers;
    } else if (command->kind == TURBO_FLOW_POOL_CORO &&
               target->executor->exec.kind == TURBO_FLOW_EXEC_CORO_POOL) {
      target->previous_parallelism = target->executor->exec.lanes;
    } else {
      return TURBO_EINVAL;
    }
  }
  return TURBO_OK;
}

static void flow_apply_pool_parallelism(flow_pool_resize_target_t *target,
                                        turbo_flow_pool_kind_t kind, uint32_t parallelism) {
  if (kind == TURBO_FLOW_POOL_DISRUPTOR) {
    target->stage->data_worker_count = parallelism;
    target->segment->width = parallelism;
  } else if (kind == TURBO_FLOW_POOL_THREAD) {
    target->stage->exec.workers = parallelism;
    target->executor->exec.workers = parallelism;
  } else {
    target->stage->exec.lanes = parallelism;
    target->executor->exec.lanes = parallelism;
  }
}

static int flow_rebuild_pool_resources(turbo_flow_t *flow) {
  flow_stop_data_planes(flow);
  flow_stop_runtime_executor_adapters(flow);
  turbo_vec_clear(&flow->pool_records);
  if (flow_start_data_planes(flow) != TURBO_OK) return flow->last_error.code;
  if (flow_start_executor_adapters(flow) != TURBO_OK) {
    flow_stop_data_planes(flow);
    return flow->last_error.code;
  }
  return TURBO_OK;
}

int turbo_flow_resize_pool(turbo_flow_t *flow, const turbo_flow_pool_resize_command_t *command) {
  flow_pool_resize_target_t target;
  flow_admission_state_t previous_admission;
  uint64_t started_at;
  uint64_t timeout_ns;
  int rc;
  int resize_rc;

  if (!flow || !command || command->size < sizeof(*command) || !command->stage_name ||
      command->parallelism == 0u || command->expected_generation == 0u ||
      command->kind < TURBO_FLOW_POOL_THREAD || command->kind > TURBO_FLOW_POOL_DISRUPTOR) {
    return TURBO_EINVAL;
  }
  if (command->kind == TURBO_FLOW_POOL_THREAD && command->parallelism > (uint32_t)INT_MAX) {
    return TURBO_ERANGE;
  }
  rc = flow_find_pool_resize_target(flow, command, &target);
  if (rc != TURBO_OK) {
    return flow_set_error_keep_state(flow, rc, 0, 0, "resize target pool was not found");
  }

  started_at = turbo_hrtime();
  timeout_ns = command->drain_timeout_ms == UINT64_MAX ||
                       command->drain_timeout_ms > UINT64_MAX / UINT64_C(1000000)
                   ? UINT64_MAX
                   : command->drain_timeout_ms * UINT64_C(1000000);
  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "pool resize requires a started flow");
  }
  if (flow->admission_state == FLOW_ADMISSION_STOPPING) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return TURBO_ESHUTDOWN;
  }
  if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return TURBO_EBUSY;
  }
  if (command->expected_generation != flow->runtime_generation) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, TURBO_EBUSY, 0, 0, "pool resource generation conflict");
  }
  if (target.previous_parallelism == command->parallelism) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return TURBO_OK;
  }
  if (flow_runtime_generation_can_advance(flow) != TURBO_OK) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return TURBO_ERANGE;
  }
  previous_admission = flow->admission_state;
  flow->admission_state = FLOW_ADMISSION_RESIZING;
  while (flow->active_publishes > 0u) {
    uint64_t elapsed;
    if (timeout_ns == UINT64_MAX) {
      turbo_cond_wait(&flow->runtime_cond, &flow->runtime_mutex);
      continue;
    }
    elapsed = turbo_hrtime() - started_at;
    if (elapsed >= timeout_ns || (turbo_cond_timedwait(&flow->runtime_cond, &flow->runtime_mutex,
                                                       timeout_ns - elapsed) != 0 &&
                                  turbo_hrtime() - started_at >= timeout_ns)) {
      flow->admission_state = FLOW_ADMISSION_PAUSED;
      turbo_mutex_unlock(&flow->runtime_mutex);
      return TURBO_ETIMEDOUT;
    }
  }
  turbo_mutex_unlock(&flow->runtime_mutex);

  flow_apply_pool_parallelism(&target, command->kind, command->parallelism);
  resize_rc = flow_rebuild_pool_resources(flow);
  if (resize_rc != TURBO_OK) {
    flow_apply_pool_parallelism(&target, command->kind, target.previous_parallelism);
    rc = flow_rebuild_pool_resources(flow);
    if (rc != TURBO_OK) {
      flow_stop_adapters(flow);
      flow_stop_data_planes(flow);
      flow_stop_reorder_states(flow);
      flow_stop_executor_adapters(flow);
      turbo_mutex_lock(&flow->runtime_mutex);
      flow->state = TURBO_FLOW_STATE_FAILED;
      flow->admission_state = FLOW_ADMISSION_CLOSED;
      turbo_mutex_unlock(&flow->runtime_mutex);
      return flow_set_error_keep_state(flow, rc, 0, 0, "pool resize and rollback both failed");
    }
    flow_runtime_generation_commit(flow);
    turbo_mutex_lock(&flow->runtime_mutex);
    flow->admission_state = previous_admission;
    turbo_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, resize_rc, 0, 0,
                                     "pool resize failed; previous configuration restored");
  }

  flow_runtime_generation_commit(flow);

  turbo_mutex_lock(&flow->runtime_mutex);
  flow->admission_state = previous_admission;
  turbo_mutex_unlock(&flow->runtime_mutex);
  flow_clear_error(flow);
  return TURBO_OK;
}

int turbo_flow_stop(turbo_flow_t *flow) {
  if (!flow) return TURBO_EINVAL;
  turbo_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED || flow->admission_state == FLOW_ADMISSION_STOPPING) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0, "flow is not started");
  }
  if (flow->admission_state == FLOW_ADMISSION_RESIZING) {
    turbo_mutex_unlock(&flow->runtime_mutex);
    return TURBO_EBUSY;
  }
  flow->admission_state = FLOW_ADMISSION_STOPPING;
  turbo_cond_broadcast(&flow->runtime_cond);
  turbo_mutex_unlock(&flow->runtime_mutex);

  flow_stop_adapters(flow);
  flow_wait_for_publishes(flow);
  flow_stop_async_ingress(flow);
  flow_stop_data_planes(flow);
  flow_stop_reorder_states(flow);
  flow_stop_executor_adapters(flow);
  turbo_mutex_lock(&flow->runtime_mutex);
  flow->state = TURBO_FLOW_STATE_STOPPED;
  flow->admission_state = FLOW_ADMISSION_CLOSED;
  turbo_mutex_unlock(&flow->runtime_mutex);
  return TURBO_OK;
}

static int flow_cancel_emission_descendant_reorders(turbo_flow_t *flow, uint32_t stage_index,
                                                    uint64_t *stage_sequences, size_t stage_count) {
  uint8_t *reachable = NULL;
  int rc = TURBO_OK;

  reachable = (uint8_t *)calloc(stage_count, sizeof(*reachable));
  if (!reachable) return TURBO_ENOMEM;
  flow_mark_reachable_from_stage(flow, reachable, stage_index);
  for (size_t i = 0u; i < stage_count; ++i) {
    int cancel_rc;
    if (i == stage_index || !reachable[i] || stage_sequences[i] == 0u) continue;
    cancel_rc = flow_reorder_cancel(flow, (uint32_t)i, stage_sequences[i]);
    if (rc == TURBO_OK && cancel_rc != TURBO_OK) rc = cancel_rc;
    stage_sequences[i] = 0u;
  }
  free(reachable);
  return rc;
}

int flow_run_message_from_stage(turbo_flow_t *flow, uint32_t origin_stage,
                                turbo_flow_msg_t *message) {
  uint8_t *reachable = NULL;
  uint8_t *done = NULL;
  uint32_t *remaining = NULL;
  uint32_t *activated = NULL;
  uint32_t *queue = NULL;
  uint64_t *stage_sequences = NULL;
  size_t head = 0;
  size_t tail = 0;
  size_t stage_count = 0;
  int rc = TURBO_OK;
  flow_stage_completion_t completion = {0};
  uint64_t sequence;

  if (!flow || !message || origin_stage >= turbo_vec_size(&flow->stages)) return TURBO_EINVAL;

  stage_count = turbo_vec_size(&flow->stages);
  reachable = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  done = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  remaining = (uint32_t *)calloc(stage_count, sizeof(uint32_t));
  activated = (uint32_t *)calloc(stage_count, sizeof(uint32_t));
  queue = (uint32_t *)calloc(stage_count, sizeof(uint32_t));
  stage_sequences = (uint64_t *)calloc(stage_count, sizeof(uint64_t));
  if (!reachable || !done || !remaining || !activated || !queue || !stage_sequences) {
    rc = flow_set_error_keep_state(flow, TURBO_ENOMEM, 0, 0, "out of memory");
    goto cleanup;
  }

  flow_mark_reachable_from_stage(flow, reachable, origin_stage);
  for (size_t i = 0; i < stage_count; ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, i);
    if (!reachable[i] || i == origin_stage || stage->is_source || stage->is_port) continue;
    rc = flow_dispatch_validate_stage(flow, (uint32_t)i);
    if (rc != TURBO_OK) goto cleanup;
  }

  turbo_mutex_lock(&flow->runtime_mutex);
  sequence = atomic_fetch_add_explicit(&flow->next_sequence, 1u, memory_order_relaxed) + 1u;
  for (size_t i = 0; i < stage_count; ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, i);
    if (!reachable[i] || i == origin_stage || stage->is_source || stage->is_port) continue;
    stage_sequences[i] = sequence;
    if (stage->reorder.capacity > 0u) {
      rc = flow_reorder_reserve(flow, (uint32_t)i, &stage_sequences[i]);
      if (rc != TURBO_OK) {
        rc = flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                       "reorder boundary is not accepting publications");
        break;
      }
    }
  }
  turbo_mutex_unlock(&flow->runtime_mutex);
  if (rc != TURBO_OK) goto cleanup;

  for (size_t i = 0; i < turbo_vec_size(&flow->runtime_edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)turbo_vec_at_const(&flow->runtime_edges, i);
    if (reachable[edge->from_stage] && reachable[edge->to_stage]) {
      remaining[edge->to_stage] += 1;
    }
  }

  memset(&completion, 0, sizeof(completion));
  rc = flow_entry_header_init(flow, &completion.entry, origin_stage, FLOW_DATA_SEGMENT_DIRECT, 0u,
                              sequence, message->id, FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE,
                              &completion);
  if (rc != TURBO_OK) goto cleanup;
  completion.status = TURBO_OK;
  rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                             queue, stage_count, &tail);
  if (rc != TURBO_OK) goto cleanup;

  while (head < tail) {
    uint32_t stage_index = queue[head++];
    flow_stage_plan_impl_t *stage =
        (flow_stage_plan_impl_t *)turbo_vec_at(&flow->stages, (size_t)stage_index);

    if (done[stage_index]) continue;
    if (stage->is_port) {
      memset(&completion, 0, sizeof(completion));
      rc = flow_entry_header_init(flow, &completion.entry, stage_index, FLOW_DATA_SEGMENT_DIRECT,
                                  0u, sequence, message->id, FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE,
                                  &completion);
      if (rc != TURBO_OK) goto cleanup;
      completion.status = TURBO_OK;
      rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                                 queue, stage_count, &tail);
      if (rc != TURBO_OK) goto cleanup;
      continue;
    }

    {
      turbo_flow_emitter_t emitter;
      memset(&emitter, 0, sizeof(emitter));
      rc = flow_dispatch_stage(flow, stage_index, message, stage_sequences[stage_index],
                               message->id, &completion, &emitter);
      if (rc == TURBO_OK && (stage->emit_fn || stage->keyed_emit_fn || stage->window_fn)) {
        rc = flow_cancel_emission_descendant_reorders(flow, stage_index, stage_sequences,
                                                      stage_count);
        if (rc == TURBO_OK) {
          for (size_t output_index = 0u; output_index < turbo_vec_size(&emitter.outputs);
               ++output_index) {
            turbo_flow_msg_t *output =
                (turbo_flow_msg_t *)turbo_vec_at(&emitter.outputs, output_index);
            rc = flow_run_message_from_stage(flow, stage_index, output);
            if (rc != TURBO_OK) break;
          }
        }
        completion.terminal = rc == TURBO_OK;
      }
      if (stage->emit_fn || stage->keyed_emit_fn || stage->window_fn) {
        flow_emitter_cleanup(&emitter);
      }
    }
    if (rc != TURBO_OK && (stage->emit_fn || stage->keyed_emit_fn || stage->window_fn) &&
        completion.status == TURBO_OK) {
      goto cleanup;
    }
    if (rc != TURBO_OK) {
      rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                                 queue, stage_count, &tail);
      if (rc != TURBO_OK) goto cleanup;
      flow_clear_error(flow);
      continue;
    }
    rc = flow_apply_completion(flow, &completion, message, done, reachable, remaining, activated,
                               queue, stage_count, &tail);
    if (rc != TURBO_OK) goto cleanup;
  }

cleanup:
  if (stage_sequences) {
    for (size_t i = 0; i < stage_count; ++i) {
      if (stage_sequences[i] != 0u) {
        int cancel_rc = flow_reorder_cancel(flow, (uint32_t)i, stage_sequences[i]);
        if (rc == TURBO_OK && cancel_rc != TURBO_OK) rc = cancel_rc;
      }
    }
  }
  free(reachable);
  free(done);
  free(remaining);
  free(activated);
  free(queue);
  free(stage_sequences);
  return rc;
}

int turbo_flow_advance_event_time_watermark(turbo_flow_t *flow,
                                            turbo_flow_event_time_window_store_t *store,
                                            uint64_t watermark_ns, size_t *closed_windows) {
  const flow_executor_plan_t *window_executor = NULL;
  int entered = 0;
  int rc;

  if (closed_windows) *closed_windows = 0u;
  if (!flow || !store) return TURBO_EINVAL;
  flow_publish_error_context_begin(flow);
  rc = flow_publish_enter(flow);
  if (rc != TURBO_OK) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                   rc == TURBO_ESHUTDOWN
                                       ? "flow is not accepting watermark advancement"
                                       : "flow must be started before watermark advancement");
    goto cleanup_watermark;
  }
  entered = 1;
  flow_clear_error(flow);

  for (size_t index = 0u; index < turbo_vec_size(&flow->executor_plans); ++index) {
    const flow_executor_plan_t *executor =
        (const flow_executor_plan_t *)turbo_vec_at_const(&flow->executor_plans, index);
    if (executor && executor->window_fn && executor->keyed_store == store) {
      window_executor = executor;
      break;
    }
  }
  if (!window_executor) {
    rc = flow_set_error_keep_state(flow, TURBO_ENOENT, 0, 0,
                                   "event-time window store is not bound to this flow");
    goto cleanup_watermark;
  }

  rc = flow_event_time_window_advance(flow, window_executor->stage_index, store,
                                      window_executor->window_close_fn, window_executor->ctx,
                                      window_executor->max_outputs, watermark_ns, closed_windows);
  if (rc != TURBO_OK && flow_error_code(flow) != rc) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0, "event-time watermark advancement failed");
  }

cleanup_watermark:
  if (entered) flow_publish_leave(flow);
  flow_publish_error_context_end(flow);
  return rc;
}

int flow_publish_local(turbo_flow_t *flow, const char *source_name, uint32_t source_index,
                       turbo_flow_msg_t *local, uint64_t observe_start,
                       turbo_flow_publish_result_t *result) {
  int rc;

  if (flow->broadcast_ring && !flow_msg_transport_context_is_borrowed(local)) {
    uint64_t sequence;
    turbo_mutex_lock(&flow->runtime_mutex);
    sequence = atomic_fetch_add_explicit(&flow->next_sequence, 1u, memory_order_relaxed) + 1u;
    turbo_mutex_unlock(&flow->runtime_mutex);
    turbo_mutex_lock(&flow->broadcast_mutex);
    rc = flow_publish_broadcast_data_plane(flow, source_index, local, sequence, result);
    turbo_mutex_unlock(&flow->broadcast_mutex);
  } else {
    rc = flow_run_message_from_stage(flow, source_index, local);
  }

  if (flow->observer_ops.message_complete) {
    flow->observer_ops.message_complete(flow->observer_ctx, source_name, local,
                                        turbo_hrtime() - observe_start, rc);
  }
  {
    const turbo_flow_protocol_settlement_envelope_t *settlement =
        turbo_flow_msg_protocol_settlement(local);
    if (settlement) result->protocol_settlement = settlement->settled_point;
  }
  result->status = rc;
  return rc;
}

int turbo_flow_publish_ex(turbo_flow_t *flow, const char *source_name, const turbo_flow_msg_t *msg,
                          turbo_flow_publish_result_t *result) {
  turbo_flow_msg_t local;
  int source_index;
  int rc = TURBO_OK;
  uint64_t observe_start = 0u;
  int publish_entered = 0;
  int local_initialized = 0;

  if (!flow || !source_name || !msg || !result || result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  *result = (turbo_flow_publish_result_t)TURBO_FLOW_PUBLISH_RESULT_INIT;
  flow_publish_error_context_begin(flow);
  rc = flow_publish_enter(flow);
  if (rc != TURBO_OK) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                   rc == TURBO_ESHUTDOWN ? "flow is not accepting publications"
                                                         : "flow must be started before publish");
    goto cleanup;
  }
  publish_entered = 1;
  if (flow->observer_ops.message_complete) observe_start = turbo_hrtime();
  flow_clear_error(flow);

  if (!msg->buffer && !msg->owned_payload && msg->payload.data) {
    rc = flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                   "publish payload requires a backing buffer or owned payload");
    goto cleanup;
  }
  source_index = turbo_flow_find_stage(flow, source_name);
  if (source_index < 0) {
    rc = flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0, "publish source is unknown");
    goto cleanup;
  }
  {
    const flow_stage_plan_impl_t *source =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, (size_t)source_index);
    if (!source->is_source) {
      rc = flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0, "publish target must be a source");
      goto cleanup;
    }
  }

  if (msg->owned_payload || msg->_content_handle) {
    rc = turbo_flow_msg_clone(&local, msg);
  } else {
    rc = turbo_flow_msg_retain_view(&local, msg);
  }
  if (rc != TURBO_OK) {
    const char *error_message = rc == TURBO_ENOTSUP ? "publish cannot clone the schema projection"
                                                    : "publish requires a cloneable payload view";
    rc = flow_set_error_keep_state(flow, rc, 0, 0, error_message);
    goto cleanup;
  }
  local_initialized = 1;

  rc = flow_publish_local(flow, source_name, (uint32_t)source_index, &local, observe_start, result);

cleanup:
  if (local_initialized) {
    turbo_flow_msg_cleanup(&local);
  }
  if (publish_entered) flow_publish_leave(flow);
  flow_publish_error_context_end(flow);
  result->status = rc;
  return rc;
}

int turbo_flow_publish(turbo_flow_t *flow, const char *source_name, const turbo_flow_msg_t *msg) {
  turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
  return turbo_flow_publish_ex(flow, source_name, msg, &result);
}
