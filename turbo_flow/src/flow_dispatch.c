#include "flow_internal.h"

#define FLOW_NANOSECONDS_PER_MILLISECOND UINT64_C(1000000)

int flow_entry_header_init(const turbo_flow_t *flow, flow_entry_header_t *header,
                           uint32_t stage_index, flow_data_segment_kind_t segment_kind,
                           uint64_t ordering_key, uint64_t sequence, uint64_t message_id,
                           flow_entry_ownership_t ownership,
                           flow_stage_completion_t *completion_handle) {
  if (!flow || !header || stage_index >= vec_size(&flow->stages) ||
      segment_kind < FLOW_DATA_SEGMENT_DIRECT || segment_kind > FLOW_DATA_SEGMENT_FANIN_GATE ||
      ownership < FLOW_ENTRY_OWNERSHIP_BORROWED || ownership > FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE) {
    return TURBO_EINVAL;
  }
  *header = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
  header->runtime_generation = flow->runtime_generation;
  header->stage_index = stage_index;
  header->segment_kind = segment_kind;
  header->ownership = ownership;
  header->ordering_key = ordering_key;
  header->sequence = sequence;
  header->message_id = message_id;
  header->completion_handle = completion_handle;
  return TURBO_OK;
}

int flow_entry_header_validate(const turbo_flow_t *flow, const flow_entry_header_t *header,
                               const turbo_flow_msg_t *message) {
  if (!flow || !header || header->size < sizeof(*header) || header->runtime_generation == 0u ||
      header->runtime_generation != flow->runtime_generation ||
      header->stage_index >= vec_size(&flow->stages) ||
      header->segment_kind < FLOW_DATA_SEGMENT_DIRECT ||
      header->segment_kind > FLOW_DATA_SEGMENT_FANIN_GATE ||
      header->ownership != FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE || !message ||
      header->message_id != message->id || flow_msg_transport_context_is_borrowed(message)) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_dispatch_prepare_completion(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                            uint32_t stage_index, uint64_t sequence,
                                            uint64_t msg_id, flow_stage_completion_t *completion) {
  if (!completion) return TURBO_EINVAL;

  *completion = (flow_stage_completion_t){0};
  completion->status = TURBO_OK;
  completion->terminal = 0;
  completion->settlement_reported = 0;
  completion->settlement_duplicate = 0;
  completion->settlement = (turbo_flow_settlement_result_t)TURBO_FLOW_SETTLEMENT_RESULT_INIT;

  if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
    flow_data_segment_plan_t *segment = flow_worker_pool_segment_for_stage(flow, stage_index);
    if (!segment || segment->width == 0u) {
      completion->status = TURBO_EINVAL;
      return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                       "worker-pool data segment is not available");
    }
    return flow_entry_header_init(flow, &completion->entry, stage_index,
                                  FLOW_DATA_SEGMENT_WORKER_POOL, 0u, sequence, msg_id,
                                  FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE, completion);
  } else {
    return flow_entry_header_init(flow, &completion->entry, stage_index, FLOW_DATA_SEGMENT_DIRECT,
                                  0u, sequence, msg_id, FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE,
                                  completion);
  }
}

static int flow_dispatch_finish_status(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                       turbo_flow_msg_t *msg, flow_stage_completion_t *completion,
                                       int status) {
  if (status != TURBO_OK) {
    msg->status = status;
    completion->status = status;
    return flow_set_error_keep_state(flow, status, stage->line, stage->column,
                                     "stage callback failed");
  }

  if (msg->data_decision.stage_index == UINT32_MAX &&
      (msg->data_decision.evaluation_status != TURBO_FLOW_DATA_NOT_EVALUATED ||
       msg->data_decision.dropped || msg->data_decision.dead_letter ||
       msg->data_decision.route[0] != '\0' || msg->data_decision.batch_key[0] != '\0' ||
       msg->data_decision.retry_class[0] != '\0')) {
    msg->data_decision.stage_index = completion->entry.stage_index;
  }

  return TURBO_OK;
}

static int flow_dispatch_inline_stage(const flow_executor_plan_t *executor, turbo_flow_msg_t *msg) {
  return executor->fn(msg, executor->ctx);
}

static int flow_dispatch_inline_emitting_stage(const flow_executor_plan_t *executor,
                                               const turbo_flow_msg_t *msg,
                                               turbo_flow_emitter_t *emitter) {
  int status;

  if (!executor || !executor->emit_fn || !emitter) return TURBO_EINVAL;
  status = executor->emit_fn(msg, emitter, executor->ctx);
  if (status == TURBO_OK && emitter->status != TURBO_OK) status = emitter->status;
  return status;
}

static int flow_dispatch_sync_stage(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                    const flow_executor_plan_t *executor, turbo_flow_msg_t *msg,
                                    const flow_adapter_registration_t *adapter,
                                    flow_stage_completion_t *completion) {
  const turbo_flow_operation_runtime_contract_t *runtime =
      flow_stage_operation_runtime(flow, stage);
  uint64_t started_at = 0u;
  int status;
  flow_stage_completion_t *previous_settlement;

  if (runtime && runtime->deadline_ms != 0u) started_at = turbo_hrtime();
  previous_settlement = flow_settlement_scope_enter(completion);
  if (adapter && adapter->ops.consume && !executor->fn) {
    status = flow_adapter_consume_stage(flow, stage, msg);
  } else {
    status = flow_dispatch_inline_stage(executor, msg);
  }
  flow_settlement_scope_leave(previous_settlement);
  if (started_at != 0u &&
      turbo_hrtime() - started_at >= runtime->deadline_ms * FLOW_NANOSECONDS_PER_MILLISECOND) {
    return TURBO_ETIMEDOUT;
  }
  return status;
}

int flow_dispatch_call_executor(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                const flow_executor_plan_t *executor, uint32_t stage_index,
                                turbo_flow_msg_t *msg, flow_stage_completion_t *completion) {
  const flow_adapter_registration_t *adapter = flow_adapter_for_stage(flow, stage);

  if (executor->exec.kind == TURBO_FLOW_EXEC_THREAD_POOL) {
    return flow_execute_threadpool_stage(flow, stage, executor, stage_index, msg, completion);
  }
  if (executor->exec.kind == TURBO_FLOW_EXEC_CORO_POOL) {
    return flow_execute_coro_stage(flow, stage, executor, stage_index, msg, completion);
  }
  (void)stage_index;
  (void)completion;
  return flow_dispatch_sync_stage(flow, stage, executor, msg, adapter, completion);
}

static int flow_dispatch_worker_pool_stage(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                           const flow_executor_plan_t *executor,
                                           uint32_t stage_index, turbo_flow_msg_t *msg,
                                           flow_stage_completion_t *completion) {
  flow_worker_pool_adapter_t *adapter = flow_worker_pool_adapter_for_stage(flow, stage_index);

  if (!adapter || !adapter->ring) {
    completion->status = TURBO_EINVAL;
    return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                     "worker-pool data plane is not started");
  }

  return flow_worker_pool_submit(adapter, msg, completion);
}

int flow_dispatch_validate_stage(turbo_flow_t *flow, uint32_t stage_index) {
  const flow_stage_plan_impl_t *stage;
  const flow_executor_plan_t *executor;

  if (!flow || stage_index >= vec_size(&flow->stages)) return TURBO_EINVAL;

  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
  executor = flow_executor_plan_for_stage(flow, stage_index);
  if (!stage || !executor) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "executor callback is not available");
  }
  if (!executor->fn && !executor->emit_fn && !executor->keyed_fn && !executor->keyed_emit_fn &&
      !executor->window_fn) {
    const flow_adapter_registration_t *adapter = flow_adapter_for_stage(flow, stage);
    if (adapter && adapter->ops.consume) return TURBO_OK;
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "executor callback is not available");
  }

  switch (executor->exec.kind) {
  case TURBO_FLOW_EXEC_INLINE:
    return TURBO_OK;
  case TURBO_FLOW_EXEC_THREAD_POOL:
    if (!flow_threadpool_adapter_for_stage(flow, stage_index)) {
      return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                       "thread executor is not started");
    }
    return TURBO_OK;
  case TURBO_FLOW_EXEC_CORO_POOL:
    if (!flow_coro_adapter_for_stage(flow, stage_index)) {
      return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                       "coro executor is not started");
    }
    return TURBO_OK;
  default:
    return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                     "unknown executor kind");
  }
}

int flow_dispatch_stage(turbo_flow_t *flow, uint32_t stage_index, turbo_flow_msg_t *msg,
                        uint64_t sequence, uint64_t msg_id, flow_stage_completion_t *completion,
                        turbo_flow_emitter_t *emitter) {
  turbo_flow_observe_event_t event;
  flow_stage_plan_impl_t *stage;
  const flow_executor_plan_t *executor;
  int rc;
  int status;
  int result;
  uint64_t observe_start = 0;

  if (!flow || !msg || !completion || stage_index >= vec_size(&flow->stages)) {
    return TURBO_EINVAL;
  }

  stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, (size_t)stage_index);
  if (flow->observer_ops.stage_complete ||
      flow_observer_event_enabled(flow, TURBO_FLOW_OBSERVE_STAGE_END)) {
    observe_start = turbo_hrtime();
  }
  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_STAGE_BEGIN;
  event.stage_name = stage->name;
  event.adapter_name = stage->adapter_name;
  event.operation_name = stage->operation_name;
  event.msg = msg;
  event.status = TURBO_OK;
  event.selected = -1;
  event.edge_kind = -1;
  event.attempt = msg->execution_attempt;
  flow_observer_emit(flow, &event);
  executor = flow_executor_plan_for_stage(flow, stage_index);
  rc = flow_dispatch_validate_stage(flow, stage_index);
  if (rc != TURBO_OK) {
    completion->entry = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
    completion->entry.stage_index = stage_index;
    completion->status = rc;
    result = rc;
    goto observe;
  }

  rc = flow_dispatch_prepare_completion(flow, stage, stage_index, sequence, msg_id, completion);
  if (rc != TURBO_OK) {
    result = rc;
    goto observe;
  }

  rc = flow_reorder_enter(flow, stage_index, sequence);
  if (rc != TURBO_OK) {
    completion->status = rc;
    result = flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                       rc == TURBO_ENOSPC      ? "reorder capacity exceeded"
                                       : rc == TURBO_ETIMEDOUT ? "reorder sequence wait timed out"
                                                               : "reorder stopped");
    goto observe;
  }

  if (executor->emit_fn || executor->keyed_emit_fn || executor->window_fn) {
    if (!emitter || flow_msg_transport_context_is_borrowed(msg)) {
      status = TURBO_ENOTSUP;
    } else {
      status = flow_emitter_init(emitter, executor->max_outputs);
      if (status == TURBO_OK) {
        if (executor->window_fn) {
          status = flow_event_time_window_execute(executor->keyed_store, executor->key_selector,
                                                  executor->key_ctx, executor->window_fn,
                                                  executor->ctx, msg);
        } else if (executor->keyed_emit_fn) {
          status = flow_keyed_state_execute_emitting(executor->keyed_store, executor->key_selector,
                                                     executor->key_ctx, executor->keyed_emit_fn,
                                                     executor->ctx, msg, emitter);
        } else {
          status = flow_dispatch_inline_emitting_stage(executor, msg, emitter);
        }
        flow_emitter_close(emitter);
      }
    }
  } else if (executor->keyed_fn) {
    msg->execution_attempt = 1u;
    status = flow_keyed_state_execute(executor->keyed_store, executor->key_selector,
                                      executor->key_ctx, executor->keyed_fn, executor->ctx, msg);
  } else if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
    msg->execution_attempt = 1u;
    status = flow_dispatch_worker_pool_stage(flow, stage, executor, stage_index, msg, completion);
  } else {
    msg->execution_attempt = 1u;
    status = flow_dispatch_call_executor(flow, stage, executor, stage_index, msg, completion);
  }
  if (!executor->emit_fn && !executor->keyed_fn && !executor->keyed_emit_fn &&
      !executor->window_fn) {
    status = flow_adapter_apply_settlement(flow, stage, msg, completion, status);
  }
  if (status != TURBO_OK && flow_error_code(flow) == status) {
    msg->status = status;
    completion->status = status;
    result = status;
    flow_reorder_leave(flow, stage_index, sequence);
    goto observe;
  }

  result = flow_dispatch_finish_status(flow, stage, msg, completion, status);
  flow_reorder_leave(flow, stage_index, sequence);

observe:
  if (flow->observer_ops.stage_complete) {
    flow->observer_ops.stage_complete(flow->observer_ctx, stage->name, stage->adapter_name, msg,
                                      turbo_hrtime() - observe_start, result);
  }
  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_STAGE_END;
  event.stage_name = stage->name;
  event.adapter_name = stage->adapter_name;
  event.operation_name = stage->operation_name;
  event.msg = msg;
  event.status = result;
  event.selected = -1;
  event.edge_kind = -1;
  event.attempt = msg->execution_attempt;
  event.duration_ns = observe_start != 0u ? turbo_hrtime() - observe_start : 0u;
  flow_observer_emit(flow, &event);
  return result;
}
