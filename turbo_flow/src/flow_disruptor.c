#include "flow_internal.h"

#include <stdlib.h>
#include <string.h>

#define FLOW_BROADCAST_RING_CAPACITY 1024u

typedef struct flow_worker_request_s {
  flow_execution_task_t execution;
  flow_worker_pool_adapter_t *adapter;
  turbo_flow_error_t error;
} flow_worker_request_t;

typedef struct flow_worker_entry_s {
  flow_entry_header_t header;
  flow_worker_request_t *request;
} flow_worker_entry_t;

typedef struct flow_broadcast_entry_s {
  flow_entry_header_t header;
  turbo_flow_msg_t message;
} flow_broadcast_entry_t;

typedef struct flow_worker_context_s {
  flow_worker_pool_adapter_t *adapter;
  uint32_t lane;
} flow_worker_context_t;

static int flow_worker_request_run(turbo_flow_msg_t *msg, void *ctx) {
  flow_worker_request_t *request = (flow_worker_request_t *)ctx;
  flow_worker_pool_adapter_t *adapter = request->adapter;
  int rc;

  flow_publish_error_context_begin(adapter->flow);
  rc = flow_dispatch_call_executor(adapter->flow, adapter->stage, adapter->executor,
                                   adapter->stage_index, msg, &request->execution.completion);
  if (rc != TURBO_OK && turbo_flow_last_error(adapter->flow)) {
    request->error = *turbo_flow_last_error(adapter->flow);
  }
  flow_publish_error_context_end(adapter->flow);
  return rc;
}

static int flow_worker_pool_should_run(void *ctx) {
  flow_worker_pool_adapter_t *adapter = (flow_worker_pool_adapter_t *)ctx;
  return adapter && (atomic_load_explicit(&adapter->running, memory_order_acquire) ||
                     atomic_load_explicit(&adapter->pending, memory_order_acquire) > 0u ||
                     atomic_load_explicit(&adapter->submitters, memory_order_acquire) > 0u);
}

static void flow_worker_pool_pending_finished(flow_worker_pool_adapter_t *adapter) {
  unsigned int previous = atomic_fetch_sub_explicit(&adapter->pending, 1u, memory_order_acq_rel);
  if (previous == 1u && !atomic_load_explicit(&adapter->running, memory_order_acquire)) {
    disruptor_worker_wake_all(adapter->ring);
  }
}

static void flow_worker_pool_run(void *arg) {
  flow_worker_context_t *context = (flow_worker_context_t *)arg;
  flow_worker_pool_adapter_t *adapter = context ? context->adapter : NULL;
  if (!adapter || context->lane >= adapter->width) return;

  for (;;) {
    disruptor_cursor_t cursor;
    flow_worker_entry_t *entry;
    flow_worker_request_t *request;

    if (!disruptor_worker_claim_wait(adapter->ring, &cursor, flow_worker_pool_should_run, adapter))
      return;

    entry = (flow_worker_entry_t *)disruptor_acquire_entry(adapter->ring, &cursor);
    request = entry ? entry->request : NULL;
    if (!request) {
      disruptor_worker_release_entry(adapter->ring, &cursor);
      flow_worker_pool_pending_finished(adapter);
      continue;
    }

    entry->header.worker_lane = context->lane;
    request->execution.completion.entry.worker_lane = context->lane;
    flow_pool_record_started(flow_pool_record_at(adapter->flow, adapter->pool_record_index));
    if (flow_entry_header_validate(adapter->flow, &entry->header, &request->execution.msg) !=
            TURBO_OK ||
        entry->header.stage_index != adapter->stage_index ||
        entry->header.segment_kind != FLOW_DATA_SEGMENT_WORKER_POOL ||
        entry->header.completion_handle != &request->execution.completion ||
        entry->header.cancel_handle != &request->execution.cancel_requested) {
      flow_execution_task_fail(&request->execution, TURBO_EPROTO);
    } else {
      flow_execution_task_run(&request->execution);
    }
    flow_pool_record_finished(flow_pool_record_at(adapter->flow, adapter->pool_record_index),
                              request->execution.status);
    entry->header = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
    entry->request = NULL;
    disruptor_worker_release_entry(adapter->ring, &cursor);
    flow_worker_pool_pending_finished(adapter);
  }
}

static void flow_worker_pool_stop(flow_worker_pool_adapter_t *adapter) {
  flow_pool_record_t *record;
  if (!adapter) return;
  record = flow_pool_record_at(adapter->flow, adapter->pool_record_index);
  flow_pool_record_set_state(record, TURBO_FLOW_POOL_DRAINING);
  atomic_store_explicit(&adapter->accepting, 0, memory_order_release);
  atomic_store_explicit(&adapter->running, 0, memory_order_release);
  if (adapter->ring) disruptor_worker_wake_all(adapter->ring);
  if (adapter->workers) {
    for (uint32_t i = 0; i < adapter->width; ++i) {
      if (adapter->workers[i]) (void)turbo_thread_join(&adapter->workers[i]);
    }
    free(adapter->workers);
    adapter->workers = NULL;
  }
  free(adapter->worker_contexts);
  adapter->worker_contexts = NULL;
  if (adapter->ring) {
    disruptor_destroy(adapter->ring);
    adapter->ring = NULL;
  }
  flow_pool_record_set_state(record, TURBO_FLOW_POOL_STOPPED);
}

static int flow_worker_pool_start(flow_worker_pool_adapter_t *adapter) {
  flow_worker_context_t *contexts;
  if (!adapter || !adapter->ring || adapter->width == 0u) return TURBO_EINVAL;
  adapter->workers = (turbo_thread_t *)calloc(adapter->width, sizeof(*adapter->workers));
  contexts = (flow_worker_context_t *)calloc(adapter->width, sizeof(*contexts));
  if (!adapter->workers || !contexts) {
    free(adapter->workers);
    free(contexts);
    adapter->workers = NULL;
    return TURBO_ENOMEM;
  }
  adapter->worker_contexts = contexts;
  atomic_store_explicit(&adapter->running, 1, memory_order_release);
  atomic_store_explicit(&adapter->accepting, 1, memory_order_release);
  for (uint32_t i = 0; i < adapter->width; ++i) {
    contexts[i].adapter = adapter;
    contexts[i].lane = i;
    if (turbo_thread_create(&adapter->workers[i], flow_worker_pool_run, &contexts[i]) != TURBO_OK) {
      adapter->width = i;
      flow_worker_pool_stop(adapter);
      return TURBO_ENOMEM;
    }
  }
  flow_pool_record_set_state(flow_pool_record_at(adapter->flow, adapter->pool_record_index),
                             TURBO_FLOW_POOL_RUNNING);
  return TURBO_OK;
}

static int flow_stage_has_worker_pool(const turbo_flow_t *flow) {
  for (size_t i = 0; i < turbo_vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, i);
    if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) return 1;
  }
  return 0;
}

static size_t flow_source_count(const turbo_flow_t *flow) {
  size_t count = 0;

  for (size_t i = 0; i < turbo_vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, i);
    if (stage->is_source) ++count;
  }
  return count;
}

static size_t flow_broadcast_consumer_count(const turbo_flow_t *flow) {
  size_t count = 0;

  for (size_t i = 0; i < turbo_vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, i);
    if (!stage->is_source) ++count;
  }
  return count;
}

static flow_broadcast_consumer_t *flow_broadcast_consumer_for_stage(turbo_flow_t *flow,
                                                                    uint32_t stage_index) {
  for (size_t i = 0; i < turbo_vec_size(&flow->broadcast_consumers); ++i) {
    flow_broadcast_consumer_t *consumer =
        (flow_broadcast_consumer_t *)turbo_vec_at(&flow->broadcast_consumers, i);
    if (consumer->stage_index == stage_index) return consumer;
  }
  return NULL;
}

static int flow_has_dynamic_edges(const turbo_flow_t *flow) {
  for (size_t i = 0; i < turbo_vec_size(&flow->runtime_edges); ++i) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)turbo_vec_at_const(&flow->runtime_edges, i);
    if (edge && edge->kind != TURBO_FLOW_EDGE_UNCONDITIONAL) return 1;
  }
  return 0;
}

static int flow_has_reorder_stage(const turbo_flow_t *flow) {
  for (size_t i = 0; i < turbo_vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, i);
    if (stage && stage->reorder.capacity > 0u) return 1;
  }
  return 0;
}

static int flow_requires_executor_data_path(const turbo_flow_t *flow) {
  for (size_t i = 0; i < turbo_vec_size(&flow->executor_plans); ++i) {
    const flow_executor_plan_t *executor =
        (const flow_executor_plan_t *)turbo_vec_at_const(&flow->executor_plans, i);
    const flow_stage_plan_impl_t *stage;
    if (!executor || executor->exec.kind != TURBO_FLOW_EXEC_INLINE || executor->emit_fn ||
        executor->keyed_fn || executor->keyed_emit_fn || executor->window_fn) {
      return 1;
    }
    stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, executor->stage_index);
    if (stage && (stage->effects & TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u) return 1;
    if (stage && flow_adapter_for_stage(flow, stage)) return 1;
  }
  return 0;
}

static void flow_release_broadcast_remaining(turbo_flow_t *flow, const uint8_t *reachable,
                                             uint8_t *done) {
  for (size_t i = 0; i < turbo_vec_size(&flow->broadcast_consumers); ++i) {
    flow_broadcast_consumer_t *consumer =
        (flow_broadcast_consumer_t *)turbo_vec_at(&flow->broadcast_consumers, i);
    disruptor_cursor_t cursor;

    if (!reachable[consumer->stage_index] || done[consumer->stage_index]) continue;

    cursor.sequence = consumer->next_sequence;
    disruptor_consumer_release_entry(flow->broadcast_ring, &consumer->consumer, &cursor);
    consumer->next_sequence += 1u;
    done[consumer->stage_index] = 1;
  }
}

void flow_stop_data_planes(turbo_flow_t *flow) {
  if (!flow) return;

  if (flow->broadcast_topology) {
    disruptor_topology_destroy(flow->broadcast_topology);
    flow->broadcast_topology = NULL;
  }
  if (flow->broadcast_ring) {
    disruptor_destroy(flow->broadcast_ring);
    flow->broadcast_ring = NULL;
  }
  turbo_vec_clear(&flow->broadcast_consumers);

  for (size_t i = 0; i < turbo_vec_size(&flow->worker_pool_adapters); ++i) {
    flow_worker_pool_adapter_t *adapter =
        (flow_worker_pool_adapter_t *)turbo_vec_at(&flow->worker_pool_adapters, i);
    flow_worker_pool_stop(adapter);
  }
  turbo_vec_clear(&flow->worker_pool_adapters);
}

int flow_start_data_planes(turbo_flow_t *flow) {
  disruptor_config_t config;
  size_t consumer_count;

  if (!flow) return TURBO_EINVAL;

  flow_stop_data_planes(flow);

  for (size_t stage_index = 0; stage_index < turbo_vec_size(&flow->stages); ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, stage_index);
    flow_data_segment_plan_t *segment;
    flow_worker_pool_adapter_t adapter;

    if (stage->data_strategy != TURBO_FLOW_DATA_WORKER_POOL) continue;
    if (stage->data_worker_count == 0u) {
      return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                       "worker-pool width must be greater than zero");
    }
    segment = flow_worker_pool_segment_for_stage(flow, (uint32_t)stage_index);
    if (!segment || segment->capacity == 0u) {
      return flow_set_error_keep_state(flow, TURBO_EINVAL, stage->line, stage->column,
                                       "worker-pool capacity is not available");
    }

    memset(&config, 0, sizeof(config));
    config.entry_size = sizeof(flow_worker_entry_t);
    config.capacity = segment->capacity;
    config.consumer_capacity = 1u;
    config.mode = DISRUPTOR_MODE_WORKER_POOL;

    memset(&adapter, 0, sizeof(adapter));
    adapter.flow = flow;
    adapter.stage_index = (uint32_t)stage_index;
    adapter.width = stage->data_worker_count;
    adapter.capacity = segment->capacity;
    adapter.stage = (flow_stage_plan_impl_t *)stage;
    adapter.executor = flow_executor_plan_for_stage(flow, (uint32_t)stage_index);
    atomic_init(&adapter.accepting, 0);
    atomic_init(&adapter.running, 0);
    atomic_init(&adapter.pending, 0u);
    atomic_init(&adapter.submitters, 0u);
    adapter.ring = disruptor_create(&config);
    if (!adapter.ring || !adapter.executor ||
        flow_pool_record_add(flow, TURBO_FLOW_POOL_DISRUPTOR, adapter.stage_index, adapter.width,
                             adapter.capacity, adapter.capacity,
                             &adapter.pool_record_index) != TURBO_OK ||
        turbo_vec_push(&flow->worker_pool_adapters, &adapter) != TURBO_OK) {
      if (adapter.ring) disruptor_destroy(adapter.ring);
      flow_stop_data_planes(flow);
      return flow_set_error_keep_state(flow, TURBO_ENOMEM, stage->line, stage->column,
                                       "failed to create worker-pool data plane");
    }
  }

  for (size_t i = 0; i < turbo_vec_size(&flow->worker_pool_adapters); ++i) {
    flow_worker_pool_adapter_t *adapter =
        (flow_worker_pool_adapter_t *)turbo_vec_at(&flow->worker_pool_adapters, i);
    int start_rc = flow_worker_pool_start(adapter);
    if (start_rc != TURBO_OK) {
      const flow_stage_plan_impl_t *stage =
          (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, adapter->stage_index);
      flow_stop_data_planes(flow);
      return flow_set_error_keep_state(flow, start_rc, stage ? stage->line : 0,
                                       stage ? stage->column : 0,
                                       "failed to start worker-pool consumers");
    }
  }

  if (flow_stage_has_worker_pool(flow) || flow_source_count(flow) != 1u ||
      flow_has_dynamic_edges(flow) || flow_has_reorder_stage(flow) ||
      flow_requires_executor_data_path(flow)) {
    return TURBO_OK;
  }

  consumer_count = flow_broadcast_consumer_count(flow);
  if (consumer_count == 0) return TURBO_OK;
  if (consumer_count > UINT32_MAX) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "broadcast consumer count is too large");
  }

  memset(&config, 0, sizeof(config));
  config.entry_size = sizeof(flow_broadcast_entry_t);
  config.capacity = FLOW_BROADCAST_RING_CAPACITY;
  config.consumer_capacity = (uint32_t)consumer_count;
  config.mode = DISRUPTOR_MODE_BROADCAST;

  flow->broadcast_ring = disruptor_create(&config);
  if (!flow->broadcast_ring) {
    return flow_set_error_keep_state(flow, TURBO_ENOMEM, 0, 0,
                                     "failed to create broadcast data plane");
  }

  flow->broadcast_topology = disruptor_topology_create(flow->broadcast_ring);
  if (!flow->broadcast_topology) {
    flow_stop_data_planes(flow);
    return flow_set_error_keep_state(flow, TURBO_ENOMEM, 0, 0,
                                     "failed to create broadcast topology");
  }

  for (size_t stage_index = 0; stage_index < turbo_vec_size(&flow->stages); ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, stage_index);
    flow_broadcast_consumer_t consumer;

    if (stage->is_source) continue;

    memset(&consumer, 0, sizeof(consumer));
    consumer.stage_index = (uint32_t)stage_index;
    consumer.next_sequence = disruptor_consumer_register(flow->broadcast_ring, &consumer.consumer);
    consumer.topology_stage =
        disruptor_topology_stage(flow->broadcast_topology, stage->name, &consumer.consumer);
    if (consumer.topology_stage == DISRUPTOR_STAGE_INVALID ||
        turbo_vec_push(&flow->broadcast_consumers, &consumer) != TURBO_OK) {
      flow_stop_data_planes(flow);
      return flow_set_error_keep_state(flow, TURBO_ENOMEM, stage->line, stage->column,
                                       "failed to register broadcast stage");
    }
  }

  for (size_t edge_index = 0; edge_index < turbo_vec_size(&flow->runtime_edges); ++edge_index) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)turbo_vec_at_const(&flow->runtime_edges, edge_index);
    flow_broadcast_consumer_t *to = flow_broadcast_consumer_for_stage(flow, edge->to_stage);
    flow_broadcast_consumer_t *from = flow_broadcast_consumer_for_stage(flow, edge->from_stage);

    if (!to || !from) continue;
    if (!disruptor_topology_after(flow->broadcast_topology, to->topology_stage,
                                  from->topology_stage)) {
      flow_stop_data_planes(flow);
      return flow_set_error_keep_state(flow, TURBO_EINVAL, edge->line, edge->column,
                                       "failed to link broadcast topology");
    }
  }

  if (!disruptor_topology_commit(flow->broadcast_topology)) {
    flow_stop_data_planes(flow);
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0, "broadcast topology is invalid");
  }

  return TURBO_OK;
}

flow_worker_pool_adapter_t *flow_worker_pool_adapter_for_stage(turbo_flow_t *flow,
                                                               uint32_t stage_index) {
  if (!flow) return NULL;
  for (size_t i = 0; i < turbo_vec_size(&flow->worker_pool_adapters); ++i) {
    flow_worker_pool_adapter_t *adapter =
        (flow_worker_pool_adapter_t *)turbo_vec_at(&flow->worker_pool_adapters, i);
    if (adapter->stage_index == stage_index) return adapter;
  }
  return NULL;
}

static int flow_worker_pool_claim(flow_worker_pool_adapter_t *adapter,
                                  turbo_flow_backpressure_kind_t policy,
                                  disruptor_cursor_t *cursor) {
  if (policy == TURBO_FLOW_BACKPRESSURE_BLOCK) {
    disruptor_publisher_next_entry_blocking(adapter->ring, cursor);
    return cursor->sequence != 0u ? TURBO_OK : TURBO_EINVAL;
  }
  if (policy == TURBO_FLOW_BACKPRESSURE_FAIL || policy == TURBO_FLOW_BACKPRESSURE_DROP_NEWEST) {
    if (disruptor_publisher_try_claim(adapter->ring, cursor)) return TURBO_OK;
    return policy == TURBO_FLOW_BACKPRESSURE_FAIL ? TURBO_ENOSPC : TURBO_ECANCELED;
  }
  return TURBO_ENOTSUP;
}

int flow_worker_pool_submit(flow_worker_pool_adapter_t *adapter, turbo_flow_msg_t *msg,
                            flow_stage_completion_t *completion) {
  flow_worker_request_t request;
  flow_worker_entry_t *entry;
  disruptor_cursor_t cursor;
  int rc;
  int submitter_registered = 0;
  const turbo_flow_operation_runtime_contract_t *runtime;
  flow_pool_record_t *record;

  if (!adapter || !adapter->ring || !msg || !completion) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->accepting, memory_order_acquire)) return TURBO_ESHUTDOWN;
  atomic_fetch_add_explicit(&adapter->submitters, 1u, memory_order_acq_rel);
  submitter_registered = 1;
  if (!atomic_load_explicit(&adapter->accepting, memory_order_acquire)) {
    atomic_fetch_sub_explicit(&adapter->submitters, 1u, memory_order_acq_rel);
    submitter_registered = 0;
    return TURBO_ESHUTDOWN;
  }

  memset(&request, 0, sizeof(request));
  request.adapter = adapter;
  runtime = flow_stage_operation_runtime(adapter->flow, adapter->stage);
  record = flow_pool_record_at(adapter->flow, adapter->pool_record_index);
  rc = flow_execution_task_init(
      &request.execution, FLOW_EXECUTION_DISRUPTOR, flow_worker_request_run, &request, msg,
      completion,
      runtime && adapter->executor->exec.kind != TURBO_FLOW_EXEC_THREAD_POOL &&
              adapter->executor->exec.kind != TURBO_FLOW_EXEC_CORO_POOL
          ? runtime->deadline_ms
          : 0u);
  if (rc != TURBO_OK) goto cleanup;
  flow_pool_record_attempted(record);

  rc = flow_worker_pool_claim(
      adapter, runtime ? runtime->backpressure : TURBO_FLOW_BACKPRESSURE_BLOCK, &cursor);
  if (rc != TURBO_OK) {
    if (rc == TURBO_ECANCELED) {
      flow_pool_record_canceled_unqueued(record);
    } else {
      flow_pool_record_rejected_unqueued(record);
    }
    completion->status = rc;
    goto restore;
  }
  entry = (flow_worker_entry_t *)disruptor_acquire_entry(adapter->ring, &cursor);
  flow_pool_record_queued(record);
  entry->header = request.execution.completion.entry;
  entry->request = &request;
  atomic_fetch_add_explicit(&adapter->pending, 1u, memory_order_acq_rel);
  disruptor_publisher_commit_entry_blocking(adapter->ring, &cursor);
  atomic_fetch_sub_explicit(&adapter->submitters, 1u, memory_order_acq_rel);
  submitter_registered = 0;

  rc = flow_execution_task_wait(&request.execution, msg, completion);
  if (rc != TURBO_OK && request.error.code != TURBO_OK) {
    rc = flow_set_error_keep_state(adapter->flow, request.error.code, request.error.line,
                                   request.error.column, request.error.message);
  }

restore:
  if (submitter_registered) {
    atomic_fetch_sub_explicit(&adapter->submitters, 1u, memory_order_acq_rel);
    submitter_registered = 0;
  }
  if (flow_execution_task_state(&request.execution) == FLOW_EXECUTION_ACCEPTED) {
    int move_rc = turbo_flow_msg_move(msg, &request.execution.msg);
    if (move_rc != TURBO_OK) rc = move_rc;
  }
cleanup:
  if (submitter_registered) {
    atomic_fetch_sub_explicit(&adapter->submitters, 1u, memory_order_acq_rel);
  }
  flow_execution_task_cleanup(&request.execution);
  if (rc == TURBO_ENOSPC) {
    return flow_set_error_keep_state(adapter->flow, rc, adapter->stage->line,
                                     adapter->stage->column,
                                     "worker-pool admission capacity is exhausted");
  }
  if (rc == TURBO_ECANCELED) {
    return flow_set_error_keep_state(adapter->flow, rc, adapter->stage->line,
                                     adapter->stage->column,
                                     "worker-pool newest request was dropped");
  }
  return rc;
}

int flow_publish_broadcast_data_plane(turbo_flow_t *flow, uint32_t source_index,
                                      turbo_flow_msg_t *msg, uint64_t sequence,
                                      turbo_flow_publish_result_t *result) {
  uint8_t *reachable = NULL;
  uint8_t *done = NULL;
  size_t stage_count;
  size_t pending = 0;
  disruptor_cursor_t publish_cursor;
  flow_broadcast_entry_t *entry = NULL;
  flow_entry_header_t header = FLOW_ENTRY_HEADER_INIT;
  int rc = TURBO_OK;

  if (!flow || !flow->broadcast_ring || !msg || !result) return TURBO_ENOTSUP;
  if (flow_msg_transport_context_is_borrowed(msg)) {
    return flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "broadcast data plane rejects borrowed transport context");
  }
  rc = flow_entry_header_init(flow, &header, source_index, FLOW_DATA_SEGMENT_BROADCAST_FANOUT, 0u,
                              sequence, msg->id, FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE, NULL);
  if (rc != TURBO_OK) return rc;

  stage_count = turbo_vec_size(&flow->stages);
  reachable = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  done = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  if (!reachable || !done) {
    rc = flow_set_error_keep_state(flow, TURBO_ENOMEM, 0, 0, "out of memory");
    goto cleanup;
  }

  flow_mark_reachable_from_stage(flow, reachable, source_index);
  for (size_t i = 0; i < turbo_vec_size(&flow->broadcast_consumers); ++i) {
    flow_broadcast_consumer_t *consumer =
        (flow_broadcast_consumer_t *)turbo_vec_at(&flow->broadcast_consumers, i);
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages, consumer->stage_index);

    if (!reachable[consumer->stage_index]) continue;
    pending += 1u;
    if (stage->is_port) continue;
    rc = flow_dispatch_validate_stage(flow, consumer->stage_index);
    if (rc != TURBO_OK) goto cleanup;
  }

  if (!disruptor_publisher_try_claim(flow->broadcast_ring, &publish_cursor)) {
    rc = flow_set_error_keep_state(flow, TURBO_ENOSPC, 0, 0, "broadcast data plane is full");
    goto cleanup;
  }

  entry = (flow_broadcast_entry_t *)disruptor_acquire_entry(flow->broadcast_ring, &publish_cursor);
  entry->header = header;
  (void)turbo_flow_msg_move(&entry->message, msg);

  disruptor_publisher_commit_entry_blocking(flow->broadcast_ring, &publish_cursor);

  while (pending > 0) {
    int progressed = 0;

    for (size_t i = 0; i < turbo_vec_size(&flow->broadcast_consumers); ++i) {
      flow_broadcast_consumer_t *consumer =
          (flow_broadcast_consumer_t *)turbo_vec_at(&flow->broadcast_consumers, i);
      flow_stage_plan_impl_t *stage =
          (flow_stage_plan_impl_t *)turbo_vec_at(&flow->stages, consumer->stage_index);
      disruptor_cursor_t cursor;

      if (!reachable[consumer->stage_index] || done[consumer->stage_index]) continue;

      cursor.sequence = consumer->next_sequence;
      if (!disruptor_consumer_wait_for_nonblocking_for(flow->broadcast_ring, &consumer->consumer,
                                                       &cursor)) {
        continue;
      }

      cursor.sequence = consumer->next_sequence;
      rc = flow_entry_header_validate(flow, &entry->header, &entry->message);
      if (rc != TURBO_OK || entry->header.stage_index != source_index ||
          entry->header.segment_kind != FLOW_DATA_SEGMENT_BROADCAST_FANOUT ||
          entry->header.sequence != sequence) {
        flow_release_broadcast_remaining(flow, reachable, done);
        turbo_flow_msg_cleanup(&entry->message);
        entry->header = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
        rc = flow_set_error_keep_state(flow, TURBO_EPROTO, 0, 0,
                                       "broadcast data plane entry header is invalid");
        goto cleanup;
      }
      if (!stage->is_port) {
        flow_stage_completion_t completion = {0};
        rc = flow_dispatch_stage(flow, consumer->stage_index, &entry->message, sequence,
                                 entry->message.id, &completion, NULL);
        if (rc != TURBO_OK) {
          flow_release_broadcast_remaining(flow, reachable, done);
          turbo_flow_msg_cleanup(&entry->message);
          entry->header = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
          goto cleanup;
        }
      }

      disruptor_consumer_release_entry(flow->broadcast_ring, &consumer->consumer, &cursor);
      consumer->next_sequence += 1u;
      done[consumer->stage_index] = 1;
      pending -= 1u;
      progressed = 1;
    }

    if (!progressed) {
      flow_release_broadcast_remaining(flow, reachable, done);
      turbo_flow_msg_cleanup(&entry->message);
      entry->header = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
      rc = flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                     "broadcast topology made no progress");
      goto cleanup;
    }
  }

  {
    const turbo_flow_protocol_settlement_envelope_t *settlement =
        turbo_flow_msg_protocol_settlement(&entry->message);
    if (settlement) result->protocol_settlement = settlement->settled_point;
  }
  turbo_flow_msg_cleanup(&entry->message);
  entry->header = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;

cleanup:
  free(reachable);
  free(done);
  return rc;
}
