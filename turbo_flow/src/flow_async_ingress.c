#include "flow_internal.h"

#include <stdlib.h>

typedef struct flow_async_publish_task_s {
  turbo_flow_t *flow;
  const char *source_name;
  uint32_t source_index;
  turbo_flow_msg_t message;
  turbo_flow_publish_completion_fn completion;
  void *completion_ctx;
} flow_async_publish_task_t;

static int flow_async_ingress_config_valid(const turbo_flow_async_ingress_config_t *config) {
  return config && config->size >= sizeof(*config) && config->workers > 0u &&
         config->workers <= TURBO_FLOW_ASYNC_INGRESS_MAX_WORKERS && config->queue_capacity > 0u &&
         config->queue_capacity <= TURBO_FLOW_ASYNC_INGRESS_MAX_CAPACITY;
}

int turbo_flow_configure_async_ingress(turbo_flow_t *flow,
                                       const turbo_flow_async_ingress_config_t *config) {
  if (!flow || !flow_async_ingress_config_valid(config)) return TURBO_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) return TURBO_EBUSY;

  turbo_mutex_lock(&flow->async_ingress_mutex);
  if (flow->async_ingress_pool) {
    turbo_mutex_unlock(&flow->async_ingress_mutex);
    return TURBO_EBUSY;
  }
  flow->async_ingress_config = *config;
  flow->async_ingress_config.size = sizeof(flow->async_ingress_config);
  turbo_mutex_unlock(&flow->async_ingress_mutex);
  return TURBO_OK;
}

static turbo_threadpool_t *flow_async_ingress_get_or_create(turbo_flow_t *flow) {
  turbo_flow_async_ingress_config_t ingress_config;
  turbo_threadpool_config_t pool_config;
  turbo_threadpool_t *candidate;
  turbo_threadpool_t *pool;

  turbo_mutex_lock(&flow->async_ingress_mutex);
  pool = flow->async_ingress_pool;
  ingress_config = flow->async_ingress_config;
  turbo_mutex_unlock(&flow->async_ingress_mutex);
  if (pool) return pool;

  pool_config.num_threads = (int)ingress_config.workers;
  pool_config.queue_capacity = ingress_config.queue_capacity;
  candidate = turbo_threadpool_create_with_config(&pool_config);
  if (!candidate) return NULL;

  turbo_mutex_lock(&flow->async_ingress_mutex);
  if (!flow->async_ingress_pool) {
    flow->async_ingress_pool = candidate;
    candidate = NULL;
  }
  pool = flow->async_ingress_pool;
  turbo_mutex_unlock(&flow->async_ingress_mutex);
  turbo_threadpool_destroy(candidate);
  return pool;
}

void flow_stop_async_ingress(turbo_flow_t *flow) {
  turbo_threadpool_t *pool;
  if (!flow || !flow->runtime_sync_initialized) return;

  turbo_mutex_lock(&flow->async_ingress_mutex);
  pool = flow->async_ingress_pool;
  flow->async_ingress_pool = NULL;
  turbo_mutex_unlock(&flow->async_ingress_mutex);
  turbo_threadpool_destroy(pool);
}

static void flow_async_publish_task_run(void *arg) {
  flow_async_publish_task_t *task = (flow_async_publish_task_t *)arg;
  turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
  uint64_t observe_start = 0u;

  flow_publish_error_context_begin(task->flow);
  flow_clear_error(task->flow);
  if (flow_observer_has_handlers(task->flow)) observe_start = turbo_hrtime();
  (void)flow_publish_local(task->flow, task->source_name, task->source_index, &task->message,
                           observe_start, &result);
  turbo_flow_msg_cleanup(&task->message);
  flow_publish_error_context_end(task->flow);
  if (task->completion) task->completion(task->completion_ctx, &result);
  flow_publish_leave(task->flow);
  free(task);
}

int turbo_flow_publish_async(turbo_flow_t *flow, const char *source_name,
                             const turbo_flow_msg_t *msg,
                             turbo_flow_publish_completion_fn completion, void *ctx) {
  flow_async_publish_task_t *task = NULL;
  const flow_stage_plan_impl_t *source;
  turbo_threadpool_t *pool;
  int source_index;
  int entered = 0;
  int rc;

  if (!flow || !source_name || !msg) return TURBO_EINVAL;
  flow_publish_error_context_begin(flow);
  rc = flow_publish_enter(flow);
  if (rc != TURBO_OK) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                   rc == TURBO_ESHUTDOWN
                                       ? "flow is not accepting async publications"
                                       : "flow must be started before async publish");
    goto cleanup;
  }
  entered = 1;
  flow_clear_error(flow);

  if (!msg->buffer && !msg->owned_payload && msg->payload.data) {
    rc = flow_set_error_keep_state(
        flow, TURBO_EINVAL, 0, 0,
        "async publish payload requires a backing buffer or owned payload");
    goto cleanup;
  }
  source_index = turbo_flow_find_stage(flow, source_name);
  if (source_index < 0) {
    rc = flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0, "async publish source is unknown");
    goto cleanup;
  }
  source = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, (size_t)source_index);
  if (!source || !source->is_source) {
    rc = flow_set_error_keep_state(flow, TURBO_EINVAL, 0, 0,
                                   "async publish target must be a source");
    goto cleanup;
  }

  task = (flow_async_publish_task_t *)calloc(1, sizeof(*task));
  if (!task) {
    rc =
        flow_set_error_keep_state(flow, TURBO_ENOMEM, 0, 0, "async publish task allocation failed");
    goto cleanup;
  }
  turbo_flow_msg_init(&task->message);
  rc = msg->owned_payload || msg->_content_handle ? turbo_flow_msg_clone(&task->message, msg)
                                                  : turbo_flow_msg_retain_view(&task->message, msg);
  if (rc != TURBO_OK) {
    rc = flow_set_error_keep_state(flow, rc, 0, 0,
                                   rc == TURBO_ENOTSUP
                                       ? "async publish cannot clone the schema projection"
                                       : "async publish requires a cloneable payload view");
    goto cleanup;
  }
  task->flow = flow;
  task->source_name = source->name;
  task->source_index = (uint32_t)source_index;
  task->completion = completion;
  task->completion_ctx = ctx;

  pool = flow_async_ingress_get_or_create(flow);
  if (!pool) {
    rc = flow_set_error_keep_state(flow, TURBO_ENOMEM, 0, 0,
                                   "async ingress worker pool creation failed");
    goto cleanup;
  }
  if (turbo_threadpool_try_submit(pool, flow_async_publish_task_run, task) != 0) {
    rc = flow_set_error_keep_state(flow, TURBO_ENOSPC, 0, 0, "async ingress capacity is exhausted");
    goto cleanup;
  }

  task = NULL;
  entered = 0;
  rc = TURBO_OK;

cleanup:
  if (task) {
    turbo_flow_msg_cleanup(&task->message);
    free(task);
  }
  if (entered) flow_publish_leave(flow);
  flow_publish_error_context_end(flow);
  return rc;
}
