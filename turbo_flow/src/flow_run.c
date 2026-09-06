#include "flow_internal.h"

#include <cflow/lower.h>

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_run_s {
  atomic_uint ref_count;
  salts_mutex_t mutex;
  salts_cond_t cond;
  turbo_flow_t *flow;
  uint32_t source_index;
  const char *source_name;
  cflow_scheduler *scheduler;
  cflow_graph graph;
  cflow_subscription subscription;
  cflow_subscriber_callbacks callbacks;
  cflow_subscriber subscriber;
  turbo_flow_run_state_t state;
  int status;
  size_t values;
  int terminal;
  atomic_int registered;
  int drain_on_stop;
  size_t pending_values;
  int upstream_done;
  int pending_status;
  turbo_flow_error_t error;
  int setup_complete;
  int deadline_fired;
  int deadline_ref_pending;
  cflow_task_id deadline_task_id;
};

static void flow_run_retain(turbo_flow_run_t *run) {
  if (run) (void)atomic_fetch_add_explicit(&run->ref_count, 1u, memory_order_relaxed);
}

static void flow_run_release(turbo_flow_run_t *run) {
  if (!run || atomic_fetch_sub_explicit(&run->ref_count, 1u, memory_order_acq_rel) != 1u) return;
  cflow_subscription_close(&run->subscription);
  cflow_graph_destroy(&run->graph);
  salts_cond_destroy(&run->cond);
  salts_mutex_destroy(&run->mutex);
  free(run);
}

static int flow_run_status_from_cflow(cflow_status status) {
  switch (status) {
    case CFLOW_STATUS_OK:
      return SALTS_OK;
    case CFLOW_STATUS_INVALID_ARGUMENT:
      return SALTS_EINVAL;
    case CFLOW_STATUS_TYPE_MISMATCH:
      return SALTS_EPROTO;
    case CFLOW_STATUS_UNSUPPORTED:
      return SALTS_ENOTSUP;
    case CFLOW_STATUS_CAPACITY_EXCEEDED:
      return SALTS_ENOSPC;
    case CFLOW_STATUS_ALLOCATION_FAILED:
      return SALTS_ENOMEM;
    case CFLOW_STATUS_CANCELLED:
      return SALTS_ECANCELED;
    case CFLOW_STATUS_CLOSED:
      return SALTS_ESHUTDOWN;
    case CFLOW_STATUS_WOULD_BLOCK:
      return SALTS_EBUSY;
    case CFLOW_STATUS_EXECUTION_ERROR:
    default:
      return SALTS_EIO;
  }
}

static int flow_run_status_from_admission(cflow_admission_status status) {
  switch (status) {
    case CFLOW_ADMISSION_ACCEPTED:
      return SALTS_OK;
    case CFLOW_ADMISSION_INVALID_ARGUMENT:
      return SALTS_EINVAL;
    case CFLOW_ADMISSION_FULL:
      return SALTS_ENOSPC;
    case CFLOW_ADMISSION_CLOSED:
      return SALTS_ESHUTDOWN;
    case CFLOW_ADMISSION_ALLOCATION_FAILED:
    default:
      return SALTS_ENOMEM;
  }
}

static void flow_run_copy_error(turbo_flow_run_t *run, int status, const char *message) {
  const turbo_flow_error_t *flow_error;
  if (!run) return;
  if (run->pending_status == status && run->error.code == status) return;
  memset(&run->error, 0, sizeof(run->error));
  run->error.code = status;
  if (message) {
    (void)snprintf(run->error.message, sizeof(run->error.message), "%s", message);
    return;
  }
  flow_error = run->flow ? turbo_flow_last_error(run->flow) : NULL;
  if (flow_error && flow_error->code == status) {
    run->error = *flow_error;
  }
}

static void flow_run_registry_remove(turbo_flow_run_t *run) {
  turbo_flow_t *flow;
  size_t count;
  if (!run) return;
  salts_mutex_lock(&run->mutex);
  if (!atomic_load_explicit(&run->registered, memory_order_acquire) ||
      !(flow = run->flow)) {
    salts_mutex_unlock(&run->mutex);
    return;
  }
  salts_mutex_lock(&flow->runtime_mutex);
  if (atomic_load_explicit(&run->registered, memory_order_relaxed)) {
    count = vec_size(&flow->active_runs);
    for (size_t i = 0u; i < count; ++i) {
      turbo_flow_run_t *const *entry =
          (turbo_flow_run_t *const *)vec_at_const(&flow->active_runs, i);
      if (entry && *entry == run) {
        (void)turbo_flow_stl_error(vec_swap_remove(&flow->active_runs, i, NULL));
        break;
      }
    }
    atomic_store_explicit(&run->registered, 0, memory_order_release);
  }
  salts_mutex_unlock(&flow->runtime_mutex);
  salts_mutex_unlock(&run->mutex);
}

static void flow_run_cancel_deadline(turbo_flow_run_t *run) {
  cflow_task_id task_id = 0u;
  int release_ref = 0;
  if (!run) return;
  salts_mutex_lock(&run->mutex);
  if (run->deadline_ref_pending && run->deadline_task_id != 0u) {
    task_id = run->deadline_task_id;
    run->deadline_task_id = 0u;
  }
  salts_mutex_unlock(&run->mutex);
  if (task_id != 0u && cflow_scheduler_cancel(run->scheduler, task_id)) {
    salts_mutex_lock(&run->mutex);
    if (run->deadline_ref_pending) {
      run->deadline_ref_pending = 0;
      release_ref = 1;
    }
    salts_mutex_unlock(&run->mutex);
  }
  if (release_ref) flow_run_release(run);
}

static int flow_run_finish(turbo_flow_run_t *run, turbo_flow_run_state_t state, int status,
                           const char *message, int detach) {
  turbo_flow_t *flow;
  int won = 0;
  if (!run) return 0;
  salts_mutex_lock(&run->mutex);
  flow = run->flow;
  if (!run->terminal) {
    run->terminal = 1;
    run->state = state;
    run->status = status;
    if (status != SALTS_OK) flow_run_copy_error(run, status, message);
    won = 1;
    salts_cond_broadcast(&run->cond);
  }
  salts_mutex_unlock(&run->mutex);
  if (!won) return 0;
  flow_run_cancel_deadline(run);
  if (detach) flow_run_registry_remove(run);
  flow_publish_leave(flow);
  if (detach) {
    salts_mutex_lock(&run->mutex);
    run->flow = NULL;
    salts_mutex_unlock(&run->mutex);
  }
  return 1;
}

static void flow_run_async_value_finish(void *user, const turbo_flow_publish_result_t *result) {
  turbo_flow_run_t *run = (turbo_flow_run_t *)user;
  int fail = 0;
  int complete = 0;
  if (!run || !result) return;
  salts_mutex_lock(&run->mutex);
  if (run->pending_values > 0u) --run->pending_values;
  if (!run->terminal) {
    if (result->status == SALTS_OK) {
      ++run->values;
      complete = run->upstream_done && run->pending_values == 0u;
    } else {
      run->pending_status = result->status;
      flow_run_copy_error(run, result->status, "async terminal stage failed");
      fail = 1;
    }
  }
  salts_mutex_unlock(&run->mutex);
  if (fail) {
    cflow_subscription_cancel(&run->subscription);
    cflow_subscription_close(&run->subscription);
    (void)flow_run_finish(run, TURBO_FLOW_RUN_FAILED, result->status, "async terminal stage failed",
                          1);
  } else if (complete) {
    (void)flow_run_finish(run, TURBO_FLOW_RUN_COMPLETED, SALTS_OK, NULL, 1);
  }
  flow_run_release(run);
}

static bool flow_run_on_value(void *user, const cmeta_type_desc *type, const void *value) {
  turbo_flow_run_t *run = (turbo_flow_run_t *)user;
  turbo_flow_t *flow;
  turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
  flow_async_publication_t *publication = NULL;
  const turbo_flow_error_t *error = NULL;
  int error_context_entered = 0;
  int rc;
  if (!run || !type || !value || !cmeta_type_equal(type, flow_message_type_descriptor())) {
    return false;
  }
  flow_run_retain(run);
  salts_mutex_lock(&run->mutex);
  flow = run->flow;
  if (run->terminal || !flow) {
    salts_mutex_unlock(&run->mutex);
    flow_run_release(run);
    return false;
  }
  salts_mutex_unlock(&run->mutex);

  if (!run->drain_on_stop && flow->has_async_terminal_stage) {
    flow_run_retain(run);
    salts_mutex_lock(&run->mutex);
    ++run->pending_values;
    salts_mutex_unlock(&run->mutex);
    publication = flow_async_publication_create(
        flow, run->source_name, (const turbo_flow_msg_t *)value,
        flow_observer_has_handlers(flow) ? salts_hrtime() : 0u, flow_run_async_value_finish, run);
    if (!publication) {
      salts_mutex_lock(&run->mutex);
      --run->pending_values;
      salts_mutex_unlock(&run->mutex);
      flow_run_release(run);
      rc = SALTS_ENOMEM;
      goto record_result;
    }
  }
  flow_publish_error_context_begin(flow);
  error_context_entered = 1;
  rc = flow_publish_message_entered(flow, run->source_name, (int)run->source_index,
                                    (const turbo_flow_msg_t *)value, &result, publication);
  error = turbo_flow_last_error(flow);
record_result:
  salts_mutex_lock(&run->mutex);
  if (!publication && rc == SALTS_OK) {
    ++run->values;
  } else if (!publication && rc != SALTS_OK) {
    run->pending_status = rc;
    if (error) run->error = *error;
  }
  salts_mutex_unlock(&run->mutex);
  if (error_context_entered) flow_publish_error_context_end(flow);
  if (publication) flow_async_publication_owner_leave(publication);
  flow_run_release(run);
  return rc == SALTS_OK;
}

static void flow_run_on_error(void *user, const char *message) {
  turbo_flow_run_t *run = (turbo_flow_run_t *)user;
  int status;
  if (!run) return;
  flow_run_retain(run);
  salts_mutex_lock(&run->mutex);
  status = run->pending_status;
  salts_mutex_unlock(&run->mutex);
  if (status == SALTS_OK) {
    status = flow_run_status_from_cflow(cflow_subscription_status(&run->subscription));
    if (status == SALTS_OK) status = SALTS_EIO;
  }
  cflow_subscription_close(&run->subscription);
  (void)flow_run_finish(run, TURBO_FLOW_RUN_FAILED, status, message, 1);
  flow_run_release(run);
}

static void flow_run_on_done(void *user) {
  turbo_flow_run_t *run = (turbo_flow_run_t *)user;
  int complete;
  if (!run) return;
  flow_run_retain(run);
  cflow_subscription_close(&run->subscription);
  salts_mutex_lock(&run->mutex);
  run->upstream_done = 1;
  complete = !run->terminal && run->pending_values == 0u;
  salts_mutex_unlock(&run->mutex);
  if (complete) (void)flow_run_finish(run, TURBO_FLOW_RUN_COMPLETED, SALTS_OK, NULL, 1);
  flow_run_release(run);
}

static int flow_run_cancel_with_status(turbo_flow_run_t *run, int status) {
  turbo_flow_run_state_t state =
      status == SALTS_ETIMEDOUT ? TURBO_FLOW_RUN_FAILED : TURBO_FLOW_RUN_CANCELED;
  int terminal;
  if (!run) return SALTS_EINVAL;
  flow_run_retain(run);
  salts_mutex_lock(&run->mutex);
  terminal = run->terminal;
  salts_mutex_unlock(&run->mutex);
  if (terminal) {
    flow_run_release(run);
    return SALTS_EALREADY;
  }
  cflow_subscription_cancel(&run->subscription);
  cflow_subscription_close(&run->subscription);
  (void)flow_run_finish(run, state, status,
                        status == SALTS_ETIMEDOUT ? "Reactive run deadline expired"
                                                  : "Reactive run canceled",
                        1);
  flow_run_release(run);
  return status == SALTS_ETIMEDOUT ? SALTS_ETIMEDOUT : SALTS_OK;
}

static void flow_run_deadline_task(void *user) {
  turbo_flow_run_t *run = (turbo_flow_run_t *)user;
  int setup_complete;
  if (!run) return;
  salts_mutex_lock(&run->mutex);
  run->deadline_task_id = 0u;
  setup_complete = run->setup_complete;
  if (!setup_complete) run->deadline_fired = 1;
  salts_mutex_unlock(&run->mutex);
  if (setup_complete) {
    cflow_subscription_cancel(&run->subscription);
    (void)flow_run_finish(run, TURBO_FLOW_RUN_FAILED, SALTS_ETIMEDOUT,
                          "Reactive run deadline expired", 0);
  }
  salts_mutex_lock(&run->mutex);
  run->deadline_ref_pending = 0;
  salts_mutex_unlock(&run->mutex);
  flow_run_release(run);
}

static int flow_run_graph_init(turbo_flow_run_t *run) {
  cflow_graph surface = {0};
  cflow_graph_init(&surface, flow_message_type_descriptor());
  if (surface.version == 0u || !cflow_graph_normalize(&run->graph, &surface)) {
    cflow_graph_destroy(&surface);
    return SALTS_ENOMEM;
  }
  cflow_graph_destroy(&surface);
  return SALTS_OK;
}

static int flow_run_registry_add(turbo_flow_t *flow, turbo_flow_run_t *run) {
  int rc = SALTS_OK;
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED || flow->admission_state != FLOW_ADMISSION_OPEN ||
      !flow->reactive_scheduler_initialized) {
    rc = flow->admission_state == FLOW_ADMISSION_STOPPING ? SALTS_ESHUTDOWN : SALTS_EINVAL;
  } else if (vec_size(&flow->active_runs) >= flow->async_ingress_config.queue_capacity) {
    rc = SALTS_ENOSPC;
  } else if (turbo_flow_stl_error(vec_push(&flow->active_runs, &run)) != SALTS_OK) {
    rc = SALTS_ENOMEM;
  } else {
    atomic_store_explicit(&run->registered, 1, memory_order_release);
  }
  salts_mutex_unlock(&flow->runtime_mutex);
  return rc;
}

int flow_run_open_internal(turbo_flow_t *flow, const char *source_name,
                           cflow_publisher *publisher,
                           const turbo_flow_run_config_t *config, int drain_on_stop,
                           turbo_flow_run_t **run_out) {
  turbo_flow_run_config_t effective = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_t *run = NULL;
  const flow_stage_plan_impl_t *source;
  cflow_status_result subscribe_result;
  int source_index;
  int rc;
  int entered = 0;
  int deadline_fired = 0;

  if (run_out) *run_out = NULL;
  if (!flow || !source_name || !publisher || !cflow_publisher_valid(publisher) || !run_out) {
    return SALTS_EINVAL;
  }
  if (config) {
    if (config->size < sizeof(*config) || config->version != TURBO_FLOW_RUN_API_VERSION) {
      return SALTS_EINVAL;
    }
    effective = *config;
  }
  rc = flow_publish_enter(flow);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, 0, 0,
                                     rc == SALTS_ESHUTDOWN
                                         ? "flow is not accepting Reactive runs"
                                         : "flow must be started before opening a Reactive run");
  }
  entered = 1;
  source_index = turbo_flow_find_stage(flow, source_name);
  if (source_index < 0) {
    rc = flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                   "Reactive run source is unknown");
    goto cleanup;
  }
  source = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, (size_t)source_index);
  if (!source || !source->is_source) {
    rc = flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0,
                                   "Reactive run target must be a source");
    goto cleanup;
  }

  run = (turbo_flow_run_t *)calloc(1, sizeof(*run));
  if (!run) {
    rc = SALTS_ENOMEM;
    goto cleanup;
  }
  atomic_init(&run->ref_count, 1u);
  atomic_init(&run->registered, 0);
  salts_mutex_init(&run->mutex);
  salts_cond_init(&run->cond);
  if (!run->mutex || !run->cond) {
    rc = SALTS_ENOMEM;
    goto cleanup;
  }
  run->flow = flow;
  run->source_index = (uint32_t)source_index;
  run->source_name = source->name;
  run->scheduler = effective.scheduler ? effective.scheduler : &flow->reactive_scheduler;
  run->state = TURBO_FLOW_RUN_OPEN;
  run->status = SALTS_OK;
  run->pending_status = SALTS_OK;
  run->drain_on_stop = drain_on_stop != 0;
  if (!cflow_scheduler_valid(run->scheduler)) {
    rc = SALTS_EINVAL;
    goto cleanup;
  }
  if (effective.deadline_ms != 0u &&
      (cflow_scheduler_capabilities(run->scheduler) & CMETA_SCHED_CAP_DELAYED) == 0u) {
    rc = SALTS_ENOTSUP;
    goto cleanup;
  }
  rc = flow_run_graph_init(run);
  if (rc != SALTS_OK) goto cleanup;
  run->callbacks = (cflow_subscriber_callbacks){flow_run_on_value, flow_run_on_error,
                                                flow_run_on_done, run};
  run->subscriber = cflow_subscriber_from_callbacks(&run->callbacks);

  if (effective.deadline_ms != 0u) {
    cflow_schedule_result deadline_result;
    flow_run_retain(run);
    run->deadline_ref_pending = 1;
    deadline_result = cflow_scheduler_try_post_after(run->scheduler, effective.deadline_ms,
                                                     flow_run_deadline_task, run);
    if (deadline_result.status != CFLOW_ADMISSION_ACCEPTED) {
      run->deadline_ref_pending = 0;
      flow_run_release(run);
      rc = flow_run_status_from_admission(deadline_result.status);
      goto cleanup;
    }
    salts_mutex_lock(&run->mutex);
    if (run->deadline_ref_pending) run->deadline_task_id = deadline_result.task_id;
    salts_mutex_unlock(&run->mutex);
  }

  rc = flow_run_registry_add(flow, run);
  if (rc != SALTS_OK) goto cleanup;
  subscribe_result = cflow_subscribe_with_options(&run->subscription, &run->graph, publisher,
                                                  run->scheduler, &run->subscriber, NULL);
  if (!cflow_status_result_is_ok(subscribe_result)) {
    rc = flow_run_status_from_cflow(subscribe_result.status);
    flow_run_registry_remove(run);
    goto cleanup;
  }
  salts_mutex_lock(&run->mutex);
  run->setup_complete = 1;
  deadline_fired = run->deadline_fired;
  salts_mutex_unlock(&run->mutex);
  *run_out = run;
  entered = 0;
  if (deadline_fired) (void)flow_run_cancel_with_status(run, SALTS_ETIMEDOUT);
  return SALTS_OK;

cleanup:
  if (run) {
    if (run->mutex) {
      flow_run_cancel_deadline(run);
      if (atomic_load_explicit(&run->registered, memory_order_acquire)) {
        flow_run_registry_remove(run);
      }
    }
    flow_run_release(run);
  }
  if (entered) flow_publish_leave(flow);
  return rc;
}

int turbo_flow_run_open(turbo_flow_t *flow, const char *source_name, cflow_publisher *publisher,
                        const turbo_flow_run_config_t *config, turbo_flow_run_t **run_out) {
  return flow_run_open_internal(flow, source_name, publisher, config, 0, run_out);
}

int turbo_flow_run_request(turbo_flow_run_t *run, size_t demand) {
  cflow_status_result result;
  int terminal;
  int status;
  if (!run || demand == 0u) return SALTS_EINVAL;
  salts_mutex_lock(&run->mutex);
  terminal = run->terminal;
  status = run->status;
  if (!terminal) run->state = TURBO_FLOW_RUN_ACTIVE;
  salts_mutex_unlock(&run->mutex);
  if (terminal) return status == SALTS_OK ? SALTS_ESHUTDOWN : status;
  result = cflow_subscription_request_result(&run->subscription, demand);
  status = flow_run_status_from_cflow(result.status);
  salts_mutex_lock(&run->mutex);
  if (run->terminal && run->status != SALTS_OK) status = run->status;
  salts_mutex_unlock(&run->mutex);
  return status;
}

static int flow_run_result_valid(const turbo_flow_run_result_t *result) {
  return result && result->size >= sizeof(*result) &&
         result->version == TURBO_FLOW_RUN_API_VERSION;
}

int turbo_flow_run_snapshot(const turbo_flow_run_t *run, turbo_flow_run_result_t *result) {
  turbo_flow_run_result_t snapshot = TURBO_FLOW_RUN_RESULT_INIT;
  if (!run || !flow_run_result_valid(result)) return SALTS_EINVAL;
  salts_mutex_lock((salts_mutex_t *)&run->mutex);
  snapshot.state = run->state;
  snapshot.status = run->status;
  snapshot.values = run->values;
  snapshot.error = run->error;
  salts_mutex_unlock((salts_mutex_t *)&run->mutex);
  snapshot.outstanding_demand = cflow_subscription_outstanding_demand(&run->subscription);
  *result = snapshot;
  return SALTS_OK;
}

int turbo_flow_run_wait(turbo_flow_run_t *run, uint64_t timeout_ms,
                        turbo_flow_run_result_t *result) {
  uint64_t started_at;
  uint64_t timeout_ns;
  int terminal;
  int status;
  if (!run || !flow_run_result_valid(result)) return SALTS_EINVAL;
  started_at = salts_hrtime();
  timeout_ns = timeout_ms == UINT64_MAX || timeout_ms > UINT64_MAX / UINT64_C(1000000)
                   ? UINT64_MAX
                   : timeout_ms * UINT64_C(1000000);
  salts_mutex_lock(&run->mutex);
  while (!run->terminal) {
    uint64_t elapsed;
    if (timeout_ns == 0u) break;
    if (timeout_ns == UINT64_MAX) {
      salts_cond_wait(&run->cond, &run->mutex);
      continue;
    }
    elapsed = salts_hrtime() - started_at;
    if (elapsed >= timeout_ns ||
        salts_cond_timedwait(&run->cond, &run->mutex, timeout_ns - elapsed) != 0) {
      break;
    }
  }
  terminal = run->terminal;
  status = run->status;
  salts_mutex_unlock(&run->mutex);
  (void)turbo_flow_run_snapshot(run, result);
  return terminal ? status : SALTS_ETIMEDOUT;
}

int turbo_flow_run_cancel(turbo_flow_run_t *run) {
  return flow_run_cancel_with_status(run, SALTS_ECANCELED);
}

void turbo_flow_run_close(turbo_flow_run_t *run) {
  int terminal;
  if (!run) return;
  salts_mutex_lock(&run->mutex);
  terminal = run->terminal;
  salts_mutex_unlock(&run->mutex);
  if (!terminal) (void)flow_run_cancel_with_status(run, SALTS_ECANCELED);
  cflow_subscription_close(&run->subscription);
  flow_run_registry_remove(run);
  flow_run_release(run);
}

int flow_reactive_runtime_start(turbo_flow_t *flow) {
  size_t capacity;
  if (!flow) return SALTS_EINVAL;
  if (flow->reactive_scheduler_initialized) return SALTS_EALREADY;
  capacity = flow->async_ingress_config.queue_capacity;
  if (turbo_flow_stl_error(vec_reserve(&flow->active_runs, capacity)) != SALTS_OK) {
    return SALTS_ENOMEM;
  }
  if (!cflow_scheduler_worker_init_with_capacity(&flow->reactive_scheduler,
                                                 flow->async_ingress_config.workers, capacity,
                                                 capacity)) {
    return SALTS_ENOMEM;
  }
  flow->reactive_scheduler_initialized = 1;
  return SALTS_OK;
}

void flow_reactive_runtime_cancel(turbo_flow_t *flow) {
  if (!flow) return;
  for (;;) {
    turbo_flow_run_t *run = NULL;
    int terminal;
    salts_mutex_lock(&flow->runtime_mutex);
    if (!vec_empty(&flow->active_runs)) {
      for (size_t i = 0u; i < vec_size(&flow->active_runs); ++i) {
        turbo_flow_run_t *const *entry =
            (turbo_flow_run_t *const *)vec_at_const(&flow->active_runs, i);
        if (entry && *entry && !(*entry)->drain_on_stop) {
          run = *entry;
          flow_run_retain(run);
          break;
        }
      }
    }
    salts_mutex_unlock(&flow->runtime_mutex);
    if (!run) break;
    salts_mutex_lock(&run->mutex);
    terminal = run->terminal;
    salts_mutex_unlock(&run->mutex);
    if (terminal) {
      cflow_subscription_close(&run->subscription);
      flow_run_registry_remove(run);
    } else {
      (void)flow_run_cancel_with_status(run, SALTS_ECANCELED);
    }
    flow_run_release(run);
  }
}

void flow_reactive_runtime_stop(turbo_flow_t *flow) {
  if (!flow) return;
  flow_reactive_runtime_cancel(flow);
  if (flow->reactive_scheduler_initialized) {
    (void)cflow_scheduler_shutdown(&flow->reactive_scheduler);
    (void)cflow_scheduler_wait_idle(&flow->reactive_scheduler);
    cflow_scheduler_destroy(&flow->reactive_scheduler);
    flow->reactive_scheduler_initialized = 0;
  }
  (void)turbo_flow_stl_error(vec_clear(&flow->active_runs));
}
