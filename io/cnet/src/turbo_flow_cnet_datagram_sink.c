#include "turbo_flow_cnet.h"
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
#include "turbo_flow_cnet_test_internal.h"
#endif

#include <cflow/executor.h>
#include <cflow/io_actor.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct datagram_sink_operation_s {
  turbo_flow_async_terminal_claim_t claim;
  size_t bytes;
  struct datagram_sink_operation_s *next_pending;
} datagram_sink_operation_t;

struct turbo_flow_cnet_datagram_sink_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr host;
  cnet_datagram_config config;
  cnet_datagram_peer peer;
  cnet_datagram datagram;
  cflow_executor executor;
  cflow_io_actor actor;
  cflow_io_request_id *delivered_ids;
  size_t delivered_count;
  size_t delivered_capacity;
  /* Only the exclusive progress owner modifies this list. Its size is bounded
   * by Actor request_capacity == native send_capacity. */
  datagram_sink_operation_t *pending;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  uint32_t stop_timeout_ms;
  uint16_t bound_port;
  atomic_uint_fast64_t next_lease;
  atomic_uint_fast64_t messages_sent;
  atomic_uint_fast64_t bytes_sent;
  atomic_int state;
  atomic_int status;
  atomic_bool datagram_initialized;
  bool executor_initialized;
  bool actor_initialized;
  atomic_bool detached;
  cmeta_mutex_t lifecycle_mutex;
  cmeta_cond_t lifecycle_cond;
  bool owner_active;
  size_t submissions_active;
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
  int test_stop_status;
  int test_destroy_status;
  bool test_stop_after_drain;
#endif
};

static int datagram_sink_stop_internal(turbo_flow_cnet_datagram_sink_t *sink);

static turbo_flow_cnet_datagram_sink_state_t
datagram_sink_state(const turbo_flow_cnet_datagram_sink_t *sink) {
  return (turbo_flow_cnet_datagram_sink_state_t)atomic_load_explicit(&sink->state,
                                                                     memory_order_acquire);
}

static int datagram_sink_submit_status(cflow_io_submit_status status) {
  switch (status) {
  case CFLOW_IO_SUBMIT_ACCEPTED:
    return SALTS_OK;
  case CFLOW_IO_SUBMIT_FULL:
    return SALTS_ENOSPC;
  case CFLOW_IO_SUBMIT_CLOSED:
    return SALTS_ESHUTDOWN;
  case CFLOW_IO_SUBMIT_LEASE_IN_USE:
    return SALTS_EALREADY;
  case CFLOW_IO_SUBMIT_ID_EXHAUSTED:
    return SALTS_ERANGE;
  case CFLOW_IO_SUBMIT_INVALID_ARGUMENT:
  default:
    return SALTS_EINVAL;
  }
}

static int datagram_sink_completion_status(const cflow_io_completion *completion) {
  if (!completion) return SALTS_EPROTO;
  switch (completion->kind) {
  case CFLOW_IO_COMPLETION_OK:
    return SALTS_OK;
  case CFLOW_IO_COMPLETION_CANCELLED:
    return SALTS_ECANCELED;
  case CFLOW_IO_COMPLETION_EOF:
    return SALTS_ESHUTDOWN;
  case CFLOW_IO_COMPLETION_FAILED:
  default:
    return completion->error != SALTS_OK ? completion->error : SALTS_EIO;
  }
}

static void datagram_sink_operation_release(void *user) {
  datagram_sink_operation_t *operation = (datagram_sink_operation_t *)user;
  if (!operation) return;
  if (operation->claim._impl)
    (void)turbo_flow_async_terminal_complete(&operation->claim, SALTS_ECANCELED, NULL);
  free(operation);
}

static int datagram_sink_backend_submit(void *user, cflow_io_actor *actor,
                                        cflow_io_request_id request_id, cflow_io_lease_id lease_id,
                                        void *operation_user) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  datagram_sink_operation_t *operation = (datagram_sink_operation_t *)operation_user;
  const turbo_flow_msg_t *message;
  int rc;
  (void)actor;
  (void)lease_id;
  if (!sink || !operation) return SALTS_EINVAL;
  message = turbo_flow_async_terminal_claim_message(&operation->claim);
  if (!message || !message->payload.data || message->payload.len == 0u ||
      message->payload.len != operation->bytes) {
    return SALTS_EPROTO;
  }
  rc = cnet_datagram_send(&sink->datagram, &sink->peer, message->payload.data,
                          message->payload.len, request_id);
  if (rc == SALTS_OK) {
    operation->next_pending = sink->pending;
    sink->pending = operation;
  }
  return rc;
}

static int datagram_sink_backend_cancel(void *user, cflow_io_request_id request_id) {
  (void)user;
  (void)request_id;
  return SALTS_ENOTSUP;
}

static void datagram_sink_actor_completion(void *user, cflow_io_request_id request_id,
                                           cflow_io_lease_id lease_id, void *operation_user,
                                           const cflow_io_completion *completion) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  datagram_sink_operation_t *operation = (datagram_sink_operation_t *)operation_user;
  datagram_sink_operation_t **pending;
  int status = datagram_sink_completion_status(completion);
  (void)lease_id;
  if (!sink || !operation) return;
  pending = &sink->pending;
  while (*pending && *pending != operation) pending = &(*pending)->next_pending;
  if (*pending) *pending = operation->next_pending;
  if (status == SALTS_OK && completion->bytes != operation->bytes) status = SALTS_EPROTO;
  if (status == SALTS_OK && operation->claim._impl) {
    uint64_t messages = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
    uint64_t bytes = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
    if (messages == UINT64_MAX || bytes > UINT64_MAX - (uint64_t)operation->bytes) {
      status = SALTS_ERANGE;
    } else {
      atomic_store_explicit(&sink->messages_sent, messages + 1u, memory_order_relaxed);
      atomic_store_explicit(&sink->bytes_sent, bytes + (uint64_t)operation->bytes,
                            memory_order_relaxed);
    }
  }
  if (sink->delivered_count < sink->delivered_capacity)
    sink->delivered_ids[sink->delivered_count++] = request_id;
  else status = SALTS_EPROTO;
  if (operation->claim._impl)
    (void)turbo_flow_async_terminal_complete(&operation->claim, status, NULL);
}

static void datagram_sink_actor_wake(void *user) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  if (sink && atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire))
    (void)cnet_datagram_wake(&sink->datagram);
}

static void datagram_sink_on_receive(void *user, cnet_datagram *datagram,
                                     const cnet_datagram_peer *peer,
                                     const cnet_receive_view *view) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  (void)datagram;
  (void)peer;
  (void)view;
  if (!sink) return;
  atomic_store_explicit(&sink->status, SALTS_EPROTO, memory_order_relaxed);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED, memory_order_release);
}

static void datagram_sink_on_send(void *user, cnet_datagram *datagram,
                                  const cnet_datagram_peer *peer, size_t size, int status,
                                  uint64_t tag) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  cflow_io_completion completion;
  (void)datagram;
  (void)peer;
  if (!sink || tag == 0u) return;
  completion.kind = status == SALTS_OK ? CFLOW_IO_COMPLETION_OK
                                       : (status == SALTS_ECANCELED ? CFLOW_IO_COMPLETION_CANCELLED
                                                                    : CFLOW_IO_COMPLETION_FAILED);
  completion.bytes = size;
  completion.error = status;
  (void)cflow_io_actor_complete(&sink->actor, (cflow_io_request_id)tag, &completion);
}

static void datagram_sink_acknowledge(turbo_flow_cnet_datagram_sink_t *sink) {
  size_t index;
  if (!sink) return;
  for (index = 0u; index < sink->delivered_count; ++index)
    (void)cflow_io_actor_acknowledge(&sink->actor, sink->delivered_ids[index]);
  sink->delivered_count = 0u;
}

static void datagram_sink_drive_actor(turbo_flow_cnet_datagram_sink_t *sink) {
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
  (void)cflow_executor_run_ready(&sink->executor);
  datagram_sink_acknowledge(sink);
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
}

static int datagram_sink_drain_actor(turbo_flow_cnet_datagram_sink_t *sink) {
  cflow_io_run_result result;
  size_t delivered;
  do {
    result = cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
    if (result.status == CFLOW_IO_RUN_BUSY) return SALTS_EBUSY;
    if (result.status == CFLOW_IO_RUN_INVALID_ARGUMENT) return SALTS_EPROTO;
    delivered = cflow_executor_run_ready(&sink->executor);
    datagram_sink_acknowledge(sink);
  } while (result.progressed > 0u || delivered > 0u);
  return cflow_io_actor_is_quiescent(&sink->actor) ? SALTS_OK : SALTS_EBUSY;
}

static int datagram_sink_start(void *ctx, turbo_flow_t *flow,
                                const turbo_flow_stage_plan_t *stage) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  cflow_io_actor_config actor_config = {0};
  uint16_t bound_port = 0u;
  int rc;
  (void)stage;
  if (!sink) return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (flow != sink->flow || atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      sink->owner_active || sink->submissions_active > 0u ||
      (datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED &&
       datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED)) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  sink->owner_active = true;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->status, SALTS_OK, memory_order_relaxed);
  sink->config.host = sink->host;
  sink->config.observer.on_receive = datagram_sink_on_receive;
  sink->config.observer.on_send = datagram_sink_on_send;
  sink->config.observer.user = sink;
  rc = cnet_datagram_init(&sink->datagram, &sink->config);
  if (rc != SALTS_OK) goto fail;
  atomic_store_explicit(&sink->datagram_initialized, true, memory_order_release);
  rc = cnet_datagram_port(&sink->datagram, &bound_port);
  if (rc != SALTS_OK) goto fail;
  if (!cflow_executor_manual_init_with_capacity(&sink->executor, sink->config.send_capacity)) {
    rc = SALTS_ENOMEM;
    goto fail;
  }
  sink->executor_initialized = true;
  actor_config.request_capacity = sink->config.send_capacity;
  actor_config.command_capacity = sink->actor_command_capacity;
  actor_config.executor = &sink->executor;
  actor_config.backend.submit = datagram_sink_backend_submit;
  actor_config.backend.cancel = datagram_sink_backend_cancel;
  actor_config.backend_user = sink;
  actor_config.completion = datagram_sink_actor_completion;
  actor_config.completion_user = sink;
  actor_config.wake = datagram_sink_actor_wake;
  actor_config.wake_user = sink;
  rc = cflow_io_actor_init(&sink->actor, &actor_config);
  if (rc != SALTS_OK) goto fail;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  sink->actor_initialized = true;
  sink->bound_port = bound_port;
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING, memory_order_release);
  sink->owner_active = false;
  cmeta_cond_broadcast(&sink->lifecycle_cond);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;

fail:
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED, memory_order_release);
  sink->owner_active = false;
  cmeta_cond_broadcast(&sink->lifecycle_cond);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return rc;
}

static int datagram_sink_stop_internal(turbo_flow_cnet_datagram_sink_t *sink) {
  int status = SALTS_OK;
  int rc;
  if (!sink) return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED ||
      datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_OK;
  }
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING, memory_order_release);
  while (sink->owner_active || sink->submissions_active > 0u)
    cmeta_cond_wait(&sink->lifecycle_cond, &sink->lifecycle_mutex);
  if (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED ||
      datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_OK;
  }
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING, memory_order_release);
  sink->owner_active = true;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  if (sink->actor_initialized) {
    status = cflow_io_actor_close(&sink->actor);
    if (status == SALTS_EALREADY) status = SALTS_OK;
    if (status == SALTS_OK) {
      rc = datagram_sink_drain_actor(sink);
      if (rc != SALTS_OK && rc != SALTS_EBUSY) status = rc;
    }
  }
  if (atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) {
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
    if (sink->test_stop_status != SALTS_OK && !sink->test_stop_after_drain) {
      rc = sink->test_stop_status;
      sink->test_stop_status = SALTS_OK;
    } else
#endif
    rc = cnet_datagram_stop(&sink->datagram, sink->stop_timeout_ms);
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
    if (rc == SALTS_OK && sink->test_stop_status != SALTS_OK) {
      rc = sink->test_stop_status;
      sink->test_stop_status = SALTS_OK;
    }
#endif
    if (status == SALTS_OK) status = rc;
  }
  if (sink->actor_initialized) {
    rc = datagram_sink_drain_actor(sink);
    if (status == SALTS_OK) status = rc;
    if (status != SALTS_OK) {
      /* CNet copied each admitted UDP payload. Settle Flow claims while the
       * Actor tokens, native slots and callback context remain retained. */
      for (datagram_sink_operation_t *op = sink->pending; op; op = op->next_pending)
        if (op->claim._impl)
          (void)turbo_flow_async_terminal_complete(&op->claim, SALTS_ESHUTDOWN, NULL);
    }
  }
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (sink->actor_initialized && cflow_io_actor_is_quiescent(&sink->actor)) {
    rc = cflow_io_actor_destroy(&sink->actor);
    if (rc == SALTS_OK) sink->actor_initialized = false;
    else if (status == SALTS_OK) status = rc;
  }
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  if (!sink->actor_initialized && sink->executor_initialized) {
    if (cflow_executor_shutdown(&sink->executor)) {
      cflow_executor_destroy(&sink->executor);
      sink->executor_initialized = false;
    } else if (status == SALTS_OK) status = SALTS_EBUSY;
  }
  if (!sink->actor_initialized && !sink->executor_initialized &&
      atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) {
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
    if (sink->test_destroy_status != SALTS_OK) {
      rc = sink->test_destroy_status;
      sink->test_destroy_status = SALTS_OK;
    } else
#endif
    rc = cnet_datagram_destroy(&sink->datagram);
    if (!sink->datagram.impl)
      atomic_store_explicit(&sink->datagram_initialized, false, memory_order_release);
    if (status == SALTS_OK) status = rc;
  }
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (status == SALTS_OK && (sink->actor_initialized || sink->executor_initialized ||
      atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire))) status = SALTS_EBUSY;
  if (!atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) sink->bound_port = 0u;
  if (status != SALTS_OK) atomic_store_explicit(&sink->status, status, memory_order_relaxed);
  atomic_store_explicit(&sink->state, status == SALTS_OK ? TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED
                                                       : TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED,
                        memory_order_release);
  sink->owner_active = false;
  cmeta_cond_broadcast(&sink->lifecycle_cond);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return status;
}

#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
int turbo_flow_cnet_test_datagram_sink_fail_next_stop(
    turbo_flow_cnet_datagram_sink_t *sink, int status, bool after_drain) {
  if (!sink || status == SALTS_OK || (after_drain && status == SALTS_ETIMEDOUT))
    return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (sink->owner_active || sink->test_stop_status != SALTS_OK ||
      !atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->test_stop_status = status;
  sink->test_stop_after_drain = after_drain;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;
}

int turbo_flow_cnet_test_datagram_sink_fail_next_destroy(
    turbo_flow_cnet_datagram_sink_t *sink, int status) {
  if (!sink || status == SALTS_OK) return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (sink->owner_active || sink->test_destroy_status != SALTS_OK ||
      !atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->test_destroy_status = status;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;
}

int turbo_flow_cnet_test_datagram_sink_submit_ready(turbo_flow_cnet_datagram_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (sink->owner_active || !sink->actor_initialized ||
      datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->owner_active = true;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  datagram_sink_drive_actor(sink);
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  sink->owner_active = false;
  cmeta_cond_broadcast(&sink->lifecycle_cond);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;
}
#endif

static void datagram_sink_stop(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  int rc;
  (void)stage;
  rc = datagram_sink_stop_internal((turbo_flow_cnet_datagram_sink_t *)ctx);
  if (rc != SALTS_OK) (void)turbo_flow_adapter_report_stop_status(flow, rc);
}

static void datagram_sink_shutdown(void *ctx) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  if (!sink || datagram_sink_stop_internal(sink) != SALTS_OK) return;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  sink->flow = NULL;
  atomic_store_explicit(&sink->detached, true, memory_order_release);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED, memory_order_release);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
}

static int datagram_sink_next_lease(turbo_flow_cnet_datagram_sink_t *sink,
                                    cflow_io_lease_id *lease_out) {
  uint_fast64_t lease;
  uint_fast64_t next;
  if (!sink || !lease_out) return SALTS_EINVAL;
  lease = atomic_load_explicit(&sink->next_lease, memory_order_relaxed);
  do {
    if (lease == 0u || lease == UINT64_MAX) return SALTS_ERANGE;
    next = lease + 1u;
  } while (!atomic_compare_exchange_weak_explicit(&sink->next_lease, &lease, next,
                                                  memory_order_relaxed, memory_order_relaxed));
  *lease_out = (cflow_io_lease_id)lease;
  return SALTS_OK;
}

static void datagram_sink_submission_finish(turbo_flow_cnet_datagram_sink_t *sink) {
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  --sink->submissions_active;
  cmeta_cond_broadcast(&sink->lifecycle_cond);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
}

static int datagram_sink_async_submit(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage,
                                      const turbo_flow_msg_t *message,
                                      turbo_flow_async_terminal_claim_t *claim) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  datagram_sink_operation_t *operation;
  cflow_io_operation io_operation;
  cflow_io_submit_result submitted;
  cflow_io_lease_id lease_id;
  turbo_flow_cnet_datagram_sink_state_t state;
  int rc;
  (void)stage;
  if (!sink || !message || !claim) return SALTS_EINVAL;
  if (!message->payload.data || message->payload.len == 0u) return SALTS_EINVAL;
  if (message->payload.len > sink->max_message_bytes) return SALTS_EMSGSIZE;
  rc = datagram_sink_next_lease(sink, &lease_id);
  if (rc != SALTS_OK) return rc;
  operation = (datagram_sink_operation_t *)calloc(1u, sizeof(*operation));
  if (!operation) return SALTS_ENOMEM;
  operation->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  operation->bytes = message->payload.len;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  state = datagram_sink_state(sink);
  if (flow != sink->flow || atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !sink->actor_initialized || state != TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING) {
    rc = state == TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED
             ? atomic_load_explicit(&sink->status, memory_order_relaxed) : SALTS_EBUSY;
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    free(operation);
    return rc;
  }
  ++sink->submissions_active;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  rc = turbo_flow_async_terminal_claim_move(&operation->claim, claim);
  if (rc != SALTS_OK) {
    free(operation);
    datagram_sink_submission_finish(sink);
    return rc;
  }
  io_operation.user = operation;
  io_operation.release = datagram_sink_operation_release;
  submitted = cflow_io_actor_try_submit(&sink->actor, lease_id, &io_operation);
  rc = datagram_sink_submit_status(submitted.status);
  if (rc != SALTS_OK) {
    (void)turbo_flow_async_terminal_claim_move(claim, &operation->claim);
    free(operation);
  }
  datagram_sink_submission_finish(sink);
  return rc;
}

static int datagram_sink_config_validate(const turbo_flow_cnet_datagram_sink_config_t *config) {
  const cnet_datagram_config *datagram;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->datagram ||
      config->max_message_bytes == 0u || config->actor_command_capacity == 0u ||
      config->actor_max_steps_per_poll == 0u || config->stop_timeout_ms == 0u) {
    return SALTS_EINVAL;
  }
  if (turbo_flow_state(config->flow) == TURBO_FLOW_STATE_COMPILED ||
      turbo_flow_state(config->flow) == TURBO_FLOW_STATE_STARTED)
    return SALTS_EBUSY;
  datagram = config->datagram;
  if (datagram->size != sizeof(*datagram) || !datagram->host || datagram->host[0] == '\0' ||
      datagram->send_capacity == 0u || datagram->request_capacity <= datagram->send_capacity ||
      datagram->completion_batch_capacity == 0u ||
      datagram->completion_batch_capacity > datagram->request_capacity ||
      datagram->max_datagram_bytes == 0u ||
      datagram->max_datagram_bytes > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      datagram->receive_buffer_bytes < datagram->max_datagram_bytes ||
      datagram->observer.on_receive || datagram->observer.on_send || datagram->observer.user ||
      (datagram->reuse_port != 0 && datagram->reuse_port != 1) ||
      !native_io_backend_kind_supported(datagram->backend) ||
      config->max_message_bytes > datagram->max_datagram_bytes || config->peer.port == 0u ||
      (config->peer.family != CNET_DATAGRAM_ADDRESS_IPV4 &&
       config->peer.family != CNET_DATAGRAM_ADDRESS_IPV6)) {
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

int turbo_flow_cnet_datagram_sink_register(const turbo_flow_cnet_datagram_sink_config_t *config,
                                           turbo_flow_cnet_datagram_sink_t **sink_out) {
  turbo_flow_cnet_datagram_sink_t *sink;
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  int rc;
  if (!sink_out) return SALTS_EINVAL;
  *sink_out = NULL;
  rc = datagram_sink_config_validate(config);
  if (rc != SALTS_OK) return rc;
  sink = (turbo_flow_cnet_datagram_sink_t *)calloc(1u, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  cmeta_mutex_init(&sink->lifecycle_mutex);
  if (!sink->lifecycle_mutex) {
    free(sink);
    return SALTS_ENOMEM;
  }
  cmeta_cond_init(&sink->lifecycle_cond);
  if (!sink->lifecycle_cond) {
    cmeta_mutex_destroy(&sink->lifecycle_mutex);
    free(sink);
    return SALTS_ENOMEM;
  }
  sink->flow = config->flow;
  sink->adapter_name = tstr_dup(config->adapter_name);
  sink->host = tstr_dup(config->datagram->host);
  sink->config = *config->datagram;
  sink->peer = config->peer;
  sink->max_message_bytes = config->max_message_bytes;
  sink->actor_command_capacity = config->actor_command_capacity;
  sink->actor_max_steps_per_poll = config->actor_max_steps_per_poll;
  sink->stop_timeout_ms = config->stop_timeout_ms;
  sink->delivered_capacity = config->datagram->send_capacity;
  sink->delivered_ids =
      (cflow_io_request_id *)calloc(sink->delivered_capacity, sizeof(*sink->delivered_ids));
  atomic_init(&sink->next_lease, 1u);
  atomic_init(&sink->messages_sent, 0u);
  atomic_init(&sink->bytes_sent, 0u);
  atomic_init(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED);
  atomic_init(&sink->status, SALTS_OK);
  atomic_init(&sink->datagram_initialized, false);
  atomic_init(&sink->detached, false);
  if (!sink->adapter_name || !sink->host || !sink->delivered_ids) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->host);
    free(sink->delivered_ids);
    cmeta_cond_destroy(&sink->lifecycle_cond);
    cmeta_mutex_destroy(&sink->lifecycle_mutex);
    free(sink);
    return SALTS_ENOMEM;
  }
  adapter_ops.start = datagram_sink_start;
  adapter_ops.stop = datagram_sink_stop;
  adapter_ops.shutdown = datagram_sink_shutdown;
  async_ops.submit = datagram_sink_async_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  rc = turbo_flow_register_async_terminal_adapter_ex(config->flow, config->adapter_name,
                                                     &adapter_ops, &async_ops, sink, &schema);
  if (rc != SALTS_OK) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->host);
    free(sink->delivered_ids);
    cmeta_cond_destroy(&sink->lifecycle_cond);
    cmeta_mutex_destroy(&sink->lifecycle_mutex);
    free(sink);
    return rc;
  }
  *sink_out = sink;
  return SALTS_OK;
}

static int datagram_sink_snapshot_locked(const turbo_flow_cnet_datagram_sink_t *sink,
                                          turbo_flow_cnet_datagram_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_datagram_sink_snapshot_t current = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  current.state = datagram_sink_state(sink);
  current.status = atomic_load_explicit(&sink->status, memory_order_relaxed);
  current.bound_port = sink->bound_port;
  current.messages_sent = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
  current.bytes_sent = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
  if (sink->actor_initialized) {
    if (!cflow_io_actor_get_stats(&sink->actor, &stats)) return SALTS_EPROTO;
    current.active_requests = stats.active_requests;
  }
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_datagram_sink_snapshot(const turbo_flow_cnet_datagram_sink_t *sink,
                                           turbo_flow_cnet_datagram_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_datagram_sink_t *mutable_sink = (turbo_flow_cnet_datagram_sink_t *)sink;
  int rc;
  if (!sink || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION) return SALTS_EINVAL;
  cmeta_mutex_lock(&mutable_sink->lifecycle_mutex);
  rc = datagram_sink_snapshot_locked(sink, snapshot);
  cmeta_mutex_unlock(&mutable_sink->lifecycle_mutex);
  return rc;
}

int turbo_flow_cnet_datagram_sink_poll(turbo_flow_cnet_datagram_sink_t *sink, uint32_t timeout_ms,
                                       turbo_flow_cnet_datagram_sink_snapshot_t *snapshot) {
  size_t events = 0u;
  int rc;
  if (!sink || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                            snapshot->version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION)))
    return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire) ||
      !sink->actor_initialized) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  if (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING ||
      datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_ESHUTDOWN;
  }
  if (sink->owner_active) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->owner_active = true;
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  datagram_sink_drive_actor(sink);
  rc = cnet_datagram_poll(&sink->datagram, timeout_ms, &events);
  if (rc != SALTS_OK && datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED) {
    atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED, memory_order_release);
  }
  datagram_sink_drive_actor(sink);
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (snapshot) {
    int snapshot_status = datagram_sink_snapshot_locked(sink, snapshot);
    if (rc == SALTS_OK) rc = snapshot_status;
  }
  sink->owner_active = false;
  cmeta_cond_broadcast(&sink->lifecycle_cond);
  if (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED)
    rc = atomic_load_explicit(&sink->status, memory_order_relaxed);
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  return rc;
}

int turbo_flow_cnet_datagram_sink_destroy(turbo_flow_cnet_datagram_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  cmeta_mutex_lock(&sink->lifecycle_mutex);
  if (!atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire) ||
      sink->actor_initialized || sink->executor_initialized || sink->owner_active ||
      sink->submissions_active > 0u) {
    cmeta_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  cmeta_mutex_unlock(&sink->lifecycle_mutex);
  tstr_free(sink->adapter_name);
  tstr_free(sink->host);
  free(sink->delivered_ids);
  cmeta_cond_destroy(&sink->lifecycle_cond);
  cmeta_mutex_destroy(&sink->lifecycle_mutex);
  free(sink);
  return SALTS_OK;
}
