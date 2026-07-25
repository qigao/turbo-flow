#include "flow_coronet_execution.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct tf_coronet_call_s {
  tf_coronet_execution_call_fn fn;
  void *arg;
  turbo_mutex_t mutex;
  turbo_cond_t cond;
  atomic_int refs;
  int started;
  int canceled;
  int done;
  int status;
} tf_coronet_call_t;

static void tf_coronet_call_release(tf_coronet_call_t *call) {
  if (!call || atomic_fetch_sub_explicit(&call->refs, 1, memory_order_acq_rel) != 1) return;
  turbo_cond_destroy(&call->cond);
  turbo_mutex_destroy(&call->mutex);
  free(call);
}

static void tf_coronet_call_run(void *arg1, void *arg2) {
  tf_coronet_call_t *call = (tf_coronet_call_t *)arg1;
  int status;
  (void)arg2;
  turbo_mutex_lock(&call->mutex);
  if (call->canceled) {
    turbo_mutex_unlock(&call->mutex);
    tf_coronet_call_release(call);
    return;
  }
  call->started = 1;
  turbo_mutex_unlock(&call->mutex);
  status = call->fn(call->arg);
  turbo_mutex_lock(&call->mutex);
  call->status = status;
  call->done = 1;
  turbo_cond_signal(&call->cond);
  turbo_mutex_unlock(&call->mutex);
  tf_coronet_call_release(call);
}

static void tf_coronet_execution_loop(void *arg) {
  tf_coronet_execution_t *execution = (tf_coronet_execution_t *)arg;
  if (execution && execution->context) {
    (void)coro_context_run(execution->context, TURBO_RUN_DEFAULT);
  }
}

static void tf_coronet_execution_stop_post(void *arg1, void *arg2) {
  (void)arg2;
  /* The caller already clears persistent mode before posting this wake-up.
   * Let the loop drain canceled admission/close coroutines before it exits;
   * coro_context_stop() would terminate after one scheduler tick and can
   * leave transport-owned sockets unreleased. */
  coro_context_set_persistent((coro_context_t *)arg1, 0);
}

int tf_coronet_execution_init(tf_coronet_execution_t *execution,
                              const turbo_flow_coronet_execution_binding_t *binding) {
  return tf_coronet_execution_init_with_pool(execution, binding, NULL);
}

int tf_coronet_execution_init_with_pool(
    tf_coronet_execution_t *execution,
    const turbo_flow_coronet_execution_binding_t *binding,
    const coro_object_pool_config_t *private_pool_config) {
  int rc;
  if (!execution) return TURBO_EINVAL;
  memset(execution, 0, sizeof(*execution));
  rc = turbo_flow_coronet_execution_binding_validate(binding);
  if (rc != TURBO_OK) return rc;
  execution->kind = binding->kind;
  switch (binding->kind) {
  case TURBO_FLOW_CORONET_EXECUTION_PRIVATE:
    execution->context = coro_context_create_ex(NULL, private_pool_config);
    execution->drives_context = 1;
    execution->owns_context = 1;
    break;
  case TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT:
    if (private_pool_config) return TURBO_ENOTSUP;
    execution->context = binding->context;
    break;
  case TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT:
    if (private_pool_config) return TURBO_ENOTSUP;
    execution->context = binding->context;
    execution->drives_context = 1;
    execution->owns_context = 1;
    break;
  case TURBO_FLOW_CORONET_EXECUTION_POOL_LANE:
    if (private_pool_config) return TURBO_ENOTSUP;
    execution->context = coro_thread_pool_get_context(binding->pool, (int)binding->lane);
    break;
  default:
    return TURBO_EINVAL;
  }
  if (!execution->context) return TURBO_ENOMEM;
  if (execution->drives_context) coro_context_set_persistent(execution->context, 1);
  return TURBO_OK;
}

int tf_coronet_execution_start(tf_coronet_execution_t *execution) {
  if (!execution || !execution->context) return TURBO_EINVAL;
  if (!execution->drives_context || execution->loop_thread_started) return TURBO_OK;
  coro_context_set_persistent(execution->context, 1);
  if (turbo_thread_create(&execution->loop_thread, tf_coronet_execution_loop, execution) !=
      TURBO_OK) {
    coro_context_set_persistent(execution->context, 0);
    return TURBO_EIO;
  }
  execution->loop_thread_started = 1;
  return TURBO_OK;
}

int tf_coronet_execution_post(tf_coronet_execution_t *execution, coro_post_fn fn, void *arg1,
                              void *arg2) {
  if (!execution || !execution->context || !fn) return TURBO_EINVAL;
  if (coro_context_current() == execution->context) {
    fn(arg1, arg2);
    return TURBO_OK;
  }
  return coro_post(execution->context, fn, arg1, arg2);
}

int tf_coronet_execution_call(tf_coronet_execution_t *execution, tf_coronet_execution_call_fn fn,
                              void *arg, uint64_t timeout_ns) {
  tf_coronet_call_t *call;
  int rc;
  if (!execution || !execution->context || !fn || timeout_ns == 0u) return TURBO_EINVAL;
  if (coro_context_current() == execution->context) return fn(arg);
  call = (tf_coronet_call_t *)calloc(1, sizeof(*call));
  if (!call) return TURBO_ENOMEM;
  call->fn = fn;
  call->arg = arg;
  call->status = TURBO_EALREADY;
  atomic_init(&call->refs, 2);
  turbo_mutex_init(&call->mutex);
  turbo_cond_init(&call->cond);
  rc = coro_post(execution->context, tf_coronet_call_run, call, NULL);
  if (rc != TURBO_OK) {
    tf_coronet_call_release(call);
    tf_coronet_call_release(call);
    return rc;
  }
  turbo_mutex_lock(&call->mutex);
  while (!call->done && !call->started) {
    if (turbo_cond_timedwait(&call->cond, &call->mutex, timeout_ns) != TURBO_OK) break;
  }
  if (!call->done && !call->started) {
    call->canceled = 1;
    rc = TURBO_ETIMEDOUT;
  } else {
    while (!call->done)
      turbo_cond_wait(&call->cond, &call->mutex);
    rc = call->status;
  }
  turbo_mutex_unlock(&call->mutex);
  tf_coronet_call_release(call);
  return rc;
}

void tf_coronet_execution_stop(tf_coronet_execution_t *execution) {
  if (!execution || !execution->context || !execution->drives_context) return;
  coro_context_set_persistent(execution->context, 0);
  if (execution->loop_thread_started) {
    if (coro_post(execution->context, tf_coronet_execution_stop_post, execution->context, NULL) !=
        TURBO_OK) {
      coro_context_stop(execution->context);
    }
    (void)turbo_thread_join(&execution->loop_thread);
    execution->loop_thread_started = 0;
  }
}

void tf_coronet_execution_destroy(tf_coronet_execution_t *execution) {
  if (!execution) return;
  tf_coronet_execution_stop(execution);
  if (execution->owns_context && execution->context) coro_context_destroy(execution->context);
  memset(execution, 0, sizeof(*execution));
}
