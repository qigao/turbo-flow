#include "flow_internal.h"

#include <string.h>

#define FLOW_NANOSECONDS_PER_MILLISECOND UINT64_C(1000000)

static SALTS_THREAD_LOCAL flow_execution_task_t *flow_current_task;

static int flow_execution_task_cancel_status(flow_execution_task_t *task) {
  uint64_t deadline_at_ns;
  if (!task) return SALTS_EINVAL;
  if (atomic_load_explicit(&task->deadline_expired, memory_order_acquire)) {
    return SALTS_ETIMEDOUT;
  }
  deadline_at_ns = atomic_load_explicit(&task->deadline_at_ns, memory_order_acquire);
  if (deadline_at_ns != 0u && salts_hrtime() >= deadline_at_ns) {
    atomic_store_explicit(&task->deadline_expired, 1, memory_order_release);
    atomic_store_explicit(&task->cancel_requested, 1, memory_order_release);
    return SALTS_ETIMEDOUT;
  }
  if (atomic_load_explicit(&task->cancel_requested, memory_order_acquire)) {
    return atomic_load_explicit(&task->deadline_expired, memory_order_acquire) ? SALTS_ETIMEDOUT
                                                                               : SALTS_ECANCELED;
  }
  return SALTS_OK;
}

static int flow_execution_task_complete(flow_execution_task_t *task, int status) {
  int completed = 0;
  salts_mutex_lock(&task->mutex);
  if (atomic_load_explicit(&task->state, memory_order_acquire) != FLOW_EXECUTION_COMPLETED &&
      atomic_load_explicit(&task->state, memory_order_acquire) != FLOW_EXECUTION_CANCELED) {
    task->status = status;
    task->completion.status = status;
    atomic_store_explicit(&task->state,
                          status == SALTS_ECANCELED ? FLOW_EXECUTION_CANCELED
                                                    : FLOW_EXECUTION_COMPLETED,
                          memory_order_release);
    salts_cond_broadcast(&task->cond);
    completed = 1;
  }
  salts_mutex_unlock(&task->mutex);
  return completed;
}

int flow_execution_task_init(flow_execution_task_t *task, flow_execution_backend_t backend,
                             turbo_flow_stage_fn fn, void *ctx, turbo_flow_msg_t *msg,
                             const flow_stage_completion_t *completion, uint64_t deadline_ms) {
  int rc;
  if (!task || !fn || !msg || !completion || backend < FLOW_EXECUTION_THREAD ||
      backend > FLOW_EXECUTION_DISRUPTOR) {
    return SALTS_EINVAL;
  }

  memset(task, 0, sizeof(*task));
  task->backend = backend;
  task->fn = fn;
  task->ctx = ctx;
  task->completion = *completion;
  task->deadline_ms = deadline_ms;
  task->status = SALTS_EALREADY;
  turbo_flow_msg_init(&task->msg);
  salts_mutex_init(&task->mutex);
  salts_cond_init(&task->cond);
  task->sync_initialized = 1;
  atomic_init(&task->state, FLOW_EXECUTION_NEW);
  atomic_init(&task->cancel_requested, 0);
  atomic_init(&task->deadline_expired, 0);
  atomic_init(&task->accounting_done, 0);
  atomic_init(&task->deadline_at_ns, 0u);
  task->completion.entry.completion_handle = &task->completion;
  task->completion.entry.cancel_handle = &task->cancel_requested;
  task->completion.entry.deadline_at_ns = 0u;
  if (flow_msg_transport_context_is_borrowed(msg)) {
    flow_execution_task_cleanup(task);
    return SALTS_EINVAL;
  }
  rc = turbo_flow_msg_move(&task->msg, msg);
  if (rc != SALTS_OK) {
    flow_execution_task_cleanup(task);
    return rc;
  }
  atomic_store_explicit(&task->state, FLOW_EXECUTION_ACCEPTED, memory_order_release);
  return SALTS_OK;
}

void flow_execution_task_run(flow_execution_task_t *task) {
  int expected = FLOW_EXECUTION_ACCEPTED;
  int status;
  flow_stage_completion_t *previous_settlement;
  if (!task) return;
  if (!atomic_compare_exchange_strong_explicit(&task->state, &expected, FLOW_EXECUTION_RUNNING,
                                               memory_order_acq_rel, memory_order_acquire)) {
    return;
  }
  if (task->deadline_ms != 0u) {
    salts_mutex_lock(&task->mutex);
    atomic_store_explicit(&task->deadline_at_ns,
                          salts_hrtime() + task->deadline_ms * FLOW_NANOSECONDS_PER_MILLISECOND,
                          memory_order_release);
    task->completion.entry.deadline_at_ns =
        atomic_load_explicit(&task->deadline_at_ns, memory_order_acquire);
    salts_cond_broadcast(&task->cond);
    salts_mutex_unlock(&task->mutex);
  }
  status = flow_execution_task_cancel_status(task);
  if (status != SALTS_OK) {
    flow_execution_task_complete(task, status);
    return;
  }

  flow_current_task = task;
  previous_settlement = flow_settlement_scope_enter(&task->completion);
  status = task->fn(&task->msg, task->ctx);
  flow_settlement_scope_leave(previous_settlement);
  flow_current_task = NULL;
  {
    int cancel_status = flow_execution_task_cancel_status(task);
    if (cancel_status == SALTS_ETIMEDOUT || (status == SALTS_OK && cancel_status != SALTS_OK)) {
      status = cancel_status;
    }
  }
  flow_execution_task_complete(task, status);
}

void flow_execution_task_fail(flow_execution_task_t *task, int status) {
  if (!task || status == SALTS_OK) return;
  (void)flow_execution_task_complete(task, status);
}

int flow_execution_task_wait(flow_execution_task_t *task, turbo_flow_msg_t *msg,
                             flow_stage_completion_t *completion) {
  int rc;
  int status;
  if (!task || !msg || !completion || !task->sync_initialized) return SALTS_EINVAL;

  salts_mutex_lock(&task->mutex);
  while (atomic_load_explicit(&task->state, memory_order_acquire) != FLOW_EXECUTION_COMPLETED &&
         atomic_load_explicit(&task->state, memory_order_acquire) != FLOW_EXECUTION_CANCELED) {
    uint64_t deadline_at_ns = atomic_load_explicit(&task->deadline_at_ns, memory_order_acquire);
    if (deadline_at_ns != 0u &&
        !atomic_load_explicit(&task->deadline_expired, memory_order_acquire)) {
      uint64_t now = salts_hrtime();
      if (now >= deadline_at_ns ||
          salts_cond_timedwait(&task->cond, &task->mutex, deadline_at_ns - now) != 0) {
        atomic_store_explicit(&task->deadline_expired, 1, memory_order_release);
        atomic_store_explicit(&task->cancel_requested, 1, memory_order_release);
      }
    } else {
      salts_cond_wait(&task->cond, &task->mutex);
    }
  }
  status = task->status;
  *completion = task->completion;
  completion->entry.completion_handle = completion;
  completion->entry.cancel_handle = NULL;
  salts_mutex_unlock(&task->mutex);

  rc = turbo_flow_msg_move(msg, &task->msg);
  return rc == SALTS_OK ? status : rc;
}

void flow_execution_task_mark_accounting_done(flow_execution_task_t *task) {
  if (!task || !task->sync_initialized) return;
  salts_mutex_lock(&task->mutex);
  atomic_store_explicit(&task->accounting_done, 1, memory_order_release);
  salts_cond_broadcast(&task->cond);
  salts_mutex_unlock(&task->mutex);
}

void flow_execution_task_wait_accounting(flow_execution_task_t *task) {
  if (!task || !task->sync_initialized) return;
  salts_mutex_lock(&task->mutex);
  while (!atomic_load_explicit(&task->accounting_done, memory_order_acquire))
    salts_cond_wait(&task->cond, &task->mutex);
  salts_mutex_unlock(&task->mutex);
}

int flow_execution_task_abort(flow_execution_task_t *task) {
  int expected;
  if (!task || !task->sync_initialized) return SALTS_EINVAL;
  atomic_store_explicit(&task->cancel_requested, 1, memory_order_release);
  expected = FLOW_EXECUTION_ACCEPTED;
  if (atomic_compare_exchange_strong_explicit(&task->state, &expected, FLOW_EXECUTION_RUNNING,
                                              memory_order_acq_rel, memory_order_acquire)) {
    flow_execution_task_complete(task, SALTS_ECANCELED);
  }
  return SALTS_OK;
}

void flow_execution_task_discard(flow_execution_task_t *task) {
  if (!task || !task->sync_initialized) return;
  atomic_store_explicit(&task->cancel_requested, 1, memory_order_release);
  flow_execution_task_complete(task, SALTS_ECANCELED);
}

int flow_execution_yield(void) {
  flow_execution_task_t *task = flow_current_task;
  int status;
  if (!task) return SALTS_EINVAL;
  status = flow_execution_task_cancel_status(task);
  if (status != SALTS_OK) return status;
  if (task->backend == FLOW_EXECUTION_CORO) {
    if (coro_yield() != 0) return SALTS_EINVAL;
  } else {
    salts_thread_yield();
  }
  return flow_execution_task_cancel_status(task);
}

int flow_execution_suspend_for_io(void) {
  coro_t *running;

  if (!flow_current_task || flow_current_task->backend != FLOW_EXECUTION_CORO) {
    return SALTS_EINVAL;
  }
  running = coro_running();
  if (!running) return SALTS_EINVAL;

  /* Salts coroutine state is process-module local on static builds. Keep all
   * scheduler mutation behind the TurboFlow DLL boundary that owns the task. */
  coro_set_waiting_for_io(running, 1);
  return flow_execution_yield();
}

int flow_execution_cancel_requested(void) {
  return flow_current_task ? flow_execution_task_cancel_status(flow_current_task) != SALTS_OK : 0;
}

int turbo_flow_execution_yield(void) { return flow_execution_yield(); }

int turbo_flow_execution_abort(void) {
  return flow_current_task ? flow_execution_task_abort(flow_current_task) : SALTS_EINVAL;
}

int turbo_flow_execution_cancel_requested(void) { return flow_execution_cancel_requested(); }

flow_execution_state_t flow_execution_task_state(const flow_execution_task_t *task) {
  return task ? (flow_execution_state_t)atomic_load_explicit(&task->state, memory_order_acquire)
              : FLOW_EXECUTION_NEW;
}

void flow_execution_task_cleanup(flow_execution_task_t *task) {
  if (!task) return;
  turbo_flow_msg_cleanup(&task->msg);
  if (task->sync_initialized) {
    salts_cond_destroy(&task->cond);
    salts_mutex_destroy(&task->mutex);
  }
  memset(task, 0, sizeof(*task));
}
