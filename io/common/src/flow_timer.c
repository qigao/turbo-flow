#include "flow_timer.h"

#define FLOW_TIMER_MAX_WAIT_SLICE_NS (UINT64_C(1000000000))

int tf_timer_init(tf_timer_t *timer) {
  if (!timer) return TURBO_EINVAL;
  atomic_init(&timer->stopped, 0);
  turbo_mutex_init(&timer->mutex);
  turbo_cond_init(&timer->cond);
  return TURBO_OK;
}

void tf_timer_destroy(tf_timer_t *timer) {
  if (!timer) return;
  turbo_cond_destroy(&timer->cond);
  turbo_mutex_destroy(&timer->mutex);
}

void tf_timer_signal(tf_timer_t *timer) {
  if (!timer) return;
  turbo_mutex_lock(&timer->mutex);
  turbo_cond_broadcast(&timer->cond);
  turbo_mutex_unlock(&timer->mutex);
}

void tf_timer_stop(tf_timer_t *timer) {
  if (!timer) return;
  turbo_mutex_lock(&timer->mutex);
  atomic_store_explicit(&timer->stopped, 1, memory_order_release);
  turbo_cond_broadcast(&timer->cond);
  turbo_mutex_unlock(&timer->mutex);
}

void tf_timer_reset(tf_timer_t *timer) {
  if (!timer) return;
  turbo_mutex_lock(&timer->mutex);
  atomic_store_explicit(&timer->stopped, 0, memory_order_release);
  turbo_mutex_unlock(&timer->mutex);
}

int tf_timer_wait_for_ms(tf_timer_t *timer, uint64_t timeout_ms) {
  uint64_t timeout_ns;
  if (!timer) return TURBO_EINVAL;
  if (timeout_ms == 0) {
    turbo_mutex_lock(&timer->mutex);
    if (atomic_load_explicit(&timer->stopped, memory_order_acquire)) {
      turbo_mutex_unlock(&timer->mutex);
      return TURBO_ESHUTDOWN;
    }
    turbo_mutex_unlock(&timer->mutex);
    return TURBO_ETIMEDOUT;
  }
  if (timeout_ms > (UINT64_MAX / UINT64_C(1000000))) {
    timeout_ns = UINT64_MAX;
  } else {
    timeout_ns = timeout_ms * UINT64_C(1000000);
  }

  turbo_mutex_lock(&timer->mutex);
  if (atomic_load_explicit(&timer->stopped, memory_order_acquire)) {
    turbo_mutex_unlock(&timer->mutex);
    return TURBO_ESHUTDOWN;
  }
  for (uint64_t remaining = timeout_ns;;) {
    uint64_t wait_ns =
        remaining < FLOW_TIMER_MAX_WAIT_SLICE_NS ? remaining : FLOW_TIMER_MAX_WAIT_SLICE_NS;
    int rc = turbo_cond_timedwait(&timer->cond, &timer->mutex, wait_ns);
    if (atomic_load_explicit(&timer->stopped, memory_order_acquire)) {
      turbo_mutex_unlock(&timer->mutex);
      return TURBO_ESHUTDOWN;
    }
    if (rc != -ETIMEDOUT) {
      turbo_mutex_unlock(&timer->mutex);
      return TURBO_OK;
    }
    if (remaining <= wait_ns) {
      turbo_mutex_unlock(&timer->mutex);
      return TURBO_ETIMEDOUT;
    }
    remaining -= wait_ns;
  }
}

int tf_timer_wait_until_ns(tf_timer_t *timer, uint64_t deadline_ns) {
  uint64_t now;
  if (!timer) return TURBO_EINVAL;
  now = turbo_hrtime();
  if (deadline_ns <= now) return TURBO_ETIMEDOUT;
  if (deadline_ns - now > (UINT64_MAX / UINT64_C(1000000))) {
    return tf_timer_wait_for_ms(timer, UINT64_MAX);
  }
  return tf_timer_wait_for_ms(timer, (deadline_ns - now + UINT64_C(999999)) / UINT64_C(1000000));
}
