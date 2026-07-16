#include "flow_io_policy.h"

#include "platform.h"
#include "turbo_error.h"

#include <limits.h>
#include <string.h>

#define TF_IO_NANOSECONDS_PER_MILLISECOND UINT64_C(1000000)

static int tf_io_budget_config_valid(const tf_io_budget_config_t *config) {
  if (!config ||
      (config->admission != TF_IO_ADMISSION_FAIL && config->admission != TF_IO_ADMISSION_BLOCK)) {
    return 0;
  }
  return config->admission == TF_IO_ADMISSION_BLOCK || config->wait_timeout_ms == 0u;
}

static int tf_io_budget_fits(const tf_io_budget_t *budget, size_t bytes) {
  if (budget->messages == SIZE_MAX || budget->bytes > SIZE_MAX - bytes) return 0;
  if (budget->max_messages != 0 && budget->messages >= budget->max_messages) return 0;
  if (budget->max_bytes != 0 && bytes > budget->max_bytes - budget->bytes) return 0;
  return 1;
}

static uint64_t tf_io_deadline_ns(uint64_t timeout_ms) {
  uint64_t now = turbo_hrtime();
  if (timeout_ms > (UINT64_MAX - now) / TF_IO_NANOSECONDS_PER_MILLISECOND) return UINT64_MAX;
  return now + timeout_ms * TF_IO_NANOSECONDS_PER_MILLISECOND;
}

static int tf_io_wait_changed(tf_io_budget_t *budget, uint64_t deadline_ns) {
  uint64_t now;
  if (deadline_ns == UINT64_MAX) {
    turbo_cond_wait(&budget->changed, &budget->mutex);
    return TURBO_OK;
  }
  now = turbo_hrtime();
  if (now >= deadline_ns) return TURBO_ETIMEDOUT;
  return turbo_cond_timedwait(&budget->changed, &budget->mutex, deadline_ns - now) == 0
             ? TURBO_OK
             : TURBO_ETIMEDOUT;
}

int tf_io_budget_init(tf_io_budget_t *budget, const tf_io_budget_config_t *config) {
  if (!budget || !tf_io_budget_config_valid(config)) return TURBO_EINVAL;
  memset(budget, 0, sizeof(*budget));
  turbo_mutex_init(&budget->mutex);
  turbo_cond_init(&budget->changed);
  budget->max_messages = config->max_messages;
  budget->max_bytes = config->max_bytes;
  budget->admission = config->admission;
  budget->wait_timeout_ms = config->wait_timeout_ms;
  budget->initialized = 1;
  return TURBO_OK;
}

void tf_io_budget_destroy(tf_io_budget_t *budget) {
  if (!budget || !budget->initialized) return;
  tf_io_budget_close(budget);
  turbo_cond_destroy(&budget->changed);
  turbo_mutex_destroy(&budget->mutex);
  memset(budget, 0, sizeof(*budget));
}

int tf_io_budget_open(tf_io_budget_t *budget) {
  if (!budget || !budget->initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&budget->mutex);
  if (budget->messages != 0 || budget->bytes != 0) {
    turbo_mutex_unlock(&budget->mutex);
    return TURBO_EBUSY;
  }
  budget->accepting = 1;
  turbo_cond_broadcast(&budget->changed);
  turbo_mutex_unlock(&budget->mutex);
  return TURBO_OK;
}

void tf_io_budget_close(tf_io_budget_t *budget) {
  if (!budget || !budget->initialized) return;
  turbo_mutex_lock(&budget->mutex);
  budget->accepting = 0;
  turbo_cond_broadcast(&budget->changed);
  turbo_mutex_unlock(&budget->mutex);
}

int tf_io_budget_acquire(tf_io_budget_t *budget, size_t bytes) {
  uint64_t deadline_ns = UINT64_MAX;
  int rc = TURBO_OK;
  if (!budget || !budget->initialized) return TURBO_EINVAL;
  if (budget->max_bytes != 0 && bytes > budget->max_bytes) return TURBO_ENOSPC;
  if (budget->admission == TF_IO_ADMISSION_BLOCK && budget->wait_timeout_ms != UINT64_MAX) {
    deadline_ns = tf_io_deadline_ns(budget->wait_timeout_ms);
  }
  turbo_mutex_lock(&budget->mutex);
  for (;;) {
    if (!budget->accepting) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    if (tf_io_budget_fits(budget, bytes)) {
      budget->messages += 1u;
      budget->bytes += bytes;
      break;
    }
    if (budget->admission == TF_IO_ADMISSION_FAIL) {
      rc = TURBO_ENOSPC;
      break;
    }
    rc = tf_io_wait_changed(budget, deadline_ns);
    if (rc != TURBO_OK) break;
  }
  turbo_mutex_unlock(&budget->mutex);
  return rc;
}

int tf_io_budget_release(tf_io_budget_t *budget, size_t bytes) {
  if (!budget || !budget->initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&budget->mutex);
  if (budget->messages == 0 || budget->bytes < bytes) {
    turbo_mutex_unlock(&budget->mutex);
    return TURBO_ERANGE;
  }
  budget->messages -= 1u;
  budget->bytes -= bytes;
  turbo_cond_broadcast(&budget->changed);
  turbo_mutex_unlock(&budget->mutex);
  return TURBO_OK;
}

int tf_io_budget_drain(tf_io_budget_t *budget, uint64_t timeout_ms) {
  uint64_t deadline_ns = timeout_ms == UINT64_MAX ? UINT64_MAX : tf_io_deadline_ns(timeout_ms);
  int rc = TURBO_OK;
  if (!budget || !budget->initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&budget->mutex);
  while (budget->messages != 0) {
    if (timeout_ms == 0) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
    rc = tf_io_wait_changed(budget, deadline_ns);
    if (rc != TURBO_OK) break;
  }
  turbo_mutex_unlock(&budget->mutex);
  return rc;
}

int tf_io_budget_snapshot(tf_io_budget_t *budget, tf_io_budget_snapshot_t *out) {
  if (!budget || !budget->initialized || !out) return TURBO_EINVAL;
  turbo_mutex_lock(&budget->mutex);
  out->messages = budget->messages;
  out->bytes = budget->bytes;
  out->max_messages = budget->max_messages;
  out->max_bytes = budget->max_bytes;
  out->accepting = budget->accepting;
  turbo_mutex_unlock(&budget->mutex);
  return TURBO_OK;
}

void tf_round_robin_init(tf_round_robin_t *selector) {
  if (!selector) return;
  atomic_init(&selector->cursor, 0u);
}

int tf_round_robin_next(tf_round_robin_t *selector, size_t candidate_count, size_t *index) {
  uint_fast64_t ticket;
  if (!selector || !index || candidate_count == 0) return TURBO_EINVAL;
  ticket = atomic_fetch_add_explicit(&selector->cursor, 1u, memory_order_relaxed);
  *index = (size_t)(ticket % candidate_count);
  return TURBO_OK;
}
