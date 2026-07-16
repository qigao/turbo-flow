#include "flow_event_time_watermark.h"

#include "platform.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>

struct tf_event_time_watermark_owner_s {
  turbo_flow_t *flow;
  turbo_flow_event_time_window_store_t *store;
  uint64_t interval_ms;
  uint64_t max_out_of_orderness_ns;
  turbo_timer_t *timer;
  turbo_mutex_t tick_mutex;
  int periodic_started;
  atomic_int timer_callback_active;
  atomic_int state;
  atomic_int last_status;
  atomic_int event_time_observed;
  atomic_int watermark_initialized;
  atomic_uint_fast64_t max_observed_event_time_ns;
  atomic_uint_fast64_t last_attempted_watermark_ns;
  atomic_uint_fast64_t last_successful_watermark_ns;
  atomic_uint_fast64_t observed_event_count;
  atomic_uint_fast64_t advance_attempt_count;
  atomic_uint_fast64_t advance_success_count;
  atomic_uint_fast64_t closed_window_count;
};

static void tf_event_time_watermark_mark_failed(tf_event_time_watermark_owner_t *owner,
                                                int status) {
  atomic_store_explicit(&owner->last_status, status, memory_order_release);
  atomic_store_explicit(&owner->state, TF_EVENT_TIME_WATERMARK_FAILED, memory_order_release);
}

int tf_event_time_watermark_owner_tick(tf_event_time_watermark_owner_t *owner) {
  uint64_t max_event_time_ns;
  uint64_t watermark_ns;
  uint64_t last_successful_watermark_ns;
  size_t closed_windows = 0u;
  int initialized;
  int state;
  int rc;

  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->tick_mutex);
  state = atomic_load_explicit(&owner->state, memory_order_acquire);
  if (state == TF_EVENT_TIME_WATERMARK_FAILED) {
    rc = atomic_load_explicit(&owner->last_status, memory_order_acquire);
    turbo_mutex_unlock(&owner->tick_mutex);
    return rc;
  }
  if (!atomic_load_explicit(&owner->event_time_observed, memory_order_acquire)) {
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_ENOENT;
  }

  max_event_time_ns =
      atomic_load_explicit(&owner->max_observed_event_time_ns, memory_order_acquire);
  watermark_ns = max_event_time_ns > owner->max_out_of_orderness_ns
                     ? max_event_time_ns - owner->max_out_of_orderness_ns
                     : 0u;
  initialized = atomic_load_explicit(&owner->watermark_initialized, memory_order_acquire);
  last_successful_watermark_ns =
      atomic_load_explicit(&owner->last_successful_watermark_ns, memory_order_acquire);
  if (initialized && watermark_ns <= last_successful_watermark_ns) {
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_OK;
  }

  atomic_store_explicit(&owner->last_attempted_watermark_ns, watermark_ns, memory_order_release);
  atomic_fetch_add_explicit(&owner->advance_attempt_count, 1u, memory_order_relaxed);
  rc = turbo_flow_advance_event_time_watermark(owner->flow, owner->store, watermark_ns,
                                               &closed_windows);
  if (closed_windows > 0u) {
    atomic_fetch_add_explicit(&owner->closed_window_count, (uint64_t)closed_windows,
                              memory_order_relaxed);
  }
  if (rc != TURBO_OK) {
    tf_event_time_watermark_mark_failed(owner, rc);
    turbo_mutex_unlock(&owner->tick_mutex);
    return rc;
  }
  atomic_store_explicit(&owner->last_successful_watermark_ns, watermark_ns, memory_order_release);
  atomic_store_explicit(&owner->watermark_initialized, 1, memory_order_release);
  atomic_fetch_add_explicit(&owner->advance_success_count, 1u, memory_order_relaxed);
  atomic_store_explicit(&owner->last_status, TURBO_OK, memory_order_release);
  turbo_mutex_unlock(&owner->tick_mutex);
  return TURBO_OK;
}

static void tf_event_time_watermark_timer_cb(turbo_timer_t *timer) {
  tf_event_time_watermark_owner_t *owner =
      (tf_event_time_watermark_owner_t *)turbo_timer_get_data(timer);
  if (!owner) return;

  if (atomic_load_explicit(&owner->state, memory_order_acquire) !=
      TF_EVENT_TIME_WATERMARK_RUNNING) {
    return;
  }
  if (atomic_exchange_explicit(&owner->timer_callback_active, 1, memory_order_acq_rel)) {
    return;
  }
  if (atomic_load_explicit(&owner->state, memory_order_acquire) ==
      TF_EVENT_TIME_WATERMARK_RUNNING) {
    (void)tf_event_time_watermark_owner_tick(owner);
  }
  atomic_store_explicit(&owner->timer_callback_active, 0, memory_order_release);
}

tf_event_time_watermark_owner_t *
tf_event_time_watermark_owner_create(const tf_event_time_watermark_config_t *config) {
  tf_event_time_watermark_owner_t *owner;
  if (!config || config->size != sizeof(*config) || !config->flow || !config->store ||
      config->interval_ms == 0u || config->interval_ms > TF_EVENT_TIME_WATERMARK_INTERVAL_MAX_MS) {
    return NULL;
  }
  owner = (tf_event_time_watermark_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return NULL;
  owner->flow = config->flow;
  owner->store = config->store;
  owner->interval_ms = config->interval_ms;
  owner->max_out_of_orderness_ns = config->max_out_of_orderness_ns;
  turbo_mutex_init(&owner->tick_mutex);
  owner->timer = turbo_timer_create(NULL);
  if (!owner->timer) {
    turbo_mutex_destroy(&owner->tick_mutex);
    free(owner);
    return NULL;
  }
  turbo_timer_set_data(owner->timer, owner);
  atomic_init(&owner->state, TF_EVENT_TIME_WATERMARK_STOPPED);
  atomic_init(&owner->last_status, TURBO_OK);
  atomic_init(&owner->timer_callback_active, 0);
  atomic_init(&owner->event_time_observed, 0);
  atomic_init(&owner->watermark_initialized, 0);
  atomic_init(&owner->max_observed_event_time_ns, 0u);
  atomic_init(&owner->last_attempted_watermark_ns, 0u);
  atomic_init(&owner->last_successful_watermark_ns, 0u);
  atomic_init(&owner->observed_event_count, 0u);
  atomic_init(&owner->advance_attempt_count, 0u);
  atomic_init(&owner->advance_success_count, 0u);
  atomic_init(&owner->closed_window_count, 0u);
  return owner;
}

void tf_event_time_watermark_owner_destroy(tf_event_time_watermark_owner_t *owner) {
  if (!owner) return;
  (void)tf_event_time_watermark_owner_stop(owner);
  turbo_mutex_destroy(&owner->tick_mutex);
  free(owner);
}

int tf_event_time_watermark_owner_start(tf_event_time_watermark_owner_t *owner) {
  int state;
  int rc;
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->tick_mutex);
  state = atomic_load_explicit(&owner->state, memory_order_acquire);
  if (state == TF_EVENT_TIME_WATERMARK_RUNNING || owner->periodic_started) {
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_EALREADY;
  }
  if (state == TF_EVENT_TIME_WATERMARK_FAILED) {
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_EBUSY;
  }
  if (!owner->timer) {
    owner->timer = turbo_timer_create(NULL);
    if (!owner->timer) {
      turbo_mutex_unlock(&owner->tick_mutex);
      return TURBO_ENOMEM;
    }
    turbo_timer_set_data(owner->timer, owner);
  }
  atomic_store_explicit(&owner->last_status, TURBO_OK, memory_order_release);
  atomic_store_explicit(&owner->state, TF_EVENT_TIME_WATERMARK_RUNNING, memory_order_release);
  owner->periodic_started = 1;
  rc = turbo_timer_start(owner->timer, tf_event_time_watermark_timer_cb, owner->interval_ms,
                         owner->interval_ms);
  if (rc != TURBO_OK) {
    owner->periodic_started = 0;
    atomic_store_explicit(&owner->state, TF_EVENT_TIME_WATERMARK_STOPPED, memory_order_release);
    atomic_store_explicit(&owner->last_status, TURBO_EIO, memory_order_release);
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_EIO;
  }
  turbo_mutex_unlock(&owner->tick_mutex);
  return TURBO_OK;
}

int tf_event_time_watermark_owner_stop(tf_event_time_watermark_owner_t *owner) {
  turbo_timer_t *timer;
  int state;
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->tick_mutex);
  state = atomic_load_explicit(&owner->state, memory_order_acquire);
  if (state == TF_EVENT_TIME_WATERMARK_RUNNING) {
    atomic_store_explicit(&owner->state, TF_EVENT_TIME_WATERMARK_STOPPED, memory_order_release);
  }
  owner->periodic_started = 0;
  timer = owner->timer;
  owner->timer = NULL;
  turbo_mutex_unlock(&owner->tick_mutex);
  turbo_timer_destroy(timer);
  return TURBO_OK;
}

int tf_event_time_watermark_owner_reset(tf_event_time_watermark_owner_t *owner) {
  int state;
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->tick_mutex);
  if (owner->periodic_started) {
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_EBUSY;
  }
  state = atomic_load_explicit(&owner->state, memory_order_acquire);
  if (state == TF_EVENT_TIME_WATERMARK_RUNNING) {
    turbo_mutex_unlock(&owner->tick_mutex);
    return TURBO_EBUSY;
  }
  atomic_store_explicit(&owner->last_status, TURBO_OK, memory_order_release);
  atomic_store_explicit(&owner->state, TF_EVENT_TIME_WATERMARK_STOPPED, memory_order_release);
  turbo_mutex_unlock(&owner->tick_mutex);
  return TURBO_OK;
}

int tf_event_time_watermark_owner_observe(tf_event_time_watermark_owner_t *owner,
                                          uint64_t event_time_ns) {
  uint64_t current;
  if (!owner) return TURBO_EINVAL;
  current = atomic_load_explicit(&owner->max_observed_event_time_ns, memory_order_relaxed);
  while (event_time_ns > current && !atomic_compare_exchange_weak_explicit(
                                        &owner->max_observed_event_time_ns, &current, event_time_ns,
                                        memory_order_release, memory_order_relaxed)) {
  }
  atomic_fetch_add_explicit(&owner->observed_event_count, 1u, memory_order_relaxed);
  atomic_store_explicit(&owner->event_time_observed, 1, memory_order_release);
  return TURBO_OK;
}

int tf_event_time_watermark_owner_publish(tf_event_time_watermark_owner_t *owner,
                                          const char *source_name, const turbo_flow_msg_t *msg) {
  int rc;
  if (!owner || !source_name || !msg) return TURBO_EINVAL;
  if (atomic_load_explicit(&owner->state, memory_order_acquire) == TF_EVENT_TIME_WATERMARK_FAILED) {
    return TURBO_EBUSY;
  }
  rc = turbo_flow_publish(owner->flow, source_name, msg);
  if (rc != TURBO_OK) return rc;
  return tf_event_time_watermark_owner_observe(owner, msg->ts_ns);
}

int tf_event_time_watermark_owner_snapshot(const tf_event_time_watermark_owner_t *owner,
                                           tf_event_time_watermark_snapshot_t *out) {
  if (!owner || !out || out->size != sizeof(*out)) return TURBO_EINVAL;
  out->state =
      (tf_event_time_watermark_state_t)atomic_load_explicit(&owner->state, memory_order_acquire);
  out->last_status = atomic_load_explicit(&owner->last_status, memory_order_acquire);
  out->event_time_observed =
      atomic_load_explicit(&owner->event_time_observed, memory_order_acquire);
  out->watermark_initialized =
      atomic_load_explicit(&owner->watermark_initialized, memory_order_acquire);
  out->max_observed_event_time_ns =
      atomic_load_explicit(&owner->max_observed_event_time_ns, memory_order_acquire);
  out->last_attempted_watermark_ns =
      atomic_load_explicit(&owner->last_attempted_watermark_ns, memory_order_acquire);
  out->last_successful_watermark_ns =
      atomic_load_explicit(&owner->last_successful_watermark_ns, memory_order_acquire);
  out->observed_event_count =
      atomic_load_explicit(&owner->observed_event_count, memory_order_relaxed);
  out->advance_attempt_count =
      atomic_load_explicit(&owner->advance_attempt_count, memory_order_relaxed);
  out->advance_success_count =
      atomic_load_explicit(&owner->advance_success_count, memory_order_relaxed);
  out->closed_window_count =
      atomic_load_explicit(&owner->closed_window_count, memory_order_relaxed);
  return TURBO_OK;
}
