#ifndef FLOW_EVENT_TIME_WATERMARK_H
#define FLOW_EVENT_TIME_WATERMARK_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TF_EVENT_TIME_WATERMARK_INTERVAL_MAX_MS UINT64_C(3600000)

typedef struct tf_event_time_watermark_owner_s tf_event_time_watermark_owner_t;

typedef enum tf_event_time_watermark_state_e {
  TF_EVENT_TIME_WATERMARK_STOPPED = 0,
  TF_EVENT_TIME_WATERMARK_RUNNING,
  TF_EVENT_TIME_WATERMARK_FAILED
} tf_event_time_watermark_state_t;

typedef struct tf_event_time_watermark_config_s {
  size_t size;
  /** Borrowed for the owner lifetime; lifecycle commands remain caller-serialized. */
  turbo_flow_t *flow;
  /** Borrowed for the owner lifetime and bound to one event-time provider in flow. */
  turbo_flow_event_time_window_store_t *store;
  /** Periodic advance interval in [1, TF_EVENT_TIME_WATERMARK_INTERVAL_MAX_MS]. */
  uint64_t interval_ms;
  /** The generated watermark is max_observed_event_time_ns minus this bound, saturated at zero. */
  uint64_t max_out_of_orderness_ns;
} tf_event_time_watermark_config_t;

#define TF_EVENT_TIME_WATERMARK_CONFIG_INIT                                                        \
  {sizeof(tf_event_time_watermark_config_t), NULL, NULL, 1000u, 0u}

typedef struct tf_event_time_watermark_snapshot_s {
  size_t size;
  tf_event_time_watermark_state_t state;
  int last_status;
  int event_time_observed;
  int watermark_initialized;
  uint64_t max_observed_event_time_ns;
  uint64_t last_attempted_watermark_ns;
  uint64_t last_successful_watermark_ns;
  uint64_t observed_event_count;
  uint64_t advance_attempt_count;
  uint64_t advance_success_count;
  uint64_t closed_window_count;
} tf_event_time_watermark_snapshot_t;

#define TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT                                                      \
  {sizeof(tf_event_time_watermark_snapshot_t),                                                     \
   TF_EVENT_TIME_WATERMARK_STOPPED,                                                                \
   TURBO_OK,                                                                                       \
   0,                                                                                              \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/** Allocate one owner. The flow and store must outlive it. */
tf_event_time_watermark_owner_t *
tf_event_time_watermark_owner_create(const tf_event_time_watermark_config_t *config);

/** Stop, join, and release an owner. Do not call concurrently with another lifecycle command. */
void tf_event_time_watermark_owner_destroy(tf_event_time_watermark_owner_t *owner);

/**
 * Start periodic advancement. Timer callbacks never advance concurrently; expiries observed while
 * a previous callback is still active are coalesced. The bound flow/store must be started.
 */
int tf_event_time_watermark_owner_start(tf_event_time_watermark_owner_t *owner);

/** Disarm the periodic timer and wait for every native timer callback to finish. */
int tf_event_time_watermark_owner_stop(tf_event_time_watermark_owner_t *owner);

/**
 * Clear FAILED after stop while retaining observations and the last successful watermark.
 * This permits an equal-watermark retry without discarding window state.
 */
int tf_event_time_watermark_owner_reset(tf_event_time_watermark_owner_t *owner);

/**
 * Record an accepted event timestamp using an allocation-free atomic maximum update.
 * Call this only after the corresponding event has been accepted by the bound flow. Observation
 * remains available in FAILED so an external admission boundary never loses an accepted timestamp.
 */
int tf_event_time_watermark_owner_observe(tf_event_time_watermark_owner_t *owner,
                                          uint64_t event_time_ns);

/**
 * Publish synchronously and observe msg->ts_ns only when publication succeeds.
 * A previously FAILED owner rejects before publication; a concurrent failure after admission does
 * not turn a successfully published message into a caller-visible failure.
 */
int tf_event_time_watermark_owner_publish(tf_event_time_watermark_owner_t *owner,
                                          const char *source_name, const turbo_flow_msg_t *msg);

/**
 * Synchronously perform one advance from the current maximum observation.
 * Returns TURBO_ENOENT before the first observation. Calls are serialized with periodic ticks.
 */
int tf_event_time_watermark_owner_tick(tf_event_time_watermark_owner_t *owner);

/** Read a lock-free diagnostic snapshot. Individual counters are monotonic. */
int tf_event_time_watermark_owner_snapshot(const tf_event_time_watermark_owner_t *owner,
                                           tf_event_time_watermark_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOW_EVENT_TIME_WATERMARK_H */
