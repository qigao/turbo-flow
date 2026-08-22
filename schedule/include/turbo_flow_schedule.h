#ifndef TURBO_FLOW_SCHEDULE_H
#define TURBO_FLOW_SCHEDULE_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SCHEDULE_MAX_PAYLOAD_SIZE 65536u
#define TURBO_FLOW_SCHEDULE_MAX_CATCH_UP 1024u
#define TURBO_FLOW_SCHEDULE_MAX_DELAY_MS (UINT64_MAX / UINT64_C(1000000))

typedef struct turbo_flow_schedule_s turbo_flow_schedule_t;

typedef enum turbo_flow_schedule_mode_e {
  TURBO_FLOW_SCHEDULE_INTERVAL = 1,
  TURBO_FLOW_SCHEDULE_ONE_SHOT,
  TURBO_FLOW_SCHEDULE_CRON
} turbo_flow_schedule_mode_t;

typedef struct turbo_flow_schedule_config_s {
  turbo_flow_schedule_mode_t mode;
  /** Interval period or one-shot delay. Must be non-zero for those modes. */
  uint64_t delay_ms;
  /** Zero means unbounded interval repetition; ignored by one-shot and cron. */
  uint64_t repeat_limit;
  /** Five-field local wall-clock expression, required for cron mode. */
  const char *cron_expression;
  /** Additional due cron ticks allowed after the first one per wake/advance. */
  uint32_t catch_up_limit;
  const char *payload;
  size_t payload_len;
  /** Cron only: do not arm a runtime timer; host advances time explicitly. */
  int manual_clock;
} turbo_flow_schedule_config_t;

typedef struct turbo_flow_schedule_snapshot_s {
  uint64_t fired;
  /** Number of advances where additional due ticks were truncated. */
  uint64_t catch_up_truncations;
  int last_status;
} turbo_flow_schedule_snapshot_t;

/** Register one source adapter and optionally return its borrowed runtime handle. */
TURBO_FLOW_C_API int turbo_flow_schedule_register_adapter(turbo_flow_t *flow, const char *name,
                                                   const turbo_flow_schedule_config_t *config,
                                                   turbo_flow_schedule_t **out_schedule);

/** Deterministically advance a manual cron source to a local wall-clock time. */
TURBO_FLOW_C_API int turbo_flow_schedule_advance(turbo_flow_schedule_t *schedule, time_t now);
TURBO_FLOW_C_API int turbo_flow_schedule_snapshot(const turbo_flow_schedule_t *schedule,
                                           turbo_flow_schedule_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_SCHEDULE_H */
