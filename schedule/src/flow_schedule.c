#include "turbo_flow_schedule.h"

#include "platform.h"
#include "cron/salts_cron.h"
#include "salts_error.h"
#include "salts_str.h"
#include "salts_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static const char *const FLOW_SCHEDULE_MODE_VALUES[] = {"interval", "one_shot", "cron"};
static const uint64_t FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS = (uint64_t)(UINT32_MAX - 1u);
static const uint64_t FLOW_SCHEDULE_CRON_SCAN_MS = UINT64_C(60000);
static const uint64_t FLOW_SCHEDULE_START_POLL_MS = UINT64_C(1);
static const turbo_flow_option_field_t FLOW_SCHEDULE_FIELDS[] = {
    {"mode", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_SCHEDULE_MODE_VALUES,
     3},
    {"delay_ms", TURBO_FLOW_OPTION_DURATION_MS,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, TURBO_FLOW_SCHEDULE_MAX_DELAY_MS,
     NULL, 0},
    {"repeat_limit", TURBO_FLOW_OPTION_U64, 0, 0, 0, NULL, 0},
    {"cron_expression", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"catch_up_limit", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MAX, 0,
     TURBO_FLOW_SCHEDULE_MAX_CATCH_UP, NULL, 0},
    {"payload", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"payload_len", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MAX, 0,
     TURBO_FLOW_SCHEDULE_MAX_PAYLOAD_SIZE, NULL, 0},
    {"manual_clock", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

struct turbo_flow_schedule_s {
  turbo_flow_t *flow;
  tstr source_name;
  tstr payload;
  turbo_flow_schedule_mode_t mode;
  salts_cron_expr_t cron_expr;
  uint64_t delay_ms;
  uint64_t repeat_limit;
  uint32_t catch_up_limit;
  int manual_clock;
  time_t cron_cursor_minute;
  salts_mutex_t mutex;
  salts_timer_t *timer;
  salts_thread_t bootstrap_thread;
  int bootstrap_thread_started;
  atomic_int started;
  atomic_int completed;
  atomic_int timer_callback_active;
  atomic_uint async_inflight;
  atomic_uint_fast64_t run_fired;
  atomic_uint_fast64_t next_due_ms;
  atomic_uint_fast64_t fired;
  atomic_uint_fast64_t catch_up_truncations;
  atomic_int last_status;
};

static time_t flow_schedule_floor_minute(time_t value) {
  time_t remainder = value % 60;
  if (remainder < 0) remainder += 60;
  return value - remainder;
}

static uint64_t flow_schedule_add_delay(uint64_t now_ms, uint64_t delay_ms) {
  return delay_ms > UINT64_MAX - now_ms ? UINT64_MAX : now_ms + delay_ms;
}

static uint64_t flow_schedule_timer_slice(uint64_t delay_ms) {
  uint64_t slice_count;
  if (delay_ms <= FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS) return delay_ms;
  slice_count = delay_ms / FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS;
  if (delay_ms % FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS != 0) slice_count += 1u;
  return delay_ms / slice_count + (delay_ms % slice_count != 0 ? 1u : 0u);
}

static void flow_schedule_publish_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  turbo_flow_schedule_t *schedule = (turbo_flow_schedule_t *)ctx;
  int status = result ? result->status : SALTS_EINVAL;

  if (status == SALTS_OK) {
    uint64_t run_fired = 0u;
    atomic_fetch_add_explicit(&schedule->fired, 1, memory_order_relaxed);
    if (schedule->mode != TURBO_FLOW_SCHEDULE_CRON) {
      run_fired = atomic_fetch_add_explicit(&schedule->run_fired, 1, memory_order_relaxed) + 1u;
      if (schedule->mode == TURBO_FLOW_SCHEDULE_ONE_SHOT ||
          (schedule->repeat_limit > 0u && run_fired >= schedule->repeat_limit)) {
        atomic_store_explicit(&schedule->completed, 1, memory_order_release);
      } else {
        atomic_store_explicit(&schedule->next_due_ms,
                              flow_schedule_add_delay(salts_monotonic_ms(), schedule->delay_ms),
                              memory_order_release);
      }
    }
    if (!atomic_load_explicit(&schedule->completed, memory_order_acquire)) {
      atomic_store_explicit(&schedule->last_status, SALTS_OK, memory_order_release);
    }
  } else {
    atomic_store_explicit(&schedule->last_status, status, memory_order_release);
    atomic_store_explicit(&schedule->completed, 1, memory_order_release);
  }
  atomic_fetch_sub_explicit(&schedule->async_inflight, 1u, memory_order_release);
}

static int flow_schedule_publish_sync(turbo_flow_schedule_t *schedule, time_t scheduled_at) {
  turbo_flow_msg_t msg;
  int rc;
  if (!schedule || !schedule->flow || !schedule->source_name ||
      !atomic_load_explicit(&schedule->started, memory_order_acquire)) {
    return SALTS_ESHUTDOWN;
  }
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_clone(schedule->payload);
  if (!msg.owned_payload) return SALTS_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  if (scheduled_at > 0 && (uint64_t)scheduled_at <= UINT64_MAX / UINT64_C(1000000000)) {
    msg.ts_ns = (uint64_t)scheduled_at * UINT64_C(1000000000);
  } else {
    msg.ts_ns = salts_hrtime();
  }
  rc = turbo_flow_publish(schedule->flow, schedule->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  atomic_store_explicit(&schedule->last_status, rc, memory_order_release);
  if (rc == SALTS_OK) {
    atomic_fetch_add_explicit(&schedule->fired, 1, memory_order_relaxed);
  }
  return rc;
}

static int flow_schedule_publish_async(turbo_flow_schedule_t *schedule, time_t scheduled_at) {
  turbo_flow_msg_t msg;
  int rc;
  if (!schedule || !schedule->flow || !schedule->source_name ||
      !atomic_load_explicit(&schedule->started, memory_order_acquire)) {
    return SALTS_ESHUTDOWN;
  }
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_clone(schedule->payload);
  if (!msg.owned_payload) return SALTS_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  if (scheduled_at > 0 && (uint64_t)scheduled_at <= UINT64_MAX / UINT64_C(1000000000)) {
    msg.ts_ns = (uint64_t)scheduled_at * UINT64_C(1000000000);
  } else {
    msg.ts_ns = salts_hrtime();
  }
  atomic_fetch_add_explicit(&schedule->async_inflight, 1u, memory_order_acq_rel);
  rc = turbo_flow_publish_async(schedule->flow, schedule->source_name, &msg,
                                flow_schedule_publish_complete, schedule);
  turbo_flow_msg_cleanup(&msg);
  if (rc != SALTS_OK) {
    atomic_fetch_sub_explicit(&schedule->async_inflight, 1u, memory_order_release);
    atomic_store_explicit(&schedule->last_status, rc, memory_order_release);
  }
  return rc;
}

static int flow_schedule_advance_cron(turbo_flow_schedule_t *schedule, time_t now) {
  time_t current_minute;
  time_t check;
  time_t next_fire;
  uint32_t emitted = 0;
  uint32_t max_emit;
  int truncated = 0;
  int rc;
  if (!schedule || schedule->mode != TURBO_FLOW_SCHEDULE_CRON) return SALTS_EINVAL;
  current_minute = flow_schedule_floor_minute(now);
  salts_mutex_lock(&schedule->mutex);
  if (schedule->cron_cursor_minute != 0 && current_minute <= schedule->cron_cursor_minute) {
    salts_mutex_unlock(&schedule->mutex);
    return 0;
  }
  check = schedule->cron_cursor_minute == 0 ? current_minute - 60 : schedule->cron_cursor_minute;
  schedule->cron_cursor_minute = current_minute;
  salts_mutex_unlock(&schedule->mutex);

  max_emit = schedule->catch_up_limit + 1u;
  for (;;) {
    rc = salts_cron_next(&schedule->cron_expr, check, &next_fire);
    if (rc != SALTS_CRON_OK) {
      atomic_store_explicit(&schedule->last_status, SALTS_EINVAL, memory_order_release);
      return SALTS_EINVAL;
    }
    if (next_fire > current_minute) break;
    if (emitted >= max_emit) {
      truncated = 1;
      break;
    }
    rc = schedule->manual_clock ? flow_schedule_publish_sync(schedule, next_fire)
                                : flow_schedule_publish_async(schedule, next_fire);
    if (rc != SALTS_OK) return rc;
    emitted += 1u;
    check = next_fire;
  }
  if (truncated) {
    atomic_fetch_add_explicit(&schedule->catch_up_truncations, 1, memory_order_relaxed);
  }
  return (int)emitted;
}

static void flow_schedule_timer_callback(salts_timer_t *timer) {
  turbo_flow_schedule_t *schedule = (turbo_flow_schedule_t *)salts_timer_get_data(timer);
  int stop_timer = 0;
  int rc = SALTS_OK;
  if (!schedule ||
      atomic_exchange_explicit(&schedule->timer_callback_active, 1, memory_order_acq_rel)) {
    return;
  }
  if (!atomic_load_explicit(&schedule->started, memory_order_acquire) ||
      atomic_load_explicit(&schedule->completed, memory_order_acquire)) {
    stop_timer = atomic_load_explicit(&schedule->completed, memory_order_acquire);
    goto done;
  }

  if (schedule->mode == TURBO_FLOW_SCHEDULE_CRON) {
    rc = flow_schedule_advance_cron(schedule, time(NULL));
  } else {
    uint64_t now_ms = salts_monotonic_ms();
    uint64_t next_due_ms = atomic_load_explicit(&schedule->next_due_ms, memory_order_acquire);
    if (now_ms < next_due_ms) goto done;
    if (atomic_load_explicit(&schedule->async_inflight, memory_order_acquire) != 0u) goto done;
    rc = flow_schedule_publish_async(schedule, 0);
  }

  if (rc < 0) {
    atomic_store_explicit(&schedule->completed, 1, memory_order_release);
    stop_timer = 1;
  }
done:
  if (stop_timer) (void)salts_timer_stop(timer);
  atomic_store_explicit(&schedule->timer_callback_active, 0, memory_order_release);
}

static int flow_schedule_arm_timer(turbo_flow_schedule_t *schedule) {
  uint64_t timeout_ms;
  uint64_t repeat_ms;
  if (schedule->mode == TURBO_FLOW_SCHEDULE_CRON) {
    time_t now = time(NULL);
    time_t next_fire = 0;
    uint64_t delay_seconds;
    if (salts_cron_next(&schedule->cron_expr, now, &next_fire) != SALTS_CRON_OK ||
        next_fire <= now) {
      return SALTS_EINVAL;
    }
    delay_seconds = (uint64_t)(next_fire - now);
    timeout_ms = delay_seconds > FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS / UINT64_C(1000)
                     ? FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS
                     : delay_seconds * UINT64_C(1000);
    repeat_ms = FLOW_SCHEDULE_CRON_SCAN_MS;
  } else {
    timeout_ms = flow_schedule_timer_slice(schedule->delay_ms);
    repeat_ms = schedule->mode == TURBO_FLOW_SCHEDULE_ONE_SHOT &&
                        schedule->delay_ms <= FLOW_SCHEDULE_NATIVE_TIMER_MAX_MS
                    ? 0
                    : timeout_ms;
    atomic_store_explicit(&schedule->next_due_ms,
                          flow_schedule_add_delay(salts_monotonic_ms(), schedule->delay_ms),
                          memory_order_release);
  }
  return salts_timer_start(schedule->timer, flow_schedule_timer_callback, timeout_ms, repeat_ms) ==
                 0
             ? SALTS_OK
             : SALTS_EIO;
}

static void flow_schedule_bootstrap_thread(void *ctx) {
  turbo_flow_schedule_t *schedule = (turbo_flow_schedule_t *)ctx;
  int rc;
  while (atomic_load_explicit(&schedule->started, memory_order_acquire) && schedule->flow &&
         turbo_flow_state(schedule->flow) != TURBO_FLOW_STATE_STARTED) {
    salts_sleep_ms(FLOW_SCHEDULE_START_POLL_MS);
  }
  if (!atomic_load_explicit(&schedule->started, memory_order_acquire)) return;
  rc = flow_schedule_arm_timer(schedule);
  if (rc != SALTS_OK) {
    atomic_store_explicit(&schedule->last_status, rc, memory_order_release);
    atomic_store_explicit(&schedule->completed, 1, memory_order_release);
  }
}

static int flow_schedule_start(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  turbo_flow_schedule_t *schedule = (turbo_flow_schedule_t *)ctx;
  if (!schedule || !flow || !stage || !stage->is_source) return SALTS_EINVAL;
  if (atomic_load_explicit(&schedule->started, memory_order_acquire)) return SALTS_EALREADY;
  tstr_freep(&schedule->source_name);
  schedule->source_name = tstr_dup(stage->name);
  if (!schedule->source_name) return SALTS_ENOMEM;
  schedule->flow = flow;
  schedule->cron_cursor_minute = 0;
  atomic_store_explicit(&schedule->last_status, SALTS_OK, memory_order_release);
  atomic_store_explicit(&schedule->completed, 0, memory_order_release);
  atomic_store_explicit(&schedule->timer_callback_active, 0, memory_order_release);
  atomic_store_explicit(&schedule->async_inflight, 0u, memory_order_release);
  atomic_store_explicit(&schedule->run_fired, 0, memory_order_release);
  atomic_store_explicit(&schedule->next_due_ms, 0, memory_order_release);
  atomic_store_explicit(&schedule->started, 1, memory_order_release);
  if (schedule->manual_clock) return SALTS_OK;
  schedule->timer = salts_timer_create(NULL);
  if (!schedule->timer) {
    atomic_store_explicit(&schedule->started, 0, memory_order_release);
    return SALTS_ENOMEM;
  }
  salts_timer_set_data(schedule->timer, schedule);
  if (salts_thread_create(&schedule->bootstrap_thread, flow_schedule_bootstrap_thread, schedule) !=
      SALTS_OK) {
    atomic_store_explicit(&schedule->started, 0, memory_order_release);
    salts_timer_destroy(schedule->timer);
    schedule->timer = NULL;
    return SALTS_EIO;
  }
  schedule->bootstrap_thread_started = 1;
  return SALTS_OK;
}

static void flow_schedule_stop(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  turbo_flow_schedule_t *schedule = (turbo_flow_schedule_t *)ctx;
  (void)flow;
  (void)stage;
  if (!schedule) return;
  atomic_store_explicit(&schedule->started, 0, memory_order_release);
  if (schedule->bootstrap_thread_started) {
    (void)salts_thread_join(&schedule->bootstrap_thread);
    schedule->bootstrap_thread_started = 0;
  }
  salts_mutex_lock(&schedule->mutex);
  salts_timer_t *timer = schedule->timer;
  schedule->timer = NULL;
  salts_mutex_unlock(&schedule->mutex);
  salts_timer_destroy(timer);
  atomic_store_explicit(&schedule->completed, 1, memory_order_release);
  atomic_store_explicit(&schedule->timer_callback_active, 0, memory_order_release);
}

static void flow_schedule_shutdown(void *ctx) {
  turbo_flow_schedule_t *schedule = (turbo_flow_schedule_t *)ctx;
  if (!schedule) return;
  flow_schedule_stop(schedule, NULL, NULL);
  salts_mutex_destroy(&schedule->mutex);
  tstr_freep(&schedule->source_name);
  tstr_freep(&schedule->payload);
  free(schedule);
}

static int flow_schedule_validate(const turbo_flow_schedule_config_t *config,
                                  salts_cron_expr_t *cron_expr) {
  char error[128];
  if (!config || config->mode < TURBO_FLOW_SCHEDULE_INTERVAL ||
      config->mode > TURBO_FLOW_SCHEDULE_CRON ||
      config->catch_up_limit > TURBO_FLOW_SCHEDULE_MAX_CATCH_UP ||
      config->payload_len > TURBO_FLOW_SCHEDULE_MAX_PAYLOAD_SIZE ||
      (config->payload_len > 0 && !config->payload))
    return SALTS_EINVAL;
  if ((config->mode == TURBO_FLOW_SCHEDULE_INTERVAL ||
       config->mode == TURBO_FLOW_SCHEDULE_ONE_SHOT) &&
      (config->delay_ms == 0 || config->delay_ms > TURBO_FLOW_SCHEDULE_MAX_DELAY_MS)) {
    return SALTS_EINVAL;
  }
  if (config->mode != TURBO_FLOW_SCHEDULE_INTERVAL && config->repeat_limit != 0) {
    return SALTS_EINVAL;
  }
  if (config->mode == TURBO_FLOW_SCHEDULE_CRON) {
    if (!config->cron_expression || config->cron_expression[0] == '\0' || !cron_expr) {
      return SALTS_EINVAL;
    }
    if (salts_cron_parse_ex(config->cron_expression, cron_expr, error, sizeof(error)) !=
        SALTS_CRON_OK)
      return SALTS_EINVAL;
  } else if (config->cron_expression || config->catch_up_limit != 0 || config->manual_clock) {
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

int turbo_flow_schedule_register_adapter(turbo_flow_t *flow, const char *name,
                                         const turbo_flow_schedule_config_t *config,
                                         turbo_flow_schedule_t **out_schedule) {
  turbo_flow_schedule_t *schedule;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  salts_cron_expr_t cron_expr;
  int rc;
  if (out_schedule) *out_schedule = NULL;
  if (!flow || !name || name[0] == '\0') return SALTS_EINVAL;
  salts_cron_expr_init(&cron_expr);
  rc = flow_schedule_validate(config, &cron_expr);
  if (rc != SALTS_OK) return rc;
  schedule = (turbo_flow_schedule_t *)calloc(1, sizeof(*schedule));
  if (!schedule) return SALTS_ENOMEM;
  schedule->mode = config->mode;
  schedule->delay_ms = config->delay_ms;
  schedule->repeat_limit = config->repeat_limit;
  schedule->catch_up_limit = config->catch_up_limit;
  schedule->manual_clock = config->manual_clock ? 1 : 0;
  schedule->cron_expr = cron_expr;
  schedule->payload = tstr_new_len(config->payload, config->payload_len);
  if (!schedule->payload) {
    free(schedule);
    return SALTS_ENOMEM;
  }
  salts_mutex_init(&schedule->mutex);
  atomic_init(&schedule->started, 0);
  atomic_init(&schedule->completed, 0);
  atomic_init(&schedule->timer_callback_active, 0);
  atomic_init(&schedule->async_inflight, 0u);
  atomic_init(&schedule->run_fired, 0);
  atomic_init(&schedule->next_due_ms, 0);
  atomic_init(&schedule->fired, 0);
  atomic_init(&schedule->catch_up_truncations, 0);
  atomic_init(&schedule->last_status, SALTS_OK);
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_schedule_start;
  ops.stop = flow_schedule_stop;
  ops.shutdown = flow_schedule_shutdown;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SCHEDULE;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
  schema.direction = TURBO_FLOW_ADAPTER_INPUT;
  schema.fields = FLOW_SCHEDULE_FIELDS;
  schema.field_count = sizeof(FLOW_SCHEDULE_FIELDS) / sizeof(FLOW_SCHEDULE_FIELDS[0]);
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, schedule, &schema);
  if (rc != SALTS_OK) {
    flow_schedule_shutdown(schedule);
    return rc;
  }
  if (out_schedule) *out_schedule = schedule;
  return SALTS_OK;
}

int turbo_flow_schedule_advance(turbo_flow_schedule_t *schedule, time_t now) {
  if (!schedule || !schedule->manual_clock ||
      !atomic_load_explicit(&schedule->started, memory_order_acquire))
    return SALTS_EINVAL;
  return flow_schedule_advance_cron(schedule, now);
}

int turbo_flow_schedule_snapshot(const turbo_flow_schedule_t *schedule,
                                 turbo_flow_schedule_snapshot_t *out) {
  if (!schedule || !out) return SALTS_EINVAL;
  out->fired = atomic_load_explicit(&schedule->fired, memory_order_relaxed);
  out->catch_up_truncations =
      atomic_load_explicit(&schedule->catch_up_truncations, memory_order_relaxed);
  out->last_status = atomic_load_explicit(&schedule->last_status, memory_order_acquire);
  return SALTS_OK;
}
