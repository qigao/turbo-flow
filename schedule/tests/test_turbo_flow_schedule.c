#include "turbo_flow_schedule.h"

#include "tinytest.h"
#include "salts_error.h"
#include "salts_thread.h"

#include <stdatomic.h>
#include <string.h>

#define SCHEDULE_TEST_WAIT_TIMEOUT_NS UINT64_C(1000000000)

typedef struct schedule_capture_s {
  char payload[64];
  size_t payload_len;
  uint64_t last_ts_ns;
  uint64_t callback_delay_ms;
  salts_mutex_t wait_mutex;
  salts_cond_t called_cond;
  atomic_int called;
  atomic_int active;
  atomic_int max_active;
} schedule_capture_t;

typedef struct schedule_lifecycle_gate_s {
  salts_mutex_t mutex;
  salts_cond_t cond;
  unsigned entered;
  unsigned released;
  unsigned stop_calls;
} schedule_lifecycle_gate_t;

static int schedule_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  schedule_capture_t *capture = (schedule_capture_t *)ctx;
  int active;
  int observed_max;
  if (!capture || !msg || msg->payload.len > sizeof(capture->payload)) return SALTS_EINVAL;
  active = atomic_fetch_add_explicit(&capture->active, 1, memory_order_acq_rel) + 1;
  observed_max = atomic_load_explicit(&capture->max_active, memory_order_acquire);
  while (active > observed_max &&
         !atomic_compare_exchange_weak_explicit(&capture->max_active, &observed_max, active,
                                                memory_order_acq_rel, memory_order_acquire)) {
  }
  if (msg->payload.len > 0) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  capture->last_ts_ns = msg->ts_ns;
  if (capture->callback_delay_ms > 0) salts_sleep_ms(capture->callback_delay_ms);
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  salts_mutex_lock(&capture->wait_mutex);
  salts_cond_broadcast(&capture->called_cond);
  salts_mutex_unlock(&capture->wait_mutex);
  atomic_fetch_sub_explicit(&capture->active, 1, memory_order_release);
  return SALTS_OK;
}

static int schedule_capture_init(schedule_capture_t *capture) {
  memset(capture, 0, sizeof(*capture));
  salts_mutex_init(&capture->wait_mutex);
  salts_cond_init(&capture->called_cond);
  if (!capture->wait_mutex || !capture->called_cond) {
    salts_cond_destroy(&capture->called_cond);
    salts_mutex_destroy(&capture->wait_mutex);
    return SALTS_ENOMEM;
  }
  atomic_init(&capture->called, 0);
  atomic_init(&capture->active, 0);
  atomic_init(&capture->max_active, 0);
  return SALTS_OK;
}

static void schedule_capture_destroy(schedule_capture_t *capture) {
  salts_cond_destroy(&capture->called_cond);
  salts_mutex_destroy(&capture->wait_mutex);
}

static int schedule_capture_called(const schedule_capture_t *capture) {
  return atomic_load_explicit(&capture->called, memory_order_acquire);
}

static int schedule_wait_called(schedule_capture_t *capture, int expected) {
  const uint64_t started_at = salts_hrtime();
  salts_mutex_lock(&capture->wait_mutex);
  while (schedule_capture_called(capture) < expected) {
    const uint64_t elapsed = salts_hrtime() - started_at;
    if (elapsed >= SCHEDULE_TEST_WAIT_TIMEOUT_NS ||
        salts_cond_timedwait(&capture->called_cond, &capture->wait_mutex,
                             SCHEDULE_TEST_WAIT_TIMEOUT_NS - elapsed) != 0) {
      break;
    }
  }
  salts_mutex_unlock(&capture->wait_mutex);
  return schedule_capture_called(capture) >= expected;
}

static int schedule_lifecycle_gate_init(schedule_lifecycle_gate_t *gate) {
  memset(gate, 0, sizeof(*gate));
  salts_mutex_init(&gate->mutex);
  salts_cond_init(&gate->cond);
  if (!gate->mutex || !gate->cond) {
    salts_cond_destroy(&gate->cond);
    salts_mutex_destroy(&gate->mutex);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static void schedule_lifecycle_gate_destroy(schedule_lifecycle_gate_t *gate) {
  salts_cond_destroy(&gate->cond);
  salts_mutex_destroy(&gate->mutex);
}

static int schedule_lifecycle_gate_consume(void *ctx, turbo_flow_t *flow,
                                           const turbo_flow_stage_plan_t *stage,
                                           turbo_flow_msg_t *msg) {
  schedule_lifecycle_gate_t *gate = (schedule_lifecycle_gate_t *)ctx;
  unsigned generation;
  (void)flow;
  (void)stage;
  (void)msg;
  if (!gate) return SALTS_EINVAL;
  salts_mutex_lock(&gate->mutex);
  generation = ++gate->entered;
  salts_cond_broadcast(&gate->cond);
  while (gate->released < generation) {
    salts_cond_wait(&gate->cond, &gate->mutex);
  }
  salts_mutex_unlock(&gate->mutex);
  return SALTS_OK;
}

static void schedule_lifecycle_gate_stop(void *ctx, turbo_flow_t *flow,
                                         const turbo_flow_stage_plan_t *stage) {
  schedule_lifecycle_gate_t *gate = (schedule_lifecycle_gate_t *)ctx;
  (void)flow;
  (void)stage;
  if (!gate) return;
  salts_mutex_lock(&gate->mutex);
  ++gate->stop_calls;
  gate->released = gate->entered;
  salts_cond_broadcast(&gate->cond);
  salts_mutex_unlock(&gate->mutex);
}

static int schedule_lifecycle_gate_wait_entered(schedule_lifecycle_gate_t *gate,
                                                unsigned expected) {
  const uint64_t started_at = salts_hrtime();
  int entered;
  salts_mutex_lock(&gate->mutex);
  while (gate->entered < expected) {
    const uint64_t elapsed = salts_hrtime() - started_at;
    if (elapsed >= SCHEDULE_TEST_WAIT_TIMEOUT_NS ||
        salts_cond_timedwait(&gate->cond, &gate->mutex,
                             SCHEDULE_TEST_WAIT_TIMEOUT_NS - elapsed) != 0) {
      break;
    }
  }
  entered = gate->entered >= expected;
  salts_mutex_unlock(&gate->mutex);
  return entered;
}

static turbo_flow_t *schedule_make_flow(const turbo_flow_schedule_config_t *config,
                                        schedule_capture_t *capture,
                                        turbo_flow_schedule_t **out_schedule) {
  static const char *dsl = "source tick adapter schedule.test\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  tick -> capture\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow ||
      turbo_flow_schedule_register_adapter(flow, "schedule.test", config, out_schedule) !=
          SALTS_OK ||
      turbo_flow_register_stage_ex(flow, "capture", schedule_capture_stage, capture, NULL) !=
          SALTS_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *schedule_make_lifecycle_flow(const turbo_flow_schedule_config_t *config,
                                                  schedule_lifecycle_gate_t *gate,
                                                  turbo_flow_schedule_t **out_schedule) {
  static const char *dsl = "source tick adapter schedule.test\n"
                           "stage gated adapter schedule.lifecycle\n"
                           "stage main {\n"
                           "  tick -> gated\n"
                           "}\n";
  turbo_flow_adapter_ops_t ops;
  turbo_flow_t *flow = turbo_flow_create();
  memset(&ops, 0, sizeof(ops));
  ops.consume = schedule_lifecycle_gate_consume;
  ops.stop = schedule_lifecycle_gate_stop;
  if (!flow ||
      turbo_flow_schedule_register_adapter(flow, "schedule.test", config, out_schedule) !=
          SALTS_OK ||
      turbo_flow_register_adapter(flow, "schedule.lifecycle", &ops, gate) != SALTS_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static time_t schedule_local_time(int year, int month, int day, int hour, int minute) {
  struct tm value;
  memset(&value, 0, sizeof(value));
  value.tm_year = year - 1900;
  value.tm_mon = month - 1;
  value.tm_mday = day;
  value.tm_hour = hour;
  value.tm_min = minute;
  value.tm_isdst = -1;
  return mktime(&value);
}

spec("turbo_flow_schedule") {
  it("emits a bounded interval sequence and exposes its schema") {
    static const char payload[] = "tick-data";
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_snapshot_t snapshot;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    const turbo_flow_adapter_schema_t *schema;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    config.delay_ms = 10;
    config.repeat_limit = 3;
    config.payload = payload;
    config.payload_len = sizeof(payload) - 1u;
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_not_null(schedule);
    schema = turbo_flow_find_adapter_schema(flow, "schedule.test");
    check_not_null(schema);
    check_equal(schema->kind, TURBO_FLOW_ADAPTER_KIND_SCHEDULE);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    check_equal(schema->direction, TURBO_FLOW_ADAPTER_INPUT);
    check_equal(schema->field_count, 8);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(schedule_wait_called(&capture, 3));
    check_equal(schedule_capture_called(&capture), 3);
    check_equal(capture.payload_len, sizeof(payload) - 1u);
    check_equal(memcmp(capture.payload, payload, sizeof(payload) - 1u), 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), SALTS_OK);
    check_equal(snapshot.fired, 3);
    check_equal(snapshot.catch_up_truncations, 0);
    check_equal(snapshot.last_status, SALTS_OK);
    check_equal(schedule_capture_called(&capture), 3);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("emits one one-shot tick") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_snapshot_t snapshot;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    int completed;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = 10;
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    completed = schedule_wait_called(&capture, 1);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), SALTS_OK);
    info("one-shot completion=%d called=%d fired=%llu status=%d", completed,
         schedule_capture_called(&capture), (unsigned long long)snapshot.fired,
         snapshot.last_status);
    check_true(completed);
    check_equal(schedule_capture_called(&capture), 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(schedule_capture_called(&capture), 1);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("drains an accepted one-shot before restarting its generation") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_snapshot_t snapshot;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_lifecycle_gate_t gate;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = 1;
    check_equal(schedule_lifecycle_gate_init(&gate), SALTS_OK);
    flow = schedule_make_lifecycle_flow(&config, &gate, &schedule);
    check_not_null(flow);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(schedule_lifecycle_gate_wait_entered(&gate, 1u));
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), SALTS_OK);
    check_equal(snapshot.fired, 1u);
    check_equal(snapshot.last_status, SALTS_OK);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(schedule_lifecycle_gate_wait_entered(&gate, 2u));
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), SALTS_OK);
    check_equal(snapshot.fired, 2u);
    check_equal(snapshot.last_status, SALTS_OK);
    check_equal(gate.entered, 2u);
    check_equal(gate.stop_calls, 2u);

    turbo_flow_destroy(flow);
    schedule_lifecycle_gate_destroy(&gate);
  }

  it("stops an interval before its first tick") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    config.delay_ms = 100;
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(schedule_capture_called(&capture), 0);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("coalesces overlapping native timer callbacks") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    config.delay_ms = 1;
    config.repeat_limit = 3;
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    capture.callback_delay_ms = 10;
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(schedule_wait_called(&capture, 3));
    check_equal(schedule_capture_called(&capture), 3);
    check_equal(atomic_load_explicit(&capture.max_active, memory_order_acquire), 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(schedule_capture_called(&capture), 3);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("resets a bounded interval count when the flow restarts") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_snapshot_t snapshot;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    config.delay_ms = 5;
    config.repeat_limit = 2;
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(schedule_wait_called(&capture, 2));
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(schedule_capture_called(&capture), 2);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(schedule_wait_called(&capture, 4));
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(schedule_capture_called(&capture), 4);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), SALTS_OK);
    check_equal(snapshot.fired, 4);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("does not truncate delays larger than a Windows native timer period") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = (uint64_t)UINT32_MAX + UINT64_C(100);
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    salts_sleep_ms(20);
    check_equal(schedule_capture_called(&capture), 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(schedule_capture_called(&capture), 0);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("bounds cron catch-up and suppresses repeated minutes") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_snapshot_t snapshot;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    time_t first = schedule_local_time(2024, 1, 15, 10, 0);
    turbo_flow_t *flow;
    check_true(first != (time_t)-1);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_CRON;
    config.cron_expression = "* * * * *";
    config.catch_up_limit = 2;
    config.manual_clock = 1;
    check_equal(schedule_capture_init(&capture), SALTS_OK);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_schedule_advance(schedule, first + 5), 1);
    check_equal(turbo_flow_schedule_advance(schedule, first + 50), 0);
    check_equal(turbo_flow_schedule_advance(schedule, first + 5 * 60 + 5), 3);
    check_equal(schedule_capture_called(&capture), 4);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), SALTS_OK);
    check_equal(snapshot.fired, 4);
    check_equal(snapshot.catch_up_truncations, 1);
    check_equal(capture.last_ts_ns, (uint64_t)(first + 3 * 60) * UINT64_C(1000000000));
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_schedule_advance(schedule, first + 6 * 60), SALTS_EINVAL);
    check_equal(schedule_capture_called(&capture), 4);
    turbo_flow_destroy(flow);
    schedule_capture_destroy(&capture);
  }

  it("rejects invalid mode-specific configuration") {
    turbo_flow_schedule_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    check_equal(turbo_flow_schedule_register_adapter(flow, "zero", &config, NULL), SALTS_EINVAL);
    config.delay_ms = TURBO_FLOW_SCHEDULE_MAX_DELAY_MS + UINT64_C(1);
    check_equal(turbo_flow_schedule_register_adapter(flow, "overflow", &config, NULL),
                 SALTS_EINVAL);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = 1;
    config.repeat_limit = 2;
    check_equal(turbo_flow_schedule_register_adapter(flow, "repeat", &config, NULL), SALTS_EINVAL);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_CRON;
    config.cron_expression = "*/0 * * * *";
    config.manual_clock = 1;
    check_equal(turbo_flow_schedule_register_adapter(flow, "bad-cron", &config, NULL),
                 SALTS_EINVAL);
    turbo_flow_destroy(flow);
  }
}
