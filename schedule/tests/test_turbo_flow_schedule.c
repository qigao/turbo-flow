#include "turbo_flow_schedule.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <string.h>

typedef struct schedule_capture_s {
  char payload[64];
  size_t payload_len;
  uint64_t last_ts_ns;
  uint64_t callback_delay_ms;
  atomic_int called;
  atomic_int active;
  atomic_int max_active;
} schedule_capture_t;

static int schedule_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  schedule_capture_t *capture = (schedule_capture_t *)ctx;
  int active;
  int observed_max;
  if (!capture || !msg || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  active = atomic_fetch_add_explicit(&capture->active, 1, memory_order_acq_rel) + 1;
  observed_max = atomic_load_explicit(&capture->max_active, memory_order_acquire);
  while (active > observed_max &&
         !atomic_compare_exchange_weak_explicit(&capture->max_active, &observed_max, active,
                                                memory_order_acq_rel, memory_order_acquire)) {
  }
  if (msg->payload.len > 0) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  capture->last_ts_ns = msg->ts_ns;
  if (capture->callback_delay_ms > 0) turbo_sleep_ms(capture->callback_delay_ms);
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  atomic_fetch_sub_explicit(&capture->active, 1, memory_order_release);
  return TURBO_OK;
}

static void schedule_capture_init(schedule_capture_t *capture) {
  memset(capture, 0, sizeof(*capture));
  atomic_init(&capture->called, 0);
  atomic_init(&capture->active, 0);
  atomic_init(&capture->max_active, 0);
}

static void schedule_wait_called(schedule_capture_t *capture, int expected) {
  for (int i = 0;
       i < 200 && atomic_load_explicit(&capture->called, memory_order_acquire) < expected; ++i) {
    turbo_sleep_ms(5);
  }
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
          TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", schedule_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
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
    schedule_capture_init(&capture);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_not_null(schedule);
    schema = turbo_flow_find_adapter_schema(flow, "schedule.test");
    check_not_null(schema);
    check_equal(schema->kind, TURBO_FLOW_ADAPTER_KIND_SCHEDULE);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    check_equal(schema->direction, TURBO_FLOW_ADAPTER_INPUT);
    check_equal(schema->field_count, 8);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    schedule_wait_called(&capture, 3);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 3);
    check_equal(capture.payload_len, sizeof(payload) - 1u);
    check_equal(memcmp(capture.payload, payload, sizeof(payload) - 1u), 0);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), TURBO_OK);
    check_equal(snapshot.fired, 3);
    check_equal(snapshot.catch_up_truncations, 0);
    check_equal(snapshot.last_status, TURBO_OK);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_sleep_ms(30);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 3);
    turbo_flow_destroy(flow);
  }

  it("emits one one-shot tick") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = 10;
    schedule_capture_init(&capture);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    schedule_wait_called(&capture, 1);
    turbo_sleep_ms(30);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("stops an interval before its first tick") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    config.delay_ms = 100;
    schedule_capture_init(&capture);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_sleep_ms(120);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    turbo_flow_destroy(flow);
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
    schedule_capture_init(&capture);
    capture.callback_delay_ms = 10;
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    schedule_wait_called(&capture, 3);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 3);
    check_equal(atomic_load_explicit(&capture.max_active, memory_order_acquire), 1);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
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
    schedule_capture_init(&capture);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    schedule_wait_called(&capture, 2);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    schedule_wait_called(&capture, 4);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 4);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), TURBO_OK);
    check_equal(snapshot.fired, 4);
    turbo_flow_destroy(flow);
  }

  it("does not truncate delays larger than a Windows native timer period") {
    turbo_flow_schedule_config_t config;
    turbo_flow_schedule_t *schedule = NULL;
    schedule_capture_t capture;
    turbo_flow_t *flow;
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = (uint64_t)UINT32_MAX + UINT64_C(100);
    schedule_capture_init(&capture);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_sleep_ms(20);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
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
    schedule_capture_init(&capture);
    flow = schedule_make_flow(&config, &capture, &schedule);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_schedule_advance(schedule, first + 5), 1);
    check_equal(turbo_flow_schedule_advance(schedule, first + 50), 0);
    check_equal(turbo_flow_schedule_advance(schedule, first + 5 * 60 + 5), 3);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 4);
    check_equal(turbo_flow_schedule_snapshot(schedule, &snapshot), TURBO_OK);
    check_equal(snapshot.fired, 4);
    check_equal(snapshot.catch_up_truncations, 1);
    check_equal(capture.last_ts_ns, (uint64_t)(first + 3 * 60) * UINT64_C(1000000000));
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    check_equal(turbo_flow_schedule_advance(schedule, first + 6 * 60), TURBO_EINVAL);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 4);
    turbo_flow_destroy(flow);
  }

  it("rejects invalid mode-specific configuration") {
    turbo_flow_schedule_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_INTERVAL;
    check_equal(turbo_flow_schedule_register_adapter(flow, "zero", &config, NULL), TURBO_EINVAL);
    config.delay_ms = TURBO_FLOW_SCHEDULE_MAX_DELAY_MS + UINT64_C(1);
    check_equal(turbo_flow_schedule_register_adapter(flow, "overflow", &config, NULL),
                 TURBO_EINVAL);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_ONE_SHOT;
    config.delay_ms = 1;
    config.repeat_limit = 2;
    check_equal(turbo_flow_schedule_register_adapter(flow, "repeat", &config, NULL), TURBO_EINVAL);
    memset(&config, 0, sizeof(config));
    config.mode = TURBO_FLOW_SCHEDULE_CRON;
    config.cron_expression = "*/0 * * * *";
    config.manual_clock = 1;
    check_equal(turbo_flow_schedule_register_adapter(flow, "bad-cron", &config, NULL),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }
}
