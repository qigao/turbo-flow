#include "flow_event_time_watermark.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

typedef struct watermark_probe_s {
  atomic_int fail_close;
  atomic_uint close_count;
  atomic_uint sink_count;
} watermark_probe_t;

typedef struct watermark_fixture_s {
  turbo_flow_t *flow;
  turbo_flow_event_time_window_store_t *store;
  tf_event_time_watermark_owner_t *owner;
  watermark_probe_t probe;
} watermark_fixture_t;

typedef struct watermark_observe_thread_s {
  tf_event_time_watermark_owner_t *owner;
  uint64_t first_event_time_ns;
  uint64_t event_time_step_ns;
  uint32_t count;
  atomic_int status;
} watermark_observe_thread_t;

static void watermark_observe_thread(void *arg) {
  watermark_observe_thread_t *observe = (watermark_observe_thread_t *)arg;
  uint64_t event_time_ns;
  int rc = TURBO_OK;
  if (!observe || !observe->owner) return;
  event_time_ns = observe->first_event_time_ns;
  for (uint32_t index = 0u; index < observe->count; ++index) {
    rc = tf_event_time_watermark_owner_observe(observe->owner, event_time_ns);
    if (rc != TURBO_OK) break;
    event_time_ns += observe->event_time_step_ns;
  }
  atomic_store_explicit(&observe->status, rc, memory_order_release);
}

static turbo_flow_operation_descriptor_t watermark_operation(const char *name, uint32_t flags) {
  turbo_flow_operation_descriptor_t operation;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = name;
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = "Message";
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  operation.flags = flags;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  if ((flags & TURBO_FLOW_OPERATION_STAGE) != 0u) {
    operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.input_type = "Message";
  }
  return operation;
}

static int watermark_select_key(const turbo_flow_msg_t *message, tstr_v *key, void *ctx) {
  (void)ctx;
  if (!message || !key) return TURBO_EINVAL;
  *key = tstr_v_from_buf((const char *)&message->id, sizeof(message->id));
  return TURBO_OK;
}

static int watermark_accumulate(const turbo_flow_msg_t *message,
                                const turbo_flow_event_time_window_t *window,
                                turbo_flow_keyed_state_t *state, void *ctx) {
  uint64_t count = 0u;
  (void)message;
  (void)ctx;
  if (window->aggregate.len > 0u) {
    if (window->aggregate.len != sizeof(count)) return TURBO_EPROTO;
    memcpy(&count, window->aggregate.data, sizeof(count));
  }
  count += 1u;
  return turbo_flow_keyed_state_put(state, tstr_v_from_buf((const char *)&count, sizeof(count)));
}

static int watermark_close(const turbo_flow_event_time_window_t *window,
                           turbo_flow_emitter_t *emitter, void *ctx) {
  watermark_probe_t *probe = (watermark_probe_t *)ctx;
  turbo_flow_msg_t output;
  uint64_t key;
  int rc;
  atomic_fetch_add_explicit(&probe->close_count, 1u, memory_order_relaxed);
  if (atomic_load_explicit(&probe->fail_close, memory_order_acquire)) return TURBO_EIO;
  if (window->key.len != sizeof(key)) return TURBO_EPROTO;
  memcpy(&key, window->key.data, sizeof(key));
  turbo_flow_msg_init(&output);
  output.id = key;
  output.ts_ns = window->start_ns;
  rc = turbo_flow_emitter_emit_move(emitter, &output);
  turbo_flow_msg_cleanup(&output);
  return rc;
}

static int watermark_sink(turbo_flow_msg_t *message, void *ctx) {
  watermark_probe_t *probe = (watermark_probe_t *)ctx;
  (void)message;
  atomic_fetch_add_explicit(&probe->sink_count, 1u, memory_order_relaxed);
  return TURBO_OK;
}

static int watermark_fixture_init(watermark_fixture_t *fixture, uint64_t interval_ms,
                                  uint64_t max_out_of_orderness_ns) {
  static const char *dsl = "source input operation data.input\n"
                           "stage window operation data.event_window\n"
                           "stage sink\n"
                           "stage main {\n"
                           "  input -> window -> sink\n"
                           "}\n";
  turbo_flow_event_time_window_store_config_t store_config =
      TURBO_FLOW_EVENT_TIME_WINDOW_STORE_CONFIG_INIT;
  tf_event_time_watermark_config_t owner_config = TF_EVENT_TIME_WATERMARK_CONFIG_INIT;
  turbo_flow_event_time_window_provider_registration_t provider =
      TURBO_FLOW_EVENT_TIME_WINDOW_PROVIDER_REGISTRATION_INIT;
  turbo_flow_operation_descriptor_t input =
      watermark_operation("data.input", TURBO_FLOW_OPERATION_SOURCE);
  turbo_flow_operation_descriptor_t window =
      watermark_operation("data.event_window", TURBO_FLOW_OPERATION_STAGE);
  int rc;

  memset(fixture, 0, sizeof(*fixture));
  atomic_init(&fixture->probe.fail_close, 0);
  atomic_init(&fixture->probe.close_count, 0u);
  atomic_init(&fixture->probe.sink_count, 0u);
  store_config.max_windows = 8u;
  store_config.max_key_size = sizeof(uint64_t);
  store_config.max_value_size = sizeof(uint64_t);
  store_config.max_total_bytes = 8u * (sizeof(uint64_t) * 3u);
  store_config.window_size_ns = 10u;
  store_config.allowed_lateness_ns = 0u;
  fixture->store = turbo_flow_event_time_window_store_create(&store_config);
  fixture->flow = turbo_flow_create();
  if (!fixture->store || !fixture->flow) return TURBO_ENOMEM;

  window.scope.state = TURBO_FLOW_STATE_SCOPE_NODE;
  window.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  window.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  provider.operation_name = window.name;
  provider.key_selector = watermark_select_key;
  provider.on_event = watermark_accumulate;
  provider.on_close = watermark_close;
  provider.ctx = &fixture->probe;
  provider.store = fixture->store;
  provider.max_outputs = 1u;
  rc = turbo_flow_register_operation(fixture->flow, &input);
  if (rc == TURBO_OK) rc = turbo_flow_register_operation(fixture->flow, &window);
  if (rc == TURBO_OK) {
    rc = turbo_flow_register_event_time_window_provider(fixture->flow, &provider);
  }
  if (rc == TURBO_OK) {
    rc = turbo_flow_register_stage_ex(fixture->flow, "sink", watermark_sink, &fixture->probe, NULL);
  }
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(fixture->flow, dsl, strlen(dsl));
  if (rc == TURBO_OK) rc = turbo_flow_compile(fixture->flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(fixture->flow);
  if (rc != TURBO_OK) return rc;

  owner_config.flow = fixture->flow;
  owner_config.store = fixture->store;
  owner_config.interval_ms = interval_ms;
  owner_config.max_out_of_orderness_ns = max_out_of_orderness_ns;
  fixture->owner = tf_event_time_watermark_owner_create(&owner_config);
  return fixture->owner ? TURBO_OK : TURBO_ENOMEM;
}

static void watermark_fixture_cleanup(watermark_fixture_t *fixture) {
  if (!fixture) return;
  tf_event_time_watermark_owner_destroy(fixture->owner);
  if (fixture->flow) {
    (void)turbo_flow_stop(fixture->flow);
    turbo_flow_destroy(fixture->flow);
  }
  turbo_flow_event_time_window_store_destroy(fixture->store);
  memset(fixture, 0, sizeof(*fixture));
}

static int watermark_publish(watermark_fixture_t *fixture, uint64_t id, uint64_t event_time_ns) {
  turbo_flow_msg_t message;
  int rc;
  turbo_flow_msg_init(&message);
  message.id = id;
  message.ts_ns = event_time_ns;
  rc = tf_event_time_watermark_owner_publish(fixture->owner, "input", &message);
  turbo_flow_msg_cleanup(&message);
  return rc;
}

spec("event-time watermark owner") {
  it("validates configuration and reports an empty manual tick") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    tf_event_time_watermark_config_t invalid = TF_EVENT_TIME_WATERMARK_CONFIG_INIT;
    check_null(tf_event_time_watermark_owner_create(&invalid));
    check_int_eq(watermark_fixture_init(&fixture, 10u, 5u), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_tick(fixture.owner), TURBO_ENOENT);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TF_EVENT_TIME_WATERMARK_STOPPED);
    check_false(snapshot.event_time_observed);
    check_uint_eq(snapshot.advance_attempt_count, 0u);
    watermark_fixture_cleanup(&fixture);
  }

  it("publishes before observing and advances a bounded watermark") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    check_int_eq(watermark_fixture_init(&fixture, 10u, 5u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 1u, 4u), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_tick(fixture.owner), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 2u, 15u), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_tick(fixture.owner), TURBO_OK);
    check_uint_eq(atomic_load_explicit(&fixture.probe.sink_count, memory_order_relaxed), 1u);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.max_observed_event_time_ns, 15u);
    check_uint_eq(snapshot.last_successful_watermark_ns, 10u);
    check_uint_eq(snapshot.observed_event_count, 2u);
    check_uint_eq(snapshot.advance_success_count, 2u);
    check_uint_eq(snapshot.closed_window_count, 1u);
    watermark_fixture_cleanup(&fixture);
  }

  it("does not observe a message rejected by publication") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    turbo_flow_msg_t message;
    check_int_eq(watermark_fixture_init(&fixture, 10u, 0u), TURBO_OK);
    turbo_flow_msg_init(&message);
    message.ts_ns = 99u;
    check_int_eq(tf_event_time_watermark_owner_publish(fixture.owner, "missing", &message),
                 TURBO_EINVAL);
    turbo_flow_msg_cleanup(&message);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_false(snapshot.event_time_observed);
    check_uint_eq(snapshot.observed_event_count, 0u);
    watermark_fixture_cleanup(&fixture);
  }

  it("merges concurrent accepted timestamps with one atomic maximum") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    watermark_observe_thread_t observations[2];
    turbo_thread_t threads[2] = {NULL, NULL};
    check_int_eq(watermark_fixture_init(&fixture, 10u, 0u), TURBO_OK);
    memset(observations, 0, sizeof(observations));
    for (uint32_t index = 0u; index < 2u; ++index) {
      observations[index].owner = fixture.owner;
      observations[index].first_event_time_ns = index + 1u;
      observations[index].event_time_step_ns = 2u;
      observations[index].count = 1000u;
      atomic_init(&observations[index].status, TURBO_EIO);
      check_int_eq(
          turbo_thread_create(&threads[index], watermark_observe_thread, &observations[index]),
          TURBO_OK);
    }
    for (uint32_t index = 0u; index < 2u; ++index) {
      check_int_eq(turbo_thread_join(&threads[index]), TURBO_OK);
      check_int_eq(atomic_load_explicit(&observations[index].status, memory_order_acquire),
                   TURBO_OK);
    }
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.max_observed_event_time_ns, 2000u);
    check_uint_eq(snapshot.observed_event_count, 2000u);
    watermark_fixture_cleanup(&fixture);
  }

  it("fails fast and permits an equal-watermark retry after reset") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    check_int_eq(watermark_fixture_init(&fixture, 10u, 0u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 1u, 4u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 2u, 10u), TURBO_OK);
    atomic_store_explicit(&fixture.probe.fail_close, 1, memory_order_release);
    check_int_eq(tf_event_time_watermark_owner_tick(fixture.owner), TURBO_EIO);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TF_EVENT_TIME_WATERMARK_FAILED);
    check_int_eq(snapshot.last_status, TURBO_EIO);
    check_uint_eq(snapshot.last_attempted_watermark_ns, 10u);
    check_uint_eq(snapshot.advance_success_count, 0u);
    check_uint_eq(snapshot.closed_window_count, 0u);
    check_int_eq(watermark_publish(&fixture, 3u, 20u), TURBO_EBUSY);
    check_int_eq(tf_event_time_watermark_owner_stop(fixture.owner), TURBO_OK);
    atomic_store_explicit(&fixture.probe.fail_close, 0, memory_order_release);
    check_int_eq(tf_event_time_watermark_owner_reset(fixture.owner), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_tick(fixture.owner), TURBO_OK);
    check_uint_eq(atomic_load_explicit(&fixture.probe.sink_count, memory_order_relaxed), 1u);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TF_EVENT_TIME_WATERMARK_STOPPED);
    check_uint_eq(snapshot.last_successful_watermark_ns, 10u);
    check_uint_eq(snapshot.advance_attempt_count, 2u);
    check_uint_eq(snapshot.closed_window_count, 1u);
    watermark_fixture_cleanup(&fixture);
  }

  it("periodically advances and stops through the coalescing native timer") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    uint64_t deadline;
    check_int_eq(watermark_fixture_init(&fixture, 5u, 5u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 1u, 4u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 2u, 15u), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_start(fixture.owner), TURBO_OK);
    deadline = turbo_hrtime() + UINT64_C(1000000000);
    while (atomic_load_explicit(&fixture.probe.sink_count, memory_order_acquire) == 0u &&
           turbo_hrtime() < deadline) {
      turbo_sleep_ms(1u);
    }
    check_uint_eq(atomic_load_explicit(&fixture.probe.sink_count, memory_order_acquire), 1u);
    check_int_eq(tf_event_time_watermark_owner_stop(fixture.owner), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TF_EVENT_TIME_WATERMARK_STOPPED);
    check_uint_eq(snapshot.last_successful_watermark_ns, 10u);
    check_uint_eq(snapshot.closed_window_count, 1u);
    watermark_fixture_cleanup(&fixture);
  }

  it("stops periodic scheduling after a callback failure until explicitly reset") {
    watermark_fixture_t fixture;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    uint64_t deadline;
    uint64_t attempts_after_failure;
    check_int_eq(watermark_fixture_init(&fixture, 2u, 0u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 1u, 4u), TURBO_OK);
    check_int_eq(watermark_publish(&fixture, 2u, 10u), TURBO_OK);
    atomic_store_explicit(&fixture.probe.fail_close, 1, memory_order_release);
    check_int_eq(tf_event_time_watermark_owner_start(fixture.owner), TURBO_OK);
    deadline = turbo_hrtime() + UINT64_C(1000000000);
    do {
      check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
      if (snapshot.state == TF_EVENT_TIME_WATERMARK_FAILED) break;
      turbo_sleep_ms(1u);
    } while (turbo_hrtime() < deadline);
    check_int_eq(snapshot.state, TF_EVENT_TIME_WATERMARK_FAILED);
    check_int_eq(snapshot.last_status, TURBO_EIO);
    attempts_after_failure = snapshot.advance_attempt_count;
    turbo_sleep_ms(20u);
    check_int_eq(tf_event_time_watermark_owner_snapshot(fixture.owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.advance_attempt_count, attempts_after_failure);
    check_int_eq(tf_event_time_watermark_owner_reset(fixture.owner), TURBO_EBUSY);
    check_int_eq(tf_event_time_watermark_owner_stop(fixture.owner), TURBO_OK);
    atomic_store_explicit(&fixture.probe.fail_close, 0, memory_order_release);
    check_int_eq(tf_event_time_watermark_owner_reset(fixture.owner), TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_tick(fixture.owner), TURBO_OK);
    watermark_fixture_cleanup(&fixture);
  }
}
