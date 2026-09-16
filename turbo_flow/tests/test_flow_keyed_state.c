#include "../../tests/flow_operation_fixture.h"
#include "salts_thread.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <stdatomic.h>
#include <string.h>

typedef struct keyed_probe_s {
  atomic_uint callback_count;
  atomic_uint sink_count;
  atomic_uint barrier_count;
  int fail_after_put;
  int synchronize_callbacks;
  uint64_t sink_ids[8];
  uint32_t sink_values[8];
} keyed_probe_t;

typedef struct keyed_publish_s {
  turbo_flow_t *flow;
  uint64_t id;
  atomic_int status;
} keyed_publish_t;

typedef struct keyed_window_probe_s {
  uint64_t window_size;
  uint32_t emit_count;
  int synchronize_callbacks;
  atomic_uint sink_count;
  atomic_uint barrier_count;
  uint64_t sink_ids[8];
  uint32_t sink_values[8];
} keyed_window_probe_t;

typedef struct event_window_probe_s {
  int fail_close;
  int fail_sink;
  int synchronize_event;
  uint32_t close_emit_count;
  atomic_uint event_count;
  atomic_uint close_count;
  atomic_uint sink_count;
  atomic_uint event_entered;
  atomic_uint event_release;
  uint64_t sink_ids[8];
  uint64_t sink_timestamps[8];
  uint32_t sink_values[8];
} event_window_probe_t;

typedef struct event_window_publish_s {
  turbo_flow_t *flow;
  uint64_t id;
  uint64_t timestamp_ns;
  atomic_int status;
} event_window_publish_t;

static int publish_event(turbo_flow_t *flow, uint64_t id, uint64_t timestamp_ns);

static turbo_flow_operation_descriptor_t keyed_operation_descriptor(const char *name,
                                                                     uint32_t flags) {
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

static int keyed_select_id(const turbo_flow_msg_t *message, vstr *key, void *ctx) {
  (void)ctx;
  if (!message || !key) return SALTS_EINVAL;
  *key = vstr_from_buf((const char *)&message->id, sizeof(message->id));
  return SALTS_OK;
}

static int keyed_count_stage(turbo_flow_msg_t *message, turbo_flow_keyed_state_t *state,
                             void *ctx) {
  keyed_probe_t *probe = (keyed_probe_t *)ctx;
  vstr value;
  uint64_t count = 0u;
  int rc = turbo_flow_keyed_state_get(state, &value, NULL);

  if (rc == SALTS_OK) {
    if (value.len != sizeof(count)) return SALTS_EPROTO;
    memcpy(&count, value.data, sizeof(count));
  } else if (rc != SALTS_ENOENT) {
    return rc;
  }
  count += 1u;
  message->type = (uint32_t)count;
  rc = turbo_flow_keyed_state_put(state, vstr_from_buf((const char *)&count, sizeof(count)));
  if (rc != SALTS_OK) return rc;
  atomic_fetch_add_explicit(&probe->callback_count, 1u, memory_order_relaxed);
  if (probe->synchronize_callbacks) {
    atomic_fetch_add_explicit(&probe->barrier_count, 1u, memory_order_acq_rel);
    while (atomic_load_explicit(&probe->barrier_count, memory_order_acquire) < 2u) {
      salts_thread_yield();
    }
  }
  return probe->fail_after_put ? SALTS_EIO : SALTS_OK;
}

static int keyed_sink(turbo_flow_msg_t *message, void *ctx) {
  keyed_probe_t *probe = (keyed_probe_t *)ctx;
  unsigned int index = atomic_fetch_add_explicit(&probe->sink_count, 1u, memory_order_relaxed);
  if (index >= sizeof(probe->sink_ids) / sizeof(probe->sink_ids[0])) return SALTS_ENOSPC;
  probe->sink_ids[index] = message->id;
  probe->sink_values[index] = message->type;
  return SALTS_OK;
}

static int keyed_window_stage(const turbo_flow_msg_t *message,
                              turbo_flow_keyed_state_t *state,
                              turbo_flow_emitter_t *emitter, void *ctx) {
  keyed_window_probe_t *probe = (keyed_window_probe_t *)ctx;
  vstr value;
  uint64_t count = 0u;
  int rc = turbo_flow_keyed_state_get(state, &value, NULL);

  if (rc == SALTS_OK) {
    if (value.len != sizeof(count)) return SALTS_EPROTO;
    memcpy(&count, value.data, sizeof(count));
  } else if (rc != SALTS_ENOENT) {
    return rc;
  }
  count += 1u;
  if (count < probe->window_size) {
    return turbo_flow_keyed_state_put(
        state, vstr_from_buf((const char *)&count, sizeof(count)));
  }
  rc = turbo_flow_keyed_state_delete(state);
  if (rc == SALTS_ENOENT) rc = SALTS_OK;
  if (rc != SALTS_OK) return rc;
  for (uint32_t output_index = 0u; output_index < probe->emit_count; ++output_index) {
    turbo_flow_msg_t output;
    turbo_flow_msg_init(&output);
    output.id = message->id;
    output.type = (uint32_t)count;
    rc = turbo_flow_emitter_emit_move(emitter, &output);
    turbo_flow_msg_cleanup(&output);
    if (rc != SALTS_OK) return rc;
  }
  if (probe->synchronize_callbacks) {
    atomic_fetch_add_explicit(&probe->barrier_count, 1u, memory_order_acq_rel);
    while (atomic_load_explicit(&probe->barrier_count, memory_order_acquire) < 2u) {
      salts_thread_yield();
    }
  }
  return SALTS_OK;
}

static int keyed_window_sink(turbo_flow_msg_t *message, void *ctx) {
  keyed_window_probe_t *probe = (keyed_window_probe_t *)ctx;
  unsigned int index = atomic_fetch_add_explicit(&probe->sink_count, 1u, memory_order_relaxed);
  if (index >= sizeof(probe->sink_ids) / sizeof(probe->sink_ids[0])) return SALTS_ENOSPC;
  probe->sink_ids[index] = message->id;
  probe->sink_values[index] = message->type;
  return SALTS_OK;
}

static int event_window_accumulate(const turbo_flow_msg_t *message,
                                   const turbo_flow_event_time_window_t *window,
                                   turbo_flow_keyed_state_t *state, void *ctx) {
  event_window_probe_t *probe = (event_window_probe_t *)ctx;
  uint64_t count = 0u;
  (void)message;
  if (window->aggregate.len > 0u) {
    if (window->aggregate.len != sizeof(count)) return SALTS_EPROTO;
    memcpy(&count, window->aggregate.data, sizeof(count));
  }
  count += 1u;
  atomic_fetch_add_explicit(&probe->event_count, 1u, memory_order_relaxed);
  {
    int rc = turbo_flow_keyed_state_put(
        state, vstr_from_buf((const char *)&count, sizeof(count)));
    if (rc != SALTS_OK) return rc;
  }
  if (probe->synchronize_event) {
    atomic_store_explicit(&probe->event_entered, 1u, memory_order_release);
    while (!atomic_load_explicit(&probe->event_release, memory_order_acquire)) {
      salts_thread_yield();
    }
  }
  return SALTS_OK;
}

static int event_window_close(const turbo_flow_event_time_window_t *window,
                              turbo_flow_emitter_t *emitter, void *ctx) {
  event_window_probe_t *probe = (event_window_probe_t *)ctx;
  uint64_t key;
  uint64_t count;
  int rc;

  atomic_fetch_add_explicit(&probe->close_count, 1u, memory_order_relaxed);
  if (probe->fail_close) return SALTS_EIO;
  if (window->key.len != sizeof(key) || window->aggregate.len != sizeof(count)) {
    return SALTS_EPROTO;
  }
  memcpy(&key, window->key.data, sizeof(key));
  memcpy(&count, window->aggregate.data, sizeof(count));
  for (uint32_t index = 0u; index < probe->close_emit_count; ++index) {
    turbo_flow_msg_t output;
    turbo_flow_msg_init(&output);
    output.id = key;
    output.ts_ns = window->start_ns;
    output.type = (uint32_t)count;
    rc = turbo_flow_emitter_emit_move(emitter, &output);
    turbo_flow_msg_cleanup(&output);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

static int event_window_sink(turbo_flow_msg_t *message, void *ctx) {
  event_window_probe_t *probe = (event_window_probe_t *)ctx;
  unsigned int index = atomic_fetch_add_explicit(&probe->sink_count, 1u, memory_order_relaxed);
  if (index >= sizeof(probe->sink_ids) / sizeof(probe->sink_ids[0])) return SALTS_ENOSPC;
  probe->sink_ids[index] = message->id;
  probe->sink_timestamps[index] = message->ts_ns;
  probe->sink_values[index] = message->type;
  return probe->fail_sink ? SALTS_EIO : SALTS_OK;
}

static void event_window_publish_thread(void *ctx) {
  event_window_publish_t *publish = (event_window_publish_t *)ctx;
  int rc = publish_event(publish->flow, publish->id, publish->timestamp_ns);
  atomic_store_explicit(&publish->status, rc, memory_order_release);
}

static void keyed_publish_thread(void *ctx) {
  keyed_publish_t *publish = (keyed_publish_t *)ctx;
  turbo_flow_msg_t message;
  int rc;
  turbo_flow_msg_init(&message);
  message.id = publish->id;
  rc = turbo_flow_publish(publish->flow, "input", &message);
  turbo_flow_msg_cleanup(&message);
  atomic_store_explicit(&publish->status, rc, memory_order_release);
}

static int register_keyed_graph(turbo_flow_t *flow, turbo_flow_keyed_state_store_t *store,
                                keyed_probe_t *probe,
                                turbo_flow_state_scope_t state_scope, const char *dsl) {
  turbo_flow_operation_descriptor_t input =
      keyed_operation_descriptor("data.input", TURBO_FLOW_OPERATION_SOURCE);
  turbo_flow_operation_descriptor_t count =
      keyed_operation_descriptor("data.count", TURBO_FLOW_OPERATION_STAGE);
  turbo_flow_keyed_operation_provider_registration_t provider =
      TURBO_FLOW_KEYED_OPERATION_PROVIDER_REGISTRATION_INIT;
  int rc;

  count.scope.state = state_scope;
  count.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  count.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  provider.operation_name = count.name;
  provider.key_selector = keyed_select_id;
  provider.fn = keyed_count_stage;
  provider.ctx = probe;
  provider.store = store;
  rc = turbo_flow_register_operation(flow, &input);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_operation(flow, &count);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_keyed_operation_provider(flow, &provider);
  if (rc != SALTS_OK) return rc;
  flow_test_operation_t operation_sink_0 = flow_test_operation_init("test.sink", keyed_sink, probe);
  operation_sink_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_sink_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  rc = flow_test_operation_register(flow, &operation_sink_0);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_parse_string(flow, dsl, strlen(dsl));
}

static int register_keyed_window_graph(turbo_flow_t *flow,
                                       turbo_flow_keyed_state_store_t *store,
                                       keyed_window_probe_t *probe, uint32_t max_outputs,
                                       const char *dsl) {
  turbo_flow_operation_descriptor_t input =
      keyed_operation_descriptor("data.input", TURBO_FLOW_OPERATION_SOURCE);
  turbo_flow_operation_descriptor_t window =
      keyed_operation_descriptor("data.window", TURBO_FLOW_OPERATION_STAGE);
  turbo_flow_keyed_emitting_operation_provider_registration_t provider =
      TURBO_FLOW_KEYED_EMITTING_OPERATION_PROVIDER_REGISTRATION_INIT;
  int rc;

  window.scope.state = TURBO_FLOW_STATE_SCOPE_NODE;
  window.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  window.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  provider.operation_name = window.name;
  provider.key_selector = keyed_select_id;
  provider.fn = keyed_window_stage;
  provider.ctx = probe;
  provider.store = store;
  provider.max_outputs = max_outputs;
  rc = turbo_flow_register_operation(flow, &input);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_operation(flow, &window);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_keyed_emitting_operation_provider(flow, &provider);
  if (rc != SALTS_OK) return rc;
  flow_test_operation_t operation_sink_1 =
      flow_test_operation_init("test.sink", keyed_window_sink, probe);
  operation_sink_1.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_sink_1.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  rc = flow_test_operation_register(flow, &operation_sink_1);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_parse_string(flow, dsl, strlen(dsl));
}

static int register_event_window_graph(turbo_flow_t *flow,
                                       turbo_flow_event_time_window_store_t *store,
                                       event_window_probe_t *probe, uint32_t max_outputs,
                                       const char *dsl) {
  turbo_flow_operation_descriptor_t input =
      keyed_operation_descriptor("data.input", TURBO_FLOW_OPERATION_SOURCE);
  turbo_flow_operation_descriptor_t window =
      keyed_operation_descriptor("data.event_window", TURBO_FLOW_OPERATION_STAGE);
  turbo_flow_event_time_window_provider_registration_t provider =
      TURBO_FLOW_EVENT_TIME_WINDOW_PROVIDER_REGISTRATION_INIT;
  int rc;

  window.scope.state = TURBO_FLOW_STATE_SCOPE_NODE;
  window.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  window.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  provider.operation_name = window.name;
  provider.key_selector = keyed_select_id;
  provider.on_event = event_window_accumulate;
  provider.on_close = event_window_close;
  provider.ctx = probe;
  provider.store = store;
  provider.max_outputs = max_outputs;
  rc = turbo_flow_register_operation(flow, &input);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_operation(flow, &window);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_event_time_window_provider(flow, &provider);
  if (rc != SALTS_OK) return rc;
  flow_test_operation_t operation_sink_2 =
      flow_test_operation_init("test.sink", event_window_sink, probe);
  operation_sink_2.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_sink_2.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  rc = flow_test_operation_register(flow, &operation_sink_2);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_parse_string(flow, dsl, strlen(dsl));
}

static turbo_flow_event_time_window_store_t *event_window_store_create(uint64_t window_size_ns,
                                                                        uint64_t lateness_ns) {
  turbo_flow_event_time_window_store_config_t config =
      TURBO_FLOW_EVENT_TIME_WINDOW_STORE_CONFIG_INIT;
  config.max_windows = 8u;
  config.max_key_size = sizeof(uint64_t);
  config.max_value_size = sizeof(uint64_t);
  config.max_total_bytes = 8u * (sizeof(uint64_t) * 3u);
  config.window_size_ns = window_size_ns;
  config.allowed_lateness_ns = lateness_ns;
  return turbo_flow_event_time_window_store_create(&config);
}

static turbo_flow_keyed_state_store_t *keyed_store_create(size_t max_entries) {
  turbo_flow_keyed_state_store_config_t config = TURBO_FLOW_KEYED_STATE_STORE_CONFIG_INIT;
  config.max_entries = max_entries;
  config.max_key_size = sizeof(uint64_t);
  config.max_value_size = sizeof(uint64_t);
  config.max_total_bytes = max_entries * (sizeof(uint64_t) * 2u);
  return turbo_flow_keyed_state_store_create(&config);
}

static int publish_id(turbo_flow_t *flow, uint64_t id) {
  turbo_flow_msg_t message;
  int rc;
  turbo_flow_msg_init(&message);
  message.id = id;
  rc = turbo_flow_publish(flow, "input", &message);
  turbo_flow_msg_cleanup(&message);
  return rc;
}

static int publish_event(turbo_flow_t *flow, uint64_t id, uint64_t timestamp_ns) {
  turbo_flow_msg_t message;
  int rc;
  turbo_flow_msg_init(&message);
  message.id = id;
  message.ts_ns = timestamp_ns;
  rc = turbo_flow_publish(flow, "input", &message);
  turbo_flow_msg_cleanup(&message);
  return rc;
}

suite("Turbo Flow keyed state") {
  static const char *linear_dsl = "source input operation data.input\n"
                                  "stage count operation data.count\n"
                                  "stage sink operation test.sink\n"
                                  "stage main {\n"
                                  "  input -> count -> sink\n"
                                  "}\n";

  it("isolates counters by selected binary key") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(4u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    check_not_null(store);
    check_not_null(flow);
    check_equal(register_keyed_graph(flow, store, &probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(publish_id(flow, 2u), SALTS_OK);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 3u);
    check_equal(probe.sink_values[0], 1u);
    check_equal(probe.sink_values[1], 1u);
    check_equal(probe.sink_values[2], 2u);
    check_equal(turbo_flow_keyed_state_store_size(store), 2u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(probe.sink_values[3], 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }

  it("rolls back a staged value when the callback fails") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(2u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.fail_after_put = 1;
    check_not_null(store);
    check_not_null(flow);
    check_equal(register_keyed_graph(flow, store, &probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 7u), SALTS_EIO);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 0u);
    probe.fail_after_put = 0;
    check_equal(publish_id(flow, 7u), SALTS_OK);
    check_equal(probe.sink_values[0], 1u);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }

  it("fails without mutation when the distinct-key bound is exhausted") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(1u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    check_not_null(store);
    check_not_null(flow);
    check_equal(register_keyed_graph(flow, store, &probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(publish_id(flow, 2u), SALTS_ENOSPC);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 2u), SALTS_OK);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }

  it("commits one of two concurrent updates to the same key") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(2u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_probe_t probe;
    keyed_publish_t publishes[2];
    salts_thread_t threads[2] = {NULL, NULL};
    int first_status;
    int second_status;
    memset(&probe, 0, sizeof(probe));
    probe.synchronize_callbacks = 1;
    check_not_null(store);
    check_not_null(flow);
    check_equal(register_keyed_graph(flow, store, &probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    for (size_t index = 0u; index < 2u; ++index) {
      publishes[index].flow = flow;
      publishes[index].id = 9u;
      atomic_init(&publishes[index].status, SALTS_EIO);
      check_equal(salts_thread_create(&threads[index], keyed_publish_thread, &publishes[index]),
                   SALTS_OK);
    }
    for (size_t index = 0u; index < 2u; ++index) {
      check_equal(salts_thread_join(&threads[index]), SALTS_OK);
    }
    first_status = atomic_load_explicit(&publishes[0].status, memory_order_acquire);
    second_status = atomic_load_explicit(&publishes[1].status, memory_order_acquire);
    check_true((first_status == SALTS_OK && second_status == SALTS_EBUSY) ||
               (first_status == SALTS_EBUSY && second_status == SALTS_OK));
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    probe.synchronize_callbacks = 0;
    check_equal(publish_id(flow, 9u), SALTS_OK);
    check_equal(probe.sink_values[1], 2u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }

  it("rejects incompatible state scope and duplicate node ownership") {
    static const char *duplicate_dsl = "source input operation data.input\n"
                                       "stage first operation data.count\n"
                                       "stage second operation data.count\n"
                                       "stage sink operation test.sink\n"
                                       "stage main {\n"
                                       "  input -> first -> second -> sink\n"
                                       "}\n";
    turbo_flow_keyed_state_store_t *scope_store = keyed_store_create(2u);
    turbo_flow_keyed_state_store_t *duplicate_store = keyed_store_create(2u);
    turbo_flow_t *scope_flow = turbo_flow_create();
    turbo_flow_t *duplicate_flow = turbo_flow_create();
    keyed_probe_t scope_probe;
    keyed_probe_t duplicate_probe;
    memset(&scope_probe, 0, sizeof(scope_probe));
    memset(&duplicate_probe, 0, sizeof(duplicate_probe));
    check_equal(register_keyed_graph(scope_flow, scope_store, &scope_probe,
                                      TURBO_FLOW_STATE_SCOPE_PRIVATE, linear_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(scope_flow), SALTS_ENOTSUP);
    check_contains(turbo_flow_last_error(scope_flow)->message, "node-local");
    check_equal(register_keyed_graph(duplicate_flow, duplicate_store, &duplicate_probe,
                                      TURBO_FLOW_STATE_SCOPE_NODE, duplicate_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(duplicate_flow), SALTS_EALREADY);
    check_contains(turbo_flow_last_error(duplicate_flow)->message, "one runtime node");
    turbo_flow_destroy(scope_flow);
    turbo_flow_destroy(duplicate_flow);
    turbo_flow_keyed_state_store_destroy(scope_store);
    turbo_flow_keyed_state_store_destroy(duplicate_store);
  }

  it("enforces one provider owner across flows and releases it on destroy") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(2u);
    turbo_flow_t *first = turbo_flow_create();
    turbo_flow_t *second = turbo_flow_create();
    keyed_probe_t first_probe;
    keyed_probe_t second_probe;
    memset(&first_probe, 0, sizeof(first_probe));
    memset(&second_probe, 0, sizeof(second_probe));
    check_equal(register_keyed_graph(first, store, &first_probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_OK);
    check_equal(register_keyed_graph(second, store, &second_probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_EALREADY);
    turbo_flow_destroy(first);
    turbo_flow_destroy(second);

    second = turbo_flow_create();
    check_equal(register_keyed_graph(second, store, &second_probe, TURBO_FLOW_STATE_SCOPE_NODE,
                                      linear_dsl),
                 SALTS_OK);
    turbo_flow_destroy(second);
    turbo_flow_keyed_state_store_destroy(store);
  }
}

suite("Turbo Flow keyed emitting state") {
  static const char *window_dsl = "source input operation data.input\n"
                                  "stage window operation data.window\n"
                                  "stage sink operation test.sink\n"
                                  "stage main {\n"
                                  "  input -> window -> sink\n"
                                  "}\n";

  it("releases one output for each completed keyed count window") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(4u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_window_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.window_size = 3u;
    probe.emit_count = 1u;
    check_equal(register_keyed_window_graph(flow, store, &probe, 1u, window_dsl), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(publish_id(flow, 2u), SALTS_OK);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(publish_id(flow, 1u), SALTS_OK);
    check_equal(publish_id(flow, 2u), SALTS_OK);
    check_equal(publish_id(flow, 2u), SALTS_OK);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 2u);
    check_equal(probe.sink_ids[0], 1u);
    check_equal(probe.sink_ids[1], 2u);
    check_equal(probe.sink_values[0], 3u);
    check_equal(probe.sink_values[1], 3u);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }

  it("rolls back state and all outputs when the emission bound is exceeded") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(2u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_window_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.window_size = 2u;
    probe.emit_count = 2u;
    check_equal(register_keyed_window_graph(flow, store, &probe, 1u, window_dsl), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 5u), SALTS_OK);
    check_equal(publish_id(flow, 5u), SALTS_ENOSPC);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 0u);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    probe.emit_count = 1u;
    check_equal(publish_id(flow, 5u), SALTS_OK);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    check_equal(probe.sink_values[0], 2u);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }

  it("discards the losing output batch on a concurrent state conflict") {
    turbo_flow_keyed_state_store_t *store = keyed_store_create(2u);
    turbo_flow_t *flow = turbo_flow_create();
    keyed_window_probe_t probe;
    keyed_publish_t publishes[2];
    salts_thread_t threads[2] = {NULL, NULL};
    int first_status;
    int second_status;
    memset(&probe, 0, sizeof(probe));
    probe.window_size = 2u;
    probe.emit_count = 1u;
    check_equal(register_keyed_window_graph(flow, store, &probe, 1u, window_dsl), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_id(flow, 8u), SALTS_OK);
    probe.synchronize_callbacks = 1;
    for (size_t index = 0u; index < 2u; ++index) {
      publishes[index].flow = flow;
      publishes[index].id = 8u;
      atomic_init(&publishes[index].status, SALTS_EIO);
      check_equal(salts_thread_create(&threads[index], keyed_publish_thread, &publishes[index]),
                   SALTS_OK);
    }
    for (size_t index = 0u; index < 2u; ++index) {
      check_equal(salts_thread_join(&threads[index]), SALTS_OK);
    }
    first_status = atomic_load_explicit(&publishes[0].status, memory_order_acquire);
    second_status = atomic_load_explicit(&publishes[1].status, memory_order_acquire);
    check_true((first_status == SALTS_OK && second_status == SALTS_EBUSY) ||
               (first_status == SALTS_EBUSY && second_status == SALTS_OK));
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_keyed_state_store_destroy(store);
  }
}

suite("Turbo Flow event-time tumbling windows") {
  static const char *event_window_dsl = "source input operation data.input\n"
                                        "stage window operation data.event_window\n"
                                        "stage sink operation test.sink\n"
                                        "stage main {\n"
                                        "  input -> window -> sink\n"
                                        "}\n";

  it("rejects invalid bounds and mismatched keyed store kinds") {
    turbo_flow_event_time_window_store_config_t invalid =
        TURBO_FLOW_EVENT_TIME_WINDOW_STORE_CONFIG_INIT;
    turbo_flow_keyed_state_store_t *keyed_store = keyed_store_create(2u);
    turbo_flow_event_time_window_store_t *event_store = event_window_store_create(10u, 0u);
    turbo_flow_t *event_flow = turbo_flow_create();
    turbo_flow_t *keyed_flow = turbo_flow_create();
    event_window_probe_t event_probe;
    keyed_probe_t keyed_probe;
    static const char *keyed_dsl = "source input operation data.input\n"
                                   "stage count operation data.count\n"
                                   "stage sink operation test.sink\n"
                                   "stage main {\n"
                                   "  input -> count -> sink\n"
                                   "}\n";
    memset(&event_probe, 0, sizeof(event_probe));
    memset(&keyed_probe, 0, sizeof(keyed_probe));
    invalid.window_size_ns = 0u;
    check_null(turbo_flow_event_time_window_store_create(&invalid));
    check_equal(register_event_window_graph(event_flow, keyed_store, &event_probe, 1u,
                                             event_window_dsl),
                 SALTS_EINVAL);
    check_equal(register_keyed_graph(keyed_flow, event_store, &keyed_probe,
                                      TURBO_FLOW_STATE_SCOPE_NODE, keyed_dsl),
                 SALTS_EINVAL);
    turbo_flow_destroy(event_flow);
    turbo_flow_destroy(keyed_flow);
    turbo_flow_keyed_state_store_destroy(keyed_store);
    turbo_flow_event_time_window_store_destroy(event_store);
  }

  it("closes keyed windows in timestamp and key order at the lateness boundary") {
    turbo_flow_event_time_window_store_t *store = event_window_store_create(10u, 5u);
    turbo_flow_t *flow = turbo_flow_create();
    event_window_probe_t probe;
    size_t closed = 99u;
    int initialized = 0;
    memset(&probe, 0, sizeof(probe));
    probe.close_emit_count = 1u;
    check_not_null(store);
    check_not_null(flow);
    check_equal(register_event_window_graph(flow, store, &probe, 1u, event_window_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_event(flow, 2u, 3u), SALTS_OK);
    check_equal(publish_event(flow, 1u, 8u), SALTS_OK);
    check_equal(publish_event(flow, 1u, 1u), SALTS_OK);
    check_equal(publish_event(flow, 1u, 12u), SALTS_OK);
    check_equal(turbo_flow_keyed_state_store_size(store), 3u);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 14u, &closed), SALTS_OK);
    check_equal(closed, 0u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 0u);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 15u, &closed), SALTS_OK);
    check_equal(closed, 2u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 2u);
    check_equal(probe.sink_ids[0], 1u);
    check_equal(probe.sink_ids[1], 2u);
    check_equal(probe.sink_values[0], 2u);
    check_equal(probe.sink_values[1], 1u);
    check_equal(probe.sink_timestamps[0], 0u);
    check_equal(probe.sink_timestamps[1], 0u);
    check_equal(publish_event(flow, 3u, 9u), SALTS_ETIMEDOUT);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 25u, &closed), SALTS_OK);
    check_equal(closed, 1u);
    check_equal(probe.sink_ids[2], 1u);
    check_equal(probe.sink_timestamps[2], 10u);
    check_equal(probe.sink_values[2], 1u);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 24u, &closed),
                 SALTS_EINVAL);
    check_equal(turbo_flow_event_time_window_watermark(store, &initialized), 25u);
    check_equal(initialized, 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }

  it("keeps aggregate state when close callback fails and retries an equal watermark") {
    turbo_flow_event_time_window_store_t *store = event_window_store_create(10u, 0u);
    turbo_flow_t *flow = turbo_flow_create();
    event_window_probe_t probe;
    size_t closed = 0u;
    memset(&probe, 0, sizeof(probe));
    probe.close_emit_count = 1u;
    probe.fail_close = 1;
    check_equal(register_event_window_graph(flow, store, &probe, 1u, event_window_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_event(flow, 7u, 4u), SALTS_OK);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_EIO);
    check_equal(closed, 0u);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 0u);
    check_equal(publish_event(flow, 7u, 4u), SALTS_ETIMEDOUT);
    probe.fail_close = 0;
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_OK);
    check_equal(closed, 1u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }

  it("keeps aggregate state when the close emission bound is exceeded") {
    turbo_flow_event_time_window_store_t *store = event_window_store_create(10u, 0u);
    turbo_flow_t *flow = turbo_flow_create();
    event_window_probe_t probe;
    size_t closed = 0u;
    memset(&probe, 0, sizeof(probe));
    probe.close_emit_count = 2u;
    check_equal(register_event_window_graph(flow, store, &probe, 1u, event_window_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_event(flow, 4u, 2u), SALTS_OK);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed),
                 SALTS_ENOSPC);
    check_equal(closed, 0u);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 0u);
    probe.close_emit_count = 1u;
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_OK);
    check_equal(closed, 1u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }

  it("rejects an in-flight event that crosses the committed watermark") {
    turbo_flow_event_time_window_store_t *store = event_window_store_create(10u, 0u);
    turbo_flow_t *flow = turbo_flow_create();
    event_window_probe_t probe;
    event_window_publish_t publish;
    salts_thread_t thread = NULL;
    size_t closed = 0u;
    memset(&probe, 0, sizeof(probe));
    memset(&publish, 0, sizeof(publish));
    probe.close_emit_count = 1u;
    probe.synchronize_event = 1;
    publish.flow = flow;
    publish.id = 6u;
    publish.timestamp_ns = 2u;
    atomic_init(&publish.status, SALTS_EIO);
    check_equal(register_event_window_graph(flow, store, &probe, 1u, event_window_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(salts_thread_create(&thread, event_window_publish_thread, &publish), SALTS_OK);
    while (!atomic_load_explicit(&probe.event_entered, memory_order_acquire)) {
      salts_thread_yield();
    }
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_OK);
    check_equal(closed, 0u);
    atomic_store_explicit(&probe.event_release, 1u, memory_order_release);
    check_equal(salts_thread_join(&thread), SALTS_OK);
    check_equal(atomic_load_explicit(&publish.status, memory_order_acquire), SALTS_ETIMEDOUT);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(atomic_load_explicit(&probe.close_count, memory_order_relaxed), 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }

  it("does not restore a closed window after downstream failure") {
    turbo_flow_event_time_window_store_t *store = event_window_store_create(10u, 0u);
    turbo_flow_t *flow = turbo_flow_create();
    event_window_probe_t probe;
    size_t closed = 0u;
    memset(&probe, 0, sizeof(probe));
    probe.close_emit_count = 1u;
    probe.fail_sink = 1;
    check_equal(register_event_window_graph(flow, store, &probe, 1u, event_window_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_event(flow, 3u, 5u), SALTS_OK);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_EIO);
    check_equal(closed, 1u);
    check_equal(turbo_flow_keyed_state_store_size(store), 0u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    probe.fail_sink = 0;
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_OK);
    check_equal(closed, 0u);
    check_equal(atomic_load_explicit(&probe.sink_count, memory_order_relaxed), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }

  it("resets aggregate state and watermark with the runtime generation") {
    turbo_flow_event_time_window_store_t *store = event_window_store_create(10u, 0u);
    turbo_flow_t *flow = turbo_flow_create();
    event_window_probe_t probe;
    size_t closed = 0u;
    int initialized = 1;
    memset(&probe, 0, sizeof(probe));
    probe.close_emit_count = 1u;
    check_equal(register_event_window_graph(flow, store, &probe, 1u, event_window_dsl),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(publish_event(flow, 9u, 3u), SALTS_OK);
    check_equal(turbo_flow_advance_event_time_watermark(flow, store, 10u, &closed), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_event_time_window_watermark(store, &initialized), 0u);
    check_equal(initialized, 0);
    check_equal(publish_event(flow, 9u, 3u), SALTS_OK);
    check_equal(turbo_flow_keyed_state_store_size(store), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }
}
