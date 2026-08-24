#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_coro.h"
#include "turbo_flow.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

static int noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static const turbo_flow_data_schema_t TEST_DATA_SCHEMA = {sizeof(turbo_flow_data_schema_t),
                                                          TURBO_FLOW_DOMAIN_DATA,
                                                          TURBO_FLOW_DATA_ENCODING_JSON,
                                                          "test.orders",
                                                          "Order",
                                                          "test.int",
                                                          17u,
                                                          1u,
                                                          "message Order { uint32 id; }"};

static const turbo_flow_data_schema_t TEST_HTTP_DATA_SCHEMA = {sizeof(turbo_flow_data_schema_t),
                                                               TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                                               TURBO_FLOW_DATA_ENCODING_JSON,
                                                               "test.http.orders",
                                                               "Order",
                                                               "test.int",
                                                               18u,
                                                               1u,
                                                               NULL};

static void destroy_owned_projection(void *ptr, void *ctx) {
  int *count = (int *)ctx;
  if (count) ++*count;
  free(ptr);
}

static int clone_owned_projection(const void *value, void *ctx, void **out) {
  int *copy;

  (void)ctx;
  if (!value || !out) return TURBO_EINVAL;
  copy = (int *)malloc(sizeof(*copy));
  if (!copy) return TURBO_ENOMEM;
  *copy = *(const int *)value;
  *out = copy;
  return TURBO_OK;
}

typedef struct publish_trace_s {
  int order[8];
  size_t count;
} publish_trace_t;

typedef struct observer_probe_s {
  size_t event_counts[TURBO_FLOW_OBSERVE_EVENT_COUNT];
  size_t selected_routes;
  int fail_kind;
  int destroy_count;
} observer_probe_t;

static int observer_probe_on_event(void *ctx, const turbo_flow_observe_event_t *event) {
  observer_probe_t *probe = (observer_probe_t *)ctx;
  check_not_null(event);
  check_equal(event->size, sizeof(*event));
  if (event->kind >= 0 && event->kind < TURBO_FLOW_OBSERVE_EVENT_COUNT) {
    probe->event_counts[event->kind] += 1u;
  }
  if (event->kind == TURBO_FLOW_OBSERVE_ROUTE_EVALUATED && event->selected == 1) {
    probe->selected_routes += 1u;
  }
  return event->kind == probe->fail_kind ? TURBO_EIO : TURBO_OK;
}

static void observer_probe_destroy(void *ctx) {
  observer_probe_t *probe = (observer_probe_t *)ctx;
  probe->destroy_count += 1;
}

typedef struct publish_stage_ctx_s {
  publish_trace_t *trace;
  int id;
  int fail_status;
} publish_stage_ctx_t;

typedef struct batch_publish_probe_s {
  uint64_t ids[8];
  size_t calls;
  uint64_t fail_id;
  int fail_status;
} batch_publish_probe_t;

typedef struct batch_prepare_probe_s {
  size_t calls;
  size_t fail_index;
  int fail_status;
} batch_prepare_probe_t;

typedef struct invalid_batch_payload_ctx_s {
  mem_buffer_t *buffer;
  const char *payload;
  size_t payload_len;
  size_t calls;
} invalid_batch_payload_ctx_t;

typedef struct payload_check_ctx_s {
  const char *expected;
  int called;
} payload_check_ctx_t;

typedef struct payload_replace_ctx_s {
  const char *replacement;
  int called;
} payload_replace_ctx_t;

typedef struct coro_check_ctx_s {
  int entered;
  int resumed;
} coro_check_ctx_t;

typedef struct coro_wait_ctx_s {
  int calls;
} coro_wait_ctx_t;

typedef struct adapter_ctx_s {
  int start_count;
  int consume_count;
  int stop_count;
  int shutdown_count;
  int source_start_count;
  int stage_start_count;
  int fail_status;
  int command_count;
  turbo_flow_adapter_command_kind_t last_command;
  const char *expected_payload;
} adapter_ctx_t;

typedef struct failure_check_ctx_s {
  const char *stage_name;
  const char *adapter_name;
  const char *route_name;
  int code;
  int called;
  turbo_flow_msg_t *snapshot;
  const char *expected_payload;
  uint32_t expected_attempt;
} failure_check_ctx_t;

typedef struct retry_adapter_ctx_s {
  uint32_t attempts;
  uint32_t waits;
  uint32_t succeed_on;
  int failure_status;
  int wait_status;
} retry_adapter_ctx_t;

typedef struct worker_probe_ctx_s {
  atomic_int active;
  atomic_int peak;
  atomic_int calls;
  atomic_int ran_off_submitter;
} worker_probe_ctx_t;

typedef struct worker_submit_ctx_s {
  flow_worker_pool_adapter_t *adapter;
  atomic_int result;
} worker_submit_ctx_t;

typedef struct execution_probe_ctx_s {
  atomic_int entered;
  atomic_int allow_exit;
  atomic_int saw_cancel;
  int yields;
} execution_probe_ctx_t;

typedef struct concurrent_publish_s {
  turbo_flow_t *flow;
  const char *source_name;
  uint64_t msg_id;
  atomic_int *ready;
  atomic_int *go;
  atomic_int result;
  int error_code;
  char error_message[128];
} concurrent_publish_t;

typedef struct stop_flow_ctx_s {
  turbo_flow_t *flow;
  atomic_int result;
} stop_flow_ctx_t;

typedef struct async_completion_ctx_s {
  atomic_int called;
  atomic_int last_status;
} async_completion_ctx_t;

typedef struct async_gate_ctx_s {
  atomic_int entered;
  atomic_int allow_exit;
  atomic_int calls;
  atomic_int ran_off_submitter;
} async_gate_ctx_t;

static TURBO_THREAD_LOCAL int worker_submitter_thread;

static int async_gate_stage(turbo_flow_msg_t *msg, void *ctx) {
  async_gate_ctx_t *gate = (async_gate_ctx_t *)ctx;
  (void)msg;
  if (!worker_submitter_thread) {
    atomic_store_explicit(&gate->ran_off_submitter, 1, memory_order_release);
  }
  atomic_fetch_add_explicit(&gate->entered, 1, memory_order_acq_rel);
  while (!atomic_load_explicit(&gate->allow_exit, memory_order_acquire))
    turbo_thread_yield();
  atomic_fetch_add_explicit(&gate->calls, 1, memory_order_acq_rel);
  return TURBO_OK;
}

static void async_publish_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  async_completion_ctx_t *completion = (async_completion_ctx_t *)ctx;
  atomic_store_explicit(&completion->last_status, result ? result->status : TURBO_EINVAL,
                        memory_order_release);
  atomic_fetch_add_explicit(&completion->called, 1, memory_order_acq_rel);
}

static int execution_yield_stage(turbo_flow_msg_t *msg, void *ctx) {
  execution_probe_ctx_t *probe = (execution_probe_ctx_t *)ctx;
  (void)msg;
  atomic_store_explicit(&probe->entered, 1, memory_order_release);
  for (int i = 0; i < probe->yields; ++i) {
    int rc = turbo_flow_execution_yield();
    if (rc == TURBO_ECANCELED) {
      atomic_store_explicit(&probe->saw_cancel, 1, memory_order_release);
      return rc;
    }
    if (rc != TURBO_OK) return rc;
  }
  while (!atomic_load_explicit(&probe->allow_exit, memory_order_acquire)) {
    int rc = turbo_flow_execution_yield();
    if (rc == TURBO_ECANCELED) {
      atomic_store_explicit(&probe->saw_cancel, 1, memory_order_release);
      return rc;
    }
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int execution_self_abort_stage(turbo_flow_msg_t *msg, void *ctx) {
  int *called = (int *)ctx;
  (void)msg;
  *called += 1;
  if (turbo_flow_execution_abort() != TURBO_OK) return TURBO_EINVAL;
  if (!turbo_flow_execution_cancel_requested()) return TURBO_EINVAL;
  return turbo_flow_execution_yield();
}

static void execution_task_thread(void *arg) {
  flow_execution_task_run((flow_execution_task_t *)arg);
}

static void execution_task_coro(coro_t *co, void *arg) {
  (void)co;
  flow_execution_task_run((flow_execution_task_t *)arg);
}

static int worker_probe_stage(turbo_flow_msg_t *msg, void *ctx) {
  worker_probe_ctx_t *probe = (worker_probe_ctx_t *)ctx;
  int active;
  int peak;
  (void)msg;

  if (!worker_submitter_thread) {
    atomic_store_explicit(&probe->ran_off_submitter, 1, memory_order_release);
  }
  active = atomic_fetch_add_explicit(&probe->active, 1, memory_order_acq_rel) + 1;
  peak = atomic_load_explicit(&probe->peak, memory_order_acquire);
  while (active > peak &&
         !atomic_compare_exchange_weak_explicit(&probe->peak, &peak, active, memory_order_acq_rel,
                                                memory_order_acquire)) {
  }
  turbo_sleep_ms(25);
  atomic_fetch_sub_explicit(&probe->active, 1, memory_order_acq_rel);
  atomic_fetch_add_explicit(&probe->calls, 1, memory_order_acq_rel);
  return TURBO_OK;
}

static void worker_submit_thread(void *arg) {
  worker_submit_ctx_t *submit = (worker_submit_ctx_t *)arg;
  turbo_flow_msg_t msg;
  flow_stage_completion_t completion = {0};
  int rc;

  turbo_flow_msg_init(&msg);
  memset(&completion, 0, sizeof(completion));
  completion.entry = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
  completion.entry.runtime_generation = submit->adapter->flow->runtime_generation;
  completion.entry.stage_index = submit->adapter->stage_index;
  completion.entry.segment_kind = FLOW_DATA_SEGMENT_WORKER_POOL;
  completion.entry.ownership = FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE;
  completion.entry.sequence = 1u;
  completion.entry.message_id = msg.id;
  completion.entry.completion_handle = &completion;
  worker_submitter_thread = 1;
  rc = flow_worker_pool_submit(submit->adapter, &msg, &completion);
  worker_submitter_thread = 0;
  turbo_flow_msg_cleanup(&msg);
  atomic_store_explicit(&submit->result, rc, memory_order_release);
}

static void concurrent_publish_thread(void *arg) {
  concurrent_publish_t *publish = (concurrent_publish_t *)arg;
  turbo_flow_msg_t msg;
  const turbo_flow_error_t *error;
  int rc;

  turbo_flow_msg_init(&msg);
  msg.id = publish->msg_id;
  atomic_fetch_add_explicit(publish->ready, 1, memory_order_acq_rel);
  while (!atomic_load_explicit(publish->go, memory_order_acquire))
    turbo_thread_yield();
  rc = turbo_flow_publish(publish->flow, publish->source_name, &msg);
  error = turbo_flow_last_error(publish->flow);
  publish->error_code = error ? error->code : TURBO_EINVAL;
  snprintf(publish->error_message, sizeof(publish->error_message), "%s",
           error ? error->message : "missing error");
  atomic_store_explicit(&publish->result, rc, memory_order_release);
  turbo_flow_msg_cleanup(&msg);
}

static void stop_flow_thread(void *arg) {
  stop_flow_ctx_t *stop = (stop_flow_ctx_t *)arg;
  atomic_store_explicit(&stop->result, turbo_flow_stop(stop->flow), memory_order_release);
}

static int fail_by_message_id_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)ctx;
  turbo_sleep_ms(10);
  return msg->id == 1u ? TURBO_EIO : TURBO_EPROTO;
}

static int record_stage(turbo_flow_msg_t *msg, void *ctx) {
  publish_stage_ctx_t *stage = (publish_stage_ctx_t *)ctx;
  (void)msg;
  if (stage->trace->count < sizeof(stage->trace->order) / sizeof(stage->trace->order[0])) {
    stage->trace->order[stage->trace->count++] = stage->id;
  }
  return stage->fail_status;
}

static int batch_publish_probe_stage(turbo_flow_msg_t *msg, void *ctx) {
  batch_publish_probe_t *probe = (batch_publish_probe_t *)ctx;
  if (!probe || !msg) return TURBO_EINVAL;
  if (probe->calls < sizeof(probe->ids) / sizeof(probe->ids[0])) {
    probe->ids[probe->calls] = msg->id;
  }
  probe->calls += 1u;
  return probe->fail_id != 0u && msg->id == probe->fail_id ? probe->fail_status : TURBO_OK;
}

static int batch_prepare_message(void *ctx, size_t index, turbo_flow_msg_t *message) {
  batch_prepare_probe_t *probe = (batch_prepare_probe_t *)ctx;
  if (!probe || !message) return TURBO_EINVAL;
  probe->calls += 1u;
  if (index == probe->fail_index) return probe->fail_status;
  message->id = index + 1u;
  return TURBO_OK;
}

static int batch_prepare_invalid_payload(void *ctx, size_t index, turbo_flow_msg_t *message) {
  invalid_batch_payload_ctx_t *invalid = (invalid_batch_payload_ctx_t *)ctx;
  (void)index;
  if (!invalid || !message) return TURBO_EINVAL;
  invalid->calls += 1u;
  message->buffer = mem_buffer_retain(invalid->buffer);
  message->payload = vstr_from_buf(invalid->payload, invalid->payload_len);
  return message->buffer ? TURBO_OK : TURBO_ENOMEM;
}

static int set_flags_stage(turbo_flow_msg_t *msg, void *ctx) {
  msg->flags = *(const uint32_t *)ctx;
  return TURBO_OK;
}

static int check_failure_stage(turbo_flow_msg_t *msg, void *ctx) {
  failure_check_ctx_t *check = (failure_check_ctx_t *)ctx;
  check_equal(msg->failure.stage_name, check->stage_name);
  if (check->adapter_name) {
    check_equal(msg->failure.adapter_name, check->adapter_name);
  } else {
    check_null(msg->failure.adapter_name);
  }
  check_equal(msg->failure.route_name, check->route_name);
  check_equal(msg->failure.code, check->code);
  check_equal(msg->failure.attempt, check->expected_attempt > 0u ? check->expected_attempt : 1u);
  check_equal(msg->status, check->code);
  if (check->expected_payload) check_equal(msg->payload.data, check->expected_payload);
  if (check->snapshot) check_equal(turbo_flow_msg_clone(check->snapshot, msg), TURBO_OK);
  check->called += 1;
  return TURBO_OK;
}

static int retry_replace_payload(turbo_flow_msg_t *msg, const char *text) {
  tstr payload = tstr_dup(text);
  if (!payload) return TURBO_ENOMEM;
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
  return TURBO_OK;
}

static int retry_adapter_attempt(void *ctx, turbo_flow_msg_t *msg, uint32_t attempt) {
  retry_adapter_ctx_t *retry = (retry_adapter_ctx_t *)ctx;
  check_equal(msg->payload.data, "original");
  check_equal(attempt, retry->attempts + 1u);
  retry->attempts += 1u;
  if (retry->succeed_on > 0u && attempt == retry->succeed_on) {
    return retry_replace_payload(msg, "success");
  }
  check_equal(retry_replace_payload(msg, "failed-attempt"), TURBO_OK);
  return retry->failure_status;
}

static int retry_adapter_retryable(void *ctx, int status) {
  retry_adapter_ctx_t *retry = (retry_adapter_ctx_t *)ctx;
  return status == retry->failure_status;
}

static int retry_adapter_wait(void *ctx, uint32_t delay_ms) {
  retry_adapter_ctx_t *retry = (retry_adapter_ctx_t *)ctx;
  check_greater(delay_ms, 0);
  retry->waits += 1u;
  return retry->wait_status;
}

static int retry_adapter_consume(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  retry_adapter_ctx_t *retry = (retry_adapter_ctx_t *)ctx;
  (void)flow;
  (void)stage;
  (void)msg;
  return retry->failure_status;
}

static int retry_adapter_consume_retry(void *ctx, turbo_flow_t *flow,
                                       const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg,
                                       const turbo_flow_retry_policy_t *policy) {
  turbo_flow_retry_ops_t ops;
  (void)flow;
  check_not_null(stage);
  check_equal(stage->retry.max_attempts, policy->max_attempts);
  memset(&ops, 0, sizeof(ops));
  ops.size = sizeof(ops);
  ops.attempt = retry_adapter_attempt;
  ops.retryable = retry_adapter_retryable;
  ops.wait = retry_adapter_wait;
  return turbo_flow_retry_execute(policy, msg, &ops, ctx);
}

static int failure_adapter_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  (void)flow;
  check_not_null(stage);
  return check_failure_stage(msg, ctx);
}

static int check_payload_stage(turbo_flow_msg_t *msg, void *ctx) {
  payload_check_ctx_t *check = (payload_check_ctx_t *)ctx;
  check->called += 1;
  check_equal(msg->payload.data, check->expected);
  return TURBO_OK;
}

static int replace_payload_stage(turbo_flow_msg_t *msg, void *ctx) {
  payload_replace_ctx_t *replace = (payload_replace_ctx_t *)ctx;
  tstr owned = tstr_dup(replace->replacement);

  check_not_null(owned);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = owned;
  msg->payload = tstr_to_v(msg->owned_payload);
  replace->called += 1;
  return TURBO_OK;
}

static int coro_check_stage(turbo_flow_msg_t *msg, void *ctx) {
  coro_check_ctx_t *check = (coro_check_ctx_t *)ctx;
  (void)msg;
  check_not_null(coro_current_scheduler());
  check->entered += 1;
  check_equal(coro_yield(), 0);
  check_not_null(coro_current_scheduler());
  check->resumed += 1;
  return TURBO_OK;
}

static int coro_wait_once_stage(turbo_flow_msg_t *msg, void *ctx) {
  coro_wait_ctx_t *wait = (coro_wait_ctx_t *)ctx;
  (void)msg;

  check_not_null(coro_current_scheduler());
  wait->calls += 1;
  if (wait->calls == 1) {
    coro_set_waiting_for_io(coro_running(), 1);
    check_equal(coro_yield(), 0);
  }
  return TURBO_OK;
}

static int set_msg_status_stage(turbo_flow_msg_t *msg, void *ctx) {
  int *status = (int *)ctx;
  msg->status = *status;
  return TURBO_OK;
}

static int check_msg_status_stage(turbo_flow_msg_t *msg, void *ctx) {
  int *expected = (int *)ctx;
  check_equal(msg->status, *expected);
  return TURBO_OK;
}

static int test_adapter_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  adapter_ctx_t *adapter = (adapter_ctx_t *)ctx;
  (void)flow;

  check_not_null(stage);
  check_not_null(stage->adapter_name);
  if (adapter->fail_status != TURBO_OK) return adapter->fail_status;

  adapter->start_count += 1;
  if (stage->is_source) adapter->source_start_count += 1;
  else adapter->stage_start_count += 1;
  return TURBO_OK;
}

static int test_adapter_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                                turbo_flow_msg_t *msg) {
  adapter_ctx_t *adapter = (adapter_ctx_t *)ctx;
  (void)flow;

  check_not_null(stage);
  check_false(stage->is_source);
  check_not_null(msg);
  if (adapter->expected_payload) check_equal(msg->payload.data, adapter->expected_payload);
  if (adapter->fail_status != TURBO_OK) return adapter->fail_status;

  adapter->consume_count += 1;
  return TURBO_OK;
}

static void test_adapter_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  adapter_ctx_t *adapter = (adapter_ctx_t *)ctx;
  (void)flow;
  check_not_null(stage);
  adapter->stop_count += 1;
}

static void test_adapter_shutdown(void *ctx) {
  adapter_ctx_t *adapter = (adapter_ctx_t *)ctx;
  adapter->shutdown_count += 1;
}

static int test_adapter_command(void *ctx, turbo_flow_t *flow,
                                const turbo_flow_adapter_command_t *command) {
  adapter_ctx_t *adapter = (adapter_ctx_t *)ctx;
  (void)flow;
  adapter->command_count += 1;
  adapter->last_command = command->kind;
  return adapter->fail_status;
}

static void register_stage_names(turbo_flow_t *flow, const char *const *names, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    check_equal(turbo_flow_register_stage_ex(flow, names[i], noop_stage, NULL, NULL), TURBO_OK);
  }
}

static void register_schema_adapter(turbo_flow_t *flow, const char *name,
                                    turbo_flow_adapter_kind_t kind, uint32_t roles,
                                    turbo_flow_adapter_direction_t direction) {
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  memset(&ops, 0, sizeof(ops));
  ops.consume = test_adapter_consume;
  memset(&schema, 0, sizeof(schema));
  schema.kind = kind;
  schema.roles = roles;
  schema.direction = direction;
  check_equal(turbo_flow_register_adapter_ex(flow, name, &ops, NULL, &schema), TURBO_OK);
}

typedef struct reorder_wait_s {
  turbo_flow_t *flow;
  uint32_t stage_index;
  uint64_t sequence;
  atomic_int started;
  atomic_int result;
  int leave_on_success;
} reorder_wait_t;

static void reorder_wait_thread(void *arg) {
  reorder_wait_t *wait = (reorder_wait_t *)arg;
  atomic_store_explicit(&wait->started, 1, memory_order_release);
  int rc = flow_reorder_enter(wait->flow, wait->stage_index, wait->sequence);
  atomic_store_explicit(&wait->result, rc, memory_order_release);
  if (rc == TURBO_OK && wait->leave_on_success)
    flow_reorder_leave(wait->flow, wait->stage_index, wait->sequence);
}

static turbo_flow_t *reorder_test_flow(uint32_t capacity, uint32_t timeout_ms,
                                       uint32_t *stage_index) {
  char source[256];
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow ||
      snprintf(source, sizeof(source),
               "source input\nstage ordered reorder capacity %u timeout %u\n"
               "stage main {\n  input -> ordered\n}\n",
               capacity, timeout_ms) < 0 ||
      turbo_flow_parse_string(flow, source, strlen(source)) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "ordered", noop_stage, NULL, NULL) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK || turbo_flow_start(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  *stage_index = (uint32_t)turbo_flow_find_stage(flow, "ordered");
  return flow;
}

suite("Turbo Flow") {
  group("DSL parser") {
    it("parses declarations and dependency shorthand into stage and edge plans") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage validate\n"
                               "stage enrich worker 4 exec thread workers 8\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input --> parse\n"
                               "  parse -> [validate, enrich] -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage = NULL;
      const turbo_flow_edge_plan_t *edge = NULL;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_PARSED);
      check_equal(turbo_flow_stage_count(flow), 5);
      check_equal(turbo_flow_edge_count(flow), 5);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "input"));
      check_not_null(stage);
      check_true(stage->is_source);
      check_equal(stage->data_strategy, TURBO_FLOW_DATA_BROADCAST);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_INLINE);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "enrich"));
      check_not_null(stage);
      check_false(stage->is_source);
      check_equal(stage->data_strategy, TURBO_FLOW_DATA_WORKER_POOL);
      check_equal(stage->data_worker_count, 4);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_THREAD_POOL);
      check_equal(stage->exec.workers, 8);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "sink"));
      check_not_null(stage);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_INLINE);

      edge = turbo_flow_edge_at(flow, 0);
      check_not_null(edge);
      check_equal(edge->from_stage, UINT32_MAX);
      check_equal(edge->to_stage, UINT32_MAX);

      turbo_flow_destroy(flow);
    }

    it("rejects removed graph, subgraph, and flow block spellings") {
      static const char *graph_src = "source input\n"
                                     "stage parse\n"
                                     "graph main {\n"
                                     "  input -> parse\n"
                                     "}\n";
      static const char *subgraph_src = "subgraph clean {\n"
                                        "  in input\n"
                                        "  out output\n"
                                        "  step trim\n"
                                        "  input -> trim -> output\n"
                                        "}\n";
      static const char *flow_src = "source input\n"
                                    "stage parse\n"
                                    "flow"
                                    " main {\n"
                                    "  input -> parse\n"
                                    "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, graph_src, strlen(graph_src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "syntax");
      check_equal(turbo_flow_parse_string(flow, subgraph_src, strlen(subgraph_src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "syntax");
      check_equal(turbo_flow_parse_string(flow, flow_src, strlen(flow_src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "syntax");

      turbo_flow_destroy(flow);
    }

    it("accepts source and step declarations inside the root stage") {
      static const char *src = "stage main {\n"
                               "  source input\n"
                               "  step parse\n"
                               "  input -> parse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 2);
      check_equal(turbo_flow_edge_count(flow), 1);
      check_true(
          turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "input"))->is_source);
      check_true(turbo_flow_find_stage(flow, "parse") >= 0);

      turbo_flow_destroy(flow);
    }

    it("accepts reusable stage definitions with ports") {
      static const char *src = "stage clean {\n"
                               "  in input\n"
                               "  out output\n"
                               "  step trim\n"
                               "  input -> trim -> output\n"
                               "}\n"
                               "stage main {\n"
                               "  source inbound\n"
                               "  step sink\n"
                               "  inbound -> clean -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 5);
      check_equal(turbo_flow_edge_count(flow), 4);
      check_true(turbo_flow_find_stage(flow, "clean.input") >= 0);
      check_true(turbo_flow_find_stage(flow, "clean.trim") >= 0);
      check_true(turbo_flow_find_stage(flow, "clean.output") >= 0);

      turbo_flow_destroy(flow);
    }

    it("accepts stage main with step declarations") {
      static const char *src = "stage main {\n"
                               "  source raw\n"
                               "  step normalize\n"
                               "  step record adapter sqlite.orders\n"
                               "  raw -> normalize -> record\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *record = NULL;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 3);
      check_equal(turbo_flow_edge_count(flow), 2);
      check_true(turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "raw"))->is_source);
      check_true(turbo_flow_find_stage(flow, "normalize") >= 0);
      record = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "record"));
      check_not_null(record);
      check_equal(record->adapter_name, "sqlite.orders");

      turbo_flow_destroy(flow);
    }

    it("accepts reusable stage blocks with ports and steps") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  step load\n"
                               "  input -> cleanse -> load\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 5);
      check_equal(turbo_flow_edge_count(flow), 4);
      check_true(turbo_flow_find_stage(flow, "cleanse.raw") >= 0);
      check_true(turbo_flow_find_stage(flow, "cleanse.trim") >= 0);
      check_true(turbo_flow_find_stage(flow, "cleanse.clean") >= 0);

      turbo_flow_destroy(flow);
    }

    it("instantiates reusable stages with use aliases") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  step load\n"
                               "  use c = cleanse\n"
                               "  input -> c -> load\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 8);
      check_equal(turbo_flow_edge_count(flow), 6);
      check_true(turbo_flow_find_stage(flow, "cleanse.trim") >= 0);
      check_true(turbo_flow_find_stage(flow, "c.raw") >= 0);
      check_true(turbo_flow_find_stage(flow, "c.trim") >= 0);
      check_true(turbo_flow_find_stage(flow, "c.clean") >= 0);

      turbo_flow_destroy(flow);
    }

    it("rejects unresolved profiles before host expansion") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  use c = cleanse with orders_us\n"
                               "  input -> c\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "syntax");

      turbo_flow_destroy(flow);
    }

    it("expands nested stage uses into finite scoped plans") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage pipeline {\n"
                               "  in raw\n"
                               "  out done\n"
                               "  use c = cleanse\n"
                               "  step encode\n"
                               "  raw -> c -> encode -> done\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  step sink\n"
                               "  use p = pipeline\n"
                               "  input -> p -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 17);
      check_equal(turbo_flow_edge_count(flow), 14);
      check_true(turbo_flow_find_stage(flow, "pipeline.c.trim") >= 0);
      check_true(turbo_flow_find_stage(flow, "p.raw") >= 0);
      check_true(turbo_flow_find_stage(flow, "p.c.trim") >= 0);
      check_true(turbo_flow_find_stage(flow, "p.encode") >= 0);
      check_true(turbo_flow_find_stage(flow, "p.done") >= 0);

      turbo_flow_destroy(flow);
    }

    it("rejects unknown reusable stage use targets") {
      static const char *src = "stage main {\n"
                               "  source input\n"
                               "  use c = missing\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "unknown reusable stage");

      turbo_flow_destroy(flow);
    }

    it("rejects use aliases that collide with existing stages") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage main {\n"
                               "  step c\n"
                               "  use c = cleanse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate");

      turbo_flow_destroy(flow);
    }

    it("rejects flow as a root orchestration spelling") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "flow"
                               " main {\n"
                               "  input -> parse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "syntax");

      turbo_flow_destroy(flow);
    }

    it("rejects unknown executor kinds and ambiguous groups") {
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_error_t *err = NULL;
      static const char *bad_exec = "stage parse exec nowhere\n";
      static const char *bad_group = "source input\n"
                                     "stage a\n"
                                     "stage b\n"
                                     "stage main {\n"
                                     "  input -> [a b]\n"
                                     "}\n";

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, bad_exec, strlen(bad_exec)), TURBO_EINVAL);
      err = turbo_flow_last_error(flow);
      check_not_null(err);
      check_equal(err->line, 1);
      check_contains(err->message, "unknown executor");

      check_equal(turbo_flow_parse_string(flow, bad_group, strlen(bad_group)), TURBO_EINVAL);
      err = turbo_flow_last_error(flow);
      check_not_null(err);
      check_equal(err->line, 5);
      check_contains(err->message, "syntax");

      turbo_flow_destroy(flow);
    }

    it("rejects declarations after orchestration starts") {
      static const char *src = "source input\n"
                               "stage main {\n"
                               "}\n"
                               "stage late\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "root block");

      turbo_flow_destroy(flow);
    }

    it("stores coro executor lane and pool options in stage IR") {
      static const char *src = "stage async exec coro lanes 2 pool 16\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage = NULL;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "async"));
      check_not_null(stage);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_CORO_POOL);
      check_equal(stage->exec.lanes, 2);
      check_equal(stage->exec.pool_capacity, 16);
      check_equal(stage->exec.workers, 0);

      turbo_flow_destroy(flow);
    }

    it("accepts bounded worker capacity before or after the worker option") {
      static const char *worker_first = "stage enrich worker 4 capacity 64\n";
      static const char *capacity_first = "stage enrich capacity 128 worker 2\n";
      turbo_flow_t *flow = turbo_flow_create();
      const flow_stage_plan_impl_t *stage;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, worker_first, strlen(worker_first)), TURBO_OK);
      stage = (const flow_stage_plan_impl_t *)vec_at_const(
          &flow->stages, (size_t)turbo_flow_find_stage(flow, "enrich"));
      check_not_null(stage);
      check_equal(stage->data_worker_count, 4);
      check_equal(stage->data_pool_capacity, 64);

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, capacity_first, strlen(capacity_first)), TURBO_OK);
      stage = (const flow_stage_plan_impl_t *)vec_at_const(
          &flow->stages, (size_t)turbo_flow_find_stage(flow, "enrich"));
      check_not_null(stage);
      check_equal(stage->data_worker_count, 2);
      check_equal(stage->data_pool_capacity, 128);

      turbo_flow_destroy(flow);
    }

    it("maps the three compute executors to their runtime kinds") {
      static const char *src = "stage cpu exec thread workers 3\n"
                               "stage async exec coro lanes 2 pool 16\n"
                               "stage rules exec inline\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage = NULL;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "cpu"));
      check_not_null(stage);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_THREAD_POOL);
      check_equal(stage->exec.workers, 3);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "async"));
      check_not_null(stage);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_CORO_POOL);
      check_equal(stage->exec.lanes, 2);
      check_equal(stage->exec.pool_capacity, 16);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "rules"));
      check_not_null(stage);
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_INLINE);

      turbo_flow_destroy(flow);
    }

    it("rejects former executor spellings") {
      static const char *legacy_threadpool = "stage cpu exec threadpool\n";
      static const char *legacy_thread_pool = "stage cpu exec thread_pool\n";
      static const char *legacy_coro_pool = "stage async exec coro_pool\n";
      static const char *legacy_coronet = "stage proxy exec coronet\n";
      static const char *legacy_socks = "stage proxy exec socks\n";
      static const char *removed_socket = "stage proxy exec socket\n";
      static const char *removed_io = "stage proxy exec io\n";
      static const char *removed_custom = "stage proxy exec custom host\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, legacy_threadpool, strlen(legacy_threadpool)),
                   TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, legacy_thread_pool, strlen(legacy_thread_pool)),
                   TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, legacy_coro_pool, strlen(legacy_coro_pool)),
                   TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, legacy_coronet, strlen(legacy_coronet)),
                   TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, legacy_socks, strlen(legacy_socks)), TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, removed_socket, strlen(removed_socket)),
                   TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, removed_io, strlen(removed_io)), TURBO_EINVAL);
      check_equal(turbo_flow_parse_string(flow, removed_custom, strlen(removed_custom)),
                   TURBO_EINVAL);

      turbo_flow_destroy(flow);
    }

    it("stores source and sink adapter bindings in stage IR") {
      static const char *src = "source http_in adapter http.server\n"
                               "stage proxy adapter socket.server\n"
                               "stage email_out adapter smtp exec thread workers 2\n"
                               "stage main {\n"
                               "  http_in -> proxy -> email_out\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage = NULL;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "http_in"));
      check_not_null(stage);
      check_true(stage->is_source);
      check_equal(stage->adapter_name, "http.server");

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "proxy"));
      check_not_null(stage);
      check_false(stage->is_source);
      check_equal(stage->adapter_name, "socket.server");
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_INLINE);

      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "email_out"));
      check_not_null(stage);
      check_equal(stage->adapter_name, "smtp");
      check_equal(stage->exec.kind, TURBO_FLOW_EXEC_THREAD_POOL);
      check_equal(stage->exec.workers, 2);

      turbo_flow_destroy(flow);
    }

    it("parses multi-segment adapter binding names") {
      static const char *src = "source input adapter http.client.poll\n"
                               "stage decode adapter codec.json.in\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "input"));
      check_not_null(stage);
      check_equal(stage->adapter_name, "http.client.poll");
      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "decode"));
      check_not_null(stage);
      check_equal(stage->adapter_name, "codec.json.in");
      turbo_flow_destroy(flow);
    }

    it("rejects whitespace inside dotted bindings and duplicate declaration options") {
      static const char *bad_dotted = "stage call adapter http . client\n";
      static const char *duplicate_adapter = "stage call adapter http adapter rpc\n";
      static const char *duplicate_worker = "stage call worker 2 worker 4\n";
      static const char *duplicate_capacity = "stage call worker 2 capacity 64 capacity 128\n";
      static const char *duplicate_exec = "stage call exec inline exec thread\n";
      static const char *duplicate_workers = "stage call exec thread workers 2 workers 4\n";
      static const char *duplicate_lanes = "stage call exec coro lanes 2 lanes 4\n";
      static const char *duplicate_pool = "stage call exec coro pool 8 pool 16\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, bad_dotted, strlen(bad_dotted)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "whitespace");
      check_equal(turbo_flow_parse_string(flow, duplicate_adapter, strlen(duplicate_adapter)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate adapter");
      check_equal(turbo_flow_parse_string(flow, duplicate_worker, strlen(duplicate_worker)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate worker");
      check_equal(turbo_flow_parse_string(flow, duplicate_capacity, strlen(duplicate_capacity)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate data pool");
      check_equal(turbo_flow_parse_string(flow, duplicate_exec, strlen(duplicate_exec)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate exec");
      check_equal(turbo_flow_parse_string(flow, duplicate_workers, strlen(duplicate_workers)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate workers");
      check_equal(turbo_flow_parse_string(flow, duplicate_lanes, strlen(duplicate_lanes)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate lanes");
      check_equal(turbo_flow_parse_string(flow, duplicate_pool, strlen(duplicate_pool)),
                   TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate pool");

      turbo_flow_destroy(flow);
    }

    it("rejects zero worker and executor counts") {
      static const char *bad_worker = "stage parse worker 0\n";
      static const char *bad_exec_workers = "stage parse exec thread workers 0\n";
      static const char *bad_coro_lanes = "stage fetch exec coro lanes 0\n";
      static const char *bad_coro_pool_capacity = "stage fetch exec coro pool 0\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, bad_worker, strlen(bad_worker)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "greater than zero");
      check_equal(turbo_flow_parse_string(flow, bad_exec_workers, strlen(bad_exec_workers)),
                   TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "greater than zero");
      check_equal(turbo_flow_parse_string(flow, bad_coro_lanes, strlen(bad_coro_lanes)),
                   TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "greater than zero");
      check_equal(
          turbo_flow_parse_string(flow, bad_coro_pool_capacity, strlen(bad_coro_pool_capacity)),
          TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "greater than zero");

      turbo_flow_destroy(flow);
    }

    it("rejects invalid or unowned worker capacities") {
      static const char *zero = "stage parse worker 2 capacity 0\n";
      static const char *not_power_of_two = "stage parse worker 2 capacity 3\n";
      static const char *too_large = "stage parse worker 2 capacity 2097152\n";
      static const char *without_worker = "stage parse capacity 64\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, zero, strlen(zero)), TURBO_ERANGE);
      check_contains(turbo_flow_last_error(flow)->message, "power of two");
      check_equal(turbo_flow_parse_string(flow, not_power_of_two, strlen(not_power_of_two)),
                   TURBO_ERANGE);
      check_contains(turbo_flow_last_error(flow)->message, "power of two");
      check_equal(turbo_flow_parse_string(flow, too_large, strlen(too_large)), TURBO_ERANGE);
      check_contains(turbo_flow_last_error(flow)->message, "1048576");
      check_equal(turbo_flow_parse_string(flow, without_worker, strlen(without_worker)),
                   TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "requires a worker");

      turbo_flow_destroy(flow);
    }

    it("rejects executor options that the selected kind does not consume") {
      static const char *inline_workers = "stage call exec inline workers 2\n";
      static const char *thread_lanes = "stage call exec thread lanes 2\n";
      static const char *thread_pool = "stage call exec thread pool 8\n";
      static const char *coro_workers = "stage call exec coro workers 2\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, inline_workers, strlen(inline_workers)),
                   TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "does not accept");
      check_equal(turbo_flow_parse_string(flow, thread_lanes, strlen(thread_lanes)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "only accepts workers");
      check_equal(turbo_flow_parse_string(flow, thread_pool, strlen(thread_pool)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "only accepts workers");
      check_equal(turbo_flow_parse_string(flow, coro_workers, strlen(coro_workers)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "lanes and pool");
      turbo_flow_destroy(flow);
    }

    it("allows blank and comment lines inside orchestration blocks") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage clean {\n"
                               "  in input\n"
                               "  out output\n"
                               "  step trim\n"
                               "\n"
                               "  %% mermaid style comment\n"
                               "  input -> trim -> output\n"
                               "  # shell style comment\n"
                               "}\n"
                               "stage main {\n"
                               "\n"
                               "  %% keep comments readable in diagrams\n"
                               "  input -> clean -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_edge_count(flow), 4);
      check_true(turbo_flow_find_stage(flow, "clean.input") >= 0);
      check_true(turbo_flow_find_stage(flow, "clean.output") >= 0);

      turbo_flow_destroy(flow);
    }

    it("rejects multiple root stage orchestration blocks") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage main {\n"
                               "  input -> parse\n"
                               "}\n"
                               "stage main {\n"
                               "  input -> parse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "only one root stage");
      check_equal(turbo_flow_last_error(flow)->line, 6);

      turbo_flow_destroy(flow);
    }

    it("rejects stage names that collide with composite stage names") {
      static const char *src = "source input\n"
                               "stage enrich {\n"
                               "  in input\n"
                               "  out output\n"
                               "  input -> output\n"
                               "}\n"
                               "stage enrich\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate");
      check_equal(turbo_flow_last_error(flow)->line, 7);

      turbo_flow_destroy(flow);
    }
  }

  group("compile validation") {
    it("compiles runtime edges into contiguous per-stage adjacency") {
      static const char *src = "source input\n"
                               "stage left\n"
                               "stage right\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> [left, right] -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      size_t next_edge = 0u;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "left", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "right", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      for (size_t stage_index = 0u; stage_index < vec_size(&flow->runtime_nodes); ++stage_index) {
        const flow_runtime_node_plan_t *node =
            (const flow_runtime_node_plan_t *)vec_at_const(&flow->runtime_nodes, stage_index);
        check_not_null(node);
        check_equal(node->outgoing_begin, next_edge);
        for (size_t offset = 0u; offset < node->outgoing_count; ++offset) {
          const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
              &flow->runtime_edges, node->outgoing_begin + offset);
          check_not_null(edge);
          check_equal(edge->from_stage, stage_index);
        }
        next_edge += node->outgoing_count;
      }
      check_equal(next_edge, vec_size(&flow->runtime_edges));
      turbo_flow_destroy(flow);
    }

    it("compiles use aliases without treating source templates as runtime stages") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  step load\n"
                               "  use c = cleanse\n"
                               "  input -> c -> load\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "c.trim", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "load", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
    }

    it("compiles nested use aliases as finite runtime instances") {
      static const char *src = "stage cleanse {\n"
                               "  in raw\n"
                               "  out clean\n"
                               "  step trim\n"
                               "  raw -> trim -> clean\n"
                               "}\n"
                               "stage pipeline {\n"
                               "  in raw\n"
                               "  out done\n"
                               "  use c = cleanse\n"
                               "  step encode\n"
                               "  raw -> c -> encode -> done\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  step sink\n"
                               "  use p = pipeline\n"
                               "  input -> p -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "p.c.trim", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "p.encode", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
    }

    it("compiles message-bus routing templates as stage compositions") {
      static const char *src = "stage pubsub_proxy {\n"
                               "  in frames\n"
                               "  out routed\n"
                               "  step decode adapter codec.json.in\n"
                               "  step filter adapter databind.filter\n"
                               "  step encode adapter codec.json.out\n"
                               "  frames -> decode -> filter -> encode -> routed\n"
                               "}\n"
                               "stage queue_proxy {\n"
                               "  in jobs\n"
                               "  out balanced\n"
                               "  step balance adapter queue.balance\n"
                               "  jobs -> balance -> balanced\n"
                               "}\n"
                               "stage router_dealer_proxy {\n"
                               "  in front_requests\n"
                               "  out back_requests\n"
                               "  in back_replies\n"
                               "  out front_replies\n"
                               "  step tag adapter databind.identity\n"
                               "  front_requests -> tag -> back_requests\n"
                               "  back_replies -> front_replies\n"
                               "}\n"
                               "stage xpub_xsub_proxy {\n"
                               "  in subscriptions\n"
                               "  out upstream\n"
                               "  in publications\n"
                               "  out downstream\n"
                               "  step sub_filter adapter databind.subscription\n"
                               "  subscriptions -> sub_filter -> upstream\n"
                               "  publications -> downstream\n"
                               "}\n"
                               "source orders_in adapter bus.orders.sub\n"
                               "source jobs_in adapter bus.jobs.pull\n"
                               "source front_in adapter bus.router\n"
                               "source back_in adapter bus.dealer\n"
                               "source xsub_in adapter bus.xsub\n"
                               "source xpub_in adapter bus.xpub\n"
                               "stage orders_out adapter bus.orders.pub\n"
                               "stage jobs_out adapter bus.jobs.push\n"
                               "stage back_out adapter bus.dealer\n"
                               "stage front_out adapter bus.router\n"
                               "stage xsub_out adapter bus.xsub\n"
                               "stage xpub_out adapter bus.xpub\n"
                               "stage main {\n"
                               "  use pubsub = pubsub_proxy\n"
                               "  use queue = queue_proxy\n"
                               "  use rd = router_dealer_proxy\n"
                               "  use xpxs = xpub_xsub_proxy\n"
                               "  orders_in -> pubsub -> orders_out\n"
                               "  jobs_in -> queue -> jobs_out\n"
                               "  front_in -> rd.front_requests\n"
                               "  rd.back_requests -> back_out\n"
                               "  back_in -> rd.back_replies\n"
                               "  rd.front_replies -> front_out\n"
                               "  xsub_in -> xpxs.subscriptions\n"
                               "  xpxs.upstream -> xsub_out\n"
                               "  xpub_in -> xpxs.publications\n"
                               "  xpxs.downstream -> xpub_out\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      register_schema_adapter(flow, "bus.orders.sub", TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
                              TURBO_FLOW_ADAPTER_SOURCE, TURBO_FLOW_ADAPTER_INPUT);
      register_schema_adapter(flow, "bus.orders.pub", TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
                              TURBO_FLOW_ADAPTER_SINK, TURBO_FLOW_ADAPTER_OUTPUT);
      register_schema_adapter(flow, "bus.jobs.pull", TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
                              TURBO_FLOW_ADAPTER_SOURCE, TURBO_FLOW_ADAPTER_INPUT);
      register_schema_adapter(flow, "bus.jobs.push", TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
                              TURBO_FLOW_ADAPTER_SINK, TURBO_FLOW_ADAPTER_OUTPUT);
      register_schema_adapter(flow, "bus.router", TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
                              TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK,
                              TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "bus.dealer", TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
                              TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK,
                              TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "bus.xsub", TURBO_FLOW_ADAPTER_KIND_CUSTOM,
                              TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK,
                              TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "bus.xpub", TURBO_FLOW_ADAPTER_KIND_CUSTOM,
                              TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK,
                              TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "codec.json.in", TURBO_FLOW_ADAPTER_KIND_CODEC,
                              TURBO_FLOW_ADAPTER_TRANSFORM, TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "codec.json.out", TURBO_FLOW_ADAPTER_KIND_CODEC,
                              TURBO_FLOW_ADAPTER_TRANSFORM, TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "databind.filter", TURBO_FLOW_ADAPTER_KIND_DATABIND,
                              TURBO_FLOW_ADAPTER_TRANSFORM, TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "databind.identity", TURBO_FLOW_ADAPTER_KIND_DATABIND,
                              TURBO_FLOW_ADAPTER_TRANSFORM, TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "databind.subscription", TURBO_FLOW_ADAPTER_KIND_DATABIND,
                              TURBO_FLOW_ADAPTER_TRANSFORM, TURBO_FLOW_ADAPTER_BIDIRECTIONAL);
      register_schema_adapter(flow, "queue.balance", TURBO_FLOW_ADAPTER_KIND_QUEUE,
                              TURBO_FLOW_ADAPTER_TRANSFORM, TURBO_FLOW_ADAPTER_BIDIRECTIONAL);

      {
        int rc = turbo_flow_compile(flow);
        if (rc != TURBO_OK && turbo_flow_last_error(flow)) {
          info("compile error: %s", turbo_flow_last_error(flow)->message);
        }
        check_equal(rc, TURBO_OK);
      }
      check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_COMPILED);
      check_true(turbo_flow_find_stage(flow, "pubsub.filter") >= 0);
      check_true(turbo_flow_find_stage(flow, "queue.balance") >= 0);
      check_true(turbo_flow_find_stage(flow, "rd.back_requests") >= 0);
      check_true(turbo_flow_find_stage(flow, "rd.front_replies") >= 0);
      check_true(turbo_flow_find_stage(flow, "xpxs.sub_filter") >= 0);

      turbo_flow_destroy(flow);
    }

    it("resolves late stage declarations across reset") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> parse -> sink\n"
                               "}\n";
      const char *names[] = {"parse", "sink"};
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_edge_plan_t *edge = NULL;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      register_stage_names(flow, names, sizeof(names) / sizeof(names[0]));
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_COMPILED);

      edge = turbo_flow_edge_at(flow, 0);
      check_not_null(edge);
      check_equal(edge->from_stage, (uint32_t)turbo_flow_find_stage(flow, "input"));
      check_equal(edge->to_stage, (uint32_t)turbo_flow_find_stage(flow, "parse"));

      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(turbo_flow_reset(flow, 1), TURBO_OK);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
    }

    it("rejects unregistered source and sink adapters during compile") {
      static const char *source_adapter = "source input adapter \"http.server\"\n"
                                          "stage main {\n"
                                          "}\n";
      static const char *sink_adapter = "source input\n"
                                        "stage sink adapter smtp\n"
                                        "stage main {\n"
                                        "  input -> sink\n"
                                        "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, source_adapter, strlen(source_adapter)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "adapter");
      check_equal(turbo_flow_last_error(flow)->line, 1);

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, sink_adapter, strlen(sink_adapter)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "adapter");
      check_equal(turbo_flow_last_error(flow)->line, 2);

      turbo_flow_destroy(flow);
    }

    it("copies and exposes typed adapter option schemas") {
      char field_name[] = "port";
      char enum_value[] = "smtp";
      const char *enum_values[] = {enum_value};
      turbo_flow_option_field_t fields[] = {
          {field_name, TURBO_FLOW_OPTION_U32,
           TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1,
           65535, NULL, 0},
          {"protocol", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, enum_values, 1}};
      turbo_flow_adapter_schema_t schema = {NULL,
                                            TURBO_FLOW_ADAPTER_KIND_EMAIL,
                                            TURBO_FLOW_ADAPTER_SINK,
                                            TURBO_FLOW_ADAPTER_OUTPUT,
                                            fields,
                                            2};
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_adapter_schema_t *registered;

      check_not_null(flow);
      check_equal(turbo_flow_register_adapter_ex(flow, "mail", NULL, NULL, &schema), TURBO_OK);
      field_name[0] = 'x';
      enum_value[0] = 'x';

      check_equal(turbo_flow_adapter_count(flow), 1);
      registered = turbo_flow_find_adapter_schema(flow, "mail");
      check_not_null(registered);
      check_equal(registered->binding_name, "mail");
      check_equal(registered->kind, TURBO_FLOW_ADAPTER_KIND_EMAIL);
      check_equal(registered->roles, TURBO_FLOW_ADAPTER_SINK);
      check_equal(registered->field_count, 2);
      check_equal(registered->fields[0].name, "port");
      check_equal(registered->fields[1].enum_values[0], "smtp");
      check_equal((const void *)turbo_flow_adapter_schema_at(flow, 0),
                  (const void *)registered);

      turbo_flow_destroy(flow);
    }

    it("rejects structurally invalid adapter option schemas") {
      turbo_flow_option_field_t field = {"mode", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, NULL, 0};
      turbo_flow_adapter_schema_t schema = {NULL,
                                            TURBO_FLOW_ADAPTER_KIND_CUSTOM,
                                            TURBO_FLOW_ADAPTER_TRANSFORM,
                                            TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
                                            &field,
                                            1};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_register_adapter_ex(flow, "invalid", NULL, NULL, &schema),
                   TURBO_EINVAL);
      check_equal(turbo_flow_adapter_count(flow), 0);

      field.type = TURBO_FLOW_OPTION_STRING;
      field.flags = TURBO_FLOW_OPTION_HAS_MIN;
      check_equal(turbo_flow_register_adapter_ex(flow, "invalid", NULL, NULL, &schema),
                   TURBO_EINVAL);

      field.type = TURBO_FLOW_OPTION_HOST_OBJECT;
      field.flags = 0;
      check_equal(turbo_flow_register_adapter_ex(flow, "invalid", NULL, NULL, &schema),
                   TURBO_EINVAL);

      turbo_flow_destroy(flow);
    }

    it("enforces adapter schema roles and terminal sink stages") {
      static const turbo_flow_adapter_schema_t sink_schema = {NULL,
                                                              TURBO_FLOW_ADAPTER_KIND_EMAIL,
                                                              TURBO_FLOW_ADAPTER_SINK,
                                                              TURBO_FLOW_ADAPTER_OUTPUT,
                                                              NULL,
                                                              0};
      static const char *source_mismatch = "source input adapter mail\n"
                                           "stage main {\n"
                                           "}\n";
      static const char *nonterminal_sink = "source input\n"
                                            "stage mail adapter mail\n"
                                            "stage tail\n"
                                            "stage main {\n"
                                            "  input -> mail -> tail\n"
                                            "}\n";
      turbo_flow_adapter_ops_t ops;
      adapter_ctx_t adapter_ctx = {0};
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter_ex(flow, "mail", &ops, &adapter_ctx, &sink_schema),
                   TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, source_mismatch, strlen(source_mismatch)),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "source usage");

      check_equal(turbo_flow_reset(flow, 1), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, nonterminal_sink, strlen(nonterminal_sink)),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "tail", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "terminal");

      turbo_flow_destroy(flow);
    }

    it("starts, stops, and shuts down registered source and sink adapters") {
      static const char *src = "source http_in adapter \"http.server\"\n"
                               "stage email_out adapter smtp\n"
                               "stage main {\n"
                               "  http_in -> email_out\n"
                               "}\n";
      adapter_ctx_t adapter_ctx = {0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.start = test_adapter_start;
      ops.stop = test_adapter_stop;
      ops.shutdown = test_adapter_shutdown;

      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "http.server", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_register_adapter(flow, "smtp", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_register_adapter(flow, "smtp", &ops, &adapter_ctx), TURBO_EALREADY);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "email_out", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_register_adapter(flow, "late", &ops, &adapter_ctx), TURBO_EBUSY);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(adapter_ctx.start_count, 2);
      check_equal(adapter_ctx.source_start_count, 1);
      check_equal(adapter_ctx.stage_start_count, 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(adapter_ctx.stop_count, 2);
      check_equal(adapter_ctx.shutdown_count, 0);

      check_equal(turbo_flow_reset(flow, 1), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(adapter_ctx.start_count, 4);
      check_equal(adapter_ctx.stop_count, 4);
      check_equal(adapter_ctx.shutdown_count, 0);

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(adapter_ctx.shutdown_count, 2);

      turbo_flow_destroy(flow);
    }

    it("delivers messages to adapter-backed sink stages without C callbacks") {
      static const char *src = "source input\n"
                               "stage sink adapter smtp\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      char raw[] = "payload";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      adapter_ctx_t adapter_ctx = {0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      adapter_ctx.expected_payload = "payload";

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_register_adapter(flow, "smtp", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(adapter_ctx.consume_count, 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects pooled execution for adapter-owned consume callbacks") {
      static const char *thread_src = "source input\n"
                                      "stage sink adapter smtp exec thread workers 1\n"
                                      "stage main {\n"
                                      "  input -> sink\n"
                                      "}\n";
      static const char *coro_src = "source input\n"
                                    "stage sink adapter smtp exec coro lanes 1\n"
                                    "stage main {\n"
                                    "  input -> sink\n"
                                    "}\n";
      turbo_flow_adapter_ops_t ops;
      adapter_ctx_t adapter_ctx = {0};
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "smtp", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, thread_src, strlen(thread_src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "inline executor");
      check_equal(turbo_flow_parse_string(flow, coro_src, strlen(coro_src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "inline executor");
      turbo_flow_destroy(flow);
    }

    it("dispatches adapter control commands only through the runtime owner") {
      static const char *src = "source input\n"
                               "stage sink adapter controlled\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      adapter_ctx_t adapter_ctx = {0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_adapter_command_t command;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      ops.command = test_adapter_command;
      memset(&command, 0, sizeof(command));
      command.size = sizeof(command);
      command.kind = TURBO_FLOW_ADAPTER_QUIESCE;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "controlled", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_register_adapter(flow, "unmanaged", NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_adapter_command(flow, "controlled", &command), TURBO_EINVAL);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_adapter_command(flow, "missing", &command), TURBO_ENOENT);
      check_equal(turbo_flow_adapter_command(flow, "unmanaged", &command), TURBO_ENOTSUP);
      check_equal(turbo_flow_adapter_command(flow, "controlled", &command), TURBO_OK);
      check_equal(adapter_ctx.command_count, 1);
      check_equal(adapter_ctx.last_command, TURBO_FLOW_ADAPTER_QUIESCE);
      adapter_ctx.fail_status = TURBO_EIO;
      command.kind = TURBO_FLOW_ADAPTER_RESUME;
      check_equal(turbo_flow_adapter_command(flow, "controlled", &command), TURBO_EIO);
      check_equal(adapter_ctx.command_count, 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(turbo_flow_adapter_command(flow, "controlled", &command), TURBO_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("allows source adapters without consume callbacks") {
      static const char *src = "source socket_in adapter \"socket.tcp\"\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  socket_in -> sink\n"
                               "}\n";
      char raw[] = "payload";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      adapter_ctx_t adapter_ctx = {0};
      payload_check_ctx_t sink_ctx = {"payload", 0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.start = test_adapter_start;
      ops.stop = test_adapter_stop;

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_register_adapter(flow, "socket.tcp", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(adapter_ctx.source_start_count, 1);
      check_equal(turbo_flow_publish(flow, "socket_in", &msg), TURBO_OK);
      check_equal(sink_ctx.called, 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(adapter_ctx.stop_count, 1);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("delivers inline stages through registered socket adapters") {
      static const char *src = "source input\n"
                               "stage socket_out adapter \"socket.tcp\"\n"
                               "stage main {\n"
                               "  input -> socket_out\n"
                               "}\n";
      char raw[] = "socket-data";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      adapter_ctx_t adapter_ctx = {0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      adapter_ctx.expected_payload = "socket-data";

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_register_adapter(flow, "socket.tcp", &ops, &adapter_ctx), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(adapter_ctx.consume_count, 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects stage plans without a publishable source") {
      static const char *stage_only = "stage parse\n"
                                      "stage main {\n"
                                      "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, "", 0), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "source");

      check_equal(turbo_flow_parse_string(flow, stage_only, strlen(stage_only)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "source");

      turbo_flow_destroy(flow);
    }

    it("rejects missing registrations unknown stages and cycles") {
      static const char *missing_cb = "source input\n"
                                      "stage parse\n"
                                      "stage main {\n"
                                      "  input -> parse\n"
                                      "}\n";
      static const char *unknown_stage = "source input\n"
                                         "stage parse\n"
                                         "stage main {\n"
                                         "  input -> missing\n"
                                         "}\n";
      static const char *cycle = "source input\n"
                                 "stage a\n"
                                 "stage b\n"
                                 "stage main {\n"
                                 "  input -> a\n"
                                 "  a -> b\n"
                                 "  b -> a\n"
                                 "}\n";
      const char *cycle_names[] = {"a", "b"};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, missing_cb, strlen(missing_cb)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "callback");
      check_equal(turbo_flow_last_error(flow)->line, 2);
      check_equal(turbo_flow_last_error(flow)->column, 7);

      check_equal(turbo_flow_parse_string(flow, unknown_stage, strlen(unknown_stage)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "unknown stage");

      check_equal(turbo_flow_parse_string(flow, cycle, strlen(cycle)), TURBO_OK);
      register_stage_names(flow, cycle_names, sizeof(cycle_names) / sizeof(cycle_names[0]));
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "cycle");

      turbo_flow_destroy(flow);
    }

    it("reports stage cycles before missing stage registrations") {
      static const char *src = "source input\n"
                               "stage a\n"
                               "stage b\n"
                               "stage main {\n"
                               "  input -> a\n"
                               "  a -> b\n"
                               "  b -> a\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "cycle");

      turbo_flow_destroy(flow);
    }

    it("rejects duplicate stage edges during compile") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage main {\n"
                               "  input -> parse\n"
                               "  input -> parse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate stage edge");
      check_equal(turbo_flow_last_error(flow)->line, 5);

      turbo_flow_destroy(flow);
    }

    it("rejects stages that are not reachable from any source") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage unused\n"
                               "stage main {\n"
                               "  input -> parse\n"
                               "}\n";
      const char *names[] = {"parse", "unused"};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      register_stage_names(flow, names, sizeof(names) / sizeof(names[0]));
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "not reachable");
      check_equal(turbo_flow_last_error(flow)->line, 3);

      turbo_flow_destroy(flow);
    }

    it("recovers from failed compile by parsing a new plan") {
      static const char *bad_src = "source input\n"
                                   "stage parse\n"
                                   "stage main {\n"
                                   "  input -> missing\n"
                                   "}\n";
      static const char *ok_src = "source input\n"
                                  "stage parse\n"
                                  "stage main {\n"
                                  "  input -> parse\n"
                                  "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, bad_src, strlen(bad_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_FAILED);
      check_contains(turbo_flow_last_error(flow)->message, "unknown stage");

      check_equal(turbo_flow_parse_string(flow, ok_src, strlen(ok_src)), TURBO_OK);
      check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_PARSED);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
    }

    it("rejects mutable stages on immediate broadcast fan-out") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage validate\n"
                               "stage enrich\n"
                               "stage main {\n"
                               "  input -> parse -> [validate, enrich]\n"
                               "}\n";
      turbo_flow_stage_options_t mutates = {TURBO_FLOW_STAGE_MUTATES_IN_PLACE};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "validate", noop_stage, NULL, &mutates),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "mutable");
      check_equal(turbo_flow_last_error(flow)->line, 6);

      turbo_flow_destroy(flow);
    }

    it("rejects mutable descendants before fan-in but allows mutation after fan-in") {
      static const char *bad_src = "source input\n"
                                   "stage parse\n"
                                   "stage validate\n"
                                   "stage mutate\n"
                                   "stage metrics\n"
                                   "stage sink\n"
                                   "stage main {\n"
                                   "  input -> parse -> [validate, metrics]\n"
                                   "  validate -> mutate -> sink\n"
                                   "  metrics -> sink\n"
                                   "}\n";
      static const char *ok_src = "source input\n"
                                  "stage parse\n"
                                  "stage validate\n"
                                  "stage metrics\n"
                                  "stage sink\n"
                                  "stage main {\n"
                                  "  input -> parse -> [validate, metrics] -> sink\n"
                                  "}\n";
      turbo_flow_stage_options_t mutates = {TURBO_FLOW_STAGE_MUTATES_IN_PLACE};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, bad_src, strlen(bad_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "validate", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "mutate", noop_stage, NULL, &mutates),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "metrics", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "mutable");

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, ok_src, strlen(ok_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "validate", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "metrics", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, &mutates),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
    }

    it("rejects worker-pool branches at ordered fan-in without reorder strategy") {
      static const char *src = "source input\n"
                               "stage enrich worker 2\n"
                               "stage metrics\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> [enrich, metrics] -> sink\n"
                               "}\n";
      const char *names[] = {"enrich", "metrics", "sink"};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      register_stage_names(flow, names, sizeof(names) / sizeof(names[0]));
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "worker-pool");
      check_contains(turbo_flow_last_error(flow)->message, "reorder");

      turbo_flow_destroy(flow);
    }

    it("rejects thread executor branches at ordered fan-in without reorder strategy") {
      static const char *src = "source input\n"
                               "stage parse exec thread workers 2\n"
                               "stage metrics\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> [parse, metrics] -> sink\n"
                               "}\n";
      const char *names[] = {"parse", "metrics", "sink"};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      register_stage_names(flow, names, sizeof(names) / sizeof(names[0]));
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "thread executor");
      check_contains(turbo_flow_last_error(flow)->message, "reorder");

      turbo_flow_destroy(flow);
    }

    it("rejects duplicate registration and registration after compile") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage main {\n"
                               "  input -> parse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL),
                   TURBO_EALREADY);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "other", noop_stage, NULL, NULL),
                   TURBO_EBUSY);

      turbo_flow_destroy(flow);
    }

    it("rejects external edges that bypass composite stage ports") {
      static const char *src = "source input\n"
                               "stage enrich {\n"
                               "  in input\n"
                               "  out output\n"
                               "  step fetch\n"
                               "  input -> fetch -> output\n"
                               "}\n"
                               "stage main {\n"
                               "  input -> enrich.fetch\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "composite stage input");

      turbo_flow_destroy(flow);
    }

    it("rejects external edges that use composite stage ports in the wrong direction") {
      static const char *to_output = "source input\n"
                                     "stage persist\n"
                                     "stage enrich {\n"
                                     "  in input\n"
                                     "  out output\n"
                                     "  step fetch\n"
                                     "  input -> fetch -> output\n"
                                     "}\n"
                                     "stage main {\n"
                                     "  input -> enrich.output\n"
                                     "}\n";
      static const char *from_input = "source input\n"
                                      "stage persist\n"
                                      "stage enrich {\n"
                                      "  in input\n"
                                      "  out output\n"
                                      "  step fetch\n"
                                      "  input -> fetch -> output\n"
                                      "}\n"
                                      "stage main {\n"
                                      "  enrich.input -> persist\n"
                                      "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, to_output, strlen(to_output)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich.fetch", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "composite stage input");

      check_equal(turbo_flow_parse_string(flow, from_input, strlen(from_input)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", noop_stage, NULL, NULL),
                   TURBO_EALREADY);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich.fetch", noop_stage, NULL, NULL),
                   TURBO_EALREADY);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "composite stage output");

      turbo_flow_destroy(flow);
    }

    it("rejects composite stage outputs that are not reachable from inputs") {
      static const char *src = "source input\n"
                               "stage persist\n"
                               "stage enrich {\n"
                               "  in input\n"
                               "  out output\n"
                               "  step fetch\n"
                               "  input -> fetch\n"
                               "}\n"
                               "stage main {\n"
                               "  input -> enrich.input\n"
                               "  enrich.output -> persist\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "not reachable");
      check_equal(turbo_flow_last_error(flow)->line, 5);

      turbo_flow_destroy(flow);
    }

    it("rejects reversed internal stage port edges") {
      static const char *from_output = "source input\n"
                                       "stage enrich {\n"
                                       "  in input\n"
                                       "  out output\n"
                                       "  step fetch\n"
                                       "  output -> fetch\n"
                                       "}\n"
                                       "stage main {\n"
                                       "  input -> enrich.input\n"
                                       "}\n";
      static const char *to_input = "source input\n"
                                    "stage enrich {\n"
                                    "  in input\n"
                                    "  out output\n"
                                    "  step fetch\n"
                                    "  fetch -> input\n"
                                    "}\n"
                                    "stage main {\n"
                                    "  input -> enrich.input\n"
                                    "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, from_output, strlen(from_output)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "output ports");

      check_equal(turbo_flow_parse_string(flow, to_input, strlen(to_input)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "input ports");

      turbo_flow_destroy(flow);
    }

    it("rejects stage shorthand when port direction is ambiguous") {
      static const char *src = "source input\n"
                               "stage enrich {\n"
                               "  in primary\n"
                               "  in secondary\n"
                               "  out output\n"
                               "  step fetch\n"
                               "  primary -> fetch -> output\n"
                               "}\n"
                               "stage main {\n"
                               "  input -> enrich\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "exactly one input");

      turbo_flow_destroy(flow);
    }

    it("defines reset behavior for keeping or clearing stage registry") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage main {\n"
                               "  input -> parse\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "callback");

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_reset(flow, 1), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
    }
  }

  group("message ownership") {
    it("treats initialized and byte-backed messages as opaque content") {
      char raw[] = "opaque";
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1u, NULL, NULL);
      turbo_flow_msg_t src;
      turbo_flow_msg_t retained;
      turbo_flow_msg_t cloned;

      check_not_null(buffer);
      turbo_flow_msg_init(&src);
      check_equal(turbo_flow_msg_content_state(&src), TURBO_FLOW_CONTENT_OPAQUE);
      src.buffer = buffer;
      src.payload = vstr_from_buf(raw, sizeof(raw) - 1u);
      check_equal(turbo_flow_msg_retain_view(&retained, &src), TURBO_OK);
      check_equal(turbo_flow_msg_clone(&cloned, &src), TURBO_OK);
      check_equal(turbo_flow_msg_content_state(&retained), TURBO_FLOW_CONTENT_OPAQUE);
      check_equal(turbo_flow_msg_content_state(&cloned), TURBO_FLOW_CONTENT_OPAQUE);
      check_equal(cloned.payload.data, raw, sizeof(raw) - 1u);

      turbo_flow_msg_cleanup(&retained);
      turbo_flow_msg_cleanup(&cloned);
      turbo_flow_msg_cleanup(&src);
    }

    it("binds and clears a schema projection without replacing payload bytes") {
      int destroy_count = 0;
      int *value = (int *)malloc(sizeof(*value));
      const turbo_flow_data_schema_t *schema = NULL;
      turbo_flow_msg_t msg;

      check_not_null(value);
      *value = 41;
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("original");
      msg.payload = tstr_to_v(msg.owned_payload);
      check_equal(turbo_flow_msg_bind_projection(&msg, &TEST_DATA_SCHEMA, value, NULL,
                                                  destroy_owned_projection, &destroy_count),
                   TURBO_OK);
      check_equal(turbo_flow_msg_content_state(&msg), TURBO_FLOW_CONTENT_SCHEMA_BOUND);
      check_equal((const void *)turbo_flow_msg_projection(&msg, &schema), (const void *)value);
      check_equal((const void *)schema, (const void *)&TEST_DATA_SCHEMA);
      check_equal(*(const int *)turbo_flow_msg_projection(&msg, NULL), 41);
      check_equal(msg.payload.data, "original");

      turbo_flow_msg_clear_projection(&msg);
      check_equal(destroy_count, 1);
      check_equal(turbo_flow_msg_content_state(&msg), TURBO_FLOW_CONTENT_OPAQUE);
      check_null(turbo_flow_msg_projection(&msg, &schema));
      check_null(schema);
      check_equal(msg.payload.data, "original");
      turbo_flow_msg_cleanup(&msg);
      check_equal(destroy_count, 1);
    }

    it("deep clones projections only when their provider supplies a clone hook") {
      int destroy_count = 0;
      int *value = (int *)malloc(sizeof(*value));
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;
      const int *copy;

      check_not_null(value);
      *value = 73;
      turbo_flow_msg_init(&src);
      check_equal(turbo_flow_msg_bind_projection(&src, &TEST_DATA_SCHEMA, value,
                                                  clone_owned_projection, destroy_owned_projection,
                                                  &destroy_count),
                   TURBO_OK);
      check_equal(turbo_flow_msg_clone(&dst, &src), TURBO_OK);
      copy = (const int *)turbo_flow_msg_projection(&dst, NULL);
      check_not_null(copy);
      check_equal(*copy, 73);
      check_true(copy != value);

      *value = 74;
      check_equal(*copy, 73);
      turbo_flow_msg_cleanup(&dst);
      turbo_flow_msg_cleanup(&src);
      check_equal(destroy_count, 2);
    }

    it("fails clone for a schema projection without provider clone support") {
      int destroy_count = 0;
      int *value = (int *)malloc(sizeof(*value));
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      check_not_null(value);
      *value = 9;
      turbo_flow_msg_init(&src);
      check_equal(turbo_flow_msg_bind_projection(&src, &TEST_DATA_SCHEMA, value, NULL,
                                                  destroy_owned_projection, &destroy_count),
                   TURBO_OK);
      check_equal(turbo_flow_msg_clone(&dst, &src), TURBO_ENOTSUP);
      check_equal(destroy_count, 0);
      check_equal(*(const int *)turbo_flow_msg_projection(&src, NULL), 9);
      turbo_flow_msg_cleanup(&src);
      check_equal(destroy_count, 1);
    }

    it("moves projection ownership and destroys it exactly once") {
      int destroy_count = 0;
      int *value = (int *)malloc(sizeof(*value));
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      check_not_null(value);
      *value = 5;
      turbo_flow_msg_init(&src);
      check_equal(turbo_flow_msg_bind_projection(&src, &TEST_DATA_SCHEMA, value, NULL,
                                                  destroy_owned_projection, &destroy_count),
                   TURBO_OK);
      check_equal(turbo_flow_msg_move(&dst, &src), TURBO_OK);
      check_equal(turbo_flow_msg_content_state(&src), TURBO_FLOW_CONTENT_OPAQUE);
      check_equal(turbo_flow_msg_content_state(&dst), TURBO_FLOW_CONTENT_SCHEMA_BOUND);
      turbo_flow_msg_cleanup(&src);
      check_equal(destroy_count, 0);
      turbo_flow_msg_cleanup(&dst);
      check_equal(destroy_count, 1);
    }

    it("borrows immutable content descriptors across clone and projection clear") {
      turbo_flow_content_descriptor_t descriptor;
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;
      int destroy_count = 0;
      int *value = (int *)malloc(sizeof(*value));
      const turbo_flow_content_descriptor_t *actual;

      check_not_null(value);
      *value = 19;
      check_equal(turbo_flow_content_descriptor_init(
                       &descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                       TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                       "application/json", "orders.created"),
                   TURBO_OK);
      check_equal(turbo_flow_content_descriptor_declare_schema(
                       &descriptor, TEST_DATA_SCHEMA.schema_name, TEST_DATA_SCHEMA.type_name,
                       TEST_DATA_SCHEMA.schema_version),
                   TURBO_OK);
      turbo_flow_msg_init(&src);
      check_equal(turbo_flow_msg_set_content_descriptor(&src, &descriptor), TURBO_OK);
      check_equal(turbo_flow_msg_bind_projection(&src, &TEST_DATA_SCHEMA, value,
                                                  clone_owned_projection, destroy_owned_projection,
                                                  &destroy_count),
                   TURBO_OK);
      check_equal(turbo_flow_msg_clone(&dst, &src), TURBO_OK);
      actual = turbo_flow_msg_content_descriptor(&dst);
      check_not_null(actual);
      check_equal((const void *)actual, (const void *)&descriptor);
      check_equal(actual->media_type, "application/json");
      check_equal(actual->identity, "orders.created");
      check_equal(actual->schema_name, TEST_DATA_SCHEMA.schema_name);
      check_equal(actual->type_name, TEST_DATA_SCHEMA.type_name);
      check_equal(actual->schema_version, TEST_DATA_SCHEMA.schema_version);
      turbo_flow_msg_clear_projection(&dst);
      check_equal(turbo_flow_msg_content_state(&dst), TURBO_FLOW_CONTENT_OPAQUE);
      check_not_null(turbo_flow_msg_content_descriptor(&dst));
      turbo_flow_msg_cleanup(&dst);
      turbo_flow_msg_cleanup(&src);
      check_equal(destroy_count, 2);
    }

    it("retains descriptor-only buffer views without sharing an owned holder") {
      char raw[] = "descriptor-view";
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1u, NULL, NULL);
      turbo_flow_content_descriptor_t descriptor;
      turbo_flow_msg_t src;
      turbo_flow_msg_t retained;

      check_not_null(buffer);
      check_equal(turbo_flow_content_descriptor_init(
                       &descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                       TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                       "application/json", "orders.created"),
                   TURBO_OK);
      turbo_flow_msg_init(&src);
      src.buffer = buffer;
      src.payload = vstr_from_buf(raw, sizeof(raw) - 1u);
      check_equal(turbo_flow_msg_set_content_descriptor(&src, &descriptor), TURBO_OK);
      check_equal(turbo_flow_msg_retain_view(&retained, &src), TURBO_OK);
      check_equal((const void *)turbo_flow_msg_content_descriptor(&retained),
                  (const void *)&descriptor);
      check_equal(mem_buffer_ref_count(buffer), 2u);
      turbo_flow_msg_cleanup(&src);
      check_equal((const void *)turbo_flow_msg_content_descriptor(&retained),
                  (const void *)&descriptor);
      check_equal(retained.payload.data, raw, sizeof(raw) - 1u);
      turbo_flow_msg_cleanup(&retained);
    }

    it("rejects invalid descriptor profile flags and media coherence") {
      turbo_flow_content_descriptor_t descriptor;

      check_equal(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                                      TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY,
                                                      TURBO_FLOW_DATA_ENCODING_JSON,
                                                      "application/json", "/orders"),
                   TURBO_OK);
      descriptor.encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
      check_equal(turbo_flow_content_descriptor_check(&descriptor), TURBO_EINVAL);

      descriptor.encoding = TURBO_FLOW_DATA_ENCODING_JSON;
      descriptor.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
      check_equal(turbo_flow_content_descriptor_check(&descriptor), TURBO_EINVAL);

      descriptor.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
      descriptor.flags = TURBO_FLOW_CONTENT_PROTOCOL_CONTROL;
      check_equal(turbo_flow_content_descriptor_check(&descriptor), TURBO_EINVAL);
    }

    it("enforces one declared schema identity across descriptor and projection binding") {
      static const turbo_flow_data_schema_t conflicting_schema = {sizeof(turbo_flow_data_schema_t),
                                                                  TURBO_FLOW_DOMAIN_DATA,
                                                                  TURBO_FLOW_DATA_ENCODING_JSON,
                                                                  "test.orders.conflict",
                                                                  "ConflictingOrder",
                                                                  "test.int",
                                                                  20u,
                                                                  1u,
                                                                  NULL};
      turbo_flow_content_descriptor_t matching_descriptor;
      turbo_flow_content_descriptor_t conflicting_descriptor;
      turbo_flow_msg_t descriptor_first;
      turbo_flow_msg_t projection_first;
      turbo_flow_msg_t matching;
      int descriptor_first_destroy_count = 0;
      int projection_first_destroy_count = 0;
      int matching_destroy_count = 0;
      int *descriptor_first_value = (int *)malloc(sizeof(*descriptor_first_value));
      int *projection_first_value = (int *)malloc(sizeof(*projection_first_value));
      int *matching_value = (int *)malloc(sizeof(*matching_value));

      check_not_null(descriptor_first_value);
      check_not_null(projection_first_value);
      check_not_null(matching_value);
      *descriptor_first_value = 1;
      *projection_first_value = 2;
      *matching_value = 3;
      check_equal(turbo_flow_content_descriptor_init(
                       &matching_descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                       TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                       "application/json", "orders.created"),
                   TURBO_OK);
      check_equal(turbo_flow_content_descriptor_declare_schema(
                       &matching_descriptor, TEST_DATA_SCHEMA.schema_name,
                       TEST_DATA_SCHEMA.type_name, TEST_DATA_SCHEMA.schema_version),
                   TURBO_OK);
      conflicting_descriptor = matching_descriptor;
      memcpy(conflicting_descriptor.schema_name, conflicting_schema.schema_name,
             strlen(conflicting_schema.schema_name) + 1u);
      memcpy(conflicting_descriptor.type_name, conflicting_schema.type_name,
             strlen(conflicting_schema.type_name) + 1u);

      turbo_flow_msg_init(&descriptor_first);
      check_equal(turbo_flow_msg_set_content_descriptor(&descriptor_first, &matching_descriptor),
                   TURBO_OK);
      check_equal(turbo_flow_msg_bind_projection(
                       &descriptor_first, &conflicting_schema, descriptor_first_value, NULL,
                       destroy_owned_projection, &descriptor_first_destroy_count),
                   TURBO_EPROTO);
      check_equal(descriptor_first_destroy_count, 0);
      free(descriptor_first_value);
      turbo_flow_msg_cleanup(&descriptor_first);

      turbo_flow_msg_init(&projection_first);
      check_equal(turbo_flow_msg_bind_projection(
                       &projection_first, &TEST_DATA_SCHEMA, projection_first_value, NULL,
                       destroy_owned_projection, &projection_first_destroy_count),
                   TURBO_OK);
      check_equal(
          turbo_flow_msg_set_content_descriptor(&projection_first, &conflicting_descriptor),
          TURBO_EPROTO);
      check_equal(projection_first_destroy_count, 0);
      turbo_flow_msg_cleanup(&projection_first);
      check_equal(projection_first_destroy_count, 1);

      turbo_flow_msg_init(&matching);
      check_equal(turbo_flow_msg_bind_projection(&matching, &TEST_DATA_SCHEMA, matching_value,
                                                  NULL, destroy_owned_projection,
                                                  &matching_destroy_count),
                   TURBO_OK);
      check_equal(turbo_flow_msg_set_content_descriptor(&matching, &matching_descriptor),
                   TURBO_OK);
      turbo_flow_msg_cleanup(&matching);
      check_equal(matching_destroy_count, 1);
    }

    it("provides exact reusable MQTT application and control content contracts") {
      static const turbo_flow_data_schema_t mqtt_schema = {sizeof(turbo_flow_data_schema_t),
                                                           TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                                                           TURBO_FLOW_DATA_ENCODING_JSON,
                                                           "mqtt.telemetry",
                                                           "Telemetry",
                                                           "test.telemetry",
                                                           21u,
                                                           1u,
                                                           NULL};
      turbo_flow_content_descriptor_t application;
      turbo_flow_content_descriptor_t control;
      turbo_flow_content_descriptor_t wrong_domain;
      turbo_flow_content_schema_ref_t selector = TURBO_FLOW_CONTENT_SCHEMA_REF_INIT;
      turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
      const turbo_flow_data_schema_t *resolved = NULL;

      check_not_null(registry);
      check_equal(turbo_flow_content_descriptor_init(
                       &application, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                       TURBO_FLOW_CONTENT_PROFILE_MQTT_APPLICATION, TURBO_FLOW_DATA_ENCODING_JSON,
                       "application/json", "sensors/temperature"),
                   TURBO_OK);
      check_equal(turbo_flow_schema_registry_register(registry, &application, &mqtt_schema),
                   TURBO_OK);
      selector.schema_name = mqtt_schema.schema_name;
      selector.type_name = mqtt_schema.type_name;
      selector.schema_version = mqtt_schema.schema_version;
      check_equal(turbo_flow_content_descriptor_resolve(&application, registry, &selector),
                   TURBO_OK);
      check_equal(turbo_flow_schema_registry_resolve(registry, &application, &resolved), TURBO_OK);
      check_equal(resolved->schema_name, mqtt_schema.schema_name);

      wrong_domain = application;
      wrong_domain.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
      check_equal(turbo_flow_content_descriptor_check(&wrong_domain), TURBO_EINVAL);
      application.flags |= TURBO_FLOW_CONTENT_PROTOCOL_CONTROL;
      check_equal(turbo_flow_content_descriptor_check(&application), TURBO_EINVAL);

      check_equal(turbo_flow_content_descriptor_init(&control, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                                                      TURBO_FLOW_CONTENT_PROFILE_MQTT_CONTROL,
                                                      TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                      "application/octet-stream", "$SYS/control"),
                   TURBO_OK);
      control.flags |= TURBO_FLOW_CONTENT_PROTOCOL_CONTROL;
      check_equal(turbo_flow_content_descriptor_check(&control), TURBO_OK);
      turbo_flow_schema_registry_destroy(registry);
    }

    it("normalizes media types and resolves a trusted schema into an owner descriptor") {
      turbo_flow_content_descriptor_t descriptor;
      turbo_flow_content_schema_ref_t selector = TURBO_FLOW_CONTENT_SCHEMA_REF_INIT;
      turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
      turbo_flow_data_encoding_t encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
      const char *media_type = NULL;
      check_not_null(registry);
      check_equal(turbo_flow_content_media_type_normalize("Application/JSON; charset=utf-8",
                                                           &encoding, &media_type),
                   TURBO_OK);
      check_equal(encoding, TURBO_FLOW_DATA_ENCODING_JSON);
      check_equal(media_type, "application/json");
      check_equal(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                                      TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY,
                                                      encoding, media_type, "/orders"),
                   TURBO_OK);
      check_equal(
          turbo_flow_schema_registry_register(registry, &descriptor, &TEST_HTTP_DATA_SCHEMA),
          TURBO_OK);
      selector.schema_name = TEST_HTTP_DATA_SCHEMA.schema_name;
      selector.type_name = TEST_HTTP_DATA_SCHEMA.type_name;
      selector.schema_version = TEST_HTTP_DATA_SCHEMA.schema_version;
      check_equal(turbo_flow_content_descriptor_resolve(&descriptor, registry, &selector),
                   TURBO_OK);
      check_bits(descriptor.flags, TURBO_FLOW_CONTENT_SCHEMA_DECLARED);
      check_equal(descriptor.schema_name, TEST_HTTP_DATA_SCHEMA.schema_name);
      check_equal(turbo_flow_content_descriptor_declare_schema(&descriptor, "wrong.schema",
                                                                "WrongType", 9u),
                   TURBO_EPROTO);
      {
        turbo_flow_content_descriptor_t wrong_profile = descriptor;
        wrong_profile.profile = TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY;
        check_equal(turbo_flow_content_descriptor_validate(&descriptor, &wrong_profile),
                     TURBO_EPROTO);
      }
      check_equal(
          turbo_flow_content_media_type_normalize("application/x-unknown", &encoding, &media_type),
          TURBO_ENOENT);
      turbo_flow_schema_registry_destroy(registry);
    }

    it("resolves trusted schemas by domain content identity") {
      turbo_flow_content_descriptor_t match;
      turbo_flow_content_descriptor_t declared;
      turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
      const turbo_flow_data_schema_t *resolved = NULL;
      turbo_flow_msg_t msg;

      check_not_null(registry);
      check_equal(turbo_flow_content_descriptor_init(&match, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                                      TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY,
                                                      TURBO_FLOW_DATA_ENCODING_JSON,
                                                      "application/json", NULL),
                   TURBO_OK);
      check_equal(turbo_flow_schema_registry_register(registry, &match, &TEST_HTTP_DATA_SCHEMA),
                   TURBO_OK);
      check_equal(turbo_flow_schema_registry_register(registry, &match, &TEST_HTTP_DATA_SCHEMA),
                   TURBO_EALREADY);
      check_equal(turbo_flow_schema_registry_resolve(registry, &match, &resolved), TURBO_ENOENT);
      declared = match;
      check_equal(turbo_flow_content_descriptor_declare_schema(
                       &declared, TEST_HTTP_DATA_SCHEMA.schema_name,
                       TEST_HTTP_DATA_SCHEMA.type_name, TEST_HTTP_DATA_SCHEMA.schema_version),
                   TURBO_OK);
      check_equal(turbo_flow_schema_registry_resolve(registry, &declared, &resolved), TURBO_OK);
      check_not_null(resolved);
      check_equal(resolved->projection_type, "test.int");

      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_msg_set_content_descriptor(&msg, &declared), TURBO_OK);
      resolved = NULL;
      check_equal(turbo_flow_msg_resolve_schema(&msg, registry, &resolved), TURBO_OK);
      check_not_null(resolved);
      declared.schema_version = 3u;
      check_equal(turbo_flow_schema_registry_resolve(registry, &declared, &resolved),
                   TURBO_ENOENT);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_schema_registry_destroy(registry);
    }

    it("deep copies schema identities and rejects runtime schema text") {
      char schema_name[] = "test.http.mutable";
      char type_name[] = "MutableOrder";
      char projection_type[] = "test.mutable";
      turbo_flow_content_descriptor_t match;
      turbo_flow_content_descriptor_t declared;
      turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
      turbo_flow_data_schema_t schema = {sizeof(turbo_flow_data_schema_t),
                                         TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                         TURBO_FLOW_DATA_ENCODING_JSON,
                                         schema_name,
                                         type_name,
                                         projection_type,
                                         19u,
                                         2u,
                                         NULL};
      turbo_flow_data_schema_t conflicting = schema;
      const turbo_flow_data_schema_t *resolved = NULL;

      check_not_null(registry);
      check_equal(turbo_flow_content_descriptor_init(&match, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                                      TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY,
                                                      TURBO_FLOW_DATA_ENCODING_JSON,
                                                      "application/json", NULL),
                   TURBO_OK);
      schema.schema_text = "message MutableOrder { uint32 id; }";
      check_equal(turbo_flow_schema_registry_register(registry, &match, &schema), TURBO_EINVAL);
      schema.schema_text = NULL;
      check_equal(turbo_flow_schema_registry_register(registry, &match, &schema), TURBO_OK);
      conflicting.schema_id = 20u;
      check_equal(turbo_flow_schema_registry_register(registry, &match, &conflicting),
                   TURBO_EPROTO);

      schema_name[0] = 'X';
      type_name[0] = 'X';
      projection_type[0] = 'X';
      declared = match;
      check_equal(turbo_flow_content_descriptor_declare_schema(&declared, "test.http.mutable",
                                                                "MutableOrder", 2u),
                   TURBO_OK);
      check_equal(turbo_flow_schema_registry_resolve(registry, &declared, &resolved), TURBO_OK);
      check_not_null(resolved);
      check_equal(resolved->schema_name, "test.http.mutable");
      check_equal(resolved->type_name, "MutableOrder");
      check_equal(resolved->projection_type, "test.mutable");
      turbo_flow_schema_registry_destroy(registry);
    }

    it("retains borrowed buffer views and releases them independently") {
      char raw[] = "payload";
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      check_not_null(buffer);
      turbo_flow_msg_init(&src);
      src.buffer = buffer;
      src.payload = vstr_from_buf(raw, sizeof(raw) - 1);
      check_equal(mem_buffer_ref_count(buffer), 1);

      check_equal(turbo_flow_msg_retain_view(&dst, &src), TURBO_OK);
      check_equal((const void *)dst.buffer, (const void *)src.buffer);
      check_equal(mem_buffer_ref_count(buffer), 2);
      check_equal(dst.payload.data, "payload");

      turbo_flow_msg_cleanup(&dst);
      check_equal(mem_buffer_ref_count(buffer), 1);
      turbo_flow_msg_cleanup(&src);
    }

    it("propagates transport context independently from content data") {
      int transport_marker = 1;
      turbo_flow_msg_t src;
      turbo_flow_msg_t retained;
      turbo_flow_msg_t cloned;

      turbo_flow_msg_init(&src);
      src.transport_context = &transport_marker;
      check_equal(turbo_flow_msg_content_state(&src), TURBO_FLOW_CONTENT_OPAQUE);
      turbo_flow_msg_clear_projection(&src);
      check_equal(turbo_flow_msg_retain_view(&retained, &src), TURBO_OK);
      check_equal(turbo_flow_msg_clone(&cloned, &src), TURBO_OK);
      check_equal((const void *)retained.transport_context, (const void *)&transport_marker);
      check_equal((const void *)cloned.transport_context, (const void *)&transport_marker);

      turbo_flow_msg_cleanup(&retained);
      turbo_flow_msg_cleanup(&cloned);
    }

    it("moves unique owned payload without double cleanup") {
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      turbo_flow_msg_init(&src);
      src.owned_payload = tstr_dup("owned");
      src.payload = tstr_to_v(src.owned_payload);

      check_equal(turbo_flow_msg_retain_view(&dst, &src), TURBO_EINVAL);
      check_equal(turbo_flow_msg_move(&dst, &src), TURBO_OK);
      check_null(src.owned_payload);
      check_equal(dst.owned_payload, "owned");

      turbo_flow_msg_cleanup(&src);
      turbo_flow_msg_cleanup(&dst);
    }

    it("clones owned payload while retaining borrowed buffers") {
      char raw[] = "backing";
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      check_not_null(buffer);
      turbo_flow_msg_init(&src);
      src.id = 42;
      src.buffer = buffer;
      src.owned_payload = tstr_dup("prefix-owned");
      src.payload = vstr_from_buf(src.owned_payload + strlen("prefix-"), strlen("owned"));
      check_equal(mem_buffer_ref_count(buffer), 1);

      check_equal(turbo_flow_msg_clone(&dst, &src), TURBO_OK);
      check_equal(mem_buffer_ref_count(buffer), 2);
      check_equal(dst.id, 42);
      check_equal(dst.owned_payload, "prefix-owned");
      check_true(dst.owned_payload != src.owned_payload);
      check_equal(dst.payload.data, "owned");
      check_true(dst.payload.data == dst.owned_payload + strlen("prefix-"));

      src.owned_payload[7] = 'O';
      check_equal(src.payload.data, "Owned");
      check_equal(dst.payload.data, "owned");

      turbo_flow_msg_cleanup(&dst);
      check_equal(mem_buffer_ref_count(buffer), 1);
      turbo_flow_msg_cleanup(&src);
    }

    it("rejects clone when owned payload view exceeds its backing string") {
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      turbo_flow_msg_init(&src);
      src.owned_payload = tstr_dup("owned");
      src.payload = vstr_from_buf(src.owned_payload + 3, 8);

      check_equal(turbo_flow_msg_clone(&dst, &src), TURBO_EINVAL);

      turbo_flow_msg_cleanup(&src);
    }

    it("rejects borrowed payload views outside their declared backing buffer") {
      char backing[] = "backing";
      char outside[] = "outside";
      mem_buffer_t *buffer = mem_wrap_external(backing, sizeof(backing) - 1u, NULL, NULL);
      turbo_flow_msg_t src;
      turbo_flow_msg_t dst;

      check_not_null(buffer);
      turbo_flow_msg_init(&src);
      src.buffer = buffer;
      src.payload = vstr_from_buf(outside, sizeof(outside) - 1u);

      turbo_flow_msg_init(&dst);
      check_equal(turbo_flow_msg_retain_view(&dst, &src), TURBO_EINVAL);
      turbo_flow_msg_cleanup(&dst);
      turbo_flow_msg_init(&dst);
      check_equal(turbo_flow_msg_clone(&dst, &src), TURBO_EINVAL);
      turbo_flow_msg_cleanup(&dst);

      src.payload = vstr_from_buf(backing + 3u, sizeof(backing));
      turbo_flow_msg_init(&dst);
      check_equal(turbo_flow_msg_retain_view(&dst, &src), TURBO_EINVAL);
      turbo_flow_msg_cleanup(&dst);
      turbo_flow_msg_cleanup(&src);
    }
  }

  group("inline runtime") {
    it("rejects publish before start and runs a linear stage plan in order after start") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage validate\n"
                               "stage main {\n"
                               "  input -> parse -> validate\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t parse_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t validate_ctx = {&trace, 2, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", record_stage, &parse_ctx, NULL),
                   TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "validate", record_stage, &validate_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EINVAL);
      check_equal(trace.count, 0);

      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(mem_buffer_ref_count(buffer), 1);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(mem_buffer_ref_count(buffer), 1);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);

      trace.count = 0;
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(mem_buffer_ref_count(buffer), 1);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);

      check_equal(turbo_flow_stop(flow), TURBO_OK);
      trace.count = 0;
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(mem_buffer_ref_count(buffer), 1);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("publishes an ordered batch under one admission and stops at the first failure") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      batch_publish_probe_t probe = {{0}, 0u, 0u, TURBO_EPROTO};
      batch_prepare_probe_t prepare_probe = {0u, SIZE_MAX, TURBO_EIO};
      turbo_flow_publish_batch_config_t batch_config = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      size_t published = SIZE_MAX;

      check_not_null(flow);
      batch_config.message_count = 4u;
      batch_config.prepare = batch_prepare_message;
      batch_config.ctx = &prepare_probe;
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", batch_publish_probe_stage,
                                                &probe, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);

      check_equal(turbo_flow_publish_batch(flow, "input", &batch_config, &published),
                   TURBO_EINVAL);
      check_equal(published, 0u);
      check_equal(probe.calls, 0u);

      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish_batch(flow, "missing", &batch_config, &published),
                   TURBO_EINVAL);
      check_equal(published, 0u);
      check_equal(probe.calls, 0u);

      check_equal(turbo_flow_publish_batch(flow, "input", &batch_config, &published), TURBO_OK);
      check_equal(published, 4u);
      check_equal(probe.calls, 4u);
      for (size_t index = 0u; index < 4u; ++index)
        check_equal(probe.ids[index], index + 1u);

      memset(probe.ids, 0, sizeof(probe.ids));
      probe.calls = 0u;
      probe.fail_id = 3u;
      check_equal(turbo_flow_publish_batch(flow, "input", &batch_config, &published),
                   TURBO_EPROTO);
      check_equal(published, 2u);
      check_equal(probe.calls, 3u);
      check_equal(probe.ids[0], 1u);
      check_equal(probe.ids[1], 2u);
      check_equal(probe.ids[2], 3u);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_EPROTO);

      memset(probe.ids, 0, sizeof(probe.ids));
      probe.calls = 0u;
      probe.fail_id = 0u;
      prepare_probe.calls = 0u;
      prepare_probe.fail_index = 2u;
      check_equal(turbo_flow_publish_batch(flow, "input", &batch_config, &published), TURBO_EIO);
      check_equal(published, 2u);
      check_equal(prepare_probe.calls, 3u);
      check_equal(probe.calls, 2u);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_EIO);

      turbo_flow_destroy(flow);
    }

    it("runs a deep graph through bounded iterative scratch storage") {
      enum { LARGE_GRAPH_STAGE_COUNT = 1024, LARGE_GRAPH_SOURCE_CAPACITY = 65536 };
      char source[LARGE_GRAPH_SOURCE_CAPACITY];
      char stage_name[32];
      size_t used = 0u;
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();
      int written;

      check_not_null(flow);
      written = snprintf(source + used, sizeof(source) - used, "source input\n");
      check_true(written > 0 && (size_t)written < sizeof(source) - used);
      used += (size_t)written;
      for (unsigned i = 0u; i < LARGE_GRAPH_STAGE_COUNT; ++i) {
        written = snprintf(source + used, sizeof(source) - used, "stage node_%u\n", i);
        check_true(written > 0 && (size_t)written < sizeof(source) - used);
        used += (size_t)written;
      }
      written = snprintf(source + used, sizeof(source) - used, "stage main {\n  input");
      check_true(written > 0 && (size_t)written < sizeof(source) - used);
      used += (size_t)written;
      for (unsigned i = 0u; i < LARGE_GRAPH_STAGE_COUNT; ++i) {
        written = snprintf(source + used, sizeof(source) - used, " -> node_%u", i);
        check_true(written > 0 && (size_t)written < sizeof(source) - used);
        used += (size_t)written;
      }
      written = snprintf(source + used, sizeof(source) - used, "\n}\n");
      check_true(written > 0 && (size_t)written < sizeof(source) - used);
      used += (size_t)written;

      check_equal(turbo_flow_parse_string(flow, source, used), TURBO_OK);
      for (unsigned i = 0u; i < LARGE_GRAPH_STAGE_COUNT; ++i) {
        written = snprintf(stage_name, sizeof(stage_name), "node_%u", i);
        check_true(written > 0 && (size_t)written < sizeof(stage_name));
        check_equal(turbo_flow_register_stage_ex(flow, stage_name, noop_stage, NULL, NULL),
                     TURBO_OK);
      }
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs a diamond stage plan only after both fan-in branches complete") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage validate\n"
                               "stage enrich\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> parse -> [validate, enrich] -> sink\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t parse_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t validate_ctx = {&trace, 2, TURBO_OK};
      publish_stage_ctx_t enrich_ctx = {&trace, 3, TURBO_OK};
      publish_stage_ctx_t sink_ctx = {&trace, 4, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", record_stage, &parse_ctx, NULL),
                   TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "validate", record_stage, &validate_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich", record_stage, &enrich_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

      check_equal(trace.count, 4);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(trace.order[2], 3);
      check_equal(trace.order[3], 4);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs only the published source slice when a plan has shared fan-in") {
      static const char *src = "source orders\n"
                               "source refunds\n"
                               "stage parse_order\n"
                               "stage parse_refund\n"
                               "stage audit\n"
                               "stage main {\n"
                               "  orders -> parse_order -> audit\n"
                               "  refunds -> parse_refund -> audit\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t order_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t refund_ctx = {&trace, 2, TURBO_OK};
      publish_stage_ctx_t audit_ctx = {&trace, 3, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "parse_order", record_stage, &order_ctx, NULL),
          TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "parse_refund", record_stage, &refund_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "audit", record_stage, &audit_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      check_equal(turbo_flow_publish(flow, "orders", &msg), TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 3);

      trace.count = 0;
      check_equal(turbo_flow_publish(flow, "refunds", &msg), TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 2);
      check_equal(trace.order[1], 3);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs a composite stage as a namespaced orchestrator with ports") {
      static const char *src = "source input\n"
                               "stage validate\n"
                               "stage persist\n"
                               "stage enrich {\n"
                               "  in input\n"
                               "  out output\n"
                               "  step fetch\n"
                               "  step normalize\n"
                               "  input --> fetch --> normalize --> output\n"
                               "}\n"
                               "stage main {\n"
                               "  input --> validate --> enrich.input\n"
                               "  enrich.output --> persist\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t validate_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t fetch_ctx = {&trace, 2, TURBO_OK};
      publish_stage_ctx_t normalize_ctx = {&trace, 3, TURBO_OK};
      publish_stage_ctx_t persist_ctx = {&trace, 4, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 7);
      check_equal(turbo_flow_edge_count(flow), 6);
      check_true(turbo_flow_find_stage(flow, "enrich.input") >= 0);
      check_true(turbo_flow_find_stage(flow, "enrich.output") >= 0);

      check_equal(
          turbo_flow_register_stage_ex(flow, "validate", record_stage, &validate_ctx, NULL),
          TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "enrich.fetch", record_stage, &fetch_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich.normalize", record_stage,
                                                &normalize_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", record_stage, &persist_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

      check_equal(trace.count, 4);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(trace.order[2], 3);
      check_equal(trace.order[3], 4);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs a single-port stage through shorthand orchestration") {
      static const char *src = "source input\n"
                               "stage persist\n"
                               "stage enrich {\n"
                               "  in input\n"
                               "  out output\n"
                               "  step fetch\n"
                               "  input -> fetch -> output\n"
                               "}\n"
                               "stage main {\n"
                               "  input -> enrich -> persist\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t fetch_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t persist_ctx = {&trace, 2, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "enrich.fetch", record_stage, &fetch_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", record_stage, &persist_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("stops normal downstream execution when a stage fails") {
      static const char *src = "source input\n"
                               "stage parse\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> parse -> sink\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t parse_ctx = {&trace, 1, TURBO_EPROTO};
      publish_stage_ctx_t sink_ctx = {&trace, 2, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", record_stage, &parse_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EPROTO);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_EPROTO);
      check_equal(turbo_flow_last_error(flow)->line, 2);
      check_equal(turbo_flow_last_error(flow)->column, 7);
      check_contains(turbo_flow_last_error(flow)->message, "stage callback");
      check_equal(trace.count, 1);
      check_equal(trace.order[0], 1);

      parse_ctx.fail_status = TURBO_OK;
      trace.count = 0;
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("publishes owned payload by cloning it for runtime ownership") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      turbo_flow_msg_t msg;
      payload_check_ctx_t sink_ctx = {"owned-payload", 0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("owned-payload");
      msg.payload = tstr_to_v(msg.owned_payload);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(sink_ctx.called, 1);
      check_equal(msg.owned_payload, "owned-payload");

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects publish payload views without backing ownership") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      char raw[] = "borrowed";
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "backing");

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects publish payload views outside their declared backing buffer") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      char backing[] = "backing";
      char outside[] = "outside";
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.buffer = mem_wrap_external(backing, sizeof(backing) - 1u, NULL, NULL);
      check_not_null(msg.buffer);
      msg.payload = vstr_from_buf(outside, sizeof(outside) - 1u);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "payload");

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects batch payload views outside their declared backing buffer") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      char backing[] = "backing";
      char outside[] = "outside";
      batch_publish_probe_t sink = {{0}, 0u, 0u, TURBO_EPROTO};
      invalid_batch_payload_ctx_t invalid = {0};
      turbo_flow_publish_batch_config_t batch = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      size_t published = SIZE_MAX;

      check_not_null(flow);
      invalid.buffer = mem_wrap_external(backing, sizeof(backing) - 1u, NULL, NULL);
      invalid.payload = outside;
      invalid.payload_len = sizeof(outside) - 1u;
      check_not_null(invalid.buffer);
      batch.message_count = 1u;
      batch.prepare = batch_prepare_invalid_payload;
      batch.ctx = &invalid;

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", batch_publish_probe_stage, &sink,
                                                NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish_batch(flow, "input", &batch, &published), TURBO_EINVAL);
      check_equal(published, 0u);
      check_equal(invalid.calls, 1u);
      check_equal(sink.calls, 0u);
      check_equal(mem_buffer_ref_count(invalid.buffer), 1u);

      mem_buffer_release(invalid.buffer);
      turbo_flow_destroy(flow);
    }

    it("rejects async payload views outside their declared backing buffer") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
      char backing[] = "backing";
      char outside[] = "outside";
      async_completion_ctx_t completion;
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&completion.called, 0);
      atomic_init(&completion.last_status, TURBO_EBUSY);
      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.buffer = mem_wrap_external(backing, sizeof(backing) - 1u, NULL, NULL);
      check_not_null(msg.buffer);
      msg.payload = vstr_from_buf(outside, sizeof(outside) - 1u);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish_async(flow, "input", &msg, async_publish_complete, &completion),
                   TURBO_EINVAL);
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 0);

      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs inline worker-pool stages and thread executor baseline") {
      static const char *worker_src = "source input\n"
                                      "stage enrich worker 2\n"
                                      "stage persist\n"
                                      "stage main {\n"
                                      "  input -> enrich -> persist\n"
                                      "}\n";
      static const char *thread_src = "source input\n"
                                      "stage parse worker 2 exec thread workers 2\n"
                                      "stage sink\n"
                                      "stage main {\n"
                                      "  input -> parse -> sink\n"
                                      "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t enrich_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t persist_ctx = {&trace, 2, TURBO_OK};
      publish_stage_ctx_t parse_ctx = {&trace, 3, TURBO_OK};
      publish_stage_ctx_t sink_ctx = {&trace, 4, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, worker_src, strlen(worker_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich", record_stage, &enrich_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", record_stage, &persist_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 6);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(trace.order[2], 1);
      check_equal(trace.order[3], 2);
      check_equal(trace.order[4], 1);
      check_equal(trace.order[5], 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      trace.count = 0;
      check_equal(turbo_flow_parse_string(flow, thread_src, strlen(thread_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", record_stage, &parse_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 3);
      check_equal(trace.order[1], 4);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      trace.count = 0;
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 3);
      check_equal(trace.order[1], 4);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs coro scheduler executor stages to synchronous completion") {
      static const char *src = "source input\n"
                               "stage async exec coro lanes 2 pool 2\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> async -> sink\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      coro_check_ctx_t async_ctx = {0, 0};
      payload_check_ctx_t sink_ctx = {"abc", 0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "async", coro_check_stage, &async_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(async_ctx.entered, 2);
      check_equal(async_ctx.resumed, 2);
      check_equal(sink_ctx.called, 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(async_ctx.entered, 3);
      check_equal(async_ctx.resumed, 3);
      check_equal(sink_ctx.called, 3);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs executor stages with default thread and coro options") {
      static const char *thread_src = "source input\n"
                                      "stage parse exec thread\n"
                                      "stage sink\n"
                                      "stage main {\n"
                                      "  input -> parse -> sink\n"
                                      "}\n";
      static const char *coro_src = "source input\n"
                                    "stage async exec coro\n"
                                    "stage sink\n"
                                    "stage main {\n"
                                    "  input -> async -> sink\n"
                                    "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t parse_ctx = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t sink_ctx = {&trace, 2, TURBO_OK};
      coro_check_ctx_t async_ctx = {0, 0};
      payload_check_ctx_t coro_sink_ctx = {"abc", 0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, thread_src, strlen(thread_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", record_stage, &parse_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, coro_src, strlen(coro_src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "async", coro_check_stage, &async_ctx, NULL),
                   TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &coro_sink_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(async_ctx.entered, 1);
      check_equal(async_ctx.resumed, 1);
      check_equal(coro_sink_ctx.called, 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects coro executor stages that suspend without ready work") {
      static const char *src = "source input\n"
                               "stage async exec coro lanes 1 pool 1\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> async -> sink\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      coro_wait_ctx_t async_ctx = {0};
      payload_check_ctx_t sink_ctx = {"abc", 0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "async", coro_wait_once_stage, &async_ctx, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ENOTSUP);
      check_contains(turbo_flow_last_error(flow)->message, "suspended");
      check_equal(sink_ctx.called, 0);

      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(async_ctx.calls, 2);
      check_equal(sink_ctx.called, 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("moves worker-pool transformed owned payloads through the data plane") {
      static const char *src = "source input\n"
                               "stage transform worker 2\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> transform -> sink\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      payload_replace_ctx_t transform_ctx = {"worker-owned", 0};
      payload_check_ctx_t sink_ctx = {"worker-owned", 0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);
      check_equal(mem_buffer_ref_count(buffer), 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", replace_payload_stage,
                                                &transform_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(transform_ctx.called, 1);
      check_equal(sink_ctx.called, 1);
      check_equal(mem_buffer_ref_count(buffer), 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("runs worker-pool requests on independent disruptor consumers") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      worker_probe_ctx_t probe;
      worker_submit_ctx_t submits[2];
      turbo_thread_t threads[2] = {0};
      turbo_flow_t *flow = turbo_flow_create();
      flow_worker_pool_adapter_t *adapter;
      uint32_t stage_index;

      atomic_init(&probe.active, 0);
      atomic_init(&probe.peak, 0);
      atomic_init(&probe.calls, 0);
      atomic_init(&probe.ran_off_submitter, 0);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", worker_probe_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      stage_index = (uint32_t)turbo_flow_find_stage(flow, "transform");
      adapter = flow_worker_pool_adapter_for_stage(flow, stage_index);
      check_not_null(adapter);
      check_equal(adapter->width, 2);
      check_equal(adapter->capacity, 64);
      check_equal(disruptor_capacity(adapter->ring), 64);
      for (size_t i = 0; i < 2; ++i) {
        submits[i].adapter = adapter;
        atomic_init(&submits[i].result, TURBO_EALREADY);
        check_equal(turbo_thread_create(&threads[i], worker_submit_thread, &submits[i]), TURBO_OK);
      }
      for (size_t i = 0; i < 2; ++i) {
        check_equal(turbo_thread_join(&threads[i]), TURBO_OK);
        check_equal(atomic_load_explicit(&submits[i].result, memory_order_acquire), TURBO_OK);
      }

      check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), 2);
      check_equal(atomic_load_explicit(&probe.ran_off_submitter, memory_order_acquire), 1);
      check_greater_equal(atomic_load_explicit(&probe.peak, memory_order_acquire), 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("runs concurrent public publishes on independent disruptor consumers") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      worker_probe_ctx_t probe;
      concurrent_publish_t publishes[2];
      turbo_thread_t threads[2] = {0};
      atomic_int ready;
      atomic_int go;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&probe.active, 0);
      atomic_init(&probe.peak, 0);
      atomic_init(&probe.calls, 0);
      atomic_init(&probe.ran_off_submitter, 0);
      atomic_init(&ready, 0);
      atomic_init(&go, 0);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", worker_probe_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      for (size_t i = 0; i < 2; ++i) {
        memset(&publishes[i], 0, sizeof(publishes[i]));
        publishes[i].flow = flow;
        publishes[i].source_name = "input";
        publishes[i].msg_id = i + 1u;
        publishes[i].ready = &ready;
        publishes[i].go = &go;
        atomic_init(&publishes[i].result, TURBO_EBUSY);
        check_equal(turbo_thread_create(&threads[i], concurrent_publish_thread, &publishes[i]),
                     TURBO_OK);
      }
      while (atomic_load_explicit(&ready, memory_order_acquire) != 2)
        turbo_thread_yield();
      atomic_store_explicit(&go, 1, memory_order_release);
      for (size_t i = 0; i < 2; ++i) {
        check_equal(turbo_thread_join(&threads[i]), TURBO_OK);
        check_equal(atomic_load_explicit(&publishes[i].result, memory_order_acquire), TURBO_OK);
        check_equal(publishes[i].error_code, TURBO_OK);
      }
      check_greater_equal(atomic_load_explicit(&probe.peak, memory_order_acquire), 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("blocks bounded worker admission without dropping concurrent publishes") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 2\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      enum { PRODUCERS = 8 };
      worker_probe_ctx_t probe;
      concurrent_publish_t publishes[PRODUCERS];
      turbo_thread_t threads[PRODUCERS] = {0};
      atomic_int ready;
      atomic_int go;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&probe.active, 0);
      atomic_init(&probe.peak, 0);
      atomic_init(&probe.calls, 0);
      atomic_init(&probe.ran_off_submitter, 0);
      atomic_init(&ready, 0);
      atomic_init(&go, 0);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", worker_probe_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      for (size_t i = 0; i < PRODUCERS; ++i) {
        memset(&publishes[i], 0, sizeof(publishes[i]));
        publishes[i].flow = flow;
        publishes[i].source_name = "input";
        publishes[i].msg_id = i + 1u;
        publishes[i].ready = &ready;
        publishes[i].go = &go;
        atomic_init(&publishes[i].result, TURBO_EBUSY);
        check_equal(turbo_thread_create(&threads[i], concurrent_publish_thread, &publishes[i]),
                     TURBO_OK);
      }
      while (atomic_load_explicit(&ready, memory_order_acquire) != PRODUCERS)
        turbo_thread_yield();
      atomic_store_explicit(&go, 1, memory_order_release);
      for (size_t i = 0; i < PRODUCERS; ++i) {
        check_equal(turbo_thread_join(&threads[i]), TURBO_OK);
        check_equal(atomic_load_explicit(&publishes[i].result, memory_order_acquire), TURBO_OK);
      }
      check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), PRODUCERS);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("uses one coroutine shell pool per concurrent scheduler lane") {
      static const char *src = "source input\n"
                               "stage transform exec coro lanes 2 pool 8\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      worker_probe_ctx_t probe;
      concurrent_publish_t publishes[2];
      turbo_thread_t threads[2] = {0};
      atomic_int ready;
      atomic_int go;
      turbo_flow_t *flow = turbo_flow_create();
      flow_coro_adapter_t *adapter;
      uint32_t stage_index;

      atomic_init(&probe.active, 0);
      atomic_init(&probe.peak, 0);
      atomic_init(&probe.calls, 0);
      atomic_init(&probe.ran_off_submitter, 0);
      atomic_init(&ready, 0);
      atomic_init(&go, 0);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", worker_probe_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      stage_index = (uint32_t)turbo_flow_find_stage(flow, "transform");
      adapter = NULL;
      for (size_t i = 0; i < vec_size(&flow->coro_adapters); ++i) {
        flow_coro_adapter_t *candidate =
            (flow_coro_adapter_t *)vec_at(&flow->coro_adapters, i);
        if (candidate && candidate->stage_index == stage_index) {
          adapter = candidate;
          break;
        }
      }
      check_not_null(adapter);
      check_equal(adapter->lanes, 2);
      for (size_t i = 0; i < 2; ++i) {
        check_not_null(adapter->pools[i]);
        check_equal(turbo_coro_pool_capacity(adapter->pools[i]), 8);
        memset(&publishes[i], 0, sizeof(publishes[i]));
        publishes[i].flow = flow;
        publishes[i].source_name = "input";
        publishes[i].msg_id = i + 1u;
        publishes[i].ready = &ready;
        publishes[i].go = &go;
        atomic_init(&publishes[i].result, TURBO_EBUSY);
        check_equal(turbo_thread_create(&threads[i], concurrent_publish_thread, &publishes[i]),
                     TURBO_OK);
      }
      while (atomic_load_explicit(&ready, memory_order_acquire) != 2)
        turbo_thread_yield();
      atomic_store_explicit(&go, 1, memory_order_release);
      for (size_t i = 0; i < 2; ++i) {
        check_equal(turbo_thread_join(&threads[i]), TURBO_OK);
        check_equal(atomic_load_explicit(&publishes[i].result, memory_order_acquire), TURBO_OK);
      }
      check_greater_equal(atomic_load_explicit(&probe.peak, memory_order_acquire), 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("keeps concurrent publish errors local to each producer") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      concurrent_publish_t publishes[2];
      turbo_thread_t threads[2] = {0};
      atomic_int ready;
      atomic_int go;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&ready, 0);
      atomic_init(&go, 0);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", fail_by_message_id_stage, NULL, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      for (size_t i = 0; i < 2; ++i) {
        memset(&publishes[i], 0, sizeof(publishes[i]));
        publishes[i].flow = flow;
        publishes[i].source_name = "input";
        publishes[i].msg_id = i + 1u;
        publishes[i].ready = &ready;
        publishes[i].go = &go;
        atomic_init(&publishes[i].result, TURBO_EBUSY);
        check_equal(turbo_thread_create(&threads[i], concurrent_publish_thread, &publishes[i]),
                     TURBO_OK);
      }
      while (atomic_load_explicit(&ready, memory_order_acquire) != 2)
        turbo_thread_yield();
      atomic_store_explicit(&go, 1, memory_order_release);
      for (size_t i = 0; i < 2; ++i)
        check_equal(turbo_thread_join(&threads[i]), TURBO_OK);
      check_equal(atomic_load_explicit(&publishes[0].result, memory_order_acquire), TURBO_EIO);
      check_equal(publishes[0].error_code, TURBO_EIO);
      check_equal(atomic_load_explicit(&publishes[1].result, memory_order_acquire), TURBO_EPROTO);
      check_equal(publishes[1].error_code, TURBO_EPROTO);
      check_contains(publishes[0].error_message, "stage callback");
      check_contains(publishes[1].error_message, "stage callback");
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("drains accepted publishes before stop and rejects new admission") {
      static const char *src = "source input\n"
                               "stage transform worker 1 capacity 8\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      execution_probe_ctx_t probe;
      concurrent_publish_t publish;
      stop_flow_ctx_t stop;
      turbo_thread_t publish_thread = NULL;
      turbo_thread_t stop_thread = NULL;
      atomic_int ready;
      atomic_int go;
      turbo_flow_msg_t rejected;
      turbo_flow_t *flow = turbo_flow_create();
      int accepting = 1;

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 0);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 0;
      atomic_init(&ready, 0);
      atomic_init(&go, 1);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", execution_yield_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      memset(&publish, 0, sizeof(publish));
      publish.flow = flow;
      publish.source_name = "input";
      publish.ready = &ready;
      publish.go = &go;
      atomic_init(&publish.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&publish_thread, concurrent_publish_thread, &publish),
                   TURBO_OK);
      while (!atomic_load_explicit(&probe.entered, memory_order_acquire))
        turbo_thread_yield();

      stop.flow = flow;
      atomic_init(&stop.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&stop_thread, stop_flow_thread, &stop), TURBO_OK);
      while (accepting) {
        turbo_mutex_lock(&flow->runtime_mutex);
        accepting = flow->admission_state == FLOW_ADMISSION_OPEN;
        turbo_mutex_unlock(&flow->runtime_mutex);
        turbo_thread_yield();
      }
      turbo_flow_msg_init(&rejected);
      check_equal(turbo_flow_publish(flow, "input", &rejected), TURBO_ESHUTDOWN);
      turbo_flow_msg_cleanup(&rejected);
      atomic_store_explicit(&probe.allow_exit, 1, memory_order_release);
      check_equal(turbo_thread_join(&publish_thread), TURBO_OK);
      check_equal(turbo_thread_join(&stop_thread), TURBO_OK);
      check_equal(atomic_load_explicit(&publish.result, memory_order_acquire), TURBO_OK);
      check_equal(atomic_load_explicit(&stop.result, memory_order_acquire), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("runs async source ingress off the producer and owns the accepted message") {
      static const char *src = "source input\n"
                               "stage transform\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
      async_completion_ctx_t completion;
      async_gate_ctx_t gate;
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&completion.called, 0);
      atomic_init(&completion.last_status, TURBO_EBUSY);
      atomic_init(&gate.entered, 0);
      atomic_init(&gate.allow_exit, 0);
      atomic_init(&gate.calls, 0);
      atomic_init(&gate.ran_off_submitter, 0);
      check_not_null(flow);
      check_equal(turbo_flow_configure_async_ingress(flow, &ingress), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", async_gate_stage, &gate, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_configure_async_ingress(flow, &ingress), TURBO_EBUSY);

      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("async-owned");
      msg.payload = tstr_to_v(msg.owned_payload);
      worker_submitter_thread = 1;
      check_equal(
          turbo_flow_publish_async(flow, "input", &msg, async_publish_complete, &completion),
          TURBO_OK);
      worker_submitter_thread = 0;
      turbo_flow_msg_cleanup(&msg);
      for (int wait = 0; wait < 2000 && !atomic_load_explicit(&gate.entered, memory_order_acquire);
           ++wait) {
        turbo_sleep_ms(1);
      }
      check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
      check_equal(atomic_load_explicit(&gate.ran_off_submitter, memory_order_acquire), 1);
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 0);
      atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
      for (int wait = 0;
           wait < 2000 && !atomic_load_explicit(&completion.called, memory_order_acquire); ++wait) {
        turbo_sleep_ms(1);
      }
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 1);
      check_equal(atomic_load_explicit(&completion.last_status, memory_order_acquire), TURBO_OK);
      check_equal(atomic_load_explicit(&gate.calls, memory_order_acquire), 1);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("accepts the legacy async ingress ABI with bounded current defaults") {
      typedef struct legacy_async_ingress_config_s {
        size_t size;
        uint32_t workers;
        size_t queue_capacity;
      } legacy_async_ingress_config_t;
      legacy_async_ingress_config_t legacy = {sizeof(legacy), 2u, 7u};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_configure_async_ingress(
                      flow, (const turbo_flow_async_ingress_config_t *)&legacy),
                  TURBO_OK);
      check_equal(flow->async_ingress_config.workers, 2u);
      check_equal(flow->async_ingress_config.queue_capacity, 7u);
      check_equal(flow->async_ingress_config.max_message_bytes,
                  TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_MESSAGE_BYTES);
      check_equal(flow->async_ingress_config.max_inflight_bytes,
                  TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES);
      turbo_flow_destroy(flow);
    }

    it("bounds retained async message bytes and releases the reservation after completion") {
      static const char *src = "source input\n"
                               "stage transform\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
      async_completion_ctx_t completion;
      async_gate_ctx_t gate;
      turbo_flow_msg_t first;
      turbo_flow_msg_t second;
      turbo_flow_msg_t oversized;
      turbo_flow_msg_t pinned_buffer;
      char pinned_storage[9] = {0};
      turbo_flow_t *flow = turbo_flow_create();

      ingress.workers = 1u;
      ingress.queue_capacity = 4u;
      ingress.max_message_bytes = 8u;
      ingress.max_inflight_bytes = 10u;
      atomic_init(&completion.called, 0);
      atomic_init(&completion.last_status, TURBO_EBUSY);
      atomic_init(&gate.entered, 0);
      atomic_init(&gate.allow_exit, 0);
      atomic_init(&gate.calls, 0);
      atomic_init(&gate.ran_off_submitter, 0);
      check_not_null(flow);
      check_equal(turbo_flow_configure_async_ingress(flow, &ingress), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", async_gate_stage, &gate, NULL),
                  TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      turbo_flow_msg_init(&first);
      first.owned_payload = tstr_dup("12345678");
      first.payload = tstr_to_v(first.owned_payload);
      turbo_flow_msg_init(&second);
      second.owned_payload = tstr_dup("12345");
      second.payload = tstr_to_v(second.owned_payload);
      turbo_flow_msg_init(&oversized);
      oversized.owned_payload = tstr_dup("123456789");
      oversized.payload = tstr_to_v(oversized.owned_payload);
      turbo_flow_msg_init(&pinned_buffer);
      pinned_buffer.buffer =
          mem_wrap_external(pinned_storage, sizeof(pinned_storage), NULL, NULL);
      pinned_buffer.payload = vstr_from_buf(pinned_storage, 1u);

      check_equal(
          turbo_flow_publish_async(flow, "input", &first, async_publish_complete, &completion),
          TURBO_OK);
      for (int wait = 0; wait < 2000 && !atomic_load_explicit(&gate.entered, memory_order_acquire);
           ++wait) {
        turbo_sleep_ms(1);
      }
      check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
      check_equal(
          turbo_flow_publish_async(flow, "input", &second, async_publish_complete, &completion),
          TURBO_ENOSPC);
      check_equal(
          turbo_flow_publish_async(flow, "input", &oversized, async_publish_complete, &completion),
          TURBO_ENOSPC);
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 0);

      atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
      for (int wait = 0;
           wait < 2000 && atomic_load_explicit(&completion.called, memory_order_acquire) < 1;
           ++wait) {
        turbo_sleep_ms(1);
      }
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 1);
      check_equal(flow->async_ingress_inflight_bytes, 0u);
      check_equal(turbo_flow_publish_async(flow, "input", &pinned_buffer,
                                           async_publish_complete, &completion),
                  TURBO_ENOSPC);
      check_equal(
          turbo_flow_publish_async(flow, "input", &second, async_publish_complete, &completion),
          TURBO_OK);
      for (int wait = 0;
           wait < 2000 && atomic_load_explicit(&completion.called, memory_order_acquire) < 2;
           ++wait) {
        turbo_sleep_ms(1);
      }
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 2);
      check_equal(flow->async_ingress_inflight_bytes, 0u);

      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&pinned_buffer);
      turbo_flow_msg_cleanup(&oversized);
      turbo_flow_msg_cleanup(&second);
      turbo_flow_msg_cleanup(&first);
      turbo_flow_destroy(flow);
    }

    it("fails fast at async ingress capacity and drains accepted work on stop") {
      static const char *src = "source input\n"
                               "stage transform\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
      async_completion_ctx_t completion;
      async_gate_ctx_t gate;
      stop_flow_ctx_t stop;
      turbo_thread_t stop_thread = NULL;
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();
      int accepting = 1;

      ingress.workers = 1u;
      ingress.queue_capacity = 1u;
      atomic_init(&completion.called, 0);
      atomic_init(&completion.last_status, TURBO_EBUSY);
      atomic_init(&gate.entered, 0);
      atomic_init(&gate.allow_exit, 0);
      atomic_init(&gate.calls, 0);
      atomic_init(&gate.ran_off_submitter, 0);
      check_not_null(flow);
      check_equal(turbo_flow_configure_async_ingress(flow, &ingress), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", async_gate_stage, &gate, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      turbo_flow_msg_init(&msg);

      check_equal(
          turbo_flow_publish_async(flow, "input", &msg, async_publish_complete, &completion),
          TURBO_OK);
      for (int wait = 0; wait < 2000 && !atomic_load_explicit(&gate.entered, memory_order_acquire);
           ++wait) {
        turbo_sleep_ms(1);
      }
      check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
      check_equal(
          turbo_flow_publish_async(flow, "input", &msg, async_publish_complete, &completion),
          TURBO_OK);
      check_equal(
          turbo_flow_publish_async(flow, "input", &msg, async_publish_complete, &completion),
          TURBO_ENOSPC);

      stop.flow = flow;
      atomic_init(&stop.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&stop_thread, stop_flow_thread, &stop), TURBO_OK);
      while (accepting) {
        turbo_mutex_lock(&flow->runtime_mutex);
        accepting = flow->admission_state == FLOW_ADMISSION_OPEN;
        turbo_mutex_unlock(&flow->runtime_mutex);
        turbo_thread_yield();
      }
      check_equal(
          turbo_flow_publish_async(flow, "input", &msg, async_publish_complete, &completion),
          TURBO_ESHUTDOWN);
      atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
      check_equal(turbo_thread_join(&stop_thread), TURBO_OK);
      check_equal(atomic_load_explicit(&stop.result, memory_order_acquire), TURBO_OK);
      check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 2);
      check_equal(atomic_load_explicit(&completion.last_status, memory_order_acquire), TURBO_OK);
      check_equal(atomic_load_explicit(&gate.calls, memory_order_acquire), 2);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("pauses resumes and stops from paused admission") {
      static const char *src = "source input\n"
                               "stage transform\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_msg_t msg;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_pause(flow), TURBO_OK);
      check_equal(turbo_flow_pause(flow), TURBO_OK);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ESHUTDOWN);
      check_equal(turbo_flow_resume(flow), TURBO_OK);
      check_equal(turbo_flow_resume(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_pause(flow), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(turbo_flow_resume(flow), TURBO_EINVAL);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("interrupts parked disruptor workers on stop and restart") {
      static const char *src = "source input\n"
                               "stage transform worker 4 capacity 16\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_msg_t msg;
      turbo_flow_pool_snapshot_t pool;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_pool_count(flow), 1);
      check_equal(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
      check_equal(pool.kind, TURBO_FLOW_POOL_DISRUPTOR);
      check_equal(pool.state, TURBO_FLOW_POOL_RUNNING);
      check_equal(pool.parallelism, 4);
      check_equal(pool.queue_capacity, 16);
      check_equal(pool.queued, 0);
      check_equal(pool.active, 0);
      turbo_sleep_ms(20);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
      check_equal(pool.state, TURBO_FLOW_POOL_STOPPED);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_pool_count(flow), 1);
      check_equal(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
      check_equal(pool.submitted, 0);
      check_equal(pool.completed, 0);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
      check_equal(pool.submitted, 1);
      check_equal(pool.started, 1);
      check_equal(pool.completed, 1);
      check_equal(pool.failed, 0);
      check_equal(pool.queued, 0);
      check_equal(pool.active, 0);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("derives pool resource status and conditions from one generation snapshot") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 16\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      uint64_t started_generation;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      status.size = sizeof(status) - 1u;
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &status), TURBO_EINVAL);
      status = (turbo_flow_pool_resource_status_t)TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &status), TURBO_OK);
      check_equal(status.resource_kind, TURBO_FLOW_RESOURCE_POOL);
      check_equal(status.uid, "pool:2:transform");
      check_equal(status.owner_name, "transform");
      started_generation = status.generation;
      check_greater(started_generation, 0u);
      check_equal(status.observed_generation, status.generation);
      check_equal(status.snapshot.kind, TURBO_FLOW_POOL_DISRUPTOR);
      check_equal(status.snapshot.parallelism, 2u);
      check_equal(status.condition_count, TURBO_FLOW_RESOURCE_CONDITION_MAX);
      check_equal(status.conditions[0].kind, TURBO_FLOW_RESOURCE_CONDITION_READY);
      check_equal(status.conditions[0].status, TURBO_FLOW_CONDITION_TRUE);
      check_equal(status.conditions[0].reason, TURBO_FLOW_RESOURCE_REASON_RUNNING);
      check_equal(status.conditions[1].status, TURBO_FLOW_CONDITION_TRUE);
      check_equal(status.conditions[2].status, TURBO_FLOW_CONDITION_TRUE);
      check_equal(status.conditions[3].status, TURBO_FLOW_CONDITION_FALSE);
      check_equal(status.conditions[3].reason, TURBO_FLOW_RESOURCE_REASON_CAPACITY_AVAILABLE);

      check_equal(turbo_flow_stop(flow), TURBO_OK);
      status = (turbo_flow_pool_resource_status_t)TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &status), TURBO_OK);
      check_equal(status.generation, started_generation);
      check_equal(status.conditions[0].status, TURBO_FLOW_CONDITION_FALSE);
      check_equal(status.conditions[0].reason, TURBO_FLOW_RESOURCE_REASON_NOT_RUNNING);
      check_equal(status.conditions[1].status, TURBO_FLOW_CONDITION_FALSE);
      check_equal(status.conditions[2].status, TURBO_FLOW_CONDITION_TRUE);

      check_equal(turbo_flow_start(flow), TURBO_OK);
      status = (turbo_flow_pool_resource_status_t)TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &status), TURBO_OK);
      check_equal(status.uid, "pool:2:transform");
      check_equal(status.generation, started_generation + 1u);
      check_equal(status.observed_generation, status.generation);
      check_equal(status.conditions[0].status, TURBO_FLOW_CONDITION_TRUE);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("advances pool generation and rejects stale resize commands") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 16\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_pool_resource_status_t before = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      turbo_flow_pool_resource_status_t after = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      turbo_flow_pool_resize_command_t command;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &before), TURBO_OK);
      memset(&command, 0, sizeof(command));
      command.size = sizeof(command);
      command.stage_name = "transform";
      command.kind = TURBO_FLOW_POOL_DISRUPTOR;
      command.parallelism = 3u;
      command.drain_timeout_ms = UINT64_MAX;
      command.expected_generation = before.observed_generation;
      check_equal(turbo_flow_resize_pool(flow, &command), TURBO_OK);
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &after), TURBO_OK);
      check_equal(after.uid, before.uid);
      check_equal(after.generation, before.generation + 1u);
      check_equal(after.snapshot.parallelism, 3u);

      command.parallelism = 4u;
      check_equal(turbo_flow_resize_pool(flow, &command), TURBO_EBUSY);
      check_contains(turbo_flow_last_error(flow)->message, "generation conflict");
      after = (turbo_flow_pool_resource_status_t)TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      check_equal(turbo_flow_pool_resource_status_at(flow, 0, &after), TURBO_OK);
      check_equal(after.snapshot.parallelism, 3u);
      check_equal(after.generation, before.generation + 1u);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("rejects unchecked and truncated pool resize commands") {
      static const char *src = "source input\n"
                               "stage transform worker 2 capacity 16\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      turbo_flow_pool_resize_command_t command;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      memset(&command, 0, sizeof(command));
      command.size = offsetof(turbo_flow_pool_resize_command_t, expected_generation);
      command.stage_name = "transform";
      command.kind = TURBO_FLOW_POOL_DISRUPTOR;
      command.parallelism = 3u;
      command.drain_timeout_ms = UINT64_MAX;
      check_equal(turbo_flow_resize_pool(flow, &command), TURBO_EINVAL);
      command.size = sizeof(command);
      command.expected_generation = 0u;
      check_equal(turbo_flow_resize_pool(flow, &command), TURBO_EINVAL);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("resizes runtime-owned disruptor thread and coro pools") {
      static const char *worker_src = "source input\n"
                                      "stage transform worker 2 capacity 16\n"
                                      "stage main {\n"
                                      "  input -> transform\n"
                                      "}\n";
      static const char *thread_src = "source input\n"
                                      "stage transform exec thread workers 2\n"
                                      "stage main {\n"
                                      "  input -> transform\n"
                                      "}\n";
      static const char *coro_src = "source input\n"
                                    "stage transform exec coro lanes 2 pool 8\n"
                                    "stage main {\n"
                                    "  input -> transform\n"
                                    "}\n";
      const char *sources[] = {worker_src, thread_src, coro_src};
      const turbo_flow_pool_kind_t kinds[] = {TURBO_FLOW_POOL_DISRUPTOR, TURBO_FLOW_POOL_THREAD,
                                              TURBO_FLOW_POOL_CORO};

      for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); ++i) {
        turbo_flow_pool_resize_command_t command;
        turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
        turbo_flow_pool_snapshot_t pool;
        turbo_flow_msg_t msg;
        turbo_flow_t *flow = turbo_flow_create();

        check_not_null(flow);
        check_equal(turbo_flow_parse_string(flow, sources[i], strlen(sources[i])), TURBO_OK);
        check_equal(turbo_flow_register_stage_ex(flow, "transform", noop_stage, NULL, NULL),
                     TURBO_OK);
        check_equal(turbo_flow_compile(flow), TURBO_OK);
        check_equal(turbo_flow_start(flow), TURBO_OK);
        memset(&command, 0, sizeof(command));
        command.size = sizeof(command);
        command.stage_name = "transform";
        command.kind = kinds[i];
        command.parallelism = 3;
        command.drain_timeout_ms = UINT64_MAX;
        check_equal(turbo_flow_pool_resource_status_at(flow, 0u, &status), TURBO_OK);
        command.expected_generation = status.observed_generation;
        if (i == 0) check_equal(turbo_flow_pause(flow), TURBO_OK);
        check_equal(turbo_flow_resize_pool(flow, &command), TURBO_OK);
        check_equal(turbo_flow_pool_count(flow), 1);
        check_equal(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
        check_equal(pool.kind, kinds[i]);
        check_equal(pool.parallelism, 3);
        check_equal(pool.state, TURBO_FLOW_POOL_RUNNING);
        check_equal(pool.submitted, 0);
        turbo_flow_msg_init(&msg);
        if (i == 0) {
          check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ESHUTDOWN);
          check_equal(turbo_flow_resume(flow), TURBO_OK);
        }
        check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
        check_equal(turbo_flow_pool_snapshot_at(flow, 0, &pool), TURBO_OK);
        check_equal(pool.submitted, 1);
        check_equal(pool.completed, 1);
        check_equal(turbo_flow_stop(flow), TURBO_OK);
        check_equal(turbo_flow_resize_pool(flow, &command), TURBO_EINVAL);
        turbo_flow_msg_cleanup(&msg);
        turbo_flow_destroy(flow);
      }
    }

    it("validates pool resize targets and keeps timeout admission paused") {
      static const char *src = "source input\n"
                               "stage transform worker 1 capacity 8\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      execution_probe_ctx_t probe;
      concurrent_publish_t publish;
      turbo_thread_t publish_thread = NULL;
      atomic_int ready;
      atomic_int go;
      turbo_flow_pool_resize_command_t command;
      turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      turbo_flow_msg_t rejected;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 0);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 0;
      atomic_init(&ready, 0);
      atomic_init(&go, 1);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", execution_yield_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      memset(&command, 0, sizeof(command));
      command.size = sizeof(command);
      command.stage_name = "missing";
      command.kind = TURBO_FLOW_POOL_DISRUPTOR;
      command.parallelism = 2;
      command.expected_generation = 1u;
      check_equal(turbo_flow_resize_pool(flow, &command), TURBO_ENOENT);
      command.stage_name = "transform";
      check_equal(turbo_flow_pool_resource_status_at(flow, 0u, &status), TURBO_OK);
      command.expected_generation = status.observed_generation;

      memset(&publish, 0, sizeof(publish));
      publish.flow = flow;
      publish.source_name = "input";
      publish.ready = &ready;
      publish.go = &go;
      atomic_init(&publish.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&publish_thread, concurrent_publish_thread, &publish),
                   TURBO_OK);
      while (!atomic_load_explicit(&probe.entered, memory_order_acquire))
        turbo_thread_yield();
      check_equal(turbo_flow_resize_pool(flow, &command), TURBO_ETIMEDOUT);
      turbo_flow_msg_init(&rejected);
      check_equal(turbo_flow_publish(flow, "input", &rejected), TURBO_ESHUTDOWN);
      atomic_store_explicit(&probe.allow_exit, 1, memory_order_release);
      check_equal(turbo_thread_join(&publish_thread), TURBO_OK);
      check_equal(atomic_load_explicit(&publish.result, memory_order_acquire), TURBO_OK);
      check_equal(turbo_flow_resume(flow), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&rejected);
      turbo_flow_destroy(flow);
    }

    it("drains active publishes with deadline and remains paused") {
      static const char *src = "source input\n"
                               "stage transform worker 1 capacity 8\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      execution_probe_ctx_t probe;
      concurrent_publish_t publish;
      turbo_thread_t publish_thread = NULL;
      atomic_int ready;
      atomic_int go;
      turbo_flow_msg_t rejected;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 0);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 0;
      atomic_init(&ready, 0);
      atomic_init(&go, 1);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "transform", execution_yield_stage, &probe, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      memset(&publish, 0, sizeof(publish));
      publish.flow = flow;
      publish.source_name = "input";
      publish.ready = &ready;
      publish.go = &go;
      atomic_init(&publish.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&publish_thread, concurrent_publish_thread, &publish),
                   TURBO_OK);
      while (!atomic_load_explicit(&probe.entered, memory_order_acquire))
        turbo_thread_yield();

      check_equal(turbo_flow_drain(flow, 0), TURBO_ETIMEDOUT);
      turbo_flow_msg_init(&rejected);
      check_equal(turbo_flow_publish(flow, "input", &rejected), TURBO_ESHUTDOWN);
      atomic_store_explicit(&probe.allow_exit, 1, memory_order_release);
      check_equal(turbo_thread_join(&publish_thread), TURBO_OK);
      check_equal(turbo_flow_drain(flow, 100), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &rejected), TURBO_ESHUTDOWN);
      check_equal(turbo_flow_resume(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &rejected), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&rejected);
      turbo_flow_destroy(flow);
    }

    it("propagates worker failures and restarts the disruptor data plane") {
      static const char *src = "source input\n"
                               "stage transform worker 1\n"
                               "stage main {\n"
                               "  input -> transform\n"
                               "}\n";
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t stage_ctx = {&trace, 7, TURBO_EIO};
      turbo_flow_t *flow = turbo_flow_create();
      flow_worker_pool_adapter_t *adapter;
      flow_stage_completion_t completion = {0};
      turbo_flow_msg_t msg;
      uint32_t stage_index;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "transform", record_stage, &stage_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);

      stage_index = (uint32_t)turbo_flow_find_stage(flow, "transform");
      adapter = flow_worker_pool_adapter_for_stage(flow, stage_index);
      check_not_null(adapter);
      turbo_flow_msg_init(&msg);
      memset(&completion, 0, sizeof(completion));
      completion.entry = (flow_entry_header_t)FLOW_ENTRY_HEADER_INIT;
      completion.entry.runtime_generation = flow->runtime_generation;
      completion.entry.stage_index = stage_index;
      completion.entry.segment_kind = FLOW_DATA_SEGMENT_WORKER_POOL;
      completion.entry.ownership = FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE;
      completion.entry.sequence = 1u;
      completion.entry.message_id = msg.id;
      completion.entry.completion_handle = &completion;
      check_equal(flow_worker_pool_submit(adapter, &msg, &completion), TURBO_EIO);
      check_equal(completion.status, TURBO_EIO);
      check_less((int)completion.entry.worker_lane, (int)adapter->width);
      check_null(completion.entry.cancel_handle);
      check_equal(trace.count, 1);

      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_null(flow_worker_pool_adapter_for_stage(flow, stage_index));
      stage_ctx.fail_status = TURBO_OK;
      check_equal(turbo_flow_start(flow), TURBO_OK);
      adapter = flow_worker_pool_adapter_for_stage(flow, stage_index);
      check_not_null(adapter);
      check_equal(flow_worker_pool_submit(adapter, &msg, &completion), TURBO_EPROTO);
      check_equal((const void *)completion.entry.completion_handle, (const void *)&completion);
      check_equal(trace.count, 1);
      completion.entry.runtime_generation = flow->runtime_generation;
      completion.entry.completion_handle = &completion;
      completion.status = TURBO_OK;
      check_equal(flow_worker_pool_submit(adapter, &msg, &completion), TURBO_OK);
      check_equal(completion.status, TURBO_OK);
      check_less((int)completion.entry.worker_lane, (int)adapter->width);
      check_null(completion.entry.cancel_handle);
      check_equal(trace.count, 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("keeps borrowed transport context inline and rejects every asynchronous boundary") {
      static const char *broadcast_src = "source input\n"
                                         "stage sink\n"
                                         "stage main {\n"
                                         "  input -> sink\n"
                                         "}\n";
      static const char *worker_src = "source input\n"
                                      "stage sink worker 1\n"
                                      "stage main {\n"
                                      "  input -> sink\n"
                                      "}\n";
      static const char *thread_src = "source input\n"
                                      "stage sink exec thread workers 1\n"
                                      "stage main {\n"
                                      "  input -> sink\n"
                                      "}\n";
      static const char *coro_src = "source input\n"
                                    "stage sink exec coro lanes 1 pool 1\n"
                                    "stage main {\n"
                                    "  input -> sink\n"
                                    "}\n";
      const char *plans[] = {broadcast_src, worker_src, thread_src, coro_src};
      int transport_marker = 7;

      for (size_t i = 0; i < sizeof(plans) / sizeof(plans[0]); ++i) {
        publish_trace_t trace = {{0}, 0};
        publish_stage_ctx_t sink_ctx = {&trace, 1, TURBO_OK};
        turbo_flow_msg_t msg;
        turbo_flow_t *flow = turbo_flow_create();

        check_not_null(flow);
        turbo_flow_msg_init(&msg);
        msg.id = 100u + i;
        msg.transport_context = &transport_marker;
        check_equal(turbo_flow_parse_string(flow, plans[i], strlen(plans[i])), TURBO_OK);
        check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                     TURBO_OK);
        check_equal(turbo_flow_compile(flow), TURBO_OK);
        check_equal(turbo_flow_start(flow), TURBO_OK);
        check_equal(turbo_flow_publish(flow, "input", &msg), i == 0u ? TURBO_OK : TURBO_EINVAL);
        check_equal((const void *)msg.transport_context, (const void *)&transport_marker);
        check_equal(msg.id, 100u + i);
        check_equal(trace.count, i == 0u ? 1u : 0u);
        check_equal(turbo_flow_stop(flow), TURBO_OK);

        turbo_flow_msg_cleanup(&msg);
        turbo_flow_destroy(flow);
      }
    }

    it("moves buffer-backed transport context through every graph data plane") {
      static const char *broadcast_src = "source input\n"
                                         "stage sink\n"
                                         "stage main {\n"
                                         "  input -> sink\n"
                                         "}\n";
      static const char *worker_src = "source input\n"
                                      "stage sink worker 1\n"
                                      "stage main {\n"
                                      "  input -> sink\n"
                                      "}\n";
      static const char *thread_src = "source input\n"
                                      "stage sink exec thread workers 1\n"
                                      "stage main {\n"
                                      "  input -> sink\n"
                                      "}\n";
      static const char *coro_src = "source input\n"
                                    "stage sink exec coro lanes 1 pool 1\n"
                                    "stage main {\n"
                                    "  input -> sink\n"
                                    "}\n";
      const char *plans[] = {broadcast_src, worker_src, thread_src, coro_src};
      struct {
        int transport_marker;
        char payload[3];
      } owned = {7, {'a', 'b', 'c'}};

      for (size_t i = 0; i < sizeof(plans) / sizeof(plans[0]); ++i) {
        publish_trace_t trace = {{0}, 0};
        publish_stage_ctx_t sink_ctx = {&trace, 1, TURBO_OK};
        turbo_flow_msg_t msg;
        turbo_flow_t *flow = turbo_flow_create();

        check_not_null(flow);
        turbo_flow_msg_init(&msg);
        msg.id = 200u + i;
        msg.buffer = mem_wrap_external(&owned, sizeof(owned), NULL, NULL);
        check_not_null(msg.buffer);
        msg.payload = vstr_from_buf(owned.payload, sizeof(owned.payload));
        msg.transport_context = &owned.transport_marker;
        check_equal(turbo_flow_parse_string(flow, plans[i], strlen(plans[i])), TURBO_OK);
        check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                     TURBO_OK);
        check_equal(turbo_flow_compile(flow), TURBO_OK);
        check_equal(turbo_flow_start(flow), TURBO_OK);
        check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
        check_equal((const void *)msg.transport_context, (const void *)&owned.transport_marker);
        check_equal(msg.id, 200u + i);
        check_equal(trace.count, 1u);
        check_equal(turbo_flow_stop(flow), TURBO_OK);

        turbo_flow_msg_cleanup(&msg);
        turbo_flow_destroy(flow);
      }
    }

    it("returns thread executor message ownership before downstream release") {
      static const char *src = "source input\n"
                               "stage parse exec thread workers 2\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> parse -> sink\n"
                               "}\n";
      char raw[] = "abc";
      int expected_status = 77;
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "parse", set_msg_status_stage, &expected_status, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_msg_status_stage,
                                                &expected_status, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("does not release thread executor downstream when callback fails") {
      static const char *src = "source input\n"
                               "stage parse exec thread workers 2\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> parse -> sink\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t parse_ctx = {&trace, 1, TURBO_EPROTO};
      publish_stage_ctx_t sink_ctx = {&trace, 2, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "parse", record_stage, &parse_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EPROTO);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_EPROTO);
      check_contains(turbo_flow_last_error(flow)->message, "stage callback");
      check_equal(trace.count, 1);
      check_equal(trace.order[0], 1);

      parse_ctx.fail_status = TURBO_OK;
      trace.count = 0;
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("does not release worker-pool downstream when callback fails") {
      static const char *src = "source input\n"
                               "stage enrich worker 2\n"
                               "stage persist\n"
                               "stage main {\n"
                               "  input -> enrich -> persist\n"
                               "}\n";
      char raw[] = "abc";
      turbo_flow_msg_t msg;
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t enrich_ctx = {&trace, 1, TURBO_EPROTO};
      publish_stage_ctx_t persist_ctx = {&trace, 2, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "enrich", record_stage, &enrich_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", record_stage, &persist_ctx, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EPROTO);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_EPROTO);
      check_contains(turbo_flow_last_error(flow)->message, "stage callback");
      check_equal(trace.count, 1);
      check_equal(trace.order[0], 1);

      enrich_ctx.fail_status = TURBO_OK;
      trace.count = 0;
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(turbo_flow_stop(flow), TURBO_OK);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("filters routes using the upstream stage output") {
      static const char *src = "source input\n"
                               "stage validate\n"
                               "stage accepted\n"
                               "stage rejected\n"
                               "stage main {\n"
                               "  input -> validate\n"
                               "  route validate -> accepted when msg.flags == 7 # accepted\n"
                               "  route validate -> rejected when msg.flags != 7 %% rejected\n"
                               "}\n";
      uint32_t validated_flags = 7;
      turbo_flow_msg_t msg;
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t accepted = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t rejected = {&trace, 2, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_edge_plan_t *edge;

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "validate", set_flags_stage, &validated_flags, NULL),
          TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "accepted", record_stage, &accepted, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "rejected", record_stage, &rejected, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_edge_count(flow), 3);
      edge = turbo_flow_edge_at(flow, 1);
      check_not_null(edge);
      check_equal(edge->kind, TURBO_FLOW_EDGE_CONDITIONAL);
      check_equal(edge->condition, "msg.flags == 7");
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 1);
      check_equal(trace.order[0], 1);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("accepts route keywords in dotted adapter bindings") {
      static const char *src =
          "stage sink adapter bus.route operation rules.when resource channel.reject\n";
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_stage_count(flow), 1u);
      stage = turbo_flow_stage_at(flow, 0u);
      check_not_null(stage);
      check_equal(stage->adapter_name, "bus.route");
      check_equal(stage->operation_name, "rules.when");
      check_equal(stage->resource_name, "channel.reject");

      turbo_flow_destroy(flow);
    }

    it("treats a conditional no-match as a successful filtered branch") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  route input -> sink when msg.flags == 9\n"
                               "}\n";
      turbo_flow_msg_t msg;
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t sink = {&trace, 1, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 0);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("resolves conditional fan-in after every potential predecessor") {
      static const char *src = "source input\n"
                               "stage left\n"
                               "stage right\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> [left, right]\n"
                               "  route left -> sink when msg.flags == 1\n"
                               "  route right -> sink when msg.flags == 2\n"
                               "}\n";
      turbo_flow_msg_t msg;
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t left = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t right = {&trace, 2, TURBO_OK};
      publish_stage_ctx_t sink = {&trace, 3, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.flags = 1;
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "left", record_stage, &left, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "right", record_stage, &right, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", record_stage, &sink, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 3);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);
      check_equal(trace.order[2], 3);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects invalid conditional route expressions during compile") {
      static const char *non_bool = "source input\n"
                                    "stage sink\n"
                                    "stage main {\n"
                                    "  route input -> sink when msg.status + 1\n"
                                    "}\n";
      static const char *unknown = "source input\n"
                                   "stage sink\n"
                                   "stage main {\n"
                                   "  route input -> sink when parsed.missing == 1\n"
                                   "}\n";
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, non_bool, strlen(non_bool)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EPROTO);
      check_contains(turbo_flow_last_error(flow)->message, "BOOL");
      check_equal(turbo_flow_reset(flow, 1), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, unknown, strlen(unknown)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "schema resolver");

      turbo_flow_destroy(flow);
    }

    it("fails publish when a selected route predicate cannot be evaluated") {
      static const char *src = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  route input -> sink when msg.flags / msg.status > 0\n"
                               "}\n";
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.flags = 1;
      msg.status = 0;
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "route evaluation");

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("routes a stage failure through one named reject edge") {
      static const char *src = "source input\n"
                               "stage validate\n"
                               "stage persist\n"
                               "stage rejected\n"
                               "stage main {\n"
                               "  input -> validate\n"
                               "  validate -> persist\n"
                               "  reject validation_failed validate -> rejected\n"
                               "}\n";
      turbo_flow_msg_t msg;
      turbo_flow_msg_t snapshot;
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t validate = {&trace, 1, TURBO_EPROTO};
      publish_stage_ctx_t persist = {&trace, 2, TURBO_OK};
      failure_check_ctx_t rejected = {"validate",   NULL, "validation_failed",
                                      TURBO_EPROTO, 0,    &snapshot};
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_edge_plan_t *edge;

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      turbo_flow_msg_init(&snapshot);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      edge = turbo_flow_edge_at(flow, 2);
      check_not_null(edge);
      check_equal(edge->kind, TURBO_FLOW_EDGE_REJECT);
      check_equal(edge->name, "validation_failed");
      check_null(edge->condition);
      check_equal(turbo_flow_register_stage_ex(flow, "validate", record_stage, &validate, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", record_stage, &persist, NULL),
                   TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "rejected", check_failure_stage, &rejected, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(turbo_flow_last_error(flow)->code, TURBO_OK);
      check_equal(trace.count, 1);
      check_equal(trace.order[0], 1);
      check_equal(rejected.called, 1);
      check_equal(snapshot.failure.stage_name, "validate");
      check_equal(snapshot.failure.route_name, "validation_failed");
      check_equal(snapshot.failure.code, TURBO_EPROTO);

      turbo_flow_msg_cleanup(&snapshot);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("keeps reject inactive when the upstream stage succeeds") {
      static const char *src = "source input\n"
                               "stage validate\n"
                               "stage persist\n"
                               "stage rejected\n"
                               "stage main {\n"
                               "  input -> validate\n"
                               "  validate -> persist\n"
                               "  reject validation_failed validate -> rejected\n"
                               "}\n";
      turbo_flow_msg_t msg;
      publish_trace_t trace = {{0}, 0};
      publish_stage_ctx_t validate = {&trace, 1, TURBO_OK};
      publish_stage_ctx_t persist = {&trace, 2, TURBO_OK};
      publish_stage_ctx_t rejected = {&trace, 3, TURBO_OK};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "validate", record_stage, &validate, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "persist", record_stage, &persist, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "rejected", record_stage, &rejected, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(trace.count, 2);
      check_equal(trace.order[0], 1);
      check_equal(trace.order[1], 2);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("captures the adapter name for a handled adapter failure") {
      static const char *src = "source input\n"
                               "stage deliver adapter failing\n"
                               "stage rejected\n"
                               "stage main {\n"
                               "  input -> deliver\n"
                               "  reject delivery_failed deliver -> rejected\n"
                               "}\n";
      turbo_flow_msg_t msg;
      adapter_ctx_t adapter = {0};
      turbo_flow_adapter_ops_t ops;
      failure_check_ctx_t rejected = {"deliver", "failing", "delivery_failed", TURBO_EIO, 0, NULL};
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      adapter.fail_status = TURBO_EIO;
      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_register_adapter(flow, "failing", &ops, &adapter), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(
          turbo_flow_register_stage_ex(flow, "rejected", check_failure_stage, &rejected, NULL),
          TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(rejected.called, 1);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects ambiguous or invalid reject route declarations") {
      static const char *duplicate_name = "source input\n"
                                          "stage first\n"
                                          "stage second\n"
                                          "stage rejected\n"
                                          "stage main {\n"
                                          "  input -> first -> second\n"
                                          "  reject failed first -> rejected\n"
                                          "  reject failed second -> rejected\n"
                                          "}\n";
      static const char *multiple = "source input\n"
                                    "stage work\n"
                                    "stage first_reject\n"
                                    "stage second_reject\n"
                                    "stage main {\n"
                                    "  input -> work\n"
                                    "  reject first work -> first_reject\n"
                                    "  reject second work -> second_reject\n"
                                    "}\n";
      static const char *source_reject = "source input\n"
                                         "stage rejected\n"
                                         "stage main {\n"
                                         "  reject source_failed input -> rejected\n"
                                         "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      const char *names[] = {"first", "second", "rejected"};

      check_not_null(flow);
      register_stage_names(flow, names, 3);
      check_equal(turbo_flow_parse_string(flow, duplicate_name, strlen(duplicate_name)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "duplicate reject route name");

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, multiple, strlen(multiple)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "work", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "first_reject", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "second_reject", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EALREADY);
      check_contains(turbo_flow_last_error(flow)->message, "more than one reject route");

      check_equal(turbo_flow_reset(flow, 0), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, source_reject, strlen(source_reject)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "rejected", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "executable stage");

      turbo_flow_destroy(flow);
    }

    it("retries through the adapter ABI and commits only the successful attempt") {
      static const char *src = "source input\n"
                               "stage remote adapter retryable retry attempts 3 delay 1\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> remote -> sink\n"
                               "}\n";
      turbo_flow_msg_t msg;
      retry_adapter_ctx_t retry = {0};
      payload_check_ctx_t sink = {"success", 0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *stage;

      memset(&ops, 0, sizeof(ops));
      ops.consume = retry_adapter_consume;
      ops.consume_retry = retry_adapter_consume_retry;
      retry.succeed_on = 3u;
      retry.failure_status = TURBO_EIO;
      retry.wait_status = TURBO_OK;
      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("original");
      msg.payload = tstr_to_v(msg.owned_payload);
      check_equal(turbo_flow_register_adapter(flow, "retryable", &ops, &retry), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink, NULL),
                   TURBO_OK);
      stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "remote"));
      check_not_null(stage);
      check_equal(stage->retry.max_attempts, 3u);
      check_equal(stage->retry.delay_ms, 1u);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(retry.attempts, 3u);
      check_equal(retry.waits, 2u);
      check_equal(sink.called, 1);
      check_equal(msg.payload.data, "original");

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("routes exhausted retries to a dead-letter adapter with the original payload") {
      static const char *src = "source input\n"
                               "stage remote adapter retryable retry attempts 3\n"
                               "stage dead adapter deadletter\n"
                               "stage main {\n"
                               "  input -> remote\n"
                               "  reject remote_dead remote -> dead\n"
                               "}\n";
      turbo_flow_msg_t msg;
      retry_adapter_ctx_t retry = {0};
      failure_check_ctx_t dead = {.stage_name = "remote",
                                  .adapter_name = "retryable",
                                  .route_name = "remote_dead",
                                  .code = TURBO_EIO,
                                  .expected_payload = "original",
                                  .expected_attempt = 3u};
      turbo_flow_adapter_ops_t retry_ops;
      turbo_flow_adapter_ops_t dead_ops;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&retry_ops, 0, sizeof(retry_ops));
      retry_ops.consume = retry_adapter_consume;
      retry_ops.consume_retry = retry_adapter_consume_retry;
      memset(&dead_ops, 0, sizeof(dead_ops));
      dead_ops.consume = failure_adapter_consume;
      retry.failure_status = TURBO_EIO;
      retry.wait_status = TURBO_OK;
      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("original");
      msg.payload = tstr_to_v(msg.owned_payload);
      check_equal(turbo_flow_register_adapter(flow, "retryable", &retry_ops, &retry), TURBO_OK);
      check_equal(turbo_flow_register_adapter(flow, "deadletter", &dead_ops, &dead), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(retry.attempts, 3u);
      check_equal(retry.waits, 0u);
      check_equal(dead.called, 1);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("propagates shutdown from an adapter retry delay without another attempt") {
      static const char *src = "source input\n"
                               "stage remote adapter retryable retry attempts 3 delay 10\n"
                               "stage main {\n"
                               "  input -> remote\n"
                               "}\n";
      turbo_flow_msg_t msg;
      retry_adapter_ctx_t retry = {0};
      turbo_flow_adapter_ops_t ops;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&ops, 0, sizeof(ops));
      ops.consume = retry_adapter_consume;
      ops.consume_retry = retry_adapter_consume_retry;
      retry.failure_status = TURBO_EIO;
      retry.wait_status = TURBO_ESHUTDOWN;
      check_not_null(flow);
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("original");
      msg.payload = tstr_to_v(msg.owned_payload);
      check_equal(turbo_flow_register_adapter(flow, "retryable", &ops, &retry), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ESHUTDOWN);
      check_equal(retry.attempts, 1u);
      check_equal(retry.waits, 1u);
      check_equal(msg.payload.data, "original");

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects invalid retry syntax and adapters without retry ownership") {
      static const char *too_few = "stage remote adapter retryable retry attempts 1\n";
      static const char *too_many = "stage remote adapter retryable retry attempts 65\n";
      static const char *too_long =
          "stage remote adapter retryable retry attempts 2 delay 3600001\n";
      static const char *unsupported = "source input\n"
                                       "stage remote adapter normal retry attempts 2\n"
                                       "stage main {\n"
                                       "  input -> remote\n"
                                       "}\n";
      turbo_flow_adapter_ops_t ops;
      adapter_ctx_t adapter = {0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, too_few, strlen(too_few)), TURBO_ERANGE);
      check_equal(turbo_flow_parse_string(flow, too_many, strlen(too_many)), TURBO_ERANGE);
      check_equal(turbo_flow_parse_string(flow, too_long, strlen(too_long)), TURBO_ERANGE);
      memset(&ops, 0, sizeof(ops));
      ops.consume = test_adapter_consume;
      check_equal(turbo_flow_register_adapter(flow, "normal", &ops, &adapter), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, unsupported, strlen(unsupported)), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_ENOTSUP);
      check_contains(turbo_flow_last_error(flow)->message, "adapter retry callback");

      turbo_flow_destroy(flow);
    }

    it("accepts bounded reorder boundaries before ordered fan-in") {
      static const char *src = "source input\n"
                               "stage enrich worker 2\n"
                               "stage ordered reorder capacity 32 timeout 100\n"
                               "stage metrics\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> [enrich, metrics]\n"
                               "  enrich -> ordered -> sink\n"
                               "  metrics -> sink\n"
                               "}\n";
      const char *names[] = {"enrich", "ordered", "metrics", "sink"};
      turbo_flow_t *flow = turbo_flow_create();
      const turbo_flow_stage_plan_t *ordered;

      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      register_stage_names(flow, names, sizeof(names) / sizeof(names[0]));
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      ordered = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "ordered"));
      check_not_null(ordered);
      check_equal(ordered->reorder.capacity, 32);
      check_equal(ordered->reorder.timeout_ms, 100);
      turbo_flow_destroy(flow);
    }

    it("uses boundary-local reorder tickets across independent sources") {
      static const char *src = "source ordered_input\n"
                               "source other_input\n"
                               "stage before worker 2 capacity 16\n"
                               "stage ordered reorder capacity 16 timeout 100\n"
                               "stage other\n"
                               "stage main {\n"
                               "  ordered_input -> before -> ordered\n"
                               "  other_input -> other\n"
                               "}\n";
      worker_probe_ctx_t probe;
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      atomic_init(&probe.active, 0);
      atomic_init(&probe.peak, 0);
      atomic_init(&probe.calls, 0);
      atomic_init(&probe.ran_off_submitter, 0);
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "before", worker_probe_stage, &probe, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "ordered", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "other", noop_stage, NULL, NULL), TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      turbo_flow_msg_init(&msg);
      check_equal(turbo_flow_publish(flow, "other_input", &msg), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "ordered_input", &msg), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }

    it("rejects invalid reorder bounds") {
      static const char *zero = "source input\n"
                                "stage ordered reorder capacity 0 timeout 100\n"
                                "stage main {\n"
                                "  input -> ordered\n"
                                "}\n";
      static const char *no_timeout = "source input\n"
                                      "stage ordered reorder capacity 2 timeout 0\n"
                                      "stage main {\n"
                                      "  input -> ordered\n"
                                      "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      check_equal(turbo_flow_parse_string(flow, zero, strlen(zero)), TURBO_ERANGE);
      check_equal(turbo_flow_reset(flow, 1), TURBO_OK);
      check_equal(turbo_flow_parse_string(flow, no_timeout, strlen(no_timeout)), TURBO_ERANGE);
      turbo_flow_destroy(flow);
    }

    it("releases out-of-order completion in sequence order") {
      uint32_t stage_index = 0;
      turbo_flow_t *flow = reorder_test_flow(2, 1000, &stage_index);
      reorder_wait_t wait = {0};
      turbo_thread_t thread;
      check_not_null(flow);
      wait.flow = flow;
      wait.stage_index = stage_index;
      wait.sequence = 2;
      wait.leave_on_success = 1;
      atomic_init(&wait.started, 0);
      atomic_init(&wait.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&thread, reorder_wait_thread, &wait), TURBO_OK);
      while (!atomic_load_explicit(&wait.started, memory_order_acquire))
        turbo_sleep_ms(1);
      turbo_sleep_ms(10);
      check_equal(atomic_load_explicit(&wait.result, memory_order_acquire), TURBO_EBUSY);
      check_equal(flow_reorder_enter(flow, stage_index, 1), TURBO_OK);
      flow_reorder_leave(flow, stage_index, 1);
      check_equal(turbo_thread_join(&thread), TURBO_OK);
      check_equal(atomic_load_explicit(&wait.result, memory_order_acquire), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("reports missing sequence timeout and bounded overflow") {
      uint32_t stage_index = 0;
      turbo_flow_t *flow = reorder_test_flow(1, 25, &stage_index);
      check_not_null(flow);
      check_equal(flow_reorder_enter(flow, stage_index, 2), TURBO_ETIMEDOUT);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);

      flow = reorder_test_flow(1, 1000, &stage_index);
      check_not_null(flow);
      check_equal(flow_reorder_enter(flow, stage_index, 1), TURBO_OK);
      reorder_wait_t wait = {0};
      turbo_thread_t thread;
      wait.flow = flow;
      wait.stage_index = stage_index;
      wait.sequence = 2;
      wait.leave_on_success = 1;
      atomic_init(&wait.started, 0);
      atomic_init(&wait.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&thread, reorder_wait_thread, &wait), TURBO_OK);
      while (!atomic_load_explicit(&wait.started, memory_order_acquire))
        turbo_sleep_ms(1);
      turbo_sleep_ms(5);
      check_equal(flow_reorder_enter(flow, stage_index, 3), TURBO_ENOSPC);
      flow_reorder_leave(flow, stage_index, 1);
      check_equal(turbo_thread_join(&thread), TURBO_OK);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("bounds and releases reorder ticket reservations") {
      uint32_t stage_index = 0;
      uint64_t first = 0;
      uint64_t second = 0;
      uint64_t third = 0;
      turbo_flow_t *flow = reorder_test_flow(1, 1000, &stage_index);

      check_not_null(flow);
      check_equal(flow_reorder_reserve(flow, stage_index, &first), TURBO_OK);
      check_equal(first, 1);
      check_equal(flow_reorder_reserve(flow, stage_index, &second), TURBO_OK);
      check_equal(second, 2);
      check_equal(flow_reorder_reserve(flow, stage_index, &third), TURBO_ENOSPC);
      check_equal(flow_reorder_cancel(flow, stage_index, first), TURBO_OK);
      check_equal(flow_reorder_reserve(flow, stage_index, &third), TURBO_OK);
      check_equal(third, 3);
      check_equal(flow_reorder_enter(flow, stage_index, second), TURBO_OK);
      flow_reorder_leave(flow, stage_index, second);
      check_equal(flow_reorder_enter(flow, stage_index, third), TURBO_OK);
      flow_reorder_leave(flow, stage_index, third);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("wakes reorder waiters during shutdown") {
      uint32_t stage_index = 0;
      turbo_flow_t *flow = reorder_test_flow(1, 1000, &stage_index);
      reorder_wait_t wait = {0};
      turbo_thread_t thread;
      check_not_null(flow);
      wait.flow = flow;
      wait.stage_index = stage_index;
      wait.sequence = 2;
      atomic_init(&wait.started, 0);
      atomic_init(&wait.result, TURBO_EBUSY);
      check_equal(turbo_thread_create(&thread, reorder_wait_thread, &wait), TURBO_OK);
      while (!atomic_load_explicit(&wait.started, memory_order_acquire))
        turbo_sleep_ms(1);
      flow_stop_reorder_states(flow);
      check_equal(turbo_thread_join(&thread), TURBO_OK);
      check_equal(atomic_load_explicit(&wait.result, memory_order_acquire), TURBO_ESHUTDOWN);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }
  }

  group("structured observers") {
    it("observes source stage route sink and completion without controlling the flow") {
      static const char *src = "source input\n"
                               "stage validate\n"
                               "stage accepted\n"
                               "stage rejected\n"
                               "stage main {\n"
                               "  input -> validate\n"
                               "  route validate -> accepted when msg.flags == 1\n"
                               "  reject failed validate -> rejected\n"
                               "}\n";
      observer_probe_t probe;
      turbo_flow_event_observer_ops_t ops = TURBO_FLOW_EVENT_OBSERVER_OPS_INIT;
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();

      memset(&probe, 0, sizeof(probe));
      probe.fail_kind = TURBO_FLOW_OBSERVE_ROUTE_EVALUATED;
      ops.on_event = observer_probe_on_event;
      ops.destroy = observer_probe_destroy;

      check_not_null(flow);
      check_equal(turbo_flow_register_observer(flow, "probe", &ops, &probe), TURBO_OK);
      check_equal(turbo_flow_register_observer(flow, "probe", &ops, &probe), TURBO_EALREADY);
      check_equal(turbo_flow_observer_count(flow), 1u);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "validate", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "accepted", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "rejected", noop_stage, NULL, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_unregister_observer(flow, "probe"), TURBO_EBUSY);

      turbo_flow_msg_init(&msg);
      msg.flags = 1u;
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      check_equal(probe.event_counts[TURBO_FLOW_OBSERVE_SOURCE_RECEIVED], 1u);
      check_equal(probe.event_counts[TURBO_FLOW_OBSERVE_FLOW_COMPLETE], 1u);
      check_equal(probe.event_counts[TURBO_FLOW_OBSERVE_STAGE_BEGIN], 2u);
      check_equal(probe.event_counts[TURBO_FLOW_OBSERVE_STAGE_END], 2u);
      check_equal(probe.event_counts[TURBO_FLOW_OBSERVE_ROUTE_EVALUATED], 3u);
      check_equal(probe.event_counts[TURBO_FLOW_OBSERVE_SINK_COMPLETE], 1u);
      check_equal(probe.selected_routes, 2u);
      check_true(turbo_flow_observer_failure_count(flow) >= 1u);

      check_equal(turbo_flow_stop(flow), TURBO_OK);
      check_equal(turbo_flow_unregister_observer(flow, "probe"), TURBO_OK);
      check_equal(probe.destroy_count, 1);
      check_equal(turbo_flow_observer_count(flow), 0u);

      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }
  }

  group("execution primitives") {
    it("submits waits and yields on a thread task") {
      flow_execution_task_t task;
      flow_stage_completion_t completion = {0};
      execution_probe_ctx_t probe;
      turbo_flow_msg_t input;
      turbo_flow_msg_t output;
      turbo_thread_t thread = NULL;

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 1);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 2;
      turbo_flow_msg_init(&input);
      turbo_flow_msg_init(&output);
      check_equal(flow_execution_task_init(&task, FLOW_EXECUTION_THREAD, execution_yield_stage,
                                            &probe, &input, &completion, 0u),
                   TURBO_OK);
      check_equal(flow_execution_task_state(&task), FLOW_EXECUTION_ACCEPTED);
      check_equal(turbo_thread_create(&thread, execution_task_thread, &task), TURBO_OK);
      check_equal(flow_execution_task_wait(&task, &output, &completion), TURBO_OK);
      check_equal(turbo_thread_join(&thread), TURBO_OK);
      check_equal(flow_execution_task_state(&task), FLOW_EXECUTION_COMPLETED);
      check_equal(atomic_load_explicit(&probe.entered, memory_order_acquire), 1);
      flow_execution_task_cleanup(&task);
      turbo_flow_msg_cleanup(&output);
    }

    it("reports a cooperative task deadline separately from explicit abort") {
      flow_execution_task_t task;
      flow_stage_completion_t completion = {0};
      execution_probe_ctx_t probe;
      turbo_flow_msg_t input;
      turbo_flow_msg_t output;
      turbo_thread_t thread = NULL;

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 0);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 0;
      turbo_flow_msg_init(&input);
      turbo_flow_msg_init(&output);
      check_equal(flow_execution_task_init(&task, FLOW_EXECUTION_THREAD, execution_yield_stage,
                                            &probe, &input, &completion, 5u),
                   TURBO_OK);
      check_equal(turbo_thread_create(&thread, execution_task_thread, &task), TURBO_OK);
      check_equal(flow_execution_task_wait(&task, &output, &completion), TURBO_ETIMEDOUT);
      check_equal(turbo_thread_join(&thread), TURBO_OK);
      check_equal(flow_execution_task_state(&task), FLOW_EXECUTION_COMPLETED);
      check_equal(completion.status, TURBO_ETIMEDOUT);
      check_equal(atomic_load_explicit(&probe.entered, memory_order_acquire), 1);
      check_equal(atomic_load_explicit(&probe.saw_cancel, memory_order_acquire), 0);
      flow_execution_task_cleanup(&task);
      turbo_flow_msg_cleanup(&output);
    }

    it("aborts accepted and running tasks cooperatively") {
      flow_execution_task_t accepted;
      flow_execution_task_t running;
      flow_stage_completion_t completion = {0};
      execution_probe_ctx_t probe;
      turbo_flow_msg_t input;
      turbo_flow_msg_t output;
      turbo_thread_t thread = NULL;

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 0);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 0;
      turbo_flow_msg_init(&input);
      turbo_flow_msg_init(&output);
      check_equal(flow_execution_task_init(&accepted, FLOW_EXECUTION_THREAD, execution_yield_stage,
                                            &probe, &input, &completion, 0u),
                   TURBO_OK);
      check_equal(flow_execution_task_abort(&accepted), TURBO_OK);
      check_equal(flow_execution_task_wait(&accepted, &output, &completion), TURBO_ECANCELED);
      check_equal(flow_execution_task_state(&accepted), FLOW_EXECUTION_CANCELED);
      flow_execution_task_cleanup(&accepted);
      turbo_flow_msg_cleanup(&output);

      turbo_flow_msg_init(&input);
      turbo_flow_msg_init(&output);
      check_equal(flow_execution_task_init(&running, FLOW_EXECUTION_THREAD, execution_yield_stage,
                                            &probe, &input, &completion, 0u),
                   TURBO_OK);
      check_equal(turbo_thread_create(&thread, execution_task_thread, &running), TURBO_OK);
      while (!atomic_load_explicit(&probe.entered, memory_order_acquire))
        turbo_thread_yield();
      check_equal(flow_execution_task_abort(&running), TURBO_OK);
      check_equal(flow_execution_task_wait(&running, &output, &completion), TURBO_ECANCELED);
      check_equal(turbo_thread_join(&thread), TURBO_OK);
      check_equal(atomic_load_explicit(&probe.saw_cancel, memory_order_acquire), 1);
      flow_execution_task_cleanup(&running);
      turbo_flow_msg_cleanup(&output);
    }

    it("yields through the coroutine backend") {
      flow_execution_task_t task;
      flow_stage_completion_t completion = {0};
      execution_probe_ctx_t probe;
      turbo_flow_msg_t input;
      turbo_flow_msg_t output;
      coro_scheduler_t *scheduler = coro_scheduler_create();

      atomic_init(&probe.entered, 0);
      atomic_init(&probe.allow_exit, 1);
      atomic_init(&probe.saw_cancel, 0);
      probe.yields = 2;
      turbo_flow_msg_init(&input);
      turbo_flow_msg_init(&output);
      check_not_null(scheduler);
      check_equal(flow_execution_task_init(&task, FLOW_EXECUTION_CORO, execution_yield_stage,
                                            &probe, &input, &completion, 0u),
                   TURBO_OK);
      check_not_null(coro_spawn(scheduler, execution_task_coro, &task, NULL));
      coro_scheduler_run(scheduler);
      check_equal(flow_execution_task_wait(&task, &output, &completion), TURBO_OK);
      check_equal(flow_execution_task_state(&task), FLOW_EXECUTION_COMPLETED);
      check_equal(atomic_load_explicit(&probe.entered, memory_order_acquire), 1);
      flow_execution_task_cleanup(&task);
      turbo_flow_msg_cleanup(&output);
      coro_scheduler_destroy(scheduler);
    }

    it("exposes cooperative abort controls to DSL stage callbacks") {
      static const char *src = "source input\n"
                               "stage cancel exec thread workers 1\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> cancel -> sink\n"
                               "}\n";
      char raw[] = "cancel";
      mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = turbo_flow_create();
      int cancel_called = 0;
      payload_check_ctx_t sink = {"cancel", 0};

      check_not_null(flow);
      check_not_null(buffer);
      turbo_flow_msg_init(&msg);
      msg.buffer = buffer;
      msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);
      check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "cancel", execution_self_abort_stage,
                                                &cancel_called, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "sink", check_payload_stage, &sink, NULL),
                   TURBO_OK);
      check_equal(turbo_flow_compile(flow), TURBO_OK);
      check_equal(turbo_flow_start(flow), TURBO_OK);
      check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ECANCELED);
      check_equal(cancel_called, 1);
      check_equal(sink.called, 0);
      check_equal(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }
  }
}
