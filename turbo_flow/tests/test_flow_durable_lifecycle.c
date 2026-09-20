#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_projection.h"
#include "../src/flow_internal.h"
#include "../src/flow_inbox_driver_internal.h"

#include <stdlib.h>
#include <string.h>

/* Exercise real bounded storage while injecting only the provider boundary faults. */
typedef struct lifecycle_fixture_s {
  turbo_flow_t *flow;
  turbo_flow_inbox_t backing;
  turbo_flow_inbox_t provider;
  turbo_flow_durable_buffer_binding_t *binding;
  size_t calls;
  atomic_size_t sinks;
  atomic_size_t terminal_slots;
  size_t owner_releases;
  int complete_status;
  size_t completions;
  int sink_status;
  int asynchronous;
  turbo_flow_async_terminal_claim_t terminal;
  turbo_flow_async_terminal_claim_t terminal2;
  int admit_status;
  int snapshot_status;
  uint64_t generation;
} lifecycle_fixture_t;

static int probe_admit(void *ctx, const turbo_flow_inbox_record_t *record,
                       turbo_flow_inbox_receipt_t *receipt) {
  lifecycle_fixture_t *f = ctx;
  ++f->calls;
  if (f->admit_status != SALTS_OK) return f->admit_status;
  return turbo_flow_inbox_admit(&f->backing, record, receipt);
}
static int probe_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  lifecycle_fixture_t *f = ctx;
  int rc;
  if (f->snapshot_status != SALTS_OK) return f->snapshot_status;
  rc = turbo_flow_inbox_snapshot(&f->backing, snapshot);
  if (rc == SALTS_OK) snapshot->generation = f->generation;
  return rc;
}
static int probe_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  return turbo_flow_inbox_claim(&((lifecycle_fixture_t *)ctx)->backing, claim);
}
static int probe_claim_ex(void *ctx, const turbo_flow_inbox_claim_request_t *request,
                          turbo_flow_inbox_claim_t *claim) {
  return turbo_flow_inbox_claim_ex(&((lifecycle_fixture_t *)ctx)->backing, request, claim);
}
static int probe_complete(void *ctx, uint64_t id, uint64_t token) {
  lifecycle_fixture_t *f = ctx;
  ++f->completions;
  if (f->complete_status) {
    if (f->complete_status == SALTS_EALREADY)
      check_equal(f->backing.ops->complete(f->backing.ctx, id, token), SALTS_OK);
    return f->complete_status;
  }
  return f->backing.ops->complete(f->backing.ctx, id, token);
}
static int probe_fail(void *ctx, uint64_t id, uint64_t token, int status) {
  lifecycle_fixture_t *f = ctx;
  return f->backing.ops->fail(f->backing.ctx, id, token, status);
}
static int probe_retry(void *ctx, uint64_t id) {
  return turbo_flow_inbox_retry(&((lifecycle_fixture_t *)ctx)->backing, id);
}
static int probe_discard(void *ctx, uint64_t id) {
  return turbo_flow_inbox_discard(&((lifecycle_fixture_t *)ctx)->backing, id);
}
static int probe_forget(void *ctx, uint64_t id) {
  return turbo_flow_inbox_forget(&((lifecycle_fixture_t *)ctx)->backing, id);
}
static int probe_scan_failed(void *ctx, uint64_t after, turbo_flow_inbox_failed_entry_t *entries,
                              size_t capacity, size_t *count) {
  return turbo_flow_inbox_scan_failed(&((lifecycle_fixture_t *)ctx)->backing,
                                      after, entries, capacity, count);
}
static int probe_scan_history(void *ctx, uint64_t after, turbo_flow_inbox_history_entry_t *entries,
                               size_t capacity, size_t *count) {
  return turbo_flow_inbox_scan_history(&((lifecycle_fixture_t *)ctx)->backing,
                                       after, entries, capacity, count);
}
static int probe_close(void *ctx) {
  return turbo_flow_inbox_close(&((lifecycle_fixture_t *)ctx)->backing);
}
static int probe_destroy(void *ctx) {
  return turbo_flow_inbox_destroy(&((lifecycle_fixture_t *)ctx)->backing);
}
static const turbo_flow_inbox_ops_v2_t probe_ops = {
    .size = sizeof(turbo_flow_inbox_ops_v2_t),
    .version = TURBO_FLOW_INBOX_API_VERSION,
    .admit = probe_admit,
    .claim = probe_claim,
    .claim_ex = probe_claim_ex,
    .complete = probe_complete,
    .fail = probe_fail,
    .retry = probe_retry,
    .discard = probe_discard,
    .forget = probe_forget,
    .scan_failed = probe_scan_failed,
    .scan_history = probe_scan_history,
    .close = probe_close,
    .snapshot = probe_snapshot,
    .destroy = probe_destroy};
#include <salts/clock.h>

static int sink(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                turbo_flow_msg_t *msg) {
  lifecycle_fixture_t *f = ctx;
  (void)flow; (void)stage; (void)msg;
  ++f->sinks;
  return f->sink_status;
}
static int submit(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                  const turbo_flow_msg_t *msg, turbo_flow_async_terminal_claim_t *claim) {
  lifecycle_fixture_t *f = ctx;
  (void)flow; (void)stage; (void)msg;
  const size_t slot = atomic_fetch_add(&f->terminal_slots, (size_t)1u);
  turbo_flow_async_terminal_claim_t *destination =
      (slot & 1u) == 0u ? &f->terminal : &f->terminal2;
  int rc = turbo_flow_async_terminal_claim_move(destination, claim);
  if (rc == SALTS_OK) ++f->sinks;
  return rc;
}
static void open_fixture_mode(lifecycle_fixture_t *f, int asynchronous,
                              turbo_flow_durable_identity_mode_t identity_mode) {
  static const char graph[] = "source input\n"
    "buffer intake resource intake.store\n"
    "stage output adapter sink\n"
    "stage main {\n input -> intake -> output\n}\n";
  turbo_flow_inbox_memory_config_t memory = turbo_flow_inbox_memory_config_default();
  memory.max_claims = 4u;
  turbo_flow_durable_buffer_binding_config_t config = TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  turbo_flow_adapter_ops_t ops = {0};
  memset(f, 0, sizeof(*f));
  atomic_init(&f->sinks, 0u);
  atomic_init(&f->terminal_slots, 0u);
  f->generation = 1u;
  f->terminal = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  f->terminal2 = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  f->backing = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  check_equal(turbo_flow_inbox_memory_create(&memory, &f->backing), SALTS_OK);
  f->provider = (turbo_flow_inbox_t){sizeof(f->provider), TURBO_FLOW_INBOX_API_VERSION, &probe_ops, f};
  f->flow = turbo_flow_create(); check_not_null(f->flow);
  if (asynchronous) {
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    async_ops.submit = submit;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK; schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_equal(turbo_flow_register_async_terminal_adapter_ex(f->flow, "sink", &ops, &async_ops, f, &schema), SALTS_OK);
  } else {
    ops.consume = sink;
    check_equal(turbo_flow_register_adapter(f->flow, "sink", &ops, f), SALTS_OK);
  }
  check_equal(turbo_flow_parse_string(f->flow, graph, sizeof(graph)-1u), SALTS_OK);
  config.resource_name = "intake.store"; config.inbox = &f->provider;
  config.identity_mode = identity_mode;
  check_equal(turbo_flow_durable_buffer_bind(f->flow, &config, &f->binding), SALTS_OK);
  check_equal(turbo_flow_compile(f->flow), SALTS_OK);
  check_equal(turbo_flow_start(f->flow), SALTS_OK);
}
static void open_fixture(lifecycle_fixture_t *f, int asynchronous) {
  open_fixture_mode(f, asynchronous, TURBO_FLOW_DURABLE_IDENTITY_GENERATED);
}

static void publish(lifecycle_fixture_t *f) {
  turbo_flow_msg_t msg;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup("payload"); msg.payload = tstr_to_v(msg.owned_payload);
  check_equal(turbo_flow_publish(f->flow, "input", &msg), SALTS_OK);
  turbo_flow_msg_cleanup(&msg);
}
static void publish_source(lifecycle_fixture_t *f, const char *source_id,
                           const char *admission_id, uint64_t sequence) {
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  turbo_flow_msg_t msg;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup(admission_id);
  msg.payload = tstr_to_v(msg.owned_payload);
  identity.source_id = vstr_from_buf(source_id, strlen(source_id));
  identity.admission_id = vstr_from_buf(admission_id, strlen(admission_id));
  identity.source_sequence = sequence;
  check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_OK);
  check_equal(turbo_flow_publish(f->flow, "input", &msg), SALTS_OK);
  turbo_flow_msg_cleanup(&msg);
}

static turbo_flow_inbox_snapshot_t snapshot(lifecycle_fixture_t *f) {
  turbo_flow_inbox_snapshot_t s = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  check_equal(turbo_flow_inbox_snapshot(&f->backing, &s), SALTS_OK);
  return s;
}
static void close_fixture(lifecycle_fixture_t *f) {
  turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  size_t count = 0;
  if (turbo_flow_state(f->flow) == TURBO_FLOW_STATE_STARTED ||
      turbo_flow_state(f->flow) == TURBO_FLOW_STATE_FAILED)
    check_equal(turbo_flow_stop(f->flow), SALTS_OK);
  check_equal(turbo_flow_durable_buffer_unbind(f->binding), SALTS_OK);
  check_equal(turbo_flow_inbox_close(&f->backing), SALTS_OK);
  while (turbo_flow_inbox_claim(&f->backing, &claim) == SALTS_OK)
    check_equal(turbo_flow_inbox_complete(&f->backing, &claim), SALTS_OK);
  do {
    check_equal(turbo_flow_inbox_scan_failed(&f->backing, 0u, &failed, 1u, &count), SALTS_OK);
    if (count) check_equal(turbo_flow_inbox_discard(&f->backing, failed.record_id), SALTS_OK);
  } while (count);
  check_equal(turbo_flow_inbox_destroy(&f->backing), SALTS_OK);
  turbo_flow_destroy(f->flow);
}
static void progress_until_settled(lifecycle_fixture_t *f, uint64_t completed) {
  for (size_t i = 0; i < 1000u && snapshot(f).completed < completed; ++i) {
    check_equal(turbo_flow_durable_buffer_progress(f->binding), SALTS_OK);
    salts_sleep_ms(1u);
  }
  check_equal(snapshot(f).completed, completed);
}
spec("durable buffer lifecycle") {
  it("configures bounded partition workers and exposes scheduler state") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_drain_config_t config =
        TURBO_FLOW_DURABLE_BUFFER_DRAIN_CONFIG_INIT;
    turbo_flow_durable_buffer_drain_snapshot_t observed =
        TURBO_FLOW_DURABLE_BUFFER_DRAIN_SNAPSHOT_INIT;
    open_fixture(&f, 0);

    config.ordering = TURBO_FLOW_DURABLE_ORDER_PARTITION;
    config.partition_by = TURBO_FLOW_DURABLE_PARTITION_SOURCE_ID;
    config.workers = 4u;
    config.max_in_flight = 4u;
    config.batch_claim = 2u;
    check_equal(turbo_flow_durable_buffer_configure_drain(
                    f.flow, "intake.store", &config), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_drain_snapshot(
                    f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.ordering, TURBO_FLOW_DURABLE_ORDER_PARTITION);
    check_equal(observed.workers, (size_t)4u);
    check_equal(observed.effective_workers, (size_t)4u);
    check_equal(observed.max_in_flight, (size_t)4u);
    check_equal(observed.batch_claim, (size_t)2u);
    check_equal(observed.active_workers, (size_t)0u);

    config.workers = TURBO_FLOW_DURABLE_BUFFER_MAX_WORKERS + 1u;
    check_equal(turbo_flow_durable_buffer_configure_drain(
                    f.flow, "intake.store", &config), SALTS_EINVAL);
    config.workers = 4u;
    config.max_in_flight = 3u;
    check_equal(turbo_flow_durable_buffer_configure_drain(
                    f.flow, "intake.store", &config), SALTS_EINVAL);
    close_fixture(&f);
  }

  it("runs different partitions concurrently without preclaiming the same partition") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_drain_config_t config =
        TURBO_FLOW_DURABLE_BUFFER_DRAIN_CONFIG_INIT;
    turbo_flow_durable_buffer_drain_snapshot_t observed =
        TURBO_FLOW_DURABLE_BUFFER_DRAIN_SNAPSHOT_INIT;
    open_fixture_mode(&f, 1, TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED);

    config.ordering = TURBO_FLOW_DURABLE_ORDER_PARTITION;
    config.partition_by = TURBO_FLOW_DURABLE_PARTITION_SOURCE_ID;
    config.workers = 3u;
    config.max_in_flight = 3u;
    config.batch_claim = 3u;
    check_equal(turbo_flow_durable_buffer_configure_drain(
                    f.flow, "intake.store", &config), SALTS_OK);

    publish_source(&f, "source-A", "a-1", 1u);
    publish_source(&f, "source-A", "a-2", 2u);
    publish_source(&f, "source-B", "b-1", 1u);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    for (size_t i = 0u; i < 1000u && atomic_load(&f.sinks) < 2u; ++i)
      salts_sleep_ms(1u);
    check_equal(atomic_load(&f.sinks), (size_t)2u);
    check_equal(snapshot(&f).pending_records, (size_t)1u);
    check_equal(snapshot(&f).in_flight_claims, (size_t)2u);

    check_equal(turbo_flow_durable_buffer_drain_snapshot(
                    f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.active_workers, (size_t)2u);
    check_equal(observed.active_partitions, (size_t)2u);
    check_equal(observed.backlog_records, (size_t)1u);
    check(observed.in_flight_claims <= config.max_in_flight);

    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    observed = (turbo_flow_durable_buffer_drain_snapshot_t)
        TURBO_FLOW_DURABLE_BUFFER_DRAIN_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_drain_snapshot(
                    f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.active_workers, (size_t)2u);
    check_equal(observed.active_partitions, (size_t)2u);
    check_equal(observed.backlog_records, (size_t)1u);
    check(observed.partition_blocked > 0u);

    check_equal(turbo_flow_async_terminal_complete(&f.terminal, SALTS_OK, NULL), SALTS_OK);
    check_equal(turbo_flow_async_terminal_complete(&f.terminal2, SALTS_OK, NULL), SALTS_OK);
    progress_until_settled(&f, 2u);
    for (size_t i = 0u; i < 1000u && atomic_load(&f.sinks) < 3u; ++i)
      salts_sleep_ms(1u);
    check_equal(atomic_load(&f.sinks), (size_t)3u);
    check_equal(snapshot(&f).in_flight_claims, (size_t)1u);
    check_equal(turbo_flow_async_terminal_complete(&f.terminal, SALTS_OK, NULL), SALTS_OK);
    progress_until_settled(&f, 3u);
    check_equal(snapshot(&f).pending_records, (size_t)0u);
    check_equal(snapshot(&f).in_flight_claims, (size_t)0u);
    close_fixture(&f);
  }
  it("empty progress succeeds and one progress owns at most one record") {
    lifecycle_fixture_t f; open_fixture(&f, 0);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    publish(&f); publish(&f);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    check(snapshot(&f).pending_records >= 1u);
    check(snapshot(&f).in_flight_claims <= 1u);
    progress_until_settled(&f, 2u);
    check_equal(f.sinks, 2u); check_equal(f.completions, 2u);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    close_fixture(&f);
  }
  it("keeps an async claim pending and returns timeout without claiming the next record") {
    lifecycle_fixture_t f; open_fixture(&f, 1); publish(&f); publish(&f);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    for (size_t i=0; i<1000u && !f.sinks; ++i) salts_sleep_ms(1u);
    check_equal(f.sinks, 1u);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    check_equal(snapshot(&f).in_flight_claims, 1u);
    check_equal(snapshot(&f).pending_records, 1u);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 0u), SALTS_ETIMEDOUT);
    check_equal(turbo_flow_async_terminal_complete(&f.terminal, SALTS_OK, NULL), SALTS_OK);
    progress_until_settled(&f, 1u);
    close_fixture(&f);
  }
  it("retains canceled async capability until the terminal callback releases it") {
    lifecycle_fixture_t f;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    open_fixture(&f, 1); publish(&f);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    for (size_t i=0; i<1000u && !f.sinks; ++i) salts_sleep_ms(1u);
    check_equal(f.sinks, 1u);
    check_equal(flow_inbox_driver_cancel(f.binding->driver, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE);
    check_equal(snapshot(&f).in_flight_claims, 1u);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 0u), SALTS_ETIMEDOUT);
    check_equal(turbo_flow_async_terminal_complete(&f.terminal, SALTS_OK, NULL), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_ECANCELED);
    check_equal(snapshot(&f).failed_records, 1u); close_fixture(&f);
  }
  it("quiesce closes provider admission but drains healthy accepted backlog") {
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f);
    check_equal(turbo_flow_durable_buffer_quiesce(f.binding), SALTS_OK);
    check_equal(snapshot(&f).accepting, 0);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(f.sinks, 1u); check_equal(snapshot(&f).records, 0u);
    close_fixture(&f);
  }
  it("rebuilds an idle driver after stopped reparse changes the buffer stage index") {
    static const char reordered[] = "stage output adapter sink\n"
      "source input\n"
      "buffer intake resource intake.store\n"
      "stage main {\n input -> intake -> output\n}\n";
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    check_equal(turbo_flow_parse_string(f.flow, reordered, sizeof(reordered)-1u), SALTS_OK);
    check_equal(turbo_flow_compile(f.flow), SALTS_OK);
    check_equal(turbo_flow_start(f.flow), SALTS_OK);
    publish(&f);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(f.sinks, 2u); close_fixture(&f);
  }
  it("ordinary paused progress preserves backlog while explicit drain may execute") {
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f);
    check_equal(turbo_flow_pause(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_ESHUTDOWN);
    check_equal(snapshot(&f).pending_records, 1u); check_equal(snapshot(&f).failed_records, 0u);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(f.sinks, 1u); close_fixture(&f);
  }

  it("snapshots one resource and pauses only ordinary durable drain") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_snapshot_t observed = TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
    open_fixture(&f, 0);

    check_equal(turbo_flow_durable_buffer_snapshot(f.flow, "missing.store", &observed), SALTS_ENOENT);
    check_equal(turbo_flow_durable_buffer_snapshot(f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.provider_generation, (uint64_t)1u);
    check_equal(observed.accepting, 1);
    check_equal(observed.drain_paused, 0);
    check_equal(observed.driver_state, TURBO_FLOW_INBOX_SOURCE_EMPTY);

    check_equal(turbo_flow_durable_buffer_pause_drain(f.flow, "intake.store"), SALTS_OK);
    publish(&f);
    publish(&f);
    observed = (turbo_flow_durable_buffer_snapshot_t)TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_snapshot(f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.accepting, 1);
    check_equal(observed.drain_paused, 1);
    check_equal(observed.admitted, (uint64_t)2u);
    check_equal(observed.pending_records, (size_t)2u);
    check_equal(observed.in_flight_claims, (size_t)0u);

    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_OK);
    check_equal(f.sinks, (size_t)0u);
    observed = (turbo_flow_durable_buffer_snapshot_t)TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_snapshot(f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.pending_records, (size_t)2u);
    check_equal(observed.in_flight_claims, (size_t)0u);

    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(f.sinks, (size_t)2u);
    observed = (turbo_flow_durable_buffer_snapshot_t)TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_snapshot(f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.drain_paused, 1);
    check_equal(observed.pending_records, (size_t)0u);
    check_equal(observed.in_flight_claims, (size_t)0u);
    check_equal(observed.completed, (uint64_t)2u);
    check_equal(observed.driver_state, TURBO_FLOW_INBOX_SOURCE_EMPTY);

    check_equal(turbo_flow_durable_buffer_resume_drain(f.flow, "intake.store"), SALTS_OK);
    observed = (turbo_flow_durable_buffer_snapshot_t)TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_snapshot(f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.drain_paused, 0);
    close_fixture(&f);
  }
  it("reports binding-lifetime throughput without counting provider history") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_runtime_snapshot_t runtime =
        TURBO_FLOW_DURABLE_BUFFER_RUNTIME_SNAPSHOT_INIT;
    open_fixture(&f, 0);
    check_equal(turbo_flow_durable_buffer_runtime_snapshot(
                    f.flow, "intake.store", &runtime), SALTS_OK);
    check_equal(runtime.admitted, UINT64_C(0));
    check_equal(runtime.completed, UINT64_C(0));
    publish(&f);
    publish(&f);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    runtime = (turbo_flow_durable_buffer_runtime_snapshot_t)
        TURBO_FLOW_DURABLE_BUFFER_RUNTIME_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_runtime_snapshot(
                    f.flow, "intake.store", &runtime), SALTS_OK);
    check_equal(runtime.admitted, UINT64_C(2));
    check_equal(runtime.completed, UINT64_C(2));
    check_equal(runtime.failed, UINT64_C(0));
    check(runtime.elapsed_ns > 0u);
    check(runtime.admitted_per_second_milli > 0u);
    check(runtime.completed_per_second_milli > 0u);
    check_equal(runtime.failed_per_second_milli, UINT64_C(0));
    close_fixture(&f);
  }

  it("reports tracked oldest-pending age and claim latency without using message time") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_latency_snapshot_t latency =
        TURBO_FLOW_DURABLE_BUFFER_LATENCY_SNAPSHOT_INIT;
    open_fixture(&f, 0);
    check_equal(turbo_flow_durable_buffer_latency_snapshot(
                    f.flow, "missing.store", &latency), SALTS_ENOENT);
    check_equal(turbo_flow_durable_buffer_latency_snapshot(
                    f.flow, "intake.store", &latency), SALTS_OK);
    check_equal(latency.oldest_pending_age_valid, 1);
    check_equal(latency.pending_records, (size_t)0u);
    check_equal(latency.claim_samples, UINT64_C(0));

    check_equal(turbo_flow_durable_buffer_pause_drain(
                    f.flow, "intake.store"), SALTS_OK);
    publish(&f);
    salts_sleep_ms(2u);
    latency = (turbo_flow_durable_buffer_latency_snapshot_t)
        TURBO_FLOW_DURABLE_BUFFER_LATENCY_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_latency_snapshot(
                    f.flow, "intake.store", &latency), SALTS_OK);
    check_equal(latency.oldest_pending_age_valid, 1);
    check_equal(latency.pending_records, (size_t)1u);
    check_equal(latency.tracked_pending_records, (size_t)1u);
    check_equal(latency.untracked_pending_records, (size_t)0u);
    check(latency.oldest_pending_age_ns > 0u);

    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    latency = (turbo_flow_durable_buffer_latency_snapshot_t)
        TURBO_FLOW_DURABLE_BUFFER_LATENCY_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_latency_snapshot(
                    f.flow, "intake.store", &latency), SALTS_OK);
    check_equal(latency.oldest_pending_age_valid, 1);
    check_equal(latency.pending_records, (size_t)0u);
    check_equal(latency.tracked_pending_records, (size_t)0u);
    check_equal(latency.claim_samples, UINT64_C(1));
    check_equal(latency.untracked_claims, UINT64_C(0));
    check(latency.last_claim_latency_ns > 0u);
    check(latency.mean_claim_latency_ns > 0u);
    check(latency.max_claim_latency_ns >= latency.last_claim_latency_ns);
    close_fixture(&f);
  }

  it("marks pre-observation backlog untracked instead of inventing queue age") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_latency_snapshot_t latency =
        TURBO_FLOW_DURABLE_BUFFER_LATENCY_SNAPSHOT_INIT;
    open_fixture(&f, 0);
    check_equal(turbo_flow_durable_buffer_pause_drain(
                    f.flow, "intake.store"), SALTS_OK);
    publish(&f);
    salts_sleep_ms(2u);

    check_equal(turbo_flow_durable_buffer_latency_snapshot(
                    f.flow, "intake.store", &latency), SALTS_OK);
    check_equal(latency.oldest_pending_age_valid, 0);
    check_equal(latency.pending_records, (size_t)1u);
    check_equal(latency.tracked_pending_records, (size_t)0u);
    check_equal(latency.untracked_pending_records, (size_t)1u);
    check_equal(latency.oldest_pending_age_ns, UINT64_C(0));

    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    latency = (turbo_flow_durable_buffer_latency_snapshot_t)
        TURBO_FLOW_DURABLE_BUFFER_LATENCY_SNAPSHOT_INIT;
    check_equal(turbo_flow_durable_buffer_latency_snapshot(
                    f.flow, "intake.store", &latency), SALTS_OK);
    check_equal(latency.oldest_pending_age_valid, 1);
    check_equal(latency.pending_records, (size_t)0u);
    check_equal(latency.claim_samples, UINT64_C(0));
    check_equal(latency.untracked_claims, UINT64_C(1));
    close_fixture(&f);
  }

  it("close-and-drain closes admission and reaches a stable idle resource") {
    lifecycle_fixture_t f;
    turbo_flow_durable_buffer_snapshot_t observed =
        TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
    turbo_flow_msg_t msg;
    open_fixture(&f, 0);
    publish(&f);
    publish(&f);
    check_equal(turbo_flow_durable_buffer_close_and_drain(
                    f.flow, "intake.store", 1000u), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_snapshot(
                    f.flow, "intake.store", &observed), SALTS_OK);
    check_equal(observed.accepting, 0);
    check_equal(observed.records, (size_t)0u);
    check_equal(observed.pending_records, (size_t)0u);
    check_equal(observed.in_flight_claims, (size_t)0u);
    check_equal(observed.completed, UINT64_C(2));
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("after-close");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(f.flow, "input", &msg), SALTS_ESHUTDOWN);
    turbo_flow_msg_cleanup(&msg);
    close_fixture(&f);
  }

  it("close-and-drain preserves timeout and missing-resource errors") {
    lifecycle_fixture_t f;
    open_fixture(&f, 1);
    publish(&f);
    check_equal(turbo_flow_durable_buffer_close_and_drain(
                    f.flow, "missing.store", 1000u), SALTS_ENOENT);
    check_equal(turbo_flow_durable_buffer_close_and_drain(
                    f.flow, "intake.store", 0u), SALTS_ETIMEDOUT);
    for (size_t i = 0u; i < 1000u && !f.sinks; ++i) salts_sleep_ms(1u);
    check_equal(f.sinks, (size_t)1u);
    check_equal(turbo_flow_async_terminal_complete(&f.terminal, SALTS_OK, NULL), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    close_fixture(&f);
  }

  it("stopped drain never claims pending backlog") {
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f);
    check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 0u), SALTS_EBUSY);
    check_equal(snapshot(&f).pending_records, 1u); check_equal(f.sinks, 0u);
    close_fixture(&f);
  }
  it("preserves failed and canceled downstream records without replay") {
    const int failures[] = {SALTS_EPROTO, SALTS_ECANCELED};
    for (size_t i=0; i<2u; ++i) {
      lifecycle_fixture_t f; open_fixture(&f, 0); f.sink_status=failures[i]; publish(&f);
      check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), failures[i]);
      check_equal(snapshot(&f).failed_records, 1u); check_equal(f.sinks, 1u);
      check_equal(turbo_flow_durable_buffer_drain(f.binding, 0u), failures[i]);
      check_equal(f.sinks, 1u); close_fixture(&f);
    }
  }
  it("retries failed work only through the public durable operator contract") {
    lifecycle_fixture_t f;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    size_t failed_count = 0u;
    open_fixture(&f, 0);
    f.sink_status = SALTS_EPROTO;
    publish(&f);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_EPROTO);
    check_equal(f.sinks, 1u);
    check_equal(turbo_flow_durable_buffer_scan_failed(
                    f.flow, "intake.store", 0u, &failed, 1u, &failed_count),
                SALTS_OK);
    check_equal(failed_count, 1u);
    check_equal(failed.kind, TURBO_FLOW_INBOX_FAILURE_PROCESSING);
    check_equal(failed.status, SALTS_EPROTO);
    f.sink_status = SALTS_OK;
    check_equal(turbo_flow_durable_buffer_retry_failed(
                    f.flow, "intake.store", failed.record_id),
                SALTS_OK);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(f.sinks, 2u);
    check_equal(snapshot(&f).failed_records, 0u);
    check_equal(snapshot(&f).completed, UINT64_C(1));
    close_fixture(&f);
  }
  it("discards failed work only through the public durable operator contract") {
    lifecycle_fixture_t f;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    size_t count = 0u;
    open_fixture(&f, 0);
    f.sink_status = SALTS_EPROTO;
    publish(&f);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_EPROTO);
    check_equal(turbo_flow_durable_buffer_scan_failed(
                    f.flow, "intake.store", 0u, &failed, 1u, &count),
                SALTS_OK);
    check_equal(count, 1u);
    check_equal(turbo_flow_durable_buffer_discard_failed(
                    f.flow, "intake.store", failed.record_id),
                SALTS_OK);
    count = 0u;
    check_equal(turbo_flow_durable_buffer_scan_failed(
                    f.flow, "intake.store", 0u, &failed, 1u, &count),
                SALTS_OK);
    check_equal(count, 0u);
    check_equal(turbo_flow_durable_buffer_scan_history(
                    f.flow, "intake.store", 0u, &history, 1u, &count),
                SALTS_OK);
    check_equal(count, 1u);
    check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_DISCARDED);
    close_fixture(&f);
  }
  it("retries a transient settlement without replaying Graph work") {
    lifecycle_fixture_t f;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    open_fixture(&f, 0);
    f.complete_status = SALTS_EBUSY;
    publish(&f);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_EBUSY);
    check_equal(f.sinks, 1u);
    check_equal(f.completions, 1u);
    f.complete_status = SALTS_OK;
    check_equal(turbo_flow_durable_buffer_retry_settlement(f.binding, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.settlement_status, SALTS_OK);
    check_equal(f.sinks, 1u);
    check_equal(f.completions, 2u);
    close_fixture(&f);
  }
  it("does not implicitly retry unknown settlement") {
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f); f.complete_status=SALTS_EALREADY;
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_EALREADY);
    check_equal(f.completions, 1u);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 0u), SALTS_EALREADY);
    check_equal(f.completions, 1u); check_equal(f.sinks, 1u);
    check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(f.binding), SALTS_EBUSY);
    check_equal(turbo_flow_reset(f.flow, 0), SALTS_EBUSY);
    check_equal(turbo_flow_parse_string(f.flow, "", 0u), SALTS_EBUSY);
    turbo_flow_destroy(f.flow);
    check_equal(turbo_flow_state(f.flow), TURBO_FLOW_STATE_STOPPED);
    {
      turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
      check_equal(turbo_flow_durable_buffer_reconcile_settlement(f.binding, &result), SALTS_OK);
    }
    close_fixture(&f);
  }
}
