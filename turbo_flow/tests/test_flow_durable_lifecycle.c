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
  size_t owner_releases;
  int complete_status;
  size_t completions;
  int sink_status;
  int asynchronous;
  turbo_flow_async_terminal_claim_t terminal;
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
  sizeof(turbo_flow_inbox_ops_v2_t), TURBO_FLOW_INBOX_API_VERSION,
  probe_admit, probe_claim, probe_complete, probe_fail, probe_retry, probe_discard, probe_forget,
  probe_scan_failed, probe_scan_history, probe_close, probe_snapshot, probe_destroy
};
/* Task 6 contract declarations deliberately precede production implementation. */
extern int turbo_flow_durable_buffer_progress(turbo_flow_durable_buffer_binding_t *);
extern int turbo_flow_durable_buffer_quiesce(turbo_flow_durable_buffer_binding_t *);
extern int turbo_flow_durable_buffer_drain(turbo_flow_durable_buffer_binding_t *, uint64_t);
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
  ++f->sinks;
  return turbo_flow_async_terminal_claim_move(&f->terminal, claim);
}
static void open_fixture(lifecycle_fixture_t *f, int asynchronous) {
  static const char graph[] = "source input\n"
    "buffer intake resource intake.store\n"
    "stage output adapter sink\n"
    "stage main {\n input -> intake -> output\n}\n";
  turbo_flow_inbox_memory_config_t memory = turbo_flow_inbox_memory_config_default();
  turbo_flow_durable_buffer_binding_config_t config = TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  turbo_flow_adapter_ops_t ops = {0};
  memset(f, 0, sizeof(*f));
  atomic_init(&f->sinks, 0u);
  f->generation = 1u;
  f->terminal = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
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
  check_equal(turbo_flow_durable_buffer_bind(f->flow, &config, &f->binding), SALTS_OK);
  check_equal(turbo_flow_compile(f->flow), SALTS_OK);
  check_equal(turbo_flow_start(f->flow), SALTS_OK);
}
static void publish(lifecycle_fixture_t *f) {
  turbo_flow_msg_t msg;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup("payload"); msg.payload = tstr_to_v(msg.owned_payload);
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
  it("ordinary paused progress preserves backlog while explicit drain may execute") {
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f);
    check_equal(turbo_flow_pause(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_progress(f.binding), SALTS_ESHUTDOWN);
    check_equal(snapshot(&f).pending_records, 1u); check_equal(snapshot(&f).failed_records, 0u);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_OK);
    check_equal(f.sinks, 1u); close_fixture(&f);
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
  it("does not implicitly retry unknown settlement") {
    lifecycle_fixture_t f; open_fixture(&f, 0); publish(&f); f.complete_status=SALTS_EALREADY;
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 1000u), SALTS_EALREADY);
    check_equal(f.completions, 1u);
    check_equal(turbo_flow_durable_buffer_drain(f.binding, 0u), SALTS_EALREADY);
    check_equal(f.completions, 1u); check_equal(f.sinks, 1u);
    check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(f.binding), SALTS_EBUSY);
    check_equal(turbo_flow_reset(f.flow, 0), SALTS_EBUSY);
    turbo_flow_destroy(f.flow);
    check_equal(turbo_flow_state(f.flow), TURBO_FLOW_STATE_STOPPED);
    {
      turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
      check_equal(flow_inbox_driver_reconcile_settlement(f.binding->driver, &result), SALTS_OK);
    }
    close_fixture(&f);
  }
}
