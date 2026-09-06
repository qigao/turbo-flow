#include "tinytest.h"
#include "turbo_flow.h"

#include <salts/clock.h>
#include <stdatomic.h>
#include <string.h>

typedef struct async_terminal_probe_s {
  turbo_flow_async_terminal_claim_t claim;
  atomic_size_t submissions;
} async_terminal_probe_t;

typedef struct async_terminal_completion_probe_s {
  atomic_size_t calls;
  atomic_int status;
} async_terminal_completion_probe_t;

typedef struct async_terminal_multi_probe_s {
  turbo_flow_async_terminal_claim_t claims[2];
  atomic_size_t submissions;
} async_terminal_multi_probe_t;

typedef struct async_terminal_observer_probe_s {
  atomic_size_t events[TURBO_FLOW_OBSERVE_EVENT_COUNT];
} async_terminal_observer_probe_t;

typedef struct async_terminal_settlement_probe_s {
  async_terminal_probe_t terminal;
  atomic_size_t calls;
  turbo_flow_settlement_result_t observed;
} async_terminal_settlement_probe_t;

static int async_terminal_submit(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage,
                                 const turbo_flow_msg_t *message,
                                 turbo_flow_async_terminal_claim_t *claim) {
  async_terminal_probe_t *probe = (async_terminal_probe_t *)ctx;
  (void)flow;
  (void)stage;
  check_not_null(message);
  int rc = turbo_flow_async_terminal_claim_move(&probe->claim, claim);
  if (rc == SALTS_OK)
    (void)atomic_fetch_add_explicit(&probe->submissions, 1u, memory_order_release);
  return rc;
}

static int async_terminal_reject(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage,
                                 const turbo_flow_msg_t *message,
                                 turbo_flow_async_terminal_claim_t *claim) {
  atomic_size_t *calls = (atomic_size_t *)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  (void)claim;
  (void)atomic_fetch_add_explicit(calls, 1u, memory_order_release);
  return SALTS_ENOSPC;
}

static int async_terminal_multi_submit(void *ctx, turbo_flow_t *flow,
                                       const turbo_flow_stage_plan_t *stage,
                                       const turbo_flow_msg_t *message,
                                       turbo_flow_async_terminal_claim_t *claim) {
  async_terminal_multi_probe_t *probe = (async_terminal_multi_probe_t *)ctx;
  size_t index = atomic_fetch_add_explicit(&probe->submissions, 1u, memory_order_acq_rel);
  (void)flow;
  (void)stage;
  (void)message;
  if (index >= 2u) return SALTS_ENOSPC;
  return turbo_flow_async_terminal_claim_move(&probe->claims[index], claim);
}

static void async_terminal_publish_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  async_terminal_completion_probe_t *probe = (async_terminal_completion_probe_t *)ctx;
  atomic_store_explicit(&probe->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&probe->calls, 1u, memory_order_release);
}

static int async_terminal_observe(void *ctx, const turbo_flow_observe_event_t *event) {
  async_terminal_observer_probe_t *probe = (async_terminal_observer_probe_t *)ctx;
  if (!probe || !event || event->kind < 0 || event->kind >= TURBO_FLOW_OBSERVE_EVENT_COUNT)
    return SALTS_EINVAL;
  (void)atomic_fetch_add_explicit(&probe->events[event->kind], 1u, memory_order_release);
  return SALTS_OK;
}

static int async_terminal_settlement_apply(void *ctx, turbo_flow_t *flow,
                                           const turbo_flow_stage_plan_t *stage,
                                           const turbo_flow_msg_t *message,
                                           const turbo_flow_settlement_result_t *result) {
  async_terminal_settlement_probe_t *probe = (async_terminal_settlement_probe_t *)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  if (!probe || !result) return SALTS_EINVAL;
  probe->observed = *result;
  (void)atomic_fetch_add_explicit(&probe->calls, 1u, memory_order_release);
  return SALTS_OK;
}

static turbo_flow_operation_descriptor_t async_terminal_operation(const char *name,
                                                                  turbo_flow_domain_t input_domain,
                                                                  turbo_flow_domain_t output_domain,
                                                                  uint32_t flags) {
  turbo_flow_operation_descriptor_t descriptor;
  memset(&descriptor, 0, sizeof(descriptor));
  descriptor.size = sizeof(descriptor);
  descriptor.name = name;
  descriptor.version = 1u;
  descriptor.domain = TURBO_FLOW_DOMAIN_DATA;
  descriptor.input_domain = input_domain;
  descriptor.input_type = input_domain == TURBO_FLOW_DOMAIN_NONE ? NULL : "Message";
  descriptor.output_domain = output_domain;
  descriptor.output_type = output_domain == TURBO_FLOW_DOMAIN_NONE ? NULL : "Message";
  descriptor.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
  descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  descriptor.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  descriptor.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  descriptor.flags = flags;
  descriptor.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  return descriptor;
}

static void async_terminal_wait_for(atomic_size_t *value, size_t expected) {
  size_t attempt;
  for (attempt = 0u;
       attempt < 5000u && atomic_load_explicit(value, memory_order_acquire) < expected; ++attempt) {
    salts_sleep_ms(1u);
  }
}

static turbo_flow_t *async_terminal_flow(async_terminal_probe_t *probe,
                                         async_terminal_observer_probe_t *observer) {
  static const char graph[] = "source input\n"
                              "stage output adapter async.out\n"
                              "stage main {\n"
                              "  input -> output\n"
                              "}\n";
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_event_observer_ops_t observer_ops = TURBO_FLOW_EVENT_OBSERVER_OPS_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  async_ops.submit = async_terminal_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  observer_ops.on_event = async_terminal_observe;
  if (!flow ||
      turbo_flow_register_async_terminal_adapter_ex(flow, "async.out", &adapter_ops, &async_ops,
                                                    probe, &schema) != SALTS_OK ||
      (observer &&
       turbo_flow_register_observer(flow, "async.terminal", &observer_ops, observer) != SALTS_OK) ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

spec("Flow async terminal adapter") {
  it("rejects synchronous publication without waiting for terminal I/O") {
    async_terminal_probe_t probe = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow;
    probe.claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    flow = async_terminal_flow(&probe, NULL);
    check_not_null(flow);
    turbo_flow_msg_init(&message);

    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_ENOTSUP);
    check_equal(atomic_load_explicit(&probe.submissions, memory_order_acquire), (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("retains a moved claim and completes async publication exactly once") {
    async_terminal_probe_t probe = {0};
    async_terminal_completion_probe_t completion = {0};
    async_terminal_observer_probe_t observer = {0};
    turbo_flow_msg_t message;
    const turbo_flow_msg_t *retained;
    turbo_flow_t *flow;
    probe.claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    atomic_init(&probe.submissions, 0u);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    flow = async_terminal_flow(&probe, &observer);
    check_not_null(flow);
    turbo_flow_msg_init(&message);
    message.owned_payload = tstr_dup("retained-terminal-message");
    message.payload = tstr_to_v(message.owned_payload);

    check_equal(turbo_flow_publish_async(flow, "input", &message, async_terminal_publish_complete,
                                         &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    async_terminal_wait_for(&probe.submissions, 1u);
    check_equal(atomic_load_explicit(&probe.submissions, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    check_equal(atomic_load_explicit(&observer.events[TURBO_FLOW_OBSERVE_FLOW_COMPLETE],
                                     memory_order_acquire),
                (size_t)0u);
    check_equal(atomic_load_explicit(&observer.events[TURBO_FLOW_OBSERVE_SINK_COMPLETE],
                                     memory_order_acquire),
                (size_t)0u);
    retained = turbo_flow_async_terminal_claim_message(&probe.claim);
    check_not_null(retained);
    check_equal(retained->payload.len, sizeof("retained-terminal-message") - 1u);
    check_equal(memcmp(retained->payload.data, "retained-terminal-message",
                       sizeof("retained-terminal-message") - 1u),
                0);

    check_equal(turbo_flow_async_terminal_complete(&probe.claim, SALTS_OK, NULL), SALTS_OK);
    check_equal(turbo_flow_async_terminal_complete(&probe.claim, SALTS_OK, NULL), SALTS_EALREADY);
    async_terminal_wait_for(&completion.calls, 1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(atomic_load_explicit(&observer.events[TURBO_FLOW_OBSERVE_FLOW_COMPLETE],
                                     memory_order_acquire),
                (size_t)1u);
    check_equal(atomic_load_explicit(&observer.events[TURBO_FLOW_OBSERVE_SINK_COMPLETE],
                                     memory_order_acquire),
                (size_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps the terminal contract transport-neutral for an HTTP adapter schema") {
    static const char graph[] = "source input\n"
                                "stage response adapter chttp.response\n"
                                "stage main {\n"
                                "  input -> response\n"
                                "}\n";
    async_terminal_probe_t probe = {0};
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_async_terminal_adapter_ops_t invalid_ops = async_ops;
    turbo_flow_adapter_schema_t schema = {0};
    turbo_flow_t *flow = turbo_flow_create();
    probe.claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    async_ops.submit = async_terminal_submit;
    invalid_ops.submit = async_terminal_submit;
    invalid_ops.version += 1u;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_HTTP;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;

    check_not_null(flow);
    check_equal(turbo_flow_register_async_terminal_adapter_ex(flow, "invalid", &adapter_ops,
                                                              &invalid_ops, &probe, &schema),
                SALTS_EINVAL);
    check_equal(turbo_flow_register_async_terminal_adapter_ex(flow, "chttp.response", &adapter_ops,
                                                              &async_ops, &probe, &schema),
                SALTS_OK);
    check_equal(turbo_flow_register_adapter_async_terminal(flow, "chttp.response", &async_ops),
                SALTS_EALREADY);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("applies settlement only when the authoritative terminal claim completes") {
    static const char graph[] = "source input operation data.input\n"
                                "stage output adapter async.settle operation data.send\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    async_terminal_settlement_probe_t probe = {0};
    async_terminal_completion_probe_t completion = {0};
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_settlement_owner_ops_t settlement_ops = TURBO_FLOW_SETTLEMENT_OWNER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    turbo_flow_operation_descriptor_t input = async_terminal_operation(
        "data.input", TURBO_FLOW_DOMAIN_NONE, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_OPERATION_SOURCE);
    turbo_flow_operation_descriptor_t send = async_terminal_operation(
        "data.send", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_OPERATION_STAGE);
    turbo_flow_settlement_result_t settlement = TURBO_FLOW_SETTLEMENT_RESULT_INIT;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();

    probe.terminal.claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    atomic_init(&probe.terminal.submissions, 0u);
    atomic_init(&probe.calls, 0u);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    async_ops.submit = async_terminal_submit;
    settlement_ops.apply = async_terminal_settlement_apply;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    send.runtime.settlement = TURBO_FLOW_SETTLEMENT_COMPLETE;
    check_not_null(flow);
    check_equal(turbo_flow_register_async_terminal_adapter_ex(flow, "async.settle", &adapter_ops,
                                                              &async_ops, &probe, &schema),
                SALTS_OK);
    check_equal(
        turbo_flow_register_adapter_settlement(flow, "async.settle", &settlement_ops, &probe),
        SALTS_OK);
    check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
    check_equal(turbo_flow_register_operation(flow, &send), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    message.id = 77u;

    check_equal(turbo_flow_publish_async(flow, "input", &message, async_terminal_publish_complete,
                                         &completion),
                SALTS_OK);
    async_terminal_wait_for(&probe.terminal.submissions, 1u);
    check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), (size_t)0u);
    settlement.action = TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE;
    settlement.status = SALTS_OK;
    check_equal(turbo_flow_async_terminal_complete(&probe.terminal.claim, SALTS_OK, &settlement),
                SALTS_OK);
    async_terminal_wait_for(&completion.calls, 1u);
    check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), (size_t)1u);
    check_equal(probe.observed.action, TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE);
    check_equal(probe.observed.message_id, (uint64_t)77u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("preserves a rejected claim and completes the publication with backpressure") {
    static const char graph[] = "source input\n"
                                "stage output adapter async.reject\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    atomic_size_t rejects;
    async_terminal_completion_probe_t completion;
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    atomic_init(&rejects, 0u);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    async_ops.submit = async_terminal_reject;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_not_null(flow);
    check_equal(turbo_flow_register_async_terminal_adapter_ex(
                    flow, "async.reject", &adapter_ops, &async_ops, (void *)&rejects, &schema),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);

    check_equal(turbo_flow_publish_async(flow, "input", &message, async_terminal_publish_complete,
                                         &completion),
                SALTS_OK);
    async_terminal_wait_for(&completion.calls, 1u);
    check_equal(atomic_load_explicit(&rejects, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ENOSPC);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("waits for every terminal fanout and preserves the first terminal failure") {
    static const char graph[] = "source input\n"
                                "stage left adapter async.left\n"
                                "stage right adapter async.right\n"
                                "stage main {\n"
                                "  input -> left\n"
                                "  input -> right\n"
                                "}\n";
    async_terminal_multi_probe_t probe = {0};
    async_terminal_completion_probe_t completion;
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    probe.claims[0] = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    probe.claims[1] = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    atomic_init(&probe.submissions, 0u);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    async_ops.submit = async_terminal_multi_submit;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_not_null(flow);
    check_equal(turbo_flow_register_async_terminal_adapter_ex(flow, "async.left", &adapter_ops,
                                                              &async_ops, &probe, &schema),
                SALTS_OK);
    check_equal(turbo_flow_register_async_terminal_adapter_ex(flow, "async.right", &adapter_ops,
                                                              &async_ops, &probe, &schema),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);

    check_equal(turbo_flow_publish_async(flow, "input", &message, async_terminal_publish_complete,
                                         &completion),
                SALTS_OK);
    async_terminal_wait_for(&probe.submissions, 2u);
    check_equal(atomic_load_explicit(&probe.submissions, memory_order_acquire), (size_t)2u);
    check_equal(turbo_flow_async_terminal_complete(&probe.claims[0], SALTS_EIO, NULL), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_async_terminal_complete(&probe.claims[1], SALTS_OK, NULL), SALTS_OK);
    async_terminal_wait_for(&completion.calls, 1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_EIO);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
