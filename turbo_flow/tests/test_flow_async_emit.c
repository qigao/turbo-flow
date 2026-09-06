#include "flow_internal.h"
#include "tinytest.h"

#include <salts/clock.h>

#include <stdatomic.h>
#include <string.h>

typedef struct async_emit_probe_s {
  turbo_flow_async_emit_claim_t claim;
  int submit_behavior;
  atomic_size_t submissions;
  atomic_size_t downstream_calls;
  atomic_size_t publication_calls;
  atomic_int publication_status;
  char downstream_payload[64];
} async_emit_probe_t;

enum {
  ASYNC_EMIT_SUBMIT_MOVE_OK = 0,
  ASYNC_EMIT_SUBMIT_ACCEPT_WITHOUT_MOVE,
  ASYNC_EMIT_SUBMIT_REJECT_AFTER_MOVE
};

static int async_emit_submit(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                             const turbo_flow_msg_t *message,
                             turbo_flow_async_emit_claim_t *claim) {
  async_emit_probe_t *probe = (async_emit_probe_t *)ctx;
  int rc;
  (void)flow;
  (void)stage;
  check_not_null(message);
  (void)atomic_fetch_add_explicit(&probe->submissions, 1u, memory_order_release);
  if (probe->submit_behavior == ASYNC_EMIT_SUBMIT_ACCEPT_WITHOUT_MOVE) return SALTS_OK;
  rc = turbo_flow_async_emit_claim_move(&probe->claim, claim);
  if (rc == SALTS_OK && probe->submit_behavior == ASYNC_EMIT_SUBMIT_REJECT_AFTER_MOVE)
    return SALTS_EIO;
  return rc;
}

static int async_emit_downstream(turbo_flow_msg_t *message, void *ctx) {
  async_emit_probe_t *probe = (async_emit_probe_t *)ctx;
  size_t size;
  if (!message || !message->payload.data) return SALTS_EINVAL;
  size = message->payload.len;
  if (size >= sizeof(probe->downstream_payload)) return SALTS_EMSGSIZE;
  memcpy(probe->downstream_payload, message->payload.data, size);
  probe->downstream_payload[size] = '\0';
  (void)atomic_fetch_add_explicit(&probe->downstream_calls, 1u, memory_order_release);
  return SALTS_OK;
}

static void async_emit_publication_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  async_emit_probe_t *probe = (async_emit_probe_t *)ctx;
  atomic_store_explicit(&probe->publication_status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&probe->publication_calls, 1u, memory_order_release);
}

static void async_emit_wait_for(atomic_size_t *value, size_t expected) {
  size_t attempt;
  for (attempt = 0u;
       attempt < 5000u && atomic_load_explicit(value, memory_order_acquire) < expected; ++attempt) {
    salts_sleep_ms(1u);
  }
}

static turbo_flow_t *async_emit_flow(async_emit_probe_t *probe) {
  static const char graph[] = "source input\n"
                              "stage request adapter async.request\n"
                              "stage output\n"
                              "stage main {\n"
                              "  input -> request -> output\n"
                              "}\n";
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_emit_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_EMIT_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_t *flow = turbo_flow_create();
  async_ops.submit = async_emit_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  if (!flow ||
      turbo_flow_register_async_emit_adapter_ex(flow, "async.request", &adapter_ops, &async_ops,
                                                probe, &schema) != SALTS_OK ||
      turbo_flow_register_stage_ex(flow, "output", async_emit_downstream, probe, NULL) !=
          SALTS_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

spec("Flow async emitting adapter") {
  it("rejects synchronous publication because completion resumes later") {
    async_emit_probe_t probe = {0};
    turbo_flow_msg_t input;
    turbo_flow_t *flow;
    probe.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    flow = async_emit_flow(&probe);
    check_not_null(flow);
    turbo_flow_msg_init(&input);

    check_equal(turbo_flow_publish(flow, "input", &input), SALTS_ENOTSUP);
    check_equal(atomic_load_explicit(&probe.submissions, memory_order_acquire), (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("moves one accepted claim and resumes downstream with one owned output") {
    async_emit_probe_t probe = {0};
    turbo_flow_async_emit_claim_t destination = TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    turbo_flow_msg_t input;
    turbo_flow_msg_t output;
    const turbo_flow_msg_t *retained;
    turbo_flow_t *flow;
    probe.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    atomic_init(&probe.publication_status, SALTS_EALREADY);
    flow = async_emit_flow(&probe);
    check_not_null(flow);
    turbo_flow_msg_init(&input);
    input.id = 41u;
    input.owned_payload = tstr_dup("request");
    input.payload = tstr_to_v(input.owned_payload);

    check_equal(
        turbo_flow_publish_async(flow, "input", &input, async_emit_publication_complete, &probe),
        SALTS_OK);
    turbo_flow_msg_cleanup(&input);
    async_emit_wait_for(&probe.submissions, 1u);
    check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)0u);
    retained = turbo_flow_async_emit_claim_message(&probe.claim);
    check_not_null(retained);
    check_equal(retained->id, (uint64_t)41u);
    check_equal(retained->payload.len, sizeof("request") - 1u);
    check_equal(turbo_flow_async_emit_claim_move(&destination, &probe.claim), SALTS_OK);
    check_null(turbo_flow_async_emit_claim_message(&probe.claim));
    check_equal(turbo_flow_async_emit_claim_move(&probe.claim, &destination), SALTS_OK);

    turbo_flow_msg_init(&output);
    output.id = 42u;
    output.owned_payload = tstr_dup("response");
    output.payload = tstr_to_v(output.owned_payload);
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_OK, &output), SALTS_OK);
    check_null(output.owned_payload);
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_OK, NULL), SALTS_EALREADY);
    async_emit_wait_for(&probe.publication_calls, 1u);
    check_equal(atomic_load_explicit(&probe.downstream_calls, memory_order_acquire), (size_t)1u);
    check_equal(probe.downstream_payload, "response");
    check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire), SALTS_OK);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("allows zero output and reports a terminal error exactly once") {
    async_emit_probe_t probe = {0};
    turbo_flow_msg_t input;
    turbo_flow_t *flow;
    probe.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    atomic_init(&probe.publication_status, SALTS_EALREADY);
    flow = async_emit_flow(&probe);
    check_not_null(flow);
    turbo_flow_msg_init(&input);

    check_equal(
        turbo_flow_publish_async(flow, "input", &input, async_emit_publication_complete, &probe),
        SALTS_OK);
    async_emit_wait_for(&probe.submissions, 1u);
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_OK, NULL), SALTS_OK);
    async_emit_wait_for(&probe.publication_calls, 1u);
    check_equal(atomic_load_explicit(&probe.downstream_calls, memory_order_acquire), (size_t)0u);
    check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire), SALTS_OK);

    atomic_store_explicit(&probe.publication_calls, 0u, memory_order_release);
    atomic_store_explicit(&probe.publication_status, SALTS_OK, memory_order_release);
    check_equal(
        turbo_flow_publish_async(flow, "input", &input, async_emit_publication_complete, &probe),
        SALTS_OK);
    async_emit_wait_for(&probe.submissions, 2u);
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_ECANCELED, NULL), SALTS_OK);
    async_emit_wait_for(&probe.publication_calls, 1u);
    check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                SALTS_ECANCELED);
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_OK, NULL), SALTS_EALREADY);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps a live claim when output validation rejects borrowed state") {
    async_emit_probe_t probe = {0};
    turbo_flow_msg_t input;
    turbo_flow_msg_t invalid_output;
    turbo_flow_t *flow;
    int borrowed = 0;
    probe.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    atomic_init(&probe.publication_status, SALTS_EALREADY);
    flow = async_emit_flow(&probe);
    check_not_null(flow);
    turbo_flow_msg_init(&input);

    check_equal(
        turbo_flow_publish_async(flow, "input", &input, async_emit_publication_complete, &probe),
        SALTS_OK);
    async_emit_wait_for(&probe.submissions, 1u);
    turbo_flow_msg_init(&invalid_output);
    invalid_output.transport_context = &borrowed;
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_OK, &invalid_output),
                SALTS_EINVAL);
    check_not_null(turbo_flow_async_emit_claim_message(&probe.claim));
    check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_async_emit_complete(&probe.claim, SALTS_EPROTO, NULL), SALTS_OK);
    async_emit_wait_for(&probe.publication_calls, 1u);
    check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                SALTS_EPROTO);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails accepted and rejected claim ownership contract violations") {
    async_emit_probe_t accepted = {0};
    async_emit_probe_t rejected = {0};
    turbo_flow_msg_t input;
    turbo_flow_t *flow;

    accepted.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    accepted.submit_behavior = ASYNC_EMIT_SUBMIT_ACCEPT_WITHOUT_MOVE;
    atomic_init(&accepted.publication_status, SALTS_OK);
    flow = async_emit_flow(&accepted);
    check_not_null(flow);
    turbo_flow_msg_init(&input);
    check_equal(
        turbo_flow_publish_async(flow, "input", &input, async_emit_publication_complete, &accepted),
        SALTS_OK);
    async_emit_wait_for(&accepted.publication_calls, 1u);
    check_equal(atomic_load_explicit(&accepted.publication_status, memory_order_acquire),
                SALTS_EPROTO);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);

    rejected.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    rejected.submit_behavior = ASYNC_EMIT_SUBMIT_REJECT_AFTER_MOVE;
    atomic_init(&rejected.publication_status, SALTS_OK);
    flow = async_emit_flow(&rejected);
    check_not_null(flow);
    check_equal(
        turbo_flow_publish_async(flow, "input", &input, async_emit_publication_complete, &rejected),
        SALTS_OK);
    async_emit_wait_for(&rejected.submissions, 1u);
    check_equal(atomic_load_explicit(&rejected.publication_calls, memory_order_acquire),
                (size_t)0u);
    check_not_null(turbo_flow_async_emit_claim_message(&rejected.claim));
    check_equal(turbo_flow_async_emit_complete(&rejected.claim, SALTS_EPROTO, NULL), SALTS_OK);
    async_emit_wait_for(&rejected.publication_calls, 1u);
    check_equal(atomic_load_explicit(&rejected.publication_status, memory_order_acquire),
                SALTS_EPROTO);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("marks the asynchronous transform as a CFlow flat-map boundary") {
    async_emit_probe_t probe = {0};
    turbo_flow_t *flow;
    int index;
    const flow_stage_semantic_plan_t *semantics;
    probe.claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    flow = async_emit_flow(&probe);
    check_not_null(flow);
    index = turbo_flow_find_stage(flow, "request");
    check(index >= 0);
    semantics = (const flow_stage_semantic_plan_t *)vec_at_const(
        &flow->compiled_plan.stage_semantics, (size_t)index);
    check_not_null(semantics);
    check_equal(semantics->cflow_operator, CFLOW_OP_FLAT_MAP);
    check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_ASYNC);
    check_bits(semantics->effects, CMETA_EFFECT_ASYNC | CMETA_EFFECT_IO);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects an async emitter registered as a sink-only adapter") {
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_emit_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_EMIT_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    async_emit_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();
    async_ops.submit = async_emit_submit;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_not_null(flow);
    check_equal(turbo_flow_register_async_emit_adapter_ex(flow, "bad", &adapter_ops, &async_ops,
                                                          &probe, &schema),
                SALTS_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("rejects an external branch merge downstream of an async emitter") {
    static const char graph[] = "source input\n"
                                "source other\n"
                                "stage request adapter async.request\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "  other -> output\n"
                                "}\n";
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_emit_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_EMIT_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    async_emit_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();
    async_ops.submit = async_emit_submit;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
    schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
    check_not_null(flow);
    check_equal(turbo_flow_register_async_emit_adapter_ex(flow, "async.request", &adapter_ops,
                                                          &async_ops, &probe, &schema),
                SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", async_emit_downstream, &probe, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
    turbo_flow_destroy(flow);
  }
}
