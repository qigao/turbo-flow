#include <tinytest.h>
#include <turbo_flow_turbodb.h>

#include <salts_error.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define OUTBOX_TEST_CAPACITY 8u

typedef struct outbox_contract_probe_s {
  size_t fetch_calls;
  size_t cancel_calls;
  size_t record_index;
  size_t record_count;
  turbo_flow_turbodb_outbox_record_t records[OUTBOX_TEST_CAPACITY];
  turbo_flow_turbodb_outbox_fetch_budget_t budgets[OUTBOX_TEST_CAPACITY];
  size_t acknowledge_calls;
  uint64_t acknowledged_tokens[OUTBOX_TEST_CAPACITY];
  size_t acknowledge_failures_remaining;
  size_t requeue_calls;
  uint64_t requeued_tokens[OUTBOX_TEST_CAPACITY];
  size_t requeue_failures_remaining;
  size_t dead_letter_calls;
  uint64_t dead_letter_tokens[OUTBOX_TEST_CAPACITY];
  int dead_letter_statuses[OUTBOX_TEST_CAPACITY];
  size_t dead_letter_failures_remaining;
  turbo_flow_turbodb_outbox_failure_disposition_t disposition;
  int record_status;
  turbo_flow_turbodb_outbox_fetch_kind_t terminal_fetch_kind;
  int terminal_fetch_status;
  int wait_before_records;
  int ready;
  int wake_during_cancel;
  size_t wait_arms;
  size_t wait_cancels;
  cflow_waker waker;
} outbox_contract_probe_t;

typedef struct outbox_sink_probe_s {
  size_t calls;
  int status;
  uint64_t message_id;
  turbo_flow_turbodb_outbox_message_context_t context;
  char identity[32];
  size_t identity_size;
  char payload[32];
  size_t payload_size;
} outbox_sink_probe_t;

static bool outbox_contract_waitable_arm(void *state, cflow_waker waker) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)state;
  if (probe->waker.wake) return false;
  probe->waker = waker;
  ++probe->wait_arms;
  return true;
}

static void outbox_contract_waitable_cancel(void *state) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)state;
  cflow_waker waker = probe->waker;
  probe->waker = (cflow_waker){0};
  ++probe->wait_cancels;
  if (probe->wake_during_cancel && waker.wake) waker.wake(waker.user);
}

CMETA_IMPLEMENTS(cflow_waitable, outbox_contract_waitable, 0, .arm = outbox_contract_waitable_arm,
                 .cancel = outbox_contract_waitable_cancel);

static turbo_flow_turbodb_outbox_fetch_step_t
outbox_contract_fetch(void *ctx, const turbo_flow_turbodb_outbox_fetch_budget_t *budget) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)ctx;
  turbo_flow_turbodb_outbox_fetch_step_t step = TURBO_FLOW_TURBODB_OUTBOX_FETCH_STEP_INIT;
  if (probe->fetch_calls < OUTBOX_TEST_CAPACITY) probe->budgets[probe->fetch_calls] = *budget;
  ++probe->fetch_calls;
  if (probe->wait_before_records && !probe->ready) {
    step.kind = TURBO_FLOW_TURBODB_OUTBOX_FETCH_WAIT;
    step.waitable = outbox_contract_waitable_as_cflow_waitable(probe);
    return step;
  }
  if (probe->record_index < probe->record_count) {
    step.kind = TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD;
    step.status = probe->record_status;
    step.record = probe->records[probe->record_index++];
  } else {
    step.kind = probe->terminal_fetch_kind;
    step.status = probe->terminal_fetch_status;
  }
  return step;
}

static int outbox_contract_cancel(void *ctx) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)ctx;
  ++probe->cancel_calls;
  return SALTS_OK;
}

static int outbox_contract_sink(turbo_flow_msg_t *message, void *ctx) {
  outbox_sink_probe_t *probe = (outbox_sink_probe_t *)ctx;
  const turbo_flow_turbodb_outbox_message_context_t *context;
  vstr identity;
  if (!probe) return SALTS_OK;
  context = turbo_flow_turbodb_outbox_message_context(message);
  identity = turbo_flow_turbodb_outbox_message_identity(message);
  ++probe->calls;
  probe->message_id = message->id;
  if (context) probe->context = *context;
  probe->identity_size = identity.len;
  if (identity.len <= sizeof(probe->identity)) memcpy(probe->identity, identity.data, identity.len);
  probe->payload_size = message->payload.len;
  if (message->payload.len <= sizeof(probe->payload))
    memcpy(probe->payload, message->payload.data, message->payload.len);
  return probe->status;
}

static turbo_flow_t *outbox_contract_flow(outbox_sink_probe_t *probe) {
  static const char dsl[] = "source input\n"
                            "stage sink\n"
                            "stage main {\n"
                            "  input -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u) != SALTS_OK ||
      turbo_flow_register_stage_ex(flow, "sink", outbox_contract_sink, probe, NULL) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static void outbox_contract_noop_task(void *ctx) { (void)ctx; }

static void outbox_contract_add_record(outbox_contract_probe_t *probe, uint64_t token,
                                       uint64_t raft_index, char *identity, char *payload) {
  turbo_flow_turbodb_outbox_record_t *record;
  if (!probe || probe->record_count >= OUTBOX_TEST_CAPACITY) return;
  record = &probe->records[probe->record_count++];
  *record = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
  record->token = token;
  record->raft_index = raft_index;
  record->term = 1u;
  record->delivery_attempt = 1u;
  record->identity = vstr_from_buf(identity, strlen(identity));
  record->payload = vstr_from_buf(payload, strlen(payload));
}

static int outbox_contract_settle(void *ctx, uint64_t token) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)ctx;
  if (probe->acknowledge_calls < OUTBOX_TEST_CAPACITY)
    probe->acknowledged_tokens[probe->acknowledge_calls] = token;
  ++probe->acknowledge_calls;
  if (probe->acknowledge_failures_remaining != 0u) {
    --probe->acknowledge_failures_remaining;
    return SALTS_EIO;
  }
  return SALTS_OK;
}

static int outbox_contract_requeue(void *ctx, uint64_t token) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)ctx;
  if (probe->requeue_calls < OUTBOX_TEST_CAPACITY)
    probe->requeued_tokens[probe->requeue_calls] = token;
  ++probe->requeue_calls;
  if (probe->requeue_failures_remaining != 0u) {
    --probe->requeue_failures_remaining;
    return SALTS_EIO;
  }
  return SALTS_OK;
}

static int outbox_contract_dead_letter(void *ctx, uint64_t token,
                                       const turbo_flow_turbodb_outbox_message_context_t *receipt,
                                       int graph_status) {
  outbox_contract_probe_t *probe = (outbox_contract_probe_t *)ctx;
  (void)receipt;
  if (probe->dead_letter_calls < OUTBOX_TEST_CAPACITY) {
    probe->dead_letter_tokens[probe->dead_letter_calls] = token;
    probe->dead_letter_statuses[probe->dead_letter_calls] = graph_status;
  }
  ++probe->dead_letter_calls;
  if (probe->dead_letter_failures_remaining != 0u) {
    --probe->dead_letter_failures_remaining;
    return SALTS_EIO;
  }
  return SALTS_OK;
}

static turbo_flow_turbodb_outbox_failure_disposition_t
outbox_contract_classify(void *ctx, const turbo_flow_turbodb_outbox_message_context_t *receipt,
                         int status) {
  (void)receipt;
  (void)status;
  return ((outbox_contract_probe_t *)ctx)->disposition;
}

static void outbox_contract_configure(turbo_flow_turbodb_outbox_source_config_t *config,
                                      turbo_flow_t *flow, outbox_contract_probe_t *probe) {
  *config = turbo_flow_turbodb_outbox_source_config_default();
  config->flow = flow;
  config->source_name = "input";
  config->provider_ctx = probe;
  config->provider.fetch = outbox_contract_fetch;
  config->provider.cancel_fetch = outbox_contract_cancel;
  config->provider.acknowledge = outbox_contract_settle;
  config->provider.requeue = outbox_contract_requeue;
  config->provider.dead_letter = outbox_contract_dead_letter;
  config->classify_failure = outbox_contract_classify;
  config->policy_ctx = probe;
}

spec("turbodb_outbox_source_contract") {
  it("returns a complete finite versioned default configuration") {
    turbo_flow_turbodb_outbox_source_config_t config =
        turbo_flow_turbodb_outbox_source_config_default();

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION);
    check_null(config.flow);
    check_null(config.source_name);
    check_null(config.scheduler);
    check_null(config.provider_ctx);
    check_equal(config.provider.size, sizeof(config.provider));
    check_null(config.provider.fetch);
    check_null(config.provider.cancel_fetch);
    check_null(config.provider.acknowledge);
    check_null(config.provider.requeue);
    check_null(config.provider.dead_letter);
    check_null(config.classify_failure);
    check_null(config.policy_ctx);
    check_equal(config.permanent_failure_policy, TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_FAIL_SOURCE);
    check_equal(config.shutdown_policy, TURBO_FLOW_TURBODB_OUTBOX_SHUTDOWN_REQUEUE);
    check(config.fetch_count > 0u);
    check(config.in_flight_messages > 0u);
    check(config.in_flight_bytes > 0u);
    check(config.max_identity_bytes > 0u);
    check(config.max_payload_bytes > 0u);
    check(config.max_delivery_attempts > 0u);
    check_equal(config.first_message_id, (uint64_t)1u);
  }

  it("fails invalid open before provider invocation and clears the output") {
    outbox_contract_probe_t probe = {0};
    turbo_flow_turbodb_outbox_source_config_t config =
        turbo_flow_turbodb_outbox_source_config_default();
    turbo_flow_turbodb_outbox_source_t *source =
        (turbo_flow_turbodb_outbox_source_t *)(uintptr_t)1u;

    config.provider_ctx = &probe;
    config.provider.fetch = outbox_contract_fetch;
    config.provider.cancel_fetch = outbox_contract_cancel;
    config.provider.acknowledge = outbox_contract_settle;
    config.provider.requeue = outbox_contract_requeue;
    config.classify_failure = outbox_contract_classify;

    check_equal(turbo_flow_turbodb_outbox_source_open(NULL, &source), SALTS_EINVAL);
    check_null(source);
    check_equal(probe.fetch_calls, 0u);

    source = (turbo_flow_turbodb_outbox_source_t *)(uintptr_t)1u;
    config.size = sizeof(config) - 1u;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);
    check_equal(probe.fetch_calls, 0u);

    source = (turbo_flow_turbodb_outbox_source_t *)(uintptr_t)1u;
    config.size = sizeof(config);
    ++config.version;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);
    check_equal(probe.fetch_calls, 0u);
  }

  it("opens without fetching and requires stop before destroy") {
    outbox_contract_probe_t probe = {0};
    turbo_flow_turbodb_outbox_source_config_t config =
        turbo_flow_turbodb_outbox_source_config_default();
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(NULL);

    check_not_null(flow);
    config.flow = flow;
    config.source_name = "input";
    config.provider_ctx = &probe;
    config.provider.fetch = outbox_contract_fetch;
    config.provider.cancel_fetch = outbox_contract_cancel;
    config.provider.acknowledge = outbox_contract_settle;
    config.provider.requeue = outbox_contract_requeue;
    config.classify_failure = outbox_contract_classify;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_not_null(source);
    check_equal(probe.fetch_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING);
    check_equal(snapshot.status, SALTS_OK);
    check_equal(snapshot.outstanding_demand, 0u);
    check_equal(snapshot.in_flight_messages, 0u);
    check_equal(snapshot.in_flight_bytes, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_EBUSY);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(probe.cancel_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects incomplete bounds and policies before provider invocation") {
    outbox_contract_probe_t provider = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_config_t invalid;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(NULL);

    check_not_null(flow);
    outbox_contract_configure(&config, flow, &provider);

    invalid = config;
    invalid.provider.fetch = NULL;
    check_equal(turbo_flow_turbodb_outbox_source_open(&invalid, &source), SALTS_EINVAL);
    check_null(source);

    invalid = config;
    invalid.in_flight_messages = 0u;
    check_equal(turbo_flow_turbodb_outbox_source_open(&invalid, &source), SALTS_EINVAL);
    check_null(source);

    invalid = config;
    invalid.in_flight_bytes = sizeof(turbo_flow_turbodb_outbox_message_context_t);
    check_equal(turbo_flow_turbodb_outbox_source_open(&invalid, &source), SALTS_EINVAL);
    check_null(source);

    invalid = config;
    invalid.max_identity_bytes = SIZE_MAX;
    check_equal(turbo_flow_turbodb_outbox_source_open(&invalid, &source), SALTS_EINVAL);
    check_null(source);

    invalid = config;
    invalid.permanent_failure_policy = TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER;
    invalid.provider.dead_letter = NULL;
    check_equal(turbo_flow_turbodb_outbox_source_open(&invalid, &source), SALTS_EINVAL);
    check_null(source);

    invalid = config;
    invalid.shutdown_policy = (turbo_flow_turbodb_outbox_shutdown_policy_t)99;
    check_equal(turbo_flow_turbodb_outbox_source_open(&invalid, &source), SALTS_EINVAL);
    check_null(source);
    check_equal(provider.fetch_calls, 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("does not fetch without demand") {
    outbox_contract_probe_t provider = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(NULL);

    check_not_null(flow);
    outbox_contract_configure(&config, flow, &provider);
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 8u), SALTS_OK);
    check_equal(provider.fetch_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("copies one demanded receipt and exposes exact remaining budgets") {
    char identity[] = "r-7";
    char payload[] = "alpha";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};
    const size_t retained = sizeof(turbo_flow_turbodb_outbox_message_context_t) + 3u + 5u;

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 77u;
    provider.records[0].raft_index = 7u;
    provider.records[0].term = 2u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, 3u);
    provider.records[0].payload = vstr_from_buf(payload, 5u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    config.fetch_count = 3u;
    config.in_flight_messages = 2u;
    config.in_flight_bytes = 512u;
    config.max_identity_bytes = 16u;
    config.max_payload_bytes = 16u;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 4u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.fetch_calls, 1u);
    check_equal(provider.budgets[0].max_records, 2u);
    check_equal(provider.budgets[0].max_retained_bytes, 512u);
    check_equal(provider.budgets[0].max_identity_bytes, 16u);
    check_equal(provider.budgets[0].max_payload_bytes, 16u);
    check_equal(sink.calls, 1u);
    check_equal(sink.context.raft_index, (uint64_t)7u);
    check_equal(sink.context.term, (uint64_t)2u);
    check_equal(sink.context.delivery_attempt, 1u);
    check_equal(sink.identity_size, 3u);
    check_equal(memcmp(sink.identity, "r-7", 3u), 0);
    check_equal(sink.payload_size, 5u);
    check_equal(memcmp(sink.payload, "alpha", 5u), 0);
    identity[0] = 'x';
    payload[0] = 'x';
    check_equal(memcmp(sink.identity, "r-7", 3u), 0);
    check_equal(memcmp(sink.payload, "alpha", 5u), 0);

    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.outstanding_demand, 3u);
    check_equal(snapshot.in_flight_messages, 1u);
    check_equal(snapshot.in_flight_bytes, retained);
    check_equal(snapshot.fetched, (uint64_t)1u);
    check_equal(snapshot.acknowledged, (uint64_t)0u);

    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(provider.acknowledged_tokens[0], (uint64_t)77u);
    check_equal(provider.requeue_calls, 0u);
    snapshot =
        (turbo_flow_turbodb_outbox_source_snapshot_t)TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.in_flight_messages, 0u);
    check_equal(snapshot.in_flight_bytes, 0u);
    check_equal(snapshot.acknowledged, (uint64_t)1u);

    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("reduces fetch slots and retained-byte budget for concurrent claims") {
    char first_identity[] = "one";
    char first_payload[] = "alpha";
    char second_identity[] = "two";
    char second_payload[] = "bravo";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};
    const size_t retained = sizeof(turbo_flow_turbodb_outbox_message_context_t) + 3u + 5u;

    check_not_null(flow);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 8u));
    outbox_contract_add_record(&provider, 81u, 8u, first_identity, first_payload);
    outbox_contract_add_record(&provider, 82u, 9u, second_identity, second_payload);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    config.fetch_count = 4u;
    config.in_flight_messages = 2u;
    config.in_flight_bytes = 256u;
    config.max_identity_bytes = 16u;
    config.max_payload_bytes = 16u;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 3u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.fetch_calls, 2u);
    check_equal(provider.budgets[0].max_records, 2u);
    check_equal(provider.budgets[0].max_retained_bytes, 256u);
    check_equal(provider.budgets[1].max_records, 1u);
    check_equal(provider.budgets[1].max_retained_bytes, 256u - retained);
    check_equal(sink.calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(provider.requeue_calls, 2u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects a token already owned by another in-flight Graph run") {
    char first_identity[] = "first";
    char first_payload[] = "alpha";
    char second_identity[] = "second";
    char second_payload[] = "bravo";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 8u));
    outbox_contract_add_record(&provider, 86u, 13u, first_identity, first_payload);
    outbox_contract_add_record(&provider, 86u, 13u, second_identity, second_payload);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    config.in_flight_messages = 2u;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 2u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EPROTO);
    check_equal(provider.fetch_calls, 2u);
    check_equal(sink.calls, 0u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(provider.requeue_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(snapshot.in_flight_messages, 1u);
    check_equal(strcmp(snapshot.error_stage, "duplicate_token"), 0);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)86u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("owns receipt bytes before an asynchronous Graph run consumes them") {
    char identity[] = "stable";
    char payload[] = "original";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 8u));
    outbox_contract_add_record(&provider, 83u, 10u, identity, payload);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(sink.calls, 0u);
    identity[0] = 'x';
    payload[0] = 'x';
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);
    check_equal(sink.calls, 1u);
    check_equal(sink.identity_size, 6u);
    check_equal(memcmp(sink.identity, "stable", 6u), 0);
    check_equal(sink.payload_size, 8u);
    check_equal(memcmp(sink.payload, "original", 8u), 0);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails before another fetch after the final message identifier is consumed") {
    char first_identity[] = "last";
    char first_payload[] = "accepted";
    char second_identity[] = "next";
    char second_payload[] = "blocked";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    outbox_contract_add_record(&provider, 84u, 11u, first_identity, first_payload);
    outbox_contract_add_record(&provider, 85u, 12u, second_identity, second_payload);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    config.first_message_id = UINT64_MAX;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 2u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(sink.calls, 1u);
    check_equal(sink.message_id, UINT64_MAX);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_ERANGE);
    check_equal(provider.fetch_calls, 1u);
    check_equal(provider.record_index, 1u);
    check_equal(provider.requeue_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(strcmp(snapshot.error_stage, "message_id"), 0);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("requeues an oversized fetched record without Graph projection") {
    char identity[] = "record";
    char payload[] = "payload-too-large";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);

    check_not_null(flow);
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 91u;
    provider.records[0].raft_index = 9u;
    provider.records[0].term = 3u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.max_payload_bytes = 4u;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EINVAL);
    check_equal(sink.calls, 0u);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)91u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(snapshot.status, SALTS_EINVAL);
    check_equal(snapshot.requeued, (uint64_t)1u);
    check_equal(snapshot.in_flight_messages, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("requeues an oversized identity without Graph projection") {
    char identity[] = "identity-too-large";
    char payload[] = "ok";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);

    check_not_null(flow);
    outbox_contract_add_record(&provider, 93u, 9u, identity, payload);
    outbox_contract_configure(&config, flow, &provider);
    config.max_identity_bytes = 4u;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EINVAL);
    check_equal(sink.calls, 0u);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)93u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("retains a rejected claim until its requeue succeeds") {
    char identity[] = "record";
    char payload[] = "payload-too-large";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);

    check_not_null(flow);
    provider.requeue_failures_remaining = 1u;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 92u;
    provider.records[0].raft_index = 9u;
    provider.records[0].term = 3u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.max_payload_bytes = 4u;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EIO);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING);
    check_equal(snapshot.in_flight_messages, 1u);
    check_equal(snapshot.requeued, (uint64_t)0u);
    check_equal(strcmp(snapshot.error_stage, "requeue_rejected"), 0);

    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EINVAL);
    check_equal(provider.requeue_calls, 2u);
    check_equal(provider.requeued_tokens[0], (uint64_t)92u);
    check_equal(provider.requeued_tokens[1], (uint64_t)92u);
    snapshot =
        (turbo_flow_turbodb_outbox_source_snapshot_t)TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(snapshot.status, SALTS_EINVAL);
    check_equal(snapshot.in_flight_messages, 0u);
    check_equal(snapshot.requeued, (uint64_t)1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("requeues one retryable Graph failure without acknowledgement") {
    char identity[] = "retry";
    char payload[] = "work";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    sink.status = SALTS_EIO;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 101u;
    provider.records[0].raft_index = 10u;
    provider.records[0].term = 3u;
    provider.records[0].delivery_attempt = 2u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)101u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.requeue_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("dead-letters a permanent failure once before retrying acknowledgement") {
    char identity[] = "dead";
    char payload[] = "letter";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    sink.status = SALTS_EPROTO;
    provider.disposition = TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT;
    provider.acknowledge_failures_remaining = 1u;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 111u;
    provider.records[0].raft_index = 11u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 3u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    config.permanent_failure_policy = TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.dead_letter_calls, 1u);
    check_equal(provider.dead_letter_tokens[0], (uint64_t)111u);
    check_equal(provider.dead_letter_statuses[0], SALTS_EPROTO);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EIO);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(provider.requeue_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.dead_letter_calls, 1u);
    check_equal(provider.acknowledge_calls, 2u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.dead_lettered, (uint64_t)1u);
    check_equal(snapshot.acknowledged, (uint64_t)1u);
    check_equal(snapshot.in_flight_messages, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("retries a failed dead-letter action before a later acknowledgement step") {
    char identity[] = "dead";
    char payload[] = "retry";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    sink.status = SALTS_EPROTO;
    provider.disposition = TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT;
    provider.dead_letter_failures_remaining = 1u;
    outbox_contract_add_record(&provider, 117u, 17u, identity, payload);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    config.permanent_failure_policy = TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EIO);
    check_equal(provider.dead_letter_calls, 1u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.dead_letter_calls, 2u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.dead_letter_calls, 2u);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("reports Graph admission rejection and requeues the retained claim on stop") {
    char identity[] = "full";
    char payload[] = "queue";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 1u));
    check(cflow_scheduler_post(&scheduler, outbox_contract_noop_task, NULL) != 0u);
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 112u;
    provider.records[0].raft_index = 12u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_ENOSPC);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING);
    check_equal(snapshot.status, SALTS_ENOSPC);
    check_equal(strcmp(snapshot.error_stage, "graph_request"), 0);
    check_equal(snapshot.outstanding_demand, 1u);
    check_equal(snapshot.in_flight_messages, 1u);
    check_equal(snapshot.fetched, (uint64_t)1u);
    check_equal(sink.calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)112u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(cflow_scheduler_run_until_idle(&scheduler, 0u), 1u);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects a RECORD carrying a non-success provider status") {
    char identity[] = "bad";
    char payload[] = "status";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);

    check_not_null(flow);
    provider.record_status = SALTS_EIO;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 113u;
    provider.records[0].raft_index = 13u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EPROTO);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)113u);
    check_equal(sink.calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(strcmp(snapshot.error_stage, "fetch_status"), 0);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails fast after requeue when the failure classifier returns an invalid value") {
    char identity[] = "bad";
    char payload[] = "class";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    sink.status = SALTS_EIO;
    provider.disposition = (turbo_flow_turbodb_outbox_failure_disposition_t)99;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 114u;
    provider.records[0].raft_index = 14u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EPROTO);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(strcmp(snapshot.error_stage, "classify_failure"), 0);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("returns a permanent fail-source status in the settling poll") {
    char identity[] = "fail";
    char payload[] = "source";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    sink.status = SALTS_EPROTO;
    provider.disposition = TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 115u;
    provider.records[0].raft_index = 15u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EPROTO);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("requeues rather than acknowledges an ACK-pending claim during stop") {
    char identity[] = "stop";
    char payload[] = "ack";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    provider.acknowledge_failures_remaining = 1u;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 116u;
    provider.records[0].raft_index = 16u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;

    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_EIO);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)116u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("arms WAIT only after demand and resumes once after wake") {
    char identity[] = "wake";
    char payload[] = "ready";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    provider.wait_before_records = 1;
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 121u;
    provider.records[0].raft_index = 12u;
    provider.records[0].term = 4u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.fetch_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.fetch_calls, 1u);
    check_equal(provider.wait_arms, 1u);
    check_not_null(provider.waker.wake);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_WAITING);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.fetch_calls, 1u);

    provider.ready = 1;
    provider.waker.wake(provider.waker.user);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.wait_cancels, 1u);
    check_equal(provider.fetch_calls, 2u);
    check_equal(sink.calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(provider.acknowledge_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("stops from WAIT through a quiescent waitable cancellation") {
    outbox_contract_probe_t provider = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(NULL);

    check_not_null(flow);
    provider.wait_before_records = 1;
    provider.wake_during_cancel = 1;
    outbox_contract_configure(&config, flow, &provider);
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_not_null(provider.waker.wake);
    check_equal(provider.wait_arms, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(provider.wait_cancels, 1u);
    check_null(provider.waker.wake);
    check_equal(provider.cancel_calls, 1u);
    check_equal(provider.fetch_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("surfaces a trimmed pending receipt as distinct data loss") {
    outbox_contract_probe_t provider = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(NULL);

    check_not_null(flow);
    provider.terminal_fetch_kind = TURBO_FLOW_TURBODB_OUTBOX_FETCH_DATA_LOSS;
    provider.terminal_fetch_status = SALTS_ENOENT;
    outbox_contract_configure(&config, flow, &provider);
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_ENOENT);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED);
    check_equal(snapshot.status, SALTS_ENOENT);
    check_equal(snapshot.data_loss_events, (uint64_t)1u);
    check_equal(strcmp(snapshot.error_stage, "data_loss"), 0);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(provider.requeue_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("stop cancels an active Graph run and requeues before destroy") {
    char identity[] = "stop";
    char payload[] = "pending";
    outbox_contract_probe_t provider = {0};
    outbox_sink_probe_t sink = {0};
    turbo_flow_turbodb_outbox_source_config_t config;
    turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
        TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
    turbo_flow_turbodb_outbox_source_t *source = NULL;
    turbo_flow_t *flow = outbox_contract_flow(&sink);
    cflow_scheduler scheduler = {0};

    check_not_null(flow);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    provider.record_count = 1u;
    provider.records[0] = (turbo_flow_turbodb_outbox_record_t)TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT;
    provider.records[0].token = 131u;
    provider.records[0].raft_index = 13u;
    provider.records[0].term = 5u;
    provider.records[0].delivery_attempt = 1u;
    provider.records[0].identity = vstr_from_buf(identity, sizeof(identity) - 1u);
    provider.records[0].payload = vstr_from_buf(payload, sizeof(payload) - 1u);
    outbox_contract_configure(&config, flow, &provider);
    config.scheduler = &scheduler;
    check_equal(turbo_flow_turbodb_outbox_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 2u), SALTS_OK);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_OK);
    check_equal(sink.calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_stop(source), SALTS_OK);
    check_equal(provider.cancel_calls, 1u);
    check_equal(provider.requeue_calls, 1u);
    check_equal(provider.requeued_tokens[0], (uint64_t)131u);
    check_equal(provider.acknowledge_calls, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED);
    check_equal(snapshot.in_flight_messages, 0u);
    check_equal(snapshot.in_flight_bytes, 0u);
    check_equal(turbo_flow_turbodb_outbox_source_request(source, 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_turbodb_outbox_source_poll(source, 1u), SALTS_ESHUTDOWN);
    check_equal(provider.fetch_calls, 1u);
    check_equal(turbo_flow_turbodb_outbox_source_destroy(source), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
