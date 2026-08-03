#include "flowie_cluster_shard_runtime_internal.h"

#include "flow_coronet_execution.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

static uint64_t flowie_cluster_shard_runtime_test_now(void *ctx) {
  (void)ctx;
  return UINT64_C(1000);
}

static int flowie_cluster_shard_runtime_test_reply(void *ctx,
                                                   const flowie_cluster_peer_frame_t *reply) {
  (void)ctx;
  return reply ? TURBO_OK : TURBO_EINVAL;
}

static int
flowie_cluster_shard_runtime_test_takeover_send(void *ctx, const flowie_cluster_peer_frame_t *frame,
                                                flowie_cluster_peer_send_complete_fn complete,
                                                void *complete_ctx) {
  (void)ctx;
  (void)frame;
  (void)complete;
  (void)complete_ctx;
  return TURBO_EIO;
}

static int flowie_cluster_shard_runtime_test_broadcast_publish(void *ctx, const void *payload,
                                                               size_t payload_size) {
  (void)ctx;
  return payload && payload_size != 0u ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_cluster_shard_runtime_test_target_claim(
    void *ctx, flowie_cluster_broadcast_target_claim_t *out) {
  (void)ctx;
  (void)out;
  return TURBO_ENOENT;
}

static int flowie_cluster_shard_runtime_test_target_settle(void *ctx, uint64_t token) {
  (void)ctx;
  return token != 0u ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_cluster_shard_runtime_test_lifecycle_apply(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_lifecycle_event_view_t *event,
    flowie_cluster_lifecycle_apply_complete_fn complete, void *complete_ctx) {
  (void)ctx;
  (void)current_owner;
  (void)event;
  (void)complete;
  (void)complete_ctx;
  return TURBO_EIO;
}

static int flowie_cluster_shard_runtime_test_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  (void)ctx;
  (void)command;
  (void)completion;
  (void)completion_ctx;
  return TURBO_EINVAL;
}

static void flowie_cluster_shard_runtime_test_fence(void *ctx, int reason) {
  (void)ctx;
  (void)reason;
}

static flowie_cluster_pgsql_config_t flowie_cluster_shard_runtime_test_coordinator(void) {
  flowie_cluster_pgsql_config_t config = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  config.conninfo = "host=127.0.0.1 port=1 connect_timeout=1";
  config.schema_name = "flowie_cluster";
  config.cluster_id = "cluster-a";
  config.listener_id = "mqtt-main";
  config.node_id = "node-a";
  config.advertised_endpoint = "127.0.0.1:7101";
  config.boot_id[0] = 1u;
  config.shard_count = 16u;
  config.lease_ttl_ms = 15000u;
  config.renew_interval_ms = 3000u;
  config.retry_interval_ms = 250u;
  config.worst_case_db_latency_ms = 1000u;
  config.safety_margin_ms = 1000u;
  config.create_schema = 1;
  return config;
}

static flowie_cluster_pgsql_fact_config_t
flowie_cluster_shard_runtime_test_fact(const flowie_cluster_pgsql_config_t *coordinator) {
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  config.coordinator = coordinator;
  config.max_key_size = 1024u;
  config.max_value_size = 65536u;
  config.max_batch_size = 8u;
  config.max_fact_records = 16u;
  config.max_receipts = 32u;
  config.max_dedupe_records = 32u;
  config.max_outbox_records = 32u;
  config.max_outbox_bytes = 1024u * 1024u;
  config.max_event_payload_size = 65536u;
  return config;
}

static flowie_cluster_session_bind_config_t flowie_cluster_shard_runtime_test_session(void) {
  flowie_cluster_session_bind_config_t config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  config.max_sessions = 8u;
  config.max_bind_payload_size = 2048u;
  config.max_fact_value_size = 8192u;
  config.max_event_payload_size = 512u;
  config.session.owner_instance_id = 9u;
  config.session.max_subscriptions = 8u;
  config.session.max_inflight = 8u;
  config.first_session_id = 10u;
  config.security_enabled = 1u;
  config.now = flowie_cluster_shard_runtime_test_now;
  return config;
}

static flowie_cluster_shard_runtime_config_t
flowie_cluster_shard_runtime_test_config(tf_coronet_execution_t *execution,
                                         const flowie_cluster_pgsql_fact_worker_config_t *worker,
                                         const flowie_cluster_session_bind_config_t *session) {
  flowie_cluster_shard_runtime_config_t config = FLOWIE_CLUSTER_SHARD_RUNTIME_CONFIG_INIT;
  config.shard_id = 0u;
  config.execution = execution;
  config.fact_worker = worker;
  config.session_bind = session;
  config.owner_max_payload_size = 4096u;
  config.owner_queue_entries = 4u;
  config.owner_queue_bytes = 1024u * 1024u;
  config.reply = flowie_cluster_shard_runtime_test_reply;
  return config;
}

spec("flowie cluster shard runtime") {
  it("accepts one bounded unambiguous composition") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    tf_coronet_execution_destroy(&execution);
  }

  it("rejects competing persistence and fencing callbacks") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    session.submit = flowie_cluster_shard_runtime_test_submit;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    session.submit = NULL;
    session.self_fence = flowie_cluster_shard_runtime_test_fence;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("requires an all-or-none takeover dispatcher configuration") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    config.takeover_poll_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.takeover_send = flowie_cluster_shard_runtime_test_takeover_send;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.takeover_retry_interval_ns = UINT64_C(1000000);
    config.takeover_reply_timeout_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    config.takeover_send = NULL;
    config.takeover_poll_interval_ns = 0u;
    config.takeover_retry_interval_ns = 0u;
    config.takeover_reply_timeout_ns = 0u;
    config.takeover_send_ctx = &config;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("requires an all-or-none durable delivery dispatcher configuration") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    config.delivery_poll_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.delivery_send = flowie_cluster_shard_runtime_test_takeover_send;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.delivery_retry_interval_ns = UINT64_C(1000000);
    config.delivery_reply_timeout_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    config.delivery_send = NULL;
    config.delivery_poll_interval_ns = 0u;
    config.delivery_retry_interval_ns = 0u;
    config.delivery_reply_timeout_ns = 0u;
    config.delivery_send_ctx = &config;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("requires an all-or-none lifecycle consumer configuration") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    config.lifecycle_poll_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.lifecycle_apply = flowie_cluster_shard_runtime_test_lifecycle_apply;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.lifecycle_retry_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    config.lifecycle_apply = NULL;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    config.lifecycle_poll_interval_ns = 0u;
    config.lifecycle_retry_interval_ns = 0u;
    config.lifecycle_apply_ctx = &config;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("requires a bounded all-or-none source broadcast configuration") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    config.broadcast_publish = flowie_cluster_shard_runtime_test_broadcast_publish;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.broadcast_max_payload_size =
        session.max_event_payload_size + FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE;
    config.broadcast_poll_interval_ns = UINT64_C(1000000);
    config.broadcast_ack_poll_interval_ns = UINT64_C(1000000);
    config.broadcast_retry_interval_ns = UINT64_C(1000000);
    config.broadcast_republish_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    config.broadcast_max_payload_size -= 1u;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.broadcast_publish = NULL;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("requires a bounded all-or-none target broadcast configuration") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    config.broadcast_target_claim = flowie_cluster_shard_runtime_test_target_claim;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.broadcast_target_ack = flowie_cluster_shard_runtime_test_target_settle;
    config.broadcast_target_requeue = flowie_cluster_shard_runtime_test_target_settle;
    config.broadcast_target_max_payload_size =
        session.max_event_payload_size + FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE;
    config.broadcast_target_poll_interval_ns = UINT64_C(1000000);
    config.broadcast_target_retry_interval_ns = UINT64_C(1000000);
    config.broadcast_target_ack_retry_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_OK);
    config.broadcast_target_ack = NULL;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.broadcast_target_ack = flowie_cluster_shard_runtime_test_target_settle;
    config.broadcast_target_max_payload_size = FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.broadcast_target_claim = NULL;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("rejects shard and capacity mismatches before database I/O") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    config.shard_id = coordinator.shard_count;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    config.shard_id = 0u;
    fact.max_fact_records = session.max_sessions - 1u;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    fact.max_fact_records = 16u;
    config.owner_queue_bytes = 1u;
    check_int_eq(flowie_cluster_shard_runtime_config_validate(&config), TURBO_EINVAL);
    tf_coronet_execution_destroy(&execution);
  }

  it("clears output when shard claim cannot open PostgreSQL") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator = flowie_cluster_shard_runtime_test_coordinator();
    flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_shard_runtime_test_fact(&coordinator);
    flowie_cluster_pgsql_fact_worker_config_t worker = FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t session = flowie_cluster_shard_runtime_test_session();
    flowie_cluster_shard_runtime_config_t config;
    flowie_cluster_shard_runtime_t *runtime = (flowie_cluster_shard_runtime_t *)(uintptr_t)1u;
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    worker.fact = &fact;
    worker.max_queue_entries = 4u;
    worker.max_queue_bytes = 1024u * 1024u;
    config = flowie_cluster_shard_runtime_test_config(&execution, &worker, &session);
    check_int_eq(flowie_cluster_shard_runtime_create(&config, &runtime), TURBO_EIO);
    check_null(runtime);
    check_int_eq(flowie_cluster_shard_runtime_destroy(NULL), TURBO_OK);
    tf_coronet_execution_destroy(&execution);
  }
}
