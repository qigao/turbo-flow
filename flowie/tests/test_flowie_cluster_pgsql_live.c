#include "flowie_cluster_pgsql_internal.h"
#include "flowie_cluster_session_bind_internal.h"
#include "flowie_cluster_shard_runtime_internal.h"

#include "flow_coronet_execution.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static flowie_cluster_pgsql_config_t
flowie_cluster_pgsql_live_config(const char *conninfo, const char *cluster_id, const char *node_id,
                                 uint8_t boot_byte, int create_schema) {
  flowie_cluster_pgsql_config_t config = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  config.conninfo = conninfo;
  config.schema_name = "flowie_cluster_test";
  config.cluster_id = cluster_id;
  config.listener_id = "mqtt-main";
  config.node_id = node_id;
  config.advertised_endpoint = "127.0.0.1:7101";
  config.boot_id[0] = boot_byte;
  config.shard_count = 1u;
  config.lease_ttl_ms = 5000u;
  config.renew_interval_ms = 1000u;
  config.retry_interval_ms = 100u;
  config.worst_case_db_latency_ms = 500u;
  config.safety_margin_ms = 100u;
  config.create_schema = create_schema;
  return config;
}

static flowie_cluster_pgsql_fact_config_t
flowie_cluster_pgsql_live_fact_config(const flowie_cluster_pgsql_config_t *coordinator) {
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  config.coordinator = coordinator;
  config.max_key_size = 1024u;
  config.max_value_size = 65536u;
  config.max_batch_size = 8u;
  config.max_fact_records = 8u;
  config.max_receipts = 16u;
  config.max_dedupe_records = 16u;
  config.max_outbox_records = 16u;
  config.max_outbox_bytes = 1024u * 1024u;
  config.max_event_payload_size = 65536u;
  return config;
}

static flowie_cluster_pgsql_fact_command_t flowie_cluster_pgsql_live_fact_command(
    const flowie_cluster_owner_token_t *owner, uint8_t command_byte, const uint8_t *key,
    size_t key_size, const uint8_t *value, size_t value_size, uint64_t expected_revision,
    uint64_t next_revision, flowie_cluster_pgsql_fact_mutation_t *mutation) {
  flowie_cluster_pgsql_fact_command_t command = FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  mutation->shard_key = key;
  mutation->shard_key_size = key_size;
  mutation->record.key = key;
  mutation->record.key_size = key_size;
  mutation->record.expected_revision = expected_revision;
  mutation->record.next_revision = next_revision;
  mutation->record.value = value;
  mutation->record.value_size = value_size;
  mutation->event_type = 1u;
  mutation->event_payload = value;
  mutation->event_payload_size = value_size;
  command.command_id[0] = command_byte;
  command.owner = *owner;
  command.mutations = mutation;
  command.mutation_count = 1u;
  return command;
}

typedef struct flowie_cluster_pgsql_live_completion_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int done;
  int status;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
} flowie_cluster_pgsql_live_completion_t;

typedef struct flowie_cluster_pgsql_live_scan_s {
  size_t count;
  tstr_t key;
  tstr_t value;
  uint64_t revision;
} flowie_cluster_pgsql_live_scan_t;

static int flowie_cluster_pgsql_live_scan_visit(void *ctx, const turbo_flow_record_view_t *record) {
  flowie_cluster_pgsql_live_scan_t *scan = (flowie_cluster_pgsql_live_scan_t *)ctx;
  tstr_t key;
  tstr_t value;
  if (!scan || !record || scan->count != 0u) return TURBO_EPROTO;
  key = tstr_new_len(record->key, record->key_size);
  value = tstr_new_len(record->value, record->value_size);
  if (!key || !value) {
    tstr_free(key);
    tstr_free(value);
    return TURBO_ENOMEM;
  }
  scan->key = key;
  scan->value = value;
  scan->revision = record->revision;
  ++scan->count;
  return TURBO_OK;
}

static void flowie_cluster_pgsql_live_complete(
    void *ctx, const uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE], int status) {
  flowie_cluster_pgsql_live_completion_t *completion =
      (flowie_cluster_pgsql_live_completion_t *)ctx;
  turbo_mutex_lock(&completion->mutex);
  memcpy(completion->command_id, command_id, sizeof(completion->command_id));
  completion->status = status;
  completion->done = 1;
  turbo_cond_broadcast(&completion->changed);
  turbo_mutex_unlock(&completion->mutex);
}

typedef struct flowie_cluster_pgsql_live_bind_completion_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int done;
  int durable_status;
  int fence_calls;
  flowie_cluster_peer_owner_finalize_fn finalize;
  void *finalize_ctx;
} flowie_cluster_pgsql_live_bind_completion_t;

typedef struct flowie_cluster_pgsql_live_runtime_reply_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int done;
  int callback_status;
  int frame_status;
  tstr_t payload;
} flowie_cluster_pgsql_live_runtime_reply_t;

typedef struct flowie_cluster_pgsql_live_lifecycle_s {
  turbo_mutex_t mutex;
  int apply_count;
  int identity_valid;
  uint32_t shard_id;
  uint64_t owner_epoch;
  uint64_t expected_fact_revision;
  uint64_t connection_generation;
  uint64_t session_generation;
} flowie_cluster_pgsql_live_lifecycle_t;

static int flowie_cluster_pgsql_live_lifecycle_apply(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_lifecycle_event_view_t *event,
    flowie_cluster_lifecycle_apply_complete_fn complete, void *complete_ctx) {
  static const uint8_t client_id[] = "live-device";
  flowie_cluster_pgsql_live_lifecycle_t *observed = (flowie_cluster_pgsql_live_lifecycle_t *)ctx;
  if (!observed || !current_owner || !event || !complete || !complete_ctx) return TURBO_EINVAL;
  turbo_mutex_lock(&observed->mutex);
  ++observed->apply_count;
  observed->identity_valid =
      event->client_id.size == sizeof(client_id) - 1u &&
      memcmp(event->client_id.data, client_id, sizeof(client_id) - 1u) == 0 &&
      event->connection_id == 1u && event->session_id != 0u;
  observed->shard_id = event->shard_id;
  observed->owner_epoch = current_owner->owner_epoch;
  observed->expected_fact_revision = event->expected_fact_revision;
  observed->connection_generation = event->connection_generation;
  observed->session_generation = event->session_generation;
  turbo_mutex_unlock(&observed->mutex);
  complete(complete_ctx, TURBO_OK);
  return TURBO_OK;
}

static int flowie_cluster_pgsql_live_runtime_reply(void *ctx,
                                                   const flowie_cluster_peer_frame_t *reply) {
  flowie_cluster_pgsql_live_runtime_reply_t *observed =
      (flowie_cluster_pgsql_live_runtime_reply_t *)ctx;
  tstr_t payload = NULL;
  int rc = TURBO_OK;
  if (!observed || !reply) return TURBO_EINVAL;
  if (reply->payload.len != 0u) {
    payload = tstr_new_len(reply->payload.data, reply->payload.len);
    if (!payload) rc = TURBO_ENOMEM;
  }
  turbo_mutex_lock(&observed->mutex);
  observed->callback_status = rc;
  observed->frame_status = reply->status;
  observed->payload = payload;
  observed->done = 1;
  turbo_cond_broadcast(&observed->changed);
  turbo_mutex_unlock(&observed->mutex);
  return rc;
}

static int flowie_cluster_pgsql_live_bind_complete(void *ctx, int durable_status,
                                                   flowie_cluster_peer_owner_finalize_fn finalize,
                                                   void *finalize_ctx) {
  flowie_cluster_pgsql_live_bind_completion_t *completion =
      (flowie_cluster_pgsql_live_bind_completion_t *)ctx;
  if (!completion || !finalize || !finalize_ctx) return TURBO_EINVAL;
  turbo_mutex_lock(&completion->mutex);
  completion->durable_status = durable_status;
  completion->finalize = finalize;
  completion->finalize_ctx = finalize_ctx;
  completion->done = 1;
  turbo_cond_broadcast(&completion->changed);
  turbo_mutex_unlock(&completion->mutex);
  return TURBO_OK;
}

static uint64_t flowie_cluster_pgsql_live_bind_now(void *ctx) {
  (void)ctx;
  return UINT64_C(1000);
}

static void flowie_cluster_pgsql_live_bind_fence(void *ctx, int reason) {
  flowie_cluster_pgsql_live_bind_completion_t *completion =
      (flowie_cluster_pgsql_live_bind_completion_t *)ctx;
  (void)reason;
  turbo_mutex_lock(&completion->mutex);
  ++completion->fence_calls;
  turbo_mutex_unlock(&completion->mutex);
}

static flowie_cluster_session_bind_config_t
flowie_cluster_pgsql_live_bind_config(flowie_cluster_pgsql_fact_worker_t *fact_worker,
                                      flowie_cluster_pgsql_live_bind_completion_t *completion) {
  flowie_cluster_session_bind_config_t config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  config.max_sessions = 4u;
  config.max_bind_payload_size = 2048u;
  config.max_fact_value_size = 8192u;
  config.max_event_payload_size = 512u;
  config.session.owner_instance_id = 9u;
  config.session.max_subscriptions = 8u;
  config.session.max_inflight = 8u;
  config.first_session_id = 10u;
  config.security_enabled = 1u;
  config.submit = flowie_cluster_session_bind_pgsql_submit;
  config.submit_ctx = fact_worker;
  config.now = flowie_cluster_pgsql_live_bind_now;
  config.self_fence = flowie_cluster_pgsql_live_bind_fence;
  config.self_fence_ctx = completion;
  return config;
}

static int flowie_cluster_pgsql_live_bind_payload(tstr_t *out) {
  static const uint8_t empty_property = 0u;
  static const uint8_t client_id[] = "live-device";
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)strcpy(principal.principal_id, "live-writer");
  (void)strcpy(principal.principal_type, "device");
  (void)strcpy(principal.root_group_id, "root-a");
  (void)strcpy(principal.auth_method, "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  principal.role_count = 1u;
  (void)strcpy(principal.roles[0], "writer");
  principal.group_count = 1u;
  (void)strcpy(principal.groups[0], "root-a");
  principal.policy_version = 1u;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.properties.values.data = &empty_property;
  connect.will_properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.will_properties.values.data = &empty_property;
  return flowie_cluster_peer_connect_bind_encode(&connect, &principal, 2048u, out);
}

spec("flowie PostgreSQL cluster coordinator live") {
  it("publishes bounded membership snapshots without advancing revision on lease renew") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    flowie_cluster_pgsql_config_t first_config;
    flowie_cluster_pgsql_config_t second_config;
    flowie_cluster_pgsql_coordinator_t *first = NULL;
    flowie_cluster_pgsql_coordinator_t *second = NULL;
    flowie_cluster_owner_token_t first_owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_owner_token_t second_owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_membership_snapshot_t snapshot =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_pgsql_membership_snapshot_t renewed =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_pgsql_shard_owner_snapshot_t owners =
        FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT;
    flowie_cluster_pgsql_shard_owner_snapshot_t released =
        FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT;
    uint64_t first_deadline = 0u;
    uint64_t second_deadline = 0u;
    uint64_t renewed_deadline = 0u;
    uint64_t revision;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-membership-%llu",
                   (unsigned long long)turbo_hrtime());
    first_config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-a", 1u, 1);
    first_config.advertised_endpoint = "127.0.0.1:7101";
    first_config.shard_count = 2u;
    second_config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-b", 2u, 0);
    second_config.advertised_endpoint = "127.0.0.1:7102";
    second_config.shard_count = 2u;
    check_int_eq(flowie_cluster_pgsql_coordinator_open(&first_config, &first), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_coordinator_open(&second_config, &second), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_claim(first, 0u, turbo_hrtime(), &first_owner,
                                                  &first_deadline),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_claim(second, 1u, turbo_hrtime(), &second_owner,
                                                  &second_deadline),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_owner_snapshot(first, &owners), TURBO_OK);
    check_size_eq(owners.owner_count, 2u);
    check_uint_eq(owners.owners[0].shard_id, 0u);
    check_uint_eq(owners.owners[0].owner.owner_epoch, first_owner.owner_epoch);
    check_str_eq(owners.owners[0].owner.node_id, first_owner.node_id);
    check_mem_eq(owners.owners[0].owner.boot_id, first_owner.boot_id,
                 FLOWIE_CLUSTER_BOOT_ID_SIZE);
    check_true(owners.owners[0].local_deadline_ns > 0u);
    check_uint_eq(owners.owners[1].shard_id, 1u);
    check_uint_eq(owners.owners[1].owner.owner_epoch, second_owner.owner_epoch);
    check_str_eq(owners.owners[1].owner.node_id, second_owner.node_id);
    check_mem_eq(owners.owners[1].owner.boot_id, second_owner.boot_id,
                 FLOWIE_CLUSTER_BOOT_ID_SIZE);
    check_true(owners.owners[1].local_deadline_ns > 0u);
    check_int_eq(flowie_cluster_pgsql_membership_snapshot(first, 1u, &snapshot), TURBO_ENOSPC);
    check_null(snapshot.members);
    check_int_eq(flowie_cluster_pgsql_membership_snapshot(first, 2u, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.membership_revision, 2u);
    check_size_eq(snapshot.member_count, 2u);
    check_str_eq(snapshot.members[0].node_id, "node-a");
    check_str_eq(snapshot.members[0].advertised_endpoint, "127.0.0.1:7101");
    check_str_eq(snapshot.members[1].node_id, "node-b");
    check_str_eq(snapshot.members[1].advertised_endpoint, "127.0.0.1:7102");
    revision = snapshot.membership_revision;
    check_int_eq(flowie_cluster_pgsql_shard_renew(first, &first_owner, turbo_hrtime(),
                                                  &renewed_deadline),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_membership_snapshot(first, 2u, &renewed), TURBO_OK);
    check_uint_eq(renewed.membership_revision, revision);
    check_uint_eq(renewed.members[0].revision, snapshot.members[0].revision);
    check_int_eq(flowie_cluster_pgsql_shard_release(second, &second_owner), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_owner_snapshot(first, &released), TURBO_OK);
    check_size_eq(released.owner_count, 2u);
    check_uint_eq(released.owners[0].owner.owner_epoch, first_owner.owner_epoch);
    check_true(released.owners[0].local_deadline_ns > 0u);
    check_uint_eq(released.owners[1].owner.owner_epoch, 0u);
    check_uint_eq(released.owners[1].local_deadline_ns, 0u);
    check_int_eq(flowie_cluster_pgsql_shard_release(first, &first_owner), TURBO_OK);
    flowie_cluster_pgsql_shard_owner_snapshot_cleanup(&released);
    flowie_cluster_pgsql_shard_owner_snapshot_cleanup(&owners);
    flowie_cluster_pgsql_membership_snapshot_cleanup(&renewed);
    flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
    flowie_cluster_pgsql_coordinator_destroy(second);
    flowie_cluster_pgsql_coordinator_destroy(first);
  }

  it("advances epochs and fences stale owners across two coordinators") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    flowie_cluster_pgsql_config_t first_config;
    flowie_cluster_pgsql_config_t second_config;
    flowie_cluster_pgsql_coordinator_t *first = NULL;
    flowie_cluster_pgsql_coordinator_t *second = NULL;
    flowie_cluster_owner_token_t first_token = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_owner_token_t second_token = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    uint64_t deadline = 0u;
    uint64_t request_start;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-live-%llu",
                   (unsigned long long)turbo_hrtime());
    first_config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-a", 1u, 1);
    second_config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-b", 2u, 0);
    check_int_eq(flowie_cluster_pgsql_coordinator_open(&first_config, &first), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_coordinator_open(&second_config, &second), TURBO_OK);

    request_start = turbo_hrtime();
    check_int_eq(
        flowie_cluster_pgsql_shard_claim(first, 0u, request_start, &first_token, &deadline),
        TURBO_OK);
    check_hex64_eq(first_token.owner_epoch, 1u);
    check_true(deadline > request_start);
    check_int_eq(
        flowie_cluster_pgsql_shard_claim(second, 0u, turbo_hrtime(), &second_token, &deadline),
        TURBO_EBUSY);
    check_int_eq(flowie_cluster_pgsql_shard_renew(first, &first_token, turbo_hrtime(), &deadline),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_require(first, &first_token, turbo_hrtime(), &deadline),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_release(first, &first_token), TURBO_OK);

    check_int_eq(
        flowie_cluster_pgsql_shard_claim(second, 0u, turbo_hrtime(), &second_token, &deadline),
        TURBO_OK);
    check_hex64_eq(second_token.owner_epoch, 2u);
    check_int_eq(flowie_cluster_pgsql_shard_require(first, &first_token, turbo_hrtime(), &deadline),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_pgsql_shard_release(second, &second_token), TURBO_OK);
    flowie_cluster_pgsql_coordinator_destroy(second);
    flowie_cluster_pgsql_coordinator_destroy(first);
  }

  it("renews on its database worker and releases during close") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    flowie_cluster_pgsql_config_t config;
    flowie_cluster_pgsql_lease_worker_t *worker = NULL;
    flowie_cluster_pgsql_lease_snapshot_t before = FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
    flowie_cluster_pgsql_lease_snapshot_t after = FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-worker-%llu",
                   (unsigned long long)turbo_hrtime());
    config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-worker", 3u, 1);
    config.renew_interval_ms = 50u;
    check_int_eq(flowie_cluster_pgsql_lease_worker_create(&config, 0u, &worker), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_lease_worker_snapshot(worker, &before), TURBO_OK);
    check_int_eq(before.state, FLOWIE_CLUSTER_SHARD_RECOVERING);
    check_int_eq(flowie_cluster_pgsql_lease_worker_activate(worker, &before.owner), TURBO_OK);
    turbo_sleep_ms(150u);
    check_int_eq(flowie_cluster_pgsql_lease_worker_snapshot(worker, &after), TURBO_OK);
    check_int_eq(after.state, FLOWIE_CLUSTER_SHARD_ACTIVE);
    check_int_eq(after.last_coordinator_status, TURBO_OK);
    check_true(after.local_deadline_ns > before.local_deadline_ns);
    check_int_eq(flowie_cluster_pgsql_lease_worker_close(worker), TURBO_OK);
    flowie_cluster_pgsql_lease_worker_destroy(worker);
  }

  it("recovers TFSE facts before activating the matching shard lease") {
    static const uint8_t client_id[] = "live-device";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    flowie_cluster_pgsql_config_t coordinator_config;
    flowie_cluster_pgsql_fact_config_t fact_config;
    flowie_cluster_pgsql_fact_worker_config_t fact_worker_config =
        FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_pgsql_lease_worker_t *lease_worker = NULL;
    flowie_cluster_pgsql_fact_worker_t *fact_worker = NULL;
    flowie_cluster_pgsql_fact_store_t *fact_store = NULL;
    flowie_cluster_pgsql_lease_snapshot_t lease = FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
    flowie_cluster_pgsql_live_bind_completion_t completion;
    flowie_cluster_session_bind_config_t bind_config;
    flowie_cluster_session_bind_t *source = NULL;
    flowie_cluster_session_bind_t *recovered = NULL;
    flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    flowie_session_snapshot_t session = FLOWIE_SESSION_SNAPSHOT_INIT;
    tstr_t payload = NULL;
    tstr_t reply = NULL;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    memset(&completion, 0, sizeof(completion));
    turbo_mutex_init(&completion.mutex);
    turbo_cond_init(&completion.changed);
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-bind-recovery-%llu",
                   (unsigned long long)turbo_hrtime());
    coordinator_config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-bind", 6u, 1);
    fact_config = flowie_cluster_pgsql_live_fact_config(&coordinator_config);
    fact_worker_config.fact = &fact_config;
    fact_worker_config.max_queue_entries = 4u;
    fact_worker_config.max_queue_bytes = 1024u * 1024u;
    check_int_eq(flowie_cluster_pgsql_lease_worker_create(&coordinator_config, 0u, &lease_worker),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_lease_worker_snapshot(lease_worker, &lease), TURBO_OK);
    check_int_eq(lease.state, FLOWIE_CLUSTER_SHARD_RECOVERING);
    check_int_eq(flowie_cluster_pgsql_fact_store_open(&fact_config, &fact_store), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_fact_worker_create(&fact_worker_config, &fact_worker),
                 TURBO_OK);
    bind_config = flowie_cluster_pgsql_live_bind_config(fact_worker, &completion);
    check_int_eq(flowie_cluster_session_bind_create(&bind_config, &source), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_live_bind_payload(&payload), TURBO_OK);
    command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
    command.operation = FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND;
    command.shard_id = lease.owner.shard_id;
    command.owner_epoch = lease.owner.owner_epoch;
    command.connection_id = 1u;
    command.connection_generation = 1u;
    command.source_node_id = tstr_v_from_cstr("edge-live");
    command.target_node_id = tstr_v_from_buf(lease.owner.node_id, lease.owner.node_id_size);
    command.source_boot_id[0] = 9u;
    memcpy(command.target_boot_id, lease.owner.boot_id, sizeof(command.target_boot_id));
    command.correlation_id[0] = 1u;
    command.payload = tstr_v_from_buf(payload, tstr_len(payload));
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     source, &command, flowie_cluster_pgsql_live_bind_complete, &completion),
                 TURBO_OK);
    turbo_mutex_lock(&completion.mutex);
    while (!completion.done)
      turbo_cond_wait(&completion.changed, &completion.mutex);
    turbo_mutex_unlock(&completion.mutex);
    check_int_eq(completion.durable_status, TURBO_OK);
    check_int_eq(completion.finalize(completion.finalize_ctx, completion.durable_status, &reply),
                 TURBO_OK);
    check_not_null(reply);
    check_int_eq(flowie_cluster_session_bind_destroy(source), TURBO_OK);
    source = NULL;
    check_int_eq(flowie_cluster_session_bind_create(&bind_config, &recovered), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_recover_pgsql(recovered, fact_store, lease_worker),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_lease_worker_snapshot(lease_worker, &lease), TURBO_OK);
    check_int_eq(lease.state, FLOWIE_CLUSTER_SHARD_ACTIVE);
    check_int_eq(
        flowie_cluster_session_bind_snapshot(
            recovered, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &session, NULL),
        TURBO_OK);
    check_false(session.active);
    check_uint_eq(session.resource_generation, 2u);
    check_int_eq(completion.fence_calls, 0);
    check_int_eq(flowie_cluster_pgsql_fact_worker_close(fact_worker), TURBO_OK);
    flowie_cluster_pgsql_fact_worker_destroy(fact_worker);
    check_int_eq(flowie_cluster_session_bind_destroy(recovered), TURBO_OK);
    flowie_cluster_pgsql_fact_store_destroy(fact_store);
    check_int_eq(flowie_cluster_pgsql_lease_worker_close(lease_worker), TURBO_OK);
    flowie_cluster_pgsql_lease_worker_destroy(lease_worker);
    tstr_free(reply);
    tstr_free(payload);
    turbo_cond_destroy(&completion.changed);
    turbo_mutex_destroy(&completion.mutex);
  }

  it("composes an active shard owner and settles TFLE through its runtime consumer") {
    static const uint8_t client_id[] = "live-device";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    turbo_flow_coronet_execution_binding_t execution_binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_pgsql_config_t coordinator_config;
    flowie_cluster_pgsql_fact_config_t fact_config;
    flowie_cluster_pgsql_fact_worker_config_t fact_worker_config =
        FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_session_bind_config_t bind_config;
    flowie_cluster_shard_runtime_config_t runtime_config = FLOWIE_CLUSTER_SHARD_RUNTIME_CONFIG_INIT;
    flowie_cluster_shard_runtime_t *runtime = NULL;
    flowie_cluster_pgsql_lease_snapshot_t lease = FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
    flowie_cluster_pgsql_live_bind_completion_t completion;
    flowie_cluster_pgsql_live_runtime_reply_t observed;
    flowie_cluster_pgsql_live_lifecycle_t lifecycle_observed;
    flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    flowie_cluster_session_bind_reply_view_t decoded = FLOWIE_CLUSTER_SESSION_BIND_REPLY_VIEW_INIT;
    flowie_session_snapshot_t session = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_lifecycle_dispatcher_snapshot_t lifecycle_snapshot =
        FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_SNAPSHOT_INIT;
    tstr_t payload = NULL;
    tstr_t lost_payload = NULL;
    uint64_t lifecycle_deadline;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    memset(&completion, 0, sizeof(completion));
    memset(&observed, 0, sizeof(observed));
    memset(&lifecycle_observed, 0, sizeof(lifecycle_observed));
    turbo_mutex_init(&completion.mutex);
    turbo_cond_init(&completion.changed);
    turbo_mutex_init(&observed.mutex);
    turbo_cond_init(&observed.changed);
    turbo_mutex_init(&lifecycle_observed.mutex);
    execution_binding.size = sizeof(execution_binding);
    execution_binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &execution_binding), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-runtime-%llu",
                   (unsigned long long)turbo_hrtime());
    coordinator_config =
        flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-runtime", 7u, 1);
    fact_config = flowie_cluster_pgsql_live_fact_config(&coordinator_config);
    fact_worker_config.fact = &fact_config;
    fact_worker_config.max_queue_entries = 4u;
    fact_worker_config.max_queue_bytes = 1024u * 1024u;
    bind_config = flowie_cluster_pgsql_live_bind_config(NULL, &completion);
    bind_config.submit = NULL;
    bind_config.submit_ctx = NULL;
    bind_config.self_fence = NULL;
    bind_config.self_fence_ctx = NULL;
    runtime_config.execution = &execution;
    runtime_config.fact_worker = &fact_worker_config;
    runtime_config.session_bind = &bind_config;
    runtime_config.owner_max_payload_size = 2048u;
    runtime_config.owner_queue_entries = 4u;
    runtime_config.owner_queue_bytes = 1024u * 1024u;
    runtime_config.reply = flowie_cluster_pgsql_live_runtime_reply;
    runtime_config.reply_ctx = &observed;
    runtime_config.self_fence = flowie_cluster_pgsql_live_bind_fence;
    runtime_config.self_fence_ctx = &completion;
    runtime_config.lifecycle_apply = flowie_cluster_pgsql_live_lifecycle_apply;
    runtime_config.lifecycle_apply_ctx = &lifecycle_observed;
    runtime_config.lifecycle_poll_interval_ns = UINT64_C(1000000);
    runtime_config.lifecycle_retry_interval_ns = UINT64_C(1000000);
    check_int_eq(flowie_cluster_shard_runtime_create(&runtime_config, &runtime), TURBO_OK);
    check_int_eq(flowie_cluster_shard_runtime_snapshot(runtime, &lease), TURBO_OK);
    check_int_eq(lease.state, FLOWIE_CLUSTER_SHARD_ACTIVE);
    check_int_eq(flowie_cluster_pgsql_live_bind_payload(&payload), TURBO_OK);
    command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
    command.operation = FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND;
    command.shard_id = lease.owner.shard_id;
    command.owner_epoch = lease.owner.owner_epoch;
    command.connection_id = 1u;
    command.connection_generation = 1u;
    command.cluster_id = tstr_v_from_cstr(coordinator_config.cluster_id);
    command.listener_id = tstr_v_from_cstr(coordinator_config.listener_id);
    command.source_node_id = tstr_v_from_cstr("edge-live");
    command.target_node_id = tstr_v_from_buf(lease.owner.node_id, lease.owner.node_id_size);
    command.source_boot_id[0] = 9u;
    memcpy(command.target_boot_id, lease.owner.boot_id, sizeof(command.target_boot_id));
    command.correlation_id[0] = 1u;
    command.payload = tstr_v_from_buf(payload, tstr_len(payload));
    check_int_eq(flowie_cluster_shard_runtime_submit(runtime, &command), TURBO_OK);
    turbo_mutex_lock(&observed.mutex);
    while (!observed.done)
      turbo_cond_wait(&observed.changed, &observed.mutex);
    turbo_mutex_unlock(&observed.mutex);
    check_int_eq(observed.callback_status, TURBO_OK);
    check_int_eq(observed.frame_status, TURBO_OK);
    check_not_null(observed.payload);
    check_int_eq(flowie_cluster_session_bind_reply_decode(
                     observed.payload, tstr_len(observed.payload), 2048u, &decoded),
                 TURBO_OK);
    check_true(decoded.accepted);
    check_int_eq(
        flowie_cluster_shard_runtime_session_snapshot(
            runtime, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &session, NULL),
        TURBO_OK);
    check_true(session.active);
    check_int_eq(flowie_cluster_peer_connection_lost_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     2048u, &lost_payload),
                 TURBO_OK);
    tstr_free(observed.payload);
    observed.payload = NULL;
    observed.done = 0;
    command.operation = FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST;
    command.correlation_id[0] = 2u;
    command.payload = tstr_v_from_buf(lost_payload, tstr_len(lost_payload));
    check_int_eq(flowie_cluster_shard_runtime_submit(runtime, &command), TURBO_OK);
    turbo_mutex_lock(&observed.mutex);
    while (!observed.done)
      turbo_cond_wait(&observed.changed, &observed.mutex);
    turbo_mutex_unlock(&observed.mutex);
    check_int_eq(observed.callback_status, TURBO_OK);
    check_int_eq(observed.frame_status, TURBO_OK);
    check_int_eq(
        flowie_cluster_shard_runtime_session_snapshot(
            runtime, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &session, NULL),
        TURBO_OK);
    check_false(session.active);
    lifecycle_deadline = turbo_monotonic_ms() + 5000u;
    do {
      check_int_eq(flowie_cluster_shard_runtime_lifecycle_snapshot(runtime, &lifecycle_snapshot),
                   TURBO_OK);
      if (lifecycle_snapshot.settled_events == 1u) break;
      turbo_sleep_ms(1u);
    } while (turbo_monotonic_ms() < lifecycle_deadline);
    check_uint_eq(lifecycle_snapshot.settled_events, 1u);
    {
      int apply_count;
      int identity_valid;
      uint32_t shard_id;
      uint64_t owner_epoch;
      uint64_t expected_fact_revision;
      uint64_t connection_generation;
      uint64_t session_generation;
      turbo_mutex_lock(&lifecycle_observed.mutex);
      apply_count = lifecycle_observed.apply_count;
      identity_valid = lifecycle_observed.identity_valid;
      shard_id = lifecycle_observed.shard_id;
      owner_epoch = lifecycle_observed.owner_epoch;
      expected_fact_revision = lifecycle_observed.expected_fact_revision;
      connection_generation = lifecycle_observed.connection_generation;
      session_generation = lifecycle_observed.session_generation;
      turbo_mutex_unlock(&lifecycle_observed.mutex);
      check_int_eq(apply_count, 1);
      check_true(identity_valid);
      check_uint_eq(shard_id, lease.owner.shard_id);
      check_uint_eq(owner_epoch, lease.owner.owner_epoch);
      check_uint_eq(expected_fact_revision, session.resource_generation);
      check_uint_eq(connection_generation, 1u);
      check_uint_eq(session_generation, session.session_generation);
    }
    check_int_eq(completion.fence_calls, 0);
    check_int_eq(flowie_cluster_shard_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_shard_runtime_destroy(runtime), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    tstr_free(observed.payload);
    tstr_free(payload);
    tstr_free(lost_payload);
    turbo_mutex_destroy(&lifecycle_observed.mutex);
    turbo_cond_destroy(&observed.changed);
    turbo_mutex_destroy(&observed.mutex);
    turbo_cond_destroy(&completion.changed);
    turbo_mutex_destroy(&completion.mutex);
  }

  it("atomically commits facts and outbox intents under one fencing token") {
    static const uint8_t key[] = "client-a";
    static const uint8_t key_b[] = "client-b";
    static const uint8_t value[] = "session-v1";
    static const uint8_t value_b[] = "session-v2";
    static const uint8_t changed[] = "different-command";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    flowie_cluster_pgsql_config_t coordinator_config;
    flowie_cluster_pgsql_fact_config_t fact_config;
    flowie_cluster_pgsql_coordinator_t *coordinator = NULL;
    flowie_cluster_pgsql_fact_store_t *store = NULL;
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
    flowie_cluster_pgsql_fact_mutation_t mutation_b = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
    flowie_cluster_pgsql_fact_command_t command;
    flowie_cluster_pgsql_fact_command_t command_b;
    flowie_cluster_pgsql_event_dedupe_t dedupe = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
    flowie_cluster_pgsql_event_dedupe_t shard_ack = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
    flowie_cluster_pgsql_event_dedupe_t shared_candidate =
        FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
    flowie_cluster_pgsql_outbox_event_t outbox = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    flowie_cluster_pgsql_outbox_event_t replay = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    flowie_cluster_pgsql_outbox_event_t empty = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    flowie_cluster_pgsql_fact_scan_t scan = FLOWIE_CLUSTER_PGSQL_FACT_SCAN_INIT;
    flowie_cluster_pgsql_live_scan_t scanned = {0};
    uint64_t deadline = 0u;
    size_t ack_count = 0u;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-fact-%llu",
                   (unsigned long long)turbo_hrtime());
    coordinator_config = flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-fact", 4u, 1);
    fact_config = flowie_cluster_pgsql_live_fact_config(&coordinator_config);
    check_int_eq(flowie_cluster_pgsql_coordinator_open(&coordinator_config, &coordinator),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_fact_store_open(&fact_config, &store), TURBO_OK);
    check_int_eq(
        flowie_cluster_pgsql_shard_claim(coordinator, 0u, turbo_hrtime(), &owner, &deadline),
        TURBO_OK);
    scan.owner = owner;
    scan.max_records = 2u;
    check_int_eq(flowie_cluster_pgsql_fact_scan(store, &scan, flowie_cluster_pgsql_live_scan_visit,
                                                &scanned),
                 TURBO_OK);
    check_size_eq(scanned.count, 0u);
    command = flowie_cluster_pgsql_live_fact_command(&owner, 1u, key, sizeof(key) - 1u, value,
                                                     sizeof(value) - 1u, 0u, 1u, &mutation);
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_fact_scan(store, &scan, flowie_cluster_pgsql_live_scan_visit,
                                                &scanned),
                 TURBO_OK);
    check_size_eq(scanned.count, 1u);
    check_str_eq(scanned.key, "client-a");
    check_str_eq(scanned.value, "session-v1");
    check_uint_eq(scanned.revision, 1u);
    check_int_eq(flowie_cluster_pgsql_fact_confirm(store, &command), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command), TURBO_EALREADY);
    check_int_eq(flowie_cluster_pgsql_outbox_next(store, &owner, 1u, &outbox), TURBO_OK);
    check(outbox.created_at_epoch_seconds != 0u);
    check_mem_eq(outbox.command_id, command.command_id, sizeof(outbox.command_id));
    check_int_eq(outbox.event_index, 0u);
    check_int_eq(outbox.shard_id, owner.shard_id);
    check_uint_eq(outbox.event_owner_epoch, owner.owner_epoch);
    check_uint_eq(outbox.fact_revision, 1u);
    check_uint_eq(outbox.event_type, 1u);
    check_int_eq(outbox.record_kind, FLOWIE_CLUSTER_KEY_SESSION);
    check_size_eq(tstr_len(outbox.record_key), sizeof(key) - 1u);
    check_mem_eq(outbox.record_key, key, sizeof(key) - 1u);
    check_size_eq(tstr_len(outbox.payload), sizeof(value) - 1u);
    check_mem_eq(outbox.payload, value, sizeof(value) - 1u);
    check_uint_eq(outbox.attempt_count, 1u);
    check_int_eq(flowie_cluster_pgsql_outbox_next(store, &owner, 1u, &replay), TURBO_OK);
    check_mem_eq(replay.command_id, outbox.command_id, sizeof(replay.command_id));
    check_uint_eq(replay.attempt_count, 2u);
    check_int_eq(flowie_cluster_pgsql_outbox_settle(store, &owner, &replay), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_outbox_settle(store, &owner, &replay), TURBO_EALREADY);
    check_int_eq(flowie_cluster_pgsql_outbox_next(store, &owner, 1u, &empty), TURBO_ENOENT);
    mutation.event_payload = changed;
    mutation.event_payload_size = sizeof(changed) - 1u;
    check_int_eq(flowie_cluster_pgsql_fact_confirm(store, &command), TURBO_EBUSY);
    mutation.event_payload = value;
    mutation.event_payload_size = sizeof(value) - 1u;
    command.command_id[0] = 2u;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command), TURBO_EBUSY);
    command_b = flowie_cluster_pgsql_live_fact_command(
        &owner, 4u, key_b, sizeof(key_b) - 1u, value_b, sizeof(value_b) - 1u, 0u, 1u, &mutation_b);
    dedupe.source_command_id[0] = 9u;
    dedupe.source_shard_id = owner.shard_id;
    dedupe.source_owner_epoch = owner.owner_epoch;
    dedupe.source_fact_revision = 7u;
    dedupe.target_session_id = 11u;
    dedupe.event_digest[0] = 8u;
    command_b.dedupe = &dedupe;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command_b), TURBO_OK);
    command_b.command_id[0] = 5u;
    mutation_b.record.expected_revision = 1u;
    mutation_b.record.next_revision = 2u;
    mutation_b.record.value = changed;
    mutation_b.record.value_size = sizeof(changed) - 1u;
    mutation_b.event_payload = changed;
    mutation_b.event_payload_size = sizeof(changed) - 1u;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command_b), TURBO_EALREADY);
    check_int_eq(flowie_cluster_pgsql_fact_confirm(store, &command_b), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_outbox_next(store, &owner, 1u, &empty), TURBO_OK);
    check_uint_eq(empty.fact_revision, 1u);
    check_size_eq(tstr_len(empty.payload), sizeof(value_b) - 1u);
    check_mem_eq(empty.payload, value_b, sizeof(value_b) - 1u);
    check_int_eq(flowie_cluster_pgsql_outbox_settle(store, &owner, &empty), TURBO_OK);
    flowie_cluster_pgsql_outbox_event_cleanup(&empty);
    check_int_eq(flowie_cluster_pgsql_outbox_next(store, &owner, 1u, &empty), TURBO_ENOENT);
    shard_ack = dedupe;
    shard_ack.target_session_id = 0u;
    command_b.command_id[0] = 6u;
    command_b.mutations = NULL;
    command_b.mutation_count = 0u;
    command_b.dedupe = &shard_ack;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command_b), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_event_ack_count(store, &shard_ack, &ack_count), TURBO_OK);
    check_size_eq(ack_count, 1u);
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command_b), TURBO_EALREADY);
    shared_candidate = dedupe;
    memset(shared_candidate.source_command_id, 0, sizeof(shared_candidate.source_command_id));
    shared_candidate.source_command_id[0] = 10u;
    shared_candidate.target_session_id = 21u;
    shared_candidate.shared_filter = (const uint8_t *)"$share/g/a/#";
    shared_candidate.shared_filter_size = sizeof("$share/g/a/#") - 1u;
    command_b.command_id[0] = 7u;
    command_b.dedupe = &shared_candidate;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command_b), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_fact_confirm(store, &command_b), TURBO_OK);
    command_b.command_id[0] = 8u;
    shared_candidate.target_session_id = 22u;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command_b), TURBO_EALREADY);
    check_int_eq(flowie_cluster_pgsql_fact_confirm(store, &command_b), TURBO_EALREADY);
    scan.max_records = 1u;
    check_int_eq(flowie_cluster_pgsql_fact_scan(store, &scan, flowie_cluster_pgsql_live_scan_visit,
                                                &scanned),
                 TURBO_ENOSPC);
    check_int_eq(flowie_cluster_pgsql_shard_release(coordinator, &owner), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_outbox_next(store, &owner, 1u, &empty), TURBO_EBUSY);
    check_int_eq(flowie_cluster_pgsql_fact_scan(store, &scan, flowie_cluster_pgsql_live_scan_visit,
                                                &scanned),
                 TURBO_EBUSY);
    command.command_id[0] = 3u;
    mutation.record.expected_revision = 1u;
    mutation.record.next_revision = 2u;
    check_int_eq(flowie_cluster_pgsql_fact_commit(store, &command), TURBO_EBUSY);
    flowie_cluster_pgsql_fact_store_destroy(store);
    flowie_cluster_pgsql_coordinator_destroy(coordinator);
    flowie_cluster_pgsql_outbox_event_cleanup(&outbox);
    flowie_cluster_pgsql_outbox_event_cleanup(&replay);
    flowie_cluster_pgsql_outbox_event_cleanup(&empty);
    tstr_free(scanned.key);
    tstr_free(scanned.value);
  }

  it("deep-copies and drains accepted fact commands on its database worker") {
    static const uint8_t key[] = "client-worker";
    static const uint8_t value[] = "worker-session";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char cluster_id[128];
    flowie_cluster_pgsql_config_t coordinator_config;
    flowie_cluster_pgsql_fact_config_t fact_config;
    flowie_cluster_pgsql_fact_worker_config_t worker_config =
        FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
    flowie_cluster_pgsql_coordinator_t *coordinator = NULL;
    flowie_cluster_pgsql_fact_worker_t *worker = NULL;
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
    flowie_cluster_pgsql_fact_command_t command;
    flowie_cluster_pgsql_live_completion_t completion;
    uint64_t deadline = 0u;
    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    memset(&completion, 0, sizeof(completion));
    turbo_mutex_init(&completion.mutex);
    turbo_cond_init(&completion.changed);
    (void)snprintf(cluster_id, sizeof(cluster_id), "flowie-fact-worker-%llu",
                   (unsigned long long)turbo_hrtime());
    coordinator_config =
        flowie_cluster_pgsql_live_config(conninfo, cluster_id, "node-fact-worker", 5u, 1);
    fact_config = flowie_cluster_pgsql_live_fact_config(&coordinator_config);
    worker_config.fact = &fact_config;
    worker_config.max_queue_entries = 4u;
    worker_config.max_queue_bytes = 1024u * 1024u;
    check_int_eq(flowie_cluster_pgsql_coordinator_open(&coordinator_config, &coordinator),
                 TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_fact_worker_create(&worker_config, &worker), TURBO_OK);
    check_int_eq(
        flowie_cluster_pgsql_shard_claim(coordinator, 0u, turbo_hrtime(), &owner, &deadline),
        TURBO_OK);
    command = flowie_cluster_pgsql_live_fact_command(&owner, 1u, key, sizeof(key) - 1u, value,
                                                     sizeof(value) - 1u, 0u, 1u, &mutation);
    check_int_eq(flowie_cluster_pgsql_fact_worker_submit(
                     worker, &command, flowie_cluster_pgsql_live_complete, &completion),
                 TURBO_OK);
    memset(&command, 0, sizeof(command));
    memset(&mutation, 0, sizeof(mutation));
    turbo_mutex_lock(&completion.mutex);
    while (!completion.done)
      turbo_cond_wait(&completion.changed, &completion.mutex);
    turbo_mutex_unlock(&completion.mutex);
    check_int_eq(completion.status, TURBO_OK);
    check_int_eq(completion.command_id[0], 1u);
    check_int_eq(flowie_cluster_pgsql_fact_worker_close(worker), TURBO_OK);
    check_int_eq(flowie_cluster_pgsql_shard_release(coordinator, &owner), TURBO_OK);
    flowie_cluster_pgsql_fact_worker_destroy(worker);
    flowie_cluster_pgsql_coordinator_destroy(coordinator);
    turbo_cond_destroy(&completion.changed);
    turbo_mutex_destroy(&completion.mutex);
  }
}
