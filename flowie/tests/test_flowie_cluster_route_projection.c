#include "flowie_cluster_route_projection_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

typedef struct flowie_cluster_route_projection_test_s {
  turbo_flow_state_store_t *state;
  flowie_cluster_route_store_t *store;
  flowie_cluster_route_projector_t *projector;
  flowie_cluster_pgsql_member_t member;
  int resolve_status;
  size_t resolve_calls;
} flowie_cluster_route_projection_test_t;

static int flowie_cluster_route_projection_test_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  (void)ctx;
  (void)command;
  (void)completion;
  (void)completion_ctx;
  return TURBO_EIO;
}

static uint64_t flowie_cluster_route_projection_test_now(void *ctx) {
  (void)ctx;
  return 1u;
}

static void flowie_cluster_route_projection_test_fence(void *ctx, int reason) {
  (void)ctx;
  (void)reason;
}

static int flowie_cluster_route_projection_test_fact_get(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_key_kind_t key_kind, const uint8_t *key, size_t key_size,
    flowie_cluster_pgsql_fact_record_t *out) {
  (void)ctx;
  (void)current_owner;
  (void)key_kind;
  (void)key;
  (void)key_size;
  (void)out;
  return TURBO_EIO;
}

static int flowie_cluster_route_projection_test_member_resolve(
    void *ctx, tstr_v node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    flowie_cluster_pgsql_member_t *out) {
  flowie_cluster_route_projection_test_t *test =
      (flowie_cluster_route_projection_test_t *)ctx;
  ++test->resolve_calls;
  if (test->resolve_status != TURBO_OK) return test->resolve_status;
  if (node_id.len != test->member.node_id_size ||
      memcmp(node_id.data, test->member.node_id, node_id.len) != 0 ||
      memcmp(boot_id, test->member.boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
    return TURBO_ENOENT;
  *out = test->member;
  return TURBO_OK;
}

static void flowie_cluster_route_projection_test_boot(
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  for (size_t index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(11u + index);
}

static flowie_cluster_session_bind_config_t
flowie_cluster_route_projection_test_session_config(void) {
  flowie_cluster_session_bind_config_t config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  config.max_sessions = 4u;
  config.max_bind_payload_size = 512u;
  config.max_fact_value_size = 4096u;
  config.max_event_payload_size = 512u;
  config.session.owner_instance_id = 1u;
  config.session.max_subscriptions = 4u;
  config.session.max_inflight = 4u;
  config.first_session_id = 1u;
  config.submit = flowie_cluster_route_projection_test_submit;
  config.now = flowie_cluster_route_projection_test_now;
  config.self_fence = flowie_cluster_route_projection_test_fence;
  return config;
}

static void flowie_cluster_route_projection_test_member_set(
    flowie_cluster_route_projection_test_t *test, flowie_cluster_node_state_t state,
    const char *endpoint, uint64_t lease_deadline_epoch_ms) {
  size_t node_id_size = strlen("edge-a");
  size_t endpoint_size = endpoint ? strlen(endpoint) : 0u;
  test->member = (flowie_cluster_pgsql_member_t)FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
  memcpy(test->member.node_id, "edge-a", node_id_size + 1u);
  test->member.node_id_size = node_id_size;
  flowie_cluster_route_projection_test_boot(test->member.boot_id);
  test->member.state = state;
  if (endpoint_size != 0u) {
    memcpy(test->member.advertised_endpoint, endpoint, endpoint_size + 1u);
    test->member.advertised_endpoint_size = endpoint_size;
  }
  test->member.lease_deadline_epoch_ms = lease_deadline_epoch_ms;
  test->member.revision = 1u;
}

static flowie_cluster_route_projection_t flowie_cluster_route_projection_test_route(
    const char *client_id, int active) {
  flowie_cluster_route_projection_t route = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
  route.client_id = (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
  route.edge_node_id = tstr_v_from_cstr("edge-a");
  flowie_cluster_route_projection_test_boot(route.edge_boot_id);
  route.session_shard = 3u;
  route.owner_epoch = 2u;
  route.fact_revision = active ? 7u : 8u;
  route.connection_id = 71u;
  route.connection_generation = 9u;
  route.session_generation = 101u;
  route.active = (uint8_t)active;
  if (active) {
    route.advertised_endpoint = tstr_v_from_cstr("10.0.0.7:1883");
    route.lease_deadline_epoch_ms = 5000u;
  }
  return route;
}

static void flowie_cluster_route_projection_test_init(
    flowie_cluster_route_projection_test_t *test) {
  turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
  flowie_cluster_route_store_config_t store_config = FLOWIE_CLUSTER_ROUTE_STORE_CONFIG_INIT;
  flowie_cluster_route_projector_config_t projector_config =
      FLOWIE_CLUSTER_ROUTE_PROJECTOR_CONFIG_INIT;
  flowie_cluster_session_bind_config_t session_config =
      flowie_cluster_route_projection_test_session_config();
  memset(test, 0, sizeof(*test));
  limits.max_records = 16u;
  limits.max_bytes = 16384u;
  limits.max_item_bytes = 1024u;
  limits.full_policy = TURBO_FLOW_STORE_FULL_REJECT;
  check_int_eq(turbo_flow_state_store_create_memory(&limits, &test->state), TURBO_OK);
  store_config.state = test->state;
  store_config.max_client_id_size = 128u;
  store_config.max_endpoint_size = 256u;
  store_config.max_cas_attempts = 3u;
  check_int_eq(flowie_cluster_route_store_create(&store_config, &test->store), TURBO_OK);
  projector_config.session = &session_config;
  projector_config.route_store = test->store;
  projector_config.fact_get = flowie_cluster_route_projection_test_fact_get;
  projector_config.member_resolve = flowie_cluster_route_projection_test_member_resolve;
  projector_config.member_resolve_ctx = test;
  check_int_eq(flowie_cluster_route_projector_create(&projector_config, &test->projector),
               TURBO_OK);
  test->resolve_status = TURBO_OK;
  flowie_cluster_route_projection_test_member_set(test, FLOWIE_CLUSTER_NODE_READY,
                                                  "10.0.0.8:1883", 7000u);
}

static void flowie_cluster_route_projection_test_cleanup(
    flowie_cluster_route_projection_test_t *test) {
  flowie_cluster_route_projector_destroy(test->projector);
  flowie_cluster_route_store_destroy(test->store);
  (void)turbo_flow_state_store_close(test->state);
  turbo_flow_state_store_destroy(test->state);
}

static void flowie_cluster_route_projection_test_seed(
    flowie_cluster_route_projection_test_t *test, const char *client_id, int active) {
  flowie_cluster_route_projection_t route =
      flowie_cluster_route_projection_test_route(client_id, active);
  flowie_cluster_route_project_result_t result = 0;
  check_int_eq(flowie_cluster_route_store_project(test->store, &route, &result), TURBO_OK);
  check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
}

spec("flowie cluster route projection reconciliation") {
  it("keeps an unchanged active session routable across member lease renewals") {
    flowie_cluster_route_projection_test_t test;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;
    size_t refreshed = 0u;
    flowie_cluster_route_projection_test_init(&test);
    flowie_cluster_route_projection_test_seed(&test, "device-a", 1);

    check_int_eq(flowie_cluster_route_projector_reconcile(test.projector, &refreshed), TURBO_OK);
    check_size_eq(refreshed, 1u);
    check_int_eq(flowie_cluster_route_store_resolve(
                     test.store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 6000u,
                     &resolved),
                 TURBO_OK);
    check_str_eq(resolved.advertised_endpoint, "10.0.0.8:1883");
    check_uint_eq(resolved.lease_deadline_epoch_ms, 7000u);
    flowie_cluster_route_record_cleanup(&resolved);

    flowie_cluster_route_projection_test_member_set(&test, FLOWIE_CLUSTER_NODE_DRAINING,
                                                    "10.0.0.9:1883", 9000u);
    check_int_eq(flowie_cluster_route_projector_reconcile(test.projector, &refreshed), TURBO_OK);
    check_size_eq(refreshed, 1u);
    check_int_eq(flowie_cluster_route_store_resolve(
                     test.store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 8000u,
                     &resolved),
                 TURBO_OK);
    check_str_eq(resolved.advertised_endpoint, "10.0.0.9:1883");
    check_uint_eq(resolved.lease_deadline_epoch_ms, 9000u);
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_projection_test_cleanup(&test);
  }

  it("leaves missing and non-routable members to expire") {
    flowie_cluster_route_projection_test_t test;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;
    size_t refreshed = 99u;
    flowie_cluster_route_projection_test_init(&test);
    flowie_cluster_route_projection_test_seed(&test, "device-a", 1);

    flowie_cluster_route_projection_test_member_set(&test, FLOWIE_CLUSTER_NODE_OFFLINE, NULL, 0u);
    check_int_eq(flowie_cluster_route_projector_reconcile(test.projector, &refreshed), TURBO_OK);
    check_size_eq(refreshed, 0u);
    check_int_eq(flowie_cluster_route_store_resolve(
                     test.store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 5000u,
                     &resolved),
                 TURBO_ENOENT);

    test.resolve_status = TURBO_ENOENT;
    check_int_eq(flowie_cluster_route_projector_reconcile(test.projector, &refreshed), TURBO_OK);
    check_size_eq(refreshed, 0u);
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_projection_test_cleanup(&test);
  }

  it("propagates member directory failures without reporting partial success") {
    flowie_cluster_route_projection_test_t test;
    size_t refreshed = 99u;
    flowie_cluster_route_projection_test_init(&test);
    flowie_cluster_route_projection_test_seed(&test, "device-a", 1);
    test.resolve_status = TURBO_EIO;

    check_int_eq(flowie_cluster_route_projector_reconcile(test.projector, &refreshed), TURBO_EIO);
    check_size_eq(refreshed, 0u);
    check_size_eq(test.resolve_calls, 1u);
    flowie_cluster_route_projection_test_cleanup(&test);
  }

  it("does not resolve members for tombstones") {
    flowie_cluster_route_projection_test_t test;
    size_t refreshed = 99u;
    flowie_cluster_route_projection_test_init(&test);
    flowie_cluster_route_projection_test_seed(&test, "device-a", 0);
    test.resolve_status = TURBO_EIO;

    check_int_eq(flowie_cluster_route_projector_reconcile(test.projector, &refreshed), TURBO_OK);
    check_size_eq(refreshed, 0u);
    check_size_eq(test.resolve_calls, 0u);
    flowie_cluster_route_projection_test_cleanup(&test);
  }
}
