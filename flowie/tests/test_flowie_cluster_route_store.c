#include "flowie_cluster_route_store_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static void flowie_cluster_route_test_boot(uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                           uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot[index] = (uint8_t)(seed + index);
}

static turbo_flow_store_limits_t flowie_cluster_route_test_limits(void) {
  turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
  limits.max_records = 16u;
  limits.max_bytes = 16384u;
  limits.max_item_bytes = 1024u;
  limits.full_policy = TURBO_FLOW_STORE_FULL_REJECT;
  return limits;
}

static flowie_cluster_route_projection_t flowie_cluster_route_test_projection(
    const char *client_id, uint64_t owner_epoch, uint64_t fact_revision, int active) {
  flowie_cluster_route_projection_t projection = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
  projection.client_id = (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
  projection.edge_node_id = tstr_v_from_cstr("edge-a");
  flowie_cluster_route_test_boot(projection.edge_boot_id, 11u);
  projection.session_shard = 3u;
  projection.owner_epoch = owner_epoch;
  projection.fact_revision = fact_revision;
  projection.connection_id = 71u;
  projection.connection_generation = 9u;
  projection.session_generation = 101u;
  projection.active = (uint8_t)active;
  if (active) {
    projection.advertised_endpoint = tstr_v_from_cstr("10.0.0.7:1883");
    projection.lease_deadline_epoch_ms = 5000u;
  }
  return projection;
}

static flowie_cluster_route_store_t *flowie_cluster_route_test_create(
    turbo_flow_state_store_t **state_out) {
  turbo_flow_store_limits_t limits = flowie_cluster_route_test_limits();
  flowie_cluster_route_store_config_t config = FLOWIE_CLUSTER_ROUTE_STORE_CONFIG_INIT;
  flowie_cluster_route_store_t *store = NULL;
  check_int_eq(turbo_flow_state_store_create_memory(&limits, state_out), TURBO_OK);
  config.state = *state_out;
  config.max_client_id_size = 128u;
  config.max_endpoint_size = 256u;
  config.max_cas_attempts = 3u;
  check_int_eq(flowie_cluster_route_store_create(&config, &store), TURBO_OK);
  return store;
}

spec("flowie cluster route store") {
  it("orders routes by owner epoch and fact revision") {
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *store = flowie_cluster_route_test_create(&state);
    flowie_cluster_route_projection_t route =
        flowie_cluster_route_test_projection("device-a", 2u, 7u, 1);
    flowie_cluster_route_project_result_t result;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;

    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
    route.fact_revision = 6u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_STALE);
    route.fact_revision = 7u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_UNCHANGED);
    route.owner_epoch = 3u;
    route.fact_revision = 1u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
    check_int_eq(flowie_cluster_route_store_resolve(
                     store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 1000u,
                     &resolved),
                 TURBO_OK);
    check_uint_eq(resolved.owner_epoch, 3u);
    check_uint_eq(resolved.fact_revision, 1u);
    check_str_eq(resolved.edge_node_id, "edge-a");
    check_str_eq(resolved.advertised_endpoint, "10.0.0.7:1883");
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_store_destroy(store);
    (void)turbo_flow_state_store_close(state);
    turbo_flow_state_store_destroy(state);
  }

  it("keeps disconnect tombstones from allowing delayed resurrection") {
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *store = flowie_cluster_route_test_create(&state);
    flowie_cluster_route_projection_t route =
        flowie_cluster_route_test_projection("device-a", 2u, 7u, 1);
    flowie_cluster_route_project_result_t result;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;

    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    route.active = 0u;
    route.advertised_endpoint = (tstr_v){NULL, 0u};
    route.lease_deadline_epoch_ms = 0u;
    route.fact_revision = 8u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
    route.active = 1u;
    route.advertised_endpoint = tstr_v_from_cstr("10.0.0.7:1883");
    route.lease_deadline_epoch_ms = 5000u;
    route.fact_revision = 7u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_STALE);
    check_int_eq(flowie_cluster_route_store_resolve(
                     store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 1000u,
                     &resolved),
                 TURBO_ENOENT);
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_store_destroy(store);
    (void)turbo_flow_state_store_close(state);
    turbo_flow_state_store_destroy(state);
  }

  it("refreshes member-derived fields for an unchanged session version") {
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *store = flowie_cluster_route_test_create(&state);
    flowie_cluster_route_projection_t route =
        flowie_cluster_route_test_projection("device-a", 2u, 7u, 1);
    flowie_cluster_route_project_result_t result;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;

    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    route.advertised_endpoint = tstr_v_from_cstr("10.0.0.8:1883");
    route.lease_deadline_epoch_ms = 7000u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
    check_int_eq(flowie_cluster_route_store_resolve(
                     store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 6000u,
                     &resolved),
                 TURBO_OK);
    check_str_eq(resolved.advertised_endpoint, "10.0.0.8:1883");
    check_uint_eq(resolved.lease_deadline_epoch_ms, 7000u);
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_store_destroy(store);
    (void)turbo_flow_state_store_close(state);
    turbo_flow_state_store_destroy(state);
  }

  it("copies a bounded stable snapshot without retaining StateStore views") {
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *store = flowie_cluster_route_test_create(&state);
    flowie_cluster_route_projection_t first =
        flowie_cluster_route_test_projection("device-a", 2u, 7u, 1);
    flowie_cluster_route_projection_t second =
        flowie_cluster_route_test_projection("device-b", 3u, 1u, 0);
    flowie_cluster_route_projection_t copied = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
    flowie_cluster_route_snapshot_t *snapshot = NULL;
    flowie_cluster_route_project_result_t result;

    check_int_eq(flowie_cluster_route_store_project(store, &first, &result), TURBO_OK);
    check_int_eq(flowie_cluster_route_store_project(store, &second, &result), TURBO_OK);
    check_int_eq(flowie_cluster_route_store_snapshot_create(store, &snapshot), TURBO_OK);
    check_not_null(snapshot);
    check_size_eq(flowie_cluster_route_snapshot_count(snapshot), 2u);
    check_int_eq(flowie_cluster_route_snapshot_at(snapshot, 0u, &copied), TURBO_OK);
    check_true(copied.client_id.size == 8u);
    check_true((memcmp(copied.client_id.data, "device-a", 8u) == 0) ||
               (memcmp(copied.client_id.data, "device-b", 8u) == 0));
    check_int_eq(flowie_cluster_route_snapshot_at(snapshot, 2u, &copied), TURBO_EINVAL);
    flowie_cluster_route_snapshot_destroy(snapshot);
    flowie_cluster_route_store_destroy(store);
    (void)turbo_flow_state_store_close(state);
    turbo_flow_state_store_destroy(state);
  }

  it("returns expired routes as absent and rejects equal-version identity divergence") {
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *store = flowie_cluster_route_test_create(&state);
    flowie_cluster_route_projection_t route =
        flowie_cluster_route_test_projection("device-a", 2u, 7u, 1);
    flowie_cluster_route_project_result_t result;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;

    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_OK);
    check_int_eq(flowie_cluster_route_store_resolve(
                     store, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, 5000u,
                     &resolved),
                 TURBO_ENOENT);
    route.edge_node_id = tstr_v_from_cstr("edge-b");
    route.lease_deadline_epoch_ms = 7000u;
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_EPROTO);
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_store_destroy(store);
    (void)turbo_flow_state_store_close(state);
    turbo_flow_state_store_destroy(state);
  }

  it("fails fast without overwriting a malformed Redis projection") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t malformed[] = "not-a-route";
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *store = flowie_cluster_route_test_create(&state);
    flowie_cluster_route_projection_t route =
        flowie_cluster_route_test_projection("device-a", 2u, 7u, 1);
    flowie_cluster_route_project_result_t result;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;
    uint64_t revision = 0u;

    check_int_eq(turbo_flow_state_store_put(
                     state, (turbo_flow_store_bytes_t){client_id, sizeof(client_id) - 1u},
                     (turbo_flow_store_bytes_t){malformed, sizeof(malformed) - 1u}, 0u, &revision),
                 TURBO_OK);
    check_int_eq(flowie_cluster_route_store_resolve(
                     store, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, 1000u,
                     &resolved),
                 TURBO_EPROTO);
    check_int_eq(flowie_cluster_route_store_project(store, &route, &result), TURBO_EPROTO);
    flowie_cluster_route_record_cleanup(&resolved);
    flowie_cluster_route_store_destroy(store);
    (void)turbo_flow_state_store_close(state);
    turbo_flow_state_store_destroy(state);
  }
}
