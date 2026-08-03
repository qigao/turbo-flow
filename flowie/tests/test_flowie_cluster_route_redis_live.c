#include "flowie_cluster_route_store_internal.h"

#include "tinytest.h"
#include "turbo_flow_redis.h"
#include "turbo_thread.h"

#include <stdio.h>
#include <string.h>

static flowie_cluster_route_projection_t
flowie_cluster_route_redis_live_projection(uint64_t fact_revision, int active) {
  flowie_cluster_route_projection_t projection = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
  size_t index;
  projection.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)"redis-device-a", sizeof("redis-device-a") - 1u};
  projection.edge_node_id = tstr_v_from_cstr("edge-a");
  for (index = 0u; index < sizeof(projection.edge_boot_id); ++index)
    projection.edge_boot_id[index] = (uint8_t)(index + 1u);
  projection.session_shard = 3u;
  projection.owner_epoch = 17u;
  projection.fact_revision = fact_revision;
  projection.connection_id = 71u;
  projection.connection_generation = 9u;
  projection.session_generation = 101u;
  projection.active = (uint8_t)active;
  if (active) {
    projection.advertised_endpoint = tstr_v_from_cstr("10.0.0.7:1883");
    projection.lease_deadline_epoch_ms = UINT64_C(4102444800000);
  }
  return projection;
}

spec("flowie cluster route Redis live") {
  it("persists active routes and newer tombstones in a Redis Hash") {
    turbo_flow_redis_record_store_config_t redis_config;
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    flowie_cluster_route_store_config_t route_config = FLOWIE_CLUSTER_ROUTE_STORE_CONFIG_INIT;
    flowie_cluster_route_projection_t projection =
        flowie_cluster_route_redis_live_projection(7u, 1);
    flowie_cluster_route_project_result_t project_result = 0;
    flowie_cluster_route_record_t resolved = FLOWIE_CLUSTER_ROUTE_RECORD_INIT;
    turbo_flow_state_record_t stored = TURBO_FLOW_STATE_RECORD_INIT;
    turbo_flow_state_store_t *state = NULL;
    flowie_cluster_route_store_t *routes = NULL;
    turbo_flow_store_bytes_t client_key = {
        (const uint8_t *)"redis-device-a", sizeof("redis-device-a") - 1u};
    char redis_key[128];

    (void)snprintf(redis_key, sizeof(redis_key), "flowie:route:{live-%llu}",
                   (unsigned long long)turbo_hrtime());
    memset(&redis_config, 0, sizeof(redis_config));
    redis_config.host = "127.0.0.1";
    redis_config.port = 6379u;
    redis_config.database = 0;
    redis_config.timeout_ms = 5000u;
    redis_config.key = redis_key;
    redis_config.max_record_key_size = 128u;
    redis_config.max_value_size = 1024u;
    redis_config.max_batch_size = 16u;
    redis_config.max_records = 16u;
    limits.max_records = 16u;
    limits.max_bytes = 16384u;
    limits.max_item_bytes = 1024u;
    limits.full_policy = TURBO_FLOW_STORE_FULL_REJECT;

    check_int_eq(turbo_flow_redis_state_store_create(&redis_config, &limits, &state), TURBO_OK);
    route_config.state = state;
    route_config.max_client_id_size = 128u;
    route_config.max_endpoint_size = 256u;
    route_config.max_cas_attempts = 4u;
    check_int_eq(flowie_cluster_route_store_create(&route_config, &routes), TURBO_OK);

    check_int_eq(flowie_cluster_route_store_project(routes, &projection, &project_result),
                 TURBO_OK);
    check_int_eq(project_result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
    check_int_eq(flowie_cluster_route_store_resolve(
                     routes, projection.client_id, UINT64_C(2000000000000), &resolved),
                 TURBO_OK);
    check_str_eq(resolved.edge_node_id, "edge-a");
    check_str_eq(resolved.advertised_endpoint, "10.0.0.7:1883");
    check_uint_eq(resolved.fact_revision, 7u);
    flowie_cluster_route_record_cleanup(&resolved);

    projection.fact_revision = 8u;
    projection.active = 0u;
    projection.advertised_endpoint = (tstr_v){NULL, 0u};
    projection.lease_deadline_epoch_ms = 0u;
    check_int_eq(flowie_cluster_route_store_project(routes, &projection, &project_result),
                 TURBO_OK);
    check_int_eq(project_result, FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED);
    check_int_eq(flowie_cluster_route_store_resolve(
                     routes, projection.client_id, UINT64_C(2000000000000), &resolved),
                 TURBO_ENOENT);

    check_int_eq(turbo_flow_state_store_get(state, client_key, &stored), TURBO_OK);
    check_int_eq(turbo_flow_state_store_remove(state, client_key, stored.revision), TURBO_OK);
    turbo_flow_state_record_cleanup(&stored);
    flowie_cluster_route_store_destroy(routes);
    check_int_eq(turbo_flow_state_store_close(state), TURBO_OK);
    turbo_flow_state_store_destroy(state);
  }
}
