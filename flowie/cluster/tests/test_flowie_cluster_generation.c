#include "flowie_cluster_generation_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

enum {
  FLOWIE_CLUSTER_GENERATION_TEST_EVENT_MAX = 32u,
  FLOWIE_CLUSTER_GENERATION_TEST_TIMEOUT_NS = 1000000000u
};

typedef enum flowie_cluster_generation_test_event_e {
  FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CREATE = 1,
  FLOWIE_CLUSTER_GENERATION_TEST_NODE_CREATE,
  FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CREATE,
  FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_START,
  FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_READY,
  FLOWIE_CLUSTER_GENERATION_TEST_NODE_START,
  FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CLOSE,
  FLOWIE_CLUSTER_GENERATION_TEST_NODE_CLOSE,
  FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CLOSE,
  FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_DESTROY,
  FLOWIE_CLUSTER_GENERATION_TEST_NODE_DESTROY,
  FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_DESTROY
} flowie_cluster_generation_test_event_t;

typedef struct flowie_cluster_generation_test_s {
  int events[FLOWIE_CLUSTER_GENERATION_TEST_EVENT_MAX];
  size_t event_count;
  int owners_ready_result;
  int node_start_result;
  int endpoint_close_results[2];
  size_t endpoint_close_count;
  tf_coronet_execution_t *injected_execution;
  unsigned char owners_directory_storage;
  flowie_endpoint_cluster_binding_t endpoint_port;
} flowie_cluster_generation_test_t;

static flowie_cluster_generation_test_t *flowie_cluster_generation_active_test;

static void flowie_cluster_generation_test_record(
    flowie_cluster_generation_test_event_t event) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  if (test && test->event_count < FLOWIE_CLUSTER_GENERATION_TEST_EVENT_MAX)
    test->events[test->event_count++] = event;
}

static int flowie_cluster_generation_test_owners_create(
    const flowie_cluster_owner_directory_runtime_config_t *config,
    flowie_cluster_owner_directory_runtime_t **out) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  if (!test || !config || !out) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CREATE);
  *out = (flowie_cluster_owner_directory_runtime_t *)test;
  return TURBO_OK;
}

static int flowie_cluster_generation_test_owners_start(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  if (!runtime) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_START);
  return TURBO_OK;
}

static int flowie_cluster_generation_test_owners_ready(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  if (!runtime || !test || timeout_ns == 0u) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_READY);
  return test->owners_ready_result;
}

static flowie_cluster_owner_directory_t *flowie_cluster_generation_test_owners_directory(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  return runtime && test
             ? (flowie_cluster_owner_directory_t *)&test->owners_directory_storage
             : NULL;
}

static int flowie_cluster_generation_test_owners_close(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns) {
  if (!runtime || timeout_ns == 0u) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CLOSE);
  return TURBO_OK;
}

static void flowie_cluster_generation_test_owners_destroy(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  if (runtime)
    flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_DESTROY);
}

static int flowie_cluster_generation_test_node_create(
    const flowie_cluster_node_config_t *config, flowie_cluster_node_t **out) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  if (!test || !config || !out || !config->listener.execution ||
      (config->local_shard_count != 0u && !config->local_shards[0].execution) ||
      (config->connector_count != 0u && !config->connectors[0].execution))
    return TURBO_EINVAL;
  test->injected_execution = config->listener.execution;
  if (config->local_shard_count != 0u &&
      config->local_shards[0].execution != test->injected_execution)
    return TURBO_EINVAL;
  if (config->connector_count != 0u &&
      config->connectors[0].execution != test->injected_execution)
    return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_NODE_CREATE);
  *out = (flowie_cluster_node_t *)test;
  return TURBO_OK;
}

static int flowie_cluster_generation_test_node_start(flowie_cluster_node_t *node,
                                                     uint64_t timeout_ns) {
  if (!node || timeout_ns == 0u) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_NODE_START);
  return flowie_cluster_generation_active_test->node_start_result;
}

static int flowie_cluster_generation_test_node_close(flowie_cluster_node_t *node,
                                                     uint64_t timeout_ns) {
  if (!node || timeout_ns == 0u) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_NODE_CLOSE);
  return TURBO_OK;
}

static int flowie_cluster_generation_test_node_destroy(flowie_cluster_node_t *node) {
  if (!node) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_NODE_DESTROY);
  return TURBO_OK;
}

static int flowie_cluster_generation_test_endpoint_create(
    const flowie_cluster_endpoint_binding_config_t *config,
    flowie_cluster_endpoint_binding_t **out) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  if (!test || !config || !out || config->execution != test->injected_execution ||
      config->node != (flowie_cluster_node_t *)test ||
      config->owners !=
          (flowie_cluster_owner_directory_t *)&test->owners_directory_storage)
    return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CREATE);
  *out = (flowie_cluster_endpoint_binding_t *)test;
  return TURBO_OK;
}

static const flowie_endpoint_cluster_binding_t *flowie_cluster_generation_test_endpoint_port(
    flowie_cluster_endpoint_binding_t *binding) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  return binding && test ? &test->endpoint_port : NULL;
}

static int flowie_cluster_generation_test_endpoint_close(
    flowie_cluster_endpoint_binding_t *binding, uint64_t timeout_ns) {
  flowie_cluster_generation_test_t *test = flowie_cluster_generation_active_test;
  int rc = TURBO_OK;
  if (!binding || !test || timeout_ns == 0u) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CLOSE);
  if (test->endpoint_close_count < 2u)
    rc = test->endpoint_close_results[test->endpoint_close_count];
  test->endpoint_close_count++;
  return rc;
}

static int flowie_cluster_generation_test_endpoint_destroy(
    flowie_cluster_endpoint_binding_t *binding) {
  if (!binding) return TURBO_EINVAL;
  flowie_cluster_generation_test_record(FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_DESTROY);
  return TURBO_OK;
}

static const flowie_cluster_generation_api_t FLOWIE_CLUSTER_GENERATION_TEST_API = {
    sizeof(flowie_cluster_generation_api_t),
    FLOWIE_CLUSTER_GENERATION_ABI_V1,
    flowie_cluster_generation_test_owners_create,
    flowie_cluster_generation_test_owners_start,
    flowie_cluster_generation_test_owners_ready,
    flowie_cluster_generation_test_owners_directory,
    flowie_cluster_generation_test_owners_close,
    flowie_cluster_generation_test_owners_destroy,
    flowie_cluster_generation_test_node_create,
    flowie_cluster_generation_test_node_start,
    flowie_cluster_generation_test_node_close,
    flowie_cluster_generation_test_node_destroy,
    flowie_cluster_generation_test_endpoint_create,
    flowie_cluster_generation_test_endpoint_port,
    flowie_cluster_generation_test_endpoint_close,
    flowie_cluster_generation_test_endpoint_destroy};

static flowie_cluster_generation_config_t flowie_cluster_generation_test_config(
    flowie_cluster_generation_test_t *test, flowie_cluster_pgsql_config_t *coordinator,
    flowie_cluster_shard_runtime_config_t *shard,
    flowie_cluster_peer_connector_config_t *connector) {
  flowie_cluster_generation_config_t config = FLOWIE_CLUSTER_GENERATION_CONFIG_INIT;
  const char *cluster_id = "cluster-a";
  const char *listener_id = "mqtt";
  const char *node_id = "node-a";
  memset(test, 0, sizeof(*test));
  test->owners_ready_result = TURBO_OK;
  test->node_start_result = TURBO_OK;
  test->endpoint_port =
      (flowie_endpoint_cluster_binding_t)FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
  *coordinator = (flowie_cluster_pgsql_config_t)FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  coordinator->cluster_id = cluster_id;
  coordinator->listener_id = listener_id;
  coordinator->node_id = node_id;
  coordinator->shard_count = 2u;
  coordinator->boot_id[0] = 1u;
  *shard = (flowie_cluster_shard_runtime_config_t)FLOWIE_CLUSTER_SHARD_RUNTIME_CONFIG_INIT;
  *connector =
      (flowie_cluster_peer_connector_config_t)FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT;
  config.owners.coordinator = coordinator;
  config.node.router.shard_count = 2u;
  config.node.router.max_links = 2u;
  config.node.router.cluster_id = tstr_v_from_cstr(cluster_id);
  config.node.router.listener_id = tstr_v_from_cstr(listener_id);
  config.node.router.local_node_id = tstr_v_from_cstr(node_id);
  config.node.router.local_boot_id[0] = 1u;
  config.node.local_shards = shard;
  config.node.local_shard_count = 1u;
  config.node.connectors = connector;
  config.node.connector_count = 1u;
  config.endpoint.cluster_id = config.node.router.cluster_id;
  config.endpoint.listener_id = config.node.router.listener_id;
  config.endpoint.local_node_id = config.node.router.local_node_id;
  config.endpoint.local_boot_id[0] = 1u;
  config.startup_timeout_ns = FLOWIE_CLUSTER_GENERATION_TEST_TIMEOUT_NS;
  config.shutdown_timeout_ns = FLOWIE_CLUSTER_GENERATION_TEST_TIMEOUT_NS;
  return config;
}

static void flowie_cluster_generation_test_check_event(
    const flowie_cluster_generation_test_t *test, size_t index,
    flowie_cluster_generation_test_event_t event) {
  check_true(index < test->event_count);
  if (index < test->event_count) check_int_eq(test->events[index], event);
}

spec("flowie cluster generation") {
  it("owns one lane and enforces dependency-ordered start and shutdown") {
    flowie_cluster_generation_test_t test;
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_shard_runtime_config_t shard;
    flowie_cluster_peer_connector_config_t connector;
    flowie_cluster_generation_config_t config =
        flowie_cluster_generation_test_config(&test, &coordinator, &shard, &connector);
    flowie_cluster_generation_t *generation = NULL;
    flowie_cluster_generation_active_test = &test;
    check_int_eq(flowie_cluster_generation_create_with_api(
                     &config, &FLOWIE_CLUSTER_GENERATION_TEST_API, &generation),
                 TURBO_OK);
    check_not_null(flowie_cluster_generation_endpoint_execution(generation));
    check_not_null(flowie_cluster_generation_endpoint_port(generation));
    check_int_eq(flowie_cluster_generation_start(generation), TURBO_OK);
    check_int_eq(flowie_cluster_generation_close(generation), TURBO_OK);
    check_int_eq(flowie_cluster_generation_destroy(generation), TURBO_OK);
    check_size_eq(test.event_count, 12u);
    flowie_cluster_generation_test_check_event(
        &test, 0u, FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CREATE);
    flowie_cluster_generation_test_check_event(&test, 1u,
                                               FLOWIE_CLUSTER_GENERATION_TEST_NODE_CREATE);
    flowie_cluster_generation_test_check_event(
        &test, 2u, FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CREATE);
    flowie_cluster_generation_test_check_event(
        &test, 3u, FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_START);
    flowie_cluster_generation_test_check_event(
        &test, 4u, FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_READY);
    flowie_cluster_generation_test_check_event(&test, 5u,
                                               FLOWIE_CLUSTER_GENERATION_TEST_NODE_START);
    flowie_cluster_generation_test_check_event(
        &test, 6u, FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CLOSE);
    flowie_cluster_generation_test_check_event(&test, 7u,
                                               FLOWIE_CLUSTER_GENERATION_TEST_NODE_CLOSE);
    flowie_cluster_generation_test_check_event(
        &test, 8u, FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CLOSE);
    flowie_cluster_generation_test_check_event(
        &test, 9u, FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_DESTROY);
    flowie_cluster_generation_test_check_event(&test, 10u,
                                               FLOWIE_CLUSTER_GENERATION_TEST_NODE_DESTROY);
    flowie_cluster_generation_test_check_event(
        &test, 11u, FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_DESTROY);
    flowie_cluster_generation_active_test = NULL;
  }

  it("rolls back the complete generation when the first owner snapshot fails") {
    flowie_cluster_generation_test_t test;
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_shard_runtime_config_t shard;
    flowie_cluster_peer_connector_config_t connector;
    flowie_cluster_generation_config_t config =
        flowie_cluster_generation_test_config(&test, &coordinator, &shard, &connector);
    flowie_cluster_generation_t *generation = NULL;
    test.owners_ready_result = TURBO_EIO;
    flowie_cluster_generation_active_test = &test;
    check_int_eq(flowie_cluster_generation_create_with_api(
                     &config, &FLOWIE_CLUSTER_GENERATION_TEST_API, &generation),
                 TURBO_OK);
    check_int_eq(flowie_cluster_generation_start(generation), TURBO_EIO);
    check_int_eq(flowie_cluster_generation_destroy(generation), TURBO_OK);
    check_size_eq(test.event_count, 11u);
    flowie_cluster_generation_test_check_event(
        &test, 5u, FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CLOSE);
    flowie_cluster_generation_test_check_event(&test, 6u,
                                               FLOWIE_CLUSTER_GENERATION_TEST_NODE_CLOSE);
    flowie_cluster_generation_test_check_event(
        &test, 7u, FLOWIE_CLUSTER_GENERATION_TEST_OWNERS_CLOSE);
    flowie_cluster_generation_active_test = NULL;
  }

  it("detaches the endpoint before destroying a node whose peer startup failed") {
    flowie_cluster_generation_test_t test;
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_shard_runtime_config_t shard;
    flowie_cluster_peer_connector_config_t connector;
    flowie_cluster_generation_config_t config =
        flowie_cluster_generation_test_config(&test, &coordinator, &shard, &connector);
    flowie_cluster_generation_t *generation = NULL;
    test.node_start_result = TURBO_EIO;
    flowie_cluster_generation_active_test = &test;
    check_int_eq(flowie_cluster_generation_create_with_api(
                     &config, &FLOWIE_CLUSTER_GENERATION_TEST_API, &generation),
                 TURBO_OK);
    check_int_eq(flowie_cluster_generation_start(generation), TURBO_EIO);
    flowie_cluster_generation_test_check_event(
        &test, 6u, FLOWIE_CLUSTER_GENERATION_TEST_ENDPOINT_CLOSE);
    flowie_cluster_generation_test_check_event(&test, 7u,
                                               FLOWIE_CLUSTER_GENERATION_TEST_NODE_CLOSE);
    check_int_eq(flowie_cluster_generation_destroy(generation), TURBO_OK);
    flowie_cluster_generation_active_test = NULL;
  }

  it("does not tear down node truth while endpoint connections still block close") {
    flowie_cluster_generation_test_t test;
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_shard_runtime_config_t shard;
    flowie_cluster_peer_connector_config_t connector;
    flowie_cluster_generation_config_t config =
        flowie_cluster_generation_test_config(&test, &coordinator, &shard, &connector);
    flowie_cluster_generation_t *generation = NULL;
    test.endpoint_close_results[0] = TURBO_EBUSY;
    test.endpoint_close_results[1] = TURBO_OK;
    flowie_cluster_generation_active_test = &test;
    check_int_eq(flowie_cluster_generation_create_with_api(
                     &config, &FLOWIE_CLUSTER_GENERATION_TEST_API, &generation),
                 TURBO_OK);
    check_int_eq(flowie_cluster_generation_start(generation), TURBO_OK);
    check_int_eq(flowie_cluster_generation_close(generation), TURBO_EBUSY);
    check_size_eq(test.event_count, 7u);
    check_int_eq(flowie_cluster_generation_close(generation), TURBO_OK);
    check_int_eq(flowie_cluster_generation_destroy(generation), TURBO_OK);
    flowie_cluster_generation_active_test = NULL;
  }

  it("rejects nested executors instead of silently replacing their ownership") {
    flowie_cluster_generation_test_t test;
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_shard_runtime_config_t shard;
    flowie_cluster_peer_connector_config_t connector;
    flowie_cluster_generation_config_t config =
        flowie_cluster_generation_test_config(&test, &coordinator, &shard, &connector);
    flowie_cluster_generation_t *generation = NULL;
    tf_coronet_execution_t conflicting_execution = {0};
    shard.execution = &conflicting_execution;
    flowie_cluster_generation_active_test = &test;
    check_int_eq(flowie_cluster_generation_create_with_api(
                     &config, &FLOWIE_CLUSTER_GENERATION_TEST_API, &generation),
                 TURBO_EINVAL);
    check_null(generation);
    check_size_eq(test.event_count, 0u);
    flowie_cluster_generation_active_test = NULL;
  }
}
