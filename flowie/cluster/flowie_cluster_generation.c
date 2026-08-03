#include "flowie_cluster_generation_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef enum flowie_cluster_generation_state_e {
  FLOWIE_CLUSTER_GENERATION_CREATED = 0,
  FLOWIE_CLUSTER_GENERATION_RUNNING,
  FLOWIE_CLUSTER_GENERATION_CLOSING,
  FLOWIE_CLUSTER_GENERATION_CLOSED
} flowie_cluster_generation_state_t;

typedef struct flowie_cluster_generation_deadline_s {
  uint64_t deadline_ns;
  int infinite;
} flowie_cluster_generation_deadline_t;

struct flowie_cluster_generation_s {
  const flowie_cluster_generation_api_t *api;
  tf_coronet_execution_t execution;
  turbo_flow_coronet_execution_binding_t endpoint_execution;
  flowie_cluster_owner_directory_runtime_t *owners;
  flowie_cluster_node_t *node;
  flowie_cluster_endpoint_binding_t *endpoint;
  flowie_cluster_node_connector_configure_fn connector_configure;
  void *connector_configure_ctx;
  uint64_t startup_timeout_ns;
  uint64_t shutdown_timeout_ns;
  flowie_cluster_generation_state_t state;
  int execution_initialized;
  int execution_started;
  int owners_started;
  int endpoint_closed;
  int node_closed;
  int owners_closed;
};

static const flowie_cluster_generation_api_t FLOWIE_CLUSTER_GENERATION_DEFAULT_API = {
    sizeof(flowie_cluster_generation_api_t),
    FLOWIE_CLUSTER_GENERATION_ABI_V1,
    flowie_cluster_owner_directory_runtime_create,
    flowie_cluster_owner_directory_runtime_start,
    flowie_cluster_owner_directory_runtime_wait_ready,
    flowie_cluster_owner_directory_runtime_directory,
    flowie_cluster_owner_directory_runtime_close,
    flowie_cluster_owner_directory_runtime_destroy,
    flowie_cluster_node_create,
    flowie_cluster_node_start,
    flowie_cluster_node_close,
    flowie_cluster_node_destroy,
    flowie_cluster_endpoint_binding_create,
    flowie_cluster_endpoint_binding_port,
    flowie_cluster_endpoint_binding_close,
    flowie_cluster_endpoint_binding_destroy};

static int flowie_cluster_generation_view_equal(tstr_v left, tstr_v right) {
  return left.data && right.data && left.len == right.len &&
         memcmp(left.data, right.data, left.len) == 0;
}

static int flowie_cluster_generation_view_equal_cstr(tstr_v left, const char *right) {
  return right && flowie_cluster_generation_view_equal(left, tstr_v_from_cstr(right));
}

static int flowie_cluster_generation_api_validate(
    const flowie_cluster_generation_api_t *api) {
  return api && api->size == sizeof(*api) &&
                 api->abi_version == FLOWIE_CLUSTER_GENERATION_ABI_V1 && api->owners_create &&
                 api->owners_start && api->owners_wait_ready && api->owners_directory &&
                 api->owners_close && api->owners_destroy && api->node_create && api->node_start &&
                 api->node_close && api->node_destroy && api->endpoint_create &&
                 api->endpoint_port && api->endpoint_close && api->endpoint_destroy
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flowie_cluster_generation_config_validate(
    const flowie_cluster_generation_config_t *config) {
  const flowie_cluster_pgsql_config_t *coordinator;
  size_t index;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_GENERATION_ABI_V1 ||
      turbo_flow_coronet_execution_binding_validate(&config->execution) != TURBO_OK ||
      config->startup_timeout_ns == 0u || config->shutdown_timeout_ns == 0u ||
      config->owners.size != sizeof(config->owners) ||
      config->owners.abi_version != FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1 ||
      !(coordinator = config->owners.coordinator) ||
      config->node.size != sizeof(config->node) ||
      config->node.abi_version != FLOWIE_CLUSTER_NODE_ABI_V1 || config->node.listener.execution ||
      (config->node.local_shard_count != 0u && !config->node.local_shards) ||
      (config->node.connector_count != 0u && !config->node.connectors) ||
      config->node.local_shard_count > SIZE_MAX / sizeof(*config->node.local_shards) ||
      config->node.connector_count > SIZE_MAX / sizeof(*config->node.connectors) ||
      config->endpoint.size != sizeof(config->endpoint) ||
      config->endpoint.abi_version != FLOWIE_CLUSTER_ENDPOINT_BINDING_ABI_V1 ||
      config->endpoint.execution || config->endpoint.node || config->endpoint.owners ||
      !flowie_cluster_generation_view_equal(config->node.router.cluster_id,
                                            config->endpoint.cluster_id) ||
      !flowie_cluster_generation_view_equal(config->node.router.listener_id,
                                            config->endpoint.listener_id) ||
      !flowie_cluster_generation_view_equal(config->node.router.local_node_id,
                                            config->endpoint.local_node_id) ||
      memcmp(config->node.router.local_boot_id, config->endpoint.local_boot_id,
             FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0 ||
      coordinator->shard_count != config->node.router.shard_count ||
      !flowie_cluster_generation_view_equal_cstr(config->node.router.cluster_id,
                                                 coordinator->cluster_id) ||
      !flowie_cluster_generation_view_equal_cstr(config->node.router.listener_id,
                                                 coordinator->listener_id) ||
      !flowie_cluster_generation_view_equal_cstr(config->node.router.local_node_id,
                                                 coordinator->node_id) ||
      memcmp(config->node.router.local_boot_id, coordinator->boot_id,
             FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
    return TURBO_EINVAL;
  for (index = 0u; index < config->node.local_shard_count; ++index)
    if (config->node.local_shards[index].execution) return TURBO_EINVAL;
  for (index = 0u; index < config->node.connector_count; ++index)
    if (config->node.connectors[index].execution) return TURBO_EINVAL;
  return TURBO_OK;
}

static flowie_cluster_generation_deadline_t flowie_cluster_generation_deadline(
    uint64_t timeout_ns) {
  flowie_cluster_generation_deadline_t deadline;
  uint64_t now_ns = turbo_hrtime();
  deadline.infinite = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - now_ns;
  deadline.deadline_ns = deadline.infinite ? UINT64_MAX : now_ns + timeout_ns;
  return deadline;
}

static int flowie_cluster_generation_remaining(
    const flowie_cluster_generation_deadline_t *deadline, uint64_t *out) {
  uint64_t now_ns;
  if (!deadline || !out) return TURBO_EINVAL;
  if (deadline->infinite) {
    *out = UINT64_MAX;
    return TURBO_OK;
  }
  now_ns = turbo_hrtime();
  if (now_ns >= deadline->deadline_ns) return TURBO_ETIMEDOUT;
  *out = deadline->deadline_ns - now_ns;
  return TURBO_OK;
}

static int flowie_cluster_generation_configure_connector(
    void *ctx, const flowie_cluster_topology_peer_t *peer,
    flowie_cluster_peer_connector_config_t *out) {
  flowie_cluster_generation_t *generation = (flowie_cluster_generation_t *)ctx;
  int rc;
  if (!generation || !generation->connector_configure || !out) return TURBO_EINVAL;
  rc = generation->connector_configure(generation->connector_configure_ctx, peer, out);
  if (rc != TURBO_OK) return rc;
  if (out->execution) return TURBO_EINVAL;
  out->execution = &generation->execution;
  return TURBO_OK;
}

static int flowie_cluster_generation_execution_ready(void *arg) {
  return arg ? TURBO_OK : TURBO_EINVAL;
}

static void flowie_cluster_generation_execution_binding_init(
    flowie_cluster_generation_t *generation,
    const turbo_flow_coronet_execution_binding_t *configured) {
  generation->endpoint_execution = *configured;
  if (configured->kind == TURBO_FLOW_CORONET_EXECUTION_PRIVATE ||
      configured->kind == TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT) {
    generation->endpoint_execution.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    generation->endpoint_execution.context = generation->execution.context;
    generation->endpoint_execution.pool = NULL;
    generation->endpoint_execution.lane = 0u;
  }
}

static int flowie_cluster_generation_close_with_timeout(
    flowie_cluster_generation_t *generation, uint64_t timeout_ns) {
  flowie_cluster_generation_deadline_t deadline;
  uint64_t remaining_ns;
  int rc;
  if (!generation || timeout_ns == 0u) return TURBO_EINVAL;
  if (generation->state == FLOWIE_CLUSTER_GENERATION_CLOSED) return TURBO_OK;
  generation->state = FLOWIE_CLUSTER_GENERATION_CLOSING;
  deadline = flowie_cluster_generation_deadline(timeout_ns);
  if (generation->endpoint && !generation->endpoint_closed) {
    rc = flowie_cluster_generation_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK)
      rc = generation->api->endpoint_close(generation->endpoint, remaining_ns);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    generation->endpoint_closed = 1;
  }
  if (generation->node && !generation->node_closed) {
    rc = flowie_cluster_generation_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = generation->api->node_close(generation->node, remaining_ns);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    generation->node_closed = 1;
  }
  if (generation->owners && !generation->owners_closed) {
    rc = flowie_cluster_generation_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = generation->api->owners_close(generation->owners, remaining_ns);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    generation->owners_closed = 1;
  }
  generation->state = FLOWIE_CLUSTER_GENERATION_CLOSED;
  return TURBO_OK;
}

static void flowie_cluster_generation_create_cleanup(flowie_cluster_generation_t *generation) {
  if (!generation) return;
  (void)flowie_cluster_generation_close_with_timeout(generation, UINT64_MAX);
  if (generation->endpoint) (void)generation->api->endpoint_destroy(generation->endpoint);
  if (generation->node) (void)generation->api->node_destroy(generation->node);
  if (generation->owners) generation->api->owners_destroy(generation->owners);
  if (generation->execution_started) tf_coronet_execution_stop(&generation->execution);
  if (generation->execution_initialized) tf_coronet_execution_destroy(&generation->execution);
  free(generation);
}

int flowie_cluster_generation_create_with_api(
    const flowie_cluster_generation_config_t *config,
    const flowie_cluster_generation_api_t *api, flowie_cluster_generation_t **out) {
  flowie_cluster_generation_t *generation;
  flowie_cluster_shard_runtime_config_t *shards = NULL;
  flowie_cluster_peer_connector_config_t *connectors = NULL;
  flowie_cluster_node_config_t node_config;
  flowie_cluster_endpoint_binding_config_t endpoint_config;
  flowie_cluster_owner_directory_t *owners;
  size_t index;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_generation_api_validate(api);
  if (rc == TURBO_OK) rc = flowie_cluster_generation_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  generation = (flowie_cluster_generation_t *)calloc(1u, sizeof(*generation));
  if (!generation) return TURBO_ENOMEM;
  generation->api = api;
  generation->startup_timeout_ns = config->startup_timeout_ns;
  generation->shutdown_timeout_ns = config->shutdown_timeout_ns;
  generation->state = FLOWIE_CLUSTER_GENERATION_CREATED;
  generation->connector_configure = config->node.connector_configure;
  generation->connector_configure_ctx = config->node.connector_configure_ctx;
  rc = tf_coronet_execution_init(&generation->execution, &config->execution);
  if (rc != TURBO_OK) goto fail;
  generation->execution_initialized = 1;
  rc = tf_coronet_execution_start(&generation->execution);
  if (rc != TURBO_OK) goto fail;
  generation->execution_started = 1;
  rc = tf_coronet_execution_call(&generation->execution,
                                 flowie_cluster_generation_execution_ready, generation,
                                 config->startup_timeout_ns);
  if (rc != TURBO_OK) goto fail;
  flowie_cluster_generation_execution_binding_init(generation, &config->execution);
  rc = api->owners_create(&config->owners, &generation->owners);
  if (rc != TURBO_OK) goto fail;
  owners = api->owners_directory(generation->owners);
  if (!owners) {
    rc = TURBO_EINVAL;
    goto fail;
  }
  if (config->node.local_shard_count != 0u) {
    shards = (flowie_cluster_shard_runtime_config_t *)malloc(
        config->node.local_shard_count * sizeof(*shards));
    if (!shards) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    memcpy(shards, config->node.local_shards,
           config->node.local_shard_count * sizeof(*shards));
    for (index = 0u; index < config->node.local_shard_count; ++index)
      shards[index].execution = &generation->execution;
  }
  if (config->node.connector_count != 0u) {
    connectors = (flowie_cluster_peer_connector_config_t *)malloc(
        config->node.connector_count * sizeof(*connectors));
    if (!connectors) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    memcpy(connectors, config->node.connectors,
           config->node.connector_count * sizeof(*connectors));
    for (index = 0u; index < config->node.connector_count; ++index)
      connectors[index].execution = &generation->execution;
  }
  node_config = config->node;
  node_config.listener.execution = &generation->execution;
  node_config.local_shards = shards;
  node_config.connectors = connectors;
  if (generation->connector_configure) {
    node_config.connector_configure = flowie_cluster_generation_configure_connector;
    node_config.connector_configure_ctx = generation;
  }
  rc = api->node_create(&node_config, &generation->node);
  free(connectors);
  connectors = NULL;
  free(shards);
  shards = NULL;
  if (rc != TURBO_OK) goto fail;
  endpoint_config = config->endpoint;
  endpoint_config.execution = &generation->execution;
  endpoint_config.node = generation->node;
  endpoint_config.owners = owners;
  rc = api->endpoint_create(&endpoint_config, &generation->endpoint);
  if (rc != TURBO_OK) goto fail;
  if (!api->endpoint_port(generation->endpoint)) {
    rc = TURBO_EINVAL;
    goto fail;
  }
  *out = generation;
  return TURBO_OK;

fail:
  free(connectors);
  free(shards);
  flowie_cluster_generation_create_cleanup(generation);
  return rc;
}

int flowie_cluster_generation_create(const flowie_cluster_generation_config_t *config,
                                     flowie_cluster_generation_t **out) {
  return flowie_cluster_generation_create_with_api(
      config, &FLOWIE_CLUSTER_GENERATION_DEFAULT_API, out);
}

int flowie_cluster_generation_start(flowie_cluster_generation_t *generation) {
  flowie_cluster_generation_deadline_t deadline;
  uint64_t remaining_ns;
  int rc;
  if (!generation || generation->state != FLOWIE_CLUSTER_GENERATION_CREATED) return TURBO_EINVAL;
  deadline = flowie_cluster_generation_deadline(generation->startup_timeout_ns);
  rc = generation->api->owners_start(generation->owners);
  if (rc != TURBO_OK) goto fail;
  generation->owners_started = 1;
  rc = flowie_cluster_generation_remaining(&deadline, &remaining_ns);
  if (rc == TURBO_OK)
    rc = generation->api->owners_wait_ready(generation->owners, remaining_ns);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_generation_remaining(&deadline, &remaining_ns);
  if (rc == TURBO_OK) rc = generation->api->node_start(generation->node, remaining_ns);
  if (rc != TURBO_OK) goto fail;
  generation->state = FLOWIE_CLUSTER_GENERATION_RUNNING;
  return TURBO_OK;

fail:
  (void)flowie_cluster_generation_close_with_timeout(generation,
                                                     generation->shutdown_timeout_ns);
  return rc;
}

const turbo_flow_coronet_execution_binding_t *flowie_cluster_generation_endpoint_execution(
    flowie_cluster_generation_t *generation) {
  return generation ? &generation->endpoint_execution : NULL;
}

const flowie_endpoint_cluster_binding_t *flowie_cluster_generation_endpoint_port(
    flowie_cluster_generation_t *generation) {
  return generation && generation->endpoint
             ? generation->api->endpoint_port(generation->endpoint)
             : NULL;
}

int flowie_cluster_generation_close(flowie_cluster_generation_t *generation) {
  return generation
             ? flowie_cluster_generation_close_with_timeout(generation,
                                                             generation->shutdown_timeout_ns)
             : TURBO_EINVAL;
}

int flowie_cluster_generation_destroy(flowie_cluster_generation_t *generation) {
  int rc;
  if (!generation) return TURBO_OK;
  rc = flowie_cluster_generation_close_with_timeout(generation, UINT64_MAX);
  if (rc != TURBO_OK) return rc;
  if (generation->endpoint) {
    rc = generation->api->endpoint_destroy(generation->endpoint);
    if (rc != TURBO_OK) return rc;
    generation->endpoint = NULL;
  }
  if (generation->node) {
    rc = generation->api->node_destroy(generation->node);
    if (rc != TURBO_OK) return rc;
    generation->node = NULL;
  }
  if (generation->owners) {
    generation->api->owners_destroy(generation->owners);
    generation->owners = NULL;
  }
  if (generation->execution_started) {
    tf_coronet_execution_stop(&generation->execution);
    generation->execution_started = 0;
  }
  if (generation->execution_initialized) {
    tf_coronet_execution_destroy(&generation->execution);
    generation->execution_initialized = 0;
  }
  free(generation);
  return TURBO_OK;
}
