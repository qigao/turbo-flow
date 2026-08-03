#include "flowie_cluster_membership_runtime_internal.h"

#include "monocypher.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

struct flowie_cluster_membership_runtime_s {
  const flowie_cluster_membership_runtime_api_t *api;
  flowie_cluster_pgsql_config_t coordinator_config;
  flowie_cluster_topology_plan_config_t topology_config;
  tstr_t conninfo;
  tstr_t schema_name;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t node_id;
  tstr_t advertised_endpoint;
  tstr_t topology_node_id;
  flowie_cluster_pgsql_coordinator_t *coordinator;
  flowie_cluster_topology_peer_t *current_peers;
  uint64_t refresh_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t apply_timeout_ns;
  flowie_cluster_membership_current_fn current;
  flowie_cluster_membership_apply_fn apply;
  void *topology_ctx;
  flowie_cluster_member_directory_t *member_directory;
  flowie_cluster_membership_maintenance_fn maintenance;
  void *maintenance_ctx;
  flowie_cluster_membership_runtime_state_t state;
  uint64_t cycle_count;
  uint64_t applied_revision;
  int last_status;
  int closing;
  int thread_started;
  int sync_initialized;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
};

static const flowie_cluster_membership_runtime_api_t FLOWIE_CLUSTER_MEMBERSHIP_DEFAULT_API = {
    sizeof(flowie_cluster_membership_runtime_api_t), FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1,
    flowie_cluster_pgsql_coordinator_open,           flowie_cluster_pgsql_coordinator_destroy,
    flowie_cluster_membership_refresh_plan,          flowie_cluster_topology_plan_destroy};

static int
flowie_cluster_membership_api_validate(const flowie_cluster_membership_runtime_api_t *api) {
  return api && api->size == sizeof(*api) &&
                 api->abi_version == FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1 &&
                 api->coordinator_open && api->coordinator_destroy && api->refresh_plan &&
                 api->plan_destroy
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flowie_cluster_membership_view_equal_cstr(tstr_v view, const char *text) {
  size_t size = text ? strlen(text) : 0u;
  return view.data && text && view.len == size && memcmp(view.data, text, size) == 0;
}

int flowie_cluster_membership_runtime_config_validate(
    const flowie_cluster_membership_runtime_config_t *config) {
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1 || !config->coordinator ||
      !config->current || !config->apply || config->refresh_interval_ns == 0u ||
      config->retry_interval_ns == 0u || config->apply_timeout_ns == 0u ||
      config->topology.last_applied_revision != 0u ||
      (config->maintenance_ctx && !config->maintenance))
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_config_validate(config->coordinator);
  if (rc != TURBO_OK) return rc;
  if (!flowie_cluster_membership_view_equal_cstr(config->topology.local_node_id,
                                                 config->coordinator->node_id) ||
      memcmp(config->topology.local_boot_id, config->coordinator->boot_id,
             FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
    return TURBO_EINVAL;
  return flowie_cluster_topology_plan_config_validate(&config->topology);
}

static int
flowie_cluster_membership_config_copy(flowie_cluster_membership_runtime_t *runtime,
                                      const flowie_cluster_membership_runtime_config_t *config) {
  const flowie_cluster_pgsql_config_t *source = config->coordinator;
  runtime->conninfo = tstr_dup(source->conninfo);
  runtime->schema_name = tstr_dup(source->schema_name);
  runtime->cluster_id = tstr_dup(source->cluster_id);
  runtime->listener_id = tstr_dup(source->listener_id);
  runtime->node_id = tstr_dup(source->node_id);
  runtime->advertised_endpoint = tstr_dup(source->advertised_endpoint);
  runtime->topology_node_id = tstr_from_v(config->topology.local_node_id);
  if (!runtime->conninfo || !runtime->schema_name || !runtime->cluster_id ||
      !runtime->listener_id || !runtime->node_id || !runtime->advertised_endpoint ||
      !runtime->topology_node_id)
    return TURBO_ENOMEM;
  runtime->coordinator_config = *source;
  runtime->coordinator_config.conninfo = runtime->conninfo;
  runtime->coordinator_config.schema_name = runtime->schema_name;
  runtime->coordinator_config.cluster_id = runtime->cluster_id;
  runtime->coordinator_config.listener_id = runtime->listener_id;
  runtime->coordinator_config.node_id = runtime->node_id;
  runtime->coordinator_config.advertised_endpoint = runtime->advertised_endpoint;
  runtime->coordinator_config.create_schema = 0;
  runtime->topology_config = config->topology;
  runtime->topology_config.local_node_id = tstr_to_v(runtime->topology_node_id);
  runtime->refresh_interval_ns = config->refresh_interval_ns;
  runtime->retry_interval_ns = config->retry_interval_ns;
  runtime->apply_timeout_ns = config->apply_timeout_ns;
  runtime->current = config->current;
  runtime->apply = config->apply;
  runtime->topology_ctx = config->topology_ctx;
  runtime->member_directory = config->member_directory;
  runtime->maintenance = config->maintenance;
  runtime->maintenance_ctx = config->maintenance_ctx;
  return TURBO_OK;
}

static void
flowie_cluster_membership_storage_destroy(flowie_cluster_membership_runtime_t *runtime) {
  if (!runtime) return;
  runtime->api->coordinator_destroy(runtime->coordinator);
  free(runtime->current_peers);
  if (runtime->conninfo) crypto_wipe(runtime->conninfo, tstr_len(runtime->conninfo));
  tstr_freep(&runtime->conninfo);
  tstr_freep(&runtime->schema_name);
  tstr_freep(&runtime->cluster_id);
  tstr_freep(&runtime->listener_id);
  tstr_freep(&runtime->node_id);
  tstr_freep(&runtime->advertised_endpoint);
  tstr_freep(&runtime->topology_node_id);
  if (runtime->sync_initialized) {
    turbo_cond_destroy(&runtime->changed);
    turbo_mutex_destroy(&runtime->mutex);
  }
  free(runtime);
}

static int flowie_cluster_membership_retryable(int status) {
  return status == TURBO_EIO || status == TURBO_ETIMEDOUT || status == TURBO_EBUSY;
}

static int flowie_cluster_membership_cycle(flowie_cluster_membership_runtime_t *runtime,
                                           uint64_t *out_revision) {
  flowie_cluster_topology_plan_config_t topology = runtime->topology_config;
  flowie_cluster_topology_plan_t *plan = NULL;
  flowie_cluster_pgsql_membership_snapshot_t membership =
      FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
  flowie_cluster_member_directory_snapshot_t *candidate = NULL;
  size_t peer_count = 0u;
  uint64_t current_revision = 0u;
  int refresh_status = TURBO_OK;
  int rc;
  if (!runtime->coordinator) {
    rc = runtime->api->coordinator_open(&runtime->coordinator_config, &runtime->coordinator);
    if (rc != TURBO_OK) return rc;
  }
  rc = runtime->current(runtime->topology_ctx, runtime->current_peers,
                        runtime->topology_config.max_nodes, &peer_count, &current_revision);
  if (rc == TURBO_OK) {
    topology.last_applied_revision = current_revision;
    refresh_status = runtime->api->refresh_plan(runtime->coordinator, &topology,
                                                runtime->current_peers, peer_count, &plan,
                                                &membership);
    rc = refresh_status;
  }
  if (rc == TURBO_OK && !plan) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && runtime->member_directory)
    rc = flowie_cluster_member_directory_prepare(runtime->member_directory, &membership,
                                                 &candidate);
  if (rc == TURBO_OK) rc = runtime->apply(runtime->topology_ctx, plan, runtime->apply_timeout_ns);
  if (rc == TURBO_OK && candidate) {
    rc = flowie_cluster_member_directory_publish(runtime->member_directory, candidate);
    if (rc == TURBO_OK) candidate = NULL;
  }
  if (rc == TURBO_OK && runtime->maintenance) {
    size_t changed = 0u;
    rc = runtime->maintenance(runtime->maintenance_ctx, &changed);
  }
  if (rc == TURBO_OK && out_revision) *out_revision = flowie_cluster_topology_plan_revision(plan);
  flowie_cluster_member_directory_snapshot_destroy(candidate);
  flowie_cluster_pgsql_membership_snapshot_cleanup(&membership);
  runtime->api->plan_destroy(plan);
  if (refresh_status == TURBO_EIO) {
    runtime->api->coordinator_destroy(runtime->coordinator);
    runtime->coordinator = NULL;
  }
  return rc;
}

static void flowie_cluster_membership_run(void *ctx) {
  flowie_cluster_membership_runtime_t *runtime = (flowie_cluster_membership_runtime_t *)ctx;
  for (;;) {
    uint64_t revision = 0u;
    uint64_t wait_ns;
    int rc;
    turbo_mutex_lock(&runtime->mutex);
    if (runtime->closing) {
      runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_CLOSED;
      turbo_cond_broadcast(&runtime->changed);
      turbo_mutex_unlock(&runtime->mutex);
      return;
    }
    turbo_mutex_unlock(&runtime->mutex);

    rc = flowie_cluster_membership_cycle(runtime, &revision);

    turbo_mutex_lock(&runtime->mutex);
    runtime->cycle_count++;
    runtime->last_status = rc;
    if (rc == TURBO_OK) {
      runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_RUNNING;
      runtime->applied_revision = revision;
      wait_ns = runtime->refresh_interval_ns;
    } else if (flowie_cluster_membership_retryable(rc)) {
      runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_RETRYING;
      wait_ns = runtime->retry_interval_ns;
    } else {
      runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_FAULTED;
      while (!runtime->closing)
        turbo_cond_wait(&runtime->changed, &runtime->mutex);
      turbo_mutex_unlock(&runtime->mutex);
      continue;
    }
    if (!runtime->closing) (void)turbo_cond_timedwait(&runtime->changed, &runtime->mutex, wait_ns);
    turbo_mutex_unlock(&runtime->mutex);
  }
}

int flowie_cluster_membership_runtime_create_with_api(
    const flowie_cluster_membership_runtime_config_t *config,
    const flowie_cluster_membership_runtime_api_t *api, flowie_cluster_membership_runtime_t **out) {
  flowie_cluster_membership_runtime_t *runtime;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_membership_api_validate(api);
  if (rc == TURBO_OK) rc = flowie_cluster_membership_runtime_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  runtime = (flowie_cluster_membership_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  runtime->api = api;
  runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_CREATED;
  runtime->last_status = TURBO_EBUSY;
  turbo_mutex_init(&runtime->mutex);
  turbo_cond_init(&runtime->changed);
  runtime->sync_initialized = 1;
  rc = flowie_cluster_membership_config_copy(runtime, config);
  if (rc != TURBO_OK) goto fail;
  runtime->current_peers = (flowie_cluster_topology_peer_t *)calloc(
      config->topology.max_nodes, sizeof(*runtime->current_peers));
  if (!runtime->current_peers) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  *out = runtime;
  return TURBO_OK;

fail:
  flowie_cluster_membership_storage_destroy(runtime);
  return rc;
}

int flowie_cluster_membership_runtime_create(
    const flowie_cluster_membership_runtime_config_t *config,
    flowie_cluster_membership_runtime_t **out) {
  return flowie_cluster_membership_runtime_create_with_api(
      config, &FLOWIE_CLUSTER_MEMBERSHIP_DEFAULT_API, out);
}

int flowie_cluster_membership_runtime_start(flowie_cluster_membership_runtime_t *runtime) {
  int rc;
  if (!runtime) return TURBO_EINVAL;
  turbo_mutex_lock(&runtime->mutex);
  if (runtime->state != FLOWIE_CLUSTER_MEMBERSHIP_CREATED || runtime->thread_started) {
    turbo_mutex_unlock(&runtime->mutex);
    return TURBO_EALREADY;
  }
  runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_STARTING;
  turbo_mutex_unlock(&runtime->mutex);
  rc = turbo_thread_create(&runtime->thread, flowie_cluster_membership_run, runtime);
  turbo_mutex_lock(&runtime->mutex);
  if (rc == TURBO_OK) {
    runtime->thread_started = 1;
  } else if (runtime->state == FLOWIE_CLUSTER_MEMBERSHIP_STARTING) {
    runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_CREATED;
  }
  turbo_mutex_unlock(&runtime->mutex);
  return rc;
}

int flowie_cluster_membership_runtime_snapshot(flowie_cluster_membership_runtime_t *runtime,
                                               flowie_cluster_membership_runtime_snapshot_t *out) {
  if (!runtime || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&runtime->mutex);
  *out =
      (flowie_cluster_membership_runtime_snapshot_t)FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_SNAPSHOT_INIT;
  out->state = runtime->state;
  out->cycle_count = runtime->cycle_count;
  out->applied_revision = runtime->applied_revision;
  out->last_status = runtime->last_status;
  turbo_mutex_unlock(&runtime->mutex);
  return TURBO_OK;
}

int flowie_cluster_membership_runtime_close(flowie_cluster_membership_runtime_t *runtime,
                                            uint64_t timeout_ns) {
  uint64_t deadline_ns;
  uint64_t now_ns;
  int infinite;
  int rc = TURBO_OK;
  if (!runtime || timeout_ns == 0u) return TURBO_EINVAL;
  now_ns = turbo_hrtime();
  infinite = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - now_ns;
  deadline_ns = infinite ? UINT64_MAX : now_ns + timeout_ns;
  turbo_mutex_lock(&runtime->mutex);
  if (!runtime->thread_started) {
    runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_CLOSED;
    turbo_mutex_unlock(&runtime->mutex);
    return TURBO_OK;
  }
  runtime->closing = 1;
  turbo_cond_broadcast(&runtime->changed);
  while (runtime->state != FLOWIE_CLUSTER_MEMBERSHIP_CLOSED) {
    if (infinite) {
      turbo_cond_wait(&runtime->changed, &runtime->mutex);
    } else {
      now_ns = turbo_hrtime();
      if (now_ns >= deadline_ns) {
        turbo_mutex_unlock(&runtime->mutex);
        return TURBO_ETIMEDOUT;
      }
      (void)turbo_cond_timedwait(&runtime->changed, &runtime->mutex, deadline_ns - now_ns);
    }
  }
  turbo_mutex_unlock(&runtime->mutex);
  rc = turbo_thread_join(&runtime->thread);
  if (rc != TURBO_OK) return rc;
  turbo_thread_destroy(&runtime->thread);
  turbo_mutex_lock(&runtime->mutex);
  runtime->thread_started = 0;
  runtime->state = FLOWIE_CLUSTER_MEMBERSHIP_CLOSED;
  turbo_mutex_unlock(&runtime->mutex);
  return TURBO_OK;
}

void flowie_cluster_membership_runtime_destroy(flowie_cluster_membership_runtime_t *runtime) {
  if (!runtime) return;
  (void)flowie_cluster_membership_runtime_close(runtime, UINT64_MAX);
  flowie_cluster_membership_storage_destroy(runtime);
}
