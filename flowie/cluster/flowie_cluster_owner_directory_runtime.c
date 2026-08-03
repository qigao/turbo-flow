#include "flowie_cluster_owner_directory_runtime_internal.h"

#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

struct flowie_cluster_owner_directory_runtime_s {
  const flowie_cluster_owner_directory_runtime_api_t *api;
  flowie_cluster_pgsql_config_t coordinator_config;
  tstr_t conninfo;
  tstr_t schema_name;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t node_id;
  tstr_t advertised_endpoint;
  flowie_cluster_pgsql_coordinator_t *coordinator;
  flowie_cluster_owner_directory_t *directory;
  flowie_cluster_owner_directory_entry_t *entries;
  uint32_t shard_count;
  uint64_t refresh_interval_ns;
  uint64_t retry_interval_ns;
  flowie_cluster_owner_directory_runtime_state_t state;
  uint64_t cycle_count;
  uint64_t refresh_generation;
  int last_status;
  int closing;
  int thread_started;
  int sync_initialized;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
};

static const flowie_cluster_owner_directory_runtime_api_t
    FLOWIE_CLUSTER_OWNER_DIRECTORY_DEFAULT_API = {
        sizeof(flowie_cluster_owner_directory_runtime_api_t),
        FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1,
        flowie_cluster_pgsql_coordinator_open,
        flowie_cluster_pgsql_coordinator_destroy,
        flowie_cluster_pgsql_shard_owner_snapshot,
        flowie_cluster_pgsql_shard_owner_snapshot_cleanup};

static int flowie_cluster_owner_directory_runtime_api_validate(
    const flowie_cluster_owner_directory_runtime_api_t *api) {
  return api && api->size == sizeof(*api) &&
                 api->abi_version == FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1 &&
                 api->coordinator_open && api->coordinator_destroy && api->owner_snapshot &&
                 api->owner_snapshot_cleanup
             ? TURBO_OK
             : TURBO_EINVAL;
}

int flowie_cluster_owner_directory_runtime_config_validate(
    const flowie_cluster_owner_directory_runtime_config_t *config) {
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1 ||
      !config->coordinator || config->hash_version != FLOWIE_CLUSTER_HASH_VERSION_1 ||
      config->refresh_interval_ns == 0u || config->retry_interval_ns == 0u)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_config_validate(config->coordinator);
  return rc;
}

static int flowie_cluster_owner_directory_runtime_config_copy(
    flowie_cluster_owner_directory_runtime_t *runtime,
    const flowie_cluster_owner_directory_runtime_config_t *config) {
  const flowie_cluster_pgsql_config_t *source = config->coordinator;
  runtime->conninfo = tstr_dup(source->conninfo);
  runtime->schema_name = tstr_dup(source->schema_name);
  runtime->cluster_id = tstr_dup(source->cluster_id);
  runtime->listener_id = tstr_dup(source->listener_id);
  runtime->node_id = tstr_dup(source->node_id);
  runtime->advertised_endpoint = tstr_dup(source->advertised_endpoint);
  if (!runtime->conninfo || !runtime->schema_name || !runtime->cluster_id ||
      !runtime->listener_id || !runtime->node_id || !runtime->advertised_endpoint)
    return TURBO_ENOMEM;
  runtime->coordinator_config = *source;
  runtime->coordinator_config.conninfo = runtime->conninfo;
  runtime->coordinator_config.schema_name = runtime->schema_name;
  runtime->coordinator_config.cluster_id = runtime->cluster_id;
  runtime->coordinator_config.listener_id = runtime->listener_id;
  runtime->coordinator_config.node_id = runtime->node_id;
  runtime->coordinator_config.advertised_endpoint = runtime->advertised_endpoint;
  runtime->shard_count = source->shard_count;
  runtime->refresh_interval_ns = config->refresh_interval_ns;
  runtime->retry_interval_ns = config->retry_interval_ns;
  return TURBO_OK;
}

static void flowie_cluster_owner_directory_runtime_storage_destroy(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  if (!runtime) return;
  runtime->api->coordinator_destroy(runtime->coordinator);
  flowie_cluster_owner_directory_destroy(runtime->directory);
  free(runtime->entries);
  if (runtime->conninfo) crypto_wipe(runtime->conninfo, tstr_len(runtime->conninfo));
  tstr_freep(&runtime->conninfo);
  tstr_freep(&runtime->schema_name);
  tstr_freep(&runtime->cluster_id);
  tstr_freep(&runtime->listener_id);
  tstr_freep(&runtime->node_id);
  tstr_freep(&runtime->advertised_endpoint);
  if (runtime->sync_initialized) {
    turbo_cond_destroy(&runtime->changed);
    turbo_mutex_destroy(&runtime->mutex);
  }
  free(runtime);
}

static int flowie_cluster_owner_directory_runtime_retryable(int status) {
  return status == TURBO_EIO || status == TURBO_ETIMEDOUT || status == TURBO_EBUSY;
}

static int flowie_cluster_owner_directory_runtime_cycle(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t generation) {
  flowie_cluster_pgsql_shard_owner_snapshot_t snapshot =
      FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT;
  int snapshot_status = TURBO_OK;
  int rc;
  if (!runtime->coordinator) {
    rc = runtime->api->coordinator_open(&runtime->coordinator_config, &runtime->coordinator);
    if (rc != TURBO_OK) return rc;
  }
  snapshot_status = runtime->api->owner_snapshot(runtime->coordinator, &snapshot);
  rc = snapshot_status;
  if (rc == TURBO_OK && snapshot.owner_count != runtime->shard_count) rc = TURBO_EPROTO;
  for (size_t index = 0u; rc == TURBO_OK && index < snapshot.owner_count; ++index) {
    const flowie_cluster_pgsql_shard_owner_t *owner = &snapshot.owners[index];
    flowie_cluster_owner_directory_entry_t *entry = &runtime->entries[index];
    if (owner->size < sizeof(*owner) || owner->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
        owner->shard_id != index || owner->owner.shard_id != index) {
      rc = TURBO_EPROTO;
      break;
    }
    *entry = (flowie_cluster_owner_directory_entry_t)FLOWIE_CLUSTER_OWNER_DIRECTORY_ENTRY_INIT;
    entry->shard_id = (uint32_t)index;
    entry->local_deadline_ns = owner->local_deadline_ns;
    entry->owner = owner->owner;
  }
  if (rc == TURBO_OK)
    rc = flowie_cluster_owner_directory_replace(runtime->directory, runtime->entries,
                                                runtime->shard_count, generation);
  runtime->api->owner_snapshot_cleanup(&snapshot);
  if (snapshot_status == TURBO_EIO) {
    runtime->api->coordinator_destroy(runtime->coordinator);
    runtime->coordinator = NULL;
  }
  return rc;
}

static void flowie_cluster_owner_directory_runtime_run(void *ctx) {
  flowie_cluster_owner_directory_runtime_t *runtime =
      (flowie_cluster_owner_directory_runtime_t *)ctx;
  for (;;) {
    uint64_t generation;
    uint64_t wait_ns;
    int rc;
    turbo_mutex_lock(&runtime->mutex);
    if (runtime->closing) {
      runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_CLOSED;
      turbo_cond_broadcast(&runtime->changed);
      turbo_mutex_unlock(&runtime->mutex);
      return;
    }
    generation = runtime->refresh_generation;
    turbo_mutex_unlock(&runtime->mutex);
    rc = generation == UINT64_MAX
             ? TURBO_ERANGE
             : flowie_cluster_owner_directory_runtime_cycle(runtime, generation + 1u);
    turbo_mutex_lock(&runtime->mutex);
    runtime->cycle_count++;
    runtime->last_status = rc;
    if (rc == TURBO_OK) {
      runtime->refresh_generation = generation + 1u;
      runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNNING;
      wait_ns = runtime->refresh_interval_ns;
    } else if (flowie_cluster_owner_directory_runtime_retryable(rc)) {
      runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_RETRYING;
      wait_ns = runtime->retry_interval_ns;
    } else {
      runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_FAULTED;
      turbo_cond_broadcast(&runtime->changed);
      while (!runtime->closing)
        turbo_cond_wait(&runtime->changed, &runtime->mutex);
      turbo_mutex_unlock(&runtime->mutex);
      continue;
    }
    turbo_cond_broadcast(&runtime->changed);
    if (!runtime->closing) (void)turbo_cond_timedwait(&runtime->changed, &runtime->mutex, wait_ns);
    turbo_mutex_unlock(&runtime->mutex);
  }
}

int flowie_cluster_owner_directory_runtime_create_with_api(
    const flowie_cluster_owner_directory_runtime_config_t *config,
    const flowie_cluster_owner_directory_runtime_api_t *api,
    flowie_cluster_owner_directory_runtime_t **out) {
  flowie_cluster_owner_directory_runtime_t *runtime;
  flowie_cluster_owner_directory_config_t directory_config =
      FLOWIE_CLUSTER_OWNER_DIRECTORY_CONFIG_INIT;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_owner_directory_runtime_api_validate(api);
  if (rc == TURBO_OK) rc = flowie_cluster_owner_directory_runtime_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  runtime = (flowie_cluster_owner_directory_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  runtime->api = api;
  runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_CREATED;
  runtime->last_status = TURBO_EBUSY;
  turbo_mutex_init(&runtime->mutex);
  turbo_cond_init(&runtime->changed);
  runtime->sync_initialized = 1;
  rc = flowie_cluster_owner_directory_runtime_config_copy(runtime, config);
  if (rc != TURBO_OK) goto fail;
  runtime->entries = (flowie_cluster_owner_directory_entry_t *)calloc(
      runtime->shard_count, sizeof(*runtime->entries));
  if (!runtime->entries) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  directory_config.hash_version = config->hash_version;
  directory_config.shard_count = runtime->shard_count;
  directory_config.cluster_id = tstr_to_v(runtime->cluster_id);
  directory_config.listener_id = tstr_to_v(runtime->listener_id);
  rc = flowie_cluster_owner_directory_create(&directory_config, &runtime->directory);
  if (rc != TURBO_OK) goto fail;
  *out = runtime;
  return TURBO_OK;

fail:
  flowie_cluster_owner_directory_runtime_storage_destroy(runtime);
  return rc;
}

int flowie_cluster_owner_directory_runtime_create(
    const flowie_cluster_owner_directory_runtime_config_t *config,
    flowie_cluster_owner_directory_runtime_t **out) {
  return flowie_cluster_owner_directory_runtime_create_with_api(
      config, &FLOWIE_CLUSTER_OWNER_DIRECTORY_DEFAULT_API, out);
}

int flowie_cluster_owner_directory_runtime_start(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  int rc;
  if (!runtime) return TURBO_EINVAL;
  turbo_mutex_lock(&runtime->mutex);
  if (runtime->state != FLOWIE_CLUSTER_OWNER_DIRECTORY_CREATED || runtime->thread_started) {
    turbo_mutex_unlock(&runtime->mutex);
    return TURBO_EALREADY;
  }
  runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_STARTING;
  turbo_mutex_unlock(&runtime->mutex);
  rc = turbo_thread_create(&runtime->thread, flowie_cluster_owner_directory_runtime_run, runtime);
  turbo_mutex_lock(&runtime->mutex);
  if (rc == TURBO_OK)
    runtime->thread_started = 1;
  else if (runtime->state == FLOWIE_CLUSTER_OWNER_DIRECTORY_STARTING)
    runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_CREATED;
  turbo_mutex_unlock(&runtime->mutex);
  return rc;
}

int flowie_cluster_owner_directory_runtime_wait_ready(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns) {
  uint64_t now_ns;
  uint64_t deadline_ns;
  int infinite;
  int rc;
  if (!runtime || timeout_ns == 0u) return TURBO_EINVAL;
  now_ns = turbo_hrtime();
  infinite = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - now_ns;
  deadline_ns = infinite ? UINT64_MAX : now_ns + timeout_ns;
  turbo_mutex_lock(&runtime->mutex);
  for (;;) {
    if (runtime->refresh_generation != 0u) {
      rc = TURBO_OK;
      break;
    }
    if (runtime->state == FLOWIE_CLUSTER_OWNER_DIRECTORY_FAULTED) {
      rc = runtime->last_status;
      break;
    }
    if (runtime->state == FLOWIE_CLUSTER_OWNER_DIRECTORY_CLOSED || runtime->closing) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    if (!runtime->thread_started && runtime->state != FLOWIE_CLUSTER_OWNER_DIRECTORY_STARTING) {
      rc = TURBO_EBUSY;
      break;
    }
    if (infinite) {
      turbo_cond_wait(&runtime->changed, &runtime->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&runtime->changed, &runtime->mutex, deadline_ns - now_ns);
  }
  turbo_mutex_unlock(&runtime->mutex);
  return rc;
}

flowie_cluster_owner_directory_t *flowie_cluster_owner_directory_runtime_directory(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  return runtime ? runtime->directory : NULL;
}

int flowie_cluster_owner_directory_runtime_snapshot(
    flowie_cluster_owner_directory_runtime_t *runtime,
    flowie_cluster_owner_directory_runtime_snapshot_t *out) {
  if (!runtime || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&runtime->mutex);
  *out = (flowie_cluster_owner_directory_runtime_snapshot_t)
      FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_SNAPSHOT_INIT;
  out->state = runtime->state;
  out->cycle_count = runtime->cycle_count;
  out->refresh_generation = runtime->refresh_generation;
  out->last_status = runtime->last_status;
  turbo_mutex_unlock(&runtime->mutex);
  return TURBO_OK;
}

int flowie_cluster_owner_directory_runtime_close(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns) {
  uint64_t deadline_ns;
  uint64_t now_ns;
  int infinite;
  int rc;
  if (!runtime || timeout_ns == 0u) return TURBO_EINVAL;
  now_ns = turbo_hrtime();
  infinite = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - now_ns;
  deadline_ns = infinite ? UINT64_MAX : now_ns + timeout_ns;
  turbo_mutex_lock(&runtime->mutex);
  if (!runtime->thread_started) {
    runtime->state = FLOWIE_CLUSTER_OWNER_DIRECTORY_CLOSED;
    turbo_mutex_unlock(&runtime->mutex);
    return TURBO_OK;
  }
  runtime->closing = 1;
  turbo_cond_broadcast(&runtime->changed);
  while (runtime->state != FLOWIE_CLUSTER_OWNER_DIRECTORY_CLOSED) {
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
  turbo_mutex_unlock(&runtime->mutex);
  return TURBO_OK;
}

void flowie_cluster_owner_directory_runtime_destroy(
    flowie_cluster_owner_directory_runtime_t *runtime) {
  if (!runtime) return;
  (void)flowie_cluster_owner_directory_runtime_close(runtime, UINT64_MAX);
  flowie_cluster_owner_directory_runtime_storage_destroy(runtime);
}
