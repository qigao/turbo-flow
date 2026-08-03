#include "flowie_cluster_pgsql_internal.h"

#include "monocypher.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLUSTER_PGSQL_NS_PER_MS UINT64_C(1000000)

struct flowie_cluster_pgsql_lease_worker_s {
  flowie_cluster_pgsql_config_t config;
  tstr_t conninfo;
  tstr_t schema_name;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t node_id;
  tstr_t advertised_endpoint;
  flowie_cluster_pgsql_coordinator_t *coordinator;
  flowie_cluster_owner_token_t owner;
  uint64_t local_deadline_ns;
  uint32_t shard_id;
  flowie_cluster_shard_state_t state;
  int last_coordinator_status;
  int close_status;
  int closing;
  int closed;
  int sync_initialized;
  int thread_started;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
};

static void
flowie_cluster_pgsql_lease_worker_storage_destroy(flowie_cluster_pgsql_lease_worker_t *worker) {
  if (!worker) return;
  if (worker->thread_started) {
    (void)turbo_thread_join(&worker->thread);
    turbo_thread_destroy(&worker->thread);
  }
  flowie_cluster_pgsql_coordinator_destroy(worker->coordinator);
  if (worker->conninfo) crypto_wipe(worker->conninfo, tstr_len(worker->conninfo));
  tstr_freep(&worker->conninfo);
  tstr_freep(&worker->schema_name);
  tstr_freep(&worker->cluster_id);
  tstr_freep(&worker->listener_id);
  tstr_freep(&worker->node_id);
  tstr_freep(&worker->advertised_endpoint);
  if (worker->sync_initialized) {
    turbo_cond_destroy(&worker->changed);
    turbo_mutex_destroy(&worker->mutex);
  }
  free(worker);
}

static int
flowie_cluster_pgsql_lease_worker_config_copy(flowie_cluster_pgsql_lease_worker_t *worker,
                                              const flowie_cluster_pgsql_config_t *config) {
  worker->conninfo = tstr_dup(config->conninfo);
  worker->schema_name = tstr_dup(config->schema_name);
  worker->cluster_id = tstr_dup(config->cluster_id);
  worker->listener_id = tstr_dup(config->listener_id);
  worker->node_id = tstr_dup(config->node_id);
  worker->advertised_endpoint = tstr_dup(config->advertised_endpoint);
  if (!worker->conninfo || !worker->schema_name || !worker->cluster_id || !worker->listener_id ||
      !worker->node_id || !worker->advertised_endpoint)
    return TURBO_ENOMEM;
  worker->config = *config;
  worker->config.conninfo = worker->conninfo;
  worker->config.schema_name = worker->schema_name;
  worker->config.cluster_id = worker->cluster_id;
  worker->config.listener_id = worker->listener_id;
  worker->config.node_id = worker->node_id;
  worker->config.advertised_endpoint = worker->advertised_endpoint;
  return TURBO_OK;
}

static int
flowie_cluster_pgsql_lease_worker_reopen(flowie_cluster_pgsql_lease_worker_t *worker,
                                         flowie_cluster_pgsql_coordinator_t **out_coordinator) {
  flowie_cluster_pgsql_config_t reopen_config;
  if (out_coordinator) *out_coordinator = NULL;
  if (!worker || !out_coordinator) return TURBO_EINVAL;
  reopen_config = worker->config;
  reopen_config.create_schema = 0;
  return flowie_cluster_pgsql_coordinator_open(&reopen_config, out_coordinator);
}

static void flowie_cluster_pgsql_lease_worker_run(void *ctx) {
  flowie_cluster_pgsql_lease_worker_t *worker = (flowie_cluster_pgsql_lease_worker_t *)ctx;
  flowie_cluster_pgsql_coordinator_t *coordinator;
  flowie_cluster_owner_token_t owner;
  uint64_t previous_deadline;
  int release_status = TURBO_OK;
  if (!worker) return;
  for (;;) {
    uint64_t wait_ms;
    uint64_t renewed_deadline = 0u;
    uint64_t request_start;
    int rc;

    turbo_mutex_lock(&worker->mutex);
    if (worker->closing) {
      coordinator = worker->coordinator;
      worker->coordinator = NULL;
      owner = worker->owner;
      turbo_mutex_unlock(&worker->mutex);
      if (coordinator && owner.owner_epoch != 0u)
        release_status = flowie_cluster_pgsql_shard_release(coordinator, &owner);
      flowie_cluster_pgsql_coordinator_destroy(coordinator);
      turbo_mutex_lock(&worker->mutex);
      worker->close_status = release_status;
      worker->closed = 1;
      turbo_cond_broadcast(&worker->changed);
      turbo_mutex_unlock(&worker->mutex);
      return;
    }
    if (worker->state == FLOWIE_CLUSTER_SHARD_FENCED) {
      turbo_cond_wait(&worker->changed, &worker->mutex);
      turbo_mutex_unlock(&worker->mutex);
      continue;
    }
    wait_ms =
        worker->coordinator ? worker->config.renew_interval_ms : worker->config.retry_interval_ms;
    (void)turbo_cond_timedwait(&worker->changed, &worker->mutex,
                               wait_ms * FLOWIE_CLUSTER_PGSQL_NS_PER_MS);
    if (worker->closing) {
      turbo_mutex_unlock(&worker->mutex);
      continue;
    }
    owner = worker->owner;
    previous_deadline = worker->local_deadline_ns;
    coordinator = worker->coordinator;
    worker->coordinator = NULL;
    turbo_mutex_unlock(&worker->mutex);

    request_start = turbo_hrtime();
    if (!coordinator) {
      rc = flowie_cluster_pgsql_lease_worker_reopen(worker, &coordinator);
      if (rc == TURBO_OK)
        rc = flowie_cluster_pgsql_shard_require(coordinator, &owner, request_start,
                                                &renewed_deadline);
    } else {
      rc = TURBO_OK;
    }
    if (rc == TURBO_OK) {
      request_start = turbo_hrtime();
      rc = flowie_cluster_pgsql_shard_renew(coordinator, &owner, request_start, &renewed_deadline);
    }

    turbo_mutex_lock(&worker->mutex);
    if (worker->closing) {
      worker->coordinator = coordinator;
      turbo_mutex_unlock(&worker->mutex);
      continue;
    }
    if (rc == TURBO_OK && turbo_hrtime() < previous_deadline) {
      worker->coordinator = coordinator;
      worker->local_deadline_ns = renewed_deadline;
      worker->last_coordinator_status = TURBO_OK;
    } else {
      flowie_cluster_pgsql_coordinator_destroy(coordinator);
      coordinator = NULL;
      worker->last_coordinator_status = rc == TURBO_OK ? TURBO_ETIMEDOUT : rc;
      if (turbo_hrtime() >= previous_deadline ||
          (rc != TURBO_EIO && rc != TURBO_ETIMEDOUT && rc != TURBO_EBUSY))
        worker->state = FLOWIE_CLUSTER_SHARD_FENCED;
      else if (rc == TURBO_EBUSY) worker->state = FLOWIE_CLUSTER_SHARD_FENCED;
    }
    turbo_cond_broadcast(&worker->changed);
    turbo_mutex_unlock(&worker->mutex);
  }
}

int flowie_cluster_pgsql_lease_worker_create(const flowie_cluster_pgsql_config_t *config,
                                             uint32_t shard_id,
                                             flowie_cluster_pgsql_lease_worker_t **out) {
  flowie_cluster_pgsql_lease_worker_t *worker;
  uint64_t request_start;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_pgsql_config_validate(config);
  if (rc != TURBO_OK || !out || shard_id >= config->shard_count)
    return rc == TURBO_OK ? TURBO_EINVAL : rc;
  if (config->renew_interval_ms > UINT64_MAX / FLOWIE_CLUSTER_PGSQL_NS_PER_MS ||
      config->retry_interval_ms > UINT64_MAX / FLOWIE_CLUSTER_PGSQL_NS_PER_MS)
    return TURBO_ERANGE;
  worker = (flowie_cluster_pgsql_lease_worker_t *)calloc(1u, sizeof(*worker));
  if (!worker) return TURBO_ENOMEM;
  turbo_mutex_init(&worker->mutex);
  turbo_cond_init(&worker->changed);
  worker->sync_initialized = 1;
  worker->shard_id = shard_id;
  worker->state = FLOWIE_CLUSTER_SHARD_CLAIMING;
  worker->last_coordinator_status = TURBO_EBUSY;
  rc = flowie_cluster_pgsql_lease_worker_config_copy(worker, config);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_pgsql_coordinator_open(&worker->config, &worker->coordinator);
  if (rc != TURBO_OK) goto fail;
  request_start = turbo_hrtime();
  rc = flowie_cluster_pgsql_shard_claim(worker->coordinator, shard_id, request_start,
                                        &worker->owner, &worker->local_deadline_ns);
  if (rc != TURBO_OK) goto fail;
  worker->state = FLOWIE_CLUSTER_SHARD_RECOVERING;
  worker->last_coordinator_status = TURBO_OK;
  rc = turbo_thread_create(&worker->thread, flowie_cluster_pgsql_lease_worker_run, worker);
  if (rc != TURBO_OK) goto fail;
  worker->thread_started = 1;
  *out = worker;
  return TURBO_OK;

fail:
  if (worker->coordinator && worker->owner.owner_epoch != 0u)
    (void)flowie_cluster_pgsql_shard_release(worker->coordinator, &worker->owner);
  flowie_cluster_pgsql_lease_worker_storage_destroy(worker);
  return rc;
}

int flowie_cluster_pgsql_lease_worker_snapshot(flowie_cluster_pgsql_lease_worker_t *worker,
                                               flowie_cluster_pgsql_lease_snapshot_t *out) {
  uint64_t now;
  if (!worker || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1)
    return TURBO_EINVAL;
  now = turbo_hrtime();
  turbo_mutex_lock(&worker->mutex);
  *out = (flowie_cluster_pgsql_lease_snapshot_t)FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
  out->state = worker->state;
  out->owner = worker->owner;
  out->local_deadline_ns = worker->local_deadline_ns;
  out->last_coordinator_status = worker->last_coordinator_status;
  if ((out->state == FLOWIE_CLUSTER_SHARD_RECOVERING ||
       out->state == FLOWIE_CLUSTER_SHARD_ACTIVE) &&
      now >= out->local_deadline_ns)
    out->state = FLOWIE_CLUSTER_SHARD_FENCED;
  turbo_mutex_unlock(&worker->mutex);
  return TURBO_OK;
}

int flowie_cluster_pgsql_lease_worker_activate(
    flowie_cluster_pgsql_lease_worker_t *worker,
    const flowie_cluster_owner_token_t *recovered_owner) {
  int rc;
  if (!worker || !recovered_owner) return TURBO_EINVAL;
  turbo_mutex_lock(&worker->mutex);
  if (worker->closing || worker->closed) {
    rc = TURBO_ESHUTDOWN;
  } else if (worker->state != FLOWIE_CLUSTER_SHARD_RECOVERING ||
             turbo_hrtime() >= worker->local_deadline_ns) {
    rc = TURBO_EBUSY;
  } else {
    rc = flowie_cluster_owner_token_require(&worker->owner, recovered_owner);
    if (rc == TURBO_OK) worker->state = FLOWIE_CLUSTER_SHARD_ACTIVE;
  }
  turbo_mutex_unlock(&worker->mutex);
  return rc;
}

int flowie_cluster_pgsql_lease_worker_close(flowie_cluster_pgsql_lease_worker_t *worker) {
  int rc;
  if (!worker) return TURBO_EINVAL;
  turbo_mutex_lock(&worker->mutex);
  if (!worker->thread_started) {
    turbo_mutex_unlock(&worker->mutex);
    return TURBO_EALREADY;
  }
  worker->closing = 1;
  turbo_cond_broadcast(&worker->changed);
  turbo_mutex_unlock(&worker->mutex);
  (void)turbo_thread_join(&worker->thread);
  turbo_thread_destroy(&worker->thread);
  worker->thread_started = 0;
  turbo_mutex_lock(&worker->mutex);
  rc = worker->close_status;
  turbo_mutex_unlock(&worker->mutex);
  return rc;
}

void flowie_cluster_pgsql_lease_worker_destroy(flowie_cluster_pgsql_lease_worker_t *worker) {
  if (!worker) return;
  if (worker->thread_started) (void)flowie_cluster_pgsql_lease_worker_close(worker);
  flowie_cluster_pgsql_lease_worker_storage_destroy(worker);
}
