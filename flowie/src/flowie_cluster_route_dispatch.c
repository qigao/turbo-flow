#include "flowie_cluster_route_dispatch_internal.h"

#include "turbo_thread.h"

#include <stdlib.h>

struct flowie_cluster_route_dispatcher_s {
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  flowie_cluster_route_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_route_source_fetch_fn fetch;
  flowie_cluster_route_source_settle_fn settle;
  flowie_cluster_route_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_outbox_before_settle_fn project;
  void *project_ctx;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
  flowie_cluster_route_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t projected_events;
  uint64_t settled_events;
  int closing;
  int closed;
  int thread_started;
  int thread_joined;
};

struct flowie_cluster_route_pgsql_source_s {
  flowie_cluster_pgsql_fact_store_t *store;
};

static int flowie_cluster_route_dispatch_retryable(int rc) {
  return rc == TURBO_EBUSY || rc == TURBO_EIO || rc == TURBO_ETIMEDOUT || rc == TURBO_ENOENT ||
         rc == TURBO_ECANCELED || rc == TURBO_ESHUTDOWN;
}

static void flowie_cluster_route_dispatch_status(flowie_cluster_route_dispatcher_t *dispatcher,
                                                 flowie_cluster_route_dispatcher_state_t state,
                                                 int status) {
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing ? FLOWIE_CLUSTER_ROUTE_DISPATCHER_CLOSING : state;
  dispatcher->last_status = status;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int flowie_cluster_route_dispatch_wait(flowie_cluster_route_dispatcher_t *dispatcher,
                                              uint64_t interval_ns) {
  uint64_t start = turbo_hrtime();
  uint64_t deadline = interval_ns > UINT64_MAX - start ? UINT64_MAX : start + interval_ns;
  int rc = TURBO_OK;
  turbo_mutex_lock(&dispatcher->mutex);
  while (!dispatcher->closing) {
    uint64_t now = turbo_hrtime();
    if (now >= deadline) break;
    if (deadline == UINT64_MAX)
      turbo_cond_wait(&dispatcher->changed, &dispatcher->mutex);
    else
      (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline - now);
  }
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int flowie_cluster_route_dispatch_closing(flowie_cluster_route_dispatcher_t *dispatcher) {
  int closing;
  turbo_mutex_lock(&dispatcher->mutex);
  closing = dispatcher->closing;
  turbo_mutex_unlock(&dispatcher->mutex);
  return closing;
}

static int flowie_cluster_route_dispatcher_config_validate(
    const flowie_cluster_route_dispatcher_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_ROUTE_DISPATCH_ABI_V1 ||
      config->max_payload_size == 0u || config->max_payload_size > UINT32_MAX ||
      config->poll_interval_ns == 0u || config->retry_interval_ns == 0u || !config->resolve ||
      !config->fetch || !config->settle || !config->recover || !config->project)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static void flowie_cluster_route_dispatcher_run(void *ctx) {
  flowie_cluster_route_dispatcher_t *dispatcher = (flowie_cluster_route_dispatcher_t *)ctx;
  static const uint64_t event_types[2] = {FLOWIE_CLUSTER_SESSION_EVENT_BOUND,
                                          FLOWIE_CLUSTER_SESSION_EVENT_UPDATED};
  size_t event_index = 0u;
  for (;;) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    int source_recover_needed = 0;
    int rc;
    if (flowie_cluster_route_dispatch_closing(dispatcher)) break;
    flowie_cluster_route_dispatch_status(dispatcher, FLOWIE_CLUSTER_ROUTE_DISPATCHER_FETCHING,
                                         TURBO_OK);
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK) {
      source_recover_needed = 1;
      rc = dispatcher->fetch(dispatcher->source_ctx, &owner, event_types[event_index], &event);
      event_index = (event_index + 1u) % 2u;
    }
    if (rc == TURBO_ENOENT) {
      (void)flowie_cluster_route_dispatch_wait(dispatcher, dispatcher->poll_interval_ns);
      continue;
    }
    if (rc == TURBO_OK) {
      source_recover_needed = 0;
      turbo_mutex_lock(&dispatcher->mutex);
      ++dispatcher->fetched_events;
      turbo_mutex_unlock(&dispatcher->mutex);
      flowie_cluster_route_dispatch_status(dispatcher, FLOWIE_CLUSTER_ROUTE_DISPATCHER_PROJECTING,
                                           TURBO_OK);
      rc = dispatcher->project(dispatcher->project_ctx, &owner, &event);
      if (rc == TURBO_OK || rc == TURBO_EALREADY) {
        turbo_mutex_lock(&dispatcher->mutex);
        ++dispatcher->projected_events;
        turbo_mutex_unlock(&dispatcher->mutex);
        flowie_cluster_route_dispatch_status(dispatcher,
                                             FLOWIE_CLUSTER_ROUTE_DISPATCHER_SETTLING, TURBO_OK);
        source_recover_needed = 1;
        rc = dispatcher->settle(dispatcher->source_ctx, &owner, &event);
        if (rc == TURBO_OK || rc == TURBO_EALREADY) {
          turbo_mutex_lock(&dispatcher->mutex);
          ++dispatcher->settled_events;
          turbo_mutex_unlock(&dispatcher->mutex);
        }
      }
      flowie_cluster_pgsql_outbox_event_cleanup(&event);
    }
    if (rc == TURBO_OK || rc == TURBO_EALREADY) continue;
    if (source_recover_needed && (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT)) {
      int recover_rc = dispatcher->recover(dispatcher->source_ctx);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (!flowie_cluster_route_dispatch_retryable(rc)) {
      flowie_cluster_route_dispatch_status(dispatcher, FLOWIE_CLUSTER_ROUTE_DISPATCHER_CLOSED, rc);
      break;
    }
    flowie_cluster_route_dispatch_status(dispatcher, FLOWIE_CLUSTER_ROUTE_DISPATCHER_RETRY_WAIT, rc);
    if (flowie_cluster_route_dispatch_wait(dispatcher, dispatcher->retry_interval_ns) != TURBO_OK)
      break;
  }
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->closed = 1;
  dispatcher->state = FLOWIE_CLUSTER_ROUTE_DISPATCHER_CLOSED;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

int flowie_cluster_route_dispatcher_create(
    const flowie_cluster_route_dispatcher_config_t *config,
    flowie_cluster_route_dispatcher_t **out) {
  flowie_cluster_route_dispatcher_t *dispatcher;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_route_dispatcher_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  dispatcher = (flowie_cluster_route_dispatcher_t *)calloc(1u, sizeof(*dispatcher));
  if (!dispatcher) return TURBO_ENOMEM;
  turbo_mutex_init(&dispatcher->mutex);
  turbo_cond_init(&dispatcher->changed);
  dispatcher->shard_id = config->shard_id;
  dispatcher->max_payload_size = config->max_payload_size;
  dispatcher->poll_interval_ns = config->poll_interval_ns;
  dispatcher->retry_interval_ns = config->retry_interval_ns;
  dispatcher->resolve = config->resolve;
  dispatcher->resolve_ctx = config->resolve_ctx;
  dispatcher->fetch = config->fetch;
  dispatcher->settle = config->settle;
  dispatcher->recover = config->recover;
  dispatcher->source_ctx = config->source_ctx;
  dispatcher->project = config->project;
  dispatcher->project_ctx = config->project_ctx;
  dispatcher->state = FLOWIE_CLUSTER_ROUTE_DISPATCHER_CREATED;
  dispatcher->last_status = TURBO_OK;
  rc = turbo_thread_create(&dispatcher->thread, flowie_cluster_route_dispatcher_run, dispatcher);
  if (rc != TURBO_OK) {
    turbo_cond_destroy(&dispatcher->changed);
    turbo_mutex_destroy(&dispatcher->mutex);
    free(dispatcher);
    return rc;
  }
  dispatcher->thread_started = 1;
  return (*out = dispatcher), TURBO_OK;
}

int flowie_cluster_route_dispatcher_snapshot(flowie_cluster_route_dispatcher_t *dispatcher,
                                             flowie_cluster_route_dispatcher_snapshot_t *out) {
  flowie_cluster_route_dispatcher_snapshot_t snapshot =
      FLOWIE_CLUSTER_ROUTE_DISPATCHER_SNAPSHOT_INIT;
  if (!dispatcher || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_ROUTE_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  snapshot.state = dispatcher->state;
  snapshot.last_status = dispatcher->last_status;
  snapshot.fetched_events = dispatcher->fetched_events;
  snapshot.projected_events = dispatcher->projected_events;
  snapshot.settled_events = dispatcher->settled_events;
  turbo_mutex_unlock(&dispatcher->mutex);
  *out = snapshot;
  return TURBO_OK;
}

int flowie_cluster_route_dispatcher_close(flowie_cluster_route_dispatcher_t *dispatcher) {
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) {
    turbo_mutex_unlock(&dispatcher->mutex);
    return TURBO_EALREADY;
  }
  dispatcher->closing = 1;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
  return TURBO_OK;
}

int flowie_cluster_route_dispatcher_drain(flowie_cluster_route_dispatcher_t *dispatcher,
                                          uint64_t timeout_ns) {
  uint64_t start;
  uint64_t deadline;
  int closed;
  if (!dispatcher) return TURBO_EINVAL;
  start = turbo_hrtime();
  deadline = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start ? UINT64_MAX : start + timeout_ns;
  turbo_mutex_lock(&dispatcher->mutex);
  while (!dispatcher->closed) {
    uint64_t now = turbo_hrtime();
    if (now >= deadline) break;
    if (deadline == UINT64_MAX)
      turbo_cond_wait(&dispatcher->changed, &dispatcher->mutex);
    else
      (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline - now);
  }
  closed = dispatcher->closed;
  turbo_mutex_unlock(&dispatcher->mutex);
  if (!closed) return TURBO_ETIMEDOUT;
  if (dispatcher->thread_started && !dispatcher->thread_joined) {
    int rc = turbo_thread_join(&dispatcher->thread);
    if (rc != TURBO_OK) return rc;
    turbo_thread_destroy(&dispatcher->thread);
    dispatcher->thread_joined = 1;
    dispatcher->thread_started = 0;
  }
  return TURBO_OK;
}

int flowie_cluster_route_dispatcher_destroy(flowie_cluster_route_dispatcher_t *dispatcher) {
  if (!dispatcher) return TURBO_OK;
  if (!dispatcher->closing || !dispatcher->closed || dispatcher->thread_started)
    return TURBO_EBUSY;
  turbo_cond_destroy(&dispatcher->changed);
  turbo_mutex_destroy(&dispatcher->mutex);
  free(dispatcher);
  return TURBO_OK;
}

int flowie_cluster_route_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                             flowie_cluster_route_pgsql_source_t **out) {
  flowie_cluster_route_pgsql_source_t *source;
  if (out) *out = NULL;
  if (!config || !out || flowie_cluster_pgsql_fact_config_validate(config) != TURBO_OK) return TURBO_EINVAL;
  source = (flowie_cluster_route_pgsql_source_t *)calloc(1u, sizeof(*source));
  if (!source) return TURBO_ENOMEM;
  {
    int rc = flowie_cluster_pgsql_fact_store_open(config, &source->store);
    if (rc != TURBO_OK) {
    free(source);
      return rc;
    }
  }
  *out = source;
  return TURBO_OK;
}

int flowie_cluster_route_pgsql_source_fetch(void *ctx, const flowie_cluster_owner_token_t *owner,
                                            uint64_t event_type,
                                            flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_route_pgsql_source_t *source = (flowie_cluster_route_pgsql_source_t *)ctx;
  return source ? flowie_cluster_pgsql_outbox_next(source->store, owner, event_type, out)
                : TURBO_EINVAL;
}

int flowie_cluster_route_pgsql_source_settle(void *ctx,
                                             const flowie_cluster_owner_token_t *owner,
                                             const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_route_pgsql_source_t *source = (flowie_cluster_route_pgsql_source_t *)ctx;
  return source ? flowie_cluster_pgsql_outbox_settle(source->store, owner, event) : TURBO_EINVAL;
}

int flowie_cluster_route_pgsql_source_recover(void *ctx) {
  flowie_cluster_route_pgsql_source_t *source = (flowie_cluster_route_pgsql_source_t *)ctx;
  return source && source->store ? flowie_cluster_pgsql_fact_store_reopen(&source->store)
                                 : TURBO_EINVAL;
}

void flowie_cluster_route_pgsql_source_destroy(flowie_cluster_route_pgsql_source_t *source) {
  if (!source) return;
  flowie_cluster_pgsql_fact_store_destroy(source->store);
  free(source);
}
