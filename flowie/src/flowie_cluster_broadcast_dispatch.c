#include "flowie_cluster_broadcast_dispatch_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

struct flowie_cluster_broadcast_dispatcher_s {
  flowie_cluster_broadcast_dispatcher_config_t config;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
  flowie_cluster_broadcast_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t publish_attempts;
  uint64_t ack_queries;
  uint64_t settled_events;
  uint64_t source_attempt_count;
  size_t observed_shard_acks;
  int closing;
  int closed;
  int thread_started;
  int thread_joined;
  int joining;
};

struct flowie_cluster_broadcast_pgsql_source_s {
  flowie_cluster_pgsql_fact_store_t *store;
};

int flowie_cluster_broadcast_dispatcher_config_validate(
    const flowie_cluster_broadcast_dispatcher_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_BROADCAST_DISPATCH_ABI_V1 ||
      config->shard_count == 0u || config->shard_id >= config->shard_count ||
      config->max_payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      config->max_payload_size > UINT32_MAX || config->poll_interval_ns == 0u ||
      config->ack_poll_interval_ns == 0u || config->retry_interval_ns == 0u ||
      config->republish_interval_ns == 0u || !config->resolve || !config->fetch ||
      !config->settle || !config->ack_count || !config->recover || !config->publish)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_broadcast_counter(uint64_t *counter) {
  if (*counter == UINT64_MAX) return TURBO_ERANGE;
  ++*counter;
  return TURBO_OK;
}

static int flowie_cluster_broadcast_closing(
    flowie_cluster_broadcast_dispatcher_t *dispatcher) {
  int closing;
  turbo_mutex_lock(&dispatcher->mutex);
  closing = dispatcher->closing;
  turbo_mutex_unlock(&dispatcher->mutex);
  return closing;
}

static void flowie_cluster_broadcast_status(
    flowie_cluster_broadcast_dispatcher_t *dispatcher,
    flowie_cluster_broadcast_dispatcher_state_t state, int status) {
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing ? FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CLOSING : state;
  dispatcher->last_status = status;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int flowie_cluster_broadcast_wait(flowie_cluster_broadcast_dispatcher_t *dispatcher,
                                         uint64_t interval_ns) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = interval_ns > UINT64_MAX - start_ns ? UINT64_MAX : start_ns + interval_ns;
  int rc = TURBO_OK;
  turbo_mutex_lock(&dispatcher->mutex);
  while (!dispatcher->closing) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline_ns - now_ns);
  }
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int flowie_cluster_broadcast_retryable(int status) {
  return status == TURBO_EBUSY || status == TURBO_ENOSPC || status == TURBO_ECANCELED ||
         status == TURBO_ESHUTDOWN || status == TURBO_EIO || status == TURBO_ETIMEDOUT ||
         status == TURBO_ENOENT;
}

static int flowie_cluster_broadcast_source_recover(
    flowie_cluster_broadcast_dispatcher_t *dispatcher, int status) {
  if (status != TURBO_EIO && status != TURBO_ETIMEDOUT) return status;
  return dispatcher->config.recover(dispatcher->config.source_ctx);
}

static int flowie_cluster_broadcast_owner(
    flowie_cluster_broadcast_dispatcher_t *dispatcher, flowie_cluster_owner_token_t *out) {
  int rc = dispatcher->config.resolve(dispatcher->config.resolve_ctx,
                                      dispatcher->config.shard_id, out);
  if (rc == TURBO_OK && out->shard_id != dispatcher->config.shard_id) return TURBO_EPROTO;
  return rc;
}

static int flowie_cluster_broadcast_publish(
    flowie_cluster_broadcast_dispatcher_t *dispatcher, const tstr_t encoded) {
  int rc;
  flowie_cluster_broadcast_status(dispatcher, FLOWIE_CLUSTER_BROADCAST_DISPATCHER_PUBLISHING,
                                  TURBO_OK);
  turbo_mutex_lock(&dispatcher->mutex);
  rc = flowie_cluster_broadcast_counter(&dispatcher->publish_attempts);
  turbo_mutex_unlock(&dispatcher->mutex);
  if (rc != TURBO_OK) return rc;
  return dispatcher->config.publish(dispatcher->config.publish_ctx, encoded, tstr_len(encoded));
}

static int flowie_cluster_broadcast_settle(
    flowie_cluster_broadcast_dispatcher_t *dispatcher,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  int rc;
  flowie_cluster_broadcast_status(dispatcher, FLOWIE_CLUSTER_BROADCAST_DISPATCHER_SETTLING,
                                  TURBO_OK);
  rc = flowie_cluster_broadcast_owner(dispatcher, &owner);
  if (rc == TURBO_OK)
    rc = dispatcher->config.settle(dispatcher->config.source_ctx, &owner, event);
  if (rc == TURBO_OK || rc == TURBO_EALREADY) {
    turbo_mutex_lock(&dispatcher->mutex);
    rc = flowie_cluster_broadcast_counter(&dispatcher->settled_events);
    dispatcher->last_status = rc;
    turbo_cond_broadcast(&dispatcher->changed);
    turbo_mutex_unlock(&dispatcher->mutex);
  }
  return rc;
}

static int flowie_cluster_broadcast_event(
    flowie_cluster_broadcast_dispatcher_t *dispatcher,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_pgsql_event_dedupe_t source = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
  tstr_t encoded = NULL;
  uint64_t last_publish_ns;
  int publish_status;
  int rc = flowie_cluster_broadcast_event_encode(event, dispatcher->config.max_payload_size,
                                                 &encoded);
  if (rc != TURBO_OK) return rc;
  memcpy(source.source_command_id, event->command_id, sizeof(source.source_command_id));
  source.event_index = event->event_index;
  source.source_shard_id = event->shard_id;
  source.source_owner_epoch = event->event_owner_epoch;
  source.source_fact_revision = event->fact_revision;
  rc = flowie_cluster_broadcast_event_digest(encoded, tstr_len(encoded),
                                             dispatcher->config.max_payload_size,
                                             source.event_digest);
  if (rc != TURBO_OK) goto cleanup;
  publish_status = flowie_cluster_broadcast_publish(dispatcher, encoded);
  last_publish_ns = turbo_hrtime();
  if (publish_status != TURBO_OK && !flowie_cluster_broadcast_retryable(publish_status)) {
    rc = publish_status;
    goto cleanup;
  }
  for (;;) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    size_t ack_count = 0u;
    uint64_t now_ns;
    if (flowie_cluster_broadcast_closing(dispatcher)) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    flowie_cluster_broadcast_status(dispatcher,
                                    FLOWIE_CLUSTER_BROADCAST_DISPATCHER_WAITING_ACK,
                                    publish_status);
    rc = flowie_cluster_broadcast_owner(dispatcher, &owner);
    if (rc == TURBO_OK) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_broadcast_counter(&dispatcher->ack_queries);
      turbo_mutex_unlock(&dispatcher->mutex);
    }
    if (rc == TURBO_OK)
      rc = dispatcher->config.ack_count(dispatcher->config.source_ctx, &source, &ack_count);
    if (rc == TURBO_OK && ack_count > dispatcher->config.shard_count) rc = TURBO_EPROTO;
    if (rc == TURBO_OK) {
      turbo_mutex_lock(&dispatcher->mutex);
      dispatcher->observed_shard_acks = ack_count;
      turbo_cond_broadcast(&dispatcher->changed);
      turbo_mutex_unlock(&dispatcher->mutex);
      if (ack_count == dispatcher->config.shard_count) {
        rc = flowie_cluster_broadcast_settle(dispatcher, event);
        if (rc == TURBO_OK) break;
      }
    }
    if (rc == TURBO_EBUSY) break;
    if (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT) {
      int recover_rc = flowie_cluster_broadcast_source_recover(dispatcher, rc);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (rc != TURBO_OK && !flowie_cluster_broadcast_retryable(rc)) break;
    flowie_cluster_broadcast_status(dispatcher,
                                    FLOWIE_CLUSTER_BROADCAST_DISPATCHER_RETRY_WAIT, rc);
    if (flowie_cluster_broadcast_wait(dispatcher, dispatcher->config.ack_poll_interval_ns) !=
        TURBO_OK) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    now_ns = turbo_hrtime();
    if (now_ns - last_publish_ns >= dispatcher->config.republish_interval_ns) {
      publish_status = flowie_cluster_broadcast_publish(dispatcher, encoded);
      last_publish_ns = turbo_hrtime();
      if (publish_status != TURBO_OK && !flowie_cluster_broadcast_retryable(publish_status)) {
        rc = publish_status;
        break;
      }
    }
  }
cleanup:
  tstr_free(encoded);
  return rc;
}

static void flowie_cluster_broadcast_run(void *ctx) {
  flowie_cluster_broadcast_dispatcher_t *dispatcher =
      (flowie_cluster_broadcast_dispatcher_t *)ctx;
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  int fatal_status = TURBO_OK;
  while (!flowie_cluster_broadcast_closing(dispatcher)) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    int rc;
    flowie_cluster_broadcast_status(dispatcher, FLOWIE_CLUSTER_BROADCAST_DISPATCHER_FETCHING,
                                    TURBO_OK);
    rc = flowie_cluster_broadcast_owner(dispatcher, &owner);
    if (rc == TURBO_OK)
      rc = dispatcher->config.fetch(dispatcher->config.source_ctx, &owner, &event);
    if (rc == TURBO_OK) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_broadcast_counter(&dispatcher->fetched_events);
      if (rc == TURBO_OK) {
        dispatcher->source_attempt_count = event.attempt_count;
        dispatcher->observed_shard_acks = 0u;
      }
      turbo_mutex_unlock(&dispatcher->mutex);
      if (rc == TURBO_OK) rc = flowie_cluster_broadcast_event(dispatcher, &event);
      flowie_cluster_pgsql_outbox_event_cleanup(&event);
      if (rc == TURBO_OK || rc == TURBO_EBUSY) continue;
    } else if (rc == TURBO_ENOENT) {
      if (flowie_cluster_broadcast_wait(dispatcher, dispatcher->config.poll_interval_ns) ==
          TURBO_OK)
        continue;
      rc = TURBO_ESHUTDOWN;
    } else if (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT) {
      int recover_rc = flowie_cluster_broadcast_source_recover(dispatcher, rc);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (rc == TURBO_ESHUTDOWN && flowie_cluster_broadcast_closing(dispatcher)) break;
    if (flowie_cluster_broadcast_retryable(rc)) {
      flowie_cluster_broadcast_status(dispatcher,
                                      FLOWIE_CLUSTER_BROADCAST_DISPATCHER_RETRY_WAIT, rc);
      if (flowie_cluster_broadcast_wait(dispatcher, dispatcher->config.retry_interval_ns) ==
          TURBO_OK)
        continue;
      break;
    }
    fatal_status = rc;
    break;
  }
  flowie_cluster_pgsql_outbox_event_cleanup(&event);
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->closing = 1;
  if (fatal_status != TURBO_OK) dispatcher->last_status = fatal_status;
  dispatcher->closed = 1;
  dispatcher->state = FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CLOSED;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

int flowie_cluster_broadcast_dispatcher_create(
    const flowie_cluster_broadcast_dispatcher_config_t *config,
    flowie_cluster_broadcast_dispatcher_t **out) {
  flowie_cluster_broadcast_dispatcher_t *dispatcher;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_broadcast_dispatcher_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  dispatcher = (flowie_cluster_broadcast_dispatcher_t *)calloc(1u, sizeof(*dispatcher));
  if (!dispatcher) return TURBO_ENOMEM;
  dispatcher->config = *config;
  dispatcher->state = FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CREATED;
  dispatcher->last_status = TURBO_OK;
  turbo_mutex_init(&dispatcher->mutex);
  turbo_cond_init(&dispatcher->changed);
  rc = turbo_thread_create(&dispatcher->thread, flowie_cluster_broadcast_run, dispatcher);
  if (rc != TURBO_OK) {
    turbo_cond_destroy(&dispatcher->changed);
    turbo_mutex_destroy(&dispatcher->mutex);
    free(dispatcher);
    return rc;
  }
  dispatcher->thread_started = 1;
  *out = dispatcher;
  return TURBO_OK;
}

int flowie_cluster_broadcast_dispatcher_snapshot(
    flowie_cluster_broadcast_dispatcher_t *dispatcher,
    flowie_cluster_broadcast_dispatcher_snapshot_t *out) {
  if (!dispatcher || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_BROADCAST_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  out->state = dispatcher->state;
  out->last_status = dispatcher->last_status;
  out->fetched_events = dispatcher->fetched_events;
  out->publish_attempts = dispatcher->publish_attempts;
  out->ack_queries = dispatcher->ack_queries;
  out->settled_events = dispatcher->settled_events;
  out->source_attempt_count = dispatcher->source_attempt_count;
  out->observed_shard_acks = dispatcher->observed_shard_acks;
  turbo_mutex_unlock(&dispatcher->mutex);
  return TURBO_OK;
}

int flowie_cluster_broadcast_dispatcher_close(flowie_cluster_broadcast_dispatcher_t *dispatcher) {
  int rc = TURBO_OK;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_EALREADY;
  else {
    dispatcher->closing = 1;
    dispatcher->state = FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CLOSING;
    turbo_cond_broadcast(&dispatcher->changed);
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

int flowie_cluster_broadcast_dispatcher_drain(flowie_cluster_broadcast_dispatcher_t *dispatcher,
                                              uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int join_thread = 0;
  int rc = TURBO_OK;
  if (!dispatcher) return TURBO_EINVAL;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  turbo_mutex_lock(&dispatcher->mutex);
  if (!dispatcher->closing) rc = TURBO_EBUSY;
  while (rc == TURBO_OK && (!dispatcher->closed || dispatcher->joining)) {
    uint64_t now_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&dispatcher->changed, &dispatcher->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) {
      rc = timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline_ns - now_ns);
  }
  if (rc == TURBO_OK && dispatcher->thread_started && !dispatcher->thread_joined) {
    dispatcher->joining = 1;
    join_thread = 1;
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  if (join_thread) {
    rc = turbo_thread_join(&dispatcher->thread);
    turbo_thread_destroy(&dispatcher->thread);
    turbo_mutex_lock(&dispatcher->mutex);
    dispatcher->joining = 0;
    dispatcher->thread_started = 0;
    dispatcher->thread_joined = rc == TURBO_OK;
    turbo_cond_broadcast(&dispatcher->changed);
    turbo_mutex_unlock(&dispatcher->mutex);
  }
  return rc;
}

int flowie_cluster_broadcast_dispatcher_destroy(
    flowie_cluster_broadcast_dispatcher_t *dispatcher) {
  int ready;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  ready = dispatcher->closing && dispatcher->closed && !dispatcher->thread_started &&
          dispatcher->thread_joined && !dispatcher->joining;
  turbo_mutex_unlock(&dispatcher->mutex);
  if (!ready) return TURBO_EBUSY;
  turbo_cond_destroy(&dispatcher->changed);
  turbo_mutex_destroy(&dispatcher->mutex);
  free(dispatcher);
  return TURBO_OK;
}

int flowie_cluster_broadcast_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                 flowie_cluster_broadcast_pgsql_source_t **out) {
  flowie_cluster_broadcast_pgsql_source_t *source;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  source = (flowie_cluster_broadcast_pgsql_source_t *)calloc(1u, sizeof(*source));
  if (!source) return TURBO_ENOMEM;
  rc = flowie_cluster_pgsql_fact_store_open(config, &source->store);
  if (rc != TURBO_OK) {
    free(source);
    return rc;
  }
  *out = source;
  return TURBO_OK;
}

int flowie_cluster_broadcast_pgsql_source_fetch(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_broadcast_pgsql_source_t *source =
      (flowie_cluster_broadcast_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_next(source->store, current_owner,
                                                    FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE, out);
}

int flowie_cluster_broadcast_pgsql_source_settle(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_broadcast_pgsql_source_t *source =
      (flowie_cluster_broadcast_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_settle(source->store, current_owner, event);
}

int flowie_cluster_broadcast_pgsql_source_ack_count(
    void *ctx, const flowie_cluster_pgsql_event_dedupe_t *source_event, size_t *out_count) {
  flowie_cluster_broadcast_pgsql_source_t *source =
      (flowie_cluster_broadcast_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_event_ack_count(source->store, source_event, out_count);
}

int flowie_cluster_broadcast_pgsql_source_recover(void *ctx) {
  flowie_cluster_broadcast_pgsql_source_t *source =
      (flowie_cluster_broadcast_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL : flowie_cluster_pgsql_fact_store_reopen(&source->store);
}

void flowie_cluster_broadcast_pgsql_source_destroy(
    flowie_cluster_broadcast_pgsql_source_t *source) {
  if (!source) return;
  flowie_cluster_pgsql_fact_store_destroy(source->store);
  free(source);
}
