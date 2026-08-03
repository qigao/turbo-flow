#include "flowie_cluster_broadcast_target_dispatch_internal.h"

#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

struct flowie_cluster_broadcast_target_dispatcher_s {
  flowie_cluster_broadcast_target_dispatcher_config_t config;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
  flowie_cluster_broadcast_target_dispatcher_state_t state;
  int last_status;
  int apply_inflight;
  int apply_status;
  int closing;
  int closed;
  int thread_started;
  int thread_joined;
  int joining;
  uint64_t claimed_events;
  uint64_t apply_attempts;
  uint64_t requeued_events;
  uint64_t ack_attempts;
  uint64_t acknowledged_events;
  uint64_t active_token;
};

int flowie_cluster_broadcast_target_dispatcher_config_validate(
    const flowie_cluster_broadcast_target_dispatcher_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1 ||
      config->max_payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      config->max_payload_size > UINT32_MAX || config->poll_interval_ns == 0u ||
      config->retry_interval_ns == 0u || config->ack_retry_interval_ns == 0u ||
      !config->claim || !config->ack || !config->requeue || !config->apply)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_broadcast_target_counter(uint64_t *counter) {
  if (!counter || *counter == UINT64_MAX) return TURBO_ERANGE;
  ++*counter;
  return TURBO_OK;
}

static int flowie_cluster_broadcast_target_closing(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher) {
  int closing;
  turbo_mutex_lock(&dispatcher->mutex);
  closing = dispatcher->closing;
  turbo_mutex_unlock(&dispatcher->mutex);
  return closing;
}

static void flowie_cluster_broadcast_target_status(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher,
    flowie_cluster_broadcast_target_dispatcher_state_t state, int status) {
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing
                          ? FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSING
                          : state;
  dispatcher->last_status = status;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int flowie_cluster_broadcast_target_wait(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, uint64_t interval_ns,
    int interrupt_on_close) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = interval_ns > UINT64_MAX - start_ns ? UINT64_MAX : start_ns + interval_ns;
  int rc = TURBO_OK;
  turbo_mutex_lock(&dispatcher->mutex);
  while (!interrupt_on_close || !dispatcher->closing) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline_ns - now_ns);
  }
  if (interrupt_on_close && dispatcher->closing) rc = TURBO_ESHUTDOWN;
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int flowie_cluster_broadcast_target_retryable(int status) {
  return status == TURBO_EBUSY || status == TURBO_ENOSPC || status == TURBO_ECANCELED ||
         status == TURBO_ESHUTDOWN || status == TURBO_EIO || status == TURBO_ETIMEDOUT ||
         status == TURBO_ENOENT;
}

static int flowie_cluster_broadcast_target_ack_uncertain(int status) {
  return status == TURBO_EIO || status == TURBO_ETIMEDOUT || status == TURBO_ECANCELED;
}

static void flowie_cluster_broadcast_target_apply_complete(void *ctx, int status) {
  flowie_cluster_broadcast_target_dispatcher_t *dispatcher =
      (flowie_cluster_broadcast_target_dispatcher_t *)ctx;
  if (!dispatcher) return;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->apply_inflight) {
    dispatcher->apply_inflight = 0;
    dispatcher->apply_status = status;
  } else if (dispatcher->last_status == TURBO_OK) {
    dispatcher->last_status = TURBO_EPROTO;
  }
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int flowie_cluster_broadcast_target_apply_wait(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, tstr_v payload) {
  int apply_rc;
  int rc;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->apply_inflight) {
    turbo_mutex_unlock(&dispatcher->mutex);
    return TURBO_EPROTO;
  }
  rc = flowie_cluster_broadcast_target_counter(&dispatcher->apply_attempts);
  if (rc == TURBO_OK) {
    dispatcher->apply_inflight = 1;
    dispatcher->apply_status = TURBO_EBUSY;
    dispatcher->state = FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_APPLYING;
    dispatcher->last_status = TURBO_OK;
    turbo_cond_broadcast(&dispatcher->changed);
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  if (rc != TURBO_OK) return rc;
  apply_rc = dispatcher->config.apply(
      dispatcher->config.apply_ctx, payload.data, payload.len,
      flowie_cluster_broadcast_target_apply_complete, dispatcher);
  turbo_mutex_lock(&dispatcher->mutex);
  if (apply_rc != TURBO_OK) {
    if (!dispatcher->apply_inflight) rc = TURBO_EPROTO;
    else {
      dispatcher->apply_inflight = 0;
      rc = apply_rc;
    }
    turbo_cond_broadcast(&dispatcher->changed);
  } else {
    while (dispatcher->apply_inflight)
      turbo_cond_wait(&dispatcher->changed, &dispatcher->mutex);
    rc = dispatcher->apply_status;
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int flowie_cluster_broadcast_target_ack(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, uint64_t token) {
  for (;;) {
    int rc;
    flowie_cluster_broadcast_target_status(
        dispatcher, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_ACKING, TURBO_OK);
    turbo_mutex_lock(&dispatcher->mutex);
    rc = flowie_cluster_broadcast_target_counter(&dispatcher->ack_attempts);
    turbo_mutex_unlock(&dispatcher->mutex);
    if (rc != TURBO_OK) return rc;
    rc = dispatcher->config.ack(dispatcher->config.transport_ctx, token);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_broadcast_target_counter(&dispatcher->acknowledged_events);
      if (rc == TURBO_OK) dispatcher->active_token = 0u;
      dispatcher->last_status = rc;
      turbo_cond_broadcast(&dispatcher->changed);
      turbo_mutex_unlock(&dispatcher->mutex);
      return rc;
    }
    if (!flowie_cluster_broadcast_target_ack_uncertain(rc)) return rc;
    flowie_cluster_broadcast_target_status(
        dispatcher, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_RETRY_WAIT, rc);
    (void)flowie_cluster_broadcast_target_wait(dispatcher,
                                               dispatcher->config.ack_retry_interval_ns, 0);
  }
}

static int flowie_cluster_broadcast_target_requeue(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, uint64_t token) {
  for (;;) {
    int rc;
    flowie_cluster_broadcast_target_status(
        dispatcher, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_REQUEUEING, TURBO_OK);
    rc = dispatcher->config.requeue(dispatcher->config.transport_ctx, token);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_broadcast_target_counter(&dispatcher->requeued_events);
      if (rc == TURBO_OK) dispatcher->active_token = 0u;
      dispatcher->last_status = rc;
      turbo_cond_broadcast(&dispatcher->changed);
      turbo_mutex_unlock(&dispatcher->mutex);
      return rc;
    }
    if (!flowie_cluster_broadcast_target_ack_uncertain(rc)) return rc;
    flowie_cluster_broadcast_target_status(
        dispatcher, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_RETRY_WAIT, rc);
    (void)flowie_cluster_broadcast_target_wait(dispatcher,
                                               dispatcher->config.retry_interval_ns, 0);
  }
}

static int flowie_cluster_broadcast_target_claim_validate(
    const flowie_cluster_broadcast_target_dispatcher_t *dispatcher,
    const flowie_cluster_broadcast_target_claim_t *claim) {
  if (!dispatcher || !claim || claim->size != sizeof(*claim) ||
      claim->abi_version != FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1 ||
      claim->token == 0u || !claim->payload.data ||
      claim->payload.len <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE)
    return TURBO_EPROTO;
  return claim->payload.len > dispatcher->config.max_payload_size ? TURBO_EMSGSIZE : TURBO_OK;
}

static void flowie_cluster_broadcast_target_run(void *ctx) {
  flowie_cluster_broadcast_target_dispatcher_t *dispatcher =
      (flowie_cluster_broadcast_target_dispatcher_t *)ctx;
  int fatal_status = TURBO_OK;
  while (!flowie_cluster_broadcast_target_closing(dispatcher)) {
    flowie_cluster_broadcast_target_claim_t claim = FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT;
    int rc;
    flowie_cluster_broadcast_target_status(
        dispatcher, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLAIMING, TURBO_OK);
    rc = dispatcher->config.claim(dispatcher->config.transport_ctx, &claim);
    if (rc == TURBO_OK) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_broadcast_target_counter(&dispatcher->claimed_events);
      if (rc == TURBO_OK) dispatcher->active_token = claim.token;
      turbo_mutex_unlock(&dispatcher->mutex);
      if (rc == TURBO_OK) rc = flowie_cluster_broadcast_target_claim_validate(dispatcher, &claim);
      if (rc == TURBO_OK) rc = flowie_cluster_broadcast_target_apply_wait(dispatcher, claim.payload);
      if (rc == TURBO_OK) rc = flowie_cluster_broadcast_target_ack(dispatcher, claim.token);
      else if (flowie_cluster_broadcast_target_retryable(rc))
        rc = flowie_cluster_broadcast_target_requeue(dispatcher, claim.token);
      if (rc == TURBO_OK) continue;
    } else if (rc == TURBO_ENOENT) {
      if (flowie_cluster_broadcast_target_wait(dispatcher, dispatcher->config.poll_interval_ns, 1) ==
          TURBO_OK)
        continue;
      rc = TURBO_ESHUTDOWN;
    }
    if (rc == TURBO_ESHUTDOWN && flowie_cluster_broadcast_target_closing(dispatcher)) break;
    if (flowie_cluster_broadcast_target_retryable(rc)) {
      flowie_cluster_broadcast_target_status(
          dispatcher, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_RETRY_WAIT, rc);
      if (flowie_cluster_broadcast_target_wait(dispatcher, dispatcher->config.retry_interval_ns, 1) ==
          TURBO_OK)
        continue;
      break;
    }
    fatal_status = rc;
    break;
  }
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->closing = 1;
  if (fatal_status != TURBO_OK) dispatcher->last_status = fatal_status;
  dispatcher->closed = 1;
  dispatcher->state = FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSED;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static void flowie_cluster_broadcast_target_storage_destroy(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher) {
  if (!dispatcher) return;
  turbo_cond_destroy(&dispatcher->changed);
  turbo_mutex_destroy(&dispatcher->mutex);
  free(dispatcher);
}

int flowie_cluster_broadcast_target_dispatcher_create(
    const flowie_cluster_broadcast_target_dispatcher_config_t *config,
    flowie_cluster_broadcast_target_dispatcher_t **out) {
  flowie_cluster_broadcast_target_dispatcher_t *dispatcher;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_broadcast_target_dispatcher_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  dispatcher = (flowie_cluster_broadcast_target_dispatcher_t *)calloc(1u, sizeof(*dispatcher));
  if (!dispatcher) return TURBO_ENOMEM;
  dispatcher->config = *config;
  dispatcher->state = FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CREATED;
  dispatcher->last_status = TURBO_OK;
  turbo_mutex_init(&dispatcher->mutex);
  turbo_cond_init(&dispatcher->changed);
  rc = turbo_thread_create(&dispatcher->thread, flowie_cluster_broadcast_target_run, dispatcher);
  if (rc != TURBO_OK) {
    flowie_cluster_broadcast_target_storage_destroy(dispatcher);
    return rc;
  }
  dispatcher->thread_started = 1;
  *out = dispatcher;
  return TURBO_OK;
}

int flowie_cluster_broadcast_target_dispatcher_snapshot(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher,
    flowie_cluster_broadcast_target_dispatcher_snapshot_t *out) {
  if (!dispatcher || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  out->state = dispatcher->state;
  out->last_status = dispatcher->last_status;
  out->claimed_events = dispatcher->claimed_events;
  out->apply_attempts = dispatcher->apply_attempts;
  out->requeued_events = dispatcher->requeued_events;
  out->ack_attempts = dispatcher->ack_attempts;
  out->acknowledged_events = dispatcher->acknowledged_events;
  out->active_token = dispatcher->active_token;
  turbo_mutex_unlock(&dispatcher->mutex);
  return TURBO_OK;
}

int flowie_cluster_broadcast_target_dispatcher_close(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher) {
  int rc = TURBO_OK;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_EALREADY;
  else {
    dispatcher->closing = 1;
    dispatcher->state = FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSING;
    turbo_cond_broadcast(&dispatcher->changed);
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

int flowie_cluster_broadcast_target_dispatcher_drain(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, uint64_t timeout_ns) {
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
  while (rc == TURBO_OK &&
         (!dispatcher->closed || dispatcher->apply_inflight || dispatcher->joining)) {
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

int flowie_cluster_broadcast_target_dispatcher_destroy(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher) {
  int ready;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  ready = dispatcher->closing && dispatcher->closed && !dispatcher->thread_started &&
          dispatcher->thread_joined && !dispatcher->apply_inflight && !dispatcher->joining;
  turbo_mutex_unlock(&dispatcher->mutex);
  if (!ready) return TURBO_EBUSY;
  flowie_cluster_broadcast_target_storage_destroy(dispatcher);
  return TURBO_OK;
}
