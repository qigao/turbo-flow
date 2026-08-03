#include "flowie_cluster_lifecycle_dispatch_internal.h"

#include "flowie_cluster_peer_wire_internal.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_LIFECYCLE_EVENT_MAGIC[4] = {'T', 'F', 'L', 'E'};

struct flowie_cluster_lifecycle_dispatcher_s {
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  flowie_cluster_lifecycle_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_lifecycle_source_fetch_fn fetch;
  flowie_cluster_lifecycle_source_settle_fn settle;
  flowie_cluster_outbox_before_settle_fn before_settle;
  void *before_settle_ctx;
  flowie_cluster_lifecycle_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_lifecycle_apply_fn apply;
  void *apply_ctx;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
  flowie_cluster_lifecycle_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t apply_attempts;
  uint64_t settled_events;
  uint64_t source_attempt_count;
  int closing;
  int closed;
  int thread_started;
  int thread_joined;
  int joining;
  int apply_inflight;
  int apply_status;
};

struct flowie_cluster_lifecycle_pgsql_source_s {
  flowie_cluster_pgsql_fact_store_t *store;
};

static int flowie_cluster_lifecycle_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static uint64_t flowie_cluster_lifecycle_deadline(uint64_t created_at, uint32_t interval) {
  return created_at > UINT64_MAX - interval ? UINT64_MAX : created_at + interval;
}

int flowie_cluster_lifecycle_decide(const flowie_cluster_lifecycle_event_view_t *event,
                                    const flowie_session_snapshot_t *snapshot,
                                    uint64_t now_epoch_seconds,
                                    flowie_cluster_lifecycle_decision_t *out) {
  flowie_cluster_lifecycle_decision_t decision = FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
  uint64_t deadline;
  uint32_t delay;
  if (!event || event->size != sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1 ||
      event->expected_fact_revision == 0u || event->created_at_epoch_seconds == 0u ||
      event->session_id == 0u || event->session_generation == 0u || !snapshot ||
      snapshot->size < sizeof(*snapshot) || snapshot->abi_version != FLOWIE_SESSION_INTERNAL_ABI_V1 ||
      now_epoch_seconds == 0u || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  if (snapshot->resource_generation < event->expected_fact_revision) return TURBO_EPROTO;
  if (snapshot->active || snapshot->session_id != event->session_id ||
      snapshot->session_generation != event->session_generation) {
    *out = decision;
    return TURBO_OK;
  }
  if (snapshot->will_pending) {
    if (!snapshot->has_will) return TURBO_EPROTO;
    delay = snapshot->will_delay_interval;
    if (snapshot->session_expiry_interval != UINT32_MAX &&
        snapshot->session_expiry_interval < delay)
      delay = snapshot->session_expiry_interval;
    deadline = flowie_cluster_lifecycle_deadline(event->created_at_epoch_seconds, delay);
    decision.deadline_epoch_seconds = deadline;
    decision.action = now_epoch_seconds < deadline ? FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT
                                                    : FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL;
    *out = decision;
    return TURBO_OK;
  }
  if (snapshot->session_expiry_interval == UINT32_MAX) {
    *out = decision;
    return TURBO_OK;
  }
  deadline = flowie_cluster_lifecycle_deadline(event->created_at_epoch_seconds,
                                               snapshot->session_expiry_interval);
  decision.deadline_epoch_seconds = deadline;
  decision.action = now_epoch_seconds < deadline ? FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT
                                                  : FLOWIE_CLUSTER_LIFECYCLE_ACTION_EXPIRE_SESSION;
  *out = decision;
  return TURBO_OK;
}

int flowie_cluster_lifecycle_event_decode(const flowie_cluster_pgsql_outbox_event_t *event,
                                          size_t max_payload_size,
                                          flowie_cluster_lifecycle_event_view_t *out) {
  flowie_cluster_lifecycle_event_view_t decoded = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
  const uint8_t *data;
  uint32_t total_size;
  uint16_t edge_node_size;
  if (!event || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1 ||
      event->size != sizeof(*event) || event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      event->event_index != 0u || event->shard_id == UINT32_MAX || event->event_owner_epoch == 0u ||
      event->fact_revision == 0u || event->created_at_epoch_seconds == 0u ||
      event->event_type != FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST ||
      event->record_kind != FLOWIE_CLUSTER_KEY_SESSION || !event->record_key ||
      tstr_len(event->record_key) == 0u || !event->payload || max_payload_size == 0u ||
      !flowie_cluster_lifecycle_nonzero(event->command_id, sizeof(event->command_id)))
    return TURBO_EINVAL;
  if (tstr_len(event->payload) > max_payload_size || tstr_len(event->payload) > UINT32_MAX)
    return TURBO_EMSGSIZE;
  if (tstr_len(event->payload) < FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE)
    return TURBO_EPROTO;
  data = (const uint8_t *)event->payload;
  total_size = flowie_cluster_peer_wire_read_u32(data + 8u);
  edge_node_size = flowie_cluster_peer_wire_read_u16(data + 64u);
  if (memcmp(data, FLOWIE_CLUSTER_LIFECYCLE_EVENT_MAGIC,
             sizeof(FLOWIE_CLUSTER_LIFECYCLE_EVENT_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(data + 4u) != FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION ||
      flowie_cluster_peer_wire_read_u16(data + 6u) !=
          FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE ||
      total_size != tstr_len(event->payload) || edge_node_size == 0u ||
      edge_node_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      total_size != FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE + edge_node_size ||
      data[12] != 0u || data[13] != 0u || data[14] != 0u || data[15] != 0u ||
      flowie_cluster_peer_wire_read_u16(data + 66u) != 0u)
    return TURBO_EPROTO;
  decoded.connection_id = flowie_cluster_peer_wire_read_u64(data + 16u);
  decoded.connection_generation = flowie_cluster_peer_wire_read_u64(data + 24u);
  decoded.session_id = flowie_cluster_peer_wire_read_u64(data + 32u);
  decoded.session_generation = flowie_cluster_peer_wire_read_u64(data + 40u);
  decoded.edge_boot_id = data + 48u;
  decoded.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)event->record_key, tstr_len(event->record_key)};
  decoded.edge_node_id =
      (flowie_mqtt_span_t){data + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, edge_node_size};
  if (decoded.connection_id == 0u || decoded.connection_generation == 0u ||
      decoded.session_id == 0u || decoded.session_generation == 0u ||
      !flowie_cluster_lifecycle_nonzero(decoded.edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      !flowie_mqtt_utf8_validate(decoded.client_id) ||
      memchr(decoded.edge_node_id.data, '\0', decoded.edge_node_id.size) != NULL)
    return TURBO_EPROTO;
  memcpy(decoded.command_id, event->command_id, sizeof(decoded.command_id));
  decoded.shard_id = event->shard_id;
  decoded.event_owner_epoch = event->event_owner_epoch;
  decoded.expected_fact_revision = event->fact_revision;
  decoded.created_at_epoch_seconds = event->created_at_epoch_seconds;
  *out = decoded;
  return TURBO_OK;
}

int flowie_cluster_lifecycle_dispatcher_config_validate(
    const flowie_cluster_lifecycle_dispatcher_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1 ||
      config->max_payload_size == 0u || config->max_payload_size > UINT32_MAX ||
      config->poll_interval_ns == 0u || config->retry_interval_ns == 0u || !config->resolve ||
      !config->fetch || !config->settle || !config->recover || !config->apply ||
      (config->before_settle_ctx && !config->before_settle))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int
flowie_cluster_lifecycle_dispatcher_closing(flowie_cluster_lifecycle_dispatcher_t *dispatcher) {
  int closing;
  turbo_mutex_lock(&dispatcher->mutex);
  closing = dispatcher->closing;
  turbo_mutex_unlock(&dispatcher->mutex);
  return closing;
}

static int flowie_cluster_lifecycle_dispatcher_counter(uint64_t *counter) {
  if (*counter == UINT64_MAX) return TURBO_ERANGE;
  ++*counter;
  return TURBO_OK;
}

static void
flowie_cluster_lifecycle_dispatcher_status(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
                                           flowie_cluster_lifecycle_dispatcher_state_t state,
                                           int status) {
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing ? FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSING : state;
  dispatcher->last_status = status;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int
flowie_cluster_lifecycle_dispatcher_wait(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
                                         uint64_t interval_ns) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = interval_ns > UINT64_MAX - start_ns ? UINT64_MAX : start_ns + interval_ns;
  int rc = TURBO_OK;
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing ? FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSING
                                          : FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_RETRY_WAIT;
  turbo_cond_broadcast(&dispatcher->changed);
  while (!dispatcher->closing) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline_ns - now_ns);
  }
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int flowie_cluster_lifecycle_dispatcher_retryable(int status) {
  return status == TURBO_EBUSY || status == TURBO_ENOSPC || status == TURBO_ECANCELED ||
         status == TURBO_ESHUTDOWN || status == TURBO_EIO || status == TURBO_ETIMEDOUT ||
         status == TURBO_ENOENT;
}

static int flowie_cluster_lifecycle_dispatcher_source_recover(
    flowie_cluster_lifecycle_dispatcher_t *dispatcher, int status) {
  if (status != TURBO_EIO && status != TURBO_ETIMEDOUT) return status;
  return dispatcher->recover(dispatcher->source_ctx);
}

static void flowie_cluster_lifecycle_dispatcher_apply_complete(void *ctx, int status) {
  flowie_cluster_lifecycle_dispatcher_t *dispatcher = (flowie_cluster_lifecycle_dispatcher_t *)ctx;
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

static int
flowie_cluster_lifecycle_dispatcher_apply_wait(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
                                               const flowie_cluster_owner_token_t *owner,
                                               const flowie_cluster_lifecycle_event_view_t *event) {
  int apply_rc;
  int rc;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  else if (dispatcher->apply_inflight) rc = TURBO_EPROTO;
  else {
    rc = flowie_cluster_lifecycle_dispatcher_counter(&dispatcher->apply_attempts);
    if (rc == TURBO_OK) {
      dispatcher->apply_inflight = 1;
      dispatcher->apply_status = TURBO_EBUSY;
      dispatcher->state = FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_APPLYING;
      dispatcher->last_status = TURBO_OK;
      turbo_cond_broadcast(&dispatcher->changed);
    }
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  if (rc != TURBO_OK) return rc;
  apply_rc = dispatcher->apply(dispatcher->apply_ctx, owner, event,
                               flowie_cluster_lifecycle_dispatcher_apply_complete, dispatcher);
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
    if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int
flowie_cluster_lifecycle_dispatcher_settle(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
                                           const flowie_cluster_pgsql_outbox_event_t *event) {
  for (;;) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    int rc;
    if (flowie_cluster_lifecycle_dispatcher_closing(dispatcher)) return TURBO_ESHUTDOWN;
    flowie_cluster_lifecycle_dispatcher_status(
        dispatcher, FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_SETTLING, TURBO_OK);
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK && dispatcher->before_settle)
      rc = dispatcher->before_settle(dispatcher->before_settle_ctx, &owner, event);
    if (rc == TURBO_OK) rc = dispatcher->settle(dispatcher->source_ctx, &owner, event);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_lifecycle_dispatcher_counter(&dispatcher->settled_events);
      dispatcher->last_status = rc;
      turbo_cond_broadcast(&dispatcher->changed);
      turbo_mutex_unlock(&dispatcher->mutex);
      return rc;
    }
    if (rc == TURBO_EBUSY) return rc;
    if (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT) {
      int recover_rc = flowie_cluster_lifecycle_dispatcher_source_recover(dispatcher, rc);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (!flowie_cluster_lifecycle_dispatcher_retryable(rc)) return rc;
    flowie_cluster_lifecycle_dispatcher_status(dispatcher,
                                               FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_RETRY_WAIT, rc);
    if (flowie_cluster_lifecycle_dispatcher_wait(dispatcher, dispatcher->retry_interval_ns) !=
        TURBO_OK)
      return TURBO_ESHUTDOWN;
  }
}

static int
flowie_cluster_lifecycle_dispatcher_event(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
                                          const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_lifecycle_event_view_t decoded = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
  int rc = flowie_cluster_lifecycle_event_decode(event, dispatcher->max_payload_size, &decoded);
  if (rc != TURBO_OK) return rc;
  if (decoded.shard_id != dispatcher->shard_id) return TURBO_EPROTO;
  for (;;) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    if (flowie_cluster_lifecycle_dispatcher_closing(dispatcher)) return TURBO_ESHUTDOWN;
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK && owner.shard_id != decoded.shard_id) rc = TURBO_EPROTO;
    if (rc == TURBO_OK)
      rc = flowie_cluster_lifecycle_dispatcher_apply_wait(dispatcher, &owner, &decoded);
    if (rc == TURBO_OK) return flowie_cluster_lifecycle_dispatcher_settle(dispatcher, event);
    if (rc == TURBO_ESHUTDOWN && flowie_cluster_lifecycle_dispatcher_closing(dispatcher)) return rc;
    if (!flowie_cluster_lifecycle_dispatcher_retryable(rc)) return rc;
    flowie_cluster_lifecycle_dispatcher_status(dispatcher,
                                               FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_RETRY_WAIT, rc);
    if (flowie_cluster_lifecycle_dispatcher_wait(dispatcher, dispatcher->retry_interval_ns) !=
        TURBO_OK)
      return TURBO_ESHUTDOWN;
  }
}

static void flowie_cluster_lifecycle_dispatcher_run(void *ctx) {
  flowie_cluster_lifecycle_dispatcher_t *dispatcher = (flowie_cluster_lifecycle_dispatcher_t *)ctx;
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  int fatal_status = TURBO_OK;
  while (!flowie_cluster_lifecycle_dispatcher_closing(dispatcher)) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    int rc;
    flowie_cluster_lifecycle_dispatcher_status(
        dispatcher, FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_FETCHING, TURBO_OK);
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK) rc = dispatcher->fetch(dispatcher->source_ctx, &owner, &event);
    if (rc == TURBO_OK) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_lifecycle_dispatcher_counter(&dispatcher->fetched_events);
      if (rc == TURBO_OK) dispatcher->source_attempt_count = event.attempt_count;
      turbo_mutex_unlock(&dispatcher->mutex);
      if (rc == TURBO_OK) rc = flowie_cluster_lifecycle_dispatcher_event(dispatcher, &event);
      flowie_cluster_pgsql_outbox_event_cleanup(&event);
      if (rc == TURBO_OK || rc == TURBO_EBUSY) continue;
    } else if (rc == TURBO_ENOENT) {
      if (flowie_cluster_lifecycle_dispatcher_wait(dispatcher, dispatcher->poll_interval_ns) ==
          TURBO_OK)
        continue;
      rc = TURBO_ESHUTDOWN;
    } else if (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT) {
      int recover_rc = flowie_cluster_lifecycle_dispatcher_source_recover(dispatcher, rc);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (rc == TURBO_ESHUTDOWN && flowie_cluster_lifecycle_dispatcher_closing(dispatcher)) break;
    if (flowie_cluster_lifecycle_dispatcher_retryable(rc)) {
      flowie_cluster_lifecycle_dispatcher_status(
          dispatcher, FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_RETRY_WAIT, rc);
      if (flowie_cluster_lifecycle_dispatcher_wait(dispatcher, dispatcher->retry_interval_ns) ==
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
  dispatcher->state = FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSING;
  if (fatal_status != TURBO_OK) dispatcher->last_status = fatal_status;
  dispatcher->closed = 1;
  dispatcher->state = FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSED;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static void flowie_cluster_lifecycle_dispatcher_storage_destroy(
    flowie_cluster_lifecycle_dispatcher_t *dispatcher) {
  if (!dispatcher) return;
  turbo_cond_destroy(&dispatcher->changed);
  turbo_mutex_destroy(&dispatcher->mutex);
  free(dispatcher);
}

int flowie_cluster_lifecycle_dispatcher_create(
    const flowie_cluster_lifecycle_dispatcher_config_t *config,
    flowie_cluster_lifecycle_dispatcher_t **out) {
  flowie_cluster_lifecycle_dispatcher_t *dispatcher;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_lifecycle_dispatcher_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  dispatcher = (flowie_cluster_lifecycle_dispatcher_t *)calloc(1u, sizeof(*dispatcher));
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
  dispatcher->before_settle = config->before_settle;
  dispatcher->before_settle_ctx = config->before_settle_ctx;
  dispatcher->recover = config->recover;
  dispatcher->source_ctx = config->source_ctx;
  dispatcher->apply = config->apply;
  dispatcher->apply_ctx = config->apply_ctx;
  dispatcher->state = FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CREATED;
  dispatcher->last_status = TURBO_OK;
  rc =
      turbo_thread_create(&dispatcher->thread, flowie_cluster_lifecycle_dispatcher_run, dispatcher);
  if (rc != TURBO_OK) {
    flowie_cluster_lifecycle_dispatcher_storage_destroy(dispatcher);
    return rc;
  }
  dispatcher->thread_started = 1;
  *out = dispatcher;
  return TURBO_OK;
}

int flowie_cluster_lifecycle_dispatcher_snapshot(
    flowie_cluster_lifecycle_dispatcher_t *dispatcher,
    flowie_cluster_lifecycle_dispatcher_snapshot_t *out) {
  if (!dispatcher || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  out->state = dispatcher->state;
  out->last_status = dispatcher->last_status;
  out->fetched_events = dispatcher->fetched_events;
  out->apply_attempts = dispatcher->apply_attempts;
  out->settled_events = dispatcher->settled_events;
  out->source_attempt_count = dispatcher->source_attempt_count;
  turbo_mutex_unlock(&dispatcher->mutex);
  return TURBO_OK;
}

int flowie_cluster_lifecycle_dispatcher_close(flowie_cluster_lifecycle_dispatcher_t *dispatcher) {
  int rc = TURBO_OK;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_EALREADY;
  else {
    dispatcher->closing = 1;
    dispatcher->state = FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSING;
    turbo_cond_broadcast(&dispatcher->changed);
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

int flowie_cluster_lifecycle_dispatcher_drain(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
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

int flowie_cluster_lifecycle_dispatcher_destroy(flowie_cluster_lifecycle_dispatcher_t *dispatcher) {
  int ready;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  ready = dispatcher->closing && dispatcher->closed && !dispatcher->thread_started &&
          dispatcher->thread_joined && !dispatcher->apply_inflight && !dispatcher->joining;
  turbo_mutex_unlock(&dispatcher->mutex);
  if (!ready) return TURBO_EBUSY;
  flowie_cluster_lifecycle_dispatcher_storage_destroy(dispatcher);
  return TURBO_OK;
}

int flowie_cluster_lifecycle_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                 flowie_cluster_lifecycle_pgsql_source_t **out) {
  flowie_cluster_lifecycle_pgsql_source_t *source;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  source = (flowie_cluster_lifecycle_pgsql_source_t *)calloc(1u, sizeof(*source));
  if (!source) return TURBO_ENOMEM;
  rc = flowie_cluster_pgsql_fact_store_open(config, &source->store);
  if (rc != TURBO_OK) {
    free(source);
    return rc;
  }
  *out = source;
  return TURBO_OK;
}

int flowie_cluster_lifecycle_pgsql_source_fetch(void *ctx,
                                                const flowie_cluster_owner_token_t *current_owner,
                                                flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_lifecycle_pgsql_source_t *source = (flowie_cluster_lifecycle_pgsql_source_t *)ctx;
  return !source
             ? TURBO_EINVAL
             : flowie_cluster_pgsql_outbox_next(source->store, current_owner,
                                                FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST, out);
}

int flowie_cluster_lifecycle_pgsql_source_settle(void *ctx,
                                                 const flowie_cluster_owner_token_t *current_owner,
                                                 const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_lifecycle_pgsql_source_t *source = (flowie_cluster_lifecycle_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_settle(source->store, current_owner, event);
}

int flowie_cluster_lifecycle_pgsql_source_recover(void *ctx) {
  flowie_cluster_lifecycle_pgsql_source_t *source = (flowie_cluster_lifecycle_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL : flowie_cluster_pgsql_fact_store_reopen(&source->store);
}

void flowie_cluster_lifecycle_pgsql_source_destroy(
    flowie_cluster_lifecycle_pgsql_source_t *source) {
  if (!source) return;
  flowie_cluster_pgsql_fact_store_destroy(source->store);
  free(source);
}
