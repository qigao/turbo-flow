#include "flowie_cluster_takeover_dispatch_internal.h"

#include "flowie_cluster_peer_wire_internal.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_TAKEOVER_EVENT_MAGIC[4] = {'T', 'F', 'T', 'E'};

typedef struct flowie_cluster_takeover_event_view_s {
  flowie_mqtt_version_t mqtt_version;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_id;
  uint64_t session_generation;
  const uint8_t *edge_boot_id;
  tstr_v edge_node_id;
} flowie_cluster_takeover_event_view_t;

static int flowie_cluster_takeover_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_takeover_view_eq(tstr_v left, tstr_v right) {
  return left.len == right.len && (left.len == 0u || memcmp(left.data, right.data, left.len) == 0);
}

static int flowie_cluster_takeover_event_decode(const flowie_cluster_pgsql_outbox_event_t *event,
                                                size_t max_payload_size,
                                                flowie_cluster_takeover_event_view_t *out) {
  const uint8_t *data;
  uint32_t total_size;
  uint16_t edge_node_size;
  if (!event || !out || event->size != sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || event->event_index != 0u ||
      event->event_owner_epoch == 0u || event->fact_revision == 0u ||
      event->event_type != FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER ||
      event->record_kind != FLOWIE_CLUSTER_KEY_SESSION || !event->record_key ||
      tstr_len(event->record_key) == 0u || !event->payload || max_payload_size == 0u ||
      !flowie_cluster_takeover_nonzero(event->command_id, sizeof(event->command_id)))
    return TURBO_EINVAL;
  if (tstr_len(event->payload) > max_payload_size || tstr_len(event->payload) > UINT32_MAX)
    return TURBO_EMSGSIZE;
  if (tstr_len(event->payload) < FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE)
    return TURBO_EPROTO;
  data = (const uint8_t *)event->payload;
  total_size = flowie_cluster_peer_wire_read_u32(data + 8u);
  edge_node_size = flowie_cluster_peer_wire_read_u16(data + 64u);
  if (memcmp(data, FLOWIE_CLUSTER_TAKEOVER_EVENT_MAGIC,
             sizeof(FLOWIE_CLUSTER_TAKEOVER_EVENT_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(data + 4u) != FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION ||
      flowie_cluster_peer_wire_read_u16(data + 6u) !=
          FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE ||
      total_size != tstr_len(event->payload) || edge_node_size == 0u ||
      edge_node_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      total_size != FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE + edge_node_size ||
      data[13] != 0u || data[14] != 0u || data[15] != 0u ||
      flowie_cluster_peer_wire_read_u16(data + 66u) != 0u)
    return TURBO_EPROTO;
  memset(out, 0, sizeof(*out));
  out->mqtt_version = (flowie_mqtt_version_t)data[12];
  out->connection_id = flowie_cluster_peer_wire_read_u64(data + 16u);
  out->connection_generation = flowie_cluster_peer_wire_read_u64(data + 24u);
  out->session_id = flowie_cluster_peer_wire_read_u64(data + 32u);
  out->session_generation = flowie_cluster_peer_wire_read_u64(data + 40u);
  out->edge_boot_id = data + 48u;
  out->edge_node_id = tstr_v_from_buf(
      (const char *)data + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, edge_node_size);
  if (!flowie_mqtt_version_is_supported(out->mqtt_version) || out->connection_id == 0u ||
      out->connection_generation == 0u || out->session_id == 0u || out->session_generation == 0u ||
      !flowie_cluster_takeover_nonzero(out->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      memchr(out->edge_node_id.data, '\0', out->edge_node_id.len) != NULL)
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowie_cluster_takeover_dispatch_prepare(const flowie_cluster_pgsql_outbox_event_t *event,
                                             const flowie_cluster_owner_token_t *current_owner,
                                             tstr_v cluster_id, tstr_v listener_id,
                                             size_t max_payload_size,
                                             flowie_cluster_takeover_command_t *out) {
  flowie_cluster_takeover_event_view_t decoded;
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  size_t encoded_size = 0u;
  int rc;
  if (out) *out = (flowie_cluster_takeover_command_t)FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
  if (!event || !current_owner || !out || max_payload_size == 0u ||
      flowie_cluster_owner_token_require(current_owner, current_owner) != TURBO_OK)
    return TURBO_EINVAL;
  if (current_owner->shard_id != event->shard_id) return TURBO_EBUSY;
  rc = flowie_cluster_takeover_event_decode(event, max_payload_size, &decoded);
  if (rc != TURBO_OK) return rc;
  out->cluster_id = tstr_from_v(cluster_id);
  out->listener_id = tstr_from_v(listener_id);
  out->source_node_id = tstr_new_len(current_owner->node_id, current_owner->node_id_size);
  out->target_node_id = tstr_from_v(decoded.edge_node_id);
  if (!out->cluster_id || !out->listener_id || !out->source_node_id || !out->target_node_id) {
    flowie_cluster_takeover_command_cleanup(out);
    return TURBO_ENOMEM;
  }
  rc = flowie_cluster_peer_takeover_close_encode(
      decoded.mqtt_version,
      (flowie_mqtt_span_t){(const uint8_t *)event->record_key, tstr_len(event->record_key)},
      max_payload_size, &out->payload);
  if (rc != TURBO_OK) {
    flowie_cluster_takeover_command_cleanup(out);
    return rc;
  }
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE;
  frame.shard_id = current_owner->shard_id;
  frame.owner_epoch = current_owner->owner_epoch;
  frame.connection_id = decoded.connection_id;
  frame.connection_generation = decoded.connection_generation;
  frame.cluster_id = tstr_to_v(out->cluster_id);
  frame.listener_id = tstr_to_v(out->listener_id);
  frame.source_node_id = tstr_to_v(out->source_node_id);
  frame.target_node_id = tstr_to_v(out->target_node_id);
  frame.payload = tstr_to_v(out->payload);
  memcpy(frame.source_boot_id, current_owner->boot_id, sizeof(frame.source_boot_id));
  memcpy(frame.target_boot_id, decoded.edge_boot_id, sizeof(frame.target_boot_id));
  memcpy(frame.correlation_id, event->command_id, sizeof(frame.correlation_id));
  rc = flowie_cluster_peer_frame_encoded_size(&frame, max_payload_size, &encoded_size);
  if (rc != TURBO_OK) {
    flowie_cluster_takeover_command_cleanup(out);
    return rc;
  }
  out->frame = frame;
  return TURBO_OK;
}

int flowie_cluster_takeover_dispatch_reply_inspect(const flowie_cluster_takeover_command_t *command,
                                                   const flowie_cluster_peer_frame_t *reply,
                                                   size_t max_payload_size, int *reply_status) {
  flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
  flowie_cluster_peer_takeover_close_view_t takeover = FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_VIEW_INIT;
  size_t encoded_size = 0u;
  int rc;
  if (!command || command->size != sizeof(*command) ||
      command->abi_version != FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1 || !command->payload ||
      !reply || max_payload_size == 0u || !reply_status)
    return TURBO_EINVAL;
  *reply_status = TURBO_EPROTO;
  rc = flowie_cluster_peer_frame_encoded_size(reply, max_payload_size, &encoded_size);
  if (rc != TURBO_OK) return rc;
  if (reply->kind != FLOWIE_CLUSTER_PEER_FRAME_REPLY ||
      reply->operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY ||
      reply->shard_id != command->frame.shard_id ||
      reply->owner_epoch != command->frame.owner_epoch ||
      reply->connection_id != command->frame.connection_id ||
      reply->connection_generation != command->frame.connection_generation ||
      !flowie_cluster_takeover_view_eq(reply->cluster_id, command->frame.cluster_id) ||
      !flowie_cluster_takeover_view_eq(reply->listener_id, command->frame.listener_id) ||
      !flowie_cluster_takeover_view_eq(reply->source_node_id, command->frame.target_node_id) ||
      !flowie_cluster_takeover_view_eq(reply->target_node_id, command->frame.source_node_id) ||
      memcmp(reply->source_boot_id, command->frame.target_boot_id, sizeof(reply->source_boot_id)) !=
          0 ||
      memcmp(reply->target_boot_id, command->frame.source_boot_id, sizeof(reply->target_boot_id)) !=
          0 ||
      memcmp(reply->correlation_id, command->frame.correlation_id, sizeof(reply->correlation_id)) !=
          0)
    return TURBO_EPROTO;
  if (reply->status != TURBO_OK) {
    if (reply->payload.len != 0u) return TURBO_EPROTO;
    *reply_status = reply->status;
    return TURBO_OK;
  }
  rc = flowie_cluster_peer_takeover_close_decode(
      command->frame.payload.data, command->frame.payload.len, max_payload_size, &takeover);
  if (rc != TURBO_OK) return TURBO_EPROTO;
  rc = flowie_cluster_peer_mqtt_reply_decode(reply->payload.data, reply->payload.len,
                                             max_payload_size, &action);
  if (rc != TURBO_OK) return rc;
  if (action.mqtt_version != takeover.mqtt_version || action.close_after_send ||
      action.settlement_point != 0 || action.packet.packet.size != 0u)
    return TURBO_EPROTO;
  *reply_status = TURBO_OK;
  return TURBO_OK;
}

int flowie_cluster_takeover_dispatch_reply_validate(
    const flowie_cluster_takeover_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size) {
  int reply_status = TURBO_EPROTO;
  int rc = flowie_cluster_takeover_dispatch_reply_inspect(command, reply, max_payload_size,
                                                          &reply_status);
  return rc == TURBO_OK ? reply_status : rc;
}

void flowie_cluster_takeover_command_cleanup(flowie_cluster_takeover_command_t *command) {
  if (!command) return;
  tstr_free(command->cluster_id);
  tstr_free(command->listener_id);
  tstr_free(command->source_node_id);
  tstr_free(command->target_node_id);
  tstr_free(command->payload);
  *command = (flowie_cluster_takeover_command_t)FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
}

struct flowie_cluster_takeover_dispatcher_s {
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t reply_timeout_ns;
  tstr_t cluster_id;
  tstr_t listener_id;
  flowie_cluster_takeover_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_takeover_source_fetch_fn fetch;
  flowie_cluster_takeover_source_settle_fn settle;
  flowie_cluster_outbox_before_settle_fn before_settle;
  void *before_settle_ctx;
  flowie_cluster_takeover_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_takeover_send_fn send;
  void *send_ctx;
  flowie_cluster_peer_outbox_prepare_fn prepare;
  flowie_cluster_peer_outbox_reply_inspect_fn reply_inspect;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
  flowie_cluster_takeover_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t send_attempts;
  uint64_t reply_timeouts;
  uint64_t settled_events;
  uint64_t source_attempt_count;
  int closing;
  int closed;
  int thread_started;
  int joining;
  int thread_joined;
  int send_inflight;
  int transport_done;
  int transport_status;
  int reply_done;
  int reply_status;
  int command_active;
  flowie_cluster_takeover_command_t command;
};

struct flowie_cluster_takeover_pgsql_source_s {
  flowie_cluster_pgsql_fact_store_t *store;
};

static int flowie_cluster_takeover_dispatch_text_validate(tstr_v text, size_t maximum) {
  if (!text.data || text.len == 0u) return TURBO_EINVAL;
  if (text.len > maximum) return TURBO_EMSGSIZE;
  return memchr(text.data, '\0', text.len) ? TURBO_EPROTO : TURBO_OK;
}

int flowie_cluster_takeover_dispatcher_config_validate(
    const flowie_cluster_takeover_dispatcher_config_t *config) {
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1 ||
      config->max_payload_size == 0u || config->max_payload_size > UINT32_MAX ||
      config->poll_interval_ns == 0u || config->retry_interval_ns == 0u ||
      config->reply_timeout_ns == 0u || !config->resolve || !config->fetch || !config->settle ||
      (config->before_settle_ctx && !config->before_settle) || !config->recover || !config->send)
    return TURBO_EINVAL;
  rc = flowie_cluster_takeover_dispatch_text_validate(config->cluster_id, FLOWIE_CLUSTER_ID_MAX);
  if (rc == TURBO_OK)
    rc = flowie_cluster_takeover_dispatch_text_validate(config->listener_id,
                                                        FLOWIE_CLUSTER_LISTENER_ID_MAX);
  return rc;
}

static int
flowie_cluster_takeover_dispatcher_closing(flowie_cluster_takeover_dispatcher_t *dispatcher) {
  int closing;
  turbo_mutex_lock(&dispatcher->mutex);
  closing = dispatcher->closing;
  turbo_mutex_unlock(&dispatcher->mutex);
  return closing;
}

static int flowie_cluster_takeover_dispatcher_counter(uint64_t *counter) {
  if (*counter == UINT64_MAX) return TURBO_ERANGE;
  ++*counter;
  return TURBO_OK;
}

static void
flowie_cluster_takeover_dispatcher_status(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                          flowie_cluster_takeover_dispatcher_state_t state,
                                          int status) {
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing ? FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSING : state;
  dispatcher->last_status = status;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int flowie_cluster_takeover_dispatcher_wait(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                                   flowie_cluster_takeover_dispatcher_state_t state,
                                                   uint64_t interval_ns) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = interval_ns > UINT64_MAX - start_ns ? UINT64_MAX : start_ns + interval_ns;
  int rc = TURBO_OK;
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->state = dispatcher->closing ? FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSING : state;
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

static int flowie_cluster_takeover_dispatcher_retryable(int status) {
  return status == TURBO_EBUSY || status == TURBO_ENOSPC || status == TURBO_ECANCELED ||
         status == TURBO_ESHUTDOWN || status == TURBO_EIO || status == TURBO_ETIMEDOUT ||
         status == TURBO_ENOENT;
}

static int
flowie_cluster_takeover_dispatcher_recover(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                           int status) {
  if (status != TURBO_EIO && status != TURBO_ETIMEDOUT) return status;
  return dispatcher->recover(dispatcher->source_ctx);
}

static void flowie_cluster_takeover_dispatcher_command_take_locked(
    flowie_cluster_takeover_dispatcher_t *dispatcher, flowie_cluster_takeover_command_t *command) {
  if (dispatcher->command_active) {
    *command = dispatcher->command;
    dispatcher->command = (flowie_cluster_takeover_command_t)FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
    dispatcher->command_active = 0;
  }
}

static void flowie_cluster_takeover_dispatcher_send_complete(void *ctx, int status) {
  flowie_cluster_takeover_dispatcher_t *dispatcher = (flowie_cluster_takeover_dispatcher_t *)ctx;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->send_inflight) {
    dispatcher->send_inflight = 0;
    dispatcher->transport_done = 1;
    dispatcher->transport_status = status;
  } else if (dispatcher->last_status == TURBO_OK) {
    dispatcher->last_status = TURBO_EPROTO;
  }
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
}

static int
flowie_cluster_takeover_dispatcher_install(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                           flowie_cluster_takeover_command_t *command) {
  int rc;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  else if (dispatcher->command_active || dispatcher->send_inflight) rc = TURBO_EPROTO;
  else {
    rc = flowie_cluster_takeover_dispatcher_counter(&dispatcher->send_attempts);
    if (rc == TURBO_OK) {
      dispatcher->command = *command;
      *command = (flowie_cluster_takeover_command_t)FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
      dispatcher->command_active = 1;
      dispatcher->send_inflight = 1;
      dispatcher->transport_done = 0;
      dispatcher->transport_status = TURBO_EBUSY;
      dispatcher->reply_done = 0;
      dispatcher->reply_status = TURBO_EBUSY;
      dispatcher->state = FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SENDING;
      dispatcher->last_status = TURBO_OK;
      turbo_cond_broadcast(&dispatcher->changed);
    }
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

static int
flowie_cluster_takeover_dispatcher_send_wait(flowie_cluster_takeover_dispatcher_t *dispatcher) {
  flowie_cluster_takeover_command_t cleanup = FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
  uint64_t start_ns;
  uint64_t deadline_ns;
  int send_rc;
  int rc = TURBO_OK;
  send_rc = dispatcher->send(dispatcher->send_ctx, &dispatcher->command.frame,
                             flowie_cluster_takeover_dispatcher_send_complete, dispatcher);
  turbo_mutex_lock(&dispatcher->mutex);
  if (send_rc != TURBO_OK) {
    if (!dispatcher->send_inflight) rc = TURBO_EPROTO;
    else {
      dispatcher->send_inflight = 0;
      rc = send_rc;
    }
    flowie_cluster_takeover_dispatcher_command_take_locked(dispatcher, &cleanup);
    turbo_cond_broadcast(&dispatcher->changed);
    turbo_mutex_unlock(&dispatcher->mutex);
    flowie_cluster_takeover_command_cleanup(&cleanup);
    return rc;
  }
  while (!dispatcher->transport_done && !dispatcher->closing)
    turbo_cond_wait(&dispatcher->changed, &dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  else if (dispatcher->transport_status != TURBO_OK) rc = dispatcher->transport_status;
  if (rc != TURBO_OK) {
    flowie_cluster_takeover_dispatcher_command_take_locked(dispatcher, &cleanup);
    turbo_mutex_unlock(&dispatcher->mutex);
    flowie_cluster_takeover_command_cleanup(&cleanup);
    return rc;
  }
  dispatcher->state = FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_WAITING_REPLY;
  start_ns = turbo_hrtime();
  deadline_ns = dispatcher->reply_timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + dispatcher->reply_timeout_ns;
  while (!dispatcher->reply_done && !dispatcher->closing) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&dispatcher->changed, &dispatcher->mutex, deadline_ns - now_ns);
  }
  if (dispatcher->closing) rc = TURBO_ESHUTDOWN;
  else if (!dispatcher->reply_done) {
    rc = flowie_cluster_takeover_dispatcher_counter(&dispatcher->reply_timeouts);
    if (rc == TURBO_OK) rc = TURBO_ETIMEDOUT;
  } else {
    rc = dispatcher->reply_status;
    dispatcher->last_status = rc;
    if (rc != TURBO_OK) rc = TURBO_EBUSY;
  }
  flowie_cluster_takeover_dispatcher_command_take_locked(dispatcher, &cleanup);
  turbo_mutex_unlock(&dispatcher->mutex);
  flowie_cluster_takeover_command_cleanup(&cleanup);
  return rc;
}

static int
flowie_cluster_takeover_dispatcher_settle(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                          const flowie_cluster_pgsql_outbox_event_t *event) {
  for (;;) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    int rc;
    if (flowie_cluster_takeover_dispatcher_closing(dispatcher)) return TURBO_ESHUTDOWN;
    flowie_cluster_takeover_dispatcher_status(
        dispatcher, FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SETTLING, TURBO_OK);
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK && dispatcher->before_settle)
      rc = dispatcher->before_settle(dispatcher->before_settle_ctx, &owner, event);
    if (rc == TURBO_OK) rc = dispatcher->settle(dispatcher->source_ctx, &owner, event);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_takeover_dispatcher_counter(&dispatcher->settled_events);
      dispatcher->last_status = rc;
      turbo_cond_broadcast(&dispatcher->changed);
      turbo_mutex_unlock(&dispatcher->mutex);
      return rc;
    }
    if (rc == TURBO_EBUSY) return rc;
    if (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT) {
      int recover_rc = flowie_cluster_takeover_dispatcher_recover(dispatcher, rc);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (!flowie_cluster_takeover_dispatcher_retryable(rc)) return rc;
    flowie_cluster_takeover_dispatcher_status(dispatcher,
                                              FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT, rc);
    if (flowie_cluster_takeover_dispatcher_wait(dispatcher,
                                                FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT,
                                                dispatcher->retry_interval_ns) != TURBO_OK)
      return TURBO_ESHUTDOWN;
  }
}

static int
flowie_cluster_takeover_dispatcher_event(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                         const flowie_cluster_pgsql_outbox_event_t *event) {
  for (;;) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_takeover_command_t command = FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
    int rc;
    if (flowie_cluster_takeover_dispatcher_closing(dispatcher)) return TURBO_ESHUTDOWN;
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK)
      rc = dispatcher->prepare(
          event, &owner, tstr_to_v(dispatcher->cluster_id), tstr_to_v(dispatcher->listener_id),
          dispatcher->max_payload_size, &command);
    if (rc == TURBO_OK) rc = flowie_cluster_takeover_dispatcher_install(dispatcher, &command);
    if (rc == TURBO_OK) rc = flowie_cluster_takeover_dispatcher_send_wait(dispatcher);
    flowie_cluster_takeover_command_cleanup(&command);
    if (rc == TURBO_OK) return flowie_cluster_takeover_dispatcher_settle(dispatcher, event);
    if (rc == TURBO_ESHUTDOWN && flowie_cluster_takeover_dispatcher_closing(dispatcher)) return rc;
    if (!flowie_cluster_takeover_dispatcher_retryable(rc)) return rc;
    flowie_cluster_takeover_dispatcher_status(dispatcher,
                                              FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT, rc);
    if (flowie_cluster_takeover_dispatcher_wait(dispatcher,
                                                FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT,
                                                dispatcher->retry_interval_ns) != TURBO_OK)
      return TURBO_ESHUTDOWN;
  }
}

static void flowie_cluster_takeover_dispatcher_run(void *ctx) {
  flowie_cluster_takeover_dispatcher_t *dispatcher = (flowie_cluster_takeover_dispatcher_t *)ctx;
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  flowie_cluster_takeover_command_t cleanup = FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
  int fatal_status = TURBO_OK;
  while (!flowie_cluster_takeover_dispatcher_closing(dispatcher)) {
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    int rc;
    flowie_cluster_takeover_dispatcher_status(
        dispatcher, FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_FETCHING, TURBO_OK);
    rc = dispatcher->resolve(dispatcher->resolve_ctx, dispatcher->shard_id, &owner);
    if (rc == TURBO_OK) rc = dispatcher->fetch(dispatcher->source_ctx, &owner, &event);
    if (rc == TURBO_OK) {
      turbo_mutex_lock(&dispatcher->mutex);
      rc = flowie_cluster_takeover_dispatcher_counter(&dispatcher->fetched_events);
      if (rc == TURBO_OK) dispatcher->source_attempt_count = event.attempt_count;
      turbo_mutex_unlock(&dispatcher->mutex);
      if (rc == TURBO_OK) rc = flowie_cluster_takeover_dispatcher_event(dispatcher, &event);
      flowie_cluster_pgsql_outbox_event_cleanup(&event);
      if (rc == TURBO_OK || rc == TURBO_EBUSY) continue;
    } else if (rc == TURBO_ENOENT) {
      if (flowie_cluster_takeover_dispatcher_wait(dispatcher,
                                                  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT,
                                                  dispatcher->poll_interval_ns) == TURBO_OK)
        continue;
      rc = TURBO_ESHUTDOWN;
    } else if (rc == TURBO_EIO || rc == TURBO_ETIMEDOUT) {
      int recover_rc = flowie_cluster_takeover_dispatcher_recover(dispatcher, rc);
      if (recover_rc != TURBO_OK) rc = recover_rc;
    }
    if (rc == TURBO_ESHUTDOWN && flowie_cluster_takeover_dispatcher_closing(dispatcher)) break;
    if (flowie_cluster_takeover_dispatcher_retryable(rc)) {
      flowie_cluster_takeover_dispatcher_status(dispatcher,
                                                FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT, rc);
      if (flowie_cluster_takeover_dispatcher_wait(dispatcher,
                                                  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT,
                                                  dispatcher->retry_interval_ns) == TURBO_OK)
        continue;
      break;
    }
    fatal_status = rc;
    break;
  }
  flowie_cluster_pgsql_outbox_event_cleanup(&event);
  turbo_mutex_lock(&dispatcher->mutex);
  dispatcher->closing = 1;
  dispatcher->state = FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSING;
  if (fatal_status != TURBO_OK) dispatcher->last_status = fatal_status;
  flowie_cluster_takeover_dispatcher_command_take_locked(dispatcher, &cleanup);
  dispatcher->closed = 1;
  dispatcher->state = FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSED;
  turbo_cond_broadcast(&dispatcher->changed);
  turbo_mutex_unlock(&dispatcher->mutex);
  flowie_cluster_takeover_command_cleanup(&cleanup);
}

static void flowie_cluster_takeover_dispatcher_storage_destroy(
    flowie_cluster_takeover_dispatcher_t *dispatcher) {
  if (!dispatcher) return;
  flowie_cluster_takeover_command_cleanup(&dispatcher->command);
  tstr_free(dispatcher->cluster_id);
  tstr_free(dispatcher->listener_id);
  turbo_cond_destroy(&dispatcher->changed);
  turbo_mutex_destroy(&dispatcher->mutex);
  free(dispatcher);
}

int flowie_cluster_takeover_dispatcher_create_strategy(
    const flowie_cluster_takeover_dispatcher_config_t *config,
    flowie_cluster_peer_outbox_prepare_fn prepare,
    flowie_cluster_peer_outbox_reply_inspect_fn reply_inspect,
    flowie_cluster_takeover_dispatcher_t **out) {
  flowie_cluster_takeover_dispatcher_t *dispatcher;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_takeover_dispatcher_config_validate(config);
  if (rc == TURBO_OK && (!prepare || !reply_inspect)) rc = TURBO_EINVAL;
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  dispatcher = (flowie_cluster_takeover_dispatcher_t *)calloc(1u, sizeof(*dispatcher));
  if (!dispatcher) return TURBO_ENOMEM;
  turbo_mutex_init(&dispatcher->mutex);
  turbo_cond_init(&dispatcher->changed);
  dispatcher->shard_id = config->shard_id;
  dispatcher->max_payload_size = config->max_payload_size;
  dispatcher->poll_interval_ns = config->poll_interval_ns;
  dispatcher->retry_interval_ns = config->retry_interval_ns;
  dispatcher->reply_timeout_ns = config->reply_timeout_ns;
  dispatcher->state = FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CREATED;
  dispatcher->last_status = TURBO_OK;
  dispatcher->command = (flowie_cluster_takeover_command_t)FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
  dispatcher->resolve = config->resolve;
  dispatcher->resolve_ctx = config->resolve_ctx;
  dispatcher->fetch = config->fetch;
  dispatcher->settle = config->settle;
  dispatcher->before_settle = config->before_settle;
  dispatcher->before_settle_ctx = config->before_settle_ctx;
  dispatcher->recover = config->recover;
  dispatcher->source_ctx = config->source_ctx;
  dispatcher->send = config->send;
  dispatcher->send_ctx = config->send_ctx;
  dispatcher->prepare = prepare;
  dispatcher->reply_inspect = reply_inspect;
  dispatcher->cluster_id = tstr_from_v(config->cluster_id);
  dispatcher->listener_id = tstr_from_v(config->listener_id);
  if (!dispatcher->cluster_id || !dispatcher->listener_id) {
    flowie_cluster_takeover_dispatcher_storage_destroy(dispatcher);
    return TURBO_ENOMEM;
  }
  rc = turbo_thread_create(&dispatcher->thread, flowie_cluster_takeover_dispatcher_run, dispatcher);
  if (rc != TURBO_OK) {
    flowie_cluster_takeover_dispatcher_storage_destroy(dispatcher);
    return rc;
  }
  dispatcher->thread_started = 1;
  *out = dispatcher;
  return TURBO_OK;
}

int flowie_cluster_takeover_dispatcher_create(
    const flowie_cluster_takeover_dispatcher_config_t *config,
    flowie_cluster_takeover_dispatcher_t **out) {
  return flowie_cluster_takeover_dispatcher_create_strategy(
      config, flowie_cluster_takeover_dispatch_prepare,
      flowie_cluster_takeover_dispatch_reply_inspect, out);
}

int flowie_cluster_takeover_dispatcher_reply(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                             const flowie_cluster_peer_frame_t *reply) {
  int reply_status = TURBO_EPROTO;
  int rc;
  if (!dispatcher || !reply) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  if (!dispatcher->command_active) {
    turbo_mutex_unlock(&dispatcher->mutex);
    return TURBO_ENOENT;
  }
  rc = dispatcher->reply_inspect(&dispatcher->command, reply, dispatcher->max_payload_size,
                                 &reply_status);
  if (rc == TURBO_OK && !dispatcher->reply_done) {
    dispatcher->reply_done = 1;
    dispatcher->reply_status = reply_status;
    turbo_cond_broadcast(&dispatcher->changed);
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

int flowie_cluster_takeover_dispatcher_snapshot(
    flowie_cluster_takeover_dispatcher_t *dispatcher,
    flowie_cluster_takeover_dispatcher_snapshot_t *out) {
  if (!dispatcher || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  out->state = dispatcher->state;
  out->last_status = dispatcher->last_status;
  out->fetched_events = dispatcher->fetched_events;
  out->send_attempts = dispatcher->send_attempts;
  out->reply_timeouts = dispatcher->reply_timeouts;
  out->settled_events = dispatcher->settled_events;
  out->source_attempt_count = dispatcher->source_attempt_count;
  turbo_mutex_unlock(&dispatcher->mutex);
  return TURBO_OK;
}

int flowie_cluster_takeover_dispatcher_close(flowie_cluster_takeover_dispatcher_t *dispatcher) {
  int rc = TURBO_OK;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  if (dispatcher->closing) rc = TURBO_EALREADY;
  else {
    dispatcher->closing = 1;
    dispatcher->state = FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSING;
    turbo_cond_broadcast(&dispatcher->changed);
  }
  turbo_mutex_unlock(&dispatcher->mutex);
  return rc;
}

int flowie_cluster_takeover_dispatcher_drain(flowie_cluster_takeover_dispatcher_t *dispatcher,
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
         (!dispatcher->closed || dispatcher->send_inflight || dispatcher->joining)) {
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

int flowie_cluster_takeover_dispatcher_destroy(flowie_cluster_takeover_dispatcher_t *dispatcher) {
  int ready;
  if (!dispatcher) return TURBO_EINVAL;
  turbo_mutex_lock(&dispatcher->mutex);
  ready = dispatcher->closing && dispatcher->closed && !dispatcher->thread_started &&
          dispatcher->thread_joined && !dispatcher->send_inflight && !dispatcher->joining;
  turbo_mutex_unlock(&dispatcher->mutex);
  if (!ready) return TURBO_EBUSY;
  flowie_cluster_takeover_dispatcher_storage_destroy(dispatcher);
  return TURBO_OK;
}

int flowie_cluster_takeover_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                flowie_cluster_takeover_pgsql_source_t **out) {
  flowie_cluster_takeover_pgsql_source_t *source;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  source = (flowie_cluster_takeover_pgsql_source_t *)calloc(1u, sizeof(*source));
  if (!source) return TURBO_ENOMEM;
  rc = flowie_cluster_pgsql_fact_store_open(config, &source->store);
  if (rc != TURBO_OK) {
    free(source);
    return rc;
  }
  *out = source;
  return TURBO_OK;
}

int flowie_cluster_takeover_pgsql_source_fetch(void *ctx,
                                               const flowie_cluster_owner_token_t *current_owner,
                                               flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_takeover_pgsql_source_t *source = (flowie_cluster_takeover_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_next(source->store, current_owner,
                                                    FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER, out);
}

int flowie_cluster_takeover_pgsql_source_settle(void *ctx,
                                                const flowie_cluster_owner_token_t *current_owner,
                                                const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_takeover_pgsql_source_t *source = (flowie_cluster_takeover_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_settle(source->store, current_owner, event);
}

int flowie_cluster_takeover_pgsql_source_recover(void *ctx) {
  flowie_cluster_takeover_pgsql_source_t *source = (flowie_cluster_takeover_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL : flowie_cluster_pgsql_fact_store_reopen(&source->store);
}

void flowie_cluster_takeover_pgsql_source_destroy(flowie_cluster_takeover_pgsql_source_t *source) {
  if (!source) return;
  flowie_cluster_pgsql_fact_store_destroy(source->store);
  free(source);
}
