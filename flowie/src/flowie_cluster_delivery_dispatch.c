#include "flowie_cluster_delivery_dispatch_internal.h"

#include "turbo_error.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

struct flowie_cluster_delivery_pgsql_source_s {
  flowie_cluster_pgsql_fact_store_t *store;
};

static int flowie_cluster_delivery_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_delivery_view_eq(tstr_v left, tstr_v right) {
  return left.len == right.len &&
         (left.len == 0u || memcmp(left.data, right.data, left.len) == 0);
}

int flowie_cluster_delivery_dispatch_prepare(const flowie_cluster_pgsql_outbox_event_t *event,
                                             const flowie_cluster_owner_token_t *current_owner,
                                             tstr_v cluster_id, tstr_v listener_id,
                                             size_t max_payload_size,
                                             flowie_cluster_delivery_command_t *out) {
  flowie_cluster_delivery_action_view_t action = FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  size_t encoded_size = 0u;
  int rc;
  if (out) *out = (flowie_cluster_delivery_command_t)FLOWIE_CLUSTER_DELIVERY_COMMAND_INIT;
  if (!event || !current_owner || !out || max_payload_size == 0u ||
      event->size != sizeof(*event) || event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      event->event_index != 0u || event->event_owner_epoch == 0u || event->fact_revision == 0u ||
      event->event_type != FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE ||
      event->record_kind != FLOWIE_CLUSTER_KEY_SESSION || !event->record_key ||
      tstr_len(event->record_key) == 0u || !event->payload ||
      !flowie_cluster_delivery_nonzero(event->command_id, sizeof(event->command_id)) ||
      flowie_cluster_owner_token_require(current_owner, current_owner) != TURBO_OK)
    return TURBO_EINVAL;
  if (current_owner->shard_id != event->shard_id) return TURBO_EBUSY;
  rc = flowie_cluster_delivery_action_decode(event->payload, tstr_len(event->payload),
                                             max_payload_size, &action);
  if (rc != TURBO_OK) return rc;
  out->cluster_id = tstr_from_v(cluster_id);
  out->listener_id = tstr_from_v(listener_id);
  out->source_node_id = tstr_new_len(current_owner->node_id, current_owner->node_id_size);
  out->target_node_id = tstr_from_v(action.edge_node_id);
  out->payload = tstr_from_v(action.encoded_action);
  if (!out->cluster_id || !out->listener_id || !out->source_node_id || !out->target_node_id ||
      !out->payload) {
    flowie_cluster_delivery_command_cleanup(out);
    return TURBO_ENOMEM;
  }
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION;
  frame.shard_id = current_owner->shard_id;
  frame.owner_epoch = current_owner->owner_epoch;
  frame.connection_id = action.connection_id;
  frame.connection_generation = action.connection_generation;
  frame.cluster_id = tstr_to_v(out->cluster_id);
  frame.listener_id = tstr_to_v(out->listener_id);
  frame.source_node_id = tstr_to_v(out->source_node_id);
  frame.target_node_id = tstr_to_v(out->target_node_id);
  frame.payload = tstr_to_v(out->payload);
  memcpy(frame.source_boot_id, current_owner->boot_id, sizeof(frame.source_boot_id));
  memcpy(frame.target_boot_id, action.edge_boot_id, sizeof(frame.target_boot_id));
  memcpy(frame.correlation_id, event->command_id, sizeof(frame.correlation_id));
  rc = flowie_cluster_peer_frame_encoded_size(&frame, max_payload_size, &encoded_size);
  if (rc != TURBO_OK) {
    flowie_cluster_delivery_command_cleanup(out);
    return rc;
  }
  out->frame = frame;
  return TURBO_OK;
}

int flowie_cluster_delivery_dispatch_reply_inspect(
    const flowie_cluster_delivery_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size, int *reply_status) {
  flowie_cluster_peer_edge_action_t action = FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT;
  uint64_t acknowledged_sequence = 0u;
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
      reply->operation != FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK ||
      reply->shard_id != command->frame.shard_id ||
      reply->owner_epoch != command->frame.owner_epoch ||
      reply->connection_id != command->frame.connection_id ||
      reply->connection_generation != command->frame.connection_generation ||
      !flowie_cluster_delivery_view_eq(reply->cluster_id, command->frame.cluster_id) ||
      !flowie_cluster_delivery_view_eq(reply->listener_id, command->frame.listener_id) ||
      !flowie_cluster_delivery_view_eq(reply->source_node_id, command->frame.target_node_id) ||
      !flowie_cluster_delivery_view_eq(reply->target_node_id, command->frame.source_node_id) ||
      memcmp(reply->source_boot_id, command->frame.target_boot_id,
             sizeof(reply->source_boot_id)) != 0 ||
      memcmp(reply->target_boot_id, command->frame.source_boot_id,
             sizeof(reply->target_boot_id)) != 0 ||
      memcmp(reply->correlation_id, command->frame.correlation_id,
             sizeof(reply->correlation_id)) != 0)
    return TURBO_EPROTO;
  if (reply->status != TURBO_OK) {
    if (reply->payload.len != 0u) return TURBO_EPROTO;
    *reply_status = reply->status;
    return TURBO_OK;
  }
  rc = flowie_cluster_peer_edge_action_decode(command->frame.payload.data,
                                              command->frame.payload.len, max_payload_size,
                                              &action);
  if (rc != TURBO_OK) return TURBO_EPROTO;
  rc = flowie_cluster_peer_edge_action_ack_decode(reply->payload.data, reply->payload.len,
                                                  &acknowledged_sequence);
  if (rc != TURBO_OK) return rc;
  if (acknowledged_sequence != action.action_sequence) return TURBO_EPROTO;
  *reply_status = TURBO_OK;
  return TURBO_OK;
}

int flowie_cluster_delivery_dispatch_reply_validate(
    const flowie_cluster_delivery_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size) {
  int reply_status = TURBO_EPROTO;
  int rc = flowie_cluster_delivery_dispatch_reply_inspect(command, reply, max_payload_size,
                                                          &reply_status);
  return rc == TURBO_OK ? reply_status : rc;
}

void flowie_cluster_delivery_command_cleanup(flowie_cluster_delivery_command_t *command) {
  flowie_cluster_takeover_command_cleanup(command);
}

static void flowie_cluster_delivery_dispatcher_config_map(
    const flowie_cluster_delivery_dispatcher_config_t *source,
    flowie_cluster_takeover_dispatcher_config_t *target) {
  *target = (flowie_cluster_takeover_dispatcher_config_t)
      FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CONFIG_INIT;
  target->shard_id = source->shard_id;
  target->max_payload_size = source->max_payload_size;
  target->poll_interval_ns = source->poll_interval_ns;
  target->retry_interval_ns = source->retry_interval_ns;
  target->reply_timeout_ns = source->reply_timeout_ns;
  target->cluster_id = source->cluster_id;
  target->listener_id = source->listener_id;
  target->resolve = source->resolve;
  target->resolve_ctx = source->resolve_ctx;
  target->fetch = source->fetch;
  target->settle = source->settle;
  target->recover = source->recover;
  target->source_ctx = source->source_ctx;
  target->send = source->send;
  target->send_ctx = source->send_ctx;
}

int flowie_cluster_delivery_dispatcher_config_validate(
    const flowie_cluster_delivery_dispatcher_config_t *config) {
  flowie_cluster_takeover_dispatcher_config_t mapped;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_DELIVERY_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  flowie_cluster_delivery_dispatcher_config_map(config, &mapped);
  return flowie_cluster_takeover_dispatcher_config_validate(&mapped);
}

int flowie_cluster_delivery_dispatcher_create(
    const flowie_cluster_delivery_dispatcher_config_t *config,
    flowie_cluster_delivery_dispatcher_t **out) {
  flowie_cluster_takeover_dispatcher_config_t mapped;
  int rc = flowie_cluster_delivery_dispatcher_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  flowie_cluster_delivery_dispatcher_config_map(config, &mapped);
  return flowie_cluster_takeover_dispatcher_create_strategy(
      &mapped, flowie_cluster_delivery_dispatch_prepare,
      flowie_cluster_delivery_dispatch_reply_inspect, out);
}

int flowie_cluster_delivery_dispatcher_reply(flowie_cluster_delivery_dispatcher_t *dispatcher,
                                             const flowie_cluster_peer_frame_t *reply) {
  return flowie_cluster_takeover_dispatcher_reply(dispatcher, reply);
}

int flowie_cluster_delivery_dispatcher_snapshot(
    flowie_cluster_delivery_dispatcher_t *dispatcher,
    flowie_cluster_delivery_dispatcher_snapshot_t *out) {
  return flowie_cluster_takeover_dispatcher_snapshot(dispatcher, out);
}

int flowie_cluster_delivery_dispatcher_close(flowie_cluster_delivery_dispatcher_t *dispatcher) {
  return flowie_cluster_takeover_dispatcher_close(dispatcher);
}

int flowie_cluster_delivery_dispatcher_drain(flowie_cluster_delivery_dispatcher_t *dispatcher,
                                             uint64_t timeout_ns) {
  return flowie_cluster_takeover_dispatcher_drain(dispatcher, timeout_ns);
}

int flowie_cluster_delivery_dispatcher_destroy(flowie_cluster_delivery_dispatcher_t *dispatcher) {
  return flowie_cluster_takeover_dispatcher_destroy(dispatcher);
}

int flowie_cluster_delivery_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                flowie_cluster_delivery_pgsql_source_t **out) {
  flowie_cluster_delivery_pgsql_source_t *source;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  source = (flowie_cluster_delivery_pgsql_source_t *)calloc(1u, sizeof(*source));
  if (!source) return TURBO_ENOMEM;
  rc = flowie_cluster_pgsql_fact_store_open(config, &source->store);
  if (rc != TURBO_OK) {
    free(source);
    return rc;
  }
  *out = source;
  return TURBO_OK;
}

int flowie_cluster_delivery_pgsql_source_fetch(void *ctx,
                                               const flowie_cluster_owner_token_t *current_owner,
                                               flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_delivery_pgsql_source_t *source =
      (flowie_cluster_delivery_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_next(
                       source->store, current_owner,
                       FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE, out);
}

int flowie_cluster_delivery_pgsql_source_settle(void *ctx,
                                                const flowie_cluster_owner_token_t *current_owner,
                                                const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_delivery_pgsql_source_t *source =
      (flowie_cluster_delivery_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL
                 : flowie_cluster_pgsql_outbox_settle(source->store, current_owner, event);
}

int flowie_cluster_delivery_pgsql_source_recover(void *ctx) {
  flowie_cluster_delivery_pgsql_source_t *source =
      (flowie_cluster_delivery_pgsql_source_t *)ctx;
  return !source ? TURBO_EINVAL : flowie_cluster_pgsql_fact_store_reopen(&source->store);
}

void flowie_cluster_delivery_pgsql_source_destroy(flowie_cluster_delivery_pgsql_source_t *source) {
  if (!source) return;
  flowie_cluster_pgsql_fact_store_destroy(source->store);
  free(source);
}
