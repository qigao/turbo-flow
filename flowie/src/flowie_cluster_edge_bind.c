#include "flowie_cluster_edge_bind_internal.h"

#include "flow_coronet_execution.h"
#include "turbo_hash.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef enum flowie_cluster_edge_pending_kind_e {
  FLOWIE_CLUSTER_EDGE_PENDING_CONNECT = 1,
  FLOWIE_CLUSTER_EDGE_PENDING_COMMAND
} flowie_cluster_edge_pending_kind_t;

typedef struct flowie_cluster_edge_pending_s {
  struct flowie_cluster_edge_pending_s *previous;
  struct flowie_cluster_edge_pending_s *next;
  flowie_cluster_owner_token_t owner;
  flowie_cluster_edge_pending_kind_t kind;
  flowie_mqtt_version_t mqtt_version;
  flowie_cluster_edge_bind_complete_fn connect_complete;
  flowie_cluster_edge_command_complete_fn command_complete;
  void *complete_ctx;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint8_t correlation_id[FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE];
  int reply_scheduled;
} flowie_cluster_edge_pending_t;

typedef struct flowie_cluster_edge_reply_job_s {
  struct flowie_cluster_edge_bind_s *edge;
  tstr_t encoded;
  uint8_t correlation_id[FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE];
} flowie_cluster_edge_reply_job_t;

struct flowie_cluster_edge_bind_s {
  tf_coronet_execution_t *execution;
  size_t max_payload_size;
  size_t max_pending_entries;
  size_t max_pending_bytes;
  size_t reservation_bytes;
  size_t pending_entries;
  size_t pending_bytes;
  size_t posted_jobs;
  uint64_t correlation_counter;
  turbo_hash_map_t pending;
  flowie_cluster_edge_pending_t *pending_head;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_edge_bind_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_edge_bind_submit_fn submit;
  void *submit_ctx;
  int accepting;
  int close_scheduled;
  int closed;
};

static int flowie_cluster_edge_nonzero(const uint8_t *value, size_t size) {
  uint8_t combined = 0u;
  if (!value) return 0;
  for (size_t index = 0u; index < size; ++index)
    combined |= value[index];
  return combined != 0u;
}

static int flowie_cluster_edge_text_validate(tstr_v value, size_t maximum) {
  if (!value.data || value.len == 0u || value.len > maximum) return TURBO_EINVAL;
  return memchr(value.data, '\0', value.len) ? TURBO_EPROTO : TURBO_OK;
}

static size_t flowie_cluster_edge_correlation_hash(const void *key, size_t key_size, void *ctx) {
  (void)ctx;
  return turbo_hash_bytes(key, key_size, NULL);
}

static bool flowie_cluster_edge_correlation_equal(const void *left, const void *right,
                                                  size_t key_size, void *ctx) {
  (void)ctx;
  return memcmp(left, right, key_size) == 0;
}

static int flowie_cluster_edge_reservation(const flowie_cluster_edge_bind_config_t *config,
                                           size_t *out) {
  size_t frame_size;
  if (!config || !out) return TURBO_EINVAL;
  frame_size = FLOWIE_CLUSTER_PEER_HEADER_SIZE + config->cluster_id.len + config->listener_id.len +
               config->local_node_id.len + FLOWIE_CLUSTER_NODE_ID_MAX;
  if (config->max_payload_size > SIZE_MAX - frame_size) return TURBO_ERANGE;
  frame_size += config->max_payload_size;
  if (frame_size >
      SIZE_MAX - sizeof(flowie_cluster_edge_pending_t) - sizeof(flowie_cluster_edge_reply_job_t))
    return TURBO_ERANGE;
  *out =
      frame_size + sizeof(flowie_cluster_edge_pending_t) + sizeof(flowie_cluster_edge_reply_job_t);
  return TURBO_OK;
}

int flowie_cluster_edge_bind_config_validate(const flowie_cluster_edge_bind_config_t *config) {
  size_t reservation = 0u;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_EDGE_BIND_ABI_V1 || !config->execution ||
      !config->execution->context || config->max_payload_size == 0u ||
      config->max_payload_size > UINT32_MAX || config->max_pending_entries == 0u ||
      config->max_pending_bytes == 0u || !config->resolve || !config->submit ||
      !flowie_cluster_edge_nonzero(config->local_boot_id, sizeof(config->local_boot_id)))
    return TURBO_EINVAL;
  rc = flowie_cluster_edge_text_validate(config->cluster_id, FLOWIE_CLUSTER_ID_MAX);
  if (rc == TURBO_OK)
    rc = flowie_cluster_edge_text_validate(config->listener_id, FLOWIE_CLUSTER_LISTENER_ID_MAX);
  if (rc == TURBO_OK)
    rc = flowie_cluster_edge_text_validate(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX);
  if (rc == TURBO_OK) rc = flowie_cluster_edge_reservation(config, &reservation);
  if (rc != TURBO_OK) return rc;
  return config->max_pending_bytes < reservation ? TURBO_EINVAL : TURBO_OK;
}

static void flowie_cluster_edge_storage_destroy(flowie_cluster_edge_bind_t *edge) {
  if (!edge) return;
  turbo_hash_map_destroy(&edge->pending);
  tstr_freep(&edge->cluster_id);
  tstr_freep(&edge->listener_id);
  tstr_freep(&edge->local_node_id);
  turbo_cond_destroy(&edge->changed);
  turbo_mutex_destroy(&edge->mutex);
  free(edge);
}

int flowie_cluster_edge_bind_create(const flowie_cluster_edge_bind_config_t *config,
                                    flowie_cluster_edge_bind_t **out) {
  flowie_cluster_edge_bind_t *edge;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flowie_cluster_edge_bind_config_validate(config);
  if (rc != TURBO_OK) return rc;
  edge = (flowie_cluster_edge_bind_t *)calloc(1u, sizeof(*edge));
  if (!edge) return TURBO_ENOMEM;
  turbo_mutex_init(&edge->mutex);
  turbo_cond_init(&edge->changed);
  edge->execution = config->execution;
  edge->max_payload_size = config->max_payload_size;
  edge->max_pending_entries = config->max_pending_entries;
  edge->max_pending_bytes = config->max_pending_bytes;
  edge->resolve = config->resolve;
  edge->resolve_ctx = config->resolve_ctx;
  edge->submit = config->submit;
  edge->submit_ctx = config->submit_ctx;
  edge->cluster_id = tstr_from_v(config->cluster_id);
  edge->listener_id = tstr_from_v(config->listener_id);
  edge->local_node_id = tstr_from_v(config->local_node_id);
  memcpy(edge->local_boot_id, config->local_boot_id, sizeof(edge->local_boot_id));
  rc = flowie_cluster_edge_reservation(config, &edge->reservation_bytes);
  if (rc == TURBO_OK && edge->cluster_id && edge->listener_id && edge->local_node_id)
    rc = turbo_hash_map_init(&edge->pending, FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE,
                             sizeof(flowie_cluster_edge_pending_t *),
                             flowie_cluster_edge_correlation_hash,
                             flowie_cluster_edge_correlation_equal, NULL);
  else if (rc == TURBO_OK) rc = TURBO_ENOMEM;
  if (rc == TURBO_OK) rc = turbo_hash_map_reserve(&edge->pending, edge->max_pending_entries);
  if (rc != TURBO_OK) {
    flowie_cluster_edge_storage_destroy(edge);
    return rc;
  }
  edge->accepting = 1;
  *out = edge;
  return TURBO_OK;
}

static void
flowie_cluster_edge_correlation_next(flowie_cluster_edge_bind_t *edge,
                                     uint8_t out[FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE]) {
  uint64_t counter = ++edge->correlation_counter;
  memcpy(out, edge->local_boot_id, FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE);
  for (size_t index = 0u; index < sizeof(counter); ++index)
    out[FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE - 1u - index] ^= (uint8_t)(counter >> (index * 8u));
}

static void flowie_cluster_edge_pending_unlink(flowie_cluster_edge_bind_t *edge,
                                               flowie_cluster_edge_pending_t *pending) {
  if (pending->previous) pending->previous->next = pending->next;
  else edge->pending_head = pending->next;
  if (pending->next) pending->next->previous = pending->previous;
  pending->previous = NULL;
  pending->next = NULL;
}

static int flowie_cluster_edge_pending_remove(flowie_cluster_edge_bind_t *edge,
                                              flowie_cluster_edge_pending_t *pending) {
  int rc;
  if (!edge || !pending) return TURBO_EINVAL;
  rc = turbo_hash_map_remove(&edge->pending, pending->correlation_id, NULL);
  if (rc != TURBO_OK) return TURBO_EPROTO;
  flowie_cluster_edge_pending_unlink(edge, pending);
  if (edge->pending_entries == 0u || edge->pending_bytes < edge->reservation_bytes)
    return TURBO_EPROTO;
  --edge->pending_entries;
  edge->pending_bytes -= edge->reservation_bytes;
  turbo_cond_broadcast(&edge->changed);
  return TURBO_OK;
}

static int flowie_cluster_edge_pending_submit(flowie_cluster_edge_bind_t *edge,
                                              flowie_cluster_edge_pending_t *pending,
                                              flowie_cluster_peer_operation_t operation,
                                              tstr_v payload) {
  flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  int rc = TURBO_OK;
  turbo_mutex_lock(&edge->mutex);
  if (!edge->accepting) rc = TURBO_ESHUTDOWN;
  else if (edge->correlation_counter == UINT64_MAX) rc = TURBO_ERANGE;
  else if (edge->pending_entries >= edge->max_pending_entries ||
           edge->reservation_bytes > edge->max_pending_bytes - edge->pending_bytes)
    rc = TURBO_ENOSPC;
  else {
    flowie_cluster_edge_correlation_next(edge, pending->correlation_id);
    rc = turbo_hash_map_put(&edge->pending, pending->correlation_id, &pending);
    if (rc == TURBO_OK) {
      pending->next = edge->pending_head;
      if (edge->pending_head) edge->pending_head->previous = pending;
      edge->pending_head = pending;
      ++edge->pending_entries;
      edge->pending_bytes += edge->reservation_bytes;
    }
  }
  turbo_mutex_unlock(&edge->mutex);
  if (rc != TURBO_OK) return rc;

  command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  command.operation = operation;
  command.shard_id = pending->owner.shard_id;
  command.owner_epoch = pending->owner.owner_epoch;
  command.connection_id = pending->connection_id;
  command.connection_generation = pending->connection_generation;
  command.cluster_id = tstr_to_v(edge->cluster_id);
  command.listener_id = tstr_to_v(edge->listener_id);
  command.source_node_id = tstr_to_v(edge->local_node_id);
  command.target_node_id = tstr_v_from_buf(pending->owner.node_id, pending->owner.node_id_size);
  command.payload = payload;
  memcpy(command.source_boot_id, edge->local_boot_id, sizeof(command.source_boot_id));
  memcpy(command.target_boot_id, pending->owner.boot_id, sizeof(command.target_boot_id));
  memcpy(command.correlation_id, pending->correlation_id, sizeof(command.correlation_id));
  rc = edge->submit(edge->submit_ctx, &command);
  if (rc == TURBO_OK) return TURBO_OK;

  turbo_mutex_lock(&edge->mutex);
  {
    flowie_cluster_edge_pending_t *const *found =
        (flowie_cluster_edge_pending_t *const *)turbo_hash_map_get_const(&edge->pending,
                                                                         pending->correlation_id);
    if (found && *found == pending) (void)flowie_cluster_edge_pending_remove(edge, pending);
  }
  turbo_mutex_unlock(&edge->mutex);
  return rc;
}

int flowie_cluster_edge_bind_connect(flowie_cluster_edge_bind_t *edge, uint64_t connection_id,
                                     uint64_t connection_generation,
                                     const flowie_mqtt_connect_view_t *connect,
                                     const turbo_flow_security_principal_t *principal,
                                     const flowie_cluster_peer_ingress_metadata_t *metadata,
                                     flowie_cluster_edge_bind_complete_fn complete,
                                     void *complete_ctx) {
  flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  flowie_cluster_edge_pending_t *pending = NULL;
  tstr_t payload = NULL;
  int rc;
  if (!edge || !connect || !complete || connection_id == 0u || connection_generation == 0u)
    return TURBO_EINVAL;
  if (coro_context_current() != edge->execution->context) return TURBO_EBUSY;
  rc = edge->resolve(edge->resolve_ctx, connect->client_id, &owner);
  if (rc == TURBO_OK) rc = flowie_cluster_owner_token_require(&owner, &owner);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connect_bind_encode_with_metadata(
        connect, principal, metadata, edge->max_payload_size, &payload);
  if (rc != TURBO_OK) return rc;
  pending = (flowie_cluster_edge_pending_t *)calloc(1u, sizeof(*pending));
  if (!pending) {
    tstr_free(payload);
    return TURBO_ENOMEM;
  }
  pending->owner = owner;
  pending->kind = FLOWIE_CLUSTER_EDGE_PENDING_CONNECT;
  pending->connect_complete = complete;
  pending->complete_ctx = complete_ctx;
  pending->connection_id = connection_id;
  pending->connection_generation = connection_generation;

  rc = flowie_cluster_edge_pending_submit(edge, pending, FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND,
                                          tstr_to_v(payload));
  tstr_free(payload);
  if (rc != TURBO_OK) free(pending);
  return rc;
}

static int flowie_cluster_edge_action_submit(flowie_cluster_edge_bind_t *edge,
                                             uint64_t connection_id, uint64_t connection_generation,
                                             const flowie_cluster_owner_token_t *owner,
                                             flowie_cluster_peer_operation_t operation,
                                             flowie_mqtt_version_t mqtt_version, tstr_v payload,
                                             flowie_cluster_edge_command_complete_fn complete,
                                             void *complete_ctx) {
  flowie_cluster_edge_pending_t *pending;
  int rc;
  if (!edge || !owner || !complete) return TURBO_EINVAL;
  pending = (flowie_cluster_edge_pending_t *)calloc(1u, sizeof(*pending));
  if (!pending) return TURBO_ENOMEM;
  pending->owner = *owner;
  pending->kind = FLOWIE_CLUSTER_EDGE_PENDING_COMMAND;
  pending->mqtt_version = mqtt_version;
  pending->command_complete = complete;
  pending->complete_ctx = complete_ctx;
  pending->connection_id = connection_id;
  pending->connection_generation = connection_generation;
  rc = flowie_cluster_edge_pending_submit(edge, pending, operation, payload);
  if (rc != TURBO_OK) free(pending);
  return rc;
}

int flowie_cluster_edge_bind_command(
    flowie_cluster_edge_bind_t *edge, uint64_t connection_id, uint64_t connection_generation,
    const flowie_cluster_owner_token_t *expected_owner, flowie_cluster_peer_operation_t operation,
    flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
    flowie_cluster_edge_command_complete_fn complete, void *complete_ctx) {
  flowie_cluster_owner_token_t current = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  tstr_t payload = NULL;
  int rc;
  if (!edge || !expected_owner || !complete || connection_id == 0u || connection_generation == 0u)
    return TURBO_EINVAL;
  if (coro_context_current() != edge->execution->context) return TURBO_EBUSY;
  rc = edge->resolve(edge->resolve_ctx, client_id, &current);
  if (rc == TURBO_OK) rc = flowie_cluster_owner_token_require(&current, expected_owner);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_mqtt_command_encode(operation, mqtt_version, client_id, packet,
                                                 edge->max_payload_size, &payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_edge_action_submit(edge, connection_id, connection_generation, &current,
                                         operation, mqtt_version, tstr_to_v(payload), complete,
                                         complete_ctx);
  tstr_free(payload);
  return rc;
}

int flowie_cluster_edge_bind_publish_settle(
    flowie_cluster_edge_bind_t *edge, uint64_t connection_id, uint64_t connection_generation,
    const flowie_cluster_owner_token_t *expected_owner, flowie_mqtt_span_t client_id,
    const turbo_flow_protocol_settlement_request_t *settlement,
    flowie_cluster_edge_command_complete_fn complete, void *complete_ctx) {
  flowie_cluster_owner_token_t current = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  tstr_t payload = NULL;
  int rc;
  if (!edge || !expected_owner || !settlement || !complete || connection_id == 0u ||
      connection_generation == 0u)
    return TURBO_EINVAL;
  if (coro_context_current() != edge->execution->context) return TURBO_EBUSY;
  rc = edge->resolve(edge->resolve_ctx, client_id, &current);
  if (rc == TURBO_OK) rc = flowie_cluster_owner_token_require(&current, expected_owner);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_publish_settle_encode(client_id, settlement,
                                                   edge->max_payload_size, &payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_edge_action_submit(
      edge, connection_id, connection_generation, &current,
      FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE,
      (flowie_mqtt_version_t)settlement->message.protocol_version, tstr_to_v(payload), complete,
      complete_ctx);
  tstr_free(payload);
  return rc;
}

int flowie_cluster_edge_bind_connection_lost(flowie_cluster_edge_bind_t *edge,
                                             uint64_t connection_id, uint64_t connection_generation,
                                             const flowie_cluster_owner_token_t *expected_owner,
                                             flowie_mqtt_version_t mqtt_version,
                                             flowie_mqtt_span_t client_id,
                                             flowie_cluster_edge_command_complete_fn complete,
                                             void *complete_ctx) {
  flowie_cluster_owner_token_t current = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  tstr_t payload = NULL;
  int rc;
  if (!edge || !expected_owner || !complete || connection_id == 0u || connection_generation == 0u)
    return TURBO_EINVAL;
  if (coro_context_current() != edge->execution->context) return TURBO_EBUSY;
  rc = edge->resolve(edge->resolve_ctx, client_id, &current);
  if (rc == TURBO_OK) rc = flowie_cluster_owner_token_require(&current, expected_owner);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connection_lost_encode(mqtt_version, client_id, edge->max_payload_size,
                                                    &payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_edge_action_submit(edge, connection_id, connection_generation, &current,
                                         FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST,
                                         mqtt_version, tstr_to_v(payload), complete, complete_ctx);
  tstr_free(payload);
  return rc;
}

static void flowie_cluster_edge_command_reply_init(
    const flowie_cluster_edge_bind_t *edge, const flowie_cluster_peer_frame_t *command,
    flowie_cluster_peer_operation_t operation, int status, tstr_t payload,
    flowie_cluster_peer_frame_t *reply) {
  *reply = (flowie_cluster_peer_frame_t)FLOWIE_CLUSTER_PEER_FRAME_INIT;
  reply->kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply->operation = operation;
  reply->shard_id = command->shard_id;
  reply->status = status;
  reply->owner_epoch = command->owner_epoch;
  reply->connection_id = command->connection_id;
  reply->connection_generation = command->connection_generation;
  reply->cluster_id = tstr_to_v(edge->cluster_id);
  reply->listener_id = tstr_to_v(edge->listener_id);
  reply->source_node_id = tstr_to_v(edge->local_node_id);
  reply->target_node_id = command->source_node_id;
  reply->payload = payload ? tstr_to_v(payload) : tstr_v_from_buf(NULL, 0u);
  memcpy(reply->source_boot_id, edge->local_boot_id, sizeof(reply->source_boot_id));
  memcpy(reply->target_boot_id, command->source_boot_id, sizeof(reply->target_boot_id));
  memcpy(reply->correlation_id, command->correlation_id, sizeof(reply->correlation_id));
}

int flowie_cluster_edge_bind_takeover_close(
    flowie_cluster_edge_bind_t *edge, const flowie_cluster_peer_frame_t *command,
    uint64_t expected_connection_id, uint64_t expected_connection_generation,
    flowie_mqtt_version_t expected_mqtt_version, flowie_mqtt_span_t expected_client_id,
    flowie_cluster_edge_takeover_close_fn close_socket, void *close_ctx) {
  flowie_cluster_peer_takeover_close_view_t decoded = FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_VIEW_INIT;
  flowie_cluster_owner_token_t current = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  flowie_cluster_owner_token_t presented = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  flowie_cluster_peer_frame_t reply;
  tstr_t reply_payload = NULL;
  size_t encoded_size = 0u;
  size_t reply_packet_limit;
  int accepting;
  int status;
  int rc;
  if (!edge || !command || !close_socket || expected_connection_id == 0u ||
      expected_connection_generation == 0u ||
      !flowie_mqtt_version_is_supported(expected_mqtt_version) || !expected_client_id.data ||
      expected_client_id.size == 0u || !flowie_mqtt_utf8_validate(expected_client_id))
    return TURBO_EINVAL;
  if (coro_context_current() != edge->execution->context) return TURBO_EBUSY;
  rc = flowie_cluster_peer_frame_encoded_size(command, edge->max_payload_size, &encoded_size);
  if (rc != TURBO_OK) return rc;
  if (command->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      command->operation != FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE)
    return TURBO_EPROTO;
  rc = flowie_cluster_peer_frame_require_target(
      command, tstr_to_v(edge->cluster_id), tstr_to_v(edge->local_node_id), edge->local_boot_id);
  if (rc != TURBO_OK || !tstr_v_eq(command->listener_id, tstr_to_v(edge->listener_id)))
    return TURBO_EPROTO;

  turbo_mutex_lock(&edge->mutex);
  accepting = edge->accepting;
  turbo_mutex_unlock(&edge->mutex);
  status = accepting ? TURBO_OK : TURBO_ESHUTDOWN;
  if (status == TURBO_OK)
    status = flowie_cluster_peer_takeover_close_decode(command->payload.data, command->payload.len,
                                                       edge->max_payload_size, &decoded);
  if (status == TURBO_OK) status = edge->resolve(edge->resolve_ctx, decoded.client_id, &current);
  if (status == TURBO_OK)
    status = flowie_cluster_owner_token_init(&presented, command->shard_id, command->owner_epoch,
                                             command->source_node_id.data,
                                             command->source_node_id.len, command->source_boot_id);
  if (status == TURBO_OK) status = flowie_cluster_owner_token_require(&current, &presented);
  if (status == TURBO_OK &&
      (command->connection_id != expected_connection_id ||
       command->connection_generation != expected_connection_generation ||
       decoded.mqtt_version != expected_mqtt_version ||
       decoded.client_id.size != expected_client_id.size ||
       memcmp(decoded.client_id.data, expected_client_id.data, expected_client_id.size) != 0))
    status = TURBO_EBUSY;
  reply_packet_limit = edge->max_payload_size < FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE
                           ? edge->max_payload_size
                           : FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
  if (status == TURBO_OK && edge->max_payload_size < FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE)
    status = TURBO_EMSGSIZE;
  if (status == TURBO_OK)
    status = flowie_cluster_peer_mqtt_reply_encode(
        decoded.mqtt_version, (flowie_mqtt_span_t){NULL, 0u}, 0,
        (turbo_flow_protocol_settlement_point_t)0, reply_packet_limit, &reply_payload);
  if (status == TURBO_OK) {
    status = close_socket(close_ctx);
    if (status == TURBO_EALREADY) status = TURBO_OK;
  }
  if (status != TURBO_OK) tstr_freep(&reply_payload);
  flowie_cluster_edge_command_reply_init(edge, command,
                                         FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY, status,
                                         reply_payload, &reply);
  rc = edge->submit(edge->submit_ctx, &reply);
  tstr_free(reply_payload);
  return rc;
}

int flowie_cluster_edge_bind_apply_action(
    flowie_cluster_edge_bind_t *edge, const flowie_cluster_peer_frame_t *command,
    const flowie_cluster_owner_token_t *expected_owner, uint64_t expected_connection_id,
    uint64_t expected_connection_generation, flowie_mqtt_version_t expected_mqtt_version,
    uint64_t *last_applied_sequence, flowie_cluster_edge_action_apply_fn apply, void *apply_ctx) {
  flowie_cluster_peer_edge_action_t decoded = FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT;
  flowie_cluster_owner_token_t presented = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  flowie_cluster_peer_frame_t reply;
  tstr_t reply_payload = NULL;
  size_t encoded_size = 0u;
  int accepting;
  int status;
  int rc;
  if (!edge || !command || !expected_owner || expected_connection_id == 0u ||
      expected_connection_generation == 0u ||
      !flowie_mqtt_version_is_supported(expected_mqtt_version) || !last_applied_sequence ||
      !apply || flowie_cluster_owner_token_require(expected_owner, expected_owner) != TURBO_OK)
    return TURBO_EINVAL;
  if (coro_context_current() != edge->execution->context) return TURBO_EBUSY;
  rc = flowie_cluster_peer_frame_encoded_size(command, edge->max_payload_size, &encoded_size);
  if (rc != TURBO_OK) return rc;
  if (command->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      command->operation != FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION)
    return TURBO_EPROTO;
  rc = flowie_cluster_peer_frame_require_target(
      command, tstr_to_v(edge->cluster_id), tstr_to_v(edge->local_node_id), edge->local_boot_id);
  if (rc != TURBO_OK || !tstr_v_eq(command->listener_id, tstr_to_v(edge->listener_id)))
    return TURBO_EPROTO;

  turbo_mutex_lock(&edge->mutex);
  accepting = edge->accepting;
  turbo_mutex_unlock(&edge->mutex);
  status = accepting ? TURBO_OK : TURBO_ESHUTDOWN;
  if (status == TURBO_OK)
    status = flowie_cluster_peer_edge_action_decode(command->payload.data, command->payload.len,
                                                    edge->max_payload_size, &decoded);
  if (status == TURBO_OK)
    status = flowie_cluster_owner_token_init(&presented, command->shard_id, command->owner_epoch,
                                             command->source_node_id.data,
                                             command->source_node_id.len, command->source_boot_id);
  if (status == TURBO_OK)
    status = flowie_cluster_owner_token_require(expected_owner, &presented);
  if (status == TURBO_OK &&
      (command->connection_id != expected_connection_id ||
       command->connection_generation != expected_connection_generation ||
       decoded.action.mqtt_version != expected_mqtt_version))
    status = TURBO_EBUSY;
  if (status == TURBO_OK && decoded.action_sequence > *last_applied_sequence) {
    if (*last_applied_sequence == UINT64_MAX ||
        decoded.action_sequence != *last_applied_sequence + 1u)
      status = TURBO_EBUSY;
    else {
      status = apply(apply_ctx, &decoded.action);
      if (status == TURBO_OK) *last_applied_sequence = decoded.action_sequence;
    }
  }
  if (status == TURBO_OK)
    status = flowie_cluster_peer_edge_action_ack_encode(decoded.action_sequence, &reply_payload);
  if (status == TURBO_OK && tstr_len(reply_payload) > edge->max_payload_size) {
    tstr_freep(&reply_payload);
    status = TURBO_EMSGSIZE;
  }
  if (status != TURBO_OK) tstr_freep(&reply_payload);
  flowie_cluster_edge_command_reply_init(edge, command,
                                         FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK, status,
                                         reply_payload, &reply);
  rc = edge->submit(edge->submit_ctx, &reply);
  tstr_free(reply_payload);
  return rc;
}

static int flowie_cluster_edge_reply_validate(flowie_cluster_edge_bind_t *edge,
                                              const flowie_cluster_edge_pending_t *pending,
                                              const flowie_cluster_peer_frame_t *frame,
                                              flowie_cluster_session_bind_reply_view_t *decoded,
                                              flowie_cluster_peer_mqtt_reply_action_t *action,
                                              int *frame_valid) {
  int rc;
  if (frame_valid) *frame_valid = 0;
  if (!edge || !pending || !frame || !decoded || !action || !frame_valid ||
      frame->kind != FLOWIE_CLUSTER_PEER_FRAME_REPLY ||
      frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY ||
      frame->listener_id.len != tstr_len(edge->listener_id) ||
      memcmp(frame->listener_id.data, edge->listener_id, frame->listener_id.len) != 0 ||
      frame->shard_id != pending->owner.shard_id ||
      frame->owner_epoch != pending->owner.owner_epoch ||
      frame->connection_id != pending->connection_id ||
      frame->connection_generation != pending->connection_generation ||
      frame->source_node_id.len != pending->owner.node_id_size ||
      memcmp(frame->source_node_id.data, pending->owner.node_id, pending->owner.node_id_size) !=
          0 ||
      memcmp(frame->source_boot_id, pending->owner.boot_id, sizeof(frame->source_boot_id)) != 0)
    return TURBO_EPROTO;
  rc = flowie_cluster_peer_frame_require_target(
      frame, tstr_to_v(edge->cluster_id), tstr_to_v(edge->local_node_id), edge->local_boot_id);
  if (rc != TURBO_OK) return rc;
  if (frame->status != TURBO_OK) {
    if (frame->payload.len != 0u) return TURBO_EPROTO;
    *frame_valid = 1;
    return frame->status;
  }
  if (!frame->payload.data || frame->payload.len == 0u) return TURBO_EPROTO;
  if (pending->kind == FLOWIE_CLUSTER_EDGE_PENDING_CONNECT)
    rc = flowie_cluster_session_bind_reply_decode(frame->payload.data, frame->payload.len,
                                                  edge->max_payload_size, decoded);
  else {
    rc = flowie_cluster_peer_mqtt_reply_decode(frame->payload.data, frame->payload.len,
                                               edge->max_payload_size, action);
    if (rc == TURBO_OK && action->mqtt_version != pending->mqtt_version) rc = TURBO_EPROTO;
  }
  if (rc == TURBO_OK) *frame_valid = 1;
  return rc;
}

static void flowie_cluster_edge_reply_run(void *arg1, void *arg2) {
  flowie_cluster_edge_reply_job_t *job = (flowie_cluster_edge_reply_job_t *)arg1;
  flowie_cluster_edge_bind_t *edge;
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  flowie_cluster_session_bind_reply_view_t reply = FLOWIE_CLUSTER_SESSION_BIND_REPLY_VIEW_INIT;
  flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
  flowie_cluster_edge_pending_t *pending = NULL;
  flowie_cluster_edge_bind_complete_fn complete = NULL;
  flowie_cluster_edge_command_complete_fn command_complete = NULL;
  void *complete_ctx = NULL;
  size_t consumed = 0u;
  int frame_valid = 0;
  int rc;
  (void)arg2;
  if (!job || !(edge = job->edge)) return;
  rc = flowie_cluster_peer_frame_decode(job->encoded, tstr_len(job->encoded),
                                        edge->max_payload_size, &frame, &consumed);
  if (rc == TURBO_OK && consumed != tstr_len(job->encoded)) rc = TURBO_EPROTO;
  turbo_mutex_lock(&edge->mutex);
  {
    flowie_cluster_edge_pending_t *const *found =
        (flowie_cluster_edge_pending_t *const *)turbo_hash_map_get_const(&edge->pending,
                                                                         job->correlation_id);
    if (found) pending = *found;
  }
  turbo_mutex_unlock(&edge->mutex);
  /* CONNECT submission, reply completion and close cancellation are serialized
   * by the same edge execution, so a found pending entry remains live while its
   * reply is validated without holding the cross-thread admission mutex. */
  if (pending && rc == TURBO_OK)
    rc = flowie_cluster_edge_reply_validate(edge, pending, &frame, &reply, &action, &frame_valid);
  turbo_mutex_lock(&edge->mutex);
  {
    flowie_cluster_edge_pending_t *const *found =
        (flowie_cluster_edge_pending_t *const *)turbo_hash_map_get_const(&edge->pending,
                                                                         job->correlation_id);
    if (!found || *found != pending) pending = NULL;
  }
  if (pending) {
    complete = pending->connect_complete;
    command_complete = pending->command_complete;
    complete_ctx = pending->complete_ctx;
    if (flowie_cluster_edge_pending_remove(edge, pending) != TURBO_OK) {
      rc = TURBO_EPROTO;
      frame_valid = 0;
    }
  } else if (rc == TURBO_OK) {
    rc = TURBO_ENOENT;
  }
  if (edge->posted_jobs != 0u) --edge->posted_jobs;
  turbo_cond_broadcast(&edge->changed);
  turbo_mutex_unlock(&edge->mutex);

  free(pending);
  if (complete)
    complete(complete_ctx, rc, frame_valid ? &frame : NULL, rc == TURBO_OK ? &reply : NULL);
  else if (command_complete)
    command_complete(complete_ctx, rc, frame_valid ? &frame : NULL,
                     rc == TURBO_OK ? &action : NULL);
  flowie_cluster_peer_frame_cleanup(&frame);
  tstr_free(job->encoded);
  free(job);
}

int flowie_cluster_edge_bind_reply(void *ctx, const flowie_cluster_peer_frame_t *reply) {
  flowie_cluster_edge_bind_t *edge = (flowie_cluster_edge_bind_t *)ctx;
  flowie_cluster_edge_pending_t *pending = NULL;
  flowie_cluster_edge_reply_job_t *job;
  int rc;
  if (!edge || !reply ||
      !flowie_cluster_edge_nonzero(reply->correlation_id, sizeof(reply->correlation_id)))
    return TURBO_EINVAL;
  turbo_mutex_lock(&edge->mutex);
  {
    flowie_cluster_edge_pending_t *const *found =
        (flowie_cluster_edge_pending_t *const *)turbo_hash_map_get_const(&edge->pending,
                                                                         reply->correlation_id);
    if (found) pending = *found;
  }
  if (!pending) rc = TURBO_ENOENT;
  else if (pending->reply_scheduled) rc = TURBO_EALREADY;
  else {
    pending->reply_scheduled = 1;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&edge->mutex);
  if (rc != TURBO_OK) return rc;
  job = (flowie_cluster_edge_reply_job_t *)calloc(1u, sizeof(*job));
  if (!job) rc = TURBO_ENOMEM;
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_frame_encode(reply, edge->max_payload_size, &job->encoded);
  if (rc == TURBO_OK) {
    job->edge = edge;
    memcpy(job->correlation_id, reply->correlation_id, sizeof(job->correlation_id));
    turbo_mutex_lock(&edge->mutex);
    ++edge->posted_jobs;
    turbo_mutex_unlock(&edge->mutex);
    rc = tf_coronet_execution_post(edge->execution, flowie_cluster_edge_reply_run, job, NULL);
    if (rc == TURBO_OK) return TURBO_OK;
    turbo_mutex_lock(&edge->mutex);
    if (edge->posted_jobs != 0u) --edge->posted_jobs;
    turbo_cond_broadcast(&edge->changed);
    turbo_mutex_unlock(&edge->mutex);
  }
  turbo_mutex_lock(&edge->mutex);
  {
    flowie_cluster_edge_pending_t *const *found =
        (flowie_cluster_edge_pending_t *const *)turbo_hash_map_get_const(&edge->pending,
                                                                         reply->correlation_id);
    if (found && *found == pending) pending->reply_scheduled = 0;
  }
  turbo_mutex_unlock(&edge->mutex);
  if (job) {
    tstr_free(job->encoded);
    free(job);
  }
  return rc;
}

int flowie_cluster_edge_bind_pending(flowie_cluster_edge_bind_t *edge, size_t *entries,
                                     size_t *bytes) {
  if (!edge || !entries || !bytes) return TURBO_EINVAL;
  turbo_mutex_lock(&edge->mutex);
  *entries = edge->pending_entries;
  *bytes = edge->pending_bytes;
  turbo_mutex_unlock(&edge->mutex);
  return TURBO_OK;
}

static void flowie_cluster_edge_close_run(void *arg1, void *arg2) {
  flowie_cluster_edge_bind_t *edge = (flowie_cluster_edge_bind_t *)arg1;
  flowie_cluster_edge_pending_t *pending;
  (void)arg2;
  if (!edge) return;
  for (;;) {
    flowie_cluster_edge_bind_complete_fn complete;
    flowie_cluster_edge_command_complete_fn command_complete;
    void *complete_ctx;
    turbo_mutex_lock(&edge->mutex);
    pending = edge->pending_head;
    if (!pending) {
      edge->closed = 1;
      if (edge->posted_jobs != 0u) --edge->posted_jobs;
      turbo_cond_broadcast(&edge->changed);
      turbo_mutex_unlock(&edge->mutex);
      return;
    }
    complete = pending->connect_complete;
    command_complete = pending->command_complete;
    complete_ctx = pending->complete_ctx;
    (void)flowie_cluster_edge_pending_remove(edge, pending);
    turbo_mutex_unlock(&edge->mutex);
    free(pending);
    if (complete) complete(complete_ctx, TURBO_ESHUTDOWN, NULL, NULL);
    else if (command_complete) command_complete(complete_ctx, TURBO_ESHUTDOWN, NULL, NULL);
  }
}

int flowie_cluster_edge_bind_close(flowie_cluster_edge_bind_t *edge) {
  int rc;
  if (!edge) return TURBO_EINVAL;
  turbo_mutex_lock(&edge->mutex);
  edge->accepting = 0;
  if (edge->closed || edge->close_scheduled) {
    turbo_mutex_unlock(&edge->mutex);
    return TURBO_OK;
  }
  edge->close_scheduled = 1;
  ++edge->posted_jobs;
  turbo_mutex_unlock(&edge->mutex);
  rc = tf_coronet_execution_post(edge->execution, flowie_cluster_edge_close_run, edge, NULL);
  if (rc == TURBO_OK) return TURBO_OK;
  turbo_mutex_lock(&edge->mutex);
  edge->close_scheduled = 0;
  if (edge->posted_jobs != 0u) --edge->posted_jobs;
  turbo_cond_broadcast(&edge->changed);
  turbo_mutex_unlock(&edge->mutex);
  return rc;
}

int flowie_cluster_edge_bind_drain(flowie_cluster_edge_bind_t *edge, uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc;
  if (!edge) return TURBO_EINVAL;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  turbo_mutex_lock(&edge->mutex);
  while (!edge->closed || edge->pending_entries != 0u || edge->posted_jobs != 0u) {
    uint64_t now_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&edge->changed, &edge->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    if (turbo_cond_timedwait(&edge->changed, &edge->mutex, deadline_ns - now_ns) != TURBO_OK) break;
  }
  rc = edge->closed && edge->pending_entries == 0u && edge->posted_jobs == 0u ? TURBO_OK
       : timeout_ns == 0u                                                     ? TURBO_EBUSY
                                                                              : TURBO_ETIMEDOUT;
  turbo_mutex_unlock(&edge->mutex);
  return rc;
}

int flowie_cluster_edge_bind_destroy(flowie_cluster_edge_bind_t *edge) {
  if (!edge) return TURBO_EINVAL;
  turbo_mutex_lock(&edge->mutex);
  if (!edge->closed || edge->pending_entries != 0u || edge->posted_jobs != 0u) {
    turbo_mutex_unlock(&edge->mutex);
    return TURBO_EBUSY;
  }
  turbo_mutex_unlock(&edge->mutex);
  flowie_cluster_edge_storage_destroy(edge);
  return TURBO_OK;
}
