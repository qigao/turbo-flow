#include "flowie_cluster_endpoint_binding_internal.h"

#include "flowie_cluster_peer_wire_internal.h"
#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_endpoint_connection_s flowie_cluster_endpoint_connection_t;

typedef struct flowie_cluster_endpoint_action_job_s {
  struct flowie_cluster_endpoint_binding_s *binding;
  tstr_t encoded;
  size_t reserved_bytes;
} flowie_cluster_endpoint_action_job_t;

struct flowie_cluster_endpoint_connection_s {
  struct flowie_cluster_endpoint_binding_s *binding;
  flowie_endpoint_cluster_socket_port_t socket_port;
  flowie_endpoint_cluster_complete_fn complete;
  void *complete_ctx;
  flowie_cluster_owner_token_t owner;
  turbo_flow_protocol_route_t owner_route;
  tstr_t client_id;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t last_applied_sequence;
  flowie_mqtt_version_t mqtt_version;
  size_t reserved_bytes;
  int pending;
  int detached;
  int public_completion;
};

TURBO_HASH_MAP_DEFINE(flowie_cluster_endpoint_connection_map_t, uint64_t,
                      flowie_cluster_endpoint_connection_t *)

struct flowie_cluster_endpoint_binding_s {
  flowie_cluster_endpoint_connection_map_t connections;
  flowie_cluster_edge_bind_t *edge;
  flowie_cluster_node_t *node;
  flowie_cluster_owner_directory_t *owners;
  tf_coronet_execution_t *execution;
  flowie_endpoint_cluster_binding_t port;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  size_t max_payload_size;
  size_t max_connections;
  size_t max_connection_bytes;
  size_t connection_count;
  size_t connection_bytes;
  size_t max_inbound_actions;
  size_t max_inbound_bytes;
  size_t inbound_actions;
  size_t inbound_bytes;
  int map_initialized;
  int edge_registered;
  int accepting;
  int closed;
};

static int flowie_cluster_endpoint_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t value = 0u;
  if (!bytes) return 0;
  for (size_t i = 0u; i < size; ++i) value |= bytes[i];
  return value != 0u;
}

static int flowie_cluster_endpoint_text_valid(tstr_v value, size_t maximum) {
  return value.data && value.len != 0u && value.len <= maximum &&
                 !memchr(value.data, '\0', value.len)
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flowie_cluster_endpoint_config_validate(
    const flowie_cluster_endpoint_binding_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_ENDPOINT_BINDING_ABI_V1 || !config->execution ||
      !config->execution->context || !config->node || !config->owners ||
      config->max_payload_size == 0u || config->max_payload_size > UINT32_MAX ||
      config->max_connections == 0u || config->max_connection_bytes == 0u ||
      config->max_pending_bytes == 0u ||
      config->max_inbound_actions == 0u || config->max_inbound_bytes == 0u ||
      config->request_timeout_ms == 0u ||
      !flowie_cluster_endpoint_nonzero(config->local_boot_id, sizeof(config->local_boot_id)))
    return TURBO_EINVAL;
  if (flowie_cluster_endpoint_text_valid(config->cluster_id, FLOWIE_CLUSTER_ID_MAX) != TURBO_OK ||
      flowie_cluster_endpoint_text_valid(config->listener_id, FLOWIE_CLUSTER_LISTENER_ID_MAX) !=
          TURBO_OK ||
      flowie_cluster_endpoint_text_valid(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX) !=
          TURBO_OK)
    return TURBO_EINVAL;
  if (config->max_inbound_bytes < sizeof(flowie_cluster_endpoint_action_job_t) +
                                      FLOWIE_CLUSTER_PEER_HEADER_SIZE)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static flowie_cluster_endpoint_connection_t *flowie_cluster_endpoint_connection_find(
    flowie_cluster_endpoint_binding_t *binding, uint64_t connection_id,
    uint64_t connection_generation) {
  flowie_cluster_endpoint_connection_t **slot;
  if (!binding || connection_id == 0u || connection_generation == 0u) return NULL;
  slot = flowie_cluster_endpoint_connection_map_t_get(&binding->connections, connection_id);
  return slot && *slot && (*slot)->connection_generation == connection_generation ? *slot : NULL;
}

static void flowie_cluster_endpoint_connection_free(
    flowie_cluster_endpoint_connection_t *connection) {
  flowie_cluster_endpoint_binding_t *binding;
  if (!connection || !(binding = connection->binding)) return;
  turbo_mutex_lock(&binding->mutex);
  if (binding->connection_count != 0u) --binding->connection_count;
  if (binding->connection_bytes >= connection->reserved_bytes)
    binding->connection_bytes -= connection->reserved_bytes;
  turbo_cond_broadcast(&binding->changed);
  turbo_mutex_unlock(&binding->mutex);
  tstr_free(connection->client_id);
  free(connection);
}

static void flowie_cluster_endpoint_connection_reservation_release(
    flowie_cluster_endpoint_binding_t *binding, size_t reservation) {
  turbo_mutex_lock(&binding->mutex);
  if (binding->connection_count != 0u) --binding->connection_count;
  if (binding->connection_bytes >= reservation) binding->connection_bytes -= reservation;
  turbo_cond_broadcast(&binding->changed);
  turbo_mutex_unlock(&binding->mutex);
}

static int flowie_cluster_endpoint_owner_from_frame(
    const flowie_cluster_peer_frame_t *frame, flowie_cluster_owner_token_t *out) {
  if (!frame || !out || frame->source_node_id.len == 0u ||
      frame->source_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || frame->owner_epoch == 0u ||
      !flowie_cluster_endpoint_nonzero(frame->source_boot_id, sizeof(frame->source_boot_id)))
    return TURBO_EPROTO;
  *out = (flowie_cluster_owner_token_t)FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  out->shard_id = frame->shard_id;
  out->owner_epoch = frame->owner_epoch;
  out->node_id_size = frame->source_node_id.len;
  memcpy(out->node_id, frame->source_node_id.data, frame->source_node_id.len);
  out->node_id[frame->source_node_id.len] = '\0';
  memcpy(out->boot_id, frame->source_boot_id, sizeof(out->boot_id));
  return TURBO_OK;
}

static void flowie_cluster_endpoint_finish(
    flowie_cluster_endpoint_connection_t *connection, int status,
    const flowie_endpoint_cluster_action_t *action) {
  flowie_endpoint_cluster_complete_fn complete;
  void *complete_ctx;
  int detached;
  if (!connection || !connection->pending) return;
  complete = connection->complete;
  complete_ctx = connection->complete_ctx;
  detached = connection->detached;
  connection->pending = 0;
  connection->complete = NULL;
  connection->complete_ctx = NULL;
  if (detached) {
    flowie_cluster_endpoint_connection_free(connection);
    return;
  }
  if (connection->public_completion && complete) complete(complete_ctx, status, action);
}

static void flowie_cluster_endpoint_connect_complete(
    void *ctx, int status, const flowie_cluster_peer_frame_t *frame,
    const flowie_cluster_session_bind_reply_view_t *reply) {
  flowie_cluster_endpoint_connection_t *connection =
      (flowie_cluster_endpoint_connection_t *)ctx;
  flowie_endpoint_cluster_action_t action = FLOWIE_ENDPOINT_CLUSTER_ACTION_INIT;
  if (!connection) return;
  if (status == TURBO_OK) {
    status = flowie_cluster_endpoint_owner_from_frame(frame, &connection->owner);
    if (status == TURBO_OK && (!reply || !reply->packet.data || reply->packet.size == 0u))
      status = TURBO_EPROTO;
  }
  if (status == TURBO_OK) {
    action.mqtt_version = connection->mqtt_version;
    action.packet = reply->packet;
    action.close_after_send = reply->close_after_reply;
    connection->owner_route = reply->route;
    connection->owner_route.size = sizeof(connection->owner_route);
  }
  flowie_cluster_endpoint_finish(connection, status, status == TURBO_OK ? &action : NULL);
}

static void flowie_cluster_endpoint_command_complete(
    void *ctx, int status, const flowie_cluster_peer_frame_t *frame,
    const flowie_cluster_peer_mqtt_reply_action_t *owner_action) {
  flowie_cluster_endpoint_connection_t *connection =
      (flowie_cluster_endpoint_connection_t *)ctx;
  flowie_endpoint_cluster_action_t action = FLOWIE_ENDPOINT_CLUSTER_ACTION_INIT;
  (void)frame;
  if (!connection) return;
  if (status == TURBO_OK && !owner_action) status = TURBO_EPROTO;
  if (status == TURBO_OK) {
    action.mqtt_version = owner_action->mqtt_version;
    action.packet = owner_action->packet.packet;
    action.close_after_send = owner_action->close_after_send;
    action.settlement_point = owner_action->settlement_point;
  }
  flowie_cluster_endpoint_finish(connection, status, status == TURBO_OK ? &action : NULL);
}

static int flowie_cluster_endpoint_operation(flowie_endpoint_cluster_command_t command,
                                             flowie_cluster_peer_operation_t *out) {
  if (!out) return TURBO_EINVAL;
  switch (command) {
  case FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH:
    *out = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
    return TURBO_OK;
  case FLOWIE_ENDPOINT_CLUSTER_COMMAND_SUBSCRIBE:
    *out = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE;
    return TURBO_OK;
  case FLOWIE_ENDPOINT_CLUSTER_COMMAND_UNSUBSCRIBE:
    *out = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE;
    return TURBO_OK;
  case FLOWIE_ENDPOINT_CLUSTER_COMMAND_ACK:
    *out = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK;
    return TURBO_OK;
  case FLOWIE_ENDPOINT_CLUSTER_COMMAND_DISCONNECT:
    *out = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT;
    return TURBO_OK;
  default:
    return TURBO_EINVAL;
  }
}

static int flowie_cluster_endpoint_connect(
    void *ctx, uint64_t connection_id, uint64_t connection_generation,
    const flowie_mqtt_connect_view_t *connect,
    const turbo_flow_security_principal_t *principal,
    const flowie_endpoint_cluster_ingress_t *ingress,
    const flowie_endpoint_cluster_socket_port_t *socket_port,
    flowie_endpoint_cluster_complete_fn complete, void *complete_ctx) {
  flowie_cluster_endpoint_binding_t *binding = (flowie_cluster_endpoint_binding_t *)ctx;
  flowie_cluster_peer_ingress_metadata_t metadata = FLOWIE_CLUSTER_PEER_INGRESS_METADATA_INIT;
  flowie_cluster_endpoint_connection_t *connection;
  flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  size_t reservation;
  int rc;
  if (!binding || !connect || !ingress || ingress->size < sizeof(*ingress) ||
      ingress->abi_version != FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT ||
      !ingress->remote_address || !ingress->transport_peer_address ||
      (!ingress->proxy_tlvs.data && ingress->proxy_tlvs.size != 0u) || !socket_port ||
      socket_port->size < sizeof(*socket_port) ||
      !socket_port->ctx || !socket_port->takeover_close || !socket_port->apply_action || !complete ||
      connection_id == 0u || connection_generation == 0u || connect->client_id.size == 0u ||
      (!connect->client_id.data && connect->client_id.size != 0u))
    return TURBO_EINVAL;
  if (coro_context_current() != binding->execution->context) return TURBO_EBUSY;
  if (flowie_cluster_endpoint_connection_find(binding, connection_id, connection_generation))
    return TURBO_EALREADY;
  if (connect->client_id.size > SIZE_MAX - sizeof(*connection)) return TURBO_ERANGE;
  reservation = sizeof(*connection) + connect->client_id.size;
  turbo_mutex_lock(&binding->mutex);
  if (!binding->accepting) rc = TURBO_ESHUTDOWN;
  else if (binding->connection_count >= binding->max_connections ||
           reservation > binding->max_connection_bytes - binding->connection_bytes)
    rc = TURBO_ENOBUFS;
  else {
    ++binding->connection_count;
    binding->connection_bytes += reservation;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&binding->mutex);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_owner_directory_resolve(binding->owners, connect->client_id, &owner);
  if (rc != TURBO_OK) {
    flowie_cluster_endpoint_connection_reservation_release(binding, reservation);
    return rc;
  }
  connection = (flowie_cluster_endpoint_connection_t *)calloc(1u, sizeof(*connection));
  if (!connection) {
    flowie_cluster_endpoint_connection_reservation_release(binding, reservation);
    return TURBO_ENOMEM;
  }
  connection->client_id = tstr_new_len(connect->client_id.data, connect->client_id.size);
  if (!connection->client_id) {
    free(connection);
    flowie_cluster_endpoint_connection_reservation_release(binding, reservation);
    return TURBO_ENOMEM;
  }
  connection->binding = binding;
  connection->socket_port = *socket_port;
  connection->complete = complete;
  connection->complete_ctx = complete_ctx;
  connection->owner = owner;
  connection->connection_id = connection_id;
  connection->connection_generation = connection_generation;
  connection->mqtt_version = connect->version;
  connection->reserved_bytes = reservation;
  connection->pending = 1;
  connection->public_completion = 1;
  metadata.remote_address = (tstr_v){ingress->remote_address, strlen(ingress->remote_address)};
  metadata.transport_peer_address =
      (tstr_v){ingress->transport_peer_address, strlen(ingress->transport_peer_address)};
  metadata.proxy_tlvs =
      (tstr_v){(const char *)ingress->proxy_tlvs.data, ingress->proxy_tlvs.size};
  rc = flowie_cluster_endpoint_connection_map_t_put(&binding->connections, connection_id,
                                                     connection);
  if (rc != TURBO_OK) {
    tstr_free(connection->client_id);
    free(connection);
    flowie_cluster_endpoint_connection_reservation_release(binding, reservation);
    return rc;
  }
  rc = flowie_cluster_edge_bind_connect(binding->edge, connection_id, connection_generation,
                                        connect, principal, &metadata,
                                        flowie_cluster_endpoint_connect_complete, connection);
  if (rc != TURBO_OK) {
    (void)flowie_cluster_endpoint_connection_map_t_remove(&binding->connections, connection_id,
                                                          NULL);
    flowie_cluster_endpoint_connection_free(connection);
  }
  return rc;
}

static int flowie_cluster_endpoint_command(
    void *ctx, uint64_t connection_id, uint64_t connection_generation,
    flowie_endpoint_cluster_command_t command, flowie_mqtt_version_t mqtt_version,
    flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
    flowie_endpoint_cluster_complete_fn complete, void *complete_ctx) {
  flowie_cluster_endpoint_binding_t *binding = (flowie_cluster_endpoint_binding_t *)ctx;
  flowie_cluster_endpoint_connection_t *connection;
  flowie_cluster_peer_operation_t operation;
  int rc;
  if (!binding || !complete || !packet.data || packet.size == 0u) return TURBO_EINVAL;
  if (coro_context_current() != binding->execution->context) return TURBO_EBUSY;
  rc = flowie_cluster_endpoint_operation(command, &operation);
  if (rc != TURBO_OK) return rc;
  connection = flowie_cluster_endpoint_connection_find(binding, connection_id,
                                                        connection_generation);
  if (!connection || connection->detached) return TURBO_ENOENT;
  if (connection->pending || mqtt_version != connection->mqtt_version ||
      client_id.size != tstr_len(connection->client_id) ||
      memcmp(client_id.data, connection->client_id, client_id.size) != 0)
    return TURBO_EBUSY;
  connection->pending = 1;
  connection->public_completion = 1;
  connection->complete = complete;
  connection->complete_ctx = complete_ctx;
  rc = flowie_cluster_edge_bind_command(
      binding->edge, connection_id, connection_generation, &connection->owner, operation,
      mqtt_version, client_id, packet, flowie_cluster_endpoint_command_complete, connection);
  if (rc != TURBO_OK) {
    connection->pending = 0;
    connection->complete = NULL;
    connection->complete_ctx = NULL;
  }
  return rc;
}

static int flowie_cluster_endpoint_settle(
    void *ctx, uint64_t connection_id, uint64_t connection_generation,
    flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id,
    const turbo_flow_protocol_settlement_request_t *settlement,
    flowie_endpoint_cluster_complete_fn complete, void *complete_ctx) {
  flowie_cluster_endpoint_binding_t *binding = (flowie_cluster_endpoint_binding_t *)ctx;
  flowie_cluster_endpoint_connection_t *connection;
  turbo_flow_protocol_settlement_request_t authoritative;
  int rc;
  if (!binding || !settlement || settlement->size < sizeof(*settlement) || !complete)
    return TURBO_EINVAL;
  if (coro_context_current() != binding->execution->context) return TURBO_EBUSY;
  connection = flowie_cluster_endpoint_connection_find(binding, connection_id,
                                                        connection_generation);
  if (!connection || connection->detached) return TURBO_ENOENT;
  if (connection->pending || mqtt_version != connection->mqtt_version ||
      client_id.size != tstr_len(connection->client_id) ||
      (!client_id.data && client_id.size != 0u) ||
      memcmp(client_id.data, connection->client_id, client_id.size) != 0 ||
      settlement->message.protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      settlement->message.protocol_version != (uint32_t)mqtt_version ||
      settlement->message.session_generation != connection_generation ||
      connection->owner_route.protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      connection->owner_route.session_generation == 0u)
    return TURBO_EBUSY;
  authoritative = *settlement;
  authoritative.size = sizeof(authoritative);
  authoritative.message.size = sizeof(authoritative.message);
  authoritative.message.session_generation = connection->owner_route.session_generation;
  connection->pending = 1;
  connection->public_completion = 1;
  connection->complete = complete;
  connection->complete_ctx = complete_ctx;
  rc = flowie_cluster_edge_bind_publish_settle(
      binding->edge, connection_id, connection_generation, &connection->owner, client_id,
      &authoritative, flowie_cluster_endpoint_command_complete, connection);
  if (rc != TURBO_OK) {
    connection->pending = 0;
    connection->complete = NULL;
    connection->complete_ctx = NULL;
  }
  return rc;
}

static int flowie_cluster_endpoint_connection_lost(
    void *ctx, uint64_t connection_id, uint64_t connection_generation,
    flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id) {
  flowie_cluster_endpoint_binding_t *binding = (flowie_cluster_endpoint_binding_t *)ctx;
  flowie_cluster_endpoint_connection_t *connection;
  int rc;
  if (!binding) return TURBO_EINVAL;
  if (coro_context_current() != binding->execution->context) return TURBO_EBUSY;
  connection = flowie_cluster_endpoint_connection_find(binding, connection_id,
                                                        connection_generation);
  if (!connection || connection->detached) return TURBO_ENOENT;
  if (connection->pending || mqtt_version != connection->mqtt_version ||
      client_id.size != tstr_len(connection->client_id) ||
      memcmp(client_id.data, connection->client_id, client_id.size) != 0)
    return TURBO_EBUSY;
  connection->pending = 1;
  connection->public_completion = 0;
  rc = flowie_cluster_edge_bind_connection_lost(
      binding->edge, connection_id, connection_generation, &connection->owner, mqtt_version,
      client_id, flowie_cluster_endpoint_command_complete, connection);
  if (rc != TURBO_OK) connection->pending = 0;
  return rc;
}

static void flowie_cluster_endpoint_detach(void *ctx, uint64_t connection_id,
                                           uint64_t connection_generation) {
  flowie_cluster_endpoint_binding_t *binding = (flowie_cluster_endpoint_binding_t *)ctx;
  flowie_cluster_endpoint_connection_t *connection;
  if (!binding || coro_context_current() != binding->execution->context) return;
  connection = flowie_cluster_endpoint_connection_find(binding, connection_id,
                                                        connection_generation);
  if (!connection) return;
  (void)flowie_cluster_endpoint_connection_map_t_remove(&binding->connections, connection_id,
                                                        NULL);
  connection->detached = 1;
  memset(&connection->socket_port, 0, sizeof(connection->socket_port));
  connection->complete = NULL;
  connection->complete_ctx = NULL;
  if (!connection->pending) flowie_cluster_endpoint_connection_free(connection);
}

static int flowie_cluster_endpoint_socket_takeover(void *ctx) {
  flowie_cluster_endpoint_connection_t *connection =
      (flowie_cluster_endpoint_connection_t *)ctx;
  return !connection || connection->detached || !connection->socket_port.takeover_close
             ? TURBO_ENOTCONN
             : connection->socket_port.takeover_close(connection->socket_port.ctx);
}

static int flowie_cluster_endpoint_socket_action(
    void *ctx, const flowie_cluster_peer_mqtt_reply_action_t *owner_action) {
  flowie_cluster_endpoint_connection_t *connection =
      (flowie_cluster_endpoint_connection_t *)ctx;
  flowie_endpoint_cluster_action_t action = FLOWIE_ENDPOINT_CLUSTER_ACTION_INIT;
  if (!connection || connection->detached || !connection->socket_port.apply_action ||
      !owner_action)
    return TURBO_ENOTCONN;
  action.mqtt_version = owner_action->mqtt_version;
  action.packet = owner_action->packet.packet;
  action.close_after_send = owner_action->close_after_send;
  action.settlement_point = owner_action->settlement_point;
  return connection->socket_port.apply_action(connection->socket_port.ctx, &action);
}

static void flowie_cluster_endpoint_action_job_run(void *arg1, void *arg2) {
  flowie_cluster_endpoint_action_job_t *job =
      (flowie_cluster_endpoint_action_job_t *)arg1;
  flowie_cluster_endpoint_binding_t *binding;
  flowie_cluster_endpoint_connection_t *connection = NULL;
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  size_t consumed = 0u;
  int rc;
  (void)arg2;
  if (!job || !(binding = job->binding)) return;
  rc = flowie_cluster_peer_frame_decode(job->encoded, tstr_len(job->encoded),
                                        binding->max_payload_size, &frame, &consumed);
  if (rc == TURBO_OK && consumed != tstr_len(job->encoded)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    connection = flowie_cluster_endpoint_connection_find(
        binding, frame.connection_id, frame.connection_generation);
  if (rc == TURBO_OK && !connection) rc = TURBO_ENOENT;
  if (rc == TURBO_OK && frame.operation == FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE)
    rc = flowie_cluster_edge_bind_takeover_close(
        binding->edge, &frame, connection->connection_id, connection->connection_generation,
        connection->mqtt_version,
        (flowie_mqtt_span_t){(const uint8_t *)connection->client_id,
                             tstr_len(connection->client_id)},
        flowie_cluster_endpoint_socket_takeover, connection);
  else if (rc == TURBO_OK && frame.operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION)
    rc = flowie_cluster_edge_bind_apply_action(
        binding->edge, &frame, &connection->owner, connection->connection_id,
        connection->connection_generation, connection->mqtt_version,
        &connection->last_applied_sequence, flowie_cluster_endpoint_socket_action, connection);
  (void)rc;
  flowie_cluster_peer_frame_cleanup(&frame);
  tstr_free(job->encoded);
  turbo_mutex_lock(&binding->mutex);
  if (binding->inbound_actions != 0u) --binding->inbound_actions;
  if (binding->inbound_bytes >= job->reserved_bytes)
    binding->inbound_bytes -= job->reserved_bytes;
  turbo_cond_broadcast(&binding->changed);
  turbo_mutex_unlock(&binding->mutex);
  free(job);
}

static int flowie_cluster_endpoint_receive(void *ctx,
                                           const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_endpoint_binding_t *binding = (flowie_cluster_endpoint_binding_t *)ctx;
  flowie_cluster_endpoint_action_job_t *job;
  tstr_t encoded = NULL;
  size_t reservation;
  int rc;
  if (!binding || !frame) return TURBO_EINVAL;
  if (frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY)
    return flowie_cluster_edge_bind_reply(binding->edge, frame);
  if (frame->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      (frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE &&
       frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION))
    return TURBO_EPROTO;
  rc = flowie_cluster_peer_frame_encode(frame, binding->max_payload_size, &encoded);
  if (rc != TURBO_OK) return rc;
  if (tstr_len(encoded) > SIZE_MAX - sizeof(*job)) {
    tstr_free(encoded);
    return TURBO_ERANGE;
  }
  reservation = sizeof(*job) + tstr_len(encoded);
  job = (flowie_cluster_endpoint_action_job_t *)calloc(1u, sizeof(*job));
  if (!job) {
    tstr_free(encoded);
    return TURBO_ENOMEM;
  }
  job->binding = binding;
  job->encoded = encoded;
  job->reserved_bytes = reservation;
  turbo_mutex_lock(&binding->mutex);
  if (!binding->accepting) rc = TURBO_ESHUTDOWN;
  else if (binding->inbound_actions >= binding->max_inbound_actions ||
           reservation > binding->max_inbound_bytes - binding->inbound_bytes)
    rc = TURBO_ENOBUFS;
  else {
    ++binding->inbound_actions;
    binding->inbound_bytes += reservation;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&binding->mutex);
  if (rc == TURBO_OK)
    rc = coro_post(binding->execution->context, flowie_cluster_endpoint_action_job_run, job, NULL);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&binding->mutex);
    if (binding->inbound_actions != 0u) --binding->inbound_actions;
    if (binding->inbound_bytes >= reservation) binding->inbound_bytes -= reservation;
    turbo_cond_broadcast(&binding->changed);
    turbo_mutex_unlock(&binding->mutex);
    tstr_free(encoded);
    free(job);
  }
  return rc;
}

int flowie_cluster_endpoint_binding_create(
    const flowie_cluster_endpoint_binding_config_t *config,
    flowie_cluster_endpoint_binding_t **out) {
  flowie_cluster_endpoint_binding_t *binding;
  flowie_cluster_edge_bind_config_t edge_config = FLOWIE_CLUSTER_EDGE_BIND_CONFIG_INIT;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flowie_cluster_endpoint_config_validate(config);
  if (rc != TURBO_OK) return rc;
  binding = (flowie_cluster_endpoint_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding) return TURBO_ENOMEM;
  turbo_mutex_init(&binding->mutex);
  turbo_cond_init(&binding->changed);
  binding->execution = config->execution;
  binding->node = config->node;
  binding->owners = config->owners;
  binding->max_payload_size = config->max_payload_size;
  binding->max_connections = config->max_connections;
  binding->max_connection_bytes = config->max_connection_bytes;
  binding->max_inbound_actions = config->max_inbound_actions;
  binding->max_inbound_bytes = config->max_inbound_bytes;
  rc = flowie_cluster_endpoint_connection_map_t_init(&binding->connections);
  if (rc != TURBO_OK) goto fail;
  binding->map_initialized = 1;
  edge_config.execution = config->execution;
  edge_config.max_payload_size = config->max_payload_size;
  edge_config.max_pending_entries = config->max_connections;
  edge_config.max_pending_bytes = config->max_pending_bytes;
  edge_config.cluster_id = config->cluster_id;
  edge_config.listener_id = config->listener_id;
  edge_config.local_node_id = config->local_node_id;
  memcpy(edge_config.local_boot_id, config->local_boot_id, sizeof(edge_config.local_boot_id));
  edge_config.resolve = flowie_cluster_owner_directory_resolve;
  edge_config.resolve_ctx = config->owners;
  edge_config.submit = flowie_cluster_node_submit;
  edge_config.submit_ctx = config->node;
  rc = flowie_cluster_edge_bind_create(&edge_config, &binding->edge);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_node_register_edge(config->node, flowie_cluster_endpoint_receive, binding);
  if (rc != TURBO_OK) goto fail;
  binding->edge_registered = 1;
  binding->accepting = 1;
  binding->port = (flowie_endpoint_cluster_binding_t)FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
  binding->port.ctx = binding;
  binding->port.request_timeout_ms = config->request_timeout_ms;
  binding->port.connect = flowie_cluster_endpoint_connect;
  binding->port.command = flowie_cluster_endpoint_command;
  binding->port.settle = flowie_cluster_endpoint_settle;
  binding->port.connection_lost = flowie_cluster_endpoint_connection_lost;
  binding->port.detach = flowie_cluster_endpoint_detach;
  *out = binding;
  return TURBO_OK;

fail:
  if (binding->edge_registered)
    (void)flowie_cluster_node_unregister_edge(binding->node, flowie_cluster_endpoint_receive,
                                              binding);
  if (binding->edge) {
    (void)flowie_cluster_edge_bind_close(binding->edge);
    (void)flowie_cluster_edge_bind_drain(binding->edge, UINT64_MAX);
    (void)flowie_cluster_edge_bind_destroy(binding->edge);
  }
  if (binding->map_initialized)
    flowie_cluster_endpoint_connection_map_t_destroy(&binding->connections);
  turbo_cond_destroy(&binding->changed);
  turbo_mutex_destroy(&binding->mutex);
  free(binding);
  return rc;
}

const flowie_endpoint_cluster_binding_t *flowie_cluster_endpoint_binding_port(
    flowie_cluster_endpoint_binding_t *binding) {
  return binding ? &binding->port : NULL;
}

int flowie_cluster_endpoint_binding_close(flowie_cluster_endpoint_binding_t *binding,
                                          uint64_t timeout_ns) {
  int rc;
  if (!binding) return TURBO_EINVAL;
  turbo_mutex_lock(&binding->mutex);
  if (binding->closed) {
    turbo_mutex_unlock(&binding->mutex);
    return TURBO_OK;
  }
  binding->accepting = 0;
  rc = binding->connection_count == 0u ? TURBO_OK : TURBO_EBUSY;
  turbo_mutex_unlock(&binding->mutex);
  if (rc != TURBO_OK) return rc;
  if (binding->edge_registered) {
    rc = flowie_cluster_node_unregister_edge(binding->node, flowie_cluster_endpoint_receive,
                                             binding);
    if (rc != TURBO_OK) return rc;
    binding->edge_registered = 0;
  }
  rc = flowie_cluster_edge_bind_close(binding->edge);
  if (rc == TURBO_OK) rc = flowie_cluster_edge_bind_drain(binding->edge, timeout_ns);
  if (rc == TURBO_OK) {
    turbo_mutex_lock(&binding->mutex);
    rc = binding->inbound_actions == 0u ? TURBO_OK : TURBO_EBUSY;
    turbo_mutex_unlock(&binding->mutex);
  }
  if (rc == TURBO_OK) binding->closed = 1;
  return rc;
}

int flowie_cluster_endpoint_binding_destroy(flowie_cluster_endpoint_binding_t *binding) {
  int rc;
  if (!binding) return TURBO_OK;
  turbo_mutex_lock(&binding->mutex);
  rc = binding->closed && binding->connection_count == 0u ? TURBO_OK : TURBO_EBUSY;
  turbo_mutex_unlock(&binding->mutex);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_edge_bind_destroy(binding->edge);
  if (rc != TURBO_OK) return rc;
  flowie_cluster_endpoint_connection_map_t_destroy(&binding->connections);
  turbo_cond_destroy(&binding->changed);
  turbo_mutex_destroy(&binding->mutex);
  free(binding);
  return TURBO_OK;
}
