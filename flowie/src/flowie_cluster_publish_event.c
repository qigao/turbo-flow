#include "flowie_cluster_publish_event_internal.h"

#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PUBLISH_EVENT_MAGIC[4] = {'T', 'F', 'P', 'E'};

static int flowie_cluster_publish_event_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  if (!bytes) return 0;
  for (size_t index = 0u; index < size; ++index)
    combined |= bytes[index];
  return combined != 0u;
}

static int flowie_cluster_publish_event_settlement_validate(
    turbo_flow_protocol_settlement_point_t requested,
    const flowie_cluster_peer_mqtt_command_view_t *publish_command) {
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  if (!publish_command || requested < TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED ||
      requested > TURBO_FLOW_PROTOCOL_SETTLE_DURABLE ||
      flowie_mqtt_publish_parse(&publish_command->packet, &publish) != FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  return publish.qos == 0u && requested != TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED ? TURBO_EPROTO
                                                                               : TURBO_OK;
}

int flowie_cluster_publish_event_encode(
    flowie_mqtt_version_t mqtt_version, turbo_flow_protocol_settlement_point_t requested_settlement,
    uint64_t connection_id, uint64_t connection_generation, uint64_t session_id,
    uint64_t session_generation, uint64_t accepted_at_epoch_seconds, tstr_v edge_node_id,
    const uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_mqtt_span_t client_id,
    flowie_mqtt_span_t packet, size_t max_payload_size, tstr_t *out) {
  flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
  tstr_t command = NULL;
  uint8_t *bytes;
  size_t packet_limit;
  size_t total;
  int rc;
  if (out) *out = NULL;
  if (!out || connection_id == 0u || connection_generation == 0u || session_id == 0u ||
      session_generation == 0u || accepted_at_epoch_seconds == 0u || !edge_node_id.data ||
      edge_node_id.len == 0u ||
      edge_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || edge_node_id.len > UINT16_MAX ||
      memchr(edge_node_id.data, '\0', edge_node_id.len) ||
      !flowie_cluster_publish_event_nonzero(edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      max_payload_size <= FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE)
    return TURBO_EINVAL;
  packet_limit = max_payload_size < FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE
                     ? max_payload_size
                     : FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
  rc = flowie_cluster_peer_mqtt_command_encode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH,
                                               mqtt_version, client_id, packet, packet_limit,
                                               &command);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_mqtt_command_decode(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, command, tstr_len(command), packet_limit,
        &decoded);
  if (rc == TURBO_OK)
    rc = flowie_cluster_publish_event_settlement_validate(requested_settlement, &decoded);
  if (rc != TURBO_OK) {
    tstr_free(command);
    return rc;
  }
  if (edge_node_id.len > SIZE_MAX - FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE ||
      tstr_len(command) >
          SIZE_MAX - FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE - edge_node_id.len) {
    tstr_free(command);
    return TURBO_ERANGE;
  }
  total = FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE + edge_node_id.len + tstr_len(command);
  if (total > max_payload_size || total > UINT32_MAX || tstr_len(command) > UINT32_MAX) {
    tstr_free(command);
    return TURBO_EMSGSIZE;
  }
  *out = tstr_new_len(NULL, total);
  if (!*out) {
    tstr_free(command);
    return TURBO_ENOMEM;
  }
  bytes = (uint8_t *)*out;
  memset(bytes, 0, FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE);
  memcpy(bytes, FLOWIE_CLUSTER_PUBLISH_EVENT_MAGIC, sizeof(FLOWIE_CLUSTER_PUBLISH_EVENT_MAGIC));
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  flowie_cluster_peer_wire_write_u32(bytes + 12u, (uint32_t)tstr_len(command));
  bytes[16] = (uint8_t)requested_settlement;
  flowie_cluster_peer_wire_write_u64(bytes + 24u, connection_id);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, connection_generation);
  flowie_cluster_peer_wire_write_u64(bytes + 40u, session_id);
  flowie_cluster_peer_wire_write_u64(bytes + 48u, session_generation);
  memcpy(bytes + 56u, edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  flowie_cluster_peer_wire_write_u64(bytes + 72u, accepted_at_epoch_seconds);
  flowie_cluster_peer_wire_write_u16(bytes + 80u, (uint16_t)edge_node_id.len);
  memcpy(bytes + FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE, edge_node_id.data, edge_node_id.len);
  memcpy(bytes + FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE + edge_node_id.len, command,
         tstr_len(command));
  tstr_free(command);
  return TURBO_OK;
}

int flowie_cluster_publish_event_decode(const void *data, size_t data_size, size_t max_payload_size,
                                        flowie_cluster_publish_event_view_t *out) {
  flowie_cluster_publish_event_view_t decoded = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  size_t command_size;
  size_t edge_node_size;
  size_t packet_limit;
  size_t header_size;
  uint16_t wire_version;
  int rc;
  if (!bytes || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION || max_payload_size == 0u)
    return TURBO_EINVAL;
  if (data_size > max_payload_size || data_size < FLOWIE_CLUSTER_PUBLISH_EVENT_V1_HEADER_SIZE)
    return data_size > max_payload_size ? TURBO_EMSGSIZE : TURBO_EPROTO;
  wire_version = flowie_cluster_peer_wire_read_u16(bytes + 4u);
  header_size = wire_version == 1u ? FLOWIE_CLUSTER_PUBLISH_EVENT_V1_HEADER_SIZE
                                  : FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE;
  if (memcmp(bytes, FLOWIE_CLUSTER_PUBLISH_EVENT_MAGIC,
             sizeof(FLOWIE_CLUSTER_PUBLISH_EVENT_MAGIC)) != 0 ||
      (wire_version != 1u && wire_version != FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION) ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) != header_size || data_size < header_size ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != data_size ||
      memcmp(bytes + 17u, "\0\0\0\0\0\0\0", 7u) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + (wire_version == 1u ? 74u : 82u)) != 0u ||
      flowie_cluster_peer_wire_read_u32(bytes + (wire_version == 1u ? 76u : 84u)) != 0u)
    return TURBO_EPROTO;
  command_size = flowie_cluster_peer_wire_read_u32(bytes + 12u);
  edge_node_size = flowie_cluster_peer_wire_read_u16(bytes + (wire_version == 1u ? 72u : 80u));
  decoded.requested_settlement = (turbo_flow_protocol_settlement_point_t)bytes[16];
  decoded.connection_id = flowie_cluster_peer_wire_read_u64(bytes + 24u);
  decoded.connection_generation = flowie_cluster_peer_wire_read_u64(bytes + 32u);
  decoded.session_id = flowie_cluster_peer_wire_read_u64(bytes + 40u);
  decoded.session_generation = flowie_cluster_peer_wire_read_u64(bytes + 48u);
  decoded.accepted_at_epoch_seconds =
      wire_version == 1u ? 0u : flowie_cluster_peer_wire_read_u64(bytes + 72u);
  decoded.edge_boot_id = bytes + 56u;
  if (edge_node_size == 0u || edge_node_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      edge_node_size > data_size - header_size ||
      command_size != data_size - header_size - edge_node_size ||
      command_size < FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE || decoded.connection_id == 0u ||
      decoded.connection_generation == 0u || decoded.session_id == 0u ||
      decoded.session_generation == 0u ||
      !flowie_cluster_publish_event_nonzero(decoded.edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EPROTO;
  decoded.edge_node_id = tstr_v_from_buf(
      (const char *)bytes + header_size, edge_node_size);
  if (memchr(decoded.edge_node_id.data, '\0', decoded.edge_node_id.len)) return TURBO_EPROTO;
  packet_limit = max_payload_size < FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE
                     ? max_payload_size
                     : FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
  rc = flowie_cluster_peer_mqtt_command_decode(
      FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH,
      bytes + header_size + edge_node_size, command_size,
      packet_limit, &decoded.publish);
  if (rc == TURBO_OK)
    rc = flowie_cluster_publish_event_settlement_validate(decoded.requested_settlement,
                                                          &decoded.publish);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}
