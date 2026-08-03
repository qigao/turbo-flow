#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_EDGE_ACTION_MAGIC[4] = {'T', 'F', 'E', 'A'};
static const uint8_t FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_MAGIC[4] = {'T', 'F', 'E', 'K'};

enum {
  FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_TOTAL_SIZE = 8,
  FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_ACTION_SIZE = 12,
  FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_SEQUENCE = 16
};

int flowie_cluster_peer_edge_action_encode(
    uint64_t action_sequence, flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t packet,
    int close_after_send, turbo_flow_protocol_settlement_point_t settlement_point,
    size_t max_payload_size, tstr_t *out) {
  tstr_t action = NULL;
  size_t packet_limit;
  size_t action_size;
  size_t total_size;
  uint8_t *encoded;
  int rc;
  if (out) *out = NULL;
  if (!out || action_sequence == 0u ||
      (packet.size == 0u && !close_after_send && settlement_point == 0) ||
      max_payload_size <=
          FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE + FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE)
    return TURBO_EINVAL;
  packet_limit = max_payload_size - FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE -
                 FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE;
  rc = flowie_cluster_peer_mqtt_reply_encode(mqtt_version, packet, close_after_send,
                                             settlement_point, packet_limit, &action);
  if (rc != TURBO_OK) return rc;
  action_size = tstr_len(action);
  if (action_size > SIZE_MAX - FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE) {
    tstr_free(action);
    return TURBO_ERANGE;
  }
  total_size = FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE + action_size;
  if (total_size > max_payload_size || total_size > UINT32_MAX || action_size > UINT32_MAX) {
    tstr_free(action);
    return TURBO_EMSGSIZE;
  }
  *out = tstr_new_len(NULL, total_size);
  if (!*out) {
    tstr_free(action);
    return TURBO_ENOMEM;
  }
  encoded = (uint8_t *)*out;
  memcpy(encoded, FLOWIE_CLUSTER_PEER_EDGE_ACTION_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_EDGE_ACTION_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_PEER_EDGE_ACTION_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_TOTAL_SIZE,
                                     (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_ACTION_SIZE,
                                     (uint32_t)action_size);
  flowie_cluster_peer_wire_write_u64(encoded + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_SEQUENCE,
                                     action_sequence);
  memcpy(encoded + FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE, action, action_size);
  tstr_free(action);
  return TURBO_OK;
}

int flowie_cluster_peer_edge_action_decode(const void *data, size_t data_size,
                                           size_t max_payload_size,
                                           flowie_cluster_peer_edge_action_t *out) {
  flowie_cluster_peer_edge_action_t decoded = FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  size_t action_size;
  size_t packet_limit;
  int rc;
  if (!bytes || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_EDGE_ACTION_VERSION ||
      max_payload_size <=
          FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE + FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE)
    return TURBO_EINVAL;
  if (data_size > max_payload_size || data_size < FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE)
    return data_size > max_payload_size ? TURBO_EMSGSIZE : TURBO_EPROTO;
  if (memcmp(bytes, FLOWIE_CLUSTER_PEER_EDGE_ACTION_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_EDGE_ACTION_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_EDGE_ACTION_VERSION ||
      flowie_cluster_peer_wire_read_u16(
          bytes + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_TOTAL_SIZE) !=
          data_size)
    return TURBO_EPROTO;
  action_size = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_ACTION_SIZE);
  decoded.action_sequence =
      flowie_cluster_peer_wire_read_u64(bytes + FLOWIE_CLUSTER_PEER_EDGE_ACTION_OFFSET_SEQUENCE);
  if (decoded.action_sequence == 0u ||
      action_size != data_size - FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE ||
      action_size < FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE)
    return TURBO_EPROTO;
  packet_limit = max_payload_size - FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE -
                 FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE;
  rc = flowie_cluster_peer_mqtt_reply_decode(
      bytes + FLOWIE_CLUSTER_PEER_EDGE_ACTION_HEADER_SIZE, action_size, packet_limit,
      &decoded.action);
  if (rc != TURBO_OK) return rc;
  if (decoded.action.packet.packet.size == 0u && !decoded.action.close_after_send &&
      decoded.action.settlement_point == 0)
    return TURBO_EPROTO;
  *out = decoded;
  return TURBO_OK;
}

int flowie_cluster_peer_edge_action_ack_encode(uint64_t action_sequence, tstr_t *out) {
  uint8_t *encoded;
  if (out) *out = NULL;
  if (!out || action_sequence == 0u) return TURBO_EINVAL;
  *out = tstr_new_len(NULL, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  memset(encoded, 0, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE);
  memcpy(encoded, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + 4u, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + 6u, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + 8u, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE);
  flowie_cluster_peer_wire_write_u64(encoded + 16u, action_sequence);
  return TURBO_OK;
}

int flowie_cluster_peer_edge_action_ack_decode(const void *data, size_t data_size,
                                               uint64_t *out_action_sequence) {
  const uint8_t *bytes = (const uint8_t *)data;
  uint64_t sequence;
  if (out_action_sequence) *out_action_sequence = 0u;
  if (!bytes || !out_action_sequence) return TURBO_EINVAL;
  if (data_size != FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE ||
      memcmp(bytes, FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + 4u) !=
          FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) != FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != FLOWIE_CLUSTER_PEER_EDGE_ACTION_ACK_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 12u) != 0u)
    return TURBO_EPROTO;
  sequence = flowie_cluster_peer_wire_read_u64(bytes + 16u);
  if (sequence == 0u) return TURBO_EPROTO;
  *out_action_sequence = sequence;
  return TURBO_OK;
}
