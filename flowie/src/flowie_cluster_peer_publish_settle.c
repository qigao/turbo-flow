#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_MAGIC[4] = {'T', 'F', 'P', 'S'};

enum {
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_TOTAL_SIZE = 8,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_CLIENT_ID_SIZE = 12,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_RESERVED16 = 14,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_PROTOCOL_VERSION = 16,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_QOS = 20,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_PACKET_ID = 24,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_POINT = 28,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_STATUS = 32,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_ATTEMPT = 36,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_SESSION_GENERATION = 40,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_MESSAGE_ID = 48,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_FLAGS = 56,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_RESERVED32 = 60
};

enum {
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_DUPLICATE = 1u << 0u,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_RETAIN = 1u << 1u,
  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_KNOWN_FLAGS =
      FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_DUPLICATE |
      FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_RETAIN
};

static int flowie_cluster_peer_publish_settlement_validate(
    flowie_mqtt_span_t client_id,
    const turbo_flow_protocol_settlement_request_t *settlement) {
  if (!client_id.data || client_id.size == 0u || client_id.size > UINT16_MAX ||
      !flowie_mqtt_utf8_validate(client_id) || !settlement ||
      settlement->size < sizeof(*settlement) || settlement->attempt == 0u ||
      settlement->point < TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED ||
      settlement->point > TURBO_FLOW_PROTOCOL_SETTLE_DURABLE ||
      turbo_flow_protocol_message_validate(&settlement->message) != TURBO_OK ||
      settlement->message.protocol != TURBO_FLOW_PROTOCOL_MQTT ||
      settlement->message.kind != TURBO_FLOW_PROTOCOL_MESSAGE_DATA ||
      (settlement->message.qos != 1u && settlement->message.qos != 2u) ||
      settlement->message.packet_id == 0u || settlement->message.packet_id > UINT16_MAX)
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowie_cluster_peer_publish_settle_encode(
    flowie_mqtt_span_t client_id,
    const turbo_flow_protocol_settlement_request_t *settlement, size_t max_payload_size,
    tstr_t *out) {
  uint8_t *encoded;
  uint32_t flags = 0u;
  size_t total_size;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = flowie_cluster_peer_publish_settlement_validate(client_id, settlement);
  if (rc != TURBO_OK) return rc;
  if (client_id.size > SIZE_MAX - FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE)
    return TURBO_ERANGE;
  total_size = FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE + client_id.size;
  if (total_size > max_payload_size || total_size > UINT32_MAX) return TURBO_EMSGSIZE;
  *out = tstr_new_len(NULL, total_size);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  if (settlement->message.duplicate) flags |= FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_DUPLICATE;
  if (settlement->message.retain) flags |= FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_RETAIN;
  memcpy(encoded, FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_MAGIC));
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_VERSION,
      FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VERSION);
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_HEADER_SIZE,
      FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_TOTAL_SIZE, (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_CLIENT_ID_SIZE,
      (uint16_t)client_id.size);
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_RESERVED16, 0u);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_PROTOCOL_VERSION,
      settlement->message.protocol_version);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_QOS,
                                     settlement->message.qos);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_PACKET_ID,
      settlement->message.packet_id);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_POINT,
                                     (uint32_t)settlement->point);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_STATUS,
                                     (uint32_t)(int32_t)settlement->status);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_ATTEMPT,
                                     settlement->attempt);
  flowie_cluster_peer_wire_write_u64(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_SESSION_GENERATION,
      settlement->message.session_generation);
  flowie_cluster_peer_wire_write_u64(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_MESSAGE_ID, settlement->message_id);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_FLAGS,
                                     flags);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_RESERVED32, 0u);
  memcpy(encoded + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE, client_id.data,
         client_id.size);
  return TURBO_OK;
}

int flowie_cluster_peer_publish_settle_decode(
    const void *data, size_t data_size, size_t max_payload_size,
    flowie_cluster_peer_publish_settle_view_t *out) {
  flowie_cluster_peer_publish_settle_view_t decoded =
      FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VIEW_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  uint32_t flags;
  size_t client_id_size;
  int rc;
  if (!out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VERSION || !bytes)
    return TURBO_EINVAL;
  if (data_size > max_payload_size) return TURBO_EMSGSIZE;
  if (data_size < FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE) return TURBO_EPROTO;
  flags = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_FLAGS);
  if (memcmp(bytes, FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(
          bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VERSION ||
      flowie_cluster_peer_wire_read_u16(
          bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(
          bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_TOTAL_SIZE) != data_size ||
      flowie_cluster_peer_wire_read_u16(
          bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_RESERVED16) != 0u ||
      flowie_cluster_peer_wire_read_u32(
          bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_RESERVED32) != 0u ||
      (flags & ~FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_KNOWN_FLAGS) != 0u)
    return TURBO_EPROTO;
  client_id_size = flowie_cluster_peer_wire_read_u16(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_CLIENT_ID_SIZE);
  if (client_id_size == 0u ||
      client_id_size != data_size - FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE)
    return TURBO_EPROTO;
  decoded.client_id = (flowie_mqtt_span_t){
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE, client_id_size};
  decoded.settlement.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  decoded.settlement.message.protocol_version = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_PROTOCOL_VERSION);
  decoded.settlement.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
  decoded.settlement.message.qos = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_QOS);
  decoded.settlement.message.packet_id = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_PACKET_ID);
  decoded.settlement.message.session_generation = flowie_cluster_peer_wire_read_u64(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_SESSION_GENERATION);
  decoded.settlement.message.duplicate =
      (flags & FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_DUPLICATE) != 0u;
  decoded.settlement.message.retain =
      (flags & FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_RETAIN) != 0u;
  decoded.settlement.point = (turbo_flow_protocol_settlement_point_t)
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_POINT);
  decoded.settlement.status = (int32_t)flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_STATUS);
  decoded.settlement.attempt = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_ATTEMPT);
  decoded.settlement.message_id = flowie_cluster_peer_wire_read_u64(
      bytes + FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_OFFSET_MESSAGE_ID);
  rc = flowie_cluster_peer_publish_settlement_validate(decoded.client_id, &decoded.settlement);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}
