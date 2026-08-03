#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_MQTT_REPLY_MAGIC[4] = {'T', 'F', 'R', 'P'};

enum {
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_TOTAL_SIZE = 8,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_PACKET_SIZE = 12,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_MQTT_VERSION = 16,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_FLAGS = 17,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_SETTLEMENT = 18,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_RESERVED = 19
};

enum {
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_PACKET = 1u << 0u,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_CLOSE = 1u << 1u,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_SETTLEMENT = 1u << 2u,
  FLOWIE_CLUSTER_PEER_MQTT_REPLY_KNOWN_FLAGS = FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_PACKET |
                                               FLOWIE_CLUSTER_PEER_MQTT_REPLY_CLOSE |
                                               FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_SETTLEMENT
};

static int
flowie_cluster_peer_mqtt_reply_settlement_valid(turbo_flow_protocol_settlement_point_t point) {
  return point == 0 || (point >= TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED &&
                        point <= TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
}

static int flowie_cluster_peer_mqtt_reply_packet_allowed(flowie_mqtt_packet_type_t type) {
  return type == FLOWIE_MQTT_PACKET_PUBLISH || type == FLOWIE_MQTT_PACKET_PUBACK ||
         type == FLOWIE_MQTT_PACKET_PUBREC || type == FLOWIE_MQTT_PACKET_PUBREL ||
         type == FLOWIE_MQTT_PACKET_PUBCOMP || type == FLOWIE_MQTT_PACKET_SUBACK ||
         type == FLOWIE_MQTT_PACKET_UNSUBACK || type == FLOWIE_MQTT_PACKET_PINGRESP ||
         type == FLOWIE_MQTT_PACKET_DISCONNECT || type == FLOWIE_MQTT_PACKET_AUTH;
}

static int flowie_cluster_peer_mqtt_reply_packet_parse(flowie_mqtt_version_t mqtt_version,
                                                       const void *data, size_t data_size,
                                                       size_t max_packet_size,
                                                       flowie_mqtt_packet_view_t *out) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  size_t consumed = 0u;
  int rc;
  if (!data || data_size == 0u) return TURBO_EINVAL;
  if (data_size > max_packet_size) return TURBO_EMSGSIZE;
  options.version = mqtt_version;
  options.max_packet_size = max_packet_size;
  rc = flowie_mqtt_packet_parse(data, data_size, &options, out, &consumed, NULL);
  if (rc == FLOWIE_MQTT_PARSE_NO_MEMORY) return TURBO_ENOMEM;
  if (rc == FLOWIE_MQTT_PARSE_TOO_LARGE) return TURBO_EMSGSIZE;
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != data_size ||
      !flowie_cluster_peer_mqtt_reply_packet_allowed(out->type))
    return TURBO_EPROTO;
  if (out->type == FLOWIE_MQTT_PACKET_PUBLISH)
    return flowie_mqtt_publish_parse(out, &publish) == FLOWIE_MQTT_PARSE_OK ? TURBO_OK
                                                                            : TURBO_EPROTO;
  return flowie_mqtt_control_packet_parse(out, &control) == FLOWIE_MQTT_PARSE_OK ? TURBO_OK
                                                                                 : TURBO_EPROTO;
}

int flowie_cluster_peer_mqtt_reply_encode(flowie_mqtt_version_t mqtt_version,
                                          flowie_mqtt_span_t packet, int close_after_send,
                                          turbo_flow_protocol_settlement_point_t settlement_point,
                                          size_t max_packet_size, tstr_t *out) {
  flowie_mqtt_packet_view_t parsed = FLOWIE_MQTT_PACKET_VIEW_INIT;
  uint8_t flags = 0u;
  uint8_t *encoded;
  size_t total_size;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!flowie_mqtt_version_is_supported(mqtt_version) || max_packet_size == 0u ||
      max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE || close_after_send < 0 ||
      close_after_send > 1 || !flowie_cluster_peer_mqtt_reply_settlement_valid(settlement_point) ||
      (packet.size != 0u && !packet.data))
    return TURBO_EINVAL;
  if (packet.size != 0u) {
    rc = flowie_cluster_peer_mqtt_reply_packet_parse(mqtt_version, packet.data, packet.size,
                                                     max_packet_size, &parsed);
    if (rc != TURBO_OK) return rc;
    flags |= FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_PACKET;
  }
  if (close_after_send) flags |= FLOWIE_CLUSTER_PEER_MQTT_REPLY_CLOSE;
  if (settlement_point != 0) flags |= FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_SETTLEMENT;
  if (packet.size > SIZE_MAX - FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE) return TURBO_ERANGE;
  total_size = FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE + packet.size;
  if (total_size > UINT32_MAX || packet.size > UINT32_MAX) return TURBO_ERANGE;
  *out = tstr_new_len(NULL, total_size);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  memcpy(encoded, FLOWIE_CLUSTER_PEER_MQTT_REPLY_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_MQTT_REPLY_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_PEER_MQTT_REPLY_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_TOTAL_SIZE,
                                     (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_PACKET_SIZE,
                                     (uint32_t)packet.size);
  encoded[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_MQTT_VERSION] = (uint8_t)mqtt_version;
  encoded[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_FLAGS] = flags;
  encoded[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_SETTLEMENT] = (uint8_t)settlement_point;
  encoded[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_RESERVED] = 0u;
  if (packet.size != 0u)
    memcpy(encoded + FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE, packet.data, packet.size);
  return TURBO_OK;
}

int flowie_cluster_peer_mqtt_reply_decode(const void *data, size_t data_size,
                                          size_t max_packet_size,
                                          flowie_cluster_peer_mqtt_reply_action_t *out) {
  flowie_cluster_peer_mqtt_reply_action_t decoded = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  uint8_t flags;
  size_t packet_size;
  int rc;
  if (!out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_MQTT_REPLY_VERSION || !bytes ||
      max_packet_size == 0u || max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE)
    return TURBO_EINVAL;
  if (data_size < FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE) return TURBO_EPROTO;
  flags = bytes[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_FLAGS];
  decoded.mqtt_version =
      (flowie_mqtt_version_t)bytes[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_MQTT_VERSION];
  decoded.settlement_point = (turbo_flow_protocol_settlement_point_t)
      bytes[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_SETTLEMENT];
  if (memcmp(bytes, FLOWIE_CLUSTER_PEER_MQTT_REPLY_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_MQTT_REPLY_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_MQTT_REPLY_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes +
                                        FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_TOTAL_SIZE) !=
          data_size ||
      bytes[FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_RESERVED] != 0u ||
      (flags & (uint8_t)~FLOWIE_CLUSTER_PEER_MQTT_REPLY_KNOWN_FLAGS) != 0u ||
      !flowie_mqtt_version_is_supported(decoded.mqtt_version) ||
      !flowie_cluster_peer_mqtt_reply_settlement_valid(decoded.settlement_point))
    return TURBO_EPROTO;
  packet_size =
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_PEER_MQTT_REPLY_OFFSET_PACKET_SIZE);
  if (packet_size > SIZE_MAX - FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE ||
      FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE + packet_size != data_size ||
      ((flags & FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_PACKET) != 0u) != (packet_size != 0u) ||
      ((flags & FLOWIE_CLUSTER_PEER_MQTT_REPLY_HAS_SETTLEMENT) != 0u) !=
          (decoded.settlement_point != 0))
    return TURBO_EPROTO;
  decoded.close_after_send = (flags & FLOWIE_CLUSTER_PEER_MQTT_REPLY_CLOSE) != 0u ? 1u : 0u;
  if (packet_size != 0u) {
    rc = flowie_cluster_peer_mqtt_reply_packet_parse(
        decoded.mqtt_version, bytes + FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE, packet_size,
        max_packet_size, &decoded.packet);
    if (rc != TURBO_OK) return rc;
  }
  *out = decoded;
  return TURBO_OK;
}
