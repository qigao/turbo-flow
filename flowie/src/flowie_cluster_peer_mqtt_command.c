#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_MQTT_COMMAND_MAGIC[4] = {'T', 'F', 'M', 'Q'};

enum {
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_TOTAL_SIZE = 8,
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_PACKET_SIZE = 12,
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_CLIENT_ID_SIZE = 16,
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_MQTT_VERSION = 18,
  FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_RESERVED = 19
};

static int
flowie_cluster_peer_mqtt_command_operation_type(flowie_cluster_peer_operation_t operation,
                                                flowie_mqtt_packet_type_t packet_type) {
  switch (operation) {
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH:
    return packet_type == FLOWIE_MQTT_PACKET_PUBLISH;
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE:
    return packet_type == FLOWIE_MQTT_PACKET_SUBSCRIBE;
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE:
    return packet_type == FLOWIE_MQTT_PACKET_UNSUBSCRIBE;
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK:
    return packet_type == FLOWIE_MQTT_PACKET_PUBACK || packet_type == FLOWIE_MQTT_PACKET_PUBREC ||
           packet_type == FLOWIE_MQTT_PACKET_PUBREL || packet_type == FLOWIE_MQTT_PACKET_PUBCOMP;
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT:
    return packet_type == FLOWIE_MQTT_PACKET_DISCONNECT;
  default:
    return 0;
  }
}

static int
flowie_cluster_peer_mqtt_command_operation_supported(flowie_cluster_peer_operation_t operation) {
  switch (operation) {
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH:
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE:
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE:
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK:
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT:
    return 1;
  default:
    return 0;
  }
}

static int
flowie_cluster_peer_mqtt_command_typed_validate(flowie_cluster_peer_operation_t operation,
                                                const flowie_mqtt_packet_view_t *packet) {
  switch (operation) {
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH: {
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    return flowie_mqtt_publish_parse(packet, &publish) == FLOWIE_MQTT_PARSE_OK ? TURBO_OK
                                                                               : TURBO_EPROTO;
  }
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE: {
    flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    return flowie_mqtt_subscribe_parse(packet, &subscribe) == FLOWIE_MQTT_PARSE_OK ? TURBO_OK
                                                                                   : TURBO_EPROTO;
  }
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE: {
    flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    return flowie_mqtt_unsubscribe_parse(packet, &unsubscribe) == FLOWIE_MQTT_PARSE_OK
               ? TURBO_OK
               : TURBO_EPROTO;
  }
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK:
  case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT: {
    flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    return flowie_mqtt_control_packet_parse(packet, &control) == FLOWIE_MQTT_PARSE_OK
               ? TURBO_OK
               : TURBO_EPROTO;
  }
  default:
    return TURBO_ENOTSUP;
  }
}

static int flowie_cluster_peer_mqtt_command_packet_parse(flowie_cluster_peer_operation_t operation,
                                                         flowie_mqtt_version_t mqtt_version,
                                                         const uint8_t *packet_data,
                                                         size_t packet_size, size_t max_packet_size,
                                                         flowie_mqtt_packet_view_t *out) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  size_t consumed = 0u;
  int rc;
  if (!flowie_cluster_peer_mqtt_command_operation_supported(operation)) return TURBO_ENOTSUP;
  if (!flowie_mqtt_version_is_supported(mqtt_version) || !packet_data || packet_size == 0u ||
      max_packet_size == 0u || max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE)
    return TURBO_EINVAL;
  if (packet_size > max_packet_size) return TURBO_EMSGSIZE;
  options.version = mqtt_version;
  options.max_packet_size = max_packet_size;
  rc = flowie_mqtt_packet_parse(packet_data, packet_size, &options, out, &consumed, NULL);
  if (rc == FLOWIE_MQTT_PARSE_NO_MEMORY) return TURBO_ENOMEM;
  if (rc == FLOWIE_MQTT_PARSE_TOO_LARGE) return TURBO_EMSGSIZE;
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != packet_size ||
      !flowie_cluster_peer_mqtt_command_operation_type(operation, out->type))
    return TURBO_EPROTO;
  return flowie_cluster_peer_mqtt_command_typed_validate(operation, out);
}

int flowie_cluster_peer_mqtt_command_encode(flowie_cluster_peer_operation_t operation,
                                            flowie_mqtt_version_t mqtt_version,
                                            flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
                                            size_t max_packet_size, tstr_t *out) {
  flowie_mqtt_packet_view_t parsed = FLOWIE_MQTT_PACKET_VIEW_INIT;
  uint8_t *encoded;
  size_t total_size;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!client_id.data || client_id.size == 0u || client_id.size > UINT16_MAX ||
      !flowie_mqtt_utf8_validate(client_id))
    return TURBO_EPROTO;
  rc = flowie_cluster_peer_mqtt_command_packet_parse(operation, mqtt_version, packet.data,
                                                     packet.size, max_packet_size, &parsed);
  if (rc != TURBO_OK) return rc;
  if (client_id.size > SIZE_MAX - FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE ||
      packet.size > SIZE_MAX - FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE - client_id.size)
    return TURBO_ERANGE;
  total_size = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE + client_id.size + packet.size;
  if (total_size > UINT32_MAX || packet.size > UINT32_MAX) return TURBO_ERANGE;
  *out = tstr_new_len(NULL, total_size);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  memcpy(encoded, FLOWIE_CLUSTER_PEER_MQTT_COMMAND_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_MQTT_COMMAND_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_TOTAL_SIZE,
                                     (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_PACKET_SIZE,
                                     (uint32_t)packet.size);
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_CLIENT_ID_SIZE, (uint16_t)client_id.size);
  encoded[FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_MQTT_VERSION] = (uint8_t)mqtt_version;
  encoded[FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_RESERVED] = 0u;
  memcpy(encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE, client_id.data, client_id.size);
  memcpy(encoded + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE + client_id.size, packet.data,
         packet.size);
  return TURBO_OK;
}

int flowie_cluster_peer_mqtt_command_decode(flowie_cluster_peer_operation_t operation,
                                            const void *data, size_t data_size,
                                            size_t max_packet_size,
                                            flowie_cluster_peer_mqtt_command_view_t *out) {
  flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  size_t client_id_size;
  size_t packet_size;
  size_t expected_size;
  int rc;
  if (!out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VERSION || !bytes)
    return TURBO_EINVAL;
  if (data_size < FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE) return TURBO_EPROTO;
  if (memcmp(bytes, FLOWIE_CLUSTER_PEER_MQTT_COMMAND_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_MQTT_COMMAND_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes +
                                        FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE ||
      bytes[FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_RESERVED] != 0u)
    return TURBO_EPROTO;
  if (flowie_cluster_peer_wire_read_u32(
          bytes + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_TOTAL_SIZE) != data_size)
    return TURBO_EPROTO;
  client_id_size = flowie_cluster_peer_wire_read_u16(
      bytes + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_CLIENT_ID_SIZE);
  packet_size = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_PACKET_SIZE);
  if (client_id_size == 0u ||
      client_id_size > SIZE_MAX - FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE ||
      packet_size > SIZE_MAX - FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE - client_id_size)
    return TURBO_EPROTO;
  expected_size = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE + client_id_size + packet_size;
  if (expected_size != data_size) return TURBO_EPROTO;
  decoded.operation = operation;
  decoded.mqtt_version =
      (flowie_mqtt_version_t)bytes[FLOWIE_CLUSTER_PEER_MQTT_COMMAND_OFFSET_MQTT_VERSION];
  decoded.client_id.data = bytes + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE;
  decoded.client_id.size = client_id_size;
  if (!flowie_mqtt_utf8_validate(decoded.client_id)) return TURBO_EPROTO;
  rc = flowie_cluster_peer_mqtt_command_packet_parse(
      operation, decoded.mqtt_version,
      bytes + FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE + client_id_size, packet_size,
      max_packet_size, &decoded.packet);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}
