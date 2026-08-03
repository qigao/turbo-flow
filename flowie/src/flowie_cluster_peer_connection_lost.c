#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_CONNECTION_LOST_MAGIC[4] = {'T', 'F', 'C', 'L'};

enum {
  FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_TOTAL_SIZE = 8,
  FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_CLIENT_ID_SIZE = 12,
  FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_MQTT_VERSION = 14,
  FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_RESERVED = 15
};

int flowie_cluster_peer_connection_lost_encode(flowie_mqtt_version_t mqtt_version,
                                               flowie_mqtt_span_t client_id,
                                               size_t max_payload_size, tstr_t *out) {
  size_t total_size;
  uint8_t *encoded;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!flowie_mqtt_version_is_supported(mqtt_version) || !client_id.data || client_id.size == 0u ||
      client_id.size > UINT16_MAX || !flowie_mqtt_utf8_validate(client_id))
    return TURBO_EPROTO;
  if (client_id.size > SIZE_MAX - FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE)
    return TURBO_ERANGE;
  total_size = FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE + client_id.size;
  if (total_size > max_payload_size || total_size > UINT32_MAX) return TURBO_EMSGSIZE;
  *out = tstr_new_len(NULL, total_size);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  memcpy(encoded, FLOWIE_CLUSTER_PEER_CONNECTION_LOST_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_CONNECTION_LOST_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_PEER_CONNECTION_LOST_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded +
                                         FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_TOTAL_SIZE, (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u16(encoded +
                                         FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_CLIENT_ID_SIZE,
                                     (uint16_t)client_id.size);
  encoded[FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_MQTT_VERSION] = (uint8_t)mqtt_version;
  encoded[FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_RESERVED] = 0u;
  memcpy(encoded + FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE, client_id.data, client_id.size);
  return TURBO_OK;
}

int flowie_cluster_peer_connection_lost_decode(const void *data, size_t data_size,
                                               size_t max_payload_size,
                                               flowie_cluster_peer_connection_lost_view_t *out) {
  flowie_cluster_peer_connection_lost_view_t decoded =
      FLOWIE_CLUSTER_PEER_CONNECTION_LOST_VIEW_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  size_t client_id_size;
  if (!out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_CONNECTION_LOST_VERSION || !bytes)
    return TURBO_EINVAL;
  if (data_size > max_payload_size) return TURBO_EMSGSIZE;
  if (data_size < FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE) return TURBO_EPROTO;
  if (memcmp(bytes, FLOWIE_CLUSTER_PEER_CONNECTION_LOST_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_CONNECTION_LOST_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes +
                                        FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_CONNECTION_LOST_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes +
                                        FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(
          bytes + FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_TOTAL_SIZE) != data_size ||
      bytes[FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_RESERVED] != 0u)
    return TURBO_EPROTO;
  client_id_size = flowie_cluster_peer_wire_read_u16(
      bytes + FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_CLIENT_ID_SIZE);
  if (client_id_size == 0u ||
      client_id_size != data_size - FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE)
    return TURBO_EPROTO;
  decoded.mqtt_version =
      (flowie_mqtt_version_t)bytes[FLOWIE_CLUSTER_PEER_CONNECTION_LOST_OFFSET_MQTT_VERSION];
  decoded.client_id =
      (flowie_mqtt_span_t){bytes + FLOWIE_CLUSTER_PEER_CONNECTION_LOST_HEADER_SIZE, client_id_size};
  if (!flowie_mqtt_version_is_supported(decoded.mqtt_version) ||
      !flowie_mqtt_utf8_validate(decoded.client_id))
    return TURBO_EPROTO;
  *out = decoded;
  return TURBO_OK;
}
