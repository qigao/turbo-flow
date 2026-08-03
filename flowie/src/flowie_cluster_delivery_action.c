#include "flowie_cluster_delivery_action_internal.h"

#include "flowie_cluster_peer_wire_internal.h"

#include <string.h>

static const uint8_t FLOWIE_CLUSTER_DELIVERY_ACTION_MAGIC[4] = {'T', 'F', 'D', 'A'};

static int flowie_cluster_delivery_action_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  if (!bytes) return 0;
  for (size_t index = 0u; index < size; ++index)
    combined |= bytes[index];
  return combined != 0u;
}

static size_t flowie_cluster_delivery_vbi_write(uint8_t *output, uint32_t value) {
  size_t written = 0u;
  do {
    uint8_t byte = (uint8_t)(value % 128u);
    value /= 128u;
    if (value != 0u) byte |= UINT8_C(0x80);
    output[written++] = byte;
  } while (value != 0u);
  return written;
}

static int flowie_cluster_delivery_properties(
    const flowie_cluster_publish_event_view_t *event, const flowie_mqtt_publish_view_t *publish,
    flowie_mqtt_version_t target_version, const uint32_t *subscription_identifiers,
    size_t subscription_identifier_count, uint64_t now_epoch_seconds, tstr_t *out,
    uint64_t *expiry_at_epoch_seconds, int *expired) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  size_t capacity;
  size_t written = 0u;
  tstr_t properties;
  int rc;
  if (!event || !publish || !out || !expiry_at_epoch_seconds || !expired ||
      (subscription_identifier_count != 0u && !subscription_identifiers))
    return TURBO_EINVAL;
  *out = NULL;
  *expiry_at_epoch_seconds = 0u;
  *expired = 0;
  if (subscription_identifier_count > (SIZE_MAX - publish->properties.values.size) / 5u)
    return TURBO_ERANGE;
  capacity = publish->properties.values.size + subscription_identifier_count * 5u;
  properties = tstr_new_len(NULL, capacity);
  if (!properties) return TURBO_ENOMEM;
  rc = flowie_mqtt_property_iterator_init(&publish->properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    tstr_free(properties);
    return TURBO_EPROTO;
  }
  for (;;) {
    const uint8_t *begin = iterator.cursor;
    size_t property_size;
    rc = flowie_mqtt_property_iterator_next(&iterator, &property);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK || !begin || iterator.cursor < begin) {
      tstr_free(properties);
      return TURBO_EPROTO;
    }
    property_size = (size_t)(iterator.cursor - begin);
    if (property.identifier == FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS ||
        property.identifier == FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER)
      continue;
    if (property.identifier == FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL) {
      uint64_t deadline;
      uint64_t remaining;
      if (event->accepted_at_epoch_seconds == 0u ||
          property.integer > UINT64_MAX - event->accepted_at_epoch_seconds || property_size < 4u) {
        tstr_free(properties);
        return event->accepted_at_epoch_seconds == 0u ? TURBO_EPROTO : TURBO_ERANGE;
      }
      deadline = event->accepted_at_epoch_seconds + property.integer;
      *expiry_at_epoch_seconds = deadline;
      if (now_epoch_seconds >= deadline) {
        *expired = 1;
        tstr_free(properties);
        return TURBO_OK;
      }
      if (target_version != FLOWIE_MQTT_VERSION_5) continue;
      remaining = deadline - now_epoch_seconds;
      memcpy(properties + written, begin, property_size);
      properties[written + property_size - 4u] = (char)(remaining >> 24u);
      properties[written + property_size - 3u] = (char)(remaining >> 16u);
      properties[written + property_size - 2u] = (char)(remaining >> 8u);
      properties[written + property_size - 1u] = (char)remaining;
      written += property_size;
    } else if (target_version == FLOWIE_MQTT_VERSION_5) {
      memcpy(properties + written, begin, property_size);
      written += property_size;
    }
  }
  if (target_version == FLOWIE_MQTT_VERSION_5) {
    for (size_t index = 0u; index < subscription_identifier_count; ++index) {
      uint32_t identifier = subscription_identifiers[index];
      if (identifier == 0u || identifier > FLOWIE_MQTT_MAX_REMAINING_LENGTH) {
        tstr_free(properties);
        return TURBO_EPROTO;
      }
      properties[written++] = (char)FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER;
      written += flowie_cluster_delivery_vbi_write((uint8_t *)properties + written, identifier);
    }
  }
  if (!tstr_set_len_checked(properties, written)) {
    tstr_free(properties);
    return TURBO_ERANGE;
  }
  *out = properties;
  return TURBO_OK;
}

int flowie_cluster_delivery_packet_encode(
    const flowie_cluster_publish_event_view_t *event, flowie_mqtt_version_t target_version,
    uint8_t maximum_qos, uint8_t retain_as_published, uint16_t packet_id,
    const uint32_t *subscription_identifiers, size_t subscription_identifier_count,
    uint64_t now_epoch_seconds, size_t max_packet_size, tstr_t *out,
    uint64_t *expiry_at_epoch_seconds, int *expired) {
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_mqtt_publish_packet_t outbound = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  tstr_t properties = NULL;
  size_t capacity;
  size_t written = 0u;
  uint8_t qos;
  int rc;
  if (out) *out = NULL;
  if (expiry_at_epoch_seconds) *expiry_at_epoch_seconds = 0u;
  if (expired) *expired = 0;
  if (!event || event->size != sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION ||
      !flowie_mqtt_version_is_supported(target_version) || maximum_qos > 2u ||
      retain_as_published > 1u || now_epoch_seconds == 0u || max_packet_size == 0u || !out ||
      !expiry_at_epoch_seconds || !expired ||
      flowie_mqtt_publish_parse(&event->publish.packet, &publish) != FLOWIE_MQTT_PARSE_OK)
    return TURBO_EINVAL;
  qos = publish.qos < maximum_qos ? publish.qos : maximum_qos;
  if ((qos == 0u && packet_id != 0u) || (qos != 0u && packet_id == 0u)) return TURBO_EINVAL;
  rc = flowie_cluster_delivery_properties(
      event, &publish, target_version, subscription_identifiers, subscription_identifier_count,
      now_epoch_seconds, &properties, expiry_at_epoch_seconds, expired);
  if (rc != TURBO_OK || *expired) return rc;
  if (event->publish.packet.packet.size > SIZE_MAX - 8u ||
      tstr_len(properties) > SIZE_MAX - event->publish.packet.packet.size - 8u) {
    rc = TURBO_ERANGE;
    goto done;
  }
  capacity = event->publish.packet.packet.size + tstr_len(properties) + 8u;
  if (capacity > max_packet_size || capacity > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE) {
    rc = TURBO_EMSGSIZE;
    goto done;
  }
  *out = tstr_new_len(NULL, capacity);
  if (!*out) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  outbound.version = target_version;
  outbound.qos = qos;
  outbound.retain = (uint8_t)(publish.retain && retain_as_published);
  outbound.packet_id = packet_id;
  outbound.topic = publish.topic;
  outbound.properties =
      (flowie_mqtt_span_t){(const uint8_t *)properties, tstr_len(properties)};
  outbound.payload = publish.payload;
  rc = flowie_mqtt_publish_packet_encode(&outbound, (uint8_t *)*out, capacity, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK || !tstr_set_len_checked(*out, written)) {
    tstr_freep(out);
    rc = TURBO_EPROTO;
  } else {
    rc = TURBO_OK;
  }

done:
  tstr_free(properties);
  return rc;
}

int flowie_cluster_delivery_action_encode(
    tstr_v edge_node_id, const uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    uint64_t connection_id, uint64_t connection_generation, uint64_t session_id,
    uint64_t session_generation, uint64_t action_sequence, flowie_mqtt_version_t mqtt_version,
    flowie_mqtt_span_t packet, size_t max_payload_size, tstr_t *out) {
  tstr_t action = NULL;
  size_t action_limit;
  size_t total_size;
  uint8_t *encoded;
  int rc;
  if (out) *out = NULL;
  if (!out || !edge_node_id.data || edge_node_id.len == 0u ||
      edge_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || edge_node_id.len > UINT16_MAX ||
      memchr(edge_node_id.data, '\0', edge_node_id.len) ||
      !flowie_cluster_delivery_action_nonzero(edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      connection_id == 0u || connection_generation == 0u || session_id == 0u ||
      session_generation == 0u || action_sequence == 0u || !packet.data || packet.size == 0u)
    return TURBO_EINVAL;
  if (edge_node_id.len > SIZE_MAX - FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE ||
      FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE + edge_node_id.len >= max_payload_size)
    return TURBO_EMSGSIZE;
  action_limit = max_payload_size - FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE - edge_node_id.len;
  rc = flowie_cluster_peer_edge_action_encode(
      action_sequence, mqtt_version, packet, 0, (turbo_flow_protocol_settlement_point_t)0,
      action_limit, &action);
  if (rc != TURBO_OK) return rc;
  if (tstr_len(action) > SIZE_MAX - FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE - edge_node_id.len) {
    tstr_free(action);
    return TURBO_ERANGE;
  }
  total_size = FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE + edge_node_id.len + tstr_len(action);
  if (total_size > max_payload_size || total_size > UINT32_MAX || tstr_len(action) > UINT32_MAX) {
    tstr_free(action);
    return TURBO_EMSGSIZE;
  }
  *out = tstr_new_len(NULL, total_size);
  if (!*out) {
    tstr_free(action);
    return TURBO_ENOMEM;
  }
  encoded = (uint8_t *)*out;
  memset(encoded, 0, FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE);
  memcpy(encoded, FLOWIE_CLUSTER_DELIVERY_ACTION_MAGIC,
         sizeof(FLOWIE_CLUSTER_DELIVERY_ACTION_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + 4u, FLOWIE_CLUSTER_DELIVERY_ACTION_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + 6u, FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + 8u, (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(encoded + 12u, (uint32_t)tstr_len(action));
  flowie_cluster_peer_wire_write_u64(encoded + 16u, connection_id);
  flowie_cluster_peer_wire_write_u64(encoded + 24u, connection_generation);
  flowie_cluster_peer_wire_write_u64(encoded + 32u, session_id);
  flowie_cluster_peer_wire_write_u64(encoded + 40u, session_generation);
  memcpy(encoded + 48u, edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  flowie_cluster_peer_wire_write_u16(encoded + 64u, (uint16_t)edge_node_id.len);
  memcpy(encoded + FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE, edge_node_id.data,
         edge_node_id.len);
  memcpy(encoded + FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE + edge_node_id.len, action,
         tstr_len(action));
  tstr_free(action);
  return TURBO_OK;
}

int flowie_cluster_delivery_action_decode(const void *data, size_t data_size,
                                          size_t max_payload_size,
                                          flowie_cluster_delivery_action_view_t *out) {
  flowie_cluster_delivery_action_view_t decoded = FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  size_t edge_node_size;
  size_t action_size;
  size_t action_limit;
  int rc;
  if (!bytes || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_DELIVERY_ACTION_VERSION || max_payload_size == 0u)
    return TURBO_EINVAL;
  if (data_size > max_payload_size || data_size < FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE)
    return data_size > max_payload_size ? TURBO_EMSGSIZE : TURBO_EPROTO;
  if (memcmp(bytes, FLOWIE_CLUSTER_DELIVERY_ACTION_MAGIC,
             sizeof(FLOWIE_CLUSTER_DELIVERY_ACTION_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + 4u) != FLOWIE_CLUSTER_DELIVERY_ACTION_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) !=
          FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != data_size ||
      flowie_cluster_peer_wire_read_u16(bytes + 66u) != 0u ||
      flowie_cluster_peer_wire_read_u32(bytes + 68u) != 0u)
    return TURBO_EPROTO;
  action_size = flowie_cluster_peer_wire_read_u32(bytes + 12u);
  edge_node_size = flowie_cluster_peer_wire_read_u16(bytes + 64u);
  decoded.connection_id = flowie_cluster_peer_wire_read_u64(bytes + 16u);
  decoded.connection_generation = flowie_cluster_peer_wire_read_u64(bytes + 24u);
  decoded.session_id = flowie_cluster_peer_wire_read_u64(bytes + 32u);
  decoded.session_generation = flowie_cluster_peer_wire_read_u64(bytes + 40u);
  decoded.edge_boot_id = bytes + 48u;
  if (edge_node_size == 0u || edge_node_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      edge_node_size > data_size - FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE ||
      action_size != data_size - FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE - edge_node_size ||
      decoded.connection_id == 0u || decoded.connection_generation == 0u ||
      decoded.session_id == 0u || decoded.session_generation == 0u ||
      !flowie_cluster_delivery_action_nonzero(decoded.edge_boot_id,
                                              FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EPROTO;
  decoded.edge_node_id = tstr_v_from_buf(
      (const char *)bytes + FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE, edge_node_size);
  if (memchr(decoded.edge_node_id.data, '\0', decoded.edge_node_id.len)) return TURBO_EPROTO;
  decoded.encoded_action = tstr_v_from_buf(
      (const char *)bytes + FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE + edge_node_size,
      action_size);
  action_limit = max_payload_size - FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE - edge_node_size;
  rc = flowie_cluster_peer_edge_action_decode(
      decoded.encoded_action.data, decoded.encoded_action.len, action_limit, &decoded.edge_action);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}
