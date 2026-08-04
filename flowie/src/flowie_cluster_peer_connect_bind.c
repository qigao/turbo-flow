#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"
#include "flowie_security_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_CONNECT_BIND_MAGIC[4] = {'T', 'F', 'C', 'B'};

enum {
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_TOTAL_SIZE = 8,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_CONNECT_SIZE = 12,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_PRINCIPAL_SIZE = 16,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_FLAGS = 20,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_REMOTE_ADDRESS_SIZE = 24,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_TRANSPORT_PEER_ADDRESS_SIZE = 26,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_PROXY_TLVS_SIZE = 28,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_FLAG_SECURITY = 1,
  FLOWIE_CLUSTER_PEER_CONNECT_BIND_PRINCIPAL_METADATA_SIZE = 24
};

static int flowie_cluster_peer_connect_bind_parse_error(int rc) {
  if (rc == FLOWIE_MQTT_PARSE_INVALID_ARGUMENT) return TURBO_EINVAL;
  if (rc == FLOWIE_MQTT_PARSE_TOO_LARGE) return TURBO_EMSGSIZE;
  if (rc == FLOWIE_MQTT_PARSE_NO_MEMORY) return TURBO_ENOMEM;
  return TURBO_EPROTO;
}

static int flowie_cluster_peer_connect_bind_size_add(size_t *total, size_t value) {
  if (!total) return TURBO_EINVAL;
  if (value > SIZE_MAX - *total) return TURBO_ERANGE;
  *total += value;
  return TURBO_OK;
}

static int
flowie_cluster_peer_connect_bind_principal_size(const turbo_flow_security_principal_t *principal,
                                                size_t *out) {
  size_t total = FLOWIE_CLUSTER_PEER_CONNECT_BIND_PRINCIPAL_METADATA_SIZE;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = 0u;
  rc = flowie_security_principal_validate(principal);
  if (rc != TURBO_OK) return rc;
#define FLOWIE_CONNECT_BIND_ADD_TEXT(value)                                                        \
  do {                                                                                             \
    rc = flowie_cluster_peer_connect_bind_size_add(&total, 2u + strlen(value));                    \
    if (rc != TURBO_OK) return rc;                                                                 \
  } while (0)
  FLOWIE_CONNECT_BIND_ADD_TEXT(principal->principal_id);
  FLOWIE_CONNECT_BIND_ADD_TEXT(principal->principal_type);
  FLOWIE_CONNECT_BIND_ADD_TEXT(principal->domain_id);
  FLOWIE_CONNECT_BIND_ADD_TEXT(principal->auth_method);
  for (uint32_t index = 0u; index < principal->role_count; ++index)
    FLOWIE_CONNECT_BIND_ADD_TEXT(principal->roles[index]);
  for (uint32_t index = 0u; index < principal->group_count; ++index)
    FLOWIE_CONNECT_BIND_ADD_TEXT(principal->groups[index]);
#undef FLOWIE_CONNECT_BIND_ADD_TEXT
  if (total > UINT32_MAX) return TURBO_ERANGE;
  *out = total;
  return TURBO_OK;
}

static void flowie_cluster_peer_connect_bind_text_encode(uint8_t *out, size_t *offset,
                                                         const char *value) {
  size_t length = strlen(value);
  flowie_cluster_peer_wire_write_u16(out + *offset, (uint16_t)length);
  *offset += 2u;
  if (length != 0u) memcpy(out + *offset, value, length);
  *offset += length;
}

static int
flowie_cluster_peer_connect_bind_principal_encode(const turbo_flow_security_principal_t *principal,
                                                  uint8_t *out, size_t capacity) {
  size_t required;
  size_t offset = FLOWIE_CLUSTER_PEER_CONNECT_BIND_PRINCIPAL_METADATA_SIZE;
  int rc = flowie_cluster_peer_connect_bind_principal_size(principal, &required);
  if (rc != TURBO_OK) return rc;
  if (!out || capacity != required) return TURBO_EINVAL;
  flowie_cluster_peer_wire_write_u32(out, (uint32_t)principal->scope);
  flowie_cluster_peer_wire_write_u16(out + 4u, (uint16_t)principal->role_count);
  flowie_cluster_peer_wire_write_u16(out + 6u, (uint16_t)principal->group_count);
  flowie_cluster_peer_wire_write_u64(out + 8u, principal->expires_at);
  flowie_cluster_peer_wire_write_u64(out + 16u, principal->policy_version);
  flowie_cluster_peer_connect_bind_text_encode(out, &offset, principal->principal_id);
  flowie_cluster_peer_connect_bind_text_encode(out, &offset, principal->principal_type);
  flowie_cluster_peer_connect_bind_text_encode(out, &offset, principal->domain_id);
  flowie_cluster_peer_connect_bind_text_encode(out, &offset, principal->auth_method);
  for (uint32_t index = 0u; index < principal->role_count; ++index)
    flowie_cluster_peer_connect_bind_text_encode(out, &offset, principal->roles[index]);
  for (uint32_t index = 0u; index < principal->group_count; ++index)
    flowie_cluster_peer_connect_bind_text_encode(out, &offset, principal->groups[index]);
  return offset == required ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_cluster_peer_connect_bind_text_decode(const uint8_t *data, size_t data_size,
                                                        size_t *offset, char *out, size_t capacity,
                                                        int required) {
  size_t length;
  if (!data || !offset || !out || capacity == 0u || *offset > data_size || data_size - *offset < 2u)
    return TURBO_EPROTO;
  length = flowie_cluster_peer_wire_read_u16(data + *offset);
  *offset += 2u;
  if (length >= capacity || length > data_size - *offset || (required && length == 0u) ||
      (length != 0u && memchr(data + *offset, '\0', length) != NULL))
    return TURBO_EPROTO;
  if (length != 0u) memcpy(out, data + *offset, length);
  out[length] = '\0';
  *offset += length;
  return TURBO_OK;
}

static int flowie_cluster_peer_connect_bind_principal_decode(const uint8_t *data, size_t data_size,
                                                             turbo_flow_security_principal_t *out) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  size_t offset = FLOWIE_CLUSTER_PEER_CONNECT_BIND_PRINCIPAL_METADATA_SIZE;
  uint32_t scope;
  int rc;
  if (!data || data_size < FLOWIE_CLUSTER_PEER_CONNECT_BIND_PRINCIPAL_METADATA_SIZE || !out)
    return TURBO_EPROTO;
  scope = flowie_cluster_peer_wire_read_u32(data);
  principal.role_count = flowie_cluster_peer_wire_read_u16(data + 4u);
  principal.group_count = flowie_cluster_peer_wire_read_u16(data + 6u);
  principal.expires_at = flowie_cluster_peer_wire_read_u64(data + 8u);
  principal.policy_version = flowie_cluster_peer_wire_read_u64(data + 16u);
  if (scope < TURBO_FLOW_SECURITY_SCOPE_SELF || scope > TURBO_FLOW_SECURITY_SCOPE_SYSTEM ||
      principal.role_count > TURBO_FLOW_SECURITY_MAX_ROLES ||
      principal.group_count > TURBO_FLOW_SECURITY_MAX_GROUPS)
    return TURBO_EPROTO;
  principal.scope = (turbo_flow_security_scope_t)scope;
  rc = flowie_cluster_peer_connect_bind_text_decode(
      data, data_size, &offset, principal.principal_id, sizeof(principal.principal_id), 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connect_bind_text_decode(
        data, data_size, &offset, principal.principal_type, sizeof(principal.principal_type), 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connect_bind_text_decode(
        data, data_size, &offset, principal.domain_id, sizeof(principal.domain_id),
        principal.scope != TURBO_FLOW_SECURITY_SCOPE_SYSTEM);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connect_bind_text_decode(
        data, data_size, &offset, principal.auth_method, sizeof(principal.auth_method), 1);
  for (uint32_t index = 0u; rc == TURBO_OK && index < principal.role_count; ++index)
    rc = flowie_cluster_peer_connect_bind_text_decode(
        data, data_size, &offset, principal.roles[index], sizeof(principal.roles[index]), 1);
  for (uint32_t index = 0u; rc == TURBO_OK && index < principal.group_count; ++index)
    rc = flowie_cluster_peer_connect_bind_text_decode(
        data, data_size, &offset, principal.groups[index], sizeof(principal.groups[index]), 1);
  if (rc != TURBO_OK || offset != data_size ||
      flowie_security_principal_validate(&principal) != TURBO_OK)
    return TURBO_EPROTO;
  *out = principal;
  return TURBO_OK;
}

static int
flowie_cluster_peer_connect_bind_session_properties(const flowie_mqtt_connect_view_t *connect,
                                                    uint8_t encoded[5], flowie_mqtt_span_t *out) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  int found = 0;
  int rc;
  if (!connect || !encoded || !out) return TURBO_EINVAL;
  *out = (flowie_mqtt_span_t){NULL, 0u};
  if (connect->version != FLOWIE_MQTT_VERSION_5) {
    return connect->properties.values.size == 0u ? TURBO_OK : TURBO_EPROTO;
  }
  rc = flowie_mqtt_property_iterator_init(&connect->properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    if (property.identifier != FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL) continue;
    if (found) return TURBO_EPROTO;
    encoded[0] = FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL;
    flowie_cluster_peer_wire_write_u32(encoded + 1u, property.integer);
    *out = (flowie_mqtt_span_t){encoded, 5u};
    found = 1;
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? TURBO_OK : TURBO_EPROTO;
}

static int
flowie_cluster_peer_connect_bind_connect_validate(const flowie_mqtt_connect_view_t *connect) {
  uint8_t ignored_data[5];
  flowie_mqtt_span_t ignored;
  if (!connect || connect->size < sizeof(*connect) ||
      connect->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      connect->properties.size < sizeof(connect->properties) ||
      connect->properties.abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      connect->will_properties.size < sizeof(connect->will_properties) ||
      connect->will_properties.abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !flowie_mqtt_version_is_supported(connect->version) || connect->client_id.size == 0u ||
      connect->client_id.size > UINT16_MAX || !flowie_mqtt_utf8_validate(connect->client_id))
    return TURBO_EINVAL;
  return flowie_cluster_peer_connect_bind_session_properties(connect, ignored_data, &ignored);
}

static int flowie_cluster_peer_connect_bind_sanitized_properties_validate(
    const flowie_mqtt_connect_view_t *connect) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  int count = 0;
  int rc;
  if (connect->version != FLOWIE_MQTT_VERSION_5)
    return connect->properties.values.size == 0u ? TURBO_OK : TURBO_EPROTO;
  rc = flowie_mqtt_property_iterator_init(&connect->properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    if (property.identifier != FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL || ++count > 1)
      return TURBO_EPROTO;
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_cluster_peer_connect_bind_metadata_validate(
    const flowie_cluster_peer_ingress_metadata_t *metadata) {
  if (!metadata) return TURBO_OK;
  if ((!metadata->remote_address.data && metadata->remote_address.len != 0u) ||
      (!metadata->transport_peer_address.data && metadata->transport_peer_address.len != 0u) ||
      (!metadata->proxy_tlvs.data && metadata->proxy_tlvs.len != 0u) ||
      metadata->remote_address.len > FLOWIE_CLUSTER_PEER_ADDRESS_MAX ||
      metadata->transport_peer_address.len > FLOWIE_CLUSTER_PEER_ADDRESS_MAX ||
      metadata->proxy_tlvs.len > UINT32_MAX ||
      ((metadata->remote_address.len == 0u) !=
       (metadata->transport_peer_address.len == 0u)) ||
      (metadata->remote_address.len != 0u &&
       memchr(metadata->remote_address.data, '\0', metadata->remote_address.len) != NULL) ||
      (metadata->transport_peer_address.len != 0u &&
       memchr(metadata->transport_peer_address.data, '\0',
              metadata->transport_peer_address.len) != NULL))
    return TURBO_EINVAL;
  return TURBO_OK;
}

int flowie_cluster_peer_connect_bind_encode(const flowie_mqtt_connect_view_t *connect,
                                            const turbo_flow_security_principal_t *principal,
                                            size_t max_payload_size, tstr_t *out) {
  return flowie_cluster_peer_connect_bind_encode_with_metadata(connect, principal, NULL,
                                                               max_payload_size, out);
}

int flowie_cluster_peer_connect_bind_encode_with_metadata(
    const flowie_mqtt_connect_view_t *connect,
    const turbo_flow_security_principal_t *principal,
    const flowie_cluster_peer_ingress_metadata_t *metadata, size_t max_payload_size,
    tstr_t *out) {
  flowie_mqtt_connect_packet_t sanitized = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  uint8_t session_expiry[5];
  flowie_mqtt_span_t properties;
  uint8_t *encoded;
  size_t principal_size = 0u;
  size_t connect_capacity;
  size_t connect_size = 0u;
  size_t remote_address_size = metadata ? metadata->remote_address.len : 0u;
  size_t transport_peer_address_size = metadata ? metadata->transport_peer_address.len : 0u;
  size_t proxy_tlvs_size = metadata ? metadata->proxy_tlvs.len : 0u;
  size_t metadata_size;
  size_t total_size;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (max_payload_size < FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE ||
      max_payload_size > UINT32_MAX)
    return TURBO_EINVAL;
  rc = flowie_cluster_peer_connect_bind_connect_validate(connect);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_peer_connect_bind_metadata_validate(metadata);
  if (rc != TURBO_OK) return rc;
  if (principal) {
    rc = flowie_cluster_peer_connect_bind_principal_size(principal, &principal_size);
    if (rc != TURBO_OK) return rc;
  }
  metadata_size = remote_address_size + transport_peer_address_size;
  if (metadata_size > SIZE_MAX - proxy_tlvs_size) return TURBO_ERANGE;
  metadata_size += proxy_tlvs_size;
  if (principal_size > max_payload_size - FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE ||
      metadata_size > max_payload_size - FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE -
                          principal_size)
    return TURBO_EMSGSIZE;
  rc = flowie_cluster_peer_connect_bind_session_properties(connect, session_expiry, &properties);
  if (rc != TURBO_OK) return rc;
  *out = tstr_new_len(NULL, max_payload_size);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  sanitized.version = connect->version;
  sanitized.clean_start = connect->clean_start;
  sanitized.has_will = connect->will_topic.size != 0u;
  sanitized.will_qos = connect->will_qos;
  sanitized.will_retain = connect->will_retain;
  sanitized.keep_alive = connect->keep_alive;
  sanitized.properties = properties;
  sanitized.client_id = connect->client_id;
  sanitized.will_properties = connect->will_properties.values;
  sanitized.will_topic = connect->will_topic;
  sanitized.will_payload = connect->will_payload;
  if (principal) {
    rc = flowie_cluster_peer_connect_bind_principal_encode(
        principal, encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE, principal_size);
    if (rc != TURBO_OK) goto fail;
  }
  if (remote_address_size != 0u)
    memcpy(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE + principal_size,
           metadata->remote_address.data, remote_address_size);
  if (transport_peer_address_size != 0u)
    memcpy(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE + principal_size +
               remote_address_size,
           metadata->transport_peer_address.data, transport_peer_address_size);
  if (proxy_tlvs_size != 0u)
    memcpy(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE + principal_size +
               remote_address_size + transport_peer_address_size,
           metadata->proxy_tlvs.data, proxy_tlvs_size);
  connect_capacity =
      max_payload_size - FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE - principal_size -
      metadata_size;
  rc = flowie_mqtt_connect_packet_encode(
      &sanitized, encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE + principal_size +
                      metadata_size,
      connect_capacity, &connect_size);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_cluster_peer_connect_bind_parse_error(rc);
    goto fail;
  }
  total_size = FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE + principal_size + metadata_size +
               connect_size;
  memcpy(encoded, FLOWIE_CLUSTER_PEER_CONNECT_BIND_MAGIC,
         sizeof(FLOWIE_CLUSTER_PEER_CONNECT_BIND_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_PEER_CONNECT_BIND_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_TOTAL_SIZE,
                                     (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_CONNECT_SIZE,
                                     (uint32_t)connect_size);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_PRINCIPAL_SIZE, (uint32_t)principal_size);
  flowie_cluster_peer_wire_write_u32(encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_FLAGS,
                                     principal ? FLOWIE_CLUSTER_PEER_CONNECT_BIND_FLAG_SECURITY
                                               : 0u);
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_REMOTE_ADDRESS_SIZE,
      (uint16_t)remote_address_size);
  flowie_cluster_peer_wire_write_u16(
      encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_TRANSPORT_PEER_ADDRESS_SIZE,
      (uint16_t)transport_peer_address_size);
  flowie_cluster_peer_wire_write_u32(
      encoded + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_PROXY_TLVS_SIZE,
      (uint32_t)proxy_tlvs_size);
  if (!tstr_set_len_checked(*out, total_size)) {
    rc = TURBO_ERANGE;
    goto fail;
  }
  return TURBO_OK;

fail:
  tstr_freep(out);
  return rc;
}

int flowie_cluster_peer_connect_bind_decode(const void *data, size_t data_size,
                                            size_t max_payload_size,
                                            flowie_cluster_peer_connect_bind_view_t *out) {
  flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  const uint8_t *principal_data;
  const uint8_t *metadata_data;
  const uint8_t *connect_data;
  size_t principal_size;
  size_t connect_size;
  size_t remote_address_size;
  size_t transport_peer_address_size;
  size_t proxy_tlvs_size;
  size_t metadata_size;
  size_t consumed = 0u;
  uint32_t flags;
  int rc;
  if (!out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_CONNECT_BIND_VERSION || !bytes ||
      max_payload_size < FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE ||
      max_payload_size > UINT32_MAX)
    return TURBO_EINVAL;
  if (data_size > max_payload_size) return TURBO_EMSGSIZE;
  if (data_size < FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE) return TURBO_EPROTO;
  if (memcmp(bytes, FLOWIE_CLUSTER_PEER_CONNECT_BIND_MAGIC,
             sizeof(FLOWIE_CLUSTER_PEER_CONNECT_BIND_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_CONNECT_BIND_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes +
                                        FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(
          bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_TOTAL_SIZE) != data_size)
    return TURBO_EPROTO;
  connect_size = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_CONNECT_SIZE);
  principal_size = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_PRINCIPAL_SIZE);
  flags = flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_FLAGS);
  remote_address_size = flowie_cluster_peer_wire_read_u16(
      bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_REMOTE_ADDRESS_SIZE);
  transport_peer_address_size = flowie_cluster_peer_wire_read_u16(
      bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_TRANSPORT_PEER_ADDRESS_SIZE);
  proxy_tlvs_size = flowie_cluster_peer_wire_read_u32(
      bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_OFFSET_PROXY_TLVS_SIZE);
  metadata_size = remote_address_size + transport_peer_address_size;
  if (metadata_size > SIZE_MAX - proxy_tlvs_size) return TURBO_EPROTO;
  metadata_size += proxy_tlvs_size;
  if ((flags & ~FLOWIE_CLUSTER_PEER_CONNECT_BIND_FLAG_SECURITY) != 0u || connect_size == 0u ||
      principal_size > data_size - FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE ||
      metadata_size > data_size - FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE - principal_size ||
      connect_size != data_size - FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE - principal_size -
                          metadata_size ||
      remote_address_size > FLOWIE_CLUSTER_PEER_ADDRESS_MAX ||
      transport_peer_address_size > FLOWIE_CLUSTER_PEER_ADDRESS_MAX ||
      ((remote_address_size == 0u) != (transport_peer_address_size == 0u)) ||
      (((flags & FLOWIE_CLUSTER_PEER_CONNECT_BIND_FLAG_SECURITY) != 0u) != (principal_size != 0u)))
    return TURBO_EPROTO;
  principal_data = bytes + FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE;
  metadata_data = principal_data + principal_size;
  connect_data = metadata_data + metadata_size;
  decoded.remote_address = (tstr_v){(const char *)metadata_data, remote_address_size};
  decoded.transport_peer_address =
      (tstr_v){(const char *)metadata_data + remote_address_size, transport_peer_address_size};
  decoded.proxy_tlvs =
      (tstr_v){(const char *)metadata_data + remote_address_size + transport_peer_address_size,
               proxy_tlvs_size};
  if ((remote_address_size != 0u &&
       memchr(decoded.remote_address.data, '\0', remote_address_size) != NULL) ||
      (transport_peer_address_size != 0u &&
       memchr(decoded.transport_peer_address.data, '\0', transport_peer_address_size) != NULL))
    return TURBO_EPROTO;
  if (principal_size != 0u) {
    rc = flowie_cluster_peer_connect_bind_principal_decode(principal_data, principal_size,
                                                           &decoded.principal);
    if (rc != TURBO_OK) return rc;
    decoded.security_enabled = 1u;
  }
  options.max_packet_size = max_payload_size;
  rc = flowie_mqtt_packet_parse(connect_data, connect_size, &options, &decoded.packet, &consumed,
                                NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_cluster_peer_connect_bind_parse_error(rc);
  if (consumed != connect_size || decoded.packet.type != FLOWIE_MQTT_PACKET_CONNECT ||
      flowie_mqtt_connect_parse(&decoded.packet, &decoded.connect) != FLOWIE_MQTT_PARSE_OK ||
      decoded.connect.client_id.size == 0u || decoded.connect.username.size != 0u ||
      decoded.connect.password.size != 0u)
    return TURBO_EPROTO;
  rc = flowie_cluster_peer_connect_bind_sanitized_properties_validate(&decoded.connect);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}
