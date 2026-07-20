#include "flowie_mqtt_protocol.h"

#include <string.h>

#define FLOWIE_MQTT_PROPERTY_BIT(id) (UINT64_C(1) << (id))

static const uint64_t flowie_mqtt_user_property_repeatable =
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_USER_PROPERTY);
static const uint64_t flowie_mqtt_connect_properties_allowed =
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS_MAXIMUM) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_REQUEST_RESPONSE_INFORMATION) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_REQUEST_PROBLEM_INFORMATION) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_USER_PROPERTY) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA);
static const uint64_t flowie_mqtt_will_properties_allowed =
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_CONTENT_TYPE) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_CORRELATION_DATA) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_USER_PROPERTY);
static const uint64_t flowie_mqtt_publish_properties_allowed =
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_CORRELATION_DATA) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_USER_PROPERTY) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_CONTENT_TYPE);
static const uint64_t flowie_mqtt_publish_properties_repeatable =
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_USER_PROPERTY) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER);
static const uint64_t flowie_mqtt_subscribe_properties_allowed =
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER) |
    FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_USER_PROPERTY);

static uint16_t flowie_mqtt_read_u16(const uint8_t *value) {
  return (uint16_t)(((uint16_t)value[0] << 8u) | value[1]);
}

static uint32_t flowie_mqtt_read_u32(const uint8_t *value) {
  return ((uint32_t)value[0] << 24u) | ((uint32_t)value[1] << 16u) | ((uint32_t)value[2] << 8u) |
         value[3];
}

static int flowie_mqtt_vbi(const uint8_t *bytes, size_t size, uint32_t *value, size_t *consumed) {
  uint32_t result = 0u;
  uint32_t multiplier = 1u;
  size_t offset = 0u;
  uint8_t byte;
  if ((!bytes && size != 0u) || !value || !consumed) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  do {
    if (offset == size) return FLOWIE_MQTT_PARSE_NEED_MORE;
    if (offset == 4u) return FLOWIE_MQTT_PARSE_MALFORMED;
    byte = bytes[offset++];
    result += (uint32_t)(byte & 0x7fu) * multiplier;
    multiplier *= 128u;
  } while ((byte & 0x80u) != 0u);
  if (offset > 1u && byte == 0u) return FLOWIE_MQTT_PARSE_MALFORMED;
  *value = result;
  *consumed = offset;
  return FLOWIE_MQTT_PARSE_OK;
}

static int flowie_mqtt_length_span(const uint8_t **cursor, const uint8_t *limit,
                                   flowie_mqtt_span_t *out, int utf8) {
  size_t length;
  if (!cursor || !*cursor || !limit || !out || *cursor > limit)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if ((size_t)(limit - *cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
  length = flowie_mqtt_read_u16(*cursor);
  *cursor += 2u;
  if ((size_t)(limit - *cursor) < length) return FLOWIE_MQTT_PARSE_MALFORMED;
  out->data = *cursor;
  out->size = length;
  *cursor += length;
  if (utf8 && !flowie_mqtt_utf8_validate(*out)) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_property_block_parse(flowie_mqtt_span_t bytes,
                                     flowie_mqtt_property_block_view_t *out, size_t *consumed) {
  flowie_mqtt_property_block_view_t parsed = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  uint32_t value_size;
  size_t prefix_size;
  int rc;
  if ((!bytes.data && bytes.size != 0u) || !out || out->size < sizeof(*out))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  rc = flowie_mqtt_vbi(bytes.data, bytes.size, &value_size, &prefix_size);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if ((size_t)value_size > bytes.size - prefix_size) return FLOWIE_MQTT_PARSE_NEED_MORE;
  parsed.encoded.data = bytes.data;
  parsed.encoded.size = prefix_size + value_size;
  parsed.values.data = bytes.data + prefix_size;
  parsed.values.size = value_size;
  *out = parsed;
  if (consumed) *consumed = parsed.encoded.size;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_property_iterator_init(const flowie_mqtt_property_block_view_t *block,
                                       flowie_mqtt_property_iterator_t *iterator) {
  if (!block || block->size < sizeof(*block) || block->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !iterator || iterator->size < sizeof(*iterator) ||
      iterator->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      (!block->values.data && block->values.size != 0u))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  iterator->cursor = block->values.data;
  iterator->limit =
      block->values.data ? block->values.data + block->values.size : block->values.data;
  return FLOWIE_MQTT_PARSE_OK;
}

static flowie_mqtt_property_type_t flowie_mqtt_property_type(uint32_t identifier) {
  switch (identifier) {
  case FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR:
  case FLOWIE_MQTT_PROPERTY_REQUEST_PROBLEM_INFORMATION:
  case FLOWIE_MQTT_PROPERTY_REQUEST_RESPONSE_INFORMATION:
  case FLOWIE_MQTT_PROPERTY_MAXIMUM_QOS:
  case FLOWIE_MQTT_PROPERTY_RETAIN_AVAILABLE:
  case FLOWIE_MQTT_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE:
  case FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER_AVAILABLE:
  case FLOWIE_MQTT_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE:
    return FLOWIE_MQTT_PROPERTY_BYTE;
  case FLOWIE_MQTT_PROPERTY_SERVER_KEEP_ALIVE:
  case FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM:
  case FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS_MAXIMUM:
  case FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS:
    return FLOWIE_MQTT_PROPERTY_UINT16;
  case FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL:
  case FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL:
  case FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL:
  case FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE:
    return FLOWIE_MQTT_PROPERTY_UINT32;
  case FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER:
    return FLOWIE_MQTT_PROPERTY_VARIABLE_INTEGER;
  case FLOWIE_MQTT_PROPERTY_CONTENT_TYPE:
  case FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC:
  case FLOWIE_MQTT_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER:
  case FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD:
  case FLOWIE_MQTT_PROPERTY_RESPONSE_INFORMATION:
  case FLOWIE_MQTT_PROPERTY_SERVER_REFERENCE:
  case FLOWIE_MQTT_PROPERTY_REASON_STRING:
    return FLOWIE_MQTT_PROPERTY_UTF8;
  case FLOWIE_MQTT_PROPERTY_CORRELATION_DATA:
  case FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA:
    return FLOWIE_MQTT_PROPERTY_BINARY;
  case FLOWIE_MQTT_PROPERTY_USER_PROPERTY:
    return FLOWIE_MQTT_PROPERTY_UTF8_PAIR;
  default:
    return 0;
  }
}

static int flowie_mqtt_property_scalar_valid(const flowie_mqtt_property_view_t *property) {
  switch (property->identifier) {
  case FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR:
  case FLOWIE_MQTT_PROPERTY_REQUEST_PROBLEM_INFORMATION:
  case FLOWIE_MQTT_PROPERTY_REQUEST_RESPONSE_INFORMATION:
  case FLOWIE_MQTT_PROPERTY_RETAIN_AVAILABLE:
  case FLOWIE_MQTT_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE:
  case FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER_AVAILABLE:
  case FLOWIE_MQTT_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE:
    return property->integer <= 1u;
  case FLOWIE_MQTT_PROPERTY_MAXIMUM_QOS:
    return property->integer <= 1u;
  case FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM:
  case FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS:
  case FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE:
  case FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER:
    return property->integer != 0u;
  default:
    return 1;
  }
}

int flowie_mqtt_property_iterator_next(flowie_mqtt_property_iterator_t *iterator,
                                       flowie_mqtt_property_view_t *out) {
  flowie_mqtt_property_view_t parsed = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  uint32_t identifier;
  size_t id_size;
  int rc;
  if (!iterator || iterator->size < sizeof(*iterator) ||
      iterator->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !out || out->size < sizeof(*out) ||
      !iterator->cursor || !iterator->limit || iterator->cursor > iterator->limit)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (iterator->cursor == iterator->limit) return FLOWIE_MQTT_PARSE_NEED_MORE;
  rc = flowie_mqtt_vbi(iterator->cursor, (size_t)(iterator->limit - iterator->cursor), &identifier,
                       &id_size);
  if (rc != FLOWIE_MQTT_PARSE_OK) return FLOWIE_MQTT_PARSE_MALFORMED;
  iterator->cursor += id_size;
  parsed.identifier = (flowie_mqtt_property_id_t)identifier;
  parsed.type = flowie_mqtt_property_type(identifier);
  if (parsed.type == 0) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  switch (parsed.type) {
  case FLOWIE_MQTT_PROPERTY_BYTE:
    if (iterator->cursor == iterator->limit) return FLOWIE_MQTT_PARSE_MALFORMED;
    parsed.integer = *iterator->cursor++;
    break;
  case FLOWIE_MQTT_PROPERTY_UINT16:
    if ((size_t)(iterator->limit - iterator->cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
    parsed.integer = flowie_mqtt_read_u16(iterator->cursor);
    iterator->cursor += 2u;
    break;
  case FLOWIE_MQTT_PROPERTY_UINT32:
    if ((size_t)(iterator->limit - iterator->cursor) < 4u) return FLOWIE_MQTT_PARSE_MALFORMED;
    parsed.integer = flowie_mqtt_read_u32(iterator->cursor);
    iterator->cursor += 4u;
    break;
  case FLOWIE_MQTT_PROPERTY_VARIABLE_INTEGER: {
    size_t integer_size;
    rc = flowie_mqtt_vbi(iterator->cursor, (size_t)(iterator->limit - iterator->cursor),
                         &parsed.integer, &integer_size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return FLOWIE_MQTT_PARSE_MALFORMED;
    iterator->cursor += integer_size;
    break;
  }
  case FLOWIE_MQTT_PROPERTY_UTF8:
    rc = flowie_mqtt_length_span(&iterator->cursor, iterator->limit, &parsed.value, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    break;
  case FLOWIE_MQTT_PROPERTY_BINARY:
    rc = flowie_mqtt_length_span(&iterator->cursor, iterator->limit, &parsed.value, 0);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    break;
  case FLOWIE_MQTT_PROPERTY_UTF8_PAIR:
    rc = flowie_mqtt_length_span(&iterator->cursor, iterator->limit, &parsed.value, 1);
    if (rc == FLOWIE_MQTT_PARSE_OK)
      rc = flowie_mqtt_length_span(&iterator->cursor, iterator->limit, &parsed.pair_value, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    break;
  default:
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  }
  if (parsed.identifier == FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC &&
      !flowie_mqtt_topic_name_validate(parsed.value))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (!flowie_mqtt_property_scalar_valid(&parsed)) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

static int flowie_mqtt_properties_validate(const flowie_mqtt_property_block_view_t *block,
                                           uint64_t allowed, uint64_t repeatable,
                                           uint64_t *present_out) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  uint64_t present = 0u;
  int rc = flowie_mqtt_property_iterator_init(block, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    uint64_t bit = FLOWIE_MQTT_PROPERTY_BIT(property.identifier);
    if ((allowed & bit) == 0u || ((present & bit) != 0u && (repeatable & bit) == 0u))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    present |= bit;
    property = (flowie_mqtt_property_view_t)FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  }
  if (rc != FLOWIE_MQTT_PARSE_NEED_MORE) return rc;
  if (present_out) *present_out = present;
  return FLOWIE_MQTT_PARSE_OK;
}

static void flowie_mqtt_empty_properties(const uint8_t *at,
                                         flowie_mqtt_property_block_view_t *out) {
  *out = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  out->encoded.data = at;
  out->values.data = at;
}

static int flowie_mqtt_typed_property_block(flowie_mqtt_span_t bytes,
                                            flowie_mqtt_property_block_view_t *out,
                                            size_t *consumed) {
  int rc = flowie_mqtt_property_block_parse(bytes, out, consumed);
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? FLOWIE_MQTT_PARSE_MALFORMED : rc;
}

static int flowie_mqtt_typed_args(const flowie_mqtt_packet_view_t *packet, size_t output_size,
                                  size_t expected_size, flowie_mqtt_packet_type_t type) {
  if (!packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || output_size < expected_size ||
      packet->type != type || !packet->body.data || packet->body.size != packet->remaining_length)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_connect_parse(const flowie_mqtt_packet_view_t *packet,
                              flowie_mqtt_connect_view_t *out) {
  flowie_mqtt_connect_view_t parsed = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  const uint8_t *cursor;
  const uint8_t *limit;
  flowie_mqtt_span_t protocol_name;
  uint64_t present = 0u;
  uint8_t flags;
  uint8_t will_flag;
  uint8_t username_flag;
  uint8_t password_flag;
  size_t property_size;
  int rc;
  if (!out) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  parsed.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  parsed.will_properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  rc = flowie_mqtt_typed_args(packet, out->size, sizeof(*out), FLOWIE_MQTT_PACKET_CONNECT);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  cursor = packet->body.data;
  limit = cursor + packet->body.size;
  rc = flowie_mqtt_length_span(&cursor, limit, &protocol_name, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if ((size_t)(limit - cursor) < 4u) return FLOWIE_MQTT_PARSE_MALFORMED;
  parsed.version = (flowie_mqtt_version_t)*cursor++;
  if (!flowie_mqtt_version_is_supported(parsed.version) ||
      (packet->version != FLOWIE_MQTT_VERSION_UNSPECIFIED && packet->version != parsed.version))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if ((parsed.version == FLOWIE_MQTT_VERSION_3_1 &&
       (protocol_name.size != 6u || memcmp(protocol_name.data, "MQIsdp", 6u) != 0)) ||
      (parsed.version != FLOWIE_MQTT_VERSION_3_1 &&
       (protocol_name.size != 4u || memcmp(protocol_name.data, "MQTT", 4u) != 0)))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  flags = *cursor++;
  parsed.keep_alive = flowie_mqtt_read_u16(cursor);
  cursor += 2u;
  will_flag = (uint8_t)((flags >> 2u) & 1u);
  parsed.clean_start = (uint8_t)((flags >> 1u) & 1u);
  parsed.will_qos = (uint8_t)((flags >> 3u) & 3u);
  parsed.will_retain = (uint8_t)((flags >> 5u) & 1u);
  password_flag = (uint8_t)((flags >> 6u) & 1u);
  username_flag = (uint8_t)((flags >> 7u) & 1u);
  if ((flags & 1u) != 0u || parsed.will_qos == 3u ||
      (!will_flag && (parsed.will_qos != 0u || parsed.will_retain)) ||
      (password_flag && !username_flag))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (parsed.version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_typed_property_block((flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)},
                                          &parsed.properties, &property_size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    cursor += property_size;
    rc = flowie_mqtt_properties_validate(&parsed.properties, flowie_mqtt_connect_properties_allowed,
                                         flowie_mqtt_user_property_repeatable, &present);
    if (rc != FLOWIE_MQTT_PARSE_OK ||
        ((present & FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA)) != 0u &&
         (present & FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD)) == 0u))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  } else {
    flowie_mqtt_empty_properties(cursor, &parsed.properties);
  }
  rc = flowie_mqtt_length_span(&cursor, limit, &parsed.client_id, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (parsed.version == FLOWIE_MQTT_VERSION_3_1 &&
      (parsed.client_id.size == 0u || parsed.client_id.size > 23u))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (will_flag) {
    if (parsed.version == FLOWIE_MQTT_VERSION_5) {
      rc = flowie_mqtt_typed_property_block((flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)},
                                            &parsed.will_properties, &property_size);
      if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
      cursor += property_size;
      rc = flowie_mqtt_properties_validate(&parsed.will_properties,
                                           flowie_mqtt_will_properties_allowed,
                                           flowie_mqtt_user_property_repeatable, NULL);
      if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    } else {
      flowie_mqtt_empty_properties(cursor, &parsed.will_properties);
    }
    rc = flowie_mqtt_length_span(&cursor, limit, &parsed.will_topic, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK || !flowie_mqtt_topic_name_validate(parsed.will_topic))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    rc = flowie_mqtt_length_span(&cursor, limit, &parsed.will_payload, 0);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else {
    flowie_mqtt_empty_properties(cursor, &parsed.will_properties);
  }
  if (username_flag) {
    rc = flowie_mqtt_length_span(&cursor, limit, &parsed.username, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  if (password_flag) {
    rc = flowie_mqtt_length_span(&cursor, limit, &parsed.password, 0);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  if (cursor != limit) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_publish_parse(const flowie_mqtt_packet_view_t *packet,
                              flowie_mqtt_publish_view_t *out) {
  flowie_mqtt_publish_view_t parsed = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  const uint8_t *cursor;
  const uint8_t *limit;
  uint64_t present = 0u;
  size_t property_size;
  int rc;
  if (!out) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  parsed.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  rc = flowie_mqtt_typed_args(packet, out->size, sizeof(*out), FLOWIE_MQTT_PACKET_PUBLISH);
  if (rc != FLOWIE_MQTT_PARSE_OK || !flowie_mqtt_version_is_supported(packet->version))
    return rc != FLOWIE_MQTT_PARSE_OK ? rc : FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  cursor = packet->body.data;
  limit = cursor + packet->body.size;
  parsed.qos = (uint8_t)((packet->flags >> 1u) & 3u);
  parsed.retain = (uint8_t)(packet->flags & 1u);
  parsed.duplicate = (uint8_t)((packet->flags >> 3u) & 1u);
  rc = flowie_mqtt_length_span(&cursor, limit, &parsed.topic, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (parsed.qos != 0u) {
    if ((size_t)(limit - cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
    parsed.packet_id = flowie_mqtt_read_u16(cursor);
    cursor += 2u;
    if (parsed.packet_id == 0u) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  }
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_typed_property_block((flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)},
                                          &parsed.properties, &property_size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    cursor += property_size;
    rc = flowie_mqtt_properties_validate(&parsed.properties, flowie_mqtt_publish_properties_allowed,
                                         flowie_mqtt_publish_properties_repeatable, &present);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else {
    flowie_mqtt_empty_properties(cursor, &parsed.properties);
  }
  if (parsed.topic.size == 0u) {
    if (packet->version != FLOWIE_MQTT_VERSION_5 ||
        (present & FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS)) == 0u)
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  } else if (!flowie_mqtt_topic_name_validate(parsed.topic)) {
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  }
  parsed.payload.data = cursor;
  parsed.payload.size = (size_t)(limit - cursor);
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_subscription_iterator_init(const flowie_mqtt_packet_view_t *packet,
                                           const flowie_mqtt_subscribe_view_t *subscribe,
                                           flowie_mqtt_subscription_iterator_t *iterator) {
  if (!packet || packet->size < sizeof(*packet) || !subscribe ||
      subscribe->size < sizeof(*subscribe) || !iterator || iterator->size < sizeof(*iterator) ||
      iterator->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !flowie_mqtt_version_is_supported(packet->version) ||
      !subscribe->entries.data)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  iterator->version = packet->version;
  iterator->cursor = subscribe->entries.data;
  iterator->limit = subscribe->entries.data + subscribe->entries.size;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_subscription_iterator_next(flowie_mqtt_subscription_iterator_t *iterator,
                                           flowie_mqtt_subscription_view_t *out) {
  flowie_mqtt_subscription_view_t parsed;
  uint8_t options;
  int rc;
  if (!iterator || iterator->size < sizeof(*iterator) ||
      iterator->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !out || !iterator->cursor ||
      !iterator->limit || iterator->cursor > iterator->limit)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (iterator->cursor == iterator->limit) return FLOWIE_MQTT_PARSE_NEED_MORE;
  memset(&parsed, 0, sizeof(parsed));
  rc = flowie_mqtt_length_span(&iterator->cursor, iterator->limit, &parsed.filter, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK || !flowie_mqtt_topic_filter_validate(parsed.filter))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (iterator->cursor == iterator->limit) return FLOWIE_MQTT_PARSE_MALFORMED;
  options = *iterator->cursor++;
  parsed.qos = (uint8_t)(options & 3u);
  parsed.no_local = (uint8_t)((options >> 2u) & 1u);
  parsed.retain_as_published = (uint8_t)((options >> 3u) & 1u);
  parsed.retain_handling = (uint8_t)((options >> 4u) & 3u);
  if (parsed.qos == 3u || (options & 0xc0u) != 0u || parsed.retain_handling == 3u ||
      (flowie_mqtt_version_is_3x(iterator->version) && (options & 0xfcu) != 0u) ||
      (parsed.no_local && parsed.filter.size >= 7u &&
       memcmp(parsed.filter.data, "$share/", 7u) == 0))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

static int flowie_mqtt_control_type(flowie_mqtt_packet_type_t type) {
  return type == FLOWIE_MQTT_PACKET_CONNACK || type == FLOWIE_MQTT_PACKET_PUBACK ||
         type == FLOWIE_MQTT_PACKET_PUBREC || type == FLOWIE_MQTT_PACKET_PUBREL ||
         type == FLOWIE_MQTT_PACKET_PUBCOMP || type == FLOWIE_MQTT_PACKET_SUBACK ||
         type == FLOWIE_MQTT_PACKET_UNSUBACK || type == FLOWIE_MQTT_PACKET_PINGRESP ||
         type == FLOWIE_MQTT_PACKET_DISCONNECT || type == FLOWIE_MQTT_PACKET_AUTH;
}

static int flowie_mqtt_reason_code_valid(flowie_mqtt_packet_type_t type,
                                         flowie_mqtt_version_t version, uint8_t reason) {
  static const uint8_t connack_v5[] = {0x00u, 0x80u, 0x81u, 0x82u, 0x83u, 0x84u, 0x85u, 0x86u,
                                       0x87u, 0x88u, 0x89u, 0x8au, 0x8cu, 0x90u, 0x95u, 0x97u,
                                       0x99u, 0x9au, 0x9bu, 0x9cu, 0x9du, 0x9fu};
  static const uint8_t publish_v5[] = {0x00u, 0x10u, 0x80u, 0x83u, 0x87u,
                                       0x90u, 0x91u, 0x97u, 0x99u};
  static const uint8_t release_v5[] = {0x00u, 0x92u};
  static const uint8_t suback_v5[] = {0x00u, 0x01u, 0x02u, 0x80u, 0x83u, 0x87u,
                                      0x8fu, 0x91u, 0x97u, 0x9eu, 0xa1u, 0xa2u};
  static const uint8_t unsuback_v5[] = {0x00u, 0x11u, 0x80u, 0x83u, 0x87u, 0x8fu, 0x91u};
  static const uint8_t disconnect_v5[] = {0x00u, 0x04u, 0x80u, 0x81u, 0x82u, 0x83u, 0x87u, 0x89u,
                                          0x8bu, 0x8cu, 0x8du, 0x8eu, 0x8fu, 0x90u, 0x93u, 0x94u,
                                          0x95u, 0x96u, 0x97u, 0x98u, 0x99u, 0x9au, 0x9bu, 0x9cu,
                                          0x9du, 0x9eu, 0x9fu, 0xa0u, 0xa1u, 0xa2u};
  static const uint8_t auth_v5[] = {0x00u, 0x18u, 0x19u};
  const uint8_t *allowed = NULL;
  size_t count = 0u;
  if (flowie_mqtt_version_is_3x(version)) {
    if (type == FLOWIE_MQTT_PACKET_CONNACK) return reason <= 5u;
    if (type == FLOWIE_MQTT_PACKET_SUBACK)
      return reason <= 2u ||
             (version == FLOWIE_MQTT_VERSION_3_1_1 && reason == UINT8_C(0x80));
    if (type == FLOWIE_MQTT_PACKET_AUTH) return 0;
    return reason == 0u;
  }
  switch (type) {
  case FLOWIE_MQTT_PACKET_CONNACK:
    allowed = connack_v5;
    count = sizeof(connack_v5);
    break;
  case FLOWIE_MQTT_PACKET_PUBACK:
  case FLOWIE_MQTT_PACKET_PUBREC:
    allowed = publish_v5;
    count = sizeof(publish_v5);
    break;
  case FLOWIE_MQTT_PACKET_PUBREL:
  case FLOWIE_MQTT_PACKET_PUBCOMP:
    allowed = release_v5;
    count = sizeof(release_v5);
    break;
  case FLOWIE_MQTT_PACKET_SUBACK:
    allowed = suback_v5;
    count = sizeof(suback_v5);
    break;
  case FLOWIE_MQTT_PACKET_UNSUBACK:
    allowed = unsuback_v5;
    count = sizeof(unsuback_v5);
    break;
  case FLOWIE_MQTT_PACKET_DISCONNECT:
    allowed = disconnect_v5;
    count = sizeof(disconnect_v5);
    break;
  case FLOWIE_MQTT_PACKET_AUTH:
    allowed = auth_v5;
    count = sizeof(auth_v5);
    break;
  default:
    return reason == 0u;
  }
  for (size_t i = 0u; i < count; ++i)
    if (allowed[i] == reason) return 1;
  return 0;
}

static size_t flowie_mqtt_vbi_size(uint32_t value) {
  size_t size = 1u;
  while (value >= 128u) {
    value /= 128u;
    ++size;
  }
  return size;
}

static size_t flowie_mqtt_vbi_write(uint8_t *output, uint32_t value) {
  size_t offset = 0u;
  do {
    uint8_t byte = (uint8_t)(value % 128u);
    value /= 128u;
    if (value != 0u) byte |= 0x80u;
    output[offset++] = byte;
  } while (value != 0u);
  return offset;
}

static int flowie_mqtt_version_valid(flowie_mqtt_version_t version) {
  return flowie_mqtt_version_is_supported(version);
}

static int flowie_mqtt_span_input_valid(flowie_mqtt_span_t value) {
  return value.data || value.size == 0u;
}

static int flowie_mqtt_length_span_input_validate(flowie_mqtt_span_t value, int utf8) {
  if (!flowie_mqtt_span_input_valid(value)) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (value.size > FLOWIE_MQTT_MAX_UTF8_SIZE) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  if (utf8 && !flowie_mqtt_utf8_validate(value)) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  return FLOWIE_MQTT_PARSE_OK;
}

static int flowie_mqtt_property_values_validate(flowie_mqtt_version_t version,
                                                flowie_mqtt_span_t values, uint64_t allowed,
                                                uint64_t repeatable, uint64_t *present_out) {
  flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  int rc;
  if (!flowie_mqtt_span_input_valid(values)) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (values.size > FLOWIE_MQTT_MAX_REMAINING_LENGTH) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  if (flowie_mqtt_version_is_3x(version))
    return values.size == 0u ? FLOWIE_MQTT_PARSE_OK : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (values.size == 0u) {
    if (present_out) *present_out = 0u;
    return FLOWIE_MQTT_PARSE_OK;
  }
  block.values = values;
  rc = flowie_mqtt_properties_validate(&block, allowed, repeatable, present_out);
  return rc == FLOWIE_MQTT_PARSE_OK ? FLOWIE_MQTT_PARSE_OK : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
}

static int flowie_mqtt_remaining_add(size_t *remaining, size_t amount) {
  if (!remaining) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (*remaining > FLOWIE_MQTT_MAX_REMAINING_LENGTH ||
      amount > (size_t)FLOWIE_MQTT_MAX_REMAINING_LENGTH - *remaining)
    return FLOWIE_MQTT_PARSE_TOO_LARGE;
  *remaining += amount;
  return FLOWIE_MQTT_PARSE_OK;
}

static int flowie_mqtt_encode_begin(flowie_mqtt_packet_type_t type, uint8_t flags, size_t remaining,
                                    uint8_t *output, size_t output_capacity, size_t *offset_out,
                                    size_t *total_out) {
  size_t header_size;
  size_t total;
  if (!output || !offset_out || !total_out) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (remaining > FLOWIE_MQTT_MAX_REMAINING_LENGTH) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  header_size = 1u + flowie_mqtt_vbi_size((uint32_t)remaining);
  total = header_size + remaining;
  if (total > output_capacity) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  output[0] = (uint8_t)(((uint8_t)type << 4u) | flags);
  *offset_out = 1u + flowie_mqtt_vbi_write(output + 1u, (uint32_t)remaining);
  *total_out = total;
  return FLOWIE_MQTT_PARSE_OK;
}

static size_t flowie_mqtt_length_span_write(uint8_t *output, flowie_mqtt_span_t value) {
  output[0] = (uint8_t)(value.size >> 8u);
  output[1] = (uint8_t)value.size;
  if (value.size != 0u) memcpy(output + 2u, value.data, value.size);
  return 2u + value.size;
}

static size_t flowie_mqtt_property_values_write(uint8_t *output, flowie_mqtt_span_t values) {
  size_t offset = flowie_mqtt_vbi_write(output, (uint32_t)values.size);
  if (values.size != 0u) memcpy(output + offset, values.data, values.size);
  return offset + values.size;
}

int flowie_mqtt_connect_packet_encode(const flowie_mqtt_connect_packet_t *packet, uint8_t *output,
                                      size_t output_capacity, size_t *written) {
  static const uint8_t protocol_name_v3[] = {'M', 'Q', 'I', 's', 'd', 'p'};
  static const uint8_t protocol_name_v4_v5[] = {'M', 'Q', 'T', 'T'};
  flowie_mqtt_span_t protocol_name;
  uint64_t present = 0u;
  size_t remaining;
  size_t offset;
  size_t total;
  uint8_t flags;
  int rc;
  if (written) *written = 0u;
  if (!output || !written || !packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !flowie_mqtt_version_valid(packet->version))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (packet->clean_start > 1u || packet->has_will > 1u || packet->will_qos > 2u ||
      packet->will_retain > 1u || packet->has_username > 1u || packet->has_password > 1u ||
      (packet->has_password && !packet->has_username) ||
      (!packet->has_will &&
       (packet->will_qos || packet->will_retain || packet->will_properties.size ||
        packet->will_topic.size || packet->will_payload.size)) ||
      (!packet->has_username && packet->username.size) ||
      (!packet->has_password && packet->password.size))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  rc = flowie_mqtt_length_span_input_validate(packet->client_id, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (packet->version == FLOWIE_MQTT_VERSION_3_1 &&
      (packet->client_id.size == 0u || packet->client_id.size > 23u))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (packet->version == FLOWIE_MQTT_VERSION_3_1_1 && packet->client_id.size == 0u &&
      !packet->clean_start)
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  protocol_name = packet->version == FLOWIE_MQTT_VERSION_3_1
                      ? (flowie_mqtt_span_t){protocol_name_v3, sizeof(protocol_name_v3)}
                      : (flowie_mqtt_span_t){protocol_name_v4_v5,
                                             sizeof(protocol_name_v4_v5)};
  remaining = 6u + protocol_name.size;
  rc = flowie_mqtt_property_values_validate(packet->version, packet->properties,
                                            flowie_mqtt_connect_properties_allowed,
                                            flowie_mqtt_user_property_repeatable, &present);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if ((present & FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA)) != 0u &&
      (present & FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD)) == 0u)
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (packet->has_will) {
    rc = flowie_mqtt_property_values_validate(packet->version, packet->will_properties,
                                              flowie_mqtt_will_properties_allowed,
                                              flowie_mqtt_user_property_repeatable, NULL);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    rc = flowie_mqtt_length_span_input_validate(packet->will_topic, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    if (!flowie_mqtt_topic_name_validate(packet->will_topic))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    rc = flowie_mqtt_length_span_input_validate(packet->will_payload, 0);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else if (!flowie_mqtt_span_input_valid(packet->will_properties) ||
             !flowie_mqtt_span_input_valid(packet->will_topic) ||
             !flowie_mqtt_span_input_valid(packet->will_payload)) {
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  if (packet->has_username) {
    rc = flowie_mqtt_length_span_input_validate(packet->username, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else if (!flowie_mqtt_span_input_valid(packet->username)) {
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  if (packet->has_password) {
    rc = flowie_mqtt_length_span_input_validate(packet->password, 0);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else if (!flowie_mqtt_span_input_valid(packet->password)) {
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_remaining_add(&remaining,
                                   flowie_mqtt_vbi_size((uint32_t)packet->properties.size) +
                                       packet->properties.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  rc = flowie_mqtt_remaining_add(&remaining, 2u + packet->client_id.size);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (packet->has_will) {
    if (packet->version == FLOWIE_MQTT_VERSION_5) {
      rc = flowie_mqtt_remaining_add(&remaining,
                                     flowie_mqtt_vbi_size((uint32_t)packet->will_properties.size) +
                                         packet->will_properties.size);
      if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    }
    rc = flowie_mqtt_remaining_add(&remaining,
                                   4u + packet->will_topic.size + packet->will_payload.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  if (packet->has_username) {
    rc = flowie_mqtt_remaining_add(&remaining, 2u + packet->username.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  if (packet->has_password) {
    rc = flowie_mqtt_remaining_add(&remaining, 2u + packet->password.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  rc = flowie_mqtt_encode_begin(FLOWIE_MQTT_PACKET_CONNECT, 0u, remaining, output, output_capacity,
                                &offset, &total);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  offset += flowie_mqtt_length_span_write(output + offset, protocol_name);
  output[offset++] = (uint8_t)packet->version;
  flags = (uint8_t)((packet->has_username << 7u) | (packet->has_password << 6u) |
                    (packet->will_retain << 5u) | (packet->will_qos << 3u) |
                    (packet->has_will << 2u) | (packet->clean_start << 1u));
  output[offset++] = flags;
  output[offset++] = (uint8_t)(packet->keep_alive >> 8u);
  output[offset++] = (uint8_t)packet->keep_alive;
  if (packet->version == FLOWIE_MQTT_VERSION_5)
    offset += flowie_mqtt_property_values_write(output + offset, packet->properties);
  offset += flowie_mqtt_length_span_write(output + offset, packet->client_id);
  if (packet->has_will) {
    if (packet->version == FLOWIE_MQTT_VERSION_5)
      offset += flowie_mqtt_property_values_write(output + offset, packet->will_properties);
    offset += flowie_mqtt_length_span_write(output + offset, packet->will_topic);
    offset += flowie_mqtt_length_span_write(output + offset, packet->will_payload);
  }
  if (packet->has_username)
    offset += flowie_mqtt_length_span_write(output + offset, packet->username);
  if (packet->has_password)
    offset += flowie_mqtt_length_span_write(output + offset, packet->password);
  if (offset != total) return FLOWIE_MQTT_PARSE_MALFORMED;
  *written = total;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_publish_packet_encode(const flowie_mqtt_publish_packet_t *packet, uint8_t *output,
                                      size_t output_capacity, size_t *written) {
  uint64_t present = 0u;
  size_t remaining = 0u;
  size_t offset;
  size_t total;
  uint8_t flags;
  int rc;
  if (written) *written = 0u;
  if (!output || !written || !packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !flowie_mqtt_version_valid(packet->version))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (packet->qos > 2u || packet->retain > 1u || packet->duplicate > 1u ||
      (packet->qos == 0u && (packet->packet_id != 0u || packet->duplicate)) ||
      (packet->qos != 0u && packet->packet_id == 0u))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  rc = flowie_mqtt_length_span_input_validate(packet->topic, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  rc = flowie_mqtt_property_values_validate(packet->version, packet->properties,
                                            flowie_mqtt_publish_properties_allowed,
                                            flowie_mqtt_publish_properties_repeatable, &present);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (packet->topic.size == 0u) {
    if (packet->version != FLOWIE_MQTT_VERSION_5 ||
        (present & FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS)) == 0u)
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  } else if (!flowie_mqtt_topic_name_validate(packet->topic)) {
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  }
  if (!flowie_mqtt_span_input_valid(packet->payload)) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  rc = flowie_mqtt_remaining_add(&remaining, 2u + packet->topic.size);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (packet->qos != 0u) {
    rc = flowie_mqtt_remaining_add(&remaining, 2u);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_remaining_add(&remaining,
                                   flowie_mqtt_vbi_size((uint32_t)packet->properties.size) +
                                       packet->properties.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  rc = flowie_mqtt_remaining_add(&remaining, packet->payload.size);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  flags = (uint8_t)((packet->duplicate << 3u) | (packet->qos << 1u) | packet->retain);
  rc = flowie_mqtt_encode_begin(FLOWIE_MQTT_PACKET_PUBLISH, flags, remaining, output,
                                output_capacity, &offset, &total);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  offset += flowie_mqtt_length_span_write(output + offset, packet->topic);
  if (packet->qos != 0u) {
    output[offset++] = (uint8_t)(packet->packet_id >> 8u);
    output[offset++] = (uint8_t)packet->packet_id;
  }
  if (packet->version == FLOWIE_MQTT_VERSION_5)
    offset += flowie_mqtt_property_values_write(output + offset, packet->properties);
  if (packet->payload.size != 0u) {
    memcpy(output + offset, packet->payload.data, packet->payload.size);
    offset += packet->payload.size;
  }
  if (offset != total) return FLOWIE_MQTT_PARSE_MALFORMED;
  *written = total;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_subscribe_packet_encode(const flowie_mqtt_subscribe_packet_t *packet,
                                        uint8_t *output, size_t output_capacity, size_t *written) {
  size_t remaining = 2u;
  size_t offset;
  size_t total;
  int rc;
  if (written) *written = 0u;
  if (!output || !written || !packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !flowie_mqtt_version_valid(packet->version) ||
      (!packet->subscriptions && packet->subscription_count != 0u))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (packet->packet_id == 0u || packet->subscription_count == 0u)
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  rc = flowie_mqtt_property_values_validate(packet->version, packet->properties,
                                            flowie_mqtt_subscribe_properties_allowed,
                                            flowie_mqtt_user_property_repeatable, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_remaining_add(&remaining,
                                   flowie_mqtt_vbi_size((uint32_t)packet->properties.size) +
                                       packet->properties.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  for (size_t i = 0u; i < packet->subscription_count; ++i) {
    const flowie_mqtt_subscription_t *entry = &packet->subscriptions[i];
    rc = flowie_mqtt_length_span_input_validate(entry->filter, 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    if (!flowie_mqtt_topic_filter_validate(entry->filter) || entry->qos > 2u ||
        entry->no_local > 1u || entry->retain_as_published > 1u || entry->retain_handling > 2u ||
        (flowie_mqtt_version_is_3x(packet->version) &&
         (entry->no_local || entry->retain_as_published || entry->retain_handling)) ||
        (entry->no_local && entry->filter.size >= 7u &&
         memcmp(entry->filter.data, "$share/", 7u) == 0))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    rc = flowie_mqtt_remaining_add(&remaining, 3u + entry->filter.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  rc = flowie_mqtt_encode_begin(FLOWIE_MQTT_PACKET_SUBSCRIBE, 0x02u, remaining, output,
                                output_capacity, &offset, &total);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  output[offset++] = (uint8_t)(packet->packet_id >> 8u);
  output[offset++] = (uint8_t)packet->packet_id;
  if (packet->version == FLOWIE_MQTT_VERSION_5)
    offset += flowie_mqtt_property_values_write(output + offset, packet->properties);
  for (size_t i = 0u; i < packet->subscription_count; ++i) {
    const flowie_mqtt_subscription_t *entry = &packet->subscriptions[i];
    offset += flowie_mqtt_length_span_write(output + offset, entry->filter);
    output[offset++] =
        (uint8_t)(entry->qos | (entry->no_local << 2u) | (entry->retain_as_published << 3u) |
                  (entry->retain_handling << 4u));
  }
  if (offset != total) return FLOWIE_MQTT_PARSE_MALFORMED;
  *written = total;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_unsubscribe_packet_encode(const flowie_mqtt_unsubscribe_packet_t *packet,
                                          uint8_t *output, size_t output_capacity,
                                          size_t *written) {
  size_t remaining = 2u;
  size_t offset;
  size_t total;
  int rc;
  if (written) *written = 0u;
  if (!output || !written || !packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !flowie_mqtt_version_valid(packet->version) ||
      (!packet->filters && packet->filter_count != 0u))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (packet->packet_id == 0u || packet->filter_count == 0u)
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  rc = flowie_mqtt_property_values_validate(packet->version, packet->properties,
                                            flowie_mqtt_user_property_repeatable,
                                            flowie_mqtt_user_property_repeatable, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_remaining_add(&remaining,
                                   flowie_mqtt_vbi_size((uint32_t)packet->properties.size) +
                                       packet->properties.size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  for (size_t i = 0u; i < packet->filter_count; ++i) {
    rc = flowie_mqtt_length_span_input_validate(packet->filters[i], 1);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    if (!flowie_mqtt_topic_filter_validate(packet->filters[i]))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    rc = flowie_mqtt_remaining_add(&remaining, 2u + packet->filters[i].size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  }
  rc = flowie_mqtt_encode_begin(FLOWIE_MQTT_PACKET_UNSUBSCRIBE, 0x02u, remaining, output,
                                output_capacity, &offset, &total);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  output[offset++] = (uint8_t)(packet->packet_id >> 8u);
  output[offset++] = (uint8_t)packet->packet_id;
  if (packet->version == FLOWIE_MQTT_VERSION_5)
    offset += flowie_mqtt_property_values_write(output + offset, packet->properties);
  for (size_t i = 0u; i < packet->filter_count; ++i)
    offset += flowie_mqtt_length_span_write(output + offset, packet->filters[i]);
  if (offset != total) return FLOWIE_MQTT_PARSE_MALFORMED;
  *written = total;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_pingreq_encode(flowie_mqtt_version_t version, uint8_t *output,
                               size_t output_capacity, size_t *written) {
  if (written) *written = 0u;
  if (!output || !written || !flowie_mqtt_version_valid(version))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (output_capacity < 2u) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  output[0] = 0xc0u;
  output[1] = 0x00u;
  *written = 2u;
  return FLOWIE_MQTT_PARSE_OK;
}

static int flowie_mqtt_control_properties_validate(flowie_mqtt_packet_type_t type,
                                                   const flowie_mqtt_property_block_view_t *block) {
  uint64_t allowed;
  if (!block || block->size < sizeof(*block) || block->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (block->values.size == 0u) return FLOWIE_MQTT_PARSE_OK;
  switch (type) {
  case FLOWIE_MQTT_PACKET_CONNACK:
    allowed = FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_MAXIMUM_QOS) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_RETAIN_AVAILABLE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS_MAXIMUM) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_REASON_STRING) |
              flowie_mqtt_user_property_repeatable |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER_AVAILABLE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SERVER_KEEP_ALIVE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_RESPONSE_INFORMATION) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SERVER_REFERENCE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA);
    break;
  case FLOWIE_MQTT_PACKET_PUBACK:
  case FLOWIE_MQTT_PACKET_PUBREC:
  case FLOWIE_MQTT_PACKET_PUBREL:
  case FLOWIE_MQTT_PACKET_PUBCOMP:
  case FLOWIE_MQTT_PACKET_SUBACK:
  case FLOWIE_MQTT_PACKET_UNSUBACK:
    allowed = FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_REASON_STRING) |
              flowie_mqtt_user_property_repeatable;
    break;
  case FLOWIE_MQTT_PACKET_DISCONNECT:
    allowed = FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_SERVER_REFERENCE) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_REASON_STRING) |
              flowie_mqtt_user_property_repeatable;
    break;
  case FLOWIE_MQTT_PACKET_AUTH:
    allowed = FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA) |
              FLOWIE_MQTT_PROPERTY_BIT(FLOWIE_MQTT_PROPERTY_REASON_STRING) |
              flowie_mqtt_user_property_repeatable;
    break;
  default:
    allowed = 0u;
    break;
  }
  return flowie_mqtt_properties_validate(block, allowed, flowie_mqtt_user_property_repeatable,
                                         NULL);
}

static int flowie_mqtt_control_validate(const flowie_mqtt_control_packet_t *packet,
                                        uint32_t *remaining_out) {
  flowie_mqtt_property_block_view_t properties = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  size_t remaining = 0u;
  if (!packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !remaining_out ||
      !flowie_mqtt_control_type(packet->type) ||
      !flowie_mqtt_version_is_supported(packet->version) ||
      (!packet->properties.data && packet->properties.size != 0u) ||
      (!packet->reason_codes.data && packet->reason_codes.size != 0u))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (packet->properties.size > FLOWIE_MQTT_MAX_REMAINING_LENGTH ||
      packet->reason_codes.size > FLOWIE_MQTT_MAX_REMAINING_LENGTH)
    return FLOWIE_MQTT_PARSE_TOO_LARGE;
  properties.values = packet->properties;
  if (packet->version == FLOWIE_MQTT_VERSION_5 &&
      flowie_mqtt_control_properties_validate(packet->type, &properties) != FLOWIE_MQTT_PARSE_OK)
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (packet->type == FLOWIE_MQTT_PACKET_PINGRESP) {
    if (packet->session_present || packet->packet_id || packet->reason_code ||
        packet->properties.size || packet->reason_codes.size)
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  } else if (packet->type == FLOWIE_MQTT_PACKET_DISCONNECT ||
             packet->type == FLOWIE_MQTT_PACKET_AUTH) {
    if (packet->session_present || packet->packet_id || packet->reason_codes.size ||
        !flowie_mqtt_reason_code_valid(packet->type, packet->version, packet->reason_code))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    if (flowie_mqtt_version_is_3x(packet->version)) {
      if (packet->type != FLOWIE_MQTT_PACKET_DISCONNECT || packet->reason_code ||
          packet->properties.size)
        return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
      remaining = 0u;
    } else if (packet->reason_code == 0u && packet->properties.size == 0u) {
      remaining = 0u;
    } else if (packet->properties.size == 0u) {
      remaining = 1u;
    } else {
      remaining =
          1u + flowie_mqtt_vbi_size((uint32_t)packet->properties.size) + packet->properties.size;
    }
  } else if (packet->type == FLOWIE_MQTT_PACKET_CONNACK) {
    if (packet->packet_id || packet->reason_codes.size || packet->session_present > 1u ||
        (packet->session_present && packet->reason_code != 0u) ||
        !flowie_mqtt_reason_code_valid(packet->type, packet->version, packet->reason_code))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    if (packet->version == FLOWIE_MQTT_VERSION_3_1 && packet->session_present)
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    remaining =
        packet->version == FLOWIE_MQTT_VERSION_5
            ? 2u + flowie_mqtt_vbi_size((uint32_t)packet->properties.size) + packet->properties.size
            : 2u;
  } else if (packet->type == FLOWIE_MQTT_PACKET_SUBACK ||
             packet->type == FLOWIE_MQTT_PACKET_UNSUBACK) {
    if (packet->session_present || packet->packet_id == 0u || packet->reason_code ||
        (packet->type == FLOWIE_MQTT_PACKET_SUBACK && packet->reason_codes.size == 0u) ||
        (packet->type == FLOWIE_MQTT_PACKET_UNSUBACK && packet->version == FLOWIE_MQTT_VERSION_5 &&
         packet->reason_codes.size == 0u))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    if (flowie_mqtt_version_is_3x(packet->version)) {
      if (packet->properties.size != 0u ||
          (packet->type == FLOWIE_MQTT_PACKET_UNSUBACK && packet->reason_codes.size != 0u))
        return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    }
    for (size_t i = 0u; i < packet->reason_codes.size; ++i)
      if (!flowie_mqtt_reason_code_valid(packet->type, packet->version,
                                         packet->reason_codes.data[i]))
        return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    remaining = 2u + packet->reason_codes.size;
    if (packet->version == FLOWIE_MQTT_VERSION_5) {
      remaining +=
          flowie_mqtt_vbi_size((uint32_t)packet->properties.size) + packet->properties.size;
    }
  } else {
    if (packet->session_present || packet->packet_id == 0u || packet->reason_codes.size ||
        !flowie_mqtt_reason_code_valid(packet->type, packet->version, packet->reason_code))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    if (flowie_mqtt_version_is_3x(packet->version)) {
      if (packet->properties.size != 0u || packet->reason_code != 0u)
        return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
      remaining = 2u;
    } else if (packet->reason_code == 0u && packet->properties.size == 0u) {
      remaining = 2u;
    } else {
      remaining =
          3u + flowie_mqtt_vbi_size((uint32_t)packet->properties.size) + packet->properties.size;
    }
  }
  if (remaining > FLOWIE_MQTT_MAX_REMAINING_LENGTH) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  *remaining_out = (uint32_t)remaining;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_control_packet_encode(const flowie_mqtt_control_packet_t *packet, uint8_t *output,
                                      size_t output_capacity, size_t *written) {
  uint32_t remaining;
  size_t header_size;
  size_t total;
  size_t offset;
  int rc;
  if (written) *written = 0u;
  if (!output || !written) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  rc = flowie_mqtt_control_validate(packet, &remaining);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  header_size = 1u + flowie_mqtt_vbi_size(remaining);
  if ((size_t)remaining > SIZE_MAX - header_size) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  total = header_size + (size_t)remaining;
  if (total > output_capacity) return FLOWIE_MQTT_PARSE_TOO_LARGE;
  output[0] = (uint8_t)((uint8_t)packet->type << 4u);
  if (packet->type == FLOWIE_MQTT_PACKET_PUBREL) output[0] |= 0x02u;
  offset = 1u + flowie_mqtt_vbi_write(output + 1u, remaining);
  if (packet->type == FLOWIE_MQTT_PACKET_PINGRESP || remaining == 0u) {
    *written = offset;
    return FLOWIE_MQTT_PARSE_OK;
  }
  if (packet->type == FLOWIE_MQTT_PACKET_DISCONNECT || packet->type == FLOWIE_MQTT_PACKET_AUTH) {
    output[offset++] = packet->reason_code;
  } else if (packet->type == FLOWIE_MQTT_PACKET_CONNACK) {
    output[offset++] = packet->session_present;
    output[offset++] = packet->reason_code;
  } else {
    output[offset++] = (uint8_t)(packet->packet_id >> 8u);
    output[offset++] = (uint8_t)packet->packet_id;
    if (packet->type != FLOWIE_MQTT_PACKET_SUBACK && packet->type != FLOWIE_MQTT_PACKET_UNSUBACK &&
        remaining > 2u) {
      output[offset++] = packet->reason_code;
    }
  }
  if (packet->version == FLOWIE_MQTT_VERSION_5 &&
      (packet->type == FLOWIE_MQTT_PACKET_CONNACK || packet->type == FLOWIE_MQTT_PACKET_SUBACK ||
       packet->type == FLOWIE_MQTT_PACKET_UNSUBACK ||
       ((packet->type == FLOWIE_MQTT_PACKET_DISCONNECT ||
         packet->type == FLOWIE_MQTT_PACKET_AUTH) &&
        packet->properties.size != 0u) ||
       (packet->type != FLOWIE_MQTT_PACKET_DISCONNECT && packet->type != FLOWIE_MQTT_PACKET_AUTH &&
        remaining > 2u))) {
    offset += flowie_mqtt_vbi_write(output + offset, (uint32_t)packet->properties.size);
    if (packet->properties.size != 0u) {
      memcpy(output + offset, packet->properties.data, packet->properties.size);
      offset += packet->properties.size;
    }
  }
  if (packet->type == FLOWIE_MQTT_PACKET_SUBACK || packet->type == FLOWIE_MQTT_PACKET_UNSUBACK) {
    memcpy(output + offset, packet->reason_codes.data, packet->reason_codes.size);
    offset += packet->reason_codes.size;
  }
  if (offset != total) return FLOWIE_MQTT_PARSE_MALFORMED;
  *written = total;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_control_packet_parse(const flowie_mqtt_packet_view_t *packet,
                                     flowie_mqtt_control_packet_view_t *out) {
  flowie_mqtt_control_packet_view_t parsed = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  const uint8_t *cursor;
  const uint8_t *limit;
  size_t consumed = 0u;
  int rc;
  if (!packet || packet->size < sizeof(*packet) ||
      packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !flowie_mqtt_control_type(packet->type) ||
      !flowie_mqtt_version_is_supported(packet->version) ||
      !packet->body.data || packet->body.size != packet->remaining_length)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  parsed.version = packet->version;
  parsed.type = packet->type;
  flowie_mqtt_empty_properties(packet->body.data, &parsed.properties);
  cursor = packet->body.data;
  limit = cursor + packet->body.size;
  if (packet->type == FLOWIE_MQTT_PACKET_PINGRESP) {
    if (cursor != limit) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    *out = parsed;
    return FLOWIE_MQTT_PARSE_OK;
  }
  if (packet->type == FLOWIE_MQTT_PACKET_DISCONNECT || packet->type == FLOWIE_MQTT_PACKET_AUTH) {
    if (flowie_mqtt_version_is_3x(packet->version)) {
      if (packet->type != FLOWIE_MQTT_PACKET_DISCONNECT || cursor != limit)
        return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
      *out = parsed;
      return FLOWIE_MQTT_PARSE_OK;
    }
    if (cursor != limit) parsed.reason_code = *cursor++;
  } else if (packet->type == FLOWIE_MQTT_PACKET_CONNACK) {
    if ((size_t)(limit - cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
    if ((cursor[0] & 0xfeu) != 0u) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    parsed.session_present = cursor[0];
    parsed.reason_code = cursor[1];
    cursor += 2u;
    if ((packet->version == FLOWIE_MQTT_VERSION_3_1 && parsed.session_present) ||
        (parsed.session_present && parsed.reason_code != 0u))
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  } else {
    if ((size_t)(limit - cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
    parsed.packet_id = flowie_mqtt_read_u16(cursor);
    cursor += 2u;
    if (parsed.packet_id == 0u) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    if (packet->type != FLOWIE_MQTT_PACKET_SUBACK && packet->type != FLOWIE_MQTT_PACKET_UNSUBACK &&
        cursor != limit) {
      if (packet->version != FLOWIE_MQTT_VERSION_5) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
      parsed.reason_code = *cursor++;
    }
  }
  if (!flowie_mqtt_reason_code_valid(packet->type, packet->version, parsed.reason_code))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    if (packet->type == FLOWIE_MQTT_PACKET_CONNACK || packet->type == FLOWIE_MQTT_PACKET_SUBACK ||
        packet->type == FLOWIE_MQTT_PACKET_UNSUBACK ||
        ((packet->type == FLOWIE_MQTT_PACKET_DISCONNECT ||
          packet->type == FLOWIE_MQTT_PACKET_AUTH) &&
         cursor != limit) ||
        (packet->type != FLOWIE_MQTT_PACKET_DISCONNECT && packet->type != FLOWIE_MQTT_PACKET_AUTH &&
         cursor != limit)) {
      rc = flowie_mqtt_property_block_parse((flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)},
                                            &parsed.properties, &consumed);
      if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
      cursor += consumed;
      rc = flowie_mqtt_control_properties_validate(packet->type, &parsed.properties);
      if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    }
  } else if (packet->type != FLOWIE_MQTT_PACKET_SUBACK && cursor != limit) {
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  }
  if (packet->type == FLOWIE_MQTT_PACKET_SUBACK || packet->type == FLOWIE_MQTT_PACKET_UNSUBACK) {
    parsed.reason_codes = (flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)};
    if (packet->type == FLOWIE_MQTT_PACKET_SUBACK && parsed.reason_codes.size == 0u)
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    if (packet->type == FLOWIE_MQTT_PACKET_UNSUBACK && packet->version == FLOWIE_MQTT_VERSION_5 &&
        parsed.reason_codes.size == 0u)
      return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    for (size_t i = 0u; i < parsed.reason_codes.size; ++i)
      if (!flowie_mqtt_reason_code_valid(packet->type, packet->version,
                                         parsed.reason_codes.data[i]))
        return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    cursor = limit;
  }
  if (cursor != limit) return FLOWIE_MQTT_PARSE_MALFORMED;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_subscribe_parse(const flowie_mqtt_packet_view_t *packet,
                                flowie_mqtt_subscribe_view_t *out) {
  flowie_mqtt_subscribe_view_t parsed = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
  flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
  flowie_mqtt_subscription_view_t entry;
  const uint8_t *cursor;
  const uint8_t *limit;
  size_t property_size;
  int rc;
  if (!out) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  parsed.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  rc = flowie_mqtt_typed_args(packet, out->size, sizeof(*out), FLOWIE_MQTT_PACKET_SUBSCRIBE);
  if (rc != FLOWIE_MQTT_PARSE_OK ||
      !flowie_mqtt_version_is_supported(packet->version))
    return rc != FLOWIE_MQTT_PARSE_OK ? rc : FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  cursor = packet->body.data;
  limit = cursor + packet->body.size;
  if ((size_t)(limit - cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
  parsed.packet_id = flowie_mqtt_read_u16(cursor);
  cursor += 2u;
  if (parsed.packet_id == 0u) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_typed_property_block((flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)},
                                          &parsed.properties, &property_size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    cursor += property_size;
    rc = flowie_mqtt_properties_validate(&parsed.properties,
                                         flowie_mqtt_subscribe_properties_allowed,
                                         flowie_mqtt_user_property_repeatable, NULL);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else {
    flowie_mqtt_empty_properties(cursor, &parsed.properties);
  }
  if (cursor == limit) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  parsed.entries.data = cursor;
  parsed.entries.size = (size_t)(limit - cursor);
  rc = flowie_mqtt_subscription_iterator_init(packet, &parsed, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  while ((rc = flowie_mqtt_subscription_iterator_next(&iterator, &entry)) == FLOWIE_MQTT_PARSE_OK)
    ++parsed.entry_count;
  if (rc != FLOWIE_MQTT_PARSE_NEED_MORE || parsed.entry_count == 0u) return rc;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_topic_filter_iterator_init(const flowie_mqtt_unsubscribe_view_t *unsubscribe,
                                           flowie_mqtt_topic_filter_iterator_t *iterator) {
  if (!unsubscribe || unsubscribe->size < sizeof(*unsubscribe) ||
      unsubscribe->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !iterator ||
      iterator->size < sizeof(*iterator) || iterator->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      !unsubscribe->filters.data || unsubscribe->filters.size == 0u)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  iterator->cursor = unsubscribe->filters.data;
  iterator->limit = unsubscribe->filters.data + unsubscribe->filters.size;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_topic_filter_iterator_next(flowie_mqtt_topic_filter_iterator_t *iterator,
                                           flowie_mqtt_span_t *out) {
  flowie_mqtt_span_t parsed = {0};
  int rc;
  if (!iterator || iterator->size < sizeof(*iterator) ||
      iterator->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !out || !iterator->cursor ||
      !iterator->limit || iterator->cursor > iterator->limit)
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (iterator->cursor == iterator->limit) return FLOWIE_MQTT_PARSE_NEED_MORE;
  rc = flowie_mqtt_length_span(&iterator->cursor, iterator->limit, &parsed, 1);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (!flowie_mqtt_topic_filter_validate(parsed)) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}

int flowie_mqtt_unsubscribe_parse(const flowie_mqtt_packet_view_t *packet,
                                  flowie_mqtt_unsubscribe_view_t *out) {
  flowie_mqtt_unsubscribe_view_t parsed = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
  flowie_mqtt_topic_filter_iterator_t iterator = FLOWIE_MQTT_TOPIC_FILTER_ITERATOR_INIT;
  flowie_mqtt_span_t filter;
  const uint8_t *cursor;
  const uint8_t *limit;
  size_t property_size;
  int rc;
  if (!out) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  parsed.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  rc = flowie_mqtt_typed_args(packet, out->size, sizeof(*out), FLOWIE_MQTT_PACKET_UNSUBSCRIBE);
  if (rc != FLOWIE_MQTT_PARSE_OK ||
      !flowie_mqtt_version_is_supported(packet->version))
    return rc != FLOWIE_MQTT_PARSE_OK ? rc : FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  cursor = packet->body.data;
  limit = cursor + packet->body.size;
  if ((size_t)(limit - cursor) < 2u) return FLOWIE_MQTT_PARSE_MALFORMED;
  parsed.packet_id = flowie_mqtt_read_u16(cursor);
  cursor += 2u;
  if (parsed.packet_id == 0u) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (packet->version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_typed_property_block((flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)},
                                          &parsed.properties, &property_size);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
    cursor += property_size;
    rc = flowie_mqtt_properties_validate(&parsed.properties, flowie_mqtt_user_property_repeatable,
                                         flowie_mqtt_user_property_repeatable, NULL);
    if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  } else {
    flowie_mqtt_empty_properties(cursor, &parsed.properties);
  }
  if (cursor == limit) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  parsed.filters = (flowie_mqtt_span_t){cursor, (size_t)(limit - cursor)};
  rc = flowie_mqtt_topic_filter_iterator_init(&parsed, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  while ((rc = flowie_mqtt_topic_filter_iterator_next(&iterator, &filter)) == FLOWIE_MQTT_PARSE_OK)
    ++parsed.filter_count;
  if (rc != FLOWIE_MQTT_PARSE_NEED_MORE || parsed.filter_count == 0u)
    return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? FLOWIE_MQTT_PARSE_PROTOCOL_ERROR : rc;
  *out = parsed;
  return FLOWIE_MQTT_PARSE_OK;
}
