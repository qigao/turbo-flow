#include "flowie_rule_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_MASK UINT32_C(0x0f)
#define FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_INVALID_MASK UINT8_C(0xf0)
#define FLOWIE_MQTT_MESSAGE_VERSION_SHIFT 8u
#define FLOWIE_MQTT_MESSAGE_VERSION_MASK UINT32_C(0xff00)
#define FLOWIE_MQTT_RULE_FACT_COUNT 15u
#define FLOWIE_MQTT_RULE_ALL_FACTS ((UINT64_C(1) << FLOWIE_MQTT_RULE_FACT_COUNT) - UINT64_C(1))
#define FLOWIE_MQTT_PROJECTION_SCHEMA_ID UINT32_C(1)
#define FLOWIE_MQTT_PROJECTION_SCHEMA_VERSION UINT32_C(2)

enum flowie_mqtt_rule_fact_id_e {
  FLOWIE_MQTT_RULE_TOPIC = 1,
  FLOWIE_MQTT_RULE_PAYLOAD,
  FLOWIE_MQTT_RULE_PAYLOAD_SIZE,
  FLOWIE_MQTT_RULE_QOS,
  FLOWIE_MQTT_RULE_RETAIN,
  FLOWIE_MQTT_RULE_DUPLICATE,
  FLOWIE_MQTT_RULE_PACKET_ID,
  FLOWIE_MQTT_RULE_VERSION,
  FLOWIE_MQTT_RULE_PAYLOAD_FORMAT_INDICATOR,
  FLOWIE_MQTT_RULE_MESSAGE_EXPIRY_INTERVAL,
  FLOWIE_MQTT_RULE_CONTENT_TYPE,
  FLOWIE_MQTT_RULE_RESPONSE_TOPIC,
  FLOWIE_MQTT_RULE_CORRELATION_DATA,
  FLOWIE_MQTT_RULE_USER_PROPERTY_COUNT,
  FLOWIE_MQTT_RULE_BROKER_WILL
};

static const turbo_flow_expr_schema_field_t FLOWIE_MQTT_RULE_FIELDS[] = {
    {"mqtt.topic", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_TOPIC},
    {"mqtt.payload", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_PAYLOAD},
    {"mqtt.payload_size", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_PAYLOAD_SIZE},
    {"mqtt.qos", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_QOS},
    {"mqtt.retain", TURBO_FLOW_EXPR_TYPE_BOOL, FLOWIE_MQTT_RULE_RETAIN},
    {"mqtt.duplicate", TURBO_FLOW_EXPR_TYPE_BOOL, FLOWIE_MQTT_RULE_DUPLICATE},
    {"mqtt.packet_id", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_PACKET_ID},
    {"mqtt.version", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_VERSION},
    {"mqtt.payload_format_indicator", TURBO_FLOW_EXPR_TYPE_I64,
     FLOWIE_MQTT_RULE_PAYLOAD_FORMAT_INDICATOR},
    {"mqtt.message_expiry_interval", TURBO_FLOW_EXPR_TYPE_I64,
     FLOWIE_MQTT_RULE_MESSAGE_EXPIRY_INTERVAL},
    {"mqtt.content_type", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_CONTENT_TYPE},
    {"mqtt.response_topic", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_RESPONSE_TOPIC},
    {"mqtt.correlation_data", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_CORRELATION_DATA},
    {"mqtt.user_property_count", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_USER_PROPERTY_COUNT},
    {"mqtt.broker_will", TURBO_FLOW_EXPR_TYPE_BOOL, FLOWIE_MQTT_RULE_BROKER_WILL}};

static const turbo_flow_expr_schema_t FLOWIE_MQTT_RULE_SCHEMA = {
    FLOWIE_MQTT_RULE_FIELDS, sizeof(FLOWIE_MQTT_RULE_FIELDS) / sizeof(FLOWIE_MQTT_RULE_FIELDS[0])};

static const turbo_flow_data_schema_t FLOWIE_MQTT_PROJECTION_SCHEMA = {
    sizeof(turbo_flow_data_schema_t),
    TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_DATA_ENCODING_OPAQUE,
    "flowie.mqtt.publish",
    "flowie_mqtt_publish_view_t",
    "flowie.mqtt.projection",
    FLOWIE_MQTT_PROJECTION_SCHEMA_ID,
    FLOWIE_MQTT_PROJECTION_SCHEMA_VERSION,
    NULL};

typedef struct flowie_mqtt_projection_s {
  flowie_mqtt_packet_view_t packet;
  flowie_mqtt_publish_view_t publish;
  turbo_flow_expr_value_t values[FLOWIE_MQTT_RULE_FACT_COUNT];
  const void *wire_data;
  size_t wire_size;
  uint64_t present_bits;
  uint64_t parsed_bits;
  uint64_t dirty_bits;
  uint64_t generation;
  uint32_t message_type;
  uint32_t message_flags;
} flowie_mqtt_projection_t;

int flowie_mqtt_message_flags_encode(flowie_mqtt_version_t version, uint8_t fixed_flags,
                                     uint32_t *flags_out) {
  if (!flags_out || !flowie_mqtt_version_is_supported(version) ||
      (fixed_flags & FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_INVALID_MASK) != 0u)
    return TURBO_EINVAL;
  *flags_out = ((uint32_t)version << FLOWIE_MQTT_MESSAGE_VERSION_SHIFT) | fixed_flags;
  return TURBO_OK;
}

const turbo_flow_expr_schema_t *flowie_mqtt_rule_schema(void) { return &FLOWIE_MQTT_RULE_SCHEMA; }

int flowie_mqtt_message_flags_version(uint32_t flags, flowie_mqtt_version_t *version_out) {
  uint32_t encoded;
  if (!version_out) return TURBO_EINVAL;
  encoded = (flags & FLOWIE_MQTT_MESSAGE_VERSION_MASK) >> FLOWIE_MQTT_MESSAGE_VERSION_SHIFT;
  if (!flowie_mqtt_version_is_supported((flowie_mqtt_version_t)encoded)) return TURBO_EPROTO;
  *version_out = (flowie_mqtt_version_t)encoded;
  return TURBO_OK;
}

static int flowie_mqtt_message_version(const turbo_flow_msg_t *message,
                                       flowie_mqtt_version_t *version_out) {
  return message ? flowie_mqtt_message_flags_version(message->flags, version_out) : TURBO_EINVAL;
}

static uint64_t flowie_mqtt_rule_fact_bit(uint32_t fact_id) {
  return UINT64_C(1) << (fact_id - 1u);
}

static void flowie_mqtt_projection_commit(flowie_mqtt_projection_t *projection, uint32_t fact_id,
                                          const turbo_flow_expr_value_t *value) {
  uint64_t bit = flowie_mqtt_rule_fact_bit(fact_id);
  projection->values[fact_id - 1u] = *value;
  projection->present_bits |= bit;
  projection->parsed_bits |= bit;
}

static int flowie_mqtt_projection_materialize_properties(flowie_mqtt_projection_t *projection,
                                                         flowie_mqtt_version_t version) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  turbo_flow_expr_value_t value;
  size_t user_property_count = 0u;
  int rc;
  if (!projection) return TURBO_EINVAL;
  if (version == FLOWIE_MQTT_VERSION_5) {
    rc = flowie_mqtt_property_iterator_init(&projection->publish.properties, &iterator);
    if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) ==
           FLOWIE_MQTT_PARSE_OK) {
      memset(&value, 0, sizeof(value));
      switch (property.identifier) {
      case FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR:
        value.type = TURBO_FLOW_EXPR_TYPE_I64;
        value.as.i64 = property.integer;
        flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_PAYLOAD_FORMAT_INDICATOR,
                                      &value);
        break;
      case FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL:
        value.type = TURBO_FLOW_EXPR_TYPE_I64;
        value.as.i64 = property.integer;
        flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_MESSAGE_EXPIRY_INTERVAL, &value);
        break;
      case FLOWIE_MQTT_PROPERTY_CONTENT_TYPE:
        value.type = TURBO_FLOW_EXPR_TYPE_STRING;
        value.as.string = tstr_v_from_buf((const char *)property.value.data, property.value.size);
        flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_CONTENT_TYPE, &value);
        break;
      case FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC:
        value.type = TURBO_FLOW_EXPR_TYPE_STRING;
        value.as.string = tstr_v_from_buf((const char *)property.value.data, property.value.size);
        flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_RESPONSE_TOPIC, &value);
        break;
      case FLOWIE_MQTT_PROPERTY_CORRELATION_DATA:
        value.type = TURBO_FLOW_EXPR_TYPE_STRING;
        value.as.string = tstr_v_from_buf((const char *)property.value.data, property.value.size);
        flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_CORRELATION_DATA, &value);
        break;
      case FLOWIE_MQTT_PROPERTY_USER_PROPERTY:
        if (user_property_count == INT64_MAX) return TURBO_ERANGE;
        user_property_count += 1u;
        break;
      default:
        break;
      }
    }
    if (rc != FLOWIE_MQTT_PARSE_NEED_MORE) return TURBO_EPROTO;
  }

  memset(&value, 0, sizeof(value));
  value.type = TURBO_FLOW_EXPR_TYPE_I64;
  value.as.i64 = (int64_t)user_property_count;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_USER_PROPERTY_COUNT, &value);
  projection->parsed_bits |= FLOWIE_MQTT_RULE_ALL_FACTS;
  return TURBO_OK;
}

static int flowie_mqtt_projection_materialize(flowie_mqtt_projection_t *projection,
                                              flowie_mqtt_version_t version,
                                              uint32_t message_flags) {
  const flowie_mqtt_publish_view_t *publish;
  turbo_flow_expr_value_t value;
  if (!projection) return TURBO_EINVAL;
  publish = &projection->publish;
  if (publish->payload.size > INT64_MAX) return TURBO_ERANGE;

  memset(&value, 0, sizeof(value));
  value.type = TURBO_FLOW_EXPR_TYPE_STRING;
  value.as.string = tstr_v_from_buf((const char *)publish->topic.data, publish->topic.size);
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_TOPIC, &value);

  value.as.string = tstr_v_from_buf((const char *)publish->payload.data, publish->payload.size);
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_PAYLOAD, &value);

  memset(&value, 0, sizeof(value));
  value.type = TURBO_FLOW_EXPR_TYPE_I64;
  value.as.i64 = (int64_t)publish->payload.size;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_PAYLOAD_SIZE, &value);

  value.as.i64 = publish->qos;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_QOS, &value);

  memset(&value, 0, sizeof(value));
  value.type = TURBO_FLOW_EXPR_TYPE_BOOL;
  value.as.boolean = publish->retain != 0u;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_RETAIN, &value);

  value.as.boolean = publish->duplicate != 0u;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_DUPLICATE, &value);

  memset(&value, 0, sizeof(value));
  value.type = TURBO_FLOW_EXPR_TYPE_I64;
  value.as.i64 = publish->packet_id;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_PACKET_ID, &value);

  value.as.i64 = version;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_VERSION, &value);

  memset(&value, 0, sizeof(value));
  value.type = TURBO_FLOW_EXPR_TYPE_BOOL;
  value.as.boolean = (message_flags & FLOWIE_MQTT_MESSAGE_BROKER_WILL) != 0u;
  flowie_mqtt_projection_commit(projection, FLOWIE_MQTT_RULE_BROKER_WILL, &value);
  return flowie_mqtt_projection_materialize_properties(projection, version);
}

static int flowie_mqtt_projection_packet(const turbo_flow_msg_t *message,
                                         const flowie_mqtt_packet_view_t *packet_hint,
                                         flowie_mqtt_version_t version,
                                         flowie_mqtt_packet_view_t *packet_out) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  size_t consumed = 0u;
  if (!message || !packet_out) return TURBO_EINVAL;
  if (!packet_hint) {
    options.version = version;
    options.max_packet_size = message->payload.len;
    if (flowie_mqtt_packet_parse((const uint8_t *)message->payload.data, message->payload.len,
                                 &options, packet_out, &consumed, NULL) != FLOWIE_MQTT_PARSE_OK ||
        consumed != message->payload.len)
      return TURBO_EPROTO;
    return TURBO_OK;
  }
  if (packet_hint->size < sizeof(*packet_hint) ||
      packet_hint->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || packet_hint->version != version ||
      packet_hint->type != FLOWIE_MQTT_PACKET_PUBLISH ||
      (message->flags & FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_MASK) != packet_hint->flags ||
      packet_hint->packet.size != message->payload.len || !packet_hint->packet.data ||
      !packet_hint->body.data)
    return TURBO_EINVAL;
  {
    uintptr_t packet_address = (uintptr_t)packet_hint->packet.data;
    uintptr_t body_address = (uintptr_t)packet_hint->body.data;
    size_t body_offset;
    if (body_address < packet_address) return TURBO_EINVAL;
    body_offset = (size_t)(body_address - packet_address);
    if (body_offset > packet_hint->packet.size ||
        packet_hint->body.size > packet_hint->packet.size - body_offset)
      return TURBO_EINVAL;
    *packet_out = *packet_hint;
    packet_out->packet.data = (const uint8_t *)message->payload.data;
    packet_out->body.data = packet_out->packet.data + body_offset;
  }
  return TURBO_OK;
}

static int flowie_mqtt_projection_decode(const turbo_flow_msg_t *message,
                                         const flowie_mqtt_packet_view_t *packet_hint,
                                         flowie_mqtt_projection_t *projection) {
  flowie_mqtt_version_t version;
  int rc;
  if (!message || !projection ||
      (message->type != 0u && message->type != FLOWIE_MQTT_PACKET_PUBLISH) ||
      message->payload.len == 0u || !message->payload.data)
    return TURBO_EINVAL;
  memset(projection, 0, sizeof(*projection));
  projection->packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  projection->publish = (flowie_mqtt_publish_view_t)FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  rc = flowie_mqtt_message_version(message, &version);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_projection_packet(message, packet_hint, version, &projection->packet);
  if (rc != TURBO_OK) return rc;
  if (projection->packet.type != FLOWIE_MQTT_PACKET_PUBLISH ||
      flowie_mqtt_publish_parse(&projection->packet, &projection->publish) != FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  rc = flowie_mqtt_projection_materialize(projection, version, message->flags);
  if (rc != TURBO_OK) return rc;
  if (projection->parsed_bits != FLOWIE_MQTT_RULE_ALL_FACTS) return TURBO_EPROTO;
  projection->wire_data = message->payload.data;
  projection->wire_size = message->payload.len;
  projection->generation = UINT64_C(1);
  projection->message_type = message->type;
  projection->message_flags = message->flags;
  return TURBO_OK;
}

static int flowie_mqtt_projection_clone(const void *value, void *ctx, void **out) {
  flowie_mqtt_projection_t *copy;
  (void)ctx;
  if (out) *out = NULL;
  if (!value || !out) return TURBO_EINVAL;
  copy = (flowie_mqtt_projection_t *)malloc(sizeof(*copy));
  if (!copy) return TURBO_ENOMEM;
  *copy = *(const flowie_mqtt_projection_t *)value;
  *out = copy;
  return TURBO_OK;
}

static void flowie_mqtt_projection_destroy(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static int flowie_mqtt_projection_matches(const flowie_mqtt_projection_t *projection,
                                          const turbo_flow_msg_t *message) {
  return projection && message && projection->wire_data == message->payload.data &&
         projection->wire_size == message->payload.len &&
         projection->message_type == message->type && projection->message_flags == message->flags &&
         projection->generation != 0u && projection->dirty_bits == 0u;
}

int flowie_mqtt_rule_bind_projection(turbo_flow_msg_t *message,
                                     const flowie_mqtt_packet_view_t *packet_hint) {
  flowie_mqtt_projection_t *projection;
  uintptr_t buffer_address;
  uintptr_t payload_address;
  size_t payload_offset;
  size_t buffer_size;
  int rc;
  if (!message || !message->buffer || !message->payload.data || message->payload.len == 0u)
    return TURBO_EINVAL;
  buffer_address = (uintptr_t)mem_buffer_const_data(message->buffer);
  payload_address = (uintptr_t)message->payload.data;
  buffer_size = mem_buffer_used(message->buffer);
  if (payload_address < buffer_address) return TURBO_EINVAL;
  payload_offset = (size_t)(payload_address - buffer_address);
  if (payload_offset > buffer_size || message->payload.len > buffer_size - payload_offset)
    return TURBO_EINVAL;
  projection = (flowie_mqtt_projection_t *)malloc(sizeof(*projection));
  if (!projection) return TURBO_ENOMEM;
  rc = flowie_mqtt_projection_decode(message, packet_hint, projection);
  if (rc == TURBO_OK)
    rc = turbo_flow_msg_bind_projection(message, &FLOWIE_MQTT_PROJECTION_SCHEMA, projection,
                                        flowie_mqtt_projection_clone,
                                        flowie_mqtt_projection_destroy, NULL);
  if (rc != TURBO_OK) free(projection);
  return rc;
}

static int flowie_mqtt_rule_values(const flowie_mqtt_projection_t *projection,
                                   const turbo_flow_expr_schema_t *schema,
                                   turbo_flow_expr_value_t *values) {
  if (!projection || !schema || !values) return TURBO_EINVAL;
  for (size_t i = 0u; i < schema->field_count; ++i) {
    const turbo_flow_expr_schema_field_t *field = &schema->fields[i];
    const turbo_flow_expr_schema_field_t *expected;
    size_t value_index;
    uint64_t bit;
    if (!field->path) return TURBO_EINVAL;
    if (field->field_id < FLOWIE_MQTT_RULE_TOPIC || field->field_id > FLOWIE_MQTT_RULE_BROKER_WILL)
      return TURBO_ENOENT;
    value_index = field->field_id - FLOWIE_MQTT_RULE_TOPIC;
    expected = &FLOWIE_MQTT_RULE_FIELDS[value_index];
    if (field->type != expected->type) return TURBO_ENOENT;
    bit = flowie_mqtt_rule_fact_bit(field->field_id);
    if ((projection->parsed_bits & bit) == 0u) return TURBO_EPROTO;
    if ((projection->present_bits & bit) != 0u) values[i] = projection->values[value_index];
    else memset(&values[i], 0, sizeof(values[i]));
  }
  return TURBO_OK;
}

int flowie_mqtt_payload_view(const turbo_flow_msg_t *message, tstr_v *payload_out, void *ctx) {
  flowie_mqtt_projection_t decoded;
  const turbo_flow_data_schema_t *projection_schema = NULL;
  const flowie_mqtt_projection_t *projection;
  int rc;

  (void)ctx;
  if (!message || !payload_out) return TURBO_EINVAL;
  *payload_out = (tstr_v){0};
  projection =
      (const flowie_mqtt_projection_t *)turbo_flow_msg_projection(message, &projection_schema);
  if (projection && projection_schema == &FLOWIE_MQTT_PROJECTION_SCHEMA) {
    if (!flowie_mqtt_projection_matches(projection, message)) return TURBO_EPROTO;
  } else {
    rc = flowie_mqtt_projection_decode(message, NULL, &decoded);
    if (rc != TURBO_OK) return rc;
    projection = &decoded;
  }
  *payload_out = tstr_v_from_buf((const char *)projection->publish.payload.data,
                                 projection->publish.payload.size);
  return TURBO_OK;
}

int flowie_mqtt_rule_facts_provider(const turbo_flow_msg_t *message,
                                    const turbo_flow_expr_schema_t *schema,
                                    const turbo_flow_expr_value_t **values_out,
                                    size_t *value_count_out, void *ctx) {
  static TURBO_THREAD_LOCAL turbo_flow_expr_value_t values[FLOWIE_MQTT_RULE_FACT_COUNT];
  flowie_mqtt_projection_t decoded;
  const turbo_flow_data_schema_t *projection_schema = NULL;
  const flowie_mqtt_projection_t *projection;
  int rc;
  (void)ctx;
  if (!message || !schema || !values_out || !value_count_out ||
      schema->field_count > FLOWIE_MQTT_RULE_FACT_COUNT ||
      (schema->field_count != 0u && !schema->fields) || message->payload.len == 0u ||
      !message->payload.data)
    return TURBO_EINVAL;
  *values_out = NULL;
  *value_count_out = 0u;
  projection =
      (const flowie_mqtt_projection_t *)turbo_flow_msg_projection(message, &projection_schema);
  if (projection && projection_schema == &FLOWIE_MQTT_PROJECTION_SCHEMA) {
    if (!flowie_mqtt_projection_matches(projection, message)) return TURBO_EPROTO;
  } else {
    rc = flowie_mqtt_projection_decode(message, NULL, &decoded);
    if (rc != TURBO_OK) return rc;
    projection = &decoded;
  }
  rc = flowie_mqtt_rule_values(projection, schema, values);
  if (rc != TURBO_OK) return rc;
  *values_out = values;
  *value_count_out = schema->field_count;
  return TURBO_OK;
}
