#include "flowie_rule_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <limits.h>
#include <string.h>

#define FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_MASK UINT32_C(0x0f)
#define FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_INVALID_MASK UINT8_C(0xf0)
#define FLOWIE_MQTT_MESSAGE_VERSION_SHIFT 8u
#define FLOWIE_MQTT_MESSAGE_VERSION_MASK UINT32_C(0xff00)
#define FLOWIE_MQTT_RULE_FACT_COUNT 8u

enum flowie_mqtt_rule_fact_id_e {
  FLOWIE_MQTT_RULE_TOPIC = 1,
  FLOWIE_MQTT_RULE_PAYLOAD,
  FLOWIE_MQTT_RULE_PAYLOAD_SIZE,
  FLOWIE_MQTT_RULE_QOS,
  FLOWIE_MQTT_RULE_RETAIN,
  FLOWIE_MQTT_RULE_DUPLICATE,
  FLOWIE_MQTT_RULE_PACKET_ID,
  FLOWIE_MQTT_RULE_VERSION
};

static const turbo_flow_expr_schema_field_t FLOWIE_MQTT_RULE_FIELDS[] = {
    {"mqtt.topic", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_TOPIC},
    {"mqtt.payload", TURBO_FLOW_EXPR_TYPE_STRING, FLOWIE_MQTT_RULE_PAYLOAD},
    {"mqtt.payload_size", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_PAYLOAD_SIZE},
    {"mqtt.qos", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_QOS},
    {"mqtt.retain", TURBO_FLOW_EXPR_TYPE_BOOL, FLOWIE_MQTT_RULE_RETAIN},
    {"mqtt.duplicate", TURBO_FLOW_EXPR_TYPE_BOOL, FLOWIE_MQTT_RULE_DUPLICATE},
    {"mqtt.packet_id", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_PACKET_ID},
    {"mqtt.version", TURBO_FLOW_EXPR_TYPE_I64, FLOWIE_MQTT_RULE_VERSION}};

static const turbo_flow_expr_schema_t FLOWIE_MQTT_RULE_SCHEMA = {
    FLOWIE_MQTT_RULE_FIELDS,
    sizeof(FLOWIE_MQTT_RULE_FIELDS) / sizeof(FLOWIE_MQTT_RULE_FIELDS[0])};

int flowie_mqtt_message_flags_encode(flowie_mqtt_version_t version, uint8_t fixed_flags,
                                     uint32_t *flags_out) {
  if (!flags_out || (version != FLOWIE_MQTT_VERSION_3_1_1 && version != FLOWIE_MQTT_VERSION_5) ||
      (fixed_flags & FLOWIE_MQTT_MESSAGE_FIXED_FLAGS_INVALID_MASK) != 0u)
    return TURBO_EINVAL;
  *flags_out = ((uint32_t)version << FLOWIE_MQTT_MESSAGE_VERSION_SHIFT) | fixed_flags;
  return TURBO_OK;
}

const turbo_flow_expr_schema_t *flowie_mqtt_rule_schema(void) {
  return &FLOWIE_MQTT_RULE_SCHEMA;
}

int flowie_mqtt_message_flags_version(uint32_t flags, flowie_mqtt_version_t *version_out) {
  uint32_t encoded;
  if (!version_out) return TURBO_EINVAL;
  encoded = (flags & FLOWIE_MQTT_MESSAGE_VERSION_MASK) >> FLOWIE_MQTT_MESSAGE_VERSION_SHIFT;
  if (encoded != FLOWIE_MQTT_VERSION_3_1_1 && encoded != FLOWIE_MQTT_VERSION_5)
    return TURBO_EPROTO;
  *version_out = (flowie_mqtt_version_t)encoded;
  return TURBO_OK;
}

static int flowie_mqtt_message_version(const turbo_flow_msg_t *message,
                                       flowie_mqtt_version_t *version_out) {
  return message ? flowie_mqtt_message_flags_version(message->flags, version_out) : TURBO_EINVAL;
}

static int flowie_mqtt_rule_value(const turbo_flow_expr_schema_field_t *field,
                                  const flowie_mqtt_publish_view_t *publish,
                                  flowie_mqtt_version_t version,
                                  turbo_flow_expr_value_t *value) {
  if (!field || !field->path || !publish || !value) return TURBO_EINVAL;
  memset(value, 0, sizeof(*value));
  if (strcmp(field->path, "mqtt.topic") == 0 &&
      field->type == TURBO_FLOW_EXPR_TYPE_STRING) {
    value->type = TURBO_FLOW_EXPR_TYPE_STRING;
    value->as.string = tstr_v_from_buf((const char *)publish->topic.data, publish->topic.size);
  } else if (strcmp(field->path, "mqtt.payload") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_STRING) {
    value->type = TURBO_FLOW_EXPR_TYPE_STRING;
    value->as.string =
        tstr_v_from_buf((const char *)publish->payload.data, publish->payload.size);
  } else if (strcmp(field->path, "mqtt.payload_size") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_I64) {
    if (publish->payload.size > INT64_MAX) return TURBO_ERANGE;
    value->type = TURBO_FLOW_EXPR_TYPE_I64;
    value->as.i64 = (int64_t)publish->payload.size;
  } else if (strcmp(field->path, "mqtt.qos") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_I64) {
    value->type = TURBO_FLOW_EXPR_TYPE_I64;
    value->as.i64 = publish->qos;
  } else if (strcmp(field->path, "mqtt.retain") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_BOOL) {
    value->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    value->as.boolean = publish->retain != 0u;
  } else if (strcmp(field->path, "mqtt.duplicate") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_BOOL) {
    value->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    value->as.boolean = publish->duplicate != 0u;
  } else if (strcmp(field->path, "mqtt.packet_id") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_I64) {
    value->type = TURBO_FLOW_EXPR_TYPE_I64;
    value->as.i64 = publish->packet_id;
  } else if (strcmp(field->path, "mqtt.version") == 0 &&
             field->type == TURBO_FLOW_EXPR_TYPE_I64) {
    value->type = TURBO_FLOW_EXPR_TYPE_I64;
    value->as.i64 = version;
  } else {
    return TURBO_ENOENT;
  }
  return TURBO_OK;
}

int flowie_mqtt_rule_facts_provider(const turbo_flow_msg_t *message,
                                    const turbo_flow_expr_schema_t *schema,
                                    const turbo_flow_expr_value_t **values_out,
                                    size_t *value_count_out, void *ctx) {
  static TURBO_THREAD_LOCAL turbo_flow_expr_value_t values[FLOWIE_MQTT_RULE_FACT_COUNT];
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_mqtt_version_t version;
  size_t consumed = 0u;
  int rc;
  (void)ctx;
  if (!message || !schema || !values_out || !value_count_out ||
      schema->field_count > FLOWIE_MQTT_RULE_FACT_COUNT ||
      (schema->field_count != 0u && !schema->fields) || message->payload.len == 0u ||
      !message->payload.data)
    return TURBO_EINVAL;
  *values_out = NULL;
  *value_count_out = 0u;
  rc = flowie_mqtt_message_version(message, &version);
  if (rc != TURBO_OK) return rc;
  options.version = version;
  options.max_packet_size = message->payload.len;
  rc = flowie_mqtt_packet_parse((const uint8_t *)message->payload.data, message->payload.len,
                                &options, &packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != message->payload.len ||
      packet.type != FLOWIE_MQTT_PACKET_PUBLISH ||
      flowie_mqtt_publish_parse(&packet, &publish) != FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  for (size_t i = 0u; i < schema->field_count; ++i) {
    rc = flowie_mqtt_rule_value(&schema->fields[i], &publish, version, &values[i]);
    if (rc != TURBO_OK) return rc;
  }
  *values_out = values;
  *value_count_out = schema->field_count;
  return TURBO_OK;
}
