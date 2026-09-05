#include "turbo_flow_mqtt_sink.h"

#include "salts_error.h"

#include <stdint.h>
#include <string.h>

static int flow_mqtt_sink_segment_length(const char *text, size_t maximum, size_t *length) {
  size_t size;
  if (!text || !length) return SALTS_EINVAL;
  for (size = 0u; size <= maximum && text[size] != '\0'; ++size) {
    const unsigned char ch = (unsigned char)text[size];
    if (ch <= 0x20u || ch >= 0x7fu || ch == '/' || ch == '+' || ch == '#') return SALTS_EPROTO;
  }
  if (size == 0u) return SALTS_EPROTO;
  if (size > maximum) return SALTS_EMSGSIZE;
  *length = size;
  return SALTS_OK;
}

static size_t flow_mqtt_sink_max_batch_size(const turbo_flow_mqtt_sink_config_t *config) {
  return config->size >= sizeof(*config) && config->max_batch_size != 0u
             ? config->max_batch_size
             : TURBO_FLOW_MQTT_SINK_DEFAULT_MAX_BATCH_SIZE;
}

static int flow_mqtt_sink_config_check(const turbo_flow_mqtt_sink_config_t *config) {
  size_t max_batch_size;
  if (!config || config->size < TURBO_FLOW_MQTT_SINK_CONFIG_V1_SIZE ||
      config->abi_version != TURBO_FLOW_MQTT_SINK_ABI_VERSION || config->qos > 2u ||
      config->retain > 1u)
    return SALTS_EINVAL;
  max_batch_size = flow_mqtt_sink_max_batch_size(config);
  return max_batch_size <= TURBO_FLOW_MQTT_SINK_MAX_BATCH_SIZE ? SALTS_OK : SALTS_EINVAL;
}

static int flow_mqtt_sink_message_shape_check(const turbo_flow_protocol_message_output_t *input,
                                              const turbo_flow_mqtt_sink_message_t *output) {
  if (!input || input->size < sizeof(*input) ||
      input->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      (!input->payload && input->payload_size != 0u) ||
      input->payload_size > input->payload_capacity ||
      input->metadata.size < sizeof(input->metadata) ||
      input->metadata.abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION || !output ||
      output->size < sizeof(*output) || output->abi_version != TURBO_FLOW_MQTT_SINK_ABI_VERSION ||
      !output->topic || output->topic_capacity == 0u)
    return SALTS_EINVAL;
  return SALTS_OK;
}

static int flow_mqtt_sink_map_one(const turbo_flow_mqtt_sink_config_t *config,
                                  const turbo_flow_protocol_message_output_t *input,
                                  turbo_flow_mqtt_sink_message_t *output) {
  const char *segments[6];
  size_t lengths[6];
  size_t topic_size = 5u;
  char *cursor;
  int rc;

  output->topic_size = 0u;
  output->payload = NULL;
  output->payload_size = 0u;
  segments[0] = config->route_prefix;
  segments[1] = turbo_flow_protocol_kind_name(input->metadata.protocol);
  segments[2] = config->tenant;
  segments[3] = input->metadata.device_id;
  segments[4] = input->metadata.direction == TURBO_FLOW_PROTOCOL_DIRECTION_UP     ? "up"
                : input->metadata.direction == TURBO_FLOW_PROTOCOL_DIRECTION_DOWN ? "down"
                                                                                  : NULL;
  segments[5] = input->metadata.operation;
  if (!segments[1] || !segments[4]) return SALTS_EPROTO;

  rc = flow_mqtt_sink_segment_length(segments[0], TURBO_FLOW_MQTT_SINK_ROUTE_PREFIX_MAX,
                                     &lengths[0]);
  if (rc == SALTS_OK)
    rc = flow_mqtt_sink_segment_length(segments[1], TURBO_FLOW_PROTOCOL_OPERATION_MAX, &lengths[1]);
  if (rc == SALTS_OK)
    rc = flow_mqtt_sink_segment_length(segments[2], TURBO_FLOW_MQTT_SINK_TENANT_MAX, &lengths[2]);
  if (rc == SALTS_OK)
    rc = flow_mqtt_sink_segment_length(segments[3], TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX, &lengths[3]);
  if (rc == SALTS_OK) rc = flow_mqtt_sink_segment_length(segments[4], 4u, &lengths[4]);
  if (rc == SALTS_OK)
    rc = flow_mqtt_sink_segment_length(segments[5], TURBO_FLOW_PROTOCOL_OPERATION_MAX, &lengths[5]);
  if (rc != SALTS_OK) return rc;

  for (size_t i = 0u; i < 6u; ++i) {
    if (lengths[i] > SIZE_MAX - topic_size) return SALTS_ERANGE;
    topic_size += lengths[i];
  }
  if (topic_size >= output->topic_capacity) return SALTS_EMSGSIZE;

  cursor = output->topic;
  for (size_t i = 0u; i < 6u; ++i) {
    memcpy(cursor, segments[i], lengths[i]);
    cursor += lengths[i];
    if (i + 1u < 6u) *cursor++ = '/';
  }
  *cursor = '\0';
  output->topic_size = topic_size;
  output->payload = input->payload;
  output->payload_size = input->payload_size;
  output->qos = config->qos;
  output->retain = config->retain;
  return SALTS_OK;
}

int turbo_flow_mqtt_sink_map_batch(const turbo_flow_mqtt_sink_config_t *config,
                                   const turbo_flow_mqtt_sink_batch_t *batch, size_t *mapped) {
  size_t max_batch_size;
  int rc;

  if (mapped) *mapped = 0u;
  rc = flow_mqtt_sink_config_check(config);
  if (rc != SALTS_OK) return rc;
  if (!batch || batch->size < sizeof(*batch) ||
      batch->abi_version != TURBO_FLOW_MQTT_SINK_ABI_VERSION || !batch->inputs || !batch->outputs ||
      batch->message_count == 0u)
    return SALTS_EINVAL;
  max_batch_size = flow_mqtt_sink_max_batch_size(config);
  if (batch->message_count > max_batch_size) return SALTS_EMSGSIZE;

  for (size_t i = 0u; i < batch->message_count; ++i) {
    rc = flow_mqtt_sink_message_shape_check(&batch->inputs[i], &batch->outputs[i]);
    if (rc != SALTS_OK) return rc;
  }
  for (size_t i = 0u; i < batch->message_count; ++i) {
    rc = flow_mqtt_sink_map_one(config, &batch->inputs[i], &batch->outputs[i]);
    if (rc != SALTS_OK) return rc;
    if (mapped) *mapped = i + 1u;
  }
  return SALTS_OK;
}

int turbo_flow_mqtt_sink_map(const turbo_flow_mqtt_sink_config_t *config,
                             const turbo_flow_protocol_message_output_t *input,
                             turbo_flow_mqtt_sink_message_t *output) {
  turbo_flow_mqtt_sink_batch_t batch = TURBO_FLOW_MQTT_SINK_BATCH_INIT;
  size_t mapped = 0u;
  int rc;

  batch.inputs = input;
  batch.outputs = output;
  batch.message_count = 1u;
  rc = turbo_flow_mqtt_sink_map_batch(config, &batch, &mapped);
  return rc == SALTS_OK && mapped != 1u ? SALTS_EPROTO : rc;
}
