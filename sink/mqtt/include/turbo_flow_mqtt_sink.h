#ifndef TURBO_FLOW_MQTT_SINK_H
#define TURBO_FLOW_MQTT_SINK_H

#include "turbo_flow_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_MQTT_SINK_ABI_VERSION 1u
#define TURBO_FLOW_MQTT_SINK_ROUTE_PREFIX_MAX 31u
#define TURBO_FLOW_MQTT_SINK_TENANT_MAX 63u
#define TURBO_FLOW_MQTT_SINK_DEFAULT_MAX_BATCH_SIZE 256u
#define TURBO_FLOW_MQTT_SINK_MAX_BATCH_SIZE 4096u

/** Immutable MQTT routing policy borrowed for one synchronous batch map call. */
typedef struct turbo_flow_mqtt_sink_config_s {
  size_t size;
  uint32_t abi_version;
  const char *route_prefix;
  const char *tenant;
  uint8_t qos;
  uint8_t retain;
  /** Hard per-call bound; zero selects TURBO_FLOW_MQTT_SINK_DEFAULT_MAX_BATCH_SIZE. */
  size_t max_batch_size;
} turbo_flow_mqtt_sink_config_t;

#define TURBO_FLOW_MQTT_SINK_CONFIG_V1_SIZE offsetof(turbo_flow_mqtt_sink_config_t, max_batch_size)

#define TURBO_FLOW_MQTT_SINK_CONFIG_INIT                                                           \
  {sizeof(turbo_flow_mqtt_sink_config_t),                                                          \
   TURBO_FLOW_MQTT_SINK_ABI_VERSION,                                                               \
   "ingress",                                                                                      \
   "default",                                                                                      \
   1u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_MQTT_SINK_DEFAULT_MAX_BATCH_SIZE}

/**
 * MQTT publication derived from one neutral protocol message.
 *
 * topic is caller-owned. payload is borrowed from the input message and remains
 * valid only while that input payload remains valid.
 */
typedef struct turbo_flow_mqtt_sink_message_s {
  size_t size;
  uint32_t abi_version;
  char *topic;
  size_t topic_capacity;
  size_t topic_size;
  const uint8_t *payload;
  size_t payload_size;
  uint8_t qos;
  uint8_t retain;
} turbo_flow_mqtt_sink_message_t;

#define TURBO_FLOW_MQTT_SINK_MESSAGE_INIT                                                          \
  {sizeof(turbo_flow_mqtt_sink_message_t),                                                         \
   TURBO_FLOW_MQTT_SINK_ABI_VERSION,                                                               \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/**
 * Caller-owned bounded batch of normalized inputs and MQTT publication outputs.
 *
 * inputs and outputs each contain message_count contiguous elements. Each
 * output owns its topic buffer. Payloads in mapped outputs borrow the matching
 * input payload and are invalidated with it. The sink never retains either
 * array and never owns an MQTT/database connection.
 */
typedef struct turbo_flow_mqtt_sink_batch_s {
  size_t size;
  uint32_t abi_version;
  const turbo_flow_protocol_message_output_t *inputs;
  turbo_flow_mqtt_sink_message_t *outputs;
  size_t message_count;
} turbo_flow_mqtt_sink_batch_t;

#define TURBO_FLOW_MQTT_SINK_BATCH_INIT                                                            \
  {sizeof(turbo_flow_mqtt_sink_batch_t), TURBO_FLOW_MQTT_SINK_ABI_VERSION, NULL, NULL, 0u}

/**
 * Map one bounded batch without performing I/O or retaining caller storage.
 *
 * Processing is synchronous and ordered. mapped is zeroed before validation
 * and receives the successful prefix length. On a per-message mapping failure,
 * outputs before mapped are valid and the failing/later outputs must not be
 * consumed. A caller may pass the mapped publications to an independently
 * owned MQTT writer, database writer, or another batch adapter.
 */
TURBO_FLOW_C_API int turbo_flow_mqtt_sink_map_batch(const turbo_flow_mqtt_sink_config_t *config,
                                             const turbo_flow_mqtt_sink_batch_t *batch,
                                             size_t *mapped);

/**
 * Map a neutral protocol message to an MQTT publication without broker I/O.
 *
 * The topic format is
 * `{route_prefix}/{protocol}/{tenant}/{device}/{up|down}/{operation}`.
 * Returns TURBO_EINVAL for an invalid ABI/argument, TURBO_EPROTO for invalid
 * metadata, or TURBO_EMSGSIZE when the caller-owned topic buffer is too small.
 * This compatibility helper delegates to a one-message batch; new sink paths
 * should use turbo_flow_mqtt_sink_map_batch().
 */
TURBO_FLOW_C_API int turbo_flow_mqtt_sink_map(const turbo_flow_mqtt_sink_config_t *config,
                                       const turbo_flow_protocol_message_output_t *input,
                                       turbo_flow_mqtt_sink_message_t *output);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_MQTT_SINK_H */
