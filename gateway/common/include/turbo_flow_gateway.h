#ifndef TURBO_FLOW_GATEWAY_H
#define TURBO_FLOW_GATEWAY_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_GATEWAY_ABI_VERSION 1u
#define TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MAJOR 1u
#define TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MINOR 1u

#define TURBO_FLOW_GATEWAY_TOPIC_PREFIX_MAX 31u
#define TURBO_FLOW_GATEWAY_TENANT_MAX 63u
#define TURBO_FLOW_GATEWAY_DEVICE_ID_MAX 127u
#define TURBO_FLOW_GATEWAY_OPERATION_MAX 63u
#define TURBO_FLOW_GATEWAY_RESOURCE_MAX 255u
#define TURBO_FLOW_GATEWAY_VERSION_MAX 15u
#define TURBO_FLOW_GATEWAY_CORRELATION_MAX 63u
#define TURBO_FLOW_GATEWAY_DEFAULT_MAX_FRAME_SIZE (1024u * 1024u)

typedef enum turbo_flow_gateway_protocol_e {
  TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN = 1,
  TURBO_FLOW_GATEWAY_PROTOCOL_COAP,
  TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M,
  TURBO_FLOW_GATEWAY_PROTOCOL_OCPP,
  TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960,
  TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808
} turbo_flow_gateway_protocol_t;

typedef enum turbo_flow_gateway_direction_e {
  TURBO_FLOW_GATEWAY_DIRECTION_UP = 1,
  TURBO_FLOW_GATEWAY_DIRECTION_DOWN
} turbo_flow_gateway_direction_t;

typedef uint64_t turbo_flow_gateway_capabilities_t;

#define TURBO_FLOW_GATEWAY_CAP_INGRESS (UINT64_C(1) << 0u)
#define TURBO_FLOW_GATEWAY_CAP_EGRESS (UINT64_C(1) << 1u)
#define TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE (UINT64_C(1) << 2u)
#define TURBO_FLOW_GATEWAY_CAP_PROTOCOL_REPLY (UINT64_C(1) << 3u)
#define TURBO_FLOW_GATEWAY_CAP_COMMAND_ENCODE (UINT64_C(1) << 4u)

typedef struct turbo_flow_gateway_s turbo_flow_gateway_t;
typedef struct turbo_flow_gateway_registry_s turbo_flow_gateway_registry_t;
typedef struct turbo_flow_gateway_owner_s turbo_flow_gateway_owner_t;

/**
 * Immutable gateway configuration borrowed only for the duration of plugin open().
 *
 * topic_prefix and tenant are single MQTT topic segments. protocol_version is
 * optional; each plugin selects its documented default when it is NULL or empty.
 */
typedef struct turbo_flow_gateway_open_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_gateway_protocol_t protocol;
  const char *topic_prefix;
  const char *tenant;
  const char *protocol_version;
  size_t max_frame_size;
} turbo_flow_gateway_open_request_t;

#define TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT                                                    \
  {sizeof(turbo_flow_gateway_open_request_t), TURBO_FLOW_GATEWAY_ABI_VERSION,                   \
   TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN, "gateway", "default", NULL,                            \
   TURBO_FLOW_GATEWAY_DEFAULT_MAX_FRAME_SIZE}

/**
 * Borrowed device-protocol frame.
 *
 * device_id may be omitted only when the protocol frame carries a stable device
 * identity (currently GB/T 32960 and JT/T 808). protocol_version may be omitted
 * to use the version selected when the gateway was opened.
 */
typedef struct turbo_flow_gateway_frame_view_s {
  size_t size;
  uint32_t abi_version;
  const uint8_t *data;
  size_t data_size;
  const char *device_id;
  const char *protocol_version;
} turbo_flow_gateway_frame_view_t;

#define TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT                                                     \
  {sizeof(turbo_flow_gateway_frame_view_t), TURBO_FLOW_GATEWAY_ABI_VERSION, NULL, 0u, NULL,     \
   NULL}

/** Pointer-free metadata copied with a mapped MQTT message. */
typedef struct turbo_flow_gateway_metadata_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_gateway_protocol_t protocol;
  turbo_flow_gateway_direction_t direction;
  uint32_t message_type;
  uint64_t sequence;
  char protocol_version[TURBO_FLOW_GATEWAY_VERSION_MAX + 1u];
  char device_id[TURBO_FLOW_GATEWAY_DEVICE_ID_MAX + 1u];
  char operation[TURBO_FLOW_GATEWAY_OPERATION_MAX + 1u];
  char correlation_id[TURBO_FLOW_GATEWAY_CORRELATION_MAX + 1u];
} turbo_flow_gateway_metadata_t;

#define TURBO_FLOW_GATEWAY_METADATA_INIT                                                       \
  {sizeof(turbo_flow_gateway_metadata_t), TURBO_FLOW_GATEWAY_ABI_VERSION, 0, 0, 0u, 0u,        \
   {0}, {0}, {0}, {0}}

/** Immutable snapshot of one opened provider-neutral gateway. */
typedef struct turbo_flow_gateway_info_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_gateway_protocol_t protocol;
  turbo_flow_gateway_capabilities_t capabilities;
  size_t max_frame_size;
  char gateway[TURBO_FLOW_GATEWAY_OPERATION_MAX + 1u];
  char protocol_version[TURBO_FLOW_GATEWAY_VERSION_MAX + 1u];
} turbo_flow_gateway_info_t;

#define TURBO_FLOW_GATEWAY_INFO_INIT                                                           \
  {sizeof(turbo_flow_gateway_info_t), TURBO_FLOW_GATEWAY_ABI_VERSION, 0, 0u, 0u, {0}, {0}}

/** Caller-owned buffers receiving one protocol-to-MQTT mapping. */
typedef struct turbo_flow_gateway_mqtt_output_s {
  size_t size;
  uint32_t abi_version;
  char *topic;
  size_t topic_capacity;
  size_t topic_size;
  uint8_t *payload;
  size_t payload_capacity;
  size_t payload_size;
  uint8_t qos;
  uint8_t retain;
  turbo_flow_gateway_metadata_t metadata;
} turbo_flow_gateway_mqtt_output_t;

#define TURBO_FLOW_GATEWAY_MQTT_OUTPUT_INIT                                                    \
  {sizeof(turbo_flow_gateway_mqtt_output_t), TURBO_FLOW_GATEWAY_ABI_VERSION, NULL, 0u, 0u,     \
   NULL, 0u, 0u, 0u, 0u, TURBO_FLOW_GATEWAY_METADATA_INIT}

/** Borrowed MQTT message on the downlink mapping boundary. */
typedef struct turbo_flow_gateway_mqtt_view_s {
  size_t size;
  uint32_t abi_version;
  const char *topic;
  size_t topic_size;
  const uint8_t *payload;
  size_t payload_size;
} turbo_flow_gateway_mqtt_view_t;

#define TURBO_FLOW_GATEWAY_MQTT_VIEW_INIT                                                      \
  {sizeof(turbo_flow_gateway_mqtt_view_t), TURBO_FLOW_GATEWAY_ABI_VERSION, NULL, 0u, NULL, 0u}

/** Caller-owned frame buffer receiving one MQTT-to-protocol mapping. */
typedef struct turbo_flow_gateway_frame_output_s {
  size_t size;
  uint32_t abi_version;
  uint8_t *data;
  size_t capacity;
  size_t data_size;
  turbo_flow_gateway_metadata_t metadata;
} turbo_flow_gateway_frame_output_t;

#define TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT                                                   \
  {sizeof(turbo_flow_gateway_frame_output_t), TURBO_FLOW_GATEWAY_ABI_VERSION, NULL, 0u, 0u,    \
   TURBO_FLOW_GATEWAY_METADATA_INIT}

/**
 * Borrowed protocol-neutral downlink command.
 *
 * operation and device_id are required. resource is protocol-specific (for
 * example a CoAP URI path or MQTT-SN topic id) and may be NULL only for
 * operations whose wire format does not use it. correlation_id and sequence
 * are copied into wire correlation fields where the protocol defines them.
 * payload is borrowed for this synchronous call.
 */
typedef struct turbo_flow_gateway_command_view_s {
  size_t size;
  uint32_t abi_version;
  const char *device_id;
  const char *operation;
  const char *resource;
  const char *correlation_id;
  uint64_t sequence;
  const uint8_t *payload;
  size_t payload_size;
} turbo_flow_gateway_command_view_t;

#define TURBO_FLOW_GATEWAY_COMMAND_VIEW_INIT                                                   \
  {sizeof(turbo_flow_gateway_command_view_t), TURBO_FLOW_GATEWAY_ABI_VERSION, NULL, NULL, NULL, \
   NULL, 0u, NULL, 0u}

/**
 * Build the protocol response that becomes legal after Flowie/FlowStore
 * settlement. A successful call may return data_size == 0 when the protocol
 * message does not require a response.
 *
 * request is the complete original ingress frame and remains caller-owned.
 * status is TURBO_OK for successful settlement, otherwise the terminal
 * settlement error that the codec maps to a protocol-defined rejection.
 */
CXX_C_API int turbo_flow_gateway_reply(
    turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_frame_view_t *request, int status,
    turbo_flow_gateway_frame_output_t *output);

/**
 * Encode one protocol-neutral downlink command into a device wire frame.
 *
 * This API is deliberately separate from raw-preserving egress so callers
 * must choose semantic encoding explicitly; malformed raw frames never fall
 * back to a different interpretation.
 */
CXX_C_API int turbo_flow_gateway_encode(
    turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_command_view_t *command,
    turbo_flow_gateway_frame_output_t *output);

/**
 * Decode and validate one device-protocol frame, then map it to MQTT.
 *
 * The MQTT payload is the complete original frame. The topic is:
 *   {prefix}/{protocol}/{tenant}/{device}/up/{operation}
 *
 * @return TURBO_OK, TURBO_EINVAL for an invalid ABI/argument, TURBO_EPROTO
 *         for a malformed frame, TURBO_ENOTSUP for a version mismatch, or
 *         TURBO_EMSGSIZE when a configured or caller-owned limit is exceeded.
 */
CXX_C_API int turbo_flow_gateway_ingress(turbo_flow_gateway_t *gateway,
                                         const turbo_flow_gateway_frame_view_t *frame,
                                         turbo_flow_gateway_mqtt_output_t *output);

/**
 * Validate a downlink MQTT topic and raw-preserved payload, then restore the
 * device-protocol frame byte-for-byte.
 *
 * @return the same bounded error set as turbo_flow_gateway_ingress().
 */
CXX_C_API int turbo_flow_gateway_egress(turbo_flow_gateway_t *gateway,
                                        const turbo_flow_gateway_mqtt_view_t *message,
                                        turbo_flow_gateway_frame_output_t *output);

/** Copy immutable identity, capability, version, and frame-limit metadata. */
CXX_C_API int turbo_flow_gateway_get_info(
    const turbo_flow_gateway_t *gateway, turbo_flow_gateway_info_t *out);

typedef struct turbo_flow_gateway_service_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_gateway_protocol_t protocol;
  /** Provider-neutral instance borrowed until plugin close(). */
  turbo_flow_gateway_t *instance;
  /** Plugin-private lifecycle handle passed back only to close(). */
  void *owner;
} turbo_flow_gateway_service_t;

#define TURBO_FLOW_GATEWAY_SERVICE_INIT                                                        \
  {sizeof(turbo_flow_gateway_service_t), TURBO_FLOW_GATEWAY_ABI_VERSION,                        \
   TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN, NULL, NULL}

typedef int (*turbo_flow_gateway_open_fn)(
    void *ctx, const turbo_flow_gateway_open_request_t *request,
    turbo_flow_gateway_service_t *service);
typedef void (*turbo_flow_gateway_close_fn)(void *ctx,
                                            turbo_flow_gateway_service_t *service);

/**
 * Stable plugin function table. Concrete codec and storage operations are not
 * exported; all data-path calls use the provider-neutral gateway instance.
 */
typedef struct turbo_flow_gateway_plugin_api_s {
  size_t size;
  uint32_t version_major;
  uint32_t version_minor;
  const char *gateway;
  turbo_flow_gateway_protocol_t protocol;
  turbo_flow_gateway_capabilities_t capabilities;
  void *ctx;
  turbo_flow_gateway_open_fn open;
  turbo_flow_gateway_close_fn close;
} turbo_flow_gateway_plugin_api_t;

#define TURBO_FLOW_GATEWAY_PLUGIN_EXPORT_SYMBOL "turbo_flow_gateway_plugin_get_api"

typedef const turbo_flow_gateway_plugin_api_t *
(*turbo_flow_gateway_plugin_get_api_fn)(void);

CXX_C_API const turbo_flow_gateway_plugin_api_t *
turbo_flow_gateway_plugin_get_api(void);

/**
 * Create a caller-serialized, bounded gateway registry.
 *
 * Direct registrations are borrowed. Dynamically loaded modules are owned by
 * the registry and remain loaded while an owner exists.
 */
CXX_C_API int turbo_flow_gateway_registry_create(
    size_t capacity, turbo_flow_gateway_registry_t **out);
CXX_C_API int
turbo_flow_gateway_registry_destroy(turbo_flow_gateway_registry_t *registry);
CXX_C_API int turbo_flow_gateway_registry_register(
    turbo_flow_gateway_registry_t *registry,
    const turbo_flow_gateway_plugin_api_t *api);
CXX_C_API int turbo_flow_gateway_registry_load(
    turbo_flow_gateway_registry_t *registry, const char *path, char *reason,
    size_t reason_size);
CXX_C_API const turbo_flow_gateway_plugin_api_t *
turbo_flow_gateway_registry_find(const turbo_flow_gateway_registry_t *registry,
                                 const char *gateway);

CXX_C_API int turbo_flow_gateway_owner_create_registered(
    turbo_flow_gateway_registry_t *registry, const char *gateway,
    const turbo_flow_gateway_open_request_t *request,
    turbo_flow_gateway_owner_t **out);
CXX_C_API int turbo_flow_gateway_owner_instance(
    const turbo_flow_gateway_owner_t *owner,
    turbo_flow_gateway_protocol_t expected_protocol,
    turbo_flow_gateway_t **out);
CXX_C_API const char *
turbo_flow_gateway_owner_name(const turbo_flow_gateway_owner_t *owner);
CXX_C_API turbo_flow_gateway_protocol_t
turbo_flow_gateway_owner_protocol(const turbo_flow_gateway_owner_t *owner);
CXX_C_API void turbo_flow_gateway_owner_destroy(
    turbo_flow_gateway_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_GATEWAY_H */
