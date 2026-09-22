#ifndef TURBO_FLOW_PROTOCOL_H
#define TURBO_FLOW_PROTOCOL_H

#include "platform.h"
#include "turbo_flow_export.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_ABI_VERSION 1u
#define TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR 1u
#define TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR 1u

#define TURBO_FLOW_PROTOCOL_TENANT_MAX 63u
#define TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX 127u
#define TURBO_FLOW_PROTOCOL_OPERATION_MAX 63u
#define TURBO_FLOW_PROTOCOL_RESOURCE_MAX 255u
#define TURBO_FLOW_PROTOCOL_VERSION_MAX 15u
#define TURBO_FLOW_PROTOCOL_CORRELATION_MAX 63u
#define TURBO_FLOW_PROTOCOL_SEMANTIC_MEDIA_TYPE_MAX 127u
#define TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE UINT32_MAX
#define TURBO_FLOW_PROTOCOL_DEFAULT_MAX_FRAME_SIZE (1024u * 1024u)

typedef enum turbo_flow_protocol_kind_e {
  TURBO_FLOW_PROTOCOL_MQTT_SN = 1,
  TURBO_FLOW_PROTOCOL_COAP,
  TURBO_FLOW_PROTOCOL_LWM2M,
  TURBO_FLOW_PROTOCOL_OCPP,
  TURBO_FLOW_PROTOCOL_GBT_32960,
  TURBO_FLOW_PROTOCOL_JTT_808
} turbo_flow_protocol_kind_t;

typedef enum turbo_flow_protocol_direction_e {
  TURBO_FLOW_PROTOCOL_DIRECTION_UP = 1,
  TURBO_FLOW_PROTOCOL_DIRECTION_DOWN
} turbo_flow_protocol_direction_t;

typedef uint64_t turbo_flow_protocol_capabilities_t;

#define TURBO_FLOW_PROTOCOL_CAP_INGRESS (UINT64_C(1) << 0u)
#define TURBO_FLOW_PROTOCOL_CAP_EGRESS (UINT64_C(1) << 1u)
#define TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE (UINT64_C(1) << 2u)
#define TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY (UINT64_C(1) << 3u)
#define TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE (UINT64_C(1) << 4u)

typedef struct turbo_flow_protocol_s turbo_flow_protocol_t;
typedef struct turbo_flow_protocol_registry_s turbo_flow_protocol_registry_t;
typedef struct turbo_flow_protocol_owner_s turbo_flow_protocol_owner_t;

/**
 * Immutable protocol configuration borrowed only for the duration of plugin open().
 *
 * protocol_version is optional; each plugin selects its documented default when
 * it is NULL or empty.
 */
typedef struct turbo_flow_protocol_open_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  const char *protocol_version;
  size_t max_frame_size;
} turbo_flow_protocol_open_request_t;

#define TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT                                                      \
  {sizeof(turbo_flow_protocol_open_request_t), TURBO_FLOW_PROTOCOL_ABI_VERSION,                    \
   TURBO_FLOW_PROTOCOL_MQTT_SN, NULL, TURBO_FLOW_PROTOCOL_DEFAULT_MAX_FRAME_SIZE}

/**
 * Borrowed device-protocol frame.
 *
 * device_id may be omitted only when the protocol frame carries a stable device
 * identity (currently GB/T 32960 and JT/T 808). protocol_version may be omitted
 * to use the version selected when the protocol was opened.
 */
typedef struct turbo_flow_protocol_frame_view_s {
  size_t size;
  uint32_t abi_version;
  const uint8_t *data;
  size_t data_size;
  const char *device_id;
  const char *protocol_version;
} turbo_flow_protocol_frame_view_t;

#define TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT                                                        \
  {sizeof(turbo_flow_protocol_frame_view_t), TURBO_FLOW_PROTOCOL_ABI_VERSION, NULL, 0u, NULL, NULL}

/** Pointer-free protocol metadata copied with every normalized ingress message. */
typedef struct turbo_flow_protocol_metadata_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_direction_t direction;
  uint32_t message_type;
  uint64_t sequence;
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
  char device_id[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u];
  char operation[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  char correlation_id[TURBO_FLOW_PROTOCOL_CORRELATION_MAX + 1u];
} turbo_flow_protocol_metadata_t;

#define TURBO_FLOW_PROTOCOL_METADATA_INIT                                                          \
  {sizeof(turbo_flow_protocol_metadata_t),                                                         \
   TURBO_FLOW_PROTOCOL_ABI_VERSION,                                                                \
   0,                                                                                              \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   {0},                                                                                            \
   {0},                                                                                            \
   {0},                                                                                            \
   {0}}

/** Immutable snapshot of one opened provider-neutral protocol. */
typedef struct turbo_flow_protocol_info_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_capabilities_t capabilities;
  size_t max_frame_size;
  char name[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
} turbo_flow_protocol_info_t;

#define TURBO_FLOW_PROTOCOL_INFO_INIT                                                              \
  {sizeof(turbo_flow_protocol_info_t), TURBO_FLOW_PROTOCOL_ABI_VERSION, 0, 0u, 0u, {0}, {0}}

/** Caller-owned buffers receiving one protocol-neutral ingress message. */
typedef struct turbo_flow_protocol_message_output_s {
  size_t size;
  uint32_t abi_version;
  uint8_t *payload;
  size_t payload_capacity;
  size_t payload_size;
  turbo_flow_protocol_metadata_t metadata;
} turbo_flow_protocol_message_output_t;

#define TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT                                                    \
  {sizeof(turbo_flow_protocol_message_output_t),                                                   \
   TURBO_FLOW_PROTOCOL_ABI_VERSION,                                                                \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_METADATA_INIT}


/**
 * Caller-owned semantic/application content produced by a codec.
 *
 * data points to caller storage and must not be replaced or retained by the
 * codec. data_size is published only after a successful semantic decode.
 * semantic_type is protocol-defined (for example CoAP Content-Format or a
 * JT/T 808 transparent-data subtype); UINT32_MAX means no discriminator.
 * media_type is protocol-derived and may be empty when the wire protocol does
 * not declare one.
 */
typedef struct turbo_flow_protocol_semantic_output_s {
  size_t size;
  uint32_t abi_version;
  uint8_t *data;
  size_t capacity;
  size_t data_size;
  uint32_t semantic_type;
  char media_type[TURBO_FLOW_PROTOCOL_SEMANTIC_MEDIA_TYPE_MAX + 1u];
} turbo_flow_protocol_semantic_output_t;

#define TURBO_FLOW_PROTOCOL_SEMANTIC_OUTPUT_INIT                                                   \
  {sizeof(turbo_flow_protocol_semantic_output_t),                                                  \
   TURBO_FLOW_PROTOCOL_ABI_VERSION,                                                                \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE,                                                         \
   {0}}

/** Caller-owned frame buffer receiving one protocol frame. */
typedef struct turbo_flow_protocol_frame_output_s {
  size_t size;
  uint32_t abi_version;
  uint8_t *data;
  size_t capacity;
  size_t data_size;
  turbo_flow_protocol_metadata_t metadata;
} turbo_flow_protocol_frame_output_t;

#define TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT                                                      \
  {sizeof(turbo_flow_protocol_frame_output_t), TURBO_FLOW_PROTOCOL_ABI_VERSION, NULL, 0u, 0u,      \
   TURBO_FLOW_PROTOCOL_METADATA_INIT}

/**
 * Borrowed protocol-neutral downlink command.
 *
 * operation and device_id are required. resource is protocol-specific (for
 * example a CoAP URI path or MQTT-SN topic id) and may be NULL only for
 * operations whose wire format does not use it. correlation_id and sequence
 * are copied into wire correlation fields where the protocol defines them.
 * payload is borrowed for this synchronous call.
 */
typedef struct turbo_flow_protocol_command_view_s {
  size_t size;
  uint32_t abi_version;
  const char *device_id;
  const char *operation;
  const char *resource;
  const char *correlation_id;
  uint64_t sequence;
  const uint8_t *payload;
  size_t payload_size;
} turbo_flow_protocol_command_view_t;

#define TURBO_FLOW_PROTOCOL_COMMAND_VIEW_INIT                                                      \
  {sizeof(turbo_flow_protocol_command_view_t),                                                     \
   TURBO_FLOW_PROTOCOL_ABI_VERSION,                                                                \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u}

/**
 * Build the protocol response that becomes legal after adapter/storage
 * settlement. A successful call may return data_size == 0 when the protocol
 * message does not require a response.
 *
 * request is the complete original ingress frame and remains caller-owned.
 * status is SALTS_OK for successful settlement, otherwise the terminal
 * settlement error that the codec maps to a protocol-defined rejection.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_reply(turbo_flow_protocol_t *protocol,
                                               const turbo_flow_protocol_frame_view_t *request,
                                               int status,
                                               turbo_flow_protocol_frame_output_t *output);

/**
 * Encode one protocol-neutral downlink command into a device wire frame.
 *
 * This API is deliberately separate from raw-preserving egress so callers
 * must choose semantic encoding explicitly; malformed raw frames never fall
 * back to a different interpretation.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_encode(turbo_flow_protocol_t *protocol,
                                                const turbo_flow_protocol_command_view_t *command,
                                                turbo_flow_protocol_frame_output_t *output);

/**
 * Decode and validate one complete device-protocol frame into a neutral message.
 *
 * The payload is the complete original frame. Protocol identity, operation,
 * sequence and correlation data are returned separately in metadata. No MQTT
 * topic, QoS, retain flag, broker state or product session enters this boundary.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_decode(turbo_flow_protocol_t *protocol,
                                                const turbo_flow_protocol_frame_view_t *frame,
                                                turbo_flow_protocol_message_output_t *output);


/**
 * Decode one ingress frame through a codec-owned single-pass semantic path.
 *
 * raw_output receives the complete original frame exactly as
 * turbo_flow_protocol_decode() does. semantic_output receives only decoded
 * application/semantic content. The codec is the sole wire parser: this API
 * never falls back to host parsing or calls inspect in addition to the semantic
 * callback. Codecs without the optional semantic callback return SALTS_ENOTSUP.
 *
 * Both output buffers are caller-owned and synchronous. On failure both
 * published sizes are zero; buffer bytes are unspecified.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_decode_semantic(
    turbo_flow_protocol_t *protocol, const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_message_output_t *raw_output,
    turbo_flow_protocol_semantic_output_t *semantic_output);

/** Return the stable lowercase name of a protocol kind, or NULL if invalid. */
TURBO_FLOW_C_API const char *turbo_flow_protocol_kind_name(turbo_flow_protocol_kind_t protocol);

/** Copy immutable identity, capability, version, and frame-limit metadata. */
TURBO_FLOW_C_API int turbo_flow_protocol_get_info(const turbo_flow_protocol_t *protocol,
                                                  turbo_flow_protocol_info_t *out);

typedef struct turbo_flow_protocol_service_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  /** Provider-neutral instance borrowed until plugin close(). */
  turbo_flow_protocol_t *instance;
  /** Plugin-private lifecycle handle passed back only to close(). */
  void *owner;
} turbo_flow_protocol_service_t;

#define TURBO_FLOW_PROTOCOL_SERVICE_INIT                                                           \
  {sizeof(turbo_flow_protocol_service_t), TURBO_FLOW_PROTOCOL_ABI_VERSION,                         \
   TURBO_FLOW_PROTOCOL_MQTT_SN, NULL, NULL}

typedef int (*turbo_flow_protocol_open_fn)(void *ctx,
                                           const turbo_flow_protocol_open_request_t *request,
                                           turbo_flow_protocol_service_t *service);
typedef void (*turbo_flow_protocol_close_fn)(void *ctx, turbo_flow_protocol_service_t *service);

/**
 * Stable plugin function table. Concrete codec and storage operations are not
 * exported; all data-path calls use the provider-neutral protocol instance.
 */
typedef struct turbo_flow_protocol_plugin_api_s {
  size_t size;
  uint32_t version_major;
  uint32_t version_minor;
  const char *name;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_capabilities_t capabilities;
  void *ctx;
  turbo_flow_protocol_open_fn open;
  turbo_flow_protocol_close_fn close;
} turbo_flow_protocol_plugin_api_t;

/**
 * Caller-serialized Protocol registry backed by a retained PluginHost snapshot.
 *
 * Creation is declared in turbo_flow_plugin_protocol.h. The registry contains
 * only immutable provider entries from the unified DLL catalog.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_registry_destroy(turbo_flow_protocol_registry_t *registry);
TURBO_FLOW_C_API const turbo_flow_protocol_plugin_api_t *
turbo_flow_protocol_registry_find(const turbo_flow_protocol_registry_t *registry, const char *name);

TURBO_FLOW_C_API int turbo_flow_protocol_owner_create_registered(
    turbo_flow_protocol_registry_t *registry, const char *name,
    const turbo_flow_protocol_open_request_t *request, turbo_flow_protocol_owner_t **out);
TURBO_FLOW_C_API int
turbo_flow_protocol_owner_instance(const turbo_flow_protocol_owner_t *owner,
                                   turbo_flow_protocol_kind_t expected_protocol,
                                   turbo_flow_protocol_t **out);
TURBO_FLOW_C_API const char *
turbo_flow_protocol_owner_name(const turbo_flow_protocol_owner_t *owner);
TURBO_FLOW_C_API turbo_flow_protocol_kind_t
turbo_flow_protocol_owner_protocol(const turbo_flow_protocol_owner_t *owner);
TURBO_FLOW_C_API void turbo_flow_protocol_owner_destroy(turbo_flow_protocol_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_H */
