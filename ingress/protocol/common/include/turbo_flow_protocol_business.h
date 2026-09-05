#ifndef TURBO_FLOW_PROTOCOL_BUSINESS_H
#define TURBO_FLOW_PROTOCOL_BUSINESS_H

#include "turbo_flow_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION 1u
#define TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MAJOR 1u
#define TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MINOR 0u

#define TURBO_FLOW_PROTOCOL_BUSINESS_NAME_MAX 63u
#define TURBO_FLOW_PROTOCOL_BUSINESS_PROFILE_MAX 63u
#define TURBO_FLOW_PROTOCOL_BUSINESS_SCHEMA_ID_MAX 127u
#define TURBO_FLOW_PROTOCOL_BUSINESS_TYPE_NAME_MAX 127u
#define TURBO_FLOW_PROTOCOL_BUSINESS_MEDIA_TYPE_MAX 63u
#define TURBO_FLOW_PROTOCOL_BUSINESS_ROUTE_MAX 511u

typedef uint64_t turbo_flow_protocol_business_capabilities_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT (UINT64_C(1) << 0u)
#define TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND (UINT64_C(1) << 1u)

typedef struct turbo_flow_protocol_business_s turbo_flow_protocol_business_t;
typedef struct turbo_flow_protocol_business_registry_s turbo_flow_protocol_business_registry_t;
typedef struct turbo_flow_protocol_business_owner_s turbo_flow_protocol_business_owner_t;

/**
 * Borrowed payload plus optional trusted-schema identity.
 *
 * schema_id and type_name must either both be present or both be absent. They
 * identify a schema already admitted by the business provider; they are never
 * interpreted as a filesystem path or schema source text.
 */
typedef struct turbo_flow_protocol_business_content_view_s {
  size_t size;
  uint32_t abi_version;
  const uint8_t *data;
  size_t data_size;
  const char *media_type;
  const char *schema_id;
  const char *type_name;
} turbo_flow_protocol_business_content_view_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_CONTENT_VIEW_INIT                                              \
  {sizeof(turbo_flow_protocol_business_content_view_t),                                             \
   TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,                                                        \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * Immutable event observed only after graph/sink settlement succeeds.
 *
 * All pointers are borrowed for the synchronous consume call. A provider that
 * queues work must copy the complete event before returning. A provider error
 * is returned to the host for retry/dead-letter policy and never rolls back or
 * changes the already completed protocol settlement.
 */
typedef struct turbo_flow_protocol_business_event_view_s {
  size_t size;
  uint32_t abi_version;
  uint64_t delivery_id;
  uint64_t session_id;
  uint64_t session_generation;
  const char *route;
  size_t route_size;
  turbo_flow_protocol_metadata_t metadata;
  turbo_flow_protocol_business_content_view_t content;
} turbo_flow_protocol_business_event_view_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_EVENT_VIEW_INIT                                                \
  {sizeof(turbo_flow_protocol_business_event_view_t),                                               \
   TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,                                                        \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_METADATA_INIT,                                                               \
   TURBO_FLOW_PROTOCOL_BUSINESS_CONTENT_VIEW_INIT}

/**
 * Borrowed domain command submitted by a business owner.
 *
 * action is a business-level verb. The provider validates its schema-bound
 * content and maps it to one protocol-neutral protocol operation.
 */
typedef struct turbo_flow_protocol_business_command_request_s {
  size_t size;
  uint32_t abi_version;
  uint64_t command_id;
  turbo_flow_protocol_kind_t protocol;
  const char *tenant;
  const char *device_id;
  const char *action;
  const char *resource;
  const char *correlation_id;
  uint64_t sequence;
  turbo_flow_protocol_business_content_view_t content;
} turbo_flow_protocol_business_command_request_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_REQUEST_INIT                                           \
  {sizeof(turbo_flow_protocol_business_command_request_t),                                          \
   TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,                                                        \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_MQTT_SN,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_BUSINESS_CONTENT_VIEW_INIT}

/**
 * Caller-owned result of business validation and mapping.
 *
 * The provider writes only fixed-size fields and payload. payload must point to
 * caller memory and is never retained. prepare_command fails with
 * SALTS_EMSGSIZE rather than returning partial output.
 */
typedef struct turbo_flow_protocol_business_command_output_s {
  size_t size;
  uint32_t abi_version;
  char device_id[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u];
  char operation[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  char resource[TURBO_FLOW_PROTOCOL_RESOURCE_MAX + 1u];
  char correlation_id[TURBO_FLOW_PROTOCOL_CORRELATION_MAX + 1u];
  uint64_t sequence;
  uint8_t *payload;
  size_t payload_capacity;
  size_t payload_size;
} turbo_flow_protocol_business_command_output_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_OUTPUT_INIT                                            \
  {sizeof(turbo_flow_protocol_business_command_output_t),                                           \
   TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,                                                        \
   {0},                                                                                            \
   {0},                                                                                            \
   {0},                                                                                            \
   {0},                                                                                            \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u}

typedef struct turbo_flow_protocol_business_info_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_business_capabilities_t capabilities;
  size_t max_payload_size;
  char business[TURBO_FLOW_PROTOCOL_BUSINESS_NAME_MAX + 1u];
  char profile[TURBO_FLOW_PROTOCOL_BUSINESS_PROFILE_MAX + 1u];
} turbo_flow_protocol_business_info_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_INFO_INIT                                                      \
  {sizeof(turbo_flow_protocol_business_info_t),                                                     \
   TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,                                                        \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   {0},                                                                                            \
   {0}}

TURBO_FLOW_C_API int turbo_flow_protocol_business_consume_committed(
    turbo_flow_protocol_business_t *business, const turbo_flow_protocol_business_event_view_t *event);

TURBO_FLOW_C_API int turbo_flow_protocol_business_prepare_command(
    turbo_flow_protocol_business_t *business,
    const turbo_flow_protocol_business_command_request_t *request,
    turbo_flow_protocol_business_command_output_t *output);

/** Create a borrowed protocol command view over a validated command output. */
TURBO_FLOW_C_API int
turbo_flow_protocol_business_command_view(const turbo_flow_protocol_business_command_output_t *output,
                                         turbo_flow_protocol_command_view_t *view);

TURBO_FLOW_C_API int turbo_flow_protocol_business_get_info(const turbo_flow_protocol_business_t *business,
                                                   turbo_flow_protocol_business_info_t *out);

typedef struct turbo_flow_protocol_business_open_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  const char *tenant;
  const char *profile;
  size_t max_payload_size;
} turbo_flow_protocol_business_open_request_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_OPEN_REQUEST_INIT                                              \
  {sizeof(turbo_flow_protocol_business_open_request_t),                                             \
   TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,                                                        \
   TURBO_FLOW_PROTOCOL_MQTT_SN,                                                            \
   "default",                                                                                      \
   NULL,                                                                                           \
   TURBO_FLOW_PROTOCOL_DEFAULT_MAX_FRAME_SIZE}

typedef struct turbo_flow_protocol_business_service_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_business_t *instance;
  void *owner;
} turbo_flow_protocol_business_service_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_SERVICE_INIT                                                   \
  {sizeof(turbo_flow_protocol_business_service_t), TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION,         \
   TURBO_FLOW_PROTOCOL_MQTT_SN, NULL, NULL}

typedef int (*turbo_flow_protocol_business_open_fn)(
    void *ctx, const turbo_flow_protocol_business_open_request_t *request,
    turbo_flow_protocol_business_service_t *service);
typedef void (*turbo_flow_protocol_business_close_fn)(
    void *ctx, turbo_flow_protocol_business_service_t *service);

/**
 * Stable business plugin export. Concrete domain operations stay behind the
 * provider-neutral business instance.
 */
typedef struct turbo_flow_protocol_business_plugin_api_s {
  size_t size;
  uint32_t version_major;
  uint32_t version_minor;
  const char *business;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_business_capabilities_t capabilities;
  void *ctx;
  turbo_flow_protocol_business_open_fn open;
  turbo_flow_protocol_business_close_fn close;
} turbo_flow_protocol_business_plugin_api_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_EXPORT_SYMBOL                                           \
  "turbo_flow_protocol_business_plugin_get_api"

typedef const turbo_flow_protocol_business_plugin_api_t *(
    *turbo_flow_protocol_business_plugin_get_api_fn)(void);

TURBO_FLOW_C_API const turbo_flow_protocol_business_plugin_api_t *
turbo_flow_protocol_business_plugin_get_api(void);

TURBO_FLOW_C_API int
turbo_flow_protocol_business_registry_create(size_t capacity,
                                            turbo_flow_protocol_business_registry_t **out);
TURBO_FLOW_C_API int
turbo_flow_protocol_business_registry_destroy(turbo_flow_protocol_business_registry_t *registry);
TURBO_FLOW_C_API int
turbo_flow_protocol_business_registry_register(turbo_flow_protocol_business_registry_t *registry,
                                              const turbo_flow_protocol_business_plugin_api_t *api);
TURBO_FLOW_C_API int
turbo_flow_protocol_business_registry_load(turbo_flow_protocol_business_registry_t *registry,
                                          const char *path, char *reason, size_t reason_size);
TURBO_FLOW_C_API const turbo_flow_protocol_business_plugin_api_t *
turbo_flow_protocol_business_registry_find(const turbo_flow_protocol_business_registry_t *registry,
                                          const char *business);

TURBO_FLOW_C_API int turbo_flow_protocol_business_owner_create_registered(
    turbo_flow_protocol_business_registry_t *registry, const char *business,
    const turbo_flow_protocol_business_open_request_t *request,
    turbo_flow_protocol_business_owner_t **out);
TURBO_FLOW_C_API int
turbo_flow_protocol_business_owner_instance(const turbo_flow_protocol_business_owner_t *owner,
                                           turbo_flow_protocol_kind_t expected_protocol,
                                           turbo_flow_protocol_business_t **out);
TURBO_FLOW_C_API void
turbo_flow_protocol_business_owner_destroy(turbo_flow_protocol_business_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_BUSINESS_H */
