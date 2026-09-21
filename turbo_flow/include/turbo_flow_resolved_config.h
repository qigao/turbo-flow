#ifndef TURBO_FLOW_RESOLVED_CONFIG_H
#define TURBO_FLOW_RESOLVED_CONFIG_H

#include "platform.h"
#include "salts_error.h"
#include "turbo_flow_config_limits.h"
#include "turbo_flow_export.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CONFIG_PATH_MAX 255u
#define TURBO_FLOW_CONFIG_MESSAGE_MAX 255u
typedef struct turbo_flow_resolved_config_s turbo_flow_resolved_config_t;

/** Borrowed immutable configured DLL identity, valid until its resolved config is destroyed. */
typedef struct turbo_flow_resolved_plugin_view_s {
  size_t size;
  const char *id;
  const char *version;
  const char *path;
} turbo_flow_resolved_plugin_view_t;

#define TURBO_FLOW_RESOLVED_PLUGIN_VIEW_INIT                                                       \
  {sizeof(turbo_flow_resolved_plugin_view_t), NULL, NULL, NULL}

typedef enum turbo_flow_config_operation_exec_e {
  TURBO_FLOW_CONFIG_OPERATION_INLINE = 0,
  TURBO_FLOW_CONFIG_OPERATION_THREAD = 1,
  TURBO_FLOW_CONFIG_OPERATION_CORO = 2
} turbo_flow_config_operation_exec_t;

typedef enum turbo_flow_config_operation_threading_e {
  TURBO_FLOW_CONFIG_OPERATION_OWNER = 0,
  TURBO_FLOW_CONFIG_OPERATION_THREAD_SAFE = 1
} turbo_flow_config_operation_threading_t;

typedef enum turbo_flow_config_operation_cancellation_e {
  TURBO_FLOW_CONFIG_OPERATION_CANCEL_NONE = 0,
  TURBO_FLOW_CONFIG_OPERATION_CANCEL_COOPERATIVE = 1
} turbo_flow_config_operation_cancellation_t;

typedef struct turbo_flow_resolved_operation_binding_view_s {
  size_t size;
  const char *operation;
  const char *resource;
  const char *plugin;
  uint32_t version;
  const char *input_schema;
  uint32_t input_schema_version;
  const char *output_schema;
  uint32_t output_schema_version;
  turbo_flow_config_operation_exec_t execution;
  turbo_flow_config_operation_threading_t threading;
  turbo_flow_config_operation_cancellation_t cancellation;
  uint32_t max_inflight;
  size_t max_input_bytes;
  size_t max_result_bytes;
  size_t max_retained_bytes;
  uint32_t max_steps;
  uint64_t deadline_ms;
  size_t permission_count;
} turbo_flow_resolved_operation_binding_view_t;

#define TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT                                            \
  {sizeof(turbo_flow_resolved_operation_binding_view_t),                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   TURBO_FLOW_CONFIG_OPERATION_INLINE,                                                             \
   TURBO_FLOW_CONFIG_OPERATION_OWNER,                                                              \
   TURBO_FLOW_CONFIG_OPERATION_CANCEL_NONE,                                                        \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

typedef struct turbo_flow_resolved_materializer_binding_view_s {
  size_t size;
  const char *plugin;
  const char *schema;
  uint32_t schema_version;
  turbo_flow_data_encoding_t encoding;
} turbo_flow_resolved_materializer_binding_view_t;

#define TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT                                         \
  {sizeof(turbo_flow_resolved_materializer_binding_view_t), NULL, NULL, 0u,                        \
   TURBO_FLOW_DATA_ENCODING_OPAQUE}

typedef enum turbo_flow_config_value_type_e {
  TURBO_FLOW_CONFIG_NULL = 0,
  TURBO_FLOW_CONFIG_BOOL,
  TURBO_FLOW_CONFIG_NUMBER,
  TURBO_FLOW_CONFIG_STRING,
  TURBO_FLOW_CONFIG_ARRAY,
  TURBO_FLOW_CONFIG_OBJECT
} turbo_flow_config_value_type_t;

/** Borrowed immutable adapter projection, valid until its resolved config is destroyed. */
typedef struct turbo_flow_resolved_adapter_view_s {
  size_t size;
  const char *name;
  const char *kind;
  const void *config;
} turbo_flow_resolved_adapter_view_t;

#define TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT                                                      \
  {sizeof(turbo_flow_resolved_adapter_view_t), NULL, NULL, NULL}

/** Borrowed immutable channel projection, valid until its resolved config is destroyed. */
typedef struct turbo_flow_resolved_channel_view_s {
  size_t size;
  const char *name;
  const char *kind;
  const void *config;
} turbo_flow_resolved_channel_view_t;

#define TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT                                                      \
  {sizeof(turbo_flow_resolved_channel_view_t), NULL, NULL, NULL}

typedef struct turbo_flow_config_error_s {
  size_t size;
  int status;
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  char message[TURBO_FLOW_CONFIG_MESSAGE_MAX + 1u];
} turbo_flow_config_error_t;

#define TURBO_FLOW_CONFIG_ERROR_INIT {sizeof(turbo_flow_config_error_t), SALTS_OK, {0}, {0}}

/**
 * Resolve one YAML document into a canonical immutable configuration snapshot.
 * @param yaml Borrowed UTF-8 YAML bytes.
 * @param yaml_len Byte length of `yaml`.
 * @param out Receives an owned snapshot destroyed by turbo_flow_resolved_config_destroy().
 * @param error Receives a structured path and reason on validation failure.
 * @return SALTS_OK, a parse/validation error, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int turbo_flow_config_resolve_yaml(const char *yaml, size_t yaml_len,
                                                    turbo_flow_resolved_config_t **out,
                                                    turbo_flow_config_error_t *error);
TURBO_FLOW_C_API void turbo_flow_resolved_config_destroy(turbo_flow_resolved_config_t *config);

/**
 * Return borrowed canonical JSON valid until `config` is destroyed.
 * @param config Borrowed immutable snapshot.
 * @param json_len Optional output byte length.
 * @return Borrowed JSON text, or NULL for an invalid snapshot.
 */
TURBO_FLOW_C_API const char *
turbo_flow_resolved_config_json(const turbo_flow_resolved_config_t *config, size_t *json_len);

/** Write the ordered configured DLL count; zeroes `count` on error. */
TURBO_FLOW_C_API int
turbo_flow_resolved_config_plugin_count(const turbo_flow_resolved_config_t *config, size_t *count);
/**
 * Project one configured DLL by load order. The returned strings are borrowed from `config`.
 * A valid-sized view is cleared on error while preserving its exact layout.
 */
TURBO_FLOW_C_API int
turbo_flow_resolved_config_plugin_at(const turbo_flow_resolved_config_t *config, size_t index,
                                     turbo_flow_resolved_plugin_view_t *view);

/** Write the binding count; zeroes `count` on error. */
TURBO_FLOW_C_API int
turbo_flow_resolved_config_operation_binding_count(const turbo_flow_resolved_config_t *config,
                                                   size_t *count);
/**
 * Project a borrowed binding by index. `view->size` must be at least sizeof(*view);
 * returned fields are valid until `config` is destroyed.
 * A valid-sized view is cleared on error while preserving size. Returns SALTS_ENOENT when absent.
 */
TURBO_FLOW_C_API int
turbo_flow_resolved_config_operation_binding_at(const turbo_flow_resolved_config_t *config,
                                                size_t index,
                                                turbo_flow_resolved_operation_binding_view_t *view);
/**
 * Project a borrowed permission by binding and permission index.
 * Sets `permission` to NULL on error and returns SALTS_ENOENT when either index is absent.
 */
TURBO_FLOW_C_API int turbo_flow_resolved_config_operation_binding_permission_at(
    const turbo_flow_resolved_config_t *config, size_t binding_index, size_t permission_index,
    const char **permission);

/** Write the explicit materializer binding count; zeroes count on error. */
TURBO_FLOW_C_API int turbo_flow_resolved_config_materializer_binding_count(
    const turbo_flow_resolved_config_t *config, size_t *count);
/** Project one explicit materializer binding by index. */
TURBO_FLOW_C_API int turbo_flow_resolved_config_materializer_binding_at(
    const turbo_flow_resolved_config_t *config, size_t index,
    turbo_flow_resolved_materializer_binding_view_t *view);

TURBO_FLOW_C_API int
turbo_flow_resolved_config_profile_adapter(const turbo_flow_resolved_config_t *config,
                                           const char *profile, const char *parameter,
                                           const char **adapter_name);
TURBO_FLOW_C_API int
turbo_flow_resolved_config_profile_adapter_optional(const turbo_flow_resolved_config_t *config,
                                                    const char *profile, const char *parameter,
                                                    const char **adapter_name);
TURBO_FLOW_C_API int
turbo_flow_resolved_config_profile_channel(const turbo_flow_resolved_config_t *config,
                                           const char *profile, const char *parameter,
                                           const char **channel_name);
TURBO_FLOW_C_API int
turbo_flow_resolved_config_profile_channel_optional(const turbo_flow_resolved_config_t *config,
                                                    const char *profile, const char *parameter,
                                                    const char **channel_name);

/** Project a borrowed named channel; `view->size` must be at least sizeof(*view), and its
 * fields are valid until `config` is destroyed. Returns SALTS_ENOENT when absent. */
TURBO_FLOW_C_API int turbo_flow_resolved_config_channel(const turbo_flow_resolved_config_t *config,
                                                        const char *name,
                                                        turbo_flow_resolved_channel_view_t *view);
TURBO_FLOW_C_API int
turbo_flow_resolved_channel_get_string(const turbo_flow_resolved_channel_view_t *view,
                                       const char *field, const char **value);

TURBO_FLOW_C_API int turbo_flow_resolved_config_preflight_adapter_kinds(
    const turbo_flow_resolved_config_t *config, const char *const *enabled_kinds,
    size_t enabled_kind_count, turbo_flow_config_error_t *error);

/** Project a borrowed named adapter; `view->size` must be at least sizeof(*view), and its
 * fields are valid until `config` is destroyed. Returns SALTS_ENOENT when absent. */
TURBO_FLOW_C_API int turbo_flow_resolved_config_adapter(const turbo_flow_resolved_config_t *config,
                                                        const char *adapter_name,
                                                        turbo_flow_resolved_adapter_view_t *view);
TURBO_FLOW_C_API size_t
turbo_flow_resolved_adapter_field_count(const turbo_flow_resolved_adapter_view_t *view);
TURBO_FLOW_C_API const char *
turbo_flow_resolved_adapter_field_name(const turbo_flow_resolved_adapter_view_t *view,
                                       size_t index);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_field_type(const turbo_flow_resolved_adapter_view_t *view,
                                       const char *field, turbo_flow_config_value_type_t *type);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_get_string(const turbo_flow_resolved_adapter_view_t *view,
                                       const char *field, const char **value);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_get_bool(const turbo_flow_resolved_adapter_view_t *view,
                                     const char *field, int *value);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_get_u64(const turbo_flow_resolved_adapter_view_t *view,
                                    const char *field, uint64_t *value);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_get_i64(const turbo_flow_resolved_adapter_view_t *view,
                                    const char *field, int64_t *value);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_array_size(const turbo_flow_resolved_adapter_view_t *view,
                                       const char *field, size_t *size);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_array_string_at(const turbo_flow_resolved_adapter_view_t *view,
                                            const char *field, size_t index, const char **value);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_RESOLVED_CONFIG_H */
