#ifndef TURBO_FLOW_RESOLVED_CONFIG_H
#define TURBO_FLOW_RESOLVED_CONFIG_H

#include "turbo_flow_export.h"
#include "platform.h"
#include "turbo_error.h"
#include "turbo_flow_config_limits.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CONFIG_PATH_MAX 255u
#define TURBO_FLOW_CONFIG_MESSAGE_MAX 255u
typedef struct turbo_flow_resolved_config_s turbo_flow_resolved_config_t;

typedef enum turbo_flow_config_value_type_e {
  TURBO_FLOW_CONFIG_NULL = 0,
  TURBO_FLOW_CONFIG_BOOL,
  TURBO_FLOW_CONFIG_NUMBER,
  TURBO_FLOW_CONFIG_STRING,
  TURBO_FLOW_CONFIG_ARRAY,
  TURBO_FLOW_CONFIG_OBJECT
} turbo_flow_config_value_type_t;

/** Borrowed immutable adapter projection from one resolved snapshot. */
typedef struct turbo_flow_resolved_adapter_view_s {
  size_t size;
  const char *name;
  const char *kind;
  const void *config;
} turbo_flow_resolved_adapter_view_t;

#define TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT                                                      \
  {sizeof(turbo_flow_resolved_adapter_view_t), NULL, NULL, NULL}

/** Borrowed immutable channel projection from one resolved snapshot. */
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

#define TURBO_FLOW_CONFIG_ERROR_INIT {sizeof(turbo_flow_config_error_t), TURBO_OK, {0}, {0}}

/**
 * Resolve one YAML document into a canonical immutable configuration snapshot.
 * @param yaml Borrowed UTF-8 YAML bytes.
 * @param yaml_len Byte length of `yaml`.
 * @param out Receives an owned snapshot destroyed by turbo_flow_resolved_config_destroy().
 * @param error Receives a structured path and reason on validation failure.
 * @return TURBO_OK, a parse/validation error, or TURBO_ENOMEM.
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
TURBO_FLOW_C_API const char *turbo_flow_resolved_config_json(const turbo_flow_resolved_config_t *config,
                                                      size_t *json_len);

TURBO_FLOW_C_API int turbo_flow_resolved_config_profile_adapter(const turbo_flow_resolved_config_t *config,
                                                         const char *profile, const char *parameter,
                                                         const char **adapter_name);
TURBO_FLOW_C_API int turbo_flow_resolved_config_profile_adapter_optional(
    const turbo_flow_resolved_config_t *config, const char *profile, const char *parameter,
    const char **adapter_name);
TURBO_FLOW_C_API int turbo_flow_resolved_config_profile_channel(const turbo_flow_resolved_config_t *config,
                                                         const char *profile, const char *parameter,
                                                         const char **channel_name);
TURBO_FLOW_C_API int turbo_flow_resolved_config_profile_channel_optional(
    const turbo_flow_resolved_config_t *config, const char *profile, const char *parameter,
    const char **channel_name);

/** Project a borrowed named channel; returns TURBO_ENOENT when absent. */
TURBO_FLOW_C_API int turbo_flow_resolved_config_channel(const turbo_flow_resolved_config_t *config,
                                                 const char *name,
                                                 turbo_flow_resolved_channel_view_t *view);
TURBO_FLOW_C_API int turbo_flow_resolved_channel_get_string(const turbo_flow_resolved_channel_view_t *view,
                                                     const char *field, const char **value);

TURBO_FLOW_C_API int turbo_flow_resolved_config_preflight_adapter_kinds(
    const turbo_flow_resolved_config_t *config, const char *const *enabled_kinds,
    size_t enabled_kind_count, turbo_flow_config_error_t *error);

/** Project a borrowed named adapter; returns TURBO_ENOENT when absent. */
TURBO_FLOW_C_API int turbo_flow_resolved_config_adapter(const turbo_flow_resolved_config_t *config,
                                                 const char *adapter_name,
                                                 turbo_flow_resolved_adapter_view_t *view);
TURBO_FLOW_C_API size_t
turbo_flow_resolved_adapter_field_count(const turbo_flow_resolved_adapter_view_t *view);
TURBO_FLOW_C_API const char *
turbo_flow_resolved_adapter_field_name(const turbo_flow_resolved_adapter_view_t *view,
                                       size_t index);
TURBO_FLOW_C_API int turbo_flow_resolved_adapter_field_type(const turbo_flow_resolved_adapter_view_t *view,
                                                     const char *field,
                                                     turbo_flow_config_value_type_t *type);
TURBO_FLOW_C_API int turbo_flow_resolved_adapter_get_string(const turbo_flow_resolved_adapter_view_t *view,
                                                     const char *field, const char **value);
TURBO_FLOW_C_API int turbo_flow_resolved_adapter_get_bool(const turbo_flow_resolved_adapter_view_t *view,
                                                   const char *field, int *value);
TURBO_FLOW_C_API int turbo_flow_resolved_adapter_get_u64(const turbo_flow_resolved_adapter_view_t *view,
                                                  const char *field, uint64_t *value);
TURBO_FLOW_C_API int turbo_flow_resolved_adapter_get_i64(const turbo_flow_resolved_adapter_view_t *view,
                                                  const char *field, int64_t *value);
TURBO_FLOW_C_API int turbo_flow_resolved_adapter_array_size(const turbo_flow_resolved_adapter_view_t *view,
                                                     const char *field, size_t *size);
TURBO_FLOW_C_API int
turbo_flow_resolved_adapter_array_string_at(const turbo_flow_resolved_adapter_view_t *view,
                                            const char *field, size_t index, const char **value);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_RESOLVED_CONFIG_H */
