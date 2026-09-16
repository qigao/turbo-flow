#include "flow_config_internal.h"

#include "salts_error.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

static const char *const flow_operation_keys[] = {"operation",        "resource",
                                                  "plugin",           "version",
                                                  "input_schema",     "input_schema_version",
                                                  "output_schema",    "output_schema_version",
                                                  "permissions",      "execution",
                                                  "threading",        "cancellation",
                                                  "max_inflight",     "max_input_bytes",
                                                  "max_result_bytes", "max_retained_bytes",
                                                  "max_steps",        "deadline_ms"};

static void flow_operation_path(char *path, size_t capacity, size_t index, const char *field) {
  (void)snprintf(path, capacity, "$.operation_bindings[%zu]%s%s", index, field ? "." : "",
                 field ? field : "");
}

static int flow_operation_identifier(const json_value_t *value, size_t index, const char *field,
                                     const char **result, turbo_flow_config_error_t *error) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  size_t length;
  const char *text;
  flow_operation_path(path, sizeof(path), index, field);
  if (!value || json_type(value) != JSON_STRING)
    return flow_config_error(error, SALTS_EINVAL, path, "expected identifier string");
  text = json_string(value);
  length = json_string_len(value);
  if (!text || length == 0u || length > TURBO_FLOW_CONFIG_OPERATION_ID_MAX ||
      strlen(text) != length)
    return flow_config_error(error, SALTS_EINVAL, path, "invalid identifier length");
  for (size_t i = 0; i < length; ++i) {
    unsigned char c = (unsigned char)text[i];
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' ||
          c == '.' || c == '-'))
      return flow_config_error(error, SALTS_EINVAL, path, "invalid identifier byte");
  }
  if (result) *result = text;
  return SALTS_OK;
}

static int flow_operation_integer(const json_value_t *value, size_t index, const char *field,
                                  uint64_t minimum, uint64_t maximum, uint64_t *result,
                                  turbo_flow_config_error_t *error) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  double number;
  uint64_t converted;
  flow_operation_path(path, sizeof(path), index, field);
  if (!value || json_type(value) != JSON_NUMBER)
    return flow_config_error(error, SALTS_EINVAL, path, "expected integer");
  number = json_number(value);
  if (!isfinite(number) || number < (double)minimum || number > (double)maximum)
    return flow_config_error(error, SALTS_EINVAL, path, "integer is outside the supported range");
  converted = (uint64_t)number;
  if ((double)converted != number)
    return flow_config_error(error, SALTS_EINVAL, path, "expected integer");
  if (result) *result = converted;
  return SALTS_OK;
}

static int flow_operation_enum(const json_value_t *value, size_t index, const char *field,
                               const char *const *values, size_t value_count,
                               turbo_flow_config_error_t *error) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  const char *text;
  flow_operation_path(path, sizeof(path), index, field);
  if (!value || json_type(value) != JSON_STRING)
    return flow_config_error(error, SALTS_EINVAL, path, "expected enum string");
  text = json_string(value);
  for (size_t i = 0; i < value_count; ++i)
    if (json_string_len(value) == strlen(values[i]) &&
        memcmp(text, values[i], json_string_len(value)) == 0)
      return SALTS_OK;
  return flow_config_error(error, SALTS_EINVAL, path, "unsupported enum value");
}

static int flow_operation_validate_permissions(const json_value_t *permissions, size_t index,
                                               turbo_flow_config_error_t *error) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  if (!permissions || json_type(permissions) != JSON_ARRAY) {
    flow_operation_path(path, sizeof(path), index, "permissions");
    return flow_config_error(error, SALTS_EINVAL, path, "permissions must be an array");
  }
  if (json_array_size(permissions) > TURBO_FLOW_CONFIG_OPERATION_MAX_PERMISSIONS) {
    flow_operation_path(path, sizeof(path), index, "permissions");
    return flow_config_error(error, SALTS_ENOSPC, path, "too many permissions");
  }
  for (size_t i = 0; i < json_array_size(permissions); ++i) {
    const char *permission = NULL;
    (void)snprintf(path, sizeof(path), "$.operation_bindings[%zu].permissions[%zu]", index, i);
    json_value_t *value = json_array_get(permissions, i);
    if (!value || json_type(value) != JSON_STRING)
      return flow_config_error(error, SALTS_EINVAL, path, "expected permission identifier");
    if (json_string_len(value) == 0u ||
        json_string_len(value) > TURBO_FLOW_CONFIG_OPERATION_ID_MAX ||
        strlen(json_string(value)) != json_string_len(value))
      return flow_config_error(error, SALTS_EINVAL, path, "invalid permission identifier");
    permission = json_string(value);
    for (size_t byte = 0; byte < json_string_len(value); ++byte) {
      unsigned char c = (unsigned char)permission[byte];
      if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
            c == '_' || c == '.' || c == '-'))
        return flow_config_error(error, SALTS_EINVAL, path, "invalid permission identifier");
    }
    for (size_t prior = 0; prior < i; ++prior)
      if (strcmp(permission, json_string(json_array_get(permissions, prior))) == 0)
        return flow_config_error(error, SALTS_EALREADY, path, "duplicate permission");
  }
  return SALTS_OK;
}

int flow_config_validate_operation_bindings(const json_value_t *bindings,
                                            const json_value_t *channels,
                                            turbo_flow_config_error_t *error) {
  static const char *const executions[] = {"inline", "thread", "coro"};
  static const char *const threadings[] = {"owner", "thread_safe"};
  static const char *const cancellations[] = {"none", "cooperative"};
  if (!bindings) return SALTS_OK;
  if (json_type(bindings) != JSON_ARRAY)
    return flow_config_error(error, SALTS_EINVAL, "$.operation_bindings", "expected array");
  if (json_array_size(bindings) > TURBO_FLOW_CONFIG_OPERATION_MAX_BINDINGS)
    return flow_config_error(error, SALTS_ENOSPC, "$.operation_bindings", "too many bindings");
  for (size_t i = 0; i < json_array_size(bindings); ++i) {
    json_value_t *binding = json_array_get(bindings, i);
    const char *operation = NULL;
    const char *resource = NULL;
    uint64_t max_result = 0u, max_retained = 0u;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    int rc;
    flow_operation_path(path, sizeof(path), i, NULL);
    rc = flow_config_object_keys(binding, path, flow_operation_keys,
                                 sizeof(flow_operation_keys) / sizeof(flow_operation_keys[0]),
                                 error);
    if (rc != SALTS_OK) return rc;
#define FLOW_ID(name)                                                                              \
  do {                                                                                             \
    rc = flow_operation_identifier(json_object_get(binding, #name), i, #name, &name, error);       \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)
    FLOW_ID(operation);
    json_value_t *resource_value = json_object_get(binding, "resource");
    if (resource_value) {
      rc = flow_operation_identifier(resource_value, i, "resource", &resource, error);
      if (rc != SALTS_OK) return rc;
      if (!channels || !json_object_get(channels, resource)) {
        flow_operation_path(path, sizeof(path), i, "resource");
        return flow_config_error(error, SALTS_EINVAL, path, "resource channel does not exist");
      }
    }
    const char *plugin = NULL, *input_schema = NULL, *output_schema = NULL;
    FLOW_ID(plugin);
    FLOW_ID(input_schema);
    FLOW_ID(output_schema);
#undef FLOW_ID
    (void)plugin;
    (void)input_schema;
    (void)output_schema;
#define FLOW_INT(field, min, max, out)                                                             \
  do {                                                                                             \
    rc =                                                                                           \
        flow_operation_integer(json_object_get(binding, #field), i, #field, min, max, out, error); \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)
    FLOW_INT(version, 1u, UINT32_MAX, NULL);
    FLOW_INT(input_schema_version, 1u, UINT32_MAX, NULL);
    FLOW_INT(output_schema_version, 1u, UINT32_MAX, NULL);
    FLOW_INT(max_inflight, 1u, TURBO_FLOW_CONFIG_OPERATION_MAX_INFLIGHT, NULL);
    FLOW_INT(max_input_bytes, 1u, TURBO_FLOW_CONFIG_OPERATION_MAX_BYTES, NULL);
    FLOW_INT(max_result_bytes, 1u, TURBO_FLOW_CONFIG_OPERATION_MAX_BYTES, &max_result);
    FLOW_INT(max_retained_bytes, 1u, TURBO_FLOW_CONFIG_OPERATION_MAX_BYTES, &max_retained);
    FLOW_INT(max_steps, 1u, TURBO_FLOW_CONFIG_OPERATION_MAX_STEPS, NULL);
    FLOW_INT(deadline_ms, 0u, TURBO_FLOW_CONFIG_OPERATION_MAX_DEADLINE_MS, NULL);
#undef FLOW_INT
    if (max_result > max_retained) {
      flow_operation_path(path, sizeof(path), i, "max_result_bytes");
      return flow_config_error(error, SALTS_EINVAL, path,
                               "max_result_bytes must not exceed max_retained_bytes");
    }
    rc = flow_operation_enum(json_object_get(binding, "execution"), i, "execution", executions,
                             sizeof(executions) / sizeof(executions[0]), error);
    if (rc != SALTS_OK) return rc;
    rc = flow_operation_enum(json_object_get(binding, "threading"), i, "threading", threadings,
                             sizeof(threadings) / sizeof(threadings[0]), error);
    if (rc != SALTS_OK) return rc;
    rc =
        flow_operation_enum(json_object_get(binding, "cancellation"), i, "cancellation",
                            cancellations, sizeof(cancellations) / sizeof(cancellations[0]), error);
    if (rc != SALTS_OK) return rc;
    rc = flow_operation_validate_permissions(json_object_get(binding, "permissions"), i, error);
    if (rc != SALTS_OK) return rc;
    for (size_t prior = 0; prior < i; ++prior) {
      json_value_t *other = json_array_get(bindings, prior);
      const char *other_operation = json_string(json_object_get(other, "operation"));
      json_value_t *other_resource_value = json_object_get(other, "resource");
      const char *other_resource = other_resource_value ? json_string(other_resource_value) : NULL;
      if (strcmp(operation, other_operation) == 0 &&
          ((!resource && !other_resource) ||
           (resource && other_resource && strcmp(resource, other_resource) == 0))) {
        flow_operation_path(path, sizeof(path), i, "operation");
        return flow_config_error(error, SALTS_EALREADY, path,
                                 "duplicate operation/resource binding");
      }
    }
  }
  return SALTS_OK;
}

static json_value_t *flow_operation_bindings(const turbo_flow_resolved_config_t *config) {
  json_value_t *bindings;
  if (!config || !config->document || json_type(config->document) != JSON_OBJECT) return NULL;
  bindings = json_object_get(config->document, "operation_bindings");
  return bindings && json_type(bindings) == JSON_ARRAY ? bindings : NULL;
}

int turbo_flow_resolved_config_operation_binding_count(const turbo_flow_resolved_config_t *config,
                                                       size_t *count) {
  json_value_t *bindings;
  if (count) *count = 0u;
  if (!config || !count) return SALTS_EINVAL;
  bindings = flow_operation_bindings(config);
  if (!bindings) return SALTS_OK;
  *count = json_array_size(bindings);
  return SALTS_OK;
}

static uint32_t flow_operation_u32(json_value_t *binding, const char *field) {
  return (uint32_t)json_number(json_object_get(binding, field));
}
static size_t flow_operation_size(json_value_t *binding, const char *field) {
  return (size_t)json_number(json_object_get(binding, field));
}

int turbo_flow_resolved_config_operation_binding_at(
    const turbo_flow_resolved_config_t *config, size_t index,
    turbo_flow_resolved_operation_binding_view_t *view) {
  size_t size;
  json_value_t *bindings, *binding, *resource, *permissions;
  const char *execution, *threading, *cancellation;
  if (!view || view->size < sizeof(*view)) return SALTS_EINVAL;
  size = view->size;
  memset(view, 0, sizeof(*view));
  view->size = size;
  if (!config) return SALTS_EINVAL;
  bindings = flow_operation_bindings(config);
  if (!bindings || index >= json_array_size(bindings)) return SALTS_ENOENT;
  binding = json_array_get(bindings, index);
  resource = json_object_get(binding, "resource");
  permissions = json_object_get(binding, "permissions");
  execution = json_string(json_object_get(binding, "execution"));
  threading = json_string(json_object_get(binding, "threading"));
  cancellation = json_string(json_object_get(binding, "cancellation"));
  view->operation = json_string(json_object_get(binding, "operation"));
  view->resource = resource ? json_string(resource) : NULL;
  view->plugin = json_string(json_object_get(binding, "plugin"));
  view->version = flow_operation_u32(binding, "version");
  view->input_schema = json_string(json_object_get(binding, "input_schema"));
  view->input_schema_version = flow_operation_u32(binding, "input_schema_version");
  view->output_schema = json_string(json_object_get(binding, "output_schema"));
  view->output_schema_version = flow_operation_u32(binding, "output_schema_version");
  view->execution = strcmp(execution, "thread") == 0 ? TURBO_FLOW_CONFIG_OPERATION_THREAD
                    : strcmp(execution, "coro") == 0 ? TURBO_FLOW_CONFIG_OPERATION_CORO
                                                     : TURBO_FLOW_CONFIG_OPERATION_INLINE;
  view->threading = strcmp(threading, "thread_safe") == 0 ? TURBO_FLOW_CONFIG_OPERATION_THREAD_SAFE
                                                          : TURBO_FLOW_CONFIG_OPERATION_OWNER;
  view->cancellation = strcmp(cancellation, "cooperative") == 0
                           ? TURBO_FLOW_CONFIG_OPERATION_CANCEL_COOPERATIVE
                           : TURBO_FLOW_CONFIG_OPERATION_CANCEL_NONE;
  view->max_inflight = flow_operation_u32(binding, "max_inflight");
  view->max_input_bytes = flow_operation_size(binding, "max_input_bytes");
  view->max_result_bytes = flow_operation_size(binding, "max_result_bytes");
  view->max_retained_bytes = flow_operation_size(binding, "max_retained_bytes");
  view->max_steps = flow_operation_u32(binding, "max_steps");
  view->deadline_ms = (uint64_t)json_number(json_object_get(binding, "deadline_ms"));
  view->permission_count = json_array_size(permissions);
  return SALTS_OK;
}

int turbo_flow_resolved_config_operation_binding_permission_at(
    const turbo_flow_resolved_config_t *config, size_t binding_index, size_t permission_index,
    const char **permission) {
  json_value_t *bindings, *binding, *permissions;
  if (permission) *permission = NULL;
  if (!config || !permission) return SALTS_EINVAL;
  bindings = flow_operation_bindings(config);
  if (!bindings || binding_index >= json_array_size(bindings)) return SALTS_ENOENT;
  binding = json_array_get(bindings, binding_index);
  permissions = json_object_get(binding, "permissions");
  if (permission_index >= json_array_size(permissions)) return SALTS_ENOENT;
  *permission = json_string(json_array_get(permissions, permission_index));
  return SALTS_OK;
}
