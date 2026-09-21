#include "turbo_flow_resolved_config.h"

#include "flow_config_internal.h"

#include "salts_error.h"

#include <cyaml/cyaml_json_adapter.h>

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_config_ingress_s {
  uint32_t workers;
  size_t queue_capacity;
  size_t max_message_bytes;
  size_t max_inflight_bytes;
} flow_config_ingress_t;

int flow_config_error(turbo_flow_config_error_t *error, int status, const char *path,
                      const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s", path ? path : "$");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int flow_config_key_allowed(const char *key, const char *const *allowed, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    if (strcmp(key, allowed[i]) == 0) return 1;
  }
  return 0;
}

int flow_config_object_keys(const json_value_t *object, const char *path,
                            const char *const *allowed, size_t count,
                            turbo_flow_config_error_t *error) {
  if (!object || json_type(object) != JSON_OBJECT) {
    return flow_config_error(error, SALTS_EINVAL, path, "expected mapping");
  }
  for (size_t i = 0; i < json_object_size(object); ++i) {
    const char *key = json_object_key(object, i);
    if (!key || !flow_config_key_allowed(key, allowed, count)) {
      char field_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      (void)snprintf(field_path, sizeof(field_path), "%s.%s", path, key ? key : "?");
      return flow_config_error(error, SALTS_EINVAL, field_path, "unknown field");
    }
  }
  return SALTS_OK;
}

static int flow_config_add_clone(json_value_t *object, const char *key, const json_value_t *value) {
  json_value_t *copy = json_clone(value);
  if (!copy) return SALTS_ENOMEM;
  if (!json_object_add_checked(object, key, copy)) {
    json_free(copy);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int flow_config_add_string(json_value_t *object, const char *key, const char *value) {
  json_value_t *child;
  if (!object || !key || !value) return SALTS_EINVAL;
  child = json_create_string(value);
  if (!child) return SALTS_ENOMEM;
  if (!json_object_add_checked(object, key, child)) {
    json_free(child);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int flow_config_add_number(json_value_t *object, const char *key, double value) {
  json_value_t *child;
  if (!object || !key) return SALTS_EINVAL;
  child = json_create_number(value);
  if (!child) return SALTS_ENOMEM;
  if (!json_object_add_checked(object, key, child)) {
    json_free(child);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int flow_config_merge(json_value_t *merged, json_value_t *sources,
                             const json_value_t *fields, const char *source, const char *path,
                             turbo_flow_config_error_t *error) {
  if (!fields || json_type(fields) != JSON_OBJECT)
    return flow_config_error(error, SALTS_EINVAL, path, "expected field mapping");
  for (size_t i = 0; i < json_object_size(fields); ++i) {
    const char *key = json_object_key(fields, i);
    json_value_t *value = json_object_value(fields, i);
    if (!key || !key[0]) return flow_config_error(error, SALTS_EINVAL, path, "empty field name");
    if (json_object_get(merged, key)) {
      char field_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      (void)snprintf(field_path, sizeof(field_path), "%s.%s", path, key);
      return flow_config_error(error, SALTS_EALREADY, field_path, "field has more than one source");
    }
    if (flow_config_add_clone(merged, key, value) != SALTS_OK) return SALTS_ENOMEM;
    if (flow_config_add_string(sources, key, source) != SALTS_OK) return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int flow_config_validate_profiles(const json_value_t *profiles, const json_value_t *adapters,
                                         const json_value_t *channels,
                                         turbo_flow_config_error_t *error) {
  if (!profiles) return SALTS_OK;
  if (json_type(profiles) != JSON_OBJECT)
    return flow_config_error(error, SALTS_EINVAL, "$.profiles", "expected mapping");
  for (size_t i = 0; i < json_object_size(profiles); ++i) {
    const char *name = json_object_key(profiles, i);
    json_value_t *profile = json_object_value(profiles, i);
    if (!profile || json_type(profile) != JSON_OBJECT)
      return flow_config_error(error, SALTS_EINVAL, "$.profiles", "profile must be a mapping");
    for (size_t field = 0; field < json_object_size(profile); ++field) {
      const char *parameter = json_object_key(profile, field);
      json_value_t *reference = json_object_value(profile, field);
      const char *adapter =
          reference && json_type(reference) == JSON_STRING ? json_string(reference) : NULL;
      const int adapter_found = adapter && json_object_get(adapters, adapter);
      const int channel_found = adapter && channels && json_object_get(channels, adapter);
      if (!adapter || (!adapter_found && !channel_found) || (adapter_found && channel_found)) {
        char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
        (void)snprintf(path, sizeof(path), "$.profiles.%s.%s", name, parameter);
        return flow_config_error(
            error,
            !adapter ? SALTS_EINVAL
                     : (adapter_found && channel_found ? SALTS_EALREADY : SALTS_ENOENT),
            path,
            !adapter ? "resource reference must be a string"
                     : (adapter_found && channel_found ? "ambiguous adapter/channel reference"
                                                       : "unresolved resource reference"));
      }
    }
  }
  return SALTS_OK;
}

static int flow_config_validate_fragments(const json_value_t *fragments,
                                          turbo_flow_config_error_t *error) {
  if (!fragments) return SALTS_OK;
  for (size_t category_index = 0; category_index < json_object_size(fragments); ++category_index) {
    const char *category = json_object_key(fragments, category_index);
    json_value_t *category_map = json_object_value(fragments, category_index);
    char category_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    (void)snprintf(category_path, sizeof(category_path), "$.fragments.%s", category);
    if (!category_map || json_type(category_map) != JSON_OBJECT)
      return flow_config_error(error, SALTS_EINVAL, category_path,
                               "fragment category must be a mapping");
    for (size_t fragment_index = 0; fragment_index < json_object_size(category_map);
         ++fragment_index) {
      const char *name = json_object_key(category_map, fragment_index);
      json_value_t *fields = json_object_value(category_map, fragment_index);
      char fragment_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      (void)snprintf(fragment_path, sizeof(fragment_path), "%s.%s", category_path,
                     name ? name : "?");
      if (!name || !name[0] || !fields || json_type(fields) != JSON_OBJECT ||
          json_object_size(fields) == 0u)
        return flow_config_error(error, SALTS_EINVAL, fragment_path,
                                 "named fragment must be a non-empty mapping");
    }
  }
  return SALTS_OK;
}

static int flow_config_plugin_string(const json_value_t *plugin, const char *field, size_t maximum,
                                     size_t index, turbo_flow_config_error_t *error) {
  json_value_t *value = plugin ? json_object_get(plugin, field) : NULL;
  const char *text = value && json_type(value) == JSON_STRING ? json_string(value) : NULL;
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  (void)snprintf(path, sizeof(path), "$.plugins[%zu].%s", index, field);
  if (!text || !text[0])
    return flow_config_error(error, SALTS_EINVAL, path, "expected non-empty string");
  if (strlen(text) > maximum)
    return flow_config_error(error, SALTS_ERANGE, path, "string exceeds configured ABI limit");
  return SALTS_OK;
}

static int flow_config_plugin_path_is_absolute(const char *path) {
  const size_t length = path ? strlen(path) : 0u;
  const unsigned char first = length > 0u ? (unsigned char)path[0] : 0u;
#ifdef _WIN32
  if (length >= 2u &&
      ((path[0] == '/' && path[1] == '/') || (path[0] == '\\' && path[1] == '\\')))
    return 1;
  if (length < 3u) return 0;
  const unsigned char drive = (unsigned char)path[1];
  const unsigned char separator = (unsigned char)path[2];
  return ((first >= (unsigned char)'A' && first <= (unsigned char)'Z') ||
          (first >= (unsigned char)'a' && first <= (unsigned char)'z')) &&
         drive == (unsigned char)':' &&
         (separator == (unsigned char)'/' || separator == (unsigned char)'\\');
#else
  return first == (unsigned char)'/';
#endif
}

static int flow_config_validate_plugins(const json_value_t *plugins,
                                        turbo_flow_config_error_t *error) {
  static const char *const plugin_keys[] = {"id", "version", "path"};
  if (!plugins) return SALTS_OK;
  if (json_type(plugins) != JSON_ARRAY)
    return flow_config_error(error, SALTS_EINVAL, "$.plugins", "expected ordered sequence");
  if (json_array_size(plugins) > TURBO_FLOW_CONFIG_PLUGIN_MAX_MODULES)
    return flow_config_error(error, SALTS_ENOSPC, "$.plugins",
                             "configured plugin module capacity is exceeded");
  for (size_t i = 0u; i < json_array_size(plugins); ++i) {
    json_value_t *plugin = json_array_get(plugins, i);
    json_value_t *id;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    int rc;
    (void)snprintf(path, sizeof(path), "$.plugins[%zu]", i);
    rc = flow_config_object_keys(plugin, path, plugin_keys,
                                 sizeof(plugin_keys) / sizeof(plugin_keys[0]), error);
    if (rc != SALTS_OK) return rc;
    rc = flow_config_plugin_string(plugin, "id", TURBO_FLOW_CONFIG_PLUGIN_ID_MAX, i, error);
    if (rc == SALTS_OK)
      rc = flow_config_plugin_string(plugin, "version", TURBO_FLOW_CONFIG_PLUGIN_VERSION_MAX, i,
                                     error);
    if (rc == SALTS_OK)
      rc = flow_config_plugin_string(plugin, "path", TURBO_FLOW_CONFIG_PLUGIN_PATH_MAX, i, error);
    if (rc != SALTS_OK) return rc;
    if (!flow_config_plugin_path_is_absolute(json_string(json_object_get(plugin, "path")))) {
      (void)snprintf(path, sizeof(path), "$.plugins[%zu].path", i);
      return flow_config_error(error, SALTS_EINVAL, path,
                               "plugin path must be absolute; search and fallback are forbidden");
    }
    id = json_object_get(plugin, "id");
    for (size_t prior = 0u; prior < i; ++prior) {
      json_value_t *other = json_array_get(plugins, prior);
      json_value_t *other_id = other ? json_object_get(other, "id") : NULL;
      if (other_id && strcmp(json_string(id), json_string(other_id)) == 0) {
        (void)snprintf(path, sizeof(path), "$.plugins[%zu].id", i);
        return flow_config_error(error, SALTS_EALREADY, path,
                                 "duplicate configured plugin identity");
      }
    }
  }
  return SALTS_OK;
}

static int flow_config_validate_channels(const json_value_t *channels,
                                         turbo_flow_config_error_t *error) {
  static const char *const channel_keys[] = {"kind", "config"};
  if (!channels) return SALTS_OK;
  if (json_type(channels) != JSON_OBJECT)
    return flow_config_error(error, SALTS_EINVAL, "$.channels", "expected mapping");
  for (size_t i = 0; i < json_object_size(channels); ++i) {
    const char *name = json_object_key(channels, i);
    json_value_t *channel = json_object_value(channels, i);
    json_value_t *kind;
    json_value_t *config;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    int rc;
    (void)snprintf(path, sizeof(path), "$.channels.%s", name ? name : "?");
    if (!name || !name[0])
      return flow_config_error(error, SALTS_EINVAL, "$.channels", "empty channel name");
    rc = flow_config_object_keys(channel, path, channel_keys,
                                 sizeof(channel_keys) / sizeof(channel_keys[0]), error);
    if (rc != SALTS_OK) return rc;
    kind = json_object_get(channel, "kind");
    config = json_object_get(channel, "config");
    if (!kind || json_type(kind) != JSON_STRING || !json_string(kind)[0])
      return flow_config_error(error, SALTS_EINVAL, path, "kind must be a non-empty string");
    if (!config || json_type(config) != JSON_OBJECT)
      return flow_config_error(error, SALTS_EINVAL, path, "config must be a mapping");
  }
  return SALTS_OK;
}

static int flow_config_read_runtime_integer(const json_value_t *value, const char *path,
                                            size_t maximum, size_t *result,
                                            turbo_flow_config_error_t *error) {
  double number;
  size_t converted;
  if (!value || !result) return SALTS_EINVAL;
  if (json_type(value) != JSON_NUMBER)
    return flow_config_error(error, SALTS_EINVAL, path, "expected integer");
  number = json_number(value);
  if (!isfinite(number) || number < 1.0 || number > (double)maximum)
    return flow_config_error(error, SALTS_ERANGE, path, "integer is outside the supported range");
  converted = (size_t)number;
  if ((double)converted != number)
    return flow_config_error(error, SALTS_EINVAL, path, "expected integer");
  *result = converted;
  return SALTS_OK;
}

static int flow_config_validate_runtime(const json_value_t *runtime,
                                        flow_config_ingress_t *async_ingress,
                                        turbo_flow_config_error_t *error) {
  static const char *const runtime_keys[] = {"ingress"};
  static const char *const ingress_keys[] = {"workers", "capacity", "max_message_bytes",
                                             "max_inflight_bytes"};
  json_value_t *ingress;
  json_value_t *workers;
  json_value_t *capacity;
  json_value_t *max_message_bytes;
  json_value_t *max_inflight_bytes;
  size_t value;
  int rc;
  if (!async_ingress) return SALTS_EINVAL;
  if (!runtime) return SALTS_OK;
  rc = flow_config_object_keys(runtime, "$.runtime", runtime_keys,
                               sizeof(runtime_keys) / sizeof(runtime_keys[0]), error);
  if (rc != SALTS_OK) return rc;
  ingress = json_object_get(runtime, "ingress");
  if (!ingress) return SALTS_OK;
  rc = flow_config_object_keys(ingress, "$.runtime.ingress", ingress_keys,
                               sizeof(ingress_keys) / sizeof(ingress_keys[0]), error);
  if (rc != SALTS_OK) return rc;
  workers = json_object_get(ingress, "workers");
  capacity = json_object_get(ingress, "capacity");
  max_message_bytes = json_object_get(ingress, "max_message_bytes");
  max_inflight_bytes = json_object_get(ingress, "max_inflight_bytes");
  if (workers) {
    rc = flow_config_read_runtime_integer(workers, "$.runtime.ingress.workers",
                                          TURBO_FLOW_CONFIG_INGRESS_MAX_WORKERS, &value, error);
    if (rc != SALTS_OK) return rc;
    async_ingress->workers = (uint32_t)value;
  }
  if (capacity) {
    rc = flow_config_read_runtime_integer(capacity, "$.runtime.ingress.capacity",
                                          TURBO_FLOW_CONFIG_INGRESS_MAX_CAPACITY, &value, error);
    if (rc != SALTS_OK) return rc;
    async_ingress->queue_capacity = value;
  }
  if (max_message_bytes) {
    rc = flow_config_read_runtime_integer(max_message_bytes, "$.runtime.ingress.max_message_bytes",
                                          TURBO_FLOW_CONFIG_INGRESS_MAX_MESSAGE_BYTES, &value,
                                          error);
    if (rc != SALTS_OK) return rc;
    async_ingress->max_message_bytes = value;
  }
  if (max_inflight_bytes) {
    rc = flow_config_read_runtime_integer(
        max_inflight_bytes, "$.runtime.ingress.max_inflight_bytes",
        TURBO_FLOW_CONFIG_INGRESS_MAX_INFLIGHT_BYTES, &value, error);
    if (rc != SALTS_OK) return rc;
    async_ingress->max_inflight_bytes = value;
  }
  if (async_ingress->max_message_bytes > async_ingress->max_inflight_bytes) {
    return flow_config_error(error, SALTS_ERANGE, "$.runtime.ingress.max_message_bytes",
                             "max_message_bytes must not exceed max_inflight_bytes");
  }
  return SALTS_OK;
}

static int flow_config_add_resolved_runtime(json_value_t *resolved,
                                            const flow_config_ingress_t *async_ingress) {
  json_value_t *runtime = NULL;
  json_value_t *ingress = NULL;
  int rc = SALTS_ENOMEM;
  if (!resolved || !async_ingress) return SALTS_EINVAL;
  runtime = json_create_object();
  ingress = json_create_object();
  if (!runtime || !ingress) goto done;
  rc = flow_config_add_number(ingress, "workers", (double)async_ingress->workers);
  if (rc != SALTS_OK) goto done;
  rc = flow_config_add_number(ingress, "capacity", (double)async_ingress->queue_capacity);
  if (rc != SALTS_OK) goto done;
  rc = flow_config_add_number(ingress, "max_message_bytes",
                              (double)async_ingress->max_message_bytes);
  if (rc != SALTS_OK) goto done;
  rc = flow_config_add_number(ingress, "max_inflight_bytes",
                              (double)async_ingress->max_inflight_bytes);
  if (rc != SALTS_OK) goto done;
  if (!json_object_add_checked(runtime, "ingress", ingress)) goto done;
  ingress = NULL;
  if (!json_object_add_checked(resolved, "runtime", runtime)) goto done;
  runtime = NULL;
  rc = SALTS_OK;

done:
  if (ingress) json_free(ingress);
  if (runtime) json_free(runtime);
  return rc;
}

static int flow_config_resolve_adapters(const json_value_t *input_adapters,
                                        const json_value_t *fragments, json_value_t *output,
                                        turbo_flow_config_error_t *error) {
  static const char *const adapter_keys[] = {"kind", "fragments", "config"};
  if (!input_adapters) return SALTS_OK;
  if (json_type(input_adapters) != JSON_OBJECT)
    return flow_config_error(error, SALTS_EINVAL, "$.adapters", "expected mapping");
  for (size_t i = 0; i < json_object_size(input_adapters); ++i) {
    const char *name = json_object_key(input_adapters, i);
    json_value_t *adapter = json_object_value(input_adapters, i);
    json_value_t *kind;
    json_value_t *refs;
    json_value_t *local;
    json_value_t *resolved = json_create_object();
    json_value_t *merged = json_create_object();
    json_value_t *sources = json_create_object();
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    int rc;
    (void)snprintf(path, sizeof(path), "$.adapters.%s", name ? name : "?");
    if (!resolved || !merged || !sources) {
      if (resolved) json_free(resolved);
      if (merged) json_free(merged);
      if (sources) json_free(sources);
      return SALTS_ENOMEM;
    }
    rc = name && name[0]
             ? flow_config_object_keys(adapter, path, adapter_keys,
                                       sizeof(adapter_keys) / sizeof(adapter_keys[0]), error)
             : flow_config_error(error, SALTS_EINVAL, "$.adapters", "empty adapter name");
    kind = adapter ? json_object_get(adapter, "kind") : NULL;
    refs = adapter ? json_object_get(adapter, "fragments") : NULL;
    local = adapter ? json_object_get(adapter, "config") : NULL;
    if (rc == SALTS_OK && (!kind || json_type(kind) != JSON_STRING || !json_string(kind)[0]))
      rc = flow_config_error(error, SALTS_EINVAL, path, "kind must be a non-empty string");
    if (rc == SALTS_OK && refs) {
      if (json_type(refs) != JSON_OBJECT)
        rc = flow_config_error(error, SALTS_EINVAL, path, "fragments must be a mapping");
      for (size_t ref = 0; rc == SALTS_OK && ref < json_object_size(refs); ++ref) {
        const char *category = json_object_key(refs, ref);
        json_value_t *reference = json_object_value(refs, ref);
        const char *fragment_name =
            reference && json_type(reference) == JSON_STRING ? json_string(reference) : NULL;
        json_value_t *category_map = fragments ? json_object_get(fragments, category) : NULL;
        json_value_t *fields =
            fragment_name && category_map ? json_object_get(category_map, fragment_name) : NULL;
        char source[256];
        if (!fragment_name || !fields) {
          rc = flow_config_error(error, fragment_name ? SALTS_ENOENT : SALTS_EINVAL, path,
                                 "invalid or unresolved fragment reference");
          break;
        }
        (void)snprintf(source, sizeof(source), "fragment.%s.%s", category, fragment_name);
        rc = flow_config_merge(merged, sources, fields, source, path, error);
      }
    }
    if (rc == SALTS_OK && local)
      rc = flow_config_merge(merged, sources, local, "adapter.config", path, error);
    if (rc == SALTS_OK && !local && !refs)
      rc = flow_config_error(error, SALTS_EINVAL, path, "config or fragments is required");
    if (rc == SALTS_OK) {
      rc = flow_config_add_string(resolved, "kind", json_string(kind));
      if (rc == SALTS_OK) {
        if (json_object_add_checked(resolved, "config", merged)) merged = NULL;
        else rc = SALTS_ENOMEM;
      }
      if (rc == SALTS_OK) {
        if (json_object_add_checked(resolved, "sources", sources)) sources = NULL;
        else rc = SALTS_ENOMEM;
      }
      if (rc == SALTS_OK) {
        if (json_object_add_checked(output, name, resolved)) resolved = NULL;
        else rc = SALTS_ENOMEM;
      }
    }
    if (resolved) json_free(resolved);
    if (merged) json_free(merged);
    if (sources) json_free(sources);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

int turbo_flow_config_resolve_yaml(const char *yaml, size_t yaml_len,
                                   turbo_flow_resolved_config_t **out,
                                   turbo_flow_config_error_t *error) {
  static const char *const root_keys[] = {"version",  "plugins",  "runtime",  "profiles",
                                          "fragments", "channels", "adapters",
                                          "operation_bindings", "materializer_bindings"};
  static const char *const fragment_keys[] = {"connection", "timer", "thread", "coro"};
  cyaml_doc_t *yaml_doc = NULL;
  json_value_t *input = NULL;
  json_value_t *resolved = NULL;
  json_value_t *resolved_adapters = NULL;
  turbo_flow_resolved_config_t *config = NULL;
  json_value_t *version;
  json_value_t *plugins;
  json_value_t *runtime;
  json_value_t *profiles;
  json_value_t *fragments;
  json_value_t *channels;
  json_value_t *adapters;
  json_value_t *operation_bindings;
  json_value_t *materializer_bindings;
  flow_config_ingress_t async_ingress = {TURBO_FLOW_CONFIG_INGRESS_DEFAULT_WORKERS,
                                         TURBO_FLOW_CONFIG_INGRESS_DEFAULT_CAPACITY,
                                         TURBO_FLOW_CONFIG_INGRESS_DEFAULT_MAX_MESSAGE_BYTES,
                                         TURBO_FLOW_CONFIG_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES};
  int rc;
  if (out) *out = NULL;
  if (!yaml || yaml_len == 0u || !out || !error || error->size < sizeof(*error))
    return SALTS_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  yaml_doc = cyaml_parse(yaml, yaml_len, NULL, NULL);
  if (!yaml_doc) return flow_config_error(error, SALTS_EINVAL, "$", "invalid YAML document");
  input = json_value_from_cyaml(yaml_doc);
  if (!input) {
    rc = flow_config_error(error, SALTS_EINVAL, "$", "YAML cannot be represented as JSON");
    goto done;
  }
  rc = flow_config_object_keys(input, "$", root_keys, sizeof(root_keys) / sizeof(root_keys[0]),
                               error);
  version = json_object_get(input, "version");
  plugins = json_object_get(input, "plugins");
  runtime = json_object_get(input, "runtime");
  profiles = json_object_get(input, "profiles");
  fragments = json_object_get(input, "fragments");
  channels = json_object_get(input, "channels");
  adapters = json_object_get(input, "adapters");
  operation_bindings = json_object_get(input, "operation_bindings");
  materializer_bindings = json_object_get(input, "materializer_bindings");
  if (rc == SALTS_OK &&
      (!version || json_type(version) != JSON_NUMBER || json_number(version) != 1.0))
    rc = flow_config_error(error, SALTS_EINVAL, "$.version", "version must be integer 1");
  if (rc == SALTS_OK) rc = flow_config_validate_plugins(plugins, error);
  if (rc == SALTS_OK) rc = flow_config_validate_runtime(runtime, &async_ingress, error);
  if (rc == SALTS_OK && fragments)
    rc = flow_config_object_keys(fragments, "$.fragments", fragment_keys,
                                 sizeof(fragment_keys) / sizeof(fragment_keys[0]), error);
  if (rc == SALTS_OK) rc = flow_config_validate_fragments(fragments, error);
  if (rc == SALTS_OK) rc = flow_config_validate_channels(channels, error);
  if (rc == SALTS_OK)
    rc = flow_config_validate_operation_bindings(operation_bindings, channels, error);
  if (rc == SALTS_OK)
    rc = flow_config_validate_materializer_bindings(materializer_bindings, error);
  if (rc != SALTS_OK) goto done;
  resolved = json_create_object();
  resolved_adapters = json_create_object();
  if (!resolved || !resolved_adapters) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  rc = flow_config_add_number(resolved, "version", 1.0);
  if (rc != SALTS_OK) goto done;
  if (plugins && flow_config_add_clone(resolved, "plugins", plugins) != SALTS_OK) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  rc = flow_config_add_resolved_runtime(resolved, &async_ingress);
  if (rc != SALTS_OK) goto done;
  if (profiles && flow_config_add_clone(resolved, "profiles", profiles) != SALTS_OK) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  if (channels && flow_config_add_clone(resolved, "channels", channels) != SALTS_OK) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  if (operation_bindings &&
      flow_config_add_clone(resolved, "operation_bindings", operation_bindings) != SALTS_OK) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  if (materializer_bindings &&
      flow_config_add_clone(resolved, "materializer_bindings", materializer_bindings) != SALTS_OK) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  rc = flow_config_resolve_adapters(adapters, fragments, resolved_adapters, error);
  if (rc != SALTS_OK) goto done;
  rc = flow_config_validate_profiles(profiles, resolved_adapters, channels, error);
  if (rc != SALTS_OK) goto done;
  if (!json_object_add_checked(resolved, "adapters", resolved_adapters)) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  resolved_adapters = NULL;
  config = (turbo_flow_resolved_config_t *)calloc(1, sizeof(*config));
  if (!config) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  config->document = resolved;
  resolved = NULL;
  config->json = json_serialize(config->document, &config->json_len);
  if (!config->json) {
    rc = SALTS_ENOMEM;
    goto done;
  }
  *out = config;
  config = NULL;
  rc = SALTS_OK;

done:
  turbo_flow_resolved_config_destroy(config);
  if (resolved_adapters) json_free(resolved_adapters);
  if (resolved) json_free(resolved);
  if (input) json_free(input);
  cyaml_free(yaml_doc);
  if (rc != SALTS_OK && error->status == SALTS_OK)
    (void)flow_config_error(error, rc, "$", rc == SALTS_ENOMEM ? "out of memory" : "error");
  return rc;
}

void turbo_flow_resolved_config_destroy(turbo_flow_resolved_config_t *config) {
  if (!config) return;
  if (config->json) json_serialize_free(config->json);
  json_free(config->document);
  free(config);
}

const char *turbo_flow_resolved_config_json(const turbo_flow_resolved_config_t *config,
                                            size_t *json_len) {
  if (json_len) *json_len = config ? config->json_len : 0u;
  return config ? config->json : NULL;
}

int turbo_flow_resolved_config_plugin_count(const turbo_flow_resolved_config_t *config,
                                            size_t *count) {
  json_value_t *plugins;
  if (count) *count = 0u;
  if (!config || !config->document || !count) return SALTS_EINVAL;
  plugins = json_object_get(config->document, "plugins");
  if (!plugins) return SALTS_OK;
  if (json_type(plugins) != JSON_ARRAY) return SALTS_EPROTO;
  *count = json_array_size(plugins);
  return SALTS_OK;
}

int turbo_flow_resolved_config_plugin_at(const turbo_flow_resolved_config_t *config, size_t index,
                                         turbo_flow_resolved_plugin_view_t *view) {
  json_value_t *plugins;
  json_value_t *plugin;
  json_value_t *id;
  json_value_t *version;
  json_value_t *path;
  if (view && view->size == sizeof(*view))
    *view = (turbo_flow_resolved_plugin_view_t)TURBO_FLOW_RESOLVED_PLUGIN_VIEW_INIT;
  if (!config || !config->document || !view || view->size != sizeof(*view)) return SALTS_EINVAL;
  plugins = json_object_get(config->document, "plugins");
  if (!plugins) return SALTS_ENOENT;
  if (json_type(plugins) != JSON_ARRAY) return SALTS_EPROTO;
  if (index >= json_array_size(plugins)) return SALTS_ENOENT;
  plugin = json_array_get(plugins, index);
  id = plugin ? json_object_get(plugin, "id") : NULL;
  version = plugin ? json_object_get(plugin, "version") : NULL;
  path = plugin ? json_object_get(plugin, "path") : NULL;
  if (!id || json_type(id) != JSON_STRING || !version || json_type(version) != JSON_STRING ||
      !path || json_type(path) != JSON_STRING)
    return SALTS_EPROTO;
  view->id = json_string(id);
  view->version = json_string(version);
  view->path = json_string(path);
  return SALTS_OK;
}

int turbo_flow_resolved_config_profile_adapter(const turbo_flow_resolved_config_t *config,
                                               const char *profile, const char *parameter,
                                               const char **adapter_name) {
  json_value_t *profiles;
  json_value_t *profile_value;
  json_value_t *reference;
  json_value_t *adapters;
  const char *name;
  if (adapter_name) *adapter_name = NULL;
  if (!config || !config->document || !profile || !profile[0] || !parameter || !parameter[0] ||
      !adapter_name) {
    return SALTS_EINVAL;
  }
  profiles = json_object_get(config->document, "profiles");
  profile_value = profiles ? json_object_get(profiles, profile) : NULL;
  reference = profile_value ? json_object_get(profile_value, parameter) : NULL;
  if (!reference || json_type(reference) != JSON_STRING) return SALTS_ENOENT;
  name = json_string(reference);
  adapters = json_object_get(config->document, "adapters");
  if (!name || !name[0] || !adapters || !json_object_get(adapters, name)) {
    return SALTS_ENOENT;
  }
  *adapter_name = name;
  return SALTS_OK;
}

int turbo_flow_resolved_config_profile_adapter_optional(const turbo_flow_resolved_config_t *config,
                                                        const char *profile, const char *parameter,
                                                        const char **adapter_name) {
  json_value_t *profiles;
  json_value_t *profile_value;
  json_value_t *reference;
  json_value_t *adapters;
  const char *name;
  if (adapter_name) *adapter_name = NULL;
  if (!config || !config->document || !profile || !profile[0] || !parameter || !parameter[0] ||
      !adapter_name) {
    return SALTS_EINVAL;
  }
  profiles = json_object_get(config->document, "profiles");
  profile_value = profiles ? json_object_get(profiles, profile) : NULL;
  if (!profile_value || json_type(profile_value) != JSON_OBJECT) return SALTS_ENOENT;
  reference = json_object_get(profile_value, parameter);
  if (!reference) return SALTS_OK;
  if (json_type(reference) != JSON_STRING) return SALTS_EPROTO;
  name = json_string(reference);
  adapters = json_object_get(config->document, "adapters");
  if (!name || !name[0] || !adapters || !json_object_get(adapters, name)) {
    return SALTS_EINVAL;
  }
  *adapter_name = name;
  return SALTS_OK;
}

int turbo_flow_resolved_config_profile_channel(const turbo_flow_resolved_config_t *config,
                                               const char *profile, const char *parameter,
                                               const char **channel_name) {
  json_value_t *profiles;
  json_value_t *profile_value;
  json_value_t *reference;
  json_value_t *channels;
  const char *name;
  if (channel_name) *channel_name = NULL;
  if (!config || !config->document || !profile || !profile[0] || !parameter || !parameter[0] ||
      !channel_name) {
    return SALTS_EINVAL;
  }
  profiles = json_object_get(config->document, "profiles");
  profile_value = profiles ? json_object_get(profiles, profile) : NULL;
  reference = profile_value ? json_object_get(profile_value, parameter) : NULL;
  if (!reference || json_type(reference) != JSON_STRING) return SALTS_ENOENT;
  name = json_string(reference);
  channels = json_object_get(config->document, "channels");
  if (!name || !name[0] || !channels || !json_object_get(channels, name)) {
    return SALTS_ENOENT;
  }
  *channel_name = name;
  return SALTS_OK;
}

int turbo_flow_resolved_config_profile_channel_optional(const turbo_flow_resolved_config_t *config,
                                                        const char *profile, const char *parameter,
                                                        const char **channel_name) {
  json_value_t *profiles;
  json_value_t *profile_value;
  json_value_t *reference;
  json_value_t *channels;
  const char *name;
  if (channel_name) *channel_name = NULL;
  if (!config || !config->document || !profile || !profile[0] || !parameter || !parameter[0] ||
      !channel_name) {
    return SALTS_EINVAL;
  }
  profiles = json_object_get(config->document, "profiles");
  profile_value = profiles ? json_object_get(profiles, profile) : NULL;
  if (!profile_value || json_type(profile_value) != JSON_OBJECT) return SALTS_ENOENT;
  reference = json_object_get(profile_value, parameter);
  if (!reference) return SALTS_OK;
  if (json_type(reference) != JSON_STRING) return SALTS_EPROTO;
  name = json_string(reference);
  channels = json_object_get(config->document, "channels");
  if (!name || !name[0] || !channels || !json_object_get(channels, name)) {
    return SALTS_EINVAL;
  }
  *channel_name = name;
  return SALTS_OK;
}

int turbo_flow_resolved_config_channel(const turbo_flow_resolved_config_t *config, const char *name,
                                       turbo_flow_resolved_channel_view_t *view) {
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  if (view && view->size >= sizeof(*view))
    *view = (turbo_flow_resolved_channel_view_t)TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
  if (!config || !config->document || !name || !name[0] || !view || view->size < sizeof(*view))
    return SALTS_EINVAL;
  channels = json_object_get(config->document, "channels");
  channel = channels ? json_object_get(channels, name) : NULL;
  if (!channel || json_type(channel) != JSON_OBJECT) return SALTS_ENOENT;
  kind = json_object_get(channel, "kind");
  fields = json_object_get(channel, "config");
  if (!kind || json_type(kind) != JSON_STRING || !json_string(kind)[0] || !fields ||
      json_type(fields) != JSON_OBJECT)
    return SALTS_EPROTO;
  view->name = name;
  view->kind = json_string(kind);
  view->config = fields;
  return SALTS_OK;
}

int turbo_flow_resolved_channel_get_string(const turbo_flow_resolved_channel_view_t *view,
                                           const char *field, const char **value) {
  json_value_t *entry;
  if (value) *value = NULL;
  if (!view || view->size < sizeof(*view) || !view->config || !field || !field[0] || !value)
    return SALTS_EINVAL;
  entry = json_object_get((const json_value_t *)view->config, field);
  if (!entry) return SALTS_ENOENT;
  if (json_type(entry) != JSON_STRING) return SALTS_EINVAL;
  *value = json_string(entry);
  return *value && (*value)[0] ? SALTS_OK : SALTS_EINVAL;
}

int turbo_flow_resolved_config_preflight_adapter_kinds(const turbo_flow_resolved_config_t *config,
                                                       const char *const *enabled_kinds,
                                                       size_t enabled_kind_count,
                                                       turbo_flow_config_error_t *error) {
  json_value_t *adapters;
  if (!config || !config->document || (enabled_kind_count > 0u && !enabled_kinds) || !error ||
      error->size < sizeof(*error))
    return SALTS_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  adapters = json_object_get(config->document, "adapters");
  if (!adapters || json_type(adapters) != JSON_OBJECT)
    return flow_config_error(error, SALTS_EPROTO, "$.adapters", "resolved adapter map is invalid");
  for (size_t i = 0u; i < json_object_size(adapters); ++i) {
    const char *name = json_object_key(adapters, i);
    json_value_t *adapter = json_object_value(adapters, i);
    json_value_t *kind = adapter ? json_object_get(adapter, "kind") : NULL;
    const char *kind_name = kind && json_type(kind) == JSON_STRING ? json_string(kind) : NULL;
    int enabled = 0;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    if (!name || !name[0] || !kind_name || !kind_name[0])
      return flow_config_error(error, SALTS_EPROTO, "$.adapters",
                               "resolved adapter identity is invalid");
    for (size_t candidate = 0u; candidate < enabled_kind_count; ++candidate) {
      if (!enabled_kinds[candidate] || !enabled_kinds[candidate][0]) return SALTS_EINVAL;
      if (strcmp(kind_name, enabled_kinds[candidate]) == 0) {
        enabled = 1;
        break;
      }
    }
    if (!enabled) {
      (void)snprintf(path, sizeof(path), "$.adapters.%s.kind", name);
      return flow_config_error(error, SALTS_ENOTSUP, path,
                               "adapter kind is disabled in this host build");
    }
  }
  return SALTS_OK;
}

int turbo_flow_resolved_config_adapter(const turbo_flow_resolved_config_t *config,
                                       const char *adapter_name,
                                       turbo_flow_resolved_adapter_view_t *view) {
  json_value_t *adapters;
  json_value_t *adapter;
  json_value_t *kind;
  json_value_t *fields;
  if (!config || !config->document || !adapter_name || !adapter_name[0] || !view ||
      view->size < sizeof(*view))
    return SALTS_EINVAL;
  adapters = json_object_get(config->document, "adapters");
  adapter = adapters ? json_object_get(adapters, adapter_name) : NULL;
  if (!adapter || json_type(adapter) != JSON_OBJECT) return SALTS_ENOENT;
  kind = json_object_get(adapter, "kind");
  fields = json_object_get(adapter, "config");
  if (!kind || json_type(kind) != JSON_STRING || !json_string(kind)[0] || !fields ||
      json_type(fields) != JSON_OBJECT)
    return SALTS_EPROTO;
  view->name = NULL;
  for (size_t i = 0u; i < json_object_size(adapters); ++i)
    if (strcmp(json_object_key(adapters, i), adapter_name) == 0) {
      view->name = json_object_key(adapters, i);
      break;
    }
  if (!view->name) return SALTS_EPROTO;
  view->kind = json_string(kind);
  view->config = fields;
  return SALTS_OK;
}

static const json_value_t *
flow_resolved_adapter_field(const turbo_flow_resolved_adapter_view_t *view, const char *field) {
  if (!view || view->size < sizeof(*view) || !view->config || !field || !field[0]) return NULL;
  return json_object_get((const json_value_t *)view->config, field);
}

size_t turbo_flow_resolved_adapter_field_count(const turbo_flow_resolved_adapter_view_t *view) {
  if (!view || view->size < sizeof(*view) || !view->config ||
      json_type((const json_value_t *)view->config) != JSON_OBJECT)
    return 0u;
  return json_object_size((const json_value_t *)view->config);
}

const char *turbo_flow_resolved_adapter_field_name(const turbo_flow_resolved_adapter_view_t *view,
                                                   size_t index) {
  if (!view || view->size < sizeof(*view) || !view->config ||
      json_type((const json_value_t *)view->config) != JSON_OBJECT)
    return NULL;
  return json_object_key((const json_value_t *)view->config, index);
}

int turbo_flow_resolved_adapter_field_type(const turbo_flow_resolved_adapter_view_t *view,
                                           const char *field,
                                           turbo_flow_config_value_type_t *type) {
  const json_value_t *value = flow_resolved_adapter_field(view, field);
  if (!type) return SALTS_EINVAL;
  if (!value) return SALTS_ENOENT;
  switch (json_type(value)) {
  case JSON_NULL:
    *type = TURBO_FLOW_CONFIG_NULL;
    break;
  case JSON_BOOL:
    *type = TURBO_FLOW_CONFIG_BOOL;
    break;
  case JSON_NUMBER:
    *type = TURBO_FLOW_CONFIG_NUMBER;
    break;
  case JSON_STRING:
    *type = TURBO_FLOW_CONFIG_STRING;
    break;
  case JSON_ARRAY:
    *type = TURBO_FLOW_CONFIG_ARRAY;
    break;
  case JSON_OBJECT:
    *type = TURBO_FLOW_CONFIG_OBJECT;
    break;
  default:
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_resolved_adapter_get_string(const turbo_flow_resolved_adapter_view_t *view,
                                           const char *field, const char **value) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (value) *value = NULL;
  if (!value) return SALTS_EINVAL;
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) != JSON_STRING) return SALTS_EINVAL;
  *value = json_string(field_value);
  return SALTS_OK;
}

int turbo_flow_resolved_adapter_get_bool(const turbo_flow_resolved_adapter_view_t *view,
                                         const char *field, int *value) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (!value) return SALTS_EINVAL;
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) != JSON_BOOL) return SALTS_EINVAL;
  *value = json_bool(field_value) != 0;
  return SALTS_OK;
}

static int flow_resolved_adapter_number(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *field, double *number) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (!number) return SALTS_EINVAL;
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) != JSON_NUMBER) return SALTS_EINVAL;
  *number = json_number(field_value);
  return isfinite(*number) ? SALTS_OK : SALTS_ERANGE;
}

int turbo_flow_resolved_adapter_get_u64(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *field, uint64_t *value) {
  const json_value_t *field_value;
  const char *text;
  char *end = NULL;
  unsigned long long parsed;
  double number;
  uint64_t converted;
  int rc;
  if (!value) return SALTS_EINVAL;
  field_value = flow_resolved_adapter_field(view, field);
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) == JSON_STRING) {
    text = json_string(field_value);
    if (!text || !text[0]) return SALTS_EINVAL;
    for (const char *cursor = text; *cursor; ++cursor) {
      if (*cursor < '0' || *cursor > '9') return SALTS_EINVAL;
    }
    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno == ERANGE || parsed > UINT64_MAX) return SALTS_ERANGE;
    if (!end || *end != '\0') return SALTS_EINVAL;
    *value = (uint64_t)parsed;
    return SALTS_OK;
  }
  rc = flow_resolved_adapter_number(view, field, &number);
  if (rc != SALTS_OK) return rc;
  if (number < 0.0 || number > 9007199254740991.0) return SALTS_ERANGE;
  converted = (uint64_t)number;
  if ((double)converted != number) return SALTS_EINVAL;
  *value = converted;
  return SALTS_OK;
}

int turbo_flow_resolved_adapter_get_i64(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *field, int64_t *value) {
  const json_value_t *field_value;
  const char *text;
  const char *digits;
  char *end = NULL;
  long long parsed;
  double number;
  int64_t converted;
  int rc;
  if (!value) return SALTS_EINVAL;
  field_value = flow_resolved_adapter_field(view, field);
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) == JSON_STRING) {
    text = json_string(field_value);
    if (!text || !text[0]) return SALTS_EINVAL;
    digits = text[0] == '-' ? text + 1 : text;
    if (!digits[0]) return SALTS_EINVAL;
    for (const char *cursor = digits; *cursor; ++cursor) {
      if (*cursor < '0' || *cursor > '9') return SALTS_EINVAL;
    }
    errno = 0;
    parsed = strtoll(text, &end, 10);
    if (errno == ERANGE || parsed < INT64_MIN || parsed > INT64_MAX) return SALTS_ERANGE;
    if (!end || *end != '\0') return SALTS_EINVAL;
    *value = (int64_t)parsed;
    return SALTS_OK;
  }
  rc = flow_resolved_adapter_number(view, field, &number);
  if (rc != SALTS_OK) return rc;
  if (number < -9007199254740991.0 || number > 9007199254740991.0) return SALTS_ERANGE;
  converted = (int64_t)number;
  if ((double)converted != number) return SALTS_EINVAL;
  *value = converted;
  return SALTS_OK;
}

int turbo_flow_resolved_adapter_array_size(const turbo_flow_resolved_adapter_view_t *view,
                                           const char *field, size_t *size) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (!size) return SALTS_EINVAL;
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) != JSON_ARRAY) return SALTS_EINVAL;
  *size = json_array_size(field_value);
  return SALTS_OK;
}

int turbo_flow_resolved_adapter_array_string_at(const turbo_flow_resolved_adapter_view_t *view,
                                                const char *field, size_t index,
                                                const char **value) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  const json_value_t *item;
  if (value) *value = NULL;
  if (!value) return SALTS_EINVAL;
  if (!field_value) return SALTS_ENOENT;
  if (json_type(field_value) != JSON_ARRAY) return SALTS_EINVAL;
  item = json_array_get(field_value, index);
  if (!item) return SALTS_ENOENT;
  if (json_type(item) != JSON_STRING) return SALTS_EINVAL;
  *value = json_string(item);
  return SALTS_OK;
}
