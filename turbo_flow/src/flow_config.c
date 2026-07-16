#include "turbo_flow_config.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_resolved_config_s {
  turbo_json_doc_t *document;
  char *json;
  size_t json_len;
};

static int flow_config_error(turbo_flow_config_error_t *error, int status, const char *path,
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

static int flow_config_object_keys(const json_value_t *object, const char *path,
                                   const char *const *allowed, size_t count,
                                   turbo_flow_config_error_t *error) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT) {
    return flow_config_error(error, TURBO_EINVAL, path, "expected mapping");
  }
  for (size_t i = 0; i < turbo_json_object_size(object); ++i) {
    const char *key = turbo_json_object_key(object, i);
    if (!key || !flow_config_key_allowed(key, allowed, count)) {
      char field_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      (void)snprintf(field_path, sizeof(field_path), "%s.%s", path, key ? key : "?");
      return flow_config_error(error, TURBO_EINVAL, field_path, "unknown field");
    }
  }
  return TURBO_OK;
}

static int flow_config_add_clone(json_value_t *object, const char *key, const json_value_t *value) {
  json_value_t *copy = turbo_json_clone(value);
  if (!copy) return TURBO_ENOMEM;
  if (!turbo_json_object_add_checked(object, key, copy)) {
    turbo_free_json((turbo_json_doc_t **)&copy);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flow_config_merge(json_value_t *merged, json_value_t *sources,
                             const json_value_t *fields, const char *source, const char *path,
                             turbo_flow_config_error_t *error) {
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT)
    return flow_config_error(error, TURBO_EINVAL, path, "expected field mapping");
  for (size_t i = 0; i < turbo_json_object_size(fields); ++i) {
    const char *key = turbo_json_object_key(fields, i);
    json_value_t *value = turbo_json_object_value(fields, i);
    if (!key || !key[0]) return flow_config_error(error, TURBO_EINVAL, path, "empty field name");
    if (turbo_json_object_get(merged, key)) {
      char field_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      (void)snprintf(field_path, sizeof(field_path), "%s.%s", path, key);
      return flow_config_error(error, TURBO_EALREADY, field_path, "field has more than one source");
    }
    if (flow_config_add_clone(merged, key, value) != TURBO_OK) return TURBO_ENOMEM;
    turbo_json_object_set_string(sources, key, source);
  }
  return TURBO_OK;
}

static int flow_config_validate_profiles(const json_value_t *profiles, const json_value_t *adapters,
                                         const json_value_t *channels,
                                         turbo_flow_config_error_t *error) {
  if (!profiles) return TURBO_OK;
  if (turbo_json_type(profiles) != TURBO_JSON_OBJECT)
    return flow_config_error(error, TURBO_EINVAL, "$.profiles", "expected mapping");
  for (size_t i = 0; i < turbo_json_object_size(profiles); ++i) {
    const char *name = turbo_json_object_key(profiles, i);
    json_value_t *profile = turbo_json_object_value(profiles, i);
    if (!profile || turbo_json_type(profile) != TURBO_JSON_OBJECT)
      return flow_config_error(error, TURBO_EINVAL, "$.profiles", "profile must be a mapping");
    for (size_t field = 0; field < turbo_json_object_size(profile); ++field) {
      const char *parameter = turbo_json_object_key(profile, field);
      json_value_t *reference = turbo_json_object_value(profile, field);
      const char *adapter = reference && turbo_json_type(reference) == TURBO_JSON_STRING
                                ? turbo_json_string(reference)
                                : NULL;
      const int adapter_found = adapter && turbo_json_object_get(adapters, adapter);
      const int channel_found = adapter && channels && turbo_json_object_get(channels, adapter);
      if (!adapter || (!adapter_found && !channel_found) || (adapter_found && channel_found)) {
        char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
        (void)snprintf(path, sizeof(path), "$.profiles.%s.%s", name, parameter);
        return flow_config_error(
            error, !adapter ? TURBO_EINVAL
                            : (adapter_found && channel_found ? TURBO_EALREADY : TURBO_ENOENT),
            path, !adapter ? "resource reference must be a string"
                           : (adapter_found && channel_found ? "ambiguous adapter/channel reference"
                                                            : "unresolved resource reference"));
      }
    }
  }
  return TURBO_OK;
}

static int flow_config_validate_fragments(const json_value_t *fragments,
                                          turbo_flow_config_error_t *error) {
  if (!fragments) return TURBO_OK;
  for (size_t category_index = 0; category_index < turbo_json_object_size(fragments);
       ++category_index) {
    const char *category = turbo_json_object_key(fragments, category_index);
    json_value_t *category_map = turbo_json_object_value(fragments, category_index);
    char category_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    (void)snprintf(category_path, sizeof(category_path), "$.fragments.%s", category);
    if (!category_map || turbo_json_type(category_map) != TURBO_JSON_OBJECT)
      return flow_config_error(error, TURBO_EINVAL, category_path,
                               "fragment category must be a mapping");
    for (size_t fragment_index = 0; fragment_index < turbo_json_object_size(category_map);
         ++fragment_index) {
      const char *name = turbo_json_object_key(category_map, fragment_index);
      json_value_t *fields = turbo_json_object_value(category_map, fragment_index);
      char fragment_path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      (void)snprintf(fragment_path, sizeof(fragment_path), "%s.%s", category_path,
                     name ? name : "?");
      if (!name || !name[0] || !fields || turbo_json_type(fields) != TURBO_JSON_OBJECT ||
          turbo_json_object_size(fields) == 0u)
        return flow_config_error(error, TURBO_EINVAL, fragment_path,
                                 "named fragment must be a non-empty mapping");
    }
  }
  return TURBO_OK;
}

static int flow_config_validate_channels(const json_value_t *channels,
                                         turbo_flow_config_error_t *error) {
  static const char *const channel_keys[] = {"kind", "config"};
  if (!channels) return TURBO_OK;
  if (turbo_json_type(channels) != TURBO_JSON_OBJECT)
    return flow_config_error(error, TURBO_EINVAL, "$.channels", "expected mapping");
  for (size_t i = 0; i < turbo_json_object_size(channels); ++i) {
    const char *name = turbo_json_object_key(channels, i);
    json_value_t *channel = turbo_json_object_value(channels, i);
    json_value_t *kind;
    json_value_t *config;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    int rc;
    (void)snprintf(path, sizeof(path), "$.channels.%s", name ? name : "?");
    if (!name || !name[0])
      return flow_config_error(error, TURBO_EINVAL, "$.channels", "empty channel name");
    rc = flow_config_object_keys(channel, path, channel_keys,
                                 sizeof(channel_keys) / sizeof(channel_keys[0]), error);
    if (rc != TURBO_OK) return rc;
    kind = turbo_json_object_get(channel, "kind");
    config = turbo_json_object_get(channel, "config");
    if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING || !turbo_json_string(kind)[0])
      return flow_config_error(error, TURBO_EINVAL, path, "kind must be a non-empty string");
    if (!config || turbo_json_type(config) != TURBO_JSON_OBJECT)
      return flow_config_error(error, TURBO_EINVAL, path, "config must be a mapping");
  }
  return TURBO_OK;
}

static int flow_config_read_runtime_integer(const json_value_t *value, const char *path,
                                            size_t maximum, size_t *result,
                                            turbo_flow_config_error_t *error) {
  double number;
  size_t converted;
  if (!value || !result) return TURBO_EINVAL;
  if (turbo_json_type(value) != TURBO_JSON_NUMBER)
    return flow_config_error(error, TURBO_EINVAL, path, "expected integer");
  number = turbo_json_number(value);
  if (!isfinite(number) || number < 1.0 || number > (double)maximum)
    return flow_config_error(error, TURBO_ERANGE, path, "integer is outside the supported range");
  converted = (size_t)number;
  if ((double)converted != number)
    return flow_config_error(error, TURBO_EINVAL, path, "expected integer");
  *result = converted;
  return TURBO_OK;
}

static int flow_config_validate_runtime(
    const json_value_t *runtime, turbo_flow_async_ingress_config_t *async_ingress,
    turbo_flow_config_error_t *error) {
  static const char *const runtime_keys[] = {"ingress"};
  static const char *const ingress_keys[] = {"workers", "capacity"};
  json_value_t *ingress;
  json_value_t *workers;
  json_value_t *capacity;
  size_t value;
  int rc;
  if (!async_ingress) return TURBO_EINVAL;
  if (!runtime) return TURBO_OK;
  rc = flow_config_object_keys(runtime, "$.runtime", runtime_keys,
                               sizeof(runtime_keys) / sizeof(runtime_keys[0]), error);
  if (rc != TURBO_OK) return rc;
  ingress = turbo_json_object_get(runtime, "ingress");
  if (!ingress) return TURBO_OK;
  rc = flow_config_object_keys(ingress, "$.runtime.ingress", ingress_keys,
                               sizeof(ingress_keys) / sizeof(ingress_keys[0]), error);
  if (rc != TURBO_OK) return rc;
  workers = turbo_json_object_get(ingress, "workers");
  capacity = turbo_json_object_get(ingress, "capacity");
  if (workers) {
    rc = flow_config_read_runtime_integer(workers, "$.runtime.ingress.workers",
                                          TURBO_FLOW_ASYNC_INGRESS_MAX_WORKERS, &value, error);
    if (rc != TURBO_OK) return rc;
    async_ingress->workers = (uint32_t)value;
  }
  if (capacity) {
    rc = flow_config_read_runtime_integer(capacity, "$.runtime.ingress.capacity",
                                          TURBO_FLOW_ASYNC_INGRESS_MAX_CAPACITY, &value, error);
    if (rc != TURBO_OK) return rc;
    async_ingress->queue_capacity = value;
  }
  return TURBO_OK;
}

static int flow_config_add_resolved_runtime(
    json_value_t *resolved, const turbo_flow_async_ingress_config_t *async_ingress) {
  json_value_t *runtime = NULL;
  json_value_t *ingress = NULL;
  int rc = TURBO_ENOMEM;
  if (!resolved || !async_ingress) return TURBO_EINVAL;
  runtime = turbo_json_create_object();
  ingress = turbo_json_create_object();
  if (!runtime || !ingress) goto done;
  turbo_json_object_set_number(ingress, "workers", (double)async_ingress->workers);
  turbo_json_object_set_number(ingress, "capacity", (double)async_ingress->queue_capacity);
  if (!turbo_json_object_add_checked(runtime, "ingress", ingress)) goto done;
  ingress = NULL;
  if (!turbo_json_object_add_checked(resolved, "runtime", runtime)) goto done;
  runtime = NULL;
  rc = TURBO_OK;

done:
  if (ingress) turbo_free_json((turbo_json_doc_t **)&ingress);
  if (runtime) turbo_free_json((turbo_json_doc_t **)&runtime);
  return rc;
}

static int flow_config_resolve_adapters(const json_value_t *input_adapters,
                                        const json_value_t *fragments, json_value_t *output,
                                        turbo_flow_config_error_t *error) {
  static const char *const adapter_keys[] = {"kind", "fragments", "config"};
  if (!input_adapters || turbo_json_type(input_adapters) != TURBO_JSON_OBJECT)
    return flow_config_error(error, TURBO_EINVAL, "$.adapters", "expected mapping");
  for (size_t i = 0; i < turbo_json_object_size(input_adapters); ++i) {
    const char *name = turbo_json_object_key(input_adapters, i);
    json_value_t *adapter = turbo_json_object_value(input_adapters, i);
    json_value_t *kind;
    json_value_t *refs;
    json_value_t *local;
    json_value_t *resolved = turbo_json_create_object();
    json_value_t *merged = turbo_json_create_object();
    json_value_t *sources = turbo_json_create_object();
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    int rc;
    (void)snprintf(path, sizeof(path), "$.adapters.%s", name ? name : "?");
    if (!resolved || !merged || !sources) return TURBO_ENOMEM;
    rc = name && name[0]
             ? flow_config_object_keys(adapter, path, adapter_keys,
                                       sizeof(adapter_keys) / sizeof(adapter_keys[0]), error)
             : flow_config_error(error, TURBO_EINVAL, "$.adapters", "empty adapter name");
    kind = adapter ? turbo_json_object_get(adapter, "kind") : NULL;
    refs = adapter ? turbo_json_object_get(adapter, "fragments") : NULL;
    local = adapter ? turbo_json_object_get(adapter, "config") : NULL;
    if (rc == TURBO_OK &&
        (!kind || turbo_json_type(kind) != TURBO_JSON_STRING || !turbo_json_string(kind)[0]))
      rc = flow_config_error(error, TURBO_EINVAL, path, "kind must be a non-empty string");
    if (rc == TURBO_OK && refs) {
      if (turbo_json_type(refs) != TURBO_JSON_OBJECT)
        rc = flow_config_error(error, TURBO_EINVAL, path, "fragments must be a mapping");
      for (size_t ref = 0; rc == TURBO_OK && ref < turbo_json_object_size(refs); ++ref) {
        const char *category = turbo_json_object_key(refs, ref);
        json_value_t *reference = turbo_json_object_value(refs, ref);
        const char *fragment_name = reference && turbo_json_type(reference) == TURBO_JSON_STRING
                                        ? turbo_json_string(reference)
                                        : NULL;
        json_value_t *category_map = fragments ? turbo_json_object_get(fragments, category) : NULL;
        json_value_t *fields = fragment_name && category_map
                                   ? turbo_json_object_get(category_map, fragment_name)
                                   : NULL;
        char source[256];
        if (!fragment_name || !fields) {
          rc = flow_config_error(error, fragment_name ? TURBO_ENOENT : TURBO_EINVAL, path,
                                 "invalid or unresolved fragment reference");
          break;
        }
        (void)snprintf(source, sizeof(source), "fragment.%s.%s", category, fragment_name);
        rc = flow_config_merge(merged, sources, fields, source, path, error);
      }
    }
    if (rc == TURBO_OK && local)
      rc = flow_config_merge(merged, sources, local, "adapter.config", path, error);
    if (rc == TURBO_OK && !local && !refs)
      rc = flow_config_error(error, TURBO_EINVAL, path, "config or fragments is required");
    if (rc == TURBO_OK) {
      turbo_json_object_set_string(resolved, "kind", turbo_json_string(kind));
      turbo_json_object_add(resolved, "config", merged);
      turbo_json_object_add(resolved, "sources", sources);
      merged = NULL;
      sources = NULL;
      if (!turbo_json_object_add_checked(output, name, resolved)) rc = TURBO_ENOMEM;
      else resolved = NULL;
    }
    if (resolved) turbo_free_json((turbo_json_doc_t **)&resolved);
    if (merged) turbo_free_json((turbo_json_doc_t **)&merged);
    if (sources) turbo_free_json((turbo_json_doc_t **)&sources);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

int turbo_flow_config_resolve_yaml(const char *yaml, size_t yaml_len,
                                   turbo_flow_resolved_config_t **out,
                                   turbo_flow_config_error_t *error) {
  static const char *const root_keys[] = {"version", "runtime", "profiles", "fragments",
                                          "channels", "adapters"};
  static const char *const fragment_keys[] = {"connection", "timer", "thread", "coro"};
  turbo_yaml_doc_t *yaml_doc = NULL;
  json_value_t *input = NULL;
  json_value_t *resolved = NULL;
  json_value_t *resolved_adapters = NULL;
  turbo_flow_resolved_config_t *config = NULL;
  json_value_t *version;
  json_value_t *runtime;
  json_value_t *profiles;
  json_value_t *fragments;
  json_value_t *channels;
  json_value_t *adapters;
  turbo_flow_async_ingress_config_t async_ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  int rc;
  if (out) *out = NULL;
  if (!yaml || yaml_len == 0u || !out || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (turbo_parse_yaml((const uint8_t *)yaml, yaml_len, &yaml_doc) != TURBO_OK || !yaml_doc)
    return flow_config_error(error, TURBO_EINVAL, "$", "invalid YAML document");
  input = turbo_yaml_to_json(yaml_doc);
  if (!input) {
    rc = flow_config_error(error, TURBO_EINVAL, "$", "YAML cannot be represented as JSON");
    goto done;
  }
  rc = flow_config_object_keys(input, "$", root_keys, sizeof(root_keys) / sizeof(root_keys[0]),
                               error);
  version = turbo_json_object_get(input, "version");
  runtime = turbo_json_object_get(input, "runtime");
  profiles = turbo_json_object_get(input, "profiles");
  fragments = turbo_json_object_get(input, "fragments");
  channels = turbo_json_object_get(input, "channels");
  adapters = turbo_json_object_get(input, "adapters");
  if (rc == TURBO_OK && (!version || turbo_json_type(version) != TURBO_JSON_NUMBER ||
                         turbo_json_number(version) != 1.0))
    rc = flow_config_error(error, TURBO_EINVAL, "$.version", "version must be integer 1");
  if (rc == TURBO_OK) rc = flow_config_validate_runtime(runtime, &async_ingress, error);
  if (rc == TURBO_OK && fragments)
    rc = flow_config_object_keys(fragments, "$.fragments", fragment_keys,
                                 sizeof(fragment_keys) / sizeof(fragment_keys[0]), error);
  if (rc == TURBO_OK) rc = flow_config_validate_fragments(fragments, error);
  if (rc == TURBO_OK) rc = flow_config_validate_channels(channels, error);
  if (rc != TURBO_OK) goto done;
  resolved = turbo_json_create_object();
  resolved_adapters = turbo_json_create_object();
  if (!resolved || !resolved_adapters) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  turbo_json_object_set_number(resolved, "version", 1.0);
  rc = flow_config_add_resolved_runtime(resolved, &async_ingress);
  if (rc != TURBO_OK) goto done;
  if (profiles && flow_config_add_clone(resolved, "profiles", profiles) != TURBO_OK) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  if (channels && flow_config_add_clone(resolved, "channels", channels) != TURBO_OK) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flow_config_resolve_adapters(adapters, fragments, resolved_adapters, error);
  if (rc != TURBO_OK) goto done;
  rc = flow_config_validate_profiles(profiles, resolved_adapters, channels, error);
  if (rc != TURBO_OK) goto done;
  turbo_json_object_add(resolved, "adapters", resolved_adapters);
  resolved_adapters = NULL;
  config = (turbo_flow_resolved_config_t *)calloc(1, sizeof(*config));
  if (!config) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  config->document = resolved;
  resolved = NULL;
  config->json = turbo_json_serialize(config->document, &config->json_len);
  if (!config->json) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  *out = config;
  config = NULL;
  rc = TURBO_OK;

done:
  turbo_flow_resolved_config_destroy(config);
  if (resolved_adapters) turbo_free_json((turbo_json_doc_t **)&resolved_adapters);
  if (resolved) turbo_free_json((turbo_json_doc_t **)&resolved);
  if (input) turbo_free_json((turbo_json_doc_t **)&input);
  turbo_free_yaml(&yaml_doc);
  if (rc != TURBO_OK && error->status == TURBO_OK)
    (void)flow_config_error(error, rc, "$", rc == TURBO_ENOMEM ? "out of memory" : "error");
  return rc;
}

void turbo_flow_resolved_config_destroy(turbo_flow_resolved_config_t *config) {
  if (!config) return;
  if (config->json) turbo_json_serialize_free(config->json);
  turbo_free_json(&config->document);
  free(config);
}

const char *turbo_flow_resolved_config_json(const turbo_flow_resolved_config_t *config,
                                            size_t *json_len) {
  if (json_len) *json_len = config ? config->json_len : 0u;
  return config ? config->json : NULL;
}

int turbo_flow_resolved_config_runtime_ingress(const turbo_flow_resolved_config_t *config,
                                               turbo_flow_async_ingress_config_t *ingress) {
  turbo_flow_async_ingress_config_t resolved_ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  json_value_t *runtime;
  json_value_t *ingress_value;
  json_value_t *workers;
  json_value_t *capacity;
  size_t value;
  if (!config || !config->document || !ingress || ingress->size < sizeof(*ingress))
    return TURBO_EINVAL;
  runtime = turbo_json_object_get(config->document, "runtime");
  ingress_value = runtime ? turbo_json_object_get(runtime, "ingress") : NULL;
  workers = ingress_value ? turbo_json_object_get(ingress_value, "workers") : NULL;
  capacity = ingress_value ? turbo_json_object_get(ingress_value, "capacity") : NULL;
  if (!runtime || turbo_json_type(runtime) != TURBO_JSON_OBJECT || !ingress_value ||
      turbo_json_type(ingress_value) != TURBO_JSON_OBJECT || !workers || !capacity ||
      flow_config_read_runtime_integer(workers, "$.runtime.ingress.workers",
                                       TURBO_FLOW_ASYNC_INGRESS_MAX_WORKERS, &value, NULL) !=
          TURBO_OK)
    return TURBO_EPROTO;
  resolved_ingress.workers = (uint32_t)value;
  if (flow_config_read_runtime_integer(capacity, "$.runtime.ingress.capacity",
                                       TURBO_FLOW_ASYNC_INGRESS_MAX_CAPACITY, &value, NULL) !=
      TURBO_OK)
    return TURBO_EPROTO;
  resolved_ingress.queue_capacity = value;
  *ingress = resolved_ingress;
  return TURBO_OK;
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
    return TURBO_EINVAL;
  }
  profiles = turbo_json_object_get(config->document, "profiles");
  profile_value = profiles ? turbo_json_object_get(profiles, profile) : NULL;
  reference = profile_value ? turbo_json_object_get(profile_value, parameter) : NULL;
  if (!reference || turbo_json_type(reference) != TURBO_JSON_STRING) return TURBO_ENOENT;
  name = turbo_json_string(reference);
  adapters = turbo_json_object_get(config->document, "adapters");
  if (!name || !name[0] || !adapters || !turbo_json_object_get(adapters, name)) {
    return TURBO_ENOENT;
  }
  *adapter_name = name;
  return TURBO_OK;
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
    return TURBO_EINVAL;
  }
  profiles = turbo_json_object_get(config->document, "profiles");
  profile_value = profiles ? turbo_json_object_get(profiles, profile) : NULL;
  reference = profile_value ? turbo_json_object_get(profile_value, parameter) : NULL;
  if (!reference || turbo_json_type(reference) != TURBO_JSON_STRING) return TURBO_ENOENT;
  name = turbo_json_string(reference);
  channels = turbo_json_object_get(config->document, "channels");
  if (!name || !name[0] || !channels || !turbo_json_object_get(channels, name)) {
    return TURBO_ENOENT;
  }
  *channel_name = name;
  return TURBO_OK;
}

int turbo_flow_resolved_config_preflight_adapter_kinds(const turbo_flow_resolved_config_t *config,
                                                       const char *const *enabled_kinds,
                                                       size_t enabled_kind_count,
                                                       turbo_flow_config_error_t *error) {
  json_value_t *adapters;
  if (!config || !config->document || (enabled_kind_count > 0u && !enabled_kinds) || !error ||
      error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  adapters = turbo_json_object_get(config->document, "adapters");
  if (!adapters || turbo_json_type(adapters) != TURBO_JSON_OBJECT)
    return flow_config_error(error, TURBO_EPROTO, "$.adapters", "resolved adapter map is invalid");
  for (size_t i = 0u; i < turbo_json_object_size(adapters); ++i) {
    const char *name = turbo_json_object_key(adapters, i);
    json_value_t *adapter = turbo_json_object_value(adapters, i);
    json_value_t *kind = adapter ? turbo_json_object_get(adapter, "kind") : NULL;
    const char *kind_name =
        kind && turbo_json_type(kind) == TURBO_JSON_STRING ? turbo_json_string(kind) : NULL;
    int enabled = 0;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    if (!name || !name[0] || !kind_name || !kind_name[0])
      return flow_config_error(error, TURBO_EPROTO, "$.adapters",
                               "resolved adapter identity is invalid");
    for (size_t candidate = 0u; candidate < enabled_kind_count; ++candidate) {
      if (!enabled_kinds[candidate] || !enabled_kinds[candidate][0]) return TURBO_EINVAL;
      if (strcmp(kind_name, enabled_kinds[candidate]) == 0) {
        enabled = 1;
        break;
      }
    }
    if (!enabled) {
      (void)snprintf(path, sizeof(path), "$.adapters.%s.kind", name);
      return flow_config_error(error, TURBO_ENOTSUP, path,
                               "adapter kind is disabled in this host build");
    }
  }
  return TURBO_OK;
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
    return TURBO_EINVAL;
  adapters = turbo_json_object_get(config->document, "adapters");
  adapter = adapters ? turbo_json_object_get(adapters, adapter_name) : NULL;
  if (!adapter || turbo_json_type(adapter) != TURBO_JSON_OBJECT) return TURBO_ENOENT;
  kind = turbo_json_object_get(adapter, "kind");
  fields = turbo_json_object_get(adapter, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING || !turbo_json_string(kind)[0] ||
      !fields || turbo_json_type(fields) != TURBO_JSON_OBJECT)
    return TURBO_EPROTO;
  view->name = NULL;
  for (size_t i = 0u; i < turbo_json_object_size(adapters); ++i)
    if (strcmp(turbo_json_object_key(adapters, i), adapter_name) == 0) {
      view->name = turbo_json_object_key(adapters, i);
      break;
    }
  if (!view->name) return TURBO_EPROTO;
  view->kind = turbo_json_string(kind);
  view->config = fields;
  return TURBO_OK;
}

static const json_value_t *
flow_resolved_adapter_field(const turbo_flow_resolved_adapter_view_t *view, const char *field) {
  if (!view || view->size < sizeof(*view) || !view->config || !field || !field[0]) return NULL;
  return turbo_json_object_get((const json_value_t *)view->config, field);
}

size_t turbo_flow_resolved_adapter_field_count(const turbo_flow_resolved_adapter_view_t *view) {
  if (!view || view->size < sizeof(*view) || !view->config ||
      turbo_json_type((const json_value_t *)view->config) != TURBO_JSON_OBJECT)
    return 0u;
  return turbo_json_object_size((const json_value_t *)view->config);
}

const char *turbo_flow_resolved_adapter_field_name(const turbo_flow_resolved_adapter_view_t *view,
                                                   size_t index) {
  if (!view || view->size < sizeof(*view) || !view->config ||
      turbo_json_type((const json_value_t *)view->config) != TURBO_JSON_OBJECT)
    return NULL;
  return turbo_json_object_key((const json_value_t *)view->config, index);
}

int turbo_flow_resolved_adapter_field_type(const turbo_flow_resolved_adapter_view_t *view,
                                           const char *field,
                                           turbo_flow_config_value_type_t *type) {
  const json_value_t *value = flow_resolved_adapter_field(view, field);
  if (!type) return TURBO_EINVAL;
  if (!value) return TURBO_ENOENT;
  switch (turbo_json_type(value)) {
  case TURBO_JSON_NULL:
    *type = TURBO_FLOW_CONFIG_NULL;
    break;
  case TURBO_JSON_BOOL:
    *type = TURBO_FLOW_CONFIG_BOOL;
    break;
  case TURBO_JSON_NUMBER:
    *type = TURBO_FLOW_CONFIG_NUMBER;
    break;
  case TURBO_JSON_STRING:
    *type = TURBO_FLOW_CONFIG_STRING;
    break;
  case TURBO_JSON_ARRAY:
    *type = TURBO_FLOW_CONFIG_ARRAY;
    break;
  case TURBO_JSON_OBJECT:
    *type = TURBO_FLOW_CONFIG_OBJECT;
    break;
  default:
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_resolved_adapter_get_string(const turbo_flow_resolved_adapter_view_t *view,
                                           const char *field, const char **value) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (value) *value = NULL;
  if (!value) return TURBO_EINVAL;
  if (!field_value) return TURBO_ENOENT;
  if (turbo_json_type(field_value) != TURBO_JSON_STRING) return TURBO_EINVAL;
  *value = turbo_json_string(field_value);
  return TURBO_OK;
}

int turbo_flow_resolved_adapter_get_bool(const turbo_flow_resolved_adapter_view_t *view,
                                         const char *field, int *value) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (!value) return TURBO_EINVAL;
  if (!field_value) return TURBO_ENOENT;
  if (turbo_json_type(field_value) != TURBO_JSON_BOOL) return TURBO_EINVAL;
  *value = turbo_json_bool(field_value) != 0;
  return TURBO_OK;
}

static int flow_resolved_adapter_number(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *field, double *number) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (!number) return TURBO_EINVAL;
  if (!field_value) return TURBO_ENOENT;
  if (turbo_json_type(field_value) != TURBO_JSON_NUMBER) return TURBO_EINVAL;
  *number = turbo_json_number(field_value);
  return isfinite(*number) ? TURBO_OK : TURBO_ERANGE;
}

int turbo_flow_resolved_adapter_get_u64(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *field, uint64_t *value) {
  double number;
  uint64_t converted;
  int rc;
  if (!value) return TURBO_EINVAL;
  rc = flow_resolved_adapter_number(view, field, &number);
  if (rc != TURBO_OK) return rc;
  if (number < 0.0 || number > 9007199254740991.0) return TURBO_ERANGE;
  converted = (uint64_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *value = converted;
  return TURBO_OK;
}

int turbo_flow_resolved_adapter_get_i64(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *field, int64_t *value) {
  double number;
  int64_t converted;
  int rc;
  if (!value) return TURBO_EINVAL;
  rc = flow_resolved_adapter_number(view, field, &number);
  if (rc != TURBO_OK) return rc;
  if (number < -9007199254740991.0 || number > 9007199254740991.0) return TURBO_ERANGE;
  converted = (int64_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *value = converted;
  return TURBO_OK;
}

int turbo_flow_resolved_adapter_array_size(const turbo_flow_resolved_adapter_view_t *view,
                                           const char *field, size_t *size) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  if (!size) return TURBO_EINVAL;
  if (!field_value) return TURBO_ENOENT;
  if (turbo_json_type(field_value) != TURBO_JSON_ARRAY) return TURBO_EINVAL;
  *size = turbo_json_array_size(field_value);
  return TURBO_OK;
}

int turbo_flow_resolved_adapter_array_string_at(const turbo_flow_resolved_adapter_view_t *view,
                                                const char *field, size_t index,
                                                const char **value) {
  const json_value_t *field_value = flow_resolved_adapter_field(view, field);
  const json_value_t *item;
  if (value) *value = NULL;
  if (!value) return TURBO_EINVAL;
  if (!field_value) return TURBO_ENOENT;
  if (turbo_json_type(field_value) != TURBO_JSON_ARRAY) return TURBO_EINVAL;
  item = turbo_json_array_get(field_value, index);
  if (!item) return TURBO_ENOENT;
  if (turbo_json_type(item) != TURBO_JSON_STRING) return TURBO_EINVAL;
  *value = turbo_json_string(item);
  return TURBO_OK;
}
