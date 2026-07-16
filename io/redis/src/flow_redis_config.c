#include "turbo_flow_redis.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

static int flow_redis_config_error(turbo_flow_config_error_t *error, int status, const char *name,
                                   const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.%s", name, field);
    else (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s", name ? name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_redis_config_field_allowed(const char *field, const char *const *allowed,
                                           size_t allowed_count) {
  for (size_t i = 0; i < allowed_count; ++i) {
    if (strcmp(field, allowed[i]) == 0) return 1;
  }
  return 0;
}

static int flow_redis_config_validate_fields(const json_value_t *fields, const char *name,
                                             const char *const *allowed, size_t allowed_count,
                                             turbo_flow_config_error_t *error) {
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT)
    return flow_redis_config_error(error, TURBO_EINVAL, name, NULL,
                                   "adapter config must be a mapping");
  for (size_t i = 0; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!field || !flow_redis_config_field_allowed(field, allowed, allowed_count))
      return flow_redis_config_error(error, TURBO_EINVAL, name, field,
                                     "field is not valid for this Redis pattern");
  }
  return TURBO_OK;
}

static const char *flow_redis_config_string(const json_value_t *fields, const char *field) {
  json_value_t *value = turbo_json_object_get(fields, field);
  if (!value || turbo_json_type(value) == TURBO_JSON_NULL) return NULL;
  return turbo_json_type(value) == TURBO_JSON_STRING ? turbo_json_string(value) : NULL;
}

static int flow_redis_config_validate_string(const json_value_t *fields, const char *field,
                                             const char *name, turbo_flow_config_error_t *error) {
  json_value_t *value = turbo_json_object_get(fields, field);
  if (!value || turbo_json_type(value) == TURBO_JSON_STRING ||
      turbo_json_type(value) == TURBO_JSON_NULL) {
    return TURBO_OK;
  }
  return flow_redis_config_error(error, TURBO_EINVAL, name, field,
                                 "field must be a string or null");
}

static int flow_redis_config_u64_value(const json_value_t *value, uint64_t maximum, uint64_t *out) {
  double number;
  uint64_t converted;
  if (!value || turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EINVAL;
  number = turbo_json_number(value);
  if (!isfinite(number) || number < 0.0 || number > (double)maximum ||
      number > 9007199254740991.0) {
    return TURBO_ERANGE;
  }
  converted = (uint64_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *out = converted;
  return TURBO_OK;
}

static int flow_redis_config_optional_u64(const json_value_t *fields, const char *field,
                                          uint64_t maximum, uint64_t *out, const char *name,
                                          turbo_flow_config_error_t *error) {
  json_value_t *value = turbo_json_object_get(fields, field);
  int rc;
  if (!value) return TURBO_OK;
  rc = flow_redis_config_u64_value(value, maximum, out);
  return rc == TURBO_OK
             ? TURBO_OK
             : flow_redis_config_error(error, rc, name, field, "non-negative integer is invalid");
}

static int flow_redis_config_common(const json_value_t *fields, const char *name, const char **host,
                                    uint16_t *port, const char **username, const char **password,
                                    int *database, uint32_t *timeout_ms,
                                    turbo_flow_config_error_t *error) {
  uint64_t value = 0u;
  int rc;
  rc = flow_redis_config_validate_string(fields, "host", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "username", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "password", name, error);
  if (rc != TURBO_OK) return rc;
  *host = flow_redis_config_string(fields, "host");
  *username = flow_redis_config_string(fields, "username");
  *password = flow_redis_config_string(fields, "password");
  if (!*host || !(*host)[0])
    return flow_redis_config_error(error, TURBO_EINVAL, name, "host",
                                   "host must be a non-empty string");
  rc = flow_redis_config_optional_u64(fields, "port", UINT16_MAX, &value, name, error);
  if (rc != TURBO_OK) return rc;
  if (!turbo_json_object_get(fields, "port") || value == 0u)
    return flow_redis_config_error(error, TURBO_EINVAL, name, "port",
                                   "port must be an integer from 1 through 65535");
  *port = (uint16_t)value;
  value = 0u;
  rc = flow_redis_config_optional_u64(fields, "database", 15u, &value, name, error);
  if (rc != TURBO_OK) return rc;
  *database = (int)value;
  value = 0u;
  rc = flow_redis_config_optional_u64(fields, "timeout_ms", INT_MAX, &value, name, error);
  if (rc != TURBO_OK) return rc;
  *timeout_ms = (uint32_t)value;
  return TURBO_OK;
}

static int flow_redis_blob_config_error(turbo_flow_config_error_t *error, int status,
                                        const char *name, const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", name, field);
    else (void)snprintf(error->path, sizeof(error->path), "$.channels.%s", name ? name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_redis_blob_config_u64(const json_value_t *fields, const char *field,
                                      uint64_t maximum, int required, uint64_t *out,
                                      const char *name, turbo_flow_config_error_t *error) {
  json_value_t *value = turbo_json_object_get(fields, field);
  int rc;
  if (!value)
    return required ? flow_redis_blob_config_error(error, TURBO_EINVAL, name, field,
                                                   "required integer is missing")
                    : TURBO_OK;
  rc = flow_redis_config_u64_value(value, maximum, out);
  return rc == TURBO_OK ? TURBO_OK
                        : flow_redis_blob_config_error(error, rc, name, field,
                                                       "non-negative integer is invalid");
}

int turbo_flow_redis_blob_store_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                const char *channel_name,
                                                turbo_flow_blob_store_t *out, char *key,
                                                size_t key_capacity,
                                                turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"backend",    "host",     "port",
                                        "username",   "password", "database",
                                        "timeout_ms", "key",      "max_value_size"};
  turbo_flow_redis_blob_store_config_t config;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  const char *backend;
  const char *json;
  size_t json_size = 0u;
  size_t key_size;
  uint64_t number = 0u;
  int rc = TURBO_OK;
  if (key && key_capacity > 0u) key[0] = '\0';
  if (!resolved || !channel_name || !channel_name[0] || !out || out->size < sizeof(*out) ||
      out->ctx || !key || key_capacity == 0u || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document)
    return flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                        "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_redis_blob_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                      "blob store channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "blob_store") != 0) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "channel kind must be blob_store");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "blob store config must be a mapping");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!field ||
        !flow_redis_config_field_allowed(field, allowed, sizeof(allowed) / sizeof(allowed[0]))) {
      rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, field,
                                        "field is not valid for a Redis blob store");
      goto done;
    }
  }
  backend = flow_redis_config_string(fields, "backend");
  memset(&config, 0, sizeof(config));
  config.host = flow_redis_config_string(fields, "host");
  config.username = flow_redis_config_string(fields, "username");
  config.password = flow_redis_config_string(fields, "password");
  config.key = flow_redis_config_string(fields, "key");
  if (!backend || strcmp(backend, "redis") != 0) {
    rc = flow_redis_blob_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, channel_name,
                                      "backend", "backend must be redis");
    goto done;
  }
  if (!config.host || !config.host[0] || !config.key || !config.key[0]) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "host and key are required strings");
    goto done;
  }
  if ((turbo_json_object_get(fields, "username") &&
       turbo_json_type(turbo_json_object_get(fields, "username")) != TURBO_JSON_STRING &&
       turbo_json_type(turbo_json_object_get(fields, "username")) != TURBO_JSON_NULL) ||
      (turbo_json_object_get(fields, "password") &&
       turbo_json_type(turbo_json_object_get(fields, "password")) != TURBO_JSON_STRING &&
       turbo_json_type(turbo_json_object_get(fields, "password")) != TURBO_JSON_NULL)) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "username and password must be strings or null");
    goto done;
  }
  key_size = strlen(config.key);
  if (key_size > TURBO_FLOW_REDIS_MAX_KEY_SIZE || key_size >= key_capacity) {
    rc = flow_redis_blob_config_error(error, TURBO_ENAMETOOLONG, channel_name, "key",
                                      "key exceeds the provider or caller bound");
    goto done;
  }
  rc = flow_redis_blob_config_u64(fields, "port", UINT16_MAX, 1, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  if (number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "port",
                                      "port must be from 1 through 65535");
    goto done;
  }
  config.port = (uint16_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "database", 15u, 0, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.database = (int)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "timeout_ms", INT_MAX, 0, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.timeout_ms = (uint32_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "max_value_size", TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE,
                                  0, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  if (turbo_json_object_get(fields, "max_value_size") && number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "max_value_size",
                                      "max_value_size must be positive");
    goto done;
  }
  config.max_value_size = (size_t)number;
  rc = turbo_flow_redis_blob_store_create(&config, out);
  if (rc != TURBO_OK) {
    rc = flow_redis_blob_config_error(error, rc, channel_name, NULL,
                                      "Redis blob store creation failed");
    goto done;
  }
  memcpy(key, config.key, key_size + 1u);

done:
  turbo_free_json(&document);
  return rc;
}

int turbo_flow_redis_record_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {
      "backend",      "host",          "port",          "username",      "password",
      "database",     "timeout_ms",    "key",           "max_key_size",  "max_value_size",
      "max_batch_size", "max_records"};
  turbo_flow_redis_record_store_config_t config;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  const char *backend;
  const char *json;
  size_t json_size = 0u;
  uint64_t number = 0u;
  int rc = TURBO_OK;
  if (!resolved || !channel_name || !channel_name[0] || !out || out->size < sizeof(*out) ||
      out->ctx || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document)
    return flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                        "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_redis_blob_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                      "record store channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "record_store") != 0) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "channel kind must be record_store");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "record store config must be a mapping");
    goto done;
  }
  backend = flow_redis_config_string(fields, "backend");
  if (!backend || strcmp(backend, "redis") != 0) {
    rc = flow_redis_blob_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, channel_name,
                                      "backend", "backend must be redis");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!field ||
        !flow_redis_config_field_allowed(field, allowed, sizeof(allowed) / sizeof(allowed[0]))) {
      rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, field,
                                        "field is not valid for a Redis record store");
      goto done;
    }
  }
  memset(&config, 0, sizeof(config));
  config.host = flow_redis_config_string(fields, "host");
  config.username = flow_redis_config_string(fields, "username");
  config.password = flow_redis_config_string(fields, "password");
  config.key = flow_redis_config_string(fields, "key");
  if (!config.host || !config.host[0] || !config.key || !config.key[0]) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "host and key are required strings");
    goto done;
  }
  if (strlen(config.key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE) {
    rc = flow_redis_blob_config_error(error, TURBO_ENAMETOOLONG, channel_name, "key",
                                      "key exceeds the provider bound");
    goto done;
  }
  if ((turbo_json_object_get(fields, "username") &&
       turbo_json_type(turbo_json_object_get(fields, "username")) != TURBO_JSON_STRING &&
       turbo_json_type(turbo_json_object_get(fields, "username")) != TURBO_JSON_NULL) ||
      (turbo_json_object_get(fields, "password") &&
       turbo_json_type(turbo_json_object_get(fields, "password")) != TURBO_JSON_STRING &&
       turbo_json_type(turbo_json_object_get(fields, "password")) != TURBO_JSON_NULL)) {
    rc = flow_redis_blob_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "username and password must be strings or null");
    goto done;
  }
  rc = flow_redis_blob_config_u64(fields, "port", UINT16_MAX, 1, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  if (number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "port",
                                      "port must be from 1 through 65535");
    goto done;
  }
  config.port = (uint16_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "database", 15u, 0, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.database = (int)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "timeout_ms", INT_MAX, 0, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.timeout_ms = (uint32_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "max_key_size",
                                  TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE, 0, &number,
                                  channel_name, error);
  if (rc != TURBO_OK) goto done;
  if (turbo_json_object_get(fields, "max_key_size") && number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "max_key_size",
                                      "max_key_size must be positive");
    goto done;
  }
  config.max_record_key_size = (size_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "max_value_size", TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE,
                                  0, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  if (turbo_json_object_get(fields, "max_value_size") && number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "max_value_size",
                                      "max_value_size must be positive");
    goto done;
  }
  config.max_value_size = (size_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "max_batch_size", UINT16_MAX, 0, &number, channel_name,
                                  error);
  if (rc != TURBO_OK) goto done;
  if (turbo_json_object_get(fields, "max_batch_size") && number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "max_batch_size",
                                      "max_batch_size must be positive");
    goto done;
  }
  config.max_batch_size = (size_t)number;
  number = 0u;
  rc = flow_redis_blob_config_u64(fields, "max_records", INT_MAX, 1, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  if (number == 0u) {
    rc = flow_redis_blob_config_error(error, TURBO_ERANGE, channel_name, "max_records",
                                      "max_records must be positive");
    goto done;
  }
  config.max_records = (size_t)number;
  rc = turbo_flow_redis_record_store_create(&config, out);
  if (rc != TURBO_OK)
    rc = flow_redis_blob_config_error(error, rc, channel_name, NULL,
                                      "Redis record store creation failed");

done:
  turbo_free_json(&document);
  return rc;
}

static int flow_redis_config_register_stream(turbo_flow_t *flow, const char *name,
                                             const json_value_t *fields,
                                             turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {
      "pattern",  "role",       "host",           "port",       "username", "password",
      "database", "timeout_ms", "stream",         "field",      "maxlen",   "poll_interval_ms",
      "group",    "consumer",   "group_start_id", "read_count", "block_ms", "create_group"};
  turbo_flow_redis_stream_config_t config;
  const char *role;
  uint64_t value = 0u;
  json_value_t *create_group;
  int rc;
  memset(&config, 0, sizeof(config));
  rc = flow_redis_config_validate_fields(fields, name, allowed,
                                         sizeof(allowed) / sizeof(allowed[0]), error);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_config_validate_string(fields, "role", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "stream", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "field", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "group", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "consumer", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "group_start_id", name, error);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_config_common(fields, name, &config.host, &config.port, &config.username,
                                &config.password, &config.database, &config.timeout_ms, error);
  if (rc != TURBO_OK) return rc;
  role = flow_redis_config_string(fields, "role");
  config.stream = flow_redis_config_string(fields, "stream");
  config.field = flow_redis_config_string(fields, "field");
  config.group = flow_redis_config_string(fields, "group");
  config.consumer = flow_redis_config_string(fields, "consumer");
  config.group_start_id = flow_redis_config_string(fields, "group_start_id");
  if (!config.stream || !config.stream[0])
    return flow_redis_config_error(error, TURBO_EINVAL, name, "stream",
                                   "stream must be a non-empty string");
  if (!role || (strcmp(role, "source") != 0 && strcmp(role, "sink") != 0))
    return flow_redis_config_error(error, TURBO_EINVAL, name, "role",
                                   "stream role must be source or sink");
  rc = flow_redis_config_optional_u64(fields, "maxlen", SIZE_MAX, &value, name, error);
  if (rc != TURBO_OK) return rc;
  config.maxlen = (size_t)value;
  value = 0u;
  rc = flow_redis_config_optional_u64(fields, "poll_interval_ms", UINT32_MAX, &value, name, error);
  if (rc != TURBO_OK) return rc;
  config.poll_interval_ms = (uint32_t)value;
  value = 0u;
  rc = flow_redis_config_optional_u64(fields, "read_count", SIZE_MAX, &value, name, error);
  if (rc != TURBO_OK) return rc;
  config.read_count = (size_t)value;
  value = 0u;
  rc = flow_redis_config_optional_u64(fields, "block_ms", TURBO_FLOW_REDIS_MAX_BLOCK_MS, &value,
                                      name, error);
  if (rc != TURBO_OK) return rc;
  config.block_ms = (uint32_t)value;
  create_group = turbo_json_object_get(fields, "create_group");
  if (create_group) {
    if (turbo_json_type(create_group) != TURBO_JSON_BOOL)
      return flow_redis_config_error(error, TURBO_EINVAL, name, "create_group",
                                     "create_group must be boolean");
    config.create_group = turbo_json_bool(create_group) ? 1 : 0;
  }
  if (strcmp(role, "source") == 0) {
    if (turbo_json_object_get(fields, "maxlen"))
      return flow_redis_config_error(error, TURBO_EINVAL, name, "maxlen",
                                     "maxlen is valid only for a stream sink");
    if (config.poll_interval_ms == 0u)
      return flow_redis_config_error(error, TURBO_EINVAL, name, "poll_interval_ms",
                                     "stream source requires a positive poll_interval_ms");
    if (!config.group || !config.group[0] || !config.consumer || !config.consumer[0])
      return flow_redis_config_error(error, TURBO_EINVAL, name, NULL,
                                     "stream source requires group and consumer");
  } else if (config.poll_interval_ms != 0u || config.group || config.consumer ||
             config.group_start_id || config.read_count != 0u || config.block_ms != 0u ||
             create_group) {
    return flow_redis_config_error(error, TURBO_EINVAL, name, "role",
                                   "stream sink does not accept source polling fields");
  }
  rc = turbo_flow_redis_register_stream_adapter(flow, name, &config);
  return rc == TURBO_OK ? TURBO_OK
                        : flow_redis_config_error(error, rc, name, NULL,
                                                  "Redis Stream configuration validation failed");
}

static int flow_redis_config_register_data(turbo_flow_t *flow, const char *name,
                                           const json_value_t *fields,
                                           turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"pattern",  "operation",     "host",     "port",
                                        "username", "password",      "database", "timeout_ms",
                                        "key",      "max_value_size"};
  turbo_flow_redis_data_config_t config;
  const char *operation;
  uint64_t value = 0u;
  int rc;
  memset(&config, 0, sizeof(config));
  rc = flow_redis_config_validate_fields(fields, name, allowed,
                                         sizeof(allowed) / sizeof(allowed[0]), error);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_config_validate_string(fields, "operation", name, error);
  if (rc == TURBO_OK) rc = flow_redis_config_validate_string(fields, "key", name, error);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_config_common(fields, name, &config.host, &config.port, &config.username,
                                &config.password, &config.database, &config.timeout_ms, error);
  if (rc != TURBO_OK) return rc;
  config.key = flow_redis_config_string(fields, "key");
  operation = flow_redis_config_string(fields, "operation");
  if (!config.key || !config.key[0] || strlen(config.key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE)
    return flow_redis_config_error(error, TURBO_EINVAL, name, "key",
                                   "key is empty or exceeds the public bound");
  if (operation && strcmp(operation, "set") == 0) config.operation = TURBO_FLOW_REDIS_DATA_SET;
  else if (operation && strcmp(operation, "get") == 0) config.operation = TURBO_FLOW_REDIS_DATA_GET;
  else
    return flow_redis_config_error(error, TURBO_EINVAL, name, "operation",
                                   "data operation must be set or get");
  rc = flow_redis_config_optional_u64(fields, "max_value_size",
                                      TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE, &value, name, error);
  if (rc != TURBO_OK) return rc;
  config.max_value_size = (size_t)value;
  rc = turbo_flow_redis_register_data_adapter(flow, name, &config);
  return rc == TURBO_OK ? TURBO_OK
                        : flow_redis_config_error(error, rc, name, NULL,
                                                  "Redis Data configuration validation failed");
}

int turbo_flow_redis_register_resolved_adapter(turbo_flow_t *flow, const char *name,
                                               const turbo_flow_resolved_config_t *resolved,
                                               turbo_flow_config_error_t *error) {
  turbo_json_doc_t *document = NULL;
  json_value_t *adapters;
  json_value_t *adapter;
  json_value_t *kind;
  json_value_t *fields;
  const char *pattern;
  const char *json;
  size_t json_len = 0u;
  int rc;
  if (!flow || !name || !name[0] || !resolved || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_len);
  if (!json || turbo_parse_json((const uint8_t *)json, json_len, &document) != TURBO_OK ||
      !document) {
    return flow_redis_config_error(error, TURBO_EINVAL, name, NULL,
                                   "invalid resolved configuration snapshot");
  }
  adapters = turbo_json_object_get(document, "adapters");
  adapter = adapters ? turbo_json_object_get(adapters, name) : NULL;
  if (!adapter || turbo_json_type(adapter) != TURBO_JSON_OBJECT) {
    rc = flow_redis_config_error(error, TURBO_ENOENT, name, NULL, "adapter is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(adapter, "kind");
  fields = turbo_json_object_get(adapter, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "redis") != 0) {
    rc = flow_redis_config_error(error, TURBO_EINVAL, name, NULL, "adapter kind must be redis");
    goto done;
  }
  pattern = flow_redis_config_string(fields, "pattern");
  if (!pattern) {
    rc = flow_redis_config_error(error, TURBO_EINVAL, name, "pattern",
                                 "pattern must be stream or data");
    goto done;
  }
  if (strcmp(pattern, "stream") == 0)
    rc = flow_redis_config_register_stream(flow, name, fields, error);
  else if (strcmp(pattern, "data") == 0)
    rc = flow_redis_config_register_data(flow, name, fields, error);
  else
    rc = flow_redis_config_error(error, TURBO_ENOTSUP, name, "pattern",
                                 "Redis pattern is not supported");

done:
  turbo_free_json(&document);
  return rc;
}
