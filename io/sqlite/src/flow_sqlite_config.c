#include "flow_sqlite_storage_internal.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int flow_sqlite_config_error(turbo_flow_config_error_t *error, int status,
                                    const char *channel_name, const char *field,
                                    const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config%s%s",
                   channel_name ? channel_name : "?", field ? "." : "", field ? field : "");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int flow_sqlite_field_allowed(const char *field, const char *const *allowed,
                                     size_t allowed_count) {
  for (size_t i = 0u; field && i < allowed_count; ++i)
    if (strcmp(field, allowed[i]) == 0) return 1;
  return 0;
}

static int flow_sqlite_config_size(const json_value_t *fields, const char *field, uint64_t maximum,
                                   size_t *out, const char *channel_name,
                                   turbo_flow_config_error_t *error) {
  json_value_t *value = turbo_json_object_get(fields, field);
  double number;
  uint64_t converted;
  if (!value) return TURBO_OK;
  if (turbo_json_type(value) != TURBO_JSON_NUMBER)
    return flow_sqlite_config_error(error, TURBO_EINVAL, channel_name, field,
                                    "limit must be a positive integer");
  number = turbo_json_number(value);
  if (!isfinite(number) || number < 1.0 || number > (double)maximum ||
      number > 9007199254740991.0) {
    return flow_sqlite_config_error(error, TURBO_ERANGE, channel_name, field,
                                    "limit is outside the supported range");
  }
  converted = (uint64_t)number;
  if ((double)converted != number)
    return flow_sqlite_config_error(error, TURBO_EINVAL, channel_name, field,
                                    "limit must not contain a fractional value");
  *out = (size_t)converted;
  return TURBO_OK;
}

int turbo_flow_sqlite_record_store_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                   const char *channel_name,
                                                   turbo_flow_record_store_t *out,
                                                   turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {
      "backend",   "database_path",  "namespace_name", "busy_timeout_ms", "max_records",
      "max_bytes", "max_item_bytes", "max_key_size",   "max_value_size",  "max_batch_size"};
  turbo_flow_sqlite_record_store_config_t config = TURBO_FLOW_SQLITE_RECORD_STORE_CONFIG_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *value;
  const char *backend;
  const char *json;
  size_t json_size = 0u;
  int rc = TURBO_OK;
  if (!resolved || !channel_name || !channel_name[0] || !out || out->size < sizeof(*out) ||
      out->ctx || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document)
    return flow_sqlite_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                    "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  kind = channel ? turbo_json_object_get(channel, "kind") : NULL;
  fields = channel ? turbo_json_object_get(channel, "config") : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT || !kind ||
      turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "record_store") != 0 || !fields ||
      turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_sqlite_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                  "channel must be kind record_store with config");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!flow_sqlite_field_allowed(field, allowed, sizeof(allowed) / sizeof(allowed[0]))) {
      rc = flow_sqlite_config_error(error, TURBO_EINVAL, channel_name, field,
                                    "unknown SQLite record store field");
      goto done;
    }
  }
  backend = turbo_json_get_string(fields, "backend");
  config.database_path = turbo_json_get_string(fields, "database_path");
  config.namespace_name = turbo_json_get_string(fields, "namespace_name");
  if (!backend || strcmp(backend, "sqlite") != 0) {
    rc = flow_sqlite_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, channel_name,
                                  "backend", "backend must be exactly sqlite");
    goto done;
  }
  if (!config.database_path || !config.database_path[0] || !config.namespace_name ||
      !config.namespace_name[0]) {
    rc = flow_sqlite_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                  "database_path and namespace_name are required");
    goto done;
  }
  value = turbo_json_object_get(fields, "busy_timeout_ms");
  if (value) {
    double number;
    if (turbo_json_type(value) != TURBO_JSON_NUMBER ||
        !isfinite(number = turbo_json_number(value)) || number < 0.0 || number > (double)INT_MAX ||
        (double)(int)number != number) {
      rc = flow_sqlite_config_error(error, TURBO_ERANGE, channel_name, "busy_timeout_ms",
                                    "busy_timeout_ms must be a non-negative bounded integer");
      goto done;
    }
    config.busy_timeout_ms = (int)number;
  }
  rc = flow_sqlite_config_size(fields, "max_records", INT64_MAX, &config.max_records, channel_name,
                               error);
  if (rc == TURBO_OK)
    rc = flow_sqlite_config_size(fields, "max_bytes", INT64_MAX, &config.max_bytes, channel_name,
                                 error);
  if (rc == TURBO_OK)
    rc = flow_sqlite_config_size(fields, "max_item_bytes", INT64_MAX, &config.max_item_bytes,
                                 channel_name, error);
  if (rc == TURBO_OK)
    rc = flow_sqlite_config_size(fields, "max_key_size", INT_MAX, &config.max_key_size,
                                 channel_name, error);
  if (rc == TURBO_OK)
    rc = flow_sqlite_config_size(fields, "max_value_size", INT_MAX, &config.max_value_size,
                                 channel_name, error);
  if (rc == TURBO_OK)
    rc = flow_sqlite_config_size(fields, "max_batch_size", UINT16_MAX, &config.max_batch_size,
                                 channel_name, error);
  if (rc != TURBO_OK) goto done;
  rc = turbo_flow_sqlite_record_store_create(&config, out);
  if (rc != TURBO_OK)
    rc = flow_sqlite_config_error(error, rc, channel_name, NULL,
                                  "SQLite record store creation failed");

done:
  turbo_free_json(&document);
  return rc;
}
