#include "flow_pgsql_storage_internal.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

static int flow_pgsql_config_error(turbo_flow_config_error_t *error, int status, const char *scope,
                                   const char *name, const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.%s.%s.config.%s", scope, name, field);
    else (void)snprintf(error->path, sizeof(error->path), "$.%s.%s", scope, name ? name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_pgsql_field_allowed(const char *field, const char *const *allowed,
                                    size_t allowed_count) {
  for (size_t i = 0u; i < allowed_count; ++i) {
    if (strcmp(field, allowed[i]) == 0) return 1;
  }
  return 0;
}

static int flow_pgsql_validate_fields(const json_value_t *fields, const char *scope,
                                      const char *name, const char *const *allowed,
                                      size_t allowed_count, turbo_flow_config_error_t *error) {
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT)
    return flow_pgsql_config_error(error, TURBO_EINVAL, scope, name, NULL,
                                   "config must be a mapping");
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!field || !flow_pgsql_field_allowed(field, allowed, allowed_count))
      return flow_pgsql_config_error(error, TURBO_EINVAL, scope, name, field,
                                     "unknown PostgreSQL field");
  }
  return TURBO_OK;
}

static const char *flow_pgsql_string(const json_value_t *fields, const char *field) {
  json_value_t *value = turbo_json_object_get(fields, field);
  return value && turbo_json_type(value) == TURBO_JSON_STRING ? turbo_json_string(value) : NULL;
}

static int flow_pgsql_required_u64(const json_value_t *fields, const char *field, uint64_t maximum,
                                   uint64_t *out, const char *channel_name,
                                   turbo_flow_config_error_t *error) {
  json_value_t *value = turbo_json_object_get(fields, field);
  double number;
  uint64_t converted;
  if (!value || turbo_json_type(value) != TURBO_JSON_NUMBER)
    return flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, field,
                                   "required positive integer is invalid");
  number = turbo_json_number(value);
  if (!isfinite(number) || number < 1.0 || number > (double)maximum || number > 9007199254740991.0)
    return flow_pgsql_config_error(error, TURBO_ERANGE, "channels", channel_name, field,
                                   "required positive integer is out of range");
  converted = (uint64_t)number;
  if ((double)converted != number)
    return flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, field,
                                   "required positive integer has a fractional value");
  *out = converted;
  return TURBO_OK;
}

static int flow_pgsql_optional_u64(const json_value_t *fields, const char *field, uint64_t maximum,
                                   uint64_t *out, const char *channel_name,
                                   turbo_flow_config_error_t *error) {
  json_value_t *value = turbo_json_object_get(fields, field);
  if (!value) {
    *out = 0u;
    return TURBO_OK;
  }
  return flow_pgsql_required_u64(fields, field, maximum, out, channel_name, error);
}

int turbo_flow_pgsql_register_resolved_outbox_adapter(turbo_flow_t *flow, const char *name,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      turbo_flow_config_error_t *error) {
  static const char *const adapter_allowed[] = {"channel", "role"};
  static const char *const channel_allowed[] = {
      "backend",          "conninfo",         "outbox_name",      "capacity",
      "max_payload_size", "poll_interval_ms", "claim_scan_limit", "create_table",
      "completion",       "max_delivery_attempts", "retry_delay_ms", "archive_ttl_ms"};
  turbo_flow_pgsql_outbox_config_t config = TURBO_FLOW_PGSQL_OUTBOX_CONFIG_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *adapters;
  json_value_t *adapter;
  json_value_t *adapter_kind;
  json_value_t *adapter_fields;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *channel_kind;
  json_value_t *channel_fields;
  json_value_t *create_table;
  json_value_t *completion;
  const char *channel_name;
  const char *role;
  const char *backend;
  const char *json;
  size_t json_size = 0u;
  uint64_t number;
  int rc;
  if (!flow || !name || !name[0] || !resolved || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document)
    return flow_pgsql_config_error(error, TURBO_EINVAL, "adapters", name, NULL,
                                   "invalid resolved configuration snapshot");

  adapters = turbo_json_object_get(document, "adapters");
  adapter = adapters ? turbo_json_object_get(adapters, name) : NULL;
  if (!adapter || turbo_json_type(adapter) != TURBO_JSON_OBJECT) {
    rc = flow_pgsql_config_error(error, TURBO_ENOENT, "adapters", name, NULL,
                                 "adapter is not resolved");
    goto done;
  }
  adapter_kind = turbo_json_object_get(adapter, "kind");
  adapter_fields = turbo_json_object_get(adapter, "config");
  if (!adapter_kind || turbo_json_type(adapter_kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(adapter_kind), "pgsql_outbox") != 0) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "adapters", name, NULL,
                                 "adapter kind must be pgsql_outbox");
    goto done;
  }
  rc = flow_pgsql_validate_fields(adapter_fields, "adapters", name, adapter_allowed,
                                  sizeof(adapter_allowed) / sizeof(adapter_allowed[0]), error);
  if (rc != TURBO_OK) goto done;
  channel_name = flow_pgsql_string(adapter_fields, "channel");
  role = flow_pgsql_string(adapter_fields, "role");
  if (!channel_name || !channel_name[0]) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "adapters", name, "channel",
                                 "channel must be a non-empty string");
    goto done;
  }
  if (!role || (strcmp(role, "sink") != 0 && strcmp(role, "source") != 0)) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "adapters", name, "role",
                                 "role must be sink or source");
    goto done;
  }
  config.role =
      strcmp(role, "source") == 0 ? TURBO_FLOW_PGSQL_OUTBOX_SOURCE : TURBO_FLOW_PGSQL_OUTBOX_SINK;

  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_pgsql_config_error(error, TURBO_ENOENT, "channels", channel_name, NULL,
                                 "outbox channel is not resolved");
    goto done;
  }
  channel_kind = turbo_json_object_get(channel, "kind");
  channel_fields = turbo_json_object_get(channel, "config");
  if (!channel_kind || turbo_json_type(channel_kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(channel_kind), "outbox") != 0) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "channel kind must be outbox");
    goto done;
  }
  rc = flow_pgsql_validate_fields(channel_fields, "channels", channel_name, channel_allowed,
                                  sizeof(channel_allowed) / sizeof(channel_allowed[0]), error);
  if (rc != TURBO_OK) goto done;
  backend = flow_pgsql_string(channel_fields, "backend");
  config.conninfo = flow_pgsql_string(channel_fields, "conninfo");
  config.outbox_name = flow_pgsql_string(channel_fields, "outbox_name");
  if (!backend || strcmp(backend, "postgresql") != 0) {
    rc = flow_pgsql_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, "channels",
                                 channel_name, "backend", "backend must be postgresql");
    goto done;
  }
  if (!config.conninfo || !config.conninfo[0]) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "conninfo",
                                 "conninfo must be a non-empty string");
    goto done;
  }
  if (!config.outbox_name || !config.outbox_name[0] ||
      strlen(config.outbox_name) > TURBO_FLOW_PGSQL_OUTBOX_NAME_MAX) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "outbox_name",
                                 "outbox_name is empty or exceeds the public bound");
    goto done;
  }
  rc = flow_pgsql_required_u64(channel_fields, "capacity", TURBO_FLOW_PGSQL_OUTBOX_MAX_CAPACITY,
                               &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.capacity = (size_t)number;
  rc = flow_pgsql_required_u64(channel_fields, "max_payload_size",
                               TURBO_FLOW_PGSQL_OUTBOX_MAX_PAYLOAD_SIZE, &number, channel_name,
                               error);
  if (rc != TURBO_OK) goto done;
  config.max_payload_size = (size_t)number;
  rc = flow_pgsql_required_u64(channel_fields, "poll_interval_ms", UINT32_MAX, &number,
                               channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.poll_interval_ms = (uint32_t)number;
  rc =
      flow_pgsql_required_u64(channel_fields, "claim_scan_limit",
                              TURBO_FLOW_PGSQL_OUTBOX_MAX_CLAIM_SCAN, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.claim_scan_limit = (size_t)number;
  create_table = turbo_json_object_get(channel_fields, "create_table");
  if (!create_table || turbo_json_type(create_table) != TURBO_JSON_BOOL) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "create_table",
                                 "create_table must be boolean");
    goto done;
  }
  config.create_table = turbo_json_bool(create_table) ? 1 : 0;
  completion = turbo_json_object_get(channel_fields, "completion");
  if (completion) {
    const char *text;
    if (turbo_json_type(completion) != TURBO_JSON_STRING) {
      rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "completion",
                                   "completion must be delete or archive");
      goto done;
    }
    text = turbo_json_string(completion);
    if (strcmp(text, "delete") == 0)
      config.completion = TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_DELETE;
    else if (strcmp(text, "archive") == 0)
      config.completion = TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE;
    else {
      rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "completion",
                                   "completion must be delete or archive");
      goto done;
    }
  }
  rc = flow_pgsql_optional_u64(channel_fields, "max_delivery_attempts",
                               TURBO_FLOW_PGSQL_OUTBOX_MAX_DELIVERY_ATTEMPTS, &number,
                               channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.max_delivery_attempts = (uint32_t)number;
  rc = flow_pgsql_optional_u64(channel_fields, "retry_delay_ms", UINT32_MAX, &number,
                               channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.retry_delay_ms = (uint32_t)number;
  rc = flow_pgsql_optional_u64(channel_fields, "archive_ttl_ms", UINT32_MAX, &number,
                               channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.archive_ttl_ms = (uint32_t)number;
  rc = turbo_flow_pgsql_register_outbox_adapter(flow, name, &config);
  if (rc != TURBO_OK)
    rc = flow_pgsql_config_error(error, rc, "adapters", name, NULL,
                                 "PostgreSQL outbox adapter registration failed");

done:
  turbo_free_json(&document);
  return rc;
}

int flow_pgsql_record_store_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                            const char *channel_name,
                                            turbo_flow_record_store_t *out,
                                            turbo_flow_config_error_t *error) {
  static const char *const channel_allowed[] = {
      "backend",       "conninfo",       "namespace_name", "max_key_size",
      "max_value_size", "max_batch_size", "max_records",    "create_table"};
  turbo_flow_pgsql_record_store_config_t config = TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *create_table;
  const char *backend;
  const char *json;
  size_t json_size = 0u;
  uint64_t number = 0u;
  int rc;
  if (!resolved || !channel_name || !channel_name[0] || !out || out->size < sizeof(*out) ||
      out->ctx || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document)
    return flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                   "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_pgsql_config_error(error, TURBO_ENOENT, "channels", channel_name, NULL,
                                 "record store channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "record_store") != 0) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "channel kind must be record_store");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "record store config must be a mapping");
    goto done;
  }
  backend = flow_pgsql_string(fields, "backend");
  if (!backend || strcmp(backend, "postgresql") != 0) {
    rc = flow_pgsql_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, "channels",
                                 channel_name, "backend", "backend must be postgresql");
    goto done;
  }
  rc = flow_pgsql_validate_fields(fields, "channels", channel_name, channel_allowed,
                                  sizeof(channel_allowed) / sizeof(channel_allowed[0]), error);
  if (rc != TURBO_OK) goto done;
  config.conninfo = flow_pgsql_string(fields, "conninfo");
  config.namespace_name = flow_pgsql_string(fields, "namespace_name");
  if (!config.conninfo || !config.conninfo[0]) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "conninfo",
                                 "conninfo must be a non-empty string");
    goto done;
  }
  if (!config.namespace_name || !config.namespace_name[0] ||
      strlen(config.namespace_name) > TURBO_FLOW_PGSQL_RECORD_STORE_NAMESPACE_MAX) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name,
                                 "namespace_name",
                                 "namespace_name is empty or exceeds the public bound");
    goto done;
  }
  rc = flow_pgsql_optional_u64(fields, "max_key_size",
                               TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_KEY_SIZE, &number,
                               channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.max_key_size = (size_t)number;
  rc = flow_pgsql_optional_u64(fields, "max_value_size",
                               TURBO_FLOW_PGSQL_RECORD_STORE_MAX_VALUE_SIZE, &number, channel_name,
                               error);
  if (rc != TURBO_OK) goto done;
  config.max_value_size = (size_t)number;
  rc = flow_pgsql_optional_u64(fields, "max_batch_size", UINT16_MAX, &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.max_batch_size = (size_t)number;
  rc = flow_pgsql_required_u64(fields, "max_records",
                               TURBO_FLOW_PGSQL_RECORD_STORE_MAX_RECORDS, &number, channel_name,
                               error);
  if (rc != TURBO_OK) goto done;
  config.max_records = (size_t)number;
  create_table = turbo_json_object_get(fields, "create_table");
  if (!create_table || turbo_json_type(create_table) != TURBO_JSON_BOOL) {
    rc = flow_pgsql_config_error(error, TURBO_EINVAL, "channels", channel_name, "create_table",
                                 "create_table must be boolean");
    goto done;
  }
  config.create_table = turbo_json_bool(create_table) ? 1 : 0;
  rc = flow_pgsql_record_store_create(&config, out);
  if (rc != TURBO_OK)
    rc = flow_pgsql_config_error(error, rc, "channels", channel_name, NULL,
                                 "PostgreSQL record store creation failed");

done:
  turbo_free_json(&document);
  return rc;
}
