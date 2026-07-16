#include "flow_queue_internal.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

static int flow_queue_config_error(turbo_flow_config_error_t *error, int status, const char *scope,
                                   const char *name, const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field) {
      (void)snprintf(error->path, sizeof(error->path), "$.%s.%s.config.%s", scope, name, field);
    } else {
      (void)snprintf(error->path, sizeof(error->path), "$.%s.%s", scope, name ? name : "?");
    }
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_queue_field_allowed(const char *field, const char *const *allowed,
                                    size_t allowed_count) {
  for (size_t i = 0; i < allowed_count; ++i) {
    if (strcmp(field, allowed[i]) == 0) return 1;
  }
  return 0;
}

static int flow_queue_validate_fields(const json_value_t *object, const char *scope,
                                      const char *name, const char *const *allowed,
                                      size_t allowed_count, turbo_flow_config_error_t *error) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT)
    return flow_queue_config_error(error, TURBO_EINVAL, scope, name, NULL,
                                   "config must be a mapping");
  for (size_t i = 0; i < turbo_json_object_size(object); ++i) {
    const char *field = turbo_json_object_key(object, i);
    if (!field || !flow_queue_field_allowed(field, allowed, allowed_count))
      return flow_queue_config_error(error, TURBO_EINVAL, scope, name, field,
                                     "unknown Queue field");
  }
  return TURBO_OK;
}

static const char *flow_queue_string(const json_value_t *fields, const char *field) {
  json_value_t *value = turbo_json_object_get(fields, field);
  return value && turbo_json_type(value) == TURBO_JSON_STRING ? turbo_json_string(value) : NULL;
}

static int flow_queue_u64(const json_value_t *fields, const char *field, uint64_t maximum,
                          uint64_t *out) {
  json_value_t *value = turbo_json_object_get(fields, field);
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

static int flow_queue_required_u64(const json_value_t *fields, const char *field, uint64_t maximum,
                                   uint64_t *out, const char *channel_name,
                                   turbo_flow_config_error_t *error) {
  int rc = flow_queue_u64(fields, field, maximum, out);
  return rc == TURBO_OK ? TURBO_OK
                        : flow_queue_config_error(error, rc, "channels", channel_name, field,
                                                  "required non-negative integer is invalid");
}

static int flow_queue_parse_document(const turbo_flow_resolved_config_t *resolved,
                                     turbo_json_doc_t **document, turbo_flow_config_error_t *error,
                                     const char *scope, const char *name) {
  const char *json;
  size_t json_len = 0u;
  if (!resolved || !document || !error || error->size < sizeof(*error)) return TURBO_EINVAL;
  *document = NULL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_len);
  if (!json || turbo_parse_json((const uint8_t *)json, json_len, document) != TURBO_OK ||
      !*document) {
    return flow_queue_config_error(error, TURBO_EINVAL, scope, name, NULL,
                                   "invalid resolved configuration snapshot");
  }
  return TURBO_OK;
}

int turbo_flow_sqlite_blob_store_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                 const char *channel_name,
                                                 turbo_flow_blob_store_t *out, char *key,
                                                 size_t key_capacity,
                                                 turbo_flow_config_error_t *error) {
  static const char *const fields_allowed[] = {"backend", "database_path", "key", "busy_timeout_ms",
                                               "max_value_size"};
  turbo_json_doc_t *document = NULL;
  turbo_flow_sqlite_blob_store_config_t config;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  const char *backend;
  uint64_t number = 0u;
  size_t key_size;
  int rc;
  if (key && key_capacity > 0u) key[0] = '\0';
  if (!channel_name || !channel_name[0] || !out || out->size < sizeof(*out) || out->ctx || !key ||
      key_capacity == 0u)
    return TURBO_EINVAL;
  rc = flow_queue_parse_document(resolved, &document, error, "channels", channel_name);
  if (rc != TURBO_OK) return rc;
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_queue_config_error(error, TURBO_ENOENT, "channels", channel_name, NULL,
                                 "blob store channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "blob_store") != 0) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "channel kind must be blob_store");
    goto done;
  }
  rc = flow_queue_validate_fields(fields, "channels", channel_name, fields_allowed,
                                  sizeof(fields_allowed) / sizeof(fields_allowed[0]), error);
  if (rc != TURBO_OK) goto done;
  backend = flow_queue_string(fields, "backend");
  memset(&config, 0, sizeof(config));
  config.database_path = flow_queue_string(fields, "database_path");
  config.key = flow_queue_string(fields, "key");
  if (!backend || strcmp(backend, "sqlite") != 0) {
    rc = flow_queue_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, "channels",
                                 channel_name, "backend", "backend must be sqlite");
    goto done;
  }
  if (!config.database_path || !config.database_path[0] || !config.key || !config.key[0]) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "database_path and key are required strings");
    goto done;
  }
  key_size = strlen(config.key);
  if (key_size > TURBO_FLOW_SQLITE_BLOB_STORE_KEY_MAX || key_size >= key_capacity) {
    rc = flow_queue_config_error(error, TURBO_ENAMETOOLONG, "channels", channel_name, "key",
                                 "key exceeds the provider or caller bound");
    goto done;
  }
  if (turbo_json_object_get(fields, "busy_timeout_ms")) {
    rc = flow_queue_u64(fields, "busy_timeout_ms", INT_MAX, &number);
    if (rc != TURBO_OK) {
      rc = flow_queue_config_error(error, rc, "channels", channel_name, "busy_timeout_ms",
                                   "busy_timeout_ms must be a non-negative integer");
      goto done;
    }
    config.busy_timeout_ms = (int)number;
  }
  number = 0u;
  if (turbo_json_object_get(fields, "max_value_size")) {
    rc = flow_queue_u64(fields, "max_value_size", INT_MAX, &number);
    if (rc != TURBO_OK || number == 0u) {
      rc = flow_queue_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, "channels",
                                   channel_name, "max_value_size",
                                   "max_value_size must be a positive bounded integer");
      goto done;
    }
    config.max_value_size = (size_t)number;
  }
  rc = turbo_flow_sqlite_blob_store_create(&config, out);
  if (rc != TURBO_OK) {
    rc = flow_queue_config_error(error, rc, "channels", channel_name, NULL,
                                 "SQLite blob store creation failed");
    goto done;
  }
  memcpy(key, config.key, key_size + 1u);

done:
  turbo_free_json(&document);
  return rc;
}

int turbo_flow_sqlite_record_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, turbo_flow_config_error_t *error) {
  static const char *const fields_allowed[] = {
      "backend",       "database_path", "namespace_name", "busy_timeout_ms",
      "max_key_size",  "max_value_size", "max_batch_size", "max_records"};
  turbo_json_doc_t *document = NULL;
  turbo_flow_sqlite_record_store_config_t config;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  const char *backend;
  uint64_t number = 0u;
  int rc;
  if (!channel_name || !channel_name[0] || !out || out->size < sizeof(*out) || out->ctx)
    return TURBO_EINVAL;
  rc = flow_queue_parse_document(resolved, &document, error, "channels", channel_name);
  if (rc != TURBO_OK) return rc;
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_queue_config_error(error, TURBO_ENOENT, "channels", channel_name, NULL,
                                 "record store channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "record_store") != 0) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "channel kind must be record_store");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "record store config must be a mapping");
    goto done;
  }
  backend = flow_queue_string(fields, "backend");
  if (!backend || strcmp(backend, "sqlite") != 0) {
    rc = flow_queue_config_error(error, backend ? TURBO_ENOTSUP : TURBO_EINVAL, "channels",
                                 channel_name, "backend", "backend must be sqlite");
    goto done;
  }
  rc = flow_queue_validate_fields(fields, "channels", channel_name, fields_allowed,
                                  sizeof(fields_allowed) / sizeof(fields_allowed[0]), error);
  if (rc != TURBO_OK) goto done;
  memset(&config, 0, sizeof(config));
  config.database_path = flow_queue_string(fields, "database_path");
  config.namespace_name = flow_queue_string(fields, "namespace_name");
  if (!config.database_path || !config.database_path[0] || !config.namespace_name ||
      !config.namespace_name[0]) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "database_path and namespace_name are required strings");
    goto done;
  }
  if (strlen(config.namespace_name) > TURBO_FLOW_SQLITE_RECORD_STORE_NAMESPACE_MAX) {
    rc = flow_queue_config_error(error, TURBO_ENAMETOOLONG, "channels", channel_name,
                                 "namespace_name", "namespace_name exceeds the provider bound");
    goto done;
  }
  if (turbo_json_object_get(fields, "busy_timeout_ms")) {
    rc = flow_queue_u64(fields, "busy_timeout_ms", INT_MAX, &number);
    if (rc != TURBO_OK) {
      rc = flow_queue_config_error(error, rc, "channels", channel_name, "busy_timeout_ms",
                                   "busy_timeout_ms must be a non-negative integer");
      goto done;
    }
    config.busy_timeout_ms = (int)number;
  }
  if (turbo_json_object_get(fields, "max_key_size")) {
    rc = flow_queue_u64(fields, "max_key_size", INT_MAX, &number);
    if (rc != TURBO_OK || number == 0u) {
      rc = flow_queue_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, "channels",
                                   channel_name, "max_key_size",
                                   "max_key_size must be a positive bounded integer");
      goto done;
    }
    config.max_key_size = (size_t)number;
  }
  number = 0u;
  if (turbo_json_object_get(fields, "max_value_size")) {
    rc = flow_queue_u64(fields, "max_value_size", INT_MAX, &number);
    if (rc != TURBO_OK || number == 0u) {
      rc = flow_queue_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, "channels",
                                   channel_name, "max_value_size",
                                   "max_value_size must be a positive bounded integer");
      goto done;
    }
    config.max_value_size = (size_t)number;
  }
  number = 0u;
  if (turbo_json_object_get(fields, "max_batch_size")) {
    rc = flow_queue_u64(fields, "max_batch_size", TURBO_FLOW_QUEUE_MAX_CAPACITY, &number);
    if (rc != TURBO_OK || number == 0u) {
      rc = flow_queue_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, "channels",
                                   channel_name, "max_batch_size",
                                   "max_batch_size must be a positive bounded integer");
      goto done;
    }
    config.max_batch_size = (size_t)number;
  }
  number = 0u;
  rc = flow_queue_required_u64(fields, "max_records", INT_MAX, &number, channel_name, error);
  if (rc != TURBO_OK || number == 0u) {
    if (rc == TURBO_OK)
      rc = flow_queue_config_error(error, TURBO_ERANGE, "channels", channel_name, "max_records",
                                   "max_records must be positive");
    goto done;
  }
  config.max_records = (size_t)number;
  rc = turbo_flow_sqlite_record_store_create(&config, out);
  if (rc != TURBO_OK)
    rc = flow_queue_config_error(error, rc, "channels", channel_name, NULL,
                                 "SQLite record store creation failed");

done:
  turbo_free_json(&document);
  return rc;
}

int turbo_flow_queue_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                     const char *channel_name, turbo_flow_queue_t **out,
                                     turbo_flow_config_error_t *error) {
  static const char *const channel_fields[] = {
      "backend",          "pattern",           "resource_uid",       "owner_name",    "capacity",
      "max_payload_size", "full_policy",       "enqueue_timeout_ms", "database_path", "queue_name",
      "busy_timeout_ms",  "max_active_claims", "max_state_size"};
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  turbo_flow_queue_config_t config;
  turbo_flow_queue_claim_owner_config_t claim_config = TURBO_FLOW_QUEUE_CLAIM_OWNER_CONFIG_INIT;
  turbo_flow_sqlite_queue_config_t sqlite_config;
  turbo_flow_queue_t *queue = NULL;
  const char *backend;
  const char *pattern;
  const char *policy;
  uint64_t number;
  int rc;
  if (out) *out = NULL;
  if (!channel_name || !channel_name[0] || !out) return TURBO_EINVAL;
  rc = flow_queue_parse_document(resolved, &document, error, "channels", channel_name);
  if (rc != TURBO_OK) return rc;
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_queue_config_error(error, TURBO_ENOENT, "channels", channel_name, NULL,
                                 "channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "queue") != 0) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "channel kind must be queue");
    goto done;
  }
  rc = flow_queue_validate_fields(fields, "channels", channel_name, channel_fields,
                                  sizeof(channel_fields) / sizeof(channel_fields[0]), error);
  if (rc != TURBO_OK) goto done;
  backend = flow_queue_string(fields, "backend");
  pattern = flow_queue_string(fields, "pattern");
  if (!backend || !backend[0]) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, "backend",
                                 "backend must be a non-empty string");
    goto done;
  }
  if (!pattern || strcmp(pattern, "push_pull") != 0) {
    rc = flow_queue_config_error(error, pattern ? TURBO_ENOTSUP : TURBO_EINVAL, "channels",
                                 channel_name, "pattern",
                                 "Queue supports only the push_pull pattern");
    goto done;
  }
  memset(&config, 0, sizeof(config));
  config.resource_uid = flow_queue_string(fields, "resource_uid");
  config.owner_name = flow_queue_string(fields, "owner_name");
  policy = flow_queue_string(fields, "full_policy");
  if (!config.resource_uid || !config.resource_uid[0] || !config.owner_name ||
      !config.owner_name[0] || !policy) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                 "resource_uid, owner_name, and full_policy are required strings");
    goto done;
  }
  if (strlen(config.resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      strlen(config.owner_name) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    rc = flow_queue_config_error(error, TURBO_ENAMETOOLONG, "channels", channel_name, NULL,
                                 "resource_uid or owner_name exceeds its public bound");
    goto done;
  }
  if (strcmp(policy, "fail") == 0) config.full_policy = TURBO_FLOW_QUEUE_FULL_FAIL;
  else if (strcmp(policy, "block") == 0) config.full_policy = TURBO_FLOW_QUEUE_FULL_BLOCK;
  else if (strcmp(policy, "drop_oldest") == 0)
    config.full_policy = TURBO_FLOW_QUEUE_FULL_DROP_OLDEST;
  else {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, "full_policy",
                                 "full_policy must be fail, block, or drop_oldest");
    goto done;
  }
  rc = flow_queue_required_u64(fields, "capacity", TURBO_FLOW_QUEUE_MAX_CAPACITY, &number,
                               channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.capacity = (size_t)number;
  if (config.capacity == 0u) {
    rc = flow_queue_config_error(error, TURBO_ERANGE, "channels", channel_name, "capacity",
                                 "capacity must be greater than zero");
    goto done;
  }
  rc = flow_queue_required_u64(fields, "max_payload_size", TURBO_FLOW_QUEUE_MAX_PAYLOAD_SIZE,
                               &number, channel_name, error);
  if (rc != TURBO_OK) goto done;
  config.max_payload_size = (size_t)number;
  if (config.max_payload_size == 0u) {
    rc = flow_queue_config_error(error, TURBO_ERANGE, "channels", channel_name, "max_payload_size",
                                 "max_payload_size must be greater than zero");
    goto done;
  }
  if (turbo_json_object_get(fields, "max_active_claims")) {
    rc = flow_queue_required_u64(fields, "max_active_claims", TURBO_FLOW_QUEUE_MAX_CAPACITY,
                                 &number, channel_name, error);
    if (rc != TURBO_OK) goto done;
    if (number == 0u || number > config.capacity) {
      rc = flow_queue_config_error(error, TURBO_ERANGE, "channels", channel_name,
                                   "max_active_claims",
                                   "max_active_claims must be within queue capacity");
      goto done;
    }
    claim_config.max_active_claims = (size_t)number;
  }
  if (turbo_json_object_get(fields, "enqueue_timeout_ms")) {
    rc = flow_queue_required_u64(fields, "enqueue_timeout_ms", TURBO_FLOW_QUEUE_MAX_TIMEOUT_MS,
                                 &config.enqueue_timeout_ms, channel_name, error);
    if (rc != TURBO_OK) goto done;
  }
  if (config.full_policy == TURBO_FLOW_QUEUE_FULL_BLOCK && config.enqueue_timeout_ms == 0u) {
    rc =
        flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, "enqueue_timeout_ms",
                                "block policy requires a positive enqueue_timeout_ms");
    goto done;
  }
  if (config.full_policy != TURBO_FLOW_QUEUE_FULL_BLOCK && config.enqueue_timeout_ms != 0u) {
    rc =
        flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, "enqueue_timeout_ms",
                                "enqueue_timeout_ms is valid only for block policy");
    goto done;
  }
  if (strcmp(backend, "memory") == 0) {
    if (turbo_json_object_get(fields, "database_path") ||
        turbo_json_object_get(fields, "queue_name") ||
        turbo_json_object_get(fields, "busy_timeout_ms") ||
        turbo_json_object_get(fields, "max_state_size")) {
      rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, "backend",
                                   "memory backend does not accept SQLite fields");
      goto done;
    }
    queue = turbo_flow_queue_create(&config);
    rc = queue ? TURBO_OK
               : flow_queue_config_error(error, TURBO_ENOMEM, "channels", channel_name, NULL,
                                         "memory queue creation failed");
  } else if (strcmp(backend, "sqlite") == 0) {
    const char *database_path = flow_queue_string(fields, "database_path");
    const char *queue_name = flow_queue_string(fields, "queue_name");
    memset(&sqlite_config, 0, sizeof(sqlite_config));
    sqlite_config.queue = config;
    sqlite_config.database_path = database_path;
    sqlite_config.queue_name = queue_name;
    if (!database_path || !database_path[0] || !queue_name || !queue_name[0]) {
      rc = flow_queue_config_error(error, TURBO_EINVAL, "channels", channel_name, NULL,
                                   "SQLite database_path and queue_name are required");
      goto done;
    }
    if (strlen(queue_name) > TURBO_FLOW_QUEUE_NAME_MAX) {
      rc = flow_queue_config_error(error, TURBO_ENAMETOOLONG, "channels", channel_name,
                                   "queue_name", "queue_name exceeds its public bound");
      goto done;
    }
    if (turbo_json_object_get(fields, "busy_timeout_ms")) {
      rc =
          flow_queue_required_u64(fields, "busy_timeout_ms", INT_MAX, &number, channel_name, error);
      if (rc != TURBO_OK) goto done;
      sqlite_config.busy_timeout_ms = (int)number;
    }
    if (turbo_json_object_get(fields, "max_state_size")) {
      rc = flow_queue_required_u64(fields, "max_state_size", INT_MAX, &number, channel_name, error);
      if (rc != TURBO_OK || number == 0u) {
        if (rc == TURBO_OK)
          rc = flow_queue_config_error(error, TURBO_ERANGE, "channels", channel_name,
                                       "max_state_size", "max_state_size must be positive");
        goto done;
      }
      sqlite_config.max_state_size = (size_t)number;
    }
    queue = turbo_flow_sqlite_queue_create(&sqlite_config);
    rc = queue ? TURBO_OK
               : flow_queue_config_error(error, TURBO_EIO, "channels", channel_name, NULL,
                                         "SQLite queue creation failed");
  } else {
    rc = flow_queue_config_error(error, TURBO_ENOTSUP, "channels", channel_name, "backend",
                                 "Queue backend is not supported");
  }
  if (rc == TURBO_OK) {
    rc = turbo_flow_queue_configure_claims(queue, &claim_config);
    if (rc != TURBO_OK) {
      rc = flow_queue_config_error(error, rc, "channels", channel_name, "max_active_claims",
                                   "claim owner configuration failed");
    }
  }
  if (rc == TURBO_OK) {
    rc = flow_queue_bind_channel(queue, channel_name);
    if (rc != TURBO_OK) {
      (void)turbo_flow_queue_destroy(queue);
      queue = NULL;
      rc = flow_queue_config_error(error, rc, "channels", channel_name, NULL,
                                   "channel identity binding failed");
    }
  }
  if (rc == TURBO_OK) {
    *out = queue;
    queue = NULL;
  }

done:
  if (queue) (void)turbo_flow_queue_destroy(queue);
  turbo_free_json(&document);
  return rc;
}

int turbo_flow_queue_register_resolved_adapter(turbo_flow_t *flow, const char *name,
                                               const turbo_flow_resolved_config_t *resolved,
                                               turbo_flow_queue_t *queue,
                                               turbo_flow_config_error_t *error) {
  static const char *const adapter_fields[] = {"channel", "role"};
  turbo_json_doc_t *document = NULL;
  json_value_t *adapters;
  json_value_t *adapter;
  json_value_t *kind;
  json_value_t *fields;
  const char *channel_name;
  const char *role;
  int rc;
  if (!flow || !name || !name[0] || !queue) return TURBO_EINVAL;
  rc = flow_queue_parse_document(resolved, &document, error, "adapters", name);
  if (rc != TURBO_OK) return rc;
  adapters = turbo_json_object_get(document, "adapters");
  adapter = adapters ? turbo_json_object_get(adapters, name) : NULL;
  if (!adapter || turbo_json_type(adapter) != TURBO_JSON_OBJECT) {
    rc = flow_queue_config_error(error, TURBO_ENOENT, "adapters", name, NULL,
                                 "adapter is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(adapter, "kind");
  fields = turbo_json_object_get(adapter, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "queue") != 0) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "adapters", name, NULL,
                                 "adapter kind must be queue");
    goto done;
  }
  rc = flow_queue_validate_fields(fields, "adapters", name, adapter_fields,
                                  sizeof(adapter_fields) / sizeof(adapter_fields[0]), error);
  if (rc != TURBO_OK) goto done;
  channel_name = flow_queue_string(fields, "channel");
  role = flow_queue_string(fields, "role");
  if (!channel_name || !channel_name[0] || !role || !role[0]) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "adapters", name, NULL,
                                 "channel and role are required strings");
    goto done;
  }
  if (!flow_queue_channel_matches(queue, channel_name)) {
    rc = flow_queue_config_error(error, TURBO_EINVAL, "adapters", name, "channel",
                                 "adapter channel does not match the Queue object");
    goto done;
  }
  if (strcmp(role, "source") == 0) rc = turbo_flow_queue_register_source_adapter(flow, name, queue);
  else if (strcmp(role, "sink") == 0)
    rc = turbo_flow_queue_register_sink_adapter(flow, name, queue);
  else rc = TURBO_EINVAL;
  if (rc != TURBO_OK)
    rc = flow_queue_config_error(error, rc, "adapters", name,
                                 strcmp(role, "source") == 0 || strcmp(role, "sink") == 0 ? NULL
                                                                                          : "role",
                                 strcmp(role, "source") == 0 || strcmp(role, "sink") == 0
                                     ? "Queue adapter registration failed"
                                     : "role must be source or sink");

done:
  turbo_free_json(&document);
  return rc;
}
