#include "turbo_flow_turbodb_durable_internal.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>

static int fail(turbo_flow_config_error_t *error, int status, const char *name,
                const char *field) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", name,
                   field ? field : "");
    (void)snprintf(error->message, sizeof(error->message),
                   "invalid configured TurboDB durable provider field: %s",
                   field && field[0] ? field : "config");
  }
  return status;
}

static int get_number(const turbo_flow_resolved_adapter_view_t *fields, const char *name,
                      const char *field, uint64_t maximum, uint64_t *out,
                      turbo_flow_config_error_t *error) {
  turbo_flow_config_value_type_t type;
  int rc = turbo_flow_resolved_adapter_field_type(fields, field, &type);
  if (rc != SALTS_OK) return fail(error, rc, name, field);
  if (type != TURBO_FLOW_CONFIG_NUMBER) return fail(error, SALTS_EINVAL, name, field);
  rc = turbo_flow_resolved_adapter_get_u64(fields, field, out);
  if (rc != SALTS_OK) return fail(error, rc, name, field);
  if (!*out || *out > maximum) return fail(error, SALTS_ERANGE, name, field);
  return SALTS_OK;
}

static int get_text(const turbo_flow_resolved_adapter_view_t *fields, const char *name,
                    const char *field, char *out, size_t capacity,
                    turbo_flow_config_error_t *error) {
  const char *value = NULL;
  int rc = turbo_flow_resolved_adapter_get_string(fields, field, &value);
  if (rc != SALTS_OK) return fail(error, rc, name, field);
  if (!value || !value[0] || strlen(value) >= capacity)
    return fail(error, SALTS_ERANGE, name, field);
  memcpy(out, value, strlen(value) + 1u);
  return SALTS_OK;
}

static int namespace_valid(const char *value) {
  if (!value || !value[0]) return 0;
  if (!((value[0] >= 'A' && value[0] <= 'Z') ||
        (value[0] >= 'a' && value[0] <= 'z') || value[0] == '_'))
    return 0;
  for (size_t i = 1u; value[i]; ++i)
    if (!((value[i] >= 'A' && value[i] <= 'Z') ||
          (value[i] >= 'a' && value[i] <= 'z') ||
          (value[i] >= '0' && value[i] <= '9') || value[i] == '_'))
      return 0;
  return 1;
}

int durable_turbodb_config_read(const turbo_flow_resolved_config_t *resolved, const char *name,
                                durable_turbodb_config_t *out,
                                turbo_flow_config_error_t *error) {
  static const char *const numbers[] = {"schema_version", "max_message_bytes", "max_records",
                                        "max_total_bytes", "max_record_bytes", "max_claims",
                                        "connection_count"};
  uint64_t values[sizeof(numbers) / sizeof(numbers[0])] = {0u};
  uint64_t expected_generation = 0u;
  turbo_flow_resolved_channel_view_t channel = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
  turbo_flow_resolved_adapter_view_t fields = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  char identity[32];
  char open_mode[32];
  int rc;
  if (!resolved || !name || !out) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  rc = turbo_flow_resolved_config_channel(resolved, name, &channel);
  if (rc != SALTS_OK) return fail(error, rc, name, NULL);
  if (!channel.kind || strcmp(channel.kind, FLOW_DURABLE_TURBODB_KIND))
    return fail(error, SALTS_EINVAL, name, "kind");
  fields.name = channel.name;
  fields.kind = channel.kind;
  fields.config = channel.config;
  if (turbo_flow_resolved_adapter_field_count(&fields) != 12u)
    return fail(error, SALTS_EINVAL, name, NULL);
  for (size_t i = 0u; i < sizeof(numbers) / sizeof(numbers[0]); ++i) {
    uint64_t maximum = i == 6u ? TURBO_FLOW_TURBODB_INBOX_MAX_CONNECTIONS : INT64_MAX;
    rc = get_number(&fields, name, numbers[i], maximum, &values[i], error);
    if (rc != SALTS_OK) return rc;
  }
  if (values[0] != 2u) return fail(error, SALTS_EINVAL, name, numbers[0]);
  if (values[4] > values[3]) return fail(error, SALTS_ERANGE, name, numbers[4]);
  if (values[5] > values[2]) return fail(error, SALTS_ERANGE, name, numbers[5]);
  rc = get_text(&fields, name, "identity_mode", identity, sizeof(identity), error);
  if (rc != SALTS_OK) return rc;
  if (!strcmp(identity, "generated"))
    out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_GENERATED;
  else if (!strcmp(identity, "stable_required"))
    out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
  else
    return fail(error, SALTS_EINVAL, name, "identity_mode");
  rc = get_text(&fields, name, "filename", out->filename, sizeof(out->filename), error);
  if (rc != SALTS_OK) return rc;
  if (!strcmp(out->filename, ":memory:")) return fail(error, SALTS_ENOTSUP, name, "filename");
  rc = get_text(&fields, name, "namespace", out->namespace_name,
                sizeof(out->namespace_name), error);
  if (rc != SALTS_OK) return rc;
  if (!namespace_valid(out->namespace_name)) return fail(error, SALTS_EINVAL, name, "namespace");
  {
    turbo_flow_config_value_type_t type;
    rc = turbo_flow_resolved_adapter_field_type(&fields, "expected_generation", &type);
    if (rc != SALTS_OK) return fail(error, rc, name, "expected_generation");
    if (type != TURBO_FLOW_CONFIG_NUMBER)
      return fail(error, SALTS_EINVAL, name, "expected_generation");
    rc = turbo_flow_resolved_adapter_get_u64(&fields, "expected_generation",
                                             &expected_generation);
    if (rc != SALTS_OK || expected_generation > INT64_MAX)
      return fail(error, rc != SALTS_OK ? rc : SALTS_ERANGE, name,
                  "expected_generation");
  }
  rc = get_text(&fields, name, "open_mode", open_mode, sizeof(open_mode), error);
  if (rc != SALTS_OK) return rc;
  if (!strcmp(open_mode, "exclusive")) {
    if (expected_generation != 0u)
      return fail(error, SALTS_EINVAL, name, "expected_generation");
    out->inbox.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE;
  } else if (!strcmp(open_mode, "takeover")) {
    if (expected_generation == 0u)
      return fail(error, SALTS_EINVAL, name, "expected_generation");
    out->inbox.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
  } else {
    return fail(error, SALTS_ENOTSUP, name, "open_mode");
  }
  out->max_message_bytes = (size_t)values[1];
  out->inbox = turbo_flow_turbodb_inbox_config_default();
  out->inbox.namespace_name = out->namespace_name;
  out->inbox.open_mode = !strcmp(open_mode, "takeover")
                             ? TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER
                             : TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE;
  out->inbox.max_records = (size_t)values[2];
  out->inbox.max_total_bytes = (size_t)values[3];
  out->inbox.max_record_bytes = (size_t)values[4];
  out->inbox.max_claims = (size_t)values[5];
  out->inbox.connection_count = (uint32_t)values[6];
  out->inbox.expected_generation = expected_generation;
  return SALTS_OK;
}


static int typed_fail(turbo_flow_config_error_t *error, int status,
                      const char *name, const char *field,
                      const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path),
                   "$.resources.%s.config%s%s", name ? name : "",
                   field && field[0] ? "." : "", field ? field : "");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "invalid TurboDB provider configuration");
  }
  return status;
}

static int copy_typed_text(tstr source, char *target, size_t capacity) {
  const size_t length = source ? tstr_len(source) : 0u;
  if (!source || length == 0u || length >= capacity) return SALTS_ERANGE;
  memcpy(target, source, length);
  target[length] = '\0';
  return SALTS_OK;
}

int durable_turbodb_config_from_typed(
    const DurableTurboDbConfig_t *typed, const char *name,
    durable_turbodb_config_t *out, turbo_flow_config_error_t *error) {
  int rc;

  if (!typed || !name || !name[0] || !out) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));

  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_EPROTO, name, "schema_version",
                      "TurboDB provider config schema version must be 2");

  if (typed->max_message_bytes > SIZE_MAX ||
      typed->max_records > SIZE_MAX ||
      typed->max_total_bytes > SIZE_MAX ||
      typed->max_record_bytes > SIZE_MAX ||
      typed->max_claims > SIZE_MAX)
    return typed_fail(error, SALTS_ERANGE, name, NULL,
                      "TurboDB provider size exceeds this platform");

  if (typed->max_record_bytes > typed->max_total_bytes)
    return typed_fail(error, SALTS_ERANGE, name, "max_record_bytes",
                      "max_record_bytes exceeds max_total_bytes");
  if (typed->max_claims > typed->max_records)
    return typed_fail(error, SALTS_ERANGE, name, "max_claims",
                      "max_claims exceeds max_records");
  if (typed->connection_count == 0u ||
      typed->connection_count > TURBO_FLOW_TURBODB_INBOX_MAX_CONNECTIONS)
    return typed_fail(error, SALTS_ERANGE, name, "connection_count",
                      "connection_count is outside the supported bound");

  out->inbox = turbo_flow_turbodb_inbox_config_default();

  switch (typed->identity_mode) {
    case DurableIdentityMode_Generated:
      out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_GENERATED;
      break;
    case DurableIdentityMode_StableRequired:
      out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
      break;
    default:
      return typed_fail(error, SALTS_EINVAL, name, "identity_mode",
                        "unknown durable identity mode");
  }

  rc = copy_typed_text(typed->filename, out->filename, sizeof(out->filename));
  if (rc != SALTS_OK)
    return typed_fail(error, rc, name, "filename",
                      "filename exceeds the admitted native bound");
  if (strcmp(out->filename, ":memory:") == 0)
    return typed_fail(error, SALTS_ENOTSUP, name, "filename",
                      "in-memory TurboDB durable storage is not supported");

  rc = copy_typed_text(
      typed->namespace_name, out->namespace_name, sizeof(out->namespace_name));
  if (rc != SALTS_OK)
    return typed_fail(error, rc, name, "namespace_name",
                      "namespace exceeds the admitted native bound");

  switch (typed->open_mode) {
    case InboxOpenMode_Exclusive:
      if (typed->expected_generation != 0u)
        return typed_fail(
            error, SALTS_EINVAL, name, "expected_generation",
            "exclusive open requires expected_generation == 0");
      out->inbox.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE;
      break;
    case InboxOpenMode_Takeover:
      if (typed->expected_generation == 0u)
        return typed_fail(
            error, SALTS_EINVAL, name, "expected_generation",
            "takeover open requires a non-zero expected_generation");
      out->inbox.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
      break;
    default:
      return typed_fail(error, SALTS_EINVAL, name, "open_mode",
                        "unknown TurboDB inbox open mode");
  }

  out->max_message_bytes = (size_t)typed->max_message_bytes;
  out->inbox.namespace_name = out->namespace_name;
  out->inbox.max_records = (size_t)typed->max_records;
  out->inbox.max_total_bytes = (size_t)typed->max_total_bytes;
  out->inbox.max_record_bytes = (size_t)typed->max_record_bytes;
  out->inbox.max_claims = (size_t)typed->max_claims;
  out->inbox.connection_count = typed->connection_count;
  out->inbox.expected_generation = typed->expected_generation;
  return SALTS_OK;
}
