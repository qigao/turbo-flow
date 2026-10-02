#include "turbo_flow_turbodb_durable_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int typed_fail(turbo_flow_config_error_t *error, int status,
                      const char *name, const char *field,
                      const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path),
                   "$.stages.%s.config%s%s", name ? name : "",
                   field && field[0] ? "." : "", field ? field : "");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "invalid TurboDB provider configuration");
  }
  return status;
}

int durable_turbodb_config_from_typed(
    const DurableTurboDbConfig_t *typed, const char *name,
    durable_turbodb_config_t *out, turbo_flow_config_error_t *error) {
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
    case 0u:
      out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_GENERATED;
      break;
    case 1u:
      out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
      break;
    default:
      return typed_fail(error, SALTS_EINVAL, name, "identity_mode",
                        "unknown durable identity mode");
  }

  switch (typed->open_mode) {
    case 0u:
      if (typed->expected_generation != 0u)
        return typed_fail(
            error, SALTS_EINVAL, name, "expected_generation",
            "exclusive open requires expected_generation == 0");
      out->inbox.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE;
      break;
    case 1u:
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
  out->inbox.max_records = (size_t)typed->max_records;
  out->inbox.max_total_bytes = (size_t)typed->max_total_bytes;
  out->inbox.max_record_bytes = (size_t)typed->max_record_bytes;
  out->inbox.max_claims = (size_t)typed->max_claims;
  out->inbox.connection_count = typed->connection_count;
  out->inbox.expected_generation = typed->expected_generation;
  return SALTS_OK;
}
