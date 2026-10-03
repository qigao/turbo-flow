#include "turbo_flow_durable_memory_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int typed_fail(
    turbo_flow_config_error_t *error, int status, const char *name,
    const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(
        error->path, sizeof(error->path),
        "$.stages.%s.config%s%s",
        name ? name : "", field && field[0] ? "." : "", field ? field : "");
    (void)snprintf(
        error->message, sizeof(error->message), "%s",
        message ? message : "invalid bounded memory provider policy");
  }
  return status;
}

int durable_memory_config_from_typed(
    const DurableMemoryConfig_t *typed,
    const turbo_flow_durable_memory_resource_view_t *resource,
    const char *name,
    durable_memory_config_t *out,
    turbo_flow_config_error_t *error) {
  if (!typed || !resource || !name || !name[0] || !out)
    return SALTS_EINVAL;
  if (!turbo_flow_durable_memory_resource_view_valid(resource))
    return typed_fail(
        error, SALTS_EPROTO, name, "resource",
        "memory deployment resource limits are invalid");

  memset(out, 0, sizeof(*out));
  if (typed->schema_version != 2u)
    return typed_fail(
        error, SALTS_EPROTO, name, "schema_version",
        "memory provider schema version must be 2");

  if (typed->max_message_bytes > SIZE_MAX ||
      typed->max_records > SIZE_MAX ||
      typed->max_total_bytes > SIZE_MAX ||
      typed->max_record_bytes > SIZE_MAX ||
      typed->max_claims > SIZE_MAX)
    return typed_fail(
        error, SALTS_ERANGE, name, NULL,
        "memory provider size exceeds this platform");

  if (typed->max_records > resource->max_records)
    return typed_fail(
        error, SALTS_ENOSPC, name, "max_records",
        "requested memory record capacity exceeds deployment resource");
  if (typed->max_total_bytes > resource->max_total_bytes)
    return typed_fail(
        error, SALTS_ENOSPC, name, "max_total_bytes",
        "requested memory byte capacity exceeds deployment resource");
  if (typed->max_record_bytes > resource->max_record_bytes)
    return typed_fail(
        error, SALTS_ENOSPC, name, "max_record_bytes",
        "requested record bound exceeds deployment resource");
  if (typed->max_claims > resource->max_claims)
    return typed_fail(
        error, SALTS_ENOSPC, name, "max_claims",
        "requested claim capacity exceeds deployment resource");

  if (typed->max_record_bytes > typed->max_total_bytes)
    return typed_fail(
        error, SALTS_ERANGE, name, "max_record_bytes",
        "max_record_bytes exceeds max_total_bytes");
  if (typed->max_claims > typed->max_records)
    return typed_fail(
        error, SALTS_ERANGE, name, "max_claims",
        "max_claims exceeds max_records");

  switch (typed->identity_mode) {
    case 0u:
      out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_GENERATED;
      break;
    case 1u:
      out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
      break;
    default:
      return typed_fail(
          error, SALTS_EINVAL, name, "identity_mode",
          "unknown durable identity mode");
  }

  out->max_message_bytes = (size_t)typed->max_message_bytes;
  out->memory = turbo_flow_inbox_memory_config_default();
  out->memory.max_records = (size_t)typed->max_records;
  out->memory.max_total_bytes = (size_t)typed->max_total_bytes;
  out->memory.max_record_bytes = (size_t)typed->max_record_bytes;
  out->memory.max_claims = (size_t)typed->max_claims;
  return SALTS_OK;
}
