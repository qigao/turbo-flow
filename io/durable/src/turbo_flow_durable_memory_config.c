#include "turbo_flow_durable_memory_internal.h"
#include <stdio.h>
#include <string.h>

static int fail(turbo_flow_config_error_t *e, int rc, const char *name, const char *field) {
  if (e && e->size >= sizeof(*e)) {
    *e = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    e->status = rc;
    (void)snprintf(e->path, sizeof(e->path), "$.channels.%s.config.%s", name, field);
    (void)snprintf(e->message, sizeof(e->message), "invalid bounded memory provider field: %s", field);
  }
  return rc;
}
int durable_memory_config_read(const turbo_flow_resolved_config_t *resolved, const char *name,
                               durable_memory_config_t *out, turbo_flow_config_error_t *error) {
  static const char *const numbers[] = {"schema_version", "max_message_bytes", "max_records",
                                        "max_total_bytes", "max_record_bytes", "max_claims"};
  uint64_t values[sizeof(numbers)/sizeof(numbers[0])] = {0};
  turbo_flow_resolved_channel_view_t channel = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
  turbo_flow_resolved_adapter_view_t fields = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  const char *identity = NULL;
  int rc;
  if (!resolved || !name || !out) return SALTS_EINVAL;
  rc = turbo_flow_resolved_config_channel(resolved, name, &channel);
  if (rc != SALTS_OK) return fail(error, rc, name, "");
  if (!channel.kind || strcmp(channel.kind, FLOW_DURABLE_MEMORY_KIND))
    return fail(error, SALTS_EINVAL, name, "kind");
  /* Both immutable projections expose the same canonical config object. */
  fields.name = channel.name; fields.kind = channel.kind; fields.config = channel.config;
  if (turbo_flow_resolved_adapter_field_count(&fields) != sizeof(numbers)/sizeof(numbers[0])+1u)
    return fail(error, SALTS_EINVAL, name, "");
  for (size_t i=0u; i<sizeof(numbers)/sizeof(numbers[0]); ++i) {
    turbo_flow_config_value_type_t type;
    rc = turbo_flow_resolved_adapter_field_type(&fields, numbers[i], &type);
    if (rc != SALTS_OK) return fail(error, rc, name, numbers[i]);
    if (type != TURBO_FLOW_CONFIG_NUMBER) return fail(error, SALTS_EINVAL, name, numbers[i]);
    rc = turbo_flow_resolved_adapter_get_u64(&fields, numbers[i], &values[i]);
    if (rc != SALTS_OK) return fail(error, rc, name, numbers[i]);
    if (!values[i] || values[i] >= SIZE_MAX) return fail(error, SALTS_ERANGE, name, numbers[i]);
  }
  if (values[0] != 1u) return fail(error, SALTS_EINVAL, name, numbers[0]);
  if (values[2] > TURBO_FLOW_INBOX_MEMORY_MAX_RECORDS)
    return fail(error, SALTS_ERANGE, name, numbers[2]);
  if (values[3] > TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES)
    return fail(error, SALTS_ERANGE, name, numbers[3]);
  if (values[4] > values[3]) return fail(error, SALTS_ERANGE, name, numbers[4]);
  if (values[5] > values[2]) return fail(error, SALTS_ERANGE, name, numbers[5]);
  rc = turbo_flow_resolved_channel_get_string(&channel, "identity_mode", &identity);
  if (rc != SALTS_OK) return fail(error, rc, name, "identity_mode");
  if (!strcmp(identity, "generated")) out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_GENERATED;
  else if (!strcmp(identity, "stable_required")) out->identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
  else return fail(error, SALTS_EINVAL, name, "identity_mode");
  out->max_message_bytes = (size_t)values[1];
  out->memory = turbo_flow_inbox_memory_config_default();
  out->memory.max_records = (size_t)values[2];
  out->memory.max_total_bytes = (size_t)values[3];
  out->memory.max_record_bytes = (size_t)values[4];
  out->memory.max_claims = (size_t)values[5];
  return SALTS_OK;
}
