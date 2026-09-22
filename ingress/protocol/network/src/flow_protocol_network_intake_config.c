#include "flow_protocol_network_intake_internal.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>

static int intake_config_error(turbo_flow_config_error_t *error, int status, const char *path,
                               const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s", path ? path : "$.protocol_intake");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int intake_config_field_error(turbo_flow_config_error_t *error, int status,
                                     const char *adapter_name, const char *field,
                                     const char *message) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  (void)snprintf(path, sizeof(path), "$.adapters.%s.config%s%s",
                 adapter_name ? adapter_name : "?", field ? "." : "", field ? field : "");
  return intake_config_error(error, status, path, message);
}

static int intake_name_copy(const char *value, char *out, size_t capacity) {
  const size_t length = value ? strlen(value) : 0u;
  if (!value || length == 0u) return SALTS_EINVAL;
  if (length >= capacity) return SALTS_ERANGE;
  memcpy(out, value, length + 1u);
  return SALTS_OK;
}

static int intake_adapter_string(const turbo_flow_resolved_adapter_view_t *view,
                                 const char *adapter_name, const char *field, char *out,
                                 size_t capacity, turbo_flow_config_error_t *error) {
  const char *value = NULL;
  int rc = turbo_flow_resolved_adapter_get_string(view, field, &value);
  if (rc != SALTS_OK || !value || !value[0])
    return intake_config_field_error(error, SALTS_EINVAL, adapter_name, field,
                                     "expected non-empty string");
  rc = intake_name_copy(value, out, capacity);
  if (rc != SALTS_OK)
    return intake_config_field_error(error, rc, adapter_name, field,
                                     "string exceeds supported bound");
  return SALTS_OK;
}

static int intake_adapter_size(const turbo_flow_resolved_adapter_view_t *view,
                               const char *adapter_name, const char *field, size_t *out,
                               turbo_flow_config_error_t *error) {
  uint64_t raw = 0u;
  int rc;
  if (!out) return SALTS_EINVAL;
  rc = turbo_flow_resolved_adapter_get_u64(view, field, &raw);
  if (rc != SALTS_OK)
    return intake_config_field_error(error, SALTS_EINVAL, adapter_name, field,
                                     "expected unsigned integer");
  if (raw == 0u || raw > (uint64_t)SIZE_MAX)
    return intake_config_field_error(error, SALTS_ERANGE, adapter_name, field,
                                     "value is outside supported positive range");
  *out = (size_t)raw;
  return SALTS_OK;
}

static int intake_adapter_schema_version(const turbo_flow_resolved_adapter_view_t *view,
                                         const char *adapter_name, uint32_t *version_out,
                                         turbo_flow_config_error_t *error) {
  uint64_t version = 0u;
  int rc;
  if (!version_out) return SALTS_EINVAL;
  rc = turbo_flow_resolved_adapter_get_u64(view, "schema_version", &version);
  if (rc != SALTS_OK)
    return intake_config_field_error(error, SALTS_EINVAL, adapter_name, "schema_version",
                                     "schema_version is required");
  if (version != 2u && version != 3u)
    return intake_config_field_error(error, SALTS_ENOTSUP, adapter_name, "schema_version",
                                     "protocol.decode supports schema versions 2 and 3");
  *version_out = (uint32_t)version;
  return SALTS_OK;
}

static int intake_field_allowed(const char *field, uint32_t schema_version) {
  static const char *const base_fields[] = {
      "schema_version", "protocol_provider", "protocol_kind", "protocol_version",
      "source_id", "max_sessions", "max_frame_size", "max_pending_claims",
      "max_pending_bytes"};
  static const char *const mapper_fields[] = {
      "mapper_plugin", "mapper_name", "mapper_profile", "mapper_message_type",
      "mapper_semantic_type", "mapper_semantic_media_type",
      "mapper_max_semantic_bytes", "mapper_max_output_bytes"};
  for (size_t i = 0u; i < sizeof(base_fields) / sizeof(base_fields[0]); ++i)
    if (field && strcmp(field, base_fields[i]) == 0) return 1;
  if (schema_version == 3u) {
    for (size_t i = 0u; i < sizeof(mapper_fields) / sizeof(mapper_fields[0]); ++i)
      if (field && strcmp(field, mapper_fields[i]) == 0) return 1;
  }
  return 0;
}

static int intake_validate_exact_fields(const turbo_flow_resolved_adapter_view_t *view,
                                        const char *adapter_name, uint32_t schema_version,
                                        turbo_flow_config_error_t *error) {
  const size_t required = schema_version == 3u ? 17u : 9u;
  const size_t actual = turbo_flow_resolved_adapter_field_count(view);
  for (size_t i = 0u; i < actual; ++i) {
    const char *field = turbo_flow_resolved_adapter_field_name(view, i);
    if (!field || !intake_field_allowed(field, schema_version))
      return intake_config_field_error(error, SALTS_EINVAL, adapter_name, field,
                                       "unknown protocol.decode field");
  }
  if (actual != required)
    return intake_config_field_error(
        error, SALTS_EINVAL, adapter_name, NULL,
        schema_version == 3u ? "protocol.decode v3 requires every mapper field exactly once"
                             : "protocol.decode v2 requires every field exactly once");
  return SALTS_OK;
}

static int intake_adapter_u32(const turbo_flow_resolved_adapter_view_t *view,
                              const char *adapter_name, const char *field,
                              uint32_t *out, turbo_flow_config_error_t *error) {
  uint64_t raw = 0u;
  int rc;
  if (!out) return SALTS_EINVAL;
  rc = turbo_flow_resolved_adapter_get_u64(view, field, &raw);
  if (rc != SALTS_OK || raw > UINT32_MAX)
    return intake_config_field_error(error, rc == SALTS_OK ? SALTS_ERANGE : SALTS_EINVAL,
                                     adapter_name, field,
                                     "expected uint32 value");
  *out = (uint32_t)raw;
  return SALTS_OK;
}

static int intake_read_source(const turbo_flow_resolved_adapter_view_t *source,
                              const char *source_adapter_name,
                              flow_protocol_network_intake_settings_t *settings,
                              size_t *transport_capacity, turbo_flow_config_error_t *error) {
  const char *mode = NULL;
  int rc;
  if (!source || !source->kind || !settings || !transport_capacity)
    return intake_config_error(error, SALTS_EINVAL, "$.adapters", "invalid source projection");

  if (strcmp(source->kind, "cnet.listener_source") == 0) {
    settings->transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP;
    rc = intake_adapter_size(source, source_adapter_name, "max_connections", transport_capacity,
                             error);
  } else if (strcmp(source->kind, "cnet.packet_source") == 0) {
    rc = turbo_flow_resolved_adapter_get_string(source, "packet_mode", &mode);
    if (rc != SALTS_OK || !mode || !mode[0])
      return intake_config_field_error(error, SALTS_EINVAL, source_adapter_name, "packet_mode",
                                       "packet_mode is required");
    if (strcmp(mode, "udp") != 0)
      return intake_config_field_error(error, SALTS_ENOTSUP, source_adapter_name, "packet_mode",
                                       "ProtocolNetworkIntake supports UDP packet sources only");
    settings->transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP;
    rc = intake_adapter_size(source, source_adapter_name, "session_capacity", transport_capacity,
                             error);
  } else {
    return intake_config_error(error, SALTS_ENOTSUP, "$.adapters",
                               "ProtocolNetworkIntake supports CNet listener or UDP packet sources");
  }
  if (rc != SALTS_OK) return rc;
  rc = intake_adapter_size(source, source_adapter_name, "scheduler_max_steps_per_poll",
                           &settings->source_scheduler_max_steps, error);
  if (rc != SALTS_OK) return rc;
  return intake_adapter_size(source, source_adapter_name, "max_message_bytes",
                             &settings->source_max_message_bytes, error);
}

static int intake_read_protocol(const turbo_flow_resolved_adapter_view_t *intake,
                                const char *decoder_adapter_name,
                                flow_protocol_network_intake_settings_t *settings,
                                turbo_flow_config_error_t *error) {
  char kind[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u] = {0};
  int rc = intake_adapter_schema_version(intake, decoder_adapter_name,
                                         &settings->schema_version, error);
  if (rc != SALTS_OK) return rc;
  rc = intake_adapter_string(intake, decoder_adapter_name, "protocol_provider",
                             settings->protocol_provider, sizeof(settings->protocol_provider), error);
  if (rc == SALTS_OK)
    rc = intake_adapter_string(intake, decoder_adapter_name, "protocol_kind", kind, sizeof(kind),
                               error);
  if (rc == SALTS_OK)
    rc = intake_adapter_string(intake, decoder_adapter_name, "protocol_version",
                               settings->protocol_version, sizeof(settings->protocol_version), error);
  if (rc == SALTS_OK)
    rc = intake_adapter_string(intake, decoder_adapter_name, "source_id", settings->source_id,
                               sizeof(settings->source_id), error);
  if (rc != SALTS_OK) return rc;

  if (strcmp(kind, "jtt808") == 0) settings->protocol_kind = TURBO_FLOW_PROTOCOL_JTT_808;
  else if (strcmp(kind, "coap") == 0) settings->protocol_kind = TURBO_FLOW_PROTOCOL_COAP;
  else
    return intake_config_field_error(error, SALTS_ENOTSUP, decoder_adapter_name, "protocol_kind",
                                     "ProtocolNetworkIntake supports jtt808 or coap only");

  rc = intake_adapter_size(intake, decoder_adapter_name, "max_sessions", &settings->max_sessions,
                           error);
  if (rc == SALTS_OK)
    rc = intake_adapter_size(intake, decoder_adapter_name, "max_frame_size", &settings->max_frame_size,
                             error);
  if (rc == SALTS_OK)
    rc = intake_adapter_size(intake, decoder_adapter_name, "max_pending_claims",
                             &settings->max_pending_claims, error);
  if (rc == SALTS_OK)
    rc = intake_adapter_size(intake, decoder_adapter_name, "max_pending_bytes",
                             &settings->max_pending_bytes, error);
  if (rc != SALTS_OK || settings->schema_version != 3u) return rc;

  rc = intake_adapter_string(intake, decoder_adapter_name, "mapper_plugin",
                             settings->mapper_plugin, sizeof(settings->mapper_plugin), error);
  if (rc == SALTS_OK)
    rc = intake_adapter_string(intake, decoder_adapter_name, "mapper_name",
                               settings->mapper_name, sizeof(settings->mapper_name), error);
  if (rc == SALTS_OK)
    rc = intake_adapter_string(intake, decoder_adapter_name, "mapper_profile",
                               settings->mapper_profile, sizeof(settings->mapper_profile), error);
  if (rc == SALTS_OK)
    rc = intake_adapter_u32(intake, decoder_adapter_name, "mapper_message_type",
                            &settings->mapper_message_type, error);
  if (rc == SALTS_OK)
    rc = intake_adapter_u32(intake, decoder_adapter_name, "mapper_semantic_type",
                            &settings->mapper_semantic_type, error);
  if (rc == SALTS_OK)
    rc = intake_adapter_string(intake, decoder_adapter_name, "mapper_semantic_media_type",
                               settings->mapper_semantic_media_type,
                               sizeof(settings->mapper_semantic_media_type), error);
  if (rc == SALTS_OK)
    rc = intake_adapter_size(intake, decoder_adapter_name, "mapper_max_semantic_bytes",
                             &settings->mapper_max_semantic_bytes, error);
  if (rc == SALTS_OK)
    rc = intake_adapter_size(intake, decoder_adapter_name, "mapper_max_output_bytes",
                             &settings->mapper_max_output_bytes, error);
  if (rc == SALTS_OK && settings->mapper_max_semantic_bytes > settings->max_frame_size)
    rc = intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name,
                                   "mapper_max_semantic_bytes",
                                   "mapper semantic bound must not exceed frame bound");
  return rc;
}

static int intake_validate_pairing(const flow_protocol_network_intake_settings_t *settings,
                                   const char *decoder_adapter_name,
                                   turbo_flow_config_error_t *error) {
  if (settings->protocol_kind == TURBO_FLOW_PROTOCOL_JTT_808) {
    if (settings->transport_kind != FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP ||
        strcmp(settings->protocol_provider, "jtt808") != 0)
      return intake_config_field_error(error, SALTS_EINVAL, decoder_adapter_name, "protocol_provider",
                                       "jtt808 requires the jtt808 provider and TCP listener source");
    if (strcmp(settings->protocol_version, "2019-A1") != 0)
      return intake_config_field_error(error, SALTS_ENOTSUP, decoder_adapter_name, "protocol_version",
                                       "jtt808 network intake requires version 2019-A1");
    return SALTS_OK;
  }

  if (settings->protocol_kind == TURBO_FLOW_PROTOCOL_COAP) {
    if (settings->transport_kind != FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP ||
        strcmp(settings->protocol_provider, "coap") != 0)
      return intake_config_field_error(error, SALTS_EINVAL, decoder_adapter_name, "protocol_provider",
                                       "coap requires the coap provider and UDP packet source");
    if (strcmp(settings->protocol_version, "RFC7252") != 0)
      return intake_config_field_error(error, SALTS_ENOTSUP, decoder_adapter_name, "protocol_version",
                                       "coap network intake requires version RFC7252");
    return SALTS_OK;
  }
  return intake_config_field_error(error, SALTS_ENOTSUP, decoder_adapter_name, "protocol_kind",
                                   "unsupported protocol kind");
}

static int intake_validate_bounds(const flow_protocol_network_intake_settings_t *settings,
                                  size_t transport_capacity, const char *decoder_adapter_name,
                                  turbo_flow_config_error_t *error) {
  size_t required_claims;
  size_t required_bytes;
  if (settings->max_sessions != transport_capacity)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name, "max_sessions",
                                     "protocol sessions must equal transport capacity");
  if (settings->max_sessions == SIZE_MAX ||
      settings->max_frame_size > SIZE_MAX / (settings->max_sessions + 1u))
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name, "max_frame_size",
                                     "protocol parser storage bound overflows");
  if (settings->source_scheduler_max_steps > SIZE_MAX / 2u)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name,
                                     "max_pending_claims", "source progress bound overflows");
  required_claims = settings->source_scheduler_max_steps * 2u;
  if (settings->max_pending_claims < required_claims)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name,
                                     "max_pending_claims",
                                     "pending claim capacity is below the bounded source progress budget");
  if (settings->source_max_message_bytes > SIZE_MAX / settings->max_pending_claims)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name,
                                     "max_pending_bytes", "pending byte budget overflows");
  required_bytes = settings->source_max_message_bytes * settings->max_pending_claims;
  if (settings->max_pending_bytes < required_bytes)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name,
                                     "max_pending_bytes",
                                     "pending byte capacity is below the bounded claim budget");
  return SALTS_OK;
}

static int intake_validate_topology(const turbo_flow_t *flow, const char *source_adapter_name,
                                    const char *decoder_adapter_name,
                                    turbo_flow_config_error_t *error) {
  size_t source_index = SIZE_MAX;
  size_t sink_index = SIZE_MAX;
  const turbo_flow_edge_plan_t *edge;
  if (turbo_flow_state(flow) != TURBO_FLOW_STATE_PARSED)
    return intake_config_error(error, SALTS_EINVAL, "$.graph", "intake Flow must be parsed");
  if (turbo_flow_stage_count(flow) != 2u || turbo_flow_edge_count(flow) != 1u)
    return intake_config_error(error, SALTS_EINVAL, "$.graph",
                               "intake Flow must contain exactly two stages and one edge");

  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->adapter_name) continue;
    if (strcmp(stage->adapter_name, source_adapter_name) == 0) {
      if (!stage->is_source || source_index != SIZE_MAX)
        return intake_config_error(error, SALTS_EINVAL, "$.graph",
                                   "configured intake Source must bind exactly one source stage");
      source_index = i;
    }
    if (strcmp(stage->adapter_name, decoder_adapter_name) == 0) {
      if (stage->is_source || sink_index != SIZE_MAX)
        return intake_config_error(error, SALTS_EINVAL, "$.graph",
                                   "protocol.decode must bind exactly one terminal stage");
      sink_index = i;
    }
  }
  if (source_index == SIZE_MAX || sink_index == SIZE_MAX)
    return intake_config_error(error, SALTS_EINVAL, "$.graph",
                               "configured Source or protocol.decode stage is missing");
  /*
   * PARSED Graph edges intentionally keep from_stage/to_stage unresolved until
   * turbo_flow_compile(). Preflight can prove the bounded two-node shape and
   * edge policy here; create validates the resolved Source->decoder endpoints
   * immediately after compile, before the owner is published.
   */
  edge = turbo_flow_edge_at(flow, 0u);
  if (!edge || edge->kind != TURBO_FLOW_EDGE_UNCONDITIONAL)
    return intake_config_error(error, SALTS_EINVAL, "$.graph",
                               "intake Flow edge must be unconditional");
  return SALTS_OK;
}

int flow_protocol_network_intake_preflight(
    const turbo_flow_resolved_config_t *resolved, const turbo_flow_t *flow,
    const char *source_adapter_name, const char *decoder_adapter_name,
    flow_protocol_network_intake_settings_t *settings, turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t source = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_resolved_adapter_view_t intake = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  flow_protocol_network_intake_settings_t current;
  size_t transport_capacity = 0u;
  int rc;

  if (settings) memset(settings, 0, sizeof(*settings));
  if (!resolved || !flow || !source_adapter_name || !source_adapter_name[0] ||
      !decoder_adapter_name || !decoder_adapter_name[0] || !settings || !error ||
      error->size < sizeof(*error))
    return intake_config_error(error, SALTS_EINVAL, "$.protocol_intake",
                               "invalid network intake preflight arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  memset(&current, 0, sizeof(current));

  rc = intake_name_copy(source_adapter_name, current.source_adapter_name,
                        sizeof(current.source_adapter_name));
  if (rc != SALTS_OK)
    return intake_config_error(error, rc, "$.graph", "source adapter name exceeds bound");
  rc = intake_name_copy(decoder_adapter_name, current.decoder_adapter_name,
                        sizeof(current.decoder_adapter_name));
  if (rc != SALTS_OK)
    return intake_config_error(error, rc, "$.graph", "intake adapter name exceeds bound");

  rc = intake_validate_topology(flow, source_adapter_name, decoder_adapter_name, error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_resolved_config_adapter(resolved, source_adapter_name, &source);
  if (rc != SALTS_OK)
    return intake_config_error(error, SALTS_EINVAL, "$.adapters",
                               "configured network Source adapter is missing");
  rc = turbo_flow_resolved_config_adapter(resolved, decoder_adapter_name, &intake);
  if (rc != SALTS_OK || !intake.kind || strcmp(intake.kind, "protocol.decode") != 0)
    return intake_config_error(error, SALTS_EINVAL, "$.adapters",
                               "configured intake adapter must have kind protocol.decode");
  {
    uint32_t schema_version = 0u;
    rc = intake_adapter_schema_version(&intake, decoder_adapter_name, &schema_version, error);
    if (rc != SALTS_OK) return rc;
    rc = intake_validate_exact_fields(&intake, decoder_adapter_name, schema_version, error);
    if (rc != SALTS_OK) return rc;
  }
  rc = intake_read_source(&source, source_adapter_name, &current, &transport_capacity, error);
  if (rc != SALTS_OK) return rc;
  rc = intake_read_protocol(&intake, decoder_adapter_name, &current, error);
  if (rc != SALTS_OK) return rc;
  rc = intake_validate_pairing(&current, decoder_adapter_name, error);
  if (rc != SALTS_OK) return rc;
  rc = intake_validate_bounds(&current, transport_capacity, decoder_adapter_name, error);
  if (rc != SALTS_OK) return rc;
  *settings = current;
  return SALTS_OK;
}
