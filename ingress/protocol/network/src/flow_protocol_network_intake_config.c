#include "flow_protocol_network_intake_internal.h"
#include "flow_provider_config_internal.h"
#include "network_decoder_config_native.h"

#include <fmt.h>
#include <limits.h>
#include <string.h>

static int intake_config_error(turbo_flow_config_error_t *error, int status, const char *path,
                               const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)fmt(error->path, sizeof(error->path), "{}", path ? path : "$.protocol_intake");
    (void)fmt(error->message, sizeof(error->message), "{}", message ? message : "error");
  }
  return status;
}

static int intake_config_field_error(turbo_flow_config_error_t *error, int status,
                                     const char *adapter_name, const char *field,
                                     const char *message) {
  char path[TURBO_FLOW_DIAGNOSTIC_PATH_MAX + 1u];
  (void)fmt(path, sizeof(path), "$.stages.{}{}{}", adapter_name ? adapter_name : "?",
            field ? "." : "", field ? field : "");
  return intake_config_error(error, status, path, message);
}

static int intake_name_copy(const char *value, char *out, size_t capacity) {
  const size_t length = value ? strlen(value) : 0u;
  if (!value || length == 0u) return SALTS_EINVAL;
  if (length >= capacity) return SALTS_ERANGE;
  memcpy(out, value, length + 1u);
  return SALTS_OK;
}

static int intake_typed_text(const tstr value, char *out, size_t capacity) {
  const size_t length = value ? tstr_len(value) : 0u;
  if (!value || length == 0u || memchr(value, '\0', length)) return SALTS_EINVAL;
  if (length >= capacity) return SALTS_ERANGE;
  memcpy(out, value, length + 1u);
  return SALTS_OK;
}

static int intake_typed_size(uint64_t value, size_t *out) {
  if (value == 0u || value > (uint64_t)SIZE_MAX) return SALTS_ERANGE;
  *out = (size_t)value;
  return SALTS_OK;
}

static int intake_read_protocol(const NetworkDecoderConfig_t *typed, const char *stage_name,
                                flow_protocol_network_intake_settings_t *settings,
                                turbo_flow_config_error_t *error) {
  char kind[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  int rc = intake_typed_text(typed->protocol_provider, settings->protocol_provider,
                             sizeof(settings->protocol_provider));
  if (rc == SALTS_OK) rc = intake_typed_text(typed->protocol_kind, kind, sizeof(kind));
  if (rc == SALTS_OK)
    rc = intake_typed_text(typed->protocol_version, settings->protocol_version,
                           sizeof(settings->protocol_version));
  if (rc == SALTS_OK)
    rc = intake_typed_text(typed->source_id, settings->source_id, sizeof(settings->source_id));
  if (rc != SALTS_OK)
    return intake_config_field_error(error, rc, stage_name, NULL,
                                     "decoder identity must fit its native text boundary");
  if (strcmp(kind, "jtt808") == 0) settings->protocol_kind = TURBO_FLOW_PROTOCOL_JTT_808;
  else if (strcmp(kind, "coap") == 0) settings->protocol_kind = TURBO_FLOW_PROTOCOL_COAP;
  else
    return intake_config_field_error(error, SALTS_ENOTSUP, stage_name, "protocol_kind",
                                     "ProtocolNetworkIntake supports jtt808 or coap only");
  if (strcmp(kind, settings->protocol_provider) != 0)
    return intake_config_field_error(error, SALTS_EINVAL, stage_name, "protocol_provider",
                                     "decoder kind requires its exact protocol provider");
  rc = intake_typed_size(typed->max_sessions, &settings->max_sessions);
  if (rc == SALTS_OK) rc = intake_typed_size(typed->max_frame_size, &settings->max_frame_size);
  if (rc == SALTS_OK)
    rc = intake_typed_size(typed->max_pending_claims, &settings->max_pending_claims);
  if (rc == SALTS_OK)
    rc = intake_typed_size(typed->max_pending_bytes, &settings->max_pending_bytes);
  if (rc != SALTS_OK)
    return intake_config_field_error(error, rc, stage_name, NULL,
                                     "decoder bounds exceed native size capacity");

  settings->mapper_enabled = (typed->_presence[NetworkDecoderConfig_OPTIONAL_mapper / 8u] &
                              (1u << (NetworkDecoderConfig_OPTIONAL_mapper % 8u))) != 0u;
  if (!settings->mapper_enabled) return SALTS_OK;
  /* Presence and required nested fields are admitted by the generated MessagePlan. */
  rc = intake_typed_text(typed->mapper.plugin, settings->mapper_plugin,
                         sizeof(settings->mapper_plugin));
  if (rc == SALTS_OK)
    rc =
        intake_typed_text(typed->mapper.name, settings->mapper_name, sizeof(settings->mapper_name));
  if (rc == SALTS_OK)
    rc = intake_typed_text(typed->mapper.profile, settings->mapper_profile,
                           sizeof(settings->mapper_profile));
  if (rc == SALTS_OK)
    rc = intake_typed_text(typed->mapper.semantic_media_type, settings->mapper_semantic_media_type,
                           sizeof(settings->mapper_semantic_media_type));
  if (rc == SALTS_OK)
    rc = intake_typed_size(typed->mapper.max_semantic_bytes, &settings->mapper_max_semantic_bytes);
  if (rc == SALTS_OK)
    rc = intake_typed_size(typed->mapper.max_output_bytes, &settings->mapper_max_output_bytes);
  if (rc != SALTS_OK)
    return intake_config_field_error(error, rc, stage_name, "mapper",
                                     "mapper values exceed native identity or size bounds");
  settings->mapper_message_type = typed->mapper.message_type;
  settings->mapper_semantic_type = typed->mapper.semantic_type;
  if (settings->mapper_max_semantic_bytes > settings->max_frame_size)
    return intake_config_field_error(error, SALTS_ERANGE, stage_name, "mapper.max_semantic_bytes",
                                     "mapper semantic bound must not exceed frame bound");
  return SALTS_OK;
}

static int intake_validate_pairing(const flow_protocol_network_intake_settings_t *settings,
                                   const char *decoder_adapter_name,
                                   turbo_flow_config_error_t *error) {
  if (settings->protocol_kind == TURBO_FLOW_PROTOCOL_JTT_808) {
    if (settings->transport_kind != FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP ||
        strcmp(settings->protocol_provider, "jtt808") != 0)
      return intake_config_field_error(
          error, SALTS_EINVAL, decoder_adapter_name, "protocol_provider",
          "jtt808 requires the jtt808 provider and TCP listener source");
    if (strcmp(settings->protocol_version, "2019-A1") != 0)
      return intake_config_field_error(error, SALTS_ENOTSUP, decoder_adapter_name,
                                       "protocol_version",
                                       "jtt808 network intake requires version 2019-A1");
    return SALTS_OK;
  }

  if (settings->protocol_kind == TURBO_FLOW_PROTOCOL_COAP) {
    if (settings->transport_kind != FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP ||
        strcmp(settings->protocol_provider, "coap") != 0)
      return intake_config_field_error(error, SALTS_EINVAL, decoder_adapter_name,
                                       "protocol_provider",
                                       "coap requires the coap provider and UDP packet source");
    if (strcmp(settings->protocol_version, "RFC7252") != 0)
      return intake_config_field_error(error, SALTS_ENOTSUP, decoder_adapter_name,
                                       "protocol_version",
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
  required_claims = settings->source_max_messages_per_poll;
  if (settings->max_pending_claims < required_claims)
    return intake_config_field_error(
        error, SALTS_ERANGE, decoder_adapter_name, "max_pending_claims",
        "pending claim capacity is below the bounded source progress budget");
  if (settings->source_max_message_bytes > SIZE_MAX / settings->max_pending_claims)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name, "max_pending_bytes",
                                     "pending byte budget overflows");
  required_bytes = settings->source_max_message_bytes * settings->max_pending_claims;
  if (settings->max_pending_bytes < required_bytes)
    return intake_config_field_error(error, SALTS_ERANGE, decoder_adapter_name, "max_pending_bytes",
                                     "pending byte capacity is below the bounded claim budget");
  return SALTS_OK;
}

static int intake_validate_topology(const turbo_flow_t *flow, const char *source_stage_name,
                                    const char *decoder_stage_name,
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
    if (strcmp(stage->name, source_stage_name) == 0) {
      if (!stage->is_source || source_index != SIZE_MAX)
        return intake_config_error(error, SALTS_EINVAL, "$.graph",
                                   "configured intake Source must bind exactly one source stage");
      source_index = i;
    }
    if (strcmp(stage->name, decoder_stage_name) == 0) {
      if (stage->is_source || sink_index != SIZE_MAX ||
          strcmp(stage->adapter_name, "protocol.decode") != 0 ||
          (stage->resource_name && stage->resource_name[0]))
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

int flow_protocol_network_intake_preflight(const turbo_flow_t *flow, const char *source_stage_name,
                                           const char *decoder_stage_name,
                                           flow_protocol_network_intake_settings_t *settings,
                                           turbo_flow_config_error_t *error) {
  flow_protocol_network_intake_settings_t current = {0};
  flow_provider_typed_config_t typed = {0};
  DataBindMessagePlanDiagnostic diagnostic = DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  const flow_stage_plan_impl_t *stage;
  int rc, cleanup;

  if (settings) memset(settings, 0, sizeof(*settings));
  if (!flow || !source_stage_name || !source_stage_name[0] || !decoder_stage_name ||
      !decoder_stage_name[0] || !settings || !error || error->size < sizeof(*error))
    return intake_config_error(error, SALTS_EINVAL, "$.protocol_intake",
                               "invalid network intake preflight arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = intake_validate_topology(flow, source_stage_name, decoder_stage_name, error);
  if (rc != SALTS_OK) return rc;
  rc = intake_name_copy(source_stage_name, current.source_stage_name,
                        sizeof(current.source_stage_name));
  if (rc == SALTS_OK)
    rc = intake_name_copy(decoder_stage_name, current.decoder_stage_name,
                          sizeof(current.decoder_stage_name));
  if (rc != SALTS_OK)
    return intake_config_error(error, rc, "$.graph", "intake stage name exceeds bound");
  stage = vec_at_const(&flow->stages, (size_t)turbo_flow_find_stage(flow, decoder_stage_name));
  if (!stage) return intake_config_error(error, SALTS_EINVAL, "$.graph", "decoder stage missing");
  rc = intake_name_copy(stage->adapter_name, current.decoder_adapter_name,
                        sizeof(current.decoder_adapter_name));
  if (rc != SALTS_OK) return rc;
  rc = flow_provider_typed_config_bind(stage, TurboFlowNetworkDecoderConfig_codec_create,
                                       NetworkDecoderConfig_native_artifact(), &typed, &diagnostic);
  if (rc != SALTS_OK)
    return intake_config_field_error(
        error, rc, decoder_stage_name, diagnostic.schema_field[0] ? diagnostic.schema_field : NULL,
        diagnostic.message[0] ? diagnostic.message : "decoder typed config binding failed");
  if (typed.value_bytes != sizeof(NetworkDecoderConfig_t)) rc = SALTS_EPROTO;
  else rc = intake_read_protocol(typed.value, decoder_stage_name, &current, error);
  /* Only bounded native settings survive preflight; no config tree or borrowed text escapes. */
  cleanup = flow_provider_typed_config_destroy(&typed);
  if (rc != SALTS_OK) return rc;
  if (cleanup != SALTS_OK)
    return intake_config_field_error(error, cleanup, decoder_stage_name, NULL,
                                     "decoder typed config cleanup failed");
  if ((current.protocol_kind == TURBO_FLOW_PROTOCOL_JTT_808 &&
       strcmp(current.protocol_version, "2019-A1") != 0) ||
      (current.protocol_kind == TURBO_FLOW_PROTOCOL_COAP &&
       strcmp(current.protocol_version, "RFC7252") != 0))
    return intake_config_field_error(error, SALTS_ENOTSUP, decoder_stage_name, "protocol_version",
                                     "unsupported network protocol version");
  *settings = current;
  return SALTS_OK;
}

int flow_protocol_network_intake_transport_bind(
    const turbo_flow_source_transport_contract_t *transport,
    flow_protocol_network_intake_settings_t *settings, turbo_flow_config_error_t *error) {
  flow_protocol_network_intake_settings_t current;
  int rc;
  if (!transport || transport->size != sizeof(*transport) ||
      transport->version != TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_VERSION || !settings || !error ||
      error->size < sizeof(*error) || transport->session_capacity == 0u ||
      transport->max_message_bytes == 0u || transport->max_messages_per_poll == 0u)
    return intake_config_error(error, SALTS_EINVAL, "$.source.transport",
                               "invalid Source transport admission contract");
  current = *settings;
  if (transport->transport == TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER)
    current.transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP;
  else if (transport->transport == TURBO_FLOW_SOURCE_TRANSPORT_UDP_PACKET)
    current.transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP;
  else
    return intake_config_error(error, SALTS_ENOTSUP, "$.source.transport",
                               "network intake requires a TCP listener or UDP Source");
  current.source_max_message_bytes = transport->max_message_bytes;
  current.source_max_messages_per_poll = transport->max_messages_per_poll;
  rc = intake_validate_pairing(&current, current.decoder_stage_name, error);
  if (rc == SALTS_OK)
    rc = intake_validate_bounds(&current, transport->session_capacity, current.decoder_stage_name,
                                error);
  if (rc == SALTS_OK) *settings = current;
  return rc;
}
