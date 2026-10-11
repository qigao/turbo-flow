#include "turbo_flow_protocol_network_intake.h"

#include "flow_protocol_network_intake_internal.h"
#include "flow_provider_instance_internal.h"
#include "turbo_flow_plugin_protocol.h"
#include "turbo_flow_plugin_protocol_mapper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_protocol_network_intake_s {
  turbo_flow_protocol_network_intake_state_t state;
  int status;
  turbo_flow_t *flow;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_protocol_registry_t *protocol_registry;
  turbo_flow_protocol_owner_t *protocol_owner;
  turbo_flow_protocol_t *protocol;
  salts_component_plugin_scope component_scope;
  flow_compiled_provider_instance_t *source_instance;
  turbo_flow_runtime_owner *source_owner;
  int source_quiesced;
  int source_drained;
  int source_shutdown;
  flow_protocol_network_intake_sink_t *sink;
  flow_protocol_network_intake_settings_t settings;
  turbo_flow_protocol_mapper_v1_t mapper;
  turbo_flow_protocol_mapper_contract_t mapper_contract;
  int mapper_bound;
  uint64_t source_polls;
  char source_endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
};

static int intake_owner_error(turbo_flow_config_error_t *error, int status, const char *path,
                              const char *message) {
  if (error && error->size >= sizeof(*error) && error->status == SALTS_OK) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s",
                   path ? path : "$.protocol_network_intake");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "ProtocolNetworkIntake error");
  }
  return status;
}

static int intake_owner_public_validate(const turbo_flow_protocol_network_intake_config_t *config,
                                        turbo_flow_t **flow_io,
                                        turbo_flow_protocol_network_intake_t **out,
                                        turbo_flow_config_error_t *error) {
  if (out) *out = NULL;
  if (!config || config->size != sizeof(*config) || !config->component_runtime ||
      !config->provider_resolver ||
      config->version != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION || !config->catalog ||
      !config->resolved || !config->downstream_flow ||
      turbo_flow_state(config->downstream_flow) != TURBO_FLOW_STATE_STARTED ||
      !config->source_stage_name || !config->source_stage_name[0] ||
      !config->decoder_adapter_name || !config->decoder_adapter_name[0] ||
      !config->decoded_source_name || !config->decoded_source_name[0] || !flow_io || !*flow_io ||
      !out || !error || error->size < sizeof(*error))
    return intake_owner_error(error, SALTS_EINVAL, "$.protocol_network_intake",
                              "invalid ProtocolNetworkIntake create arguments");
  if (turbo_flow_state(*flow_io) != TURBO_FLOW_STATE_PARSED)
    return intake_owner_error(error, SALTS_EINVAL, "$.graph",
                              "ProtocolNetworkIntake requires one parsed intake Flow");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

static int intake_owner_reply_policy(const turbo_flow_protocol_network_intake_config_t *config,
                                     flow_protocol_network_intake_settings_t *settings,
                                     turbo_flow_config_error_t *error) {
  const turbo_flow_protocol_network_reply_policy_t *policy = NULL;
  if (!config || !settings) return SALTS_EINVAL;
  settings->reply_point = TURBO_FLOW_PROTOCOL_NETWORK_REPLY_NONE;
  settings->reply_max_encoded_bytes = 0u;
  if (config->size >= sizeof(*config)) policy = config->reply_policy;
  if (!policy) return SALTS_OK;
  if (policy->size < sizeof(*policy) ||
      policy->version != TURBO_FLOW_PROTOCOL_NETWORK_REPLY_POLICY_API_VERSION)
    return intake_owner_error(error, SALTS_EINVAL, "$.protocol_network_intake.reply",
                              "invalid protocol reply policy ABI");
  if (policy->point == TURBO_FLOW_PROTOCOL_NETWORK_REPLY_NONE) {
    if (policy->max_encoded_bytes != 0u)
      return intake_owner_error(error, SALTS_EINVAL, "$.protocol_network_intake.reply",
                                "disabled reply policy must not reserve encoded bytes");
    return SALTS_OK;
  }
  if (policy->point != TURBO_FLOW_PROTOCOL_NETWORK_REPLY_DURABLE_ADMISSION)
    return intake_owner_error(error, SALTS_ENOTSUP, "$.protocol_network_intake.reply",
                              "unsupported protocol reply settlement point");
  if (policy->max_encoded_bytes == 0u || policy->max_encoded_bytes > settings->max_frame_size)
    return intake_owner_error(error, SALTS_ERANGE, "$.protocol_network_intake.reply",
                              "reply bound must fit the configured protocol frame bound");
  settings->reply_point = policy->point;
  settings->reply_max_encoded_bytes = policy->max_encoded_bytes;
  return SALTS_OK;
}

static int intake_owner_mapper_bind(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                    const flow_protocol_network_intake_settings_t *settings,
                                    turbo_flow_protocol_mapper_v1_t *mapper_out,
                                    turbo_flow_protocol_mapper_contract_t *contract_out,
                                    turbo_flow_config_error_t *error) {
  turbo_flow_plugin_protocol_mapper_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_CATALOG_V1_INIT;
  turbo_flow_protocol_mapper_preflight_request_t request =
      TURBO_FLOW_PROTOCOL_MAPPER_PREFLIGHT_REQUEST_INIT;
  const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *selected = NULL;
  turbo_flow_protocol_mapper_contract_t contract = TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
  size_t matches = 0u;
  int rc;
  if (!snapshot || !settings || !mapper_out || !contract_out)
    return intake_owner_error(error, SALTS_EINVAL, "$.protocol_mapper",
                              "invalid protocol mapper binding arguments");
  memset(mapper_out, 0, sizeof(*mapper_out));
  *contract_out = (turbo_flow_protocol_mapper_contract_t)TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
  if (settings->schema_version != 3u) return SALTS_OK;

  rc = turbo_flow_plugin_catalog_snapshot_protocol_mapper_catalog(snapshot, &catalog);
  if (rc != SALTS_OK)
    return intake_owner_error(error, rc, "$.protocol_mapper",
                              "failed to project protocol mapper catalog");
  for (size_t i = 0u; i < catalog.count; ++i) {
    const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *entry = &catalog.entries[i];
    if (!entry->plugin_id || strcmp(entry->plugin_id, settings->mapper_plugin) != 0 ||
        entry->mapper.protocol != settings->protocol_kind || !entry->mapper.name ||
        strcmp(entry->mapper.name, settings->mapper_name) != 0 || !entry->mapper.profile ||
        strcmp(entry->mapper.profile, settings->mapper_profile) != 0)
      continue;
    selected = entry;
    ++matches;
  }
  if (matches != 1u || !selected)
    return intake_owner_error(error, matches ? SALTS_EALREADY : SALTS_ENOENT, "$.protocol_mapper",
                              matches ? "configured protocol mapper is ambiguous"
                                      : "configured protocol mapper is missing");
  if (settings->mapper_max_semantic_bytes > selected->mapper.max_semantic_bytes ||
      settings->mapper_max_output_bytes > selected->mapper.max_output_bytes)
    return intake_owner_error(error, SALTS_ENOSPC, "$.protocol_mapper",
                              "configured mapper bounds exceed provider limits");

  request.protocol = settings->protocol_kind;
  request.profile = settings->mapper_profile;
  request.message_type = settings->mapper_message_type;
  request.semantic_type = settings->mapper_semantic_type;
  request.semantic_media_type = settings->mapper_semantic_media_type;
  request.max_semantic_bytes = settings->mapper_max_semantic_bytes;
  request.max_output_bytes = settings->mapper_max_output_bytes;
  rc = selected->mapper.preflight(selected->mapper.ctx, &request, &contract);
  if (rc != SALTS_OK)
    return intake_owner_error(error, rc, "$.protocol_mapper.preflight",
                              "protocol mapper preflight failed");
  if (contract.size != sizeof(contract) ||
      contract.abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      contract.protocol != request.protocol || contract.message_type != request.message_type ||
      contract.semantic_type != request.semantic_type ||
      memchr(contract.profile, '\0', sizeof(contract.profile)) == NULL ||
      strcmp(contract.profile, request.profile) != 0 || !contract.max_semantic_bytes ||
      contract.max_semantic_bytes > request.max_semantic_bytes || !contract.max_output_bytes ||
      contract.max_output_bytes > request.max_output_bytes ||
      turbo_flow_content_descriptor_check(&contract.content) != SALTS_OK ||
      contract.content.domain != TURBO_FLOW_DOMAIN_DATA ||
      (contract.content.flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u)
    return intake_owner_error(error, SALTS_EPROTO, "$.protocol_mapper.preflight",
                              "protocol mapper returned an invalid compiled contract");

  *mapper_out = selected->mapper;
  *contract_out = contract;
  return SALTS_OK;
}

static int intake_component_status(salts_component_plugin_status status) {
  if (status == SALTS_COMPONENT_PLUGIN_OK) return SALTS_OK;
  if (status == SALTS_COMPONENT_PLUGIN_BUSY) return SALTS_EBUSY;
  if (status == SALTS_COMPONENT_PLUGIN_CAPACITY_EXCEEDED) return SALTS_ENOSPC;
  if (status == SALTS_COMPONENT_PLUGIN_INVALID_ARGUMENT) return SALTS_EINVAL;
  return SALTS_EPROTO;
}

static int intake_owner_create_reject(turbo_flow_protocol_network_intake_t *intake,
                                      turbo_flow_protocol_network_intake_t **out,
                                      int original_status, turbo_flow_config_error_t *error) {
  int rc;
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED;
  intake->status = original_status;
  rc = turbo_flow_protocol_network_intake_stop(intake, 0u);
  if (rc == SALTS_OK) rc = turbo_flow_protocol_network_intake_destroy(intake);
  if (rc == SALTS_OK) return original_status;
  *out = intake;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return intake_owner_error(error, rc, "$.protocol_network_intake.cleanup",
                            "create rollback retained an owner; retry stop/destroy");
}

static int intake_owner_refresh_endpoint(turbo_flow_protocol_network_intake_t *intake) {
  turbo_flow_connection_snapshot_t snapshot;
  const char *terminator;
  int rc;
  if (!intake || !intake->flow) return SALTS_EINVAL;
  rc = turbo_flow_provider_adapter_source_connection_snapshot(
      intake->flow, intake->settings.source_stage_name, &snapshot);
  if (rc != SALTS_OK) return rc;
  terminator = (const char *)memchr(snapshot.endpoint, '\0', sizeof(snapshot.endpoint));
  if (!terminator) return SALTS_EPROTO;
  memcpy(intake->source_endpoint, snapshot.endpoint, (size_t)(terminator - snapshot.endpoint) + 1u);
  return SALTS_OK;
}

static int
intake_owner_compiled_topology_validate(const turbo_flow_protocol_network_intake_t *intake,
                                        turbo_flow_config_error_t *error) {
  size_t source_index = SIZE_MAX;
  size_t decoder_index = SIZE_MAX;
  const turbo_flow_edge_plan_t *edge;

  if (!intake || !intake->flow || turbo_flow_state(intake->flow) != TURBO_FLOW_STATE_COMPILED)
    return intake_owner_error(error, SALTS_EPROTO, "$.graph",
                              "compiled intake Flow is unavailable");

  for (size_t i = 0u; i < turbo_flow_stage_count(intake->flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(intake->flow, i);
    if (!stage || !stage->adapter_name) continue;
    if (strcmp(stage->name, intake->settings.source_stage_name) == 0) source_index = i;
    if (strcmp(stage->adapter_name, intake->settings.decoder_adapter_name) == 0) decoder_index = i;
  }
  edge = turbo_flow_edge_at(intake->flow, 0u);
  if (source_index == SIZE_MAX || decoder_index == SIZE_MAX || !edge ||
      edge->from_stage != source_index || edge->to_stage != decoder_index ||
      edge->kind != TURBO_FLOW_EDGE_UNCONDITIONAL)
    return intake_owner_error(
        error, SALTS_EINVAL, "$.graph",
        "compiled intake Flow must be one unconditional Source-to-decoder edge");
  return SALTS_OK;
}

static int intake_owner_snapshot_copy(const turbo_flow_protocol_network_intake_t *intake,
                                      turbo_flow_protocol_network_intake_snapshot_t *snapshot) {
  flow_protocol_network_intake_sink_metrics_t metrics;
  turbo_flow_protocol_network_intake_snapshot_t current =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  if (!intake || !snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION)
    return SALTS_EINVAL;
  memset(&metrics, 0, sizeof(metrics));
  flow_protocol_network_intake_sink_metrics(intake->sink, &metrics);
  current.state = intake->state;
  current.status = intake->status;
  current.active_sessions = metrics.active_sessions;
  current.pending_claims = metrics.pending_claims;
  current.pending_bytes = metrics.pending_bytes;
  current.frames_admitted = metrics.frames_admitted;
  current.source_polls = intake->source_polls;
  current.backpressured = metrics.backpressured;
  memcpy(current.source_endpoint, intake->source_endpoint, sizeof(current.source_endpoint));
  *snapshot = current;
  return SALTS_OK;
}

static int intake_owner_fail(turbo_flow_protocol_network_intake_t *intake, int status,
                             turbo_flow_protocol_network_intake_snapshot_t *snapshot) {
  if (intake) {
    intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED;
    intake->status = status;
    if (snapshot) (void)intake_owner_snapshot_copy(intake, snapshot);
  }
  return status;
}

int turbo_flow_protocol_network_intake_create(
    const turbo_flow_protocol_network_intake_config_t *config, turbo_flow_t **intake_flow_io,
    turbo_flow_protocol_network_intake_t **out, turbo_flow_config_error_t *error) {
  turbo_flow_protocol_network_intake_t *intake;
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_source_transport_contract_t transport = TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT;
  flow_protocol_network_intake_sink_config_t sink_config;
  int rc;
  int source_index;

  rc = intake_owner_public_validate(config, intake_flow_io, out, error);
  if (rc != SALTS_OK) return rc;
  intake = (turbo_flow_protocol_network_intake_t *)calloc(1u, sizeof(*intake));
  if (!intake)
    return intake_owner_error(error, SALTS_ENOMEM, "$.protocol_network_intake",
                              "failed to allocate intake owner");
  intake->component_scope = (salts_component_plugin_scope)SALTS_COMPONENT_PLUGIN_SCOPE_INIT;
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED;
  intake->status = SALTS_OK;
  rc = flow_protocol_network_intake_preflight(
      config->resolved, *intake_flow_io, config->source_stage_name, config->decoder_adapter_name,
      &intake->settings, error);
  if (rc != SALTS_OK) goto fail;
  rc = intake_owner_reply_policy(config, &intake->settings, error);
  if (rc != SALTS_OK) goto fail;
  source_index = turbo_flow_find_stage(*intake_flow_io, config->source_stage_name);
  if (source_index < 0) {
    rc = SALTS_ENOENT;
    goto fail;
  }
  rc = intake_component_status(
      salts_component_plugin_scope_acquire(config->component_runtime, &intake->component_scope));
  if (rc != SALTS_OK) goto fail;
  rc = flow_compiled_provider_instance_prepare(
      *intake_flow_io, (size_t)source_index, &intake->component_scope, config->provider_resolver,
      config->resource_resolver, &intake->source_instance, error);
  if (rc != SALTS_OK) goto fail;

  rc = turbo_flow_plugin_catalog_snapshot_retain(config->catalog);
  if (rc != SALTS_OK) goto fail;
  intake->catalog = config->catalog;
  rc = turbo_flow_protocol_registry_create(config->catalog, &intake->protocol_registry);
  if (rc != SALTS_OK) goto fail;
  request.protocol = intake->settings.protocol_kind;
  request.protocol_version = intake->settings.protocol_version;
  request.max_frame_size = intake->settings.max_frame_size;
  rc = turbo_flow_protocol_owner_create_registered(intake->protocol_registry,
                                                   intake->settings.protocol_provider, &request,
                                                   &intake->protocol_owner);
  if (rc != SALTS_OK) goto fail;
  rc = turbo_flow_protocol_owner_instance(intake->protocol_owner, intake->settings.protocol_kind,
                                          &intake->protocol);
  if (rc != SALTS_OK || !intake->protocol) {
    if (rc == SALTS_OK) rc = SALTS_EPROTO;
    goto fail;
  }
  if (intake->settings.reply_point != TURBO_FLOW_PROTOCOL_NETWORK_REPLY_NONE) {
    turbo_flow_protocol_info_t info = TURBO_FLOW_PROTOCOL_INFO_INIT;
    rc = turbo_flow_protocol_get_info(intake->protocol, &info);
    if (rc != SALTS_OK) goto fail;
    if ((info.capabilities & TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY) == 0u) {
      rc = SALTS_ENOTSUP;
      goto fail;
    }
  }
  rc = intake_owner_mapper_bind(intake->catalog, &intake->settings, &intake->mapper,
                                &intake->mapper_contract, error);
  if (rc != SALTS_OK) goto fail;
  intake->mapper_bound = intake->settings.schema_version == 3u;

  /* First side effect can register callbacks. From here Graph teardown is ours. */
  intake->flow = *intake_flow_io;
  *intake_flow_io = NULL;
  rc = flow_compiled_provider_instance_materialize(intake->source_instance, intake->flow, error);
  /* A rejected owner contract must keep its compiled instance/scope retained. */
  (void)flow_compiled_provider_instance_owner(intake->source_instance, &intake->source_owner);
  if (rc != SALTS_OK) goto fail;
  if (!intake->source_owner || (turbo_flow_runtime_owner_capabilities(intake->source_owner) &
                                TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL) == 0u) {
    rc = SALTS_EPROTO;
    goto fail;
  }
  rc = turbo_flow_provider_adapter_source_transport_query(
      intake->flow, intake->settings.source_stage_name, &transport);
  if (rc != SALTS_OK) goto fail;
  rc = flow_protocol_network_intake_transport_bind(&transport, &intake->settings, error);
  if (rc != SALTS_OK) goto fail;

  memset(&sink_config, 0, sizeof(sink_config));
  sink_config.flow = intake->flow;
  sink_config.adapter_name = intake->settings.decoder_adapter_name;
  sink_config.protocol = intake->protocol;
  sink_config.downstream_flow = config->downstream_flow;
  sink_config.decoded_source_name = config->decoded_source_name;
  sink_config.settings = &intake->settings;
  sink_config.mapper = intake->mapper_bound ? &intake->mapper : NULL;
  sink_config.mapper_contract = intake->mapper_bound ? &intake->mapper_contract : NULL;
  rc = flow_protocol_network_intake_sink_create(&sink_config, &intake->sink);
  if (rc != SALTS_OK) goto fail;
  rc = flow_protocol_network_intake_sink_register(intake->sink);
  if (rc != SALTS_OK) goto fail;
  rc = turbo_flow_compile(intake->flow);
  if (rc != SALTS_OK) goto fail;
  /* Exact-stage reply bindings are frozen by compile, before Source start. */
  if (intake->settings.reply_point != TURBO_FLOW_PROTOCOL_NETWORK_REPLY_NONE) {
    rc = turbo_flow_transport_reply_supported_stage(intake->flow,
                                                    intake->settings.source_stage_name);
    if (rc != SALTS_OK) goto fail;
  }
  rc = intake_owner_compiled_topology_validate(intake, error);
  if (rc != SALTS_OK) goto fail;
  *out = intake;
  return SALTS_OK;

fail:
  intake_owner_error(error, rc, "$.protocol_network_intake", "canonical intake creation failed");
  return intake_owner_create_reject(intake, out, rc, error);
}

int turbo_flow_protocol_network_intake_start(turbo_flow_protocol_network_intake_t *intake) {
  int rc;
  if (!intake) return SALTS_EINVAL;
  if (intake->state != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED) return SALTS_EBUSY;
  rc = turbo_flow_start(intake->flow);
  if (rc != SALTS_OK) return intake_owner_fail(intake, rc, NULL);
  rc = intake_owner_refresh_endpoint(intake);
  if (rc != SALTS_OK) return intake_owner_fail(intake, rc, NULL);
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING;
  intake->status = SALTS_OK;
  return SALTS_OK;
}

int turbo_flow_protocol_network_intake_poll(
    turbo_flow_protocol_network_intake_t *intake, uint32_t timeout_ms,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot) {
  flow_protocol_network_intake_sink_metrics_t metrics;
  int rc;
  if (!intake || !snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION)
    return SALTS_EINVAL;
  if (intake->state != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING &&
      intake->state != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_BACKPRESSURED) {
    (void)intake_owner_snapshot_copy(intake, snapshot);
    return SALTS_EBUSY;
  }
  rc = flow_protocol_network_intake_sink_retry(intake->sink);
  if (rc != SALTS_OK) return intake_owner_fail(intake, rc, snapshot);
  memset(&metrics, 0, sizeof(metrics));
  flow_protocol_network_intake_sink_metrics(intake->sink, &metrics);
  if (metrics.terminal_status != SALTS_OK)
    return intake_owner_fail(intake, metrics.terminal_status, snapshot);
  if (metrics.backpressured || metrics.pending_claims != 0u) {
    intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_BACKPRESSURED;
    intake->status = SALTS_OK;
    return intake_owner_snapshot_copy(intake, snapshot);
  }
  if (intake->source_polls == UINT64_MAX) return intake_owner_fail(intake, SALTS_ERANGE, snapshot);
  ++intake->source_polls;
  rc = turbo_flow_runtime_owner_poll(intake->source_owner, timeout_ms);
  if (rc != SALTS_OK) return intake_owner_fail(intake, rc, snapshot);
  rc = flow_protocol_network_intake_sink_retry(intake->sink);
  if (rc != SALTS_OK) return intake_owner_fail(intake, rc, snapshot);
  rc = intake_owner_refresh_endpoint(intake);
  if (rc != SALTS_OK) return intake_owner_fail(intake, rc, snapshot);
  memset(&metrics, 0, sizeof(metrics));
  flow_protocol_network_intake_sink_metrics(intake->sink, &metrics);
  if (metrics.terminal_status != SALTS_OK)
    return intake_owner_fail(intake, metrics.terminal_status, snapshot);
  intake->state = (metrics.backpressured || metrics.pending_claims != 0u)
                      ? TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_BACKPRESSURED
                      : TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING;
  intake->status = SALTS_OK;
  return intake_owner_snapshot_copy(intake, snapshot);
}

int turbo_flow_protocol_network_intake_snapshot(
    const turbo_flow_protocol_network_intake_t *intake,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot) {
  return intake_owner_snapshot_copy(intake, snapshot);
}

int turbo_flow_protocol_network_intake_stop(turbo_flow_protocol_network_intake_t *intake,
                                            uint64_t timeout_ms) {
  int rc;
  turbo_flow_state_t flow_state;
  if (!intake) return SALTS_EINVAL;
  if (intake->state == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED) return SALTS_EALREADY;
  if (intake->state == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPING) return SALTS_EBUSY;
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPING;
  if (intake->source_owner && !intake->source_quiesced) {
    rc = turbo_flow_runtime_owner_quiesce(intake->source_owner, timeout_ms);
    if (rc != SALTS_OK) return intake_owner_fail(intake, rc, NULL);
    intake->source_quiesced = 1;
  }
  if (intake->sink) flow_protocol_network_intake_sink_cancel(intake->sink, SALTS_ECANCELED);
  flow_state = intake->flow ? turbo_flow_state(intake->flow) : TURBO_FLOW_STATE_STOPPED;
  if (intake->flow &&
      (flow_state == TURBO_FLOW_STATE_STARTED || flow_state == TURBO_FLOW_STATE_FAILED)) {
    rc = turbo_flow_stop(intake->flow);
    if (rc != SALTS_OK) return intake_owner_fail(intake, rc, NULL);
  }
  if (intake->sink) {
    flow_protocol_network_intake_sink_metrics_t metrics;
    rc = flow_protocol_network_intake_sink_retry(intake->sink);
    if (rc != SALTS_OK && rc != SALTS_ECANCELED) return intake_owner_fail(intake, rc, NULL);
    memset(&metrics, 0, sizeof(metrics));
    flow_protocol_network_intake_sink_metrics(intake->sink, &metrics);
    if (metrics.pending_replies != 0u) return intake_owner_fail(intake, SALTS_EBUSY, NULL);
  }
  if (intake->source_owner && !intake->source_drained) {
    rc = turbo_flow_runtime_owner_drain(intake->source_owner, timeout_ms);
    if (rc != SALTS_OK) return intake_owner_fail(intake, rc, NULL);
    intake->source_drained = 1;
  }
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED;
  intake->status = SALTS_OK;
  return SALTS_OK;
}

int turbo_flow_protocol_network_intake_destroy(turbo_flow_protocol_network_intake_t *intake) {
  int rc;
  if (!intake) return SALTS_OK;
  if (intake->state != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED) return SALTS_EBUSY;
  if (intake->source_owner && !intake->source_shutdown) {
    rc = turbo_flow_runtime_owner_shutdown(intake->source_owner);
    if (rc != SALTS_OK) return rc;
    intake->source_shutdown = 1;
  }
  if (intake->flow) {
    turbo_flow_destroy(intake->flow);
    intake->flow = NULL;
  }
  if (intake->sink) {
    flow_protocol_network_intake_sink_destroy(intake->sink);
    intake->sink = NULL;
  }
  if (intake->source_owner) {
    rc = flow_compiled_provider_instance_owner_destroy(intake->source_instance);
    if (rc != SALTS_OK) return rc;
    intake->source_owner = NULL;
  }
  if (intake->source_instance) {
    rc = flow_compiled_provider_instance_release(&intake->source_instance);
    if (rc != SALTS_OK) return rc;
  }
  if (intake->component_scope.live) {
    rc = intake_component_status(salts_component_plugin_scope_release(&intake->component_scope));
    if (rc != SALTS_OK) return rc;
  }
  if (intake->protocol_owner) {
    turbo_flow_protocol_owner_destroy(intake->protocol_owner);
    intake->protocol_owner = NULL;
    intake->protocol = NULL;
  }
  if (intake->protocol_registry) {
    rc = turbo_flow_protocol_registry_destroy(intake->protocol_registry);
    if (rc != SALTS_OK) return rc;
    intake->protocol_registry = NULL;
  }
  if (intake->catalog) turbo_flow_plugin_catalog_snapshot_destroy(intake->catalog);
  free(intake);
  return SALTS_OK;
}
