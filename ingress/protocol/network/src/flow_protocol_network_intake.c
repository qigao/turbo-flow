#include "turbo_flow_protocol_network_intake.h"

#include "flow_protocol_network_intake_internal.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_protocol_network_intake_s {
  turbo_flow_protocol_network_intake_state_t state;
  int status;
  turbo_flow_t *flow;
  turbo_flow_inbox_t *inbox;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_protocol_registry_t *protocol_registry;
  turbo_flow_protocol_owner_t *protocol_owner;
  turbo_flow_protocol_t *protocol;
  turbo_flow_plugin_product_owner_v1_t source_owner;
  flow_protocol_network_intake_sink_t *sink;
  flow_protocol_network_intake_settings_t settings;
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

static int intake_owner_inbox_valid(const turbo_flow_inbox_t *inbox) {
  const turbo_flow_inbox_ops_v2_t *ops;
  if (!inbox || inbox->size != sizeof(*inbox) || inbox->version != TURBO_FLOW_INBOX_API_VERSION ||
      !inbox->ops || !inbox->ctx)
    return 0;
  ops = inbox->ops;
  return ops->size == sizeof(*ops) && ops->version == TURBO_FLOW_INBOX_API_VERSION && ops->admit &&
         ops->claim && ops->complete && ops->fail && ops->retry && ops->discard && ops->forget &&
         ops->scan_failed && ops->scan_history && ops->close && ops->snapshot && ops->destroy;
}

static int intake_owner_public_validate(
    const turbo_flow_protocol_network_intake_config_t *config, turbo_flow_t **flow_io,
    turbo_flow_protocol_network_intake_t **out, turbo_flow_config_error_t *error) {
  if (out) *out = NULL;
  if (!config || config->size != sizeof(*config) ||
      config->version != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION || !config->catalog ||
      !config->resolved || !intake_owner_inbox_valid(config->inbox) || !config->source_adapter_name ||
      !config->source_adapter_name[0] || !config->intake_adapter_name ||
      !config->intake_adapter_name[0] || !flow_io || !*flow_io || !out || !error ||
      error->size < sizeof(*error))
    return intake_owner_error(error, SALTS_EINVAL, "$.protocol_network_intake",
                              "invalid ProtocolNetworkIntake create arguments");
  if (turbo_flow_state(*flow_io) != TURBO_FLOW_STATE_PARSED)
    return intake_owner_error(error, SALTS_EINVAL, "$.graph",
                              "ProtocolNetworkIntake requires one parsed intake Flow");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

static int intake_owner_product_catalog(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
    turbo_flow_config_error_t *error) {
  int rc;
  *catalog = (turbo_flow_plugin_transactional_product_catalog_v1_t)
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  rc = turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, catalog);
  if (rc != SALTS_OK)
    return intake_owner_error(error, rc, "$.providers",
                              "failed to project transactional provider catalog");
  if (catalog->size != sizeof(*catalog) ||
      catalog->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      catalog->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      (catalog->adapter_provider_count != 0u && !catalog->adapter_providers))
    return intake_owner_error(error, SALTS_EPROTO, "$.providers",
                              "transactional provider catalog is malformed");
  return SALTS_OK;
}

static int intake_owner_source_provider(
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
    const turbo_flow_resolved_config_t *resolved, const char *source_name,
    const turbo_flow_plugin_transactional_adapter_provider_v1_t **provider_out,
    turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t source = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  const turbo_flow_plugin_transactional_adapter_provider_v1_t *found = NULL;
  size_t matches = 0u;
  int rc;
  if (provider_out) *provider_out = NULL;
  if (!catalog || !resolved || !source_name || !provider_out) return SALTS_EINVAL;
  rc = turbo_flow_resolved_config_adapter(resolved, source_name, &source);
  if (rc != SALTS_OK || !source.kind || !source.kind[0])
    return intake_owner_error(error, rc != SALTS_OK ? rc : SALTS_EINVAL, "$.adapters",
                              "configured Source adapter is missing or malformed");
  for (size_t i = 0u; i < catalog->adapter_provider_count; ++i) {
    const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider =
        &catalog->adapter_providers[i];
    if (provider->size != sizeof(*provider) ||
        provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
        provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !provider->kind ||
        !provider->kind[0] || !provider->preflight || !provider->materialize)
      return intake_owner_error(error, SALTS_EPROTO, "$.providers.adapters",
                                "transactional Source provider is malformed");
    if (strcmp(provider->kind, source.kind) != 0) continue;
    found = provider;
    ++matches;
  }
  if (matches == 0u)
    return intake_owner_error(error, SALTS_ENOTSUP, "$.providers.adapters",
                              "configured Source kind has no transactional provider");
  if (matches != 1u)
    return intake_owner_error(error, SALTS_EALREADY, "$.providers.adapters",
                              "configured Source kind has duplicate transactional providers");
  *provider_out = found;
  return SALTS_OK;
}

static int intake_owner_source_owner_abi_valid(const turbo_flow_plugin_product_owner_v1_t *owner) {
  return owner && owner->size == sizeof(*owner) &&
         owner->abi_major == TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR &&
         owner->abi_minor == TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
}

static int intake_owner_source_owner_valid(const turbo_flow_plugin_product_owner_v1_t *owner) {
  const turbo_flow_plugin_product_owner_flags_t known =
      TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD | TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE |
      TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  return intake_owner_source_owner_abi_valid(owner) && owner->ctx &&
         (owner->flags & ~known) == 0u &&
         (((owner->flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD) != 0u) !=
          ((owner->flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE) != 0u)) &&
         (owner->flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL) != 0u && owner->quiesce &&
         owner->drain && owner->shutdown && owner->destroy && owner->poll;
}

static void intake_owner_destroy_unstarted_product(
    turbo_flow_plugin_product_owner_v1_t *owner) {
  if (!owner || !owner->ctx || !owner->destroy) return;
  /* The Graph has already removed every registered callback. This mirrors generation-create
     rollback: an unstarted Product owner is retired directly by its validated destroy callback. */
  owner->destroy(owner->ctx);
  *owner = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
}

static void intake_owner_pretransfer_cleanup(turbo_flow_protocol_network_intake_t *intake) {
  if (!intake) return;
  flow_protocol_network_intake_sink_destroy(intake->sink);
  if (intake->protocol_owner) turbo_flow_protocol_owner_destroy(intake->protocol_owner);
  if (intake->protocol_registry) (void)turbo_flow_protocol_registry_destroy(intake->protocol_registry);
  if (intake->catalog) turbo_flow_plugin_catalog_snapshot_destroy(intake->catalog);
  free(intake);
}

static void intake_owner_failed_create_cleanup(turbo_flow_protocol_network_intake_t *intake) {
  if (!intake) return;
  if (intake->flow) {
    turbo_flow_destroy(intake->flow);
    intake->flow = NULL;
  }
  intake_owner_destroy_unstarted_product(&intake->source_owner);
  flow_protocol_network_intake_sink_destroy(intake->sink);
  if (intake->protocol_owner) turbo_flow_protocol_owner_destroy(intake->protocol_owner);
  if (intake->protocol_registry) (void)turbo_flow_protocol_registry_destroy(intake->protocol_registry);
  if (intake->catalog) turbo_flow_plugin_catalog_snapshot_destroy(intake->catalog);
  free(intake);
}

static void intake_owner_pin_unretirable(turbo_flow_protocol_network_intake_t *intake) {
  if (!intake) return;
  if (intake->flow) turbo_flow_destroy(intake->flow);
  flow_protocol_network_intake_sink_destroy(intake->sink);
  if (intake->protocol_owner) turbo_flow_protocol_owner_destroy(intake->protocol_owner);
  if (intake->protocol_registry) (void)turbo_flow_protocol_registry_destroy(intake->protocol_registry);
  /* Keep the explicit catalog retain alive forever. The current ABI has no force-unload or
     owner-repair operation for an opaque Product owner whose layout cannot be trusted. */
  intake->catalog = NULL;
  free(intake);
}

static int intake_owner_refresh_endpoint(turbo_flow_protocol_network_intake_t *intake) {
  turbo_flow_connection_snapshot_t snapshot;
  if (!intake || !intake->flow) return SALTS_EINVAL;
  for (size_t i = 0u; i < turbo_flow_adapter_count(intake->flow); ++i) {
    const char *terminator;
    int rc;
    memset(&snapshot, 0, sizeof(snapshot));
    rc = turbo_flow_adapter_connection_snapshot_at(intake->flow, i, &snapshot);
    if (rc == SALTS_ENOTSUP || rc == SALTS_ENOENT) continue;
    if (rc != SALTS_OK) return rc;
    if (!snapshot.adapter_name ||
        strcmp(snapshot.adapter_name, intake->settings.source_adapter_name) != 0)
      continue;
    terminator = (const char *)memchr(snapshot.endpoint, '\0', sizeof(snapshot.endpoint));
    if (!terminator) return SALTS_EPROTO;
    memcpy(intake->source_endpoint, snapshot.endpoint,
           (size_t)(terminator - snapshot.endpoint) + 1u);
    return SALTS_OK;
  }
  return SALTS_ENOENT;
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
  turbo_flow_plugin_transactional_product_catalog_v1_t catalog;
  const turbo_flow_plugin_transactional_adapter_provider_v1_t *source_provider = NULL;
  turbo_flow_protocol_network_intake_t *intake = NULL;
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_plugin_product_owner_v1_t source_owner = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  flow_protocol_network_intake_sink_config_t sink_config;
  int rc;

  rc = intake_owner_public_validate(config, intake_flow_io, out, error);
  if (rc != SALTS_OK) return rc;
  intake = (turbo_flow_protocol_network_intake_t *)calloc(1u, sizeof(*intake));
  if (!intake)
    return intake_owner_error(error, SALTS_ENOMEM, "$.protocol_network_intake",
                              "failed to allocate ProtocolNetworkIntake owner");
  intake->source_owner =
      (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED;
  intake->status = SALTS_OK;
  intake->inbox = config->inbox;

  rc = flow_protocol_network_intake_preflight(config->resolved, *intake_flow_io,
                                              config->source_adapter_name,
                                              config->intake_adapter_name, &intake->settings, error);
  if (rc != SALTS_OK) {
    free(intake);
    return rc;
  }
  rc = intake_owner_product_catalog(config->catalog, &catalog, error);
  if (rc != SALTS_OK) {
    free(intake);
    return rc;
  }
  rc = intake_owner_source_provider(&catalog, config->resolved, config->source_adapter_name,
                                    &source_provider, error);
  if (rc != SALTS_OK) {
    free(intake);
    return rc;
  }
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = source_provider->preflight(source_provider->ctx, config->resolved,
                                  config->source_adapter_name, error);
  if (rc != SALTS_OK) {
    free(intake);
    return intake_owner_error(error, rc, "$.adapters",
                              "configured Source provider preflight failed");
  }

  rc = turbo_flow_plugin_catalog_snapshot_retain(config->catalog);
  if (rc != SALTS_OK) {
    free(intake);
    return intake_owner_error(error, rc, "$.catalog", "failed to retain plugin catalog");
  }
  intake->catalog = config->catalog;
  rc = turbo_flow_protocol_registry_create(config->catalog, &intake->protocol_registry);
  if (rc != SALTS_OK) {
    intake_owner_pretransfer_cleanup(intake);
    return intake_owner_error(error, rc, "$.protocol",
                              "failed to create protocol registry from catalog");
  }
  request.protocol = intake->settings.protocol_kind;
  request.protocol_version = intake->settings.protocol_version;
  request.max_frame_size = intake->settings.max_frame_size;
  rc = turbo_flow_protocol_owner_create_registered(intake->protocol_registry,
                                                   intake->settings.protocol_provider, &request,
                                                   &intake->protocol_owner);
  if (rc != SALTS_OK) {
    intake_owner_pretransfer_cleanup(intake);
    return intake_owner_error(error, rc, "$.protocol",
                              "configured protocol provider/version could not be opened");
  }
  rc = turbo_flow_protocol_owner_instance(intake->protocol_owner, intake->settings.protocol_kind,
                                          &intake->protocol);
  if (rc != SALTS_OK || !intake->protocol) {
    intake_owner_pretransfer_cleanup(intake);
    return intake_owner_error(error, rc != SALTS_OK ? rc : SALTS_EPROTO, "$.protocol",
                              "configured protocol owner returned no matching instance");
  }

  memset(&sink_config, 0, sizeof(sink_config));
  sink_config.flow = *intake_flow_io;
  sink_config.adapter_name = intake->settings.intake_adapter_name;
  sink_config.protocol = intake->protocol;
  sink_config.inbox = intake->inbox;
  sink_config.settings = &intake->settings;
  rc = flow_protocol_network_intake_sink_create(&sink_config, &intake->sink);
  if (rc != SALTS_OK) {
    intake_owner_pretransfer_cleanup(intake);
    return intake_owner_error(error, rc, "$.protocol_network_intake.sink",
                              "failed to create bounded protocol intake Sink");
  }

  intake->flow = *intake_flow_io;
  *intake_flow_io = NULL;
  rc = flow_protocol_network_intake_sink_register(intake->sink);
  if (rc != SALTS_OK) {
    intake_owner_failed_create_cleanup(intake);
    return intake_owner_error(error, rc, "$.protocol_network_intake.sink",
                              "failed to register protocol intake Sink");
  }

  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = source_provider->materialize(source_provider->ctx, intake->flow, config->resolved,
                                    config->source_adapter_name, &source_owner, error);
  if (rc != SALTS_OK) {
    intake_owner_failed_create_cleanup(intake);
    return intake_owner_error(error, rc, "$.adapters",
                              "configured Source provider materialization failed");
  }
  intake->source_owner = source_owner;
  if (!intake_owner_source_owner_abi_valid(&intake->source_owner)) {
    intake_owner_pin_unretirable(intake);
    return intake_owner_error(error, SALTS_EINVAL, "$.adapters.owner",
                              "provider returned an incompatible Product owner ABI");
  }
  if (!intake_owner_source_owner_valid(&intake->source_owner)) {
    if (!intake->source_owner.ctx || !intake->source_owner.destroy) {
      intake_owner_pin_unretirable(intake);
      return intake_owner_error(error, SALTS_EPROTO, "$.adapters.owner",
                                "provider returned an unretirable Product owner vtable");
    }
    intake_owner_failed_create_cleanup(intake);
    return intake_owner_error(error, SALTS_EPROTO, "$.adapters.owner",
                              "provider returned an invalid Product owner vtable");
  }

  rc = turbo_flow_compile(intake->flow);
  if (rc != SALTS_OK) {
    intake_owner_failed_create_cleanup(intake);
    return intake_owner_error(error, rc, "$.graph", "failed to compile intake Flow");
  }
  *out = intake;
  return SALTS_OK;
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
  rc = intake->source_owner.poll(intake->source_owner.ctx, timeout_ms);
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
  int first_status = SALTS_OK;
  turbo_flow_state_t flow_state;
  if (!intake) return SALTS_EINVAL;
  if (intake->state == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED) return SALTS_EALREADY;
  if (intake->state == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPING) return SALTS_EBUSY;
  if (intake->state == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED) {
    rc = intake->source_owner.quiesce(intake->source_owner.ctx, timeout_ms);
    if (rc != SALTS_OK) return intake_owner_fail(intake, rc, NULL);
    intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED;
    intake->status = SALTS_OK;
    return SALTS_OK;
  }

  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPING;
  rc = intake->source_owner.quiesce(intake->source_owner.ctx, timeout_ms);
  if (rc != SALTS_OK) first_status = rc;
  flow_protocol_network_intake_sink_cancel(intake->sink, SALTS_ECANCELED);
  flow_state = intake->flow ? turbo_flow_state(intake->flow) : TURBO_FLOW_STATE_STOPPED;
  if (intake->flow &&
      (flow_state == TURBO_FLOW_STATE_STARTED || flow_state == TURBO_FLOW_STATE_FAILED)) {
    rc = turbo_flow_stop(intake->flow);
    if (rc != SALTS_OK && first_status == SALTS_OK) first_status = rc;
  }
  if (first_status == SALTS_OK) {
    rc = intake->source_owner.drain(intake->source_owner.ctx, timeout_ms);
    if (rc != SALTS_OK) first_status = rc;
  }
  if (first_status != SALTS_OK) return intake_owner_fail(intake, first_status, NULL);
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED;
  intake->status = SALTS_OK;
  return SALTS_OK;
}

int turbo_flow_protocol_network_intake_destroy(turbo_flow_protocol_network_intake_t *intake) {
  int rc;
  if (!intake) return SALTS_OK;
  if (intake->state != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED) return SALTS_EBUSY;
  if (intake->flow) {
    turbo_flow_destroy(intake->flow);
    intake->flow = NULL;
  }
  if (intake->source_owner.ctx) {
    rc = intake->source_owner.shutdown(intake->source_owner.ctx);
    if (rc != SALTS_OK) return rc;
    intake->source_owner.destroy(intake->source_owner.ctx);
    intake->source_owner =
        (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  }
  if (intake->sink) {
    flow_protocol_network_intake_sink_destroy(intake->sink);
    intake->sink = NULL;
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
  if (intake->catalog) {
    turbo_flow_plugin_catalog_snapshot_destroy(intake->catalog);
    intake->catalog = NULL;
  }
  free(intake);
  return SALTS_OK;
}
