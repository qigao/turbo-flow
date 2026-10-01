#include "turbo_flow_turbodb_durable_internal.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct provider_state_s {
  bool started;
  bool stopping;
  size_t owners;
} provider_state_t;

typedef struct turbodb_owner_s {
  provider_state_t *provider;
  turbo_flow_inbox_t inbox;
  turbo_flow_durable_buffer_binding_t *binding;
} turbodb_owner_t;

static provider_state_t provider_state;

static int provider_fail(
    turbo_flow_config_error_t *error, int status, const char *name,
    const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path),
                   "$.stages.%s", name ? name : "");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "TurboDB provider failure");
  }
  return status;
}

static int owner_idle(turbodb_owner_t *owner) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  int rc;
  if (!owner) return SALTS_EINVAL;
  if (!owner->inbox.ctx) return SALTS_OK;
  rc = turbo_flow_inbox_snapshot(&owner->inbox, &snapshot);
  if (rc != SALTS_OK) return rc;
  if (snapshot.failed_records) {
    turbo_flow_inbox_failed_entry_t entry = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    size_t count = 0u;
    rc = turbo_flow_inbox_scan_failed(&owner->inbox, 0u, &entry, 1u, &count);
    if (rc != SALTS_OK) return rc;
    if (count != 1u || entry.status == SALTS_OK) return SALTS_EPROTO;
    return entry.status;
  }
  return snapshot.records || snapshot.in_flight_claims ? SALTS_EBUSY : SALTS_OK;
}

static int owner_quiesce(void *self, uint64_t timeout_ms) {
  turbodb_owner_t *owner = (turbodb_owner_t *)self;
  int rc = owner_idle(owner);
  (void)timeout_ms;
  if (rc != SALTS_OK || !owner || !owner->inbox.ctx) return rc;
  return turbo_flow_inbox_close(&owner->inbox);
}

static int owner_drain(void *self, uint64_t timeout_ms) {
  (void)timeout_ms;
  return owner_idle((turbodb_owner_t *)self);
}

static int owner_shutdown(void *self) {
  turbodb_owner_t *owner = (turbodb_owner_t *)self;
  int rc = owner_idle(owner);
  if (rc != SALTS_OK) return rc;
  if (owner->binding) {
    rc = turbo_flow_durable_buffer_unbind(owner->binding);
    if (rc != SALTS_OK) return rc;
    owner->binding = NULL;
  }
  return owner->inbox.ctx ? turbo_flow_inbox_destroy(&owner->inbox) : SALTS_OK;
}

static int owner_poll(void *self, uint32_t timeout_ms) {
  turbodb_owner_t *owner = (turbodb_owner_t *)self;
  (void)timeout_ms;
  return owner && owner->binding
             ? turbo_flow_durable_buffer_progress(owner->binding)
             : SALTS_ESHUTDOWN;
}

static void owner_destroy(void *self) {
  turbodb_owner_t *owner = (turbodb_owner_t *)self;
  provider_state_t *provider;
  if (!owner || owner->binding || owner->inbox.ctx) return;
  provider = owner->provider;
  if (provider && provider->owners != 0u) --provider->owners;
  free(owner);
}

CMETA_IMPLEMENTS(
    turbo_flow_runtime_owner, turbodb_runtime_owner,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
        TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL,
    .quiesce = owner_quiesce,
    .drain = owner_drain,
    .shutdown = owner_shutdown,
    .poll = owner_poll,
    .destroy = owner_destroy);

static int instance_config(
    const turbo_flow_provider_instance_v1_t *instance,
    durable_turbodb_config_t *config,
    turbo_flow_config_error_t *error) {
  const DataBindMessageNativeArtifact *artifact;
  DataBindNativeTypeBinding native = {0};
  DataBindError databind_error = DATA_BIND_ERROR_INIT;

  if (!instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0] ||
      instance->config.size != sizeof(instance->config) ||
      !instance->config.type_name || !instance->config.data ||
      !instance->config.value ||
      instance->config.value_bytes != sizeof(DurableTurboDbConfig_t))
    return provider_fail(
        error, SALTS_EINVAL,
        instance ? instance->instance_name : NULL,
        "invalid TurboDB provider instance");

  artifact = DurableTurboDbConfig_native_artifact();
  if (!artifact ||
      strcmp(instance->config.type_name, artifact->type_name) != 0 ||
      artifact->native_binding(&native, &databind_error) != DATA_BIND_OK ||
      instance->config.data != native.data)
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "TurboDB typed config does not match the provider contract");

  return durable_turbodb_config_from_typed(
      (const DurableTurboDbConfig_t *)instance->config.value,
      instance->instance_name, config, error);
}

static int database_view_admitted(
    const turbo_flow_turbodb_database_view_t *view) {
  orm_driver_info_t info = {0};
  orm_error_t orm_error;
  size_t i;
  int file_backed = 0;

  if (!turbo_flow_turbodb_database_view_valid(view) ||
      view->database->struct_size != sizeof(*view->database) ||
      view->database->abi_version != ORM_C_ABI_VERSION ||
      (view->database->option_count != 0u && !view->database->options) ||
      view->database->driver.len != sizeof("sqlite") - 1u ||
      !view->database->driver.data ||
      memcmp(view->database->driver.data, "sqlite",
             sizeof("sqlite") - 1u) != 0)
    return 0;

  for (i = 0u; i < view->database->option_count; ++i) {
    const orm_option_t *option = &view->database->options[i];
    if (option->keyword.len != sizeof("filename") - 1u ||
        !option->keyword.data ||
        memcmp(option->keyword.data, "filename",
               sizeof("filename") - 1u) != 0)
      continue;
    file_backed =
        option->value.data && option->value.len != 0u &&
        !(option->value.len == sizeof(":memory:") - 1u &&
          memcmp(option->value.data, ":memory:",
                 sizeof(":memory:") - 1u) == 0);
    break;
  }
  if (!file_backed) return 0;

  orm_error_init(&orm_error);
  return orm_runtime_driver_info(
             view->runtime, orm_view("sqlite"), &info, &orm_error) ==
             ORM_STATUS_OK &&
         info.canonical_id_size == sizeof("sqlite") - 1u &&
         memcmp(info.canonical_id, "sqlite",
                sizeof("sqlite") - 1u) == 0;
}

static int instance_database(
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_turbodb_database_view_t *view,
    turbo_flow_config_error_t *error) {
  turbo_flow_turbodb_database_resource *resource;
  int rc;

  if (!instance || !view || !instance->resource ||
      instance->resource->size != sizeof(*instance->resource) ||
      !instance->resource->identity || !instance->resource->identity[0] ||
      !instance->resource->interface_desc ||
      !instance->resource->interface_value ||
      !cmeta_interface_desc_equal(
          instance->resource->interface_desc,
          turbo_flow_turbodb_database_resource_interface()))
    return provider_fail(
        error, SALTS_EPROTO,
        instance ? instance->instance_name : NULL,
        "TurboDB provider requires an exact database resource Interface");

  resource = (turbo_flow_turbodb_database_resource *)
      instance->resource->interface_value;
  if (!turbo_flow_turbodb_database_resource_valid(resource))
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "TurboDB database resource handle is invalid");

  *view = (turbo_flow_turbodb_database_view_t)
      TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT;
  rc = turbo_flow_turbodb_database_resource_snapshot(resource, view);
  if (rc != SALTS_OK) return provider_fail(
      error, rc, instance->instance_name,
      "TurboDB database resource snapshot failed");

  if (!database_view_admitted(view))
    return provider_fail(
        error, SALTS_ENOTSUP, instance->instance_name,
        "TurboDB database resource is not an admitted file-backed SQLite runtime");

  return SALTS_OK;
}

static int references(turbo_flow_t *flow, const char *resource_identity) {
  size_t count = 0u;
  size_t i;
  if (!flow || !resource_identity || !resource_identity[0])
    return SALTS_EINVAL;
  for (i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->resource_name ||
        strcmp(stage->resource_name, resource_identity) != 0)
      continue;
    if (!stage->is_buffer) return SALTS_EINVAL;
    ++count;
  }
  return count == 1u ? SALTS_OK : SALTS_EINVAL;
}

static int provider_describe(
    void *self, turbo_flow_provider_contract_v1_t *out) {
  provider_state_t *state = (provider_state_t *)self;
  if (!state || !out || out->size != sizeof(*out)) return SALTS_EINVAL;

  *out = (turbo_flow_provider_contract_v1_t)
      TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory = TurboFlowTurboDbProviderConfig_codec_create;
  out->config.message_artifact = DurableTurboDbConfig_native_artifact();
  out->resource.contract_id =
      TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_ID;
  out->resource.contract_version =
      TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_VERSION;
  out->resource.required_capabilities =
      TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT;
  out->resource.expected_interface =
      turbo_flow_turbodb_database_resource_interface();
  return SALTS_OK;
}

static int provider_preflight(
    void *self, const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  provider_state_t *state = (provider_state_t *)self;
  durable_turbodb_config_t config;
  turbo_flow_turbodb_database_view_t database =
      TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT;
  int rc;

  if (!state || !state->started || state->stopping)
    return SALTS_ESHUTDOWN;
  rc = instance_config(instance, &config, error);
  if (rc != SALTS_OK) return rc;
  return instance_database(instance, &database, error);
}

static int provider_materialize(
    void *self, turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  provider_state_t *state = (provider_state_t *)self;
  durable_turbodb_config_t config;
  turbo_flow_turbodb_database_view_t database =
      TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT;
  turbo_flow_durable_buffer_binding_config_t binding =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  turbodb_owner_t *owner;
  orm_error_t orm_error;
  int rc;

  if (!state || !state->started || state->stopping || !flow ||
      !owner_out || turbo_flow_runtime_owner_valid(owner_out))
    return SALTS_EINVAL;

  rc = instance_config(instance, &config, error);
  if (rc != SALTS_OK) return rc;
  rc = instance_database(instance, &database, error);
  if (rc != SALTS_OK) return rc;
  rc = references(flow, instance->resource->identity);
  if (rc != SALTS_OK)
    return provider_fail(
        error, rc, instance->instance_name,
        "TurboDB durable provider requires exactly one buffer reference");

  config.inbox.database_runtime = database.runtime;
  config.inbox.database = database.database;
  config.inbox.namespace_name = database.namespace_name;

  owner = (turbodb_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->provider = state;
  owner->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;

  rc = turbo_flow_turbodb_inbox_create(
      &config.inbox, &owner->inbox, &orm_error);
  if (rc != SALTS_OK) {
    free(owner);
    return provider_fail(
        error, rc, instance->instance_name,
        "TurboDB durable inbox creation failed");
  }

  ++state->owners;
  binding.resource_name = instance->resource->identity;
  binding.inbox = &owner->inbox;
  binding.identity_mode = config.identity_mode;
  binding.max_message_bytes = config.max_message_bytes;
  rc = turbo_flow_durable_buffer_bind(flow, &binding, &owner->binding);
  if (rc != SALTS_OK) {
    int cleanup = owner_quiesce(owner, 0u);
    if (cleanup == SALTS_OK) cleanup = owner_shutdown(owner);
    if (cleanup == SALTS_OK) owner_destroy(owner);
    return cleanup == SALTS_OK
               ? provider_fail(
                     error, rc, instance->instance_name,
                     "TurboDB durable buffer binding failed")
               : provider_fail(
                     error, cleanup, instance->instance_name,
                     "TurboDB owner rollback failed");
  }

  *owner_out = turbodb_runtime_owner_as_turbo_flow_runtime_owner(owner);
  if (error)
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_provider_factory, turbodb_provider_factory, 0u,
    .contract = provider_describe,
    .preflight = provider_preflight,
    .materialize = provider_materialize);

static turbo_flow_provider_factory provider_factory;
static salts_plugin_export provider_export;
static salts_plugin_manifest provider_manifest = {
    .struct_size = SALTS_PLUGIN_MANIFEST_SIZE,
    .abi_version = SALTS_PLUGIN_ABI_VERSION,
    .plugin_id = "turbo-flow.durable.turbodb",
    .version = {2u, 0u, 0u},
    .self = &provider_state,
};
static salts_once_t provider_once = SALTS_ONCE_INIT;

static void provider_init(void) {
  provider_factory =
      turbodb_provider_factory_as_turbo_flow_provider_factory(&provider_state);
  provider_export = (salts_plugin_export){
      .struct_size = SALTS_PLUGIN_EXPORT_SIZE,
      .kind = SALTS_PLUGIN_EXPORT_INTERFACE,
      .contract_version = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
      .capabilities = 0u,
      .export_id = FLOW_DURABLE_TURBODB_KIND,
      .contract_id = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_provider_factory_interface(),
          .value = &provider_factory,
      },
  };
  provider_manifest.exports = &provider_export;
  provider_manifest.export_count = 1u;
}

static salts_plugin_status SALTS_PLUGIN_CALL provider_start(void *self) {
  provider_state_t *state = (provider_state_t *)self;
  if (!state) return SALTS_PLUGIN_INVALID_ARGUMENT;
  if (state->owners != 0u) return SALTS_PLUGIN_BUSY;
  state->stopping = false;
  state->started = true;
  return SALTS_PLUGIN_OK;
}

static salts_plugin_status SALTS_PLUGIN_CALL provider_request_stop(void *self) {
  provider_state_t *state = (provider_state_t *)self;
  if (!state) return SALTS_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return SALTS_PLUGIN_OK;
}

static bool SALTS_PLUGIN_CALL provider_is_quiescent(const void *self) {
  const provider_state_t *state = (const provider_state_t *)self;
  return state && state->stopping && state->owners == 0u;
}

static void SALTS_PLUGIN_CALL provider_destroy(void *self) {
  provider_state_t *state = (provider_state_t *)self;
  if (!state || state->owners != 0u) return;
  state->started = false;
  state->stopping = true;
}

SALTS_PLUGIN_QUERY_EXPORT
const salts_plugin_manifest *SALTS_PLUGIN_CALL
salts_plugin_query(uint32_t host_abi) {
  if (host_abi != SALTS_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&provider_once, provider_init);
  provider_manifest.start = provider_start;
  provider_manifest.request_stop = provider_request_stop;
  provider_manifest.is_quiescent = provider_is_quiescent;
  provider_manifest.destroy = provider_destroy;
  return &provider_manifest;
}
