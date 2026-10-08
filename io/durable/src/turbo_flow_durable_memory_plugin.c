#include "turbo_flow_durable_memory_internal.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct memory_provider_state_s {
  bool started;
  bool stopping;
  size_t owners;
} memory_provider_state_t;

typedef struct memory_owner_s {
  memory_provider_state_t *provider;
  turbo_flow_inbox_t inbox;
  turbo_flow_durable_buffer_binding_t *binding;
} memory_owner_t;

static memory_provider_state_t provider_state;

static int provider_fail(
    turbo_flow_config_error_t *error, int status, const char *name,
    const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(
        error->path, sizeof(error->path), "$.stages.%s",
        name ? name : "");
    (void)snprintf(
        error->message, sizeof(error->message), "%s",
        message ? message : "bounded memory provider failure");
  }
  return status;
}

static int owner_idle(memory_owner_t *owner) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  int rc;
  if (!owner) return SALTS_EINVAL;
  if (!owner->inbox.ctx) return SALTS_OK;
  rc = turbo_flow_inbox_snapshot(&owner->inbox, &snapshot);
  if (rc != SALTS_OK) return rc;
  if (snapshot.failed_records) {
    turbo_flow_inbox_failed_entry_t entry =
        TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    size_t count = 0u;
    rc = turbo_flow_inbox_scan_failed(
        &owner->inbox, 0u, &entry, 1u, &count);
    if (rc != SALTS_OK) return rc;
    if (count != 1u || entry.status == SALTS_OK) return SALTS_EPROTO;
    return entry.status;
  }
  return snapshot.records || snapshot.in_flight_claims
             ? SALTS_EBUSY
             : SALTS_OK;
}

static int owner_quiesce(void *self, uint64_t timeout_ms) {
  memory_owner_t *owner = (memory_owner_t *)self;
  int rc = owner_idle(owner);
  (void)timeout_ms;
  if (rc != SALTS_OK || !owner || !owner->inbox.ctx) return rc;
  return turbo_flow_inbox_close(&owner->inbox);
}

static int owner_drain(void *self, uint64_t timeout_ms) {
  (void)timeout_ms;
  return owner_idle((memory_owner_t *)self);
}

static int owner_shutdown(void *self) {
  memory_owner_t *owner = (memory_owner_t *)self;
  int rc = owner_idle(owner);
  if (rc != SALTS_OK) return rc;
  if (owner->binding) {
    rc = turbo_flow_durable_buffer_unbind(owner->binding);
    if (rc != SALTS_OK) return rc;
    owner->binding = NULL;
  }
  return owner->inbox.ctx
             ? turbo_flow_inbox_destroy(&owner->inbox)
             : SALTS_OK;
}

static int owner_poll(void *self, uint32_t timeout_ms) {
  memory_owner_t *owner = (memory_owner_t *)self;
  (void)timeout_ms;
  return owner && owner->binding
             ? turbo_flow_durable_buffer_progress(owner->binding)
             : SALTS_ESHUTDOWN;
}

static void owner_destroy(void *self) {
  memory_owner_t *owner = (memory_owner_t *)self;
  memory_provider_state_t *provider;
  if (!owner || owner->binding || owner->inbox.ctx) return;
  provider = owner->provider;
  if (provider && provider->owners != 0u) --provider->owners;
  free(owner);
}

CMETA_IMPLEMENTS(
    turbo_flow_runtime_owner, memory_runtime_owner,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
        TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL,
    .quiesce = owner_quiesce,
    .drain = owner_drain,
    .shutdown = owner_shutdown,
    .poll = owner_poll,
    .destroy = owner_destroy);

static int memory_resource_snapshot(
    void *self, turbo_flow_durable_memory_resource_view_t *view_out) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  if (!state || !state->started || state->stopping ||
      !view_out || view_out->size != sizeof(*view_out))
    return SALTS_EINVAL;

  *view_out =
      (turbo_flow_durable_memory_resource_view_t)
          TURBO_FLOW_DURABLE_MEMORY_RESOURCE_VIEW_INIT;
  view_out->max_records = TURBO_FLOW_INBOX_MEMORY_MAX_RECORDS;
  view_out->max_total_bytes = TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES;
  view_out->max_record_bytes = TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES;
  view_out->max_claims = TURBO_FLOW_INBOX_MEMORY_MAX_RECORDS;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_durable_memory_resource, memory_resource_impl,
    TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS,
    .snapshot = memory_resource_snapshot);

static int instance_resource(
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_durable_memory_resource_view_t *view,
    turbo_flow_config_error_t *error) {
  turbo_flow_durable_memory_resource *resource;
  int rc;

  if (!instance || !view || !instance->resource ||
      instance->resource->size != sizeof(*instance->resource) ||
      !instance->resource->reference_name ||
      !instance->resource->reference_name[0] ||
      !instance->resource->identity ||
      !instance->resource->identity[0] ||
      !instance->resource->interface_desc ||
      !instance->resource->interface_value ||
      !cmeta_interface_desc_equal(
          instance->resource->interface_desc,
          turbo_flow_durable_memory_resource_interface()))
    return provider_fail(
        error, SALTS_EPROTO,
        instance ? instance->instance_name : NULL,
        "memory provider requires an exact deployment resource Interface");

  resource = (turbo_flow_durable_memory_resource *)
      instance->resource->interface_value;
  if (!turbo_flow_durable_memory_resource_valid(resource))
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "memory deployment resource handle is invalid");

  *view = (turbo_flow_durable_memory_resource_view_t)
      TURBO_FLOW_DURABLE_MEMORY_RESOURCE_VIEW_INIT;
  rc = turbo_flow_durable_memory_resource_snapshot(resource, view);
  if (rc != SALTS_OK)
    return provider_fail(
        error, rc, instance->instance_name,
        "memory deployment resource snapshot failed");
  if (!turbo_flow_durable_memory_resource_view_valid(view))
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "memory deployment resource limits are invalid");
  return SALTS_OK;
}

static int instance_config(
    const turbo_flow_provider_instance_v1_t *instance,
    const turbo_flow_durable_memory_resource_view_t *resource,
    durable_memory_config_t *config,
    turbo_flow_config_error_t *error) {
  const DataBindMessageNativeArtifact *artifact;
  DataBindNativeTypeBinding native = {0};
  DataBindError databind_error = DATA_BIND_ERROR_INIT;

  if (!instance || !resource || !config ||
      instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0] ||
      instance->config.size != sizeof(instance->config) ||
      !instance->config.type_name || !instance->config.data ||
      !instance->config.value ||
      instance->config.value_bytes != sizeof(DurableMemoryConfig_t))
    return provider_fail(
        error, SALTS_EINVAL,
        instance ? instance->instance_name : NULL,
        "invalid memory provider instance");

  artifact = DurableMemoryConfig_native_artifact();
  if (!artifact ||
      strcmp(instance->config.type_name, artifact->type_name) != 0 ||
      artifact->native_binding(&native, &databind_error) != DATA_BIND_OK ||
      instance->config.data != native.data)
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "memory typed config does not match provider contract");

  return durable_memory_config_from_typed(
      (const DurableMemoryConfig_t *)instance->config.value,
      resource, instance->instance_name, config, error);
}

static int references(
    turbo_flow_t *flow, const char *resource_reference) {
  size_t count = 0u;
  if (!flow || !resource_reference || !resource_reference[0])
    return SALTS_EINVAL;
  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->resource_name ||
        strcmp(stage->resource_name, resource_reference) != 0)
      continue;
    if (!stage->is_buffer) return SALTS_EINVAL;
    ++count;
  }
  return count == 1u ? SALTS_OK : SALTS_EINVAL;
}

static int provider_contract(
    void *self, turbo_flow_provider_contract_v1_t *out) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  if (!state || !out || out->size != sizeof(*out)) return SALTS_EINVAL;

  *out = (turbo_flow_provider_contract_v1_t)
      TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory =
      TurboFlowDurableMemoryProviderConfig_codec_create;
  out->config.message_artifact =
      DurableMemoryConfig_native_artifact();
  out->resource.contract_id =
      TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_ID;
  out->resource.contract_version =
      TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_VERSION;
  out->resource.required_capabilities =
      TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS;
  out->resource.expected_interface =
      turbo_flow_durable_memory_resource_interface();
  return SALTS_OK;
}

static int provider_preflight(
    void *self, const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  turbo_flow_durable_memory_resource_view_t resource =
      TURBO_FLOW_DURABLE_MEMORY_RESOURCE_VIEW_INIT;
  durable_memory_config_t config;
  int rc;

  if (!state || !state->started || state->stopping)
    return SALTS_ESHUTDOWN;
  rc = instance_resource(instance, &resource, error);
  if (rc != SALTS_OK) return rc;
  return instance_config(instance, &resource, &config, error);
}

static int provider_materialize(
    void *self, turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  turbo_flow_durable_memory_resource_view_t resource =
      TURBO_FLOW_DURABLE_MEMORY_RESOURCE_VIEW_INIT;
  durable_memory_config_t config;
  turbo_flow_durable_buffer_binding_config_t binding =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  memory_owner_t *owner;
  int rc;

  if (!state || !state->started || state->stopping ||
      !flow || !owner_out ||
      turbo_flow_runtime_owner_valid(owner_out))
    return SALTS_EINVAL;

  rc = instance_resource(instance, &resource, error);
  if (rc != SALTS_OK) return rc;
  rc = instance_config(instance, &resource, &config, error);
  if (rc != SALTS_OK) return rc;
  rc = references(flow, instance->resource->reference_name);
  if (rc != SALTS_OK)
    return provider_fail(
        error, rc, instance->instance_name,
        "memory provider requires exactly one buffer reference");

  owner = (memory_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->provider = state;
  owner->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;

  rc = turbo_flow_inbox_memory_create(&config.memory, &owner->inbox);
  if (rc != SALTS_OK) {
    free(owner);
    return provider_fail(
        error, rc, instance->instance_name,
        "bounded memory Inbox creation failed");
  }

  ++state->owners;
  binding.resource_name = instance->resource->reference_name;
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
                     "bounded memory durable binding failed")
               : provider_fail(
                     error, cleanup, instance->instance_name,
                     "bounded memory owner rollback failed");
  }

  *owner_out =
      memory_runtime_owner_as_turbo_flow_runtime_owner(owner);
  if (error)
    *error = (turbo_flow_config_error_t)
        TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_provider_factory, memory_provider_factory_impl, 0u,
    .contract = provider_contract,
    .preflight = provider_preflight,
    .materialize = provider_materialize);

static turbo_flow_provider_factory provider_factory;
static turbo_flow_durable_memory_resource resource_handle;
static cmeta_plugin_export plugin_exports[2];
static salts_once_t provider_once = SALTS_ONCE_INIT;

static void provider_init(void) {
  memset(&provider_state, 0, sizeof(provider_state));
  provider_factory =
      memory_provider_factory_impl_as_turbo_flow_provider_factory(
          &provider_state);
  resource_handle =
      memory_resource_impl_as_turbo_flow_durable_memory_resource(
          &provider_state);

  plugin_exports[0] = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .contract_version =
          TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
      .capabilities = 0u,
      .export_id = FLOW_DURABLE_MEMORY_KIND,
      .contract_id = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_provider_factory_interface(),
          .value = &provider_factory,
      },
  };
  plugin_exports[1] = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .contract_version =
          TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_VERSION,
      .capabilities =
          TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS,
      .export_id = FLOW_DURABLE_MEMORY_RESOURCE_EXPORT_ID,
      .contract_id =
          TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_durable_memory_resource_interface(),
          .value = &resource_handle,
      },
  };
}

static cmeta_plugin_status CMETA_PLUGIN_CALL
provider_start(void *self) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  if (state->owners != 0u) return CMETA_PLUGIN_BUSY;
  state->stopping = false;
  state->started = true;
  return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL
provider_request_stop(void *self) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL
provider_is_quiescent(const void *self) {
  const memory_provider_state_t *state =
      (const memory_provider_state_t *)self;
  return state && state->stopping && state->owners == 0u;
}

static void CMETA_PLUGIN_CALL provider_destroy(void *self) {
  memory_provider_state_t *state = (memory_provider_state_t *)self;
  if (!state || state->owners != 0u) return;
  state->started = false;
  state->stopping = true;
}

static cmeta_plugin_manifest provider_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "turbo-flow.durable.memory",
    .version = {2u, 0u, 0u},
    .self = &provider_state,
};

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&provider_once, provider_init);
  provider_manifest.exports = plugin_exports;
  provider_manifest.export_count = 2u;
  provider_manifest.start = provider_start;
  provider_manifest.request_stop = provider_request_stop;
  provider_manifest.is_quiescent = provider_is_quiescent;
  provider_manifest.destroy = provider_destroy;
  return &provider_manifest;
}
