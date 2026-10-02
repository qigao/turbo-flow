#include "turbo_flow_chttp_provider_adapter_internal.h"
#include "turbo_flow_chttp_typed_config_internal.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  CHTTP_PROVIDER_CLIENT = 1u,
  CHTTP_PROVIDER_SERVER = 2u,
  CHTTP_PROVIDER_WEBSOCKET = 4u,
  CHTTP_PROVIDER_COUNT = 3u
};

#define CHTTP_PROVIDER_CLIENT_ID "chttp.client"
#define CHTTP_PROVIDER_SERVER_ID "chttp.server"
#define CHTTP_PROVIDER_WEBSOCKET_ID "chttp.websocket_server"

typedef struct chttp_provider_root_s chttp_provider_root_t;

typedef struct chttp_provider_slot_s {
  chttp_provider_root_t *root;
  unsigned kind;
} chttp_provider_slot_t;

struct chttp_provider_root_s {
  bool started;
  bool stopping;
  size_t owners;
  chttp_provider_slot_t slots[CHTTP_PROVIDER_COUNT];
};

typedef struct chttp_runtime_owner_s {
  chttp_provider_root_t *root;
  unsigned kind;
  chttp_typed_runtime_config_t runtime;
  chttp_tls_profile tls;
  bool tls_live;
  union {
    turbo_flow_chttp_client_t *client;
    turbo_flow_chttp_server_t *server;
    turbo_flow_chttp_websocket_server_t *websocket;
    void *any;
  } handle;
} chttp_runtime_owner_t;

static chttp_provider_root_t provider_root;
static turbo_flow_provider_factory provider_factories[CHTTP_PROVIDER_COUNT];
static salts_plugin_export provider_exports[CHTTP_PROVIDER_COUNT];
static salts_once_t provider_once = SALTS_ONCE_INIT;

static const char *provider_identity(unsigned kind) {
  switch (kind) {
  case CHTTP_PROVIDER_CLIENT:
    return CHTTP_PROVIDER_CLIENT_ID;
  case CHTTP_PROVIDER_SERVER:
    return CHTTP_PROVIDER_SERVER_ID;
  case CHTTP_PROVIDER_WEBSOCKET:
    return CHTTP_PROVIDER_WEBSOCKET_ID;
  default:
    return NULL;
  }
}

static int provider_fail(
    turbo_flow_config_error_t *error, int status, const char *instance_name,
    const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(
        error->path, sizeof(error->path), "$.stages.%s",
        instance_name ? instance_name : "");
    (void)snprintf(
        error->message, sizeof(error->message), "%s",
        message ? message : "CHTTP provider failure");
  }
  return status;
}

static const DataBindMessageNativeArtifact *provider_artifact(unsigned kind) {
  switch (kind) {
  case CHTTP_PROVIDER_CLIENT:
    return CHttpClientConfig_native_artifact();
  case CHTTP_PROVIDER_SERVER:
    return CHttpServerConfig_native_artifact();
  case CHTTP_PROVIDER_WEBSOCKET:
    return CHttpWebSocketServerConfig_native_artifact();
  default:
    return NULL;
  }
}

static size_t provider_value_bytes(unsigned kind) {
  switch (kind) {
  case CHTTP_PROVIDER_CLIENT:
    return sizeof(CHttpClientConfig_t);
  case CHTTP_PROVIDER_SERVER:
    return sizeof(CHttpServerConfig_t);
  case CHTTP_PROVIDER_WEBSOCKET:
    return sizeof(CHttpWebSocketServerConfig_t);
  default:
    return 0u;
  }
}

static const char *provider_resource_contract(unsigned kind) {
  return kind == CHTTP_PROVIDER_CLIENT
             ? TURBO_FLOW_CHTTP_CLIENT_RESOURCE_CONTRACT_ID
             : TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID;
}

static int instance_deployment(
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_chttp_deployment_view_t *deployment,
    turbo_flow_config_error_t *error) {
  turbo_flow_chttp_deployment_resource *resource;
  int rc;

  if (!instance || !deployment || !instance->resource ||
      instance->resource->size != sizeof(*instance->resource) ||
      !instance->resource->reference_name ||
      !instance->resource->reference_name[0] ||
      !instance->resource->identity ||
      !instance->resource->identity[0] ||
      !instance->resource->interface_desc ||
      !instance->resource->interface_value ||
      !cmeta_interface_desc_equal(
          instance->resource->interface_desc,
          turbo_flow_chttp_deployment_resource_interface()))
    return provider_fail(
        error, SALTS_EPROTO,
        instance ? instance->instance_name : NULL,
        "CHTTP provider requires an exact deployment resource Interface");

  resource = (turbo_flow_chttp_deployment_resource *)
      instance->resource->interface_value;
  if (!turbo_flow_chttp_deployment_resource_valid(resource))
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "CHTTP deployment resource handle is invalid");

  *deployment =
      (turbo_flow_chttp_deployment_view_t)
          TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
  rc = turbo_flow_chttp_deployment_resource_snapshot(resource, deployment);
  if (rc != SALTS_OK)
    return provider_fail(
        error, rc, instance->instance_name,
        "CHTTP deployment resource snapshot failed");
  if (!turbo_flow_chttp_deployment_view_valid(deployment))
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "CHTTP deployment resource returned an invalid view");
  return SALTS_OK;
}

static int instance_runtime_config(
    const chttp_provider_slot_t *slot,
    const turbo_flow_provider_instance_v1_t *instance,
    chttp_typed_runtime_config_t *runtime,
    turbo_flow_config_error_t *error) {
  const DataBindMessageNativeArtifact *artifact;
  DataBindNativeTypeBinding native =
      DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
  DataBindError databind_error = DATA_BIND_ERROR_INIT;
  turbo_flow_chttp_deployment_view_t deployment =
      TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
  int rc;

  if (!slot || !slot->root || !instance ||
      instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0] ||
      instance->config.size != sizeof(instance->config) ||
      !instance->config.type_name || !instance->config.data ||
      !instance->config.value || !runtime)
    return provider_fail(
        error, SALTS_EINVAL,
        instance ? instance->instance_name : NULL,
        "invalid CHTTP provider instance");

  artifact = provider_artifact(slot->kind);
  if (!artifact ||
      instance->config.value_bytes != provider_value_bytes(slot->kind) ||
      strcmp(instance->config.type_name, artifact->type_name) != 0 ||
      artifact->native_binding(&native, &databind_error) != DATA_BIND_OK ||
      instance->config.data != native.data)
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "CHTTP typed config does not match the provider contract");

  rc = instance_deployment(instance, &deployment, error);
  if (rc != SALTS_OK) return rc;

  switch (slot->kind) {
  case CHTTP_PROVIDER_CLIENT:
    return chttp_typed_client_config(
        (const CHttpClientConfig_t *)instance->config.value,
        &deployment, instance->instance_name, runtime, error);
  case CHTTP_PROVIDER_SERVER:
    return chttp_typed_server_config(
        (const CHttpServerConfig_t *)instance->config.value,
        &deployment, instance->instance_name, runtime, error);
  case CHTTP_PROVIDER_WEBSOCKET:
    return chttp_typed_websocket_config(
        (const CHttpWebSocketServerConfig_t *)instance->config.value,
        &deployment, instance->instance_name, runtime, error);
  default:
    return provider_fail(
        error, SALTS_EINVAL, instance->instance_name,
        "unknown CHTTP provider kind");
  }
}

static int stage_pair(
    turbo_flow_t *flow, const chttp_provider_slot_t *slot,
    const turbo_flow_provider_instance_v1_t *instance,
    const char **stage_names, size_t *stage_count, const char **source_name,
    turbo_flow_config_error_t *error) {
  const char *identity;
  const char *resource_name;
  const turbo_flow_stage_plan_t *instance_stage;
  const char *terminal_name;
  int instance_index;
  size_t sources = 0u;
  size_t terminals = 0u;

  if (stage_count) *stage_count = 0u;
  if (source_name) *source_name = NULL;
  if (!flow || !slot || !instance || !stage_names || !stage_count ||
      !source_name || !instance->resource)
    return SALTS_EINVAL;

  identity = provider_identity(slot->kind);
  resource_name = instance->resource->reference_name;
  if (!identity || !resource_name || !resource_name[0])
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "CHTTP provider requires an explicit deployment resource");

  instance_index = turbo_flow_find_stage(flow, instance->instance_name);
  instance_stage =
      instance_index >= 0
          ? turbo_flow_stage_at(flow, (size_t)instance_index)
          : NULL;
  if (!instance_stage || !instance_stage->name ||
      instance_stage->is_source || !instance_stage->adapter_name ||
      strcmp(instance_stage->adapter_name, identity) != 0 ||
      !instance_stage->resource_name ||
      strcmp(instance_stage->resource_name, resource_name) != 0)
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "CHTTP materialization instance must be an exact terminal stage");
  terminal_name = instance_stage->name;

  if (slot->kind == CHTTP_PROVIDER_CLIENT) {
    stage_names[0] = terminal_name;
    *stage_count = 1u;
    return SALTS_OK;
  }

  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->adapter_name || !stage->resource_name ||
        strcmp(stage->adapter_name, identity) != 0 ||
        strcmp(stage->resource_name, resource_name) != 0)
      continue;
    if (stage->is_source) {
      ++sources;
      *source_name = stage->name;
    } else {
      ++terminals;
      if (strcmp(stage->name, terminal_name) != 0)
        return provider_fail(
            error, SALTS_EALREADY, instance->instance_name,
            "deployment resource already has another CHTTP terminal owner");
    }
  }

  if (sources != 1u || terminals != 1u || !*source_name)
    return provider_fail(
        error, SALTS_EPROTO, instance->instance_name,
        "CHTTP server resource requires exactly one source and one terminal stage");

  stage_names[0] = *source_name;
  stage_names[1] = terminal_name;
  *stage_count = 2u;
  return SALTS_OK;
}

static int owner_idle(chttp_runtime_owner_t *owner) {
  int rc;
  if (!owner || !owner->handle.any) return SALTS_EINVAL;

  switch (owner->kind) {
  case CHTTP_PROVIDER_CLIENT: {
    turbo_flow_chttp_client_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    rc = turbo_flow_chttp_client_snapshot(owner->handle.client, &snapshot);
    if (rc != SALTS_OK) return rc;
    return (snapshot.state == TURBO_FLOW_CHTTP_CLIENT_REGISTERED ||
            snapshot.state == TURBO_FLOW_CHTTP_CLIENT_STOPPED ||
            snapshot.state == TURBO_FLOW_CHTTP_CLIENT_DETACHED ||
            snapshot.state == TURBO_FLOW_CHTTP_CLIENT_QUIESCED) &&
                   snapshot.active_requests == 0u &&
                   snapshot.queued_requests == 0u
               ? SALTS_OK
               : SALTS_EBUSY;
  }
  case CHTTP_PROVIDER_SERVER: {
    turbo_flow_chttp_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    rc = turbo_flow_chttp_server_snapshot(owner->handle.server, &snapshot);
    if (rc != SALTS_OK) return rc;
    return (snapshot.state == TURBO_FLOW_CHTTP_SERVER_REGISTERED ||
            snapshot.state == TURBO_FLOW_CHTTP_SERVER_STOPPED ||
            snapshot.state == TURBO_FLOW_CHTTP_SERVER_DETACHED ||
            snapshot.state == TURBO_FLOW_CHTTP_SERVER_QUIESCED) &&
                   snapshot.active_requests == 0u
               ? SALTS_OK
               : SALTS_EBUSY;
  }
  case CHTTP_PROVIDER_WEBSOCKET: {
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    rc = turbo_flow_chttp_websocket_server_snapshot(
        owner->handle.websocket, &snapshot);
    if (rc != SALTS_OK) return rc;
    return (snapshot.state ==
                TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_REGISTERED ||
            snapshot.state ==
                TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED ||
            snapshot.state ==
                TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_DETACHED ||
            snapshot.state ==
                TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED) &&
                   snapshot.active_sessions == 0u &&
                   snapshot.in_flight_frames == 0u
               ? SALTS_OK
               : SALTS_EBUSY;
  }
  default:
    return SALTS_EINVAL;
  }
}

static int owner_quiesce(void *self, uint64_t timeout_ms) {
  chttp_runtime_owner_t *owner = (chttp_runtime_owner_t *)self;
  int rc;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  rc = owner_idle(owner);
  if (rc == SALTS_OK) return SALTS_OK;
  if (rc != SALTS_EBUSY) return rc;

  switch (owner->kind) {
  case CHTTP_PROVIDER_CLIENT:
    return turbo_flow_chttp_client_quiesce(owner->handle.client);
  case CHTTP_PROVIDER_SERVER:
    return turbo_flow_chttp_server_quiesce(owner->handle.server);
  case CHTTP_PROVIDER_WEBSOCKET:
    return turbo_flow_chttp_websocket_server_quiesce(
        owner->handle.websocket);
  default:
    return SALTS_EINVAL;
  }
}

static int owner_drain(void *self, uint64_t timeout_ms) {
  (void)timeout_ms;
  return owner_idle((chttp_runtime_owner_t *)self);
}

static int owner_shutdown(void *self) {
  return owner_idle((chttp_runtime_owner_t *)self);
}

static int owner_poll(void *self, uint32_t timeout_ms) {
  chttp_runtime_owner_t *owner = (chttp_runtime_owner_t *)self;
  if (!owner || owner->kind != CHTTP_PROVIDER_CLIENT)
    return SALTS_ENOTSUP;
  if (timeout_ms > owner->runtime.poll_budget_ms)
    timeout_ms = owner->runtime.poll_budget_ms;
  return turbo_flow_chttp_client_poll(
      owner->handle.client, timeout_ms, NULL);
}

static void owner_destroy(void *self) {
  chttp_runtime_owner_t *owner = (chttp_runtime_owner_t *)self;
  chttp_provider_root_t *root;
  int rc;

  if (!owner) return;
  root = owner->root;
  switch (owner->kind) {
  case CHTTP_PROVIDER_CLIENT:
    rc = turbo_flow_chttp_client_destroy(owner->handle.client);
    break;
  case CHTTP_PROVIDER_SERVER:
    rc = turbo_flow_chttp_server_destroy(owner->handle.server);
    break;
  case CHTTP_PROVIDER_WEBSOCKET:
    rc = turbo_flow_chttp_websocket_server_destroy(
        owner->handle.websocket);
    break;
  default:
    return;
  }
  if (rc != SALTS_OK) return;

  if (owner->tls_live) {
    if (chttp_tls_profile_destroy(&owner->tls) != SALTS_OK) return;
    owner->tls_live = false;
  }
  if (root && root->owners != 0u) --root->owners;
  memset(owner, 0, sizeof(*owner));
  free(owner);
}

CMETA_IMPLEMENTS(
    turbo_flow_runtime_owner, chttp_client_runtime_owner,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
        TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL,
    .quiesce = owner_quiesce,
    .drain = owner_drain,
    .shutdown = owner_shutdown,
    .poll = owner_poll,
    .destroy = owner_destroy);

CMETA_IMPLEMENTS(
    turbo_flow_runtime_owner, chttp_passive_runtime_owner,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD,
    .quiesce = owner_quiesce,
    .drain = owner_drain,
    .shutdown = owner_shutdown,
    .poll = owner_poll,
    .destroy = owner_destroy);

static int provider_contract(
    void *self, turbo_flow_provider_contract_v1_t *out) {
  chttp_provider_slot_t *slot = (chttp_provider_slot_t *)self;
  const char *resource_contract;
  if (!slot || !slot->root || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;

  resource_contract = provider_resource_contract(slot->kind);
  if (!resource_contract || !provider_artifact(slot->kind))
    return SALTS_EINVAL;

  *out =
      (turbo_flow_provider_contract_v1_t)
          TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory = TurboFlowCHttpProviderConfig_codec_create;
  out->config.message_artifact = provider_artifact(slot->kind);
  out->resource.contract_id = resource_contract;
  out->resource.contract_version =
      TURBO_FLOW_CHTTP_RESOURCE_CONTRACT_VERSION;
  out->resource.required_capabilities =
      TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
      (slot->kind == CHTTP_PROVIDER_CLIENT
           ? TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT
           : TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND);
  out->resource.expected_interface =
      turbo_flow_chttp_deployment_resource_interface();
  return SALTS_OK;
}

static int provider_preflight(
    void *self, const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  chttp_provider_slot_t *slot = (chttp_provider_slot_t *)self;
  chttp_typed_runtime_config_t runtime;
  if (!slot || !slot->root || !slot->root->started ||
      slot->root->stopping)
    return SALTS_ESHUTDOWN;
  return instance_runtime_config(slot, instance, &runtime, error);
}

static int provider_materialize(
    void *self, turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  chttp_provider_slot_t *slot = (chttp_provider_slot_t *)self;
  chttp_runtime_owner_t *owner;
  const char *stages[2] = {NULL, NULL};
  const char *source_name = NULL;
  size_t stage_count = 0u;
  const char *identity;
  int rc;

  if (!slot || !slot->root || !slot->root->started ||
      slot->root->stopping || !flow || !owner_out ||
      turbo_flow_runtime_owner_valid(owner_out))
    return SALTS_EINVAL;

  identity = provider_identity(slot->kind);
  if (!identity)
    return provider_fail(
        error, SALTS_EINVAL,
        instance ? instance->instance_name : NULL,
        "unknown CHTTP provider identity");

  owner = (chttp_runtime_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->root = slot->root;
  owner->kind = slot->kind;

  rc = instance_runtime_config(slot, instance, &owner->runtime, error);
  if (rc != SALTS_OK) goto fail;
  rc = stage_pair(
      flow, slot, instance, stages, &stage_count, &source_name, error);
  if (rc != SALTS_OK) goto fail;

  switch (slot->kind) {
  case CHTTP_PROVIDER_CLIENT:
    owner->runtime.client_adapter.flow = flow;
    owner->runtime.client_adapter.adapter_name = instance->instance_name;
    if (owner->runtime.tls_enabled) {
      rc = chttp_tls_profile_init(
          &owner->tls, &owner->runtime.tls_client);
      if (rc != SALTS_OK) {
        provider_fail(
            error, rc, instance->instance_name,
            "CHTTP TLS client profile initialization failed");
        goto fail;
      }
      owner->tls_live = true;
      owner->runtime.client_adapter.tls = &owner->tls;
    }
    rc = turbo_flow_chttp_client_register_provider(
        &owner->runtime.client_adapter, identity,
        stages, stage_count, &owner->handle.client);
    break;

  case CHTTP_PROVIDER_SERVER:
    owner->runtime.server_adapter.flow = flow;
    owner->runtime.server_adapter.adapter_name = instance->instance_name;
    owner->runtime.server_adapter.source_name = source_name;
    rc = turbo_flow_chttp_server_register_provider(
        &owner->runtime.server_adapter, identity,
        stages, stage_count, &owner->handle.server);
    break;

  case CHTTP_PROVIDER_WEBSOCKET:
    owner->runtime.websocket_adapter.flow = flow;
    owner->runtime.websocket_adapter.adapter_name = instance->instance_name;
    owner->runtime.websocket_adapter.source_name = source_name;
    rc = turbo_flow_chttp_websocket_server_register_provider(
        &owner->runtime.websocket_adapter, identity,
        stages, stage_count, &owner->handle.websocket);
    break;

  default:
    rc = SALTS_EINVAL;
    break;
  }

  if (rc != SALTS_OK) {
    provider_fail(
        error, rc, instance->instance_name,
        "CHTTP native adapter registration failed");
    goto fail;
  }

  ++slot->root->owners;
  *owner_out =
      slot->kind == CHTTP_PROVIDER_CLIENT
          ? chttp_client_runtime_owner_as_turbo_flow_runtime_owner(owner)
          : chttp_passive_runtime_owner_as_turbo_flow_runtime_owner(owner);
  if (error)
    *error =
        (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;

fail:
  if (owner->tls_live) {
    (void)chttp_tls_profile_destroy(&owner->tls);
    owner->tls_live = false;
  }
  free(owner);
  return rc;
}

CMETA_IMPLEMENTS(
    turbo_flow_provider_factory, chttp_provider_factory_impl, 0u,
    .contract = provider_contract,
    .preflight = provider_preflight,
    .materialize = provider_materialize);

static void provider_init(void) {
  static const unsigned kinds[CHTTP_PROVIDER_COUNT] = {
      CHTTP_PROVIDER_CLIENT,
      CHTTP_PROVIDER_SERVER,
      CHTTP_PROVIDER_WEBSOCKET};

  memset(&provider_root, 0, sizeof(provider_root));
  for (size_t i = 0u; i < CHTTP_PROVIDER_COUNT; ++i) {
    provider_root.slots[i].root = &provider_root;
    provider_root.slots[i].kind = kinds[i];
    provider_factories[i] =
        chttp_provider_factory_impl_as_turbo_flow_provider_factory(
            &provider_root.slots[i]);
    provider_exports[i] = (salts_plugin_export){
        .struct_size = SALTS_PLUGIN_EXPORT_SIZE,
        .kind = SALTS_PLUGIN_EXPORT_INTERFACE,
        .contract_version =
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
        .capabilities = 0u,
        .export_id = provider_identity(kinds[i]),
        .contract_id = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
        .value.interface = {
            .desc = turbo_flow_provider_factory_interface(),
            .value = &provider_factories[i],
        },
    };
  }
}

static salts_plugin_status SALTS_PLUGIN_CALL provider_start(void *self) {
  chttp_provider_root_t *root = (chttp_provider_root_t *)self;
  if (!root) return SALTS_PLUGIN_INVALID_ARGUMENT;
  if (root->owners != 0u) return SALTS_PLUGIN_BUSY;
  root->stopping = false;
  root->started = true;
  return SALTS_PLUGIN_OK;
}

static salts_plugin_status SALTS_PLUGIN_CALL
provider_request_stop(void *self) {
  chttp_provider_root_t *root = (chttp_provider_root_t *)self;
  if (!root) return SALTS_PLUGIN_INVALID_ARGUMENT;
  root->stopping = true;
  root->started = false;
  return SALTS_PLUGIN_OK;
}

static bool SALTS_PLUGIN_CALL provider_is_quiescent(const void *self) {
  const chttp_provider_root_t *root =
      (const chttp_provider_root_t *)self;
  return root && root->stopping && root->owners == 0u;
}

static void SALTS_PLUGIN_CALL provider_destroy(void *self) {
  chttp_provider_root_t *root = (chttp_provider_root_t *)self;
  if (!root || root->owners != 0u) return;
  root->started = false;
  root->stopping = true;
}

static salts_plugin_manifest provider_manifest = {
    .struct_size = SALTS_PLUGIN_MANIFEST_SIZE,
    .abi_version = SALTS_PLUGIN_ABI_VERSION,
    .plugin_id = "turbo-flow.chttp",
    .version = {2u, 0u, 0u},
    .self = &provider_root,
};

SALTS_PLUGIN_QUERY_EXPORT
const salts_plugin_manifest *SALTS_PLUGIN_CALL
salts_plugin_query(uint32_t host_abi) {
  if (host_abi != SALTS_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&provider_once, provider_init);
  provider_manifest.exports = provider_exports;
  provider_manifest.export_count = CHTTP_PROVIDER_COUNT;
  provider_manifest.start = provider_start;
  provider_manifest.request_stop = provider_request_stop;
  provider_manifest.is_quiescent = provider_is_quiescent;
  provider_manifest.destroy = provider_destroy;
  return &provider_manifest;
}
