#include "turbo_flow_provider.h"
#include "network_source_config_native.h"
#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct network_source_fixture_root_s network_source_fixture_root_t;
typedef struct network_source_fixture_owner_s network_source_fixture_owner_t;

struct network_source_fixture_root_s {
  network_source_fixture_owner_t *owner;
  int started;
};
static network_source_fixture_root_t root;

struct network_source_fixture_owner_s {
  network_source_fixture_root_t *root;
  char name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  int started;
  int quiesced;
  int status;
  uint64_t polls;
  uint32_t mode;
  unsigned drain_calls;
  unsigned shutdown_calls;
};

static int fixture_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int count;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  count = snprintf(metadata.uid, sizeof(metadata.uid), "fixture-source:%s", owner->name);
  if (count < 0 || (size_t)count >= sizeof(metadata.uid)) return SALTS_ERANGE;
  memcpy(metadata.owner_name, owner->name, strlen(owner->name) + 1u);
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  *out = metadata;
  return SALTS_OK;
}

static int fixture_descriptor(void *ctx, turbo_flow_managed_boundary_descriptor_t *out) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_managed_boundary_descriptor_t descriptor = TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  int rc;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  rc = fixture_metadata(owner, &metadata);
  if (rc != SALTS_OK) return rc;
  descriptor.domain = metadata.domain;
  descriptor.kind = metadata.kind;
  memcpy(descriptor.uid, metadata.uid, strlen(metadata.uid) + 1u);
  memcpy(descriptor.owner_name, metadata.owner_name, strlen(metadata.owner_name) + 1u);
  descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SOURCE;
  descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE;
  rc = turbo_flow_content_descriptor_init(&descriptor.output, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                          TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                          "application/octet-stream", "fixture.network.source");
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(&descriptor.output,
                                                    "ProtocolNetworkFixture", "MessageOwnedBytes",
                                                    1u);
  if (rc != SALTS_OK) return rc;
  *out = descriptor;
  return SALTS_OK;
}

static int fixture_boundary_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  int rc;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  rc = fixture_metadata(owner, &metadata);
  if (rc != SALTS_OK) return rc;
  memcpy(snapshot.uid, metadata.uid, strlen(metadata.uid) + 1u);
  snapshot.generation = 1u;
  snapshot.observed_generation = 1u;
  snapshot.state = owner->status != SALTS_OK
                       ? TURBO_FLOW_MANAGED_BOUNDARY_FAILED
                       : (owner->started ? TURBO_FLOW_MANAGED_BOUNDARY_RUNNING
                                         : TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED);
  snapshot.last_status = owner->status;
  *out = snapshot;
  return SALTS_OK;
}

static int fixture_source_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  (void)flow;
  (void)stage;
  if (!owner || owner->started || owner->quiesced) return SALTS_EBUSY;
  if (owner->mode == 4u) {
    owner->status = SALTS_EIO;
    return SALTS_EIO;
  }
  owner->started = 1;
  owner->status = SALTS_OK;
  return SALTS_OK;
}

static void fixture_source_stop(void *ctx, turbo_flow_t *flow,
                                const turbo_flow_stage_plan_t *stage) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  (void)flow;
  (void)stage;
  if (owner) owner->started = 0;
}

static void fixture_source_shutdown(void *ctx) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  if (owner) owner->started = 0;
}

static int fixture_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  int count;
  if (!owner || !out) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  out->adapter_name = owner->name;
  out->adapter_kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  out->direction = TURBO_FLOW_ADAPTER_INPUT;
  out->connection_limit = 1u;
  out->connections_current = owner->started ? 1u : 0u;
  out->state = owner->status != SALTS_OK
                   ? TURBO_FLOW_CONNECTION_FAILED
                   : (owner->started ? TURBO_FLOW_CONNECTION_READY
                                     : TURBO_FLOW_CONNECTION_STOPPED);
  out->last_status = owner->status;
  count = snprintf(out->endpoint, sizeof(out->endpoint), "tcp://127.0.0.1:%u",
                   owner->started ? 12345u : 0u);
  return count < 0 || (size_t)count >= sizeof(out->endpoint) ? SALTS_ERANGE : SALTS_OK;
}

static int fixture_register_source(network_source_fixture_owner_t *owner, turbo_flow_t *flow) {
  turbo_flow_adapter_ops_t ops = {0};
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_managed_boundary_provider_ops_t boundary =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  const char *stages[] = {owner->name};
  turbo_flow_provider_adapter_registration_v1_t registration = TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT;
  turbo_flow_source_transport_contract_t limits = TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT;
  int rc;
  ops.start = fixture_source_start;
  ops.stop = fixture_source_stop;
  ops.shutdown = fixture_source_shutdown;
  ops.connection_snapshot = fixture_connection_snapshot;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
  schema.direction = TURBO_FLOW_ADAPTER_INPUT;
  boundary.resource.metadata = fixture_metadata;
  boundary.descriptor = fixture_descriptor;
  boundary.snapshot = fixture_boundary_snapshot;
  registration.provider_identity = "fixture.source";
  registration.stage_names = stages;
  registration.stage_count = 1u;
  registration.adapter_ops = &ops;
  registration.schema = &schema;
  registration.managed_owner_name = owner->name;
  registration.managed_boundary_ops = &boundary;
  registration.ctx = owner;
  rc = turbo_flow_provider_adapter_register(flow, &registration);
  if (rc != SALTS_OK) return rc;
  limits.transport = owner->mode == 7u ? TURBO_FLOW_SOURCE_TRANSPORT_KCP_PACKET
                                      : TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER;
  limits.session_capacity = 1u;
  limits.max_message_bytes = 64u;
  limits.max_messages_per_poll = 2u;
  return turbo_flow_provider_adapter_source_transport_register(flow, owner->name, &limits);
}

static int fixture_owner_quiesce(void *ctx, uint64_t timeout_ms) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  owner->quiesced = 1;
  return SALTS_OK;
}

static int fixture_owner_drain(void *ctx, uint64_t timeout_ms) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  if ((owner->mode == 5u || owner->mode == 7u) && owner->drain_calls++ == 0u) return SALTS_EBUSY;
  return owner->started ? SALTS_EBUSY : SALTS_OK;
}

static int fixture_owner_shutdown(void *ctx) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  if (!owner || !owner->quiesced || owner->started) return SALTS_EBUSY;
  if (owner->mode == 6u && owner->shutdown_calls++ == 0u) return SALTS_EBUSY;
  return SALTS_OK;
}

static void fixture_owner_destroy(void *ctx) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  if (!owner) return;
  owner->root->owner = NULL;
  memset(owner, 0, sizeof(*owner));
  free(owner);
}

static int fixture_owner_poll(void *ctx, uint32_t timeout_ms) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner || !owner->started || owner->quiesced) return SALTS_EBUSY;
  if (owner->polls == UINT64_MAX) return SALTS_ERANGE;
  ++owner->polls;
  if (owner->mode == 3u) {
    owner->status = SALTS_EIO;
    return SALTS_EIO;
  }
  return SALTS_OK;
}

CMETA_IMPLEMENTS(turbo_flow_runtime_owner, fixture_runtime,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD | TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL,
    .quiesce = fixture_owner_quiesce, .drain = fixture_owner_drain,
    .shutdown = fixture_owner_shutdown, .poll = fixture_owner_poll, .destroy = fixture_owner_destroy);

static int fixture_contract(void *ctx, turbo_flow_provider_contract_v1_t *out) {
  if (ctx != &root || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  *out = (turbo_flow_provider_contract_v1_t)TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory = NetworkSourceFixture_codec_create;
  out->config.message_artifact = NetworkSourceConfig_native_artifact();
  return SALTS_OK;
}

static int fixture_preflight(void *ctx, const turbo_flow_provider_instance_v1_t *instance,
                             turbo_flow_config_error_t *error) {
  const NetworkSourceConfig_t *config;
  (void)error;
  if (ctx != &root || !root.started || !instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->config.value ||
      instance->config.value_bytes != sizeof(NetworkSourceConfig_t)) return SALTS_EINVAL;
  config = instance->config.value;
  return config->mode == 1u ? SALTS_EIO : SALTS_OK;
}

static int fixture_materialize(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_provider_instance_v1_t *instance,
                               turbo_flow_runtime_owner *owner_out,
                               turbo_flow_config_error_t *error) {
  network_source_fixture_owner_t *owner;
  const NetworkSourceConfig_t *config;
  int rc = fixture_preflight(ctx, instance, error);
  if (rc != SALTS_OK || !flow || !owner_out) return rc != SALTS_OK ? rc : SALTS_EINVAL;
  config = instance->config.value;
  if (config->mode == 2u) return SALTS_EIO;
  if (root.owner) return SALTS_EBUSY;
  if (strlen(instance->instance_name) >= sizeof(owner->name)) return SALTS_ERANGE;
  owner = calloc(1u, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->root = &root;
  owner->mode = config->mode;
  memcpy(owner->name, instance->instance_name, strlen(instance->instance_name) + 1u);
  rc = fixture_register_source(owner, flow);
  if (rc != SALTS_OK) { free(owner); return rc; }
  root.owner = owner;
  *owner_out = fixture_runtime_as_turbo_flow_runtime_owner(owner);
  return SALTS_OK;
}

CMETA_IMPLEMENTS(turbo_flow_provider_factory, fixture_factory, 0u,
    .contract = fixture_contract, .preflight = fixture_preflight, .materialize = fixture_materialize);
static turbo_flow_provider_factory factory;
static cmeta_plugin_export factory_export;
static cmeta_once_t once = SALTS_ONCE_INIT;

static void fixture_init(void) {
  factory = fixture_factory_as_turbo_flow_provider_factory(&root);
  factory_export = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE, .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .export_id = "fixture.source", .contract_id = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      .contract_version = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
      .value.interface = {.desc = turbo_flow_provider_factory_interface(), .value = &factory}};
}
static cmeta_plugin_status CMETA_PLUGIN_CALL fixture_start(void *ctx) {
  if (ctx != &root || root.owner) return CMETA_PLUGIN_BUSY;
  root.started = 1;
  return CMETA_PLUGIN_OK;
}
static cmeta_plugin_status CMETA_PLUGIN_CALL fixture_request_stop(void *ctx) {
  if (ctx != &root) return CMETA_PLUGIN_INVALID_ARGUMENT;
  root.started = 0;
  return CMETA_PLUGIN_OK;
}
static bool CMETA_PLUGIN_CALL fixture_is_quiescent(const void *ctx) {
  return ctx == &root && !root.started && !root.owner;
}
static void CMETA_PLUGIN_CALL fixture_destroy(void *ctx) {
  if (ctx == &root) memset(&root, 0, sizeof(root));
}
static cmeta_plugin_manifest manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE, .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "fixture.network.source", .version = {1u, 0u, 0u}, .self = &root,
    .start = fixture_start, .request_stop = fixture_request_stop,
    .is_quiescent = fixture_is_quiescent, .destroy = fixture_destroy};
CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  cmeta_once(&once, fixture_init);
  manifest.exports = &factory_export;
  manifest.export_count = 1u;
  return &manifest;
}
