#include "turbo_flow_plugin_generation.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef FLOW_PROTOCOL_NETWORK_SOURCE_KIND
#define FLOW_PROTOCOL_NETWORK_SOURCE_KIND "cnet.listener_source"
#endif
#ifndef FLOW_PROTOCOL_NETWORK_SOURCE_SCHEME
#define FLOW_PROTOCOL_NETWORK_SOURCE_SCHEME "tcp"
#endif

typedef struct network_source_fixture_root_s network_source_fixture_root_t;
typedef struct network_source_fixture_owner_s network_source_fixture_owner_t;

typedef struct network_source_fixture_provider_s {
  network_source_fixture_root_t *root;
} network_source_fixture_provider_t;

struct network_source_fixture_root_s {
  const turbo_flow_plugin_host_v1_t *host;
  network_source_fixture_provider_t provider;
  network_source_fixture_owner_t *owner;
  int quiesced;
};

struct network_source_fixture_owner_s {
  network_source_fixture_root_t *root;
  char name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  int started;
  int quiesced;
  int status;
  uint64_t polls;
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
  if (strcmp(owner->name, "start.fail") == 0) {
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
  count = snprintf(out->endpoint, sizeof(out->endpoint),
                   FLOW_PROTOCOL_NETWORK_SOURCE_SCHEME "://127.0.0.1:%u",
                   owner->started ? 12345u : 0u);
  return count < 0 || (size_t)count >= sizeof(out->endpoint) ? SALTS_ERANGE : SALTS_OK;
}

static int fixture_register_source(network_source_fixture_owner_t *owner, turbo_flow_t *flow) {
  turbo_flow_adapter_ops_t ops = {0};
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_managed_boundary_provider_ops_t boundary =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_source_registration_t registration =
      TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_INIT;
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
  registration.adapter_name = owner->name;
  registration.adapter_ops = &ops;
  registration.schema = &schema;
  registration.owner_name = owner->name;
  registration.boundary_ops = &boundary;
  registration.ctx = owner;
  return turbo_flow_register_managed_source_adapter(flow, &registration);
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
  return owner->started ? SALTS_EBUSY : SALTS_OK;
}

static int fixture_owner_shutdown(void *ctx) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  if (!owner || !owner->quiesced || owner->started) return SALTS_EBUSY;
  return SALTS_OK;
}

static void fixture_owner_destroy(void *ctx) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  const turbo_flow_plugin_host_v1_t *host;
  if (!owner) return;
  host = owner->root->host;
  owner->root->owner = NULL;
  memset(owner, 0, sizeof(*owner));
  host->deallocate(host->ctx, owner);
}

static int fixture_owner_poll(void *ctx, uint32_t timeout_ms) {
  network_source_fixture_owner_t *owner = (network_source_fixture_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner || !owner->started || owner->quiesced) return SALTS_EBUSY;
  if (owner->polls == UINT64_MAX) return SALTS_ERANGE;
  ++owner->polls;
  if (strcmp(owner->name, "poll.fail") == 0) {
    owner->status = SALTS_EIO;
    return SALTS_EIO;
  }
  return SALTS_OK;
}

static int fixture_preflight(void *ctx, const turbo_flow_resolved_config_t *resolved,
                             const char *name, turbo_flow_config_error_t *error) {
  network_source_fixture_provider_t *provider = (network_source_fixture_provider_t *)ctx;
  (void)resolved;
  if (!provider || !provider->root || provider->root->quiesced || !name || !name[0])
    return SALTS_EINVAL;
  if (strcmp(name, "preflight.fail") == 0) {
    if (error && error->size >= sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_EIO;
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.fixture_preflight", name);
    }
    return SALTS_EIO;
  }
  return SALTS_OK;
}

static int fixture_materialize(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_resolved_config_t *resolved, const char *name,
                               turbo_flow_plugin_product_owner_v1_t *owner_out,
                               turbo_flow_config_error_t *error) {
  network_source_fixture_provider_t *provider = (network_source_fixture_provider_t *)ctx;
  network_source_fixture_owner_t *owner;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  size_t name_size;
  int rc;
  (void)resolved;
  if (!provider || !provider->root || !flow || !name || !owner_out ||
      owner_out->size != sizeof(*owner_out))
    return SALTS_EINVAL;
  if (provider->root->owner) return SALTS_EBUSY;
  if (strcmp(name, "materialize.fail") == 0) {
    if (error && error->size >= sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_EIO;
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.fixture_materialize", name);
    }
    return SALTS_EIO;
  }
  name_size = strlen(name);
  if (name_size == 0u || name_size >= sizeof(owner->name)) return SALTS_ERANGE;
  owner = (network_source_fixture_owner_t *)provider->root->host->allocate(
      provider->root->host->ctx, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  memset(owner, 0, sizeof(*owner));
  owner->root = provider->root;
  owner->status = SALTS_OK;
  memcpy(owner->name, name, name_size + 1u);
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD |
                     TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  descriptor.ctx = owner;
  descriptor.quiesce = fixture_owner_quiesce;
  descriptor.drain = fixture_owner_drain;
  descriptor.shutdown = fixture_owner_shutdown;
  descriptor.destroy = fixture_owner_destroy;
  descriptor.poll = fixture_owner_poll;
  rc = turbo_flow_plugin_product_owner_publish(owner_out, &descriptor);
  if (rc != SALTS_OK) goto fail;
  rc = fixture_register_source(owner, flow);
  if (rc != SALTS_OK) {
    *owner_out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
    goto fail;
  }
  provider->root->owner = owner;
  return SALTS_OK;
fail:
  memset(owner, 0, sizeof(*owner));
  provider->root->host->deallocate(provider->root->host->ctx, owner);
  return rc;
}

static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **plugin_out) {
  network_source_fixture_root_t *root;
  if (plugin_out) *plugin_out = NULL;
  if (!host || !plugin_out || host->size != sizeof(*host) || !host->allocate || !host->deallocate)
    return SALTS_EINVAL;
  root = (network_source_fixture_root_t *)host->allocate(host->ctx, sizeof(*root));
  if (!root) return SALTS_ENOMEM;
  memset(root, 0, sizeof(*root));
  root->host = host;
  root->provider.root = root;
  *plugin_out = root;
  return SALTS_OK;
}

static int fixture_register(void *plugin,
                            const turbo_flow_plugin_registration_v1_t *registration) {
  network_source_fixture_root_t *root = (network_source_fixture_root_t *)plugin;
  turbo_flow_plugin_transactional_adapter_provider_v1_t provider =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT;
  if (!root || !registration || registration->size != sizeof(*registration) ||
      !registration->add_transactional_adapter_provider)
    return SALTS_EINVAL;
  provider.kind = FLOW_PROTOCOL_NETWORK_SOURCE_KIND;
  provider.ctx = &root->provider;
  provider.preflight = fixture_preflight;
  provider.materialize = fixture_materialize;
  return registration->add_transactional_adapter_provider(registration->ctx, &provider);
}

static int fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  network_source_fixture_root_t *root = (network_source_fixture_root_t *)plugin;
  (void)timeout_ms;
  if (!root) return SALTS_EINVAL;
  if (root->owner) return SALTS_EBUSY;
  root->quiesced = 1;
  return SALTS_OK;
}

static int fixture_shutdown(void *plugin) {
  network_source_fixture_root_t *root = (network_source_fixture_root_t *)plugin;
  return root && root->quiesced && !root->owner ? SALTS_OK : SALTS_EBUSY;
}

static void fixture_destroy(void *plugin) {
  network_source_fixture_root_t *root = (network_source_fixture_root_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!root) return;
  host = root->host;
  memset(root, 0, sizeof(*root));
  host->deallocate(host->ctx, root);
}

static const turbo_flow_plugin_api_v1_t fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    "turbo-flow.protocol-network-source-fixture",
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL,
    fixture_load,
    fixture_register,
    fixture_quiesce,
    fixture_shutdown,
    fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &fixture_api;
}
