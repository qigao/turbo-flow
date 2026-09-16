#include "turbo_flow_turbodb_durable_internal.h"

#include <string.h>

typedef struct plugin_root_s {
  const turbo_flow_plugin_host_v1_t *host;
  size_t owners;
  int quiesced;
} plugin_root_t;

typedef struct turbodb_owner_s {
  plugin_root_t *root;
  turbo_flow_inbox_t inbox;
  turbo_flow_durable_buffer_binding_t *binding;
} turbodb_owner_t;

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
    /* Opaque bindings cannot expose a safe recovery handle yet. Stay failed closed. */
    return entry.status;
  }
  return snapshot.records || snapshot.in_flight_claims ? SALTS_EBUSY : SALTS_OK;
}

static int owner_quiesce(void *ctx, uint64_t timeout_ms) {
  turbodb_owner_t *owner = ctx;
  int rc = owner_idle(owner);
  (void)timeout_ms;
  if (rc != SALTS_OK || !owner->inbox.ctx) return rc;
  return turbo_flow_inbox_close(&owner->inbox);
}

static int owner_drain(void *ctx, uint64_t timeout_ms) {
  (void)timeout_ms;
  return owner_idle(ctx);
}

static int owner_shutdown(void *ctx) {
  turbodb_owner_t *owner = ctx;
  int rc = owner_idle(owner);
  if (rc != SALTS_OK) return rc;
  if (owner->binding) {
    rc = turbo_flow_durable_buffer_unbind(owner->binding);
    if (rc != SALTS_OK) return rc;
    owner->binding = NULL;
  }
  return owner->inbox.ctx ? turbo_flow_inbox_destroy(&owner->inbox) : SALTS_OK;
}

static void owner_destroy(void *ctx) {
  turbodb_owner_t *owner = ctx;
  if (!owner || owner->binding || owner->inbox.ctx) return;
  plugin_root_t *root = owner->root;
  --root->owners;
  root->host->deallocate(root->host->ctx, owner);
}

static int owner_poll(void *ctx, uint32_t timeout_ms) {
  turbodb_owner_t *owner = ctx;
  (void)timeout_ms;
  return owner && owner->binding ? turbo_flow_durable_buffer_progress(owner->binding)
                                 : SALTS_ESHUTDOWN;
}

static int preflight(void *ctx, const turbo_flow_resolved_config_t *resolved, const char *name,
                     turbo_flow_config_error_t *error) {
  plugin_root_t *root = ctx;
  durable_turbodb_config_t config;
  if (!root || root->quiesced) return SALTS_ESHUTDOWN;
  return durable_turbodb_config_read(resolved, name, &config, error);
}

static int references(turbo_flow_t *flow, const char *name) {
  size_t count = 0u;
  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->resource_name || strcmp(stage->resource_name, name)) continue;
    if (!stage->is_buffer) return SALTS_EINVAL;
    ++count;
  }
  return count == 1u ? SALTS_OK : SALTS_EINVAL;
}

static int materialize(void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved,
                       const char *name, turbo_flow_plugin_product_owner_v1_t *out,
                       turbo_flow_config_error_t *error) {
  plugin_root_t *root = ctx;
  durable_turbodb_config_t config;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  turbo_flow_durable_buffer_binding_config_t binding =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  orm_config_t database;
  orm_option_t filename;
  orm_error_t orm_error;
  turbodb_owner_t *owner;
  int rc;
  if (!root || root->quiesced || !flow || !name || !out || out->size != sizeof(*out) ||
      out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  rc = durable_turbodb_config_read(resolved, name, &config, error);
  if (rc != SALTS_OK) return rc;
  rc = references(flow, name);
  if (rc != SALTS_OK) return rc;
  owner = root->host->allocate(root->host->ctx, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  memset(owner, 0, sizeof(*owner));
  owner->root = root;
  owner->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  orm_config(&database);
  filename.keyword = orm_view("filename");
  filename.value = orm_view(config.filename);
  database.driver = orm_view("sqlite");
  database.options = &filename;
  database.option_count = 1u;
  config.inbox.database = &database;
  rc = turbo_flow_turbodb_inbox_create(&config.inbox, &owner->inbox, &orm_error);
  if (rc != SALTS_OK) {
    root->host->deallocate(root->host->ctx, owner);
    return rc;
  }
  ++root->owners;
  descriptor.ctx = owner;
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD |
                     TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  descriptor.quiesce = owner_quiesce;
  descriptor.drain = owner_drain;
  descriptor.shutdown = owner_shutdown;
  descriptor.destroy = owner_destroy;
  descriptor.poll = owner_poll;
  binding.resource_name = name;
  binding.inbox = &owner->inbox;
  binding.identity_mode = config.identity_mode;
  binding.max_message_bytes = config.max_message_bytes;
  rc = turbo_flow_durable_buffer_bind(flow, &binding, &owner->binding);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_product_owner_publish(out, &descriptor);
  if (rc != SALTS_OK) {
    int cleanup = owner_quiesce(owner, 0u);
    if (cleanup == SALTS_OK) cleanup = owner_shutdown(owner);
    if (cleanup != SALTS_OK) return cleanup;
    owner_destroy(owner);
  }
  return rc;
}

static int plugin_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  plugin_root_t *root;
  if (out) *out = NULL;
  if (!out || !host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !host->allocate ||
      !host->deallocate)
    return SALTS_EINVAL;
  root = host->allocate(host->ctx, sizeof(*root));
  if (!root) return SALTS_ENOMEM;
  memset(root, 0, sizeof(*root));
  root->host = host;
  *out = root;
  return SALTS_OK;
}

static int plugin_register(void *ctx, const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_transactional_resource_provider_v1_t provider =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_RESOURCE_PROVIDER_V1_INIT;
  if (!ctx || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_transactional_resource_provider)
    return SALTS_EINVAL;
  provider.kind = FLOW_DURABLE_TURBODB_KIND;
  provider.ctx = ctx;
  provider.preflight = preflight;
  provider.materialize = materialize;
  return registration->add_transactional_resource_provider(registration->ctx, &provider);
}

static int plugin_quiesce(void *ctx, uint64_t timeout_ms) {
  plugin_root_t *root = ctx;
  (void)timeout_ms;
  if (!root) return SALTS_EINVAL;
  if (root->owners) return SALTS_EBUSY;
  root->quiesced = 1;
  return SALTS_OK;
}

static int plugin_shutdown(void *ctx) {
  plugin_root_t *root = ctx;
  return root && root->quiesced && !root->owners ? SALTS_OK : SALTS_EBUSY;
}

static void plugin_destroy(void *ctx) {
  plugin_root_t *root = ctx;
  if (root && !root->owners) root->host->deallocate(root->host->ctx, root);
}

static const turbo_flow_plugin_api_v1_t api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    "turbo-flow.durable.turbodb",
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE | TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL,
    plugin_load,
    plugin_register,
    plugin_quiesce,
    plugin_shutdown,
    plugin_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &api;
}
