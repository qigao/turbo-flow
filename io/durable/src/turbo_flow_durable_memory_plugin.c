#include "turbo_flow_durable_memory_internal.h"
#include <string.h>

typedef struct plugin_root_s {
  const turbo_flow_plugin_host_v1_t *host;
  size_t owners;
  int quiesced;
} plugin_root_t;
typedef struct memory_owner_s {
  plugin_root_t *root;
  turbo_flow_inbox_t inbox;
  turbo_flow_durable_buffer_binding_t *binding;
} memory_owner_t;

/* Retirement owns progress. Late owner callbacks only verify storage and release it. */
static int owner_idle(memory_owner_t *o) {
  turbo_flow_inbox_snapshot_t s = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  int rc;
  if (!o) return SALTS_EINVAL;
  if (!o->inbox.ctx) return SALTS_OK;
  rc = turbo_flow_inbox_snapshot(&o->inbox, &s);
  if (rc != SALTS_OK) return rc;
  if (s.failed_records) {
    turbo_flow_inbox_failed_entry_t entry = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    size_t count = 0u;
    rc = turbo_flow_inbox_scan_failed(&o->inbox, 0u, &entry, 1u, &count);
    if (rc != SALTS_OK) return rc;
    if (count != 1u || entry.status == SALTS_OK) return SALTS_EPROTO;
    return entry.status;
  }
  return s.records || s.in_flight_claims ? SALTS_EBUSY : SALTS_OK;
}
static int owner_quiesce(void *ctx, uint64_t timeout_ms) {
  memory_owner_t *o = ctx;
  int rc = owner_idle(o);
  (void)timeout_ms;
  if (rc != SALTS_OK || !o->inbox.ctx) return rc;
  return turbo_flow_inbox_close(&o->inbox);
}
static int owner_drain(void *ctx, uint64_t timeout_ms) {
  (void)timeout_ms;
  return owner_idle(ctx);
}
static int owner_shutdown(void *ctx) {
  memory_owner_t *o = ctx;
  int rc = owner_idle(o);
  if (rc != SALTS_OK) return rc;
  if (o->binding) {
    rc = turbo_flow_durable_buffer_unbind(o->binding);
    if (rc != SALTS_OK) return rc;
    o->binding = NULL;
  }
  return o->inbox.ctx ? turbo_flow_inbox_destroy(&o->inbox) : SALTS_OK;
}
static void owner_destroy(void *ctx) {
  memory_owner_t *o = ctx;
  if (!o || o->binding || o->inbox.ctx) return;
  plugin_root_t *root = o->root;
  --root->owners;
  root->host->deallocate(root->host->ctx, o);
}
static int owner_poll(void *ctx, uint32_t timeout_ms) {
  memory_owner_t *o = ctx;
  (void)timeout_ms;
  return o && o->binding ? turbo_flow_durable_buffer_progress(o->binding) : SALTS_ESHUTDOWN;
}
static int preflight(void *ctx, const turbo_flow_resolved_config_t *resolved, const char *name,
                     turbo_flow_config_error_t *error) {
  plugin_root_t *root = ctx;
  durable_memory_config_t config;
  if (!root || root->quiesced) return SALTS_ESHUTDOWN;
  return durable_memory_config_read(resolved, name, &config, error);
}
static int references(turbo_flow_t *flow, const char *name) {
  size_t count = 0u;
  for (size_t i=0u; i<turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *s = turbo_flow_stage_at(flow, i);
    if (!s || !s->resource_name || strcmp(s->resource_name, name)) continue;
    if (!s->is_buffer) return SALTS_EINVAL;
    ++count;
  }
  return count == 1u ? SALTS_OK : SALTS_EINVAL;
}
static int materialize(void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved,
                       const char *name, turbo_flow_plugin_product_owner_v1_t *out,
                       turbo_flow_config_error_t *error) {
  plugin_root_t *root = ctx;
  durable_memory_config_t config;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  turbo_flow_durable_buffer_binding_config_t binding = TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  memory_owner_t *o;
  int rc;
  if (!root || root->quiesced || !flow || !name || !out || out->size != sizeof(*out) ||
      out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) return SALTS_EINVAL;
  rc = durable_memory_config_read(resolved, name, &config, error);
  if (rc != SALTS_OK) return rc;
  rc = references(flow, name);
  if (rc != SALTS_OK) return rc;
  o = root->host->allocate(root->host->ctx, sizeof(*o));
  if (!o) return SALTS_ENOMEM;
  memset(o, 0, sizeof(*o)); o->root = root;
  o->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  rc = turbo_flow_inbox_memory_create(&config.memory, &o->inbox);
  if (rc != SALTS_OK) { root->host->deallocate(root->host->ctx, o); return rc; }
  ++root->owners;
  descriptor.ctx = o;
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD | TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  descriptor.quiesce = owner_quiesce; descriptor.drain = owner_drain;
  descriptor.shutdown = owner_shutdown; descriptor.destroy = owner_destroy; descriptor.poll = owner_poll;
  binding.resource_name = name; binding.inbox = &o->inbox;
  binding.identity_mode = config.identity_mode; binding.max_message_bytes = config.max_message_bytes;
  rc = turbo_flow_durable_buffer_bind(flow, &binding, &o->binding);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_product_owner_publish(out, &descriptor);
  if (rc != SALTS_OK) {
    /* No admission exists before compile, so built-in memory cleanup is synchronous. */
    int cleanup = owner_quiesce(o, 0u);
    if (cleanup == SALTS_OK) cleanup = owner_shutdown(o);
    if (cleanup != SALTS_OK) return cleanup;
    owner_destroy(o);
  }
  return rc;
}
static int plugin_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  plugin_root_t *root;
  if (out) *out = NULL;
  if (!out || !host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !host->allocate || !host->deallocate)
    return SALTS_EINVAL;
  root = host->allocate(host->ctx, sizeof(*root));
  if (!root) return SALTS_ENOMEM;
  memset(root, 0, sizeof(*root)); root->host = host; *out = root;
  return SALTS_OK;
}
static int plugin_register(void *ctx, const turbo_flow_plugin_registration_v1_t *r) {
  turbo_flow_plugin_transactional_resource_provider_v1_t p = TURBO_FLOW_PLUGIN_TRANSACTIONAL_RESOURCE_PROVIDER_V1_INIT;
  if (!ctx || !r || r->size != sizeof(*r) || r->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      r->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !r->add_transactional_resource_provider)
    return SALTS_EINVAL;
  p.kind = FLOW_DURABLE_MEMORY_KIND; p.ctx = ctx; p.preflight = preflight; p.materialize = materialize;
  return r->add_transactional_resource_provider(r->ctx, &p);
}
static int plugin_quiesce(void *ctx, uint64_t timeout_ms) {
  plugin_root_t *root = ctx; (void)timeout_ms;
  if (!root) return SALTS_EINVAL;
  if (root->owners) return SALTS_EBUSY;
  root->quiesced = 1; return SALTS_OK;
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
  sizeof(turbo_flow_plugin_api_v1_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
  TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, "turbo-flow.durable.memory", "1.0.0",
  TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE | TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL,
  plugin_load, plugin_register, plugin_quiesce, plugin_shutdown, plugin_destroy
};
TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) { return &api; }
