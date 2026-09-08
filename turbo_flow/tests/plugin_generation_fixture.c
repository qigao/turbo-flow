#include "turbo_flow_plugin_generation.h"

#include <string.h>

#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_ID
  #define FLOW_PLUGIN_GENERATION_FIXTURE_ID "fixture.transactional"
#endif
#ifndef FLOW_PLUGIN_GENERATION_ADAPTER_KIND
  #define FLOW_PLUGIN_GENERATION_ADAPTER_KIND "fixture.transactional.adapter"
#endif
#ifndef FLOW_PLUGIN_GENERATION_RESOURCE_KIND
  #define FLOW_PLUGIN_GENERATION_RESOURCE_KIND "fixture.transactional.resource"
#endif

typedef struct flow_plugin_generation_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
} flow_plugin_generation_fixture_t;

typedef struct flow_plugin_generation_owner_s {
  const turbo_flow_plugin_host_v1_t *host;
} flow_plugin_generation_owner_t;

static int flow_plugin_generation_preflight(void *ctx, const turbo_flow_resolved_config_t *resolved,
                                            const char *name, turbo_flow_config_error_t *error) {
  (void)error;
  return ctx && resolved && name && name[0] ? SALTS_OK : SALTS_EINVAL;
}

static int flow_plugin_generation_owner_quiesce(void *ctx, uint64_t timeout_ms) {
  (void)timeout_ms;
  return ctx ? SALTS_OK : SALTS_EINVAL;
}

static int flow_plugin_generation_owner_drain(void *ctx, uint64_t timeout_ms) {
  (void)timeout_ms;
  return ctx ? SALTS_OK : SALTS_EINVAL;
}

static int flow_plugin_generation_owner_shutdown(void *ctx) {
  return ctx ? SALTS_OK : SALTS_EINVAL;
}

static void flow_plugin_generation_owner_destroy(void *ctx) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  const turbo_flow_plugin_host_v1_t *host;
  if (!owner) return;
  host = owner->host;
  memset(owner, 0, sizeof(*owner));
  host->deallocate(host->ctx, owner);
}

static int flow_plugin_generation_materialize_adapter(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  flow_plugin_generation_owner_t *owner;
  int rc;
  (void)resolved;
  (void)error;
  if (!fixture || !flow || !name || !name[0] || !owner_out) return SALTS_EINVAL;
  owner =
      (flow_plugin_generation_owner_t *)fixture->host->allocate(fixture->host->ctx, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->host = fixture->host;
  rc = turbo_flow_register_adapter(flow, name, NULL, owner);
  if (rc != SALTS_OK) {
    fixture->host->deallocate(fixture->host->ctx, owner);
    return rc;
  }
  *owner_out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  owner_out->flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  owner_out->ctx = owner;
  owner_out->quiesce = flow_plugin_generation_owner_quiesce;
  owner_out->drain = flow_plugin_generation_owner_drain;
  owner_out->shutdown = flow_plugin_generation_owner_shutdown;
  owner_out->destroy = flow_plugin_generation_owner_destroy;
  return SALTS_OK;
}

static int flow_plugin_generation_materialize_resource(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  flow_plugin_generation_owner_t *owner;
  (void)flow;
  (void)resolved;
  (void)error;
  if (!fixture || !name || !name[0] || !owner_out) return SALTS_EINVAL;
  owner =
      (flow_plugin_generation_owner_t *)fixture->host->allocate(fixture->host->ctx, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->host = fixture->host;
  *owner_out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  owner_out->flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  owner_out->ctx = owner;
  owner_out->quiesce = flow_plugin_generation_owner_quiesce;
  owner_out->drain = flow_plugin_generation_owner_drain;
  owner_out->shutdown = flow_plugin_generation_owner_shutdown;
  owner_out->destroy = flow_plugin_generation_owner_destroy;
  return SALTS_OK;
}

static int flow_plugin_generation_fixture_load(const turbo_flow_plugin_host_v1_t *host,
                                               void **plugin_out) {
  flow_plugin_generation_fixture_t *fixture;
  if (!host || !plugin_out || !host->allocate || !host->deallocate) return SALTS_EINVAL;
  *plugin_out = NULL;
  fixture = (flow_plugin_generation_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  fixture->host = host;
  *plugin_out = fixture;
  return SALTS_OK;
}

static int
flow_plugin_generation_fixture_register(void *plugin,
                                        const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_transactional_adapter_provider_v1_t adapter =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT;
  turbo_flow_plugin_transactional_resource_provider_v1_t resource =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_RESOURCE_PROVIDER_V1_INIT;
  int rc;
  if (!plugin || !registration || !registration->add_transactional_adapter_provider ||
      !registration->add_transactional_resource_provider)
    return SALTS_EINVAL;
  adapter.kind = FLOW_PLUGIN_GENERATION_ADAPTER_KIND;
  adapter.ctx = plugin;
  adapter.preflight = flow_plugin_generation_preflight;
  adapter.materialize = flow_plugin_generation_materialize_adapter;
  resource.kind = FLOW_PLUGIN_GENERATION_RESOURCE_KIND;
  resource.ctx = plugin;
  resource.preflight = flow_plugin_generation_preflight;
  resource.materialize = flow_plugin_generation_materialize_resource;
  rc = registration->add_transactional_adapter_provider(registration->ctx, &adapter);
  if (rc != SALTS_OK) return rc;
  return registration->add_transactional_resource_provider(registration->ctx, &resource);
}

static int flow_plugin_generation_fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static int flow_plugin_generation_fixture_shutdown(void *plugin) {
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static void flow_plugin_generation_fixture_destroy(void *plugin) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!fixture) return;
  host = fixture->host;
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t flow_plugin_generation_fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    FLOW_PLUGIN_GENERATION_FIXTURE_ID,
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE,
    flow_plugin_generation_fixture_load,
    flow_plugin_generation_fixture_register,
    flow_plugin_generation_fixture_quiesce,
    flow_plugin_generation_fixture_shutdown,
    flow_plugin_generation_fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &flow_plugin_generation_fixture_api;
}
