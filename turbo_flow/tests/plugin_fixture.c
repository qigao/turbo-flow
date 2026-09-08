#include "turbo_flow_plugin.h"

#include <string.h>

#if defined(FLOW_PLUGIN_FIXTURE_NON_ASCII_ID)
  #define FLOW_PLUGIN_FIXTURE_ID "fixture.\xC3\xA9"
#elif !defined(FLOW_PLUGIN_FIXTURE_ID)
  #define FLOW_PLUGIN_FIXTURE_ID "fixture.one"
#endif

#ifndef FLOW_PLUGIN_FIXTURE_VERSION
  #define FLOW_PLUGIN_FIXTURE_VERSION "1.0.0"
#endif

#ifndef FLOW_PLUGIN_FIXTURE_ADAPTER_KIND
  #define FLOW_PLUGIN_FIXTURE_ADAPTER_KIND "fixture.adapter.one"
#endif

#ifndef FLOW_PLUGIN_FIXTURE_RESOURCE_KIND
  #define FLOW_PLUGIN_FIXTURE_RESOURCE_KIND "fixture.resource.one"
#endif

#ifndef FLOW_PLUGIN_FIXTURE_ABI_MAJOR
  #define FLOW_PLUGIN_FIXTURE_ABI_MAJOR TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR
#endif

#ifndef FLOW_PLUGIN_FIXTURE_API_SIZE
  #define FLOW_PLUGIN_FIXTURE_API_SIZE sizeof(turbo_flow_plugin_api_v1_t)
#endif

#define FLOW_PLUGIN_FIXTURE_MODE_GOOD 0
#define FLOW_PLUGIN_FIXTURE_MODE_REGISTER_FAIL 1
#define FLOW_PLUGIN_FIXTURE_MODE_DUPLICATE_STAGED 2
#define FLOW_PLUGIN_FIXTURE_MODE_LOAD_FAIL 3
#define FLOW_PLUGIN_FIXTURE_MODE_QUIESCE_ONCE 4
#define FLOW_PLUGIN_FIXTURE_MODE_SHUTDOWN_ONCE 5
#define FLOW_PLUGIN_FIXTURE_MODE_CAPABILITY_MISMATCH 6
#define FLOW_PLUGIN_FIXTURE_MODE_INVALID_PROVIDER 7
#define FLOW_PLUGIN_FIXTURE_MODE_INVALID_PROVIDER_ABI 8

#ifndef FLOW_PLUGIN_FIXTURE_MODE
  #define FLOW_PLUGIN_FIXTURE_MODE FLOW_PLUGIN_FIXTURE_MODE_GOOD
#endif
#ifndef FLOW_PLUGIN_FIXTURE_CAPABILITIES
  #define FLOW_PLUGIN_FIXTURE_CAPABILITIES                                                         \
    (TURBO_FLOW_PLUGIN_CAP_PRODUCT_ADAPTER | TURBO_FLOW_PLUGIN_CAP_PRODUCT_RESOURCE)
#endif

typedef struct flow_plugin_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
} flow_plugin_fixture_t;

static int flow_plugin_fixture_register_adapter(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *adapter_name,
                                                turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)flow;
  (void)resolved;
  (void)adapter_name;
  (void)error;
  return SALTS_OK;
}

static int flow_plugin_fixture_register_resource(void *ctx, turbo_flow_t *flow,
                                                 const turbo_flow_resolved_config_t *resolved,
                                                 const char *resource_name,
                                                 turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)flow;
  (void)resolved;
  (void)resource_name;
  (void)error;
  return SALTS_OK;
}

static int flow_plugin_fixture_load(const turbo_flow_plugin_host_v1_t *host, void **plugin_out) {
  flow_plugin_fixture_t *fixture;
  if (!host || !plugin_out || !host->allocate || !host->deallocate) return SALTS_EINVAL;
  *plugin_out = NULL;
  fixture = (flow_plugin_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  fixture->host = host;
  *plugin_out = fixture;
#if FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_LOAD_FAIL
  return SALTS_EIO;
#else
  return SALTS_OK;
#endif
}

static int flow_plugin_fixture_register(void *plugin,
                                        const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_product_adapter_provider_v1_t adapter =
      TURBO_FLOW_PLUGIN_PRODUCT_ADAPTER_PROVIDER_V1_INIT;
  turbo_flow_plugin_product_resource_provider_v1_t resource =
      TURBO_FLOW_PLUGIN_PRODUCT_RESOURCE_PROVIDER_V1_INIT;
  int rc;
  if (!plugin || !registration || !registration->add_adapter_provider ||
      !registration->add_resource_provider)
    return SALTS_EINVAL;
  adapter.provider.kind = FLOW_PLUGIN_FIXTURE_ADAPTER_KIND;
  adapter.provider.register_adapter = flow_plugin_fixture_register_adapter;
  adapter.provider.ctx = plugin;
#if FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_INVALID_PROVIDER
  adapter.size = 0u;
#elif FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_INVALID_PROVIDER_ABI
  adapter.abi_major = 99u;
#endif
  resource.provider.kind = FLOW_PLUGIN_FIXTURE_RESOURCE_KIND;
  resource.provider.register_resource = flow_plugin_fixture_register_resource;
  resource.provider.ctx = plugin;
  rc = registration->add_adapter_provider(registration->ctx, &adapter);
  if (rc != SALTS_OK) return rc;
#if FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_REGISTER_FAIL
  return SALTS_EIO;
#elif FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_DUPLICATE_STAGED
  return registration->add_adapter_provider(registration->ctx, &adapter);
#elif FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_CAPABILITY_MISMATCH
  return SALTS_OK;
#else
  return registration->add_resource_provider(registration->ctx, &resource);
#endif
}

static int flow_plugin_fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  static int calls;
  (void)timeout_ms;
#if FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_QUIESCE_ONCE
  if (calls++ == 0) return SALTS_EIO;
#else
  (void)calls;
#endif
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static int flow_plugin_fixture_shutdown(void *plugin) {
#if FLOW_PLUGIN_FIXTURE_MODE == FLOW_PLUGIN_FIXTURE_MODE_SHUTDOWN_ONCE
  static int calls;
  if (calls++ == 0) return SALTS_EIO;
#endif
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static void flow_plugin_fixture_destroy(void *plugin) {
  flow_plugin_fixture_t *fixture = (flow_plugin_fixture_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!fixture) return;
  host = fixture->host;
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t flow_plugin_fixture_api = {
    FLOW_PLUGIN_FIXTURE_API_SIZE,
    FLOW_PLUGIN_FIXTURE_ABI_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    FLOW_PLUGIN_FIXTURE_ID,
    FLOW_PLUGIN_FIXTURE_VERSION,
    FLOW_PLUGIN_FIXTURE_CAPABILITIES,
    flow_plugin_fixture_load,
    flow_plugin_fixture_register,
    flow_plugin_fixture_quiesce,
    flow_plugin_fixture_shutdown,
#if defined(FLOW_PLUGIN_FIXTURE_MISSING_DESTROY)
    NULL};
#else
    flow_plugin_fixture_destroy};
#endif

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &flow_plugin_fixture_api;
}
