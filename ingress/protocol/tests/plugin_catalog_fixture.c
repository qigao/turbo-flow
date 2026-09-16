#include "turbo_flow_plugin_protocol.h"

#include <string.h>

#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_ID
  #define FLOW_PROTOCOL_CATALOG_FIXTURE_ID "catalog.fixture.one"
#endif
#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_PROTOCOL
  #define FLOW_PROTOCOL_CATALOG_FIXTURE_PROTOCOL "catalog-protocol-one"
#endif
#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_BUSINESS
  #define FLOW_PROTOCOL_CATALOG_FIXTURE_BUSINESS "catalog-business-one"
#endif
#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_MODE
  #define FLOW_PROTOCOL_CATALOG_FIXTURE_MODE 0
#endif

typedef struct flow_protocol_catalog_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
} flow_protocol_catalog_fixture_t;

static int flow_protocol_catalog_open(void *ctx, const turbo_flow_protocol_open_request_t *request,
                                      turbo_flow_protocol_service_t *service) {
  (void)ctx;
  (void)request;
  (void)service;
  return SALTS_ENOTSUP;
}

static void flow_protocol_catalog_close(void *ctx, turbo_flow_protocol_service_t *service) {
  (void)ctx;
  if (service) memset(service, 0, sizeof(*service));
}

static int
flow_protocol_catalog_business_open(void *ctx,
                                    const turbo_flow_protocol_business_open_request_t *request,
                                    turbo_flow_protocol_business_service_t *service) {
  (void)ctx;
  (void)request;
  (void)service;
  return SALTS_ENOTSUP;
}

static void flow_protocol_catalog_business_close(void *ctx,
                                                 turbo_flow_protocol_business_service_t *service) {
  (void)ctx;
  if (service) memset(service, 0, sizeof(*service));
}

static int flow_protocol_catalog_load(const turbo_flow_plugin_host_v1_t *host, void **plugin_out) {
  flow_protocol_catalog_fixture_t *plugin;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !host->allocate ||
      !host->deallocate || !plugin_out)
    return SALTS_EINVAL;
  *plugin_out = NULL;
  plugin = (flow_protocol_catalog_fixture_t *)host->allocate(host->ctx, sizeof(*plugin));
  if (!plugin) return SALTS_ENOMEM;
  plugin->host = host;
  *plugin_out = plugin;
  return SALTS_OK;
}

static int flow_protocol_catalog_register(void *plugin,
                                          const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_protocol_provider_v1_t protocol = TURBO_FLOW_PLUGIN_PROTOCOL_PROVIDER_V1_INIT;
  int rc;
  if (!plugin || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_protocol_provider || !registration->add_business_provider)
    return SALTS_EINVAL;
  protocol.provider.size = sizeof(protocol.provider);
  protocol.provider.version_major = TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR;
  protocol.provider.version_minor = TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR;
  protocol.provider.name = FLOW_PROTOCOL_CATALOG_FIXTURE_PROTOCOL;
  protocol.provider.protocol = TURBO_FLOW_PROTOCOL_COAP;
  protocol.provider.capabilities = TURBO_FLOW_PROTOCOL_CAP_INGRESS |
                                   TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                                   TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE;
  protocol.provider.ctx = plugin;
  protocol.provider.open = flow_protocol_catalog_open;
  protocol.provider.close = flow_protocol_catalog_close;
  rc = registration->add_protocol_provider(registration->ctx, &protocol);
  if (rc != SALTS_OK) return rc;
#if FLOW_PROTOCOL_CATALOG_FIXTURE_MODE == 1
  return SALTS_OK;
#else
  turbo_flow_plugin_business_provider_v1_t business = TURBO_FLOW_PLUGIN_BUSINESS_PROVIDER_V1_INIT;
  business.provider.size = sizeof(business.provider);
  business.provider.version_major = TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MAJOR;
  business.provider.version_minor = TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MINOR;
  business.provider.business = FLOW_PROTOCOL_CATALOG_FIXTURE_BUSINESS;
  business.provider.protocol = TURBO_FLOW_PROTOCOL_OCPP;
  business.provider.capabilities = TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT;
  business.provider.ctx = plugin;
  business.provider.open = flow_protocol_catalog_business_open;
  business.provider.close = flow_protocol_catalog_business_close;
  return registration->add_business_provider(registration->ctx, &business);
#endif
}

static int flow_protocol_catalog_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static int flow_protocol_catalog_shutdown(void *plugin) { return plugin ? SALTS_OK : SALTS_EINVAL; }

static void flow_protocol_catalog_destroy(void *value) {
  flow_protocol_catalog_fixture_t *plugin = (flow_protocol_catalog_fixture_t *)value;
  const turbo_flow_plugin_host_v1_t *host;
  if (!plugin) return;
  host = plugin->host;
  plugin->host = NULL;
  host->deallocate(host->ctx, plugin);
}

static const turbo_flow_plugin_api_v1_t FLOW_PROTOCOL_CATALOG_API = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    FLOW_PROTOCOL_CATALOG_FIXTURE_ID,
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_PROTOCOL | TURBO_FLOW_PLUGIN_CAP_PROTOCOL_BUSINESS,
    flow_protocol_catalog_load,
    flow_protocol_catalog_register,
    flow_protocol_catalog_quiesce,
    flow_protocol_catalog_shutdown,
    flow_protocol_catalog_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &FLOW_PROTOCOL_CATALOG_API;
}
