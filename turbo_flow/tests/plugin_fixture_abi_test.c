#include "turbo_flow_plugin.h"
#include <tinytest.h>
#include <stdlib.h>

typedef struct fixture_probe_s { size_t allocations; size_t registrations; } fixture_probe_t;
static void *probe_allocate(void *ctx, size_t size) {
  ++((fixture_probe_t *)ctx)->allocations;
  return malloc(size);
}
static void probe_deallocate(void *ctx, void *value) {
  ++((fixture_probe_t *)ctx)->allocations;
  free(value);
}
static int probe_add_resource(void *ctx,
    const turbo_flow_plugin_product_resource_provider_v1_t *provider) {
  (void)provider; ++((fixture_probe_t *)ctx)->registrations; return SALTS_OK;
}
static int probe_add_transactional_adapter(
    void *ctx, const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider) {
  (void)provider; ++((fixture_probe_t *)ctx)->registrations; return SALTS_OK;
}
static int probe_add_transactional_resource(
    void *ctx, const turbo_flow_plugin_transactional_resource_provider_v1_t *provider) {
  (void)provider; ++((fixture_probe_t *)ctx)->registrations; return SALTS_OK;
}

spec("plugin fixture shared ABI boundary") {
  it("rejects invalid host and registration ABI without side effects") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    turbo_flow_plugin_host_v1_t host = {0};
    turbo_flow_plugin_registration_v1_t registration = {0};
    fixture_probe_t probe = {0};
    void *plugin = (void *)1;
    host.size = sizeof(host); host.abi_major = 2u; host.abi_minor = 0u;
    host.ctx = &probe; host.allocate = probe_allocate; host.deallocate = probe_deallocate;
    check_equal(api->load(&host, &plugin), SALTS_EINVAL);
    check_null(plugin);
    check_equal(probe.allocations, (size_t)0);
    host.abi_major = 3u;
    check_equal(api->load(&host, &plugin), SALTS_OK);
    check_not_null(plugin);
    registration.size = sizeof(registration); registration.abi_major = 2u;
    registration.ctx = &probe; registration.add_resource_provider = probe_add_resource;
    registration.add_transactional_adapter_provider = probe_add_transactional_adapter;
    registration.add_transactional_resource_provider = probe_add_transactional_resource;
    check_equal(api->register_capabilities(plugin, &registration), SALTS_EINVAL);
    check_equal(probe.registrations, (size_t)0);
    check_equal(api->quiesce(plugin, 0u), SALTS_OK);
    check_equal(api->shutdown(plugin), SALTS_OK);
    api->destroy(plugin);
  }
}
