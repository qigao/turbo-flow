#include "turbo_flow_plugin.h"
#include <tinytest.h>

static void *abi_test_allocate(void *ctx, size_t size) {
  size_t *calls = (size_t *)ctx;
  (void)size;
  ++*calls;
  return NULL;
}

static void abi_test_deallocate(void *ctx, void *memory) {
  (void)memory;
  ++*(size_t *)ctx;
}

spec("production plugin ABI boundary") {
  it("publishes ABI 2.0 and rejects an ABI 1.4 host before allocation") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    turbo_flow_plugin_host_v1_t host = {0};
    size_t calls = 0u;
    void *plugin = &calls;
    check_not_null(api);
    check_equal(api->abi_major, 2u);
    check_equal(api->abi_minor, 0u);
    host.size = sizeof(host);
    host.abi_major = 1u;
    host.abi_minor = 4u;
    host.ctx = &calls;
    host.allocate = abi_test_allocate;
    host.deallocate = abi_test_deallocate;
    check_equal(api->load(&host, &plugin), SALTS_EPROTO);
    check_null(plugin);
    check_equal(calls, (size_t)0);
  }
}
