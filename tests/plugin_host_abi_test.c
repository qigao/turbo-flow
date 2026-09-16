#include "turbo_flow_plugin.h"
#include <stdlib.h>
#include <string.h>
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

static void *abi_test_real_allocate(void *ctx, size_t size) {
  ++*(size_t *)ctx;
  return malloc(size);
}

static void abi_test_real_deallocate(void *ctx, void *memory) {
  ++*(size_t *)ctx;
  free(memory);
}

spec("production plugin ABI boundary") {
  it("publishes ABI 3.0 and accepts only an exact ABI 3.0 host before allocation") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    const uint32_t majors[] = {1u, 2u, 3u, 4u};
    const uint32_t minors[] = {0u, 1u};
    check_not_null(api);
    check_equal(api->abi_major, 3u);
    check_equal(api->abi_minor, 0u);
    for (size_t i = 0u; i < sizeof(majors) / sizeof(majors[0]); ++i) {
      for (size_t j = 0u; j < sizeof(minors) / sizeof(minors[0]); ++j) {
        turbo_flow_plugin_host_v1_t host = {0};
        size_t calls = 0u;
        void *plugin = &calls;
        host.size = sizeof(host);
        host.abi_major = majors[i];
        host.abi_minor = minors[j];
        host.ctx = &calls;
        host.allocate = abi_test_allocate;
        host.deallocate = abi_test_deallocate;
        if (majors[i] == 3u && minors[j] == 0u) {
          check_equal(api->load(&host, &plugin), SALTS_ENOMEM);
          check_equal(calls, (size_t)1);
        } else {
          check_not_equal(api->load(&host, &plugin), SALTS_OK);
          check_null(plugin);
          check_equal(calls, (size_t)0);
        }
      }
    }
  }

  it("rejects every non-exact host size without accessing tail fields") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    const size_t sizes[] = {
        0u, offsetof(turbo_flow_plugin_host_v1_t, abi_minor) + sizeof(uint32_t) - 1u,
        sizeof(turbo_flow_plugin_host_v1_t) - 1u, sizeof(turbo_flow_plugin_host_v1_t) + 1u};
    for (size_t i = 0u; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
      union {
        double alignment;
        unsigned char bytes[sizeof(turbo_flow_plugin_host_v1_t) - 1u];
      } storage;
      void *plugin = (void *)1;
      memset(&storage, 0, sizeof(storage));
      memcpy(storage.bytes, &sizes[i], sizeof(size_t));
      check_not_equal(api->load((const turbo_flow_plugin_host_v1_t *)storage.bytes, &plugin),
                      SALTS_OK);
      check_null(plugin);
    }
  }

  it("rejects a future-minor host service before allocation") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    turbo_flow_plugin_host_v1_t host = {0};
    size_t calls = 0u;
    void *plugin = (void *)1;
    host.size = sizeof(host);
    host.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
    host.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR + 1u;
    host.ctx = &calls;
    host.allocate = abi_test_allocate;
    host.deallocate = abi_test_deallocate;
    check_equal(api->load(&host, &plugin), SALTS_EINVAL);
    check_null(plugin);
    check_equal(calls, (size_t)0);
  }

  it("rejects a physical version-header-only host before reading service callbacks") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    struct host_version_header_s {
      size_t size;
      uint32_t abi_major;
      uint32_t abi_minor;
    } host = {sizeof(turbo_flow_plugin_host_v1_t), 2u, 0u};
    void *plugin = (void *)1;
    check_equal(api->load((const turbo_flow_plugin_host_v1_t *)&host, &plugin), SALTS_EINVAL);
    check_null(plugin);
  }

  it("rejects a physical version-header-only registration before reading callbacks") {
    const turbo_flow_plugin_api_v1_t *api = turbo_flow_plugin_get_api();
    turbo_flow_plugin_host_v1_t host = {0};
    struct registration_version_header_s {
      size_t size;
      uint32_t abi_major;
      uint32_t abi_minor;
    } registration = {sizeof(turbo_flow_plugin_registration_v1_t), 2u, 0u};
    size_t allocation_calls = 0u;
    void *plugin = NULL;
    host.size = sizeof(host);
    host.abi_major = 3u;
    host.abi_minor = 0u;
    host.ctx = &allocation_calls;
    host.allocate = abi_test_real_allocate;
    host.deallocate = abi_test_real_deallocate;
    check_equal(api->load(&host, &plugin), SALTS_OK);
    check_not_null(plugin);
    check_equal(api->register_capabilities(
                    plugin, (const turbo_flow_plugin_registration_v1_t *)&registration),
                SALTS_EINVAL);
    check_equal(api->quiesce(plugin, 0u), SALTS_OK);
    check_equal(api->shutdown(plugin), SALTS_OK);
    api->destroy(plugin);
    check_true(allocation_calls > 0u);
  }
}
