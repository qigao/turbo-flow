/* Test-only DLL: real providers with allocator fault injection or selective registration. */
#include "../src/turbo_flow_chttp_plugin_internal.h"
#include <stdlib.h>
static unsigned delivery_quiesce_attempts;
#if CHTTP_DELIVERY_MODE == 3
static int delivery_server_quiesce(turbo_flow_chttp_server_t *server);
  #define turbo_flow_chttp_server_quiesce delivery_server_quiesce
#elif CHTTP_DELIVERY_MODE == 4
static int delivery_websocket_quiesce(turbo_flow_chttp_websocket_server_t *server);
  #define turbo_flow_chttp_websocket_server_quiesce delivery_websocket_quiesce
#endif
#undef TURBO_FLOW_PLUGIN_ENTRY
#define TURBO_FLOW_PLUGIN_ENTRY
#define turbo_flow_plugin_get_api chttp_delivery_native_api
#include "../src/turbo_flow_chttp_plugin.c"
#undef turbo_flow_plugin_get_api
#if CHTTP_DELIVERY_MODE == 3
  #undef turbo_flow_chttp_server_quiesce
static int delivery_server_quiesce(turbo_flow_chttp_server_t *server) {
  if (++delivery_quiesce_attempts == 1u) return SALTS_ETIMEDOUT;
  return turbo_flow_chttp_server_quiesce(server);
}
#elif CHTTP_DELIVERY_MODE == 4
  #undef turbo_flow_chttp_websocket_server_quiesce
static int delivery_websocket_quiesce(turbo_flow_chttp_websocket_server_t *server) {
  if (++delivery_quiesce_attempts == 1u) return SALTS_EBUSY;
  return turbo_flow_chttp_websocket_server_quiesce(server);
}
#endif

enum { FIXTURE_ALLOCATIONS = 4, FIXTURE_FAIL_SECOND_OWNER = 3 };
typedef struct delivery_root_s {
  const turbo_flow_plugin_host_v1_t *outer;
  turbo_flow_plugin_host_v1_t inner;
  void *native;
  void *allocations[FIXTURE_ALLOCATIONS];
  size_t attempts, allocated, freed;
  int invalid_free;
} delivery_root_t;

static void *delivery_allocate(void *ctx, size_t bytes) {
  delivery_root_t *root = ctx;
  ++root->attempts;
#if CHTTP_DELIVERY_MODE == 0
  if (root->attempts == FIXTURE_FAIL_SECOND_OWNER) return NULL;
#endif
  if (root->allocated >= FIXTURE_ALLOCATIONS) return NULL;
  void *memory = root->outer->allocate(root->outer->ctx, bytes);
  if (memory) root->allocations[root->allocated++] = memory;
  return memory;
}
static void delivery_deallocate(void *ctx, void *memory) {
  delivery_root_t *root = ctx;
  for (size_t i = 0u; i < root->allocated; ++i) {
    if (root->allocations[i] == memory && memory) {
      root->allocations[i] = NULL;
      ++root->freed;
      root->outer->deallocate(root->outer->ctx, memory);
      return;
    }
  }
  root->invalid_free = 1;
}
static int delivery_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  delivery_quiesce_attempts = 0u;
  delivery_root_t *root = host->allocate(host->ctx, sizeof(*root));
  if (!root) return SALTS_ENOMEM;
  memset(root, 0, sizeof(*root));
  root->outer = host;
  root->inner = *host;
  root->inner.ctx = root;
  root->inner.allocate = delivery_allocate;
  root->inner.deallocate = delivery_deallocate;
  int rc = chttp_delivery_native_api()->load(&root->inner, &root->native);
  if (rc != SALTS_OK) {
    host->deallocate(host->ctx, root);
    return rc;
  }
  *out = root;
  return SALTS_OK;
}
typedef struct delivery_registration_s {
  const turbo_flow_plugin_registration_v1_t *outer;
  size_t index;
} delivery_registration_t;
static int delivery_add(void *ctx, const turbo_flow_plugin_transactional_adapter_provider_v1_t *p) {
  delivery_registration_t *registration = ctx;
#if CHTTP_DELIVERY_MODE == 1 || CHTTP_DELIVERY_MODE == 2
  if (registration->index++ != CHTTP_DELIVERY_MODE) return SALTS_OK;
#endif
  return registration->outer->add_transactional_adapter_provider(registration->outer->ctx, p);
}
static int delivery_register(void *ctx, const turbo_flow_plugin_registration_v1_t *registration) {
  delivery_root_t *root = ctx;
  delivery_registration_t capture = {.outer = registration};
  turbo_flow_plugin_registration_v1_t inner = *registration;
  inner.ctx = &capture;
  inner.add_transactional_adapter_provider = delivery_add;
  return chttp_delivery_native_api()->register_capabilities(root->native, &inner);
}
static int delivery_quiesce(void *ctx, uint64_t timeout) {
  delivery_root_t *root = ctx;
  if (root->invalid_free || root->allocated - root->freed != 1u) return SALTS_EPROTO;
#if CHTTP_DELIVERY_MODE == 0
  if (root->attempts != FIXTURE_FAIL_SECOND_OWNER || root->allocated != 2u || root->freed != 1u)
    return SALTS_EPROTO;
#endif
#if CHTTP_DELIVERY_MODE == 3 || CHTTP_DELIVERY_MODE == 4
  if (delivery_quiesce_attempts != 2u || root->allocated != 2u || root->freed != 1u)
    return SALTS_EPROTO;
#endif
#if CHTTP_DELIVERY_MODE == 5
  if (root->allocated != 2u || root->freed != 1u) return SALTS_EPROTO;
#endif
  return chttp_delivery_native_api()->quiesce(root->native, timeout);
}
static int delivery_shutdown(void *ctx) {
  delivery_root_t *root = ctx;
  return chttp_delivery_native_api()->shutdown(root->native);
}
static void delivery_destroy(void *ctx) {
  delivery_root_t *root = ctx;
  chttp_delivery_native_api()->destroy(root->native);
  /* Violations intentionally retain the fixture so ASan cannot hide a foreign/double free. */
  if (root->invalid_free || root->allocated != root->freed) abort();
  root->outer->deallocate(root->outer->ctx, root);
}
#if defined(_WIN32)
__declspec(dllexport)
#elif defined(__GNUC__)
__attribute__((visibility("default")))
#endif
const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  static const turbo_flow_plugin_api_v1_t delivery_api = {
      sizeof(turbo_flow_plugin_api_v1_t),
      TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
      TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
      "test.chttp.delivery",
      "1.0.0",
      TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL,
      delivery_load,
      delivery_register,
      delivery_quiesce,
      delivery_shutdown,
      delivery_destroy};
  return &delivery_api;
}
