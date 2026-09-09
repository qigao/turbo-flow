#include "plugin_projection_fixture.h"
#include "turbo_flow_plugin.h"
#include <stdlib.h>

typedef struct projection_context_s { projection_observer_t *observer; } projection_context_t;
static const turbo_flow_data_schema_t projection_schema = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
    TURBO_FLOW_DATA_ENCODING_OPAQUE, "fixture.projection", "Integer", "fixture.int", 1u, 1u, NULL};

static void projection_record(projection_observer_t *observer, int event) {
  salts_mutex_lock(&observer->mutex);
  if (observer->count < PROJECTION_TEST_EVENTS) observer->events[observer->count++] = event;
  salts_mutex_unlock(&observer->mutex);
}
static int projection_barrier(projection_observer_t *observer, int destroying) {
  int error;
  salts_mutex_lock(&observer->mutex);
  if (destroying ? observer->block_destroy : observer->block_clone) {
    ++observer->entered;
    salts_cond_broadcast(&observer->cond);
    while (!observer->proceed) salts_cond_wait(&observer->cond, &observer->mutex);
  }
  error = observer->clone_error;
  salts_mutex_unlock(&observer->mutex);
  return error;
}
static int projection_clone(const void *value, void *ctx, void **out) {
  projection_context_t *context = (projection_context_t *)ctx;
  int error = projection_barrier(context->observer, 0);
  int *copy = (int *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const int *)value;
  *out = copy;
  return error ? SALTS_EIO : SALTS_OK;
}
static void projection_destroy(void *value, void *ctx) {
  projection_context_t *context = (projection_context_t *)ctx;
  (void)projection_barrier(context->observer, 1);
  free(value);
  projection_record(context->observer, PROJECTION_PAYLOAD_DESTROY);
}
static int projection_release(void *ctx) {
  projection_context_t *context = (projection_context_t *)ctx;
  projection_observer_t *observer = context->observer;
  int busy;
  salts_mutex_lock(&observer->mutex);
  ++observer->release_attempts;
  busy = observer->release_busy;
  salts_mutex_unlock(&observer->mutex);
  if (busy) return SALTS_EBUSY;
  projection_record(observer, PROJECTION_CONTEXT_RELEASE);
  free(context);
  return SALTS_OK;
}
static int projection_create(projection_observer_t *observer,
                             turbo_flow_projection_owner_config_t *config, void **value) {
  turbo_flow_projection_owner_config_t result = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  projection_context_t *context;
  int *payload;
  if (!observer || !config || !value) return SALTS_EINVAL;
  *value = NULL;
  context = (projection_context_t *)malloc(sizeof(*context));
  if (!context) return SALTS_ENOMEM;
  payload = (int *)malloc(sizeof(*payload));
  if (!payload) { free(context); return SALTS_ENOMEM; }
  context->observer = observer;
  *payload = PROJECTION_TEST_VALUE;
  result.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                 TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  result.capacity = PROJECTION_TEST_CAPACITY;
  result.max_result_bytes = sizeof(*payload);
  result.max_retained_bytes = PROJECTION_TEST_CAPACITY * sizeof(*payload);
  result.schema = &projection_schema;
  result.clone = projection_clone;
  result.destroy = projection_destroy;
  result.release_context = projection_release;
  result.ctx = context;
  *config = result;
  *value = payload;
  return SALTS_OK;
}
static projection_fixture_protocol_t projection_protocol = {projection_create};
static int fixture_resource(void *ctx, turbo_flow_t *flow,
    const turbo_flow_resolved_config_t *resolved, const char *name, turbo_flow_config_error_t *error) {
  (void)ctx; (void)flow; (void)resolved; (void)name; (void)error;
  /* 本 provider 仅发布测试协议，不提供 Product resource 实例。 */
  return SALTS_ENOTSUP;
}
static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  if (!host || !out) return SALTS_EINVAL;
  *out = &projection_protocol;
  return SALTS_OK;
}
static int fixture_register(void *plugin, const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_product_resource_provider_v1_t provider =
      TURBO_FLOW_PLUGIN_PRODUCT_RESOURCE_PROVIDER_V1_INIT;
  if (!plugin || !registration || !registration->add_resource_provider) return SALTS_EINVAL;
  provider.provider.kind = "fixture.projection";
  provider.provider.ctx = plugin;
  provider.provider.register_resource = fixture_resource;
  return registration->add_resource_provider(registration->ctx, &provider);
}
static int fixture_quiesce(void *plugin, uint64_t timeout) {
  (void)timeout; return plugin ? SALTS_OK : SALTS_EINVAL;
}
static int fixture_shutdown(void *plugin) { return plugin ? SALTS_OK : SALTS_EINVAL; }
static void fixture_destroy(void *plugin) {
  /* 静态 protocol 无拥有型资源，结果 context 由独立 release callback 释放。 */
  (void)plugin;
}
static const turbo_flow_plugin_api_v1_t fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, "fixture.projection", "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_PRODUCT_RESOURCE, fixture_load, fixture_register,
    fixture_quiesce, fixture_shutdown, fixture_destroy};
TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &fixture_api;
}
