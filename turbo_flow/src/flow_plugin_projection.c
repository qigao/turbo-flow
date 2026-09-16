#include "turbo_flow_plugin_operation.h"
#include "turbo_flow_plugin.h"
#include <stdlib.h>

typedef struct flow_plugin_projection_bridge_s {
  turbo_flow_projection_owner_config_t original;
  turbo_flow_data_schema_t schema;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
} flow_plugin_projection_bridge_t;

static int flow_plugin_projection_clone(const void *value, void *ctx, void **out) {
  flow_plugin_projection_bridge_t *bridge = (flow_plugin_projection_bridge_t *)ctx;
  return bridge->original.clone(value, bridge->original.ctx, out);
}

static void flow_plugin_projection_destroy(void *value, void *ctx) {
  flow_plugin_projection_bridge_t *bridge = (flow_plugin_projection_bridge_t *)ctx;
  bridge->original.destroy(value, bridge->original.ctx);
}

static int flow_plugin_projection_release(void *ctx) {
  flow_plugin_projection_bridge_t *bridge = (flow_plugin_projection_bridge_t *)ctx;
  int rc = bridge->original.release_context(bridge->original.ctx);
  /* 失败保留完整 context 和模块 lease，控制线程可重试同一个 owner。 */
  if (rc != SALTS_OK) return rc;
  turbo_flow_plugin_catalog_snapshot_destroy(bridge->snapshot);
  free(bridge);
  return SALTS_OK;
}

int turbo_flow_plugin_projection_owner_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    const turbo_flow_projection_owner_config_t *config,
    turbo_flow_projection_owner_t **out) {
  flow_plugin_projection_bridge_t *bridge;
  turbo_flow_projection_owner_config_t adapted;
  int rc;
  if (!out) return SALTS_EINVAL;
  *out = NULL;
  /* 转发回调前验证原始必填项，不能用非 NULL 的 bridge callback 掩盖缺项。 */
  if (!snapshot || !config || config->size != sizeof(*config) ||
      config->abi_major != TURBO_FLOW_PROJECTION_ABI_MAJOR ||
      config->abi_minor != TURBO_FLOW_PROJECTION_ABI_MINOR || !config->schema ||
      config->schema->size != sizeof(*config->schema) || !config->destroy ||
      !config->release_context) return SALTS_EINVAL;
  bridge = (flow_plugin_projection_bridge_t *)malloc(sizeof(*bridge));
  if (!bridge) return SALTS_ENOMEM;
  bridge->original = *config;
  bridge->schema = *config->schema;
  bridge->original.schema = &bridge->schema;
  bridge->snapshot = snapshot;
  rc = turbo_flow_plugin_catalog_snapshot_retain(snapshot);
  if (rc != SALTS_OK) { free(bridge); return rc; }
  adapted = bridge->original;
  adapted.clone = config->clone ? flow_plugin_projection_clone : NULL;
  adapted.destroy = flow_plugin_projection_destroy;
  adapted.release_context = flow_plugin_projection_release;
  adapted.ctx = bridge;
  rc = turbo_flow_projection_owner_create(&adapted, out);
  if (rc != SALTS_OK) {
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    free(bridge);
  }
  return rc;
}
