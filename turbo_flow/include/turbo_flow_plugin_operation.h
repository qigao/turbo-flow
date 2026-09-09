#ifndef TURBO_FLOW_PLUGIN_OPERATION_H
#define TURBO_FLOW_PLUGIN_OPERATION_H

#include "turbo_flow_export.h"

#include <cmeta/data.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PLUGIN_SCHEMA_ABI_MINOR 4u

typedef struct turbo_flow_plugin_catalog_snapshot_s turbo_flow_plugin_catalog_snapshot_t;

typedef struct turbo_flow_plugin_schema_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t schema_version;
  const cmeta_data_desc *data;
} turbo_flow_plugin_schema_v1_t;

#define TURBO_FLOW_PLUGIN_SCHEMA_V1_INIT                                                   \
  {sizeof(turbo_flow_plugin_schema_v1_t), 1u, TURBO_FLOW_PLUGIN_SCHEMA_ABI_MINOR, 0u, NULL}

typedef struct turbo_flow_plugin_schema_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_schema_v1_t *schemas;
  size_t schema_count;
} turbo_flow_plugin_schema_catalog_v1_t;

#define TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT                                      \
  {sizeof(turbo_flow_plugin_schema_catalog_v1_t), 1u, TURBO_FLOW_PLUGIN_SCHEMA_ABI_MINOR, NULL, 0u}

typedef int (*turbo_flow_plugin_add_schema_fn)(void *ctx,
                                               const turbo_flow_plugin_schema_v1_t *schema);

TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_schema_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_schema_catalog_v1_t *catalog_out);

#ifdef __cplusplus
}
#endif
#endif
