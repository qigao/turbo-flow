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

/**
 * Size/version wrapper for one DLL-owned schema descriptor.
 *
 * The plugin must keep this metadata, the referenced CMeta descriptor tree,
 * and every callback reachable from it immutable and valid until the last
 * snapshot that contains the schema is destroyed. The host copies this
 * wrapper only; ownership of `data` and its reachable metadata is not
 * transferred.
 */
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

/**
 * Borrow the immutable schema array captured by a live catalog snapshot.
 *
 * Initialize `catalog_out` with `TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT`
 * before calling. On success, `schemas` and all metadata or callbacks
 * reachable through its entries remain DLL-owned and may be read only until
 * the corresponding snapshot is destroyed; callers must not retain any such
 * pointer beyond that lifetime.
 *
 * Example, after `snapshot` has been created successfully:
 * @code
 * turbo_flow_plugin_schema_catalog_v1_t catalog =
 *     TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
 * int rc = turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &catalog);
 * if (rc == SALTS_OK) {
 *   for (size_t i = 0u; i < catalog.schema_count; ++i) {
 *     const cmeta_data_desc *data = catalog.schemas[i].data;
 *     (void)data; // Borrowed only while snapshot remains live.
 *   }
 * }
 * turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
 * @endcode
 *
 * @param snapshot Live snapshot that owns the module leases and copied wrappers.
 * @param catalog_out Caller-owned initialized output structure.
 * @return `SALTS_OK` on success; `SALTS_EINVAL` if either argument is invalid,
 *         the snapshot is no longer live, or the output size/ABI version is unsupported.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_schema_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_schema_catalog_v1_t *catalog_out);

#ifdef __cplusplus
}
#endif
#endif
