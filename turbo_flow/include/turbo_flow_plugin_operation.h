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
 * The host copies the wrapper during registration; the original wrapper need
 * not outlive the registration call. The CMeta descriptor tree referenced by
 * `data` and every callback reachable from it remain DLL-owned. The plugin must
 * keep them immutable and valid for the registered module's lifetime. Snapshot
 * leases prevent module unload while snapshots still reference that metadata.
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
 * before calling. On success, `schemas` points to snapshot-owned wrapper
 * copies; their referenced CMeta metadata and callbacks remain DLL-owned.
 * Hold a live snapshot reference throughout the query and every use of these
 * borrowed pointers. A released reference must not be used again, and passing
 * a pointer to a destroyed snapshot is invalid, not a detectable error case.
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
 * @return `SALTS_OK` on success; `SALTS_EINVAL` for NULL arguments or an
 *         unsupported output size/ABI version. Non-NULL arguments must point
 *         to valid objects; the snapshot must be live.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_schema_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_schema_catalog_v1_t *catalog_out);

#ifdef __cplusplus
}
#endif
#endif
