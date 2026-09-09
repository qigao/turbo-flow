#ifndef TURBO_FLOW_PLUGIN_OPERATION_H
#define TURBO_FLOW_PLUGIN_OPERATION_H

#include "turbo_flow_export.h"
#include "turbo_flow_projection.h"

#include <cmeta/data.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PLUGIN_SCHEMA_ABI_MINOR 4u

typedef struct turbo_flow_plugin_catalog_snapshot_s turbo_flow_plugin_catalog_snapshot_t;

/**
 * 控制线程从 live snapshot 创建独立结果 owner。可信 host binding 必须保证 snapshot
 * 包含 config 中 callback、metadata 所属模块及依赖；本接口不认证其来源。
 * config/schema wrapper 按值复制，字符串由独立 context 或模块保持有效。
 * 成功才转移 config->ctx；失败置 *out 为 NULL，平衡临时 snapshot 引用且不释放原 ctx。
 * 返回 Graph owner_create 的参数/ABI/flags/额度/OOM 错误，或 snapshot retain 错误。
 * release_context 成功后才释放 snapshot；失败保留 owner/context/snapshot 供控制线程重试。
 * worker 仅 clone/clear；控制线程须 stop 并等待全部 worker API 返回后 destroy owner。
 * 独立结果可越过 generation；依赖 session 的结果不符合本接口准入契约。
 * @param snapshot 持有 callback/metadata 模块及依赖的 live 引用，调用期间保持存活。
 * @param config 以 OWNER_CONFIG_INIT 初始化的完整 retained 契约。
 * @param out 接收 owner 的非 NULL 输出。
 * @code
 * int rc = turbo_flow_plugin_projection_owner_create(snapshot, &config, &owner);
 * // rc == SALTS_OK 时 owner 接管 ctx；按 projection.h 协议 drain/销毁。
 * @endcode
 */
TURBO_FLOW_C_API int turbo_flow_plugin_projection_owner_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    const turbo_flow_projection_owner_config_t *config,
    turbo_flow_projection_owner_t **out);

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
