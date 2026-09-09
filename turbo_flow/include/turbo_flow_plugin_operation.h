#ifndef TURBO_FLOW_PLUGIN_OPERATION_H
#define TURBO_FLOW_PLUGIN_OPERATION_H

#include "turbo_flow_export.h"
#include "turbo_flow_projection.h"
#include "turbo_flow_resolved_config.h"

#include <cmeta/data.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Shared plugin ABI includes every descriptor passed across the DLL boundary. */
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 3u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 0u

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
  {sizeof(turbo_flow_plugin_schema_v1_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR, \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, 0u, NULL}

typedef struct turbo_flow_plugin_schema_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_schema_v1_t *schemas;
  size_t schema_count;
} turbo_flow_plugin_schema_catalog_v1_t;

#define TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT                                      \
  {sizeof(turbo_flow_plugin_schema_catalog_v1_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR, \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, NULL, 0u}

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

typedef struct turbo_flow_plugin_result_domain_s turbo_flow_plugin_result_domain_t;
typedef struct turbo_flow_plugin_error_s turbo_flow_plugin_error_t;
enum {
  TURBO_FLOW_PLUGIN_CAP_OPERATION = UINT64_C(1) << 8,
  TURBO_FLOW_PLUGIN_OPERATION_SYNC = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_THREAD_SAFE = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_CANCEL_NONE = 0u,
  TURBO_FLOW_PLUGIN_OPERATION_EFFECT_RESULT = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_STEPS_CHARGED = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_PERMISSIONS = 32u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_SCHEMA_DEPTH = 16u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_SCHEMA_NODES = 256u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_STRUCT_FIELDS = 64u
};
typedef struct turbo_flow_plugin_operation_schema_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t schema_version;
  const cmeta_data_desc *data;
  const turbo_flow_data_schema_t *projection;
} turbo_flow_plugin_operation_schema_v3_t;
typedef struct turbo_flow_plugin_operation_limits_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t max_inflight;
  size_t max_input_bytes, max_result_bytes, max_retained_bytes;
  uint32_t max_steps;
  uint32_t deadline_ms;
} turbo_flow_plugin_operation_limits_v3_t;
typedef struct turbo_flow_plugin_operation_request_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_resolved_config_t *resolved;
  const char *operation_name;
  const char *resource_name;
  turbo_flow_plugin_operation_limits_v3_t limits;
} turbo_flow_plugin_operation_request_v3_t;
typedef struct turbo_flow_plugin_operation_input_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const void *value;
  const cmeta_data_desc *data;
  size_t bytes;
} turbo_flow_plugin_operation_input_v3_t;
typedef int (*turbo_flow_plugin_operation_charge_fn)(void *ctx, uint32_t steps);
typedef struct turbo_flow_plugin_operation_budget_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  void *ctx;
  turbo_flow_plugin_operation_charge_fn charge;
} turbo_flow_plugin_operation_budget_v3_t;
typedef struct turbo_flow_plugin_operation_error_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  int status;
  int64_t engine_status;
  uint32_t phase;
  char message[256];
} turbo_flow_plugin_operation_error_v3_t;
enum {
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_NONE = 0u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT = 2u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION = 3u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE = 4u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT = 5u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_RELEASE = 6u
};
/**
 * All four operation callbacks must preserve the Host-initialized error size and ABI 3.0.
 * On return the Host validates size, then version, before reading diagnostics or continuing.
 * A malformed header takes SALTS_EINVAL precedence over callback status; a fresh Host diagnostic
 * reports the current boundary phase without copying the invalid tail. Independently returned
 * outputs still follow the existing release/retry contract; borrowed aliases are never freed.
 */
typedef int (*turbo_flow_plugin_operation_preflight_fn)(
    void *factory_ctx, const turbo_flow_plugin_operation_request_v3_t *request,
    turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_result_context_create_fn)(
    void *factory_ctx, const turbo_flow_plugin_operation_request_v3_t *request, void **context_out,
    turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_session_create_fn)(
    void *factory_ctx, const turbo_flow_plugin_operation_request_v3_t *request,
    void *result_context, void **session_out, turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_execute_fn)(
    void *session, const turbo_flow_plugin_operation_input_v3_t *input,
    const turbo_flow_plugin_operation_budget_v3_t *budget, void **result_out,
    turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_release_fn)(void *ctx);
typedef struct turbo_flow_plugin_operation_vtable_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_plugin_operation_execute_fn execute;
  turbo_flow_projection_clone_fn clone_result;
  turbo_flow_destroy_fn destroy_result;
  turbo_flow_plugin_operation_release_fn release_session;
  turbo_flow_plugin_operation_release_fn release_result_context;
} turbo_flow_plugin_operation_vtable_v3_t;
typedef struct turbo_flow_plugin_operation_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const char *operation_name;
  uint32_t operation_version;
  turbo_flow_plugin_operation_schema_v3_t input, output;
  uint32_t execution, threading, cancellation, effects, guarantees;
  const char *const *permissions;
  size_t permission_count;
  turbo_flow_plugin_operation_limits_v3_t limits;
  size_t max_session_bytes, max_result_context_bytes;
  void *factory_ctx;
  turbo_flow_plugin_operation_preflight_fn preflight;
  turbo_flow_plugin_operation_result_context_create_fn create_result_context;
  turbo_flow_plugin_operation_session_create_fn create_session;
  turbo_flow_plugin_operation_vtable_v3_t vtable;
} turbo_flow_plugin_operation_v3_t;
typedef int (*turbo_flow_plugin_add_operation_fn)(
    void *ctx, const turbo_flow_plugin_operation_v3_t *operation);
typedef struct turbo_flow_plugin_operation_catalog_entry_v3_s {
  const char *plugin_id;
  turbo_flow_plugin_operation_v3_t operation;
} turbo_flow_plugin_operation_catalog_entry_v3_t;
typedef struct turbo_flow_plugin_operation_catalog_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_operation_catalog_entry_v3_t *entries;
  size_t count;
} turbo_flow_plugin_operation_catalog_v3_t;
/** Borrow immutable copied wrappers from a live snapshot; exact initialized ABI3 output required.
 * Returns OK or EINVAL. References remain valid only while caller retains the snapshot.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_operation_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_operation_catalog_v3_t *catalog_out);

static inline void
turbo_flow_plugin_operation_schema_v3_init(turbo_flow_plugin_operation_schema_v3_t *out) {
  turbo_flow_plugin_operation_schema_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_limits_v3_init(turbo_flow_plugin_operation_limits_v3_t *out) {
  turbo_flow_plugin_operation_limits_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_input_v3_init(turbo_flow_plugin_operation_input_v3_t *out) {
  turbo_flow_plugin_operation_input_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_budget_v3_init(turbo_flow_plugin_operation_budget_v3_t *out) {
  turbo_flow_plugin_operation_budget_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_error_v3_init(turbo_flow_plugin_operation_error_v3_t *out) {
  turbo_flow_plugin_operation_error_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_vtable_v3_init(turbo_flow_plugin_operation_vtable_v3_t *out) {
  turbo_flow_plugin_operation_vtable_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_request_v3_init(turbo_flow_plugin_operation_request_v3_t *out) {
  turbo_flow_plugin_operation_request_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  turbo_flow_plugin_operation_limits_v3_init(&initial.limits);
  *out = initial;
}

static inline void turbo_flow_plugin_operation_v3_init(turbo_flow_plugin_operation_v3_t *out) {
  turbo_flow_plugin_operation_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  turbo_flow_plugin_operation_schema_v3_init(&initial.input);
  turbo_flow_plugin_operation_schema_v3_init(&initial.output);
  turbo_flow_plugin_operation_limits_v3_init(&initial.limits);
  turbo_flow_plugin_operation_vtable_v3_init(&initial.vtable);
  *out = initial;
}

static inline void
turbo_flow_plugin_operation_catalog_v3_init(turbo_flow_plugin_operation_catalog_v3_t *out) {
  turbo_flow_plugin_operation_catalog_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

/**
 * 控制线程创建caller-owned结果域；capacity为1..1024，预留全部entry后retain同一live snapshot。
 * 失败清空domain_out且不消耗snapshot；返回EINVAL、ENOMEM或精确的容器/retain错误。
 * 仅允许同snapshot的generation在所有preflight成功后attach一次；域不得跨generation复用。
 * 使用示例和完整销毁顺序见tests/test_flow_plugin_operation_runtime.c。
 */
TURBO_FLOW_C_API int turbo_flow_plugin_result_domain_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot, size_t capacity,
    turbo_flow_plugin_result_domain_t **domain_out, turbo_flow_plugin_error_t *error);
/**
 * 控制线程退休域。ATTACHED返回EBUSY且不stop；否则stop全部owner，全部drain后逆序释放。
 * caller必须禁止新API并join worker；outstanding0不能替代join。EBUSY或release错误保留
 * 域、未释放context和snapshot，调用方须重试同一对象；OK才使domain指针失效。
 * generation_create失败也必须显式退休域，不能因cleanup_out为空而跳过。
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_result_domain_destroy(turbo_flow_plugin_result_domain_t *domain,
                                        turbo_flow_plugin_error_t *error);
typedef struct turbo_flow_plugin_result_domain_snapshot_v3_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t state;
  size_t capacity, owner_count, outstanding, retained_bytes;
  int last_cleanup_status;
} turbo_flow_plugin_result_domain_snapshot_v3_t;
enum {
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY = 0u,
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED = 1u,
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED = 2u,
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_RETIRING = 3u
};
/** Read control-thread domain state and aggregated worker-safe owner counters, without cleanup.
 * Exact initialized ABI3 output required; returns OK, EINVAL, or the exact owner-query error.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_result_domain_snapshot(const turbo_flow_plugin_result_domain_t *domain,
                                         turbo_flow_plugin_result_domain_snapshot_v3_t *out);
static inline void turbo_flow_plugin_result_domain_snapshot_v3_init(
    turbo_flow_plugin_result_domain_snapshot_v3_t *out) {
  turbo_flow_plugin_result_domain_snapshot_v3_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  initial.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  *out = initial;
}

#ifdef __cplusplus
}
#endif
#endif
