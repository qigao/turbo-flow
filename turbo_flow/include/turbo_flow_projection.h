#ifndef TURBO_FLOW_PROJECTION_H
#define TURBO_FLOW_PROJECTION_H

#include "turbo_flow.h"
#include <cmeta/data.h>

typedef struct turbo_flow_projection_owner_s turbo_flow_projection_owner_t;
typedef struct turbo_flow_result_claim_s turbo_flow_result_claim_t;
/** 控制线程释放独立 context。成功销毁 ctx；失败须保留完整 ctx，允许重试。 */
typedef int (*turbo_flow_projection_context_release_fn)(void *ctx);
enum {
  TURBO_FLOW_PROJECTION_IMMUTABLE = 1u,
  TURBO_FLOW_PROJECTION_CROSS_THREAD = 2u,
  TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT = 4u,
  TURBO_FLOW_PROJECTION_ABI_MAJOR = 1u,
  TURBO_FLOW_PROJECTION_ABI_MINOR = 0u
};
typedef struct turbo_flow_projection_owner_config_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t flags; /**< 必须恰为 IMMUTABLE | CROSS_THREAD | INDEPENDENT_CONTEXT。 */
  size_t capacity; /**< 同时在途 reservation 和已绑定 projection 的总上限，必须非零。 */
  size_t max_result_bytes; /**< 每份 payload（含嵌套拥有型数据）的可信 provider 保留上界。 */
  size_t max_retained_bytes; /**< payload 收费总上限；每份固定收取 max_result_bytes。 */
  const turbo_flow_data_schema_t *schema;
  turbo_flow_projection_clone_fn clone; /**< 可为 NULL，此时消息 clone 返回 ENOTSUP。 */
  turbo_flow_destroy_fn destroy; /**< 必填；跨线程可靠完成 payload 释放，不可失败。 */
  void *ctx;
  turbo_flow_projection_context_release_fn release_context;
} turbo_flow_projection_owner_config_t;
#define TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT \
  {sizeof(turbo_flow_projection_owner_config_t), TURBO_FLOW_PROJECTION_ABI_MAJOR, \
   TURBO_FLOW_PROJECTION_ABI_MINOR, 0u, 0u, 0u, 0u, NULL, NULL, NULL, NULL, NULL}

typedef struct turbo_flow_projection_owner_snapshot_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  int accepting;
  size_t outstanding;
  size_t retained_bytes;
  size_t peak_outstanding;
  size_t peak_retained_bytes;
} turbo_flow_projection_owner_snapshot_t;
#define TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT \
  {sizeof(turbo_flow_projection_owner_snapshot_t), TURBO_FLOW_PROJECTION_ABI_MAJOR, \
   TURBO_FLOW_PROJECTION_ABI_MINOR, 0, 0u, 0u, 0u, 0u}

/**
 * 创建接受新结果的 Graph owner；config/out 必填，config 使用 CONFIG_INIT 初始化。
 * 校验完整 size、精确 ABI major/minor、合法 schema、非零额度以及 capacity 乘以
 * max_result_bytes / 私有 wrapper 大小不溢出。max_result_bytes 不得超过总上限。
 * 返回 OK 才接管 ctx；失败将 *out 置 NULL，ctx 仍由调用者拥有。
 * flags 不支持返回 ENOTSUP，参数/版本/算术非法返回 EINVAL，分配失败返回 ENOMEM。
 * config 与 schema wrapper 按值复制；schema 字符串须由独立 ctx 或静态模块保持有效。
 * payload 必须不可变，clone/destroy 必须可由不同 worker 并发调用，ctx 不得借用
 * 已销毁 generation/session。所有 callback 不可重入 owner 生命周期 API。
 * 额度是可信 provider 声明的上界，不是 malloc 拦截或 native 内存隔离。
 */
TURBO_FLOW_C_API int turbo_flow_projection_owner_create(
    const turbo_flow_projection_owner_config_t *config, turbo_flow_projection_owner_t **out);
/**
 * 幂等关闭 owner 新 bind/clone admission，返回 OK；NULL 返回 EINVAL。
 * 可与 worker 调用并发；已接受 reservation 可完成。新 reservation 返回 ECANCELED。
 */
TURBO_FLOW_C_API int turbo_flow_projection_owner_stop(turbo_flow_projection_owner_t *owner);
/**
 * 返回 owner 的同步只读快照；out 必须用 SNAPSHOT_INIT 初始化。
 * 返回 OK；NULL、size 不足或 ABI 不匹配返回 EINVAL 且不修改 out。
 * outstanding/retained_bytes 含正在 clone/destroy 的结果，payload destroy 返回后归还。
 * peak 字段记录生命周期峰值；查询不会推进状态，可与 worker 并发。
 */
TURBO_FLOW_C_API int turbo_flow_projection_owner_snapshot(
    turbo_flow_projection_owner_t *owner, turbo_flow_projection_owner_snapshot_t *out);
/**
 * 控制线程非阻塞尝试销毁。未 stop 或 outstanding 非零返回 EBUSY；NULL 返回 EINVAL。
 * STOPPED 且无结果时在锁外调用 release_context；其错误原样返回，保留 STOPPED owner
 * 和 context 以供重试。成功返回 OK，owner 指针随即失效。
 * 调用者必须禁止后续 API 进入，并确保所有 worker API 已返回，才可最终销毁；
 * 仅观察 outstanding 为零不能证明裸 owner 指针的外部使用者已经静止。
 * 所有分配、释放及 callback 均在额度锁外。worker 不得销毁 owner 或卸载 callback 模块。
 */
TURBO_FLOW_C_API int turbo_flow_projection_owner_destroy(turbo_flow_projection_owner_t *owner);
/**
 * 为已初始化 msg 绑定 owner 的独立非 NULL value。仅返回 OK 时转移 value 所有权；
 * 任何失败均保持原消息和值不变。原值在 bind 前由调用者拥有且尚不计入 owner 配额。
 * NULL 返回 EINVAL，已有 projection 返回 EBUSY，descriptor/schema 不匹配返回 EPROTO；
 * 额度满返回 ENOSPC，stop 后返回 ECANCELED，wrapper OOM 返回 ENOMEM。
 * 消息 clone 先预留额度，再分配 wrapper、调用 clone，最后发布。clone 失败返回其错误，
 * 并销毁非 NULL 临时结果；成功输出 NULL 或任何输出源值别名返回 EPROTO（不销毁源值）。
 * 不同消息可并发 clone/clear；同一源在 clone 期间必须存活且不被修改，目标须满足
 * 既有 clone/move 的空目标前提。move 只转移 wrapper，clear/cleanup 仅归还一次额度。
 * clear_projection 保留 descriptor 的原 borrowed/copy 契约；descriptor 不由 owner
 * 自动 pin。若借用 owner 内 descriptor，调用者仍须保持其生命周期或显式 copy。
 * 此 API 不改变 payload bytes，也不改变旧 bind_projection 的 borrowed context 语义。
 * 可编译 C++ 使用示例见 turbo_flow/tests/projection_header_cpp.cpp。
 */
TURBO_FLOW_C_API int turbo_flow_msg_bind_retained_projection(
    turbo_flow_msg_t *msg, turbo_flow_projection_owner_t *owner, void *value);

typedef struct turbo_flow_result_memory_requirements_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  size_t owner_bytes;
  size_t claim_bytes;
  size_t message_bytes;
  size_t peak_metadata_bytes;
  size_t payload_bound_bytes;
} turbo_flow_result_memory_requirements_t;

static inline void turbo_flow_result_memory_requirements_init(
    turbo_flow_result_memory_requirements_t *out) {
  if (!out) return;
  out->size = sizeof(*out);
  out->abi_major = TURBO_FLOW_PROJECTION_ABI_MAJOR;
  out->abi_minor = TURBO_FLOW_PROJECTION_ABI_MINOR;
  out->owner_bytes = 0u;
  out->claim_bytes = 0u;
  out->message_bytes = 0u;
  out->peak_metadata_bytes = 0u;
  out->payload_bound_bytes = 0u;
}

/** Compare two nonempty representable half-open ranges without dereferencing them. */
TURBO_FLOW_C_API int turbo_flow_value_require_disjoint(
    const void *borrowed, size_t borrowed_bytes, const void *candidate, size_t candidate_bytes);
/** Query exact Graph-private metadata and payload bounds without allocating or invoking callbacks.
 * `out` must have the exact initialized ABI1/0 layout. Valid-output failures clear all cost fields.
 */
TURBO_FLOW_C_API int turbo_flow_result_memory_requirements(
    size_t capacity, size_t max_result_bytes, turbo_flow_result_memory_requirements_t *out);
/** Match exact schema identity and bounded semantic/physical CMeta layout; schema_text is ignored. */
TURBO_FLOW_C_API int turbo_flow_data_schema_match(
    const turbo_flow_data_schema_t *expected_schema, const cmeta_data_desc *expected_data,
    const turbo_flow_data_schema_t *actual_schema, const cmeta_data_desc *actual_data);
/** Bind an owned projection with validated CMeta provenance; ownership transfers only on success. */
TURBO_FLOW_C_API int turbo_flow_msg_bind_typed_projection(
    turbo_flow_msg_t *msg, const turbo_flow_data_schema_t *schema,
    const cmeta_data_desc *data, void *value, turbo_flow_projection_clone_fn clone,
    turbo_flow_destroy_fn destroy, void *ctx);
/** Return borrowed validated provenance for a typed projection, or NULL for ordinary projections. */
TURBO_FLOW_C_API const cmeta_data_desc *turbo_flow_msg_projection_data(
    const turbo_flow_msg_t *msg);
/**
 * Reserve one unpublished result slot. Success transfers no value ownership; failure clears `out`
 * and leaves the message unchanged. Existing result returns EALREADY, full owner ENOSPC, stopped
 * owner EBUSY, unsupported untyped input ENOTSUP. From success until commit/abort, the caller must
 * not move, clone, clean up, or mutate `msg`.
 * A complete claim/commit/abort/cleanup example is built and run in
 * `turbo_flow/tests/test_flow_operation_result.c`.
 */
TURBO_FLOW_C_API int turbo_flow_msg_result_claim(
    turbo_flow_msg_t *msg, turbo_flow_projection_owner_t *owner,
    const cmeta_data_desc *data, turbo_flow_result_claim_t **out);
/** Atomically publish a non-NULL result disjoint from every known borrowed span.
 * Success clears both IO pointers and transfers value ownership; EPROTO retains both unchanged.
 */
TURBO_FLOW_C_API int turbo_flow_msg_result_commit(
    turbo_flow_result_claim_t **claim, void **value);
/** Abort an unpublished claim without destroying the caller-owned candidate; NULL is accepted. */
TURBO_FLOW_C_API void turbo_flow_msg_result_abort(turbo_flow_result_claim_t **claim);
/** Borrow the immutable committed result and optionally its schema/data provenance. */
TURBO_FLOW_C_API const void *turbo_flow_msg_result(
    const turbo_flow_msg_t *msg, const turbo_flow_data_schema_t **schema_out,
    const cmeta_data_desc **data_out);
/** Destroy only the committed result and retain projection/content provenance. */
TURBO_FLOW_C_API void turbo_flow_msg_clear_result(turbo_flow_msg_t *msg);

#endif
