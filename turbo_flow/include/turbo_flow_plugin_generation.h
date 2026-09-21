#ifndef TURBO_FLOW_PLUGIN_GENERATION_H
#define TURBO_FLOW_PLUGIN_GENERATION_H

#include "turbo_flow_plugin.h"

#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t turbo_flow_plugin_product_owner_flags_t;
enum {
  TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD = UINT64_C(1) << 0,
  TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE = UINT64_C(1) << 1,
  TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL = UINT64_C(1) << 2
};

typedef int (*turbo_flow_plugin_product_owner_quiesce_fn)(void *ctx, uint64_t timeout_ms);
typedef int (*turbo_flow_plugin_product_owner_drain_fn)(void *ctx, uint64_t timeout_ms);
typedef int (*turbo_flow_plugin_product_owner_shutdown_fn)(void *ctx);
typedef void (*turbo_flow_plugin_product_owner_destroy_fn)(void *ctx);
typedef int (*turbo_flow_plugin_product_owner_poll_fn)(void *ctx, uint32_t timeout_ms);

/**
 * One plugin-owned Product instance. The descriptor is copied by the host.
 * The plugin that creates ctx must destroy it through this vtable. Exactly one threading flag is
 * required. CONTROL_THREAD confines every lifecycle callback to the caller-serialized Gateway
 * control thread; THREAD_SAFE declares that the opaque owner tolerates calls from any host thread.
 */
typedef struct turbo_flow_plugin_product_owner_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_plugin_product_owner_flags_t flags;
  void *ctx;
  turbo_flow_plugin_product_owner_quiesce_fn quiesce;
  turbo_flow_plugin_product_owner_drain_fn drain;
  turbo_flow_plugin_product_owner_shutdown_fn shutdown;
  turbo_flow_plugin_product_owner_destroy_fn destroy;
  turbo_flow_plugin_product_owner_poll_fn poll;
} turbo_flow_plugin_product_owner_v1_t;

#define TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT                                                    \
  {sizeof(turbo_flow_plugin_product_owner_v1_t),                                                   \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * Publish a complete local owner descriptor into an exact current shared plugin ABI destination.
 *
 * A materializer must preserve the host-seeded size/version and use this helper instead of
 * assigning the complete structure. Both source and destination must have the exact size and
 * version; incompatible headers fail with SALTS_EINVAL without modifying any output bytes.
 */
static inline int
turbo_flow_plugin_product_owner_publish(turbo_flow_plugin_product_owner_v1_t *owner_out,
                                        const turbo_flow_plugin_product_owner_v1_t *owner) {
  if (!owner_out || !owner) return SALTS_EINVAL;
  if (owner_out->size != sizeof(*owner_out) ||
      owner_out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      owner_out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      owner->size != sizeof(*owner) || owner->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      owner->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  memcpy(owner_out, owner, sizeof(*owner_out));
  owner_out->size = sizeof(*owner_out);
  return SALTS_OK;
}

typedef int (*turbo_flow_plugin_product_preflight_fn)(void *ctx,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      const char *name,
                                                      turbo_flow_config_error_t *error);
typedef int (*turbo_flow_plugin_product_materialize_fn)(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error);

/**
 * A DLL-owned transactional adapter factory descriptor. preflight must validate without external
 * side effects. A successful materialize transfers exactly one owner; after failure, the plugin
 * remains responsible for any partial state and must leave owner_out empty. materialize treats the
 * initial owner_out size/version as an exact ABI contract and publishes through
 * turbo_flow_plugin_product_owner_publish(); a plugin that may publish EXTERNAL_POLL also declares
 * TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL in its root API.
 */
struct turbo_flow_plugin_transactional_adapter_provider_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const char *kind;
  void *ctx;
  turbo_flow_plugin_product_preflight_fn preflight;
  turbo_flow_plugin_product_materialize_fn materialize;
};

#define TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT                                   \
  {sizeof(turbo_flow_plugin_transactional_adapter_provider_v1_t),                                  \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * A DLL-owned transactional resource factory descriptor. Its transaction and ownership rules are
 * identical to the adapter provider contract.
 */
struct turbo_flow_plugin_transactional_resource_provider_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const char *kind;
  void *ctx;
  turbo_flow_plugin_product_preflight_fn preflight;
  turbo_flow_plugin_product_materialize_fn materialize;
};

#define TURBO_FLOW_PLUGIN_TRANSACTIONAL_RESOURCE_PROVIDER_V1_INIT                                  \
  {sizeof(turbo_flow_plugin_transactional_resource_provider_v1_t),                                 \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/** Immutable transactional Product provider arrays borrowed from a catalog snapshot. */
typedef struct turbo_flow_plugin_transactional_product_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_transactional_adapter_provider_v1_t *adapter_providers;
  size_t adapter_provider_count;
  const turbo_flow_plugin_transactional_resource_provider_v1_t *resource_providers;
  size_t resource_provider_count;
} turbo_flow_plugin_transactional_product_catalog_v1_t;

#define TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT                                    \
  {sizeof(turbo_flow_plugin_transactional_product_catalog_v1_t),                                   \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u}

#define TURBO_FLOW_PLUGIN_GENERATION_MAX_OWNERS 65536u

typedef struct turbo_flow_plugin_generation_s turbo_flow_plugin_generation_t;

typedef enum turbo_flow_plugin_generation_state_e {
  TURBO_FLOW_PLUGIN_GENERATION_INVALID = 0,
  TURBO_FLOW_PLUGIN_GENERATION_COMPILED,
  TURBO_FLOW_PLUGIN_GENERATION_ACTIVE,
  TURBO_FLOW_PLUGIN_GENERATION_QUIESCED,
  TURBO_FLOW_PLUGIN_GENERATION_STOPPED,
  TURBO_FLOW_PLUGIN_GENERATION_DRAINED,
  TURBO_FLOW_PLUGIN_GENERATION_SHUTDOWN,
  TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP = 7
} turbo_flow_plugin_generation_state_t;

typedef struct turbo_flow_plugin_generation_config_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  size_t owner_capacity;
  size_t operation_memory_budget_bytes;
} turbo_flow_plugin_generation_config_t;

enum { TURBO_FLOW_PLUGIN_OPERATION_MEMORY_BUDGET_DEFAULT = 67108864u };

#define TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_plugin_generation_config_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,             \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, 256u, TURBO_FLOW_PLUGIN_OPERATION_MEMORY_BUDGET_DEFAULT}

/**
 * Fill a caller-owned immutable transactional Product catalog view.
 * @param snapshot Live snapshot that retains all represented DLLs.
 * @param catalog_out Initialized size/versioned output borrowing arrays from snapshot.
 * @return SALTS_OK, or SALTS_EINVAL for an invalid snapshot or output descriptor.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_transactional_product_catalog_v1_t *catalog_out);

/**
 * Consume one parsed Graph only after bounded preflight succeeds, then materialize and compile it.
 * `*flow_io` remains caller-owned on preflight failure and becomes NULL before the first factory
 * side effect. The generation owns the Graph and one retained catalog snapshot on success. Create,
 * lease mutation, and destroy are caller-serialized on the Gateway control thread.
 * @param snapshot Live immutable provider snapshot; retained by the generation.
 * @param resolved Immutable resolved configuration borrowed for the duration of this call.
 * @param flow_io In/out parsed Graph; moved only after every preflight succeeds.
 * @param config Initialized size/versioned capacity configuration.
 * @param result_domain Caller-owned READY domain with the same snapshot; required for operations.
 * @param generation_out Receives the owned generation on success and NULL on failure.
 * @param cleanup_out Required distinct output retaining failed retirement. Retry recoverable
 * busy/lifecycle failures. An invalid successful Product publication without a verifiable cleanup
 * contract permanently returns EINVAL from destroy and pins the Graph/domain/modules until process
 * exit; no force-unload or repair API exists. A NULL output does not release the caller-owned
 * domain.
 * @param error Initialized structured error output.
 * @return SALTS_OK, or the exact validation, capacity, provider, allocation, or compile error.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_generation_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot, const turbo_flow_resolved_config_t *resolved,
    turbo_flow_t **flow_io, const turbo_flow_plugin_generation_config_t *config,
    turbo_flow_plugin_result_domain_t *result_domain,
    turbo_flow_plugin_generation_t **generation_out, turbo_flow_plugin_generation_t **cleanup_out,
    turbo_flow_config_error_t *error);

/** Read the most recently completed failure for the resolved binding index; never retries cleanup.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_operation_error(const turbo_flow_plugin_generation_t *generation,
                                             size_t binding_index,
                                             turbo_flow_plugin_operation_error_v3_t *out);
/** Read the most recent retirement failure separately from the original create error. */
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_cleanup_error(const turbo_flow_plugin_generation_t *generation,
                                           turbo_flow_config_error_t *out);

/** Borrowed mutable Graph, or NULL for FAILED_CLEANUP; generation remains its destruction owner. */
TURBO_FLOW_C_API turbo_flow_t *
turbo_flow_plugin_generation_flow(turbo_flow_plugin_generation_t *generation);
TURBO_FLOW_C_API turbo_flow_plugin_generation_state_t
turbo_flow_plugin_generation_state(const turbo_flow_plugin_generation_t *generation);
TURBO_FLOW_C_API size_t
turbo_flow_plugin_generation_owner_count(const turbo_flow_plugin_generation_t *generation);

/** Return the number of schema-level materializer bindings compiled into this generation. */
TURBO_FLOW_C_API size_t
turbo_flow_plugin_generation_materializer_count(
    const turbo_flow_plugin_generation_t *generation);

/**
 * Decode one canonical message payload through an already-compiled materializer binding.
 *
 * binding_index is the resolved materializer_bindings array index. No catalog, symbol, provider
 * name, or schema-name lookup occurs on this data path. The message content descriptor must
 * exactly match the compiled schema/version/encoding before the callback is invoked.
 *
 * On success, one caller-buffer native value is attached as a message-owned exact CMeta typed
 * projection while the original payload bytes remain the source of truth. Projection clone and
 * destroy retain/release a generation-local materialization lease, so generation retirement
 * returns SALTS_EBUSY until every materialized projection is gone.
 *
 * @return SALTS_OK, SALTS_EINVAL for invalid arguments/content, SALTS_ENOENT for an invalid
 * binding index, SALTS_EALREADY when a projection already exists, SALTS_ENOSPC for encoded-byte
 * or configured concurrent-projection capacity exhaustion, SALTS_EBUSY after materialization
 * admission has closed, SALTS_EPROTO for schema mismatch, or the exact provider callback error.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_generation_materialize_at(
    turbo_flow_plugin_generation_t *generation, size_t binding_index,
    turbo_flow_msg_t *message, turbo_flow_config_error_t *error);

/**
 * Drive one external-poll round while the Graph is started.
 *
 * On a successful round each eligible owner is invoked exactly once. One rotating owner receives
 * the total `timeout_ms`; every remaining owner receives zero, so the round has one bounded
 * blocking budget. Lifecycle-only owners are skipped. The first callback error stops the round.
 * Calls are serialized on the Gateway control thread and must not re-enter generation poll,
 * lifecycle, or lease mutation from an owner callback.
 * @return SALTS_OK, SALTS_EBUSY outside the active state, or the exact owner callback error.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_generation_poll(turbo_flow_plugin_generation_t *generation,
                                                       uint32_t timeout_ms,
                                                       turbo_flow_config_error_t *error);

/**
 * Acquire one control-thread-serialized lease before publishing a run, claim, or callback that can
 * outlive its initiating control operation. The lease must be released after its last callback.
 * @return SALTS_OK, SALTS_EINVAL for no generation, or SALTS_ENOSPC on counter overflow.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_lease_acquire(turbo_flow_plugin_generation_t *generation);
/**
 * Release one control-thread-serialized active lease.
 * @return SALTS_OK, or SALTS_EINVAL for no generation or lease underflow.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_lease_release(turbo_flow_plugin_generation_t *generation);

/**
 * Retire owners and the Graph in retryable reverse lifecycle order, then release the DLL snapshot.
 * Active leases return SALTS_EBUSY without invoking lifecycle callbacks. Timeout or lifecycle
 * failure leaves an inspectable generation whose completed transitions are not repeated. On
 * SALTS_OK the generation is freed and must not be used again.
 * Invalid Product publications with no verifiable cleanup contract return SALTS_EINVAL without
 * callbacks or releasing the Graph/domain/module pins. Repeating destroy cannot repair that state.
 * @param generation Owned generation, called from the same serialized control thread.
 * @param timeout_ms Bounded timeout forwarded independently to each owner callback.
 * @param error Initialized structured error output.
 * @return SALTS_OK, SALTS_EBUSY, SALTS_ETIMEDOUT, or the exact owner/Graph lifecycle error.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_destroy(turbo_flow_plugin_generation_t *generation,
                                     uint64_t timeout_ms, turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PLUGIN_GENERATION_H */
