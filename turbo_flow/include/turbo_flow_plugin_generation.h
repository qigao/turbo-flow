#ifndef TURBO_FLOW_PLUGIN_GENERATION_H
#define TURBO_FLOW_PLUGIN_GENERATION_H

#include "turbo_flow_plugin.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t turbo_flow_plugin_product_owner_flags_t;
enum {
  TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD = UINT64_C(1) << 0,
  TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE = UINT64_C(1) << 1
};

typedef int (*turbo_flow_plugin_product_owner_quiesce_fn)(void *ctx, uint64_t timeout_ms);
typedef int (*turbo_flow_plugin_product_owner_drain_fn)(void *ctx, uint64_t timeout_ms);
typedef int (*turbo_flow_plugin_product_owner_shutdown_fn)(void *ctx);
typedef void (*turbo_flow_plugin_product_owner_destroy_fn)(void *ctx);

/**
 * One plugin-owned Product instance. The descriptor is copied by the host.
 * The plugin that creates ctx must destroy it through this vtable.
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
   NULL}

typedef int (*turbo_flow_plugin_product_preflight_fn)(void *ctx,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      const char *name,
                                                      turbo_flow_config_error_t *error);
typedef int (*turbo_flow_plugin_product_materialize_fn)(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error);

/** A DLL-owned transactional adapter factory descriptor. */
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

/** A DLL-owned transactional resource factory descriptor. */
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
  TURBO_FLOW_PLUGIN_GENERATION_SHUTDOWN
} turbo_flow_plugin_generation_state_t;

typedef struct turbo_flow_plugin_generation_config_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  size_t owner_capacity;
} turbo_flow_plugin_generation_config_t;

#define TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_plugin_generation_config_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,             \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, 256u}

/** Fill a caller-owned immutable transactional Product catalog view. */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_transactional_product_catalog_v1_t *catalog_out);

/**
 * Consume one parsed Graph only after bounded preflight succeeds, then materialize and compile it.
 * `*flow_io` remains caller-owned on preflight failure and becomes NULL before the first factory
 * side effect. The generation owns the Graph and one retained catalog snapshot on success.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_generation_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot, const turbo_flow_resolved_config_t *resolved,
    turbo_flow_t **flow_io, const turbo_flow_plugin_generation_config_t *config,
    turbo_flow_plugin_generation_t **generation_out, turbo_flow_config_error_t *error);

/** Borrowed mutable Graph; the generation remains its sole destruction owner. */
TURBO_FLOW_C_API turbo_flow_t *
turbo_flow_plugin_generation_flow(turbo_flow_plugin_generation_t *generation);
TURBO_FLOW_C_API turbo_flow_plugin_generation_state_t
turbo_flow_plugin_generation_state(const turbo_flow_plugin_generation_t *generation);
TURBO_FLOW_C_API size_t
turbo_flow_plugin_generation_owner_count(const turbo_flow_plugin_generation_t *generation);

TURBO_FLOW_C_API int
turbo_flow_plugin_generation_lease_acquire(turbo_flow_plugin_generation_t *generation);
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_lease_release(turbo_flow_plugin_generation_t *generation);

/**
 * Retire owners and the Graph in retryable reverse lifecycle order, then release the DLL snapshot.
 * On SALTS_OK the generation is freed and must not be used again.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_generation_destroy(turbo_flow_plugin_generation_t *generation,
                                     uint64_t timeout_ms, turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PLUGIN_GENERATION_H */
