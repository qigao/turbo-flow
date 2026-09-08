#ifndef TURBO_FLOW_PLUGIN_H
#define TURBO_FLOW_PLUGIN_H

#include "turbo_flow_product.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 1u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 0u
#define TURBO_FLOW_PLUGIN_EXPORT_SYMBOL "turbo_flow_plugin_get_api"

#define TURBO_FLOW_PLUGIN_ID_MAX 127u
#define TURBO_FLOW_PLUGIN_VERSION_MAX 63u
#define TURBO_FLOW_PLUGIN_PATH_MAX 511u
#define TURBO_FLOW_PLUGIN_ERROR_MESSAGE_MAX 255u
#define TURBO_FLOW_PLUGIN_HOST_MAX_MODULES 1024u
#define TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS 65536u

#if defined(_WIN32) && defined(TURBO_FLOW_PLUGIN_BUILD)
  #define TURBO_FLOW_PLUGIN_ENTRY __declspec(dllexport)
#elif !defined(_WIN32) && defined(TURBO_FLOW_PLUGIN_BUILD) && defined(__GNUC__) && __GNUC__ >= 4
  #define TURBO_FLOW_PLUGIN_ENTRY __attribute__((visibility("default")))
#else
  #define TURBO_FLOW_PLUGIN_ENTRY
#endif

typedef struct turbo_flow_plugin_host_s turbo_flow_plugin_host_t;
typedef struct turbo_flow_plugin_catalog_snapshot_s turbo_flow_plugin_catalog_snapshot_t;

typedef uint64_t turbo_flow_plugin_capabilities_t;
enum {
  TURBO_FLOW_PLUGIN_CAP_PRODUCT_ADAPTER = UINT64_C(1) << 0,
  TURBO_FLOW_PLUGIN_CAP_PRODUCT_RESOURCE = UINT64_C(1) << 1
};

typedef enum turbo_flow_plugin_error_stage_e {
  TURBO_FLOW_PLUGIN_STAGE_NONE = 0,
  TURBO_FLOW_PLUGIN_STAGE_ARGUMENT,
  TURBO_FLOW_PLUGIN_STAGE_STATE,
  TURBO_FLOW_PLUGIN_STAGE_CAPACITY,
  TURBO_FLOW_PLUGIN_STAGE_OPEN,
  TURBO_FLOW_PLUGIN_STAGE_SYMBOL,
  TURBO_FLOW_PLUGIN_STAGE_API,
  TURBO_FLOW_PLUGIN_STAGE_IDENTITY,
  TURBO_FLOW_PLUGIN_STAGE_LOAD,
  TURBO_FLOW_PLUGIN_STAGE_REGISTRATION,
  TURBO_FLOW_PLUGIN_STAGE_COMMIT,
  TURBO_FLOW_PLUGIN_STAGE_LEASE,
  TURBO_FLOW_PLUGIN_STAGE_QUIESCE,
  TURBO_FLOW_PLUGIN_STAGE_SHUTDOWN
} turbo_flow_plugin_error_stage_t;

typedef enum turbo_flow_plugin_lifecycle_event_e {
  TURBO_FLOW_PLUGIN_LIFECYCLE_LOAD = 1,
  TURBO_FLOW_PLUGIN_LIFECYCLE_REGISTER,
  TURBO_FLOW_PLUGIN_LIFECYCLE_COMMIT,
  TURBO_FLOW_PLUGIN_LIFECYCLE_ROLLBACK,
  TURBO_FLOW_PLUGIN_LIFECYCLE_QUIESCE,
  TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN,
  TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY,
  TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD
} turbo_flow_plugin_lifecycle_event_t;

typedef void (*turbo_flow_plugin_lifecycle_observer_fn)(void *ctx,
                                                        turbo_flow_plugin_lifecycle_event_t event,
                                                        const char *plugin_id, int status);

typedef struct turbo_flow_plugin_error_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  int status;
  turbo_flow_plugin_error_stage_t stage;
  char plugin_id[TURBO_FLOW_PLUGIN_ID_MAX + 1u];
  char path[TURBO_FLOW_PLUGIN_PATH_MAX + 1u];
  char message[TURBO_FLOW_PLUGIN_ERROR_MESSAGE_MAX + 1u];
} turbo_flow_plugin_error_t;

#define TURBO_FLOW_PLUGIN_ERROR_INIT                                                               \
  {sizeof(turbo_flow_plugin_error_t),                                                              \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   SALTS_OK,                                                                                       \
   TURBO_FLOW_PLUGIN_STAGE_NONE,                                                                   \
   {0},                                                                                            \
   {0},                                                                                            \
   {0}}

typedef struct turbo_flow_plugin_host_config_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  size_t module_capacity;
  size_t adapter_provider_capacity;
  size_t resource_provider_capacity;
  turbo_flow_plugin_lifecycle_observer_fn lifecycle_observer;
  void *lifecycle_observer_ctx;
} turbo_flow_plugin_host_config_t;

#define TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT                                                         \
  {sizeof(turbo_flow_plugin_host_config_t),                                                        \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   16u,                                                                                            \
   64u,                                                                                            \
   64u,                                                                                            \
   NULL,                                                                                           \
   NULL}

typedef void *(*turbo_flow_plugin_host_allocate_fn)(void *ctx, size_t size);
typedef void (*turbo_flow_plugin_host_deallocate_fn)(void *ctx, void *memory);

/** Host services remain valid until the plugin's destroy callback returns. */
typedef struct turbo_flow_plugin_host_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  void *ctx;
  turbo_flow_plugin_host_allocate_fn allocate;
  turbo_flow_plugin_host_deallocate_fn deallocate;
} turbo_flow_plugin_host_v1_t;

/** Size/version wrapper for one DLL-owned Product adapter provider descriptor. */
typedef struct turbo_flow_plugin_product_adapter_provider_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_product_adapter_provider_t provider;
} turbo_flow_plugin_product_adapter_provider_v1_t;

#define TURBO_FLOW_PLUGIN_PRODUCT_ADAPTER_PROVIDER_V1_INIT                                         \
  {sizeof(turbo_flow_plugin_product_adapter_provider_v1_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,   \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT}

/** Size/version wrapper for one DLL-owned Product resource provider descriptor. */
typedef struct turbo_flow_plugin_product_resource_provider_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_product_resource_provider_t provider;
} turbo_flow_plugin_product_resource_provider_v1_t;

#define TURBO_FLOW_PLUGIN_PRODUCT_RESOURCE_PROVIDER_V1_INIT                                        \
  {sizeof(turbo_flow_plugin_product_resource_provider_v1_t), TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,  \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, TURBO_FLOW_PRODUCT_RESOURCE_PROVIDER_INIT}

typedef int (*turbo_flow_plugin_add_adapter_provider_fn)(
    void *ctx, const turbo_flow_plugin_product_adapter_provider_v1_t *provider);
typedef int (*turbo_flow_plugin_add_resource_provider_fn)(
    void *ctx, const turbo_flow_plugin_product_resource_provider_v1_t *provider);

/** Registration is valid only during one root register_capabilities callback. */
typedef struct turbo_flow_plugin_registration_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  void *ctx;
  turbo_flow_plugin_add_adapter_provider_fn add_adapter_provider;
  turbo_flow_plugin_add_resource_provider_fn add_resource_provider;
} turbo_flow_plugin_registration_v1_t;

typedef int (*turbo_flow_plugin_load_fn)(const turbo_flow_plugin_host_v1_t *host,
                                         void **plugin_out);
typedef int (*turbo_flow_plugin_register_capabilities_fn)(
    void *plugin, const turbo_flow_plugin_registration_v1_t *registration);
typedef int (*turbo_flow_plugin_quiesce_fn)(void *plugin, uint64_t timeout_ms);
typedef int (*turbo_flow_plugin_shutdown_fn)(void *plugin);
typedef void (*turbo_flow_plugin_destroy_fn)(void *plugin);

/**
 * Stable root vtable returned by the one canonical DLL export.
 *
 * plugin_id, plugin_version, callback code, provider descriptors, and every
 * provider context remain owned by the DLL until destroy returns. A plugin
 * instance is always destroyed on the same DLL/CRT side that created it.
 */
typedef struct turbo_flow_plugin_api_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const char *plugin_id;
  const char *plugin_version;
  turbo_flow_plugin_capabilities_t capabilities;
  turbo_flow_plugin_load_fn load;
  turbo_flow_plugin_register_capabilities_fn register_capabilities;
  turbo_flow_plugin_quiesce_fn quiesce;
  turbo_flow_plugin_shutdown_fn shutdown;
  turbo_flow_plugin_destroy_fn destroy;
} turbo_flow_plugin_api_v1_t;

typedef const turbo_flow_plugin_api_v1_t *(*turbo_flow_plugin_get_api_fn)(void);

/** Implemented by each plugin DLL, not by PluginHost. */
TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void);

/**
 * Create one bounded, control-thread-confined host.
 * @param config Caller-owned capacity and observer configuration, copied on success.
 * @param host_out Receives the owned opaque host; set to NULL on failure.
 * @param error Caller-owned initialized structured error output.
 * @return SALTS_OK, SALTS_EINVAL, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_host_create(const turbo_flow_plugin_host_config_t *config,
                                                   turbo_flow_plugin_host_t **host_out,
                                                   turbo_flow_plugin_error_t *error);

/**
 * Load one explicit UTF-8 DLL path and atomically commit all of its providers.
 * @param host Host owner, confined to its control thread.
 * @param path Explicit path; no search candidate or fallback is attempted.
 * @param error Caller-owned initialized structured error output.
 * @return SALTS_OK or the exact open/symbol/ABI/callback/registry error.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_host_load(turbo_flow_plugin_host_t *host, const char *path,
                                                 turbo_flow_plugin_error_t *error);

TURBO_FLOW_C_API size_t turbo_flow_plugin_host_module_count(const turbo_flow_plugin_host_t *host);
TURBO_FLOW_C_API size_t
turbo_flow_plugin_host_adapter_provider_count(const turbo_flow_plugin_host_t *host);
TURBO_FLOW_C_API size_t
turbo_flow_plugin_host_resource_provider_count(const turbo_flow_plugin_host_t *host);

/**
 * Copy an immutable Product catalog and retain every represented DLL.
 * Release only after the owning Graph generation has drained all callbacks.
 * @param host Host owner, confined to its control thread.
 * @param snapshot_out Receives an owned snapshot; set to NULL on failure.
 * @param error Caller-owned initialized structured error output.
 * @return SALTS_OK, SALTS_EBUSY, SALTS_ENOSPC, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int
turbo_flow_plugin_catalog_snapshot_create(turbo_flow_plugin_host_t *host,
                                          turbo_flow_plugin_catalog_snapshot_t **snapshot_out,
                                          turbo_flow_plugin_error_t *error);
/**
 * Fill a caller-owned Product registry with immutable arrays borrowed from a snapshot.
 * @return SALTS_OK or SALTS_EINVAL for an invalid snapshot/output structure.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_product_registry(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_product_provider_registry_t *registry_out);
/** Release a snapshot and all module leases; accepts NULL. */
TURBO_FLOW_C_API void
turbo_flow_plugin_catalog_snapshot_destroy(turbo_flow_plugin_catalog_snapshot_t *snapshot);

/**
 * Quiesce, shut down, destroy, and unload all modules in reverse load order.
 * SALTS_EBUSY leaves the host untouched while any catalog snapshot is alive.
 * A quiesce/shutdown error leaves explicit retryable lifecycle state in host.
 * On SALTS_OK the host itself has been freed and must not be used again.
 * @param host Host to shut down and free.
 * @param quiesce_timeout_ms Bounded timeout forwarded to each plugin in reverse order.
 * @param error Caller-owned initialized structured error output.
 * @return SALTS_OK, SALTS_EBUSY, or the exact lifecycle callback error.
 */
TURBO_FLOW_C_API int turbo_flow_plugin_host_destroy(turbo_flow_plugin_host_t *host,
                                                    uint64_t quiesce_timeout_ms,
                                                    turbo_flow_plugin_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PLUGIN_H */
