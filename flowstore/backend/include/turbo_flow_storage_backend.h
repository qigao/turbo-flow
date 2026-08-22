#ifndef TURBO_FLOW_STORAGE_BACKEND_H
#define TURBO_FLOW_STORAGE_BACKEND_H

#include "turbo_flow_export.h"
#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION 1u
#define TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR 1u
#define TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR 0u

typedef struct turbo_flow_resolved_config_s turbo_flow_resolved_config_t;
typedef struct turbo_flow_config_error_s turbo_flow_config_error_t;

typedef enum turbo_flow_storage_model_e {
  TURBO_FLOW_STORAGE_MODEL_RECORD = 1,
  TURBO_FLOW_STORAGE_MODEL_STATE = 2,
  TURBO_FLOW_STORAGE_MODEL_INDEX = 3,
  TURBO_FLOW_STORAGE_MODEL_LOG = 4,
  TURBO_FLOW_STORAGE_MODEL_SERIES = 5
} turbo_flow_storage_model_t;

typedef uint64_t turbo_flow_storage_capabilities_t;

#define TURBO_FLOW_STORAGE_CAP_RECORD (UINT64_C(1) << 0u)
#define TURBO_FLOW_STORAGE_CAP_STATE (UINT64_C(1) << 1u)
#define TURBO_FLOW_STORAGE_CAP_INDEX (UINT64_C(1) << 2u)
#define TURBO_FLOW_STORAGE_CAP_LOG (UINT64_C(1) << 3u)
#define TURBO_FLOW_STORAGE_CAP_SERIES (UINT64_C(1) << 4u)

typedef struct turbo_flow_storage_backend_open_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_storage_model_t model;
  /** Borrowed immutable config snapshot, valid only for the duration of open(). */
  const turbo_flow_resolved_config_t *resolved;
  /** Borrowed channel name in resolved, valid only for the duration of open(). */
  const char *channel_name;
  /** Optional backend-defined, immutable direct-open options; trailing fields are reserved. */
  const void *options;
  size_t options_size;
} turbo_flow_storage_backend_open_request_t;

#define TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT                                               \
  {sizeof(turbo_flow_storage_backend_open_request_t), TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION,      \
   TURBO_FLOW_STORAGE_MODEL_RECORD, NULL, NULL, NULL, 0u}

typedef struct turbo_flow_storage_backend_service_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_storage_model_t model;
  /** Provider-neutral service pointer borrowed until the plugin close() call. */
  void *instance;
  /** Plugin-private lifecycle handle passed back only to close(). */
  void *owner;
} turbo_flow_storage_backend_service_t;

#define TURBO_FLOW_STORAGE_BACKEND_SERVICE_INIT                                                    \
  {sizeof(turbo_flow_storage_backend_service_t), TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION,           \
   TURBO_FLOW_STORAGE_MODEL_RECORD, NULL, NULL}

typedef int (*turbo_flow_storage_backend_open_fn)(
    void *ctx, const turbo_flow_storage_backend_open_request_t *request,
    turbo_flow_storage_backend_service_t *service, turbo_flow_config_error_t *error);
typedef void (*turbo_flow_storage_backend_close_fn)(
    void *ctx, turbo_flow_storage_backend_service_t *service);

typedef struct turbo_flow_storage_backend_plugin_api_s {
  size_t size;
  uint32_t version_major;
  uint32_t version_minor;
  /** Borrowed static backend identifier, valid while the module is loaded. */
  const char *backend;
  turbo_flow_storage_capabilities_t capabilities;
  void *ctx;
  turbo_flow_storage_backend_open_fn open;
  turbo_flow_storage_backend_close_fn close;
} turbo_flow_storage_backend_plugin_api_t;

#define TURBO_FLOW_STORAGE_BACKEND_PLUGIN_EXPORT_SYMBOL                                            \
  "turbo_flow_storage_backend_plugin_get_api"

typedef const turbo_flow_storage_backend_plugin_api_t *
(*turbo_flow_storage_backend_plugin_get_api_fn)(void);

/** Canonical symbol exported by dynamically loaded storage backend modules. */
TURBO_FLOW_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_plugin_get_api(void);

typedef struct turbo_flow_storage_backend_registry_s turbo_flow_storage_backend_registry_t;
typedef struct turbo_flow_storage_backend_owner_s turbo_flow_storage_backend_owner_t;

/**
 * Create a caller-serialized, bounded backend registry.
 *
 * The registry borrows directly registered APIs and owns dynamically loaded modules. Destroy
 * returns TURBO_EBUSY while any owner is alive.
 */
TURBO_FLOW_C_API int turbo_flow_storage_backend_registry_create(
    size_t capacity, turbo_flow_storage_backend_registry_t **out);
TURBO_FLOW_C_API int
turbo_flow_storage_backend_registry_destroy(turbo_flow_storage_backend_registry_t *registry);

/** Register one static API. Duplicate backend identifiers return TURBO_EALREADY. */
TURBO_FLOW_C_API int turbo_flow_storage_backend_registry_register(
    turbo_flow_storage_backend_registry_t *registry,
    const turbo_flow_storage_backend_plugin_api_t *api);

/** Load the canonical API from one module and register it atomically. */
TURBO_FLOW_C_API int turbo_flow_storage_backend_registry_load(
    turbo_flow_storage_backend_registry_t *registry, const char *path, char *reason,
    size_t reason_size);

TURBO_FLOW_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_registry_find(const turbo_flow_storage_backend_registry_t *registry,
                                         const char *backend);

/**
 * Open one backend service and retain its module until owner destruction.
 *
 * The returned owner is the only mutable lifecycle owner. The service pointer is borrowed and
 * must not be freed or retained after owner destruction.
 */
TURBO_FLOW_C_API int turbo_flow_storage_backend_owner_create_registered(
    turbo_flow_storage_backend_registry_t *registry, const char *backend,
    const turbo_flow_storage_backend_open_request_t *request,
    turbo_flow_storage_backend_owner_t **out, turbo_flow_config_error_t *error);
TURBO_FLOW_C_API int turbo_flow_storage_backend_owner_service(
    const turbo_flow_storage_backend_owner_t *owner, turbo_flow_storage_model_t expected_model,
    void **out);
TURBO_FLOW_C_API const char *
turbo_flow_storage_backend_owner_backend(const turbo_flow_storage_backend_owner_t *owner);
TURBO_FLOW_C_API turbo_flow_storage_model_t
turbo_flow_storage_backend_owner_model(const turbo_flow_storage_backend_owner_t *owner);
TURBO_FLOW_C_API void
turbo_flow_storage_backend_owner_destroy(turbo_flow_storage_backend_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_STORAGE_BACKEND_H */
