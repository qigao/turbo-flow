#include "flow_plugin_operation_internal.h"
#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_protocol.h"

#include "turbo_flow_stl_error_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <windows.h>
typedef HMODULE flow_plugin_module_handle_t;
#else
  #include <dlfcn.h>
typedef void *flow_plugin_module_handle_t;
#endif

typedef enum flow_plugin_host_state_e {
  FLOW_PLUGIN_HOST_ACTIVE = 1,
  FLOW_PLUGIN_HOST_SHUTTING_DOWN
} flow_plugin_host_state_t;

typedef enum flow_plugin_module_state_e {
  FLOW_PLUGIN_MODULE_ACTIVE = 1,
  FLOW_PLUGIN_MODULE_QUIESCED,
  FLOW_PLUGIN_MODULE_SHUTDOWN,
  FLOW_PLUGIN_MODULE_UNLOADED
} flow_plugin_module_state_t;

typedef struct flow_plugin_module_s {
  flow_plugin_module_handle_t handle;
  const turbo_flow_plugin_api_v1_t *api;
  void *plugin;
  size_t leases;
  flow_plugin_module_state_t state;
  char plugin_id[TURBO_FLOW_PLUGIN_ID_MAX + 1u];
  char plugin_version[TURBO_FLOW_PLUGIN_VERSION_MAX + 1u];
} flow_plugin_module_t;

typedef struct flow_plugin_adapter_provider_s {
  turbo_flow_product_adapter_provider_t provider;
  size_t module_index;
} flow_plugin_adapter_provider_t;

typedef struct flow_plugin_resource_provider_s {
  turbo_flow_product_resource_provider_t provider;
  size_t module_index;
} flow_plugin_resource_provider_t;

typedef struct flow_plugin_protocol_provider_s {
  turbo_flow_protocol_plugin_api_t provider;
  size_t module_index;
} flow_plugin_protocol_provider_t;

typedef struct flow_plugin_business_provider_s {
  turbo_flow_protocol_business_plugin_api_t provider;
  size_t module_index;
} flow_plugin_business_provider_t;

typedef struct flow_plugin_transactional_adapter_provider_s {
  turbo_flow_plugin_transactional_adapter_provider_v1_t provider;
  size_t module_index;
} flow_plugin_transactional_adapter_provider_t;

typedef struct flow_plugin_transactional_resource_provider_s {
  turbo_flow_plugin_transactional_resource_provider_v1_t provider;
  size_t module_index;
} flow_plugin_transactional_resource_provider_t;

typedef struct flow_plugin_schema_s {
  turbo_flow_plugin_schema_v1_t schema;
  size_t module_index;
} flow_plugin_schema_t;

typedef struct flow_plugin_operation_s {
  turbo_flow_plugin_operation_v3_t operation;
  size_t module_index;
} flow_plugin_operation_t;

struct turbo_flow_plugin_host_s {
  turbo_flow_plugin_host_config_t config;
  turbo_flow_plugin_host_v1_t host_api;
  vec_t modules;
  vec_t adapter_providers;
  vec_t resource_providers;
  vec_t protocol_providers;
  vec_t business_providers;
  vec_t transactional_adapter_providers;
  vec_t transactional_resource_providers;
  vec_t schemas;
  vec_t operations;
  size_t active_snapshots;
  flow_plugin_host_state_t state;
};

struct turbo_flow_plugin_catalog_snapshot_s {
  turbo_flow_plugin_host_t *host;
  vec_t adapter_providers;
  vec_t resource_providers;
  vec_t protocol_providers;
  vec_t business_providers;
  vec_t transactional_adapter_providers;
  vec_t transactional_resource_providers;
  vec_t schemas;
  vec_t operations;
  size_t leased_module_count;
  size_t references;
};

typedef struct flow_plugin_registration_context_s {
  turbo_flow_plugin_host_t *host;
  size_t module_index;
  size_t adapter_count_before;
  size_t resource_count_before;
  size_t protocol_count_before;
  size_t business_count_before;
  size_t transactional_adapter_count_before;
  size_t transactional_resource_count_before;
  size_t schema_count_before;
  size_t operation_count_before;
  int first_error;
} flow_plugin_registration_context_t;

static int flow_plugin_error_valid(const turbo_flow_plugin_error_t *error) {
  return error && error->size == sizeof(*error) &&
         error->abi_major == TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR &&
         error->abi_minor == TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
}

static void flow_plugin_copy_text(char *target, size_t capacity, const char *source) {
  size_t size = 0u;
  if (!target || capacity == 0u) return;
  if (source) {
    while (size + 1u < capacity && source[size] != '\0')
      ++size;
    if (size > 0u) memcpy(target, source, size);
  }
  target[size] = '\0';
}

static int flow_plugin_error_write(turbo_flow_plugin_error_t *error, int status,
                                   turbo_flow_plugin_error_stage_t stage, const char *plugin_id,
                                   const char *path, const char *message) {
  size_t caller_size;
  if (!flow_plugin_error_valid(error)) return status;
  caller_size = error->size;
  memset(error, 0, sizeof(*error));
  error->size = caller_size;
  error->abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  error->abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  error->status = status;
  error->stage = stage;
  flow_plugin_copy_text(error->plugin_id, sizeof(error->plugin_id), plugin_id);
  flow_plugin_copy_text(error->path, sizeof(error->path), path);
  flow_plugin_copy_text(error->message, sizeof(error->message), message);
  return status;
}

static void flow_plugin_error_clear(turbo_flow_plugin_error_t *error) {
  (void)flow_plugin_error_write(error, SALTS_OK, TURBO_FLOW_PLUGIN_STAGE_NONE, NULL, NULL, NULL);
}

static size_t flow_plugin_bounded_length(const char *text, size_t maximum) {
  size_t size = 0u;
  if (!text) return maximum + 1u;
  while (size <= maximum && text[size] != '\0')
    ++size;
  return size;
}

static int flow_plugin_ascii_alnum(unsigned char value) {
  return (value >= (unsigned char)'0' && value <= (unsigned char)'9') ||
         (value >= (unsigned char)'A' && value <= (unsigned char)'Z') ||
         (value >= (unsigned char)'a' && value <= (unsigned char)'z');
}

static int flow_plugin_identity_valid(const char *identity) {
  const size_t size = flow_plugin_bounded_length(identity, TURBO_FLOW_PLUGIN_ID_MAX);
  if (size == 0u || size > TURBO_FLOW_PLUGIN_ID_MAX) return 0;
  for (size_t i = 0u; i < size; ++i) {
    const unsigned char value = (unsigned char)identity[i];
    if (!flow_plugin_ascii_alnum(value) && value != '.' && value != '_' && value != '-') return 0;
  }
  return 1;
}

static int flow_plugin_version_valid(const char *version) {
  const size_t size = flow_plugin_bounded_length(version, TURBO_FLOW_PLUGIN_VERSION_MAX);
  if (size == 0u || size > TURBO_FLOW_PLUGIN_VERSION_MAX) return 0;
  for (size_t i = 0u; i < size; ++i) {
    const unsigned char value = (unsigned char)version[i];
    if (!flow_plugin_ascii_alnum(value) && value != '.' && value != '_' && value != '-' &&
        value != '+')
      return 0;
  }
  return 1;
}

static int flow_plugin_kind_valid(const char *kind) {
  const size_t size = flow_plugin_bounded_length(kind, TURBO_FLOW_PLUGIN_ID_MAX);
  return size > 0u && size <= TURBO_FLOW_PLUGIN_ID_MAX;
}

static void flow_plugin_observe(turbo_flow_plugin_host_t *host,
                                turbo_flow_plugin_lifecycle_event_t event, const char *plugin_id,
                                int status) {
  if (host && host->config.lifecycle_observer)
    host->config.lifecycle_observer(host->config.lifecycle_observer_ctx, event,
                                    plugin_id ? plugin_id : "", status);
}

static void *flow_plugin_host_allocate(void *ctx, size_t size) {
  (void)ctx;
  if (size == 0u) return NULL;
  return malloc(size);
}

static void flow_plugin_host_deallocate(void *ctx, void *memory) {
  (void)ctx;
  free(memory);
}

static flow_plugin_module_handle_t flow_plugin_module_open(const char *path) {
#ifdef _WIN32
  wchar_t wide_path[TURBO_FLOW_PLUGIN_PATH_MAX + 1u];
  int wide_size;
  int index;
  if (!path) return NULL;
  SetLastError(ERROR_SUCCESS);
  wide_size = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide_path,
                                  (int)(sizeof(wide_path) / sizeof(wide_path[0])));
  if (wide_size <= 0) return NULL;
  for (index = 0; index < wide_size; ++index)
    if (wide_path[index] == L'/') wide_path[index] = L'\\';
  return LoadLibraryExW(wide_path, NULL,
                        LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);
#else
  if (!path) return NULL;
  (void)dlerror();
  return dlopen(path, RTLD_NOW | RTLD_LOCAL);
#endif
}

static void flow_plugin_module_close(flow_plugin_module_handle_t module) {
  if (!module) return;
#ifdef _WIN32
  (void)FreeLibrary(module);
#else
  (void)dlclose(module);
#endif
}

static void *flow_plugin_module_symbol(flow_plugin_module_handle_t module, const char *name) {
  if (!module || !name) return NULL;
#ifdef _WIN32
  SetLastError(ERROR_SUCCESS);
  return (void *)GetProcAddress(module, name);
#else
  (void)dlerror();
  return dlsym(module, name);
#endif
}

static void flow_plugin_module_error(char *buffer, size_t capacity) {
  if (!buffer || capacity == 0u) return;
#ifdef _WIN32
  (void)snprintf(buffer, capacity, "Win32 dynamic library error %lu",
                 (unsigned long)GetLastError());
#else
  {
    const char *message = dlerror();
    (void)snprintf(buffer, capacity, "%s", message ? message : "dynamic library error");
  }
#endif
}

static flow_plugin_module_t *flow_plugin_module_at(turbo_flow_plugin_host_t *host, size_t index) {
  return host ? (flow_plugin_module_t *)vec_at(&host->modules, index) : NULL;
}

static const flow_plugin_module_t *flow_plugin_module_at_const(const turbo_flow_plugin_host_t *host,
                                                               size_t index) {
  return host ? (const flow_plugin_module_t *)vec_at_const(&host->modules, index) : NULL;
}

static int flow_plugin_find_module(const turbo_flow_plugin_host_t *host, const char *plugin_id) {
  if (!host || !plugin_id) return -1;
  for (size_t i = 0u; i < vec_size(&host->modules); ++i) {
    const flow_plugin_module_t *module = flow_plugin_module_at_const(host, i);
    if (module && strcmp(module->plugin_id, plugin_id) == 0) return (int)i;
  }
  return -1;
}

static int flow_plugin_find_adapter_kind(const turbo_flow_plugin_host_t *host, const char *kind) {
  if (!host || !kind) return -1;
  for (size_t i = 0u; i < vec_size(&host->adapter_providers); ++i) {
    const flow_plugin_adapter_provider_t *entry =
        (const flow_plugin_adapter_provider_t *)vec_at_const(&host->adapter_providers, i);
    if (entry && entry->provider.kind && strcmp(entry->provider.kind, kind) == 0) return (int)i;
  }
  return -1;
}

static int flow_plugin_find_resource_kind(const turbo_flow_plugin_host_t *host, const char *kind) {
  if (!host || !kind) return -1;
  for (size_t i = 0u; i < vec_size(&host->resource_providers); ++i) {
    const flow_plugin_resource_provider_t *entry =
        (const flow_plugin_resource_provider_t *)vec_at_const(&host->resource_providers, i);
    if (entry && entry->provider.kind && strcmp(entry->provider.kind, kind) == 0) return (int)i;
  }
  return -1;
}

static int flow_plugin_find_transactional_adapter_kind(const turbo_flow_plugin_host_t *host,
                                                       const char *kind) {
  if (!host || !kind) return -1;
  for (size_t i = 0u; i < vec_size(&host->transactional_adapter_providers); ++i) {
    const flow_plugin_transactional_adapter_provider_t *entry =
        (const flow_plugin_transactional_adapter_provider_t *)vec_at_const(
            &host->transactional_adapter_providers, i);
    if (entry && entry->provider.kind && strcmp(entry->provider.kind, kind) == 0) return (int)i;
  }
  return -1;
}

static int flow_plugin_find_transactional_resource_kind(const turbo_flow_plugin_host_t *host,
                                                        const char *kind) {
  if (!host || !kind) return -1;
  for (size_t i = 0u; i < vec_size(&host->transactional_resource_providers); ++i) {
    const flow_plugin_transactional_resource_provider_t *entry =
        (const flow_plugin_transactional_resource_provider_t *)vec_at_const(
            &host->transactional_resource_providers, i);
    if (entry && entry->provider.kind && strcmp(entry->provider.kind, kind) == 0) return (int)i;
  }
  return -1;
}

static int flow_plugin_find_protocol_name(const turbo_flow_plugin_host_t *host, const char *name) {
  if (!host || !name) return -1;
  for (size_t i = 0u; i < vec_size(&host->protocol_providers); ++i) {
    const flow_plugin_protocol_provider_t *entry =
        (const flow_plugin_protocol_provider_t *)vec_at_const(&host->protocol_providers, i);
    if (entry && entry->provider.name && strcmp(entry->provider.name, name) == 0) return (int)i;
  }
  return -1;
}

static int flow_plugin_find_business_name(const turbo_flow_plugin_host_t *host,
                                          const char *business) {
  if (!host || !business) return -1;
  for (size_t i = 0u; i < vec_size(&host->business_providers); ++i) {
    const flow_plugin_business_provider_t *entry =
        (const flow_plugin_business_provider_t *)vec_at_const(&host->business_providers, i);
    if (entry && entry->provider.business && strcmp(entry->provider.business, business) == 0)
      return (int)i;
  }
  return -1;
}

static int flow_plugin_protocol_provider_valid(const turbo_flow_protocol_plugin_api_t *provider) {
  const turbo_flow_protocol_capabilities_t required = TURBO_FLOW_PROTOCOL_CAP_INGRESS |
                                                      TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                                                      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE;
  return provider && provider->size >= sizeof(*provider) &&
         provider->version_major == TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR &&
         flow_plugin_identity_valid(provider->name) &&
         provider->protocol >= TURBO_FLOW_PROTOCOL_MQTT_SN &&
         provider->protocol <= TURBO_FLOW_PROTOCOL_JTT_808 &&
         (provider->capabilities & required) == required && provider->open && provider->close;
}

static int
flow_plugin_business_provider_valid(const turbo_flow_protocol_business_plugin_api_t *provider) {
  const turbo_flow_protocol_business_capabilities_t known =
      TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT |
      TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND;
  return provider && provider->size >= sizeof(*provider) &&
         provider->version_major == TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MAJOR &&
         flow_plugin_identity_valid(provider->business) &&
         provider->protocol >= TURBO_FLOW_PROTOCOL_MQTT_SN &&
         provider->protocol <= TURBO_FLOW_PROTOCOL_JTT_808 && provider->capabilities != 0u &&
         (provider->capabilities & ~known) == 0u && provider->open && provider->close;
}

static int flow_plugin_api_validate(const turbo_flow_plugin_api_v1_t *api,
                                    turbo_flow_plugin_error_t *error, const char *path) {
  const turbo_flow_plugin_capabilities_t known =
      TURBO_FLOW_PLUGIN_CAP_PRODUCT_ADAPTER | TURBO_FLOW_PLUGIN_CAP_PRODUCT_RESOURCE |
      TURBO_FLOW_PLUGIN_CAP_PROTOCOL | TURBO_FLOW_PLUGIN_CAP_PROTOCOL_BUSINESS |
      TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE |
      TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL | TURBO_FLOW_PLUGIN_CAP_SCHEMA |
      (uint32_t)TURBO_FLOW_PLUGIN_CAP_OPERATION;
  if (!api || api->size != sizeof(*api) || api->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      api->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_API, NULL, path,
                                   "invalid plugin root ABI");
  }
  if ((api->capabilities & ~known) != 0u || api->capabilities == 0u || !api->load ||
      !api->register_capabilities || !api->quiesce || !api->shutdown || !api->destroy) {
    return flow_plugin_error_write(error, SALTS_EPROTO, TURBO_FLOW_PLUGIN_STAGE_API, NULL, path,
                                   "invalid plugin root capabilities or lifecycle vtable");
  }
  if ((api->capabilities & TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL) != 0u &&
      (api->capabilities & (TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER |
                            TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE)) == 0u) {
    return flow_plugin_error_write(error, SALTS_EPROTO, TURBO_FLOW_PLUGIN_STAGE_API, NULL, path,
                                   "external progress requires a transactional Product provider");
  }
  if (!flow_plugin_identity_valid(api->plugin_id) ||
      !flow_plugin_version_valid(api->plugin_version)) {
    return flow_plugin_error_write(error, SALTS_EPROTO, TURBO_FLOW_PLUGIN_STAGE_IDENTITY, NULL,
                                   path, "invalid plugin identity or version");
  }
  return SALTS_OK;
}

static int flow_plugin_add_adapter_provider(
    void *ctx, const turbo_flow_plugin_product_adapter_provider_v1_t *registration_provider) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_adapter_provider_t entry;
  const turbo_flow_product_adapter_provider_t *provider;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!registration_provider || registration_provider->size != sizeof(*registration_provider) ||
      registration_provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration_provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  provider = &registration_provider->provider;
  if (provider->size < sizeof(*provider) || !flow_plugin_kind_valid(provider->kind) ||
      !provider->register_adapter) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  if (flow_plugin_find_adapter_kind(registration->host, provider->kind) >= 0 ||
      flow_plugin_find_transactional_adapter_kind(registration->host, provider->kind) >= 0) {
    registration->first_error = SALTS_EALREADY;
    return registration->first_error;
  }
  if (vec_size(&registration->host->adapter_providers) >=
      registration->host->config.adapter_provider_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.provider = *provider;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->adapter_providers, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_add_resource_provider(
    void *ctx, const turbo_flow_plugin_product_resource_provider_v1_t *registration_provider) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_resource_provider_t entry;
  const turbo_flow_product_resource_provider_t *provider;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!registration_provider || registration_provider->size != sizeof(*registration_provider) ||
      registration_provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration_provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  provider = &registration_provider->provider;
  if (provider->size < sizeof(*provider) || !flow_plugin_kind_valid(provider->kind) ||
      !provider->register_resource) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  if (flow_plugin_find_resource_kind(registration->host, provider->kind) >= 0 ||
      flow_plugin_find_transactional_resource_kind(registration->host, provider->kind) >= 0) {
    registration->first_error = SALTS_EALREADY;
    return registration->first_error;
  }
  if (vec_size(&registration->host->resource_providers) >=
      registration->host->config.resource_provider_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.provider = *provider;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->resource_providers, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_add_protocol_provider(
    void *ctx, const turbo_flow_plugin_protocol_provider_v1_t *registration_provider) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_protocol_provider_t entry;
  const turbo_flow_protocol_plugin_api_t *provider;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!registration_provider || registration_provider->size != sizeof(*registration_provider) ||
      registration_provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration_provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  provider = &registration_provider->provider;
  if (!flow_plugin_protocol_provider_valid(provider)) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  if (flow_plugin_find_protocol_name(registration->host, provider->name) >= 0) {
    registration->first_error = SALTS_EALREADY;
    return registration->first_error;
  }
  if (vec_size(&registration->host->protocol_providers) >=
      registration->host->config.protocol_provider_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.provider = *provider;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->protocol_providers, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_add_business_provider(
    void *ctx, const turbo_flow_plugin_business_provider_v1_t *registration_provider) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_business_provider_t entry;
  const turbo_flow_protocol_business_plugin_api_t *provider;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!registration_provider || registration_provider->size != sizeof(*registration_provider) ||
      registration_provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration_provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  provider = &registration_provider->provider;
  if (!flow_plugin_business_provider_valid(provider)) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  if (flow_plugin_find_business_name(registration->host, provider->business) >= 0) {
    registration->first_error = SALTS_EALREADY;
    return registration->first_error;
  }
  if (vec_size(&registration->host->business_providers) >=
      registration->host->config.business_provider_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.provider = *provider;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->business_providers, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_add_transactional_adapter_provider(
    void *ctx, const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_transactional_adapter_provider_t entry;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!provider || provider->size != sizeof(*provider) ||
      provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !flow_plugin_kind_valid(provider->kind) || !provider->preflight || !provider->materialize) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  if (flow_plugin_find_adapter_kind(registration->host, provider->kind) >= 0 ||
      flow_plugin_find_transactional_adapter_kind(registration->host, provider->kind) >= 0) {
    registration->first_error = SALTS_EALREADY;
    return registration->first_error;
  }
  if (vec_size(&registration->host->transactional_adapter_providers) >=
      registration->host->config.transactional_adapter_provider_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.provider = *provider;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->transactional_adapter_providers, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_add_transactional_resource_provider(
    void *ctx, const turbo_flow_plugin_transactional_resource_provider_v1_t *provider) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_transactional_resource_provider_t entry;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!provider || provider->size != sizeof(*provider) ||
      provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !flow_plugin_kind_valid(provider->kind) || !provider->preflight || !provider->materialize) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  if (flow_plugin_find_resource_kind(registration->host, provider->kind) >= 0 ||
      flow_plugin_find_transactional_resource_kind(registration->host, provider->kind) >= 0) {
    registration->first_error = SALTS_EALREADY;
    return registration->first_error;
  }
  if (vec_size(&registration->host->transactional_resource_providers) >=
      registration->host->config.transactional_resource_provider_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.provider = *provider;
  entry.module_index = registration->module_index;
  rc =
      turbo_flow_stl_error(vec_push(&registration->host->transactional_resource_providers, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_add_schema(void *ctx, const turbo_flow_plugin_schema_v1_t *schema) {
  flow_plugin_registration_context_t *registration = (flow_plugin_registration_context_t *)ctx;
  flow_plugin_schema_t entry;
  int rc;
  if (!registration) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  if (!schema || schema->size != sizeof(*schema) ||
      schema->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      schema->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || schema->schema_version == 0u ||
      !schema->data || !cmeta_data_desc_valid(schema->data) ||
      !cmeta_type_desc_valid(schema->data->storage_type) ||
      !flow_plugin_identity_valid(schema->data->stable_id)) {
    registration->first_error = SALTS_EINVAL;
    return registration->first_error;
  }
  for (size_t i = 0u; i < vec_size(&registration->host->schemas); ++i) {
    const flow_plugin_schema_t *existing =
        (const flow_plugin_schema_t *)vec_at_const(&registration->host->schemas, i);
    if (existing && existing->schema.schema_version == schema->schema_version &&
        strcmp(existing->schema.data->stable_id, schema->data->stable_id) == 0) {
      registration->first_error = SALTS_EALREADY;
      return registration->first_error;
    }
  }
  if (vec_size(&registration->host->schemas) >= registration->host->config.schema_capacity) {
    registration->first_error = SALTS_ENOSPC;
    return registration->first_error;
  }
  memset(&entry, 0, sizeof(entry));
  entry.schema = *schema;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->schemas, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_operation_schema_valid(const turbo_flow_plugin_operation_schema_v3_t *s) {
  if (s->size != sizeof(*s) || s->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      s->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !s->schema_version || !s->projection ||
      s->projection->size != sizeof(*s->projection) ||
      s->projection->schema_version != s->schema_version)
    return SALTS_EINVAL;
  return turbo_flow_data_schema_match(s->projection, s->data, s->projection, s->data);
}

static int flow_plugin_operation_valid(const turbo_flow_plugin_operation_v3_t *op) {
  const turbo_flow_plugin_operation_limits_v3_t *limits;
  int rc;
  if (!op || op->size != sizeof(*op) || op->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      op->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  limits = &op->limits;
  if (!flow_plugin_identity_valid(op->operation_name) || !op->operation_version || !op->preflight ||
      !op->create_session || !op->create_result_context || op->vtable.size != sizeof(op->vtable) ||
      op->vtable.abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      op->vtable.abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !op->vtable.execute ||
      !op->vtable.clone_result || !op->vtable.destroy_result || !op->vtable.release_session ||
      !op->vtable.release_result_context || limits->size != sizeof(*limits) ||
      limits->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      limits->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !limits->max_inflight ||
      limits->max_inflight > FLOW_PLUGIN_OPERATION_MAX_INFLIGHT || !limits->max_input_bytes ||
      limits->max_input_bytes > FLOW_PLUGIN_OPERATION_MAX_BYTES || !limits->max_result_bytes ||
      limits->max_result_bytes > FLOW_PLUGIN_OPERATION_MAX_BYTES || !limits->max_retained_bytes ||
      limits->max_retained_bytes > FLOW_PLUGIN_OPERATION_MAX_BYTES || !limits->max_steps ||
      !op->max_session_bytes || op->max_session_bytes > FLOW_PLUGIN_OPERATION_MAX_BYTES ||
      !op->max_result_context_bytes ||
      op->max_result_context_bytes > FLOW_PLUGIN_OPERATION_MAX_BYTES ||
      op->permission_count > TURBO_FLOW_PLUGIN_OPERATION_MAX_PERMISSIONS ||
      (op->permission_count && !op->permissions))
    return SALTS_EINVAL;
  for (size_t i = 0; i < op->permission_count; ++i) {
    if (!flow_plugin_identity_valid(op->permissions[i])) return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (!strcmp(op->permissions[i], op->permissions[j])) return SALTS_EALREADY;
  }
  if (op->execution != TURBO_FLOW_PLUGIN_OPERATION_SYNC ||
      op->threading != TURBO_FLOW_PLUGIN_OPERATION_THREAD_SAFE ||
      op->cancellation != TURBO_FLOW_PLUGIN_OPERATION_CANCEL_NONE ||
      op->effects != TURBO_FLOW_PLUGIN_OPERATION_EFFECT_RESULT ||
      op->guarantees != TURBO_FLOW_PLUGIN_OPERATION_STEPS_CHARGED || limits->deadline_ms ||
      op->permission_count)
    return SALTS_ENOTSUP;
  rc = flow_plugin_operation_schema_valid(&op->input);
  return rc == SALTS_OK ? flow_plugin_operation_schema_valid(&op->output) : rc;
}

static int flow_plugin_add_operation(void *ctx, const turbo_flow_plugin_operation_v3_t *op) {
  flow_plugin_registration_context_t *registration = ctx;
  flow_plugin_operation_t entry;
  int rc;
  if (!registration || !registration->host) return SALTS_EINVAL;
  if (registration->first_error != SALTS_OK) return registration->first_error;
  rc = flow_plugin_operation_valid(op);
  if (rc != SALTS_OK) return registration->first_error = rc;
  for (size_t i = 0; i < vec_size(&registration->host->operations); ++i) {
    const flow_plugin_operation_t *old = vec_at_const(&registration->host->operations, i);
    if (old->module_index == registration->module_index &&
        old->operation.operation_version == op->operation_version &&
        !strcmp(old->operation.operation_name, op->operation_name))
      return registration->first_error = SALTS_EALREADY;
  }
  if (vec_size(&registration->host->operations) >= registration->host->config.operation_capacity)
    return registration->first_error = SALTS_ENOSPC;
  entry.operation = *op;
  entry.module_index = registration->module_index;
  rc = turbo_flow_stl_error(vec_push(&registration->host->operations, &entry));
  if (rc != SALTS_OK) registration->first_error = rc;
  return rc;
}

static int flow_plugin_operation_resolve_schemas(flow_plugin_registration_context_t *r) {
  for (size_t i = r->operation_count_before; i < vec_size(&r->host->operations); ++i) {
    const flow_plugin_operation_t *entry = vec_at_const(&r->host->operations, i);
    const turbo_flow_plugin_operation_schema_v3_t *schemas[] = {&entry->operation.input,
                                                                &entry->operation.output};
    for (size_t k = 0; k < 2; ++k) {
      const turbo_flow_plugin_operation_schema_v3_t *wanted = schemas[k];
      int found = 0;
      for (size_t j = 0; j < vec_size(&r->host->schemas); ++j) {
        const flow_plugin_schema_t *s = vec_at_const(&r->host->schemas, j);
        if (s->module_index != r->module_index ||
            s->schema.schema_version != wanted->schema_version ||
            strcmp(s->schema.data->stable_id, wanted->data->stable_id))
          continue;
        int rc = turbo_flow_data_schema_match(wanted->projection, wanted->data, wanted->projection,
                                              s->schema.data);
        if (rc != SALTS_OK) return rc;
        found = 1;
        break;
      }
      if (!found) return SALTS_EPROTO;
    }
  }
  return SALTS_OK;
}

static void flow_plugin_zero_vector_tail(vec_t *values, size_t first) {
  if (!values) return;
  for (size_t i = first; i < vec_size(values); ++i) {
    void *value = vec_at(values, i);
    if (value) memset(value, 0, values->elem_size);
  }
  (void)vec_resize(values, first);
}

static void flow_plugin_registration_rollback(flow_plugin_registration_context_t *registration) {
  if (!registration || !registration->host) return;
  flow_plugin_zero_vector_tail(&registration->host->adapter_providers,
                               registration->adapter_count_before);
  flow_plugin_zero_vector_tail(&registration->host->resource_providers,
                               registration->resource_count_before);
  flow_plugin_zero_vector_tail(&registration->host->protocol_providers,
                               registration->protocol_count_before);
  flow_plugin_zero_vector_tail(&registration->host->business_providers,
                               registration->business_count_before);
  flow_plugin_zero_vector_tail(&registration->host->transactional_adapter_providers,
                               registration->transactional_adapter_count_before);
  flow_plugin_zero_vector_tail(&registration->host->transactional_resource_providers,
                               registration->transactional_resource_count_before);
  flow_plugin_zero_vector_tail(&registration->host->schemas, registration->schema_count_before);
  flow_plugin_zero_vector_tail(&registration->host->operations,
                               registration->operation_count_before);
}

static void flow_plugin_cleanup_uncommitted(turbo_flow_plugin_host_t *host,
                                            flow_plugin_module_handle_t module,
                                            const turbo_flow_plugin_api_v1_t *api, void *plugin,
                                            const char *plugin_id, int rollback_status) {
  char copied_id[TURBO_FLOW_PLUGIN_ID_MAX + 1u] = {0};
  flow_plugin_copy_text(copied_id, sizeof(copied_id), plugin_id);
  if (rollback_status != SALTS_OK)
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_ROLLBACK, copied_id, rollback_status);
  if (api && plugin) {
    const int shutdown_rc = api->shutdown(plugin);
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN, copied_id, shutdown_rc);
    api->destroy(plugin);
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY, copied_id, SALTS_OK);
  }
  flow_plugin_module_close(module);
  flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD, copied_id, SALTS_OK);
}

static int flow_plugin_vectors_initialize(turbo_flow_plugin_host_t *host) {
  int rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&host->operations, sizeof(flow_plugin_operation_t),
                                           _Alignof(turbo_flow_max_align_t),
                                           host->config.operation_capacity));
  if (rc != SALTS_OK) return rc;
  if (host->config.operation_capacity) {
    rc = turbo_flow_stl_error(vec_reserve(&host->operations, host->config.operation_capacity));
    if (rc != SALTS_OK) return rc;
  }
  rc = turbo_flow_stl_error(vec_init_bytes(&host->modules, sizeof(flow_plugin_module_t),
                                           _Alignof(turbo_flow_max_align_t),
                                           host->config.module_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&host->adapter_providers, sizeof(flow_plugin_adapter_provider_t),
                     _Alignof(turbo_flow_max_align_t), host->config.adapter_provider_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&host->resource_providers, sizeof(flow_plugin_resource_provider_t),
                     _Alignof(turbo_flow_max_align_t), host->config.resource_provider_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&host->protocol_providers, sizeof(flow_plugin_protocol_provider_t),
                     _Alignof(turbo_flow_max_align_t), host->config.protocol_provider_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&host->business_providers, sizeof(flow_plugin_business_provider_t),
                     _Alignof(turbo_flow_max_align_t), host->config.business_provider_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(
      &host->transactional_adapter_providers, sizeof(flow_plugin_transactional_adapter_provider_t),
      _Alignof(turbo_flow_max_align_t), host->config.transactional_adapter_provider_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&host->transactional_resource_providers,
                                           sizeof(flow_plugin_transactional_resource_provider_t),
                                           _Alignof(turbo_flow_max_align_t),
                                           host->config.transactional_resource_provider_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&host->schemas, sizeof(flow_plugin_schema_t),
                                           _Alignof(turbo_flow_max_align_t),
                                           host->config.schema_capacity));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_reserve(&host->modules, host->config.module_capacity));
  if (rc == SALTS_OK && host->config.adapter_provider_capacity > 0u)
    rc = turbo_flow_stl_error(
        vec_reserve(&host->adapter_providers, host->config.adapter_provider_capacity));
  if (rc == SALTS_OK && host->config.resource_provider_capacity > 0u)
    rc = turbo_flow_stl_error(
        vec_reserve(&host->resource_providers, host->config.resource_provider_capacity));
  if (rc == SALTS_OK && host->config.protocol_provider_capacity > 0u)
    rc = turbo_flow_stl_error(
        vec_reserve(&host->protocol_providers, host->config.protocol_provider_capacity));
  if (rc == SALTS_OK && host->config.business_provider_capacity > 0u)
    rc = turbo_flow_stl_error(
        vec_reserve(&host->business_providers, host->config.business_provider_capacity));
  if (rc == SALTS_OK && host->config.transactional_adapter_provider_capacity > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&host->transactional_adapter_providers,
                                          host->config.transactional_adapter_provider_capacity));
  if (rc == SALTS_OK && host->config.transactional_resource_provider_capacity > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&host->transactional_resource_providers,
                                          host->config.transactional_resource_provider_capacity));
  if (rc == SALTS_OK && host->config.schema_capacity > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&host->schemas, host->config.schema_capacity));
  return rc;
}

static void flow_plugin_vectors_destroy(turbo_flow_plugin_host_t *host) {
  if (!host) return;
  vec_destroy(&host->operations);
  vec_destroy(&host->schemas);
  vec_destroy(&host->transactional_resource_providers);
  vec_destroy(&host->transactional_adapter_providers);
  vec_destroy(&host->business_providers);
  vec_destroy(&host->protocol_providers);
  vec_destroy(&host->resource_providers);
  vec_destroy(&host->adapter_providers);
  vec_destroy(&host->modules);
}

int turbo_flow_plugin_host_create(const turbo_flow_plugin_host_config_t *config,
                                  turbo_flow_plugin_host_t **host_out,
                                  turbo_flow_plugin_error_t *error) {
  turbo_flow_plugin_host_config_t normalized = {0};
  turbo_flow_plugin_host_t *host;
  int rc;
  if (host_out) *host_out = NULL;
  if (!host_out || !flow_plugin_error_valid(error) || !config ||
      config->size != sizeof(*config) ||
      config->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      config->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   NULL, "invalid bounded PluginHost configuration");
  }
  normalized = *config;
  if (normalized.module_capacity == 0u ||
      normalized.module_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_MODULES ||
      normalized.adapter_provider_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.resource_provider_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.protocol_provider_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.business_provider_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.transactional_adapter_provider_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.transactional_resource_provider_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.schema_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS ||
      normalized.operation_capacity > TURBO_FLOW_PLUGIN_HOST_MAX_PROVIDERS) {
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   NULL, "invalid bounded PluginHost configuration");
  }
  flow_plugin_error_clear(error);
  host = (turbo_flow_plugin_host_t *)calloc(1u, sizeof(*host));
  if (!host)
    return flow_plugin_error_write(error, SALTS_ENOMEM, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   NULL, "failed to allocate PluginHost");
  host->config = normalized;
  host->host_api.size = sizeof(host->host_api);
  host->host_api.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  host->host_api.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  host->host_api.ctx = host;
  host->host_api.allocate = flow_plugin_host_allocate;
  host->host_api.deallocate = flow_plugin_host_deallocate;
  host->state = FLOW_PLUGIN_HOST_ACTIVE;
  rc = flow_plugin_vectors_initialize(host);
  if (rc != SALTS_OK) {
    flow_plugin_vectors_destroy(host);
    free(host);
    return flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL, NULL,
                                   "failed to allocate bounded PluginHost registries");
  }
  *host_out = host;
  return SALTS_OK;
}

static int flow_plugin_host_load_expected(turbo_flow_plugin_host_t *host, const char *path,
                                          const char *expected_id,
                                          const char *expected_version,
                                          turbo_flow_plugin_error_t *error) {
  flow_plugin_module_handle_t module;
  turbo_flow_plugin_get_api_fn get_api = NULL;
  const turbo_flow_plugin_api_v1_t *api;
  turbo_flow_plugin_registration_v1_t registration_api;
  flow_plugin_registration_context_t registration;
  flow_plugin_module_t committed;
  char module_error[TURBO_FLOW_PLUGIN_ERROR_MESSAGE_MAX + 1u];
  void *symbol;
  void *plugin = NULL;
  int rc;
  if (!host || !flow_plugin_error_valid(error) || !path || !path[0] ||
      flow_plugin_bounded_length(path, TURBO_FLOW_PLUGIN_PATH_MAX) > TURBO_FLOW_PLUGIN_PATH_MAX)
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   path, "invalid explicit plugin path or host");
  flow_plugin_error_clear(error);
  if (host->state != FLOW_PLUGIN_HOST_ACTIVE)
    return flow_plugin_error_write(error, SALTS_EBUSY, TURBO_FLOW_PLUGIN_STAGE_STATE, NULL, path,
                                   "PluginHost is shutting down");
  if (vec_size(&host->modules) >= host->config.module_capacity)
    return flow_plugin_error_write(error, SALTS_ENOSPC, TURBO_FLOW_PLUGIN_STAGE_CAPACITY, NULL,
                                   path, "plugin module capacity is exhausted");
  module = flow_plugin_module_open(path);
  if (!module) {
    flow_plugin_module_error(module_error, sizeof(module_error));
    return flow_plugin_error_write(error, SALTS_ENOENT, TURBO_FLOW_PLUGIN_STAGE_OPEN, NULL, path,
                                   module_error);
  }
  symbol = flow_plugin_module_symbol(module, TURBO_FLOW_PLUGIN_EXPORT_SYMBOL);
  if (!symbol) {
    flow_plugin_module_error(module_error, sizeof(module_error));
    flow_plugin_module_close(module);
    return flow_plugin_error_write(error, SALTS_ENOENT, TURBO_FLOW_PLUGIN_STAGE_SYMBOL, NULL, path,
                                   module_error);
  }
  memcpy(&get_api, &symbol, sizeof(get_api));
  api = get_api();
  rc = flow_plugin_api_validate(api, error, path);
  if (rc != SALTS_OK) {
    flow_plugin_module_close(module);
    return rc;
  }
  if ((expected_id && strcmp(api->plugin_id, expected_id) != 0) ||
      (expected_version && strcmp(api->plugin_version, expected_version) != 0)) {
    flow_plugin_error_write(error, SALTS_EPROTO, TURBO_FLOW_PLUGIN_STAGE_IDENTITY,
                            expected_id ? expected_id : api->plugin_id, path,
                            "configured plugin identity or version does not match DLL vtable");
    flow_plugin_module_close(module);
    return SALTS_EPROTO;
  }
  if (flow_plugin_find_module(host, api->plugin_id) >= 0) {
    flow_plugin_error_write(error, SALTS_EALREADY, TURBO_FLOW_PLUGIN_STAGE_IDENTITY, api->plugin_id,
                            path, "duplicate plugin identity");
    flow_plugin_module_close(module);
    return SALTS_EALREADY;
  }
  rc = api->load(&host->host_api, &plugin);
  flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_LOAD, api->plugin_id, rc);
  if (rc != SALTS_OK || !plugin) {
    const int status = rc != SALTS_OK ? rc : SALTS_EPROTO;
    flow_plugin_error_write(error, status, TURBO_FLOW_PLUGIN_STAGE_LOAD, api->plugin_id, path,
                            rc != SALTS_OK ? "plugin load callback failed"
                                           : "plugin load returned no instance");
    flow_plugin_cleanup_uncommitted(host, module, api, plugin, api->plugin_id, SALTS_OK);
    return status;
  }

  memset(&registration, 0, sizeof(registration));
  registration.host = host;
  registration.module_index = vec_size(&host->modules);
  registration.adapter_count_before = vec_size(&host->adapter_providers);
  registration.resource_count_before = vec_size(&host->resource_providers);
  registration.protocol_count_before = vec_size(&host->protocol_providers);
  registration.business_count_before = vec_size(&host->business_providers);
  registration.transactional_adapter_count_before =
      vec_size(&host->transactional_adapter_providers);
  registration.transactional_resource_count_before =
      vec_size(&host->transactional_resource_providers);
  registration.schema_count_before = vec_size(&host->schemas);
  registration.operation_count_before = vec_size(&host->operations);
  registration.first_error = SALTS_OK;
  registration_api.size = sizeof(registration_api);
  registration_api.abi_major = TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR;
  registration_api.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  registration_api.ctx = &registration;
  registration_api.add_adapter_provider = flow_plugin_add_adapter_provider;
  registration_api.add_resource_provider = flow_plugin_add_resource_provider;
  registration_api.add_protocol_provider = flow_plugin_add_protocol_provider;
  registration_api.add_business_provider = flow_plugin_add_business_provider;
  registration_api.add_transactional_adapter_provider =
      flow_plugin_add_transactional_adapter_provider;
  registration_api.add_transactional_resource_provider =
      flow_plugin_add_transactional_resource_provider;
  registration_api.add_schema = flow_plugin_add_schema;
  registration_api.add_operation = flow_plugin_add_operation;
  rc = api->register_capabilities(plugin, &registration_api);
  if (registration.first_error != SALTS_OK) rc = registration.first_error;
  if (rc == SALTS_OK) rc = flow_plugin_operation_resolve_schemas(&registration);
  flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_REGISTER, api->plugin_id, rc);
  if (rc == SALTS_OK) {
    turbo_flow_plugin_capabilities_t actual = 0u;
    if (vec_size(&host->adapter_providers) > registration.adapter_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_PRODUCT_ADAPTER;
    if (vec_size(&host->resource_providers) > registration.resource_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_PRODUCT_RESOURCE;
    if (vec_size(&host->protocol_providers) > registration.protocol_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_PROTOCOL;
    if (vec_size(&host->business_providers) > registration.business_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_PROTOCOL_BUSINESS;
    if (vec_size(&host->transactional_adapter_providers) >
        registration.transactional_adapter_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER;
    if (vec_size(&host->transactional_resource_providers) >
        registration.transactional_resource_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE;
    if (vec_size(&host->schemas) > registration.schema_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_SCHEMA;
    if (vec_size(&host->operations) > registration.operation_count_before)
      actual |= TURBO_FLOW_PLUGIN_CAP_OPERATION;
    if (actual != (api->capabilities & ~TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL)) rc = SALTS_EPROTO;
  }
  if (rc != SALTS_OK) {
    flow_plugin_registration_rollback(&registration);
    flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION, api->plugin_id, path,
                            "plugin capability registration failed or mismatched its declaration");
    flow_plugin_cleanup_uncommitted(host, module, api, plugin, api->plugin_id, rc);
    return rc;
  }

  memset(&committed, 0, sizeof(committed));
  committed.handle = module;
  committed.api = api;
  committed.plugin = plugin;
  committed.state = FLOW_PLUGIN_MODULE_ACTIVE;
  flow_plugin_copy_text(committed.plugin_id, sizeof(committed.plugin_id), api->plugin_id);
  flow_plugin_copy_text(committed.plugin_version, sizeof(committed.plugin_version),
                        api->plugin_version);
  rc = turbo_flow_stl_error(vec_push(&host->modules, &committed));
  if (rc != SALTS_OK) {
    flow_plugin_registration_rollback(&registration);
    flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_COMMIT, api->plugin_id, path,
                            "failed to commit plugin module after provider validation");
    flow_plugin_cleanup_uncommitted(host, module, api, plugin, api->plugin_id, rc);
    return rc;
  }
  flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_COMMIT, committed.plugin_id, SALTS_OK);
  return SALTS_OK;
}

int turbo_flow_plugin_host_load(turbo_flow_plugin_host_t *host, const char *path,
                                turbo_flow_plugin_error_t *error) {
  return flow_plugin_host_load_expected(host, path, NULL, NULL, error);
}

int turbo_flow_plugin_host_create_configured(
    const turbo_flow_plugin_host_config_t *host_config,
    const turbo_flow_resolved_config_t *resolved, uint64_t rollback_timeout_ms,
    turbo_flow_plugin_host_t **host_out, turbo_flow_plugin_error_t *error) {
  turbo_flow_plugin_host_t *host = NULL;
  size_t count = 0u;
  int rc;
  if (host_out) *host_out = NULL;
  if (!host_config || !resolved || !host_out || !flow_plugin_error_valid(error))
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   NULL, "invalid configured PluginHost arguments");
  rc = turbo_flow_resolved_config_plugin_count(resolved, &count);
  if (rc != SALTS_OK)
    return flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL, NULL,
                                   "resolved plugin manifest is invalid");
  rc = turbo_flow_plugin_host_create(host_config, &host, error);
  if (rc != SALTS_OK) return rc;
  if (count > host->config.module_capacity) {
    turbo_flow_plugin_error_t cleanup_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    rc = flow_plugin_error_write(error, SALTS_ENOSPC, TURBO_FLOW_PLUGIN_STAGE_CAPACITY, NULL, NULL,
                                 "configured plugin count exceeds host module capacity");
    if (turbo_flow_plugin_host_destroy(host, rollback_timeout_ms, &cleanup_error) != SALTS_OK) {
      *host_out = host;
      *error = cleanup_error;
      return cleanup_error.status;
    }
    return rc;
  }
  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resolved_plugin_view_t plugin = TURBO_FLOW_RESOLVED_PLUGIN_VIEW_INIT;
    rc = turbo_flow_resolved_config_plugin_at(resolved, i, &plugin);
    if (rc != SALTS_OK)
      (void)flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL, NULL,
                                    "resolved plugin manifest projection failed");
    if (rc == SALTS_OK)
      rc = flow_plugin_host_load_expected(host, plugin.path, plugin.id, plugin.version, error);
    if (rc != SALTS_OK) {
      turbo_flow_plugin_error_t load_error = *error;
      turbo_flow_plugin_error_t rollback_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      const int rollback_rc =
          turbo_flow_plugin_host_destroy(host, rollback_timeout_ms, &rollback_error);
      if (rollback_rc != SALTS_OK) {
        *host_out = host;
        *error = rollback_error;
        return rollback_rc;
      }
      *error = load_error;
      return rc;
    }
  }
  *host_out = host;
  return SALTS_OK;
}

size_t turbo_flow_plugin_host_module_count(const turbo_flow_plugin_host_t *host) {
  return host ? vec_size(&host->modules) : 0u;
}

size_t turbo_flow_plugin_host_adapter_provider_count(const turbo_flow_plugin_host_t *host) {
  return host ? vec_size(&host->adapter_providers) : 0u;
}

size_t turbo_flow_plugin_host_resource_provider_count(const turbo_flow_plugin_host_t *host) {
  return host ? vec_size(&host->resource_providers) : 0u;
}

size_t
turbo_flow_plugin_host_transactional_adapter_provider_count(const turbo_flow_plugin_host_t *host) {
  return host ? vec_size(&host->transactional_adapter_providers) : 0u;
}

size_t
turbo_flow_plugin_host_transactional_resource_provider_count(const turbo_flow_plugin_host_t *host) {
  return host ? vec_size(&host->transactional_resource_providers) : 0u;
}

static int flow_plugin_snapshot_vectors_initialize(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                                   size_t adapters, size_t resources,
                                                   size_t protocols, size_t businesses,
                                                   size_t transactional_adapters,
                                                   size_t transactional_resources, size_t schemas,
                                                   size_t operations) {
  int rc = turbo_flow_stl_error(vec_init_bytes(&snapshot->adapter_providers,
                                               sizeof(turbo_flow_product_adapter_provider_t),
                                               _Alignof(turbo_flow_max_align_t), adapters));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&snapshot->operations,
                                           sizeof(turbo_flow_plugin_operation_catalog_entry_v3_t),
                                           _Alignof(turbo_flow_max_align_t), operations));
  if (rc != SALTS_OK) return rc;
  if (operations) {
    rc = turbo_flow_stl_error(vec_reserve(&snapshot->operations, operations));
    if (rc != SALTS_OK) return rc;
  }
  rc = turbo_flow_stl_error(vec_init_bytes(&snapshot->schemas,
                                           sizeof(turbo_flow_plugin_schema_v1_t),
                                           _Alignof(turbo_flow_max_align_t), schemas));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&snapshot->resource_providers,
                                           sizeof(turbo_flow_product_resource_provider_t),
                                           _Alignof(turbo_flow_max_align_t), resources));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&snapshot->protocol_providers,
                                           sizeof(turbo_flow_protocol_plugin_api_t),
                                           _Alignof(turbo_flow_max_align_t), protocols));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(vec_init_bytes(&snapshot->business_providers,
                                           sizeof(turbo_flow_protocol_business_plugin_api_t),
                                           _Alignof(turbo_flow_max_align_t), businesses));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&snapshot->transactional_adapter_providers,
                     sizeof(turbo_flow_plugin_transactional_adapter_provider_v1_t),
                     _Alignof(turbo_flow_max_align_t), transactional_adapters));
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&snapshot->transactional_resource_providers,
                     sizeof(turbo_flow_plugin_transactional_resource_provider_v1_t),
                     _Alignof(turbo_flow_max_align_t), transactional_resources));
  if (rc != SALTS_OK) return rc;
  if (adapters > 0u) rc = turbo_flow_stl_error(vec_reserve(&snapshot->adapter_providers, adapters));
  if (rc == SALTS_OK && resources > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&snapshot->resource_providers, resources));
  if (rc == SALTS_OK && protocols > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&snapshot->protocol_providers, protocols));
  if (rc == SALTS_OK && businesses > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&snapshot->business_providers, businesses));
  if (rc == SALTS_OK && transactional_adapters > 0u)
    rc = turbo_flow_stl_error(
        vec_reserve(&snapshot->transactional_adapter_providers, transactional_adapters));
  if (rc == SALTS_OK && transactional_resources > 0u)
    rc = turbo_flow_stl_error(
        vec_reserve(&snapshot->transactional_resource_providers, transactional_resources));
  if (rc == SALTS_OK && schemas > 0u)
    rc = turbo_flow_stl_error(vec_reserve(&snapshot->schemas, schemas));
  return rc;
}

static void flow_plugin_snapshot_vectors_destroy(turbo_flow_plugin_catalog_snapshot_t *snapshot) {
  if (!snapshot) return;
  vec_destroy(&snapshot->operations);
  vec_destroy(&snapshot->schemas);
  vec_destroy(&snapshot->transactional_resource_providers);
  vec_destroy(&snapshot->transactional_adapter_providers);
  vec_destroy(&snapshot->business_providers);
  vec_destroy(&snapshot->protocol_providers);
  vec_destroy(&snapshot->resource_providers);
  vec_destroy(&snapshot->adapter_providers);
}

int turbo_flow_plugin_catalog_snapshot_create(turbo_flow_plugin_host_t *host,
                                              turbo_flow_plugin_catalog_snapshot_t **snapshot_out,
                                              turbo_flow_plugin_error_t *error) {
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  int rc;
  if (snapshot_out) *snapshot_out = NULL;
  if (!host || !snapshot_out || !flow_plugin_error_valid(error))
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   NULL, "invalid catalog snapshot arguments");
  flow_plugin_error_clear(error);
  if (host->state != FLOW_PLUGIN_HOST_ACTIVE)
    return flow_plugin_error_write(error, SALTS_EBUSY, TURBO_FLOW_PLUGIN_STAGE_STATE, NULL, NULL,
                                   "PluginHost is shutting down");
  for (size_t i = 0u; i < vec_size(&host->modules); ++i) {
    const flow_plugin_module_t *module = flow_plugin_module_at_const(host, i);
    if (!module || module->leases == SIZE_MAX)
      return flow_plugin_error_write(error, SALTS_ENOSPC, TURBO_FLOW_PLUGIN_STAGE_LEASE, NULL, NULL,
                                     "plugin module lease capacity is exhausted");
  }
  if (host->active_snapshots == SIZE_MAX)
    return flow_plugin_error_write(error, SALTS_ENOSPC, TURBO_FLOW_PLUGIN_STAGE_LEASE, NULL, NULL,
                                   "catalog snapshot capacity is exhausted");
  snapshot = (turbo_flow_plugin_catalog_snapshot_t *)calloc(1u, sizeof(*snapshot));
  if (!snapshot)
    return flow_plugin_error_write(error, SALTS_ENOMEM, TURBO_FLOW_PLUGIN_STAGE_LEASE, NULL, NULL,
                                   "failed to allocate catalog snapshot");
  rc = flow_plugin_snapshot_vectors_initialize(
      snapshot, vec_size(&host->adapter_providers), vec_size(&host->resource_providers),
      vec_size(&host->protocol_providers), vec_size(&host->business_providers),
      vec_size(&host->transactional_adapter_providers),
      vec_size(&host->transactional_resource_providers), vec_size(&host->schemas),
      vec_size(&host->operations));
  if (rc != SALTS_OK) goto allocation_failed;
  for (size_t i = 0u; i < vec_size(&host->adapter_providers); ++i) {
    const flow_plugin_adapter_provider_t *entry =
        (const flow_plugin_adapter_provider_t *)vec_at_const(&host->adapter_providers, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(vec_push(&snapshot->adapter_providers, &entry->provider));
    if (rc != SALTS_OK) {
      goto allocation_failed;
    }
  }
  for (size_t i = 0u; i < vec_size(&host->resource_providers); ++i) {
    const flow_plugin_resource_provider_t *entry =
        (const flow_plugin_resource_provider_t *)vec_at_const(&host->resource_providers, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(vec_push(&snapshot->resource_providers, &entry->provider));
    if (rc != SALTS_OK) {
      goto allocation_failed;
    }
  }
  for (size_t i = 0u; i < vec_size(&host->protocol_providers); ++i) {
    const flow_plugin_protocol_provider_t *entry =
        (const flow_plugin_protocol_provider_t *)vec_at_const(&host->protocol_providers, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(vec_push(&snapshot->protocol_providers, &entry->provider));
    if (rc != SALTS_OK) goto allocation_failed;
  }
  for (size_t i = 0u; i < vec_size(&host->business_providers); ++i) {
    const flow_plugin_business_provider_t *entry =
        (const flow_plugin_business_provider_t *)vec_at_const(&host->business_providers, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(vec_push(&snapshot->business_providers, &entry->provider));
    if (rc != SALTS_OK) goto allocation_failed;
  }
  for (size_t i = 0u; i < vec_size(&host->transactional_adapter_providers); ++i) {
    const flow_plugin_transactional_adapter_provider_t *entry =
        (const flow_plugin_transactional_adapter_provider_t *)vec_at_const(
            &host->transactional_adapter_providers, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(
        vec_push(&snapshot->transactional_adapter_providers, &entry->provider));
    if (rc != SALTS_OK) goto allocation_failed;
  }
  for (size_t i = 0u; i < vec_size(&host->transactional_resource_providers); ++i) {
    const flow_plugin_transactional_resource_provider_t *entry =
        (const flow_plugin_transactional_resource_provider_t *)vec_at_const(
            &host->transactional_resource_providers, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(
        vec_push(&snapshot->transactional_resource_providers, &entry->provider));
    if (rc != SALTS_OK) goto allocation_failed;
  }
  for (size_t i = 0u; i < vec_size(&host->schemas); ++i) {
    const flow_plugin_schema_t *entry =
        (const flow_plugin_schema_t *)vec_at_const(&host->schemas, i);
    if (!entry) {
      rc = SALTS_EPROTO;
      goto allocation_failed;
    }
    rc = turbo_flow_stl_error(vec_push(&snapshot->schemas, &entry->schema));
    if (rc != SALTS_OK) goto allocation_failed;
  }
  for (size_t i = 0; i < vec_size(&host->operations); ++i) {
    const flow_plugin_operation_t *entry = vec_at_const(&host->operations, i);
    const flow_plugin_module_t *module = flow_plugin_module_at_const(host, entry->module_index);
    turbo_flow_plugin_operation_catalog_entry_v3_t copy;
    copy.plugin_id = module->api->plugin_id;
    copy.operation = entry->operation;
    rc = turbo_flow_stl_error(vec_push(&snapshot->operations, &copy));
    if (rc != SALTS_OK) goto allocation_failed;
  }
  snapshot->host = host;
  snapshot->leased_module_count = vec_size(&host->modules);
  snapshot->references = 1u;
  for (size_t i = 0u; i < snapshot->leased_module_count; ++i) {
    flow_plugin_module_t *module = flow_plugin_module_at(host, i);
    module->leases++;
  }
  host->active_snapshots++;
  *snapshot_out = snapshot;
  return SALTS_OK;

allocation_failed:
  flow_plugin_snapshot_vectors_destroy(snapshot);
  free(snapshot);
  return flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_LEASE, NULL, NULL,
                                 "failed to copy bounded plugin catalog");
}

int turbo_flow_plugin_catalog_snapshot_product_registry(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_product_provider_registry_t *registry_out) {
  if (!snapshot || !registry_out || registry_out->size < sizeof(*registry_out)) return SALTS_EINVAL;
  registry_out->adapter_providers =
      (const turbo_flow_product_adapter_provider_t *)vec_data_const(&snapshot->adapter_providers);
  registry_out->adapter_provider_count = vec_size(&snapshot->adapter_providers);
  registry_out->resource_providers =
      (const turbo_flow_product_resource_provider_t *)vec_data_const(&snapshot->resource_providers);
  registry_out->resource_provider_count = vec_size(&snapshot->resource_providers);
  return SALTS_OK;
}

int turbo_flow_plugin_catalog_snapshot_schema_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_schema_catalog_v1_t *catalog_out) {
  if (!snapshot || snapshot->references == 0u || !catalog_out ||
      catalog_out->size != sizeof(*catalog_out) ||
      catalog_out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      catalog_out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  catalog_out->abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  catalog_out->schemas =
      (const turbo_flow_plugin_schema_v1_t *)vec_data_const(&snapshot->schemas);
  catalog_out->schema_count = vec_size(&snapshot->schemas);
  return SALTS_OK;
}

int turbo_flow_plugin_catalog_snapshot_operation_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_operation_catalog_v3_t *out) {
  if (!out || out->size != sizeof(*out) || out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  turbo_flow_plugin_operation_catalog_v3_init(out);
  if (!snapshot || !snapshot->references) return SALTS_EINVAL;
  out->entries = vec_data_const(&snapshot->operations);
  out->count = vec_size(&snapshot->operations);
  return SALTS_OK;
}

int turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_transactional_product_catalog_v1_t *catalog_out) {
  if (!snapshot || snapshot->references == 0u || !catalog_out ||
      catalog_out->size != sizeof(*catalog_out) ||
      catalog_out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      catalog_out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  catalog_out->abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  catalog_out->adapter_providers =
      (const turbo_flow_plugin_transactional_adapter_provider_v1_t *)vec_data_const(
          &snapshot->transactional_adapter_providers);
  catalog_out->adapter_provider_count = vec_size(&snapshot->transactional_adapter_providers);
  catalog_out->resource_providers =
      (const turbo_flow_plugin_transactional_resource_provider_v1_t *)vec_data_const(
          &snapshot->transactional_resource_providers);
  catalog_out->resource_provider_count = vec_size(&snapshot->transactional_resource_providers);
  return SALTS_OK;
}

void turbo_flow_plugin_catalog_snapshot_destroy(turbo_flow_plugin_catalog_snapshot_t *snapshot) {
  turbo_flow_plugin_host_t *host;
  if (!snapshot) return;
  if (snapshot->references > 1u) {
    snapshot->references--;
    return;
  }
  host = snapshot->host;
  if (host) {
    for (size_t i = 0u; i < snapshot->leased_module_count; ++i) {
      flow_plugin_module_t *module = flow_plugin_module_at(host, i);
      if (module && module->leases > 0u) module->leases--;
    }
    if (host->active_snapshots > 0u) host->active_snapshots--;
  }
  flow_plugin_snapshot_vectors_destroy(snapshot);
  memset(snapshot, 0, sizeof(*snapshot));
  free(snapshot);
}

int turbo_flow_plugin_catalog_snapshot_retain(turbo_flow_plugin_catalog_snapshot_t *snapshot) {
  if (!snapshot || snapshot->references == 0u) return SALTS_EINVAL;
  if (snapshot->references == SIZE_MAX) return SALTS_ENOSPC;
  snapshot->references++;
  return SALTS_OK;
}

int turbo_flow_plugin_catalog_snapshot_protocol_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_protocol_catalog_v1_t *catalog_out) {
  if (!snapshot || snapshot->references == 0u || !catalog_out ||
      catalog_out->size != sizeof(*catalog_out) ||
      catalog_out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      catalog_out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  catalog_out->abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
  catalog_out->protocol_providers =
      (const turbo_flow_protocol_plugin_api_t *)vec_data_const(&snapshot->protocol_providers);
  catalog_out->protocol_provider_count = vec_size(&snapshot->protocol_providers);
  catalog_out->business_providers =
      (const turbo_flow_protocol_business_plugin_api_t *)vec_data_const(
          &snapshot->business_providers);
  catalog_out->business_provider_count = vec_size(&snapshot->business_providers);
  return SALTS_OK;
}

int turbo_flow_plugin_host_destroy(turbo_flow_plugin_host_t *host, uint64_t quiesce_timeout_ms,
                                   turbo_flow_plugin_error_t *error) {
  if (!host || !flow_plugin_error_valid(error))
    return flow_plugin_error_write(error, SALTS_EINVAL, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT, NULL,
                                   NULL, "invalid PluginHost destroy arguments");
  flow_plugin_error_clear(error);
  if (host->active_snapshots != 0u) {
    return flow_plugin_error_write(error, SALTS_EBUSY, TURBO_FLOW_PLUGIN_STAGE_LEASE, NULL, NULL,
                                   "catalog snapshots still retain plugin modules");
  }
  for (size_t i = 0u; i < vec_size(&host->modules); ++i) {
    const flow_plugin_module_t *module = flow_plugin_module_at_const(host, i);
    if (module && module->leases != 0u) {
      return flow_plugin_error_write(error, SALTS_EBUSY, TURBO_FLOW_PLUGIN_STAGE_LEASE,
                                     module->plugin_id, NULL,
                                     "plugin module still has active generation leases");
    }
  }
  host->state = FLOW_PLUGIN_HOST_SHUTTING_DOWN;
  for (size_t i = vec_size(&host->modules); i > 0u; --i) {
    flow_plugin_module_t *module = flow_plugin_module_at(host, i - 1u);
    int rc;
    if (!module || module->state != FLOW_PLUGIN_MODULE_ACTIVE) continue;
    rc = module->api->quiesce(module->plugin, quiesce_timeout_ms);
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_QUIESCE, module->plugin_id, rc);
    if (rc != SALTS_OK)
      return flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_QUIESCE, module->plugin_id,
                                     NULL, "plugin quiesce callback failed");
    module->state = FLOW_PLUGIN_MODULE_QUIESCED;
  }
  for (size_t i = vec_size(&host->modules); i > 0u; --i) {
    flow_plugin_module_t *module = flow_plugin_module_at(host, i - 1u);
    int rc;
    if (!module || module->state != FLOW_PLUGIN_MODULE_QUIESCED) continue;
    rc = module->api->shutdown(module->plugin);
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN, module->plugin_id, rc);
    if (rc != SALTS_OK)
      return flow_plugin_error_write(error, rc, TURBO_FLOW_PLUGIN_STAGE_SHUTDOWN, module->plugin_id,
                                     NULL, "plugin shutdown callback failed");
    module->state = FLOW_PLUGIN_MODULE_SHUTDOWN;
  }
  for (size_t i = vec_size(&host->modules); i > 0u; --i) {
    flow_plugin_module_t *module = flow_plugin_module_at(host, i - 1u);
    if (!module || module->state == FLOW_PLUGIN_MODULE_UNLOADED) continue;
    module->api->destroy(module->plugin);
    module->plugin = NULL;
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY, module->plugin_id, SALTS_OK);
    flow_plugin_module_close(module->handle);
    module->handle = NULL;
    module->api = NULL;
    module->state = FLOW_PLUGIN_MODULE_UNLOADED;
    flow_plugin_observe(host, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD, module->plugin_id, SALTS_OK);
  }
  flow_plugin_vectors_destroy(host);
  memset(host, 0, sizeof(*host));
  free(host);
  return SALTS_OK;
}
