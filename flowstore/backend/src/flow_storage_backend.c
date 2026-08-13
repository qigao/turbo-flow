#include "turbo_flow_storage_backend.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <windows.h>
typedef HMODULE flow_storage_module_t;
#else
  #include <dlfcn.h>
typedef void *flow_storage_module_t;
#endif

typedef struct flow_storage_backend_entry_s {
  const turbo_flow_storage_backend_plugin_api_t *api;
  flow_storage_module_t module;
  size_t active_owners;
} flow_storage_backend_entry_t;

struct turbo_flow_storage_backend_registry_s {
  flow_storage_backend_entry_t *entries;
  size_t count;
  size_t capacity;
};

struct turbo_flow_storage_backend_owner_s {
  flow_storage_backend_entry_t *entry;
  turbo_flow_storage_backend_service_t service;
};

/* An open implementation owns any partially initialized service after it has
 * returned control.  Give it one cleanup path on every failure edge. */
static void flow_storage_service_cleanup(flow_storage_backend_entry_t *entry,
                                         turbo_flow_storage_backend_service_t *service) {
  if (!entry || !entry->api || !entry->api->close || !service ||
      (!service->instance && !service->owner)) {
    return;
  }
  entry->api->close(entry->api->ctx, service);
}

static turbo_flow_storage_capabilities_t
flow_storage_model_capability(turbo_flow_storage_model_t model) {
  switch (model) {
  case TURBO_FLOW_STORAGE_MODEL_RECORD:
    return TURBO_FLOW_STORAGE_CAP_RECORD;
  case TURBO_FLOW_STORAGE_MODEL_STATE:
    return TURBO_FLOW_STORAGE_CAP_STATE;
  case TURBO_FLOW_STORAGE_MODEL_INDEX:
    return TURBO_FLOW_STORAGE_CAP_INDEX;
  case TURBO_FLOW_STORAGE_MODEL_LOG:
    return TURBO_FLOW_STORAGE_CAP_LOG;
  case TURBO_FLOW_STORAGE_MODEL_SERIES:
    return TURBO_FLOW_STORAGE_CAP_SERIES;
  default:
    return 0u;
  }
}

static int flow_storage_api_validate(const turbo_flow_storage_backend_plugin_api_t *api) {
  if (!api || api->size < sizeof(*api) ||
      api->version_major != TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR ||
      !api->backend || !api->backend[0] || api->capabilities == 0u || !api->open || !api->close) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flow_storage_request_validate(
    const turbo_flow_storage_backend_open_request_t *request) {
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION ||
      flow_storage_model_capability(request->model) == 0u ||
      (request->options_size > 0u && !request->options)) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static flow_storage_backend_entry_t *
flow_storage_registry_entry(turbo_flow_storage_backend_registry_t *registry,
                            const char *backend) {
  if (!registry || !backend || !backend[0]) return NULL;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (strcmp(registry->entries[i].api->backend, backend) == 0) return &registry->entries[i];
  }
  return NULL;
}

static void flow_storage_reason_write(char *reason, size_t reason_size, const char *format,
                                      const char *detail) {
  if (!reason || reason_size == 0u) return;
  (void)snprintf(reason, reason_size, format, detail ? detail : "");
}

static flow_storage_module_t flow_storage_module_open(const char *path) {
#ifdef _WIN32
  return path ? LoadLibraryA(path) : NULL;
#else
  return path ? dlopen(path, RTLD_NOW | RTLD_LOCAL) : NULL;
#endif
}

static void flow_storage_module_close(flow_storage_module_t module) {
  if (!module) return;
#ifdef _WIN32
  (void)FreeLibrary(module);
#else
  (void)dlclose(module);
#endif
}

static void *flow_storage_module_symbol(flow_storage_module_t module, const char *name) {
  if (!module || !name) return NULL;
#ifdef _WIN32
  return (void *)GetProcAddress(module, name);
#else
  return dlsym(module, name);
#endif
}

static const char *flow_storage_module_error(char *buffer, size_t buffer_size) {
  if (!buffer || buffer_size == 0u) return NULL;
#ifdef _WIN32
  {
    DWORD error = GetLastError();
    if (error == 0u) {
      buffer[0] = '\0';
    } else {
      (void)snprintf(buffer, buffer_size, "Win32 dynamic library error %lu",
                     (unsigned long)error);
    }
  }
#else
  {
    const char *error = dlerror();
    (void)snprintf(buffer, buffer_size, "%s", error ? error : "");
  }
#endif
  return buffer;
}

int turbo_flow_storage_backend_registry_create(
    size_t capacity, turbo_flow_storage_backend_registry_t **out) {
  turbo_flow_storage_backend_registry_t *registry;
  if (!out || capacity == 0u || capacity > SIZE_MAX / sizeof(flow_storage_backend_entry_t)) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  registry = (turbo_flow_storage_backend_registry_t *)calloc(1u, sizeof(*registry));
  if (!registry) return TURBO_ENOMEM;
  registry->entries =
      (flow_storage_backend_entry_t *)calloc(capacity, sizeof(*registry->entries));
  if (!registry->entries) {
    free(registry);
    return TURBO_ENOMEM;
  }
  registry->capacity = capacity;
  *out = registry;
  return TURBO_OK;
}

int turbo_flow_storage_backend_registry_destroy(
    turbo_flow_storage_backend_registry_t *registry) {
  if (!registry) return TURBO_OK;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (registry->entries[i].active_owners != 0u) return TURBO_EBUSY;
  }
  while (registry->count > 0u) {
    flow_storage_backend_entry_t *entry = &registry->entries[--registry->count];
    flow_storage_module_close(entry->module);
    memset(entry, 0, sizeof(*entry));
  }
  free(registry->entries);
  free(registry);
  return TURBO_OK;
}

int turbo_flow_storage_backend_registry_register(
    turbo_flow_storage_backend_registry_t *registry,
    const turbo_flow_storage_backend_plugin_api_t *api) {
  flow_storage_backend_entry_t *entry;
  int rc = flow_storage_api_validate(api);
  if (rc != TURBO_OK || !registry) return TURBO_EINVAL;
  if (flow_storage_registry_entry(registry, api->backend)) return TURBO_EALREADY;
  if (registry->count >= registry->capacity) return TURBO_ENOSPC;
  entry = &registry->entries[registry->count++];
  entry->api = api;
  return TURBO_OK;
}

int turbo_flow_storage_backend_registry_load(
    turbo_flow_storage_backend_registry_t *registry, const char *path, char *reason,
    size_t reason_size) {
  flow_storage_module_t module;
  turbo_flow_storage_backend_plugin_get_api_fn get_api = NULL;
  const turbo_flow_storage_backend_plugin_api_t *api;
  void *symbol;
  char module_error[256];
  int rc;
  if (reason && reason_size > 0u) reason[0] = '\0';
  if (!registry || !path || !path[0]) return TURBO_EINVAL;
  module = flow_storage_module_open(path);
  if (!module) {
    flow_storage_reason_write(reason, reason_size, "failed to load module: %s",
                              flow_storage_module_error(module_error, sizeof(module_error)));
    return TURBO_ENOENT;
  }
  symbol = flow_storage_module_symbol(module, TURBO_FLOW_STORAGE_BACKEND_PLUGIN_EXPORT_SYMBOL);
  if (!symbol) {
    flow_storage_reason_write(reason, reason_size, "missing canonical storage backend symbol: %s",
                              flow_storage_module_error(module_error, sizeof(module_error)));
    flow_storage_module_close(module);
    return TURBO_ENOENT;
  }
  memcpy(&get_api, &symbol, sizeof(get_api));
  api = get_api();
  rc = flow_storage_api_validate(api);
  if (rc != TURBO_OK) {
    flow_storage_reason_write(reason, reason_size, "invalid storage backend API: %s", path);
    flow_storage_module_close(module);
    return rc;
  }
  rc = turbo_flow_storage_backend_registry_register(registry, api);
  if (rc != TURBO_OK) {
    flow_storage_reason_write(reason, reason_size, "failed to register storage backend: %s",
                              api->backend);
    flow_storage_module_close(module);
    return rc;
  }
  registry->entries[registry->count - 1u].module = module;
  return TURBO_OK;
}

const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_registry_find(const turbo_flow_storage_backend_registry_t *registry,
                                         const char *backend) {
  if (!registry || !backend || !backend[0]) return NULL;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (strcmp(registry->entries[i].api->backend, backend) == 0) {
      return registry->entries[i].api;
    }
  }
  return NULL;
}

int turbo_flow_storage_backend_owner_create_registered(
    turbo_flow_storage_backend_registry_t *registry, const char *backend,
    const turbo_flow_storage_backend_open_request_t *request,
    turbo_flow_storage_backend_owner_t **out, turbo_flow_config_error_t *error) {
  turbo_flow_storage_backend_service_t service = TURBO_FLOW_STORAGE_BACKEND_SERVICE_INIT;
  turbo_flow_storage_backend_owner_t *owner;
  flow_storage_backend_entry_t *entry;
  turbo_flow_storage_capabilities_t capability;
  int rc;
  if (out) *out = NULL;
  rc = flow_storage_request_validate(request);
  if (rc != TURBO_OK || !registry || !backend || !backend[0] || !out) return TURBO_EINVAL;
  entry = flow_storage_registry_entry(registry, backend);
  if (!entry) return TURBO_ENOTSUP;
  capability = flow_storage_model_capability(request->model);
  if ((entry->api->capabilities & capability) == 0u) return TURBO_ENOTSUP;
  service.model = request->model;
  rc = entry->api->open(entry->api->ctx, request, &service, error);
  if (rc != TURBO_OK) {
    flow_storage_service_cleanup(entry, &service);
    return rc;
  }
  if (service.size < sizeof(service) ||
      service.abi_version != TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION ||
      service.model != request->model || !service.instance || !service.owner) {
    flow_storage_service_cleanup(entry, &service);
    return TURBO_EPROTO;
  }
  owner = (turbo_flow_storage_backend_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    flow_storage_service_cleanup(entry, &service);
    return TURBO_ENOMEM;
  }
  owner->entry = entry;
  owner->service = service;
  entry->active_owners++;
  *out = owner;
  return TURBO_OK;
}

int turbo_flow_storage_backend_owner_service(
    const turbo_flow_storage_backend_owner_t *owner, turbo_flow_storage_model_t expected_model,
    void **out) {
  if (out) *out = NULL;
  if (!owner || !owner->entry || !out || expected_model != owner->service.model ||
      !owner->service.instance) {
    return TURBO_EINVAL;
  }
  *out = owner->service.instance;
  return TURBO_OK;
}

const char *
turbo_flow_storage_backend_owner_backend(const turbo_flow_storage_backend_owner_t *owner) {
  return owner && owner->entry && owner->entry->api ? owner->entry->api->backend : NULL;
}

turbo_flow_storage_model_t
turbo_flow_storage_backend_owner_model(const turbo_flow_storage_backend_owner_t *owner) {
  return owner ? owner->service.model : (turbo_flow_storage_model_t)0;
}

void turbo_flow_storage_backend_owner_destroy(turbo_flow_storage_backend_owner_t *owner) {
  if (!owner) return;
  if (owner->entry && owner->entry->api && owner->service.owner) {
    owner->entry->api->close(owner->entry->api->ctx, &owner->service);
    if (owner->entry->active_owners > 0u) owner->entry->active_owners--;
  }
  free(owner);
}
