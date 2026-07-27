#include "turbo_flow_gateway.h"
#include "turbo_flow_gateway_business.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <windows.h>
typedef HMODULE flow_gateway_module_t;
#else
  #include <dlfcn.h>
typedef void *flow_gateway_module_t;
#endif

typedef struct flow_gateway_entry_s {
  const turbo_flow_gateway_plugin_api_t *api;
  flow_gateway_module_t module;
  size_t active_owners;
} flow_gateway_entry_t;

struct turbo_flow_gateway_registry_s {
  flow_gateway_entry_t *entries;
  size_t count;
  size_t capacity;
};

struct turbo_flow_gateway_owner_s {
  flow_gateway_entry_t *entry;
  turbo_flow_gateway_service_t service;
};

typedef struct flow_gateway_business_entry_s {
  const turbo_flow_gateway_business_plugin_api_t *api;
  flow_gateway_module_t module;
  size_t active_owners;
} flow_gateway_business_entry_t;

struct turbo_flow_gateway_business_registry_s {
  flow_gateway_business_entry_t *entries;
  size_t count;
  size_t capacity;
};

struct turbo_flow_gateway_business_owner_s {
  flow_gateway_business_entry_t *entry;
  turbo_flow_gateway_business_service_t service;
};

static int
flow_gateway_api_validate(const turbo_flow_gateway_plugin_api_t *api) {
  static const turbo_flow_gateway_capabilities_t required =
      TURBO_FLOW_GATEWAY_CAP_INGRESS | TURBO_FLOW_GATEWAY_CAP_EGRESS |
      TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE;
  if (!api || api->size < sizeof(*api) ||
      api->version_major != TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MAJOR ||
      !api->gateway || !api->gateway[0] ||
      api->protocol < TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN ||
      api->protocol > TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808 ||
      (api->capabilities & required) != required || !api->open || !api->close)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_gateway_request_validate(
    const turbo_flow_gateway_open_request_t *request) {
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      request->protocol < TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN ||
      request->protocol > TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808 ||
      !request->topic_prefix || !request->tenant ||
      request->max_frame_size == 0u)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static flow_gateway_entry_t *flow_gateway_registry_entry(
    turbo_flow_gateway_registry_t *registry, const char *gateway) {
  if (!registry || !gateway || !gateway[0]) return NULL;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (strcmp(registry->entries[i].api->gateway, gateway) == 0)
      return &registry->entries[i];
  }
  return NULL;
}

static void flow_gateway_service_cleanup(
    flow_gateway_entry_t *entry, turbo_flow_gateway_service_t *service) {
  if (!entry || !entry->api || !entry->api->close || !service ||
      (!service->instance && !service->owner))
    return;
  entry->api->close(entry->api->ctx, service);
}

static void flow_gateway_reason_write(char *reason, size_t reason_size,
                                      const char *format,
                                      const char *detail) {
  if (!reason || reason_size == 0u) return;
  (void)snprintf(reason, reason_size, format, detail ? detail : "");
}

static flow_gateway_module_t flow_gateway_module_open(const char *path) {
#ifdef _WIN32
  return path ? LoadLibraryA(path) : NULL;
#else
  return path ? dlopen(path, RTLD_NOW | RTLD_LOCAL) : NULL;
#endif
}

static void flow_gateway_module_close(flow_gateway_module_t module) {
  if (!module) return;
#ifdef _WIN32
  (void)FreeLibrary(module);
#else
  (void)dlclose(module);
#endif
}

static void *flow_gateway_module_symbol(flow_gateway_module_t module,
                                        const char *name) {
  if (!module || !name) return NULL;
#ifdef _WIN32
  return (void *)GetProcAddress(module, name);
#else
  return dlsym(module, name);
#endif
}

static const char *flow_gateway_module_error(char *buffer,
                                             size_t buffer_size) {
  if (!buffer || buffer_size == 0u) return NULL;
#ifdef _WIN32
  {
    const DWORD error = GetLastError();
    if (error == 0u)
      buffer[0] = '\0';
    else
      (void)snprintf(buffer, buffer_size,
                     "Win32 dynamic library error %lu",
                     (unsigned long)error);
  }
#else
  {
    const char *error = dlerror();
    (void)snprintf(buffer, buffer_size, "%s", error ? error : "");
  }
#endif
  return buffer;
}

int turbo_flow_gateway_registry_create(
    size_t capacity, turbo_flow_gateway_registry_t **out) {
  turbo_flow_gateway_registry_t *registry;
  if (!out || capacity == 0u ||
      capacity > SIZE_MAX / sizeof(flow_gateway_entry_t))
    return TURBO_EINVAL;
  *out = NULL;
  registry =
      (turbo_flow_gateway_registry_t *)calloc(1u, sizeof(*registry));
  if (!registry) return TURBO_ENOMEM;
  registry->entries =
      (flow_gateway_entry_t *)calloc(capacity, sizeof(*registry->entries));
  if (!registry->entries) {
    free(registry);
    return TURBO_ENOMEM;
  }
  registry->capacity = capacity;
  *out = registry;
  return TURBO_OK;
}

int turbo_flow_gateway_registry_destroy(
    turbo_flow_gateway_registry_t *registry) {
  if (!registry) return TURBO_OK;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (registry->entries[i].active_owners != 0u) return TURBO_EBUSY;
  }
  while (registry->count > 0u) {
    flow_gateway_entry_t *entry = &registry->entries[--registry->count];
    flow_gateway_module_close(entry->module);
    memset(entry, 0, sizeof(*entry));
  }
  free(registry->entries);
  free(registry);
  return TURBO_OK;
}

int turbo_flow_gateway_registry_register(
    turbo_flow_gateway_registry_t *registry,
    const turbo_flow_gateway_plugin_api_t *api) {
  flow_gateway_entry_t *entry;
  const int rc = flow_gateway_api_validate(api);
  if (rc != TURBO_OK || !registry) return TURBO_EINVAL;
  if (flow_gateway_registry_entry(registry, api->gateway))
    return TURBO_EALREADY;
  if (registry->count >= registry->capacity) return TURBO_ENOSPC;
  entry = &registry->entries[registry->count++];
  entry->api = api;
  return TURBO_OK;
}

int turbo_flow_gateway_registry_load(
    turbo_flow_gateway_registry_t *registry, const char *path, char *reason,
    size_t reason_size) {
  flow_gateway_module_t module;
  turbo_flow_gateway_plugin_get_api_fn get_api = NULL;
  const turbo_flow_gateway_plugin_api_t *api;
  char module_error[256];
  void *symbol;
  int rc;
  if (reason && reason_size > 0u) reason[0] = '\0';
  if (!registry || !path || !path[0]) return TURBO_EINVAL;
  module = flow_gateway_module_open(path);
  if (!module) {
    flow_gateway_reason_write(
        reason, reason_size, "failed to load gateway module: %s",
        flow_gateway_module_error(module_error, sizeof(module_error)));
    return TURBO_ENOENT;
  }
  symbol =
      flow_gateway_module_symbol(module, TURBO_FLOW_GATEWAY_PLUGIN_EXPORT_SYMBOL);
  if (!symbol) {
    flow_gateway_reason_write(
        reason, reason_size, "missing canonical gateway symbol: %s",
        flow_gateway_module_error(module_error, sizeof(module_error)));
    flow_gateway_module_close(module);
    return TURBO_ENOENT;
  }
  memcpy(&get_api, &symbol, sizeof(get_api));
  api = get_api();
  rc = flow_gateway_api_validate(api);
  if (rc != TURBO_OK) {
    flow_gateway_reason_write(reason, reason_size,
                              "invalid gateway API: %s", path);
    flow_gateway_module_close(module);
    return rc;
  }
  rc = turbo_flow_gateway_registry_register(registry, api);
  if (rc != TURBO_OK) {
    flow_gateway_reason_write(reason, reason_size,
                              "failed to register gateway: %s", api->gateway);
    flow_gateway_module_close(module);
    return rc;
  }
  registry->entries[registry->count - 1u].module = module;
  return TURBO_OK;
}

const turbo_flow_gateway_plugin_api_t *turbo_flow_gateway_registry_find(
    const turbo_flow_gateway_registry_t *registry, const char *gateway) {
  if (!registry || !gateway || !gateway[0]) return NULL;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (strcmp(registry->entries[i].api->gateway, gateway) == 0)
      return registry->entries[i].api;
  }
  return NULL;
}

int turbo_flow_gateway_owner_create_registered(
    turbo_flow_gateway_registry_t *registry, const char *gateway,
    const turbo_flow_gateway_open_request_t *request,
    turbo_flow_gateway_owner_t **out) {
  turbo_flow_gateway_service_t service = TURBO_FLOW_GATEWAY_SERVICE_INIT;
  turbo_flow_gateway_owner_t *owner;
  flow_gateway_entry_t *entry;
  int rc;
  if (out) *out = NULL;
  rc = flow_gateway_request_validate(request);
  if (rc != TURBO_OK || !registry || !gateway || !gateway[0] || !out)
    return TURBO_EINVAL;
  entry = flow_gateway_registry_entry(registry, gateway);
  if (!entry) return TURBO_ENOTSUP;
  if (entry->api->protocol != request->protocol) return TURBO_ENOTSUP;
  service.protocol = request->protocol;
  rc = entry->api->open(entry->api->ctx, request, &service);
  if (rc != TURBO_OK) {
    flow_gateway_service_cleanup(entry, &service);
    return rc;
  }
  if (service.size < sizeof(service) ||
      service.abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      service.protocol != request->protocol || !service.instance ||
      !service.owner) {
    flow_gateway_service_cleanup(entry, &service);
    return TURBO_EPROTO;
  }
  owner = (turbo_flow_gateway_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    flow_gateway_service_cleanup(entry, &service);
    return TURBO_ENOMEM;
  }
  owner->entry = entry;
  owner->service = service;
  entry->active_owners++;
  *out = owner;
  return TURBO_OK;
}

int turbo_flow_gateway_owner_instance(
    const turbo_flow_gateway_owner_t *owner,
    turbo_flow_gateway_protocol_t expected_protocol,
    turbo_flow_gateway_t **out) {
  if (out) *out = NULL;
  if (!owner || !owner->entry || !out ||
      expected_protocol != owner->service.protocol ||
      !owner->service.instance)
    return TURBO_EINVAL;
  *out = owner->service.instance;
  return TURBO_OK;
}

const char *
turbo_flow_gateway_owner_name(const turbo_flow_gateway_owner_t *owner) {
  return owner && owner->entry && owner->entry->api
             ? owner->entry->api->gateway
             : NULL;
}

turbo_flow_gateway_protocol_t
turbo_flow_gateway_owner_protocol(const turbo_flow_gateway_owner_t *owner) {
  return owner ? owner->service.protocol : (turbo_flow_gateway_protocol_t)0;
}

void turbo_flow_gateway_owner_destroy(turbo_flow_gateway_owner_t *owner) {
  if (!owner) return;
  if (owner->entry && owner->entry->api && owner->service.owner) {
    owner->entry->api->close(owner->entry->api->ctx, &owner->service);
    if (owner->entry->active_owners > 0u) owner->entry->active_owners--;
  }
  free(owner);
}

static int flow_gateway_business_api_validate(
    const turbo_flow_gateway_business_plugin_api_t *api) {
  const turbo_flow_gateway_business_capabilities_t known =
      TURBO_FLOW_GATEWAY_BUSINESS_CAP_COMMITTED_EVENT |
      TURBO_FLOW_GATEWAY_BUSINESS_CAP_PREPARE_COMMAND;
  if (!api || api->size < sizeof(*api) ||
      api->version_major !=
          TURBO_FLOW_GATEWAY_BUSINESS_PLUGIN_API_VERSION_MAJOR ||
      !api->business || !api->business[0] ||
      api->protocol < TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN ||
      api->protocol > TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808 ||
      api->capabilities == 0u || (api->capabilities & ~known) != 0u ||
      !api->open || !api->close)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_gateway_business_open_request_validate(
    const turbo_flow_gateway_business_open_request_t *request) {
  size_t tenant_size = 0u;
  size_t profile_size = 0u;
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_GATEWAY_BUSINESS_ABI_VERSION ||
      request->protocol < TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN ||
      request->protocol > TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808 ||
      !request->tenant || !request->profile ||
      request->max_payload_size == 0u)
    return TURBO_EINVAL;
  while (tenant_size <= TURBO_FLOW_GATEWAY_TENANT_MAX &&
         request->tenant[tenant_size] != '\0')
    tenant_size++;
  while (profile_size <= TURBO_FLOW_GATEWAY_BUSINESS_PROFILE_MAX &&
         request->profile[profile_size] != '\0')
    profile_size++;
  if (tenant_size == 0u || tenant_size > TURBO_FLOW_GATEWAY_TENANT_MAX ||
      profile_size == 0u ||
      profile_size > TURBO_FLOW_GATEWAY_BUSINESS_PROFILE_MAX)
    return TURBO_EMSGSIZE;
  for (size_t i = 0u; i < tenant_size; ++i) {
    const unsigned char ch = (unsigned char)request->tenant[i];
    if (ch <= 0x20u || ch >= 0x7fu || ch == '/' || ch == '+' || ch == '#')
      return TURBO_EINVAL;
  }
  for (size_t i = 0u; i < profile_size; ++i) {
    const unsigned char ch = (unsigned char)request->profile[i];
    if (ch < 0x20u || ch == 0x7fu) return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static flow_gateway_business_entry_t *flow_gateway_business_registry_entry(
    turbo_flow_gateway_business_registry_t *registry,
    const char *business) {
  if (!registry || !business || !business[0]) return NULL;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (strcmp(registry->entries[i].api->business, business) == 0)
      return &registry->entries[i];
  }
  return NULL;
}

static void flow_gateway_business_service_cleanup(
    flow_gateway_business_entry_t *entry,
    turbo_flow_gateway_business_service_t *service) {
  if (!entry || !entry->api || !entry->api->close || !service ||
      (!service->instance && !service->owner))
    return;
  entry->api->close(entry->api->ctx, service);
}

int turbo_flow_gateway_business_registry_create(
    size_t capacity, turbo_flow_gateway_business_registry_t **out) {
  turbo_flow_gateway_business_registry_t *registry;
  if (!out || capacity == 0u ||
      capacity > SIZE_MAX / sizeof(flow_gateway_business_entry_t))
    return TURBO_EINVAL;
  *out = NULL;
  registry = (turbo_flow_gateway_business_registry_t *)calloc(
      1u, sizeof(*registry));
  if (!registry) return TURBO_ENOMEM;
  registry->entries = (flow_gateway_business_entry_t *)calloc(
      capacity, sizeof(*registry->entries));
  if (!registry->entries) {
    free(registry);
    return TURBO_ENOMEM;
  }
  registry->capacity = capacity;
  *out = registry;
  return TURBO_OK;
}

int turbo_flow_gateway_business_registry_destroy(
    turbo_flow_gateway_business_registry_t *registry) {
  if (!registry) return TURBO_OK;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (registry->entries[i].active_owners != 0u) return TURBO_EBUSY;
  }
  while (registry->count > 0u) {
    flow_gateway_business_entry_t *entry =
        &registry->entries[--registry->count];
    flow_gateway_module_close(entry->module);
    memset(entry, 0, sizeof(*entry));
  }
  free(registry->entries);
  free(registry);
  return TURBO_OK;
}

int turbo_flow_gateway_business_registry_register(
    turbo_flow_gateway_business_registry_t *registry,
    const turbo_flow_gateway_business_plugin_api_t *api) {
  flow_gateway_business_entry_t *entry;
  const int rc = flow_gateway_business_api_validate(api);
  if (rc != TURBO_OK || !registry) return TURBO_EINVAL;
  if (flow_gateway_business_registry_entry(registry, api->business))
    return TURBO_EALREADY;
  if (registry->count >= registry->capacity) return TURBO_ENOSPC;
  entry = &registry->entries[registry->count++];
  entry->api = api;
  return TURBO_OK;
}

int turbo_flow_gateway_business_registry_load(
    turbo_flow_gateway_business_registry_t *registry, const char *path,
    char *reason, size_t reason_size) {
  flow_gateway_module_t module;
  turbo_flow_gateway_business_plugin_get_api_fn get_api = NULL;
  const turbo_flow_gateway_business_plugin_api_t *api;
  char module_error[256];
  void *symbol;
  int rc;
  if (reason && reason_size > 0u) reason[0] = '\0';
  if (!registry || !path || !path[0]) return TURBO_EINVAL;
  module = flow_gateway_module_open(path);
  if (!module) {
    flow_gateway_reason_write(
        reason, reason_size, "failed to load business module: %s",
        flow_gateway_module_error(module_error, sizeof(module_error)));
    return TURBO_ENOENT;
  }
  symbol = flow_gateway_module_symbol(
      module, TURBO_FLOW_GATEWAY_BUSINESS_PLUGIN_EXPORT_SYMBOL);
  if (!symbol) {
    flow_gateway_reason_write(
        reason, reason_size, "missing canonical business symbol: %s",
        flow_gateway_module_error(module_error, sizeof(module_error)));
    flow_gateway_module_close(module);
    return TURBO_ENOENT;
  }
  memcpy(&get_api, &symbol, sizeof(get_api));
  api = get_api();
  rc = flow_gateway_business_api_validate(api);
  if (rc != TURBO_OK) {
    flow_gateway_reason_write(reason, reason_size,
                              "invalid business API: %s", path);
    flow_gateway_module_close(module);
    return rc;
  }
  rc = turbo_flow_gateway_business_registry_register(registry, api);
  if (rc != TURBO_OK) {
    flow_gateway_reason_write(reason, reason_size,
                              "failed to register business: %s",
                              api->business);
    flow_gateway_module_close(module);
    return rc;
  }
  registry->entries[registry->count - 1u].module = module;
  return TURBO_OK;
}

const turbo_flow_gateway_business_plugin_api_t *
turbo_flow_gateway_business_registry_find(
    const turbo_flow_gateway_business_registry_t *registry,
    const char *business) {
  if (!registry || !business || !business[0]) return NULL;
  for (size_t i = 0u; i < registry->count; ++i) {
    if (strcmp(registry->entries[i].api->business, business) == 0)
      return registry->entries[i].api;
  }
  return NULL;
}

int turbo_flow_gateway_business_owner_create_registered(
    turbo_flow_gateway_business_registry_t *registry, const char *business,
    const turbo_flow_gateway_business_open_request_t *request,
    turbo_flow_gateway_business_owner_t **out) {
  turbo_flow_gateway_business_service_t service =
      TURBO_FLOW_GATEWAY_BUSINESS_SERVICE_INIT;
  turbo_flow_gateway_business_info_t info =
      TURBO_FLOW_GATEWAY_BUSINESS_INFO_INIT;
  turbo_flow_gateway_business_owner_t *owner;
  flow_gateway_business_entry_t *entry;
  int rc;
  if (out) *out = NULL;
  rc = flow_gateway_business_open_request_validate(request);
  if (rc != TURBO_OK || !registry || !business || !business[0] || !out)
    return rc != TURBO_OK ? rc : TURBO_EINVAL;
  entry = flow_gateway_business_registry_entry(registry, business);
  if (!entry) return TURBO_ENOTSUP;
  if (entry->api->protocol != request->protocol) return TURBO_ENOTSUP;
  service.protocol = request->protocol;
  rc = entry->api->open(entry->api->ctx, request, &service);
  if (rc != TURBO_OK) {
    flow_gateway_business_service_cleanup(entry, &service);
    return rc;
  }
  if (service.size < sizeof(service) ||
      service.abi_version != TURBO_FLOW_GATEWAY_BUSINESS_ABI_VERSION ||
      service.protocol != request->protocol || !service.instance ||
      !service.owner) {
    flow_gateway_business_service_cleanup(entry, &service);
    return TURBO_EPROTO;
  }
  rc = turbo_flow_gateway_business_get_info(service.instance, &info);
  if (rc != TURBO_OK || info.protocol != request->protocol ||
      info.capabilities != entry->api->capabilities ||
      strcmp(info.business, entry->api->business) != 0 ||
      info.max_payload_size > request->max_payload_size) {
    flow_gateway_business_service_cleanup(entry, &service);
    return TURBO_EPROTO;
  }
  owner =
      (turbo_flow_gateway_business_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    flow_gateway_business_service_cleanup(entry, &service);
    return TURBO_ENOMEM;
  }
  owner->entry = entry;
  owner->service = service;
  entry->active_owners++;
  *out = owner;
  return TURBO_OK;
}

int turbo_flow_gateway_business_owner_instance(
    const turbo_flow_gateway_business_owner_t *owner,
    turbo_flow_gateway_protocol_t expected_protocol,
    turbo_flow_gateway_business_t **out) {
  if (out) *out = NULL;
  if (!owner || !owner->entry || !out ||
      expected_protocol != owner->service.protocol ||
      !owner->service.instance)
    return TURBO_EINVAL;
  *out = owner->service.instance;
  return TURBO_OK;
}

void turbo_flow_gateway_business_owner_destroy(
    turbo_flow_gateway_business_owner_t *owner) {
  if (!owner) return;
  if (owner->entry && owner->entry->api && owner->service.owner) {
    owner->entry->api->close(owner->entry->api->ctx, &owner->service);
    if (owner->entry->active_owners > 0u)
      owner->entry->active_owners--;
  }
  free(owner);
}
