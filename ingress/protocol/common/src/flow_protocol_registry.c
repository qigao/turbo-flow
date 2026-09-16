#include "turbo_flow_plugin_protocol.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_business.h"

#include "salts_error.h"
#include "turbo_flow_stl_error_internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_protocol_entry_s {
  const turbo_flow_protocol_plugin_api_t *api;
  size_t active_owners;
} flow_protocol_entry_t;

struct turbo_flow_protocol_registry_s {
  vec_t entries;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
};

struct turbo_flow_protocol_owner_s {
  flow_protocol_entry_t *entry;
  turbo_flow_protocol_service_t service;
};

typedef struct flow_protocol_business_entry_s {
  const turbo_flow_protocol_business_plugin_api_t *api;
  size_t active_owners;
} flow_protocol_business_entry_t;

struct turbo_flow_protocol_business_registry_s {
  vec_t entries;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
};

struct turbo_flow_protocol_business_owner_s {
  flow_protocol_business_entry_t *entry;
  turbo_flow_protocol_business_service_t service;
};

static int flow_protocol_request_validate(const turbo_flow_protocol_open_request_t *request) {
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      request->protocol < TURBO_FLOW_PROTOCOL_MQTT_SN ||
      request->protocol > TURBO_FLOW_PROTOCOL_JTT_808 || request->max_frame_size == 0u)
    return SALTS_EINVAL;
  return SALTS_OK;
}

static flow_protocol_entry_t *flow_protocol_registry_entry(turbo_flow_protocol_registry_t *registry,
                                                           const char *name) {
  if (!registry || !name || !name[0]) return NULL;
  for (size_t i = 0u; i < vec_size(&registry->entries); ++i) {
    flow_protocol_entry_t *entry = (flow_protocol_entry_t *)vec_at(&registry->entries, i);
    if (entry && strcmp(entry->api->name, name) == 0) return entry;
  }
  return NULL;
}

static void flow_protocol_service_cleanup(flow_protocol_entry_t *entry,
                                          turbo_flow_protocol_service_t *service) {
  if (!entry || !entry->api || !entry->api->close || !service ||
      (!service->instance && !service->owner))
    return;
  entry->api->close(entry->api->ctx, service);
}

int turbo_flow_protocol_registry_create(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                        turbo_flow_protocol_registry_t **out) {
  turbo_flow_plugin_protocol_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
  turbo_flow_protocol_registry_t *registry;
  flow_protocol_entry_t entry;
  int rc;
  if (!out || !snapshot) return SALTS_EINVAL;
  *out = NULL;
  rc = turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, &catalog);
  if (rc != SALTS_OK) return rc;
  if (catalog.protocol_provider_count == 0u) return SALTS_ENOTSUP;
  registry = (turbo_flow_protocol_registry_t *)calloc(1u, sizeof(*registry));
  if (!registry) return SALTS_ENOMEM;
  rc = turbo_flow_stl_error(vec_init_bytes(&registry->entries, sizeof(flow_protocol_entry_t),
                                           _Alignof(turbo_flow_max_align_t),
                                           catalog.protocol_provider_count));
  if (rc == SALTS_OK)
    rc = turbo_flow_stl_error(vec_reserve(&registry->entries, catalog.protocol_provider_count));
  memset(&entry, 0, sizeof(entry));
  for (size_t i = 0u; rc == SALTS_OK && i < catalog.protocol_provider_count; ++i) {
    entry.api = &catalog.protocol_providers[i];
    rc = turbo_flow_stl_error(vec_push(&registry->entries, &entry));
  }
  if (rc != SALTS_OK) {
    vec_destroy(&registry->entries);
    free(registry);
    return rc;
  }
  rc = turbo_flow_plugin_catalog_snapshot_retain(snapshot);
  if (rc != SALTS_OK) {
    vec_destroy(&registry->entries);
    free(registry);
    return rc;
  }
  registry->snapshot = snapshot;
  *out = registry;
  return SALTS_OK;
}

int turbo_flow_protocol_registry_destroy(turbo_flow_protocol_registry_t *registry) {
  if (!registry) return SALTS_OK;
  for (size_t i = 0u; i < vec_size(&registry->entries); ++i) {
    const flow_protocol_entry_t *entry =
        (const flow_protocol_entry_t *)vec_at_const(&registry->entries, i);
    if (entry && entry->active_owners != 0u) return SALTS_EBUSY;
  }
  turbo_flow_plugin_catalog_snapshot_destroy(registry->snapshot);
  vec_destroy(&registry->entries);
  free(registry);
  return SALTS_OK;
}

const turbo_flow_protocol_plugin_api_t *
turbo_flow_protocol_registry_find(const turbo_flow_protocol_registry_t *registry,
                                  const char *name) {
  if (!registry || !name || !name[0]) return NULL;
  for (size_t i = 0u; i < vec_size(&registry->entries); ++i) {
    const flow_protocol_entry_t *entry =
        (const flow_protocol_entry_t *)vec_at_const(&registry->entries, i);
    if (entry && strcmp(entry->api->name, name) == 0) return entry->api;
  }
  return NULL;
}

int turbo_flow_protocol_owner_create_registered(turbo_flow_protocol_registry_t *registry,
                                                const char *name,
                                                const turbo_flow_protocol_open_request_t *request,
                                                turbo_flow_protocol_owner_t **out) {
  turbo_flow_protocol_service_t service = TURBO_FLOW_PROTOCOL_SERVICE_INIT;
  turbo_flow_protocol_owner_t *owner;
  flow_protocol_entry_t *entry;
  int rc;
  if (out) *out = NULL;
  rc = flow_protocol_request_validate(request);
  if (rc != SALTS_OK || !registry || !name || !name[0] || !out) return SALTS_EINVAL;
  entry = flow_protocol_registry_entry(registry, name);
  if (!entry) return SALTS_ENOTSUP;
  if (entry->api->protocol != request->protocol) return SALTS_ENOTSUP;
  service.protocol = request->protocol;
  rc = entry->api->open(entry->api->ctx, request, &service);
  if (rc != SALTS_OK) {
    flow_protocol_service_cleanup(entry, &service);
    return rc;
  }
  if (service.size < sizeof(service) || service.abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      service.protocol != request->protocol || !service.instance || !service.owner) {
    flow_protocol_service_cleanup(entry, &service);
    return SALTS_EPROTO;
  }
  owner = (turbo_flow_protocol_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    flow_protocol_service_cleanup(entry, &service);
    return SALTS_ENOMEM;
  }
  owner->entry = entry;
  owner->service = service;
  entry->active_owners++;
  *out = owner;
  return SALTS_OK;
}

int turbo_flow_protocol_owner_instance(const turbo_flow_protocol_owner_t *owner,
                                       turbo_flow_protocol_kind_t expected_protocol,
                                       turbo_flow_protocol_t **out) {
  if (out) *out = NULL;
  if (!owner || !owner->entry || !out || expected_protocol != owner->service.protocol ||
      !owner->service.instance)
    return SALTS_EINVAL;
  *out = owner->service.instance;
  return SALTS_OK;
}

const char *turbo_flow_protocol_owner_name(const turbo_flow_protocol_owner_t *owner) {
  return owner && owner->entry && owner->entry->api ? owner->entry->api->name : NULL;
}

turbo_flow_protocol_kind_t
turbo_flow_protocol_owner_protocol(const turbo_flow_protocol_owner_t *owner) {
  return owner ? owner->service.protocol : (turbo_flow_protocol_kind_t)0;
}

void turbo_flow_protocol_owner_destroy(turbo_flow_protocol_owner_t *owner) {
  if (!owner) return;
  if (owner->entry && owner->entry->api && owner->service.owner) {
    owner->entry->api->close(owner->entry->api->ctx, &owner->service);
    if (owner->entry->active_owners > 0u) owner->entry->active_owners--;
  }
  free(owner);
}

static int flow_protocol_business_open_request_validate(
    const turbo_flow_protocol_business_open_request_t *request) {
  size_t tenant_size = 0u;
  size_t profile_size = 0u;
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      request->protocol < TURBO_FLOW_PROTOCOL_MQTT_SN ||
      request->protocol > TURBO_FLOW_PROTOCOL_JTT_808 || !request->tenant || !request->profile ||
      request->max_payload_size == 0u)
    return SALTS_EINVAL;
  while (tenant_size <= TURBO_FLOW_PROTOCOL_TENANT_MAX && request->tenant[tenant_size] != '\0')
    tenant_size++;
  while (profile_size <= TURBO_FLOW_PROTOCOL_BUSINESS_PROFILE_MAX &&
         request->profile[profile_size] != '\0')
    profile_size++;
  if (tenant_size == 0u || tenant_size > TURBO_FLOW_PROTOCOL_TENANT_MAX || profile_size == 0u ||
      profile_size > TURBO_FLOW_PROTOCOL_BUSINESS_PROFILE_MAX)
    return SALTS_EMSGSIZE;
  for (size_t i = 0u; i < tenant_size; ++i) {
    const unsigned char ch = (unsigned char)request->tenant[i];
    if (ch <= 0x20u || ch >= 0x7fu || ch == '/' || ch == '+' || ch == '#') return SALTS_EINVAL;
  }
  for (size_t i = 0u; i < profile_size; ++i) {
    const unsigned char ch = (unsigned char)request->profile[i];
    if (ch < 0x20u || ch == 0x7fu) return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static flow_protocol_business_entry_t *
flow_protocol_business_registry_entry(turbo_flow_protocol_business_registry_t *registry,
                                      const char *business) {
  if (!registry || !business || !business[0]) return NULL;
  for (size_t i = 0u; i < vec_size(&registry->entries); ++i) {
    flow_protocol_business_entry_t *entry =
        (flow_protocol_business_entry_t *)vec_at(&registry->entries, i);
    if (entry && strcmp(entry->api->business, business) == 0) return entry;
  }
  return NULL;
}

static void
flow_protocol_business_service_cleanup(flow_protocol_business_entry_t *entry,
                                       turbo_flow_protocol_business_service_t *service) {
  if (!entry || !entry->api || !entry->api->close || !service ||
      (!service->instance && !service->owner))
    return;
  entry->api->close(entry->api->ctx, service);
}

int turbo_flow_protocol_business_registry_create(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                                 turbo_flow_protocol_business_registry_t **out) {
  turbo_flow_plugin_protocol_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
  turbo_flow_protocol_business_registry_t *registry;
  flow_protocol_business_entry_t entry;
  int rc;
  if (!out || !snapshot) return SALTS_EINVAL;
  *out = NULL;
  rc = turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, &catalog);
  if (rc != SALTS_OK) return rc;
  if (catalog.business_provider_count == 0u) return SALTS_ENOTSUP;
  registry = (turbo_flow_protocol_business_registry_t *)calloc(1u, sizeof(*registry));
  if (!registry) return SALTS_ENOMEM;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&registry->entries, sizeof(flow_protocol_business_entry_t),
                     _Alignof(turbo_flow_max_align_t), catalog.business_provider_count));
  if (rc == SALTS_OK)
    rc = turbo_flow_stl_error(vec_reserve(&registry->entries, catalog.business_provider_count));
  memset(&entry, 0, sizeof(entry));
  for (size_t i = 0u; rc == SALTS_OK && i < catalog.business_provider_count; ++i) {
    entry.api = &catalog.business_providers[i];
    rc = turbo_flow_stl_error(vec_push(&registry->entries, &entry));
  }
  if (rc != SALTS_OK) {
    vec_destroy(&registry->entries);
    free(registry);
    return rc;
  }
  rc = turbo_flow_plugin_catalog_snapshot_retain(snapshot);
  if (rc != SALTS_OK) {
    vec_destroy(&registry->entries);
    free(registry);
    return rc;
  }
  registry->snapshot = snapshot;
  *out = registry;
  return SALTS_OK;
}

int turbo_flow_protocol_business_registry_destroy(
    turbo_flow_protocol_business_registry_t *registry) {
  if (!registry) return SALTS_OK;
  for (size_t i = 0u; i < vec_size(&registry->entries); ++i) {
    const flow_protocol_business_entry_t *entry =
        (const flow_protocol_business_entry_t *)vec_at_const(&registry->entries, i);
    if (entry && entry->active_owners != 0u) return SALTS_EBUSY;
  }
  turbo_flow_plugin_catalog_snapshot_destroy(registry->snapshot);
  vec_destroy(&registry->entries);
  free(registry);
  return SALTS_OK;
}

const turbo_flow_protocol_business_plugin_api_t *
turbo_flow_protocol_business_registry_find(const turbo_flow_protocol_business_registry_t *registry,
                                           const char *business) {
  if (!registry || !business || !business[0]) return NULL;
  for (size_t i = 0u; i < vec_size(&registry->entries); ++i) {
    const flow_protocol_business_entry_t *entry =
        (const flow_protocol_business_entry_t *)vec_at_const(&registry->entries, i);
    if (entry && strcmp(entry->api->business, business) == 0) return entry->api;
  }
  return NULL;
}

int turbo_flow_protocol_business_owner_create_registered(
    turbo_flow_protocol_business_registry_t *registry, const char *business,
    const turbo_flow_protocol_business_open_request_t *request,
    turbo_flow_protocol_business_owner_t **out) {
  turbo_flow_protocol_business_service_t service = TURBO_FLOW_PROTOCOL_BUSINESS_SERVICE_INIT;
  turbo_flow_protocol_business_info_t info = TURBO_FLOW_PROTOCOL_BUSINESS_INFO_INIT;
  turbo_flow_protocol_business_owner_t *owner;
  flow_protocol_business_entry_t *entry;
  int rc;
  if (out) *out = NULL;
  rc = flow_protocol_business_open_request_validate(request);
  if (rc != SALTS_OK || !registry || !business || !business[0] || !out)
    return rc != SALTS_OK ? rc : SALTS_EINVAL;
  entry = flow_protocol_business_registry_entry(registry, business);
  if (!entry) return SALTS_ENOTSUP;
  if (entry->api->protocol != request->protocol) return SALTS_ENOTSUP;
  service.protocol = request->protocol;
  rc = entry->api->open(entry->api->ctx, request, &service);
  if (rc != SALTS_OK) {
    flow_protocol_business_service_cleanup(entry, &service);
    return rc;
  }
  if (service.size < sizeof(service) ||
      service.abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      service.protocol != request->protocol || !service.instance || !service.owner) {
    flow_protocol_business_service_cleanup(entry, &service);
    return SALTS_EPROTO;
  }
  rc = turbo_flow_protocol_business_get_info(service.instance, &info);
  if (rc != SALTS_OK || info.protocol != request->protocol ||
      info.capabilities != entry->api->capabilities ||
      strcmp(info.business, entry->api->business) != 0 ||
      info.max_payload_size > request->max_payload_size) {
    flow_protocol_business_service_cleanup(entry, &service);
    return SALTS_EPROTO;
  }
  owner = (turbo_flow_protocol_business_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    flow_protocol_business_service_cleanup(entry, &service);
    return SALTS_ENOMEM;
  }
  owner->entry = entry;
  owner->service = service;
  entry->active_owners++;
  *out = owner;
  return SALTS_OK;
}

int turbo_flow_protocol_business_owner_instance(const turbo_flow_protocol_business_owner_t *owner,
                                                turbo_flow_protocol_kind_t expected_protocol,
                                                turbo_flow_protocol_business_t **out) {
  if (out) *out = NULL;
  if (!owner || !owner->entry || !out || expected_protocol != owner->service.protocol ||
      !owner->service.instance)
    return SALTS_EINVAL;
  *out = owner->service.instance;
  return SALTS_OK;
}

void turbo_flow_protocol_business_owner_destroy(turbo_flow_protocol_business_owner_t *owner) {
  if (!owner) return;
  if (owner->entry && owner->entry->api && owner->service.owner) {
    owner->entry->api->close(owner->entry->api->ctx, &owner->service);
    if (owner->entry->active_owners > 0u) owner->entry->active_owners--;
  }
  free(owner);
}
