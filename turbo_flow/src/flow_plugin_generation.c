#include "flow_plugin_operation_internal.h"
#include "flow_internal.h"
#include "turbo_flow_plugin_generation.h"

#include "turbo_flow_stl_error_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum flow_plugin_generation_owner_state_e {
  FLOW_PLUGIN_GENERATION_OWNER_ACTIVE = 1,
  FLOW_PLUGIN_GENERATION_OWNER_QUIESCED,
  FLOW_PLUGIN_GENERATION_OWNER_DRAINED,
  FLOW_PLUGIN_GENERATION_OWNER_SHUTDOWN
} flow_plugin_generation_owner_state_t;

typedef struct flow_plugin_generation_owner_s {
  turbo_flow_plugin_product_owner_v1_t owner;
  flow_plugin_generation_owner_state_t state;
  const char *name;
  int resource;
} flow_plugin_generation_owner_t;

typedef struct flow_plugin_generation_materializer_binding_s {
  turbo_flow_plugin_materializer_v1_t materializer;
} flow_plugin_generation_materializer_binding_t;

struct turbo_flow_plugin_generation_s {
  turbo_flow_t *flow;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  vec_t owners;
  vec_t bindings;
  vec_t materializers;
  turbo_flow_plugin_result_domain_t *domain;
  turbo_flow_config_error_t cleanup_error;
  const char *unretirable_owner;
  int unretirable_resource;
  int failed;
  size_t leases;
  size_t poll_cursor;
  int poll_closed;
  turbo_flow_plugin_generation_state_t state;
};

static int flow_plugin_generation_error(turbo_flow_config_error_t *error, int status,
                                        const char *path, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s", path ? path : "$.generation");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int flow_plugin_generation_provider_error(turbo_flow_config_error_t *error, int status,
                                                 const char *scope, const char *name,
                                                 const char *message) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  if (error && error->size >= sizeof(*error) && error->status != SALTS_OK) return status;
  (void)snprintf(path, sizeof(path), "$.%s.%s", scope, name);
  return flow_plugin_generation_error(error, status, path, message);
}

static int flow_plugin_generation_owner_error(turbo_flow_config_error_t *error, int status,
                                              const flow_plugin_generation_owner_t *owner,
                                              const char *phase) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  (void)snprintf(path, sizeof(path), "$.%s.%s.owner.%s",
                 owner && owner->resource ? "channels" : "adapters",
                 owner && owner->name ? owner->name : "unknown", phase);
  return flow_plugin_generation_error(error, status, path, "Product owner callback failed");
}

static int flow_plugin_generation_reference_seen(const turbo_flow_t *flow, size_t stage_index,
                                                 int resource, const char *name) {
  if (!name) return 1;
  for (size_t i = 0u; i < stage_index; ++i) {
    const turbo_flow_stage_plan_t *previous = turbo_flow_stage_at(flow, i);
    const char *previous_name =
        previous ? (resource ? previous->resource_name : previous->adapter_name) : NULL;
    if (previous_name && strcmp(previous_name, name) == 0) return 1;
  }
  return 0;
}

static const turbo_flow_plugin_transactional_adapter_provider_v1_t *
flow_plugin_generation_adapter_provider(
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog, const char *kind) {
  if (!catalog || !kind) return NULL;
  for (size_t i = 0u; i < catalog->adapter_provider_count; ++i) {
    const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider =
        &catalog->adapter_providers[i];
    if (provider->kind && strcmp(provider->kind, kind) == 0) return provider;
  }
  return NULL;
}

static const turbo_flow_plugin_transactional_resource_provider_v1_t *
flow_plugin_generation_resource_provider(
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog, const char *kind) {
  if (!catalog || !kind) return NULL;
  for (size_t i = 0u; i < catalog->resource_provider_count; ++i) {
    const turbo_flow_plugin_transactional_resource_provider_v1_t *provider =
        &catalog->resource_providers[i];
    if (provider->kind && strcmp(provider->kind, kind) == 0) return provider;
  }
  return NULL;
}

static int flow_plugin_generation_catalog_validate(
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
    turbo_flow_config_error_t *error) {
  if (!catalog || catalog->size != sizeof(*catalog) ||
      catalog->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      catalog->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      (catalog->adapter_provider_count > 0u && !catalog->adapter_providers) ||
      (catalog->resource_provider_count > 0u && !catalog->resource_providers))
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.providers",
                                        "invalid transactional provider catalog");
  for (size_t i = 0u; i < catalog->adapter_provider_count; ++i) {
    const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider =
        &catalog->adapter_providers[i];
    if (provider->size != sizeof(*provider) ||
        provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
        provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !provider->kind ||
        !provider->kind[0] || !provider->preflight || !provider->materialize)
      return flow_plugin_generation_error(error, SALTS_EINVAL, "$.providers.adapters",
                                          "invalid transactional adapter provider");
    for (size_t previous = 0u; previous < i; ++previous) {
      if (strcmp(provider->kind, catalog->adapter_providers[previous].kind) == 0)
        return flow_plugin_generation_error(error, SALTS_EALREADY, "$.providers.adapters",
                                            "duplicate transactional adapter provider kind");
    }
  }
  for (size_t i = 0u; i < catalog->resource_provider_count; ++i) {
    const turbo_flow_plugin_transactional_resource_provider_v1_t *provider =
        &catalog->resource_providers[i];
    if (provider->size != sizeof(*provider) ||
        provider->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
        provider->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !provider->kind ||
        !provider->kind[0] || !provider->preflight || !provider->materialize)
      return flow_plugin_generation_error(error, SALTS_EINVAL, "$.providers.resources",
                                          "invalid transactional resource provider");
    for (size_t previous = 0u; previous < i; ++previous) {
      if (strcmp(provider->kind, catalog->resource_providers[previous].kind) == 0)
        return flow_plugin_generation_error(error, SALTS_EALREADY, "$.providers.resources",
                                            "duplicate transactional resource provider kind");
    }
  }
  return SALTS_OK;
}

static int flow_plugin_generation_resolve_reference(
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
    const turbo_flow_resolved_config_t *resolved, const char *name, int resource,
    const void **provider_out, turbo_flow_config_error_t *error) {
  int rc;
  if (provider_out) *provider_out = NULL;
  if (!catalog || !resolved || !name || !name[0] || !provider_out) return SALTS_EINVAL;
  if (resource) {
    turbo_flow_resolved_channel_view_t view = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
    rc = turbo_flow_resolved_config_channel(resolved, name, &view);
    if (rc != SALTS_OK)
      return flow_plugin_generation_provider_error(error, rc, "channels", name,
                                                   "Graph resource has no configured channel");
    *provider_out = flow_plugin_generation_resource_provider(catalog, view.kind);
    if (!*provider_out)
      return flow_plugin_generation_provider_error(
          error, SALTS_ENOTSUP, "channels", name,
          "Graph resource kind has no transactional provider");
  } else {
    turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
    rc = turbo_flow_resolved_config_adapter(resolved, name, &view);
    if (rc != SALTS_OK)
      return flow_plugin_generation_provider_error(error, rc, "adapters", name,
                                                   "Graph adapter has no resolved configuration");
    *provider_out = flow_plugin_generation_adapter_provider(catalog, view.kind);
    if (!*provider_out)
      return flow_plugin_generation_provider_error(
          error, SALTS_ENOTSUP, "adapters", name,
          "Graph adapter kind has no transactional provider");
  }
  return SALTS_OK;
}

static int flow_plugin_generation_plan_validate(
    const turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog, size_t owner_capacity,
    size_t *required_out, turbo_flow_config_error_t *error) {
  const size_t stage_count = turbo_flow_stage_count(flow);
  size_t required = 0u;
  for (size_t i = 0u; i < stage_count; ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    /* stage_at reuses one thread-local view; the names remain Graph-owned across nested lookups. */
    const char *resource_name = stage ? stage->resource_name : NULL;
    const char *adapter_name = stage ? stage->adapter_name : NULL;
    const void *provider;
    int rc;
    if (!stage)
      return flow_plugin_generation_error(error, SALTS_EPROTO, "$.graph",
                                          "Graph contains an invalid stage plan");
    if (resource_name && !flow_plugin_generation_reference_seen(flow, i, 1, resource_name)) {
      rc = flow_plugin_generation_resolve_reference(catalog, resolved, resource_name, 1, &provider,
                                                    error);
      if (rc != SALTS_OK) {
        if (error->status == SALTS_OK)
          flow_plugin_generation_provider_error(error, rc, "channels", resource_name,
                                                "resource reference validation failed");
        return rc;
      }
      if (required == SIZE_MAX) return SALTS_ERANGE;
      ++required;
    }
    if (adapter_name && !flow_plugin_generation_reference_seen(flow, i, 0, adapter_name)) {
      rc = flow_plugin_generation_resolve_reference(catalog, resolved, adapter_name, 0, &provider,
                                                    error);
      if (rc != SALTS_OK) {
        if (error->status == SALTS_OK)
          flow_plugin_generation_provider_error(error, rc, "adapters", adapter_name,
                                                "adapter reference validation failed");
        return rc;
      }
      if (required == SIZE_MAX) return SALTS_ERANGE;
      ++required;
    }
  }
  if (required > owner_capacity)
    return flow_plugin_generation_error(error, SALTS_ENOSPC, "$.generation.owner_capacity",
                                        "Graph generation owner capacity is exhausted");
  *required_out = required;
  return SALTS_OK;
}

static int flow_plugin_generation_preflight(
    const turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
    turbo_flow_config_error_t *error) {
  const size_t stage_count = turbo_flow_stage_count(flow);
  for (int resource = 1; resource >= 0; --resource) {
    for (size_t i = 0u; i < stage_count; ++i) {
      const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
      const char *name = stage ? (resource ? stage->resource_name : stage->adapter_name) : NULL;
      const void *untyped_provider = NULL;
      int rc;
      if (!name || flow_plugin_generation_reference_seen(flow, i, resource, name)) continue;
      rc = flow_plugin_generation_resolve_reference(catalog, resolved, name, resource,
                                                    &untyped_provider, error);
      if (rc != SALTS_OK) return rc;
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      if (resource) {
        const turbo_flow_plugin_transactional_resource_provider_v1_t *provider =
            (const turbo_flow_plugin_transactional_resource_provider_v1_t *)untyped_provider;
        rc = provider->preflight(provider->ctx, resolved, name, error);
        if (rc != SALTS_OK)
          return flow_plugin_generation_provider_error(error, rc, "channels", name,
                                                       "resource provider preflight failed");
      } else {
        const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider =
            (const turbo_flow_plugin_transactional_adapter_provider_v1_t *)untyped_provider;
        rc = provider->preflight(provider->ctx, resolved, name, error);
        if (rc != SALTS_OK)
          return flow_plugin_generation_provider_error(error, rc, "adapters", name,
                                                       "adapter provider preflight failed");
      }
    }
  }
  return SALTS_OK;
}

static int
flow_plugin_generation_owner_abi_valid(const turbo_flow_plugin_product_owner_v1_t *owner) {
  return owner && owner->size == sizeof(*owner) &&
         owner->abi_major == TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR &&
         owner->abi_minor == TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
}

static int flow_plugin_generation_owner_valid(const turbo_flow_plugin_product_owner_v1_t *owner) {
  const turbo_flow_plugin_product_owner_flags_t known =
      TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD | TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE |
      TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  int external_poll;
  if (!flow_plugin_generation_owner_abi_valid(owner)) return 0;
  external_poll = (owner->flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL) != 0u;
  return owner->ctx && (owner->flags & ~known) == 0u &&
         ((owner->flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD) != 0u) !=
             ((owner->flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE) != 0u) &&
         owner->quiesce && owner->drain && owner->shutdown && owner->destroy &&
         (!external_poll || owner->poll) && (external_poll || !owner->poll);
}


static int flow_plugin_generation_materializer_error(
    turbo_flow_config_error_t *error, int status, size_t index,
    const char *field, const char *message) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  (void)snprintf(path, sizeof(path), "$.materializer_bindings[%zu]%s%s", index,
                 field ? "." : "", field ? field : "");
  return flow_plugin_generation_error(error, status, path, message);
}

static int flow_plugin_generation_materializer_encoding(
    turbo_flow_config_materializer_encoding_t configured,
    turbo_flow_data_encoding_t *out) {
  if (!out) return SALTS_EINVAL;
  switch (configured) {
  case TURBO_FLOW_CONFIG_MATERIALIZER_TBE:
    *out = TURBO_FLOW_DATA_ENCODING_TBE;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_JSON:
    *out = TURBO_FLOW_DATA_ENCODING_JSON;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_CSV:
    *out = TURBO_FLOW_DATA_ENCODING_CSV;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_XML:
    *out = TURBO_FLOW_DATA_ENCODING_XML;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_UTF8:
    *out = TURBO_FLOW_DATA_ENCODING_UTF8;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_OPAQUE:
    *out = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    return SALTS_OK;
  default:
    return SALTS_EINVAL;
  }
}

static int flow_plugin_generation_prepare_materializers(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    const turbo_flow_resolved_config_t *resolved,
    const vec_t *operations, vec_t *out,
    turbo_flow_config_error_t *error) {
  turbo_flow_plugin_materializer_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
  size_t count = 0u;
  int rc;
  rc = turbo_flow_resolved_config_materializer_binding_count(resolved, &count);
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer binding config is unavailable");
  rc = turbo_flow_stl_error(
      vec_init_bytes(out, sizeof(flow_plugin_generation_materializer_binding_t),
                     _Alignof(turbo_flow_max_align_t), count));
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer binding allocation failed");
  if (count == 0u) return SALTS_OK;
  rc = turbo_flow_stl_error(vec_reserve(out, count));
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer binding reservation failed");
  rc = turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog);
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer catalog is unavailable");

  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resolved_materializer_binding_view_t wanted =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    const turbo_flow_plugin_materializer_catalog_entry_v1_t *selected = NULL;
    flow_plugin_generation_materializer_binding_t compiled;
    turbo_flow_data_encoding_t encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    size_t consumer_count = 0u;
    rc = turbo_flow_resolved_config_materializer_binding_at(resolved, i, &wanted);
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, i, NULL, "materializer binding projection failed");
    rc = flow_plugin_generation_materializer_encoding(wanted.encoding, &encoding);
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, i, "encoding", "materializer encoding is invalid");

    for (size_t j = 0u; j < catalog.count; ++j) {
      const turbo_flow_plugin_materializer_catalog_entry_v1_t *entry = &catalog.entries[j];
      if (!entry->plugin_id || strcmp(entry->plugin_id, wanted.plugin) != 0)
        continue;
      if (strcmp(entry->materializer.schema.schema_name, wanted.schema) != 0 ||
          entry->materializer.schema.schema_version != wanted.schema_version ||
          entry->materializer.schema.encoding != encoding)
        continue;
      selected = entry;
      break;
    }
    if (!selected)
      return flow_plugin_generation_materializer_error(
          error, SALTS_ENOENT, i, "plugin",
          "configured materializer provider/schema/version/encoding was not registered");

    for (size_t j = 0u; j < vec_size(operations); ++j) {
      const flow_plugin_operation_binding_t *operation =
          (const flow_plugin_operation_binding_t *)vec_at_const(operations, j);
      if (!operation || !operation->operation.input.data ||
          !operation->operation.input.projection)
        continue;
      if (strcmp(operation->operation.input.data->stable_id, wanted.schema) != 0 ||
          operation->operation.input.schema_version != wanted.schema_version ||
          operation->operation.input.projection->encoding != encoding)
        continue;
      rc = turbo_flow_data_schema_match(
          &selected->materializer.schema, selected->materializer.data,
          operation->operation.input.projection, operation->operation.input.data);
      if (rc != SALTS_OK)
        return flow_plugin_generation_materializer_error(
            error, SALTS_EPROTO, i, "schema",
            "materializer schema/CMeta does not exactly match every operation input consumer");
      ++consumer_count;
    }
    if (consumer_count == 0u)
      return flow_plugin_generation_materializer_error(
          error, SALTS_EPROTO, i, "schema",
          "materializer binding has no exact typed-operation input consumer");

    memset(&compiled, 0, sizeof(compiled));
    compiled.materializer = selected->materializer;
    rc = turbo_flow_stl_error(vec_push(out, &compiled));
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, i, NULL, "materializer binding commit failed");
  }
  return SALTS_OK;
}

static int flow_plugin_generation_materialize(
    turbo_flow_plugin_generation_t *generation, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
    turbo_flow_config_error_t *error) {
  const size_t stage_count = turbo_flow_stage_count(generation->flow);
  for (int resource = 1; resource >= 0; --resource) {
    for (size_t i = 0u; i < stage_count; ++i) {
      const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(generation->flow, i);
      const char *name = stage ? (resource ? stage->resource_name : stage->adapter_name) : NULL;
      const void *untyped_provider = NULL;
      turbo_flow_plugin_product_owner_v1_t owner = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      flow_plugin_generation_owner_t entry;
      int rc;
      if (!name || flow_plugin_generation_reference_seen(generation->flow, i, resource, name))
        continue;
      rc = flow_plugin_generation_resolve_reference(catalog, resolved, name, resource,
                                                    &untyped_provider, error);
      if (rc != SALTS_OK) return rc;
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      if (resource) {
        const turbo_flow_plugin_transactional_resource_provider_v1_t *provider =
            (const turbo_flow_plugin_transactional_resource_provider_v1_t *)untyped_provider;
        rc = provider->materialize(provider->ctx, generation->flow, resolved, name, &owner, error);
      } else {
        const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider =
            (const turbo_flow_plugin_transactional_adapter_provider_v1_t *)untyped_provider;
        rc = provider->materialize(provider->ctx, generation->flow, resolved, name, &owner, error);
      }
      if (rc != SALTS_OK)
        return flow_plugin_generation_provider_error(
            error, rc, resource ? "channels" : "adapters", name,
            resource ? "resource provider materialization failed"
                     : "adapter provider materialization failed");
      if (!flow_plugin_generation_owner_abi_valid(&owner)) {
        /* Unknown layout grants no callback authority. Pin the Graph and module permanently;
           the current ABI intentionally has no force-unload or owner-repair operation. */
        generation->unretirable_owner = name;
        generation->unretirable_resource = resource;
        return flow_plugin_generation_provider_error(
            error, SALTS_EINVAL, resource ? "channels" : "adapters", name,
            "provider returned an incompatible Product owner ABI");
      }
      if (!flow_plugin_generation_owner_valid(&owner)) {
        if (owner.destroy && owner.ctx) {
          memset(&entry, 0, sizeof(entry));
          entry.owner = owner;
          /* No lifecycle may run on a rejected owner. Only its validated destroy is usable,
             after the Graph has removed all registered callback references. */
          entry.state = FLOW_PLUGIN_GENERATION_OWNER_SHUTDOWN;
          entry.name = name;
          entry.resource = resource;
          rc = turbo_flow_stl_error(vec_push(&generation->owners, &entry));
          if (rc != SALTS_OK) {
            turbo_flow_destroy(generation->flow);
            generation->flow = NULL;
            owner.destroy(owner.ctx);
            return flow_plugin_generation_error(
                error, rc, "$.generation.owners",
                "reserved owner storage rejected invalid-owner cleanup");
          }
        } else {
          generation->unretirable_owner = name;
          generation->unretirable_resource = resource;
        }
        return flow_plugin_generation_provider_error(
            error, SALTS_EPROTO, resource ? "channels" : "adapters", name,
            "provider returned an invalid Product owner vtable");
      }
      memset(&entry, 0, sizeof(entry));
      entry.owner = owner;
      entry.state = FLOW_PLUGIN_GENERATION_OWNER_ACTIVE;
      entry.name = name;
      entry.resource = resource;
      rc = turbo_flow_stl_error(vec_push(&generation->owners, &entry));
      if (rc != SALTS_OK) {
        turbo_flow_destroy(generation->flow);
        generation->flow = NULL;
        owner.destroy(owner.ctx);
        return flow_plugin_generation_error(error, rc, "$.generation.owners",
                                            "reserved owner storage rejected materialization");
      }
    }
  }
  return SALTS_OK;
}

int turbo_flow_plugin_generation_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot, const turbo_flow_resolved_config_t *resolved,
    turbo_flow_t **flow_io, const turbo_flow_plugin_generation_config_t *config,
    turbo_flow_plugin_result_domain_t *domain, turbo_flow_plugin_generation_t **generation_out,
    turbo_flow_plugin_generation_t **cleanup_out, turbo_flow_config_error_t *error) {
  turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  turbo_flow_plugin_generation_t *generation;
  size_t required = 0;
  int rc;
  if (generation_out) *generation_out = NULL;
  if (cleanup_out) *cleanup_out = NULL;
  if (!snapshot || !resolved || !flow_io || !*flow_io || !config || !generation_out ||
      !cleanup_out || generation_out == cleanup_out || !error || error->size != sizeof(*error) ||
      config->size != sizeof(*config) || config->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      config->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      config->owner_capacity > TURBO_FLOW_PLUGIN_GENERATION_MAX_OWNERS)
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.generation",
                                        "invalid generation arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (turbo_flow_state(*flow_io) != TURBO_FLOW_STATE_PARSED)
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.graph", "Graph must be parsed");
  rc = turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog);
  if (rc != SALTS_OK)
    return flow_plugin_generation_error(error, rc, "$.providers", "catalog unavailable");
  rc = flow_plugin_generation_catalog_validate(&catalog, error);
  if (rc != SALTS_OK) return rc;
  rc = flow_plugin_generation_plan_validate(*flow_io, resolved, &catalog, config->owner_capacity,
                                            &required, error);
  if (rc != SALTS_OK) return rc;
  generation = calloc(1, sizeof(*generation));
  if (!generation)
    return flow_plugin_generation_error(error, SALTS_ENOMEM, "$.generation", "allocation failed");
  generation->cleanup_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&generation->owners, sizeof(flow_plugin_generation_owner_t),
                     _Alignof(turbo_flow_max_align_t), config->owner_capacity));
  if (rc == SALTS_OK && required)
    rc = turbo_flow_stl_error(vec_reserve(&generation->owners, required));
  if (rc != SALTS_OK) goto preflight_failed;
  rc = flow_plugin_operations_prepare(snapshot, resolved, *flow_io, domain,
                                      config->operation_memory_budget_bytes, &generation->bindings,
                                      error);
  if (rc != SALTS_OK) goto preflight_failed;
  rc = flow_plugin_generation_prepare_materializers(snapshot, resolved, &generation->bindings,
                                                    &generation->materializers, error);
  if (rc != SALTS_OK) goto preflight_failed;
  rc = flow_plugin_generation_preflight(*flow_io, resolved, &catalog, error);
  if (rc != SALTS_OK) goto preflight_failed;
  rc = flow_plugin_operations_preflight(&generation->bindings, error);
  if (rc != SALTS_OK) goto preflight_failed;
  rc = turbo_flow_plugin_catalog_snapshot_retain(snapshot);
  if (rc != SALTS_OK) goto preflight_failed;
  generation->snapshot = snapshot;
  if (domain) {
    rc = flow_plugin_result_domain_attach(domain);
    if (rc != SALTS_OK) goto preflight_failed;
  }
  generation->domain = domain;
  generation->flow = *flow_io;
  *flow_io = NULL;
  rc = flow_plugin_generation_materialize(generation, resolved, &catalog, error);
  if (rc == SALTS_OK)
    rc = flow_plugin_operations_materialize(&generation->bindings, domain, generation->flow, error);
  if (rc == SALTS_OK) {
    rc = turbo_flow_compile(generation->flow);
    if (rc != SALTS_OK) {
      const turbo_flow_error_t *graph_error = turbo_flow_last_error(generation->flow);
      flow_plugin_generation_error(error, rc, "$.graph",
                                   graph_error && graph_error->message[0] ? graph_error->message
                                                                          : "Graph compile failed");
    }
  }
  if (rc != SALTS_OK) {
    turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    generation->failed = 1;
    generation->state = TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP;
    if (turbo_flow_plugin_generation_destroy(generation, 0, &cleanup_error) != SALTS_OK)
      *cleanup_out = generation;
    return rc;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_COMPILED;
  *generation_out = generation;
  return SALTS_OK;
preflight_failed:
  if (error->status == SALTS_OK)
    flow_plugin_generation_error(error, rc, "$.generation.preflight",
                                 "generation preflight failed");
  if (generation->snapshot) turbo_flow_plugin_catalog_snapshot_destroy(generation->snapshot);
  vec_destroy(&generation->materializers);
  flow_plugin_operations_free(&generation->bindings);
  vec_destroy(&generation->owners);
  free(generation);
  return rc;
}

turbo_flow_t *turbo_flow_plugin_generation_flow(turbo_flow_plugin_generation_t *generation) {
  return generation && !generation->failed ? generation->flow : NULL;
}

turbo_flow_plugin_generation_state_t
turbo_flow_plugin_generation_state(const turbo_flow_plugin_generation_t *generation) {
  if (!generation) return TURBO_FLOW_PLUGIN_GENERATION_INVALID;
  if (generation->failed) return TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP;
  if (generation->state == TURBO_FLOW_PLUGIN_GENERATION_COMPILED && generation->flow &&
      turbo_flow_state(generation->flow) == TURBO_FLOW_STATE_STARTED)
    return TURBO_FLOW_PLUGIN_GENERATION_ACTIVE;
  return generation->state;
}

size_t turbo_flow_plugin_generation_owner_count(const turbo_flow_plugin_generation_t *generation) {
  return generation ? vec_size(&generation->owners) : 0u;
}

int turbo_flow_plugin_generation_poll(turbo_flow_plugin_generation_t *generation,
                                      uint32_t timeout_ms, turbo_flow_config_error_t *error) {
  size_t owner_count;
  size_t first = SIZE_MAX;
  if (!generation || !error || error->size < sizeof(*error))
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.generation.poll",
                                        "invalid Graph generation poll arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (generation->failed || generation->poll_closed || !generation->flow ||
      turbo_flow_state(generation->flow) != TURBO_FLOW_STATE_STARTED)
    return flow_plugin_generation_error(error, SALTS_EBUSY, "$.generation.poll",
                                        "Graph generation is not accepting progress");
  owner_count = vec_size(&generation->owners);
  if (owner_count == 0u) return SALTS_OK;
  for (size_t offset = 0u; offset < owner_count; ++offset) {
    const size_t index = (generation->poll_cursor + offset) % owner_count;
    const flow_plugin_generation_owner_t *entry =
        (const flow_plugin_generation_owner_t *)vec_at_const(&generation->owners, index);
    if (entry && (entry->owner.flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL) != 0u) {
      first = index;
      break;
    }
  }
  if (first == SIZE_MAX) return SALTS_OK;
  generation->poll_cursor = (first + 1u) % owner_count;
  for (size_t offset = 0u; offset < owner_count; ++offset) {
    const size_t index = (first + offset) % owner_count;
    const flow_plugin_generation_owner_t *entry =
        (const flow_plugin_generation_owner_t *)vec_at_const(&generation->owners, index);
    int rc;
    if (!entry ||
        (entry->owner.flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL) == 0u)
      continue;
    rc = entry->owner.poll(entry->owner.ctx, index == first ? timeout_ms : 0u);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "poll");
  }
  return SALTS_OK;
}

int turbo_flow_plugin_generation_lease_acquire(turbo_flow_plugin_generation_t *generation) {
  if (!generation) return SALTS_EINVAL;
  if (generation->leases == SIZE_MAX) return SALTS_ENOSPC;
  generation->leases++;
  return SALTS_OK;
}

int turbo_flow_plugin_generation_lease_release(turbo_flow_plugin_generation_t *generation) {
  if (!generation || generation->leases == 0u) return SALTS_EINVAL;
  generation->leases--;
  return SALTS_OK;
}

static int flow_plugin_generation_retire(turbo_flow_plugin_generation_t *generation,
                                         uint64_t timeout_ms, turbo_flow_config_error_t *error) {
  int rc;
  if (!generation || !error || error->size < sizeof(*error))
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.generation",
                                        "invalid Graph generation destroy arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (generation->unretirable_owner)
    return flow_plugin_generation_provider_error(
        error, SALTS_EINVAL, generation->unretirable_resource ? "channels" : "adapters",
        generation->unretirable_owner,
        "Product owner has no verifiable cleanup contract; resources remain pinned");
  rc = flow_durable_buffers_prepare_retire(generation->flow, timeout_ms);
  if (rc != SALTS_OK)
    return flow_plugin_generation_error(error, rc, "$.generation.buffers",
                                        "durable backlog must settle before retirement");
  rc = flow_plugin_operations_close(&generation->bindings);
  if (rc != SALTS_OK)
    return flow_plugin_generation_error(error, rc, "$.generation.inflight",
                                        "operation still executing");
  if (generation->leases != 0u)
    return flow_plugin_generation_error(error, SALTS_EBUSY, "$.generation.leases",
                                        "Graph generation still has active leases");
  generation->poll_closed = 1;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (!entry || entry->state >= FLOW_PLUGIN_GENERATION_OWNER_QUIESCED) continue;
    rc = entry->owner.quiesce(entry->owner.ctx, timeout_ms);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "quiesce");
    entry->state = FLOW_PLUGIN_GENERATION_OWNER_QUIESCED;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_QUIESCED;
  /* A failed create never started execution; compile failure has no running Graph to stop. */
  if (!generation->failed && generation->flow &&
      (turbo_flow_state(generation->flow) == TURBO_FLOW_STATE_STARTED ||
       turbo_flow_state(generation->flow) == TURBO_FLOW_STATE_FAILED)) {
    rc = turbo_flow_stop(generation->flow);
    if (rc != SALTS_OK)
      return flow_plugin_generation_error(error, rc, "$.generation.graph.stop",
                                          "Graph stop failed");
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_STOPPED;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (!entry || entry->state >= FLOW_PLUGIN_GENERATION_OWNER_DRAINED) continue;
    rc = entry->owner.drain(entry->owner.ctx, timeout_ms);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "drain");
    entry->state = FLOW_PLUGIN_GENERATION_OWNER_DRAINED;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_DRAINED;
  rc = flow_plugin_operations_release(&generation->bindings, error);
  if (rc != SALTS_OK) return rc;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (!entry || entry->state >= FLOW_PLUGIN_GENERATION_OWNER_SHUTDOWN) continue;
    rc = entry->owner.shutdown(entry->owner.ctx);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "shutdown");
    entry->state = FLOW_PLUGIN_GENERATION_OWNER_SHUTDOWN;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_SHUTDOWN;
  turbo_flow_destroy(generation->flow);
  generation->flow = NULL;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (entry) entry->owner.destroy(entry->owner.ctx);
  }
  flow_plugin_result_domain_detach(generation->domain);
  turbo_flow_plugin_catalog_snapshot_destroy(generation->snapshot);
  vec_destroy(&generation->materializers);
  flow_plugin_operations_free(&generation->bindings);
  vec_destroy(&generation->owners);
  memset(generation, 0, sizeof(*generation));
  free(generation);
  return SALTS_OK;
}

int turbo_flow_plugin_generation_destroy(turbo_flow_plugin_generation_t *generation,
                                         uint64_t timeout_ms, turbo_flow_config_error_t *error) {
  int rc;
  if (!generation || !error || error->size != sizeof(*error)) return SALTS_EINVAL;
  rc = flow_plugin_generation_retire(generation, timeout_ms, error);
  if (rc != SALTS_OK) generation->cleanup_error = *error;
  return rc;
}
int turbo_flow_plugin_generation_cleanup_error(const turbo_flow_plugin_generation_t *generation,
                                               turbo_flow_config_error_t *out) {
  if (!generation || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  *out = generation->cleanup_error;
  return SALTS_OK;
}
int turbo_flow_plugin_generation_operation_error(const turbo_flow_plugin_generation_t *generation,
                                                 size_t index,
                                                 turbo_flow_plugin_operation_error_v3_t *out) {
  flow_plugin_operation_binding_t *binding;
  if (!generation || !out || out->size != sizeof(*out) ||
      out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      index >= vec_size(&generation->bindings))
    return SALTS_EINVAL;
  binding = (flow_plugin_operation_binding_t *)vec_at_const(&generation->bindings, index);
  salts_mutex_lock(&binding->error_mutex);
  *out = binding->error;
  salts_mutex_unlock(&binding->error_mutex);
  return SALTS_OK;
}
