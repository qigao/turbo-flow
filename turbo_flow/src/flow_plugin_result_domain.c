#include "flow_plugin_operation_internal.h"
#include "turbo_flow_stl_error_internal.h"
#include <stdlib.h>
#include <string.h>

enum { FLOW_RESULT_DOMAIN_MAX_OWNERS = 1024 };
static int domain_error_valid(const turbo_flow_plugin_error_t *error) {
  return error && error->size == sizeof(*error) &&
         error->abi_major == TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR &&
         error->abi_minor == TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR;
}
static int domain_error(turbo_flow_plugin_error_t *error, int rc) {
  if (domain_error_valid(error)) {
    *error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    error->status = rc;
    error->stage = rc == SALTS_OK ? TURBO_FLOW_PLUGIN_STAGE_NONE : TURBO_FLOW_PLUGIN_STAGE_STATE;
  }
  return rc;
}
int turbo_flow_plugin_result_domain_create(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                           size_t capacity, turbo_flow_plugin_result_domain_t **out,
                                           turbo_flow_plugin_error_t *error) {
  turbo_flow_plugin_result_domain_t *domain;
  int rc;
  if (out) *out = NULL;
  if (!domain_error_valid(error)) return SALTS_EINVAL;
  if (!snapshot || !out || !capacity || capacity > FLOW_RESULT_DOMAIN_MAX_OWNERS ||
      capacity > SIZE_MAX / sizeof(flow_plugin_result_entry_t))
    return domain_error(error, SALTS_EINVAL);
  domain = calloc(1, sizeof(*domain));
  if (!domain) return domain_error(error, SALTS_ENOMEM);
  rc = turbo_flow_stl_error(vec_init_bytes(&domain->entries, sizeof(flow_plugin_result_entry_t),
                                           _Alignof(turbo_flow_max_align_t), capacity));
  if (rc == SALTS_OK) rc = turbo_flow_stl_error(vec_resize(&domain->entries, capacity));
  if (rc == SALTS_OK) rc = turbo_flow_plugin_catalog_snapshot_retain(snapshot);
  if (rc != SALTS_OK) {
    vec_destroy(&domain->entries);
    free(domain);
    return domain_error(error, rc);
  }
  memset(vec_data(&domain->entries), 0, capacity * sizeof(flow_plugin_result_entry_t));
  domain->snapshot = snapshot;
  domain->state = TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY;
  *out = domain;
  return domain_error(error, SALTS_OK);
}
int flow_plugin_result_domain_admit(const turbo_flow_plugin_result_domain_t *domain,
                                    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                    size_t count) {
  if (!domain || domain->state != TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY ||
      domain->snapshot != snapshot)
    return SALTS_EINVAL;
  return count > vec_size(&domain->entries) ? SALTS_ENOSPC : SALTS_OK;
}
int flow_plugin_result_domain_attach(turbo_flow_plugin_result_domain_t *domain) {
  if (!domain || domain->state != TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY) return SALTS_EINVAL;
  domain->state = TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED;
  return SALTS_OK;
}
void flow_plugin_result_domain_detach(turbo_flow_plugin_result_domain_t *domain) {
  if (domain && domain->state == TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED)
    domain->state = TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED;
}
static int domain_clone(const void *value, void *ctx, void **out) {
  flow_plugin_result_entry_t *entry = ctx;
  return entry->vtable.clone_result(value, entry->context, out);
}
static void domain_destroy_value(void *value, void *ctx) {
  flow_plugin_result_entry_t *entry = ctx;
  entry->vtable.destroy_result(value, entry->context);
}
static int domain_release_context(void *ctx) {
  flow_plugin_result_entry_t *entry = ctx;
  int rc = entry->vtable.release_result_context(entry->context);
  if (rc == SALTS_OK) entry->context = NULL;
  return rc;
}
int flow_plugin_result_domain_materialize(turbo_flow_plugin_result_domain_t *domain,
                                          const turbo_flow_plugin_operation_v3_t *op,
                                          const turbo_flow_plugin_operation_request_v3_t *request,
                                          flow_plugin_result_entry_t **out,
                                          turbo_flow_plugin_operation_error_v3_t *error) {
  turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  flow_plugin_result_entry_t *entry;
  int rc;
  *out = NULL;
  if (!domain || domain->state != TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED ||
      domain->count >= vec_size(&domain->entries))
    return SALTS_EINVAL;
  entry = vec_at(&domain->entries, domain->count++);
  entry->vtable = op->vtable;
  *out = entry;
  rc = op->create_result_context(op->factory_ctx, request, &entry->context, error);
  rc = flow_plugin_operation_callback_result(error,
                                             TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT, rc);
  if (entry->context && entry->context == op->factory_ctx) {
    entry->context = NULL;
    return rc != SALTS_OK ? rc : SALTS_EPROTO;
  }
  if (rc != SALTS_OK) return rc;
  if (!entry->context) return SALTS_EPROTO;
  config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                 TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  config.capacity = request->limits.max_retained_bytes / request->limits.max_result_bytes;
  config.max_result_bytes = request->limits.max_result_bytes;
  config.max_retained_bytes = request->limits.max_retained_bytes;
  config.schema = op->output.projection;
  config.clone = domain_clone;
  config.destroy = domain_destroy_value;
  config.ctx = entry;
  config.release_context = domain_release_context;
  return turbo_flow_projection_owner_create(&config, &entry->owner);
}
int turbo_flow_plugin_result_domain_snapshot(const turbo_flow_plugin_result_domain_t *domain,
                                             turbo_flow_plugin_result_domain_snapshot_v3_t *out) {
  turbo_flow_plugin_result_domain_snapshot_v3_t state;
  if (!out || out->size != sizeof(*out) || out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return SALTS_EINVAL;
  turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
  if (!domain) {
    *out = state;
    return SALTS_EINVAL;
  }
  state.state = domain->state;
  state.capacity = vec_size(&domain->entries);
  state.last_cleanup_status = domain->last_cleanup_status;
  for (size_t i = 0; i < domain->count; ++i) {
    const flow_plugin_result_entry_t *entry = vec_at_const(&domain->entries, i);
    turbo_flow_projection_owner_snapshot_t owner = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    if (entry->context || entry->owner) ++state.owner_count;
    if (!entry->owner) continue;
    int rc = turbo_flow_projection_owner_snapshot(entry->owner, &owner);
    if (rc != SALTS_OK) return rc;
    if (owner.outstanding > SIZE_MAX - state.outstanding ||
        owner.retained_bytes > SIZE_MAX - state.retained_bytes)
      return SALTS_EINVAL;
    state.outstanding += owner.outstanding;
    state.retained_bytes += owner.retained_bytes;
  }
  *out = state;
  return SALTS_OK;
}
int turbo_flow_plugin_result_domain_destroy(turbo_flow_plugin_result_domain_t *domain,
                                            turbo_flow_plugin_error_t *error) {
  int rc;
  if (!domain_error_valid(error)) return SALTS_EINVAL;
  if (!domain) return domain_error(error, SALTS_EINVAL);
  if (domain->state == TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED)
    return domain_error(error, SALTS_EBUSY);
  domain->state = TURBO_FLOW_PLUGIN_RESULT_DOMAIN_RETIRING;
  for (size_t i = 0; i < domain->count; ++i) {
    flow_plugin_result_entry_t *entry = vec_at(&domain->entries, i);
    if (entry->owner) {
      rc = turbo_flow_projection_owner_stop(entry->owner);
      if (rc != SALTS_OK) goto failed;
    }
  }
  /* No context is released until every stopped owner has drained. Join remains caller-owned. */
  for (size_t i = 0; i < domain->count; ++i) {
    const flow_plugin_result_entry_t *entry = vec_at_const(&domain->entries, i);
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    if (!entry->owner) continue;
    rc = turbo_flow_projection_owner_snapshot(entry->owner, &state);
    if (rc != SALTS_OK) goto failed;
    if (state.outstanding) {
      rc = SALTS_EBUSY;
      goto failed;
    }
  }
  while (domain->count) {
    flow_plugin_result_entry_t *entry = vec_at(&domain->entries, domain->count - 1);
    rc = entry->owner     ? turbo_flow_projection_owner_destroy(entry->owner)
         : entry->context ? domain_release_context(entry)
                          : SALTS_OK;
    if (rc != SALTS_OK) goto failed;
    memset(entry, 0, sizeof(*entry));
    --domain->count;
  }
  turbo_flow_plugin_catalog_snapshot_destroy(domain->snapshot);
  vec_destroy(&domain->entries);
  free(domain);
  return domain_error(error, SALTS_OK);
failed:
  domain->last_cleanup_status = rc;
  return domain_error(error, rc);
}
