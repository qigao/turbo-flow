#include "flow_projection_owner_internal.h"
#include <salts/thread.h>
#include <stdlib.h>

struct turbo_flow_projection_owner_s {
  turbo_flow_projection_owner_config_t config;
  turbo_flow_data_schema_t schema;
  salts_mutex_t mutex;
  turbo_flow_projection_owner_snapshot_t state;
};

static int flow_projection_config_check(const turbo_flow_projection_owner_config_t *config) {
  const turbo_flow_data_schema_t *schema;
  turbo_flow_result_memory_requirements_t requirements;
  const uint32_t required = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                            TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  if (!config || config->size < sizeof(*config) ||
      config->abi_major != TURBO_FLOW_PROJECTION_ABI_MAJOR ||
      config->abi_minor != TURBO_FLOW_PROJECTION_ABI_MINOR) return SALTS_EINVAL;
  if (config->flags != required) return SALTS_ENOTSUP;
  schema = config->schema;
  if (!schema || schema->size < sizeof(*schema) || schema->domain <= TURBO_FLOW_DOMAIN_NONE ||
      schema->domain > TURBO_FLOW_DOMAIN_MANAGEMENT ||
      !schema->schema_name || !schema->schema_name[0] || !schema->type_name ||
      !schema->type_name[0] || !schema->projection_type || !schema->projection_type[0] ||
      !schema->schema_version || schema->encoding < TURBO_FLOW_DATA_ENCODING_TBE ||
      schema->encoding > TURBO_FLOW_DATA_ENCODING_OPAQUE || !config->destroy ||
      !config->release_context || !config->capacity || !config->max_result_bytes ||
      !config->max_retained_bytes || config->max_result_bytes > config->max_retained_bytes)
    return SALTS_EINVAL;
  turbo_flow_result_memory_requirements_init(&requirements);
  if (turbo_flow_result_memory_requirements(config->capacity, config->max_result_bytes,
                                            &requirements) != SALTS_OK)
    return SALTS_EINVAL;
  return SALTS_OK;
}

int turbo_flow_projection_owner_create(const turbo_flow_projection_owner_config_t *config,
                                       turbo_flow_projection_owner_t **out) {
  turbo_flow_projection_owner_t *owner;
  int rc;
  if (!out) return SALTS_EINVAL;
  *out = NULL;
  rc = flow_projection_config_check(config);
  if (rc != SALTS_OK) return rc;
  owner = (turbo_flow_projection_owner_t *)calloc(1, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  salts_mutex_init(&owner->mutex);
  if (!owner->mutex) {
    free(owner);
    return SALTS_ENOMEM;
  }
  owner->config = *config;
  owner->config.size = sizeof(owner->config);
  owner->schema = *config->schema;
  owner->schema.size = sizeof(owner->schema);
  owner->config.schema = &owner->schema;
  owner->state = (turbo_flow_projection_owner_snapshot_t)TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
  owner->state.accepting = 1;
  *out = owner;
  return SALTS_OK;
}

int turbo_flow_projection_owner_stop(turbo_flow_projection_owner_t *owner) {
  if (!owner) return SALTS_EINVAL;
  salts_mutex_lock(&owner->mutex);
  owner->state.accepting = 0;
  salts_mutex_unlock(&owner->mutex);
  return SALTS_OK;
}

int turbo_flow_projection_owner_snapshot(turbo_flow_projection_owner_t *owner,
                                         turbo_flow_projection_owner_snapshot_t *out) {
  if (!owner || !out || out->size < sizeof(*out) ||
      out->abi_major != TURBO_FLOW_PROJECTION_ABI_MAJOR ||
      out->abi_minor != TURBO_FLOW_PROJECTION_ABI_MINOR) return SALTS_EINVAL;
  salts_mutex_lock(&owner->mutex);
  *out = owner->state;
  salts_mutex_unlock(&owner->mutex);
  return SALTS_OK;
}

int turbo_flow_projection_owner_destroy(turbo_flow_projection_owner_t *owner) {
  int busy;
  int rc;
  if (!owner) return SALTS_EINVAL;
  salts_mutex_lock(&owner->mutex);
  busy = owner->state.accepting || owner->state.outstanding != 0u;
  salts_mutex_unlock(&owner->mutex);
  if (busy) return SALTS_EBUSY;
  rc = owner->config.release_context(owner->config.ctx);
  if (rc != SALTS_OK) return rc;
  salts_mutex_destroy(&owner->mutex);
  free(owner);
  return SALTS_OK;
}

int flow_projection_owner_reserve(turbo_flow_projection_owner_t *owner) {
  int rc = SALTS_OK;
  salts_mutex_lock(&owner->mutex);
  if (!owner->state.accepting) {
    rc = SALTS_ECANCELED;
    goto done;
  }
  if (owner->state.outstanding >= owner->config.capacity ||
      owner->config.max_result_bytes > owner->config.max_retained_bytes - owner->state.retained_bytes) {
    rc = SALTS_ENOSPC;
    goto done;
  }
  ++owner->state.outstanding;
  owner->state.retained_bytes += owner->config.max_result_bytes;
  if (owner->state.outstanding > owner->state.peak_outstanding)
    owner->state.peak_outstanding = owner->state.outstanding;
  if (owner->state.retained_bytes > owner->state.peak_retained_bytes)
    owner->state.peak_retained_bytes = owner->state.retained_bytes;
done:
  salts_mutex_unlock(&owner->mutex);
  return rc;
}

void flow_projection_owner_release(turbo_flow_projection_owner_t *owner) {
  salts_mutex_lock(&owner->mutex);
  --owner->state.outstanding;
  owner->state.retained_bytes -= owner->config.max_result_bytes;
  salts_mutex_unlock(&owner->mutex);
}

const turbo_flow_projection_owner_config_t *
flow_projection_owner_config(const turbo_flow_projection_owner_t *owner) {
  return &owner->config;
}

int turbo_flow_result_memory_requirements(size_t capacity, size_t max_result_bytes,
                                          turbo_flow_result_memory_requirements_t *out) {
  size_t per, peak, payload;
  if (!out || out->size != sizeof(*out) ||
      out->abi_major != TURBO_FLOW_PROJECTION_ABI_MAJOR ||
      out->abi_minor != TURBO_FLOW_PROJECTION_ABI_MINOR) return SALTS_EINVAL;
  out->owner_bytes = out->claim_bytes = out->message_bytes = 0u;
  out->peak_metadata_bytes = out->payload_bound_bytes = 0u;
  if (!capacity || !max_result_bytes) return SALTS_EINVAL;
  per = sizeof(struct turbo_flow_result_claim_s) + sizeof(flow_msg_projection_t);
  if (capacity > (SIZE_MAX - sizeof(turbo_flow_projection_owner_t)) / per ||
      capacity > SIZE_MAX / max_result_bytes) return SALTS_EINVAL;
  peak = sizeof(turbo_flow_projection_owner_t) + capacity * per;
  payload = capacity * max_result_bytes;
  out->owner_bytes = sizeof(turbo_flow_projection_owner_t);
  out->claim_bytes = sizeof(struct turbo_flow_result_claim_s);
  out->message_bytes = sizeof(flow_msg_projection_t);
  out->peak_metadata_bytes = peak;
  out->payload_bound_bytes = payload;
  return SALTS_OK;
}
