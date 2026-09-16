#include "flow_internal.h"
#include "flow_projection_owner_internal.h"

#include <stdlib.h>
#include <string.h>

#define FLOW_MSG_PROJECTION_MAGIC UINT64_C(0x544650524f4a5631)

static int flow_msg_projection_empty(const flow_msg_projection_t *projection) {
  return projection && !projection->descriptor && !projection->value && !projection->result_value &&
         !projection->has_durable_identity;
}

static turbo_flow_projection_owner_t *flow_msg_result_release_value(flow_msg_projection_t *p) {
  turbo_flow_projection_owner_t *owner = p->result_owner;
  if (p->result_value && p->result_destroy) p->result_destroy(p->result_value, p->result_ctx);
  p->result_schema = NULL; p->result_data = NULL; p->result_value = NULL;
  p->result_clone = NULL; p->result_destroy = NULL; p->result_ctx = NULL; p->result_owner = NULL;
  return owner;
}

static turbo_flow_projection_owner_t *flow_msg_projection_release_value(
    flow_msg_projection_t *projection) {
  turbo_flow_projection_owner_t *owner = projection->owner;
  if (projection->value && projection->destroy)
    projection->destroy(projection->value, projection->ctx);
  /* The caller returns quota after freeing or detaching the retained wrapper. */
  projection->owner = NULL;
  projection->value = NULL;
  projection->schema = NULL;
  projection->data = NULL;
  projection->clone = NULL;
  projection->destroy = NULL;
  projection->ctx = NULL;
  return owner;
}

static void flow_msg_projection_destroy(void *ptr, void *ctx) {
  flow_msg_projection_t *projection = (flow_msg_projection_t *)ptr;
  turbo_flow_projection_owner_t *owner;
  turbo_flow_projection_owner_t *result_owner;

  (void)ctx;
  if (!projection || projection->magic != FLOW_MSG_PROJECTION_MAGIC) return;
  projection->magic = 0u;
  result_owner = flow_msg_result_release_value(projection);
  owner = flow_msg_projection_release_value(projection);
  free(projection);
  if (owner) flow_projection_owner_release(owner);
  if (result_owner) flow_projection_owner_release(result_owner);
}

static const flow_msg_projection_t *flow_msg_projection(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *projection;

  if (!msg || !msg->_content_handle) return NULL;
  projection = (const flow_msg_projection_t *)msg->_content_handle;
  return projection->magic == FLOW_MSG_PROJECTION_MAGIC ? projection : NULL;
}

static int flow_msg_durable_identity_validate(const turbo_flow_durable_identity_t *identity) {
  if (!identity || identity->size != sizeof(*identity) ||
      identity->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION) {
    return SALTS_EINVAL;
  }
  if (!identity->source_id.data || identity->source_id.len == 0u ||
      !identity->admission_id.data || identity->admission_id.len == 0u ||
      (!identity->correlation.data && identity->correlation.len != 0u)) {
    return SALTS_EINVAL;
  }
  if (identity->source_id.len > TURBO_FLOW_DURABLE_SOURCE_ID_MAX ||
      identity->admission_id.len > TURBO_FLOW_DURABLE_ADMISSION_ID_MAX ||
      identity->correlation.len > TURBO_FLOW_DURABLE_CORRELATION_MAX) {
    return SALTS_ERANGE;
  }
  return SALTS_OK;
}

int turbo_flow_msg_set_durable_identity(turbo_flow_msg_t *msg,
                                        const turbo_flow_durable_identity_t *identity) {
  flow_msg_projection_t *projection;
  int rc;

  if (!msg) return SALTS_EINVAL;
  rc = flow_msg_durable_identity_validate(identity);
  if (rc != SALTS_OK) return rc;
  projection = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (projection && projection->claim_active) return SALTS_EBUSY;
  if (!projection) {
    projection = (flow_msg_projection_t *)calloc(1, sizeof(*projection));
    if (!projection) return SALTS_ENOMEM;
    projection->magic = FLOW_MSG_PROJECTION_MAGIC;
    msg->_content_handle = projection;
  }

  memcpy(projection->durable_source_id, identity->source_id.data, identity->source_id.len);
  projection->durable_source_id[identity->source_id.len] = '\0';
  memcpy(projection->durable_admission_id, identity->admission_id.data,
         identity->admission_id.len);
  projection->durable_admission_id[identity->admission_id.len] = '\0';
  if (identity->correlation.len != 0u) {
    memcpy(projection->durable_correlation, identity->correlation.data,
           identity->correlation.len);
  }
  projection->durable_correlation[identity->correlation.len] = '\0';
  projection->durable_source_id_len = identity->source_id.len;
  projection->durable_admission_id_len = identity->admission_id.len;
  projection->durable_correlation_len = identity->correlation.len;
  projection->durable_source_sequence = identity->source_sequence;
  projection->has_durable_identity = 1;
  return SALTS_OK;
}

int turbo_flow_msg_durable_identity(const turbo_flow_msg_t *msg,
                                    turbo_flow_durable_identity_t *out) {
  const flow_msg_projection_t *projection;

  if (!msg || !out || out->size != sizeof(*out) ||
      out->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION) {
    return SALTS_EINVAL;
  }
  projection = flow_msg_projection(msg);
  if (!projection || !projection->has_durable_identity) return SALTS_ENOENT;

  out->source_id = vstr_from_buf(projection->durable_source_id,
                                 projection->durable_source_id_len);
  out->admission_id = vstr_from_buf(projection->durable_admission_id,
                                    projection->durable_admission_id_len);
  out->correlation = vstr_from_buf(projection->durable_correlation,
                                   projection->durable_correlation_len);
  out->source_sequence = projection->durable_source_sequence;
  return SALTS_OK;
}

static int flow_msg_descriptor_accepts_schema(const turbo_flow_content_descriptor_t *descriptor,
                                              const turbo_flow_data_schema_t *schema) {
  if (!descriptor || !schema || (descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u) {
    return SALTS_OK;
  }
  return descriptor->encoding == schema->encoding &&
                 descriptor->schema_version == schema->schema_version &&
                 strcmp(descriptor->schema_name, schema->schema_name) == 0 &&
                 strcmp(descriptor->type_name, schema->type_name) == 0
             ? SALTS_OK
             : SALTS_EPROTO;
}

static int flow_msg_candidate_independent(const turbo_flow_msg_t *msg, const void *candidate,
                                          size_t candidate_size, const void *temporary,
                                          size_t temporary_size) {
  const flow_msg_projection_t *source = flow_msg_projection(msg);
  int rc;
  if (!candidate || !candidate_size) return SALTS_EPROTO;
  rc = turbo_flow_value_require_disjoint(candidate, candidate_size, candidate, candidate_size);
  if (rc == SALTS_EINVAL) return SALTS_EPROTO;
  if (source && source->value) {
    if (!source->data) {
      if (candidate == source->value) return SALTS_EPROTO;
    } else {
      rc = turbo_flow_value_require_disjoint(source->value, source->data->storage_type->size,
                                             candidate, candidate_size);
      if (rc != SALTS_OK) return SALTS_EPROTO;
    }
  }
  if (source && source->result_value) {
    rc = turbo_flow_value_require_disjoint(source->result_value,
        source->result_data->storage_type->size, candidate, candidate_size);
    if (rc != SALTS_OK) return SALTS_EPROTO;
  }
  if (msg->payload.data && msg->payload.len) {
    rc = turbo_flow_value_require_disjoint(msg->payload.data, msg->payload.len,
                                           candidate, candidate_size);
    if (rc != SALTS_OK) return SALTS_EPROTO;
  }
  if (temporary && temporary_size) {
    rc = turbo_flow_value_require_disjoint(temporary, temporary_size, candidate, candidate_size);
    if (rc != SALTS_OK) return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int flow_msg_projection_clone(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src) {
  const flow_msg_projection_t *source = flow_msg_projection(src);
  flow_msg_projection_t *copy = NULL;
  void *value = NULL, *result = NULL;
  int projection_reserved = 0, result_reserved = 0;
  int value_independent = 0, result_independent = 0;
  int rc = SALTS_OK;

  if (!source) return SALTS_EINVAL;
  if (source->claim_active) return SALTS_EBUSY;
  if (source->result_value && source->value && !source->data) return SALTS_ENOTSUP;
  if (source->owner) {
    rc = flow_projection_owner_reserve(source->owner);
    if (rc != SALTS_OK) return rc;
    projection_reserved = 1;
  }
  copy = (flow_msg_projection_t *)calloc(1, sizeof(*copy));
  if (!copy) { rc = SALTS_ENOMEM; goto fail; }
  if (source->value) {
    if (!source->clone) { rc = SALTS_ENOTSUP; goto fail; }
    rc = source->clone(source->value, source->ctx, &value);
    if (value) value_independent = flow_msg_candidate_independent(
        src, value, source->data ? source->data->storage_type->size : 1u, NULL, 0u) == SALTS_OK;
    if (rc == SALTS_OK && !value_independent) rc = SALTS_EPROTO;
    if (rc != SALTS_OK) goto fail;
  }
  if (source->result_value) {
    if (!source->result_clone) { rc = SALTS_ENOTSUP; goto fail; }
    rc = flow_projection_owner_reserve(source->result_owner);
    if (rc != SALTS_OK) goto fail;
    result_reserved = 1;
    rc = source->result_clone(source->result_value, source->result_ctx, &result);
    if (result) result_independent = flow_msg_candidate_independent(
        src, result, source->result_data->storage_type->size, value,
        source->data ? source->data->storage_type->size : 0u) == SALTS_OK;
    if (rc == SALTS_OK && !result_independent) rc = SALTS_EPROTO;
    if (rc != SALTS_OK) goto fail;
  }
  *copy = *source;
  if (copy->owns_descriptor) copy->descriptor = &copy->owned_descriptor;
  copy->value = value;
  copy->result_value = result;
  dst->_content_handle = copy;
  return SALTS_OK;
fail:
  if (result && result_independent) source->result_destroy(result, source->result_ctx);
  if (result_reserved) flow_projection_owner_release(source->result_owner);
  if (value && value_independent) source->destroy(value, source->ctx);
  if (projection_reserved) flow_projection_owner_release(source->owner);
  free(copy);
  return rc;
}

static void flow_msg_failure_cleanup(turbo_flow_failure_t *failure) {
  if (!failure) return;
  tstr_freep(&failure->stage_name);
  tstr_freep(&failure->adapter_name);
  tstr_freep(&failure->route_name);
  failure->code = SALTS_OK;
  failure->attempt = 0;
}

static int flow_msg_failure_copy(turbo_flow_failure_t *dst, const turbo_flow_failure_t *src) {
  if (!dst || !src) return SALTS_EINVAL;
  memset(dst, 0, sizeof(*dst));
  if (src->stage_name && !(dst->stage_name = tstr_from_v(tstr_to_v(src->stage_name)))) goto nomem;
  if (src->adapter_name && !(dst->adapter_name = tstr_from_v(tstr_to_v(src->adapter_name)))) {
    goto nomem;
  }
  if (src->route_name && !(dst->route_name = tstr_from_v(tstr_to_v(src->route_name)))) goto nomem;
  dst->code = src->code;
  dst->attempt = src->attempt;
  return SALTS_OK;

nomem:
  flow_msg_failure_cleanup(dst);
  return SALTS_ENOMEM;
}

static int flow_msg_view_within(const vstr *view, const void *base, size_t size) {
  uintptr_t data_address;
  uintptr_t base_address;
  size_t offset;

  if (!view || !base || !view->data) return 0;
  data_address = (uintptr_t)view->data;
  base_address = (uintptr_t)base;
  if (data_address < base_address) return 0;
  offset = (size_t)(data_address - base_address);
  return offset <= size && view->len <= size - offset;
}

int flow_msg_has_active_result_claim(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *projection = flow_msg_projection(msg);
  return projection && projection->claim_active;
}

int flow_msg_payload_validate(const turbo_flow_msg_t *msg) {
  if (!msg || (!msg->payload.data && msg->payload.len != 0u)) return SALTS_EINVAL;
  if (!msg->payload.data) return SALTS_OK;
  if (msg->owned_payload &&
      flow_msg_view_within(&msg->payload, msg->owned_payload, tstr_len(msg->owned_payload))) {
    return SALTS_OK;
  }
  if (msg->buffer && flow_msg_view_within(&msg->payload, mem_buffer_const_data(msg->buffer),
                                          mem_buffer_used(msg->buffer))) {
    return SALTS_OK;
  }
  return SALTS_EINVAL;
}

int flow_msg_transport_context_is_borrowed(const turbo_flow_msg_t *msg) {
  uintptr_t context_address;
  uintptr_t buffer_address;
  size_t offset;
  size_t used;

  if (!msg || !msg->transport_context) return 0;
  if (!msg->buffer) return 1;
  used = mem_buffer_used(msg->buffer);
  if (used == 0u) return 1;
  context_address = (uintptr_t)msg->transport_context;
  buffer_address = (uintptr_t)mem_buffer_const_data(msg->buffer);
  if (context_address < buffer_address) return 1;
  offset = (size_t)(context_address - buffer_address);
  return offset >= used;
}

int flow_msg_set_failure(turbo_flow_msg_t *msg, const char *stage_name, const char *adapter_name,
                         const char *route_name, int code, uint32_t attempt) {
  turbo_flow_failure_t failure;

  if (!msg || !stage_name || !route_name || code == SALTS_OK || attempt == 0u) {
    return SALTS_EINVAL;
  }
  memset(&failure, 0, sizeof(failure));
  failure.stage_name = tstr_dup(stage_name);
  failure.route_name = tstr_dup(route_name);
  if (adapter_name) failure.adapter_name = tstr_dup(adapter_name);
  if (!failure.stage_name || !failure.route_name || (adapter_name && !failure.adapter_name)) {
    flow_msg_failure_cleanup(&failure);
    return SALTS_ENOMEM;
  }
  failure.code = code;
  failure.attempt = attempt;
  flow_msg_failure_cleanup(&msg->failure);
  msg->failure = failure;
  return SALTS_OK;
}

void turbo_flow_msg_init(turbo_flow_msg_t *msg) {
  if (!msg) return;
  memset(msg, 0, sizeof(*msg));
  msg->data_decision = (turbo_flow_data_decision_t)TURBO_FLOW_DATA_DECISION_INIT;
}

void turbo_flow_msg_cleanup(turbo_flow_msg_t *msg) {
  if (!msg) return;
  if (flow_msg_projection(msg) && flow_msg_projection(msg)->claim_active) return;
  flow_msg_projection_destroy(msg->_content_handle, NULL);
  flow_msg_failure_cleanup(&msg->failure);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  turbo_flow_msg_init(msg);
}

int turbo_flow_msg_retain_view(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src) {
  const flow_msg_projection_t *source_binding;
  flow_msg_projection_t *binding_copy = NULL;

  if (!dst || !src || flow_msg_payload_validate(src) != SALTS_OK) return SALTS_EINVAL;
  source_binding = flow_msg_projection(src);
  if (src->owned_payload || (source_binding &&
      (source_binding->value || source_binding->result_value || source_binding->claim_active))) {
    return SALTS_EINVAL;
  }
  if (source_binding) {
    binding_copy = (flow_msg_projection_t *)calloc(1, sizeof(*binding_copy));
    if (!binding_copy) return SALTS_ENOMEM;
    *binding_copy = *source_binding;
    if (binding_copy->owns_descriptor) binding_copy->descriptor = &binding_copy->owned_descriptor;
  }

  turbo_flow_msg_init(dst);
  *dst = *src;
  dst->buffer = mem_buffer_retain(src->buffer);
  dst->owned_payload = NULL;
  dst->_content_handle = binding_copy;
  memset(&dst->failure, 0, sizeof(dst->failure));
  if (flow_msg_failure_copy(&dst->failure, &src->failure) != SALTS_OK) {
    mem_buffer_release(dst->buffer);
    free(binding_copy);
    turbo_flow_msg_init(dst);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

int turbo_flow_msg_clone(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src) {
  int has_projection;
  int payload_in_owned;

  if (!dst || !src || flow_msg_payload_validate(src) != SALTS_OK) return SALTS_EINVAL;
  has_projection = flow_msg_projection(src) != NULL;
  payload_in_owned = src->owned_payload && src->payload.data &&
                     flow_msg_view_within(&src->payload, src->owned_payload,
                                          tstr_len(src->owned_payload));
  turbo_flow_msg_init(dst);
  *dst = *src;
  dst->buffer = mem_buffer_retain(src->buffer);
  dst->owned_payload = NULL;
  dst->_content_handle = NULL;
  memset(&dst->failure, 0, sizeof(dst->failure));
  if (flow_msg_failure_copy(&dst->failure, &src->failure) != SALTS_OK) {
    mem_buffer_release(dst->buffer);
    turbo_flow_msg_init(dst);
    return SALTS_ENOMEM;
  }

  if (src->owned_payload) {
    dst->owned_payload = tstr_from_v(tstr_to_v(src->owned_payload));
    if (!dst->owned_payload) {
      mem_buffer_release(dst->buffer);
      turbo_flow_msg_init(dst);
      return SALTS_ENOMEM;
    }
    if (payload_in_owned) {
      size_t offset = (size_t)((uintptr_t)src->payload.data - (uintptr_t)src->owned_payload);
      dst->payload = vstr_from_buf(dst->owned_payload + offset, src->payload.len);
    } else if (!src->payload.data) {
      dst->payload = tstr_to_v(dst->owned_payload);
    }
  }

  if (has_projection) {
    int rc = flow_msg_projection_clone(dst, src);
    if (rc != SALTS_OK) {
      turbo_flow_msg_cleanup(dst);
      return rc;
    }
  }

  return SALTS_OK;
}

int turbo_flow_msg_move(turbo_flow_msg_t *dst, turbo_flow_msg_t *src) {
  if (!dst || !src) return SALTS_EINVAL;
  if (flow_msg_projection(src) && flow_msg_projection(src)->claim_active) return SALTS_EBUSY;
  turbo_flow_msg_init(dst);
  *dst = *src;
  turbo_flow_msg_init(src);
  return SALTS_OK;
}

turbo_flow_content_state_t turbo_flow_msg_content_state(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *projection = flow_msg_projection(msg);
  return projection && projection->value ? TURBO_FLOW_CONTENT_SCHEMA_BOUND
                                         : TURBO_FLOW_CONTENT_OPAQUE;
}

int turbo_flow_msg_bind_projection(turbo_flow_msg_t *msg, const turbo_flow_data_schema_t *schema,
                                   void *projection, turbo_flow_projection_clone_fn clone,
                                   turbo_flow_destroy_fn destroy, void *ctx) {
  flow_msg_projection_t *binding;

  if (!msg || !schema || schema->size < sizeof(*schema) ||
      schema->domain == TURBO_FLOW_DOMAIN_NONE || !schema->schema_name ||
      schema->schema_name[0] == '\0' || !schema->type_name || schema->type_name[0] == '\0' ||
      !schema->projection_type || schema->projection_type[0] == '\0' ||
      schema->schema_version == 0u || schema->encoding < TURBO_FLOW_DATA_ENCODING_TBE ||
      schema->encoding > TURBO_FLOW_DATA_ENCODING_OPAQUE || !projection || !destroy) {
    return SALTS_EINVAL;
  }
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (binding && binding->claim_active) return SALTS_EBUSY;
  if (binding && binding->value) return SALTS_EBUSY;
  if (binding && flow_msg_descriptor_accepts_schema(binding->descriptor, schema) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  if (!binding) {
    binding = (flow_msg_projection_t *)calloc(1, sizeof(*binding));
    if (!binding) return SALTS_ENOMEM;
    binding->magic = FLOW_MSG_PROJECTION_MAGIC;
  }
  binding->schema = schema;
  binding->value = projection;
  binding->clone = clone;
  binding->destroy = destroy;
  binding->ctx = ctx;
  msg->_content_handle = binding;
  return SALTS_OK;
}

int turbo_flow_msg_bind_typed_projection(turbo_flow_msg_t *msg,
    const turbo_flow_data_schema_t *schema, const cmeta_data_desc *data, void *value,
    turbo_flow_projection_clone_fn clone, turbo_flow_destroy_fn destroy, void *ctx) {
  int rc = turbo_flow_data_schema_match(schema, data, schema, data);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_msg_bind_projection(msg, schema, value, clone, destroy, ctx);
  if (rc == SALTS_OK) ((flow_msg_projection_t *)msg->_content_handle)->data = data;
  return rc;
}

int turbo_flow_msg_bind_retained_projection(turbo_flow_msg_t *msg,
                                            turbo_flow_projection_owner_t *owner, void *value) {
  const turbo_flow_projection_owner_config_t *config;
  int rc;
  if (!msg || !owner || !value) return SALTS_EINVAL;
  config = flow_projection_owner_config(owner);
  rc = flow_projection_owner_reserve(owner);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_msg_bind_projection(msg, config->schema, value, config->clone,
                                      config->destroy, config->ctx);
  if (rc != SALTS_OK) {
    flow_projection_owner_release(owner);
    return rc;
  }
  ((flow_msg_projection_t *)msg->_content_handle)->owner = owner;
  return SALTS_OK;
}

const void *turbo_flow_msg_projection(const turbo_flow_msg_t *msg,
                                      const turbo_flow_data_schema_t **schema_out) {
  const flow_msg_projection_t *projection = flow_msg_projection(msg);

  if (schema_out) *schema_out = projection && projection->value ? projection->schema : NULL;
  return projection ? projection->value : NULL;
}

const cmeta_data_desc *turbo_flow_msg_projection_data(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *p = flow_msg_projection(msg);
  return p && p->value ? p->data : NULL;
}

void turbo_flow_msg_clear_projection(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *projection = (flow_msg_projection_t *)flow_msg_projection(msg);
  turbo_flow_projection_owner_t *owner;
  if (!projection || !projection->value || projection->claim_active) return;
  owner = flow_msg_projection_release_value(projection);
  if (flow_msg_projection_empty(projection)) {
    free(projection);
    msg->_content_handle = NULL;
  }
  if (owner) flow_projection_owner_release(owner);
}

void turbo_flow_msg_clear_content(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *projection;
  turbo_flow_projection_owner_t *owner;
  if (!msg) return;
  projection = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!projection || projection->claim_active) return;
  owner = flow_msg_projection_release_value(projection);
  projection->descriptor = NULL;
  projection->owns_descriptor = 0;
  memset(&projection->owned_descriptor, 0, sizeof(projection->owned_descriptor));
  if (flow_msg_projection_empty(projection)) {
    free(projection);
    msg->_content_handle = NULL;
  }
  if (owner) flow_projection_owner_release(owner);
}

int turbo_flow_msg_result_claim(turbo_flow_msg_t *msg, turbo_flow_projection_owner_t *owner,
                                const cmeta_data_desc *data, turbo_flow_result_claim_t **out) {
  const turbo_flow_projection_owner_config_t *config;
  flow_msg_projection_t *source, *prepared;
  turbo_flow_result_claim_t *claim;
  int rc;
  if (!out) return SALTS_EINVAL;
  *out = NULL;
  if (!msg || !owner || !data) return SALTS_EINVAL;
  source = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (source && (source->result_value || source->claim_active)) return SALTS_EALREADY;
  if (source && source->value && !source->data) return SALTS_ENOTSUP;
  config = flow_projection_owner_config(owner);
  rc = turbo_flow_data_schema_match(config->schema, data, config->schema, data);
  if (rc != SALTS_OK) return rc;
  if (data->storage_type->size > config->max_result_bytes) return SALTS_EPROTO;
  rc = flow_projection_owner_reserve(owner);
  if (rc == SALTS_ECANCELED) rc = SALTS_EBUSY;
  if (rc != SALTS_OK) return rc;
  claim = (turbo_flow_result_claim_t *)calloc(1, sizeof(*claim));
  prepared = (flow_msg_projection_t *)calloc(1, sizeof(*prepared));
  if (!claim || !prepared) { free(claim); free(prepared); flow_projection_owner_release(owner); return SALTS_ENOMEM; }
  claim->owns_original = source == NULL;
  /* A claim must be visible even on a message without a prior projection. */
  if (!source) {
    source = (flow_msg_projection_t *)calloc(1, sizeof(*source));
    if (!source) {
      free(claim); free(prepared); flow_projection_owner_release(owner);
      return SALTS_ENOMEM;
    }
    source->magic = FLOW_MSG_PROJECTION_MAGIC;
    msg->_content_handle = source;
  }
  *prepared = *source;
  if (prepared->owns_descriptor) prepared->descriptor = &prepared->owned_descriptor;
  prepared->result_schema = config->schema; prepared->result_data = data;
  prepared->result_clone = config->clone; prepared->result_destroy = config->destroy;
  prepared->result_ctx = config->ctx; prepared->result_owner = owner;
  claim->msg = msg; claim->original = source; claim->prepared = prepared; claim->owner = owner;
  source->claim_active = 1;
  *out = claim;
  return SALTS_OK;
}

int turbo_flow_msg_result_commit(turbo_flow_result_claim_t **io, void **value) {
  turbo_flow_result_claim_t *claim;
  size_t output_size;
  int rc;
  if (!io || !(claim = *io) || !value) return SALTS_EINVAL;
  if (!*value) return SALTS_EPROTO;
  if (claim->msg->_content_handle != claim->original) return SALTS_EBUSY;
  output_size = claim->prepared->result_data->storage_type->size;
  rc = flow_msg_candidate_independent(claim->msg, *value, output_size, NULL, 0u);
  if (rc != SALTS_OK) return SALTS_EPROTO;
  claim->prepared->result_value = *value;
  claim->prepared->claim_active = 0;
  claim->msg->_content_handle = claim->prepared;
  if (claim->original) {
    claim->original->magic = 0u;
    free(claim->original);
  }
  *value = NULL; free(claim); *io = NULL;
  return SALTS_OK;
}

void turbo_flow_msg_result_abort(turbo_flow_result_claim_t **io) {
  turbo_flow_result_claim_t *claim;
  if (!io || !(claim = *io)) return;
  if (claim->original && claim->msg && claim->msg->_content_handle == claim->original)
    claim->original->claim_active = 0;
  if (claim->owns_original && claim->msg && claim->msg->_content_handle == claim->original) {
    claim->msg->_content_handle = NULL;
    flow_msg_projection_destroy(claim->original, NULL);
  }
  free(claim->prepared);
  flow_projection_owner_release(claim->owner);
  free(claim); *io = NULL;
}

const void *turbo_flow_msg_result(const turbo_flow_msg_t *msg,
                                  const turbo_flow_data_schema_t **schema_out,
                                  const cmeta_data_desc **data_out) {
  const flow_msg_projection_t *p = flow_msg_projection(msg);
  if (schema_out) *schema_out = p && p->result_value ? p->result_schema : NULL;
  if (data_out) *data_out = p && p->result_value ? p->result_data : NULL;
  return p ? p->result_value : NULL;
}

void turbo_flow_msg_clear_result(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *p = (flow_msg_projection_t *)flow_msg_projection(msg);
  turbo_flow_projection_owner_t *owner;
  if (!p || !p->result_value || p->claim_active) return;
  owner = flow_msg_result_release_value(p);
  if (flow_msg_projection_empty(p)) { free(p); msg->_content_handle = NULL; }
  if (owner) flow_projection_owner_release(owner);
}

int turbo_flow_msg_set_content_descriptor(turbo_flow_msg_t *msg,
                                          const turbo_flow_content_descriptor_t *descriptor) {
  flow_msg_projection_t *binding;
  if (!msg || turbo_flow_content_descriptor_check(descriptor) != SALTS_OK) {
    return SALTS_EINVAL;
  }
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (binding && binding->claim_active) return SALTS_EBUSY;
  if (binding && binding->schema &&
      flow_msg_descriptor_accepts_schema(descriptor, binding->schema) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  if (binding && binding->descriptor &&
      turbo_flow_content_descriptor_validate(binding->descriptor, descriptor) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  if (!binding) {
    binding = (flow_msg_projection_t *)calloc(1, sizeof(*binding));
    if (!binding) return SALTS_ENOMEM;
    binding->magic = FLOW_MSG_PROJECTION_MAGIC;
    msg->_content_handle = binding;
  }
  binding->descriptor = descriptor;
  binding->owns_descriptor = 0;
  return SALTS_OK;
}

int turbo_flow_msg_copy_content_descriptor(turbo_flow_msg_t *msg,
                                           const turbo_flow_content_descriptor_t *descriptor) {
  flow_msg_projection_t *binding;
  int rc = turbo_flow_msg_set_content_descriptor(msg, descriptor);
  if (rc != SALTS_OK) return rc;
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  binding->owned_descriptor = *descriptor;
  binding->owned_descriptor.size = sizeof(binding->owned_descriptor);
  binding->descriptor = &binding->owned_descriptor;
  binding->owns_descriptor = 1;
  return SALTS_OK;
}

const turbo_flow_content_descriptor_t *
turbo_flow_msg_content_descriptor(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding ? binding->descriptor : NULL;
}

int turbo_flow_msg_content_descriptor_owned(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding && binding->descriptor && binding->owns_descriptor;
}
