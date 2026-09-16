#include "flow_internal.h"

#include "turbo_flow_projection.h"
#include "salts_uuid.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_DURABLE_OPAQUE_SCHEMA "turbo-flow.durable.opaque"
#define FLOW_DURABLE_OPAQUE_TYPE "OpaquePayload"
#define FLOW_DURABLE_OPAQUE_SCHEMA_VERSION UINT32_C(1)

static turbo_flow_durable_buffer_binding_t *
flow_durable_buffer_find_binding(const turbo_flow_t *flow, const char *resource_name,
                                 size_t *index_out) {
  size_t i;

  if (index_out) *index_out = SIZE_MAX;
  if (!flow || !resource_name || resource_name[0] == '\0') return NULL;
  for (i = 0u; i < vec_size(&flow->durable_buffer_bindings); ++i) {
    turbo_flow_durable_buffer_binding_t *const *slot =
        (turbo_flow_durable_buffer_binding_t *const *)vec_at_const(
            &flow->durable_buffer_bindings, i);
    turbo_flow_durable_buffer_binding_t *binding = slot ? *slot : NULL;
    if (!binding || !binding->bound || !binding->resource_name) continue;
    if (strcmp(binding->resource_name, resource_name) == 0) {
      if (index_out) *index_out = i;
      return binding;
    }
  }
  return NULL;
}

/* The caller keeps the borrowed handle immutable. Providers must fence takeover
 * atomically with admission; this snapshot detects already-stale bindings. */
static int flow_durable_buffer_validate_provider(const turbo_flow_durable_buffer_binding_t *binding) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  int rc;
  if (!binding || !binding->inbox) return SALTS_EINVAL;
  if (binding->inbox->ops != binding->provider_ops ||
      binding->inbox->ctx != binding->provider_ctx) return SALTS_ECANCELED;
  rc = turbo_flow_inbox_snapshot(binding->inbox, &snapshot);
  if (rc != SALTS_OK) return rc;
  return snapshot.generation == binding->provider_generation ? SALTS_OK : SALTS_ECANCELED;
}

int flow_durable_buffer_resolve_bindings(turbo_flow_t *flow) {
  if (!flow) return SALTS_EINVAL;

  /* Rebuild derived stage identities after parse/reset or a stopped recompile. */
  for (size_t i = 0u; i < vec_size(&flow->durable_buffer_bindings); ++i) {
    turbo_flow_durable_buffer_binding_t **slot =
        (turbo_flow_durable_buffer_binding_t **)vec_at(&flow->durable_buffer_bindings, i);
    if (slot && *slot) (*slot)->stage_index = SIZE_MAX;
  }
  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    turbo_flow_durable_buffer_binding_t *binding;

    if (!stage || !stage->is_buffer) continue;
    binding = flow_durable_buffer_find_binding(flow, stage->resource_name, NULL);
    if (!binding || !binding->inbox) {
      return flow_set_error_keep_state(flow, SALTS_ENOENT, stage->line, stage->column,
                                       "durable buffer resource is not bound");
    }
    if (binding->stage_index != SIZE_MAX) {
      return flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                       "durable buffer binding must identify exactly one buffer");
    }
    int rc = flow_durable_buffer_validate_provider(binding);
    if (rc != SALTS_OK) {
      return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                       "durable buffer provider binding is stale or unavailable");
    }
    binding->stage_index = i;
  }
  return SALTS_OK;
}

static int flow_durable_buffer_next_sequence(turbo_flow_durable_buffer_binding_t *binding,
                                             uint64_t *sequence_out) {
  uint_fast64_t observed;

  if (!binding || !sequence_out) return SALTS_EINVAL;
  observed = atomic_load_explicit(&binding->next_sequence, memory_order_relaxed);
  for (;;) {
    if (observed == UINT64_MAX) return SALTS_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&binding->next_sequence, &observed, observed + 1u,
                                              memory_order_relaxed, memory_order_relaxed)) {
      *sequence_out = (uint64_t)(observed + 1u);
      return SALTS_OK;
    }
  }
}

static int flow_durable_buffer_record_bytes(vstr source_id, vstr admission_id, vstr correlation,
                                            vstr payload, size_t *out) {
  size_t total = 0u;
  const size_t lengths[] = {source_id.len, admission_id.len, correlation.len, payload.len};

  if (!out) return SALTS_EINVAL;
  for (size_t i = 0u; i < sizeof(lengths) / sizeof(lengths[0]); ++i) {
    if (lengths[i] > SIZE_MAX - total) return SALTS_ERANGE;
    total += lengths[i];
  }
  *out = total;
  return SALTS_OK;
}

static int flow_durable_buffer_generic_content(const flow_stage_plan_impl_t *stage,
                                               turbo_flow_content_descriptor_t *content) {
  int rc;

  if (!stage || !stage->name || !content || tstr_len(stage->name) > TURBO_FLOW_CONTENT_IDENTITY_MAX) {
    return SALTS_ERANGE;
  }
  rc = turbo_flow_content_descriptor_init(content, TURBO_FLOW_DOMAIN_DATA,
                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                          TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                          "application/octet-stream", stage->name);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_content_descriptor_declare_schema(content, FLOW_DURABLE_OPAQUE_SCHEMA,
                                                      FLOW_DURABLE_OPAQUE_TYPE,
                                                      FLOW_DURABLE_OPAQUE_SCHEMA_VERSION);
}

static int flow_durable_buffer_encode_record(turbo_flow_durable_buffer_binding_t *binding,
                                             const flow_stage_plan_impl_t *stage,
                                             const turbo_flow_msg_t *message,
                                             turbo_flow_inbox_record_t *record,
                                             char *generated_admission,
                                             size_t generated_admission_capacity) {
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  const turbo_flow_content_descriptor_t *descriptor;
  const void *projection;
  size_t retained_bytes;
  uint64_t sequence = 0u;
  int count;
  int rc;

  if (!binding || !stage || !message || !record || !generated_admission ||
      generated_admission_capacity == 0u) {
    return SALTS_EINVAL;
  }
  if (flow_msg_payload_validate(message) != SALTS_OK) return SALTS_EINVAL;
  if (message->transport_context != NULL) return SALTS_ENOTSUP;
  if (turbo_flow_msg_result(message, NULL, NULL) != NULL) return SALTS_ENOTSUP;

  descriptor = turbo_flow_msg_content_descriptor(message);
  projection = turbo_flow_msg_projection(message, NULL);
  if (projection && (!descriptor || message->payload.len == 0u)) return SALTS_ENOTSUP;

  turbo_flow_inbox_record_init(record);
  if (descriptor) {
    if (turbo_flow_content_descriptor_check(descriptor) != SALTS_OK ||
        descriptor->domain != TURBO_FLOW_DOMAIN_DATA ||
        (descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u) {
      return SALTS_EPROTO;
    }
    record->content = *descriptor;
    record->content.size = sizeof(record->content);
  } else {
    rc = flow_durable_buffer_generic_content(stage, &record->content);
    if (rc != SALTS_OK) return rc;
  }

  if (binding->identity_mode == TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED) {
    rc = turbo_flow_msg_durable_identity(message, &identity);
    if (rc == SALTS_ENOENT) return SALTS_EINVAL;
    if (rc != SALTS_OK) return rc;
    record->source_id = identity.source_id;
    record->admission_id = identity.admission_id;
    record->correlation = identity.correlation;
    record->source_sequence = identity.source_sequence;
  } else {
    if (tstr_len(stage->name) > TURBO_FLOW_DURABLE_SOURCE_ID_MAX) return SALTS_ERANGE;
    rc = flow_durable_buffer_next_sequence(binding, &sequence);
    if (rc != SALTS_OK) return rc;
    count = snprintf(generated_admission, generated_admission_capacity, "g%" PRIu64 ":%s:%" PRIu64,
                     binding->provider_generation, binding->admission_namespace, sequence);
    if (count < 0 || (size_t)count >= generated_admission_capacity) return SALTS_ERANGE;
    record->source_id = vstr_from_buf(stage->name, tstr_len(stage->name));
    record->admission_id = vstr_from_buf(generated_admission, (size_t)count);
    record->correlation = (vstr){NULL, 0u};
    record->source_sequence = sequence;
  }

  record->timestamp_ns = message->ts_ns;
  record->message_type = message->type;
  record->message_flags = message->flags;
  record->payload = message->payload;

  rc = flow_durable_buffer_record_bytes(record->source_id, record->admission_id,
                                        record->correlation, record->payload, &retained_bytes);
  if (rc != SALTS_OK) return rc;
  if (retained_bytes > binding->max_message_bytes) return SALTS_ENOSPC;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_bind(
    turbo_flow_t *flow, const turbo_flow_durable_buffer_binding_config_t *config,
    turbo_flow_durable_buffer_binding_t **out) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_durable_buffer_binding_t *binding;
  int rc;

  if (out) *out = NULL;
  if (!flow || !config || !out || config->size != sizeof(*config) ||
      config->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION || !config->resource_name ||
      config->resource_name[0] == '\0' || !config->inbox || config->max_message_bytes == 0u ||
      config->max_message_bytes == SIZE_MAX ||
      (config->identity_mode != TURBO_FLOW_DURABLE_IDENTITY_GENERATED &&
       config->identity_mode != TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED)) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED ||
      flow->state == TURBO_FLOW_STATE_FAILED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "durable buffer binding must be configured before compile");
  }
  if (flow_durable_buffer_find_binding(flow, config->resource_name, NULL)) return SALTS_EALREADY;

  for (size_t i = 0u; i < vec_size(&flow->durable_buffer_bindings); ++i) {
    turbo_flow_durable_buffer_binding_t *const *slot =
        (turbo_flow_durable_buffer_binding_t *const *)vec_at_const(&flow->durable_buffer_bindings, i);
    if (slot && *slot && (*slot)->bound && (*slot)->provider_ops == config->inbox->ops &&
        (*slot)->provider_ctx == config->inbox->ctx) return SALTS_EALREADY;
  }

  rc = turbo_flow_inbox_snapshot(config->inbox, &snapshot);
  if (rc != SALTS_OK) return rc;
  if (snapshot.generation == 0u) return SALTS_EPROTO;

  binding = (turbo_flow_durable_buffer_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding) return SALTS_ENOMEM;
  binding->resource_name = tstr_dup(config->resource_name);
  if (!binding->resource_name) {
    free(binding);
    return SALTS_ENOMEM;
  }
  if (config->identity_mode == TURBO_FLOW_DURABLE_IDENTITY_GENERATED) {
    salts_uuid_t uuid;
    rc = salts_uuid_v4_generate(&uuid);
    if (rc == SALTS_OK)
      rc = salts_uuid_format(&uuid, binding->admission_namespace, sizeof(binding->admission_namespace));
    if (rc != SALTS_OK) {
      tstr_freep(&binding->resource_name);
      free(binding);
      return rc;
    }
  }
  binding->provider_ops = config->inbox->ops;
  binding->provider_ctx = config->inbox->ctx;
  binding->flow = flow;
  binding->inbox = config->inbox;
  binding->identity_mode = config->identity_mode;
  binding->max_message_bytes = config->max_message_bytes;
  binding->provider_generation = snapshot.generation;
  binding->stage_index = SIZE_MAX;
  atomic_init(&binding->next_sequence, 0u);
  binding->bound = 1;

  rc = turbo_flow_stl_error(vec_push(&flow->durable_buffer_bindings, &binding));
  if (rc != SALTS_OK) {
    tstr_freep(&binding->resource_name);
    free(binding);
    return rc;
  }
  *out = binding;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_unbind(turbo_flow_durable_buffer_binding_t *binding) {
  turbo_flow_t *flow;
  size_t index;
  int rc;

  if (!binding || !binding->bound || !(flow = binding->flow)) return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) return SALTS_EBUSY;
  if (flow_durable_buffer_find_binding(flow, binding->resource_name, &index) != binding ||
      index == SIZE_MAX) {
    return SALTS_ENOENT;
  }
  rc = turbo_flow_stl_error(vec_erase(&flow->durable_buffer_bindings, index, NULL));
  if (rc != SALTS_OK) return rc;
  binding->bound = 0;
  binding->flow = NULL;
  binding->inbox = NULL;
  tstr_freep(&binding->resource_name);
  free(binding);
  return SALTS_OK;
}

void flow_durable_buffer_clear_bindings(turbo_flow_t *flow) {
  if (!flow) return;
  for (size_t i = 0u; i < vec_size(&flow->durable_buffer_bindings); ++i) {
    turbo_flow_durable_buffer_binding_t **slot =
        (turbo_flow_durable_buffer_binding_t **)vec_at(&flow->durable_buffer_bindings, i);
    turbo_flow_durable_buffer_binding_t *binding = slot ? *slot : NULL;
    if (!binding) continue;
    binding->bound = 0;
    binding->flow = NULL;
    binding->inbox = NULL;
    tstr_freep(&binding->resource_name);
    free(binding);
  }
  (void)turbo_flow_stl_error(vec_clear(&flow->durable_buffer_bindings));
}

int flow_durable_buffer_admit_stage(turbo_flow_t *flow, uint32_t stage_index,
                                    const turbo_flow_msg_t *message) {
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
  turbo_flow_durable_buffer_binding_t *binding;
  const flow_stage_plan_impl_t *stage;
  char generated_admission[TURBO_FLOW_DURABLE_ADMISSION_ID_MAX + 1u];
  int rc;

  if (!flow || !message || stage_index >= vec_size(&flow->stages)) return SALTS_EINVAL;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, (size_t)stage_index);
  if (!stage || !stage->is_buffer || !stage->resource_name) return SALTS_EINVAL;
  binding = flow_durable_buffer_find_binding(flow, stage->resource_name, NULL);
  if (!binding || !binding->inbox) {
    return flow_set_error_keep_state(flow, SALTS_ENOENT, stage->line, stage->column,
                                     "durable buffer resource is not bound");
  }

  if (binding->stage_index != (size_t)stage_index) {
    return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                     "durable buffer binding does not match compiled stage");
  }

  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "durable buffer provider binding is stale or unavailable");
  }
  memset(generated_admission, 0, sizeof(generated_admission));
  rc = flow_durable_buffer_encode_record(binding, stage, message, &record, generated_admission,
                                         sizeof(generated_admission));
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "message cannot cross durable buffer boundary");
  }
  rc = turbo_flow_inbox_admit(binding->inbox, &record, &receipt);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "durable buffer provider admission failed");
  }
  return SALTS_OK;
}
