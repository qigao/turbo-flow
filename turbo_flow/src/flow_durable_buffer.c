#include "flow_internal.h"
#include "turbo_flow_durable_buffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_durable_buffer_binding_s {
  turbo_flow_t *flow;
  struct turbo_flow_durable_buffer_binding_s *next;
  tstr resource_name;
  turbo_flow_inbox_t *inbox;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
  uint64_t provider_generation;
  atomic_uint_fast64_t next_generated_sequence;
  uint32_t stage_index;
};

static turbo_flow_durable_buffer_binding_t *flow_durable_buffer_find_resource(
    const turbo_flow_t *flow, const char *resource_name) {
  turbo_flow_durable_buffer_binding_t *binding;

  if (!flow || !resource_name) return NULL;
  for (binding = flow->durable_buffer_bindings; binding; binding = binding->next) {
    if (binding->resource_name && strcmp(binding->resource_name, resource_name) == 0) {
      return binding;
    }
  }
  return NULL;
}

static turbo_flow_durable_buffer_binding_t *flow_durable_buffer_find_stage(
    const turbo_flow_t *flow, uint32_t stage_index) {
  turbo_flow_durable_buffer_binding_t *binding;

  if (!flow) return NULL;
  for (binding = flow->durable_buffer_bindings; binding; binding = binding->next) {
    if (binding->stage_index == stage_index) return binding;
  }
  return NULL;
}

static void flow_durable_buffer_binding_destroy(turbo_flow_durable_buffer_binding_t *binding) {
  if (!binding) return;
  tstr_freep(&binding->resource_name);
  binding->flow = NULL;
  binding->inbox = NULL;
  binding->next = NULL;
  free(binding);
}

void flow_durable_buffer_clear_bindings(turbo_flow_t *flow) {
  turbo_flow_durable_buffer_binding_t *binding;

  if (!flow) return;
  binding = flow->durable_buffer_bindings;
  flow->durable_buffer_bindings = NULL;
  while (binding) {
    turbo_flow_durable_buffer_binding_t *next = binding->next;
    flow_durable_buffer_binding_destroy(binding);
    binding = next;
  }
}

int turbo_flow_durable_buffer_bind(
    turbo_flow_t *flow, const turbo_flow_durable_buffer_binding_config_t *config,
    turbo_flow_durable_buffer_binding_t **out) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_durable_buffer_binding_t *binding;
  int rc;

  if (out) *out = NULL;
  if (!flow || !config || !out || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION || !config->resource_name ||
      config->resource_name[0] == '\0' || !config->inbox ||
      (config->identity_mode != TURBO_FLOW_DURABLE_IDENTITY_GENERATED &&
       config->identity_mode != TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED) ||
      config->max_message_bytes == 0u || config->max_message_bytes == SIZE_MAX) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED ||
      flow->state == TURBO_FLOW_STATE_FAILED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot bind durable buffer after compile");
  }
  if (flow_durable_buffer_find_resource(flow, config->resource_name)) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0,
                                     "durable buffer resource is already bound");
  }

  rc = turbo_flow_inbox_snapshot(config->inbox, &snapshot);
  if (rc != SALTS_OK) return rc;
  if (snapshot.size < sizeof(snapshot) || snapshot.version != TURBO_FLOW_INBOX_API_VERSION ||
      snapshot.generation == 0u) {
    return SALTS_EPROTO;
  }

  binding = (turbo_flow_durable_buffer_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding) return SALTS_ENOMEM;
  binding->resource_name = tstr_dup(config->resource_name);
  if (!binding->resource_name) {
    free(binding);
    return SALTS_ENOMEM;
  }
  binding->flow = flow;
  binding->inbox = config->inbox;
  binding->identity_mode = config->identity_mode;
  binding->max_message_bytes = config->max_message_bytes;
  binding->provider_generation = snapshot.generation;
  atomic_init(&binding->next_generated_sequence, 0u);
  binding->stage_index = FLOW_PLAN_INDEX_NONE;
  binding->next = flow->durable_buffer_bindings;
  flow->durable_buffer_bindings = binding;
  *out = binding;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_unbind(turbo_flow_durable_buffer_binding_t *binding) {
  turbo_flow_durable_buffer_binding_t **cursor;
  turbo_flow_t *flow;

  if (!binding || !binding->flow) return SALTS_EINVAL;
  flow = binding->flow;
  if (flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot unbind durable buffer while flow is started");
  }
  cursor = &flow->durable_buffer_bindings;
  while (*cursor && *cursor != binding) cursor = &(*cursor)->next;
  if (!*cursor) return SALTS_ENOENT;
  *cursor = binding->next;
  flow_durable_buffer_binding_destroy(binding);
  return SALTS_OK;
}

int flow_durable_buffer_resolve_bindings(turbo_flow_t *flow) {
  turbo_flow_durable_buffer_binding_t *binding;

  if (!flow) return SALTS_EINVAL;
  for (binding = flow->durable_buffer_bindings; binding; binding = binding->next) {
    binding->stage_index = FLOW_PLAN_INDEX_NONE;
  }

  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (!stage || !stage->is_buffer) continue;
    binding = flow_durable_buffer_find_resource(flow, stage->resource_name);
    if (!binding) {
      goto unbound;
    }
    if (binding->stage_index != FLOW_PLAN_INDEX_NONE) {
      for (turbo_flow_durable_buffer_binding_t *current = flow->durable_buffer_bindings;
           current; current = current->next) {
        current->stage_index = FLOW_PLAN_INDEX_NONE;
      }
      return flow_set_error(flow, SALTS_EALREADY, stage->line, stage->column,
                            "durable buffer binding may back only one buffer stage");
    }
    binding->stage_index = (uint32_t)i;
  }
  return SALTS_OK;

unbound:
  for (binding = flow->durable_buffer_bindings; binding; binding = binding->next) {
    binding->stage_index = FLOW_PLAN_INDEX_NONE;
  }
  {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    return flow_set_error(flow, SALTS_ENOENT, stage ? stage->line : 0u,
                          stage ? stage->column : 0u,
                          "durable buffer resource is not bound");
  }
}

static int flow_durable_buffer_next_sequence(turbo_flow_durable_buffer_binding_t *binding,
                                             uint64_t *sequence) {
  uint_fast64_t current;

  if (!binding || !sequence) return SALTS_EINVAL;
  current = atomic_load_explicit(&binding->next_generated_sequence, memory_order_relaxed);
  for (;;) {
    if (current == UINT64_MAX) return SALTS_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&binding->next_generated_sequence, &current,
                                              current + 1u, memory_order_relaxed,
                                              memory_order_relaxed)) {
      *sequence = (uint64_t)(current + 1u);
      return SALTS_OK;
    }
  }
}

static int flow_durable_buffer_content(const flow_stage_plan_impl_t *stage,
                                       const turbo_flow_msg_t *message,
                                       turbo_flow_content_descriptor_t *content) {
  const turbo_flow_content_descriptor_t *descriptor;
  const turbo_flow_data_schema_t *schema = NULL;
  const void *projection;
  int rc;

  if (!stage || !message || !content) return SALTS_EINVAL;
  descriptor = turbo_flow_msg_content_descriptor(message);
  projection = turbo_flow_msg_projection(message, &schema);
  if (projection && (!schema || !descriptor)) return SALTS_EPROTO;
  if (descriptor) {
    rc = turbo_flow_content_descriptor_check(descriptor);
    if (rc != SALTS_OK) return rc;
    *content = *descriptor;
    content->size = sizeof(*content);
    return SALTS_OK;
  }
  return turbo_flow_content_descriptor_init(content, TURBO_FLOW_DOMAIN_DATA,
                                            TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                            TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                            "application/octet-stream", stage->name);
}

int flow_durable_buffer_admit_stage(turbo_flow_t *flow, uint32_t stage_index,
                                    const turbo_flow_msg_t *message) {
  turbo_flow_durable_buffer_binding_t *binding;
  const flow_stage_plan_impl_t *stage;
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
  turbo_flow_content_descriptor_t content = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  char admission_id[TURBO_FLOW_DURABLE_ADMISSION_ID_MAX + 1u];
  vstr source_id;
  vstr durable_admission_id;
  vstr correlation = {NULL, 0u};
  uint64_t source_sequence = 0u;
  int written;
  int rc;

  if (!flow || !message) return SALTS_EINVAL;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
  binding = flow_durable_buffer_find_stage(flow, stage_index);
  if (!stage || !stage->is_buffer || !binding || !binding->inbox) {
    return flow_set_error_keep_state(flow, SALTS_EPROTO, stage ? stage->line : 0u,
                                     stage ? stage->column : 0u,
                                     "durable buffer binding is not materialized");
  }
  rc = flow_msg_payload_validate(message);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, stage->line, stage->column,
                                     "durable buffer payload is not persistable");
  }
  if (message->payload.len > binding->max_message_bytes) {
    return flow_set_error_keep_state(flow, SALTS_ENOSPC, stage->line, stage->column,
                                     "durable buffer message exceeds configured byte limit");
  }
  if (message->transport_context) {
    return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                     "durable buffer cannot persist transport context");
  }
  if (turbo_flow_msg_result(message, NULL, NULL)) {
    return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                     "durable buffer cannot persist result sidecar state");
  }
  rc = flow_durable_buffer_content(stage, message, &content);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "durable buffer content descriptor is not persistable");
  }

  if (binding->identity_mode == TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED) {
    rc = turbo_flow_msg_durable_identity(message, &identity);
    if (rc != SALTS_OK) {
      return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                       "durable buffer requires stable message identity");
    }
    source_id = identity.source_id;
    durable_admission_id = identity.admission_id;
    correlation = identity.correlation;
    source_sequence = identity.source_sequence;
  } else {
    rc = flow_durable_buffer_next_sequence(binding, &source_sequence);
    if (rc != SALTS_OK) {
      return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                       "durable buffer generated identity is exhausted");
    }
    written = snprintf(admission_id, sizeof(admission_id), "g%llu:%llu",
                       (unsigned long long)binding->provider_generation,
                       (unsigned long long)source_sequence);
    if (written <= 0 || (size_t)written >= sizeof(admission_id)) {
      return flow_set_error_keep_state(flow, SALTS_ERANGE, stage->line, stage->column,
                                       "durable buffer generated admission identity is too long");
    }
    source_id = vstr_from_cstr(stage->name);
    durable_admission_id = vstr_from_buf(admission_id, (size_t)written);
  }

  turbo_flow_inbox_record_init(&record);
  record.source_id = source_id;
  record.admission_id = durable_admission_id;
  record.source_sequence = source_sequence;
  record.timestamp_ns = message->ts_ns;
  record.message_type = message->type;
  record.message_flags = message->flags;
  record.content = content;
  record.correlation = correlation;
  record.payload = message->payload;
  rc = turbo_flow_inbox_admit(binding->inbox, &record, &receipt);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "durable buffer admission failed");
  }
  return SALTS_OK;
}
