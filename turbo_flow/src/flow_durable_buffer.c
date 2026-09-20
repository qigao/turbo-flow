#include "flow_internal.h"
#include "flow_inbox_driver_internal.h"
#include <salts/clock.h>

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

static void flow_durable_buffer_count_rejection(
    turbo_flow_durable_buffer_binding_t *binding, int status, int provider_admission) {
  atomic_uint_fast64_t *counter;
  if (!binding || status == SALTS_OK) return;
  if (!provider_admission) {
    counter = &binding->rejected_message;
  } else if (status == SALTS_ENOSPC) {
    counter = &binding->rejected_backpressure;
  } else if (status == SALTS_ESHUTDOWN) {
    counter = &binding->rejected_closed;
  } else {
    counter = &binding->rejected_provider;
  }
  (void)atomic_fetch_add_explicit(counter, UINT64_C(1), memory_order_relaxed);
}

static turbo_flow_durable_buffer_pressure_state_t flow_durable_buffer_pressure_state(
    const turbo_flow_durable_buffer_pressure_config_t *config,
    const turbo_flow_inbox_snapshot_t *provider) {
  int low = 1;
  if (!config || !provider ||
      (config->high_records == 0u && config->high_retained_bytes == 0u))
    return TURBO_FLOW_DURABLE_PRESSURE_DISABLED;
  if ((config->high_records != 0u && provider->records >= config->high_records) ||
      (config->high_retained_bytes != 0u &&
       provider->retained_bytes >= config->high_retained_bytes))
    return TURBO_FLOW_DURABLE_PRESSURE_HIGH;
  if (config->high_records != 0u && provider->records > config->low_records) low = 0;
  if (config->high_retained_bytes != 0u &&
      provider->retained_bytes > config->low_retained_bytes) low = 0;
  return low ? TURBO_FLOW_DURABLE_PRESSURE_LOW : TURBO_FLOW_DURABLE_PRESSURE_NORMAL;
}

static size_t flow_durable_latency_find(
    const turbo_flow_durable_buffer_binding_t *binding, uint64_t record_id) {
  if (!binding || record_id == 0u) return SIZE_MAX;
  for (size_t i = 0u; i < vec_size(&binding->latency_pending); ++i) {
    const flow_durable_latency_pending_t *entry =
        (const flow_durable_latency_pending_t *)vec_at_const(&binding->latency_pending, i);
    if (entry && entry->record_id == record_id) return i;
  }
  return SIZE_MAX;
}

static int flow_durable_latency_claim_begin(void *ctx) {
  turbo_flow_durable_buffer_binding_t *binding =
      (turbo_flow_durable_buffer_binding_t *)ctx;
  if (!binding ||
      !atomic_load_explicit(&binding->latency_enabled, memory_order_acquire))
    return 0;
  salts_mutex_lock(&binding->latency_mutex);
  return 1;
}

static void flow_durable_latency_claim_end(void *ctx, int observed,
                                           int claim_status, uint64_t record_id) {
  turbo_flow_durable_buffer_binding_t *binding =
      (turbo_flow_durable_buffer_binding_t *)ctx;
  if (!binding || !observed) return;
  if (claim_status == SALTS_OK &&
      atomic_load_explicit(&binding->latency_enabled, memory_order_relaxed)) {
    const size_t index = flow_durable_latency_find(binding, record_id);
    if (index == SIZE_MAX) {
      if (binding->latency_untracked_claims != UINT64_MAX)
        ++binding->latency_untracked_claims;
    } else {
      const flow_durable_latency_pending_t *entry =
          (const flow_durable_latency_pending_t *)vec_at_const(
              &binding->latency_pending, index);
      const uint64_t now = salts_hrtime();
      const uint64_t latency =
          entry && now >= entry->admitted_ns ? now - entry->admitted_ns : 0u;
      if (binding->latency_claim_samples != UINT64_MAX) {
        const uint64_t samples = ++binding->latency_claim_samples;
        if (latency >= binding->latency_claim_mean_ns)
          binding->latency_claim_mean_ns +=
              (latency - binding->latency_claim_mean_ns) / samples;
        else
          binding->latency_claim_mean_ns -=
              (binding->latency_claim_mean_ns - latency) / samples;
      }
      binding->latency_claim_last_ns = latency;
      if (latency > binding->latency_claim_max_ns)
        binding->latency_claim_max_ns = latency;
      (void)vec_erase(&binding->latency_pending, index, NULL);
    }
  }
  salts_mutex_unlock(&binding->latency_mutex);
}

static int flow_durable_buffer_admit_observed(
    turbo_flow_durable_buffer_binding_t *binding,
    const turbo_flow_inbox_record_t *record,
    turbo_flow_inbox_receipt_t *receipt) {
  turbo_flow_inbox_snapshot_t before = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_inbox_snapshot_t after = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  int rc;
  if (!atomic_load_explicit(&binding->latency_enabled, memory_order_acquire))
    return turbo_flow_inbox_admit(binding->inbox, record, receipt);

  salts_mutex_lock(&binding->latency_mutex);
  rc = turbo_flow_inbox_snapshot(binding->inbox, &before);
  if (rc != SALTS_OK) {
    binding->latency_tracking_uncertain = 1;
    rc = turbo_flow_inbox_admit(binding->inbox, record, receipt);
    salts_mutex_unlock(&binding->latency_mutex);
    return rc;
  }

  rc = turbo_flow_inbox_admit(binding->inbox, record, receipt);
  if (rc == SALTS_OK) {
    const int snapshot_rc = turbo_flow_inbox_snapshot(binding->inbox, &after);
    if (snapshot_rc != SALTS_OK) {
      binding->latency_tracking_uncertain = 1;
    } else if (after.admitted == before.admitted + UINT64_C(1) &&
               after.pending_records == before.pending_records + 1u) {
      flow_durable_latency_pending_t entry = {
          receipt->record_id, salts_hrtime()};
      if (turbo_flow_stl_error(vec_push(&binding->latency_pending, &entry)) != SALTS_OK)
        binding->latency_tracking_uncertain = 1;
    } else if (after.admitted != before.admitted ||
               after.pending_records != before.pending_records) {
      binding->latency_tracking_uncertain = 1;
    }
  }
  salts_mutex_unlock(&binding->latency_mutex);
  return rc;
}

int flow_durable_buffer_resolve_bindings(turbo_flow_t *flow) {
  if (!flow) return SALTS_EINVAL;
  if (!flow_durable_buffers_idle(flow)) return SALTS_EBUSY;

  /* Rebuild derived stage identities after parse/reset or a stopped recompile. */
  for (size_t i = 0u; i < vec_size(&flow->durable_buffer_bindings); ++i) {
    turbo_flow_durable_buffer_binding_t **slot =
        (turbo_flow_durable_buffer_binding_t **)vec_at(&flow->durable_buffer_bindings, i);
    if (slot && *slot) {
      if ((*slot)->driver) {
        int rc = flow_inbox_driver_destroy((*slot)->driver);
        if (rc != SALTS_OK) return rc;
        (*slot)->driver = NULL;
      }
      (*slot)->stage_index = SIZE_MAX;
    }
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
  if (flow_msg_has_active_result_claim(message)) return SALTS_EBUSY;
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
  binding->pressure =
      (turbo_flow_durable_buffer_pressure_config_t)TURBO_FLOW_DURABLE_BUFFER_PRESSURE_CONFIG_INIT;
  atomic_init(&binding->rejected_backpressure, 0u);
  atomic_init(&binding->rejected_closed, 0u);
  atomic_init(&binding->rejected_provider, 0u);
  atomic_init(&binding->rejected_message, 0u);
  binding->runtime_started_ns = salts_hrtime();
  binding->baseline_admitted = snapshot.admitted;
  binding->baseline_completed = snapshot.completed;
  binding->baseline_failed = snapshot.failed;
  binding->baseline_retried = snapshot.retried;
  binding->baseline_discarded = snapshot.discarded;
  salts_mutex_init(&binding->latency_mutex);
  rc = turbo_flow_stl_error(vec_init_bytes(
      &binding->latency_pending, sizeof(flow_durable_latency_pending_t),
      _Alignof(flow_durable_latency_pending_t), SIZE_MAX));
  if (rc != SALTS_OK) {
    salts_mutex_destroy(&binding->latency_mutex);
    tstr_freep(&binding->resource_name);
    free(binding);
    return rc;
  }
  atomic_init(&binding->latency_enabled, 0);
  binding->bound = 1;

  rc = turbo_flow_stl_error(vec_push(&flow->durable_buffer_bindings, &binding));
  if (rc != SALTS_OK) {
    vec_destroy(&binding->latency_pending);
    salts_mutex_destroy(&binding->latency_mutex);
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
  if (binding->driver) {
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    rc = flow_inbox_driver_status(binding->driver, &result);
    if (rc != SALTS_OK) return rc;
    if (result.state != TURBO_FLOW_INBOX_SOURCE_EMPTY) return SALTS_EBUSY;
  }
  if (flow_durable_buffer_find_binding(flow, binding->resource_name, &index) != binding ||
      index == SIZE_MAX) {
    return SALTS_ENOENT;
  }
  rc = turbo_flow_stl_error(vec_erase(&flow->durable_buffer_bindings, index, NULL));
  if (rc != SALTS_OK) return rc;
  if (binding->driver) {
    rc = flow_inbox_driver_destroy(binding->driver);
    if (rc != SALTS_OK) return rc;
  }
  binding->bound = 0;
  binding->flow = NULL;
  binding->inbox = NULL;
  vec_destroy(&binding->latency_pending);
  salts_mutex_destroy(&binding->latency_mutex);
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
    if (binding->driver) (void)flow_inbox_driver_destroy(binding->driver);
    binding->bound = 0;
    binding->flow = NULL;
    binding->inbox = NULL;
    vec_destroy(&binding->latency_pending);
    salts_mutex_destroy(&binding->latency_mutex);
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
    flow_durable_buffer_count_rejection(binding, rc, 1);
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "durable buffer provider binding is stale or unavailable");
  }
  memset(generated_admission, 0, sizeof(generated_admission));
  rc = flow_durable_buffer_encode_record(binding, stage, message, &record, generated_admission,
                                         sizeof(generated_admission));
  if (rc != SALTS_OK) {
    flow_durable_buffer_count_rejection(binding, rc, 0);
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "message cannot cross durable buffer boundary");
  }
  rc = flow_durable_buffer_admit_observed(binding, &record, &receipt);
  if (rc != SALTS_OK) {
    flow_durable_buffer_count_rejection(binding, rc, 1);
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "durable buffer provider admission failed");
  }
  return SALTS_OK;
}

int flow_durable_buffers_idle(const turbo_flow_t *flow) {
  if (!flow) return 1;
  for (size_t i = 0u; i < vec_size(&flow->durable_buffer_bindings); ++i) {
    turbo_flow_durable_buffer_binding_t *const *slot =
        (turbo_flow_durable_buffer_binding_t *const *)vec_at_const(&flow->durable_buffer_bindings, i);
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    if (slot && *slot && (*slot)->driver &&
        (flow_inbox_driver_status((*slot)->driver, &result) != SALTS_OK ||
         result.state != TURBO_FLOW_INBOX_SOURCE_EMPTY)) return 0;
  }
  return 1;
}

static int flow_durable_buffer_progress_internal(turbo_flow_durable_buffer_binding_t *binding,
                                                  int draining) {
  turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
  int rc;
  if (!binding || !binding->bound || !binding->flow) return SALTS_EINVAL;
  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) return rc;
  if (binding->driver) {
    rc = flow_inbox_driver_status(binding->driver, &result);
    if (rc != SALTS_OK) return rc;
    if (result.state == TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING ||
        result.state == TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN)
      return result.settlement_status != SALTS_OK ? result.settlement_status : SALTS_EBUSY;
    if (result.state == TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE)
      return flow_inbox_driver_poll(binding->driver, &result);
  }
  if (binding->flow->state != TURBO_FLOW_STATE_STARTED) return SALTS_ESHUTDOWN;
  if (!draining && binding->drain_paused) return SALTS_OK;
  if (!binding->driver) {
    flow_inbox_driver_config_t config = {
      binding->inbox, binding->flow, FLOW_INBOX_DRIVER_BUFFER,
      (uint32_t)binding->stage_index, NULL, binding->max_message_bytes,
      flow_durable_latency_claim_begin, flow_durable_latency_claim_end, binding};
    if (binding->stage_index == SIZE_MAX || binding->stage_index > UINT32_MAX) return SALTS_EINVAL;
    rc = flow_inbox_driver_create(&config, &binding->driver);
    if (rc != SALTS_OK) return rc;
  }
  rc = draining ? flow_inbox_driver_request_drain(binding->driver)
                : flow_inbox_driver_request(binding->driver);
  return rc == SALTS_ENOENT ? SALTS_OK : rc;
}

int turbo_flow_durable_buffer_progress(turbo_flow_durable_buffer_binding_t *binding) {
  return flow_durable_buffer_progress_internal(binding, 0);
}

int turbo_flow_durable_buffer_quiesce(turbo_flow_durable_buffer_binding_t *binding) {
  int rc;
  if (!binding || !binding->bound || !binding->flow) return SALTS_EINVAL;
  rc = flow_durable_buffer_validate_provider(binding);
  return rc == SALTS_OK ? turbo_flow_inbox_close(binding->inbox) : rc;
}

static int flow_durable_buffer_failed_status(turbo_flow_durable_buffer_binding_t *binding) {
  turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
  size_t count = 0u;
  int rc = turbo_flow_inbox_scan_failed(binding->inbox, 0u, &failed, 1u, &count);
  if (rc != SALTS_OK) return rc;
  if (count != 1u) return SALTS_EPROTO;
  if (failed.kind == TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN) return SALTS_ECANCELED;
  return failed.status != SALTS_OK ? failed.status : SALTS_EPROTO;
}

/* One wall-clock budget is shared across upstream drain and every buffer. */
static uint64_t flow_durable_remaining_ms(uint64_t started, uint64_t timeout_ms) {
  uint64_t elapsed;
  if (timeout_ms == UINT64_MAX) return UINT64_MAX;
  elapsed = (salts_hrtime() - started) / UINT64_C(1000000);
  return elapsed >= timeout_ms ? 0u : timeout_ms - elapsed;
}

int turbo_flow_durable_buffer_drain(turbo_flow_durable_buffer_binding_t *binding,
                                     uint64_t timeout_ms) {
  const uint64_t started = salts_hrtime();
  int rc;
  if (!binding || !binding->bound || !binding->flow) return SALTS_EINVAL;
  for (;;) {
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    rc = flow_durable_buffer_validate_provider(binding);
    if (rc != SALTS_OK) return rc;
    if (binding->driver) {
      rc = flow_inbox_driver_status(binding->driver, &result);
      if (rc != SALTS_OK) return rc;
      if (result.state == TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING ||
          result.state == TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN)
        return result.settlement_status != SALTS_OK ? result.settlement_status : SALTS_EBUSY;
    }
    rc = turbo_flow_inbox_snapshot(binding->inbox, &snapshot);
    if (rc != SALTS_OK) return rc;
    if (snapshot.failed_records) return flow_durable_buffer_failed_status(binding);
    if (snapshot.records == 0u && snapshot.in_flight_claims == 0u &&
        result.state == TURBO_FLOW_INBOX_SOURCE_EMPTY) return SALTS_OK;
    if (binding->flow->state != TURBO_FLOW_STATE_STARTED &&
        result.state == TURBO_FLOW_INBOX_SOURCE_EMPTY) return SALTS_EBUSY;
    rc = flow_durable_buffer_progress_internal(binding, 1);
    if (rc != SALTS_OK) return rc;
    /* A non-blocking call gets one progress opportunity, then checks actual idle. */
    if (flow_durable_remaining_ms(started, timeout_ms) == 0u) {
      result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
      if (binding->driver) {
        rc = flow_inbox_driver_status(binding->driver, &result);
        if (rc != SALTS_OK) return rc;
      }
      rc = turbo_flow_inbox_snapshot(binding->inbox, &snapshot);
      if (rc != SALTS_OK) return rc;
      if (snapshot.failed_records) return flow_durable_buffer_failed_status(binding);
      return snapshot.records == 0u && snapshot.in_flight_claims == 0u &&
             result.state == TURBO_FLOW_INBOX_SOURCE_EMPTY ? SALTS_OK : SALTS_ETIMEDOUT;
    }
    salts_sleep_ms(1u);
  }
}

int turbo_flow_durable_buffer_close_and_drain(
    turbo_flow_t *flow, const char *resource_name, uint64_t timeout_ms) {
  turbo_flow_durable_buffer_binding_t *binding;
  const uint64_t started = salts_hrtime();
  int rc;
  if (!flow || !resource_name || resource_name[0] == '\0') return SALTS_EINVAL;
  binding = flow_durable_buffer_find_binding(flow, resource_name, NULL);
  if (!binding) return SALTS_ENOENT;
  rc = turbo_flow_durable_buffer_quiesce(binding);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_durable_buffer_drain(
      binding, flow_durable_remaining_ms(started, timeout_ms));
}

int turbo_flow_durable_buffer_retry_settlement(
    turbo_flow_durable_buffer_binding_t *binding,
    turbo_flow_inbox_source_result_t *result) {
  int rc;
  if (!binding || !binding->bound || !binding->flow || !result) return SALTS_EINVAL;
  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) return rc;
  if (!binding->driver) return SALTS_EINVAL;
  return flow_inbox_driver_retry_settlement(binding->driver, result);
}

int turbo_flow_durable_buffer_reconcile_settlement(
    turbo_flow_durable_buffer_binding_t *binding,
    turbo_flow_inbox_source_result_t *result) {
  int rc;
  if (!binding || !binding->bound || !binding->flow || !result) return SALTS_EINVAL;
  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) return rc;
  if (!binding->driver) return SALTS_EINVAL;
  return flow_inbox_driver_reconcile_settlement(binding->driver, result);
}

static int flow_durable_buffer_operator_binding(
    turbo_flow_t *flow, const char *resource_name,
    turbo_flow_durable_buffer_binding_t **binding_out) {
  turbo_flow_durable_buffer_binding_t *binding;
  int rc;
  if (binding_out) *binding_out = NULL;
  if (!flow || !resource_name || resource_name[0] == '\0' || !binding_out) return SALTS_EINVAL;
  binding = flow_durable_buffer_find_binding(flow, resource_name, NULL);
  if (!binding || !binding->inbox) return SALTS_ENOENT;
  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) return rc;
  *binding_out = binding;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_snapshot(
    turbo_flow_t *flow, const char *resource_name,
    turbo_flow_durable_buffer_snapshot_t *snapshot) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  turbo_flow_inbox_snapshot_t provider = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_inbox_source_result_t driver = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
  turbo_flow_durable_buffer_snapshot_t observed = TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT;
  int rc;

  if (!snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION)
    return SALTS_EINVAL;
  rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_inbox_snapshot(binding->inbox, &provider);
  if (rc != SALTS_OK) return rc;
  if (binding->driver) {
    rc = flow_inbox_driver_status(binding->driver, &driver);
    if (rc != SALTS_OK) return rc;
  }

  observed.provider_generation = provider.generation;
  observed.accepting = provider.accepting;
  observed.drain_paused = binding->drain_paused;
  observed.records = provider.records;
  observed.history_records = provider.history_records;
  observed.pending_records = provider.pending_records;
  observed.failed_records = provider.failed_records;
  observed.in_flight_claims = provider.in_flight_claims;
  observed.retained_bytes = provider.retained_bytes;
  observed.admitted = provider.admitted;
  observed.completed = provider.completed;
  observed.failed = provider.failed;
  observed.retried = provider.retried;
  observed.discarded = provider.discarded;
  observed.driver_state = driver.state;
  observed.active_record_id = driver.record_id;
  observed.graph_status = driver.graph_status;
  observed.settlement_status = driver.settlement_status;
  *snapshot = observed;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_configure_pressure(
    turbo_flow_t *flow, const char *resource_name,
    const turbo_flow_durable_buffer_pressure_config_t *config) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION ||
      (config->high_records == 0u && config->high_retained_bytes == 0u) ||
      (config->high_records == 0u && config->low_records != 0u) ||
      (config->high_retained_bytes == 0u && config->low_retained_bytes != 0u) ||
      (config->high_records != 0u && config->low_records >= config->high_records) ||
      (config->high_retained_bytes != 0u &&
       config->low_retained_bytes >= config->high_retained_bytes))
    return SALTS_EINVAL;
  rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  binding->pressure = *config;
  binding->pressure.size = sizeof(binding->pressure);
  binding->pressure.version = TURBO_FLOW_DURABLE_BUFFER_API_VERSION;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_pressure_snapshot(
    turbo_flow_t *flow, const char *resource_name,
    turbo_flow_durable_buffer_pressure_snapshot_t *snapshot) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  turbo_flow_inbox_snapshot_t provider = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_durable_buffer_pressure_snapshot_t observed =
      TURBO_FLOW_DURABLE_BUFFER_PRESSURE_SNAPSHOT_INIT;
  int rc;
  if (!snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION)
    return SALTS_EINVAL;
  rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_inbox_snapshot(binding->inbox, &provider);
  if (rc != SALTS_OK) return rc;

  observed.state = flow_durable_buffer_pressure_state(&binding->pressure, &provider);
  observed.high_records = binding->pressure.high_records;
  observed.low_records = binding->pressure.low_records;
  observed.high_retained_bytes = binding->pressure.high_retained_bytes;
  observed.low_retained_bytes = binding->pressure.low_retained_bytes;
  observed.rejected_backpressure =
      atomic_load_explicit(&binding->rejected_backpressure, memory_order_relaxed);
  observed.rejected_closed =
      atomic_load_explicit(&binding->rejected_closed, memory_order_relaxed);
  observed.rejected_provider =
      atomic_load_explicit(&binding->rejected_provider, memory_order_relaxed);
  observed.rejected_message =
      atomic_load_explicit(&binding->rejected_message, memory_order_relaxed);
  *snapshot = observed;
  return SALTS_OK;
}

static uint64_t flow_durable_rate_milli(uint64_t count, uint64_t elapsed_ns) {
  if (count == 0u || elapsed_ns == 0u) return 0u;
  if (count > UINT64_MAX / UINT64_C(1000000000000))
    return UINT64_MAX;
  return (count * UINT64_C(1000000000000)) / elapsed_ns;
}

int turbo_flow_durable_buffer_runtime_snapshot(
    turbo_flow_t *flow, const char *resource_name,
    turbo_flow_durable_buffer_runtime_snapshot_t *snapshot) {
  turbo_flow_durable_buffer_binding_t *binding;
  turbo_flow_inbox_snapshot_t provider = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_durable_buffer_runtime_snapshot_t observed =
      TURBO_FLOW_DURABLE_BUFFER_RUNTIME_SNAPSHOT_INIT;
  uint64_t now;
  int rc;

  if (!snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION)
    return SALTS_EINVAL;
  binding = flow_durable_buffer_find_binding(flow, resource_name, NULL);
  if (!binding) return SALTS_ENOENT;
  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_inbox_snapshot(binding->inbox, &provider);
  if (rc != SALTS_OK) return rc;
  if (provider.admitted < binding->baseline_admitted ||
      provider.completed < binding->baseline_completed ||
      provider.failed < binding->baseline_failed ||
      provider.retried < binding->baseline_retried ||
      provider.discarded < binding->baseline_discarded)
    return SALTS_EPROTO;

  now = salts_hrtime();
  observed.elapsed_ns = now >= binding->runtime_started_ns
                            ? now - binding->runtime_started_ns
                            : 0u;
  observed.admitted = provider.admitted - binding->baseline_admitted;
  observed.completed = provider.completed - binding->baseline_completed;
  observed.failed = provider.failed - binding->baseline_failed;
  observed.retried = provider.retried - binding->baseline_retried;
  observed.discarded = provider.discarded - binding->baseline_discarded;
  observed.admitted_per_second_milli =
      flow_durable_rate_milli(observed.admitted, observed.elapsed_ns);
  observed.completed_per_second_milli =
      flow_durable_rate_milli(observed.completed, observed.elapsed_ns);
  observed.failed_per_second_milli =
      flow_durable_rate_milli(observed.failed, observed.elapsed_ns);
  *snapshot = observed;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_latency_snapshot(
    turbo_flow_t *flow, const char *resource_name,
    turbo_flow_durable_buffer_latency_snapshot_t *snapshot) {
  turbo_flow_durable_buffer_binding_t *binding;
  turbo_flow_inbox_snapshot_t provider = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_durable_buffer_latency_snapshot_t observed =
      TURBO_FLOW_DURABLE_BUFFER_LATENCY_SNAPSHOT_INIT;
  uint64_t now;
  size_t tracked;
  int rc;

  if (!snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION)
    return SALTS_EINVAL;
  binding = flow_durable_buffer_find_binding(flow, resource_name, NULL);
  if (!binding) return SALTS_ENOENT;
  rc = flow_durable_buffer_validate_provider(binding);
  if (rc != SALTS_OK) return rc;

  salts_mutex_lock(&binding->latency_mutex);
  rc = turbo_flow_inbox_snapshot(binding->inbox, &provider);
  if (rc != SALTS_OK) {
    salts_mutex_unlock(&binding->latency_mutex);
    return rc;
  }
  now = salts_hrtime();
  if (!atomic_load_explicit(&binding->latency_enabled, memory_order_relaxed)) {
    binding->latency_started_ns = now;
    binding->latency_claim_samples = 0u;
    binding->latency_claim_last_ns = 0u;
    binding->latency_claim_mean_ns = 0u;
    binding->latency_claim_max_ns = 0u;
    binding->latency_untracked_claims = 0u;
    binding->latency_tracking_uncertain = 0;
    (void)vec_clear(&binding->latency_pending);
    atomic_store_explicit(&binding->latency_enabled, 1, memory_order_release);
  }

  tracked = vec_size(&binding->latency_pending);
  if (provider.pending_records == 0u && tracked == 0u)
    binding->latency_tracking_uncertain = 0;
  if (tracked > provider.pending_records)
    binding->latency_tracking_uncertain = 1;

  observed.observation_elapsed_ns =
      now >= binding->latency_started_ns ? now - binding->latency_started_ns : 0u;
  observed.pending_records = provider.pending_records;
  observed.tracked_pending_records = tracked;
  observed.untracked_pending_records =
      provider.pending_records > tracked ? provider.pending_records - tracked : 0u;
  observed.oldest_pending_age_valid =
      !binding->latency_tracking_uncertain &&
      provider.pending_records == tracked;
  if (observed.oldest_pending_age_valid && tracked != 0u) {
    uint64_t oldest = now;
    for (size_t i = 0u; i < tracked; ++i) {
      const flow_durable_latency_pending_t *entry =
          (const flow_durable_latency_pending_t *)vec_at_const(
              &binding->latency_pending, i);
      if (entry && entry->admitted_ns < oldest) oldest = entry->admitted_ns;
    }
    observed.oldest_pending_age_ns = now >= oldest ? now - oldest : 0u;
  }
  observed.claim_samples = binding->latency_claim_samples;
  observed.last_claim_latency_ns = binding->latency_claim_last_ns;
  observed.mean_claim_latency_ns = binding->latency_claim_mean_ns;
  observed.max_claim_latency_ns = binding->latency_claim_max_ns;
  observed.untracked_claims = binding->latency_untracked_claims;
  *snapshot = observed;
  salts_mutex_unlock(&binding->latency_mutex);
  return SALTS_OK;
}

int turbo_flow_durable_buffer_pause_drain(turbo_flow_t *flow, const char *resource_name) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  binding->drain_paused = 1;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_resume_drain(turbo_flow_t *flow, const char *resource_name) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  binding->drain_paused = 0;
  return SALTS_OK;
}

int turbo_flow_durable_buffer_scan_failed(
    turbo_flow_t *flow, const char *resource_name, uint64_t after_record_id,
    turbo_flow_inbox_failed_entry_t *entries, size_t capacity, size_t *out_count) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_inbox_scan_failed(binding->inbox, after_record_id, entries, capacity,
                                      out_count);
}

int turbo_flow_durable_buffer_retry_failed(turbo_flow_t *flow, const char *resource_name,
                                           uint64_t record_id) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  if (!atomic_load_explicit(&binding->latency_enabled, memory_order_acquire))
    return turbo_flow_inbox_retry(binding->inbox, record_id);
  salts_mutex_lock(&binding->latency_mutex);
  rc = turbo_flow_inbox_retry(binding->inbox, record_id);
  salts_mutex_unlock(&binding->latency_mutex);
  return rc;
}

int turbo_flow_durable_buffer_discard_failed(turbo_flow_t *flow, const char *resource_name,
                                             uint64_t record_id) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_inbox_discard(binding->inbox, record_id);
}

int turbo_flow_durable_buffer_scan_history(
    turbo_flow_t *flow, const char *resource_name, uint64_t after_record_id,
    turbo_flow_inbox_history_entry_t *entries, size_t capacity, size_t *out_count) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_inbox_scan_history(binding->inbox, after_record_id, entries, capacity,
                                       out_count);
}

int turbo_flow_durable_buffer_forget(turbo_flow_t *flow, const char *resource_name,
                                     uint64_t record_id) {
  turbo_flow_durable_buffer_binding_t *binding = NULL;
  int rc = flow_durable_buffer_operator_binding(flow, resource_name, &binding);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_inbox_forget(binding->inbox, record_id);
}

int flow_durable_buffers_prepare_retire(turbo_flow_t *flow, uint64_t timeout_ms) {
  const uint64_t started = salts_hrtime();
  size_t *indegree = NULL;
  uint32_t *order = NULL;
  const size_t binding_count = flow ? vec_size(&flow->durable_buffer_bindings) : 0u;
  size_t count, ordered = 0u;
  int rc = SALTS_OK;
  if (!binding_count) return SALTS_OK;
  if (flow->state != TURBO_FLOW_STATE_STARTED) {
    for (size_t i = 0u; i < binding_count; ++i) {
      turbo_flow_durable_buffer_binding_t *binding =
          *(turbo_flow_durable_buffer_binding_t **)vec_at(&flow->durable_buffer_bindings, i);
      rc = turbo_flow_durable_buffer_quiesce(binding);
      if (rc == SALTS_OK)
        rc = turbo_flow_durable_buffer_drain(binding, flow_durable_remaining_ms(started, timeout_ms));
      if (rc != SALTS_OK) return rc;
    }
    return SALTS_OK;
  }
  count = vec_size(&flow->stages);
  if (count == 0u || count > UINT32_MAX || count > SIZE_MAX / sizeof(*indegree)) return SALTS_EPROTO;
  indegree = calloc(count, sizeof(*indegree));
  order = calloc(count, sizeof(*order));
  if (!indegree || !order) { rc = SALTS_ENOMEM; goto done; }
  for (size_t i = 0u; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *edge = vec_at_const(&flow->edges, i);
    if (edge->from_stage >= count || edge->to_stage >= count) { rc = SALTS_EPROTO; goto done; }
    ++indegree[edge->to_stage];
  }
  for (size_t i = 0u; i < count; ++i) if (!indegree[i]) order[ordered++] = (uint32_t)i;
  /* The compiler rejects cycles. O(V*E + V*B) retirement-only traversal, O(V) memory.
   * Close/drain in upstream order: later Inbox admission must remain available
   * for already accepted records crossing another durable cut. */
  for (size_t i = 0u; i < ordered; ++i) {
    for (size_t j = 0u; j < vec_size(&flow->edges); ++j) {
      const flow_edge_plan_impl_t *edge = vec_at_const(&flow->edges, j);
      if (edge->from_stage == order[i] && --indegree[edge->to_stage] == 0u)
        order[ordered++] = edge->to_stage;
    }
  }
  if (ordered != count) { rc = SALTS_EPROTO; goto done; }
  rc = flow_run_prepare_buffer_retire(flow, flow_durable_remaining_ms(started, timeout_ms));
  if (rc != SALTS_OK) goto done;
  for (size_t i = 0u; i < count; ++i) {
    for (size_t j = 0u; j < binding_count; ++j) {
      turbo_flow_durable_buffer_binding_t *binding =
          *(turbo_flow_durable_buffer_binding_t **)vec_at(&flow->durable_buffer_bindings, j);
      if (binding->stage_index != order[i]) continue;
      rc = turbo_flow_durable_buffer_quiesce(binding);
      if (rc == SALTS_OK)
        rc = turbo_flow_durable_buffer_drain(binding, flow_durable_remaining_ms(started, timeout_ms));
      if (rc != SALTS_OK) goto done;
    }
  }
  for (size_t i = 0u; i < binding_count; ++i) {
    turbo_flow_durable_buffer_binding_t *binding =
        *(turbo_flow_durable_buffer_binding_t **)vec_at(&flow->durable_buffer_bindings, i);
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    rc = turbo_flow_inbox_snapshot(binding->inbox, &snapshot);
    if (rc != SALTS_OK) goto done;
    if (snapshot.records || snapshot.in_flight_claims || snapshot.accepting) { rc = SALTS_EBUSY; goto done; }
  }
done:
  free(order);
  free(indegree);
  return rc;
}
