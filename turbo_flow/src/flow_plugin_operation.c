#include "flow_plugin_operation_internal.h"
#include "turbo_flow_stl_error_internal.h"
#include <stdio.h>
#include <string.h>

#define FLOW_OPERATION_CLOSED (UINT32_C(1) << 31)
typedef struct flow_operation_budget_s {
  uint32_t limit, used;
  int status;
} flow_operation_budget_t;
int flow_plugin_operation_callback_result(turbo_flow_plugin_operation_error_v3_t *error,
                                          uint32_t phase, int status) {
  if (error->size == sizeof(*error) && error->abi_major == TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR &&
      error->abi_minor == TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR)
    return status;
  /* An unknown header grants no access to the DLL diagnostic tail. Replace it with a new
     Host diagnostic, not a normalized copy of the untrusted result. */
  turbo_flow_plugin_operation_error_v3_t diagnostic;
  turbo_flow_plugin_operation_error_v3_init(&diagnostic);
  diagnostic.status = SALTS_EINVAL;
  diagnostic.phase = phase;
  snprintf(diagnostic.message, sizeof(diagnostic.message), "invalid operation error ABI");
  *error = diagnostic;
  return SALTS_EINVAL;
}
static int operation_error(turbo_flow_config_error_t *error, size_t index, const char *field,
                           int rc) {
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  error->status = rc;
  snprintf(error->path, sizeof(error->path), "$.operation_bindings[%zu].%s", index, field);
  snprintf(error->message, sizeof(error->message), "operation %s failed", field);
  return rc;
}
static int same_name(const char *a, const char *b) { return a && b ? !strcmp(a, b) : a == b; }
static int checked_add(size_t *sum, size_t add) {
  if (add > SIZE_MAX - *sum) return SALTS_EINVAL;
  *sum += add;
  return SALTS_OK;
}
static int operation_graph_validate(turbo_flow_t *flow,
                                    const turbo_flow_resolved_operation_binding_view_t *v,
                                    const char **resource) {
  const turbo_flow_operation_descriptor_t *m = turbo_flow_find_operation(flow, v->operation);
  int found = 0;
  if (!m) return SALTS_EINVAL;
  if (m->size != sizeof(*m)) return SALTS_EINVAL;
  if (m->version != v->version || m->input_domain != m->output_domain ||
      !same_name(m->input_type, m->output_type))
    return SALTS_EPROTO;
  if (m->domain != TURBO_FLOW_DOMAIN_DATA || m->input_domain != TURBO_FLOW_DOMAIN_DATA ||
      !m->input_type || m->scope.data != TURBO_FLOW_DATA_SCOPE_MESSAGE ||
      m->scope.state != TURBO_FLOW_STATE_SCOPE_NONE ||
      m->scope.lifetime != TURBO_FLOW_LIFETIME_CALL ||
      m->scope.concurrency != TURBO_FLOW_CONCURRENCY_INLINE_LANE ||
      m->scope.authority != TURBO_FLOW_AUTHORITY_DATA_MUTATION ||
      m->flags != TURBO_FLOW_OPERATION_STAGE ||
      m->execution_mask != TURBO_FLOW_OPERATION_EXEC_INLINE ||
      m->runtime.handoff != TURBO_FLOW_HANDOFF_DIRECT || m->runtime.settlement ||
      m->runtime.deadline_ms || m->runtime.cancellation != TURBO_FLOW_CANCELLATION_NONE)
    return SALTS_ENOTSUP;
  for (size_t j = 0; j < turbo_flow_stage_count(flow); ++j) {
    const turbo_flow_stage_plan_t *s = turbo_flow_stage_at(flow, j);
    if (!s || !same_name(s->operation_name, v->operation)) continue;
    if (!same_name(s->resource_name, v->resource)) continue;
    if (s->is_source || s->exec.kind != TURBO_FLOW_EXEC_INLINE ||
        s->effects != TURBO_FLOW_STAGE_EFFECT_NONE)
      return SALTS_ENOTSUP;
    *resource = s->resource_name;
    found = 1;
  }
  return found ? SALTS_OK : SALTS_EINVAL;
}
int flow_plugin_operations_prepare(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                   const turbo_flow_resolved_config_t *resolved, turbo_flow_t *flow,
                                   turbo_flow_plugin_result_domain_t *domain, size_t budget,
                                   vec_t *bindings, turbo_flow_config_error_t *error) {
  turbo_flow_plugin_operation_catalog_v3_t catalog;
  size_t count = 0, required = 0;
  int rc = turbo_flow_resolved_config_operation_binding_count(resolved, &count);
  if (rc != SALTS_OK) return operation_error(error, 0, "config", rc);
  rc = turbo_flow_stl_error(vec_init_bytes(bindings, sizeof(flow_plugin_operation_binding_t),
                                           _Alignof(turbo_flow_max_align_t), count));
  if (rc != SALTS_OK) return operation_error(error, 0, "allocation", rc);
  if (count || domain) {
    rc = flow_plugin_result_domain_admit(domain, snapshot, count);
    if (rc != SALTS_OK) return operation_error(error, 0, "result_domain", rc);
  }
  if (!count) return SALTS_OK;
  turbo_flow_plugin_operation_catalog_v3_init(&catalog);
  rc = turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog);
  if (rc != SALTS_OK) return operation_error(error, 0, "catalog", rc);
  rc = turbo_flow_stl_error(vec_resize(bindings, count));
  if (rc != SALTS_OK) return operation_error(error, 0, "allocation", rc);
  memset(vec_data(bindings), 0, count * sizeof(flow_plugin_operation_binding_t));
  for (size_t i = 0; i < count; ++i) {
    turbo_flow_resolved_operation_binding_view_t v =
        TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
    turbo_flow_result_memory_requirements_t cost;
    flow_plugin_operation_binding_t *b = vec_at(bindings, i);
    const turbo_flow_plugin_operation_v3_t *op = NULL;
    int plugin_found = 0, operation_found = 0;
    const char *resource = NULL;
    size_t capacity;
    rc = turbo_flow_resolved_config_operation_binding_at(resolved, i, &v);
    if (rc != SALTS_OK) return operation_error(error, i, "config", rc);
    for (size_t j = 0; j < i; ++j) {
      const flow_plugin_operation_binding_t *previous = vec_at_const(bindings, j);
      if (same_name(previous->request.operation_name, v.operation) &&
          same_name(previous->request.resource_name, v.resource))
        return operation_error(error, i, "operation", SALTS_EALREADY);
    }
    for (size_t j = 0; j < catalog.count; ++j) {
      if (!same_name(catalog.entries[j].plugin_id, v.plugin)) continue;
      plugin_found = 1;
      if (!same_name(catalog.entries[j].operation.operation_name, v.operation)) continue;
      operation_found = 1;
      if (catalog.entries[j].operation.operation_version == v.version)
        op = &catalog.entries[j].operation;
    }
    if (!op)
      return operation_error(error, i,
                             !plugin_found      ? "plugin"
                             : !operation_found ? "operation"
                                                : "version",
                             SALTS_EINVAL);
    if (!same_name(op->input.data->stable_id, v.input_schema) ||
        op->input.schema_version != v.input_schema_version)
      return operation_error(error, i, "input_schema", SALTS_EPROTO);
    if (!same_name(op->output.data->stable_id, v.output_schema) ||
        op->output.schema_version != v.output_schema_version)
      return operation_error(error, i, "output_schema", SALTS_EPROTO);
    if (v.execution != TURBO_FLOW_CONFIG_OPERATION_INLINE ||
        v.threading != TURBO_FLOW_CONFIG_OPERATION_THREAD_SAFE ||
        v.cancellation != TURBO_FLOW_CONFIG_OPERATION_CANCEL_NONE || v.deadline_ms ||
        v.permission_count)
      return operation_error(error, i, "profile", SALTS_ENOTSUP);
    rc = operation_graph_validate(flow, &v, &resource);
    if (rc != SALTS_OK) return operation_error(error, i, "graph", rc);
    if (v.resource) {
      turbo_flow_resolved_channel_view_t channel = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
      rc = turbo_flow_resolved_config_channel(resolved, v.resource, &channel);
      if (rc != SALTS_OK) return operation_error(error, i, "resource", SALTS_EINVAL);
    }
    if (!v.max_inflight || !v.max_input_bytes || !v.max_result_bytes || !v.max_retained_bytes ||
        !v.max_steps)
      return operation_error(error, i, "limits", SALTS_EINVAL);
    capacity = v.max_retained_bytes / v.max_result_bytes;
    if (v.max_inflight > op->limits.max_inflight ||
        v.max_input_bytes > op->limits.max_input_bytes ||
        v.max_result_bytes > op->limits.max_result_bytes ||
        v.max_retained_bytes > op->limits.max_retained_bytes ||
        v.max_steps > op->limits.max_steps || capacity < v.max_inflight ||
        capacity > FLOW_PLUGIN_OPERATION_MAX_INFLIGHT ||
        op->input.data->storage_type->size > v.max_input_bytes ||
        op->output.data->storage_type->size > v.max_result_bytes)
      return operation_error(error, i, "limits", SALTS_ENOSPC);
    turbo_flow_result_memory_requirements_init(&cost);
    rc = turbo_flow_result_memory_requirements(capacity, v.max_result_bytes, &cost);
    /* Per-generation admission conservatively charges the bridge as well as the binding;
       the caller's separately bounded domain has already reserved the bridge storage. */
    if (rc != SALTS_OK || checked_add(&required, op->max_session_bytes) != SALTS_OK ||
        checked_add(&required, op->max_result_context_bytes) != SALTS_OK ||
        checked_add(&required, cost.peak_metadata_bytes) != SALTS_OK ||
        checked_add(&required, sizeof(*b)) != SALTS_OK ||
        checked_add(&required, sizeof(flow_plugin_result_entry_t)) != SALTS_OK)
      return operation_error(error, i, "memory", SALTS_EINVAL);
    b->operation = *op;
    turbo_flow_plugin_operation_request_v3_init(&b->request);
    b->request.resolved = resolved;
    b->request.operation_name = op->operation_name;
    b->request.resource_name = resource;
    b->request.limits.max_inflight = v.max_inflight;
    b->request.limits.max_input_bytes = v.max_input_bytes;
    b->request.limits.max_result_bytes = v.max_result_bytes;
    b->request.limits.max_retained_bytes = v.max_retained_bytes;
    b->request.limits.max_steps = v.max_steps;
    turbo_flow_plugin_operation_error_v3_init(&b->error);
    atomic_init(&b->admission, 0);
    salts_mutex_init(&b->error_mutex);
    if (!b->error_mutex) return operation_error(error, i, "allocation", SALTS_ENOMEM);
  }
  return required > budget ? operation_error(error, 0, "memory", SALTS_ENOSPC) : SALTS_OK;
}
int flow_plugin_operations_preflight(vec_t *bindings, turbo_flow_config_error_t *error) {
  for (size_t i = 0; i < vec_size(bindings); ++i) {
    flow_plugin_operation_binding_t *b = vec_at(bindings, i);
    int rc = b->operation.preflight(b->operation.factory_ctx, &b->request, &b->error);
    rc = flow_plugin_operation_callback_result(&b->error,
                                               TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT, rc);
    if (rc != SALTS_OK) return operation_error(error, i, "preflight", rc);
  }
  return SALTS_OK;
}
static int operation_charge(void *ctx, uint32_t steps) {
  flow_operation_budget_t *b = ctx;
  if (b->status != SALTS_OK) return b->status;
  if (!steps) return b->status = SALTS_EINVAL;
  if (steps > b->limit - b->used) return b->status = SALTS_ENOSPC;
  b->used += steps;
  return SALTS_OK;
}
static int operation_invoke(turbo_flow_msg_t *msg, void *ctx) {
  flow_plugin_operation_binding_t *b = ctx;
  const turbo_flow_data_schema_t *schema = NULL;
  const void *value = turbo_flow_msg_projection(msg, &schema);
  const cmeta_data_desc *data = turbo_flow_msg_projection_data(msg);
  turbo_flow_plugin_operation_error_v3_t error;
  turbo_flow_plugin_operation_input_v3_t input;
  turbo_flow_plugin_operation_budget_v3_t budget;
  flow_operation_budget_t charged = {b->request.limits.max_steps, 0, SALTS_OK};
  turbo_flow_result_claim_t *claim = NULL;
  void *candidate = NULL;
  unsigned admission;
  int rc, independent = 0, accepted = 0;
  turbo_flow_plugin_operation_error_v3_init(&error);
  error.phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT;
  if (!data || !value) {
    rc = SALTS_ENOTSUP;
    goto done;
  }
  rc = turbo_flow_data_schema_match(b->operation.input.projection, b->operation.input.data, schema,
                                    data);
  if (rc != SALTS_OK) goto done;
  admission = atomic_load(&b->admission);
  for (;;) {
    if (admission & FLOW_OPERATION_CLOSED) {
      rc = SALTS_EBUSY;
      goto done;
    }
    if (admission >= b->request.limits.max_inflight) {
      rc = SALTS_ENOSPC;
      goto done;
    }
    if (atomic_compare_exchange_weak(&b->admission, &admission, admission + 1)) break;
  }
  accepted = 1;
  error.phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT;
  rc = turbo_flow_msg_result_claim(msg, b->result->owner, b->operation.output.data, &claim);
  if (rc != SALTS_OK) goto done;
  turbo_flow_plugin_operation_input_v3_init(&input);
  input.value = value;
  input.data = data;
  input.bytes = data->storage_type->size;
  turbo_flow_plugin_operation_budget_v3_init(&budget);
  budget.ctx = &charged;
  budget.charge = operation_charge;
  error.phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE;
  rc = b->operation.vtable.execute(b->session, &input, &budget, &candidate, &error);
  rc = flow_plugin_operation_callback_result(&error, TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE, rc);
  if (rc == SALTS_OK && charged.status != SALTS_OK) rc = charged.status;
  independent =
      turbo_flow_value_require_disjoint(value, input.bytes, candidate,
                                        b->operation.output.data->storage_type->size) == SALTS_OK;
  if (independent && msg->payload.data && msg->payload.len)
    independent =
        turbo_flow_value_require_disjoint(msg->payload.data, msg->payload.len, candidate,
                                          b->operation.output.data->storage_type->size) == SALTS_OK;
  if (!independent && rc == SALTS_OK) {
    rc = SALTS_EPROTO;
    error.phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT;
  }
  if (rc == SALTS_OK) {
    rc = turbo_flow_msg_result_commit(&claim, &candidate);
    if (rc != SALTS_OK) error.phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT;
  }
  if (candidate && independent) b->operation.vtable.destroy_result(candidate, b->result->context);
  turbo_flow_msg_result_abort(&claim);
done:
  if (rc != SALTS_OK) {
    error.status = rc;
    error.message[sizeof(error.message) - 1] = 0;
    salts_mutex_lock(&b->error_mutex);
    b->error = error;
    salts_mutex_unlock(&b->error_mutex);
  }
  if (accepted) atomic_fetch_sub(&b->admission, 1);
  return rc;
}
int flow_plugin_operations_materialize(vec_t *bindings, turbo_flow_plugin_result_domain_t *domain,
                                       turbo_flow_t *flow, turbo_flow_config_error_t *error) {
  for (size_t i = 0; i < vec_size(bindings); ++i) {
    flow_plugin_operation_binding_t *b = vec_at(bindings, i);
    turbo_flow_operation_provider_registration_t provider =
        TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
    int rc = flow_plugin_result_domain_materialize(domain, &b->operation, &b->request, &b->result,
                                                   &b->error);
    if (rc != SALTS_OK) return operation_error(error, i, "result_context", rc);
    rc = b->operation.create_session(b->operation.factory_ctx, &b->request, b->result->context,
                                     &b->session, &b->error);
    rc = flow_plugin_operation_callback_result(&b->error, TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION,
                                               rc);
    if (b->session &&
        (b->session == b->result->context || b->session == b->operation.factory_ctx)) {
      b->session = NULL;
      if (rc == SALTS_OK) rc = SALTS_EPROTO;
    }
    if (rc != SALTS_OK || !b->session)
      return operation_error(error, i, "session", rc ? rc : SALTS_EPROTO);
    b->request.resolved = NULL;
    provider.operation_name = b->operation.operation_name;
    provider.resource_name = b->request.resource_name;
    provider.fn = operation_invoke;
    provider.ctx = b;
    provider.options.mutability = TURBO_FLOW_STAGE_MUTATES_IN_PLACE;
    rc = turbo_flow_register_operation_provider(flow, &provider);
    if (rc != SALTS_OK) return operation_error(error, i, "provider", rc);
  }
  return SALTS_OK;
}
int flow_plugin_operations_close(vec_t *bindings) {
  int busy = 0;
  for (size_t i = 0; i < vec_size(bindings); ++i) {
    flow_plugin_operation_binding_t *b = vec_at(bindings, i);
    if (atomic_fetch_or(&b->admission, FLOW_OPERATION_CLOSED) & ~FLOW_OPERATION_CLOSED) busy = 1;
  }
  return busy ? SALTS_EBUSY : SALTS_OK;
}
int flow_plugin_operations_release(vec_t *bindings, turbo_flow_config_error_t *error) {
  for (size_t i = vec_size(bindings); i; --i) {
    flow_plugin_operation_binding_t *b = vec_at(bindings, i - 1);
    if (!b->session) continue;
    int rc = b->operation.vtable.release_session(b->session);
    if (rc != SALTS_OK) return operation_error(error, i - 1, "release_session", rc);
    b->session = NULL;
  }
  return SALTS_OK;
}
void flow_plugin_operations_free(vec_t *bindings) {
  for (size_t i = 0; i < vec_size(bindings); ++i) {
    flow_plugin_operation_binding_t *b = vec_at(bindings, i);
    if (b->error_mutex) salts_mutex_destroy(&b->error_mutex);
  }
  vec_destroy(bindings);
}
