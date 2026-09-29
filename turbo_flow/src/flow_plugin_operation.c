#include "flow_plugin_operation_internal.h"
#include "flow_internal.h"
#include "flow_projection_owner_internal.h"
#include "turbo_flow_stl_error_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_plugin_cflow_adapter_capture_s {
  void *context;
  salts_plugin_function_invoke_fn invoke;
} flow_plugin_cflow_adapter_capture_t;

typedef struct flow_plugin_cflow_lease_owner_s {
  salts_plugin_registry *registry;
  salts_plugin_lease lease;
} flow_plugin_cflow_lease_owner_t;

_Static_assert(sizeof(flow_plugin_cflow_adapter_capture_t) <= CMETA_CAPTURE_INLINE,
               "plugin CFlow adapter capture must remain inline");

static int flow_plugin_status_to_salts(salts_plugin_status status) {
  switch (status) {
    case SALTS_PLUGIN_OK: return SALTS_OK;
    case SALTS_PLUGIN_INVALID_ARGUMENT:
    case SALTS_PLUGIN_INVALID_MANIFEST:
    case SALTS_PLUGIN_INVALID_STATE:
      return SALTS_EINVAL;
    case SALTS_PLUGIN_UNKNOWN_EXPORT:
    case SALTS_PLUGIN_UNKNOWN_PLUGIN:
      return SALTS_ENOENT;
    case SALTS_PLUGIN_UNSUPPORTED_ABI:
    case SALTS_PLUGIN_INCOMPATIBLE_CONTRACT:
      return SALTS_ENOTSUP;
    case SALTS_PLUGIN_CAPACITY_EXCEEDED: return SALTS_ENOSPC;
    case SALTS_PLUGIN_ALLOCATION_FAILED: return SALTS_ENOMEM;
    case SALTS_PLUGIN_ALREADY: return SALTS_EALREADY;
    case SALTS_PLUGIN_BUSY: return SALTS_EBUSY;
    case SALTS_PLUGIN_STALE:
    case SALTS_PLUGIN_DUPLICATE_PLUGIN_ID:
    case SALTS_PLUGIN_DUPLICATE_EXPORT:
    case SALTS_PLUGIN_LOAD_FAILED:
    case SALTS_PLUGIN_QUERY_MISSING:
    case SALTS_PLUGIN_QUERY_REJECTED:
    case SALTS_PLUGIN_UNLOAD_FAILED:
      return SALTS_EPROTO;
  }
  return SALTS_EPROTO;
}

static bool flow_plugin_cflow_adapter_invoke(
    const cmeta_callable *self, void *out, const void *const *args) {
  flow_plugin_cflow_adapter_capture_t capture;
  void *params[1];
  if (!self || self->capture_size != sizeof(capture) || !args || !args[0])
    return false;
  memcpy(&capture, self->capture.bytes, sizeof(capture));
  if (!capture.invoke) return false;
  params[0] = (void *)args[0];
  return capture.invoke(capture.context, out, params, 1u);
}

static cmeta_callable flow_plugin_cflow_adapter(
    const salts_plugin_function_export *function) {
  cmeta_callable adapter = {0};
  flow_plugin_cflow_adapter_capture_t capture = {0};
  if (!function || !function->desc || !function->invoke) return adapter;
  capture.context = function->context;
  capture.invoke = function->invoke;
  adapter.meta.sig = CMETA_SIG_INVALID;
  adapter.meta.effects = function->desc->effects;
  adapter.meta.properties = function->desc->properties;
  adapter.invoke = flow_plugin_cflow_adapter_invoke;
  adapter.dispatch = CMETA_CALLABLE_DISPATCH_ADAPTER;
  adapter.capture_size = sizeof(capture);
  memcpy(adapter.capture.bytes, &capture, sizeof(capture));
  return adapter;
}

static void flow_plugin_cflow_lease_release(void *ctx) {
  flow_plugin_cflow_lease_owner_t *owner =
      (flow_plugin_cflow_lease_owner_t *)ctx;
  if (!owner) return;
  if (owner->registry && salts_plugin_lease_valid(owner->lease)) {
    (void)salts_plugin_registry_release(owner->registry, &owner->lease);
  }
  free(owner);
}

TURBO_FLOW_API int flow_plugin_bind_cflow_function(
    turbo_flow_t *flow,
    const flow_plugin_cflow_function_binding_t *binding) {
  const salts_plugin_manifest *manifest = NULL;
  const salts_plugin_export *entry = NULL;
  const salts_plugin_function_export *function;
  const cmeta_param_desc *param;
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
  turbo_flow_operation_port_binding_t ports[2];
  flow_plugin_cflow_lease_owner_t *owner = NULL;
  salts_plugin_lease lease = {0};
  salts_plugin_status plugin_status;
  cmeta_callable adapter;
  int rc;

  if (!flow || !binding || !binding->registry ||
      !salts_plugin_ref_valid(binding->plugin) ||
      !binding->export_id || !binding->export_id[0] ||
      !binding->operation || !binding->input_data || !binding->output_data ||
      turbo_flow_state(flow) != TURBO_FLOW_STATE_PARSED) {
    return SALTS_EINVAL;
  }
  if (!cmeta_data_desc_valid(binding->input_data) ||
      !cmeta_data_desc_valid(binding->output_data) ||
      !binding->input_data->storage_type ||
      !binding->output_data->storage_type) {
    return SALTS_EINVAL;
  }

  plugin_status = salts_plugin_registry_acquire(
      binding->registry, binding->plugin, &lease, &manifest);
  if (plugin_status != SALTS_PLUGIN_OK) {
    return flow_set_error_keep_state(
        flow, flow_plugin_status_to_salts(plugin_status), 0u, 0u,
        salts_plugin_status_string(plugin_status));
  }
  plugin_status = salts_plugin_manifest_find_export(
      manifest, binding->export_id, &entry);
  if (plugin_status != SALTS_PLUGIN_OK || !entry ||
      entry->kind != SALTS_PLUGIN_EXPORT_FUNCTION) {
    rc = flow_set_error_keep_state(
        flow,
        plugin_status == SALTS_PLUGIN_OK
            ? SALTS_ENOTSUP
            : flow_plugin_status_to_salts(plugin_status),
        0u, 0u,
        plugin_status == SALTS_PLUGIN_OK
            ? "plugin export is not a canonical function"
            : salts_plugin_status_string(plugin_status));
    goto release_lease;
  }

  function = &entry->value.function;
  if (!function->desc || !function->abi || !function->invoke ||
      !cmeta_function_desc_valid(function->desc) ||
      !cmeta_function_abi_desc_valid(function->abi) ||
      !cmeta_function_desc_equal(function->desc, function->abi->function) ||
      function->desc->param_count != 1u ||
      !function->desc->return_type ||
      function->desc->return_type->kind == CMETA_T_VOID) {
    rc = flow_set_error_keep_state(
        flow, SALTS_ENOTSUP, 0u, 0u,
        "plugin function is not a supported unary value operation");
    goto release_lease;
  }
  param = cmeta_function_param(function->desc, 0u);
  if (!param ||
      (param->flags & CMETA_PARAM_DIRECTION_MASK) != CMETA_PARAM_IN ||
      !cmeta_effects_are_pure(function->desc->effects) ||
      !cmeta_properties_include(function->desc->properties, CMETA_PROP_TOTAL)) {
    rc = flow_set_error_keep_state(
        flow, SALTS_ENOTSUP, 0u, 0u,
        "plugin function must be unary IN and PURE+TOTAL");
    goto release_lease;
  }
  if (!cmeta_type_equal(param->type, binding->input_data->storage_type) ||
      !cmeta_type_equal(function->desc->return_type,
                        binding->output_data->storage_type)) {
    rc = flow_set_error_keep_state(
        flow, SALTS_EPROTO, 0u, 0u,
        "plugin function DataDesc storage type mismatch");
    goto release_lease;
  }

  adapter = flow_plugin_cflow_adapter(function);
  if (!adapter.invoke) {
    rc = SALTS_EPROTO;
    goto release_lease;
  }

  ports[0] = (turbo_flow_operation_port_binding_t)
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  ports[0].port_index = 0u;
  ports[0].domain = TURBO_FLOW_DOMAIN_DATA;
  ports[0].direction = TURBO_FLOW_OPERATION_PORT_INPUT;
  ports[0].value_kind = TURBO_FLOW_OPERATION_VALUE_PARAMETER;
  ports[0].storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  ports[0].parameter_index = 0u;
  ports[0].data = binding->input_data;

  ports[1] = (turbo_flow_operation_port_binding_t)
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  ports[1].port_index = 0u;
  ports[1].domain = TURBO_FLOW_DOMAIN_DATA;
  ports[1].direction = TURBO_FLOW_OPERATION_PORT_OUTPUT;
  ports[1].value_kind = TURBO_FLOW_OPERATION_VALUE_RETURN;
  ports[1].storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  ports[1].parameter_index = SIZE_MAX;
  ports[1].data = binding->output_data;

  owner = (flow_plugin_cflow_lease_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    rc = SALTS_ENOMEM;
    goto release_lease;
  }
  owner->registry = binding->registry;
  owner->lease = lease;
  memset(&lease, 0, sizeof(lease));

  rc = flow_plan_owned_resources_reserve(flow, 1u);
  if (rc != SALTS_OK) goto release_owner;
  rc = flow_plan_owned_resource_stage(
      flow, owner, flow_plugin_cflow_lease_release);
  if (rc != SALTS_OK) goto release_owner;

  registration.operation = binding->operation;
  registration.function = function->desc;
  registration.abi = function->abi;
  registration.adapter = adapter;
  registration.ports = ports;
  registration.port_count = 2u;
  registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

  rc = turbo_flow_register_reflected_operation(flow, &registration);
  if (rc != SALTS_OK) {
    const int rollback =
        flow_plan_owned_resource_unstage_last(flow, owner);
    if (rollback != SALTS_OK)
      return flow_set_error_keep_state(
          flow, SALTS_EPROTO, 0u, 0u,
          "plugin function binding rollback failed");
    return rc;
  }
  return SALTS_OK;

release_owner:
  flow_plugin_cflow_lease_release(owner);
  return rc;

release_lease:
  if (salts_plugin_lease_valid(lease))
    (void)salts_plugin_registry_release(binding->registry, &lease);
  return rc;
}

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
    turbo_flow_config_error_t materializer_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    if (!b->materializer || !flow_msg_has_durable_claim(msg)) {
      rc = SALTS_ENOTSUP;
      goto done;
    }
    rc = turbo_flow_plugin_materializer_materialize(b->materializer, msg, &materializer_error);
    if (rc != SALTS_OK) {
      (void)snprintf(error.message, sizeof(error.message),
                     "typed materialization failed before operation execution");
      goto done;
    }
    value = turbo_flow_msg_projection(msg, &schema);
    data = turbo_flow_msg_projection_data(msg);
    if (!data || !value) {
      rc = SALTS_EPROTO;
      goto done;
    }
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
