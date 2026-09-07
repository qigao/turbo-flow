#include "flow_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static SALTS_THREAD_LOCAL turbo_flow_t *flow_active_error_owner;
static SALTS_THREAD_LOCAL turbo_flow_t *flow_last_publish_error_owner;
static SALTS_THREAD_LOCAL turbo_flow_error_t flow_publish_error;

static turbo_flow_error_t *flow_error_target(turbo_flow_t *flow) {
  if (flow && flow_active_error_owner == flow) return &flow_publish_error;
  flow_last_publish_error_owner = NULL;
  return flow ? &flow->last_error : NULL;
}

int flow_set_error(turbo_flow_t *flow, int code, uint32_t line, uint32_t column,
                   const char *message) {
  turbo_flow_error_t *error;
  if (!flow) return code;
  error = flow_error_target(flow);
  error->code = code;
  error->line = line;
  error->column = column;
  snprintf(error->message, sizeof(error->message), "%s", message ? message : salts_strerror(code));
  flow->state = TURBO_FLOW_STATE_FAILED;
  return code;
}

int flow_set_error_keep_state(turbo_flow_t *flow, int code, uint32_t line, uint32_t column,
                              const char *message) {
  turbo_flow_error_t *error;
  if (!flow) return code;
  error = flow_error_target(flow);
  error->code = code;
  error->line = line;
  error->column = column;
  snprintf(error->message, sizeof(error->message), "%s", message ? message : salts_strerror(code));
  return code;
}

void flow_clear_error(turbo_flow_t *flow) {
  turbo_flow_error_t *error;
  if (!flow) return;
  error = flow_error_target(flow);
  error->code = SALTS_OK;
  error->line = 0;
  error->column = 0;
  error->message[0] = '\0';
}

void flow_publish_error_context_begin(turbo_flow_t *flow) {
  flow_active_error_owner = flow;
  flow_last_publish_error_owner = flow;
  memset(&flow_publish_error, 0, sizeof(flow_publish_error));
}

void flow_publish_error_context_end(turbo_flow_t *flow) {
  if (flow_active_error_owner == flow) flow_active_error_owner = NULL;
}

int flow_error_code(const turbo_flow_t *flow) {
  if (!flow) return SALTS_EINVAL;
  if (flow_active_error_owner == flow || flow_last_publish_error_owner == flow) {
    return flow_publish_error.code;
  }
  return flow->last_error.code;
}

int flow_view_eq_cstr(vstr view, const char *text) {
  size_t len = text ? strlen(text) : 0;
  return view.len == len && (len == 0 || memcmp(view.data, text, len) == 0);
}

static int flow_name_eq_cstr(const tstr name, const char *text) {
  return name && text && strcmp(name, text) == 0;
}

void flow_stage_impl_destroy(flow_stage_plan_impl_t *stage) {
  if (!stage) return;
  tstr_freep(&stage->name);
  tstr_freep(&stage->adapter_name);
  tstr_freep(&stage->operation_name);
  tstr_freep(&stage->resource_name);
}

void flow_registration_destroy(flow_stage_registration_t *reg) {
  if (!reg) return;
  tstr_freep(&reg->name);
  reg->fn = NULL;
  reg->ctx = NULL;
}

void flow_operation_provider_registration_destroy(
    flow_operation_provider_registration_t *provider) {
  if (!provider) return;
  tstr_freep(&provider->operation_name);
  tstr_freep(&provider->resource_name);
  tstr_freep(&provider->module_name);
  provider->fn = NULL;
  provider->emit_fn = NULL;
  provider->key_selector = NULL;
  provider->key_ctx = NULL;
  provider->keyed_fn = NULL;
  provider->keyed_emit_fn = NULL;
  provider->window_fn = NULL;
  provider->window_close_fn = NULL;
  flow_keyed_state_store_unbind(provider->keyed_store);
  provider->keyed_store = NULL;
  provider->max_outputs = 0u;
  provider->ctx = NULL;
}

static void flow_schema_string_free(const char *value) {
  tstr owned = (tstr)value;
  if (!value) return;
  tstr_freep(&owned);
}

static void flow_adapter_schema_destroy(flow_adapter_registration_t *adapter) {
  size_t i;

  if (!adapter || !adapter->schema_fields) return;
  for (i = 0; i < adapter->schema.field_count; ++i) {
    turbo_flow_option_field_t *field = &adapter->schema_fields[i];
    size_t j;
    flow_schema_string_free(field->name);
    for (j = 0; j < field->enum_value_count; ++j) {
      flow_schema_string_free(field->enum_values[j]);
    }
    free((void *)field->enum_values);
  }
  free(adapter->schema_fields);
  adapter->schema_fields = NULL;
  memset(&adapter->schema, 0, sizeof(adapter->schema));
}

void flow_adapter_registration_destroy(flow_adapter_registration_t *adapter) {
  size_t i;
  if (!adapter) return;
  if (adapter->ops.shutdown) adapter->ops.shutdown(adapter->ctx);
  for (i = 0; i < vec_size(&adapter->operation_bindings); ++i) {
    flow_adapter_operation_binding_t *binding =
        (flow_adapter_operation_binding_t *)vec_at(&adapter->operation_bindings, i);
    if (!binding) continue;
    tstr_freep(&binding->operation_name);
    tstr_freep(&binding->module_name);
    tstr_freep(&binding->resource_name);
  }
  vec_destroy(&adapter->operation_bindings);
  flow_adapter_schema_destroy(adapter);
  tstr_freep(&adapter->name);
  memset(&adapter->ops, 0, sizeof(adapter->ops));
  memset(&adapter->async_terminal_ops, 0, sizeof(adapter->async_terminal_ops));
  memset(&adapter->async_emit_ops, 0, sizeof(adapter->async_emit_ops));
  memset(&adapter->settlement_ops, 0, sizeof(adapter->settlement_ops));
  adapter->ctx = NULL;
  adapter->settlement_ctx = NULL;
}

void flow_resource_registration_destroy(flow_resource_registration_t *resource) {
  if (!resource) return;
  tstr_freep(&resource->owner_name);
  memset(&resource->ops, 0, sizeof(resource->ops));
  resource->ctx = NULL;
}

void flow_edge_impl_destroy(flow_edge_plan_impl_t *edge) {
  if (!edge) return;
  turbo_flow_expr_destroy(edge->predicate);
  edge->predicate = NULL;
  tstr_freep(&edge->condition);
  tstr_freep(&edge->name);
  tstr_freep(&edge->from_name);
  tstr_freep(&edge->to_name);
  edge->from_stage = UINT32_MAX;
  edge->to_stage = UINT32_MAX;
}

void flow_clear_plan(turbo_flow_t *flow) {
  size_t i;

  if (!flow) return;
  flow_close_publish_admission(flow);
  flow_stop_adapters(flow);
  flow_wait_for_publishes(flow);
  flow_stop_data_planes(flow);
  flow_stop_reorder_states(flow);
  flow_clear_reorder_states(flow);
  flow_stop_executor_adapters(flow);
  flow_clear_runtime_plan(flow);
  for (i = 0; i < vec_size(&flow->stages); ++i) {
    flow_stage_plan_impl_t *stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, i);
    flow_stage_impl_destroy(stage);
  }
  turbo_flow_stl_error(vec_clear(&flow->stages));
  for (i = 0; i < vec_size(&flow->edges); ++i) {
    flow_edge_plan_impl_t *edge = (flow_edge_plan_impl_t *)vec_at(&flow->edges, i);
    flow_edge_impl_destroy(edge);
  }
  turbo_flow_stl_error(vec_clear(&flow->edges));
}

void flow_clear_registry(turbo_flow_t *flow) {
  size_t i;

  if (!flow) return;
  for (i = 0; i < vec_size(&flow->registrations); ++i) {
    flow_stage_registration_t *reg =
        (flow_stage_registration_t *)vec_at(&flow->registrations, i);
    flow_registration_destroy(reg);
  }
  turbo_flow_stl_error(vec_clear(&flow->registrations));

  for (i = 0; i < vec_size(&flow->operation_providers); ++i) {
    flow_operation_provider_registration_t *provider =
        (flow_operation_provider_registration_t *)vec_at(&flow->operation_providers, i);
    flow_operation_provider_registration_destroy(provider);
  }
  turbo_flow_stl_error(vec_clear(&flow->operation_providers));

  for (i = 0; i < vec_size(&flow->primitives); ++i) {
    flow_primitive_registration_t *primitive =
        (flow_primitive_registration_t *)vec_at(&flow->primitives, i);
    flow_primitive_registration_destroy(primitive);
  }
  turbo_flow_stl_error(vec_clear(&flow->primitives));

  for (i = 0; i < vec_size(&flow->operations); ++i) {
    flow_operation_registration_t *operation =
        (flow_operation_registration_t *)vec_at(&flow->operations, i);
    flow_operation_registration_destroy(operation);
  }
  turbo_flow_stl_error(vec_clear(&flow->operations));

  for (i = 0; i < vec_size(&flow->modules); ++i) {
    flow_module_registration_t *module =
        (flow_module_registration_t *)vec_at(&flow->modules, i);
    flow_module_registration_destroy(module);
  }
  turbo_flow_stl_error(vec_clear(&flow->modules));

  for (i = 0; i < vec_size(&flow->resources); ++i) {
    flow_resource_registration_t *resource =
        (flow_resource_registration_t *)vec_at(&flow->resources, i);
    flow_resource_registration_destroy(resource);
  }
  turbo_flow_stl_error(vec_clear(&flow->resources));

  for (i = 0; i < vec_size(&flow->adapters); ++i) {
    flow_adapter_registration_t *adapter =
        (flow_adapter_registration_t *)vec_at(&flow->adapters, i);
    flow_adapter_registration_destroy(adapter);
  }
  turbo_flow_stl_error(vec_clear(&flow->adapters));
  flow_expr_projection_clear(flow);
}

int flow_find_stage_view(const turbo_flow_t *flow, vstr name) {
  size_t i;

  if (!flow || !name.data || name.len == 0) return -1;
  for (i = 0; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (stage && stage->name && tstr_len(stage->name) == name.len &&
        memcmp(stage->name, name.data, name.len) == 0) {
      return (int)i;
    }
  }
  return -1;
}

int flow_find_registration(const turbo_flow_t *flow, const char *name) {
  size_t i;

  if (!flow || !name) return -1;
  for (i = 0; i < vec_size(&flow->registrations); ++i) {
    const flow_stage_registration_t *reg =
        (const flow_stage_registration_t *)vec_at_const(&flow->registrations, i);
    if (reg && flow_name_eq_cstr(reg->name, name)) return (int)i;
  }
  return -1;
}

int flow_find_operation_provider(const turbo_flow_t *flow, const char *operation_name,
                                 const char *resource_name) {
  size_t i;

  if (!flow || !operation_name) return -1;
  for (i = 0; i < vec_size(&flow->operation_providers); ++i) {
    const flow_operation_provider_registration_t *provider =
        (const flow_operation_provider_registration_t *)vec_at_const(
            &flow->operation_providers, i);
    int resource_match;
    if (!provider) continue;
    resource_match = (!provider->resource_name && !resource_name) ||
                     (provider->resource_name && resource_name &&
                      strcmp(provider->resource_name, resource_name) == 0);
    if (provider->operation_name && strcmp(provider->operation_name, operation_name) == 0 &&
        resource_match) {
      return (int)i;
    }
  }
  return -1;
}

int flow_find_adapter(const turbo_flow_t *flow, const char *name) {
  size_t i;

  if (!flow || !name) return -1;
  for (i = 0; i < vec_size(&flow->adapters); ++i) {
    const flow_adapter_registration_t *adapter =
        (const flow_adapter_registration_t *)vec_at_const(&flow->adapters, i);
    if (adapter && flow_name_eq_cstr(adapter->name, name)) return (int)i;
  }
  return -1;
}

turbo_flow_t *turbo_flow_create(void) {
  turbo_flow_t *flow = (turbo_flow_t *)calloc(1, sizeof(turbo_flow_t));
  if (!flow) return NULL;

  salts_mutex_init(&flow->runtime_mutex);
  salts_cond_init(&flow->runtime_cond);
  salts_mutex_init(&flow->broadcast_mutex);
  salts_mutex_init(&flow->async_ingress_mutex);
  if (!flow->runtime_mutex || !flow->runtime_cond || !flow->broadcast_mutex ||
      !flow->async_ingress_mutex) {
    salts_cond_destroy(&flow->runtime_cond);
    salts_mutex_destroy(&flow->async_ingress_mutex);
    salts_mutex_destroy(&flow->broadcast_mutex);
    salts_mutex_destroy(&flow->runtime_mutex);
    free(flow);
    return NULL;
  }
  flow->runtime_sync_initialized = 1;
  flow->async_ingress_config =
      (turbo_flow_async_ingress_config_t)TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  atomic_init(&flow->next_sequence, 0u);

  if (turbo_flow_stl_error(vec_init_bytes(&flow->stages, sizeof(flow_stage_plan_impl_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->edges, sizeof(flow_edge_plan_impl_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      flow_compiled_plan_init(&flow->compiled_plan) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->runtime_stage_configs, sizeof(flow_runtime_stage_config_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->threadpool_adapters, sizeof(flow_threadpool_adapter_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->threadpool_adapter_by_stage, sizeof(uint32_t),
                                          _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->coro_adapters, sizeof(flow_coro_adapter_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->coro_adapter_by_stage, sizeof(uint32_t),
                                          _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->broadcast_consumers, sizeof(flow_broadcast_consumer_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->worker_pool_adapters, sizeof(flow_worker_pool_adapter_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->worker_pool_adapter_by_stage, sizeof(uint32_t),
                                          _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->reorder_states, sizeof(flow_reorder_state_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->registrations, sizeof(flow_stage_registration_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->operation_providers, sizeof(flow_operation_provider_registration_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->primitives, sizeof(flow_primitive_registration_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->operations, sizeof(flow_operation_registration_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->modules, sizeof(flow_module_registration_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->adapters, sizeof(flow_adapter_registration_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->resources, sizeof(flow_resource_registration_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(
          &flow->expr_projection_registrations, sizeof(flow_expr_projection_registration_t),
          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->active_adapters, sizeof(flow_active_adapter_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->pool_records, sizeof(flow_pool_record_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->resource_command_history, sizeof(flow_resource_command_record_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&flow->event_observers, sizeof(flow_event_observer_registration_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&flow->active_runs, sizeof(turbo_flow_run_t *),
                                          _Alignof(turbo_flow_run_t *),
                                          TURBO_FLOW_ASYNC_INGRESS_MAX_CAPACITY)) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }

  flow->state = TURBO_FLOW_STATE_NEW;
  atomic_init(&flow->observer_failures, 0u);
  flow_clear_error(flow);
  return flow;
}

void turbo_flow_destroy(turbo_flow_t *flow) {
  if (!flow) return;
  if (flow->state == TURBO_FLOW_STATE_STARTED) (void)turbo_flow_stop(flow);
  flow_stop_async_ingress(flow);
  flow_reactive_runtime_stop(flow);
  flow_clear_plan(flow);
  flow_clear_registry(flow);
  flow_observer_clear(flow);
  vec_destroy(&flow->stages);
  vec_destroy(&flow->edges);
  flow_compiled_plan_destroy(&flow->compiled_plan);
  vec_destroy(&flow->runtime_stage_configs);
  vec_destroy(&flow->threadpool_adapters);
  vec_destroy(&flow->threadpool_adapter_by_stage);
  vec_destroy(&flow->coro_adapters);
  vec_destroy(&flow->coro_adapter_by_stage);
  vec_destroy(&flow->broadcast_consumers);
  vec_destroy(&flow->worker_pool_adapters);
  vec_destroy(&flow->worker_pool_adapter_by_stage);
  vec_destroy(&flow->reorder_states);
  vec_destroy(&flow->registrations);
  vec_destroy(&flow->operation_providers);
  vec_destroy(&flow->primitives);
  vec_destroy(&flow->operations);
  vec_destroy(&flow->modules);
  vec_destroy(&flow->adapters);
  vec_destroy(&flow->resources);
  vec_destroy(&flow->expr_projection_registrations);
  vec_destroy(&flow->active_adapters);
  vec_destroy(&flow->pool_records);
  vec_destroy(&flow->resource_command_history);
  vec_destroy(&flow->event_observers);
  vec_destroy(&flow->active_runs);
  if (flow->observer_ops.flow_destroyed) {
    flow->observer_ops.flow_destroyed(flow->observer_ctx);
  }
  if (flow->runtime_sync_initialized) {
    salts_cond_destroy(&flow->runtime_cond);
    salts_mutex_destroy(&flow->async_ingress_mutex);
    salts_mutex_destroy(&flow->broadcast_mutex);
    salts_mutex_destroy(&flow->runtime_mutex);
  }
  if (flow_active_error_owner == flow) flow_active_error_owner = NULL;
  if (flow_last_publish_error_owner == flow) flow_last_publish_error_owner = NULL;
  free(flow);
}

int turbo_flow_reset(turbo_flow_t *flow, int keep_registry) {
  if (!flow) return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0, "cannot reset a started flow");
  }
  flow_clear_plan(flow);
  flow->has_async_stage = 0;
  flow->required_backend = FLOW_PLAN_BACKEND_NATIVE;
  turbo_flow_stl_error(vec_clear(&flow->resource_command_history));
  if (!keep_registry) flow_clear_registry(flow);
  atomic_store_explicit(&flow->next_sequence, 0u, memory_order_release);
  flow->state = TURBO_FLOW_STATE_NEW;
  flow_clear_error(flow);
  return SALTS_OK;
}

int turbo_flow_register_stage_ex(turbo_flow_t *flow, const char *name, turbo_flow_stage_fn fn,
                                 void *ctx, const turbo_flow_stage_options_t *options) {
  flow_stage_registration_t reg;

  if (!flow || !name || name[0] == '\0' || !fn ||
      (options && (options->mutability < TURBO_FLOW_STAGE_READONLY ||
                   options->mutability > TURBO_FLOW_STAGE_MUTATES_IN_PLACE ||
                   (options->effects & ~TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u))) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register stage after compile");
  }
  if (flow_find_registration(flow, name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "duplicate stage registration");
  }

  memset(&reg, 0, sizeof(reg));
  reg.name = tstr_dup(name);
  if (!reg.name) return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  reg.fn = fn;
  reg.ctx = ctx;
  if (options) reg.options = *options;
  else reg.options.mutability = TURBO_FLOW_STAGE_READONLY;

  if (turbo_flow_stl_error(vec_push(&flow->registrations, &reg)) != SALTS_OK) {
    flow_registration_destroy(&reg);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }

  return SALTS_OK;
}

static int flow_register_operation_provider_impl(turbo_flow_t *flow, const char *operation_name,
                                                 const char *resource_name,
                                                 flow_operation_provider_registration_t *provider) {
  int rc;

  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register operation provider after compile");
  }
  if (flow_find_operation_provider(flow, operation_name, resource_name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "duplicate operation provider");
  }
  if (provider->keyed_store) {
    rc = flow_keyed_state_store_bind(provider->keyed_store);
    if (rc != SALTS_OK) {
      return flow_set_error_keep_state(flow, rc, 0, 0,
                                       rc == SALTS_EALREADY
                                           ? "keyed state store already has a provider owner"
                                           : "keyed state store is invalid");
    }
  }

  provider->operation_name = tstr_dup(operation_name);
  if (resource_name) provider->resource_name = tstr_dup(resource_name);
  if (!provider->operation_name || (resource_name && !provider->resource_name)) {
    flow_operation_provider_registration_destroy(provider);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  if (turbo_flow_stl_error(vec_push(&flow->operation_providers, provider)) != SALTS_OK) {
    flow_operation_provider_registration_destroy(provider);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

int turbo_flow_register_operation_provider(
    turbo_flow_t *flow, const turbo_flow_operation_provider_registration_t *registration) {
  flow_operation_provider_registration_t provider;

  if (!flow || !registration || registration->size < sizeof(*registration) ||
      !registration->operation_name || registration->operation_name[0] == '\0' ||
      (registration->resource_name && registration->resource_name[0] == '\0') ||
      !registration->fn || registration->options.mutability < TURBO_FLOW_STAGE_READONLY ||
      registration->options.mutability > TURBO_FLOW_STAGE_MUTATES_IN_PLACE ||
      (registration->options.effects & ~TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u) {
    return SALTS_EINVAL;
  }
  memset(&provider, 0, sizeof(provider));
  provider.fn = registration->fn;
  provider.ctx = registration->ctx;
  provider.options = registration->options;
  return flow_register_operation_provider_impl(flow, registration->operation_name,
                                               registration->resource_name, &provider);
}

int turbo_flow_register_emitting_operation_provider(
    turbo_flow_t *flow, const turbo_flow_emitting_operation_provider_registration_t *registration) {
  flow_operation_provider_registration_t provider;

  if (!flow || !registration || registration->size < sizeof(*registration) ||
      !registration->operation_name || registration->operation_name[0] == '\0' ||
      (registration->resource_name && registration->resource_name[0] == '\0') ||
      !registration->fn || registration->max_outputs == 0u ||
      registration->max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS ||
      registration->options.mutability != TURBO_FLOW_STAGE_READONLY ||
      registration->options.effects != TURBO_FLOW_STAGE_EFFECT_NONE) {
    return SALTS_EINVAL;
  }
  memset(&provider, 0, sizeof(provider));
  provider.emit_fn = registration->fn;
  provider.max_outputs = registration->max_outputs;
  provider.ctx = registration->ctx;
  provider.options = registration->options;
  provider.options.effects |= TURBO_FLOW_STAGE_EFFECT_EMITS;
  return flow_register_operation_provider_impl(flow, registration->operation_name,
                                               registration->resource_name, &provider);
}

int turbo_flow_register_keyed_operation_provider(
    turbo_flow_t *flow, const turbo_flow_keyed_operation_provider_registration_t *registration) {
  flow_operation_provider_registration_t provider;

  if (!flow || !registration || registration->size < sizeof(*registration) ||
      !registration->operation_name || registration->operation_name[0] == '\0' ||
      (registration->resource_name && registration->resource_name[0] == '\0') ||
      !registration->key_selector || !registration->fn || !registration->store ||
      flow_keyed_state_store_is_event_time(registration->store) ||
      registration->options.mutability != TURBO_FLOW_STAGE_MUTATES_IN_PLACE ||
      (registration->options.effects & ~TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u) {
    return SALTS_EINVAL;
  }
  memset(&provider, 0, sizeof(provider));
  provider.keyed_store = registration->store;
  provider.key_selector = registration->key_selector;
  provider.key_ctx = registration->key_ctx;
  provider.keyed_fn = registration->fn;
  provider.ctx = registration->ctx;
  provider.options = registration->options;
  return flow_register_operation_provider_impl(flow, registration->operation_name,
                                               registration->resource_name, &provider);
}

int turbo_flow_register_keyed_emitting_operation_provider(
    turbo_flow_t *flow,
    const turbo_flow_keyed_emitting_operation_provider_registration_t *registration) {
  flow_operation_provider_registration_t provider;

  if (!flow || !registration || registration->size < sizeof(*registration) ||
      !registration->operation_name || registration->operation_name[0] == '\0' ||
      (registration->resource_name && registration->resource_name[0] == '\0') ||
      !registration->key_selector || !registration->fn || !registration->store ||
      flow_keyed_state_store_is_event_time(registration->store) ||
      registration->max_outputs == 0u ||
      registration->max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS ||
      registration->options.mutability != TURBO_FLOW_STAGE_READONLY ||
      (registration->options.effects & ~TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u) {
    return SALTS_EINVAL;
  }
  memset(&provider, 0, sizeof(provider));
  provider.key_selector = registration->key_selector;
  provider.key_ctx = registration->key_ctx;
  provider.keyed_emit_fn = registration->fn;
  provider.keyed_store = registration->store;
  provider.max_outputs = registration->max_outputs;
  provider.ctx = registration->ctx;
  provider.options = registration->options;
  provider.options.effects |= TURBO_FLOW_STAGE_EFFECT_EMITS;
  return flow_register_operation_provider_impl(flow, registration->operation_name,
                                               registration->resource_name, &provider);
}

int turbo_flow_register_event_time_window_provider(
    turbo_flow_t *flow, const turbo_flow_event_time_window_provider_registration_t *registration) {
  flow_operation_provider_registration_t provider;

  if (!flow || !registration || registration->size < sizeof(*registration) ||
      !registration->operation_name || registration->operation_name[0] == '\0' ||
      (registration->resource_name && registration->resource_name[0] == '\0') ||
      !registration->key_selector || !registration->on_event || !registration->on_close ||
      !registration->store || !flow_keyed_state_store_is_event_time(registration->store) ||
      registration->max_outputs == 0u ||
      registration->max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS ||
      registration->options.mutability != TURBO_FLOW_STAGE_READONLY ||
      (registration->options.effects & ~TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u) {
    return SALTS_EINVAL;
  }
  memset(&provider, 0, sizeof(provider));
  provider.key_selector = registration->key_selector;
  provider.key_ctx = registration->key_ctx;
  provider.window_fn = registration->on_event;
  provider.window_close_fn = registration->on_close;
  provider.keyed_store = registration->store;
  provider.max_outputs = registration->max_outputs;
  provider.ctx = registration->ctx;
  provider.options = registration->options;
  provider.options.effects |= TURBO_FLOW_STAGE_EFFECT_EMITS;
  return flow_register_operation_provider_impl(flow, registration->operation_name,
                                               registration->resource_name, &provider);
}

int turbo_flow_register_stage_with_resources(
    turbo_flow_t *flow, const char *name, turbo_flow_stage_fn fn, void *ctx,
    const turbo_flow_stage_options_t *options,
    const turbo_flow_resource_provider_registration_t *resources, size_t resource_count) {
  size_t resources_before;
  size_t registrations_before;
  int rc;
  if (!flow || (resource_count > 0u && !resources)) return SALTS_EINVAL;
  for (size_t i = 0; i < resource_count; ++i) {
    if (resources[i].size < sizeof(resources[i]) || !resources[i].owner_name ||
        resources[i].owner_name[0] == '\0') {
      return SALTS_EINVAL;
    }
  }
  resources_before = vec_size(&flow->resources);
  registrations_before = vec_size(&flow->registrations);
  rc = turbo_flow_register_stage_ex(flow, name, fn, ctx, options);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < resource_count; ++i) {
    rc = turbo_flow_register_resource_provider(flow, resources[i].owner_name, &resources[i].ops,
                                               resources[i].ctx);
    if (rc == SALTS_OK) continue;
    while (vec_size(&flow->resources) > resources_before) {
      size_t last = vec_size(&flow->resources) - 1u;
      flow_resource_registration_t *resource =
          (flow_resource_registration_t *)vec_at(&flow->resources, last);
      flow_resource_registration_destroy(resource);
      (void)turbo_flow_stl_error(vec_resize(&flow->resources, last));
    }
    if (vec_size(&flow->registrations) > registrations_before) {
      size_t last = vec_size(&flow->registrations) - 1u;
      flow_stage_registration_t *registration =
          (flow_stage_registration_t *)vec_at(&flow->registrations, last);
      flow_registration_destroy(registration);
      (void)turbo_flow_stl_error(vec_resize(&flow->registrations, last));
    }
    return rc;
  }
  return SALTS_OK;
}

int turbo_flow_register_adapter(turbo_flow_t *flow, const char *name,
                                const turbo_flow_adapter_ops_t *ops, void *ctx) {
  return turbo_flow_register_adapter_ex(flow, name, ops, ctx, NULL);
}

int turbo_flow_register_adapter_async_terminal(turbo_flow_t *flow, const char *name,
                                               const turbo_flow_async_terminal_adapter_ops_t *ops) {
  int index;
  flow_adapter_registration_t *adapter;
  if (!flow || !name || !ops || ops->size < sizeof(*ops) ||
      ops->version != TURBO_FLOW_ASYNC_TERMINAL_API_VERSION || !ops->submit) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register async terminal adapter after compile");
  }
  index = flow_find_adapter(flow, name);
  if (index < 0) return SALTS_ENOENT;
  adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, (size_t)index);
  if (!adapter) return SALTS_ENOENT;
  if (adapter->async_terminal_ops.submit || adapter->async_emit_ops.submit) return SALTS_EALREADY;
  if (adapter->ops.consume || adapter->ops.consume_retry) return SALTS_EINVAL;
  adapter->async_terminal_ops = *ops;
  adapter->async_terminal_ops.size = sizeof(adapter->async_terminal_ops);
  return SALTS_OK;
}

int turbo_flow_register_adapter_async_emit(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_async_emit_adapter_ops_t *ops) {
  int index;
  flow_adapter_registration_t *adapter;
  if (!flow || !name || !ops || ops->size < sizeof(*ops) ||
      ops->version != TURBO_FLOW_ASYNC_EMIT_API_VERSION || !ops->submit) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register async emitting adapter after compile");
  }
  index = flow_find_adapter(flow, name);
  if (index < 0) return SALTS_ENOENT;
  adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, (size_t)index);
  if (!adapter) return SALTS_ENOENT;
  if (adapter->async_emit_ops.submit || adapter->async_terminal_ops.submit) return SALTS_EALREADY;
  if (adapter->ops.consume || adapter->ops.consume_retry) return SALTS_EINVAL;
  adapter->async_emit_ops = *ops;
  adapter->async_emit_ops.size = sizeof(adapter->async_emit_ops);
  return SALTS_OK;
}

int turbo_flow_register_adapter_settlement(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_settlement_owner_ops_t *ops,
                                           void *ctx) {
  int index;
  flow_adapter_registration_t *adapter;
  if (!flow || !name || !ops || ops->size < sizeof(*ops) || !ops->apply) return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register settlement owner after compile");
  }
  index = flow_find_adapter(flow, name);
  if (index < 0) return SALTS_ENOENT;
  adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, (size_t)index);
  if (!adapter) return SALTS_ENOENT;
  if (adapter->settlement_ops.apply) return SALTS_EALREADY;
  adapter->settlement_ops = *ops;
  adapter->settlement_ops.size = sizeof(adapter->settlement_ops);
  adapter->settlement_ctx = ctx;
  return SALTS_OK;
}

int turbo_flow_register_resource_provider(turbo_flow_t *flow, const char *owner_name,
                                          const turbo_flow_resource_provider_ops_t *ops,
                                          void *ctx) {
  flow_resource_registration_t resource;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!flow || !owner_name || owner_name[0] == '\0' || !ops || ops->size < sizeof(*ops) ||
      !ops->metadata) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register resource provider after compile");
  }
  if (ops->metadata(ctx, &metadata) != SALTS_OK || !flow_resource_metadata_valid(&metadata) ||
      strcmp(metadata.owner_name, owner_name) != 0) {
    return SALTS_EPROTO;
  }
  for (size_t i = 0; i < vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *existing =
        (const flow_resource_registration_t *)vec_at_const(&flow->resources, i);
    turbo_flow_resource_metadata_t current = TURBO_FLOW_RESOURCE_METADATA_INIT;
    if (!existing || existing->ops.metadata(existing->ctx, &current) != SALTS_OK ||
        !flow_resource_metadata_valid(&current)) {
      return SALTS_EPROTO;
    }
    if (strcmp(current.uid, metadata.uid) == 0) return SALTS_EALREADY;
  }
  memset(&resource, 0, sizeof(resource));
  resource.owner_name = tstr_dup(owner_name);
  if (!resource.owner_name) return SALTS_ENOMEM;
  resource.ops = *ops;
  resource.ops.size = sizeof(resource.ops);
  resource.ctx = ctx;
  if (turbo_flow_stl_error(vec_push(&flow->resources, &resource)) != SALTS_OK) {
    flow_resource_registration_destroy(&resource);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int flow_adapter_schema_copy(flow_adapter_registration_t *adapter,
                                    const turbo_flow_adapter_schema_t *schema) {
  size_t i;

  if (!schema) return SALTS_OK;
  if (schema->kind < TURBO_FLOW_ADAPTER_KIND_CUSTOM ||
      schema->kind > TURBO_FLOW_ADAPTER_KIND_SQLITE || schema->roles == 0 ||
      (schema->roles & ~(uint32_t)(TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK |
                                   TURBO_FLOW_ADAPTER_TRANSFORM)) != 0 ||
      schema->direction < TURBO_FLOW_ADAPTER_INPUT ||
      schema->direction > TURBO_FLOW_ADAPTER_BIDIRECTIONAL ||
      (schema->field_count > 0 && !schema->fields)) {
    return SALTS_EINVAL;
  }

  adapter->schema = *schema;
  adapter->schema.binding_name = adapter->name;
  adapter->schema.fields = NULL;
  if (schema->field_count == 0) return SALTS_OK;

  adapter->schema_fields =
      (turbo_flow_option_field_t *)calloc(schema->field_count, sizeof(*adapter->schema_fields));
  if (!adapter->schema_fields) return SALTS_ENOMEM;
  adapter->schema.fields = adapter->schema_fields;

  for (i = 0; i < schema->field_count; ++i) {
    const turbo_flow_option_field_t *src = &schema->fields[i];
    turbo_flow_option_field_t *dst = &adapter->schema_fields[i];
    int is_enum = src->type == TURBO_FLOW_OPTION_ENUM || src->type == TURBO_FLOW_OPTION_ENUM_SET;
    int is_numeric = src->type == TURBO_FLOW_OPTION_U32 || src->type == TURBO_FLOW_OPTION_U64 ||
                     src->type == TURBO_FLOW_OPTION_SIZE ||
                     src->type == TURBO_FLOW_OPTION_DURATION_MS;
    size_t j;

    if (!src->name || src->name[0] == '\0' || src->type < TURBO_FLOW_OPTION_BOOL ||
        src->type > TURBO_FLOW_OPTION_HOST_OBJECT ||
        (((src->flags & (TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX)) != 0) &&
         !is_numeric) ||
        ((src->flags & TURBO_FLOW_OPTION_HAS_MIN) && (src->flags & TURBO_FLOW_OPTION_HAS_MAX) &&
         src->min_value > src->max_value) ||
        (is_enum && src->enum_value_count == 0) || (!is_enum && src->enum_value_count != 0) ||
        (src->enum_value_count > 0 && !src->enum_values) ||
        (src->type == TURBO_FLOW_OPTION_HOST_OBJECT &&
         !(src->flags & TURBO_FLOW_OPTION_NOT_SERIALIZABLE))) {
      return SALTS_EINVAL;
    }
    *dst = *src;
    dst->name = tstr_dup(src->name);
    dst->enum_values = NULL;
    if (!dst->name) return SALTS_ENOMEM;
    if (src->enum_value_count == 0) continue;
    dst->enum_values = (const char *const *)calloc(src->enum_value_count, sizeof(char *));
    if (!dst->enum_values) return SALTS_ENOMEM;
    for (j = 0; j < src->enum_value_count; ++j) {
      if (!src->enum_values[j] || src->enum_values[j][0] == '\0') return SALTS_EINVAL;
      ((const char **)dst->enum_values)[j] = tstr_dup(src->enum_values[j]);
      if (!dst->enum_values[j]) return SALTS_ENOMEM;
    }
  }
  return SALTS_OK;
}

int turbo_flow_register_adapter_ex(turbo_flow_t *flow, const char *name,
                                   const turbo_flow_adapter_ops_t *ops, void *ctx,
                                   const turbo_flow_adapter_schema_t *schema) {
  flow_adapter_registration_t adapter;
  int rc;

  if (!flow || !name || name[0] == '\0') return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register adapter after compile");
  }
  if (flow_find_adapter(flow, name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "duplicate adapter");
  }

  memset(&adapter, 0, sizeof(adapter));
  if (turbo_flow_stl_error(vec_init_bytes(&adapter.operation_bindings, sizeof(flow_adapter_operation_binding_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  adapter.name = tstr_dup(name);
  if (!adapter.name) {
    vec_destroy(&adapter.operation_bindings);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  rc = flow_adapter_schema_copy(&adapter, schema);
  if (rc != SALTS_OK) {
    flow_adapter_schema_destroy(&adapter);
    tstr_freep(&adapter.name);
    vec_destroy(&adapter.operation_bindings);
    return flow_set_error_keep_state(
        flow, rc, 0, 0, rc == SALTS_ENOMEM ? "out of memory" : "invalid adapter option schema");
  }
  adapter.ctx = ctx;
  if (ops) {
    adapter.ops = *ops;
  }
  if (turbo_flow_stl_error(vec_push(&flow->adapters, &adapter)) != SALTS_OK) {
    flow_adapter_schema_destroy(&adapter);
    tstr_freep(&adapter.name);
    vec_destroy(&adapter.operation_bindings);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

int turbo_flow_register_async_terminal_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_adapter_ops_t *adapter_ops,
    const turbo_flow_async_terminal_adapter_ops_t *async_ops, void *ctx,
    const turbo_flow_adapter_schema_t *schema) {
  size_t adapters_before;
  int rc;

  if (!flow || !async_ops || async_ops->size < sizeof(*async_ops) ||
      async_ops->version != TURBO_FLOW_ASYNC_TERMINAL_API_VERSION || !async_ops->submit) {
    return SALTS_EINVAL;
  }
  adapters_before = vec_size(&flow->adapters);
  rc = turbo_flow_register_adapter_ex(flow, name, adapter_ops, ctx, schema);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_adapter_async_terminal(flow, name, async_ops);
  if (rc == SALTS_OK) return SALTS_OK;

  while (vec_size(&flow->adapters) > adapters_before) {
    size_t last = vec_size(&flow->adapters) - 1u;
    flow_adapter_registration_t *adapter =
        (flow_adapter_registration_t *)vec_at(&flow->adapters, last);
    flow_adapter_registration_destroy(adapter);
    (void)turbo_flow_stl_error(vec_resize(&flow->adapters, last));
  }
  return rc;
}

int turbo_flow_register_async_emit_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_adapter_ops_t *adapter_ops,
    const turbo_flow_async_emit_adapter_ops_t *async_ops, void *ctx,
    const turbo_flow_adapter_schema_t *schema) {
  size_t adapters_before;
  int rc;

  if (!flow || !async_ops || async_ops->size < sizeof(*async_ops) ||
      async_ops->version != TURBO_FLOW_ASYNC_EMIT_API_VERSION || !async_ops->submit || !schema ||
      (schema->roles & TURBO_FLOW_ADAPTER_TRANSFORM) == 0u) {
    return SALTS_EINVAL;
  }
  adapters_before = vec_size(&flow->adapters);
  rc = turbo_flow_register_adapter_ex(flow, name, adapter_ops, ctx, schema);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_adapter_async_emit(flow, name, async_ops);
  if (rc == SALTS_OK) return SALTS_OK;

  while (vec_size(&flow->adapters) > adapters_before) {
    size_t last = vec_size(&flow->adapters) - 1u;
    flow_adapter_registration_t *adapter =
        (flow_adapter_registration_t *)vec_at(&flow->adapters, last);
    flow_adapter_registration_destroy(adapter);
    (void)turbo_flow_stl_error(vec_resize(&flow->adapters, last));
  }
  return rc;
}

int turbo_flow_register_adapter_with_resources(
    turbo_flow_t *flow, const char *name, const turbo_flow_adapter_ops_t *ops, void *ctx,
    const turbo_flow_adapter_schema_t *schema,
    const turbo_flow_resource_provider_registration_t *resources, size_t resource_count) {
  size_t resources_before;
  size_t adapters_before;
  int rc;
  if (!flow || (resource_count > 0u && !resources)) return SALTS_EINVAL;
  for (size_t i = 0; i < resource_count; ++i) {
    if (resources[i].size < sizeof(resources[i]) || !resources[i].owner_name ||
        resources[i].owner_name[0] == '\0') {
      return SALTS_EINVAL;
    }
  }
  resources_before = vec_size(&flow->resources);
  adapters_before = vec_size(&flow->adapters);
  rc = turbo_flow_register_adapter_ex(flow, name, ops, ctx, schema);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < resource_count; ++i) {
    rc = turbo_flow_register_resource_provider(flow, resources[i].owner_name, &resources[i].ops,
                                               resources[i].ctx);
    if (rc == SALTS_OK) continue;
    while (vec_size(&flow->resources) > resources_before) {
      size_t last = vec_size(&flow->resources) - 1u;
      flow_resource_registration_t *resource =
          (flow_resource_registration_t *)vec_at(&flow->resources, last);
      flow_resource_registration_destroy(resource);
      (void)turbo_flow_stl_error(vec_resize(&flow->resources, last));
    }
    if (vec_size(&flow->adapters) > adapters_before) {
      size_t last = vec_size(&flow->adapters) - 1u;
      flow_adapter_registration_t *adapter =
          (flow_adapter_registration_t *)vec_at(&flow->adapters, last);
      flow_adapter_registration_destroy(adapter);
      (void)turbo_flow_stl_error(vec_resize(&flow->adapters, last));
    }
    return rc;
  }
  return SALTS_OK;
}

const flow_adapter_operation_binding_t *flow_find_adapter_operation_binding(
    const flow_adapter_registration_t *adapter, const char *operation_name) {
  size_t i;
  if (!adapter || !operation_name) return NULL;
  for (i = 0; i < vec_size(&adapter->operation_bindings); ++i) {
    const flow_adapter_operation_binding_t *binding =
        (const flow_adapter_operation_binding_t *)vec_at_const(
            &adapter->operation_bindings, i);
    if (binding && binding->operation_name &&
        strcmp(binding->operation_name, operation_name) == 0) {
      return binding;
    }
  }
  return NULL;
}

static int flow_module_adapter_operation_valid(
    const turbo_flow_module_adapter_registration_t *registration,
    const turbo_flow_operation_descriptor_t *operation) {
  uint32_t roles;
  int source_supported;
  int stage_supported;
  if (!registration || !registration->ops || !registration->schema || !operation ||
      (operation->scope.state != TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER &&
       operation->scope.state != TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER &&
       operation->scope.state != TURBO_FLOW_STATE_SCOPE_PROTOCOL_SESSION)) {
    return 0;
  }
  roles = registration->schema->roles;
  source_supported = (operation->flags & TURBO_FLOW_OPERATION_SOURCE) != 0u &&
                     (roles & TURBO_FLOW_ADAPTER_SOURCE) != 0u &&
                     registration->ops->start != NULL;
  stage_supported = (operation->flags & TURBO_FLOW_OPERATION_STAGE) != 0u &&
                    (roles & (TURBO_FLOW_ADAPTER_SINK | TURBO_FLOW_ADAPTER_TRANSFORM)) != 0u &&
                    registration->ops->consume != NULL;
  return source_supported || stage_supported;
}

static int flow_module_exports_primitive(const flow_module_registration_t *module,
                                         const char *type_name) {
  size_t i;
  if (!module || !type_name) return 0;
  for (i = 0; i < vec_size(&module->primitive_types); ++i) {
    const tstr *current =
        (const tstr *)vec_at_const(&module->primitive_types, i);
    if (current && *current && strcmp(*current, type_name) == 0) return 1;
  }
  return 0;
}

static const turbo_flow_primitive_descriptor_t *flow_module_adapter_pending_primitive(
    const turbo_flow_module_adapter_registration_t *registration, const char *name) {
  size_t i;
  if (!registration || !name) return NULL;
  for (i = 0; i < registration->primitive_count; ++i) {
    const turbo_flow_primitive_descriptor_t *primitive = &registration->primitives[i];
    if (primitive->name && strcmp(primitive->name, name) == 0) return primitive;
  }
  return NULL;
}

static int flow_module_adapter_primitive_compatible(
    const turbo_flow_primitive_descriptor_t *primitive,
    const turbo_flow_operation_descriptor_t *operation) {
  if (!primitive || !operation || primitive->size < sizeof(*primitive) ||
      primitive->kind != TURBO_FLOW_PRIMITIVE_RESOURCE ||
      primitive->domain != operation->resource_domain || !primitive->type_name ||
      strcmp(primitive->type_name, operation->resource_type) != 0) {
    return 0;
  }
  return operation->resource_min_version == 0u ||
         (primitive->version >= operation->resource_min_version &&
          (operation->resource_max_version == 0u ||
           primitive->version <= operation->resource_max_version));
}

static void flow_module_adapter_registry_rollback(turbo_flow_t *flow, size_t resources_before,
                                                  size_t primitives_before) {
  while (vec_size(&flow->resources) > resources_before) {
    size_t last = vec_size(&flow->resources) - 1u;
    flow_resource_registration_t *resource =
        (flow_resource_registration_t *)vec_at(&flow->resources, last);
    flow_resource_registration_destroy(resource);
    (void)turbo_flow_stl_error(vec_resize(&flow->resources, last));
  }
  while (vec_size(&flow->primitives) > primitives_before) {
    size_t last = vec_size(&flow->primitives) - 1u;
    flow_primitive_registration_t *primitive =
        (flow_primitive_registration_t *)vec_at(&flow->primitives, last);
    flow_primitive_registration_destroy(primitive);
    (void)turbo_flow_stl_error(vec_resize(&flow->primitives, last));
  }
}

static void flow_module_adapter_bindings_destroy(vec_t *bindings) {
  size_t i;
  if (!bindings) return;
  for (i = 0; i < vec_size(bindings); ++i) {
    flow_adapter_operation_binding_t *binding =
        (flow_adapter_operation_binding_t *)vec_at(bindings, i);
    if (!binding) continue;
    tstr_freep(&binding->operation_name);
    tstr_freep(&binding->module_name);
    tstr_freep(&binding->resource_name);
  }
  vec_destroy(bindings);
}

int turbo_flow_register_module_adapter(
    turbo_flow_t *flow, const turbo_flow_module_adapter_registration_t *registration) {
  turbo_flow_module_adapter_registration_t normalized;
  const flow_module_registration_t *module;
  flow_adapter_registration_t *adapter;
  vec_t bindings = {0};
  size_t adapters_before;
  size_t resources_before;
  size_t primitives_before;
  size_t i;
  int module_index;
  int rc;

  memset(&bindings, 0, sizeof(bindings));
  memset(&normalized, 0, sizeof(normalized));

  if (!flow || !registration ||
      registration->size < TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_V1_SIZE) {
    return SALTS_EINVAL;
  }
  memcpy(&normalized, registration,
         registration->size < sizeof(normalized) ? registration->size : sizeof(normalized));
  normalized.size = sizeof(normalized);
  registration = &normalized;
  if (!registration->module_name || registration->module_name[0] == '\0' ||
      !registration->adapter_name || registration->adapter_name[0] == '\0' ||
      !registration->ops || !registration->schema || !registration->operation_names ||
      registration->operation_count == 0u ||
      (registration->resource_count != 0u && !registration->resources) ||
      (registration->primitive_count != 0u && !registration->primitives)) {
    return SALTS_EINVAL;
  }
  module_index = flow_find_module_index(flow, registration->module_name);
  if (module_index < 0) {
    return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                     "adapter module is not registered");
  }
  module = (const flow_module_registration_t *)vec_at_const(&flow->modules,
                                                                  (size_t)module_index);
  for (i = 0; i < registration->operation_count; ++i) {
    const turbo_flow_operation_descriptor_t *operation;
    const turbo_flow_primitive_descriptor_t *primitive;
    const char *resource_name = registration->operation_resource_names
                                    ? registration->operation_resource_names[i]
                                    : NULL;
    int export_index;
    size_t j;
    if (!registration->operation_names[i] || registration->operation_names[i][0] == '\0') {
      return SALTS_EINVAL;
    }
    for (j = 0; j < i; ++j) {
      if (strcmp(registration->operation_names[i], registration->operation_names[j]) == 0) {
        return SALTS_EINVAL;
      }
    }
    operation = turbo_flow_find_operation(flow, registration->operation_names[i]);
    export_index = flow_find_operation_export_module(flow, registration->operation_names[i]);
    if (!operation || export_index < 0) {
      return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                       "adapter operation is not exported by a module");
    }
    if ((size_t)export_index != (size_t)module_index || !module ||
        !flow_module_adapter_operation_valid(registration, operation)) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "adapter operation is incompatible with its module owner");
    }
    if (operation->scope.state == TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER &&
        operation->resource_type) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "adapter-owner operation cannot require a resource primitive");
    }
    if (operation->resource_type) {
      if (!resource_name || resource_name[0] == '\0' ||
          !flow_module_exports_primitive(module, operation->resource_type)) {
        return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                         "adapter operation resource is not exported by its module");
      }
      primitive = turbo_flow_find_primitive(flow, resource_name);
      if (!primitive) {
        primitive = flow_module_adapter_pending_primitive(registration, resource_name);
      }
      if (!flow_module_adapter_primitive_compatible(primitive, operation)) {
        return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                         "adapter operation resource primitive is incompatible");
      }
    } else if (resource_name) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "adapter-owned operation cannot bind a resource primitive");
    }
  }

  for (i = 0; i < registration->primitive_count; ++i) {
    const turbo_flow_primitive_descriptor_t *primitive = &registration->primitives[i];
    const turbo_flow_primitive_descriptor_t *current;
    size_t j;
    if (primitive->size < sizeof(*primitive) || !primitive->name ||
        primitive->name[0] == '\0' || !primitive->type_name ||
        primitive->type_name[0] == '\0' || primitive->version == 0u ||
        primitive->domain < TURBO_FLOW_DOMAIN_DATA ||
        primitive->domain > TURBO_FLOW_DOMAIN_MANAGEMENT ||
        primitive->kind != TURBO_FLOW_PRIMITIVE_RESOURCE) {
      return SALTS_EINVAL;
    }
    for (j = 0; j < i; ++j) {
      if (strcmp(primitive->name, registration->primitives[j].name) == 0) return SALTS_EINVAL;
    }
    current = turbo_flow_find_primitive(flow, primitive->name);
    if (current &&
        (current->version != primitive->version || current->domain != primitive->domain ||
         current->kind != primitive->kind || strcmp(current->type_name, primitive->type_name) != 0)) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "adapter primitive contract is incompatible");
    }
  }

  if (turbo_flow_stl_error(vec_init_bytes(&bindings, sizeof(flow_adapter_operation_binding_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_reserve(&bindings, registration->operation_count)) != SALTS_OK) {
    flow_module_adapter_bindings_destroy(&bindings);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  for (i = 0; i < registration->operation_count; ++i) {
    flow_adapter_operation_binding_t binding;
    memset(&binding, 0, sizeof(binding));
    binding.operation_name = tstr_dup(registration->operation_names[i]);
    binding.module_name = tstr_dup(registration->module_name);
    if (registration->operation_resource_names && registration->operation_resource_names[i]) {
      binding.resource_name = tstr_dup(registration->operation_resource_names[i]);
    }
    if (!binding.operation_name || !binding.module_name ||
        (registration->operation_resource_names && registration->operation_resource_names[i] &&
         !binding.resource_name) ||
        turbo_flow_stl_error(vec_push(&bindings, &binding)) != SALTS_OK) {
      tstr_freep(&binding.operation_name);
      tstr_freep(&binding.module_name);
      tstr_freep(&binding.resource_name);
      flow_module_adapter_bindings_destroy(&bindings);
      return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    }
  }

  adapters_before = vec_size(&flow->adapters);
  resources_before = vec_size(&flow->resources);
  primitives_before = vec_size(&flow->primitives);
  for (i = 0; i < registration->primitive_count; ++i) {
    if (turbo_flow_find_primitive(flow, registration->primitives[i].name)) continue;
    rc = turbo_flow_register_primitive(flow, &registration->primitives[i]);
    if (rc != SALTS_OK) {
      flow_module_adapter_registry_rollback(flow, resources_before, primitives_before);
      flow_module_adapter_bindings_destroy(&bindings);
      return rc;
    }
  }
  for (i = 0; i < registration->resource_count; ++i) {
    const turbo_flow_resource_provider_registration_t *resource = &registration->resources[i];
    if (resource->size < sizeof(*resource) || !resource->owner_name ||
        resource->owner_name[0] == '\0') {
      flow_module_adapter_registry_rollback(flow, resources_before, primitives_before);
      flow_module_adapter_bindings_destroy(&bindings);
      return SALTS_EINVAL;
    }
    rc = turbo_flow_register_resource_provider(flow, resource->owner_name, &resource->ops,
                                               resource->ctx);
    if (rc != SALTS_OK) {
      flow_module_adapter_registry_rollback(flow, resources_before, primitives_before);
      flow_module_adapter_bindings_destroy(&bindings);
      return rc;
    }
  }
  rc = turbo_flow_register_adapter_ex(flow, registration->adapter_name, registration->ops,
                                      registration->ctx, registration->schema);
  if (rc != SALTS_OK) {
    flow_module_adapter_registry_rollback(flow, resources_before, primitives_before);
    flow_module_adapter_bindings_destroy(&bindings);
    return rc;
  }
  adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, adapters_before);
  adapter->consume_batch = registration->consume_batch;
  vec_destroy(&adapter->operation_bindings);
  adapter->operation_bindings = bindings;
  return SALTS_OK;
}

const char *turbo_flow_adapter_operation_module(const turbo_flow_t *flow,
                                                const char *adapter_name,
                                                const char *operation_name) {
  const flow_adapter_registration_t *adapter;
  const flow_adapter_operation_binding_t *binding;
  int index;
  if (!flow || !adapter_name || !operation_name) return NULL;
  index = flow_find_adapter(flow, adapter_name);
  if (index < 0) return NULL;
  adapter = (const flow_adapter_registration_t *)vec_at_const(&flow->adapters,
                                                                    (size_t)index);
  binding = flow_find_adapter_operation_binding(adapter, operation_name);
  return binding ? binding->module_name : NULL;
}

const char *turbo_flow_adapter_operation_resource(const turbo_flow_t *flow,
                                                  const char *adapter_name,
                                                  const char *operation_name) {
  const flow_adapter_registration_t *adapter;
  const flow_adapter_operation_binding_t *binding;
  int index;
  if (!flow || !adapter_name || !operation_name) return NULL;
  index = flow_find_adapter(flow, adapter_name);
  if (index < 0) return NULL;
  adapter = (const flow_adapter_registration_t *)vec_at_const(&flow->adapters,
                                                                    (size_t)index);
  binding = flow_find_adapter_operation_binding(adapter, operation_name);
  return binding ? binding->resource_name : NULL;
}

int turbo_flow_set_observer(turbo_flow_t *flow, const turbo_flow_observer_ops_t *ops, void *ctx) {
  if (!flow) return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "observer cannot change while flow is started");
  }
  if (ops && (flow->observer_ops.message_complete || flow->observer_ops.stage_complete ||
              flow->observer_ops.adapter_event || flow->observer_ops.flow_destroyed)) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "flow already has an observer");
  }
  if (ops && (ops->size < sizeof(*ops) || (!ops->message_complete && !ops->stage_complete &&
                                           !ops->adapter_event && !ops->flow_destroyed))) {
    return flow_set_error_keep_state(flow, SALTS_EINVAL, 0, 0, "observer callbacks are invalid");
  }
  if (ops) flow->observer_ops = *ops;
  else memset(&flow->observer_ops, 0, sizeof(flow->observer_ops));
  flow->observer_ctx = ops ? ctx : NULL;
  return SALTS_OK;
}

static int flow_find_event_observer(const turbo_flow_t *flow, const char *name) {
  if (!flow || !name) return -1;
  for (size_t i = 0u; i < vec_size(&flow->event_observers); ++i) {
    const flow_event_observer_registration_t *observer =
        (const flow_event_observer_registration_t *)vec_at_const(&flow->event_observers, i);
    if (observer && observer->name && strcmp(observer->name, name) == 0) return (int)i;
  }
  return -1;
}

static void flow_event_observer_destroy(flow_event_observer_registration_t *observer) {
  if (!observer) return;
  if (observer->ops.destroy) observer->ops.destroy(observer->ctx);
  tstr_freep(&observer->name);
  memset(&observer->ops, 0, sizeof(observer->ops));
  observer->ctx = NULL;
}

int flow_observer_event_enabled(const turbo_flow_t *flow,
                                turbo_flow_observe_event_kind_t kind) {
  const uint64_t mask =
      kind >= 0 && kind < TURBO_FLOW_OBSERVE_EVENT_COUNT
          ? TURBO_FLOW_OBSERVE_EVENT_MASK(kind)
          : 0u;
  if (!flow || mask == 0u) return 0;
  for (size_t i = 0u; i < vec_size(&flow->event_observers); ++i) {
    const flow_event_observer_registration_t *observer =
        (const flow_event_observer_registration_t *)vec_at_const(&flow->event_observers, i);
    if (observer && observer->ops.on_event && (observer->ops.event_mask & mask) != 0u) return 1;
  }
  return 0;
}

int flow_observer_has_handlers(const turbo_flow_t *flow) {
  if (!flow) return 0;
  return flow->observer_ops.message_complete || flow->observer_ops.stage_complete ||
         flow->observer_ops.adapter_event || vec_size(&flow->event_observers) != 0u;
}

void flow_observer_emit(turbo_flow_t *flow, const turbo_flow_observe_event_t *event) {
  turbo_flow_observe_event_t view;
  uint64_t mask;
  if (!flow || !event || event->kind < 0 || event->kind >= TURBO_FLOW_OBSERVE_EVENT_COUNT) return;
  mask = TURBO_FLOW_OBSERVE_EVENT_MASK(event->kind);
  view = *event;
  view.size = sizeof(view);
  if (view.timestamp_ns == 0u) view.timestamp_ns = salts_hrtime();
  for (size_t i = 0u; i < vec_size(&flow->event_observers); ++i) {
    const flow_event_observer_registration_t *observer =
        (const flow_event_observer_registration_t *)vec_at_const(&flow->event_observers, i);
    if (!observer || !observer->ops.on_event || (observer->ops.event_mask & mask) == 0u) continue;
    if (observer->ops.on_event(observer->ctx, &view) != SALTS_OK) {
      atomic_fetch_add_explicit(&flow->observer_failures, 1u, memory_order_relaxed);
    }
  }
}

void flow_observer_clear(turbo_flow_t *flow) {
  if (!flow) return;
  for (size_t i = 0u; i < vec_size(&flow->event_observers); ++i) {
    flow_event_observer_registration_t *observer =
        (flow_event_observer_registration_t *)vec_at(&flow->event_observers, i);
    flow_event_observer_destroy(observer);
  }
  turbo_flow_stl_error(vec_clear(&flow->event_observers));
}

int turbo_flow_register_observer(turbo_flow_t *flow, const char *name,
                                 const turbo_flow_event_observer_ops_t *ops, void *ctx) {
  flow_event_observer_registration_t observer;
  int rc;
  if (!flow || !name || name[0] == '\0' || !ops || ops->size < sizeof(*ops) || !ops->on_event ||
      ops->event_mask == 0u || (ops->event_mask & ~TURBO_FLOW_OBSERVE_ALL_EVENTS) != 0u) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "observer cannot register while flow is started");
  }
  if (flow_find_event_observer(flow, name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0,
                                     "observer name is already registered");
  }
  if (vec_size(&flow->event_observers) >= TURBO_FLOW_MAX_EVENT_OBSERVERS) {
    return flow_set_error_keep_state(flow, SALTS_ENOSPC, 0, 0,
                                     "observer registration limit reached");
  }
  memset(&observer, 0, sizeof(observer));
  observer.name = tstr_dup(name);
  if (!observer.name) return SALTS_ENOMEM;
  observer.ops = *ops;
  observer.ctx = ctx;
  rc = turbo_flow_stl_error(vec_push(&flow->event_observers, &observer));
  if (rc != SALTS_OK) {
    tstr_freep(&observer.name);
    return rc;
  }
  return SALTS_OK;
}

int turbo_flow_unregister_observer(turbo_flow_t *flow, const char *name) {
  int index;
  flow_event_observer_registration_t *observer;
  if (!flow || !name || name[0] == '\0') return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "observer cannot unregister while flow is started");
  }
  index = flow_find_event_observer(flow, name);
  if (index < 0) return SALTS_ENOENT;
  observer =
      (flow_event_observer_registration_t *)vec_at(&flow->event_observers, (size_t)index);
  flow_event_observer_destroy(observer);
  return turbo_flow_stl_error(vec_erase(&flow->event_observers, (size_t)index, NULL));
}

size_t turbo_flow_observer_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->event_observers) : 0u;
}

uint64_t turbo_flow_observer_failure_count(const turbo_flow_t *flow) {
  return flow ? atomic_load_explicit(&flow->observer_failures, memory_order_relaxed) : 0u;
}

size_t turbo_flow_adapter_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->adapters) : 0;
}

int turbo_flow_adapter_connection_snapshot_at(const turbo_flow_t *flow, size_t index,
                                              turbo_flow_connection_snapshot_t *out) {
  const flow_adapter_registration_t *adapter;
  int rc;
  if (!flow || !out) return SALTS_EINVAL;
  adapter = (const flow_adapter_registration_t *)vec_at_const(&flow->adapters, index);
  if (!adapter) return SALTS_EINVAL;
  if (!adapter->ops.connection_snapshot) return SALTS_ENOTSUP;
  memset(out, 0, sizeof(*out));
  rc = adapter->ops.connection_snapshot(adapter->ctx, out);
  if (rc != SALTS_OK) return rc;
  out->adapter_name = adapter->name;
  out->adapter_kind = adapter->schema.kind;
  out->direction = adapter->schema.direction;
  return SALTS_OK;
}

size_t turbo_flow_resource_count(const turbo_flow_t *flow) {
  size_t count = 0u;
  if (!flow) return 0u;
  for (size_t i = 0; i < vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *resource =
        (const flow_resource_registration_t *)vec_at_const(&flow->resources, i);
    if (resource && resource->ops.snapshot) ++count;
  }
  return count + flow_native_resource_count(flow) + vec_size(&flow->pool_records);
}

int turbo_flow_resource_snapshot_at(const turbo_flow_t *flow, size_t index,
                                    turbo_flow_resource_snapshot_t *out) {
  turbo_flow_pool_snapshot_t pool;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;

  if (!flow || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  for (size_t i = 0; i < vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *resource =
        (const flow_resource_registration_t *)vec_at_const(&flow->resources, i);
    if (!resource || !resource->ops.snapshot) continue;
    if (index != 0u) {
      --index;
      continue;
    }
    *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    rc = resource->ops.snapshot(resource->ctx, out);
    if (rc != SALTS_OK) return rc;
    rc = resource->ops.metadata(resource->ctx, &metadata);
    if (rc != SALTS_OK) return rc;
    if (!flow_resource_metadata_valid(&metadata) || out->size < sizeof(*out) ||
        out->domain != metadata.domain || out->kind != metadata.kind ||
        strcmp(out->uid, metadata.uid) != 0 || strcmp(out->owner_name, metadata.owner_name) != 0 ||
        out->generation != metadata.generation ||
        out->observed_generation != metadata.observed_generation ||
        out->observed_generation > out->generation) {
      return SALTS_EPROTO;
    }
    return SALTS_OK;
  }

  if (index < flow_native_resource_count(flow)) {
    return flow_native_resource_snapshot_at(flow, index, out);
  }
  index -= flow_native_resource_count(flow);

  rc = turbo_flow_pool_snapshot_at(flow, index, &pool);
  if (rc != SALTS_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = TURBO_FLOW_DOMAIN_EXECUTION;
  out->kind = TURBO_FLOW_RESOURCE_POOL;
  {
    turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    int uid_written;
    int owner_written;
    rc = turbo_flow_pool_resource_status_at(flow, index, &status);
    if (rc != SALTS_OK) return rc;
    uid_written = snprintf(out->uid, sizeof(out->uid), "%s", status.uid);
    owner_written = snprintf(out->owner_name, sizeof(out->owner_name), "%s", status.owner_name);
    if (uid_written < 0 || (size_t)uid_written >= sizeof(out->uid) || owner_written < 0 ||
        (size_t)owner_written >= sizeof(out->owner_name)) {
      return SALTS_ENAMETOOLONG;
    }
    out->generation = status.generation;
    out->observed_generation = status.observed_generation;
  }
  out->load = pool.active > UINT64_MAX - pool.queued ? UINT64_MAX : pool.active + pool.queued;
  out->capacity = pool.resource_capacity;
  if (out->capacity == 0u) {
    out->capacity = pool.queue_capacity > UINT64_MAX - pool.parallelism
                        ? UINT64_MAX
                        : pool.queue_capacity + pool.parallelism;
  }
  out->saturated = turbo_flow_pool_saturated(&pool);
  return SALTS_OK;
}

int flow_resource_metadata_valid(const turbo_flow_resource_metadata_t *metadata) {
  return metadata && metadata->size >= sizeof(*metadata) &&
         metadata->domain > TURBO_FLOW_DOMAIN_NONE &&
         metadata->domain <= TURBO_FLOW_DOMAIN_MANAGEMENT &&
         metadata->kind >= TURBO_FLOW_RESOURCE_CONNECTION &&
         metadata->kind <= TURBO_FLOW_RESOURCE_SECURITY_REALM && metadata->uid[0] != '\0' &&
         memchr(metadata->uid, '\0', sizeof(metadata->uid)) != NULL &&
         metadata->owner_name[0] != '\0' &&
         memchr(metadata->owner_name, '\0', sizeof(metadata->owner_name)) != NULL &&
         metadata->generation != 0u && metadata->observed_generation <= metadata->generation;
}

size_t turbo_flow_resource_metadata_count(const turbo_flow_t *flow) {
  if (!flow) return 0u;
  return vec_size(&flow->resources) + flow_native_resource_count(flow) +
         vec_size(&flow->pool_records);
}

int turbo_flow_resource_metadata_at(const turbo_flow_t *flow, size_t index,
                                    turbo_flow_resource_metadata_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!flow || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  for (size_t i = 0; i < vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *resource =
        (const flow_resource_registration_t *)vec_at_const(&flow->resources, i);
    int rc;
    if (index != 0u) {
      --index;
      continue;
    }
    if (!resource) return SALTS_EPROTO;
    rc = resource->ops.metadata(resource->ctx, &metadata);
    if (rc != SALTS_OK) return rc;
    if (!flow_resource_metadata_valid(&metadata) ||
        strcmp(metadata.owner_name, resource->owner_name) != 0) {
      return SALTS_EPROTO;
    }
    memcpy(out, &metadata, sizeof(metadata));
    return SALTS_OK;
  }
  if (index < flow_native_resource_count(flow)) {
    return flow_native_resource_metadata_at(flow, index, out);
  }
  index -= flow_native_resource_count(flow);
  {
    turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    int rc = turbo_flow_pool_resource_status_at(flow, index, &status);
    int written;
    if (rc != SALTS_OK) return rc;
    metadata.domain = TURBO_FLOW_DOMAIN_EXECUTION;
    metadata.kind = TURBO_FLOW_RESOURCE_POOL;
    metadata.generation = status.generation;
    metadata.observed_generation = status.observed_generation;
    written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", status.uid);
    if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return SALTS_ENAMETOOLONG;
    written = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", status.owner_name);
    if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return SALTS_ENAMETOOLONG;
    memcpy(out, &metadata, sizeof(metadata));
    return SALTS_OK;
  }
}

int turbo_flow_adapter_command(turbo_flow_t *flow, const char *adapter_name,
                               const turbo_flow_adapter_command_t *command) {
  flow_adapter_registration_t *adapter;
  int index;
  int rc;

  if (!flow || !adapter_name || adapter_name[0] == '\0' || !command ||
      command->size < sizeof(*command) || command->kind < TURBO_FLOW_ADAPTER_QUIESCE ||
      command->kind > TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->state != TURBO_FLOW_STATE_STARTED || flow->admission_state == FLOW_ADMISSION_STOPPING ||
      flow->admission_state == FLOW_ADMISSION_RESIZING) {
    rc = flow->state == TURBO_FLOW_STATE_STARTED ? SALTS_EBUSY : SALTS_EINVAL;
    salts_mutex_unlock(&flow->runtime_mutex);
    return rc;
  }
  salts_mutex_unlock(&flow->runtime_mutex);

  index = flow_find_adapter(flow, adapter_name);
  if (index < 0) return SALTS_ENOENT;
  adapter = (flow_adapter_registration_t *)vec_at(&flow->adapters, (size_t)index);
  if (!adapter || !adapter->ops.command) return SALTS_ENOTSUP;
  rc = adapter->ops.command(adapter->ctx, flow, command);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, 0, 0, "adapter command failed");
  }
  flow_clear_error(flow);
  return SALTS_OK;
}

const turbo_flow_adapter_schema_t *turbo_flow_adapter_schema_at(const turbo_flow_t *flow,
                                                                size_t index) {
  const flow_adapter_registration_t *adapter;

  if (!flow) return NULL;
  adapter = (const flow_adapter_registration_t *)vec_at_const(&flow->adapters, index);
  if (!adapter || adapter->schema.roles == 0) return NULL;
  return &adapter->schema;
}

const turbo_flow_adapter_schema_t *turbo_flow_find_adapter_schema(const turbo_flow_t *flow,
                                                                  const char *name) {
  int index = flow_find_adapter(flow, name);
  if (index < 0) return NULL;
  return turbo_flow_adapter_schema_at(flow, (size_t)index);
}

turbo_flow_state_t turbo_flow_state(const turbo_flow_t *flow) {
  turbo_flow_state_t state;
  if (!flow || !flow->runtime_sync_initialized) return TURBO_FLOW_STATE_FAILED;
  salts_mutex_lock((salts_mutex_t *)&flow->runtime_mutex);
  state = flow->state;
  salts_mutex_unlock((salts_mutex_t *)&flow->runtime_mutex);
  return state;
}

int turbo_flow_runtime_snapshot(const turbo_flow_t *flow, turbo_flow_runtime_snapshot_t *out) {
  if (!flow || !out || !flow->runtime_sync_initialized) return SALTS_EINVAL;
  salts_mutex_lock((salts_mutex_t *)&flow->runtime_mutex);
  memset(out, 0, sizeof(*out));
  out->state = flow->state;
  out->accepting_publishes = flow->admission_state == FLOW_ADMISSION_OPEN;
  out->active_publishes = flow->active_publishes;
  out->stage_count = vec_size(&flow->stages);
  out->edge_count = vec_size(&flow->edges);
  out->adapter_count = vec_size(&flow->adapters);
  out->pool_count = vec_size(&flow->pool_records);
  salts_mutex_unlock((salts_mutex_t *)&flow->runtime_mutex);
  return SALTS_OK;
}

const turbo_flow_error_t *turbo_flow_last_error(const turbo_flow_t *flow) {
  if (!flow) return NULL;
  if (flow_active_error_owner == flow || flow_last_publish_error_owner == flow) {
    return &flow_publish_error;
  }
  return &flow->last_error;
}

size_t turbo_flow_stage_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->stages) : 0;
}

const turbo_flow_stage_plan_t *turbo_flow_stage_at(const turbo_flow_t *flow, size_t index) {
  static SALTS_THREAD_LOCAL turbo_flow_stage_plan_t view;
  const flow_stage_plan_impl_t *stage;

  if (!flow) return NULL;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, index);
  if (!stage) return NULL;
  memset(&view, 0, sizeof(view));
  view.name = stage->name;
  view.is_source = stage->is_source;
  view.adapter_name = stage->adapter_name;
  view.operation_name =
      stage->operation_resolved ? stage->resolved_operation.name : stage->operation_name;
  view.resource_name = stage->resource_name;
  view.data_strategy = stage->data_strategy;
  view.data_worker_count = stage->data_worker_count;
  view.exec = stage->exec;
  view.mutability = stage->mutability;
  view.effects = stage->effects;
  view.retry = stage->retry;
  view.reorder = stage->reorder;
  return &view;
}

const turbo_flow_operation_descriptor_t *turbo_flow_stage_operation_at(const turbo_flow_t *flow,
                                                                       size_t index) {
  const flow_stage_plan_impl_t *stage;
  if (!flow) return NULL;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, index);
  return flow_stage_operation_descriptor(stage);
}

int turbo_flow_find_stage(const turbo_flow_t *flow, const char *name) {
  return flow && name ? flow_find_stage_view(flow, vstr_from_cstr(name)) : -1;
}

size_t turbo_flow_edge_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->edges) : 0;
}

const turbo_flow_edge_plan_t *turbo_flow_edge_at(const turbo_flow_t *flow, size_t index) {
  static SALTS_THREAD_LOCAL turbo_flow_edge_plan_t view;
  const flow_edge_plan_impl_t *edge;

  if (!flow) return NULL;
  edge = (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, index);
  if (!edge) return NULL;
  view.from_stage = edge->from_stage;
  view.to_stage = edge->to_stage;
  view.line = edge->line;
  view.column = edge->column;
  view.kind = edge->kind;
  view.condition = edge->condition;
  view.name = edge->name;
  return &view;
}
