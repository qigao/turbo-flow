#include "flow_internal.h"

#include <stdlib.h>
#include <string.h>

static int compile_validate_edges(turbo_flow_t *flow) {
  size_t i;

  for (i = 0; i < vec_size(&flow->edges); ++i) {
    flow_edge_plan_impl_t *edge = (flow_edge_plan_impl_t *)vec_at(&flow->edges, i);
    int from_stage = flow_find_stage_view(flow, tstr_to_v(edge->from_name));
    int to_stage = flow_find_stage_view(flow, tstr_to_v(edge->to_name));
    if (from_stage < 0 || to_stage < 0) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "edge references an unknown stage");
    }
    edge->from_stage = (uint32_t)from_stage;
    edge->to_stage = (uint32_t)to_stage;
    if (edge->kind == TURBO_FLOW_EDGE_CONDITIONAL) {
      turbo_flow_error_t error;
      int rc;

      turbo_flow_expr_destroy(edge->predicate);
      edge->predicate = NULL;
      rc = flow_expr_projection_compile(flow, edge->condition, tstr_len(edge->condition),
                                        &edge->predicate, &error);
      if (rc != SALTS_OK) {
        return flow_set_error(flow, rc, edge->line, edge->column, error.message);
      }
      if (turbo_flow_expr_result_type(edge->predicate) != TURBO_FLOW_EXPR_TYPE_BOOL) {
        turbo_flow_expr_destroy(edge->predicate);
        edge->predicate = NULL;
        return flow_set_error(flow, SALTS_EPROTO, edge->line, edge->column,
                              "conditional route expression must return BOOL");
      }
    }
  }
  return SALTS_OK;
}

static int compile_validate_reject_edges(turbo_flow_t *flow) {
  for (size_t i = 0; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);
    const flow_stage_plan_impl_t *from;

    if (!edge || edge->kind != TURBO_FLOW_EDGE_REJECT) continue;
    from = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->from_stage);
    if (!from || from->is_source || from->is_port) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "reject route source must be an executable stage");
    }
    for (size_t j = i + 1; j < vec_size(&flow->edges); ++j) {
      const flow_edge_plan_impl_t *other =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, j);
      if (!other || other->kind != TURBO_FLOW_EDGE_REJECT) continue;
      if (strcmp(edge->name, other->name) == 0) {
        return flow_set_error(flow, SALTS_EALREADY, other->line, other->column,
                              "duplicate reject route name");
      }
      if (edge->from_stage == other->from_stage) {
        return flow_set_error(flow, SALTS_EALREADY, other->line, other->column,
                              "stage has more than one reject route");
      }
    }
  }
  return SALTS_OK;
}

static int compile_validate_sources(turbo_flow_t *flow) {
  size_t i;

  for (i = 0; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (stage->is_source) return SALTS_OK;
  }

  return flow_set_error(flow, SALTS_EINVAL, 0, 0, "stage plan requires at least one source");
}

static int stage_composite_prefix(const flow_stage_plan_impl_t *stage, const char **prefix,
                                  size_t *prefix_len) {
  const char *dot;

  if (!stage || !stage->name) return 0;
  dot = strchr(stage->name, '.');
  if (!dot || dot == stage->name) return 0;
  if (prefix) *prefix = stage->name;
  if (prefix_len) *prefix_len = (size_t)(dot - stage->name);
  return 1;
}

static int same_composite_prefix(const flow_stage_plan_impl_t *left,
                                 const flow_stage_plan_impl_t *right) {
  const char *left_prefix = NULL;
  const char *right_prefix = NULL;
  size_t left_len = 0;
  size_t right_len = 0;

  if (!stage_composite_prefix(left, &left_prefix, &left_len) ||
      !stage_composite_prefix(right, &right_prefix, &right_len)) {
    return 0;
  }

  return left_len == right_len && strncmp(left_prefix, right_prefix, left_len) == 0;
}

static int stage_direct_composite_prefix(const flow_stage_plan_impl_t *stage, const char **prefix,
                                         size_t *prefix_len) {
  const char *dot;

  if (!stage || !stage->name) return 0;
  dot = strrchr(stage->name, '.');
  if (!dot || dot == stage->name) return 0;
  if (prefix) *prefix = stage->name;
  if (prefix_len) *prefix_len = (size_t)(dot - stage->name);
  return 1;
}

static int same_direct_composite_prefix(const flow_stage_plan_impl_t *left,
                                        const flow_stage_plan_impl_t *right) {
  const char *left_prefix = NULL;
  const char *right_prefix = NULL;
  size_t left_len = 0;
  size_t right_len = 0;

  if (!stage_direct_composite_prefix(left, &left_prefix, &left_len) ||
      !stage_direct_composite_prefix(right, &right_prefix, &right_len)) {
    return 0;
  }

  return left_len == right_len && strncmp(left_prefix, right_prefix, left_len) == 0;
}

static int stage_matches_composite_prefix(const flow_stage_plan_impl_t *stage, const char *prefix,
                                          size_t prefix_len) {
  const char *stage_prefix = NULL;
  size_t stage_prefix_len = 0;

  if (!stage_composite_prefix(stage, &stage_prefix, &stage_prefix_len)) return 0;
  return stage_prefix_len == prefix_len && strncmp(stage_prefix, prefix, prefix_len) == 0;
}

static int composite_prefix_is_active(const turbo_flow_t *flow, const char *prefix,
                                      size_t prefix_len) {
  size_t edge_index;

  for (edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    const flow_stage_plan_impl_t *from;
    const flow_stage_plan_impl_t *to;

    if (!edge || edge->is_stage_internal) continue;
    from = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->from_stage);
    to = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);
    if (stage_matches_composite_prefix(from, prefix, prefix_len) ||
        stage_matches_composite_prefix(to, prefix, prefix_len)) {
      return 1;
    }
  }
  return 0;
}

static int stage_in_inactive_template(const turbo_flow_t *flow,
                                      const flow_stage_plan_impl_t *stage) {
  const char *prefix = NULL;
  size_t prefix_len = 0;

  if (!stage_composite_prefix(stage, &prefix, &prefix_len)) return 0;
  return !composite_prefix_is_active(flow, prefix, prefix_len);
}

static int compile_validate_composite_stage_boundaries(turbo_flow_t *flow) {
  size_t i;

  for (i = 0; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);
    const flow_stage_plan_impl_t *from =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->from_stage);
    const flow_stage_plan_impl_t *to =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);

    if (edge->is_stage_internal) {
      if (!same_composite_prefix(from, to)) {
        return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                              "stage internal edges must stay inside the same composite stage");
      }
      if (from->is_port && from->is_port_output && same_direct_composite_prefix(from, to)) {
        return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                              "stage internal edges may not leave output ports");
      }
      if (to->is_port && !to->is_port_output && same_direct_composite_prefix(from, to)) {
        return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                              "stage internal edges may not enter input ports");
      }
      continue;
    }

    if (stage_composite_prefix(from, NULL, NULL) && (!from->is_port || !from->is_port_output)) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "external edges may only leave composite stage output ports");
    }

    if (stage_composite_prefix(to, NULL, NULL) && (!to->is_port || to->is_port_output)) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "external edges may only enter composite stage input ports");
    }
  }

  return SALTS_OK;
}

static int compile_validate_duplicate_edges(turbo_flow_t *flow) {
  size_t i;
  size_t j;

  for (i = 0; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *left =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);

    for (j = i + 1; j < vec_size(&flow->edges); ++j) {
      const flow_edge_plan_impl_t *right =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, j);
      if (left->from_stage == right->from_stage && left->to_stage == right->to_stage &&
          left->kind == right->kind) {
        return flow_set_error(flow, SALTS_EALREADY, right->line, right->column,
                              "duplicate stage edge");
      }
    }
  }

  return SALTS_OK;
}

static void mark_reachable_from_stage(const turbo_flow_t *flow, uint8_t *reachable,
                                      uint32_t stage) {
  if (reachable[stage]) return;
  reachable[stage] = 1;

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    if (edge->from_stage == stage) mark_reachable_from_stage(flow, reachable, edge->to_stage);
  }
}

static int compile_validate_source_reachability(turbo_flow_t *flow) {
  size_t stage_count = vec_size(&flow->stages);
  uint8_t *reachable = NULL;
  int rc = SALTS_OK;

  if (stage_count == 0) return SALTS_OK;

  reachable = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  if (!reachable) return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    if (stage->is_source) mark_reachable_from_stage(flow, reachable, (uint32_t)stage_index);
  }

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    if (!reachable[stage_index] && !stage_in_inactive_template(flow, stage)) {
      rc = flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                          "stage is not reachable from any source");
      goto cleanup;
    }
  }

cleanup:
  free(reachable);
  return rc;
}

static int composite_stage_reaches_input_reverse(turbo_flow_t *flow, uint32_t stage_index,
                                                 uint8_t *seen) {
  const flow_stage_plan_impl_t *stage =
      (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);

  if (seen[stage_index]) return 0;
  seen[stage_index] = 1;

  if (stage->is_port && !stage->is_port_output) return 1;

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    const flow_stage_plan_impl_t *from;

    if (!edge->is_stage_internal || edge->to_stage != stage_index) continue;
    from = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->from_stage);
    if (!same_composite_prefix(stage, from)) continue;
    if (composite_stage_reaches_input_reverse(flow, edge->from_stage, seen)) return 1;
  }

  return 0;
}

static int compile_validate_composite_stage_reachability(turbo_flow_t *flow) {
  size_t stage_count = vec_size(&flow->stages);
  uint8_t *seen = NULL;
  int rc = SALTS_OK;

  if (stage_count == 0) return SALTS_OK;

  seen = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  if (!seen) return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);

    if (!stage->is_port || !stage->is_port_output) continue;
    if (stage_in_inactive_template(flow, stage)) continue;
    memset(seen, 0, stage_count * sizeof(uint8_t));
    if (!composite_stage_reaches_input_reverse(flow, (uint32_t)stage_index, seen)) {
      rc = flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                          "stage output port is not reachable from an input port");
      goto cleanup;
    }
  }

cleanup:
  free(seen);
  return rc;
}

static int compile_validate_registrations(turbo_flow_t *flow) {
  size_t i;

  flow->has_async_terminal_stage = 0;
  for (i = 0; i < vec_size(&flow->stages); ++i) {
    flow_stage_plan_impl_t *stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, i);
    int reg_index = flow_find_registration(flow, stage->name);
    int provider_index = stage->operation_name
                             ? flow_find_operation_provider(flow, stage->operation_name,
                                                            stage->resource_name)
                             : -1;
    int module_index = stage->operation_name
                           ? flow_find_operation_export_module(flow, stage->operation_name)
                           : -1;
    const turbo_flow_operation_descriptor_t *operation =
        stage->operation_name ? turbo_flow_find_operation(flow, stage->operation_name) : NULL;
    const flow_adapter_registration_t *adapter = NULL;

    if (stage_in_inactive_template(flow, stage)) continue;

    if (stage->adapter_name) {
      adapter = flow_adapter_for_stage(flow, stage);
      if (!adapter) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "stage or source adapter is not registered");
      }
      if (adapter->async_terminal_ops.submit) {
        size_t edge_index;
        flow->has_async_terminal_stage = 1;
        if (stage->is_source || stage->is_port || stage->exec.kind != TURBO_FLOW_EXEC_INLINE ||
            stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL || stage->retry.max_attempts > 1u ||
            stage->reorder.capacity > 0u) {
          return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                "async terminal adapter requires a direct inline terminal stage");
        }
        for (edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
          const flow_edge_plan_impl_t *edge =
              (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
          if (edge && edge->from_stage == (uint32_t)i) {
            return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                                  "async terminal adapter stage must not have outgoing edges");
          }
        }
      }
      if (adapter->schema.roles != 0) {
        uint32_t roles = adapter->schema.roles;
        if (stage->is_source && !(roles & TURBO_FLOW_ADAPTER_SOURCE)) {
          return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                                "adapter schema does not allow source usage");
        }
        if (!stage->is_source && !stage->is_port &&
            !(roles & (TURBO_FLOW_ADAPTER_SINK | TURBO_FLOW_ADAPTER_TRANSFORM))) {
          return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                                "adapter schema does not allow stage usage");
        }
        if (!stage->is_source && !stage->is_port && (roles & TURBO_FLOW_ADAPTER_SINK) &&
            !(roles & TURBO_FLOW_ADAPTER_TRANSFORM)) {
          size_t edge_index;
          for (edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
            const flow_edge_plan_impl_t *edge =
                (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
            if (edge && edge->from_stage == (uint32_t)i && edge->kind != TURBO_FLOW_EDGE_REJECT) {
              return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                                    "sink adapter stage must be terminal");
            }
          }
        }
      }
    }

    if (stage->retry.max_attempts > 1u) {
      if (!adapter || !adapter->ops.consume_retry) {
        return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                              "retry requires an adapter retry callback");
      }
      if (reg_index >= 0 || provider_index >= 0) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "retry adapter stage may not override consume with a callback");
      }
    }

    if (module_index >= 0) {
      const flow_module_registration_t *module =
          (const flow_module_registration_t *)vec_at_const(&flow->modules,
                                                                 (size_t)module_index);
      if (provider_index >= 0) {
        const flow_operation_provider_registration_t *provider =
            (const flow_operation_provider_registration_t *)vec_at_const(
                &flow->operation_providers, (size_t)provider_index);
        if (!provider || !provider->module_name || !module || !module->name ||
            strcmp(provider->module_name, module->name) != 0) {
          return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                                "cataloged operation provider is not bound to its module owner");
        }
      } else if (reg_index >= 0) {
        return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                              "legacy stage callback cannot implement a cataloged operation");
      } else if (adapter) {
        const flow_adapter_operation_binding_t *binding =
            flow_find_adapter_operation_binding(adapter, stage->operation_name);
        if (!binding || !binding->module_name || !module || !module->name ||
            strcmp(binding->module_name, module->name) != 0) {
          return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                                "cataloged adapter operation is not bound to its module owner");
        }
        if (operation && operation->resource_type &&
            (!binding->resource_name || !stage->resource_name ||
             strcmp(binding->resource_name, stage->resource_name) != 0)) {
          return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                                "cataloged adapter operation is bound to another resource primitive");
        }
        if (stage->is_source && !adapter->ops.start) {
          return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                                "cataloged source adapter operation has no start callback");
        }
      } else if (operation &&
                 operation->scope.state == TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER) {
        return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                              "adapter-owner operation requires a typed adapter binding");
      }
    } else if (operation && operation->scope.state == TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER) {
      return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                            "adapter-owner operation requires a module owner");
    }

    if (!stage->is_source && !stage->is_port && reg_index < 0 && provider_index < 0 &&
        (!adapter || (!adapter->ops.consume && !adapter->async_terminal_ops.submit))) {
      return flow_set_error(
          flow, SALTS_EINVAL, stage->line, stage->column,
          "stage callback, operation provider, or adapter consume callback is not registered");
    }
    if (!stage->is_source && !stage->is_port && reg_index < 0 && provider_index < 0 && adapter &&
        (adapter->ops.consume || adapter->async_terminal_ops.submit) &&
        stage->exec.kind != TURBO_FLOW_EXEC_INLINE) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "adapter-owned consume requires the inline executor");
    }
    if (!stage->is_source && !stage->is_port && (reg_index >= 0 || provider_index >= 0)) {
      const flow_stage_registration_t *reg = NULL;
      const flow_operation_provider_registration_t *provider = NULL;
      if (provider_index >= 0) {
        provider = (const flow_operation_provider_registration_t *)vec_at_const(
            &flow->operation_providers, (size_t)provider_index);
      } else {
        reg = (const flow_stage_registration_t *)vec_at_const(&flow->registrations,
                                                                    (size_t)reg_index);
      }
      stage->mutability = provider ? provider->options.mutability : reg->options.mutability;
      stage->effects = provider ? provider->options.effects : reg->options.effects;
      if ((stage->effects & TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u &&
          stage->exec.kind != TURBO_FLOW_EXEC_INLINE) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "dynamic decision stage requires the inline executor");
      }
      stage->fn = provider ? provider->fn : reg->fn;
      stage->emit_fn = provider ? provider->emit_fn : NULL;
      stage->key_selector = provider ? provider->key_selector : NULL;
      stage->key_ctx = provider ? provider->key_ctx : NULL;
      stage->keyed_fn = provider ? provider->keyed_fn : NULL;
      stage->keyed_emit_fn = provider ? provider->keyed_emit_fn : NULL;
      stage->window_fn = provider ? provider->window_fn : NULL;
      stage->window_close_fn = provider ? provider->window_close_fn : NULL;
      stage->keyed_store = provider ? provider->keyed_store : NULL;
      stage->max_outputs = provider ? provider->max_outputs : 0u;
      stage->ctx = provider ? provider->ctx : reg->ctx;
    }
  }
  return SALTS_OK;
}

static uint32_t operation_exec_bit(turbo_flow_exec_kind_t kind) {
  switch (kind) {
  case TURBO_FLOW_EXEC_INLINE:
    return TURBO_FLOW_OPERATION_EXEC_INLINE;
  case TURBO_FLOW_EXEC_THREAD_POOL:
    return TURBO_FLOW_OPERATION_EXEC_THREAD;
  case TURBO_FLOW_EXEC_CORO_POOL:
    return TURBO_FLOW_OPERATION_EXEC_CORO;
  default:
    return 0;
  }
}

static const char FLOW_CORE_MESSAGE_TYPE[] = "Message";
static const char FLOW_CORE_SOURCE_OPERATION[] = "core.source";
static const char FLOW_CORE_INLINE_OPERATION[] = "core.stage.inline";
static const char FLOW_CORE_OWNER_OPERATION[] = "core.stage.owner";
static const char FLOW_CORE_THREAD_OPERATION[] = "core.stage.thread";
static const char FLOW_CORE_CORO_OPERATION[] = "core.stage.coro";
static const char FLOW_CORE_WORKER_OPERATION[] = "core.stage.worker";
static const char FLOW_CORE_INPUT_PORT_OPERATION[] = "core.port.input";
static const char FLOW_CORE_OUTPUT_PORT_OPERATION[] = "core.port.output";

static int stage_has_reject_edge(const turbo_flow_t *flow, uint32_t stage_index);

static const char *flow_core_operation_name(const flow_stage_plan_impl_t *stage) {
  if (stage->is_port) {
    return stage->is_port_output ? FLOW_CORE_OUTPUT_PORT_OPERATION : FLOW_CORE_INPUT_PORT_OPERATION;
  }
  if (stage->is_source) return FLOW_CORE_SOURCE_OPERATION;
  if (stage->adapter_name && !stage->fn) return FLOW_CORE_OWNER_OPERATION;
  if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) return FLOW_CORE_WORKER_OPERATION;
  if (stage->exec.kind == TURBO_FLOW_EXEC_THREAD_POOL) return FLOW_CORE_THREAD_OPERATION;
  if (stage->exec.kind == TURBO_FLOW_EXEC_CORO_POOL) return FLOW_CORE_CORO_OPERATION;
  return FLOW_CORE_INLINE_OPERATION;
}

static void flow_resolve_core_operation(const turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                        uint32_t stage_index) {
  turbo_flow_operation_descriptor_t *operation = &stage->resolved_operation;
  memset(operation, 0, sizeof(*operation));
  operation->size = sizeof(*operation);
  operation->name = flow_core_operation_name(stage);
  operation->version = 1u;
  operation->domain = TURBO_FLOW_DOMAIN_DATA;
  if (!stage->is_port) {
    operation->output_domain = TURBO_FLOW_DOMAIN_DATA;
    operation->output_type = FLOW_CORE_MESSAGE_TYPE;
  }
  operation->scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation->scope.state = TURBO_FLOW_STATE_SCOPE_PRIVATE;
  operation->scope.lifetime = stage->is_source ? TURBO_FLOW_LIFETIME_DISPATCH
                                                : TURBO_FLOW_LIFETIME_TASK;
  operation->scope.authority = stage->is_source ? TURBO_FLOW_AUTHORITY_OWNER_LOCAL
                                                 : TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  operation->runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;
  if (stage->is_port) {
    operation->flags = TURBO_FLOW_OPERATION_STAGE;
    operation->execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
    operation->scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
    operation->scope.authority = TURBO_FLOW_AUTHORITY_PURE;
    operation->scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  } else if (stage->is_source) {
    operation->flags = TURBO_FLOW_OPERATION_SOURCE;
    operation->execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
    operation->scope.concurrency = stage->adapter_name ? TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT
                                                       : TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  } else {
    operation->flags = TURBO_FLOW_OPERATION_STAGE;
    operation->input_domain = TURBO_FLOW_DOMAIN_DATA;
    operation->input_type = FLOW_CORE_MESSAGE_TYPE;
    operation->execution_mask = operation_exec_bit(stage->exec.kind);
    operation->scope.concurrency = stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL ||
                                           stage->exec.kind != TURBO_FLOW_EXEC_INLINE
                                       ? TURBO_FLOW_CONCURRENCY_POOL
                                       : TURBO_FLOW_CONCURRENCY_INLINE_LANE;
    if (stage->adapter_name && !stage->fn) {
      operation->scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
      operation->scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    }
    if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
      operation->runtime.handoff = TURBO_FLOW_HANDOFF_BOUNDED;
      operation->runtime.backpressure = TURBO_FLOW_BACKPRESSURE_BLOCK;
      operation->runtime.capacity = stage->data_pool_capacity;
    }
    if (stage->retry.max_attempts > 1u) {
      operation->runtime.error_mode = TURBO_FLOW_ERROR_RETRY;
      operation->runtime.settlement = TURBO_FLOW_SETTLEMENT_RETRY;
    } else if (stage_has_reject_edge(flow, stage_index)) {
      operation->runtime.error_mode = TURBO_FLOW_ERROR_REJECT;
    }
  }
  stage->operation_resolved = 1;
}

static int flow_port_bind_type(turbo_flow_t *flow, flow_stage_plan_impl_t *port,
                               turbo_flow_domain_t domain, const char *type, uint32_t line,
                               uint32_t column, int *changed) {
  turbo_flow_operation_descriptor_t *operation;
  if (!port || !port->is_port || !type || domain == TURBO_FLOW_DOMAIN_NONE || !changed)
    return SALTS_EINVAL;
  operation = &port->resolved_operation;
  if (!operation->input_type) {
    operation->domain = domain;
    operation->input_domain = domain;
    operation->input_type = type;
    operation->output_domain = domain;
    operation->output_type = type;
    *changed = 1;
    return SALTS_OK;
  }
  if (operation->input_domain != domain || strcmp(operation->input_type, type) != 0) {
    return flow_set_error(flow, SALTS_EINVAL, line, column,
                          "composite port connects incompatible operation types");
  }
  return SALTS_OK;
}

static int compile_resolve_port_types(turbo_flow_t *flow) {
  size_t stage_count = vec_size(&flow->stages);
  for (size_t pass = 0u; pass < stage_count; ++pass) {
    int changed = 0;
    for (size_t i = 0u; i < vec_size(&flow->edges); ++i) {
      const flow_edge_plan_impl_t *edge =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);
      flow_stage_plan_impl_t *from =
          (flow_stage_plan_impl_t *)vec_at(&flow->stages, edge->from_stage);
      flow_stage_plan_impl_t *to =
          (flow_stage_plan_impl_t *)vec_at(&flow->stages, edge->to_stage);
      const turbo_flow_operation_descriptor_t *from_operation;
      const turbo_flow_operation_descriptor_t *to_operation;
      int rc;
      if (!from || !to || stage_in_inactive_template(flow, from) ||
          stage_in_inactive_template(flow, to)) {
        continue;
      }
      from_operation = flow_stage_operation_descriptor(from);
      to_operation = flow_stage_operation_descriptor(to);
      if (!from_operation || !to_operation) return SALTS_EPROTO;
      if (to->is_port && from_operation->output_type) {
        rc = flow_port_bind_type(flow, to, from_operation->output_domain,
                                 from_operation->output_type, edge->line, edge->column, &changed);
        if (rc != SALTS_OK) return rc;
      }
      if (from->is_port && to_operation->input_type) {
        rc = flow_port_bind_type(flow, from, to_operation->input_domain, to_operation->input_type,
                                 edge->line, edge->column, &changed);
        if (rc != SALTS_OK) return rc;
      }
    }
    if (!changed) break;
  }
  for (size_t i = 0u; i < stage_count; ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (stage && stage->is_port && !stage_in_inactive_template(flow, stage) &&
        !stage->resolved_operation.input_type) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "composite port type cannot be resolved from operation edges");
    }
  }
  return SALTS_OK;
}

static int compile_resolve_operations(turbo_flow_t *flow) {
  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    flow_stage_plan_impl_t *stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, i);
    const turbo_flow_operation_descriptor_t *registered;
    if (!stage) return SALTS_EINVAL;
    memset(&stage->resolved_operation, 0, sizeof(stage->resolved_operation));
    stage->operation_resolved = 0;
    if (stage_in_inactive_template(flow, stage)) continue;
    if (!stage->operation_name) {
      flow_resolve_core_operation(flow, stage, (uint32_t)i);
      continue;
    }
    registered = turbo_flow_find_operation(flow, stage->operation_name);
    if (!registered) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "stage or source operation is not registered");
    }
    stage->resolved_operation = *registered;
    stage->operation_resolved = 1;
  }
  return SALTS_OK;
}

static int stage_has_reject_edge(const turbo_flow_t *flow, uint32_t stage_index) {
  size_t i;
  for (i = 0; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);
    if (edge && edge->from_stage == stage_index && edge->kind == TURBO_FLOW_EDGE_REJECT) return 1;
  }
  return 0;
}

static int compile_validate_operation_runtime(turbo_flow_t *flow,
                                              const flow_stage_plan_impl_t *stage,
                                              uint32_t stage_index,
                                              const turbo_flow_operation_descriptor_t *operation) {
  const turbo_flow_operation_runtime_contract_t *runtime = &operation->runtime;
  const uint32_t owner_settlement =
      TURBO_FLOW_SETTLEMENT_COMPLETE | TURBO_FLOW_SETTLEMENT_REQUEUE |
      TURBO_FLOW_SETTLEMENT_DEAD_LETTER | TURBO_FLOW_SETTLEMENT_ACKNOWLEDGE |
      TURBO_FLOW_SETTLEMENT_CANCELED;
  const flow_adapter_registration_t *adapter = flow_adapter_for_stage(flow, stage);

  if (stage->is_source && runtime->deadline_ms != 0u) {
    return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                          "source operation deadline requires an adapter owner contract");
  }
  if (adapter && adapter->async_terminal_ops.submit && runtime->deadline_ms != 0u) {
    return flow_set_error(
        flow, SALTS_ENOTSUP, stage->line, stage->column,
        "async terminal operation deadline requires adapter-owned timeout completion");
  }
  if (stage->is_source && runtime->settlement != 0u) {
    return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                          "source settlement requires an ingress owner completion contract");
  }
  if (((runtime->settlement & owner_settlement) != 0 ||
       runtime->error_mode == TURBO_FLOW_ERROR_SETTLE) &&
      (!adapter || !adapter->settlement_ops.apply)) {
    return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                          "operation settlement owner is not configured");
  }
  if (runtime->handoff == TURBO_FLOW_HANDOFF_BOUNDED) {
    if (stage->is_source || stage->data_strategy != TURBO_FLOW_DATA_WORKER_POOL) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "bounded operation handoff requires a worker segment");
    }
    if (runtime->capacity != stage->data_pool_capacity) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "operation capacity does not match worker capacity");
    }
    if (runtime->backpressure == TURBO_FLOW_BACKPRESSURE_DROP_OLDEST) {
      return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                            "worker Disruptor cannot drop an older entry before active sequences complete");
    }
    if (runtime->backpressure != TURBO_FLOW_BACKPRESSURE_BLOCK &&
        runtime->backpressure != TURBO_FLOW_BACKPRESSURE_FAIL &&
        runtime->backpressure != TURBO_FLOW_BACKPRESSURE_DROP_NEWEST) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "bounded operation backpressure policy is invalid");
    }
  } else if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
    return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                          "worker segment requires a bounded operation handoff");
  }
  if (runtime->ordering == TURBO_FLOW_ORDERING_PRESERVE_INPUT &&
      stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL && stage->reorder.capacity == 0) {
    return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                          "ordered worker operation requires a reorder boundary");
  }
  if (runtime->cancellation == TURBO_FLOW_CANCELLATION_COOPERATIVE &&
      (stage->is_source || (stage->data_strategy != TURBO_FLOW_DATA_WORKER_POOL &&
                            stage->exec.kind != TURBO_FLOW_EXEC_THREAD_POOL &&
                            stage->exec.kind != TURBO_FLOW_EXEC_CORO_POOL))) {
    return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                          "cooperative cancellation requires a task executor");
  }
  if (runtime->error_mode == TURBO_FLOW_ERROR_REJECT && !stage_has_reject_edge(flow, stage_index)) {
    return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                          "reject error mode requires a reject edge");
  }
  if (runtime->error_mode == TURBO_FLOW_ERROR_RETRY) {
    if (stage->retry.max_attempts <= 1u) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "retry error mode requires a retry policy");
    }
    if ((runtime->settlement & TURBO_FLOW_SETTLEMENT_RETRY) == 0) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "retry error mode requires retry settlement");
    }
  }
  return SALTS_OK;
}

static int compile_validate_operation_bindings(turbo_flow_t *flow) {
  size_t i;

  for (i = 0; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    const turbo_flow_operation_descriptor_t *operation;
    const turbo_flow_primitive_descriptor_t *resource = NULL;
    const flow_adapter_registration_t *adapter = NULL;
    uint32_t required_role;
    uint32_t exec_bit;

    if (!stage || stage_in_inactive_template(flow, stage) || stage->is_port) continue;
    operation = flow_stage_operation_descriptor(stage);
    if (!operation) {
      return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                            "runtime node has no resolved operation contract");
    }
    adapter = flow_adapter_for_stage(flow, stage);
    required_role = stage->is_source ? TURBO_FLOW_OPERATION_SOURCE : TURBO_FLOW_OPERATION_STAGE;
    if (!(operation->flags & required_role)) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            stage->is_source ? "operation does not allow source usage"
                                             : "operation does not allow stage usage");
    }
    if (stage->is_source && (operation->input_type || !operation->output_type)) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "source operation must have no graph input and one output");
    }
    if (!stage->is_source && !operation->input_type) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "stage operation must declare an input type");
    }
    if (operation->scope.authority == TURBO_FLOW_AUTHORITY_OWNER_COMMAND ||
        operation->domain == TURBO_FLOW_DOMAIN_MANAGEMENT) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "management command operation cannot bind to a payload graph node");
    }
    if (operation->resource_type) {
      if (!stage->resource_name) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "operation requires a resource primitive binding");
      }
      resource = turbo_flow_find_primitive(flow, stage->resource_name);
      if (!resource) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "resource primitive is not registered");
      }
      if (resource->kind != TURBO_FLOW_PRIMITIVE_RESOURCE ||
          resource->domain != operation->resource_domain ||
          strcmp(resource->type_name, operation->resource_type) != 0) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "resource primitive does not satisfy operation contract");
      }
      if (operation->resource_min_version != 0u &&
          (resource->version < operation->resource_min_version ||
           (operation->resource_max_version != 0u &&
            resource->version > operation->resource_max_version))) {
        return flow_set_error(flow, SALTS_EPROTO, stage->line, stage->column,
                              "resource primitive version is incompatible with operation contract");
      }
    } else if (stage->resource_name) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "stateless operation cannot bind a resource primitive");
    }
    if (!stage->is_source) {
      exec_bit = operation_exec_bit(stage->exec.kind);
      if (exec_bit == 0 || !(operation->execution_mask & exec_bit)) {
        return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                              "operation scope does not allow the selected executor");
      }
    }
    if (operation->scope.concurrency == TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT &&
        ((stage->is_source && !stage->adapter_name) ||
         (!stage->is_source &&
          (!adapter || (!adapter->ops.consume && !adapter->async_terminal_ops.submit) ||
           stage->exec.kind != TURBO_FLOW_EXEC_INLINE)))) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            stage->is_source
                                ? "owner-context source operation requires an adapter owner"
                                : "owner-context stage operation requires an inline adapter owner");
    }
    if (!stage->is_source && operation->scope.concurrency == TURBO_FLOW_CONCURRENCY_POOL &&
        stage->data_strategy != TURBO_FLOW_DATA_WORKER_POOL &&
        stage->exec.kind != TURBO_FLOW_EXEC_THREAD_POOL &&
        stage->exec.kind != TURBO_FLOW_EXEC_CORO_POOL) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "pool-scoped operation requires a pooled executor or worker segment");
    }
    if (operation->scope.concurrency == TURBO_FLOW_CONCURRENCY_LOCK_FREE_SNAPSHOT &&
        operation->scope.authority != TURBO_FLOW_AUTHORITY_OBSERVE_ONLY) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "lock-free snapshot scope requires observe-only authority");
    }
    {
      int runtime_rc = compile_validate_operation_runtime(flow, stage, (uint32_t)i, operation);
      if (runtime_rc != SALTS_OK) return runtime_rc;
    }
  }
  return SALTS_OK;
}

static int compile_validate_emitting_operations(turbo_flow_t *flow) {
  const size_t stage_count = vec_size(&flow->stages);

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    const turbo_flow_operation_descriptor_t *operation;
    uint8_t *descendants;
    int changed;

    if (!stage || (!stage->emit_fn && !stage->keyed_emit_fn && !stage->window_fn)) continue;
    operation = flow_stage_operation_descriptor(stage);
    if (!operation) return SALTS_EINVAL;
    if (stage->exec.kind != TURBO_FLOW_EXEC_INLINE ||
        stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL || stage->retry.max_attempts > 1u) {
      return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                            "emitting operation requires direct inline execution without retry");
    }
    if ((operation->scope.authority != TURBO_FLOW_AUTHORITY_PURE &&
         operation->scope.authority != TURBO_FLOW_AUTHORITY_DATA_MUTATION) ||
        operation->runtime.handoff != TURBO_FLOW_HANDOFF_DIRECT ||
        operation->runtime.settlement != 0u || operation->runtime.deadline_ms != 0u ||
        (operation->execution_mask & TURBO_FLOW_OPERATION_EXEC_INLINE) == 0u) {
      return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                            "emitting operation must be an inline direct pure-data contract without settlement");
    }

    descendants = (uint8_t *)calloc(stage_count, sizeof(*descendants));
    if (!descendants) return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    descendants[stage_index] = 1u;
    do {
      changed = 0;
      for (size_t edge_index = 0u; edge_index < vec_size(&flow->edges); ++edge_index) {
        const flow_edge_plan_impl_t *edge =
            (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
        if (edge && descendants[edge->from_stage] && !descendants[edge->to_stage]) {
          descendants[edge->to_stage] = 1u;
          changed = 1;
        }
      }
    } while (changed);

    for (size_t edge_index = 0u; edge_index < vec_size(&flow->edges); ++edge_index) {
      const flow_edge_plan_impl_t *edge =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
      if (edge && edge->to_stage != stage_index && descendants[edge->to_stage] &&
          !descendants[edge->from_stage]) {
        free(descendants);
        return flow_set_error(flow, SALTS_ENOTSUP, edge->line, edge->column,
                              "emitting operation downstream may not merge an external branch");
      }
    }
    free(descendants);
  }
  return SALTS_OK;
}

static int compile_validate_keyed_operations(turbo_flow_t *flow) {
  const size_t stage_count = vec_size(&flow->stages);

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    const turbo_flow_operation_descriptor_t *operation;

    if (!stage || (!stage->keyed_fn && !stage->keyed_emit_fn && !stage->window_fn)) continue;
    operation = flow_stage_operation_descriptor(stage);
    if (!operation) return SALTS_EINVAL;
    if (!stage->key_selector || !stage->keyed_store || stage->fn || stage->emit_fn ||
        ((stage->keyed_fn != NULL) + (stage->keyed_emit_fn != NULL) +
             (stage->window_fn != NULL) !=
         1) ||
        ((stage->window_fn != NULL) != (stage->window_close_fn != NULL)) ||
        stage->exec.kind != TURBO_FLOW_EXEC_INLINE ||
        stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL || stage->retry.max_attempts > 1u) {
      return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                            "keyed operation requires one direct inline keyed provider without retry");
    }
    if ((stage->keyed_fn && stage->mutability != TURBO_FLOW_STAGE_MUTATES_IN_PLACE) ||
        ((stage->keyed_emit_fn || stage->window_fn) &&
         (stage->mutability != TURBO_FLOW_STAGE_READONLY || stage->max_outputs == 0u ||
          stage->max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS ||
          (stage->effects & TURBO_FLOW_STAGE_EFFECT_EMITS) == 0u))) {
      return flow_set_error(flow, SALTS_EINVAL, stage->line, stage->column,
                            "keyed provider mutability or output bound is invalid");
    }
    if (operation->scope.data != TURBO_FLOW_DATA_SCOPE_MESSAGE ||
        operation->scope.state != TURBO_FLOW_STATE_SCOPE_NODE ||
        operation->scope.lifetime != TURBO_FLOW_LIFETIME_RUNTIME_GENERATION ||
        operation->scope.concurrency != TURBO_FLOW_CONCURRENCY_INLINE_LANE ||
        operation->scope.authority != TURBO_FLOW_AUTHORITY_DATA_MUTATION ||
        operation->runtime.handoff != TURBO_FLOW_HANDOFF_DIRECT ||
        operation->runtime.settlement != 0u || operation->runtime.deadline_ms != 0u ||
        (operation->runtime.error_mode != TURBO_FLOW_ERROR_PROPAGATE &&
         operation->runtime.error_mode != TURBO_FLOW_ERROR_REJECT) ||
        (operation->execution_mask & TURBO_FLOW_OPERATION_EXEC_INLINE) == 0u) {
      return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                            "keyed operation contract must own node-local runtime-generation state");
    }
    for (size_t prior_index = 0u; prior_index < stage_index; ++prior_index) {
      const flow_stage_plan_impl_t *prior =
          (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, prior_index);
      if (prior && prior->keyed_store == stage->keyed_store) {
        return flow_set_error(flow, SALTS_EALREADY, stage->line, stage->column,
                              "keyed state store may bind only one runtime node");
      }
    }
  }
  return SALTS_OK;
}

static int operation_types_equal(turbo_flow_domain_t left_domain, const char *left_type,
                                 turbo_flow_domain_t right_domain, const char *right_type) {
  return left_domain == right_domain && left_type && right_type &&
         strcmp(left_type, right_type) == 0;
}

static int compile_validate_operation_edges(turbo_flow_t *flow) {
  size_t i;

  for (i = 0; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);
    const flow_stage_plan_impl_t *from;
    const flow_stage_plan_impl_t *to;
    const turbo_flow_operation_descriptor_t *from_operation;
    const turbo_flow_operation_descriptor_t *to_operation;

    if (!edge || edge->from_stage == UINT32_MAX || edge->to_stage == UINT32_MAX) continue;
    from = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->from_stage);
    to = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);
    if (!from || !to || stage_in_inactive_template(flow, from) ||
        stage_in_inactive_template(flow, to)) {
      continue;
    }
    from_operation = flow_stage_operation_descriptor(from);
    to_operation = flow_stage_operation_descriptor(to);
    if (!from_operation || !to_operation) {
      return flow_set_error(flow, SALTS_EPROTO, edge->line, edge->column,
                            "runtime edge endpoint has no resolved operation contract");
    }
    if (!from_operation->output_type) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "operation without output cannot have a downstream edge");
    }
    if (!to_operation->input_type) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "operation without input cannot have an upstream edge");
    }
    if (!operation_types_equal(from_operation->output_domain, from_operation->output_type,
                               to_operation->input_domain, to_operation->input_type)) {
      return flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "operation edge domain or type is incompatible");
    }
  }
  return SALTS_OK;
}

static int stage_output_is_unordered(const flow_stage_plan_impl_t *stage) {
  return stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL ||
         stage->exec.kind == TURBO_FLOW_EXEC_THREAD_POOL;
}

static int branch_contains_unordered_before_fanin(turbo_flow_t *flow,
                                                  const uint32_t *incoming_counts,
                                                  uint32_t stage_index, uint8_t *visiting) {
  const flow_stage_plan_impl_t *stage =
      (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);

  if (visiting[stage_index]) return 0;
  visiting[stage_index] = 1;

  if (stage->reorder.capacity > 0) return 0;
  if (stage_output_is_unordered(stage)) return 1;
  if (incoming_counts[stage_index] > 1) return 0;

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    if (edge->to_stage != stage_index) continue;
    if (branch_contains_unordered_before_fanin(flow, incoming_counts, edge->from_stage, visiting)) {
      return 1;
    }
  }

  return 0;
}

static int compile_validate_unordered_fanin(turbo_flow_t *flow) {
  size_t stage_count = vec_size(&flow->stages);
  uint32_t *incoming_counts = NULL;
  uint8_t *visiting = NULL;
  int rc = SALTS_OK;

  incoming_counts = (uint32_t *)calloc(stage_count, sizeof(uint32_t));
  visiting = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  if (!incoming_counts || !visiting) {
    rc = flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    goto cleanup;
  }

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    incoming_counts[edge->to_stage] += 1u;
  }

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    if (incoming_counts[edge->to_stage] <= 1u) continue;

    memset(visiting, 0, stage_count * sizeof(uint8_t));
    if (branch_contains_unordered_before_fanin(flow, incoming_counts, edge->from_stage, visiting)) {
      rc = flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                          "unordered worker-pool or thread executor branch cannot fan-in without a "
                          "reorder strategy");
      goto cleanup;
    }
  }

cleanup:
  free(incoming_counts);
  free(visiting);
  return rc;
}

static int fanout_branch_contains_mutable(turbo_flow_t *flow, const uint32_t *incoming_counts,
                                          uint32_t stage_index, uint8_t *visiting) {
  const flow_stage_plan_impl_t *stage =
      (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);

  if (visiting[stage_index]) return 0;
  visiting[stage_index] = 1;

  if (stage->mutability != TURBO_FLOW_STAGE_READONLY) {
    return 1;
  }

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    if (edge->from_stage != stage_index) continue;
    if (incoming_counts[edge->to_stage] > 1) continue;
    if (fanout_branch_contains_mutable(flow, incoming_counts, edge->to_stage, visiting)) return 1;
  }

  return 0;
}

static int compile_validate_fanout_mutability(turbo_flow_t *flow) {
  size_t stage_count = vec_size(&flow->stages);
  uint32_t *incoming_counts = NULL;
  uint8_t *visiting = NULL;
  int rc = SALTS_OK;

  incoming_counts = (uint32_t *)calloc(stage_count, sizeof(uint32_t));
  visiting = (uint8_t *)calloc(stage_count, sizeof(uint8_t));
  if (!incoming_counts || !visiting) {
    rc = flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    goto cleanup;
  }

  for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    incoming_counts[edge->to_stage] += 1;
  }

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    uint32_t outgoing_count = 0;

    for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
      const flow_edge_plan_impl_t *edge =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
      if (edge->from_stage == (uint32_t)stage_index && edge->kind != TURBO_FLOW_EDGE_REJECT) {
        ++outgoing_count;
      }
    }

    if (outgoing_count <= 1) continue;

    for (size_t edge_index = 0; edge_index < vec_size(&flow->edges); ++edge_index) {
      const flow_edge_plan_impl_t *edge =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);

      if (edge->from_stage != (uint32_t)stage_index) continue;
      memset(visiting, 0, stage_count * sizeof(uint8_t));
      if (fanout_branch_contains_mutable(flow, incoming_counts, edge->to_stage, visiting)) {
        rc = flow_set_error(flow, SALTS_EINVAL, edge->line, edge->column,
                            "mutable stage is not allowed on broadcast fan-out");
        goto cleanup;
      }
    }
  }

cleanup:
  free(incoming_counts);
  free(visiting);
  return rc;
}

static int dfs_cycle(const turbo_flow_t *flow, uint8_t *state, uint32_t node) {
  size_t i;

  state[node] = 1;
  for (i = 0; i < vec_size(&flow->edges); ++i) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, i);
    if (edge->from_stage != node) continue;
    if (state[edge->to_stage] == 1) return 1;
    if (state[edge->to_stage] == 0 && dfs_cycle(flow, state, edge->to_stage)) return 1;
  }
  state[node] = 2;
  return 0;
}

static int compile_validate_cycles(turbo_flow_t *flow) {
  size_t count = vec_size(&flow->stages);
  uint8_t *state;
  size_t i;

  if (count == 0) return SALTS_OK;
  state = (uint8_t *)calloc(count, sizeof(uint8_t));
  if (!state) return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");

  for (i = 0; i < count; ++i) {
    if (state[i] == 0 && dfs_cycle(flow, state, (uint32_t)i)) {
      free(state);
      return flow_set_error(flow, SALTS_EINVAL, 0, 0, "stage plan contains a cycle");
    }
  }

  free(state);
  return SALTS_OK;
}

int turbo_flow_compile(turbo_flow_t *flow) {
  int rc;

  if (!flow) return SALTS_EINVAL;
  if (flow->state != TURBO_FLOW_STATE_PARSED && flow->state != TURBO_FLOW_STATE_STOPPED) {
    return flow_set_error(flow, SALTS_EINVAL, 0, 0, "stage plan must be parsed before compile");
  }

  flow_clear_error(flow);
  rc = compile_validate_sources(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_edges(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_reject_edges(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_composite_stage_boundaries(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_composite_stage_reachability(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_duplicate_edges(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_source_reachability(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_cycles(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_registrations(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_resolve_operations(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_resolve_port_types(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_operation_bindings(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_operation_edges(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_emitting_operations(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_keyed_operations(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_unordered_fanin(flow);
  if (rc != SALTS_OK) return rc;
  rc = compile_validate_fanout_mutability(flow);
  if (rc != SALTS_OK) return rc;
  rc = flow_build_runtime_plan(flow);
  if (rc != SALTS_OK) return rc;
  salts_mutex_lock(&flow->runtime_mutex);
  if (flow->runtime_generation == UINT64_MAX) {
    salts_mutex_unlock(&flow->runtime_mutex);
    flow_clear_runtime_plan(flow);
    return flow_set_error(flow, SALTS_ERANGE, 0, 0, "runtime generation is exhausted");
  }
  ++flow->runtime_generation;
  flow->state = TURBO_FLOW_STATE_COMPILED;
  salts_mutex_unlock(&flow->runtime_mutex);
  return SALTS_OK;
}
