#include "flow_internal.h"

#include <string.h>

int flow_runtime_stage_index_reset(vec_t *index_by_stage, size_t stage_count) {
  int rc;
  if (!index_by_stage) return SALTS_EINVAL;
  rc = turbo_flow_stl_error(vec_resize(index_by_stage, stage_count));
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0u; i < stage_count; ++i) {
    *(uint32_t *)vec_at(index_by_stage, i) = FLOW_PLAN_INDEX_NONE;
  }
  return SALTS_OK;
}

int flow_compiled_plan_init(flow_compiled_plan_t *plan) {
  if (!plan) return SALTS_EINVAL;
  memset(plan, 0, sizeof(*plan));
  if (turbo_flow_stl_error(vec_init_bytes(&plan->nodes, sizeof(flow_runtime_node_plan_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->edges, sizeof(flow_runtime_edge_plan_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->data_segments, sizeof(flow_data_segment_plan_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->executors, sizeof(flow_executor_plan_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->executor_by_stage, sizeof(uint32_t),
                                          _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->adapter_by_stage, sizeof(uint32_t),
                                          _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->data_segment_by_stage, sizeof(uint32_t),
                                          _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&plan->semantic_types, sizeof(flow_semantic_type_plan_t),
                                          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) !=
          SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&plan->stage_semantics, sizeof(flow_stage_semantic_plan_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&plan->cflow_regions, sizeof(flow_cflow_region_plan_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&plan->cflow_region_by_stage, sizeof(uint32_t),
                         _Alignof(uint32_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(
          vec_init_bytes(&plan->owned_resources, sizeof(flow_plan_owned_resource_t),
                         _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    flow_compiled_plan_destroy(plan);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static void flow_plan_owned_resource_vector_release(vec_t *resources) {
  if (!resources) return;
  for (size_t i = vec_size(resources); i > 0u; --i) {
    flow_plan_owned_resource_t *resource =
        (flow_plan_owned_resource_t *)vec_at(resources, i - 1u);
    if (resource && resource->release) {
      resource->release(resource->ctx);
      resource->ctx = NULL;
      resource->release = NULL;
    }
  }
  (void)turbo_flow_stl_error(vec_clear(resources));
}

void flow_compiled_plan_destroy(flow_compiled_plan_t *plan) {
  if (!plan) return;
  flow_plan_owned_resource_vector_release(&plan->owned_resources);
  for (size_t i = 0u; i < vec_size(&plan->cflow_regions); ++i) {
    flow_cflow_region_plan_t *region =
        (flow_cflow_region_plan_t *)vec_at(&plan->cflow_regions, i);
    if (region) cflow_plan_destroy(&region->plan);
  }
  for (size_t i = 0u; i < vec_size(&plan->semantic_types); ++i) {
    flow_semantic_type_plan_t *type = (flow_semantic_type_plan_t *)vec_at(&plan->semantic_types, i);
    if (type) tstr_freep(&type->stable_id);
  }
  vec_destroy(&plan->nodes);
  vec_destroy(&plan->edges);
  vec_destroy(&plan->data_segments);
  vec_destroy(&plan->executors);
  vec_destroy(&plan->executor_by_stage);
  vec_destroy(&plan->adapter_by_stage);
  vec_destroy(&plan->data_segment_by_stage);
  vec_destroy(&plan->semantic_types);
  vec_destroy(&plan->stage_semantics);
  vec_destroy(&plan->cflow_regions);
  vec_destroy(&plan->cflow_region_by_stage);
  vec_destroy(&plan->owned_resources);
  memset(plan, 0, sizeof(*plan));
}

int flow_plan_owned_resources_reserve(turbo_flow_t *flow, size_t additional) {
  size_t required;
  if (!flow || flow->compiled_plan.sealed) return SALTS_EINVAL;
  if (additional > SIZE_MAX - vec_size(&flow->pending_plan_resources))
    return SALTS_ERANGE;
  required = vec_size(&flow->pending_plan_resources) + additional;
  return turbo_flow_stl_error(vec_reserve(&flow->pending_plan_resources, required));
}

int flow_plan_owned_resource_stage(
    turbo_flow_t *flow, void *ctx,
    flow_plan_owned_resource_release_fn release) {
  flow_plan_owned_resource_t resource;
  if (!flow || !ctx || !release || flow->compiled_plan.sealed)
    return SALTS_EINVAL;
  resource.ctx = ctx;
  resource.release = release;
  return turbo_flow_stl_error(vec_push(&flow->pending_plan_resources, &resource));
}

int flow_plan_owned_resource_unstage_last(turbo_flow_t *flow, void *ctx) {
  flow_plan_owned_resource_t resource = {0};
  flow_plan_owned_resource_t *last;
  const size_t count = flow ? vec_size(&flow->pending_plan_resources) : 0u;
  int rc;
  if (!flow || !ctx || flow->compiled_plan.sealed || count == 0u)
    return SALTS_EINVAL;
  last = (flow_plan_owned_resource_t *)vec_at(
      &flow->pending_plan_resources, count - 1u);
  if (!last || last->ctx != ctx || !last->release) return SALTS_EPROTO;
  rc = turbo_flow_stl_error(vec_pop(&flow->pending_plan_resources, &resource));
  if (rc != SALTS_OK) return rc;
  resource.release(resource.ctx);
  return SALTS_OK;
}

void flow_plan_owned_resources_clear_pending(turbo_flow_t *flow) {
  if (!flow) return;
  flow_plan_owned_resource_vector_release(&flow->pending_plan_resources);
}

static int flow_plan_owned_resources_transfer(
    turbo_flow_t *flow, flow_compiled_plan_t *candidate) {
  vec_t replacement = {0};
  int rc;
  if (!flow || !candidate || candidate->sealed ||
      !vec_empty(&candidate->owned_resources))
    return SALTS_EINVAL;
  rc = turbo_flow_stl_error(
      vec_init_bytes(&replacement, sizeof(flow_plan_owned_resource_t),
                     _Alignof(turbo_flow_max_align_t), SIZE_MAX));
  if (rc != SALTS_OK) return rc;

  /*
   * vec_t is the owning storage handle. Move it only after all fallible plan
   * construction/validation work has succeeded and a fresh pending owner is
   * ready, so either the mutable flow or the sealed plan owns every resource.
   */
  vec_destroy(&candidate->owned_resources);
  candidate->owned_resources = flow->pending_plan_resources;
  flow->pending_plan_resources = replacement;
  return SALTS_OK;
}

void flow_clear_runtime_plan(turbo_flow_t *flow) {
  if (!flow) return;
  flow_compiled_plan_destroy(&flow->compiled_plan);
  (void)flow_compiled_plan_init(&flow->compiled_plan);
  (void)turbo_flow_stl_error(vec_clear(&flow->runtime_stage_configs));
}

const flow_runtime_stage_config_t *flow_runtime_stage_config_for_stage(const turbo_flow_t *flow,
                                                                       uint32_t stage_index) {
  if (!flow || !flow->compiled_plan.sealed) return NULL;
  return (const flow_runtime_stage_config_t *)vec_at_const(&flow->runtime_stage_configs,
                                                           stage_index);
}

flow_runtime_stage_config_t *flow_runtime_stage_config_for_stage_mut(turbo_flow_t *flow,
                                                                     uint32_t stage_index) {
  if (!flow || !flow->compiled_plan.sealed) return NULL;
  return (flow_runtime_stage_config_t *)vec_at(&flow->runtime_stage_configs, stage_index);
}

static int flow_plan_fail(turbo_flow_t *flow, flow_compiled_plan_t *candidate, int code,
                          const char *message) {
  flow_compiled_plan_destroy(candidate);
  return flow_set_error(flow, code, 0, 0, message);
}

static int flow_push_data_segment(flow_compiled_plan_t *plan, flow_data_segment_kind_t kind,
                                  uint32_t stage_index, uint32_t edge_index, uint32_t width,
                                  uint32_t capacity,
                                  const turbo_flow_operation_runtime_contract_t *operation,
                                  uint32_t *index_out) {
  flow_data_segment_plan_t segment;
  size_t index;
  int rc;

  if (!plan || plan->sealed) return SALTS_EINVAL;
  index = vec_size(&plan->data_segments);
  if (index > UINT32_MAX) return SALTS_ERANGE;
  memset(&segment, 0, sizeof(segment));
  segment.kind = kind;
  segment.stage_index = stage_index;
  segment.edge_index = edge_index;
  segment.width = width;
  segment.capacity = capacity;
  if (operation) segment.operation = *operation;
  rc = turbo_flow_stl_error(vec_push(&plan->data_segments, &segment));
  if (rc == SALTS_OK && index_out) *index_out = (uint32_t)index;
  return rc;
}

static const turbo_flow_operation_runtime_contract_t *
flow_direct_operation_contract(const turbo_flow_t *flow, const flow_stage_plan_impl_t *stage) {
  const turbo_flow_operation_runtime_contract_t *runtime =
      flow_stage_operation_runtime(flow, stage);
  return runtime && runtime->handoff == TURBO_FLOW_HANDOFF_DIRECT ? runtime : NULL;
}

static int flow_verify_compiled_plan(const flow_compiled_plan_t *plan, size_t stage_count,
                                     size_t adapter_count) {
  if (!plan || plan->sealed || vec_size(&plan->nodes) != stage_count ||
      vec_size(&plan->executor_by_stage) != stage_count ||
      vec_size(&plan->adapter_by_stage) != stage_count ||
      vec_size(&plan->data_segment_by_stage) != stage_count ||
      vec_size(&plan->stage_semantics) != stage_count ||
      vec_size(&plan->cflow_region_by_stage) != stage_count || !plan->message_type ||
      !plan->operation_type || !cmeta_type_desc_valid(plan->message_type) ||
      !cmeta_type_desc_valid(plan->operation_type)) {
    return SALTS_EPROTO;
  }
  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_runtime_node_plan_t *node =
        (const flow_runtime_node_plan_t *)vec_at_const(&plan->nodes, stage_index);
    const uint32_t *executor_index =
        (const uint32_t *)vec_at_const(&plan->executor_by_stage, stage_index);
    const uint32_t *adapter_index =
        (const uint32_t *)vec_at_const(&plan->adapter_by_stage, stage_index);
    const uint32_t *segment_index =
        (const uint32_t *)vec_at_const(&plan->data_segment_by_stage, stage_index);
    const flow_stage_semantic_plan_t *semantics =
        (const flow_stage_semantic_plan_t *)vec_at_const(&plan->stage_semantics, stage_index);
    const uint32_t *cflow_region_index =
        (const uint32_t *)vec_at_const(&plan->cflow_region_by_stage, stage_index);
    const flow_executor_plan_t *executor =
        executor_index && *executor_index != FLOW_PLAN_INDEX_NONE
            ? (const flow_executor_plan_t *)vec_at_const(&plan->executors, *executor_index)
            : NULL;
    const flow_data_segment_plan_t *segment =
        segment_index && *segment_index != FLOW_PLAN_INDEX_NONE
            ? (const flow_data_segment_plan_t *)vec_at_const(&plan->data_segments, *segment_index)
            : NULL;
    if (!node || !executor_index || !adapter_index || !segment_index || !cflow_region_index ||
        node->stage_index != stage_index ||
        node->outgoing_begin > vec_size(&plan->edges) ||
        node->outgoing_count > vec_size(&plan->edges) - node->outgoing_begin ||
        (*executor_index != FLOW_PLAN_INDEX_NONE &&
         *executor_index >= vec_size(&plan->executors)) ||
        (*adapter_index != FLOW_PLAN_INDEX_NONE && *adapter_index >= adapter_count) ||
        (*segment_index != FLOW_PLAN_INDEX_NONE &&
         *segment_index >= vec_size(&plan->data_segments)) ||
        (executor && executor->stage_index != stage_index) ||
        (segment &&
         (segment->stage_index != stage_index || segment->kind != FLOW_DATA_SEGMENT_WORKER_POOL)) ||
        !semantics ||
        (((node->flags & (FLOW_RUNTIME_NODE_SOURCE | FLOW_RUNTIME_NODE_PORT |
                          FLOW_RUNTIME_NODE_BUFFER)) != 0u) !=
         (*executor_index == FLOW_PLAN_INDEX_NONE)) ||
        (((node->flags & FLOW_RUNTIME_NODE_WORKER_POOL) != 0u) !=
         (*segment_index != FLOW_PLAN_INDEX_NONE)) ||
        (semantics->input_type_index != FLOW_PLAN_INDEX_NONE &&
         semantics->input_type_index >= vec_size(&plan->semantic_types)) ||
        (semantics->output_type_index != FLOW_PLAN_INDEX_NONE &&
         semantics->output_type_index >= vec_size(&plan->semantic_types)) ||
        (semantics->reflected &&
         (semantics->input_type_index != FLOW_PLAN_INDEX_NONE ||
          semantics->output_type_index != FLOW_PLAN_INDEX_NONE ||
          (semantics->canonical_input_type &&
           !cmeta_type_desc_valid(semantics->canonical_input_type)) ||
          (semantics->canonical_output_type &&
           !cmeta_type_desc_valid(semantics->canonical_output_type)) ||
          (!!semantics->typed !=
           (semantics->canonical_input_type != NULL &&
            semantics->canonical_output_type != NULL)) ||
          (semantics->typed && !semantics->reflected_typed_adapter &&
           !cmeta_callable_contract_valid(semantics->callable)) ||
          (semantics->reflected_typed_adapter &&
           !cflow_function_typed_adapter_projection_valid(
               &semantics->typed_adapter_projection)))) ||
        (!semantics->reflected &&
         (semantics->reflected_typed_adapter ||
          semantics->canonical_input_type != NULL ||
          semantics->canonical_output_type != NULL ||
          (!!semantics->typed !=
           (semantics->input_type_index != FLOW_PLAN_INDEX_NONE &&
            semantics->output_type_index != FLOW_PLAN_INDEX_NONE)))) ||
        (semantics->candidate_region != FLOW_PLAN_INDEX_NONE &&
         semantics->candidate_region >= plan->candidate_region_count) ||
        (!!semantics->lowering_candidate !=
         (semantics->candidate_region != FLOW_PLAN_INDEX_NONE)) ||
        (*cflow_region_index != FLOW_PLAN_INDEX_NONE &&
         *cflow_region_index >= vec_size(&plan->cflow_regions))) {
      return SALTS_EPROTO;
    }
    for (size_t offset = 0u; offset < node->outgoing_count; ++offset) {
      const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
          &plan->edges, node->outgoing_begin + offset);
      if (!edge || edge->from_stage != stage_index || edge->to_stage >= stage_count) {
        return SALTS_EPROTO;
      }
    }
  }
  for (size_t region_index = 0u; region_index < vec_size(&plan->cflow_regions);
       ++region_index) {
    const flow_cflow_region_plan_t *region =
        (const flow_cflow_region_plan_t *)vec_at_const(&plan->cflow_regions, region_index);
    if (!region || !region->plan.impl || region->stage_count == 0u ||
        region->entry_stage >= stage_count || region->exit_stage >= stage_count ||
        region->candidate_region >= plan->candidate_region_count ||
        region->backend != FLOW_CFLOW_REGION_BACKEND_DIRECT ||
        !region->input_data || !region->output_data ||
        !cmeta_data_desc_valid(region->input_data) ||
        !cmeta_data_desc_valid(region->output_data) ||
        !cmeta_data_desc_equal(region->input_data, region->output_data) ||
        !region->plan.input_type || !region->plan.output_type ||
        !cmeta_type_desc_valid(region->plan.input_type) ||
        !cmeta_type_desc_valid(region->plan.output_type) ||
        !cmeta_type_equal(region->input_data->storage_type, region->plan.input_type) ||
        !cmeta_type_equal(region->output_data->storage_type, region->plan.output_type)) {
      return SALTS_EPROTO;
    }
  }
  for (size_t type_index = 0u; type_index < vec_size(&plan->semantic_types); ++type_index) {
    const flow_semantic_type_plan_t *type =
        (const flow_semantic_type_plan_t *)vec_at_const(&plan->semantic_types, type_index);
    if (!type || !type->stable_id || !cmeta_type_desc_valid(&type->descriptor) ||
        type->descriptor.identity != &type->identity) {
      return SALTS_EPROTO;
    }
  }
  return SALTS_OK;
}

static int flow_verify_execution_backends(
    turbo_flow_t *flow, const flow_compiled_plan_t *candidate) {
  const size_t stage_count = candidate ? vec_size(&candidate->nodes) : 0u;

  if (!flow || !candidate) return SALTS_EINVAL;
  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_runtime_node_plan_t *node =
        (const flow_runtime_node_plan_t *)vec_at_const(&candidate->nodes, stage_index);
    const uint32_t *executor_index =
        (const uint32_t *)vec_at_const(&candidate->executor_by_stage, stage_index);
    const uint32_t *adapter_index =
        (const uint32_t *)vec_at_const(&candidate->adapter_by_stage, stage_index);
    const uint32_t *region_index =
        (const uint32_t *)vec_at_const(&candidate->cflow_region_by_stage, stage_index);
    const flow_executor_plan_t *executor = NULL;
    const flow_adapter_registration_t *adapter = NULL;
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);

    if (!node || !executor_index || !adapter_index || !region_index || !stage)
      return SALTS_EPROTO;
    /*
     * Composite template declarations remain in the parsed stage vector so
     * source locations and type propagation stay stable, but inactive
     * templates are intentionally not materialized or provider-bound.
     * Only instantiated stages have a resolved runtime operation contract.
     */
    if (!stage->operation_resolved) continue;
    if ((node->flags & (FLOW_RUNTIME_NODE_SOURCE | FLOW_RUNTIME_NODE_PORT |
                        FLOW_RUNTIME_NODE_BUFFER)) != 0u)
      continue;
    if (*region_index != FLOW_PLAN_INDEX_NONE) continue;

    if (*executor_index != FLOW_PLAN_INDEX_NONE)
      executor = (const flow_executor_plan_t *)vec_at_const(
          &candidate->executors, *executor_index);
    if (*adapter_index != FLOW_PLAN_INDEX_NONE)
      adapter = (const flow_adapter_registration_t *)vec_at_const(
          &flow->adapters, *adapter_index);

    if ((executor &&
         (executor->fn || executor->emit_fn || executor->keyed_fn ||
          executor->keyed_emit_fn || executor->window_fn)) ||
        (adapter &&
         (adapter->ops.consume || adapter->async_terminal_ops.submit ||
          adapter->async_emit_ops.submit))) {
      continue;
    }

    return flow_set_error_keep_state(
        flow, SALTS_ENOTSUP, stage->line, stage->column,
        "stage has neither a compiled CFlow region nor a native execution backend");
  }
  return SALTS_OK;
}

static int flow_require_cflow_backend(turbo_flow_t *flow, const flow_compiled_plan_t *candidate) {
  const size_t stage_count = vec_size(&candidate->nodes);

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_runtime_node_plan_t *node =
        (const flow_runtime_node_plan_t *)vec_at_const(&candidate->nodes, stage_index);
    const flow_stage_semantic_plan_t *semantics =
        (const flow_stage_semantic_plan_t *)vec_at_const(&candidate->stage_semantics, stage_index);
    const uint32_t *region_index =
        (const uint32_t *)vec_at_const(&candidate->cflow_region_by_stage, stage_index);
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    const flow_cflow_region_plan_t *region = NULL;

    if (!node || !semantics || !stage || !region_index) {
      return flow_set_error(flow, SALTS_EPROTO, 0, 0,
                            "required CFlow backend received an invalid candidate plan");
    }
    /* Inactive composite templates are declarations, not runtime backend obligations. */
    if (!stage->operation_resolved) continue;
    if ((node->flags & (FLOW_RUNTIME_NODE_SOURCE | FLOW_RUNTIME_NODE_PORT |
                        FLOW_RUNTIME_NODE_BUFFER)) != 0u) {
      continue;
    }
    if (*region_index != FLOW_PLAN_INDEX_NONE &&
        *region_index < vec_size(&candidate->cflow_regions)) {
      region = (const flow_cflow_region_plan_t *)vec_at_const(
          &candidate->cflow_regions, *region_index);
    }
    if (!semantics->typed || semantics->barriers != FLOW_LOWERING_BARRIER_NONE ||
        !region || region->backend != FLOW_CFLOW_REGION_BACKEND_DIRECT) {
      return flow_set_error(flow, SALTS_ENOTSUP, stage->line, stage->column,
                            "required CFlow backend cannot lower this stage");
    }
  }
  return SALTS_OK;
}

int flow_build_runtime_plan(turbo_flow_t *flow) {
  flow_compiled_plan_t candidate;
  size_t stage_count;
  size_t edge_count;
  uint32_t *write_offsets = NULL;
  int rc;

  if (!flow || (flow->required_backend != FLOW_PLAN_BACKEND_NATIVE &&
                flow->required_backend != FLOW_PLAN_BACKEND_CFLOW)) {
    return SALTS_EINVAL;
  }
  stage_count = vec_size(&flow->stages);
  edge_count = vec_size(&flow->edges);
  if (stage_count > UINT32_MAX || edge_count > UINT32_MAX) {
    return flow_set_error(flow, SALTS_ERANGE, 0, 0, "runtime graph exceeds plan index range");
  }
  rc = flow_compiled_plan_init(&candidate);
  if (rc != SALTS_OK) return flow_set_error(flow, rc, 0, 0, "out of memory");

  rc = turbo_flow_stl_error(vec_resize(&candidate.nodes, stage_count));
  if (rc == SALTS_OK) {
    rc = turbo_flow_stl_error(vec_resize(&candidate.executor_by_stage, stage_count));
  }
  if (rc == SALTS_OK) {
    rc = turbo_flow_stl_error(vec_resize(&candidate.adapter_by_stage, stage_count));
  }
  if (rc == SALTS_OK) {
    rc = turbo_flow_stl_error(vec_resize(&candidate.data_segment_by_stage, stage_count));
  }
  if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
  memset(vec_data(&candidate.nodes), 0, stage_count * sizeof(flow_runtime_node_plan_t));
  for (size_t i = 0u; i < stage_count; ++i) {
    *(uint32_t *)vec_at(&candidate.executor_by_stage, i) = FLOW_PLAN_INDEX_NONE;
    *(uint32_t *)vec_at(&candidate.adapter_by_stage, i) = FLOW_PLAN_INDEX_NONE;
    *(uint32_t *)vec_at(&candidate.data_segment_by_stage, i) = FLOW_PLAN_INDEX_NONE;
  }

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    flow_runtime_node_plan_t *node =
        (flow_runtime_node_plan_t *)vec_at(&candidate.nodes, stage_index);

    node->stage_index = (uint32_t)stage_index;
    if (stage->adapter_name) {
      int adapter_index = flow_find_adapter(flow, stage->adapter_name);
      if (adapter_index < 0) {
        return flow_plan_fail(flow, &candidate, SALTS_EPROTO,
                              "compiled adapter binding is inconsistent");
      }
      *(uint32_t *)vec_at(&candidate.adapter_by_stage, stage_index) =
          (uint32_t)adapter_index;
    }
    if (stage->is_source) node->flags |= FLOW_RUNTIME_NODE_SOURCE;
    if (stage->is_port) node->flags |= FLOW_RUNTIME_NODE_PORT;
    if (stage->is_buffer) node->flags |= FLOW_RUNTIME_NODE_BUFFER;
    if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
      node->flags |= FLOW_RUNTIME_NODE_WORKER_POOL;
    }
    if (!stage->is_source && !stage->is_port && !stage->is_buffer) {
      flow_executor_plan_t executor;
      const size_t executor_index = vec_size(&candidate.executors);

      if (executor_index > UINT32_MAX) {
        return flow_plan_fail(flow, &candidate, SALTS_ERANGE,
                              "runtime graph has too many executors");
      }
      memset(&executor, 0, sizeof(executor));
      executor.stage_index = (uint32_t)stage_index;
      executor.exec = stage->exec;
      executor.fn = stage->fn;
      executor.emit_fn = stage->emit_fn;
      executor.key_selector = stage->key_selector;
      executor.key_ctx = stage->key_ctx;
      executor.keyed_fn = stage->keyed_fn;
      executor.keyed_emit_fn = stage->keyed_emit_fn;
      executor.window_fn = stage->window_fn;
      executor.window_close_fn = stage->window_close_fn;
      executor.keyed_store = stage->keyed_store;
      executor.max_outputs = stage->max_outputs;
      executor.ctx = stage->ctx;
      rc = turbo_flow_stl_error(vec_push(&candidate.executors, &executor));
      if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
      *(uint32_t *)vec_at(&candidate.executor_by_stage, stage_index) = (uint32_t)executor_index;
    }
  }

  for (size_t edge_index = 0; edge_index < edge_count; ++edge_index) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
    flow_runtime_node_plan_t *from_node =
        (flow_runtime_node_plan_t *)vec_at(&candidate.nodes, edge->from_stage);
    flow_runtime_node_plan_t *to_node =
        (flow_runtime_node_plan_t *)vec_at(&candidate.nodes, edge->to_stage);
    const flow_stage_plan_impl_t *to =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, edge->to_stage);

    rc = flow_push_data_segment(&candidate, FLOW_DATA_SEGMENT_DIRECT, edge->from_stage,
                                (uint32_t)edge_index, 1u, 0u,
                                flow_direct_operation_contract(flow, to), NULL);
    if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
    from_node->outgoing_count += 1u;
    to_node->incoming_count += 1u;
  }

  {
    uint32_t outgoing_begin = 0u;
    for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
      flow_runtime_node_plan_t *node =
          (flow_runtime_node_plan_t *)vec_at(&candidate.nodes, stage_index);
      node->outgoing_begin = outgoing_begin;
      outgoing_begin += node->outgoing_count;
    }
    rc = turbo_flow_stl_error(vec_resize(&candidate.edges, edge_count));
    if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
    write_offsets = stage_count > 0u ? (uint32_t *)calloc(stage_count, sizeof(*write_offsets)) : NULL;
    if (stage_count > 0u && !write_offsets) {
      return flow_plan_fail(flow, &candidate, SALTS_ENOMEM, "out of memory");
    }
    for (size_t edge_index = 0; edge_index < edge_count; ++edge_index) {
      const flow_edge_plan_impl_t *edge =
          (const flow_edge_plan_impl_t *)vec_at_const(&flow->edges, edge_index);
      const flow_runtime_node_plan_t *node =
          (const flow_runtime_node_plan_t *)vec_at_const(&candidate.nodes, edge->from_stage);
      flow_runtime_edge_plan_t *runtime_edge = (flow_runtime_edge_plan_t *)vec_at(
          &candidate.edges, node->outgoing_begin + write_offsets[edge->from_stage]++);

      memset(runtime_edge, 0, sizeof(*runtime_edge));
      runtime_edge->from_stage = edge->from_stage;
      runtime_edge->to_stage = edge->to_stage;
      runtime_edge->line = edge->line;
      runtime_edge->column = edge->column;
      runtime_edge->is_stage_internal = edge->is_stage_internal;
      runtime_edge->kind = edge->kind;
      runtime_edge->name = edge->name;
      runtime_edge->predicate = edge->predicate;
    }
    free(write_offsets);
    write_offsets = NULL;
  }

  for (size_t stage_index = 0; stage_index < stage_count; ++stage_index) {
    flow_runtime_node_plan_t *node =
        (flow_runtime_node_plan_t *)vec_at(&candidate.nodes, stage_index);
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    if (stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL) {
      uint32_t segment_index = FLOW_PLAN_INDEX_NONE;
      rc = flow_push_data_segment(&candidate, FLOW_DATA_SEGMENT_WORKER_POOL, (uint32_t)stage_index,
                                  UINT32_MAX, stage->data_worker_count, stage->data_pool_capacity,
                                  flow_stage_operation_runtime(flow, stage), &segment_index);
      if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
      *(uint32_t *)vec_at(&candidate.data_segment_by_stage, stage_index) = segment_index;
    }
    if (node->outgoing_count > 1u) {
      node->flags |= FLOW_RUNTIME_NODE_FANOUT;
      rc = flow_push_data_segment(&candidate, FLOW_DATA_SEGMENT_BROADCAST_FANOUT,
                                  (uint32_t)stage_index, UINT32_MAX, node->outgoing_count, 0u, NULL,
                                  NULL);
      if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
    }
    if (node->incoming_count > 1u) {
      node->flags |= FLOW_RUNTIME_NODE_FANIN;
      rc = flow_push_data_segment(&candidate, FLOW_DATA_SEGMENT_FANIN_GATE, (uint32_t)stage_index,
                                  UINT32_MAX, node->incoming_count, 0u, NULL, NULL);
      if (rc != SALTS_OK) return flow_plan_fail(flow, &candidate, rc, "out of memory");
    }
  }

  rc = flow_plan_build_semantics(flow, &candidate);
  if (rc != SALTS_OK) {
    return flow_plan_fail(flow, &candidate, rc,
                          rc == SALTS_ENOMEM ? "out of memory"
                                             : "compiled semantic plan is inconsistent");
  }
  rc = flow_plan_build_cflow_regions(flow, &candidate);
  if (rc != SALTS_OK) {
    return flow_plan_fail(flow, &candidate, rc,
                          rc == SALTS_ENOMEM ? "out of memory"
                                             : "CFlow execution-region compilation failed");
  }
  rc = flow_verify_compiled_plan(&candidate, stage_count, vec_size(&flow->adapters));
  if (rc != SALTS_OK) {
    return flow_plan_fail(flow, &candidate, rc, "compiled runtime plan is inconsistent");
  }
  rc = flow_verify_execution_backends(flow, &candidate);
  if (rc != SALTS_OK) {
    flow_compiled_plan_destroy(&candidate);
    return rc;
  }
  if (flow->required_backend == FLOW_PLAN_BACKEND_CFLOW) {
    rc = flow_require_cflow_backend(flow, &candidate);
    if (rc != SALTS_OK) {
      flow_compiled_plan_destroy(&candidate);
      return rc;
    }
  }
  rc = turbo_flow_stl_error(vec_resize(&flow->runtime_stage_configs, stage_count));
  if (rc != SALTS_OK) {
    return flow_plan_fail(flow, &candidate, rc, "out of memory");
  }
  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    flow_runtime_stage_config_t *runtime_config =
        (flow_runtime_stage_config_t *)vec_at(&flow->runtime_stage_configs, stage_index);
    runtime_config->data_workers = stage->data_worker_count;
    runtime_config->thread_workers = stage->exec.workers;
    runtime_config->coro_lanes = stage->exec.lanes;
  }
  rc = flow_plan_owned_resources_transfer(flow, &candidate);
  if (rc != SALTS_OK) {
    return flow_plan_fail(flow, &candidate, rc, "out of memory");
  }
  candidate.sealed = 1;
  flow_compiled_plan_destroy(&flow->compiled_plan);
  flow->compiled_plan = candidate;
  return SALTS_OK;
}

size_t turbo_flow_segment_count(const turbo_flow_t *flow) {
  return flow && flow->compiled_plan.sealed &&
                 (flow->state == TURBO_FLOW_STATE_COMPILED ||
                  flow->state == TURBO_FLOW_STATE_STARTED ||
                  flow->state == TURBO_FLOW_STATE_STOPPED)
             ? vec_size(&flow->compiled_plan.data_segments)
             : 0u;
}

int turbo_flow_segment_plan_at(const turbo_flow_t *flow, size_t index,
                               turbo_flow_segment_plan_t *out) {
  const flow_data_segment_plan_t *segment;
  if (!flow || !out || !flow->compiled_plan.sealed ||
      (flow->state != TURBO_FLOW_STATE_COMPILED && flow->state != TURBO_FLOW_STATE_STARTED &&
       flow->state != TURBO_FLOW_STATE_STOPPED)) {
    return SALTS_EINVAL;
  }
  segment =
      (const flow_data_segment_plan_t *)vec_at_const(&flow->compiled_plan.data_segments, index);
  if (!segment) return SALTS_ENOENT;
  memset(out, 0, sizeof(*out));
  out->kind = (turbo_flow_segment_kind_t)segment->kind;
  out->stage_index = segment->stage_index;
  out->edge_index = segment->edge_index;
  out->width = segment->width;
  out->capacity = segment->capacity;
  out->operation = segment->operation;
  return SALTS_OK;
}

const flow_executor_plan_t *flow_executor_plan_for_stage(const turbo_flow_t *flow,
                                                         uint32_t stage_index) {
  const uint32_t *executor_index;
  if (!flow || !flow->compiled_plan.sealed) return NULL;
  executor_index =
      (const uint32_t *)vec_at_const(&flow->compiled_plan.executor_by_stage, stage_index);
  if (!executor_index || *executor_index == FLOW_PLAN_INDEX_NONE) return NULL;
  return (const flow_executor_plan_t *)vec_at_const(&flow->compiled_plan.executors,
                                                    *executor_index);
}

const flow_data_segment_plan_t *flow_worker_pool_segment_for_stage(const turbo_flow_t *flow,
                                                                   uint32_t stage_index) {
  const uint32_t *segment_index;
  if (!flow || !flow->compiled_plan.sealed) return NULL;
  segment_index =
      (const uint32_t *)vec_at_const(&flow->compiled_plan.data_segment_by_stage, stage_index);
  if (!segment_index || *segment_index == FLOW_PLAN_INDEX_NONE) return NULL;
  return (const flow_data_segment_plan_t *)vec_at_const(&flow->compiled_plan.data_segments,
                                                        *segment_index);
}
