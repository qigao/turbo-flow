#include "flow_internal.h"

#include <string.h>

static const cmeta_type_identity FLOW_MESSAGE_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turbo.flow.Message");
static bool flow_message_copy(void *destination, const void *source) {
  return turbo_flow_msg_clone((turbo_flow_msg_t *)destination,
                              (const turbo_flow_msg_t *)source) == SALTS_OK;
}

static void flow_message_move(void *destination, void *source) {
  (void)turbo_flow_msg_move((turbo_flow_msg_t *)destination, (turbo_flow_msg_t *)source);
}

static void flow_message_destroy(void *value) {
  turbo_flow_msg_cleanup((turbo_flow_msg_t *)value);
}

static const cmeta_type_traits FLOW_MESSAGE_TRAITS = {
    .flags = CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY,
    .copy_construct = flow_message_copy,
    .move_construct = flow_message_move,
    .destroy = flow_message_destroy};
static const cmeta_type_desc FLOW_MESSAGE_TYPE = {
    .name = "turbo_flow_msg_t",
    .size = sizeof(turbo_flow_msg_t),
    .align = _Alignof(turbo_flow_msg_t),
    .kind = CMETA_T_OBJECT,
    .traits = &FLOW_MESSAGE_TRAITS,
    .identity = &FLOW_MESSAGE_IDENTITY};

const cmeta_type_desc *flow_message_type_descriptor(void) { return &FLOW_MESSAGE_TYPE; }

const cmeta_type_desc *turbo_flow_message_type(void) { return flow_message_type_descriptor(); }

static const cmeta_type_identity FLOW_OPERATION_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turbo.flow.Operation");
static const cmeta_type_desc FLOW_OPERATION_TYPE = {
    .name = "turbo_flow_operation_descriptor_t",
    .size = sizeof(turbo_flow_operation_descriptor_t),
    .align = _Alignof(turbo_flow_operation_descriptor_t),
    .kind = CMETA_T_OBJECT,
    .identity = &FLOW_OPERATION_IDENTITY};

static int flow_semantic_type_add(flow_compiled_plan_t *plan, turbo_flow_domain_t domain,
                                  const char *name, uint32_t *index_out) {
  flow_semantic_type_plan_t type;
  tstr stable_id;
  size_t index;
  int rc;
  tstr formatted;

  if (!plan || !name || !index_out || plan->sealed) return SALTS_EINVAL;
  index = vec_size(&plan->semantic_types);
  if (index > UINT32_MAX) return SALTS_ERANGE;
  stable_id = tstr_new();
  if (!stable_id) return SALTS_ENOMEM;
  formatted = tstr_cat_fmt(stable_id, "turbo.flow.type.%u.%s", (unsigned)domain, name);
  if (!formatted) {
    tstr_free(stable_id);
    return SALTS_ENOMEM;
  }
  stable_id = formatted;
  memset(&type, 0, sizeof(type));
  type.stable_id = stable_id;
  type.identity = (cmeta_type_identity)CMETA_TYPE_ID_ATOM_INIT(NULL);
  type.descriptor.name = stable_id;
  type.descriptor.size = sizeof(turbo_flow_msg_t);
  type.descriptor.align = _Alignof(turbo_flow_msg_t);
  type.descriptor.kind = CMETA_T_OBJECT;
  rc = turbo_flow_stl_error(vec_push(&plan->semantic_types, &type));
  if (rc != SALTS_OK) {
    tstr_free(stable_id);
    return rc;
  }
  *index_out = (uint32_t)index;
  return SALTS_OK;
}

static uint32_t flow_stage_barriers(const flow_stage_plan_impl_t *stage,
                                    const flow_runtime_node_plan_t *node,
                                    const turbo_flow_operation_descriptor_t *operation,
                                    cmeta_effects *effects_out) {
  const turbo_flow_operation_runtime_contract_t *runtime =
      operation ? &operation->runtime : NULL;
  uint32_t barriers = FLOW_LOWERING_BARRIER_NONE;
  cmeta_effects effects = CMETA_EFFECT_PURE;

  if (!stage || !node || !operation) {
    if (effects_out) *effects_out = CMETA_EFFECT_UNKNOWN;
    return FLOW_LOWERING_BARRIER_UNTYPED_CALLABLE;
  }
  if (!stage->is_source && !stage->is_port) {
    barriers |= FLOW_LOWERING_BARRIER_UNTYPED_CALLABLE;
    effects |= CMETA_EFFECT_MAY_FAIL;
    if (!stage->operation_name) effects |= CMETA_EFFECT_UNKNOWN;
  }
  if (stage->mutability != TURBO_FLOW_STAGE_READONLY ||
      (stage->operation_name && operation->scope.authority == TURBO_FLOW_AUTHORITY_DATA_MUTATION)) {
    barriers |= FLOW_LOWERING_BARRIER_MESSAGE_MUTATION;
  }
  if (stage->keyed_fn || stage->keyed_emit_fn || stage->window_fn || stage->keyed_store ||
      (stage->operation_name && operation->scope.state != TURBO_FLOW_STATE_SCOPE_NONE) ||
      operation->scope.authority == TURBO_FLOW_AUTHORITY_OWNER_LOCAL ||
      operation->scope.authority == TURBO_FLOW_AUTHORITY_OWNER_COMMAND) {
    barriers |= FLOW_LOWERING_BARRIER_STATEFUL;
    effects |= CMETA_EFFECT_STATEFUL;
  }
  if (stage->exec.kind != TURBO_FLOW_EXEC_INLINE ||
      stage->data_strategy == TURBO_FLOW_DATA_WORKER_POOL ||
      (runtime && runtime->handoff != TURBO_FLOW_HANDOFF_DIRECT)) {
    barriers |= FLOW_LOWERING_BARRIER_ASYNC;
    effects |= CMETA_EFFECT_ASYNC;
  }
  if (stage->retry.max_attempts > 1u ||
      (runtime && runtime->error_mode == TURBO_FLOW_ERROR_RETRY)) {
    barriers |= FLOW_LOWERING_BARRIER_RETRY;
    effects |= CMETA_EFFECT_MAY_FAIL;
  }
  if (runtime && runtime->settlement != 0u) {
    barriers |= FLOW_LOWERING_BARRIER_SETTLEMENT;
    effects |= CMETA_EFFECT_IO;
  }
  if (stage->window_fn || stage->window_close_fn) {
    barriers |= FLOW_LOWERING_BARRIER_WINDOW;
    effects |= CMETA_EFFECT_STATEFUL;
  }
  if ((stage->effects & TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u) {
    barriers |= FLOW_LOWERING_BARRIER_DYNAMIC_ROUTE;
    effects |= CMETA_EFFECT_MAY_FAIL;
  }
  if (stage->is_source || stage->adapter_name || stage->resource_name ||
      operation->domain == TURBO_FLOW_DOMAIN_IO_TRANSPORT ||
      operation->domain == TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN ||
      operation->resource_domain != TURBO_FLOW_DOMAIN_NONE) {
    barriers |= FLOW_LOWERING_BARRIER_EXTERNAL_IO;
    effects |= CMETA_EFFECT_IO;
  }
  if (stage->reorder.capacity != 0u ||
      (runtime && runtime->ordering != TURBO_FLOW_ORDERING_UNORDERED)) {
    barriers |= FLOW_LOWERING_BARRIER_ORDERING;
    effects |= CMETA_EFFECT_STATEFUL;
  }
  if (node->incoming_count > 1u || node->outgoing_count > 1u || stage->is_port) {
    barriers |= FLOW_LOWERING_BARRIER_RELATION;
  }
  if (effects_out) *effects_out = effects;
  return barriers;
}

static int flow_node_has_dynamic_route(const flow_compiled_plan_t *plan,
                                       const flow_runtime_node_plan_t *node) {
  if (!plan || !node) return 0;
  for (size_t offset = 0u; offset < node->outgoing_count; ++offset) {
    const flow_runtime_edge_plan_t *edge = (const flow_runtime_edge_plan_t *)vec_at_const(
        &plan->edges, node->outgoing_begin + offset);
    if (edge && edge->kind != TURBO_FLOW_EDGE_UNCONDITIONAL) return 1;
  }
  return 0;
}

static uint32_t flow_region_find(vec_t *parents, uint32_t index) {
  uint32_t root = index;
  uint32_t *value;

  while ((value = (uint32_t *)vec_at(parents, root)) != NULL && *value != root) {
    root = *value;
  }
  while (index != root) {
    value = (uint32_t *)vec_at(parents, index);
    if (!value) break;
    index = *value;
    *value = root;
  }
  return root;
}

static int flow_region_union(vec_t *parents, vec_t *ranks, uint32_t left, uint32_t right) {
  uint32_t left_root = flow_region_find(parents, left);
  uint32_t right_root = flow_region_find(parents, right);
  uint8_t *left_rank;
  uint8_t *right_rank;

  if (left_root == right_root) return SALTS_OK;
  left_rank = (uint8_t *)vec_at(ranks, left_root);
  right_rank = (uint8_t *)vec_at(ranks, right_root);
  if (!left_rank || !right_rank) return SALTS_EPROTO;
  if (*left_rank < *right_rank) {
    *(uint32_t *)vec_at(parents, left_root) = right_root;
  } else {
    *(uint32_t *)vec_at(parents, right_root) = left_root;
    if (*left_rank == *right_rank) ++*left_rank;
  }
  return SALTS_OK;
}

static int flow_assign_candidate_regions(flow_compiled_plan_t *plan) {
  vec_t parents = {0};
  vec_t ranks = {0};
  vec_t root_regions = {0};
  const size_t stage_count = vec_size(&plan->stage_semantics);
  int rc = SALTS_OK;

  if (turbo_flow_stl_error(vec_init_bytes(&parents, sizeof(uint32_t), _Alignof(uint32_t),
                                          SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&ranks, sizeof(uint8_t), _Alignof(uint8_t),
                                          SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&root_regions, sizeof(uint32_t), _Alignof(uint32_t),
                                          SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_resize(&parents, stage_count)) != SALTS_OK ||
      turbo_flow_stl_error(vec_resize(&ranks, stage_count)) != SALTS_OK ||
      turbo_flow_stl_error(vec_resize(&root_regions, stage_count)) != SALTS_OK) {
    rc = SALTS_ENOMEM;
    goto cleanup;
  }
  memset(vec_data(&ranks), 0, stage_count * sizeof(uint8_t));
  for (size_t i = 0u; i < stage_count; ++i) {
    *(uint32_t *)vec_at(&parents, i) = (uint32_t)i;
    *(uint32_t *)vec_at(&root_regions, i) = FLOW_PLAN_INDEX_NONE;
  }
  for (size_t edge_index = 0u; edge_index < vec_size(&plan->edges); ++edge_index) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&plan->edges, edge_index);
    const flow_runtime_node_plan_t *from_node =
        (const flow_runtime_node_plan_t *)vec_at_const(&plan->nodes, edge->from_stage);
    const flow_runtime_node_plan_t *to_node =
        (const flow_runtime_node_plan_t *)vec_at_const(&plan->nodes, edge->to_stage);
    const flow_stage_semantic_plan_t *from_semantics =
        (const flow_stage_semantic_plan_t *)vec_at_const(&plan->stage_semantics,
                                                         edge->from_stage);
    const flow_stage_semantic_plan_t *to_semantics =
        (const flow_stage_semantic_plan_t *)vec_at_const(&plan->stage_semantics,
                                                         edge->to_stage);
    if (edge->kind == TURBO_FLOW_EDGE_UNCONDITIONAL && from_node && to_node &&
        from_node->outgoing_count == 1u && to_node->incoming_count == 1u &&
        from_semantics && to_semantics && from_semantics->lowering_candidate &&
        to_semantics->lowering_candidate) {
      rc = flow_region_union(&parents, &ranks, edge->from_stage, edge->to_stage);
      if (rc != SALTS_OK) goto cleanup;
    }
  }
  plan->candidate_region_count = 0u;
  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    flow_stage_semantic_plan_t *semantics =
        (flow_stage_semantic_plan_t *)vec_at(&plan->stage_semantics, stage_index);
    uint32_t root;
    uint32_t *region;
    if (!semantics || !semantics->lowering_candidate) continue;
    root = flow_region_find(&parents, (uint32_t)stage_index);
    region = (uint32_t *)vec_at(&root_regions, root);
    if (!region) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    if (*region == FLOW_PLAN_INDEX_NONE) {
      if (plan->candidate_region_count == UINT32_MAX) {
        rc = SALTS_ERANGE;
        goto cleanup;
      }
      *region = plan->candidate_region_count++;
    }
    semantics->candidate_region = *region;
  }

cleanup:
  vec_destroy(&root_regions);
  vec_destroy(&ranks);
  vec_destroy(&parents);
  return rc;
}

int flow_plan_build_semantics(const turbo_flow_t *flow, flow_compiled_plan_t *plan) {
  const size_t stage_count = flow ? vec_size(&flow->stages) : 0u;
  int rc;

  if (!flow || !plan || plan->sealed || !cmeta_type_desc_valid(&FLOW_MESSAGE_TYPE) ||
      !cmeta_type_desc_valid(&FLOW_OPERATION_TYPE)) {
    return SALTS_EINVAL;
  }
  plan->message_type = &FLOW_MESSAGE_TYPE;
  plan->operation_type = &FLOW_OPERATION_TYPE;
  rc = turbo_flow_stl_error(vec_resize(&plan->stage_semantics, stage_count));
  if (rc != SALTS_OK) return rc;
  memset(vec_data(&plan->stage_semantics), 0,
         stage_count * sizeof(flow_stage_semantic_plan_t));

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    const flow_runtime_node_plan_t *node =
        (const flow_runtime_node_plan_t *)vec_at_const(&plan->nodes, stage_index);
    const turbo_flow_operation_descriptor_t *operation =
        flow_stage_operation_descriptor(stage);
    flow_stage_semantic_plan_t *semantics =
        (flow_stage_semantic_plan_t *)vec_at(&plan->stage_semantics, stage_index);

    semantics->input_type_index = FLOW_PLAN_INDEX_NONE;
    semantics->output_type_index = FLOW_PLAN_INDEX_NONE;
    semantics->candidate_region = FLOW_PLAN_INDEX_NONE;
    semantics->cflow_operator =
        stage->emit_fn || stage->keyed_emit_fn ? CFLOW_OP_FLAT_MAP : CFLOW_OP_MAP;
    if (stage->is_source || stage->is_port) semantics->cflow_operator = CFLOW_OP_INPUT;
    if (operation && operation->input_type) {
      rc = flow_semantic_type_add(plan, operation->input_domain, operation->input_type,
                                  &semantics->input_type_index);
      if (rc != SALTS_OK) return rc;
    }
    if (operation && operation->output_type) {
      rc = flow_semantic_type_add(plan, operation->output_domain, operation->output_type,
                                  &semantics->output_type_index);
      if (rc != SALTS_OK) return rc;
    }
    semantics->typed = semantics->input_type_index != FLOW_PLAN_INDEX_NONE &&
                       semantics->output_type_index != FLOW_PLAN_INDEX_NONE;
    semantics->barriers = flow_stage_barriers(stage, node, operation, &semantics->effects);
    if (flow_node_has_dynamic_route(plan, node)) {
      semantics->barriers |= FLOW_LOWERING_BARRIER_DYNAMIC_ROUTE;
      semantics->effects |= CMETA_EFFECT_MAY_FAIL;
    }
    semantics->lowering_candidate =
        semantics->typed && !stage->is_source && !stage->is_port &&
        (semantics->barriers & ~FLOW_LOWERING_BARRIER_UNTYPED_CALLABLE) == 0u &&
        (semantics->effects & CMETA_EFFECT_UNKNOWN) == 0u;
  }

  /*
   * Descriptor self-pointers become stable only after all vector growth is
   * complete. A sealed plan never mutates this storage.
   */
  for (size_t i = 0u; i < vec_size(&plan->semantic_types); ++i) {
    flow_semantic_type_plan_t *type =
        (flow_semantic_type_plan_t *)vec_at(&plan->semantic_types, i);
    type->identity.stable_atom_id = type->stable_id;
    type->descriptor.name = type->stable_id;
    type->descriptor.identity = &type->identity;
    if (!cmeta_type_desc_valid(&type->descriptor)) return SALTS_EPROTO;
  }
  return flow_assign_candidate_regions(plan);
}
