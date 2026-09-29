#include "flow_internal.h"

#include <string.h>

static const flow_stage_semantic_plan_t *
flow_cflow_stage_semantics(const flow_compiled_plan_t *plan, uint32_t stage_index) {
  if (!plan) return NULL;
  return (const flow_stage_semantic_plan_t *)vec_at_const(&plan->stage_semantics,
                                                          stage_index);
}

static int flow_cflow_stage_in_candidate(const flow_compiled_plan_t *plan,
                                         uint32_t stage_index,
                                         uint32_t candidate_region) {
  const flow_stage_semantic_plan_t *semantics =
      flow_cflow_stage_semantics(plan, stage_index);
  return semantics && semantics->candidate_region == candidate_region;
}

static int flow_cflow_stage_data_contract(
    const turbo_flow_t *flow, uint32_t stage_index,
    const cmeta_data_desc **input_out, const cmeta_data_desc **output_out) {
  const flow_stage_plan_impl_t *stage;
  const flow_operation_registration_t *registration;
  const cmeta_data_desc *input = NULL;
  const cmeta_data_desc *output = NULL;

  if (input_out) *input_out = NULL;
  if (output_out) *output_out = NULL;
  if (!flow || stage_index >= vec_size(&flow->stages)) return SALTS_EINVAL;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
  if (!stage || !stage->operation_name) return SALTS_EPROTO;
  registration = flow_find_operation_registration(flow, stage->operation_name);
  if (!registration || !registration->reflected ||
      registration->reflected_lowering != TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP)
    return SALTS_EPROTO;

  for (size_t i = 0u; i < vec_size(&registration->reflected_ports); ++i) {
    const turbo_flow_operation_port_binding_t *port =
        (const turbo_flow_operation_port_binding_t *)vec_at_const(
            &registration->reflected_ports, i);
    if (!port || !port->data) return SALTS_EPROTO;
    if (port->direction == TURBO_FLOW_OPERATION_PORT_INPUT) {
      if (input) return SALTS_EPROTO;
      input = port->data;
    } else if (port->direction == TURBO_FLOW_OPERATION_PORT_OUTPUT) {
      if (output) return SALTS_EPROTO;
      output = port->data;
    } else {
      return SALTS_EPROTO;
    }
  }
  if (!input || !output) return SALTS_EPROTO;
  if (input_out) *input_out = input;
  if (output_out) *output_out = output;
  return SALTS_OK;
}

int flow_cflow_value_slot_plan_classify(
    const cmeta_type_desc *input_type, const cmeta_type_desc *output_type,
    flow_cflow_value_slot_plan_t *out) {
  const cmeta_trait_flags trivial =
      CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY;
  const cmeta_trait_flags managed =
      CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY;

  if (!out) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  if (!cmeta_type_desc_valid(input_type) || !cmeta_type_desc_valid(output_type))
    return SALTS_EINVAL;

  out->extent = output_type->size;
  out->alignment = output_type->align;
  out->available_traits =
      output_type->traits ? output_type->traits->flags & CMETA_TRAIT_MASK : 0u;

  if (!cmeta_type_equal(input_type, output_type))
    return SALTS_OK;

  if (cmeta_type_require_traits(output_type, trivial) == CMETA_OK) {
    out->mode = FLOW_CFLOW_VALUE_SLOT_REUSE_INPUT;
    out->transfer = FLOW_CFLOW_VALUE_TRANSFER_TRIVIAL_COPY;
    out->required_traits = trivial;
    out->source_destroy_after_transfer = 0;
    return SALTS_OK;
  }

  if (cmeta_type_require_traits(output_type, managed) == CMETA_OK) {
    /*
     * CMeta proves that a moved-from source can be destroyed and a destination
     * can be move-constructed. Runtime reuse is deliberately not admitted yet:
     * TurboFlow must also own the destination storage before replacing a live
     * non-trivial projection in place.
     */
    out->mode = FLOW_CFLOW_VALUE_SLOT_NONE;
    out->transfer = FLOW_CFLOW_VALUE_TRANSFER_MOVE_CONSTRUCT;
    out->required_traits = CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY;
    out->source_destroy_after_transfer = 1;
  }
  return SALTS_OK;
}

static int flow_cflow_direct_boundary_supported(
    const turbo_flow_t *flow, uint32_t entry_stage, uint32_t exit_stage,
    const cflow_plan *plan, const cmeta_data_desc **input_data_out,
    const cmeta_data_desc **output_data_out) {
  const cmeta_data_desc *input_data = NULL;
  const cmeta_data_desc *output_data = NULL;
  const cmeta_trait_flags trivial =
      CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY;
  const cmeta_trait_flags managed =
      CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY;
  int rc;

  if (!flow || !plan || !plan->input_type || !plan->output_type)
    return SALTS_EINVAL;
  rc = flow_cflow_stage_data_contract(flow, entry_stage, &input_data, NULL);
  if (rc != SALTS_OK) return rc;
  rc = flow_cflow_stage_data_contract(flow, exit_stage, NULL, &output_data);
  if (rc != SALTS_OK) return rc;

  if (!cmeta_data_desc_equal(input_data, output_data) ||
      !cmeta_type_equal(input_data->storage_type, plan->input_type) ||
      !cmeta_type_equal(output_data->storage_type, plan->output_type) ||
      !cmeta_type_equal(plan->input_type, plan->output_type)) {
    return SALTS_ENOTSUP;
  }
  if (cmeta_type_require_traits(plan->output_type, trivial) != CMETA_OK &&
      cmeta_type_require_traits(plan->output_type, managed) != CMETA_OK) {
    return SALTS_ENOTSUP;
  }
  if (input_data_out) *input_data_out = input_data;
  if (output_data_out) *output_data_out = output_data;
  return SALTS_OK;
}

static int flow_cflow_region_shape(const flow_compiled_plan_t *plan,
                                   uint32_t candidate_region,
                                   uint32_t *entry_out,
                                   uint32_t *exit_out,
                                   uint32_t *stage_count_out,
                                   int *compilable_out) {
  const size_t stage_count = plan ? vec_size(&plan->stage_semantics) : 0u;
  uint32_t entry = FLOW_PLAN_INDEX_NONE;
  uint32_t exit = FLOW_PLAN_INDEX_NONE;
  uint32_t count = 0u;
  int compilable = 1;

  if (!plan || !entry_out || !exit_out || !stage_count_out || !compilable_out)
    return SALTS_EINVAL;

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_semantic_plan_t *semantics =
        flow_cflow_stage_semantics(plan, (uint32_t)stage_index);
    size_t incoming = 0u;
    size_t outgoing = 0u;

    if (!semantics || semantics->candidate_region != candidate_region) continue;
    if (count == UINT32_MAX) return SALTS_ERANGE;
    ++count;

    if (!semantics->reflected ||
        (!semantics->reflected_typed_adapter &&
         !cmeta_callable_contract_valid(semantics->callable)) ||
        (semantics->reflected_typed_adapter &&
         !cflow_function_typed_adapter_projection_valid(
             &semantics->typed_adapter_projection)) ||
        !semantics->canonical_input_type || !semantics->canonical_output_type) {
      compilable = 0;
    }

    for (size_t edge_index = 0u; edge_index < vec_size(&plan->edges); ++edge_index) {
      const flow_runtime_edge_plan_t *edge =
          (const flow_runtime_edge_plan_t *)vec_at_const(&plan->edges, edge_index);
      if (!edge) return SALTS_EPROTO;
      if (edge->to_stage == stage_index &&
          flow_cflow_stage_in_candidate(plan, edge->from_stage, candidate_region))
        ++incoming;
      if (edge->from_stage == stage_index &&
          flow_cflow_stage_in_candidate(plan, edge->to_stage, candidate_region))
        ++outgoing;
    }

    if (incoming > 1u || outgoing > 1u) return SALTS_EPROTO;
    if (incoming == 0u) {
      if (entry != FLOW_PLAN_INDEX_NONE) return SALTS_EPROTO;
      entry = (uint32_t)stage_index;
    }
    if (outgoing == 0u) {
      if (exit != FLOW_PLAN_INDEX_NONE) return SALTS_EPROTO;
      exit = (uint32_t)stage_index;
    }
  }

  if (count == 0u || entry == FLOW_PLAN_INDEX_NONE || exit == FLOW_PLAN_INDEX_NONE)
    return SALTS_EPROTO;

  *entry_out = entry;
  *exit_out = exit;
  *stage_count_out = count;
  *compilable_out = compilable;
  return SALTS_OK;
}

static int flow_cflow_region_successor(const flow_compiled_plan_t *plan,
                                       uint32_t candidate_region,
                                       uint32_t stage_index,
                                       uint32_t *successor_out) {
  uint32_t successor = FLOW_PLAN_INDEX_NONE;
  if (!plan || !successor_out) return SALTS_EINVAL;

  for (size_t edge_index = 0u; edge_index < vec_size(&plan->edges); ++edge_index) {
    const flow_runtime_edge_plan_t *edge =
        (const flow_runtime_edge_plan_t *)vec_at_const(&plan->edges, edge_index);
    if (!edge) return SALTS_EPROTO;
    if (edge->from_stage != stage_index ||
        !flow_cflow_stage_in_candidate(plan, edge->to_stage, candidate_region))
      continue;
    if (successor != FLOW_PLAN_INDEX_NONE) return SALTS_EPROTO;
    successor = edge->to_stage;
  }
  *successor_out = successor;
  return SALTS_OK;
}

static int flow_cflow_region_compile(const turbo_flow_t *flow,
                                     const flow_compiled_plan_t *plan,
                                     uint32_t candidate_region,
                                     uint32_t entry_stage,
                                     uint32_t exit_stage,
                                     uint32_t stage_count,
                                     flow_cflow_region_plan_t *region_out) {
  cflow_graph surface = {0};
  flow_cflow_region_plan_t region;
  const cmeta_type_desc *previous_output = NULL;
  uint32_t current = entry_stage;
  uint32_t visited = 0u;
  int rc = SALTS_OK;

  if (!flow || !plan || !region_out || stage_count == 0u) return SALTS_EINVAL;
  memset(&region, 0, sizeof(region));
  region.candidate_region = candidate_region;
  region.entry_stage = entry_stage;
  region.exit_stage = exit_stage;
  region.stage_count = stage_count;
  surface.root = CMETA_INVALID_ID;

  {
    const flow_stage_semantic_plan_t *entry =
        flow_cflow_stage_semantics(plan, entry_stage);
    if (!entry || !entry->canonical_input_type) return SALTS_EPROTO;
    cflow_graph_init(&surface, entry->canonical_input_type);
    if (surface.error) {
      cflow_graph_destroy(&surface);
      return SALTS_EPROTO;
    }
  }

  while (current != FLOW_PLAN_INDEX_NONE) {
    const flow_stage_semantic_plan_t *semantics =
        flow_cflow_stage_semantics(plan, current);
    uint32_t successor = FLOW_PLAN_INDEX_NONE;

    if (!semantics || semantics->candidate_region != candidate_region ||
        !semantics->reflected || !semantics->lowering_candidate ||
        !semantics->canonical_input_type || !semantics->canonical_output_type ||
        (!semantics->reflected_typed_adapter &&
         !cmeta_callable_contract_valid(semantics->callable)) ||
        (semantics->reflected_typed_adapter &&
         !cflow_function_typed_adapter_projection_valid(
             &semantics->typed_adapter_projection))) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    if (previous_output &&
        !cmeta_type_equal(previous_output, semantics->canonical_input_type)) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    if (semantics->reflected_typed_adapter) {
      if (!cflow_graph_add_function_typed_adapter_projection(
              &surface, &semantics->typed_adapter_projection)) {
        rc = SALTS_ENOTSUP;
        goto cleanup;
      }
    } else if (!cflow_graph_add(&surface, semantics->cflow_operator,
                                semantics->callable, NULL)) {
      rc = SALTS_ENOTSUP;
      goto cleanup;
    }

    previous_output = semantics->canonical_output_type;
    ++visited;
    if (visited > stage_count) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    rc = flow_cflow_region_successor(plan, candidate_region, current, &successor);
    if (rc != SALTS_OK) goto cleanup;
    if (successor == FLOW_PLAN_INDEX_NONE) {
      if (current != exit_stage || visited != stage_count) rc = SALTS_EPROTO;
      break;
    }
    current = successor;
  }
  if (rc != SALTS_OK) goto cleanup;

  if (!cflow_plan_compile_surface(&region.plan, &surface, &region.stats) ||
      !region.plan.impl || region.plan.error ||
      !cmeta_type_equal(region.plan.input_type,
                        flow_cflow_stage_semantics(plan, entry_stage)->canonical_input_type) ||
      !cmeta_type_equal(region.plan.output_type,
                        flow_cflow_stage_semantics(plan, exit_stage)->canonical_output_type)) {
    rc = SALTS_ENOTSUP;
    goto cleanup;
  }

  rc = flow_cflow_direct_boundary_supported(
      flow, entry_stage, exit_stage, &region.plan,
      &region.input_data, &region.output_data);
  if (rc != SALTS_OK) goto cleanup;
  region.backend = FLOW_CFLOW_REGION_BACKEND_DIRECT;
  rc = flow_cflow_value_slot_plan_classify(
      region.plan.input_type, region.plan.output_type, &region.value_slot);
  if (rc != SALTS_OK) goto cleanup;
  if (region.value_slot.mode == FLOW_CFLOW_VALUE_SLOT_REUSE_INPUT &&
      region.value_slot.transfer == FLOW_CFLOW_VALUE_TRANSFER_TRIVIAL_COPY) {
    /* Trivial direct-region commit remains in-place. */
  } else if (region.value_slot.mode == FLOW_CFLOW_VALUE_SLOT_NONE &&
             region.value_slot.transfer == FLOW_CFLOW_VALUE_TRANSFER_MOVE_CONSTRUCT &&
             region.value_slot.source_destroy_after_transfer == 1 &&
             region.value_slot.required_traits ==
                 (CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY)) {
    /*
     * The runtime provides an independently allocated aligned destination
     * before invoking the no-fail CMeta move contract.
     */
    region.value_slot.mode = FLOW_CFLOW_VALUE_SLOT_OWNED_OUTPUT;
  } else {
    rc = SALTS_ENOTSUP;
    goto cleanup;
  }

  *region_out = region;
  memset(&region, 0, sizeof(region));

cleanup:
  cflow_plan_destroy(&region.plan);
  cflow_graph_destroy(&surface);
  return rc;
}

int flow_plan_build_cflow_regions(const turbo_flow_t *flow, flow_compiled_plan_t *plan) {
  const size_t stage_count = plan ? vec_size(&plan->stage_semantics) : 0u;
  int rc;

  if (!flow || !plan || plan->sealed) return SALTS_EINVAL;
  rc = turbo_flow_stl_error(vec_resize(&plan->cflow_region_by_stage, stage_count));
  if (rc != SALTS_OK) return rc;
  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index)
    *(uint32_t *)vec_at(&plan->cflow_region_by_stage, stage_index) = FLOW_PLAN_INDEX_NONE;

  for (uint32_t candidate = 0u; candidate < plan->candidate_region_count; ++candidate) {
    flow_cflow_region_plan_t region;
    uint32_t entry = FLOW_PLAN_INDEX_NONE;
    uint32_t exit = FLOW_PLAN_INDEX_NONE;
    uint32_t count = 0u;
    size_t compiled_index;
    int compilable = 0;

    rc = flow_cflow_region_shape(plan, candidate, &entry, &exit, &count, &compilable);
    if (rc != SALTS_OK) return rc;
    if (!compilable) continue;

    memset(&region, 0, sizeof(region));
    rc = flow_cflow_region_compile(flow, plan, candidate, entry, exit, count, &region);
    if (rc != SALTS_OK) return rc;
    compiled_index = vec_size(&plan->cflow_regions);
    if (compiled_index > UINT32_MAX) {
      cflow_plan_destroy(&region.plan);
      return SALTS_ERANGE;
    }
    rc = turbo_flow_stl_error(vec_push(&plan->cflow_regions, &region));
    if (rc != SALTS_OK) {
      cflow_plan_destroy(&region.plan);
      return rc;
    }
    memset(&region, 0, sizeof(region));

    for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
      const flow_stage_semantic_plan_t *semantics =
          flow_cflow_stage_semantics(plan, (uint32_t)stage_index);
      if (semantics && semantics->candidate_region == candidate)
        *(uint32_t *)vec_at(&plan->cflow_region_by_stage, stage_index) =
            (uint32_t)compiled_index;
    }
  }

  return SALTS_OK;
}


const flow_cflow_region_plan_t *flow_cflow_region_for_entry(
    const turbo_flow_t *flow, uint32_t stage_index, uint32_t *region_index_out) {
  const uint32_t *region_index;
  const flow_cflow_region_plan_t *region;

  if (region_index_out) *region_index_out = FLOW_PLAN_INDEX_NONE;
  if (!flow || !flow->compiled_plan.sealed ||
      stage_index >= vec_size(&flow->compiled_plan.cflow_region_by_stage))
    return NULL;
  region_index = (const uint32_t *)vec_at_const(
      &flow->compiled_plan.cflow_region_by_stage, stage_index);
  if (!region_index || *region_index == FLOW_PLAN_INDEX_NONE ||
      *region_index >= vec_size(&flow->compiled_plan.cflow_regions))
    return NULL;
  region = (const flow_cflow_region_plan_t *)vec_at_const(
      &flow->compiled_plan.cflow_regions, *region_index);
  if (!region || region->backend != FLOW_CFLOW_REGION_BACKEND_DIRECT ||
      region->entry_stage != stage_index)
    return NULL;
  if (region_index_out) *region_index_out = *region_index;
  return region;
}

int flow_cflow_region_execute(
    turbo_flow_t *flow, const flow_cflow_region_plan_t *region,
    turbo_flow_msg_t *message) {
  cflow_result result = {0};
  const cmeta_data_desc *input_data;
  const void *input_value;
  int rc;

  if (!flow || !region || !message ||
      region->backend != FLOW_CFLOW_REGION_BACKEND_DIRECT ||
      !region->input_data || !region->output_data)
    return SALTS_EINVAL;

  input_value = turbo_flow_msg_projection(message, NULL);
  input_data = turbo_flow_msg_projection_data(message);
  if (!input_value || !input_data ||
      !cmeta_data_desc_equal(input_data, region->input_data))
    return SALTS_EPROTO;

  if (!cflow_plan_eval_array(&region->plan, input_value, 1u, &result))
    return SALTS_EPROTO;
  if (result.count != 1u || !result.data ||
      !cmeta_type_equal(result.type, region->plan.output_type)) {
    cflow_result_destroy(&result);
    return SALTS_EPROTO;
  }

  if (region->value_slot.extent != region->plan.output_type->size ||
      region->value_slot.alignment != region->plan.output_type->align) {
    cflow_result_destroy(&result);
    return SALTS_EPROTO;
  }

  if (region->value_slot.mode == FLOW_CFLOW_VALUE_SLOT_REUSE_INPUT &&
      region->value_slot.transfer == FLOW_CFLOW_VALUE_TRANSFER_TRIVIAL_COPY &&
      region->value_slot.source_destroy_after_transfer == 0 &&
      region->value_slot.required_traits ==
          (CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY)) {
    rc = flow_msg_commit_trivial_projection_in_place(
        message, region->input_data, region->output_data, result.data);
  } else if (region->value_slot.mode == FLOW_CFLOW_VALUE_SLOT_OWNED_OUTPUT &&
             region->value_slot.transfer == FLOW_CFLOW_VALUE_TRANSFER_MOVE_CONSTRUCT &&
             region->value_slot.source_destroy_after_transfer == 1 &&
             region->value_slot.required_traits ==
                 (CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY)) {
    rc = flow_msg_commit_managed_projection_move(
        message, region->input_data, region->output_data, result.data);
  } else {
    rc = SALTS_EPROTO;
  }
  cflow_result_destroy(&result);
  return rc;
}
