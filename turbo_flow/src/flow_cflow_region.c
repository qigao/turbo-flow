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
        !cmeta_callable_contract_valid(semantics->callable) ||
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

static int flow_cflow_region_compile(const flow_compiled_plan_t *plan,
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

  if (!plan || !region_out || stage_count == 0u) return SALTS_EINVAL;
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
        !cmeta_callable_contract_valid(semantics->callable)) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    if (previous_output &&
        !cmeta_type_equal(previous_output, semantics->canonical_input_type)) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    if (!cflow_graph_add(&surface, semantics->cflow_operator,
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
    rc = flow_cflow_region_compile(plan, candidate, entry, exit, count, &region);
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
