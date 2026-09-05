#include "flow_internal.h"

#include <stdio.h>
#include <string.h>

static int flow_condition_valid(const turbo_flow_resource_condition_t *condition) {
  return condition && condition->kind >= TURBO_FLOW_RESOURCE_CONDITION_READY &&
         condition->kind <= TURBO_FLOW_RESOURCE_CONDITION_SATURATED &&
         condition->status >= TURBO_FLOW_CONDITION_UNKNOWN &&
         condition->status <= TURBO_FLOW_CONDITION_TRUE &&
         condition->reason >= TURBO_FLOW_RESOURCE_REASON_NONE &&
         condition->reason <= TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED;
}

static int flow_conditions_valid(const turbo_flow_resource_condition_t *conditions,
                                 uint32_t count) {
  if (count > TURBO_FLOW_RESOURCE_CONDITION_MAX) return 0;
  for (uint32_t i = 0u; i < count; ++i) {
    if (!flow_condition_valid(&conditions[i])) return 0;
    for (uint32_t j = 0u; j < i; ++j) {
      if (conditions[j].kind == conditions[i].kind) return 0;
    }
  }
  return 1;
}

static int flow_condition_matches(const turbo_flow_resource_condition_t *observed,
                                  const turbo_flow_resource_condition_t *desired) {
  return observed->kind == desired->kind && observed->status == desired->status &&
         (desired->reason == TURBO_FLOW_RESOURCE_REASON_NONE ||
          observed->reason == desired->reason);
}

int turbo_flow_resource_reconcile_tick(
    turbo_flow_t *flow, const turbo_flow_resource_reconcile_request_t *request,
    turbo_flow_resource_reconcile_result_t *out) {
  turbo_flow_resource_reconcile_result_t result = TURBO_FLOW_RESOURCE_RECONCILE_RESULT_INIT;
  turbo_flow_resource_command_t command;
  int rc;

  if (!flow || !request || request->size < sizeof(*request) || !out ||
      out->size < sizeof(*out) || !flow_resource_metadata_valid(&request->metadata) ||
      !flow_conditions_valid(request->conditions, request->condition_count) ||
      !flow_conditions_valid(request->desired_conditions, request->desired_condition_count) ||
      strcmp(request->command.target_uid, request->metadata.uid) != 0) {
    return SALTS_EINVAL;
  }
  command = request->command;
  command.size = sizeof(command);
  command.expected_generation = request->metadata.observed_generation;
  if (!flow_resource_command_valid(&command)) return SALTS_EINVAL;

  result.value_matches = request->observed_value == request->desired_value;
  for (uint32_t i = 0u; i < request->desired_condition_count; ++i) {
    for (uint32_t j = 0u; j < request->condition_count; ++j) {
      if (flow_condition_matches(&request->conditions[j], &request->desired_conditions[i])) {
        ++result.matched_condition_count;
        break;
      }
    }
  }
  if (result.value_matches &&
      result.matched_condition_count == request->desired_condition_count) {
    *out = result;
    return SALTS_OK;
  }
  if (request->metadata.observed_generation != request->metadata.generation) {
    result.action = TURBO_FLOW_RESOURCE_RECONCILE_OBSERVING;
    *out = result;
    return SALTS_OK;
  }

  rc = turbo_flow_resource_command(flow, &command, &result.command_result);
  result.status = rc;
  result.action = rc == SALTS_OK ? TURBO_FLOW_RESOURCE_RECONCILE_COMMAND_APPLIED
                                 : TURBO_FLOW_RESOURCE_RECONCILE_COMMAND_FAILED;
  *out = result;
  return rc;
}

static int flow_resize_workflow_spec_valid(const turbo_flow_resize_workflow_spec_t *spec) {
  return spec && spec->size >= sizeof(*spec) && spec->workflow_id[0] != '\0' &&
         memchr(spec->workflow_id, '\0', sizeof(spec->workflow_id)) != NULL &&
         spec->ingress_uid[0] != '\0' && spec->pool_uid[0] != '\0' &&
         memchr(spec->ingress_uid, '\0', sizeof(spec->ingress_uid)) != NULL &&
         memchr(spec->pool_uid, '\0', sizeof(spec->pool_uid)) != NULL &&
         strcmp(spec->ingress_uid, spec->pool_uid) != 0 &&
         spec->ingress_generation != 0u && spec->pool_generation != 0u &&
         spec->parallelism != 0u && spec->deadline_ns != 0u;
}

static int flow_resize_workflow_command(turbo_flow_t *flow,
                                        const turbo_flow_resize_workflow_spec_t *spec,
                                        turbo_flow_resize_workflow_state_t *state,
                                        turbo_flow_resource_command_kind_t kind, const char *uid,
                                        uint64_t generation, uint32_t parallelism,
                                        turbo_flow_resource_command_result_t *out) {
  turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
  int written;

  command.kind = kind;
  written = snprintf(command.target_uid, sizeof(command.target_uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(command.target_uid)) return SALTS_ENAMETOOLONG;
  written = snprintf(command.idempotency_key, sizeof(command.idempotency_key), "%s:%u:%u",
                     spec->workflow_id, (unsigned)state->phase, (unsigned)state->attempt);
  if (written < 0 || (size_t)written >= sizeof(command.idempotency_key))
    return SALTS_ENAMETOOLONG;
  command.expected_generation = generation;
  command.deadline_ns = spec->deadline_ns;
  command.parallelism = parallelism;
  command.drain_timeout_ms = spec->drain_timeout_ms;
  return turbo_flow_resource_command(flow, &command, out);
}

static uint64_t flow_resize_workflow_drain_timeout(const turbo_flow_resize_workflow_spec_t *spec) {
  uint64_t now;
  uint64_t remaining_ms;
  if (spec->deadline_ns == UINT64_MAX) return spec->drain_timeout_ms;
  now = salts_hrtime();
  if (now >= spec->deadline_ns) return 0u;
  remaining_ms = (spec->deadline_ns - now + UINT64_C(999999)) / UINT64_C(1000000);
  return spec->drain_timeout_ms < remaining_ms ? spec->drain_timeout_ms : remaining_ms;
}

int turbo_flow_resize_workflow_init(const turbo_flow_resize_workflow_spec_t *spec,
                                    turbo_flow_resize_workflow_state_t *state) {
  if (!flow_resize_workflow_spec_valid(spec) || !state || state->size < sizeof(*state))
    return SALTS_EINVAL;
  memset(state, 0, sizeof(*state));
  state->size = sizeof(*state);
  state->spec = *spec;
  state->spec.size = sizeof(state->spec);
  state->phase = TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS;
  state->failed_phase = TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS;
  state->ingress_generation = spec->ingress_generation;
  state->pool_generation = spec->pool_generation;
  state->last_status = SALTS_OK;
  return SALTS_OK;
}

static int flow_resize_workflow_fail(turbo_flow_resize_workflow_state_t *state,
                                     turbo_flow_resize_workflow_result_t *result, int status) {
  state->failed_phase = state->phase;
  state->phase = TURBO_FLOW_RESIZE_WORKFLOW_FAILED;
  state->last_status = status;
  result->action = TURBO_FLOW_RESIZE_WORKFLOW_STEP_FAILED;
  result->status = status;
  result->phase_after = state->phase;
  return status;
}

int turbo_flow_resize_workflow_tick(turbo_flow_t *flow,
                                    turbo_flow_resize_workflow_state_t *state,
                                    turbo_flow_resize_workflow_result_t *out) {
  turbo_flow_resize_workflow_result_t result = TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
  const turbo_flow_resize_workflow_spec_t *spec;
  int rc;

  if (!flow || !state || state->size < sizeof(*state) || !out || out->size < sizeof(*out) ||
      state->phase < TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS ||
      state->phase > TURBO_FLOW_RESIZE_WORKFLOW_FAILED) {
    return SALTS_EINVAL;
  }
  spec = &state->spec;
  if (!flow_resize_workflow_spec_valid(spec)) return SALTS_EINVAL;
  result.phase_before = state->phase;
  result.phase_after = state->phase;
  if (state->phase == TURBO_FLOW_RESIZE_WORKFLOW_DONE ||
      state->phase == TURBO_FLOW_RESIZE_WORKFLOW_FAILED) {
    result.status = state->last_status;
    *out = result;
    return state->last_status;
  }
  if (spec->deadline_ns != UINT64_MAX && salts_hrtime() >= spec->deadline_ns) {
    rc = flow_resize_workflow_fail(state, &result, SALTS_ETIMEDOUT);
    *out = result;
    return rc;
  }

  switch (state->phase) {
    case TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS:
      rc = flow_resize_workflow_command(flow, spec, state, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE,
                                        spec->ingress_uid, state->ingress_generation, 0u,
                                        &result.command_result);
      if (result.command_result.generation_before == state->ingress_generation &&
          result.command_result.generation_after != 0u)
        state->ingress_generation = result.command_result.generation_after;
      if (rc != SALTS_OK) break;
      state->phase = TURBO_FLOW_RESIZE_WORKFLOW_DRAIN_GRAPH;
      ++state->commands_applied;
      result.action = TURBO_FLOW_RESIZE_WORKFLOW_COMMAND_APPLIED;
      break;
    case TURBO_FLOW_RESIZE_WORKFLOW_DRAIN_GRAPH:
      rc = turbo_flow_drain(flow, flow_resize_workflow_drain_timeout(spec));
      if (rc != SALTS_OK) break;
      state->phase = TURBO_FLOW_RESIZE_WORKFLOW_RESIZE_POOL;
      result.action = TURBO_FLOW_RESIZE_WORKFLOW_GRAPH_DRAINED;
      break;
    case TURBO_FLOW_RESIZE_WORKFLOW_RESIZE_POOL:
      rc = flow_resize_workflow_command(flow, spec, state,
                                        TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL, spec->pool_uid,
                                        state->pool_generation, spec->parallelism,
                                        &result.command_result);
      if (result.command_result.generation_before == state->pool_generation &&
          result.command_result.generation_after != 0u)
        state->pool_generation = result.command_result.generation_after;
      if (rc != SALTS_OK) break;
      state->phase = TURBO_FLOW_RESIZE_WORKFLOW_RESUME_GRAPH;
      ++state->commands_applied;
      result.action = TURBO_FLOW_RESIZE_WORKFLOW_COMMAND_APPLIED;
      break;
    case TURBO_FLOW_RESIZE_WORKFLOW_RESUME_GRAPH:
      rc = turbo_flow_resume(flow);
      if (rc != SALTS_OK) break;
      state->phase = TURBO_FLOW_RESIZE_WORKFLOW_RESUME_INGRESS;
      result.action = TURBO_FLOW_RESIZE_WORKFLOW_GRAPH_RESUMED;
      break;
    case TURBO_FLOW_RESIZE_WORKFLOW_RESUME_INGRESS:
      rc = flow_resize_workflow_command(flow, spec, state, TURBO_FLOW_RESOURCE_COMMAND_RESUME,
                                        spec->ingress_uid, state->ingress_generation, 0u,
                                        &result.command_result);
      if (result.command_result.generation_before == state->ingress_generation &&
          result.command_result.generation_after != 0u)
        state->ingress_generation = result.command_result.generation_after;
      if (rc != SALTS_OK) break;
      state->phase = TURBO_FLOW_RESIZE_WORKFLOW_DONE;
      ++state->commands_applied;
      result.action = TURBO_FLOW_RESIZE_WORKFLOW_COMMAND_APPLIED;
      break;
    default:
      return SALTS_EINVAL;
  }
  if (rc != SALTS_OK) {
    rc = flow_resize_workflow_fail(state, &result, rc);
  } else {
    state->last_status = SALTS_OK;
    result.status = SALTS_OK;
    result.phase_after = state->phase;
  }
  *out = result;
  return rc;
}

int turbo_flow_resize_workflow_retry(turbo_flow_resize_workflow_state_t *state) {
  if (!state || state->size < sizeof(*state) ||
      state->phase != TURBO_FLOW_RESIZE_WORKFLOW_FAILED ||
      state->failed_phase >= TURBO_FLOW_RESIZE_WORKFLOW_DONE || state->attempt == UINT32_MAX) {
    return SALTS_EINVAL;
  }
  ++state->attempt;
  state->phase = state->failed_phase;
  state->last_status = SALTS_OK;
  return SALTS_OK;
}
