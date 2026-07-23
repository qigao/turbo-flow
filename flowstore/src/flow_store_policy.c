#include "turbo_flow_store_policy.h"

static int flow_store_policy_valid(const turbo_flow_store_policy_t *policy) {
  return policy && policy->size >= sizeof(*policy) &&
         policy->abi_version == TURBO_FLOW_STORE_ABI_VERSION && policy->local_max_records > 0u &&
         policy->local_max_bytes > 0u && policy->local_max_item_bytes > 0u &&
         policy->local_max_writes_per_second > 0u && policy->local_max_retention_ms > 0u;
}

static int flow_store_requirements_valid(const turbo_flow_store_requirements_t *requirements) {
  return requirements && requirements->size >= sizeof(*requirements) &&
         requirements->abi_version == TURBO_FLOW_STORE_ABI_VERSION &&
         requirements->data_class >= TURBO_FLOW_STORE_DATA_STATE &&
         requirements->data_class <= TURBO_FLOW_STORE_DATA_METRIC &&
         requirements->expected_records > 0u && requirements->expected_bytes > 0u &&
         requirements->max_item_bytes > 0u && requirements->writes_per_second > 0u &&
         requirements->retention_ms > 0u;
}

static turbo_flow_storage_capabilities_t
flow_store_model_capability(turbo_flow_store_model_t model) {
  switch (model) {
  case TURBO_FLOW_STORE_MODEL_STATE:
    return TURBO_FLOW_STORAGE_CAP_STATE;
  case TURBO_FLOW_STORE_MODEL_INDEX:
    return TURBO_FLOW_STORAGE_CAP_INDEX;
  case TURBO_FLOW_STORE_MODEL_LOG:
    return TURBO_FLOW_STORAGE_CAP_LOG;
  case TURBO_FLOW_STORE_MODEL_TIME_SERIES:
    return TURBO_FLOW_STORAGE_CAP_SERIES;
  default:
    return 0u;
  }
}

int turbo_flow_store_route(const turbo_flow_store_policy_t *policy,
                           const turbo_flow_store_requirements_t *requirements,
                           turbo_flow_store_decision_t *decision) {
  turbo_flow_storage_capabilities_t required_capability;
  turbo_flow_store_model_t model;
  turbo_flow_store_placement_t placement;
  size_t output_size;
  int requires_remote;
  if (!flow_store_policy_valid(policy) || !flow_store_requirements_valid(requirements) ||
      !decision || decision->size < sizeof(*decision) ||
      decision->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  switch (requirements->data_class) {
  case TURBO_FLOW_STORE_DATA_STATE:
    model = TURBO_FLOW_STORE_MODEL_STATE;
    break;
  case TURBO_FLOW_STORE_DATA_MEMBERSHIP:
    model = TURBO_FLOW_STORE_MODEL_INDEX;
    break;
  case TURBO_FLOW_STORE_DATA_EVENT:
    model = TURBO_FLOW_STORE_MODEL_LOG;
    break;
  case TURBO_FLOW_STORE_DATA_METRIC:
    model = TURBO_FLOW_STORE_MODEL_TIME_SERIES;
    break;
  default:
    return TURBO_EINVAL;
  }
  required_capability = flow_store_model_capability(model);
  requires_remote = requirements->durable ||
                    requirements->expected_records > policy->local_max_records ||
                    requirements->expected_bytes > policy->local_max_bytes ||
                    requirements->max_item_bytes > policy->local_max_item_bytes ||
                    requirements->writes_per_second > policy->local_max_writes_per_second ||
                    requirements->retention_ms > policy->local_max_retention_ms;
  if (requires_remote && (policy->remote_capabilities & required_capability) == 0u)
    return TURBO_ENOTSUP;
  placement = requires_remote ? TURBO_FLOW_STORE_PLACEMENT_BACKEND
                              : TURBO_FLOW_STORE_PLACEMENT_MEMORY;
  output_size = decision->size;
  *decision = (turbo_flow_store_decision_t)TURBO_FLOW_STORE_DECISION_INIT;
  decision->size = output_size;
  decision->model = model;
  decision->placement = placement;
  return TURBO_OK;
}
