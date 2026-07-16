#include "turbo_flow_fmq_compatibility.h"

#include "turbo_error.h"
#include "turbo_str_view.h"

#include <string.h>

static int flow_fmq_contract_text_valid(const char *value, size_t capacity) {
  const char *end;
  if (!value || capacity == 0u) return 0;
  end = (const char *)memchr(value, '\0', capacity);
  return end && end != value && tstr_v_utf8_valid(tstr_v_from_buf(value, (size_t)(end - value)));
}

static int flow_fmq_minor_range_valid(const turbo_flow_fmq_minor_range_t *range) {
  return range && range->major > 0u && range->minor_min <= range->minor_max;
}

static int flow_fmq_store_range_valid(const turbo_flow_fmq_store_range_t *range) {
  return range && range->major > 0u && range->read_minor_min <= range->read_minor_max &&
         range->write_minor_min <= range->write_minor_max &&
         range->write_minor_min >= range->read_minor_min &&
         range->write_minor_max <= range->read_minor_max;
}

static int flow_fmq_contract_valid(const turbo_flow_fmq_compatibility_contract_t *contract) {
  return contract && contract->size >= sizeof(*contract) &&
         flow_fmq_contract_text_valid(contract->release, sizeof(contract->release)) &&
         contract->fmq_wire_min > 0u && contract->fmq_wire_min <= contract->fmq_wire_max &&
         flow_fmq_minor_range_valid(&contract->tfmp) &&
         flow_fmq_store_range_valid(&contract->management_store) &&
         flow_fmq_minor_range_valid(&contract->yaml_schema);
}

static int flow_fmq_u16_intersection(uint16_t left_min, uint16_t left_max, uint16_t right_min,
                                     uint16_t right_max, uint16_t *selected) {
  uint16_t low = left_min > right_min ? left_min : right_min;
  uint16_t high = left_max < right_max ? left_max : right_max;
  if (low > high) return 0;
  *selected = high;
  return 1;
}

static int flow_fmq_minor_intersection(const turbo_flow_fmq_minor_range_t *left,
                                       const turbo_flow_fmq_minor_range_t *right,
                                       uint16_t *selected) {
  return left->major == right->major &&
         flow_fmq_u16_intersection(left->minor_min, left->minor_max, right->minor_min,
                                   right->minor_max, selected);
}

static int flow_fmq_store_intersection(const turbo_flow_fmq_store_range_t *left,
                                       const turbo_flow_fmq_store_range_t *right,
                                       uint16_t *selected) {
  uint16_t low;
  uint16_t high;
  if (left->major != right->major) return 0;
  low = left->write_minor_min;
  if (right->write_minor_min > low) low = right->write_minor_min;
  if (left->read_minor_min > low) low = left->read_minor_min;
  if (right->read_minor_min > low) low = right->read_minor_min;
  high = left->write_minor_max;
  if (right->write_minor_max < high) high = right->write_minor_max;
  if (left->read_minor_max < high) high = left->read_minor_max;
  if (right->read_minor_max < high) high = right->read_minor_max;
  if (low > high) return 0;
  *selected = high;
  return 1;
}

static int flow_fmq_gateway_protocols(const turbo_flow_fmq_compatibility_contract_t *current,
                                      const turbo_flow_fmq_compatibility_contract_t *candidate,
                                      const turbo_flow_fmq_compatibility_contract_t *gateway) {
  uint16_t selected;
  return gateway &&
         flow_fmq_u16_intersection(current->fmq_wire_min, current->fmq_wire_max,
                                   gateway->fmq_wire_min, gateway->fmq_wire_max, &selected) &&
         flow_fmq_u16_intersection(candidate->fmq_wire_min, candidate->fmq_wire_max,
                                   gateway->fmq_wire_min, gateway->fmq_wire_max, &selected) &&
         flow_fmq_minor_intersection(&current->tfmp, &gateway->tfmp, &selected) &&
         flow_fmq_minor_intersection(&candidate->tfmp, &gateway->tfmp, &selected);
}

int turbo_flow_fmq_compatibility_evaluate(const turbo_flow_fmq_compatibility_contract_t *current,
                                          const turbo_flow_fmq_compatibility_contract_t *candidate,
                                          const turbo_flow_fmq_compatibility_contract_t *gateway,
                                          uint64_t required_capabilities,
                                          turbo_flow_fmq_compatibility_result_t *result) {
  uint16_t wire = 0u;
  uint16_t tfmp = 0u;
  uint16_t store = 0u;
  uint16_t yaml = 0u;
  int wire_ok;
  int tfmp_ok;
  int store_ok;
  int yaml_ok;
  if (!flow_fmq_contract_valid(current) || !flow_fmq_contract_valid(candidate) ||
      (gateway && !flow_fmq_contract_valid(gateway)) || !result || result->size < sizeof(*result))
    return TURBO_EINVAL;
  *result = (turbo_flow_fmq_compatibility_result_t)TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT;
  result->missing_capabilities =
      required_capabilities & ~(current->capabilities & candidate->capabilities);
  if (result->missing_capabilities != 0u) {
    result->reason = TURBO_FLOW_FMQ_MISSING_CAPABILITY;
    return TURBO_OK;
  }
  wire_ok = flow_fmq_u16_intersection(current->fmq_wire_min, current->fmq_wire_max,
                                      candidate->fmq_wire_min, candidate->fmq_wire_max, &wire);
  tfmp_ok = flow_fmq_minor_intersection(&current->tfmp, &candidate->tfmp, &tfmp);
  store_ok =
      flow_fmq_store_intersection(&current->management_store, &candidate->management_store, &store);
  yaml_ok = flow_fmq_minor_intersection(&current->yaml_schema, &candidate->yaml_schema, &yaml);
  if (!store_ok || !yaml_ok) {
    result->mode = TURBO_FLOW_FMQ_ROLLOUT_STOP_THE_WORLD;
    result->reason = !store_ok ? TURBO_FLOW_FMQ_STORE_GAP : TURBO_FLOW_FMQ_YAML_GAP;
    return TURBO_OK;
  }
  result->negotiated_store_minor = store;
  result->negotiated_yaml_minor = yaml;
  if (wire_ok && tfmp_ok) {
    result->mode = TURBO_FLOW_FMQ_ROLLOUT_DIRECT;
    result->reason = TURBO_FLOW_FMQ_COMPATIBLE;
    result->negotiated_wire = wire;
    result->negotiated_tfmp_minor = tfmp;
    return TURBO_OK;
  }
  result->reason = !wire_ok ? TURBO_FLOW_FMQ_WIRE_GAP : TURBO_FLOW_FMQ_TFMP_GAP;
  if (flow_fmq_gateway_protocols(current, candidate, gateway)) {
    result->mode = TURBO_FLOW_FMQ_ROLLOUT_GATEWAY;
    return TURBO_OK;
  }
  result->mode = TURBO_FLOW_FMQ_ROLLOUT_STOP_THE_WORLD;
  return TURBO_OK;
}
