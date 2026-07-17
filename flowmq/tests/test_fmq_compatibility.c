#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_fmq_compatibility.h"

#include <string.h>

static turbo_flow_fmq_compatibility_contract_t
compatibility_contract(const char *release, uint16_t wire_min, uint16_t wire_max,
                       uint16_t tfmp_major, uint16_t tfmp_min, uint16_t tfmp_max,
                       uint16_t store_major, uint16_t store_read_min, uint16_t store_read_max,
                       uint16_t store_write_min, uint16_t store_write_max) {
  turbo_flow_fmq_compatibility_contract_t contract = TURBO_FLOW_FMQ_COMPATIBILITY_CONTRACT_INIT;
  memcpy(contract.release, release, strlen(release) + 1u);
  contract.fmq_wire_min = wire_min;
  contract.fmq_wire_max = wire_max;
  contract.tfmp.major = tfmp_major;
  contract.tfmp.minor_min = tfmp_min;
  contract.tfmp.minor_max = tfmp_max;
  contract.management_store.major = store_major;
  contract.management_store.read_minor_min = store_read_min;
  contract.management_store.read_minor_max = store_read_max;
  contract.management_store.write_minor_min = store_write_min;
  contract.management_store.write_minor_max = store_write_max;
  contract.yaml_schema.major = 1u;
  contract.yaml_schema.minor_min = 0u;
  contract.yaml_schema.minor_max = 0u;
  contract.capabilities = TURBO_FLOW_FMQ_CAPABILITY_BIT(1u) | TURBO_FLOW_FMQ_CAPABILITY_BIT(2u) |
                          TURBO_FLOW_FMQ_CAPABILITY_BIT(6u);
  return contract;
}

spec("fmq_compatibility") {
  it("selects a direct mixed-version window without widening FMQ v2") {
    turbo_flow_fmq_compatibility_contract_t current =
        compatibility_contract("2.0.0", 2u, 2u, 1u, 0u, 0u, 1u, 0u, 0u, 0u, 0u);
    turbo_flow_fmq_compatibility_contract_t candidate =
        compatibility_contract("2.1.0", 2u, 2u, 1u, 0u, 1u, 1u, 0u, 1u, 0u, 1u);
    turbo_flow_fmq_compatibility_result_t result = TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT;

    check_int_eq(turbo_flow_fmq_compatibility_evaluate(&current, &candidate, NULL,
                                                       TURBO_FLOW_FMQ_CAPABILITY_BIT(1u) |
                                                           TURBO_FLOW_FMQ_CAPABILITY_BIT(6u),
                                                       &result),
                 TURBO_OK);
    check_int_eq(result.mode, TURBO_FLOW_FMQ_ROLLOUT_DIRECT);
    check_int_eq(result.reason, TURBO_FLOW_FMQ_COMPATIBLE);
    check_uint_eq(result.negotiated_wire, 2u);
    check_uint_eq(result.negotiated_tfmp_minor, 0u);
    check_uint_eq(result.negotiated_store_minor, 0u);
    check_uint_eq(result.negotiated_yaml_minor, 0u);
  }

  it("requires a stop when a new durable writer has no shared schema") {
    turbo_flow_fmq_compatibility_contract_t current =
        compatibility_contract("2.0.0", 2u, 2u, 1u, 0u, 0u, 1u, 0u, 0u, 0u, 0u);
    turbo_flow_fmq_compatibility_contract_t candidate =
        compatibility_contract("2.1.0", 2u, 2u, 1u, 0u, 1u, 1u, 0u, 1u, 1u, 1u);
    turbo_flow_fmq_compatibility_result_t result = TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT;

    check_int_eq(turbo_flow_fmq_compatibility_evaluate(&current, &candidate, NULL, 0u, &result),
                 TURBO_OK);
    check_int_eq(result.mode, TURBO_FLOW_FMQ_ROLLOUT_STOP_THE_WORLD);
    check_int_eq(result.reason, TURBO_FLOW_FMQ_STORE_GAP);
  }

  it("uses only an explicit dual-stack gateway for a wire gap") {
    turbo_flow_fmq_compatibility_contract_t current =
        compatibility_contract("2.0.0", 2u, 2u, 1u, 0u, 0u, 1u, 0u, 0u, 0u, 0u);
    turbo_flow_fmq_compatibility_contract_t candidate =
        compatibility_contract("3.0.0", 3u, 3u, 1u, 0u, 0u, 1u, 0u, 0u, 0u, 0u);
    turbo_flow_fmq_compatibility_contract_t gateway =
        compatibility_contract("2-to-3", 2u, 3u, 1u, 0u, 0u, 1u, 0u, 0u, 0u, 0u);
    turbo_flow_fmq_compatibility_result_t result = TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT;

    check_int_eq(turbo_flow_fmq_compatibility_evaluate(&current, &candidate, NULL, 0u, &result),
                 TURBO_OK);
    check_int_eq(result.mode, TURBO_FLOW_FMQ_ROLLOUT_STOP_THE_WORLD);
    check_int_eq(result.reason, TURBO_FLOW_FMQ_WIRE_GAP);
    result = (turbo_flow_fmq_compatibility_result_t)TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_compatibility_evaluate(&current, &candidate, &gateway, 0u, &result),
                 TURBO_OK);
    check_int_eq(result.mode, TURBO_FLOW_FMQ_ROLLOUT_GATEWAY);
    check_int_eq(result.reason, TURBO_FLOW_FMQ_WIRE_GAP);
  }

  it("fails the rollout contract when either release lacks a required capability") {
    turbo_flow_fmq_compatibility_contract_t current =
        compatibility_contract("2.0.0", 2u, 2u, 1u, 0u, 0u, 1u, 0u, 0u, 0u, 0u);
    turbo_flow_fmq_compatibility_contract_t candidate = current;
    turbo_flow_fmq_compatibility_result_t result = TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT;
    memcpy(candidate.release, "2.0.1", sizeof("2.0.1"));

    check_int_eq(turbo_flow_fmq_compatibility_evaluate(&current, &candidate, NULL,
                                                       TURBO_FLOW_FMQ_CAPABILITY_BIT(9u), &result),
                 TURBO_OK);
    check_int_eq(result.mode, TURBO_FLOW_FMQ_ROLLOUT_INCOMPATIBLE);
    check_int_eq(result.reason, TURBO_FLOW_FMQ_MISSING_CAPABILITY);
    check_uint_eq(result.missing_capabilities, TURBO_FLOW_FMQ_CAPABILITY_BIT(9u));
  }
}
