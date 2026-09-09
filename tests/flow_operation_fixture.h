#ifndef FLOW_OPERATION_FIXTURE_H
#define FLOW_OPERATION_FIXTURE_H

#include "turbo_flow.h"

typedef struct flow_test_operation_s {
  turbo_flow_operation_descriptor_t descriptor;
  turbo_flow_operation_provider_registration_t provider;
} flow_test_operation_t;

/* Inline Message -> Message by default. Non-default state, execution and runtime
 * contracts belong at each caller; this fixture never reads a graph. */
static inline flow_test_operation_t flow_test_operation_init(const char *operation_name,
                                                             turbo_flow_stage_fn fn, void *ctx) {
  flow_test_operation_t operation = {0};
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  operation.descriptor.size = sizeof(operation.descriptor);
  operation.descriptor.name = operation_name;
  operation.descriptor.version = 1u;
  operation.descriptor.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.descriptor.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.descriptor.input_type = "Message";
  operation.descriptor.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.descriptor.output_type = "Message";
  operation.descriptor.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.descriptor.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_PRIVATE;
  operation.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_TASK;
  operation.descriptor.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.descriptor.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  operation.descriptor.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  provider.operation_name = operation_name;
  provider.fn = fn;
  provider.ctx = ctx;
  operation.provider = provider;
  return operation;
}

static inline int flow_test_operation_register(turbo_flow_t *flow,
                                               const flow_test_operation_t *operation) {
  int rc = turbo_flow_register_operation(flow, &operation->descriptor);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_register_operation_provider(flow, &operation->provider);
}

#endif
