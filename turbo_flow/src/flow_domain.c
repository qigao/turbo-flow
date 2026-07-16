#include "flow_internal.h"

#include <string.h>

static int flow_domain_valid(turbo_flow_domain_t domain) {
  return domain >= TURBO_FLOW_DOMAIN_DATA && domain <= TURBO_FLOW_DOMAIN_MANAGEMENT;
}

void flow_primitive_registration_destroy(flow_primitive_registration_t *primitive) {
  if (!primitive) return;
  tstr_freep(&primitive->name);
  tstr_freep(&primitive->type_name);
  memset(&primitive->descriptor, 0, sizeof(primitive->descriptor));
}

void flow_operation_registration_destroy(flow_operation_registration_t *operation) {
  if (!operation) return;
  tstr_freep(&operation->name);
  tstr_freep(&operation->input_type);
  tstr_freep(&operation->output_type);
  tstr_freep(&operation->resource_type);
  memset(&operation->descriptor, 0, sizeof(operation->descriptor));
}

int flow_find_primitive_index(const turbo_flow_t *flow, const char *name) {
  size_t i;
  if (!flow || !name) return -1;
  for (i = 0; i < turbo_vec_size(&flow->primitives); ++i) {
    const flow_primitive_registration_t *primitive =
        (const flow_primitive_registration_t *)turbo_vec_at_const(&flow->primitives, i);
    if (primitive && primitive->name && strcmp(primitive->name, name) == 0) return (int)i;
  }
  return -1;
}

int flow_find_operation_index(const turbo_flow_t *flow, const char *name) {
  size_t i;
  if (!flow || !name) return -1;
  for (i = 0; i < turbo_vec_size(&flow->operations); ++i) {
    const flow_operation_registration_t *operation =
        (const flow_operation_registration_t *)turbo_vec_at_const(&flow->operations, i);
    if (operation && operation->name && strcmp(operation->name, name) == 0) return (int)i;
  }
  return -1;
}

const turbo_flow_operation_runtime_contract_t *
flow_stage_operation_runtime(const turbo_flow_t *flow, const flow_stage_plan_impl_t *stage) {
  const turbo_flow_operation_descriptor_t *operation;
  (void)flow;
  operation = flow_stage_operation_descriptor(stage);
  return operation ? &operation->runtime : NULL;
}

const turbo_flow_operation_descriptor_t *
flow_stage_operation_descriptor(const flow_stage_plan_impl_t *stage) {
  return stage && stage->operation_resolved ? &stage->resolved_operation : NULL;
}

int turbo_flow_register_primitive(turbo_flow_t *flow,
                                  const turbo_flow_primitive_descriptor_t *descriptor) {
  flow_primitive_registration_t primitive;

  if (!flow || !descriptor || descriptor->size < sizeof(*descriptor) || !descriptor->name ||
      descriptor->name[0] == '\0' || !descriptor->type_name || descriptor->type_name[0] == '\0' ||
      descriptor->version == 0 || !flow_domain_valid(descriptor->domain) ||
      descriptor->kind < TURBO_FLOW_PRIMITIVE_VALUE ||
      descriptor->kind > TURBO_FLOW_PRIMITIVE_RESOURCE) {
    return TURBO_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, TURBO_EBUSY, 0, 0,
                                     "cannot register primitive after compile");
  }
  if (flow_find_primitive_index(flow, descriptor->name) >= 0) {
    return flow_set_error_keep_state(flow, TURBO_EALREADY, 0, 0, "duplicate primitive");
  }

  memset(&primitive, 0, sizeof(primitive));
  primitive.name = tstr_dup(descriptor->name);
  primitive.type_name = tstr_dup(descriptor->type_name);
  if (!primitive.name || !primitive.type_name) {
    flow_primitive_registration_destroy(&primitive);
    return flow_set_error(flow, TURBO_ENOMEM, 0, 0, "out of memory");
  }
  primitive.descriptor = *descriptor;
  primitive.descriptor.name = primitive.name;
  primitive.descriptor.type_name = primitive.type_name;
  if (turbo_vec_push(&flow->primitives, &primitive) != TURBO_OK) {
    flow_primitive_registration_destroy(&primitive);
    return flow_set_error(flow, TURBO_ENOMEM, 0, 0, "out of memory");
  }
  return TURBO_OK;
}

static int flow_runtime_contract_valid(const turbo_flow_operation_runtime_contract_t *runtime) {
  const uint32_t valid_settlement =
      TURBO_FLOW_SETTLEMENT_COMPLETE | TURBO_FLOW_SETTLEMENT_RETRY | TURBO_FLOW_SETTLEMENT_REQUEUE |
      TURBO_FLOW_SETTLEMENT_DEAD_LETTER | TURBO_FLOW_SETTLEMENT_PROTOCOL_ACK |
      TURBO_FLOW_SETTLEMENT_CANCELED;
  if (!runtime || runtime->handoff < TURBO_FLOW_HANDOFF_DIRECT ||
      runtime->handoff > TURBO_FLOW_HANDOFF_BOUNDED ||
      runtime->ordering < TURBO_FLOW_ORDERING_UNORDERED ||
      runtime->ordering > TURBO_FLOW_ORDERING_PRESERVE_INPUT ||
      runtime->backpressure < TURBO_FLOW_BACKPRESSURE_NONE ||
      runtime->backpressure > TURBO_FLOW_BACKPRESSURE_DROP_OLDEST ||
      runtime->cancellation < TURBO_FLOW_CANCELLATION_NONE ||
      runtime->cancellation > TURBO_FLOW_CANCELLATION_COOPERATIVE ||
      runtime->error_mode < TURBO_FLOW_ERROR_PROPAGATE ||
      runtime->error_mode > TURBO_FLOW_ERROR_SETTLE ||
      runtime->deadline_ms > TURBO_FLOW_OPERATION_MAX_DEADLINE_MS ||
      (runtime->settlement & ~valid_settlement) != 0) {
    return 0;
  }
  if (runtime->handoff == TURBO_FLOW_HANDOFF_DIRECT) {
    if (runtime->capacity != 0 || runtime->backpressure != TURBO_FLOW_BACKPRESSURE_NONE) return 0;
  } else if (runtime->capacity == 0 ||
             runtime->capacity > TURBO_FLOW_OPERATION_MAX_SEGMENT_CAPACITY ||
             (runtime->capacity & (runtime->capacity - 1u)) != 0u ||
             runtime->backpressure == TURBO_FLOW_BACKPRESSURE_NONE) {
    return 0;
  }
  if (runtime->error_mode == TURBO_FLOW_ERROR_SETTLE && runtime->settlement == 0) return 0;
  if ((runtime->settlement & TURBO_FLOW_SETTLEMENT_RETRY) != 0 &&
      runtime->error_mode != TURBO_FLOW_ERROR_RETRY) {
    return 0;
  }
  return 1;
}

static int flow_operation_descriptor_valid(const turbo_flow_operation_descriptor_t *descriptor) {
  const uint32_t valid_flags =
      TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE;
  const uint32_t valid_exec = TURBO_FLOW_OPERATION_EXEC_INLINE | TURBO_FLOW_OPERATION_EXEC_THREAD |
                              TURBO_FLOW_OPERATION_EXEC_CORO;
  int has_resource;

  if (!descriptor || descriptor->size < sizeof(*descriptor) ||
      !descriptor->name || descriptor->name[0] == '\0' || descriptor->version == 0 ||
      !flow_domain_valid(descriptor->domain) || descriptor->flags == 0 ||
      (descriptor->flags & ~valid_flags) != 0 || descriptor->execution_mask == 0 ||
      !(descriptor->flags & (TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_STAGE)) ||
      (descriptor->execution_mask & ~valid_exec) != 0 ||
      descriptor->scope.data < TURBO_FLOW_DATA_SCOPE_NONE ||
      descriptor->scope.data > TURBO_FLOW_DATA_SCOPE_SNAPSHOT ||
      descriptor->scope.state < TURBO_FLOW_STATE_SCOPE_NONE ||
      descriptor->scope.state > TURBO_FLOW_STATE_SCOPE_PROTOCOL_SESSION ||
      descriptor->scope.lifetime < TURBO_FLOW_LIFETIME_CALL ||
      descriptor->scope.lifetime > TURBO_FLOW_LIFETIME_PERSISTENT ||
      descriptor->scope.concurrency < TURBO_FLOW_CONCURRENCY_INLINE_LANE ||
      descriptor->scope.concurrency > TURBO_FLOW_CONCURRENCY_LOCK_FREE_SNAPSHOT ||
      descriptor->scope.authority < TURBO_FLOW_AUTHORITY_PURE ||
      descriptor->scope.authority > TURBO_FLOW_AUTHORITY_OWNER_COMMAND ||
      !flow_runtime_contract_valid(&descriptor->runtime)) {
    return 0;
  }
  if ((descriptor->input_type == NULL) != (descriptor->input_domain == TURBO_FLOW_DOMAIN_NONE) ||
      (descriptor->output_type == NULL) != (descriptor->output_domain == TURBO_FLOW_DOMAIN_NONE)) {
    return 0;
  }
  if ((descriptor->input_type &&
       (!flow_domain_valid(descriptor->input_domain) || descriptor->input_type[0] == '\0')) ||
      (descriptor->output_type &&
       (!flow_domain_valid(descriptor->output_domain) || descriptor->output_type[0] == '\0'))) {
    return 0;
  }
  has_resource = descriptor->resource_type != NULL;
  if (has_resource != (descriptor->resource_domain != TURBO_FLOW_DOMAIN_NONE) ||
      (has_resource &&
       (!flow_domain_valid(descriptor->resource_domain) || descriptor->resource_type[0] == '\0'))) {
    return 0;
  }
  if ((descriptor->scope.state == TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER ||
       descriptor->scope.state == TURBO_FLOW_STATE_SCOPE_PROTOCOL_SESSION) &&
      !has_resource) {
    return 0;
  }
  if (descriptor->scope.authority == TURBO_FLOW_AUTHORITY_OWNER_COMMAND &&
      descriptor->domain != TURBO_FLOW_DOMAIN_MANAGEMENT) {
    return 0;
  }
  if (!(descriptor->flags & TURBO_FLOW_OPERATION_BRIDGE) &&
      ((descriptor->input_domain != TURBO_FLOW_DOMAIN_NONE &&
        descriptor->input_domain != descriptor->domain) ||
       (descriptor->output_domain != TURBO_FLOW_DOMAIN_NONE &&
        descriptor->output_domain != descriptor->domain))) {
    return 0;
  }
  return 1;
}

int turbo_flow_register_operation(turbo_flow_t *flow,
                                  const turbo_flow_operation_descriptor_t *descriptor) {
  flow_operation_registration_t operation;
  turbo_flow_operation_descriptor_t normalized;

  if (!flow || !flow_operation_descriptor_valid(descriptor)) return TURBO_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, TURBO_EBUSY, 0, 0,
                                     "cannot register operation after compile");
  }
  if (flow_find_operation_index(flow, descriptor->name) >= 0) {
    return flow_set_error_keep_state(flow, TURBO_EALREADY, 0, 0, "duplicate operation");
  }
  memset(&operation, 0, sizeof(operation));
  normalized = *descriptor;
  normalized.size = sizeof(normalized);
  operation.name = tstr_dup(descriptor->name);
  if (descriptor->input_type) operation.input_type = tstr_dup(descriptor->input_type);
  if (descriptor->output_type) operation.output_type = tstr_dup(descriptor->output_type);
  if (descriptor->resource_type) operation.resource_type = tstr_dup(descriptor->resource_type);
  if (!operation.name || (descriptor->input_type && !operation.input_type) ||
      (descriptor->output_type && !operation.output_type) ||
      (descriptor->resource_type && !operation.resource_type)) {
    flow_operation_registration_destroy(&operation);
    return flow_set_error(flow, TURBO_ENOMEM, 0, 0, "out of memory");
  }
  operation.descriptor = normalized;
  operation.descriptor.name = operation.name;
  operation.descriptor.input_type = operation.input_type;
  operation.descriptor.output_type = operation.output_type;
  operation.descriptor.resource_type = operation.resource_type;
  if (turbo_vec_push(&flow->operations, &operation) != TURBO_OK) {
    flow_operation_registration_destroy(&operation);
    return flow_set_error(flow, TURBO_ENOMEM, 0, 0, "out of memory");
  }
  return TURBO_OK;
}

size_t turbo_flow_primitive_count(const turbo_flow_t *flow) {
  return flow ? turbo_vec_size(&flow->primitives) : 0;
}

size_t turbo_flow_operation_count(const turbo_flow_t *flow) {
  return flow ? turbo_vec_size(&flow->operations) : 0;
}

const turbo_flow_primitive_descriptor_t *turbo_flow_primitive_at(const turbo_flow_t *flow,
                                                                 size_t index) {
  const flow_primitive_registration_t *primitive;
  if (!flow) return NULL;
  primitive = (const flow_primitive_registration_t *)turbo_vec_at_const(&flow->primitives, index);
  return primitive ? &primitive->descriptor : NULL;
}

const turbo_flow_operation_descriptor_t *turbo_flow_operation_at(const turbo_flow_t *flow,
                                                                 size_t index) {
  const flow_operation_registration_t *operation;
  if (!flow) return NULL;
  operation = (const flow_operation_registration_t *)turbo_vec_at_const(&flow->operations, index);
  return operation ? &operation->descriptor : NULL;
}

const turbo_flow_primitive_descriptor_t *turbo_flow_find_primitive(const turbo_flow_t *flow,
                                                                   const char *name) {
  int index = flow_find_primitive_index(flow, name);
  return index < 0 ? NULL : turbo_flow_primitive_at(flow, (size_t)index);
}

const turbo_flow_operation_descriptor_t *turbo_flow_find_operation(const turbo_flow_t *flow,
                                                                   const char *name) {
  int index = flow_find_operation_index(flow, name);
  return index < 0 ? NULL : turbo_flow_operation_at(flow, (size_t)index);
}
