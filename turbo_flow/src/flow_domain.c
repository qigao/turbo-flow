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
  vec_destroy(&operation->reflected_ports);
  memset(&operation->descriptor, 0, sizeof(operation->descriptor));
  operation->function = NULL;
  operation->abi = NULL;
  memset(&operation->callable, 0, sizeof(operation->callable));
  memset(&operation->projection, 0, sizeof(operation->projection));
  operation->reflected_lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;
  operation->reflected = 0;
}

void flow_module_registration_destroy(flow_module_registration_t *module) {
  size_t i;
  if (!module) return;
  tstr_freep(&module->name);
  for (i = 0; i < vec_size(&module->primitive_types); ++i) {
    tstr *value = (tstr *)vec_at(&module->primitive_types, i);
    if (value) tstr_freep(value);
  }
  for (i = 0; i < vec_size(&module->operation_names); ++i) {
    tstr *value = (tstr *)vec_at(&module->operation_names, i);
    if (value) tstr_freep(value);
  }
  for (i = 0; i < vec_size(&module->requirements); ++i) {
    turbo_flow_module_requirement_t *requirement =
        (turbo_flow_module_requirement_t *)vec_at(&module->requirements, i);
    if (requirement && requirement->module_name) {
      tstr owned_name = (tstr)requirement->module_name;
      tstr_freep(&owned_name);
      requirement->module_name = NULL;
    }
  }
  vec_destroy(&module->primitive_types);
  vec_destroy(&module->operation_names);
  vec_destroy(&module->requirements);
  memset(&module->descriptor, 0, sizeof(module->descriptor));
}

int flow_find_primitive_index(const turbo_flow_t *flow, const char *name) {
  size_t i;
  if (!flow || !name) return -1;
  for (i = 0; i < vec_size(&flow->primitives); ++i) {
    const flow_primitive_registration_t *primitive =
        (const flow_primitive_registration_t *)vec_at_const(&flow->primitives, i);
    if (primitive && primitive->name && strcmp(primitive->name, name) == 0) return (int)i;
  }
  return -1;
}

int flow_find_operation_index(const turbo_flow_t *flow, const char *name) {
  size_t i;
  if (!flow || !name) return -1;
  for (i = 0; i < vec_size(&flow->operations); ++i) {
    const flow_operation_registration_t *operation =
        (const flow_operation_registration_t *)vec_at_const(&flow->operations, i);
    if (operation && operation->name && strcmp(operation->name, name) == 0) return (int)i;
  }
  return -1;
}

const flow_operation_registration_t *
flow_find_operation_registration(const turbo_flow_t *flow, const char *name) {
  int index = flow_find_operation_index(flow, name);
  if (index < 0) return NULL;
  return (const flow_operation_registration_t *)vec_at_const(
      &flow->operations, (size_t)index);
}

int flow_find_module_index(const turbo_flow_t *flow, const char *name) {
  size_t i;
  if (!flow || !name) return -1;
  for (i = 0; i < vec_size(&flow->modules); ++i) {
    const flow_module_registration_t *module =
        (const flow_module_registration_t *)vec_at_const(&flow->modules, i);
    if (module && module->name && strcmp(module->name, name) == 0) return (int)i;
  }
  return -1;
}

int flow_find_operation_export_module(const turbo_flow_t *flow, const char *operation_name) {
  size_t i;
  if (!flow || !operation_name) return -1;
  for (i = 0; i < vec_size(&flow->modules); ++i) {
    const flow_module_registration_t *module =
        (const flow_module_registration_t *)vec_at_const(&flow->modules, i);
    size_t j;
    if (!module) continue;
    for (j = 0; j < vec_size(&module->operation_names); ++j) {
      const tstr *name = (const tstr *)vec_at_const(&module->operation_names, j);
      if (name && *name && strcmp(*name, operation_name) == 0) return (int)i;
    }
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
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register primitive after compile");
  }
  if (flow_find_primitive_index(flow, descriptor->name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "duplicate primitive");
  }

  memset(&primitive, 0, sizeof(primitive));
  primitive.name = tstr_dup(descriptor->name);
  primitive.type_name = tstr_dup(descriptor->type_name);
  if (!primitive.name || !primitive.type_name) {
    flow_primitive_registration_destroy(&primitive);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  primitive.descriptor = *descriptor;
  primitive.descriptor.name = primitive.name;
  primitive.descriptor.type_name = primitive.type_name;
  if (turbo_flow_stl_error(vec_push(&flow->primitives, &primitive)) != SALTS_OK) {
    flow_primitive_registration_destroy(&primitive);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

static int flow_runtime_contract_valid(const turbo_flow_operation_runtime_contract_t *runtime) {
  const uint32_t valid_settlement =
      TURBO_FLOW_SETTLEMENT_COMPLETE | TURBO_FLOW_SETTLEMENT_RETRY | TURBO_FLOW_SETTLEMENT_REQUEUE |
      TURBO_FLOW_SETTLEMENT_DEAD_LETTER | TURBO_FLOW_SETTLEMENT_ACKNOWLEDGE |
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
      descriptor->scope.state > TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER ||
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
  if ((!has_resource &&
       (descriptor->resource_min_version != 0u || descriptor->resource_max_version != 0u)) ||
      (descriptor->resource_min_version == 0u && descriptor->resource_max_version != 0u) ||
      (descriptor->resource_max_version != 0u &&
       descriptor->resource_max_version < descriptor->resource_min_version)) {
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

  if (!flow || !descriptor || descriptor->size != sizeof(*descriptor)) {
    return SALTS_EINVAL;
  }
  if (!flow_operation_descriptor_valid(descriptor)) return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register operation after compile");
  }
  if (flow_find_operation_index(flow, descriptor->name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "duplicate operation");
  }
  memset(&operation, 0, sizeof(operation));
  operation.name = tstr_dup(descriptor->name);
  if (descriptor->input_type) operation.input_type = tstr_dup(descriptor->input_type);
  if (descriptor->output_type) operation.output_type = tstr_dup(descriptor->output_type);
  if (descriptor->resource_type) operation.resource_type = tstr_dup(descriptor->resource_type);
  if (!operation.name || (descriptor->input_type && !operation.input_type) ||
      (descriptor->output_type && !operation.output_type) ||
      (descriptor->resource_type && !operation.resource_type)) {
    flow_operation_registration_destroy(&operation);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  operation.descriptor = *descriptor;
  operation.descriptor.name = operation.name;
  operation.descriptor.input_type = operation.input_type;
  operation.descriptor.output_type = operation.output_type;
  operation.descriptor.resource_type = operation.resource_type;
  if (turbo_flow_stl_error(vec_push(&flow->operations, &operation)) != SALTS_OK) {
    flow_operation_registration_destroy(&operation);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

static int flow_reflected_port_matches(
    const turbo_flow_operation_port_binding_t *port,
    const cmeta_function_desc *function) {
  const cmeta_type_desc *expected_type = NULL;
  cmeta_param_flags direction = CMETA_PARAM_UNKNOWN;

  if (!port || port->size != sizeof(*port) ||
      !flow_domain_valid(port->domain) ||
      (port->direction != TURBO_FLOW_OPERATION_PORT_INPUT &&
       port->direction != TURBO_FLOW_OPERATION_PORT_OUTPUT) ||
      (port->storage != TURBO_FLOW_OPERATION_STORAGE_DIRECT &&
       port->storage != TURBO_FLOW_OPERATION_STORAGE_POINTEE) ||
      !cmeta_data_desc_valid(port->data)) {
    return 0;
  }

  if (port->value_kind == TURBO_FLOW_OPERATION_VALUE_RETURN) {
    if (port->direction != TURBO_FLOW_OPERATION_PORT_OUTPUT ||
        port->storage != TURBO_FLOW_OPERATION_STORAGE_DIRECT ||
        port->parameter_index != SIZE_MAX ||
        !function || function->return_type->kind == CMETA_T_VOID) {
      return 0;
    }
    expected_type = function->return_type;
  } else if (port->value_kind == TURBO_FLOW_OPERATION_VALUE_PARAMETER) {
    const cmeta_param_desc *param;
    if (!function || port->parameter_index >= function->param_count) return 0;
    param = cmeta_function_param(function, port->parameter_index);
    if (!param || !cmeta_param_direction_known(param)) return 0;
    direction = param->flags & CMETA_PARAM_DIRECTION_MASK;
    if (port->direction == TURBO_FLOW_OPERATION_PORT_INPUT &&
        (direction & CMETA_PARAM_IN) == 0u) {
      return 0;
    }
    if (port->direction == TURBO_FLOW_OPERATION_PORT_OUTPUT &&
        (direction & CMETA_PARAM_OUT) == 0u) {
      return 0;
    }
    if (port->storage == TURBO_FLOW_OPERATION_STORAGE_DIRECT) {
      expected_type = param->type;
    } else {
      if (!param->type || param->type->kind != CMETA_T_POINTER ||
          !param->type->pointee) {
        return 0;
      }
      expected_type = param->type->pointee;
    }
  } else {
    return 0;
  }

  return expected_type &&
         cmeta_type_equal(port->data->storage_type, expected_type);
}

static int flow_reflected_ports_valid(
    const turbo_flow_reflected_operation_registration_t *registration) {
  const cmeta_function_desc *function = registration->function;
  size_t i;
  size_t param_index;
  int return_mapped = function->return_type->kind == CMETA_T_VOID;

  if ((registration->port_count == 0u) != (registration->ports == NULL))
    return 0;

  for (i = 0u; i < registration->port_count; ++i) {
    const turbo_flow_operation_port_binding_t *port = &registration->ports[i];
    size_t j;
    if (!flow_reflected_port_matches(port, function)) return 0;

    if (port->value_kind == TURBO_FLOW_OPERATION_VALUE_RETURN)
      return_mapped = 1;

    for (j = 0u; j < i; ++j) {
      const turbo_flow_operation_port_binding_t *previous =
          &registration->ports[j];
      if (previous->direction == port->direction &&
          previous->port_index == port->port_index) {
        return 0;
      }
      if (previous->direction == port->direction &&
          previous->value_kind == port->value_kind &&
          previous->parameter_index == port->parameter_index) {
        return 0;
      }
    }
  }

  if (!return_mapped) return 0;

  for (param_index = 0u; param_index < function->param_count; ++param_index) {
    const cmeta_param_desc *param = cmeta_function_param(function, param_index);
    cmeta_param_flags direction;
    int has_input = 0;
    int has_output = 0;
    if (!param || !cmeta_param_direction_known(param)) return 0;
    direction = param->flags & CMETA_PARAM_DIRECTION_MASK;
    for (i = 0u; i < registration->port_count; ++i) {
      const turbo_flow_operation_port_binding_t *port = &registration->ports[i];
      if (port->value_kind != TURBO_FLOW_OPERATION_VALUE_PARAMETER ||
          port->parameter_index != param_index) {
        continue;
      }
      if (port->direction == TURBO_FLOW_OPERATION_PORT_INPUT) has_input = 1;
      if (port->direction == TURBO_FLOW_OPERATION_PORT_OUTPUT) has_output = 1;
    }
    if (((direction & CMETA_PARAM_IN) != 0u) != !!has_input ||
        ((direction & CMETA_PARAM_OUT) != 0u) != !!has_output) {
      return 0;
    }
  }
  return 1;
}

static int flow_reflected_unary_ports(
    const turbo_flow_reflected_operation_registration_t *registration,
    const turbo_flow_operation_port_binding_t **input_out,
    const turbo_flow_operation_port_binding_t **output_out) {
  const turbo_flow_operation_port_binding_t *input = NULL;
  const turbo_flow_operation_port_binding_t *output = NULL;
  size_t i;

  for (i = 0u; i < registration->port_count; ++i) {
    const turbo_flow_operation_port_binding_t *port = &registration->ports[i];
    if (port->direction == TURBO_FLOW_OPERATION_PORT_INPUT) {
      if (input) return 0;
      input = port;
    } else {
      if (output) return 0;
      output = port;
    }
  }
  if (!input || !output ||
      input->value_kind != TURBO_FLOW_OPERATION_VALUE_PARAMETER ||
      input->storage != TURBO_FLOW_OPERATION_STORAGE_DIRECT ||
      input->parameter_index != 0u ||
      output->value_kind != TURBO_FLOW_OPERATION_VALUE_RETURN ||
      output->storage != TURBO_FLOW_OPERATION_STORAGE_DIRECT) {
    return 0;
  }
  if (input_out) *input_out = input;
  if (output_out) *output_out = output;
  return 1;
}

int turbo_flow_register_reflected_operation(
    turbo_flow_t *flow,
    const turbo_flow_reflected_operation_registration_t *registration) {
  flow_operation_registration_t operation;
  const turbo_flow_operation_descriptor_t *descriptor;
  const turbo_flow_operation_port_binding_t *input_port = NULL;
  const turbo_flow_operation_port_binding_t *output_port = NULL;
  cflow_function_projection_status projection_status;
  size_t i;

  if (!flow || !registration ||
      registration->size != sizeof(*registration) ||
      !(descriptor = registration->operation) ||
      !flow_operation_descriptor_valid(descriptor) ||
      !registration->function ||
      !cmeta_function_desc_valid(registration->function) ||
      !registration->abi ||
      !cmeta_function_abi_desc_valid(registration->abi) ||
      !cmeta_function_desc_equal(registration->abi->function,
                                 registration->function) ||
      !flow_reflected_ports_valid(registration) ||
      (registration->lowering != TURBO_FLOW_REFLECTED_LOWERING_NONE &&
       registration->lowering != TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP)) {
    return SALTS_EINVAL;
  }

  /* Reflected function/data descriptors are the native type/effect authority. */
  if (descriptor->input_type || descriptor->output_type ||
      descriptor->input_domain != TURBO_FLOW_DOMAIN_NONE ||
      descriptor->output_domain != TURBO_FLOW_DOMAIN_NONE) {
    return SALTS_EINVAL;
  }

  if (flow->state == TURBO_FLOW_STATE_COMPILED ||
      flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(
        flow, SALTS_EBUSY, 0, 0,
        "cannot register reflected operation after compile");
  }
  if (flow_find_operation_index(flow, descriptor->name) >= 0) {
    return flow_set_error_keep_state(
        flow, SALTS_EALREADY, 0, 0, "duplicate operation");
  }

  memset(&operation, 0, sizeof(operation));
  if (turbo_flow_stl_error(vec_init_bytes(
          &operation.reflected_ports,
          sizeof(turbo_flow_operation_port_binding_t),
          _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }

  operation.name = tstr_dup(descriptor->name);
  if (descriptor->resource_type)
    operation.resource_type = tstr_dup(descriptor->resource_type);
  if (!operation.name ||
      (descriptor->resource_type && !operation.resource_type)) {
    flow_operation_registration_destroy(&operation);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }

  operation.descriptor = *descriptor;
  operation.descriptor.name = operation.name;
  operation.descriptor.resource_type = operation.resource_type;
  operation.function = registration->function;
  operation.abi = registration->abi;
  operation.callable = registration->adapter;
  operation.reflected_lowering = registration->lowering;
  operation.reflected = 1;

  for (i = 0u; i < registration->port_count; ++i) {
    if (turbo_flow_stl_error(vec_push(
            &operation.reflected_ports, &registration->ports[i])) != SALTS_OK) {
      flow_operation_registration_destroy(&operation);
      return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    }
  }

  if (flow_reflected_unary_ports(registration, &input_port, &output_port)) {
    operation.input_type = tstr_dup(input_port->data->stable_id);
    operation.output_type = tstr_dup(output_port->data->stable_id);
    if (!operation.input_type || !operation.output_type) {
      flow_operation_registration_destroy(&operation);
      return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
    }
  }

  if (registration->lowering == TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP) {
    if (!cmeta_callable_contract_valid(registration->adapter) ||
        registration->adapter.meta.effects != registration->function->effects ||
        registration->adapter.meta.properties != registration->function->properties) {
      flow_operation_registration_destroy(&operation);
      return SALTS_EINVAL;
    }
    if (!input_port || !output_port) {
      flow_operation_registration_destroy(&operation);
      return SALTS_EINVAL;
    }
    projection_status = cflow_function_projection_admit(
        registration->function, registration->abi,
        registration->adapter, CFLOW_OP_MAP, &operation.projection);
    if (projection_status != CFLOW_FUNCTION_PROJECTION_OK) {
      flow_operation_registration_destroy(&operation);
      return flow_set_error_keep_state(
          flow, SALTS_ENOTSUP, 0, 0,
          cflow_function_projection_status_string(projection_status));
    }
    if (!cmeta_type_equal(operation.projection.input_type,
                          input_port->data->storage_type) ||
        !cmeta_type_equal(operation.projection.output_type,
                          output_port->data->storage_type)) {
      flow_operation_registration_destroy(&operation);
      return SALTS_EPROTO;
    }
  }

  if (turbo_flow_stl_error(vec_push(&flow->operations, &operation)) != SALTS_OK) {
    flow_operation_registration_destroy(&operation);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

int turbo_flow_reflected_operation(
    const turbo_flow_t *flow, const char *operation_name,
    turbo_flow_reflected_operation_view_t *out) {
  const flow_operation_registration_t *operation =
      flow_find_operation_registration(flow, operation_name);
  if (!out || out->size != sizeof(*out) || !operation_name)
    return SALTS_EINVAL;
  if (!operation || !operation->reflected) return SALTS_ENOENT;

  out->function = operation->function;
  out->abi = operation->abi;
  out->ports = vec_size(&operation->reflected_ports) != 0u
                   ? (const turbo_flow_operation_port_binding_t *)
                         vec_at_const(&operation->reflected_ports, 0u)
                   : NULL;
  out->port_count = vec_size(&operation->reflected_ports);
  out->lowering = operation->reflected_lowering;
  return SALTS_OK;
}


static int flow_module_string_array_valid(const char *const *values, size_t count) {
  size_t i;
  size_t j;
  if ((count == 0u) != (values == NULL)) return 0;
  for (i = 0; i < count; ++i) {
    if (!values[i] || values[i][0] == '\0') return 0;
    for (j = 0; j < i; ++j) {
      if (strcmp(values[i], values[j]) == 0) return 0;
    }
  }
  return 1;
}

static int flow_module_exports_operation(const flow_module_registration_t *module,
                                         const char *operation_name) {
  size_t i;
  if (!module || !operation_name) return 0;
  for (i = 0; i < vec_size(&module->operation_names); ++i) {
    const tstr *name = (const tstr *)vec_at_const(&module->operation_names, i);
    if (name && *name && strcmp(*name, operation_name) == 0) return 1;
  }
  return 0;
}

static int flow_module_exports_primitive_type(const flow_module_registration_t *module,
                                              const char *type_name) {
  size_t i;
  if (!module || !type_name) return 0;
  for (i = 0; i < vec_size(&module->primitive_types); ++i) {
    const tstr *name = (const tstr *)vec_at_const(&module->primitive_types, i);
    if (name && *name && strcmp(*name, type_name) == 0) return 1;
  }
  return 0;
}

static int flow_module_copy_string_array(vec_t *target, const char *const *values,
                                         size_t count) {
  size_t i;
  if (count != 0u && turbo_flow_stl_error(vec_reserve(target, count)) != SALTS_OK) return SALTS_ENOMEM;
  for (i = 0; i < count; ++i) {
    tstr value = tstr_dup(values[i]);
    if (!value) return SALTS_ENOMEM;
    if (turbo_flow_stl_error(vec_push(target, &value)) != SALTS_OK) {
      tstr_freep(&value);
      return SALTS_ENOMEM;
    }
  }
  return SALTS_OK;
}

int turbo_flow_register_module(turbo_flow_t *flow,
                               const turbo_flow_module_descriptor_t *descriptor) {
  const uint32_t valid_capabilities = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                      TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                                      TURBO_FLOW_MODULE_NATIVE_API;
  flow_module_registration_t module;
  size_t i;
  int rc;

  if (!flow || !descriptor || descriptor->size < sizeof(*descriptor) || !descriptor->name ||
      descriptor->name[0] == '\0' || descriptor->version == 0u ||
      descriptor->capability_flags == 0u ||
      (descriptor->capability_flags & ~valid_capabilities) != 0u ||
      !flow_module_string_array_valid(descriptor->primitive_types,
                                      descriptor->primitive_type_count) ||
      !flow_module_string_array_valid(descriptor->operation_names, descriptor->operation_count) ||
      (descriptor->requirement_count == 0u) != (descriptor->requirements == NULL) ||
      (((descriptor->capability_flags & TURBO_FLOW_MODULE_GRAPH_OPERATIONS) != 0u) !=
       (descriptor->operation_count != 0u)) ||
      ((descriptor->capability_flags & TURBO_FLOW_MODULE_MANAGED_RESOURCES) != 0u &&
       descriptor->primitive_type_count == 0u)) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot register module after compile");
  }
  if (flow_find_module_index(flow, descriptor->name) >= 0) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0, "duplicate module");
  }
  for (i = 0; i < descriptor->operation_count; ++i) {
    if (flow_find_operation_index(flow, descriptor->operation_names[i]) < 0) {
      return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                       "module operation export is not registered");
    }
    if (flow_find_operation_export_module(flow, descriptor->operation_names[i]) >= 0) {
      return flow_set_error_keep_state(flow, SALTS_EALREADY, 0, 0,
                                       "operation already has a module owner");
    }
  }
  for (i = 0; i < descriptor->requirement_count; ++i) {
    const turbo_flow_module_requirement_t *requirement = &descriptor->requirements[i];
    const turbo_flow_module_descriptor_t *dependency;
    size_t j;
    if (requirement->size < sizeof(*requirement) || !requirement->module_name ||
        requirement->module_name[0] == '\0' || requirement->min_version == 0u ||
        (requirement->max_version != 0u &&
         requirement->max_version < requirement->min_version) ||
        (requirement->capability_flags & ~valid_capabilities) != 0u ||
        strcmp(requirement->module_name, descriptor->name) == 0) {
      return SALTS_EINVAL;
    }
    for (j = 0; j < i; ++j) {
      if (strcmp(requirement->module_name, descriptor->requirements[j].module_name) == 0) {
        return SALTS_EINVAL;
      }
    }
    dependency = turbo_flow_find_module(flow, requirement->module_name);
    if (!dependency) {
      return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                       "required module is not registered");
    }
    if (dependency->version < requirement->min_version ||
        (requirement->max_version != 0u && dependency->version > requirement->max_version) ||
        (dependency->capability_flags & requirement->capability_flags) !=
            requirement->capability_flags) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "required module contract is incompatible");
    }
  }

  memset(&module, 0, sizeof(module));
  if (turbo_flow_stl_error(vec_init_bytes(&module.primitive_types, sizeof(tstr), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&module.operation_names, sizeof(tstr), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(vec_init_bytes(&module.requirements, sizeof(turbo_flow_module_requirement_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    flow_module_registration_destroy(&module);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  module.name = tstr_dup(descriptor->name);
  if (!module.name) {
    flow_module_registration_destroy(&module);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  rc = flow_module_copy_string_array(&module.primitive_types, descriptor->primitive_types,
                                     descriptor->primitive_type_count);
  if (rc == SALTS_OK) {
    rc = flow_module_copy_string_array(&module.operation_names, descriptor->operation_names,
                                       descriptor->operation_count);
  }
  for (i = 0; rc == SALTS_OK && i < descriptor->requirement_count; ++i) {
    turbo_flow_module_requirement_t requirement = descriptor->requirements[i];
    requirement.module_name = tstr_dup(descriptor->requirements[i].module_name);
    if (!requirement.module_name || turbo_flow_stl_error(vec_push(&module.requirements, &requirement)) != SALTS_OK) {
      if (requirement.module_name) {
        tstr owned_name = (tstr)requirement.module_name;
        tstr_freep(&owned_name);
      }
      rc = SALTS_ENOMEM;
    }
  }
  if (rc != SALTS_OK) {
    flow_module_registration_destroy(&module);
    return flow_set_error(flow, rc, 0, 0, "out of memory");
  }
  module.descriptor = *descriptor;
  module.descriptor.name = module.name;
  module.descriptor.primitive_types =
      (const char *const *)vec_data_const(&module.primitive_types);
  module.descriptor.operation_names =
      (const char *const *)vec_data_const(&module.operation_names);
  module.descriptor.requirements = (const turbo_flow_module_requirement_t *)vec_data_const(
      &module.requirements);
  if (turbo_flow_stl_error(vec_push(&flow->modules, &module)) != SALTS_OK) {
    flow_module_registration_destroy(&module);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

static int flow_optional_string_equal(const char *left, const char *right) {
  if (!left || !right) return left == right;
  return strcmp(left, right) == 0;
}

static int flow_operation_contract_compatible(
    const turbo_flow_operation_descriptor_t *current,
    const turbo_flow_operation_descriptor_t *required) {
  return current && required && required->size == sizeof(*required) &&
         current->version == required->version && current->domain == required->domain &&
         current->input_domain == required->input_domain &&
         current->output_domain == required->output_domain &&
         current->resource_domain == required->resource_domain &&
         flow_optional_string_equal(current->input_type, required->input_type) &&
         flow_optional_string_equal(current->output_type, required->output_type) &&
         flow_optional_string_equal(current->resource_type, required->resource_type) &&
         current->scope.data == required->scope.data &&
         current->scope.state == required->scope.state &&
         current->scope.lifetime == required->scope.lifetime &&
         current->scope.concurrency == required->scope.concurrency &&
         current->scope.authority == required->scope.authority &&
         current->flags == required->flags &&
         current->execution_mask == required->execution_mask &&
         current->runtime.handoff == required->runtime.handoff &&
         current->runtime.ordering == required->runtime.ordering &&
         current->runtime.backpressure == required->runtime.backpressure &&
         current->runtime.cancellation == required->runtime.cancellation &&
         current->runtime.error_mode == required->runtime.error_mode &&
         current->runtime.capacity == required->runtime.capacity &&
         current->runtime.deadline_ms == required->runtime.deadline_ms &&
         current->runtime.settlement == required->runtime.settlement &&
         current->resource_min_version == required->resource_min_version &&
         current->resource_max_version == required->resource_max_version;
}

static int flow_module_contract_compatible(const turbo_flow_module_descriptor_t *current,
                                           const turbo_flow_module_descriptor_t *required) {
  size_t i;
  if (!current || !required || required->size < sizeof(*required) ||
      current->version != required->version ||
      current->capability_flags != required->capability_flags ||
      current->primitive_type_count != required->primitive_type_count ||
      current->operation_count != required->operation_count ||
      current->requirement_count != required->requirement_count) {
    return 0;
  }
  for (i = 0; i < required->primitive_type_count; ++i) {
    if (strcmp(current->primitive_types[i], required->primitive_types[i]) != 0) return 0;
  }
  for (i = 0; i < required->operation_count; ++i) {
    if (strcmp(current->operation_names[i], required->operation_names[i]) != 0) return 0;
  }
  for (i = 0; i < required->requirement_count; ++i) {
    const turbo_flow_module_requirement_t *left = &current->requirements[i];
    const turbo_flow_module_requirement_t *right = &required->requirements[i];
    if (right->size < sizeof(*right) || strcmp(left->module_name, right->module_name) != 0 ||
        left->min_version != right->min_version || left->max_version != right->max_version ||
        left->capability_flags != right->capability_flags) {
      return 0;
    }
  }
  return 1;
}

static void flow_operation_registry_rollback(turbo_flow_t *flow, size_t operation_count) {
  while (vec_size(&flow->operations) > operation_count) {
    size_t last = vec_size(&flow->operations) - 1u;
    flow_operation_registration_t *operation =
        (flow_operation_registration_t *)vec_at(&flow->operations, last);
    flow_operation_registration_destroy(operation);
    (void)turbo_flow_stl_error(vec_resize(&flow->operations, last));
  }
}

int turbo_flow_register_module_contract(
    turbo_flow_t *flow, const turbo_flow_module_descriptor_t *module,
    const turbo_flow_operation_descriptor_t *operations, size_t operation_count) {
  const turbo_flow_module_descriptor_t *current_module;
  size_t operations_before;
  size_t i;
  int rc;
  if (!flow || !module || module->size < sizeof(*module) || !operations ||
      operation_count == 0u || module->operation_count != operation_count ||
      !module->operation_names) {
    return SALTS_EINVAL;
  }
  for (i = 0; i < operation_count; ++i) {
    const turbo_flow_operation_descriptor_t *current;
    if (operations[i].size != sizeof(operations[i]) || !operations[i].name ||
        strcmp(module->operation_names[i], operations[i].name) != 0) {
      return SALTS_EINVAL;
    }
    current = turbo_flow_find_operation(flow, operations[i].name);
    if (current && !flow_operation_contract_compatible(current, &operations[i])) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "module operation contract is incompatible");
    }
  }
  current_module = turbo_flow_find_module(flow, module->name);
  if (current_module) {
    if (!flow_module_contract_compatible(current_module, module)) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "module contract is incompatible");
    }
    return SALTS_OK;
  }
  for (i = 0; i < operation_count; ++i) {
    if (flow_find_operation_export_module(flow, operations[i].name) >= 0) {
      return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                       "module operation already has another owner");
    }
  }
  operations_before = vec_size(&flow->operations);
  for (i = 0; i < operation_count; ++i) {
    if (turbo_flow_find_operation(flow, operations[i].name)) continue;
    rc = turbo_flow_register_operation(flow, &operations[i]);
    if (rc != SALTS_OK) {
      flow_operation_registry_rollback(flow, operations_before);
      return rc;
    }
  }
  rc = turbo_flow_register_module(flow, module);
  if (rc != SALTS_OK) flow_operation_registry_rollback(flow, operations_before);
  return rc;
}

int turbo_flow_bind_operation_provider_module(turbo_flow_t *flow, const char *module_name,
                                              const char *operation_name,
                                              const char *resource_name) {
  flow_module_registration_t *module;
  flow_operation_provider_registration_t *provider;
  int module_index;
  int provider_index;
  if (!flow || !module_name || module_name[0] == '\0' || !operation_name ||
      operation_name[0] == '\0' || (resource_name && resource_name[0] == '\0')) {
    return SALTS_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0,
                                     "cannot bind provider module after compile");
  }
  module_index = flow_find_module_index(flow, module_name);
  if (module_index < 0) {
    return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0, "module is not registered");
  }
  module = (flow_module_registration_t *)vec_at(&flow->modules, (size_t)module_index);
  if (!flow_module_exports_operation(module, operation_name)) {
    return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                     "operation is not exported by module");
  }
  provider_index = flow_find_operation_provider(flow, operation_name, resource_name);
  if (provider_index < 0) {
    return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                     "operation provider is not registered");
  }
  provider = (flow_operation_provider_registration_t *)vec_at(
      &flow->operation_providers, (size_t)provider_index);
  if (provider->module_name) {
    return flow_set_error_keep_state(flow,
                                     strcmp(provider->module_name, module_name) == 0
                                         ? SALTS_EALREADY
                                         : SALTS_EBUSY,
                                     0, 0, "operation provider already has a module owner");
  }
  if (resource_name) {
    const turbo_flow_primitive_descriptor_t *primitive = turbo_flow_find_primitive(flow, resource_name);
    if (!primitive) {
      return flow_set_error_keep_state(flow, SALTS_ENOENT, 0, 0,
                                       "provider resource primitive is not registered");
    }
    if (!flow_module_exports_primitive_type(module, primitive->type_name)) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, 0, 0,
                                       "provider resource type is not exported by module");
    }
  }
  provider->module_name = tstr_dup(module_name);
  if (!provider->module_name) return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  return SALTS_OK;
}

size_t turbo_flow_primitive_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->primitives) : 0;
}

size_t turbo_flow_operation_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->operations) : 0;
}

size_t turbo_flow_module_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->modules) : 0;
}

const turbo_flow_primitive_descriptor_t *turbo_flow_primitive_at(const turbo_flow_t *flow,
                                                                 size_t index) {
  const flow_primitive_registration_t *primitive;
  if (!flow) return NULL;
  primitive = (const flow_primitive_registration_t *)vec_at_const(&flow->primitives, index);
  return primitive ? &primitive->descriptor : NULL;
}

const turbo_flow_operation_descriptor_t *turbo_flow_operation_at(const turbo_flow_t *flow,
                                                                 size_t index) {
  const flow_operation_registration_t *operation;
  if (!flow) return NULL;
  operation = (const flow_operation_registration_t *)vec_at_const(&flow->operations, index);
  return operation ? &operation->descriptor : NULL;
}

const turbo_flow_module_descriptor_t *turbo_flow_module_at(const turbo_flow_t *flow,
                                                           size_t index) {
  const flow_module_registration_t *module;
  if (!flow) return NULL;
  module = (const flow_module_registration_t *)vec_at_const(&flow->modules, index);
  return module ? &module->descriptor : NULL;
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

const turbo_flow_module_descriptor_t *turbo_flow_find_module(const turbo_flow_t *flow,
                                                             const char *name) {
  int index = flow_find_module_index(flow, name);
  return index < 0 ? NULL : turbo_flow_module_at(flow, (size_t)index);
}

const char *turbo_flow_operation_provider_module(const turbo_flow_t *flow,
                                                 const char *operation_name,
                                                 const char *resource_name) {
  int index = flow_find_operation_provider(flow, operation_name, resource_name);
  const flow_operation_provider_registration_t *provider;
  if (index < 0) return NULL;
  provider = (const flow_operation_provider_registration_t *)vec_at_const(
      &flow->operation_providers, (size_t)index);
  return provider ? provider->module_name : NULL;
}
