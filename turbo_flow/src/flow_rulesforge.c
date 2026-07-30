#include "turbo_flow_rulesforge.h"

#include "flow_internal.h"

#include <string.h>

static const uint32_t FLOW_RULEFORGE_MODULE_VERSION = 1u;

static int flow_rulesforge_status(ruleforge_status_t status) {
  switch (status) {
  case RULES_FORGE_OK:
    return TURBO_OK;
  case RULES_FORGE_ERROR_INVALID_ARGUMENT:
    return TURBO_EINVAL;
  case RULES_FORGE_ERROR_MEMORY_ALLOCATION:
    return TURBO_ENOMEM;
  case RULES_FORGE_ERROR_RESOURCE_LIMIT:
    return TURBO_ENOSPC;
  case RULES_FORGE_ERROR_COMPILATION_FAILED:
  case RULES_FORGE_ERROR_FACT_INSERTION_FAILED:
  case RULES_FORGE_ERROR_SESSION_INCONSISTENT:
  case RULES_FORGE_STATUS_END_OF_STREAM:
    return TURBO_EPROTO;
  case RULES_FORGE_ERROR_GENERIC:
  case RULES_FORGE_ERROR_SESSION_CREATION_FAILED:
  case RULES_FORGE_ERROR_QUERY_FAILED:
  default:
    return TURBO_EIO;
  }
}

static int flow_rulesforge_databind_clone(const void *value, void *ctx, void **out) {
  ruleforge_data_bind_object_t clone = NULL;
  ruleforge_status_t status;
  (void)ctx;
  if (!value || !out) return TURBO_EINVAL;
  *out = NULL;
  status = ruleforge_data_bind_object_clone((ruleforge_data_bind_object_t)value, &clone);
  if (status != RULES_FORGE_OK) return flow_rulesforge_status(status);
  *out = clone;
  return TURBO_OK;
}

static void flow_rulesforge_databind_destroy(void *value, void *ctx) {
  (void)ctx;
  if (value) (void)ruleforge_data_bind_object_destroy((ruleforge_data_bind_object_t)value);
}

int turbo_flow_rulesforge_set_result(turbo_flow_msg_t *message, uint32_t match_count, int status) {
  turbo_flow_data_decision_t decision = TURBO_FLOW_DATA_DECISION_INIT;
  if (!message || (status != TURBO_OK && match_count != 0u)) return TURBO_EINVAL;
  if (status == TURBO_OK) {
    decision.evaluation_status =
        match_count > 0u ? TURBO_FLOW_DATA_MATCHED : TURBO_FLOW_DATA_NOT_MATCHED;
  } else {
    decision.evaluation_status = TURBO_FLOW_DATA_EVALUATION_ERROR;
    decision.evaluation_error = status;
  }
  decision.match_count = match_count;
  message->data_decision = decision;
  return TURBO_OK;
}

static int flow_rulesforge_execute(turbo_flow_msg_t *message, void *ctx) {
  const turbo_flow_rulesforge_data_operation_registration_t *registration =
      (const turbo_flow_rulesforge_data_operation_registration_t *)ctx;
  int rc;
  if (!message || !registration || registration->size < sizeof(*registration) || !registration->fn ||
      !registration->resource_name || registration->resource_name[0] == '\0') {
    return TURBO_EINVAL;
  }
  rc = registration->fn(message, registration->resource_name, registration->callback_ctx);
  if (rc != TURBO_OK &&
      message->data_decision.evaluation_status == TURBO_FLOW_DATA_NOT_EVALUATED) {
    (void)turbo_flow_rulesforge_set_result(message, 0u, rc);
  }
  return rc;
}

static int flow_rulesforge_finish_databind(
    turbo_flow_msg_t *message, const turbo_flow_rulesforge_databind_provider_t *provider,
    int fired, int rc) {
  int result_rc;
  if (!message || !provider || fired < 0) return TURBO_EINVAL;
  message->flags &= ~provider->matched_flag;
  result_rc = turbo_flow_rulesforge_set_result(
      message, rc == TURBO_OK ? (uint32_t)fired : 0u, rc);
  if (result_rc != TURBO_OK) return result_rc;
  if (rc == TURBO_OK && fired > 0) message->flags |= provider->matched_flag;
  return rc;
}

static int flow_rulesforge_execute_databind(turbo_flow_msg_t *message, void *ctx) {
  const turbo_flow_rulesforge_databind_provider_t *provider =
      (const turbo_flow_rulesforge_databind_provider_t *)ctx;
  const turbo_flow_data_schema_t *schema = NULL;
  const void *projection;
  ruleforge_stateful_session_t session = NULL;
  ruleforge_status_t status;
  int fired = 0;
  int rc;

  if (!message || !provider || provider->size < sizeof(*provider) || !provider->knowledge_base ||
      provider->matched_flag == 0u || provider->max_rules <= 0) {
    return TURBO_EINVAL;
  }
  projection = turbo_flow_msg_projection(message, &schema);
  if (!projection || !schema || schema->size < sizeof(*schema) ||
      schema->domain != TURBO_FLOW_DOMAIN_DATA || !schema->projection_type ||
      !schema->type_name) {
    return flow_rulesforge_finish_databind(message, provider, 0, TURBO_EPROTO);
  }

  status = ruleforge_session_create(provider->knowledge_base, &session);
  if (status != RULES_FORGE_OK) {
    return flow_rulesforge_finish_databind(message, provider, 0,
                                          flow_rulesforge_status(status));
  }
  if (strcmp(schema->projection_type,
             TURBO_FLOW_RULEFORGE_DATABIND_PROJECTION_TYPE) == 0) {
    ruleforge_data_bind_object_t object =
        (ruleforge_data_bind_object_t)projection;
    const char *object_type =
        ruleforge_data_bind_object_get_type_name(object);
    if (schema->encoding != TURBO_FLOW_DATA_ENCODING_TBE || !object_type ||
        strcmp(object_type, schema->type_name) != 0) {
      (void)ruleforge_session_destroy(session);
      return flow_rulesforge_finish_databind(message, provider, 0, TURBO_EPROTO);
    }
    status = ruleforge_session_add_data_bind_object(session, object, NULL);
  } else if (strcmp(
                 schema->projection_type,
                 TURBO_FLOW_RULEFORGE_DATABIND_VALUE_PROJECTION_TYPE) == 0) {
    status = ruleforge_session_add_data_bind_value(
        session, schema->type_name, (const DataBindValue *)projection, NULL);
  } else {
    (void)ruleforge_session_destroy(session);
    return flow_rulesforge_finish_databind(message, provider, 0, TURBO_EPROTO);
  }
  if (status == RULES_FORGE_OK) {
    status = ruleforge_session_fire_all_rules(session, provider->max_rules, &fired);
  }
  rc = flow_rulesforge_status(status);
  status = ruleforge_session_destroy(session);
  if (rc == TURBO_OK && status != RULES_FORGE_OK) rc = flow_rulesforge_status(status);
  return flow_rulesforge_finish_databind(message, provider, fired, rc);
}

static turbo_flow_operation_descriptor_t flow_rulesforge_apply_operation_descriptor(void) {
  turbo_flow_operation_descriptor_t operation;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = TURBO_FLOW_RULEFORGE_APPLY_OPERATION;
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_RULES;
  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = "Message";
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = "Message";
  operation.resource_domain = TURBO_FLOW_DOMAIN_RULES;
  operation.resource_type = TURBO_FLOW_RULEFORGE_RESOURCE_TYPE;
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
  operation.scope.lifetime = TURBO_FLOW_LIFETIME_CALL;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  operation.flags = TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  operation.runtime.handoff = TURBO_FLOW_HANDOFF_DIRECT;
  operation.runtime.ordering = TURBO_FLOW_ORDERING_UNORDERED;
  operation.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_NONE;
  operation.runtime.cancellation = TURBO_FLOW_CANCELLATION_NONE;
  operation.runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;
  return operation;
}

static int flow_rulesforge_operation_compatible(const turbo_flow_operation_descriptor_t *actual,
                                              const turbo_flow_operation_descriptor_t *expected) {
  if (!actual || !expected || actual->version != expected->version ||
      actual->domain != expected->domain || actual->input_domain != expected->input_domain ||
      actual->output_domain != expected->output_domain ||
      actual->resource_domain != expected->resource_domain ||
      actual->scope.data != expected->scope.data || actual->scope.state != expected->scope.state ||
      actual->scope.lifetime != expected->scope.lifetime ||
      actual->scope.concurrency != expected->scope.concurrency ||
      actual->scope.authority != expected->scope.authority || actual->flags != expected->flags ||
      actual->execution_mask != expected->execution_mask ||
      actual->runtime.handoff != expected->runtime.handoff ||
      actual->runtime.ordering != expected->runtime.ordering ||
      actual->runtime.backpressure != expected->runtime.backpressure ||
      actual->runtime.cancellation != expected->runtime.cancellation ||
      actual->runtime.error_mode != expected->runtime.error_mode ||
      actual->runtime.capacity != expected->runtime.capacity ||
      actual->runtime.deadline_ms != expected->runtime.deadline_ms ||
      actual->runtime.settlement != expected->runtime.settlement) {
    return 0;
  }
  return ((!actual->input_type && !expected->input_type) ||
          (actual->input_type && expected->input_type &&
           strcmp(actual->input_type, expected->input_type) == 0)) &&
         ((!actual->output_type && !expected->output_type) ||
          (actual->output_type && expected->output_type &&
           strcmp(actual->output_type, expected->output_type) == 0)) &&
         ((!actual->resource_type && !expected->resource_type) ||
          (actual->resource_type && expected->resource_type &&
           strcmp(actual->resource_type, expected->resource_type) == 0));
}

static int flow_rulesforge_primitive_compatible(const turbo_flow_primitive_descriptor_t *actual,
                                              const turbo_flow_primitive_descriptor_t *expected) {
  return actual && expected && actual->version == expected->version &&
         actual->domain == expected->domain && actual->kind == expected->kind &&
         actual->type_name && strcmp(actual->type_name, expected->type_name) == 0;
}

static int flow_rulesforge_register_apply_contract(turbo_flow_t *flow, const char *resource_name) {
  turbo_flow_primitive_descriptor_t primitive = {0};
  turbo_flow_operation_descriptor_t operation = flow_rulesforge_apply_operation_descriptor();
  const turbo_flow_primitive_descriptor_t *existing_primitive;
  const turbo_flow_operation_descriptor_t *existing_operation;
  int rc;

  primitive.size = sizeof(primitive);
  primitive.name = resource_name;
  primitive.type_name = TURBO_FLOW_RULEFORGE_RESOURCE_TYPE;
  primitive.version = 1u;
  primitive.domain = TURBO_FLOW_DOMAIN_RULES;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;

  existing_primitive = turbo_flow_find_primitive(flow, resource_name);
  if (existing_primitive) {
    if (!flow_rulesforge_primitive_compatible(existing_primitive, &primitive)) return TURBO_EPROTO;
  } else {
    rc = turbo_flow_register_primitive(flow, &primitive);
    if (rc != TURBO_OK) return rc;
  }

  existing_operation = turbo_flow_find_operation(flow, TURBO_FLOW_RULEFORGE_APPLY_OPERATION);
  if (existing_operation) {
    if (!flow_rulesforge_operation_compatible(existing_operation, &operation)) return TURBO_EPROTO;
  } else {
    rc = turbo_flow_register_operation(flow, &operation);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_rulesforge_register_module_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {TURBO_FLOW_RULEFORGE_RESOURCE_TYPE};
  static const char *const operation_names[] = {TURBO_FLOW_RULEFORGE_APPLY_OPERATION};
  turbo_flow_module_descriptor_t descriptor = {0};
  const turbo_flow_module_descriptor_t *existing =
      turbo_flow_find_module(flow, TURBO_FLOW_RULEFORGE_MODULE);

  if (existing) {
    if (existing->version != FLOW_RULEFORGE_MODULE_VERSION ||
        existing->capability_flags !=
            (TURBO_FLOW_MODULE_GRAPH_OPERATIONS | TURBO_FLOW_MODULE_MANAGED_RESOURCES |
             TURBO_FLOW_MODULE_NATIVE_API) ||
        existing->primitive_type_count != 1u || existing->operation_count != 1u ||
        existing->requirement_count != 0u ||
        strcmp(existing->primitive_types[0], TURBO_FLOW_RULEFORGE_RESOURCE_TYPE) != 0 ||
        strcmp(existing->operation_names[0], TURBO_FLOW_RULEFORGE_APPLY_OPERATION) != 0) {
      return TURBO_EPROTO;
    }
    return TURBO_OK;
  }
  descriptor.size = sizeof(descriptor);
  descriptor.name = TURBO_FLOW_RULEFORGE_MODULE;
  descriptor.version = FLOW_RULEFORGE_MODULE_VERSION;
  descriptor.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                                TURBO_FLOW_MODULE_NATIVE_API;
  descriptor.primitive_types = primitive_types;
  descriptor.primitive_type_count = 1u;
  descriptor.operation_names = operation_names;
  descriptor.operation_count = 1u;
  return turbo_flow_register_module(flow, &descriptor);
}

static void flow_rulesforge_rollback_operation_registration(turbo_flow_t *flow, size_t providers_before,
                                                          size_t modules_before,
                                                          size_t primitives_before,
                                                          size_t operations_before) {
  while (turbo_vec_size(&flow->operation_providers) > providers_before) {
    size_t index = turbo_vec_size(&flow->operation_providers) - 1u;
    flow_operation_provider_registration_t *provider =
        (flow_operation_provider_registration_t *)turbo_vec_at(&flow->operation_providers, index);
    flow_operation_provider_registration_destroy(provider);
    (void)turbo_vec_resize(&flow->operation_providers, index);
  }
  while (turbo_vec_size(&flow->modules) > modules_before) {
    size_t index = turbo_vec_size(&flow->modules) - 1u;
    flow_module_registration_t *module =
        (flow_module_registration_t *)turbo_vec_at(&flow->modules, index);
    flow_module_registration_destroy(module);
    (void)turbo_vec_resize(&flow->modules, index);
  }
  while (turbo_vec_size(&flow->primitives) > primitives_before) {
    size_t index = turbo_vec_size(&flow->primitives) - 1u;
    flow_primitive_registration_t *primitive =
        (flow_primitive_registration_t *)turbo_vec_at(&flow->primitives, index);
    flow_primitive_registration_destroy(primitive);
    (void)turbo_vec_resize(&flow->primitives, index);
  }
  while (turbo_vec_size(&flow->operations) > operations_before) {
    size_t index = turbo_vec_size(&flow->operations) - 1u;
    flow_operation_registration_t *operation =
        (flow_operation_registration_t *)turbo_vec_at(&flow->operations, index);
    flow_operation_registration_destroy(operation);
    (void)turbo_vec_resize(&flow->operations, index);
  }
}

static int flow_rulesforge_register_provider(turbo_flow_t *flow, const char *resource_name,
                                             turbo_flow_stage_fn fn, void *ctx,
                                             turbo_flow_stage_options_t options) {
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  size_t providers_before;
  size_t modules_before;
  size_t primitives_before;
  size_t operations_before;
  int rc;

  if (!flow || !resource_name || resource_name[0] == '\0' || !fn) {
    return TURBO_EINVAL;
  }

  providers_before = turbo_vec_size(&flow->operation_providers);
  modules_before = turbo_vec_size(&flow->modules);
  primitives_before = turbo_vec_size(&flow->primitives);
  operations_before = turbo_vec_size(&flow->operations);

  rc = flow_rulesforge_register_apply_contract(flow, resource_name);
  if (rc != TURBO_OK) {
    flow_rulesforge_rollback_operation_registration(flow, providers_before, modules_before,
                                                    primitives_before, operations_before);
    return rc;
  }
  rc = flow_rulesforge_register_module_contract(flow);
  if (rc != TURBO_OK) {
    flow_rulesforge_rollback_operation_registration(flow, providers_before, modules_before,
                                                    primitives_before, operations_before);
    return rc;
  }

  provider.operation_name = TURBO_FLOW_RULEFORGE_APPLY_OPERATION;
  provider.resource_name = resource_name;
  provider.fn = fn;
  provider.ctx = ctx;
  provider.options = options;
  rc = turbo_flow_register_operation_provider(flow, &provider);
  if (rc != TURBO_OK) {
    flow_rulesforge_rollback_operation_registration(flow, providers_before, modules_before,
                                                    primitives_before, operations_before);
    return rc;
  }
  rc = turbo_flow_bind_operation_provider_module(
      flow, TURBO_FLOW_RULEFORGE_MODULE, TURBO_FLOW_RULEFORGE_APPLY_OPERATION,
      resource_name);
  if (rc != TURBO_OK) {
    flow_rulesforge_rollback_operation_registration(flow, providers_before, modules_before,
                                                    primitives_before, operations_before);
    return rc;
  }
  return TURBO_OK;
}

int turbo_flow_rulesforge_register_data_operation(
    turbo_flow_t *flow,
    const turbo_flow_rulesforge_data_operation_registration_t *registration) {
  if (!registration || registration->size < sizeof(*registration) ||
      !registration->resource_name || registration->resource_name[0] == '\0' || !registration->fn) {
    return TURBO_EINVAL;
  }
  return flow_rulesforge_register_provider(flow, registration->resource_name,
                                           flow_rulesforge_execute, (void *)registration,
                                           registration->options);
}

int turbo_flow_rulesforge_bind_databind_object(turbo_flow_msg_t *message,
                                               const turbo_flow_data_schema_t *schema,
                                               ruleforge_data_bind_object_t object) {
  const char *object_type;
  if (!message || !schema || schema->size < sizeof(*schema) || !object ||
      schema->domain != TURBO_FLOW_DOMAIN_DATA || schema->encoding != TURBO_FLOW_DATA_ENCODING_TBE ||
      !schema->projection_type ||
      strcmp(schema->projection_type, TURBO_FLOW_RULEFORGE_DATABIND_PROJECTION_TYPE) != 0 ||
      !schema->type_name) {
    return TURBO_EINVAL;
  }
  object_type = ruleforge_data_bind_object_get_type_name(object);
  if (!object_type || strcmp(object_type, schema->type_name) != 0) return TURBO_EPROTO;
  return turbo_flow_msg_bind_projection(message, schema, object, flow_rulesforge_databind_clone,
                                        flow_rulesforge_databind_destroy, NULL);
}

static int flow_rulesforge_finish_json(
    turbo_flow_msg_t *message, const turbo_flow_rulesforge_json_provider_t *provider,
    ruleforge_stateful_session_t session, ruleforge_data_bind_stream_t stream, int fired, int rc) {
  ruleforge_status_t status;
  int result_rc;

  if (stream) {
    status = ruleforge_data_bind_stream_destroy(stream);
    if (rc == TURBO_OK && status != RULES_FORGE_OK) rc = flow_rulesforge_status(status);
  }
  if (session) {
    status = ruleforge_session_destroy(session);
    if (rc == TURBO_OK && status != RULES_FORGE_OK) rc = flow_rulesforge_status(status);
  }
  if (rc != TURBO_OK) fired = 0;
  if (fired > 0)
    message->flags |= provider->matched_flag;
  else
    message->flags &= ~provider->matched_flag;
  result_rc =
      turbo_flow_rulesforge_set_result(message, fired > 0 ? (uint32_t)fired : 0u, rc);
  return rc != TURBO_OK ? rc : result_rc;
}

static int flow_rulesforge_execute_json(turbo_flow_msg_t *message, void *ctx) {
  const turbo_flow_rulesforge_json_provider_t *provider =
      (const turbo_flow_rulesforge_json_provider_t *)ctx;
  ruleforge_stateful_session_t session = NULL;
  ruleforge_data_bind_stream_t stream = NULL;
  tstr_v payload = {0};
  ruleforge_status_t status;
  int loaded = 0;
  int fired = 0;
  int rc;

  if (!message || !provider) return TURBO_EINVAL;
  message->flags &= ~provider->matched_flag;
  if (provider->payload_view) {
    rc = provider->payload_view(message, &payload, provider->payload_ctx);
    if (rc != TURBO_OK)
      return flow_rulesforge_finish_json(message, provider, NULL, NULL, 0, rc);
  } else {
    payload = message->payload;
  }
  if (!payload.data || payload.len == 0u)
    return flow_rulesforge_finish_json(message, provider, NULL, NULL, 0, TURBO_EPROTO);

  status = ruleforge_session_create(provider->knowledge_base, &session);
  if (status != RULES_FORGE_OK)
    return flow_rulesforge_finish_json(message, provider, session, NULL, 0,
                                       flow_rulesforge_status(status));
  status = ruleforge_data_bind_stream_json_create(session, provider->fact_type, &stream);
  if (status != RULES_FORGE_OK)
    return flow_rulesforge_finish_json(message, provider, session, stream, 0,
                                       flow_rulesforge_status(status));
  status = ruleforge_data_bind_stream_feed(stream, payload.data, payload.len);
  if (status != RULES_FORGE_OK)
    return flow_rulesforge_finish_json(message, provider, session, stream, 0,
                                       flow_rulesforge_status(status));
  status = ruleforge_data_bind_stream_finish(stream, NULL, &loaded);
  if (status != RULES_FORGE_OK)
    return flow_rulesforge_finish_json(message, provider, session, stream, 0,
                                       flow_rulesforge_status(status));
  status = ruleforge_data_bind_stream_destroy(stream);
  stream = NULL;
  if (status != RULES_FORGE_OK)
    return flow_rulesforge_finish_json(message, provider, session, NULL, 0,
                                       flow_rulesforge_status(status));
  if (loaded != 1)
    return flow_rulesforge_finish_json(message, provider, session, NULL, 0, TURBO_EPROTO);

  status =
      ruleforge_session_fire_all_rules(session, provider->max_rules, &fired);
  rc = flow_rulesforge_status(status);
  return flow_rulesforge_finish_json(message, provider, session, NULL, fired, rc);
}

int turbo_flow_rulesforge_register_json_provider(
    turbo_flow_t *flow, const char *resource_name,
    const turbo_flow_rulesforge_json_provider_t *provider) {
  if (!flow || !resource_name || resource_name[0] == '\0' || !provider ||
      provider->size < sizeof(*provider) || !provider->knowledge_base || !provider->fact_type ||
      provider->fact_type[0] == '\0' || provider->matched_flag == 0u ||
      provider->max_rules <= 0) {
    return TURBO_EINVAL;
  }
  return flow_rulesforge_register_provider(flow, resource_name, flow_rulesforge_execute_json,
                                           (void *)provider, provider->options);
}

int turbo_flow_rulesforge_register_databind_provider(
    turbo_flow_t *flow, const char *resource_name,
    const turbo_flow_rulesforge_databind_provider_t *provider) {
  if (!provider || provider->size < sizeof(*provider) || !provider->knowledge_base ||
      provider->matched_flag == 0u || provider->max_rules <= 0) {
    return TURBO_EINVAL;
  }
  return flow_rulesforge_register_provider(flow, resource_name, flow_rulesforge_execute_databind,
                                           (void *)provider, provider->options);
}
