#include "turbo_flow_policy.h"

#include "flow_expr_internal.h"
#include "flow_internal.h"
#include "salts_error.h"
#include <json_parser.h>
#include "salts_str.h"
#include "turbo_flow_stl_error_internal.h"

#include <limits.h>
#include <math.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_RULE_STAGE_MAX_ACTIONS 64u
#define FLOW_RULE_RESOLVED_MAX_RULES 4096u

static const uint32_t FLOW_RULE_MODULE_VERSION = 1u;

int turbo_flow_rule_projection_facts_provider(const turbo_flow_msg_t *message,
                                              const turbo_flow_expr_schema_t *schema,
                                              const turbo_flow_expr_value_t **values_out,
                                              size_t *value_count_out, void *ctx) {
  const turbo_flow_rule_projection_provider_t *provider =
      (const turbo_flow_rule_projection_provider_t *)ctx;
  const turbo_flow_data_schema_t *projection_schema = NULL;
  const void *projection;
  int rc;

  if (!message || !schema || !values_out || !value_count_out || !provider ||
      provider->size < sizeof(*provider) || !provider->materialize) {
    return SALTS_EINVAL;
  }
  *values_out = NULL;
  *value_count_out = 0u;
  projection = turbo_flow_msg_projection(message, &projection_schema);
  if (!projection || !projection_schema) return SALTS_ENOENT;
  rc = provider->materialize(projection, projection_schema, schema, values_out, value_count_out,
                             provider->ctx);
  if (rc != SALTS_OK) return rc;
  if (*value_count_out != schema->field_count || (*value_count_out > 0u && !*values_out)) {
    *values_out = NULL;
    *value_count_out = 0u;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static turbo_flow_operation_descriptor_t flow_rule_apply_operation_descriptor(void) {
  turbo_flow_operation_descriptor_t operation;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = TURBO_FLOW_RULE_APPLY_OPERATION;
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_RULES;
  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = "Message";
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = "Message";
  operation.resource_domain = TURBO_FLOW_DOMAIN_RULES;
  operation.resource_type = TURBO_FLOW_RULE_SET_TYPE;
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

static int flow_rule_operation_compatible(const turbo_flow_operation_descriptor_t *actual,
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

static int flow_rule_primitive_compatible(const turbo_flow_primitive_descriptor_t *actual,
                                          const turbo_flow_primitive_descriptor_t *expected) {
  return actual && expected && actual->version == expected->version &&
         actual->domain == expected->domain && actual->kind == expected->kind &&
         actual->type_name && strcmp(actual->type_name, expected->type_name) == 0;
}

static int flow_rule_register_apply_contract(turbo_flow_t *flow, const char *resource_name) {
  turbo_flow_primitive_descriptor_t primitive = {0};
  turbo_flow_operation_descriptor_t operation = flow_rule_apply_operation_descriptor();
  const turbo_flow_primitive_descriptor_t *existing_primitive;
  const turbo_flow_operation_descriptor_t *existing_operation;
  int rc;

  primitive.size = sizeof(primitive);
  primitive.name = resource_name;
  primitive.type_name = TURBO_FLOW_RULE_SET_TYPE;
  primitive.version = 1u;
  primitive.domain = TURBO_FLOW_DOMAIN_RULES;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;

  existing_primitive = turbo_flow_find_primitive(flow, resource_name);
  if (existing_primitive) {
    if (!flow_rule_primitive_compatible(existing_primitive, &primitive)) return SALTS_EPROTO;
  } else {
    rc = turbo_flow_register_primitive(flow, &primitive);
    if (rc != SALTS_OK) return rc;
  }

  existing_operation = turbo_flow_find_operation(flow, TURBO_FLOW_RULE_APPLY_OPERATION);
  if (existing_operation) {
    if (!flow_rule_operation_compatible(existing_operation, &operation)) return SALTS_EPROTO;
  } else {
    rc = turbo_flow_register_operation(flow, &operation);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

static int flow_rule_register_module_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {TURBO_FLOW_RULE_SET_TYPE};
  static const char *const operation_names[] = {TURBO_FLOW_RULE_APPLY_OPERATION};
  turbo_flow_module_descriptor_t descriptor = {0};
  const turbo_flow_module_descriptor_t *existing =
      turbo_flow_find_module(flow, TURBO_FLOW_RULE_MODULE);
  if (existing) {
    if (existing->version != FLOW_RULE_MODULE_VERSION ||
        existing->capability_flags !=
            (TURBO_FLOW_MODULE_GRAPH_OPERATIONS | TURBO_FLOW_MODULE_MANAGED_RESOURCES |
             TURBO_FLOW_MODULE_NATIVE_API) ||
        existing->primitive_type_count != 1u || existing->operation_count != 1u ||
        existing->requirement_count != 0u ||
        strcmp(existing->primitive_types[0], TURBO_FLOW_RULE_SET_TYPE) != 0 ||
        strcmp(existing->operation_names[0], TURBO_FLOW_RULE_APPLY_OPERATION) != 0) {
      return SALTS_EPROTO;
    }
    return SALTS_OK;
  }
  descriptor.size = sizeof(descriptor);
  descriptor.name = TURBO_FLOW_RULE_MODULE;
  descriptor.version = FLOW_RULE_MODULE_VERSION;
  descriptor.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                TURBO_FLOW_MODULE_MANAGED_RESOURCES | TURBO_FLOW_MODULE_NATIVE_API;
  descriptor.primitive_types = primitive_types;
  descriptor.primitive_type_count = 1u;
  descriptor.operation_names = operation_names;
  descriptor.operation_count = 1u;
  return turbo_flow_register_module(flow, &descriptor);
}

static void flow_rule_rollback_operation_registration(turbo_flow_t *flow, size_t resources_before,
                                                      size_t providers_before,
                                                      size_t modules_before,
                                                      size_t primitives_before,
                                                      size_t operations_before) {
  while (vec_size(&flow->operation_providers) > providers_before) {
    size_t index = vec_size(&flow->operation_providers) - 1u;
    flow_operation_provider_registration_t *provider =
        (flow_operation_provider_registration_t *)vec_at(&flow->operation_providers, index);
    flow_operation_provider_registration_destroy(provider);
    (void)turbo_flow_stl_error(vec_resize(&flow->operation_providers, index));
  }
  while (vec_size(&flow->resources) > resources_before) {
    size_t index = vec_size(&flow->resources) - 1u;
    flow_resource_registration_t *resource =
        (flow_resource_registration_t *)vec_at(&flow->resources, index);
    flow_resource_registration_destroy(resource);
    (void)turbo_flow_stl_error(vec_resize(&flow->resources, index));
  }
  while (vec_size(&flow->modules) > modules_before) {
    size_t index = vec_size(&flow->modules) - 1u;
    flow_module_registration_t *module =
        (flow_module_registration_t *)vec_at(&flow->modules, index);
    flow_module_registration_destroy(module);
    (void)turbo_flow_stl_error(vec_resize(&flow->modules, index));
  }
  while (vec_size(&flow->primitives) > primitives_before) {
    size_t index = vec_size(&flow->primitives) - 1u;
    flow_primitive_registration_t *primitive =
        (flow_primitive_registration_t *)vec_at(&flow->primitives, index);
    flow_primitive_registration_destroy(primitive);
    (void)turbo_flow_stl_error(vec_resize(&flow->primitives, index));
  }
  while (vec_size(&flow->operations) > operations_before) {
    size_t index = vec_size(&flow->operations) - 1u;
    flow_operation_registration_t *operation =
        (flow_operation_registration_t *)vec_at(&flow->operations, index);
    flow_operation_registration_destroy(operation);
    (void)turbo_flow_stl_error(vec_resize(&flow->operations, index));
  }
}

typedef struct flow_compiled_rule_s {
  turbo_flow_expr_t *predicate;
  turbo_flow_rule_action_t action;
  size_t instructions;
  size_t memory_bytes;
} flow_compiled_rule_t;

struct turbo_flow_rule_processor_s {
  vec_t rules;
  vec_t schema_fields;
  tstr resource_uid;
  tstr owner_name;
  turbo_flow_rule_facts_provider_fn facts_provider;
  void *facts_provider_ctx;
  turbo_flow_rule_domain_t domain;
  turbo_flow_rule_mode_t mode;
  turbo_flow_rule_limits_t limits;
  size_t instructions;
  size_t memory_bytes;
  atomic_uint_fast64_t evaluations;
  atomic_uint_fast64_t matches;
  atomic_uint_fast64_t failures;
  atomic_int last_status;
};

static const char FLOW_RULE_SET_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowRuleResource [id(1), version(1)];\n"
    "message RuleSetStatus {\n"
    "  uint32 domain;\n"
    "  uint32 mode;\n"
    "  string rule_count;\n"
    "  string instructions;\n"
    "  string memory_bytes;\n"
    "  string evaluations;\n"
    "  string matches;\n"
    "  string failures;\n"
    "  int32 last_status;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_RULE_SET_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_RULES,
    TURBO_FLOW_RESOURCE_RULE_SET,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowRuleResource",
    "RuleSetStatus",
    1u,
    1u,
    FLOW_RULE_SET_STATUS_SCHEMA_TEXT};

typedef struct flow_rule_fact_reader_s {
  const turbo_flow_rule_facts_t *facts;
} flow_rule_fact_reader_t;

static int flow_rule_limits_valid(const turbo_flow_rule_limits_t *limits) {
  return limits && limits->max_instructions > 0u && limits->max_time_ns > 0u &&
         limits->max_memory_bytes > 0u && limits->max_output_actions > 0u &&
         limits->max_output_bytes > 0u;
}

static int flow_rule_key_valid(const char key[TURBO_FLOW_RULE_KEY_MAX + 1u], int required) {
  const char *end = (const char *)memchr(key, '\0', TURBO_FLOW_RULE_KEY_MAX + 1u);
  return end && (!required || end != key);
}

static int flow_rule_command_valid(const turbo_flow_resource_command_t *command) {
  if (!command || command->size < sizeof(*command) ||
      command->kind < TURBO_FLOW_RESOURCE_COMMAND_QUIESCE ||
      command->kind > TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL ||
      !memchr(command->target_uid, '\0', sizeof(command->target_uid)) ||
      command->target_uid[0] == '\0' ||
      !memchr(command->idempotency_key, '\0', sizeof(command->idempotency_key)) ||
      command->idempotency_key[0] == '\0') {
    return 0;
  }
  return command->expected_generation > 0u;
}

static int flow_rule_action_valid(turbo_flow_rule_domain_t domain,
                                  const turbo_flow_rule_action_t *action) {
  if (!action || action->size < sizeof(*action) ||
      action->abi_version != TURBO_FLOW_RULE_ACTION_ABI_V1 ||
      action->kind < TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE ||
      action->kind > TURBO_FLOW_RULE_ACTION_PROPOSE_COMMAND ||
      !flow_rule_key_valid(action->key, 0)) {
    return 0;
  }
  if (domain == TURBO_FLOW_RULE_CONTROL) {
    return action->kind == TURBO_FLOW_RULE_ACTION_PROPOSE_COMMAND &&
           flow_rule_command_valid(&action->command);
  }
  if (domain != TURBO_FLOW_RULE_DATA || action->kind == TURBO_FLOW_RULE_ACTION_PROPOSE_COMMAND) {
    return 0;
  }
  if (action->kind == TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE) {
    return action->private_field >= TURBO_FLOW_RULE_PRIVATE_MSG_TYPE &&
           action->private_field <= TURBO_FLOW_RULE_PRIVATE_MSG_STATUS;
  }
  if (action->kind == TURBO_FLOW_RULE_ACTION_ROUTE ||
      action->kind == TURBO_FLOW_RULE_ACTION_BATCH_KEY ||
      action->kind == TURBO_FLOW_RULE_ACTION_RETRY_CLASS) {
    return flow_rule_key_valid(action->key, 1);
  }
  return action->kind != TURBO_FLOW_RULE_ACTION_DEAD_LETTER || action->status != SALTS_OK;
}

static int flow_rule_config_valid(const turbo_flow_rule_processor_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->program_abi_version != TURBO_FLOW_RULE_PROGRAM_ABI_V1 || !config->resource_uid ||
      !config->resource_uid[0] || strlen(config->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      !config->owner_name || !config->owner_name[0] ||
      strlen(config->owner_name) > TURBO_FLOW_RESOURCE_OWNER_MAX || !config->rules ||
      config->rule_count == 0u || !flow_rule_limits_valid(&config->limits)) {
    return 0;
  }
  if (config->domain < TURBO_FLOW_RULE_DATA || config->domain > TURBO_FLOW_RULE_CONTROL) return 0;
  return config->mode == TURBO_FLOW_RULE_FIRST_MATCH || config->mode == TURBO_FLOW_RULE_ALL_MATCHES;
}

static int flow_rule_read_fact(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  const flow_rule_fact_reader_t *reader = (const flow_rule_fact_reader_t *)ctx;
  const turbo_flow_rule_facts_t *facts;
  if (!reader || !reader->facts || !out) return SALTS_EINVAL;
  facts = reader->facts;
  for (size_t i = 0; i < facts->schema->field_count; ++i) {
    if (facts->schema->fields[i].field_id != field_id) continue;
    *out = facts->values[i];
    return out->type == TURBO_FLOW_EXPR_TYPE_NULL || out->type == facts->schema->fields[i].type
               ? SALTS_OK
               : SALTS_EPROTO;
  }
  return SALTS_ENOENT;
}

static int flow_rule_facts_valid(const turbo_flow_rule_facts_t *facts) {
  if (!facts || facts->size < sizeof(*facts)) return 0;
  if (!facts->schema) return !facts->values && facts->value_count == 0u;
  if (facts->schema->field_count != facts->value_count ||
      (facts->value_count > 0u && (!facts->schema->fields || !facts->values))) {
    return 0;
  }
  return 1;
}

static void flow_rule_schema_destroy(turbo_flow_rule_processor_t *processor) {
  if (!processor) return;
  for (size_t i = 0; i < vec_size(&processor->schema_fields); ++i) {
    turbo_flow_expr_schema_field_t *field =
        (turbo_flow_expr_schema_field_t *)vec_at(&processor->schema_fields, i);
    if (field) free((void *)field->path);
  }
  vec_destroy(&processor->schema_fields);
}

static int flow_rule_schema_copy(turbo_flow_rule_processor_t *processor,
                                 const turbo_flow_expr_schema_t *schema) {
  size_t descriptor_bytes;
  if (!schema) return SALTS_OK;
  if (schema->field_count > TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS ||
      (schema->field_count > 0u && !schema->fields)) {
    return SALTS_EINVAL;
  }
  if (schema->field_count > SIZE_MAX / sizeof(turbo_flow_expr_schema_field_t)) {
    return SALTS_ERANGE;
  }
  descriptor_bytes = schema->field_count * sizeof(turbo_flow_expr_schema_field_t);
  if (descriptor_bytes > processor->limits.max_memory_bytes - processor->memory_bytes) {
    return SALTS_ENOSPC;
  }
  if (turbo_flow_stl_error(vec_reserve(&processor->schema_fields, schema->field_count)) != SALTS_OK) {
    return SALTS_ENOMEM;
  }
  processor->memory_bytes += descriptor_bytes;
  for (size_t i = 0; i < schema->field_count; ++i) {
    turbo_flow_expr_schema_field_t copy = schema->fields[i];
    size_t length;
    char *path;
    if (!copy.path || copy.path[0] == '\0' || copy.type < TURBO_FLOW_EXPR_TYPE_BOOL ||
        copy.type > TURBO_FLOW_EXPR_TYPE_STRING) {
      return SALTS_EINVAL;
    }
    length = strlen(copy.path);
    if (length + 1u > processor->limits.max_memory_bytes - processor->memory_bytes) {
      return SALTS_ENOSPC;
    }
    path = (char *)malloc(length + 1u);
    if (!path) return SALTS_ENOMEM;
    memcpy(path, copy.path, length + 1u);
    copy.path = path;
    if (turbo_flow_stl_error(vec_push(&processor->schema_fields, &copy)) != SALTS_OK) {
      free(path);
      return SALTS_ENOMEM;
    }
    processor->memory_bytes += length + 1u;
  }
  return SALTS_OK;
}

static turbo_flow_expr_schema_t
flow_rule_schema_view(const turbo_flow_rule_processor_t *processor) {
  turbo_flow_expr_schema_t schema = {0};
  if (!processor) return schema;
  schema.field_count = vec_size(&processor->schema_fields);
  if (schema.field_count > 0u) {
    schema.fields =
        (const turbo_flow_expr_schema_field_t *)vec_at_const(&processor->schema_fields, 0u);
  }
  return schema;
}

static int flow_rule_facts_match_program(const turbo_flow_rule_processor_t *processor,
                                         const turbo_flow_rule_facts_t *facts) {
  size_t count = vec_size(&processor->schema_fields);
  if (count == 0u) return facts->schema == NULL || facts->schema->field_count == 0u;
  if (!facts->schema || facts->schema->field_count != count) return 0;
  for (size_t i = 0; i < count; ++i) {
    const turbo_flow_expr_schema_field_t *expected =
        (const turbo_flow_expr_schema_field_t *)vec_at_const(&processor->schema_fields, i);
    const turbo_flow_expr_schema_field_t *actual = &facts->schema->fields[i];
    if (!expected || !actual->path || expected->type != actual->type ||
        expected->field_id != actual->field_id || strcmp(expected->path, actual->path) != 0) {
      return 0;
    }
  }
  return 1;
}

void turbo_flow_rule_processor_destroy(turbo_flow_rule_processor_t *processor) {
  if (!processor) return;
  for (size_t i = 0; i < vec_size(&processor->rules); ++i) {
    flow_compiled_rule_t *rule = (flow_compiled_rule_t *)vec_at(&processor->rules, i);
    if (rule) turbo_flow_expr_destroy(rule->predicate);
  }
  vec_destroy(&processor->rules);
  flow_rule_schema_destroy(processor);
  tstr_freep(&processor->resource_uid);
  tstr_freep(&processor->owner_name);
  free(processor);
}

int turbo_flow_rule_processor_create(const turbo_flow_rule_processor_config_t *config,
                                     turbo_flow_rule_processor_t **out, turbo_flow_error_t *error) {
  turbo_flow_rule_processor_t *processor;
  int rc;
  if (!out) return SALTS_EINVAL;
  *out = NULL;
  if (!flow_rule_config_valid(config)) return SALTS_EINVAL;
  processor = (turbo_flow_rule_processor_t *)calloc(1, sizeof(*processor));
  if (!processor) return SALTS_ENOMEM;
  rc = turbo_flow_stl_error(vec_init_bytes(&processor->rules, sizeof(flow_compiled_rule_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX));
  if (rc != SALTS_OK) {
    free(processor);
    return rc;
  }
  rc = turbo_flow_stl_error(vec_init_bytes(&processor->schema_fields, sizeof(turbo_flow_expr_schema_field_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX));
  if (rc != SALTS_OK) {
    vec_destroy(&processor->rules);
    free(processor);
    return rc;
  }
  processor->domain = config->domain;
  processor->mode = config->mode;
  processor->limits = config->limits;
  processor->facts_provider = config->facts_provider;
  processor->facts_provider_ctx = config->facts_provider_ctx;
  processor->memory_bytes = sizeof(*processor);
  processor->resource_uid = tstr_dup(config->resource_uid);
  processor->owner_name = tstr_dup(config->owner_name);
  atomic_init(&processor->evaluations, 0u);
  atomic_init(&processor->matches, 0u);
  atomic_init(&processor->failures, 0u);
  atomic_init(&processor->last_status, SALTS_OK);
  if (!processor->resource_uid || !processor->owner_name) {
    rc = SALTS_ENOMEM;
    goto fail;
  }
  if (processor->memory_bytes > config->limits.max_memory_bytes) {
    rc = SALTS_ENOSPC;
    goto fail;
  }
  rc = turbo_flow_stl_error(vec_reserve(&processor->rules, config->rule_count));
  if (rc != SALTS_OK) goto fail;
  if (config->rule_count >
      (config->limits.max_memory_bytes - processor->memory_bytes) / sizeof(flow_compiled_rule_t)) {
    rc = SALTS_ENOSPC;
    goto fail;
  }
  processor->memory_bytes += config->rule_count * sizeof(flow_compiled_rule_t);
  rc = flow_rule_schema_copy(processor, config->schema);
  if (rc != SALTS_OK) goto fail;
  for (size_t i = 0; i < config->rule_count; ++i) {
    const turbo_flow_rule_t *input = &config->rules[i];
    flow_compiled_rule_t rule;
    size_t length;
    if (!input->predicate || input->predicate[0] == '\0' ||
        !flow_rule_action_valid(config->domain, &input->action)) {
      rc = SALTS_EINVAL;
      goto fail;
    }
    length = input->predicate_len ? input->predicate_len : strlen(input->predicate);
    memset(&rule, 0, sizeof(rule));
    rc = turbo_flow_expr_compile(input->predicate, length, config->schema, &rule.predicate, error);
    if (rc != SALTS_OK) goto fail;
    if (turbo_flow_expr_result_type(rule.predicate) != TURBO_FLOW_EXPR_TYPE_BOOL) {
      turbo_flow_expr_destroy(rule.predicate);
      rc = SALTS_EPROTO;
      goto fail;
    }
    rule.instructions = flow_expr_ast_node_count(&rule.predicate->ast);
    rule.memory_bytes = length + 1u + rule.instructions * sizeof(flow_expr_node_t);
    if (rule.instructions > config->limits.max_instructions - processor->instructions ||
        rule.memory_bytes > config->limits.max_memory_bytes - processor->memory_bytes) {
      turbo_flow_expr_destroy(rule.predicate);
      rc = SALTS_ENOSPC;
      goto fail;
    }
    rule.action = input->action;
    processor->instructions += rule.instructions;
    processor->memory_bytes += rule.memory_bytes;
    rc = turbo_flow_stl_error(vec_push(&processor->rules, &rule));
    if (rc != SALTS_OK) {
      turbo_flow_expr_destroy(rule.predicate);
      goto fail;
    }
  }
  *out = processor;
  return SALTS_OK;

fail:
  turbo_flow_rule_processor_destroy(processor);
  return rc;
}

static int flow_rule_resolved_error(turbo_flow_config_error_t *error, int status,
                                    const char *channel_name, const char *field,
                                    const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field && field[0]) {
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", channel_name,
                     field);
    } else {
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s", channel_name);
    }
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_rule_resolved_key_allowed(const char *key, const char *const *allowed,
                                          size_t allowed_count) {
  for (size_t i = 0u; i < allowed_count; ++i)
    if (strcmp(key, allowed[i]) == 0) return 1;
  return 0;
}

static int flow_rule_resolved_fields(const json_value_t *object, const char *channel_name,
                                     const char *scope, const char *const *allowed,
                                     size_t allowed_count, turbo_flow_config_error_t *error) {
  if (!object || json_type(object) != JSON_OBJECT) {
    return flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, scope, "expected a mapping");
  }
  for (size_t i = 0u; i < json_object_size(object); ++i) {
    const char *key = json_object_key(object, i);
    if (!key || !flow_rule_resolved_key_allowed(key, allowed, allowed_count)) {
      char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
      if (scope && scope[0]) (void)snprintf(path, sizeof(path), "%s.%s", scope, key ? key : "?");
      else (void)snprintf(path, sizeof(path), "%s", key ? key : "?");
      return flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, path,
                                      "unknown RuleSet field");
    }
  }
  return SALTS_OK;
}

static const char *flow_rule_resolved_string(const json_value_t *object, const char *field) {
  json_value_t *value = object ? json_object_get(object, field) : NULL;
  return value && json_type(value) == JSON_STRING ? json_string(value) : NULL;
}

static int flow_rule_resolved_u64(const json_value_t *object, const char *field, uint64_t maximum,
                                  uint64_t *out) {
  json_value_t *value = object ? json_object_get(object, field) : NULL;
  double number;
  uint64_t converted;
  if (!value || json_type(value) != JSON_NUMBER || !out) return SALTS_EINVAL;
  number = json_number(value);
  if (!isfinite(number) || number < 0.0 || number > (double)maximum ||
      number > 9007199254740991.0) {
    return SALTS_ERANGE;
  }
  converted = (uint64_t)number;
  if ((double)converted != number) return SALTS_EINVAL;
  *out = converted;
  return SALTS_OK;
}

static int flow_rule_resolved_i32(const json_value_t *object, const char *field, int *out) {
  json_value_t *value = object ? json_object_get(object, field) : NULL;
  double number;
  int converted;
  if (!value || json_type(value) != JSON_NUMBER || !out) return SALTS_EINVAL;
  number = json_number(value);
  if (!isfinite(number) || number < INT_MIN || number > INT_MAX) return SALTS_ERANGE;
  converted = (int)number;
  if ((double)converted != number) return SALTS_EINVAL;
  *out = converted;
  return SALTS_OK;
}

static int flow_rule_resolved_no_fields(const json_value_t *object, const char *const *fields,
                                        size_t field_count) {
  for (size_t i = 0u; i < field_count; ++i)
    if (json_object_get(object, fields[i])) return SALTS_EINVAL;
  return SALTS_OK;
}

static int flow_rule_resolved_parse_action(const json_value_t *object, const char *channel_name,
                                           size_t index, turbo_flow_rule_action_t *action,
                                           turbo_flow_config_error_t *error) {
  static const char *const optional[] = {"key", "value", "mask", "status"};
  const char *name = flow_rule_resolved_string(object, "action");
  const char *key = flow_rule_resolved_string(object, "key");
  char scope[64];
  uint64_t value = 0u;
  uint64_t mask = 0u;
  int status = SALTS_OK;
  int rc = SALTS_EINVAL;
  (void)snprintf(scope, sizeof(scope), "rules[%llu]", (unsigned long long)index);
  *action = (turbo_flow_rule_action_t)TURBO_FLOW_RULE_ACTION_INIT;
  if (!name || !name[0]) goto invalid;
  if (strcmp(name, "route") == 0 || strcmp(name, "batch_key") == 0 ||
      strcmp(name, "retry_class") == 0) {
    static const char *const forbidden[] = {"value", "mask", "status"};
    if (!key || !key[0] || strlen(key) > TURBO_FLOW_RULE_KEY_MAX ||
        flow_rule_resolved_no_fields(object, forbidden, sizeof(forbidden) / sizeof(forbidden[0])) !=
            SALTS_OK) {
      goto invalid;
    }
    action->kind = strcmp(name, "route") == 0       ? TURBO_FLOW_RULE_ACTION_ROUTE
                   : strcmp(name, "batch_key") == 0 ? TURBO_FLOW_RULE_ACTION_BATCH_KEY
                                                    : TURBO_FLOW_RULE_ACTION_RETRY_CLASS;
    memcpy(action->key, key, strlen(key) + 1u);
    return SALTS_OK;
  }
  if (strcmp(name, "drop") == 0) {
    if (flow_rule_resolved_no_fields(object, optional, sizeof(optional) / sizeof(optional[0])) !=
        SALTS_OK) {
      goto invalid;
    }
    action->kind = TURBO_FLOW_RULE_ACTION_DROP;
    return SALTS_OK;
  }
  if (strcmp(name, "dead_letter") == 0 || strcmp(name, "mutate_status") == 0) {
    static const char *const forbidden[] = {"key", "value", "mask"};
    if (flow_rule_resolved_no_fields(object, forbidden, sizeof(forbidden) / sizeof(forbidden[0])) !=
            SALTS_OK ||
        flow_rule_resolved_i32(object, "status", &status) != SALTS_OK ||
        (strcmp(name, "dead_letter") == 0 && status == SALTS_OK)) {
      goto invalid;
    }
    action->kind = strcmp(name, "dead_letter") == 0 ? TURBO_FLOW_RULE_ACTION_DEAD_LETTER
                                                    : TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE;
    action->private_field = TURBO_FLOW_RULE_PRIVATE_MSG_STATUS;
    action->status = status;
    return SALTS_OK;
  }
  if (strcmp(name, "mutate_type") == 0 || strcmp(name, "mutate_flags") == 0) {
    static const char *const forbidden[] = {"key", "status"};
    if (flow_rule_resolved_no_fields(object, forbidden, sizeof(forbidden) / sizeof(forbidden[0])) !=
            SALTS_OK ||
        flow_rule_resolved_u64(object, "value", UINT32_MAX, &value) != SALTS_OK ||
        flow_rule_resolved_u64(object, "mask", UINT32_MAX, &mask) != SALTS_OK) {
      goto invalid;
    }
    action->kind = TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE;
    action->private_field = strcmp(name, "mutate_type") == 0 ? TURBO_FLOW_RULE_PRIVATE_MSG_TYPE
                                                             : TURBO_FLOW_RULE_PRIVATE_MSG_FLAGS;
    action->value = value;
    action->mask = mask;
    return SALTS_OK;
  }

invalid:
  return flow_rule_resolved_error(error, rc, channel_name, scope,
                                  "invalid or unsupported RuleSet action");
}

static int flow_rule_resolved_parse_rule(const json_value_t *value, const char *channel_name,
                                         size_t index, turbo_flow_rule_t *rule,
                                         turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"when", "action", "key", "value", "mask", "status"};
  const char *predicate;
  char scope[64];
  int rc;
  (void)snprintf(scope, sizeof(scope), "rules[%llu]", (unsigned long long)index);
  rc = flow_rule_resolved_fields(value, channel_name, scope, allowed,
                                 sizeof(allowed) / sizeof(allowed[0]), error);
  if (rc != SALTS_OK) return rc;
  predicate = flow_rule_resolved_string(value, "when");
  if (!predicate || !predicate[0]) {
    return flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, scope,
                                    "rule when must be a non-empty string");
  }
  memset(rule, 0, sizeof(*rule));
  rule->predicate = predicate;
  rc = flow_rule_resolved_parse_action(value, channel_name, index, &rule->action, error);
  return rc;
}

static int flow_rule_resolved_limit(const json_value_t *fields, const char *channel_name,
                                    const char *field, uint64_t maximum, size_t *out,
                                    turbo_flow_config_error_t *error) {
  uint64_t value;
  int rc;
  if (!json_object_get(fields, field)) return SALTS_OK;
  rc = flow_rule_resolved_u64(fields, field, maximum, &value);
  if (rc != SALTS_OK || value == 0u) {
    return flow_rule_resolved_error(error, rc == SALTS_OK ? SALTS_ERANGE : rc, channel_name, field,
                                    "RuleSet limit must be a positive bounded integer");
  }
  *out = (size_t)value;
  return SALTS_OK;
}

int turbo_flow_rule_processor_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_expr_schema_t *schema, turbo_flow_rule_facts_provider_fn facts_provider,
    void *facts_provider_ctx, turbo_flow_rule_processor_t **out, turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"resource_uid",
                                        "owner_name",
                                        "mode",
                                        "rules",
                                        "max_instructions",
                                        "max_time_ns",
                                        "max_memory_bytes",
                                        "max_output_actions",
                                        "max_output_bytes"};
  turbo_flow_rule_processor_config_t config = TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT;
  turbo_flow_error_t expression_error = {0};
  json_value_t *document = NULL;
  vec_t rules = {0};
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *rules_value;
  const char *json;
  const char *mode;
  size_t json_size = 0u;
  int rules_initialized = 0;
  int rc;
  if (out) *out = NULL;
  if (!resolved || !channel_name || !channel_name[0] || !out || !error ||
      error->size < sizeof(*error) || (schema && schema->field_count > 0u && !facts_provider)) {
    return SALTS_EINVAL;
  }
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (json) document = json_parse(json, json_size);
  if (!document) {
    return flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, NULL,
                                    "invalid resolved configuration snapshot");
  }
  channels = json_object_get(document, "channels");
  channel = channels ? json_object_get(channels, channel_name) : NULL;
  kind = channel ? json_object_get(channel, "kind") : NULL;
  fields = channel ? json_object_get(channel, "config") : NULL;
  if (!channel || json_type(channel) != JSON_OBJECT) {
    rc = flow_rule_resolved_error(error, SALTS_ENOENT, channel_name, NULL,
                                  "RuleSet channel is not resolved");
    goto done;
  }
  if (!kind || json_type(kind) != JSON_STRING ||
      strcmp(json_string(kind), "rule_set") != 0) {
    rc = flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, NULL,
                                  "channel kind must be rule_set");
    goto done;
  }
  rc = flow_rule_resolved_fields(fields, channel_name, NULL, allowed,
                                 sizeof(allowed) / sizeof(allowed[0]), error);
  if (rc != SALTS_OK) goto done;
  config.resource_uid = flow_rule_resolved_string(fields, "resource_uid");
  config.owner_name = flow_rule_resolved_string(fields, "owner_name");
  mode = flow_rule_resolved_string(fields, "mode");
  if (!config.resource_uid || !config.resource_uid[0] || !config.owner_name ||
      !config.owner_name[0] || !mode) {
    rc = flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, NULL,
                                  "resource_uid, owner_name, and mode are required strings");
    goto done;
  }
  if (strcmp(mode, "first_match") == 0) {
    config.mode = TURBO_FLOW_RULE_FIRST_MATCH;
  } else if (strcmp(mode, "all_matches") == 0) {
    config.mode = TURBO_FLOW_RULE_ALL_MATCHES;
  } else {
    rc = flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, "mode",
                                  "mode must be first_match or all_matches");
    goto done;
  }
  config.schema = schema;
  config.facts_provider = facts_provider;
  config.facts_provider_ctx = facts_provider_ctx;
  rc = flow_rule_resolved_limit(fields, channel_name, "max_instructions", SIZE_MAX,
                                &config.limits.max_instructions, error);
  if (rc == SALTS_OK)
    rc = flow_rule_resolved_limit(fields, channel_name, "max_memory_bytes", SIZE_MAX,
                                  &config.limits.max_memory_bytes, error);
  if (rc == SALTS_OK)
    rc = flow_rule_resolved_limit(fields, channel_name, "max_output_actions",
                                  FLOW_RULE_STAGE_MAX_ACTIONS, &config.limits.max_output_actions,
                                  error);
  if (rc == SALTS_OK)
    rc = flow_rule_resolved_limit(fields, channel_name, "max_output_bytes", SIZE_MAX,
                                  &config.limits.max_output_bytes, error);
  if (rc != SALTS_OK) goto done;
  if (json_object_get(fields, "max_time_ns")) {
    uint64_t value;
    rc = flow_rule_resolved_u64(fields, "max_time_ns", UINT64_MAX, &value);
    if (rc != SALTS_OK || value == 0u) {
      rc = flow_rule_resolved_error(error, rc == SALTS_OK ? SALTS_ERANGE : rc, channel_name,
                                    "max_time_ns",
                                    "RuleSet limit must be a positive bounded integer");
      goto done;
    }
    config.limits.max_time_ns = value;
  }
  rules_value = json_object_get(fields, "rules");
  if (!rules_value || json_type(rules_value) != JSON_ARRAY ||
      json_array_size(rules_value) == 0u ||
      json_array_size(rules_value) > FLOW_RULE_RESOLVED_MAX_RULES) {
    rc = flow_rule_resolved_error(error, SALTS_EINVAL, channel_name, "rules",
                                  "rules must be a non-empty bounded array");
    goto done;
  }
  rc = turbo_flow_stl_error(vec_init_bytes(&rules, sizeof(turbo_flow_rule_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX));
  if (rc != SALTS_OK) goto done;
  rules_initialized = 1;
  rc = turbo_flow_stl_error(vec_reserve(&rules, json_array_size(rules_value)));
  for (size_t i = 0u; rc == SALTS_OK && i < json_array_size(rules_value); ++i) {
    turbo_flow_rule_t rule;
    rc = flow_rule_resolved_parse_rule(json_array_get(rules_value, i), channel_name, i, &rule,
                                       error);
    if (rc == SALTS_OK) rc = turbo_flow_stl_error(vec_push(&rules, &rule));
  }
  if (rc != SALTS_OK) goto done;
  config.rules = (const turbo_flow_rule_t *)rules.data;
  config.rule_count = vec_size(&rules);
  rc = turbo_flow_rule_processor_create(&config, out, &expression_error);
  if (rc != SALTS_OK) {
    const char *message =
        expression_error.message[0] ? expression_error.message : "RuleSet compilation failed";
    rc = flow_rule_resolved_error(error, rc, channel_name, "rules", message);
  }

done:
  if (rules_initialized) vec_destroy(&rules);
  json_free(document);
  return rc;
}

int turbo_flow_rule_processor_evaluate(const turbo_flow_rule_processor_t *processor,
                                       const turbo_flow_rule_facts_t *facts,
                                       turbo_flow_rule_action_t *actions, size_t action_capacity,
                                       turbo_flow_rule_result_t *result) {
  turbo_flow_rule_result_t local = TURBO_FLOW_RULE_RESULT_INIT;
  turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
  flow_rule_fact_reader_t reader;
  uint64_t started_at;
  int rc;
  if (!processor || !flow_rule_facts_valid(facts) || !actions || action_capacity == 0u) {
    return SALTS_EINVAL;
  }
  if (!flow_rule_facts_match_program(processor, facts)) return SALTS_EPROTO;
  if (facts->schema && facts->schema->field_count > 0u) {
    reader.facts = facts;
    context.read_schema_field = flow_rule_read_fact;
    context.schema_ctx = &reader;
  }
  context.message = facts->message;
  local.instructions = processor->instructions;
  local.memory_bytes = processor->memory_bytes;
  started_at = salts_hrtime();
  for (size_t i = 0; i < vec_size(&processor->rules); ++i) {
    const flow_compiled_rule_t *rule =
        (const flow_compiled_rule_t *)vec_at_const(&processor->rules, i);
    turbo_flow_expr_value_t value;
    memset(&value, 0, sizeof(value));
    rc = turbo_flow_expr_evaluate(rule->predicate, &context, &value);
    local.evaluated += 1u;
    local.elapsed_ns = salts_hrtime() - started_at;
    if (rc != SALTS_OK) goto done;
    if (local.elapsed_ns > processor->limits.max_time_ns) {
      rc = SALTS_ETIMEDOUT;
      goto done;
    }
    if (value.type != TURBO_FLOW_EXPR_TYPE_BOOL) {
      rc = SALTS_EPROTO;
      goto done;
    }
    if (!value.as.boolean) continue;
    local.matched += 1u;
    local.last_match = i;
    if (local.emitted >= action_capacity || local.emitted >= processor->limits.max_output_actions ||
        sizeof(*actions) > processor->limits.max_output_bytes - local.output_bytes) {
      rc = SALTS_ENOSPC;
      goto done;
    }
    actions[local.emitted++] = rule->action;
    local.output_bytes += sizeof(*actions);
    if (processor->mode == TURBO_FLOW_RULE_FIRST_MATCH) break;
  }
  rc = SALTS_OK;

done:
  local.elapsed_ns = salts_hrtime() - started_at;
  atomic_fetch_add_explicit((atomic_uint_fast64_t *)&processor->evaluations, 1u,
                            memory_order_relaxed);
  atomic_fetch_add_explicit((atomic_uint_fast64_t *)&processor->matches, local.matched,
                            memory_order_relaxed);
  if (rc != SALTS_OK) {
    atomic_fetch_add_explicit((atomic_uint_fast64_t *)&processor->failures, 1u,
                              memory_order_relaxed);
  }
  atomic_store_explicit((atomic_int *)&processor->last_status, rc, memory_order_release);
  if (result) *result = local;
  return rc;
}

static int flow_rule_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  const turbo_flow_rule_processor_t *processor = (const turbo_flow_rule_processor_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int written;
  if (!processor || !out || out->size < sizeof(*out) || !processor->resource_uid ||
      !processor->owner_name) {
    return SALTS_EINVAL;
  }
  metadata.domain = TURBO_FLOW_DOMAIN_RULES;
  metadata.kind = TURBO_FLOW_RESOURCE_RULE_SET;
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", processor->resource_uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return SALTS_ENAMETOOLONG;
  written = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", processor->owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return SALTS_ENAMETOOLONG;
  *out = metadata;
  return SALTS_OK;
}

static int flow_rule_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  turbo_flow_rule_processor_t *processor = (turbo_flow_rule_processor_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;
  if (!out || out->size < sizeof(*out)) return SALTS_EINVAL;
  rc = flow_rule_resource_metadata(ctx, &metadata);
  if (rc != SALTS_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  out->last_status = atomic_load_explicit(&processor->last_status, memory_order_acquire);
  return SALTS_OK;
}

static int flow_rule_resource_document(void *ctx, turbo_flow_resource_document_kind_t document_kind,
                                       turbo_flow_resource_document_t *out) {
  turbo_flow_rule_processor_t *processor = (turbo_flow_rule_processor_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  char payload[512];
  uint64_t evaluations;
  uint64_t matches;
  uint64_t failures;
  int last_status;
  int written;
  int rc;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return SALTS_ENOTSUP;
  if (!processor || !out) return SALTS_EINVAL;
  rc = flow_rule_resource_metadata(processor, &metadata);
  if (rc != SALTS_OK) return rc;
  evaluations = atomic_load_explicit(&processor->evaluations, memory_order_relaxed);
  matches = atomic_load_explicit(&processor->matches, memory_order_relaxed);
  failures = atomic_load_explicit(&processor->failures, memory_order_relaxed);
  last_status = atomic_load_explicit(&processor->last_status, memory_order_acquire);
  written = snprintf(payload, sizeof(payload),
                     "{\"domain\":%u,\"mode\":%u,\"rule_count\":\"%llu\","
                     "\"instructions\":\"%llu\",\"memory_bytes\":\"%llu\","
                     "\"evaluations\":\"%llu\",\"matches\":\"%llu\",\"failures\":\"%llu\","
                     "\"last_status\":%d}",
                     (unsigned)processor->domain, (unsigned)processor->mode,
                     (unsigned long long)vec_size(&processor->rules),
                     (unsigned long long)processor->instructions,
                     (unsigned long long)processor->memory_bytes, (unsigned long long)evaluations,
                     (unsigned long long)matches, (unsigned long long)failures, last_status);
  if (written < 0 || (size_t)written >= sizeof(payload)) return SALTS_ERANGE;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, &FLOW_RULE_SET_STATUS_SCHEMA,
                                                     payload, (size_t)written);
  return rc;
}

static int flow_rule_copy_key(char dst[TURBO_FLOW_RULE_KEY_MAX + 1u], const char *src) {
  size_t length = strlen(src);
  if (length > TURBO_FLOW_RULE_KEY_MAX) return SALTS_ENOSPC;
  memcpy(dst, src, length + 1u);
  return SALTS_OK;
}

int turbo_flow_rule_apply_data_actions(turbo_flow_msg_t *message,
                                       const turbo_flow_rule_action_t *actions, size_t action_count,
                                       turbo_flow_rule_data_decision_t *decision) {
  turbo_flow_rule_data_decision_t pending = TURBO_FLOW_RULE_DATA_DECISION_INIT;
  uint32_t type;
  uint32_t flags;
  int status;
  int route_set = 0;
  int batch_key_set = 0;
  int retry_class_set = 0;
  int rc;
  if (!message || !decision || decision->size < sizeof(*decision) ||
      (action_count > 0u && !actions)) {
    return SALTS_EINVAL;
  }
  type = message->type;
  flags = message->flags;
  status = message->status;
  for (size_t i = 0; i < action_count; ++i) {
    const turbo_flow_rule_action_t *action = &actions[i];
    if (!flow_rule_action_valid(TURBO_FLOW_RULE_DATA, action)) return SALTS_EINVAL;
    switch (action->kind) {
    case TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE:
      if (action->private_field == TURBO_FLOW_RULE_PRIVATE_MSG_TYPE) {
        if (action->value > UINT32_MAX || action->mask > UINT32_MAX) return SALTS_ERANGE;
        type =
            (type & ~(uint32_t)action->mask) | ((uint32_t)action->value & (uint32_t)action->mask);
      } else if (action->private_field == TURBO_FLOW_RULE_PRIVATE_MSG_FLAGS) {
        if (action->value > UINT32_MAX || action->mask > UINT32_MAX) return SALTS_ERANGE;
        flags =
            (flags & ~(uint32_t)action->mask) | ((uint32_t)action->value & (uint32_t)action->mask);
      } else {
        status = action->status;
      }
      break;
    case TURBO_FLOW_RULE_ACTION_ROUTE:
      if (route_set) return SALTS_EPROTO;
      rc = flow_rule_copy_key(pending.route, action->key);
      if (rc != SALTS_OK) return rc;
      route_set = 1;
      break;
    case TURBO_FLOW_RULE_ACTION_DROP:
      pending.dropped = 1;
      break;
    case TURBO_FLOW_RULE_ACTION_BATCH_KEY:
      if (batch_key_set) return SALTS_EPROTO;
      rc = flow_rule_copy_key(pending.batch_key, action->key);
      if (rc != SALTS_OK) return rc;
      batch_key_set = 1;
      break;
    case TURBO_FLOW_RULE_ACTION_RETRY_CLASS:
      if (retry_class_set) return SALTS_EPROTO;
      rc = flow_rule_copy_key(pending.retry_class, action->key);
      if (rc != SALTS_OK) return rc;
      retry_class_set = 1;
      break;
    case TURBO_FLOW_RULE_ACTION_DEAD_LETTER:
      pending.dead_letter = 1;
      pending.dead_letter_status = action->status;
      break;
    default:
      return SALTS_EINVAL;
    }
  }
  if (pending.dropped && pending.dead_letter) return SALTS_EPROTO;
  message->type = type;
  message->flags = flags;
  message->status = status;
  *decision = pending;
  return SALTS_OK;
}

static int flow_rule_data_stage(turbo_flow_msg_t *message, void *ctx) {
  turbo_flow_rule_processor_t *processor = (turbo_flow_rule_processor_t *)ctx;
  turbo_flow_expr_schema_t schema;
  const turbo_flow_expr_value_t *values = NULL;
  size_t value_count = 0u;
  turbo_flow_rule_facts_t facts = TURBO_FLOW_RULE_FACTS_INIT;
  turbo_flow_rule_result_t result = TURBO_FLOW_RULE_RESULT_INIT;
  turbo_flow_rule_data_decision_t decision = TURBO_FLOW_RULE_DATA_DECISION_INIT;
  static SALTS_THREAD_LOCAL turbo_flow_rule_action_t actions[FLOW_RULE_STAGE_MAX_ACTIONS];
  size_t capacity;
  int rc;
  if (!processor || !message || processor->domain != TURBO_FLOW_RULE_DATA) {
    return SALTS_EINVAL;
  }
  schema = flow_rule_schema_view(processor);
  facts.message = message;
  if (schema.field_count > 0u) {
    if (!processor->facts_provider) return SALTS_EINVAL;
    rc = processor->facts_provider(message, &schema, &values, &value_count,
                                   processor->facts_provider_ctx);
    if (rc != SALTS_OK) return rc;
    facts.schema = &schema;
    facts.values = values;
    facts.value_count = value_count;
  }
  capacity = processor->limits.max_output_actions;
  if (capacity > vec_size(&processor->rules)) capacity = vec_size(&processor->rules);
  if (capacity == 0u || capacity > FLOW_RULE_STAGE_MAX_ACTIONS) return SALTS_ENOSPC;
  /* Rule actions include a large command envelope; keep bounded ingress coroutine stacks small. */
  rc = turbo_flow_rule_processor_evaluate(processor, &facts, actions, capacity, &result);
  if (rc == SALTS_OK) {
    rc = turbo_flow_rule_apply_data_actions(message, actions, result.emitted, &decision);
  }
  if (rc != SALTS_OK) return rc;
  decision.stage_index = UINT32_MAX;
  message->data_decision = decision;
  return SALTS_OK;
}

int turbo_flow_rule_register_data_stage(turbo_flow_t *flow, const char *stage_name,
                                        turbo_flow_rule_processor_t *processor,
                                        const turbo_flow_stage_options_t *options) {
  turbo_flow_stage_options_t effective;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  if (!flow || !stage_name || stage_name[0] == '\0' || !processor ||
      processor->domain != TURBO_FLOW_RULE_DATA ||
      vec_size(&processor->schema_fields) != 0u ||
      processor->limits.max_output_actions > FLOW_RULE_STAGE_MAX_ACTIONS) {
    return SALTS_EINVAL;
  }
  effective = options ? *options : (turbo_flow_stage_options_t){0};
  effective.mutability = TURBO_FLOW_STAGE_MUTATES_PRIVATE;
  effective.effects |= TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION;
  resource.owner_name = processor->owner_name;
  resource.ops.metadata = flow_rule_resource_metadata;
  resource.ops.snapshot = flow_rule_resource_snapshot;
  resource.ops.document = flow_rule_resource_document;
  resource.ctx = processor;
  return turbo_flow_register_stage_with_resources(flow, stage_name, flow_rule_data_stage, processor,
                                                  &effective, &resource, 1u);
}

int turbo_flow_rule_register_data_operation(turbo_flow_t *flow, const char *resource_name,
                                            turbo_flow_rule_processor_t *processor) {
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  size_t resources_before;
  size_t providers_before;
  size_t modules_before;
  size_t primitives_before;
  size_t operations_before;
  int rc;

  if (!flow || !resource_name || resource_name[0] == '\0' || !processor ||
      processor->domain != TURBO_FLOW_RULE_DATA ||
      (vec_size(&processor->schema_fields) > 0u && !processor->facts_provider) ||
      processor->limits.max_output_actions > FLOW_RULE_STAGE_MAX_ACTIONS) {
    return SALTS_EINVAL;
  }

  resources_before = vec_size(&flow->resources);
  providers_before = vec_size(&flow->operation_providers);
  modules_before = vec_size(&flow->modules);
  primitives_before = vec_size(&flow->primitives);
  operations_before = vec_size(&flow->operations);

  rc = flow_rule_register_apply_contract(flow, resource_name);
  if (rc != SALTS_OK) {
    flow_rule_rollback_operation_registration(flow, resources_before, providers_before,
                                              modules_before, primitives_before, operations_before);
    return rc;
  }
  rc = flow_rule_register_module_contract(flow);
  if (rc != SALTS_OK) {
    flow_rule_rollback_operation_registration(flow, resources_before, providers_before,
                                              modules_before, primitives_before, operations_before);
    return rc;
  }

  resource.owner_name = processor->owner_name;
  resource.ops.metadata = flow_rule_resource_metadata;
  resource.ops.snapshot = flow_rule_resource_snapshot;
  resource.ops.document = flow_rule_resource_document;
  resource.ctx = processor;
  rc =
      turbo_flow_register_resource_provider(flow, resource.owner_name, &resource.ops, resource.ctx);
  if (rc != SALTS_OK) {
    flow_rule_rollback_operation_registration(flow, resources_before, providers_before,
                                              modules_before, primitives_before, operations_before);
    return rc;
  }

  provider.operation_name = TURBO_FLOW_RULE_APPLY_OPERATION;
  provider.resource_name = resource_name;
  provider.fn = flow_rule_data_stage;
  provider.ctx = processor;
  provider.options.mutability = TURBO_FLOW_STAGE_MUTATES_PRIVATE;
  provider.options.effects = TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION;
  rc = turbo_flow_register_operation_provider(flow, &provider);
  if (rc != SALTS_OK) {
    flow_rule_rollback_operation_registration(flow, resources_before, providers_before,
                                              modules_before, primitives_before, operations_before);
    return rc;
  }
  rc = turbo_flow_bind_operation_provider_module(flow, TURBO_FLOW_RULE_MODULE,
                                                 TURBO_FLOW_RULE_APPLY_OPERATION, resource_name);
  if (rc != SALTS_OK) {
    flow_rule_rollback_operation_registration(flow, resources_before, providers_before,
                                              modules_before, primitives_before, operations_before);
    return rc;
  }
  return SALTS_OK;
}

int turbo_flow_rule_authorize_command(const turbo_flow_rule_action_t *proposal,
                                      const turbo_flow_rule_authority_t *authority,
                                      turbo_flow_resource_command_t *command_out) {
  uint32_t command_bit;
  if (!authority || authority->size < sizeof(*authority) || !command_out ||
      !flow_rule_action_valid(TURBO_FLOW_RULE_CONTROL, proposal) ||
      !memchr(authority->target_uid, '\0', sizeof(authority->target_uid))) {
    return SALTS_EINVAL;
  }
  command_bit = UINT32_C(1) << (uint32_t)proposal->command.kind;
  if ((authority->allowed_command_mask & command_bit) == 0u) return SALTS_EPERM;
  if (authority->observed_generation == 0u ||
      proposal->command.expected_generation != authority->observed_generation) {
    return SALTS_EBUSY;
  }
  if (authority->target_uid[0] != '\0' &&
      strcmp(authority->target_uid, proposal->command.target_uid) != 0) {
    return SALTS_EPERM;
  }
  *command_out = proposal->command;
  return SALTS_OK;
}
