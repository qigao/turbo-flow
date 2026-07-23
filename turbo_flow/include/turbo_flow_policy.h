#ifndef TURBO_FLOW_POLICY_H
#define TURBO_FLOW_POLICY_H

#include "turbo_flow_config.h"
#include "turbo_flow_expr.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_RULE_PROGRAM_ABI_V1 1u
#define TURBO_FLOW_RULE_ACTION_ABI_V1 1u
#define TURBO_FLOW_RULE_KEY_MAX TURBO_FLOW_DATA_DECISION_KEY_MAX
#define TURBO_FLOW_RULE_MODULE "rules.policy"
#define TURBO_FLOW_RULE_APPLY_OPERATION "rules.apply"
#define TURBO_FLOW_RULE_SET_TYPE "RuleSet"

typedef struct turbo_flow_rule_processor_s turbo_flow_rule_processor_t;

typedef enum turbo_flow_rule_mode_e {
  TURBO_FLOW_RULE_FIRST_MATCH = 0,
  TURBO_FLOW_RULE_ALL_MATCHES
} turbo_flow_rule_mode_t;

typedef enum turbo_flow_rule_domain_e {
  TURBO_FLOW_RULE_DATA = 1,
  TURBO_FLOW_RULE_CONTROL
} turbo_flow_rule_domain_t;

typedef enum turbo_flow_rule_action_kind_e {
  TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE = 1,
  TURBO_FLOW_RULE_ACTION_ROUTE,
  TURBO_FLOW_RULE_ACTION_DROP,
  TURBO_FLOW_RULE_ACTION_BATCH_KEY,
  TURBO_FLOW_RULE_ACTION_RETRY_CLASS,
  TURBO_FLOW_RULE_ACTION_DEAD_LETTER,
  TURBO_FLOW_RULE_ACTION_PROPOSE_COMMAND
} turbo_flow_rule_action_kind_t;

typedef enum turbo_flow_rule_private_field_e {
  TURBO_FLOW_RULE_PRIVATE_MSG_TYPE = 1,
  TURBO_FLOW_RULE_PRIVATE_MSG_FLAGS,
  TURBO_FLOW_RULE_PRIVATE_MSG_STATUS
} turbo_flow_rule_private_field_t;

/** Pointer-free action copied into and out of an immutable rule program. */
typedef struct turbo_flow_rule_action_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_rule_action_kind_t kind;
  turbo_flow_rule_private_field_t private_field;
  uint64_t value;
  uint64_t mask;
  int status;
  char key[TURBO_FLOW_RULE_KEY_MAX + 1u];
  turbo_flow_resource_command_t command;
} turbo_flow_rule_action_t;

#define TURBO_FLOW_RULE_ACTION_INIT                                                               \
  {sizeof(turbo_flow_rule_action_t), TURBO_FLOW_RULE_ACTION_ABI_V1, 0, 0, 0u, UINT64_MAX,         \
   TURBO_OK, {0}, TURBO_FLOW_RESOURCE_COMMAND_INIT}

typedef struct turbo_flow_rule_s {
  /** Borrowed only while the immutable program is compiled. */
  const char *predicate;
  /** Zero uses strlen(predicate). */
  size_t predicate_len;
  /** Pointer-free template deep-copied into the program. */
  turbo_flow_rule_action_t action;
} turbo_flow_rule_t;

typedef struct turbo_flow_rule_limits_s {
  size_t max_instructions;
  uint64_t max_time_ns;
  size_t max_memory_bytes;
  size_t max_output_actions;
  size_t max_output_bytes;
} turbo_flow_rule_limits_t;

#define TURBO_FLOW_RULE_LIMITS_DEFAULT {4096u, UINT64_C(10000000), 1048576u, 64u, 65536u}

/**
 * Materialize the typed schema facts used by one rule evaluation.
 *
 * The provider owns the returned value array and any string storage. The array must remain
 * valid, immutable, and type-compatible with `schema` until the current `rules.apply` call
 * returns. The provider must not retain `message`, `schema`, or the output pointers. Providers
 * may be called concurrently when the enclosing flow is concurrent, so `ctx` must provide the
 * required synchronization or use reentrant storage.
 */
typedef int (*turbo_flow_rule_facts_provider_fn)(
    const turbo_flow_msg_t *message, const turbo_flow_expr_schema_t *schema,
    const turbo_flow_expr_value_t **values_out, size_t *value_count_out, void *ctx);

/**
 * Adapt one opaque message projection into the typed values required by a rule schema.
 *
 * The adapter owns the projection implementation and must validate `projection_schema` before
 * reading `projection`. Returned values remain borrowed until the enclosing `rules.apply` call
 * returns. The adapter must not retain the projection, either schema, or output pointers.
 */
typedef int (*turbo_flow_rule_projection_materialize_fn)(
    const void *projection, const turbo_flow_data_schema_t *projection_schema,
    const turbo_flow_expr_schema_t *rule_schema, const turbo_flow_expr_value_t **values_out,
    size_t *value_count_out, void *ctx);

typedef struct turbo_flow_rule_projection_provider_s {
  size_t size;
  turbo_flow_rule_projection_materialize_fn materialize;
  void *ctx;
} turbo_flow_rule_projection_provider_t;

#define TURBO_FLOW_RULE_PROJECTION_PROVIDER_INIT \
  {sizeof(turbo_flow_rule_projection_provider_t), NULL, NULL}

/** Standard facts provider that reads a message's attached projection through the adapter. */
CXX_C_API int turbo_flow_rule_projection_facts_provider(
    const turbo_flow_msg_t *message, const turbo_flow_expr_schema_t *schema,
    const turbo_flow_expr_value_t **values_out, size_t *value_count_out, void *ctx);

typedef struct turbo_flow_rule_processor_config_s {
  size_t size;
  uint32_t program_abi_version;
  /** Stable rule-set resource identity. Required and deep-copied. */
  const char *resource_uid;
  /** Stable rule-set owner name. Required and deep-copied. */
  const char *owner_name;
  turbo_flow_rule_domain_t domain;
  turbo_flow_rule_mode_t mode;
  const turbo_flow_rule_t *rules;
  size_t rule_count;
  /** Borrowed only while predicates are compiled. */
  const turbo_flow_expr_schema_t *schema;
  turbo_flow_rule_limits_t limits;
  /** Borrowed callback/context; required by rules.apply when schema contains fields. */
  turbo_flow_rule_facts_provider_fn facts_provider;
  void *facts_provider_ctx;
} turbo_flow_rule_processor_config_t;

#define TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT                                                     \
  {sizeof(turbo_flow_rule_processor_config_t), TURBO_FLOW_RULE_PROGRAM_ABI_V1,                    \
   NULL, NULL, TURBO_FLOW_RULE_DATA, TURBO_FLOW_RULE_FIRST_MATCH, NULL, 0u, NULL,                 \
   TURBO_FLOW_RULE_LIMITS_DEFAULT, NULL, NULL}

/** Immutable, already materialized facts for one deterministic evaluation. */
typedef struct turbo_flow_rule_facts_s {
  size_t size;
  const turbo_flow_msg_t *message;
  const turbo_flow_expr_schema_t *schema;
  const turbo_flow_expr_value_t *values;
  size_t value_count;
} turbo_flow_rule_facts_t;

#define TURBO_FLOW_RULE_FACTS_INIT {sizeof(turbo_flow_rule_facts_t), NULL, NULL, NULL, 0u}

typedef struct turbo_flow_rule_result_s {
  size_t size;
  size_t evaluated;
  size_t matched;
  size_t emitted;
  size_t last_match;
  size_t instructions;
  size_t memory_bytes;
  size_t output_bytes;
  uint64_t elapsed_ns;
} turbo_flow_rule_result_t;

#define TURBO_FLOW_RULE_RESULT_INIT                                                               \
  {sizeof(turbo_flow_rule_result_t), 0u, 0u, 0u, SIZE_MAX, 0u, 0u, 0u, 0u}

typedef turbo_flow_data_decision_t turbo_flow_rule_data_decision_t;

#define TURBO_FLOW_RULE_DATA_DECISION_INIT TURBO_FLOW_DATA_DECISION_INIT

typedef struct turbo_flow_rule_authority_s {
  size_t size;
  /** Bit N authorizes turbo_flow_resource_command_kind_t value N. */
  uint32_t allowed_command_mask;
  uint64_t observed_generation;
  /** Empty authorizes any target; otherwise requires this exact stable UID. */
  char target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
} turbo_flow_rule_authority_t;

#define TURBO_FLOW_RULE_AUTHORITY_INIT                                                            \
  {sizeof(turbo_flow_rule_authority_t), 0u, 0u, {0}}

CXX_C_API int turbo_flow_rule_processor_create(const turbo_flow_rule_processor_config_t *config,
                                               turbo_flow_rule_processor_t **out,
                                               turbo_flow_error_t *error);
CXX_C_API void turbo_flow_rule_processor_destroy(turbo_flow_rule_processor_t *processor);

/**
 * Create one data RuleSet from strict `channels.<name>.kind: rule_set` YAML.
 *
 * The resolved channel owns identity, mode, bounds, and a non-empty rule array. Each rule uses
 * `{when, action}` plus the fields required by that action. The supplied schema and facts provider
 * remain caller-owned under the same contract as turbo_flow_rule_processor_create().
 */
CXX_C_API int turbo_flow_rule_processor_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_expr_schema_t *schema, turbo_flow_rule_facts_provider_fn facts_provider,
    void *facts_provider_ctx, turbo_flow_rule_processor_t **out,
    turbo_flow_config_error_t *error);

/** Evaluate immutable facts and emit caller-owned actions without changing runtime state. */
CXX_C_API int turbo_flow_rule_processor_evaluate(const turbo_flow_rule_processor_t *processor,
                                                 const turbo_flow_rule_facts_t *facts,
                                                 turbo_flow_rule_action_t *actions,
                                                 size_t action_capacity,
                                                 turbo_flow_rule_result_t *result);

/** Validate and atomically apply data actions to one private message copy and decision. */
CXX_C_API int turbo_flow_rule_apply_data_actions(turbo_flow_msg_t *message,
                                                 const turbo_flow_rule_action_t *actions,
                                                 size_t action_count,
                                                 turbo_flow_rule_data_decision_t *decision);

/** Register an inline data-rule stage. The caller retains immutable program ownership. */
CXX_C_API int turbo_flow_rule_register_data_stage(turbo_flow_t *flow, const char *stage_name,
                                                  turbo_flow_rule_processor_t *processor,
                                                  const turbo_flow_stage_options_t *options);

/**
 * Register the standard TurboFlow Policy data operation and bind one rule-set resource to it.
 *
 * The DSL can then use `operation rules.apply resource <resource_name>` from any graph node.
 * To bind schema-backed rules through this operation, the processor configuration must provide
 * `facts_provider`. The provider materializes typed values for the current message; the processor
 * never reads opaque message bytes directly. Direct `turbo_flow_rule_processor_evaluate()` calls
 * may continue to supply an already materialized `turbo_flow_rule_facts_t` instead.
 * The caller retains immutable processor ownership for the lifetime of the flow.
 */
CXX_C_API int turbo_flow_rule_register_data_operation(turbo_flow_t *flow, const char *resource_name,
                                                       turbo_flow_rule_processor_t *processor);

/** Authorize one control proposal. This function never dispatches the owner command. */
CXX_C_API int turbo_flow_rule_authorize_command(const turbo_flow_rule_action_t *proposal,
                                                const turbo_flow_rule_authority_t *authority,
                                                turbo_flow_resource_command_t *command_out);

#ifdef __cplusplus
}
#endif

#endif
