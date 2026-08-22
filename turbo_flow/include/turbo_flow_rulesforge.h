#ifndef TURBO_FLOW_RULESFORGE_H
#define TURBO_FLOW_RULESFORGE_H

#include "turbo_flow.h"

/* RulesForge 0.9 still consumes TurboUtils' removed compatibility marker.
 * Keep the shim scoped to its header until that package owns its API macro. */
#ifndef CXX_C_API
#define CXX_C_API TURBO_C_API
#define TURBO_FLOW_UNDEF_RULESFORGE_CXX_C_API
#endif
#include "rules_forge.h"
#ifdef TURBO_FLOW_UNDEF_RULESFORGE_CXX_C_API
#undef TURBO_FLOW_UNDEF_RULESFORGE_CXX_C_API
#undef CXX_C_API
#endif

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_RULEFORGE_MODULE "rulesforge.bridge"
#define TURBO_FLOW_RULEFORGE_APPLY_OPERATION "rulesforge.apply"
#define TURBO_FLOW_RULEFORGE_RESOURCE_TYPE "RuleForgeResource"
#define TURBO_FLOW_RULEFORGE_DATABIND_PROJECTION_TYPE "RulesForge.DataBindObject"
#define TURBO_FLOW_RULEFORGE_DATABIND_VALUE_PROJECTION_TYPE "TurboUtils.DataBindValue"
#define TURBO_FLOW_RULEFORGE_DEFAULT_MAX_RULES 1024

typedef int (*turbo_flow_rulesforge_data_fn)(turbo_flow_msg_t *message, const char *resource_name,
                                            void *ctx);

/**
 * RulesForge-backed data operation registration.
 *
 * The callback receives the mutable message and must return TurboFlow status code.
 * A RulesForge no-match is a successful evaluation and must return TURBO_OK.
 * To filter data, record the match result in the stage's working message and select
 * downstream stages with a conditional graph route, for example:
 *
 *   route rulesforge -> sink when msg.rule_matched
 *
 * The callback is responsible for its own concurrency safety when the graph
 * executes stages in parallel.
 */
typedef struct turbo_flow_rulesforge_data_operation_registration_s {
  size_t size;
  /** Borrowed primitive binding name; must remain valid for the flow lifetime. */
  const char *resource_name;
  /** Required callback entrypoint. */
  turbo_flow_rulesforge_data_fn fn;
  /** Borrowed user context, passed to `fn`; must remain valid for the flow lifetime. */
  void *callback_ctx;
  /** Stage behavior contract used by the operation provider. */
  turbo_flow_stage_options_t options;
} turbo_flow_rulesforge_data_operation_registration_t;

#define TURBO_FLOW_RULEFORGE_DATA_OPERATION_REGISTRATION_INIT                                      \
  {                                                                                                \
    sizeof(turbo_flow_rulesforge_data_operation_registration_t), NULL, NULL, NULL,                  \
        {TURBO_FLOW_STAGE_MUTATES_PRIVATE, TURBO_FLOW_STAGE_EFFECT_NONE}                           \
  }

TURBO_FLOW_C_API int turbo_flow_rulesforge_register_data_operation(
    turbo_flow_t *flow, const turbo_flow_rulesforge_data_operation_registration_t *registration);

/**
 * Replace the message's current data-rule decision with one RulesForge evaluation result.
 *
 * `status == TURBO_OK` records MATCHED or NOT_MATCHED from `match_count`. A failure status
 * requires `match_count == 0` and records EVALUATION_ERROR. The runtime assigns the producing
 * stage index after a successful callback.
 */
TURBO_FLOW_C_API int turbo_flow_rulesforge_set_result(turbo_flow_msg_t *message, uint32_t match_count,
                                               int status);

/**
 * Per-message RulesForge provider for schema-bound DataBind objects or values.
 *
 * The knowledge base is borrowed and must outlive the provider registration and
 * flow. Each invocation creates a fresh stateful session, copies one message
 * projection, fires at most `max_rules`, and destroys the session. Supported
 * projection types are RulesForge.DataBindObject and TurboUtils.DataBindValue.
 * This provides
 * deterministic message isolation and permits worker-pool execution without
 * sharing mutable RulesForge session state.
 *
 * Use the generic data-operation callback instead when rules intentionally
 * retain facts across messages or use a continuous session.
 */
typedef struct turbo_flow_rulesforge_databind_provider_s {
  size_t size;
  ruleforge_knowledge_base_t knowledge_base;
  /** Non-zero message flag set when at least one rule fires and cleared otherwise. */
  uint32_t matched_flag;
  /** Positive per-message rule firing bound. */
  int max_rules;
  turbo_flow_stage_options_t options;
} turbo_flow_rulesforge_databind_provider_t;

#define TURBO_FLOW_RULEFORGE_DATABIND_PROVIDER_INIT                                                \
  {                                                                                                \
    sizeof(turbo_flow_rulesforge_databind_provider_t), NULL, 0u,                                  \
        TURBO_FLOW_RULEFORGE_DEFAULT_MAX_RULES, {                                                  \
      TURBO_FLOW_STAGE_MUTATES_PRIVATE, TURBO_FLOW_STAGE_EFFECT_NONE                              \
    }                                                                                              \
  }

/**
 * Transfer one owned RulesForge DataBind object to a message projection.
 *
 * The schema must describe DATA/TBE content, use projection type
 * TURBO_FLOW_RULEFORGE_DATABIND_PROJECTION_TYPE, and match the object's type
 * name. On success the message owns `object`; on failure ownership remains with
 * the caller. Clone and destroy operations use the matching RulesForge C API.
 */
TURBO_FLOW_C_API int turbo_flow_rulesforge_bind_databind_object(
    turbo_flow_msg_t *message, const turbo_flow_data_schema_t *schema,
    ruleforge_data_bind_object_t object);

/**
 * Register an isolated DataBind provider for one `rulesforge.apply` resource.
 *
 * RulesForge global initialization and knowledge-base loading remain application
 * responsibilities. `resource_name` and `provider` are borrowed and must outlive
 * the flow registry entry.
 */
TURBO_FLOW_C_API int turbo_flow_rulesforge_register_databind_provider(
    turbo_flow_t *flow, const char *resource_name,
    const turbo_flow_rulesforge_databind_provider_t *provider);

/**
 * Resolve the schema-bound JSON bytes consumed by a RulesForge JSON provider.
 *
 * The returned view is borrowed and only needs to remain valid for the callback.
 * A NULL callback makes the provider consume `message->payload` directly.
 */
typedef int (*turbo_flow_rulesforge_payload_view_fn)(const turbo_flow_msg_t *message,
                                                    vstr *payload_out, void *ctx);

/**
 * Per-message RulesForge provider for one schema-bound JSON object.
 *
 * The Knowledge Base must already import `fact_type`. Each invocation creates an
 * isolated session, parses exactly one JSON object through RulesForge/DataBind,
 * fires at most `max_rules`, records `message->data_decision`, and destroys all
 * temporary parsing/session state before returning.
 *
 * This configuration and all borrowed strings/contexts must remain valid for
 * the flow lifetime. The Knowledge Base must support concurrent session creation
 * when the stage is configured with workers.
 */
typedef struct turbo_flow_rulesforge_json_provider_s {
  size_t size;
  ruleforge_knowledge_base_t knowledge_base;
  const char *fact_type;
  uint32_t matched_flag;
  int max_rules;
  turbo_flow_rulesforge_payload_view_fn payload_view;
  void *payload_ctx;
  turbo_flow_stage_options_t options;
} turbo_flow_rulesforge_json_provider_t;

#define TURBO_FLOW_RULEFORGE_JSON_PROVIDER_INIT                                                    \
  {                                                                                                \
    sizeof(turbo_flow_rulesforge_json_provider_t), NULL, NULL, 0u, 1, NULL, NULL,                  \
        {TURBO_FLOW_STAGE_MUTATES_PRIVATE, TURBO_FLOW_STAGE_EFFECT_NONE}                           \
  }

/**
 * Register a schema-bound JSON provider for one `rulesforge.apply` resource.
 */
TURBO_FLOW_C_API int turbo_flow_rulesforge_register_json_provider(
    turbo_flow_t *flow, const char *resource_name,
    const turbo_flow_rulesforge_json_provider_t *provider);

#ifdef __cplusplus
}
#endif

#endif
