#ifndef TURBO_FLOW_EXPR_H
#define TURBO_FLOW_EXPR_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_expr_s turbo_flow_expr_t;

#define TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS 4096u

typedef enum turbo_flow_expr_value_type_e {
  TURBO_FLOW_EXPR_TYPE_INVALID = -1,
  TURBO_FLOW_EXPR_TYPE_NULL = 0,
  TURBO_FLOW_EXPR_TYPE_BOOL,
  TURBO_FLOW_EXPR_TYPE_I64,
  TURBO_FLOW_EXPR_TYPE_F64,
  TURBO_FLOW_EXPR_TYPE_STRING
} turbo_flow_expr_value_type_t;

typedef enum turbo_flow_expr_field_scope_e {
  TURBO_FLOW_EXPR_FIELD_INVALID = 0,
  TURBO_FLOW_EXPR_FIELD_BUILTIN,
  TURBO_FLOW_EXPR_FIELD_SCHEMA
} turbo_flow_expr_field_scope_t;

typedef enum turbo_flow_expr_builtin_field_e {
  TURBO_FLOW_EXPR_FIELD_MSG_ID = 1,
  TURBO_FLOW_EXPR_FIELD_MSG_TS_NS,
  TURBO_FLOW_EXPR_FIELD_MSG_TYPE,
  TURBO_FLOW_EXPR_FIELD_MSG_FLAGS,
  TURBO_FLOW_EXPR_FIELD_MSG_STATUS,
  TURBO_FLOW_EXPR_FIELD_MSG_PAYLOAD,
  TURBO_FLOW_EXPR_FIELD_MSG_RULE_STATUS,
  TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCHED,
  TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCH_COUNT,
  TURBO_FLOW_EXPR_FIELD_MSG_RULE_ERROR
} turbo_flow_expr_builtin_field_t;

typedef struct turbo_flow_expr_value_s {
  turbo_flow_expr_value_type_t type;
  union {
    int boolean;
    int64_t i64;
    double f64;
    vstr string;
  } as;
} turbo_flow_expr_value_t;

/** Read one schema field into a borrowed value. The callback must not retain `out`. */
typedef int (*turbo_flow_expr_read_schema_field_fn)(void *ctx, uint32_t field_id,
                                                    turbo_flow_expr_value_t *out);

typedef struct turbo_flow_expr_eval_context_s {
  /** Set to sizeof(turbo_flow_expr_eval_context_t) for ABI versioning. */
  size_t size;
  /** Borrowed immutable message valid for the duration of one evaluation. */
  const turbo_flow_msg_t *message;
  /** Required only when the compiled expression loads schema fields. */
  turbo_flow_expr_read_schema_field_fn read_schema_field;
  void *schema_ctx;
} turbo_flow_expr_eval_context_t;

#define TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT {sizeof(turbo_flow_expr_eval_context_t), NULL, NULL, NULL}

typedef struct turbo_flow_expr_schema_field_s {
  /** Fully qualified field path, normally `parsed.<path>`. Borrowed during compile. */
  const char *path;
  turbo_flow_expr_value_type_t type;
  /** Schema-owned stable identifier used by the future evaluation context. */
  uint32_t field_id;
} turbo_flow_expr_schema_field_t;

typedef struct turbo_flow_expr_schema_s {
  const turbo_flow_expr_schema_field_t *fields;
  size_t field_count;
} turbo_flow_expr_schema_t;

/**
 * Read one field from a borrowed schema projection.
 *
 * `projection` is the value attached to the current working message and is
 * valid only for the callback duration. `ctx` remains caller-owned and must
 * outlive the flow registry entry. The callback must not retain `projection`
 * or `out`.
 */
typedef int (*turbo_flow_expr_projection_field_fn)(const void *projection, uint32_t field_id,
                                                   turbo_flow_expr_value_t *out, void *ctx);

/**
 * Register one immutable projection schema for graph route expressions.
 *
 * TurboFlow copies the schema identity and expression field paths. The reader
 * and its context remain provider-owned. Registrations sharing a field path
 * must use the same type and field id; conflicting path/id mappings fail fast.
 */
typedef struct turbo_flow_expr_projection_registration_s {
  size_t size;
  const turbo_flow_data_schema_t *projection_schema;
  const turbo_flow_expr_schema_t *expr_schema;
  turbo_flow_expr_projection_field_fn read_field;
  void *ctx;
} turbo_flow_expr_projection_registration_t;

#define TURBO_FLOW_EXPR_PROJECTION_REGISTRATION_INIT                                               \
  {sizeof(turbo_flow_expr_projection_registration_t), NULL, NULL, NULL, NULL}

typedef enum turbo_flow_expr_backend_e {
  TURBO_FLOW_EXPR_BACKEND_INVALID = -1,
  TURBO_FLOW_EXPR_MIR_INTERP = 0,
  TURBO_FLOW_EXPR_MIR_JIT,
  TURBO_FLOW_EXPR_AUTO
} turbo_flow_expr_backend_t;

typedef struct turbo_flow_expr_compile_options_s {
  /** Set to sizeof(turbo_flow_expr_compile_options_t) for ABI versioning. */
  size_t size;
  turbo_flow_expr_backend_t backend;
} turbo_flow_expr_compile_options_t;

#define TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT                                                       \
  {sizeof(turbo_flow_expr_compile_options_t), TURBO_FLOW_EXPR_AUTO}

/**
 * Add one projection schema to the flow-level `parsed.*` expression namespace.
 *
 * Registration is allowed before compile. It is preserved by reset with
 * `keep_registry != 0` and removed by reset without registry preservation or
 * flow destruction.
 */
TURBO_FLOW_C_API int turbo_flow_register_expr_projection(
    turbo_flow_t *flow, const turbo_flow_expr_projection_registration_t *registration);

/**
 * Parse, resolve fields, and type-check one backend-neutral expression.
 *
 * The returned object owns its AST and copied strings. `schema` is borrowed
 * only for this call. Unknown fields and incompatible operators fail without
 * returning a partially compiled object.
 *
 * Standard predicates include `has_flag(integer_value, positive_literal_mask)`.
 * It returns true only when every bit in the non-zero mask is present.
 *
 * Returns TURBO_OK on success. Parse/type/schema errors return their Turbo
 * error code and set `*out` to NULL. `error` may be NULL.
 */
TURBO_FLOW_C_API int turbo_flow_expr_compile(const char *text, size_t len,
                                      const turbo_flow_expr_schema_t *schema,
                                      turbo_flow_expr_t **out, turbo_flow_error_t *error);

/** Compile and finalize one expression for a selected MIR backend. */
TURBO_FLOW_C_API int turbo_flow_expr_compile_ex(const char *text, size_t len,
                                         const turbo_flow_expr_schema_t *schema,
                                         const turbo_flow_expr_compile_options_t *options,
                                         turbo_flow_expr_t **out, turbo_flow_error_t *error);

TURBO_FLOW_C_API void turbo_flow_expr_destroy(turbo_flow_expr_t *expr);

/** Return the type of the compiled root expression. */
TURBO_FLOW_C_API turbo_flow_expr_value_type_t turbo_flow_expr_result_type(const turbo_flow_expr_t *expr);

/** Return the resolved backend; AUTO is never returned by a compiled object. */
TURBO_FLOW_C_API turbo_flow_expr_backend_t turbo_flow_expr_backend(const turbo_flow_expr_t *expr);

/** Return non-zero when this build/platform supports the MIR JIT backend. */
TURBO_FLOW_C_API int turbo_flow_expr_jit_available(void);

/** Evaluate the finalized MIR function without compiling or heap allocation. */
TURBO_FLOW_C_API int turbo_flow_expr_evaluate(const turbo_flow_expr_t *expr,
                                       const turbo_flow_expr_eval_context_t *context,
                                       turbo_flow_expr_value_t *out);

/**
 * Read a typed field through the stable evaluation ABI.
 *
 * Built-in fields are read from `context->message`; schema fields are delegated
 * to `read_schema_field`. Returned string values are borrowed and valid only as
 * long as their message/schema backing storage remains valid.
 */
TURBO_FLOW_C_API int turbo_flow_expr_read_field(const turbo_flow_expr_eval_context_t *context,
                                         turbo_flow_expr_field_scope_t scope, uint32_t field_id,
                                         turbo_flow_expr_value_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_EXPR_H */
