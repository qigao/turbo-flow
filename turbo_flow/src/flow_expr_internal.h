#ifndef TURBO_FLOW_EXPR_INTERNAL_H
#define TURBO_FLOW_EXPR_INTERNAL_H

#include "turbo_flow_expr.h"
#include "turbo_vec.h"

#include <stdint.h>

#define FLOW_EXPR_INVALID_NODE UINT32_MAX
#define FLOW_EXPR_MAX_NODES 4096u
#define FLOW_EXPR_MAX_TEXT 4096u
#define FLOW_EXPR_MAX_EVAL_DEPTH 256u

typedef struct flow_expr_mir_program_s flow_expr_mir_program_t;

typedef enum flow_expr_value_type_e {
  FLOW_EXPR_TYPE_UNRESOLVED = 0,
  FLOW_EXPR_TYPE_NULL = TURBO_FLOW_EXPR_TYPE_NULL + 1,
  FLOW_EXPR_TYPE_BOOL = TURBO_FLOW_EXPR_TYPE_BOOL + 1,
  FLOW_EXPR_TYPE_I64 = TURBO_FLOW_EXPR_TYPE_I64 + 1,
  FLOW_EXPR_TYPE_F64 = TURBO_FLOW_EXPR_TYPE_F64 + 1,
  FLOW_EXPR_TYPE_STRING = TURBO_FLOW_EXPR_TYPE_STRING + 1
} flow_expr_value_type_t;

typedef enum flow_expr_field_scope_e {
  FLOW_EXPR_FIELD_SCOPE_NONE = TURBO_FLOW_EXPR_FIELD_INVALID,
  FLOW_EXPR_FIELD_SCOPE_BUILTIN = TURBO_FLOW_EXPR_FIELD_BUILTIN,
  FLOW_EXPR_FIELD_SCOPE_EXTERNAL = TURBO_FLOW_EXPR_FIELD_SCHEMA
} flow_expr_field_scope_t;

typedef enum flow_expr_builtin_field_e {
  FLOW_EXPR_FIELD_EXTERNAL = 0,
  FLOW_EXPR_FIELD_MSG_ID = TURBO_FLOW_EXPR_FIELD_MSG_ID,
  FLOW_EXPR_FIELD_MSG_TS_NS = TURBO_FLOW_EXPR_FIELD_MSG_TS_NS,
  FLOW_EXPR_FIELD_MSG_TYPE = TURBO_FLOW_EXPR_FIELD_MSG_TYPE,
  FLOW_EXPR_FIELD_MSG_FLAGS = TURBO_FLOW_EXPR_FIELD_MSG_FLAGS,
  FLOW_EXPR_FIELD_MSG_STATUS = TURBO_FLOW_EXPR_FIELD_MSG_STATUS,
  FLOW_EXPR_FIELD_MSG_PAYLOAD = TURBO_FLOW_EXPR_FIELD_MSG_PAYLOAD,
  FLOW_EXPR_FIELD_MSG_RULE_STATUS = TURBO_FLOW_EXPR_FIELD_MSG_RULE_STATUS,
  FLOW_EXPR_FIELD_MSG_RULE_MATCHED = TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCHED,
  FLOW_EXPR_FIELD_MSG_RULE_MATCH_COUNT = TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCH_COUNT,
  FLOW_EXPR_FIELD_MSG_RULE_ERROR = TURBO_FLOW_EXPR_FIELD_MSG_RULE_ERROR
} flow_expr_builtin_field_t;

typedef enum flow_expr_node_kind_e {
  FLOW_EXPR_NULL = 0,
  FLOW_EXPR_BOOL,
  FLOW_EXPR_I64,
  FLOW_EXPR_F64,
  FLOW_EXPR_STRING,
  FLOW_EXPR_FIELD,
  FLOW_EXPR_NOT,
  FLOW_EXPR_POS,
  FLOW_EXPR_NEG,
  FLOW_EXPR_ADD,
  FLOW_EXPR_SUB,
  FLOW_EXPR_MUL,
  FLOW_EXPR_DIV,
  FLOW_EXPR_MOD,
  FLOW_EXPR_EQ,
  FLOW_EXPR_NE,
  FLOW_EXPR_LT,
  FLOW_EXPR_LE,
  FLOW_EXPR_GT,
  FLOW_EXPR_GE,
  FLOW_EXPR_HAS_FLAG,
  FLOW_EXPR_AND,
  FLOW_EXPR_OR
} flow_expr_node_kind_t;

typedef struct flow_expr_node_s {
  flow_expr_node_kind_t kind;
  flow_expr_value_type_t value_type;
  uint32_t left;
  uint32_t right;
  uint32_t field_id;
  flow_expr_field_scope_t field_scope;
  uint32_t line;
  uint32_t column;
  int64_t i64;
  double f64;
  int boolean;
  tstr_t text;
} flow_expr_node_t;

typedef int (*flow_expr_resolve_field_fn)(void *ctx, const char *path, size_t path_len,
                                          flow_expr_value_type_t *type, uint32_t *field_id);

typedef struct flow_expr_resolver_s {
  flow_expr_resolve_field_fn resolve;
  void *ctx;
} flow_expr_resolver_t;

typedef struct flow_expr_ast_s {
  turbo_vec_t nodes;
  uint32_t root;
} flow_expr_ast_t;

struct turbo_flow_expr_s {
  flow_expr_ast_t ast;
  flow_expr_mir_program_t *mir;
  turbo_flow_expr_backend_t backend;
};

typedef struct flow_expr_token_s {
  const char *value;
  size_t length;
  uint32_t line;
  uint32_t column;
} flow_expr_token_t;

typedef struct flow_expr_lexer_s {
  const char *cursor;
  const char *limit;
  uint32_t line;
  uint32_t column;
} flow_expr_lexer_t;

typedef struct flow_expr_parse_ctx_s {
  flow_expr_ast_t *ast;
  turbo_flow_error_t *error;
  int failed;
  int code;
} flow_expr_parse_ctx_t;

CXX_C_API int flow_expr_parse(const char *text, size_t len, flow_expr_ast_t *ast,
                              turbo_flow_error_t *error);
CXX_C_API void flow_expr_ast_destroy(flow_expr_ast_t *ast);
CXX_C_API size_t flow_expr_ast_node_count(const flow_expr_ast_t *ast);
CXX_C_API const flow_expr_node_t *flow_expr_ast_node_at(const flow_expr_ast_t *ast, uint32_t index);
CXX_C_API int flow_expr_type_check(flow_expr_ast_t *ast, const flow_expr_resolver_t *resolver,
                                   turbo_flow_error_t *error);
CXX_C_API int flow_expr_mir_compile(turbo_flow_expr_t *expr, turbo_flow_expr_backend_t requested,
                                    turbo_flow_error_t *error);
CXX_C_API void flow_expr_mir_destroy(flow_expr_mir_program_t *program);
CXX_C_API int flow_expr_mir_evaluate(const turbo_flow_expr_t *expr,
                                     const turbo_flow_expr_eval_context_t *context,
                                     turbo_flow_expr_value_t *out);

void flow_expr_lexer_init(flow_expr_lexer_t *lexer, const char *text, size_t len);
int flow_expr_lexer_next(flow_expr_lexer_t *lexer, flow_expr_token_t *token);

uint32_t flow_expr_push_null(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token);
uint32_t flow_expr_push_bool(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token, int value);
uint32_t flow_expr_push_i64(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token);
uint32_t flow_expr_push_f64(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token);
uint32_t flow_expr_push_string(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token);
uint32_t flow_expr_push_field(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token);
uint32_t flow_expr_append_field(flow_expr_parse_ctx_t *ctx, uint32_t field,
                                flow_expr_token_t member);
uint32_t flow_expr_push_unary(flow_expr_parse_ctx_t *ctx, flow_expr_node_kind_t kind,
                              uint32_t child, flow_expr_token_t token);
uint32_t flow_expr_push_binary(flow_expr_parse_ctx_t *ctx, flow_expr_node_kind_t kind,
                               uint32_t left, uint32_t right, flow_expr_token_t token);
void flow_expr_syntax_error(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token);

#endif /* TURBO_FLOW_EXPR_INTERNAL_H */
