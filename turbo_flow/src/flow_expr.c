#include "flow_expr_internal.h"

#include "turbo_flow_expr_grammar_gen.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *TurboFlowExprParseAlloc(void *(*malloc_proc)(size_t));
void TurboFlowExprParse(void *parser, int token_id, flow_expr_token_t token,
                        flow_expr_parse_ctx_t *ctx);
void TurboFlowExprParseFree(void *parser, void (*free_proc)(void *));

static void flow_expr_set_error(flow_expr_parse_ctx_t *ctx, int code, uint32_t line,
                                uint32_t column, const char *message) {
  if (!ctx || ctx->failed) return;
  ctx->failed = 1;
  ctx->code = code;
  if (!ctx->error) return;
  ctx->error->code = code;
  ctx->error->line = line;
  ctx->error->column = column;
  snprintf(ctx->error->message, sizeof(ctx->error->message), "%s",
           message ? message : "expression error");
}

static uint32_t flow_expr_push_node(flow_expr_parse_ctx_t *ctx, flow_expr_node_t *node) {
  size_t index;
  if (!ctx || !ctx->ast || !node || ctx->failed) return FLOW_EXPR_INVALID_NODE;
  index = turbo_vec_size(&ctx->ast->nodes);
  if (index >= FLOW_EXPR_MAX_NODES) {
    tstr_freep(&node->text);
    flow_expr_set_error(ctx, TURBO_ENOSPC, node->line, node->column,
                        "expression node limit exceeded");
    return FLOW_EXPR_INVALID_NODE;
  }
  if (turbo_vec_push(&ctx->ast->nodes, node) != TURBO_OK) {
    tstr_freep(&node->text);
    flow_expr_set_error(ctx, TURBO_ENOMEM, node->line, node->column,
                        "out of memory building expression");
    return FLOW_EXPR_INVALID_NODE;
  }
  node->text = NULL;
  return (uint32_t)index;
}

static flow_expr_node_t flow_expr_node(flow_expr_node_kind_t kind, flow_expr_token_t token) {
  flow_expr_node_t node;
  memset(&node, 0, sizeof(node));
  node.kind = kind;
  node.left = FLOW_EXPR_INVALID_NODE;
  node.right = FLOW_EXPR_INVALID_NODE;
  node.line = token.line;
  node.column = token.column;
  return node;
}

static char *flow_expr_token_copy(flow_expr_token_t token) {
  char *copy = (char *)malloc(token.length + 1);
  if (!copy) return NULL;
  if (token.length > 0) memcpy(copy, token.value, token.length);
  copy[token.length] = '\0';
  return copy;
}

uint32_t flow_expr_push_null(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(FLOW_EXPR_NULL, token);
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_push_bool(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token, int value) {
  flow_expr_node_t node = flow_expr_node(FLOW_EXPR_BOOL, token);
  node.boolean = value != 0;
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_push_i64(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(FLOW_EXPR_I64, token);
  char *text = flow_expr_token_copy(token);
  char *end = NULL;
  long long value;
  if (!text) {
    flow_expr_set_error(ctx, TURBO_ENOMEM, token.line, token.column,
                        "out of memory parsing integer");
    return FLOW_EXPR_INVALID_NODE;
  }
  errno = 0;
  value = strtoll(text, &end, 10);
  if (errno == ERANGE || !end || *end != '\0') {
    free(text);
    flow_expr_set_error(ctx, TURBO_ERANGE, token.line, token.column,
                        "integer literal is out of range");
    return FLOW_EXPR_INVALID_NODE;
  }
  free(text);
  node.i64 = (int64_t)value;
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_push_f64(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(FLOW_EXPR_F64, token);
  char *text = flow_expr_token_copy(token);
  char *end = NULL;
  double value;
  if (!text) {
    flow_expr_set_error(ctx, TURBO_ENOMEM, token.line, token.column, "out of memory parsing float");
    return FLOW_EXPR_INVALID_NODE;
  }
  errno = 0;
  value = strtod(text, &end);
  if (errno == ERANGE || !end || *end != '\0') {
    free(text);
    flow_expr_set_error(ctx, TURBO_ERANGE, token.line, token.column,
                        "floating literal is out of range");
    return FLOW_EXPR_INVALID_NODE;
  }
  free(text);
  node.f64 = value;
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_push_string(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(FLOW_EXPR_STRING, token);
  tstr decoded = tstr_new_len("", 0);
  if (token.length > FLOW_EXPR_MAX_TEXT) {
    flow_expr_set_error(ctx, TURBO_ENOSPC, token.line, token.column,
                        "expression string limit exceeded");
    return FLOW_EXPR_INVALID_NODE;
  }
  if (!decoded) {
    flow_expr_set_error(ctx, TURBO_ENOMEM, token.line, token.column,
                        "out of memory parsing string");
    return FLOW_EXPR_INVALID_NODE;
  }
  for (size_t i = 0; i < token.length; ++i) {
    char value = token.value[i];
    if (value == '\\') {
      if (++i >= token.length) {
        tstr_freep(&decoded);
        flow_expr_set_error(ctx, TURBO_EINVAL, token.line, token.column,
                            "unterminated string escape");
        return FLOW_EXPR_INVALID_NODE;
      }
      switch (token.value[i]) {
      case '\\':
        value = '\\';
        break;
      case '"':
        value = '"';
        break;
      case 'n':
        value = '\n';
        break;
      case 'r':
        value = '\r';
        break;
      case 't':
        value = '\t';
        break;
      case 'b':
        value = '\b';
        break;
      case 'f':
        value = '\f';
        break;
      default:
        tstr_freep(&decoded);
        flow_expr_set_error(ctx, TURBO_EINVAL, token.line, token.column,
                            "unsupported string escape");
        return FLOW_EXPR_INVALID_NODE;
      }
    }
    {
      tstr next = tstr_cat_len(decoded, &value, 1);
      if (!next) {
        tstr_freep(&decoded);
        flow_expr_set_error(ctx, TURBO_ENOMEM, token.line, token.column,
                            "out of memory parsing string");
        return FLOW_EXPR_INVALID_NODE;
      }
      decoded = next;
    }
  }
  node.text = decoded;
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_push_field(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(FLOW_EXPR_FIELD, token);
  if (token.length > FLOW_EXPR_MAX_TEXT) {
    flow_expr_set_error(ctx, TURBO_ENOSPC, token.line, token.column,
                        "expression field limit exceeded");
    return FLOW_EXPR_INVALID_NODE;
  }
  node.text = tstr_new_len(token.value, token.length);
  if (!node.text) {
    flow_expr_set_error(ctx, TURBO_ENOMEM, token.line, token.column, "out of memory parsing field");
    return FLOW_EXPR_INVALID_NODE;
  }
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_append_field(flow_expr_parse_ctx_t *ctx, uint32_t field,
                                flow_expr_token_t member) {
  flow_expr_node_t *node;
  tstr next;
  if (!ctx || !ctx->ast || field == FLOW_EXPR_INVALID_NODE || ctx->failed) {
    return FLOW_EXPR_INVALID_NODE;
  }
  node = (flow_expr_node_t *)turbo_vec_at(&ctx->ast->nodes, field);
  if (!node || node->kind != FLOW_EXPR_FIELD || !node->text) {
    flow_expr_set_error(ctx, TURBO_EINVAL, member.line, member.column, "invalid field reference");
    return FLOW_EXPR_INVALID_NODE;
  }
  if (member.length >= FLOW_EXPR_MAX_TEXT ||
      tstr_len(node->text) > FLOW_EXPR_MAX_TEXT - member.length - 1) {
    flow_expr_set_error(ctx, TURBO_ENOSPC, member.line, member.column,
                        "expression field limit exceeded");
    return FLOW_EXPR_INVALID_NODE;
  }
  next = tstr_cat_len(node->text, ".", 1);
  if (!next) {
    flow_expr_set_error(ctx, TURBO_ENOMEM, member.line, member.column,
                        "out of memory parsing field");
    return FLOW_EXPR_INVALID_NODE;
  }
  node->text = next;
  next = tstr_cat_len(node->text, member.value, member.length);
  if (!next) {
    flow_expr_set_error(ctx, TURBO_ENOMEM, member.line, member.column,
                        "out of memory parsing field");
    return FLOW_EXPR_INVALID_NODE;
  }
  node->text = next;
  return field;
}

uint32_t flow_expr_push_unary(flow_expr_parse_ctx_t *ctx, flow_expr_node_kind_t kind,
                              uint32_t child, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(kind, token);
  if (child == FLOW_EXPR_INVALID_NODE) return FLOW_EXPR_INVALID_NODE;
  node.left = child;
  return flow_expr_push_node(ctx, &node);
}

uint32_t flow_expr_push_binary(flow_expr_parse_ctx_t *ctx, flow_expr_node_kind_t kind,
                               uint32_t left, uint32_t right, flow_expr_token_t token) {
  flow_expr_node_t node = flow_expr_node(kind, token);
  if (left == FLOW_EXPR_INVALID_NODE || right == FLOW_EXPR_INVALID_NODE) {
    return FLOW_EXPR_INVALID_NODE;
  }
  node.left = left;
  node.right = right;
  return flow_expr_push_node(ctx, &node);
}

void flow_expr_syntax_error(flow_expr_parse_ctx_t *ctx, flow_expr_token_t token) {
  flow_expr_set_error(ctx, TURBO_EINVAL, token.line, token.column,
                      token.value ? "expression syntax error" : "unexpected end of expression");
}

void flow_expr_ast_destroy(flow_expr_ast_t *ast) {
  if (!ast) return;
  for (size_t i = 0; i < turbo_vec_size(&ast->nodes); ++i) {
    flow_expr_node_t *node = (flow_expr_node_t *)turbo_vec_at(&ast->nodes, i);
    if (node) tstr_freep(&node->text);
  }
  turbo_vec_destroy(&ast->nodes);
  ast->root = FLOW_EXPR_INVALID_NODE;
}

size_t flow_expr_ast_node_count(const flow_expr_ast_t *ast) {
  return ast ? turbo_vec_size(&ast->nodes) : 0;
}

const flow_expr_node_t *flow_expr_ast_node_at(const flow_expr_ast_t *ast, uint32_t index) {
  if (!ast) return NULL;
  return (const flow_expr_node_t *)turbo_vec_at_const(&ast->nodes, index);
}

static int flow_expr_type_error(turbo_flow_error_t *error, const flow_expr_node_t *node, int code,
                                const char *message) {
  if (error) {
    memset(error, 0, sizeof(*error));
    error->code = code;
    if (node) {
      error->line = node->line;
      error->column = node->column;
    }
    snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return code;
}

static int flow_expr_is_numeric(flow_expr_value_type_t type) {
  return type == FLOW_EXPR_TYPE_I64 || type == FLOW_EXPR_TYPE_F64;
}

static flow_expr_value_type_t flow_expr_numeric_result(flow_expr_value_type_t left,
                                                       flow_expr_value_type_t right) {
  return left == FLOW_EXPR_TYPE_F64 || right == FLOW_EXPR_TYPE_F64 ? FLOW_EXPR_TYPE_F64
                                                                   : FLOW_EXPR_TYPE_I64;
}

static int flow_expr_resolve_builtin(flow_expr_node_t *node) {
  static const struct {
    const char *name;
    flow_expr_value_type_t type;
    uint32_t id;
  } fields[] = {{"msg.id", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_ID},
                {"msg.ts_ns", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_TS_NS},
                {"msg.type", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_TYPE},
                {"msg.flags", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_FLAGS},
                {"msg.status", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_STATUS},
                {"msg.payload", FLOW_EXPR_TYPE_STRING, FLOW_EXPR_FIELD_MSG_PAYLOAD},
                {"msg.rule_status", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_RULE_STATUS},
                {"msg.rule_matched", FLOW_EXPR_TYPE_BOOL, FLOW_EXPR_FIELD_MSG_RULE_MATCHED},
                {"msg.rule_match_count", FLOW_EXPR_TYPE_I64,
                 FLOW_EXPR_FIELD_MSG_RULE_MATCH_COUNT},
                {"msg.rule_error", FLOW_EXPR_TYPE_I64, FLOW_EXPR_FIELD_MSG_RULE_ERROR}};
  size_t i;
  for (i = 0; i < sizeof(fields) / sizeof(fields[0]); ++i) {
    if (strcmp(node->text, fields[i].name) == 0) {
      node->value_type = fields[i].type;
      node->field_id = fields[i].id;
      node->field_scope = FLOW_EXPR_FIELD_SCOPE_BUILTIN;
      return TURBO_OK;
    }
  }
  return TURBO_EINVAL;
}

static int flow_expr_resolve_field(flow_expr_node_t *node, const flow_expr_resolver_t *resolver,
                                   turbo_flow_error_t *error) {
  int rc;
  if (strncmp(node->text, "msg.", 4) == 0) {
    if (flow_expr_resolve_builtin(node) == TURBO_OK) return TURBO_OK;
    return flow_expr_type_error(error, node, TURBO_EINVAL, "unknown message field");
  }
  if (!resolver || !resolver->resolve) {
    return flow_expr_type_error(error, node, TURBO_EINVAL,
                                "external field requires a schema resolver");
  }
  rc = resolver->resolve(resolver->ctx, node->text, tstr_len(node->text), &node->value_type,
                         &node->field_id);
  if (rc != TURBO_OK) {
    node->value_type = FLOW_EXPR_TYPE_UNRESOLVED;
    node->field_id = FLOW_EXPR_FIELD_EXTERNAL;
    node->field_scope = FLOW_EXPR_FIELD_SCOPE_NONE;
    return flow_expr_type_error(error, node, rc, "unknown external field");
  }
  if (node->value_type <= FLOW_EXPR_TYPE_UNRESOLVED || node->value_type > FLOW_EXPR_TYPE_STRING) {
    return flow_expr_type_error(error, node, TURBO_EINVAL,
                                "schema resolver returned an invalid field type");
  }
  node->field_scope = FLOW_EXPR_FIELD_SCOPE_EXTERNAL;
  return TURBO_OK;
}

static int flow_expr_type_check_inplace(flow_expr_ast_t *ast, const flow_expr_resolver_t *resolver,
                                        turbo_flow_error_t *error) {
  size_t count;
  size_t i;
  if (error) memset(error, 0, sizeof(*error));
  if (!ast) return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression AST is null");
  count = turbo_vec_size(&ast->nodes);
  if (count == 0 || ast->root >= count) {
    return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression AST has an invalid root");
  }
  for (i = 0; i < count; ++i) {
    flow_expr_node_t *node = (flow_expr_node_t *)turbo_vec_at(&ast->nodes, i);
    if (!node) return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression AST is invalid");
    node->value_type = FLOW_EXPR_TYPE_UNRESOLVED;
    node->field_id = FLOW_EXPR_FIELD_EXTERNAL;
    node->field_scope = FLOW_EXPR_FIELD_SCOPE_NONE;
  }
  for (i = 0; i < count; ++i) {
    flow_expr_node_t *node = (flow_expr_node_t *)turbo_vec_at(&ast->nodes, i);
    flow_expr_node_t *left = NULL;
    flow_expr_node_t *right = NULL;
    if (!node) return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression AST is invalid");
    if (node->left != FLOW_EXPR_INVALID_NODE) {
      if (node->left >= i) {
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "expression AST child order is invalid");
      }
      left = (flow_expr_node_t *)turbo_vec_at(&ast->nodes, node->left);
    }
    if (node->right != FLOW_EXPR_INVALID_NODE) {
      if (node->right >= i) {
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "expression AST child order is invalid");
      }
      right = (flow_expr_node_t *)turbo_vec_at(&ast->nodes, node->right);
    }
    switch (node->kind) {
    case FLOW_EXPR_NULL:
      node->value_type = FLOW_EXPR_TYPE_NULL;
      break;
    case FLOW_EXPR_BOOL:
      node->value_type = FLOW_EXPR_TYPE_BOOL;
      break;
    case FLOW_EXPR_I64:
      node->value_type = FLOW_EXPR_TYPE_I64;
      break;
    case FLOW_EXPR_F64:
      node->value_type = FLOW_EXPR_TYPE_F64;
      break;
    case FLOW_EXPR_STRING:
      node->value_type = FLOW_EXPR_TYPE_STRING;
      break;
    case FLOW_EXPR_FIELD: {
      int rc = flow_expr_resolve_field(node, resolver, error);
      if (rc != TURBO_OK) return rc;
      break;
    }
    case FLOW_EXPR_NOT:
      if (!left || left->value_type != FLOW_EXPR_TYPE_BOOL)
        return flow_expr_type_error(error, node, TURBO_EINVAL, "not requires a boolean operand");
      node->value_type = FLOW_EXPR_TYPE_BOOL;
      break;
    case FLOW_EXPR_POS:
    case FLOW_EXPR_NEG:
      if (!left || !flow_expr_is_numeric(left->value_type))
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "numeric unary operator requires a numeric operand");
      node->value_type = left->value_type;
      break;
    case FLOW_EXPR_ADD:
    case FLOW_EXPR_SUB:
    case FLOW_EXPR_MUL:
    case FLOW_EXPR_DIV:
      if (!left || !right || !flow_expr_is_numeric(left->value_type) ||
          !flow_expr_is_numeric(right->value_type))
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "arithmetic operator requires numeric operands");
      node->value_type = flow_expr_numeric_result(left->value_type, right->value_type);
      break;
    case FLOW_EXPR_MOD:
      if (!left || !right || left->value_type != FLOW_EXPR_TYPE_I64 ||
          right->value_type != FLOW_EXPR_TYPE_I64)
        return flow_expr_type_error(error, node, TURBO_EINVAL, "modulo requires integer operands");
      node->value_type = FLOW_EXPR_TYPE_I64;
      break;
    case FLOW_EXPR_EQ:
    case FLOW_EXPR_NE:
      if (!left || !right ||
          !(left->value_type == right->value_type ||
            (flow_expr_is_numeric(left->value_type) && flow_expr_is_numeric(right->value_type)) ||
            left->value_type == FLOW_EXPR_TYPE_NULL || right->value_type == FLOW_EXPR_TYPE_NULL))
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "equality operands have incompatible types");
      node->value_type = FLOW_EXPR_TYPE_BOOL;
      break;
    case FLOW_EXPR_LT:
    case FLOW_EXPR_LE:
    case FLOW_EXPR_GT:
    case FLOW_EXPR_GE:
      if (!left || !right ||
          !((flow_expr_is_numeric(left->value_type) && flow_expr_is_numeric(right->value_type)) ||
            (left->value_type == FLOW_EXPR_TYPE_STRING &&
             right->value_type == FLOW_EXPR_TYPE_STRING)))
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "ordering operands have incompatible types");
      node->value_type = FLOW_EXPR_TYPE_BOOL;
      break;
    case FLOW_EXPR_HAS_FLAG:
      if (!left || !right || left->value_type != FLOW_EXPR_TYPE_I64 ||
          right->value_type != FLOW_EXPR_TYPE_I64 || right->kind != FLOW_EXPR_I64 ||
          right->i64 <= 0)
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "has_flag requires an integer value and a positive literal mask");
      node->value_type = FLOW_EXPR_TYPE_BOOL;
      break;
    case FLOW_EXPR_AND:
    case FLOW_EXPR_OR:
      if (!left || !right || left->value_type != FLOW_EXPR_TYPE_BOOL ||
          right->value_type != FLOW_EXPR_TYPE_BOOL)
        return flow_expr_type_error(error, node, TURBO_EINVAL,
                                    "logical operator requires boolean operands");
      node->value_type = FLOW_EXPR_TYPE_BOOL;
      break;
    default:
      return flow_expr_type_error(error, node, TURBO_EINVAL,
                                  "expression AST contains an unknown operation");
    }
  }
  return TURBO_OK;
}

int flow_expr_type_check(flow_expr_ast_t *ast, const flow_expr_resolver_t *resolver,
                         turbo_flow_error_t *error) {
  flow_expr_ast_t pending;
  size_t count;
  size_t i;
  int rc;

  if (!ast) return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression AST is null");
  memset(&pending, 0, sizeof(pending));
  pending.root = ast->root;
  count = turbo_vec_size(&ast->nodes);
  if (turbo_vec_init(&pending.nodes, sizeof(flow_expr_node_t)) != TURBO_OK) {
    return flow_expr_type_error(error, NULL, TURBO_ENOMEM,
                                "out of memory checking expression types");
  }
  for (i = 0; i < count; ++i) {
    const flow_expr_node_t *node = (const flow_expr_node_t *)turbo_vec_at_const(&ast->nodes, i);
    if (!node || turbo_vec_push(&pending.nodes, node) != TURBO_OK) {
      turbo_vec_destroy(&pending.nodes);
      return flow_expr_type_error(error, NULL, node ? TURBO_ENOMEM : TURBO_EINVAL,
                                  node ? "out of memory checking expression types"
                                       : "expression AST is invalid");
    }
  }
  rc = flow_expr_type_check_inplace(&pending, resolver, error);
  if (rc == TURBO_OK) {
    for (i = 0; i < count; ++i) {
      flow_expr_node_t *node = (flow_expr_node_t *)turbo_vec_at(&ast->nodes, i);
      const flow_expr_node_t *resolved =
          (const flow_expr_node_t *)turbo_vec_at_const(&pending.nodes, i);
      node->value_type = resolved->value_type;
      node->field_id = resolved->field_id;
      node->field_scope = resolved->field_scope;
    }
  }
  turbo_vec_destroy(&pending.nodes);
  return rc;
}

int flow_expr_parse(const char *text, size_t len, flow_expr_ast_t *ast, turbo_flow_error_t *error) {
  flow_expr_parse_ctx_t ctx;
  flow_expr_lexer_t lexer;
  flow_expr_token_t token;
  void *parser;
  int token_id;
  if (!ast || !text || len == 0) return TURBO_EINVAL;
  memset(ast, 0, sizeof(*ast));
  ast->root = FLOW_EXPR_INVALID_NODE;
  if (error) memset(error, 0, sizeof(*error));
  if (turbo_vec_init(&ast->nodes, sizeof(flow_expr_node_t)) != TURBO_OK) {
    return TURBO_ENOMEM;
  }
  memset(&ctx, 0, sizeof(ctx));
  ctx.ast = ast;
  ctx.error = error;
  parser = TurboFlowExprParseAlloc(malloc);
  if (!parser) {
    flow_expr_ast_destroy(ast);
    return TURBO_ENOMEM;
  }
  flow_expr_lexer_init(&lexer, text, len);
  while ((token_id = flow_expr_lexer_next(&lexer, &token)) != 0) {
    if (token_id < 0) {
      flow_expr_set_error(&ctx, TURBO_EINVAL, token.line, token.column,
                          "unexpected character in expression");
      break;
    }
    TurboFlowExprParse(parser, token_id, token, &ctx);
    if (ctx.failed) break;
  }
  if (!ctx.failed) {
    memset(&token, 0, sizeof(token));
    token.line = lexer.line;
    token.column = lexer.column;
    TurboFlowExprParse(parser, 0, token, &ctx);
  }
  TurboFlowExprParseFree(parser, free);
  if (ctx.failed || ast->root == FLOW_EXPR_INVALID_NODE) {
    int rc = ctx.code ? ctx.code : TURBO_EINVAL;
    flow_expr_ast_destroy(ast);
    return rc;
  }
  return TURBO_OK;
}

typedef struct flow_expr_public_schema_ctx_s {
  const turbo_flow_expr_schema_t *schema;
} flow_expr_public_schema_ctx_t;

static int flow_expr_public_schema_resolve(void *ctx, const char *path, size_t path_len,
                                           flow_expr_value_type_t *type, uint32_t *field_id) {
  const flow_expr_public_schema_ctx_t *schema_ctx = (const flow_expr_public_schema_ctx_t *)ctx;
  size_t i;
  if (!schema_ctx || !schema_ctx->schema || !path || !type || !field_id) {
    return TURBO_EINVAL;
  }
  for (i = 0; i < schema_ctx->schema->field_count; ++i) {
    const turbo_flow_expr_schema_field_t *field = &schema_ctx->schema->fields[i];
    size_t field_len = strlen(field->path);
    if (field_len == path_len && memcmp(field->path, path, path_len) == 0) {
      *type = (flow_expr_value_type_t)((int)field->type + 1);
      *field_id = field->field_id;
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

static int flow_expr_validate_public_schema(const turbo_flow_expr_schema_t *schema,
                                            turbo_flow_error_t *error) {
  size_t i;
  size_t j;
  if (!schema) return TURBO_OK;
  if (schema->field_count > TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS) {
    return flow_expr_type_error(error, NULL, TURBO_ENOSPC,
                                "expression schema field limit exceeded");
  }
  if (schema->field_count > 0 && !schema->fields) {
    return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression schema fields are null");
  }
  for (i = 0; i < schema->field_count; ++i) {
    const turbo_flow_expr_schema_field_t *field = &schema->fields[i];
    if (!field->path || !strchr(field->path, '.') || field->path[0] == '.' ||
        strncmp(field->path, "msg.", 4) == 0) {
      return flow_expr_type_error(error, NULL, TURBO_EINVAL,
                                  "schema field must use a non-msg namespace");
    }
    if (field->type < TURBO_FLOW_EXPR_TYPE_BOOL || field->type > TURBO_FLOW_EXPR_TYPE_STRING) {
      return flow_expr_type_error(error, NULL, TURBO_EINVAL,
                                  "expression schema field type is invalid");
    }
    for (j = 0; j < i; ++j) {
      if (strcmp(field->path, schema->fields[j].path) == 0) {
        return flow_expr_type_error(error, NULL, TURBO_EALREADY,
                                    "duplicate expression schema field path");
      }
      if (field->field_id == schema->fields[j].field_id) {
        return flow_expr_type_error(error, NULL, TURBO_EALREADY,
                                    "duplicate expression schema field id");
      }
    }
  }
  return TURBO_OK;
}

int turbo_flow_expr_compile(const char *text, size_t len, const turbo_flow_expr_schema_t *schema,
                            turbo_flow_expr_t **out, turbo_flow_error_t *error) {
  turbo_flow_expr_compile_options_t options = TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT;
  return turbo_flow_expr_compile_ex(text, len, schema, &options, out, error);
}

int turbo_flow_expr_compile_ex(const char *text, size_t len, const turbo_flow_expr_schema_t *schema,
                               const turbo_flow_expr_compile_options_t *options,
                               turbo_flow_expr_t **out, turbo_flow_error_t *error) {
  turbo_flow_expr_t *expr;
  flow_expr_public_schema_ctx_t schema_ctx;
  flow_expr_resolver_t resolver;
  int rc;
  if (!out)
    return flow_expr_type_error(error, NULL, TURBO_EINVAL, "compiled expression output is null");
  *out = NULL;
  if (!options || options->size < sizeof(*options) ||
      options->backend < TURBO_FLOW_EXPR_MIR_INTERP || options->backend > TURBO_FLOW_EXPR_AUTO) {
    return flow_expr_type_error(error, NULL, TURBO_EINVAL,
                                "expression compile options are invalid");
  }
  if (!text || len == 0) {
    return flow_expr_type_error(error, NULL, TURBO_EINVAL, "expression text is empty");
  }
  rc = flow_expr_validate_public_schema(schema, error);
  if (rc != TURBO_OK) return rc;
  expr = (turbo_flow_expr_t *)calloc(1, sizeof(*expr));
  if (!expr) {
    return flow_expr_type_error(error, NULL, TURBO_ENOMEM, "out of memory compiling expression");
  }
  rc = flow_expr_parse(text, len, &expr->ast, error);
  if (rc != TURBO_OK) {
    free(expr);
    return rc;
  }
  memset(&schema_ctx, 0, sizeof(schema_ctx));
  memset(&resolver, 0, sizeof(resolver));
  if (schema) {
    schema_ctx.schema = schema;
    resolver.resolve = flow_expr_public_schema_resolve;
    resolver.ctx = &schema_ctx;
  }
  rc = flow_expr_type_check(&expr->ast, schema ? &resolver : NULL, error);
  if (rc != TURBO_OK) {
    flow_expr_ast_destroy(&expr->ast);
    free(expr);
    return rc;
  }
  rc = flow_expr_mir_compile(expr, options->backend, error);
  if (rc != TURBO_OK) {
    flow_expr_ast_destroy(&expr->ast);
    free(expr);
    return rc;
  }
  *out = expr;
  return TURBO_OK;
}

void turbo_flow_expr_destroy(turbo_flow_expr_t *expr) {
  if (!expr) return;
  flow_expr_mir_destroy(expr->mir);
  flow_expr_ast_destroy(&expr->ast);
  free(expr);
}

turbo_flow_expr_backend_t turbo_flow_expr_backend(const turbo_flow_expr_t *expr) {
  return expr && expr->mir ? expr->backend : TURBO_FLOW_EXPR_BACKEND_INVALID;
}

int turbo_flow_expr_evaluate(const turbo_flow_expr_t *expr,
                             const turbo_flow_expr_eval_context_t *context,
                             turbo_flow_expr_value_t *out) {
  return flow_expr_mir_evaluate(expr, context, out);
}

turbo_flow_expr_value_type_t turbo_flow_expr_result_type(const turbo_flow_expr_t *expr) {
  const flow_expr_node_t *root;
  if (!expr || expr->ast.root == FLOW_EXPR_INVALID_NODE) return TURBO_FLOW_EXPR_TYPE_INVALID;
  root = flow_expr_ast_node_at(&expr->ast, expr->ast.root);
  if (!root || root->value_type <= FLOW_EXPR_TYPE_UNRESOLVED ||
      root->value_type > FLOW_EXPR_TYPE_STRING) {
    return TURBO_FLOW_EXPR_TYPE_INVALID;
  }
  return (turbo_flow_expr_value_type_t)((int)root->value_type - 1);
}

static int flow_expr_validate_read_value(turbo_flow_expr_value_t *value) {
  if (!value || value->type < TURBO_FLOW_EXPR_TYPE_NULL ||
      value->type > TURBO_FLOW_EXPR_TYPE_STRING) {
    return TURBO_EPROTO;
  }
  if (value->type == TURBO_FLOW_EXPR_TYPE_STRING && value->as.string.len > 0 &&
      !value->as.string.data) {
    return TURBO_EPROTO;
  }
  if (value->type == TURBO_FLOW_EXPR_TYPE_BOOL) value->as.boolean = value->as.boolean != 0;
  return TURBO_OK;
}

int turbo_flow_expr_read_field(const turbo_flow_expr_eval_context_t *context,
                               turbo_flow_expr_field_scope_t scope, uint32_t field_id,
                               turbo_flow_expr_value_t *out) {
  const turbo_flow_msg_t *msg;
  int rc;
  if (!context || context->size < sizeof(*context) || !out) {
    return TURBO_EINVAL;
  }
  memset(out, 0, sizeof(*out));
  if (scope == TURBO_FLOW_EXPR_FIELD_SCHEMA) {
    if (!context->read_schema_field) return TURBO_ENOTSUP;
    rc = context->read_schema_field(context->schema_ctx, field_id, out);
    if (rc != TURBO_OK) return rc;
    return flow_expr_validate_read_value(out);
  }
  if (scope != TURBO_FLOW_EXPR_FIELD_BUILTIN) return TURBO_EINVAL;
  if (!context->message) return TURBO_EINVAL;
  msg = context->message;
  switch (field_id) {
  case TURBO_FLOW_EXPR_FIELD_MSG_ID:
    if (msg->id > INT64_MAX) return TURBO_ERANGE;
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->id;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_TS_NS:
    if (msg->ts_ns > INT64_MAX) return TURBO_ERANGE;
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->ts_ns;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_TYPE:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->type;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_FLAGS:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->flags;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_STATUS:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->status;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_PAYLOAD:
    out->type = TURBO_FLOW_EXPR_TYPE_STRING;
    out->as.string = msg->payload;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_RULE_STATUS:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->data_decision.evaluation_status;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCHED:
    out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    out->as.boolean =
        msg->data_decision.evaluation_status == TURBO_FLOW_DATA_MATCHED;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCH_COUNT:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->data_decision.match_count;
    break;
  case TURBO_FLOW_EXPR_FIELD_MSG_RULE_ERROR:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = (int64_t)msg->data_decision.evaluation_error;
    break;
  default:
    return TURBO_ENOENT;
  }
  return TURBO_OK;
}
