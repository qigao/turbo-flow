#include "flow_expr_internal.h"

#include "tinytest.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static int resolve_test_field(void *ctx, const char *path, size_t path_len,
                              flow_expr_value_type_t *type, uint32_t *field_id) {
  (void)ctx;
  if (path_len == 10 && memcmp(path, "parsed.age", path_len) == 0) {
    *type = FLOW_EXPR_TYPE_I64;
    *field_id = 100;
    return TURBO_OK;
  }
  if (path_len == 11 && memcmp(path, "parsed.name", path_len) == 0) {
    *type = FLOW_EXPR_TYPE_STRING;
    *field_id = 101;
    return TURBO_OK;
  }
  return TURBO_EINVAL;
}

static int resolve_invalid_type(void *ctx, const char *path, size_t path_len,
                                flow_expr_value_type_t *type, uint32_t *field_id) {
  (void)ctx;
  (void)path;
  (void)path_len;
  *type = (flow_expr_value_type_t)999;
  *field_id = 999;
  return TURBO_OK;
}

static int read_test_schema_field(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  (void)ctx;
  if (field_id != 10) return TURBO_ENOENT;
  out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
  out->as.boolean = 7;
  return TURBO_OK;
}

static int read_invalid_schema_field(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  (void)ctx;
  (void)field_id;
  out->type = (turbo_flow_expr_value_type_t)999;
  return TURBO_OK;
}

typedef struct expr_eval_state_s {
  int64_t age;
  int64_t minimum;
  const char *name;
  int name_is_null;
  int bad_reads;
} expr_eval_state_t;

enum {
  EXPR_FIELD_AGE = 1,
  EXPR_FIELD_NAME,
  EXPR_FIELD_BAD,
  EXPR_FIELD_WRONG,
  EXPR_FIELD_MINIMUM,
  EXPR_FIELD_NULL_BOOL
};

static const turbo_flow_expr_schema_field_t EXPR_EVAL_FIELDS[] = {
    {"parsed.age", TURBO_FLOW_EXPR_TYPE_I64, EXPR_FIELD_AGE},
    {"parsed.name", TURBO_FLOW_EXPR_TYPE_STRING, EXPR_FIELD_NAME},
    {"parsed.bad", TURBO_FLOW_EXPR_TYPE_I64, EXPR_FIELD_BAD},
    {"parsed.wrong", TURBO_FLOW_EXPR_TYPE_I64, EXPR_FIELD_WRONG},
    {"parsed.minimum", TURBO_FLOW_EXPR_TYPE_I64, EXPR_FIELD_MINIMUM},
    {"parsed.null_bool", TURBO_FLOW_EXPR_TYPE_BOOL, EXPR_FIELD_NULL_BOOL}};

static const turbo_flow_expr_schema_t EXPR_EVAL_SCHEMA = {
    EXPR_EVAL_FIELDS, sizeof(EXPR_EVAL_FIELDS) / sizeof(EXPR_EVAL_FIELDS[0])};

static int read_eval_schema_field(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  expr_eval_state_t *state = (expr_eval_state_t *)ctx;
  if (!state || !out) return TURBO_EINVAL;
  switch (field_id) {
  case EXPR_FIELD_AGE:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = state->age;
    return TURBO_OK;
  case EXPR_FIELD_NAME:
    if (state->name_is_null) {
      out->type = TURBO_FLOW_EXPR_TYPE_NULL;
    } else {
      out->type = TURBO_FLOW_EXPR_TYPE_STRING;
      out->as.string = tstr_v_from_cstr(state->name ? state->name : "");
    }
    return TURBO_OK;
  case EXPR_FIELD_BAD:
    state->bad_reads += 1;
    return TURBO_EIO;
  case EXPR_FIELD_WRONG:
    out->type = TURBO_FLOW_EXPR_TYPE_STRING;
    out->as.string = tstr_v_from_cstr("wrong");
    return TURBO_OK;
  case EXPR_FIELD_MINIMUM:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = state->minimum;
    return TURBO_OK;
  case EXPR_FIELD_NULL_BOOL:
    out->type = TURBO_FLOW_EXPR_TYPE_NULL;
    return TURBO_OK;
  default:
    return TURBO_ENOENT;
  }
}

static int compile_expr_backend(const char *text, turbo_flow_expr_backend_t backend,
                                turbo_flow_expr_t **out) {
  turbo_flow_expr_compile_options_t options = TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT;
  turbo_flow_error_t error;
  options.backend = backend;
  return turbo_flow_expr_compile_ex(text, strlen(text), &EXPR_EVAL_SCHEMA, &options, out, &error);
}

static void check_eval_value_pair(const char *text, turbo_flow_expr_eval_context_t *context,
                                  turbo_flow_expr_value_type_t expected_type,
                                  const turbo_flow_expr_value_t *expected) {
  turbo_flow_expr_t *interp = NULL;
  turbo_flow_expr_t *jit = NULL;
  turbo_flow_expr_value_t interp_value;
  turbo_flow_expr_value_t jit_value;
  memset(&jit_value, 0, sizeof(jit_value));
  check_int_eq(compile_expr_backend(text, TURBO_FLOW_EXPR_MIR_INTERP, &interp), TURBO_OK);
  check_not_null(interp);
  check_int_eq(turbo_flow_expr_backend(interp), TURBO_FLOW_EXPR_MIR_INTERP);
  check_int_eq(turbo_flow_expr_evaluate(interp, context, &interp_value), TURBO_OK);
  check_int_eq(interp_value.type, expected_type);
  if (turbo_flow_expr_jit_available()) {
    check_int_eq(compile_expr_backend(text, TURBO_FLOW_EXPR_MIR_JIT, &jit), TURBO_OK);
    check_not_null(jit);
    check_int_eq(turbo_flow_expr_backend(jit), TURBO_FLOW_EXPR_MIR_JIT);
    check_int_eq(turbo_flow_expr_evaluate(jit, context, &jit_value), TURBO_OK);
    check_int_eq(jit_value.type, interp_value.type);
  }
  switch (expected_type) {
  case TURBO_FLOW_EXPR_TYPE_BOOL:
    check_int_eq(interp_value.as.boolean, expected->as.boolean);
    if (jit) check_int_eq(jit_value.as.boolean, interp_value.as.boolean);
    break;
  case TURBO_FLOW_EXPR_TYPE_I64:
    check_int_eq(interp_value.as.i64, expected->as.i64);
    if (jit) check_int_eq(jit_value.as.i64, interp_value.as.i64);
    break;
  case TURBO_FLOW_EXPR_TYPE_F64:
    check_double_eq(interp_value.as.f64, expected->as.f64, 1e-12);
    if (jit) check_double_eq(jit_value.as.f64, interp_value.as.f64, 1e-12);
    break;
  case TURBO_FLOW_EXPR_TYPE_STRING:
    check_size_eq(interp_value.as.string.len, expected->as.string.len);
    check_mem_eq(interp_value.as.string.data, expected->as.string.data, expected->as.string.len);
    if (jit) {
      check_size_eq(jit_value.as.string.len, interp_value.as.string.len);
      check_mem_eq(jit_value.as.string.data, interp_value.as.string.data,
                   interp_value.as.string.len);
    }
    break;
  case TURBO_FLOW_EXPR_TYPE_NULL:
    break;
  default:
    check_true(0);
    break;
  }
  turbo_flow_expr_destroy(jit);
  turbo_flow_expr_destroy(interp);
}

static const flow_expr_node_t *expr_node(const flow_expr_ast_t *ast, uint32_t index,
                                         flow_expr_node_kind_t kind) {
  const flow_expr_node_t *node = flow_expr_ast_node_at(ast, index);
  check_not_null(node);
  check_int_eq(node->kind, kind);
  return node;
}

spec("flow_expr") {
  it("builds precedence and logical unary nodes") {
    static const char text[] = "1 + 2 * 3 == 7 and not false";
    flow_expr_ast_t ast;
    turbo_flow_error_t error;
    const flow_expr_node_t *root;
    const flow_expr_node_t *equality;
    const flow_expr_node_t *add;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    check_size_eq(flow_expr_ast_node_count(&ast), 10);
    root = expr_node(&ast, ast.root, FLOW_EXPR_AND);
    equality = expr_node(&ast, root->left, FLOW_EXPR_EQ);
    add = expr_node(&ast, equality->left, FLOW_EXPR_ADD);
    expr_node(&ast, add->right, FLOW_EXPR_MUL);
    expr_node(&ast, root->right, FLOW_EXPR_NOT);
    flow_expr_ast_destroy(&ast);
  }

  it("owns dotted fields and decoded string literals") {
    static const char text[] = "msg.status != 0 or parsed.user.name == \"A\\nB\"";
    flow_expr_ast_t ast;
    turbo_flow_error_t error;
    const flow_expr_node_t *root;
    const flow_expr_node_t *left;
    const flow_expr_node_t *right;
    const flow_expr_node_t *field;
    const flow_expr_node_t *string;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    root = expr_node(&ast, ast.root, FLOW_EXPR_OR);
    left = expr_node(&ast, root->left, FLOW_EXPR_NE);
    field = expr_node(&ast, left->left, FLOW_EXPR_FIELD);
    check_str_eq(field->text, "msg.status");
    right = expr_node(&ast, root->right, FLOW_EXPR_EQ);
    field = expr_node(&ast, right->left, FLOW_EXPR_FIELD);
    check_str_eq(field->text, "parsed.user.name");
    string = expr_node(&ast, right->right, FLOW_EXPR_STRING);
    check_size_eq(tstr_len(string->text), 3);
    check_mem_eq(string->text, "A\nB", 3);
    flow_expr_ast_destroy(&ast);
  }

  it("parses floats parentheses and arithmetic unary operators") {
    static const char text[] = "-(1.5e2 + +2) / 4";
    flow_expr_ast_t ast;
    turbo_flow_error_t error;
    const flow_expr_node_t *root;
    const flow_expr_node_t *negative;
    const flow_expr_node_t *add;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    root = expr_node(&ast, ast.root, FLOW_EXPR_DIV);
    negative = expr_node(&ast, root->left, FLOW_EXPR_NEG);
    add = expr_node(&ast, negative->left, FLOW_EXPR_ADD);
    expr_node(&ast, add->left, FLOW_EXPR_F64);
    expr_node(&ast, add->right, FLOW_EXPR_POS);
    flow_expr_ast_destroy(&ast);
  }

  it("accepts symbolic logic comparisons modulo and null") {
    static const char text[] = "a <= 1 || b >= 2 && c < 3 || d > 4 && e != null || 5 % 2 == 1";
    flow_expr_ast_t ast;
    turbo_flow_error_t error;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    expr_node(&ast, ast.root, FLOW_EXPR_OR);
    check_int_gt((int)flow_expr_ast_node_count(&ast), 20);
    flow_expr_ast_destroy(&ast);
  }

  it("rejects invalid syntax ranges escapes and multiline input") {
    flow_expr_ast_t ast;
    turbo_flow_error_t error;

    check_int_eq(flow_expr_parse("1 +", 3, &ast, &error), TURBO_EINVAL);
    check_int_eq(error.line, 1);
    check_int_gt(error.column, 0);

    check_int_eq(flow_expr_parse("9223372036854775808", 19, &ast, &error), TURBO_ERANGE);
    check_int_eq(error.line, 1);
    check_int_eq(error.column, 1);

    check_int_eq(flow_expr_parse("\"bad\\x\"", 7, &ast, &error), TURBO_EINVAL);
    check_str_contains(error.message, "escape");

    check_int_eq(flow_expr_parse("true\nand false", 14, &ast, &error), TURBO_EINVAL);
    check_int_eq(error.line, 1);
  }

  it("resolves built-in fields and computes predicate types") {
    static const char text[] = "msg.status == 0 and msg.payload != \"\"";
    flow_expr_ast_t ast;
    turbo_flow_error_t error;
    const flow_expr_node_t *root;
    const flow_expr_node_t *left;
    const flow_expr_node_t *right;
    const flow_expr_node_t *field;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_OK);
    root = expr_node(&ast, ast.root, FLOW_EXPR_AND);
    check_int_eq(root->value_type, FLOW_EXPR_TYPE_BOOL);
    left = expr_node(&ast, root->left, FLOW_EXPR_EQ);
    field = expr_node(&ast, left->left, FLOW_EXPR_FIELD);
    check_int_eq(field->value_type, FLOW_EXPR_TYPE_I64);
    check_int_eq(field->field_id, FLOW_EXPR_FIELD_MSG_STATUS);
    right = expr_node(&ast, root->right, FLOW_EXPR_NE);
    field = expr_node(&ast, right->left, FLOW_EXPR_FIELD);
    check_int_eq(field->value_type, FLOW_EXPR_TYPE_STRING);
    check_int_eq(field->field_id, FLOW_EXPR_FIELD_MSG_PAYLOAD);
    flow_expr_ast_destroy(&ast);
  }

  it("resolves schema fields and promotes mixed numeric arithmetic") {
    static const char text[] = "parsed.age + 2.5 > 3 and parsed.name == null";
    flow_expr_resolver_t resolver = {resolve_test_field, NULL};
    flow_expr_ast_t ast;
    turbo_flow_error_t error;
    const flow_expr_node_t *root;
    const flow_expr_node_t *ordering;
    const flow_expr_node_t *add;
    const flow_expr_node_t *field;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, &resolver, &error), TURBO_OK);
    root = expr_node(&ast, ast.root, FLOW_EXPR_AND);
    ordering = expr_node(&ast, root->left, FLOW_EXPR_GT);
    add = expr_node(&ast, ordering->left, FLOW_EXPR_ADD);
    check_int_eq(add->value_type, FLOW_EXPR_TYPE_F64);
    field = expr_node(&ast, add->left, FLOW_EXPR_FIELD);
    check_int_eq(field->field_id, 100);
    field = expr_node(&ast, expr_node(&ast, root->right, FLOW_EXPR_EQ)->left, FLOW_EXPR_FIELD);
    check_int_eq(field->field_id, 101);
    flow_expr_ast_destroy(&ast);
  }

  it("rejects unknown fields and incompatible operators") {
    flow_expr_resolver_t resolver = {resolve_test_field, NULL};
    flow_expr_ast_t ast;
    turbo_flow_error_t error;

    check_int_eq(flow_expr_parse("parsed.missing == 1", 19, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, &resolver, &error), TURBO_EINVAL);
    check_str_contains(error.message, "unknown external field");
    flow_expr_ast_destroy(&ast);

    check_int_eq(flow_expr_parse("msg.unknown == 1", 16, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_EINVAL);
    check_str_contains(error.message, "unknown message field");
    flow_expr_ast_destroy(&ast);

    check_int_eq(flow_expr_parse("value == 1", 10, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_EINVAL);
    check_str_contains(error.message, "schema resolver");
    flow_expr_ast_destroy(&ast);

    check_int_eq(flow_expr_parse("msg.status and true", 19, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_EINVAL);
    check_str_contains(error.message, "boolean operands");
    flow_expr_ast_destroy(&ast);

    check_int_eq(flow_expr_parse("msg.payload < 1", 15, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_EINVAL);
    check_str_contains(error.message, "ordering operands");
    flow_expr_ast_destroy(&ast);

    check_int_eq(flow_expr_parse("1 % 2.0", 7, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_EINVAL);
    check_str_contains(error.message, "integer operands");
    flow_expr_ast_destroy(&ast);
  }

  it("keeps a previously typed AST unchanged when rechecking fails") {
    static const char text[] = "parsed.age + 1";
    flow_expr_resolver_t valid = {resolve_test_field, NULL};
    flow_expr_resolver_t invalid = {resolve_invalid_type, NULL};
    flow_expr_ast_t ast;
    turbo_flow_error_t error;
    const flow_expr_node_t *root;
    const flow_expr_node_t *field;

    check_int_eq(flow_expr_parse(text, sizeof(text) - 1, &ast, &error), TURBO_OK);
    check_int_eq(flow_expr_type_check(&ast, &valid, &error), TURBO_OK);
    root = expr_node(&ast, ast.root, FLOW_EXPR_ADD);
    field = expr_node(&ast, root->left, FLOW_EXPR_FIELD);
    check_int_eq(root->value_type, FLOW_EXPR_TYPE_I64);
    check_int_eq(field->field_id, 100);
    check_int_eq(field->field_scope, FLOW_EXPR_FIELD_SCOPE_EXTERNAL);

    check_int_eq(flow_expr_type_check(&ast, &invalid, &error), TURBO_EINVAL);
    check_str_contains(error.message, "invalid field type");
    root = expr_node(&ast, ast.root, FLOW_EXPR_ADD);
    field = expr_node(&ast, root->left, FLOW_EXPR_FIELD);
    check_int_eq(root->value_type, FLOW_EXPR_TYPE_I64);
    check_int_eq(field->field_id, 100);
    check_int_eq(field->field_scope, FLOW_EXPR_FIELD_SCOPE_EXTERNAL);
    flow_expr_ast_destroy(&ast);
  }

  it("compiles an opaque typed expression with a borrowed field schema") {
    static const turbo_flow_expr_schema_field_t fields[] = {
        {"parsed.age", TURBO_FLOW_EXPR_TYPE_I64, 10},
        {"parsed.name", TURBO_FLOW_EXPR_TYPE_STRING, 11}};
    static const turbo_flow_expr_schema_t schema = {fields, sizeof(fields) / sizeof(fields[0])};
    turbo_flow_expr_t *expr = NULL;
    turbo_flow_error_t error;

    check_int_eq(turbo_flow_expr_compile("parsed.age + 2.5", 16, &schema, &expr, &error), TURBO_OK);
    check_not_null(expr);
    check_int_eq(turbo_flow_expr_result_type(expr), TURBO_FLOW_EXPR_TYPE_F64);
    turbo_flow_expr_destroy(expr);

    expr = NULL;
    check_int_eq(turbo_flow_expr_compile("msg.status == 0", 15, NULL, &expr, &error), TURBO_OK);
    check_not_null(expr);
    check_int_eq(turbo_flow_expr_result_type(expr), TURBO_FLOW_EXPR_TYPE_BOOL);
    turbo_flow_expr_destroy(expr);
  }

  it("rejects invalid public schemas and never returns partial expressions") {
    static const turbo_flow_expr_schema_field_t duplicate_paths[] = {
        {"parsed.age", TURBO_FLOW_EXPR_TYPE_I64, 1}, {"parsed.age", TURBO_FLOW_EXPR_TYPE_I64, 2}};
    static const turbo_flow_expr_schema_field_t duplicate_ids[] = {
        {"parsed.age", TURBO_FLOW_EXPR_TYPE_I64, 1},
        {"parsed.name", TURBO_FLOW_EXPR_TYPE_STRING, 1}};
    static const turbo_flow_expr_schema_field_t invalid_namespace[] = {
        {"msg.custom", TURBO_FLOW_EXPR_TYPE_I64, 1}};
    static const turbo_flow_expr_schema_field_t invalid_type[] = {
        {"parsed.value", TURBO_FLOW_EXPR_TYPE_NULL, 1}};
    turbo_flow_expr_schema_t schema;
    turbo_flow_expr_t *expr = (turbo_flow_expr_t *)(uintptr_t)1;
    turbo_flow_error_t error;

    schema.fields = duplicate_paths;
    schema.field_count = 2;
    check_int_eq(turbo_flow_expr_compile("true", 4, &schema, &expr, &error), TURBO_EALREADY);
    check_null(expr);
    check_str_contains(error.message, "field path");

    schema.fields = duplicate_ids;
    check_int_eq(turbo_flow_expr_compile("true", 4, &schema, &expr, &error), TURBO_EALREADY);
    check_null(expr);
    check_str_contains(error.message, "field id");

    schema.fields = invalid_namespace;
    schema.field_count = 1;
    check_int_eq(turbo_flow_expr_compile("true", 4, &schema, &expr, &error), TURBO_EINVAL);
    check_null(expr);
    check_str_contains(error.message, "non-msg namespace");

    schema.fields = invalid_type;
    check_int_eq(turbo_flow_expr_compile("true", 4, &schema, &expr, &error), TURBO_EINVAL);
    check_null(expr);
    check_str_contains(error.message, "field type");

    schema.fields = duplicate_ids;
    schema.field_count = 1;
    check_int_eq(turbo_flow_expr_compile("parsed.missing == 1", 19, &schema, &expr, &error),
                 TURBO_ENOENT);
    check_null(expr);
    check_str_contains(error.message, "unknown external field");
    check_int_eq(turbo_flow_expr_result_type(NULL), TURBO_FLOW_EXPR_TYPE_INVALID);
  }

  it("reads built-in and schema fields through the read-only evaluation ABI") {
    turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
    turbo_flow_expr_value_t value;
    turbo_flow_msg_t msg;

    turbo_flow_msg_init(&msg);
    msg.id = 42;
    msg.status = -7;
    msg.data_decision.evaluation_status = TURBO_FLOW_DATA_MATCHED;
    msg.data_decision.match_count = 2u;
    msg.payload = tstr_v_from_buf("body", 4);
    context.message = &msg;
    context.read_schema_field = read_test_schema_field;

    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_ID, &value),
                 TURBO_OK);
    check_int_eq(value.type, TURBO_FLOW_EXPR_TYPE_I64);
    check_int_eq(value.as.i64, 42);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_STATUS, &value),
                 TURBO_OK);
    check_int_eq(value.as.i64, -7);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_PAYLOAD, &value),
                 TURBO_OK);
    check_int_eq(value.type, TURBO_FLOW_EXPR_TYPE_STRING);
    check_size_eq(value.as.string.len, 4);
    check_mem_eq(value.as.string.data, "body", 4);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_RULE_STATUS, &value),
                 TURBO_OK);
    check_int_eq(value.as.i64, TURBO_FLOW_DATA_MATCHED);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCHED, &value),
                 TURBO_OK);
    check_true(value.as.boolean);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_RULE_MATCH_COUNT, &value),
                 TURBO_OK);
    check_int_eq(value.as.i64, 2);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_RULE_ERROR, &value),
                 TURBO_OK);
    check_int_eq(value.as.i64, TURBO_OK);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_SCHEMA, 10, &value),
                 TURBO_OK);
    check_int_eq(value.type, TURBO_FLOW_EXPR_TYPE_BOOL);
    check_int_eq(value.as.boolean, 1);
    context.message = NULL;
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_SCHEMA, 10, &value),
                 TURBO_OK);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_ID, &value),
                 TURBO_EINVAL);
    context.message = &msg;

    msg.id = UINT64_MAX;
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN,
                                            TURBO_FLOW_EXPR_FIELD_MSG_ID, &value),
                 TURBO_ERANGE);
    context.read_schema_field = NULL;
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_SCHEMA, 10, &value),
                 TURBO_ENOTSUP);
    context.read_schema_field = read_invalid_schema_field;
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_SCHEMA, 10, &value),
                 TURBO_EPROTO);
    check_int_eq(turbo_flow_expr_read_field(&context, TURBO_FLOW_EXPR_FIELD_BUILTIN, 999, &value),
                 TURBO_ENOENT);
  }

  it("keeps MIR interpreter and JIT scalar semantics identical") {
    turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
    turbo_flow_expr_value_t expected;
    turbo_flow_msg_t msg;
    expr_eval_state_t state;
    turbo_flow_msg_init(&msg);
    msg.flags = 6u;
    msg.data_decision.evaluation_status = TURBO_FLOW_DATA_MATCHED;
    msg.data_decision.match_count = 2u;
    memset(&state, 0, sizeof(state));
    state.age = 21;
    state.name = "Ada";
    context.message = &msg;
    context.read_schema_field = read_eval_schema_field;
    context.schema_ctx = &state;

    memset(&expected, 0, sizeof(expected));
    expected.type = TURBO_FLOW_EXPR_TYPE_I64;
    expected.as.i64 = 7;
    check_eval_value_pair("1 + 2 * 3", &context, expected.type, &expected);
    expected.as.i64 = 2;
    check_eval_value_pair("5 / 2", &context, expected.type, &expected);
    expected.as.i64 = 1;
    check_eval_value_pair("5 % 2", &context, expected.type, &expected);
    expected.as.i64 = 7;
    check_eval_value_pair("10 - 3", &context, expected.type, &expected);
    expected.as.i64 = 5;
    check_eval_value_pair("+5", &context, expected.type, &expected);
    expected.as.i64 = 0;
    state.minimum = INT64_MIN;
    check_eval_value_pair("parsed.minimum % -1", &context, expected.type, &expected);

    expected.type = TURBO_FLOW_EXPR_TYPE_F64;
    expected.as.f64 = -0.5;
    check_eval_value_pair("-2 + 1.5", &context, expected.type, &expected);

    expected.type = TURBO_FLOW_EXPR_TYPE_BOOL;
    expected.as.boolean = 1;
    check_eval_value_pair("has_flag(msg.flags, 2)", &context, expected.type, &expected);
    expected.as.boolean = 0;
    check_eval_value_pair("has_flag(msg.flags, 8)", &context, expected.type, &expected);
    expected.as.boolean = 1;
    check_eval_value_pair(
        "msg.rule_matched and msg.rule_status == 2 and msg.rule_match_count == 2 and "
        "msg.rule_error == 0",
        &context, expected.type, &expected);
    check_eval_value_pair("\"abc\" < \"abd\"", &context, expected.type, &expected);
    check_eval_value_pair("\"abc\" <= \"abc\"", &context, expected.type, &expected);
    check_eval_value_pair("\"b\" >= \"a\"", &context, expected.type, &expected);
    check_eval_value_pair("\"same\" == \"same\"", &context, expected.type, &expected);
    check_eval_value_pair("3 < 4 and 4 <= 4 and 5 > 4 and 5 >= 5", &context, expected.type,
                          &expected);
    check_eval_value_pair("not false and (false or true)", &context, expected.type, &expected);
    check_eval_value_pair("9007199254740993 != 9007199254740992", &context, expected.type,
                          &expected);
    msg.status = -7;
    check_eval_value_pair("msg.status == -7 and parsed.age > 20", &context, expected.type,
                          &expected);

    expected.type = TURBO_FLOW_EXPR_TYPE_STRING;
    expected.as.string = tstr_v_from_cstr("literal");
    check_eval_value_pair("\"literal\"", &context, expected.type, &expected);

    state.name_is_null = 1;
    expected.type = TURBO_FLOW_EXPR_TYPE_BOOL;
    expected.as.boolean = 1;
    check_eval_value_pair("parsed.name == null", &context, expected.type, &expected);
    expected.as.boolean = 0;
    check_eval_value_pair("parsed.name != null", &context, expected.type, &expected);
  }

  it("short circuits MIR branches before erroring field reads") {
    static const char *expressions[] = {"false and parsed.bad > 0", "true or parsed.bad > 0"};
    turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
    turbo_flow_expr_value_t expected;
    turbo_flow_msg_t msg;
    expr_eval_state_t state;
    turbo_flow_msg_init(&msg);
    memset(&state, 0, sizeof(state));
    context.message = &msg;
    context.read_schema_field = read_eval_schema_field;
    context.schema_ctx = &state;
    expected.type = TURBO_FLOW_EXPR_TYPE_BOOL;
    for (size_t i = 0; i < sizeof(expressions) / sizeof(expressions[0]); ++i) {
      expected.as.boolean = i == 1;
      check_eval_value_pair(expressions[i], &context, expected.type, &expected);
    }
    check_int_eq(state.bad_reads, 0);
  }

  it("returns identical MIR evaluation errors") {
    static const struct {
      const char *text;
      int expected;
    } cases[] = {{"1 / 0", TURBO_EINVAL},
                 {"1.0 / 0.0", TURBO_EINVAL},
                 {"1 % 0", TURBO_EINVAL},
                 {"9223372036854775807 + 1", TURBO_ERANGE},
                 {"parsed.minimum - 1", TURBO_ERANGE},
                 {"parsed.minimum * -1", TURBO_ERANGE},
                 {"parsed.minimum / -1", TURBO_ERANGE},
                 {"-parsed.minimum", TURBO_ERANGE},
                 {"parsed.wrong + 1", TURBO_EPROTO},
                 {"parsed.null_bool and true", TURBO_EPROTO},
                 {"true and parsed.bad > 0", TURBO_EIO}};
    turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
    turbo_flow_msg_t msg;
    expr_eval_state_t state;
    turbo_flow_msg_init(&msg);
    memset(&state, 0, sizeof(state));
    state.minimum = INT64_MIN;
    context.message = &msg;
    context.read_schema_field = read_eval_schema_field;
    context.schema_ctx = &state;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_expr_t *interp = NULL;
      turbo_flow_expr_t *jit = NULL;
      turbo_flow_expr_value_t value;
      check_int_eq(compile_expr_backend(cases[i].text, TURBO_FLOW_EXPR_MIR_INTERP, &interp),
                   TURBO_OK);
      check_int_eq(turbo_flow_expr_evaluate(interp, &context, &value), cases[i].expected);
      if (turbo_flow_expr_jit_available()) {
        check_int_eq(compile_expr_backend(cases[i].text, TURBO_FLOW_EXPR_MIR_JIT, &jit), TURBO_OK);
        check_int_eq(turbo_flow_expr_evaluate(jit, &context, &value), cases[i].expected);
      }
      turbo_flow_expr_destroy(jit);
      turbo_flow_expr_destroy(interp);
    }
  }

  it("resolves AUTO once and validates backend options") {
    turbo_flow_expr_compile_options_t options = TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT;
    turbo_flow_expr_t *expr = NULL;
    turbo_flow_error_t error;
    check_int_eq(turbo_flow_expr_compile_ex("true", 4, NULL, &options, &expr, &error), TURBO_OK);
    check_int_eq(turbo_flow_expr_backend(expr), turbo_flow_expr_jit_available()
                                                    ? TURBO_FLOW_EXPR_MIR_JIT
                                                    : TURBO_FLOW_EXPR_MIR_INTERP);
    turbo_flow_expr_destroy(expr);
    check_int_eq(turbo_flow_expr_backend(NULL), TURBO_FLOW_EXPR_BACKEND_INVALID);

    expr = NULL;
    options.size = 0;
    check_int_eq(turbo_flow_expr_compile_ex("true", 4, NULL, &options, &expr, &error),
                 TURBO_EINVAL);
    check_null(expr);
    options = (turbo_flow_expr_compile_options_t)TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT;
    options.backend = (turbo_flow_expr_backend_t)99;
    check_int_eq(turbo_flow_expr_compile_ex("true", 4, NULL, &options, &expr, &error),
                 TURBO_EINVAL);
    check_null(expr);
    if (!turbo_flow_expr_jit_available()) {
      options.backend = TURBO_FLOW_EXPR_MIR_JIT;
      check_int_eq(turbo_flow_expr_compile_ex("true", 4, NULL, &options, &expr, &error),
                   TURBO_ENOTSUP);
      check_null(expr);
    }
  }

  it("bounds MIR evaluation depth and tears down repeated programs") {
    turbo_flow_expr_t depth_expr;
    turbo_flow_expr_compile_options_t options = TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT;
    turbo_flow_expr_t *expr = NULL;
    turbo_flow_error_t error;
    flow_expr_node_t node;
    uint32_t root;
    memset(&depth_expr, 0, sizeof(depth_expr));
    check_int_eq(turbo_vec_init(&depth_expr.ast.nodes, sizeof(flow_expr_node_t)), TURBO_OK);
    memset(&node, 0, sizeof(node));
    node.kind = FLOW_EXPR_I64;
    node.value_type = FLOW_EXPR_TYPE_I64;
    node.left = FLOW_EXPR_INVALID_NODE;
    node.right = FLOW_EXPR_INVALID_NODE;
    node.i64 = 1;
    check_int_eq(turbo_vec_push(&depth_expr.ast.nodes, &node), TURBO_OK);
    root = 0;
    for (size_t i = 0; i < FLOW_EXPR_MAX_EVAL_DEPTH; ++i) {
      uint32_t left = (uint32_t)turbo_vec_size(&depth_expr.ast.nodes);
      check_int_eq(turbo_vec_push(&depth_expr.ast.nodes, &node), TURBO_OK);
      memset(&node, 0, sizeof(node));
      node.kind = FLOW_EXPR_ADD;
      node.value_type = FLOW_EXPR_TYPE_I64;
      node.left = left;
      node.right = root;
      root = (uint32_t)turbo_vec_size(&depth_expr.ast.nodes);
      check_int_eq(turbo_vec_push(&depth_expr.ast.nodes, &node), TURBO_OK);
      memset(&node, 0, sizeof(node));
      node.kind = FLOW_EXPR_I64;
      node.value_type = FLOW_EXPR_TYPE_I64;
      node.left = FLOW_EXPR_INVALID_NODE;
      node.right = FLOW_EXPR_INVALID_NODE;
      node.i64 = 1;
    }
    depth_expr.ast.root = root;
    options.backend = TURBO_FLOW_EXPR_MIR_INTERP;
    check_int_eq(flow_expr_mir_compile(&depth_expr, options.backend, &error), TURBO_ENOSPC);
    check_str_contains(error.message, "depth");
    flow_expr_ast_destroy(&depth_expr.ast);

    for (int i = 0; i < 20; ++i) {
      check_int_eq(turbo_flow_expr_compile_ex("1 + 2", 5, NULL, &options, &expr, &error), TURBO_OK);
      turbo_flow_expr_destroy(expr);
      expr = NULL;
    }
  }

  it("returns resource limit errors without an error output") {
    char *text = (char *)malloc(FLOW_EXPR_MAX_TEXT + 2);
    check_not_null(text);
    memset(text, 'a', FLOW_EXPR_MAX_TEXT + 1);
    text[FLOW_EXPR_MAX_TEXT + 1] = '\0';
    {
      flow_expr_ast_t ast;
      check_int_eq(flow_expr_parse(text, FLOW_EXPR_MAX_TEXT + 1, &ast, NULL), TURBO_ENOSPC);
    }
    free(text);
  }
}
