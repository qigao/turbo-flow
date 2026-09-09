#include "flow_control_internal.h"

#include "flow_expr_internal.h"
#include "flow_internal.h"
#include "turbo_flow_control.h"
#include "turbo_flow_control_grammar_gen.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *TurboFlowControlParseAlloc(void *(*malloc_proc)(size_t));
void TurboFlowControlParse(void *parser, int token_id, flow_control_token_t token,
                           flow_control_parse_ctx_t *ctx);
void TurboFlowControlParseFree(void *parser, void (*free_proc)(void *));

typedef struct flow_control_fact_s {
  turbo_flow_expr_schema_field_t schema;
  turbo_flow_expr_value_t value;
  char *path;
  uint32_t provider_id;
  int external;
  char *owned_string;
} flow_control_fact_t;

typedef struct flow_control_eval_s {
  flow_control_fact_t *facts;
  size_t count;
  const turbo_flow_control_facts_t *external;
} flow_control_eval_t;

static atomic_uint_fast64_t flow_control_command_sequence = 0u;

static void flow_control_error(turbo_flow_error_t *error, int code, uint32_t line, uint32_t column,
                               const char *message) {
  if (!error || error->code != SALTS_OK) return;
  error->code = code;
  error->line = line;
  error->column = column;
  snprintf(error->message, sizeof(error->message), "%s", message ? message : "control error");
}

void flow_control_syntax_error(flow_control_parse_ctx_t *ctx, flow_control_token_t token) {
  if (!ctx) return;
  flow_control_error(&ctx->error, SALTS_EINVAL, token.line, token.column,
                     token.value ? "control syntax error" : "unexpected end of control command");
}

static int flow_control_copy_token(char *dst, size_t capacity, flow_control_token_t token,
                                   flow_control_parse_ctx_t *ctx, const char *message) {
  if (!dst || capacity == 0u || !token.value || token.length >= capacity) {
    flow_control_error(&ctx->error, SALTS_ENOSPC, token.line, token.column, message);
    return SALTS_ENOSPC;
  }
  memcpy(dst, token.value, token.length);
  dst[token.length] = '\0';
  return SALTS_OK;
}

static int flow_control_u64(flow_control_token_t token, uint64_t *out,
                            flow_control_parse_ctx_t *ctx) {
  char text[32];
  char *end = NULL;
  unsigned long long value;
  if (!out || token.length == 0u || token.length >= sizeof(text)) {
    flow_control_error(&ctx->error, SALTS_ERANGE, token.line, token.column,
                       "control integer is out of range");
    return SALTS_ERANGE;
  }
  memcpy(text, token.value, token.length);
  text[token.length] = '\0';
  errno = 0;
  value = strtoull(text, &end, 10);
  if (errno == ERANGE || !end || *end != '\0') {
    flow_control_error(&ctx->error, SALTS_ERANGE, token.line, token.column,
                       "control integer is out of range");
    return SALTS_ERANGE;
  }
  *out = (uint64_t)value;
  return SALTS_OK;
}

static void flow_control_begin(flow_control_parse_ctx_t *ctx, turbo_flow_control_kind_t kind) {
  if (!ctx || ctx->error.code != SALTS_OK) return;
  if (ctx->has_command) {
    flow_control_error(&ctx->error, SALTS_EINVAL, 0u, 0u,
                       "exactly one control command is required");
    return;
  }
  ctx->command.kind = kind;
  ctx->command.size = sizeof(ctx->command);
  ctx->has_command = 1;
}

void flow_control_set_simple(flow_control_parse_ctx_t *ctx, turbo_flow_control_kind_t kind) {
  flow_control_begin(ctx, kind);
}

void flow_control_set_drain(flow_control_parse_ctx_t *ctx, flow_control_token_t timeout) {
  flow_control_begin(ctx, TURBO_FLOW_CONTROL_DRAIN);
  if (ctx && ctx->error.code == SALTS_OK) flow_control_u64(timeout, &ctx->command.timeout_ms, ctx);
}

void flow_control_set_resize(flow_control_parse_ctx_t *ctx, flow_control_token_t target,
                             turbo_flow_pool_kind_t kind, flow_control_token_t parallelism,
                             flow_control_token_t timeout) {
  uint64_t parsed = 0u;
  flow_control_begin(ctx, TURBO_FLOW_CONTROL_RESIZE_POOL);
  if (!ctx || ctx->error.code != SALTS_OK) return;
  if (flow_control_copy_token(ctx->command.target, sizeof(ctx->command.target), target, ctx,
                              "control target is too long") != SALTS_OK ||
      flow_control_u64(parallelism, &parsed, ctx) != SALTS_OK)
    return;
  if (parsed == 0u || parsed > UINT32_MAX) {
    flow_control_error(&ctx->error, SALTS_ERANGE, parallelism.line, parallelism.column,
                       "pool parallelism is out of range");
    return;
  }
  ctx->command.pool_kind = kind;
  ctx->command.parallelism = (uint32_t)parsed;
  if (timeout.value) flow_control_u64(timeout, &ctx->command.timeout_ms, ctx);
}

void flow_control_set_adapter(flow_control_parse_ctx_t *ctx, flow_control_token_t target,
                              turbo_flow_resource_command_kind_t resource_kind) {
  flow_control_begin(ctx, TURBO_FLOW_CONTROL_ADAPTER);
  if (!ctx || ctx->error.code != SALTS_OK) return;
  if (flow_control_copy_token(ctx->command.target, sizeof(ctx->command.target), target, ctx,
                              "adapter target is too long") != SALTS_OK)
    return;
  ctx->command.resource_kind = resource_kind;
}

void flow_control_set_replace(flow_control_parse_ctx_t *ctx, flow_control_token_t target,
                              flow_control_token_t host, flow_control_token_t port,
                              flow_control_token_t path) {
  uint64_t parsed = 0u;
  flow_control_set_adapter(ctx, target, TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT);
  if (!ctx || ctx->error.code != SALTS_OK) return;
  if (flow_control_copy_token(ctx->command.endpoint_host, sizeof(ctx->command.endpoint_host), host,
                              ctx, "endpoint host is too long") != SALTS_OK ||
      flow_control_u64(port, &parsed, ctx) != SALTS_OK)
    return;
  if (parsed == 0u || parsed > 65535u) {
    flow_control_error(&ctx->error, SALTS_ERANGE, port.line, port.column,
                       "endpoint port is out of range");
    return;
  }
  if (path.value &&
      flow_control_copy_token(ctx->command.endpoint_path, sizeof(ctx->command.endpoint_path), path,
                              ctx, "endpoint path is too long") != SALTS_OK)
    return;
  ctx->command.endpoint_port = (int)parsed;
}

void flow_control_set_condition(flow_control_parse_ctx_t *ctx, flow_control_token_t token) {
  flow_expr_ast_t ast;
  turbo_flow_error_t error;
  char expression[TURBO_FLOW_CONTROL_EXPR_MAX + 1u];
  int rc;
  if (!ctx || ctx->error.code != SALTS_OK) return;
  if (flow_control_copy_token(ctx->command.condition, sizeof(ctx->command.condition), token, ctx,
                              "control condition is too long") != SALTS_OK)
    return;
  memcpy(expression, ctx->command.condition, token.length + 1u);
  memset(&ast, 0, sizeof(ast));
  memset(&error, 0, sizeof(error));
  rc = flow_expr_parse(expression, token.length, &ast, &error);
  if (rc != SALTS_OK) {
    flow_control_error(&ctx->error, rc, token.line + error.line - 1u,
                       error.line == 1u ? token.column + error.column - 1u : error.column,
                       error.message);
    return;
  }
  flow_expr_ast_destroy(&ast);
}

int turbo_flow_control_parse(const char *text, size_t len, turbo_flow_control_command_t *out,
                             turbo_flow_error_t *error) {
  flow_control_parse_ctx_t ctx;
  flow_control_lexer_t lexer;
  flow_control_token_t token;
  void *parser;
  int token_id;
  if (!out || !text || len == 0u) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  if (error) memset(error, 0, sizeof(*error));
  memset(&ctx, 0, sizeof(ctx));
  parser = TurboFlowControlParseAlloc(malloc);
  if (!parser) return SALTS_ENOMEM;
  flow_control_lexer_init(&lexer, text, len);
  while ((token_id = flow_control_lexer_next(&lexer, &token)) > 0) {
    TurboFlowControlParse(parser, token_id, token, &ctx);
    if (ctx.error.code != SALTS_OK) break;
  }
  if (token_id < 0 && ctx.error.code == SALTS_OK) {
    flow_control_error(&ctx.error, SALTS_EINVAL, token.line, token.column, "invalid control token");
  }
  if (ctx.error.code == SALTS_OK) {
    memset(&token, 0, sizeof(token));
    token.line = lexer.line;
    token.column = lexer.column;
    TurboFlowControlParse(parser, 0, token, &ctx);
  }
  TurboFlowControlParseFree(parser, free);
  if (ctx.error.code != SALTS_OK || !ctx.has_command) {
    int rc = ctx.error.code ? ctx.error.code : SALTS_EINVAL;
    if (error) *error = ctx.error;
    return rc;
  }
  *out = ctx.command;
  return SALTS_OK;
}

static int flow_control_add_fact(flow_control_eval_t *eval, const char *path,
                                 turbo_flow_expr_value_t value) {
  flow_control_fact_t *fact;
  size_t path_len;
  if (!eval || !path || eval->count >= TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS) return SALTS_ENOSPC;
  fact = &eval->facts[eval->count];
  path_len = strlen(path);
  fact->path = (char *)malloc(path_len + 1u);
  if (!fact->path) return SALTS_ENOMEM;
  memcpy(fact->path, path, path_len + 1u);
  fact->schema.path = fact->path;
  fact->schema.type = value.type;
  fact->schema.field_id = (uint32_t)eval->count;
  fact->value = value;
  if (value.type == TURBO_FLOW_EXPR_TYPE_STRING && value.as.string.len > 0u) {
    fact->owned_string = (char *)malloc(value.as.string.len + 1u);
    if (!fact->owned_string) {
      free(fact->path);
      memset(fact, 0, sizeof(*fact));
      return SALTS_ENOMEM;
    }
    memcpy(fact->owned_string, value.as.string.data, value.as.string.len);
    fact->owned_string[value.as.string.len] = '\0';
    fact->value.as.string.data = fact->owned_string;
  }
  eval->count++;
  return SALTS_OK;
}

static turbo_flow_expr_value_t flow_control_i64(int64_t value) {
  turbo_flow_expr_value_t result;
  memset(&result, 0, sizeof(result));
  result.type = TURBO_FLOW_EXPR_TYPE_I64;
  result.as.i64 = value;
  return result;
}

static turbo_flow_expr_value_t flow_control_bool(int value) {
  turbo_flow_expr_value_t result;
  memset(&result, 0, sizeof(result));
  result.type = TURBO_FLOW_EXPR_TYPE_BOOL;
  result.as.boolean = value != 0;
  return result;
}

static turbo_flow_expr_value_t flow_control_string(const char *value) {
  turbo_flow_expr_value_t result;
  memset(&result, 0, sizeof(result));
  result.type = TURBO_FLOW_EXPR_TYPE_STRING;
  result.as.string.data = value ? value : "";
  result.as.string.len = value ? strlen(value) : 0u;
  return result;
}

static int flow_control_add_named(flow_control_eval_t *eval, const char *prefix, const char *name,
                                  const char *field, turbo_flow_expr_value_t value) {
  char path[TURBO_FLOW_CONTROL_EXPR_MAX + 1u];
  int written = snprintf(path, sizeof(path), "%s.%s.%s", prefix, name, field);
  if (written < 0 || (size_t)written >= sizeof(path)) return SALTS_ENOSPC;
  return flow_control_add_fact(eval, path, value);
}

static int flow_control_add_u64(flow_control_eval_t *eval, const char *path, uint64_t value) {
  if (value > INT64_MAX) return SALTS_ERANGE;
  return flow_control_add_fact(eval, path, flow_control_i64((int64_t)value));
}

static int flow_control_add_named_u64(flow_control_eval_t *eval, const char *prefix,
                                      const char *name, const char *field, uint64_t value) {
  if (value > INT64_MAX) return SALTS_ERANGE;
  return flow_control_add_named(eval, prefix, name, field, flow_control_i64((int64_t)value));
}

#define ADD_FIXED(path, value)                                                                     \
  do {                                                                                             \
    rc = flow_control_add_fact(eval, path, value);                                                 \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)
#define ADD_NAMED(prefix, name, field, value)                                                      \
  do {                                                                                             \
    rc = flow_control_add_named(eval, prefix, name, field, value);                                 \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)
#define ADD_U64(path, value)                                                                       \
  do {                                                                                             \
    rc = flow_control_add_u64(eval, path, value);                                                  \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)
#define ADD_NAMED_U64(prefix, name, field, value)                                                  \
  do {                                                                                             \
    rc = flow_control_add_named_u64(eval, prefix, name, field, value);                             \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)

static const char *flow_control_pool_kind(turbo_flow_pool_kind_t kind) {
  switch (kind) {
  case TURBO_FLOW_POOL_THREAD:
    return "thread";
  case TURBO_FLOW_POOL_CORO:
    return "coro";
  case TURBO_FLOW_POOL_DISRUPTOR:
    return "disruptor";
  default:
    return "unknown";
  }
}

static int flow_control_collect_core(turbo_flow_t *flow, flow_control_eval_t *eval) {
  turbo_flow_runtime_snapshot_t runtime;
  size_t i;
  int rc = turbo_flow_runtime_snapshot(flow, &runtime);
  if (rc != SALTS_OK) return rc;
  ADD_FIXED("runtime.state", flow_control_i64((int64_t)runtime.state));
  ADD_FIXED("runtime.accepting", flow_control_bool(runtime.accepting_publishes));
  ADD_U64("runtime.active_publishes", runtime.active_publishes);
  ADD_U64("runtime.stage_count", runtime.stage_count);
  ADD_U64("runtime.edge_count", runtime.edge_count);
  ADD_U64("runtime.adapter_count", runtime.adapter_count);
  ADD_U64("runtime.pool_count", runtime.pool_count);
  for (i = 0; i < turbo_flow_pool_count(flow); ++i) {
    turbo_flow_pool_snapshot_t pool;
    char name[TURBO_FLOW_CONTROL_NAME_MAX + 32u];
    rc = turbo_flow_pool_snapshot_at(flow, i, &pool);
    if (rc != SALTS_OK) return rc;
    if (snprintf(name, sizeof(name), "%s.%s", pool.stage_name, flow_control_pool_kind(pool.kind)) >=
        (int)sizeof(name))
      return SALTS_ENOSPC;
    ADD_NAMED("pool", name, "state", flow_control_i64((int64_t)pool.state));
    ADD_NAMED_U64("pool", name, "parallelism", pool.parallelism);
    ADD_NAMED_U64("pool", name, "queue_capacity", pool.queue_capacity);
    ADD_NAMED_U64("pool", name, "resource_capacity", pool.resource_capacity);
    ADD_NAMED_U64("pool", name, "submitted", pool.submitted);
    ADD_NAMED_U64("pool", name, "started", pool.started);
    ADD_NAMED_U64("pool", name, "completed", pool.completed);
    ADD_NAMED_U64("pool", name, "failed", pool.failed);
    ADD_NAMED_U64("pool", name, "canceled", pool.canceled);
    ADD_NAMED_U64("pool", name, "rejected", pool.rejected);
    ADD_NAMED_U64("pool", name, "queued", pool.queued);
    ADD_NAMED_U64("pool", name, "active", pool.active);
    ADD_NAMED("pool", name, "accepting", flow_control_bool(turbo_flow_pool_accepting(&pool)));
    ADD_NAMED("pool", name, "drained", flow_control_bool(turbo_flow_pool_drained(&pool)));
    ADD_NAMED("pool", name, "saturated", flow_control_bool(turbo_flow_pool_saturated(&pool)));
  }
  for (i = 0; i < turbo_flow_adapter_count(flow); ++i) {
    turbo_flow_connection_snapshot_t adapter;
    const turbo_flow_adapter_schema_t *schema = turbo_flow_adapter_schema_at(flow, i);
    if (!schema || !schema->binding_name) return SALTS_EINVAL;
    rc = turbo_flow_adapter_connection_snapshot_at(flow, i, &adapter);
    ADD_NAMED("adapter", schema->binding_name, "kind", flow_control_i64((int64_t)schema->kind));
    ADD_NAMED("adapter", schema->binding_name, "direction",
              flow_control_i64((int64_t)schema->direction));
    ADD_NAMED("adapter", schema->binding_name, "observable",
              flow_control_bool(rc != SALTS_ENOTSUP));
    if (rc == SALTS_ENOTSUP) continue;
    if (rc != SALTS_OK) return rc;
    ADD_NAMED("adapter", adapter.adapter_name, "state", flow_control_i64((int64_t)adapter.state));
    ADD_NAMED("adapter", adapter.adapter_name, "endpoint", flow_control_string(adapter.endpoint));
    ADD_NAMED_U64("adapter", adapter.adapter_name, "connections_current",
                  adapter.connections_current);
    ADD_NAMED_U64("adapter", adapter.adapter_name, "connection_limit", adapter.connection_limit);
    ADD_NAMED_U64("adapter", adapter.adapter_name, "in_flight_messages",
                  adapter.in_flight_messages);
    ADD_NAMED_U64("adapter", adapter.adapter_name, "in_flight_bytes", adapter.in_flight_bytes);
    ADD_NAMED("adapter", adapter.adapter_name, "last_status",
              flow_control_i64((int64_t)adapter.last_status));
  }
  return SALTS_OK;
}

#undef ADD_FIXED
#undef ADD_NAMED
#undef ADD_U64
#undef ADD_NAMED_U64

static int flow_control_collect_external(flow_control_eval_t *eval,
                                         const turbo_flow_control_facts_t *external) {
  size_t i;
  if (!external) return SALTS_OK;
  if (external->size < sizeof(*external) || !external->schema || !external->read_field) {
    return SALTS_EINVAL;
  }
  if (external->schema->field_count > TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS ||
      (external->schema->field_count > 0u && !external->schema->fields)) {
    return external->schema->field_count > TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS ? SALTS_ENOSPC
                                                                             : SALTS_EINVAL;
  }
  for (i = 0; i < external->schema->field_count; ++i) {
    const turbo_flow_expr_schema_field_t *source = &external->schema->fields[i];
    flow_control_fact_t *fact;
    int rc;
    turbo_flow_expr_value_t placeholder;
    if (!source->path || source->type < TURBO_FLOW_EXPR_TYPE_BOOL ||
        source->type > TURBO_FLOW_EXPR_TYPE_STRING) {
      return SALTS_EINVAL;
    }
    memset(&placeholder, 0, sizeof(placeholder));
    placeholder.type = source->type;
    rc = flow_control_add_fact(eval, source->path, placeholder);
    if (rc != SALTS_OK) return rc;
    fact = &eval->facts[eval->count - 1u];
    fact->external = 1;
    fact->provider_id = source->field_id;
  }
  eval->external = external;
  return SALTS_OK;
}

static int flow_control_read_fact(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  flow_control_eval_t *eval = (flow_control_eval_t *)ctx;
  flow_control_fact_t *fact;
  int rc;
  if (!eval || !out || field_id >= eval->count) return SALTS_EINVAL;
  fact = &eval->facts[field_id];
  if (!fact->external) {
    *out = fact->value;
    return SALTS_OK;
  }
  rc = eval->external->read_field(eval->external->ctx, fact->provider_id, out);
  if (rc != SALTS_OK) return rc;
  return out->type == fact->schema.type ? SALTS_OK : SALTS_EPROTO;
}

static void flow_control_eval_destroy(flow_control_eval_t *eval) {
  size_t i;
  if (!eval) return;
  for (i = 0; i < eval->count; ++i) {
    free(eval->facts[i].owned_string);
    free(eval->facts[i].path);
  }
  free(eval->facts);
  memset(eval, 0, sizeof(*eval));
}

static int flow_control_condition(turbo_flow_t *flow, const turbo_flow_control_command_t *command,
                                  const turbo_flow_control_facts_t *external,
                                  turbo_flow_error_t *error, int *matched) {
  flow_control_eval_t eval;
  turbo_flow_expr_schema_t schema;
  turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
  turbo_flow_expr_value_t result;
  turbo_flow_expr_t *expr = NULL;
  size_t i;
  int rc;
  if (!matched) return SALTS_EINVAL;
  *matched = 1;
  if (command->condition[0] == '\0') return SALTS_OK;
  memset(&eval, 0, sizeof(eval));
  eval.facts =
      (flow_control_fact_t *)calloc(TURBO_FLOW_EXPR_MAX_SCHEMA_FIELDS, sizeof(*eval.facts));
  if (!eval.facts) return SALTS_ENOMEM;
  rc = flow_control_collect_core(flow, &eval);
  if (rc == SALTS_OK) rc = flow_control_collect_external(&eval, external);
  if (rc != SALTS_OK) {
    flow_control_error(error, rc, 0u, 0u, "failed to collect control facts");
    flow_control_eval_destroy(&eval);
    return rc;
  }
  schema.fields = (const turbo_flow_expr_schema_field_t *)eval.facts;
  schema.field_count = eval.count;
  /* Schema fields are embedded first in each fact; materialize contiguous descriptors. */
  {
    turbo_flow_expr_schema_field_t *fields =
        (turbo_flow_expr_schema_field_t *)calloc(eval.count, sizeof(*fields));
    if (!fields) {
      flow_control_eval_destroy(&eval);
      return SALTS_ENOMEM;
    }
    for (i = 0; i < eval.count; ++i)
      fields[i] = eval.facts[i].schema;
    schema.fields = fields;
    rc = turbo_flow_expr_compile(command->condition, strlen(command->condition), &schema, &expr,
                                 error);
    free(fields);
  }
  if (rc == SALTS_OK && turbo_flow_expr_result_type(expr) != TURBO_FLOW_EXPR_TYPE_BOOL) {
    flow_control_error(error, SALTS_EINVAL, 0u, 0u, "control condition must return BOOL");
    rc = SALTS_EINVAL;
  }
  if (rc == SALTS_OK) {
    context.read_schema_field = flow_control_read_fact;
    context.schema_ctx = &eval;
    rc = turbo_flow_expr_evaluate(expr, &context, &result);
    if (rc == SALTS_OK) *matched = result.as.boolean != 0;
  }
  turbo_flow_expr_destroy(expr);
  flow_control_eval_destroy(&eval);
  return rc;
}

static int flow_control_next_command_key(char *key, size_t key_size) {
  uint_fast64_t current;
  int written;
  if (!key || key_size == 0u) return SALTS_EINVAL;
  current = atomic_load_explicit(&flow_control_command_sequence, memory_order_relaxed);
  for (;;) {
    if (current == UINT_FAST64_MAX) return SALTS_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&flow_control_command_sequence, &current, current + 1u,
                                              memory_order_relaxed,
                                              memory_order_relaxed)) {
      break;
    }
  }
  written = snprintf(key, key_size, "control:%llu", (unsigned long long)(current + 1u));
  return written < 0 || (size_t)written >= key_size ? SALTS_ENAMETOOLONG : SALTS_OK;
}

static int flow_control_resource_command_init(turbo_flow_t *flow,
                                              turbo_flow_resource_command_t *resource_command,
                                              turbo_flow_resource_command_kind_t kind,
                                              const char *target_uid, uint64_t generation) {
  int written;
  int rc;
  if (!resource_command || !target_uid || target_uid[0] == '\0' || generation == 0u) {
    return SALTS_EINVAL;
  }
  *resource_command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
  resource_command->kind = kind;
  resource_command->expected_generation = generation;
  written = snprintf(resource_command->target_uid, sizeof(resource_command->target_uid), "%s",
                     target_uid);
  if (written < 0 || (size_t)written >= sizeof(resource_command->target_uid)) {
    return SALTS_ENAMETOOLONG;
  }
  (void)flow;
  rc = flow_control_next_command_key(resource_command->idempotency_key,
                                     sizeof(resource_command->idempotency_key));
  return rc;
}

static int flow_control_execute_resource_command(turbo_flow_t *flow,
                                                 turbo_flow_resource_command_t *resource_command) {
  turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
  return turbo_flow_resource_command(flow, resource_command, &result);
}

static int flow_control_find_adapter_resource(turbo_flow_t *flow, const char *adapter_name,
                                              turbo_flow_resource_metadata_t *metadata) {
  int found = 0;
  if (!flow || !adapter_name || !metadata) return SALTS_EINVAL;
  for (size_t i = 0u; i < vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *resource =
        (const flow_resource_registration_t *)vec_at_const(&flow->resources, i);
    turbo_flow_resource_metadata_t current = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc;
    if (!resource || !resource->ops.command) continue;
    rc = resource->ops.metadata(resource->ctx, &current);
    if (rc != SALTS_OK) return rc;
    if (!flow_resource_metadata_valid(&current) ||
        strcmp(current.owner_name, resource->owner_name) != 0) {
      return SALTS_EPROTO;
    }
    if (current.kind != TURBO_FLOW_RESOURCE_CONNECTION ||
        strcmp(current.owner_name, adapter_name) != 0) {
      continue;
    }
    if (found) return SALTS_EPROTO;
    *metadata = current;
    found = 1;
  }
  return found ? SALTS_OK : SALTS_ENOENT;
}

static int flow_control_execute_action(turbo_flow_t *flow,
                                       const turbo_flow_control_command_t *command) {
  switch (command->kind) {
  case TURBO_FLOW_CONTROL_PAUSE:
    return turbo_flow_pause(flow);
  case TURBO_FLOW_CONTROL_RESUME:
    return turbo_flow_resume(flow);
  case TURBO_FLOW_CONTROL_DRAIN:
    return turbo_flow_drain(flow, command->timeout_ms);
  case TURBO_FLOW_CONTROL_RESIZE_POOL: {
    turbo_flow_resource_command_t resource_command;
    turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    int rc;
    int found = 0;
    for (size_t i = 0u; i < turbo_flow_pool_count(flow); ++i) {
      rc = turbo_flow_pool_resource_status_at(flow, i, &status);
      if (rc != SALTS_OK) return rc;
      if (status.snapshot.kind == command->pool_kind &&
          strcmp(status.owner_name, command->target) == 0) {
        found = 1;
        break;
      }
      status = (turbo_flow_pool_resource_status_t)TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    }
    if (!found) return SALTS_ENOENT;
    rc = flow_control_resource_command_init(flow, &resource_command,
                                            TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL, status.uid,
                                            status.generation);
    if (rc != SALTS_OK) return rc;
    resource_command.parallelism = command->parallelism;
    resource_command.drain_timeout_ms = command->timeout_ms;
    return flow_control_execute_resource_command(flow, &resource_command);
  }
  case TURBO_FLOW_CONTROL_ADAPTER: {
    turbo_flow_resource_command_t resource_command;
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc;
    rc = flow_control_find_adapter_resource(flow, command->target, &metadata);
    if (rc != SALTS_OK) return rc;
    rc = flow_control_resource_command_init(flow, &resource_command, command->resource_kind,
                                            metadata.uid, metadata.generation);
    if (rc != SALTS_OK) return rc;
    if (command->resource_kind == TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT) {
      int host_written =
          snprintf(resource_command.endpoint_host, sizeof(resource_command.endpoint_host), "%s",
                   command->endpoint_host);
      int path_written =
          snprintf(resource_command.endpoint_path, sizeof(resource_command.endpoint_path), "%s",
                   command->endpoint_path);
      if (host_written < 0 || (size_t)host_written >= sizeof(resource_command.endpoint_host) ||
          path_written < 0 || (size_t)path_written >= sizeof(resource_command.endpoint_path)) {
        return SALTS_ENAMETOOLONG;
      }
      resource_command.endpoint_port = command->endpoint_port;
    }
    return flow_control_execute_resource_command(flow, &resource_command);
  }
  default:
    return SALTS_EINVAL;
  }
}

static int flow_control_validate_command(const turbo_flow_control_command_t *command) {
  if (!command || command->size < sizeof(*command) ||
      !memchr(command->condition, '\0', sizeof(command->condition))) {
    return SALTS_EINVAL;
  }
  if (command->kind < TURBO_FLOW_CONTROL_PAUSE || command->kind > TURBO_FLOW_CONTROL_ADAPTER) {
    return SALTS_EINVAL;
  }
  if (command->kind == TURBO_FLOW_CONTROL_RESIZE_POOL &&
      (!memchr(command->target, '\0', sizeof(command->target)) || command->target[0] == '\0' ||
       command->parallelism == 0u || command->pool_kind < TURBO_FLOW_POOL_THREAD ||
       command->pool_kind > TURBO_FLOW_POOL_DISRUPTOR)) {
    return SALTS_EINVAL;
  }
  if (command->kind == TURBO_FLOW_CONTROL_ADAPTER) {
    if (!memchr(command->target, '\0', sizeof(command->target)) || command->target[0] == '\0' ||
        (command->resource_kind != TURBO_FLOW_RESOURCE_COMMAND_QUIESCE &&
         command->resource_kind != TURBO_FLOW_RESOURCE_COMMAND_RESUME &&
         command->resource_kind != TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT)) {
      return SALTS_EINVAL;
    }
    if (command->resource_kind == TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT &&
        (!memchr(command->endpoint_host, '\0', sizeof(command->endpoint_host)) ||
         !memchr(command->endpoint_path, '\0', sizeof(command->endpoint_path)) ||
         command->endpoint_host[0] == '\0' || command->endpoint_port <= 0 ||
         command->endpoint_port > 65535)) {
      return SALTS_EINVAL;
    }
  }
  return SALTS_OK;
}

int turbo_flow_control_execute_ex(turbo_flow_t *flow, const turbo_flow_control_command_t *command,
                                  const turbo_flow_control_facts_t *facts,
                                  turbo_flow_error_t *error) {
  int matched;
  int rc;
  if (error) memset(error, 0, sizeof(*error));
  if (!flow || !command) return SALTS_EINVAL;
  rc = flow_control_validate_command(command);
  if (rc != SALTS_OK) {
    flow_control_error(error, rc, 0u, 0u, "control command is invalid");
    return rc;
  }
  if (turbo_flow_state(flow) != TURBO_FLOW_STATE_STARTED) {
    flow_control_error(error, SALTS_EINVAL, 0u, 0u, "control commands require a started flow");
    return SALTS_EINVAL;
  }
  rc = flow_control_condition(flow, command, facts, error, &matched);
  if (rc != SALTS_OK || !matched) return rc;
  rc = flow_control_execute_action(flow, command);
  if (rc != SALTS_OK) flow_control_error(error, rc, 0u, 0u, "control command failed");
  return rc;
}

int turbo_flow_control_execute(turbo_flow_t *flow, const turbo_flow_control_command_t *command) {
  return turbo_flow_control_execute_ex(flow, command, NULL, NULL);
}

int turbo_flow_control_ex(turbo_flow_t *flow, const char *text, size_t len,
                          const turbo_flow_control_facts_t *facts, turbo_flow_error_t *error) {
  turbo_flow_control_command_t command;
  int rc = turbo_flow_control_parse(text, len, &command, error);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_control_execute_ex(flow, &command, facts, error);
}

int turbo_flow_control(turbo_flow_t *flow, const char *text, size_t len) {
  return turbo_flow_control_ex(flow, text, len, NULL, NULL);
}
