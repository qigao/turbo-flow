#include "flow_parser_internal.h"

#include "turbo_flow_grammar_gen.h"

#include <stdlib.h>
#include <string.h>

void *TurboFlowParseAlloc(void *(*mallocProc)(size_t));
void TurboFlowParseFree(void *parser, void (*freeProc)(void *));
void TurboFlowParse(void *parser, int token_id, flow_token_t token, flow_parse_ctx_t *ctx);

typedef struct flow_stage_template_decl_s {
  tstr name;
  tstr first_input;
  tstr first_output;
  uint32_t line;
  uint32_t column;
  uint32_t input_count;
  uint32_t output_count;
} flow_stage_template_decl_t;

static vstr token_view(flow_token_t token) { return vstr_from_buf(token.value, token.length); }

static int parse_fail(flow_parse_ctx_t *ctx, int code, uint32_t line, uint32_t column,
                      const char *message) {
  if (!ctx || ctx->error) return code;
  ctx->error = 1;
  return flow_set_error(ctx->flow, code, line, column, message);
}

static int token_eq_cstr(flow_token_t token, const char *text) {
  size_t len;

  if (!token.value || !text) return 0;
  len = strlen(text);
  return token.length == len && strncmp(token.value, text, len) == 0;
}

int flow_parse_ctx_init(flow_parse_ctx_t *ctx, turbo_flow_t *flow) {
  if (!ctx || !flow) return SALTS_EINVAL;
  memset(ctx, 0, sizeof(*ctx));
  ctx->flow = flow;
  ctx->current_stage_template_index = SIZE_MAX;
  if (turbo_flow_stl_error(vec_init_bytes(&ctx->node_refs, sizeof(flow_node_ref_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  if (turbo_flow_stl_error(vec_init_bytes(&ctx->stage_templates, sizeof(flow_stage_template_decl_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    vec_destroy(&ctx->node_refs);
    return flow_set_error(flow, SALTS_ENOMEM, 0, 0, "out of memory");
  }
  return SALTS_OK;
}

void flow_parse_ctx_destroy(flow_parse_ctx_t *ctx) {
  size_t i;

  if (!ctx) return;
  for (i = 0; i < vec_size(&ctx->stage_templates); ++i) {
    flow_stage_template_decl_t *stage_template =
        (flow_stage_template_decl_t *)vec_at(&ctx->stage_templates, i);
    if (!stage_template) continue;
    tstr_freep(&stage_template->name);
    tstr_freep(&stage_template->first_input);
    tstr_freep(&stage_template->first_output);
  }
  tstr_freep(&ctx->root_stage_name);
  vec_destroy(&ctx->stage_templates);
  vec_destroy(&ctx->node_refs);
}

flow_stage_spec_t flow_stage_spec_default(void) {
  flow_stage_spec_t spec;
  memset(&spec, 0, sizeof(spec));
  spec.data_strategy = TURBO_FLOW_DATA_BROADCAST;
  spec.data_pool_capacity = FLOW_WORKER_POOL_DEFAULT_CAPACITY;
  spec.exec.kind = TURBO_FLOW_EXEC_INLINE;
  spec.retry.max_attempts = 1u;
  return spec;
}

turbo_flow_exec_config_t flow_exec_config_default(void) {
  turbo_flow_exec_config_t exec;
  memset(&exec, 0, sizeof(exec));
  exec.kind = TURBO_FLOW_EXEC_INLINE;
  return exec;
}

flow_exec_options_t flow_exec_options_default(void) {
  flow_exec_options_t options;
  memset(&options, 0, sizeof(options));
  options.config = flow_exec_config_default();
  return options;
}

flow_exec_spec_t flow_exec_spec_default(void) {
  flow_exec_spec_t spec;
  memset(&spec, 0, sizeof(spec));
  spec.exec = flow_exec_config_default();
  return spec;
}

flow_exec_spec_t flow_exec_spec_make(turbo_flow_exec_kind_t kind, flow_exec_options_t options) {
  flow_exec_spec_t spec;
  memset(&spec, 0, sizeof(spec));
  spec.exec = options.config;
  spec.exec.kind = kind;
  spec.valid = 1;
  return spec;
}

int flow_parse_u32(flow_parse_ctx_t *ctx, flow_token_t token, uint32_t *out) {
  uint64_t value = 0;
  size_t i;

  if (!out || !token.value || token.length == 0) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "number required");
  }

  for (i = 0; i < token.length; ++i) {
    value = value * 10u + (uint32_t)(token.value[i] - '0');
    if (value > UINT32_MAX) {
      return parse_fail(ctx, SALTS_ERANGE, token.line, token.column, "number is out of range");
    }
  }

  *out = (uint32_t)value;
  return SALTS_OK;
}

int flow_parse_set_worker(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token) {
  uint32_t count = 0;
  int rc;
  if (!spec) return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "stage required");
  if (spec->has_worker) {
    return parse_fail(ctx, SALTS_EALREADY, token.line, token.column, "duplicate worker option");
  }
  rc = flow_parse_u32(ctx, token, &count);
  if (rc != SALTS_OK) return rc;
  if (count == 0) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column,
                      "worker count must be greater than zero");
  }
  spec->data_strategy = TURBO_FLOW_DATA_WORKER_POOL;
  spec->data_worker_count = count;
  spec->has_worker = 1;
  return SALTS_OK;
}

int flow_parse_set_data_pool(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token) {
  uint32_t capacity = 0;
  int rc;
  if (!spec) return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "stage required");
  if (spec->has_data_pool) {
    return parse_fail(ctx, SALTS_EALREADY, token.line, token.column, "duplicate data pool option");
  }
  rc = flow_parse_u32(ctx, token, &capacity);
  if (rc != SALTS_OK) return rc;
  if (capacity == 0u || capacity > FLOW_WORKER_POOL_MAX_CAPACITY ||
      (capacity & (capacity - 1u)) != 0u) {
    return parse_fail(ctx, SALTS_ERANGE, token.line, token.column,
                      "data pool capacity must be a power of two between 1 and 1048576");
  }
  spec->data_pool_capacity = capacity;
  spec->has_data_pool = 1;
  return SALTS_OK;
}

int flow_parse_set_retry(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t attempts,
                         const flow_token_t *delay) {
  uint32_t max_attempts = 0;
  uint32_t delay_ms = 0;
  int rc;

  if (!spec) return parse_fail(ctx, SALTS_EINVAL, attempts.line, attempts.column, "stage required");
  if (spec->has_retry) {
    return parse_fail(ctx, SALTS_EALREADY, attempts.line, attempts.column,
                      "duplicate retry option");
  }
  rc = flow_parse_u32(ctx, attempts, &max_attempts);
  if (rc != SALTS_OK) return rc;
  if (max_attempts < 2u || max_attempts > TURBO_FLOW_RETRY_MAX_ATTEMPTS) {
    return parse_fail(ctx, SALTS_ERANGE, attempts.line, attempts.column,
                      "retry attempts must be between 2 and 64");
  }
  if (delay) {
    rc = flow_parse_u32(ctx, *delay, &delay_ms);
    if (rc != SALTS_OK) return rc;
    if (delay_ms > TURBO_FLOW_RETRY_MAX_DELAY_MS) {
      return parse_fail(ctx, SALTS_ERANGE, delay->line, delay->column,
                        "retry delay exceeds one hour");
    }
  }
  spec->retry.max_attempts = max_attempts;
  spec->retry.delay_ms = delay_ms;
  spec->has_retry = 1;
  return SALTS_OK;
}

int flow_parse_set_reorder(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t capacity,
                           flow_token_t timeout) {
  uint32_t capacity_value = 0;
  uint32_t timeout_ms = 0;
  int rc;

  if (!spec) return parse_fail(ctx, SALTS_EINVAL, capacity.line, capacity.column, "stage required");
  if (spec->has_reorder) {
    return parse_fail(ctx, SALTS_EALREADY, capacity.line, capacity.column,
                      "duplicate reorder option");
  }
  rc = flow_parse_u32(ctx, capacity, &capacity_value);
  if (rc != SALTS_OK) return rc;
  rc = flow_parse_u32(ctx, timeout, &timeout_ms);
  if (rc != SALTS_OK) return rc;
  if (capacity_value == 0 || capacity_value > TURBO_FLOW_REORDER_MAX_CAPACITY) {
    return parse_fail(ctx, SALTS_ERANGE, capacity.line, capacity.column,
                      "reorder capacity must be between 1 and 1048576");
  }
  if (timeout_ms == 0 || timeout_ms > TURBO_FLOW_REORDER_MAX_TIMEOUT_MS) {
    return parse_fail(ctx, SALTS_ERANGE, timeout.line, timeout.column,
                      "reorder timeout must be between 1 ms and one hour");
  }
  spec->reorder.capacity = capacity_value;
  spec->reorder.timeout_ms = timeout_ms;
  spec->has_reorder = 1;
  return SALTS_OK;
}

int flow_parse_set_adapter(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token) {
  if (!spec || !token.value || token.length == 0) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "adapter name required");
  }
  if (spec->has_adapter) {
    return parse_fail(ctx, SALTS_EALREADY, token.line, token.column, "duplicate adapter option");
  }
  spec->adapter_name = token_view(token);
  spec->adapter_line = token.line;
  spec->adapter_column = token.column;
  spec->has_adapter = 1;
  return SALTS_OK;
}

int flow_parse_set_operation(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token) {
  if (!spec || token.length == 0) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "operation name required");
  }
  if (spec->has_operation) {
    return parse_fail(ctx, SALTS_EALREADY, token.line, token.column, "duplicate operation option");
  }
  spec->operation_name = token_view(token);
  spec->operation_line = token.line;
  spec->operation_column = token.column;
  spec->has_operation = 1;
  return SALTS_OK;
}

int flow_parse_set_resource(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token) {
  if (!spec || token.length == 0) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "resource name required");
  }
  if (spec->has_resource) {
    return parse_fail(ctx, SALTS_EALREADY, token.line, token.column, "duplicate resource option");
  }
  spec->resource_name = token_view(token);
  spec->resource_line = token.line;
  spec->resource_column = token.column;
  spec->has_resource = 1;
  return SALTS_OK;
}

flow_token_t flow_parse_append_dotted_name(flow_parse_ctx_t *ctx, flow_token_t left,
                                           flow_token_t dot, flow_token_t right) {
  flow_token_t out = left;
  const char *left_end = left.value ? left.value + left.length : NULL;
  const char *dot_end = dot.value ? dot.value + dot.length : NULL;

  if (!left.value || left.length == 0 || !dot.value || dot.length != 1 || !right.value ||
      right.length == 0 || left_end != dot.value || dot_end != right.value) {
    parse_fail(ctx, SALTS_EINVAL, dot.line, dot.column,
               "dotted binding names cannot contain whitespace");
    return out;
  }
  out.length = (size_t)((right.value + right.length) - left.value);
  return out;
}

int flow_parse_set_exec_count(flow_parse_ctx_t *ctx, flow_exec_options_t *options,
                              flow_token_t token, int field) {
  uint32_t count = 0;
  uint32_t flag;
  int rc;

  if (!options || field < 0 || field > 2) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "executor option is invalid");
  }
  flag = 1u << (uint32_t)field;
  if (options->seen & flag) {
    return parse_fail(ctx, SALTS_EALREADY, token.line, token.column,
                      field == 0
                          ? "duplicate workers option"
                          : (field == 1 ? "duplicate lanes option" : "duplicate pool option"));
  }
  rc = flow_parse_u32(ctx, token, &count);
  if (rc != SALTS_OK) return rc;
  if (count == 0) {
    return parse_fail(ctx, SALTS_EINVAL, token.line, token.column,
                      "executor option value must be greater than zero");
  }

  if (field == 0) options->config.workers = count;
  else if (field == 1) options->config.lanes = count;
  else options->config.pool_capacity = count;
  options->seen |= flag;
  return SALTS_OK;
}

int flow_parse_set_exec(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_exec_spec_t exec_spec,
                        flow_token_t exec_token) {
  if (!spec) {
    return parse_fail(ctx, SALTS_EINVAL, exec_token.line, exec_token.column, "stage required");
  }
  if (spec->has_exec) {
    return parse_fail(ctx, SALTS_EALREADY, exec_token.line, exec_token.column,
                      "duplicate exec option");
  }
  if (!exec_spec.valid) {
    return parse_fail(ctx, SALTS_EINVAL, exec_token.line, exec_token.column,
                      "unknown executor kind");
  }
  if (exec_spec.exec.kind == TURBO_FLOW_EXEC_INLINE &&
      (exec_spec.exec.workers != 0 || exec_spec.exec.lanes != 0 ||
       exec_spec.exec.pool_capacity != 0)) {
    return parse_fail(ctx, SALTS_EINVAL, exec_token.line, exec_token.column,
                      "executor kind does not accept workers, lanes, or pool options");
  }
  if (exec_spec.exec.kind == TURBO_FLOW_EXEC_THREAD_POOL &&
      (exec_spec.exec.lanes != 0 || exec_spec.exec.pool_capacity != 0)) {
    return parse_fail(ctx, SALTS_EINVAL, exec_token.line, exec_token.column,
                      "thread executor only accepts workers");
  }
  if (exec_spec.exec.kind == TURBO_FLOW_EXEC_CORO_POOL && exec_spec.exec.workers != 0) {
    return parse_fail(ctx, SALTS_EINVAL, exec_token.line, exec_token.column,
                      "coro executor only accepts lanes and pool");
  }
  spec->exec = exec_spec.exec;
  spec->has_exec = 1;
  return SALTS_OK;
}

static tstr make_scoped_name(flow_parse_ctx_t *ctx, vstr name) {
  tstr full;

  if (!ctx->in_stage_template) return tstr_from_v(name);

  full = tstr_from_v(ctx->current_stage_template);
  if (!full) return NULL;
  full = tstr_cat(full, ".");
  if (!full) return NULL;
  full = tstr_cat_v(full, name);
  return full;
}

static int find_stage_template_view(const flow_parse_ctx_t *ctx, vstr name) {
  size_t i;

  for (i = 0; i < vec_size(&ctx->stage_templates); ++i) {
    const flow_stage_template_decl_t *stage_template =
        (const flow_stage_template_decl_t *)vec_at_const(&ctx->stage_templates, i);
    if (!stage_template || !stage_template->name) continue;
    if (tstr_eq_v(stage_template->name, name)) return (int)i;
  }
  return -1;
}

static int name_has_scoped_prefix(vstr name, vstr prefix) {
  if (!name.data || !prefix.data) return 0;
  if (name.len <= prefix.len) return 0;
  if (memcmp(name.data, prefix.data, prefix.len) != 0) return 0;
  return name.data[prefix.len] == '.';
}

static tstr replace_scoped_prefix(vstr name, vstr prefix, vstr replacement) {
  tstr out;

  if (!name_has_scoped_prefix(name, prefix)) return NULL;
  out = tstr_from_v(replacement);
  if (!out) return NULL;
  out = tstr_cat_v(out, vstr_from_buf(name.data + prefix.len, name.len - prefix.len));
  return out;
}

static int clone_tstr(tstr *dst, tstr src) {
  if (!src) return SALTS_OK;
  *dst = tstr_from_v(tstr_to_v(src));
  return *dst ? SALTS_OK : SALTS_ENOMEM;
}

static int push_stage_template_copy(flow_parse_ctx_t *ctx, vstr name,
                                    const flow_stage_template_decl_t *source, uint32_t line,
                                    uint32_t column) {
  flow_stage_template_decl_t stage_template;

  if (flow_find_stage_view(ctx->flow, name) >= 0 || find_stage_template_view(ctx, name) >= 0) {
    return parse_fail(ctx, SALTS_EALREADY, line, column, "duplicate stage or source name");
  }

  memset(&stage_template, 0, sizeof(stage_template));
  stage_template.name = tstr_from_v(name);
  if (!stage_template.name) return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  if (clone_tstr(&stage_template.first_input, source->first_input) != SALTS_OK ||
      clone_tstr(&stage_template.first_output, source->first_output) != SALTS_OK) {
    tstr_freep(&stage_template.name);
    tstr_freep(&stage_template.first_input);
    tstr_freep(&stage_template.first_output);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }
  stage_template.line = line;
  stage_template.column = column;
  stage_template.input_count = source->input_count;
  stage_template.output_count = source->output_count;

  if (turbo_flow_stl_error(vec_push(&ctx->stage_templates, &stage_template)) != SALTS_OK) {
    tstr_freep(&stage_template.name);
    tstr_freep(&stage_template.first_input);
    tstr_freep(&stage_template.first_output);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }
  return SALTS_OK;
}

static int copy_stage_with_prefix(flow_parse_ctx_t *ctx, const flow_stage_plan_impl_t *source,
                                  vstr target_prefix, vstr alias_prefix, uint32_t line,
                                  uint32_t column) {
  flow_stage_plan_impl_t stage;

  memset(&stage, 0, sizeof(stage));
  stage.name = replace_scoped_prefix(tstr_to_v(source->name), target_prefix, alias_prefix);
  if (!stage.name) return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  if (flow_find_stage_view(ctx->flow, tstr_to_v(stage.name)) >= 0 ||
      find_stage_template_view(ctx, tstr_to_v(stage.name)) >= 0) {
    flow_stage_impl_destroy(&stage);
    return parse_fail(ctx, SALTS_EALREADY, line, column, "duplicate stage or source name");
  }

  stage.line = source->line;
  stage.column = source->column;
  stage.is_source = source->is_source;
  stage.is_port = source->is_port;
  stage.is_port_output = source->is_port_output;
  stage.data_strategy = source->data_strategy;
  stage.data_worker_count = source->data_worker_count;
  stage.data_pool_capacity = source->data_pool_capacity;
  stage.exec = source->exec;
  stage.mutability = source->mutability;
  stage.retry = source->retry;
  stage.reorder = source->reorder;
  if (clone_tstr(&stage.adapter_name, source->adapter_name) != SALTS_OK ||
      clone_tstr(&stage.operation_name, source->operation_name) != SALTS_OK ||
      clone_tstr(&stage.resource_name, source->resource_name) != SALTS_OK) {
    flow_stage_impl_destroy(&stage);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }

  if (turbo_flow_stl_error(vec_push(&ctx->flow->stages, &stage)) != SALTS_OK) {
    flow_stage_impl_destroy(&stage);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }
  return SALTS_OK;
}

static int copy_edge_with_prefix(flow_parse_ctx_t *ctx, const flow_edge_plan_impl_t *source,
                                 vstr target_prefix, vstr alias_prefix, uint32_t line,
                                 uint32_t column) {
  flow_edge_plan_impl_t edge;

  memset(&edge, 0, sizeof(edge));
  edge.from_name = replace_scoped_prefix(tstr_to_v(source->from_name), target_prefix, alias_prefix);
  edge.to_name = replace_scoped_prefix(tstr_to_v(source->to_name), target_prefix, alias_prefix);
  if (!edge.from_name || !edge.to_name) {
    flow_edge_impl_destroy(&edge);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }
  edge.from_stage = UINT32_MAX;
  edge.to_stage = UINT32_MAX;
  edge.line = source->line;
  edge.column = source->column;
  edge.is_stage_internal = source->is_stage_internal;
  edge.kind = source->kind;
  if (clone_tstr(&edge.condition, source->condition) != SALTS_OK ||
      clone_tstr(&edge.name, source->name) != SALTS_OK) {
    flow_edge_impl_destroy(&edge);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }

  if (turbo_flow_stl_error(vec_push(&ctx->flow->edges, &edge)) != SALTS_OK) {
    flow_edge_impl_destroy(&edge);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }
  return SALTS_OK;
}

static int stage_plan_add(flow_parse_ctx_t *ctx, vstr name, int is_source, int is_port,
                          int is_port_output, flow_stage_spec_t spec, uint32_t line,
                          uint32_t column) {
  flow_stage_plan_impl_t stage;
  int stage_index;

  if (flow_find_stage_view(ctx->flow, name) >= 0 || find_stage_template_view(ctx, name) >= 0) {
    return parse_fail(ctx, SALTS_EALREADY, line, column, "duplicate stage or source name");
  }

  memset(&stage, 0, sizeof(stage));
  stage.name = tstr_from_v(name);
  if (!stage.name) return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  stage.line = line;
  stage.column = column;
  stage.is_source = is_source;
  stage.is_port = is_port;
  stage.is_port_output = is_port_output;
  stage.data_strategy = spec.data_strategy;
  stage.data_worker_count = spec.data_worker_count;
  stage.data_pool_capacity = spec.data_pool_capacity;
  stage.exec = spec.exec;
  stage.retry = spec.retry;
  stage.reorder = spec.reorder;
  stage.mutability = TURBO_FLOW_STAGE_READONLY;
  if (spec.adapter_name.data && spec.adapter_name.len > 0) {
    stage.adapter_name = tstr_from_v(spec.adapter_name);
    if (!stage.adapter_name) {
      flow_stage_impl_destroy(&stage);
      return parse_fail(ctx, SALTS_ENOMEM, spec.adapter_line, spec.adapter_column, "out of memory");
    }
  }
  if (spec.operation_name.data && spec.operation_name.len > 0) {
    stage.operation_name = tstr_from_v(spec.operation_name);
    if (!stage.operation_name) {
      flow_stage_impl_destroy(&stage);
      return parse_fail(ctx, SALTS_ENOMEM, spec.operation_line, spec.operation_column,
                        "out of memory");
    }
  }
  if (spec.resource_name.data && spec.resource_name.len > 0) {
    stage.resource_name = tstr_from_v(spec.resource_name);
    if (!stage.resource_name) {
      flow_stage_impl_destroy(&stage);
      return parse_fail(ctx, SALTS_ENOMEM, spec.resource_line, spec.resource_column,
                        "out of memory");
    }
  }
  if (turbo_flow_stl_error(vec_push(&ctx->flow->stages, &stage)) != SALTS_OK) {
    flow_stage_impl_destroy(&stage);
    return parse_fail(ctx, SALTS_ENOMEM, line, column, "out of memory");
  }

  stage_index = flow_find_stage_view(ctx->flow, name);
  if (stage_index < 0) {
    return parse_fail(ctx, SALTS_EINVAL, line, column, "stage declaration was not recorded");
  }
  return SALTS_OK;
}

int flow_parse_add_source(flow_parse_ctx_t *ctx, flow_token_t name, flow_stage_spec_t spec) {
  if (ctx->in_stage_template) {
    return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "source declarations are only allowed in the root stage");
  }
  if (ctx->has_root_stage && !ctx->in_root_stage) {
    return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "declarations are not allowed after the root block");
  }
  return stage_plan_add(ctx, token_view(name), 1, 0, 0, spec, name.line, name.column);
}

int flow_parse_add_stage(flow_parse_ctx_t *ctx, flow_token_t name, flow_stage_spec_t spec) {
  tstr scoped = make_scoped_name(ctx, token_view(name));
  int rc;

  if (spec.has_data_pool && !spec.has_worker) {
    tstr_freep(&scoped);
    return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "worker capacity requires a worker option");
  }
  if (!ctx->in_stage_template && ctx->has_root_stage && !ctx->in_root_stage) {
    return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "declarations are not allowed after the root block");
  }
  if (!scoped) return parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
  rc = stage_plan_add(ctx, tstr_to_v(scoped), 0, 0, 0, spec, name.line, name.column);
  tstr_freep(&scoped);
  return rc;
}

int flow_parse_add_port(flow_parse_ctx_t *ctx, flow_token_t name, int is_output) {
  tstr scoped;
  flow_stage_spec_t spec = flow_stage_spec_default();
  flow_stage_template_decl_t *stage_template;
  int rc;

  if (!ctx->in_stage_template) {
    return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "stage ports require a stage block");
  }

  scoped = make_scoped_name(ctx, token_view(name));
  if (!scoped) return parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
  rc = stage_plan_add(ctx, tstr_to_v(scoped), 0, 1, is_output, spec, name.line, name.column);
  if (rc == SALTS_OK) {
    stage_template = (flow_stage_template_decl_t *)vec_at(&ctx->stage_templates,
                                                                ctx->current_stage_template_index);
    if (!stage_template) {
      rc = parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "stage declaration state is invalid");
    } else if (is_output) {
      stage_template->output_count += 1u;
      if (!stage_template->first_output) {
        stage_template->first_output = tstr_from_v(token_view(name));
        if (!stage_template->first_output) {
          rc = parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
        }
      }
    } else {
      stage_template->input_count += 1u;
      if (!stage_template->first_input) {
        stage_template->first_input = tstr_from_v(token_view(name));
        if (!stage_template->first_input) {
          rc = parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
        }
      }
    }
  }
  tstr_freep(&scoped);
  return rc;
}

int flow_parse_use_stage(flow_parse_ctx_t *ctx, flow_token_t alias, flow_token_t target) {
  tstr alias_name;
  vstr alias_view;
  vstr target_view = token_view(target);
  const flow_stage_template_decl_t *target_template;
  size_t stage_count;
  size_t edge_count;
  size_t template_count;
  int target_index;
  int rc;
  size_t i;

  if (!ctx || ctx->error) return ctx ? ctx->flow->last_error.code : SALTS_EINVAL;
  if (!ctx->in_root_stage && !ctx->in_stage_template) {
    return parse_fail(ctx, SALTS_EINVAL, alias.line, alias.column,
                      "use declarations require a stage block");
  }

  target_index = find_stage_template_view(ctx, target_view);
  if (target_index < 0) {
    return parse_fail(ctx, SALTS_EINVAL, target.line, target.column, "unknown reusable stage");
  }
  if (ctx->in_stage_template && ctx->current_stage_template.len == target_view.len &&
      memcmp(ctx->current_stage_template.data, target_view.data, target_view.len) == 0) {
    return parse_fail(ctx, SALTS_EINVAL, target.line, target.column, "stage cannot use itself");
  }

  target_template = (const flow_stage_template_decl_t *)vec_at_const(&ctx->stage_templates,
                                                                           (size_t)target_index);
  if (!target_template) {
    return parse_fail(ctx, SALTS_EINVAL, target.line, target.column,
                      "reusable stage declaration state is invalid");
  }

  alias_name = make_scoped_name(ctx, token_view(alias));
  if (!alias_name) return parse_fail(ctx, SALTS_ENOMEM, alias.line, alias.column, "out of memory");
  alias_view = tstr_to_v(alias_name);

  stage_count = vec_size(&ctx->flow->stages);
  edge_count = vec_size(&ctx->flow->edges);
  template_count = vec_size(&ctx->stage_templates);

  rc = push_stage_template_copy(ctx, alias_view, target_template, alias.line, alias.column);
  if (rc != SALTS_OK) goto cleanup;

  for (i = 0; i < template_count; ++i) {
    const flow_stage_template_decl_t *nested =
        (const flow_stage_template_decl_t *)vec_at_const(&ctx->stage_templates, i);
    tstr nested_name;

    if (!nested || !nested->name || !name_has_scoped_prefix(tstr_to_v(nested->name), target_view)) {
      continue;
    }
    nested_name = replace_scoped_prefix(tstr_to_v(nested->name), target_view, alias_view);
    if (!nested_name) {
      rc = parse_fail(ctx, SALTS_ENOMEM, alias.line, alias.column, "out of memory");
      goto cleanup;
    }
    rc = push_stage_template_copy(ctx, tstr_to_v(nested_name), nested, alias.line, alias.column);
    tstr_freep(&nested_name);
    if (rc != SALTS_OK) goto cleanup;
  }

  for (i = 0; i < stage_count; ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&ctx->flow->stages, i);

    if (!stage || !stage->name || !name_has_scoped_prefix(tstr_to_v(stage->name), target_view)) {
      continue;
    }
    rc = copy_stage_with_prefix(ctx, stage, target_view, alias_view, alias.line, alias.column);
    if (rc != SALTS_OK) goto cleanup;
  }

  for (i = 0; i < edge_count; ++i) {
    const flow_edge_plan_impl_t *edge =
        (const flow_edge_plan_impl_t *)vec_at_const(&ctx->flow->edges, i);

    if (!edge || !edge->is_stage_internal || !edge->from_name || !edge->to_name ||
        !name_has_scoped_prefix(tstr_to_v(edge->from_name), target_view) ||
        !name_has_scoped_prefix(tstr_to_v(edge->to_name), target_view)) {
      continue;
    }
    rc = copy_edge_with_prefix(ctx, edge, target_view, alias_view, alias.line, alias.column);
    if (rc != SALTS_OK) goto cleanup;
  }

cleanup:
  tstr_freep(&alias_name);
  return rc;
}

int flow_parse_enter_stage_block(flow_parse_ctx_t *ctx, flow_token_t name) {
  if (!token_eq_cstr(name, "main")) {
    if (ctx->has_root_stage) {
      return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                        "stage declarations are not allowed after the root stage");
    }
    return flow_parse_enter_stage_template(ctx, name);
  }

  if (ctx->has_root_stage) {
    return parse_fail(ctx, SALTS_EALREADY, name.line, name.column,
                      "only one root stage orchestration block is supported");
  }

  tstr_freep(&ctx->root_stage_name);
  ctx->root_stage_name = tstr_from_v(token_view(name));
  if (!ctx->root_stage_name)
    return parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
  ctx->has_root_stage = 1;
  ctx->in_root_stage = 1;
  return SALTS_OK;
}

void flow_parse_leave_stage_block(flow_parse_ctx_t *ctx) {
  if (!ctx) return;
  if (ctx->in_stage_template) flow_parse_leave_stage_template(ctx);
  else ctx->in_root_stage = 0;
}

int flow_parse_enter_stage_template(flow_parse_ctx_t *ctx, flow_token_t name) {
  flow_stage_template_decl_t stage_template;
  size_t index;

  if (ctx->in_stage_template) {
    return parse_fail(ctx, SALTS_EINVAL, name.line, name.column,
                      "nested stage declarations are not supported");
  }
  if (flow_find_stage_view(ctx->flow, token_view(name)) >= 0 ||
      find_stage_template_view(ctx, token_view(name)) >= 0) {
    return parse_fail(ctx, SALTS_EALREADY, name.line, name.column,
                      "duplicate stage or source name");
  }

  memset(&stage_template, 0, sizeof(stage_template));
  stage_template.name = tstr_from_v(token_view(name));
  if (!stage_template.name)
    return parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
  stage_template.line = name.line;
  stage_template.column = name.column;

  index = vec_size(&ctx->stage_templates);
  if (turbo_flow_stl_error(vec_push(&ctx->stage_templates, &stage_template)) != SALTS_OK) {
    tstr_freep(&stage_template.name);
    return parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
  }

  ctx->current_stage_template = tstr_to_v(stage_template.name);
  ctx->current_stage_template_index = index;
  ctx->in_stage_template = 1;
  return SALTS_OK;
}

void flow_parse_leave_stage_template(flow_parse_ctx_t *ctx) {
  if (!ctx) return;
  ctx->current_stage_template = vstr_from_buf(NULL, 0);
  ctx->current_stage_template_index = SIZE_MAX;
  ctx->in_stage_template = 0;
}

flow_node_list_t flow_parse_node(flow_parse_ctx_t *ctx, flow_token_t name) {
  flow_node_ref_t ref;
  flow_node_list_t list;

  memset(&list, 0, sizeof(list));
  memset(&ref, 0, sizeof(ref));
  ref.first = token_view(name);
  ref.line = name.line;
  ref.column = name.column;

  list.start = vec_size(&ctx->node_refs);
  list.count = 1;
  if (turbo_flow_stl_error(vec_push(&ctx->node_refs, &ref)) != SALTS_OK) {
    parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
    list.count = 0;
  }
  return list;
}

flow_node_list_t flow_parse_qualified_node(flow_parse_ctx_t *ctx, flow_token_t first,
                                           flow_token_t second) {
  flow_node_ref_t ref;
  flow_node_list_t list;

  memset(&list, 0, sizeof(list));
  memset(&ref, 0, sizeof(ref));
  ref.first = token_view(first);
  ref.second = token_view(second);
  ref.line = first.line;
  ref.column = first.column;
  ref.qualified = 1;

  list.start = vec_size(&ctx->node_refs);
  list.count = 1;
  if (turbo_flow_stl_error(vec_push(&ctx->node_refs, &ref)) != SALTS_OK) {
    parse_fail(ctx, SALTS_ENOMEM, first.line, first.column, "out of memory");
    list.count = 0;
  }
  return list;
}

flow_node_list_t flow_parse_node_list_append(flow_node_list_t left, flow_node_list_t right) {
  flow_node_list_t out = left;
  out.count += right.count;
  return out;
}

static tstr make_stage_template_port_name(flow_parse_ctx_t *ctx,
                                            const flow_stage_template_decl_t *stage_template,
                                            const flow_node_ref_t *ref, int use_output) {
  tstr name;
  const tstr port = use_output ? stage_template->first_output : stage_template->first_input;
  uint32_t port_count = use_output ? stage_template->output_count : stage_template->input_count;

  if (port_count != 1u || !port) {
    parse_fail(ctx, SALTS_EINVAL, ref->line, ref->column,
               "stage shorthand requires exactly one input and one output port");
    return NULL;
  }

  name = tstr_from_v(tstr_to_v(stage_template->name));
  if (!name) return NULL;
  name = tstr_cat(name, ".");
  if (!name) return NULL;
  name = tstr_cat_v(name, tstr_to_v(port));
  return name;
}

static tstr resolve_node_name(flow_parse_ctx_t *ctx, const flow_node_ref_t *ref, int use_output) {
  tstr name;
  int template_index;

  if (ref->qualified) {
    name = tstr_from_v(ref->first);
    if (!name) return NULL;
    name = tstr_cat(name, ".");
    if (!name) return NULL;
    name = tstr_cat_v(name, ref->second);
    if (ctx->in_stage_template) {
      tstr scoped = make_scoped_name(ctx, tstr_to_v(name));
      tstr_freep(&name);
      return scoped;
    }
    return name;
  }

  if (ctx->in_stage_template) {
    name = make_scoped_name(ctx, ref->first);
    if (!name) return NULL;
    template_index = find_stage_template_view(ctx, tstr_to_v(name));
    if (template_index >= 0 && flow_find_stage_view(ctx->flow, tstr_to_v(name)) < 0) {
      const flow_stage_template_decl_t *stage_template =
          (const flow_stage_template_decl_t *)vec_at_const(&ctx->stage_templates,
                                                                 (size_t)template_index);
      tstr_freep(&name);
      return make_stage_template_port_name(ctx, stage_template, ref, use_output);
    }
    return name;
  }

  template_index = find_stage_template_view(ctx, ref->first);
  if (template_index >= 0 && flow_find_stage_view(ctx->flow, ref->first) < 0) {
    const flow_stage_template_decl_t *stage_template =
        (const flow_stage_template_decl_t *)vec_at_const(&ctx->stage_templates,
                                                               (size_t)template_index);
    return make_stage_template_port_name(ctx, stage_template, ref, use_output);
  }

  return tstr_from_v(ref->first);
}

int flow_parse_add_edges(flow_parse_ctx_t *ctx, flow_node_list_t from, flow_node_list_t to,
                         flow_token_t arrow) {
  size_t i;
  size_t j;

  if (ctx->error) return ctx->flow->last_error.code;

  for (i = 0; i < from.count; ++i) {
    const flow_node_ref_t *from_ref =
        (const flow_node_ref_t *)vec_at_const(&ctx->node_refs, from.start + i);

    for (j = 0; j < to.count; ++j) {
      const flow_node_ref_t *to_ref =
          (const flow_node_ref_t *)vec_at_const(&ctx->node_refs, to.start + j);
      flow_edge_plan_impl_t edge;

      memset(&edge, 0, sizeof(edge));
      edge.from_name = resolve_node_name(ctx, from_ref, 1);
      edge.to_name = resolve_node_name(ctx, to_ref, 0);
      if (!edge.from_name || !edge.to_name) {
        flow_edge_impl_destroy(&edge);
        if (ctx->error) return ctx->flow->last_error.code;
        return parse_fail(ctx, SALTS_ENOMEM, arrow.line, arrow.column, "out of memory");
      }
      edge.from_stage = UINT32_MAX;
      edge.to_stage = UINT32_MAX;
      edge.line = arrow.line;
      edge.column = arrow.column;
      edge.is_stage_internal = ctx->in_stage_template;

      if (turbo_flow_stl_error(vec_push(&ctx->flow->edges, &edge)) != SALTS_OK) {
        flow_edge_impl_destroy(&edge);
        return parse_fail(ctx, SALTS_ENOMEM, arrow.line, arrow.column, "out of memory");
      }
    }
  }

  return SALTS_OK;
}

int flow_parse_add_conditional_edge(flow_parse_ctx_t *ctx, flow_node_list_t from,
                                    flow_node_list_t to, flow_token_t arrow,
                                    flow_token_t condition) {
  const flow_node_ref_t *from_ref;
  const flow_node_ref_t *to_ref;
  flow_edge_plan_impl_t edge;

  if (!ctx || ctx->error) return ctx ? ctx->flow->last_error.code : SALTS_EINVAL;
  if (ctx->in_stage_template || from.count != 1u || to.count != 1u || condition.length == 0u) {
    return parse_fail(ctx, SALTS_EINVAL, arrow.line, arrow.column,
                      "conditional route requires one flow-level source and destination");
  }
  from_ref = (const flow_node_ref_t *)vec_at_const(&ctx->node_refs, from.start);
  to_ref = (const flow_node_ref_t *)vec_at_const(&ctx->node_refs, to.start);
  if (!from_ref || !to_ref) {
    return parse_fail(ctx, SALTS_EINVAL, arrow.line, arrow.column,
                      "conditional route contains an invalid node");
  }

  memset(&edge, 0, sizeof(edge));
  edge.from_name = resolve_node_name(ctx, from_ref, 1);
  edge.to_name = resolve_node_name(ctx, to_ref, 0);
  edge.condition = tstr_from_v(vstr_from_buf(condition.value, condition.length));
  if (!edge.from_name || !edge.to_name || !edge.condition) {
    flow_edge_impl_destroy(&edge);
    if (ctx->error) return ctx->flow->last_error.code;
    return parse_fail(ctx, SALTS_ENOMEM, condition.line, condition.column, "out of memory");
  }
  edge.from_stage = UINT32_MAX;
  edge.to_stage = UINT32_MAX;
  edge.line = arrow.line;
  edge.column = arrow.column;
  edge.kind = TURBO_FLOW_EDGE_CONDITIONAL;
  if (turbo_flow_stl_error(vec_push(&ctx->flow->edges, &edge)) != SALTS_OK) {
    flow_edge_impl_destroy(&edge);
    return parse_fail(ctx, SALTS_ENOMEM, arrow.line, arrow.column, "out of memory");
  }
  return SALTS_OK;
}

int flow_parse_add_reject_edge(flow_parse_ctx_t *ctx, flow_token_t name, flow_node_list_t from,
                               flow_node_list_t to, flow_token_t arrow) {
  const flow_node_ref_t *from_ref;
  const flow_node_ref_t *to_ref;
  flow_edge_plan_impl_t edge;

  if (!ctx || ctx->error) return ctx ? ctx->flow->last_error.code : SALTS_EINVAL;
  if (ctx->in_stage_template || from.count != 1u || to.count != 1u || name.length == 0u) {
    return parse_fail(ctx, SALTS_EINVAL, arrow.line, arrow.column,
                      "reject route requires a name and one flow-level source and destination");
  }
  from_ref = (const flow_node_ref_t *)vec_at_const(&ctx->node_refs, from.start);
  to_ref = (const flow_node_ref_t *)vec_at_const(&ctx->node_refs, to.start);
  if (!from_ref || !to_ref) {
    return parse_fail(ctx, SALTS_EINVAL, arrow.line, arrow.column,
                      "reject route contains an invalid node");
  }

  memset(&edge, 0, sizeof(edge));
  edge.from_name = resolve_node_name(ctx, from_ref, 1);
  edge.to_name = resolve_node_name(ctx, to_ref, 0);
  edge.name = tstr_from_v(vstr_from_buf(name.value, name.length));
  if (!edge.from_name || !edge.to_name || !edge.name) {
    flow_edge_impl_destroy(&edge);
    if (ctx->error) return ctx->flow->last_error.code;
    return parse_fail(ctx, SALTS_ENOMEM, name.line, name.column, "out of memory");
  }
  edge.from_stage = UINT32_MAX;
  edge.to_stage = UINT32_MAX;
  edge.line = arrow.line;
  edge.column = arrow.column;
  edge.kind = TURBO_FLOW_EDGE_REJECT;
  if (turbo_flow_stl_error(vec_push(&ctx->flow->edges, &edge)) != SALTS_OK) {
    flow_edge_impl_destroy(&edge);
    return parse_fail(ctx, SALTS_ENOMEM, arrow.line, arrow.column, "out of memory");
  }
  return SALTS_OK;
}

void flow_parse_syntax_error(flow_parse_ctx_t *ctx, flow_token_t token) {
  if (!ctx || ctx->error) return;
  if (token.value && token.length > 0) {
    parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "syntax error in flow grammar");
  } else {
    parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "unexpected end of flow grammar");
  }
}

void flow_parse_unknown_executor(flow_parse_ctx_t *ctx, flow_token_t token) {
  parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "unknown executor kind");
}

static int run_generated_parser(flow_parse_ctx_t *ctx, const char *text, size_t len) {
  flow_lexer_t lexer;
  flow_token_t token;
  flow_token_t last_token;
  void *parser;
  int token_id;
  int saw_token = 0;

  flow_lexer_init(&lexer, text, len);
  memset(&last_token, 0, sizeof(last_token));

  parser = TurboFlowParseAlloc(malloc);
  if (!parser) return parse_fail(ctx, SALTS_ENOMEM, 0, 0, "out of memory");

  while ((token_id = flow_lexer_next(&lexer, &token)) != 0) {
    if (token_id < 0) {
      parse_fail(ctx, SALTS_EINVAL, token.line, token.column, "unexpected character");
      break;
    }

    TurboFlowParse(parser, token_id, token, ctx);
    last_token = token;
    saw_token = 1;
    if (ctx->error) break;
  }

  if (!ctx->error) {
    if (!saw_token || token_id == 0) {
      if (!saw_token || last_token.length == 0 ||
          !(last_token.value[0] == '\n' || last_token.value[0] == '\r')) {
        flow_token_t newline;
        memset(&newline, 0, sizeof(newline));
        newline.line = lexer.line;
        newline.column = lexer.column;
        TurboFlowParse(parser, TURBO_FLOW_TOKEN_NEWLINE, newline, ctx);
      }
    }
  }

  if (!ctx->error) {
    memset(&token, 0, sizeof(token));
    token.line = lexer.line;
    token.column = lexer.column;
    TurboFlowParse(parser, 0, token, ctx);
  }

  TurboFlowParseFree(parser, free);
  return ctx->error ? ctx->flow->last_error.code : SALTS_OK;
}

int turbo_flow_parse_string(turbo_flow_t *flow, const char *text, size_t len) {
  flow_parse_ctx_t ctx;
  int rc;

  if (!flow || (!text && len > 0)) return SALTS_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, SALTS_EBUSY, 0, 0, "cannot parse after compile");
  }

  flow_clear_plan(flow);
  flow_clear_error(flow);

  rc = flow_parse_ctx_init(&ctx, flow);
  if (rc != SALTS_OK) return rc;

  rc = run_generated_parser(&ctx, text ? text : "", len);
  flow_parse_ctx_destroy(&ctx);
  if (rc != SALTS_OK) return rc;

  flow->state = TURBO_FLOW_STATE_PARSED;
  return SALTS_OK;
}
