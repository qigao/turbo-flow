#ifndef TURBO_FLOW_PARSER_INTERNAL_H
#define TURBO_FLOW_PARSER_INTERNAL_H

#include "flow_internal.h"

#include <stddef.h>
#include <stdint.h>

typedef struct flow_lexer_s {
  const char *input;
  const char *cursor;
  const char *limit;
  uint32_t line;
  uint32_t column;
  int expression_pending;
  int line_has_arrow;
} flow_lexer_t;

typedef struct flow_token_s {
  const char *value;
  size_t length;
  uint32_t line;
  uint32_t column;
} flow_token_t;

typedef struct flow_node_ref_s {
  vstr first;
  vstr second;
  uint32_t line;
  uint32_t column;
  int qualified;
} flow_node_ref_t;

typedef struct flow_node_list_s {
  size_t start;
  size_t count;
} flow_node_list_t;

typedef struct flow_exec_spec_s {
  turbo_flow_exec_config_t exec;
  int valid;
} flow_exec_spec_t;

typedef struct flow_exec_options_s {
  turbo_flow_exec_config_t config;
  uint32_t seen;
} flow_exec_options_t;

typedef struct flow_stage_spec_s {
  turbo_flow_data_strategy_t data_strategy;
  uint32_t data_worker_count;
  uint32_t data_pool_capacity;
  int has_worker;
  int has_data_pool;
  vstr adapter_name;
  uint32_t adapter_line;
  uint32_t adapter_column;
  int has_adapter;
  vstr operation_name;
  uint32_t operation_line;
  uint32_t operation_column;
  int has_operation;
  vstr resource_name;
  uint32_t resource_line;
  uint32_t resource_column;
  int has_resource;
  turbo_flow_exec_config_t exec;
  int has_exec;
  turbo_flow_retry_policy_t retry;
  int has_retry;
  turbo_flow_reorder_config_t reorder;
  int has_reorder;
} flow_stage_spec_t;

typedef struct flow_parse_ctx_s {
  turbo_flow_t *flow;
  vec_t node_refs;
  vec_t stage_templates;
  tstr root_stage_name;
  vstr current_stage_template;
  size_t current_stage_template_index;
  int has_root_stage;
  int in_root_stage;
  int in_stage_template;
  int error;
} flow_parse_ctx_t;

void flow_lexer_init(flow_lexer_t *lexer, const char *input, size_t length);
int flow_lexer_next(flow_lexer_t *lexer, flow_token_t *token);

int flow_parse_ctx_init(flow_parse_ctx_t *ctx, turbo_flow_t *flow);
void flow_parse_ctx_destroy(flow_parse_ctx_t *ctx);

flow_stage_spec_t flow_stage_spec_default(void);
flow_exec_spec_t flow_exec_spec_default(void);
flow_exec_spec_t flow_exec_spec_make(turbo_flow_exec_kind_t kind, flow_exec_options_t options);
turbo_flow_exec_config_t flow_exec_config_default(void);
flow_exec_options_t flow_exec_options_default(void);

int flow_parse_u32(flow_parse_ctx_t *ctx, flow_token_t token, uint32_t *out);
int flow_parse_set_worker(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token);
int flow_parse_set_data_pool(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token);
int flow_parse_set_retry(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t attempts,
                         const flow_token_t *delay);
int flow_parse_set_reorder(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t capacity,
                           flow_token_t timeout);
int flow_parse_set_adapter(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token);
int flow_parse_set_operation(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token);
int flow_parse_set_resource(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_token_t token);
flow_token_t flow_parse_append_dotted_name(flow_parse_ctx_t *ctx, flow_token_t left,
                                           flow_token_t dot, flow_token_t right);
int flow_parse_set_exec(flow_parse_ctx_t *ctx, flow_stage_spec_t *spec, flow_exec_spec_t exec_spec,
                        flow_token_t exec_token);
int flow_parse_set_exec_count(flow_parse_ctx_t *ctx, flow_exec_options_t *options,
                              flow_token_t token, int field);

int flow_parse_add_source(flow_parse_ctx_t *ctx, flow_token_t name, flow_stage_spec_t spec);
int flow_parse_add_buffer(flow_parse_ctx_t *ctx, flow_token_t name, flow_token_t resource);
int flow_parse_add_stage(flow_parse_ctx_t *ctx, flow_token_t name, flow_stage_spec_t spec);
int flow_parse_add_port(flow_parse_ctx_t *ctx, flow_token_t name, int is_output);
int flow_parse_use_stage(flow_parse_ctx_t *ctx, flow_token_t alias, flow_token_t target);
int flow_parse_enter_stage_block(flow_parse_ctx_t *ctx, flow_token_t name);
void flow_parse_leave_stage_block(flow_parse_ctx_t *ctx);
int flow_parse_enter_stage_template(flow_parse_ctx_t *ctx, flow_token_t name);
void flow_parse_leave_stage_template(flow_parse_ctx_t *ctx);

flow_node_list_t flow_parse_node(flow_parse_ctx_t *ctx, flow_token_t name);
flow_node_list_t flow_parse_qualified_node(flow_parse_ctx_t *ctx, flow_token_t first,
                                           flow_token_t second);
flow_node_list_t flow_parse_node_list_append(flow_node_list_t left, flow_node_list_t right);
int flow_parse_add_edges(flow_parse_ctx_t *ctx, flow_node_list_t from, flow_node_list_t to,
                         flow_token_t arrow);
int flow_parse_add_conditional_edge(flow_parse_ctx_t *ctx, flow_node_list_t from,
                                    flow_node_list_t to, flow_token_t arrow,
                                    flow_token_t condition);
int flow_parse_add_reject_edge(flow_parse_ctx_t *ctx, flow_token_t name, flow_node_list_t from,
                               flow_node_list_t to, flow_token_t arrow);
void flow_parse_syntax_error(flow_parse_ctx_t *ctx, flow_token_t token);
void flow_parse_unknown_executor(flow_parse_ctx_t *ctx, flow_token_t token);

#endif /* TURBO_FLOW_PARSER_INTERNAL_H */
