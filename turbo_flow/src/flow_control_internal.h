#ifndef TURBO_FLOW_CONTROL_INTERNAL_H
#define TURBO_FLOW_CONTROL_INTERNAL_H

#include "turbo_flow.h"

typedef struct flow_control_token_s {
  const char *value;
  size_t length;
  uint32_t line;
  uint32_t column;
} flow_control_token_t;

typedef struct flow_control_lexer_s {
  const char *cursor;
  const char *limit;
  uint32_t line;
  uint32_t column;
  int condition_pending;
} flow_control_lexer_t;

typedef struct flow_control_parse_ctx_s {
  turbo_flow_control_command_t command;
  turbo_flow_error_t error;
  int has_command;
  flow_control_token_t condition;
} flow_control_parse_ctx_t;

void flow_control_lexer_init(flow_control_lexer_t *lexer, const char *input, size_t length);
int flow_control_lexer_next(flow_control_lexer_t *lexer, flow_control_token_t *token);
void flow_control_set_simple(flow_control_parse_ctx_t *ctx, turbo_flow_control_kind_t kind);
void flow_control_set_drain(flow_control_parse_ctx_t *ctx, flow_control_token_t timeout);
void flow_control_set_resize(flow_control_parse_ctx_t *ctx, flow_control_token_t target,
                             turbo_flow_pool_kind_t kind, flow_control_token_t parallelism,
                             flow_control_token_t timeout);
void flow_control_set_adapter(flow_control_parse_ctx_t *ctx, flow_control_token_t target,
                              turbo_flow_adapter_command_kind_t kind);
void flow_control_set_replace(flow_control_parse_ctx_t *ctx, flow_control_token_t target,
                              flow_control_token_t host, flow_control_token_t port,
                              flow_control_token_t path);
void flow_control_syntax_error(flow_control_parse_ctx_t *ctx, flow_control_token_t token);
void flow_control_set_condition(flow_control_parse_ctx_t *ctx, flow_control_token_t token);

#endif
