%name TurboFlowParse
%token_prefix TURBO_FLOW_TOKEN_
%token_type {flow_token_t}
%default_type {flow_node_list_t}
%stack_size 512

%extra_argument {flow_parse_ctx_t *ctx}

%include {
#include "flow_parser_internal.h"
#include <string.h>
}

%token SOURCE STAGE STEP USE IN OUT WORKER EXEC WORKERS LANES POOL ADAPTER OPERATION RESOURCE.
%token INLINE THREAD CORO ROUTE WHEN REJECT RETRY ATTEMPTS DELAY.
%token REORDER CAPACITY TIMEOUT.
%token IDENT NUMBER STRING ARROW EXPR.
%token LBRACE RBRACE LBRACKET RBRACKET COMMA DOT EQUAL NEWLINE.

%type stage_options {flow_stage_spec_t}
%type source_options {flow_stage_spec_t}
%type exec_spec {flow_exec_spec_t}
%type exec_options {flow_exec_options_t}
%type adapter_name {flow_token_t}
%type dotted_name {flow_token_t}
%type binding_segment {flow_token_t}

%start_symbol start

start ::= program.

program ::= top_items.

top_items ::= .
top_items ::= top_items top_item.

top_item ::= source_decl NEWLINE.
top_item ::= stage_decl NEWLINE.
top_item ::= stage_block.
top_item ::= NEWLINE.

source_decl ::= SOURCE IDENT(N) source_options(O). {
  flow_parse_add_source(ctx, N, O);
}

stage_decl ::= STAGE IDENT(N) stage_options(O). {
  flow_parse_add_stage(ctx, N, O);
}
step_decl ::= STEP IDENT(N) stage_options(O). {
  flow_parse_add_stage(ctx, N, O);
}

source_options(A) ::= . {
  A = flow_stage_spec_default();
}
source_options(A) ::= source_options(B) ADAPTER adapter_name(N). {
  A = B;
  flow_parse_set_adapter(ctx, &A, N);
}
source_options(A) ::= source_options(B) OPERATION adapter_name(N). {
  A = B;
  flow_parse_set_operation(ctx, &A, N);
}
source_options(A) ::= source_options(B) RESOURCE adapter_name(N). {
  A = B;
  flow_parse_set_resource(ctx, &A, N);
}

stage_options(A) ::= . {
  A = flow_stage_spec_default();
}
stage_options(A) ::= stage_options(B) WORKER NUMBER(N). {
  A = B;
  flow_parse_set_worker(ctx, &A, N);
}
stage_options(A) ::= stage_options(B) CAPACITY NUMBER(N). {
  A = B;
  flow_parse_set_data_pool(ctx, &A, N);
}
stage_options(A) ::= stage_options(B) ADAPTER adapter_name(N). {
  A = B;
  flow_parse_set_adapter(ctx, &A, N);
}
stage_options(A) ::= stage_options(B) OPERATION adapter_name(N). {
  A = B;
  flow_parse_set_operation(ctx, &A, N);
}
stage_options(A) ::= stage_options(B) RESOURCE adapter_name(N). {
  A = B;
  flow_parse_set_resource(ctx, &A, N);
}
stage_options(A) ::= stage_options(B) EXEC(E) exec_spec(S). {
  A = B;
  flow_parse_set_exec(ctx, &A, S, E);
}
stage_options(A) ::= stage_options(B) RETRY ATTEMPTS NUMBER(N). {
  A = B;
  flow_parse_set_retry(ctx, &A, N, NULL);
}
stage_options(A) ::= stage_options(B) RETRY ATTEMPTS NUMBER(N) DELAY NUMBER(D). {
  A = B;
  flow_parse_set_retry(ctx, &A, N, &D);
}
stage_options(A) ::= stage_options(B) REORDER CAPACITY NUMBER(C) TIMEOUT NUMBER(T). {
  A = B;
  flow_parse_set_reorder(ctx, &A, C, T);
}
stage_options(A) ::= stage_options(B) EXEC(E) IDENT(BAD). {
  A = B;
  (void)E;
  flow_parse_unknown_executor(ctx, BAD);
}

exec_spec(A) ::= INLINE exec_options(O). {
  A = flow_exec_spec_make(TURBO_FLOW_EXEC_INLINE, O);
}
exec_spec(A) ::= THREAD exec_options(O). {
  A = flow_exec_spec_make(TURBO_FLOW_EXEC_THREAD_POOL, O);
}
exec_spec(A) ::= CORO exec_options(O). {
  A = flow_exec_spec_make(TURBO_FLOW_EXEC_CORO_POOL, O);
}
adapter_name(A) ::= dotted_name(T). { A = T; }
adapter_name(A) ::= STRING(T). { A = T; }
dotted_name(A) ::= binding_segment(T). { A = T; }
dotted_name(A) ::= dotted_name(L) DOT(D) binding_segment(R). {
  A = flow_parse_append_dotted_name(ctx, L, D, R);
}
binding_segment(A) ::= IDENT(T). { A = T; }
binding_segment(A) ::= SOURCE(T). { A = T; }
binding_segment(A) ::= STAGE(T). { A = T; }
binding_segment(A) ::= STEP(T). { A = T; }
binding_segment(A) ::= USE(T). { A = T; }
binding_segment(A) ::= IN(T). { A = T; }
binding_segment(A) ::= OUT(T). { A = T; }
binding_segment(A) ::= WORKER(T). { A = T; }
binding_segment(A) ::= EXEC(T). { A = T; }
binding_segment(A) ::= WORKERS(T). { A = T; }
binding_segment(A) ::= LANES(T). { A = T; }
binding_segment(A) ::= POOL(T). { A = T; }
binding_segment(A) ::= ADAPTER(T). { A = T; }
binding_segment(A) ::= OPERATION(T). { A = T; }
binding_segment(A) ::= RESOURCE(T). { A = T; }
binding_segment(A) ::= INLINE(T). { A = T; }
binding_segment(A) ::= THREAD(T). { A = T; }
binding_segment(A) ::= CORO(T). { A = T; }
binding_segment(A) ::= ROUTE(T). { A = T; }
binding_segment(A) ::= WHEN(T). { A = T; }
binding_segment(A) ::= REJECT(T). { A = T; }
binding_segment(A) ::= RETRY(T). { A = T; }
binding_segment(A) ::= ATTEMPTS(T). { A = T; }
binding_segment(A) ::= DELAY(T). { A = T; }
binding_segment(A) ::= REORDER(T). { A = T; }
binding_segment(A) ::= CAPACITY(T). { A = T; }
binding_segment(A) ::= TIMEOUT(T). { A = T; }

exec_options(A) ::= . {
  A = flow_exec_options_default();
}
exec_options(A) ::= exec_options(B) WORKERS NUMBER(N). {
  A = B;
  flow_parse_set_exec_count(ctx, &A, N, 0);
}
exec_options(A) ::= exec_options(B) LANES NUMBER(N). {
  A = B;
  flow_parse_set_exec_count(ctx, &A, N, 1);
}
exec_options(A) ::= exec_options(B) POOL NUMBER(N). {
  A = B;
  flow_parse_set_exec_count(ctx, &A, N, 2);
}

stage_block ::= stage_block_start NEWLINE stage_lines RBRACE NEWLINE. {
  flow_parse_leave_stage_block(ctx);
}

stage_block_start ::= STAGE IDENT(N) LBRACE. {
  flow_parse_enter_stage_block(ctx, N);
}

stage_lines ::= .
stage_lines ::= stage_lines stage_line.

stage_line ::= step_decl NEWLINE.
stage_line ::= use_stmt NEWLINE.
stage_line ::= port_decl NEWLINE.
stage_line ::= edge_stmt NEWLINE.
stage_line ::= route_stmt NEWLINE.
stage_line ::= reject_stmt NEWLINE.
stage_line ::= source_decl NEWLINE.
stage_line ::= NEWLINE.

port_decl ::= IN IDENT(N). {
  flow_parse_add_port(ctx, N, 0);
}
port_decl ::= OUT IDENT(N). {
  flow_parse_add_port(ctx, N, 1);
}

use_stmt ::= USE IDENT(A) EQUAL IDENT(T). {
  flow_parse_use_stage(ctx, A, T);
}

edge_stmt ::= path.

route_stmt ::= ROUTE node(F) ARROW(AR) node(T) WHEN EXPR(E). {
  flow_parse_add_conditional_edge(ctx, F, T, AR, E);
}

reject_stmt ::= REJECT IDENT(N) node(F) ARROW(AR) node(T). {
  flow_parse_add_reject_edge(ctx, N, F, T, AR);
}

path(A) ::= term(T). {
  A = T;
}
path(A) ::= path(P) ARROW(AR) term(T). {
  flow_parse_add_edges(ctx, P, T, AR);
  A = T;
}

term(A) ::= node(N). {
  A = N;
}
term(A) ::= LBRACKET node_list(N) RBRACKET. {
  A = N;
}

node_list(A) ::= node(N). {
  A = N;
}
node_list(A) ::= node_list(L) COMMA node(N). {
  A = flow_parse_node_list_append(L, N);
}

node(A) ::= IDENT(N). {
  A = flow_parse_node(ctx, N);
}
node(A) ::= IDENT(F) DOT IDENT(S). {
  A = flow_parse_qualified_node(ctx, F, S);
}

%syntax_error {
  flow_parse_syntax_error(ctx, TOKEN);
}

%parse_failure {
  flow_token_t token;
  memset(&token, 0, sizeof(token));
  flow_parse_syntax_error(ctx, token);
}
