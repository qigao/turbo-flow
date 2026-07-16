%name TurboFlowControlParse
%token_prefix TURBO_FLOW_CONTROL_TOKEN_
%token_type {flow_control_token_t}
%default_type {flow_control_token_t}
%stack_size 64

%extra_argument {flow_control_parse_ctx_t *ctx}

%include {
#include "flow_control_internal.h"
#include <string.h>
}

%token FLOW PAUSE RESUME DRAIN TIMEOUT POOL RESIZE THREAD CORO DISRUPTOR.
%token ADAPTER QUIESCE REPLACE HOST PORT PATH IDENT STRING NUMBER DOT NEWLINE.
%token IF WHEN THEN EXPR.

%type name {flow_control_token_t}
%type segment {flow_control_token_t}
%type optional_timeout {flow_control_token_t}
%type optional_path {flow_control_token_t}

%start_symbol start

start ::= command optional_newline.
start ::= IF EXPR(E) THEN command optional_newline. { flow_control_set_condition(ctx, E); }
start ::= WHEN EXPR(E) THEN command optional_newline. { flow_control_set_condition(ctx, E); }
optional_newline ::= .
optional_newline ::= NEWLINE.

command ::= FLOW PAUSE. { flow_control_set_simple(ctx, TURBO_FLOW_CONTROL_PAUSE); }
command ::= FLOW RESUME. { flow_control_set_simple(ctx, TURBO_FLOW_CONTROL_RESUME); }
command ::= FLOW DRAIN TIMEOUT NUMBER(T). { flow_control_set_drain(ctx, T); }
command ::= POOL name(N) THREAD RESIZE NUMBER(P) optional_timeout(T). {
  flow_control_set_resize(ctx, N, TURBO_FLOW_POOL_THREAD, P, T);
}
command ::= POOL name(N) CORO RESIZE NUMBER(P) optional_timeout(T). {
  flow_control_set_resize(ctx, N, TURBO_FLOW_POOL_CORO, P, T);
}
command ::= POOL name(N) DISRUPTOR RESIZE NUMBER(P) optional_timeout(T). {
  flow_control_set_resize(ctx, N, TURBO_FLOW_POOL_DISRUPTOR, P, T);
}
command ::= ADAPTER name(N) QUIESCE. {
  flow_control_set_adapter(ctx, N, TURBO_FLOW_ADAPTER_QUIESCE);
}
command ::= ADAPTER name(N) RESUME. {
  flow_control_set_adapter(ctx, N, TURBO_FLOW_ADAPTER_RESUME);
}
command ::= ADAPTER name(N) REPLACE HOST STRING(H) PORT NUMBER(P) optional_path(R). {
  flow_control_set_replace(ctx, N, H, P, R);
}

optional_timeout(A) ::= . { memset(&A, 0, sizeof(A)); }
optional_timeout(A) ::= TIMEOUT NUMBER(T). { A = T; }
optional_path(A) ::= . { memset(&A, 0, sizeof(A)); }
optional_path(A) ::= PATH STRING(P). { A = P; }

name(A) ::= segment(S). { A = S; }
name(A) ::= name(L) DOT(D) segment(R). {
  A = L;
  if (L.value + L.length != D.value || D.value + D.length != R.value) {
    flow_control_syntax_error(ctx, D);
  } else {
    A.length = (size_t)((R.value + R.length) - L.value);
  }
}
segment(A) ::= IDENT(T). { A = T; }
segment(A) ::= FLOW(T). { A = T; }
segment(A) ::= POOL(T). { A = T; }
segment(A) ::= ADAPTER(T). { A = T; }
segment(A) ::= THREAD(T). { A = T; }
segment(A) ::= CORO(T). { A = T; }
segment(A) ::= RESIZE(T). { A = T; }
segment(A) ::= RESUME(T). { A = T; }
segment(A) ::= DRAIN(T). { A = T; }
segment(A) ::= TIMEOUT(T). { A = T; }
segment(A) ::= PAUSE(T). { A = T; }
segment(A) ::= QUIESCE(T). { A = T; }
segment(A) ::= REPLACE(T). { A = T; }
segment(A) ::= HOST(T). { A = T; }
segment(A) ::= PORT(T). { A = T; }
segment(A) ::= PATH(T). { A = T; }
segment(A) ::= DISRUPTOR(T). { A = T; }
segment(A) ::= IF(T). { A = T; }
segment(A) ::= WHEN(T). { A = T; }
segment(A) ::= THEN(T). { A = T; }

%syntax_error { flow_control_syntax_error(ctx, TOKEN); }
%parse_failure {
  flow_control_token_t token;
  memset(&token, 0, sizeof(token));
  flow_control_syntax_error(ctx, token);
}
