%name TurboFlowExprParse
%token_prefix TURBO_FLOW_EXPR_TOKEN_
%token_type {flow_expr_token_t}
%default_type {uint32_t}
%stack_size 256

%extra_argument {flow_expr_parse_ctx_t *ctx}

%include {
#include "flow_expr_internal.h"
}

%token IDENT INTEGER FLOAT STRING TRUE FALSE NULL_VALUE.
%token OR AND NOT EQ NE LT LE GT GE PLUS MINUS MUL DIV MOD DOT.
%token HAS_FLAG LPAREN RPAREN COMMA.

%type expression {uint32_t}
%type or_expression {uint32_t}
%type and_expression {uint32_t}
%type equality_expression {uint32_t}
%type relation_expression {uint32_t}
%type additive_expression {uint32_t}
%type multiply_expression {uint32_t}
%type unary_expression {uint32_t}
%type primary_expression {uint32_t}
%type field_reference {uint32_t}
%type literal {uint32_t}

%start_symbol input

input ::= expression(E). { ctx->ast->root = E; }

expression(A) ::= or_expression(E). { A = E; }

or_expression(A) ::= and_expression(E). { A = E; }
or_expression(A) ::= or_expression(L) OR(O) and_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_OR, L, R, O);
}

and_expression(A) ::= equality_expression(E). { A = E; }
and_expression(A) ::= and_expression(L) AND(O) equality_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_AND, L, R, O);
}

equality_expression(A) ::= relation_expression(E). { A = E; }
equality_expression(A) ::= equality_expression(L) EQ(O) relation_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_EQ, L, R, O);
}
equality_expression(A) ::= equality_expression(L) NE(O) relation_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_NE, L, R, O);
}

relation_expression(A) ::= additive_expression(E). { A = E; }
relation_expression(A) ::= relation_expression(L) LT(O) additive_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_LT, L, R, O);
}
relation_expression(A) ::= relation_expression(L) LE(O) additive_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_LE, L, R, O);
}
relation_expression(A) ::= relation_expression(L) GT(O) additive_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_GT, L, R, O);
}
relation_expression(A) ::= relation_expression(L) GE(O) additive_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_GE, L, R, O);
}

additive_expression(A) ::= multiply_expression(E). { A = E; }
additive_expression(A) ::= additive_expression(L) PLUS(O) multiply_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_ADD, L, R, O);
}
additive_expression(A) ::= additive_expression(L) MINUS(O) multiply_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_SUB, L, R, O);
}

multiply_expression(A) ::= unary_expression(E). { A = E; }
multiply_expression(A) ::= multiply_expression(L) MUL(O) unary_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_MUL, L, R, O);
}
multiply_expression(A) ::= multiply_expression(L) DIV(O) unary_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_DIV, L, R, O);
}
multiply_expression(A) ::= multiply_expression(L) MOD(O) unary_expression(R). {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_MOD, L, R, O);
}

unary_expression(A) ::= primary_expression(E). { A = E; }
unary_expression(A) ::= NOT(O) unary_expression(E). {
  A = flow_expr_push_unary(ctx, FLOW_EXPR_NOT, E, O);
}
unary_expression(A) ::= PLUS(O) unary_expression(E). {
  A = flow_expr_push_unary(ctx, FLOW_EXPR_POS, E, O);
}
unary_expression(A) ::= MINUS(O) unary_expression(E). {
  A = flow_expr_push_unary(ctx, FLOW_EXPR_NEG, E, O);
}

primary_expression(A) ::= literal(E). { A = E; }
primary_expression(A) ::= field_reference(E). { A = E; }
primary_expression(A) ::= LPAREN expression(E) RPAREN. { A = E; }
primary_expression(A) ::= HAS_FLAG(O) LPAREN expression(V) COMMA expression(M) RPAREN. {
  A = flow_expr_push_binary(ctx, FLOW_EXPR_HAS_FLAG, V, M, O);
}

field_reference(A) ::= IDENT(I). { A = flow_expr_push_field(ctx, I); }
field_reference(A) ::= field_reference(F) DOT IDENT(I). {
  A = flow_expr_append_field(ctx, F, I);
}

literal(A) ::= NULL_VALUE(T). { A = flow_expr_push_null(ctx, T); }
literal(A) ::= TRUE(T). { A = flow_expr_push_bool(ctx, T, 1); }
literal(A) ::= FALSE(T). { A = flow_expr_push_bool(ctx, T, 0); }
literal(A) ::= INTEGER(T). { A = flow_expr_push_i64(ctx, T); }
literal(A) ::= FLOAT(T). { A = flow_expr_push_f64(ctx, T); }
literal(A) ::= STRING(T). { A = flow_expr_push_string(ctx, T); }

%syntax_error { flow_expr_syntax_error(ctx, TOKEN); }
%parse_failure {
  flow_expr_token_t token = {0};
  flow_expr_syntax_error(ctx, token);
}
