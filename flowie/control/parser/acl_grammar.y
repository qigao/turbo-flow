%name FlowieControlAclParse
%token_prefix FLOWIE_CONTROL_ACL_TOKEN_
%token_type {flowie_control_acl_token_t}
%default_type {flowie_control_acl_token_t}
%stack_size 128

%extra_argument {flowie_control_acl_parse_ctx_t *ctx}

%include {
#include "flowie_control_acl_internal.h"
}

%token USER ALLOW DENY READ WRITE READWRITE TOPIC IDENT.
%token LBRACE RBRACE SLASH COMMA GROUPS DEVICES USER_REF CLIENT_REF PLUS HASH.

%type connection_effect {turbo_flow_security_effect_t}
%type entry_effect {turbo_flow_security_effect_t}
%type access_mode {uint32_t}
%type group_path {flowie_control_acl_group_path_t}
%type alternatives {flowie_control_acl_token_list_t}
%type topic_pattern {flowie_control_acl_topic_parse_t}
%type device {flowie_control_acl_token_t}
%type leaf {flowie_control_acl_token_t}

%start_symbol document

document ::= USER IDENT(U) connection_effect(E) optional_entries. {
  flowie_control_acl_parse_accept(ctx, U, E);
}

connection_effect(E) ::= ALLOW. { E = TURBO_FLOW_SECURITY_ALLOW; }
connection_effect(E) ::= DENY. { E = TURBO_FLOW_SECURITY_DENY; }

optional_entries ::= .
optional_entries ::= LBRACE entries RBRACE.

entries ::= entry.
entries ::= entries entry.

entry ::= entry_effect(E) access_mode(A) TOPIC topic_pattern(T). {
  flowie_control_acl_entry_add(ctx, E, A, T);
}

entry_effect(E) ::= . { E = TURBO_FLOW_SECURITY_ALLOW; }
entry_effect(E) ::= ALLOW. { E = TURBO_FLOW_SECURITY_ALLOW; }
entry_effect(E) ::= DENY. { E = TURBO_FLOW_SECURITY_DENY; }

access_mode(A) ::= READ. { A = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE; }
access_mode(A) ::= WRITE. { A = TURBO_FLOW_SECURITY_ACTION_PUBLISH; }
access_mode(A) ::= READWRITE. {
  A = TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
}

topic_pattern(T) ::= IDENT(D) SLASH GROUPS SLASH group_path(G) SLASH DEVICES SLASH device(V)
                     SLASH leaf(L). {
  T = flowie_control_acl_topic_build(ctx, D, G, V, L);
}
topic_pattern(T) ::= IDENT(D) SLASH GROUPS SLASH group_path(G) SLASH DEVICES SLASH device(V)
                     SLASH LBRACE(O) alternatives(A) RBRACE(C). {
  T = flowie_control_acl_topic_build_alternatives(ctx, D, G, V, O, A, C);
}

group_path(G) ::= IDENT(I). { G = flowie_control_acl_group_path_start(ctx, I); }
group_path(G) ::= group_path(P) SLASH IDENT(I). {
  G = flowie_control_acl_group_path_append(ctx, P, I);
}

device(V) ::= IDENT(I). { V = I; }
device(V) ::= USER_REF(I). { V = I; }
device(V) ::= CLIENT_REF(I). { V = I; }
device(V) ::= PLUS(I). { V = I; }

leaf(V) ::= IDENT(I). { V = I; }
leaf(V) ::= PLUS(I). { V = I; }
leaf(V) ::= HASH(I). { V = I; }

alternatives(A) ::= IDENT(I) COMMA IDENT(J). {
  A = flowie_control_acl_token_list_start(ctx, I);
  A = flowie_control_acl_token_list_append(ctx, A, J);
}
alternatives(A) ::= alternatives(L) COMMA IDENT(I). {
  A = flowie_control_acl_token_list_append(ctx, L, I);
}

%syntax_error { flowie_control_acl_parse_fail(ctx); }
%parse_failure { flowie_control_acl_parse_fail(ctx); }
%stack_overflow { flowie_control_acl_parse_fail(ctx); }
