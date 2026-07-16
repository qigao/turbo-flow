%name FlowieMqttParse
%token_prefix FLOWIE_MQTT_TOKEN_
%token_type {flowie_mqtt_token_t}
%default_type {flowie_mqtt_token_t}
%stack_size 8

%extra_argument {flowie_mqtt_parse_ctx_t *ctx}

%include {
#include "flowie_mqtt_internal.h"
#include <string.h>
}

%token FIXED_HEADER REMAINING_LENGTH BODY.
%type optional_body {flowie_mqtt_token_t}
%start_symbol packet

packet ::= FIXED_HEADER(H) REMAINING_LENGTH(R) optional_body(B). {
  flowie_mqtt_parser_accept(ctx, H, R, B);
}

optional_body(B) ::= . { memset(&B, 0, sizeof(B)); }
optional_body(B) ::= BODY(V). { B = V; }

%syntax_error { flowie_mqtt_parser_fail(ctx, TOKEN); }
%parse_failure {
  flowie_mqtt_token_t token;
  memset(&token, 0, sizeof(token));
  flowie_mqtt_parser_fail(ctx, token);
}
