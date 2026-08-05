// re2c $INPUT -o $OUTPUT
#include "flowie_control_acl_internal.h"
#include "flowie_control_acl_grammar_gen.h"

#include <string.h>

static int flowie_control_acl_keyword(const char *value, size_t length) {
#define FLOWIE_ACL_KEYWORD(text, token)                                                            \
  if (length == sizeof(text) - 1u && memcmp(value, text, sizeof(text) - 1u) == 0) return token
  FLOWIE_ACL_KEYWORD("user", FLOWIE_CONTROL_ACL_TOKEN_USER);
  FLOWIE_ACL_KEYWORD("allow", FLOWIE_CONTROL_ACL_TOKEN_ALLOW);
  FLOWIE_ACL_KEYWORD("deny", FLOWIE_CONTROL_ACL_TOKEN_DENY);
  FLOWIE_ACL_KEYWORD("read", FLOWIE_CONTROL_ACL_TOKEN_READ);
  FLOWIE_ACL_KEYWORD("write", FLOWIE_CONTROL_ACL_TOKEN_WRITE);
  FLOWIE_ACL_KEYWORD("readwrite", FLOWIE_CONTROL_ACL_TOKEN_READWRITE);
  FLOWIE_ACL_KEYWORD("topic", FLOWIE_CONTROL_ACL_TOKEN_TOPIC);
#undef FLOWIE_ACL_KEYWORD
  return FLOWIE_CONTROL_ACL_TOKEN_IDENT;
}

void flowie_control_acl_lexer_init(flowie_control_acl_lexer_t *lexer, const char *text,
                                   size_t text_size) {
  if (!lexer) return;
  memset(lexer, 0, sizeof(*lexer));
  lexer->cursor = text;
  lexer->limit = text ? text + text_size : text;
  lexer->line = 1u;
  lexer->column = 1u;
}

int flowie_control_acl_lexer_next(flowie_control_acl_lexer_t *lexer,
                                  flowie_control_acl_token_t *token) {
  const char *YYCURSOR;
  const char *YYLIMIT;
  const char *start;
  if (!lexer || !token || !lexer->cursor || !lexer->limit) return FLOWIE_CONTROL_ACL_LEX_ERROR;
  memset(token, 0, sizeof(*token));
  YYCURSOR = lexer->cursor;
  YYLIMIT = lexer->limit;

default_lex:
  start = YYCURSOR;
  token->value = start;
  token->line = lexer->line;
  token->column = lexer->column;
  if (lexer->mode == FLOWIE_CONTROL_ACL_LEX_TOPIC) goto topic_lex;
  /*!re2c
    re2c:define:YYCTYPE = "unsigned char";
    re2c:yyfill:enable = 0;
    re2c:eof = 0;
    ident = [A-Za-z0-9_.:@~-]+;
    hspace = [ \t]+;
    newline = "\r\n" | "\n" | "\r";
    $ { lexer->cursor = YYCURSOR; return FLOWIE_CONTROL_ACL_LEX_END; }
    hspace { lexer->column += (uint32_t)(YYCURSOR - start); goto default_lex; }
    newline { lexer->line++; lexer->column = 1u; goto default_lex; }
    "{" { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return FLOWIE_CONTROL_ACL_TOKEN_LBRACE; }
    "}" { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return FLOWIE_CONTROL_ACL_TOKEN_RBRACE; }
    ident {
      int id;
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length;
      id = flowie_control_acl_keyword(token->value, token->length);
      if (id == FLOWIE_CONTROL_ACL_TOKEN_TOPIC) {
        lexer->mode = FLOWIE_CONTROL_ACL_LEX_TOPIC;
        lexer->topic_started = 0;
        lexer->topic_brace_depth = 0u;
      }
      return id;
    }
    * { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return FLOWIE_CONTROL_ACL_LEX_ERROR; }
  */

topic_lex:
  start = YYCURSOR;
  token->value = start;
  token->line = lexer->line;
  token->column = lexer->column;
  /*!re2c
    re2c:define:YYCTYPE = "unsigned char";
    re2c:yyfill:enable = 0;
    re2c:eof = 0;
    segment = [A-Za-z0-9_.:@~-]+;
    topic_hspace = [ \t]+;
    topic_newline = "\r\n" | "\n" | "\r";
    $ {
      lexer->cursor = YYCURSOR;
      lexer->mode = FLOWIE_CONTROL_ACL_LEX_DEFAULT;
      return lexer->topic_brace_depth == 0u ? FLOWIE_CONTROL_ACL_LEX_END
                                            : FLOWIE_CONTROL_ACL_LEX_ERROR;
    }
    topic_hspace {
      lexer->column += (uint32_t)(YYCURSOR - start);
      if (lexer->topic_started && lexer->topic_brace_depth == 0u)
        lexer->mode = FLOWIE_CONTROL_ACL_LEX_DEFAULT;
      goto default_lex;
    }
    topic_newline {
      lexer->line++;
      lexer->column = 1u;
      if (lexer->topic_started && lexer->topic_brace_depth == 0u)
        lexer->mode = FLOWIE_CONTROL_ACL_LEX_DEFAULT;
      goto default_lex;
    }
    "/" { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; lexer->topic_started = 1; return FLOWIE_CONTROL_ACL_TOKEN_SLASH; }
    "," { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return FLOWIE_CONTROL_ACL_TOKEN_COMMA; }
    "{" { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; lexer->topic_brace_depth++; return FLOWIE_CONTROL_ACL_TOKEN_LBRACE; }
    "}" {
      if (lexer->topic_brace_depth == 0u) {
        lexer->cursor = start;
        lexer->mode = FLOWIE_CONTROL_ACL_LEX_DEFAULT;
        YYCURSOR = start;
        goto default_lex;
      }
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column++;
      lexer->topic_brace_depth--;
      return FLOWIE_CONTROL_ACL_TOKEN_RBRACE;
    }
    "%u" { token->length = 2u; lexer->cursor = YYCURSOR; lexer->column += 2u; lexer->topic_started = 1; return FLOWIE_CONTROL_ACL_TOKEN_USER_REF; }
    "%c" { token->length = 2u; lexer->cursor = YYCURSOR; lexer->column += 2u; lexer->topic_started = 1; return FLOWIE_CONTROL_ACL_TOKEN_CLIENT_REF; }
    "+" { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; lexer->topic_started = 1; return FLOWIE_CONTROL_ACL_TOKEN_PLUS; }
    "#" { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; lexer->topic_started = 1; return FLOWIE_CONTROL_ACL_TOKEN_HASH; }
    segment {
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length;
      lexer->topic_started = 1;
      if (token->length == sizeof("groups") - 1u &&
          memcmp(token->value, "groups", sizeof("groups") - 1u) == 0)
        return FLOWIE_CONTROL_ACL_TOKEN_GROUPS;
      if (token->length == sizeof("devices") - 1u &&
          memcmp(token->value, "devices", sizeof("devices") - 1u) == 0)
        return FLOWIE_CONTROL_ACL_TOKEN_DEVICES;
      return FLOWIE_CONTROL_ACL_TOKEN_IDENT;
    }
    * { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return FLOWIE_CONTROL_ACL_LEX_ERROR; }
  */
}
