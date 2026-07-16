// re2c $INPUT -o $OUTPUT
#include "flow_expr_internal.h"
#include "turbo_flow_expr_grammar_gen.h"

#include <string.h>

static int flow_expr_keyword_token(const char *value, size_t length) {
  if (length == 2 && strncmp(value, "or", 2) == 0) return TURBO_FLOW_EXPR_TOKEN_OR;
  if (length == 3 && strncmp(value, "and", 3) == 0) return TURBO_FLOW_EXPR_TOKEN_AND;
  if (length == 3 && strncmp(value, "not", 3) == 0) return TURBO_FLOW_EXPR_TOKEN_NOT;
  if (length == 4 && strncmp(value, "true", 4) == 0) return TURBO_FLOW_EXPR_TOKEN_TRUE;
  if (length == 4 && strncmp(value, "null", 4) == 0) return TURBO_FLOW_EXPR_TOKEN_NULL_VALUE;
  if (length == 5 && strncmp(value, "false", 5) == 0) return TURBO_FLOW_EXPR_TOKEN_FALSE;
  return TURBO_FLOW_EXPR_TOKEN_IDENT;
}

void flow_expr_lexer_init(flow_expr_lexer_t *lexer,
                          const char *text, size_t len) {
  if (!lexer) return;
  lexer->cursor = text;
  lexer->limit = text + len;
  lexer->line = 1;
  lexer->column = 1;
}

int flow_expr_lexer_next(flow_expr_lexer_t *lexer, flow_expr_token_t *token) {
  const char *YYCURSOR;
  const char *YYMARKER;
  const char *YYLIMIT;
  const char *start;

  if (!lexer || !token) return -1;
  YYCURSOR = lexer->cursor;
  YYLIMIT = lexer->limit;

lex_start:
  start = YYCURSOR;
  token->value = start;
  token->length = 0;
  token->line = lexer->line;
  token->column = lexer->column;

  /*!re2c
    re2c:define:YYCTYPE = "unsigned char";
    re2c:yyfill:enable = 0;
    re2c:eof = 0;

    ident = [A-Za-z_][A-Za-z0-9_]*;
    integer = [0-9]+;
    exponent = [eE][+-]?[0-9]+;
    float = ([0-9]+ "." [0-9]* | "." [0-9]+) exponent? | [0-9]+ exponent;
    string = "\"" ([^"\\\r\n] | "\\" [^\r\n])* "\"";
    space = [ \t]+;
    newline = "\r\n" | "\n" | "\r";

    $ { lexer->cursor = YYCURSOR; return 0; }

    space {
      lexer->column += (uint32_t)(YYCURSOR - start);
      goto lex_start;
    }
    newline {
      lexer->cursor = YYCURSOR;
      lexer->line += 1u;
      lexer->column = 1u;
      return -1;
    }
    "||" {
      token->length = 2; lexer->cursor = YYCURSOR; lexer->column += 2;
      return TURBO_FLOW_EXPR_TOKEN_OR;
    }
    "&&" {
      token->length = 2; lexer->cursor = YYCURSOR; lexer->column += 2;
      return TURBO_FLOW_EXPR_TOKEN_AND;
    }
    "==" {
      token->length = 2; lexer->cursor = YYCURSOR; lexer->column += 2;
      return TURBO_FLOW_EXPR_TOKEN_EQ;
    }
    "!=" {
      token->length = 2; lexer->cursor = YYCURSOR; lexer->column += 2;
      return TURBO_FLOW_EXPR_TOKEN_NE;
    }
    "<=" {
      token->length = 2; lexer->cursor = YYCURSOR; lexer->column += 2;
      return TURBO_FLOW_EXPR_TOKEN_LE;
    }
    ">=" {
      token->length = 2; lexer->cursor = YYCURSOR; lexer->column += 2;
      return TURBO_FLOW_EXPR_TOKEN_GE;
    }
    "!" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_NOT; }
    "<" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_LT; }
    ">" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_GT; }
    "+" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_PLUS; }
    "-" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_MINUS; }
    "*" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_MUL; }
    "/" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_DIV; }
    "%" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_MOD; }
    "." { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_DOT; }
    "(" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_LPAREN; }
    ")" { token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1; return TURBO_FLOW_EXPR_TOKEN_RPAREN; }
    float {
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR; lexer->column += (uint32_t)token->length;
      return TURBO_FLOW_EXPR_TOKEN_FLOAT;
    }
    integer {
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR; lexer->column += (uint32_t)token->length;
      return TURBO_FLOW_EXPR_TOKEN_INTEGER;
    }
    string {
      token->value = start + 1;
      token->length = (size_t)(YYCURSOR - start - 2);
      token->column += 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)(YYCURSOR - start);
      return TURBO_FLOW_EXPR_TOKEN_STRING;
    }
    ident {
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR; lexer->column += (uint32_t)token->length;
      return flow_expr_keyword_token(token->value, token->length);
    }
    * {
      token->length = 1; lexer->cursor = YYCURSOR; lexer->column += 1;
      return -1;
    }
  */
}
