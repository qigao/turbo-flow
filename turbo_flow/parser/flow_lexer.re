// re2c $INPUT -o $OUTPUT
#include "flow_parser_internal.h"
#include "turbo_flow_grammar_gen.h"
#include "turbo_simd_scan.h"

#include <string.h>

static int flow_keyword_token(const char *value, size_t length) {
  if (length == 2 && strncmp(value, "in", 2) == 0) return TURBO_FLOW_TOKEN_IN;
  if (length == 3 && strncmp(value, "out", 3) == 0) return TURBO_FLOW_TOKEN_OUT;
  if (length == 4 && strncmp(value, "coro", 4) == 0) return TURBO_FLOW_TOKEN_CORO;
  if (length == 4 && strncmp(value, "exec", 4) == 0) return TURBO_FLOW_TOKEN_EXEC;
  if (length == 3 && strncmp(value, "use", 3) == 0) return TURBO_FLOW_TOKEN_USE;
  if (length == 4 && strncmp(value, "step", 4) == 0) return TURBO_FLOW_TOKEN_STEP;
  if (length == 4 && strncmp(value, "when", 4) == 0) return TURBO_FLOW_TOKEN_WHEN;
  if (length == 4 && strncmp(value, "pool", 4) == 0) return TURBO_FLOW_TOKEN_POOL;
  if (length == 5 && strncmp(value, "lanes", 5) == 0) return TURBO_FLOW_TOKEN_LANES;
  if (length == 5 && strncmp(value, "stage", 5) == 0) return TURBO_FLOW_TOKEN_STAGE;
  if (length == 5 && strncmp(value, "route", 5) == 0) return TURBO_FLOW_TOKEN_ROUTE;
  if (length == 6 && strncmp(value, "reject", 6) == 0) return TURBO_FLOW_TOKEN_REJECT;
  if (length == 5 && strncmp(value, "retry", 5) == 0) return TURBO_FLOW_TOKEN_RETRY;
  if (length == 8 && strncmp(value, "attempts", 8) == 0) return TURBO_FLOW_TOKEN_ATTEMPTS;
  if (length == 5 && strncmp(value, "delay", 5) == 0) return TURBO_FLOW_TOKEN_DELAY;
  if (length == 7 && strncmp(value, "reorder", 7) == 0) return TURBO_FLOW_TOKEN_REORDER;
  if (length == 8 && strncmp(value, "capacity", 8) == 0) return TURBO_FLOW_TOKEN_CAPACITY;
  if (length == 7 && strncmp(value, "timeout", 7) == 0) return TURBO_FLOW_TOKEN_TIMEOUT;
  if (length == 7 && strncmp(value, "adapter", 7) == 0) return TURBO_FLOW_TOKEN_ADAPTER;
  if (length == 9 && strncmp(value, "operation", 9) == 0) return TURBO_FLOW_TOKEN_OPERATION;
  if (length == 8 && strncmp(value, "resource", 8) == 0) return TURBO_FLOW_TOKEN_RESOURCE;
  if (length == 6 && strncmp(value, "inline", 6) == 0) return TURBO_FLOW_TOKEN_INLINE;
  if (length == 6 && strncmp(value, "source", 6) == 0) return TURBO_FLOW_TOKEN_SOURCE;
  if (length == 6 && strncmp(value, "worker", 6) == 0) return TURBO_FLOW_TOKEN_WORKER;
  if (length == 7 && strncmp(value, "workers", 7) == 0) return TURBO_FLOW_TOKEN_WORKERS;
  if (length == 6 && strncmp(value, "thread", 6) == 0) return TURBO_FLOW_TOKEN_THREAD;
  return TURBO_FLOW_TOKEN_IDENT;
}

void flow_lexer_init(flow_lexer_t *lexer, const char *input, size_t length) {
  if (!lexer) return;
  lexer->input = input;
  lexer->cursor = input;
  lexer->limit = input + length;
  lexer->line = TURBO_FLOW_LINE_START;
  lexer->column = TURBO_FLOW_COLUMN_START;
  lexer->expression_pending = 0;
}

int flow_lexer_next(flow_lexer_t *lexer, flow_token_t *token) {
  const char *YYCURSOR;
  const char *YYMARKER;
  const char *YYLIMIT;
  const char *start;

  if (!lexer || !token) return -1;
  if (lexer->expression_pending) {
    const char *expression_start = lexer->cursor;
    const char *expression_end;
    const char *line_end;

    while (expression_start < lexer->limit &&
           (*expression_start == ' ' || *expression_start == '\t')) {
      ++expression_start;
    }
    line_end = expression_start;
    while (line_end < lexer->limit && *line_end != '\r' && *line_end != '\n') ++line_end;
    expression_end = line_end;
    while (expression_end > expression_start &&
           (expression_end[-1] == ' ' || expression_end[-1] == '\t')) {
      --expression_end;
    }
    token->value = expression_start;
    token->length = (size_t)(expression_end - expression_start);
    token->line = lexer->line;
    token->column = lexer->column + (uint32_t)(expression_start - lexer->cursor);
    lexer->column += (uint32_t)(line_end - lexer->cursor);
    lexer->cursor = line_end;
    lexer->expression_pending = 0;
    return token->length > 0 ? TURBO_FLOW_TOKEN_EXPR : -1;
  }
  YYCURSOR = lexer->cursor;
  YYLIMIT = lexer->limit;

lex_start:
  for (;;) {
    const char *next = turbo_scan_skip_sp_tab(YYCURSOR, YYLIMIT);
    if (next != YYCURSOR) {
      lexer->column += (uint32_t)(next - YYCURSOR);
      YYCURSOR = next;
      continue;
    }
    if (YYCURSOR < YYLIMIT && *YYCURSOR == '#') {
      next = turbo_scan_to_any2(YYCURSOR, YYLIMIT, '\r', '\n');
      lexer->column += (uint32_t)(next - YYCURSOR);
      YYCURSOR = next;
      continue;
    }
    if ((size_t)(YYLIMIT - YYCURSOR) >= 2u && YYCURSOR[0] == '%' && YYCURSOR[1] == '%') {
      next = turbo_scan_to_any2(YYCURSOR, YYLIMIT, '\r', '\n');
      lexer->column += (uint32_t)(next - YYCURSOR);
      YYCURSOR = next;
      continue;
    }
    break;
  }

  start = YYCURSOR;
  token->value = start;
  token->length = 0;
  token->line = lexer->line;
  token->column = lexer->column;

  if (YYCURSOR < YYLIMIT && *YYCURSOR == '"') {
    const char *string_end = turbo_scan_to_any3(YYCURSOR + 1, YYLIMIT, '"', '\r', '\n');
    if (string_end < YYLIMIT && *string_end == '"') {
      YYCURSOR = string_end + 1;
      token->value = start + 1;
      token->length = (size_t)(string_end - start - 1);
      token->column += 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)(YYCURSOR - start);
      return TURBO_FLOW_TOKEN_STRING;
    }
  }

  /*!re2c
    re2c:define:YYCTYPE = "unsigned char";
    re2c:yyfill:enable = 0;
    re2c:eof = 0;

    ident = [A-Za-z_][A-Za-z0-9_]*;
    number = [0-9]+;
    string = "\"" [^"\r\n]* "\"";
    space = [ \t]+;
    comment = "#" [^\r\n]* | "%%" [^\r\n]*;
    newline = "\r\n" | "\n" | "\r";

    $ {
      lexer->cursor = YYCURSOR;
      return 0;
    }

    space {
      lexer->column += (uint32_t)(YYCURSOR - start);
      goto lex_start;
    }

    comment {
      lexer->column += (uint32_t)(YYCURSOR - start);
      goto lex_start;
    }

    newline {
      token->value = start;
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR;
      lexer->line += 1u;
      lexer->column = TURBO_FLOW_COLUMN_START;
      return TURBO_FLOW_TOKEN_NEWLINE;
    }

    "-->" | "->" {
      token->value = start;
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length;
      return TURBO_FLOW_TOKEN_ARROW;
    }

    "{" {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_LBRACE;
    }

    "}" {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_RBRACE;
    }

    "[" {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_LBRACKET;
    }

    "]" {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_RBRACKET;
    }

    "," {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_COMMA;
    }

    "." {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_DOT;
    }

    "=" {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return TURBO_FLOW_TOKEN_EQUAL;
    }

    number {
      token->value = start;
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length;
      return TURBO_FLOW_TOKEN_NUMBER;
    }

    string {
      token->value = start + 1;
      token->length = (size_t)(YYCURSOR - start - 2);
      token->column += 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)(YYCURSOR - start);
      return TURBO_FLOW_TOKEN_STRING;
    }

    ident {
      token->value = start;
      token->length = (size_t)(YYCURSOR - start);
      lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length;
      {
        int keyword = flow_keyword_token(token->value, token->length);
        if (keyword == TURBO_FLOW_TOKEN_WHEN) lexer->expression_pending = 1;
        return keyword;
      }
    }

    * {
      token->value = start;
      token->length = 1u;
      lexer->cursor = YYCURSOR;
      lexer->column += 1u;
      return -1;
    }
  */
}
