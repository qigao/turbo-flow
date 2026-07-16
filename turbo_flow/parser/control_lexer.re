// re2c $INPUT -o $OUTPUT
#include "flow_control_internal.h"
#include "turbo_flow_control_grammar_gen.h"

#include <string.h>

static int flow_control_keyword(const char *value, size_t length) {
#define KW(text, token) \
  if (length == sizeof(text) - 1u && memcmp(value, text, sizeof(text) - 1u) == 0) return token
  KW("flow", TURBO_FLOW_CONTROL_TOKEN_FLOW);
  KW("pause", TURBO_FLOW_CONTROL_TOKEN_PAUSE);
  KW("resume", TURBO_FLOW_CONTROL_TOKEN_RESUME);
  KW("drain", TURBO_FLOW_CONTROL_TOKEN_DRAIN);
  KW("timeout", TURBO_FLOW_CONTROL_TOKEN_TIMEOUT);
  KW("pool", TURBO_FLOW_CONTROL_TOKEN_POOL);
  KW("resize", TURBO_FLOW_CONTROL_TOKEN_RESIZE);
  KW("thread", TURBO_FLOW_CONTROL_TOKEN_THREAD);
  KW("coro", TURBO_FLOW_CONTROL_TOKEN_CORO);
  KW("disruptor", TURBO_FLOW_CONTROL_TOKEN_DISRUPTOR);
  KW("adapter", TURBO_FLOW_CONTROL_TOKEN_ADAPTER);
  KW("quiesce", TURBO_FLOW_CONTROL_TOKEN_QUIESCE);
  KW("replace", TURBO_FLOW_CONTROL_TOKEN_REPLACE);
  KW("host", TURBO_FLOW_CONTROL_TOKEN_HOST);
  KW("port", TURBO_FLOW_CONTROL_TOKEN_PORT);
  KW("path", TURBO_FLOW_CONTROL_TOKEN_PATH);
  KW("if", TURBO_FLOW_CONTROL_TOKEN_IF);
  KW("when", TURBO_FLOW_CONTROL_TOKEN_WHEN);
  KW("then", TURBO_FLOW_CONTROL_TOKEN_THEN);
#undef KW
  return TURBO_FLOW_CONTROL_TOKEN_IDENT;
}

void flow_control_lexer_init(flow_control_lexer_t *lexer, const char *input, size_t length) {
  if (!lexer) return;
  memset(lexer, 0, sizeof(*lexer));
  lexer->cursor = input;
  lexer->limit = input + length;
  lexer->line = 1u;
  lexer->column = 1u;
}

int flow_control_lexer_next(flow_control_lexer_t *lexer, flow_control_token_t *token) {
  const char *YYCURSOR;
  const char *YYMARKER;
  const char *YYLIMIT;
  const char *start;
  if (!lexer || !token) return -1;
  if (lexer->condition_pending) {
    const char *scan = lexer->cursor;
    const char *end = lexer->limit;
    int quoted = 0;
    int escaped = 0;
    while (scan < end && (*scan == ' ' || *scan == '\t')) ++scan;
    start = scan;
    while (scan < end) {
      if (!quoted && (scan == start || scan[-1] == ' ' || scan[-1] == '\t') &&
          (size_t)(end - scan) >= 4u && memcmp(scan, "then", 4u) == 0 &&
          (scan + 4u == end || scan[4] == ' ' || scan[4] == '\t')) break;
      if (*scan == '\r' || *scan == '\n') break;
      if (escaped) {
        escaped = 0;
      } else if (quoted && *scan == '\\') {
        escaped = 1;
      } else if (*scan == '"') {
        quoted = !quoted;
      }
      ++scan;
    }
    end = scan;
    while (end > start && (end[-1] == ' ' || end[-1] == '\t')) --end;
    token->value = start;
    token->length = (size_t)(end - start);
    token->line = lexer->line;
    token->column = lexer->column + (uint32_t)(start - lexer->cursor);
    lexer->column += (uint32_t)(scan - lexer->cursor);
    lexer->cursor = scan;
    lexer->condition_pending = 0;
    return token->length ? TURBO_FLOW_CONTROL_TOKEN_EXPR : -1;
  }
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
    number = [0-9]+;
    string = "\"" ([^"\\\r\n] | "\\" [^\r\n])* "\"";
    space = [ \t]+;
    newline = "\r\n" | "\n" | "\r";
    $ { lexer->cursor = YYCURSOR; return 0; }
    space { lexer->column += (uint32_t)(YYCURSOR - start); goto lex_start; }
    newline {
      token->length = (size_t)(YYCURSOR - start); lexer->cursor = YYCURSOR;
      lexer->line++; lexer->column = 1u; return TURBO_FLOW_CONTROL_TOKEN_NEWLINE;
    }
    "." { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return TURBO_FLOW_CONTROL_TOKEN_DOT; }
    number {
      token->length = (size_t)(YYCURSOR - start); lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length; return TURBO_FLOW_CONTROL_TOKEN_NUMBER;
    }
    string {
      token->value = start + 1; token->length = (size_t)(YYCURSOR - start - 2);
      token->column++; lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)(YYCURSOR - start); return TURBO_FLOW_CONTROL_TOKEN_STRING;
    }
    ident {
      int id; token->length = (size_t)(YYCURSOR - start); lexer->cursor = YYCURSOR;
      lexer->column += (uint32_t)token->length; id = flow_control_keyword(token->value, token->length);
      if (id == TURBO_FLOW_CONTROL_TOKEN_IF || id == TURBO_FLOW_CONTROL_TOKEN_WHEN) lexer->condition_pending = 1;
      return id;
    }
    * { token->length = 1u; lexer->cursor = YYCURSOR; lexer->column++; return -1; }
  */
}
