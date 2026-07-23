// re2c $INPUT -o $OUTPUT
#include "flowie_mqtt_grammar_gen.h"
#include "flowie_mqtt_internal.h"
#include <turbo_str_view.h>

#include <ctype.h>
#include <string.h>

static int flowie_mqtt_fixed_header_valid(uint8_t first, flowie_mqtt_version_t version) {
  static const uint8_t required_flags[16] = {0xffu, 0x00u, 0x00u, 0xffu, 0x00u, 0x00u,
                                             0x02u, 0x00u, 0x02u, 0x00u, 0x02u, 0x00u,
                                             0x00u, 0x00u, 0x00u, 0x00u};
  uint8_t type = (uint8_t)(first >> 4u);
  uint8_t flags = (uint8_t)(first & 0x0fu);
  if (type == 0u || type > FLOWIE_MQTT_PACKET_AUTH) return 0;
  if (type == FLOWIE_MQTT_PACKET_AUTH && version != FLOWIE_MQTT_VERSION_5) return 0;
  if (type == FLOWIE_MQTT_PACKET_PUBLISH) {
    uint8_t qos = (uint8_t)((flags >> 1u) & 0x03u);
    uint8_t duplicate = (uint8_t)((flags >> 3u) & 0x01u);
    return qos != 0x03u && !(qos == 0u && duplicate);
  }
  return flags == required_flags[type];
}

void flowie_mqtt_lexer_init(flowie_mqtt_lexer_t *lexer, const uint8_t *bytes, size_t byte_count) {
  if (!lexer) return;
  memset(lexer, 0, sizeof(*lexer));
  lexer->input = bytes;
  lexer->cursor = bytes;
  lexer->limit = bytes ? bytes + byte_count : bytes;
}

int flowie_mqtt_lexer_next(flowie_mqtt_lexer_t *lexer, flowie_mqtt_token_t *token,
                           flowie_mqtt_version_t version) {
  const uint8_t *YYCURSOR;
  const uint8_t *YYLIMIT;
  const uint8_t *start;
  uint32_t value;
  uint32_t multiplier;
  size_t count;
  uint8_t byte;
  if (!lexer || !token) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  memset(token, 0, sizeof(*token));
  token->offset = lexer->input && lexer->cursor ? (size_t)(lexer->cursor - lexer->input) : 0u;
  if (lexer->state == FLOWIE_MQTT_LEX_DONE) return 0;
  if (!lexer->cursor || lexer->cursor == lexer->limit) return FLOWIE_MQTT_LEXER_NEED_MORE;

  if (lexer->state == FLOWIE_MQTT_LEX_FIXED_HEADER) {
    YYCURSOR = lexer->cursor;
    YYLIMIT = lexer->limit;
    start = YYCURSOR;
    /*!re2c
      re2c:define:YYCTYPE = "uint8_t";
      re2c:yyfill:enable = 0;
      re2c:eof = 0;
      [\x10-\xff] {
        if (!flowie_mqtt_fixed_header_valid(start[0], version))
          return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
        token->integer = start[0];
        lexer->cursor = YYCURSOR;
        lexer->state = FLOWIE_MQTT_LEX_REMAINING_LENGTH;
        return FLOWIE_MQTT_TOKEN_FIXED_HEADER;
      }
      $ { return FLOWIE_MQTT_LEXER_NEED_MORE; }
      * { return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR; }
    */
  }

  if (lexer->state == FLOWIE_MQTT_LEX_REMAINING_LENGTH) {
    start = lexer->cursor;
    value = 0u;
    multiplier = 1u;
    count = 0u;
    do {
      if (lexer->cursor == lexer->limit) return FLOWIE_MQTT_LEXER_NEED_MORE;
      if (count == 4u) return FLOWIE_MQTT_PARSE_MALFORMED;
      byte = *lexer->cursor++;
      value += (uint32_t)(byte & 0x7fu) * multiplier;
      multiplier *= 128u;
      ++count;
    } while ((byte & 0x80u) != 0u);
    if (count > 1u && byte == 0u) return FLOWIE_MQTT_PARSE_MALFORMED;
    lexer->remaining_length = value;
    lexer->remaining_length_bytes = count;
    lexer->state = value == 0u ? FLOWIE_MQTT_LEX_DONE : FLOWIE_MQTT_LEX_BODY;
    token->integer = value;
    token->span.data = start;
    token->span.size = count;
    return FLOWIE_MQTT_TOKEN_REMAINING_LENGTH;
  }

  if (lexer->state == FLOWIE_MQTT_LEX_BODY) {
    size_t available = (size_t)(lexer->limit - lexer->cursor);
    if (available < (size_t)lexer->remaining_length) return FLOWIE_MQTT_LEXER_NEED_MORE;
    token->span.data = lexer->cursor;
    token->span.size = (size_t)lexer->remaining_length;
    lexer->cursor += lexer->remaining_length;
    lexer->state = FLOWIE_MQTT_LEX_DONE;
    return FLOWIE_MQTT_TOKEN_BODY;
  }

  return FLOWIE_MQTT_PARSE_MALFORMED;
}

static int flowie_mqtt_noncharacter(uint32_t codepoint) {
  return (codepoint >= 0xfdd0u && codepoint <= 0xfdefu) || ((codepoint & 0xfffeu) == 0xfffeu);
}

int flowie_mqtt_utf8_validate(flowie_mqtt_span_t value) {
  tstr_v rest;
  if (!value.data && value.size != 0u) return 0;
  rest = tstr_v_from_buf((const char *)value.data, value.size);
  while (rest.len != 0u) {
    uint32_t codepoint;
    if (!tstr_v_utf8_next(&rest, &codepoint) || codepoint == 0u ||
        flowie_mqtt_noncharacter(codepoint))
      return 0;
  }
  return 1;
}

static int flowie_mqtt_span_equal(flowie_mqtt_span_t left, flowie_mqtt_span_t right) {
  return left.size == right.size &&
         (left.size == 0u || memcmp(left.data, right.data, left.size) == 0);
}

int flowie_mqtt_topic_name_validate(flowie_mqtt_span_t topic) {
  size_t i;
  if (!topic.data || topic.size == 0u || topic.size > FLOWIE_MQTT_MAX_UTF8_SIZE ||
      !flowie_mqtt_utf8_validate(topic))
    return 0;
  for (i = 0u; i < topic.size; ++i) {
    if (topic.data[i] == '+' || topic.data[i] == '#') return 0;
  }
  return 1;
}

static int flowie_mqtt_shared_filter(flowie_mqtt_span_t filter, flowie_mqtt_span_t *effective) {
  static const uint8_t prefix[] = "$share/";
  size_t group_end;
  if (filter.size < sizeof(prefix) - 1u || memcmp(filter.data, prefix, sizeof(prefix) - 1u) != 0) {
    *effective = filter;
    return 1;
  }
  group_end = sizeof(prefix) - 1u;
  while (group_end < filter.size && filter.data[group_end] != '/') {
    if (filter.data[group_end] == '+' || filter.data[group_end] == '#') return 0;
    ++group_end;
  }
  if (group_end == sizeof(prefix) - 1u || group_end == filter.size || group_end + 1u == filter.size)
    return 0;
  effective->data = filter.data + group_end + 1u;
  effective->size = filter.size - group_end - 1u;
  return 1;
}

int flowie_mqtt_topic_filter_validate(flowie_mqtt_span_t filter) {
  flowie_mqtt_span_t effective;
  size_t i;
  size_t level_start = 0u;
  if (!filter.data || filter.size == 0u || filter.size > FLOWIE_MQTT_MAX_UTF8_SIZE ||
      !flowie_mqtt_utf8_validate(filter) || !flowie_mqtt_shared_filter(filter, &effective))
    return 0;
  for (i = 0u; i < effective.size; ++i) {
    uint8_t ch = effective.data[i];
    if (ch == '/') {
      level_start = i + 1u;
      continue;
    }
    if (ch == '#') {
      if (i != level_start || i + 1u != effective.size) return 0;
    } else if (ch == '+') {
      if (i != level_start || (i + 1u != effective.size && effective.data[i + 1u] != '/')) return 0;
    }
  }
  return 1;
}

static void flowie_mqtt_next_level(flowie_mqtt_span_t value, size_t *offset,
                                   flowie_mqtt_span_t *level, int *more) {
  size_t start = *offset;
  size_t end = start;
  while (end < value.size && value.data[end] != '/')
    ++end;
  level->data = value.data + start;
  level->size = end - start;
  *more = end < value.size;
  *offset = *more ? end + 1u : end;
}

int flowie_mqtt_topic_matches(flowie_mqtt_span_t filter, flowie_mqtt_span_t topic,
                              int *matched_out) {
  flowie_mqtt_span_t effective;
  size_t filter_offset = 0u;
  size_t topic_offset = 0u;
  int topic_available = 1;
  if (!matched_out) return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  *matched_out = 0;
  if (!flowie_mqtt_topic_filter_validate(filter) || !flowie_mqtt_topic_name_validate(topic))
    return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (!flowie_mqtt_shared_filter(filter, &effective)) return FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
  if (topic.data[0] == '$' && (effective.data[0] == '+' || effective.data[0] == '#'))
    return FLOWIE_MQTT_PARSE_OK;
  for (;;) {
    flowie_mqtt_span_t filter_level;
    flowie_mqtt_span_t topic_level;
    int filter_more;
    int topic_more;
    flowie_mqtt_next_level(effective, &filter_offset, &filter_level, &filter_more);
    if (filter_level.size == 1u && filter_level.data[0] == '#') {
      *matched_out = 1;
      return FLOWIE_MQTT_PARSE_OK;
    }
    if (!topic_available) return FLOWIE_MQTT_PARSE_OK;
    flowie_mqtt_next_level(topic, &topic_offset, &topic_level, &topic_more);
    if (!(filter_level.size == 1u && filter_level.data[0] == '+') &&
        !flowie_mqtt_span_equal(filter_level, topic_level))
      return FLOWIE_MQTT_PARSE_OK;
    if (!filter_more) {
      *matched_out = !topic_more;
      return FLOWIE_MQTT_PARSE_OK;
    }
    topic_available = topic_more;
  }
}

static int flowie_mqtt_acl_keyword(flowie_mqtt_span_t token, int action, int *value) {
  const uint8_t *YYCURSOR = token.data;
  const uint8_t *YYLIMIT = token.data + token.size;
  const uint8_t *YYMARKER;
  const uint8_t *start = YYCURSOR;
  (void)start;
  /*!re2c
    re2c:define:YYCTYPE = "uint8_t";
    re2c:yyfill:enable = 0;
    "allow" { if (YYCURSOR != YYLIMIT || !action) return 0; *value = FLOWIE_MQTT_ACL_ALLOW; return
    1; } "deny" { if (YYCURSOR != YYLIMIT || !action) return 0; *value = FLOWIE_MQTT_ACL_DENY;
    return 1; } "read" { if (YYCURSOR != YYLIMIT || action) return 0; *value = FLOWIE_MQTT_ACL_READ;
    return 1; } "write" { if (YYCURSOR != YYLIMIT || action) return 0; *value =
    FLOWIE_MQTT_ACL_WRITE; return 1; } "readwrite" { if (YYCURSOR != YYLIMIT || action) return 0;
    *value = FLOWIE_MQTT_ACL_READ_WRITE; return 1; } $ { return 0; }
    * { return 0; }
  */
}

static int flowie_mqtt_acl_subject(flowie_mqtt_span_t token, flowie_mqtt_span_t *out) {
  size_t i;
  if (token.size == 1u && token.data[0] == '*') {
    out->data = NULL;
    out->size = 0u;
    return 1;
  }
  if (token.size == 0u) return 0;
  for (i = 0u; i < token.size; ++i) {
    uint8_t ch = token.data[i];
    if (!(isalnum((unsigned char)ch) || ch == '_' || ch == '-' || ch == '.')) return 0;
  }
  *out = token;
  return 1;
}

int flowie_mqtt_acl_parse_line(const char *line, size_t line_size,
                               flowie_mqtt_acl_rule_view_t *out) {
  flowie_mqtt_acl_rule_view_t parsed = FLOWIE_MQTT_ACL_RULE_VIEW_INIT;
  flowie_mqtt_span_t fields[7];
  const uint8_t *bytes = (const uint8_t *)line;
  size_t start = 0u;
  size_t end = line_size;
  size_t cursor;
  size_t field_count = 0u;
  int value;
  if ((!line && line_size != 0u) || !out || out->size < sizeof(*out))
    return FLOWIE_MQTT_ACL_PARSE_INVALID_ARGUMENT;
  while (start < end && (bytes[start] == ' ' || bytes[start] == '\t'))
    ++start;
  while (end > start && (bytes[end - 1u] == ' ' || bytes[end - 1u] == '\t' ||
                         bytes[end - 1u] == '\r' || bytes[end - 1u] == '\n'))
    --end;
  if (start == end || bytes[start] == '#') return FLOWIE_MQTT_ACL_PARSE_SKIP;
  cursor = start;
  while (field_count < 7u) {
    size_t field_end = cursor;
    while (field_end < end && bytes[field_end] != ':')
      ++field_end;
    fields[field_count].data = bytes + cursor;
    fields[field_count].size = field_end - cursor;
    ++field_count;
    if (field_end == end) break;
    cursor = field_end + 1u;
  }
  if (field_count != 7u || cursor > end || (fields[6].data + fields[6].size != bytes + end))
    return FLOWIE_MQTT_ACL_PARSE_INVALID_FORMAT;
  if (!flowie_mqtt_acl_keyword(fields[0], 1, &value)) return FLOWIE_MQTT_ACL_PARSE_INVALID_ACTION;
  parsed.effect = (flowie_mqtt_acl_effect_t)value;
  if (!flowie_mqtt_acl_keyword(fields[1], 0, &value))
    return FLOWIE_MQTT_ACL_PARSE_INVALID_PERMISSION;
  parsed.permission = (flowie_mqtt_acl_permission_t)value;
  if (!flowie_mqtt_acl_subject(fields[2], &parsed.role) ||
      !flowie_mqtt_acl_subject(fields[3], &parsed.scope) ||
      !flowie_mqtt_acl_subject(fields[4], &parsed.username) ||
      !flowie_mqtt_acl_subject(fields[5], &parsed.client_id))
    return FLOWIE_MQTT_ACL_PARSE_INVALID_SUBJECT;
  if (!flowie_mqtt_topic_filter_validate(fields[6])) return FLOWIE_MQTT_ACL_PARSE_INVALID_TOPIC;
  parsed.topic_filter = fields[6];
  *out = parsed;
  return FLOWIE_MQTT_ACL_PARSE_OK;
}
