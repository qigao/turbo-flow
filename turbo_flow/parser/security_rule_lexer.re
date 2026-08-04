// re2c $INPUT -o $OUTPUT
#include "turbo_flow_security.h"

#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

typedef struct flow_security_rule_span_s {
  const uint8_t *data;
  size_t size;
} flow_security_rule_span_t;

typedef enum flow_security_rule_token_e {
  FLOW_SECURITY_RULE_TOKEN_INVALID = 0,
  FLOW_SECURITY_RULE_TOKEN_ALLOW,
  FLOW_SECURITY_RULE_TOKEN_DENY,
  FLOW_SECURITY_RULE_TOKEN_ANY,
  FLOW_SECURITY_RULE_TOKEN_PRINCIPAL,
  FLOW_SECURITY_RULE_TOKEN_ROLE,
  FLOW_SECURITY_RULE_TOKEN_GROUP,
  FLOW_SECURITY_RULE_TOKEN_CONNECT,
  FLOW_SECURITY_RULE_TOKEN_PUBLISH,
  FLOW_SECURITY_RULE_TOKEN_SUBSCRIBE,
  FLOW_SECURITY_RULE_TOKEN_READ,
  FLOW_SECURITY_RULE_TOKEN_WRITE,
  FLOW_SECURITY_RULE_TOKEN_EXECUTE,
  FLOW_SECURITY_RULE_TOKEN_ADMIN,
  FLOW_SECURITY_RULE_TOKEN_GENERIC,
  FLOW_SECURITY_RULE_TOKEN_MQTT_TOPIC,
  FLOW_SECURITY_RULE_TOKEN_FLOW_RESOURCE,
  FLOW_SECURITY_RULE_TOKEN_SQL_OBJECT,
  FLOW_SECURITY_RULE_TOKEN_SECRET,
  FLOW_SECURITY_RULE_TOKEN_EXACT,
  FLOW_SECURITY_RULE_TOKEN_PREFIX,
  FLOW_SECURITY_RULE_TOKEN_ADAPTER
} flow_security_rule_token_t;

static flow_security_rule_token_t flow_security_rule_keyword(flow_security_rule_span_t span) {
  const uint8_t *YYCURSOR = span.data;
  const uint8_t *YYLIMIT = span.data + span.size;
  const uint8_t *YYMARKER;
  /*!re2c
    re2c:define:YYCTYPE = "uint8_t";
    re2c:yyfill:enable = 0;
    re2c:eof = 0;
    "allow"         { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_ALLOW : 0; }
    "deny"          { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_DENY : 0; }
    "any"           { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_ANY : 0; }
    "principal"     { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_PRINCIPAL : 0; }
    "role"          { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_ROLE : 0; }
    "group"         { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_GROUP : 0; }
    "connect"       { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_CONNECT : 0; }
    "publish"       { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_PUBLISH : 0; }
    "subscribe"     { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_SUBSCRIBE : 0; }
    "read"          { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_READ : 0; }
    "write"         { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_WRITE : 0; }
    "execute"       { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_EXECUTE : 0; }
    "admin"         { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_ADMIN : 0; }
    "generic"       { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_GENERIC : 0; }
    "mqtt_topic"    { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_MQTT_TOPIC : 0; }
    "flow_resource" { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_FLOW_RESOURCE : 0; }
    "sql_object"    { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_SQL_OBJECT : 0; }
    "secret"        { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_SECRET : 0; }
    "exact"         { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_EXACT : 0; }
    "prefix"        { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_PREFIX : 0; }
    "adapter"       { return YYCURSOR == YYLIMIT ? FLOW_SECURITY_RULE_TOKEN_ADAPTER : 0; }
    $                { return FLOW_SECURITY_RULE_TOKEN_INVALID; }
    *                { return FLOW_SECURITY_RULE_TOKEN_INVALID; }
  */
}

static int flow_security_rule_hex(uint8_t byte) {
  if (byte >= '0' && byte <= '9') return byte - '0';
  if (byte >= 'a' && byte <= 'f') return byte - 'a' + 10;
  if (byte >= 'A' && byte <= 'F') return byte - 'A' + 10;
  return -1;
}

static int flow_security_rule_unescape(flow_security_rule_span_t span, char *output,
                                       size_t capacity, int wildcard_empty) {
  size_t read = 0u;
  size_t written = 0u;
  if (!output || capacity == 0u) return TURBO_EINVAL;
  if (wildcard_empty && span.size == 1u && span.data[0] == '*') {
    output[0] = '\0';
    return TURBO_OK;
  }
  while (read < span.size) {
    uint8_t byte = span.data[read++];
    if (byte == '\\') {
      int high;
      int low;
      if (read >= span.size) return TURBO_EPROTO;
      byte = span.data[read++];
      if (byte == '\\' || byte == '|') {
        /* Canonical single-byte escape. */
      } else if (byte == 'x') {
        if (read + 2u > span.size || (high = flow_security_rule_hex(span.data[read])) < 0 ||
            (low = flow_security_rule_hex(span.data[read + 1u])) < 0)
          return TURBO_EPROTO;
        byte = (uint8_t)((high << 4) | low);
        read += 2u;
      } else {
        return TURBO_EPROTO;
      }
    }
    if (byte == 0u || written + 1u >= capacity) return TURBO_EPROTO;
    output[written++] = (char)byte;
  }
  if (written == 0u) return TURBO_EPROTO;
  output[written] = '\0';
  return TURBO_OK;
}

static int flow_security_rule_split(const uint8_t *line, size_t size,
                                    flow_security_rule_span_t fields[8]) {
  size_t field = 0u;
  size_t start = 0u;
  size_t cursor = 0u;
  while (cursor < size) {
    if (line[cursor] == '\\') {
      if (++cursor >= size) return TURBO_EPROTO;
      if (line[cursor] == 'x') {
        if (cursor + 2u >= size) return TURBO_EPROTO;
        cursor += 3u;
      } else {
        ++cursor;
      }
      continue;
    }
    if (line[cursor] == '|') {
      if (field >= 7u) return TURBO_EPROTO;
      fields[field++] = (flow_security_rule_span_t){line + start, cursor - start};
      start = ++cursor;
      continue;
    }
    ++cursor;
  }
  if (field != 7u) return TURBO_EPROTO;
  fields[field] = (flow_security_rule_span_t){line + start, size - start};
  return TURBO_OK;
}

static int flow_security_rule_actions(flow_security_rule_span_t span, uint32_t *mask_out) {
  size_t start = 0u;
  uint32_t mask = 0u;
  if (!mask_out || span.size == 0u) return TURBO_EPROTO;
  while (start < span.size) {
    size_t end = start;
    uint32_t action = 0u;
    flow_security_rule_token_t token;
    while (end < span.size && span.data[end] != ',') ++end;
    if (end == start) return TURBO_EPROTO;
    token = flow_security_rule_keyword(
        (flow_security_rule_span_t){span.data + start, end - start});
    switch (token) {
    case FLOW_SECURITY_RULE_TOKEN_CONNECT: action = TURBO_FLOW_SECURITY_ACTION_CONNECT; break;
    case FLOW_SECURITY_RULE_TOKEN_PUBLISH: action = TURBO_FLOW_SECURITY_ACTION_PUBLISH; break;
    case FLOW_SECURITY_RULE_TOKEN_SUBSCRIBE: action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE; break;
    case FLOW_SECURITY_RULE_TOKEN_READ: action = TURBO_FLOW_SECURITY_ACTION_READ; break;
    case FLOW_SECURITY_RULE_TOKEN_WRITE: action = TURBO_FLOW_SECURITY_ACTION_WRITE; break;
    case FLOW_SECURITY_RULE_TOKEN_EXECUTE: action = TURBO_FLOW_SECURITY_ACTION_EXECUTE; break;
    case FLOW_SECURITY_RULE_TOKEN_ADMIN: action = TURBO_FLOW_SECURITY_ACTION_ADMIN; break;
    default: return TURBO_EPROTO;
    }
    if ((mask & action) != 0u) return TURBO_EPROTO;
    mask |= action;
    start = end + 1u;
  }
  *mask_out = mask;
  return TURBO_OK;
}

int turbo_flow_security_rule_parse_line(const char *line, size_t line_size,
                                        turbo_flow_security_rule_t *rule_out) {
  flow_security_rule_span_t fields[8];
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  flow_security_rule_token_t token;
  int rc;
  if (!rule_out || rule_out->size < sizeof(*rule_out) || !line || line_size == 0u ||
      line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX)
    return TURBO_EINVAL;
  while (line_size > 0u && (line[line_size - 1u] == '\n' || line[line_size - 1u] == '\r'))
    --line_size;
  if (line_size == 0u) return TURBO_EPROTO;
  rc = flow_security_rule_split((const uint8_t *)line, line_size, fields);
  if (rc != TURBO_OK) return rc;

  token = flow_security_rule_keyword(fields[0]);
  if (token == FLOW_SECURITY_RULE_TOKEN_ALLOW) rule.effect = TURBO_FLOW_SECURITY_ALLOW;
  else if (token != FLOW_SECURITY_RULE_TOKEN_DENY) return TURBO_EPROTO;

  token = flow_security_rule_keyword(fields[1]);
  if (token == FLOW_SECURITY_RULE_TOKEN_ANY) rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ANY;
  else if (token == FLOW_SECURITY_RULE_TOKEN_PRINCIPAL)
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
  else if (token == FLOW_SECURITY_RULE_TOKEN_ROLE)
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
  else if (token == FLOW_SECURITY_RULE_TOKEN_GROUP)
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_GROUP;
  else return TURBO_EPROTO;

  rc = flow_security_rule_unescape(fields[2], rule.subject, sizeof(rule.subject),
                                   rule.subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY);
  if (rc != TURBO_OK ||
      (rule.subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY && fields[2].size != 1u))
    return TURBO_EPROTO;
  rc = flow_security_rule_unescape(fields[3], rule.domain_id,
                                   sizeof(rule.domain_id), 0);
  if (rc != TURBO_OK) return rc;
  rc = flow_security_rule_actions(fields[4], &rule.action_mask);
  if (rc != TURBO_OK) return rc;

  token = flow_security_rule_keyword(fields[5]);
  if (token == FLOW_SECURITY_RULE_TOKEN_GENERIC)
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
  else if (token == FLOW_SECURITY_RULE_TOKEN_MQTT_TOPIC)
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  else if (token == FLOW_SECURITY_RULE_TOKEN_FLOW_RESOURCE)
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_FLOW_RESOURCE;
  else if (token == FLOW_SECURITY_RULE_TOKEN_SQL_OBJECT)
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_SQL_OBJECT;
  else if (token == FLOW_SECURITY_RULE_TOKEN_SECRET)
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_SECRET;
  else return TURBO_EPROTO;

  token = flow_security_rule_keyword(fields[6]);
  if (token == FLOW_SECURITY_RULE_TOKEN_EXACT) rule.match_kind = TURBO_FLOW_SECURITY_MATCH_EXACT;
  else if (token == FLOW_SECURITY_RULE_TOKEN_PREFIX)
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
  else if (token == FLOW_SECURITY_RULE_TOKEN_ADAPTER)
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
  else return TURBO_EPROTO;
  rc = flow_security_rule_unescape(fields[7], rule.pattern, sizeof(rule.pattern), 0);
  if (rc != TURBO_OK) return rc;
  *rule_out = rule;
  return TURBO_OK;
}

static int flow_security_rule_append(char *output, size_t capacity, size_t *offset,
                                     const char *text) {
  size_t size;
  if (!output || !offset || !text) return TURBO_EINVAL;
  size = strlen(text);
  if (*offset > capacity || size > capacity - *offset) return TURBO_ENOSPC;
  memcpy(output + *offset, text, size);
  *offset += size;
  return TURBO_OK;
}

static int flow_security_rule_append_escaped(char *output, size_t capacity, size_t *offset,
                                             const char *text, size_t text_capacity) {
  size_t size;
  if (!output || !offset || !text) return TURBO_EINVAL;
  size = strnlen(text, text_capacity);
  if (size == 0u || size >= text_capacity) return TURBO_EPROTO;
  for (size_t i = 0u; i < size; ++i) {
    char escaped[3] = {'\\', text[i], '\0'};
    if (text[i] == '|' || text[i] == '\\') {
      int rc = flow_security_rule_append(output, capacity, offset, escaped);
      if (rc != TURBO_OK) return rc;
    } else {
      char single[2] = {text[i], '\0'};
      int rc = flow_security_rule_append(output, capacity, offset, single);
      if (rc != TURBO_OK) return rc;
    }
  }
  return TURBO_OK;
}

static const char *flow_security_rule_action_name(size_t index) {
  static const char *const names[] = {"connect", "publish", "subscribe", "read",
                                      "write", "execute", "admin"};
  return index < sizeof(names) / sizeof(names[0]) ? names[index] : NULL;
}

int turbo_flow_security_rule_format_line(const turbo_flow_security_rule_t *rule, char *line_out,
                                         size_t line_capacity, size_t *line_size_out) {
  static const char *const subject_names[] = {"any", "principal", "role", "group"};
  static const char *const resource_names[] = {"generic", "mqtt_topic", "flow_resource",
                                               "sql_object", "secret"};
  static const char *const match_names[] = {"exact", "prefix", "adapter"};
  size_t offset = 0u;
  int rc;
  if (line_size_out) *line_size_out = 0u;
  if (!rule || rule->size < sizeof(*rule) || rule->abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      !line_out || !line_size_out || line_capacity == 0u ||
      rule->effect > TURBO_FLOW_SECURITY_ALLOW || rule->subject_kind >= 4u ||
      rule->resource_type < TURBO_FLOW_SECURITY_RESOURCE_GENERIC ||
      rule->resource_type > TURBO_FLOW_SECURITY_RESOURCE_SECRET || rule->match_kind >= 3u ||
      rule->action_mask == 0u || (rule->action_mask & ~TURBO_FLOW_SECURITY_ACTION_ALL) != 0u)
    return TURBO_EINVAL;
  rc = flow_security_rule_append(line_out, line_capacity,
                                 &offset, rule->effect == TURBO_FLOW_SECURITY_ALLOW ? "allow|"
                                                                                      : "deny|");
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset,
                                                      subject_names[rule->subject_kind]);
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset, "|");
  if (rc == TURBO_OK) {
    rc = rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY
             ? flow_security_rule_append(line_out, line_capacity, &offset, "*")
             : flow_security_rule_append_escaped(line_out, line_capacity, &offset, rule->subject,
                                                 sizeof(rule->subject));
  }
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset, "|");
  if (rc == TURBO_OK)
    rc = flow_security_rule_append_escaped(line_out, line_capacity, &offset, rule->domain_id,
                                           sizeof(rule->domain_id));
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset, "|");
  for (size_t i = 0u; rc == TURBO_OK && i < 7u; ++i) {
    if ((rule->action_mask & (UINT32_C(1) << i)) == 0u) continue;
    if (offset != 0u && line_out[offset - 1u] != '|')
      rc = flow_security_rule_append(line_out, line_capacity, &offset, ",");
    if (rc == TURBO_OK)
      rc = flow_security_rule_append(line_out, line_capacity, &offset,
                                     flow_security_rule_action_name(i));
  }
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset, "|");
  if (rc == TURBO_OK)
    rc = flow_security_rule_append(line_out, line_capacity, &offset,
                                   resource_names[rule->resource_type - 1u]);
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset, "|");
  if (rc == TURBO_OK)
    rc = flow_security_rule_append(line_out, line_capacity, &offset,
                                   match_names[rule->match_kind]);
  if (rc == TURBO_OK) rc = flow_security_rule_append(line_out, line_capacity, &offset, "|");
  if (rc == TURBO_OK)
    rc = flow_security_rule_append_escaped(line_out, line_capacity, &offset, rule->pattern,
                                           sizeof(rule->pattern));
  if (rc == TURBO_OK && offset > TURBO_FLOW_SECURITY_RULE_LINE_MAX) rc = TURBO_EFBIG;
  if (rc != TURBO_OK) {
    memset(line_out, 0, line_capacity);
    return rc;
  }
  *line_size_out = offset;
  return TURBO_OK;
}
