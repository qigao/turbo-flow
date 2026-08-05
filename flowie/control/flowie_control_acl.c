#include "flowie_control_acl_internal.h"
#include "flowie_control_acl_grammar_gen.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *FlowieControlAclParseAlloc(void *(*malloc_proc)(size_t));
void FlowieControlAclParse(void *parser, int token_id, flowie_control_acl_token_t token,
                           flowie_control_acl_parse_ctx_t *ctx);
void FlowieControlAclParseFree(void *parser, void (*free_proc)(void *));

static void flowie_control_acl_set_error(flowie_control_acl_parse_ctx_t *ctx, int status) {
  if (ctx && ctx->status == TURBO_OK) ctx->status = status;
}

void flowie_control_acl_parse_fail(flowie_control_acl_parse_ctx_t *ctx) {
  flowie_control_acl_set_error(ctx, TURBO_EPROTO);
}

static int flowie_control_acl_copy_token(char *output, size_t capacity,
                                         flowie_control_acl_token_t token) {
  if (!output || capacity == 0u || !token.value || token.length == 0u || token.length >= capacity)
    return TURBO_EPROTO;
  memcpy(output, token.value, token.length);
  output[token.length] = '\0';
  return TURBO_OK;
}

void flowie_control_acl_parse_accept(flowie_control_acl_parse_ctx_t *ctx,
                                     flowie_control_acl_token_t subject,
                                     turbo_flow_security_effect_t connection_effect) {
  if (!ctx || ctx->status != TURBO_OK) return;
  if (connection_effect == TURBO_FLOW_SECURITY_DENY && ctx->document.entry_count != 0u) {
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
    return;
  }
  if (flowie_control_acl_copy_token(ctx->document.subject, sizeof(ctx->document.subject),
                                    subject) != TURBO_OK) {
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
    return;
  }
  ctx->document.connection_effect = connection_effect;
  ctx->accepted = 1;
}

flowie_control_acl_group_path_t
flowie_control_acl_group_path_start(flowie_control_acl_parse_ctx_t *ctx,
                                    flowie_control_acl_token_t group) {
  flowie_control_acl_group_path_t path;
  memset(&path, 0, sizeof(path));
  if (!ctx || ctx->status != TURBO_OK || !group.value || group.length == 0u) {
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
    return path;
  }
  path.items[path.count++] = group;
  return path;
}

flowie_control_acl_group_path_t
flowie_control_acl_group_path_append(flowie_control_acl_parse_ctx_t *ctx,
                                     flowie_control_acl_group_path_t path,
                                     flowie_control_acl_token_t group) {
  if (!ctx || ctx->status != TURBO_OK || !group.value || group.length == 0u) return path;
  if (path.count >= TURBO_FLOW_SECURITY_MAX_GROUPS) {
    flowie_control_acl_set_error(ctx, TURBO_ENOSPC);
    return path;
  }
  path.items[path.count++] = group;
  return path;
}

flowie_control_acl_token_list_t
flowie_control_acl_token_list_start(flowie_control_acl_parse_ctx_t *ctx,
                                    flowie_control_acl_token_t item) {
  flowie_control_acl_token_list_t list;
  memset(&list, 0, sizeof(list));
  if (!ctx || ctx->status != TURBO_OK || !item.value || item.length == 0u) {
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
    return list;
  }
  list.items[list.count++] = item;
  return list;
}

flowie_control_acl_token_list_t
flowie_control_acl_token_list_append(flowie_control_acl_parse_ctx_t *ctx,
                                     flowie_control_acl_token_list_t list,
                                     flowie_control_acl_token_t item) {
  if (!ctx || ctx->status != TURBO_OK || !item.value || item.length == 0u) return list;
  if (list.count >= FLOWIE_CONTROL_ACL_MAX_ALTERNATIVES) {
    flowie_control_acl_set_error(ctx, TURBO_ENOSPC);
    return list;
  }
  for (size_t index = 0u; index < list.count; ++index) {
    if (list.items[index].length == item.length &&
        memcmp(list.items[index].value, item.value, item.length) == 0) {
      flowie_control_acl_set_error(ctx, TURBO_EPROTO);
      return list;
    }
  }
  list.items[list.count++] = item;
  return list;
}

static flowie_control_acl_topic_parse_t
flowie_control_acl_topic_finish(flowie_control_acl_parse_ctx_t *ctx,
                                flowie_control_acl_token_t domain,
                                flowie_control_acl_group_path_t groups,
                                flowie_control_acl_token_t device, const char *end,
                                size_t alternative_count) {
  flowie_control_acl_topic_parse_t topic;
  memset(&topic, 0, sizeof(topic));
  if (!ctx || ctx->status != TURBO_OK || !domain.value || domain.length == 0u ||
      groups.count == 0u || !device.value || device.length == 0u || !end || end <= domain.value) {
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
    return topic;
  }
  topic.domain = domain;
  topic.groups = groups;
  topic.device = device;
  topic.complete.value = domain.value;
  topic.complete.length = (size_t)(end - domain.value);
  topic.complete.line = domain.line;
  topic.complete.column = domain.column;
  topic.alternative_count = alternative_count;
  topic.uses_username = device.length == 2u && memcmp(device.value, "%u", 2u) == 0;
  topic.uses_client_id = device.length == 2u && memcmp(device.value, "%c", 2u) == 0;
  return topic;
}

flowie_control_acl_topic_parse_t
flowie_control_acl_topic_build(flowie_control_acl_parse_ctx_t *ctx,
                               flowie_control_acl_token_t domain,
                               flowie_control_acl_group_path_t groups,
                               flowie_control_acl_token_t device,
                               flowie_control_acl_token_t leaf) {
  return flowie_control_acl_topic_finish(ctx, domain, groups, device, leaf.value + leaf.length, 1u);
}

flowie_control_acl_topic_parse_t
flowie_control_acl_topic_build_alternatives(flowie_control_acl_parse_ctx_t *ctx,
                                            flowie_control_acl_token_t domain,
                                            flowie_control_acl_group_path_t groups,
                                            flowie_control_acl_token_t device,
                                            flowie_control_acl_token_t open,
                                            flowie_control_acl_token_list_t alternatives,
                                            flowie_control_acl_token_t close) {
  if (!open.value || open.length != 1u || alternatives.count < 2u || !close.value ||
      close.length != 1u)
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
  return flowie_control_acl_topic_finish(ctx, domain, groups, device,
                                         close.value ? close.value + close.length : NULL,
                                         alternatives.count);
}

void flowie_control_acl_entry_add(flowie_control_acl_parse_ctx_t *ctx,
                                  turbo_flow_security_effect_t effect, uint32_t action_mask,
                                  flowie_control_acl_topic_parse_t topic) {
  flowie_control_acl_entry_t *entry;
  if (!ctx || ctx->status != TURBO_OK) return;
  if (ctx->document.entry_count >= FLOWIE_CONTROL_ACL_MAX_ENTRIES) {
    flowie_control_acl_set_error(ctx, TURBO_ENOSPC);
    return;
  }
  if (!topic.complete.value || topic.complete.length == 0u ||
      topic.complete.length > TURBO_FLOW_SECURITY_PATTERN_MAX || topic.groups.count == 0u ||
      topic.groups.count > TURBO_FLOW_SECURITY_MAX_GROUPS || topic.alternative_count == 0u ||
      topic.alternative_count > FLOWIE_CONTROL_ACL_MAX_ALTERNATIVES) {
    flowie_control_acl_set_error(ctx, TURBO_EPROTO);
    return;
  }
  entry = &ctx->document.entries[ctx->document.entry_count];
  memset(entry, 0, sizeof(*entry));
  entry->effect = effect;
  entry->action_mask = action_mask;
  memcpy(entry->topic, topic.complete.value, topic.complete.length);
  entry->topic[topic.complete.length] = '\0';
  entry->group_count = (uint8_t)topic.groups.count;
  entry->alternative_count = (uint8_t)topic.alternative_count;
  entry->uses_username = topic.uses_username;
  entry->uses_client_id = topic.uses_client_id;
  for (size_t index = 0u; index < topic.groups.count; ++index) {
    size_t offset = (size_t)(topic.groups.items[index].value - topic.complete.value);
    if (offset > UINT16_MAX || topic.groups.items[index].length > UINT16_MAX) {
      flowie_control_acl_set_error(ctx, TURBO_EPROTO);
      return;
    }
    entry->group_offsets[index] = (uint16_t)offset;
    entry->group_lengths[index] = (uint16_t)topic.groups.items[index].length;
  }
  {
    size_t device_offset = (size_t)(topic.device.value - topic.complete.value);
    if (device_offset > UINT16_MAX || topic.device.length > UINT16_MAX) {
      flowie_control_acl_set_error(ctx, TURBO_EPROTO);
      return;
    }
    entry->device_offset = (uint16_t)device_offset;
    entry->device_length = (uint16_t)topic.device.length;
  }
  ++ctx->document.entry_count;
}

int flowie_control_acl_parse(const char *text, size_t text_size,
                             flowie_control_acl_document_t *out) {
  flowie_control_acl_parse_ctx_t ctx;
  flowie_control_acl_lexer_t lexer;
  flowie_control_acl_token_t token;
  void *parser;
  int token_id = FLOWIE_CONTROL_ACL_LEX_END;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_acl_document_t)FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
  if (!text || text_size == 0u || text_size > FLOWIE_CONTROL_ACL_DOCUMENT_MAX ||
      memchr(text, '\0', text_size) || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  memset(&ctx, 0, sizeof(ctx));
  ctx.document = (flowie_control_acl_document_t)FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
  parser = FlowieControlAclParseAlloc(malloc);
  if (!parser) return TURBO_ENOMEM;
  flowie_control_acl_lexer_init(&lexer, text, text_size);
  while ((token_id = flowie_control_acl_lexer_next(&lexer, &token)) > 0) {
    FlowieControlAclParse(parser, token_id, token, &ctx);
    if (ctx.status != TURBO_OK) break;
  }
  if (token_id < 0 && ctx.status == TURBO_OK) ctx.status = TURBO_EPROTO;
  if (ctx.status == TURBO_OK) {
    memset(&token, 0, sizeof(token));
    token.line = lexer.line;
    token.column = lexer.column;
    FlowieControlAclParse(parser, 0, token, &ctx);
  }
  FlowieControlAclParseFree(parser, free);
  if (ctx.status != TURBO_OK || !ctx.accepted) return ctx.status ? ctx.status : TURBO_EPROTO;
  *out = ctx.document;
  return TURBO_OK;
}

static const char *flowie_control_acl_access_name(uint32_t action_mask) {
  if (action_mask == TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE) return "read";
  if (action_mask == TURBO_FLOW_SECURITY_ACTION_PUBLISH) return "write";
  if (action_mask ==
      (TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE))
    return "readwrite";
  return NULL;
}

static int flowie_control_acl_append(char *output, size_t capacity, size_t *offset,
                                     const char *text, size_t text_size) {
  if (!output || !offset || !text || *offset > capacity || text_size > capacity - *offset)
    return TURBO_ENOSPC;
  memcpy(output + *offset, text, text_size);
  *offset += text_size;
  return TURBO_OK;
}

int flowie_control_acl_format(const flowie_control_acl_document_t *document, char *text_out,
                              size_t text_capacity, size_t *text_size_out) {
  size_t offset = 0u;
  int rc = TURBO_OK;
  if (text_size_out) *text_size_out = 0u;
  if (!document || document->size < sizeof(*document) || !document->subject[0] ||
      document->entry_count > FLOWIE_CONTROL_ACL_MAX_ENTRIES || !text_out ||
      text_capacity == 0u || !text_size_out)
    return TURBO_EINVAL;
#define FLOWIE_ACL_APPEND_LITERAL(value)                                                           \
  flowie_control_acl_append(text_out, text_capacity - 1u, &offset, value, sizeof(value) - 1u)
  rc = FLOWIE_ACL_APPEND_LITERAL("user ");
  if (rc == TURBO_OK)
    rc = flowie_control_acl_append(text_out, text_capacity - 1u, &offset, document->subject,
                                   strlen(document->subject));
  if (rc == TURBO_OK)
    rc = document->connection_effect == TURBO_FLOW_SECURITY_ALLOW
             ? FLOWIE_ACL_APPEND_LITERAL(" allow")
             : FLOWIE_ACL_APPEND_LITERAL(" deny");
  if (rc == TURBO_OK && document->entry_count != 0u) rc = FLOWIE_ACL_APPEND_LITERAL(" {\n");
  for (size_t index = 0u; rc == TURBO_OK && index < document->entry_count; ++index) {
    const flowie_control_acl_entry_t *entry = &document->entries[index];
    const char *access = flowie_control_acl_access_name(entry->action_mask);
    if (!access || !entry->topic[0] ||
        (entry->effect != TURBO_FLOW_SECURITY_ALLOW &&
         entry->effect != TURBO_FLOW_SECURITY_DENY)) {
      rc = TURBO_EINVAL;
      break;
    }
    rc = FLOWIE_ACL_APPEND_LITERAL("  ");
    if (rc == TURBO_OK && entry->effect == TURBO_FLOW_SECURITY_DENY)
      rc = FLOWIE_ACL_APPEND_LITERAL("deny ");
    if (rc == TURBO_OK)
      rc = flowie_control_acl_append(text_out, text_capacity - 1u, &offset, access,
                                     strlen(access));
    if (rc == TURBO_OK) rc = FLOWIE_ACL_APPEND_LITERAL(" topic ");
    if (rc == TURBO_OK)
      rc = flowie_control_acl_append(text_out, text_capacity - 1u, &offset, entry->topic,
                                     strlen(entry->topic));
    if (rc == TURBO_OK) rc = FLOWIE_ACL_APPEND_LITERAL("\n");
  }
  if (rc == TURBO_OK && document->entry_count != 0u) rc = FLOWIE_ACL_APPEND_LITERAL("}");
  if (rc == TURBO_OK) {
    text_out[offset] = '\0';
    *text_size_out = offset;
  }
#undef FLOWIE_ACL_APPEND_LITERAL
  return rc;
}

static int flowie_control_acl_rule_base(const flowie_control_acl_document_t *document,
                                        const char *domain_id,
                                        turbo_flow_security_rule_t *rule) {
  size_t subject_size;
  size_t domain_size;
  if (!document || !domain_id || !rule) return TURBO_EINVAL;
  subject_size = strnlen(document->subject, sizeof(document->subject));
  domain_size = strnlen(domain_id, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  if (subject_size == 0u || subject_size >= sizeof(document->subject) || domain_size == 0u ||
      domain_size > TURBO_FLOW_SECURITY_ID_MAX)
    return TURBO_EINVAL;
  *rule = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
  rule->subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
  memcpy(rule->subject, document->subject, subject_size + 1u);
  memcpy(rule->domain_id, domain_id, domain_size + 1u);
  return TURBO_OK;
}

static int flowie_control_acl_expand_topic(const flowie_control_acl_document_t *document,
                                           const flowie_control_acl_entry_t *entry,
                                           const char *alternative, size_t alternative_size,
                                           char output[TURBO_FLOW_SECURITY_PATTERN_MAX + 1u]) {
  const char *topic;
  size_t topic_size;
  size_t read = 0u;
  size_t written = 0u;
  if (!document || !entry || !output) return TURBO_EINVAL;
  topic = entry->topic;
  topic_size = strnlen(topic, sizeof(entry->topic));
  if (topic_size == 0u || topic_size >= sizeof(entry->topic)) return TURBO_EPROTO;
  while (read < topic_size) {
    const char *replacement = NULL;
    size_t replacement_size = 0u;
    if (topic[read] == '{') {
      const char *close = strchr(topic + read + 1u, '}');
      if (!alternative || !close || close[1] != '\0') return TURBO_EPROTO;
      replacement = alternative;
      replacement_size = alternative_size;
      read = topic_size;
    } else {
      replacement = topic + read;
      replacement_size = 1u;
      ++read;
    }
    if (replacement_size > TURBO_FLOW_SECURITY_PATTERN_MAX - written) return TURBO_ENOSPC;
    memcpy(output + written, replacement, replacement_size);
    written += replacement_size;
  }
  output[written] = '\0';
  return TURBO_OK;
}

static int flowie_control_acl_rule_add(const flowie_control_acl_document_t *document,
                                       const char *domain_id,
                                       const flowie_control_acl_entry_t *entry,
                                       const char *alternative, size_t alternative_size,
                                       turbo_flow_security_rule_t *rules, size_t rule_capacity,
                                       size_t *count) {
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  int rc;
  if (!entry || !rules || !count) return TURBO_EINVAL;
  if (*count >= rule_capacity) return TURBO_ENOSPC;
  rc = flowie_control_acl_rule_base(document, domain_id, &rule);
  if (rc != TURBO_OK) return rc;
  rule.effect = entry->effect;
  rule.action_mask = entry->action_mask;
  rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  rule.match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
  rc = flowie_control_acl_expand_topic(document, entry, alternative, alternative_size,
                                       rule.pattern);
  if (rc != TURBO_OK) return rc;
  rules[(*count)++] = rule;
  return TURBO_OK;
}

int flowie_control_acl_compile(const flowie_control_acl_document_t *document,
                               const char *domain_id, turbo_flow_security_rule_t *rules,
                               size_t rule_capacity, size_t *rule_count_out) {
  size_t count = 0u;
  int rc;
  if (rule_count_out) *rule_count_out = 0u;
  if (!document || document->size < sizeof(*document) || !domain_id || !rules ||
      rule_capacity == 0u || !rule_count_out ||
      document->entry_count > FLOWIE_CONTROL_ACL_MAX_ENTRIES)
    return TURBO_EINVAL;
  rc = flowie_control_acl_rule_base(document, domain_id, &rules[count]);
  if (rc != TURBO_OK) return rc;
  rules[count].effect = document->connection_effect;
  rules[count].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
  rules[count].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
  rules[count].match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
  rules[count].pattern[0] = '\0';
  ++count;
  for (size_t index = 0u; rc == TURBO_OK && index < document->entry_count; ++index) {
    const flowie_control_acl_entry_t *entry = &document->entries[index];
    if (entry->alternative_count <= 1u) {
      rc = flowie_control_acl_rule_add(document, domain_id, entry, NULL, 0u, rules,
                                       rule_capacity, &count);
      continue;
    }
    {
      const char *open = strrchr(entry->topic, '{');
      const char *close = open ? strchr(open + 1u, '}') : NULL;
      const char *cursor = open ? open + 1u : NULL;
      size_t expanded = 0u;
      if (!open || !close || close[1] != '\0') {
        rc = TURBO_EPROTO;
        break;
      }
      while (cursor < close) {
        const char *comma = memchr(cursor, ',', (size_t)(close - cursor));
        const char *end = comma ? comma : close;
        if (end == cursor) {
          rc = TURBO_EPROTO;
          break;
        }
        rc = flowie_control_acl_rule_add(document, domain_id, entry, cursor,
                                         (size_t)(end - cursor), rules, rule_capacity, &count);
        if (rc != TURBO_OK) break;
        ++expanded;
        cursor = comma ? comma + 1u : close;
      }
      if (rc == TURBO_OK && expanded != entry->alternative_count) rc = TURBO_EPROTO;
    }
  }
  if (rc != TURBO_OK) return rc;
  *rule_count_out = count;
  return TURBO_OK;
}
