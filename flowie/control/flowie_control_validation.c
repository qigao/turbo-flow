#include "flowie_control_validation_internal.h"

#include "flowie_mqtt_protocol.h"
#include "turbo_error.h"

#include <string.h>

int flowie_control_text_valid(const char *value, size_t limit) {
  size_t length;
  if (!value || limit == 0u) return 0;
  length = strnlen(value, limit + 1u);
  if (length == 0u || length > limit) return 0;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

int flowie_control_policy_rule_syntax_validate(const char *domain_id, const char *rule_line,
                                               size_t rule_line_size,
                                               turbo_flow_security_rule_t *rule_out) {
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  char canonical[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u];
  size_t canonical_size = 0u;
  int rc;
  if (!domain_id || !rule_line || rule_line_size == 0u ||
      rule_line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX || memchr(rule_line, '\0', rule_line_size))
    return TURBO_EINVAL;
  rc = turbo_flow_security_rule_parse_line(rule_line, rule_line_size, &rule);
  if (rc != TURBO_OK || strcmp(rule.domain_id, domain_id) != 0) return TURBO_EPROTO;
  rc = turbo_flow_security_rule_format_line(&rule, canonical, sizeof(canonical), &canonical_size);
  if (rc != TURBO_OK || canonical_size != rule_line_size ||
      memcmp(canonical, rule_line, rule_line_size) != 0)
    return TURBO_EPROTO;
  if (rule.resource_type == TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC &&
      rule.match_kind == TURBO_FLOW_SECURITY_MATCH_ADAPTER &&
      !flowie_mqtt_topic_filter_validate(
          (flowie_mqtt_span_t){(const uint8_t *)rule.pattern, strlen(rule.pattern)}))
    return TURBO_EPROTO;
  if (rule_out) *rule_out = rule;
  return TURBO_OK;
}
