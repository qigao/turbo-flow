#include "flowie.h"

#include "turbo_error.h"

#include <string.h>

static flowie_mqtt_span_t flowie_security_filter_inner(flowie_mqtt_span_t filter) {
  static const uint8_t prefix[] = "$share/";
  if (filter.size <= sizeof(prefix) - 1u || memcmp(filter.data, prefix, sizeof(prefix) - 1u) != 0)
    return filter;
  for (size_t i = sizeof(prefix) - 1u; i < filter.size; ++i) {
    if (filter.data[i] == '/')
      return (flowie_mqtt_span_t){filter.data + i + 1u, filter.size - i - 1u};
  }
  return (flowie_mqtt_span_t){NULL, 0u};
}

static flowie_mqtt_span_t flowie_security_filter_token(flowie_mqtt_span_t filter, size_t *offset,
                                                       int *finished) {
  size_t begin = *offset;
  size_t end = begin;
  while (end < filter.size && filter.data[end] != '/')
    ++end;
  *finished = end == filter.size;
  *offset = *finished ? end : end + 1u;
  return (flowie_mqtt_span_t){filter.data + begin, end - begin};
}

static int flowie_security_token_is(flowie_mqtt_span_t token, uint8_t value) {
  return token.size == 1u && token.data[0] == value;
}

/*
 * Decide whether every topic selected by `requested` is also selected by `policy`.
 * Both inputs are valid MQTT filters. The walk is O(levels), uses no allocation,
 * and preserves MQTT's root `$` wildcard exclusion.
 */
static int flowie_mqtt_filter_contains(flowie_mqtt_span_t policy, flowie_mqtt_span_t requested,
                                       int *matched_out) {
  size_t policy_offset = 0u;
  size_t requested_offset = 0u;
  int policy_finished = 0;
  int requested_finished = 0;
  int root = 1;
  if (!matched_out || !flowie_mqtt_topic_filter_validate(policy) ||
      !flowie_mqtt_topic_filter_validate(requested))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  policy = flowie_security_filter_inner(policy);
  requested = flowie_security_filter_inner(requested);
  if (!policy.data || !requested.data || !flowie_mqtt_topic_filter_validate(policy) ||
      !flowie_mqtt_topic_filter_validate(requested))
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  *matched_out = 0;
  for (;;) {
    flowie_mqtt_span_t policy_token =
        flowie_security_filter_token(policy, &policy_offset, &policy_finished);
    flowie_mqtt_span_t requested_token =
        flowie_security_filter_token(requested, &requested_offset, &requested_finished);
    int policy_hash = flowie_security_token_is(policy_token, '#');
    int requested_hash = flowie_security_token_is(requested_token, '#');
    int policy_plus = flowie_security_token_is(policy_token, '+');
    int requested_plus = flowie_security_token_is(requested_token, '+');

    if (policy_hash) {
      if (root && requested_token.size != 0u && requested_token.data[0] == '$') return TURBO_OK;
      *matched_out = 1;
      return TURBO_OK;
    }
    if (requested_hash) return TURBO_OK;
    if (policy_plus) {
      if (root && !requested_plus && requested_token.size != 0u && requested_token.data[0] == '$')
        return TURBO_OK;
    } else if (requested_plus || policy_token.size != requested_token.size ||
               memcmp(policy_token.data, requested_token.data, policy_token.size) != 0) {
      return TURBO_OK;
    }
    if (requested_finished) {
      if (policy_finished) {
        *matched_out = 1;
      } else {
        flowie_mqtt_span_t remaining =
            flowie_security_filter_token(policy, &policy_offset, &policy_finished);
        *matched_out = policy_finished && flowie_security_token_is(remaining, '#');
      }
      return TURBO_OK;
    }
    if (policy_finished) {
      return TURBO_OK;
    }
    root = 0;
  }
}

int flowie_publish_message_map(const flowie_mqtt_publish_view_t *publish,
                               flowie_mqtt_version_t version, uint64_t owner_instance_id,
                               uint64_t session_id, uint64_t session_generation,
                               flowie_publish_message_view_t *out) {
  flowie_publish_message_view_t mapped = FLOWIE_PUBLISH_MESSAGE_VIEW_INIT;
  int rc;
  if (!publish || publish->size < sizeof(*publish) ||
      publish->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_ABI_V1 || owner_instance_id == 0u || session_id == 0u ||
      session_generation == 0u || (!publish->topic.data && publish->topic.size != 0u) ||
      (!publish->payload.data && publish->payload.size != 0u) ||
      (version != FLOWIE_MQTT_VERSION_3_1_1 && version != FLOWIE_MQTT_VERSION_5) ||
      publish->qos > 2u) {
    return TURBO_EINVAL;
  }
  mapped.metadata.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  mapped.metadata.protocol_version = (uint32_t)version;
  mapped.metadata.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
  mapped.metadata.qos = publish->qos;
  mapped.metadata.packet_id = publish->packet_id;
  mapped.metadata.session_generation = session_generation;
  mapped.metadata.duplicate = publish->duplicate;
  mapped.metadata.retain = publish->retain;
  rc = turbo_flow_protocol_message_validate(&mapped.metadata);
  if (rc != TURBO_OK) return rc;
  mapped.route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  mapped.route.owner_instance_id = owner_instance_id;
  mapped.route.session_id = session_id;
  mapped.route.session_generation = session_generation;
  mapped.topic = publish->topic;
  mapped.properties = publish->properties;
  mapped.payload = publish->payload;
  *out = mapped;
  return TURBO_OK;
}

static int flowie_mqtt_security_match(void *ctx, const turbo_flow_security_rule_t *rule,
                                      const turbo_flow_security_request_t *request,
                                      int *matched_out) {
  const flowie_mqtt_security_context_t *protocol_context;
  flowie_mqtt_span_t filter;
  flowie_mqtt_span_t topic;
  int rc;
  (void)ctx;
  if (!rule || rule->size < sizeof(*rule) || !request || request->size < sizeof(*request) ||
      !matched_out)
    return TURBO_EINVAL;
  *matched_out = 0;
  if (rule->resource_type != TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC ||
      request->resource_type != TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC || !request->resource)
    return TURBO_OK;
  filter.data = (const uint8_t *)rule->pattern;
  filter.size = strlen(rule->pattern);
  topic.data = (const uint8_t *)request->resource;
  topic.size = strlen(request->resource);
  protocol_context = (const flowie_mqtt_security_context_t *)request->protocol_context;
  if (protocol_context && protocol_context->size >= sizeof(*protocol_context) &&
      protocol_context->kind == FLOWIE_MQTT_SECURITY_TOPIC_FILTER) {
    rc = flowie_mqtt_filter_contains(filter, topic, matched_out);
  } else {
    rc = flowie_mqtt_topic_matches(filter, topic, matched_out);
  }
  return rc == FLOWIE_MQTT_PARSE_OK ? TURBO_OK : TURBO_EPROTO;
}

int flowie_mqtt_security_matcher_init(turbo_flow_security_matcher_t *out) {
  turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  matcher.match = flowie_mqtt_security_match;
  *out = matcher;
  return TURBO_OK;
}
