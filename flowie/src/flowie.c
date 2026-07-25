#include "flowie.h"
#include "flowie_security_internal.h"
#include "flowie_topic_index_internal.h"

#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

int flowie_publish_message_map(const flowie_mqtt_publish_view_t *publish,
                               flowie_mqtt_version_t version, uint64_t owner_instance_id,
                               uint64_t session_id, uint64_t session_generation,
                               flowie_publish_message_view_t *out) {
  flowie_publish_message_view_t mapped = FLOWIE_PUBLISH_MESSAGE_VIEW_INIT;
  int rc;
  if (!publish || publish->size < sizeof(*publish) ||
      publish->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 || !out || out->size != sizeof(*out) ||
      owner_instance_id == 0u || session_id == 0u ||
      session_generation == 0u || (!publish->topic.data && publish->topic.size != 0u) ||
      (!publish->payload.data && publish->payload.size != 0u) ||
      !flowie_mqtt_version_is_supported(version) || publish->qos > 2u) {
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

typedef struct flowie_mqtt_security_leaf_s {
  flowie_topic_index_t topics;
} flowie_mqtt_security_leaf_t;

static const uint8_t FLOWIE_MQTT_VALIDATED_SECURITY_PROVENANCE = 0u;

int flowie_mqtt_validated_security_context_init(flowie_mqtt_validated_security_context_t *out,
                                                flowie_mqtt_security_resource_kind_t kind,
                                                tstr_t parser_validated_resource) {
  flowie_mqtt_validated_security_context_t initialized =
      FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_INIT;
  if (!out || (kind != FLOWIE_MQTT_SECURITY_TOPIC && kind != FLOWIE_MQTT_SECURITY_TOPIC_FILTER) ||
      !parser_validated_resource || tstr_len(parser_validated_resource) == 0u)
    return TURBO_EINVAL;
  initialized.public_context.kind = kind;
  initialized.resource = (flowie_mqtt_span_t){(const uint8_t *)parser_validated_resource,
                                              tstr_len(parser_validated_resource)};
  initialized.provenance = &FLOWIE_MQTT_VALIDATED_SECURITY_PROVENANCE;
  *out = initialized;
  return TURBO_OK;
}

static int flowie_mqtt_security_resource(const turbo_flow_security_request_t *request,
                                         flowie_mqtt_span_t *resource_out,
                                         flowie_mqtt_security_resource_kind_t *kind_out,
                                         int *validated_out) {
  const flowie_mqtt_security_context_t *context;
  flowie_mqtt_span_t resource;
  flowie_mqtt_security_resource_kind_t kind = FLOWIE_MQTT_SECURITY_TOPIC;
  int validated = 0;
  if (!request || !request->resource || !resource_out || !kind_out || !validated_out)
    return TURBO_EINVAL;
  context = (const flowie_mqtt_security_context_t *)request->protocol_context;
  if (context) {
    if (context->size < sizeof(*context)) return TURBO_EPROTO;
    kind = context->kind;
    if (kind != FLOWIE_MQTT_SECURITY_TOPIC && kind != FLOWIE_MQTT_SECURITY_TOPIC_FILTER)
      return TURBO_EPROTO;
    if (context->size == sizeof(flowie_mqtt_validated_security_context_t)) {
      const flowie_mqtt_validated_security_context_t *trusted =
          (const flowie_mqtt_validated_security_context_t *)context;
      if (trusted->abi_version != FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_ABI_V1 ||
          trusted->provenance != &FLOWIE_MQTT_VALIDATED_SECURITY_PROVENANCE ||
          trusted->resource.data != (const uint8_t *)request->resource ||
          trusted->resource.size == 0u) {
        return TURBO_EPROTO;
      }
      resource = trusted->resource;
      validated = 1;
    } else {
      resource =
          (flowie_mqtt_span_t){(const uint8_t *)request->resource, strlen(request->resource)};
    }
  } else {
    resource = (flowie_mqtt_span_t){(const uint8_t *)request->resource, strlen(request->resource)};
  }
  *resource_out = resource;
  *kind_out = kind;
  *validated_out = validated;
  return TURBO_OK;
}

static int flowie_mqtt_security_compile_leaf(void *ctx,
                                             const turbo_flow_security_matcher_leaf_t *input,
                                             void **compiled_leaf_out) {
  flowie_mqtt_security_leaf_t *compiled;
  int rc;
  (void)ctx;
  if (compiled_leaf_out) *compiled_leaf_out = NULL;
  if (!input || input->size < sizeof(*input) || !input->rules || input->rule_count == 0u ||
      !input->candidate_rule_indices || input->candidate_count == 0u || !compiled_leaf_out)
    return TURBO_EINVAL;
  compiled = (flowie_mqtt_security_leaf_t *)calloc(1u, sizeof(*compiled));
  if (!compiled) return TURBO_ENOMEM;
  rc = flowie_topic_index_init(&compiled->topics);
  for (size_t position = 0u; rc == TURBO_OK && position < input->candidate_count; ++position) {
    size_t rule_index = input->candidate_rule_indices[position];
    const turbo_flow_security_rule_t *rule =
        rule_index < input->rule_count ? &input->rules[rule_index] : NULL;
    flowie_mqtt_span_t filter;
    if (!rule || rule->size < sizeof(*rule) || rule->abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
        rule->match_kind != TURBO_FLOW_SECURITY_MATCH_ADAPTER ||
        rule->resource_type != TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC) {
      rc = TURBO_EPROTO;
      break;
    }
    filter = (flowie_mqtt_span_t){(const uint8_t *)rule->pattern, strlen(rule->pattern)};
    if (!flowie_mqtt_topic_filter_validate(filter)) {
      rc = TURBO_EPROTO;
      break;
    }
    rc = flowie_topic_index_insert(&compiled->topics, filter, position);
  }
  if (rc != TURBO_OK) {
    flowie_topic_index_destroy(&compiled->topics);
    free(compiled);
    return rc;
  }
  *compiled_leaf_out = compiled;
  return TURBO_OK;
}

static int flowie_mqtt_security_evaluate_leaf(void *ctx, const void *compiled_leaf,
                                              const turbo_flow_security_request_t *request,
                                              turbo_flow_security_match_emit_fn emit,
                                              void *emit_ctx) {
  const flowie_mqtt_security_leaf_t *compiled = (const flowie_mqtt_security_leaf_t *)compiled_leaf;
  flowie_mqtt_span_t resource;
  flowie_mqtt_security_resource_kind_t kind;
  int validated;
  int rc;
  (void)ctx;
  if (!compiled || !request || request->size < sizeof(*request) || !emit || !request->resource)
    return TURBO_EINVAL;
  if (request->resource_type != TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC) return TURBO_EPROTO;
  rc = flowie_mqtt_security_resource(request, &resource, &kind, &validated);
  if (rc != TURBO_OK) return rc;
  if (kind == FLOWIE_MQTT_SECURITY_TOPIC_FILTER) {
    if (!validated && !flowie_mqtt_topic_filter_validate(resource)) return TURBO_EPROTO;
    return flowie_topic_index_visit_validated_containing_filters(&compiled->topics, resource, emit,
                                                                 emit_ctx);
  }
  if (!validated && !flowie_mqtt_topic_name_validate(resource)) return TURBO_EPROTO;
  return flowie_topic_index_visit_validated_topic(&compiled->topics, resource, emit, emit_ctx);
}

static void flowie_mqtt_security_destroy_leaf(void *ctx, void *compiled_leaf) {
  flowie_mqtt_security_leaf_t *compiled = (flowie_mqtt_security_leaf_t *)compiled_leaf;
  (void)ctx;
  if (!compiled) return;
  flowie_topic_index_destroy(&compiled->topics);
  free(compiled);
}

int flowie_mqtt_security_matcher_init(turbo_flow_security_matcher_t *out) {
  turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  matcher.compile_leaf = flowie_mqtt_security_compile_leaf;
  matcher.evaluate_leaf = flowie_mqtt_security_evaluate_leaf;
  matcher.destroy_leaf = flowie_mqtt_security_destroy_leaf;
  *out = matcher;
  return TURBO_OK;
}
