#include "flowie.h"
#include "flowie_security_internal.h"
#include "flowie_session_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <stdlib.h>
#include <string.h>

static void flowie_copy(char *out, size_t capacity, const char *value) {
  size_t size = strlen(value);
  check(size < capacity);
  memcpy(out, value, size + 1u);
}

static void flowie_test_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flowie_test_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flowie_test_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - i * 8u));
}

static uint32_t flowie_test_xorshift32(uint32_t *state) {
  uint32_t value = *state;
  value ^= value << 13u;
  value ^= value >> 17u;
  value ^= value << 5u;
  *state = value;
  return value;
}

static size_t flowie_test_legacy_delivery_record(uint8_t minor, const uint8_t *packet,
                                                 size_t packet_size, uint8_t *out,
                                                 size_t capacity) {
  enum { FLOWIE_TEST_DELIVERY_WAIT_ACK = 2 };
  uint8_t header[8] = {'F', 'S', 'E', 'S', 0u, 1u, 0u, minor};
  uint8_t metadata[25] = {0};
  uint8_t delivery[4] = {0};
  size_t offset = 0u;
  size_t written;
  metadata[0] = (uint8_t)FLOWIE_MQTT_VERSION_5;
  flowie_test_write_u64(metadata + 1u, 71u);
  flowie_test_write_u64(metadata + 9u, 1u);
  flowie_test_write_u16(metadata + 17u, 60u);
  flowie_test_write_u32(metadata + 19u, 60u);
  flowie_test_write_u16(metadata + 23u, 1u);
  flowie_test_write_u16(delivery, 1u);
  delivery[2] = 1u;
  delivery[3] = FLOWIE_TEST_DELIVERY_WAIT_ACK;
  written = turbo_ltv_build(1u, header, sizeof(header), out + offset, capacity - offset);
  if (written == 0u) return 0u;
  offset += written;
  written = turbo_ltv_build(2u, metadata, sizeof(metadata), out + offset, capacity - offset);
  if (written == 0u) return 0u;
  offset += written;
  written = turbo_ltv_build(6u, delivery, sizeof(delivery), out + offset, capacity - offset);
  if (written == 0u) return 0u;
  offset += written;
  written = turbo_ltv_build(7u, packet, packet_size, out + offset, capacity - offset);
  if (written == 0u) return 0u;
  return offset + written;
}

static int flowie_test_owner_round_trip(flowie_session_owner_t *owner,
                                        const flowie_session_config_t *config,
                                        flowie_mqtt_span_t client_id, uint64_t owner_instance_id,
                                        flowie_session_owner_t **restored_out) {
  flowie_session_config_t restored_config;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  uint8_t *record = NULL;
  size_t record_size = 0u;
  int rc;
  if (!owner || !config || !restored_out) return TURBO_EINVAL;
  *restored_out = NULL;
  rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc != TURBO_OK) return rc;
  rc = flowie_session_owner_record_encode(owner, NULL, 0u, &record_size);
  if (rc != TURBO_ENOSPC || record_size == 0u) return rc == TURBO_OK ? TURBO_EPROTO : rc;
  record = (uint8_t *)malloc(record_size);
  if (!record) return TURBO_ENOMEM;
  rc = flowie_session_owner_record_encode(owner, record, record_size, &record_size);
  if (rc == TURBO_OK) {
    restored_config = *config;
    restored_config.owner_instance_id = owner_instance_id;
    rc = flowie_session_owner_record_restore(&restored_config, client_id,
                                             snapshot.resource_generation, record, record_size,
                                             restored_out);
  }
  free(record);
  return rc;
}

spec("flowie application bridges") {
  it("rejects a truncated endpoint bindings v1 prefix") {
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    bindings.size = FLOWIE_ENDPOINT_BINDINGS_V1_SIZE - 1u;
    bindings.persistence = &persistence;
    check_int_eq(flowie_register_bound_endpoint(flow, "mqtt.endpoint", &config, &bindings),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("maps a parsed publish into pointer-free protocol metadata and an owned route token") {
    static const uint8_t topic[] = "root-a/events";
    static const uint8_t payload[] = "value";
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_publish_message_view_t message = FLOWIE_PUBLISH_MESSAGE_VIEW_INIT;
    publish.qos = 1u;
    publish.packet_id = 42u;
    publish.duplicate = 1u;
    publish.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
    publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
    publish.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;

    check_int_eq(flowie_publish_message_map(&publish, FLOWIE_MQTT_VERSION_5, 7u, 9u, 11u, &message),
                 TURBO_OK);
    check_int_eq(turbo_flow_protocol_message_validate(&message.metadata), TURBO_OK);
    check_int_eq(message.metadata.protocol, TURBO_FLOW_PROTOCOL_MQTT);
    check_uint_eq(message.metadata.packet_id, 42u);
    check_uint_eq(message.route.owner_instance_id, 7u);
    check_uint_eq(message.route.session_id, 9u);
    check_uint_eq(message.route.session_generation, 11u);
    check(message.payload.data == payload);

    message = (flowie_publish_message_view_t)FLOWIE_PUBLISH_MESSAGE_VIEW_INIT;
    check_int_eq(flowie_publish_message_map(&publish, FLOWIE_MQTT_VERSION_5, 7u, 9u, 0u, &message),
                 TURBO_EINVAL);
  }

  it("injects MQTT topic-filter semantics into SecurityRealm") {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
    turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    turbo_flow_security_decision_t expected = TURBO_FLOW_SECURITY_DECISION_INIT;
    flowie_mqtt_security_context_t context = FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
    flowie_mqtt_validated_security_context_t validated_context =
        FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    tstr_t validated_resource = NULL;

    rule.effect = TURBO_FLOW_SECURITY_ALLOW;
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    flowie_copy(rule.subject, sizeof(rule.subject), "writer");
    flowie_copy(rule.domain_id, sizeof(rule.domain_id), "root-a");
    rule.action_mask = TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
    flowie_copy(rule.pattern, sizeof(rule.pattern), "root-a/+/events/#");
    check_int_eq(flowie_mqtt_security_matcher_init(&matcher), TURBO_OK);
    config.resource_uid = "security:flowie";
    config.owner_name = "flowie.security";
    config.policy_version = 3u;
    config.rules = &rule;
    config.rule_count = 1u;
    config.matcher = matcher;
    check_int_eq(turbo_flow_security_realm_create(&config, &realm), TURBO_OK);

    flowie_copy(principal.principal_id, sizeof(principal.principal_id), "device-1");
    flowie_copy(principal.principal_type, sizeof(principal.principal_type), "device");
    flowie_copy(principal.domain_id, sizeof(principal.domain_id), "root-a");
    flowie_copy(principal.auth_method, sizeof(principal.auth_method), "token");
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
    principal.role_count = 1u;
    flowie_copy(principal.roles[0], sizeof(principal.roles[0]), "writer");
    principal.policy_version = 3u;
    check_int_eq(flowie_security_principal_validate(&principal), TURBO_OK);
    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/device-1/events/temperature";
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);

    request.resource = "root-a/device-1/commands/reboot";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_EPERM);

    context.kind = FLOWIE_MQTT_SECURITY_TOPIC_FILTER;
    request.protocol_context = &context;
    request.action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    request.resource = "root-a/+/events/temperature";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    request.resource = "root-a/device-1/events";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    request.resource = "root-a/#";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_EPERM);

    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.protocol_context = NULL;
    request.resource = "root-a/+/events/temperature";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPROTO);
    context.kind = FLOWIE_MQTT_SECURITY_TOPIC_FILTER;
    request.action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    request.resource = "root-a/#/invalid";
    request.protocol_context = &context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPROTO);
    context.kind = (flowie_mqtt_security_resource_kind_t)99;
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource = "root-a/device-1/events/temperature";
    request.protocol_context = &context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPROTO);
    context = (flowie_mqtt_security_context_t)FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
    context.size = sizeof(context.size);
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPROTO);

    request.protocol_context = NULL;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    expected = decision;
    validated_resource = tstr_new_len("root-a/device-1/events/temperature",
                                      sizeof("root-a/device-1/events/temperature") - 1u);
    check_not_null(validated_resource);
    check_int_eq(flowie_mqtt_validated_security_context_init(
                     &validated_context, FLOWIE_MQTT_SECURITY_TOPIC, validated_resource),
                 TURBO_OK);
    request.resource = validated_resource;
    request.protocol_context = &validated_context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    check_int_eq(decision.effect, expected.effect);
    check_int_eq(decision.reason, expected.reason);
    check_size_eq(decision.matched_rule, expected.matched_rule);
    check_uint_eq(decision.policy_version, expected.policy_version);

    validated_context.provenance = NULL;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPROTO);
    tstr_freep(&validated_resource);

    context = (flowie_mqtt_security_context_t)FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
    context.kind = FLOWIE_MQTT_SECURITY_TOPIC_FILTER;
    request.action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    request.resource = "root-a/+/events/temperature";
    request.protocol_context = &context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    expected = decision;
    validated_resource =
        tstr_new_len("root-a/+/events/temperature", sizeof("root-a/+/events/temperature") - 1u);
    check_not_null(validated_resource);
    check_int_eq(flowie_mqtt_validated_security_context_init(
                     &validated_context, FLOWIE_MQTT_SECURITY_TOPIC_FILTER, validated_resource),
                 TURBO_OK);
    request.action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    request.resource = validated_resource;
    request.protocol_context = &validated_context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    check_int_eq(decision.effect, expected.effect);
    check_int_eq(decision.reason, expected.reason);
    check_size_eq(decision.matched_rule, expected.matched_rule);
    check_uint_eq(decision.policy_version, expected.policy_version);
    tstr_freep(&validated_resource);
    turbo_flow_security_realm_destroy(realm);
  }

  it("resolves ACL username and client-id placeholders as exact topic segments") {
    turbo_flow_security_rule_t rules[2] = {TURBO_FLOW_SECURITY_RULE_INIT,
                                           TURBO_FLOW_SECURITY_RULE_INIT};
    turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
    turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    flowie_mqtt_security_context_t context = FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
    turbo_flow_security_realm_t *realm = NULL;

    for (size_t index = 0u; index < 2u; ++index) {
      rules[index].effect = TURBO_FLOW_SECURITY_ALLOW;
      rules[index].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
      flowie_copy(rules[index].subject, sizeof(rules[index].subject), "device-1");
      flowie_copy(rules[index].domain_id, sizeof(rules[index].domain_id), "root-a");
      rules[index].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
      rules[index].match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
    }
    rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    flowie_copy(rules[0].pattern, sizeof(rules[0].pattern),
                "root-a/groups/operators/devices/%u/event");
    rules[1].action_mask = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    flowie_copy(rules[1].pattern, sizeof(rules[1].pattern),
                "root-a/groups/operators/devices/%c/command");
    check_int_eq(flowie_mqtt_security_matcher_init(&matcher), TURBO_OK);
    config.resource_uid = "security:placeholders";
    config.owner_name = "flowie.security";
    config.policy_version = 4u;
    config.rules = rules;
    config.rule_count = 2u;
    config.matcher = matcher;
    check_int_eq(turbo_flow_security_realm_create(&config, &realm), TURBO_OK);

    flowie_copy(principal.principal_id, sizeof(principal.principal_id), "device-1");
    flowie_copy(principal.principal_type, sizeof(principal.principal_type), "device");
    flowie_copy(principal.domain_id, sizeof(principal.domain_id), "root-a");
    flowie_copy(principal.auth_method, sizeof(principal.auth_method), "token");
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
    principal.policy_version = 4u;
    context.username = (flowie_mqtt_span_t){(const uint8_t *)"mqtt-user", 9u};
    context.client_id = (flowie_mqtt_span_t){(const uint8_t *)"client-a", 8u};
    request.principal = &principal;
    request.domain_id = "root-a";
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.protocol_context = &context;

    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource = "root-a/groups/operators/devices/mqtt-user/event";
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    request.resource = "root-a/groups/operators/devices/device-1/event";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPERM);

    context.kind = FLOWIE_MQTT_SECURITY_TOPIC_FILTER;
    request.action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    request.resource = "root-a/groups/operators/devices/client-a/command";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision), TURBO_OK);
    request.resource = "root-a/groups/operators/devices/+/command";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPERM);
    context.client_id = (flowie_mqtt_span_t){NULL, 0u};
    request.resource = "root-a/groups/operators/devices/client-a/command";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 10u, &decision),
                 TURBO_EPERM);
    turbo_flow_security_realm_destroy(realm);
  }
}

static flowie_mqtt_connect_view_t flowie_test_connect(flowie_mqtt_version_t version,
                                                      const char *client_id, int clean_start,
                                                      uint32_t expiry) {
  static uint8_t expiry_property[5];
  static const uint8_t empty_property = 0u;
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.version = version;
  connect.clean_start = (uint8_t)clean_start;
  connect.keep_alive = 30u;
  connect.client_id.data = (const uint8_t *)client_id;
  connect.client_id.size = strlen(client_id);
  connect.properties.values.data = &empty_property;
  if (version == FLOWIE_MQTT_VERSION_5 && expiry != 0u) {
    expiry_property[0] = FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL;
    expiry_property[1] = (uint8_t)(expiry >> 24u);
    expiry_property[2] = (uint8_t)(expiry >> 16u);
    expiry_property[3] = (uint8_t)(expiry >> 8u);
    expiry_property[4] = (uint8_t)expiry;
    connect.properties.values.data = expiry_property;
    connect.properties.values.size = sizeof(expiry_property);
  }
  return connect;
}

enum {
  FLOWIE_OWNER_MODEL_PACKET_IDS = 16,
  FLOWIE_OWNER_MODEL_HWM = 8,
};

typedef enum flowie_owner_model_operation_e {
  FLOWIE_OWNER_MODEL_BEGIN = 0,
  FLOWIE_OWNER_MODEL_SETTLE = 1,
  FLOWIE_OWNER_MODEL_RELEASE = 2,
  FLOWIE_OWNER_MODEL_INVALID_PACKET_ID = 3,
} flowie_owner_model_operation_t;

typedef struct flowie_owner_model_event_s {
  uint16_t packet_id;
  uint8_t operation;
  uint8_t qos;
} flowie_owner_model_event_t;

typedef struct flowie_owner_model_entry_s {
  flowie_session_publish_begin_result_t begin;
  uint8_t active;
  uint8_t qos;
  uint8_t pubrec_sent;
} flowie_owner_model_entry_t;

static int flowie_owner_model_replay(const flowie_owner_model_event_t *events, size_t event_count,
                                     size_t *failed_index) {
  static const uint8_t topic[] = "model/events";
  static const uint8_t payload[] = "x";
  flowie_owner_model_entry_t model[FLOWIE_OWNER_MODEL_PACKET_IDS + 1u];
  flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
  flowie_mqtt_connect_view_t connect =
      flowie_test_connect(FLOWIE_MQTT_VERSION_5, "state-model", 0, 60u);
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  flowie_session_owner_t *owner = NULL;
  size_t active_count = 0u;
  size_t step = 0u;
  int rc = TURBO_OK;
  if ((!events && event_count != 0u) || !failed_index) return TURBO_EINVAL;
  *failed_index = SIZE_MAX;
  memset(model, 0, sizeof(model));
  config.owner_instance_id = 101u;
  config.session_id = 103u;
  config.max_subscriptions = 4u;
  config.max_inflight = FLOWIE_OWNER_MODEL_HWM;
  config.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
  config.settlement.qos2 = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
  owner = flowie_session_owner_create(&config);
  if (!owner) return TURBO_ENOMEM;
  rc = flowie_session_owner_open(owner, &connect);
  if (rc != TURBO_OK) goto done;
  rc = flowie_session_owner_route(owner, &route);
  if (rc != TURBO_OK) goto done;

#define FLOWIE_OWNER_MODEL_REQUIRE(condition)                                                      \
  do {                                                                                             \
    if (!(condition)) {                                                                            \
      rc = TURBO_EPROTO;                                                                           \
      goto done;                                                                                   \
    }                                                                                              \
  } while (0)

  for (step = 0u; step < event_count; ++step) {
    const flowie_owner_model_event_t *event = &events[step];
    uint16_t packet_id = event->packet_id;
    flowie_owner_model_entry_t *entry;
    FLOWIE_OWNER_MODEL_REQUIRE(packet_id != 0u &&
                               packet_id <= FLOWIE_OWNER_MODEL_PACKET_IDS);
    entry = &model[packet_id];
    switch ((flowie_owner_model_operation_t)event->operation) {
      case FLOWIE_OWNER_MODEL_BEGIN: {
        flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
        flowie_session_publish_begin_result_t begin = FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
        publish.qos = entry->active ? entry->qos : event->qos;
        publish.packet_id = packet_id;
        publish.duplicate = entry->active;
        publish.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
        publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
        publish.properties =
            (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
        rc = flowie_session_owner_publish_begin(owner, &publish, &begin);
        if (entry->active) {
          FLOWIE_OWNER_MODEL_REQUIRE(rc == TURBO_OK && !begin.admit_graph);
          if (entry->pubrec_sent)
            FLOWIE_OWNER_MODEL_REQUIRE(begin.has_ack &&
                                       begin.ack.kind == FLOWIE_SESSION_ACK_PUBREC &&
                                       begin.ack.packet_id == packet_id);
        } else if (active_count == FLOWIE_OWNER_MODEL_HWM) {
          FLOWIE_OWNER_MODEL_REQUIRE(rc == TURBO_ENOSPC);
        } else {
          FLOWIE_OWNER_MODEL_REQUIRE(rc == TURBO_OK && begin.admit_graph && !begin.has_ack);
          entry->begin = begin;
          entry->active = 1u;
          entry->qos = publish.qos;
          entry->pubrec_sent = 0u;
          ++active_count;
        }
        break;
      }
      case FLOWIE_OWNER_MODEL_SETTLE:
        if (entry->active && !entry->pubrec_sent) {
          turbo_flow_protocol_settlement_request_t settlement =
              TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
          flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
          settlement.message = entry->begin.message.metadata;
          settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
          settlement.status = TURBO_OK;
          rc = flowie_session_owner_publish_settle(owner, &route, &settlement, &ack);
          FLOWIE_OWNER_MODEL_REQUIRE(rc == TURBO_OK && ack.packet_id == packet_id);
          if (entry->qos == 1u) {
            FLOWIE_OWNER_MODEL_REQUIRE(ack.kind == FLOWIE_SESSION_ACK_PUBACK);
            entry->active = 0u;
            --active_count;
          } else {
            FLOWIE_OWNER_MODEL_REQUIRE(ack.kind == FLOWIE_SESSION_ACK_PUBREC);
            entry->pubrec_sent = 1u;
          }
        }
        break;
      case FLOWIE_OWNER_MODEL_RELEASE: {
        flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
        rc = flowie_session_owner_qos2_release(owner, &route, packet_id, &ack);
        if (entry->active && entry->qos == 2u && entry->pubrec_sent) {
          FLOWIE_OWNER_MODEL_REQUIRE(rc == TURBO_OK && ack.kind == FLOWIE_SESSION_ACK_PUBCOMP &&
                                     ack.packet_id == packet_id);
          entry->active = 0u;
          --active_count;
        } else {
          FLOWIE_OWNER_MODEL_REQUIRE(rc != TURBO_OK && ack.kind == FLOWIE_SESSION_ACK_NONE);
        }
        break;
      }
      case FLOWIE_OWNER_MODEL_INVALID_PACKET_ID: {
        flowie_mqtt_publish_view_t invalid = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
        flowie_session_publish_begin_result_t begin = FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
        invalid.qos = 1u;
        invalid.packet_id = 0u;
        invalid.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
        invalid.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
        FLOWIE_OWNER_MODEL_REQUIRE(
            flowie_session_owner_publish_begin(owner, &invalid, &begin) != TURBO_OK);
        break;
      }
      default:
        FLOWIE_OWNER_MODEL_REQUIRE(0);
    }
    rc = flowie_session_owner_snapshot(owner, &snapshot);
    FLOWIE_OWNER_MODEL_REQUIRE(rc == TURBO_OK && snapshot.inflight_count == active_count &&
                               snapshot.inflight_count <= FLOWIE_OWNER_MODEL_HWM);
  }

done:
  if (rc != TURBO_OK) *failed_index = step;
  if (owner) {
    int close_rc = flowie_session_owner_close(owner);
    if (rc == TURBO_OK && close_rc != TURBO_OK) rc = close_rc;
    flowie_session_owner_destroy(owner);
  }
#undef FLOWIE_OWNER_MODEL_REQUIRE
  return rc;
}

static size_t flowie_owner_model_shrink(flowie_owner_model_event_t *events, size_t event_count,
                                        flowie_owner_model_event_t *candidate,
                                        size_t *failed_index) {
  size_t chunk = event_count / 2u;
  while (chunk != 0u && event_count != 0u) {
    int reduced = 0;
    for (size_t start = 0u; start < event_count; start += chunk) {
      size_t removed = chunk < event_count - start ? chunk : event_count - start;
      size_t candidate_count = event_count - removed;
      int rc;
      if (candidate_count == 0u) continue;
      memcpy(candidate, events, start * sizeof(*events));
      memcpy(candidate + start, events + start + removed,
             (event_count - start - removed) * sizeof(*events));
      rc = flowie_owner_model_replay(candidate, candidate_count, failed_index);
      if (rc == TURBO_EPROTO) {
        memcpy(events, candidate, candidate_count * sizeof(*events));
        event_count = candidate_count;
        reduced = 1;
        break;
      }
    }
    if (!reduced) chunk /= 2u;
    else if (chunk > event_count) chunk = event_count;
  }
  return event_count;
}

static void flowie_test_subscription_packet(flowie_mqtt_packet_view_t *packet,
                                            flowie_mqtt_subscribe_view_t *subscribe,
                                            const uint8_t *entries, size_t entries_size,
                                            size_t entry_count, uint16_t packet_id) {
  *packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  packet->version = FLOWIE_MQTT_VERSION_5;
  packet->type = FLOWIE_MQTT_PACKET_SUBSCRIBE;
  *subscribe = (flowie_mqtt_subscribe_view_t)FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
  subscribe->packet_id = packet_id;
  subscribe->entries = (flowie_mqtt_span_t){entries, entries_size};
  subscribe->entry_count = entry_count;
}

static void flowie_test_unsubscribe_packet(flowie_mqtt_packet_view_t *packet,
                                           flowie_mqtt_unsubscribe_view_t *unsubscribe,
                                           const uint8_t *filters, size_t filters_size,
                                           size_t filter_count, uint16_t packet_id) {
  *packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  packet->version = FLOWIE_MQTT_VERSION_5;
  packet->type = FLOWIE_MQTT_PACKET_UNSUBSCRIBE;
  *unsubscribe = (flowie_mqtt_unsubscribe_view_t)FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
  unsubscribe->packet_id = packet_id;
  unsubscribe->filters = (flowie_mqtt_span_t){filters, filters_size};
  unsubscribe->filter_count = filter_count;
}

spec("flowie internal session owner") {
  it("owns CONNECT acceptance, session-present, generation, and rejection policy") {
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "device-1", 0, 60u);
    flowie_session_connect_result_t result = FLOWIE_SESSION_CONNECT_RESULT_INIT;
    flowie_session_owner_t *owner;

    config.owner_instance_id = 7u;
    config.session_id = 11u;
    config.max_subscriptions = 8u;
    config.max_inflight = 8u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);

    check_int_eq(flowie_session_owner_connect(owner, &connect, &result), TURBO_OK);
    check_true(result.accepted);
    check_false(result.close_after_reply);
    check_false(result.session_present);
    check_uint_eq(result.reply.type, FLOWIE_MQTT_PACKET_CONNACK);
    check_uint_eq(result.reply.reason_code, 0u);
    check_uint_eq(result.route.session_generation, 1u);

    result = (flowie_session_connect_result_t)FLOWIE_SESSION_CONNECT_RESULT_INIT;
    check_int_eq(flowie_session_owner_connect(owner, &connect, &result), TURBO_OK);
    check_false(result.accepted);
    check_true(result.close_after_reply);
    check_uint_eq(result.reply.reason_code, 0x89u);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);

    result = (flowie_session_connect_result_t)FLOWIE_SESSION_CONNECT_RESULT_INIT;
    check_int_eq(flowie_session_owner_connect(owner, &connect, &result), TURBO_OK);
    check_true(result.accepted);
    check_true(result.session_present);
    check_true(result.reply.session_present);
    check_uint_eq(result.route.session_generation, 2u);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);

    connect = flowie_test_connect(FLOWIE_MQTT_VERSION_5, "", 1, 0u);
    result = (flowie_session_connect_result_t)FLOWIE_SESSION_CONNECT_RESULT_INIT;
    check_int_eq(flowie_session_owner_connect(owner, &connect, &result), TURBO_OK);
    check_false(result.accepted);
    check_true(result.close_after_reply);
    check_uint_eq(result.reply.reason_code, 0x85u);
    flowie_session_owner_destroy(owner);
  }

  it("restores MQTT 3.1 state without emitting the later Session Present flag") {
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_3_1, "legacy-device", 0, 0u);
    flowie_session_connect_result_t result = FLOWIE_SESSION_CONNECT_RESULT_INIT;
    flowie_session_owner_t *owner;

    config.owner_instance_id = 8u;
    config.session_id = 12u;
    config.max_subscriptions = 8u;
    config.max_inflight = 8u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);

    check_int_eq(flowie_session_owner_connect(owner, &connect, &result), TURBO_OK);
    check_true(result.accepted);
    check_false(result.session_present);
    check_false(result.reply.session_present);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);

    result = (flowie_session_connect_result_t)FLOWIE_SESSION_CONNECT_RESULT_INIT;
    check_int_eq(flowie_session_owner_connect(owner, &connect, &result), TURBO_OK);
    check_true(result.accepted);
    check_true(result.session_present);
    check_false(result.reply.session_present);
    check_uint_eq(result.reply.version, FLOWIE_MQTT_VERSION_3_1);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-003 fences stale routes across persistent reconnect generations") {
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "device-1", 0, 60u);
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    turbo_flow_protocol_route_t first = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    turbo_flow_protocol_route_t second = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    flowie_session_owner_t *owner;

    config.owner_instance_id = 7u;
    config.session_id = 9u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_EALREADY);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_int_eq(snapshot.active, 1);
    check_uint_eq(snapshot.session_expiry_interval, 60u);
    check_size_eq(snapshot.client_id.size, strlen("device-1"));
    check_int_eq(flowie_session_owner_route(owner, &first), TURBO_OK);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_route(owner, &second), TURBO_EBUSY);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_route(owner, &second), TURBO_OK);
    check_uint_eq(second.session_generation, first.session_generation + 1u);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-005 applies SUBSCRIBE atomically at the owner quota") {
    static const uint8_t entries[] = {0x00, 0x05, 'a', '/', '+', '/', 'c', 0x01,
                                      0x00, 0x0d, '$', 's', 'h', 'a', 'r', 'e',
                                      '/',  'g',  '/', 'j', 'o', 'b', 's', 0x02};
    static const uint8_t overflow[] = {0x00, 0x08, 'o', 'v', 'e', 'r', 'f', 'l', 'o', 'w', 0x00};
    static const uint8_t update[] = {0x00, 0x05, 'a', '/', '+', '/', 'c', 0x02};
    static const uint8_t remove[] = {0x00, 0x05, 'a', '/', '+', '/', 'c', 0x00,
                                     0x07, 'm',  'i', 's', 's', 'i', 'n', 'g'};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "device-2", 0, 120u);
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    flowie_session_subscribe_result_t result = FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
    flowie_session_unsubscribe_result_t unsubscribe_result = FLOWIE_SESSION_UNSUBSCRIBE_RESULT_INIT;
    flowie_session_subscription_t subscription = FLOWIE_SESSION_SUBSCRIPTION_INIT;
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;
    uint8_t reasons[2] = {0xa5u, 0xa5u};

    config.owner_instance_id = 11u;
    config.session_id = 13u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);

    flowie_test_subscription_packet(&packet, &subscribe, entries, sizeof(entries), 2u, 41u);
    check_int_eq(flowie_session_owner_subscribe(owner, &packet, &subscribe, &result), TURBO_OK);
    check_uint_eq(result.packet_id, 41u);
    check_size_eq(result.accepted_count, 2u);
    check_int_eq(result.changed, 1);
    check_int_eq(flowie_session_owner_subscription_at(owner, 0u, &subscription), TURBO_OK);
    check_size_eq(subscription.filter.size, 5u);
    check_int_eq(subscription.qos, 1);
    check_int_eq(flowie_session_owner_snapshot(owner, &before), TURBO_OK);

    flowie_test_subscription_packet(&packet, &subscribe, overflow, sizeof(overflow), 1u, 42u);
    result = (flowie_session_subscribe_result_t)FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
    check_int_eq(flowie_session_owner_subscribe(owner, &packet, &subscribe, &result), TURBO_ENOSPC);
    check_int_eq(flowie_session_owner_snapshot(owner, &after), TURBO_OK);
    check_size_eq(after.subscription_count, before.subscription_count);
    check_uint_eq(after.resource_generation, before.resource_generation);

    flowie_test_subscription_packet(&packet, &subscribe, update, sizeof(update), 1u, 43u);
    result = (flowie_session_subscribe_result_t)FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
    check_int_eq(flowie_session_owner_subscribe(owner, &packet, &subscribe, &result), TURBO_OK);
    check_int_eq(flowie_session_owner_subscription_at(owner, 0u, &subscription), TURBO_OK);
    check_int_eq(subscription.qos, 2);
    check_int_eq(flowie_session_owner_snapshot(owner, &after), TURBO_OK);
    check_size_eq(after.subscription_count, 2u);
    check_size_eq(after.inflight_count, 0u);

    flowie_test_unsubscribe_packet(&packet, &unsubscribe, remove, sizeof(remove), 2u, 44u);
    check_int_eq(flowie_session_owner_snapshot(owner, &before), TURBO_OK);
    check_int_eq(flowie_session_owner_unsubscribe(owner, &packet, &unsubscribe, reasons, 1u,
                                                  &unsubscribe_result),
                 TURBO_ENOSPC);
    check_uint_eq(reasons[0], 0xa5u);
    check_int_eq(flowie_session_owner_snapshot(owner, &after), TURBO_OK);
    check_size_eq(after.subscription_count, before.subscription_count);
    check_uint_eq(after.resource_generation, before.resource_generation);

    check_int_eq(flowie_session_owner_unsubscribe(owner, &packet, &unsubscribe, reasons,
                                                  sizeof(reasons), &unsubscribe_result),
                 TURBO_OK);
    check_uint_eq(unsubscribe_result.packet_id, 44u);
    check_size_eq(unsubscribe_result.filter_count, 2u);
    check_size_eq(unsubscribe_result.removed_count, 1u);
    check_true(unsubscribe_result.changed);
    check_uint_eq(reasons[0], 0x00u);
    check_uint_eq(reasons[1], 0x11u);
    check_int_eq(flowie_session_owner_snapshot(owner, &after), TURBO_OK);
    check_size_eq(after.subscription_count, 1u);

    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-007 clears persistent state only at an explicit clean-session boundary") {
    static const uint8_t entry[] = {0x00, 0x03, 'a', '/', '#', 0x00};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t persistent =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "device-3", 0, 30u);
    flowie_mqtt_connect_view_t clean =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "device-3", 1, 0u);
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    flowie_session_subscribe_result_t result = FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;

    config.owner_instance_id = 17u;
    config.session_id = 19u;
    config.max_subscriptions = 4u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &persistent), TURBO_OK);
    flowie_test_subscription_packet(&packet, &subscribe, entry, sizeof(entry), 1u, 51u);
    check_int_eq(flowie_session_owner_subscribe(owner, &packet, &subscribe, &result), TURBO_OK);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.subscription_count, 1u);
    check_int_eq(flowie_session_owner_open(owner, &clean), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.subscription_count, 0u);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-001/002 MQTT-STORE-004 separates settlement from QoS ACK state") {
    static const uint8_t topic[] = "devices/1/events";
    static const uint8_t payload[] = "value";
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "device-4", 0, 60u);
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_session_publish_begin_result_t begin = FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
    flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
    turbo_flow_protocol_settlement_request_t settlement =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
    turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;

    config.owner_instance_id = 23u;
    config.session_id = 29u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    config.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    config.settlement.qos2 = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_route(owner, &route), TURBO_OK);

    publish.qos = 1u;
    publish.packet_id = 71u;
    publish.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
    publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
    publish.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    check_int_eq(flowie_session_owner_publish_begin(owner, &publish, &begin), TURBO_OK);
    check_int_eq(begin.admit_graph, 1);
    check_int_eq(begin.has_ack, 0);
    settlement.message = begin.message.metadata;
    settlement.status = TURBO_EIO;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack), TURBO_EIO);
    check_int_eq(ack.kind, FLOWIE_SESSION_ACK_NONE);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight_count, 1u);
    settlement.status = TURBO_OK;
    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack),
                 TURBO_EBUSY);
    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack), TURBO_OK);
    check_int_eq(ack.kind, FLOWIE_SESSION_ACK_PUBACK);
    check_uint_eq(ack.packet_id, 71u);
    {
      static const uint8_t expected[] = {0x40u, 0x02u, 0x00u, 0x47u};
      flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
      uint8_t encoded[8];
      size_t written = 0u;
      check_int_eq(flowie_session_ack_control_packet(&ack, FLOWIE_MQTT_VERSION_5, &control),
                   TURBO_OK);
      check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                   FLOWIE_MQTT_PARSE_OK);
      check_size_eq(written, sizeof(expected));
      check_mem_eq(encoded, expected, sizeof(expected));
    }

    publish.packet_id = 73u;
    begin = (flowie_session_publish_begin_result_t)FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
    check_int_eq(flowie_session_owner_publish_begin(owner, &publish, &begin), TURBO_OK);
    settlement.message = begin.message.metadata;
    settlement.status = TURBO_ETIMEDOUT;
    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    ack = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack),
                 TURBO_ETIMEDOUT);
    check_int_eq(ack.kind, FLOWIE_SESSION_ACK_NONE);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight_count, 1u);
    settlement.status = TURBO_OK;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack), TURBO_OK);
    check_int_eq(ack.kind, FLOWIE_SESSION_ACK_PUBACK);
    check_uint_eq(ack.packet_id, 73u);

    publish.qos = 2u;
    publish.packet_id = 72u;
    begin = (flowie_session_publish_begin_result_t)FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
    check_int_eq(flowie_session_owner_publish_begin(owner, &publish, &begin), TURBO_OK);
    settlement.message = begin.message.metadata;
    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
    ack = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack),
                 TURBO_EBUSY);
    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &ack), TURBO_OK);
    check_int_eq(ack.kind, FLOWIE_SESSION_ACK_PUBREC);

    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    ack = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_session_owner_qos2_release(owner, &route, 72u, &ack), TURBO_EBUSY);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    route = (turbo_flow_protocol_route_t)TURBO_FLOW_PROTOCOL_ROUTE_INIT;
    check_int_eq(flowie_session_owner_route(owner, &route), TURBO_OK);
    publish.duplicate = 1u;
    begin = (flowie_session_publish_begin_result_t)FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
    check_int_eq(flowie_session_owner_publish_begin(owner, &publish, &begin), TURBO_OK);
    check_int_eq(begin.admit_graph, 0);
    check_int_eq(begin.has_ack, 1);
    check_int_eq(begin.ack.kind, FLOWIE_SESSION_ACK_PUBREC);
    ack = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_session_owner_qos2_release(owner, &route, 72u, &ack), TURBO_OK);
    check_int_eq(ack.kind, FLOWIE_SESSION_ACK_PUBCOMP);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight_count, 0u);

    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-002/004 MQTT-STORE-004 owns QoS retransmission stages across restore") {
    static const uint8_t publish_qos1[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t publish_qos2[] = {0x34u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubrec[] = {0x50u, 0x02u, 0x00u, 0x02u};
    static const uint8_t pubcomp[] = {0x70u, 0x02u, 0x00u, 0x02u};
    static const uint8_t expected_pubrel[] = {0x62u, 0x02u, 0x00u, 0x02u};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "subscriber", 0, 60u);
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_session_ack_intent_t reply = FLOWIE_SESSION_ACK_INTENT_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_mqtt_span_t pending = {0};
    flowie_session_owner_t *owner;
    uint16_t qos1_id = 0u;
    uint16_t qos2_id = 0u;
    uint16_t rejected_id = 0u;

    config.owner_instance_id = 31u;
    config.session_id = 37u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &qos1_id), TURBO_OK);
    check_uint_eq(qos1_id, 1u);
    check_int_eq(flowie_session_owner_delivery_commit(
                     owner, qos1_id, (flowie_mqtt_span_t){publish_qos1, sizeof(publish_qos1)}, 0u),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 2u, &qos2_id), TURBO_OK);
    check_uint_eq(qos2_id, 2u);
    check_int_eq(flowie_session_owner_delivery_commit(
                     owner, qos2_id, (flowie_mqtt_span_t){publish_qos2, sizeof(publish_qos2)}, 0u),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &rejected_id), TURBO_ENOSPC);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight_count, 2u);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_size_eq(pending.size, sizeof(publish_qos1));
    check_uint_eq(pending.data[0], 0x3au);

    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(puback, sizeof(puback), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_session_owner_delivery_ack(owner, &packet, &reply), TURBO_OK);
    check_int_eq(reply.kind, FLOWIE_SESSION_ACK_NONE);

    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    reply = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_mqtt_packet_parse(pubrec, sizeof(pubrec), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_session_owner_delivery_ack(owner, &packet, &reply), TURBO_OK);
    check_int_eq(reply.kind, FLOWIE_SESSION_ACK_PUBREL);
    check_uint_eq(reply.packet_id, qos2_id);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_size_eq(pending.size, sizeof(expected_pubrel));
    check_mem_eq(pending.data, expected_pubrel, sizeof(expected_pubrel));

    reply = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_session_owner_delivery_ack(owner, &packet, &reply), TURBO_OK);
    check_int_eq(reply.kind, FLOWIE_SESSION_ACK_PUBREL);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_mem_eq(pending.data, expected_pubrel, sizeof(expected_pubrel));

    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    reply = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_mqtt_packet_parse(pubcomp, sizeof(pubcomp), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_session_owner_delivery_ack(owner, &packet, &reply), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight_count, 0u);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-STORE-004 restores every committed inbound and outbound QoS 2 stage") {
    static const uint8_t outbound_publish[] = {0x34u, 0x07u, 0x00u, 0x01u, 'a',
                                               0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t pubrec[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubcomp[] = {0x70u, 0x02u, 0x00u, 0x01u};
    static const uint8_t expected_pubrel[] = {0x62u, 0x02u, 0x00u, 0x01u};
    static const uint8_t topic[] = "qos2/restore";
    static const uint8_t payload[] = "value";
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "qos2-checkpoints", 0, 60u);
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_session_ack_intent_t reply = FLOWIE_SESSION_ACK_INTENT_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_mqtt_span_t pending = {0};
    flowie_session_owner_t *owner;
    flowie_session_owner_t *restored = NULL;
    uint16_t packet_id = 0u;

    config.owner_instance_id = 101u;
    config.session_id = 103u;
    config.max_subscriptions = 2u;
    config.max_inflight = 4u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 2u, &packet_id), TURBO_OK);
    check_uint_eq(packet_id, 1u);
    check_int_eq(flowie_session_owner_delivery_commit(
                     owner, packet_id,
                     (flowie_mqtt_span_t){outbound_publish, sizeof(outbound_publish)}, 0u),
                 TURBO_OK);

    /* Committed PUBLISH: restore retransmits the one owned packet with DUP set. */
    check_int_eq(flowie_test_owner_round_trip(owner, &config, connect.client_id, 107u, &restored),
                 TURBO_OK);
    flowie_session_owner_destroy(owner);
    owner = restored;
    restored = NULL;
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_size_eq(pending.size, sizeof(outbound_publish));
    check_uint_eq(pending.data[0], 0x3cu);

    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(pubrec, sizeof(pubrec), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_session_owner_delivery_ack(owner, &packet, &reply), TURBO_OK);
    check_int_eq(reply.kind, FLOWIE_SESSION_ACK_PUBREL);

    /* Committed PUBREC and emitted PUBREL share one durable WAIT_PUBCOMP state. */
    check_int_eq(flowie_test_owner_round_trip(owner, &config, connect.client_id, 109u, &restored),
                 TURBO_OK);
    flowie_session_owner_destroy(owner);
    owner = restored;
    restored = NULL;
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_mem_eq(pending.data, expected_pubrel, sizeof(expected_pubrel));
    check_int_eq(flowie_test_owner_round_trip(owner, &config, connect.client_id, 113u, &restored),
                 TURBO_OK);
    flowie_session_owner_destroy(owner);
    owner = restored;
    restored = NULL;
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_mem_eq(pending.data, expected_pubrel, sizeof(expected_pubrel));

    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    reply = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
    check_int_eq(flowie_mqtt_packet_parse(pubcomp, sizeof(pubcomp), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_session_owner_delivery_ack(owner, &packet, &reply), TURBO_OK);
    check_int_eq(flowie_test_owner_round_trip(owner, &config, connect.client_id, 127u, &restored),
                 TURBO_OK);
    flowie_session_owner_destroy(owner);
    owner = restored;
    restored = NULL;
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight_count, 0u);
    flowie_session_owner_destroy(owner);

    /* Inbound QoS 2: a committed PUBLISH restores PUBREC without graph re-admission. */
    config.owner_instance_id = 131u;
    config.session_id = 137u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    {
      flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
      flowie_session_publish_begin_result_t begin = FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
      turbo_flow_protocol_settlement_request_t settlement =
          TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
      turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
      publish.qos = 2u;
      publish.packet_id = 77u;
      publish.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
      publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
      check_int_eq(flowie_session_owner_route(owner, &route), TURBO_OK);
      check_int_eq(flowie_session_owner_publish_begin(owner, &publish, &begin), TURBO_OK);
      check_true(begin.admit_graph);
      settlement.message = begin.message.metadata;
      settlement.status = TURBO_OK;
      settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
      check_int_eq(flowie_session_owner_publish_settle(owner, &route, &settlement, &reply),
                   TURBO_OK);
      check_int_eq(reply.kind, FLOWIE_SESSION_ACK_PUBREC);
      check_int_eq(flowie_test_owner_round_trip(owner, &config, connect.client_id, 139u, &restored),
                   TURBO_OK);
      flowie_session_owner_destroy(owner);
      owner = restored;
      restored = NULL;
      check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
      publish.duplicate = 1u;
      begin = (flowie_session_publish_begin_result_t)FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
      check_int_eq(flowie_session_owner_publish_begin(owner, &publish, &begin), TURBO_OK);
      check_false(begin.admit_graph);
      check_true(begin.has_ack);
      check_int_eq(begin.ack.kind, FLOWIE_SESSION_ACK_PUBREC);
      route = (turbo_flow_protocol_route_t)TURBO_FLOW_PROTOCOL_ROUTE_INIT;
      check_int_eq(flowie_session_owner_route(owner, &route), TURBO_OK);
      reply = (flowie_session_ack_intent_t)FLOWIE_SESSION_ACK_INTENT_INIT;
      check_int_eq(flowie_session_owner_qos2_release(owner, &route, 77u, &reply), TURBO_OK);
      check_int_eq(reply.kind, FLOWIE_SESSION_ACK_PUBCOMP);
      check_int_eq(flowie_test_owner_round_trip(owner, &config, connect.client_id, 149u, &restored),
                   TURBO_OK);
      flowie_session_owner_destroy(owner);
      owner = restored;
      restored = NULL;
      check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
      check_size_eq(snapshot.inflight_count, 0u);
    }
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-007 queues offline persistent delivery before retransmission") {
    static const uint8_t publish_qos1[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x01u, 0x00u, 'x'};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_session_config_t restored_config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "offline-subscriber", 0, 60u);
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;
    flowie_session_owner_t *restored = NULL;
    flowie_mqtt_span_t pending = {0};
    uint8_t *record;
    size_t record_size = 0u;
    uint16_t packet_id = 0u;

    config.owner_instance_id = 41u;
    config.session_id = 43u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.session_expiry_interval, 60u);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &packet_id), TURBO_OK);
    check_uint_eq(packet_id, 1u);
    check_int_eq(
        flowie_session_owner_delivery_commit_queued(
            owner, packet_id, (flowie_mqtt_span_t){publish_qos1, sizeof(publish_qos1)}, 0u),
        TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    record = (uint8_t *)malloc(record_size);
    check_not_null(record);
    check_int_eq(flowie_session_owner_record_encode(owner, record, record_size, &record_size),
                 TURBO_OK);
    restored_config = config;
    restored_config.owner_instance_id = 47u;
    restored_config.session_id = 1u;
    check_int_eq(flowie_session_owner_record_restore(
                     &restored_config,
                     (flowie_mqtt_span_t){connect.client_id.data, connect.client_id.size},
                     snapshot.resource_generation, record, record_size, &restored),
                 TURBO_OK);
    free(record);
    check_not_null(restored);
    check_int_eq(flowie_session_owner_open(restored, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at(restored, 0u, &pending), TURBO_OK);
    check_uint_eq(pending.data[0], 0x3au);
    check_int_eq(flowie_session_owner_close(restored), TURBO_OK);
    flowie_session_owner_destroy(restored);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_uint_eq(pending.data[0], 0x32u);
    check_int_eq(flowie_session_owner_delivery_pending_at(owner, 0u, &pending), TURBO_OK);
    check_uint_eq(pending.data[0], 0x3au);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);
  }

  it("round trips canonical durable session records without restoring live routes") {
    static const uint8_t publish_qos1[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t subscription_entry[] = {0x00u, 0x09u, 'd', 'u', 'r', 'a',
                                                 'b',   'l',   'e', '/', '#', 0x01u};
    static const uint8_t subscription_properties[] = {FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER,
                                                      0x2au};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_session_config_t restored_config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "durable-client", 0, 60u);
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t restored_snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_mqtt_packet_view_t subscribe_packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    flowie_session_subscribe_result_t subscribe_result = FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
    flowie_session_subscription_t restored_subscription = FLOWIE_SESSION_SUBSCRIPTION_INIT;
    flowie_mqtt_property_iterator_t property_iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    flowie_mqtt_subscription_iterator_t subscription_iterator =
        FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
    flowie_mqtt_subscription_view_t subscription_view = {0};
    flowie_session_owner_t *owner;
    flowie_session_owner_t *clone;
    flowie_session_owner_t *restored = NULL;
    flowie_mqtt_span_t pending = {0};
    uint8_t *record;
    size_t record_size = 0u;
    uint16_t packet_id = 0u;

    config.owner_instance_id = 51u;
    config.session_id = 57u;
    config.max_subscriptions = 4u;
    config.max_inflight = 4u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    flowie_test_subscription_packet(&subscribe_packet, &subscribe, subscription_entry,
                                    sizeof(subscription_entry), 1u, 17u);
    subscribe.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    subscribe.properties.values =
        (flowie_mqtt_span_t){subscription_properties, sizeof(subscription_properties)};
    check_int_eq(flowie_mqtt_property_iterator_init(&subscribe.properties, &property_iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&property_iterator, &property),
                 FLOWIE_MQTT_PARSE_OK);
    check_uint_eq(property.integer, 42u);
    check_int_eq(flowie_mqtt_property_iterator_next(&property_iterator, &property),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
    check_int_eq(flowie_mqtt_subscription_iterator_init(&subscribe_packet, &subscribe,
                                                        &subscription_iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscription_iterator_next(&subscription_iterator, &subscription_view),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscription_iterator_next(&subscription_iterator, &subscription_view),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
    check_int_eq(
        flowie_session_owner_subscribe(owner, &subscribe_packet, &subscribe, &subscribe_result),
        TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &packet_id), TURBO_OK);
    check_int_eq(
        flowie_session_owner_delivery_commit(
            owner, packet_id, (flowie_mqtt_span_t){publish_qos1, sizeof(publish_qos1)}, 0u),
        TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    clone = flowie_session_owner_clone(owner);
    check_not_null(clone);
    check_int_eq(flowie_session_owner_snapshot(clone, &restored_snapshot), TURBO_OK);
    check_uint_eq(restored_snapshot.resource_generation, snapshot.resource_generation);
    check_int_eq(flowie_session_owner_delivery_pending_at(clone, 0u, &pending), TURBO_OK);
    check_size_eq(pending.size, sizeof(publish_qos1));
    check_mem_eq(pending.data + 1u, publish_qos1 + 1u, sizeof(publish_qos1) - 1u);
    flowie_session_owner_destroy(clone);

    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    check_true(record_size > 0u);
    record = (uint8_t *)malloc(record_size);
    check_not_null(record);
    check_int_eq(flowie_session_owner_record_encode(owner, record, record_size, &record_size),
                 TURBO_OK);
    restored_config = config;
    restored_config.owner_instance_id = 61u;
    restored_config.session_id = 1u;
    check_int_eq(flowie_session_owner_record_restore(
                     &restored_config,
                     (flowie_mqtt_span_t){connect.client_id.data, connect.client_id.size},
                     snapshot.resource_generation, record, record_size, &restored),
                 TURBO_OK);
    check_not_null(restored);
    check_int_eq(flowie_session_owner_snapshot(restored, &restored_snapshot), TURBO_OK);
    check_false(restored_snapshot.active);
    check_uint_eq(restored_snapshot.owner_instance_id, restored_config.owner_instance_id);
    check_uint_eq(restored_snapshot.session_id, snapshot.session_id);
    check_uint_eq(restored_snapshot.session_generation, snapshot.session_generation);
    check_uint_eq(restored_snapshot.resource_generation, snapshot.resource_generation);
    check_size_eq(restored_snapshot.inflight_count, 1u);
    check_size_eq(restored_snapshot.subscription_count, 1u);
    check_int_eq(flowie_session_owner_subscription_at(restored, 0u, &restored_subscription),
                 TURBO_OK);
    check_uint_eq(restored_subscription.subscription_identifier, 42u);
    check_int_eq(flowie_session_owner_delivery_pending_at(restored, 0u, &pending), TURBO_OK);
    check_uint_eq(pending.data[0], 0x3au);
    flowie_session_owner_destroy(restored);
    restored = NULL;
    record[0] ^= 1u;
    check_int_eq(flowie_session_owner_record_restore(
                     &restored_config,
                     (flowie_mqtt_span_t){connect.client_id.data, connect.client_id.size},
                     snapshot.resource_generation, record, record_size, &restored),
                 TURBO_EPROTO);
    check_null(restored);
    free(record);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-011 persists absolute delivery expiry and restores legacy records") {
    static const uint8_t expiring_publish[] = {
        0x32u, 0x0cu, 0x00u,
        0x01u, 'a',   0x00u,
        0x01u, 0x05u, FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
        0x00u, 0x00u, 0x00u,
        0x0au, 'x'};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_session_config_t restored_config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "expiry-delivery", 0, 60u);
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;
    flowie_session_owner_t *restored = NULL;
    turbo_ltv_message_t *header_message = NULL;
    flowie_mqtt_span_t pending = {0};
    uint8_t legacy_record[128];
    uint8_t *record;
    const uint8_t *header;
    uint64_t expiry_at = 0u;
    size_t legacy_size;
    size_t record_size = 0u;
    size_t removed_count = 0u;
    uint16_t packet_id = 0u;
    uint16_t pending_packet_id = 0u;

    config.owner_instance_id = 67u;
    config.session_id = 71u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &packet_id), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_commit_queued(
                     owner, packet_id,
                     (flowie_mqtt_span_t){expiring_publish, sizeof(expiring_publish)}, 110u),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &before), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_pending_at_ex(owner, 0u, 105u, &pending,
                                                             &pending_packet_id, &expiry_at),
                 TURBO_OK);
    check_uint_eq(pending_packet_id, packet_id);
    check_uint_eq(expiry_at, 110u);
    check_uint_eq(pending.data[9], 0u);
    check_uint_eq(pending.data[10], 0u);
    check_uint_eq(pending.data[11], 0u);
    check_uint_eq(pending.data[12], 5u);

    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    record = (uint8_t *)malloc(record_size);
    check_not_null(record);
    check_int_eq(flowie_session_owner_record_encode(owner, record, record_size, &record_size),
                 TURBO_OK);
    check_int_eq(turbo_parse_ltv(record, record_size, &header_message), TURBO_OK);
    check_not_null(header_message);
    check_uint_eq(turbo_ltv_type(header_message), 1u);
    check_size_eq(turbo_ltv_value_len(header_message), 8u);
    header = turbo_ltv_value(header_message);
    check_mem_eq(header, "FSES", 4u);
    check_uint_eq(header[5], 1u);
    check_uint_eq(header[7], 3u);
    turbo_free_ltv(&header_message);

    restored_config = config;
    restored_config.owner_instance_id = 73u;
    restored_config.session_id = 1u;
    check_int_eq(flowie_session_owner_record_restore(&restored_config, connect.client_id,
                                                     before.resource_generation, record,
                                                     record_size, &restored),
                 TURBO_OK);
    free(record);
    check_not_null(restored);
    check_int_eq(flowie_session_owner_delivery_pending_at_ex(restored, 0u, 107u, &pending,
                                                             &pending_packet_id, &expiry_at),
                 TURBO_OK);
    check_uint_eq(expiry_at, 110u);
    check_uint_eq(pending.data[12], 3u);
    check_int_eq(flowie_session_owner_snapshot(restored, &before), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_expire(restored, 110u, &removed_count), TURBO_OK);
    check_size_eq(removed_count, 1u);
    check_int_eq(flowie_session_owner_snapshot(restored, &after), TURBO_OK);
    check_uint_eq(after.resource_generation, before.resource_generation + 1u);
    check_size_eq(after.inflight_count, 0u);
    flowie_session_owner_destroy(restored);
    restored = NULL;

    legacy_size = flowie_test_legacy_delivery_record(2u, expiring_publish, sizeof(expiring_publish),
                                                     legacy_record, sizeof(legacy_record));
    check_true(legacy_size != 0u);
    check_int_eq(
        flowie_session_owner_record_restore(
            &restored_config,
            (flowie_mqtt_span_t){(const uint8_t *)"legacy-expiry", sizeof("legacy-expiry") - 1u},
            2u, legacy_record, legacy_size, &restored),
        TURBO_OK);
    check_not_null(restored);
    check_int_eq(flowie_session_owner_delivery_pending_at(restored, 0u, &pending), TURBO_OK);
    check_uint_eq(pending.data[12], 10u);
    flowie_session_owner_destroy(restored);
    restored = NULL;

    legacy_size = flowie_test_legacy_delivery_record(3u, expiring_publish, sizeof(expiring_publish),
                                                     legacy_record, sizeof(legacy_record));
    check_true(legacy_size != 0u);
    check_int_eq(
        flowie_session_owner_record_restore(
            &restored_config,
            (flowie_mqtt_span_t){(const uint8_t *)"legacy-expiry", sizeof("legacy-expiry") - 1u},
            2u, legacy_record, legacy_size, &restored),
        TURBO_EPROTO);
    check_null(restored);
    flowie_session_owner_destroy(owner);
  }

  it("rejects durable expiry metadata when the PUBLISH has no expiry property") {
    static const uint8_t publish_qos1[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x01u, 0x00u, 'x'};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "invalid-expiry", 0, 60u);
    flowie_session_owner_t *owner;
    size_t record_size = 0u;
    uint16_t packet_id = 0u;
    config.owner_instance_id = 79u;
    config.session_id = 83u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &packet_id), TURBO_OK);
    check_int_eq(
        flowie_session_owner_delivery_commit(
            owner, packet_id, (flowie_mqtt_span_t){publish_qos1, sizeof(publish_qos1)}, 110u),
        TURBO_EPROTO);
    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-011 applies broker expiry to MQTT 3 delivery without a wire property") {
    static const uint8_t publish_qos1[] = {0x32u, 0x06u, 0x00u, 0x01u, 'a', 0x00u, 0x01u, 'x'};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_3_1_1, "v3-expiry", 0, 0u);
    flowie_session_owner_t *owner;
    flowie_mqtt_span_t pending = {0};
    uint64_t expiry_at = 0u;
    size_t record_size = 0u;
    size_t removed_count = 0u;
    uint16_t packet_id = 0u;
    uint16_t pending_packet_id = 0u;
    config.owner_instance_id = 89u;
    config.session_id = 97u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_delivery_reserve(owner, 1u, &packet_id), TURBO_OK);
    check_int_eq(
        flowie_session_owner_delivery_commit(
            owner, packet_id, (flowie_mqtt_span_t){publish_qos1, sizeof(publish_qos1)}, 110u),
        TURBO_OK);
    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    check_true(record_size != 0u);
    check_int_eq(flowie_session_owner_delivery_pending_at_ex(owner, 0u, 107u, &pending,
                                                             &pending_packet_id, &expiry_at),
                 TURBO_OK);
    check_uint_eq(pending_packet_id, packet_id);
    check_uint_eq(expiry_at, 110u);
    check_size_eq(pending.size, sizeof(publish_qos1));
    check_uint_eq(pending.data[0], 0x3au);
    check_mem_eq(pending.data + 1u, publish_qos1 + 1u, sizeof(publish_qos1) - 1u);
    check_int_eq(flowie_session_owner_delivery_expire(owner, 110u, &removed_count), TURBO_OK);
    check_size_eq(removed_count, 1u);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-OWNER-008 MQTT-STORE-005/009 persists binary Will flags and lifecycle state") {
    static const uint8_t will_properties[] = {FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL, 0x00u,
                                              0x00u, 0x00u, 0x02u};
    uint8_t will_topic[] = "status/device";
    uint8_t will_payload[] = {0x00u, 0xffu, 0x7fu};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "will-owner", 0, 60u);
    flowie_mqtt_connect_view_t reconnect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "will-owner", 0, 60u);
    flowie_mqtt_control_packet_view_t disconnect = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t restored_snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;
    flowie_session_owner_t *restored = NULL;
    uint8_t *record = NULL;
    size_t record_size = 0u;
    uint64_t generation;
    config.owner_instance_id = 71u;
    config.session_id = 73u;
    config.max_subscriptions = 4u;
    config.max_inflight = 4u;
    connect.will_qos = 2u;
    connect.will_retain = 1u;
    connect.will_topic = (flowie_mqtt_span_t){will_topic, sizeof(will_topic) - 1u};
    connect.will_payload = (flowie_mqtt_span_t){will_payload, sizeof(will_payload)};
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties.values = (flowie_mqtt_span_t){will_properties, sizeof(will_properties)};
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    will_topic[0] = 'X';
    will_payload[0] = 0x55u;
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_true(snapshot.has_will);
    check_false(snapshot.will_pending);
    check_uint_eq(snapshot.will_qos, 2u);
    check_true(snapshot.will_retain);
    check_uint_eq(snapshot.will_delay_interval, 2u);
    check_size_eq(snapshot.will_topic.size, sizeof(will_topic) - 1u);
    check_mem_eq(snapshot.will_topic.data, "status/device", sizeof(will_topic) - 1u);
    check_mem_eq(snapshot.will_payload.data, "\x00\xff\x7f", sizeof(will_payload));
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_true(snapshot.will_pending);
    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    record = (uint8_t *)malloc(record_size);
    check_not_null(record);
    check_int_eq(flowie_session_owner_record_encode(owner, record, record_size, &record_size),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_record_restore(&config, reconnect.client_id,
                                                     snapshot.resource_generation, record,
                                                     record_size, &restored),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(restored, &restored_snapshot), TURBO_OK);
    check_true(restored_snapshot.will_pending);
    check_uint_eq(restored_snapshot.will_delay_interval, 2u);
    generation = restored_snapshot.resource_generation;
    check_int_eq(flowie_session_owner_will_complete(restored), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(restored, &restored_snapshot), TURBO_OK);
    check_false(restored_snapshot.has_will);
    check_false(restored_snapshot.will_pending);
    check_uint_eq(restored_snapshot.resource_generation, generation + 1u);
    check_int_eq(flowie_session_owner_will_complete(restored), TURBO_ENOENT);
    flowie_session_owner_destroy(restored);
    restored = NULL;
    free(record);
    record = NULL;

    check_int_eq(flowie_session_owner_open(owner, &reconnect), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_false(snapshot.has_will);
    check_false(snapshot.will_pending);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);

    connect.will_topic.data = (const uint8_t *)"status/device";
    connect.will_payload.data = (const uint8_t *)"offline";
    connect.will_payload.size = strlen("offline");
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    disconnect.version = FLOWIE_MQTT_VERSION_5;
    disconnect.type = FLOWIE_MQTT_PACKET_DISCONNECT;
    disconnect.reason_code = 0u;
    disconnect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    disconnect.properties.values.data = will_properties;
    check_int_eq(flowie_session_owner_disconnect(owner, &disconnect), TURBO_OK);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_false(snapshot.has_will);
    check_false(snapshot.will_pending);

    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    disconnect.reason_code = 0x04u;
    check_int_eq(flowie_session_owner_disconnect(owner, &disconnect), TURBO_OK);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_true(snapshot.has_will);
    check_true(snapshot.will_pending);
    flowie_session_owner_destroy(owner);
  }

  it("applies the MQTT 5 DISCONNECT session-expiry override before close") {
    static const uint8_t expiry_one[] = {FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL, 0x00u, 0x00u,
                                         0x00u, 0x01u};
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect =
        flowie_test_connect(FLOWIE_MQTT_VERSION_5, "expiry-owner", 0, 60u);
    flowie_mqtt_control_packet_view_t disconnect = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner;
    config.owner_instance_id = 41u;
    config.session_id = 43u;
    config.max_subscriptions = 2u;
    config.max_inflight = 2u;
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    disconnect.version = FLOWIE_MQTT_VERSION_5;
    disconnect.type = FLOWIE_MQTT_PACKET_DISCONNECT;
    disconnect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    disconnect.properties.values = (flowie_mqtt_span_t){expiry_one, sizeof(expiry_one)};
    check_int_eq(flowie_session_owner_disconnect(owner, &disconnect), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.session_expiry_interval, 1u);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);

    connect = flowie_test_connect(FLOWIE_MQTT_VERSION_5, "expiry-zero", 1, 0u);
    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_disconnect(owner, &disconnect), TURBO_EPROTO);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    flowie_session_owner_destroy(owner);
  }

  it("MQTT-STORE-009 round trips maximum UTF-8 fields and the configured payload bound") {
    enum {
      STORE_BOUNDARY_UTF8_SIZE = FLOWIE_MQTT_MAX_UTF8_SIZE,
      STORE_BOUNDARY_CONTENT_TYPE_SIZE = FLOWIE_MQTT_MAX_UTF8_SIZE - 3u,
      STORE_BOUNDARY_PAYLOAD_SIZE = 1024u * 1024u,
    };
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t restored_snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_owner_t *owner = NULL;
    flowie_session_owner_t *restored = NULL;
    uint8_t *client_id = NULL;
    uint8_t *topic = NULL;
    uint8_t *properties = NULL;
    uint8_t *payload = NULL;
    uint8_t *record = NULL;
    size_t record_size = 0u;

    client_id = (uint8_t *)malloc(STORE_BOUNDARY_UTF8_SIZE);
    topic = (uint8_t *)malloc(STORE_BOUNDARY_UTF8_SIZE);
    properties = (uint8_t *)malloc(STORE_BOUNDARY_UTF8_SIZE);
    payload = (uint8_t *)malloc(STORE_BOUNDARY_PAYLOAD_SIZE);
    check_not_null(client_id);
    check_not_null(topic);
    check_not_null(properties);
    check_not_null(payload);
    memset(client_id, 'c', STORE_BOUNDARY_UTF8_SIZE);
    memset(topic, 't', STORE_BOUNDARY_UTF8_SIZE);
    properties[0] = FLOWIE_MQTT_PROPERTY_CONTENT_TYPE;
    properties[1] = (uint8_t)(STORE_BOUNDARY_CONTENT_TYPE_SIZE >> 8u);
    properties[2] = (uint8_t)(STORE_BOUNDARY_CONTENT_TYPE_SIZE & 0xffu);
    memset(properties + 3u, 'p', STORE_BOUNDARY_CONTENT_TYPE_SIZE);
    for (size_t i = 0u; i < STORE_BOUNDARY_PAYLOAD_SIZE; ++i)
      payload[i] = (uint8_t)(i * 29u + 7u);

    config.owner_instance_id = 157u;
    config.session_id = 163u;
    config.max_subscriptions = 1u;
    config.max_inflight = 1u;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 0u;
    connect.keep_alive = 60u;
    connect.properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.client_id =
        (flowie_mqtt_span_t){client_id, STORE_BOUNDARY_UTF8_SIZE};
    connect.will_qos = 2u;
    connect.will_retain = 1u;
    connect.will_topic = (flowie_mqtt_span_t){topic, STORE_BOUNDARY_UTF8_SIZE};
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties.values =
        (flowie_mqtt_span_t){properties, STORE_BOUNDARY_UTF8_SIZE};
    connect.will_payload =
        (flowie_mqtt_span_t){payload, STORE_BOUNDARY_PAYLOAD_SIZE};

    owner = flowie_session_owner_create(&config);
    check_not_null(owner);
    check_int_eq(flowie_session_owner_open(owner, &connect), TURBO_OK);
    check_int_eq(flowie_session_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_true(snapshot.will_pending);
    check_int_eq(flowie_session_owner_record_encode(owner, NULL, 0u, &record_size), TURBO_ENOSPC);
    check_size_gt(record_size, STORE_BOUNDARY_PAYLOAD_SIZE);
    record = (uint8_t *)malloc(record_size);
    check_not_null(record);
    check_int_eq(flowie_session_owner_record_encode(owner, record, record_size, &record_size),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_record_restore(
                     &config, connect.client_id, snapshot.resource_generation, record,
                     record_size, &restored),
                 TURBO_OK);
    check_int_eq(flowie_session_owner_snapshot(restored, &restored_snapshot), TURBO_OK);
    check_size_eq(restored_snapshot.client_id.size, STORE_BOUNDARY_UTF8_SIZE);
    check_mem_eq(restored_snapshot.client_id.data, client_id, STORE_BOUNDARY_UTF8_SIZE);
    check_size_eq(restored_snapshot.will_topic.size, STORE_BOUNDARY_UTF8_SIZE);
    check_mem_eq(restored_snapshot.will_topic.data, topic, STORE_BOUNDARY_UTF8_SIZE);
    check_size_eq(restored_snapshot.will_properties.size, STORE_BOUNDARY_UTF8_SIZE);
    check_mem_eq(restored_snapshot.will_properties.data, properties, STORE_BOUNDARY_UTF8_SIZE);
    check_size_eq(restored_snapshot.will_payload.size, STORE_BOUNDARY_PAYLOAD_SIZE);
    check_mem_eq(restored_snapshot.will_payload.data, payload, STORE_BOUNDARY_PAYLOAD_SIZE);
    check_uint_eq(restored_snapshot.will_qos, 2u);
    check_true(restored_snapshot.will_retain);
    check_true(restored_snapshot.will_pending);

    flowie_session_owner_destroy(restored);
    flowie_session_owner_destroy(owner);
    free(record);
    free(payload);
    free(properties);
    free(topic);
    free(client_id);
  }

  it("MQTT-FUZZ-004 preserves owner packet ID HWM and ACK prerequisites for generated events") {
    enum { MODEL_STEPS = 4096 };
    const uint32_t initial_seed = UINT32_C(0x4d515454);
    flowie_owner_model_event_t events[MODEL_STEPS];
    flowie_owner_model_event_t candidate[MODEL_STEPS];
    uint32_t state = initial_seed;
    size_t failed_index = SIZE_MAX;
    size_t original_failed_index = SIZE_MAX;
    size_t reduced_count = MODEL_STEPS;
    int rc;
    for (size_t step = 0u; step < MODEL_STEPS; ++step) {
      uint32_t random = flowie_test_xorshift32(&state);
      events[step].packet_id =
          (uint16_t)(1u + random % FLOWIE_OWNER_MODEL_PACKET_IDS);
      events[step].operation = (uint8_t)((random >> 8u) & 3u);
      events[step].qos = (uint8_t)(1u + ((random >> 10u) & 1u));
    }
    info("seed=0x%08x events=%u", (unsigned)initial_seed, (unsigned)MODEL_STEPS);
    rc = flowie_owner_model_replay(events, MODEL_STEPS, &failed_index);
    if (rc == TURBO_EPROTO) {
      original_failed_index = failed_index;
      reduced_count = flowie_owner_model_shrink(events, MODEL_STEPS, candidate, &failed_index);
      info("first_failed=%zu minimized_events=%zu minimized_failed=%zu", original_failed_index,
           reduced_count, failed_index);
    }
    check_int_eq(rc, TURBO_OK);
  }
}
