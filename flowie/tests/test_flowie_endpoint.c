#include "flowie.h"
#include "flowie_test_socket.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define FLOWIE_TEST_WAIT_STEPS 2000u

typedef struct flowie_endpoint_capture_s {
  atomic_size_t calls;
  uint32_t types[4];
  size_t sizes[4];
  uint8_t packets[4][32];
  int result;
} flowie_endpoint_capture_t;

typedef struct flowie_security_fixture_s {
  size_t calls;
} flowie_security_fixture_t;

static int flowie_test_authenticate(void *ctx, const turbo_flow_security_auth_request_t *request,
                                    turbo_flow_security_principal_t *principal) {
  flowie_security_fixture_t *fixture = (flowie_security_fixture_t *)ctx;
  if (!fixture || !request || !principal) return TURBO_EINVAL;
  ++fixture->calls;
  if (strcmp(request->identity, "writer") != 0 || strcmp(request->method, "password") != 0 ||
      request->secret_size != sizeof("secret") - 1u ||
      memcmp(request->secret, "secret", sizeof("secret") - 1u) != 0)
    return TURBO_EPERM;
  *principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)snprintf(principal->principal_id, sizeof(principal->principal_id), "%s", request->identity);
  (void)snprintf(principal->principal_type, sizeof(principal->principal_type), "%s", "device");
  (void)snprintf(principal->tenant_id, sizeof(principal->tenant_id), "%s", "tenant-a");
  (void)snprintf(principal->auth_method, sizeof(principal->auth_method), "%s", request->method);
  principal->scope = TURBO_FLOW_SECURITY_SCOPE_TENANT;
  principal->role_count = 1u;
  (void)snprintf(principal->roles[0], sizeof(principal->roles[0]), "%s", "writer");
  principal->policy_version = 1u;
  return TURBO_OK;
}

static int flowie_endpoint_build_reply(turbo_flow_msg_t *msg, void *ctx) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  uint8_t encoded[32];
  tstr_t owned;
  size_t consumed = 0u;
  size_t written = 0u;
  int rc;
  (void)ctx;
  if (!msg || !turbo_flow_msg_protocol_route(msg)) return TURBO_EINVAL;
  options.version = FLOWIE_MQTT_VERSION_5;
  rc = flowie_mqtt_packet_parse((const uint8_t *)msg->payload.data, msg->payload.len, &options,
                                &packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != msg->payload.len) return TURBO_EPROTO;
  reply.version = FLOWIE_MQTT_VERSION_5;
  if (packet.type == FLOWIE_MQTT_PACKET_CONNECT) {
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    if (flowie_mqtt_connect_parse(&packet, &connect) != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    reply.type = FLOWIE_MQTT_PACKET_CONNACK;
  } else if (packet.type == FLOWIE_MQTT_PACKET_PUBLISH) {
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    if (flowie_mqtt_publish_parse(&packet, &publish) != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    if (publish.qos == 0u) {
      owned = tstr_new_len(msg->payload.data, msg->payload.len);
      if (!owned) return TURBO_ENOMEM;
      tstr_freep(&msg->owned_payload);
      msg->owned_payload = owned;
      msg->payload = tstr_to_v(owned);
      return TURBO_OK;
    }
    if (publish.qos != 1u) return TURBO_EPROTO;
    reply.type = FLOWIE_MQTT_PACKET_PUBACK;
    reply.packet_id = publish.packet_id;
  } else if (packet.type == FLOWIE_MQTT_PACKET_PINGREQ) {
    reply.type = FLOWIE_MQTT_PACKET_PINGRESP;
  } else {
    return TURBO_ENOTSUP;
  }
  rc = flowie_mqtt_control_packet_encode(&reply, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  owned = tstr_new_len(encoded, written);
  if (!owned) return TURBO_ENOMEM;
  tstr_freep(&msg->owned_payload);
  msg->owned_payload = owned;
  msg->payload = tstr_to_v(owned);
  msg->type = (uint32_t)reply.type;
  return TURBO_OK;
}

static int flowie_endpoint_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  flowie_endpoint_capture_t *capture = (flowie_endpoint_capture_t *)ctx;
  size_t index = atomic_load_explicit(&capture->calls, memory_order_relaxed);
  if (!msg || index >= 4u || msg->payload.len > sizeof(capture->packets[index])) {
    return TURBO_EMSGSIZE;
  }
  capture->types[index] = msg->type;
  capture->sizes[index] = msg->payload.len;
  if (msg->payload.len != 0u) {
    memcpy(capture->packets[index], msg->payload.data, msg->payload.len);
  }
  atomic_store_explicit(&capture->calls, index + 1u, memory_order_release);
  return capture->result;
}

static int flowie_wait_calls(flowie_endpoint_capture_t *capture, size_t expected) {
  for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
    if (atomic_load_explicit(&capture->calls, memory_order_acquire) >= expected) return TURBO_OK;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_test_encode_connect(uint8_t *output, size_t capacity, size_t *written,
                                      const char *client_id, uint32_t session_expiry,
                                      const char *will_topic, const char *will_payload,
                                      uint32_t will_delay) {
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  uint8_t properties[5];
  uint8_t will_properties[5];
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.keep_alive = 60u;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)client_id, client_id ? strlen(client_id) : 0u};
  if (session_expiry != 0u) {
    properties[0] = FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL;
    properties[1] = (uint8_t)(session_expiry >> 24u);
    properties[2] = (uint8_t)(session_expiry >> 16u);
    properties[3] = (uint8_t)(session_expiry >> 8u);
    properties[4] = (uint8_t)session_expiry;
    connect.properties = (flowie_mqtt_span_t){properties, sizeof(properties)};
  }
  if (will_topic) {
    connect.has_will = 1u;
    connect.will_topic = (flowie_mqtt_span_t){(const uint8_t *)will_topic, strlen(will_topic)};
    connect.will_payload = (flowie_mqtt_span_t){(const uint8_t *)will_payload,
                                                will_payload ? strlen(will_payload) : 0u};
    if (will_delay != 0u) {
      will_properties[0] = FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL;
      will_properties[1] = (uint8_t)(will_delay >> 24u);
      will_properties[2] = (uint8_t)(will_delay >> 16u);
      will_properties[3] = (uint8_t)(will_delay >> 8u);
      will_properties[4] = (uint8_t)will_delay;
      connect.will_properties = (flowie_mqtt_span_t){will_properties, sizeof(will_properties)};
    }
  }
  return flowie_mqtt_connect_packet_encode(&connect, output, capacity, written) ==
                 FLOWIE_MQTT_PARSE_OK
             ? TURBO_OK
             : TURBO_EPROTO;
}

static turbo_flow_t *flowie_endpoint_flow(unsigned short port, size_t max_packet_size,
                                          uint32_t max_connections,
                                          flowie_endpoint_capture_t *capture) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage capture\n"
                              "stage main {\n"
                              "  mqtt_in -> capture\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_packet_size = max_packet_size;
  config.max_connections = max_connections;
  config.recv_timeout_ms = 5000u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_managed_session_flow(unsigned short port,
                                                 flowie_endpoint_capture_t *capture) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage capture worker 1 capacity 8\n"
                              "stage main {\n"
                              "  mqtt_in -> capture\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_connections = 4u;
  config.recv_timeout_ms = 5000u;
  config.manage_sessions = 1;
  config.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
  config.settlement.qos2 = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
  config.max_sessions = 4u;
  config.max_subscriptions_per_session = 8u;
  config.max_inflight_per_session = 8u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *
flowie_settlement_failure_flow(unsigned short port, flowie_endpoint_capture_t *capture,
                               turbo_flow_protocol_settlement_point_t settlement_point) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage capture worker 1 capacity 8\n"
                              "stage main {\n"
                              "  mqtt_in -> capture\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_connections = 2u;
  config.recv_timeout_ms = 5000u;
  config.manage_sessions = 1;
  config.settlement.qos1 = settlement_point;
  config.settlement.qos2 = settlement_point;
  config.max_sessions = 2u;
  config.max_subscriptions_per_session = 2u;
  config.max_inflight_per_session = 2u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_reply_flow(unsigned short port, size_t send_hwm_bytes) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint operation "
                              FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION "\n"
                              "stage build_reply\n"
                              "stage mqtt_reply adapter flowie.endpoint operation "
                              FLOWIE_MQTT_PACKET_EGRESS_OPERATION "\n"
                              "stage main {\n"
                              "  mqtt_in -> build_reply -> mqtt_reply\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_connections = 4u;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = send_hwm_bytes;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "build_reply", flowie_endpoint_build_reply, NULL, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_fanout_flow_with_limits(unsigned short port, size_t max_packet_size,
                                                    size_t send_hwm_bytes, size_t max_inflight) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage mqtt_fanout adapter flowie.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_packet_size = max_packet_size;
  config.max_connections = 8u;
  config.coroutine_stack_size = FLOWIE_MIN_COROUTINE_STACK_SIZE;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = send_hwm_bytes;
  config.manage_sessions = 1;
  config.max_sessions = 8u;
  config.max_subscriptions_per_session = 8u;
  config.max_inflight_per_session = max_inflight;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_fanout_flow_with_inflight(unsigned short port, size_t max_inflight) {
  return flowie_fanout_flow_with_limits(port, 4096u, 4096u, max_inflight);
}

static turbo_flow_t *flowie_fanout_flow(unsigned short port) {
  return flowie_fanout_flow_with_inflight(port, 8u);
}

static turbo_flow_t *flowie_will_flow(unsigned short port, flowie_endpoint_capture_t *capture) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage capture worker 1 capacity 8\n"
                              "stage mqtt_fanout adapter flowie.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> capture -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_connections = 8u;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = 4096u;
  config.manage_sessions = 1;
  config.max_sessions = 8u;
  config.max_subscriptions_per_session = 8u;
  config.max_inflight_per_session = 8u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

spec("Flowie MQTT endpoint primitive") {
  it("rejects invalid endpoint configuration before registration") {
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    config.abi_version = FLOWIE_ABI_V1;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V2;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V3;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V4;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V5;
    config.host = "127.0.0.1";
    config.port = 1883;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V6;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V7;
    config.host = NULL;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.host = "127.0.0.1";
    config.port = 0;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.transport = FLOWIE_TRANSPORT_PIPE;
    config.host = NULL;
    config.path = NULL;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.transport = (flowie_transport_t)99;
    config.host = "127.0.0.1";
    config.port = 1883;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ENOTSUP);
    config.transport = FLOWIE_TRANSPORT_TCP;
    config.max_packet_size = FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE + 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ERANGE);
    config.max_packet_size = 0u;
    config.coroutine_stack_size = FLOWIE_MIN_COROUTINE_STACK_SIZE - 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ERANGE);
    config.coroutine_stack_size = 0u;
    config.recv_buffer_size = FLOWIE_MIN_RECV_BUFFER_SIZE - 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ERANGE);
    config.recv_buffer_size = FLOWIE_MIN_RECV_BUFFER_SIZE;
    config.context = coro_context_create(NULL);
    check_not_null(config.context);
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ENOTSUP);
    coro_context_destroy(config.context);
    config.context = NULL;
    config.recv_buffer_size = 0u;
    config.coroutine_stack_size = FLOWIE_MIN_COROUTINE_STACK_SIZE;
    config.context = coro_context_create(NULL);
    check_not_null(config.context);
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ENOTSUP);
    coro_context_destroy(config.context);
    turbo_flow_destroy(flow);
  }

  it("registers Connection, bounded reply Queue, and ProtocolAggregate resources") {
    static const turbo_flow_resource_document_kind_t document_kinds[] = {
        TURBO_FLOW_RESOURCE_DOCUMENT_SPEC, TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS,
        TURBO_FLOW_RESOURCE_DOCUMENT_EVENT};
    static const char *const status_types[] = {"MqttConnectionStatus", "MqttQueueStatus",
                                               "MqttProtocolStatus"};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    config.host = "127.0.0.1";
    config.port = 1883;
    check_not_null(flow);
    check_int_eq(flowie_register_endpoint(flow, "flowie.endpoint", &config), TURBO_OK);
    check_size_eq(turbo_flow_resource_metadata_count(flow), 3u);
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &metadata), TURBO_OK);
    check_int_eq(metadata.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_str_eq(metadata.uid, "flowie.endpoint.connection");
    metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_int_eq(turbo_flow_resource_metadata_at(flow, 1u, &metadata), TURBO_OK);
    check_int_eq(metadata.kind, TURBO_FLOW_RESOURCE_QUEUE_BUFFER);
    check_str_eq(metadata.uid, "flowie.endpoint.queue");
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 1u, &snapshot), TURBO_OK);
    check_size_eq(snapshot.capacity,
                  FLOWIE_DEFAULT_SEND_HWM_BYTES * FLOWIE_DEFAULT_MAX_CONNECTIONS);
    metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_int_eq(turbo_flow_resource_metadata_at(flow, 2u, &metadata), TURBO_OK);
    check_int_eq(metadata.kind, TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE);
    check_str_eq(metadata.uid, "flowie.endpoint.protocol");
    for (size_t i = 0u; i < turbo_flow_resource_metadata_count(flow); ++i) {
      check_int_eq(turbo_flow_resource_metadata_at(flow, i, &metadata), TURBO_OK);
      for (size_t j = 0u; j < sizeof(document_kinds) / sizeof(document_kinds[0]); ++j) {
        turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
        const turbo_flow_resource_schema_t *schema = turbo_flow_resource_governance_schema(
            metadata.domain, metadata.kind, document_kinds[j]);
        tstr_t json;
        check_not_null(schema);
        check_int_eq(turbo_flow_resource_document_at(flow, i, document_kinds[j], &document),
                     TURBO_OK);
        if (metadata.kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE &&
            document_kinds[j] != TURBO_FLOW_RESOURCE_DOCUMENT_SPEC) {
          check_str_eq(document.schema->schema_name, "FlowieMqttProtocolResource");
          check_str_eq(document.schema->type_name,
                       document_kinds[j] == TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS
                           ? "MqttProtocolConditions"
                           : "MqttProtocolEvent");
        } else {
          check(document.schema == schema);
        }
        check_str_eq(document.uid, metadata.uid);
        check_int_eq(turbo_flow_resource_document_validate(&document, document.schema), TURBO_OK);
        json = tstr_new_len(mem_buffer_const_data(document.payload),
                            mem_buffer_used(document.payload));
        check_not_null(json);
        check_null(strstr(json, "password"));
        check_null(strstr(json, "credential"));
        check_null(strstr(json, "client_id"));
        check_null(strstr(json, "payload"));
        check_null(strstr(json, "topic"));
        tstr_free(json);
        turbo_flow_resource_document_cleanup(&document);
      }
      {
        turbo_flow_resource_document_t status = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
        tstr_t json = NULL;
        check_int_eq(
            turbo_flow_resource_document_at(flow, i, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &status),
            TURBO_OK);
        check_str_eq(status.schema->type_name, status_types[i]);
        check_int_eq(turbo_flow_resource_document_validate(&status, status.schema), TURBO_OK);
        if (i == 1u) {
          check_uint_eq(status.schema->schema_version, 2u);
          json =
              tstr_new_len(mem_buffer_const_data(status.payload), mem_buffer_used(status.payload));
          check_not_null(json);
          check_not_null(strstr(json, "\"connection_hwm_bytes\":\"1048576\""));
          check_not_null(strstr(json, "\"slow_subscriber_policy\":1"));
          check_not_null(strstr(json, "\"slow_subscriber_disconnects\":\"0\""));
          tstr_freep(&json);
        }
        turbo_flow_resource_document_cleanup(&status);
      }
    }
    check_int_eq(flowie_register_endpoint(flow, "flowie.endpoint", &config), TURBO_EALREADY);
    turbo_flow_destroy(flow);
  }

  it("quiesces only new MQTT admission and resumes it through the ProtocolAggregate owner") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'm',   'g',   't'};
    static const uint8_t resumed_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'n',   'e',   'w'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_endpoint_capture_t capture = {0};
    turbo_flow_resource_metadata_t protocol = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    turbo_flow_resource_document_t status = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t rejected = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t resumed = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t received[sizeof(connack)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_managed_session_flow(port, &capture);
    tstr_t json = NULL;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_str_eq(turbo_flow_adapter_operation_module(
                     flow, "flowie.endpoint", FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION),
                 FLOWIE_MQTT_SERVER_MODULE);
    check_str_eq(turbo_flow_adapter_operation_module(
                     flow, "flowie.endpoint", FLOWIE_MQTT_PACKET_EGRESS_OPERATION),
                 FLOWIE_MQTT_SERVER_MODULE);
    if (!flow) return;
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(turbo_flow_resource_metadata_at(flow, 2u, &protocol), TURBO_OK);
    check_str_eq(protocol.uid, "flowie.endpoint.protocol");

    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memcpy(command.target_uid, protocol.uid, strlen(protocol.uid) + 1u);
    memcpy(command.idempotency_key, "flowie-quiesce-1", sizeof("flowie-quiesce-1"));
    command.expected_generation = protocol.generation;
    command.deadline_ns = UINT64_MAX;
    check_int_eq(turbo_flow_resource_command(flow, &command, &result), TURBO_OK);
    check_uint_eq(result.generation_after, result.generation_before + 1u);
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_int_eq(turbo_flow_resource_command(flow, &command, &result), TURBO_OK);
    check_true(result.replayed);

    rejected = flowie_test_connect(port);
    check_true(rejected != FLOWIE_TEST_INVALID_SOCKET);
    if (rejected != FLOWIE_TEST_INVALID_SOCKET) {
      int reject_rc = flowie_test_send(rejected, resumed_connect, sizeof(resumed_connect));
      if (reject_rc == TURBO_OK)
        reject_rc = flowie_test_recv_exact(rejected, received, sizeof(connack));
      check_int_ne(reject_rc, TURBO_OK);
      flowie_test_socket_close(rejected);
      rejected = FLOWIE_TEST_INVALID_SOCKET;
    }
    check_int_eq(flowie_test_send(client, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    check_int_eq(
        turbo_flow_resource_document_at(flow, 2u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &status),
        TURBO_OK);
    json = tstr_new_len(mem_buffer_const_data(status.payload), mem_buffer_used(status.payload));
    check_not_null(json);
    check_not_null(strstr(json, "\"started\":true"));
    check_not_null(strstr(json, "\"accepting\":false"));
    tstr_freep(&json);
    turbo_flow_resource_document_cleanup(&status);

    protocol = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_int_eq(turbo_flow_resource_metadata_at(flow, 2u, &protocol), TURBO_OK);
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    memcpy(command.target_uid, protocol.uid, strlen(protocol.uid) + 1u);
    memcpy(command.idempotency_key, "flowie-resume-1", sizeof("flowie-resume-1"));
    command.expected_generation = protocol.generation;
    command.deadline_ns = UINT64_MAX;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_int_eq(turbo_flow_resource_command(flow, &command, &result), TURBO_OK);
    check_uint_eq(result.generation_after, result.generation_before + 1u);
    resumed = flowie_test_connect(port);
    check_true(resumed != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(resumed, resumed_connect, sizeof(resumed_connect)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(client, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));

    flowie_test_socket_close(resumed);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("registers only strict flowie_endpoint fields from resolved YAML") {
    static const char valid[] = "version: 1\n"
                                "adapters:\n"
                                "  mqtt.endpoint:\n"
                                "    kind: flowie_endpoint\n"
                                "    config:\n"
                                "      transport: tcp\n"
                                "      host: 127.0.0.1\n"
                                "      port: 1883\n"
                                "      max_packet_size: 1048576\n"
                                "      max_connections: 100000\n"
                                "      coroutine_stack_size: 65536\n"
                                "      recv_buffer_size: 4096\n"
                                "      send_hwm_bytes: 1048576\n"
                                "      slow_subscriber_policy: disconnect\n"
                                "      manage_sessions: true\n"
                                "      settlement_qos1: accepted\n"
                                "      settlement_qos2: processed\n"
                                "      max_sessions: 100000\n"
                                "      max_subscriptions_per_session: 1024\n"
                                "      max_inflight_per_session: 64\n";
    static const char unknown[] = "version: 1\n"
                                  "adapters:\n"
                                  "  mqtt.endpoint:\n"
                                  "    kind: flowie_endpoint\n"
                                  "    config:\n"
                                  "      transport: tcp\n"
                                  "      host: 127.0.0.1\n"
                                  "      port: 1883\n"
                                  "      session_store: mqtt.sessions\n";
    static const char wrong_type[] = "version: 1\n"
                                     "adapters:\n"
                                     "  mqtt.endpoint:\n"
                                     "    kind: flowie_endpoint\n"
                                     "    config:\n"
                                     "      transport: tcp\n"
                                     "      host: 127.0.0.1\n"
                                     "      port: wrong\n";
    static const char wrong_kind[] = "version: 1\n"
                                     "adapters:\n"
                                     "  mqtt.endpoint:\n"
                                     "    kind: fmq\n"
                                     "    config:\n"
                                     "      transport: tcp\n"
                                     "      host: 127.0.0.1\n"
                                     "      port: 1883\n";
    static const char unsupported_settlement[] = "version: 1\n"
                                                 "adapters:\n"
                                                 "  mqtt.endpoint:\n"
                                                 "    kind: flowie_endpoint\n"
                                                 "    config:\n"
                                                 "      transport: tcp\n"
                                                 "      host: 127.0.0.1\n"
                                                 "      port: 1883\n"
                                                 "      manage_sessions: true\n"
                                                 "      settlement_qos1: arbitrary\n";
    static const char unsupported_slow_policy[] = "version: 1\n"
                                                  "adapters:\n"
                                                  "  mqtt.endpoint:\n"
                                                  "    kind: flowie_endpoint\n"
                                                  "    config:\n"
                                                  "      transport: tcp\n"
                                                  "      host: 127.0.0.1\n"
                                                  "      port: 1883\n"
                                                  "      slow_subscriber_policy: drop_oldest\n";
    static const char undersized_coroutine_stack[] = "version: 1\n"
                                                     "adapters:\n"
                                                     "  mqtt.endpoint:\n"
                                                     "    kind: flowie_endpoint\n"
                                                     "    config:\n"
                                                     "      transport: tcp\n"
                                                     "      host: 127.0.0.1\n"
                                                     "      port: 1883\n"
                                                     "      coroutine_stack_size: 16384\n";
    static const char undersized_recv_buffer[] = "version: 1\n"
                                                 "adapters:\n"
                                                 "  mqtt.endpoint:\n"
                                                 "    kind: flowie_endpoint\n"
                                                 "    config:\n"
                                                 "      transport: tcp\n"
                                                 "      host: 127.0.0.1\n"
                                                 "      port: 1883\n"
                                                 "      recv_buffer_size: 512\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(valid, sizeof(valid) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_str_eq(snapshot.endpoint, "tcp://127.0.0.1:1883");
    check_size_eq(snapshot.connection_limit, 100000u);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);

    {
      const struct {
        const char *yaml;
        size_t size;
        const char *path;
      } cases[] = {
          {unknown, sizeof(unknown) - 1u, "session_store"},
          {wrong_type, sizeof(wrong_type) - 1u, "port"},
          {wrong_kind, sizeof(wrong_kind) - 1u, "mqtt.endpoint"},
          {unsupported_settlement, sizeof(unsupported_settlement) - 1u, "settlement_qos1"},
          {unsupported_slow_policy, sizeof(unsupported_slow_policy) - 1u, "slow_subscriber_policy"},
          {undersized_coroutine_stack, sizeof(undersized_coroutine_stack) - 1u,
           "coroutine_stack_size"},
          {undersized_recv_buffer, sizeof(undersized_recv_buffer) - 1u, "recv_buffer_size"}};
      for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        resolved = NULL;
        error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
        flow = turbo_flow_create();
        check_not_null(flow);
        check_int_eq(
            turbo_flow_config_resolve_yaml(cases[i].yaml, cases[i].size, &resolved, &error),
            TURBO_OK);
        check_int_ne(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                     TURBO_OK);
        check_str_contains(error.path, cases[i].path);
        turbo_flow_resolved_config_destroy(resolved);
        turbo_flow_destroy(flow);
      }
    }
  }

  it("keeps the installed flowie.yml endpoint resolvable and registrable") {
    char path[1024];
    char *yaml;
    size_t yaml_size = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_gt(snprintf(path, sizeof(path), "%s/examples/flowie.yml", FLOWIE_SOURCE_DIR), 0);
    yaml = tt_read_file(path, &yaml_size);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_size, &resolved, &error), TURBO_OK);
    check_not_null(resolved);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
    check_size_eq(turbo_flow_adapter_count(flow), 1u);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
    free(yaml);
  }

  it("authenticates CONNECT and authorizes publish and subscribe from YAML realm policy") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  security.main:\n"
                               "    kind: security_realm\n"
                               "    config:\n"
                               "      resource_uid: security:main\n"
                               "      owner_name: security.main\n"
                               "      policy_version: 1\n"
                               "      rules:\n"
                               "        - effect: allow\n"
                               "          subject_kind: role\n"
                               "          subject: writer\n"
                               "          tenant_id: tenant-a\n"
                               "          actions: [connect]\n"
                               "          resource_type: generic\n"
                               "          match: prefix\n"
                               "          pattern: secure-\n"
                               "        - effect: allow\n"
                               "          subject_kind: role\n"
                               "          subject: writer\n"
                               "          tenant_id: tenant-a\n"
                               "          actions: [publish, subscribe]\n"
                               "          resource_type: mqtt_topic\n"
                               "          match: adapter\n"
                               "          pattern: tenant-a/#\n"
                               "adapters:\n"
                               "  flowie.endpoint:\n"
                               "    kind: flowie_endpoint\n"
                               "    config:\n"
                               "      transport: tcp\n"
                               "      host: 127.0.0.1\n"
                               "      port: 1883\n"
                               "      manage_sessions: true\n"
                               "      max_connections: 4\n"
                               "      max_sessions: 4\n"
                               "      max_subscriptions_per_session: 8\n"
                               "      max_inflight_per_session: 8\n"
                               "      security_realm: security.main\n"
                               "      auth_method: password\n";
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const uint8_t connack_ok[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t connack_bad_password[] = {0x20u, 0x03u, 0x00u, 0x86u, 0x00u};
    static const uint8_t publish_allowed[] = {0x32u, 0x15u, 0x00u, 0x0fu, 't',   'e',   'n', 'a',
                                              'n',   't',   '-',   'a',   '/',   'e',   'v', 'e',
                                              'n',   't',   's',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t publish_denied[] = {0x32u, 0x15u, 0x00u, 0x0fu, 't',   'e',   'n', 'a',
                                             'n',   't',   '-',   'b',   '/',   'e',   'v', 'e',
                                             'n',   't',   's',   0x00u, 0x02u, 0x00u, 'x'};
    static const uint8_t puback_ok[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t puback_denied[] = {0x40u, 0x04u, 0x00u, 0x02u, 0x87u, 0x00u};
    static const uint8_t subscribe_allowed[] = {0x82u, 0x10u, 0x00u, 0x03u, 0x00u, 0x00u,
                                                0x0au, 't',   'e',   'n',   'a',   'n',
                                                't',   '-',   'a',   '/',   '#',   0x01u};
    static const uint8_t subscribe_denied[] = {0x82u, 0x07u, 0x00u, 0x04u, 0x00u,
                                               0x00u, 0x01u, '#',   0x01u};
    static const uint8_t suback_allowed[] = {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x01u};
    static const uint8_t suback_denied[] = {0x90u, 0x04u, 0x00u, 0x04u, 0x00u, 0x87u};
    flowie_endpoint_capture_t capture;
    flowie_security_fixture_t auth = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packet[128];
    uint8_t bad_connect_packet[128];
    uint8_t received[8];
    size_t connect_size = 0u;
    size_t bad_connect_size = 0u;
    unsigned short port = flowie_test_port();
    flowie_test_socket_t client;
    flowie_test_socket_t rejected;

    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    security.realm_channel = "security.main";
    security.auth_method = "password";
    security.auth_provider = &provider;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(flowie_mqtt_security_matcher_init(&matcher), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_create_resolved(resolved, "security.main", &matcher,
                                                           &realm, &error),
                 TURBO_OK);
    security.realm = realm;
    {
      turbo_flow_t *validation_flow = turbo_flow_create();
      check_not_null(validation_flow);
      check_int_eq(flowie_register_resolved_secure_endpoint(validation_flow, "flowie.endpoint",
                                                            resolved, &security, &error),
                   TURBO_OK);
      turbo_flow_destroy(validation_flow);
    }
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);
    /* Direct registration supplies the ephemeral test port; resolved wiring was checked above. */
    {
      flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
      config.host = "127.0.0.1";
      config.port = (int)port;
      config.max_connections = 4u;
      config.manage_sessions = 1;
      config.max_sessions = 4u;
      config.max_subscriptions_per_session = 8u;
      config.max_inflight_per_session = 8u;
      check_int_eq(flowie_register_secure_endpoint(flow, "flowie.endpoint", &config, &security),
                   TURBO_OK);
    }
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-1", 8u};
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"wrong", 5u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, bad_connect_packet,
                                                   sizeof(bad_connect_packet), &bad_connect_size),
                 FLOWIE_MQTT_PARSE_OK);

    rejected = flowie_test_connect(port);
    check_true(rejected != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(rejected, bad_connect_packet, bad_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(rejected, received, sizeof(connack_bad_password)),
                 TURBO_OK);
    check_mem_eq(received, connack_bad_password, sizeof(connack_bad_password));
    flowie_test_socket_close(rejected);

    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack_ok)), TURBO_OK);
    check_mem_eq(received, connack_ok, sizeof(connack_ok));
    check_int_eq(flowie_test_send(client, publish_allowed, sizeof(publish_allowed)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback_ok)), TURBO_OK);
    check_mem_eq(received, puback_ok, sizeof(puback_ok));
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_int_eq(flowie_test_send(client, publish_denied, sizeof(publish_denied)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback_denied)), TURBO_OK);
    check_mem_eq(received, puback_denied, sizeof(puback_denied));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);
    check_int_eq(flowie_test_send(client, subscribe_allowed, sizeof(subscribe_allowed)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(suback_allowed)), TURBO_OK);
    check_mem_eq(received, suback_allowed, sizeof(suback_allowed));
    check_int_eq(flowie_test_send(client, subscribe_denied, sizeof(subscribe_denied)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(suback_denied)), TURBO_OK);
    check_mem_eq(received, suback_denied, sizeof(suback_denied));
    check_size_eq(auth.calls, 2u);

    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("frames fragmented and sticky MQTT packets on the connection lane") {
    static const uint8_t connect_packet[] = {0x10, 0x17, 0x00, 0x04, 'M',  'Q',  'T',  'T', 0x05,
                                             0x02, 0x00, 0x3c, 0x07, 0x15, 0x00, 0x04, 'n', 'o',
                                             'n',  'e',  0x00, 0x03, 'c',  'l',  'i'};
    static const uint8_t publish[] = {0x30, 0x07, 0x00, 0x01, 'a', 0x00, 'o', 'k', '!'};
    static const uint8_t pings[] = {0xc0, 0x00, 0xc0, 0x00};
    unsigned short port = flowie_test_port();
    flowie_endpoint_capture_t capture;
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_t *flow;
    flowie_test_socket_t client;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    flow = flowie_endpoint_flow(port, sizeof(connect_packet), 4u, &capture);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_int_eq(flowie_test_send(client, publish, 3u), TURBO_OK);
    turbo_sleep_ms(20u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);
    check_int_eq(flowie_test_send(client, publish + 3u, sizeof(publish) - 3u), TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);
    check_int_eq(flowie_test_send(client, pings, sizeof(pings)), TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 4u), TURBO_OK);
    check_uint_eq(capture.types[0], FLOWIE_MQTT_PACKET_CONNECT);
    check_uint_eq(capture.types[1], FLOWIE_MQTT_PACKET_PUBLISH);
    check_size_eq(capture.sizes[1], sizeof(publish));
    check_mem_eq(capture.packets[1], publish, sizeof(publish));
    check_uint_eq(capture.types[2], FLOWIE_MQTT_PACKET_PINGREQ);
    check_uint_eq(capture.types[3], FLOWIE_MQTT_PACKET_PINGREQ);
    check_mem_eq(capture.packets[2], pings, 2u);
    check_mem_eq(capture.packets[3], pings + 2u, 2u);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_size_eq(snapshot.connections_current, 1u);

    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 0u);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_size_eq(snapshot.connections_current, 0u);
    flowie_test_socket_close(client);
    turbo_flow_destroy(flow);
  }

  it("routes graph-owned MQTT control replies back to the connection owner lane") {
    static const uint8_t connect_packet[] = {
        0x10u, 0x17u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x07u,
        0x15u, 0x00u, 0x04u, 'n',   'o', 'n', 'e', 0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_reply_flow(port, 1024u);
    flowie_test_socket_t client;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(client, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_int_eq(flowie_test_send(client, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("binds managed CONNECT sessions across reconnect and rejects an active duplicate") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t first_connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t resumed_connack[] = {0x20u, 0x03u, 0x01u, 0x00u, 0x00u};
    static const uint8_t duplicate_connack[] = {0x20u, 0x03u, 0x00u, 0x89u, 0x00u};
    static const uint8_t publish_qos1[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x07u, 0x00u, 0x00u,
                                        0x03u, 'a',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x07u, 0x00u, 0x01u};
    static const uint8_t unsubscribe[] = {0xa2u, 0x08u, 0x00u, 0x08u, 0x00u,
                                          0x00u, 0x03u, 'a',   '/',   '#'};
    static const uint8_t unsuback[] = {0xb0u, 0x04u, 0x00u, 0x08u, 0x00u, 0x00u};
    static const uint8_t publish_qos2[] = {0x34u, 0x07u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t publish_qos2_duplicate[] = {0x3cu, 0x07u, 0x00u, 0x01u, 'a',
                                                     0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t pubrec[] = {0x50u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t pubrel[] = {0x62u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t pubcomp[] = {0x70u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t unknown_pubrel[] = {0x62u, 0x02u, 0x00u, 0x2cu};
    static const uint8_t unknown_pubcomp[] = {0x70u, 0x04u, 0x00u, 0x2cu, 0x92u, 0x00u};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    static const uint8_t auth[] = {0xf0u, 0x00u};
    static const uint8_t auth_disconnect[] = {0xe0u, 0x01u, 0x8cu};
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    flowie_endpoint_capture_t capture;
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_resource_snapshot_t session_snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    turbo_flow_t *flow;
    flowie_test_socket_t first;
    flowie_test_socket_t resumed;
    flowie_test_socket_t duplicate;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    flow = flowie_managed_session_flow(port, &capture);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    first = flowie_test_connect(port);
    check_true(first != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(first, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first, received, sizeof(first_connack)), TURBO_OK);
    check_mem_eq(received, first_connack, sizeof(first_connack));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    flowie_test_socket_close(first);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 0u);
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 2u, &session_snapshot), TURBO_OK);
    check_size_eq(session_snapshot.load, 1u);
    check_size_eq(session_snapshot.capacity, 4u);

    resumed = flowie_test_connect(port);
    check_true(resumed != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(resumed, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(resumed_connack)), TURBO_OK);
    check_mem_eq(received, resumed_connack, sizeof(resumed_connack));

    duplicate = flowie_test_connect(port);
    check_true(duplicate != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(duplicate, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(duplicate_connack)), TURBO_OK);
    check_mem_eq(received, duplicate_connack, sizeof(duplicate_connack));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 1u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);

    check_int_eq(flowie_test_send(resumed, publish_qos1, sizeof(publish_qos1)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_uint_eq(capture.types[0], FLOWIE_MQTT_PACKET_PUBLISH);
    check_mem_eq(capture.packets[0], publish_qos1, sizeof(publish_qos1));

    check_int_eq(flowie_test_send(resumed, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);

    check_int_eq(flowie_test_send(resumed, unsubscribe, sizeof(unsubscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(unsuback)), TURBO_OK);
    check_mem_eq(received, unsuback, sizeof(unsuback));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);

    check_int_eq(flowie_test_send(resumed, publish_qos2, sizeof(publish_qos2)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(pubrec)), TURBO_OK);
    check_mem_eq(received, pubrec, sizeof(pubrec));
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);
    check_uint_eq(capture.types[1], FLOWIE_MQTT_PACKET_PUBLISH);
    check_mem_eq(capture.packets[1], publish_qos2, sizeof(publish_qos2));

    check_int_eq(flowie_test_send(resumed, publish_qos2_duplicate, sizeof(publish_qos2_duplicate)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(pubrec)), TURBO_OK);
    check_mem_eq(received, pubrec, sizeof(pubrec));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 2u);

    check_int_eq(flowie_test_send(resumed, pubrel, sizeof(pubrel)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(pubcomp)), TURBO_OK);
    check_mem_eq(received, pubcomp, sizeof(pubcomp));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 2u);

    check_int_eq(flowie_test_send(resumed, unknown_pubrel, sizeof(unknown_pubrel)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(unknown_pubcomp)), TURBO_OK);
    check_mem_eq(received, unknown_pubcomp, sizeof(unknown_pubcomp));

    check_int_eq(flowie_test_send(resumed, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 2u);

    check_int_eq(flowie_test_send(resumed, auth, sizeof(auth)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(auth_disconnect)), TURBO_OK);
    check_mem_eq(received, auth_disconnect, sizeof(auth_disconnect));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 0u);
    flowie_test_socket_close(duplicate);
    flowie_test_socket_close(resumed);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("preserves FIFO through a full TCP reply batch and closes after its terminal packet") {
    enum { PIPELINED_PING_COUNT = 63u };
    static const uint8_t connect_packet[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'b',   'a',   't'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t auth_disconnect[] = {0xe0u, 0x01u, 0x8cu};
    uint8_t pipeline[PIPELINED_PING_COUNT * 2u + 2u];
    uint8_t expected[PIPELINED_PING_COUNT * 2u + sizeof(auth_disconnect)];
    uint8_t received[sizeof(expected)];
    uint8_t received_connack[sizeof(connack)];
    unsigned short port = flowie_test_port();
    flowie_endpoint_capture_t capture;
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_t *flow;
    flowie_test_socket_t client;

    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    for (size_t i = 0u; i < PIPELINED_PING_COUNT; ++i) {
      pipeline[i * 2u] = 0xc0u;
      pipeline[i * 2u + 1u] = 0x00u;
      expected[i * 2u] = 0xd0u;
      expected[i * 2u + 1u] = 0x00u;
    }
    pipeline[PIPELINED_PING_COUNT * 2u] = 0xf0u;
    pipeline[PIPELINED_PING_COUNT * 2u + 1u] = 0x00u;
    memcpy(expected + PIPELINED_PING_COUNT * 2u, auth_disconnect, sizeof(auth_disconnect));

    check_int_gt(port, 0);
    flow = flowie_managed_session_flow(port, &capture);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received_connack, sizeof(received_connack)), TURBO_OK);
    check_mem_eq(received_connack, connack, sizeof(connack));

    check_int_eq(flowie_test_send(client, pipeline, sizeof(pipeline)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, expected, sizeof(expected));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 0u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);

    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("sends the default RECEIVED PUBACK before closing on graph failure") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'r',   'c',   'v'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
    flowie_endpoint_capture_t capture;
    turbo_flow_connection_snapshot_t snapshot = {0};
    uint8_t received[sizeof(connack)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t client;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    capture.result = TURBO_EIO;
    check_int_gt(port, 0);
    flow = flowie_settlement_failure_flow(port, &capture, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(client, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 0u);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("does not send a PROCESSED PUBACK when the graph fails") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'p',   'r',   'c'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u, 'x'};
    flowie_endpoint_capture_t capture;
    uint8_t received[sizeof(connack)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t client;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    capture.result = TURBO_EIO;
    check_int_gt(port, 0);
    flow = flowie_settlement_failure_flow(port, &capture, TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(client, publish, sizeof(publish)), TURBO_OK);
    check_int_ne(flowie_test_recv_exact(client, received, 4u), TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("fans one graph PUBLISH to wildcard and shared subscriptions on the owner lane") {
    static const uint8_t connect_template[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'x',   '0',   '1'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t normal_subscribe[] = {0x82u, 0x0fu, 0x00u, 0x01u, 0x00u, 0x00u,
                                               0x09u, 's',   'e',   'n',   's',   'o',
                                               'r',   's',   '/',   '+',   0x01u};
    static const uint8_t no_local_subscribe[] = {0x82u, 0x0fu, 0x00u, 0x02u, 0x00u, 0x00u,
                                                 0x09u, 's',   'e',   'n',   's',   'o',
                                                 'r',   's',   '/',   '+',   0x04u};
    static const uint8_t overlap_subscribe[] = {0x82u, 0x0fu, 0x00u, 0x05u, 0x00u, 0x00u,
                                                0x09u, 's',   'e',   'n',   's',   'o',
                                                'r',   's',   '/',   '#',   0x02u};
    static const uint8_t shared_subscribe[] = {
        0x82u, 0x1eu, 0x00u, 0x03u, 0x00u, 0x00u, 0x18u, '$', 's', 'h',  'a',
        'r',   'e',   '/',   'w',   'o',   'r',   'k',   'e', 'r', 's',  '/',
        's',   'e',   'n',   's',   'o',   'r',   's',   '/', '+', 0x00u};
    static const uint8_t shared_update_subscribe[] = {
        0x82u, 0x1eu, 0x00u, 0x08u, 0x00u, 0x00u, 0x18u, '$', 's', 'h',  'a',
        'r',   'e',   '/',   'w',   'o',   'r',   'k',   'e', 'r', 's',  '/',
        's',   'e',   'n',   's',   'o',   'r',   's',   '/', '+', 0x08u};
    static const uint8_t normal_suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t no_local_suback[] = {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x00u};
    static const uint8_t overlap_suback[] = {0x90u, 0x04u, 0x00u, 0x05u, 0x00u, 0x02u};
    static const uint8_t shared_suback[] = {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x00u};
    static const uint8_t shared_update_suback[] = {0x90u, 0x04u, 0x00u, 0x08u, 0x00u, 0x00u};
    static const uint8_t publish_first[] = {0x32u, 0x0fu, 0x00u, 0x09u, 's', 'e',
                                            'n',   's',   'o',   'r',   's', '/',
                                            'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publish_second[] = {0x32u, 0x0fu, 0x00u, 0x09u, 's', 'e',
                                             'n',   's',   'o',   'r',   's', '/',
                                             'a',   0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t publisher_puback_first[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_puback_second[] = {0x40u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t normal_unsubscribe[] = {
        0xa2u, 0x19u, 0x00u, 0x04u, 0x00u, 0x00u, 0x09u, 's', 'e', 'n', 's', 'o', 'r', 's',
        '/',   '+',   0x00u, 0x09u, 's',   'e',   'n',   's', 'o', 'r', 's', '/', '#'};
    static const uint8_t normal_unsuback[] = {0xb0u, 0x05u, 0x00u, 0x04u, 0x00u, 0x00u, 0x00u};
    static const uint8_t publish_third[] = {0x32u, 0x0fu, 0x00u, 0x09u, 's', 'e',
                                            'n',   's',   'o',   'r',   's', '/',
                                            'a',   0x00u, 0x2cu, 0x00u, 'z'};
    static const uint8_t publisher_puback_third[] = {0x40u, 0x02u, 0x00u, 0x2cu};
    static const uint8_t normal_delivery_first[] = {0x32u, 0x0fu, 0x00u, 0x09u, 's', 'e',
                                                    'n',   's',   'o',   'r',   's', '/',
                                                    'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t normal_delivery_second[] = {0x32u, 0x0fu, 0x00u, 0x09u, 's', 'e',
                                                     'n',   's',   'o',   'r',   's', '/',
                                                     'a',   0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t normal_puback_first[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t normal_puback_second[] = {0x40u, 0x02u, 0x00u, 0x02u};
    static const uint8_t shared_delivery_first[] = {
        0x30u, 0x0du, 0x00u, 0x09u, 's', 'e', 'n', 's', 'o', 'r', 's', '/', 'a', 0x00u, 'x'};
    static const uint8_t shared_delivery_second[] = {
        0x30u, 0x0du, 0x00u, 0x09u, 's', 'e', 'n', 's', 'o', 'r', 's', '/', 'a', 0x00u, 'y'};
    static const uint8_t shared_delivery_third[] = {
        0x30u, 0x0du, 0x00u, 0x09u, 's', 'e', 'n', 's', 'o', 'r', 's', '/', 'a', 0x00u, 'z'};
    static const uint8_t unrelated_subscribe[] = {
        0x82u, 0x0du, 0x00u, 0x06u, 0x00u, 0x00u, 0x07u, 'o', 't', 'h', 'e', 'r', '/', '#', 0x00u};
    static const uint8_t unrelated_suback[] = {0x90u, 0x04u, 0x00u, 0x06u, 0x00u, 0x00u};
    static const uint8_t sys_subscribe[] = {0x82u, 0x0cu, 0x00u, 0x07u, 0x00u, 0x00u, 0x06u,
                                            '$',   'S',   'Y',   'S',   '/',   '#',   0x00u};
    static const uint8_t sys_suback[] = {0x90u, 0x04u, 0x00u, 0x07u, 0x00u, 0x00u};
    static const uint8_t sys_publish[] = {0x30u, 0x0fu, 0x00u, 0x0bu, '$', 'S', 'Y',   'S', '/',
                                          's',   't',   'a',   't',   'u', 's', 0x00u, 'v'};
    uint8_t connects[4][sizeof(connect_template)];
    uint8_t received[32];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t normal;
    flowie_test_socket_t shared_a;
    flowie_test_socket_t shared_b;
    flowie_test_socket_t first_shared;
    flowie_test_socket_t second_shared;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    for (size_t i = 0u; i < 4u; ++i) {
      memcpy(connects[i], connect_template, sizeof(connect_template));
      connects[i][sizeof(connect_template) - 3u] = (uint8_t)"pnss"[i];
      connects[i][sizeof(connect_template) - 2u] = (uint8_t)('0' + (int)i);
    }
    publisher = flowie_test_connect(port);
    normal = flowie_test_connect(port);
    shared_a = flowie_test_connect(port);
    shared_b = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(normal != FLOWIE_TEST_INVALID_SOCKET);
    check_true(shared_a != FLOWIE_TEST_INVALID_SOCKET);
    check_true(shared_b != FLOWIE_TEST_INVALID_SOCKET);
    {
      flowie_test_socket_t clients[] = {publisher, normal, shared_a, shared_b};
      for (size_t i = 0u; i < 4u; ++i) {
        check_int_eq(flowie_test_send(clients[i], connects[i], sizeof(connects[i])), TURBO_OK);
        check_int_eq(flowie_test_recv_exact(clients[i], received, sizeof(connack)), TURBO_OK);
        check_mem_eq(received, connack, sizeof(connack));
      }
    }
    check_int_eq(flowie_test_send(normal, normal_subscribe, sizeof(normal_subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(normal_suback)), TURBO_OK);
    check_mem_eq(received, normal_suback, sizeof(normal_suback));
    check_int_eq(flowie_test_send(publisher, no_local_subscribe, sizeof(no_local_subscribe)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(no_local_suback)), TURBO_OK);
    check_mem_eq(received, no_local_suback, sizeof(no_local_suback));
    check_int_eq(flowie_test_send(normal, overlap_subscribe, sizeof(overlap_subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(overlap_suback)), TURBO_OK);
    check_mem_eq(received, overlap_suback, sizeof(overlap_suback));
    check_int_eq(flowie_test_send(shared_a, shared_subscribe, sizeof(shared_subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(shared_a, received, sizeof(shared_suback)), TURBO_OK);
    check_mem_eq(received, shared_suback, sizeof(shared_suback));
    check_int_eq(flowie_test_send(shared_b, shared_subscribe, sizeof(shared_subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(shared_b, received, sizeof(shared_suback)), TURBO_OK);
    check_mem_eq(received, shared_suback, sizeof(shared_suback));

    check_int_eq(flowie_test_send(publisher, publish_first, sizeof(publish_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_puback_first)),
                 TURBO_OK);
    check_mem_eq(received, publisher_puback_first, sizeof(publisher_puback_first));
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(normal_delivery_first)), TURBO_OK);
    check_mem_eq(received, normal_delivery_first, sizeof(normal_delivery_first));
    check_int_eq(flowie_test_send(normal, normal_puback_first, sizeof(normal_puback_first)),
                 TURBO_OK);
    check_false(flowie_test_socket_readable(normal, 50u));
    check_false(flowie_test_socket_readable(publisher, 50u));
    check(flowie_test_socket_readable(shared_a, 500u) !=
          flowie_test_socket_readable(shared_b, 500u));
    first_shared = flowie_test_socket_readable(shared_a, 0u) ? shared_a : shared_b;
    second_shared = first_shared == shared_a ? shared_b : shared_a;
    check_int_eq(flowie_test_recv_exact(first_shared, received, sizeof(shared_delivery_first)),
                 TURBO_OK);
    check_mem_eq(received, shared_delivery_first, sizeof(shared_delivery_first));
    check_int_eq(
        flowie_test_send(first_shared, shared_update_subscribe, sizeof(shared_update_subscribe)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first_shared, received, sizeof(shared_update_suback)),
                 TURBO_OK);
    check_mem_eq(received, shared_update_suback, sizeof(shared_update_suback));
    check_int_eq(flowie_test_send(normal, unrelated_subscribe, sizeof(unrelated_subscribe)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(unrelated_suback)), TURBO_OK);
    check_mem_eq(received, unrelated_suback, sizeof(unrelated_suback));

    check_int_eq(flowie_test_send(publisher, publish_second, sizeof(publish_second)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_puback_second)),
                 TURBO_OK);
    check_mem_eq(received, publisher_puback_second, sizeof(publisher_puback_second));
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(normal_delivery_second)),
                 TURBO_OK);
    check_mem_eq(received, normal_delivery_second, sizeof(normal_delivery_second));
    check_int_eq(flowie_test_send(normal, normal_puback_second, sizeof(normal_puback_second)),
                 TURBO_OK);
    check_true(flowie_test_socket_readable(second_shared, 500u));
    check_false(flowie_test_socket_readable(first_shared, 50u));
    check_int_eq(flowie_test_recv_exact(second_shared, received, sizeof(shared_delivery_second)),
                 TURBO_OK);
    check_mem_eq(received, shared_delivery_second, sizeof(shared_delivery_second));
    check_false(flowie_test_socket_readable(publisher, 50u));

    check_int_eq(flowie_test_send(normal, normal_unsubscribe, sizeof(normal_unsubscribe)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(normal_unsuback)), TURBO_OK);
    check_mem_eq(received, normal_unsuback, sizeof(normal_unsuback));
    check_int_eq(flowie_test_send(publisher, publish_third, sizeof(publish_third)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_puback_third)),
                 TURBO_OK);
    check_mem_eq(received, publisher_puback_third, sizeof(publisher_puback_third));
    check_false(flowie_test_socket_readable(normal, 50u));
    check_true(flowie_test_socket_readable(first_shared, 500u));
    check_false(flowie_test_socket_readable(second_shared, 50u));
    check_int_eq(flowie_test_recv_exact(first_shared, received, sizeof(shared_delivery_third)),
                 TURBO_OK);
    check_mem_eq(received, shared_delivery_third, sizeof(shared_delivery_third));
    check_false(flowie_test_socket_readable(publisher, 50u));

    check_int_eq(flowie_test_send(first_shared, sys_subscribe, sizeof(sys_subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first_shared, received, sizeof(sys_suback)), TURBO_OK);
    check_mem_eq(received, sys_suback, sizeof(sys_suback));
    check_int_eq(flowie_test_send(publisher, sys_publish, sizeof(sys_publish)), TURBO_OK);
    check_true(flowie_test_socket_readable(first_shared, 500u));
    check_int_eq(flowie_test_recv_exact(first_shared, received, sizeof(sys_publish)), TURBO_OK);
    check_mem_eq(received, sys_publish, sizeof(sys_publish));
    check_false(flowie_test_socket_readable(normal, 50u));
    check_false(flowie_test_socket_readable(second_shared, 50u));
    check_false(flowie_test_socket_readable(publisher, 50u));

    flowie_test_socket_close(shared_b);
    flowie_test_socket_close(shared_a);
    flowie_test_socket_close(normal);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("stores retained publications and applies MQTT 5 retain handling on subscribe") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'r',   'p',   '1'};
    static const uint8_t subscriber_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                 'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                 0x00u, 0x00u, 0x03u, 'r',   's',   '1'};
    static const uint8_t second_subscriber_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                        'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                        0x00u, 0x00u, 0x03u, 'r',   's',   '2'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t retained_publish[] = {0x31u, 0x0bu, 0x00u, 0x07u, 's',   't', 'a',
                                               't',   'e',   '/',   'a',   0x00u, 'x'};
    static const uint8_t subscribe_rh0[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 's',
                                            't',   'a',   't',   'e',   '/',   '#',   0x00u};
    static const uint8_t subscribe_rh1[] = {0x82u, 0x0du, 0x00u, 0x02u, 0x00u, 0x00u, 0x07u, 's',
                                            't',   'a',   't',   'e',   '/',   '#',   0x10u};
    static const uint8_t subscribe_rh0_again[] = {
        0x82u, 0x0du, 0x00u, 0x03u, 0x00u, 0x00u, 0x07u, 's', 't', 'a', 't', 'e', '/', '#', 0x00u};
    static const uint8_t subscribe_rh2[] = {0x82u, 0x0du, 0x00u, 0x04u, 0x00u, 0x00u, 0x07u, 's',
                                            't',   'a',   't',   'e',   '/',   '#',   0x20u};
    static const uint8_t suback1[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t suback2[] = {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x00u};
    static const uint8_t suback3[] = {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x00u};
    static const uint8_t suback4[] = {0x90u, 0x04u, 0x00u, 0x04u, 0x00u, 0x00u};
    static const uint8_t retained_delete[] = {0x31u, 0x0au, 0x00u, 0x07u, 's', 't',
                                              'a',   't',   'e',   '/',   'a', 0x00u};
    static const uint8_t forwarded_delete[] = {0x30u, 0x0au, 0x00u, 0x07u, 's', 't',
                                               'a',   't',   'e',   '/',   'a', 0x00u};
    static const uint8_t expiring_retained[] = {0x31u, 0x10u, 0x00u, 0x07u, 's',   't',
                                                'a',   't',   'e',   '/',   'e',   0x05u,
                                                0x02u, 0x00u, 0x00u, 0x00u, 0x01u, 'y'};
    static const uint8_t forwarded_expiring[] = {0x30u, 0x10u, 0x00u, 0x07u, 's',   't',
                                                 'a',   't',   'e',   '/',   'e',   0x05u,
                                                 0x02u, 0x00u, 0x00u, 0x00u, 0x01u, 'y'};
    uint8_t received[32];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;
    flowie_test_socket_t second_subscriber;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    subscriber = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));

    check_int_eq(flowie_test_send(publisher, retained_publish, sizeof(retained_publish)), TURBO_OK);
    check_false(flowie_test_socket_readable(publisher, 50u));
    check_int_eq(flowie_test_send(subscriber, subscribe_rh0, sizeof(subscribe_rh0)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback1)), TURBO_OK);
    check_mem_eq(received, suback1, sizeof(suback1));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(retained_publish)), TURBO_OK);
    check_mem_eq(received, retained_publish, sizeof(retained_publish));

    check_int_eq(flowie_test_send(subscriber, subscribe_rh1, sizeof(subscribe_rh1)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback2)), TURBO_OK);
    check_mem_eq(received, suback2, sizeof(suback2));
    check_false(flowie_test_socket_readable(subscriber, 50u));

    check_int_eq(flowie_test_send(subscriber, subscribe_rh0_again, sizeof(subscribe_rh0_again)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback3)), TURBO_OK);
    check_mem_eq(received, suback3, sizeof(suback3));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(retained_publish)), TURBO_OK);
    check_mem_eq(received, retained_publish, sizeof(retained_publish));

    check_int_eq(flowie_test_send(subscriber, subscribe_rh2, sizeof(subscribe_rh2)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback4)), TURBO_OK);
    check_mem_eq(received, suback4, sizeof(suback4));
    check_false(flowie_test_socket_readable(subscriber, 50u));

    check_int_eq(flowie_test_send(publisher, retained_delete, sizeof(retained_delete)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(forwarded_delete)), TURBO_OK);
    check_mem_eq(received, forwarded_delete, sizeof(forwarded_delete));
    check_int_eq(flowie_test_send(publisher, expiring_retained, sizeof(expiring_retained)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(forwarded_expiring)),
                 TURBO_OK);
    check_mem_eq(received, forwarded_expiring, sizeof(forwarded_expiring));
    turbo_sleep_ms(1100u);

    second_subscriber = flowie_test_connect(port);
    check_true(second_subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(second_subscriber, second_subscriber_connect,
                                  sizeof(second_subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(second_subscriber, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(second_subscriber, subscribe_rh0, sizeof(subscribe_rh0)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(second_subscriber, received, sizeof(suback1)), TURBO_OK);
    check_mem_eq(received, suback1, sizeof(suback1));
    check_false(flowie_test_socket_readable(second_subscriber, 50u));

    flowie_test_socket_close(second_subscriber);
    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("replays broker-owned QoS 2 delivery and completes the subscriber handshake") {
    static const uint8_t publisher_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'q',   '0',   '1'};
    static const uint8_t subscriber_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'q',   '0',   '2'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t resumed_connack[] = {0x20u, 0x03u, 0x01u, 0x00u, 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x03u, 'q',   '/',   '#',   0x02u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x02u};
    static const uint8_t publish[] = {0x34u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                      'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publisher_pubrec[] = {0x50u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubrel[] = {0x62u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubcomp[] = {0x70u, 0x02u, 0x00u, 0x2au};
    static const uint8_t delivery[] = {0x34u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                       'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t replay[] = {0x3cu, 0x09u, 0x00u, 0x03u, 'q', '/',
                                     'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t subscriber_pubrec[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubrel[] = {0x62u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubcomp[] = {0x70u, 0x02u, 0x00u, 0x01u};
    turbo_flow_connection_snapshot_t snapshot = {0};
    uint8_t received[16];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    subscriber = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    check_int_eq(flowie_test_send(publisher, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubrec)), TURBO_OK);
    check_mem_eq(received, publisher_pubrec, sizeof(publisher_pubrec));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(delivery)), TURBO_OK);
    check_mem_eq(received, delivery, sizeof(delivery));
    check_int_eq(flowie_test_send(publisher, publisher_pubrel, sizeof(publisher_pubrel)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubcomp)), TURBO_OK);
    check_mem_eq(received, publisher_pubcomp, sizeof(publisher_pubcomp));

    flowie_test_socket_close(subscriber);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 1u);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(resumed_connack)), TURBO_OK);
    check_mem_eq(received, resumed_connack, sizeof(resumed_connack));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(replay)), TURBO_OK);
    check_mem_eq(received, replay, sizeof(replay));
    check_int_eq(flowie_test_send(subscriber, subscriber_pubrec, sizeof(subscriber_pubrec)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(subscriber_pubrel)), TURBO_OK);
    check_mem_eq(received, subscriber_pubrel, sizeof(subscriber_pubrel));
    check_int_eq(flowie_test_send(subscriber, subscriber_pubcomp, sizeof(subscriber_pubcomp)),
                 TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber, 50u));

    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("publishes abnormal and requested Wills through the configured graph") {
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x10u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x0au, 'w',   'i',   'l',   'l',   '/',
                                        't',   'o',   'p',   'i',   'c',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t normal_disconnect[] = {0xe0u, 0x00u};
    static const uint8_t requested_disconnect[] = {0xe0u, 0x01u, 0x04u};
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_endpoint_capture_t capture;
    uint8_t subscriber_connect[128];
    uint8_t abnormal_connect[128];
    uint8_t normal_connect[128];
    uint8_t requested_connect[128];
    uint8_t expected_abnormal[64];
    uint8_t expected_requested[64];
    uint8_t received[64];
    size_t subscriber_connect_size = 0u;
    size_t abnormal_connect_size = 0u;
    size_t normal_connect_size = 0u;
    size_t requested_connect_size = 0u;
    size_t expected_abnormal_size = 0u;
    size_t expected_requested_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t subscriber;
    flowie_test_socket_t abnormal;
    flowie_test_socket_t normal;
    flowie_test_socket_t requested;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    check_int_eq(flowie_test_encode_connect(subscriber_connect, sizeof(subscriber_connect),
                                            &subscriber_connect_size, "will-sub", 60u, NULL, NULL,
                                            0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(abnormal_connect, sizeof(abnormal_connect),
                                            &abnormal_connect_size, "will-abnormal", 0u,
                                            "will/topic", "offline", 0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(normal_connect, sizeof(normal_connect),
                                            &normal_connect_size, "will-normal", 0u, "will/topic",
                                            "suppressed", 0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(requested_connect, sizeof(requested_connect),
                                            &requested_connect_size, "will-requested", 0u,
                                            "will/topic", "requested", 0u),
                 TURBO_OK);
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = (flowie_mqtt_span_t){(const uint8_t *)"will/topic", strlen("will/topic")};
    publish.payload = (flowie_mqtt_span_t){(const uint8_t *)"offline", strlen("offline")};
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, expected_abnormal,
                                                   sizeof(expected_abnormal),
                                                   &expected_abnormal_size),
                 FLOWIE_MQTT_PARSE_OK);
    publish.payload = (flowie_mqtt_span_t){(const uint8_t *)"requested", strlen("requested")};
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, expected_requested,
                                                   sizeof(expected_requested),
                                                   &expected_requested_size),
                 FLOWIE_MQTT_PARSE_OK);
    flow = flowie_will_flow(port, &capture);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    abnormal = flowie_test_connect(port);
    check_true(abnormal != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(abnormal, abnormal_connect, abnormal_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(abnormal, received, sizeof(connack)), TURBO_OK);
    flowie_test_socket_close(abnormal);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, expected_abnormal_size), TURBO_OK);
    check_mem_eq(received, expected_abnormal, expected_abnormal_size);
    check_size_eq(capture.sizes[0], expected_abnormal_size);
    check_mem_eq(capture.packets[0], expected_abnormal, expected_abnormal_size);

    normal = flowie_test_connect(port);
    check_true(normal != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(normal, normal_connect, normal_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(normal, received, sizeof(connack)), TURBO_OK);
    check_int_eq(flowie_test_send(normal, normal_disconnect, sizeof(normal_disconnect)), TURBO_OK);
    turbo_sleep_ms(100u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    flowie_test_socket_close(normal);

    requested = flowie_test_connect(port);
    check_true(requested != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(requested, requested_connect, requested_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(requested, received, sizeof(connack)), TURBO_OK);
    check_int_eq(flowie_test_send(requested, requested_disconnect, sizeof(requested_disconnect)),
                 TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, expected_requested_size), TURBO_OK);
    check_mem_eq(received, expected_requested, expected_requested_size);
    check_size_eq(capture.sizes[1], expected_requested_size);
    check_mem_eq(capture.packets[1], expected_requested, expected_requested_size);
    flowie_test_socket_close(requested);
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("cancels delayed Will on reconnect and publishes at delay or earlier session expiry") {
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t resumed_connack[] = {0x20u, 0x03u, 0x01u, 0x00u, 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x10u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x0au, 'w',   'i',   'l',   'l',   '/',
                                        't',   'o',   'p',   'i',   'c',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t normal_disconnect[] = {0xe0u, 0x00u};
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_endpoint_capture_t capture;
    turbo_flow_connection_snapshot_t connection_snapshot = {0};
    uint8_t subscriber_connect[128];
    uint8_t delayed_connect[128];
    uint8_t reconnect_packet[128];
    uint8_t expiry_connect[128];
    uint8_t expected_delayed[64];
    uint8_t expected_expiry[64];
    uint8_t received[64];
    size_t subscriber_connect_size = 0u;
    size_t delayed_connect_size = 0u;
    size_t reconnect_size = 0u;
    size_t expiry_connect_size = 0u;
    size_t expected_delayed_size = 0u;
    size_t expected_expiry_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t subscriber;
    flowie_test_socket_t publisher;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    check_int_eq(flowie_test_encode_connect(subscriber_connect, sizeof(subscriber_connect),
                                            &subscriber_connect_size, "will-delay-sub", 60u, NULL,
                                            NULL, 0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(delayed_connect, sizeof(delayed_connect),
                                            &delayed_connect_size, "will-delay", 5u, "will/topic",
                                            "delayed", 1u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(reconnect_packet, sizeof(reconnect_packet),
                                            &reconnect_size, "will-delay", 5u, NULL, NULL, 0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(expiry_connect, sizeof(expiry_connect),
                                            &expiry_connect_size, "will-expiry", 1u, "will/topic",
                                            "expiry", 5u),
                 TURBO_OK);
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = (flowie_mqtt_span_t){(const uint8_t *)"will/topic", strlen("will/topic")};
    publish.payload = (flowie_mqtt_span_t){(const uint8_t *)"delayed", strlen("delayed")};
    check_int_eq(flowie_mqtt_publish_packet_encode(
                     &publish, expected_delayed, sizeof(expected_delayed), &expected_delayed_size),
                 FLOWIE_MQTT_PARSE_OK);
    publish.payload = (flowie_mqtt_span_t){(const uint8_t *)"expiry", strlen("expiry")};
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, expected_expiry,
                                                   sizeof(expected_expiry), &expected_expiry_size),
                 FLOWIE_MQTT_PARSE_OK);
    flow = flowie_will_flow(port, &capture);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(connack)), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);

    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, delayed_connect, delayed_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(connack)), TURBO_OK);
    flowie_test_socket_close(publisher);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connection_snapshot),
                   TURBO_OK);
      if (connection_snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, reconnect_packet, reconnect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(resumed_connack)), TURBO_OK);
    check_mem_eq(received, resumed_connack, sizeof(resumed_connack));
    turbo_sleep_ms(1200u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    check_int_eq(flowie_test_send(publisher, normal_disconnect, sizeof(normal_disconnect)),
                 TURBO_OK);
    flowie_test_socket_close(publisher);

    check_int_eq(flowie_test_encode_connect(delayed_connect, sizeof(delayed_connect),
                                            &delayed_connect_size, "will-delay-live", 5u,
                                            "will/topic", "delayed", 1u),
                 TURBO_OK);
    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, delayed_connect, delayed_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(connack)), TURBO_OK);
    flowie_test_socket_close(publisher);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, expected_delayed_size), TURBO_OK);
    check_mem_eq(received, expected_delayed, expected_delayed_size);

    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, expiry_connect, expiry_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(connack)), TURBO_OK);
    flowie_test_socket_close(publisher);
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, expected_expiry_size), TURBO_OK);
    check_mem_eq(received, expected_expiry, expected_expiry_size);
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("expires an inactive MQTT 5 session and cancels the old deadline on reconnect") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'e',   '0',   '1'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t resumed_connack[] = {0x20u, 0x03u, 0x01u, 0x00u, 0x00u};
    static const uint8_t disconnect_expiry_one[] = {0xe0u, 0x07u, 0x00u, 0x05u, 0x11u,
                                                    0x00u, 0x00u, 0x00u, 0x01u};
    turbo_flow_connection_snapshot_t connection = {0};
    turbo_flow_resource_snapshot_t sessions = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t client;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(client, disconnect_expiry_one, sizeof(disconnect_expiry_one)),
                 TURBO_OK);
    flowie_test_socket_close(client);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connection), TURBO_OK);
      if (connection.connections_current == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(connection.connections_current, 0u);
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 2u, &sessions), TURBO_OK);
    check_size_eq(sessions.load, 1u);

    turbo_sleep_ms(500u);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(resumed_connack)), TURBO_OK);
    check_mem_eq(received, resumed_connack, sizeof(resumed_connack));
    turbo_sleep_ms(700u);
    sessions = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 2u, &sessions), TURBO_OK);
    check_size_eq(sessions.load, 1u);

    check_int_eq(flowie_test_send(client, disconnect_expiry_one, sizeof(disconnect_expiry_one)),
                 TURBO_OK);
    flowie_test_socket_close(client);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      sessions = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
      check_int_eq(turbo_flow_resource_snapshot_at(flow, 2u, &sessions), TURBO_OK);
      if (sessions.load == 0u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(sessions.load, 0u);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("disconnects only a stalled fan-out subscriber at its byte HWM") {
    enum {
      STALLED_PAYLOAD_BYTES = 512u * 1024u,
      STALLED_PACKET_CAPACITY = STALLED_PAYLOAD_BYTES + 64u,
      STALLED_SEND_HWM_BYTES = 768u * 1024u,
      STALLED_MESSAGES = 4u
    };
    static const uint8_t connect_template[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                               'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                               0x00u, 0x00u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, '#',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    uint8_t connects[3][sizeof(connect_template)];
    uint8_t reply[8];
    uint8_t *payload = (uint8_t *)calloc(STALLED_PAYLOAD_BYTES, 1u);
    uint8_t *wire = (uint8_t *)malloc(STALLED_PACKET_CAPACITY);
    uint8_t *received = (uint8_t *)malloc(STALLED_PACKET_CAPACITY);
    size_t wire_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_resource_document_t queue_status = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    tstr_t queue_json = NULL;
    turbo_flow_t *flow =
        flowie_fanout_flow_with_limits(port, STALLED_PACKET_CAPACITY, STALLED_SEND_HWM_BYTES, 8u);
    flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t slow = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t fast = FLOWIE_TEST_INVALID_SOCKET;
    check_int_gt(port, 0);
    check_not_null(payload);
    check_not_null(wire);
    check_not_null(received);
    check_not_null(flow);
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = (flowie_mqtt_span_t){(const uint8_t *)"h", 1u};
    publish.payload = (flowie_mqtt_span_t){payload, STALLED_PAYLOAD_BYTES};
    check_int_eq(
        flowie_mqtt_publish_packet_encode(&publish, wire, STALLED_PACKET_CAPACITY, &wire_size),
        FLOWIE_MQTT_PARSE_OK);
    check_size_gt(wire_size, STALLED_SEND_HWM_BYTES / 2u);
    check_size_lt(wire_size, STALLED_SEND_HWM_BYTES);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    slow = flowie_test_connect(port);
    fast = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(slow != FLOWIE_TEST_INVALID_SOCKET);
    check_true(fast != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_socket_set_recv_buffer(slow, 1024u), TURBO_OK);
    for (size_t i = 0u; i < 3u; ++i) {
      flowie_test_socket_t client = i == 0u ? publisher : (i == 1u ? slow : fast);
      memcpy(connects[i], connect_template, sizeof(connect_template));
      memcpy(connects[i] + sizeof(connect_template) - 3u,
             i == 0u   ? "pub"
             : i == 1u ? "slw"
                       : "fst",
             3u);
      check_int_eq(flowie_test_send(client, connects[i], sizeof(connects[i])), TURBO_OK);
      check_int_eq(flowie_test_recv_exact(client, reply, sizeof(connack)), TURBO_OK);
      check_mem_eq(reply, connack, sizeof(connack));
    }
    check_int_eq(flowie_test_send(slow, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(slow, reply, sizeof(suback)), TURBO_OK);
    check_mem_eq(reply, suback, sizeof(suback));
    check_int_eq(flowie_test_send(fast, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast, reply, sizeof(suback)), TURBO_OK);
    check_mem_eq(reply, suback, sizeof(suback));
    for (size_t i = 0u; i < STALLED_MESSAGES; ++i) {
      wire[wire_size - 1u] = (uint8_t)i;
      check_int_eq(flowie_test_send(publisher, wire, wire_size), TURBO_OK);
      check_int_eq(flowie_test_recv_exact(fast, received, wire_size), TURBO_OK);
      check_mem_eq(received, wire, wire_size);
    }
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 2u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 2u);
    check_int_eq(turbo_flow_resource_document_at(flow, 1u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
                                                 &queue_status),
                 TURBO_OK);
    queue_json = tstr_new_len(mem_buffer_const_data(queue_status.payload),
                              mem_buffer_used(queue_status.payload));
    check_not_null(queue_json);
    check_not_null(strstr(queue_json, "\"slow_subscriber_disconnects\":\"1\""));
    check_not_null(strstr(queue_json, "\"saturated\":false"));
    tstr_freep(&queue_json);
    turbo_flow_resource_document_cleanup(&queue_status);
    check_int_eq(flowie_test_send(fast, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast, reply, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(reply, pingresp, sizeof(pingresp));
    flowie_test_socket_close(fast);
    flowie_test_socket_close(slow);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    free(received);
    free(wire);
    free(payload);
  }

  it("disconnects only the subscriber whose inflight quota is exhausted") {
    static const uint8_t connect_template[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                               'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                               0x00u, 0x00u, 0x03u, 'x',   '0',   '1'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x03u, 'q',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t publish_first[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                            'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publish_second[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t publisher_puback_first[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_puback_second[] = {0x40u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t delivery_first[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t delivery_second[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                              'a',   0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t fast_puback[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    uint8_t connects[3][sizeof(connect_template)];
    uint8_t received[16];
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_resource_document_t queue_status = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    tstr_t queue_json = NULL;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow_with_inflight(port, 1u);
    flowie_test_socket_t publisher;
    flowie_test_socket_t slow;
    flowie_test_socket_t fast;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    slow = flowie_test_connect(port);
    fast = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(slow != FLOWIE_TEST_INVALID_SOCKET);
    check_true(fast != FLOWIE_TEST_INVALID_SOCKET);
    for (size_t i = 0u; i < 3u; ++i) {
      flowie_test_socket_t client = i == 0u ? publisher : (i == 1u ? slow : fast);
      memcpy(connects[i], connect_template, sizeof(connect_template));
      connects[i][sizeof(connect_template) - 3u] = (uint8_t)"psf"[i];
      check_int_eq(flowie_test_send(client, connects[i], sizeof(connects[i])), TURBO_OK);
      check_int_eq(flowie_test_recv_exact(client, received, sizeof(connack)), TURBO_OK);
      check_mem_eq(received, connack, sizeof(connack));
    }
    check_int_eq(flowie_test_send(slow, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(slow, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_int_eq(flowie_test_send(fast, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    check_int_eq(flowie_test_send(publisher, publish_first, sizeof(publish_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_puback_first)),
                 TURBO_OK);
    check_mem_eq(received, publisher_puback_first, sizeof(publisher_puback_first));
    check_int_eq(flowie_test_recv_exact(slow, received, sizeof(delivery_first)), TURBO_OK);
    check_mem_eq(received, delivery_first, sizeof(delivery_first));
    check_int_eq(flowie_test_recv_exact(fast, received, sizeof(delivery_first)), TURBO_OK);
    check_mem_eq(received, delivery_first, sizeof(delivery_first));
    check_int_eq(flowie_test_send(fast, fast_puback, sizeof(fast_puback)), TURBO_OK);
    check_int_eq(flowie_test_send(fast, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));

    check_int_eq(flowie_test_send(publisher, publish_second, sizeof(publish_second)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_puback_second)),
                 TURBO_OK);
    check_mem_eq(received, publisher_puback_second, sizeof(publisher_puback_second));
    check_int_eq(flowie_test_recv_exact(fast, received, sizeof(delivery_second)), TURBO_OK);
    check_mem_eq(received, delivery_second, sizeof(delivery_second));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 2u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 2u);
    check_int_eq(turbo_flow_resource_document_at(flow, 1u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
                                                 &queue_status),
                 TURBO_OK);
    queue_json = tstr_new_len(mem_buffer_const_data(queue_status.payload),
                              mem_buffer_used(queue_status.payload));
    check_not_null(queue_json);
    check_not_null(strstr(queue_json, "\"slow_subscriber_disconnects\":\"1\""));
    check_not_null(strstr(queue_json, "\"saturated\":false"));
    check_not_null(strstr(queue_json, "\"accepting\":true"));
    tstr_freep(&queue_json);
    turbo_flow_resource_document_cleanup(&queue_status);
    check_int_eq(flowie_test_send(fast, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));

    flowie_test_socket_close(fast);
    flowie_test_socket_close(slow);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("closes only the connection whose reply exceeds the configured byte HWM") {
    static const uint8_t connect_packet[] = {
        0x10u, 0x17u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x07u,
        0x15u, 0x00u, 0x04u, 'n',   'o', 'n', 'e', 0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t oversized_publish[] = {0x30u, 0x05u, 0x00u, 0x01u, 'a', 0x00u, 'x'};
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    uint8_t received[8];
    uint8_t healthy_connect[sizeof(connect_packet)];
    unsigned short port = flowie_test_port();
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_t *flow = flowie_reply_flow(port, sizeof(connack));
    flowie_test_socket_t slow;
    flowie_test_socket_t healthy;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    slow = flowie_test_connect(port);
    healthy = flowie_test_connect(port);
    check_true(slow != FLOWIE_TEST_INVALID_SOCKET);
    check_true(healthy != FLOWIE_TEST_INVALID_SOCKET);
    memcpy(healthy_connect, connect_packet, sizeof(connect_packet));
    healthy_connect[sizeof(healthy_connect) - 1u] = 'h';
    check_int_eq(flowie_test_send(slow, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(slow, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    check_int_eq(flowie_test_send(healthy, healthy_connect, sizeof(healthy_connect)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(healthy, received, sizeof(connack)), TURBO_OK);
    check_mem_eq(received, connack, sizeof(connack));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 2u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 2u);
    check_int_eq(flowie_test_send(slow, oversized_publish, sizeof(oversized_publish)), TURBO_OK);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 1u);
    check_int_eq(flowie_test_send(healthy, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(healthy, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(healthy);
    flowie_test_socket_close(slow);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }
}
