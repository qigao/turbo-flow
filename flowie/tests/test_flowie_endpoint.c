#include "flowie.h"
#include "flowie_test_socket.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_uuid.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define FLOWIE_TEST_WAIT_STEPS 2000u
#define FLOWIE_TEST_EPOCH_WAIT_STEPS 4000u

typedef struct flowie_endpoint_capture_s {
  atomic_size_t calls;
  uint32_t types[4];
  size_t sizes[4];
  uint8_t packets[4][32];
  int result;
} flowie_endpoint_capture_t;

typedef struct flowie_security_fixture_s {
  size_t calls;
  uint64_t expires_at;
} flowie_security_fixture_t;

typedef struct flowie_policy_fixture_s {
  turbo_flow_security_rule_t rules[2];
} flowie_policy_fixture_t;

typedef struct flowie_enhanced_security_fixture_s {
  size_t begin_calls;
  size_t continue_calls;
  size_t cancel_calls;
  uint64_t first_expires_at;
  uint64_t next_expires_at;
} flowie_enhanced_security_fixture_t;

static void flowie_test_security_principal(turbo_flow_security_principal_t *principal,
                                           const char *method) {
  *principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)snprintf(principal->principal_id, sizeof(principal->principal_id), "%s", "writer");
  (void)snprintf(principal->principal_type, sizeof(principal->principal_type), "%s", "device");
  (void)snprintf(principal->root_group_id, sizeof(principal->root_group_id), "%s", "root-a");
  (void)snprintf(principal->auth_method, sizeof(principal->auth_method), "%s", method);
  principal->scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  principal->role_count = 1u;
  (void)snprintf(principal->roles[0], sizeof(principal->roles[0]), "%s", "writer");
  principal->group_count = 1u;
  (void)snprintf(principal->groups[0], sizeof(principal->groups[0]), "%s", "root-a");
  principal->policy_version = 1u;
}

static int flowie_test_enhanced_begin(void *ctx,
                                      const turbo_flow_security_enhanced_auth_request_t *request,
                                      void **exchange_out,
                                      turbo_flow_security_enhanced_auth_result_t *result_out) {
  static const uint8_t challenge[] = "server-first";
  flowie_enhanced_security_fixture_t *fixture = (flowie_enhanced_security_fixture_t *)ctx;
  if (!fixture || !request || !exchange_out || !result_out ||
      strcmp(request->method, "challenge") != 0 ||
      request->data_size != sizeof("client-first") - 1u ||
      memcmp(request->data, "client-first", sizeof("client-first") - 1u) != 0)
    return TURBO_EPERM;
  ++fixture->begin_calls;
  *exchange_out = fixture;
  result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE;
  result_out->data = challenge;
  result_out->data_size = sizeof(challenge) - 1u;
  return TURBO_OK;
}

static int flowie_test_enhanced_continue(void *ctx, void *exchange,
                                         const turbo_flow_security_enhanced_auth_request_t *request,
                                         turbo_flow_security_enhanced_auth_result_t *result_out) {
  static const uint8_t final_data[] = "server-final";
  flowie_enhanced_security_fixture_t *fixture = (flowie_enhanced_security_fixture_t *)ctx;
  if (!fixture || exchange != fixture || !request || !result_out ||
      strcmp(request->method, "challenge") != 0 ||
      request->data_size != sizeof("client-final") - 1u ||
      memcmp(request->data, "client-final", sizeof("client-final") - 1u) != 0)
    return TURBO_EPERM;
  ++fixture->continue_calls;
  result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS;
  result_out->data = final_data;
  result_out->data_size = sizeof(final_data) - 1u;
  flowie_test_security_principal(&result_out->principal, "challenge");
  result_out->principal.expires_at =
      fixture->continue_calls == 1u ? fixture->first_expires_at : fixture->next_expires_at;
  return TURBO_OK;
}

static void flowie_test_enhanced_cancel(void *ctx, void *exchange) {
  flowie_enhanced_security_fixture_t *fixture = (flowie_enhanced_security_fixture_t *)ctx;
  if (fixture && exchange == fixture) ++fixture->cancel_calls;
}

static int flowie_test_policy_load(void *ctx, uint64_t required_version,
                                   turbo_flow_security_policy_bundle_t *bundle) {
  flowie_policy_fixture_t *fixture = (flowie_policy_fixture_t *)ctx;
  if (!fixture || !bundle || bundle->size < sizeof(*bundle) ||
      (required_version != 0u && required_version != 1u))
    return TURBO_EINVAL;
  bundle->policy_version = 1u;
  bundle->rules = fixture->rules;
  bundle->rule_count = 2u;
  bundle->provider_bundle = fixture;
  return TURBO_OK;
}

static void flowie_test_policy_release(void *ctx, turbo_flow_security_policy_bundle_t *bundle) {
  (void)ctx;
  if (bundle) *bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
}

static int flowie_test_authenticate(void *ctx, const turbo_flow_security_auth_request_t *request,
                                    turbo_flow_security_principal_t *principal) {
  flowie_security_fixture_t *fixture = (flowie_security_fixture_t *)ctx;
  if (!fixture || !request || !principal) return TURBO_EINVAL;
  ++fixture->calls;
  if (strcmp(request->identity, "writer") != 0 || strcmp(request->method, "password") != 0 ||
      request->secret_size != sizeof("secret") - 1u ||
      memcmp(request->secret, "secret", sizeof("secret") - 1u) != 0)
    return TURBO_EPERM;
  flowie_test_security_principal(principal, request->method);
  principal->expires_at = fixture->expires_at;
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

static int flowie_test_wait_epoch(uint64_t deadline) {
  for (size_t i = 0u; i < FLOWIE_TEST_EPOCH_WAIT_STEPS; ++i) {
    time_t now = time(NULL);
    if (now >= 0 && (uint64_t)now >= deadline) return TURBO_OK;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_test_recv_connack_ex(flowie_test_socket_t socket, uint8_t session_present,
                                       uint8_t reason_code, char *assigned_client_id,
                                       size_t assigned_client_id_capacity, char *auth_method,
                                       size_t auth_method_capacity, char *auth_data,
                                       size_t auth_data_capacity) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  uint8_t wire[256];
  uint32_t remaining = 0u;
  uint32_t multiplier = 1u;
  size_t fixed_size = 1u;
  size_t consumed = 0u;
  int receive_maximum_seen = 0;
  int maximum_packet_size_seen = 0;
  int rc;
  if (assigned_client_id && assigned_client_id_capacity != 0u) assigned_client_id[0] = '\0';
  if (auth_method && auth_method_capacity != 0u) auth_method[0] = '\0';
  if (auth_data && auth_data_capacity != 0u) auth_data[0] = '\0';
  rc = flowie_test_recv_exact(socket, wire, 1u);
  if (rc != TURBO_OK || wire[0] != UINT8_C(0x20)) return TURBO_EPROTO;
  do {
    uint8_t byte;
    if (fixed_size >= 5u || flowie_test_recv_exact(socket, &byte, 1u) != TURBO_OK)
      return TURBO_EPROTO;
    wire[fixed_size++] = byte;
    remaining += (uint32_t)(byte & UINT8_C(0x7f)) * multiplier;
    if ((byte & UINT8_C(0x80)) == 0u) break;
    multiplier *= 128u;
  } while (1);
  if (remaining > sizeof(wire) - fixed_size ||
      flowie_test_recv_exact(socket, wire + fixed_size, remaining) != TURBO_OK)
    return TURBO_EPROTO;
  options.version = FLOWIE_MQTT_VERSION_5;
  options.max_packet_size = sizeof(wire);
  rc = flowie_mqtt_packet_parse(wire, fixed_size + remaining, &options, &packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != fixed_size + remaining ||
      flowie_mqtt_control_packet_parse(&packet, &control) != FLOWIE_MQTT_PARSE_OK ||
      control.type != FLOWIE_MQTT_PACKET_CONNACK || control.session_present != session_present ||
      control.reason_code != reason_code)
    return TURBO_EPROTO;
  if (reason_code != 0u) return TURBO_OK;
  if (control.properties.values.size != 0u) {
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    rc = flowie_mqtt_property_iterator_init(&control.properties, &iterator);
    if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) ==
           FLOWIE_MQTT_PARSE_OK) {
      if (property.identifier == FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM)
        receive_maximum_seen = property.integer != 0u;
      else if (property.identifier == FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE)
        maximum_packet_size_seen = property.integer != 0u;
      else if (property.identifier == FLOWIE_MQTT_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER &&
               assigned_client_id && property.value.size < assigned_client_id_capacity) {
        memcpy(assigned_client_id, property.value.data, property.value.size);
        assigned_client_id[property.value.size] = '\0';
      } else if (property.identifier == FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD && auth_method &&
                 property.value.size < auth_method_capacity) {
        memcpy(auth_method, property.value.data, property.value.size);
        auth_method[property.value.size] = '\0';
      } else if (property.identifier == FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA && auth_data &&
                 property.value.size < auth_data_capacity) {
        memcpy(auth_data, property.value.data, property.value.size);
        auth_data[property.value.size] = '\0';
      }
    }
    if (rc != FLOWIE_MQTT_PARSE_NEED_MORE) return TURBO_EPROTO;
  }
  return receive_maximum_seen && maximum_packet_size_seen ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_test_recv_connack(flowie_test_socket_t socket, uint8_t session_present,
                                    uint8_t reason_code) {
  return flowie_test_recv_connack_ex(socket, session_present, reason_code, NULL, 0u, NULL, 0u, NULL,
                                     0u);
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

static int flowie_test_auth_properties_encode(const char *method, const char *data, uint8_t *output,
                                              size_t capacity, size_t *written) {
  size_t method_size;
  size_t data_size;
  size_t offset = 0u;
  if (!method || !method[0] || !data || !output || !written) return TURBO_EINVAL;
  method_size = strlen(method);
  data_size = strlen(data);
  if (method_size > UINT16_MAX || data_size > UINT16_MAX ||
      method_size > SIZE_MAX - data_size - 6u || capacity < method_size + data_size + 6u)
    return TURBO_ENOSPC;
  output[offset++] = FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD;
  output[offset++] = (uint8_t)(method_size >> 8u);
  output[offset++] = (uint8_t)method_size;
  memcpy(output + offset, method, method_size);
  offset += method_size;
  output[offset++] = FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA;
  output[offset++] = (uint8_t)(data_size >> 8u);
  output[offset++] = (uint8_t)data_size;
  memcpy(output + offset, data, data_size);
  offset += data_size;
  *written = offset;
  return TURBO_OK;
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
  config.topic_alias_maximum = 16u;
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
  static const char graph[] =
      "source mqtt_in adapter flowie.endpoint operation " FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION "\n"
      "stage build_reply\n"
      "stage mqtt_reply adapter flowie.endpoint operation " FLOWIE_MQTT_PACKET_EGRESS_OPERATION "\n"
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
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.abi_version = FLOWIE_ENDPOINT_ABI_V8;
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
    check_str_eq(turbo_flow_adapter_operation_module(flow, "flowie.endpoint",
                                                     FLOWIE_MQTT_PUBLISH_INGRESS_OPERATION),
                 FLOWIE_MQTT_SERVER_MODULE);
    check_str_eq(turbo_flow_adapter_operation_module(flow, "flowie.endpoint",
                                                     FLOWIE_MQTT_PACKET_EGRESS_OPERATION),
                 FLOWIE_MQTT_SERVER_MODULE);
    if (!flow) return;
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(resumed, 0u, 0u), TURBO_OK);
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

  it("authenticates CONNECT and authorizes from a referenced dynamic ACL provider") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  acl.test:\n"
                               "    kind: acl_provider\n"
                               "    config:\n"
                               "      backend: test\n"
                               "  security.main:\n"
                               "    kind: security_realm\n"
                               "    config:\n"
                               "      resource_uid: security:main\n"
                               "      owner_name: security.main\n"
                               "      policy_source: acl.test\n"
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
    static const uint8_t publish_allowed[] = {0x32u, 0x13u, 0x00u, 0x0du, 'r',   'o',   'o',
                                              't',   '-',   'a',   '/',   'e',   'v',   'e',
                                              'n',   't',   's',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t publish_denied[] = {0x32u, 0x13u, 0x00u, 0x0du, 'r',   'o',   'o',
                                             't',   '-',   'b',   '/',   'e',   'v',   'e',
                                             'n',   't',   's',   0x00u, 0x02u, 0x00u, 'x'};
    static const uint8_t puback_ok[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t puback_denied[] = {0x40u, 0x04u, 0x00u, 0x02u, 0x87u, 0x00u};
    static const uint8_t subscribe_allowed[] = {0x82u, 0x0eu, 0x00u, 0x03u, 0x00u, 0x00u,
                                                0x08u, 'r',   'o',   'o',   't',   '-',
                                                'a',   '/',   '#',   0x01u};
    static const uint8_t subscribe_denied[] = {0x82u, 0x07u, 0x00u, 0x04u, 0x00u,
                                               0x00u, 0x01u, '#',   0x01u};
    static const uint8_t suback_allowed[] = {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x01u};
    static const uint8_t suback_denied[] = {0x90u, 0x04u, 0x00u, 0x04u, 0x00u, 0x87u};
    static const uint8_t connack_v31[] = {0x20u, 0x02u, 0x00u, 0x00u};
    static const uint8_t normal_disconnect[] = {0xe0u, 0x00u};
    static const uint8_t subscribe_denied_v31[] = {0x82u, 0x06u, 0x00u, 0x04u,
                                                   0x00u, 0x01u, '#',   0x01u};
    flowie_endpoint_capture_t capture;
    flowie_security_fixture_t auth = {0};
    flowie_policy_fixture_t policy = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_policy_provider_t policy_provider = {
        sizeof(policy_provider), &policy, flowie_test_policy_load, flowie_test_policy_release};
    turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packet[128];
    uint8_t bad_connect_packet[128];
    uint8_t legacy_connect_packet[128];
    uint8_t allowed_will_connect_packet[160];
    uint8_t denied_will_connect_packet[160];
    uint8_t received[8];
    size_t connect_size = 0u;
    size_t bad_connect_size = 0u;
    size_t legacy_connect_size = 0u;
    size_t allowed_will_connect_size = 0u;
    size_t denied_will_connect_size = 0u;
    unsigned short port = flowie_test_port();
    flowie_test_socket_t client;
    flowie_test_socket_t legacy;
    flowie_test_socket_t rejected;
    flowie_test_socket_t allowed_will;
    flowie_test_socket_t denied_will;

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
    policy.rules[0] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
    policy.rules[0].effect = TURBO_FLOW_SECURITY_ALLOW;
    policy.rules[0].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    (void)snprintf(policy.rules[0].subject, sizeof(policy.rules[0].subject), "%s", "writer");
    (void)snprintf(policy.rules[0].root_group_id, sizeof(policy.rules[0].root_group_id), "%s",
                   "root-a");
    policy.rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    policy.rules[0].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    policy.rules[0].match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    (void)snprintf(policy.rules[0].pattern, sizeof(policy.rules[0].pattern), "%s", "secure-");
    policy.rules[1] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
    policy.rules[1].effect = TURBO_FLOW_SECURITY_ALLOW;
    policy.rules[1].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    (void)snprintf(policy.rules[1].subject, sizeof(policy.rules[1].subject), "%s", "writer");
    (void)snprintf(policy.rules[1].root_group_id, sizeof(policy.rules[1].root_group_id), "%s",
                   "root-a");
    policy.rules[1].action_mask =
        TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    policy.rules[1].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    policy.rules[1].match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
    (void)snprintf(policy.rules[1].pattern, sizeof(policy.rules[1].pattern), "%s", "root-a/#");
    check_int_eq(turbo_flow_security_realm_create_resolved(resolved, "security.main", &matcher,
                                                           &realm, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_security_realm_bind_policy_provider(realm, &policy_provider), TURBO_OK);
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
    connect.version = FLOWIE_MQTT_VERSION_3_1;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-3", 8u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, legacy_connect_packet,
                                                   sizeof(legacy_connect_packet),
                                                   &legacy_connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-will-allowed", 19u};
    connect.has_will = 1u;
    connect.will_topic = (flowie_mqtt_span_t){(const uint8_t *)"root-a/will", 11u};
    connect.will_payload = (flowie_mqtt_span_t){(const uint8_t *)"offline", 7u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, allowed_will_connect_packet,
                                                   sizeof(allowed_will_connect_packet),
                                                   &allowed_will_connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-will-denied", 18u};
    connect.will_topic = (flowie_mqtt_span_t){(const uint8_t *)"root-b/will", 11u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, denied_will_connect_packet,
                                                   sizeof(denied_will_connect_packet),
                                                   &denied_will_connect_size),
                 FLOWIE_MQTT_PARSE_OK);

    rejected = flowie_test_connect(port);
    check_true(rejected != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(rejected, bad_connect_packet, bad_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(rejected, 0u, UINT8_C(0x86)), TURBO_OK);
    flowie_test_socket_close(rejected);

    denied_will = flowie_test_connect(port);
    check_true(denied_will != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(
        flowie_test_send(denied_will, denied_will_connect_packet, denied_will_connect_size),
        TURBO_OK);
    check_int_eq(flowie_test_recv_connack(denied_will, 0u, UINT8_C(0x87)), TURBO_OK);
    flowie_test_socket_close(denied_will);

    allowed_will = flowie_test_connect(port);
    check_true(allowed_will != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(
        flowie_test_send(allowed_will, allowed_will_connect_packet, allowed_will_connect_size),
        TURBO_OK);
    check_int_eq(flowie_test_recv_connack(allowed_will, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(allowed_will, normal_disconnect, sizeof(normal_disconnect)),
                 TURBO_OK);
    flowie_test_socket_close(allowed_will);

    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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

    legacy = flowie_test_connect(port);
    check_true(legacy != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(legacy, legacy_connect_packet, legacy_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(legacy, received, sizeof(connack_v31)), TURBO_OK);
    check_mem_eq(received, connack_v31, sizeof(connack_v31));
    check_int_eq(flowie_test_send(legacy, subscribe_denied_v31, sizeof(subscribe_denied_v31)),
                 TURBO_OK);
    check_true(flowie_test_socket_readable(legacy, 500u));
    check(flowie_test_recv_exact(legacy, received, 1u) != TURBO_OK);
    check_size_eq(auth.calls, 5u);

    flowie_test_socket_close(legacy);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("disconnects idle MQTT 5 and MQTT 3 connections when their principals expire") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const uint8_t connack_v311[] = {0x20u, 0x02u, 0x00u, 0x00u};
    static const uint8_t expired_disconnect[] = {0xe0u, 0x01u, 0x87u};
    flowie_endpoint_capture_t capture = {0};
    flowie_security_fixture_t auth = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_v5[128];
    uint8_t connect_v311[128];
    uint8_t received[sizeof(connack_v311)];
    size_t connect_v5_size = 0u;
    size_t connect_v311_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client_v5;
    flowie_test_socket_t client_v311;
    time_t now;

    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    check_not_null(flow);
    rule.effect = TURBO_FLOW_SECURITY_ALLOW;
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    (void)snprintf(rule.subject, sizeof(rule.subject), "%s", "writer");
    (void)snprintf(rule.root_group_id, sizeof(rule.root_group_id), "%s", "root-a");
    rule.action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    (void)snprintf(rule.pattern, sizeof(rule.pattern), "%s", "secure-");
    realm_config.resource_uid = "security:principal-expiry-test";
    realm_config.owner_name = "security.principal-expiry-test";
    realm_config.policy_version = 1u;
    realm_config.rules = &rule;
    realm_config.rule_count = 1u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);
    security.realm_channel = "security.principal-expiry-test";
    security.auth_method = "password";
    security.auth_provider = &provider;
    security.realm = realm;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_connections = 2u;
    config.recv_timeout_ms = 0u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 2u;
    config.max_inflight_per_session = 2u;
    check_int_eq(flowie_register_secure_endpoint(flow, "flowie.endpoint", &config, &security),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    now = time(NULL);
    check_true(now >= 0);
    auth.expires_at = (uint64_t)now + 2u;
    connect.clean_start = 1u;
    connect.keep_alive = 0u;
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-expiry-v5", 16u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_v5, sizeof(connect_v5),
                                                   &connect_v5_size),
                 FLOWIE_MQTT_PARSE_OK);
    connect.version = FLOWIE_MQTT_VERSION_3_1_1;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-expiry-v311", 18u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_v311, sizeof(connect_v311),
                                                   &connect_v311_size),
                 FLOWIE_MQTT_PARSE_OK);

    client_v5 = flowie_test_connect(port);
    client_v311 = flowie_test_connect(port);
    check_true(client_v5 != FLOWIE_TEST_INVALID_SOCKET);
    check_true(client_v311 != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client_v5, connect_v5, connect_v5_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client_v5, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client_v311, connect_v311, connect_v311_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(connack_v311)), TURBO_OK);
    check_mem_eq(received, connack_v311, sizeof(connack_v311));
    check_int_eq(flowie_test_wait_epoch(auth.expires_at), TURBO_OK);

    check_true(flowie_test_socket_readable(client_v5, 1500u));
    check_int_eq(flowie_test_recv_exact(client_v5, received, sizeof(expired_disconnect)), TURBO_OK);
    check_mem_eq(received, expired_disconnect, sizeof(expired_disconnect));
    check_true(flowie_test_socket_readable(client_v311, 1500u));
    check_int_ne(flowie_test_recv_exact(client_v311, received, 1u), TURBO_OK);
    check_size_eq(auth.calls, 2u);

    flowie_test_socket_close(client_v311);
    flowie_test_socket_close(client_v5);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
  }

  it("completes initial Enhanced AUTH and connected MQTT 5 re-authentication") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    static const uint8_t expired_disconnect[] = {0xe0u, 0x01u, 0x87u};
    flowie_endpoint_capture_t capture = {0};
    flowie_security_fixture_t basic_fixture = {0};
    flowie_enhanced_security_fixture_t enhanced_fixture = {0};
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_security_auth_provider_t basic_provider = {sizeof(basic_provider), &basic_fixture,
                                                          flowie_test_authenticate};
    turbo_flow_security_enhanced_auth_provider_t enhanced_provider = {
        sizeof(enhanced_provider), &enhanced_fixture, flowie_test_enhanced_begin,
        flowie_test_enhanced_continue, flowie_test_enhanced_cancel};
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_control_packet_t auth = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    uint8_t client_first_properties[64];
    uint8_t client_final_properties[64];
    uint8_t server_first_properties[64];
    uint8_t server_final_properties[64];
    uint8_t connect_packet[192];
    uint8_t client_continue[128];
    uint8_t client_reauth[128];
    uint8_t server_challenge[128];
    uint8_t server_success[128];
    uint8_t received[128];
    char connack_method[32];
    char connack_data[32];
    size_t client_first_size = 0u;
    size_t client_final_size = 0u;
    size_t server_first_size = 0u;
    size_t server_final_size = 0u;
    size_t connect_size = 0u;
    size_t client_continue_size = 0u;
    size_t client_reauth_size = 0u;
    size_t server_challenge_size = 0u;
    size_t server_success_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client;
    time_t now;

    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    check_not_null(flow);
    rule.effect = TURBO_FLOW_SECURITY_ALLOW;
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    (void)snprintf(rule.subject, sizeof(rule.subject), "%s", "writer");
    (void)snprintf(rule.root_group_id, sizeof(rule.root_group_id), "%s", "root-a");
    rule.action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    (void)snprintf(rule.pattern, sizeof(rule.pattern), "%s", "secure-");
    realm_config.resource_uid = "security:enhanced-test";
    realm_config.owner_name = "security.enhanced-test";
    realm_config.policy_version = 1u;
    realm_config.rules = &rule;
    realm_config.rule_count = 1u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);

    security.realm_channel = "security.enhanced-test";
    security.auth_method = "challenge";
    security.auth_provider = &basic_provider;
    security.enhanced_auth_provider = &enhanced_provider;
    security.realm = realm;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_connections = 2u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 2u;
    config.max_inflight_per_session = 2u;
    check_int_eq(flowie_register_secure_endpoint(flow, "flowie.endpoint", &config, &security),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);

    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "client-first", client_first_properties,
                                           sizeof(client_first_properties), &client_first_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "client-final", client_final_properties,
                                           sizeof(client_final_properties), &client_final_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "server-first", server_first_properties,
                                           sizeof(server_first_properties), &server_first_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "server-final", server_final_properties,
                                           sizeof(server_final_properties), &server_final_size),
        TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"secure-enhanced", 15u};
    connect.has_username = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.properties = (flowie_mqtt_span_t){client_first_properties, client_first_size};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    auth.version = FLOWIE_MQTT_VERSION_5;
    auth.type = FLOWIE_MQTT_PACKET_AUTH;
    auth.reason_code = UINT8_C(0x18);
    auth.properties = (flowie_mqtt_span_t){client_final_properties, client_final_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, client_continue, sizeof(client_continue),
                                                   &client_continue_size),
                 FLOWIE_MQTT_PARSE_OK);
    auth.reason_code = UINT8_C(0x19);
    auth.properties = (flowie_mqtt_span_t){client_first_properties, client_first_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, client_reauth, sizeof(client_reauth),
                                                   &client_reauth_size),
                 FLOWIE_MQTT_PARSE_OK);
    auth.reason_code = UINT8_C(0x18);
    auth.properties = (flowie_mqtt_span_t){server_first_properties, server_first_size};
    check_int_eq(flowie_mqtt_control_packet_encode(
                     &auth, server_challenge, sizeof(server_challenge), &server_challenge_size),
                 FLOWIE_MQTT_PARSE_OK);
    auth.reason_code = UINT8_C(0x00);
    auth.properties = (flowie_mqtt_span_t){server_final_properties, server_final_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, server_success, sizeof(server_success),
                                                   &server_success_size),
                 FLOWIE_MQTT_PARSE_OK);

    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    now = time(NULL);
    check_true(now >= 0);
    enhanced_fixture.first_expires_at = (uint64_t)now + 3u;
    enhanced_fixture.next_expires_at = (uint64_t)now + 6u;
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_challenge_size), TURBO_OK);
    check_mem_eq(received, server_challenge, server_challenge_size);
    check_int_eq(flowie_test_send(client, client_continue, client_continue_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack_ex(client, 0u, 0u, NULL, 0u, connack_method,
                                             sizeof(connack_method), connack_data,
                                             sizeof(connack_data)),
                 TURBO_OK);
    check_str_eq(connack_method, "challenge");
    check_str_eq(connack_data, "server-final");

    check_int_eq(flowie_test_send(client, client_reauth, client_reauth_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_challenge_size), TURBO_OK);
    check_mem_eq(received, server_challenge, server_challenge_size);
    check_int_eq(flowie_test_send(client, client_continue, client_continue_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_success_size), TURBO_OK);
    check_mem_eq(received, server_success, server_success_size);
    check_int_eq(flowie_test_wait_epoch(enhanced_fixture.first_expires_at), TURBO_OK);
    check_int_eq(flowie_test_send(client, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    check_int_eq(flowie_test_wait_epoch(enhanced_fixture.next_expires_at), TURBO_OK);
    check_true(flowie_test_socket_readable(client, 1500u));
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(expired_disconnect)), TURBO_OK);
    check_mem_eq(received, expired_disconnect, sizeof(expired_disconnect));
    check_size_eq(enhanced_fixture.begin_calls, 2u);
    check_size_eq(enhanced_fixture.continue_calls, 2u);
    check_size_eq(enhanced_fixture.cancel_calls, 2u);
    check_size_eq(basic_fixture.calls, 0u);

    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
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

  it("resolves MQTT 5 Topic Alias before graph publication and rejects an invalid alias") {
    static const uint8_t alias_register[] = {0x30u, 0x0au, 0x00u, 0x03u, 'a',   '/',
                                             'b',   0x03u, 0x23u, 0x00u, 0x01u, 'x'};
    static const uint8_t alias_publish[] = {0x30u, 0x07u, 0x00u, 0x00u, 0x03u,
                                            0x23u, 0x00u, 0x01u, 'y'};
    static const uint8_t invalid_alias[] = {0x30u, 0x07u, 0x00u, 0x00u, 0x03u,
                                            0x23u, 0x00u, 0x11u, 'z'};
    static const uint8_t disconnect_alias_invalid[] = {0xe0u, 0x01u, 0x94u};
    flowie_endpoint_capture_t capture = {0};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    uint8_t connect[128];
    uint8_t received[sizeof(disconnect_alias_invalid)];
    size_t connect_size = 0u;
    size_t consumed = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t client;

    atomic_init(&capture.calls, 0u);
    flow = flowie_managed_session_flow(port, &capture);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_test_encode_connect(connect, sizeof(connect), &connect_size, "alias-client",
                                            60u, NULL, NULL, 0u),
                 TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client, alias_register, sizeof(alias_register)), TURBO_OK);
    check_int_eq(flowie_test_send(client, alias_publish, sizeof(alias_publish)), TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);

    options.version = FLOWIE_MQTT_VERSION_5;
    options.max_packet_size = sizeof(capture.packets[1]);
    check_int_eq(flowie_mqtt_packet_parse(capture.packets[1], capture.sizes[1], &options, &packet,
                                          &consumed, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, capture.sizes[1]);
    check_int_eq(flowie_mqtt_publish_parse(&packet, &publish), FLOWIE_MQTT_PARSE_OK);
    check_size_eq(publish.topic.size, 3u);
    check_mem_eq(publish.topic.data, "a/b", 3u);
    check_size_eq(publish.payload.size, 1u);
    check_uint_eq(publish.payload.data[0], 'y');

    check_int_eq(flowie_test_send(client, invalid_alias, sizeof(invalid_alias)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, disconnect_alias_invalid, sizeof(disconnect_alias_invalid));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("assigns and returns a stable MQTT 5 Client Identifier for an empty clean-start ID") {
    static const uint8_t session_expiry[] = {FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL, 0x00u,
                                             0x00u, 0x00u, 0x3cu};
    flowie_endpoint_capture_t capture = {0};
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t encoded[128];
    char assigned_client_id[64];
    size_t written = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t client;

    atomic_init(&capture.calls, 0u);
    flow = flowie_managed_session_flow(port, &capture);
    check_int_gt(port, 0);
    check_not_null(flow);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.properties = (flowie_mqtt_span_t){session_expiry, sizeof(session_expiry)};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, encoded, written), TURBO_OK);
    check_int_eq(flowie_test_recv_connack_ex(client, 0u, 0u, assigned_client_id,
                                             sizeof(assigned_client_id), NULL, 0u, NULL, 0u),
                 TURBO_OK);
    check_str_contains(assigned_client_id, "flowie-");
    check_size_eq(strlen(assigned_client_id), sizeof("flowie-") - 1u + TURBO_UUID_STRING_LENGTH);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("enforces the MQTT Keep Alive 1.5x receive deadline on an idle connection") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    flowie_endpoint_capture_t capture = {0};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t encoded[128];
    uint8_t byte = 0u;
    size_t written = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client;

    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    check_not_null(flow);
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_connections = 1u;
    config.manage_sessions = 1;
    config.max_sessions = 1u;
    config.max_subscriptions_per_session = 1u;
    config.max_inflight_per_session = 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.endpoint", &config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 1u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"idle-client", 11u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, encoded, written), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
    check_true(flowie_test_socket_readable(client, 3000u));
    check_int_eq(flowie_test_recv_exact(client, &byte, 1u), TURBO_EIO);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(first, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(resumed, 1u, 0u), TURBO_OK);

    duplicate = flowie_test_connect(port);
    check_true(duplicate != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(duplicate, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(duplicate, 0u, UINT8_C(0x89)), TURBO_OK);
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
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'b',   'a',   't'};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t auth_disconnect[] = {0xe0u, 0x01u, 0x8cu};
    uint8_t pipeline[PIPELINED_PING_COUNT * 2u + 2u];
    uint8_t expected[PIPELINED_PING_COUNT * 2u + sizeof(auth_disconnect)];
    uint8_t received[sizeof(expected)];
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
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);

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
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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
    static const uint8_t normal_subscribe[] = {0x82u, 0x11u, 0x00u, 0x01u, 0x02u, 0x0bu, 0x2au,
                                               0x00u, 0x09u, 's',   'e',   'n',   's',   'o',
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
    static const uint8_t normal_delivery_first[] = {0x32u, 0x11u, 0x00u, 0x09u, 's', 'e', 'n',
                                                    's',   'o',   'r',   's',   '/', 'a', 0x00u,
                                                    0x01u, 0x02u, 0x0bu, 0x2au, 'x'};
    static const uint8_t normal_delivery_second[] = {0x32u, 0x11u, 0x00u, 0x09u, 's', 'e', 'n',
                                                     's',   'o',   'r',   's',   '/', 'a', 0x00u,
                                                     0x02u, 0x02u, 0x0bu, 0x2au, 'y'};
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
        check_int_eq(flowie_test_recv_connack(clients[i], 0u, 0u), TURBO_OK);
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

  it("bridges PUBLISH traffic across MQTT 3.1, MQTT 3.1.1, and MQTT 5 sessions") {
    static const uint8_t connect_v31[] = {0x10u, 0x11u, 0x00u, 0x06u, 'M',   'Q',   'I',
                                          's',   'd',   'p',   0x03u, 0x02u, 0x00u, 0x3cu,
                                          0x00u, 0x03u, 'v',   '3',   'l'};
    static const uint8_t connect_v311[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u,
                                           0x02u, 0x00u, 0x3cu, 0x00u, 0x03u, 'v', '3', 's'};
    static const uint8_t connect_v5[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',   'T', 'T', 0x05u,
                                         0x02u, 0x00u, 0x3cu, 0x00u, 0x00u, 0x03u, 'v', '5', 'p'};
    static const uint8_t connack_v311[] = {0x20u, 0x02u, 0x00u, 0x00u};
    static const uint8_t subscribe_v31[] = {0x82u, 0x0cu, 0x00u, 0x03u, 0x00u, 0x07u, 'c',
                                            'r',   'o',   's',   's',   '/',   '+',   0x01u};
    static const uint8_t suback_v31[] = {0x90u, 0x03u, 0x00u, 0x03u, 0x01u};
    static const uint8_t subscribe_v311[] = {0x82u, 0x0cu, 0x00u, 0x01u, 0x00u, 0x07u, 'c',
                                             'r',   'o',   's',   's',   '/',   '+',   0x01u};
    static const uint8_t suback_v311[] = {0x90u, 0x03u, 0x00u, 0x01u, 0x01u};
    static const uint8_t subscribe_v5[] = {0x82u, 0x0fu, 0x00u, 0x02u, 0x00u, 0x00u,
                                           0x09u, 'r',   'e',   'v',   'e',   'r',
                                           's',   'e',   '/',   '+',   0x01u};
    static const uint8_t suback_v5[] = {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x01u};
    static const uint8_t publish_v5[] = {0x32u, 0x14u, 0x00u, 0x07u, 'c',   'r',   'o',   's',
                                         's',   '/',   'a',   0x00u, 0x2au, 0x07u, 0x26u, 0x00u,
                                         0x01u, 'k',   0x00u, 0x01u, 'v',   'x'};
    static const uint8_t puback_v5[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t delivery_v311[] = {0x32u, 0x0cu, 0x00u, 0x07u, 'c',   'r',   'o',
                                            's',   's',   '/',   'a',   0x00u, 0x01u, 'x'};
    static const uint8_t delivery_v311_ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t delivery_v31[] = {0x32u, 0x0cu, 0x00u, 0x07u, 'c',   'r',   'o',
                                           's',   's',   '/',   'a',   0x00u, 0x01u, 'x'};
    static const uint8_t delivery_v31_ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t publish_v311[] = {0x32u, 0x0eu, 0x00u, 0x09u, 'r', 'e',   'v',   'e',
                                           'r',   's',   'e',   '/',   'a', 0x00u, 0x2bu, 'y'};
    static const uint8_t puback_v311[] = {0x40u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t delivery_v5[] = {0x32u, 0x0fu, 0x00u, 0x09u, 'r',   'e',   'v',   'e', 'r',
                                          's',   'e',   '/',   'a',   0x00u, 0x01u, 0x00u, 'y'};
    static const uint8_t delivery_v5_ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t publish_v31[] = {0x32u, 0x0eu, 0x00u, 0x09u, 'r', 'e',   'v',   'e',
                                          'r',   's',   'e',   '/',   'b', 0x00u, 0x2cu, 'z'};
    static const uint8_t puback_v31[] = {0x40u, 0x02u, 0x00u, 0x2cu};
    static const uint8_t second_delivery_v5[] = {0x32u, 0x0fu, 0x00u, 0x09u, 'r', 'e',
                                                 'v',   'e',   'r',   's',   'e', '/',
                                                 'b',   0x00u, 0x02u, 0x00u, 'z'};
    static const uint8_t second_delivery_v5_ack[] = {0x40u, 0x02u, 0x00u, 0x02u};
    uint8_t received[32];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t client_v31;
    flowie_test_socket_t client_v311;
    flowie_test_socket_t client_v5;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client_v31 = flowie_test_connect(port);
    client_v311 = flowie_test_connect(port);
    client_v5 = flowie_test_connect(port);
    check_true(client_v31 != FLOWIE_TEST_INVALID_SOCKET);
    check_true(client_v311 != FLOWIE_TEST_INVALID_SOCKET);
    check_true(client_v5 != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client_v31, connect_v31, sizeof(connect_v31)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(connack_v311)), TURBO_OK);
    check_mem_eq(received, connack_v311, sizeof(connack_v311));
    check_int_eq(flowie_test_send(client_v311, connect_v311, sizeof(connect_v311)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(connack_v311)), TURBO_OK);
    check_mem_eq(received, connack_v311, sizeof(connack_v311));
    check_int_eq(flowie_test_send(client_v5, connect_v5, sizeof(connect_v5)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client_v5, 0u, 0u), TURBO_OK);

    check_int_eq(flowie_test_send(client_v31, subscribe_v31, sizeof(subscribe_v31)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(suback_v31)), TURBO_OK);
    check_mem_eq(received, suback_v31, sizeof(suback_v31));
    check_int_eq(flowie_test_send(client_v311, subscribe_v311, sizeof(subscribe_v311)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(suback_v311)), TURBO_OK);
    check_mem_eq(received, suback_v311, sizeof(suback_v311));
    check_int_eq(flowie_test_send(client_v5, publish_v5, sizeof(publish_v5)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v5, received, sizeof(puback_v5)), TURBO_OK);
    check_mem_eq(received, puback_v5, sizeof(puback_v5));
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(delivery_v311)), TURBO_OK);
    check_mem_eq(received, delivery_v311, sizeof(delivery_v311));
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(delivery_v31)), TURBO_OK);
    check_mem_eq(received, delivery_v31, sizeof(delivery_v31));
    check_int_eq(flowie_test_send(client_v31, delivery_v31_ack, sizeof(delivery_v31_ack)),
                 TURBO_OK);
    check_int_eq(flowie_test_send(client_v311, delivery_v311_ack, sizeof(delivery_v311_ack)),
                 TURBO_OK);

    check_int_eq(flowie_test_send(client_v5, subscribe_v5, sizeof(subscribe_v5)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v5, received, sizeof(suback_v5)), TURBO_OK);
    check_mem_eq(received, suback_v5, sizeof(suback_v5));
    check_int_eq(flowie_test_send(client_v311, publish_v311, sizeof(publish_v311)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(puback_v311)), TURBO_OK);
    check_mem_eq(received, puback_v311, sizeof(puback_v311));
    check_int_eq(flowie_test_recv_exact(client_v5, received, sizeof(delivery_v5)), TURBO_OK);
    check_mem_eq(received, delivery_v5, sizeof(delivery_v5));
    check_int_eq(flowie_test_send(client_v5, delivery_v5_ack, sizeof(delivery_v5_ack)), TURBO_OK);

    check_int_eq(flowie_test_send(client_v31, publish_v31, sizeof(publish_v31)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(puback_v31)), TURBO_OK);
    check_mem_eq(received, puback_v31, sizeof(puback_v31));
    check_int_eq(flowie_test_recv_exact(client_v5, received, sizeof(second_delivery_v5)), TURBO_OK);
    check_mem_eq(received, second_delivery_v5, sizeof(second_delivery_v5));
    check_int_eq(
        flowie_test_send(client_v5, second_delivery_v5_ack, sizeof(second_delivery_v5_ack)),
        TURBO_OK);

    flowie_test_socket_close(client_v5);
    flowie_test_socket_close(client_v311);
    flowie_test_socket_close(client_v31);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("holds QoS delivery at the client Receive Maximum until PUBACK advances the window") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'p',   '0',   '1'};
    static const uint8_t subscriber_connect[] = {0x10u, 0x13u, 0x00u, 0x04u, 'M',   'Q',   'T',
                                                 'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x03u, 0x21u,
                                                 0x00u, 0x01u, 0x00u, 0x03u, 's',   '0',   '1'};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x03u, 'q',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t publish_first[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                            'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publish_second[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t publisher_ack_first[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_ack_second[] = {0x40u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t delivery_first[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t delivery_second[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                              'a',   0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t subscriber_ack_first[] = {0x40u, 0x02u, 0x00u, 0x01u};
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
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    check_int_eq(flowie_test_send(publisher, publish_first, sizeof(publish_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_ack_first)),
                 TURBO_OK);
    check_mem_eq(received, publisher_ack_first, sizeof(publisher_ack_first));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(delivery_first)), TURBO_OK);
    check_mem_eq(received, delivery_first, sizeof(delivery_first));
    check_int_eq(flowie_test_send(publisher, publish_second, sizeof(publish_second)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_ack_second)),
                 TURBO_OK);
    check_mem_eq(received, publisher_ack_second, sizeof(publisher_ack_second));
    check_false(flowie_test_socket_readable(subscriber, 100u));
    check_int_eq(flowie_test_send(subscriber, subscriber_ack_first, sizeof(subscriber_ack_first)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(delivery_second)), TURBO_OK);
    check_mem_eq(received, delivery_second, sizeof(delivery_second));

    flowie_test_socket_close(subscriber);
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
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);

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
    check_int_eq(flowie_test_recv_connack(second_subscriber, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(subscriber, 1u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    abnormal = flowie_test_connect(port);
    check_true(abnormal != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(abnormal, abnormal_connect, abnormal_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(abnormal, 0u, 0u), TURBO_OK);
    flowie_test_socket_close(abnormal);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, expected_abnormal_size), TURBO_OK);
    check_mem_eq(received, expected_abnormal, expected_abnormal_size);
    check_size_eq(capture.sizes[0], expected_abnormal_size);
    check_mem_eq(capture.packets[0], expected_abnormal, expected_abnormal_size);

    normal = flowie_test_connect(port);
    check_true(normal != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(normal, normal_connect, normal_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(normal, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(normal, normal_disconnect, sizeof(normal_disconnect)), TURBO_OK);
    turbo_sleep_ms(100u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    flowie_test_socket_close(normal);

    requested = flowie_test_connect(port);
    check_true(requested != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(requested, requested_connect, requested_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(requested, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);

    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, delayed_connect, delayed_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(publisher, 1u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    flowie_test_socket_close(publisher);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, expected_delayed_size), TURBO_OK);
    check_mem_eq(received, expected_delayed, expected_delayed_size);

    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, expiry_connect, expiry_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
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
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t client;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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
    check_int_eq(flowie_test_recv_connack(client, 1u, 0u), TURBO_OK);
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
      check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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
      check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
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
