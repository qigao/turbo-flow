#include "flowie.h"
#include "flowie_task_group_internal.h"
#include "flowie_test_socket.h"
#include "tls_test_support.h"

#include "CoroNet/turbo_coro_socket.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_coronet_execution.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_uuid.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define FLOWIE_TEST_WAIT_STEPS 2000u
#define FLOWIE_TEST_EPOCH_WAIT_STEPS 4000u
#define FLOWIE_TEST_STOP_MAX_MS 2000u

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
  uint64_t policy_version;
  int result;
  int revoked;
  char remote_address[CORO_SOCKET_ADDRESS_TEXT_CAPACITY];
  char transport_peer_address[CORO_SOCKET_ADDRESS_TEXT_CAPACITY];
  char peer_certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
} flowie_security_fixture_t;

typedef struct flowie_policy_fixture_s {
  turbo_flow_security_rule_t rules[2];
  uint64_t policy_version;
  size_t rule_count;
} flowie_policy_fixture_t;

typedef struct flowie_enhanced_security_fixture_s {
  size_t begin_calls;
  size_t continue_calls;
  size_t cancel_calls;
  size_t rounds_per_exchange;
  uint64_t first_expires_at;
  uint64_t next_expires_at;
} flowie_enhanced_security_fixture_t;

typedef struct flowie_task_group_wait_fixture_s {
  flowie_task_group_t *group;
  atomic_int entered;
  atomic_int exited;
} flowie_task_group_wait_fixture_t;

typedef struct flowie_cluster_endpoint_fixture_s {
  coro_context_t *context;
  flowie_endpoint_cluster_complete_fn pending_complete;
  void *pending_complete_ctx;
  flowie_endpoint_cluster_socket_port_t socket_port;
  flowie_endpoint_cluster_command_t pending_command;
  flowie_mqtt_version_t mqtt_version;
  uint64_t connection_id;
  uint64_t connection_generation;
  atomic_size_t connect_calls;
  atomic_size_t command_calls;
  atomic_size_t settlement_calls;
  atomic_size_t lost_calls;
  atomic_size_t detach_calls;
  uint8_t command_packet[32];
  size_t command_packet_size;
  uint8_t proxy_tlvs[32];
  size_t proxy_tlvs_size;
  char client_id[64];
  char remote_address[64];
  char transport_peer_address[64];
  turbo_flow_protocol_settlement_request_t settlement;
  int publish_admit_graph;
} flowie_cluster_endpoint_fixture_t;

static void flowie_cluster_endpoint_complete_post(void *arg1, void *arg2) {
  static const uint8_t connack_v5[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
  static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
  static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x2au, 0x00u, 0x01u};
  flowie_cluster_endpoint_fixture_t *fixture = (flowie_cluster_endpoint_fixture_t *)arg1;
  flowie_endpoint_cluster_action_t action = FLOWIE_ENDPOINT_CLUSTER_ACTION_INIT;
  (void)arg2;
  if (!fixture || !fixture->pending_complete) return;
  action.mqtt_version = fixture->mqtt_version;
  if (fixture->pending_command == FLOWIE_ENDPOINT_CLUSTER_COMMAND_NONE)
    action.packet = (flowie_mqtt_span_t){connack_v5, sizeof(connack_v5)};
  else if (fixture->pending_command == FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH) {
    if (fixture->publish_admit_graph) action.settlement_point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    else action.packet = (flowie_mqtt_span_t){puback, sizeof(puback)};
  } else if (fixture->pending_command == FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH_SETTLE)
    action.packet = (flowie_mqtt_span_t){puback, sizeof(puback)};
  else if (fixture->pending_command == FLOWIE_ENDPOINT_CLUSTER_COMMAND_SUBSCRIBE)
    action.packet = (flowie_mqtt_span_t){suback, sizeof(suback)};
  else if (fixture->pending_command == FLOWIE_ENDPOINT_CLUSTER_COMMAND_DISCONNECT)
    action.close_after_send = 1u;
  fixture->pending_complete(fixture->pending_complete_ctx, TURBO_OK, &action);
  fixture->pending_complete = NULL;
  fixture->pending_complete_ctx = NULL;
}

static int flowie_cluster_endpoint_connect(void *ctx, uint64_t connection_id,
                                           uint64_t connection_generation,
                                           const flowie_mqtt_connect_view_t *connect,
                                           const turbo_flow_security_principal_t *principal,
                                           const flowie_endpoint_cluster_ingress_t *ingress,
                                           const flowie_endpoint_cluster_socket_port_t *socket_port,
                                           flowie_endpoint_cluster_complete_fn complete,
                                           void *complete_ctx) {
  flowie_cluster_endpoint_fixture_t *fixture = (flowie_cluster_endpoint_fixture_t *)ctx;
  (void)principal;
  if (!fixture || !connect || !ingress || ingress->size < sizeof(*ingress) ||
      !ingress->remote_address || !ingress->remote_address[0] || !ingress->transport_peer_address ||
      !ingress->transport_peer_address[0] || !socket_port || !complete ||
      socket_port->size < sizeof(*socket_port) || !socket_port->takeover_close ||
      !socket_port->apply_action || connect->client_id.size >= sizeof(fixture->client_id) ||
      fixture->pending_complete)
    return TURBO_EINVAL;
  fixture->context = coro_context_current();
  fixture->connection_id = connection_id;
  fixture->connection_generation = connection_generation;
  fixture->socket_port = *socket_port;
  fixture->mqtt_version = connect->version;
  if (ingress->proxy_tlvs.size > sizeof(fixture->proxy_tlvs)) return TURBO_EMSGSIZE;
  (void)snprintf(fixture->remote_address, sizeof(fixture->remote_address), "%s",
                 ingress->remote_address);
  (void)snprintf(fixture->transport_peer_address, sizeof(fixture->transport_peer_address), "%s",
                 ingress->transport_peer_address);
  if (ingress->proxy_tlvs.size != 0u)
    memcpy(fixture->proxy_tlvs, ingress->proxy_tlvs.data, ingress->proxy_tlvs.size);
  fixture->proxy_tlvs_size = ingress->proxy_tlvs.size;
  memcpy(fixture->client_id, connect->client_id.data, connect->client_id.size);
  fixture->client_id[connect->client_id.size] = '\0';
  fixture->pending_command = FLOWIE_ENDPOINT_CLUSTER_COMMAND_NONE;
  fixture->pending_complete = complete;
  fixture->pending_complete_ctx = complete_ctx;
  atomic_fetch_add_explicit(&fixture->connect_calls, 1u, memory_order_release);
  return coro_post(fixture->context, flowie_cluster_endpoint_complete_post, fixture, NULL);
}

static int flowie_cluster_endpoint_command(void *ctx, uint64_t connection_id,
                                           uint64_t connection_generation,
                                           flowie_endpoint_cluster_command_t command,
                                           flowie_mqtt_version_t mqtt_version,
                                           flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
                                           flowie_endpoint_cluster_complete_fn complete,
                                           void *complete_ctx) {
  flowie_cluster_endpoint_fixture_t *fixture = (flowie_cluster_endpoint_fixture_t *)ctx;
  if (!fixture || !complete || fixture->pending_complete ||
      connection_id != fixture->connection_id ||
      connection_generation != fixture->connection_generation ||
      mqtt_version != fixture->mqtt_version || packet.size > sizeof(fixture->command_packet) ||
      client_id.size != strlen(fixture->client_id) ||
      memcmp(client_id.data, fixture->client_id, client_id.size) != 0)
    return TURBO_EINVAL;
  memcpy(fixture->command_packet, packet.data, packet.size);
  fixture->command_packet_size = packet.size;
  fixture->pending_command = command;
  fixture->pending_complete = complete;
  fixture->pending_complete_ctx = complete_ctx;
  atomic_fetch_add_explicit(&fixture->command_calls, 1u, memory_order_release);
  return coro_post(fixture->context, flowie_cluster_endpoint_complete_post, fixture, NULL);
}

static int
flowie_cluster_endpoint_settle(void *ctx, uint64_t connection_id, uint64_t connection_generation,
                               flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id,
                               const turbo_flow_protocol_settlement_request_t *settlement,
                               flowie_endpoint_cluster_complete_fn complete, void *complete_ctx) {
  flowie_cluster_endpoint_fixture_t *fixture = (flowie_cluster_endpoint_fixture_t *)ctx;
  if (!fixture || !settlement || settlement->size < sizeof(*settlement) || !complete ||
      fixture->pending_complete || connection_id != fixture->connection_id ||
      connection_generation != fixture->connection_generation ||
      mqtt_version != fixture->mqtt_version || client_id.size != strlen(fixture->client_id) ||
      memcmp(client_id.data, fixture->client_id, client_id.size) != 0)
    return TURBO_EINVAL;
  fixture->settlement = *settlement;
  fixture->pending_command = FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH_SETTLE;
  fixture->pending_complete = complete;
  fixture->pending_complete_ctx = complete_ctx;
  atomic_fetch_add_explicit(&fixture->settlement_calls, 1u, memory_order_release);
  return coro_post(fixture->context, flowie_cluster_endpoint_complete_post, fixture, NULL);
}

static int flowie_cluster_endpoint_connection_lost(void *ctx, uint64_t connection_id,
                                                   uint64_t connection_generation,
                                                   flowie_mqtt_version_t mqtt_version,
                                                   flowie_mqtt_span_t client_id) {
  flowie_cluster_endpoint_fixture_t *fixture = (flowie_cluster_endpoint_fixture_t *)ctx;
  if (!fixture || connection_id != fixture->connection_id ||
      connection_generation != fixture->connection_generation ||
      mqtt_version != fixture->mqtt_version || client_id.size != strlen(fixture->client_id))
    return TURBO_EINVAL;
  atomic_fetch_add_explicit(&fixture->lost_calls, 1u, memory_order_release);
  return TURBO_OK;
}

static void flowie_cluster_endpoint_detach(void *ctx, uint64_t connection_id,
                                           uint64_t connection_generation) {
  flowie_cluster_endpoint_fixture_t *fixture = (flowie_cluster_endpoint_fixture_t *)ctx;
  if (!fixture || connection_id != fixture->connection_id ||
      connection_generation != fixture->connection_generation)
    return;
  fixture->pending_complete = NULL;
  fixture->pending_complete_ctx = NULL;
  atomic_fetch_add_explicit(&fixture->detach_calls, 1u, memory_order_release);
}

typedef struct flowie_tls_auth_client_s {
  coro_context_t *context;
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  const uint8_t *connect_packet;
  size_t connect_packet_size;
  const uint8_t *proxy_header;
  size_t proxy_header_size;
  unsigned short port;
  int done;
  int status;
} flowie_tls_auth_client_t;

static void flowie_tls_auth_client_run(coro_t *coroutine, void *arg) {
  flowie_tls_auth_client_t *client = (flowie_tls_auth_client_t *)arg;
  turbo_tls_client_config_t tls_config = {0};
  coro_socket_t *socket = NULL;
  char *response = NULL;
  size_t response_size = 0u;
  int rc = TURBO_EINVAL;
  (void)coroutine;
  if (!client || !client->context || !client->ca_file || !client->connect_packet ||
      client->connect_packet_size == 0u)
    goto done;
  socket = coro_socket_create(client->context,
                              client->proxy_header ? CORO_SOCKET_TCP_V4 : CORO_SOCKET_TLS);
  if (!socket) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  tls_config.ca_file = client->ca_file;
  tls_config.cert_file = client->cert_file;
  tls_config.key_file = client->key_file;
  tls_config.verify_peer = 1;
  rc = coro_socket_set_tls_client_config(socket, &tls_config);
  if (rc == TURBO_OK) coro_socket_set_timeout(socket, 5000u);
  if (rc == TURBO_OK)
    rc =
        coro_socket_connect(socket, client->proxy_header ? "127.0.0.1" : "localhost", client->port);
  if (rc == TURBO_OK && client->proxy_header)
    rc = coro_socket_send(socket, (const char *)client->proxy_header, client->proxy_header_size);
  if (rc == TURBO_OK && client->proxy_header) rc = coro_socket_upgrade_tls(socket, "localhost");
  if (rc == TURBO_OK)
    rc =
        coro_socket_send(socket, (const char *)client->connect_packet, client->connect_packet_size);
  if (rc == TURBO_OK) rc = coro_socket_recv(socket, &response, &response_size);
  if (rc == TURBO_OK && (response_size < 4u || (uint8_t)response[0] != UINT8_C(0x20) ||
                         (uint8_t)response[3] != UINT8_C(0x00)))
    rc = TURBO_EPROTO;
done:
  coro_socket_free_recv(response);
  coro_socket_destroy(socket);
  client->status = rc;
  client->done = 1;
}

static int flowie_tls_auth_client_execute(flowie_tls_auth_client_t *client) {
  uint64_t deadline;
  int rc;
  if (!client) return TURBO_EINVAL;
  client->context = coro_context_create(NULL);
  if (!client->context) return TURBO_ENOMEM;
  client->done = 0;
  client->status = TURBO_EBUSY;
  rc = coro_context_spawn(client->context, flowie_tls_auth_client_run, client);
  deadline = turbo_monotonic_ms() + 5000u;
  while (rc == TURBO_OK && !client->done && turbo_monotonic_ms() < deadline)
    rc = coro_context_run(client->context, TURBO_RUN_ONCE);
  if (rc == TURBO_OK) rc = client->done ? client->status : TURBO_ETIMEDOUT;
  coro_context_destroy(client->context);
  client->context = NULL;
  return rc;
}

static void flowie_task_group_wait_thread(void *arg) {
  flowie_task_group_wait_fixture_t *fixture = (flowie_task_group_wait_fixture_t *)arg;
  atomic_store_explicit(&fixture->entered, 1, memory_order_release);
  flowie_task_group_wait(fixture->group);
  atomic_store_explicit(&fixture->exited, 1, memory_order_release);
}

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
  static const uint8_t middle_data[] = "server-middle";
  static const uint8_t final_data[] = "server-final";
  flowie_enhanced_security_fixture_t *fixture = (flowie_enhanced_security_fixture_t *)ctx;
  size_t rounds;
  size_t round;
  const char *expected;
  size_t expected_size;
  if (!fixture) return TURBO_EINVAL;
  rounds = fixture->rounds_per_exchange ? fixture->rounds_per_exchange : 1u;
  round = fixture->continue_calls % rounds;
  expected = round + 1u < rounds ? "client-middle" : "client-final";
  expected_size = strlen(expected);
  if (!fixture || exchange != fixture || !request || !result_out ||
      strcmp(request->method, "challenge") != 0 || request->data_size != expected_size ||
      memcmp(request->data, expected, expected_size) != 0)
    return TURBO_EPERM;
  ++fixture->continue_calls;
  if (round + 1u < rounds) {
    result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE;
    result_out->data = middle_data;
    result_out->data_size = sizeof(middle_data) - 1u;
    return TURBO_OK;
  }
  result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS;
  result_out->data = final_data;
  result_out->data_size = sizeof(final_data) - 1u;
  flowie_test_security_principal(&result_out->principal, "challenge");
  result_out->principal.expires_at =
      fixture->begin_calls == 1u ? fixture->first_expires_at : fixture->next_expires_at;
  return TURBO_OK;
}

static void flowie_test_enhanced_cancel(void *ctx, void *exchange) {
  flowie_enhanced_security_fixture_t *fixture = (flowie_enhanced_security_fixture_t *)ctx;
  if (fixture && exchange == fixture) ++fixture->cancel_calls;
}

static int flowie_test_policy_load(void *ctx, uint64_t required_version,
                                   turbo_flow_security_policy_bundle_t *bundle) {
  flowie_policy_fixture_t *fixture = (flowie_policy_fixture_t *)ctx;
  uint64_t policy_version;
  size_t rule_count;
  if (!fixture) return TURBO_EINVAL;
  policy_version = fixture->policy_version ? fixture->policy_version : 1u;
  rule_count = fixture->rule_count ? fixture->rule_count : 2u;
  if (!fixture || !bundle || bundle->size < sizeof(*bundle) ||
      (required_version != 0u && required_version != policy_version) || rule_count > 2u)
    return TURBO_EINVAL;
  bundle->policy_version = policy_version;
  bundle->rules = fixture->rules;
  bundle->rule_count = rule_count;
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
  (void)snprintf(fixture->remote_address, sizeof(fixture->remote_address), "%s",
                 request->remote_address ? request->remote_address : "");
  (void)snprintf(fixture->transport_peer_address, sizeof(fixture->transport_peer_address), "%s",
                 request->size >= sizeof(*request) && request->transport_peer_address
                     ? request->transport_peer_address
                     : "");
  (void)snprintf(fixture->peer_certificate_sha256, sizeof(fixture->peer_certificate_sha256), "%s",
                 request->size >= sizeof(*request) && request->peer_certificate_sha256
                     ? request->peer_certificate_sha256
                     : "");
  if (fixture->result != TURBO_OK) return fixture->result;
  if (fixture->revoked || strcmp(request->identity, "writer") != 0 ||
      strcmp(request->method, "password") != 0 || request->secret_size != sizeof("secret") - 1u ||
      memcmp(request->secret, "secret", sizeof("secret") - 1u) != 0)
    return TURBO_EPERM;
  flowie_test_security_principal(principal, request->method);
  principal->expires_at = fixture->expires_at;
  principal->policy_version = fixture->policy_version ? fixture->policy_version : 1u;
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

static int flowie_endpoint_core_capture(flowie_endpoint_core_t *endpoint, turbo_flow_msg_t *message,
                                        turbo_flow_publish_result_t *result, void *ctx) {
  int rc;
  (void)endpoint;
  if (!result) return TURBO_EINVAL;
  rc = flowie_endpoint_capture_stage(message, ctx);
  result->status = rc;
  return rc;
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

static int flowie_test_recv_packet(flowie_test_socket_t socket, uint8_t *wire, size_t capacity,
                                   size_t *wire_size) {
  uint32_t remaining = 0u;
  uint32_t multiplier = 1u;
  size_t fixed_size = 1u;
  int rc;
  if (!wire || capacity < 2u || !wire_size) return TURBO_EINVAL;
  *wire_size = 0u;
  rc = flowie_test_recv_exact(socket, wire, 1u);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    uint8_t byte;
    if (fixed_size >= 5u) return TURBO_EPROTO;
    rc = flowie_test_recv_exact(socket, &byte, 1u);
    if (rc != TURBO_OK) return rc;
    wire[fixed_size++] = byte;
    remaining += (uint32_t)(byte & UINT8_C(0x7f)) * multiplier;
    if ((byte & UINT8_C(0x80)) == 0u) break;
    multiplier *= 128u;
  }
  if ((size_t)remaining > capacity - fixed_size) return TURBO_EMSGSIZE;
  rc = flowie_test_recv_exact(socket, wire + fixed_size, remaining);
  if (rc != TURBO_OK) return rc;
  *wire_size = fixed_size + (size_t)remaining;
  return TURBO_OK;
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

static turbo_flow_t *flowie_connection_hwm_flow(unsigned short port,
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
  config.max_packet_size = 4096u;
  config.max_connections = 1u;
  config.recv_timeout_ms = 5000u;
  config.manage_sessions = 1;
  config.max_sessions = 1u;
  config.max_subscriptions_per_session = 1u;
  config.max_inflight_per_session = 1u;
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

static turbo_flow_t *flowie_quota_flow(unsigned short port) {
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
  config.max_packet_size = 4096u;
  config.max_connections = 4u;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = 4096u;
  config.manage_sessions = 1;
  config.max_sessions = 1u;
  config.max_subscriptions_per_session = 1u;
  config.max_inflight_per_session = 2u;
  config.max_retained_messages = 1u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_durable_replay_flow(unsigned short port) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "source durable_replay\n"
                              "stage mqtt_fanout adapter flowie.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> mqtt_fanout\n"
                              "  durable_replay -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_packet_size = 4096u;
  config.max_connections = 4u;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = 4096u;
  config.manage_sessions = 1;
  config.max_sessions = 4u;
  config.max_subscriptions_per_session = 4u;
  config.max_inflight_per_session = 4u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
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

static int flowie_test_processed_settlement_rejection(int stage_status, const char *client_id) {
  static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u, 'x'};
  flowie_endpoint_capture_t capture;
  uint8_t connect[128];
  uint8_t received[4];
  size_t connect_size = 0u;
  unsigned short port = flowie_test_port();
  turbo_flow_t *flow = NULL;
  flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
  int started = 0;
  int rc;
  if (port == 0u || !client_id || stage_status == TURBO_OK) return TURBO_EINVAL;
  memset(&capture, 0, sizeof(capture));
  atomic_init(&capture.calls, 0u);
  capture.result = stage_status;
  rc = flowie_test_encode_connect(connect, sizeof(connect), &connect_size, client_id, 60u, NULL,
                                  NULL, 0u);
  if (rc != TURBO_OK) return rc;
  flow = flowie_settlement_failure_flow(port, &capture, TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED);
  if (!flow) return TURBO_ENOMEM;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  started = 1;
  client = flowie_test_connect(port);
  if (client == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_EIO;
    goto done;
  }
  rc = flowie_test_send(client, connect, connect_size);
  if (rc != TURBO_OK) goto done;
  rc = flowie_test_recv_connack(client, 0u, 0u);
  if (rc != TURBO_OK) goto done;
  rc = flowie_test_send(client, publish, sizeof(publish));
  if (rc != TURBO_OK) goto done;
  if (flowie_test_recv_exact(client, received, sizeof(received)) == TURBO_OK) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_wait_calls(&capture, 1u);

done:
  flowie_test_socket_close(client);
  if (started) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

spec("Flowie MQTT endpoint primitive") {
  it("delegates coalesced MQTT packets to an asynchronous cluster owner in order") {
    static const uint8_t publish[] = {0x32u, 0x06u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u};
    static const uint8_t expected[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u,
                                       0x00u, 0x04u, 0x27u, 0x00u, 0x00u, 0x04u,
                                       0x00u, 0x40u, 0x02u, 0x00u, 0x2au};
    const turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t fixture;
    flowie_endpoint_capture_t capture;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t connect_packet[128];
    uint8_t input[160];
    uint8_t received[sizeof(expected)];
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&fixture, 0, sizeof(fixture));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&fixture.connect_calls, 0u);
    atomic_init(&fixture.command_calls, 0u);
    atomic_init(&fixture.lost_calls, 0u);
    atomic_init(&fixture.detach_calls, 0u);
    atomic_init(&capture.calls, 0u);
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.recv_timeout_ms = 5000u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;
    cluster.ctx = &fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.cluster = &cluster;

    check_int_gt(port, 0);
    check_int_eq(flowie_test_encode_connect(connect_packet, sizeof(connect_packet), &connect_size,
                                            "cluster-a", 60u, NULL, NULL, 0u),
                 TURBO_OK);
    memcpy(input, connect_packet, connect_size);
    memcpy(input + connect_size, publish, sizeof(publish));
    cluster.abi_version = FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_V1;
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster", &config, &options, &execution,
                                                &bindings, &endpoint),
                 TURBO_EINVAL);
    check_null(endpoint);
    cluster.abi_version = FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT;
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster", &config, &options, &execution,
                                                &bindings, &endpoint),
                 TURBO_OK);
    check_not_null(endpoint);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, input, connect_size + sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, expected, sizeof(expected));
    check_uint_eq(atomic_load_explicit(&fixture.connect_calls, memory_order_acquire), 1u);
    check_uint_eq(atomic_load_explicit(&fixture.command_calls, memory_order_acquire), 1u);
    check_uint_eq(fixture.pending_command, FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH);
    check_uint_eq(fixture.command_packet_size, sizeof(publish));
    check_mem_eq(fixture.command_packet, publish, sizeof(publish));
    check_true(strncmp(fixture.remote_address, "127.0.0.1:", sizeof("127.0.0.1:") - 1u) == 0);
    check_true(strncmp(fixture.transport_peer_address, "127.0.0.1:", sizeof("127.0.0.1:") - 1u) ==
               0);
    check_size_eq(fixture.proxy_tlvs_size, 0u);
    check_uint_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS &&
                        atomic_load_explicit(&fixture.detach_calls, memory_order_acquire) == 0u;
         ++i)
      turbo_sleep_ms(1u);
    check_uint_eq(atomic_load_explicit(&fixture.lost_calls, memory_order_acquire), 1u);
    check_uint_eq(atomic_load_explicit(&fixture.detach_calls, memory_order_acquire), 1u);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
  }

  it("publishes owner-admitted MQTT data into the graph and settles it back asynchronously") {
    static const uint8_t publish[] = {0x32u, 0x06u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u};
    static const uint8_t expected[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u,
                                       0x00u, 0x04u, 0x27u, 0x00u, 0x00u, 0x04u,
                                       0x00u, 0x40u, 0x02u, 0x00u, 0x2au};
    const turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t fixture;
    flowie_endpoint_capture_t capture;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t connect_packet[128];
    uint8_t input[160];
    uint8_t received[sizeof(expected)];
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&fixture, 0, sizeof(fixture));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&fixture.connect_calls, 0u);
    atomic_init(&fixture.command_calls, 0u);
    atomic_init(&fixture.settlement_calls, 0u);
    atomic_init(&fixture.lost_calls, 0u);
    atomic_init(&fixture.detach_calls, 0u);
    atomic_init(&capture.calls, 0u);
    fixture.publish_admit_graph = 1;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.recv_timeout_ms = 5000u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    config.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;
    cluster.ctx = &fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.cluster = &cluster;

    check_int_gt(port, 0);
    check_int_eq(flowie_test_encode_connect(connect_packet, sizeof(connect_packet), &connect_size,
                                            "cluster-graph", 60u, NULL, NULL, 0u),
                 TURBO_OK);
    memcpy(input, connect_packet, connect_size);
    memcpy(input + connect_size, publish, sizeof(publish));
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster.graph", &config, &options,
                                                &execution, &bindings, &endpoint),
                 TURBO_OK);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, input, connect_size + sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, expected, sizeof(expected));
    check_uint_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);
    check_uint_eq(atomic_load_explicit(&fixture.command_calls, memory_order_acquire), 1u);
    check_uint_eq(atomic_load_explicit(&fixture.settlement_calls, memory_order_acquire), 1u);
    check_uint_eq(fixture.pending_command, FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH_SETTLE);
    check_int_eq(fixture.settlement.point, TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED);
    check_uint_eq(fixture.settlement.message.packet_id, 42u);
    check_uint_eq(fixture.settlement.message.session_generation, fixture.connection_generation);
    check_uint_eq(fixture.settlement.attempt, 1u);
    flowie_test_socket_close(client);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
  }

  it("normalizes connection-local Topic Aliases before cluster owner submission") {
    static const uint8_t register_alias[] = {0x32u, 0x09u, 0x00u, 0x01u, 'a',  0x00u,
                                             0x2au, 0x03u, 0x23u, 0x00u, 0x01u};
    static const uint8_t use_alias[] = {0x32u, 0x08u, 0x00u, 0x00u, 0x00u,
                                        0x2au, 0x03u, 0x23u, 0x00u, 0x01u};
    static const uint8_t expected_normalized[] = {0x32u, 0x09u, 0x00u, 0x01u, 'a',  0x00u,
                                                  0x2au, 0x03u, 0x23u, 0x00u, 0x01u};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
    const turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t fixture;
    flowie_endpoint_capture_t capture;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t connect_packet[128];
    uint8_t received[sizeof(puback)];
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&fixture, 0, sizeof(fixture));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&fixture.connect_calls, 0u);
    atomic_init(&fixture.command_calls, 0u);
    atomic_init(&fixture.lost_calls, 0u);
    atomic_init(&fixture.detach_calls, 0u);
    atomic_init(&capture.calls, 0u);
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.recv_timeout_ms = 5000u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    config.topic_alias_maximum = 4u;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;
    cluster.ctx = &fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.cluster = &cluster;

    check_int_gt(port, 0);
    check_int_eq(flowie_test_encode_connect(connect_packet, sizeof(connect_packet), &connect_size,
                                            "cluster-alias", 60u, NULL, NULL, 0u),
                 TURBO_OK);
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster.alias", &config, &options,
                                                &execution, &bindings, &endpoint),
                 TURBO_OK);
    check_not_null(endpoint);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client, register_alias, sizeof(register_alias)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_int_eq(flowie_test_send(client, use_alias, sizeof(use_alias)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_uint_eq(atomic_load_explicit(&fixture.command_calls, memory_order_acquire), 2u);
    check_size_eq(fixture.command_packet_size, sizeof(expected_normalized));
    check_mem_eq(fixture.command_packet, expected_normalized, sizeof(expected_normalized));
    check_uint_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    flowie_test_socket_close(client);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
  }

  it("rejects an unauthorized cluster PUBLISH before owner submission") {
    static const uint8_t denied_publish[] = {0x32u, 0x0bu, 0x00u, 0x06u, 'd',   'e',  'n',
                                             'i',   'e',   'd',   0x00u, 0x2au, 0x00u};
    static const uint8_t denied_puback[] = {0x40u, 0x04u, 0x00u, 0x2au, 0x87u, 0x00u};
    const turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t fixture;
    flowie_endpoint_capture_t capture;
    flowie_security_fixture_t auth = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packet[128];
    uint8_t received[sizeof(denied_puback)];
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&fixture, 0, sizeof(fixture));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&fixture.connect_calls, 0u);
    atomic_init(&fixture.command_calls, 0u);
    atomic_init(&fixture.lost_calls, 0u);
    atomic_init(&fixture.detach_calls, 0u);
    atomic_init(&capture.calls, 0u);
    rule.effect = TURBO_FLOW_SECURITY_ALLOW;
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    (void)snprintf(rule.subject, sizeof(rule.subject), "%s", "writer");
    (void)snprintf(rule.root_group_id, sizeof(rule.root_group_id), "%s", "root-a");
    rule.action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    (void)snprintf(rule.pattern, sizeof(rule.pattern), "%s", "cluster-secure");
    realm_config.resource_uid = "security:cluster-publish";
    realm_config.owner_name = "security.cluster-publish";
    realm_config.policy_version = 1u;
    realm_config.rules = &rule;
    realm_config.rule_count = 1u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    security.realm_channel = "security.cluster-publish";
    security.auth_method = "password";
    security.auth_provider = &provider;
    security.realm = realm;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.recv_timeout_ms = 5000u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;
    cluster.ctx = &fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.security = &security;
    bindings.cluster = &cluster;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"cluster-secure", 14u};
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};

    check_int_gt(port, 0);
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster.secure", &config, &options,
                                                &execution, &bindings, &endpoint),
                 TURBO_OK);
    check_not_null(endpoint);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client, denied_publish, sizeof(denied_publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, denied_puback, sizeof(denied_puback));
    check_uint_eq(atomic_load_explicit(&fixture.command_calls, memory_order_acquire), 0u);
    check_uint_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    flowie_test_socket_close(client);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
    turbo_flow_security_realm_destroy(realm);
  }

  it("merges denied cluster subscriptions without exposing them to the owner") {
    static const uint8_t subscribe[] = {
        0x82u, 0x1au, 0x00u, 0x2au, 0x00u, 0x00u, 0x09u, 'a', 'l', 'l', 'o', 'w', 'e', 'd',
        '/',   '#',   0x01u, 0x00u, 0x08u, 'd',   'e',   'n', 'i', 'e', 'd', '/', '#', 0x02u};
    static const uint8_t filtered_subscribe[] = {0x82u, 0x0fu, 0x00u, 0x2au, 0x00u, 0x00u,
                                                 0x09u, 'a',   'l',   'l',   'o',   'w',
                                                 'e',   'd',   '/',   '#',   0x01u};
    static const uint8_t merged_suback[] = {0x90u, 0x05u, 0x00u, 0x2au, 0x00u, 0x01u, 0x87u};
    const turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t fixture;
    flowie_endpoint_capture_t capture;
    flowie_security_fixture_t auth = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_rule_t rules[2];
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packet[128];
    uint8_t received[sizeof(merged_suback)];
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&fixture, 0, sizeof(fixture));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&fixture.connect_calls, 0u);
    atomic_init(&fixture.command_calls, 0u);
    atomic_init(&fixture.lost_calls, 0u);
    atomic_init(&fixture.detach_calls, 0u);
    atomic_init(&capture.calls, 0u);
    for (size_t index = 0u; index < 2u; ++index) {
      rules[index] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
      rules[index].effect = TURBO_FLOW_SECURITY_ALLOW;
      rules[index].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
      (void)snprintf(rules[index].subject, sizeof(rules[index].subject), "%s", "writer");
      (void)snprintf(rules[index].root_group_id, sizeof(rules[index].root_group_id), "%s",
                     "root-a");
      rules[index].match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    }
    rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    rules[0].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    (void)snprintf(rules[0].pattern, sizeof(rules[0].pattern), "%s", "cluster-subscribe");
    rules[1].action_mask = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    rules[1].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    (void)snprintf(rules[1].pattern, sizeof(rules[1].pattern), "%s", "allowed/");
    realm_config.resource_uid = "security:cluster-subscribe";
    realm_config.owner_name = "security.cluster-subscribe";
    realm_config.policy_version = 1u;
    realm_config.rules = rules;
    realm_config.rule_count = 2u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    security.realm_channel = "security.cluster-subscribe";
    security.auth_method = "password";
    security.auth_provider = &provider;
    security.realm = realm;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.recv_timeout_ms = 5000u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;
    cluster.ctx = &fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.security = &security;
    bindings.cluster = &cluster;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"cluster-subscribe", 17u};
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};

    check_int_gt(port, 0);
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster.subscribe", &config, &options,
                                                &execution, &bindings, &endpoint),
                 TURBO_OK);
    check_not_null(endpoint);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, merged_suback, sizeof(merged_suback));
    check_uint_eq(atomic_load_explicit(&fixture.command_calls, memory_order_acquire), 1u);
    check_uint_eq(fixture.pending_command, FLOWIE_ENDPOINT_CLUSTER_COMMAND_SUBSCRIBE);
    check_size_eq(fixture.command_packet_size, sizeof(filtered_subscribe));
    check_mem_eq(fixture.command_packet, filtered_subscribe, sizeof(filtered_subscribe));
    check_uint_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    flowie_test_socket_close(client);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
    turbo_flow_security_realm_destroy(realm);
  }

  it("adds endpoint MQTT 5 limits and an assigned Client ID to cluster CONNACK") {
    const turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t fixture;
    flowie_endpoint_capture_t capture;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packet[128];
    char assigned_client_id[64] = {0};
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&fixture, 0, sizeof(fixture));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&fixture.connect_calls, 0u);
    atomic_init(&fixture.command_calls, 0u);
    atomic_init(&fixture.lost_calls, 0u);
    atomic_init(&fixture.detach_calls, 0u);
    atomic_init(&capture.calls, 0u);
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.recv_timeout_ms = 5000u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;
    cluster.ctx = &fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.cluster = &cluster;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;

    check_int_gt(port, 0);
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_endpoint_core_create_ex("flowie.cluster.assigned", &config, &options,
                                                &execution, &bindings, &endpoint),
                 TURBO_OK);
    check_not_null(endpoint);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack_ex(client, 0u, 0u, assigned_client_id,
                                             sizeof(assigned_client_id), NULL, 0u, NULL, 0u),
                 TURBO_OK);
    check_str_contains(assigned_client_id, "flowie-");
    check_str_eq(assigned_client_id, fixture.client_id);
    check_uint_eq(atomic_load_explicit(&fixture.connect_calls, memory_order_acquire), 1u);
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS &&
                        atomic_load_explicit(&fixture.detach_calls, memory_order_acquire) == 0u;
         ++i)
      turbo_sleep_ms(1u);
    check_uint_eq(atomic_load_explicit(&fixture.lost_calls, memory_order_acquire), 1u);
    check_uint_eq(atomic_load_explicit(&fixture.detach_calls, memory_order_acquire), 1u);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
  }

  it("starts and stops a direct endpoint Core without creating a graph") {
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_core_options_t options = FLOWIE_ENDPOINT_CORE_OPTIONS_INIT;
    flowie_endpoint_capture_t capture;
    flowie_endpoint_core_t *endpoint = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t connect_packet[128];
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();

    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 4u;
    config.recv_timeout_ms = 5000u;
    options.on_message = flowie_endpoint_core_capture;
    options.message_ctx = &capture;

    check_int_gt(port, 0);
    check_int_eq(flowie_test_encode_connect(connect_packet, sizeof(connect_packet), &connect_size,
                                            "d", 60u, NULL, NULL, 0u),
                 TURBO_OK);
    check_int_eq(flowie_endpoint_core_create("flowie.direct", &config, &options, &endpoint),
                 TURBO_OK);
    check_not_null(endpoint);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_EALREADY);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_uint_eq(capture.types[0], FLOWIE_MQTT_PACKET_CONNECT);
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    check_int_eq(flowie_endpoint_core_start(endpoint), TURBO_OK);
    check_int_eq(flowie_endpoint_core_stop(endpoint), TURBO_OK);
    flowie_endpoint_core_destroy(endpoint);
  }

  it("closes task admission before draining already admitted work") {
    flowie_task_group_t group;
    flowie_task_group_wait_fixture_t fixture;
    turbo_thread_t waiter;
    int create_rc;
    int rejected_rc;
    int waiter_blocked;
    int join_rc = TURBO_OK;

    flowie_task_group_init(&group);
    fixture.group = &group;
    atomic_init(&fixture.entered, 0);
    atomic_init(&fixture.exited, 0);
    create_rc = flowie_task_group_open(&group);
    if (create_rc == TURBO_OK) create_rc = flowie_task_group_try_begin(&group);
    if (create_rc == TURBO_OK)
      create_rc = turbo_thread_create(&waiter, flowie_task_group_wait_thread, &fixture);
    if (create_rc == TURBO_OK) {
      while (!atomic_load_explicit(&fixture.entered, memory_order_acquire))
        turbo_thread_yield();
      flowie_task_group_close(&group);
      rejected_rc = flowie_task_group_try_begin(&group);
      turbo_sleep_ms(10u);
      waiter_blocked = !atomic_load_explicit(&fixture.exited, memory_order_acquire);
      flowie_task_group_end(&group);
      join_rc = turbo_thread_join(&waiter);
      turbo_thread_destroy(&waiter);
    } else {
      rejected_rc = TURBO_EALREADY;
      waiter_blocked = 0;
      if (group.count != 0u) flowie_task_group_end(&group);
    }
    flowie_task_group_destroy(&group);

    check_int_eq(create_rc, TURBO_OK);
    check_int_eq(rejected_rc, TURBO_ESHUTDOWN);
    check_true(waiter_blocked);
    check_int_eq(join_rc, TURBO_OK);
    check_int_eq(atomic_load_explicit(&fixture.exited, memory_order_acquire), 1);
  }

  it("serializes managed-session expiry cancellation with immediate shutdown") {
    enum { IMMEDIATE_STOP_CYCLES = 32u };
    flowie_endpoint_capture_t capture = {0};
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;

    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    flow = flowie_managed_session_flow(port, &capture);
    check_not_null(flow);
    if (!flow) return;

    for (size_t cycle = 0u; cycle < IMMEDIATE_STOP_CYCLES; ++cycle) {
      uint64_t started_at;
      uint64_t elapsed;

      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      started_at = turbo_monotonic_ms();
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
      elapsed = turbo_monotonic_ms() - started_at;
      info("cycle=%zu stop_elapsed_ms=%llu", cycle, (unsigned long long)elapsed);
      check_size_le((size_t)elapsed, (size_t)FLOWIE_TEST_STOP_MAX_MS);
    }

    turbo_flow_destroy(flow);
  }

  it("rejects invalid endpoint configuration before registration") {
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    config.size -= 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.size = sizeof(config) + 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.size = sizeof(config);
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
    config.stream_recv_buffer_bytes = FLOWIE_MIN_RECV_BUFFER_SIZE - 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ERANGE);
    config.stream_recv_buffer_bytes = 0u;
    config.socket_recv_buffer_bytes = (size_t)INT_MAX + 1u;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_ERANGE);
    config.socket_recv_buffer_bytes = 1024u * 1024u;
    config.transport = FLOWIE_TRANSPORT_PIPE;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.transport = FLOWIE_TRANSPORT_TCP;
    config.socket_recv_buffer_bytes = 0u;
    config.tls_client_ca_file = "client-ca.pem";
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
    config.transport = FLOWIE_TRANSPORT_TLS;
    check_int_eq(flowie_register_endpoint(flow, "flowie.invalid", &config), TURBO_EINVAL);
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
    config.stream_recv_buffer_bytes = 128u * 1024u;
    config.socket_recv_buffer_bytes = 1024u * 1024u;
    config.socket_send_buffer_bytes = 512u * 1024u;
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

  it("MQTT-SEC-008 keeps undeclared Origin policy out of strict endpoint configuration") {
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
                                "      stream_recv_buffer_bytes: 4096\n"
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
                                                 "      stream_recv_buffer_bytes: 512\n";
    static const char undeclared_origin_policy[] =
        "version: 1\nadapters:\n  mqtt.endpoint:\n    kind: flowie_endpoint\n    config:\n"
        "      transport: wss\n      host: 127.0.0.1\n      port: 8884\n"
        "      path: /mqtt\n      allowed_origins: https://console.example\n";
    static const char valid_trusted_proxy[] =
        "version: 1\nadapters:\n  mqtt.endpoint:\n    kind: flowie_endpoint\n    config:\n"
        "      transport: tcp\n      host: 127.0.0.1\n      port: 18883\n"
        "      trusted_proxy_cidrs: '127.0.0.1/32, ::1/128'\n"
        "      proxy_header_max_bytes: 256\n      proxy_header_timeout_ms: 1000\n";
    static const char incomplete_trusted_proxy[] =
        "version: 1\nadapters:\n  mqtt.endpoint:\n    kind: flowie_endpoint\n    config:\n"
        "      transport: tls\n      host: 127.0.0.1\n      port: 8883\n"
        "      trusted_proxy_cidrs: 127.0.0.1/32\n";
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

    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(
                     valid_trusted_proxy, sizeof(valid_trusted_proxy) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
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
          {undersized_recv_buffer, sizeof(undersized_recv_buffer) - 1u, "stream_recv_buffer_bytes"},
          {undeclared_origin_policy, sizeof(undeclared_origin_policy) - 1u, "allowed_origins"},
          {incomplete_trusted_proxy, sizeof(incomplete_trusted_proxy) - 1u, "trusted_proxy_cidrs"}};
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

  it("MQTT-SEC-005/006 authenticates CONNECT against one dynamic ACL generation") {
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
    static const uint8_t subscribe_mixed[] = {
        0x82u, 0x2au, 0x00u, 0x05u, 0x00u, 0x00u, 0x0fu, 'r', 'o', 'o',   't',
        '-',   'a',   '/',   '+',   '/',   'e',   'v',   'e', 'n', 't',   's',
        0x01u, 0x00u, 0x06u, '$',   'S',   'Y',   'S',   '/', '#', 0x01u, 0x00u,
        0x09u, 'r',   'o',   'o',   't',   '-',   'a',   'x', '/', '#',   0x01u};
    static const uint8_t suback_mixed[] = {0x90u, 0x06u, 0x00u, 0x05u, 0x00u, 0x01u, 0x87u, 0x87u};
    static const uint8_t connack_v31[] = {0x20u, 0x02u, 0x00u, 0x00u};
    static const uint8_t normal_disconnect[] = {0xe0u, 0x00u};
    static const uint8_t subscribe_denied_v31[] = {0x82u, 0x06u, 0x00u, 0x04u,
                                                   0x00u, 0x01u, '#',   0x01u};
    static const uint8_t publish_denied_v31[] = {0x32u, 0x12u, 0x00u, 0x0du, 'r',   'o', 'o',
                                                 't',   '-',   'b',   '/',   'e',   'v', 'e',
                                                 'n',   't',   's',   0x00u, 0x02u, 'x'};
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
    flowie_test_socket_t legacy_publish;
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
    check_int_eq(flowie_test_send(client, subscribe_mixed, sizeof(subscribe_mixed)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(suback_mixed)), TURBO_OK);
    check_mem_eq(received, suback_mixed, sizeof(suback_mixed));

    legacy = flowie_test_connect(port);
    check_true(legacy != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(legacy, legacy_connect_packet, legacy_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(legacy, received, sizeof(connack_v31)), TURBO_OK);
    check_mem_eq(received, connack_v31, sizeof(connack_v31));
    check_int_eq(flowie_test_send(legacy, subscribe_denied_v31, sizeof(subscribe_denied_v31)),
                 TURBO_OK);
    check_true(flowie_test_socket_readable(legacy, 500u));
    check(flowie_test_recv_exact(legacy, received, 1u) != TURBO_OK);
    flowie_test_socket_close(legacy);

    legacy_publish = flowie_test_connect(port);
    check_true(legacy_publish != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(legacy_publish, legacy_connect_packet, legacy_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(legacy_publish, received, sizeof(connack_v31)), TURBO_OK);
    check_mem_eq(received, connack_v31, sizeof(connack_v31));
    check_int_eq(flowie_test_send(legacy_publish, publish_denied_v31, sizeof(publish_denied_v31)),
                 TURBO_OK);
    check_true(flowie_test_socket_readable(legacy_publish, 500u));
    check(flowie_test_recv_exact(legacy_publish, received, 1u) != TURBO_OK);
    check_size_eq(auth.calls, 6u);
    check_true(strncmp(auth.remote_address, "127.0.0.1:", sizeof("127.0.0.1:") - 1u) == 0);
    check_str_eq(auth.peer_certificate_sha256, "");

    flowie_test_socket_close(legacy_publish);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("accepts HAProxy PROXY v1 before plaintext MQTT and forwards trusted provenance") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const uint8_t expected_connack[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                                               0x02u, 0x27u, 0x00u, 0x00u, 0x04u, 0x00u};
    static const char *const trusted_proxy_cidrs[] = {"127.0.0.1/32"};
    flowie_endpoint_capture_t capture = {0};
    flowie_endpoint_proxy_binding_t proxy = FLOWIE_ENDPOINT_PROXY_BINDING_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t cluster_fixture;
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packet[128];
    uint8_t request[256];
    uint8_t received[sizeof(expected_connack)];
    char proxy_header[128];
    size_t connect_size = 0u;
    size_t request_size;
    int proxy_header_size;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;

    memset(&cluster_fixture, 0, sizeof(cluster_fixture));
    atomic_init(&capture.calls, 0u);
    atomic_init(&cluster_fixture.connect_calls, 0u);
    atomic_init(&cluster_fixture.command_calls, 0u);
    atomic_init(&cluster_fixture.settlement_calls, 0u);
    atomic_init(&cluster_fixture.lost_calls, 0u);
    atomic_init(&cluster_fixture.detach_calls, 0u);
    check_int_gt(port, 0);
    check_not_null(flow);
    proxy_header_size =
        snprintf(proxy_header, sizeof(proxy_header),
                 "PROXY TCP4 203.0.113.9 127.0.0.1 45678 %u\r\n", (unsigned int)port);
    check_int_gt(proxy_header_size, 8);
    check_true((size_t)proxy_header_size < sizeof(proxy_header));

    config.transport = FLOWIE_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_packet_size = 1024u;
    config.max_connections = 2u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 2u;
    config.max_inflight_per_session = 2u;
    proxy.trusted_peer_cidrs = trusted_proxy_cidrs;
    proxy.trusted_peer_count = 1u;
    proxy.max_header_bytes = 256u;
    proxy.header_timeout_ms = 1000u;
    cluster.ctx = &cluster_fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.proxy = &proxy;
    bindings.cluster = &cluster;
    check_int_eq(flowie_register_bound_endpoint(flow, "flowie.endpoint", &config, &bindings),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id =
        (flowie_mqtt_span_t){(const uint8_t *)"haproxy-client", sizeof("haproxy-client") - 1u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    request_size = (size_t)proxy_header_size + connect_size;
    check_true(request_size <= sizeof(request));
    memcpy(request, proxy_header, (size_t)proxy_header_size);
    memcpy(request + (size_t)proxy_header_size, connect_packet, connect_size);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, request, request_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, expected_connack, sizeof(expected_connack));
    check_uint_eq(atomic_load_explicit(&cluster_fixture.connect_calls, memory_order_acquire), 1u);
    check_str_eq(cluster_fixture.client_id, "haproxy-client");
    check_str_eq(cluster_fixture.remote_address, "203.0.113.9:45678");
    check_true(strncmp(cluster_fixture.transport_peer_address,
                       "127.0.0.1:", sizeof("127.0.0.1:") - 1u) == 0);
    check_size_eq(cluster_fixture.proxy_tlvs_size, 0u);

    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-SEC-003 forwards verified TLS and trusted PROXY identities to Auth") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    flowie_endpoint_capture_t capture = {0};
    flowie_security_fixture_t auth = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    static const char *const trusted_proxy_cidrs[] = {"127.0.0.1/32"};
    static const uint8_t expected_proxy_tlvs[] = {0xe0u, 0x00u, 0x02u, 'a', 'b'};
    flowie_endpoint_proxy_binding_t proxy = FLOWIE_ENDPOINT_PROXY_BINDING_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_cluster_endpoint_fixture_t cluster_fixture;
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_tls_auth_client_t client = {0};
    uint8_t connect_packet[128];
    uint8_t proxy_header[33] = {0x0du, 0x0au, 0x0du, 0x0au, 0x00u, 0x0du, 0x0au, 0x51u, 0x55u,
                                0x49u, 0x54u, 0x0au, 0x21u, 0x11u, 0x00u, 0x11u, 203u,  0u,
                                113u,  9u,    127u,  0u,    0u,    1u,    0xb2u, 0x6eu, 0u,
                                0u,    0xe0u, 0x00u, 0x02u, 'a',   'b'};
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();

    memset(&cluster_fixture, 0, sizeof(cluster_fixture));
    atomic_init(&capture.calls, 0u);
    atomic_init(&cluster_fixture.connect_calls, 0u);
    atomic_init(&cluster_fixture.command_calls, 0u);
    atomic_init(&cluster_fixture.lost_calls, 0u);
    atomic_init(&cluster_fixture.detach_calls, 0u);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(tls_test_set_server_env(cert_file, key_file), 0);

    rule.effect = TURBO_FLOW_SECURITY_ALLOW;
    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    (void)snprintf(rule.subject, sizeof(rule.subject), "%s", "writer");
    (void)snprintf(rule.root_group_id, sizeof(rule.root_group_id), "%s", "root-a");
    rule.action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    rule.match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    (void)snprintf(rule.pattern, sizeof(rule.pattern), "%s", "mtls-");
    realm_config.resource_uid = "security:mtls-auth-context";
    realm_config.owner_name = "security.mtls-auth-context";
    realm_config.policy_version = 1u;
    realm_config.rules = &rule;
    realm_config.rule_count = 1u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);

    security.realm_channel = "security.mtls-auth-context";
    security.auth_method = "password";
    security.auth_provider = &provider;
    security.realm = realm;
    config.transport = FLOWIE_TRANSPORT_TLS;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_connections = 2u;
    config.manage_sessions = 1;
    config.max_sessions = 2u;
    config.max_subscriptions_per_session = 2u;
    config.max_inflight_per_session = 2u;
    config.tls_client_ca_file = ca_file;
    proxy_header[26] = (uint8_t)(port >> 8u);
    proxy_header[27] = (uint8_t)port;
    proxy.trusted_peer_cidrs = trusted_proxy_cidrs;
    proxy.trusted_peer_count = 1u;
    proxy.max_header_bytes = 256u;
    proxy.header_timeout_ms = 1000u;
    cluster.ctx = &cluster_fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = flowie_cluster_endpoint_connect;
    cluster.command = flowie_cluster_endpoint_command;
    cluster.settle = flowie_cluster_endpoint_settle;
    cluster.connection_lost = flowie_cluster_endpoint_connection_lost;
    cluster.detach = flowie_cluster_endpoint_detach;
    bindings.security = &security;
    bindings.proxy = &proxy;
    bindings.cluster = &cluster;
    check_int_eq(flowie_register_bound_endpoint(flow, "flowie.endpoint", &config, &bindings),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"mtls-client", 11u};
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    client.ca_file = ca_file;
    client.connect_packet = connect_packet;
    client.connect_packet_size = connect_size;
    client.port = port;
    client.cert_file = cert_file;
    client.key_file = key_file;
    check_int_ne(flowie_tls_auth_client_execute(&client), TURBO_OK);
    check_size_eq(auth.calls, 0u);

    client.cert_file = NULL;
    client.key_file = NULL;
    client.proxy_header = proxy_header;
    client.proxy_header_size = sizeof(proxy_header);
    check_int_ne(flowie_tls_auth_client_execute(&client), TURBO_OK);
    check_size_eq(auth.calls, 0u);

    client.cert_file = cert_file;
    client.key_file = key_file;
    check_int_eq(flowie_tls_auth_client_execute(&client), TURBO_OK);
    check_size_eq(auth.calls, 1u);
    check_str_eq(auth.remote_address, "203.0.113.9:45678");
    check_true(strncmp(auth.transport_peer_address, "127.0.0.1:", sizeof("127.0.0.1:") - 1u) == 0);
    check_uint_eq(atomic_load_explicit(&cluster_fixture.connect_calls, memory_order_acquire), 1u);
    check_str_eq(cluster_fixture.remote_address, "203.0.113.9:45678");
    check_true(strncmp(cluster_fixture.transport_peer_address,
                       "127.0.0.1:", sizeof("127.0.0.1:") - 1u) == 0);
    check_size_eq(cluster_fixture.proxy_tlvs_size, sizeof(expected_proxy_tlvs));
    check_mem_eq(cluster_fixture.proxy_tlvs, expected_proxy_tlvs, sizeof(expected_proxy_tlvs));
    check_size_eq(
        strlen(auth.peer_certificate_sha256),
        sizeof("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef") - 1u);
    check_true(strncmp(auth.peer_certificate_sha256, "sha256:", sizeof("sha256:") - 1u) == 0);
    for (size_t index = sizeof("sha256:") - 1u; auth.peer_certificate_sha256[index] != '\0';
         ++index)
      check_true((auth.peer_certificate_sha256[index] >= '0' &&
                  auth.peer_certificate_sha256[index] <= '9') ||
                 (auth.peer_certificate_sha256[index] >= 'a' &&
                  auth.peer_certificate_sha256[index] <= 'f'));

    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
    tls_test_clear_server_env();
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(ca_file);
  }

  it("MQTT-SEC-003 maps remote auth denial to CONNACK and provider failures to close") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const int provider_failures[] = {TURBO_ETIMEDOUT, TURBO_EIO, TURBO_EPROTO};
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
    uint8_t connect_packet[128];
    uint8_t received = 0u;
    size_t connect_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client;
    int close_rc;

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
    realm_config.resource_uid = "security:http-auth-failure-test";
    realm_config.owner_name = "security.http-auth-failure-test";
    realm_config.policy_version = 1u;
    realm_config.rules = &rule;
    realm_config.rule_count = 1u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);
    security.realm_channel = "security.http-auth-failure-test";
    security.auth_method = "password";
    security.auth_provider = &provider;
    security.realm = realm;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_connections = 4u;
    config.manage_sessions = 1;
    config.max_sessions = 1u;
    config.max_subscriptions_per_session = 1u;
    config.max_inflight_per_session = 1u;
    check_int_eq(flowie_register_secure_endpoint(flow, "flowie.endpoint", &config, &security),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id =
        (flowie_mqtt_span_t){(const uint8_t *)"secure-http-auth", sizeof("secure-http-auth") - 1u};
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", sizeof("writer") - 1u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", sizeof("secret") - 1u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);

    auth.result = TURBO_EPERM;
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, UINT8_C(0x86)), TURBO_OK);
    flowie_test_socket_close(client);
    for (size_t i = 0u; i < sizeof(provider_failures) / sizeof(provider_failures[0]); ++i) {
      auth.result = provider_failures[i];
      client = flowie_test_connect(port);
      check_true(client != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
      check_true(flowie_test_socket_readable(client, 1500u));
      close_rc = flowie_test_recv_exact(client, &received, 1u);
      check_true(close_rc != TURBO_OK || received == UINT8_C(0xe0));
      flowie_test_socket_close(client);
    }
    check_size_eq(auth.calls, 4u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
  }

  it("MQTT-SEC-006 applies ACL generations atomically and revokes at authentication boundaries") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const uint8_t publish_v1[] = {0x32u, 0x14u, 0x00u, 0x0eu, 'r',   'o', 'o', 't',
                                         '-',   'a',   '/',   'v',   '1',   '/', 'd', 'a',
                                         't',   'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t publish_v2_first[] = {0x32u, 0x14u, 0x00u, 0x0eu, 'r',   'o', 'o', 't',
                                               '-',   'a',   '/',   'v',   '2',   '/', 'd', 'a',
                                               't',   'a',   0x00u, 0x03u, 0x00u, 'x'};
    static const uint8_t publish_v2_after_revoke[] = {
        0x32u, 0x14u, 0x00u, 0x0eu, 'r', 'o', 'o', 't',   '-',   'a',   '/',
        'v',   '2',   '/',   'd',   'a', 't', 'a', 0x00u, 0x04u, 0x00u, 'x'};
    static const uint8_t subscribe_v1[] = {0x82u, 0x11u, 0x00u, 0x02u, 0x00u, 0x00u, 0x0bu,
                                           'r',   'o',   'o',   't',   '-',   'a',   '/',
                                           'v',   '1',   '/',   '#',   0x01u};
    static const uint8_t puback_first[] = {0x40u, 0x02u, 0x00u, 0x03u};
    static const uint8_t puback_after_revoke[] = {0x40u, 0x02u, 0x00u, 0x04u};
    static const char *client_ids[] = {"secure-generation-publish", "secure-generation-subscribe",
                                       "secure-generation-current", "secure-generation-reconnect"};
    flowie_endpoint_capture_t capture = {0};
    flowie_security_fixture_t auth = {0};
    flowie_policy_fixture_t policy = {0};
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &auth,
                                                    flowie_test_authenticate};
    turbo_flow_security_policy_provider_t policy_provider = {
        sizeof(policy_provider), &policy, flowie_test_policy_load, flowie_test_policy_release};
    turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t connect_packets[4][128];
    size_t connect_sizes[4] = {0};
    uint8_t received[sizeof(puback_first)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t old_publish;
    flowie_test_socket_t old_subscribe;
    flowie_test_socket_t current;
    flowie_test_socket_t reconnect;
    int close_rc;

    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(flowie_mqtt_security_matcher_init(&matcher), TURBO_OK);
    for (size_t i = 0u; i < 2u; ++i) {
      policy.rules[i] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
      policy.rules[i].effect = TURBO_FLOW_SECURITY_ALLOW;
      policy.rules[i].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
      (void)snprintf(policy.rules[i].subject, sizeof(policy.rules[i].subject), "%s", "writer");
      (void)snprintf(policy.rules[i].root_group_id, sizeof(policy.rules[i].root_group_id), "%s",
                     "root-a");
    }
    policy.rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    policy.rules[0].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    policy.rules[0].match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
    (void)snprintf(policy.rules[0].pattern, sizeof(policy.rules[0].pattern), "%s", "secure-");
    policy.rules[1].action_mask =
        TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    policy.rules[1].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    policy.rules[1].match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
    (void)snprintf(policy.rules[1].pattern, sizeof(policy.rules[1].pattern), "%s", "root-a/v1/#");
    policy.policy_version = 1u;
    policy.rule_count = 2u;
    auth.policy_version = 1u;

    realm_config.resource_uid = "security:generation-test";
    realm_config.owner_name = "security.generation-test";
    realm_config.matcher = matcher;
    realm_config.policy_source = "acl.generation-test";
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_bind_policy_provider(realm, &policy_provider), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);
    security.realm_channel = "security.generation-test";
    security.auth_method = "password";
    security.auth_provider = &provider;
    security.realm = realm;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.max_connections = 4u;
    config.recv_timeout_ms = 0u;
    config.manage_sessions = 1;
    config.max_sessions = 4u;
    config.max_subscriptions_per_session = 4u;
    config.max_inflight_per_session = 4u;
    check_int_eq(flowie_register_secure_endpoint(flow, "flowie.endpoint", &config, &security),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_endpoint_capture_stage,
                                              &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.username = (flowie_mqtt_span_t){(const uint8_t *)"writer", 6u};
    connect.password = (flowie_mqtt_span_t){(const uint8_t *)"secret", 6u};
    for (size_t i = 0u; i < 4u; ++i) {
      connect.client_id =
          (flowie_mqtt_span_t){(const uint8_t *)client_ids[i], strlen(client_ids[i])};
      check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_packets[i],
                                                     sizeof(connect_packets[i]), &connect_sizes[i]),
                   FLOWIE_MQTT_PARSE_OK);
    }

    old_publish = flowie_test_connect(port);
    old_subscribe = flowie_test_connect(port);
    check_true(old_publish != FLOWIE_TEST_INVALID_SOCKET);
    check_true(old_subscribe != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(old_publish, connect_packets[0], connect_sizes[0]), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(old_publish, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(old_subscribe, connect_packets[1], connect_sizes[1]), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(old_subscribe, 0u, 0u), TURBO_OK);

    policy.policy_version = 2u;
    auth.policy_version = 2u;
    (void)snprintf(policy.rules[1].pattern, sizeof(policy.rules[1].pattern), "%s", "root-a/v2/#");
    check_int_eq(turbo_flow_security_realm_refresh(realm, 2u, (uint64_t)time(NULL)), TURBO_OK);

    check_int_eq(flowie_test_send(old_publish, publish_v1, sizeof(publish_v1)), TURBO_OK);
    check_true(flowie_test_socket_readable(old_publish, 1500u));
    close_rc = flowie_test_recv_exact(old_publish, received, 1u);
    check_true(close_rc != TURBO_OK || received[0] == UINT8_C(0xe0));
    check_int_eq(flowie_test_send(old_subscribe, subscribe_v1, sizeof(subscribe_v1)), TURBO_OK);
    check_true(flowie_test_socket_readable(old_subscribe, 1500u));
    close_rc = flowie_test_recv_exact(old_subscribe, received, 1u);
    check_true(close_rc != TURBO_OK || received[0] == UINT8_C(0xe0));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);

    current = flowie_test_connect(port);
    check_true(current != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(current, connect_packets[2], connect_sizes[2]), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(current, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(current, publish_v2_first, sizeof(publish_v2_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(current, received, sizeof(puback_first)), TURBO_OK);
    check_mem_eq(received, puback_first, sizeof(puback_first));
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);

    auth.revoked = 1;
    check_int_eq(
        flowie_test_send(current, publish_v2_after_revoke, sizeof(publish_v2_after_revoke)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(current, received, sizeof(puback_after_revoke)), TURBO_OK);
    check_mem_eq(received, puback_after_revoke, sizeof(puback_after_revoke));
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);
    reconnect = flowie_test_connect(port);
    check_true(reconnect != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(reconnect, connect_packets[3], connect_sizes[3]), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(reconnect, 0u, UINT8_C(0x86)), TURBO_OK);

    flowie_test_socket_close(reconnect);
    flowie_test_socket_close(current);
    flowie_test_socket_close(old_subscribe);
    flowie_test_socket_close(old_publish);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
  }

  it("MQTT-SEC-006 disconnects active MQTT 5 and MQTT 3 principals after expiry") {
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

  it("MQTT-SEC-004 completes initial Enhanced AUTH and rejects identity-changing re-auth") {
    static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                                "stage capture worker 1 capacity 8\n"
                                "stage main {\n"
                                "  mqtt_in -> capture\n"
                                "}\n";
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    static const uint8_t expired_disconnect[] = {0xe0u, 0x01u, 0x87u};
    static const uint8_t method_disconnect[] = {0xe0u, 0x01u, 0x8cu};
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
    uint8_t client_middle_properties[64];
    uint8_t client_final_properties[64];
    uint8_t wrong_method_properties[64];
    uint8_t server_first_properties[64];
    uint8_t server_middle_properties[64];
    uint8_t server_final_properties[64];
    uint8_t connect_packet[192];
    uint8_t client_middle[128];
    uint8_t client_continue[128];
    uint8_t client_reauth[128];
    uint8_t wrong_method_continue[128];
    uint8_t server_challenge[128];
    uint8_t server_middle[128];
    uint8_t server_success[128];
    uint8_t received[128];
    char connack_method[32];
    char connack_data[32];
    size_t client_first_size = 0u;
    size_t client_middle_size = 0u;
    size_t client_final_size = 0u;
    size_t wrong_method_size = 0u;
    size_t server_first_size = 0u;
    size_t server_middle_size = 0u;
    size_t server_final_size = 0u;
    size_t connect_size = 0u;
    size_t client_middle_packet_size = 0u;
    size_t client_continue_size = 0u;
    size_t client_reauth_size = 0u;
    size_t wrong_method_packet_size = 0u;
    size_t server_challenge_size = 0u;
    size_t server_middle_packet_size = 0u;
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
        flowie_test_auth_properties_encode("challenge", "client-middle", client_middle_properties,
                                           sizeof(client_middle_properties), &client_middle_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "client-final", client_final_properties,
                                           sizeof(client_final_properties), &client_final_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("wrong", "client-middle", wrong_method_properties,
                                           sizeof(wrong_method_properties), &wrong_method_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "server-first", server_first_properties,
                                           sizeof(server_first_properties), &server_first_size),
        TURBO_OK);
    check_int_eq(
        flowie_test_auth_properties_encode("challenge", "server-middle", server_middle_properties,
                                           sizeof(server_middle_properties), &server_middle_size),
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
    auth.properties = (flowie_mqtt_span_t){client_middle_properties, client_middle_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, client_middle, sizeof(client_middle),
                                                   &client_middle_packet_size),
                 FLOWIE_MQTT_PARSE_OK);
    auth.properties = (flowie_mqtt_span_t){client_final_properties, client_final_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, client_continue, sizeof(client_continue),
                                                   &client_continue_size),
                 FLOWIE_MQTT_PARSE_OK);
    auth.properties = (flowie_mqtt_span_t){wrong_method_properties, wrong_method_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, wrong_method_continue,
                                                   sizeof(wrong_method_continue),
                                                   &wrong_method_packet_size),
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
    auth.properties = (flowie_mqtt_span_t){server_middle_properties, server_middle_size};
    check_int_eq(flowie_mqtt_control_packet_encode(&auth, server_middle, sizeof(server_middle),
                                                   &server_middle_packet_size),
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
    enhanced_fixture.rounds_per_exchange = 2u;
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_challenge_size), TURBO_OK);
    check_mem_eq(received, server_challenge, server_challenge_size);
    check_int_eq(flowie_test_send(client, client_middle, client_middle_packet_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_middle_packet_size), TURBO_OK);
    check_mem_eq(received, server_middle, server_middle_packet_size);
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
    check_int_eq(flowie_test_send(client, client_middle, client_middle_packet_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_middle_packet_size), TURBO_OK);
    check_mem_eq(received, server_middle, server_middle_packet_size);
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
    flowie_test_socket_close(client);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, server_challenge_size), TURBO_OK);
    check_mem_eq(received, server_challenge, server_challenge_size);
    check_int_eq(flowie_test_send(client, wrong_method_continue, wrong_method_packet_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(method_disconnect)), TURBO_OK);
    check_mem_eq(received, method_disconnect, sizeof(method_disconnect));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS && enhanced_fixture.cancel_calls < 3u; ++i)
      turbo_sleep_ms(1u);
    check_size_eq(enhanced_fixture.begin_calls, 3u);
    check_size_eq(enhanced_fixture.continue_calls, 4u);
    check_size_eq(enhanced_fixture.cancel_calls, 3u);
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

  it("processes coalesced QoS1 publishes and acknowledgement followed by ping in order") {
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, 'a',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t publishes[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x2au, 0x00u, 'x',
                                        0x32u, 0x07u, 0x00u, 0x01u, 'a', 0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t publisher_pubacks[] = {0x40u, 0x02u, 0x00u, 0x2au,
                                                0x40u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t subscriber_deliveries[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',   0x00u,
                                                    0x01u, 0x00u, 'x',   0x32u, 0x07u, 0x00u,
                                                    0x01u, 'a',   0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t acknowledgements_and_ping[] = {0x40u, 0x02u, 0x00u, 0x01u, 0x40u,
                                                        0x02u, 0x00u, 0x02u, 0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    static const uint8_t disconnect[] = {0xe0u, 0x00u};
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    turbo_flow_resource_snapshot_t sessions = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    uint8_t publisher_connect[128];
    uint8_t subscriber_connect[128];
    uint8_t received[sizeof(subscriber_deliveries)];
    size_t publisher_connect_size = 0u;
    size_t subscriber_connect_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;

    check_int_gt(port, 0);
    check_not_null(flow);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"coalesced-publisher", 19u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, publisher_connect,
                                                   sizeof(publisher_connect),
                                                   &publisher_connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"coalesced-subscriber", 20u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, subscriber_connect,
                                                   sizeof(subscriber_connect),
                                                   &subscriber_connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    subscriber = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, publisher_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    check_int_eq(flowie_test_send(publisher, publishes, sizeof(publishes)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubacks)), TURBO_OK);
    check_mem_eq(received, publisher_pubacks, sizeof(publisher_pubacks));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(subscriber_deliveries)),
                 TURBO_OK);
    check_mem_eq(received, subscriber_deliveries, sizeof(subscriber_deliveries));
    check_int_eq(
        flowie_test_send(subscriber, acknowledgements_and_ping, sizeof(acknowledgements_and_ping)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));

    check_int_eq(flowie_test_send(subscriber, disconnect, sizeof(disconnect)), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, disconnect, sizeof(disconnect)), TURBO_OK);
    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
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

  it("MQTT-PROTO-004 resolves Topic Alias and disconnects on an alias above the limit") {
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

  it("preserves the MQTT 5 takeover reason across the initial CONNACK completion handoff") {
    enum { TAKEOVER_CONNACK_HANDOFF_CYCLES = 32u };
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t takeover_disconnect[] = {0xe0u, 0x01u, 0x8eu};
    uint8_t received[sizeof(takeover_disconnect)];

    for (size_t cycle = 0u; cycle < TAKEOVER_CONNACK_HANDOFF_CYCLES; ++cycle) {
      flowie_endpoint_capture_t capture = {0};
      unsigned short port = flowie_test_port();
      turbo_flow_t *flow;
      flowie_test_socket_t first;
      flowie_test_socket_t replacement;

      atomic_init(&capture.calls, 0u);
      check_int_gt(port, 0);
      flow = flowie_managed_session_flow(port, &capture);
      check_not_null(flow);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      first = flowie_test_connect(port);
      check_true(first != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(first, connect_packet, sizeof(connect_packet)), TURBO_OK);
      check_int_eq(flowie_test_recv_connack(first, 0u, 0u), TURBO_OK);
      replacement = flowie_test_connect(port);
      check_true(replacement != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(replacement, connect_packet, sizeof(connect_packet)), TURBO_OK);
      check_int_eq(flowie_test_recv_connack(replacement, 1u, 0u), TURBO_OK);
      check_int_eq(flowie_test_recv_exact(first, received, sizeof(received)), TURBO_OK);
      check_mem_eq(received, takeover_disconnect, sizeof(takeover_disconnect));
      flowie_test_socket_close(replacement);
      flowie_test_socket_close(first);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }
  }

  it("MQTT-OWNER-003 takes over MQTT 5 and fences the old connection") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t first_connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t resumed_connack[] = {0x20u, 0x03u, 0x01u, 0x00u, 0x00u};
    static const uint8_t takeover_connack[] = {0x20u, 0x03u, 0x01u, 0x00u, 0x00u};
    static const uint8_t takeover_disconnect[] = {0xe0u, 0x01u, 0x8eu};
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
    check_int_eq(flowie_test_recv_connack(duplicate, 1u, 0u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(resumed, received, sizeof(takeover_disconnect)), TURBO_OK);
    check_mem_eq(received, takeover_disconnect, sizeof(takeover_disconnect));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 1u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 0u);

    check_int_eq(flowie_test_send(duplicate, publish_qos1, sizeof(publish_qos1)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_int_eq(flowie_wait_calls(&capture, 1u), TURBO_OK);
    check_uint_eq(capture.types[0], FLOWIE_MQTT_PACKET_PUBLISH);
    check_mem_eq(capture.packets[0], publish_qos1, sizeof(publish_qos1));

    check_int_eq(flowie_test_send(duplicate, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);

    check_int_eq(flowie_test_send(duplicate, unsubscribe, sizeof(unsubscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(unsuback)), TURBO_OK);
    check_mem_eq(received, unsuback, sizeof(unsuback));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);

    check_int_eq(flowie_test_send(duplicate, publish_qos2, sizeof(publish_qos2)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(pubrec)), TURBO_OK);
    check_mem_eq(received, pubrec, sizeof(pubrec));
    check_int_eq(flowie_wait_calls(&capture, 2u), TURBO_OK);
    check_uint_eq(capture.types[1], FLOWIE_MQTT_PACKET_PUBLISH);
    check_mem_eq(capture.packets[1], publish_qos2, sizeof(publish_qos2));

    check_int_eq(
        flowie_test_send(duplicate, publish_qos2_duplicate, sizeof(publish_qos2_duplicate)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(pubrec)), TURBO_OK);
    check_mem_eq(received, pubrec, sizeof(pubrec));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 2u);

    check_int_eq(flowie_test_send(duplicate, pubrel, sizeof(pubrel)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(pubcomp)), TURBO_OK);
    check_mem_eq(received, pubcomp, sizeof(pubcomp));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 2u);

    check_int_eq(flowie_test_send(duplicate, unknown_pubrel, sizeof(unknown_pubrel)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(unknown_pubcomp)), TURBO_OK);
    check_mem_eq(received, unknown_pubcomp, sizeof(unknown_pubcomp));

    check_int_eq(flowie_test_send(duplicate, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 2u);

    check_int_eq(flowie_test_send(duplicate, auth, sizeof(auth)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(duplicate, received, sizeof(auth_disconnect)), TURBO_OK);
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

  it("MQTT-OWNER-003 takes over MQTT 3.1.1 by closing the old connection") {
    static const uint8_t connect_packet[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q',
                                             'T',   'T',   0x04u, 0x00u, 0x00u, 0x3cu,
                                             0x00u, 0x03u, 'v',   '3',   'x'};
    static const uint8_t first_connack[] = {0x20u, 0x02u, 0x00u, 0x00u};
    static const uint8_t takeover_connack[] = {0x20u, 0x02u, 0x01u, 0x00u};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_endpoint_capture_t capture;
    uint8_t received[sizeof(first_connack)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t old_connection;
    flowie_test_socket_t replacement;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    flow = flowie_managed_session_flow(port, &capture);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    old_connection = flowie_test_connect(port);
    replacement = flowie_test_connect(port);
    check_true(old_connection != FLOWIE_TEST_INVALID_SOCKET);
    check_true(replacement != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(old_connection, connect_packet, sizeof(connect_packet)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(old_connection, received, sizeof(first_connack)), TURBO_OK);
    check_mem_eq(received, first_connack, sizeof(first_connack));
    check_int_eq(flowie_test_send(replacement, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(replacement, received, sizeof(takeover_connack)), TURBO_OK);
    check_mem_eq(received, takeover_connack, sizeof(takeover_connack));
    check_true(flowie_test_socket_readable(old_connection, 1000u));
    check_int_ne(flowie_test_recv_exact(old_connection, received, 1u), TURBO_OK);
    check_int_eq(flowie_test_send(replacement, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(replacement, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(replacement);
    flowie_test_socket_close(old_connection);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-PROTO-001/002/003/005/006/008/010 returns exact reasons for hostile packets") {
    static const uint8_t connect_packet[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                             'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                             0x00u, 0x00u, 0x03u, 'b',   'a',   'd'};
    static const uint8_t invalid_flags[] = {0xc1u, 0x00u};
    static const uint8_t malformed_vbi[] = {0x30u, 0x80u, 0x00u};
    static const uint8_t invalid_utf8[] = {0x30u, 0x05u, 0x00u, 0x02u, 0xc0u, 0x80u, 0x00u};
    static const uint8_t duplicate_topic_alias[] = {0x30u, 0x0au, 0x00u, 0x01u, 'a',   0x06u,
                                                    0x23u, 0x00u, 0x01u, 0x23u, 0x00u, 0x02u};
    static const uint8_t disallowed_receive_maximum[] = {0x30u, 0x07u, 0x00u, 0x01u, 'a',
                                                         0x03u, 0x21u, 0x00u, 0x01u};
    static const uint8_t oversized_header[] = {0x30u, 0x1fu};
    static const struct {
      const uint8_t *packet;
      size_t packet_size;
      size_t first_fragment_size;
      uint8_t reason_code;
    } cases[] = {{connect_packet, sizeof(connect_packet), sizeof(connect_packet), 0x82u},
                 {invalid_flags, sizeof(invalid_flags), sizeof(invalid_flags), 0x82u},
                 {malformed_vbi, sizeof(malformed_vbi), sizeof(malformed_vbi), 0x81u},
                 {invalid_utf8, sizeof(invalid_utf8), sizeof(invalid_utf8), 0x82u},
                 {duplicate_topic_alias, sizeof(duplicate_topic_alias),
                  sizeof(duplicate_topic_alias), 0x82u},
                 {disallowed_receive_maximum, sizeof(disallowed_receive_maximum),
                  sizeof(disallowed_receive_maximum), 0x82u},
                 {oversized_header, sizeof(oversized_header), 1u, 0x95u}};
    uint8_t received[3];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow_with_limits(port, 32u, 4096u, 8u);

    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      flowie_test_socket_t client = flowie_test_connect(port);
      const uint8_t expected[] = {0xe0u, 0x01u, cases[i].reason_code};
      info("hostile_case=%zu reason=0x%02x", i, (unsigned int)cases[i].reason_code);
      check_true(client != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
      check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
      check_int_eq(flowie_test_send(client, cases[i].packet, cases[i].first_fragment_size),
                   TURBO_OK);
      if (cases[i].first_fragment_size < cases[i].packet_size) {
        check_int_eq(flowie_test_send(client, cases[i].packet + cases[i].first_fragment_size,
                                      cases[i].packet_size - cases[i].first_fragment_size),
                     TURBO_OK);
      }
      {
        int recv_rc = flowie_test_recv_exact(client, received, sizeof(expected));
        info("hostile_case=%zu terminal_recv=%d", i, recv_rc);
        check_int_eq(recv_rc, TURBO_OK);
      }
      check_mem_eq(received, expected, sizeof(expected));
      check_true(flowie_test_socket_readable(client, 1000u));
      check_int_ne(flowie_test_recv_exact(client, received, 1u), TURBO_OK);
      flowie_test_socket_close(client);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-PROTO-004 rejects zero MQTT 5 CONNECT limits before admitting a session") {
    static const uint8_t zero_receive_maximum[] = {0x10u, 0x13u, 0x00u, 0x04u, 'M',   'Q',   'T',
                                                   'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x03u, 0x21u,
                                                   0x00u, 0x00u, 0x00u, 0x03u, 'z',   'r',   'm'};
    static const uint8_t zero_maximum_packet_size[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
        0x05u, 0x27u, 0x00u, 0x00u, 0x00u, 0x00u, 0x00u, 0x03u, 'z',   'm',   'p'};
    static const struct {
      const uint8_t *packet;
      size_t packet_size;
    } cases[] = {{zero_receive_maximum, sizeof(zero_receive_maximum)},
                 {zero_maximum_packet_size, sizeof(zero_maximum_packet_size)}};
    uint8_t rejected_byte = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      flowie_test_socket_t client = flowie_test_connect(port);
      check_true(client != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(client, cases[i].packet, cases[i].packet_size), TURBO_OK);
      check_true(flowie_test_socket_readable(client, 1000u));
      check_int_ne(flowie_test_recv_exact(client, &rejected_byte, 1u), TURBO_OK);
      flowie_test_socket_close(client);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-OWNER-005 returns quota reasons without disturbing admitted state") {
    static const uint8_t first_connect[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                            0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                            0x00u, 0x3cu, 0x00u, 0x03u, 'o',   'n',   'e'};
    static const uint8_t second_connect[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 't',   'w',   'o'};
    static const uint8_t subscribe_first[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                              0x00u, 0x01u, 'a',   0x00u};
    static const uint8_t subscribe_over_quota[] = {0x82u, 0x07u, 0x00u, 0x02u, 0x00u,
                                                   0x00u, 0x01u, 'b',   0x00u};
    static const uint8_t suback_first[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t suback_quota[] = {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x97u};
    static const uint8_t connack_quota[] = {0x20u, 0x03u, 0x00u, 0x97u, 0x00u};
    static const uint8_t retained_first[] = {0x33u, 0x09u, 0x00u, 0x03u, 'r', '/',
                                             '1',   0x00u, 0x0au, 0x00u, 'x'};
    static const uint8_t retained_over_quota[] = {0x33u, 0x09u, 0x00u, 0x03u, 'r', '/',
                                                  '2',   0x00u, 0x0bu, 0x00u, 'y'};
    static const uint8_t retained_ack[] = {0x40u, 0x02u, 0x00u, 0x0au};
    static const uint8_t retained_quota_ack[] = {0x40u, 0x04u, 0x00u, 0x0bu, 0x97u, 0x00u};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_quota_flow(port);
    flowie_test_socket_t first;
    flowie_test_socket_t rejected;

    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    first = flowie_test_connect(port);
    check_true(first != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(first, first_connect, sizeof(first_connect)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(first, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(first, subscribe_first, sizeof(subscribe_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first, received, sizeof(suback_first)), TURBO_OK);
    check_mem_eq(received, suback_first, sizeof(suback_first));
    check_int_eq(flowie_test_send(first, subscribe_over_quota, sizeof(subscribe_over_quota)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first, received, sizeof(suback_quota)), TURBO_OK);
    check_mem_eq(received, suback_quota, sizeof(suback_quota));
    check_int_eq(flowie_test_send(first, retained_first, sizeof(retained_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first, received, sizeof(retained_ack)), TURBO_OK);
    check_mem_eq(received, retained_ack, sizeof(retained_ack));
    check_int_eq(flowie_test_send(first, retained_over_quota, sizeof(retained_over_quota)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first, received, sizeof(retained_quota_ack)), TURBO_OK);
    check_mem_eq(received, retained_quota_ack, sizeof(retained_quota_ack));

    rejected = flowie_test_connect(port);
    check_true(rejected != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(rejected, second_connect, sizeof(second_connect)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(rejected, received, sizeof(connack_quota)), TURBO_OK);
    check_mem_eq(received, connack_quota, sizeof(connack_quota));
    check_true(flowie_test_socket_readable(rejected, 1000u));
    check_int_ne(flowie_test_recv_exact(rejected, received, 1u), TURBO_OK);

    check_int_eq(flowie_test_send(first, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(first, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(rejected);
    flowie_test_socket_close(first);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-OWNER-005 closes only the transport admitted beyond the connection HWM") {
    static const uint8_t connect_packet[] = {0x10u, 0x12u, 0x00u, 0x04u, 'M',   'Q',   'T',
                                             'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x00u, 0x00u,
                                             0x05u, 'h',   'w',   'm',   '-',   '1'};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_endpoint_capture_t capture = {0};
    turbo_flow_connection_snapshot_t snapshot = {0};
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t admitted;
    flowie_test_socket_t rejected;
    atomic_init(&capture.calls, 0u);
    flow = flowie_connection_hwm_flow(port, &capture);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    admitted = flowie_test_connect(port);
    check_true(admitted != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(admitted, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(admitted, 0u, 0u), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_size_eq(snapshot.connections_current, 1u);

    rejected = flowie_test_connect(port);
    check_true(rejected != FLOWIE_TEST_INVALID_SOCKET);
    (void)flowie_test_send(rejected, connect_packet, sizeof(connect_packet));
    check_true(flowie_test_socket_readable(rejected, 1000u));
    check_int_ne(flowie_test_recv_exact(rejected, received, 1u), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_size_eq(snapshot.connections_current, 1u);

    check_int_eq(flowie_test_send(admitted, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(admitted, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(rejected);
    flowie_test_socket_close(admitted);
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

  it("MQTT-OWNER-001 sends RECEIVED PUBACK before a later graph failure") {
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

  it("MQTT-OWNER-001 withholds PROCESSED PUBACK when the graph fails") {
    check_int_eq(flowie_test_processed_settlement_rejection(TURBO_EIO, "processed-failure"),
                 TURBO_OK);
  }

  it("MQTT-OWNER-001 withholds PROCESSED PUBACK when the graph times out") {
    check_int_eq(flowie_test_processed_settlement_rejection(TURBO_ETIMEDOUT, "processed-timeout"),
                 TURBO_OK);
  }

  it("MQTT-OWNER-009/010 fans wildcard shared and No Local subscriptions") {
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

  it("fans a durable replay from serializable origin without restoring a live route") {
    static const uint8_t connect_packet[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x00u, 0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u,
                                             0x00u, 0x3cu, 0x00u, 0x03u, 's',   'u',   'b'};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x07u, 0x00u, 0x00u,
                                        0x03u, 'a',   '/',   '#',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x07u, 0x00u, 0x00u};
    static const uint8_t publish[] = {0x30u, 0x05u, 0x00u, 0x01u, 'a', 0x00u, 'x'};
    turbo_flow_protocol_origin_t origin = TURBO_FLOW_PROTOCOL_ORIGIN_INIT;
    turbo_flow_msg_t message;
    uint8_t received[sizeof(publish)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_durable_replay_flow(port);
    flowie_test_socket_t subscriber;

    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    turbo_flow_msg_init(&message);
    message.type = FLOWIE_MQTT_PACKET_PUBLISH;
    message.owned_payload = tstr_new_len(publish, sizeof(publish));
    check_not_null(message.owned_payload);
    message.payload = tstr_to_v(message.owned_payload);
    origin.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    origin.protocol_version = TURBO_FLOW_MQTT_PROTOCOL_5_0;
    origin.session_id = 999u;
    check_int_eq(turbo_flow_msg_set_protocol_origin(&message, &origin), TURBO_OK);
    check_int_eq(turbo_flow_publish(flow, "durable_replay", &message), TURBO_OK);
    turbo_flow_msg_cleanup(&message);

    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(publish)), TURBO_OK);
    check_mem_eq(received, publish, sizeof(publish));
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("bridges PUBLISH traffic across MQTT 3.1, MQTT 3.1.1, and MQTT 5 sessions") {
    enum {
      BRIDGE_EXPIRY_INTERVAL_SECONDS = 10u,
      BRIDGE_EXPIRY_VALUE_SIZE = 4u,
      BRIDGE_TRAILING_PAYLOAD_SIZE = 1u,
    };
    static const uint8_t connect_v31[] = {0x10u, 0x11u, 0x00u, 0x06u, 'M',   'Q',   'I',
                                          's',   'd',   'p',   0x03u, 0x02u, 0x00u, 0x3cu,
                                          0x00u, 0x03u, 'v',   '3',   'l'};
    static const uint8_t connect_v311[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u,
                                           0x02u, 0x00u, 0x3cu, 0x00u, 0x03u, 'v', '3', 's'};
    static const uint8_t connect_v5[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',   'T', 'T', 0x05u,
                                         0x02u, 0x00u, 0x3cu, 0x00u, 0x00u, 0x03u, 'v', '5', 'p'};
    static const uint8_t connect_v5_sub[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                             'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                             0x00u, 0x00u, 0x03u, 'v',   '5',   's'};
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
    static const uint8_t subscribe_v5_properties[] = {
        0x82u, 0x0du, 0x00u, 0x03u, 0x00u, 0x00u, 0x07u, 'c', 'r', 'o', 's', 's', '/', '+', 0x01u};
    static const uint8_t suback_v5_properties[] = {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x01u};
    static const uint8_t publish_v5[] = {0x32u, 0x19u,
                                         0x00u, 0x07u,
                                         'c',   'r',
                                         'o',   's',
                                         's',   '/',
                                         'a',   0x00u,
                                         0x2au, 0x0cu,
                                         0x26u, 0x00u,
                                         0x01u, 'k',
                                         0x00u, 0x01u,
                                         'v',   FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
                                         0x00u, 0x00u,
                                         0x00u, BRIDGE_EXPIRY_INTERVAL_SECONDS,
                                         'x'};
    static const uint8_t puback_v5[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t delivery_v311[] = {0x32u, 0x0cu, 0x00u, 0x07u, 'c',   'r',   'o',
                                            's',   's',   '/',   'a',   0x00u, 0x01u, 'x'};
    static const uint8_t delivery_v311_ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t delivery_v31[] = {0x32u, 0x0cu, 0x00u, 0x07u, 'c',   'r',   'o',
                                           's',   's',   '/',   'a',   0x00u, 0x01u, 'x'};
    static const uint8_t delivery_v31_ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t delivery_v5_properties[] = {
        0x32u, 0x19u,
        0x00u, 0x07u,
        'c',   'r',
        'o',   's',
        's',   '/',
        'a',   0x00u,
        0x01u, 0x0cu,
        0x26u, 0x00u,
        0x01u, 'k',
        0x00u, 0x01u,
        'v',   FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
        0x00u, 0x00u,
        0x00u, BRIDGE_EXPIRY_INTERVAL_SECONDS,
        'x'};
    static const uint8_t delivery_v5_properties_ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
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
    flowie_test_socket_t client_v5_sub;
    const size_t delivery_expiry_offset =
        sizeof(delivery_v5_properties) - BRIDGE_EXPIRY_VALUE_SIZE - BRIDGE_TRAILING_PAYLOAD_SIZE;
    uint32_t delivery_expiry_interval;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client_v31 = flowie_test_connect(port);
    client_v311 = flowie_test_connect(port);
    client_v5 = flowie_test_connect(port);
    client_v5_sub = flowie_test_connect(port);
    check_true(client_v31 != FLOWIE_TEST_INVALID_SOCKET);
    check_true(client_v311 != FLOWIE_TEST_INVALID_SOCKET);
    check_true(client_v5 != FLOWIE_TEST_INVALID_SOCKET);
    check_true(client_v5_sub != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client_v31, connect_v31, sizeof(connect_v31)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(connack_v311)), TURBO_OK);
    check_mem_eq(received, connack_v311, sizeof(connack_v311));
    check_int_eq(flowie_test_send(client_v311, connect_v311, sizeof(connect_v311)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(connack_v311)), TURBO_OK);
    check_mem_eq(received, connack_v311, sizeof(connack_v311));
    check_int_eq(flowie_test_send(client_v5, connect_v5, sizeof(connect_v5)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client_v5, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client_v5_sub, connect_v5_sub, sizeof(connect_v5_sub)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client_v5_sub, 0u, 0u), TURBO_OK);

    check_int_eq(flowie_test_send(client_v31, subscribe_v31, sizeof(subscribe_v31)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(suback_v31)), TURBO_OK);
    check_mem_eq(received, suback_v31, sizeof(suback_v31));
    check_int_eq(flowie_test_send(client_v311, subscribe_v311, sizeof(subscribe_v311)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(suback_v311)), TURBO_OK);
    check_mem_eq(received, suback_v311, sizeof(suback_v311));
    check_int_eq(
        flowie_test_send(client_v5_sub, subscribe_v5_properties, sizeof(subscribe_v5_properties)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v5_sub, received, sizeof(suback_v5_properties)),
                 TURBO_OK);
    check_mem_eq(received, suback_v5_properties, sizeof(suback_v5_properties));
    check_int_eq(flowie_test_send(client_v5, publish_v5, sizeof(publish_v5)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client_v5, received, sizeof(puback_v5)), TURBO_OK);
    check_mem_eq(received, puback_v5, sizeof(puback_v5));
    check_int_eq(flowie_test_recv_exact(client_v311, received, sizeof(delivery_v311)), TURBO_OK);
    check_mem_eq(received, delivery_v311, sizeof(delivery_v311));
    check_int_eq(flowie_test_recv_exact(client_v31, received, sizeof(delivery_v31)), TURBO_OK);
    check_mem_eq(received, delivery_v31, sizeof(delivery_v31));
    check_int_eq(flowie_test_recv_exact(client_v5_sub, received, sizeof(delivery_v5_properties)),
                 TURBO_OK);
    check_mem_eq(received, delivery_v5_properties, delivery_expiry_offset);
    delivery_expiry_interval = ((uint32_t)received[delivery_expiry_offset] << 24u) |
                               ((uint32_t)received[delivery_expiry_offset + 1u] << 16u) |
                               ((uint32_t)received[delivery_expiry_offset + 2u] << 8u) |
                               (uint32_t)received[delivery_expiry_offset + 3u];
    info("forwarded_message_expiry_interval=%u", delivery_expiry_interval);
    check_true(delivery_expiry_interval != 0u);
    check_true(delivery_expiry_interval <= BRIDGE_EXPIRY_INTERVAL_SECONDS);
    check_mem_eq(received + delivery_expiry_offset + BRIDGE_EXPIRY_VALUE_SIZE,
                 delivery_v5_properties + delivery_expiry_offset + BRIDGE_EXPIRY_VALUE_SIZE,
                 BRIDGE_TRAILING_PAYLOAD_SIZE);
    check_int_eq(flowie_test_send(client_v31, delivery_v31_ack, sizeof(delivery_v31_ack)),
                 TURBO_OK);
    check_int_eq(flowie_test_send(client_v311, delivery_v311_ack, sizeof(delivery_v311_ack)),
                 TURBO_OK);
    check_int_eq(flowie_test_send(client_v5_sub, delivery_v5_properties_ack,
                                  sizeof(delivery_v5_properties_ack)),
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
    flowie_test_socket_close(client_v5_sub);
    flowie_test_socket_close(client_v311);
    flowie_test_socket_close(client_v31);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-OWNER-004 holds delivery at Receive Maximum until PUBACK advances it") {
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

  it("MQTT-OWNER-004 advances a QoS 2 Receive Maximum window only after PUBREC") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'p',   '2',   '1'};
    static const uint8_t subscriber_connect[] = {0x10u, 0x13u, 0x00u, 0x04u, 'M',   'Q',   'T',
                                                 'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x03u, 0x21u,
                                                 0x00u, 0x01u, 0x00u, 0x03u, 's',   '2',   '1'};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x03u, 'q',   '/',   '#',   0x02u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x02u};
    static const uint8_t publish_first[] = {0x34u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                            'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publish_second[] = {0x34u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x2bu, 0x00u, 'y'};
    static const uint8_t publisher_pubrec_first[] = {0x50u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubrec_second[] = {0x50u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t publisher_pubrel_first[] = {0x62u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubrel_second[] = {0x62u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t publisher_pubcomp_first[] = {0x70u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubcomp_second[] = {0x70u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t delivery_first[] = {0x34u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t delivery_second[] = {0x34u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                              'a',   0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t subscriber_pubrec_first[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubrec_second[] = {0x50u, 0x02u, 0x00u, 0x02u};
    static const uint8_t subscriber_pubrel_first[] = {0x62u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubrel_second[] = {0x62u, 0x02u, 0x00u, 0x02u};
    static const uint8_t subscriber_pubcomp_first[] = {0x70u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubcomp_second[] = {0x70u, 0x02u, 0x00u, 0x02u};
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
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubrec_first)),
                 TURBO_OK);
    check_mem_eq(received, publisher_pubrec_first, sizeof(publisher_pubrec_first));
    check_int_eq(
        flowie_test_send(publisher, publisher_pubrel_first, sizeof(publisher_pubrel_first)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubcomp_first)),
                 TURBO_OK);
    check_mem_eq(received, publisher_pubcomp_first, sizeof(publisher_pubcomp_first));

    check_int_eq(flowie_test_send(publisher, publish_second, sizeof(publish_second)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubrec_second)),
                 TURBO_OK);
    check_mem_eq(received, publisher_pubrec_second, sizeof(publisher_pubrec_second));
    check_int_eq(
        flowie_test_send(publisher, publisher_pubrel_second, sizeof(publisher_pubrel_second)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubcomp_second)),
                 TURBO_OK);
    check_mem_eq(received, publisher_pubcomp_second, sizeof(publisher_pubcomp_second));

    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(delivery_first)), TURBO_OK);
    check_mem_eq(received, delivery_first, sizeof(delivery_first));
    check_false(flowie_test_socket_readable(subscriber, 100u));
    check_int_eq(
        flowie_test_send(subscriber, subscriber_pubrec_first, sizeof(subscriber_pubrec_first)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(delivery_second)), TURBO_OK);
    check_mem_eq(received, delivery_second, sizeof(delivery_second));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(subscriber_pubrel_first)),
                 TURBO_OK);
    check_mem_eq(received, subscriber_pubrel_first, sizeof(subscriber_pubrel_first));
    check_int_eq(
        flowie_test_send(subscriber, subscriber_pubcomp_first, sizeof(subscriber_pubcomp_first)),
        TURBO_OK);
    check_int_eq(
        flowie_test_send(subscriber, subscriber_pubrec_second, sizeof(subscriber_pubrec_second)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(subscriber_pubrel_second)),
                 TURBO_OK);
    check_mem_eq(received, subscriber_pubrel_second, sizeof(subscriber_pubrel_second));
    check_int_eq(
        flowie_test_send(subscriber, subscriber_pubcomp_second, sizeof(subscriber_pubcomp_second)),
        TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber, 50u));

    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-PROTO-004 does not send a delivery above the client Maximum Packet Size") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'm',   'p',   'p'};
    static const uint8_t subscribe[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 'l',
                                        'a',   'r',   'g',   'e',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t publish[] = {0x32u, 0x1cu, 0x00u, 0x07u, 'l', 'a', 'r', 'g', 'e', '/',
                                      'a',   0x00u, 0x2au, 0x00u, '0', '1', '2', '3', '4', '5',
                                      '6',   '7',   '8',   '9',   'a', 'b', 'c', 'd', 'e', 'f'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t maximum_packet_properties[] = {FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE,
                                                        0x00u, 0x00u, 0x00u, 0x14u};
    flowie_mqtt_connect_packet_t subscriber_connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t subscriber_connect_packet[64];
    uint8_t received[sizeof(suback)];
    size_t subscriber_connect_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;
    check_int_gt(port, 0);
    check_not_null(flow);
    subscriber_connect.version = FLOWIE_MQTT_VERSION_5;
    subscriber_connect.clean_start = 1u;
    subscriber_connect.keep_alive = 60u;
    subscriber_connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"mps", 3u};
    subscriber_connect.properties =
        (flowie_mqtt_span_t){maximum_packet_properties, sizeof(maximum_packet_properties)};
    check_int_eq(flowie_mqtt_connect_packet_encode(&subscriber_connect, subscriber_connect_packet,
                                                   sizeof(subscriber_connect_packet),
                                                   &subscriber_connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    subscriber = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect_packet, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_int_eq(flowie_test_send(publisher, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_true(flowie_test_socket_readable(subscriber, 1000u));
    check_int_ne(flowie_test_recv_exact(subscriber, received, 1u), TURBO_OK);
    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("disconnects MQTT 5 when the broker Receive Maximum is exceeded") {
    static const uint8_t connect_packet[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                             'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                             0x00u, 0x00u, 0x03u, 'r',   'x',   'm'};
    static const uint8_t publish_first[] = {0x34u, 0x09u, 0x00u, 0x03u, 'r', '/',
                                            'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t publish_second[] = {0x34u, 0x09u, 0x00u, 0x03u, 'r', '/',
                                             'a',   0x00u, 0x02u, 0x00u, 'y'};
    static const uint8_t pubrec[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t disconnect[] = {0xe0u, 0x01u, 0x93u};
    uint8_t received[sizeof(pubrec)];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow_with_inflight(port, 1u);
    flowie_test_socket_t client;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(client, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(client, publish_first, sizeof(publish_first)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(pubrec)), TURBO_OK);
    check_mem_eq(received, pubrec, sizeof(pubrec));
    check_int_eq(flowie_test_send(client, publish_second, sizeof(publish_second)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(disconnect)), TURBO_OK);
    check_mem_eq(received, disconnect, sizeof(disconnect));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("drops an MQTT 5 delivery that expires while blocked by Receive Maximum") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'p',   'e',   '1'};
    static const uint8_t subscriber_connect[] = {0x10u, 0x13u, 0x00u, 0x04u, 'M',   'Q',   'T',
                                                 'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x03u, 0x21u,
                                                 0x00u, 0x01u, 0x00u, 0x03u, 's',   'e',   '1'};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x03u, 'q',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t publish_first[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                            'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publish_expiring[] = {0x32u,
                                               0x0eu,
                                               0x00u,
                                               0x03u,
                                               'q',
                                               '/',
                                               'a',
                                               0x00u,
                                               0x2bu,
                                               0x05u,
                                               FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
                                               0x00u,
                                               0x00u,
                                               0x00u,
                                               0x01u,
                                               'y'};
    static const uint8_t publisher_ack_first[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_ack_expiring[] = {0x40u, 0x02u, 0x00u, 0x2bu};
    static const uint8_t delivery_first[] = {0x32u, 0x09u, 0x00u, 0x03u, 'q', '/',
                                             'a',   0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t subscriber_ack_first[] = {0x40u, 0x02u, 0x00u, 0x01u};
    uint8_t received[16];
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;
    time_t accepted_at;

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
    check_int_eq(flowie_test_send(publisher, publish_expiring, sizeof(publish_expiring)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_ack_expiring)),
                 TURBO_OK);
    check_mem_eq(received, publisher_ack_expiring, sizeof(publisher_ack_expiring));
    check_false(flowie_test_socket_readable(subscriber, 100u));
    accepted_at = time(NULL);
    check_true(accepted_at >= 0);
    check_int_eq(flowie_test_wait_epoch((uint64_t)accepted_at + 2u), TURBO_OK);

    check_int_eq(flowie_test_send(subscriber, subscriber_ack_first, sizeof(subscriber_ack_first)),
                 TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber, 100u));

    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-OWNER-006 MQTT-STORE-005 replaces deletes and restores retained publications") {
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
    static const uint8_t subscribe_rap1_rh2[] = {
        0x82u, 0x0du, 0x00u, 0x05u, 0x00u, 0x00u, 0x07u, 's', 't', 'a', 't', 'e', '/', '#', 0x28u};
    static const uint8_t suback1[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t suback2[] = {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x00u};
    static const uint8_t suback3[] = {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x00u};
    static const uint8_t suback4[] = {0x90u, 0x04u, 0x00u, 0x04u, 0x00u, 0x00u};
    static const uint8_t suback5[] = {0x90u, 0x04u, 0x00u, 0x05u, 0x00u, 0x00u};
    static const uint8_t retained_update[] = {0x31u, 0x0bu, 0x00u, 0x07u, 's',   't', 'a',
                                              't',   'e',   '/',   'a',   0x00u, 'z'};
    static const uint8_t forwarded_update[] = {0x30u, 0x0bu, 0x00u, 0x07u, 's',   't', 'a',
                                               't',   'e',   '/',   'a',   0x00u, 'z'};
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

    second_subscriber = flowie_test_connect(port);
    check_true(second_subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(second_subscriber, second_subscriber_connect,
                                  sizeof(second_subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(second_subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(
        flowie_test_send(second_subscriber, subscribe_rap1_rh2, sizeof(subscribe_rap1_rh2)),
        TURBO_OK);
    check_int_eq(flowie_test_recv_exact(second_subscriber, received, sizeof(suback5)), TURBO_OK);
    check_mem_eq(received, suback5, sizeof(suback5));
    check_false(flowie_test_socket_readable(second_subscriber, 50u));
    check_int_eq(flowie_test_send(publisher, retained_update, sizeof(retained_update)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(forwarded_update)), TURBO_OK);
    check_mem_eq(received, forwarded_update, sizeof(forwarded_update));
    check_int_eq(flowie_test_recv_exact(second_subscriber, received, sizeof(retained_update)),
                 TURBO_OK);
    check_mem_eq(received, retained_update, sizeof(retained_update));
    flowie_test_socket_close(second_subscriber);
    second_subscriber = FLOWIE_TEST_INVALID_SOCKET;

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

  it("MQTT-OWNER-010 merges QoS and every Subscription Identifier into one delivery") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'i',   'p',   '1'};
    static const uint8_t subscriber_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                 'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                 0x00u, 0x00u, 0x03u, 'i',   's',   '1'};
    static const uint8_t subscribe_wildcard[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x02u,
                                                 0x0bu, 0x07u, 0x00u, 0x05u, 's',
                                                 'i',   'd',   '/',   '#',   0x01u};
    static const uint8_t subscribe_exact[] = {0x82u, 0x0du, 0x00u, 0x02u, 0x02u,
                                              0x0bu, 0x09u, 0x00u, 0x05u, 's',
                                              'i',   'd',   '/',   'a',   0x02u};
    static const uint8_t suback_wildcard[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t suback_exact[] = {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x02u};
    static const uint8_t publish[] = {0x34u, 0x0bu, 0x00u, 0x05u, 's',   'i', 'd',
                                      '/',   'a',   0x00u, 0x2au, 0x00u, 'x'};
    static const uint8_t publisher_pubrec[] = {0x50u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubrel[] = {0x62u, 0x02u, 0x00u, 0x2au};
    static const uint8_t publisher_pubcomp[] = {0x70u, 0x02u, 0x00u, 0x2au};
    static const uint8_t subscriber_pubrec[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubrel[] = {0x62u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscriber_pubcomp[] = {0x70u, 0x02u, 0x00u, 0x01u};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t delivery = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    uint32_t identifiers[2] = {0u, 0u};
    uint8_t received[17];
    size_t consumed = 0u;
    size_t identifier_count = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;
    int rc;

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
    check_int_eq(flowie_test_send(subscriber, subscribe_wildcard, sizeof(subscribe_wildcard)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback_wildcard)), TURBO_OK);
    check_mem_eq(received, suback_wildcard, sizeof(suback_wildcard));
    check_int_eq(flowie_test_send(subscriber, subscribe_exact, sizeof(subscribe_exact)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback_exact)), TURBO_OK);
    check_mem_eq(received, suback_exact, sizeof(suback_exact));

    check_int_eq(flowie_test_send(publisher, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubrec)), TURBO_OK);
    check_mem_eq(received, publisher_pubrec, sizeof(publisher_pubrec));
    check_int_eq(flowie_test_send(publisher, publisher_pubrel, sizeof(publisher_pubrel)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_pubcomp)), TURBO_OK);
    check_mem_eq(received, publisher_pubcomp, sizeof(publisher_pubcomp));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(received)), TURBO_OK);
    options.version = FLOWIE_MQTT_VERSION_5;
    options.max_packet_size = sizeof(received);
    check_int_eq(
        flowie_mqtt_packet_parse(received, sizeof(received), &options, &packet, &consumed, NULL),
        FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(received));
    check_int_eq(flowie_mqtt_publish_parse(&packet, &delivery), FLOWIE_MQTT_PARSE_OK);
    check_uint_eq(delivery.qos, 2u);
    check_uint_eq(delivery.packet_id, 1u);
    check_size_eq(delivery.payload.size, 1u);
    check_uint_eq(delivery.payload.data[0], (uint8_t)'x');
    check_int_eq(flowie_mqtt_property_iterator_init(&delivery.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) ==
           FLOWIE_MQTT_PARSE_OK) {
      check_uint_eq(property.identifier, FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER);
      check_size_lt(identifier_count, 2u);
      if (identifier_count < 2u) identifiers[identifier_count] = property.integer;
      ++identifier_count;
    }
    check_int_eq(rc, FLOWIE_MQTT_PARSE_NEED_MORE);
    check_size_eq(identifier_count, 2u);
    check_true((identifiers[0] == 7u && identifiers[1] == 9u) ||
               (identifiers[0] == 9u && identifiers[1] == 7u));
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

  it("MQTT-STORE-004 replays broker-owned QoS 2 across both acknowledgement stages") {
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
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(subscriber_pubrel)), TURBO_OK);
    check_mem_eq(received, subscriber_pubrel, sizeof(subscriber_pubrel));
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

  it("MQTT-OWNER-011 expires offline delivery and decrements its replay interval") {
    static const uint8_t publisher_connect[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                                'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                                0x00u, 0x00u, 0x03u, 'e',   'p',   '1'};
    static const uint8_t subscriber_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'e',   's',   '1'};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x03u, 'e',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t expiring_publish[] = {0x32u,
                                               0x0eu,
                                               0x00u,
                                               0x03u,
                                               'e',
                                               '/',
                                               'a',
                                               0x00u,
                                               0x2au,
                                               0x05u,
                                               FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
                                               0x00u,
                                               0x00u,
                                               0x00u,
                                               0x01u,
                                               'x'};
    static const uint8_t live_publish[] = {0x32u,
                                           0x0eu,
                                           0x00u,
                                           0x03u,
                                           'e',
                                           '/',
                                           'a',
                                           0x00u,
                                           0x2bu,
                                           0x05u,
                                           FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
                                           0x00u,
                                           0x00u,
                                           0x00u,
                                           0x05u,
                                           'y'};
    static const uint8_t expiring_puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
    static const uint8_t live_puback[] = {0x40u, 0x02u, 0x00u, 0x2bu};
    turbo_flow_connection_snapshot_t snapshot = {0};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t delivery = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    uint8_t received[sizeof(live_publish)];
    size_t consumed = 0u;
    uint32_t replay_interval = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_fanout_flow(port);
    flowie_test_socket_t publisher;
    flowie_test_socket_t subscriber;
    time_t accepted_at;
    int rc;

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

    flowie_test_socket_close(subscriber);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 1u);
    check_int_eq(flowie_test_send(publisher, expiring_publish, sizeof(expiring_publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(expiring_puback)), TURBO_OK);
    check_mem_eq(received, expiring_puback, sizeof(expiring_puback));
    accepted_at = time(NULL);
    check_true(accepted_at >= 0);
    check_int_eq(flowie_test_wait_epoch((uint64_t)accepted_at + 2u), TURBO_OK);

    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 1u, 0u), TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber, 100u));

    flowie_test_socket_close(subscriber);
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 1u);
    check_int_eq(flowie_test_send(publisher, live_publish, sizeof(live_publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(live_puback)), TURBO_OK);
    check_mem_eq(received, live_puback, sizeof(live_puback));
    accepted_at = time(NULL);
    check_true(accepted_at >= 0);
    check_int_eq(flowie_test_wait_epoch((uint64_t)accepted_at + 2u), TURBO_OK);

    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 1u, 0u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(live_publish)), TURBO_OK);
    options.version = FLOWIE_MQTT_VERSION_5;
    options.max_packet_size = sizeof(received);
    check_int_eq(
        flowie_mqtt_packet_parse(received, sizeof(received), &options, &packet, &consumed, NULL),
        FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(received));
    check_int_eq(flowie_mqtt_publish_parse(&packet, &delivery), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_init(&delivery.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) ==
           FLOWIE_MQTT_PARSE_OK) {
      if (property.identifier == FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL)
        replay_interval = property.integer;
    }
    check_int_eq(rc, FLOWIE_MQTT_PARSE_NEED_MORE);
    check_true(replay_interval > 0u);
    check_true(replay_interval < 5u);

    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("MQTT-OWNER-008 publishes abnormal and requested Wills through the graph") {
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x10u, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x0au, 'w',   'i',   'l',   'l',   '/',
                                        't',   'o',   'p',   'i',   'c',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t normal_disconnect[] = {0xe0u, 0x00u};
    static const uint8_t requested_disconnect[] = {0xe0u, 0x01u, 0x04u};
    static const uint8_t takeover_disconnect[] = {0xe0u, 0x01u, 0x8eu};
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_endpoint_capture_t capture;
    uint8_t subscriber_connect[128];
    uint8_t abnormal_connect[128];
    uint8_t normal_connect[128];
    uint8_t takeover_connect[128];
    uint8_t replacement_connect[128];
    uint8_t requested_connect[128];
    uint8_t expected_abnormal[64];
    uint8_t expected_requested[64];
    uint8_t received[64];
    size_t subscriber_connect_size = 0u;
    size_t abnormal_connect_size = 0u;
    size_t normal_connect_size = 0u;
    size_t takeover_connect_size = 0u;
    size_t replacement_connect_size = 0u;
    size_t requested_connect_size = 0u;
    size_t expected_abnormal_size = 0u;
    size_t expected_requested_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow;
    flowie_test_socket_t subscriber;
    flowie_test_socket_t abnormal;
    flowie_test_socket_t normal;
    flowie_test_socket_t takeover;
    flowie_test_socket_t replacement;
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
    check_int_eq(flowie_test_encode_connect(takeover_connect, sizeof(takeover_connect),
                                            &takeover_connect_size, "will-takeover", 60u,
                                            "will/topic", "takeover-suppressed", 0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(replacement_connect, sizeof(replacement_connect),
                                            &replacement_connect_size, "will-takeover", 60u, NULL,
                                            NULL, 0u),
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

    takeover = flowie_test_connect(port);
    check_true(takeover != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(takeover, takeover_connect, takeover_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(takeover, 0u, 0u), TURBO_OK);
    replacement = flowie_test_connect(port);
    check_true(replacement != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(replacement, replacement_connect, replacement_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(replacement, 1u, 0u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(takeover, received, sizeof(takeover_disconnect)), TURBO_OK);
    check_mem_eq(received, takeover_disconnect, sizeof(takeover_disconnect));
    turbo_sleep_ms(100u);
    check_size_eq(atomic_load_explicit(&capture.calls, memory_order_acquire), 1u);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    check_int_eq(flowie_test_send(replacement, normal_disconnect, sizeof(normal_disconnect)),
                 TURBO_OK);
    flowie_test_socket_close(replacement);
    flowie_test_socket_close(takeover);

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

  it("MQTT-OWNER-008 resolves delayed Will reconnect and session-expiry races") {
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

  it("MQTT-OWNER-009 isolates a stalled shared fan-out subscriber at its HWM") {
    enum {
      STALLED_PAYLOAD_BYTES = 1024u,
      STALLED_PACKET_CAPACITY = 2048u,
      STALLED_SEND_HWM_BYTES = STALLED_PAYLOAD_BYTES + 8u,
      HEALTHY_MESSAGES_AFTER_ISOLATION = 2u
    };
    static const uint8_t connect_template[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                               'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                               0x00u, 0x00u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t slow_subscribe[] = {
        0x82u, 0x18u, 0x00u, 0x01u, 0x02u, 0x0bu, 0x01u, 0x00u, 0x10u, '$', 's', 'h', 'a',
        'r',   'e',   '/',   'w',   'o',   'r',   'k',   'e',   'r',   's', '/', '#', 0x00u};
    static const uint8_t subscribe[] = {0x82u, 0x16u, 0x00u, 0x01u, 0x00u, 0x00u, 0x10u, '$',
                                        's',   'h',   'a',   'r',   'e',   '/',   'w',   'o',
                                        'r',   'k',   'e',   'r',   's',   '/',   '#',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    uint8_t connects[4][sizeof(connect_template)];
    uint8_t reply[8];
    uint8_t *payload = (uint8_t *)calloc(STALLED_PAYLOAD_BYTES, 1u);
    uint8_t *wire = (uint8_t *)malloc(STALLED_PACKET_CAPACITY);
    uint8_t *received = (uint8_t *)malloc(STALLED_PACKET_CAPACITY);
    size_t wire_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_resource_document_t queue_status = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    tstr_t queue_json = NULL;
    size_t fast_a_deliveries = 0u;
    size_t fast_b_deliveries = 0u;
    turbo_flow_t *flow =
        flowie_fanout_flow_with_limits(port, STALLED_PACKET_CAPACITY, STALLED_SEND_HWM_BYTES, 8u);
    flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t slow = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t fast_a = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t fast_b = FLOWIE_TEST_INVALID_SOCKET;
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
    check_size_eq(wire_size, STALLED_SEND_HWM_BYTES - 1u);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    slow = flowie_test_connect(port);
    fast_a = flowie_test_connect(port);
    fast_b = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(slow != FLOWIE_TEST_INVALID_SOCKET);
    check_true(fast_a != FLOWIE_TEST_INVALID_SOCKET);
    check_true(fast_b != FLOWIE_TEST_INVALID_SOCKET);
    for (size_t i = 0u; i < 4u; ++i) {
      static const char *const client_ids[] = {"pub", "slw", "fa1", "fb2"};
      flowie_test_socket_t clients[] = {publisher, slow, fast_a, fast_b};
      memcpy(connects[i], connect_template, sizeof(connect_template));
      memcpy(connects[i] + sizeof(connect_template) - 3u, client_ids[i], 3u);
      check_int_eq(flowie_test_send(clients[i], connects[i], sizeof(connects[i])), TURBO_OK);
      check_int_eq(flowie_test_recv_connack(clients[i], 0u, 0u), TURBO_OK);
    }
    check_int_eq(flowie_test_send(slow, slow_subscribe, sizeof(slow_subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(slow, reply, sizeof(suback)), TURBO_OK);
    check_mem_eq(reply, suback, sizeof(suback));
    check_int_eq(flowie_test_send(fast_a, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast_a, reply, sizeof(suback)), TURBO_OK);
    check_mem_eq(reply, suback, sizeof(suback));
    check_int_eq(flowie_test_send(fast_b, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast_b, reply, sizeof(suback)), TURBO_OK);
    check_mem_eq(reply, suback, sizeof(suback));

    /* The selector first chooses the lowest session id. Its Subscription Identifier adds
     * two wire bytes, placing only that delivery above the connection-local HWM. */
    check_int_eq(flowie_test_send(publisher, wire, wire_size), TURBO_OK);
    check_false(flowie_test_socket_readable(fast_a, 50u));
    check_false(flowie_test_socket_readable(fast_b, 50u));
    for (size_t i = 0u; i < FLOWIE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 3u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 3u);
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

    for (size_t i = 0u; i < HEALTHY_MESSAGES_AFTER_ISOLATION; ++i) {
      flowie_test_socket_t selected;
      int fast_a_ready = 0;
      int fast_b_ready = 0;
      wire[wire_size - 1u] = (uint8_t)(i + 1u);
      check_int_eq(flowie_test_send(publisher, wire, wire_size), TURBO_OK);
      for (size_t wait = 0u; wait < FLOWIE_TEST_WAIT_STEPS; ++wait) {
        fast_a_ready = flowie_test_socket_readable(fast_a, 1u);
        fast_b_ready = flowie_test_socket_readable(fast_b, 1u);
        if (fast_a_ready || fast_b_ready) break;
      }
      check_true(fast_a_ready != fast_b_ready);
      selected = fast_a_ready ? fast_a : fast_b;
      fast_a_deliveries += fast_a_ready ? 1u : 0u;
      fast_b_deliveries += fast_b_ready ? 1u : 0u;
      check_int_eq(flowie_test_recv_exact(selected, received, wire_size), TURBO_OK);
      check_mem_eq(received, wire, wire_size);
    }
    check_size_eq(fast_a_deliveries, 1u);
    check_size_eq(fast_b_deliveries, 1u);
    check_int_eq(flowie_test_send(fast_a, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast_a, reply, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(reply, pingresp, sizeof(pingresp));
    check_int_eq(flowie_test_send(fast_b, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(fast_b, reply, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(reply, pingresp, sizeof(pingresp));
    flowie_test_socket_close(fast_b);
    flowie_test_socket_close(fast_a);
    flowie_test_socket_close(slow);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    free(received);
    free(wire);
    free(payload);
  }

  it("MQTT-OWNER-005 isolates the subscriber whose inflight quota is exhausted") {
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

  it("MQTT-OWNER-001/005 closes only the connection whose ACK exceeds its HWM") {
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

  it("MQTT-OWNER-003 MQTT-NET-004 fences a pending send and releases it during shutdown") {
    enum {
      PENDING_SEND_PAYLOAD_BYTES = 4u * 1024u * 1024u,
      PENDING_SEND_PACKET_CAPACITY = PENDING_SEND_PAYLOAD_BYTES + 256u,
      PENDING_SEND_HWM_BYTES = 64u * 1024u * 1024u,
      PENDING_SEND_MESSAGES = 8u,
      PENDING_SEND_WAIT_STEPS = 5000u,
      PENDING_SEND_CONTROL_HEADROOM = 256u,
    };
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, '#',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t takeover_publish[] = {0x30u, 0x0fu, 0x00u, 0x0bu, 't', 'a',
                                               'k',   'e',   'o',   'v',   'e', 'r',
                                               '/',   'o',   'k',   0x00u, 'x'};
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    uint8_t publisher_connect[128];
    uint8_t subscriber_connect[128];
    uint8_t received[sizeof(suback)];
    uint8_t *payload = (uint8_t *)calloc(PENDING_SEND_PAYLOAD_BYTES, 1u);
    uint8_t *wire = (uint8_t *)malloc(PENDING_SEND_PACKET_CAPACITY);
    size_t publisher_connect_size = 0u;
    size_t subscriber_connect_size = 0u;
    size_t wire_size = 0u;
    size_t received_size = 0u;
    int takeover_seen = 0;
    uint64_t started_at;
    uint64_t elapsed;
    unsigned short port = flowie_test_port();
    turbo_flow_connection_snapshot_t snapshot = {0};
    turbo_flow_resource_snapshot_t queue_snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    turbo_flow_resource_snapshot_t session_snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    turbo_flow_t *flow = flowie_fanout_flow_with_limits(port, PENDING_SEND_PACKET_CAPACITY,
                                                        PENDING_SEND_HWM_BYTES, 8u);
    flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t replacement = FLOWIE_TEST_INVALID_SOCKET;
    check_int_gt(port, 0);
    check_not_null(payload);
    check_not_null(wire);
    check_not_null(flow);
    check_int_eq(flowie_test_encode_connect(publisher_connect, sizeof(publisher_connect),
                                            &publisher_connect_size, "pending-send-publisher", 0u,
                                            NULL, NULL, 0u),
                 TURBO_OK);
    check_int_eq(flowie_test_encode_connect(subscriber_connect, sizeof(subscriber_connect),
                                            &subscriber_connect_size, "pending-send-subscriber",
                                            60u, NULL, NULL, 0u),
                 TURBO_OK);
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = (flowie_mqtt_span_t){(const uint8_t *)"shutdown/pending-send",
                                         sizeof("shutdown/pending-send") - 1u};
    publish.payload = (flowie_mqtt_span_t){payload, PENDING_SEND_PAYLOAD_BYTES};
    check_int_eq(
        flowie_mqtt_publish_packet_encode(&publish, wire, PENDING_SEND_PACKET_CAPACITY, &wire_size),
        FLOWIE_MQTT_PARSE_OK);
    check_size_lt(wire_size, PENDING_SEND_HWM_BYTES);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    subscriber = flowie_test_connect_with_recv_buffer(port, 1024u);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, publisher_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_connack(publisher, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(subscriber, 0u, 0u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    for (size_t i = 0u; i < PENDING_SEND_MESSAGES; ++i) {
      wire[wire_size - 1u] = (uint8_t)i;
      check_int_eq(flowie_test_send(publisher, wire, wire_size), TURBO_OK);
    }
    for (size_t i = 0u; i < PENDING_SEND_WAIT_STEPS; ++i) {
      queue_snapshot = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
      check_int_eq(turbo_flow_resource_snapshot_at(flow, 1u, &queue_snapshot), TURBO_OK);
      if (queue_snapshot.load >= wire_size) break;
      turbo_sleep_ms(1u);
    }
    info("pending_queue_load=%zu wire_size=%zu", queue_snapshot.load, wire_size);
    check_size_ge(queue_snapshot.load, wire_size);
    check_size_le(queue_snapshot.load,
                  wire_size * PENDING_SEND_MESSAGES + PENDING_SEND_CONTROL_HEADROOM);
    replacement = flowie_test_connect(port);
    check_true(replacement != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(replacement, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_connack(replacement, 1u, 0u), TURBO_OK);
    for (size_t i = 0u; i < PENDING_SEND_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      if (snapshot.connections_current == 2u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(snapshot.connections_current, 2u);
    check_int_eq(flowie_test_send(publisher, takeover_publish, sizeof(takeover_publish)), TURBO_OK);
    for (size_t i = 0u; i <= PENDING_SEND_MESSAGES; ++i) {
      int readable = flowie_test_socket_readable(replacement, 4000u);
      int recv_rc =
          flowie_test_recv_packet(replacement, wire, PENDING_SEND_PACKET_CAPACITY, &received_size);
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      info("pending_recv=%zu readable=%d status=%d size=%zu type=%u connections=%zu", i, readable,
           recv_rc, received_size, recv_rc == TURBO_OK ? (unsigned int)(wire[0] >> 4u) : 0u,
           snapshot.connections_current);
      check_true(readable);
      check_int_eq(recv_rc, TURBO_OK);
      if (received_size == sizeof(takeover_publish) &&
          memcmp(wire, takeover_publish, sizeof(takeover_publish)) == 0) {
        takeover_seen = 1;
        break;
      }
      check_size_eq(received_size, wire_size);
      check_uint_eq(wire[0] >> 4u, FLOWIE_MQTT_PACKET_PUBLISH);
    }
    check_true(takeover_seen);
    check_int_eq(flowie_test_send(replacement, pingreq, sizeof(pingreq)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(replacement, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    started_at = turbo_monotonic_ms();
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    elapsed = turbo_monotonic_ms() - started_at;
    check_size_le((size_t)elapsed, (size_t)FLOWIE_TEST_STOP_MAX_MS);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_size_eq(snapshot.connections_current, 0u);
    check_size_eq(snapshot.in_flight_messages, 0u);
    check_size_eq(snapshot.in_flight_bytes, 0u);
    queue_snapshot = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 1u, &queue_snapshot), TURBO_OK);
    check_size_eq(queue_snapshot.load, 0u);
    check_int_eq(turbo_flow_resource_snapshot_at(flow, 2u, &session_snapshot), TURBO_OK);
    check_size_eq(session_snapshot.load, 1u);
    flowie_test_socket_close(replacement);
    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    turbo_flow_destroy(flow);
    free(wire);
    free(payload);
  }
}
