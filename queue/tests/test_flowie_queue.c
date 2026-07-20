#include "flowie.h"
#include "flowie_rule_internal.h"
#include "flowie_test_socket.h"
#include "queue_test_paths.h"
#include "socket.h"
#include "turbo_flow_policy.h"
#include "turbo_flow_queue.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_QUEUE_TEST_WAIT_STEPS 2000u
#define FLOWIE_QUEUE_TEST_YAML_CAPACITY 4096u
#define FLOWIE_QUEUE_RULE_TRANSFORM_STATUS 73

typedef struct flowie_queue_capture_s {
  atomic_size_t calls;
  int has_route;
  int has_settlement;
  uint8_t packet[32];
  size_t packet_size;
} flowie_queue_capture_t;

typedef struct flowie_queue_rule_probe_s {
  atomic_size_t calls;
  atomic_size_t transformed;
} flowie_queue_rule_probe_t;

static const turbo_flow_expr_schema_field_t FLOWIE_QUEUE_RULE_FIELDS[] = {
    {"mqtt.topic", TURBO_FLOW_EXPR_TYPE_STRING, 1u}};

static const turbo_flow_expr_schema_t FLOWIE_QUEUE_RULE_SCHEMA = {
    FLOWIE_QUEUE_RULE_FIELDS,
    sizeof(FLOWIE_QUEUE_RULE_FIELDS) / sizeof(FLOWIE_QUEUE_RULE_FIELDS[0])};

static int flowie_queue_rule_facts(const turbo_flow_msg_t *message,
                                   const turbo_flow_expr_schema_t *schema,
                                   const turbo_flow_expr_value_t **values_out,
                                   size_t *value_count_out, void *ctx) {
  flowie_queue_rule_probe_t *probe = (flowie_queue_rule_probe_t *)ctx;
  int rc;
  if (!probe) return TURBO_EINVAL;
  rc = flowie_mqtt_rule_facts_provider(message, schema, values_out, value_count_out, NULL);
  if (rc != TURBO_OK) return rc;
  atomic_fetch_add_explicit(&probe->calls, 1u, memory_order_relaxed);
  return TURBO_OK;
}

static turbo_flow_rule_action_t flowie_queue_route_action(const char *route) {
  turbo_flow_rule_action_t action = TURBO_FLOW_RULE_ACTION_INIT;
  size_t len = route ? strlen(route) : 0u;
  action.kind = TURBO_FLOW_RULE_ACTION_ROUTE;
  if (!route || len > TURBO_FLOW_RULE_KEY_MAX) {
    action.kind = 0;
    return action;
  }
  memcpy(action.key, route, len + 1u);
  return action;
}

static turbo_flow_rule_action_t flowie_queue_status_action(int status) {
  turbo_flow_rule_action_t action = TURBO_FLOW_RULE_ACTION_INIT;
  action.kind = TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE;
  action.private_field = TURBO_FLOW_RULE_PRIVATE_MSG_STATUS;
  action.status = status;
  return action;
}

static int flowie_queue_rule_transform_probe(turbo_flow_msg_t *message, void *ctx) {
  flowie_queue_rule_probe_t *probe = (flowie_queue_rule_probe_t *)ctx;
  if (!message || !probe || message->status != FLOWIE_QUEUE_RULE_TRANSFORM_STATUS)
    return TURBO_EPROTO;
  atomic_fetch_add_explicit(&probe->transformed, 1u, memory_order_relaxed);
  return TURBO_OK;
}

static flowie_test_socket_t flowie_queue_test_listen(unsigned short *port) {
  struct sockaddr_in address;
  flowie_test_socket_t socket_handle = FLOWIE_TEST_INVALID_SOCKET;
#ifdef _WIN32
  int address_size = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return FLOWIE_TEST_INVALID_SOCKET;
#else
  socklen_t address_size = (socklen_t)sizeof(address);
#endif
  if (!port) return FLOWIE_TEST_INVALID_SOCKET;
  *port = 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return FLOWIE_TEST_INVALID_SOCKET;
  if (bind(socket_handle, (struct sockaddr *)&address, sizeof(address)) != 0 ||
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) != 0 ||
      listen(socket_handle, 1) != 0) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
  *port = ntohs(address.sin_port);
  return socket_handle;
}

static int flowie_queue_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  flowie_queue_capture_t *capture = (flowie_queue_capture_t *)ctx;
  if (!msg || !capture || msg->payload.len > sizeof(capture->packet)) return TURBO_EMSGSIZE;
  capture->has_route = turbo_flow_msg_protocol_route(msg) != NULL;
  capture->has_settlement = turbo_flow_msg_protocol_settlement(msg) != NULL;
  capture->packet_size = msg->payload.len;
  if (msg->payload.len != 0u) memcpy(capture->packet, msg->payload.data, msg->payload.len);
  atomic_store_explicit(&capture->calls, 1u, memory_order_release);
  return TURBO_OK;
}

static int flowie_queue_wait(flowie_queue_capture_t *capture, turbo_flow_queue_t *queue) {
  for (size_t i = 0u; i < FLOWIE_QUEUE_TEST_WAIT_STEPS; ++i) {
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    if (atomic_load_explicit(&capture->calls, memory_order_acquire) == 1u &&
        turbo_flow_queue_snapshot(queue, &snapshot) == TURBO_OK && snapshot.delivered == 1u)
      return TURBO_OK;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_queue_start_bound_endpoint(
    const turbo_flow_resolved_config_t *resolved, const flowie_endpoint_bindings_t *bindings,
    const char *graph, turbo_flow_t **flow_out) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_t *flow;
  int rc;
  if (flow_out) *flow_out = NULL;
  if (!resolved || !bindings || !graph || !flow_out) return TURBO_EINVAL;
  flow = turbo_flow_create();
  if (!flow) return TURBO_ENOMEM;
  rc = flowie_register_resolved_bound_endpoint(flow, "mqtt.endpoint", resolved, bindings, &error);
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(flow, graph, strlen(graph));
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) {
    turbo_flow_destroy(flow);
    return rc;
  }
  *flow_out = flow;
  return TURBO_OK;
}

static int flowie_queue_encode_connect(uint8_t *output, size_t capacity, size_t *written,
                                       const char *client_id, uint32_t session_expiry,
                                       const char *will_topic, const char *will_payload,
                                       uint32_t will_delay) {
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  uint8_t properties[5];
  uint8_t will_properties[5];
  if (!output || !written || !client_id) return TURBO_EINVAL;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.keep_alive = 60u;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
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
    connect.will_topic =
        (flowie_mqtt_span_t){(const uint8_t *)will_topic, strlen(will_topic)};
    connect.will_payload = (flowie_mqtt_span_t){
        (const uint8_t *)will_payload, will_payload ? strlen(will_payload) : 0u};
    if (will_delay != 0u) {
      will_properties[0] = FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL;
      will_properties[1] = (uint8_t)(will_delay >> 24u);
      will_properties[2] = (uint8_t)(will_delay >> 16u);
      will_properties[3] = (uint8_t)(will_delay >> 8u);
      will_properties[4] = (uint8_t)will_delay;
      connect.will_properties =
          (flowie_mqtt_span_t){will_properties, sizeof(will_properties)};
    }
  }
  return flowie_mqtt_connect_packet_encode(&connect, output, capacity, written) ==
                 FLOWIE_MQTT_PARSE_OK
             ? TURBO_OK
             : TURBO_EPROTO;
}

spec("Flowie memory Queue settlement composition") {
  it("keeps the Flowie YAML ACCEPTED resources resolvable and registrable") {
    char path[1024];
    char *yaml;
    size_t yaml_size = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_queue_t *queue = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_gt(snprintf(path, sizeof(path), "%s/examples/flowie.yml", FLOWIE_SOURCE_DIR), 0);
    yaml = tt_read_file(path, &yaml_size);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_size, &resolved, &error), TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "mqtt.accepted", &queue, &error),
                 TURBO_OK);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_register_resolved_adapter(flow, "queue.mqtt.accept", resolved,
                                                            queue, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_register_resolved_adapter(
                     flow, "queue.mqtt.accepted", resolved, queue, &error),
                 TURBO_OK);
    check_size_eq(turbo_flow_adapter_count(flow), 3u);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    free(yaml);
  }

  it("ACKs ACCEPTED only after the configured Queue commit and republishes downstream") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage accept adapter queue.accept\n"
                                "source accepted adapter queue.accepted\n"
                                "stage capture\n"
                                "stage main {\n"
                                "  mqtt_in -> accept\n"
                                "  accepted -> capture\n"
                                "}\n";
    static const uint8_t connect_packet[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T', 0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'a',   'c',   'c'};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                      0x00u, 0x35u, 0x00u, 'q'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x35u};
    char yaml[FLOWIE_QUEUE_TEST_YAML_CAPACITY];
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_queue_ack_snapshot_t acknowledgements = TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
    turbo_flow_queue_t *queue = NULL;
    turbo_flow_t *flow = NULL;
    flowie_queue_capture_t capture;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    int yaml_size;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.calls, 0u);
    check_int_gt(port, 0);
    yaml_size = snprintf(
        yaml, sizeof(yaml),
        "version: 1\n"
        "channels:\n"
        "  mqtt.accepted:\n"
        "    kind: queue\n"
        "    config:\n"
        "      backend: memory\n"
        "      pattern: push_pull\n"
        "      resource_uid: queue:mqtt.accepted\n"
        "      owner_name: mqtt-accepted\n"
        "      capacity: 8\n"
        "      max_payload_size: 1024\n"
        "      full_policy: fail\n"
        "adapters:\n"
        "  mqtt.endpoint:\n"
        "    kind: flowie_endpoint\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: %hu\n"
        "      max_connections: 4\n"
        "      manage_sessions: true\n"
        "      settlement_qos1: accepted\n"
        "      settlement_qos2: accepted\n"
        "      max_sessions: 4\n"
        "      max_subscriptions_per_session: 8\n"
        "      max_inflight_per_session: 8\n"
        "  queue.accept:\n"
        "    kind: queue\n"
        "    config:\n"
        "      channel: mqtt.accepted\n"
        "      role: sink\n"
        "  queue.accepted:\n"
        "    kind: queue\n"
        "    config:\n"
        "      channel: mqtt.accepted\n"
        "      role: source\n",
        port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "mqtt.accepted", &queue, &error),
                 TURBO_OK);
    check_not_null(queue);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_register_resolved_adapter(flow, "queue.accept", resolved, queue,
                                                            &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_register_resolved_adapter(
                     flow, "queue.accepted", resolved, queue, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", flowie_queue_capture_stage, &capture,
                                              NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 0u, 8u, FLOWIE_DEFAULT_MAX_PACKET_SIZE),
                 TURBO_OK);
    check_int_eq(flowie_test_send(client, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    check_int_eq(flowie_queue_wait(&capture, queue), TURBO_OK);
    check_true(capture.has_route);
    check_false(capture.has_settlement);
    check_size_eq(capture.packet_size, sizeof(publish));
    check_mem_eq(capture.packet, publish, sizeof(publish));
    check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
    check_size_eq(snapshot.depth, 0u);
    check_uint_eq(snapshot.enqueued, 1u);
    check_uint_eq(snapshot.delivered, 1u);
    check_int_eq(turbo_flow_queue_ack_snapshot(queue, &acknowledgements), TURBO_OK);
    check_uint_eq(acknowledgements.accept_acks, 1u);
    check_uint_eq(acknowledgements.delivery_acks, 1u);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("routes Queue-owned MQTT PUBLISH packets through RulesForge to endpoint or socket output") {
    static const char graph[] =
        "source mqtt_in adapter mqtt.endpoint\n"
        "stage accept adapter queue.accept\n"
        "source accepted adapter queue.accepted\n"
        "stage rules_route operation rules.apply resource rules.mqtt\n"
        "stage mqtt_fanout adapter mqtt.endpoint\n"
        "stage transformed_output\n"
        "stage application_output adapter socket.output\n"
        "stage main {\n"
        "  mqtt_in -> accept\n"
        "  accepted -> rules_route -> [mqtt_fanout, transformed_output]\n"
        "  transformed_output -> application_output\n"
        "}\n";
    static const uint8_t publisher_connect[] = {
        0x10u, 0x10u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T', 0x05u,
        0x02u, 0x00u, 0x3cu, 0x00u, 0x00u, 0x03u, 'p', 'u', 'b'};
    static const uint8_t subscriber_connect[] = {
        0x10u, 0x10u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T', 0x05u,
        0x02u, 0x00u, 0x3cu, 0x00u, 0x00u, 0x03u, 's', 'u', 'b'};
    static const uint8_t subscribe[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u,
                                        0x07u, 'l',   'o',   'c',   'a',   'l',
                                        '/',   '#',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t local_publish[] = {0x32u, 0x0du, 0x00u, 0x07u, 'l', 'o', 'c', 'a',
                                            'l',   '/',   'a',   0x00u, 0x11u, 0x00u, 'x'};
    static const uint8_t local_puback[] = {0x40u, 0x02u, 0x00u, 0x11u};
    static const uint8_t local_delivery[] = {0x30u, 0x0bu, 0x00u, 0x07u, 'l', 'o', 'c', 'a',
                                             'l',   '/',   'a',   0x00u, 'x'};
    static const uint8_t egress_publish[] = {0x32u, 0x0eu, 0x00u, 0x08u, 'e', 'g', 'r', 'e',
                                             's',   's',   '/',   'a',   0x00u, 0x12u, 0x00u, 'y'};
    static const uint8_t egress_puback[] = {0x40u, 0x02u, 0x00u, 0x12u};
    const turbo_flow_rule_t rules[] = {
        {"mqtt.topic == \"egress/a\"", 0u,
         flowie_queue_status_action(FLOWIE_QUEUE_RULE_TRANSFORM_STATUS)},
        {"mqtt.topic == \"egress/a\"", 0u, flowie_queue_route_action("transformed_output")},
        {"mqtt.topic != \"egress/a\"", 0u, flowie_queue_route_action("mqtt_fanout")}};
    turbo_flow_rule_processor_config_t rule_config = TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT;
    turbo_flow_coronet_socket_config_t socket_config;
    flowie_endpoint_config_t endpoint_config = FLOWIE_ENDPOINT_CONFIG_INIT;
    turbo_flow_queue_config_t queue_config;
    turbo_flow_queue_snapshot_t queue_snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_queue_t *queue = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    flowie_queue_rule_probe_t rule_probe;
    flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t output_listener = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t output_client = FLOWIE_TEST_INVALID_SOCKET;
    unsigned short endpoint_port = flowie_test_port();
    unsigned short output_port = 0u;
    uint8_t received[32];
    memset(&socket_config, 0, sizeof(socket_config));
    memset(&queue_config, 0, sizeof(queue_config));
    atomic_init(&rule_probe.calls, 0u);
    atomic_init(&rule_probe.transformed, 0u);
    check_not_null(flow);
    check_int_gt(endpoint_port, 0);
    output_listener = flowie_queue_test_listen(&output_port);
    check_true(output_listener != FLOWIE_TEST_INVALID_SOCKET);
    check_int_gt(output_port, 0);

    endpoint_config.host = "127.0.0.1";
    endpoint_config.port = (int)endpoint_port;
    endpoint_config.max_connections = 4u;
    endpoint_config.manage_sessions = 1;
    endpoint_config.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    endpoint_config.settlement.qos2 = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    endpoint_config.max_sessions = 4u;
    endpoint_config.max_subscriptions_per_session = 8u;
    endpoint_config.max_inflight_per_session = 8u;
    queue_config.resource_uid = "queue:mqtt.rules";
    queue_config.owner_name = "mqtt-rules";
    queue_config.capacity = 8u;
    queue_config.max_payload_size = 1024u;
    queue_config.full_policy = TURBO_FLOW_QUEUE_FULL_FAIL;
    socket_config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    socket_config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    socket_config.host = "127.0.0.1";
    socket_config.port = (int)output_port;
    socket_config.timeout_ms = 2000u;
    rule_config.resource_uid = "rule-set:mqtt-route";
    rule_config.owner_name = "mqtt-rules";
    rule_config.rules = rules;
    rule_config.rule_count = sizeof(rules) / sizeof(rules[0]);
    rule_config.mode = TURBO_FLOW_RULE_ALL_MATCHES;
    rule_config.schema = &FLOWIE_QUEUE_RULE_SCHEMA;
    rule_config.facts_provider = flowie_queue_rule_facts;
    rule_config.facts_provider_ctx = &rule_probe;

    queue = turbo_flow_queue_create(&queue_config);
    check_not_null(queue);
    check_int_eq(flowie_register_endpoint(flow, "mqtt.endpoint", &endpoint_config), TURBO_OK);
    check_int_eq(turbo_flow_queue_register_sink_adapter(flow, "queue.accept", queue), TURBO_OK);
    check_int_eq(turbo_flow_queue_register_source_adapter(flow, "queue.accepted", queue),
                 TURBO_OK);
    check_int_eq(turbo_flow_coronet_register_socket_adapter(flow, "socket.output", &socket_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_rule_processor_create(&rule_config, &processor, NULL), TURBO_OK);
    check_int_eq(turbo_flow_rule_register_data_operation(flow, "rules.mqtt", processor), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "transformed_output",
                                              flowie_queue_rule_transform_probe, &rule_probe, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    publisher = flowie_test_connect(endpoint_port);
    subscriber = flowie_test_connect(endpoint_port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(publisher, 0u, 8u, FLOWIE_DEFAULT_MAX_PACKET_SIZE),
                 TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, FLOWIE_DEFAULT_MAX_PACKET_SIZE),
                 TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));

    check_int_eq(flowie_test_send(publisher, local_publish, sizeof(local_publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(local_puback)), TURBO_OK);
    check_mem_eq(received, local_puback, sizeof(local_puback));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(local_delivery)), TURBO_OK);
    check_mem_eq(received, local_delivery, sizeof(local_delivery));
    check_false(flowie_test_socket_readable(output_listener, 50u));

    check_int_eq(flowie_test_send(publisher, egress_publish, sizeof(egress_publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(egress_puback)), TURBO_OK);
    check_mem_eq(received, egress_puback, sizeof(egress_puback));
    check_true(flowie_test_socket_readable(output_listener, 2000u));
    output_client = accept(output_listener, NULL, NULL);
    check_true(output_client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_recv_exact(output_client, received, sizeof(egress_publish)), TURBO_OK);
    check_mem_eq(received, egress_publish, sizeof(egress_publish));
    check_false(flowie_test_socket_readable(subscriber, 50u));

    for (size_t i = 0u; i < FLOWIE_QUEUE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_queue_snapshot(queue, &queue_snapshot), TURBO_OK);
      if (queue_snapshot.delivered == 2u) break;
      turbo_sleep_ms(1u);
    }
    check_uint_eq(queue_snapshot.enqueued, 2u);
    check_uint_eq(queue_snapshot.delivered, 2u);
    check_size_eq(atomic_load_explicit(&rule_probe.calls, memory_order_relaxed), 2u);
    check_size_eq(atomic_load_explicit(&rule_probe.transformed, memory_order_relaxed), 1u);

    flowie_test_socket_close(output_client);
    flowie_test_socket_close(output_listener);
    flowie_test_socket_close(subscriber);
    flowie_test_socket_close(publisher);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }

  it("replays a durable MQTT PUBLISH through RulesForge into the existing socket output") {
    static const char producer_graph[] = "source input\n"
                                         "stage persist adapter queue.persist\n"
                                         "stage main {\n"
                                         "  input -> persist\n"
                                         "}\n";
    static const char consumer_graph[] =
        "source replay adapter queue.replay\n"
        "stage rules_route operation rules.apply resource rules.mqtt\n"
        "stage application_output adapter socket.output\n"
        "stage main {\n"
        "  replay -> rules_route -> application_output\n"
        "}\n";
    static const uint8_t publish[] = {0x32u, 0x14u, 0x00u, 0x0eu, 'e', 'g', 'r', 'e', 's', 's',
                                      '/',   'd',   'u',   'r',   'a', 'b', 'l', 'e', 0x00u, 0x31u,
                                      0x00u, 'z'};
    const turbo_flow_rule_t rule = {
        "mqtt.topic == \"egress/durable\"", 0u,
        flowie_queue_route_action("application_output")};
    turbo_flow_rule_processor_config_t rule_config = TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT;
    turbo_flow_coronet_socket_config_t socket_config;
    turbo_flow_sqlite_queue_config_t sqlite_config;
    turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_queue_t *queue = NULL;
    turbo_flow_t *producer = turbo_flow_create();
    turbo_flow_t *consumer = NULL;
    turbo_flow_msg_t message;
    flowie_queue_rule_probe_t rule_probe;
    flowie_test_socket_t output_listener = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t output_client = FLOWIE_TEST_INVALID_SOCKET;
    char database_path[TURBO_FS_MAX_PATH];
    unsigned short output_port = 0u;
    uint8_t received[sizeof(publish)];
    memset(&socket_config, 0, sizeof(socket_config));
    memset(&sqlite_config, 0, sizeof(sqlite_config));
    atomic_init(&rule_probe.calls, 0u);
    atomic_init(&rule_probe.transformed, 0u);
    queue_test_database_path(database_path, sizeof(database_path));
    check_not_null(producer);
    sqlite_config.queue.resource_uid = "queue:mqtt.rules.durable";
    sqlite_config.queue.owner_name = "mqtt-rules-durable";
    sqlite_config.queue.capacity = 8u;
    sqlite_config.queue.max_payload_size = 1024u;
    sqlite_config.queue.full_policy = TURBO_FLOW_QUEUE_FULL_FAIL;
    sqlite_config.database_path = database_path;
    sqlite_config.queue_name = "mqtt-rules";
    sqlite_config.busy_timeout_ms = 1000;

    queue = turbo_flow_sqlite_queue_create(&sqlite_config);
    check_not_null(queue);
    check_int_eq(turbo_flow_queue_register_sink_adapter(producer, "queue.persist", queue),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(producer, producer_graph, sizeof(producer_graph) - 1u),
                 TURBO_OK);
    check_int_eq(turbo_flow_compile(producer), TURBO_OK);
    check_int_eq(turbo_flow_start(producer), TURBO_OK);
    turbo_flow_msg_init(&message);
    message.owned_payload = tstr_new_len(publish, sizeof(publish));
    check_not_null(message.owned_payload);
    message.payload = tstr_to_v(message.owned_payload);
    check_int_eq(flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, publish[0] & 0x0fu,
                                                  &message.flags),
                 TURBO_OK);
    check_int_eq(turbo_flow_publish(producer, "input", &message), TURBO_OK);
    turbo_flow_msg_cleanup(&message);
    check_int_eq(turbo_flow_stop(producer), TURBO_OK);
    turbo_flow_destroy(producer);
    producer = NULL;
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);

    output_listener = flowie_queue_test_listen(&output_port);
    check_true(output_listener != FLOWIE_TEST_INVALID_SOCKET);
    check_int_gt(output_port, 0);
    queue = turbo_flow_sqlite_queue_create(&sqlite_config);
    consumer = turbo_flow_create();
    check_not_null(queue);
    check_not_null(consumer);
    socket_config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    socket_config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    socket_config.host = "127.0.0.1";
    socket_config.port = (int)output_port;
    socket_config.timeout_ms = 2000u;
    rule_config.resource_uid = "rule-set:mqtt-durable-route";
    rule_config.owner_name = "mqtt-rules-durable";
    rule_config.rules = &rule;
    rule_config.rule_count = 1u;
    rule_config.schema = &FLOWIE_QUEUE_RULE_SCHEMA;
    rule_config.facts_provider = flowie_queue_rule_facts;
    rule_config.facts_provider_ctx = &rule_probe;
    check_int_eq(turbo_flow_queue_register_source_adapter(consumer, "queue.replay", queue),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_coronet_register_socket_adapter(consumer, "socket.output", &socket_config),
        TURBO_OK);
    check_int_eq(turbo_flow_rule_processor_create(&rule_config, &processor, NULL), TURBO_OK);
    check_int_eq(turbo_flow_rule_register_data_operation(consumer, "rules.mqtt", processor),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(consumer, consumer_graph, sizeof(consumer_graph) - 1u),
                 TURBO_OK);
    check_int_eq(turbo_flow_compile(consumer), TURBO_OK);
    check_int_eq(turbo_flow_start(consumer), TURBO_OK);
    check_true(flowie_test_socket_readable(output_listener, 2000u));
    output_client = accept(output_listener, NULL, NULL);
    check_true(output_client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_recv_exact(output_client, received, sizeof(received)), TURBO_OK);
    check_mem_eq(received, publish, sizeof(publish));
    for (size_t i = 0u; i < FLOWIE_QUEUE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_queue_snapshot(queue, &snapshot), TURBO_OK);
      if (snapshot.delivered == 1u && snapshot.depth == 0u) break;
      turbo_sleep_ms(1u);
    }
    /* Runtime counters restart with the owner; the SQLite row is the durable fact. */
    check_uint_eq(snapshot.enqueued, 0u);
    check_uint_eq(snapshot.delivered, 1u);
    check_size_eq(snapshot.depth, 0u);
    check_size_eq(atomic_load_explicit(&rule_probe.calls, memory_order_relaxed), 1u);
    flowie_test_socket_close(output_client);
    flowie_test_socket_close(output_listener);
    check_int_eq(turbo_flow_stop(consumer), TURBO_OK);
    turbo_flow_destroy(consumer);
    turbo_flow_rule_processor_destroy(processor);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    queue_remove_database(database_path);
  }

  it("ACKs DURABLE only after SQLite COMMIT and replays after Queue recreation") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage persist adapter queue.persist\n"
                                "stage main {\n"
                                "  mqtt_in -> persist\n"
                                "}\n";
    static const uint8_t connect_packet[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T', 0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 's',   'q',   'l'};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                      0x00u, 0x55u, 0x00u, 'd'};
    static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x55u};
    char database_path[TURBO_FS_MAX_PATH];
    char yaml[FLOWIE_QUEUE_TEST_YAML_CAPACITY];
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
    turbo_flow_queue_t *queue = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    int yaml_size;
    queue_test_database_path(database_path, sizeof(database_path));
    check_int_gt(port, 0);
    check_not_null(flow);
    yaml_size = snprintf(
        yaml, sizeof(yaml),
        "version: 1\n"
        "channels:\n"
        "  mqtt.durable:\n"
        "    kind: queue\n"
        "    config:\n"
        "      backend: sqlite\n"
        "      pattern: push_pull\n"
        "      resource_uid: queue:mqtt.durable\n"
        "      owner_name: mqtt-durable\n"
        "      capacity: 8\n"
        "      max_payload_size: 1024\n"
        "      full_policy: fail\n"
        "      database_path: '%s'\n"
        "      queue_name: mqtt-publish\n"
        "      busy_timeout_ms: 1000\n"
        "adapters:\n"
        "  mqtt.endpoint:\n"
        "    kind: flowie_endpoint\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: %hu\n"
        "      max_connections: 4\n"
        "      manage_sessions: true\n"
        "      settlement_qos1: durable\n"
        "      settlement_qos2: durable\n"
        "      max_sessions: 4\n"
        "      max_subscriptions_per_session: 8\n"
        "      max_inflight_per_session: 8\n"
        "  queue.persist:\n"
        "    kind: queue\n"
        "    config:\n"
        "      channel: mqtt.durable\n"
        "      role: sink\n",
        database_path, port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "mqtt.durable", &queue, &error),
                 TURBO_OK);
    check_int_eq(flowie_register_resolved_endpoint(flow, "mqtt.endpoint", resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_register_resolved_adapter(flow, "queue.persist", resolved, queue,
                                                            &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 0u, 8u, FLOWIE_DEFAULT_MAX_PACKET_SIZE),
                 TURBO_OK);
    check_int_eq(flowie_test_send(client, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(puback)), TURBO_OK);
    check_mem_eq(received, puback, sizeof(puback));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);

    queue = NULL;
    check_int_eq(turbo_flow_queue_create_resolved(resolved, "mqtt.durable", &queue, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_queue_claim(queue, &claim), TURBO_OK);
    check_mem_eq(claim.message->payload.data, publish, sizeof(publish));
    check_null(turbo_flow_msg_protocol_route(claim.message));
    check_null(turbo_flow_msg_protocol_settlement(claim.message));
    check_int_eq(turbo_flow_queue_claim_ack(queue, claim.token), TURBO_OK);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    queue_remove_database(database_path);
  }

  it("restores a persistent MQTT subscription through the SQLite record store") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage mqtt_fanout adapter mqtt.endpoint\n"
                                "stage main {\n"
                                "  mqtt_in -> mqtt_fanout\n"
                                "}\n";
    static const uint8_t subscriber_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T', 0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 's',   'u',   'b'};
    static const uint8_t publisher_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T', 0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'p',   'u',   'b'};
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, 'a',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x01u};
    static const uint8_t publish[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                      0x00u, 0x31u, 0x00u, 'x'};
    static const uint8_t publisher_puback[] = {0x40u, 0x02u, 0x00u, 0x31u};
    static const uint8_t delivery[] = {0x32u, 0x07u, 0x00u, 0x01u, 'a',
                                       0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t replay[] = {0x3au, 0x07u, 0x00u, 0x01u, 'a',
                                     0x00u, 0x01u, 0x00u, 'x'};
    static const uint8_t subscriber_puback[] = {0x40u, 0x02u, 0x00u, 0x01u};
    char database_path[TURBO_FS_MAX_PATH];
    char yaml[FLOWIE_QUEUE_TEST_YAML_CAPACITY];
    uint8_t received[16];
    unsigned short port = flowie_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
    turbo_flow_t *flow = NULL;
    int yaml_size;
    queue_test_database_path(database_path, sizeof(database_path));
    check_int_gt(port, 0);
    yaml_size = snprintf(
        yaml, sizeof(yaml),
        "version: 1\n"
        "channels:\n"
        "  mqtt.sessions:\n"
        "    kind: record_store\n"
        "    config:\n"
        "      backend: sqlite\n"
        "      database_path: '%s'\n"
        "      namespace_name: mqtt.sessions\n"
        "      busy_timeout_ms: 1000\n"
        "      max_key_size: 65538\n"
        "      max_value_size: 1048576\n"
        "      max_batch_size: 16\n"
        "      max_records: 8\n"
        "adapters:\n"
        "  mqtt.endpoint:\n"
        "    kind: flowie_endpoint\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: %hu\n"
        "      max_packet_size: 4096\n"
        "      max_connections: 4\n"
        "      manage_sessions: true\n"
        "      max_sessions: 4\n"
        "      max_retained_messages: 4\n"
        "      max_subscriptions_per_session: 8\n"
        "      max_inflight_per_session: 8\n"
        "      session_store: mqtt.sessions\n",
        database_path, port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    persistence.store_channel = "mqtt.sessions";
    persistence.store = &store;
    bindings.persistence = &persistence;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(flowie_register_resolved_bound_endpoint(flow, "mqtt.endpoint", resolved,
                                                         &bindings, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    flowie_test_socket_close(subscriber);
    subscriber = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    turbo_flow_sqlite_record_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(flowie_register_resolved_bound_endpoint(flow, "mqtt.endpoint", resolved,
                                                         &bindings, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 1u, 8u, 4096u), TURBO_OK);
    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(publisher, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, publish, sizeof(publish)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(publisher_puback)), TURBO_OK);
    check_mem_eq(received, publisher_puback, sizeof(publisher_puback));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(delivery)), TURBO_OK);
    check_mem_eq(received, delivery, sizeof(delivery));
    flowie_test_socket_close(publisher);
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    turbo_flow_sqlite_record_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(flowie_register_resolved_bound_endpoint(flow, "mqtt.endpoint", resolved,
                                                         &bindings, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 1u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(replay)), TURBO_OK);
    check_mem_eq(received, replay, sizeof(replay));
    check_int_eq(flowie_test_send(subscriber, subscriber_puback, sizeof(subscriber_puback)),
                 TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_sqlite_record_store_destroy(&store);
    turbo_flow_resolved_config_destroy(resolved);
    queue_remove_database(database_path);
  }

  it("restores, deletes, and expires retained MQTT publications through SQLite") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage mqtt_fanout adapter mqtt.endpoint\n"
                                "stage main {\n"
                                "  mqtt_in -> mqtt_fanout\n"
                                "}\n";
    static const uint8_t publisher_connect[] = {
        0x10u, 0x10u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T', 0x05u, 0x02u,
        0x00u, 0x3cu, 0x00u, 0x00u, 0x03u, 'p', 'u', 'b'};
    static const uint8_t subscriber_connect[] = {
        0x10u, 0x10u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T', 0x05u, 0x02u,
        0x00u, 0x3cu, 0x00u, 0x00u, 0x03u, 's', 'u', 'b'};
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, 'a',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t retained[] = {0x31u, 0x05u, 0x00u, 0x01u, 'a', 0x00u, 'x'};
    static const uint8_t retained_delete[] = {0x31u, 0x04u, 0x00u, 0x01u, 'a', 0x00u};
    static const uint8_t retained_expiring[] = {0x31u, 0x0au, 0x00u, 0x01u, 'a', 0x05u,
                                                 0x02u, 0x00u, 0x00u, 0x00u, 0x01u, 'y'};
    static const uint8_t ping[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    char database_path[TURBO_FS_MAX_PATH];
    char yaml[FLOWIE_QUEUE_TEST_YAML_CAPACITY];
    uint8_t received[16];
    unsigned short port = flowie_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
    turbo_flow_t *flow = NULL;
    int yaml_size;
    queue_test_database_path(database_path, sizeof(database_path));
    check_int_gt(port, 0);
    yaml_size = snprintf(
        yaml, sizeof(yaml),
        "version: 1\n"
        "channels:\n"
        "  mqtt.sessions:\n"
        "    kind: record_store\n"
        "    config:\n"
        "      backend: sqlite\n"
        "      database_path: '%s'\n"
        "      namespace_name: mqtt.retained\n"
        "      busy_timeout_ms: 1000\n"
        "      max_key_size: 65538\n"
        "      max_value_size: 65536\n"
        "      max_batch_size: 8\n"
        "      max_records: 8\n"
        "adapters:\n"
        "  mqtt.endpoint:\n"
        "    kind: flowie_endpoint\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: %hu\n"
        "      max_packet_size: 4096\n"
        "      max_connections: 4\n"
        "      manage_sessions: true\n"
        "      max_sessions: 4\n"
        "      max_retained_messages: 4\n"
        "      max_subscriptions_per_session: 8\n"
        "      max_inflight_per_session: 8\n"
        "      session_store: mqtt.sessions\n",
        database_path, port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    persistence.store_channel = "mqtt.sessions";
    persistence.store = &store;
    bindings.persistence = &persistence;

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    check_int_eq(flowie_queue_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(publisher, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, retained, sizeof(retained)), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(publisher);
    publisher = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    turbo_flow_sqlite_record_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    check_int_eq(flowie_queue_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(retained)), TURBO_OK);
    check_mem_eq(received, retained, sizeof(retained));
    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(publisher, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, retained_delete, sizeof(retained_delete)), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(publisher);
    flowie_test_socket_close(subscriber);
    publisher = FLOWIE_TEST_INVALID_SOCKET;
    subscriber = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    turbo_flow_sqlite_record_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    check_int_eq(flowie_queue_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_false(flowie_test_socket_readable(subscriber, 50u));
    publisher = flowie_test_connect(port);
    check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(publisher, publisher_connect, sizeof(publisher_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(publisher, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(publisher, retained_expiring, sizeof(retained_expiring)),
                 TURBO_OK);
    check_int_eq(flowie_test_send(publisher, ping, sizeof(ping)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(publisher, received, sizeof(pingresp)), TURBO_OK);
    check_mem_eq(received, pingresp, sizeof(pingresp));
    flowie_test_socket_close(publisher);
    flowie_test_socket_close(subscriber);
    publisher = FLOWIE_TEST_INVALID_SOCKET;
    subscriber = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    turbo_flow_sqlite_record_store_destroy(&store);

    turbo_sleep_ms(1100u);
    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    check_int_eq(flowie_queue_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, sizeof(subscriber_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    check_false(flowie_test_socket_readable(subscriber, 50u));
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_sqlite_record_store_destroy(&store);
    turbo_flow_resolved_config_destroy(resolved);
    queue_remove_database(database_path);
  }

  it("restores a pending MQTT Will through SQLite and publishes it at session expiry") {
    enum { WILL_DELIVERY_WAIT_MS = 4000u };
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage mqtt_fanout adapter mqtt.endpoint\n"
                                "stage main {\n"
                                "  mqtt_in -> mqtt_fanout\n"
                                "}\n";
    static const uint8_t subscribe[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                        0x00u, 0x01u, 'w',   0x00u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t expected_will[] = {0x30u, 0x05u, 0x00u, 0x01u,
                                            'w',   0x00u, 'x'};
    char database_path[TURBO_FS_MAX_PATH];
    char yaml[FLOWIE_QUEUE_TEST_YAML_CAPACITY];
    uint8_t subscriber_connect[128];
    uint8_t will_connect[128];
    uint8_t received[32];
    size_t subscriber_connect_size = 0u;
    size_t will_connect_size = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    turbo_flow_connection_snapshot_t connections = {0};
    flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t will_client = FLOWIE_TEST_INVALID_SOCKET;
    turbo_flow_t *flow = NULL;
    int yaml_size;
    queue_test_database_path(database_path, sizeof(database_path));
    check_int_gt(port, 0);
    check_int_eq(flowie_queue_encode_connect(subscriber_connect, sizeof(subscriber_connect),
                                             &subscriber_connect_size, "sub", 60u, NULL, NULL,
                                             0u),
                 TURBO_OK);
    check_int_eq(flowie_queue_encode_connect(will_connect, sizeof(will_connect),
                                             &will_connect_size, "wil", 2u, "w", "x", 5u),
                 TURBO_OK);
    yaml_size = snprintf(
        yaml, sizeof(yaml),
        "version: 1\n"
        "channels:\n"
        "  mqtt.sessions:\n"
        "    kind: record_store\n"
        "    config:\n"
        "      backend: sqlite\n"
        "      database_path: '%s'\n"
        "      namespace_name: mqtt.will\n"
        "      busy_timeout_ms: 1000\n"
        "      max_key_size: 65538\n"
        "      max_value_size: 1048576\n"
        "      max_batch_size: 16\n"
        "      max_records: 8\n"
        "adapters:\n"
        "  mqtt.endpoint:\n"
        "    kind: flowie_endpoint\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: %hu\n"
        "      max_packet_size: 4096\n"
        "      max_connections: 4\n"
        "      manage_sessions: true\n"
        "      max_sessions: 4\n"
        "      max_retained_messages: 4\n"
        "      max_subscriptions_per_session: 8\n"
        "      max_inflight_per_session: 8\n"
        "      session_store: mqtt.sessions\n",
        database_path, port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    persistence.store_channel = "mqtt.sessions";
    persistence.store = &store;
    bindings.persistence = &persistence;

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    check_int_eq(flowie_queue_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 0u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(subscriber, subscribe, sizeof(subscribe)), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(suback)), TURBO_OK);
    check_mem_eq(received, suback, sizeof(suback));
    will_client = flowie_test_connect(port);
    check_true(will_client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(will_client, will_connect, will_connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(will_client, 0u, 8u, 4096u), TURBO_OK);
    flowie_test_socket_close(will_client);
    will_client = FLOWIE_TEST_INVALID_SOCKET;
    for (size_t i = 0u; i < FLOWIE_QUEUE_TEST_WAIT_STEPS; ++i) {
      check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connections), TURBO_OK);
      if (connections.connections_current == 1u) break;
      turbo_sleep_ms(1u);
    }
    check_size_eq(connections.connections_current, 1u);
    check_false(flowie_test_socket_readable(subscriber, 50u));
    flowie_test_socket_close(subscriber);
    subscriber = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    turbo_flow_sqlite_record_store_destroy(&store);

    check_int_eq(turbo_flow_sqlite_record_store_create_resolved(resolved, "mqtt.sessions", &store,
                                                                &error),
                 TURBO_OK);
    check_int_eq(flowie_queue_start_bound_endpoint(resolved, &bindings, graph, &flow), TURBO_OK);
    subscriber = flowie_test_connect(port);
    check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(subscriber, subscriber_connect, subscriber_connect_size),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(subscriber, 1u, 8u, 4096u), TURBO_OK);
    check_true(flowie_test_socket_readable(subscriber, WILL_DELIVERY_WAIT_MS));
    check_int_eq(flowie_test_recv_exact(subscriber, received, sizeof(expected_will)), TURBO_OK);
    check_mem_eq(received, expected_will, sizeof(expected_will));
    flowie_test_socket_close(subscriber);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_sqlite_record_store_destroy(&store);
    turbo_flow_resolved_config_destroy(resolved);
    queue_remove_database(database_path);
  }

  it("does not send CONNACK when the durable session CAS conflicts") {
    static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                                "stage mqtt_fanout adapter mqtt.endpoint\n"
                                "stage main {\n"
                                "  mqtt_in -> mqtt_fanout\n"
                                "}\n";
    static const uint8_t connect_packet[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T', 0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'c',   'a',   's'};
    static const uint8_t conflicting_value[] = {'x'};
    char database_path[TURBO_FS_MAX_PATH];
    uint8_t received[8];
    unsigned short port = flowie_test_port();
    turbo_flow_sqlite_record_store_config_t store_config;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
    flowie_endpoint_config_t endpoint_config = FLOWIE_ENDPOINT_CONFIG_INIT;
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    queue_test_database_path(database_path, sizeof(database_path));
    memset(&store_config, 0, sizeof(store_config));
    store_config.database_path = database_path;
    store_config.namespace_name = "mqtt.sessions";
    store_config.busy_timeout_ms = 1000;
    store_config.max_key_size = 65538u;
    store_config.max_value_size = 1048576u;
    store_config.max_batch_size = 16u;
    store_config.max_records = 8u;
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_sqlite_record_store_create(&store_config, &store), TURBO_OK);
    persistence.store_channel = "mqtt.sessions";
    persistence.store = &store;
    bindings.persistence = &persistence;
    endpoint_config.host = "127.0.0.1";
    endpoint_config.port = (int)port;
    endpoint_config.max_connections = 4u;
    endpoint_config.max_packet_size = 4096u;
    endpoint_config.manage_sessions = 1;
    endpoint_config.max_sessions = 4u;
    endpoint_config.max_retained_messages = 4u;
    endpoint_config.max_subscriptions_per_session = 8u;
    endpoint_config.max_inflight_per_session = 8u;
    check_int_eq(flowie_register_bound_endpoint(flow, "mqtt.endpoint", &endpoint_config, &bindings),
                 TURBO_OK);
    mutation.key = (const uint8_t *)"cas";
    mutation.key_size = 3u;
    mutation.next_revision = 1u;
    mutation.value = conflicting_value;
    mutation.value_size = sizeof(conflicting_value);
    check_int_eq(store.commit(store.ctx, &mutation, 1u), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_packet, sizeof(connect_packet)), TURBO_OK);
    check_int_ne(flowie_test_recv_exact(client, received, 5u), TURBO_OK);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_sqlite_record_store_destroy(&store);
    queue_remove_database(database_path);
  }
}
