#include "flowie.h"
#include "flowie_test_socket.h"

#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "platform.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_config.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum {
  FLOWIE_CLIENT_ADAPTER_TEST_PACKET_CAPACITY = 1024,
  FLOWIE_CLIENT_ADAPTER_TEST_SOCKET_TIMEOUT_MS = 500,
  FLOWIE_CLIENT_ADAPTER_TEST_CASE_TIMEOUT_MS = 5000,
  FLOWIE_CLIENT_ADAPTER_TEST_PACKET_CONNECT = 1,
  FLOWIE_CLIENT_ADAPTER_TEST_PACKET_PUBLISH = 3,
  FLOWIE_CLIENT_ADAPTER_TEST_PACKET_PUBACK = 4,
  FLOWIE_CLIENT_ADAPTER_TEST_PACKET_SUBSCRIBE = 8,
  FLOWIE_CLIENT_ADAPTER_TEST_PACKET_ID = 42,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_LISTENING = 0,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_ACCEPTED,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_CONNECT_RECEIVED,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_CONNACK_SENT,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_SUBSCRIBE_RECEIVED,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_SUBACK_SENT,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_PUBLISH_SENT,
  FLOWIE_CLIENT_ADAPTER_TEST_PHASE_ACK_OBSERVED
};

typedef struct flowie_client_adapter_test_state_s {
  atomic_int accepted;
  atomic_int broker_phase;
  atomic_int graph_calls;
  atomic_int server_done;
  atomic_int server_status;
  int graph_result;
  int expect_puback;
} flowie_client_adapter_test_state_t;

typedef struct flowie_client_adapter_test_stream_s {
  coro_socket_t *socket;
  uint8_t data[FLOWIE_CLIENT_ADAPTER_TEST_PACKET_CAPACITY];
  size_t size;
} flowie_client_adapter_test_stream_t;

typedef struct flowie_client_adapter_test_packet_s {
  uint8_t type;
  uint16_t packet_id;
} flowie_client_adapter_test_packet_t;

static int flowie_client_adapter_test_next_packet(
    flowie_client_adapter_test_stream_t *stream,
    flowie_client_adapter_test_packet_t *packet) {
  for (;;) {
    size_t body_size = 0u;
    size_t multiplier = 1u;
    size_t body_offset = 0u;
    int header_complete = 0;

    if (stream->size >= 2u) {
      for (size_t index = 1u; index <= 4u && index < stream->size; ++index) {
        uint8_t byte = stream->data[index];
        body_size += (size_t)(byte & 0x7fu) * multiplier;
        if ((byte & 0x80u) == 0u) {
          body_offset = index + 1u;
          header_complete = 1;
          break;
        }
        multiplier *= 128u;
      }
      if (!header_complete && stream->size >= 5u) return TURBO_EPROTO;
      if (header_complete) {
        size_t packet_size;
        if (body_size > sizeof(stream->data) - body_offset) return TURBO_EMSGSIZE;
        packet_size = body_offset + body_size;
        if (stream->size >= packet_size) {
          packet->type = stream->data[0] >> 4u;
          packet->packet_id = 0u;
          if ((packet->type == FLOWIE_CLIENT_ADAPTER_TEST_PACKET_SUBSCRIBE ||
               packet->type == FLOWIE_CLIENT_ADAPTER_TEST_PACKET_PUBACK) &&
              body_size >= 2u) {
            packet->packet_id =
                (uint16_t)(((uint16_t)stream->data[body_offset] << 8u) |
                           (uint16_t)stream->data[body_offset + 1u]);
          }
          memmove(stream->data, stream->data + packet_size, stream->size - packet_size);
          stream->size -= packet_size;
          return TURBO_OK;
        }
      }
    }

    {
      char *received = NULL;
      size_t received_size = 0u;
      int rc = coro_socket_recv(stream->socket, &received, &received_size);
      if (rc != TURBO_OK) return rc;
      if (!received || received_size == 0u) {
        coro_socket_free_recv(received);
        return TURBO_EOF;
      }
      if (received_size > sizeof(stream->data) - stream->size) {
        coro_socket_free_recv(received);
        return TURBO_EMSGSIZE;
      }
      memcpy(stream->data + stream->size, received, received_size);
      stream->size += received_size;
      coro_socket_free_recv(received);
    }
  }
}

static int flowie_client_adapter_test_send_publish(coro_socket_t *socket) {
  static const uint8_t topic[] = "public/todos";
  static const uint8_t payload[] = "{\"id\":1,\"completed\":false}";
  uint8_t packet[128];
  const size_t body_size =
      2u + sizeof(topic) - 1u + 2u + 1u + sizeof(payload) - 1u;
  size_t offset = 0u;

  if (body_size > 0x7fu || body_size + 2u > sizeof(packet)) return TURBO_EMSGSIZE;
  packet[offset++] = 0x32u;
  packet[offset++] = (uint8_t)body_size;
  packet[offset++] = 0u;
  packet[offset++] = (uint8_t)(sizeof(topic) - 1u);
  memcpy(packet + offset, topic, sizeof(topic) - 1u);
  offset += sizeof(topic) - 1u;
  packet[offset++] = 0u;
  packet[offset++] = FLOWIE_CLIENT_ADAPTER_TEST_PACKET_ID;
  packet[offset++] = 0u;
  memcpy(packet + offset, payload, sizeof(payload) - 1u);
  offset += sizeof(payload) - 1u;
  return coro_socket_send(socket, (const char *)packet, offset);
}

static void flowie_client_adapter_test_broker(coro_socket_t *socket, void *arg) {
  static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
  flowie_client_adapter_test_state_t *state =
      (flowie_client_adapter_test_state_t *)arg;
  flowie_client_adapter_test_stream_t stream = {0};
  flowie_client_adapter_test_packet_t packet = {0};
  uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x00u, 0x00u, 0x01u};
  int rc = TURBO_OK;

  if (atomic_fetch_add_explicit(&state->accepted, 1, memory_order_relaxed) != 0) return;
  atomic_store_explicit(&state->broker_phase, FLOWIE_CLIENT_ADAPTER_TEST_PHASE_ACCEPTED,
                        memory_order_release);
  stream.socket = socket;
  coro_socket_set_timeout(socket, FLOWIE_CLIENT_ADAPTER_TEST_SOCKET_TIMEOUT_MS);

  rc = flowie_client_adapter_test_next_packet(&stream, &packet);
  if (rc == TURBO_OK && packet.type != FLOWIE_CLIENT_ADAPTER_TEST_PACKET_CONNECT)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    atomic_store_explicit(&state->broker_phase,
                          FLOWIE_CLIENT_ADAPTER_TEST_PHASE_CONNECT_RECEIVED,
                          memory_order_release);
    rc = coro_socket_send(socket, (const char *)connack, sizeof(connack));
  }
  if (rc == TURBO_OK)
    atomic_store_explicit(&state->broker_phase, FLOWIE_CLIENT_ADAPTER_TEST_PHASE_CONNACK_SENT,
                          memory_order_release);
  if (rc == TURBO_OK) rc = flowie_client_adapter_test_next_packet(&stream, &packet);
  if (rc == TURBO_OK && packet.type != FLOWIE_CLIENT_ADAPTER_TEST_PACKET_SUBSCRIBE)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    atomic_store_explicit(&state->broker_phase,
                          FLOWIE_CLIENT_ADAPTER_TEST_PHASE_SUBSCRIBE_RECEIVED,
                          memory_order_release);
    suback[2] = (uint8_t)(packet.packet_id >> 8u);
    suback[3] = (uint8_t)packet.packet_id;
    rc = coro_socket_send(socket, (const char *)suback, sizeof(suback));
  }
  if (rc == TURBO_OK)
    atomic_store_explicit(&state->broker_phase, FLOWIE_CLIENT_ADAPTER_TEST_PHASE_SUBACK_SENT,
                          memory_order_release);
  if (rc == TURBO_OK) rc = flowie_client_adapter_test_send_publish(socket);
  if (rc == TURBO_OK)
    atomic_store_explicit(&state->broker_phase, FLOWIE_CLIENT_ADAPTER_TEST_PHASE_PUBLISH_SENT,
                          memory_order_release);
  if (rc == TURBO_OK) {
    rc = flowie_client_adapter_test_next_packet(&stream, &packet);
    if (state->expect_puback) {
      if (rc == TURBO_OK &&
          (packet.type != FLOWIE_CLIENT_ADAPTER_TEST_PACKET_PUBACK ||
           packet.packet_id != FLOWIE_CLIENT_ADAPTER_TEST_PACKET_ID))
        rc = TURBO_EPROTO;
    } else if (rc == TURBO_OK &&
               packet.type == FLOWIE_CLIENT_ADAPTER_TEST_PACKET_PUBACK) {
      rc = TURBO_EPROTO;
    } else {
      rc = TURBO_OK;
    }
    if (rc == TURBO_OK)
      atomic_store_explicit(&state->broker_phase,
                            FLOWIE_CLIENT_ADAPTER_TEST_PHASE_ACK_OBSERVED,
                            memory_order_release);
  }

  atomic_store_explicit(&state->server_status, rc, memory_order_relaxed);
  atomic_store_explicit(&state->server_done, 1, memory_order_release);
}

static void flowie_client_adapter_test_context_runner(void *arg) {
  (void)coro_context_run((coro_context_t *)arg, TURBO_RUN_DEFAULT);
}

static int flowie_client_adapter_capture(turbo_flow_msg_t *message, void *ctx) {
  flowie_client_adapter_test_state_t *state =
      (flowie_client_adapter_test_state_t *)ctx;
  (void)message;
  atomic_fetch_add_explicit(&state->graph_calls, 1, memory_order_release);
  return state->graph_result;
}

static int flowie_client_adapter_integration_case(int graph_result,
                                                  int expect_puback) {
  static const char graph[] =
      "source upstream adapter mqtt.upstream\n"
      "stage capture\n"
      "stage main {\n"
      "  upstream -> capture\n"
      "}\n";
  flowie_client_adapter_test_state_t state;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_t *flow = NULL;
  coro_context_t *server_context = NULL;
  coro_socket_t *server = NULL;
  turbo_thread_t server_thread;
  char yaml[1024];
  uint64_t deadline;
  int flow_started = 0;
  int thread_started = 0;
  int port;
  int written;
  int rc = TURBO_OK;

  memset(&state, 0, sizeof(state));
  memset(&server_thread, 0, sizeof(server_thread));
  atomic_init(&state.accepted, 0);
  atomic_init(&state.broker_phase, FLOWIE_CLIENT_ADAPTER_TEST_PHASE_LISTENING);
  atomic_init(&state.graph_calls, 0);
  atomic_init(&state.server_done, 0);
  atomic_init(&state.server_status, TURBO_EBUSY);
  state.graph_result = graph_result;
  state.expect_puback = expect_puback;

  server_context = coro_context_create(NULL);
  if (!server_context) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  server = coro_socket_create_tcpv4(server_context);
  if (!server) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  port = (int)flowie_test_port();
  if (port <= 0) {
    rc = TURBO_EIO;
    goto done;
  }
  rc = coro_socket_listen_on(server, "127.0.0.1", port,
                             flowie_client_adapter_test_broker, &state);
  if (rc != TURBO_OK) goto done;
  coro_context_set_persistent(server_context, 1);
  rc = turbo_thread_create(&server_thread,
                           flowie_client_adapter_test_context_runner,
                           server_context);
  if (rc != TURBO_OK) goto done;
  thread_started = 1;

  written = snprintf(
      yaml, sizeof(yaml),
      "version: 1\n"
      "adapters:\n"
      "  mqtt.upstream:\n"
      "    kind: flowie_client\n"
      "    config:\n"
      "      transport: tcp\n"
      "      host: 127.0.0.1\n"
      "      port: %d\n"
      "      client_id: turboflow-integration\n"
      "      topic_filter: public/todos\n"
      "      qos: 1\n"
      "      clean_start: true\n"
      "      keep_alive: 30\n"
      "      timeout_ms: 1000\n"
      "      max_packet_size: 1048576\n"
      "      reconnect_initial_ms: 1000\n"
      "      reconnect_max_ms: 1000\n",
      port);
  if (written < 0 || (size_t)written >= sizeof(yaml)) {
    rc = TURBO_EMSGSIZE;
    goto done;
  }

  flow = turbo_flow_create();
  if (!flow) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = turbo_flow_register_stage_ex(flow, "capture",
                                    flowie_client_adapter_capture, &state, NULL);
  if (rc != TURBO_OK) goto done;
  rc = turbo_flow_config_resolve_yaml(yaml, (size_t)written, &resolved, &error);
  if (rc != TURBO_OK) goto done;
  rc = flowie_register_resolved_client_source(flow, "mqtt.upstream", resolved, &error);
  if (rc != TURBO_OK) goto done;
  rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc != TURBO_OK) goto done;
  rc = turbo_flow_compile(flow);
  if (rc != TURBO_OK) goto done;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  flow_started = 1;

  deadline = turbo_monotonic_ms() + FLOWIE_CLIENT_ADAPTER_TEST_CASE_TIMEOUT_MS;
  while (turbo_monotonic_ms() < deadline) {
    int server_done = atomic_load_explicit(&state.server_done, memory_order_acquire);
    int graph_calls = atomic_load_explicit(&state.graph_calls, memory_order_acquire);
    int server_status = atomic_load_explicit(&state.server_status, memory_order_relaxed);
    if (server_done && (server_status != TURBO_OK || graph_calls != 0)) break;
    turbo_sleep_ms(1u);
  }
  if (atomic_load_explicit(&state.server_done, memory_order_acquire) &&
      atomic_load_explicit(&state.server_status, memory_order_relaxed) != TURBO_OK) {
    rc = atomic_load_explicit(&state.server_status, memory_order_relaxed);
    fprintf(stderr,
            "flowie client broker failed: status=%d phase=%d accepted=%d graph_calls=%d\n",
            rc, atomic_load_explicit(&state.broker_phase, memory_order_relaxed),
            atomic_load_explicit(&state.accepted, memory_order_relaxed),
            atomic_load_explicit(&state.graph_calls, memory_order_relaxed));
    goto done;
  }
  if (!atomic_load_explicit(&state.server_done, memory_order_acquire) ||
      atomic_load_explicit(&state.graph_calls, memory_order_acquire) != 1) {
    rc = TURBO_ETIMEDOUT;
    fprintf(stderr,
            "flowie client integration timed out: phase=%d accepted=%d server_done=%d "
            "server_status=%d graph_calls=%d\n",
            atomic_load_explicit(&state.broker_phase, memory_order_relaxed),
            atomic_load_explicit(&state.accepted, memory_order_relaxed),
            atomic_load_explicit(&state.server_done, memory_order_relaxed),
            atomic_load_explicit(&state.server_status, memory_order_relaxed),
            atomic_load_explicit(&state.graph_calls, memory_order_relaxed));
    goto done;
  }
  rc = atomic_load_explicit(&state.server_status, memory_order_relaxed);

done:
  if (flow_started) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_destroy(flow);
  if (server_context) {
    coro_context_set_persistent(server_context, 0);
    coro_context_stop(server_context);
  }
  if (thread_started) {
    int join_rc = turbo_thread_join(&server_thread);
    if (rc == TURBO_OK && join_rc != TURBO_OK) rc = join_rc;
    turbo_thread_destroy(&server_thread);
  }
  coro_socket_destroy(server);
  coro_context_destroy(server_context);
  return rc;
}

spec("Flowie MQTT client TurboFlow adapter") {
  it("registers one explicit topic-filter source from strict YAML") {
    static const char yaml[] =
        "version: 1\n"
        "adapters:\n"
        "  mqtt.upstream:\n"
        "    kind: flowie_client\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: 1884\n"
        "      client_id: turboflow-bridge\n"
        "      topic_filter: public/todos\n"
        "      qos: 1\n"
        "      clean_start: true\n"
        "      keep_alive: 60\n"
        "      timeout_ms: 30000\n"
        "      max_packet_size: 1048576\n"
        "      reconnect_initial_ms: 100\n"
        "      reconnect_max_ms: 5000\n";
    static const char graph[] =
        "source upstream adapter mqtt.upstream\n"
        "stage capture\n"
        "stage main {\n"
        "  upstream -> capture\n"
        "}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_int_eq(turbo_flow_register_stage_ex(
                     flow, "capture", flowie_client_adapter_capture, NULL, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        flowie_register_resolved_client_source(flow, "mqtt.upstream", resolved, &error),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("moves broker QoS1 PUBLISH through Graph before PUBACK") {
    check_int_eq(flowie_client_adapter_integration_case(TURBO_OK, 1), TURBO_OK);
  }

  it("withholds PUBACK when Graph rejects broker PUBLISH") {
    check_int_eq(flowie_client_adapter_integration_case(TURBO_EIO, 0), TURBO_OK);
  }

  it("rejects unknown client fields before network I/O") {
    static const char yaml[] =
        "version: 1\n"
        "adapters:\n"
        "  mqtt.upstream:\n"
        "    kind: flowie_client\n"
        "    config:\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: 1884\n"
        "      client_id: bridge\n"
        "      topic_filter: public/todos\n"
        "      qos: 1\n"
        "      clean_start: true\n"
        "      keep_alive: 60\n"
        "      timeout_ms: 30000\n"
        "      max_packet_size: 1048576\n"
        "      reconnect_initial_ms: 100\n"
        "      reconnect_max_ms: 5000\n"
        "      fallback: true\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        flowie_register_resolved_client_source(flow, "mqtt.upstream", resolved, &error),
        TURBO_EINVAL);
    check_str_eq(error.path, "$.adapters.mqtt.upstream.config.fallback");
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
  }
}
