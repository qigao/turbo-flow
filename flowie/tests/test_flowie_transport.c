#include "flowie.h"
#include "flowie_mqtt_client.h"
#include "flowie_test_socket.h"
#include "tls_test_pki.h"
#include "tls_test_support.h"

#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <openssl/ssl.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <dirent.h>
#endif

#define FLOWIE_TRANSPORT_TEST_TIMEOUT_MS 10000u
#define FLOWIE_TRANSPORT_STOP_MAX_MS 2000u

typedef struct flowie_transport_client_state_s {
  atomic_int done;
  atomic_int status;
  flowie_mqtt_subscription_t subscription;
  flowie_mqtt_subscribe_packet_t subscribe;
  flowie_mqtt_client_publish_topic_t publish_topic;
  flowie_mqtt_client_publish_topic_vec_t publish;
  flowie_mqtt_span_t unsubscribe_filter;
  flowie_mqtt_unsubscribe_packet_t unsubscribe;
} flowie_transport_client_state_t;

typedef struct flowie_pipe_client_state_s {
  coro_context_t *context;
  const char *path;
  flowie_mqtt_version_t version;
  int done;
  int status;
} flowie_pipe_client_state_t;

typedef struct flowie_ws_raw_state_s {
  coro_context_t *context;
  flowie_transport_t transport;
  unsigned short port;
  const char *path;
  const char *subprotocol;
  const char *ca_file;
  int text_frame;
  int frame_case;
  int done;
  int status;
} flowie_ws_raw_state_t;

typedef enum flowie_transport_framing_mode_e {
  FLOWIE_TRANSPORT_FRAMING_BYTEWISE,
  FLOWIE_TRANSPORT_FRAMING_COALESCED,
} flowie_transport_framing_mode_t;

typedef struct flowie_transport_framing_state_s {
  coro_context_t *context;
  flowie_transport_t transport;
  unsigned short port;
  const char *path;
  const uint8_t *connect_packet;
  size_t connect_packet_size;
  flowie_transport_framing_mode_t mode;
  const char *phase;
  int done;
  int status;
} flowie_transport_framing_state_t;

typedef enum flowie_transport_shutdown_mode_e {
  FLOWIE_TRANSPORT_SHUTDOWN_PENDING_RECV,
  FLOWIE_TRANSPORT_SHUTDOWN_PARTIAL_MQTT,
} flowie_transport_shutdown_mode_t;

typedef struct flowie_transport_shutdown_state_s {
  coro_context_t *context;
  coro_socket_t *socket;
  flowie_transport_t transport;
  unsigned short port;
  const char *path;
  const uint8_t *connect_packet;
  size_t connect_packet_size;
  flowie_transport_shutdown_mode_t mode;
  int done;
  int status;
} flowie_transport_shutdown_state_t;

typedef struct flowie_wss_shutdown_state_s {
  flowie_ws_raw_state_t handshake;
  coro_socket_t *socket;
} flowie_wss_shutdown_state_t;

enum {
  FLOWIE_WS_OPCODE_CONTINUATION = 0x0u,
  FLOWIE_WS_OPCODE_TEXT = 0x1u,
  FLOWIE_WS_OPCODE_BINARY = 0x2u,
  FLOWIE_WS_OPCODE_CLOSE = 0x8u,
};

typedef enum flowie_ws_rejection_case_e {
  FLOWIE_WS_REJECTION_NONE,
  FLOWIE_WS_REJECTION_TEXT,
  FLOWIE_WS_REJECTION_SINGLE_OVERSIZE,
  FLOWIE_WS_REJECTION_FRAGMENTED_OVERSIZE,
  FLOWIE_WS_REJECTION_INVALID_CLOSE,
} flowie_ws_rejection_case_t;

static size_t flowie_ws_frame_build_header(uint8_t output[14], int fin, uint8_t opcode,
                                           size_t payload_size, const uint8_t mask[4]) {
  size_t mask_offset;
  if (payload_size > UINT16_MAX) return 0u;
  output[0] = (uint8_t)((fin ? 0x80u : 0u) | (opcode & 0x0fu));
  if (payload_size <= 125u) {
    output[1] = (uint8_t)(0x80u | payload_size);
    mask_offset = 2u;
  } else {
    output[1] = 0xfeu;
    output[2] = (uint8_t)(payload_size >> 8u);
    output[3] = (uint8_t)payload_size;
    mask_offset = 4u;
  }
  memcpy(output + mask_offset, mask, 4u);
  return mask_offset + 4u;
}

static int flowie_transport_process_handle_count(size_t *count) {
  if (!count) return TURBO_EINVAL;
#if defined(_WIN32)
  {
    DWORD handles = 0u;
    if (!GetProcessHandleCount(GetCurrentProcess(), &handles)) return TURBO_EIO;
    *count = (size_t)handles;
  }
#else
  {
    DIR *directory = opendir("/proc/self/fd");
    struct dirent *entry;
    size_t observed = 0u;
    if (!directory) return TURBO_ENOTSUP;
    while ((entry = readdir(directory)) != NULL) {
      if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) ++observed;
    }
    closedir(directory);
    *count = observed - 1u;
  }
#endif
  return TURBO_OK;
}

static const char *flowie_transport_test_ca_file(void) {
  const char *path = getenv("TURBONET_TLS_CA_FILE");
  return path && path[0] != '\0' ? path : NULL;
}

static int flowie_transport_discard(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return TURBO_OK;
}

static turbo_flow_t *flowie_transport_flow_limit(flowie_transport_t transport, unsigned short port,
                                                 const char *path, size_t max_packet_size) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage discard\n"
                              "stage main {\n"
                              "  mqtt_in -> discard\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.transport = transport;
  config.host = transport == FLOWIE_TRANSPORT_PIPE ? NULL : "127.0.0.1";
  config.port = transport == FLOWIE_TRANSPORT_PIPE ? 0 : (int)port;
  config.path = path;
  config.max_packet_size = max_packet_size;
  config.max_connections = 2u;
  config.recv_timeout_ms = FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  config.manage_sessions = 1;
  config.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
  config.settlement.qos2 = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
  config.max_sessions = 2u;
  config.max_subscriptions_per_session = 2u;
  config.max_inflight_per_session = 2u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "discard", flowie_transport_discard, NULL, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_transport_flow(flowie_transport_t transport, unsigned short port,
                                           const char *path) {
  return flowie_transport_flow_limit(transport, port, path, 0u);
}

static void flowie_transport_complete(flowie_transport_client_state_t *state, int status) {
  if (status != TURBO_OK) atomic_store_explicit(&state->status, status, memory_order_relaxed);
  atomic_store_explicit(&state->done, 1, memory_order_release);
}

static void flowie_transport_on_connect(flowie_mqtt_client_t *client, int status,
                                        const flowie_mqtt_control_packet_view_t *response,
                                        void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  if (status == TURBO_OK && (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_client_subscribe(client, &state->subscribe);
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_subscribe(flowie_mqtt_client_t *client, int status,
                                          const flowie_mqtt_control_packet_view_t *response,
                                          void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  if (status == TURBO_OK && (!response || response->type != FLOWIE_MQTT_PACKET_SUBACK))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_client_publish(client, &state->publish);
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_publish(flowie_mqtt_client_t *client, int status,
                                        const flowie_mqtt_control_packet_view_t *response,
                                        void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  if (status == TURBO_OK && (!response || response->type != FLOWIE_MQTT_PACKET_PUBCOMP))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_client_unsubscribe(client, &state->unsubscribe);
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_unsubscribe(flowie_mqtt_client_t *client, int status,
                                            const flowie_mqtt_control_packet_view_t *response,
                                            void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  if (status == TURBO_OK && (!response || response->type != FLOWIE_MQTT_PACKET_UNSUBACK))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_client_ping(client);
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_ping(flowie_mqtt_client_t *client, int status,
                                     const flowie_mqtt_control_packet_view_t *response,
                                     void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  (void)response;
  if (status == TURBO_OK)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_disconnect(flowie_mqtt_client_t *client, int status,
                                           const flowie_mqtt_control_packet_view_t *response,
                                           void *user_data) {
  (void)client;
  (void)response;
  flowie_transport_complete((flowie_transport_client_state_t *)user_data, status);
}

static void flowie_transport_on_error(flowie_mqtt_client_t *client, int status, void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  (void)client;
  if (!atomic_load_explicit(&state->done, memory_order_acquire))
    flowie_transport_complete(state, status);
}

static void flowie_transport_recovery_on_connect(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  if (status == TURBO_OK && (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK))
    status = TURBO_EPROTO;
  if (status == TURBO_OK)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static int flowie_transport_recovery_client(flowie_transport_t transport, unsigned short port,
                                            const char *path, unsigned int client_number) {
  char client_id[48];
  flowie_transport_client_state_t state = {0};
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_t *client = NULL;
  uint64_t deadline;
  int rc;

  atomic_init(&state.done, 0);
  atomic_init(&state.status, TURBO_OK);
  config.transport = transport == FLOWIE_TRANSPORT_WSS ? FLOWIE_MQTT_CLIENT_TRANSPORT_WSS
                                                        : FLOWIE_MQTT_CLIENT_TRANSPORT_WS;
  config.host = transport == FLOWIE_TRANSPORT_WSS ? "localhost" : "127.0.0.1";
  config.port = (int)port;
  config.path = path;
  config.timeout_ms = FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  config.on_connect = flowie_transport_recovery_on_connect;
  config.on_disconnect = flowie_transport_on_disconnect;
  config.on_error = flowie_transport_on_error;
  config.user_data = &state;
  if (transport == FLOWIE_TRANSPORT_WSS) {
    config.tls.ca_file = flowie_transport_test_ca_file();
    if (!config.tls.ca_file) return TURBO_EINVAL;
  }
  rc = flowie_mqtt_client_create(&config, &client);
  if (rc != TURBO_OK) return rc;
  (void)snprintf(client_id, sizeof(client_id), "flowie-ws-recovery-%u", client_number);
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
  rc = flowie_mqtt_client_connect(client, &connect);
  if (rc == TURBO_OK) {
    deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
    while (!atomic_load_explicit(&state.done, memory_order_acquire) &&
           turbo_monotonic_ms() < deadline)
      turbo_sleep_ms(1u);
    rc = atomic_load_explicit(&state.done, memory_order_acquire)
             ? atomic_load_explicit(&state.status, memory_order_relaxed)
             : TURBO_ETIMEDOUT;
  }
  flowie_mqtt_client_destroy(client);
  return rc;
}

static int flowie_transport_resource(const turbo_flow_t *flow, turbo_flow_resource_kind_t kind,
                                     const char *uid, size_t *index) {
  size_t count;
  if (!flow || !uid || !index) return TURBO_EINVAL;
  count = turbo_flow_resource_metadata_count(flow);
  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc = turbo_flow_resource_metadata_at(flow, i, &metadata);
    if (rc != TURBO_OK) return rc;
    if (metadata.kind == kind && strcmp(metadata.uid, uid) == 0) {
      *index = i;
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

static int flowie_transport_wait_resource(const turbo_flow_t *flow,
                                          turbo_flow_resource_kind_t kind, const char *uid,
                                          size_t expected) {
  size_t index = 0u;
  uint64_t deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  int rc = flowie_transport_resource(flow, kind, uid, &index);
  if (rc != TURBO_OK) return rc;
  while (turbo_monotonic_ms() < deadline) {
    turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    rc = turbo_flow_resource_snapshot_at(flow, index, &snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot.load == expected) return TURBO_OK;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_transport_wait_connections(const turbo_flow_t *flow, size_t expected) {
  return flowie_transport_wait_resource(flow, TURBO_FLOW_RESOURCE_CONNECTION,
                                        "flowie.endpoint.connection", expected);
}

static int flowie_transport_wait_sessions(const turbo_flow_t *flow, size_t expected) {
  return flowie_transport_wait_resource(flow, TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
                                        "flowie.endpoint.protocol", expected);
}

static int flowie_transport_wait_idle(const turbo_flow_t *flow) {
  int rc = flowie_transport_wait_connections(flow, 0u);
  return rc == TURBO_OK ? flowie_transport_wait_sessions(flow, 0u) : rc;
}

static coro_socket_t *flowie_ws_raw_socket(flowie_ws_raw_state_t *state) {
  coro_socket_t *socket = coro_socket_create(
      state->context, state->transport == FLOWIE_TRANSPORT_WSS ? CORO_SOCKET_TLS
                                                                : CORO_SOCKET_TCP_V4);
  if (socket && state->transport == FLOWIE_TRANSPORT_WSS && state->ca_file) {
    turbo_tls_client_config_t tls_config = {0};
    tls_config.ca_file = state->ca_file;
    tls_config.verify_peer = 1;
    if (coro_socket_set_tls_client_config(socket, &tls_config) != TURBO_OK) {
      coro_socket_destroy(socket);
      return NULL;
    }
  }
  if (socket) coro_socket_set_timeout(socket, FLOWIE_TRANSPORT_TEST_TIMEOUT_MS);
  return socket;
}

static int flowie_ws_raw_handshake(flowie_ws_raw_state_t *state, coro_socket_t *socket,
                                   int expect_upgrade) {
  char request[512];
  char *response = NULL;
  size_t response_size = 0u;
  int written;
  int rc;
  int upgraded;
  const char *host = state->transport == FLOWIE_TRANSPORT_WSS ? "localhost" : "127.0.0.1";
  written = snprintf(request, sizeof(request),
                     "GET %s HTTP/1.1\r\n"
                     "Host: %s\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     "Sec-WebSocket-Version: 13\r\n"
                     "%s%s%s"
                     "\r\n",
                     state->path, host, state->subprotocol ? "Sec-WebSocket-Protocol: " : "",
                     state->subprotocol ? state->subprotocol : "",
                     state->subprotocol ? "\r\n" : "");
  if (written < 0 || (size_t)written >= sizeof(request)) return TURBO_ERANGE;
  rc = coro_socket_connect(socket, host, state->port);
  if (rc != TURBO_OK) return rc;
  rc = coro_socket_send(socket, request, (size_t)written);
  if (rc != TURBO_OK) return rc;
  rc = coro_socket_recv(socket, &response, &response_size);
  if (rc != TURBO_OK) {
    coro_socket_free_recv(response);
    return state->text_frame ? rc : TURBO_OK;
  }
  upgraded = response_size >= 12u && memcmp(response, "HTTP/1.1 101", 12u) == 0;
  coro_socket_free_recv(response);
  return upgraded == expect_upgrade ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_ws_raw_send_frame(coro_socket_t *socket, int fin, uint8_t opcode,
                                    const uint8_t *payload, size_t payload_size,
                                    const uint8_t mask[4]) {
  uint8_t frame[256];
  size_t header_size = flowie_ws_frame_build_header(frame, fin, opcode, payload_size, mask);
  if (!socket || (!payload && payload_size != 0u) || header_size == 0u ||
      payload_size > sizeof(frame) - header_size)
    return TURBO_EINVAL;
  for (size_t i = 0u; i < payload_size; ++i)
    frame[header_size + i] = payload[i] ^ mask[i & 3u];
  return coro_socket_send(socket, (const char *)frame, header_size + payload_size);
}

static int flowie_ws_raw_expect_close(coro_socket_t *socket, uint16_t expected_code,
                                      int allow_transport_close) {
  char *response = NULL;
  size_t response_size = 0u;
  int rc = coro_socket_recv(socket, &response, &response_size);
  if (rc != TURBO_OK) {
    coro_socket_free_recv(response);
    return allow_transport_close ? TURBO_OK : rc;
  }
  if (response_size < 2u || ((uint8_t)response[0] & 0x0fu) != FLOWIE_WS_OPCODE_CLOSE) {
    coro_socket_free_recv(response);
    return TURBO_EPROTO;
  }
  if (expected_code != 0u &&
      (response_size < 4u || (((uint16_t)(uint8_t)response[2] << 8u) |
                              (uint16_t)(uint8_t)response[3]) != expected_code)) {
    coro_socket_free_recv(response);
    return TURBO_EPROTO;
  }
  coro_socket_free_recv(response);
  return TURBO_OK;
}

static void flowie_ws_raw_client(coro_t *coroutine, void *arg) {
  static const uint8_t connect_packet[] = {
      0x10u, 0x1au, 0x00u, 0x04u, 'M', 'Q', 'T', 'T', 0x04u, 0x02u, 0x00u, 0x1eu,
      0x00u, 0x0eu, 'f', 'l', 'o', 'w', 'i', 'e', '-', 'w', 's', '-', 't', 'e', 'x', 't'};
  static const uint8_t mask[4] = {0x41u, 0x52u, 0x63u, 0x74u};
  flowie_ws_raw_state_t *state = (flowie_ws_raw_state_t *)arg;
  coro_socket_t *socket = flowie_ws_raw_socket(state);
  uint8_t payload[80];
  int rc = socket ? TURBO_OK : TURBO_ENOMEM;
  (void)coroutine;
  if (rc == TURBO_OK)
    rc = flowie_ws_raw_handshake(state, socket,
                                 state->frame_case != FLOWIE_WS_REJECTION_NONE);
  memset(payload, 0x5au, sizeof(payload));
  if (rc == TURBO_OK && state->frame_case == FLOWIE_WS_REJECTION_TEXT) {
    rc = flowie_ws_raw_send_frame(socket, 1, FLOWIE_WS_OPCODE_TEXT, connect_packet,
                                  sizeof(connect_packet), mask);
    if (rc == TURBO_OK) rc = flowie_ws_raw_expect_close(socket, 1003u, 0);
  } else if (rc == TURBO_OK && state->frame_case == FLOWIE_WS_REJECTION_SINGLE_OVERSIZE) {
    rc = flowie_ws_raw_send_frame(socket, 1, FLOWIE_WS_OPCODE_BINARY, payload, sizeof(payload),
                                  mask);
    if (rc == TURBO_OK) rc = flowie_ws_raw_expect_close(socket, 1009u, 0);
  } else if (rc == TURBO_OK &&
             state->frame_case == FLOWIE_WS_REJECTION_FRAGMENTED_OVERSIZE) {
    rc = flowie_ws_raw_send_frame(socket, 0, FLOWIE_WS_OPCODE_BINARY, payload, 40u, mask);
    if (rc == TURBO_OK)
      rc = flowie_ws_raw_send_frame(socket, 1, FLOWIE_WS_OPCODE_CONTINUATION, payload + 40u, 40u,
                                    mask);
    if (rc == TURBO_OK) rc = flowie_ws_raw_expect_close(socket, 1009u, 0);
  } else if (rc == TURBO_OK && state->frame_case == FLOWIE_WS_REJECTION_INVALID_CLOSE) {
    rc = flowie_ws_raw_send_frame(socket, 1, FLOWIE_WS_OPCODE_CLOSE, payload, 1u, mask);
    if (rc == TURBO_OK) rc = flowie_ws_raw_expect_close(socket, 0u, 1);
  }
  if (socket) coro_socket_destroy(socket);
  state->status = rc;
  state->done = 1;
}

static int flowie_transport_raw_rejection(flowie_transport_t transport, unsigned short port,
                                          const char *path, const char *subprotocol,
                                          flowie_ws_rejection_case_t frame_case) {
  flowie_ws_raw_state_t state = {0};
  uint64_t deadline;
  int rc;
  state.context = coro_context_create(NULL);
  if (!state.context) return TURBO_ENOMEM;
  state.transport = transport;
  state.port = port;
  state.path = path;
  state.subprotocol = subprotocol;
  state.ca_file = transport == FLOWIE_TRANSPORT_WSS ? flowie_transport_test_ca_file() : NULL;
  if (transport == FLOWIE_TRANSPORT_WSS && !state.ca_file) {
    coro_context_destroy(state.context);
    return TURBO_EINVAL;
  }
  state.text_frame = frame_case != FLOWIE_WS_REJECTION_NONE;
  state.frame_case = frame_case;
  state.status = TURBO_EBUSY;
  rc = coro_context_spawn(state.context, flowie_ws_raw_client, &state);
  if (rc == TURBO_OK) {
    deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
    while (!state.done && turbo_monotonic_ms() < deadline) {
      rc = coro_context_run(state.context, TURBO_RUN_ONCE);
      if (rc != TURBO_OK) break;
    }
    if (rc == TURBO_OK) rc = state.done ? state.status : TURBO_ETIMEDOUT;
  }
  coro_context_destroy(state.context);
  return rc;
}

static int flowie_transport_ws_policy_case(flowie_transport_t transport,
                                           const char **failed_phase) {
  unsigned short port = flowie_test_port();
  const char *path = "/mqtt";
  turbo_flow_t *flow = NULL;
  int rc;
  if (failed_phase) *failed_phase = "start";
  if (port == 0u) return TURBO_EIO;
  flow = flowie_transport_flow(transport, port, path);
  if (!flow) return TURBO_ENOMEM;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  if (failed_phase) *failed_phase = "wrong-path rejection";
  rc = flowie_transport_raw_rejection(transport, port, "/wrong", "mqtt",
                                      FLOWIE_WS_REJECTION_NONE);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-path cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-path recovery";
  if (rc == TURBO_OK) rc = flowie_transport_recovery_client(transport, port, path, 1u);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-path recovery cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "missing-subprotocol rejection";
  if (rc == TURBO_OK)
    rc = flowie_transport_raw_rejection(transport, port, path, NULL, FLOWIE_WS_REJECTION_NONE);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "missing-subprotocol cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "missing-subprotocol recovery";
  if (rc == TURBO_OK) rc = flowie_transport_recovery_client(transport, port, path, 2u);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "missing-subprotocol recovery cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-subprotocol rejection";
  if (rc == TURBO_OK)
    rc = flowie_transport_raw_rejection(transport, port, path, "not-mqtt",
                                        FLOWIE_WS_REJECTION_NONE);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-subprotocol cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-subprotocol recovery";
  if (rc == TURBO_OK) rc = flowie_transport_recovery_client(transport, port, path, 3u);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "wrong-subprotocol recovery cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "text-frame rejection";
  if (rc == TURBO_OK)
    rc = flowie_transport_raw_rejection(transport, port, path, "mqtt", FLOWIE_WS_REJECTION_TEXT);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "text-frame cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "text-frame recovery";
  if (rc == TURBO_OK) rc = flowie_transport_recovery_client(transport, port, path, 4u);
  if (failed_phase && rc == TURBO_OK) *failed_phase = "text-frame recovery cleanup";
  if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
done:
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

static int flowie_transport_ws_size_policy_case(flowie_transport_t transport,
                                                const char **failed_phase) {
  static const flowie_ws_rejection_case_t cases[] = {
      FLOWIE_WS_REJECTION_SINGLE_OVERSIZE,
      FLOWIE_WS_REJECTION_FRAGMENTED_OVERSIZE,
      FLOWIE_WS_REJECTION_INVALID_CLOSE,
  };
  unsigned short port = flowie_test_port();
  turbo_flow_t *flow = NULL;
  int rc = port == 0u ? TURBO_EIO : TURBO_OK;
  if (failed_phase) *failed_phase = "size-policy start";
  if (rc == TURBO_OK) flow = flowie_transport_flow_limit(transport, port, "/mqtt", 64u);
  if (rc == TURBO_OK && !flow) rc = TURBO_ENOMEM;
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  for (size_t i = 0u; rc == TURBO_OK && i < sizeof(cases) / sizeof(cases[0]); ++i) {
    if (failed_phase)
      *failed_phase = cases[i] == FLOWIE_WS_REJECTION_SINGLE_OVERSIZE
                          ? "single-frame oversize rejection"
                      : cases[i] == FLOWIE_WS_REJECTION_FRAGMENTED_OVERSIZE
                          ? "fragmented oversize rejection"
                          : "invalid-close rejection";
    rc = flowie_transport_raw_rejection(transport, port, "/mqtt", "mqtt", cases[i]);
    if (failed_phase && rc == TURBO_OK) *failed_phase = "size-policy rejection cleanup";
    if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
    if (failed_phase && rc == TURBO_OK) *failed_phase = "size-policy recovery";
    if (rc == TURBO_OK) rc = flowie_transport_recovery_client(transport, port, "/mqtt", 10u + i);
    if (failed_phase && rc == TURBO_OK) *failed_phase = "size-policy recovery cleanup";
    if (rc == TURBO_OK) rc = flowie_transport_wait_idle(flow);
  }
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

static int flowie_transport_client_case(flowie_transport_t server_transport,
                                        flowie_mqtt_client_transport_t client_transport,
                                        const char *path, unsigned int client_number,
                                        flowie_mqtt_version_t version) {
  char client_id[48];
  flowie_transport_client_state_t state = {0};
  flowie_mqtt_client_config_t client_config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  static const uint8_t filter[] = "transport/data";
  static const uint8_t payload[] = "payload";
  flowie_mqtt_client_t *client = NULL;
  turbo_flow_t *flow = NULL;
  unsigned short port = 0u;
  uint64_t deadline;
  int rc;

  atomic_init(&state.done, 0);
  atomic_init(&state.status, TURBO_OK);
  state.subscription.filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  state.subscription.qos = 2u;
  state.subscribe = (flowie_mqtt_subscribe_packet_t)FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  state.subscribe.version = version;
  state.subscribe.subscriptions = &state.subscription;
  state.subscribe.subscription_count = 1u;
  state.publish_topic.qos = 2u;
  state.publish_topic.topic = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  state.publish_topic.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
  state.publish = (flowie_mqtt_client_publish_topic_vec_t)FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  state.publish.version = version;
  state.publish.data = &state.publish_topic;
  state.publish.count = 1u;
  state.unsubscribe_filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  state.unsubscribe = (flowie_mqtt_unsubscribe_packet_t)FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  state.unsubscribe.version = version;
  state.unsubscribe.filters = &state.unsubscribe_filter;
  state.unsubscribe.filter_count = 1u;
  port = flowie_test_port();
  if (port == 0u) return TURBO_EIO;
  flow = flowie_transport_flow(server_transport, port, path);
  if (!flow) return TURBO_EIO;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;

  client_config.transport = client_transport;
  client_config.host =
      server_transport == FLOWIE_TRANSPORT_TLS || server_transport == FLOWIE_TRANSPORT_WSS
          ? "localhost"
          : "127.0.0.1";
  client_config.port = (int)port;
  client_config.path = path;
  client_config.timeout_ms = FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  if (server_transport == FLOWIE_TRANSPORT_TLS || server_transport == FLOWIE_TRANSPORT_WSS) {
    client_config.tls.ca_file = flowie_transport_test_ca_file();
    if (!client_config.tls.ca_file) {
      rc = TURBO_EINVAL;
      goto done;
    }
  }
  client_config.on_connect = flowie_transport_on_connect;
  client_config.on_subscribe = flowie_transport_on_subscribe;
  client_config.on_publish = flowie_transport_on_publish;
  client_config.on_unsubscribe = flowie_transport_on_unsubscribe;
  client_config.on_ping = flowie_transport_on_ping;
  client_config.on_disconnect = flowie_transport_on_disconnect;
  client_config.on_error = flowie_transport_on_error;
  client_config.user_data = &state;
  rc = flowie_mqtt_client_create(&client_config, &client);
  if (rc != TURBO_OK) goto done;
  (void)snprintf(client_id, sizeof(client_id), "flowie-transport-%u", client_number);
  connect.version = version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
  rc = flowie_mqtt_client_connect(client, &connect);
  if (rc != TURBO_OK) goto done;
  deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  while (!atomic_load_explicit(&state.done, memory_order_acquire) &&
         turbo_monotonic_ms() < deadline)
    turbo_sleep_ms(1u);
  rc = atomic_load_explicit(&state.done, memory_order_acquire)
           ? atomic_load_explicit(&state.status, memory_order_relaxed)
           : TURBO_ETIMEDOUT;

done:
  flowie_mqtt_client_destroy(client);
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

static int flowie_transport_tls_policy_rejection_case(int legacy_version) {
  char endpoint[64];
  SSL_CTX *client_context = NULL;
  SSL *connection = NULL;
  BIO *socket_bio = NULL;
  turbo_flow_t *flow = NULL;
  unsigned short port = flowie_test_port();
  int rc = TURBO_EIO;
  if (port == 0u) return TURBO_EIO;
  flow = flowie_transport_flow(FLOWIE_TRANSPORT_TLS, port, NULL);
  if (!flow || turbo_flow_start(flow) != TURBO_OK) goto cleanup;
  client_context = SSL_CTX_new(TLS_client_method());
  if (!client_context) goto cleanup;
  SSL_CTX_set_verify(client_context, SSL_VERIFY_NONE, NULL);
  if (legacy_version) {
    if (SSL_CTX_set_min_proto_version(client_context, TLS1_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(client_context, TLS1_1_VERSION) != 1)
      goto cleanup;
  } else {
    if (SSL_CTX_set_min_proto_version(client_context, TLS1_2_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(client_context, TLS1_2_VERSION) != 1 ||
        SSL_CTX_set_cipher_list(client_context, "PSK-AES128-CBC-SHA") != 1)
      goto cleanup;
  }
  if (snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", (unsigned int)port) <= 0)
    goto cleanup;
  socket_bio = BIO_new_connect(endpoint);
  connection = SSL_new(client_context);
  if (!socket_bio || !connection) goto cleanup;
  SSL_set_bio(connection, socket_bio, socket_bio);
  socket_bio = NULL;
  SSL_set_connect_state(connection);
  rc = SSL_connect(connection) <= 0 ? TURBO_OK : TURBO_EPROTO;
cleanup:
  BIO_free(socket_bio);
  SSL_free(connection);
  SSL_CTX_free(client_context);
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

static int flowie_transport_incomplete_handshake_stop_case(flowie_transport_t transport) {
  unsigned short port = flowie_test_port();
  turbo_flow_t *flow = NULL;
  flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
  uint64_t started_at;
  uint64_t elapsed;
  int rc;
  if (port == 0u) return TURBO_EIO;
  flow = flowie_transport_flow(transport, port,
                               transport == FLOWIE_TRANSPORT_WSS ? "/mqtt" : NULL);
  if (!flow) return TURBO_EIO;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  client = flowie_test_connect(port);
  if (client == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_EIO;
    goto done;
  }
  turbo_sleep_ms(50u);
  started_at = turbo_monotonic_ms();
  rc = turbo_flow_stop(flow);
  elapsed = turbo_monotonic_ms() - started_at;
  if (rc == TURBO_OK && elapsed > FLOWIE_TRANSPORT_STOP_MAX_MS) rc = TURBO_ETIMEDOUT;

done:
  flowie_test_socket_close(client);
  if (flow && rc != TURBO_OK) (void)turbo_flow_stop(flow);
  turbo_flow_destroy(flow);
  return rc;
}

static int flowie_pipe_recv_exact(coro_socket_t *socket, uint8_t *output, size_t expected) {
  size_t offset = 0u;
  while (offset < expected) {
    char *data = NULL;
    size_t size = 0u;
    int rc = coro_socket_recv(socket, &data, &size);
    if (rc != TURBO_OK) return rc;
    if (!data || size == 0u || size > expected - offset) {
      coro_socket_free_recv(data);
      return TURBO_EPROTO;
    }
    memcpy(output + offset, data, size);
    offset += size;
    coro_socket_free_recv(data);
  }
  return TURBO_OK;
}

static coro_socket_t *flowie_transport_framing_socket(
    flowie_transport_framing_state_t *state) {
  coro_socket_t *socket;
  switch (state->transport) {
  case FLOWIE_TRANSPORT_TCP:
  case FLOWIE_TRANSPORT_WS:
    return coro_socket_create(state->context, CORO_SOCKET_TCP_V4);
  case FLOWIE_TRANSPORT_TLS:
  case FLOWIE_TRANSPORT_WSS: {
    turbo_tls_client_config_t tls_config = {0};
    tls_config.ca_file = flowie_transport_test_ca_file();
    tls_config.verify_peer = 1;
    if (!tls_config.ca_file) return NULL;
    socket = coro_socket_create(state->context, CORO_SOCKET_TLS);
    if (socket && coro_socket_set_tls_client_config(socket, &tls_config) != TURBO_OK) {
      coro_socket_destroy(socket);
      socket = NULL;
    }
    return socket;
  }
  case FLOWIE_TRANSPORT_PIPE:
    return coro_socket_create_pipe(state->context);
  default:
    return NULL;
  }
}

static int flowie_transport_framing_connect(flowie_transport_framing_state_t *state,
                                            coro_socket_t *socket) {
  switch (state->transport) {
  case FLOWIE_TRANSPORT_TCP:
    return coro_socket_connect(socket, "127.0.0.1", state->port);
  case FLOWIE_TRANSPORT_TLS:
    return coro_socket_connect(socket, "localhost", state->port);
  case FLOWIE_TRANSPORT_WS:
    return coro_socket_connect_ws_ex(socket, "127.0.0.1", state->port, state->path, 0, "mqtt");
  case FLOWIE_TRANSPORT_WSS:
    return coro_socket_connect_ws_ex(socket, "localhost", state->port, state->path, 1, "mqtt");
  case FLOWIE_TRANSPORT_PIPE:
    return coro_socket_connect_pipe(socket, state->path);
  default:
    return TURBO_EINVAL;
  }
}

static void flowie_transport_shutdown_client(coro_t *coroutine, void *arg) {
  static const uint8_t connack[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                                    0x02u, 0x27u, 0x00u, 0x10u, 0x00u, 0x00u};
  static const uint8_t partial_ping[] = {0xc0u};
  flowie_transport_shutdown_state_t *state = (flowie_transport_shutdown_state_t *)arg;
  flowie_transport_framing_state_t framing = {0};
  coro_socket_t *socket;
  uint8_t response[sizeof(connack)];
  int rc;
  (void)coroutine;
  framing.context = state->context;
  framing.transport = state->transport;
  framing.port = state->port;
  framing.path = state->path;
  socket = flowie_transport_framing_socket(&framing);
  rc = socket ? TURBO_OK : TURBO_ENOMEM;
  if (rc == TURBO_OK) coro_socket_set_timeout(socket, FLOWIE_TRANSPORT_TEST_TIMEOUT_MS);
  if (rc == TURBO_OK) rc = flowie_transport_framing_connect(&framing, socket);
  if (rc == TURBO_OK && state->mode == FLOWIE_TRANSPORT_SHUTDOWN_PARTIAL_MQTT) {
    rc = coro_socket_send(socket, (const char *)state->connect_packet, state->connect_packet_size);
    if (rc == TURBO_OK) rc = flowie_pipe_recv_exact(socket, response, sizeof(response));
    if (rc == TURBO_OK && memcmp(response, connack, sizeof(connack)) != 0) rc = TURBO_EPROTO;
    if (rc == TURBO_OK)
      rc = coro_socket_send(socket, (const char *)partial_ping, sizeof(partial_ping));
  }
  if (rc == TURBO_OK) {
    state->socket = socket;
    socket = NULL;
  }
  if (socket) coro_socket_destroy(socket);
  state->status = rc;
  state->done = 1;
}

static int flowie_transport_shutdown_run_client(flowie_transport_shutdown_state_t *state) {
  uint64_t deadline;
  int rc;
  state->context = coro_context_create(NULL);
  if (!state->context) return TURBO_ENOMEM;
  state->done = 0;
  state->status = TURBO_EBUSY;
  rc = coro_context_spawn(state->context, flowie_transport_shutdown_client, state);
  if (rc == TURBO_OK) {
    deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
    while (!state->done && turbo_monotonic_ms() < deadline) {
      rc = coro_context_run(state->context, TURBO_RUN_ONCE);
      if (rc != TURBO_OK) break;
    }
    if (rc == TURBO_OK) rc = state->done ? state->status : TURBO_ETIMEDOUT;
  }
  return rc;
}

static void flowie_transport_shutdown_client_destroy(flowie_transport_shutdown_state_t *state) {
  if (!state) return;
  if (state->socket) {
    coro_socket_destroy(state->socket);
    state->socket = NULL;
  }
  if (state->context) {
    coro_context_destroy(state->context);
    state->context = NULL;
  }
}

static int flowie_transport_stop_bounded(turbo_flow_t *flow) {
  uint64_t started_at;
  uint64_t elapsed;
  int rc;
  if (!flow) return TURBO_EINVAL;
  started_at = turbo_monotonic_ms();
  rc = turbo_flow_stop(flow);
  elapsed = turbo_monotonic_ms() - started_at;
  if (rc == TURBO_OK && elapsed > FLOWIE_TRANSPORT_STOP_MAX_MS) return TURBO_ETIMEDOUT;
  if (rc == TURBO_OK) rc = flowie_transport_wait_connections(flow, 0u);
  if (rc == TURBO_OK) rc = flowie_transport_wait_sessions(flow, 0u);
  return rc;
}

static int flowie_transport_shutdown_case(flowie_transport_shutdown_mode_t mode) {
  static const uint8_t client_id[] = "flowie-shutdown";
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_transport_shutdown_state_t state = {0};
  uint8_t connect_packet[128];
  size_t connect_packet_size = 0u;
  unsigned short port = flowie_test_port();
  turbo_flow_t *flow = NULL;
  int rc;
  if (port == 0u) return TURBO_EIO;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  rc = flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                         &connect_packet_size);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  flow = flowie_transport_flow(FLOWIE_TRANSPORT_TCP, port, NULL);
  if (!flow) return TURBO_ENOMEM;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  state.transport = FLOWIE_TRANSPORT_TCP;
  state.port = port;
  state.connect_packet = connect_packet;
  state.connect_packet_size = connect_packet_size;
  state.mode = mode;
  rc = flowie_transport_shutdown_run_client(&state);
  if (rc == TURBO_OK) rc = flowie_transport_wait_connections(flow, 1u);
  if (rc == TURBO_OK)
    rc = flowie_transport_wait_sessions(
        flow, mode == FLOWIE_TRANSPORT_SHUTDOWN_PARTIAL_MQTT ? 1u : 0u);
  if (rc == TURBO_OK) rc = flowie_transport_stop_bounded(flow);

done:
  flowie_transport_shutdown_client_destroy(&state);
  if (flow && rc != TURBO_OK) (void)turbo_flow_stop(flow);
  turbo_flow_destroy(flow);
  return rc;
}

static void flowie_wss_partial_close_client(coro_t *coroutine, void *arg) {
  static const uint8_t mask[4] = {0x51u, 0x62u, 0x73u, 0x84u};
  flowie_wss_shutdown_state_t *state = (flowie_wss_shutdown_state_t *)arg;
  coro_socket_t *socket = flowie_ws_raw_socket(&state->handshake);
  uint8_t frame[8];
  size_t header_size;
  int rc = socket ? TURBO_OK : TURBO_ENOMEM;
  (void)coroutine;
  if (rc == TURBO_OK) rc = flowie_ws_raw_handshake(&state->handshake, socket, 1);
  header_size = flowie_ws_frame_build_header(frame, 1, FLOWIE_WS_OPCODE_CLOSE, 2u, mask);
  if (rc == TURBO_OK && header_size == 0u) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    frame[header_size] = UINT8_C(0x03) ^ mask[0];
    rc = coro_socket_send(socket, (const char *)frame, header_size + 1u);
  }
  if (rc == TURBO_OK) {
    state->socket = socket;
    socket = NULL;
  }
  if (socket) coro_socket_destroy(socket);
  state->handshake.status = rc;
  state->handshake.done = 1;
}

static int flowie_transport_wss_partial_close_stop_case(const char *ca_file) {
  flowie_wss_shutdown_state_t state = {0};
  uint64_t deadline;
  unsigned short port = flowie_test_port();
  turbo_flow_t *flow = NULL;
  int rc;
  if (port == 0u || !ca_file) return TURBO_EINVAL;
  flow = flowie_transport_flow(FLOWIE_TRANSPORT_WSS, port, "/mqtt");
  if (!flow) return TURBO_ENOMEM;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  state.handshake.context = coro_context_create(NULL);
  if (!state.handshake.context) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  state.handshake.transport = FLOWIE_TRANSPORT_WSS;
  state.handshake.port = port;
  state.handshake.path = "/mqtt";
  state.handshake.subprotocol = "mqtt";
  state.handshake.ca_file = ca_file;
  state.handshake.status = TURBO_EBUSY;
  rc = coro_context_spawn(state.handshake.context, flowie_wss_partial_close_client, &state);
  if (rc == TURBO_OK) {
    deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
    while (!state.handshake.done && turbo_monotonic_ms() < deadline) {
      rc = coro_context_run(state.handshake.context, TURBO_RUN_ONCE);
      if (rc != TURBO_OK) break;
    }
    if (rc == TURBO_OK)
      rc = state.handshake.done ? state.handshake.status : TURBO_ETIMEDOUT;
  }
  if (rc == TURBO_OK) rc = flowie_transport_wait_connections(flow, 1u);
  if (rc == TURBO_OK) rc = flowie_transport_wait_sessions(flow, 0u);
  if (rc == TURBO_OK) rc = flowie_transport_stop_bounded(flow);

done:
  if (state.socket) coro_socket_destroy(state.socket);
  if (state.handshake.context) coro_context_destroy(state.handshake.context);
  if (flow && rc != TURBO_OK) (void)turbo_flow_stop(flow);
  turbo_flow_destroy(flow);
  return rc;
}

static void flowie_transport_framing_client(coro_t *coroutine, void *arg) {
  static const uint8_t connack[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                                    0x02u, 0x27u, 0x00u, 0x10u, 0x00u, 0x00u};
  static const uint8_t ping_requests[] = {0xc0u, 0x00u, 0xc0u, 0x00u};
  static const uint8_t ping_responses[] = {0xd0u, 0x00u, 0xd0u, 0x00u};
  static const uint8_t disconnect[] = {0xe0u, 0x00u};
  flowie_transport_framing_state_t *state = (flowie_transport_framing_state_t *)arg;
  coro_socket_t *socket = flowie_transport_framing_socket(state);
  uint8_t request[512];
  uint8_t response[sizeof(connack) + sizeof(ping_responses)];
  size_t request_size = 0u;
  size_t response_size = 0u;
  int rc = socket ? TURBO_OK : TURBO_ENOMEM;
  (void)coroutine;
  state->phase = "socket creation";
  if (rc == TURBO_OK) coro_socket_set_timeout(socket, FLOWIE_TRANSPORT_TEST_TIMEOUT_MS);
  if (rc == TURBO_OK) state->phase = "transport connect";
  if (rc == TURBO_OK) rc = flowie_transport_framing_connect(state, socket);
  if (rc == TURBO_OK && state->mode == FLOWIE_TRANSPORT_FRAMING_BYTEWISE) {
    state->phase = "bytewise CONNECT send";
    for (size_t i = 0u; i < state->connect_packet_size && rc == TURBO_OK; ++i)
      rc = coro_socket_send(socket, (const char *)state->connect_packet + i, 1u);
    if (rc == TURBO_OK) {
      state->phase = "bytewise CONNACK receive";
      response_size = sizeof(connack);
      rc = flowie_pipe_recv_exact(socket, response, response_size);
    }
    if (rc == TURBO_OK && memcmp(response, connack, sizeof(connack)) != 0) rc = TURBO_EPROTO;
    if (rc == TURBO_OK) {
      state->phase = "bytewise PING send";
      rc = coro_socket_send(socket, (const char *)ping_requests, sizeof(ping_requests));
    }
    if (rc == TURBO_OK) {
      state->phase = "bytewise PINGRESP receive";
      response_size = sizeof(ping_responses);
      rc = flowie_pipe_recv_exact(socket, response, response_size);
    }
    if (rc == TURBO_OK && memcmp(response, ping_responses, sizeof(ping_responses)) != 0)
      rc = TURBO_EPROTO;
  } else if (rc == TURBO_OK) {
    state->phase = "coalesced CONNECT and PING send";
    if (state->connect_packet_size > sizeof(request) - sizeof(ping_requests)) {
      rc = TURBO_ERANGE;
    } else {
      memcpy(request, state->connect_packet, state->connect_packet_size);
      memcpy(request + state->connect_packet_size, ping_requests, sizeof(ping_requests));
      request_size = state->connect_packet_size + sizeof(ping_requests);
      rc = coro_socket_send(socket, (const char *)request, request_size);
    }
    if (rc == TURBO_OK) {
      state->phase = "coalesced CONNACK and PINGRESP receive";
      response_size = sizeof(response);
      rc = flowie_pipe_recv_exact(socket, response, response_size);
    }
    if (rc == TURBO_OK &&
        (memcmp(response, connack, sizeof(connack)) != 0 ||
         memcmp(response + sizeof(connack), ping_responses, sizeof(ping_responses)) != 0))
      rc = TURBO_EPROTO;
  }
  if (rc == TURBO_OK) {
    state->phase = "DISCONNECT send";
    rc = coro_socket_send(socket, (const char *)disconnect, sizeof(disconnect));
  }
  if (socket) coro_socket_destroy(socket);
  state->status = rc;
  state->done = 1;
}

static int flowie_transport_framing_run_client(flowie_transport_framing_state_t *state) {
  uint64_t deadline;
  int rc;
  state->context = coro_context_create(NULL);
  if (!state->context) return TURBO_ENOMEM;
  state->done = 0;
  state->status = TURBO_EBUSY;
  rc = coro_context_spawn(state->context, flowie_transport_framing_client, state);
  if (rc == TURBO_OK) {
    deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
    while (!state->done && turbo_monotonic_ms() < deadline) {
      rc = coro_context_run(state->context, TURBO_RUN_ONCE);
      if (rc != TURBO_OK) break;
    }
    if (rc == TURBO_OK) rc = state->done ? state->status : TURBO_ETIMEDOUT;
  }
  coro_context_destroy(state->context);
  state->context = NULL;
  return rc;
}

static int flowie_transport_framing_case(flowie_transport_t transport,
                                         const char **failed_phase) {
  enum { FLOWIE_FRAMING_CLIENT_ID_SIZE = 130u };
  char path[96];
  uint8_t client_id[FLOWIE_FRAMING_CLIENT_ID_SIZE];
  uint8_t connect_packet[256];
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_transport_framing_state_t state = {0};
  turbo_flow_t *flow = NULL;
  unsigned short port = transport == FLOWIE_TRANSPORT_PIPE ? 0u : flowie_test_port();
  size_t connect_packet_size = 0u;
  int rc;
  if (failed_phase) *failed_phase = "start";
  if (transport != FLOWIE_TRANSPORT_PIPE && port == 0u) return TURBO_EIO;
  memset(client_id, 'f', sizeof(client_id));
  memcpy(client_id, "flowie-framing-", sizeof("flowie-framing-") - 1u);
  (void)snprintf(path, sizeof(path), "pipe://flowie-framing-%llu",
                 (unsigned long long)turbo_monotonic_ms());
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id)};
  if (flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                       &connect_packet_size) != FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  if (connect_packet_size < 3u || (connect_packet[1] & 0x80u) == 0u) return TURBO_EPROTO;
  state.transport = transport;
  state.port = port;
  state.path = transport == FLOWIE_TRANSPORT_PIPE ? path : "/mqtt";
  state.connect_packet = connect_packet;
  state.connect_packet_size = connect_packet_size;
  state.phase = "server start";
  flow = flowie_transport_flow(transport, port, state.path);
  if (!flow) return TURBO_ENOMEM;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  state.mode = FLOWIE_TRANSPORT_FRAMING_BYTEWISE;
  rc = flowie_transport_framing_run_client(&state);
  if (failed_phase) *failed_phase = state.phase;
  if (rc == TURBO_OK) {
    if (failed_phase) *failed_phase = "bytewise connection cleanup";
    rc = flowie_transport_wait_idle(flow);
  }
  client_id[sizeof(client_id) - 1u] = 'c';
  if (rc == TURBO_OK &&
      flowie_mqtt_connect_packet_encode(&connect, connect_packet, sizeof(connect_packet),
                                       &connect_packet_size) != FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  state.connect_packet_size = connect_packet_size;
  state.mode = FLOWIE_TRANSPORT_FRAMING_COALESCED;
  if (rc == TURBO_OK) rc = flowie_transport_framing_run_client(&state);
  if (failed_phase) *failed_phase = state.phase;
  if (rc == TURBO_OK) {
    if (failed_phase) *failed_phase = "coalesced connection cleanup";
    rc = flowie_transport_wait_idle(flow);
  }
done:
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

static void flowie_pipe_client(coro_t *coroutine, void *arg) {
  static const uint8_t connect_v31[] = {
      0x10u, 0x1du, 0x00u, 0x06u, 'M',   'Q',   'I',   's',   'd',   'p',   0x03u,
      0x02u, 0x00u, 0x1eu, 0x00u, 0x0fu, 'f',   'l',   'o',   'w',   'i',   'e',
      '-',   'p',   'i',   'p',   'e',   '-',   't',   'e',   's'};
  static const uint8_t connect_v311[] = {
      0x10u, 0x1bu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u, 0x02u, 0x00u, 0x1eu, 0x00u,
      0x0fu, 'f',   'l',   'o',   'w',   'i', 'e', '-', 'p',   'i',   'p',   'e',   '-',
      't',   'e',   's'};
  static const uint8_t connect_v5[] = {
      0x10u, 0x1cu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x05u, 0x02u, 0x00u, 0x1eu, 0x00u,
      0x00u, 0x0fu, 'f',   'l',   'o',   'w', 'i', 'e', '-',   'p',   'i',   'p',   'e',
      '-',   't',   'e',   's'};
  static const uint8_t connack_v311[] = {0x20u, 0x02u, 0x00u, 0x00u};
  static const uint8_t connack_v5[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                                       0x02u, 0x27u, 0x00u, 0x10u, 0x00u, 0x00u};
  static const uint8_t ping[] = {0xc0u, 0x00u};
  static const uint8_t pingresp[] = {0xd0u, 0x00u};
  static const uint8_t filter[] = "pipe/data";
  static const uint8_t payload[] = "payload";
  static const uint8_t subscribe_reason[] = {0x01u};
  static const uint8_t unsubscribe_reason[] = {0x00u};
  flowie_pipe_client_state_t *state = (flowie_pipe_client_state_t *)arg;
  coro_socket_t *socket = coro_socket_create_pipe(state->context);
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_mqtt_span_t unsubscribe_filter = {filter, sizeof(filter) - 1u};
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_control_packet_t expected_control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  const uint8_t *connect = NULL;
  size_t connect_size = 0u;
  const uint8_t *connack = NULL;
  size_t connack_size = 0u;
  uint8_t response[sizeof(connack_v5)];
  uint8_t request[128];
  uint8_t expected_response[32];
  size_t request_size = 0u;
  size_t expected_response_size = 0u;
  int rc = socket ? TURBO_OK : TURBO_ENOMEM;
  (void)coroutine;
  switch (state->version) {
    case FLOWIE_MQTT_VERSION_3_1:
      connect = connect_v31;
      connect_size = sizeof(connect_v31);
      connack = connack_v311;
      connack_size = sizeof(connack_v311);
      break;
    case FLOWIE_MQTT_VERSION_3_1_1:
      connect = connect_v311;
      connect_size = sizeof(connect_v311);
      connack = connack_v311;
      connack_size = sizeof(connack_v311);
      break;
    case FLOWIE_MQTT_VERSION_5:
      connect = connect_v5;
      connect_size = sizeof(connect_v5);
      connack = connack_v5;
      connack_size = sizeof(connack_v5);
      break;
    default:
      rc = TURBO_EINVAL;
      break;
  }
  if (rc == TURBO_OK) coro_socket_set_timeout(socket, FLOWIE_TRANSPORT_TEST_TIMEOUT_MS);
  if (rc == TURBO_OK) rc = coro_socket_connect_pipe(socket, state->path);
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)connect, connect_size);
  if (rc == TURBO_OK) rc = flowie_pipe_recv_exact(socket, response, connack_size);
  if (rc == TURBO_OK && memcmp(response, connack, connack_size) != 0) rc = TURBO_EPROTO;
  subscription.filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  subscription.qos = 1u;
  subscribe.version = state->version;
  subscribe.packet_id = 1u;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  if (rc == TURBO_OK &&
      flowie_mqtt_subscribe_packet_encode(&subscribe, request, sizeof(request), &request_size) !=
          FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  expected_control.version = state->version;
  expected_control.type = FLOWIE_MQTT_PACKET_SUBACK;
  expected_control.packet_id = 1u;
  expected_control.reason_codes =
      (flowie_mqtt_span_t){subscribe_reason, sizeof(subscribe_reason)};
  if (rc == TURBO_OK && flowie_mqtt_control_packet_encode(
                            &expected_control, expected_response, sizeof(expected_response),
                            &expected_response_size) != FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)request, request_size);
  if (rc == TURBO_OK)
    rc = flowie_pipe_recv_exact(socket, response, expected_response_size);
  if (rc == TURBO_OK && memcmp(response, expected_response, expected_response_size) != 0)
    rc = TURBO_EPROTO;

  publish.version = state->version;
  publish.qos = 1u;
  publish.packet_id = 2u;
  publish.topic = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
  if (rc == TURBO_OK &&
      flowie_mqtt_publish_packet_encode(&publish, request, sizeof(request), &request_size) !=
          FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  expected_control = (flowie_mqtt_control_packet_t)FLOWIE_MQTT_CONTROL_PACKET_INIT;
  expected_control.version = state->version;
  expected_control.type = FLOWIE_MQTT_PACKET_PUBACK;
  expected_control.packet_id = 2u;
  if (rc == TURBO_OK && flowie_mqtt_control_packet_encode(
                            &expected_control, expected_response, sizeof(expected_response),
                            &expected_response_size) != FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)request, request_size);
  if (rc == TURBO_OK)
    rc = flowie_pipe_recv_exact(socket, response, expected_response_size);
  if (rc == TURBO_OK && memcmp(response, expected_response, expected_response_size) != 0)
    rc = TURBO_EPROTO;

  unsubscribe.version = state->version;
  unsubscribe.packet_id = 3u;
  unsubscribe.filters = &unsubscribe_filter;
  unsubscribe.filter_count = 1u;
  if (rc == TURBO_OK && flowie_mqtt_unsubscribe_packet_encode(
                            &unsubscribe, request, sizeof(request), &request_size) !=
                            FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  expected_control = (flowie_mqtt_control_packet_t)FLOWIE_MQTT_CONTROL_PACKET_INIT;
  expected_control.version = state->version;
  expected_control.type = FLOWIE_MQTT_PACKET_UNSUBACK;
  expected_control.packet_id = 3u;
  if (state->version == FLOWIE_MQTT_VERSION_5)
    expected_control.reason_codes =
        (flowie_mqtt_span_t){unsubscribe_reason, sizeof(unsubscribe_reason)};
  if (rc == TURBO_OK && flowie_mqtt_control_packet_encode(
                            &expected_control, expected_response, sizeof(expected_response),
                            &expected_response_size) != FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)request, request_size);
  if (rc == TURBO_OK)
    rc = flowie_pipe_recv_exact(socket, response, expected_response_size);
  if (rc == TURBO_OK && memcmp(response, expected_response, expected_response_size) != 0)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)ping, sizeof(ping));
  if (rc == TURBO_OK) rc = flowie_pipe_recv_exact(socket, response, sizeof(pingresp));
  if (rc == TURBO_OK && memcmp(response, pingresp, sizeof(pingresp)) != 0) rc = TURBO_EPROTO;
  coro_socket_destroy(socket);
  state->status = rc;
  state->done = 1;
}

static int flowie_pipe_case(flowie_mqtt_version_t version) {
  char path[96];
  flowie_pipe_client_state_t state = {0};
  turbo_flow_t *flow = NULL;
  uint64_t deadline;
  int rc;
  (void)snprintf(path, sizeof(path), "pipe://flowie-transport-%llu",
                 (unsigned long long)turbo_monotonic_ms());
  flow = flowie_transport_flow(FLOWIE_TRANSPORT_PIPE, 0u, path);
  if (!flow) return TURBO_EIO;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  state.context = coro_context_create(NULL);
  state.path = path;
  state.version = version;
  state.status = TURBO_EBUSY;
  if (!state.context) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = coro_context_spawn(state.context, flowie_pipe_client, &state);
  if (rc != TURBO_OK) goto done;
  deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  while (!state.done && turbo_monotonic_ms() < deadline) {
    rc = coro_context_run(state.context, TURBO_RUN_ONCE);
    if (rc != TURBO_OK) break;
  }
  if (rc == TURBO_OK) rc = state.done ? state.status : TURBO_ETIMEDOUT;

done:
  coro_context_destroy(state.context);
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

spec("Flowie server transport contract") {
  it("MQTT-SEC-001 fails fast when TLS credentials are missing or do not match") {
    char cert_path[512] = {0};
    char key_path[512] = {0};
    char unrelated_key_path[512] = {0};
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = NULL;
    check_int_ne(port, 0u);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_write_unrelated_key_file(unrelated_key_path, sizeof(unrelated_key_path)),
                 0);
    tls_test_clear_server_env();
    flow = flowie_transport_flow(FLOWIE_TRANSPORT_TLS, port, NULL);
    check_not_null(flow);
    check_int_ne(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    check_int_eq(tls_test_set_server_env(cert_path, unrelated_key_path), 0);
    flow = flowie_transport_flow(FLOWIE_TRANSPORT_TLS, port, NULL);
    check_not_null(flow);
    check_int_ne(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    tls_test_clear_server_env();
    tls_test_remove_file(unrelated_key_path);
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
  }

  it("MQTT-SEC-001 rejects an expired server certificate before MQTT CONNECT") {
    char cert_path[512] = {0};
    char key_path[512] = {0};
    check_int_eq(tls_test_write_expired_server_files(cert_path, sizeof(cert_path), key_path,
                                                     sizeof(key_path)),
                 0);
    check_int_eq(tls_test_set_ca_file_env(cert_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    check_int_ne(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 14u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
  }

  it("MQTT-SEC-001 rejects legacy TLS versions and an unsupported cipher suite") {
    char cert_path[512] = {0};
    char key_path[512] = {0};
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    check_int_eq(flowie_transport_tls_policy_rejection_case(1), TURBO_OK);
    check_int_eq(flowie_transport_tls_policy_rejection_case(0), TURBO_OK);
    tls_test_clear_server_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
  }

  it("serves MQTT 3.1, MQTT 3.1.1, and MQTT 5 over TCP, TLS, WS, and WSS") {
    char ca_path[512] = {0};
    char cert_path[512] = {0};
    char key_path[512] = {0};
    check_int_eq(tls_test_write_ca_file(ca_path, sizeof(ca_path)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TCP,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, NULL, 9u,
                                              FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 10u,
                                              FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WS, FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
                                              "/mqtt", 11u, FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WSS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "/mqtt", 12u,
                                              FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TCP,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, NULL, 1u,
                                              FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 2u,
                                              FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WS, FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
                                              "/mqtt", 3u, FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WSS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "/mqtt", 4u,
                                              FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TCP,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, NULL, 5u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 6u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WS, FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
                                              "/mqtt", 7u, FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WSS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "/mqtt", 8u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    tls_test_remove_file(ca_path);
  }

  it("MQTT-NET-004 interrupts TLS and WSS handshakes and returns process handles to baseline") {
    char cert_path[512] = {0};
    char key_path[512] = {0};
    size_t before_handles = 0u;
    size_t after_handles = 0u;
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    /* OpenSSL initializes process-global handles on first use; take the leak baseline only
     * after that one-time initialization, then require repeated connection work to return. */
    check_int_eq(flowie_transport_incomplete_handshake_stop_case(FLOWIE_TRANSPORT_TLS), TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&before_handles), TURBO_OK);
    check_int_eq(flowie_transport_incomplete_handshake_stop_case(FLOWIE_TRANSPORT_TLS), TURBO_OK);
    check_int_eq(flowie_transport_incomplete_handshake_stop_case(FLOWIE_TRANSPORT_WSS), TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&after_handles), TURBO_OK);
    check_size_eq(after_handles, before_handles);
    tls_test_clear_server_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
  }

  it("MQTT-NET-004 interrupts pending recv and returns process handles to baseline") {
    size_t before_handles = 0u;
    size_t after_handles = 0u;
    /* CoroNet may allocate process-global socket resources on first use.  Warm that
     * path before taking the baseline so this assertion measures per-run ownership. */
    check_int_eq(flowie_transport_shutdown_case(FLOWIE_TRANSPORT_SHUTDOWN_PENDING_RECV),
                 TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&before_handles), TURBO_OK);
    check_int_eq(flowie_transport_shutdown_case(FLOWIE_TRANSPORT_SHUTDOWN_PENDING_RECV),
                 TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&after_handles), TURBO_OK);
    check_size_eq(after_handles, before_handles);
  }

  it("MQTT-NET-004 discards partial MQTT and returns process handles to baseline") {
    size_t before_handles = 0u;
    size_t after_handles = 0u;
    /* CoroNet may allocate process-global socket resources on first use.  Warm that
     * path before taking the baseline so this assertion measures per-run ownership. */
    check_int_eq(flowie_transport_shutdown_case(FLOWIE_TRANSPORT_SHUTDOWN_PARTIAL_MQTT),
                 TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&before_handles), TURBO_OK);
    check_int_eq(flowie_transport_shutdown_case(FLOWIE_TRANSPORT_SHUTDOWN_PARTIAL_MQTT),
                 TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&after_handles), TURBO_OK);
    check_size_eq(after_handles, before_handles);
  }

  it("MQTT-NET-004 interrupts WSS close and returns process handles to baseline") {
    char ca_path[512] = {0};
    char cert_path[512] = {0};
    char key_path[512] = {0};
    size_t before_handles = 0u;
    size_t after_handles = 0u;
    check_int_eq(tls_test_write_ca_file(ca_path, sizeof(ca_path)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    check_int_eq(flowie_transport_wss_partial_close_stop_case(ca_path), TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&before_handles), TURBO_OK);
    check_int_eq(flowie_transport_wss_partial_close_stop_case(ca_path), TURBO_OK);
    check_int_eq(flowie_transport_process_handle_count(&after_handles), TURBO_OK);
    check_size_eq(after_handles, before_handles);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    tls_test_remove_file(ca_path);
  }

  it("MQTT-NET-006 rejects WS admission and frame policy without leaking sessions") {
    const char *phase = NULL;
    int rc = flowie_transport_ws_policy_case(FLOWIE_TRANSPORT_WS, &phase);
    info("WS policy phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
    rc = flowie_transport_ws_size_policy_case(FLOWIE_TRANSPORT_WS, &phase);
    info("WS size-policy phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
  }

  it("MQTT-NET-006 rejects WSS admission and frame policy without leaking sessions") {
    char ca_path[512] = {0};
    char cert_path[512] = {0};
    char key_path[512] = {0};
    check_int_eq(tls_test_write_ca_file(ca_path, sizeof(ca_path)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    const char *phase = NULL;
    int rc = flowie_transport_ws_policy_case(FLOWIE_TRANSPORT_WSS, &phase);
    info("WSS policy phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
    rc = flowie_transport_ws_size_policy_case(FLOWIE_TRANSPORT_WSS, &phase);
    info("WSS size-policy phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    tls_test_remove_file(ca_path);
  }

  it("frames bytewise and coalesced MQTT packets over TCP") {
    const char *phase = NULL;
    int rc = flowie_transport_framing_case(FLOWIE_TRANSPORT_TCP, &phase);
    info("TCP framing phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
  }

  it("frames bytewise and coalesced MQTT packets over TLS") {
    char ca_path[512] = {0};
    char cert_path[512] = {0};
    char key_path[512] = {0};
    const char *phase = NULL;
    int rc;
    check_int_eq(tls_test_write_ca_file(ca_path, sizeof(ca_path)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    rc = flowie_transport_framing_case(FLOWIE_TRANSPORT_TLS, &phase);
    info("TLS framing phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    tls_test_remove_file(ca_path);
  }

  it("frames bytewise and coalesced MQTT packets over WS") {
    const char *phase = NULL;
    int rc = flowie_transport_framing_case(FLOWIE_TRANSPORT_WS, &phase);
    info("WS framing phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
  }

  it("frames bytewise and coalesced MQTT packets over WSS") {
    char ca_path[512] = {0};
    char cert_path[512] = {0};
    char key_path[512] = {0};
    const char *phase = NULL;
    int rc;
    check_int_eq(tls_test_write_ca_file(ca_path, sizeof(ca_path)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    rc = flowie_transport_framing_case(FLOWIE_TRANSPORT_WSS, &phase);
    info("WSS framing phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    tls_test_remove_file(ca_path);
  }

  it("frames bytewise and coalesced MQTT packets over Pipe") {
    const char *phase = NULL;
    int rc = flowie_transport_framing_case(FLOWIE_TRANSPORT_PIPE, &phase);
    info("Pipe framing phase=%s status=%d", phase ? phase : "unknown", rc);
    check_int_eq(rc, TURBO_OK);
  }

  it("serves MQTT 3.1, MQTT 3.1.1, and MQTT 5 over Pipe") {
    check_int_eq(flowie_pipe_case(FLOWIE_MQTT_VERSION_3_1), TURBO_OK);
    check_int_eq(flowie_pipe_case(FLOWIE_MQTT_VERSION_3_1_1), TURBO_OK);
    check_int_eq(flowie_pipe_case(FLOWIE_MQTT_VERSION_5), TURBO_OK);
  }
}
