#include "flowie_mqtt_client.h"

#include "flowie_mqtt_protocol.h"
#include "flowie_test_socket.h"

#include "platform.h"
#include "CoroNet/turbo_coro_socket.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <stdatomic.h>
#include <string.h>

#define FLOWIE_MQTT_CLIENT_TEST_BUFFER_SIZE 4096u
#define FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS 5000u
#define FLOWIE_MQTT_CLIENT_TEST_CLOSE_GRACE_MS 10u
#define FLOWIE_MQTT_CLIENT_TEST_MANAGED_DISCONNECT_GRACE_MS 50u

typedef struct flowie_mqtt_test_broker_stream_s {
  coro_socket_t *socket;
  uint8_t bytes[FLOWIE_MQTT_CLIENT_TEST_BUFFER_SIZE];
  size_t size;
  size_t pending_size;
} flowie_mqtt_test_broker_stream_t;

typedef struct flowie_mqtt_test_state_s {
  coro_context_t *context;
  flowie_mqtt_client_t *client;
  flowie_mqtt_version_t version;
  int client_rc;
  int server_rc;
  int client_done;
  int server_done;
  int publish_count;
  char topic[32];
  char payload[32];
  int expect_disconnect;
  atomic_int managed_completions;
  atomic_int managed_error;
  atomic_int managed_disconnect_error;
} flowie_mqtt_test_state_t;

typedef struct flowie_mqtt_test_completion_s {
  flowie_mqtt_test_state_t *state;
  flowie_mqtt_packet_type_t expected_type;
  int expects_response;
} flowie_mqtt_test_completion_t;

typedef struct flowie_mqtt_test_shutdown_s {
  int count;
  int statuses[2];
} flowie_mqtt_test_shutdown_t;

static int flowie_mqtt_test_next_packet(flowie_mqtt_test_broker_stream_t *stream,
                                        flowie_mqtt_version_t version,
                                        flowie_mqtt_packet_view_t *out) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  if (!stream || !stream->socket || !out) return TURBO_EINVAL;
  if (stream->pending_size != 0u) {
    if (stream->pending_size > stream->size) return TURBO_EPROTO;
    memmove(stream->bytes, stream->bytes + stream->pending_size,
            stream->size - stream->pending_size);
    stream->size -= stream->pending_size;
    stream->pending_size = 0u;
  }
  options.version = version;
  options.max_packet_size = sizeof(stream->bytes);
  for (;;) {
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    size_t consumed = 0u;
    int rc;
    if (stream->size != 0u) {
      rc =
          flowie_mqtt_packet_parse(stream->bytes, stream->size, &options, &packet, &consumed, NULL);
      if (rc == FLOWIE_MQTT_PARSE_OK) {
        if (consumed == 0u || consumed > stream->size) return TURBO_EPROTO;
        stream->pending_size = consumed;
        *out = packet;
        return TURBO_OK;
      }
      if (rc != FLOWIE_MQTT_PARSE_NEED_MORE) return TURBO_EPROTO;
    }
    {
      char *received = NULL;
      size_t received_size = 0u;
      rc = coro_socket_recv(stream->socket, &received, &received_size);
      if (rc != TURBO_OK) return rc;
      if (!received || received_size == 0u ||
          received_size > sizeof(stream->bytes) - stream->size) {
        if (received) coro_socket_free_recv(received);
        return TURBO_ECONNRESET;
      }
      memcpy(stream->bytes + stream->size, received, received_size);
      stream->size += received_size;
      coro_socket_free_recv(received);
    }
  }
}

static int flowie_mqtt_test_send_control(coro_socket_t *socket, flowie_mqtt_version_t version,
                                         flowie_mqtt_packet_type_t type, uint16_t packet_id,
                                         uint8_t reason_code, flowie_mqtt_span_t reason_codes) {
  flowie_mqtt_control_packet_t packet = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  uint8_t encoded[64];
  size_t written = 0u;
  int rc;
  packet.version = version;
  packet.type = type;
  packet.packet_id = packet_id;
  packet.reason_code = reason_code;
  packet.reason_codes = reason_codes;
  rc = flowie_mqtt_control_packet_encode(&packet, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  return coro_socket_send(socket, (const char *)encoded, written);
}

static int flowie_mqtt_test_expect_type(flowie_mqtt_test_broker_stream_t *stream,
                                        flowie_mqtt_version_t version,
                                        flowie_mqtt_packet_type_t expected,
                                        flowie_mqtt_packet_view_t *packet) {
  int rc = flowie_mqtt_test_next_packet(stream, version, packet);
  if (rc != TURBO_OK) return rc;
  return packet->type == expected ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_mqtt_test_broker_run(coro_socket_t *socket, flowie_mqtt_test_state_t *state) {
  static const uint8_t inbound_topic[] = "server/topic";
  static const uint8_t inbound_payload[] = "from-broker";
  flowie_mqtt_test_broker_stream_t stream = {0};
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_mqtt_publish_packet_t outbound = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  uint8_t encoded[128];
  uint8_t granted_qos = 1u;
  uint8_t unsubscribe_reason = 0u;
  flowie_mqtt_span_t subscribe_reasons = {&granted_qos, 1u};
  flowie_mqtt_span_t unsubscribe_reasons = {&unsubscribe_reason, 1u};
  size_t written = 0u;
  uint16_t packet_id;
  int rc;

  stream.socket = socket;
  coro_socket_set_timeout(socket, FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS);

  rc = flowie_mqtt_test_expect_type(&stream, FLOWIE_MQTT_VERSION_UNSPECIFIED,
                                    FLOWIE_MQTT_PACKET_CONNECT, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_connect_parse(&packet, &connect);
  if (rc != FLOWIE_MQTT_PARSE_OK || connect.version != state->version) return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_CONNACK, 0u, 0u,
                                     (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_SUBSCRIBE, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_subscribe_parse(&packet, &subscribe);
  if (rc != FLOWIE_MQTT_PARSE_OK || subscribe.packet_id != 1u || subscribe.entry_count != 1u)
    return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_SUBACK,
                                     subscribe.packet_id, 0u, subscribe_reasons);
  if (rc != TURBO_OK) return rc;

  outbound.version = state->version;
  outbound.qos = 1u;
  outbound.packet_id = 77u;
  outbound.topic = (flowie_mqtt_span_t){inbound_topic, sizeof(inbound_topic) - 1u};
  outbound.payload = (flowie_mqtt_span_t){inbound_payload, sizeof(inbound_payload) - 1u};
  rc = flowie_mqtt_publish_packet_encode(&outbound, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  rc = coro_socket_send(socket, (const char *)encoded, written);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PUBACK, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_control_packet_parse(&packet, &control);
  if (rc != FLOWIE_MQTT_PARSE_OK || control.packet_id != 77u) return TURBO_EPROTO;

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PUBLISH, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_publish_parse(&packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK || publish.qos != 1u || publish.packet_id != 2u)
    return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_PUBACK,
                                     publish.packet_id, 0u, (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  publish = (flowie_mqtt_publish_view_t)FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PUBLISH, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_publish_parse(&packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK || publish.qos != 2u || publish.packet_id != 3u)
    return TURBO_EPROTO;
  packet_id = publish.packet_id;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_PUBREC, packet_id,
                                     0u, (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;
  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  control = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PUBREL, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_control_packet_parse(&packet, &control);
  if (rc != FLOWIE_MQTT_PARSE_OK || control.packet_id != packet_id) return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_PUBCOMP, packet_id,
                                     0u, (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_UNSUBSCRIBE,
                                    &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_unsubscribe_parse(&packet, &unsubscribe);
  if (rc != FLOWIE_MQTT_PARSE_OK || unsubscribe.packet_id != 4u || unsubscribe.filter_count != 1u)
    return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_control(
      socket, state->version, FLOWIE_MQTT_PACKET_UNSUBACK, unsubscribe.packet_id, 0u,
      state->version == FLOWIE_MQTT_VERSION_5 ? unsubscribe_reasons : (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PINGREQ, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_PINGRESP, 0u, 0u,
                                     (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  if (!state->expect_disconnect) {
    coro_sleep(coro_context_current(), FLOWIE_MQTT_CLIENT_TEST_MANAGED_DISCONNECT_GRACE_MS);
    return TURBO_OK;
  }

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  return flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_DISCONNECT,
                                      &packet);
}

static void flowie_mqtt_test_broker_handler(coro_socket_t *socket, void *arg) {
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)arg;
  state->server_rc = flowie_mqtt_test_broker_run(socket, state);
  if (state->server_rc == TURBO_OK)
    coro_sleep(coro_context_current(), FLOWIE_MQTT_CLIENT_TEST_CLOSE_GRACE_MS);
  state->server_done = 1;
}

static int flowie_mqtt_test_on_publish(flowie_mqtt_client_t *client,
                                       const flowie_mqtt_publish_view_t *publish, void *user_data) {
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)user_data;
  (void)client;
  if (!publish || publish->topic.size >= sizeof(state->topic) ||
      publish->payload.size >= sizeof(state->payload))
    return TURBO_EMSGSIZE;
  memcpy(state->topic, publish->topic.data, publish->topic.size);
  state->topic[publish->topic.size] = '\0';
  memcpy(state->payload, publish->payload.data, publish->payload.size);
  state->payload[publish->payload.size] = '\0';
  ++state->publish_count;
  return TURBO_OK;
}

static void flowie_mqtt_test_managed_completion(flowie_mqtt_client_t *client, int status,
                                                const flowie_mqtt_control_packet_view_t *response,
                                                void *user_data) {
  flowie_mqtt_test_completion_t *expected = (flowie_mqtt_test_completion_t *)user_data;
  int error = status;
  (void)client;
  if (error == TURBO_OK && expected->expects_response &&
      (!response || response->type != expected->expected_type))
    error = TURBO_EPROTO;
  if (error == TURBO_OK && !expected->expects_response && response) error = TURBO_EPROTO;
  if (error != TURBO_OK) {
    int unset = TURBO_OK;
    (void)atomic_compare_exchange_strong_explicit(&expected->state->managed_error, &unset, error,
                                                  memory_order_acq_rel, memory_order_acquire);
  }
  atomic_fetch_add_explicit(&expected->state->managed_completions, 1, memory_order_release);
}

static void flowie_mqtt_test_managed_disconnect(flowie_mqtt_client_t *client, int status,
                                                void *user_data) {
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)user_data;
  (void)client;
  atomic_store_explicit(&state->managed_disconnect_error, status, memory_order_release);
}

static void flowie_mqtt_test_count_completion(flowie_mqtt_client_t *client, int status,
                                              const flowie_mqtt_control_packet_view_t *response,
                                              void *user_data) {
  int *count = (int *)user_data;
  (void)client;
  (void)status;
  (void)response;
  ++*count;
}

static void flowie_mqtt_test_shutdown_completion(flowie_mqtt_client_t *client, int status,
                                                 const flowie_mqtt_control_packet_view_t *response,
                                                 void *user_data) {
  flowie_mqtt_test_shutdown_t *shutdown = (flowie_mqtt_test_shutdown_t *)user_data;
  (void)client;
  (void)response;
  if (shutdown->count < (int)(sizeof(shutdown->statuses) / sizeof(shutdown->statuses[0])))
    shutdown->statuses[shutdown->count] = status;
  ++shutdown->count;
}

static void flowie_mqtt_test_client_task(coro_t *co, void *arg) {
  static const uint8_t client_id[] = "flowie-test-client";
  static const uint8_t filter_bytes[] = "client/topic";
  static const uint8_t payload_bytes[] = "from-client";
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)arg;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_control_packet_view_t response = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_mqtt_span_t filter = {filter_bytes, sizeof(filter_bytes) - 1u};
  flowie_mqtt_packet_type_t received_type = (flowie_mqtt_packet_type_t)0;
  int rc;
  (void)co;

  connect.version = state->version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  rc = flowie_mqtt_client_connect(state->client, &connect, &response);
  if (rc != TURBO_OK || response.reason_code != 0u ||
      !flowie_mqtt_client_is_connected(state->client))
    goto done;

  subscription.filter = filter;
  subscription.qos = 1u;
  subscribe.version = state->version;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  response = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_client_subscribe(state->client, &subscribe, &response);
  if (rc != TURBO_OK || response.reason_codes.size != 1u || response.reason_codes.data[0] != 1u)
    goto done;

  rc = flowie_mqtt_client_poll(state->client, &received_type);
  if (rc != TURBO_OK || received_type != FLOWIE_MQTT_PACKET_PUBLISH) goto done;

  publish.version = state->version;
  publish.qos = 1u;
  publish.topic = filter;
  publish.payload = (flowie_mqtt_span_t){payload_bytes, sizeof(payload_bytes) - 1u};
  response = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_client_publish(state->client, &publish, &response);
  if (rc != TURBO_OK || response.type != FLOWIE_MQTT_PACKET_PUBACK || response.packet_id != 2u)
    goto done;

  publish.qos = 2u;
  response = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_client_publish(state->client, &publish, &response);
  if (rc != TURBO_OK || response.type != FLOWIE_MQTT_PACKET_PUBCOMP || response.packet_id != 3u)
    goto done;

  unsubscribe.version = state->version;
  unsubscribe.filters = &filter;
  unsubscribe.filter_count = 1u;
  response = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_client_unsubscribe(state->client, &unsubscribe, &response);
  if (rc != TURBO_OK || response.packet_id != 4u ||
      (state->version == FLOWIE_MQTT_VERSION_5 && response.reason_codes.size != 1u))
    goto done;

  rc = flowie_mqtt_client_ping(state->client);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_disconnect(state->client, 0u, (flowie_mqtt_span_t){0});

done:
  if (rc != TURBO_OK && flowie_mqtt_client_is_connected(state->client))
    (void)flowie_mqtt_client_disconnect(state->client, 0u, (flowie_mqtt_span_t){0});
  state->client_rc = rc;
  state->client_done = 1;
}

static int flowie_mqtt_test_run(flowie_mqtt_version_t version, flowie_mqtt_test_state_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  coro_socket_t *server = NULL;
  unsigned short port = flowie_test_port();
  uint64_t deadline;
  int rc = TURBO_OK;

  if (port == 0u) return TURBO_EIO;
  memset(state, 0, sizeof(*state));
  state->version = version;
  state->expect_disconnect = 1;
  state->client_rc = TURBO_EBUSY;
  state->server_rc = TURBO_EBUSY;
  state->context = coro_context_create(NULL);
  if (!state->context) return TURBO_ENOMEM;
  server = coro_socket_create_tcpv4(state->context);
  if (!server) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  config.context = state->context;
  config.host = "127.0.0.1";
  config.port = port;
  config.timeout_ms = FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS;
  config.on_publish = flowie_mqtt_test_on_publish;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) goto done;
  rc = coro_socket_listen_on(server, config.host, port, flowie_mqtt_test_broker_handler, state);
  if (rc != TURBO_OK) goto done;
  rc = coro_context_spawn(state->context, flowie_mqtt_test_client_task, state);
  if (rc != TURBO_OK) goto done;

  deadline = turbo_monotonic_ms() + FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS * 2u;
  while ((!state->client_done || !state->server_done) && turbo_monotonic_ms() < deadline)
    (void)coro_context_run(state->context, TURBO_RUN_ONCE);
  if (!state->client_done || !state->server_done) rc = TURBO_ETIMEDOUT;
  else if (state->client_rc != TURBO_OK) rc = state->client_rc;
  else rc = state->server_rc;

done:
  if (server) coro_socket_destroy(server);
  flowie_mqtt_client_destroy(state->client);
  coro_context_destroy(state->context);
  state->client = NULL;
  state->context = NULL;
  return rc;
}

static int flowie_mqtt_test_run_managed(flowie_mqtt_version_t version,
                                        flowie_mqtt_test_state_t *state) {
  enum { FLOWIE_MQTT_MANAGED_COMMAND_COUNT = 7 };
  uint8_t client_id[] = "flowie-managed-client";
  uint8_t filter_bytes[] = "client/topic";
  uint8_t payload_bytes[] = "from-client";
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_span_t filter = {filter_bytes, sizeof(filter_bytes) - 1u};
  flowie_mqtt_test_completion_t completions[FLOWIE_MQTT_MANAGED_COMMAND_COUNT] = {
      {state, FLOWIE_MQTT_PACKET_CONNACK, 1},   {state, FLOWIE_MQTT_PACKET_SUBACK, 1},
      {state, FLOWIE_MQTT_PACKET_PUBACK, 1},    {state, FLOWIE_MQTT_PACKET_PUBCOMP, 1},
      {state, FLOWIE_MQTT_PACKET_UNSUBACK, 1},  {state, (flowie_mqtt_packet_type_t)0, 0},
      {state, FLOWIE_MQTT_PACKET_DISCONNECT, 0}};
  coro_socket_t *server = NULL;
  unsigned short port = flowie_test_port();
  uint64_t deadline;
  int rc = TURBO_OK;

  if (port == 0u) return TURBO_EIO;
  memset(state, 0, sizeof(*state));
  state->version = version;
  state->expect_disconnect = 0;
  state->server_rc = TURBO_EBUSY;
  atomic_init(&state->managed_completions, 0);
  atomic_init(&state->managed_error, TURBO_OK);
  atomic_init(&state->managed_disconnect_error, TURBO_OK);
  state->context = coro_context_create(NULL);
  if (!state->context) return TURBO_ENOMEM;
  server = coro_socket_create_tcpv4(state->context);
  if (!server) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  config.host = "127.0.0.1";
  config.port = port;
  config.timeout_ms = FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS;
  config.command_queue_capacity = 8u;
  config.on_publish = flowie_mqtt_test_on_publish;
  config.on_disconnect = flowie_mqtt_test_managed_disconnect;
  config.user_data = state;
  rc = coro_socket_listen_on(server, config.host, port, flowie_mqtt_test_broker_handler, state);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) goto done;
  if (!flowie_mqtt_client_is_managed(state->client)) {
    rc = TURBO_EPROTO;
    goto done;
  }

  connect.version = version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  subscription.filter = filter;
  subscription.qos = 1u;
  subscribe.version = version;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  publish.version = version;
  publish.qos = 1u;
  publish.topic = filter;
  publish.payload = (flowie_mqtt_span_t){payload_bytes, sizeof(payload_bytes) - 1u};
  unsubscribe.version = version;
  unsubscribe.filters = &filter;
  unsubscribe.filter_count = 1u;

  rc = flowie_mqtt_client_connect_async(state->client, &connect,
                                        flowie_mqtt_test_managed_completion, &completions[0]);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_subscribe_async(state->client, &subscribe,
                                          flowie_mqtt_test_managed_completion, &completions[1]);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_publish_async(state->client, &publish,
                                        flowie_mqtt_test_managed_completion, &completions[2]);
  if (rc != TURBO_OK) goto done;
  publish.qos = 2u;
  rc = flowie_mqtt_client_publish_async(state->client, &publish,
                                        flowie_mqtt_test_managed_completion, &completions[3]);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_unsubscribe_async(state->client, &unsubscribe,
                                            flowie_mqtt_test_managed_completion, &completions[4]);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_ping_async(state->client, flowie_mqtt_test_managed_completion,
                                     &completions[5]);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_disconnect_async(state->client, 0u, (flowie_mqtt_span_t){0},
                                           flowie_mqtt_test_managed_completion, &completions[6]);
  if (rc != TURBO_OK) goto done;

  memset(client_id, 0, sizeof(client_id));
  memset(filter_bytes, 0, sizeof(filter_bytes));
  memset(payload_bytes, 0, sizeof(payload_bytes));
  deadline = turbo_monotonic_ms() + FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS * 2u;
  while ((!state->server_done ||
          atomic_load_explicit(&state->managed_completions, memory_order_acquire) !=
              FLOWIE_MQTT_MANAGED_COMMAND_COUNT) &&
         turbo_monotonic_ms() < deadline)
    (void)coro_context_run(state->context, TURBO_RUN_ONCE);
  if (!state->server_done ||
      atomic_load_explicit(&state->managed_completions, memory_order_acquire) !=
          FLOWIE_MQTT_MANAGED_COMMAND_COUNT)
    rc = TURBO_ETIMEDOUT;
  else if (atomic_load_explicit(&state->managed_error, memory_order_acquire) != TURBO_OK)
    rc = atomic_load_explicit(&state->managed_error, memory_order_acquire);
  else if (atomic_load_explicit(&state->managed_disconnect_error, memory_order_acquire) != TURBO_OK)
    rc = atomic_load_explicit(&state->managed_disconnect_error, memory_order_acquire);
  else rc = state->server_rc;
  if (rc == TURBO_EOF) rc = TURBO_OK;

done:
  if (server) coro_socket_destroy(server);
  flowie_mqtt_client_destroy(state->client);
  coro_context_destroy(state->context);
  state->client = NULL;
  state->context = NULL;
  return rc;
}

spec("flowie mqtt coroutine client") {
  it("validates configuration and starts disconnected") {
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_client_t *client = (flowie_mqtt_client_t *)(uintptr_t)1u;
    coro_context_t *context = coro_context_create(NULL);
    check_not_null(context);
    config.context = context;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_EINVAL);
    check_null(client);
    config.host = "127.0.0.1";
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    check_not_null(client);
    check_false(flowie_mqtt_client_is_connected(client));
    check_int_eq(flowie_mqtt_client_ping(client), TURBO_EBUSY);
    flowie_mqtt_client_destroy(client);

    config.size = offsetof(flowie_mqtt_client_config_t, command_queue_capacity);
    config.abi_version = FLOWIE_MQTT_CLIENT_ABI_V1;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    check_false(flowie_mqtt_client_is_managed(client));
    flowie_mqtt_client_destroy(client);
    coro_context_destroy(context);
  }

  it("rejects managed commands that exceed the configured queue byte budget") {
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_client_t *client = NULL;
    int completion_count = 0;
    config.host = "127.0.0.1";
    config.command_queue_max_bytes = 1u;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    check_true(flowie_mqtt_client_is_managed(client));
    check_int_eq(
        flowie_mqtt_client_ping_async(client, flowie_mqtt_test_count_completion, &completion_count),
        TURBO_ENOSPC);
    check_int_eq(completion_count, 0);
    flowie_mqtt_client_destroy(client);
  }

  it("cancels accepted managed commands before releasing its worker context") {
    static const uint8_t client_id[] = "flowie-shutdown-client";
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_client_t *client = NULL;
    flowie_mqtt_test_shutdown_t shutdown = {0};
    coro_context_t *server_context = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = flowie_test_port();
    check_not_null(server_context);
    check_uint_ne(port, 0u);
    server = coro_socket_create_tcpv4(server_context);
    check_not_null(server);
    check_int_eq(
        coro_socket_listen_on(server, "127.0.0.1", port, flowie_mqtt_test_broker_handler, NULL),
        TURBO_OK);
    config.host = "127.0.0.1";
    config.port = port;
    config.timeout_ms = FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    check_int_eq(flowie_mqtt_client_connect_async(client, &connect,
                                                  flowie_mqtt_test_shutdown_completion, &shutdown),
                 TURBO_OK);
    check_int_eq(
        flowie_mqtt_client_ping_async(client, flowie_mqtt_test_shutdown_completion, &shutdown),
        TURBO_OK);
    flowie_mqtt_client_destroy(client);
    check_int_eq(shutdown.count, 2);
    check_int_eq(shutdown.statuses[0], TURBO_ESHUTDOWN);
    check_int_eq(shutdown.statuses[1], TURBO_ESHUTDOWN);
    coro_socket_destroy(server);
    coro_context_destroy(server_context);
  }

  it("completes MQTT 3.1.1 client exchanges against a local broker") {
    flowie_mqtt_test_state_t state;
    check_int_eq(flowie_mqtt_test_run(FLOWIE_MQTT_VERSION_3_1_1, &state), TURBO_OK);
    check_int_eq(state.publish_count, 1);
    check_str_eq(state.topic, "server/topic");
    check_str_eq(state.payload, "from-broker");
  }

  it("completes MQTT 5 client exchanges against a local broker") {
    flowie_mqtt_test_state_t state;
    check_int_eq(flowie_mqtt_test_run(FLOWIE_MQTT_VERSION_5, &state), TURBO_OK);
    check_int_eq(state.publish_count, 1);
    check_str_eq(state.topic, "server/topic");
    check_str_eq(state.payload, "from-broker");
  }

  it("owns CoroNet and processes managed MQTT 5 commands on its DLL worker") {
    flowie_mqtt_test_state_t state;
    check_int_eq(flowie_mqtt_test_run_managed(FLOWIE_MQTT_VERSION_5, &state), TURBO_OK);
    check_int_eq(state.publish_count, 1);
    check_str_eq(state.topic, "server/topic");
    check_str_eq(state.payload, "from-broker");
  }
}
