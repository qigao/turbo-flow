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
#define FLOWIE_MQTT_CLIENT_TEST_CALLBACK_DISCONNECT_GRACE_MS 50u

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
  int server_rc;
  int server_done;
  int publish_count;
  int secondary_match_count;
  char topics[2][32];
  char payloads[2][32];
  int expect_disconnect;
  atomic_int completions;
  atomic_int publish_completions;
  atomic_int completion_error;
  atomic_int background_error;
} flowie_mqtt_test_state_t;

typedef struct flowie_mqtt_test_shutdown_s {
  int count;
  int statuses[2];
} flowie_mqtt_test_shutdown_t;

typedef struct flowie_mqtt_test_error_state_s {
  atomic_int connect_done;
  atomic_int connect_status;
  atomic_int error_count;
  atomic_int error_status;
  int server_status;
  int server_done;
} flowie_mqtt_test_error_state_t;

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

static int flowie_mqtt_test_send_publish(flowie_mqtt_test_broker_stream_t *stream,
                                         flowie_mqtt_version_t version, uint16_t packet_id,
                                         flowie_mqtt_span_t topic, flowie_mqtt_span_t payload) {
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  uint8_t encoded[128];
  size_t written = 0u;
  int rc;
  publish.version = version;
  publish.qos = 1u;
  publish.packet_id = packet_id;
  publish.topic = topic;
  publish.payload = payload;
  rc = flowie_mqtt_publish_packet_encode(&publish, encoded, sizeof(encoded), &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  rc = coro_socket_send(stream->socket, (const char *)encoded, written);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_test_expect_type(stream, version, FLOWIE_MQTT_PACKET_PUBACK, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_control_packet_parse(&packet, &control);
  return rc == FLOWIE_MQTT_PARSE_OK && control.packet_id == packet_id ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_mqtt_test_broker_run(coro_socket_t *socket, flowie_mqtt_test_state_t *state) {
  static const uint8_t inbound_topics[2][17] = {"server/topic/one", "server/topic/two"};
  static const uint8_t inbound_payloads[2][16] = {"from-broker-one", "from-broker-two"};
  static const uint8_t outbound_topics[2][17] = {"client/topic/one", "client/topic/two"};
  static const uint8_t outbound_payloads[2][16] = {"from-client-one", "from-client-two"};
  flowie_mqtt_test_broker_stream_t stream = {0};
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  uint8_t granted_qos[2] = {1u, 1u};
  uint8_t unsubscribe_reason[2] = {0u, 0u};
  flowie_mqtt_span_t subscribe_reasons = {granted_qos, 2u};
  flowie_mqtt_span_t unsubscribe_reasons = {unsubscribe_reason, 2u};
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
  if (rc != FLOWIE_MQTT_PARSE_OK || subscribe.packet_id != 1u || subscribe.entry_count != 2u)
    return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_SUBACK,
                                     subscribe.packet_id, 0u, subscribe_reasons);
  if (rc != TURBO_OK) return rc;

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PUBLISH, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_publish_parse(&packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK || publish.qos != 1u || publish.packet_id != 2u ||
      publish.topic.size != sizeof(outbound_topics[0]) - 1u ||
      memcmp(publish.topic.data, outbound_topics[0], publish.topic.size) != 0 ||
      publish.payload.size != sizeof(outbound_payloads[0]) - 1u ||
      memcmp(publish.payload.data, outbound_payloads[0], publish.payload.size) != 0)
    return TURBO_EPROTO;
  rc = flowie_mqtt_test_send_publish(
      &stream, state->version, 77u,
      (flowie_mqtt_span_t){inbound_topics[0], sizeof(inbound_topics[0]) - 1u},
      (flowie_mqtt_span_t){inbound_payloads[0], sizeof(inbound_payloads[0]) - 1u});
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_test_send_control(socket, state->version, FLOWIE_MQTT_PACKET_PUBACK,
                                     publish.packet_id, 0u, (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  publish = (flowie_mqtt_publish_view_t)FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  rc = flowie_mqtt_test_expect_type(&stream, state->version, FLOWIE_MQTT_PACKET_PUBLISH, &packet);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_publish_parse(&packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK || publish.qos != 2u || publish.packet_id != 3u ||
      publish.topic.size != sizeof(outbound_topics[1]) - 1u ||
      memcmp(publish.topic.data, outbound_topics[1], publish.topic.size) != 0 ||
      publish.payload.size != sizeof(outbound_payloads[1]) - 1u ||
      memcmp(publish.payload.data, outbound_payloads[1], publish.payload.size) != 0)
    return TURBO_EPROTO;
  packet_id = publish.packet_id;
  rc = flowie_mqtt_test_send_publish(
      &stream, state->version, 78u,
      (flowie_mqtt_span_t){inbound_topics[1], sizeof(inbound_topics[1]) - 1u},
      (flowie_mqtt_span_t){inbound_payloads[1], sizeof(inbound_payloads[1]) - 1u});
  if (rc != TURBO_OK) return rc;
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
  if (rc != FLOWIE_MQTT_PARSE_OK || unsubscribe.packet_id != 4u || unsubscribe.filter_count != 2u)
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
    coro_sleep(coro_context_current(), FLOWIE_MQTT_CLIENT_TEST_CALLBACK_DISCONNECT_GRACE_MS);
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
  int index = state->publish_count;
  (void)client;
  if (!publish || index < 0 || index >= 2 || publish->topic.size >= sizeof(state->topics[index]) ||
      publish->payload.size >= sizeof(state->payloads[index]))
    return TURBO_EMSGSIZE;
  memcpy(state->topics[index], publish->topic.data, publish->topic.size);
  state->topics[index][publish->topic.size] = '\0';
  memcpy(state->payloads[index], publish->payload.data, publish->payload.size);
  state->payloads[index][publish->payload.size] = '\0';
  ++state->publish_count;
  return TURBO_OK;
}

static int flowie_mqtt_test_on_secondary_match(flowie_mqtt_client_t *client,
                                               const flowie_mqtt_publish_view_t *publish,
                                               void *user_data) {
  static const uint8_t expected_topic[] = "server/topic/two";
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)user_data;
  (void)client;
  if (!publish || publish->topic.size != sizeof(expected_topic) - 1u ||
      memcmp(publish->topic.data, expected_topic, sizeof(expected_topic) - 1u) != 0)
    return TURBO_EPROTO;
  ++state->secondary_match_count;
  return TURBO_OK;
}

static void flowie_mqtt_test_record_completion(flowie_mqtt_test_state_t *state, int status,
                                               const flowie_mqtt_control_packet_view_t *response,
                                               flowie_mqtt_packet_type_t expected_type,
                                               int expects_response) {
  int error = status;
  if (error == TURBO_OK && expects_response && (!response || response->type != expected_type))
    error = TURBO_EPROTO;
  if (error == TURBO_OK && !expects_response && response) error = TURBO_EPROTO;
  if (error != TURBO_OK) {
    int unset = TURBO_OK;
    (void)atomic_compare_exchange_strong_explicit(&state->completion_error, &unset, error,
                                                  memory_order_acq_rel, memory_order_acquire);
  }
  atomic_fetch_add_explicit(&state->completions, 1, memory_order_release);
}

static void flowie_mqtt_test_connect_completion(flowie_mqtt_client_t *client, int status,
                                                const flowie_mqtt_control_packet_view_t *response,
                                                void *user_data) {
  (void)client;
  flowie_mqtt_test_record_completion((flowie_mqtt_test_state_t *)user_data, status, response,
                                     FLOWIE_MQTT_PACKET_CONNACK, 1);
}

static void flowie_mqtt_test_subscribe_completion(flowie_mqtt_client_t *client, int status,
                                                  const flowie_mqtt_control_packet_view_t *response,
                                                  void *user_data) {
  (void)client;
  flowie_mqtt_test_record_completion((flowie_mqtt_test_state_t *)user_data, status, response,
                                     FLOWIE_MQTT_PACKET_SUBACK, 1);
}

static void flowie_mqtt_test_publish_completion(flowie_mqtt_client_t *client, int status,
                                                const flowie_mqtt_control_packet_view_t *response,
                                                void *user_data) {
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)user_data;
  int index = atomic_fetch_add_explicit(&state->publish_completions, 1, memory_order_relaxed);
  flowie_mqtt_packet_type_t expected =
      index == 0 ? FLOWIE_MQTT_PACKET_PUBACK : FLOWIE_MQTT_PACKET_PUBCOMP;
  (void)client;
  if (index > 1) status = TURBO_EPROTO;
  flowie_mqtt_test_record_completion(state, status, response, expected, 1);
}

static void
flowie_mqtt_test_unsubscribe_completion(flowie_mqtt_client_t *client, int status,
                                        const flowie_mqtt_control_packet_view_t *response,
                                        void *user_data) {
  (void)client;
  flowie_mqtt_test_record_completion((flowie_mqtt_test_state_t *)user_data, status, response,
                                     FLOWIE_MQTT_PACKET_UNSUBACK, 1);
}

static void flowie_mqtt_test_ping_completion(flowie_mqtt_client_t *client, int status,
                                             const flowie_mqtt_control_packet_view_t *response,
                                             void *user_data) {
  (void)client;
  flowie_mqtt_test_record_completion((flowie_mqtt_test_state_t *)user_data, status, response,
                                     (flowie_mqtt_packet_type_t)0, 0);
}

static void
flowie_mqtt_test_disconnect_completion(flowie_mqtt_client_t *client, int status,
                                       const flowie_mqtt_control_packet_view_t *response,
                                       void *user_data) {
  (void)client;
  flowie_mqtt_test_record_completion((flowie_mqtt_test_state_t *)user_data, status, response,
                                     (flowie_mqtt_packet_type_t)0, 0);
}

static void flowie_mqtt_test_error(flowie_mqtt_client_t *client, int status, void *user_data) {
  flowie_mqtt_test_state_t *state = (flowie_mqtt_test_state_t *)user_data;
  (void)client;
  atomic_store_explicit(&state->background_error, status, memory_order_release);
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

static void
flowie_mqtt_test_error_connect_completion(flowie_mqtt_client_t *client, int status,
                                          const flowie_mqtt_control_packet_view_t *response,
                                          void *user_data) {
  flowie_mqtt_test_error_state_t *state = (flowie_mqtt_test_error_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK || response->reason_code != 0u))
    status = TURBO_EPROTO;
  atomic_store_explicit(&state->connect_status, status, memory_order_relaxed);
  atomic_store_explicit(&state->connect_done, 1, memory_order_release);
}

static void flowie_mqtt_test_background_error(flowie_mqtt_client_t *client, int status,
                                              void *user_data) {
  flowie_mqtt_test_error_state_t *state = (flowie_mqtt_test_error_state_t *)user_data;
  (void)client;
  atomic_store_explicit(&state->error_status, status, memory_order_relaxed);
  atomic_fetch_add_explicit(&state->error_count, 1, memory_order_release);
}

static void flowie_mqtt_test_closing_broker_handler(coro_socket_t *socket, void *arg) {
  flowie_mqtt_test_error_state_t *state = (flowie_mqtt_test_error_state_t *)arg;
  flowie_mqtt_test_broker_stream_t stream = {0};
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  int rc;
  stream.socket = socket;
  coro_socket_set_timeout(socket, FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS);
  rc = flowie_mqtt_test_expect_type(&stream, FLOWIE_MQTT_VERSION_UNSPECIFIED,
                                    FLOWIE_MQTT_PACKET_CONNECT, &packet);
  if (rc == TURBO_OK && (flowie_mqtt_connect_parse(&packet, &connect) != FLOWIE_MQTT_PARSE_OK ||
                         connect.version != FLOWIE_MQTT_VERSION_5))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_mqtt_test_send_control(socket, FLOWIE_MQTT_VERSION_5, FLOWIE_MQTT_PACKET_CONNACK,
                                       0u, 0u, (flowie_mqtt_span_t){0});
  if (rc == TURBO_OK) coro_sleep(coro_context_current(), FLOWIE_MQTT_CLIENT_TEST_CLOSE_GRACE_MS);
  state->server_status = rc;
  state->server_done = 1;
}

static int flowie_mqtt_test_run_callbacks(flowie_mqtt_version_t version,
                                          flowie_mqtt_test_state_t *state) {
  enum { FLOWIE_MQTT_MANAGED_COMMAND_COUNT = 7 };
  uint8_t client_id[] = "flowie-callback-client";
  uint8_t filter_one[] = "client/topic/one";
  uint8_t filter_two[] = "client/topic/two";
  uint8_t inbound_filter[] = "server/topic/+";
  uint8_t exact_filter[] = "server/topic/two";
  uint8_t payload_one[] = "from-client-one";
  uint8_t payload_two[] = "from-client-two";
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_client_topic_handler_t topic_handlers[2] = {{0}};
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_subscription_t subscriptions[2] = {{0}};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_client_publish_topic_t publish_topics[2] = {{0}};
  flowie_mqtt_client_publish_topic_vec_t publish = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_span_t filters[2] = {{filter_one, sizeof(filter_one) - 1u},
                                   {filter_two, sizeof(filter_two) - 1u}};
  coro_socket_t *server = NULL;
  unsigned short port = flowie_test_port();
  uint64_t deadline;
  int rc = TURBO_OK;

  if (port == 0u) return TURBO_EIO;
  memset(state, 0, sizeof(*state));
  state->version = version;
  state->expect_disconnect = 0;
  state->server_rc = TURBO_EBUSY;
  atomic_init(&state->completions, 0);
  atomic_init(&state->publish_completions, 0);
  atomic_init(&state->completion_error, TURBO_OK);
  atomic_init(&state->background_error, TURBO_OK);
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
  topic_handlers[0].filter = (flowie_mqtt_span_t){inbound_filter, sizeof(inbound_filter) - 1u};
  topic_handlers[0].on_message = flowie_mqtt_test_on_publish;
  topic_handlers[1].filter =
      (flowie_mqtt_span_t){exact_filter, sizeof(exact_filter) - 1u};
  topic_handlers[1].on_message = flowie_mqtt_test_on_secondary_match;
  config.topic_handlers =
      (flowie_mqtt_client_topic_handler_map_t){topic_handlers, 2u};
  config.on_connect = flowie_mqtt_test_connect_completion;
  config.on_publish = flowie_mqtt_test_publish_completion;
  config.on_subscribe = flowie_mqtt_test_subscribe_completion;
  config.on_unsubscribe = flowie_mqtt_test_unsubscribe_completion;
  config.on_ping = flowie_mqtt_test_ping_completion;
  config.on_disconnect = flowie_mqtt_test_disconnect_completion;
  config.on_error = flowie_mqtt_test_error;
  config.user_data = state;
  rc = coro_socket_listen_on(server, config.host, port, flowie_mqtt_test_broker_handler, state);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) goto done;
  connect.version = version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  subscriptions[0].filter = filters[0];
  subscriptions[0].qos = 1u;
  subscriptions[1].filter = filters[1];
  subscriptions[1].qos = 1u;
  subscribe.version = version;
  subscribe.subscriptions = subscriptions;
  subscribe.subscription_count = 2u;
  publish.version = version;
  publish.data = publish_topics;
  publish.count = 2u;
  publish_topics[0].qos = 1u;
  publish_topics[0].topic = filters[0];
  publish_topics[0].payload = (flowie_mqtt_span_t){payload_one, sizeof(payload_one) - 1u};
  publish_topics[1].qos = 2u;
  publish_topics[1].topic = filters[1];
  publish_topics[1].payload = (flowie_mqtt_span_t){payload_two, sizeof(payload_two) - 1u};
  unsubscribe.version = version;
  unsubscribe.filters = filters;
  unsubscribe.filter_count = 2u;

  rc = flowie_mqtt_client_connect(state->client, &connect);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_subscribe(state->client, &subscribe);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_publish(state->client, &publish);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_unsubscribe(state->client, &unsubscribe);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_ping(state->client);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_client_disconnect(state->client, 0u, (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) goto done;

  memset(client_id, 0, sizeof(client_id));
  memset(filter_one, 0, sizeof(filter_one));
  memset(filter_two, 0, sizeof(filter_two));
  memset(inbound_filter, 0, sizeof(inbound_filter));
  memset(exact_filter, 0, sizeof(exact_filter));
  memset(payload_one, 0, sizeof(payload_one));
  memset(payload_two, 0, sizeof(payload_two));
  deadline = turbo_monotonic_ms() + FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS * 2u;
  while ((!state->server_done || atomic_load_explicit(&state->completions, memory_order_acquire) !=
                                     FLOWIE_MQTT_MANAGED_COMMAND_COUNT) &&
         turbo_monotonic_ms() < deadline)
    (void)coro_context_run(state->context, TURBO_RUN_ONCE);
  if (!state->server_done || atomic_load_explicit(&state->completions, memory_order_acquire) !=
                                 FLOWIE_MQTT_MANAGED_COMMAND_COUNT)
    rc = TURBO_ETIMEDOUT;
  else if (atomic_load_explicit(&state->completion_error, memory_order_acquire) != TURBO_OK)
    rc = atomic_load_explicit(&state->completion_error, memory_order_acquire);
  else if (atomic_load_explicit(&state->background_error, memory_order_acquire) != TURBO_OK)
    rc = atomic_load_explicit(&state->background_error, memory_order_acquire);
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

spec("flowie mqtt callback client") {
  it("validates configuration and starts disconnected") {
    static const uint8_t duplicate_filter[] = "duplicate/#";
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_client_topic_handler_t duplicate_handlers[2] = {{0}};
    flowie_mqtt_client_t *client = (flowie_mqtt_client_t *)(uintptr_t)1u;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_EINVAL);
    check_null(client);
    config.host = "127.0.0.1";
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    check_not_null(client);
    check_false(flowie_mqtt_client_is_connected(client));
    check_int_eq(flowie_mqtt_client_ping(client), TURBO_ENOTSUP);
    flowie_mqtt_client_destroy(client);
    client = NULL;
    duplicate_handlers[0].filter =
        (flowie_mqtt_span_t){duplicate_filter, sizeof(duplicate_filter) - 1u};
    duplicate_handlers[0].on_message = flowie_mqtt_test_on_publish;
    duplicate_handlers[1] = duplicate_handlers[0];
    config.topic_handlers =
        (flowie_mqtt_client_topic_handler_map_t){duplicate_handlers, 2u};
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_EINVAL);
    check_null(client);
  }

  it("rejects commands that exceed the configured queue byte budget") {
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_client_t *client = NULL;
    int completion_count = 0;
    config.host = "127.0.0.1";
    config.command_queue_max_bytes = 1u;
    config.on_ping = flowie_mqtt_test_count_completion;
    config.user_data = &completion_count;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    check_int_eq(flowie_mqtt_client_ping(client), TURBO_ENOSPC);
    check_int_eq(completion_count, 0);
    flowie_mqtt_client_destroy(client);
  }

  it("rejects a topic vector atomically when the command queue is too small") {
    static const uint8_t topic_names[2][12] = {"batch/one", "batch/two"};
    static const uint8_t payloads[2][4] = {"one", "two"};
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_client_publish_topic_t topics[2] = {{0}};
    flowie_mqtt_client_publish_topic_vec_t topic_vec =
        FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
    flowie_mqtt_client_t *client = NULL;
    int completion_count = 0;
    config.host = "127.0.0.1";
    config.command_queue_capacity = 1u;
    config.on_publish = flowie_mqtt_test_count_completion;
    config.user_data = &completion_count;
    topics[0].topic = (flowie_mqtt_span_t){topic_names[0], strlen((const char *)topic_names[0])};
    topics[0].payload = (flowie_mqtt_span_t){payloads[0], strlen((const char *)payloads[0])};
    topics[1].topic = (flowie_mqtt_span_t){topic_names[1], strlen((const char *)topic_names[1])};
    topics[1].payload = (flowie_mqtt_span_t){payloads[1], strlen((const char *)payloads[1])};
    topic_vec.version = FLOWIE_MQTT_VERSION_5;
    topic_vec.data = topics;
    topic_vec.count = 2u;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    check_int_eq(flowie_mqtt_client_publish(client, &topic_vec), TURBO_ENOSPC);
    check_int_eq(completion_count, 0);
    flowie_mqtt_client_destroy(client);
  }

  it("cancels accepted commands before releasing its worker context") {
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
    config.on_connect = flowie_mqtt_test_shutdown_completion;
    config.on_ping = flowie_mqtt_test_shutdown_completion;
    config.user_data = &shutdown;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    check_int_eq(flowie_mqtt_client_connect(client, &connect), TURBO_OK);
    check_int_eq(flowie_mqtt_client_ping(client), TURBO_OK);
    flowie_mqtt_client_destroy(client);
    check_int_eq(shutdown.count, 2);
    check_int_eq(shutdown.statuses[0], TURBO_ESHUTDOWN);
    check_int_eq(shutdown.statuses[1], TURBO_ESHUTDOWN);
    coro_socket_destroy(server);
    coro_context_destroy(server_context);
  }

  it("reports an unsolicited connection close through the error callback") {
    static const uint8_t client_id[] = "flowie-error-client";
    flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_test_error_state_t state = {0};
    flowie_mqtt_client_t *client = NULL;
    coro_context_t *server_context = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = flowie_test_port();
    uint64_t deadline;
    int error_status;
    check_not_null(server_context);
    check_uint_ne(port, 0u);
    atomic_init(&state.connect_done, 0);
    atomic_init(&state.connect_status, TURBO_EBUSY);
    atomic_init(&state.error_count, 0);
    atomic_init(&state.error_status, TURBO_OK);
    state.server_status = TURBO_EBUSY;
    server = coro_socket_create_tcpv4(server_context);
    check_not_null(server);
    check_int_eq(coro_socket_listen_on(server, "127.0.0.1", port,
                                       flowie_mqtt_test_closing_broker_handler, &state),
                 TURBO_OK);
    config.host = "127.0.0.1";
    config.port = port;
    config.timeout_ms = FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS;
    config.on_connect = flowie_mqtt_test_error_connect_completion;
    config.on_error = flowie_mqtt_test_background_error;
    config.user_data = &state;
    check_int_eq(flowie_mqtt_client_create(&config, &client), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    check_int_eq(flowie_mqtt_client_connect(client, &connect), TURBO_OK);
    deadline = turbo_monotonic_ms() + FLOWIE_MQTT_CLIENT_TEST_TIMEOUT_MS * 2u;
    while ((!state.server_done ||
            atomic_load_explicit(&state.error_count, memory_order_acquire) == 0) &&
           turbo_monotonic_ms() < deadline)
      (void)coro_context_run(server_context, TURBO_RUN_ONCE);
    check_true(state.server_done);
    check_int_eq(state.server_status, TURBO_OK);
    check_true(atomic_load_explicit(&state.connect_done, memory_order_acquire));
    check_int_eq(atomic_load_explicit(&state.connect_status, memory_order_relaxed), TURBO_OK);
    check_int_eq(atomic_load_explicit(&state.error_count, memory_order_acquire), 1);
    error_status = atomic_load_explicit(&state.error_status, memory_order_relaxed);
    check_true(error_status == TURBO_EOF || error_status == TURBO_ECONNRESET);
    flowie_mqtt_client_destroy(client);
    coro_socket_destroy(server);
    coro_context_destroy(server_context);
  }

  it("completes MQTT 3.1.1 callbacks against a local broker") {
    flowie_mqtt_test_state_t state;
    check_int_eq(flowie_mqtt_test_run_callbacks(FLOWIE_MQTT_VERSION_3_1_1, &state), TURBO_OK);
    check_int_eq(state.publish_count, 2);
    check_str_eq(state.topics[0], "server/topic/one");
    check_str_eq(state.payloads[0], "from-broker-one");
    check_str_eq(state.topics[1], "server/topic/two");
    check_str_eq(state.payloads[1], "from-broker-two");
    check_int_eq(state.secondary_match_count, 1);
  }

  it("completes MQTT 5 callbacks against a local broker") {
    flowie_mqtt_test_state_t state;
    check_int_eq(flowie_mqtt_test_run_callbacks(FLOWIE_MQTT_VERSION_5, &state), TURBO_OK);
    check_int_eq(state.publish_count, 2);
    check_str_eq(state.topics[0], "server/topic/one");
    check_str_eq(state.payloads[0], "from-broker-one");
    check_str_eq(state.topics[1], "server/topic/two");
    check_str_eq(state.payloads[1], "from-broker-two");
    check_int_eq(state.secondary_match_count, 1);
  }
}
