#include "flowie_mqtt_client.h"

#include "platform.h"
#include "CoroNet/turbo_coro_context.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS 10000u
#define FLOWIE_MQTT_LIVE_TEST_TIMEOUT_MS 30000u
#define FLOWIE_MQTT_LIVE_BUFFER_SIZE 128u
#define FLOWIE_MQTT_LIVE_QOS_COUNT 3u
#define FLOWIE_MQTT_LIVE_RECEIVED_ALL ((1u << FLOWIE_MQTT_LIVE_QOS_COUNT) - 1u)

typedef struct flowie_mqtt_live_case_s {
  const char *name;
  const char *host;
  int port;
  const char *path;
  flowie_mqtt_client_transport_t transport;
  flowie_mqtt_version_t version;
} flowie_mqtt_live_case_t;

typedef struct flowie_mqtt_live_state_s {
  const flowie_mqtt_live_case_t *test_case;
  coro_context_t *context;
  flowie_mqtt_client_t *client;
  int result;
  int done;
  unsigned int received_mask;
  char client_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char payloads[FLOWIE_MQTT_LIVE_QOS_COUNT][FLOWIE_MQTT_LIVE_BUFFER_SIZE];
} flowie_mqtt_live_state_t;

static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_HIVEMQ_TCP_3 = {
    "HiveMQ TCP MQTT 3.1.1",          "broker.hivemq.com",      1883, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_HIVEMQ_WS_5 = {
    "HiveMQ WebSocket MQTT 5",       "broker.hivemq.com",  8000, "/mqtt",
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_TCP_3 = {
    "EMQX TCP MQTT 3.1.1",    "broker.emqx.io", 1883, NULL, FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,
    FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_TLS_5 = {
    "EMQX TLS MQTT 5",    "broker.emqx.io", 8883, NULL, FLOWIE_MQTT_CLIENT_TRANSPORT_TLS,
    FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_WS_5 = {
    "EMQX WebSocket MQTT 5",         "broker.emqx.io",     8083, "/mqtt",
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_WSS_5 = {
    "EMQX secure WebSocket MQTT 5",   "broker.emqx.io",     8084, "/mqtt",
    FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_MOSQUITTO_TCP_5 = {
    "Mosquitto TCP MQTT 5",           "test.mosquitto.org", 1883, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_MOSQUITTO_TLS_3 = {
    "Mosquitto TLS MQTT 3.1.1",       "test.mosquitto.org",     8886, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_MOSQUITTO_WS_5 = {
    "Mosquitto WebSocket MQTT 5",    "test.mosquitto.org", 8080, "/mqtt",
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS, FLOWIE_MQTT_VERSION_5};

static int flowie_mqtt_live_span_equals(flowie_mqtt_span_t span, const char *expected) {
  size_t expected_size = strlen(expected);
  return span.size == expected_size &&
         (expected_size == 0u || memcmp(span.data, expected, expected_size) == 0);
}

static int flowie_mqtt_live_on_publish(flowie_mqtt_client_t *client,
                                       const flowie_mqtt_publish_view_t *publish, void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  (void)client;
  if (!publish || !state) return TURBO_EINVAL;
  if (!flowie_mqtt_live_span_equals(publish->topic, state->topic)) return TURBO_OK;
  for (unsigned int qos = 0u; qos < FLOWIE_MQTT_LIVE_QOS_COUNT; ++qos) {
    if (!flowie_mqtt_live_span_equals(publish->payload, state->payloads[qos])) continue;
    if (publish->qos != qos) return TURBO_EPROTO;
    state->received_mask |= 1u << qos;
    return TURBO_OK;
  }
  return TURBO_OK;
}

static int flowie_mqtt_live_publish_all(flowie_mqtt_live_state_t *state, flowie_mqtt_span_t topic) {
  for (unsigned int qos = 0u; qos < FLOWIE_MQTT_LIVE_QOS_COUNT; ++qos) {
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_mqtt_control_packet_view_t response = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_mqtt_packet_type_t expected =
        qos == 1u ? FLOWIE_MQTT_PACKET_PUBACK : FLOWIE_MQTT_PACKET_PUBCOMP;
    int rc;
    publish.version = state->test_case->version;
    publish.qos = (uint8_t)qos;
    publish.topic = topic;
    publish.payload =
        (flowie_mqtt_span_t){(const uint8_t *)state->payloads[qos], strlen(state->payloads[qos])};
    rc = flowie_mqtt_client_publish(state->client, &publish, qos == 0u ? NULL : &response);
    if (rc != TURBO_OK) return rc;
    if (qos != 0u && (response.type != expected || response.reason_code >= 0x80u))
      return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static void flowie_mqtt_live_client_task(coro_t *co, void *arg) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)arg;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_control_packet_view_t response = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_mqtt_span_t topic = {(const uint8_t *)state->topic, strlen(state->topic)};
  unsigned int polls = 0u;
  int rc;
  (void)co;

  connect.version = state->test_case->version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)state->client_id, strlen(state->client_id)};
  rc = flowie_mqtt_client_connect(state->client, &connect, &response);
  if (rc != TURBO_OK || response.reason_code != 0u) {
    if (rc == TURBO_OK) rc = TURBO_ECONNREFUSED;
    goto done;
  }

  subscription.filter = topic;
  subscription.qos = 2u;
  subscribe.version = state->test_case->version;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  response = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_client_subscribe(state->client, &subscribe, &response);
  if (rc != TURBO_OK) goto done;
  if (response.reason_codes.size != 1u || response.reason_codes.data[0] != 2u) {
    rc = TURBO_EPROTO;
    goto done;
  }

  rc = flowie_mqtt_live_publish_all(state, topic);
  if (rc != TURBO_OK) goto done;
  while (state->received_mask != FLOWIE_MQTT_LIVE_RECEIVED_ALL &&
         polls < FLOWIE_MQTT_LIVE_QOS_COUNT) {
    rc = flowie_mqtt_client_poll(state->client, NULL);
    if (rc != TURBO_OK) goto done;
    ++polls;
  }
  if (state->received_mask != FLOWIE_MQTT_LIVE_RECEIVED_ALL) {
    rc = TURBO_EPROTO;
    goto done;
  }

  rc = flowie_mqtt_client_ping(state->client);
  if (rc != TURBO_OK) goto done;

  unsubscribe.version = state->test_case->version;
  unsubscribe.filters = &topic;
  unsubscribe.filter_count = 1u;
  response = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  rc = flowie_mqtt_client_unsubscribe(state->client, &unsubscribe, &response);
  if (rc != TURBO_OK) goto done;
  if ((state->test_case->version == FLOWIE_MQTT_VERSION_5 &&
       (response.reason_codes.size != 1u || response.reason_codes.data[0] >= 0x80u)) ||
      (state->test_case->version == FLOWIE_MQTT_VERSION_3_1_1 &&
       response.reason_codes.size != 0u)) {
    rc = TURBO_EPROTO;
    goto done;
  }

  rc = flowie_mqtt_client_disconnect(state->client, 0u, (flowie_mqtt_span_t){0});

done:
  if (rc != TURBO_OK && flowie_mqtt_client_is_connected(state->client))
    (void)flowie_mqtt_client_disconnect(state->client, 0u, (flowie_mqtt_span_t){0});
  state->result = rc;
  state->done = 1;
}

static int flowie_mqtt_live_run(const flowie_mqtt_live_case_t *test_case,
                                flowie_mqtt_live_state_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  uint64_t unique = turbo_hrtime();
  uint64_t deadline;
  int rc;

  memset(state, 0, sizeof(*state));
  state->test_case = test_case;
  state->result = TURBO_EBUSY;
  if (snprintf(state->client_id, sizeof(state->client_id), "flowie-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(state->topic, sizeof(state->topic), "flowie/live/%llu", (unsigned long long)unique) <
          0)
    return TURBO_EIO;
  for (unsigned int qos = 0u; qos < FLOWIE_MQTT_LIVE_QOS_COUNT; ++qos) {
    if (snprintf(state->payloads[qos], sizeof(state->payloads[qos]), "flowie-%llu-qos-%u",
                 (unsigned long long)unique, qos) < 0)
      return TURBO_EIO;
  }

  state->context = coro_context_create(NULL);
  if (!state->context) return TURBO_ENOMEM;
  config.context = state->context;
  config.host = test_case->host;
  config.port = test_case->port;
  config.path = test_case->path;
  config.transport = test_case->transport;
  config.timeout_ms = FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS;
  config.on_publish = flowie_mqtt_live_on_publish;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) goto done;
  rc = coro_context_spawn(state->context, flowie_mqtt_live_client_task, state);
  if (rc != TURBO_OK) goto done;

  deadline = turbo_monotonic_ms() + FLOWIE_MQTT_LIVE_TEST_TIMEOUT_MS;
  while (!state->done && turbo_monotonic_ms() < deadline)
    (void)coro_context_run(state->context, TURBO_RUN_ONCE);
  rc = state->done ? state->result : TURBO_ETIMEDOUT;

done:
  flowie_mqtt_client_destroy(state->client);
  coro_context_destroy(state->context);
  state->client = NULL;
  state->context = NULL;
  return rc;
}

static void flowie_mqtt_live_check(const flowie_mqtt_live_case_t *test_case) {
  flowie_mqtt_live_state_t state;
  int rc = flowie_mqtt_live_run(test_case, &state);
  info("endpoint=%s:%d transport=%d version=%d", test_case->host, test_case->port,
       (int)test_case->transport, (int)test_case->version);
  check_int_eq(rc, TURBO_OK);
  check_uint_eq(state.received_mask, FLOWIE_MQTT_LIVE_RECEIVED_ALL);
}

spec("flowie mqtt public broker integration") {
  it("round-trips MQTT 3.1.1 QoS 0/1/2 through HiveMQ TCP") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_HIVEMQ_TCP_3);
  }

  it("round-trips MQTT 5 QoS 0/1/2 through HiveMQ WebSocket") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_HIVEMQ_WS_5);
  }

  it("round-trips MQTT 3.1.1 QoS 0/1/2 through EMQX TCP") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_EMQX_TCP_3);
  }

  it("round-trips MQTT 5 QoS 0/1/2 through EMQX TLS") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_EMQX_TLS_5);
  }

  it("round-trips MQTT 5 QoS 0/1/2 through EMQX WebSocket") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_EMQX_WS_5);
  }

  it("round-trips MQTT 5 QoS 0/1/2 through EMQX secure WebSocket") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_EMQX_WSS_5);
  }

  it("round-trips MQTT 5 QoS 0/1/2 through Mosquitto TCP") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_MOSQUITTO_TCP_5);
  }

  it("round-trips MQTT 3.1.1 QoS 0/1/2 through Mosquitto TLS") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_MOSQUITTO_TLS_3);
  }

  it("round-trips MQTT 5 QoS 0/1/2 through Mosquitto WebSocket") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_LIVE_MOSQUITTO_WS_5);
  }
}
