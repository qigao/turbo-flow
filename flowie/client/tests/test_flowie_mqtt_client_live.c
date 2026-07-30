#include "flowie_mqtt_client.h"
#include "flowie_mqtt_protocol.h"

#include "platform.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS 10000u
#define FLOWIE_MQTT_LIVE_TEST_TIMEOUT_MS 30000u
#define FLOWIE_MQTT_LIVE_BUFFER_SIZE 128u
#define FLOWIE_MQTT_LIVE_PROPERTY_BUFFER_SIZE 256u
#define FLOWIE_MQTT_LIVE_PROPERTY_PREFIX_SIZE 3u
#define FLOWIE_MQTT_LIVE_QOS_COUNT 3u
#define FLOWIE_MQTT_LIVE_RECEIVED_ALL ((1u << FLOWIE_MQTT_LIVE_QOS_COUNT) - 1u)

typedef struct flowie_mqtt_live_case_s {
  const char *name;
  const char *host;
  int port;
  const char *path;
  const char *ca_file;
  flowie_mqtt_client_transport_t transport;
  flowie_mqtt_version_t version;
} flowie_mqtt_live_case_t;

typedef struct flowie_mqtt_live_state_s {
  const flowie_mqtt_live_case_t *test_case;
  flowie_mqtt_client_t *client;
  atomic_int result;
  atomic_int done;
  unsigned int received_mask;
  unsigned int publish_index;
  int publishes_done;
  int shutdown_started;
  const char *stage;
  char client_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char payloads[FLOWIE_MQTT_LIVE_QOS_COUNT][FLOWIE_MQTT_LIVE_BUFFER_SIZE];
} flowie_mqtt_live_state_t;

typedef struct flowie_mqtt_live_reqrep_state_s {
  const flowie_mqtt_live_case_t *test_case;
  flowie_mqtt_client_t *client;
  atomic_int result;
  atomic_int done;
  int request_received;
  int response_received;
  unsigned int publish_completions;
  int response_publish_done;
  int unsubscribe_started;
  char client_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char request_topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char response_topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char correlation[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
} flowie_mqtt_live_reqrep_state_t;

typedef struct flowie_mqtt_live_alias_state_s {
  const flowie_mqtt_live_case_t *test_case;
  flowie_mqtt_client_t *client;
  atomic_int result;
  atomic_int done;
  unsigned int received_count;
  unsigned int publish_completions;
  int second_publish_started;
  int disconnect_started;
  char client_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
} flowie_mqtt_live_alias_state_t;

#if !defined(FLOWIE_MQTT_FIXED_INTEROP)
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_HIVEMQ_TCP_3 = {
    "HiveMQ TCP MQTT 3.1.1",          "broker.hivemq.com",      1883, NULL, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_HIVEMQ_WS_5 = {
    "HiveMQ WebSocket MQTT 5",       "broker.hivemq.com",  8000, "/mqtt", NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_TCP_3 = {
    "EMQX TCP MQTT 3.1.1", "broker.emqx.io", 1883, NULL, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,
    FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_TLS_5 = {
    "EMQX TLS MQTT 5", "broker.emqx.io", 8883, NULL, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TLS,
    FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_WS_5 = {
    "EMQX WebSocket MQTT 5",         "broker.emqx.io",     8083, "/mqtt", NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_LIVE_EMQX_WSS_5 = {
    "EMQX secure WebSocket MQTT 5", "broker.emqx.io", 8084, "/mqtt", NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, FLOWIE_MQTT_VERSION_5};
#else
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_TCP_4 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " TCP MQTT 3.1.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_TCP_PORT, NULL, NULL, FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,
    FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_TCP_5 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " TCP MQTT 5", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_TCP_PORT, NULL, NULL, FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,
    FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_TLS_4 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " TLS MQTT 3.1.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_TLS_PORT, NULL, FLOWIE_MQTT_FIXED_CA_FILE,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TLS,
    FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_TLS_5 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " TLS MQTT 5", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_TLS_PORT, NULL, FLOWIE_MQTT_FIXED_CA_FILE,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_WS_4 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " WS MQTT 3.1.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_WS_PORT, FLOWIE_MQTT_FIXED_WS_PATH, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
    FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_WS_5 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " WS MQTT 5", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_WS_PORT, FLOWIE_MQTT_FIXED_WS_PATH, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
    FLOWIE_MQTT_VERSION_5};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_WSS_4 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " WSS MQTT 3.1.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_WSS_PORT, FLOWIE_MQTT_FIXED_WS_PATH, FLOWIE_MQTT_FIXED_CA_FILE,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WSS,
    FLOWIE_MQTT_VERSION_3_1_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_WSS_5 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " WSS MQTT 5", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_WSS_PORT, FLOWIE_MQTT_FIXED_WS_PATH, FLOWIE_MQTT_FIXED_CA_FILE,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WSS,
    FLOWIE_MQTT_VERSION_5};
#if FLOWIE_MQTT_FIXED_SUPPORT_31
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_TCP_3 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " TCP MQTT 3.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_TCP_PORT, NULL, NULL, FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,
    FLOWIE_MQTT_VERSION_3_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_TLS_3 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " TLS MQTT 3.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_TLS_PORT, NULL, FLOWIE_MQTT_FIXED_CA_FILE,
    FLOWIE_MQTT_CLIENT_TRANSPORT_TLS,
    FLOWIE_MQTT_VERSION_3_1};
#endif
#if FLOWIE_MQTT_FIXED_SUPPORT_31_WS
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_WS_3 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " WS MQTT 3.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_WS_PORT, FLOWIE_MQTT_FIXED_WS_PATH, NULL,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
    FLOWIE_MQTT_VERSION_3_1};
static const flowie_mqtt_live_case_t FLOWIE_MQTT_FIXED_WSS_3 = {
    FLOWIE_MQTT_FIXED_BROKER_NAME " WSS MQTT 3.1", FLOWIE_MQTT_FIXED_HOST,
    FLOWIE_MQTT_FIXED_WSS_PORT, FLOWIE_MQTT_FIXED_WS_PATH, FLOWIE_MQTT_FIXED_CA_FILE,
    FLOWIE_MQTT_CLIENT_TRANSPORT_WSS,
    FLOWIE_MQTT_VERSION_3_1};
#endif
#endif

static int flowie_mqtt_live_span_equals(flowie_mqtt_span_t span, const char *text) {
  size_t text_size = strlen(text);
  return span.size == text_size && memcmp(span.data, text, text_size) == 0;
}

static void flowie_mqtt_live_finish(atomic_int *result, atomic_int *done, int status) {
  if (atomic_load_explicit(done, memory_order_acquire)) return;
  atomic_store_explicit(result, status, memory_order_relaxed);
  atomic_store_explicit(done, 1, memory_order_release);
}

static int flowie_mqtt_live_wait(atomic_int *result, atomic_int *done) {
  uint64_t deadline = turbo_monotonic_ms() + FLOWIE_MQTT_LIVE_TEST_TIMEOUT_MS;
  while (!atomic_load_explicit(done, memory_order_acquire) && turbo_monotonic_ms() < deadline)
    turbo_sleep_ms(1u);
  return atomic_load_explicit(done, memory_order_acquire)
             ? atomic_load_explicit(result, memory_order_relaxed)
             : TURBO_ETIMEDOUT;
}

static void
flowie_mqtt_live_disconnect_completion(flowie_mqtt_client_t *client, int status,
                                       const flowie_mqtt_control_packet_view_t *response,
                                       void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void
flowie_mqtt_live_unsubscribe_completion(flowie_mqtt_client_t *client, int status,
                                        const flowie_mqtt_control_packet_view_t *response,
                                        void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_UNSUBACK ||
       (state->test_case->version == FLOWIE_MQTT_VERSION_5 &&
        (response->reason_codes.size != 1u || response->reason_codes.data[0] >= 0x80u))))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) state->stage = "disconnect";
  if (status == TURBO_OK)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_ping_completion(flowie_mqtt_client_t *client, int status,
                                             const flowie_mqtt_control_packet_view_t *response,
                                             void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_span_t filter = {(const uint8_t *)state->topic, strlen(state->topic)};
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  if (status == TURBO_OK) {
    state->stage = "unsubscribe";
    unsubscribe.version = state->test_case->version;
    unsubscribe.filters = &filter;
    unsubscribe.filter_count = 1u;
    status = flowie_mqtt_client_unsubscribe(client, &unsubscribe);
  }
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_maybe_shutdown(flowie_mqtt_live_state_t *state) {
  int rc;
  if (!state->publishes_done || state->received_mask != FLOWIE_MQTT_LIVE_RECEIVED_ALL ||
      state->shutdown_started)
    return;
  state->shutdown_started = 1;
  state->stage = "ping";
  rc = flowie_mqtt_client_ping(state->client);
  if (rc != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, rc);
}

static int flowie_mqtt_live_submit_publish(flowie_mqtt_live_state_t *state);

static void flowie_mqtt_live_publish_completion(flowie_mqtt_client_t *client, int status,
                                                const flowie_mqtt_control_packet_view_t *response,
                                                void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  unsigned int qos = state->publish_index;
  (void)client;
  if (status == TURBO_OK &&
      ((qos == 0u && response) ||
       (qos == 1u && (!response || response->type != FLOWIE_MQTT_PACKET_PUBACK ||
                      response->reason_code >= 0x80u)) ||
       (qos == 2u && (!response || response->type != FLOWIE_MQTT_PACKET_PUBCOMP ||
                      response->reason_code >= 0x80u))))
    status = TURBO_EPROTO;
  if (status != TURBO_OK) {
    flowie_mqtt_live_finish(&state->result, &state->done, status);
    return;
  }
  ++state->publish_index;
  if (state->publish_index < FLOWIE_MQTT_LIVE_QOS_COUNT) {
    status = flowie_mqtt_live_submit_publish(state);
    if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
    return;
  }
  state->publishes_done = 1;
  flowie_mqtt_live_maybe_shutdown(state);
}

static int flowie_mqtt_live_submit_publish(flowie_mqtt_live_state_t *state) {
  flowie_mqtt_client_publish_topic_t topic = {0};
  flowie_mqtt_client_publish_topic_vec_t publish = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  state->stage = "publish";
  publish.version = state->test_case->version;
  publish.data = &topic;
  publish.count = 1u;
  topic.qos = (uint8_t)state->publish_index;
  topic.topic = (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
  topic.payload = (flowie_mqtt_span_t){(const uint8_t *)state->payloads[state->publish_index],
                                       strlen(state->payloads[state->publish_index])};
  return flowie_mqtt_client_publish(state->client, &publish);
}

static void flowie_mqtt_live_subscribe_completion(flowie_mqtt_client_t *client, int status,
                                                  const flowie_mqtt_control_packet_view_t *response,
                                                  void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_SUBACK ||
       response->reason_codes.size != 1u || response->reason_codes.data[0] != 2u))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_live_submit_publish(state);
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_connect_completion(flowie_mqtt_client_t *client, int status,
                                                const flowie_mqtt_control_packet_view_t *response,
                                                void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK || response->reason_code != 0u))
    status = TURBO_ECONNREFUSED;
  if (status == TURBO_OK) {
    state->stage = "subscribe";
    subscription.filter = (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
    subscription.qos = 2u;
    subscribe.version = state->test_case->version;
    subscribe.subscriptions = &subscription;
    subscribe.subscription_count = 1u;
    status = flowie_mqtt_client_subscribe(client, &subscribe);
  }
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_live_on_publish(flowie_mqtt_client_t *client,
                                       const flowie_mqtt_publish_view_t *publish, void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  (void)client;
  if (!publish || !flowie_mqtt_live_span_equals(publish->topic, state->topic)) return TURBO_EPROTO;
  for (unsigned int qos = 0u; qos < FLOWIE_MQTT_LIVE_QOS_COUNT; ++qos) {
    if (flowie_mqtt_live_span_equals(publish->payload, state->payloads[qos])) {
      state->received_mask |= 1u << qos;
      flowie_mqtt_live_maybe_shutdown(state);
      return TURBO_OK;
    }
  }
  return TURBO_EPROTO;
}

static void flowie_mqtt_live_on_error(flowie_mqtt_client_t *client, int status, void *user_data) {
  flowie_mqtt_live_state_t *state = (flowie_mqtt_live_state_t *)user_data;
  (void)client;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_live_run(const flowie_mqtt_live_case_t *test_case,
                                flowie_mqtt_live_state_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_topic_handler_t topic_handler = {0};
  uint64_t unique = turbo_hrtime();
  int rc;
  memset(state, 0, sizeof(*state));
  state->test_case = test_case;
  atomic_init(&state->result, TURBO_EBUSY);
  atomic_init(&state->done, 0);
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
  config.host = test_case->host;
  config.port = test_case->port;
  config.path = test_case->path;
  config.tls.ca_file = test_case->ca_file;
  config.transport = test_case->transport;
  config.timeout_ms = FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS;
  topic_handler.filter =
      (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
  topic_handler.on_message = flowie_mqtt_live_on_publish;
  config.topic_handlers =
      (flowie_mqtt_client_topic_handler_map_t){&topic_handler, 1u};
  config.on_connect = flowie_mqtt_live_connect_completion;
  config.on_publish = flowie_mqtt_live_publish_completion;
  config.on_subscribe = flowie_mqtt_live_subscribe_completion;
  config.on_unsubscribe = flowie_mqtt_live_unsubscribe_completion;
  config.on_ping = flowie_mqtt_live_ping_completion;
  config.on_disconnect = flowie_mqtt_live_disconnect_completion;
  config.on_error = flowie_mqtt_live_on_error;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) return rc;
  connect.version = test_case->version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)state->client_id, strlen(state->client_id)};
  state->stage = "connect";
  rc = flowie_mqtt_client_connect(state->client, &connect);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&state->result, &state->done);
  flowie_mqtt_client_destroy(state->client);
  state->client = NULL;
  return rc;
}

static void flowie_mqtt_live_check(const flowie_mqtt_live_case_t *test_case) {
  flowie_mqtt_live_state_t state;
  int rc = flowie_mqtt_live_run(test_case, &state);
  info("endpoint=%s:%d transport=%d version=%d stage=%s", test_case->host, test_case->port,
       (int)test_case->transport, (int)test_case->version, state.stage ? state.stage : "create");
  check_int_eq(rc, TURBO_OK);
  check_uint_eq(state.received_mask, FLOWIE_MQTT_LIVE_RECEIVED_ALL);
}

static int flowie_mqtt_live_property_append(uint8_t identifier, const char *value, uint8_t *output,
                                            size_t capacity, size_t *size) {
  size_t value_size;
  if (!value || !output || !size) return TURBO_EINVAL;
  value_size = strlen(value);
  if (value_size > UINT16_MAX || *size > capacity ||
      capacity - *size < value_size + FLOWIE_MQTT_LIVE_PROPERTY_PREFIX_SIZE)
    return TURBO_EMSGSIZE;
  output[(*size)++] = identifier;
  output[(*size)++] = (uint8_t)(value_size >> 8u);
  output[(*size)++] = (uint8_t)value_size;
  memcpy(output + *size, value, value_size);
  *size += value_size;
  return TURBO_OK;
}

static int
flowie_mqtt_live_reqrep_properties_match(const flowie_mqtt_property_block_view_t *properties,
                                         const char *response_topic, const char *correlation) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  int found_response_topic = 0;
  int found_correlation = 0;
  int rc;
  if (!properties || !correlation) return TURBO_EINVAL;
  rc = flowie_mqtt_property_iterator_init(properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  for (;;) {
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    rc = flowie_mqtt_property_iterator_next(&iterator, &property);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    if (property.identifier == FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC) {
      if (!response_topic || found_response_topic ||
          !flowie_mqtt_live_span_equals(property.value, response_topic))
        return TURBO_EPROTO;
      found_response_topic = 1;
    } else if (property.identifier == FLOWIE_MQTT_PROPERTY_CORRELATION_DATA) {
      if (found_correlation || !flowie_mqtt_live_span_equals(property.value, correlation))
        return TURBO_EPROTO;
      found_correlation = 1;
    }
  }
  return found_correlation && found_response_topic == (response_topic != NULL) ? TURBO_OK
                                                                               : TURBO_EPROTO;
}

static void
flowie_mqtt_live_reqrep_disconnect_completion(flowie_mqtt_client_t *client, int status,
                                              const flowie_mqtt_control_packet_view_t *response,
                                              void *user_data) {
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void
flowie_mqtt_live_reqrep_unsubscribe_completion(flowie_mqtt_client_t *client, int status,
                                               const flowie_mqtt_control_packet_view_t *response,
                                               void *user_data) {
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_UNSUBACK ||
       response->reason_codes.size != 2u || response->reason_codes.data[0] >= 0x80u ||
       response->reason_codes.data[1] >= 0x80u))
    status = TURBO_EPROTO;
  if (status == TURBO_OK)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_reqrep_maybe_unsubscribe(flowie_mqtt_live_reqrep_state_t *state) {
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_span_t topics[2] = {
      {(const uint8_t *)state->request_topic, strlen(state->request_topic)},
      {(const uint8_t *)state->response_topic, strlen(state->response_topic)}};
  int rc;
  if (!state->response_received || !state->response_publish_done || state->unsubscribe_started)
    return;
  state->unsubscribe_started = 1;
  unsubscribe.version = FLOWIE_MQTT_VERSION_5;
  unsubscribe.filters = topics;
  unsubscribe.filter_count = 2u;
  rc = flowie_mqtt_client_unsubscribe(state->client, &unsubscribe);
  if (rc != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, rc);
}

static void
flowie_mqtt_live_reqrep_publish_completion(flowie_mqtt_client_t *client, int status,
                                           const flowie_mqtt_control_packet_view_t *response,
                                           void *user_data) {
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_PUBACK || response->reason_code >= 0x80u))
    status = TURBO_EPROTO;
  if (status != TURBO_OK) {
    flowie_mqtt_live_finish(&state->result, &state->done, status);
    return;
  }
  ++state->publish_completions;
  if (state->publish_completions == 2u) {
    state->response_publish_done = 1;
    flowie_mqtt_live_reqrep_maybe_unsubscribe(state);
  } else if (state->publish_completions > 2u) {
    flowie_mqtt_live_finish(&state->result, &state->done, TURBO_EPROTO);
  }
}

static int flowie_mqtt_live_reqrep_submit_request(flowie_mqtt_live_reqrep_state_t *state) {
  static const char request_payload[] = "flowie-request";
  flowie_mqtt_client_publish_topic_t topic = {0};
  flowie_mqtt_client_publish_topic_vec_t publish = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  uint8_t properties[FLOWIE_MQTT_LIVE_PROPERTY_BUFFER_SIZE];
  size_t properties_size = 0u;
  int rc =
      flowie_mqtt_live_property_append(FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC, state->response_topic,
                                       properties, sizeof(properties), &properties_size);
  if (rc == TURBO_OK)
    rc = flowie_mqtt_live_property_append(FLOWIE_MQTT_PROPERTY_CORRELATION_DATA, state->correlation,
                                          properties, sizeof(properties), &properties_size);
  if (rc != TURBO_OK) return rc;
  publish.version = FLOWIE_MQTT_VERSION_5;
  publish.data = &topic;
  publish.count = 1u;
  topic.qos = 1u;
  topic.topic =
      (flowie_mqtt_span_t){(const uint8_t *)state->request_topic, strlen(state->request_topic)};
  topic.properties = (flowie_mqtt_span_t){properties, properties_size};
  topic.payload =
      (flowie_mqtt_span_t){(const uint8_t *)request_payload, sizeof(request_payload) - 1u};
  return flowie_mqtt_client_publish(state->client, &publish);
}

static int flowie_mqtt_live_reqrep_submit_response(flowie_mqtt_live_reqrep_state_t *state) {
  static const char response_payload[] = "flowie-response";
  flowie_mqtt_client_publish_topic_t topic = {0};
  flowie_mqtt_client_publish_topic_vec_t publish = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  uint8_t properties[FLOWIE_MQTT_LIVE_PROPERTY_BUFFER_SIZE];
  size_t properties_size = 0u;
  int rc =
      flowie_mqtt_live_property_append(FLOWIE_MQTT_PROPERTY_CORRELATION_DATA, state->correlation,
                                       properties, sizeof(properties), &properties_size);
  if (rc != TURBO_OK) return rc;
  publish.version = FLOWIE_MQTT_VERSION_5;
  publish.data = &topic;
  publish.count = 1u;
  topic.qos = 1u;
  topic.topic =
      (flowie_mqtt_span_t){(const uint8_t *)state->response_topic, strlen(state->response_topic)};
  topic.properties = (flowie_mqtt_span_t){properties, properties_size};
  topic.payload =
      (flowie_mqtt_span_t){(const uint8_t *)response_payload, sizeof(response_payload) - 1u};
  return flowie_mqtt_client_publish(state->client, &publish);
}

static int flowie_mqtt_live_reqrep_on_publish(flowie_mqtt_client_t *client,
                                              const flowie_mqtt_publish_view_t *publish,
                                              void *user_data) {
  static const char request_payload[] = "flowie-request";
  static const char response_payload[] = "flowie-response";
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  int rc;
  (void)client;
  if (!publish || publish->qos != 1u) return TURBO_EPROTO;
  if (flowie_mqtt_live_span_equals(publish->topic, state->request_topic)) {
    if (state->request_received || !flowie_mqtt_live_span_equals(publish->payload, request_payload))
      return TURBO_EPROTO;
    rc = flowie_mqtt_live_reqrep_properties_match(&publish->properties, state->response_topic,
                                                  state->correlation);
    if (rc != TURBO_OK) return rc;
    state->request_received = 1;
    return flowie_mqtt_live_reqrep_submit_response(state);
  }
  if (flowie_mqtt_live_span_equals(publish->topic, state->response_topic)) {
    if (state->response_received ||
        !flowie_mqtt_live_span_equals(publish->payload, response_payload))
      return TURBO_EPROTO;
    rc = flowie_mqtt_live_reqrep_properties_match(&publish->properties, NULL, state->correlation);
    if (rc != TURBO_OK) return rc;
    state->response_received = 1;
    flowie_mqtt_live_reqrep_maybe_unsubscribe(state);
    return TURBO_OK;
  }
  return TURBO_EPROTO;
}

static void
flowie_mqtt_live_reqrep_subscribe_completion(flowie_mqtt_client_t *client, int status,
                                             const flowie_mqtt_control_packet_view_t *response,
                                             void *user_data) {
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_SUBACK ||
       response->reason_codes.size != 2u || response->reason_codes.data[0] != 1u ||
       response->reason_codes.data[1] != 1u))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_live_reqrep_submit_request(state);
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void
flowie_mqtt_live_reqrep_connect_completion(flowie_mqtt_client_t *client, int status,
                                           const flowie_mqtt_control_packet_view_t *response,
                                           void *user_data) {
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  flowie_mqtt_subscription_t subscriptions[2] = {{0}};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK || response->reason_code != 0u))
    status = TURBO_ECONNREFUSED;
  if (status == TURBO_OK) {
    subscriptions[0].filter =
        (flowie_mqtt_span_t){(const uint8_t *)state->request_topic, strlen(state->request_topic)};
    subscriptions[0].qos = 1u;
    subscriptions[1].filter =
        (flowie_mqtt_span_t){(const uint8_t *)state->response_topic, strlen(state->response_topic)};
    subscriptions[1].qos = 1u;
    subscribe.version = FLOWIE_MQTT_VERSION_5;
    subscribe.subscriptions = subscriptions;
    subscribe.subscription_count = 2u;
    status = flowie_mqtt_client_subscribe(client, &subscribe);
  }
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_reqrep_on_error(flowie_mqtt_client_t *client, int status,
                                             void *user_data) {
  flowie_mqtt_live_reqrep_state_t *state = (flowie_mqtt_live_reqrep_state_t *)user_data;
  (void)client;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_live_reqrep_run(const flowie_mqtt_live_case_t *test_case,
                                       flowie_mqtt_live_reqrep_state_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_topic_handler_t topic_handlers[2] = {{0}};
  uint64_t unique = turbo_hrtime();
  int rc;
  if (!test_case || test_case->version != FLOWIE_MQTT_VERSION_5) return TURBO_EINVAL;
  memset(state, 0, sizeof(*state));
  state->test_case = test_case;
  atomic_init(&state->result, TURBO_EBUSY);
  atomic_init(&state->done, 0);
  if (snprintf(state->client_id, sizeof(state->client_id), "flowie-rr-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(state->request_topic, sizeof(state->request_topic), "flowie/rr/%llu/request",
               (unsigned long long)unique) < 0 ||
      snprintf(state->response_topic, sizeof(state->response_topic), "flowie/rr/%llu/response",
               (unsigned long long)unique) < 0 ||
      snprintf(state->correlation, sizeof(state->correlation), "correlation-%llu",
               (unsigned long long)unique) < 0)
    return TURBO_EIO;
  config.host = test_case->host;
  config.port = test_case->port;
  config.path = test_case->path;
  config.tls.ca_file = test_case->ca_file;
  config.transport = test_case->transport;
  config.timeout_ms = FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS;
  topic_handlers[0].filter = (flowie_mqtt_span_t){(const uint8_t *)state->request_topic,
                                                  strlen(state->request_topic)};
  topic_handlers[0].on_message = flowie_mqtt_live_reqrep_on_publish;
  topic_handlers[1].filter = (flowie_mqtt_span_t){(const uint8_t *)state->response_topic,
                                                  strlen(state->response_topic)};
  topic_handlers[1].on_message = flowie_mqtt_live_reqrep_on_publish;
  config.topic_handlers =
      (flowie_mqtt_client_topic_handler_map_t){topic_handlers, 2u};
  config.on_connect = flowie_mqtt_live_reqrep_connect_completion;
  config.on_publish = flowie_mqtt_live_reqrep_publish_completion;
  config.on_subscribe = flowie_mqtt_live_reqrep_subscribe_completion;
  config.on_unsubscribe = flowie_mqtt_live_reqrep_unsubscribe_completion;
  config.on_disconnect = flowie_mqtt_live_reqrep_disconnect_completion;
  config.on_error = flowie_mqtt_live_reqrep_on_error;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) return rc;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)state->client_id, strlen(state->client_id)};
  rc = flowie_mqtt_client_connect(state->client, &connect);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&state->result, &state->done);
  flowie_mqtt_client_destroy(state->client);
  state->client = NULL;
  return rc;
}

static void flowie_mqtt_live_reqrep_check(const flowie_mqtt_live_case_t *test_case) {
  flowie_mqtt_live_reqrep_state_t state;
  int rc = flowie_mqtt_live_reqrep_run(test_case, &state);
  info("endpoint=%s:%d transport=%d version=%d", test_case->host, test_case->port,
       (int)test_case->transport, (int)test_case->version);
  check_int_eq(rc, TURBO_OK);
  check_true(state.request_received);
  check_true(state.response_received);
}

static int flowie_mqtt_live_subscription_identifier_matches(
    const flowie_mqtt_property_block_view_t *properties, uint32_t expected) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  unsigned int matches = 0u;
  int rc;
  if (!properties || expected == 0u) return TURBO_EINVAL;
  rc = flowie_mqtt_property_iterator_init(properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  for (;;) {
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    rc = flowie_mqtt_property_iterator_next(&iterator, &property);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
    if (property.identifier == FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER) {
      if (matches != 0u || property.integer != expected) return TURBO_EPROTO;
      ++matches;
    }
  }
  return matches == 1u ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_mqtt_live_alias_submit(flowie_mqtt_live_alias_state_t *state, int reuse_alias) {
  static const uint8_t alias_property[] = {FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS, 0x00u, 0x01u};
  static const char first_payload[] = "alias-bind";
  static const char second_payload[] = "alias-reuse";
  flowie_mqtt_client_publish_topic_t topic = {0};
  flowie_mqtt_client_publish_topic_vec_t publish = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  const char *payload = reuse_alias ? second_payload : first_payload;
  publish.version = FLOWIE_MQTT_VERSION_5;
  publish.data = &topic;
  publish.count = 1u;
  topic.qos = 1u;
  if (!reuse_alias)
    topic.topic =
        (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
  topic.properties = (flowie_mqtt_span_t){alias_property, sizeof(alias_property)};
  topic.payload = (flowie_mqtt_span_t){(const uint8_t *)payload, strlen(payload)};
  return flowie_mqtt_client_publish(state->client, &publish);
}

static int flowie_mqtt_live_alias_advance(flowie_mqtt_live_alias_state_t *state) {
  if (!state->second_publish_started && state->received_count >= 1u &&
      state->publish_completions >= 1u) {
    state->second_publish_started = 1;
    return flowie_mqtt_live_alias_submit(state, 1);
  }
  if (!state->disconnect_started && state->received_count >= 2u &&
      state->publish_completions >= 2u) {
    state->disconnect_started = 1;
    return flowie_mqtt_client_disconnect(state->client, 0u, (flowie_mqtt_span_t){0});
  }
  return TURBO_OK;
}

static int flowie_mqtt_live_alias_on_publish(flowie_mqtt_client_t *client,
                                             const flowie_mqtt_publish_view_t *publish,
                                             void *user_data) {
  static const char *const payloads[] = {"alias-bind", "alias-reuse"};
  flowie_mqtt_live_alias_state_t *state = (flowie_mqtt_live_alias_state_t *)user_data;
  int rc;
  (void)client;
  if (!publish || state->received_count >= 2u || publish->qos != 1u ||
      !flowie_mqtt_live_span_equals(publish->topic, state->topic) ||
      !flowie_mqtt_live_span_equals(publish->payload, payloads[state->received_count]))
    return TURBO_EPROTO;
  rc = flowie_mqtt_live_subscription_identifier_matches(&publish->properties, 42u);
  if (rc != TURBO_OK) return rc;
  ++state->received_count;
  return flowie_mqtt_live_alias_advance(state);
}

static void flowie_mqtt_live_alias_disconnect_completion(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_live_alias_state_t *state = (flowie_mqtt_live_alias_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_alias_publish_completion(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_live_alias_state_t *state = (flowie_mqtt_live_alias_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_PUBACK || response->reason_code >= 0x80u))
    status = TURBO_EPROTO;
  if (status == TURBO_OK && ++state->publish_completions > 2u) status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_live_alias_advance(state);
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_alias_subscribe_completion(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_live_alias_state_t *state = (flowie_mqtt_live_alias_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_SUBACK ||
       response->reason_codes.size != 1u || response->reason_codes.data[0] != 1u))
    status = TURBO_EPROTO;
  if (status == TURBO_OK) status = flowie_mqtt_live_alias_submit(state, 0);
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_alias_connect_completion(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  static const uint8_t subscription_identifier[] = {
      FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER, 42u};
  flowie_mqtt_live_alias_state_t *state = (flowie_mqtt_live_alias_state_t *)user_data;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK || response->reason_code != 0u))
    status = TURBO_ECONNREFUSED;
  if (status == TURBO_OK) {
    subscription.filter =
        (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
    subscription.qos = 1u;
    subscribe.version = FLOWIE_MQTT_VERSION_5;
    subscribe.properties =
        (flowie_mqtt_span_t){subscription_identifier, sizeof(subscription_identifier)};
    subscribe.subscriptions = &subscription;
    subscribe.subscription_count = 1u;
    status = flowie_mqtt_client_subscribe(client, &subscribe);
  }
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_live_alias_on_error(flowie_mqtt_client_t *client, int status,
                                            void *user_data) {
  flowie_mqtt_live_alias_state_t *state = (flowie_mqtt_live_alias_state_t *)user_data;
  (void)client;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_live_alias_run(const flowie_mqtt_live_case_t *test_case,
                                      flowie_mqtt_live_alias_state_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_topic_handler_t handler = {0};
  uint64_t unique = turbo_hrtime();
  int rc;
  if (!test_case || !state || test_case->version != FLOWIE_MQTT_VERSION_5) return TURBO_EINVAL;
  memset(state, 0, sizeof(*state));
  state->test_case = test_case;
  atomic_init(&state->result, TURBO_EBUSY);
  atomic_init(&state->done, 0);
  if (snprintf(state->client_id, sizeof(state->client_id), "flowie-alias-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(state->topic, sizeof(state->topic), "flowie/alias/%llu",
               (unsigned long long)unique) < 0)
    return TURBO_EIO;
  config.host = test_case->host;
  config.port = test_case->port;
  config.path = test_case->path;
  config.tls.ca_file = test_case->ca_file;
  config.transport = test_case->transport;
  config.timeout_ms = FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS;
  handler.filter = (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
  handler.on_message = flowie_mqtt_live_alias_on_publish;
  config.topic_handlers = (flowie_mqtt_client_topic_handler_map_t){&handler, 1u};
  config.on_connect = flowie_mqtt_live_alias_connect_completion;
  config.on_publish = flowie_mqtt_live_alias_publish_completion;
  config.on_subscribe = flowie_mqtt_live_alias_subscribe_completion;
  config.on_disconnect = flowie_mqtt_live_alias_disconnect_completion;
  config.on_error = flowie_mqtt_live_alias_on_error;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) return rc;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)state->client_id, strlen(state->client_id)};
  rc = flowie_mqtt_client_connect(state->client, &connect);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&state->result, &state->done);
  flowie_mqtt_client_destroy(state->client);
  state->client = NULL;
  return rc;
}

static void flowie_mqtt_live_alias_check(const flowie_mqtt_live_case_t *test_case) {
  flowie_mqtt_live_alias_state_t state;
  int rc = flowie_mqtt_live_alias_run(test_case, &state);
  info("endpoint=%s:%d transport=%d version=%d", test_case->host, test_case->port,
       (int)test_case->transport, (int)test_case->version);
  check_int_eq(rc, TURBO_OK);
  check_uint_eq(state.received_count, 2u);
  check_uint_eq(state.publish_completions, 2u);
}

#if defined(FLOWIE_MQTT_FIXED_INTEROP)
#define FLOWIE_MQTT_FIXED_SESSION_EXPIRY_SECONDS 60u
#define FLOWIE_MQTT_FIXED_MESSAGE_EXPIRY_SECONDS 1u
#define FLOWIE_MQTT_FIXED_EXPIRY_OBSERVE_MS 2500u

typedef struct flowie_mqtt_fixed_publisher_s {
  const flowie_mqtt_live_case_t *test_case;
  flowie_mqtt_client_t *client;
  atomic_int result;
  atomic_int done;
  atomic_int ready;
  const char *client_id;
  const char *topic;
  const char *payload;
  flowie_mqtt_span_t connect_properties;
  flowie_mqtt_span_t publish_properties;
  uint8_t qos;
  uint8_t retain;
  uint8_t connect_only;
  uint8_t has_will;
  uint8_t will_qos;
  const char *will_topic;
  const char *will_payload;
} flowie_mqtt_fixed_publisher_t;

typedef struct flowie_mqtt_fixed_subscriber_s {
  const flowie_mqtt_live_case_t *test_case;
  flowie_mqtt_client_t *client;
  atomic_int result;
  atomic_int done;
  atomic_int ready;
  atomic_uint received;
  const char *client_id;
  const char *topic;
  const char *payload;
  flowie_mqtt_span_t connect_properties;
  int expected_session_present;
  int expected_retain;
  uint8_t clean_start;
  uint8_t subscribe;
  uint8_t disconnect_after_subscribe;
  uint8_t disconnect_after_message;
} flowie_mqtt_fixed_subscriber_t;

static const uint8_t FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY[] = {
    FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL, 0u, 0u, 0u,
    FLOWIE_MQTT_FIXED_SESSION_EXPIRY_SECONDS};
static const uint8_t FLOWIE_MQTT_FIXED_MESSAGE_EXPIRY_PROPERTY[] = {
    FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL, 0u, 0u, 0u,
    FLOWIE_MQTT_FIXED_MESSAGE_EXPIRY_SECONDS};

static void flowie_mqtt_fixed_apply_config(const flowie_mqtt_live_case_t *test_case,
                                           flowie_mqtt_client_config_t *config) {
  config->host = test_case->host;
  config->port = test_case->port;
  config->path = test_case->path;
  config->tls.ca_file = test_case->ca_file;
  config->transport = test_case->transport;
  config->timeout_ms = FLOWIE_MQTT_LIVE_IO_TIMEOUT_MS;
}

static int flowie_mqtt_fixed_wait_ready(atomic_int *result, atomic_int *done,
                                        atomic_int *ready) {
  uint64_t deadline = turbo_monotonic_ms() + FLOWIE_MQTT_LIVE_TEST_TIMEOUT_MS;
  while (!atomic_load_explicit(ready, memory_order_acquire) &&
         !atomic_load_explicit(done, memory_order_acquire) && turbo_monotonic_ms() < deadline)
    turbo_sleep_ms(1u);
  if (atomic_load_explicit(done, memory_order_acquire))
    return atomic_load_explicit(result, memory_order_relaxed);
  return atomic_load_explicit(ready, memory_order_acquire) ? TURBO_OK : TURBO_ETIMEDOUT;
}

static void flowie_mqtt_fixed_publisher_error(flowie_mqtt_client_t *client, int status,
                                              void *user_data) {
  flowie_mqtt_fixed_publisher_t *state = (flowie_mqtt_fixed_publisher_t *)user_data;
  (void)client;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_fixed_publisher_disconnect(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_fixed_publisher_t *state = (flowie_mqtt_fixed_publisher_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_fixed_publisher_publish(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_fixed_publisher_t *state = (flowie_mqtt_fixed_publisher_t *)user_data;
  if (status == TURBO_OK &&
      ((state->qos == 0u && response) ||
       (state->qos == 1u &&
        (!response || response->type != FLOWIE_MQTT_PACKET_PUBACK ||
         response->reason_code >= 0x80u))))
    status = TURBO_EPROTO;
  if (status == TURBO_OK)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_fixed_publisher_connect(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_fixed_publisher_t *state = (flowie_mqtt_fixed_publisher_t *)user_data;
  flowie_mqtt_client_publish_topic_t topic = {0};
  flowie_mqtt_client_publish_topic_vec_t publish = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK || response->reason_code != 0u))
    status = TURBO_ECONNREFUSED;
  if (status == TURBO_OK && state->connect_only) {
    atomic_store_explicit(&state->ready, 1, memory_order_release);
    return;
  }
  if (status == TURBO_OK) {
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.data = &topic;
    publish.count = 1u;
    topic.qos = state->qos;
    topic.retain = state->retain;
    topic.topic =
        (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
    topic.properties = state->publish_properties;
    topic.payload =
        (flowie_mqtt_span_t){(const uint8_t *)state->payload, strlen(state->payload)};
    status = flowie_mqtt_client_publish(client, &publish);
  }
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_fixed_publisher_start(flowie_mqtt_fixed_publisher_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  int rc;
  if (!state || !state->test_case || !state->client_id ||
      (!state->connect_only && (!state->topic || !state->payload)))
    return TURBO_EINVAL;
  atomic_init(&state->result, TURBO_EBUSY);
  atomic_init(&state->done, 0);
  atomic_init(&state->ready, 0);
  flowie_mqtt_fixed_apply_config(state->test_case, &config);
  config.on_connect = flowie_mqtt_fixed_publisher_connect;
  config.on_publish = flowie_mqtt_fixed_publisher_publish;
  config.on_disconnect = flowie_mqtt_fixed_publisher_disconnect;
  config.on_error = flowie_mqtt_fixed_publisher_error;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) return rc;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.properties = state->connect_properties;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)state->client_id, strlen(state->client_id)};
  connect.has_will = state->has_will;
  connect.will_qos = state->will_qos;
  if (state->has_will) {
    connect.will_topic = (flowie_mqtt_span_t){(const uint8_t *)state->will_topic,
                                             strlen(state->will_topic)};
    connect.will_payload = (flowie_mqtt_span_t){(const uint8_t *)state->will_payload,
                                               strlen(state->will_payload)};
  }
  rc = flowie_mqtt_client_connect(state->client, &connect);
  if (rc != TURBO_OK) {
    flowie_mqtt_client_destroy(state->client);
    state->client = NULL;
  }
  return rc;
}

static int flowie_mqtt_fixed_publisher_run(flowie_mqtt_fixed_publisher_t *state) {
  int rc = flowie_mqtt_fixed_publisher_start(state);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&state->result, &state->done);
  if (state->client) flowie_mqtt_client_destroy(state->client);
  state->client = NULL;
  return rc;
}

static void flowie_mqtt_fixed_subscriber_error(flowie_mqtt_client_t *client, int status,
                                               void *user_data) {
  flowie_mqtt_fixed_subscriber_t *state = (flowie_mqtt_fixed_subscriber_t *)user_data;
  (void)client;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_fixed_subscriber_disconnect(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_fixed_subscriber_t *state = (flowie_mqtt_fixed_subscriber_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_fixed_subscriber_message(flowie_mqtt_client_t *client,
                                                const flowie_mqtt_publish_view_t *publish,
                                                void *user_data) {
  flowie_mqtt_fixed_subscriber_t *state = (flowie_mqtt_fixed_subscriber_t *)user_data;
  unsigned int received;
  int status = TURBO_OK;
  if (!publish || !flowie_mqtt_live_span_equals(publish->topic, state->topic) ||
      !flowie_mqtt_live_span_equals(publish->payload, state->payload) || publish->qos != 1u ||
      (state->expected_retain >= 0 && publish->retain != (uint8_t)state->expected_retain))
    status = TURBO_EPROTO;
  received = atomic_fetch_add_explicit(&state->received, 1u, memory_order_relaxed) + 1u;
  if (received != 1u) status = TURBO_EPROTO;
  if (status == TURBO_OK && state->disconnect_after_message)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
  return status;
}

static void flowie_mqtt_fixed_subscriber_subscribe(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_fixed_subscriber_t *state = (flowie_mqtt_fixed_subscriber_t *)user_data;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_SUBACK ||
       response->reason_codes.size != 1u || response->reason_codes.data[0] != 1u))
    status = TURBO_EPROTO;
  if (status == TURBO_OK && state->disconnect_after_subscribe)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  else if (status == TURBO_OK)
    atomic_store_explicit(&state->ready, 1, memory_order_release);
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static void flowie_mqtt_fixed_subscriber_connect(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_mqtt_fixed_subscriber_t *state = (flowie_mqtt_fixed_subscriber_t *)user_data;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  if (status == TURBO_OK &&
      (!response || response->type != FLOWIE_MQTT_PACKET_CONNACK || response->reason_code != 0u ||
       (state->expected_session_present >= 0 &&
        response->session_present != (uint8_t)state->expected_session_present)))
    status = TURBO_ECONNREFUSED;
  if (status == TURBO_OK && state->subscribe) {
    subscription.filter =
        (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
    subscription.qos = 1u;
    subscribe.version = FLOWIE_MQTT_VERSION_5;
    subscribe.subscriptions = &subscription;
    subscribe.subscription_count = 1u;
    status = flowie_mqtt_client_subscribe(client, &subscribe);
  } else if (status == TURBO_OK) {
    atomic_store_explicit(&state->ready, 1, memory_order_release);
  }
  if (status != TURBO_OK) flowie_mqtt_live_finish(&state->result, &state->done, status);
}

static int flowie_mqtt_fixed_subscriber_start(flowie_mqtt_fixed_subscriber_t *state) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_topic_handler_t handler = {0};
  int rc;
  if (!state || !state->test_case || !state->client_id || !state->topic || !state->payload)
    return TURBO_EINVAL;
  atomic_init(&state->result, TURBO_EBUSY);
  atomic_init(&state->done, 0);
  atomic_init(&state->ready, 0);
  atomic_init(&state->received, 0u);
  flowie_mqtt_fixed_apply_config(state->test_case, &config);
  handler.filter = (flowie_mqtt_span_t){(const uint8_t *)state->topic, strlen(state->topic)};
  handler.on_message = flowie_mqtt_fixed_subscriber_message;
  config.topic_handlers = (flowie_mqtt_client_topic_handler_map_t){&handler, 1u};
  config.on_connect = flowie_mqtt_fixed_subscriber_connect;
  config.on_subscribe = flowie_mqtt_fixed_subscriber_subscribe;
  config.on_disconnect = flowie_mqtt_fixed_subscriber_disconnect;
  config.on_error = flowie_mqtt_fixed_subscriber_error;
  config.user_data = state;
  rc = flowie_mqtt_client_create(&config, &state->client);
  if (rc != TURBO_OK) return rc;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = state->clean_start;
  connect.keep_alive = 30u;
  connect.properties = state->connect_properties;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)state->client_id, strlen(state->client_id)};
  rc = flowie_mqtt_client_connect(state->client, &connect);
  if (rc != TURBO_OK) {
    flowie_mqtt_client_destroy(state->client);
    state->client = NULL;
  }
  return rc;
}

static void flowie_mqtt_fixed_subscriber_destroy(flowie_mqtt_fixed_subscriber_t *state) {
  if (state->client) flowie_mqtt_client_destroy(state->client);
  state->client = NULL;
}

static int flowie_mqtt_fixed_retained_run(const flowie_mqtt_live_case_t *test_case) {
  static const char retained_payload[] = "flowie-fixed-retained";
  flowie_mqtt_fixed_publisher_t publisher = {0};
  flowie_mqtt_fixed_subscriber_t subscriber = {0};
  char publisher_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char subscriber_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char clear_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  uint64_t unique = turbo_hrtime();
  int rc;
  if (snprintf(publisher_id, sizeof(publisher_id), "flowie-fixed-retained-pub-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(subscriber_id, sizeof(subscriber_id), "flowie-fixed-retained-sub-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(clear_id, sizeof(clear_id), "flowie-fixed-retained-clear-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(topic, sizeof(topic), "flowie/fixed/retained/%llu",
               (unsigned long long)unique) < 0)
    return TURBO_EIO;
  publisher.test_case = test_case;
  publisher.client_id = publisher_id;
  publisher.topic = topic;
  publisher.payload = retained_payload;
  publisher.qos = 1u;
  publisher.retain = 1u;
  rc = flowie_mqtt_fixed_publisher_run(&publisher);
  if (rc != TURBO_OK) return rc;

  subscriber.test_case = test_case;
  subscriber.client_id = subscriber_id;
  subscriber.topic = topic;
  subscriber.payload = retained_payload;
  subscriber.expected_session_present = 0;
  subscriber.expected_retain = 1;
  subscriber.clean_start = 1u;
  subscriber.subscribe = 1u;
  subscriber.disconnect_after_message = 1u;
  rc = flowie_mqtt_fixed_subscriber_start(&subscriber);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&subscriber.result, &subscriber.done);
  flowie_mqtt_fixed_subscriber_destroy(&subscriber);
  if (rc != TURBO_OK) return rc;
  if (atomic_load_explicit(&subscriber.received, memory_order_relaxed) != 1u)
    return TURBO_EPROTO;

  memset(&publisher, 0, sizeof(publisher));
  publisher.test_case = test_case;
  publisher.client_id = clear_id;
  publisher.topic = topic;
  publisher.payload = "";
  publisher.qos = 1u;
  publisher.retain = 1u;
  return flowie_mqtt_fixed_publisher_run(&publisher);
}

static int flowie_mqtt_fixed_create_offline_session(
    const flowie_mqtt_live_case_t *test_case, const char *client_id, const char *topic,
    const char *payload) {
  flowie_mqtt_fixed_subscriber_t subscriber = {0};
  int rc;
  subscriber.test_case = test_case;
  subscriber.client_id = client_id;
  subscriber.topic = topic;
  subscriber.payload = payload;
  subscriber.connect_properties =
      (flowie_mqtt_span_t){FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY,
                           sizeof(FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY)};
  subscriber.expected_session_present = 0;
  subscriber.expected_retain = 0;
  subscriber.clean_start = 1u;
  subscriber.subscribe = 1u;
  subscriber.disconnect_after_subscribe = 1u;
  rc = flowie_mqtt_fixed_subscriber_start(&subscriber);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&subscriber.result, &subscriber.done);
  flowie_mqtt_fixed_subscriber_destroy(&subscriber);
  return rc;
}

static int flowie_mqtt_fixed_publish_offline(
    const flowie_mqtt_live_case_t *test_case, const char *client_id, const char *topic,
    const char *payload, flowie_mqtt_span_t properties) {
  flowie_mqtt_fixed_publisher_t publisher = {0};
  publisher.test_case = test_case;
  publisher.client_id = client_id;
  publisher.topic = topic;
  publisher.payload = payload;
  publisher.publish_properties = properties;
  publisher.qos = 1u;
  return flowie_mqtt_fixed_publisher_run(&publisher);
}

static int flowie_mqtt_fixed_offline_replay_run(const flowie_mqtt_live_case_t *test_case) {
  static const char payload[] = "flowie-fixed-offline";
  flowie_mqtt_fixed_subscriber_t subscriber = {0};
  char subscriber_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char publisher_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  uint64_t unique = turbo_hrtime();
  int rc;
  if (snprintf(subscriber_id, sizeof(subscriber_id), "flowie-fixed-session-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(publisher_id, sizeof(publisher_id), "flowie-fixed-offline-pub-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(topic, sizeof(topic), "flowie/fixed/offline/%llu",
               (unsigned long long)unique) < 0)
    return TURBO_EIO;
  rc = flowie_mqtt_fixed_create_offline_session(test_case, subscriber_id, topic, payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_fixed_publish_offline(test_case, publisher_id, topic, payload,
                                         (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;

  subscriber.test_case = test_case;
  subscriber.client_id = subscriber_id;
  subscriber.topic = topic;
  subscriber.payload = payload;
  subscriber.connect_properties =
      (flowie_mqtt_span_t){FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY,
                           sizeof(FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY)};
  subscriber.expected_session_present = 1;
  subscriber.expected_retain = 0;
  subscriber.clean_start = 0u;
  subscriber.disconnect_after_message = 1u;
  rc = flowie_mqtt_fixed_subscriber_start(&subscriber);
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&subscriber.result, &subscriber.done);
  flowie_mqtt_fixed_subscriber_destroy(&subscriber);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&subscriber.received, memory_order_relaxed) == 1u ? TURBO_OK
                                                                               : TURBO_EPROTO;
}

static int flowie_mqtt_fixed_message_expiry_run(const flowie_mqtt_live_case_t *test_case) {
  static const char payload[] = "flowie-fixed-expiring";
  flowie_mqtt_fixed_subscriber_t subscriber = {0};
  char subscriber_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char publisher_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  uint64_t unique = turbo_hrtime();
  int rc;
  if (snprintf(subscriber_id, sizeof(subscriber_id), "flowie-fixed-expiry-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(publisher_id, sizeof(publisher_id), "flowie-fixed-expiry-pub-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(topic, sizeof(topic), "flowie/fixed/expiry/%llu",
               (unsigned long long)unique) < 0)
    return TURBO_EIO;
  rc = flowie_mqtt_fixed_create_offline_session(test_case, subscriber_id, topic, payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_fixed_publish_offline(
      test_case, publisher_id, topic, payload,
      (flowie_mqtt_span_t){FLOWIE_MQTT_FIXED_MESSAGE_EXPIRY_PROPERTY,
                           sizeof(FLOWIE_MQTT_FIXED_MESSAGE_EXPIRY_PROPERTY)});
  if (rc != TURBO_OK) return rc;
  turbo_sleep_ms(FLOWIE_MQTT_FIXED_EXPIRY_OBSERVE_MS);

  subscriber.test_case = test_case;
  subscriber.client_id = subscriber_id;
  subscriber.topic = topic;
  subscriber.payload = payload;
  subscriber.connect_properties =
      (flowie_mqtt_span_t){FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY,
                           sizeof(FLOWIE_MQTT_FIXED_SESSION_EXPIRY_PROPERTY)};
  subscriber.expected_session_present = 1;
  subscriber.expected_retain = 0;
  subscriber.clean_start = 0u;
  rc = flowie_mqtt_fixed_subscriber_start(&subscriber);
  if (rc == TURBO_OK)
    rc = flowie_mqtt_fixed_wait_ready(&subscriber.result, &subscriber.done, &subscriber.ready);
  if (rc == TURBO_OK) turbo_sleep_ms(FLOWIE_MQTT_FIXED_EXPIRY_OBSERVE_MS);
  if (rc == TURBO_OK &&
      atomic_load_explicit(&subscriber.received, memory_order_relaxed) != 0u)
    rc = TURBO_EPROTO;
  flowie_mqtt_fixed_subscriber_destroy(&subscriber);
  return rc;
}

static int flowie_mqtt_fixed_will_run(const flowie_mqtt_live_case_t *test_case) {
  static const char payload[] = "flowie-fixed-will";
  flowie_mqtt_fixed_subscriber_t watcher = {0};
  flowie_mqtt_fixed_publisher_t will_client = {0};
  char watcher_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char will_id[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  char topic[FLOWIE_MQTT_LIVE_BUFFER_SIZE];
  uint64_t unique = turbo_hrtime();
  int rc;
  if (snprintf(watcher_id, sizeof(watcher_id), "flowie-fixed-will-watch-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(will_id, sizeof(will_id), "flowie-fixed-will-owner-%llu",
               (unsigned long long)unique) < 0 ||
      snprintf(topic, sizeof(topic), "flowie/fixed/will/%llu",
               (unsigned long long)unique) < 0)
    return TURBO_EIO;
  watcher.test_case = test_case;
  watcher.client_id = watcher_id;
  watcher.topic = topic;
  watcher.payload = payload;
  watcher.expected_session_present = 0;
  watcher.expected_retain = 0;
  watcher.clean_start = 1u;
  watcher.subscribe = 1u;
  watcher.disconnect_after_message = 1u;
  rc = flowie_mqtt_fixed_subscriber_start(&watcher);
  if (rc == TURBO_OK)
    rc = flowie_mqtt_fixed_wait_ready(&watcher.result, &watcher.done, &watcher.ready);
  if (rc != TURBO_OK) {
    flowie_mqtt_fixed_subscriber_destroy(&watcher);
    return rc;
  }

  will_client.test_case = test_case;
  will_client.client_id = will_id;
  will_client.connect_only = 1u;
  will_client.has_will = 1u;
  will_client.will_qos = 1u;
  will_client.will_topic = topic;
  will_client.will_payload = payload;
  rc = flowie_mqtt_fixed_publisher_start(&will_client);
  if (rc == TURBO_OK)
    rc = flowie_mqtt_fixed_wait_ready(&will_client.result, &will_client.done, &will_client.ready);
  if (will_client.client) flowie_mqtt_client_destroy(will_client.client);
  will_client.client = NULL;
  if (rc == TURBO_OK) rc = flowie_mqtt_live_wait(&watcher.result, &watcher.done);
  flowie_mqtt_fixed_subscriber_destroy(&watcher);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&watcher.received, memory_order_relaxed) == 1u ? TURBO_OK
                                                                            : TURBO_EPROTO;
}
#endif

#if !defined(FLOWIE_MQTT_FIXED_INTEROP)
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

  it("round-trips MQTT 5 request-response properties through HiveMQ") {
    flowie_mqtt_live_reqrep_check(&FLOWIE_MQTT_LIVE_HIVEMQ_WS_5);
  }

  it("round-trips MQTT 5 request-response properties through EMQX") {
    flowie_mqtt_live_reqrep_check(&FLOWIE_MQTT_LIVE_EMQX_TLS_5);
  }

}
#else
spec("Flowie MQTT fixed-version broker interoperability") {
  it("MQTT-INTEROP-001 runs MQTT 3.1.1 and 5 core trace over fixed TCP") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_TCP_4);
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_TCP_5);
  }

  it("MQTT-INTEROP-001 runs MQTT 3.1.1 and 5 core trace over fixed TLS") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_TLS_4);
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_TLS_5);
  }

  it("MQTT-INTEROP-001 runs MQTT 3.1.1 and 5 core trace over fixed WS") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_WS_4);
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_WS_5);
  }

  it("MQTT-INTEROP-001 runs MQTT 3.1.1 and 5 core trace over fixed WSS") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_WSS_4);
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_WSS_5);
  }

  it("MQTT-INTEROP-004 preserves Response Topic and Correlation Data byte for byte") {
    flowie_mqtt_live_reqrep_check(&FLOWIE_MQTT_FIXED_TCP_5);
  }

  it("MQTT-INTEROP-004 reuses Topic Alias and returns Subscription Identifier") {
    flowie_mqtt_live_alias_check(&FLOWIE_MQTT_FIXED_TCP_5);
  }

  it("MQTT-INTEROP-003 receives and clears retained state through the fixed broker") {
    check_int_eq(flowie_mqtt_fixed_retained_run(&FLOWIE_MQTT_FIXED_TCP_5), TURBO_OK);
  }

  it("MQTT-INTEROP-003 resumes a fixed-broker session and replays its offline QoS1 message") {
    check_int_eq(flowie_mqtt_fixed_offline_replay_run(&FLOWIE_MQTT_FIXED_TCP_5), TURBO_OK);
  }

  it("MQTT-INTEROP-003 suppresses a fixed-broker offline message after its expiry interval") {
    int rc = flowie_mqtt_fixed_message_expiry_run(&FLOWIE_MQTT_FIXED_TCP_5);
    if (rc == TURBO_ETIMEDOUT) rc = flowie_mqtt_fixed_message_expiry_run(&FLOWIE_MQTT_FIXED_TCP_5);
    check_int_eq(rc, TURBO_OK);
  }

  it("MQTT-INTEROP-003 receives a fixed-broker Will after an ungraceful Flowie close") {
    check_int_eq(flowie_mqtt_fixed_will_run(&FLOWIE_MQTT_FIXED_TCP_5), TURBO_OK);
  }

#if FLOWIE_MQTT_FIXED_SUPPORT_31
  it("MQTT-INTEROP-005 runs supported MQTT 3.1 TCP and TLS without protocol fallback") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_TCP_3);
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_TLS_3);
  }
#endif
#if FLOWIE_MQTT_FIXED_SUPPORT_31_WS
  it("MQTT-INTEROP-005 runs declared MQTT 3.1 WS and WSS with the mqtt subprotocol") {
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_WS_3);
    flowie_mqtt_live_check(&FLOWIE_MQTT_FIXED_WSS_3);
  }
#endif
}
#endif
