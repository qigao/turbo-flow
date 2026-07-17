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
