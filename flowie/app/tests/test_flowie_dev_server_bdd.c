#include "flowie_mqtt_client.h"
#include "flowie_test_socket.h"

#include "platform.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_process.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef FLOWIE_DEV_SERVER_EXECUTABLE
  #error "FLOWIE_DEV_SERVER_EXECUTABLE must point to flowie_server"
#endif
#ifndef FLOWIE_DEV_CONFIG_PATH
  #error "FLOWIE_DEV_CONFIG_PATH must point to flowie-dev.yml"
#endif
#ifndef FLOWIE_DEV_GRAPH_PATH
  #error "FLOWIE_DEV_GRAPH_PATH must point to flowie-dev.flow"
#endif

#define FLOWIE_DEV_TEST_TIMEOUT_MS 5000u
#define FLOWIE_DEV_TEST_QUIET_MS 200u
#define FLOWIE_DEV_TEST_DIAGNOSTIC_CAPACITY 4096u
#define FLOWIE_DEV_TEST_MESSAGE_CAPACITY 3u
#define FLOWIE_DEV_TEST_TOPIC_CAPACITY 64u
#define FLOWIE_DEV_TEST_PAYLOAD_CAPACITY 128u

typedef struct flowie_dev_client_state_s {
  atomic_int connect_done;
  atomic_int connect_status;
  atomic_int connect_session_present;
  atomic_int subscribe_done;
  atomic_int subscribe_status;
  atomic_int unsubscribe_done;
  atomic_int unsubscribe_status;
  atomic_int disconnect_done;
  atomic_int disconnect_status;
  atomic_int publish_done;
  atomic_int publish_status;
  atomic_int publish_expected_type;
  atomic_int ping_done;
  atomic_int ping_status;
  atomic_int message_count;
  atomic_int background_error;
  uint8_t message_qos[FLOWIE_DEV_TEST_MESSAGE_CAPACITY];
  uint8_t message_retain[FLOWIE_DEV_TEST_MESSAGE_CAPACITY];
  char topics[FLOWIE_DEV_TEST_MESSAGE_CAPACITY][FLOWIE_DEV_TEST_TOPIC_CAPACITY];
  char payloads[FLOWIE_DEV_TEST_MESSAGE_CAPACITY][FLOWIE_DEV_TEST_PAYLOAD_CAPACITY];
} flowie_dev_client_state_t;

typedef struct flowie_dev_server_fixture_s {
  turbo_process_t *process;
  char *config_path;
  unsigned short port;
} flowie_dev_server_fixture_t;

static void flowie_dev_client_state_init(flowie_dev_client_state_t *state) {
  memset(state, 0, sizeof(*state));
  atomic_init(&state->connect_done, 0);
  atomic_init(&state->connect_status, TURBO_EBUSY);
  atomic_init(&state->connect_session_present, 0);
  atomic_init(&state->subscribe_done, 0);
  atomic_init(&state->subscribe_status, TURBO_EBUSY);
  atomic_init(&state->unsubscribe_done, 0);
  atomic_init(&state->unsubscribe_status, TURBO_EBUSY);
  atomic_init(&state->disconnect_done, 0);
  atomic_init(&state->disconnect_status, TURBO_EBUSY);
  atomic_init(&state->publish_done, 0);
  atomic_init(&state->publish_status, TURBO_OK);
  atomic_init(&state->publish_expected_type, 0);
  atomic_init(&state->ping_done, 0);
  atomic_init(&state->ping_status, TURBO_EBUSY);
  atomic_init(&state->message_count, 0);
  atomic_init(&state->background_error, TURBO_OK);
}

static int flowie_dev_completion_status(int status,
                                        const flowie_mqtt_control_packet_view_t *response,
                                        flowie_mqtt_packet_type_t expected_type) {
  if (status != TURBO_OK) return status;
  if (!response || response->type != expected_type || response->reason_code != 0u)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static void flowie_dev_connect_completion(flowie_mqtt_client_t *client, int status,
                                          const flowie_mqtt_control_packet_view_t *response,
                                          void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  (void)client;
  status = flowie_dev_completion_status(status, response, FLOWIE_MQTT_PACKET_CONNACK);
  if (status == TURBO_OK)
    atomic_store_explicit(&state->connect_session_present, response->session_present,
                          memory_order_relaxed);
  atomic_store_explicit(&state->connect_status, status, memory_order_relaxed);
  atomic_store_explicit(&state->connect_done, 1, memory_order_release);
}

static void flowie_dev_subscribe_completion(flowie_mqtt_client_t *client, int status,
                                            const flowie_mqtt_control_packet_view_t *response,
                                            void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  (void)client;
  status = flowie_dev_completion_status(status, response, FLOWIE_MQTT_PACKET_SUBACK);
  atomic_store_explicit(&state->subscribe_status, status, memory_order_relaxed);
  atomic_store_explicit(&state->subscribe_done, 1, memory_order_release);
}

static void flowie_dev_unsubscribe_completion(flowie_mqtt_client_t *client, int status,
                                              const flowie_mqtt_control_packet_view_t *response,
                                              void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  (void)client;
  status = flowie_dev_completion_status(status, response, FLOWIE_MQTT_PACKET_UNSUBACK);
  atomic_store_explicit(&state->unsubscribe_status, status, memory_order_relaxed);
  atomic_store_explicit(&state->unsubscribe_done, 1, memory_order_release);
}

static void flowie_dev_publish_completion(flowie_mqtt_client_t *client, int status,
                                          const flowie_mqtt_control_packet_view_t *response,
                                          void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  int expected_type = atomic_load_explicit(&state->publish_expected_type, memory_order_relaxed);
  int index = atomic_load_explicit(&state->publish_done, memory_order_relaxed);
  (void)client;
  if (status == TURBO_OK) {
    if (expected_type == 0) {
      if (response) status = TURBO_EPROTO;
    } else if (!response || response->type != (flowie_mqtt_packet_type_t)expected_type ||
               response->reason_code != 0u) {
      status = TURBO_EPROTO;
    }
  }
  if (status != TURBO_OK)
    atomic_store_explicit(&state->publish_status, status, memory_order_relaxed);
  atomic_store_explicit(&state->publish_done, index + 1, memory_order_release);
}

static void flowie_dev_ping_completion(flowie_mqtt_client_t *client, int status,
                                       const flowie_mqtt_control_packet_view_t *response,
                                       void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  atomic_store_explicit(&state->ping_status, status, memory_order_relaxed);
  atomic_fetch_add_explicit(&state->ping_done, 1, memory_order_release);
}

static void flowie_dev_disconnect_completion(flowie_mqtt_client_t *client, int status,
                                             const flowie_mqtt_control_packet_view_t *response,
                                             void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  (void)client;
  if (status == TURBO_OK && response) status = TURBO_EPROTO;
  atomic_store_explicit(&state->disconnect_status, status, memory_order_relaxed);
  atomic_store_explicit(&state->disconnect_done, 1, memory_order_release);
}

static void flowie_dev_background_error(flowie_mqtt_client_t *client, int status, void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  int expected = TURBO_OK;
  (void)client;
  (void)atomic_compare_exchange_strong_explicit(&state->background_error, &expected, status,
                                                memory_order_release, memory_order_relaxed);
}

static int flowie_dev_message(flowie_mqtt_client_t *client,
                              const flowie_mqtt_publish_view_t *message, void *user_data) {
  flowie_dev_client_state_t *state = (flowie_dev_client_state_t *)user_data;
  int index = atomic_load_explicit(&state->message_count, memory_order_relaxed);
  (void)client;
  if (!message || index < 0 || index >= (int)FLOWIE_DEV_TEST_MESSAGE_CAPACITY ||
      message->topic.size >= FLOWIE_DEV_TEST_TOPIC_CAPACITY ||
      message->payload.size >= FLOWIE_DEV_TEST_PAYLOAD_CAPACITY)
    return TURBO_EMSGSIZE;
  memcpy(state->topics[index], message->topic.data, message->topic.size);
  state->topics[index][message->topic.size] = '\0';
  memcpy(state->payloads[index], message->payload.data, message->payload.size);
  state->payloads[index][message->payload.size] = '\0';
  state->message_qos[index] = message->qos;
  state->message_retain[index] = message->retain;
  atomic_store_explicit(&state->message_count, index + 1, memory_order_release);
  return TURBO_OK;
}

static int flowie_dev_wait_for(const atomic_int *value, int expected) {
  uint64_t deadline = turbo_monotonic_ms() + FLOWIE_DEV_TEST_TIMEOUT_MS;
  while (atomic_load_explicit(value, memory_order_acquire) < expected &&
         turbo_monotonic_ms() < deadline)
    turbo_sleep_ms(1u);
  return atomic_load_explicit(value, memory_order_acquire) >= expected ? TURBO_OK : TURBO_ETIMEDOUT;
}

static int flowie_dev_expect_message(flowie_dev_client_state_t *state, int expected_count,
                                     const uint8_t *topic, const uint8_t *payload) {
  int rc = flowie_dev_wait_for(&state->message_count, expected_count);
  if (rc != TURBO_OK) return rc;
  if (strcmp(state->topics[expected_count - 1], (const char *)topic) != 0 ||
      strcmp(state->payloads[expected_count - 1], (const char *)payload) != 0)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_dev_expect_message_flags(flowie_dev_client_state_t *state, int expected_count,
                                           const uint8_t *topic, const uint8_t *payload,
                                           uint8_t qos, uint8_t retain) {
  int rc = flowie_dev_expect_message(state, expected_count, topic, payload);
  if (rc != TURBO_OK) return rc;
  return state->message_qos[expected_count - 1] == qos &&
                 state->message_retain[expected_count - 1] == retain
             ? TURBO_OK
             : TURBO_EPROTO;
}

static int flowie_dev_expect_message_count_quiet(flowie_dev_client_state_t *state,
                                                 int expected_count) {
  turbo_sleep_ms(FLOWIE_DEV_TEST_QUIET_MS);
  return atomic_load_explicit(&state->message_count, memory_order_acquire) == expected_count
             ? TURBO_OK
             : TURBO_EPROTO;
}

static int flowie_dev_write_config(unsigned short port, char **path_out) {
  static const char marker[] = "      port: 1883";
  char replacement[sizeof(marker) + 8u];
  char *source = NULL;
  char *output = NULL;
  char *match;
  char *second;
  char *path = NULL;
  size_t source_size = 0u;
  size_t prefix_size;
  size_t replacement_size;
  size_t suffix_size;
  size_t output_size;
  int written;
  int rc = TURBO_EIO;

  if (path_out) *path_out = NULL;
  if (!path_out || port == 0u) return TURBO_EINVAL;
  source = tt_read_file(FLOWIE_DEV_CONFIG_PATH, &source_size);
  if (!source) goto cleanup;
  match = strstr(source, marker);
  if (!match) goto cleanup;
  second = strstr(match + sizeof(marker) - 1u, marker);
  if (second) goto cleanup;
  written = snprintf(replacement, sizeof(replacement), "      port: %u", (unsigned int)port);
  if (written <= 0 || (size_t)written >= sizeof(replacement)) goto cleanup;
  prefix_size = (size_t)(match - source);
  replacement_size = (size_t)written;
  suffix_size = source_size - prefix_size - (sizeof(marker) - 1u);
  if (prefix_size > SIZE_MAX - replacement_size ||
      prefix_size + replacement_size > SIZE_MAX - suffix_size)
    goto cleanup;
  output_size = prefix_size + replacement_size + suffix_size;
  output = (char *)malloc(output_size + 1u);
  if (!output) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  memcpy(output, source, prefix_size);
  memcpy(output + prefix_size, replacement, replacement_size);
  memcpy(output + prefix_size + replacement_size, match + sizeof(marker) - 1u, suffix_size);
  output[output_size] = '\0';
  path = tt_make_temp_file("flowie-dev-bdd", ".yml");
  if (!path || tt_write_file(path, output, output_size) != 0) goto cleanup;
  *path_out = path;
  path = NULL;
  rc = TURBO_OK;

cleanup:
  if (path) {
    (void)tt_remove_file(path);
    free(path);
  }
  free(output);
  free(source);
  return rc;
}

static int flowie_dev_wait_until_listening(turbo_process_t *process, unsigned short port) {
  uint64_t deadline = turbo_monotonic_ms() + FLOWIE_DEV_TEST_TIMEOUT_MS;
  while (turbo_monotonic_ms() < deadline) {
    turbo_process_result_t result;
    flowie_test_socket_t socket_handle;
    int rc = turbo_process_poll(process, &result);
    if (rc == TURBO_OK) return TURBO_ECONNREFUSED;
    if (rc != TURBO_EBUSY) return rc;
    socket_handle = flowie_test_connect(port);
    if (socket_handle != FLOWIE_TEST_INVALID_SOCKET) {
      flowie_test_socket_close(socket_handle);
      return TURBO_OK;
    }
    turbo_sleep_ms(10u);
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_dev_connect_packet(flowie_mqtt_client_t *client, flowie_dev_client_state_t *state,
                                     const flowie_mqtt_connect_packet_t *connect,
                                     int expected_session_present) {
  int rc;
  rc = flowie_mqtt_client_connect(client, connect);
  if (rc != TURBO_OK) return rc;
  rc = flowie_dev_wait_for(&state->connect_done, 1);
  if (rc != TURBO_OK) return rc;
  rc = atomic_load_explicit(&state->connect_status, memory_order_relaxed);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&state->connect_session_present, memory_order_relaxed) ==
                 expected_session_present
             ? TURBO_OK
             : TURBO_EPROTO;
}

static int flowie_dev_connect_client(flowie_mqtt_client_t *client, flowie_dev_client_state_t *state,
                                     flowie_mqtt_version_t version, const uint8_t *client_id,
                                     size_t client_id_size) {
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  connect.version = version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, client_id_size};
  return flowie_dev_connect_packet(client, state, &connect, 0);
}

static int flowie_dev_publish_ex(flowie_mqtt_client_t *client, flowie_dev_client_state_t *state,
                                 uint8_t qos, uint8_t retain, flowie_mqtt_version_t version,
                                 const uint8_t *topic, size_t topic_size, const uint8_t *payload,
                                 size_t payload_size, int expected_completion_count) {
  flowie_mqtt_client_publish_topic_t item = {0};
  flowie_mqtt_client_publish_topic_vec_t request = FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  int rc;
  item.qos = qos;
  item.retain = retain;
  item.topic = (flowie_mqtt_span_t){topic, topic_size};
  item.payload = (flowie_mqtt_span_t){payload, payload_size};
  request.version = version;
  request.data = &item;
  request.count = 1u;
  atomic_store_explicit(&state->publish_expected_type,
                        qos == 1u   ? FLOWIE_MQTT_PACKET_PUBACK
                        : qos == 2u ? FLOWIE_MQTT_PACKET_PUBCOMP
                                    : 0,
                        memory_order_relaxed);
  rc = flowie_mqtt_client_publish(client, &request);
  if (rc != TURBO_OK) return rc;
  rc = flowie_dev_wait_for(&state->publish_done, expected_completion_count);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&state->publish_status, memory_order_relaxed);
}

static int flowie_dev_publish(flowie_mqtt_client_t *client, flowie_dev_client_state_t *state,
                              uint8_t qos, flowie_mqtt_version_t version, const uint8_t *topic,
                              size_t topic_size, const uint8_t *payload, size_t payload_size,
                              int expected_completion_count) {
  return flowie_dev_publish_ex(client, state, qos, 0u, version, topic, topic_size, payload,
                               payload_size, expected_completion_count);
}

static int flowie_dev_subscribe(flowie_mqtt_client_t *client, flowie_dev_client_state_t *state,
                                flowie_mqtt_version_t version, const uint8_t *filter,
                                size_t filter_size, uint8_t qos) {
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  int rc;
  subscription.filter = (flowie_mqtt_span_t){filter, filter_size};
  subscription.qos = qos;
  subscribe.version = version;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  rc = flowie_mqtt_client_subscribe(client, &subscribe);
  if (rc != TURBO_OK) return rc;
  rc = flowie_dev_wait_for(&state->subscribe_done, 1);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&state->subscribe_status, memory_order_relaxed);
}

static int flowie_dev_ping_client(flowie_mqtt_client_t *client, flowie_dev_client_state_t *state,
                                  int expected_count) {
  int rc = flowie_mqtt_client_ping(client);
  if (rc != TURBO_OK) return rc;
  rc = flowie_dev_wait_for(&state->ping_done, expected_count);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&state->ping_status, memory_order_relaxed);
}

static int flowie_dev_create_client(unsigned short port, flowie_dev_client_state_t *state,
                                    const uint8_t *filter, size_t filter_size,
                                    flowie_mqtt_client_t **out) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_client_topic_handler_t handler = {0};
  config.host = "127.0.0.1";
  config.port = port;
  config.timeout_ms = FLOWIE_DEV_TEST_TIMEOUT_MS;
  config.on_connect = flowie_dev_connect_completion;
  config.on_publish = flowie_dev_publish_completion;
  config.on_subscribe = flowie_dev_subscribe_completion;
  config.on_unsubscribe = flowie_dev_unsubscribe_completion;
  config.on_ping = flowie_dev_ping_completion;
  config.on_disconnect = flowie_dev_disconnect_completion;
  config.on_error = flowie_dev_background_error;
  config.user_data = state;
  if (filter && filter_size != 0u) {
    handler.filter = (flowie_mqtt_span_t){filter, filter_size};
    handler.on_message = flowie_dev_message;
    config.topic_handlers = (flowie_mqtt_client_topic_handler_map_t){&handler, 1u};
  }
  return flowie_mqtt_client_create(&config, out);
}

static int flowie_dev_disconnect_client(flowie_mqtt_client_t *client,
                                        flowie_dev_client_state_t *state) {
  int rc = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) return rc;
  rc = flowie_dev_wait_for(&state->disconnect_done, 1);
  if (rc != TURBO_OK) return rc;
  return atomic_load_explicit(&state->disconnect_status, memory_order_relaxed);
}

static void flowie_dev_print_child_stderr(turbo_process_t *process) {
  char diagnostic[FLOWIE_DEV_TEST_DIAGNOSTIC_CAPACITY];
  size_t size = 0u;
  int rc;
  if (!process) return;
  rc = turbo_process_read_stderr(process, diagnostic, sizeof(diagnostic) - 1u, &size);
  if ((rc == TURBO_OK || rc == TURBO_EOF) && size != 0u) {
    diagnostic[size] = '\0';
    (void)fprintf(stderr, "flowie dev server stderr: %s\n", diagnostic);
  }
}

static int flowie_dev_server_start(flowie_dev_server_fixture_t *fixture) {
  turbo_process_options_t options;
  const char *args[] = {NULL, FLOWIE_DEV_GRAPH_PATH, NULL};
  int rc;
  if (!fixture) return TURBO_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  fixture->port = flowie_test_port();
  if (fixture->port == 0u) return TURBO_EIO;
  rc = flowie_dev_write_config(fixture->port, &fixture->config_path);
  if (rc != TURBO_OK) return rc;
  args[0] = fixture->config_path;
  turbo_process_options_init(&options);
  options.program = FLOWIE_DEV_SERVER_EXECUTABLE;
  options.args = args;
  options.flags = TURBO_PROCESS_CAPTURE_STDOUT | TURBO_PROCESS_CAPTURE_STDERR;
  options.max_output_bytes = 65536u;
  rc = turbo_process_spawn(&options, &fixture->process);
  if (rc != TURBO_OK) return rc;
  return flowie_dev_wait_until_listening(fixture->process, fixture->port);
}

static int flowie_dev_server_stop(flowie_dev_server_fixture_t *fixture, int scenario_status) {
  turbo_process_result_t result;
  int stop_rc = TURBO_OK;
  if (!fixture) return scenario_status == TURBO_OK ? TURBO_EINVAL : scenario_status;
  if (fixture->process) {
    stop_rc = turbo_process_poll(fixture->process, &result);
    if (stop_rc == TURBO_EBUSY) {
      stop_rc = turbo_process_terminate(fixture->process);
      if (stop_rc == TURBO_OK) stop_rc = turbo_process_wait(fixture->process, &result);
    } else if (stop_rc == TURBO_OK && scenario_status == TURBO_OK) {
      scenario_status = TURBO_EIO;
      (void)fprintf(stderr, "flowie dev server exited early: child state=%s\n",
                    turbo_process_state_name(result.state));
    }
    if (stop_rc != TURBO_OK && scenario_status == TURBO_OK) scenario_status = stop_rc;
    if (scenario_status != TURBO_OK) flowie_dev_print_child_stderr(fixture->process);
    turbo_process_destroy(fixture->process);
  }
  if (fixture->config_path) {
    (void)tt_remove_file(fixture->config_path);
    free(fixture->config_path);
  }
  memset(fixture, 0, sizeof(*fixture));
  return scenario_status;
}

static int flowie_dev_run_scenario(flowie_mqtt_version_t version) {
  static const uint8_t publisher_id[] = "flowie-dev-bdd-publisher";
  static const uint8_t subscriber_a_id[] = "flowie-dev-bdd-subscriber-a";
  static const uint8_t subscriber_b_id[] = "flowie-dev-bdd-subscriber-b";
  static const uint8_t filter[] = "bdd/dev/#";
  static const uint8_t topic[] = "bdd/dev/message";
  static const uint8_t qos0_payload[] = "hello-qos0";
  static const uint8_t qos1_payload[] = "hello-qos1";
  static const uint8_t after_unsubscribe_payload[] = "after-unsubscribe";
  flowie_dev_client_state_t publisher_state;
  flowie_dev_client_state_t subscriber_a_state;
  flowie_dev_client_state_t subscriber_b_state;
  flowie_mqtt_client_config_t publisher_config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_client_config_t subscriber_config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_client_topic_handler_t handler = {0};
  flowie_mqtt_client_t *publisher = NULL;
  flowie_mqtt_client_t *subscriber_a = NULL;
  flowie_mqtt_client_t *subscriber_b = NULL;
  flowie_dev_server_fixture_t server;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_span_t unsubscribe_filter = {filter, sizeof(filter) - 1u};
  const char *failure_stage = "initialization";
  int rc = TURBO_EIO;

  flowie_dev_client_state_init(&publisher_state);
  flowie_dev_client_state_init(&subscriber_a_state);
  flowie_dev_client_state_init(&subscriber_b_state);
  failure_stage = "start flowie_server";
  rc = flowie_dev_server_start(&server);
  if (rc != TURBO_OK) goto cleanup;

  publisher_config.host = "127.0.0.1";
  publisher_config.port = server.port;
  publisher_config.timeout_ms = FLOWIE_DEV_TEST_TIMEOUT_MS;
  publisher_config.on_connect = flowie_dev_connect_completion;
  publisher_config.on_publish = flowie_dev_publish_completion;
  publisher_config.on_disconnect = flowie_dev_disconnect_completion;
  publisher_config.on_error = flowie_dev_background_error;
  publisher_config.user_data = &publisher_state;
  subscriber_config = publisher_config;
  subscriber_config.on_publish = NULL;
  subscriber_config.on_subscribe = flowie_dev_subscribe_completion;
  subscriber_config.on_unsubscribe = flowie_dev_unsubscribe_completion;
  handler.filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  handler.on_message = flowie_dev_message;
  subscriber_config.topic_handlers = (flowie_mqtt_client_topic_handler_map_t){&handler, 1u};

  failure_stage = "create MQTT clients";
  subscriber_config.user_data = &subscriber_a_state;
  rc = flowie_mqtt_client_create(&subscriber_config, &subscriber_a);
  if (rc != TURBO_OK) goto cleanup;
  subscriber_config.user_data = &subscriber_b_state;
  rc = flowie_mqtt_client_create(&subscriber_config, &subscriber_b);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_mqtt_client_create(&publisher_config, &publisher);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "CONNECT subscriber A";
  rc = flowie_dev_connect_client(subscriber_a, &subscriber_a_state, version, subscriber_a_id,
                                 sizeof(subscriber_a_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "CONNECT subscriber B";
  rc = flowie_dev_connect_client(subscriber_b, &subscriber_b_state, version, subscriber_b_id,
                                 sizeof(subscriber_b_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "CONNECT publisher";
  rc = flowie_dev_connect_client(publisher, &publisher_state, version, publisher_id,
                                 sizeof(publisher_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;

  subscription.filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  subscription.qos = 1u;
  subscribe.version = version;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  failure_stage = "SUBSCRIBE subscriber A";
  rc = flowie_mqtt_client_subscribe(subscriber_a, &subscribe);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_wait_for(&subscriber_a_state.subscribe_done, 1);
  if (rc == TURBO_OK)
    rc = atomic_load_explicit(&subscriber_a_state.subscribe_status, memory_order_relaxed);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "SUBSCRIBE subscriber B";
  rc = flowie_mqtt_client_subscribe(subscriber_b, &subscribe);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_wait_for(&subscriber_b_state.subscribe_done, 1);
  if (rc == TURBO_OK)
    rc = atomic_load_explicit(&subscriber_b_state.subscribe_status, memory_order_relaxed);
  if (rc != TURBO_OK) goto cleanup;

  failure_stage = "PUBLISH QoS 0";
  rc = flowie_dev_publish(publisher, &publisher_state, 0u, version, topic, sizeof(topic) - 1u,
                          qos0_payload, sizeof(qos0_payload) - 1u, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message(&subscriber_a_state, 1, topic, qos0_payload);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message(&subscriber_b_state, 1, topic, qos0_payload);
  if (rc != TURBO_OK) goto cleanup;

  failure_stage = "PUBLISH QoS 1 and receive PUBACK";
  rc = flowie_dev_publish(publisher, &publisher_state, 1u, version, topic, sizeof(topic) - 1u,
                          qos1_payload, sizeof(qos1_payload) - 1u, 2);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message(&subscriber_a_state, 2, topic, qos1_payload);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message(&subscriber_b_state, 2, topic, qos1_payload);
  if (rc != TURBO_OK) goto cleanup;

  unsubscribe.version = version;
  unsubscribe.filters = &unsubscribe_filter;
  unsubscribe.filter_count = 1u;
  failure_stage = "UNSUBSCRIBE subscriber A";
  rc = flowie_mqtt_client_unsubscribe(subscriber_a, &unsubscribe);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_wait_for(&subscriber_a_state.unsubscribe_done, 1);
  if (rc == TURBO_OK)
    rc = atomic_load_explicit(&subscriber_a_state.unsubscribe_status, memory_order_relaxed);
  if (rc != TURBO_OK) goto cleanup;

  failure_stage = "verify unsubscribe only removes subscriber A";
  rc = flowie_dev_publish(publisher, &publisher_state, 0u, version, topic, sizeof(topic) - 1u,
                          after_unsubscribe_payload, sizeof(after_unsubscribe_payload) - 1u, 3);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message(&subscriber_b_state, 3, topic, after_unsubscribe_payload);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_DEV_TEST_QUIET_MS);
  if (atomic_load_explicit(&subscriber_a_state.message_count, memory_order_acquire) != 2) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }

  failure_stage = "DISCONNECT clients";
  rc = flowie_dev_disconnect_client(publisher, &publisher_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber_a, &subscriber_a_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber_b, &subscriber_b_state);
  if (rc != TURBO_OK) goto cleanup;
  if (atomic_load_explicit(&publisher_state.background_error, memory_order_acquire) != TURBO_OK ||
      atomic_load_explicit(&subscriber_a_state.background_error, memory_order_acquire) !=
          TURBO_OK ||
      atomic_load_explicit(&subscriber_b_state.background_error, memory_order_acquire) !=
          TURBO_OK) {
    rc = TURBO_EIO;
    goto cleanup;
  }
  rc = TURBO_OK;

cleanup:
  if (rc != TURBO_OK)
    (void)fprintf(stderr, "flowie dev BDD failed at %s: status=%d\n", failure_stage, rc);
  flowie_mqtt_client_destroy(publisher);
  flowie_mqtt_client_destroy(subscriber_b);
  flowie_mqtt_client_destroy(subscriber_a);
  return flowie_dev_server_stop(&server, rc);
}

static int flowie_dev_run_immediate_publisher_close_scenario(void) {
  static const uint8_t subscriber_id[] = "flowie-dev-immediate-subscriber";
  static const uint8_t topic[] = "bdd/immediate";
  static const uint8_t payload[] = "after-close";
  static const uint8_t connect_packet[] = {
      0x10u, 0x19u, 0x00u, 0x04u, 'M',  'Q',  'T',  'T', 0x04u,
      0x02u, 0x00u, 0x3cu, 0x00u, 0x0du, 'r', 'a', 'w', '-',  'p',
      'u',   'b',   'l',   'i',   's',   'h',  'e', 'r'};
  static const uint8_t connack[] = {0x20u, 0x02u, 0x00u, 0x00u};
  static const uint8_t publish_packet[] = {
      0x30u, 0x1au, 0x00u, 0x0du, 'b', 'd', 'd', '/', 'i', 'm', 'm', 'e', 'd',
      'i',   'a',   't',   'e',   'a', 'f', 't', 'e', 'r', '-', 'c', 'l', 'o',
      's',   'e'};
  flowie_dev_server_fixture_t server;
  flowie_dev_client_state_t subscriber_state;
  flowie_mqtt_client_t *subscriber = NULL;
  flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
  uint8_t response[sizeof(connack)];
  const char *failure_stage = "initialization";
  int rc;

  flowie_dev_client_state_init(&subscriber_state);
  rc = flowie_dev_server_start(&server);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "create and subscribe the receiving client";
  rc = flowie_dev_create_client(server.port, &subscriber_state, topic, sizeof(topic) - 1u,
                                &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_connect_client(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_3_1_1,
                                 subscriber_id, sizeof(subscriber_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_subscribe(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_3_1_1, topic,
                            sizeof(topic) - 1u, 0u);
  if (rc != TURBO_OK) goto cleanup;

  failure_stage = "publish QoS 0 and close before the asynchronous graph returns";
  publisher = flowie_test_connect(server.port);
  if (publisher == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ECONNREFUSED;
    goto cleanup;
  }
  rc = flowie_test_send(publisher, connect_packet, sizeof(connect_packet));
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_test_recv_exact(publisher, response, sizeof(response));
  if (rc != TURBO_OK || memcmp(response, connack, sizeof(connack)) != 0) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  rc = flowie_test_send(publisher, publish_packet, sizeof(publish_packet));
  if (rc != TURBO_OK) goto cleanup;
  flowie_test_socket_close(publisher);
  publisher = FLOWIE_TEST_INVALID_SOCKET;
  rc = flowie_dev_expect_message_flags(&subscriber_state, 1, topic, payload, 0u, 0u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber, &subscriber_state);

cleanup:
  if (rc != TURBO_OK)
    (void)fprintf(stderr, "flowie dev immediate-close BDD failed at %s: status=%d\n",
                  failure_stage, rc);
  flowie_test_socket_close(publisher);
  flowie_mqtt_client_destroy(subscriber);
  return flowie_dev_server_stop(&server, rc);
}

static int flowie_dev_run_qos2_scenario(void) {
  static const uint8_t publisher_id[] = "flowie-dev-qos2-publisher";
  static const uint8_t subscriber_a_id[] = "flowie-dev-qos2-subscriber-a";
  static const uint8_t subscriber_b_id[] = "flowie-dev-qos2-subscriber-b";
  static const uint8_t filter[] = "bdd/qos2/#";
  static const uint8_t topic[] = "bdd/qos2/message";
  static const uint8_t payload[] = "exactly-once";
  flowie_dev_server_fixture_t server;
  flowie_dev_client_state_t publisher_state;
  flowie_dev_client_state_t subscriber_a_state;
  flowie_dev_client_state_t subscriber_b_state;
  flowie_mqtt_client_t *publisher = NULL;
  flowie_mqtt_client_t *subscriber_a = NULL;
  flowie_mqtt_client_t *subscriber_b = NULL;
  const char *failure_stage = "initialization";
  int rc;

  flowie_dev_client_state_init(&publisher_state);
  flowie_dev_client_state_init(&subscriber_a_state);
  flowie_dev_client_state_init(&subscriber_b_state);
  rc = flowie_dev_server_start(&server);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "create QoS 2 clients";
  rc = flowie_dev_create_client(server.port, &publisher_state, NULL, 0u, &publisher);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_create_client(server.port, &subscriber_a_state, filter, sizeof(filter) - 1u,
                                &subscriber_a);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_create_client(server.port, &subscriber_b_state, filter, sizeof(filter) - 1u,
                                &subscriber_b);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "connect QoS 2 clients";
  rc = flowie_dev_connect_client(publisher, &publisher_state, FLOWIE_MQTT_VERSION_5, publisher_id,
                                 sizeof(publisher_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_connect_client(subscriber_a, &subscriber_a_state, FLOWIE_MQTT_VERSION_5,
                                 subscriber_a_id, sizeof(subscriber_a_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_connect_client(subscriber_b, &subscriber_b_state, FLOWIE_MQTT_VERSION_5,
                                 subscriber_b_id, sizeof(subscriber_b_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "subscribe both clients at QoS 2";
  rc = flowie_dev_subscribe(subscriber_a, &subscriber_a_state, FLOWIE_MQTT_VERSION_5, filter,
                            sizeof(filter) - 1u, 2u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_subscribe(subscriber_b, &subscriber_b_state, FLOWIE_MQTT_VERSION_5, filter,
                            sizeof(filter) - 1u, 2u);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "complete publisher and subscriber QoS 2 handshakes";
  rc = flowie_dev_publish(publisher, &publisher_state, 2u, FLOWIE_MQTT_VERSION_5, topic,
                          sizeof(topic) - 1u, payload, sizeof(payload) - 1u, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_flags(&subscriber_a_state, 1, topic, payload, 2u, 0u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_flags(&subscriber_b_state, 1, topic, payload, 2u, 0u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_ping_client(subscriber_a, &subscriber_a_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_ping_client(subscriber_b, &subscriber_b_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_DEV_TEST_QUIET_MS);
  if (atomic_load_explicit(&subscriber_a_state.message_count, memory_order_acquire) != 1 ||
      atomic_load_explicit(&subscriber_b_state.message_count, memory_order_acquire) != 1) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  failure_stage = "disconnect QoS 2 clients";
  rc = flowie_dev_disconnect_client(publisher, &publisher_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber_a, &subscriber_a_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber_b, &subscriber_b_state);

cleanup:
  if (rc != TURBO_OK)
    (void)fprintf(stderr, "flowie dev QoS 2 BDD failed at %s: status=%d\n", failure_stage, rc);
  flowie_mqtt_client_destroy(subscriber_b);
  flowie_mqtt_client_destroy(subscriber_a);
  flowie_mqtt_client_destroy(publisher);
  return flowie_dev_server_stop(&server, rc);
}

static int flowie_dev_run_session_resume_scenario(void) {
  static const uint8_t session_properties[] = {0x11u, 0x00u, 0x00u, 0x00u, 0x3cu};
  static const uint8_t subscriber_id[] = "flowie-dev-session-subscriber";
  static const uint8_t publisher_id[] = "flowie-dev-session-publisher";
  static const uint8_t filter[] = "bdd/session/#";
  static const uint8_t topic[] = "bdd/session/offline";
  static const uint8_t payload[] = "queued-while-offline";
  flowie_dev_server_fixture_t server;
  flowie_dev_client_state_t subscriber_state;
  flowie_dev_client_state_t resumed_state;
  flowie_dev_client_state_t clean_state;
  flowie_dev_client_state_t publisher_state;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_t *subscriber = NULL;
  flowie_mqtt_client_t *publisher = NULL;
  const char *failure_stage = "initialization";
  int rc;

  flowie_dev_client_state_init(&subscriber_state);
  flowie_dev_client_state_init(&resumed_state);
  flowie_dev_client_state_init(&clean_state);
  flowie_dev_client_state_init(&publisher_state);
  rc = flowie_dev_server_start(&server);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "create persistent session clients";
  rc = flowie_dev_create_client(server.port, &subscriber_state, filter, sizeof(filter) - 1u,
                                &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_create_client(server.port, &publisher_state, NULL, 0u, &publisher);
  if (rc != TURBO_OK) goto cleanup;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.properties = (flowie_mqtt_span_t){session_properties, sizeof(session_properties)};
  connect.client_id = (flowie_mqtt_span_t){subscriber_id, sizeof(subscriber_id) - 1u};
  failure_stage = "create persistent subscriber session";
  rc = flowie_dev_connect_packet(subscriber, &subscriber_state, &connect, 0);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_subscribe(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_5, filter,
                            sizeof(filter) - 1u, 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber, &subscriber_state);
  if (rc != TURBO_OK) goto cleanup;
  flowie_mqtt_client_destroy(subscriber);
  subscriber = NULL;
  failure_stage = "publish while persistent subscriber is offline";
  rc = flowie_dev_connect_client(publisher, &publisher_state, FLOWIE_MQTT_VERSION_5, publisher_id,
                                 sizeof(publisher_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_publish(publisher, &publisher_state, 1u, FLOWIE_MQTT_VERSION_5, topic,
                          sizeof(topic) - 1u, payload, sizeof(payload) - 1u, 1);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "resume session and replay offline delivery";
  rc = flowie_dev_create_client(server.port, &resumed_state, filter, sizeof(filter) - 1u,
                                &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  connect.clean_start = 0u;
  rc = flowie_dev_connect_packet(subscriber, &resumed_state, &connect, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_flags(&resumed_state, 1, topic, payload, 1u, 0u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_ping_client(subscriber, &resumed_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber, &resumed_state);
  if (rc != TURBO_OK) goto cleanup;
  flowie_mqtt_client_destroy(subscriber);
  subscriber = NULL;

  failure_stage = "replace resumed state at the explicit clean-session boundary";
  rc = flowie_dev_create_client(server.port, &clean_state, filter, sizeof(filter) - 1u,
                                &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  connect.clean_start = 1u;
  connect.properties = (flowie_mqtt_span_t){NULL, 0u};
  rc = flowie_dev_connect_packet(subscriber, &clean_state, &connect, 0);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_publish(publisher, &publisher_state, 1u, FLOWIE_MQTT_VERSION_5, topic,
                          sizeof(topic) - 1u, payload, sizeof(payload) - 1u, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_ping_client(subscriber, &clean_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_count_quiet(&clean_state, 0);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber, &clean_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(publisher, &publisher_state);

cleanup:
  if (rc != TURBO_OK)
    (void)fprintf(stderr, "flowie dev session BDD failed at %s: status=%d\n", failure_stage, rc);
  flowie_mqtt_client_destroy(subscriber);
  flowie_mqtt_client_destroy(publisher);
  return flowie_dev_server_stop(&server, rc);
}

static int flowie_dev_run_retained_scenario(void) {
  static const uint8_t publisher_id[] = "flowie-dev-retained-publisher";
  static const uint8_t subscriber_id[] = "flowie-dev-retained-subscriber";
  static const uint8_t after_delete_id[] = "flowie-dev-retained-after-delete";
  static const uint8_t filter[] = "bdd/retained/#";
  static const uint8_t topic[] = "bdd/retained/value";
  static const uint8_t payload[] = "retained-value";
  flowie_dev_server_fixture_t server;
  flowie_dev_client_state_t publisher_state;
  flowie_dev_client_state_t subscriber_state;
  flowie_dev_client_state_t after_delete_state;
  flowie_mqtt_client_t *publisher = NULL;
  flowie_mqtt_client_t *subscriber = NULL;
  flowie_mqtt_client_t *after_delete = NULL;
  const char *failure_stage = "initialization";
  int rc;

  flowie_dev_client_state_init(&publisher_state);
  flowie_dev_client_state_init(&subscriber_state);
  flowie_dev_client_state_init(&after_delete_state);
  rc = flowie_dev_server_start(&server);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "create retained clients";
  rc = flowie_dev_create_client(server.port, &publisher_state, NULL, 0u, &publisher);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_create_client(server.port, &subscriber_state, filter, sizeof(filter) - 1u,
                                &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_connect_client(publisher, &publisher_state, FLOWIE_MQTT_VERSION_5, publisher_id,
                                 sizeof(publisher_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "store retained publication";
  rc = flowie_dev_publish_ex(publisher, &publisher_state, 1u, 1u, FLOWIE_MQTT_VERSION_5, topic,
                             sizeof(topic) - 1u, payload, sizeof(payload) - 1u, 1);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "replay retained publication to a new subscription";
  rc = flowie_dev_connect_client(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_5,
                                 subscriber_id, sizeof(subscriber_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_subscribe(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_5, filter,
                            sizeof(filter) - 1u, 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_flags(&subscriber_state, 1, topic, payload, 1u, 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber, &subscriber_state);
  if (rc != TURBO_OK) goto cleanup;
  flowie_mqtt_client_destroy(subscriber);
  subscriber = NULL;
  failure_stage = "delete retained publication with an empty payload";
  rc = flowie_dev_publish_ex(publisher, &publisher_state, 1u, 1u, FLOWIE_MQTT_VERSION_5, topic,
                             sizeof(topic) - 1u, NULL, 0u, 2);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "verify a new subscription receives no deleted retained value";
  rc = flowie_dev_create_client(server.port, &after_delete_state, filter, sizeof(filter) - 1u,
                                &after_delete);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_connect_client(after_delete, &after_delete_state, FLOWIE_MQTT_VERSION_5,
                                 after_delete_id, sizeof(after_delete_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_subscribe(after_delete, &after_delete_state, FLOWIE_MQTT_VERSION_5, filter,
                            sizeof(filter) - 1u, 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_ping_client(after_delete, &after_delete_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_count_quiet(&after_delete_state, 0);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(after_delete, &after_delete_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(publisher, &publisher_state);

cleanup:
  if (rc != TURBO_OK)
    (void)fprintf(stderr, "flowie dev retained BDD failed at %s: status=%d\n", failure_stage, rc);
  flowie_mqtt_client_destroy(after_delete);
  flowie_mqtt_client_destroy(subscriber);
  flowie_mqtt_client_destroy(publisher);
  return flowie_dev_server_stop(&server, rc);
}

static int flowie_dev_run_will_scenario(void) {
  static const uint8_t subscriber_id[] = "flowie-dev-will-subscriber";
  static const uint8_t abnormal_id[] = "flowie-dev-will-abnormal";
  static const uint8_t normal_id[] = "flowie-dev-will-normal";
  static const uint8_t topic[] = "bdd/will/status";
  static const uint8_t abnormal_payload[] = "unexpected-close";
  static const uint8_t normal_payload[] = "normal-close";
  flowie_dev_server_fixture_t server;
  flowie_dev_client_state_t subscriber_state;
  flowie_dev_client_state_t abnormal_state;
  flowie_dev_client_state_t normal_state;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_t *subscriber = NULL;
  flowie_mqtt_client_t *abnormal = NULL;
  flowie_mqtt_client_t *normal = NULL;
  const char *failure_stage = "initialization";
  int rc;

  flowie_dev_client_state_init(&subscriber_state);
  flowie_dev_client_state_init(&abnormal_state);
  flowie_dev_client_state_init(&normal_state);
  rc = flowie_dev_server_start(&server);
  if (rc != TURBO_OK) goto cleanup;
  failure_stage = "create Will clients";
  rc = flowie_dev_create_client(server.port, &subscriber_state, topic, sizeof(topic) - 1u,
                                &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_create_client(server.port, &abnormal_state, NULL, 0u, &abnormal);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_create_client(server.port, &normal_state, NULL, 0u, &normal);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_connect_client(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_5,
                                 subscriber_id, sizeof(subscriber_id) - 1u);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_subscribe(subscriber, &subscriber_state, FLOWIE_MQTT_VERSION_5, topic,
                            sizeof(topic) - 1u, 1u);
  if (rc != TURBO_OK) goto cleanup;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.has_will = 1u;
  connect.will_qos = 1u;
  connect.keep_alive = 30u;
  connect.will_topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
  connect.will_payload = (flowie_mqtt_span_t){abnormal_payload, sizeof(abnormal_payload) - 1u};
  connect.client_id = (flowie_mqtt_span_t){abnormal_id, sizeof(abnormal_id) - 1u};
  failure_stage = "publish Will after an abnormal transport close";
  rc = flowie_dev_connect_packet(abnormal, &abnormal_state, &connect, 0);
  if (rc != TURBO_OK) goto cleanup;
  flowie_mqtt_client_destroy(abnormal);
  abnormal = NULL;
  rc = flowie_dev_expect_message_flags(&subscriber_state, 1, topic, abnormal_payload, 1u, 0u);
  if (rc != TURBO_OK) goto cleanup;
  connect.will_payload = (flowie_mqtt_span_t){normal_payload, sizeof(normal_payload) - 1u};
  connect.client_id = (flowie_mqtt_span_t){normal_id, sizeof(normal_id) - 1u};
  failure_stage = "suppress Will after a normal MQTT DISCONNECT";
  rc = flowie_dev_connect_packet(normal, &normal_state, &connect, 0);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(normal, &normal_state);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_ping_client(subscriber, &subscriber_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_expect_message_count_quiet(&subscriber_state, 1);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_dev_disconnect_client(subscriber, &subscriber_state);

cleanup:
  if (rc != TURBO_OK)
    (void)fprintf(stderr, "flowie dev Will BDD failed at %s: status=%d\n", failure_stage, rc);
  flowie_mqtt_client_destroy(normal);
  flowie_mqtt_client_destroy(abnormal);
  flowie_mqtt_client_destroy(subscriber);
  return flowie_dev_server_stop(&server, rc);
}

spec("Flowie dev server") {
  given("the real dev product configuration and graph") {
    when("MQTT 3.1.1 publishes to two subscribers") {
      then("both receive fan-out and one unsubscribe does not affect the other") {
        check_int_eq(flowie_dev_run_scenario(FLOWIE_MQTT_VERSION_3_1_1), TURBO_OK);
      }
    }
    when("MQTT 5 publishes to two subscribers") {
      then("both receive fan-out and one unsubscribe does not affect the other") {
        check_int_eq(flowie_dev_run_scenario(FLOWIE_MQTT_VERSION_5), TURBO_OK);
      }
    }
    when("a QoS 0 publisher closes immediately after its socket accepts the packet") {
      then("the owned graph message still fans out without the publisher connection") {
        check_int_eq(flowie_dev_run_immediate_publisher_close_scenario(), TURBO_OK);
      }
    }
    when("an MQTT 5 QoS 2 publication fans out to two subscribers") {
      then("all handshakes complete and each subscriber receives exactly one message") {
        check_int_eq(flowie_dev_run_qos2_scenario(), TURBO_OK);
      }
    }
    when("MQTT-OWNER-007 resumes and then explicitly replaces an MQTT 5 persistent session") {
      then("Session Present, offline replay, and Clean Start state removal remain exact") {
        check_int_eq(flowie_dev_run_session_resume_scenario(), TURBO_OK);
      }
    }
    when("an MQTT 5 retained publication is stored and then deleted") {
      then("new subscriptions replay only the retained value that still exists") {
        check_int_eq(flowie_dev_run_retained_scenario(), TURBO_OK);
      }
    }
    when("MQTT 5 clients close abnormally and normally with a Will") {
      then("only the abnormal close publishes its Will") {
        check_int_eq(flowie_dev_run_will_scenario(), TURBO_OK);
      }
    }
  }
}
