#include "flowie_test_socket.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_process.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef FLOWIE_MOSQUITTO_PUB
  #error "FLOWIE_MOSQUITTO_PUB must point to mosquitto_pub"
#endif
#ifndef FLOWIE_MOSQUITTO_SUB
  #error "FLOWIE_MOSQUITTO_SUB must point to mosquitto_sub"
#endif

#define FLOWIE_INTEROP_TIMEOUT_MS 10000u
#define FLOWIE_INTEROP_READY_MS 1000u
#define FLOWIE_INTEROP_EXPIRY_WAIT_MS 3000u
#define FLOWIE_INTEROP_OUTPUT_CAPACITY 4096u
#define FLOWIE_INTEROP_PORT_TEXT_CAPACITY 8u
#define FLOWIE_INTEROP_NAME_CAPACITY 96u

#if !defined(FLOWIE_INTEROP_EXTERNAL_BROKER)
  #ifndef FLOWIE_DEV_SERVER_EXECUTABLE
    #error "FLOWIE_DEV_SERVER_EXECUTABLE must point to flowie_server"
  #endif
  #ifndef FLOWIE_DEV_CONFIG_PATH
    #error "FLOWIE_DEV_CONFIG_PATH must point to flowie-dev.yml"
  #endif
  #ifndef FLOWIE_DEV_GRAPH_PATH
    #error "FLOWIE_DEV_GRAPH_PATH must point to flowie-dev.flow"
  #endif
#else
  #ifndef FLOWIE_INTEROP_BROKER_HOST
    #error "FLOWIE_INTEROP_BROKER_HOST is required"
  #endif
  #ifndef FLOWIE_INTEROP_BROKER_PORT
    #error "FLOWIE_INTEROP_BROKER_PORT is required"
  #endif
#endif

typedef struct flowie_interop_broker_s {
  turbo_process_t *process;
  char *config_path;
  const char *host;
  unsigned short port;
} flowie_interop_broker_t;

static void flowie_interop_report_stderr(turbo_process_t *process, const char *phase,
                                         const turbo_process_result_t *result) {
  char diagnostic[FLOWIE_INTEROP_OUTPUT_CAPACITY];
  size_t total = 0u;
  int rc;

  if (!process || !phase) return;
  while (total + 1u < sizeof(diagnostic)) {
    size_t count = 0u;
    rc = turbo_process_read_stderr(process, diagnostic + total,
                                   sizeof(diagnostic) - total - 1u, &count);
    total += count;
    if (rc == TURBO_EOF || count == 0u) break;
    if (rc != TURBO_OK) break;
  }
  diagnostic[total] = '\0';
  fprintf(stderr, "interop phase=%s pid=%d state=%s exit=%d signal=%d error=%d stderr=%s\n",
          phase, turbo_process_pid(process),
          result ? turbo_process_state_name(result->state) : "wait-failed",
          result ? result->exit_code : 0, result ? result->term_signal : 0,
          result ? result->error_code : 0, total ? diagnostic : "(empty)");
  total = 0u;
  while (total + 1u < sizeof(diagnostic)) {
    size_t count = 0u;
    rc = turbo_process_read_stdout(process, diagnostic + total,
                                   sizeof(diagnostic) - total - 1u, &count);
    total += count;
    if (rc == TURBO_EOF || count == 0u) break;
    if (rc != TURBO_OK) break;
  }
  diagnostic[total] = '\0';
  if (total != 0u) fprintf(stderr, "interop phase=%s stdout=%s\n", phase, diagnostic);
}

static int flowie_interop_wait_process(turbo_process_t *process, int require_success,
                                       char *output, size_t capacity, size_t *output_size) {
  turbo_process_result_t result;
  size_t total = 0u;
  int rc;
  if (output_size) *output_size = 0u;
  if (!process || (capacity != 0u && !output)) return TURBO_EINVAL;
  rc = turbo_process_wait_for(process, FLOWIE_INTEROP_TIMEOUT_MS, &result);
  if (rc != TURBO_OK) {
    flowie_interop_report_stderr(process, "wait", NULL);
    return rc;
  }
  if (require_success &&
      (result.state != TURBO_PROCESS_EXITED || result.exit_code != 0)) {
    flowie_interop_report_stderr(process, "exit", &result);
    return TURBO_EIO;
  }
  while (total < capacity) {
    size_t count = 0u;
    rc = turbo_process_read_stdout(process, output + total, capacity - total, &count);
    total += count;
    if (rc == TURBO_EOF) break;
    if (rc != TURBO_OK) return rc;
    if (count == 0u) break;
  }
  if (output_size) *output_size = total;
  return TURBO_OK;
}

static int flowie_interop_spawn(const char *program, const char *const *args, unsigned int flags,
                                turbo_process_t **out) {
  turbo_process_options_t options;
  turbo_process_options_init(&options);
  options.program = program;
  options.args = args;
  options.flags = flags | TURBO_PROCESS_CAPTURE_STDOUT | TURBO_PROCESS_CAPTURE_STDERR;
  options.timeout_ms = FLOWIE_INTEROP_TIMEOUT_MS;
  options.max_output_bytes = FLOWIE_INTEROP_OUTPUT_CAPACITY;
  return turbo_process_spawn(&options, out);
}

static int flowie_interop_run(const char *program, const char *const *args) {
  turbo_process_t *process = NULL;
  int rc = flowie_interop_spawn(program, args, 0u, &process);
  if (rc == TURBO_OK)
    rc = flowie_interop_wait_process(process, 1, NULL, 0u, NULL);
  turbo_process_destroy(process);
  return rc;
}

#if !defined(FLOWIE_INTEROP_EXTERNAL_BROKER)
static int flowie_interop_write_config(unsigned short port, char **path_out) {
  static const char marker[] = "      port: 1883";
  char replacement[32];
  char *source = NULL;
  char *output = NULL;
  char *match;
  char *path = NULL;
  size_t source_size = 0u;
  size_t prefix_size;
  size_t replacement_size;
  size_t suffix_size;
  int written;
  int rc = TURBO_EIO;
  if (path_out) *path_out = NULL;
  if (!path_out || port == 0u) return TURBO_EINVAL;
  source = tt_read_file(FLOWIE_DEV_CONFIG_PATH, &source_size);
  if (!source) goto cleanup;
  match = strstr(source, marker);
  if (!match || strstr(match + sizeof(marker) - 1u, marker)) goto cleanup;
  written = snprintf(replacement, sizeof(replacement), "      port: %u", (unsigned int)port);
  if (written <= 0 || (size_t)written >= sizeof(replacement)) goto cleanup;
  prefix_size = (size_t)(match - source);
  replacement_size = (size_t)written;
  suffix_size = source_size - prefix_size - (sizeof(marker) - 1u);
  if (prefix_size > SIZE_MAX - replacement_size ||
      prefix_size + replacement_size > SIZE_MAX - suffix_size)
    goto cleanup;
  output = (char *)malloc(prefix_size + replacement_size + suffix_size + 1u);
  if (!output) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  memcpy(output, source, prefix_size);
  memcpy(output + prefix_size, replacement, replacement_size);
  memcpy(output + prefix_size + replacement_size, match + sizeof(marker) - 1u, suffix_size);
  output[prefix_size + replacement_size + suffix_size] = '\0';
  path = tt_make_temp_file("flowie-mosquitto-interop", ".yml");
  if (!path || tt_write_file(path, output, prefix_size + replacement_size + suffix_size) != 0)
    goto cleanup;
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

static int flowie_interop_wait_listener(turbo_process_t *process, unsigned short port) {
  const uint64_t deadline = turbo_monotonic_ms() + FLOWIE_INTEROP_TIMEOUT_MS;
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
#endif

static int flowie_interop_broker_start(flowie_interop_broker_t *broker) {
  if (!broker) return TURBO_EINVAL;
  memset(broker, 0, sizeof(*broker));
#if defined(FLOWIE_INTEROP_EXTERNAL_BROKER)
  broker->host = FLOWIE_INTEROP_BROKER_HOST;
  broker->port = (unsigned short)FLOWIE_INTEROP_BROKER_PORT;
  return broker->port == 0u ? TURBO_EINVAL : TURBO_OK;
#else
  const char *args[] = {NULL, FLOWIE_DEV_GRAPH_PATH, NULL};
  int rc;
  broker->host = "127.0.0.1";
  broker->port = flowie_test_port();
  if (broker->port == 0u) return TURBO_EIO;
  rc = flowie_interop_write_config(broker->port, &broker->config_path);
  if (rc != TURBO_OK) return rc;
  args[0] = broker->config_path;
  rc = flowie_interop_spawn(FLOWIE_DEV_SERVER_EXECUTABLE, args, 0u, &broker->process);
  if (rc != TURBO_OK) return rc;
  return flowie_interop_wait_listener(broker->process, broker->port);
#endif
}

static int flowie_interop_broker_stop(flowie_interop_broker_t *broker, int status) {
  if (!broker) return status == TURBO_OK ? TURBO_EINVAL : status;
  if (broker->process) {
    turbo_process_result_t result;
    int rc = turbo_process_poll(broker->process, &result);
    if (rc == TURBO_EBUSY) {
      rc = turbo_process_terminate(broker->process);
      if (rc == TURBO_OK) rc = turbo_process_wait(broker->process, &result);
    }
    if (status != TURBO_OK) {
      flowie_interop_report_stderr(broker->process, "flowie-server",
                                   rc == TURBO_OK ? &result : NULL);
    }
    if (status == TURBO_OK && rc != TURBO_OK) status = rc;
    turbo_process_destroy(broker->process);
  }
  if (broker->config_path) {
    (void)tt_remove_file(broker->config_path);
    free(broker->config_path);
  }
  memset(broker, 0, sizeof(*broker));
  return status;
}

static int flowie_interop_format_endpoint(const flowie_interop_broker_t *broker, char *port,
                                          size_t port_capacity) {
  int written;
  if (!broker || !broker->host || !port || port_capacity == 0u) return TURBO_EINVAL;
  written = snprintf(port, port_capacity, "%u", (unsigned int)broker->port);
  return written > 0 && (size_t)written < port_capacity ? TURBO_OK : TURBO_EMSGSIZE;
}

static int flowie_interop_expect_output(turbo_process_t *subscriber, const char *expected) {
  char output[FLOWIE_INTEROP_OUTPUT_CAPACITY];
  size_t output_size = 0u;
  size_t expected_size = strlen(expected);
  int rc = flowie_interop_wait_process(subscriber, 1, output, sizeof(output), &output_size);
  if (rc != TURBO_OK) return rc;
  while (output_size != 0u && (output[output_size - 1u] == '\n' || output[output_size - 1u] == '\r'))
    --output_size;
  return output_size == expected_size && memcmp(output, expected, expected_size) == 0
             ? TURBO_OK
             : TURBO_EPROTO;
}

static int flowie_interop_core_trace(const char *version, int qos) {
  flowie_interop_broker_t broker;
  turbo_process_t *subscriber = NULL;
  char port[FLOWIE_INTEROP_PORT_TEXT_CAPACITY];
  char qos_text[4];
  char topic[FLOWIE_INTEROP_NAME_CAPACITY];
  char payload[FLOWIE_INTEROP_NAME_CAPACITY];
  char subscriber_id[FLOWIE_INTEROP_NAME_CAPACITY];
  char publisher_id[FLOWIE_INTEROP_NAME_CAPACITY];
  const char *sub_args[] = {"-h", NULL, "-p", port, "-V", version, "-i", subscriber_id,
                            "-t", topic, "-q", qos_text, "-C", "1", "-W", "5", "-F",
                            "%p", NULL};
  const char *pub_args[] = {"-h", NULL, "-p", port, "-V", version, "-i", publisher_id,
                            "-t", topic, "-q", qos_text, "-m", payload, NULL};
  int rc = flowie_interop_broker_start(&broker);
  if (rc != TURBO_OK) goto cleanup;
  sub_args[1] = broker.host;
  pub_args[1] = broker.host;
  if (flowie_interop_format_endpoint(&broker, port, sizeof(port)) != TURBO_OK ||
      snprintf(qos_text, sizeof(qos_text), "%d", qos) <= 0 ||
      snprintf(topic, sizeof(topic), "interop/core/%llu/%d", (unsigned long long)turbo_hrtime(),
               qos) <= 0 ||
      snprintf(payload, sizeof(payload), "payload-qos-%d", qos) <= 0 ||
      snprintf(subscriber_id, sizeof(subscriber_id), "interop-sub-%llu-%d",
               (unsigned long long)turbo_hrtime(), qos) <= 0 ||
      snprintf(publisher_id, sizeof(publisher_id), "interop-pub-%llu-%d",
               (unsigned long long)turbo_hrtime(), qos) <= 0) {
    rc = TURBO_EMSGSIZE;
    goto cleanup;
  }
  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, sub_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, pub_args);
  if (rc == TURBO_OK) rc = flowie_interop_expect_output(subscriber, payload);
cleanup:
  if (rc != TURBO_OK)
    fprintf(stderr, "interop core version=%s qos=%d status=%d\n", version, qos, rc);
  turbo_process_destroy(subscriber);
  return flowie_interop_broker_stop(&broker, rc);
}

static int flowie_interop_unsubscribe_trace(const char *version) {
  flowie_interop_broker_t broker;
  turbo_process_t *subscriber = NULL;
  char port[FLOWIE_INTEROP_PORT_TEXT_CAPACITY];
  char topic[FLOWIE_INTEROP_NAME_CAPACITY];
  char probe_topic[FLOWIE_INTEROP_NAME_CAPACITY];
  char client_id[FLOWIE_INTEROP_NAME_CAPACITY];
  char publisher_id[FLOWIE_INTEROP_NAME_CAPACITY];
  char expected_probe[FLOWIE_INTEROP_NAME_CAPACITY * 2u];
  static const char initial_payload[] = "initial";
  static const char stale_payload[] = "must-not-replay";
  static const char probe_payload[] = "probe";
  const char *initial_sub_args[] = {"-h", NULL, "-p", port, "-V", version, "-c", "-i",
                                    client_id, "-t", topic, "-q", "1", "-C", "1", "-W",
                                    "5", "-F", "%p", NULL};
  const char *unsubscribe_args[] = {"-h", NULL, "-p", port, "-V", version, "-c", "-i",
                                    client_id, "-U", topic, "-t", probe_topic, "-q", "1",
                                    "-C", "1", "-W", "5", "-F", "%p", NULL};
  const char *resume_args[] = {"-h", NULL, "-p", port, "-V", version, "-c", "-i", client_id,
                               "-t", probe_topic, "-q", "1", "-C", "1", "-W", "5", "-F",
                               "%t:%p", NULL};
  const char *initial_pub_args[] = {"-h", NULL, "-p", port, "-V", version, "-i", publisher_id,
                                    "-t", topic, "-q", "1", "-m", initial_payload, NULL};
  const char *unsubscribe_probe_args[] = {
      "-h", NULL, "-p", port, "-V", version, "-i", publisher_id,
      "-t", probe_topic, "-q", "1", "-m", probe_payload, NULL};
  const char *stale_pub_args[] = {"-h", NULL, "-p", port, "-V", version, "-i", publisher_id,
                                  "-t", topic, "-q", "1", "-m", stale_payload, NULL};
  const char *cleanup_args[] = {"-h", NULL, "-p", port, "-V", version, "-i", client_id,
                                "-t", probe_topic, "-q", "0", "-n", NULL};
  int rc = flowie_interop_broker_start(&broker);
  if (rc != TURBO_OK) goto cleanup;
  initial_sub_args[1] = unsubscribe_args[1] = resume_args[1] = broker.host;
  initial_pub_args[1] = unsubscribe_probe_args[1] = stale_pub_args[1] = cleanup_args[1] =
      broker.host;
  if (flowie_interop_format_endpoint(&broker, port, sizeof(port)) != TURBO_OK ||
      snprintf(topic, sizeof(topic), "interop/unsubscribe/%llu",
               (unsigned long long)turbo_hrtime()) <= 0 ||
      snprintf(probe_topic, sizeof(probe_topic), "interop/unsubscribe-probe/%llu",
               (unsigned long long)turbo_hrtime()) <= 0 ||
      snprintf(client_id, sizeof(client_id), "i%016llx",
               (unsigned long long)turbo_hrtime()) <= 0 ||
      snprintf(publisher_id, sizeof(publisher_id), "p%016llx",
               (unsigned long long)turbo_hrtime()) <= 0 ||
      snprintf(expected_probe, sizeof(expected_probe), "%s:%s", probe_topic, probe_payload) <= 0) {
    rc = TURBO_EMSGSIZE;
    goto cleanup;
  }

  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, initial_sub_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, initial_pub_args);
  if (rc == TURBO_OK) rc = flowie_interop_expect_output(subscriber, initial_payload);
  turbo_process_destroy(subscriber);
  subscriber = NULL;
  if (rc != TURBO_OK) goto cleanup;

  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, unsubscribe_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, unsubscribe_probe_args);
  if (rc == TURBO_OK) rc = flowie_interop_expect_output(subscriber, probe_payload);
  turbo_process_destroy(subscriber);
  subscriber = NULL;
  if (rc != TURBO_OK) goto cleanup;

  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, stale_pub_args);
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, resume_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, unsubscribe_probe_args);
  if (rc == TURBO_OK) rc = flowie_interop_expect_output(subscriber, expected_probe);
  if (rc == TURBO_OK) rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, cleanup_args);
cleanup:
  turbo_process_destroy(subscriber);
  return flowie_interop_broker_stop(&broker, rc);
}

static int flowie_interop_retained_trace(void) {
  flowie_interop_broker_t broker;
  turbo_process_t *subscriber = NULL;
  char port[FLOWIE_INTEROP_PORT_TEXT_CAPACITY];
  char topic[FLOWIE_INTEROP_NAME_CAPACITY];
  static const char payload[] = "retained-value";
  const char *pub_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                            "-q", "1", "-r", "-m", payload, NULL};
  const char *sub_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                            "-q", "1", "-C", "1", "-W", "5", "-F", "%p", NULL};
  const char *clear_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                              "-q", "1", "-r", "-n", NULL};
  int rc = flowie_interop_broker_start(&broker);
  if (rc != TURBO_OK) goto cleanup;
  pub_args[1] = sub_args[1] = clear_args[1] = broker.host;
  if (flowie_interop_format_endpoint(&broker, port, sizeof(port)) != TURBO_OK ||
      snprintf(topic, sizeof(topic), "interop/retained/%llu",
               (unsigned long long)turbo_hrtime()) <= 0) {
    rc = TURBO_EMSGSIZE;
    goto cleanup;
  }
  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, pub_args);
  if (rc == TURBO_OK) rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, sub_args, 0u, &subscriber);
  if (rc == TURBO_OK) rc = flowie_interop_expect_output(subscriber, payload);
  if (flowie_interop_run(FLOWIE_MOSQUITTO_PUB, clear_args) != TURBO_OK && rc == TURBO_OK)
    rc = TURBO_EIO;
cleanup:
  turbo_process_destroy(subscriber);
  return flowie_interop_broker_stop(&broker, rc);
}

static int flowie_interop_offline_trace(int expire_message) {
  flowie_interop_broker_t broker;
  turbo_process_t *subscriber = NULL;
  char output[FLOWIE_INTEROP_OUTPUT_CAPACITY];
  char port[FLOWIE_INTEROP_PORT_TEXT_CAPACITY];
  char topic[FLOWIE_INTEROP_NAME_CAPACITY];
  char client_id[FLOWIE_INTEROP_NAME_CAPACITY];
  static const char payload[] = "offline-value";
  const char *sub_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                            "-q", "1", "-c", "-i", client_id, NULL};
  const char *resume_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                               "-q", "1", "-c", "-i", client_id, "-C", "1", "-W",
                               "2", "-F", "%p", NULL};
  const char *normal_pub_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                                   "-q", "1", "-m", payload, NULL};
  const char *expiry_pub_args[] = {
      "-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic, "-q", "1", "-D",
      "publish", "message-expiry-interval", "1", "-m", payload, NULL};
  const char *const *pub_args = expire_message ? expiry_pub_args : normal_pub_args;
  int rc = flowie_interop_broker_start(&broker);
  if (rc != TURBO_OK) goto cleanup;
  sub_args[1] = resume_args[1] = normal_pub_args[1] = expiry_pub_args[1] = broker.host;
  if (flowie_interop_format_endpoint(&broker, port, sizeof(port)) != TURBO_OK ||
      snprintf(topic, sizeof(topic), "interop/offline/%llu",
               (unsigned long long)turbo_hrtime()) <= 0 ||
      snprintf(client_id, sizeof(client_id), "interop-session-%llu",
               (unsigned long long)turbo_hrtime()) <= 0) {
    rc = TURBO_EMSGSIZE;
    goto cleanup;
  }
  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, sub_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = turbo_process_terminate(subscriber);
  if (rc == TURBO_OK) {
    turbo_process_result_t result;
    rc = turbo_process_wait(subscriber, &result);
  }
  turbo_process_destroy(subscriber);
  subscriber = NULL;
  if (rc != TURBO_OK) goto cleanup;
  rc = flowie_interop_run(FLOWIE_MOSQUITTO_PUB, pub_args);
  if (rc != TURBO_OK) goto cleanup;
  if (expire_message) turbo_sleep_ms(FLOWIE_INTEROP_EXPIRY_WAIT_MS);
  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, resume_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  if (!expire_message) {
    rc = flowie_interop_expect_output(subscriber, payload);
  } else {
    size_t output_size = 0u;
    rc = flowie_interop_wait_process(subscriber, 0, output, sizeof(output), &output_size);
    if (rc == TURBO_OK && output_size != 0u) rc = TURBO_EPROTO;
  }
cleanup:
  turbo_process_destroy(subscriber);
  return flowie_interop_broker_stop(&broker, rc);
}

static int flowie_interop_will_trace(void) {
  flowie_interop_broker_t broker;
  turbo_process_t *subscriber = NULL;
  turbo_process_t *will_client = NULL;
  char port[FLOWIE_INTEROP_PORT_TEXT_CAPACITY];
  char topic[FLOWIE_INTEROP_NAME_CAPACITY];
  static const char payload[] = "unexpected-close";
  const char *sub_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-t", topic,
                            "-q", "1", "-C", "1", "-W", "5", "-F", "%p", NULL};
  const char *will_args[] = {"-h", NULL, "-p", port, "-V", "mqttv5", "-l", "-t",
                             "interop/unused", "--will-topic", topic, "--will-payload", payload,
                             "--will-qos", "1", NULL};
  int rc = flowie_interop_broker_start(&broker);
  if (rc != TURBO_OK) goto cleanup;
  sub_args[1] = will_args[1] = broker.host;
  if (flowie_interop_format_endpoint(&broker, port, sizeof(port)) != TURBO_OK ||
      snprintf(topic, sizeof(topic), "interop/will/%llu", (unsigned long long)turbo_hrtime()) <= 0) {
    rc = TURBO_EMSGSIZE;
    goto cleanup;
  }
  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_SUB, sub_args, 0u, &subscriber);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = flowie_interop_spawn(FLOWIE_MOSQUITTO_PUB, will_args, TURBO_PROCESS_PIPE_STDIN,
                            &will_client);
  if (rc != TURBO_OK) goto cleanup;
  turbo_sleep_ms(FLOWIE_INTEROP_READY_MS);
  rc = turbo_process_terminate(will_client);
  if (rc == TURBO_OK) {
    turbo_process_result_t result;
    rc = turbo_process_wait(will_client, &result);
  }
  if (rc == TURBO_OK) rc = flowie_interop_expect_output(subscriber, payload);
cleanup:
  turbo_process_destroy(will_client);
  turbo_process_destroy(subscriber);
  return flowie_interop_broker_stop(&broker, rc);
}

spec("Flowie fixed MQTT interoperability") {
  it("MQTT-INTEROP-002 runs Mosquitto MQTT 3.1.1 core QoS trace") {
    for (int qos = 0; qos <= 2; ++qos)
      check_int_eq(flowie_interop_core_trace("mqttv311", qos), TURBO_OK);
    check_int_eq(flowie_interop_unsubscribe_trace("mqttv311"), TURBO_OK);
  }

  it("MQTT-INTEROP-002 runs Mosquitto MQTT 5 core QoS trace") {
    for (int qos = 0; qos <= 2; ++qos)
      check_int_eq(flowie_interop_core_trace("mqttv5", qos), TURBO_OK);
    check_int_eq(flowie_interop_unsubscribe_trace("mqttv5"), TURBO_OK);
  }

  it("MQTT-INTEROP-003 round trips retained and offline persistent delivery") {
    check_int_eq(flowie_interop_retained_trace(), TURBO_OK);
    check_int_eq(flowie_interop_offline_trace(0), TURBO_OK);
  }

  it("MQTT-INTEROP-003 publishes Will once and suppresses expired offline delivery") {
    check_int_eq(flowie_interop_will_trace(), TURBO_OK);
    check_int_eq(flowie_interop_offline_trace(1), TURBO_OK);
  }
}
