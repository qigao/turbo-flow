#include "fmq_bench_stats.h"
#include "flow_fmq_profile_internal.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_fmq.h"
#include "turbo_thread.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
#endif

#define FMQ_BENCH_TYPICAL_PAYLOAD_BYTES 64u
#define FMQ_BENCH_TYPICAL_WARMUP 32u
#define FMQ_BENCH_TYPICAL_SAMPLES 1000u
#define FMQ_BENCH_LARGE_PAYLOAD_BYTES (64u * 1024u)
#define FMQ_BENCH_LARGE_WARMUP 8u
#define FMQ_BENCH_LARGE_SAMPLES 128u
#define FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES 64u
#define FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE 64u
#define FMQ_BENCH_THROUGHPUT_SAMPLES 200u
#define FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES 32u
#define FMQ_BENCH_LARGE_BATCH_PAYLOAD_BYTES (64u * 1024u)
#define FMQ_BENCH_LARGE_BATCH_MESSAGES_PER_SAMPLE 8u
#define FMQ_BENCH_LARGE_BATCH_SAMPLES 64u
#define FMQ_BENCH_LARGE_BATCH_WARMUP_MESSAGES 8u
#define FMQ_BENCH_WAIT_TIMEOUT_NS UINT64_C(2000000000)
#define FMQ_BENCH_CONNECT_ATTEMPTS 400u
#define FMQ_BENCH_CONNECT_RETRY_MS 5u
#define FMQ_BENCH_REQ_READY_ATTEMPTS 400u

typedef struct fmq_bench_case_s {
  const char *title;
  size_t payload_bytes;
  size_t warmup;
  size_t samples;
} fmq_bench_case_t;

typedef struct fmq_bench_throughput_case_s {
  const char *title;
  const char *result_pattern;
  turbo_flow_fmq_pattern_t sender_pattern;
  turbo_flow_fmq_endpoint_mode_t sender_mode;
  turbo_flow_fmq_pattern_t receiver_pattern;
  turbo_flow_fmq_endpoint_mode_t receiver_mode;
  const char *sender_identity;
  const char *sender_topic;
  const char *receiver_topic;
  size_t payload_bytes;
  size_t messages_per_sample;
  size_t warmup_messages;
  size_t samples;
  int use_batch_api;
  int use_async_api;
} fmq_bench_throughput_case_t;

typedef struct fmq_bench_receive_state_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  uint64_t received_ns;
  size_t received;
  size_t expected_payload_bytes;
  size_t last_payload_bytes;
  int status;
} fmq_bench_receive_state_t;

typedef struct fmq_bench_async_completion_state_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  uint64_t completed_ns;
  size_t completed;
  int status;
} fmq_bench_async_completion_state_t;

static int fmq_bench_profile_requested(void) {
  const char *value = getenv("FLOWMQ_BENCH_PROFILE");
  return value && value[0] != '\0' && strcmp(value, "0") != 0;
}

static int fmq_bench_profile_begin(void) {
  if (!fmq_bench_profile_requested()) return 0;
  flow_fmq_send_profile_set_enabled(0);
  flow_fmq_send_profile_reset();
  flow_fmq_send_profile_set_enabled(1);
  return 1;
}

static uint64_t fmq_bench_profile_average(uint64_t sum, uint64_t count) {
  return count == 0u ? 0u : sum / count;
}

static uint64_t fmq_bench_profile_remainder(uint64_t total, uint64_t first_part,
                                            uint64_t second_part) {
  if (total <= first_part) return 0u;
  total -= first_part;
  return total > second_part ? total - second_part : 0u;
}

static void fmq_bench_profile_print(const char *pattern,
                                    const flow_fmq_send_profile_snapshot_t *profile) {
  if (!pattern || !profile) return;
  printf("FMQ_PROFILE_RESULT component=send_owner_lane pattern=%s samples=%" PRIu64
         " post_samples=%" PRIu64 " socket_samples=%" PRIu64
         " socket_cpu_samples=%" PRIu64
         " avg_enqueue_ns=%" PRIu64 " avg_owner_wait_ns=%" PRIu64
         " avg_post_call_ns=%" PRIu64 " avg_owner_dispatch_ns=%" PRIu64
         " avg_socket_send_ns=%" PRIu64 " avg_socket_thread_cpu_ns=%" PRIu64
         " avg_socket_estimated_off_cpu_ns=%" PRIu64 " avg_completion_ns=%" PRIu64
         " avg_waiter_wake_ns=%" PRIu64 " avg_total_ns=%" PRIu64 "\n",
         pattern, profile->samples, profile->post_samples, profile->socket_samples,
         profile->socket_cpu_samples,
         fmq_bench_profile_average(profile->enqueue_sum_ns, profile->samples),
         fmq_bench_profile_average(profile->owner_wait_sum_ns, profile->samples),
         fmq_bench_profile_average(profile->post_call_sum_ns, profile->post_samples),
         fmq_bench_profile_average(profile->owner_dispatch_sum_ns, profile->samples),
         fmq_bench_profile_average(profile->socket_send_sum_ns, profile->socket_samples),
         fmq_bench_profile_average(profile->socket_thread_cpu_sum_ns,
                                   profile->socket_cpu_samples),
         fmq_bench_profile_average(profile->socket_estimated_off_cpu_sum_ns,
                                   profile->socket_cpu_samples),
         fmq_bench_profile_average(profile->completion_sum_ns, profile->samples),
         fmq_bench_profile_average(profile->waiter_wake_sum_ns, profile->samples),
         fmq_bench_profile_average(profile->total_sum_ns, profile->samples));
  if (profile->batch_socket_calls == 0u) return;
  printf("FMQ_PROFILE_BATCH_RESULT component=send_owner_lane pattern=%s"
         " samples=%" PRIu64 " post_samples=%" PRIu64
         " socket_calls=%" PRIu64 " socket_cpu_samples=%" PRIu64
         " frames=%" PRIu64 " iov_segments=%" PRIu64
         " avg_build_ns=%" PRIu64 " avg_payload_prepare_ns=%" PRIu64
         " avg_graph_publish_ns=%" PRIu64 " avg_adapter_consume_ns=%" PRIu64
         " avg_graph_runtime_ns=%" PRIu64 " avg_enqueue_prepare_ns=%" PRIu64
         " avg_owner_wait_ns=%" PRIu64
         " avg_post_call_ns=%" PRIu64 " avg_owner_work_ns=%" PRIu64
         " avg_frames_per_call=%" PRIu64 " avg_iov_segments_per_call=%" PRIu64
         " avg_socket_send_ns=%" PRIu64 " avg_socket_thread_cpu_ns=%" PRIu64
         " avg_socket_estimated_off_cpu_ns=%" PRIu64
         " avg_waiter_wake_ns=%" PRIu64 " avg_total_ns=%" PRIu64 "\n",
         pattern, profile->batch_samples, profile->batch_post_samples,
         profile->batch_socket_calls, profile->batch_socket_cpu_samples,
         profile->batch_socket_frames, profile->batch_socket_iov_segments,
         fmq_bench_profile_average(profile->batch_build_sum_ns, profile->batch_samples),
         fmq_bench_profile_average(profile->batch_payload_prepare_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(profile->batch_graph_publish_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(profile->batch_adapter_consume_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(
             fmq_bench_profile_remainder(profile->batch_graph_publish_sum_ns,
                                         profile->batch_payload_prepare_sum_ns,
                                         profile->batch_adapter_consume_sum_ns),
             profile->batch_samples),
         fmq_bench_profile_average(profile->batch_enqueue_prepare_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(profile->batch_owner_wait_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(profile->batch_post_call_sum_ns,
                                   profile->batch_post_samples),
         fmq_bench_profile_average(profile->batch_owner_work_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(profile->batch_socket_frames,
                                   profile->batch_socket_calls),
         fmq_bench_profile_average(profile->batch_socket_iov_segments,
                                   profile->batch_socket_calls),
         fmq_bench_profile_average(profile->batch_socket_send_sum_ns,
                                   profile->batch_socket_calls),
         fmq_bench_profile_average(profile->batch_socket_thread_cpu_sum_ns,
                                   profile->batch_socket_cpu_samples),
         fmq_bench_profile_average(profile->batch_socket_estimated_off_cpu_sum_ns,
                                   profile->batch_socket_cpu_samples),
         fmq_bench_profile_average(profile->batch_waiter_wake_sum_ns,
                                   profile->batch_samples),
         fmq_bench_profile_average(profile->batch_total_sum_ns, profile->batch_samples));
}

static unsigned short fmq_bench_loopback_port(void) {
  struct sockaddr_in address;
  unsigned short port = 0u;
#ifdef _WIN32
  SOCKET handle = INVALID_SOCKET;
  int address_size = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0u;
#else
  int handle = -1;
  socklen_t address_size = (socklen_t)sizeof(address);
#endif

  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0u);
  handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (handle == INVALID_SOCKET) goto done;
#else
  if (handle < 0) goto done;
#endif
  if (bind(handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(handle, (struct sockaddr *)&address, &address_size) == 0)
    port = ntohs(address.sin_port);

done:
#ifdef _WIN32
  if (handle != INVALID_SOCKET) closesocket(handle);
  WSACleanup();
#else
  if (handle >= 0) close(handle);
#endif
  return port;
}

static void fmq_bench_receive_state_init(fmq_bench_receive_state_t *state,
                                         size_t expected_payload_bytes) {
  memset(state, 0, sizeof(*state));
  turbo_mutex_init(&state->mutex);
  turbo_cond_init(&state->changed);
  state->expected_payload_bytes = expected_payload_bytes;
  state->status = TURBO_OK;
}

static void fmq_bench_receive_state_destroy(fmq_bench_receive_state_t *state) {
  turbo_cond_destroy(&state->changed);
  turbo_mutex_destroy(&state->mutex);
}

static int fmq_bench_echo_request(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  (void)app;
  (void)ctx;
  if (!message) return TURBO_EINVAL;
  return turbo_flow_fmq_app_message_set_payload_copy(message, message->payload.data,
                                                     message->payload.len);
}

static int fmq_bench_capture_message(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message,
                                     void *ctx) {
  fmq_bench_receive_state_t *state = (fmq_bench_receive_state_t *)ctx;
  const uint64_t received_ns = turbo_hrtime();
  int status = TURBO_OK;
  (void)app;
  if (!message || !state) return TURBO_EINVAL;
  if (message->payload.len != state->expected_payload_bytes) status = TURBO_EPROTO;

  turbo_mutex_lock(&state->mutex);
  if (state->status == TURBO_OK) state->status = status;
  state->received_ns = received_ns;
  state->last_payload_bytes = message->payload.len;
  state->received += 1u;
  turbo_cond_signal(&state->changed);
  turbo_mutex_unlock(&state->mutex);
  return status;
}

static int fmq_bench_wait_received(fmq_bench_receive_state_t *state, size_t expected_received,
                                   uint64_t *received_ns) {
  const uint64_t started_ns = turbo_hrtime();
  const uint64_t deadline_ns = started_ns > UINT64_MAX - FMQ_BENCH_WAIT_TIMEOUT_NS
                                   ? UINT64_MAX
                                   : started_ns + FMQ_BENCH_WAIT_TIMEOUT_NS;
  int rc = TURBO_OK;
  if (!state || expected_received == 0u) return TURBO_EINVAL;

  turbo_mutex_lock(&state->mutex);
  while (state->received < expected_received && state->status == TURBO_OK) {
    const uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns ||
        turbo_cond_timedwait(&state->changed, &state->mutex, deadline_ns - now_ns) != TURBO_OK) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
  }
  if (rc == TURBO_OK) rc = state->status;
  if (rc == TURBO_OK && received_ns) *received_ns = state->received_ns;
  turbo_mutex_unlock(&state->mutex);
  return rc;
}

static void fmq_bench_async_completion_state_init(
    fmq_bench_async_completion_state_t *state) {
  memset(state, 0, sizeof(*state));
  turbo_mutex_init(&state->mutex);
  turbo_cond_init(&state->changed);
  state->status = TURBO_OK;
}

static void fmq_bench_async_completion_state_destroy(
    fmq_bench_async_completion_state_t *state) {
  turbo_cond_destroy(&state->changed);
  turbo_mutex_destroy(&state->mutex);
}

static void fmq_bench_async_completion(void *ctx, int status) {
  fmq_bench_async_completion_state_t *state =
      (fmq_bench_async_completion_state_t *)ctx;
  if (!state) return;
  turbo_mutex_lock(&state->mutex);
  if (state->status == TURBO_OK && status != TURBO_OK) state->status = status;
  state->completed_ns = turbo_hrtime();
  state->completed += 1u;
  turbo_cond_signal(&state->changed);
  turbo_mutex_unlock(&state->mutex);
}

static int fmq_bench_wait_async_completed(fmq_bench_async_completion_state_t *state,
                                          size_t expected_completed,
                                          uint64_t *completed_ns) {
  const uint64_t started_ns = turbo_hrtime();
  const uint64_t deadline_ns = started_ns > UINT64_MAX - FMQ_BENCH_WAIT_TIMEOUT_NS
                                   ? UINT64_MAX
                                   : started_ns + FMQ_BENCH_WAIT_TIMEOUT_NS;
  int rc = TURBO_OK;
  if (!state || expected_completed == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&state->mutex);
  while (state->completed < expected_completed && state->status == TURBO_OK) {
    const uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns ||
        turbo_cond_timedwait(&state->changed, &state->mutex, deadline_ns - now_ns) != TURBO_OK) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
  }
  if (rc == TURBO_OK) rc = state->status;
  if (rc == TURBO_OK && completed_ns) *completed_ns = state->completed_ns;
  turbo_mutex_unlock(&state->mutex);
  return rc;
}

static int fmq_bench_req_send_when_ready(turbo_flow_fmq_app_t *client, const void *payload,
                                         size_t payload_bytes) {
  int rc = TURBO_EBUSY;
  for (size_t attempt = 0u; attempt < FMQ_BENCH_REQ_READY_ATTEMPTS && rc == TURBO_EBUSY;
       ++attempt) {
    rc = turbo_flow_fmq_app_send(client, payload, payload_bytes);
    if (rc == TURBO_EBUSY) turbo_thread_yield();
  }
  return rc;
}

static int fmq_bench_roundtrip(turbo_flow_fmq_app_t *client, fmq_bench_receive_state_t *state,
                               const void *payload, size_t payload_bytes, size_t expected_replies,
                               uint64_t *latency_ns) {
  const uint64_t started_ns = turbo_hrtime();
  uint64_t received_ns = 0u;
  int rc;
  if (!client || !state || !payload || payload_bytes == 0u || expected_replies == 0u)
    return TURBO_EINVAL;

  rc = fmq_bench_req_send_when_ready(client, payload, payload_bytes);
  if (rc != TURBO_OK) return rc;
  rc = fmq_bench_wait_received(state, expected_replies, &received_ns);
  if (rc != TURBO_OK) return rc;
  if (received_ns < started_ns) return TURBO_EPROTO;
  if (latency_ns) *latency_ns = received_ns - started_ns;
  return TURBO_OK;
}

static int fmq_bench_first_roundtrip(turbo_flow_fmq_app_t *client, fmq_bench_receive_state_t *state,
                                     const void *payload, size_t payload_bytes) {
  int rc = TURBO_ENOTCONN;
  for (size_t attempt = 0u; attempt < FMQ_BENCH_CONNECT_ATTEMPTS && rc == TURBO_ENOTCONN;
       ++attempt) {
    rc = fmq_bench_roundtrip(client, state, payload, payload_bytes, 1u, NULL);
    if (rc == TURBO_ENOTCONN) turbo_sleep_ms(FMQ_BENCH_CONNECT_RETRY_MS);
  }
  return rc;
}

static int fmq_bench_send_batch(turbo_flow_fmq_app_t *sender, fmq_bench_receive_state_t *state,
                                const void *payload, size_t payload_bytes, size_t message_count,
                                size_t expected_received, uint64_t *latency_ns) {
  const uint64_t started_ns = turbo_hrtime();
  uint64_t received_ns = 0u;
  int rc = TURBO_OK;
  if (!sender || !state || !payload || payload_bytes == 0u || message_count == 0u ||
      expected_received < message_count)
    return TURBO_EINVAL;

  for (size_t i = 0u; i < message_count; ++i) {
    rc = turbo_flow_fmq_app_send(sender, payload, payload_bytes);
    if (rc != TURBO_OK) return rc;
  }
  rc = fmq_bench_wait_received(state, expected_received, &received_ns);
  if (rc != TURBO_OK) return rc;
  if (received_ns < started_ns) return TURBO_EPROTO;
  if (latency_ns) *latency_ns = received_ns - started_ns;
  return TURBO_OK;
}

static int fmq_bench_send_api_batch(turbo_flow_fmq_app_t *sender,
                                    fmq_bench_receive_state_t *state,
                                    const turbo_flow_fmq_app_send_item_t *items,
                                    size_t message_count, size_t expected_received,
                                    uint64_t *latency_ns) {
  const uint64_t started_ns = turbo_hrtime();
  uint64_t received_ns = 0u;
  size_t submitted = 0u;
  int rc;
  if (!sender || !state || !items || message_count == 0u ||
      expected_received < message_count)
    return TURBO_EINVAL;

  rc = turbo_flow_fmq_app_send_batch(sender, items, message_count, &submitted);
  if (rc != TURBO_OK) return rc;
  if (submitted != message_count) return TURBO_EPROTO;
  rc = fmq_bench_wait_received(state, expected_received, &received_ns);
  if (rc != TURBO_OK) return rc;
  if (received_ns < started_ns) return TURBO_EPROTO;
  if (latency_ns) *latency_ns = received_ns - started_ns;
  return TURBO_OK;
}

static int fmq_bench_send_async_batch(
    turbo_flow_fmq_app_t *sender, fmq_bench_receive_state_t *receive_state,
    fmq_bench_async_completion_state_t *completion_state, const void *payload,
    size_t payload_bytes, size_t message_count, size_t expected_received,
    size_t expected_completed, uint64_t *latency_ns) {
  const uint64_t started_ns = turbo_hrtime();
  uint64_t received_ns = 0u;
  uint64_t completed_ns = 0u;
  int rc = TURBO_OK;
  if (!sender || !receive_state || !completion_state || !payload || payload_bytes == 0u ||
      message_count == 0u || expected_received < message_count ||
      expected_completed < message_count)
    return TURBO_EINVAL;
  for (size_t i = 0u; i < message_count; ++i) {
    rc = turbo_flow_fmq_app_send_async(sender, payload, payload_bytes,
                                       fmq_bench_async_completion, completion_state);
    if (rc != TURBO_OK) return rc;
  }
  rc = fmq_bench_wait_received(receive_state, expected_received, &received_ns);
  if (rc == TURBO_OK)
    rc = fmq_bench_wait_async_completed(completion_state, expected_completed, &completed_ns);
  if (rc != TURBO_OK) return rc;
  if (received_ns < started_ns || completed_ns < started_ns) return TURBO_EPROTO;
  if (latency_ns)
    *latency_ns = (received_ns > completed_ns ? received_ns : completed_ns) - started_ns;
  return TURBO_OK;
}

static int fmq_bench_first_message(turbo_flow_fmq_app_t *sender, fmq_bench_receive_state_t *state,
                                   const void *payload, size_t payload_bytes) {
  int rc = TURBO_ENOTCONN;
  for (size_t attempt = 0u; attempt < FMQ_BENCH_CONNECT_ATTEMPTS && rc == TURBO_ENOTCONN;
       ++attempt) {
    rc = turbo_flow_fmq_app_send(sender, payload, payload_bytes);
    if (rc == TURBO_OK) rc = fmq_bench_wait_received(state, 1u, NULL);
    if (rc == TURBO_ENOTCONN) turbo_sleep_ms(FMQ_BENCH_CONNECT_RETRY_MS);
  }
  return rc;
}

static void fmq_bench_tcp_req_rep(const fmq_bench_case_t *bench_case) {
  turbo_flow_fmq_config_t server_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_config_t client_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_app_options_t server_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_options_t client_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_t *server = NULL;
  turbo_flow_fmq_app_t *client = NULL;
  fmq_bench_receive_state_t receive_state;
  unsigned char *payload = NULL;
  uint64_t *latencies = NULL;
  uint64_t measurement_started_ns = 0u;
  uint64_t measurement_finished_ns = 0u;
  uint64_t elapsed_ns;
  size_t expected_replies = 0u;
  size_t completed = 0u;
  size_t sample_index = 0u;
  size_t captured_replies = 0u;
  size_t captured_payload_bytes = 0u;
  unsigned short port;
  int captured_status = TURBO_EALREADY;
  int receive_state_initialized = 0;
  int server_started = 0;
  int client_started = 0;
  int rc = TURBO_OK;
  double throughput;
  double mib_per_second;
  flow_fmq_send_profile_snapshot_t profile = FLOW_FMQ_SEND_PROFILE_SNAPSHOT_INIT;
  int profile_enabled = 0;
  int profile_status = TURBO_ENOTSUP;

  check_not_null(bench_case);
  if (!bench_case) return;
  check_not_null(bench_case->title);
  check_size_gt(bench_case->payload_bytes, 0u);
  check_size_gt(bench_case->warmup, 0u);
  check_size_gt(bench_case->samples, 0u);
  if (!bench_case->title || bench_case->payload_bytes == 0u || bench_case->warmup == 0u ||
      bench_case->samples == 0u)
    return;

  port = fmq_bench_loopback_port();
  payload = (unsigned char *)malloc(bench_case->payload_bytes);
  latencies = (uint64_t *)calloc(bench_case->samples, sizeof(*latencies));
  check_int_gt(port, 0);
  check_not_null(payload);
  check_not_null(latencies);
  if (port == 0u || !payload || !latencies) goto cleanup;
  memset(payload, 0x5a, bench_case->payload_bytes);
  fmq_bench_receive_state_init(&receive_state, bench_case->payload_bytes);
  receive_state_initialized = 1;

  server_endpoint.pattern = TURBO_FLOW_FMQ_REP;
  server_endpoint.mode = TURBO_FLOW_FMQ_BIND;
  server_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  server_endpoint.host = "127.0.0.1";
  server_endpoint.port = (int)port;
  server_endpoint.timeout_ms = 2000u;
  server_options.on_message = fmq_bench_echo_request;

  client_endpoint.pattern = TURBO_FLOW_FMQ_REQ;
  client_endpoint.mode = TURBO_FLOW_FMQ_CONNECT;
  client_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  client_endpoint.host = "127.0.0.1";
  client_endpoint.port = (int)port;
  client_endpoint.timeout_ms = 2000u;
  client_options.on_message = fmq_bench_capture_message;
  client_options.message_ctx = &receive_state;

  rc = turbo_flow_fmq_app_create(&server_endpoint, &server_options, &server);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  rc = turbo_flow_fmq_app_create(&client_endpoint, &client_options, &client);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  rc = turbo_flow_fmq_app_start(server);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  server_started = 1;
  rc = turbo_flow_fmq_app_start(client);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  client_started = 1;

  rc = fmq_bench_first_roundtrip(client, &receive_state, payload, bench_case->payload_bytes);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  expected_replies = 1u;
  for (size_t i = 1u; i < bench_case->warmup; ++i) {
    expected_replies += 1u;
    rc = fmq_bench_roundtrip(client, &receive_state, payload, bench_case->payload_bytes,
                             expected_replies, NULL);
    if (rc != TURBO_OK) break;
  }
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;

  profile_enabled = fmq_bench_profile_begin();
  benchmark_io(bench_case->title, bench_case->samples, 1u, bench_case->payload_bytes * 2u) {
    if (sample_index == 0u) measurement_started_ns = turbo_hrtime();
    if (rc == TURBO_OK) {
      expected_replies += 1u;
      rc = fmq_bench_roundtrip(client, &receive_state, payload, bench_case->payload_bytes,
                               expected_replies, &latencies[sample_index]);
      if (rc == TURBO_OK) {
        completed += 1u;
        measurement_finished_ns = turbo_hrtime();
      }
    }
    sample_index += 1u;
  }
  if (profile_enabled) {
    flow_fmq_send_profile_set_enabled(0);
    profile_enabled = 0;
    profile_status = flow_fmq_send_profile_snapshot(&profile);
    check_int_eq(profile_status, TURBO_OK);
  }

  check_int_eq(rc, TURBO_OK);
  check_size_eq(sample_index, bench_case->samples);
  check_size_eq(completed, bench_case->samples);
  if (rc != TURBO_OK || completed != bench_case->samples) goto cleanup;

  turbo_mutex_lock(&receive_state.mutex);
  captured_replies = receive_state.received;
  captured_payload_bytes = receive_state.last_payload_bytes;
  captured_status = receive_state.status;
  turbo_mutex_unlock(&receive_state.mutex);
  check_int_eq(captured_status, TURBO_OK);
  check_size_eq(captured_replies, bench_case->warmup + bench_case->samples);
  check_size_eq(captured_payload_bytes, bench_case->payload_bytes);
  if (captured_status != TURBO_OK || captured_replies != bench_case->warmup + bench_case->samples ||
      captured_payload_bytes != bench_case->payload_bytes)
    goto cleanup;

  elapsed_ns = measurement_finished_ns - measurement_started_ns;
  check_true(measurement_finished_ns >= measurement_started_ns);
  check_true(elapsed_ns > 0u);
  if (measurement_finished_ns < measurement_started_ns || elapsed_ns == 0u) goto cleanup;

  /* Reporting sorts one bounded sample array: O(n log n) time and O(n) sample space. */
  qsort(latencies, completed, sizeof(*latencies), fmq_bench_u64_compare);
  throughput = ((double)completed * 1000000000.0) / (double)elapsed_ns;
  mib_per_second = ((double)completed * (double)bench_case->payload_bytes * 2.0 * 1000000000.0) /
                   ((double)elapsed_ns * 1024.0 * 1024.0);
  printf("FMQ_BENCH_RESULT component=application_facade pattern=req_rep transport=tcp"
         " mode=serialized-echo payload_bytes=%zu application_bytes_roundtrip=%zu"
         " warmup=%zu samples=%zu throughput_roundtrip_s=%.2f mib_s=%.2f"
         " p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
         bench_case->payload_bytes, bench_case->payload_bytes * 2u, bench_case->warmup,
         bench_case->samples, throughput, mib_per_second,
         fmq_bench_percentile(latencies, completed, 50u),
         fmq_bench_percentile(latencies, completed, 95u),
         fmq_bench_percentile(latencies, completed, 99u));
  if (profile_status == TURBO_OK) {
    check_uint_eq(profile.samples, (uint64_t)bench_case->samples * 2u);
    check_uint_eq(profile.socket_samples, profile.samples);
    check_true(profile.socket_cpu_samples == 0u ||
               profile.socket_cpu_samples == profile.socket_samples);
    fmq_bench_profile_print("req_rep", &profile);
  }

cleanup:
  if (profile_enabled) flow_fmq_send_profile_set_enabled(0);
  if (client_started) check_int_eq(turbo_flow_fmq_app_stop(client), TURBO_OK);
  if (server_started) check_int_eq(turbo_flow_fmq_app_stop(server), TURBO_OK);
  turbo_flow_fmq_app_destroy(client);
  turbo_flow_fmq_app_destroy(server);
  if (receive_state_initialized) fmq_bench_receive_state_destroy(&receive_state);
  free(latencies);
  free(payload);
}

static void fmq_bench_tcp_one_way(const fmq_bench_throughput_case_t *bench_case) {
  turbo_flow_fmq_config_t sender_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_config_t receiver_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_app_options_t sender_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_options_t receiver_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_t *sender = NULL;
  turbo_flow_fmq_app_t *receiver = NULL;
  fmq_bench_receive_state_t receive_state;
  fmq_bench_async_completion_state_t completion_state;
  unsigned char *payload = NULL;
  turbo_flow_fmq_app_send_item_t *batch_items = NULL;
  uint64_t *latencies = NULL;
  uint64_t measurement_started_ns = 0u;
  uint64_t measurement_finished_ns = 0u;
  uint64_t elapsed_ns;
  size_t bytes_per_sample;
  size_t measured_messages;
  size_t expected_received = 0u;
  size_t expected_completed = 0u;
  size_t completed_samples = 0u;
  size_t sample_index = 0u;
  size_t captured_received = 0u;
  size_t captured_payload_bytes = 0u;
  unsigned short port;
  int captured_status = TURBO_EALREADY;
  int receive_state_initialized = 0;
  int completion_state_initialized = 0;
  int sender_started = 0;
  int receiver_started = 0;
  int rc = TURBO_OK;
  double throughput;
  double mib_per_second;
  flow_fmq_send_profile_snapshot_t profile = FLOW_FMQ_SEND_PROFILE_SNAPSHOT_INIT;
  int profile_enabled = 0;
  int profile_status = TURBO_ENOTSUP;

  check_not_null(bench_case);
  if (!bench_case) return;
  check_not_null(bench_case->title);
  check_not_null(bench_case->result_pattern);
  check_size_gt(bench_case->payload_bytes, 0u);
  check_size_gt(bench_case->messages_per_sample, 0u);
  check_size_gt(bench_case->warmup_messages, 0u);
  check_size_gt(bench_case->samples, 0u);
  check_true(bench_case->sender_mode != bench_case->receiver_mode);
  check_true(bench_case->payload_bytes <= SIZE_MAX / bench_case->messages_per_sample);
  check_true(bench_case->messages_per_sample <= SIZE_MAX / bench_case->samples);
  check_false(bench_case->use_batch_api && bench_case->use_async_api);
  check_true((!bench_case->use_batch_api && !bench_case->use_async_api) ||
             bench_case->warmup_messages <= bench_case->messages_per_sample);
  if (!bench_case->title || !bench_case->result_pattern || bench_case->payload_bytes == 0u ||
      bench_case->messages_per_sample == 0u || bench_case->warmup_messages == 0u ||
      bench_case->samples == 0u || bench_case->sender_mode == bench_case->receiver_mode ||
      bench_case->payload_bytes > SIZE_MAX / bench_case->messages_per_sample ||
      bench_case->messages_per_sample > SIZE_MAX / bench_case->samples ||
      (bench_case->use_batch_api && bench_case->use_async_api) ||
      ((bench_case->use_batch_api || bench_case->use_async_api) &&
       bench_case->warmup_messages > bench_case->messages_per_sample))
    return;

  bytes_per_sample = bench_case->payload_bytes * bench_case->messages_per_sample;
  measured_messages = bench_case->messages_per_sample * bench_case->samples;
  check_true(bench_case->warmup_messages <= SIZE_MAX - measured_messages);
  if (bench_case->warmup_messages > SIZE_MAX - measured_messages) return;

  port = fmq_bench_loopback_port();
  payload = (unsigned char *)malloc(bench_case->payload_bytes);
  latencies = (uint64_t *)calloc(bench_case->samples, sizeof(*latencies));
  if (bench_case->use_batch_api) {
    batch_items = (turbo_flow_fmq_app_send_item_t *)calloc(
        bench_case->messages_per_sample, sizeof(*batch_items));
  }
  check_int_gt(port, 0);
  check_not_null(payload);
  check_not_null(latencies);
  if (bench_case->use_batch_api) check_not_null(batch_items);
  if (port == 0u || !payload || !latencies ||
      (bench_case->use_batch_api && !batch_items))
    goto cleanup;
  memset(payload, 0x5a, bench_case->payload_bytes);
  if (bench_case->use_batch_api) {
    for (size_t i = 0u; i < bench_case->messages_per_sample; ++i) {
      batch_items[i].data = payload;
      batch_items[i].data_size = bench_case->payload_bytes;
    }
  }
  fmq_bench_receive_state_init(&receive_state, bench_case->payload_bytes);
  receive_state_initialized = 1;
  if (bench_case->use_async_api) {
    fmq_bench_async_completion_state_init(&completion_state);
    completion_state_initialized = 1;
  }

  sender_endpoint.pattern = bench_case->sender_pattern;
  sender_endpoint.mode = bench_case->sender_mode;
  sender_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  sender_endpoint.host = "127.0.0.1";
  sender_endpoint.port = (int)port;
  sender_endpoint.timeout_ms = 2000u;
  sender_endpoint.identity = bench_case->sender_identity;
  sender_endpoint.topic = bench_case->sender_topic;

  receiver_endpoint.pattern = bench_case->receiver_pattern;
  receiver_endpoint.mode = bench_case->receiver_mode;
  receiver_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  receiver_endpoint.host = "127.0.0.1";
  receiver_endpoint.port = (int)port;
  receiver_endpoint.timeout_ms = 2000u;
  receiver_endpoint.topic = bench_case->receiver_topic;
  receiver_options.on_message = fmq_bench_capture_message;
  receiver_options.message_ctx = &receive_state;

  rc = turbo_flow_fmq_app_create(&sender_endpoint, &sender_options, &sender);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  rc = turbo_flow_fmq_app_create(&receiver_endpoint, &receiver_options, &receiver);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  if (bench_case->use_async_api) {
    turbo_flow_fmq_app_async_send_config_t async_config =
        TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT;
    async_config.batch_size = bench_case->messages_per_sample;
    rc = turbo_flow_fmq_app_configure_async_send(sender, &async_config);
    check_int_eq(rc, TURBO_OK);
    if (rc != TURBO_OK) goto cleanup;
  }

  if (bench_case->sender_mode == TURBO_FLOW_FMQ_BIND) {
    rc = turbo_flow_fmq_app_start(sender);
    check_int_eq(rc, TURBO_OK);
    if (rc != TURBO_OK) goto cleanup;
    sender_started = 1;
    rc = turbo_flow_fmq_app_start(receiver);
    check_int_eq(rc, TURBO_OK);
    if (rc != TURBO_OK) goto cleanup;
    receiver_started = 1;
  } else {
    rc = turbo_flow_fmq_app_start(receiver);
    check_int_eq(rc, TURBO_OK);
    if (rc != TURBO_OK) goto cleanup;
    receiver_started = 1;
    rc = turbo_flow_fmq_app_start(sender);
    check_int_eq(rc, TURBO_OK);
    if (rc != TURBO_OK) goto cleanup;
    sender_started = 1;
  }

  rc = fmq_bench_first_message(sender, &receive_state, payload, bench_case->payload_bytes);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto cleanup;
  expected_received = 1u;
  if (bench_case->warmup_messages > 1u) {
    const size_t remaining_warmup = bench_case->warmup_messages - 1u;
    expected_received += remaining_warmup;
    if (bench_case->use_async_api) {
      expected_completed += remaining_warmup;
      rc = fmq_bench_send_async_batch(
          sender, &receive_state, &completion_state, payload, bench_case->payload_bytes,
          remaining_warmup, expected_received, expected_completed, NULL);
    } else if (bench_case->use_batch_api) {
      rc = fmq_bench_send_api_batch(sender, &receive_state, batch_items, remaining_warmup,
                                    expected_received, NULL);
    } else {
      rc = fmq_bench_send_batch(sender, &receive_state, payload, bench_case->payload_bytes,
                                remaining_warmup, expected_received, NULL);
    }
    check_int_eq(rc, TURBO_OK);
    if (rc != TURBO_OK) goto cleanup;
  }

  profile_enabled = fmq_bench_profile_begin();
  benchmark_io(bench_case->title, bench_case->samples, bench_case->messages_per_sample,
               bytes_per_sample) {
    if (sample_index == 0u) measurement_started_ns = turbo_hrtime();
    if (rc == TURBO_OK) {
      expected_received += bench_case->messages_per_sample;
      if (bench_case->use_async_api) {
        expected_completed += bench_case->messages_per_sample;
        rc = fmq_bench_send_async_batch(
            sender, &receive_state, &completion_state, payload, bench_case->payload_bytes,
            bench_case->messages_per_sample, expected_received, expected_completed,
            &latencies[sample_index]);
      } else if (bench_case->use_batch_api) {
        rc = fmq_bench_send_api_batch(sender, &receive_state, batch_items,
                                      bench_case->messages_per_sample, expected_received,
                                      &latencies[sample_index]);
      } else {
        rc = fmq_bench_send_batch(sender, &receive_state, payload,
                                  bench_case->payload_bytes,
                                  bench_case->messages_per_sample, expected_received,
                                  &latencies[sample_index]);
      }
      if (rc == TURBO_OK) {
        completed_samples += 1u;
        measurement_finished_ns = turbo_hrtime();
      }
    }
    sample_index += 1u;
  }
  if (profile_enabled) {
    flow_fmq_send_profile_set_enabled(0);
    profile_enabled = 0;
    profile_status = flow_fmq_send_profile_snapshot(&profile);
    check_int_eq(profile_status, TURBO_OK);
  }

  check_int_eq(rc, TURBO_OK);
  check_size_eq(sample_index, bench_case->samples);
  check_size_eq(completed_samples, bench_case->samples);
  if (rc != TURBO_OK || completed_samples != bench_case->samples) goto cleanup;

  turbo_mutex_lock(&receive_state.mutex);
  captured_received = receive_state.received;
  captured_payload_bytes = receive_state.last_payload_bytes;
  captured_status = receive_state.status;
  turbo_mutex_unlock(&receive_state.mutex);
  check_int_eq(captured_status, TURBO_OK);
  check_size_eq(captured_received, bench_case->warmup_messages + measured_messages);
  check_size_eq(captured_payload_bytes, bench_case->payload_bytes);
  if (captured_status != TURBO_OK ||
      captured_received != bench_case->warmup_messages + measured_messages ||
      captured_payload_bytes != bench_case->payload_bytes)
    goto cleanup;

  elapsed_ns = measurement_finished_ns - measurement_started_ns;
  check_true(measurement_finished_ns >= measurement_started_ns);
  check_true(elapsed_ns > 0u);
  if (measurement_finished_ns < measurement_started_ns || elapsed_ns == 0u) goto cleanup;

  qsort(latencies, completed_samples, sizeof(*latencies), fmq_bench_u64_compare);
  throughput = ((double)measured_messages * 1000000000.0) / (double)elapsed_ns;
  mib_per_second = ((double)measured_messages * (double)bench_case->payload_bytes * 1000000000.0) /
                   ((double)elapsed_ns * 1024.0 * 1024.0);
  info("FMQ_BENCH_RESULT component=application_facade pattern=%s transport=tcp"
         " mode=%s payload_bytes=%zu messages_per_sample=%zu"
         " warmup_messages=%zu samples=%zu throughput_message_s=%.2f mib_s=%.2f"
         " batch_p50_ns=%" PRIu64 " batch_p95_ns=%" PRIu64 " batch_p99_ns=%" PRIu64 "\n",
         bench_case->result_pattern,
         bench_case->use_async_api
             ? "async-micro-batch-one-way"
             : (bench_case->use_batch_api ? "batch-api-one-way" : "serialized-one-way"),
         bench_case->payload_bytes, bench_case->messages_per_sample, bench_case->warmup_messages,
         bench_case->samples, throughput, mib_per_second,
         fmq_bench_percentile(latencies, completed_samples, 50u),
         fmq_bench_percentile(latencies, completed_samples, 95u),
         fmq_bench_percentile(latencies, completed_samples, 99u));
  if (profile_status == TURBO_OK) {
    check_uint_eq(profile.samples, measured_messages);
    if (bench_case->use_batch_api || bench_case->use_async_api) {
      check_uint_eq(profile.socket_samples, 0u);
      check_true(profile.batch_samples > 0u);
      check_true(profile.batch_socket_calls > 0u);
      check_uint_eq(profile.batch_socket_frames, measured_messages);
      check_true(profile.batch_socket_cpu_samples == 0u ||
                 profile.batch_socket_cpu_samples == profile.batch_socket_calls);
    } else {
      check_uint_eq(profile.socket_samples, profile.samples);
      check_true(profile.socket_cpu_samples == 0u ||
                 profile.socket_cpu_samples == profile.socket_samples);
    }
    fmq_bench_profile_print(bench_case->result_pattern, &profile);
  }

cleanup:
  if (profile_enabled) flow_fmq_send_profile_set_enabled(0);
  if (bench_case && bench_case->sender_mode == TURBO_FLOW_FMQ_CONNECT) {
    if (sender_started) check_int_eq(turbo_flow_fmq_app_stop(sender), TURBO_OK);
    if (receiver_started) check_int_eq(turbo_flow_fmq_app_stop(receiver), TURBO_OK);
  } else {
    if (receiver_started) check_int_eq(turbo_flow_fmq_app_stop(receiver), TURBO_OK);
    if (sender_started) check_int_eq(turbo_flow_fmq_app_stop(sender), TURBO_OK);
  }
  turbo_flow_fmq_app_destroy(receiver);
  turbo_flow_fmq_app_destroy(sender);
  if (receive_state_initialized) fmq_bench_receive_state_destroy(&receive_state);
  if (completion_state_initialized)
    fmq_bench_async_completion_state_destroy(&completion_state);
  free(latencies);
  free(batch_items);
  free(payload);
}

spec("flowmq application facade") {
  bench("real TCP REQ REP 64-byte serialized round trip") {
    static const fmq_bench_case_t bench_case = {
        "real TCP REQ REP 64-byte serialized round trip", FMQ_BENCH_TYPICAL_PAYLOAD_BYTES,
        FMQ_BENCH_TYPICAL_WARMUP, FMQ_BENCH_TYPICAL_SAMPLES};
    fmq_bench_tcp_req_rep(&bench_case);
  }

  bench("real TCP REQ REP 64-KiB serialized round trip") {
    static const fmq_bench_case_t bench_case = {"real TCP REQ REP 64-KiB serialized round trip",
                                                FMQ_BENCH_LARGE_PAYLOAD_BYTES,
                                                FMQ_BENCH_LARGE_WARMUP, FMQ_BENCH_LARGE_SAMPLES};
    fmq_bench_tcp_req_rep(&bench_case);
  }

  bench("real TCP DEALER ROUTER 64-byte serialized one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP DEALER ROUTER 64-byte serialized one-way throughput",
        "dealer_router",
        TURBO_FLOW_FMQ_DEALER,
        TURBO_FLOW_FMQ_CONNECT,
        TURBO_FLOW_FMQ_ROUTER,
        TURBO_FLOW_FMQ_BIND,
        "bench-dealer",
        NULL,
        NULL,
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        0};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP DEALER ROUTER 64-byte batch API one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP DEALER ROUTER 64-byte batch API one-way throughput",
        "dealer_router",
        TURBO_FLOW_FMQ_DEALER,
        TURBO_FLOW_FMQ_CONNECT,
        TURBO_FLOW_FMQ_ROUTER,
        TURBO_FLOW_FMQ_BIND,
        "bench-dealer",
        NULL,
        NULL,
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP DEALER ROUTER 64-byte async micro-batch one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP DEALER ROUTER 64-byte async micro-batch one-way throughput",
        "dealer_router",
        TURBO_FLOW_FMQ_DEALER,
        TURBO_FLOW_FMQ_CONNECT,
        TURBO_FLOW_FMQ_ROUTER,
        TURBO_FLOW_FMQ_BIND,
        "bench-dealer",
        NULL,
        NULL,
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        0,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP DEALER ROUTER 64-KiB batch API one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP DEALER ROUTER 64-KiB batch API one-way throughput",
        "dealer_router",
        TURBO_FLOW_FMQ_DEALER,
        TURBO_FLOW_FMQ_CONNECT,
        TURBO_FLOW_FMQ_ROUTER,
        TURBO_FLOW_FMQ_BIND,
        "bench-dealer",
        NULL,
        NULL,
        FMQ_BENCH_LARGE_BATCH_PAYLOAD_BYTES,
        FMQ_BENCH_LARGE_BATCH_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_LARGE_BATCH_WARMUP_MESSAGES,
        FMQ_BENCH_LARGE_BATCH_SAMPLES,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUB SUB 64-byte serialized one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUB SUB 64-byte serialized one-way throughput",
        "pub_sub",
        TURBO_FLOW_FMQ_PUB,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_SUB,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        "bench.throughput",
        "bench.",
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        0};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUB SUB 64-byte batch API one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUB SUB 64-byte batch API one-way throughput",
        "pub_sub",
        TURBO_FLOW_FMQ_PUB,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_SUB,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        "bench.throughput",
        "bench.",
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUB SUB 64-byte async micro-batch one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUB SUB 64-byte async micro-batch one-way throughput",
        "pub_sub",
        TURBO_FLOW_FMQ_PUB,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_SUB,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        "bench.throughput",
        "bench.",
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        0,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUB SUB 64-KiB batch API one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUB SUB 64-KiB batch API one-way throughput",
        "pub_sub",
        TURBO_FLOW_FMQ_PUB,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_SUB,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        "bench.throughput",
        "bench.",
        FMQ_BENCH_LARGE_BATCH_PAYLOAD_BYTES,
        FMQ_BENCH_LARGE_BATCH_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_LARGE_BATCH_WARMUP_MESSAGES,
        FMQ_BENCH_LARGE_BATCH_SAMPLES,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUSH PULL 64-byte serialized one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUSH PULL 64-byte serialized one-way throughput",
        "push_pull",
        TURBO_FLOW_FMQ_PUSH,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_PULL,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        NULL,
        NULL,
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        0};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUSH PULL 64-byte batch API one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUSH PULL 64-byte batch API one-way throughput",
        "push_pull",
        TURBO_FLOW_FMQ_PUSH,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_PULL,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        NULL,
        NULL,
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUSH PULL 64-byte async micro-batch one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUSH PULL 64-byte async micro-batch one-way throughput",
        "push_pull",
        TURBO_FLOW_FMQ_PUSH,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_PULL,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        NULL,
        NULL,
        FMQ_BENCH_THROUGHPUT_PAYLOAD_BYTES,
        FMQ_BENCH_THROUGHPUT_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_THROUGHPUT_WARMUP_MESSAGES,
        FMQ_BENCH_THROUGHPUT_SAMPLES,
        0,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }

  bench("real TCP PUSH PULL 64-KiB batch API one-way throughput") {
    static const fmq_bench_throughput_case_t bench_case = {
        "real TCP PUSH PULL 64-KiB batch API one-way throughput",
        "push_pull",
        TURBO_FLOW_FMQ_PUSH,
        TURBO_FLOW_FMQ_BIND,
        TURBO_FLOW_FMQ_PULL,
        TURBO_FLOW_FMQ_CONNECT,
        NULL,
        NULL,
        NULL,
        FMQ_BENCH_LARGE_BATCH_PAYLOAD_BYTES,
        FMQ_BENCH_LARGE_BATCH_MESSAGES_PER_SAMPLE,
        FMQ_BENCH_LARGE_BATCH_WARMUP_MESSAGES,
        FMQ_BENCH_LARGE_BATCH_SAMPLES,
        1};
    fmq_bench_tcp_one_way(&bench_case);
  }
}
