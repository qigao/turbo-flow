#include "turbo_flow_redis.h"

#include "CoroNet/turbo_coro_socket.h"
#include "redis_client.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow.h"
#include "turbo_thread.h"

#include <inttypes.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  REDIS_BENCH_SAMPLES = 5,
  REDIS_BENCH_ENTRIES = 1024,
  REDIS_BENCH_READ_COUNT = 64,
  REDIS_BENCH_PAYLOAD_BYTES = 64,
  REDIS_BENCH_TIMEOUT_MS = 30000
};

typedef struct redis_bench_admin_s {
  const char *stream;
  size_t entry_count;
  int status;
  int done;
} redis_bench_admin_t;

typedef struct redis_bench_capture_s {
  atomic_uint_fast64_t messages;
  atomic_uint_fast64_t bytes;
} redis_bench_capture_t;

static int redis_bench_u64_compare(const void *lhs, const void *rhs) {
  const uint64_t left = *(const uint64_t *)lhs;
  const uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

static void redis_bench_admin_task(coro_t *co, void *arg) {
  redis_bench_admin_t *admin = (redis_bench_admin_t *)arg;
  redis_client_t *client = NULL;
  redis_command_result_t command = REDIS_COMMAND_RESULT_INIT;
  static const char payload[REDIS_BENCH_PAYLOAD_BYTES] = {0x52};
  const char *del_argv[] = {"DEL", admin->stream};
  const char *fields[] = {"payload"};
  const char *values[] = {payload};
  const size_t lengths[] = {sizeof(payload)};
  (void)co;

  admin->status = TURBO_ENOMEM;
  client = redis_client_create("127.0.0.1", 6379u);
  if (!client) goto done;
  admin->status = redis_client_connect(client, NULL, NULL);
  if (admin->status != TURBO_OK) goto done;
  admin->status = redis_commandv_result(client, 2, del_argv, NULL, &command);
  redis_command_result_clear(&command);
  for (size_t i = 0u; admin->status == TURBO_OK && i < admin->entry_count; ++i) {
    admin->status =
        redis_xadd_result(client, admin->stream, 0u, 1u, fields, values, lengths, &command);
    redis_command_result_clear(&command);
  }

done:
  redis_command_result_clear(&command);
  redis_client_destroy(client);
  admin->done = 1;
}

static int redis_bench_admin_run(const char *stream, size_t entry_count) {
  coro_context_t *context = coro_context_create(NULL);
  redis_bench_admin_t admin;
  int rc;
  if (!context) return TURBO_ENOMEM;
  memset(&admin, 0, sizeof(admin));
  admin.stream = stream;
  admin.entry_count = entry_count;
  admin.status = TURBO_EALREADY;
  rc = coro_context_spawn(context, redis_bench_admin_task, &admin);
  while (rc == TURBO_OK && !admin.done)
    (void)coro_context_run(context, TURBO_RUN_ONCE);
  if (rc == TURBO_OK) rc = admin.status;
  coro_context_destroy(context);
  return rc;
}

static int redis_bench_capture(turbo_flow_msg_t *message, void *ctx) {
  redis_bench_capture_t *capture = (redis_bench_capture_t *)ctx;
  if (!capture || !message || (message->payload.len > 0u && !message->payload.data)) {
    return TURBO_EINVAL;
  }
  atomic_fetch_add_explicit(&capture->bytes, message->payload.len, memory_order_relaxed);
  atomic_fetch_add_explicit(&capture->messages, 1u, memory_order_release);
  return TURBO_OK;
}

static int redis_bench_wait_messages(redis_bench_capture_t *capture, uint64_t expected) {
  const uint64_t deadline = turbo_hrtime() + (uint64_t)REDIS_BENCH_TIMEOUT_MS * UINT64_C(1000000);
  while (atomic_load_explicit(&capture->messages, memory_order_acquire) < expected) {
    if (turbo_hrtime() >= deadline) return TURBO_ETIMEDOUT;
    turbo_thread_yield();
  }
  return TURBO_OK;
}

static int redis_bench_source_sample(size_t sample, uint64_t *elapsed_ns,
                                     uint64_t *delivered_bytes) {
  static const char graph[] = "source redis_in adapter \"redis.in\"\n"
                              "stage capture\n"
                              "stage main {\n"
                              "  redis_in -> capture\n"
                              "}\n";
  redis_bench_capture_t capture;
  turbo_flow_redis_stream_config_t config;
  turbo_flow_t *flow = NULL;
  char stream[128];
  char group[128];
  char consumer[128];
  uint64_t begin = 0u;
  int started = 0;
  int rc;

  atomic_init(&capture.messages, 0u);
  atomic_init(&capture.bytes, 0u);
  memset(&config, 0, sizeof(config));
  (void)snprintf(stream, sizeof(stream), "turboflow:bench:source:%" PRIu64 ":%zu",
                 turbo_hrtime(), sample);
  (void)snprintf(group, sizeof(group), "bench-group-%zu", sample);
  (void)snprintf(consumer, sizeof(consumer), "bench-consumer-%zu", sample);
  rc = redis_bench_admin_run(stream, REDIS_BENCH_ENTRIES);
  if (rc != TURBO_OK) return rc;

  flow = turbo_flow_create();
  if (!flow) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  config.host = "127.0.0.1";
  config.port = 6379u;
  config.timeout_ms = REDIS_BENCH_TIMEOUT_MS;
  config.stream = stream;
  config.field = "payload";
  config.group = group;
  config.consumer = consumer;
  config.group_start_id = "0";
  config.read_count = REDIS_BENCH_READ_COUNT;
  config.block_ms = 1u;
  config.poll_interval_ms = 1u;
  config.create_group = 1;
  rc = turbo_flow_redis_register_stream_adapter(flow, "redis.in", &config);
  if (rc == TURBO_OK)
    rc = turbo_flow_register_stage_ex(flow, "capture", redis_bench_capture, &capture, NULL);
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  begin = turbo_hrtime();
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  if (rc == TURBO_OK) started = 1;
  if (rc == TURBO_OK) rc = redis_bench_wait_messages(&capture, REDIS_BENCH_ENTRIES);
  if (started) {
    const int stop_rc = turbo_flow_stop(flow);
    started = 0;
    if (rc == TURBO_OK) rc = stop_rc;
  }
  *elapsed_ns = turbo_hrtime() - begin;
  *delivered_bytes = atomic_load_explicit(&capture.bytes, memory_order_acquire);

done:
  if (started) (void)turbo_flow_stop(flow);
  turbo_flow_destroy(flow);
  {
    const int cleanup_rc = redis_bench_admin_run(stream, 0u);
    if (rc == TURBO_OK) rc = cleanup_rc;
  }
  return rc;
}

static void redis_bench_stream_source(void) {
  uint64_t elapsed[REDIS_BENCH_SAMPLES] = {0};
  uint64_t bytes[REDIS_BENCH_SAMPLES] = {0};
  int rc = TURBO_OK;
  for (size_t sample = 0u; sample < REDIS_BENCH_SAMPLES && rc == TURBO_OK; ++sample) {
    rc = redis_bench_source_sample(sample, &elapsed[sample], &bytes[sample]);
  }
  check_equal(rc, TURBO_OK);
  if (rc == TURBO_OK) {
    const size_t median = REDIS_BENCH_SAMPLES / 2u;
    qsort(elapsed, REDIS_BENCH_SAMPLES, sizeof(elapsed[0]), redis_bench_u64_compare);
    qsort(bytes, REDIS_BENCH_SAMPLES, sizeof(bytes[0]), redis_bench_u64_compare);
    check_equal(bytes[median],
                  (uint64_t)REDIS_BENCH_ENTRIES * (uint64_t)REDIS_BENCH_PAYLOAD_BYTES);
    printf("REDIS_BENCH_RESULT operation=stream_source samples=%u entries=%u read_count=%u "
           "payload_bytes=%u median_ns=%" PRIu64 " throughput_msg_s=%.2f throughput_mib_s=%.2f\n",
           REDIS_BENCH_SAMPLES, REDIS_BENCH_ENTRIES, REDIS_BENCH_READ_COUNT,
           REDIS_BENCH_PAYLOAD_BYTES, elapsed[median],
           elapsed[median] ? ((double)REDIS_BENCH_ENTRIES * 1000000000.0) / (double)elapsed[median]
                           : 0.0,
           elapsed[median] ? ((double)bytes[median] * 1000000000.0) /
                                 ((double)elapsed[median] * 1024.0 * 1024.0)
                           : 0.0);
  }
}

spec("TurboFlow Redis provider benchmarks") {
  bench("live Redis Stream source delivery") { redis_bench_stream_source(); }
}
