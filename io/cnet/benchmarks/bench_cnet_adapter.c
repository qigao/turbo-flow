#include "cnet_adapter_benchmark_stats.h"
#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include <salts/clock.h>
#include <salts/error_codes.h>
#include <salts/thread.h>

#include <inttypes.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(_WIN32)
  #include <windows.h>
#endif

#ifndef TF_CNET_BENCH_GIT_COMMIT
  #error "TF_CNET_BENCH_GIT_COMMIT must identify the measured source revision"
#endif

#ifndef TF_CNET_BENCH_BUILD_TYPE
  #error "TF_CNET_BENCH_BUILD_TYPE must identify the measured CMake build type"
#endif

#ifndef TF_CNET_BENCH_GIT_EXECUTABLE
  #error "TF_CNET_BENCH_GIT_EXECUTABLE must identify the Git executable"
#endif

#ifndef TF_CNET_BENCH_SOURCE_DIR
  #error "TF_CNET_BENCH_SOURCE_DIR must identify the measured source tree"
#endif

#ifndef TF_CNET_BENCH_CPU_MODEL
  #error "TF_CNET_BENCH_CPU_MODEL must identify the build-host processor"
#endif

#ifndef TF_CNET_BENCH_ASAN
  #error "TF_CNET_BENCH_ASAN must identify the AddressSanitizer mode"
#endif

#ifndef TF_CNET_BENCH_PRESET
  #error "TF_CNET_BENCH_PRESET must identify the active public user preset"
#endif

#if defined(_WIN32)
  #define cnet_bench_popen _popen
  #define cnet_bench_pclose _pclose
#else
  #define cnet_bench_popen popen
  #define cnet_bench_pclose pclose
#endif

enum {
  CNET_BENCH_PAYLOAD_BYTES = 256,
  CNET_BENCH_WARMUP_MESSAGES = 256,
  CNET_BENCH_SAMPLE_MESSAGES = 131072,
  CNET_BENCH_REPLICATES = 7,
  CNET_BENCH_CAPACITY = 64,
  CNET_BENCH_QUEUE_CAPACITY = 128,
  CNET_BENCH_TIMEOUT_MS = 20000,
  CNET_BENCH_GIT_COMMAND_BYTES = 4096,
  CNET_BENCH_GIT_OUTPUT_BYTES = 128,
  CNET_BENCH_OWNER_ALLOCATIONS_PER_MESSAGE = 3
};

typedef struct cnet_bench_run_state_s cnet_bench_run_state_t;

typedef struct cnet_bench_completion_s {
  cnet_bench_run_state_t *run;
  uint64_t started_ns;
  uint64_t latency_ns;
  int status;
  atomic_bool done;
} cnet_bench_completion_t;

struct cnet_bench_run_state_s {
  atomic_size_t completed;
  atomic_bool terminal_captured;
  size_t expected;
  uint64_t terminal_wall_ns;
  uint64_t terminal_cpu_ns;
  int terminal_cpu_status;
  bool record_metrics;
};

typedef struct cnet_bench_peer_s {
  cnet_packet_endpoint endpoint;
  size_t receives;
  size_t invalid_receives;
  int status;
  bool initialized;
} cnet_bench_peer_t;

typedef struct cnet_bench_fixture_s {
  cnet_bench_peer_t peer;
  turbo_flow_t *flow;
  turbo_flow_cnet_packet_sink_t *sink;
  turbo_flow_msg_t message;
  cnet_bench_completion_t *completions;
  cnet_bench_run_state_t run;
  bool flow_started;
} cnet_bench_fixture_t;

typedef struct cnet_bench_run_result_s {
  double throughput_msg_s;
  double cpu_wall_ratio;
  double shutdown_us;
  uint64_t p50_ns;
  uint64_t p95_ns;
  uint64_t p99_ns;
  size_t peak_active_requests;
  size_t retained_payload_bytes_max;
  size_t saturation_rejected;
  size_t saturation_recovered;
  size_t pre_shutdown_active_requests;
} cnet_bench_run_result_t;

static const char *cnet_bench_os_name(void) {
#if defined(_WIN32)
  return "windows";
#elif defined(__linux__)
  return "linux";
#elif defined(__APPLE__)
  return "macos";
#else
  #error "bench_cnet_adapter does not support this operating system"
#endif
}

static const char *cnet_bench_compiler_name(void) {
#if defined(_MSC_VER)
  return "msvc";
#elif defined(__clang__)
  return "clang";
#elif defined(__GNUC__)
  return "gcc";
#else
  #error "bench_cnet_adapter does not support this compiler"
#endif
}

static unsigned long cnet_bench_compiler_version(void) {
#if defined(_MSC_VER)
  return (unsigned long)_MSC_VER;
#elif defined(__clang__)
  return (unsigned long)(__clang_major__ * 10000u + __clang_minor__ * 100u + __clang_patchlevel__);
#elif defined(__GNUC__)
  return (unsigned long)(__GNUC__ * 10000u + __GNUC_MINOR__ * 100u + __GNUC_PATCHLEVEL__);
#endif
}

static int cnet_bench_process_cpu_ns(uint64_t *out) {
  if (!out) return SALTS_EINVAL;
#if defined(_WIN32)
  FILETIME created;
  FILETIME exited;
  FILETIME kernel;
  FILETIME user;
  ULARGE_INTEGER kernel_time;
  ULARGE_INTEGER user_time;
  if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user)) return SALTS_EIO;
  kernel_time.LowPart = kernel.dwLowDateTime;
  kernel_time.HighPart = kernel.dwHighDateTime;
  user_time.LowPart = user.dwLowDateTime;
  user_time.HighPart = user.dwHighDateTime;
  *out = (kernel_time.QuadPart + user_time.QuadPart) * UINT64_C(100);
#else
  struct timespec value;
  if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &value) != 0) return SALTS_EIO;
  *out = (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
#endif
  return SALTS_OK;
}

static int cnet_bench_git_query(const char *arguments, char *output, size_t output_capacity,
                                bool *has_output) {
  char command[CNET_BENCH_GIT_COMMAND_BYTES];
  char chunk[CNET_BENCH_GIT_OUTPUT_BYTES];
  FILE *pipe;
  size_t used = 0u;
  bool overflow = false;
  int written;
  int close_status;

  if (!arguments || !has_output || (output && output_capacity == 0u)) return SALTS_EINVAL;
  *has_output = false;
  if (output) output[0] = '\0';
#if defined(_WIN32)
  written = snprintf(command, sizeof(command), "\"\"%s\" -C \"%s\" %s 2>&1\"",
                     TF_CNET_BENCH_GIT_EXECUTABLE, TF_CNET_BENCH_SOURCE_DIR, arguments);
#else
  written = snprintf(command, sizeof(command), "\"%s\" -C \"%s\" %s 2>&1",
                     TF_CNET_BENCH_GIT_EXECUTABLE, TF_CNET_BENCH_SOURCE_DIR, arguments);
#endif
  if (written < 0 || (size_t)written >= sizeof(command)) return SALTS_ERANGE;
  pipe = cnet_bench_popen(command, "r");
  if (!pipe) return SALTS_EIO;
  for (;;) {
    const size_t count = fread(chunk, 1u, sizeof(chunk), pipe);
    if (count == 0u) break;
    *has_output = true;
    if (output) {
      const size_t available = output_capacity - 1u - used;
      const size_t copied = count < available ? count : available;
      if (copied != 0u) {
        memcpy(output + used, chunk, copied);
        used += copied;
      }
      if (copied != count) overflow = true;
    }
  }
  if (output) output[used] = '\0';
  if (ferror(pipe)) {
    (void)cnet_bench_pclose(pipe);
    return SALTS_EIO;
  }
  close_status = cnet_bench_pclose(pipe);
  if (close_status != 0) {
    fprintf(stderr,
            "CNET_BENCH_ERROR stage=git_query close_status=%d command=%s output=%s\n",
            close_status, command, output ? output : "<not-captured>");
    return SALTS_EIO;
  }
  return overflow ? SALTS_ERANGE : SALTS_OK;
}

static int cnet_bench_validate_live_provenance(void) {
  char actual_commit[CNET_BENCH_GIT_OUTPUT_BYTES];
  bool has_output = false;
  bool source_dirty = false;
  size_t length;
  int status = cnet_bench_git_query("rev-parse --verify HEAD", actual_commit,
                                    sizeof(actual_commit), &has_output);
  if (status != SALTS_OK || !has_output) return status != SALTS_OK ? status : SALTS_EPROTO;
  length = strlen(actual_commit);
  while (length != 0u &&
         (actual_commit[length - 1u] == '\r' || actual_commit[length - 1u] == '\n'))
    actual_commit[--length] = '\0';
  status = cnet_bench_git_query("status --porcelain=v1 --untracked-files=normal -- .", NULL, 0u,
                                &source_dirty);
  if (status != SALTS_OK) return status;
  status =
      tf_cnet_benchmark_validate_provenance(TF_CNET_BENCH_GIT_COMMIT, actual_commit, source_dirty);
  if (status != SALTS_OK) {
    printf("CNET_BENCH_ERROR stage=provenance status=%d expected_commit=%s actual_commit=%s "
           "source_dirty=%d action=reconfigure_and_rebuild_clean_tree\n",
           status, TF_CNET_BENCH_GIT_COMMIT, actual_commit, source_dirty ? 1 : 0);
  }
  return status;
}

static native_io_backend_kind cnet_bench_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#elif defined(__APPLE__)
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static cnet_datagram_peer cnet_bench_peer_address(uint16_t port) {
  cnet_datagram_peer peer = {0};
  peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  peer.port = port;
  peer.address[0] = 127u;
  peer.address[3] = 1u;
  return peer;
}

static cnet_packet_endpoint_config cnet_bench_endpoint_config(void) {
  cnet_packet_endpoint_config config = CNET_PACKET_ENDPOINT_CONFIG_INIT;
  config.protocol = CNET_PACKET_UDP;
  config.session_capacity = 2u;
  config.datagram.backend = cnet_bench_backend();
  config.datagram.host = "127.0.0.1";
  config.datagram.port = 0u;
  config.datagram.send_capacity = CNET_BENCH_QUEUE_CAPACITY;
  config.datagram.request_capacity = CNET_BENCH_QUEUE_CAPACITY + 1u;
  config.datagram.completion_batch_capacity = CNET_BENCH_CAPACITY;
  config.datagram.max_datagram_bytes = CNET_BENCH_PAYLOAD_BYTES;
  config.datagram.receive_buffer_bytes = CNET_BENCH_PAYLOAD_BYTES;
  return config;
}

static int cnet_bench_peer_admit(void *user, cnet_packet_endpoint *endpoint,
                                 cnet_packet_protocol protocol, const cnet_datagram_peer *peer,
                                 uint32_t conversation) {
  (void)user;
  (void)endpoint;
  (void)protocol;
  (void)peer;
  (void)conversation;
  return SALTS_OK;
}

static void cnet_bench_peer_state(void *user, cnet_packet_endpoint *endpoint,
                                  cnet_packet_session session, cnet_packet_session_state state,
                                  const cnet_datagram_peer *peer, uint32_t conversation) {
  (void)user;
  (void)endpoint;
  (void)session;
  (void)state;
  (void)peer;
  (void)conversation;
}

static void cnet_bench_peer_receive(void *user, cnet_packet_endpoint *endpoint,
                                    cnet_packet_session session, const cnet_receive_view *view) {
  cnet_bench_peer_t *peer = (cnet_bench_peer_t *)user;
  (void)endpoint;
  (void)session;
  if (!peer || !view || view->size != CNET_BENCH_PAYLOAD_BYTES) {
    if (peer) ++peer->invalid_receives;
    return;
  }
  ++peer->receives;
}

static void cnet_bench_peer_error(void *user, cnet_packet_endpoint *endpoint,
                                  cnet_packet_session session, int status) {
  cnet_bench_peer_t *peer = (cnet_bench_peer_t *)user;
  (void)endpoint;
  (void)session;
  if (peer && peer->status == SALTS_OK) peer->status = status;
}

static void cnet_bench_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  cnet_bench_completion_t *completion = (cnet_bench_completion_t *)ctx;
  cnet_bench_run_state_t *run;
  uint64_t completed_at;
  size_t previous;
  if (!completion || !completion->run) return;
  run = completion->run;
  completed_at = salts_hrtime();
  completion->latency_ns = completed_at - completion->started_ns;
  completion->status = result ? result->status : SALTS_EPROTO;
  atomic_store_explicit(&completion->done, true, memory_order_release);
  previous = atomic_fetch_add_explicit(&run->completed, 1u, memory_order_acq_rel);
  if (previous + 1u == run->expected) {
    run->terminal_wall_ns = completed_at;
    run->terminal_cpu_status =
        run->record_metrics ? cnet_bench_process_cpu_ns(&run->terminal_cpu_ns) : SALTS_OK;
    atomic_store_explicit(&run->terminal_captured, true, memory_order_release);
  }
}

static int cnet_bench_progress(cnet_bench_fixture_t *fixture, size_t *peak_active_requests) {
  turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  int status;
  if (!fixture || !fixture->sink || !fixture->peer.initialized) return SALTS_EINVAL;
  status = turbo_flow_cnet_packet_sink_poll(fixture->sink, 0u, &snapshot);
  if (status != SALTS_OK) return status;
  if (peak_active_requests && snapshot.active_requests > *peak_active_requests)
    *peak_active_requests = snapshot.active_requests;
  for (size_t poll = 0u; poll < CNET_BENCH_CAPACITY; ++poll) {
    size_t events = 0u;
    status = cnet_packet_poll(&fixture->peer.endpoint, 0u, &events);
    if (status != SALTS_OK) return status;
    if (events == 0u) break;
  }
  return fixture->peer.status;
}

static int cnet_bench_fixture_cleanup(cnet_bench_fixture_t *fixture) {
  int first_status = SALTS_OK;
  int status;
  if (!fixture) return SALTS_EINVAL;
  if (fixture->flow) {
    if (fixture->flow_started) {
      status = turbo_flow_stop(fixture->flow);
      if (first_status == SALTS_OK && status != SALTS_OK) first_status = status;
    }
    turbo_flow_destroy(fixture->flow);
    fixture->flow = NULL;
    fixture->flow_started = false;
  }
  if (fixture->sink) {
    status = turbo_flow_cnet_packet_sink_destroy(fixture->sink);
    if (first_status == SALTS_OK && status != SALTS_OK) first_status = status;
    fixture->sink = NULL;
  }
  turbo_flow_msg_cleanup(&fixture->message);
  if (fixture->peer.initialized) {
    status = cnet_packet_endpoint_stop(&fixture->peer.endpoint, CNET_BENCH_TIMEOUT_MS);
    if (first_status == SALTS_OK && status != SALTS_OK) first_status = status;
    status = cnet_packet_endpoint_destroy(&fixture->peer.endpoint);
    if (first_status == SALTS_OK && status != SALTS_OK) first_status = status;
    fixture->peer.initialized = false;
  }
  free(fixture->completions);
  fixture->completions = NULL;
  return first_status;
}

static int cnet_bench_fixture_init(cnet_bench_fixture_t *fixture) {
  static const char graph[] = "source input\n"
                              "stage output adapter cnet.packet.out\n"
                              "stage main {\n"
                              "  input -> output\n"
                              "}\n";
  cnet_packet_endpoint_config peer_config = cnet_bench_endpoint_config();
  cnet_packet_endpoint_config sink_endpoint = cnet_bench_endpoint_config();
  turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_config_t sink_config = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  cnet_datagram_peer sink_peer;
  uint16_t peer_port = 0u;
  uint64_t deadline;
  int status;

  if (!fixture) return SALTS_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  turbo_flow_msg_init(&fixture->message);
  fixture->peer.status = SALTS_OK;
  fixture->completions = (cnet_bench_completion_t *)calloc(
      CNET_BENCH_SAMPLE_MESSAGES, sizeof(*fixture->completions));
  if (!fixture->completions) return SALTS_ENOMEM;

  peer_config.observer.on_admit = cnet_bench_peer_admit;
  peer_config.observer.on_state = cnet_bench_peer_state;
  peer_config.observer.on_receive = cnet_bench_peer_receive;
  peer_config.observer.on_error = cnet_bench_peer_error;
  peer_config.observer.user = &fixture->peer;
  status = cnet_packet_endpoint_init(&fixture->peer.endpoint, &peer_config);
  if (status != SALTS_OK) goto fail;
  fixture->peer.initialized = true;
  status = cnet_packet_endpoint_port(&fixture->peer.endpoint, &peer_port);
  if (status != SALTS_OK) goto fail;

  fixture->message.buffer = mem_get_buffer(mem_global(), CNET_BENCH_PAYLOAD_BYTES);
  if (!fixture->message.buffer) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  memset(mem_buffer_data(fixture->message.buffer), 0x5a, CNET_BENCH_PAYLOAD_BYTES);
  mem_set_used(fixture->message.buffer, CNET_BENCH_PAYLOAD_BYTES);
  fixture->message.payload =
      vstr_from_buf(mem_buffer_const_data(fixture->message.buffer), CNET_BENCH_PAYLOAD_BYTES);

  fixture->flow = turbo_flow_create();
  if (!fixture->flow) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  ingress.workers = 1u;
  ingress.queue_capacity = CNET_BENCH_QUEUE_CAPACITY;
  ingress.max_message_bytes = CNET_BENCH_PAYLOAD_BYTES;
  ingress.max_inflight_bytes = CNET_BENCH_QUEUE_CAPACITY * CNET_BENCH_PAYLOAD_BYTES;
  status = turbo_flow_configure_async_ingress(fixture->flow, &ingress);
  if (status != SALTS_OK) goto fail;

  sink_peer = cnet_bench_peer_address(peer_port);
  sink_config.flow = fixture->flow;
  sink_config.adapter_name = "cnet.packet.out";
  sink_config.endpoint = &sink_endpoint;
  sink_config.peer = sink_peer;
  sink_config.conversation = 0u;
  sink_config.send_capacity = CNET_BENCH_CAPACITY;
  sink_config.max_message_bytes = CNET_BENCH_PAYLOAD_BYTES;
  sink_config.actor_command_capacity = CNET_BENCH_CAPACITY;
  sink_config.actor_max_steps_per_poll = CNET_BENCH_CAPACITY;
  sink_config.stop_timeout_ms = CNET_BENCH_TIMEOUT_MS;
  status = turbo_flow_cnet_packet_sink_register(&sink_config, &fixture->sink);
  if (status != SALTS_OK) goto fail;
  status = turbo_flow_parse_string(fixture->flow, graph, sizeof(graph) - 1u);
  if (status != SALTS_OK) goto fail;
  status = turbo_flow_compile(fixture->flow);
  if (status != SALTS_OK) goto fail;
  status = turbo_flow_start(fixture->flow);
  if (status != SALTS_OK) goto fail;
  fixture->flow_started = true;

  deadline = salts_monotonic_ms() + CNET_BENCH_TIMEOUT_MS;
  do {
    status = cnet_bench_progress(fixture, NULL);
    if (status != SALTS_OK) goto fail;
    status = turbo_flow_cnet_packet_sink_snapshot(fixture->sink, &snapshot);
    if (status != SALTS_OK) goto fail;
  } while (!snapshot.session_open && salts_monotonic_ms() < deadline);
  if (!snapshot.session_open) {
    status = SALTS_ETIMEDOUT;
    goto fail;
  }
  return SALTS_OK;

fail:
  {
    const int cleanup_status = cnet_bench_fixture_cleanup(fixture);
    if (cleanup_status != SALTS_OK)
      printf("CNET_BENCH_DIAG stage=fixture_init_cleanup status=%d\n", cleanup_status);
  }
  return status;
}

static void cnet_bench_prepare_completions(cnet_bench_fixture_t *fixture, size_t count,
                                           bool record_metrics) {
  cnet_bench_run_state_t *run = &fixture->run;
  atomic_init(&run->completed, 0u);
  atomic_init(&run->terminal_captured, false);
  run->expected = count;
  run->terminal_wall_ns = 0u;
  run->terminal_cpu_ns = 0u;
  run->terminal_cpu_status = SALTS_EALREADY;
  run->record_metrics = record_metrics;
  for (size_t index = 0u; index < count; ++index) {
    fixture->completions[index].run = run;
    fixture->completions[index].started_ns = 0u;
    fixture->completions[index].latency_ns = 0u;
    fixture->completions[index].status = SALTS_EALREADY;
    atomic_init(&fixture->completions[index].done, false);
  }
}

static int cnet_bench_wait_for_receives(cnet_bench_fixture_t *fixture, size_t target,
                                        size_t *peak_active_requests) {
  const uint64_t deadline = salts_monotonic_ms() + CNET_BENCH_TIMEOUT_MS;
  int status;
  while (fixture->peer.receives < target && salts_monotonic_ms() < deadline) {
    status = cnet_bench_progress(fixture, peak_active_requests);
    if (status != SALTS_OK) return status;
  }
  if (fixture->peer.invalid_receives != 0u) return SALTS_EPROTO;
  if (fixture->peer.receives == target) return SALTS_OK;
  printf("CNET_BENCH_DIAG stage=wait_for_receives target=%zu receives=%zu invalid_receives=%zu "
         "peer_status=%d\n",
         target, fixture->peer.receives, fixture->peer.invalid_receives, fixture->peer.status);
  return SALTS_ETIMEDOUT;
}

static int cnet_bench_run_batch(cnet_bench_fixture_t *fixture, size_t count,
                                cnet_bench_run_result_t *result, bool record_metrics) {
  cnet_bench_run_state_t *run;
  size_t receives_before;
  size_t submitted = 0u;
  size_t completed;
  uint64_t wall_start = 0u;
  uint64_t wall_elapsed;
  uint64_t cpu_start = 0u;
  uint64_t cpu_elapsed = 0u;
  uint64_t deadline;
  uint64_t *latencies = NULL;
  int status;

  if (!fixture || count == 0u || count > CNET_BENCH_SAMPLE_MESSAGES ||
      (record_metrics && !result))
    return SALTS_EINVAL;
  receives_before = fixture->peer.receives;
  run = &fixture->run;
  cnet_bench_prepare_completions(fixture, count, record_metrics);
  if (record_metrics) {
    status = cnet_bench_process_cpu_ns(&cpu_start);
    if (status != SALTS_OK) return status;
    wall_start = salts_hrtime();
  }
  deadline = salts_monotonic_ms() + CNET_BENCH_TIMEOUT_MS;
  while (!atomic_load_explicit(&run->terminal_captured, memory_order_acquire) &&
         salts_monotonic_ms() < deadline) {
    completed = atomic_load_explicit(&run->completed, memory_order_acquire);
    while (submitted < count && submitted - completed < CNET_BENCH_CAPACITY) {
      cnet_bench_completion_t *completion = &fixture->completions[submitted];
      fixture->message.id = submitted;
      completion->started_ns = salts_hrtime();
      status = turbo_flow_publish_async(fixture->flow, "input", &fixture->message,
                                        cnet_bench_complete, completion);
      if (status != SALTS_OK) return status;
      ++submitted;
    }
    status = cnet_bench_progress(fixture, result ? &result->peak_active_requests : NULL);
    if (status != SALTS_OK) return status;
  }
  if (!atomic_load_explicit(&run->terminal_captured, memory_order_acquire) ||
      atomic_load_explicit(&run->completed, memory_order_acquire) != count) {
    turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
    int snapshot_status = turbo_flow_cnet_packet_sink_snapshot(fixture->sink, &snapshot);
    printf("CNET_BENCH_DIAG stage=wait_for_terminals submitted=%zu completed=%zu expected=%zu "
           "receives=%zu snapshot_status=%d active_requests=%zu messages_sent=%" PRIu64 "\n",
           submitted, atomic_load_explicit(&run->completed, memory_order_acquire), count,
           fixture->peer.receives, snapshot_status, snapshot.active_requests,
           snapshot.messages_sent);
    return SALTS_ETIMEDOUT;
  }
  if (record_metrics) {
    if (run->terminal_cpu_status != SALTS_OK) return run->terminal_cpu_status;
    if (run->terminal_wall_ns < wall_start || run->terminal_cpu_ns < cpu_start)
      return SALTS_ERANGE;
    wall_elapsed = run->terminal_wall_ns - wall_start;
    cpu_elapsed = run->terminal_cpu_ns - cpu_start;
  } else {
    wall_elapsed = 0u;
  }
  for (size_t index = 0u; index < count; ++index) {
    if (!atomic_load_explicit(&fixture->completions[index].done, memory_order_acquire) ||
        fixture->completions[index].status != SALTS_OK)
      return fixture->completions[index].status != SALTS_OK
                 ? fixture->completions[index].status
                 : SALTS_EPROTO;
  }
  status = cnet_bench_wait_for_receives(fixture, receives_before + count,
                                        result ? &result->peak_active_requests : NULL);
  if (status != SALTS_OK) return status;
  if (!record_metrics) return SALTS_OK;
  if (wall_elapsed == 0u) return SALTS_ERANGE;

  latencies = (uint64_t *)malloc(count * sizeof(*latencies));
  if (!latencies) return SALTS_ENOMEM;
  for (size_t index = 0u; index < count; ++index)
    latencies[index] = fixture->completions[index].latency_ns;
  result->throughput_msg_s = (double)count * 1000000000.0 / (double)wall_elapsed;
  result->cpu_wall_ratio = (double)cpu_elapsed / (double)wall_elapsed;
  result->retained_payload_bytes_max = result->peak_active_requests * CNET_BENCH_PAYLOAD_BYTES;
  status = tf_cnet_benchmark_percentile_u64(latencies, count, 50u, &result->p50_ns);
  if (status == SALTS_OK)
    status = tf_cnet_benchmark_percentile_u64(latencies, count, 95u, &result->p95_ns);
  if (status == SALTS_OK)
    status = tf_cnet_benchmark_percentile_u64(latencies, count, 99u, &result->p99_ns);
  free(latencies);
  return status;
}

static int cnet_bench_saturation_recovery(cnet_bench_fixture_t *fixture,
                                          cnet_bench_run_result_t *result) {
  enum { SATURATION_MESSAGES = CNET_BENCH_CAPACITY + 1 };
  cnet_bench_run_state_t *run;
  size_t receives_before;
  uint64_t deadline;
  size_t rejected = 0u;
  size_t succeeded = 0u;
  int status;

  if (!fixture || !result) return SALTS_EINVAL;
  run = &fixture->run;
  receives_before = fixture->peer.receives;
  cnet_bench_prepare_completions(fixture, SATURATION_MESSAGES, false);
  for (size_t index = 0u; index < SATURATION_MESSAGES; ++index) {
    fixture->message.id = index;
    fixture->completions[index].started_ns = salts_hrtime();
    status = turbo_flow_publish_async(fixture->flow, "input", &fixture->message,
                                      cnet_bench_complete, &fixture->completions[index]);
    if (status != SALTS_OK) return status;
  }
  deadline = salts_monotonic_ms() + CNET_BENCH_TIMEOUT_MS;
  while (atomic_load_explicit(&run->completed, memory_order_acquire) == 0u &&
         salts_monotonic_ms() < deadline)
    salts_thread_yield();
  if (atomic_load_explicit(&run->completed, memory_order_acquire) == 0u)
    return SALTS_ETIMEDOUT;

  while (atomic_load_explicit(&run->completed, memory_order_acquire) < SATURATION_MESSAGES &&
         salts_monotonic_ms() < deadline) {
    status = cnet_bench_progress(fixture, &result->peak_active_requests);
    if (status != SALTS_OK) return status;
  }
  if (atomic_load_explicit(&run->completed, memory_order_acquire) != SATURATION_MESSAGES)
    return SALTS_ETIMEDOUT;
  for (size_t index = 0u; index < SATURATION_MESSAGES; ++index) {
    if (!atomic_load_explicit(&fixture->completions[index].done, memory_order_acquire))
      return SALTS_EPROTO;
    if (fixture->completions[index].status == SALTS_ENOSPC)
      ++rejected;
    else if (fixture->completions[index].status == SALTS_OK)
      ++succeeded;
    else
      return fixture->completions[index].status;
  }
  if (rejected == 0u || succeeded == 0u) return SALTS_EPROTO;
  status = cnet_bench_wait_for_receives(fixture, receives_before + succeeded,
                                        &result->peak_active_requests);
  if (status != SALTS_OK) return status;

  cnet_bench_prepare_completions(fixture, 1u, false);
  fixture->message.id = UINT64_MAX;
  fixture->completions[0].started_ns = salts_hrtime();
  status = turbo_flow_publish_async(fixture->flow, "input", &fixture->message,
                                    cnet_bench_complete, &fixture->completions[0]);
  if (status != SALTS_OK) return status;
  deadline = salts_monotonic_ms() + CNET_BENCH_TIMEOUT_MS;
  while (atomic_load_explicit(&run->completed, memory_order_acquire) == 0u &&
         salts_monotonic_ms() < deadline) {
    status = cnet_bench_progress(fixture, &result->peak_active_requests);
    if (status != SALTS_OK) return status;
  }
  if (atomic_load_explicit(&run->completed, memory_order_acquire) != 1u ||
      !atomic_load_explicit(&fixture->completions[0].done, memory_order_acquire))
    return SALTS_ETIMEDOUT;
  if (fixture->completions[0].status != SALTS_OK) return fixture->completions[0].status;
  status = cnet_bench_wait_for_receives(fixture, receives_before + succeeded + 1u,
                                        &result->peak_active_requests);
  if (status != SALTS_OK) return status;
  result->saturation_rejected = rejected;
  result->saturation_recovered = 1u;
  result->retained_payload_bytes_max = result->peak_active_requests * CNET_BENCH_PAYLOAD_BYTES;
  return SALTS_OK;
}

static int cnet_bench_shutdown(cnet_bench_fixture_t *fixture, cnet_bench_run_result_t *result) {
  turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  uint64_t started;
  int status;
  if (!fixture || !fixture->flow || !fixture->sink || !fixture->peer.initialized || !result)
    return SALTS_EINVAL;
  status = turbo_flow_cnet_packet_sink_snapshot(fixture->sink, &snapshot);
  if (status != SALTS_OK) return status;
  result->pre_shutdown_active_requests = snapshot.active_requests;
  if (result->pre_shutdown_active_requests != 0u) return SALTS_EBUSY;
  started = salts_hrtime();
  status = turbo_flow_stop(fixture->flow);
  if (status != SALTS_OK) return status;
  fixture->flow_started = false;
  turbo_flow_destroy(fixture->flow);
  fixture->flow = NULL;
  status = turbo_flow_cnet_packet_sink_destroy(fixture->sink);
  if (status != SALTS_OK) return status;
  fixture->sink = NULL;
  status = cnet_packet_endpoint_stop(&fixture->peer.endpoint, CNET_BENCH_TIMEOUT_MS);
  if (status != SALTS_OK) return status;
  status = cnet_packet_endpoint_destroy(&fixture->peer.endpoint);
  if (status != SALTS_OK) return status;
  fixture->peer.initialized = false;
  result->shutdown_us = (double)(salts_hrtime() - started) / 1000.0;
  if (result->shutdown_us <= 0.0) return SALTS_ERANGE;
  turbo_flow_msg_cleanup(&fixture->message);
  free(fixture->completions);
  fixture->completions = NULL;
  return SALTS_OK;
}

static int cnet_bench_print_summary(const char *metric, const double *values, size_t count,
                                    const char *unit) {
  tf_cnet_benchmark_summary_t summary = {0};
  int status = tf_cnet_benchmark_summarize(values, count, &summary);
  if (status != SALTS_OK) return status;
  printf("CNET_BENCH_SUMMARY metric=%s median=%.3f mad=%.3f unit=%s\n", metric,
         summary.median, summary.mad, unit);
  return SALTS_OK;
}

static int cnet_adapter_benchmark_run(void) {
  cnet_bench_run_result_t results[CNET_BENCH_REPLICATES] = {0};
  double throughput[CNET_BENCH_REPLICATES];
  double p50[CNET_BENCH_REPLICATES];
  double p95[CNET_BENCH_REPLICATES];
  double p99[CNET_BENCH_REPLICATES];
  double cpu_ratio[CNET_BENCH_REPLICATES];
  double shutdown_us[CNET_BENCH_REPLICATES];
  int logical_cpus = salts_cpu_count();
  int status = SALTS_OK;
  const bool baseline_eligible =
      strcmp(TF_CNET_BENCH_PRESET, "win-release-user") == 0 &&
      strcmp(TF_CNET_BENCH_BUILD_TYPE, "Release") == 0 && TF_CNET_BENCH_ASAN == 0;

  if (logical_cpus <= 0) return SALTS_EIO;
  status = cnet_bench_validate_live_provenance();
  if (status != SALTS_OK) return status;
  printf("CNET_BENCH_ENV version=1 scenario=udp_packet_terminal os=%s cpu_model=%s "
         "logical_cpus=%d "
         "compiler=%s compiler_version=%lu cmake_build_type=%s actual_preset=%s "
         "required_baseline_preset=win-release-user asan=%d baseline_eligible=%d "
         "source_commit=%s source_dirty=0 payload_bytes=%u warmup_messages=%u "
         "sample_messages=%u replicates=%u ingress_workers=1 ingress_capacity=%u "
         "send_capacity=%u actor_capacity=%u allocation_events_per_message=%u "
         "allocation_scope=turbo_flow_owner_objects\n",
         cnet_bench_os_name(), TF_CNET_BENCH_CPU_MODEL, logical_cpus, cnet_bench_compiler_name(),
         cnet_bench_compiler_version(), TF_CNET_BENCH_BUILD_TYPE, TF_CNET_BENCH_PRESET,
         TF_CNET_BENCH_ASAN, baseline_eligible ? 1 : 0, TF_CNET_BENCH_GIT_COMMIT,
         CNET_BENCH_PAYLOAD_BYTES, CNET_BENCH_WARMUP_MESSAGES, CNET_BENCH_SAMPLE_MESSAGES,
         CNET_BENCH_REPLICATES, CNET_BENCH_QUEUE_CAPACITY, CNET_BENCH_CAPACITY,
         CNET_BENCH_CAPACITY, CNET_BENCH_OWNER_ALLOCATIONS_PER_MESSAGE);
  printf("CNET_BENCH_AUDIT source_commit=%s "
         "allocation_1=turbo_flow/src/flow_async_ingress.c:227 "
         "allocation_2=turbo_flow/src/flow_async_terminal.c:95 "
         "allocation_3=turbo_flow/src/flow_async_terminal.c:322\n",
         TF_CNET_BENCH_GIT_COMMIT);

  for (size_t replicate = 0u; replicate < CNET_BENCH_REPLICATES; ++replicate) {
    cnet_bench_fixture_t fixture;
    status = cnet_bench_fixture_init(&fixture);
    if (status != SALTS_OK) {
      printf("CNET_BENCH_ERROR replicate=%zu stage=fixture_init status=%d\n", replicate + 1u,
             status);
      return status;
    }
    status = cnet_bench_run_batch(&fixture, CNET_BENCH_WARMUP_MESSAGES, NULL, false);
    if (status != SALTS_OK) {
      printf("CNET_BENCH_ERROR replicate=%zu stage=warmup status=%d\n", replicate + 1u, status);
      {
        const int cleanup_status = cnet_bench_fixture_cleanup(&fixture);
        if (cleanup_status != SALTS_OK)
          printf("CNET_BENCH_DIAG replicate=%zu stage=warmup_cleanup status=%d\n",
                 replicate + 1u, cleanup_status);
      }
      return status;
    }
    benchmark_io("scenario=udp_packet_terminal measured batch", 1u,
                 CNET_BENCH_SAMPLE_MESSAGES,
                 CNET_BENCH_SAMPLE_MESSAGES * CNET_BENCH_PAYLOAD_BYTES) {
      status = cnet_bench_run_batch(&fixture, CNET_BENCH_SAMPLE_MESSAGES, &results[replicate], true);
    }
    if (status == SALTS_OK) status = cnet_bench_saturation_recovery(&fixture, &results[replicate]);
    if (status == SALTS_OK) status = cnet_bench_shutdown(&fixture, &results[replicate]);
    if (status != SALTS_OK) {
      printf("CNET_BENCH_ERROR replicate=%zu stage=measured status=%d\n", replicate + 1u, status);
      {
        const int cleanup_status = cnet_bench_fixture_cleanup(&fixture);
        if (cleanup_status != SALTS_OK)
          printf("CNET_BENCH_DIAG replicate=%zu stage=measured_cleanup status=%d\n",
                 replicate + 1u, cleanup_status);
      }
      return status;
    }
    throughput[replicate] = results[replicate].throughput_msg_s;
    p50[replicate] = (double)results[replicate].p50_ns;
    p95[replicate] = (double)results[replicate].p95_ns;
    p99[replicate] = (double)results[replicate].p99_ns;
    cpu_ratio[replicate] = results[replicate].cpu_wall_ratio;
    shutdown_us[replicate] = results[replicate].shutdown_us;
    printf("CNET_BENCH_RUN replicate=%zu throughput_msg_s=%.3f p50_ns=%" PRIu64
           " p95_ns=%" PRIu64 " p99_ns=%" PRIu64
           " cpu_wall_ratio=%.6f peak_active_requests=%zu retained_payload_bytes_max=%zu "
           "saturation_rejected=%zu saturation_recovered=%zu "
           "pre_shutdown_active_requests=%zu shutdown_us=%.3f\n",
           replicate + 1u, results[replicate].throughput_msg_s, results[replicate].p50_ns,
           results[replicate].p95_ns, results[replicate].p99_ns,
           results[replicate].cpu_wall_ratio, results[replicate].peak_active_requests,
           results[replicate].retained_payload_bytes_max, results[replicate].saturation_rejected,
           results[replicate].saturation_recovered,
           results[replicate].pre_shutdown_active_requests, results[replicate].shutdown_us);
  }

  status = cnet_bench_print_summary("throughput_msg_s", throughput, CNET_BENCH_REPLICATES,
                                    "messages_per_second");
  if (status == SALTS_OK)
    status = cnet_bench_print_summary("p50_ns", p50, CNET_BENCH_REPLICATES, "nanoseconds");
  if (status == SALTS_OK)
    status = cnet_bench_print_summary("p95_ns", p95, CNET_BENCH_REPLICATES, "nanoseconds");
  if (status == SALTS_OK)
    status = cnet_bench_print_summary("p99_ns", p99, CNET_BENCH_REPLICATES, "nanoseconds");
  if (status == SALTS_OK)
    status = cnet_bench_print_summary("cpu_wall_ratio", cpu_ratio, CNET_BENCH_REPLICATES, "ratio");
  if (status == SALTS_OK)
    status = cnet_bench_print_summary("shutdown_us", shutdown_us, CNET_BENCH_REPLICATES,
                                      "microseconds");
  return status;
}

spec("TurboFlow CNet adapter benchmark") {
  bench("raw UDP packet terminal") {
    check_equal(cnet_adapter_benchmark_run(), 0);
  }
}
