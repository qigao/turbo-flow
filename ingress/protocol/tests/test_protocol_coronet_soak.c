#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_process.h"
#include "turbo_thread.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(PROTOCOL_SOAK_HAS_ASAN)
  #include <sanitizer/allocator_interface.h>
#endif

#if defined(_WIN32)
  #include <psapi.h>
  #include <tlhelp32.h>
  #include <windows.h>
#elif defined(__linux__)
  #include <dirent.h>
  #include <unistd.h>
#endif

#ifndef PROTOCOL_CORONET_SOAK_TEST
  #error "PROTOCOL_CORONET_SOAK_TEST must point to test_protocol_coronet"
#endif

#define PROTOCOL_SOAK_DEFAULT_ITERATIONS 5u
#define PROTOCOL_SOAK_MAX_ITERATIONS 1000000u
#define PROTOCOL_SOAK_CHILD_TIMEOUT_MS 120000u
#define PROTOCOL_SOAK_PROCESS_RELEASE_TIMEOUT_MS 1000u
#define PROTOCOL_SOAK_MAX_DURATION_MS (8u * 60u * 60u * 1000u)
#define PROTOCOL_SOAK_LATENCY_SAMPLES 4096u
#define PROTOCOL_SOAK_DEFAULT_RSS_TOLERANCE (8u * 1024u * 1024u)
#define PROTOCOL_SOAK_MAX_RSS_TOLERANCE (1024u * 1024u * 1024u)
#define PROTOCOL_SOAK_DEFAULT_SEED UINT64_C(0x4741544557415953)

typedef struct protocol_soak_resources_s {
  size_t rss_bytes;
  size_t handles;
  size_t threads;
} protocol_soak_resources_t;

typedef struct protocol_soak_trace_s {
  const char *name;
  const char *filter;
} protocol_soak_trace_t;

static const protocol_soak_trace_t protocol_soak_traces[] = {
    {"udp-single-datagram-shutdown",
     "maps one CoAP datagram on a bounded UDP session"},
    {"tcp-settlement-shutdown",
     "keeps the event loop live until a pending TCP publication settles"},
    {"tls-reconnect", "reconnects TLS clients without retaining session resources"},
    {"tls-startup-recovery", "releases a failed TLS startup before same-port recovery"},
    {"wss-mtls-rejection", "requires a verified client certificate before WSS identity admission"},
    {"wss-mtls-abort-cleanup",
     "releases WSS plugin ownership when the identity trace aborts after open"},
    {"wss-mtls-reconnect",
     "reconnects authenticated WSS clients without retaining session resources"},
};

static int protocol_soak_compare_u64(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return (a > b) - (a < b);
}

static uint64_t protocol_soak_percentile(uint64_t *values, size_t count, size_t percent) {
  size_t index;
  if (!values || count == 0u) return 0u;
  qsort(values, count, sizeof(*values), protocol_soak_compare_u64);
  index = ((count - 1u) * percent) / 100u;
  return values[index];
}

static uint64_t protocol_soak_random(uint64_t *state) {
  uint64_t value = *state;
  value ^= value << 13u;
  value ^= value >> 7u;
  value ^= value << 17u;
  *state = value;
  return value;
}

static int protocol_soak_env_u64(const char *name, uint64_t fallback, uint64_t maximum,
                                int allow_zero, uint64_t *out) {
  const char *value;
  char *end = NULL;
  unsigned long long parsed;
  if (!name || !out || maximum == 0u) return TURBO_EINVAL;
  value = getenv(name);
  if (!value || !value[0]) {
    *out = fallback;
    return TURBO_OK;
  }
  errno = 0;
  parsed = strtoull(value, &end, 0);
  if (errno == ERANGE || !end || *end != '\0' || (!allow_zero && parsed == 0u) || parsed > maximum)
    return TURBO_EINVAL;
  *out = (uint64_t)parsed;
  return TURBO_OK;
}

static int protocol_soak_resource_snapshot(protocol_soak_resources_t *out) {
  if (!out) return TURBO_EINVAL;
#if defined(PROTOCOL_SOAK_HAS_ASAN)
  /*
   * Parent RSS is a live-resource gate, not an ASan allocator-cache metric.
   * LeakSanitizer still validates reachability when this process exits.
   */
  __sanitizer_purge_allocator();
#endif
  memset(out, 0, sizeof(*out));
#if defined(_WIN32)
  {
    PROCESS_MEMORY_COUNTERS memory = {0};
    DWORD handles = 0u;
    HANDLE snapshot;
    THREADENTRY32 entry;
    const DWORD process_id = GetCurrentProcessId();
    memory.cb = sizeof(memory);
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &memory, sizeof(memory)) ||
        !GetProcessHandleCount(GetCurrentProcess(), &handles))
      return TURBO_EIO;
    out->rss_bytes = (size_t)memory.WorkingSetSize;
    out->handles = (size_t)handles;
    snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0u);
    if (snapshot == INVALID_HANDLE_VALUE) return TURBO_EIO;
    memset(&entry, 0, sizeof(entry));
    entry.dwSize = sizeof(entry);
    if (Thread32First(snapshot, &entry)) {
      do {
        if (entry.th32OwnerProcessID == process_id) ++out->threads;
      } while (Thread32Next(snapshot, &entry));
    }
    CloseHandle(snapshot);
  }
#elif defined(__linux__)
  {
    FILE *statm = fopen("/proc/self/statm", "r");
    unsigned long rss_pages = 0u;
    DIR *directory;
    struct dirent *entry;
    const long page_size = sysconf(_SC_PAGESIZE);
    if (!statm || fscanf(statm, "%*lu %lu", &rss_pages) != 1 || page_size <= 0) {
      if (statm) fclose(statm);
      return TURBO_EIO;
    }
    fclose(statm);
    out->rss_bytes = (size_t)rss_pages * (size_t)page_size;
    directory = opendir("/proc/self/fd");
    if (!directory) return TURBO_EIO;
    while ((entry = readdir(directory)) != NULL)
      if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) ++out->handles;
    closedir(directory);
    directory = opendir("/proc/self/task");
    if (!directory) return TURBO_EIO;
    while ((entry = readdir(directory)) != NULL)
      if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) ++out->threads;
    closedir(directory);
  }
#else
  return TURBO_ENOTSUP;
#endif
  return TURBO_OK;
}

static int protocol_soak_wait_for_process_release(const protocol_soak_resources_t *baseline,
                                                 protocol_soak_resources_t *current) {
  const uint64_t deadline =
      turbo_monotonic_ms() + (uint64_t)PROTOCOL_SOAK_PROCESS_RELEASE_TIMEOUT_MS;
  int rc;
  if (!baseline || !current) return TURBO_EINVAL;
  do {
    rc = protocol_soak_resource_snapshot(current);
    if (rc != TURBO_OK) return rc;
    if (current->handles <= baseline->handles && current->threads <= baseline->threads)
      return TURBO_OK;
    turbo_sleep_ms(1u);
  } while (turbo_monotonic_ms() < deadline);
  return TURBO_ENOSPC;
}

static void protocol_soak_report_child_stream(turbo_process_t *process, int stdout_stream) {
  char buffer[4096];
  size_t count = 0u;
  int rc;
  if (!process) return;
  do {
    rc = stdout_stream ? turbo_process_read_stdout(process, buffer, sizeof(buffer), &count)
                       : turbo_process_read_stderr(process, buffer, sizeof(buffer), &count);
    if (count != 0u) (void)fwrite(buffer, 1u, count, stderr);
  } while (rc == TURBO_OK && count != 0u);
}

static int protocol_soak_run_child(const protocol_soak_trace_t *trace, int verify_resource_release) {
  const char *args[3];
  turbo_process_options_t options;
  turbo_process_t *process = NULL;
  turbo_process_result_t result = {0};
  protocol_soak_resources_t resources_before;
  protocol_soak_resources_t resources_after;
  int release_rc;
  int rc;
  if (!trace || !trace->name || !trace->filter) return TURBO_EINVAL;
  rc = protocol_soak_resource_snapshot(&resources_before);
  if (rc != TURBO_OK) return rc;
  args[0] = "--filter";
  args[1] = trace->filter;
  args[2] = NULL;
  turbo_process_options_init(&options);
  options.program = PROTOCOL_CORONET_SOAK_TEST;
  options.args = args;
  options.flags = TURBO_PROCESS_CAPTURE_STDOUT | TURBO_PROCESS_CAPTURE_STDERR;
  options.timeout_ms = PROTOCOL_SOAK_CHILD_TIMEOUT_MS;
  options.max_output_bytes = 65536u;
  rc = turbo_process_spawn(&options, &process);
  if (rc != TURBO_OK) {
    fprintf(stderr, "SOAK_CHILD_FAILURE trace=%s spawn_status=%d\n", trace->name, rc);
    return rc;
  }
  rc = turbo_process_wait(process, &result);
  if (rc == TURBO_OK && (result.state != TURBO_PROCESS_EXITED || result.exit_code != 0)) {
    fprintf(stderr,
            "SOAK_CHILD_FAILURE trace=%s state=%s exit_code=%d term_signal=%d "
            "error_code=%d\nstdout:\n",
            trace->name, turbo_process_state_name(result.state), result.exit_code,
            result.term_signal, result.error_code);
    protocol_soak_report_child_stream(process, 1);
    fputs("\nstderr:\n", stderr);
    protocol_soak_report_child_stream(process, 0);
    fputc('\n', stderr);
    rc = TURBO_EIO;
  } else if (rc != TURBO_OK) {
    fprintf(stderr, "SOAK_CHILD_FAILURE trace=%s wait_status=%d\n", trace->name, rc);
  }
  turbo_process_destroy(process);
  if (!verify_resource_release) return rc;
  release_rc = protocol_soak_wait_for_process_release(&resources_before, &resources_after);
  if (release_rc != TURBO_OK) {
    fprintf(stderr,
            "SOAK_CHILD_RESOURCE_FAILURE trace=%s handles_before=%zu handles_current=%zu "
            "threads_before=%zu threads_current=%zu settle_timeout_ms=%u\n",
            trace->name, resources_before.handles, resources_after.handles,
            resources_before.threads, resources_after.threads,
            PROTOCOL_SOAK_PROCESS_RELEASE_TIMEOUT_MS);
    if (rc == TURBO_OK) rc = release_rc;
  }
  return rc;
}

static int protocol_soak_run_series(void) {
  uint64_t latencies[PROTOCOL_SOAK_LATENCY_SAMPLES];
  size_t trace_counts[sizeof(protocol_soak_traces) / sizeof(protocol_soak_traces[0])] = {0};
  protocol_soak_resources_t baseline;
  protocol_soak_resources_t current;
  uint64_t iterations_value = 0u;
  uint64_t duration_ms = 0u;
  uint64_t seed = 0u;
  uint64_t rss_tolerance_value = 0u;
  uint64_t random_state;
  size_t completed = 0u;
  size_t failures = 0u;
  size_t latency_count = 0u;
  size_t iterations;
  size_t rss_tolerance;
  const size_t trace_count = sizeof(protocol_soak_traces) / sizeof(protocol_soak_traces[0]);
  const char *selected_trace_name = getenv("PROTOCOL_TRANSPORT_SOAK_TRACE");
  size_t selected_trace = SIZE_MAX;
  uint64_t series_started_at;
  uint64_t deadline_ms;
  int rc;

  rc = protocol_soak_env_u64("PROTOCOL_TRANSPORT_SOAK_ITERATIONS", PROTOCOL_SOAK_DEFAULT_ITERATIONS,
                            PROTOCOL_SOAK_MAX_ITERATIONS, 0, &iterations_value);
  if (rc == TURBO_OK)
    rc = protocol_soak_env_u64("PROTOCOL_TRANSPORT_SOAK_DURATION_MS", 0u,
                              PROTOCOL_SOAK_MAX_DURATION_MS, 1, &duration_ms);
  if (rc == TURBO_OK)
    rc = protocol_soak_env_u64("PROTOCOL_TRANSPORT_SOAK_SEED", PROTOCOL_SOAK_DEFAULT_SEED, UINT64_MAX,
                              0, &seed);
  if (rc == TURBO_OK)
    rc = protocol_soak_env_u64("PROTOCOL_TRANSPORT_SOAK_RSS_TOLERANCE_BYTES",
                              PROTOCOL_SOAK_DEFAULT_RSS_TOLERANCE, PROTOCOL_SOAK_MAX_RSS_TOLERANCE, 1,
                              &rss_tolerance_value);
  if (rc != TURBO_OK) {
    fputs("SOAK_CONFIGURATION_FAILURE invalid protocol transport soak environment\n", stderr);
    return rc;
  }
  iterations = (size_t)iterations_value;
  rss_tolerance = (size_t)rss_tolerance_value;
  random_state = seed;
  if (selected_trace_name && selected_trace_name[0]) {
    for (size_t trace_index = 0u; trace_index < trace_count; ++trace_index) {
      if (strcmp(selected_trace_name, protocol_soak_traces[trace_index].name) == 0) {
        selected_trace = trace_index;
        break;
      }
    }
    if (selected_trace == SIZE_MAX) {
      fprintf(stderr, "SOAK_CONFIGURATION_FAILURE unknown trace=%s\n", selected_trace_name);
      return TURBO_EINVAL;
    }
  }

  /*
   * Process monitoring and the sanitizer runtime may retain bounded global
   * handles after their first use. The stable leak baseline begins only after
   * that one-time initialization has completed.
   */
  rc = protocol_soak_run_child(
      &protocol_soak_traces[selected_trace == SIZE_MAX ? 0u : selected_trace], 0);
  if (rc != TURBO_OK) return rc;
  rc = protocol_soak_resource_snapshot(&baseline);
  if (rc != TURBO_OK) return rc;
  current = baseline;
  series_started_at = turbo_hrtime();
  deadline_ms = duration_ms != 0u ? turbo_monotonic_ms() + duration_ms : 0u;

  while (completed < iterations || (deadline_ms != 0u && turbo_monotonic_ms() < deadline_ms)) {
    const size_t trace_index = selected_trace == SIZE_MAX
                                   ? (size_t)(protocol_soak_random(&random_state) % trace_count)
                                   : selected_trace;
    const uint64_t started_at = turbo_hrtime();
    uint64_t latency;
    rc = protocol_soak_run_child(&protocol_soak_traces[trace_index], 1);
    latency = turbo_hrtime() - started_at;
    ++completed;
    ++trace_counts[trace_index];
    if (latency_count < PROTOCOL_SOAK_LATENCY_SAMPLES) {
      latencies[latency_count++] = latency;
    } else {
      const size_t slot = (size_t)(protocol_soak_random(&random_state) % completed);
      if (slot < PROTOCOL_SOAK_LATENCY_SAMPLES) latencies[slot] = latency;
    }
    if (rc != TURBO_OK) ++failures;
    rc = protocol_soak_resource_snapshot(&current);
    if (rc != TURBO_OK) return rc;
    if (current.handles > baseline.handles || current.threads > baseline.threads ||
        (current.rss_bytes > baseline.rss_bytes &&
         current.rss_bytes - baseline.rss_bytes > rss_tolerance)) {
      fprintf(stderr,
              "SOAK_RESOURCE_FAILURE id=PROTOCOL-SOAK-001 rss_baseline=%zu "
              "rss_current=%zu rss_tolerance=%zu handles_baseline=%zu handles_current=%zu "
              "threads_baseline=%zu threads_current=%zu\n",
              baseline.rss_bytes, current.rss_bytes, rss_tolerance, baseline.handles,
              current.handles, baseline.threads, current.threads);
      return TURBO_ENOSPC;
    }
    if (failures != 0u) break;
  }

  printf("SOAK_RESULT id=PROTOCOL-SOAK-001 seed=%" PRIu64
         " operations=%zu failures=%zu duration_ns=%" PRIu64
         " payload_distribution=transport-security-reconnect-shutdown "
         "udp_single_datagram_shutdown=%zu tcp_settlement=%zu "
         "tls_reconnect=%zu tls_startup_recovery=%zu "
         "wss_mtls_rejection=%zu wss_mtls_abort_cleanup=%zu wss_mtls_reconnect=%zu "
         "p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64
         " rss_baseline=%zu rss_final=%zu handles_baseline=%zu handles_final=%zu "
         "threads_baseline=%zu threads_final=%zu resource_monotonic_growth=false\n",
         seed, completed, failures, turbo_hrtime() - series_started_at,
         trace_counts[0], trace_counts[1], trace_counts[2], trace_counts[3],
         trace_counts[4], trace_counts[5], trace_counts[6],
         protocol_soak_percentile(latencies, latency_count, 50u),
         protocol_soak_percentile(latencies, latency_count, 95u),
         protocol_soak_percentile(latencies, latency_count, 99u), baseline.rss_bytes,
         current.rss_bytes, baseline.handles, current.handles, baseline.threads, current.threads);
  return failures == 0u ? TURBO_OK : TURBO_EIO;
}

spec("Protocol CoroNet scheduled transport soak") {
  it("PROTOCOL-SOAK-001 preserves transport lifecycle and parent resources") {
    check_int_eq(protocol_soak_run_series(), TURBO_OK);
  }
}
