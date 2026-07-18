#include "socket.h"

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

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
typedef SOCKET socket_bench_fd_t;
  #define SOCKET_BENCH_INVALID_FD INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int socket_bench_fd_t;
  #define SOCKET_BENCH_INVALID_FD (-1)
#endif

enum {
  SOCKET_BENCH_SAMPLES = 7,
  SOCKET_BENCH_CHUNK_BYTES = 64 * 1024,
  SOCKET_BENCH_WARMUP_BYTES = 8 * 1024 * 1024,
  SOCKET_BENCH_SAMPLE_BYTES = 64 * 1024 * 1024,
  SOCKET_BENCH_TIMEOUT_MS = 30000
};

typedef struct socket_bench_state_s {
  atomic_uint_fast64_t bytes;
  atomic_uint_fast64_t messages;
} socket_bench_state_t;

static int socket_bench_u64_compare(const void *lhs, const void *rhs) {
  const uint64_t left = *(const uint64_t *)lhs;
  const uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

static void socket_bench_close(socket_bench_fd_t fd) {
  if (fd == SOCKET_BENCH_INVALID_FD) return;
#ifdef _WIN32
  closesocket(fd);
#else
  close(fd);
#endif
}

static int socket_bench_platform_init(void) {
#ifdef _WIN32
  WSADATA data;
  return WSAStartup(MAKEWORD(2, 2), &data) == 0 ? TURBO_OK : TURBO_EIO;
#else
  return TURBO_OK;
#endif
}

static void socket_bench_platform_cleanup(void) {
#ifdef _WIN32
  WSACleanup();
#endif
}

static unsigned short socket_bench_pick_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  int address_size = (int)sizeof(address);
#else
  socklen_t address_size = (socklen_t)sizeof(address);
#endif
  socket_bench_fd_t fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  unsigned short port = 0u;
  if (fd == SOCKET_BENCH_INVALID_FD) return 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(fd, (const struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(fd, (struct sockaddr *)&address, &address_size) == 0) {
    port = ntohs(address.sin_port);
  }
  socket_bench_close(fd);
  return port;
}

static int socket_bench_capture(turbo_flow_msg_t *message, void *ctx) {
  socket_bench_state_t *state = (socket_bench_state_t *)ctx;
  if (!state || !message || (message->payload.len > 0u && !message->payload.data)) {
    return TURBO_EINVAL;
  }
  atomic_fetch_add_explicit(&state->bytes, message->payload.len, memory_order_relaxed);
  atomic_fetch_add_explicit(&state->messages, 1u, memory_order_relaxed);
  return TURBO_OK;
}

static int socket_bench_send_bytes(unsigned short port, size_t total_bytes) {
  struct sockaddr_in address;
  socket_bench_fd_t fd = SOCKET_BENCH_INVALID_FD;
  char *payload = NULL;
  size_t sent_total = 0u;
  int rc = TURBO_OK;

  payload = (char *)malloc(SOCKET_BENCH_CHUNK_BYTES);
  if (!payload) return TURBO_ENOMEM;
  memset(payload, 0x5a, SOCKET_BENCH_CHUNK_BYTES);
  fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd == SOCKET_BENCH_INVALID_FD) {
    rc = TURBO_EIO;
    goto done;
  }
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1 ||
      connect(fd, (const struct sockaddr *)&address, sizeof(address)) != 0) {
    rc = TURBO_ENOTCONN;
    goto done;
  }
  while (sent_total < total_bytes) {
    const size_t remaining = total_bytes - sent_total;
    const size_t requested =
        remaining < SOCKET_BENCH_CHUNK_BYTES ? remaining : SOCKET_BENCH_CHUNK_BYTES;
#ifdef _WIN32
    const int sent = send(fd, payload, (int)requested, 0);
#else
    const ssize_t sent = send(fd, payload, requested, 0);
#endif
    if (sent <= 0) {
      rc = TURBO_EIO;
      break;
    }
    sent_total += (size_t)sent;
  }

done:
  socket_bench_close(fd);
  free(payload);
  return rc;
}

static int socket_bench_wait_bytes(socket_bench_state_t *state, uint64_t expected) {
  const uint64_t deadline = turbo_hrtime() + (uint64_t)SOCKET_BENCH_TIMEOUT_MS * UINT64_C(1000000);
  while (atomic_load_explicit(&state->bytes, memory_order_acquire) < expected) {
    if (turbo_hrtime() >= deadline) return TURBO_ETIMEDOUT;
    turbo_thread_yield();
  }
  return TURBO_OK;
}

static void socket_bench_tcp_source(void) {
  static const char graph[] = "source socket_in adapter \"socket.tcp\"\n"
                              "stage capture\n"
                              "stage main {\n"
                              "  socket_in -> capture\n"
                              "}\n";
  socket_bench_state_t state;
  turbo_flow_coronet_socket_config_t config;
  turbo_flow_t *flow = NULL;
  uint64_t elapsed[SOCKET_BENCH_SAMPLES] = {0};
  uint64_t messages[SOCKET_BENCH_SAMPLES] = {0};
  unsigned short port;
  int platform_started = 0;
  int flow_started = 0;
  int rc;

  atomic_init(&state.bytes, 0u);
  atomic_init(&state.messages, 0u);
  memset(&config, 0, sizeof(config));
  rc = socket_bench_platform_init();
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) return;
  platform_started = 1;
  port = socket_bench_pick_port();
  check_int_gt((int)port, 0);
  if (port == 0u) goto done;

  flow = turbo_flow_create();
  check_not_null(flow);
  if (!flow) goto done;
  config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
  config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.timeout_ms = SOCKET_BENCH_TIMEOUT_MS;
  rc = turbo_flow_coronet_register_socket_adapter(flow, "socket.tcp", &config);
  if (rc == TURBO_OK)
    rc = turbo_flow_register_stage_ex(flow, "capture", socket_bench_capture, &state, NULL);
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  flow_started = 1;

  rc = socket_bench_send_bytes(port, SOCKET_BENCH_WARMUP_BYTES);
  if (rc == TURBO_OK) rc = socket_bench_wait_bytes(&state, SOCKET_BENCH_WARMUP_BYTES);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;

  for (size_t sample = 0u; sample < SOCKET_BENCH_SAMPLES; ++sample) {
    uint64_t begin;
    atomic_store_explicit(&state.bytes, 0u, memory_order_release);
    atomic_store_explicit(&state.messages, 0u, memory_order_release);
    begin = turbo_hrtime();
    rc = socket_bench_send_bytes(port, SOCKET_BENCH_SAMPLE_BYTES);
    if (rc == TURBO_OK) rc = socket_bench_wait_bytes(&state, SOCKET_BENCH_SAMPLE_BYTES);
    elapsed[sample] = turbo_hrtime() - begin;
    messages[sample] = atomic_load_explicit(&state.messages, memory_order_acquire);
    if (rc != TURBO_OK) break;
  }
  check_int_eq(rc, TURBO_OK);
  if (rc == TURBO_OK) {
    const size_t median = SOCKET_BENCH_SAMPLES / 2u;
    qsort(elapsed, SOCKET_BENCH_SAMPLES, sizeof(elapsed[0]), socket_bench_u64_compare);
    qsort(messages, SOCKET_BENCH_SAMPLES, sizeof(messages[0]), socket_bench_u64_compare);
    printf("SOCKET_BENCH_RESULT operation=tcp_source samples=%u bytes=%u chunk_bytes=%u "
           "median_ns=%" PRIu64 " throughput_mib_s=%.2f callbacks_s=%.2f\n",
           SOCKET_BENCH_SAMPLES, SOCKET_BENCH_SAMPLE_BYTES, SOCKET_BENCH_CHUNK_BYTES,
           elapsed[median],
           elapsed[median] ? ((double)SOCKET_BENCH_SAMPLE_BYTES * 1000000000.0) /
                                 ((double)elapsed[median] * 1024.0 * 1024.0)
                           : 0.0,
           elapsed[median] ? ((double)messages[median] * 1000000000.0) / (double)elapsed[median]
                           : 0.0);
  }

done:
  if (flow && flow_started) check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
  if (platform_started) socket_bench_platform_cleanup();
}

spec("TurboFlow socket provider benchmarks") {
  bench("real TCP source delivery") { socket_bench_tcp_source(); }
}
