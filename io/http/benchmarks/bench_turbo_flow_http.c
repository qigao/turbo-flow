#include "turbo_flow_http_client.h"
#include "turbo_flow_http_server.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow.h"
#include "turbo_thread.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
typedef SOCKET http_bench_fd_t;
  #define HTTP_BENCH_INVALID_FD INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int http_bench_fd_t;
  #define HTTP_BENCH_INVALID_FD (-1)
#endif

enum {
  HTTP_BENCH_SAMPLES = 7,
  HTTP_BENCH_REQUESTS = 128,
  HTTP_BENCH_PAYLOAD_BYTES = 64 * 1024,
  HTTP_BENCH_TIMEOUT_MS = 30000,
  HTTP_BENCH_PUMP_ITERATIONS = 100000
};

typedef struct http_bench_capture_s {
  size_t expected_size;
  size_t calls;
} http_bench_capture_t;

static int http_bench_u64_compare(const void *lhs, const void *rhs) {
  const uint64_t left = *(const uint64_t *)lhs;
  const uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

static void http_bench_close(http_bench_fd_t fd) {
  if (fd == HTTP_BENCH_INVALID_FD) return;
#ifdef _WIN32
  closesocket(fd);
#else
  close(fd);
#endif
}

static int http_bench_platform_init(void) {
#ifdef _WIN32
  WSADATA data;
  return WSAStartup(MAKEWORD(2, 2), &data) == 0 ? TURBO_OK : TURBO_EIO;
#else
  return TURBO_OK;
#endif
}

static void http_bench_platform_cleanup(void) {
#ifdef _WIN32
  WSACleanup();
#endif
}

static unsigned short http_bench_pick_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  int address_size = (int)sizeof(address);
#else
  socklen_t address_size = (socklen_t)sizeof(address);
#endif
  http_bench_fd_t fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  unsigned short port = 0u;
  if (fd == HTTP_BENCH_INVALID_FD) return 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(fd, (const struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(fd, (struct sockaddr *)&address, &address_size) == 0) {
    port = ntohs(address.sin_port);
  }
  http_bench_close(fd);
  return port;
}

static int http_bench_capture(turbo_flow_msg_t *message, void *ctx) {
  http_bench_capture_t *capture = (http_bench_capture_t *)ctx;
  if (!capture || !message || message->payload.len != capture->expected_size ||
      (message->payload.len > 0u && !message->payload.data)) {
    return TURBO_EPROTO;
  }
  capture->calls += 1u;
  return TURBO_OK;
}

static void http_bench_round_trip(void) {
  static const char server_graph[] =
      "source request adapter http.server.echo operation "
      TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION " resource http.server.echo\n"
      "stage response adapter http.server.echo operation "
      TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION " resource http.server.echo\n"
      "stage main {\n"
      "  request -> response\n"
      "}\n";
  static const char client_graph[] =
      "source input\n"
      "stage request adapter http.client.echo operation "
      TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION " resource http.client.echo\n"
      "stage capture\n"
      "stage main {\n"
      "  input -> request -> capture\n"
      "}\n";
  turbo_flow_http_server_config_t server_config;
  turbo_flow_http_client_config_t client_config;
  turbo_flow_t *server_flow = NULL;
  turbo_flow_t *client_flow = NULL;
  turbo_flow_msg_t message;
  http_bench_capture_t capture;
  uint64_t elapsed[HTTP_BENCH_SAMPLES] = {0};
  char url[128];
  unsigned short port;
  int platform_started = 0;
  int server_started = 0;
  int client_started = 0;
  int rc;

  turbo_flow_msg_init(&message);
  memset(&capture, 0, sizeof(capture));
  memset(&server_config, 0, sizeof(server_config));
  memset(&client_config, 0, sizeof(client_config));
  capture.expected_size = HTTP_BENCH_PAYLOAD_BYTES;
  rc = http_bench_platform_init();
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) return;
  platform_started = 1;
  port = http_bench_pick_port();
  check_int_gt((int)port, 0);
  if (port == 0u) goto done;

  server_flow = turbo_flow_create();
  client_flow = turbo_flow_create();
  check_not_null(server_flow);
  check_not_null(client_flow);
  if (!server_flow || !client_flow) goto done;
  server_config.port = port;
  server_config.route = "/echo";
  server_config.method = TURBO_FLOW_HTTP_POST;
  server_config.max_body_size = HTTP_BENCH_PAYLOAD_BYTES;
  server_config.response_content_type = "application/octet-stream";
  rc = turbo_flow_http_register_server_adapter(server_flow, "http.server.echo", &server_config);
  if (rc == TURBO_OK)
    rc = turbo_flow_parse_string(server_flow, server_graph, sizeof(server_graph) - 1u);
  if (rc == TURBO_OK) rc = turbo_flow_compile(server_flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(server_flow);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  server_started = 1;

  (void)snprintf(url, sizeof(url), "http://127.0.0.1:%u/echo", (unsigned int)port);
  client_config.url = url;
  client_config.method = TURBO_FLOW_HTTP_POST;
  client_config.timeout_ms = HTTP_BENCH_TIMEOUT_MS;
  client_config.max_response_size = HTTP_BENCH_PAYLOAD_BYTES;
  client_config.max_pump_iterations = HTTP_BENCH_PUMP_ITERATIONS;
  rc = turbo_flow_http_register_client_adapter(client_flow, "http.client.echo", &client_config);
  if (rc == TURBO_OK)
    rc = turbo_flow_register_stage_ex(client_flow, "capture", http_bench_capture, &capture, NULL);
  if (rc == TURBO_OK)
    rc = turbo_flow_parse_string(client_flow, client_graph, sizeof(client_graph) - 1u);
  if (rc == TURBO_OK) rc = turbo_flow_compile(client_flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(client_flow);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  client_started = 1;

  message.owned_payload = tstr_new_len(NULL, HTTP_BENCH_PAYLOAD_BYTES);
  check_not_null(message.owned_payload);
  if (!message.owned_payload) goto done;
  memset(message.owned_payload, 0x48, HTTP_BENCH_PAYLOAD_BYTES);
  message.payload = tstr_to_v(message.owned_payload);
  rc = turbo_flow_publish(client_flow, "input", &message);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;

  for (size_t sample = 0u; sample < HTTP_BENCH_SAMPLES && rc == TURBO_OK; ++sample) {
    const uint64_t begin = turbo_hrtime();
    for (size_t request = 0u; request < HTTP_BENCH_REQUESTS; ++request) {
      rc = turbo_flow_publish(client_flow, "input", &message);
      if (rc != TURBO_OK) break;
    }
    elapsed[sample] = turbo_hrtime() - begin;
  }
  check_int_eq(rc, TURBO_OK);
  if (rc == TURBO_OK) {
    const size_t median = HTTP_BENCH_SAMPLES / 2u;
    const size_t expected_calls = 1u + HTTP_BENCH_SAMPLES * HTTP_BENCH_REQUESTS;
    qsort(elapsed, HTTP_BENCH_SAMPLES, sizeof(elapsed[0]), http_bench_u64_compare);
    check_size_eq(capture.calls, expected_calls);
    printf("HTTP_BENCH_RESULT operation=echo_round_trip samples=%u requests=%u payload_bytes=%u "
           "median_ns=%" PRIu64 " throughput_req_s=%.2f throughput_mib_s=%.2f\n",
           HTTP_BENCH_SAMPLES, HTTP_BENCH_REQUESTS, HTTP_BENCH_PAYLOAD_BYTES, elapsed[median],
           elapsed[median] ? ((double)HTTP_BENCH_REQUESTS * 1000000000.0) /
                                 (double)elapsed[median]
                           : 0.0,
           elapsed[median] ? ((double)HTTP_BENCH_REQUESTS * HTTP_BENCH_PAYLOAD_BYTES *
                                  1000000000.0) /
                                 ((double)elapsed[median] * 1024.0 * 1024.0)
                           : 0.0);
  }

done:
  turbo_flow_msg_cleanup(&message);
  if (client_started) check_int_eq(turbo_flow_stop(client_flow), TURBO_OK);
  if (server_started) check_int_eq(turbo_flow_stop(server_flow), TURBO_OK);
  turbo_flow_destroy(client_flow);
  turbo_flow_destroy(server_flow);
  if (platform_started) http_bench_platform_cleanup();
}

spec("TurboFlow HTTP provider benchmarks") {
  bench("real HTTP 64 KiB echo round trips") { http_bench_round_trip(); }
}
