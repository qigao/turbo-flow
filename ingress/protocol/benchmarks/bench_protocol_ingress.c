#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_protocol_coronet.h"
#include "turbo_flow_protocol_graph.h"
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
typedef SOCKET protocol_bench_socket_t;
  #define PROTOCOL_BENCH_INVALID_SOCKET INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int protocol_bench_socket_t;
  #define PROTOCOL_BENCH_INVALID_SOCKET (-1)
#endif

enum {
  PROTOCOL_BENCH_FRAME_BYTES = 25,
  PROTOCOL_BENCH_STREAM_BUFFER_BYTES = 256 * 1024,
  PROTOCOL_BENCH_WARMUP_MESSAGES = 4096,
  PROTOCOL_BENCH_SAMPLE_MESSAGES = 32768,
  PROTOCOL_BENCH_SAMPLES = 7,
  PROTOCOL_BENCH_TIMEOUT_MS = 30000
};

typedef struct protocol_bench_capture_s {
  atomic_uint_fast64_t messages;
  atomic_int status;
} protocol_bench_capture_t;

typedef struct protocol_bench_receiver_s {
  protocol_bench_socket_t socket_handle;
  atomic_uint_fast64_t bytes;
  atomic_int stopping;
  atomic_int status;
} protocol_bench_receiver_t;

static int protocol_bench_u64_compare(const void *lhs, const void *rhs) {
  const uint64_t left = *(const uint64_t *)lhs;
  const uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

static int protocol_bench_platform_init(void) {
#ifdef _WIN32
  WSADATA data;
  return WSAStartup(MAKEWORD(2, 2), &data) == 0 ? TURBO_OK : TURBO_EIO;
#else
  return TURBO_OK;
#endif
}

static void protocol_bench_platform_cleanup(void) {
#ifdef _WIN32
  WSACleanup();
#endif
}

static void protocol_bench_socket_shutdown(protocol_bench_socket_t socket_handle) {
  if (socket_handle == PROTOCOL_BENCH_INVALID_SOCKET) return;
#ifdef _WIN32
  (void)shutdown(socket_handle, SD_BOTH);
#else
  (void)shutdown(socket_handle, SHUT_RDWR);
#endif
}

static void protocol_bench_socket_close(protocol_bench_socket_t socket_handle) {
  if (socket_handle == PROTOCOL_BENCH_INVALID_SOCKET) return;
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
}

static unsigned short protocol_bench_pick_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  int address_size = (int)sizeof(address);
#else
  socklen_t address_size = (socklen_t)sizeof(address);
#endif
  protocol_bench_socket_t socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  unsigned short port = 0u;
  if (socket_handle == PROTOCOL_BENCH_INVALID_SOCKET) return 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(socket_handle, (const struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) == 0)
    port = ntohs(address.sin_port);
  protocol_bench_socket_close(socket_handle);
  return port;
}

static protocol_bench_socket_t protocol_bench_connect(unsigned short port) {
  struct sockaddr_in address;
  protocol_bench_socket_t socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == PROTOCOL_BENCH_INVALID_SOCKET) return PROTOCOL_BENCH_INVALID_SOCKET;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (connect(socket_handle, (const struct sockaddr *)&address, sizeof(address)) != 0) {
    protocol_bench_socket_close(socket_handle);
    return PROTOCOL_BENCH_INVALID_SOCKET;
  }
  return socket_handle;
}

static int protocol_bench_send_all(protocol_bench_socket_t socket_handle, const uint8_t *data,
                                   size_t size) {
  size_t sent_total = 0u;
  if (!data || size == 0u) return TURBO_EINVAL;
  while (sent_total < size) {
#ifdef _WIN32
    int sent = send(socket_handle, (const char *)data + sent_total, (int)(size - sent_total), 0);
#else
    ssize_t sent = send(socket_handle, data + sent_total, size - sent_total, 0);
#endif
    if (sent <= 0) return TURBO_EIO;
    sent_total += (size_t)sent;
  }
  return TURBO_OK;
}

static void protocol_bench_receiver_thread(void *arg) {
  protocol_bench_receiver_t *receiver = (protocol_bench_receiver_t *)arg;
  uint8_t buffer[16u * 1024u];
  for (;;) {
#ifdef _WIN32
    int received = recv(receiver->socket_handle, (char *)buffer, (int)sizeof(buffer), 0);
#else
    ssize_t received = recv(receiver->socket_handle, buffer, sizeof(buffer), 0);
#endif
    if (received > 0) {
      atomic_fetch_add_explicit(&receiver->bytes, (uint64_t)received, memory_order_relaxed);
      continue;
    }
    if (!atomic_load_explicit(&receiver->stopping, memory_order_acquire))
      atomic_store_explicit(&receiver->status, TURBO_EIO, memory_order_release);
    break;
  }
}

static void protocol_bench_gbt_frame(uint8_t frame[PROTOCOL_BENCH_FRAME_BYTES]) {
  static const char vin[] = "L1234567890123456";
  uint8_t checksum = 0u;
  memset(frame, 0, PROTOCOL_BENCH_FRAME_BYTES);
  frame[0] = 0x23u;
  frame[1] = 0x23u;
  frame[2] = 0x02u;
  frame[3] = 0xfeu;
  memcpy(frame + 4u, vin, sizeof(vin) - 1u);
  frame[21] = 0x01u;
  for (size_t i = 2u; i < PROTOCOL_BENCH_FRAME_BYTES - 1u; ++i)
    checksum ^= frame[i];
  frame[PROTOCOL_BENCH_FRAME_BYTES - 1u] = checksum;
}

static int protocol_bench_capture(turbo_flow_msg_t *message, void *ctx) {
  protocol_bench_capture_t *capture = (protocol_bench_capture_t *)ctx;
  const turbo_flow_protocol_metadata_t *metadata = turbo_flow_protocol_graph_metadata(message);
  if (!capture || !message || !metadata || message->payload.len != PROTOCOL_BENCH_FRAME_BYTES ||
      metadata->protocol != TURBO_FLOW_PROTOCOL_GBT_32960) {
    if (capture) atomic_store_explicit(&capture->status, TURBO_EPROTO, memory_order_release);
    return TURBO_EPROTO;
  }
  atomic_fetch_add_explicit(&capture->messages, 1u, memory_order_release);
  return TURBO_OK;
}

static int protocol_bench_wait_messages(protocol_bench_capture_t *capture,
                                        protocol_bench_receiver_t *receiver, uint64_t expected) {
  const uint64_t deadline = turbo_monotonic_ms() + PROTOCOL_BENCH_TIMEOUT_MS;
  while (atomic_load_explicit(&capture->messages, memory_order_acquire) < expected) {
    int status = atomic_load_explicit(&capture->status, memory_order_acquire);
    if (status != TURBO_OK) return status;
    status = atomic_load_explicit(&receiver->status, memory_order_acquire);
    if (status != TURBO_OK) return status;
    if (turbo_monotonic_ms() >= deadline) return TURBO_ETIMEDOUT;
    turbo_thread_yield();
  }
  return TURBO_OK;
}

static int protocol_bench_server_cleanup(turbo_flow_protocol_coronet_server_t *server) {
  int rc;
  if (!server) return TURBO_OK;
  rc = turbo_flow_protocol_coronet_server_destroy(server);
  if (rc != TURBO_EBUSY) return rc;
  rc = turbo_flow_protocol_coronet_server_begin_shutdown(server);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_coronet_server_wait_shutdown(server, PROTOCOL_BENCH_TIMEOUT_MS);
  if (rc == TURBO_ETIMEDOUT)
    rc = turbo_flow_protocol_coronet_server_force_shutdown(server, TURBO_ECANCELED,
                                                           PROTOCOL_BENCH_TIMEOUT_MS);
  if (rc == TURBO_OK) rc = turbo_flow_protocol_coronet_server_destroy(server);
  return rc;
}

static int protocol_bench_plugin_open(turbo_flow_protocol_registry_t **registry_out,
                                      turbo_flow_protocol_owner_t **owner_out,
                                      turbo_flow_protocol_t **protocol_out) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  char reason[256];
  int rc;
  request.protocol = TURBO_FLOW_PROTOCOL_GBT_32960;
  request.protocol_version = "2025";
  request.max_frame_size = PROTOCOL_BENCH_STREAM_BUFFER_BYTES;
  rc = turbo_flow_protocol_registry_create(1u, &registry);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_registry_load(registry, FLOW_PROTOCOL_GBT32960_MODULE, reason,
                                           sizeof(reason));
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_owner_create_registered(registry, "gbt32960", &request, &owner);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_owner_instance(owner, TURBO_FLOW_PROTOCOL_GBT_32960, &protocol);
  if (rc != TURBO_OK) {
    turbo_flow_protocol_owner_destroy(owner);
    if (registry) (void)turbo_flow_protocol_registry_destroy(registry);
    return rc;
  }
  *registry_out = registry;
  *owner_out = owner;
  *protocol_out = protocol;
  return TURBO_OK;
}

static void protocol_bench_socket_to_graph(void) {
  static const char graph[] = "source protocol_in\n"
                              "stage capture\n"
                              "stage main {\n"
                              "  protocol_in -> capture\n"
                              "}\n";
  const size_t batch_bytes = (size_t)PROTOCOL_BENCH_SAMPLE_MESSAGES * PROTOCOL_BENCH_FRAME_BYTES;
  uint8_t frame[PROTOCOL_BENCH_FRAME_BYTES];
  uint8_t *batch = NULL;
  uint64_t elapsed[PROTOCOL_BENCH_SAMPLES] = {0};
  uint64_t expected_messages = 0u;
  protocol_bench_capture_t capture;
  protocol_bench_receiver_t receiver;
  turbo_thread_t receiver_thread = NULL;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_coronet_server_t *server = NULL;
  turbo_flow_protocol_coronet_config_t config = TURBO_FLOW_PROTOCOL_CORONET_CONFIG_INIT;
  turbo_flow_protocol_graph_sink_t graph_sink = TURBO_FLOW_PROTOCOL_GRAPH_SINK_INIT;
  turbo_flow_t *flow = NULL;
  protocol_bench_socket_t client = PROTOCOL_BENCH_INVALID_SOCKET;
  unsigned short port = 0u;
  int platform_started = 0;
  int flow_started = 0;
  int receiver_started = 0;
  int rc;

  atomic_init(&capture.messages, 0u);
  atomic_init(&capture.status, TURBO_OK);
  memset(&receiver, 0, sizeof(receiver));
  receiver.socket_handle = PROTOCOL_BENCH_INVALID_SOCKET;
  atomic_init(&receiver.bytes, 0u);
  atomic_init(&receiver.stopping, 0);
  atomic_init(&receiver.status, TURBO_OK);
  rc = protocol_bench_platform_init();
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) return;
  platform_started = 1;
  port = protocol_bench_pick_port();
  check_int_gt((int)port, 0);
  if (port == 0u) goto done;

  rc = protocol_bench_plugin_open(&registry, &owner, &protocol);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  flow = turbo_flow_create();
  check_not_null(flow);
  if (!flow) goto done;
  rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc == TURBO_OK)
    rc = turbo_flow_register_stage_ex(flow, "capture", protocol_bench_capture, &capture, NULL);
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  flow_started = 1;

  graph_sink.flow = flow;
  graph_sink.source_name = "protocol_in";
  config.protocol = protocol;
  config.port = (int)port;
  config.runtime.max_sessions = 1u;
  config.runtime.max_frame_size = PROTOCOL_BENCH_STREAM_BUFFER_BYTES;
  config.runtime.max_buffered_bytes = 2u * PROTOCOL_BENCH_STREAM_BUFFER_BYTES;
  config.runtime_ops.publish = turbo_flow_protocol_graph_publish;
  config.runtime_ctx = &graph_sink;
  config.resolve_identity = NULL;
  rc = turbo_flow_protocol_coronet_server_create(&config, &server);
  if (rc == TURBO_OK) rc = turbo_flow_protocol_coronet_server_start(server);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;

  client = protocol_bench_connect(port);
  check_true(client != PROTOCOL_BENCH_INVALID_SOCKET);
  if (client == PROTOCOL_BENCH_INVALID_SOCKET) goto done;
  receiver.socket_handle = client;
  rc = turbo_thread_create(&receiver_thread, protocol_bench_receiver_thread, &receiver);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  receiver_started = 1;

  protocol_bench_gbt_frame(frame);
  batch = (uint8_t *)malloc(batch_bytes);
  check_not_null(batch);
  if (!batch) goto done;
  for (size_t offset = 0u; offset < batch_bytes; offset += sizeof(frame))
    memcpy(batch + offset, frame, sizeof(frame));

  rc = protocol_bench_send_all(client, batch,
                               (size_t)PROTOCOL_BENCH_WARMUP_MESSAGES * sizeof(frame));
  expected_messages += PROTOCOL_BENCH_WARMUP_MESSAGES;
  if (rc == TURBO_OK) rc = protocol_bench_wait_messages(&capture, &receiver, expected_messages);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;

  for (size_t sample = 0u; sample < PROTOCOL_BENCH_SAMPLES; ++sample) {
    uint64_t begin = turbo_hrtime();
    rc = protocol_bench_send_all(client, batch, batch_bytes);
    expected_messages += PROTOCOL_BENCH_SAMPLE_MESSAGES;
    if (rc == TURBO_OK) rc = protocol_bench_wait_messages(&capture, &receiver, expected_messages);
    elapsed[sample] = turbo_hrtime() - begin;
    if (rc != TURBO_OK) break;
  }
  check_int_eq(rc, TURBO_OK);
  check_int_eq(atomic_load_explicit(&receiver.status, memory_order_acquire), TURBO_OK);
  if (rc == TURBO_OK) {
    const size_t median = PROTOCOL_BENCH_SAMPLES / 2u;
    qsort(elapsed, PROTOCOL_BENCH_SAMPLES, sizeof(elapsed[0]), protocol_bench_u64_compare);
    printf("PROTOCOL_BENCH_RESULT operation=tcp_gbt32960_graph samples=%u messages=%u "
           "frame_bytes=%u median_ns=%" PRIu64 " messages_s=%.2f input_mib_s=%.2f\n",
           PROTOCOL_BENCH_SAMPLES, PROTOCOL_BENCH_SAMPLE_MESSAGES, PROTOCOL_BENCH_FRAME_BYTES,
           elapsed[median],
           elapsed[median]
               ? ((double)PROTOCOL_BENCH_SAMPLE_MESSAGES * 1000000000.0) / (double)elapsed[median]
               : 0.0,
           elapsed[median]
               ? ((double)batch_bytes * 1000000000.0) / ((double)elapsed[median] * 1024.0 * 1024.0)
               : 0.0);
  }

done:
  free(batch);
  if (receiver_started) {
    atomic_store_explicit(&receiver.stopping, 1, memory_order_release);
    protocol_bench_socket_shutdown(client);
    check_int_eq(turbo_thread_join(&receiver_thread), TURBO_OK);
  }
  protocol_bench_socket_close(client);
  check_int_eq(protocol_bench_server_cleanup(server), TURBO_OK);
  if (flow && flow_started) check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
  turbo_flow_protocol_owner_destroy(owner);
  if (registry) check_int_eq(turbo_flow_protocol_registry_destroy(registry), TURBO_OK);
  if (platform_started) protocol_bench_platform_cleanup();
}

spec("TurboFlow protocol ingress benchmarks") {
  bench("TCP GB/T 32960 through protocol decode and Graph") { protocol_bench_socket_to_graph(); }
}
