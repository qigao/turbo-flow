#include "socket.h"

#include "CoroNet/turbo_kcp.h"
#include "CoroNet/turbo_coro_socket.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_flow_config.h"
#include "turbo_flow_control.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <sys/time.h>
  #include <unistd.h>
#endif

#ifdef _WIN32
typedef SOCKET flow_test_socket_t;
  #define FLOW_TEST_INVALID_SOCKET INVALID_SOCKET
#else
typedef int flow_test_socket_t;
  #define FLOW_TEST_INVALID_SOCKET (-1)
#endif

typedef struct socket_test_context_runner_s {
  coro_context_t *context;
  turbo_thread_t thread;
  int started;
} socket_test_context_runner_t;

typedef struct socket_test_spawn_request_s {
  coro_fn fn;
  void *arg;
  atomic_int done;
  int status;
} socket_test_spawn_request_t;

static socket_test_context_runner_t socket_test_runner;
static const char SOCKET_TEST_KCP_PSK[] =
    "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";

static int socket_test_apply_kcp_config(coro_socket_t *socket) {
  turbo_kcp_config_t config;
  int rc;
  if (!socket) return TURBO_EINVAL;
  turbo_kcp_config_default(&config);
  memset(config.pre_shared_key, 0x5a, sizeof(config.pre_shared_key));
  rc = coro_socket_set_kcp_config(socket, &config);
  turbo_kcp_config_wipe(&config);
  return rc;
}

static void socket_test_context_thread(void *arg) {
  socket_test_context_runner_t *runner = (socket_test_context_runner_t *)arg;
  if (runner && runner->context) (void)coro_context_run(runner->context, TURBO_RUN_DEFAULT);
}

static void socket_test_context_stop_post(void *arg1, void *arg2) {
  (void)arg2;
  coro_context_stop((coro_context_t *)arg1);
}

static int socket_test_context_start(coro_context_t *context) {
  if (!context || socket_test_runner.started) return TURBO_EINVAL;
  memset(&socket_test_runner, 0, sizeof(socket_test_runner));
  socket_test_runner.context = context;
  coro_context_set_persistent(context, 1);
  if (turbo_thread_create(&socket_test_runner.thread, socket_test_context_thread,
                          &socket_test_runner) != TURBO_OK) {
    coro_context_set_persistent(context, 0);
    socket_test_runner.context = NULL;
    return TURBO_EIO;
  }
  socket_test_runner.started = 1;
  return TURBO_OK;
}

static void socket_test_context_stop(coro_context_t *context) {
  if (!socket_test_runner.started || socket_test_runner.context != context) return;
  coro_context_set_persistent(context, 0);
  if (coro_post(context, socket_test_context_stop_post, context, NULL) != TURBO_OK) {
    coro_context_stop(context);
  }
  (void)turbo_thread_join(&socket_test_runner.thread);
  memset(&socket_test_runner, 0, sizeof(socket_test_runner));
}

static void socket_test_spawn_post(void *arg1, void *arg2) {
  socket_test_spawn_request_t *request = (socket_test_spawn_request_t *)arg1;
  (void)arg2;
  request->status = coro_context_spawn(coro_context_current(), request->fn, request->arg);
  atomic_store_explicit(&request->done, 1, memory_order_release);
}

static int socket_test_context_spawn(coro_context_t *context, coro_fn fn, void *arg) {
  socket_test_spawn_request_t request;
  int rc;
  if (!context || !fn) return TURBO_EINVAL;
  if (!socket_test_runner.started || socket_test_runner.context != context) {
    return coro_context_spawn(context, fn, arg);
  }
  memset(&request, 0, sizeof(request));
  request.fn = fn;
  request.arg = arg;
  request.status = TURBO_EALREADY;
  atomic_init(&request.done, 0);
  rc = coro_post(context, socket_test_spawn_post, &request, NULL);
  if (rc != TURBO_OK) return rc;
  while (!atomic_load_explicit(&request.done, memory_order_acquire))
    turbo_sleep_ms(1);
  return request.status;
}

typedef struct silent_peer_state_s {
  flow_test_socket_t listener;
  unsigned short port;
  atomic_int accepted;
  atomic_int release;
  atomic_int status;
} silent_peer_state_t;

typedef struct tcp_timeout_client_state_s {
  coro_context_t *ctx;
  unsigned short port;
  int connect_rc;
  int recv_rc;
  int done;
} tcp_timeout_client_state_t;

static void flow_test_close_socket(flow_test_socket_t socket_handle) {
  if (socket_handle == FLOW_TEST_INVALID_SOCKET) return;
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
}

static int silent_peer_open(silent_peer_state_t *state) {
  struct sockaddr_in addr;
#ifdef _WIN32
  int addr_len = (int)sizeof(addr);
#else
  socklen_t addr_len = (socklen_t)sizeof(addr);
#endif
  if (!state) return TURBO_EINVAL;
  memset(state, 0, sizeof(*state));
  atomic_init(&state->accepted, 0);
  atomic_init(&state->release, 0);
  atomic_init(&state->status, TURBO_EALREADY);
  state->listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (state->listener == FLOW_TEST_INVALID_SOCKET) return TURBO_EIO;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);
  if (bind(state->listener, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
      listen(state->listener, 1) != 0 ||
      getsockname(state->listener, (struct sockaddr *)&addr, &addr_len) != 0) {
    flow_test_close_socket(state->listener);
    state->listener = FLOW_TEST_INVALID_SOCKET;
    return TURBO_EIO;
  }
  state->port = ntohs(addr.sin_port);
  return TURBO_OK;
}

static void silent_peer_thread(void *arg) {
  silent_peer_state_t *state = (silent_peer_state_t *)arg;
  flow_test_socket_t client;
  if (!state) return;
  client = accept(state->listener, NULL, NULL);
  if (client == FLOW_TEST_INVALID_SOCKET) {
    atomic_store_explicit(&state->status, TURBO_EIO, memory_order_release);
    return;
  }
  atomic_store_explicit(&state->accepted, 1, memory_order_release);
  while (!atomic_load_explicit(&state->release, memory_order_acquire))
    turbo_sleep_ms(1);
  flow_test_close_socket(client);
  atomic_store_explicit(&state->status, TURBO_OK, memory_order_release);
}

static void tcp_timeout_client_task(coro_t *co, void *arg) {
  tcp_timeout_client_state_t *state = (tcp_timeout_client_state_t *)arg;
  coro_socket_t *socket_handle;
  char *data = NULL;
  size_t len = 0;
  (void)co;
  if (!state || !state->ctx) return;
  socket_handle = coro_socket_create_tcpv4(state->ctx);
  if (!socket_handle) {
    state->connect_rc = TURBO_ENOMEM;
    state->done = 1;
    return;
  }
  coro_socket_set_timeout(socket_handle, 50);
  state->connect_rc = coro_socket_connect(socket_handle, "127.0.0.1", state->port);
  if (state->connect_rc == TURBO_OK) state->recv_rc = coro_socket_recv(socket_handle, &data, &len);
  if (data) coro_socket_free_recv(data);
  coro_socket_destroy(socket_handle);
  state->done = 1;
}

typedef struct socket_timeout_result_s {
  int setup_rc;
  int connect_rc;
  int operation_rc;
  int peer_rc;
} socket_timeout_result_t;

static socket_timeout_result_t run_tcp_recv_timeout_case(void) {
  socket_timeout_result_t result = {TURBO_EIO, TURBO_EALREADY, TURBO_EALREADY, TURBO_EALREADY};
  silent_peer_state_t peer;
  tcp_timeout_client_state_t client_state;
  turbo_thread_t peer_thread;
  coro_context_t *ctx = NULL;
  int peer_started = 0;

  memset(&peer, 0, sizeof(peer));
  peer.listener = FLOW_TEST_INVALID_SOCKET;
  memset(&client_state, 0, sizeof(client_state));
  ctx = coro_context_create(NULL);
  if (!ctx) {
    result.setup_rc = TURBO_ENOMEM;
    goto cleanup;
  }
  result.setup_rc = silent_peer_open(&peer);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  result.setup_rc = turbo_thread_create(&peer_thread, silent_peer_thread, &peer);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  peer_started = 1;
  client_state.ctx = ctx;
  client_state.port = peer.port;
  client_state.connect_rc = TURBO_EALREADY;
  client_state.recv_rc = TURBO_EALREADY;
  result.setup_rc = coro_context_spawn(ctx, tcp_timeout_client_task, &client_state);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  result.setup_rc = socket_test_context_start(ctx);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  for (int i = 0; i < 2000 && !client_state.done; ++i) {
    turbo_sleep_ms(1);
  }
  result.connect_rc = client_state.connect_rc;
  result.operation_rc = client_state.recv_rc;

cleanup:
  atomic_store_explicit(&peer.release, 1, memory_order_release);
  flow_test_close_socket(peer.listener);
  peer.listener = FLOW_TEST_INVALID_SOCKET;
  if (peer_started) {
    (void)turbo_thread_join(&peer_thread);
    result.peer_rc = atomic_load_explicit(&peer.status, memory_order_acquire);
  }
  if (ctx) {
    socket_test_context_stop(ctx);
    coro_context_destroy(ctx);
  }
  return result;
}

static socket_timeout_result_t run_ws_handshake_timeout_case(void) {
  static const char *src = "source input\n"
                           "stage socket_out adapter \"socket.timeout.ws\"\n"
                           "stage main {\n"
                           "  input -> socket_out\n"
                           "}\n";
  socket_timeout_result_t result = {TURBO_EIO, TURBO_EALREADY, TURBO_EALREADY, TURBO_EALREADY};
  silent_peer_state_t peer;
  turbo_thread_t peer_thread;
  turbo_flow_coronet_socket_config_t config;
  turbo_flow_msg_t msg;
  turbo_flow_t *flow = NULL;
  coro_context_t *ctx = NULL;
  mem_buffer_t *buffer = NULL;
  char payload[] = "ws-timeout";
  int peer_started = 0;
  int flow_started = 0;

  memset(&peer, 0, sizeof(peer));
  peer.listener = FLOW_TEST_INVALID_SOCKET;
  memset(&config, 0, sizeof(config));
  turbo_flow_msg_init(&msg);
  ctx = coro_context_create(NULL);
  if (!ctx) {
    result.setup_rc = TURBO_ENOMEM;
    goto cleanup;
  }
  result.setup_rc = silent_peer_open(&peer);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  result.setup_rc = turbo_thread_create(&peer_thread, silent_peer_thread, &peer);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  peer_started = 1;
  flow = turbo_flow_create();
  buffer = mem_wrap_external(payload, sizeof(payload) - 1u, NULL, NULL);
  msg.buffer = buffer;
  if (!ctx || !flow || !buffer) {
    result.setup_rc = TURBO_ENOMEM;
    goto cleanup;
  }
  config.context = ctx;
  result.setup_rc = socket_test_context_start(ctx);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
  config.transport = TURBO_FLOW_CORONET_TRANSPORT_WS;
  config.host = "127.0.0.1";
  config.port = peer.port;
  config.path = "/";
  config.handshake_timeout_ms = 50;
  config.max_pump_iterations = 20000;
  msg.payload = vstr_from_buf(payload, sizeof(payload) - 1u);
  result.setup_rc = turbo_flow_coronet_register_socket_adapter(flow, "socket.timeout.ws", &config);
  if (result.setup_rc == TURBO_OK)
    result.setup_rc = turbo_flow_parse_string(flow, src, strlen(src));
  if (result.setup_rc == TURBO_OK) result.setup_rc = turbo_flow_compile(flow);
  if (result.setup_rc == TURBO_OK) result.setup_rc = turbo_flow_start(flow);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  flow_started = 1;
  result.operation_rc = turbo_flow_publish(flow, "input", &msg);
  result.connect_rc =
      atomic_load_explicit(&peer.accepted, memory_order_acquire) ? TURBO_OK : TURBO_ENOTCONN;

cleanup:
  if (flow_started) (void)turbo_flow_stop(flow);
  socket_test_context_stop(ctx);
  turbo_flow_msg_cleanup(&msg);
  if (flow) turbo_flow_destroy(flow);
  atomic_store_explicit(&peer.release, 1, memory_order_release);
  flow_test_close_socket(peer.listener);
  peer.listener = FLOW_TEST_INVALID_SOCKET;
  if (peer_started) {
    (void)turbo_thread_join(&peer_thread);
    result.peer_rc = atomic_load_explicit(&peer.status, memory_order_acquire);
  }
  if (ctx) coro_context_destroy(ctx);
  return result;
}

typedef struct tcp_sink_state_s {
  char received[64];
  size_t received_len;
  int handler_called;
  int recv_rc;
} tcp_sink_state_t;

typedef struct flow_record_state_s {
  char payload[64];
  size_t payload_len;
  int called;
} flow_record_state_t;

typedef struct flow_async_source_gate_s {
  atomic_int entered;
  atomic_int allow_exit;
  atomic_int completed;
} flow_async_source_gate_t;

typedef struct ws_source_client_state_s {
  coro_context_t *ctx;
  const char *data;
  size_t len;
  int port;
  int secure;
  int timeout_ms;
  int rc;
  atomic_int done;
} ws_source_client_state_t;

typedef struct tls_source_client_state_s {
  coro_context_t *ctx;
  const char *data;
  size_t len;
  int port;
  int rc;
  int done;
} tls_source_client_state_t;

typedef struct kcp_source_client_state_s {
  coro_context_t *ctx;
  const char *data;
  size_t len;
  int port;
  int rc;
  int done;
} kcp_source_client_state_t;

#ifdef _WIN32
typedef struct pipe_source_client_state_s {
  coro_context_t *ctx;
  const char *data;
  size_t len;
  const char *path;
  int rc;
  int done;
} pipe_source_client_state_t;
#endif

typedef struct flow_threaded_record_state_s {
  turbo_mutex_t mutex;
  char payload[64];
  size_t payload_len;
  int called;
} flow_threaded_record_state_t;

static int flow_record_stage(turbo_flow_msg_t *msg, void *ctx) {
  flow_record_state_t *state = (flow_record_state_t *)ctx;

  if (!state || !msg) return TURBO_EINVAL;
  state->called += 1;
  state->payload_len =
      msg->payload.len < sizeof(state->payload) ? msg->payload.len : sizeof(state->payload);
  if (state->payload_len > 0) memcpy(state->payload, msg->payload.data, state->payload_len);
  return TURBO_OK;
}

static int flow_async_source_gate_stage(turbo_flow_msg_t *msg, void *ctx) {
  flow_async_source_gate_t *gate = (flow_async_source_gate_t *)ctx;
  if (!gate || !msg) return TURBO_EINVAL;
  atomic_store_explicit(&gate->entered, 1, memory_order_release);
  while (!atomic_load_explicit(&gate->allow_exit, memory_order_acquire))
    turbo_sleep_ms(1);
  atomic_store_explicit(&gate->completed, 1, memory_order_release);
  return TURBO_OK;
}

static void socket_test_mark_post(void *arg1, void *arg2) {
  atomic_int *called = (atomic_int *)arg1;
  (void)arg2;
  atomic_store_explicit(called, 1, memory_order_release);
}

static int flow_threaded_record_stage(turbo_flow_msg_t *msg, void *ctx) {
  flow_threaded_record_state_t *state = (flow_threaded_record_state_t *)ctx;

  if (!state || !msg) return TURBO_EINVAL;
  turbo_mutex_lock(&state->mutex);
  state->called += 1;
  state->payload_len =
      msg->payload.len < sizeof(state->payload) ? msg->payload.len : sizeof(state->payload);
  if (state->payload_len > 0) memcpy(state->payload, msg->payload.data, state->payload_len);
  turbo_mutex_unlock(&state->mutex);
  return TURBO_OK;
}

static int flow_threaded_record_called(flow_threaded_record_state_t *state) {
  int called;

  turbo_mutex_lock(&state->mutex);
  called = state->called;
  turbo_mutex_unlock(&state->mutex);
  return called;
}

static unsigned short test_pick_loopback_port(void) {
  unsigned short port = 0;
  struct sockaddr_in addr;
#ifdef _WIN32
  int addr_len = (int)sizeof(addr);
  SOCKET sock = INVALID_SOCKET;
#else
  socklen_t addr_len = (socklen_t)sizeof(addr);
  int sock = -1;
#endif

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (sock == INVALID_SOCKET) return 0;
#else
  if (sock < 0) return 0;
#endif

  if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
      getsockname(sock, (struct sockaddr *)&addr, &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }

#ifdef _WIN32
  closesocket(sock);
#else
  close(sock);
#endif
  return port;
}

static unsigned short test_pick_loopback_udp_port(void) {
  unsigned short port = 0;
  struct sockaddr_in addr;
#ifdef _WIN32
  int addr_len = (int)sizeof(addr);
  SOCKET sock = INVALID_SOCKET;
#else
  socklen_t addr_len = (socklen_t)sizeof(addr);
  int sock = -1;
#endif

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
#ifdef _WIN32
  if (sock == INVALID_SOCKET) return 0;
#else
  if (sock < 0) return 0;
#endif

  if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
      getsockname(sock, (struct sockaddr *)&addr, &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }

#ifdef _WIN32
  closesocket(sock);
#else
  close(sock);
#endif
  return port;
}

static int test_send_loopback_payload(unsigned short port, const char *data, size_t len) {
  struct sockaddr_in addr;
  size_t sent = 0;
#ifdef _WIN32
  SOCKET sock = INVALID_SOCKET;
#else
  int sock = -1;
#endif

  if (!data && len > 0) return TURBO_EINVAL;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) return TURBO_EINVAL;

  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (sock == INVALID_SOCKET) return TURBO_EINVAL;
#else
  if (sock < 0) return TURBO_EINVAL;
#endif

  if (connect(sock, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
#ifdef _WIN32
    closesocket(sock);
#else
    close(sock);
#endif
    return TURBO_EINVAL;
  }

  while (sent < len) {
#ifdef _WIN32
    int n = send(sock, data + sent, (int)(len - sent), 0);
#else
    ssize_t n = send(sock, data + sent, len - sent, 0);
#endif
    if (n <= 0) {
#ifdef _WIN32
      closesocket(sock);
#else
      close(sock);
#endif
      return TURBO_EINVAL;
    }
    sent += (size_t)n;
  }

#ifdef _WIN32
  shutdown(sock, SD_BOTH);
  closesocket(sock);
#else
  shutdown(sock, SHUT_RDWR);
  close(sock);
#endif
  return TURBO_OK;
}

static int test_send_loopback_udp_payload(unsigned short port, const char *data, size_t len) {
  struct sockaddr_in addr;
#ifdef _WIN32
  SOCKET sock = INVALID_SOCKET;
  int sent;
#else
  int sock = -1;
  ssize_t sent;
#endif

  if (!data && len > 0) return TURBO_EINVAL;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) return TURBO_EINVAL;

  sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
#ifdef _WIN32
  if (sock == INVALID_SOCKET) return TURBO_EINVAL;
#else
  if (sock < 0) return TURBO_EINVAL;
#endif

#ifdef _WIN32
  sent = sendto(sock, data, (int)len, 0, (const struct sockaddr *)&addr, sizeof(addr));
  closesocket(sock);
#else
  sent = sendto(sock, data, len, 0, (const struct sockaddr *)&addr, sizeof(addr));
  close(sock);
#endif

  return sent == (int)len ? TURBO_OK : TURBO_EINVAL;
}

static void ws_source_client_task(coro_t *co, void *arg) {
  ws_source_client_state_t *state = (ws_source_client_state_t *)arg;
  coro_socket_t *client;
  int rc;

  (void)co;
  if (!state || !state->ctx || !state->data) return;

  client = coro_socket_create_tcpv4(state->ctx);
  if (!client) {
    state->rc = TURBO_ENOMEM;
    atomic_store_explicit(&state->done, 1, memory_order_release);
    return;
  }

  coro_socket_set_timeout(client, state->timeout_ms);
  rc = coro_socket_connect_ws(client, state->secure ? "localhost" : "127.0.0.1", state->port, "/",
                              state->secure);
  if (rc == TURBO_OK) rc = coro_socket_send(client, state->data, state->len);

  coro_socket_destroy(client);
  state->rc = rc;
  atomic_store_explicit(&state->done, 1, memory_order_release);
}

enum { FLOW_WS_SOURCE_TIMEOUT_MS = 5000, FLOW_WS_SOURCE_WAIT_ITERATIONS = 7000 };

typedef struct ws_source_case_result_s {
  int setup_rc;
  int stop_rc;
  int client_done;
  int client_rc;
  int record_called;
  size_t payload_len;
  char payload[64];
} ws_source_case_result_t;

static ws_source_case_result_t run_ws_source_case(int secure) {
  static const char *plain_dsl = "source socket_in adapter \"socket.ws\"\n"
                                 "stage record\n"
                                 "stage main {\n"
                                 "  socket_in -> record\n"
                                 "}\n";
  static const char *secure_dsl = "source socket_in adapter \"socket.wss\"\n"
                                  "stage record\n"
                                  "stage main {\n"
                                  "  socket_in -> record\n"
                                  "}\n";
  static const char plain_payload[] = "ws-frame";
  static const char secure_payload[] = "wss-frame";
  ws_source_case_result_t result = {TURBO_EIO, TURBO_EALREADY, 0, TURBO_EALREADY, 0, 0, {0}};
  flow_threaded_record_state_t record;
  ws_source_client_state_t client;
  turbo_flow_coronet_socket_config_t config;
  turbo_flow_t *flow = NULL;
  coro_context_t *ctx = NULL;
  const char *dsl = secure ? secure_dsl : plain_dsl;
  const char *payload = secure ? secure_payload : plain_payload;
  char ca_file[512] = {0};
  char cert_file[512] = {0};
  char key_file[512] = {0};
  unsigned short port;
  int flow_started = 0;
  int runner_started = 0;
  int mutex_initialized = 0;

  memset(&record, 0, sizeof(record));
  memset(&client, 0, sizeof(client));
  memset(&config, 0, sizeof(config));
  atomic_init(&client.done, 0);
  turbo_mutex_init(&record.mutex);
  mutex_initialized = 1;

  if (secure) {
    result.setup_rc = tls_test_write_ca_file(ca_file, sizeof(ca_file));
    if (result.setup_rc != TURBO_OK) goto cleanup;
    result.setup_rc =
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file));
    if (result.setup_rc != TURBO_OK) goto cleanup;
    result.setup_rc = tls_test_set_ca_file_env(ca_file);
    if (result.setup_rc != TURBO_OK) goto cleanup;
    result.setup_rc = tls_test_set_server_env(cert_file, key_file);
    if (result.setup_rc != TURBO_OK) goto cleanup;
  }

  ctx = coro_context_create(NULL);
  flow = turbo_flow_create();
  if (!ctx || !flow) {
    result.setup_rc = TURBO_ENOMEM;
    goto cleanup;
  }
  port = test_pick_loopback_port();
  if (port == 0u) {
    result.setup_rc = TURBO_EADDRNOTAVAIL;
    goto cleanup;
  }
  result.setup_rc = socket_test_context_start(ctx);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  runner_started = 1;

  config.context = ctx;
  config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
  config.transport = secure ? TURBO_FLOW_CORONET_TRANSPORT_WSS : TURBO_FLOW_CORONET_TRANSPORT_WS;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.timeout_ms = FLOW_WS_SOURCE_TIMEOUT_MS;
  client.ctx = ctx;
  client.port = (int)port;
  client.secure = secure;
  client.timeout_ms = FLOW_WS_SOURCE_TIMEOUT_MS;
  client.data = payload;
  client.len = strlen(payload);
  client.rc = TURBO_EBUSY;

  result.setup_rc = turbo_flow_coronet_register_socket_adapter(
      flow, secure ? "socket.wss" : "socket.ws", &config);
  if (result.setup_rc == TURBO_OK) {
    result.setup_rc =
        turbo_flow_register_stage_ex(flow, "record", flow_threaded_record_stage, &record, NULL);
  }
  if (result.setup_rc == TURBO_OK)
    result.setup_rc = turbo_flow_parse_string(flow, dsl, strlen(dsl));
  if (result.setup_rc == TURBO_OK) result.setup_rc = turbo_flow_compile(flow);
  if (result.setup_rc == TURBO_OK) result.setup_rc = turbo_flow_start(flow);
  if (result.setup_rc != TURBO_OK) goto cleanup;
  flow_started = 1;
  result.setup_rc = socket_test_context_spawn(ctx, ws_source_client_task, &client);
  if (result.setup_rc != TURBO_OK) goto cleanup;

  for (int i = 0; i < FLOW_WS_SOURCE_WAIT_ITERATIONS &&
                  (!flow_threaded_record_called(&record) ||
                   !atomic_load_explicit(&client.done, memory_order_acquire));
       ++i) {
    turbo_sleep_ms(1);
  }
  result.client_done = atomic_load_explicit(&client.done, memory_order_acquire);
  if (result.client_done) result.client_rc = client.rc;
  turbo_mutex_lock(&record.mutex);
  result.record_called = record.called;
  result.payload_len = record.payload_len;
  if (result.payload_len > 0u) memcpy(result.payload, record.payload, result.payload_len);
  turbo_mutex_unlock(&record.mutex);

cleanup:
  if (flow_started) result.stop_rc = turbo_flow_stop(flow);
  if (runner_started) socket_test_context_stop(ctx);
  if (flow) turbo_flow_destroy(flow);
  if (ctx) coro_context_destroy(ctx);
  if (mutex_initialized) turbo_mutex_destroy(&record.mutex);
  if (secure) {
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }
  return result;
}

static void tls_source_client_task(coro_t *co, void *arg) {
  tls_source_client_state_t *state = (tls_source_client_state_t *)arg;
  coro_socket_t *client;
  int rc;

  (void)co;
  if (!state || !state->ctx || !state->data) return;

  client = coro_socket_create(state->ctx, CORO_SOCKET_TLS);
  if (!client) {
    state->rc = TURBO_ENOMEM;
    state->done = 1;
    return;
  }

  coro_socket_set_timeout(client, 1000);
  rc = coro_socket_connect(client, "localhost", state->port);
  if (rc == TURBO_OK) rc = coro_socket_send(client, state->data, state->len);

  coro_socket_destroy(client);
  state->rc = rc;
  state->done = 1;
}

static void kcp_source_client_task(coro_t *co, void *arg) {
  kcp_source_client_state_t *state = (kcp_source_client_state_t *)arg;
  coro_socket_t *client;
  int rc;

  (void)co;
  if (!state || !state->ctx || !state->data) return;

  client = coro_socket_create_kcp(state->ctx);
  if (!client) {
    state->rc = TURBO_ENOMEM;
    state->done = 1;
    return;
  }

  coro_socket_set_timeout(client, 1000);
  rc = socket_test_apply_kcp_config(client);
  if (rc == TURBO_OK) rc = coro_socket_connect(client, "127.0.0.1", state->port);
  if (rc == TURBO_OK) rc = coro_socket_send(client, state->data, state->len);

  coro_socket_destroy(client);
  state->rc = rc;
  state->done = 1;
}

#ifdef _WIN32
static void pipe_source_client_task(coro_t *co, void *arg) {
  pipe_source_client_state_t *state = (pipe_source_client_state_t *)arg;
  coro_socket_t *client;
  int rc;

  (void)co;
  if (!state || !state->ctx || !state->data || !state->path) return;

  client = coro_socket_create_pipe(state->ctx);
  if (!client) {
    state->rc = TURBO_ENOMEM;
    state->done = 1;
    return;
  }

  coro_socket_set_timeout(client, 1000);
  rc = coro_socket_connect_pipe(client, state->path);
  if (rc == TURBO_OK) rc = coro_socket_send(client, state->data, state->len);

  coro_socket_destroy(client);
  state->rc = rc;
  state->done = 1;
}
#endif

static void tcp_sink_server_handler(coro_socket_t *client, void *arg) {
  tcp_sink_state_t *state = (tcp_sink_state_t *)arg;
  char *data = NULL;
  size_t len = 0;

  coro_socket_set_timeout(client, 1000);
  state->recv_rc = coro_socket_recv(client, &data, &len);
  state->handler_called = 1;
  if (state->recv_rc == TURBO_OK && data) {
    state->received_len = len < sizeof(state->received) ? len : sizeof(state->received);
    memcpy(state->received, data, state->received_len);
    coro_socket_free_recv(data);
  }
}

spec("turbo_flow_coronet") {
  it("times out a silent TCP receive operation") {
    socket_timeout_result_t result = run_tcp_recv_timeout_case();
    check_equal(result.setup_rc, TURBO_OK);
    check_equal(result.connect_rc, TURBO_OK);
    check_equal(result.operation_rc, TURBO_ETIMEDOUT);
    check_equal(result.peer_rc, TURBO_OK);
  }

  it("times out a silent WebSocket upgrade with the handshake deadline") {
    socket_timeout_result_t result = run_ws_handshake_timeout_case();
    check_equal(result.setup_rc, TURBO_OK);
    check_equal(result.connect_rc, TURBO_OK);
    check_equal(result.operation_rc, TURBO_ETIMEDOUT);
    check_equal(result.peer_rc, TURBO_OK);
  }

  it("registers a generic socket adapter for inline owner stages") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.tcp\" operation "
                             TURBO_FLOW_SOCKET_SEND_OPERATION " resource \"socket.tcp\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;
    turbo_flow_connection_snapshot_t connection;
    const turbo_flow_option_field_t *transport = NULL;

    check_not_null(flow);
    check_not_null(buffer);
    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.tcp", NULL), TURBO_OK);
    check_equal(turbo_flow_adapter_operation_module(
                     flow, "socket.tcp", TURBO_FLOW_SOCKET_SEND_OPERATION),
                 TURBO_FLOW_SOCKET_MODULE);
    check_equal(turbo_flow_adapter_operation_resource(
                     flow, "socket.tcp", TURBO_FLOW_SOCKET_SEND_OPERATION),
                 "socket.tcp");
    check_equal(turbo_flow_find_primitive(flow, "socket.tcp")->type_name,
                 TURBO_FLOW_SOCKET_PRIMITIVE_TYPE);
    schema = turbo_flow_find_adapter_schema(flow, "socket.tcp");
    check_not_null(schema);
    check_equal(schema->kind, TURBO_FLOW_ADAPTER_KIND_SOCKET);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK);
    memset(&connection, 0, sizeof(connection));
    check_equal(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_OK);
    check_equal(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_equal(connection.endpoint, "tcp://:0");
    check_equal(schema->field_count, 40);
    const turbo_flow_option_field_t *kcp_pre_shared_key = NULL;
    const turbo_flow_option_field_t *kcp_mtu = NULL;
    const turbo_flow_option_field_t *kcp_send_window = NULL;
    const turbo_flow_option_field_t *kcp_receive_window = NULL;
    const turbo_flow_option_field_t *kcp_interval_ms = NULL;
    const turbo_flow_option_field_t *kcp_handshake_retry_ms = NULL;
    const turbo_flow_option_field_t *kcp_fast_resend = NULL;
    const turbo_flow_option_field_t *kcp_congestion_control = NULL;
    const turbo_flow_option_field_t *kcp_fec_data_shards = NULL;
    const turbo_flow_option_field_t *kcp_fec_parity_shards = NULL;
    const turbo_flow_option_field_t *kcp_fec_max_payload_size = NULL;
    const turbo_flow_option_field_t *kcp_fec_receive_groups = NULL;
    const turbo_flow_option_field_t *connect_timeout_ms = NULL;
    const turbo_flow_option_field_t *send_timeout_ms = NULL;
    const turbo_flow_option_field_t *recv_timeout_ms = NULL;
    const turbo_flow_option_field_t *handshake_timeout_ms = NULL;
    const turbo_flow_option_field_t *reuse_port = NULL;
    const turbo_flow_option_field_t *tcp_keepalive = NULL;
    const turbo_flow_option_field_t *tcp_keepalive_idle_ms = NULL;
    const turbo_flow_option_field_t *tcp_keepalive_interval_ms = NULL;
    const turbo_flow_option_field_t *tcp_keepalive_count = NULL;
    const turbo_flow_option_field_t *linger = NULL;
    const turbo_flow_option_field_t *linger_ms = NULL;
    const turbo_flow_option_field_t *send_hwm_bytes = NULL;
    const turbo_flow_option_field_t *source_handoff = NULL;
    const turbo_flow_option_field_t *udp_multicast_group = NULL;
    const turbo_flow_option_field_t *udp_multicast_interface = NULL;
    const turbo_flow_option_field_t *udp_option_flags = NULL;
    const turbo_flow_option_field_t *udp_multicast_loop = NULL;
    const turbo_flow_option_field_t *udp_multicast_ttl = NULL;
    const turbo_flow_option_field_t *udp_broadcast = NULL;
    for (size_t i = 0; i < schema->field_count; ++i) {
      if (strcmp(schema->fields[i].name, "transport") == 0) transport = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_pre_shared_key") == 0)
        kcp_pre_shared_key = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_mtu") == 0) kcp_mtu = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_send_window") == 0)
        kcp_send_window = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_receive_window") == 0)
        kcp_receive_window = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_interval_ms") == 0)
        kcp_interval_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_handshake_retry_ms") == 0)
        kcp_handshake_retry_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fast_resend") == 0)
        kcp_fast_resend = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_congestion_control") == 0)
        kcp_congestion_control = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fec_data_shards") == 0)
        kcp_fec_data_shards = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fec_parity_shards") == 0)
        kcp_fec_parity_shards = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fec_max_payload_size") == 0)
        kcp_fec_max_payload_size = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fec_receive_groups") == 0)
        kcp_fec_receive_groups = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "connect_timeout_ms") == 0)
        connect_timeout_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "send_timeout_ms") == 0)
        send_timeout_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "recv_timeout_ms") == 0)
        recv_timeout_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "handshake_timeout_ms") == 0)
        handshake_timeout_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "reuse_port") == 0) reuse_port = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "tcp_keepalive") == 0) tcp_keepalive = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "tcp_keepalive_idle_ms") == 0)
        tcp_keepalive_idle_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "tcp_keepalive_interval_ms") == 0)
        tcp_keepalive_interval_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "tcp_keepalive_count") == 0)
        tcp_keepalive_count = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "linger") == 0) linger = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "linger_ms") == 0) linger_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "send_hwm_bytes") == 0)
        send_hwm_bytes = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "source_handoff") == 0)
        source_handoff = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "udp_multicast_group") == 0)
        udp_multicast_group = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "udp_multicast_interface") == 0)
        udp_multicast_interface = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "udp_option_flags") == 0)
        udp_option_flags = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "udp_multicast_loop") == 0)
        udp_multicast_loop = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "udp_multicast_ttl") == 0)
        udp_multicast_ttl = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "udp_broadcast") == 0) udp_broadcast = &schema->fields[i];
    }
    check_not_null(transport);
    check_not_null(kcp_pre_shared_key);
    check_not_null(kcp_mtu);
    check_not_null(kcp_send_window);
    check_not_null(kcp_receive_window);
    check_not_null(kcp_interval_ms);
    check_not_null(kcp_handshake_retry_ms);
    check_not_null(kcp_fast_resend);
    check_not_null(kcp_congestion_control);
    check_not_null(kcp_fec_data_shards);
    check_not_null(kcp_fec_parity_shards);
    check_not_null(kcp_fec_max_payload_size);
    check_not_null(kcp_fec_receive_groups);
    check_not_null(connect_timeout_ms);
    check_not_null(send_timeout_ms);
    check_not_null(recv_timeout_ms);
    check_not_null(handshake_timeout_ms);
    check_not_null(reuse_port);
    check_not_null(tcp_keepalive);
    check_not_null(tcp_keepalive_idle_ms);
    check_not_null(tcp_keepalive_interval_ms);
    check_not_null(tcp_keepalive_count);
    check_not_null(linger);
    check_not_null(linger_ms);
    check_not_null(send_hwm_bytes);
    check_not_null(source_handoff);
    check_not_null(udp_multicast_group);
    check_not_null(udp_multicast_interface);
    check_not_null(udp_option_flags);
    check_not_null(udp_multicast_loop);
    check_not_null(udp_multicast_ttl);
    check_not_null(udp_broadcast);
    check_equal(transport->enum_value_count, 7);
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_TCP], "tcp");
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_UDP], "udp");
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_KCP], "kcp");
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_TLS], "tls");
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_WS], "ws");
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_WSS], "wss");
    check_equal(transport->enum_values[TURBO_FLOW_CORONET_TRANSPORT_PIPE], "pipe");
    check_equal(source_handoff->enum_value_count, 2u);
    check_equal(source_handoff->enum_values[TURBO_FLOW_SOURCE_HANDOFF_INLINE], "inline");
    check_equal(source_handoff->enum_values[TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED],
                "async_bounded");
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ENOTSUP);
    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
  }

  it("validates concrete socket endpoint configurations") {
    static const char KCP_PSK[] =
        "102132435465768798a9bacbdcedfe0f1f2e3d4c5b6a798897a6b5c4d3e2f101";
    turbo_flow_coronet_socket_config_t config;
    memset(&config, 0, sizeof(config));

    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);

    config.connect_timeout_ms = TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.connect_timeout_ms = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TLS;
    config.connect_timeout_ms = 250;
    config.handshake_timeout_ms = 500;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.handshake_timeout_ms = 250;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.connect_timeout_ms = 0;
    config.handshake_timeout_ms = 500;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_ENOTSUP);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.handshake_timeout_ms = 0;

    config.tcp_keepalive = 1;
    config.tcp_keepalive_idle_ms = 30000;
    config.tcp_keepalive_interval_ms = 5000;
    config.tcp_keepalive_count = 3;
    config.linger = 1;
    config.linger_ms = 250;
    config.send_hwm_bytes = 8192;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.tcp_keepalive = 0;
    config.tcp_keepalive_idle_ms = 0;
    config.tcp_keepalive_interval_ms = 0;
    config.tcp_keepalive_count = 0;
    config.linger = 0;
    config.linger_ms = 0;
    config.send_hwm_bytes = 0;

    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.reuse_port = 1;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_UDP;
    config.udp_option_flags = TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_LOOP |
                              TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_TTL |
                              TURBO_FLOW_CORONET_UDP_OPTION_BROADCAST;
    config.udp_multicast_loop = 1;
    config.udp_multicast_ttl = 1;
    config.udp_broadcast = 1;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.udp_multicast_group = "239.255.0.1";
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.udp_multicast_group = NULL;
    config.udp_option_flags = 0;
    config.udp_multicast_loop = 0;
    config.udp_multicast_ttl = 0;
    config.udp_broadcast = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_KCP;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.kcp_pre_shared_key = KCP_PSK;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TLS;
    config.kcp_pre_shared_key = NULL;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_WS;
    config.path = "/";
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_WSS;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_PIPE;
    config.host = NULL;
    config.port = 0;
    config.path = "pipe://turbo_flow_socket_config";
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.reuse_port = 0;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);

    config.send_hwm_bytes = 4096;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_OK);
  }

  it("registers socket configuration from shared resolved YAML fragments") {
    static const char yaml[] = "version: 1\n"
                               "fragments:\n"
                               "  connection:\n"
                               "    local:\n"
                               "      transport: tcp\n"
                               "      host: 127.0.0.1\n"
                               "      port: 7001\n"
                               "  timer:\n"
                               "    bounded:\n"
                               "      timeout_ms: 250\n"
                               "  coro:\n"
                               "    buffered:\n"
                               "      send_hwm_bytes: 8192\n"
                               "adapters:\n"
                               "  socket.out:\n"
                               "    kind: socket\n"
                               "    fragments:\n"
                               "      connection: local\n"
                               "      timer: bounded\n"
                               "      coro: buffered\n"
                               "    config:\n"
                               "      role: sink\n"
                               "      tcp_keepalive: true\n"
                               "      tcp_keepalive_idle_ms: 30000\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_connection_snapshot_t connection;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_equal(turbo_flow_coronet_register_socket_resolved_adapter(flow, resolved, "socket.out"),
                 TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "socket.out");
    check_not_null(schema);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    memset(&connection, 0, sizeof(connection));
    check_equal(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connection), TURBO_OK);
    check_equal(connection.endpoint, "tcp://127.0.0.1:7001");
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("registers async bounded socket source handoff from resolved YAML") {
    static const char yaml[] =
        "version: 1\n"
        "adapters:\n"
        "  socket.in:\n"
        "    kind: socket\n"
        "    config:\n"
        "      role: source\n"
        "      transport: tcp\n"
        "      host: 127.0.0.1\n"
        "      port: 7002\n"
        "      source_handoff: async_bounded\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                TURBO_OK);
    check_equal(turbo_flow_coronet_register_socket_resolved_adapter(flow, resolved, "socket.in"),
                TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "socket.in");
    check_not_null(schema);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("fails resolved socket registration for unknown mistyped and host-only fields") {
    static const char unknown[] =
        "version: 1\nadapters:\n  socket.out:\n    kind: socket\n    config:\n"
        "      role: sink\n      transport: tcp\n      host: 127.0.0.1\n      port: 7001\n"
        "      polling: eager\n";
    static const char mistyped[] =
        "version: 1\nadapters:\n  socket.out:\n    kind: socket\n    config:\n"
        "      role: sink\n      transport: tcp\n      host: 127.0.0.1\n      port: bad\n";
    static const char host_only[] =
        "version: 1\nadapters:\n  socket.out:\n    kind: socket\n    config:\n"
        "      role: sink\n      transport: tcp\n      host: 127.0.0.1\n      port: 7001\n"
        "      take_context_ownership: true\n";
    const char *documents[] = {unknown, mistyped, host_only};
    const size_t lengths[] = {sizeof(unknown) - 1u, sizeof(mistyped) - 1u, sizeof(host_only) - 1u};
    for (size_t i = 0u; i < sizeof(documents) / sizeof(documents[0]); ++i) {
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_resolved_config_t *resolved = NULL;
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      check_equal(turbo_flow_config_resolve_yaml(documents[i], lengths[i], &resolved, &error),
                   TURBO_OK);
      check_equal(
          turbo_flow_coronet_register_socket_resolved_adapter(flow, resolved, "socket.out"),
          TURBO_EINVAL);
      turbo_flow_resolved_config_destroy(resolved);
      turbo_flow_destroy(flow);
    }
  }

  it("rejects incomplete socket endpoint configurations") {
    static const char KCP_PSK[] =
        "102132435465768798a9bacbdcedfe0f1f2e3d4c5b6a798897a6b5c4d3e2f101";
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    memset(&config, 0, sizeof(config));
    check_not_null(flow);

    check_equal(turbo_flow_coronet_socket_config_validate(NULL), TURBO_EINVAL);

    config.role = TURBO_FLOW_CORONET_SOCKET_UNCONFIGURED;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);

    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = (turbo_flow_coronet_transport_t)99;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.kcp_pre_shared_key = KCP_PSK;
    config.kcp_fec_data_shards = 4;
    config.kcp_fec_parity_shards = 2;
    config.kcp_fec_max_payload_size = 1200;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.kcp_pre_shared_key = NULL;
    config.kcp_fec_data_shards = 0;
    config.kcp_fec_parity_shards = 0;
    config.kcp_fec_max_payload_size = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_UDP;
    config.tcp_keepalive = 1;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.tcp_keepalive = 0;

    config.udp_option_flags = TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_TTL;
    config.udp_multicast_ttl = 256;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_ERANGE);
    config.udp_multicast_ttl = 1;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.udp_option_flags = 0;
    config.udp_multicast_ttl = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.tcp_keepalive_idle_ms = 30000;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.tcp_keepalive_idle_ms = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_KCP;
    config.send_hwm_bytes = 4096;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.send_hwm_bytes = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_PIPE;
    config.linger = 1;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    config.linger = 0;

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = NULL;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);

    config.host = "127.0.0.1";
    config.port = 0;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);

    config.transport = TURBO_FLOW_CORONET_TRANSPORT_PIPE;
    config.host = NULL;
    config.path = NULL;
    check_equal(turbo_flow_coronet_socket_config_validate(&config), TURBO_EINVAL);
    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.invalid", &config),
                 TURBO_EINVAL);

    turbo_flow_destroy(flow);
  }

  it("rejects invalid socket source handoff profiles") {
    turbo_flow_coronet_execution_binding_t execution;
    turbo_flow_coronet_socket_source_options_t source_options =
        TURBO_FLOW_CORONET_SOCKET_SOURCE_OPTIONS_INIT;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&execution, 0, sizeof(execution));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = 7003;

    source_options.size = sizeof(source_options) - 1u;
    check_equal(turbo_flow_coronet_register_socket_adapter_with_source_options(
                    flow, "socket.bad-size", &config, &execution, &source_options),
                TURBO_EINVAL);
    source_options = (turbo_flow_coronet_socket_source_options_t)
        TURBO_FLOW_CORONET_SOCKET_SOURCE_OPTIONS_INIT;
    source_options.abi_version += 1u;
    check_equal(turbo_flow_coronet_register_socket_adapter_with_source_options(
                    flow, "socket.bad-abi", &config, &execution, &source_options),
                TURBO_EINVAL);
    source_options = (turbo_flow_coronet_socket_source_options_t)
        TURBO_FLOW_CORONET_SOCKET_SOURCE_OPTIONS_INIT;
    source_options.handoff = (turbo_flow_source_handoff_mode_t)99;
    check_equal(turbo_flow_coronet_register_socket_adapter_with_source_options(
                    flow, "socket.bad-mode", &config, &execution, &source_options),
                TURBO_EINVAL);
    source_options = (turbo_flow_coronet_socket_source_options_t)
        TURBO_FLOW_CORONET_SOCKET_SOURCE_OPTIONS_INIT;
    source_options.handoff = TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED;
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    check_equal(turbo_flow_coronet_register_socket_adapter_with_source_options(
                    flow, "socket.bad-role", &config, &execution, &source_options),
                TURBO_EINVAL);
    check_equal(turbo_flow_coronet_register_socket_adapter_with_source_options(
                    flow, "socket.no-config", NULL, &execution, &source_options),
                TURBO_EINVAL);

    turbo_flow_destroy(flow);
  }

  it("fails start when a borrowed CoroNet context has no host driver") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.borrowed\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    coro_context_t *ctx = coro_context_create(NULL);
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    check_not_null(ctx);
    check_not_null(flow);
    config.context = ctx;
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "203.0.113.1";
    config.port = 9;
    config.timeout_ms = 50;
    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.borrowed", &config),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_ETIMEDOUT);

    (void)coro_context_run(ctx, TURBO_RUN_ONCE);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

  it("owns pending sink requests until the send deadline expires") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.pending\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "pending-payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "203.0.113.1";
    config.port = 9;
    config.timeout_ms = 100;
    config.max_pump_iterations = 1;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.pending", &config),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    /* The TEST-NET sink either hangs until the send deadline (TURBO_ETIMEDOUT)
     * or fails the connect immediately with a platform-specific connect error.
     * Either way the payload must not be accepted as delivered. */
    int publish_rc = turbo_flow_publish(flow, "input", &msg);
    check_true(publish_rc != TURBO_OK);
    check_not_equal(publish_rc, TURBO_EBUSY);

    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

  it("controls sink admission through the control DSL") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.control\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    static const char *quiesce = "adapter socket.control quiesce";
    static const char *resume = "adapter socket.control resume";
    char raw[] = "controlled-payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    int resumed_rc;

    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);
    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "203.0.113.1";
    config.port = 9;
    config.timeout_ms = 100;
    config.max_pump_iterations = 1;
    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.control", &config),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_control_ex(flow, quiesce, strlen(quiesce), NULL, NULL), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EBUSY);
    check_equal(turbo_flow_control_ex(flow, resume, strlen(resume), NULL, NULL), TURBO_OK);
    resumed_rc = turbo_flow_publish(flow, "input", &msg);
    check_not_equal(resumed_rc, TURBO_EBUSY);

    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

  it("sends flow payloads to a configured TCP socket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.tcp\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    port = test_pick_loopback_port();
    check_greater(port, 0);
    server = coro_socket_create_tcpv4(ctx);
    check_not_null(server);
    check_equal(coro_socket_listen_on(server, "127.0.0.1", port, tcp_sink_server_handler, &state),
                 TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.tcp", &config), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }

  it("sends flow payloads to a configured UDP socket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.udp\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "udp-payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    port = test_pick_loopback_udp_port();
    check_greater(port, 0);
    server = coro_socket_create_udpv4(ctx);
    check_not_null(server);
    check_equal(coro_socket_listen_on(server, "127.0.0.1", port, tcp_sink_server_handler, &state),
                 TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_UDP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.udp", &config), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }

  it("sends flow payloads to a configured KCP socket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.kcp\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "kcp-payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    port = test_pick_loopback_udp_port();
    check_greater(port, 0);
    server = coro_socket_create_kcp(ctx);
    check_not_null(server);
    check_equal(socket_test_apply_kcp_config(server), TURBO_OK);
    check_equal(
        coro_socket_listen_on(server, "127.0.0.1", (int)port, tcp_sink_server_handler, &state),
        TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_KCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;
    config.kcp_pre_shared_key = SOCKET_TEST_KCP_PSK;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.kcp", &config), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }

#ifdef _WIN32
  it("sends flow payloads to a configured Pipe socket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.pipe\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "pipe-payload";
    char path[128];
    unsigned short port = 0;
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    memset(path, 0, sizeof(path));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);
    port = test_pick_loopback_port();
    check_greater(port, 0);
    check_greater(snprintf(path, sizeof(path), "pipe://turbo_flow_socket_kcp_%u", port), 0);

    server = coro_socket_create_pipe(ctx);
    check_not_null(server);
    check_equal(coro_socket_listen_on(server, path, 0, tcp_sink_server_handler, &state), TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_PIPE;
    config.path = path;
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.pipe", &config),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }
#endif

  it("sends flow payloads to a configured WebSocket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.ws\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "ws-payload";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    port = test_pick_loopback_port();
    check_greater(port, 0);
    server = coro_socket_create_tcpv4(ctx);
    check_not_null(server);
    check_equal(
        coro_socket_listen_ws(server, "127.0.0.1", port, 0, tcp_sink_server_handler, &state),
        TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_WS;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.path = "/";
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.ws", &config), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }

  it("sends flow payloads to a configured TLS socket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.tls\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "tls-payload";
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(ca_file, 0, sizeof(ca_file));
    memset(cert_file, 0, sizeof(cert_file));
    memset(key_file, 0, sizeof(key_file));
    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    check_equal(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_equal(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_equal(tls_test_set_ca_file_env(ca_file), 0);
    check_equal(tls_test_set_server_env(cert_file, key_file), 0);

    port = test_pick_loopback_port();
    check_greater(port, 0);
    server = coro_socket_create(ctx, CORO_SOCKET_TLS);
    check_not_null(server);
    check_equal(coro_socket_listen_on(server, "127.0.0.1", port, tcp_sink_server_handler, &state),
                 TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TLS;
    config.host = "localhost";
    config.port = (int)port;
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.tls", &config), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("sends flow payloads to a configured secure WebSocket sink") {
    static const char *src = "source input\n"
                             "stage socket_out adapter \"socket.wss\"\n"
                             "stage main {\n"
                             "  input -> socket_out\n"
                             "}\n";
    char raw[] = "wss-payload";
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    tcp_sink_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(ca_file, 0, sizeof(ca_file));
    memset(cert_file, 0, sizeof(cert_file));
    memset(key_file, 0, sizeof(key_file));
    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    check_equal(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_equal(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_equal(tls_test_set_ca_file_env(ca_file), 0);
    check_equal(tls_test_set_server_env(cert_file, key_file), 0);

    port = test_pick_loopback_port();
    check_greater(port, 0);
    server = coro_socket_create_tcpv4(ctx);
    check_not_null(server);
    check_equal(
        coro_socket_listen_ws(server, "127.0.0.1", port, 1, tcp_sink_server_handler, &state),
        TURBO_OK);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SINK;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_WSS;
    config.host = "localhost";
    config.port = (int)port;
    config.path = "/";
    config.timeout_ms = 1000;
    config.max_pump_iterations = 20000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.wss", &config), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !state.handler_called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.handler_called, 1);
    check_equal(state.recv_rc, TURBO_OK);
    check_equal(state.received_len, sizeof(raw) - 1);
    check_equal(state.received, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    socket_test_context_stop(ctx);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("publishes TCP socket source payloads into the flow graph") {
    static const char *src = "source socket_in adapter \"socket.tcp\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "inbound";
    coro_context_t *ctx = coro_context_create(NULL);
    unsigned short port = 0;
    flow_record_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(ctx);

    port = test_pick_loopback_port();
    check_greater(port, 0);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.tcp", &config), TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "record", flow_record_stage, &state, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(test_send_loopback_payload(port, raw, sizeof(raw) - 1), TURBO_OK);

    for (int i = 0; i < 2000 && !state.called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.called, 1);
    check_equal(state.payload_len, sizeof(raw) - 1);
    check_equal(state.payload, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

  it("keeps the CoroNet owner responsive during async bounded source handoff") {
    static const char *src = "source socket_in adapter \"socket.tcp\"\n"
                             "stage gate\n"
                             "stage main {\n"
                             "  socket_in -> gate\n"
                             "}\n";
    const char raw[] = "async-inbound";
    coro_context_t *ctx = coro_context_create(NULL);
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    turbo_flow_coronet_execution_binding_t execution;
    turbo_flow_coronet_socket_source_options_t source_options =
        TURBO_FLOW_CORONET_SOCKET_SOURCE_OPTIONS_INIT;
    turbo_flow_coronet_socket_config_t config;
    flow_async_source_gate_t gate;
    atomic_int owner_posted;
    turbo_flow_t *flow = turbo_flow_create();
    unsigned short port = test_pick_loopback_port();
    int post_rc;
    int owner_responsive;

    memset(&execution, 0, sizeof(execution));
    memset(&config, 0, sizeof(config));
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.allow_exit, 0);
    atomic_init(&gate.completed, 0);
    atomic_init(&owner_posted, 0);
    check_not_null(ctx);
    check_not_null(flow);
    check_greater(port, 0);
    ingress.workers = 1u;
    ingress.queue_capacity = 2u;
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    execution.context = ctx;
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000u;
    source_options.handoff = TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED;

    check_equal(socket_test_context_start(ctx), TURBO_OK);
    check_equal(turbo_flow_configure_async_ingress(flow, &ingress), TURBO_OK);
    check_equal(turbo_flow_coronet_register_socket_adapter_with_source_options(
                    flow, "socket.tcp", &config, &execution, &source_options),
                TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "gate", flow_async_source_gate_stage, &gate,
                                              NULL),
                TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(test_send_loopback_payload(port, raw, sizeof(raw) - 1u), TURBO_OK);

    for (int i = 0; i < 1000 && !atomic_load_explicit(&gate.entered, memory_order_acquire); ++i)
      turbo_sleep_ms(1);
    post_rc = coro_post(ctx, socket_test_mark_post, (void *)&owner_posted, NULL);
    for (int i = 0; i < 200 && !atomic_load_explicit(&owner_posted, memory_order_acquire); ++i)
      turbo_sleep_ms(1);
    owner_responsive = atomic_load_explicit(&owner_posted, memory_order_acquire);
    atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
    for (int i = 0; i < 1000 && !atomic_load_explicit(&gate.completed, memory_order_acquire); ++i)
      turbo_sleep_ms(1);

    check_equal(post_rc, TURBO_OK);
    check_equal(owner_responsive, 1);
    check_equal(atomic_load_explicit(&gate.completed, memory_order_acquire), 1);
    check_equal(turbo_flow_stop(flow), TURBO_OK);

    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

  it("publishes TLS socket source payloads into the flow graph") {
    static const char *src = "source socket_in adapter \"socket.tls\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "tls-frame";
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    coro_context_t *ctx = coro_context_create(NULL);
    unsigned short port = 0;
    flow_record_state_t state;
    tls_source_client_state_t client_state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(ca_file, 0, sizeof(ca_file));
    memset(cert_file, 0, sizeof(cert_file));
    memset(key_file, 0, sizeof(key_file));
    memset(&state, 0, sizeof(state));
    memset(&client_state, 0, sizeof(client_state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(ctx);

    check_equal(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_equal(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_equal(tls_test_set_ca_file_env(ca_file), 0);
    check_equal(tls_test_set_server_env(cert_file, key_file), 0);

    port = test_pick_loopback_port();
    check_greater(port, 0);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TLS;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;

    client_state.ctx = ctx;
    client_state.port = (int)port;
    client_state.data = raw;
    client_state.len = sizeof(raw) - 1;
    client_state.rc = TURBO_EBUSY;

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.tls", &config), TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "record", flow_record_stage, &state, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(socket_test_context_spawn(ctx, tls_source_client_task, &client_state), TURBO_OK);

    for (int i = 0; i < 3000 && (!state.called || !client_state.done); ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(client_state.done, 1);
    check_equal(client_state.rc, TURBO_OK);
    check_equal(state.called, 1);
    check_equal(state.payload_len, sizeof(raw) - 1);
    check_equal(state.payload, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("retains owned TCP source payloads across worker stages") {
    static const char *src = "source socket_in adapter \"socket.tcp\"\n"
                             "stage record worker 1\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "owned-loop";
    coro_context_t *bootstrap_ctx = coro_context_create(NULL);
    unsigned short port = 0;
    flow_threaded_record_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    size_t payload_len;
    char payload[64];

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    memset(payload, 0, sizeof(payload));
    turbo_mutex_init(&state.mutex);
    check_not_null(flow);
    check_not_null(bootstrap_ctx);

    port = test_pick_loopback_port();
    check_greater(port, 0);
    coro_context_destroy(bootstrap_ctx);

    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.tcp", &config), TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "record", flow_threaded_record_stage, &state, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(test_send_loopback_payload(port, raw, sizeof(raw) - 1), TURBO_OK);

    for (int i = 0; i < 200 && !flow_threaded_record_called(&state); ++i) {
      turbo_sleep_ms(5);
    }

    turbo_mutex_lock(&state.mutex);
    payload_len = state.payload_len;
    memcpy(payload, state.payload, payload_len);
    turbo_mutex_unlock(&state.mutex);

    check_equal(flow_threaded_record_called(&state), 1);
    check_equal(payload_len, sizeof(raw) - 1);
    check_equal(payload, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_destroy(flow);
    turbo_mutex_destroy(&state.mutex);
  }

  it("drives an explicitly transferred TCP socket source context like a private context") {
    static const char *src = "source socket_in adapter \"socket.owned\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "explicit-owned-loop";
    coro_context_t *owned_ctx = coro_context_create(NULL);
    turbo_flow_coronet_execution_binding_t execution;
    flow_threaded_record_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    unsigned short port = test_pick_loopback_port();
    size_t payload_len;
    char payload[64];

    memset(&execution, 0, sizeof(execution));
    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    memset(payload, 0, sizeof(payload));
    turbo_mutex_init(&state.mutex);
    check_not_null(owned_ctx);
    check_not_null(flow);
    check_greater(port, 0);
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT;
    execution.context = owned_ctx;
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;

    check_equal(
        turbo_flow_coronet_register_socket_adapter_ex(flow, "socket.owned", &config, &execution),
        TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "record", flow_threaded_record_stage, &state, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(test_send_loopback_payload(port, raw, sizeof(raw) - 1), TURBO_OK);

    for (int i = 0; i < 200 && !flow_threaded_record_called(&state); ++i) {
      turbo_sleep_ms(5);
    }
    turbo_mutex_lock(&state.mutex);
    payload_len = state.payload_len;
    memcpy(payload, state.payload, payload_len);
    turbo_mutex_unlock(&state.mutex);
    check_equal(flow_threaded_record_called(&state), 1);
    check_equal(payload_len, sizeof(raw) - 1);
    check_equal(payload, raw, sizeof(raw) - 1);
    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_destroy(flow);
    turbo_mutex_destroy(&state.mutex);
  }

  it("binds a socket adapter to one stable CoroNet pool lane") {
    static const char *src = "source socket_in adapter \"socket.pool\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    coro_thread_pool_t *pool = coro_thread_pool_create(1);
    turbo_flow_coronet_execution_binding_t execution;
    turbo_flow_coronet_socket_config_t config;
    flow_record_state_t state;
    turbo_flow_t *flow = turbo_flow_create();
    unsigned short port = test_pick_loopback_port();

    memset(&execution, 0, sizeof(execution));
    memset(&config, 0, sizeof(config));
    memset(&state, 0, sizeof(state));
    check_not_null(pool);
    check_not_null(flow);
    check_greater(port, 0);
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_POOL_LANE;
    execution.pool = pool;
    execution.lane = 0;
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;

    check_equal(turbo_flow_coronet_execution_binding_validate(&execution), TURBO_OK);
    execution.lane = 1;
    check_equal(turbo_flow_coronet_execution_binding_validate(&execution), TURBO_ERANGE);
    execution.lane = 0;
    check_equal(
        turbo_flow_coronet_register_socket_adapter_ex(flow, "socket.pool", &config, &execution),
        TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "record", flow_record_stage, &state, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_destroy(flow);
    coro_thread_pool_destroy(pool);
  }

  it("publishes UDP socket source datagrams into the flow graph") {
    static const char *src = "source socket_in adapter \"socket.udp\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "datagram";
    coro_context_t *ctx = coro_context_create(NULL);
    unsigned short port = 0;
    flow_record_state_t state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(ctx);

    port = test_pick_loopback_udp_port();
    check_greater(port, 0);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_UDP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;
    config.udp_multicast_group = "239.255.0.1";
    config.udp_option_flags = TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_LOOP |
                              TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_TTL |
                              TURBO_FLOW_CORONET_UDP_OPTION_BROADCAST;
    config.udp_multicast_loop = 1;
    config.udp_multicast_ttl = 1;
    config.udp_broadcast = 1;

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.udp", &config), TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "record", flow_record_stage, &state, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(test_send_loopback_udp_payload(port, raw, sizeof(raw) - 1), TURBO_OK);

    for (int i = 0; i < 2000 && !state.called; ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(state.called, 1);
    check_equal(state.payload_len, sizeof(raw) - 1);
    check_equal(state.payload, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

  it("publishes KCP socket source datagrams into the flow graph") {
    static const char *src = "source socket_in adapter \"socket.kcp\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "kcp-datagram";
    coro_context_t *ctx = coro_context_create(NULL);
    unsigned short port = 0;
    flow_record_state_t state;
    kcp_source_client_state_t client_state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&state, 0, sizeof(state));
    memset(&client_state, 0, sizeof(client_state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(ctx);

    port = test_pick_loopback_udp_port();
    check_greater(port, 0);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_KCP;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 1000;
    config.kcp_pre_shared_key = SOCKET_TEST_KCP_PSK;

    client_state.ctx = ctx;
    client_state.port = (int)port;
    client_state.data = raw;
    client_state.len = sizeof(raw) - 1;
    client_state.rc = TURBO_EBUSY;

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.kcp", &config), TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "record", flow_record_stage, &state, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(socket_test_context_spawn(ctx, kcp_source_client_task, &client_state), TURBO_OK);

    for (int i = 0; i < 3000 && (!state.called || !client_state.done); ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(client_state.done, 1);
    check_equal(client_state.rc, TURBO_OK);
    check_equal(state.called, 1);
    check_equal(state.payload_len, sizeof(raw) - 1);
    check_equal(state.payload, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }

#ifdef _WIN32
  it("publishes Pipe socket source frames into the flow graph") {
    static const char *src = "source socket_in adapter \"socket.pipe\"\n"
                             "stage record\n"
                             "stage main {\n"
                             "  socket_in -> record\n"
                             "}\n";
    const char raw[] = "pipe-frame";
    unsigned short port = 0;
    char path[128];
    coro_context_t *ctx = coro_context_create(NULL);
    flow_record_state_t state;
    pipe_source_client_state_t client_state;
    turbo_flow_coronet_socket_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(path, 0, sizeof(path));
    memset(&state, 0, sizeof(state));
    memset(&client_state, 0, sizeof(client_state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(ctx);
    port = test_pick_loopback_port();
    check_greater(port, 0);
    check_greater(snprintf(path, sizeof(path), "pipe://turbo_flow_socket_kcp_%u", port), 0);

    config.context = ctx;
    check_equal(socket_test_context_start(ctx), TURBO_OK);
    config.role = TURBO_FLOW_CORONET_SOCKET_SOURCE;
    config.transport = TURBO_FLOW_CORONET_TRANSPORT_PIPE;
    config.path = path;
    config.timeout_ms = 1000;

    client_state.ctx = ctx;
    client_state.path = path;
    client_state.data = raw;
    client_state.len = sizeof(raw) - 1;
    client_state.rc = TURBO_EBUSY;

    check_equal(turbo_flow_coronet_register_socket_adapter(flow, "socket.pipe", &config),
                 TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "record", flow_record_stage, &state, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(socket_test_context_spawn(ctx, pipe_source_client_task, &client_state), TURBO_OK);

    for (int i = 0; i < 3000 && (!state.called || !client_state.done); ++i) {
      turbo_sleep_ms(1);
    }
    check_equal(client_state.done, 1);
    check_equal(client_state.rc, TURBO_OK);
    check_equal(state.called, 1);
    check_equal(state.payload_len, sizeof(raw) - 1);
    check_equal(state.payload, raw, sizeof(raw) - 1);

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    socket_test_context_stop(ctx);
    turbo_flow_destroy(flow);
    coro_context_destroy(ctx);
  }
#endif

  it("publishes WebSocket source frames into the flow graph") {
    const char raw[] = "ws-frame";
    ws_source_case_result_t result = run_ws_source_case(0);

    check_equal(result.setup_rc, TURBO_OK);
    check_equal(result.stop_rc, TURBO_OK);
    check_equal(result.client_done, 1);
    check_equal(result.client_rc, TURBO_OK);
    check_equal(result.record_called, 1);
    check_equal(result.payload_len, sizeof(raw) - 1u);
    check_equal(result.payload, raw, sizeof(raw) - 1u);
  }

  it("publishes secure WebSocket source frames into the flow graph") {
    const char raw[] = "wss-frame";
    ws_source_case_result_t result = run_ws_source_case(1);

    check_equal(result.setup_rc, TURBO_OK);
    check_equal(result.stop_rc, TURBO_OK);
    check_equal(result.client_done, 1);
    check_equal(result.client_rc, TURBO_OK);
    check_equal(result.record_called, 1);
    check_equal(result.payload_len, sizeof(raw) - 1u);
    check_equal(result.payload, raw, sizeof(raw) - 1u);
  }
}
