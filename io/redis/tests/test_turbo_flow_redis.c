#include "tinytest.h"
#include "turbo_flow_redis.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
typedef SOCKET redis_test_socket_t;
  #define REDIS_TEST_INVALID_SOCKET INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int redis_test_socket_t;
  #define REDIS_TEST_INVALID_SOCKET (-1)
#endif

#define REDIS_TEST_WAIT_ITERATIONS 400
#define REDIS_TEST_STOP_LIMIT_NS UINT64_C(500000000)

typedef struct redis_block_server_s {
  redis_test_socket_t listener;
  atomic_int saw_xreadgroup;
  atomic_int client_closed;
  atomic_int status;
} redis_block_server_t;

typedef enum redis_runtime_server_mode_e {
  REDIS_RUNTIME_XADD = 1,
  REDIS_RUNTIME_XREADGROUP,
  REDIS_RUNTIME_XACK_REPLY_LOST,
  REDIS_RUNTIME_XACK_REPLY_LOST_PENDING,
  REDIS_RUNTIME_XACK_REPLY_LOST_MOVED,
  REDIS_RUNTIME_XREADGROUP_ERROR,
  REDIS_RUNTIME_XGROUP_BUSY,
  REDIS_RUNTIME_SET,
  REDIS_RUNTIME_GET,
  REDIS_RUNTIME_GET_MISSING
} redis_runtime_server_mode_t;

typedef struct redis_runtime_server_s {
  redis_test_socket_t listener;
  redis_runtime_server_mode_t mode;
  atomic_int saw_xadd;
  atomic_int saw_xreadgroup;
  atomic_int saw_xack;
  atomic_int xack_count;
  atomic_int saw_xpending;
  atomic_int payload_matched;
  atomic_int client_closed;
  atomic_int status;
} redis_runtime_server_t;

typedef struct redis_capture_s {
  atomic_int called;
  int result;
  char payload[32];
  size_t payload_len;
} redis_capture_t;

static void redis_test_close_socket(redis_test_socket_t socket_handle) {
  if (socket_handle == REDIS_TEST_INVALID_SOCKET) return;
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
}

static unsigned short redis_test_listener_open(redis_test_socket_t *listener) {
  struct sockaddr_in address;
#ifdef _WIN32
  int address_len = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
#else
  socklen_t address_len = (socklen_t)sizeof(address);
#endif
  if (!listener) return 0;
  *listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (*listener == REDIS_TEST_INVALID_SOCKET) return 0;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(*listener, (struct sockaddr *)&address, sizeof(address)) != 0 ||
      listen(*listener, 1) != 0 ||
      getsockname(*listener, (struct sockaddr *)&address, &address_len) != 0) {
    redis_test_close_socket(*listener);
    *listener = REDIS_TEST_INVALID_SOCKET;
    return 0;
  }
  return ntohs(address.sin_port);
}

static unsigned short redis_block_server_open(redis_block_server_t *server) {
  if (!server) return 0;
  memset(server, 0, sizeof(*server));
  atomic_init(&server->saw_xreadgroup, 0);
  atomic_init(&server->client_closed, 0);
  atomic_init(&server->status, TURBO_EIO);
  return redis_test_listener_open(&server->listener);
}

static void redis_block_server_thread(void *ctx) {
  redis_block_server_t *server = (redis_block_server_t *)ctx;
  redis_test_socket_t client;
  char request[4096];
  size_t used = 0;
  client = accept(server->listener, NULL, NULL);
  if (client == REDIS_TEST_INVALID_SOCKET) return;
  while (used < sizeof(request) - 1u) {
    int received = recv(client, request + used, (int)(sizeof(request) - 1u - used), 0);
    if (received <= 0) break;
    used += (size_t)received;
    request[used] = '\0';
    if (strstr(request, "XREADGROUP")) {
      atomic_store_explicit(&server->saw_xreadgroup, 1, memory_order_release);
      break;
    }
  }
  if (atomic_load_explicit(&server->saw_xreadgroup, memory_order_acquire)) {
    char ignored;
    while (recv(client, &ignored, 1, 0) > 0) {
    }
    atomic_store_explicit(&server->client_closed, 1, memory_order_release);
    atomic_store_explicit(&server->status, TURBO_OK, memory_order_release);
  }
  redis_test_close_socket(client);
}

static int redis_test_send_all(redis_test_socket_t client, const char *data, size_t length) {
  size_t sent = 0u;
  while (sent < length) {
    int amount = send(client, data + sent, (int)(length - sent), 0);
    if (amount <= 0) return TURBO_EIO;
    sent += (size_t)amount;
  }
  return TURBO_OK;
}

static unsigned short redis_runtime_server_open(redis_runtime_server_t *server,
                                                redis_runtime_server_mode_t mode) {
  if (!server) return 0;
  memset(server, 0, sizeof(*server));
  server->mode = mode;
  atomic_init(&server->saw_xadd, 0);
  atomic_init(&server->saw_xreadgroup, 0);
  atomic_init(&server->saw_xack, 0);
  atomic_init(&server->xack_count, 0);
  atomic_init(&server->saw_xpending, 0);
  atomic_init(&server->payload_matched, 0);
  atomic_init(&server->client_closed, 0);
  atomic_init(&server->status, TURBO_EIO);
  return redis_test_listener_open(&server->listener);
}

static void redis_runtime_server_thread(void *ctx) {
  static const char empty_stream_reply[] = "*1\r\n*2\r\n$9\r\nflow:test\r\n*0\r\n";
  static const char stream_reply[] =
      "*1\r\n*2\r\n$9\r\nflow:test\r\n*1\r\n*2\r\n$15\r\n1710000000000-0\r\n"
      "*2\r\n$7\r\npayload\r\n$5\r\nhello\r\n";
  redis_runtime_server_t *server = (redis_runtime_server_t *)ctx;
  redis_test_socket_t client = accept(server->listener, NULL, NULL);
  char request[8192];
  size_t used = 0u;
  int xread_count = 0;
  if (client == REDIS_TEST_INVALID_SOCKET) return;
  while (used < sizeof(request) - 1u) {
    int received = recv(client, request + used, (int)(sizeof(request) - 1u - used), 0);
    if (received <= 0) break;
    used += (size_t)received;
    request[used] = '\0';
    if (server->mode == REDIS_RUNTIME_XADD && strstr(request, "XADD")) {
      atomic_store_explicit(&server->saw_xadd, 1, memory_order_release);
      atomic_store_explicit(&server->payload_matched, strstr(request, "hello") != NULL,
                            memory_order_release);
      if (redis_test_send_all(client, "$15\r\n1710000000000-0\r\n", 22u) != TURBO_OK) break;
      used = 0u;
    } else if (server->mode == REDIS_RUNTIME_SET && strstr(request, "SET")) {
      atomic_store_explicit(&server->payload_matched, strstr(request, "hello") != NULL,
                            memory_order_release);
      if (redis_test_send_all(client, "+OK\r\n", 5u) != TURBO_OK) break;
      used = 0u;
    } else if ((server->mode == REDIS_RUNTIME_GET || server->mode == REDIS_RUNTIME_GET_MISSING) &&
               strstr(request, "GET")) {
      const char *reply = server->mode == REDIS_RUNTIME_GET ? "$5\r\nhello\r\n" : "$-1\r\n";
      size_t reply_size = server->mode == REDIS_RUNTIME_GET ? 11u : 5u;
      if (redis_test_send_all(client, reply, reply_size) != TURBO_OK) break;
      used = 0u;
    } else if (server->mode == REDIS_RUNTIME_XREADGROUP_ERROR && strstr(request, "XREADGROUP")) {
      static const char error_reply[] = "-NOGROUP missing consumer group\r\n";
      atomic_store_explicit(&server->saw_xreadgroup, 1, memory_order_release);
      if (redis_test_send_all(client, error_reply, sizeof(error_reply) - 1u) != TURBO_OK) break;
      used = 0u;
    } else if (server->mode == REDIS_RUNTIME_XGROUP_BUSY && strstr(request, "XGROUP")) {
      static const char error_reply[] = "-BUSYGROUP group already exists\r\n";
      if (redis_test_send_all(client, error_reply, sizeof(error_reply) - 1u) != TURBO_OK) break;
      used = 0u;
    } else if ((server->mode == REDIS_RUNTIME_XREADGROUP ||
                server->mode == REDIS_RUNTIME_XACK_REPLY_LOST ||
                server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_PENDING ||
                server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_MOVED) &&
               strstr(request, "XREADGROUP")) {
      atomic_store_explicit(&server->saw_xreadgroup, 1, memory_order_release);
      used = 0u;
      ++xread_count;
      if (xread_count == 1) {
        if (redis_test_send_all(client, empty_stream_reply, sizeof(empty_stream_reply) - 1u) !=
            TURBO_OK)
          break;
      } else if (xread_count == 2) {
        if (redis_test_send_all(client, stream_reply, sizeof(stream_reply) - 1u) != TURBO_OK) break;
      }
    } else if ((server->mode == REDIS_RUNTIME_XREADGROUP ||
                server->mode == REDIS_RUNTIME_XACK_REPLY_LOST ||
                server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_PENDING ||
                server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_MOVED) &&
               strstr(request, "XACK")) {
      atomic_store_explicit(&server->saw_xack, 1, memory_order_release);
      atomic_fetch_add_explicit(&server->xack_count, 1, memory_order_release);
      used = 0u;
      if (server->mode == REDIS_RUNTIME_XACK_REPLY_LOST ||
          server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_PENDING ||
          server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_MOVED)
        break;
      if (redis_test_send_all(client, ":1\r\n", 4u) != TURBO_OK) break;
    }
  }
  redis_test_close_socket(client);
  client = REDIS_TEST_INVALID_SOCKET;
  if (server->mode == REDIS_RUNTIME_XACK_REPLY_LOST ||
      server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_PENDING ||
      server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_MOVED) {
    client = accept(server->listener, NULL, NULL);
    used = 0u;
    if (client == REDIS_TEST_INVALID_SOCKET) return;
    while (used < sizeof(request) - 1u) {
      int received = recv(client, request + used, (int)(sizeof(request) - 1u - used), 0);
      if (received <= 0) break;
      used += (size_t)received;
      request[used] = '\0';
      if (strstr(request, "XPENDING")) {
        static const char no_pending_reply[] = "*0\r\n";
        static const char pending_reply[] =
            "*1\r\n*4\r\n$15\r\n1710000000000-0\r\n$10\r\nreply-lost\r\n:0\r\n:1\r\n";
        static const char moved_reply[] =
            "*1\r\n*4\r\n$15\r\n1710000000000-0\r\n$5\r\nother\r\n:0\r\n:1\r\n";
        const char *reply = no_pending_reply;
        size_t reply_size = sizeof(no_pending_reply) - 1u;
        if (server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_PENDING) {
          reply = pending_reply;
          reply_size = sizeof(pending_reply) - 1u;
        } else if (server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_MOVED) {
          reply = moved_reply;
          reply_size = sizeof(moved_reply) - 1u;
        }
        atomic_store_explicit(&server->saw_xpending, 1, memory_order_release);
        if (redis_test_send_all(client, reply, reply_size) != TURBO_OK) {
          break;
        }
        used = 0u;
      } else if (server->mode == REDIS_RUNTIME_XACK_REPLY_LOST_PENDING && strstr(request, "XACK")) {
        atomic_fetch_add_explicit(&server->xack_count, 1, memory_order_release);
        if (redis_test_send_all(client, ":1\r\n", 4u) != TURBO_OK) break;
        used = 0u;
      }
    }
  }
  atomic_store_explicit(&server->client_closed, 1, memory_order_release);
  atomic_store_explicit(&server->status, TURBO_OK, memory_order_release);
  redis_test_close_socket(client);
}

static int redis_capture(turbo_flow_msg_t *msg, void *ctx) {
  redis_capture_t *capture = (redis_capture_t *)ctx;
  if (!capture || !msg || msg->payload.len >= sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload[msg->payload.len] = '\0';
  capture->payload_len = msg->payload.len;
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return capture->result;
}

static int redis_noop(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static turbo_flow_redis_stream_config_t redis_config(void) {
  turbo_flow_redis_stream_config_t config;
  memset(&config, 0, sizeof(config));
  config.host = "127.0.0.1";
  config.port = 6379;
  config.stream = "flow:test";
  return config;
}

static int redis_publish_payload(turbo_flow_t *flow, const char *payload) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup(payload);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static void redis_run_stream_source_case(int stage_result, int expect_ack) {
  static const char *dsl = "source events adapter redis.in\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  events -> capture\n"
                           "}\n";
  redis_runtime_server_t server;
  redis_capture_t capture;
  turbo_flow_redis_stream_config_t config = redis_config();
  turbo_thread_t server_thread;
  turbo_flow_t *flow = turbo_flow_create();
  unsigned short port = redis_runtime_server_open(&server, REDIS_RUNTIME_XREADGROUP);
  int saw_ack = 0;
  check_not_null(flow);
  check_true(port > 0);
  check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server), TURBO_OK);
  memset(&capture, 0, sizeof(capture));
  atomic_init(&capture.called, 0);
  capture.result = stage_result;
  config.port = port;
  config.poll_interval_ms = 1u;
  config.group = "workers";
  config.consumer = expect_ack ? "success" : "failure";
  config.block_ms = 1000u;
  check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.in", &config), TURBO_OK);
  check_int_eq(turbo_flow_register_stage_ex(flow, "capture", redis_capture, &capture, NULL),
               TURBO_OK);
  check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  for (int i = 0; i < REDIS_TEST_WAIT_ITERATIONS; ++i) {
    if (atomic_load_explicit(&capture.called, memory_order_acquire) > 0) break;
    turbo_sleep_ms(5);
  }
  check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
  check_size_eq(capture.payload_len, 5u);
  check_str_eq(capture.payload, "hello");
  for (int i = 0; i < REDIS_TEST_WAIT_ITERATIONS; ++i) {
    if (atomic_load_explicit(&server.saw_xack, memory_order_acquire)) {
      saw_ack = 1;
      break;
    }
    turbo_sleep_ms(1);
  }
  check_int_eq(saw_ack, expect_ack);
  check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
  check_int_eq(atomic_load_explicit(&server.saw_xreadgroup, memory_order_acquire), 1);
  check_int_eq(atomic_load_explicit(&server.client_closed, memory_order_acquire), 1);
  check_int_eq(atomic_load_explicit(&server.status, memory_order_acquire), TURBO_OK);
  redis_test_close_socket(server.listener);
  turbo_flow_destroy(flow);
#ifdef _WIN32
  WSACleanup();
#endif
}

static void redis_run_lost_xack_case(redis_runtime_server_mode_t mode, int expected_retry_status,
                                     int expected_xack_count) {
  redis_runtime_server_t server;
  turbo_thread_t server_thread;
  turbo_flow_redis_stream_config_t config = redis_config();
  turbo_flow_redis_stream_owner_t *owner = NULL;
  turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
  unsigned short port = redis_runtime_server_open(&server, mode);
  int rc;
  check_true(port > 0);
  check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server), TURBO_OK);
  config.port = port;
  config.group = "workers";
  config.consumer = "reply-lost";
  config.block_ms = 10u;
  check_int_eq(turbo_flow_redis_stream_owner_create(&config, &owner), TURBO_OK);
  check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &claim), TURBO_OK);
  check_true(claim.token != 0u);
  rc = turbo_flow_redis_stream_owner_ack(owner, claim.token);
  check_true(rc != TURBO_OK);
  check_int_eq(atomic_load_explicit(&server.saw_xack, memory_order_acquire), 1);
  check_int_eq(turbo_flow_redis_stream_owner_ack(owner, claim.token), expected_retry_status);
  check_int_eq(atomic_load_explicit(&server.saw_xpending, memory_order_acquire), 1);
  check_int_eq(atomic_load_explicit(&server.xack_count, memory_order_acquire), expected_xack_count);
  if (expected_retry_status == TURBO_OK)
    check_int_eq(turbo_flow_redis_stream_owner_ack(owner, claim.token), TURBO_EALREADY);
  else check_int_eq(turbo_flow_redis_stream_owner_requeue(owner, claim.token), TURBO_EBUSY);
  turbo_flow_redis_stream_owner_destroy(owner);
  check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
  check_int_eq(atomic_load_explicit(&server.status, memory_order_acquire), TURBO_OK);
  redis_test_close_socket(server.listener);
#ifdef _WIN32
  WSACleanup();
#endif
}

spec("turbo_flow_redis") {
  it("propagates Redis Stream errors instead of reporting an empty queue") {
    redis_runtime_server_t server;
    turbo_thread_t server_thread;
    turbo_flow_redis_stream_config_t config = redis_config();
    turbo_flow_redis_stream_owner_t *owner = NULL;
    turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
    unsigned short port = redis_runtime_server_open(&server, REDIS_RUNTIME_XREADGROUP_ERROR);
    check_true(port > 0);
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    config.port = port;
    config.group = "missing";
    config.consumer = "worker";
    config.block_ms = 10u;
    check_int_eq(turbo_flow_redis_stream_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(turbo_flow_redis_stream_owner_claim(owner, &claim), TURBO_EIO);
    turbo_flow_redis_stream_owner_destroy(owner);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&server.saw_xreadgroup, memory_order_acquire), 1);
    redis_test_close_socket(server.listener);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("keeps BUSYGROUP idempotency in the Flow owner policy") {
    redis_runtime_server_t server;
    turbo_thread_t server_thread;
    turbo_flow_redis_stream_config_t config = redis_config();
    turbo_flow_redis_stream_owner_t *owner = NULL;
    unsigned short port = redis_runtime_server_open(&server, REDIS_RUNTIME_XGROUP_BUSY);
    check_true(port > 0);
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    config.port = port;
    config.group = "existing";
    config.consumer = "worker";
    config.create_group = 1;
    check_int_eq(turbo_flow_redis_stream_owner_create(&config, &owner), TURBO_OK);
    turbo_flow_redis_stream_owner_destroy(owner);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    redis_test_close_socket(server.listener);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("reconciles a lost XACK reply against the Redis PEL") {
    redis_run_lost_xack_case(REDIS_RUNTIME_XACK_REPLY_LOST, TURBO_OK, 1);
  }

  it("retries XACK only while the same consumer still owns the PEL entry") {
    redis_run_lost_xack_case(REDIS_RUNTIME_XACK_REPLY_LOST_PENDING, TURBO_OK, 2);
  }

  it("does not XACK a PEL entry that moved to another consumer") {
    redis_run_lost_xack_case(REDIS_RUNTIME_XACK_REPLY_LOST_MOVED, TURBO_EBUSY, 1);
  }

  it("registers Stream and Data patterns from the installed YAML example") {
    char path[1024];
    char *yaml;
    size_t yaml_len = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;
    const char *adapter_name = NULL;
    check_not_null(flow);
    (void)snprintf(path, sizeof(path), "%s/examples/redis.yml", TURBO_FLOW_REDIS_SOURCE_DIR);
    yaml = tt_read_file(path, &yaml_len);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_len, &resolved, &error), TURBO_OK);
    check_int_eq(
        turbo_flow_redis_register_resolved_adapter(flow, "redis.events.in", resolved, &error),
        TURBO_OK);
    check_int_eq(
        turbo_flow_redis_register_resolved_adapter(flow, "redis.events.out", resolved, &error),
        TURBO_OK);
    check_int_eq(
        turbo_flow_redis_register_resolved_adapter(flow, "redis.checkpoint.set", resolved, &error),
        TURBO_OK);
    check_int_eq(
        turbo_flow_redis_register_resolved_adapter(flow, "redis.checkpoint.get", resolved, &error),
        TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "redis.events.in");
    check_not_null(schema);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    schema = turbo_flow_find_adapter_schema(flow, "redis.events.out");
    check_not_null(schema);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    schema = turbo_flow_find_adapter_schema(flow, "redis.checkpoint.set");
    check_not_null(schema);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    schema = turbo_flow_find_adapter_schema(flow, "redis.checkpoint.get");
    check_not_null(schema);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_TRANSFORM);
    check_int_eq(
        turbo_flow_resolved_config_profile_adapter(resolved, "worker", "events_in", &adapter_name),
        TURBO_OK);
    check_str_eq(adapter_name, "redis.events.in");
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
    free(yaml);
  }

  it("rejects fields that belong to a different Redis pattern") {
    static const char yaml[] = "version: 1\nadapters:\n  redis.bad:\n    kind: redis\n    config:\n"
                               "      pattern: data\n      operation: set\n      host: 127.0.0.1\n"
                               "      port: 6379\n      key: flow:data\n      stream: misplaced\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_register_resolved_adapter(flow, "redis.bad", resolved, &error),
                 TURBO_EINVAL);
    check_str_eq(error.path, "$.adapters.redis.bad.config.stream");
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("registers XADD output schema") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_redis_stream_config_t config = redis_config();
    const turbo_flow_adapter_schema_t *schema;
    turbo_flow_connection_snapshot_t connection;
    check_not_null(flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.out", &config), TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "redis.out");
    check_not_null(schema);
    check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_REDIS);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    check_int_eq(schema->direction, TURBO_FLOW_ADAPTER_OUTPUT);
    memset(&connection, 0, sizeof(connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_OK);
    check_int_eq(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_str_eq(connection.endpoint, "redis://127.0.0.1:6379/0");
    turbo_flow_destroy(flow);
  }

  it("registers XREADGROUP input schema and compiles a source") {
    static const char *dsl = "source events adapter redis.in\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  events -> capture\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_redis_stream_config_t config = redis_config();
    const turbo_flow_adapter_schema_t *schema;
    config.poll_interval_ms = 10;
    config.group = "workers";
    config.consumer = "test-1";
    check_not_null(flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.in", &config), TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "redis.in");
    check_not_null(schema);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    check_int_eq(schema->direction, TURBO_FLOW_ADAPTER_INPUT);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", redis_noop, NULL, NULL), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects input mode without consumer group identity") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_redis_stream_config_t config = redis_config();
    config.poll_interval_ms = 10;
    check_not_null(flow);
    check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.in", &config), TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("delivers XADD payloads to the Redis Streams runtime") {
    static const char *dsl = "source input\n"
                             "stage append adapter redis.out\n"
                             "stage main {\n"
                             "  input -> append\n"
                             "}\n";
    redis_runtime_server_t server;
    turbo_flow_redis_stream_config_t config = redis_config();
    turbo_thread_t server_thread;
    turbo_flow_t *flow = turbo_flow_create();
    unsigned short port = redis_runtime_server_open(&server, REDIS_RUNTIME_XADD);
    check_not_null(flow);
    check_true(port > 0);
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    config.port = port;
    check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.out", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(redis_publish_payload(flow, "hello"), TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&server.saw_xadd, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&server.payload_matched, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&server.status, memory_order_acquire), TURBO_OK);
    redis_test_close_socket(server.listener);
    turbo_flow_destroy(flow);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("acknowledges XREADGROUP only after successful graph delivery") {
    redis_run_stream_source_case(TURBO_OK, 1);
  }

  it("does not acknowledge XREADGROUP after downstream failure") {
    redis_run_stream_source_case(TURBO_EIO, 0);
  }

  it("supports binary-safe Redis SET sinks and GET transforms") {
    static const char *set_dsl = "source input\n"
                                 "stage store adapter redis.data\n"
                                 "stage main {\n"
                                 "  input -> store\n"
                                 "}\n";
    static const char *get_dsl = "source input\n"
                                 "stage load adapter redis.data\n"
                                 "stage capture\n"
                                 "stage main {\n"
                                 "  input -> load -> capture\n"
                                 "}\n";
    redis_runtime_server_t server;
    redis_capture_t capture;
    turbo_flow_redis_data_config_t config;
    turbo_thread_t server_thread;
    turbo_flow_t *flow;
    unsigned short port;

    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.database = 0;
    config.key = "flow:data";
    config.operation = TURBO_FLOW_REDIS_DATA_SET;
    config.max_value_size = 64u;
    port = redis_runtime_server_open(&server, REDIS_RUNTIME_SET);
    flow = turbo_flow_create();
    check_true(port > 0);
    check_not_null(flow);
    config.port = port;
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_register_data_adapter(flow, "redis.data", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, set_dsl, strlen(set_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(redis_publish_payload(flow, "hello"), TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&server.payload_matched, memory_order_acquire), 1);
    redis_test_close_socket(server.listener);
    turbo_flow_destroy(flow);
#ifdef _WIN32
    WSACleanup();
#endif

    port = redis_runtime_server_open(&server, REDIS_RUNTIME_GET);
    flow = turbo_flow_create();
    check_true(port > 0);
    check_not_null(flow);
    config.port = port;
    config.operation = TURBO_FLOW_REDIS_DATA_GET;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    capture.result = TURBO_OK;
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_register_data_adapter(flow, "redis.data", &config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", redis_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, get_dsl, strlen(get_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(redis_publish_payload(flow, "trigger"), TURBO_OK);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_str_eq(capture.payload, "hello");
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    redis_test_close_socket(server.listener);
    turbo_flow_destroy(flow);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("exposes Redis SET GET as one atomic fixed-key blob store") {
    redis_runtime_server_t server;
    turbo_thread_t server_thread;
    turbo_flow_redis_blob_store_config_t config;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    char yaml[1024];
    char resolved_key[TURBO_FLOW_REDIS_MAX_KEY_SIZE + 1u];
    uint8_t loaded[8];
    size_t loaded_size = 0u;
    unsigned short port;

    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.database = 0;
    config.timeout_ms = 2000u;
    config.key = "fmq:management:test";
    config.max_value_size = sizeof(loaded);

    port = redis_runtime_server_open(&server, REDIS_RUNTIME_SET);
    check_true(port > 0);
    config.port = port;
    check_true(snprintf(yaml, sizeof(yaml),
                        "version: 1\nchannels:\n  operations:\n    kind: blob_store\n"
                        "    config:\n      backend: redis\n      host: 127.0.0.1\n"
                        "      port: %u\n      database: 0\n      timeout_ms: 2000\n"
                        "      key: fmq:management:test\n      max_value_size: 8\n"
                        "adapters: {}\n",
                        (unsigned int)port) > 0);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_blob_store_create_resolved(
                     resolved, "operations", &store, resolved_key, sizeof(resolved_key), &error),
                 TURBO_OK);
    check_str_eq(resolved_key, config.key);
    turbo_flow_resolved_config_destroy(resolved);
    check_int_eq(store.commit(store.ctx, resolved_key, (const uint8_t *)"hello", 5u), TURBO_OK);
    check_int_eq(store.commit(store.ctx, "wrong-key", (const uint8_t *)"hello", 5u), TURBO_EINVAL);
    turbo_flow_redis_blob_store_destroy(&store);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&server.payload_matched, memory_order_acquire), 1);
    redis_test_close_socket(server.listener);
#ifdef _WIN32
    WSACleanup();
#endif

    port = redis_runtime_server_open(&server, REDIS_RUNTIME_GET);
    check_true(port > 0);
    config.port = port;
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_blob_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.load(store.ctx, config.key, loaded, sizeof(loaded), &loaded_size), TURBO_OK);
    check_size_eq(loaded_size, 5u);
    check_mem_eq(loaded, "hello", 5u);
    turbo_flow_redis_blob_store_destroy(&store);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    redis_test_close_socket(server.listener);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("declines a foreign record-store backend before validating provider fields") {
    static const char yaml[] =
        "version: 1\nchannels:\n  mqtt.sessions:\n    kind: record_store\n"
        "    config:\n      backend: sqlite\n      database_path: ':memory:'\n"
        "      namespace_name: mqtt.sessions\nadapters: {}\n";
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_redis_record_store_create_resolved(resolved, "mqtt.sessions", &store, &error),
        TURBO_ENOTSUP);
    check_str_eq(error.path, "$.channels.mqtt.sessions.config.backend");
    check_null(store.ctx);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("fails fast for invalid Redis data configuration, oversized SET, and missing GET keys") {
    static const char *set_dsl = "source input\n"
                                 "stage store adapter redis.data\n"
                                 "stage main {\n"
                                 "  input -> store\n"
                                 "}\n";
    static const char *get_dsl = "source input\n"
                                 "stage load adapter redis.data\n"
                                 "stage main {\n"
                                 "  input -> load\n"
                                 "}\n";
    redis_runtime_server_t server;
    turbo_flow_redis_data_config_t config;
    turbo_thread_t server_thread;
    turbo_flow_t *flow = turbo_flow_create();
    unsigned short port;
    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.host = "127.0.0.1";
    config.port = 6379u;
    config.key = "flow:data";
    config.operation = (turbo_flow_redis_data_operation_t)0;
    check_int_eq(turbo_flow_redis_register_data_adapter(flow, "redis.data", &config), TURBO_EINVAL);
    config.operation = TURBO_FLOW_REDIS_DATA_SET;
    config.max_value_size = 3u;
    check_int_eq(turbo_flow_redis_register_data_adapter(flow, "redis.data", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, set_dsl, strlen(set_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(redis_publish_payload(flow, "large"), TURBO_EMSGSIZE);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    port = redis_runtime_server_open(&server, REDIS_RUNTIME_GET_MISSING);
    flow = turbo_flow_create();
    check_true(port > 0);
    check_not_null(flow);
    config.port = port;
    config.operation = TURBO_FLOW_REDIS_DATA_GET;
    config.max_value_size = 64u;
    check_int_eq(turbo_thread_create(&server_thread, redis_runtime_server_thread, &server),
                 TURBO_OK);
    check_int_eq(turbo_flow_redis_register_data_adapter(flow, "redis.data", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, get_dsl, strlen(get_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(redis_publish_payload(flow, "trigger"), TURBO_ENOENT);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    redis_test_close_socket(server.listener);
    turbo_flow_destroy(flow);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("interrupts a blocked XREADGROUP when the source flow stops") {
    static const char *dsl = "source events adapter redis.in\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  events -> capture\n"
                             "}\n";
    redis_block_server_t server;
    turbo_flow_redis_stream_config_t config = redis_config();
    turbo_flow_connection_snapshot_t connection;
    turbo_thread_t server_thread;
    turbo_flow_t *flow = turbo_flow_create();
    unsigned short port = redis_block_server_open(&server);
    uint64_t stop_started_ns;
    int saw_block = 0;
    check_not_null(flow);
    check_true(port > 0);
    check_int_eq(turbo_thread_create(&server_thread, redis_block_server_thread, &server), TURBO_OK);
    config.port = port;
    config.timeout_ms = 5000;
    config.poll_interval_ms = 1;
    config.group = "workers";
    config.consumer = "stop-test";
    config.block_ms = 60000;
    check_int_eq(turbo_flow_redis_register_stream_adapter(flow, "redis.in", &config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "capture", redis_noop, NULL, NULL), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    for (int i = 0; i < REDIS_TEST_WAIT_ITERATIONS; ++i) {
      if (atomic_load_explicit(&server.saw_xreadgroup, memory_order_acquire)) {
        saw_block = 1;
        break;
      }
      turbo_sleep_ms(5);
    }
    check_int_eq(saw_block, 1);
    stop_started_ns = turbo_hrtime();
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_true(turbo_hrtime() - stop_started_ns < REDIS_TEST_STOP_LIMIT_NS);
    memset(&connection, 0, sizeof(connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_OK);
    check_int_eq(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(connection.last_status, TURBO_ESHUTDOWN);
    check_int_eq(turbo_thread_join(&server_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&server.client_closed, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&server.status, memory_order_acquire), TURBO_OK);
    redis_test_close_socket(server.listener);
    turbo_flow_destroy(flow);
#ifdef _WIN32
    WSACleanup();
#endif
  }

}
