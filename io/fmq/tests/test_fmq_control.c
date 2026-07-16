#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_fmq.h"
#include "turbo_flow_fmq_control.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
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

typedef struct control_capture_s {
  atomic_int called;
  uint8_t payload[TURBO_FLOW_FMQ_CONTROL_REPLY_MAX_SIZE];
  size_t payload_len;
} control_capture_t;

static int control_noop(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static int control_capture(turbo_flow_msg_t *msg, void *ctx) {
  control_capture_t *capture = (control_capture_t *)ctx;
  if (!msg || !capture || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static turbo_flow_t *control_target(void) {
  static const char *dsl = "source input\n"
                           "stage sink\n"
                           "stage main {\n"
                           "  input -> sink\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_register_stage_ex(flow, "sink", control_noop, NULL, NULL) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK || turbo_flow_start(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_fmq_control_request_t control_request(uint64_t id, const char *key,
                                                        turbo_flow_control_kind_t kind) {
  turbo_flow_fmq_control_request_t request = TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
  request.operation = TURBO_FLOW_FMQ_CONTROL_EXECUTE;
  request.request_id = id;
  memcpy(request.target, "data-plane", sizeof("data-plane"));
  memcpy(request.idempotency_key, key, strlen(key) + 1u);
  request.command.size = sizeof(request.command);
  request.command.kind = kind;
  return request;
}

static turbo_flow_fmq_control_config_t control_config(void) {
  turbo_flow_fmq_control_config_t config = TURBO_FLOW_FMQ_CONTROL_CONFIG_INIT;
  memcpy(config.target, "data-plane", sizeof("data-plane"));
  return config;
}

static int control_publish(turbo_flow_t *flow, const uint8_t *data, size_t len) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(data, len);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static unsigned short control_test_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  SOCKET handle = INVALID_SOCKET;
  int address_len = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0u;
#else
  int handle = -1;
  socklen_t address_len = (socklen_t)sizeof(address);
#endif
  unsigned short port = 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (handle == INVALID_SOCKET) return 0u;
#else
  if (handle < 0) return 0u;
#endif
  if (bind(handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(handle, (struct sockaddr *)&address, &address_len) == 0)
    port = ntohs(address.sin_port);
#ifdef _WIN32
  closesocket(handle);
#else
  close(handle);
#endif
  return port;
}

spec("fmq_control_v1") {
  it("round trips typed request and reply envelopes and rejects reserved bytes") {
    turbo_flow_fmq_control_request_t request =
        control_request(42u, "pause-42", TURBO_FLOW_CONTROL_PAUSE);
    turbo_flow_fmq_control_request_t decoded = TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
    turbo_flow_fmq_control_reply_t reply = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    turbo_flow_fmq_control_reply_t decoded_reply = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    uint8_t bytes[TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE];
    size_t len = 0u;
    check_int_eq(turbo_flow_fmq_control_request_encode(&request, bytes, sizeof(bytes), &len),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_control_request_decode(bytes, len, &decoded), TURBO_OK);
    check_uint_eq(decoded.request_id, 42u);
    check_str_eq(decoded.target, "data-plane");
    check_int_eq(decoded.command.kind, TURBO_FLOW_CONTROL_PAUSE);
    bytes[52] = 1u;
    check_int_eq(turbo_flow_fmq_control_request_decode(bytes, len, &decoded), TURBO_EPROTO);

    request = control_request(43u, "replace-43", TURBO_FLOW_CONTROL_ADAPTER);
    request.command.adapter_kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    memcpy(request.command.target, "fmq.events.pub", sizeof("fmq.events.pub"));
    memcpy(request.command.endpoint_host, "127.0.0.1", sizeof("127.0.0.1"));
    memcpy(request.command.endpoint_path, "/control", sizeof("/control"));
    request.command.endpoint_port = 8800;
    memcpy(request.command.condition, "runtime.accepting", sizeof("runtime.accepting"));
    check_int_eq(turbo_flow_fmq_control_request_encode(&request, bytes, sizeof(bytes), &len),
                 TURBO_OK);
    decoded = (turbo_flow_fmq_control_request_t)TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
    check_int_eq(turbo_flow_fmq_control_request_decode(bytes, len, &decoded), TURBO_OK);
    check_int_eq(decoded.command.adapter_kind, TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT);
    check_str_eq(decoded.command.target, "fmq.events.pub");
    check_str_eq(decoded.command.endpoint_path, "/control");
    check_int_eq(decoded.command.endpoint_port, 8800);

    reply.request_id = 42u;
    reply.status = TURBO_EBUSY;
    reply.runtime.state = TURBO_FLOW_STATE_STARTED;
    reply.runtime.accepting_publishes = 1;
    reply.runtime.active_publishes = 3u;
    reply.runtime.stage_count = 4u;
    reply.runtime.edge_count = 5u;
    reply.runtime.adapter_count = 6u;
    reply.runtime.pool_count = 7u;
    reply.error.code = TURBO_EBUSY;
    memcpy(reply.error.message, "busy", sizeof("busy"));
    check_int_eq(turbo_flow_fmq_control_reply_encode(&reply, bytes, sizeof(bytes), &len), TURBO_OK);
    check_int_eq(turbo_flow_fmq_control_reply_decode(bytes, len, &decoded_reply), TURBO_OK);
    check_int_eq(decoded_reply.status, TURBO_EBUSY);
    check_true(decoded_reply.runtime.accepting_publishes);
    check_uint_eq(decoded_reply.runtime.active_publishes, 3u);
    check_size_eq(decoded_reply.runtime.stage_count, 4u);
    check_size_eq(decoded_reply.runtime.pool_count, 7u);
    check_str_eq(decoded_reply.error.message, "busy");
  }

  it("separates protocol dispatch from command status and replays idempotently") {
    turbo_flow_t *target = control_target();
    turbo_flow_fmq_control_config_t config = control_config();
    turbo_flow_fmq_control_service_t *service = NULL;
    turbo_flow_fmq_control_request_t request =
        control_request(7u, "pause-7", TURBO_FLOW_CONTROL_PAUSE);
    turbo_flow_fmq_control_reply_t reply = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    turbo_flow_fmq_control_request_t conflict;
    uint8_t bytes[TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE];
    size_t len = 0u;
    check_not_null(target);
    check_int_eq(turbo_flow_fmq_control_service_create(target, &config, &service), TURBO_OK);
    check_int_eq(turbo_flow_fmq_control_request_encode(&request, bytes, sizeof(bytes), &len),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_control_service_execute(service, bytes, len, &reply), TURBO_OK);
    check_int_eq(reply.status, TURBO_OK);
    check_false(reply.runtime.accepting_publishes);
    reply = (turbo_flow_fmq_control_reply_t)TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    check_int_eq(turbo_flow_fmq_control_service_execute(service, bytes, len, &reply), TURBO_OK);
    check_true(reply.replayed);

    conflict = control_request(7u, "pause-7", TURBO_FLOW_CONTROL_RESUME);
    check_int_eq(turbo_flow_fmq_control_request_encode(&conflict, bytes, sizeof(bytes), &len),
                 TURBO_OK);
    reply = (turbo_flow_fmq_control_reply_t)TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    check_int_eq(turbo_flow_fmq_control_service_execute(service, bytes, len, &reply), TURBO_OK);
    check_int_eq(reply.status, TURBO_EPROTO);
    reply = (turbo_flow_fmq_control_reply_t)TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    check_int_eq(turbo_flow_fmq_control_service_execute(service, NULL, 0u, &reply), TURBO_OK);
    check_int_eq(reply.status, TURBO_EPROTO);

    request = (turbo_flow_fmq_control_request_t)TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
    request.operation = TURBO_FLOW_FMQ_CONTROL_STATUS;
    request.request_id = 8u;
    memcpy(request.target, "data-plane", sizeof("data-plane"));
    check_int_eq(turbo_flow_fmq_control_request_encode(&request, bytes, sizeof(bytes), &len),
                 TURBO_OK);
    reply = (turbo_flow_fmq_control_reply_t)TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    check_int_eq(turbo_flow_fmq_control_service_execute(service, bytes, len, &reply), TURBO_OK);
    check_int_eq(reply.status, TURBO_OK);
    check_false(reply.runtime.accepting_publishes);
    check_size_gt(reply.runtime.stage_count, 0u);

    reply = (turbo_flow_fmq_control_reply_t)TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    check_int_eq(
        turbo_flow_fmq_control_service_execute(service, (const uint8_t *)"bad", 3u, &reply),
        TURBO_OK);
    check_int_eq(reply.status, TURBO_EPROTO);
    turbo_flow_fmq_control_service_destroy(service);
    check_int_eq(turbo_flow_stop(target), TURBO_OK);
    turbo_flow_destroy(target);
  }

  it("resolves the bounded service policy from YAML") {
    static const char *yaml = "version: 1\n"
                              "channels:\n"
                              "  control:\n"
                              "    kind: fmq_control\n"
                              "    config:\n"
                              "      protocol_version: 1\n"
                              "      target: data-plane\n"
                              "      dedup_capacity: 32\n"
                              "      max_request_bytes: 4096\n"
                              "adapters:\n"
                              "  placeholder:\n"
                              "    kind: test\n"
                              "    config:\n"
                              "      enabled: true\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_fmq_control_config_t config = TURBO_FLOW_FMQ_CONTROL_CONFIG_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
    check_int_eq(turbo_flow_fmq_control_config_resolve(resolved, "control", &config, &error),
                 TURBO_OK);
    check_str_eq(config.target, "data-plane");
    check_size_eq(config.dedup_capacity, 32u);
    check_size_eq(config.max_request_bytes, 4096u);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("executes a remote control request over the ordinary TCP REQ REP graph") {
    static const char *server_dsl = "source request adapter fmq.rep\n"
                                    "stage control\n"
                                    "stage reply adapter fmq.rep\n"
                                    "stage main {\n"
                                    "  request -> control -> reply\n"
                                    "}\n";
    static const char *client_dsl = "source response adapter fmq.req\n"
                                    "source input\n"
                                    "stage send adapter fmq.req\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "  response -> capture\n"
                                    "}\n";
    unsigned short port = control_test_port();
    turbo_flow_fmq_config_t rep = TURBO_FLOW_FMQ_CONFIG_INIT;
    turbo_flow_fmq_config_t req = TURBO_FLOW_FMQ_CONFIG_INIT;
    turbo_flow_fmq_control_config_t service_config = control_config();
    turbo_flow_fmq_control_service_t *service = NULL;
    turbo_flow_fmq_control_request_t request =
        control_request(99u, "pause-99", TURBO_FLOW_CONTROL_PAUSE);
    turbo_flow_fmq_control_reply_t reply = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
    control_capture_t capture;
    turbo_flow_t *target = control_target();
    turbo_flow_t *server = turbo_flow_create();
    turbo_flow_t *client = turbo_flow_create();
    uint8_t bytes[TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE];
    size_t len = 0u;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    check_int_gt(port, 0);
    check_not_null(target);
    check_not_null(server);
    check_not_null(client);
    rep.pattern = TURBO_FLOW_FMQ_REP;
    rep.mode = TURBO_FLOW_FMQ_BIND;
    rep.transport = TURBO_FLOW_FMQ_TCP;
    rep.host = "127.0.0.1";
    rep.port = (int)port;
    rep.timeout_ms = 2000u;
    req = rep;
    req.pattern = TURBO_FLOW_FMQ_REQ;
    req.mode = TURBO_FLOW_FMQ_CONNECT;
    check_int_eq(turbo_flow_fmq_control_service_create(target, &service_config, &service),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter(server, "fmq.rep", &rep), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(server, "control", turbo_flow_fmq_control_stage,
                                              service, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(server, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server), TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter(client, "fmq.req", &req), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(client, "capture", control_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client), TURBO_OK);
    check_int_eq(turbo_flow_start(server), TURBO_OK);
    check_int_eq(turbo_flow_start(client), TURBO_OK);
    check_int_eq(turbo_flow_fmq_control_request_encode(&request, bytes, sizeof(bytes), &len),
                 TURBO_OK);
    check_int_eq(control_publish(client, bytes, len), TURBO_OK);
    for (int i = 0; i < 400 && atomic_load_explicit(&capture.called, memory_order_acquire) == 0;
         ++i)
      turbo_sleep_ms(5u);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(turbo_flow_fmq_control_reply_decode(capture.payload, capture.payload_len, &reply),
                 TURBO_OK);
    check_uint_eq(reply.request_id, 99u);
    check_int_eq(reply.status, TURBO_OK);
    check_false(reply.runtime.accepting_publishes);
    check_int_eq(turbo_flow_stop(client), TURBO_OK);
    check_int_eq(turbo_flow_stop(server), TURBO_OK);
    turbo_flow_destroy(client);
    turbo_flow_destroy(server);
    turbo_flow_fmq_control_service_destroy(service);
    check_int_eq(turbo_flow_stop(target), TURBO_OK);
    turbo_flow_destroy(target);
  }
}
