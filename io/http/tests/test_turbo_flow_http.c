#include "data_bind.h"
#include "flow_http_acl_internal.h"
#include "flow_http_auth_internal.h"
#include "turbo_flow_http_acl.h"
#include "turbo_flow_http_auth.h"
#include "turbo_flow_http_client.h"
#include "turbo_flow_http_server.h"
#include "turbo_flow_observe.h"

#include "CoroNet/turbo_coro_context.h"
#include "mtls_test_server.h"
#include "tinytest.h"
#include "tls_test_pki.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
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

#define HTTP_TEST_DRAIN_RETRY_COUNT 200u
#define HTTP_TEST_DRAIN_RETRY_DELAY_MS 10u

typedef struct http_capture_s {
  char payload[128];
  size_t payload_len;
  atomic_int called;
  turbo_flow_content_profile_t profile;
  turbo_flow_data_encoding_t encoding;
  char media_type[TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX + 1u];
} http_capture_t;

typedef struct http_auth_secret_fixture_s {
  int acquire_calls;
  int release_calls;
} http_auth_secret_fixture_t;

static int http_auth_secret_acquire(void *ctx, const char *reference,
                                    turbo_flow_security_secret_lease_t *lease) {
  static const uint8_t token[] = "service-token";
  http_auth_secret_fixture_t *fixture = (http_auth_secret_fixture_t *)ctx;
  if (!fixture || !reference || strcmp(reference, "env://TURBO_FLOW_AUTH_TOKEN") != 0 || !lease)
    return TURBO_ENOENT;
  ++fixture->acquire_calls;
  lease->bytes = token;
  lease->byte_count = sizeof(token) - 1u;
  lease->version = 1u;
  lease->provider_lease = fixture;
  return TURBO_OK;
}

static void http_auth_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  http_auth_secret_fixture_t *fixture = (http_auth_secret_fixture_t *)ctx;
  if (fixture) ++fixture->release_calls;
  if (lease) *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}

typedef struct http_mtls_auth_task_s {
  const turbo_flow_security_auth_provider_t *provider;
  turbo_flow_security_auth_request_t request;
  turbo_flow_security_principal_t principal;
  atomic_int done;
  int status;
} http_mtls_auth_task_t;

typedef struct http_mtls_acl_task_s {
  const turbo_flow_security_authorization_provider_t *provider;
  turbo_flow_security_principal_t principal;
  turbo_flow_security_request_t request;
  turbo_flow_security_decision_t decision;
  atomic_int done;
  int status;
} http_mtls_acl_task_t;

static void http_mtls_authenticate_task(coro_t *coroutine, void *arg) {
  http_mtls_auth_task_t *task = (http_mtls_auth_task_t *)arg;
  (void)coroutine;
  task->status = turbo_flow_security_authenticate(task->provider, &task->request, &task->principal);
  atomic_store_explicit(&task->done, 1, memory_order_release);
}

static void http_mtls_acl_load_task(coro_t *coroutine, void *arg) {
  http_mtls_acl_task_t *task = (http_mtls_acl_task_t *)arg;
  (void)coroutine;
  task->status = task->provider->authorize(task->provider->ctx, &task->request, 1000u,
                                           &task->decision);
  atomic_store_explicit(&task->done, 1, memory_order_release);
}

static int http_auth_run_response_case_delayed(const uint8_t *response, size_t response_size,
                                               uint32_t response_delay_ms, const char *ca_file,
                                               const char *cert_file, const char *key_file,
                                               turbo_flow_security_principal_t *principal_out) {
  flow_mtls_test_server_t server;
  http_auth_secret_fixture_t fixture = {0};
  turbo_flow_http_auth_provider_config_t config = TURBO_FLOW_HTTP_AUTH_PROVIDER_CONFIG_INIT;
  turbo_flow_http_auth_provider_t *provider = NULL;
  http_mtls_auth_task_t task;
  coro_context_t *context = NULL;
  char url[160];
  int rc;
  if (!ca_file || !cert_file || !key_file || !principal_out) return TURBO_EINVAL;
  memset(&task, 0, sizeof(task));
  atomic_init(&task.done, 0);
  task.status = TURBO_EBUSY;
  if (flow_mtls_test_server_start_delayed(&server, response, response_size, response_delay_ms) != 0)
    return TURBO_EIO;
  if (snprintf(url, sizeof(url), "https://localhost:%u/v4/authenticate", server.port) <= 0) {
    flow_mtls_test_server_join(&server);
    return TURBO_EIO;
  }
  config.url = url;
  config.method = "password";
  config.service_id = "broker-main";
  config.service_domain = "root-a";
  config.service_token_ref = "env://TURBO_FLOW_AUTH_TOKEN";
  config.timeout_ms = 250u;
  config.key_provider =
      (turbo_flow_security_key_provider_t){sizeof(turbo_flow_security_key_provider_t), &fixture,
                                           http_auth_secret_acquire, http_auth_secret_release};
  config.tls.ca_file = ca_file;
  config.tls.client_cert_file = cert_file;
  config.tls.client_key_file = key_file;
  rc = turbo_flow_http_auth_provider_create(&config, &provider);
  if (rc != TURBO_OK) goto done;
  task.provider = turbo_flow_http_auth_provider_interface(provider);
  task.request = (turbo_flow_security_auth_request_t)TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
  task.principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  task.request.identity = "device-a";
  task.request.method = "password";
  task.request.secret = (const uint8_t *)"secret";
  task.request.secret_size = sizeof("secret") - 1u;
  task.request.remote_address = "127.0.0.1";
  task.request.protocol = "mqtt5";
  task.request.peer_certificate_sha256 =
      "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  context = coro_context_create(NULL);
  if (!context) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = coro_context_spawn(context, http_mtls_authenticate_task, &task);
  while (rc == TURBO_OK && !atomic_load_explicit(&task.done, memory_order_acquire))
    rc = coro_context_run(context, TURBO_RUN_ONCE);
  if (rc == TURBO_OK) rc = task.status;
  *principal_out = task.principal;

done:
  if (context) coro_context_destroy(context);
  turbo_flow_http_auth_provider_destroy(provider);
  flow_mtls_test_server_join(&server);
  return rc;
}

static int http_auth_run_response_case(const uint8_t *response, size_t response_size,
                                       const char *ca_file, const char *cert_file,
                                       const char *key_file,
                                       turbo_flow_security_principal_t *principal_out) {
  return http_auth_run_response_case_delayed(response, response_size, 0u, ca_file, cert_file,
                                             key_file, principal_out);
}

static int capture_response(turbo_flow_msg_t *msg, void *ctx) {
  http_capture_t *capture = (http_capture_t *)ctx;
  if (!msg || !capture || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  {
    const turbo_flow_content_descriptor_t *descriptor = turbo_flow_msg_content_descriptor(msg);
    if (descriptor) {
      capture->profile = descriptor->profile;
      capture->encoding = descriptor->encoding;
      memcpy(capture->media_type, descriptor->media_type, sizeof(capture->media_type));
    }
  }
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static int http_wait_connection_drained(turbo_flow_t *flow, size_t index,
                                        turbo_flow_connection_snapshot_t *snapshot) {
  int rc;
  if (!flow || !snapshot) return TURBO_EINVAL;
  for (size_t attempt = 0u; attempt < HTTP_TEST_DRAIN_RETRY_COUNT; ++attempt) {
    rc = turbo_flow_adapter_connection_snapshot_at(flow, index, snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot->in_flight_messages == 0u && snapshot->in_flight_bytes == 0u) return TURBO_OK;
    turbo_sleep_ms(HTTP_TEST_DRAIN_RETRY_DELAY_MS);
  }
  return TURBO_ETIMEDOUT;
}

static unsigned short pick_loopback_port(void) {
  struct sockaddr_in addr;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int addr_len = (int)sizeof(addr);
#else
  int socket_handle = -1;
  socklen_t addr_len = (socklen_t)sizeof(addr);
#endif
  unsigned short port = 0;

#ifdef _WIN32
  {
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
  }
#endif
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (socket_handle == INVALID_SOCKET) return 0;
#else
  if (socket_handle < 0) return 0;
#endif
  if (bind(socket_handle, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&addr, &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
  return port;
}

spec("turbo_flow_http") {
  it("registers HTTP client and server from one resolved YAML snapshot") {
    static const char yaml[] = "version: 1\n"
                               "fragments:\n"
                               "  connection:\n"
                               "    remote:\n"
                               "      url: http://127.0.0.1:1/orders\n"
                               "    local:\n"
                               "      port: 18081\n"
                               "      route: /orders\n"
                               "  timer:\n"
                               "    bounded:\n"
                               "      timeout_ms: 250\n"
                               "  thread:\n"
                               "    standard:\n"
                               "      headers: [\"Accept: application/json\"]\n"
                               "adapters:\n"
                               "  http.client:\n"
                               "    kind: http\n"
                               "    fragments:\n"
                               "      connection: remote\n"
                               "      timer: bounded\n"
                               "      thread: standard\n"
                               "    config:\n"
                               "      method: get\n"
                               "      max_response_size: 4096\n"
                               "  http.server:\n"
                               "    kind: http\n"
                               "    fragments:\n"
                               "      connection: local\n"
                               "    config:\n"
                               "      method: post\n"
                               "      max_body_size: 4096\n"
                               "      response_status: 202\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_graph_snapshot_t graph;
    const turbo_flow_adapter_schema_t *client_schema;
    const turbo_flow_adapter_schema_t *server_schema;
    static const char dsl[] =
        "source request adapter http.server operation " TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION
        " resource http.server\n"
        "stage call adapter http.client operation " TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION
        " resource http.client\n"
        "stage response adapter http.server operation " TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION
        " resource http.server\n"
        "stage main {\n"
        "  request -> call -> response\n"
        "}\n";

    check_not_null(flow);
    check_not_null(observe);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_http_register_client_resolved_adapter(flow, resolved, "http.client"),
                 TURBO_OK);
    check_int_eq(turbo_flow_http_register_server_resolved_adapter(flow, resolved, "http.server"),
                 TURBO_OK);
    client_schema = turbo_flow_find_adapter_schema(flow, "http.client");
    server_schema = turbo_flow_find_adapter_schema(flow, "http.server");
    check_not_null(client_schema);
    check_not_null(server_schema);
    check_uint_eq(client_schema->roles, TURBO_FLOW_ADAPTER_TRANSFORM);
    check_uint_eq(server_schema->roles, TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK);
    check_str_eq(turbo_flow_adapter_operation_module(flow, "http.client",
                                                     TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION),
                 TURBO_FLOW_HTTP_CLIENT_MODULE);
    check_str_eq(turbo_flow_adapter_operation_resource(flow, "http.client",
                                                       TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION),
                 "http.client");
    check_str_eq(turbo_flow_adapter_operation_module(flow, "http.server",
                                                     TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION),
                 TURBO_FLOW_HTTP_SERVER_MODULE);
    check_str_eq(turbo_flow_adapter_operation_module(flow, "http.server",
                                                     TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION),
                 TURBO_FLOW_HTTP_SERVER_MODULE);
    check_str_eq(turbo_flow_adapter_operation_resource(flow, "http.server",
                                                       TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION),
                 "http.server");
    check_str_eq(turbo_flow_find_primitive(flow, "http.client")->type_name,
                 TURBO_FLOW_HTTP_CLIENT_PRIMITIVE_TYPE);
    check_str_eq(turbo_flow_find_primitive(flow, "http.server")->type_name,
                 TURBO_FLOW_HTTP_SERVER_PRIMITIVE_TYPE);
    check_int_eq(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    memset(&graph, 0, sizeof(graph));
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
    check_size_eq(graph.runtime.adapter_count, 2u);
    check_size_eq(graph.resource_providers, 3u);
    check_int_eq(turbo_flow_observe_detach(observe), TURBO_OK);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("rejects host-only and mistyped fields in resolved HTTP adapters") {
    static const char yaml[] =
        "version: 1\nadapters:\n  http.client:\n    kind: http\n    config:\n"
        "      url: http://127.0.0.1:1\n      method: get\n      client: injected\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_http_register_client_resolved_adapter(flow, resolved, "http.client"),
                 TURBO_EINVAL);
    check_size_eq(turbo_flow_adapter_count(flow), 0u);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("rejects negative client timeouts at registration") {
    turbo_flow_http_client_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1";
    config.method = TURBO_FLOW_HTTP_GET;
    config.timeout_ms = -1;
    check_int_eq(turbo_flow_http_register_client_adapter(flow, "http.invalid", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("creates only an HTTPS authentication-service provider and keeps ACL local") {
    static const char yaml[] =
        "version: 1\nchannels:\n  mqtt.auth-service:\n    kind: auth_provider\n    config:\n"
        "      backend: https\n      url: https://auth.internal.example/v4/authenticate\n"
        "      method: password\n      service_id: broker-main\n"
        "      service_domain: root-a\n      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n"
        "      timeout_ms: 2500\n      max_secret_size: 2048\nadapters: {}\n";
    http_auth_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t keys = {sizeof(keys), &fixture, http_auth_secret_acquire,
                                               http_auth_secret_release};
    turbo_flow_security_auth_provider_owner_t owner = TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
    turbo_flow_security_auth_request_t request = TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_security_auth_provider_owner_create_resolved(
                     turbo_flow_http_auth_provider_factory(), resolved, "mqtt.auth-service", &keys,
                     &owner, &error),
                 TURBO_OK);
    check_str_eq(owner.backend, "https");
    check_str_eq(owner.method, "password");
    check_not_null(owner.enhanced_provider);
    check_ptr_eq(owner.enhanced_provider,
                 turbo_flow_http_enhanced_auth_provider_interface(
                     (const turbo_flow_http_auth_provider_t *)owner.owner));
    check_int_eq(fixture.acquire_calls, 1);
    check_int_eq(fixture.release_calls, 1);

    request.identity = "device-a";
    request.method = "password";
    request.secret = (const uint8_t *)"secret";
    request.secret_size = sizeof("secret") - 1u;
    request.protocol = "mqtt5";
    check_int_eq(turbo_flow_security_authenticate(owner.provider, &request, &principal),
                 TURBO_ENOTSUP);
    check_int_eq(fixture.acquire_calls, 1);
    turbo_flow_security_auth_provider_owner_destroy(&owner);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("MQTT-SEC-003 authenticates through verified mTLS HTTPS and validates principal v3") {
    static const char body[] = "{\"version\":3,\"authenticated\":true,\"principal\":{"
                               "\"id\":\"device-a\",\"type\":\"device\",\"domain\":\"root-a\","
                               "\"auth_method\":\"password\",\"scope\":\"domain\","
                               "\"roles\":[\"mqtt-user\"],\"groups\":[\"root-a\"],\"expires_at\":0,"
                               "\"policy_version\":7}}";
    char response[1024];
    char yaml[4096];
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    flow_mtls_test_server_t server;
    http_auth_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t keys = {sizeof(keys), &fixture, http_auth_secret_acquire,
                                               http_auth_secret_release};
    turbo_flow_security_auth_provider_owner_t owner = TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    coro_context_t *context = NULL;
    http_mtls_auth_task_t task;
    int written;

    memset(&task, 0, sizeof(task));
    atomic_init(&task.done, 0);
    task.status = TURBO_EBUSY;
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    written = snprintf(response, sizeof(response),
                       "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                       "Content-Length: %zu\r\nConnection: close\r\n\r\n%s",
                       sizeof(body) - 1u, body);
    check_true(written > 0 && (size_t)written < sizeof(response));
    check_int_eq(flow_mtls_test_server_start(&server, (const uint8_t *)response, (size_t)written),
                 0);
    written = snprintf(yaml, sizeof(yaml),
                       "version: 1\nchannels:\n  auth:\n    kind: auth_provider\n    config:\n"
                       "      backend: https\n      url: https://localhost:%u/v4/authenticate\n"
                       "      method: password\n      service_id: broker-main\n"
                       "      service_domain: root-a\n"
                       "      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n"
                       "      timeout_ms: 5000\n      tls:\n        ca_file: %s\n"
                       "        client_cert_file: %s\n        client_key_file: %s\nadapters: {}\n",
                       server.port, ca_file, cert_file, key_file);
    check_true(written > 0 && (size_t)written < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)written, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_security_auth_provider_owner_create_resolved(
            turbo_flow_http_auth_provider_factory(), resolved, "auth", &keys, &owner, &error),
        TURBO_OK);
    task.provider = owner.provider;
    task.request = (turbo_flow_security_auth_request_t)TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
    task.principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    task.request.identity = "device-a";
    task.request.method = "password";
    task.request.secret = (const uint8_t *)"secret";
    task.request.secret_size = sizeof("secret") - 1u;
    task.request.remote_address = "127.0.0.1";
    task.request.protocol = "mqtt5";
    task.request.peer_certificate_sha256 =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    context = coro_context_create(NULL);
    check_not_null(context);
    check_int_eq(coro_context_spawn(context, http_mtls_authenticate_task, &task), TURBO_OK);
    while (!atomic_load_explicit(&task.done, memory_order_acquire))
      check_int_eq(coro_context_run(context, TURBO_RUN_ONCE), TURBO_OK);
    flow_mtls_test_server_join(&server);
    check_int_eq(server.status, 0);
    check_true(server.peer_verified);
    check_int_eq(task.status, TURBO_OK);
    check_str_eq(task.principal.principal_id, "device-a");
    coro_context_destroy(context);
    turbo_flow_security_auth_provider_owner_destroy(&owner);
    turbo_flow_resolved_config_destroy(resolved);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(ca_file);
  }

  it("MQTT-SEC-003 rejects HTTPS status content JSON limits and unavailable replies") {
    static const char unauthorized[] =
        "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: 2\r\n"
        "Connection: close\r\n\r\n{}";
    static const char server_error[] =
        "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n"
        "Content-Length: 2\r\nConnection: close\r\n\r\n{}";
    static const char wrong_content[] =
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n"
        "Connection: close\r\n\r\n{}";
    static const char malformed_json[] =
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1\r\n"
        "Connection: close\r\n\r\n{";
    struct response_case {
      const uint8_t *bytes;
      size_t size;
      int expected;
    } cases[] = {
        {(const uint8_t *)unauthorized, sizeof(unauthorized) - 1u, TURBO_EPERM},
        {(const uint8_t *)server_error, sizeof(server_error) - 1u, TURBO_EIO},
        {(const uint8_t *)wrong_content, sizeof(wrong_content) - 1u, TURBO_EIO},
        {(const uint8_t *)malformed_json, sizeof(malformed_json) - 1u, TURBO_EPROTO},
        {NULL, 0u, TURBO_EIO},
    };
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    char wrong_ca_file[512] = {0};
    char wrong_ca_key_file[512] = {0};
    char *oversized = NULL;
    size_t oversized_header;
    size_t oversized_size;
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
      check_int_eq(http_auth_run_response_case(cases[i].bytes, cases[i].size, ca_file, cert_file,
                                               key_file, &principal),
                   cases[i].expected);
      check_str_eq(principal.principal_id, "");
      check_uint_eq(principal.policy_version, 0u);
    }
    oversized_size = TURBO_FLOW_HTTP_AUTH_RESPONSE_LIMIT + 1u;
    oversized = (char *)malloc(oversized_size + 256u);
    check_not_null(oversized);
    if (oversized) {
      turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
      int written = snprintf(oversized, 256u,
                             "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                             "Content-Length: %zu\r\nConnection: close\r\n\r\n",
                             oversized_size);
      check_true(written > 0);
      oversized_header = written > 0 ? (size_t)written : 0u;
      memset(oversized + oversized_header, 'x', oversized_size);
      check_int_eq(http_auth_run_response_case((const uint8_t *)oversized,
                                               oversized_header + oversized_size, ca_file,
                                               cert_file, key_file, &principal),
                   TURBO_EIO);
      check_str_eq(principal.principal_id, "");
      free(oversized);
    }
    {
      turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
      check_int_eq(http_auth_run_response_case_delayed((const uint8_t *)server_error,
                                                       sizeof(server_error) - 1u, 500u, ca_file,
                                                       cert_file, key_file, &principal),
                   TURBO_EIO);
      check_str_eq(principal.principal_id, "");
    }
    check_int_eq(tls_test_write_self_signed_files(wrong_ca_file, sizeof(wrong_ca_file),
                                                  wrong_ca_key_file, sizeof(wrong_ca_key_file),
                                                  "untrusted-ca", 1),
                 0);
    {
      turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
      check_int_eq(http_auth_run_response_case((const uint8_t *)server_error,
                                               sizeof(server_error) - 1u, wrong_ca_file, cert_file,
                                               key_file, &principal),
                   TURBO_EIO);
      check_str_eq(principal.principal_id, "");
    }
    tls_test_remove_file(wrong_ca_key_file);
    tls_test_remove_file(wrong_ca_file);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(ca_file);
  }

  it("MQTT-SEC-003 rejects wrong auth protocol versions and principal identity fields") {
    static const char wrong_version[] = "{\"version\":1,\"authenticated\":true,\"principal\":{}}";
    static const char wrong_principal[] =
        "{\"version\":3,\"authenticated\":true,\"principal\":{"
        "\"id\":\"device-a\",\"type\":\"device\",\"domain\":\"root-a\","
        "\"auth_method\":\"certificate\",\"scope\":\"domain\",\"roles\":[],"
        "\"groups\":[\"root-a\"],\"expires_at\":0,\"policy_version\":7}}";
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    check_int_eq(flow_http_auth_decode_response(wrong_version, sizeof(wrong_version) - 1u,
                                                "password", &principal),
                 TURBO_EPROTO);
    check_str_eq(principal.principal_id, "");
    check_int_eq(flow_http_auth_decode_response(wrong_principal, sizeof(wrong_principal) - 1u,
                                                "password", &principal),
                 TURBO_EPROTO);
    check_str_eq(principal.principal_id, "");
  }

  it("rejects plaintext HTTP and database fields in authentication provider config") {
    static const char insecure_yaml[] =
        "version: 1\nchannels:\n  auth:\n    kind: auth_provider\n    config:\n"
        "      backend: https\n      url: http://auth.internal/v4/authenticate\n"
        "      method: password\n      service_id: broker-main\n"
        "      service_domain: root-a\n      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n"
        "adapters: {}\n";
    static const char database_yaml[] =
        "version: 1\nchannels:\n  auth:\n    kind: auth_provider\n    config:\n"
        "      backend: https\n      url: https://auth.internal/v4/authenticate\n"
        "      method: password\n      service_id: broker-main\n"
        "      service_domain: root-a\n      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n"
        "      database: 3\nadapters: {}\n";
    static const char invalid_tls_yaml[] =
        "version: 1\nchannels:\n  auth:\n    kind: auth_provider\n    config:\n"
        "      backend: https\n      url: https://auth.internal/v4/authenticate\n"
        "      method: password\n      service_id: broker-main\n"
        "      service_domain: root-a\n      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n"
        "      tls:\n        client_cert_file: client.pem\n"
        "        client_key_file: false\nadapters: {}\n";
    http_auth_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t keys = {sizeof(keys), &fixture, http_auth_secret_acquire,
                                               http_auth_secret_release};
    turbo_flow_http_auth_provider_t *provider = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;

    check_int_eq(turbo_flow_config_resolve_yaml(insecure_yaml, sizeof(insecure_yaml) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_http_auth_provider_create_resolved(resolved, "auth", &keys, &provider, &error),
        TURBO_EINVAL);
    check_null(provider);
    turbo_flow_resolved_config_destroy(resolved);
    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(database_yaml, sizeof(database_yaml) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_http_auth_provider_create_resolved(resolved, "auth", &keys, &provider, &error),
        TURBO_EINVAL);
    check_str_contains(error.path, "database");
    check_null(provider);
    turbo_flow_resolved_config_destroy(resolved);
    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(invalid_tls_yaml, sizeof(invalid_tls_yaml) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_http_auth_provider_create_resolved(resolved, "auth", &keys, &provider, &error),
        TURBO_EINVAL);
    check_str_contains(error.path, "tls");
    check_null(provider);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("creates only an HTTPS ACL decision provider and requires a coroutine") {
    static const char yaml[] =
        "version: 1\nchannels:\n  acl:\n    kind: acl_provider\n    config:\n"
        "      backend: https\n      url: https://auth.internal/v4/acl/check\n"
        "      service_id: broker-main\n      service_domain: root-a\n"
        "      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n"
        "      timeout_ms: 2500\n      max_response_size: 4194304\nadapters: {}\n";
    http_auth_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t keys = {sizeof(keys), &fixture, http_auth_secret_acquire,
                                               http_auth_secret_release};
    turbo_flow_security_policy_provider_owner_t owner =
        TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_security_policy_provider_owner_create_resolved(
            turbo_flow_http_acl_provider_factory(), resolved, "acl", &keys, &owner, &error),
        TURBO_OK);
    check_str_eq(owner.backend, "https");
    check_null(owner.provider);
    check_not_null(owner.authorization_provider);
    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/groups/ops/device-a?event";
    check_int_eq(owner.authorization_provider->authorize(owner.authorization_provider->ctx,
                                                         &request, 1000u, &decision),
                 TURBO_ENOTSUP);
    turbo_flow_security_policy_provider_owner_destroy(&owner);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("authorizes one ACL request through verified mTLS HTTPS") {
    static const char body[] =
        "{\"version\":4,\"allowed\":true,\"reason\":\"allow_rule\",\"policy_version\":7}";
    char response[512];
    char yaml[4096];
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    flow_mtls_test_server_t server;
    http_auth_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t keys = {sizeof(keys), &fixture, http_auth_secret_acquire,
                                               http_auth_secret_release};
    turbo_flow_security_policy_provider_owner_t owner =
        TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    coro_context_t *context = NULL;
    http_mtls_acl_task_t task;
    int written;

    memset(&task, 0, sizeof(task));
    task.principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    task.request = (turbo_flow_security_request_t)TURBO_FLOW_SECURITY_REQUEST_INIT;
    task.decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    atomic_init(&task.done, 0);
    task.status = TURBO_EBUSY;
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    written = snprintf(response, sizeof(response),
                       "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                       "Content-Length: %zu\r\nConnection: close\r\n\r\n%s",
                       sizeof(body) - 1u, body);
    check_true(written > 0 && (size_t)written < sizeof(response));
    check_int_eq(flow_mtls_test_server_start(&server, (const uint8_t *)response, (size_t)written),
                 0);
    written = snprintf(yaml, sizeof(yaml),
                       "version: 1\nchannels:\n  acl:\n    kind: acl_provider\n    config:\n"
                       "      backend: https\n      url: https://localhost:%u/v4/acl/check\n"
                       "      service_id: broker-main\n      service_domain: root-a\n"
                       "      service_token_ref: env://TURBO_FLOW_AUTH_TOKEN\n      timeout_ms: 5000\n"
                       "      max_response_size: 4194304\n      tls:\n"
                       "        ca_file: %s\n        client_cert_file: %s\n"
                       "        client_key_file: %s\nadapters: {}\n",
                       server.port, ca_file, cert_file, key_file);
    check_true(written > 0 && (size_t)written < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)written, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_security_policy_provider_owner_create_resolved(
            turbo_flow_http_acl_provider_factory(), resolved, "acl", &keys, &owner, &error),
        TURBO_OK);
    task.provider = owner.authorization_provider;
    memcpy(task.principal.principal_id, "device-a", sizeof("device-a"));
    memcpy(task.principal.principal_type, "device", sizeof("device"));
    memcpy(task.principal.domain_id, "root-a", sizeof("root-a"));
    memcpy(task.principal.auth_method, "password", sizeof("password"));
    memcpy(task.principal.roles[0], "mqtt-user", sizeof("mqtt-user"));
    task.principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
    task.principal.role_count = 1u;
    task.principal.policy_version = 7u;
    task.request.principal = &task.principal;
    task.request.domain_id = task.principal.domain_id;
    task.request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    task.request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    task.request.resource = "root-a/groups/ops/device-a?event";
    task.request.username = (const uint8_t *)"device-a";
    task.request.username_size = sizeof("device-a") - 1u;
    task.request.client_id = (const uint8_t *)"client-a";
    task.request.client_id_size = sizeof("client-a") - 1u;
    context = coro_context_create(NULL);
    check_not_null(context);
    check_int_eq(coro_context_spawn(context, http_mtls_acl_load_task, &task), TURBO_OK);
    while (!atomic_load_explicit(&task.done, memory_order_acquire))
      check_int_eq(coro_context_run(context, TURBO_RUN_ONCE), TURBO_OK);
    flow_mtls_test_server_join(&server);
    check_int_eq(server.status, 0);
    check_true(server.peer_verified);
    check_int_eq(task.status, TURBO_OK);
    check_int_eq(task.decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_int_eq(task.decision.reason, TURBO_FLOW_SECURITY_REASON_ALLOW_RULE);
    check_uint_eq(task.decision.policy_version, 7u);
    coro_context_destroy(context);
    turbo_flow_security_policy_provider_owner_destroy(&owner);
    turbo_flow_resolved_config_destroy(resolved);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(ca_file);
  }

  it("strictly decodes a versioned ACL decision") {
    static const char success[] =
        "{\"version\":4,\"allowed\":true,\"reason\":\"allow_rule\",\"policy_version\":7}";
    static const char extra[] =
        "{\"version\":4,\"allowed\":true,\"reason\":\"allow_rule\","
        "\"policy_version\":7,\"debug\":true}";
    static const char inconsistent[] =
        "{\"version\":4,\"allowed\":false,\"reason\":\"allow_rule\",\"policy_version\":7}";
    static const char legacy[] =
        "{\"version\":3,\"allowed\":true,\"reason\":\"allow_rule\",\"policy_version\":7}";
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    check_int_eq(flow_http_acl_decode_check_response(success, sizeof(success) - 1u, &decision),
                 TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_int_eq(decision.reason, TURBO_FLOW_SECURITY_REASON_ALLOW_RULE);
    check_uint_eq(decision.policy_version, 7u);
    check_int_eq(flow_http_acl_decode_check_response(extra, sizeof(extra) - 1u, &decision),
                 TURBO_EPROTO);
    check_int_eq(flow_http_acl_decode_check_response(inconsistent, sizeof(inconsistent) - 1u,
                                                     &decision),
                 TURBO_EPROTO);
    check_int_eq(flow_http_acl_decode_check_response(legacy, sizeof(legacy) - 1u, &decision),
                 TURBO_EPROTO);
    check_uint_eq(decision.policy_version, 0u);
  }

  it("strictly decodes the versioned authentication-service response") {
    static const char success[] =
        "{\"version\":3,\"authenticated\":true,\"principal\":{"
        "\"id\":\"device-a\",\"type\":\"device\",\"domain\":\"root-a\","
        "\"auth_method\":\"password\",\"scope\":\"domain\","
        "\"roles\":[\"mqtt-user\"],\"groups\":[\"backend\"],\"expires_at\":0,"
        "\"policy_version\":7}}";
    static const char extra_field[] =
        "{\"version\":3,\"authenticated\":true,\"debug\":true,\"principal\":{}}";
    static const char legacy[] =
        "{\"version\":1,\"authenticated\":true,\"principal\":{"
        "\"id\":\"device-a\",\"type\":\"device\",\"tenant\":\"tenant-a\","
        "\"auth_method\":\"password\",\"scope\":\"tenant\",\"roles\":[],\"groups\":[],"
        "\"expires_at\":0,\"policy_version\":7}}";
    static const char no_groups[] =
        "{\"version\":3,\"authenticated\":true,\"principal\":{"
        "\"id\":\"device-a\",\"type\":\"device\",\"domain\":\"root-a\","
        "\"auth_method\":\"password\",\"scope\":\"domain\",\"roles\":[],"
        "\"groups\":[],\"expires_at\":0,\"policy_version\":7}}";
    static const char missing_domain[] =
        "{\"version\":3,\"authenticated\":true,\"principal\":{"
        "\"id\":\"device-a\",\"type\":\"device\",\"domain\":\"\","
        "\"auth_method\":\"password\",\"scope\":\"domain\",\"roles\":[],"
        "\"groups\":[],\"expires_at\":0,\"policy_version\":7}}";
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;

    check_int_eq(
        flow_http_auth_decode_response(success, sizeof(success) - 1u, "password", &principal),
        TURBO_OK);
    check_str_eq(principal.principal_id, "device-a");
    check_str_eq(principal.principal_type, "device");
    check_str_eq(principal.domain_id, "root-a");
    check_str_eq(principal.roles[0], "mqtt-user");
    check_str_eq(principal.groups[0], "backend");
    check_uint_eq(principal.policy_version, 7u);
    principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    check_int_eq(flow_http_auth_decode_response(success, sizeof(success) - 1u, "token", &principal),
                 TURBO_EPROTO);
    check_int_eq(flow_http_auth_decode_response(extra_field, sizeof(extra_field) - 1u, "password",
                                                &principal),
                 TURBO_EPROTO);
    check_int_eq(
        flow_http_auth_decode_response(legacy, sizeof(legacy) - 1u, "password", &principal),
        TURBO_EPROTO);
    check_int_eq(flow_http_auth_decode_response(no_groups, sizeof(no_groups) - 1u, "password",
                                                &principal),
                 TURBO_OK);
    check_int_eq(flow_http_auth_decode_response(missing_domain, sizeof(missing_domain) - 1u,
                                                "password", &principal),
                 TURBO_EPROTO);
  }

  it("exposes a typed credential-free HTTP client status document") {
    turbo_flow_http_client_config_t config;
    turbo_flow_resource_metadata_t before = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_metadata_t after = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = NULL;
    DataBindValue *value = NULL;
    tstr_t payload = NULL;
    int32_t last_status = 0;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1/private?token=url-secret";
    config.method = TURBO_FLOW_HTTP_POST;
    config.bearer_token = "bearer-secret";
    check_int_eq(turbo_flow_http_register_client_adapter(flow, "http.private", &config), TURBO_OK);
    check_size_eq(turbo_flow_resource_metadata_count(flow), 1u);
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &before), TURBO_OK);
    check_str_eq(before.uid, "http:http.private");
    check_str_eq(before.owner_name, "http.private");
    check_int_eq(before.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_int_eq(before.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_int_eq(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_str_eq(document.schema->schema_name, "TurboFlowHttpResource");
    check_str_eq(document.schema->type_name, "HttpClientStatus");
    check_uint_eq(document.schema->schema_id, 101u);
    check_uint_eq(document.schema->schema_version, 1u);
    check_int_eq(turbo_flow_resource_document_validate(&document, document.schema), TURBO_OK);
    {
      turbo_flow_resource_schema_t unknown = *document.schema;
      unknown.schema_version = 2u;
      check_int_eq(turbo_flow_resource_document_validate(&document, &unknown), TURBO_EPROTO);
    }
    check_int_eq(data_bind_create_from_text(document.schema->schema_text,
                                            strlen(document.schema->schema_text), &codec, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_parse_json(codec, document.schema->type_name,
                                      mem_buffer_const_data(document.payload),
                                      mem_buffer_used(document.payload), &value, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_validate_json(codec, document.schema->type_name,
                                         "{\"state\":0,\"last_status\":-1}",
                                         sizeof("{\"state\":0,\"last_status\":-1}") - 1u, &error),
                 DATA_BIND_ERR_TYPE_MISMATCH);
    check_int_eq(
        data_bind_validate_json(codec, document.schema->type_name,
                                "{\"state\":\"bad\",\"last_status\":-1,"
                                "\"connections_current\":\"0\",\"connection_limit\":\"1\","
                                "\"in_flight_messages\":\"0\",\"in_flight_bytes\":\"0\"}",
                                sizeof("{\"state\":\"bad\",\"last_status\":-1,"
                                       "\"connections_current\":\"0\",\"connection_limit\":\"1\","
                                       "\"in_flight_messages\":\"0\",\"in_flight_bytes\":\"0\"}") -
                                    1u,
                                &error),
        DATA_BIND_ERR_TYPE_MISMATCH);
    check_int_eq(data_bind_value_get_int32(data_bind_value_get(value, "last_status"), &last_status),
                 DATA_BIND_OK);
    check_int_eq(last_status, TURBO_ENOTCONN);
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_null(strstr(payload, "url-secret"));
    check_null(strstr(payload, "bearer-secret"));
    check_null(strstr(payload, "/private"));
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &after), TURBO_OK);
    check_size_eq(after.generation, before.generation);
    check_size_eq(after.observed_generation, before.observed_generation);

    tstr_freep(&payload);
    data_bind_value_free(value);
    data_bind_free(codec);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
  }

  it("rejects structurally incomplete schema bindings at registration") {
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    turbo_flow_http_server_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    binding.schema.schema_name = "http.orders";
    memset(&config, 0, sizeof(config));
    config.port = pick_loopback_port();
    config.route = "/orders";
    config.method = TURBO_FLOW_HTTP_POST;
    config.content_binding = &binding;
    check_int_eq(turbo_flow_http_register_server_adapter(flow, "http.orders", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("round trips payloads through server and client adapters") {
    static const char *server_dsl =
        "source request adapter http.server.echo "
        "operation " TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION " resource http.server.echo\n"
        "stage response adapter http.server.echo operation " TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION
        " resource http.server.echo\n"
        "stage main {\n"
        "  request -> response\n"
        "}\n";
    static const char *client_dsl =
        "source input\n"
        "stage request adapter http.client.once operation " TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION
        " resource http.client.once\n"
        "stage capture worker 1\n"
        "stage main {\n"
        "  input -> request -> capture\n"
        "}\n";
    unsigned short port = pick_loopback_port();
    char url[128];
    turbo_flow_http_server_config_t server_config;
    turbo_flow_http_client_config_t client_config;
    turbo_flow_content_descriptor_t input_descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_msg_t msg;
    turbo_flow_connection_snapshot_t server_connection;
    turbo_flow_connection_snapshot_t client_connection;
    http_capture_t capture;

    check_int_gt(port, 0);
    check_not_null(server_flow);
    check_not_null(client_flow);
    check_not_null(observe);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.route = "/echo";
    server_config.method = TURBO_FLOW_HTTP_POST;
    server_config.response_content_type = "application/octet-stream";
    check_int_eq(
        turbo_flow_http_register_server_adapter(server_flow, "http.server.echo", &server_config),
        TURBO_OK);
    check_str_eq(turbo_flow_adapter_operation_module(server_flow, "http.server.echo",
                                                     TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION),
                 TURBO_FLOW_HTTP_SERVER_MODULE);
    memset(&server_connection, 0, sizeof(server_connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_eq(server_connection.connections_current, 1);
    check_int_gt(snprintf(url, sizeof(url), "http://0.0.0.0:%u/echo", (unsigned)port), 0);
    check_str_eq(server_connection.endpoint, url);

    snprintf(url, sizeof(url), "http://127.0.0.1:%u/echo", (unsigned)port);
    memset(&client_config, 0, sizeof(client_config));
    client_config.url = url;
    client_config.method = TURBO_FLOW_HTTP_POST;
    client_config.timeout_ms = 2000;
    client_config.max_pump_iterations = 20000;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    check_int_eq(
        turbo_flow_http_register_client_adapter(client_flow, "http.client.once", &client_config),
        TURBO_OK);
    check_str_eq(turbo_flow_adapter_operation_module(client_flow, "http.client.once",
                                                     TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION),
                 TURBO_FLOW_HTTP_CLIENT_MODULE);
    memset(&client_connection, 0, sizeof(client_connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_str_eq(client_connection.endpoint, url);
    check_int_eq(
        turbo_flow_register_stage_ex(client_flow, "capture", capture_response, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, client_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(client_flow), TURBO_OK);

    {
      static const char rule[] =
          "when graph.connection_providers == 1 then adapter http.client.once quiesce";
      turbo_flow_observe_graph_snapshot_t graph;
      turbo_flow_observe_control_facts_snapshot_t facts_snapshot;
      turbo_flow_control_facts_t facts = TURBO_FLOW_CONTROL_FACTS_INIT;
      check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
      check_size_eq(graph.connection_providers, 1);
      check_size_eq(graph.connections_current, 0);
      check_int_eq(turbo_flow_observe_control_facts(observe, &facts_snapshot, &facts), TURBO_OK);
      check_int_eq(turbo_flow_control_ex(client_flow, rule, sizeof(rule) - 1u, &facts, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_control(client_flow, "adapter http.client.once resume",
                                      sizeof("adapter http.client.once resume") - 1u),
                   TURBO_OK);
    }

    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("round-trip");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_content_descriptor_from_media(&input_descriptor, TURBO_FLOW_DOMAIN_DATA,
                                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                                          "application/json", "request-body", NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_msg_set_content_descriptor(&msg, &input_descriptor), TURBO_OK);
    check_int_eq(turbo_flow_publish(client_flow, "input", &msg), TURBO_OK);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 10);
    check_mem_eq(capture.payload, "round-trip", 10);
    check_int_eq(capture.profile, TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY);
    check_int_eq(capture.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_str_eq(capture.media_type, "application/octet-stream");
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_eq(client_connection.connections_current, 0);
    check_size_eq(client_connection.in_flight_messages, 0);
    check_size_eq(client_connection.in_flight_bytes, 0);
    check_int_eq(http_wait_connection_drained(server_flow, 0u, &server_connection), TURBO_OK);
    check_size_eq(server_connection.in_flight_messages, 0);
    check_size_eq(server_connection.in_flight_bytes, 0);

    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_stop(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(client_connection.last_status, TURBO_ESHUTDOWN);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(server_connection.last_status, TURBO_ESHUTDOWN);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("publishes periodic GET responses as source messages") {
    static const char *server_dsl =
        "source request adapter http.server.poll "
        "operation " TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION " resource http.server.poll\n"
        "stage response adapter http.server.poll operation " TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION
        " resource http.server.poll\n"
        "stage main {\n"
        "  request -> response\n"
        "}\n";
    static const char *client_dsl =
        "source remote adapter http.client.poll operation " TURBO_FLOW_HTTP_CLIENT_POLL_OPERATION
        " resource http.client.poll\n"
        "stage capture worker 1\n"
        "stage main {\n"
        "  remote -> capture\n"
        "}\n";
    unsigned short port = pick_loopback_port();
    char url[128];
    turbo_flow_http_server_config_t server_config;
    turbo_flow_http_client_config_t client_config;
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    http_capture_t capture;

    check_int_gt(port, 0);
    check_not_null(server_flow);
    check_not_null(client_flow);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.route = "/poll";
    server_config.method = TURBO_FLOW_HTTP_GET;
    check_int_eq(
        turbo_flow_http_register_server_adapter(server_flow, "http.server.poll", &server_config),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(server_flow), TURBO_OK);

    snprintf(url, sizeof(url), "http://127.0.0.1:%u/poll", (unsigned)port);
    memset(&client_config, 0, sizeof(client_config));
    client_config.url = url;
    client_config.method = TURBO_FLOW_HTTP_GET;
    client_config.timeout_ms = 2000;
    client_config.poll_interval_ms = 20;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    check_int_eq(
        turbo_flow_http_register_client_adapter(client_flow, "http.client.poll", &client_config),
        TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(client_flow, "capture", capture_response, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(client_flow), TURBO_OK);
    for (int i = 0; i < 200 && atomic_load_explicit(&capture.called, memory_order_acquire) == 0;
         ++i) {
      turbo_sleep_ms(10);
    }
    check_int_gt(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    check_int_eq(turbo_flow_stop(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(server_flow), TURBO_OK);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
  }

  it("enables explicit retries only for idempotent transform methods") {
    static const char *get_dsl = "source input\n"
                                 "stage request adapter http.client.get retry attempts 2 delay 1\n"
                                 "stage main {\n"
                                 "  input -> request\n"
                                 "}\n";
    static const char *post_dsl = "source input\n"
                                  "stage request adapter http.client.post retry attempts 2\n"
                                  "stage main {\n"
                                  "  input -> request\n"
                                  "}\n";
    turbo_flow_http_client_config_t config;
    turbo_flow_t *get_flow = turbo_flow_create();
    turbo_flow_t *post_flow = turbo_flow_create();

    check_not_null(get_flow);
    check_not_null(post_flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1/retry";
    config.method = TURBO_FLOW_HTTP_GET;
    check_int_eq(turbo_flow_http_register_client_adapter(get_flow, "http.client.get", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(get_flow, get_dsl, strlen(get_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(get_flow), TURBO_OK);

    config.method = TURBO_FLOW_HTTP_POST;
    check_int_eq(turbo_flow_http_register_client_adapter(post_flow, "http.client.post", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(post_flow, post_dsl, strlen(post_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(post_flow), TURBO_ENOTSUP);
    check_str_contains(turbo_flow_last_error(post_flow)->message, "adapter retry callback");

    turbo_flow_destroy(get_flow);
    turbo_flow_destroy(post_flow);
  }
}
