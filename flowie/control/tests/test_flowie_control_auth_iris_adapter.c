#include "flowie_control_auth_iris_adapter_internal.h"
#include "flowie_control_auth_iris_endpoint_internal.h"
#include "flowie_control_credential_internal.h"
#include "flowie_control_store_internal.h"

#include "CoroNet.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define AUTH_EXECUTOR_CERT "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

typedef struct auth_executor_fixture_s {
  char *database_path;
  flowie_control_store_t *store;
  flowie_control_auth_service_t *service;
  flowie_control_service_credential_resolver_t *service_credentials;
  flowie_control_auth_iris_adapter_t *adapter;
  flowie_control_auth_iris_endpoint_t *endpoint;
  flowie_control_generated_credential_t credential;
  flowie_control_generated_credential_t service_credential;
} auth_executor_fixture_t;

typedef struct auth_executor_task_s {
  flowie_control_auth_iris_endpoint_t *endpoint;
  const flowie_control_auth_http_request_t *request;
  turbo_flow_security_principal_t principal;
  int result;
} auth_executor_task_t;

typedef struct auth_endpoint_service_fixture_s {
  char *database_path;
  flowie_control_store_t *store;
  flowie_control_service_credential_resolver_t *resolver;
  flowie_control_generated_credential_t credential;
} auth_endpoint_service_fixture_t;

static int auth_endpoint_make_adapter(flowie_control_auth_iris_adapter_t **adapter_out) {
  flowie_control_auth_iris_adapter_config_t config = FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
  /* Transport-failure tests must prove the core pointer is never dereferenced. */
  config.service = (flowie_control_auth_service_t *)(uintptr_t)1u;
  return flowie_control_auth_iris_adapter_create(&config, adapter_out);
}

static uint64_t auth_endpoint_service_create(flowie_control_store_t *store, uint64_t revision,
                                             flowie_control_generated_credential_t *credential) {
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

  user.domain_id = "root-a";
  user.principal_id = "broker-main";
  user.principal_type = "service";
  user.actor = "bootstrap";
  user.request_id = "auth-endpoint-service";
  user.expected_revision = revision;
  user.occurred_at = 1100u + revision;
  check_int_eq(flowie_control_store_user_create(store, &user, &result), TURBO_OK);
  revision = result.revision;

  issue.domain_id = "root-a";
  issue.principal_id = "broker-main";
  issue.actor = "bootstrap";
  issue.request_id = "auth-endpoint-service-credential";
  issue.expected_revision = revision;
  issue.occurred_at = 1200u + revision;
  check_int_eq(flowie_control_store_credential_generate(store, &issue, credential), TURBO_OK);
  revision = credential->revision;

  role.domain_id = "root-a";
  role.role_id = FLOWIE_CONTROL_SERVICE_ROLE_AUTH_CLIENT;
  role.actor = "bootstrap";
  role.request_id = "auth-endpoint-service-role";
  role.expected_revision = revision;
  role.occurred_at = 1300u + revision;
  check_int_eq(flowie_control_store_role_create(store, &role, &result), TURBO_OK);
  revision = result.revision;

  assignment.domain_id = "root-a";
  assignment.principal_id = "broker-main";
  assignment.role_id = FLOWIE_CONTROL_SERVICE_ROLE_AUTH_CLIENT;
  assignment.actor = "bootstrap";
  assignment.request_id = "auth-endpoint-service-assignment";
  assignment.expected_revision = revision;
  assignment.occurred_at = 1400u + revision;
  check_int_eq(flowie_control_store_user_role_add(store, &assignment, &result), TURBO_OK);
  return result.revision;
}

static void auth_endpoint_service_fixture_open(auth_endpoint_service_fixture_t *fixture) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_domain_create_command_t domain = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_service_credential_config_t resolver_config =
      FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT;

  memset(fixture, 0, sizeof(*fixture));
  fixture->credential =
      (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  fixture->database_path = tt_make_temp_file("flowie-auth-endpoint-service", ".sqlite3");
  check_not_null(fixture->database_path);
  store_config.database_path = fixture->database_path;
  check_int_eq(flowie_control_store_open(&store_config, &fixture->store), TURBO_OK);

  domain.domain_id = "root-a";
  domain.actor = "bootstrap";
  domain.request_id = "auth-endpoint-domain";
  domain.occurred_at = 1000u;
  check_int_eq(flowie_control_store_domain_create(fixture->store, &domain, &result), TURBO_OK);
  (void)auth_endpoint_service_create(fixture->store, result.revision, &fixture->credential);

  resolver_config.listener_id = "broker-https";
  resolver_config.repository = flowie_control_store_repository(fixture->store);
  check_int_eq(flowie_control_service_credential_resolver_create(&resolver_config,
                                                                 &fixture->resolver),
               TURBO_OK);
}

static void auth_endpoint_service_fixture_close(auth_endpoint_service_fixture_t *fixture) {
  flowie_control_service_credential_resolver_destroy(fixture->resolver);
  flowie_control_generated_credential_wipe(&fixture->credential);
  flowie_control_store_destroy(fixture->store);
  check_int_eq(tt_remove_file(fixture->database_path), 0);
  free(fixture->database_path);
  memset(fixture, 0, sizeof(*fixture));
}

static int auth_endpoint_make_endpoint(flowie_control_auth_iris_adapter_t *adapter,
                                       auth_endpoint_service_fixture_t *service_fixture,
                                       flowie_control_auth_iris_endpoint_t **endpoint_out) {
  flowie_control_auth_iris_endpoint_config_t config = FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT;
  config.adapter = adapter;
  config.service_credentials = service_fixture->resolver;
  return flowie_control_auth_iris_endpoint_create(&config, endpoint_out);
}

static void auth_endpoint_request_init(Req *request, char *body, size_t body_size,
                                       request_item_t *headers, int header_count,
                                       coro_socket_t *client) {
  memset(request, 0, sizeof(*request));
  request->method = "POST";
  request->path = FLOWIE_CONTROL_AUTH_HTTP_PATH;
  request->body = body;
  request->body_len = body_size;
  request->headers.items = headers;
  request->headers.count = header_count;
  request->client = client;
}

static uint64_t auth_executor_clock(void *ctx) {
  (void)ctx;
  return 10000u;
}

static int auth_executor_policy_version(void *ctx, const char *domain_id,
                                        uint64_t *policy_version_out) {
  (void)ctx;
  if (policy_version_out) *policy_version_out = 0u;
  if (!domain_id || strcmp(domain_id, "root-a") != 0 || !policy_version_out)
    return TURBO_EINVAL;
  *policy_version_out = 1u;
  return TURBO_OK;
}

static void auth_executor_fixture_open(auth_executor_fixture_t *fixture, uint32_t workers,
                                       size_t queue_capacity,
                                       uint32_t deadline_ms) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_service_credential_config_t credential_config =
      FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT;
  flowie_control_auth_service_config_t service_config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_auth_iris_adapter_config_t adapter_config =
      FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
  flowie_control_auth_iris_endpoint_config_t endpoint_config =
      FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT;

  memset(fixture, 0, sizeof(*fixture));
  fixture->credential =
      (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  fixture->service_credential =
      (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  fixture->database_path = tt_make_temp_file("flowie-auth-executor", ".sqlite3");
  check_not_null(fixture->database_path);
  store_config.database_path = fixture->database_path;
  check_int_eq(flowie_control_store_open(&store_config, &fixture->store), TURBO_OK);
  check_not_null(fixture->store);

  root.domain_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "executor-root";
  root.occurred_at = 1000u;
  check_int_eq(flowie_control_store_domain_create(fixture->store, &root, &result), TURBO_OK);

  user.domain_id = "root-a";
  user.principal_id = "device-a";
  user.principal_type = "device";
  user.actor = "bootstrap";
  user.request_id = "executor-user";
  user.expected_revision = 1u;
  user.occurred_at = 1001u;
  check_int_eq(flowie_control_store_user_create(fixture->store, &user, &result), TURBO_OK);

  issue.domain_id = "root-a";
  issue.principal_id = "device-a";
  issue.actor = "bootstrap";
  issue.request_id = "executor-credential";
  issue.expected_revision = 2u;
  issue.occurred_at = 1002u;
  check_int_eq(
      flowie_control_store_credential_generate(fixture->store, &issue, &fixture->credential),
      TURBO_OK);
  (void)auth_endpoint_service_create(fixture->store, fixture->credential.revision,
                                     &fixture->service_credential);

  service_config.repository = flowie_control_store_repository(fixture->store);
  service_config.policy_version.current = auth_executor_policy_version;
  service_config.clock_seconds = auth_executor_clock;
  check_int_eq(flowie_control_auth_service_create(&service_config, &fixture->service), TURBO_OK);
  check_not_null(fixture->service);

  adapter_config.service = fixture->service;
  check_int_eq(flowie_control_auth_iris_adapter_create(&adapter_config, &fixture->adapter),
               TURBO_OK);
  check_not_null(fixture->adapter);

  credential_config.listener_id = "broker-https";
  credential_config.repository = flowie_control_store_repository(fixture->store);
  check_int_eq(flowie_control_service_credential_resolver_create(
                   &credential_config, &fixture->service_credentials),
               TURBO_OK);
  endpoint_config.adapter = fixture->adapter;
  endpoint_config.service_credentials = fixture->service_credentials;
  endpoint_config.local_executor_enabled = 1;
  endpoint_config.local_executor_workers = workers;
  endpoint_config.local_executor_queue_capacity = queue_capacity;
  endpoint_config.local_executor_deadline_ms = deadline_ms;
  check_int_eq(flowie_control_auth_iris_endpoint_create(&endpoint_config, &fixture->endpoint),
               TURBO_OK);
  check_not_null(fixture->endpoint);
}

static void auth_executor_fixture_close(auth_executor_fixture_t *fixture) {
  flowie_control_auth_iris_endpoint_destroy(fixture->endpoint);
  flowie_control_service_credential_resolver_destroy(fixture->service_credentials);
  flowie_control_auth_iris_adapter_destroy(fixture->adapter);
  flowie_control_auth_service_destroy(fixture->service);
  flowie_control_generated_credential_wipe(&fixture->credential);
  flowie_control_generated_credential_wipe(&fixture->service_credential);
  flowie_control_store_destroy(fixture->store);
  check_int_eq(tt_remove_file(fixture->database_path), 0);
  free(fixture->database_path);
  memset(fixture, 0, sizeof(*fixture));
}

static flowie_control_auth_http_request_t
auth_executor_request(const auth_executor_fixture_t *fixture) {
  flowie_control_auth_http_request_t request = {0};
  memcpy(request.identity, "device-a", sizeof("device-a"));
  memcpy(request.method, "password", sizeof("password"));
  memcpy(request.protocol, "mqtt", sizeof("mqtt"));
  memcpy(request.remote_address, "192.0.2.10:1883", sizeof("192.0.2.10:1883"));
  memcpy(request.secret, fixture->credential.token, fixture->credential.token_size);
  request.secret_size = fixture->credential.token_size;
  return request;
}

static void auth_executor_authenticate_task(coro_t *co, void *arg) {
  auth_executor_task_t *task = (auth_executor_task_t *)arg;
  flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
  (void)co;
  memset(&task->principal, 0, sizeof(task->principal));
  task->principal.size = sizeof(task->principal);
  task->principal.abi_version = TURBO_FLOW_SECURITY_ABI_V3;
  caller.listener_id = "broker-https";
  caller.service_id = "broker-main";
  caller.domain_id = "root-a";
  caller.peer_certificate_sha256 = AUTH_EXECUTOR_CERT;
  caller.permissions = FLOWIE_CONTROL_SERVICE_AUTHENTICATE;
  caller.authenticated = 1;
  task->result = flowie_control_auth_iris_endpoint_authenticate_verified(
      task->endpoint, &caller, task->request, &task->principal);
}

spec("flowie control auth iris adapter") {
  it("rejects invalid adapter configuration") {
    flowie_control_auth_iris_adapter_config_t config = FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
    flowie_control_auth_iris_adapter_t *adapter = NULL;

    check_int_eq(flowie_control_auth_iris_adapter_create(&config, &adapter), TURBO_EINVAL);
    check_null(adapter);
  }

  it("treats a connection without a client certificate as an empty optional identity") {
    coro_context_t *context = coro_context_create(NULL);
    coro_socket_t *plain;
    Req request;
    char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY];

    check_not_null(context);
    plain = coro_socket_create(context, CORO_SOCKET_TCP_V4);
    check_not_null(plain);
    memset(&request, 0, sizeof(request));
    request.client = plain;
    memset(fingerprint, 0xa5, sizeof(fingerprint));
    check_int_eq(flowie_control_auth_iris_adapter_optional_verified_peer_certificate(
                     &request, fingerprint),
                 TURBO_OK);
    check_str_eq(fingerprint, "");

    coro_socket_destroy(plain);
    coro_context_destroy(context);
  }

  it("strictly decodes the versioned request and wipes decoded credentials") {
    static const char body[] = "{\"version\":3,\"identity\":\"device-a\",\"method\":\"password\","
                               "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
                               "\"remote_address\":\"127.0.0.1\","
                               "\"peer_certificate_sha256\":\"" AUTH_EXECUTOR_CERT "\"}";
    flowie_control_auth_http_request_t request;
    flowie_control_auth_http_request_t zero = {0};

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u, &request),
                 TURBO_OK);
    check_str_eq(request.identity, "device-a");
    check_str_eq(request.method, "password");
    check_str_eq(request.protocol, "mqtt");
    check_str_eq(request.peer_certificate_sha256, AUTH_EXECUTOR_CERT);
    check_size_eq(request.secret_size, 6u);
    check_mem_eq(request.secret, "secret", 6u);
    flowie_control_auth_http_request_clear(&request);
    check_mem_eq(&request, &zero, sizeof(request));
  }

  it("rejects caller-supplied domain fields") {
    static const char body[] = "{\"version\":3,\"identity\":\"device-a\",\"method\":\"password\","
                               "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
                               "\"remote_address\":\"127.0.0.1\","
                               "\"peer_certificate_sha256\":\"\",\"domain\":\"root-a\"}";
    flowie_control_auth_http_request_t request;

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u, &request),
                 TURBO_EPROTO);
    check_size_eq(request.secret_size, 0u);
  }

  it("rejects non-canonical base64 credentials") {
    static const char body[] = "{\"version\":3,\"identity\":\"device-a\",\"method\":\"password\","
                               "\"secret_base64\":\"c2VjcmV0=\",\"protocol\":\"mqtt\","
                               "\"remote_address\":\"127.0.0.1\","
                               "\"peer_certificate_sha256\":\"\"}";
    flowie_control_auth_http_request_t request;

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u, &request),
                 TURBO_EPROTO);
    check_size_eq(request.secret_size, 0u);
  }

  it("rejects a non-canonical MQTT client certificate fingerprint") {
    static const char body[] =
        "{\"version\":3,\"identity\":\"device-a\",\"method\":\"password\","
        "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
        "\"remote_address\":\"127.0.0.1\","
        "\"peer_certificate_sha256\":"
        "\"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"}";
    flowie_control_auth_http_request_t request;

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u, &request),
                 TURBO_EPROTO);
    check_size_eq(request.secret_size, 0u);
  }

  it("encodes a complete principal response with version 3") {
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_json_doc_t *document = NULL;
    json_value_t *principal_json;
    char *body = NULL;
    size_t body_size = 0u;

    memcpy(principal.principal_id, "device-a", sizeof("device-a"));
    memcpy(principal.principal_type, "device", sizeof("device"));
    memcpy(principal.domain_id, "root-a", sizeof("root-a"));
    memcpy(principal.auth_method, "password", sizeof("password"));
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
    memcpy(principal.roles[0], "mqtt-user", sizeof("mqtt-user"));
    memcpy(principal.groups[0], "operators", sizeof("operators"));
    principal.role_count = 1u;
    principal.group_count = 1u;
    principal.expires_at = 100u;
    principal.policy_version = 7u;

    check_int_eq(flowie_control_auth_http_encode_principal(&principal, &body, &body_size),
                 TURBO_OK);
    check_not_null(body);
    check_int_eq(turbo_parse_json((const uint8_t *)body, body_size, &document), TURBO_OK);
    check_double_eq(turbo_json_number(turbo_json_object_get(document, "version")), 3.0, 0.001);
    check_true(turbo_json_bool(turbo_json_object_get(document, "authenticated")));
    principal_json = turbo_json_object_get(document, "principal");
    check_not_null(principal_json);
    check_str_eq(turbo_json_get_string(principal_json, "domain"), "root-a");
    check_double_eq(turbo_json_number(turbo_json_object_get(principal_json, "policy_version")), 7.0,
                    0.001);
    turbo_free_json(&document);
    turbo_json_serialize_free(body);
  }

  it("rejects a principal with an unterminated domain") {
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    char *body = NULL;
    size_t body_size = 0u;

    memcpy(principal.principal_id, "device-a", sizeof("device-a"));
    memcpy(principal.principal_type, "device", sizeof("device"));
    memset(principal.domain_id, 'a', sizeof(principal.domain_id));
    memcpy(principal.auth_method, "password", sizeof("password"));
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
    principal.policy_version = 1u;

    check_int_eq(flowie_control_auth_http_encode_principal(&principal, &body, &body_size),
                 TURBO_EINVAL);
    check_null(body);
    check_size_eq(body_size, 0u);
  }

  it("rejects a request without a bearer token") {
    static const char body[] = "{\"version\":3,\"identity\":\"device-a\",\"method\":\"password\","
                               "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
                               "\"remote_address\":\"127.0.0.1\","
                               "\"peer_certificate_sha256\":\"\"}";
    char mutable_body[sizeof(body)];
    request_item_t headers[1] = {{"Content-Type", "application/json"}};
    auth_endpoint_service_fixture_t service_fixture;
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    flowie_control_auth_iris_endpoint_t *endpoint = NULL;
    Req request;
    int status = 0;
    char *response = NULL;
    size_t response_size = 0u;

    auth_endpoint_service_fixture_open(&service_fixture);
    check_int_eq(auth_endpoint_make_adapter(&adapter), TURBO_OK);
    check_int_eq(auth_endpoint_make_endpoint(adapter, &service_fixture, &endpoint), TURBO_OK);
    memcpy(mutable_body, body, sizeof(body));
    auth_endpoint_request_init(&request, mutable_body, sizeof(body) - 1u, headers, 1, NULL);
    check_int_eq(flowie_control_auth_iris_endpoint_process(endpoint, &request, &status, &response,
                                                           &response_size),
                 TURBO_OK);
    check_int_eq(status, FORBIDDEN);
    check_mem_eq(mutable_body, (char[sizeof(body)]){0}, sizeof(body));
    turbo_json_serialize_free(response);
    flowie_control_auth_iris_endpoint_destroy(endpoint);
    flowie_control_auth_iris_adapter_destroy(adapter);
    auth_endpoint_service_fixture_close(&service_fixture);
  }

  it("rejects an invalid repository-backed service token before authentication") {
    static const char body[] = "{\"version\":3,\"identity\":\"device-a\",\"method\":\"password\","
                               "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
                               "\"remote_address\":\"127.0.0.1\","
                               "\"peer_certificate_sha256\":\"\"}";
    char mutable_body[sizeof(body)];
    char content_type[] = "application/json";
    char authorization[] = "Bearer wrong-service-token";
    request_item_t headers[4] = {{"Content-Type", content_type},
                                 {"Authorization", authorization},
                                 {"X-Flowie-Service-Id", "broker-main"},
                                 {"X-Flowie-Service-Domain", "root-a"}};
    auth_endpoint_service_fixture_t service_fixture;
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    flowie_control_auth_iris_endpoint_t *endpoint = NULL;
    Req request;
    int status = 0;
    char *response = NULL;
    size_t response_size = 0u;

    auth_endpoint_service_fixture_open(&service_fixture);
    check_int_eq(auth_endpoint_make_adapter(&adapter), TURBO_OK);
    check_int_eq(auth_endpoint_make_endpoint(adapter, &service_fixture, &endpoint), TURBO_OK);
    memcpy(mutable_body, body, sizeof(body));
    auth_endpoint_request_init(&request, mutable_body, sizeof(body) - 1u, headers, 4, NULL);
    check_int_eq(flowie_control_auth_iris_endpoint_process(endpoint, &request, &status, &response,
                                                           &response_size),
                 TURBO_OK);
    check_int_eq(status, FORBIDDEN);
    check_not_null(response);
    check_mem_eq(mutable_body, (char[sizeof(body)]){0}, sizeof(body));
    check_mem_eq(authorization, (char[sizeof(authorization)]){0}, sizeof(authorization));
    turbo_json_serialize_free(response);
    flowie_control_auth_iris_endpoint_destroy(endpoint);
    flowie_control_auth_iris_adapter_destroy(adapter);
    auth_endpoint_service_fixture_close(&service_fixture);
  }

  it("returns at the local executor deadline and drains accepted work on destroy") {
    auth_executor_fixture_t fixture;
    flowie_control_auth_http_request_t request;
    auth_executor_task_t task;
    coro_context_t *context = coro_context_create(NULL);

    check_not_null(context);
    auth_executor_fixture_open(&fixture, 1u, 1u, 1u);
    request = auth_executor_request(&fixture);
    task.endpoint = fixture.endpoint;
    task.request = &request;
    memset(&task.principal, 0, sizeof(task.principal));
    task.principal.size = sizeof(task.principal);
    task.principal.abi_version = TURBO_FLOW_SECURITY_ABI_V3;
    task.result = TURBO_EALREADY;
    check_int_eq(coro_context_spawn(context, auth_executor_authenticate_task, &task), TURBO_OK);
    check_int_eq(coro_context_run(context, TURBO_RUN_DEFAULT), TURBO_OK);
    check_int_eq(task.result, TURBO_ETIMEDOUT);
    check_str_eq(task.principal.principal_id, "");

    auth_executor_fixture_close(&fixture);
    flowie_control_auth_http_request_clear(&request);
    coro_context_destroy(context);
  }

  it("rejects excess local authentication work without blocking the owner lane") {
    enum { TASK_COUNT = 6 };
    auth_executor_fixture_t fixture;
    flowie_control_auth_http_request_t request;
    auth_executor_task_t tasks[TASK_COUNT];
    coro_context_t *context = coro_context_create(NULL);
    int succeeded = 0;
    int overloaded = 0;

    check_not_null(context);
    auth_executor_fixture_open(&fixture, 1u, 1u, 10000u);
    request = auth_executor_request(&fixture);
    memset(tasks, 0, sizeof(tasks));
    for (size_t index = 0u; index < TASK_COUNT; ++index) {
      tasks[index].endpoint = fixture.endpoint;
      tasks[index].request = &request;
      memset(&tasks[index].principal, 0, sizeof(tasks[index].principal));
      tasks[index].principal.size = sizeof(tasks[index].principal);
      tasks[index].principal.abi_version = TURBO_FLOW_SECURITY_ABI_V3;
      tasks[index].result = TURBO_EALREADY;
      check_int_eq(coro_context_spawn(context, auth_executor_authenticate_task, &tasks[index]),
                   TURBO_OK);
    }
    check_int_eq(coro_context_run(context, TURBO_RUN_DEFAULT), TURBO_OK);
    for (size_t index = 0u; index < TASK_COUNT; ++index) {
      if (tasks[index].result == TURBO_OK) {
        ++succeeded;
        check_str_eq(tasks[index].principal.principal_id, "device-a");
      } else if (tasks[index].result == TURBO_EBUSY) {
        ++overloaded;
      } else {
        check_int_eq(tasks[index].result, TURBO_OK);
      }
    }
    check_int_gt(succeeded, 0);
    check_int_gt(overloaded, 0);

    auth_executor_fixture_close(&fixture);
    flowie_control_auth_http_request_clear(&request);
    coro_context_destroy(context);
  }

  it("binds one fixed endpoint path and unbinds without a dangling context") {
    auth_endpoint_service_fixture_t service_fixture;
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    flowie_control_auth_iris_endpoint_t *endpoint = NULL;
    iris_app_t *app = iris_app_create();

    check_not_null(app);
    auth_endpoint_service_fixture_open(&service_fixture);
    check_int_eq(auth_endpoint_make_adapter(&adapter), TURBO_OK);
    check_int_eq(auth_endpoint_make_endpoint(adapter, &service_fixture, &endpoint), TURBO_OK);
    check_int_eq(flowie_control_auth_iris_endpoint_register(endpoint, app), TURBO_OK);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_AUTH_HTTP_PATH), endpoint);
    check_int_eq(flowie_control_auth_iris_endpoint_register(endpoint, app), TURBO_EINVAL);
    flowie_control_auth_iris_endpoint_destroy(endpoint);
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_AUTH_HTTP_PATH));
    flowie_control_auth_iris_adapter_destroy(adapter);
    auth_endpoint_service_fixture_close(&service_fixture);
    iris_app_destroy(app);
  }
}
