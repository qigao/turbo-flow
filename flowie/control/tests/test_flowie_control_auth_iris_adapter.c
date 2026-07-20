#include "flowie_control_auth_iris_adapter_internal.h"
#include "flowie_control_auth_iris_endpoint_internal.h"

#include "CoroNet.h"
#include "tinytest.h"
#include "turbo_parser.h"
#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

typedef struct auth_endpoint_secret_fixture_s {
  const uint8_t *bytes;
  size_t size;
  int acquire_count;
  int release_count;
} auth_endpoint_secret_fixture_t;

static int auth_endpoint_secret_acquire(void *ctx, const char *reference,
                                        turbo_flow_security_secret_lease_t *lease) {
  auth_endpoint_secret_fixture_t *fixture = (auth_endpoint_secret_fixture_t *)ctx;
  if (!fixture || !reference || strcmp(reference, "env://FLOWIE_AUTH_SERVICE_TOKEN") != 0 ||
      !lease || lease->size < sizeof(*lease))
    return TURBO_EINVAL;
  ++fixture->acquire_count;
  lease->bytes = fixture->bytes;
  lease->byte_count = fixture->size;
  lease->provider_lease = fixture;
  return TURBO_OK;
}

static void auth_endpoint_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  auth_endpoint_secret_fixture_t *fixture = (auth_endpoint_secret_fixture_t *)ctx;
  if (fixture) ++fixture->release_count;
  if (lease) *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}

static int auth_endpoint_make_adapter(flowie_control_auth_iris_adapter_t **adapter_out) {
  flowie_control_auth_iris_adapter_config_t config =
      FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
  /* Transport-failure tests must prove the core pointer is never dereferenced. */
  config.service = (flowie_control_auth_service_t *)(uintptr_t)1u;
  config.listener_id = "broker-mtls";
  return flowie_control_auth_iris_adapter_create(&config, adapter_out);
}

static int auth_endpoint_make_endpoint(flowie_control_auth_iris_adapter_t *adapter,
                                       auth_endpoint_secret_fixture_t *fixture,
                                       flowie_control_auth_iris_endpoint_t **endpoint_out) {
  flowie_control_auth_iris_endpoint_config_t config =
      FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT;
  config.adapter = adapter;
  config.service_token_ref = "env://FLOWIE_AUTH_SERVICE_TOKEN";
  config.key_provider = (turbo_flow_security_key_provider_t){
      sizeof(turbo_flow_security_key_provider_t), fixture, auth_endpoint_secret_acquire,
      auth_endpoint_secret_release};
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

spec("flowie control auth iris adapter") {
  it("rejects invalid adapter configuration") {
    flowie_control_auth_iris_adapter_config_t config =
        FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
    flowie_control_auth_iris_adapter_t *adapter = NULL;

    check_int_eq(flowie_control_auth_iris_adapter_create(&config, &adapter), TURBO_EINVAL);
    check_null(adapter);
  }

  it("fails closed before core authentication on a plain HTTP connection") {
    flowie_control_auth_iris_adapter_config_t config =
        FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    coro_context_t *context = coro_context_create(NULL);
    coro_socket_t *plain;
    Req request;
    int cache_hit = 1;

    check_not_null(context);
    plain = coro_socket_create(context, CORO_SOCKET_TCP_V4);
    check_not_null(plain);
    memset(&request, 0, sizeof(request));
    request.client = plain;
    config.service = (flowie_control_auth_service_t *)(uintptr_t)1u;
    config.listener_id = "broker-mtls";
    check_int_eq(flowie_control_auth_iris_adapter_create(&config, &adapter), TURBO_OK);
    check_int_eq(flowie_control_auth_iris_adapter_authenticate(
                     adapter, &request, "device-a", "password",
                     (const uint8_t *)"secret", sizeof("secret") - 1u,
                     &principal, &cache_hit),
                 TURBO_EPERM);
    check_false(cache_hit);
    check_str_eq(principal.principal_id, "");

    flowie_control_auth_iris_adapter_destroy(adapter);
    coro_socket_destroy(plain);
    coro_context_destroy(context);
  }

  it("strictly decodes the versioned request and wipes decoded credentials") {
    static const char body[] =
        "{\"version\":2,\"identity\":\"device-a\",\"method\":\"password\","
        "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
        "\"remote_address\":\"127.0.0.1\"}";
    flowie_control_auth_http_request_t request;
    flowie_control_auth_http_request_t zero = {0};

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u,
                                                         &request),
                 TURBO_OK);
    check_str_eq(request.identity, "device-a");
    check_str_eq(request.method, "password");
    check_str_eq(request.protocol, "mqtt");
    check_size_eq(request.secret_size, 6u);
    check_mem_eq(request.secret, "secret", 6u);
    flowie_control_auth_http_request_clear(&request);
    check_mem_eq(&request, &zero, sizeof(request));
  }

  it("rejects caller-supplied root-group fields") {
    static const char body[] =
        "{\"version\":2,\"identity\":\"device-a\",\"method\":\"password\","
        "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
        "\"remote_address\":\"127.0.0.1\",\"root_group\":\"root-a\"}";
    flowie_control_auth_http_request_t request;

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u,
                                                         &request),
                 TURBO_EPROTO);
    check_size_eq(request.secret_size, 0u);
  }

  it("rejects non-canonical base64 credentials") {
    static const char body[] =
        "{\"version\":2,\"identity\":\"device-a\",\"method\":\"password\","
        "\"secret_base64\":\"c2VjcmV0=\",\"protocol\":\"mqtt\","
        "\"remote_address\":\"127.0.0.1\"}";
    flowie_control_auth_http_request_t request;

    check_int_eq(flowie_control_auth_http_decode_request(body, sizeof(body) - 1u, 4096u,
                                                         &request),
                 TURBO_EPROTO);
    check_size_eq(request.secret_size, 0u);
  }

  it("encodes a complete principal response with version 2") {
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_json_doc_t *document = NULL;
    json_value_t *principal_json;
    char *body = NULL;
    size_t body_size = 0u;

    memcpy(principal.principal_id, "device-a", sizeof("device-a"));
    memcpy(principal.principal_type, "device", sizeof("device"));
    memcpy(principal.root_group_id, "root-a", sizeof("root-a"));
    memcpy(principal.auth_method, "password", sizeof("password"));
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
    memcpy(principal.roles[0], "mqtt-user", sizeof("mqtt-user"));
    memcpy(principal.groups[0], "root-a", sizeof("root-a"));
    principal.role_count = 1u;
    principal.group_count = 1u;
    principal.expires_at = 100u;
    principal.policy_version = 7u;

    check_int_eq(flowie_control_auth_http_encode_principal(&principal, &body, &body_size),
                 TURBO_OK);
    check_not_null(body);
    check_int_eq(turbo_parse_json((const uint8_t *)body, body_size, &document), TURBO_OK);
    check_double_eq(turbo_json_number(turbo_json_object_get(document, "version")), 2.0, 0.001);
    check_true(turbo_json_bool(turbo_json_object_get(document, "authenticated")));
    principal_json = turbo_json_object_get(document, "principal");
    check_not_null(principal_json);
    check_str_eq(turbo_json_get_string(principal_json, "root_group"), "root-a");
    check_double_eq(turbo_json_number(turbo_json_object_get(principal_json, "policy_version")),
                    7.0, 0.001);
    turbo_free_json(&document);
    turbo_json_serialize_free(body);
  }

  it("rejects a principal with an unterminated root group") {
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    char *body = NULL;
    size_t body_size = 0u;

    memcpy(principal.principal_id, "device-a", sizeof("device-a"));
    memcpy(principal.principal_type, "device", sizeof("device"));
    memset(principal.root_group_id, 'a', sizeof(principal.root_group_id));
    memcpy(principal.auth_method, "password", sizeof("password"));
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
    principal.policy_version = 1u;

    check_int_eq(flowie_control_auth_http_encode_principal(&principal, &body, &body_size),
                 TURBO_EINVAL);
    check_null(body);
    check_size_eq(body_size, 0u);
  }

  it("rejects a request without acquiring a missing bearer token") {
    static const uint8_t token[] = "service-token";
    static const char body[] =
        "{\"version\":2,\"identity\":\"device-a\",\"method\":\"password\","
        "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
        "\"remote_address\":\"127.0.0.1\"}";
    char mutable_body[sizeof(body)];
    request_item_t headers[1] = {{"Content-Type", "application/json"}};
    auth_endpoint_secret_fixture_t fixture = {token, sizeof(token) - 1u, 0, 0};
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    flowie_control_auth_iris_endpoint_t *endpoint = NULL;
    Req request;
    int status = 0;
    char *response = NULL;
    size_t response_size = 0u;

    check_int_eq(auth_endpoint_make_adapter(&adapter), TURBO_OK);
    check_int_eq(auth_endpoint_make_endpoint(adapter, &fixture, &endpoint), TURBO_OK);
    memcpy(mutable_body, body, sizeof(body));
    auth_endpoint_request_init(&request, mutable_body, sizeof(body) - 1u, headers, 1, NULL);
    check_int_eq(flowie_control_auth_iris_endpoint_process(endpoint, &request, &status, &response,
                                                           &response_size),
                 TURBO_OK);
    check_int_eq(status, FORBIDDEN);
    check_int_eq(fixture.acquire_count, 0);
    check_int_eq(fixture.release_count, 0);
    check_mem_eq(mutable_body, (char[sizeof(body)]){0}, sizeof(body));
    turbo_json_serialize_free(response);
    flowie_control_auth_iris_endpoint_destroy(endpoint);
    flowie_control_auth_iris_adapter_destroy(adapter);
  }

  it("accepts the service token then fail-closes on a plain transport") {
    static const uint8_t token[] = "service-token";
    static const char body[] =
        "{\"version\":2,\"identity\":\"device-a\",\"method\":\"password\","
        "\"secret_base64\":\"c2VjcmV0\",\"protocol\":\"mqtt\","
        "\"remote_address\":\"127.0.0.1\"}";
    char mutable_body[sizeof(body)];
    char content_type[] = "application/json";
    char authorization[] = "Bearer service-token";
    request_item_t headers[2] = {{"Content-Type", content_type},
                                 {"Authorization", authorization}};
    auth_endpoint_secret_fixture_t fixture = {token, sizeof(token) - 1u, 0, 0};
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    flowie_control_auth_iris_endpoint_t *endpoint = NULL;
    coro_context_t *context = coro_context_create(NULL);
    coro_socket_t *plain = NULL;
    Req request;
    int status = 0;
    char *response = NULL;
    size_t response_size = 0u;

    check_not_null(context);
    check_int_eq(auth_endpoint_make_adapter(&adapter), TURBO_OK);
    check_int_eq(auth_endpoint_make_endpoint(adapter, &fixture, &endpoint), TURBO_OK);
    plain = coro_socket_create(context, CORO_SOCKET_TCP_V4);
    check_not_null(plain);
    memcpy(mutable_body, body, sizeof(body));
    auth_endpoint_request_init(&request, mutable_body, sizeof(body) - 1u, headers, 2, plain);
    check_int_eq(flowie_control_auth_iris_endpoint_process(endpoint, &request, &status, &response,
                                                           &response_size),
                 TURBO_OK);
    check_int_eq(status, FORBIDDEN);
    check_int_eq(fixture.acquire_count, 1);
    check_int_eq(fixture.release_count, 1);
    check_not_null(response);
    check_mem_eq(mutable_body, (char[sizeof(body)]){0}, sizeof(body));
    check_mem_eq(authorization, (char[sizeof(authorization)]){0}, sizeof(authorization));
    turbo_json_serialize_free(response);
    flowie_control_auth_iris_endpoint_destroy(endpoint);
    flowie_control_auth_iris_adapter_destroy(adapter);
    coro_socket_destroy(plain);
    coro_context_destroy(context);
  }

  it("binds one fixed endpoint path and unbinds without a dangling context") {
    static const uint8_t token[] = "service-token";
    auth_endpoint_secret_fixture_t fixture = {token, sizeof(token) - 1u, 0, 0};
    flowie_control_auth_iris_adapter_t *adapter = NULL;
    flowie_control_auth_iris_endpoint_t *endpoint = NULL;
    iris_app_t *app = iris_app_create();

    check_not_null(app);
    check_int_eq(auth_endpoint_make_adapter(&adapter), TURBO_OK);
    check_int_eq(auth_endpoint_make_endpoint(adapter, &fixture, &endpoint), TURBO_OK);
    check_int_eq(flowie_control_auth_iris_endpoint_register(endpoint, app), TURBO_OK);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_AUTH_HTTP_PATH), endpoint);
    check_int_eq(flowie_control_auth_iris_endpoint_register(endpoint, app), TURBO_EINVAL);
    flowie_control_auth_iris_endpoint_destroy(endpoint);
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_AUTH_HTTP_PATH));
    flowie_control_auth_iris_adapter_destroy(adapter);
    iris_app_destroy(app);
  }
}
