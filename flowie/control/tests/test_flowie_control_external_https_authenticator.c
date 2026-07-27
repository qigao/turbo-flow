#include "flowie_control_external_https_authenticator_internal.h"

#include "CoroNet/turbo_coro_context.h"
#include "mtls_test_server.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_coro.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#define EXTERNAL_HTTPS_CLIENT_CERT                                                                 \
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

typedef struct external_https_secret_fixture_s {
  int acquire_calls;
  int release_calls;
  int acquire_status;
  const uint8_t *token;
  size_t token_size;
  uint64_t token_version;
} external_https_secret_fixture_t;

typedef struct external_https_task_s {
  const flowie_control_external_authenticator_t *authenticator;
  flowie_control_external_auth_request_t request;
  flowie_control_external_auth_assertion_t assertion;
  atomic_int done;
  int status;
} external_https_task_t;

typedef struct external_https_network_result_s {
  int status;
  int peer_verified;
  int acquire_calls;
  int release_calls;
  uint8_t request[FLOW_MTLS_TEST_REQUEST_CAPACITY];
  size_t request_size;
  flowie_control_external_auth_assertion_t assertion;
  flowie_control_external_https_authenticator_stats_t stats;
} external_https_network_result_t;

typedef struct external_https_network_options_s {
  const char *response;
  size_t response_size;
  uint32_t response_delay_ms;
  uint32_t timeout_ms;
  uint32_t max_in_flight;
  int configure_ca;
  int configure_client_identity;
  const uint8_t *creation_token;
  size_t creation_token_size;
  const uint8_t *request_token;
  size_t request_token_size;
} external_https_network_options_t;

static const char EXTERNAL_HTTPS_RESPONSE_BODY[] =
    "{\"version\":2,\"authenticated\":true,\"assertion\":{"
    "\"issuer\":\"https://idp.example\",\"subject\":\"tenant-42/device-a\","
    "\"subject_type\":\"device\",\"auth_method\":\"oidc-token\","
    "\"issued_at\":100,\"expires_at\":200,\"revision\":9,\"assurance_level\":2,"
    "\"account_enabled\":true,\"groups\":[\"fleet-a\",\"operators\"]}}";

static int external_https_secret_acquire(void *ctx, const char *reference,
                                         turbo_flow_security_secret_lease_t *lease) {
  static const uint8_t default_token[] = "service-token";
  external_https_secret_fixture_t *fixture = (external_https_secret_fixture_t *)ctx;
  if (!fixture || !reference || strcmp(reference, "env://EXTERNAL_AUTH_TOKEN") != 0 || !lease)
    return TURBO_ENOENT;
  ++fixture->acquire_calls;
  if (fixture->acquire_status != TURBO_OK) return fixture->acquire_status;
  lease->bytes = fixture->token ? fixture->token : default_token;
  lease->byte_count = fixture->token ? fixture->token_size : sizeof(default_token) - 1u;
  lease->version = fixture->token ? fixture->token_version : 1u;
  lease->provider_lease = fixture;
  return TURBO_OK;
}

static void external_https_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  external_https_secret_fixture_t *fixture = (external_https_secret_fixture_t *)ctx;
  if (fixture) ++fixture->release_calls;
  if (lease) *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}

static flowie_control_external_auth_request_t external_https_request(void) {
  static const uint8_t secret[] = "signed-token";
  flowie_control_external_auth_request_t request = FLOWIE_CONTROL_EXTERNAL_AUTH_REQUEST_INIT;
  request.root_group_id = "root-a";
  request.presented_identity = "external-device";
  request.method = "oidc-token";
  request.secret = secret;
  request.secret_size = sizeof(secret) - 1u;
  request.protocol = "mqtt";
  request.remote_address = "192.0.2.10:1883";
  request.peer_certificate_sha256 = EXTERNAL_HTTPS_CLIENT_CERT;
  return request;
}

static flowie_control_external_https_authenticator_config_t
external_https_config(external_https_secret_fixture_t *fixture) {
  flowie_control_external_https_authenticator_config_t config =
      FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_CONFIG_INIT;
  config.url = "https://idp.example/v1/authenticate";
  config.method = "oidc-token";
  config.service_token_ref = "env://EXTERNAL_AUTH_TOKEN";
  config.key_provider = (turbo_flow_security_key_provider_t){
      sizeof(turbo_flow_security_key_provider_t), fixture, external_https_secret_acquire,
      external_https_secret_release};
  return config;
}

static void external_https_verify_task(coro_t *coroutine, void *arg) {
  external_https_task_t *task = (external_https_task_t *)arg;
  (void)coroutine;
  task->status =
      task->authenticator->verify(task->authenticator->ctx, &task->request, &task->assertion);
  atomic_store_explicit(&task->done, 1, memory_order_release);
}

static int external_https_http_response(char *buffer, size_t capacity, int status,
                                        const char *reason, const char *content_type,
                                        const char *body) {
  int result;
  size_t body_size = body ? strlen(body) : 0u;
  if (!buffer || capacity == 0u || !reason) return -1;
  result = snprintf(buffer, capacity,
                    "HTTP/1.1 %d %s\r\n%s%s%sContent-Length: %zu\r\n"
                    "Connection: close\r\n\r\n%s",
                    status, reason, content_type ? "Content-Type: " : "",
                    content_type ? content_type : "", content_type ? "\r\n" : "", body_size,
                    body ? body : "");
  return result > 0 && (size_t)result < capacity ? result : -1;
}

static external_https_network_options_t external_https_network_options(const char *response,
                                                                       size_t response_size) {
  static const uint8_t token[] = "service-token";
  external_https_network_options_t options;
  memset(&options, 0, sizeof(options));
  options.response = response;
  options.response_size = response_size;
  options.timeout_ms = 3000u;
  options.max_in_flight = 64u;
  options.configure_ca = 1;
  options.configure_client_identity = 1;
  options.creation_token = token;
  options.creation_token_size = sizeof(token) - 1u;
  options.request_token = token;
  options.request_token_size = sizeof(token) - 1u;
  return options;
}

static int external_https_run_mtls(const external_https_network_options_t *options,
                                   external_https_network_result_t *result) {
  char cert_file[512] = {0};
  char key_file[512] = {0};
  char url[160];
  flow_mtls_test_server_t server;
  external_https_secret_fixture_t fixture;
  flowie_control_external_https_authenticator_config_t config = external_https_config(&fixture);
  flowie_control_external_https_authenticator_t *authenticator = NULL;
  external_https_task_t task;
  coro_context_t *context = NULL;
  int rc = TURBO_EIO;

  if (!options || (!options->response && options->response_size != 0u) ||
      options->timeout_ms == 0u || options->max_in_flight == 0u || !options->creation_token ||
      options->creation_token_size == 0u || !options->request_token ||
      options->request_token_size == 0u || !result)
    return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  memset(&fixture, 0, sizeof(fixture));
  fixture.token = options->creation_token;
  fixture.token_size = options->creation_token_size;
  fixture.token_version = 1u;
  result->status = TURBO_EIO;
  result->assertion =
      (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  result->stats = (flowie_control_external_https_authenticator_stats_t)
      FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;
  memset(&server, 0, sizeof(server));
  memset(&task, 0, sizeof(task));
  atomic_init(&task.done, 0);
  task.status = TURBO_EBUSY;
  if (tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)) != 0)
    goto done;
  if (flow_mtls_test_server_start_delayed(&server, (const uint8_t *)options->response,
                                          options->response_size, options->response_delay_ms) != 0)
    goto done;
  if (snprintf(url, sizeof(url), "https://localhost:%u/v1/authenticate", server.port) <= 0)
    goto done;
  config.url = url;
  config.timeout_ms = options->timeout_ms;
  config.max_in_flight = options->max_in_flight;
  config.tls.ca_file = options->configure_ca ? cert_file : NULL;
  config.tls.client_cert_file = options->configure_client_identity ? cert_file : NULL;
  config.tls.client_key_file = options->configure_client_identity ? key_file : NULL;
  rc = flowie_control_external_https_authenticator_create(&config, &authenticator);
  if (rc != TURBO_OK) goto done;
  fixture.token = options->request_token;
  fixture.token_size = options->request_token_size;
  fixture.token_version = 2u;
  task.authenticator = flowie_control_external_https_authenticator_interface(authenticator);
  task.request = external_https_request();
  task.assertion =
      (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  context = coro_context_create(NULL);
  if (!context) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = coro_context_spawn(context, external_https_verify_task, &task);
  if (rc != TURBO_OK) goto done;
  while (!atomic_load_explicit(&task.done, memory_order_acquire)) {
    rc = coro_context_run(context, TURBO_RUN_ONCE);
    if (rc != TURBO_OK) goto done;
  }
  result->status = task.status;
  result->assertion = task.assertion;
  rc = TURBO_OK;

done:
  if (context) coro_context_destroy(context);
  if (authenticator &&
      flowie_control_external_https_authenticator_get_stats(authenticator, &result->stats) !=
          TURBO_OK &&
      rc == TURBO_OK)
    rc = TURBO_EIO;
  flowie_control_external_https_authenticator_destroy(authenticator);
  flow_mtls_test_server_join(&server);
  result->peer_verified = server.peer_verified;
  result->request_size = server.request_size;
  if (server.request_size != 0u) memcpy(result->request, server.request, server.request_size + 1u);
  result->acquire_calls = fixture.acquire_calls;
  result->release_calls = fixture.release_calls;
  tls_test_remove_file(key_file);
  tls_test_remove_file(cert_file);
  return rc;
}

spec("Flowie control external HTTPS authenticator") {
  it("strictly decodes one typed assertion and explicit denial") {
    flowie_control_external_auth_assertion_t assertion =
        FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
    static const char denied[] = "{\"version\":2,\"authenticated\":false}";

    check_int_eq(flowie_control_external_https_decode_response(
                     EXTERNAL_HTTPS_RESPONSE_BODY, sizeof(EXTERNAL_HTTPS_RESPONSE_BODY) - 1u,
                     "oidc-token", &assertion),
                 TURBO_OK);
    check_str_eq(assertion.issuer, "https://idp.example");
    check_str_eq(assertion.subject, "tenant-42/device-a");
    check_str_eq(assertion.subject_type, "device");
    check_str_eq(assertion.auth_method, "oidc-token");
    check_uint_eq(assertion.issued_at, 100u);
    check_uint_eq(assertion.expires_at, 200u);
    check_uint_eq(assertion.revision, 9u);
    check_uint_eq(assertion.assurance_level, FLOWIE_CONTROL_EXTERNAL_ASSURANCE_MULTI_FACTOR);
    check_true(assertion.account_enabled);
    check_uint_eq(assertion.external_group_count, 2u);
    check_str_eq(assertion.external_groups[0], "fleet-a");
    check_str_eq(assertion.external_groups[1], "operators");

    check_int_eq(flowie_control_external_https_decode_response(denied, sizeof(denied) - 1u,
                                                               "oidc-token", &assertion),
                 TURBO_EPERM);
    check_str_eq(assertion.subject, "");
  }

  it("rejects unknown fields, duplicate groups, wrong methods, and noncanonical numbers") {
    flowie_control_external_auth_assertion_t assertion =
        FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
    static const char unknown[] = "{\"version\":2,\"authenticated\":false,\"reason\":\"hidden\"}";
    static const char duplicate_field[] =
        "{\"version\":2,\"authenticated\":false,\"authenticated\":false}";
    static const char duplicate_group[] =
        "{\"version\":2,\"authenticated\":true,\"assertion\":{"
        "\"issuer\":\"idp\",\"subject\":\"device-a\",\"subject_type\":\"device\","
        "\"auth_method\":\"oidc-token\",\"issued_at\":100,\"expires_at\":200,"
        "\"revision\":9,\"assurance_level\":2,\"account_enabled\":true,"
        "\"groups\":[\"fleet-a\",\"fleet-a\"]}}";
    static const char leading_zero[] = "{\"version\":01,\"authenticated\":false}";
    static const char overflow[] = "{\"version\":18446744073709551616,\"authenticated\":false}";

    check_int_eq(flowie_control_external_https_decode_response(unknown, sizeof(unknown) - 1u,
                                                               "oidc-token", &assertion),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_external_https_decode_response(
                     duplicate_field, sizeof(duplicate_field) - 1u, "oidc-token", &assertion),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_external_https_decode_response(
                     duplicate_group, sizeof(duplicate_group) - 1u, "oidc-token", &assertion),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_external_https_decode_response(
                     EXTERNAL_HTTPS_RESPONSE_BODY, sizeof(EXTERNAL_HTTPS_RESPONSE_BODY) - 1u,
                     "password", &assertion),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_external_https_decode_response(
                     leading_zero, sizeof(leading_zero) - 1u, "oidc-token", &assertion),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_external_https_decode_response(overflow, sizeof(overflow) - 1u,
                                                               "oidc-token", &assertion),
                 TURBO_EPROTO);
  }

  it("encodes only the bounded versioned request fields") {
    flowie_control_external_auth_request_t request = external_https_request();
    turbo_json_doc_t *document = NULL;
    char *body = NULL;
    size_t body_size = 0u;

    check_int_eq(flowie_control_external_https_encode_request(&request, &body, &body_size),
                 TURBO_OK);
    check_not_null(body);
    check_int_eq(turbo_parse_json((const uint8_t *)body, body_size, &document), TURBO_OK);
    check_not_null(document);
    check_size_eq(turbo_json_object_size(document), 8u);
    check_double_eq(turbo_json_number(turbo_json_object_get(document, "version")), 2.0, 0.001);
    check_str_eq(turbo_json_string(turbo_json_object_get(document, "root_group")), "root-a");
    check_str_eq(turbo_json_string(turbo_json_object_get(document, "identity")), "external-device");
    check_str_eq(turbo_json_string(turbo_json_object_get(document, "secret_base64")),
                 "c2lnbmVkLXRva2Vu");
    check_str_eq(turbo_json_string(turbo_json_object_get(document, "protocol")), "mqtt");
    check_str_eq(turbo_json_string(turbo_json_object_get(document, "remote_address")),
                 "192.0.2.10:1883");
    check_str_eq(turbo_json_string(turbo_json_object_get(document, "peer_certificate_sha256")),
                 EXTERNAL_HTTPS_CLIENT_CERT);

    turbo_free_json(&document);
    turbo_json_serialize_free(body);
  }

  it("rejects a non-canonical MQTT client certificate fingerprint before HTTPS") {
    flowie_control_external_auth_request_t request = external_https_request();
    char *body = NULL;
    size_t body_size = 0u;

    request.peer_certificate_sha256 =
        "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    check_int_eq(flowie_control_external_https_encode_request(&request, &body, &body_size),
                 TURBO_EINVAL);
    check_null(body);
    check_size_eq(body_size, 0u);
  }

  it("fails fast on insecure URLs, incomplete TLS identity, and non-coroutine use") {
    external_https_secret_fixture_t fixture = {0};
    external_https_secret_fixture_t invalid_tls_fixture = {0};
    flowie_control_external_https_authenticator_config_t config = external_https_config(&fixture);
    flowie_control_external_https_authenticator_t *authenticator = NULL;
    const flowie_control_external_authenticator_t *interface;
    flowie_control_external_auth_request_t request = external_https_request();
    flowie_control_external_auth_assertion_t assertion =
        FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
    flowie_control_external_https_authenticator_stats_t stats =
        FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;

    config.url = "http://idp.example/v1/authenticate";
    check_int_eq(flowie_control_external_https_authenticator_create(&config, &authenticator),
                 TURBO_EINVAL);
    check_null(authenticator);
    config = external_https_config(&invalid_tls_fixture);
    config.tls.client_cert_file = "missing-client-cert.pem";
    config.tls.client_key_file = "missing-client-key.pem";
    config.tls.client_key_password_ref = "env://EXTERNAL_AUTH_TOKEN";
    check_int_eq(flowie_control_external_https_authenticator_create(&config, &authenticator),
                 TURBO_EIO);
    check_null(authenticator);
    check_uint_eq(invalid_tls_fixture.acquire_calls, 2u);
    check_uint_eq(invalid_tls_fixture.release_calls, 2u);
    config = external_https_config(&fixture);
    config.tls.client_cert_file = "client.pem";
    check_int_eq(flowie_control_external_https_authenticator_create(&config, &authenticator),
                 TURBO_EINVAL);
    check_null(authenticator);
    config = external_https_config(&fixture);
    check_int_eq(flowie_control_external_https_authenticator_create(&config, &authenticator),
                 TURBO_OK);
    check_not_null(authenticator);
    interface = flowie_control_external_https_authenticator_interface(authenticator);
    check_not_null(interface);
    check_int_eq(flowie_control_external_authenticator_validate(interface), TURBO_OK);
    request.protocol = NULL;
    check_int_eq(interface->verify(interface->ctx, &request, &assertion), TURBO_EINVAL);
    check_uint_eq(fixture.acquire_calls, 1u);
    request = external_https_request();
    check_int_eq(interface->verify(interface->ctx, &request, &assertion), TURBO_ENOTSUP);
    check_uint_eq(fixture.acquire_calls, 1u);
    check_uint_eq(fixture.release_calls, 1u);
    check_int_eq(flowie_control_external_https_authenticator_get_stats(authenticator, &stats),
                 TURBO_OK);
    check_uint_eq(stats.started_requests, 0u);
    stats.size = 0u;
    check_int_eq(flowie_control_external_https_authenticator_get_stats(authenticator, &stats),
                 TURBO_EINVAL);

    flowie_control_external_https_authenticator_destroy(authenticator);
  }

  it("performs one coroutine-first mTLS request and returns the remote assertion") {
    char response[4096];
    external_https_network_result_t result;
    external_https_network_options_t options;
    int response_size;

    response_size = external_https_http_response(response, sizeof(response), 200, "OK",
                                                 "application/json", EXTERNAL_HTTPS_RESPONSE_BODY);
    check_int_gt(response_size, 0);
    options = external_https_network_options(response, (size_t)response_size);
    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_OK);
    check_true(result.peer_verified);
    check_str_eq(result.assertion.subject, "tenant-42/device-a");
    check_uint_eq(result.assertion.external_group_count, 2u);
    check_uint_eq(result.acquire_calls, 2u);
    check_uint_eq(result.release_calls, 2u);
    check_uint_eq(result.stats.started_requests, 1u);
    check_uint_eq(result.stats.in_flight, 0u);
    check_uint_eq(result.stats.succeeded, 1u);
  }

  it("maps remote authentication rejection to permission denied") {
    static const char response[] =
        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    external_https_network_result_t result;
    external_https_network_options_t options =
        external_https_network_options(response, sizeof(response) - 1u);

    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EPERM);
    check_true(result.peer_verified);
    check_uint_eq(result.stats.started_requests, 1u);
    check_uint_eq(result.stats.denied, 1u);
  }

  it("maps remote overload to busy without retrying") {
    static const char response[] =
        "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    external_https_network_result_t result;
    external_https_network_options_t options =
        external_https_network_options(response, sizeof(response) - 1u);

    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EBUSY);
    check_true(result.peer_verified);
    check_uint_eq(result.acquire_calls, 2u);
    check_uint_eq(result.stats.remote_overload, 1u);
  }

  it("distinguishes a third-party server failure from a protocol failure") {
    static const char response[] =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    external_https_network_result_t result;
    external_https_network_options_t options =
        external_https_network_options(response, sizeof(response) - 1u);

    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EIO);
    check_true(result.peer_verified);
    check_uint_eq(result.stats.remote_server_failures, 1u);
    check_uint_eq(result.stats.protocol_failures, 0u);
  }

  it("fails closed on a malformed JSON assertion") {
    static const char invalid_body[] = "{\"version\":2,\"authenticated\":";
    char response[512];
    external_https_network_result_t result;
    external_https_network_options_t options;
    int response_size = external_https_http_response(response, sizeof(response), 200, "OK",
                                                     "application/json", invalid_body);

    check_int_gt(response_size, 0);
    options = external_https_network_options(response, (size_t)response_size);
    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EPROTO);
    check_true(result.peer_verified);
    check_uint_eq(result.stats.protocol_failures, 1u);
  }

  it("fails closed when the third-party response exceeds its timeout") {
    char response[4096];
    external_https_network_result_t result;
    external_https_network_options_t options;
    int response_size = external_https_http_response(
        response, sizeof(response), 200, "OK", "application/json", EXTERNAL_HTTPS_RESPONSE_BODY);

    check_int_gt(response_size, 0);
    options = external_https_network_options(response, (size_t)response_size);
    options.response_delay_ms = 250u;
    options.timeout_ms = 50u;
    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EIO);
    check_true(result.peer_verified);
    check_uint_eq(result.stats.transport_failures, 1u);
  }

  it("fails closed when the TLS peer closes before an HTTP response") {
    external_https_network_result_t result;
    external_https_network_options_t options = external_https_network_options(NULL, 0u);

    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EIO);
    check_true(result.peer_verified);
    check_size_gt(result.request_size, 0u);
    check_uint_eq(result.stats.transport_failures, 1u);
  }

  it("fails the handshake when the third-party service requires an absent client certificate") {
    char response[4096];
    external_https_network_result_t result;
    external_https_network_options_t options;
    int response_size = external_https_http_response(
        response, sizeof(response), 200, "OK", "application/json", EXTERNAL_HTTPS_RESPONSE_BODY);

    check_int_gt(response_size, 0);
    options = external_https_network_options(response, (size_t)response_size);
    options.configure_client_identity = 0;
    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EIO);
    check_false(result.peer_verified);
    check_size_eq(result.request_size, 0u);
    check_uint_eq(result.stats.transport_failures, 1u);
  }

  it("fails the handshake when the third-party server certificate is untrusted") {
    char response[4096];
    external_https_network_result_t result;
    external_https_network_options_t options;
    int response_size = external_https_http_response(
        response, sizeof(response), 200, "OK", "application/json", EXTERNAL_HTTPS_RESPONSE_BODY);

    check_int_gt(response_size, 0);
    options = external_https_network_options(response, (size_t)response_size);
    options.configure_ca = 0;
    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_EIO);
    check_size_eq(result.request_size, 0u);
    check_uint_eq(result.stats.transport_failures, 1u);
  }

  it("loads the service token again for each outgoing request") {
    static const uint8_t creation_token[] = "initial-token";
    static const uint8_t request_token[] = "rotated-token";
    char response[4096];
    external_https_network_result_t result;
    external_https_network_options_t options;
    int response_size = external_https_http_response(
        response, sizeof(response), 200, "OK", "application/json", EXTERNAL_HTTPS_RESPONSE_BODY);

    check_int_gt(response_size, 0);
    options = external_https_network_options(response, (size_t)response_size);
    options.creation_token = creation_token;
    options.creation_token_size = sizeof(creation_token) - 1u;
    options.request_token = request_token;
    options.request_token_size = sizeof(request_token) - 1u;
    check_int_eq(external_https_run_mtls(&options, &result), TURBO_OK);
    check_int_eq(result.status, TURBO_OK);
    check_uint_eq(result.acquire_calls, 2u);
    check_str_contains((const char *)result.request, "Authorization: Bearer rotated-token\r\n");
    check_null(strstr((const char *)result.request, "Authorization: Bearer initial-token"));
  }

  it("reports a runtime service-token provider failure without attempting network access") {
    external_https_secret_fixture_t fixture = {0};
    flowie_control_external_https_authenticator_config_t config = external_https_config(&fixture);
    flowie_control_external_https_authenticator_t *authenticator = NULL;
    external_https_task_t task;
    coro_context_t *context = NULL;
    flowie_control_external_https_authenticator_stats_t stats =
        FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;

    memset(&task, 0, sizeof(task));
    atomic_init(&task.done, 0);
    check_int_eq(flowie_control_external_https_authenticator_create(&config, &authenticator),
                 TURBO_OK);
    check_not_null(authenticator);
    fixture.acquire_status = TURBO_EIO;
    task.authenticator = flowie_control_external_https_authenticator_interface(authenticator);
    task.request = external_https_request();
    task.assertion =
        (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
    context = coro_context_create(NULL);
    check_not_null(context);
    check_int_eq(coro_context_spawn(context, external_https_verify_task, &task), TURBO_OK);
    while (!atomic_load_explicit(&task.done, memory_order_acquire))
      check_int_eq(coro_context_run(context, TURBO_RUN_ONCE), TURBO_OK);

    check_int_eq(task.status, TURBO_EIO);
    check_int_eq(flowie_control_external_https_authenticator_get_stats(authenticator, &stats),
                 TURBO_OK);
    check_uint_eq(stats.started_requests, 1u);
    check_uint_eq(stats.in_flight, 0u);
    check_uint_eq(stats.local_failures, 1u);
    check_uint_eq(stats.transport_failures, 0u);
    check_uint_eq(fixture.acquire_calls, 2u);

    coro_context_destroy(context);
    flowie_control_external_https_authenticator_destroy(authenticator);
  }

  it("rejects excess concurrent authentication before secret or network access") {
    char cert_file[512] = {0};
    char key_file[512] = {0};
    char response[4096];
    char url[160];
    flow_mtls_test_server_t server;
    external_https_secret_fixture_t fixture = {0};
    flowie_control_external_https_authenticator_config_t config = external_https_config(&fixture);
    flowie_control_external_https_authenticator_t *authenticator = NULL;
    external_https_task_t first;
    external_https_task_t second;
    coro_context_t *context = NULL;
    flowie_control_external_https_authenticator_stats_t stats =
        FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;
    int response_size;

    memset(&server, 0, sizeof(server));
    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    atomic_init(&first.done, 0);
    atomic_init(&second.done, 0);
    first.status = TURBO_EIO;
    second.status = TURBO_EIO;
    response_size = external_https_http_response(response, sizeof(response), 200, "OK",
                                                 "application/json", EXTERNAL_HTTPS_RESPONSE_BODY);
    check_int_gt(response_size, 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(flow_mtls_test_server_start_delayed(&server, (const uint8_t *)response,
                                                     (size_t)response_size, 1000u),
                 0);
    check_int_gt(snprintf(url, sizeof(url), "https://localhost:%u/v1/authenticate", server.port),
                 0);
    config.url = url;
    config.max_in_flight = 1u;
    config.tls.ca_file = cert_file;
    config.tls.client_cert_file = cert_file;
    config.tls.client_key_file = key_file;
    check_int_eq(flowie_control_external_https_authenticator_create(&config, &authenticator),
                 TURBO_OK);
    check_not_null(authenticator);
    first.authenticator = flowie_control_external_https_authenticator_interface(authenticator);
    first.request = external_https_request();
    first.assertion =
        (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
    second.authenticator = first.authenticator;
    second.request = external_https_request();
    second.assertion =
        (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
    context = coro_context_create(NULL);
    check_not_null(context);
    check_int_eq(coro_context_spawn(context, external_https_verify_task, &first), TURBO_OK);
    while (fixture.acquire_calls < 2 && !atomic_load_explicit(&first.done, memory_order_acquire))
      check_int_eq(coro_context_run(context, TURBO_RUN_ONCE), TURBO_OK);
    check_false(atomic_load_explicit(&first.done, memory_order_acquire));
    check_uint_eq(fixture.acquire_calls, 2u);
    check_int_eq(coro_context_spawn(context, external_https_verify_task, &second), TURBO_OK);
    while (!atomic_load_explicit(&second.done, memory_order_acquire))
      check_int_eq(coro_context_run(context, TURBO_RUN_ONCE), TURBO_OK);
    check_int_eq(second.status, TURBO_EBUSY);
    check_uint_eq(fixture.acquire_calls, 2u);
    while (!atomic_load_explicit(&first.done, memory_order_acquire))
      check_int_eq(coro_context_run(context, TURBO_RUN_ONCE), TURBO_OK);
    check_int_eq(first.status, TURBO_OK);
    check_true(server.peer_verified);
    check_uint_eq(fixture.acquire_calls, 2u);
    check_uint_eq(fixture.release_calls, 2u);
    check_int_eq(flowie_control_external_https_authenticator_get_stats(authenticator, &stats),
                 TURBO_OK);
    check_uint_eq(stats.started_requests, 2u);
    check_uint_eq(stats.in_flight, 0u);
    check_uint_eq(stats.succeeded, 1u);
    check_uint_eq(stats.local_overload, 1u);

    coro_context_destroy(context);
    flowie_control_external_https_authenticator_destroy(authenticator);
    flow_mtls_test_server_join(&server);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }
}
