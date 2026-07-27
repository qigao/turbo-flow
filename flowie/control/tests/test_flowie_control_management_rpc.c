#include "flowie_control_management_rpc_internal.h"

#include "base64_utils.h"
#include "flowie_control_credential_internal.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <stdlib.h>
#include <string.h>

typedef struct management_rpc_fixture_s {
  flowie_control_management_caller_t caller;
  uint64_t now;
  int resolver_rc;
  int external_https_enabled;
  size_t external_https_stats_calls;
  flowie_control_external_https_authenticator_stats_t external_https_stats;
} management_rpc_fixture_t;

static int management_rpc_resolve(void *ctx, const Req *request,
                                  flowie_control_management_caller_t *caller_out) {
  management_rpc_fixture_t *fixture = (management_rpc_fixture_t *)ctx;
  (void)request;
  if (fixture->resolver_rc != TURBO_OK) return fixture->resolver_rc;
  *caller_out = fixture->caller;
  return TURBO_OK;
}

static uint64_t management_rpc_clock(void *ctx) { return ((management_rpc_fixture_t *)ctx)->now; }

static int management_rpc_external_https_stats(
    void *ctx, flowie_control_external_https_authenticator_stats_t *stats_out) {
  management_rpc_fixture_t *fixture = (management_rpc_fixture_t *)ctx;
  if (!fixture || !stats_out || stats_out->size < sizeof(*stats_out)) return TURBO_EINVAL;
  ++fixture->external_https_stats_calls;
  if (!fixture->external_https_enabled) return TURBO_ENOENT;
  *stats_out = fixture->external_https_stats;
  return TURBO_OK;
}

static flowie_control_management_rpc_server_t *
management_rpc_open(char **path_out, flowie_control_store_t **store_out,
                    flowie_control_management_service_t **service_out, rpc_context_t **rpc_out,
                    iris_app_t **app_out, management_rpc_fixture_t *fixture) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t root_result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_management_service_config_t service_config =
      FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_management_rpc_server_config_t server_config =
      FLOWIE_CONTROL_MANAGEMENT_RPC_SERVER_CONFIG_INIT;
  rpc_config_t rpc_config = RPC_DEFAULT_CONFIG();
  flowie_control_management_rpc_server_t *server = NULL;

  *path_out = tt_make_temp_file("flowie-management-rpc", ".sqlite3");
  check_not_null(*path_out);
  store_config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&store_config, store_out), TURBO_OK);
  root.root_group_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "request-root";
  root.occurred_at = 1000u;
  check_int_eq(flowie_control_store_root_group_create(*store_out, &root, &root_result), TURBO_OK);
  service_config.repository = flowie_control_store_repository(*store_out);
  check_int_eq(flowie_control_management_service_create(&service_config, service_out), TURBO_OK);
  rpc_config.endpoint = "/v1/management/rpc";
  rpc_config.enable_batch = 0;
  rpc_config.enable_introspection = 0;
  rpc_config.max_batch_size = 0;
  rpc_config.max_request_size = 16384u;
  *rpc_out = rpc_init(&rpc_config);
  check_not_null(*rpc_out);
  server_config.service = *service_out;
  server_config.rpc_context = *rpc_out;
  server_config.resolve_caller = management_rpc_resolve;
  server_config.resolve_caller_ctx = fixture;
  server_config.clock = management_rpc_clock;
  server_config.clock_ctx = fixture;
  server_config.external_https_stats = management_rpc_external_https_stats;
  server_config.external_https_stats_ctx = fixture;
  check_int_eq(flowie_control_management_rpc_server_create(&server_config, &server), TURBO_OK);
  *app_out = iris_app_create();
  check_not_null(*app_out);
  check_int_eq(flowie_control_management_rpc_server_bind(server, *app_out), TURBO_OK);
  return server;
}

static void management_rpc_close(flowie_control_management_rpc_server_t *server, rpc_context_t *rpc,
                                 iris_app_t *app, flowie_control_management_service_t *service,
                                 flowie_control_store_t *store, char *path) {
  flowie_control_management_rpc_server_destroy(server);
  check_uint_eq(rpc->method_count, 0u);
  iris_app_destroy(app);
  rpc_destroy(rpc);
  flowie_control_management_service_destroy(service);
  flowie_control_store_destroy(store);
  check_int_eq(tt_remove_file(path), 0);
  free(path);
}

static turbo_json_doc_t *management_rpc_call(flowie_control_management_rpc_server_t *server,
                                             iris_app_t *app, mem_pool_t *arena,
                                             iris_security_context_t *security, const char *body,
                                             int *status_out) {
  Req request;
  rpc_response_t response;
  turbo_json_doc_t *document = NULL;
  char *response_json = NULL;
  size_t response_size = 0u;
  memset(&request, 0, sizeof(request));
  memset(&response, 0, sizeof(response));
  request.app = app;
  request.arena = arena;
  request.method = "POST";
  request.path = "/v1/management/rpc";
  request.body = (char *)body;
  request.body_len = strlen(body);
  request.security = security;
  *status_out = flowie_control_management_rpc_server_execute(server, &request, &response);
  check_int_eq(rpc_build_response(&response, &response_json, &response_size), 0);
  check_not_null(response_json);
  check_int_eq(turbo_parse_json((const uint8_t *)response_json, response_size, &document),
               TURBO_OK);
  return document;
}

static int management_rpc_error_code(turbo_json_doc_t *document) {
  json_value_t *error = turbo_json_object_get(document, "error");
  return error ? (int)turbo_json_number(turbo_json_object_get(error, "code")) : 0;
}

spec("Flowie management JSON-RPC") {
  it("binds a dedicated caller-owned context and rejects unsafe RPC forms") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_rpc_server_t *server = NULL;
    rpc_context_t *rpc = NULL;
    iris_app_t *app = NULL;
    management_rpc_fixture_t fixture = {FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT, 5000u, TURBO_OK};
    iris_security_context_t security = {0};
    mem_pool_t arena;
    turbo_json_doc_t *document = NULL;
    int status = 0;

    fixture.caller.root_group_id = "root-a";
    fixture.caller.actor = "viewer-1";
    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    check_int_eq(mem_init(&arena, 0u), 0);
    server = management_rpc_open(&path, &store, &service, &rpc, &app, &fixture);
    check_uint_eq(rpc->method_count, 28u);
    check_ptr_eq(iris_app_lookup_rpc_context(app, "/v1/management/rpc"), server);

    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.system.status\",\"id\":1}", &status);
    check_int_eq(management_rpc_error_code(document), -32001);
    turbo_free_json(&document);
    security.authenticated = true;
    document = management_rpc_call(
        server, app, &arena, &security,
        "[{\"jsonrpc\":\"2.0\",\"method\":\"flowie.system.status\",\"id\":1}]", &status);
    check_int_eq(management_rpc_error_code(document), RPC_ERROR_INVALID_REQUEST);
    turbo_free_json(&document);
    document =
        management_rpc_call(server, app, &arena, &security,
                            "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.system.status\"}", &status);
    check_int_eq(management_rpc_error_code(document), RPC_ERROR_INVALID_REQUEST);
    turbo_free_json(&document);

    management_rpc_close(server, rpc, app, service, store, path);
    mem_destroy(&arena);
  }

  it("restricts global external HTTPS statistics to security administrators") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_rpc_server_t *server = NULL;
    rpc_context_t *rpc = NULL;
    iris_app_t *app = NULL;
    management_rpc_fixture_t fixture = {FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT, 5000u, TURBO_OK};
    iris_security_context_t security = {0};
    mem_pool_t arena;
    turbo_json_doc_t *document = NULL;
    json_value_t *result = NULL;
    int status = 0;

    fixture.caller.root_group_id = "root-a";
    fixture.caller.actor = "viewer-1";
    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    fixture.external_https_enabled = 1;
    fixture.external_https_stats = (flowie_control_external_https_authenticator_stats_t)
        FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;
    fixture.external_https_stats.started_requests = 17u;
    fixture.external_https_stats.in_flight = 2u;
    fixture.external_https_stats.succeeded = 8u;
    fixture.external_https_stats.denied = 3u;
    fixture.external_https_stats.local_overload = 1u;
    fixture.external_https_stats.remote_overload = 1u;
    fixture.external_https_stats.remote_server_failures = 1u;
    fixture.external_https_stats.transport_failures = 1u;
    fixture.external_https_stats.protocol_failures = 1u;
    fixture.external_https_stats.local_failures = 1u;
    security.authenticated = true;
    check_int_eq(mem_init(&arena, 0u), 0);
    server = management_rpc_open(&path, &store, &service, &rpc, &app, &fixture);

    document =
        management_rpc_call(server, app, &arena, &security,
                            "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.auth.external_https.stats\","
                            "\"params\":{\"identity\":\"forbidden\"},\"id\":1}",
                            &status);
    check_int_eq(status, TURBO_EPROTO);
    check_int_eq(management_rpc_error_code(document), RPC_ERROR_INVALID_PARAMS);
    check_size_eq(fixture.external_https_stats_calls, 0u);
    turbo_free_json(&document);

    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.auth.external_https.stats\",\"id\":2}", &status);
    check_int_eq(status, TURBO_EPERM);
    check_int_eq(management_rpc_error_code(document), -32003);
    check_size_eq(fixture.external_https_stats_calls, 0u);
    turbo_free_json(&document);

    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.auth.external_https.stats\",\"id\":3}", &status);
    check_int_eq(status, TURBO_OK);
    check_int_eq(management_rpc_error_code(document), 0);
    result = turbo_json_object_get(document, "result");
    check_not_null(result);
    check_size_eq(turbo_json_object_size(result), 11u);
    check_true(turbo_json_bool(turbo_json_object_get(result, "enabled")));
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "started_requests")), 17.0,
                    0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "in_flight")), 2.0, 0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "succeeded")), 8.0, 0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "denied")), 3.0, 0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "local_overload")), 1.0, 0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "remote_overload")), 1.0,
                    0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "remote_server_failures")), 1.0,
                    0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "transport_failures")), 1.0,
                    0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "protocol_failures")), 1.0,
                    0.001);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "local_failures")), 1.0, 0.001);
    check_size_eq(fixture.external_https_stats_calls, 1u);
    turbo_free_json(&document);

    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.auth.external_https.stats\",\"id\":4}", &status);
    check_int_eq(status, TURBO_EPERM);
    check_int_eq(management_rpc_error_code(document), -32003);
    check_size_eq(fixture.external_https_stats_calls, 1u);
    turbo_free_json(&document);

    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    fixture.external_https_enabled = 0;
    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.auth.external_https.stats\",\"params\":{},"
        "\"id\":5}",
        &status);
    check_int_eq(status, TURBO_OK);
    check_int_eq(management_rpc_error_code(document), 0);
    result = turbo_json_object_get(document, "result");
    check_not_null(result);
    check_size_eq(turbo_json_object_size(result), 1u);
    check_false(turbo_json_bool(turbo_json_object_get(result, "enabled")));
    check_size_eq(fixture.external_https_stats_calls, 2u);
    turbo_free_json(&document);

    management_rpc_close(server, rpc, app, service, store, path);
    mem_destroy(&arena);
  }

  it("injects root actor and time while enforcing permissions and exact params") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_rpc_server_t *server = NULL;
    rpc_context_t *rpc = NULL;
    iris_app_t *app = NULL;
    management_rpc_fixture_t fixture = {FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT, 5000u, TURBO_OK};
    iris_security_context_t security = {0};
    mem_pool_t arena;
    turbo_json_doc_t *document = NULL;
    json_value_t *result = NULL;
    uint64_t revision = 0u;
    int status = 0;

    fixture.caller.root_group_id = "root-a";
    fixture.caller.actor = "admin-1";
    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    security.authenticated = true;
    check_int_eq(mem_init(&arena, 0u), 0);
    server = management_rpc_open(&path, &store, &service, &rpc, &app, &fixture);

    document =
        management_rpc_call(server, app, &arena, &security,
                            "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.user.create\",\"params\":{"
                            "\"principal_id\":\"device-1\",\"principal_type\":\"device\","
                            "\"request_id\":\"request-user\",\"expected_revision\":1},\"id\":1}",
                            &status);
    check_int_eq(management_rpc_error_code(document), -32003);
    turbo_free_json(&document);
    fixture.caller.permissions |= FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    document =
        management_rpc_call(server, app, &arena, &security,
                            "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.user.create\",\"params\":{"
                            "\"principal_id\":\"device-1\",\"principal_type\":\"device\","
                            "\"request_id\":\"request-user\",\"expected_revision\":1,"
                            "\"root_group\":\"root-b\"},\"id\":2}",
                            &status);
    check_int_eq(management_rpc_error_code(document), RPC_ERROR_INVALID_PARAMS);
    turbo_free_json(&document);
    document =
        management_rpc_call(server, app, &arena, &security,
                            "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.user.create\",\"params\":{"
                            "\"principal_id\":\"device-1\",\"principal_type\":\"device\","
                            "\"request_id\":\"request-user\",\"expected_revision\":1},\"id\":3}",
                            &status);
    check_int_eq(management_rpc_error_code(document), 0);
    result = turbo_json_object_get(document, "result");
    check_not_null(result);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "revision")), 2.0, 0.001);
    turbo_free_json(&document);
    check_int_eq(flowie_control_store_revision(store, &revision), TURBO_OK);
    check_uint_eq(revision, 2u);

    management_rpc_close(server, rpc, app, service, store, path);
    mem_destroy(&arena);
  }

  it("returns generated credentials once and enforces secure lifecycle permissions") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_rpc_server_t *server = NULL;
    rpc_context_t *rpc = NULL;
    iris_app_t *app = NULL;
    management_rpc_fixture_t fixture = {FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT, 5000u, TURBO_OK};
    iris_security_context_t security = {0};
    mem_pool_t arena;
    turbo_json_doc_t *document = NULL;
    json_value_t *result = NULL;
    const char *encoded = NULL;
    uint8_t *first_secret = NULL;
    uint8_t *rotated_secret = NULL;
    size_t first_secret_size = 0u;
    size_t rotated_secret_size = 0u;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    int status = 0;

    fixture.caller.root_group_id = "root-a";
    fixture.caller.actor = "user-admin-1";
    fixture.caller.permissions =
        FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    security.authenticated = true;
    check_int_eq(mem_init(&arena, 0u), 0);
    server = management_rpc_open(&path, &store, &service, &rpc, &app, &fixture);

    document =
        management_rpc_call(server, app, &arena, &security,
                            "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.user.create\",\"params\":{"
                            "\"principal_id\":\"device-1\",\"principal_type\":\"device\","
                            "\"request_id\":\"request-user\",\"expected_revision\":1},\"id\":1}",
                            &status);
    check_int_eq(management_rpc_error_code(document), 0);
    turbo_free_json(&document);

    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.credential.generate\",\"params\":{"
        "\"principal_id\":\"device-1\",\"request_id\":\"request-generate\","
        "\"expected_revision\":2},\"id\":2}",
        &status);
    check_int_eq(management_rpc_error_code(document), -32003);
    turbo_free_json(&document);

    fixture.caller.actor = "security-admin-1";
    fixture.caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.credential.generate\",\"params\":{"
        "\"principal_id\":\"device-1\",\"request_id\":\"request-generate\","
        "\"expected_revision\":2},\"id\":3}",
        &status);
    check_int_eq(management_rpc_error_code(document), 0);
    result = turbo_json_object_get(document, "result");
    check_not_null(result);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "revision")), 3.0, 0.001);
    encoded = turbo_json_string(turbo_json_object_get(result, "secret_base64"));
    check_not_null(encoded);
    check_int_eq(tn_base64_decode(encoded, &first_secret, &first_secret_size), 0);
    check_size_eq(first_secret_size, FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE);
    turbo_free_json(&document);

    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.credential.generate\",\"params\":{"
        "\"principal_id\":\"device-1\",\"request_id\":\"request-generate\","
        "\"expected_revision\":2},\"id\":4}",
        &status);
    check_int_eq(management_rpc_error_code(document), -32010);
    check_null(turbo_json_object_get(document, "result"));
    turbo_free_json(&document);

    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.credential.rotate\",\"params\":{"
        "\"principal_id\":\"device-1\",\"request_id\":\"request-rotate\","
        "\"expected_revision\":3},\"id\":5}",
        &status);
    check_int_eq(management_rpc_error_code(document), 0);
    result = turbo_json_object_get(document, "result");
    check_not_null(result);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "revision")), 4.0, 0.001);
    encoded = turbo_json_string(turbo_json_object_get(result, "secret_base64"));
    check_not_null(encoded);
    check_int_eq(tn_base64_decode(encoded, &rotated_secret, &rotated_secret_size), 0);
    check_size_eq(rotated_secret_size, FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE);
    turbo_free_json(&document);

    check_int_eq(flowie_control_store_credential_verify(store, "root-a", "device-1", first_secret,
                                                        first_secret_size, &verified),
                 TURBO_EPERM);
    verified =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    check_int_eq(flowie_control_store_credential_verify(store, "root-a", "device-1", rotated_secret,
                                                        rotated_secret_size, &verified),
                 TURBO_OK);
    check_uint_eq(verified.credential_revision, 4u);

    document = management_rpc_call(
        server, app, &arena, &security,
        "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.credential.revoke\",\"params\":{"
        "\"principal_id\":\"device-1\",\"request_id\":\"request-revoke\","
        "\"expected_revision\":4},\"id\":6}",
        &status);
    check_int_eq(management_rpc_error_code(document), 0);
    result = turbo_json_object_get(document, "result");
    check_not_null(result);
    check_double_eq(turbo_json_number(turbo_json_object_get(result, "revision")), 5.0, 0.001);
    turbo_free_json(&document);
    check_int_eq(flowie_control_store_credential_verify(store, "root-a", "device-1", rotated_secret,
                                                        rotated_secret_size, &verified),
                 TURBO_EPERM);

    flowie_control_credential_wipe(first_secret, first_secret_size);
    flowie_control_credential_wipe(rotated_secret, rotated_secret_size);
    free(first_secret);
    free(rotated_secret);
    management_rpc_close(server, rpc, app, service, store, path);
    mem_destroy(&arena);
  }
}
