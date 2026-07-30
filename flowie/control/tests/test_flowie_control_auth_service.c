#include "flowie_control_auth_repository_contract.h"
#include "flowie_control_auth_service_internal.h"
#include "flowie_control_credential_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define AUTH_SERVICE_CLIENT_CERT                                                                   \
  "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

#define AUTH_SERVICE_CERT_A                                                                        \
  "sha256:"                                                                                        \
  "aaaaaaaaaaaaaaaa"                                                                               \
  "aaaaaaaaaaaaaaaa"                                                                               \
  "aaaaaaaaaaaaaaaa"                                                                               \
  "aaaaaaaaaaaaaaaa"
#define AUTH_SERVICE_CERT_B                                                                        \
  "sha256:"                                                                                        \
  "bbbbbbbbbbbbbbbb"                                                                               \
  "bbbbbbbbbbbbbbbb"                                                                               \
  "bbbbbbbbbbbbbbbb"                                                                               \
  "bbbbbbbbbbbbbbbb"
#define AUTH_SERVICE_CERT_UNKNOWN                                                                  \
  "sha256:"                                                                                        \
  "cccccccccccccccc"                                                                               \
  "cccccccccccccccc"                                                                               \
  "cccccccccccccccc"                                                                               \
  "cccccccccccccccc"

typedef struct auth_service_policy_fixture_s {
  uint64_t root_a_version;
  uint64_t root_b_version;
  int result;
} auth_service_policy_fixture_t;

typedef struct auth_service_external_fixture_s {
  int verify_result;
  int map_result;
  uint64_t expires_at;
  int account_enabled;
  int saw_transport_context;
  int saw_mapping_context;
} auth_service_external_fixture_t;

static int auth_service_policy_version(void *ctx, const char *root_group_id,
                                       uint64_t *policy_version_out) {
  auth_service_policy_fixture_t *fixture = (auth_service_policy_fixture_t *)ctx;
  if (policy_version_out) *policy_version_out = 0u;
  if (!fixture || !root_group_id || !policy_version_out) return TURBO_EINVAL;
  if (fixture->result != TURBO_OK) return fixture->result;
  if (strcmp(root_group_id, "root-a") == 0) *policy_version_out = fixture->root_a_version;
  else if (strcmp(root_group_id, "root-b") == 0) *policy_version_out = fixture->root_b_version;
  else return TURBO_EPERM;
  return TURBO_OK;
}

static uint64_t auth_service_clock(void *ctx) { return *(const uint64_t *)ctx; }

static int auth_service_external_verify(void *ctx,
                                        const flowie_control_external_auth_request_t *request,
                                        flowie_control_external_auth_assertion_t *assertion_out) {
  auth_service_external_fixture_t *fixture = (auth_service_external_fixture_t *)ctx;
  flowie_control_external_auth_assertion_t assertion = FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  if (!fixture || !request || request->size < sizeof(*request) || !assertion_out ||
      assertion_out->size < sizeof(*assertion_out))
    return TURBO_EINVAL;
  fixture->saw_transport_context =
      strcmp(request->root_group_id, "root-a") == 0 &&
      strcmp(request->presented_identity, "external-device") == 0 &&
      strcmp(request->method, "oidc-token") == 0 && strcmp(request->protocol, "mqtt") == 0 &&
      strcmp(request->remote_address, "192.0.2.10:1883") == 0 && request->peer_certificate_sha256 &&
      strcmp(request->peer_certificate_sha256, AUTH_SERVICE_CLIENT_CERT) == 0 &&
      request->secret_size == sizeof("signed-token") - 1u &&
      memcmp(request->secret, "signed-token", sizeof("signed-token") - 1u) == 0;
  if (fixture->verify_result != TURBO_OK) return fixture->verify_result;
  memcpy(assertion.issuer, "https://idp.example", sizeof("https://idp.example"));
  memcpy(assertion.subject, "tenant-42/device-a", sizeof("tenant-42/device-a"));
  memcpy(assertion.subject_type, "device", sizeof("device"));
  memcpy(assertion.auth_method, "oidc-token", sizeof("oidc-token"));
  assertion.issued_at = 9900u;
  assertion.expires_at = fixture->expires_at;
  assertion.revision = 44u;
  assertion.assurance_level = FLOWIE_CONTROL_EXTERNAL_ASSURANCE_MULTI_FACTOR;
  assertion.account_enabled = fixture->account_enabled;
  assertion.external_group_count = 1u;
  memcpy(assertion.external_groups[0], "idp-administrators", sizeof("idp-administrators"));
  *assertion_out = assertion;
  return TURBO_OK;
}

static int auth_service_external_map(void *ctx,
                                     const flowie_control_external_identity_map_request_t *request,
                                     flowie_control_external_identity_map_result_t *result_out) {
  auth_service_external_fixture_t *fixture = (auth_service_external_fixture_t *)ctx;
  flowie_control_external_identity_map_result_t result =
      FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_RESULT_INIT;
  if (!fixture || !request || request->size < sizeof(*request) || !request->assertion ||
      !result_out || result_out->size < sizeof(*result_out))
    return TURBO_EINVAL;
  fixture->saw_mapping_context =
      strcmp(request->root_group_id, "root-a") == 0 &&
      strcmp(request->presented_identity, "external-device") == 0 &&
      strcmp(request->assertion->issuer, "https://idp.example") == 0 &&
      strcmp(request->assertion->subject, "tenant-42/device-a") == 0 &&
      request->assertion->external_group_count == 1u &&
      strcmp(request->assertion->external_groups[0], "idp-administrators") == 0;
  if (fixture->map_result != TURBO_OK) return fixture->map_result;
  memcpy(result.principal_id, "device-a", sizeof("device-a"));
  *result_out = result;
  return TURBO_OK;
}

static flowie_control_store_t *auth_service_store_open(char **path_out) {
  flowie_control_store_config_t config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_store_t *store = NULL;
  *path_out = tt_make_temp_file("flowie-auth-service", ".sqlite3");
  check_not_null(*path_out);
  config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&config, &store), TURBO_OK);
  check_not_null(store);
  return store;
}

static void auth_service_store_close(flowie_control_store_t *store, char *path) {
  flowie_control_store_destroy(store);
  check_int_eq(tt_remove_file(path), 0);
  free(path);
}

static int auth_service_root_create(flowie_control_store_t *store, const char *root_group_id,
                                    const char *request_id, uint64_t expected_revision) {
  flowie_control_root_group_create_command_t command =
      FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = root_group_id;
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 1000u + expected_revision;
  return flowie_control_store_root_group_create(store, &command, &result);
}

static int auth_service_user_create(flowie_control_store_t *store, const char *root_group_id,
                                    const char *request_id, uint64_t expected_revision) {
  flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = root_group_id;
  command.principal_id = "device-a";
  command.principal_type = "device";
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 2000u + expected_revision;
  return flowie_control_store_user_create(store, &command, &result);
}

static int auth_service_credential_generate(flowie_control_store_t *store,
                                            const char *root_group_id, const char *request_id,
                                            uint64_t expected_revision,
                                            flowie_control_generated_credential_t *generated) {
  flowie_control_credential_issue_command_t command = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  command.root_group_id = root_group_id;
  command.principal_id = "device-a";
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 3000u + expected_revision;
  return flowie_control_store_credential_generate(store, &command, generated);
}

static int auth_service_group_create(flowie_control_store_t *store, uint64_t expected_revision) {
  flowie_control_group_create_command_t command = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.group_id = "engineering";
  command.parent_group_id = "root-a";
  command.actor = "admin-1";
  command.request_id = "request-group-a";
  command.expected_revision = expected_revision;
  command.occurred_at = 4000u;
  return flowie_control_store_group_create(store, &command, &result);
}

static int auth_service_membership_add(flowie_control_store_t *store, uint64_t expected_revision) {
  flowie_control_membership_add_command_t command = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "device-a";
  command.group_id = "engineering";
  command.actor = "admin-1";
  command.request_id = "request-membership-a";
  command.expected_revision = expected_revision;
  command.occurred_at = 5000u;
  return flowie_control_store_membership_add(store, &command, &result);
}

static int auth_service_group_create_named(flowie_control_store_t *store, const char *group_id,
                                           const char *request_id, uint64_t expected_revision) {
  flowie_control_group_create_command_t command = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.group_id = group_id;
  command.parent_group_id = "root-a";
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 5100u + expected_revision;
  return flowie_control_store_group_create(store, &command, &result);
}

static int auth_service_membership_add_named(flowie_control_store_t *store, const char *group_id,
                                             const char *request_id, uint64_t expected_revision) {
  flowie_control_membership_add_command_t command = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "device-a";
  command.group_id = group_id;
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 5200u + expected_revision;
  return flowie_control_store_membership_add(store, &command, &result);
}

static int auth_service_role_create(flowie_control_store_t *store, uint64_t expected_revision) {
  flowie_control_role_create_command_t command = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.role_id = "publisher";
  command.actor = "admin-1";
  command.request_id = "request-role-a";
  command.expected_revision = expected_revision;
  command.occurred_at = 6000u;
  return flowie_control_store_role_create(store, &command, &result);
}

static int auth_service_role_add(flowie_control_store_t *store, uint64_t expected_revision) {
  flowie_control_user_role_add_command_t command = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "device-a";
  command.role_id = "publisher";
  command.actor = "admin-1";
  command.request_id = "request-user-role-a";
  command.expected_revision = expected_revision;
  command.occurred_at = 7000u;
  return flowie_control_store_user_role_add(store, &command, &result);
}

static int auth_service_credential_revoke(flowie_control_store_t *store,
                                          uint64_t expected_revision) {
  flowie_control_credential_revoke_command_t command =
      FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "device-a";
  command.actor = "admin-1";
  command.request_id = "request-revoke-a";
  command.expected_revision = expected_revision;
  command.occurred_at = 8000u;
  return flowie_control_store_credential_revoke(store, &command, &result);
}

static int auth_service_user_disable(flowie_control_store_t *store, uint64_t expected_revision) {
  flowie_control_user_disable_command_t command = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "device-a";
  command.actor = "admin-1";
  command.request_id = "request-disable-external-user";
  command.expected_revision = expected_revision;
  command.occurred_at = 8100u;
  return flowie_control_store_user_disable(store, &command, &result);
}

static flowie_control_auth_service_t *
auth_service_create(flowie_control_store_t *store, auth_service_policy_fixture_t *policy,
                    uint64_t *now_seconds) {
  flowie_control_auth_service_config_t config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_auth_service_t *service = NULL;
  config.repository = flowie_control_store_repository(store);
  config.policy_version.ctx = policy;
  config.policy_version.current = auth_service_policy_version;
  config.clock_seconds = auth_service_clock;
  config.clock_ctx = now_seconds;
  check_int_eq(flowie_control_auth_service_create(&config, &service), TURBO_OK);
  check_not_null(service);
  return service;
}

static int auth_service_group_present(const turbo_flow_security_principal_t *principal,
                                      const char *group) {
  for (uint32_t index = 0u; index < principal->group_count; ++index)
    if (strcmp(principal->groups[index], group) == 0) return 1;
  return 0;
}

spec("Flowie control trusted authentication service") {
  it("uses the provider-neutral Repository for local Auth and ACL generation") {
    char *path = NULL;
    flowie_control_store_t *store = auth_service_store_open(&path);
    flowie_control_auth_repository_contract_run(flowie_control_store_repository(store));
    auth_service_store_close(store, path);
  }

  it("authenticates duplicate MQTT identities only inside the service credential Root Group") {
    char *path = NULL;
    flowie_control_store_t *store = auth_service_store_open(&path);
    flowie_control_generated_credential_t root_a = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_generated_credential_t root_b = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    auth_service_policy_fixture_t policy = {11u, 12u, TURBO_OK};
    uint64_t now_seconds = 10000u;
    flowie_control_auth_service_t *service;
    flowie_control_verified_caller_t caller = {sizeof(flowie_control_verified_caller_t),
                                               "auth-listener", "broker-a", "root-a",
                                               AUTH_SERVICE_CERT_A, 1};
    flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    int cache_hit = -1;

    check_int_eq(auth_service_root_create(store, "root-a", "request-root-a", 0u), TURBO_OK);
    check_int_eq(auth_service_root_create(store, "root-b", "request-root-b", 1u), TURBO_OK);
    check_int_eq(auth_service_user_create(store, "root-a", "request-user-a", 2u), TURBO_OK);
    check_int_eq(
        auth_service_credential_generate(store, "root-a", "request-credential-a", 3u, &root_a),
        TURBO_OK);
    check_int_eq(auth_service_user_create(store, "root-b", "request-user-b", 4u), TURBO_OK);
    check_int_eq(
        auth_service_credential_generate(store, "root-b", "request-credential-b", 5u, &root_b),
        TURBO_OK);
    check_int_eq(auth_service_group_create(store, 6u), TURBO_OK);
    check_int_eq(auth_service_membership_add(store, 7u), TURBO_OK);
    check_int_eq(auth_service_role_create(store, 8u), TURBO_OK);
    check_int_eq(auth_service_role_add(store, 9u), TURBO_OK);
    service = auth_service_create(store, &policy, &now_seconds);

    request.caller = &caller;
    request.identity = "device-a";
    request.method = "password";
    request.secret = root_a.secret;
    request.secret_size = root_a.secret_size;
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_OK);
    check_false(cache_hit);
    check_str_eq(principal.principal_id, "device-a");
    check_str_eq(principal.root_group_id, "root-a");
    check_str_eq(principal.principal_type, "device");
    check_str_eq(principal.auth_method, "password");
    check_uint_eq(principal.scope, TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP);
    check_uint_eq(principal.policy_version, 11u);
    check_uint_eq(principal.expires_at, 10300u);
    check_true(auth_service_group_present(&principal, "root-a"));
    check_true(auth_service_group_present(&principal, "engineering"));
    check_uint_eq(principal.role_count, 1u);
    check_str_eq(principal.roles[0], "publisher");
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_OK);
    check_true(cache_hit);
    check_int_eq(
        auth_service_group_create_named(store, "operations", "request-group-operations", 10u),
        TURBO_OK);
    check_int_eq(auth_service_membership_add_named(store, "operations",
                                                   "request-membership-operations", 11u),
                 TURBO_OK);
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_OK);
    check_true(cache_hit);
    check_true(auth_service_group_present(&principal, "operations"));
    check_uint_eq(principal.policy_version, 11u);

    request.secret = root_b.secret;
    request.secret_size = root_b.secret_size;
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_EPERM);
    check_uint_eq(principal.policy_version, 0u);
    caller.root_group_id = "root-b";
    caller.service_id = "broker-b";
    caller.peer_certificate_sha256 = AUTH_SERVICE_CERT_B;
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_OK);
    check_str_eq(principal.root_group_id, "root-b");
    check_uint_eq(principal.group_count, 1u);
    check_str_eq(principal.groups[0], "root-b");
    check_uint_eq(principal.policy_version, 12u);

    caller.root_group_id = "";
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_EPERM);
    caller.root_group_id = "root-b";
    caller.authenticated = 0;
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_EPERM);

    flowie_control_auth_service_destroy(service);
    flowie_control_generated_credential_wipe(&root_a);
    flowie_control_generated_credential_wipe(&root_b);
    auth_service_store_close(store, path);
  }

  it("MQTT-SEC-006/007 fails closed on policy outage and revoked credentials") {
    char *path = NULL;
    flowie_control_store_t *store = auth_service_store_open(&path);
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    auth_service_policy_fixture_t policy = {21u, 0u, TURBO_EIO};
    uint64_t now_seconds = 9000u;
    flowie_control_auth_service_t *service;
    flowie_control_verified_caller_t caller = {sizeof(flowie_control_verified_caller_t),
                                               "listener-a", "broker-a", "root-a",
                                               AUTH_SERVICE_CERT_A, 1};
    flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;

    check_int_eq(auth_service_root_create(store, "root-a", "request-root-a", 0u), TURBO_OK);
    check_int_eq(auth_service_user_create(store, "root-a", "request-user-a", 1u), TURBO_OK);
    check_int_eq(
        auth_service_credential_generate(store, "root-a", "request-credential-a", 2u, &generated),
        TURBO_OK);
    service = auth_service_create(store, &policy, &now_seconds);
    request.caller = &caller;
    request.identity = "device-a";
    request.method = "password";
    request.secret = generated.secret;
    request.secret_size = generated.secret_size;

    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EIO);
    check_uint_eq(principal.policy_version, 0u);
    policy.result = TURBO_OK;
    policy.root_a_version = 0u;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPROTO);
    policy.root_a_version = 21u;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_OK);
    check_int_eq(auth_service_credential_revoke(store, 3u), TURBO_OK);
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);

    flowie_control_auth_service_destroy(service);
    flowie_control_generated_credential_wipe(&generated);
    auth_service_store_close(store, path);
  }

  it("uses third-party authentication facts while keeping local authorization authoritative") {
    char *path = NULL;
    flowie_control_store_t *store = auth_service_store_open(&path);
    auth_service_policy_fixture_t policy = {31u, 0u, TURBO_OK};
    auth_service_external_fixture_t external = {TURBO_OK, TURBO_OK, 10120u, 1, 0, 0};
    flowie_control_external_authenticator_t authenticator =
        FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_INIT;
    flowie_control_external_identity_mapper_t mapper = FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_INIT;
    flowie_control_auth_service_config_t config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
    flowie_control_auth_service_t *service = NULL;
    flowie_control_verified_caller_t caller = {sizeof(flowie_control_verified_caller_t),
                                               "listener-a", "broker-a", "root-a",
                                               AUTH_SERVICE_CERT_A, 1};
    flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    uint64_t now_seconds = 10000u;
    int cache_hit = 1;

    check_int_eq(auth_service_root_create(store, "root-a", "external-root", 0u), TURBO_OK);
    check_int_eq(auth_service_user_create(store, "root-a", "external-user", 1u), TURBO_OK);
    check_int_eq(auth_service_group_create(store, 2u), TURBO_OK);
    check_int_eq(auth_service_membership_add(store, 3u), TURBO_OK);
    check_int_eq(auth_service_role_create(store, 4u), TURBO_OK);
    check_int_eq(auth_service_role_add(store, 5u), TURBO_OK);

    authenticator.capabilities = FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES |
                                 FLOWIE_CONTROL_EXTERNAL_AUTH_GROUP_CLAIMS;
    authenticator.ctx = &external;
    authenticator.method = "oidc-token";
    authenticator.verify = auth_service_external_verify;
    mapper.ctx = &external;
    mapper.map = auth_service_external_map;
    config.repository = flowie_control_store_repository(store);
    config.method = "oidc-token";
    config.policy_version.ctx = &policy;
    config.policy_version.current = auth_service_policy_version;
    config.external_authenticator = &authenticator;
    config.clock_seconds = auth_service_clock;
    config.clock_ctx = &now_seconds;
    check_int_eq(flowie_control_auth_service_create(&config, &service), TURBO_EINVAL);
    check_null(service);
    config.external_identity_mapper = &mapper;
    check_int_eq(flowie_control_auth_service_create(&config, &service), TURBO_OK);
    check_not_null(service);

    request.caller = &caller;
    request.identity = "external-device";
    request.method = "oidc-token";
    request.secret = (const uint8_t *)"signed-token";
    request.secret_size = sizeof("signed-token") - 1u;
    request.protocol = "mqtt";
    request.remote_address = "192.0.2.10:1883";
    request.peer_certificate_sha256 = AUTH_SERVICE_CLIENT_CERT;
    check_int_eq(
        flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
        TURBO_OK);
    check_true(external.saw_transport_context);
    check_true(external.saw_mapping_context);
    check_false(cache_hit);
    check_str_eq(principal.principal_id, "device-a");
    check_str_eq(principal.auth_method, "oidc-token");
    check_uint_eq(principal.expires_at, 10120u);
    check_true(auth_service_group_present(&principal, "root-a"));
    check_true(auth_service_group_present(&principal, "engineering"));
    check_false(auth_service_group_present(&principal, "idp-administrators"));
    check_uint_eq(principal.role_count, 1u);
    check_str_eq(principal.roles[0], "publisher");

    external.map_result = TURBO_EPERM;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);
    check_str_eq(principal.principal_id, "");
    external.map_result = TURBO_OK;
    external.verify_result = TURBO_EIO;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EIO);
    check_str_eq(principal.principal_id, "");
    external.verify_result = TURBO_OK;
    external.expires_at = now_seconds;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);
    external.expires_at = 10120u;
    external.account_enabled = 0;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);
    external.account_enabled = 1;
    check_int_eq(auth_service_user_disable(store, 6u), TURBO_OK);
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);

    flowie_control_auth_service_destroy(service);
    auth_service_store_close(store, path);
  }

  it("limits repeated credential failures before KDF evaluation") {
    char *path = NULL;
    flowie_control_store_t *store = auth_service_store_open(&path);
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    auth_service_policy_fixture_t policy = {1u, 0u, TURBO_OK};
    flowie_control_auth_service_config_t config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
    flowie_control_auth_service_t *service = NULL;
    flowie_control_verified_caller_t caller = {sizeof(flowie_control_verified_caller_t),
                                               "listener-a", "broker-a", "root-a",
                                               AUTH_SERVICE_CERT_A, 1};
    flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    uint64_t now_ms = 100u;
    static const uint8_t wrong_secret[] = "wrong-secret";

    check_int_eq(auth_service_root_create(store, "root-a", "request-rate-root", 0u), TURBO_OK);
    check_int_eq(auth_service_user_create(store, "root-a", "request-rate-user", 1u), TURBO_OK);
    check_int_eq(auth_service_credential_generate(store, "root-a", "request-rate-credential", 2u,
                                                  &generated),
                 TURBO_OK);
    config.repository = flowie_control_store_repository(store);
    config.policy_version.ctx = &policy;
    config.policy_version.current = auth_service_policy_version;
    config.rate_limiter.caller_capacity = 1u;
    config.rate_limiter.identity_capacity = 1u;
    config.rate_limiter.caller_per_second = 100u;
    config.rate_limiter.caller_burst = 10u;
    config.rate_limiter.identity_per_second = 1u;
    config.rate_limiter.identity_burst = 2u;
    config.rate_limiter.clock_ms = auth_service_clock;
    config.rate_limiter.clock_ctx = &now_ms;
    check_int_eq(flowie_control_auth_service_create(&config, &service), TURBO_OK);
    request.caller = &caller;
    request.identity = "device-a";
    request.method = "password";
    request.secret = wrong_secret;
    request.secret_size = sizeof(wrong_secret) - 1u;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EBUSY);
    now_ms = 1100u;
    check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
                 TURBO_EPERM);

    flowie_control_auth_service_destroy(service);
    flowie_control_generated_credential_wipe(&generated);
    auth_service_store_close(store, path);
  }
}
