#include "flowie_control_external_authenticator_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static int external_auth_verify(void *ctx, const flowie_control_external_auth_request_t *request,
                                flowie_control_external_auth_assertion_t *assertion_out) {
  (void)ctx;
  (void)request;
  (void)assertion_out;
  return TURBO_EPERM;
}

static int external_identity_map(void *ctx,
                                 const flowie_control_external_identity_map_request_t *request,
                                 flowie_control_external_identity_map_result_t *result_out) {
  (void)ctx;
  (void)request;
  (void)result_out;
  return TURBO_EPERM;
}

static flowie_control_external_auth_assertion_t valid_assertion(void) {
  flowie_control_external_auth_assertion_t assertion = FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  memcpy(assertion.issuer, "https://idp.example", sizeof("https://idp.example"));
  memcpy(assertion.subject, "tenant-42/device-a", sizeof("tenant-42/device-a"));
  memcpy(assertion.subject_type, "device", sizeof("device"));
  memcpy(assertion.auth_method, "oidc-token", sizeof("oidc-token"));
  assertion.issued_at = 100u;
  assertion.expires_at = 200u;
  assertion.revision = 9u;
  assertion.assurance_level = FLOWIE_CONTROL_EXTERNAL_ASSURANCE_MULTI_FACTOR;
  assertion.account_enabled = 1;
  assertion.external_group_count = 2u;
  memcpy(assertion.external_groups[0], "fleet-a", sizeof("fleet-a"));
  memcpy(assertion.external_groups[1], "operators", sizeof("operators"));
  return assertion;
}

spec("Flowie control external authenticator contract") {
  it("requires versioned capabilities and a complete identity mapper") {
    flowie_control_external_authenticator_t authenticator =
        FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_INIT;
    flowie_control_external_identity_mapper_t mapper = FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_INIT;

    authenticator.capabilities = FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES;
    authenticator.method = "oidc-token";
    authenticator.verify = external_auth_verify;
    mapper.map = external_identity_map;
    check_int_eq(flowie_control_external_authenticator_validate(&authenticator), TURBO_OK);
    check_int_eq(flowie_control_external_identity_mapper_validate(&mapper), TURBO_OK);

    authenticator.capabilities &= ~FLOWIE_CONTROL_EXTERNAL_AUTH_ACCOUNT_STATE;
    check_int_eq(flowie_control_external_authenticator_validate(&authenticator), TURBO_EINVAL);
    authenticator.capabilities = FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES;
    authenticator.version++;
    check_int_eq(flowie_control_external_authenticator_validate(&authenticator), TURBO_EINVAL);
    mapper.map = NULL;
    check_int_eq(flowie_control_external_identity_mapper_validate(&mapper), TURBO_EINVAL);
  }

  it("accepts only bounded, enabled, current, non-ambiguous authentication facts") {
    flowie_control_external_auth_assertion_t assertion = valid_assertion();

    check_int_eq(flowie_control_external_auth_assertion_validate(&assertion, "oidc-token", 150u),
                 TURBO_OK);
    assertion.account_enabled = 0;
    check_int_eq(flowie_control_external_auth_assertion_validate(&assertion, "oidc-token", 150u),
                 TURBO_EINVAL);
    assertion = valid_assertion();
    assertion.expires_at = 150u;
    check_int_eq(flowie_control_external_auth_assertion_validate(&assertion, "oidc-token", 150u),
                 TURBO_EINVAL);
    assertion = valid_assertion();
    assertion.issued_at = 151u;
    check_int_eq(flowie_control_external_auth_assertion_validate(&assertion, "oidc-token", 150u),
                 TURBO_EINVAL);
    assertion = valid_assertion();
    memcpy(assertion.external_groups[1], assertion.external_groups[0],
           sizeof(assertion.external_groups[1]));
    check_int_eq(flowie_control_external_auth_assertion_validate(&assertion, "oidc-token", 150u),
                 TURBO_EINVAL);
    assertion = valid_assertion();
    check_int_eq(flowie_control_external_auth_assertion_validate(&assertion, "password", 150u),
                 TURBO_EINVAL);
  }

  it("requires the mapper to return one bounded local principal id") {
    flowie_control_external_identity_map_result_t result =
        FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_RESULT_INIT;
    check_int_eq(flowie_control_external_identity_map_result_validate(&result), TURBO_EINVAL);
    memcpy(result.principal_id, "device-a", sizeof("device-a"));
    check_int_eq(flowie_control_external_identity_map_result_validate(&result), TURBO_OK);
    result.principal_id[0] = '\n';
    check_int_eq(flowie_control_external_identity_map_result_validate(&result), TURBO_EINVAL);
  }

  it("maps only the configured issuer and subject type to a local principal id") {
    flowie_control_external_subject_mapper_config_t config =
        FLOWIE_CONTROL_EXTERNAL_SUBJECT_MAPPER_CONFIG_INIT;
    flowie_control_external_subject_mapper_t *mapper = NULL;
    const flowie_control_external_identity_mapper_t *interface;
    flowie_control_external_auth_assertion_t assertion = valid_assertion();
    flowie_control_external_identity_map_request_t request =
        FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_REQUEST_INIT;
    flowie_control_external_identity_map_result_t result =
        FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_RESULT_INIT;

    config.trusted_issuer = "https://idp.example";
    config.subject_type = "device";
    check_int_eq(flowie_control_external_subject_mapper_create(&config, &mapper), TURBO_OK);
    check_not_null(mapper);
    interface = flowie_control_external_subject_mapper_interface(mapper);
    check_int_eq(flowie_control_external_identity_mapper_validate(interface), TURBO_OK);
    request.root_group_id = "root-a";
    request.presented_identity = "device@example";
    request.assertion = &assertion;
    check_int_eq(interface->map(interface->ctx, &request, &result), TURBO_OK);
    check_str_eq(result.principal_id, "tenant-42/device-a");

    memcpy(assertion.issuer, "https://other.example", sizeof("https://other.example"));
    check_int_eq(interface->map(interface->ctx, &request, &result), TURBO_EPERM);
    check_str_eq(result.principal_id, "");
    assertion = valid_assertion();
    memcpy(assertion.subject_type, "operator", sizeof("operator"));
    check_int_eq(interface->map(interface->ctx, &request, &result), TURBO_EPERM);

    flowie_control_external_subject_mapper_destroy(mapper);
  }
}
