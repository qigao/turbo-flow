#include "flowie_control_service_credential_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

#define SERVICE_CERT_A                                                                            \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
#define SERVICE_CERT_B                                                                            \
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

typedef struct service_secret_fixture_s {
  const char *token_a;
  const char *token_b;
} service_secret_fixture_t;

static int service_secret_acquire(void *ctx, const char *reference,
                                  turbo_flow_security_secret_lease_t *lease) {
  service_secret_fixture_t *fixture = (service_secret_fixture_t *)ctx;
  const char *token = NULL;
  if (!fixture || !reference || !lease || lease->size < sizeof(*lease)) return TURBO_EINVAL;
  if (strcmp(reference, "env://BROKER_A_TOKEN") == 0)
    token = fixture->token_a;
  else if (strcmp(reference, "env://BROKER_B_TOKEN") == 0)
    token = fixture->token_b;
  if (!token) return TURBO_ENOENT;
  lease->bytes = (const uint8_t *)token;
  lease->byte_count = strlen(token);
  lease->provider_lease = fixture;
  return TURBO_OK;
}

static void service_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  (void)ctx;
  if (lease) *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}

static flowie_control_service_credential_resolver_t *service_resolver_create(
    service_secret_fixture_t *fixture, const char *certificate_a) {
  flowie_control_service_credential_binding_t bindings[] = {
      {sizeof(flowie_control_service_credential_binding_t), "broker-a", "env://BROKER_A_TOKEN",
       "root-a", certificate_a},
      {sizeof(flowie_control_service_credential_binding_t), "broker-b", "env://BROKER_B_TOKEN",
       "root-b", NULL}};
  flowie_control_service_credential_config_t config =
      FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT;
  flowie_control_service_credential_resolver_t *resolver = NULL;
  config.listener_id = "flowie-control-auth";
  config.bindings = bindings;
  config.binding_count = sizeof(bindings) / sizeof(bindings[0]);
  config.key_provider = (turbo_flow_security_key_provider_t){
      sizeof(turbo_flow_security_key_provider_t), fixture, service_secret_acquire,
      service_secret_release};
  check_int_eq(flowie_control_service_credential_resolver_create(&config, &resolver), TURBO_OK);
  check_not_null(resolver);
  return resolver;
}

spec("Flowie scoped service credentials") {
  it("maps a bearer token to exactly one service and Root Group without mTLS") {
    service_secret_fixture_t fixture = {"token-a", "token-b"};
    flowie_control_service_credential_resolver_t *resolver =
        service_resolver_create(&fixture, NULL);
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    check_int_eq(flowie_control_service_credential_resolve(
                     resolver, (const uint8_t *)fixture.token_b, strlen(fixture.token_b), NULL,
                     &caller),
                 TURBO_OK);
    check_str_eq(caller.listener_id, "flowie-control-auth");
    check_str_eq(caller.service_id, "broker-b");
    check_str_eq(caller.root_group_id, "root-b");
    check_null(caller.peer_certificate_sha256);
    check_true(caller.authenticated);

    flowie_control_service_credential_resolver_destroy(resolver);
  }

  it("uses a configured client certificate only as an additional service factor") {
    service_secret_fixture_t fixture = {"token-a", "token-b"};
    flowie_control_service_credential_resolver_t *resolver =
        service_resolver_create(&fixture, SERVICE_CERT_A);
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    check_int_eq(flowie_control_service_credential_resolve(
                     resolver, (const uint8_t *)fixture.token_a, strlen(fixture.token_a), NULL,
                     &caller),
                 TURBO_EPERM);
    check_int_eq(flowie_control_service_credential_resolve(
                     resolver, (const uint8_t *)fixture.token_a, strlen(fixture.token_a),
                     SERVICE_CERT_B, &caller),
                 TURBO_EPERM);
    check_int_eq(flowie_control_service_credential_resolve(
                     resolver, (const uint8_t *)fixture.token_a, strlen(fixture.token_a),
                     SERVICE_CERT_A, &caller),
                 TURBO_OK);
    check_str_eq(caller.service_id, "broker-a");
    check_str_eq(caller.root_group_id, "root-a");
    check_str_eq(caller.peer_certificate_sha256, SERVICE_CERT_A);

    flowie_control_service_credential_resolver_destroy(resolver);
  }

  it("fails closed if rotated providers expose one token through multiple bindings") {
    service_secret_fixture_t fixture = {"token-a", "token-b"};
    flowie_control_service_credential_resolver_t *resolver =
        service_resolver_create(&fixture, NULL);
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    fixture.token_b = fixture.token_a;
    check_int_eq(flowie_control_service_credential_resolve(
                     resolver, (const uint8_t *)fixture.token_a, strlen(fixture.token_a), NULL,
                     &caller),
                 TURBO_EPERM);
    check_false(caller.authenticated);

    flowie_control_service_credential_resolver_destroy(resolver);
  }
}
