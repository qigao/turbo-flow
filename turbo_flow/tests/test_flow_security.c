#include "turbo_flow_security.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <stdlib.h>
#include <string.h>

static void security_copy(char *out, size_t capacity, const char *value) {
  size_t size = strlen(value);
  check(size < capacity);
  memcpy(out, value, size + 1u);
}

static turbo_flow_security_principal_t security_principal(const char *tenant,
                                                          uint64_t policy_version) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  security_copy(principal.principal_id, sizeof(principal.principal_id), "device-7");
  security_copy(principal.principal_type, sizeof(principal.principal_type), "device");
  security_copy(principal.tenant_id, sizeof(principal.tenant_id), tenant);
  security_copy(principal.auth_method, sizeof(principal.auth_method), "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_TENANT;
  principal.role_count = 1u;
  security_copy(principal.roles[0], sizeof(principal.roles[0]), "writer");
  principal.policy_version = policy_version;
  return principal;
}

static turbo_flow_security_rule_t security_rule(turbo_flow_security_effect_t effect,
                                                turbo_flow_security_match_kind_t match,
                                                const char *pattern) {
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  rule.effect = effect;
  rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
  security_copy(rule.subject, sizeof(rule.subject), "writer");
  rule.action_mask = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
  rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  rule.match_kind = match;
  security_copy(rule.pattern, sizeof(rule.pattern), pattern);
  return rule;
}

static turbo_flow_security_realm_t *security_realm(const turbo_flow_security_rule_t *rules,
                                                   size_t rule_count) {
  turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
  turbo_flow_security_realm_t *realm = NULL;
  config.resource_uid = "security:main";
  config.owner_name = "security.main";
  config.policy_version = 9u;
  config.rules = rules;
  config.rule_count = rule_count;
  check_int_eq(turbo_flow_security_realm_create(&config, &realm), TURBO_OK);
  check_not_null(realm);
  return realm;
}

static int security_authenticate(void *ctx, const turbo_flow_security_auth_request_t *request,
                                 turbo_flow_security_principal_t *principal) {
  int *calls = (int *)ctx;
  ++*calls;
  if (request->secret_size != sizeof("secret") - 1u ||
      memcmp(request->secret, "secret", sizeof("secret") - 1u) != 0) {
    return TURBO_EPERM;
  }
  *principal = security_principal("tenant-a", 9u);
  return TURBO_OK;
}

typedef struct security_secret_fixture_s {
  int acquire_calls;
  int release_calls;
  int malformed;
  int fail_after_lease;
} security_secret_fixture_t;

static int security_secret_acquire(void *ctx, const char *reference,
                                   turbo_flow_security_secret_lease_t *lease) {
  static const uint8_t bytes[] = {1u, 2u, 3u};
  security_secret_fixture_t *fixture = (security_secret_fixture_t *)ctx;
  ++fixture->acquire_calls;
  if (strcmp(reference, "kms://mqtt/client") != 0) return TURBO_ENOENT;
  lease->bytes = bytes;
  lease->byte_count = sizeof(bytes);
  lease->version = 4u;
  lease->provider_lease = fixture->malformed ? NULL : fixture;
  if (fixture->fail_after_lease) return TURBO_EIO;
  return TURBO_OK;
}

static void security_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  security_secret_fixture_t *fixture = (security_secret_fixture_t *)ctx;
  (void)lease;
  ++fixture->release_calls;
}

spec("security realm v1") {
  it("normalizes provider authentication output without retaining credentials") {
    int calls = 0;
    turbo_flow_security_auth_provider_t provider = {sizeof(provider), &calls,
                                                    security_authenticate};
    turbo_flow_security_auth_request_t request = TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;

    request.identity = "device-7";
    request.method = "token";
    request.secret = (const uint8_t *)"secret";
    request.secret_size = sizeof("secret") - 1u;
    request.protocol = "mqtt5";
    check_int_eq(turbo_flow_security_authenticate(&provider, &request, &principal), TURBO_OK);
    check_int_eq(calls, 1);
    check_str_eq(principal.principal_id, "device-7");
    check_str_eq(principal.tenant_id, "tenant-a");
    check_str_eq(principal.auth_method, "token");
  }

  it("uses explicit deny precedence and default deny") {
    turbo_flow_security_rule_t rules[] = {
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX,
                      "tenant-a/telemetry/"),
        security_rule(TURBO_FLOW_SECURITY_DENY, TURBO_FLOW_SECURITY_MATCH_EXACT,
                      "tenant-a/telemetry/private")};
    turbo_flow_security_realm_t *realm = security_realm(rules, 2u);
    turbo_flow_security_principal_t principal = security_principal("tenant-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    request.principal = &principal;
    request.tenant_id = "tenant-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "tenant-a/telemetry/value";
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_size_eq(decision.matched_rule, 0u);

    request.resource = "tenant-a/telemetry/private";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 TURBO_EPERM);
    check_int_eq(decision.reason, TURBO_FLOW_SECURITY_REASON_DENY_RULE);
    check_size_eq(decision.matched_rule, 1u);

    request.resource = "tenant-a/commands/reboot";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_evaluate(realm, &request, 100u, &decision), TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_DENY);
    check_int_eq(decision.reason, TURBO_FLOW_SECURITY_REASON_DEFAULT_DENY);
    turbo_flow_security_realm_destroy(realm);
  }

  it("rejects tenant escape stale policy and expired principals before rule matching") {
    turbo_flow_security_rule_t rule =
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX, "tenant-b/");
    turbo_flow_security_realm_t *realm = security_realm(&rule, 1u);
    turbo_flow_security_principal_t principal = security_principal("tenant-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    request.principal = &principal;
    request.tenant_id = "tenant-b";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "tenant-b/value";
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 TURBO_EPERM);
    check_int_eq(decision.reason, TURBO_FLOW_SECURITY_REASON_TENANT_MISMATCH);

    principal.scope = TURBO_FLOW_SECURITY_SCOPE_SYSTEM;
    principal.policy_version = 8u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 TURBO_EPERM);
    check_int_eq(decision.reason, TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH);

    principal.policy_version = 9u;
    principal.expires_at = 100u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 TURBO_EPERM);
    check_int_eq(decision.reason, TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED);
    turbo_flow_security_realm_destroy(realm);
  }

  it("creates and exposes a credential-free resource from strict YAML") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  security.main:\n"
                               "    kind: security_realm\n"
                               "    config:\n"
                               "      resource_uid: security:main\n"
                               "      owner_name: security.main\n"
                               "      policy_version: 9\n"
                               "      rules:\n"
                               "        - effect: allow\n"
                               "          subject_kind: role\n"
                               "          subject: writer\n"
                               "          tenant_id: tenant-a\n"
                               "          actions: [publish]\n"
                               "          resource_type: mqtt_topic\n"
                               "          match: prefix\n"
                               "          pattern: tenant-a/telemetry/\n"
                               "adapters: {}\n";
    static const char bad_yaml[] =
        "version: 1\nchannels:\n  security.main:\n    kind: security_realm\n"
        "    config:\n      resource_uid: security:main\n      owner_name: security.main\n"
        "      policy_version: 9\n      plaintext_key: forbidden\n      rules: []\n"
        "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    tstr_t payload;

    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_security_realm_create_resolved(resolved, "security.main", NULL, &realm, &error),
        TURBO_OK);
    check_int_eq(turbo_flow_security_realm_register(flow, realm), TURBO_OK);
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &metadata), TURBO_OK);
    check_int_eq(metadata.kind, TURBO_FLOW_RESOURCE_SECURITY_REALM);
    check_uint_eq(metadata.generation, 9u);
    check_int_eq(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_str_eq(document.schema->type_name, "SecurityRealmStatus");
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_not_null(strstr(payload, "\"policy_version\":\"9\""));
    check_null(strstr(payload, "tenant-a"));
    check_null(strstr(payload, "telemetry"));
    tstr_free(payload);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    realm = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(bad_yaml, sizeof(bad_yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_security_realm_create_resolved(resolved, "security.main", NULL, &realm, &error),
        TURBO_EINVAL);
    check_str_contains(error.path, "plaintext_key");
    check_null(realm);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("requires explicit release for short-lived provider-owned secret leases") {
    security_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t provider = {
        sizeof(provider), &fixture, security_secret_acquire, security_secret_release};
    turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;

    check_int_eq(turbo_flow_security_secret_acquire(&provider, "kms://mqtt/client", &lease),
                 TURBO_OK);
    check_size_eq(lease.byte_count, 3u);
    check_uint_eq(lease.version, 4u);
    turbo_flow_security_secret_release(&provider, &lease);
    check_int_eq(fixture.acquire_calls, 1);
    check_int_eq(fixture.release_calls, 1);
    check_null(lease.bytes);

    fixture.malformed = 1;
    check_int_eq(turbo_flow_security_secret_acquire(&provider, "kms://mqtt/client", &lease),
                 TURBO_EPROTO);
    check_int_eq(fixture.release_calls, 2);

    fixture.malformed = 0;
    fixture.fail_after_lease = 1;
    check_int_eq(turbo_flow_security_secret_acquire(&provider, "kms://mqtt/client", &lease),
                 TURBO_EIO);
    check_int_eq(fixture.release_calls, 3);
  }
}
