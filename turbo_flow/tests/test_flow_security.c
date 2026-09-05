#include "turbo_flow_security.h"

#include "tinytest.h"
#include "salts_error.h"
#include "salts_str.h"

#include <stdlib.h>
#include <string.h>

static void security_copy(char *out, size_t capacity, const char *value) {
  size_t size = strlen(value);
  check(size < capacity);
  memcpy(out, value, size + 1u);
}

static turbo_flow_security_principal_t security_principal(const char *domain,
                                                          uint64_t policy_version) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  security_copy(principal.principal_id, sizeof(principal.principal_id), "device-7");
  security_copy(principal.principal_type, sizeof(principal.principal_type), "device");
  security_copy(principal.domain_id, sizeof(principal.domain_id), domain);
  security_copy(principal.auth_method, sizeof(principal.auth_method), "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
  principal.role_count = 1u;
  security_copy(principal.roles[0], sizeof(principal.roles[0]), "writer");
  principal.group_count = 1u;
  security_copy(principal.groups[0], sizeof(principal.groups[0]), domain);
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
  security_copy(rule.domain_id, sizeof(rule.domain_id), "root-a");
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
  check_equal(turbo_flow_security_realm_create(&config, &realm), SALTS_OK);
  check_not_null(realm);
  return realm;
}

static int security_authenticate(void *ctx, const turbo_flow_security_auth_request_t *request,
                                 turbo_flow_security_principal_t *principal) {
  int *calls = (int *)ctx;
  ++*calls;
  if (request->secret_size != sizeof("secret") - 1u ||
      memcmp(request->secret, "secret", sizeof("secret") - 1u) != 0) {
    return SALTS_EPERM;
  }
  *principal = security_principal("root-a", 9u);
  return SALTS_OK;
}

typedef struct security_secret_fixture_s {
  int acquire_calls;
  int release_calls;
  int malformed;
  int fail_after_lease;
} security_secret_fixture_t;

typedef struct security_enhanced_fixture_s {
  int begin_calls;
  int continue_calls;
  int cancel_calls;
  int omit_exchange;
} security_enhanced_fixture_t;

typedef struct security_policy_fixture_s {
  turbo_flow_security_rule_t rule;
  int load_calls;
  int release_calls;
} security_policy_fixture_t;

typedef struct security_matcher_fixture_s {
  size_t candidate_count;
  size_t emit_position;
  int compile_calls;
  int evaluate_calls;
  int destroy_calls;
} security_matcher_fixture_t;

static int security_matcher_compile(void *ctx, const turbo_flow_security_matcher_leaf_t *input,
                                    void **compiled_leaf_out) {
  security_matcher_fixture_t *fixture = (security_matcher_fixture_t *)ctx;
  if (!fixture || !input || input->size < sizeof(*input) || !input->rules ||
      !input->candidate_rule_indices || input->candidate_count == 0u || !compiled_leaf_out)
    return SALTS_EINVAL;
  ++fixture->compile_calls;
  fixture->candidate_count = input->candidate_count;
  *compiled_leaf_out = fixture;
  return SALTS_OK;
}

static int security_matcher_evaluate(void *ctx, const void *compiled_leaf,
                                     const turbo_flow_security_request_t *request,
                                     turbo_flow_security_match_emit_fn emit, void *emit_ctx) {
  security_matcher_fixture_t *fixture = (security_matcher_fixture_t *)ctx;
  if (!fixture || compiled_leaf != fixture || !request || !emit) return SALTS_EINVAL;
  ++fixture->evaluate_calls;
  return emit(emit_ctx, fixture->emit_position);
}

static void security_matcher_destroy(void *ctx, void *compiled_leaf) {
  security_matcher_fixture_t *fixture = (security_matcher_fixture_t *)ctx;
  check_equal((const void *)compiled_leaf, (const void *)fixture);
  ++fixture->destroy_calls;
}

static int security_policy_load(void *ctx, uint64_t required_version,
                                turbo_flow_security_policy_bundle_t *bundle_out) {
  security_policy_fixture_t *fixture = (security_policy_fixture_t *)ctx;
  ++fixture->load_calls;
  if (required_version != 9u) return SALTS_ENOENT;
  *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  bundle_out->policy_version = 9u;
  bundle_out->rules = &fixture->rule;
  bundle_out->rule_count = 1u;
  bundle_out->provider_bundle = fixture;
  return SALTS_OK;
}

static void security_policy_release(void *ctx, turbo_flow_security_policy_bundle_t *bundle) {
  security_policy_fixture_t *fixture = (security_policy_fixture_t *)ctx;
  check_equal((const void *)bundle->provider_bundle, (const void *)fixture);
  ++fixture->release_calls;
  memset(&fixture->rule, 0xa5, sizeof(fixture->rule));
  *bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
}

static int security_enhanced_begin(void *ctx,
                                   const turbo_flow_security_enhanced_auth_request_t *request,
                                   void **exchange_out,
                                   turbo_flow_security_enhanced_auth_result_t *result_out) {
  static const uint8_t challenge[] = "server-first";
  security_enhanced_fixture_t *fixture = (security_enhanced_fixture_t *)ctx;
  ++fixture->begin_calls;
  check_equal(request->identity, "device-7");
  check_equal(request->method, "token");
  check_equal(request->data_size, sizeof("client-first") - 1u);
  *exchange_out = fixture->omit_exchange ? NULL : fixture;
  result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE;
  result_out->data = challenge;
  result_out->data_size = sizeof(challenge) - 1u;
  return SALTS_OK;
}

static int security_enhanced_continue(void *ctx, void *exchange,
                                      const turbo_flow_security_enhanced_auth_request_t *request,
                                      turbo_flow_security_enhanced_auth_result_t *result_out) {
  static const uint8_t final_data[] = "server-final";
  security_enhanced_fixture_t *fixture = (security_enhanced_fixture_t *)ctx;
  check_equal((const void *)exchange, (const void *)fixture);
  ++fixture->continue_calls;
  check_equal(request->method, "token");
  check_equal(request->data_size, sizeof("client-final") - 1u);
  result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS;
  result_out->data = final_data;
  result_out->data_size = sizeof(final_data) - 1u;
  result_out->principal = security_principal("root-a", 9u);
  return SALTS_OK;
}

static void security_enhanced_cancel(void *ctx, void *exchange) {
  security_enhanced_fixture_t *fixture = (security_enhanced_fixture_t *)ctx;
  check_equal((const void *)exchange, (const void *)fixture);
  ++fixture->cancel_calls;
}

static int security_secret_acquire(void *ctx, const char *reference,
                                   turbo_flow_security_secret_lease_t *lease) {
  static const uint8_t bytes[] = {1u, 2u, 3u};
  security_secret_fixture_t *fixture = (security_secret_fixture_t *)ctx;
  ++fixture->acquire_calls;
  if (strcmp(reference, "kms://mqtt/client") != 0) return SALTS_ENOENT;
  lease->bytes = bytes;
  lease->byte_count = sizeof(bytes);
  lease->version = 4u;
  lease->provider_lease = fixture->malformed ? NULL : fixture;
  if (fixture->fail_after_lease) return SALTS_EIO;
  return SALTS_OK;
}

static void security_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  security_secret_fixture_t *fixture = (security_secret_fixture_t *)ctx;
  (void)lease;
  ++fixture->release_calls;
}

spec("security realm v3") {
  it("parses canonical rule lines with re2c keywords and escaped separators") {
    static const char line[] =
        "allow|role|writer|root-a|publish,subscribe|mqtt_topic|adapter|root-a/tele\\|metry/#\n";
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;

    check_equal(turbo_flow_security_rule_parse_line(line, sizeof(line) - 1u, &rule), SALTS_OK);
    check_equal(rule.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_equal(rule.subject_kind, TURBO_FLOW_SECURITY_SUBJECT_ROLE);
    check_equal(rule.subject, "writer");
    check_equal(rule.domain_id, "root-a");
    check_equal(rule.action_mask,
                 TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE);
    check_equal(rule.resource_type, TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC);
    check_equal(rule.match_kind, TURBO_FLOW_SECURITY_MATCH_ADAPTER);
    check_equal(rule.pattern, "root-a/tele|metry/#");

    {
      char formatted[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u] = {0};
      size_t formatted_size = 0u;
      turbo_flow_security_rule_t round_trip = TURBO_FLOW_SECURITY_RULE_INIT;
      check_equal(turbo_flow_security_rule_format_line(&rule, formatted, sizeof(formatted),
                                                        &formatted_size),
                   SALTS_OK);
      check_equal(turbo_flow_security_rule_parse_line(formatted, formatted_size, &round_trip),
                   SALTS_OK);
      check_equal(round_trip.pattern, rule.pattern);
      check_equal(round_trip.action_mask, rule.action_mask);
    }
  }

  it("rejects ambiguous or malformed canonical rule lines") {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    static const char duplicate_action[] =
        "allow|role|writer|root-a|publish,publish|mqtt_topic|exact|root-a/#";
    static const char invalid_any[] =
        "allow|any|anonymous|root-a|publish|mqtt_topic|exact|root-a/#";

    check_equal(
        turbo_flow_security_rule_parse_line(duplicate_action, sizeof(duplicate_action) - 1u, &rule),
        SALTS_EPROTO);
    check_equal(turbo_flow_security_rule_parse_line(invalid_any, sizeof(invalid_any) - 1u, &rule),
                 SALTS_EPROTO);
  }

  it("round trips and evaluates an empty prefix only for a connect parent rule") {
    static const char line[] =
        "allow|principal|device-7|root-a|connect|generic|prefix|";
    static const char invalid[] =
        "allow|principal|device-7|root-a|publish|mqtt_topic|prefix|";
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    turbo_flow_security_realm_t *realm;
    char formatted[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u] = {0};
    size_t formatted_size = 0u;

    check_equal(turbo_flow_security_rule_parse_line(line, sizeof(line) - 1u, &rule), SALTS_OK);
    check_equal(turbo_flow_security_rule_format_line(&rule, formatted, sizeof(formatted),
                                                      &formatted_size),
                 SALTS_OK);
    check_equal(formatted, line);
    check_equal(turbo_flow_security_rule_parse_line(invalid, sizeof(invalid) - 1u, &rule),
                 SALTS_EPROTO);

    check_equal(turbo_flow_security_rule_parse_line(line, sizeof(line) - 1u, &rule), SALTS_OK);
    realm = security_realm(&rule, 1u);
    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    request.resource = "arbitrary-client-id";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    turbo_flow_security_realm_destroy(realm);
  }

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
    check_equal(turbo_flow_security_authenticate(&provider, &request, &principal), SALTS_OK);
    check_equal(calls, 1);
    check_equal(principal.principal_id, "device-7");
    check_equal(principal.domain_id, "root-a");
    check_equal(principal.auth_method, "token");
    request.size = TURBO_FLOW_SECURITY_AUTH_REQUEST_BASE_SIZE;
    principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    check_equal(turbo_flow_security_authenticate(&provider, &request, &principal), SALTS_OK);
    check_equal(calls, 2);
    request.size = TURBO_FLOW_SECURITY_AUTH_REQUEST_BASE_SIZE - 1u;
    check_equal(turbo_flow_security_authenticate(&provider, &request, &principal), SALTS_EINVAL);
    check_equal(calls, 2);
  }

  it("validates and owns enhanced authentication exchange lifecycle") {
    security_enhanced_fixture_t fixture = {0};
    turbo_flow_security_enhanced_auth_provider_t provider = {
        sizeof(provider), &fixture, security_enhanced_begin, security_enhanced_continue,
        security_enhanced_cancel};
    turbo_flow_security_enhanced_auth_request_t request =
        TURBO_FLOW_SECURITY_ENHANCED_AUTH_REQUEST_INIT;
    turbo_flow_security_enhanced_auth_result_t result =
        TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT;
    void *exchange = NULL;

    request.identity = "device-7";
    request.method = "token";
    request.data = (const uint8_t *)"client-first";
    request.data_size = sizeof("client-first") - 1u;
    request.protocol = "mqtt5";
    request.size = TURBO_FLOW_SECURITY_ENHANCED_AUTH_REQUEST_BASE_SIZE;
    check_equal(turbo_flow_security_enhanced_auth_begin(&provider, &request, &exchange, &result),
                 SALTS_OK);
    check_equal((const void *)exchange, (const void *)&fixture);
    check_equal(result.status, TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE);
    check_equal(result.data_size, sizeof("server-first") - 1u);

    request.data = (const uint8_t *)"client-final";
    request.data_size = sizeof("client-final") - 1u;
    result =
        (turbo_flow_security_enhanced_auth_result_t)TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT;
    check_equal(turbo_flow_security_enhanced_auth_continue(&provider, exchange, &request, &result),
                 SALTS_OK);
    check_equal(result.status, TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS);
    check_equal(result.principal.principal_id, "device-7");
    turbo_flow_security_enhanced_auth_cancel(&provider, exchange);
    check_equal(fixture.begin_calls, 1);
    check_equal(fixture.continue_calls, 1);
    check_equal(fixture.cancel_calls, 1);

    request.size = TURBO_FLOW_SECURITY_ENHANCED_AUTH_REQUEST_BASE_SIZE - 1u;
    exchange = NULL;
    result =
        (turbo_flow_security_enhanced_auth_result_t)TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT;
    check_equal(turbo_flow_security_enhanced_auth_begin(&provider, &request, &exchange, &result),
                 SALTS_EINVAL);
    check_equal(fixture.begin_calls, 1);

    fixture.omit_exchange = 1;
    request.size = TURBO_FLOW_SECURITY_ENHANCED_AUTH_REQUEST_BASE_SIZE;
    exchange = NULL;
    result =
        (turbo_flow_security_enhanced_auth_result_t)TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT;
    request.data = (const uint8_t *)"client-first";
    request.data_size = sizeof("client-first") - 1u;
    check_equal(turbo_flow_security_enhanced_auth_begin(&provider, &request, &exchange, &result),
                 SALTS_EPROTO);
    check_null(exchange);
  }

  it("uses explicit deny precedence and default deny") {
    turbo_flow_security_rule_t rules[] = {
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX,
                      "root-a/telemetry/"),
        security_rule(TURBO_FLOW_SECURITY_DENY, TURBO_FLOW_SECURITY_MATCH_EXACT,
                      "root-a/telemetry/private")};
    turbo_flow_security_realm_t *realm = security_realm(rules, 2u);
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/telemetry/value";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_equal(decision.matched_rule, 0u);

    request.resource = "root-a/telemetry/private";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 SALTS_EPERM);
    check_equal(decision.reason, TURBO_FLOW_SECURITY_REASON_DENY_RULE);
    check_equal(decision.matched_rule, 1u);

    request.resource = "root-a/commands/reboot";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_evaluate(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_DENY);
    check_equal(decision.reason, TURBO_FLOW_SECURITY_REASON_DEFAULT_DENY);
    turbo_flow_security_realm_destroy(realm);
  }

  it("preserves deny precedence across indexed prefix leaves") {
    turbo_flow_security_rule_t rules[] = {
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX, "root-a/"),
        security_rule(TURBO_FLOW_SECURITY_DENY, TURBO_FLOW_SECURITY_MATCH_PREFIX,
                      "root-a/telemetry/private"),
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX,
                      "root-a/telemetry/")};
    turbo_flow_security_realm_t *realm = security_realm(rules, 3u);
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/telemetry/private/value";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 SALTS_EPERM);
    check_equal(decision.reason, TURBO_FLOW_SECURITY_REASON_DENY_RULE);
    check_equal(decision.matched_rule, 1u);

    request.resource = "root-a/telemetry/public/value";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_equal(decision.matched_rule, 0u);
    turbo_flow_security_realm_destroy(realm);
  }

  it("bounds compiled matcher output to its immutable subject leaf") {
    security_matcher_fixture_t fixture = {0};
    turbo_flow_security_rule_t rule =
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_ADAPTER, "root-a/#");
    turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    turbo_flow_security_realm_t *realm = NULL;

    config.resource_uid = "security:compiled-matcher";
    config.owner_name = "security.compiled-matcher";
    config.policy_version = 9u;
    config.rules = &rule;
    config.rule_count = 1u;
    config.matcher.ctx = &fixture;
    config.matcher.compile_leaf = security_matcher_compile;
    config.matcher.evaluate_leaf = security_matcher_evaluate;
    config.matcher.destroy_leaf = security_matcher_destroy;
    check_equal(turbo_flow_security_realm_create(&config, &realm), SALTS_OK);
    check_equal(fixture.compile_calls, 1);
    check_equal(fixture.candidate_count, 1u);

    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/device-7";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_ALLOW);

    fixture.emit_position = fixture.candidate_count;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_evaluate(realm, &request, 100u, &decision),
                 SALTS_EPROTO);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_DENY);
    turbo_flow_security_realm_destroy(realm);
    check_equal(fixture.evaluate_calls, 2);
    check_equal(fixture.destroy_calls, 1);
  }

  it("rejects an incomplete compiled matcher lifecycle") {
    security_matcher_fixture_t fixture = {0};
    turbo_flow_security_rule_t rule =
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_ADAPTER, "root-a/#");
    turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;

    config.resource_uid = "security:incomplete-matcher";
    config.owner_name = "security.incomplete-matcher";
    config.policy_version = 9u;
    config.rules = &rule;
    config.rule_count = 1u;
    config.matcher.ctx = &fixture;
    config.matcher.compile_leaf = security_matcher_compile;
    check_equal(turbo_flow_security_realm_create(&config, &realm), SALTS_EINVAL);
    check_null(realm);
    check_equal(fixture.compile_calls, 0);
  }

  it("keeps compiled subject keys after a provider releases its bundle") {
    security_policy_fixture_t fixture = {0};
    turbo_flow_security_policy_provider_t provider =
        (turbo_flow_security_policy_provider_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_INIT;
    turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    fixture.rule = security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX,
                                 "root-a/telemetry/");
    provider.ctx = &fixture;
    provider.load = security_policy_load;
    provider.release = security_policy_release;
    config.resource_uid = "security:provider-lifetime";
    config.owner_name = "security.provider-lifetime";
    config.policy_source = "acl.fixture";
    check_equal(turbo_flow_security_realm_create(&config, &realm), SALTS_OK);
    check_equal(turbo_flow_security_realm_bind_policy_provider(realm, &provider), SALTS_OK);

    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/telemetry/value";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    check_equal(fixture.load_calls, 1);
    check_equal(fixture.release_calls, 1);
    turbo_flow_security_realm_destroy(realm);
  }

  it("authorizes nested members through their bounded effective-group ancestry") {
    turbo_flow_security_rule_t rule =
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX, "devices/");
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    turbo_flow_security_realm_t *realm;

    rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_GROUP;
    security_copy(rule.subject, sizeof(rule.subject), "engineering");
    principal.group_count = 3u;
    security_copy(principal.groups[0], sizeof(principal.groups[0]), "root-a");
    security_copy(principal.groups[1], sizeof(principal.groups[1]), "engineering");
    security_copy(principal.groups[2], sizeof(principal.groups[2]), "backend");
    realm = security_realm(&rule, 1u);
    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "devices/7/events";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), SALTS_OK);
    check_equal(decision.effect, TURBO_FLOW_SECURITY_ALLOW);

    security_copy(principal.groups[0], sizeof(principal.groups[0]), "backend");
    principal.group_count = 1u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 SALTS_EPERM);
    turbo_flow_security_realm_destroy(realm);
  }

  it("rejects domain escape stale policy and expired principals before rule matching") {
    turbo_flow_security_rule_t rule =
        security_rule(TURBO_FLOW_SECURITY_ALLOW, TURBO_FLOW_SECURITY_MATCH_PREFIX, "root-b/");
    security_copy(rule.domain_id, sizeof(rule.domain_id), "root-b");
    turbo_flow_security_realm_t *realm = security_realm(&rule, 1u);
    turbo_flow_security_principal_t principal = security_principal("root-a", 9u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    request.principal = &principal;
    request.domain_id = "root-b";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-b/value";
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 SALTS_EPERM);
    check_equal(decision.reason, TURBO_FLOW_SECURITY_REASON_DOMAIN_MISMATCH);

    principal.scope = TURBO_FLOW_SECURITY_SCOPE_SYSTEM;
    principal.policy_version = 8u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 SALTS_EPERM);
    check_equal(decision.reason, TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH);

    principal.policy_version = 9u;
    principal.expires_at = 100u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_equal(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 SALTS_EPERM);
    check_equal(decision.reason, TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED);
    turbo_flow_security_realm_destroy(realm);
  }

  it("creates a fail-closed realm from YAML metadata without ACL bodies") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  security.main:\n"
                               "    kind: security_realm\n"
                               "    config:\n"
                               "      resource_uid: security:main\n"
                               "      owner_name: security.main\n"
                               "      policy_source: acl.provider\n"
                               "adapters: {}\n";
    static const char bad_yaml[] =
        "version: 1\nchannels:\n  security.main:\n    kind: security_realm\n"
        "    config:\n      resource_uid: security:main\n      owner_name: security.main\n"
        "      policy_source: acl.provider\n      rules: []\n"
        "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    tstr payload;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 SALTS_OK);
    check_equal(
        turbo_flow_security_realm_create_resolved(resolved, "security.main", NULL, &realm, &error),
        SALTS_OK);
    check_equal(turbo_flow_security_realm_register(flow, realm), SALTS_OK);
    check_equal(turbo_flow_resource_metadata_at(flow, 0u, &metadata), SALTS_OK);
    check_equal(metadata.kind, TURBO_FLOW_RESOURCE_SECURITY_REALM);
    check_equal(metadata.generation, 1u);
    check_equal(metadata.observed_generation, 0u);
    check_equal(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        SALTS_OK);
    check_equal(document.schema->type_name, "SecurityRealmStatus");
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_not_null(strstr(payload, "\"policy_version\":\"0\""));
    check_null(strstr(payload, "root-a"));
    check_null(strstr(payload, "telemetry"));
    tstr_free(payload);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
    turbo_flow_security_realm_destroy(realm);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    realm = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(bad_yaml, sizeof(bad_yaml) - 1u, &resolved, &error),
                 SALTS_OK);
    check_equal(
        turbo_flow_security_realm_create_resolved(resolved, "security.main", NULL, &realm, &error),
        SALTS_EINVAL);
    check_contains(error.path, "rules");
    check_null(realm);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("requires explicit release for short-lived provider-owned secret leases") {
    security_secret_fixture_t fixture = {0};
    turbo_flow_security_key_provider_t provider = {
        sizeof(provider), &fixture, security_secret_acquire, security_secret_release};
    turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;

    check_equal(turbo_flow_security_secret_acquire(&provider, "kms://mqtt/client", &lease),
                 SALTS_OK);
    check_equal(lease.byte_count, 3u);
    check_equal(lease.version, 4u);
    turbo_flow_security_secret_release(&provider, &lease);
    check_equal(fixture.acquire_calls, 1);
    check_equal(fixture.release_calls, 1);
    check_null(lease.bytes);

    fixture.malformed = 1;
    check_equal(turbo_flow_security_secret_acquire(&provider, "kms://mqtt/client", &lease),
                 SALTS_EPROTO);
    check_equal(fixture.release_calls, 2);

    fixture.malformed = 0;
    fixture.fail_after_lease = 1;
    check_equal(turbo_flow_security_secret_acquire(&provider, "kms://mqtt/client", &lease),
                 SALTS_EIO);
    check_equal(fixture.release_calls, 3);
  }
}
