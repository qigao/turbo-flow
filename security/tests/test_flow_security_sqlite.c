#include "turbo_flow_security_sqlite.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

static void acl_copy(char *out, size_t capacity, const char *value) {
  size_t size = strlen(value);
  check(size < capacity);
  memcpy(out, value, size + 1u);
}

static turbo_flow_security_rule_t acl_rule(const char *pattern) {
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  rule.effect = TURBO_FLOW_SECURITY_ALLOW;
  rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
  acl_copy(rule.subject, sizeof(rule.subject), "writer");
  acl_copy(rule.root_group_id, sizeof(rule.root_group_id), "root-a");
  rule.action_mask = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
  rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  rule.match_kind = TURBO_FLOW_SECURITY_MATCH_PREFIX;
  acl_copy(rule.pattern, sizeof(rule.pattern), pattern);
  return rule;
}

static turbo_flow_security_principal_t acl_principal(uint64_t version) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  acl_copy(principal.principal_id, sizeof(principal.principal_id), "device-a");
  acl_copy(principal.principal_type, sizeof(principal.principal_type), "device");
  acl_copy(principal.root_group_id, sizeof(principal.root_group_id), "root-a");
  acl_copy(principal.auth_method, sizeof(principal.auth_method), "password");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  principal.role_count = 1u;
  acl_copy(principal.roles[0], sizeof(principal.roles[0]), "writer");
  principal.group_count = 1u;
  acl_copy(principal.groups[0], sizeof(principal.groups[0]), "root-a");
  principal.policy_version = version;
  return principal;
}

spec("SQLite ACL control store") {
  it("publishes monotonic bundles and refreshes a fail-closed realm") {
    char *path = tt_make_temp_file("flow-acl", ".sqlite3");
    turbo_flow_security_sqlite_config_t sqlite_config = TURBO_FLOW_SECURITY_SQLITE_CONFIG_INIT;
    turbo_flow_security_sqlite_provider_t *provider = NULL;
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_security_rule_t first = acl_rule("root-a/telemetry/");
    turbo_flow_security_rule_t second = acl_rule("root-a/new/");
    turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    turbo_flow_security_principal_t principal = acl_principal(1u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;

    check_not_null(path);
    sqlite_config.database_path = path;
    sqlite_config.namespace_name = "mqtt";
    check_int_eq(turbo_flow_security_sqlite_provider_create(&sqlite_config, &provider), TURBO_OK);
    check_not_null(provider);
    bundle.policy_version = 1u;
    bundle.rules = &first;
    bundle.rule_count = 1u;
    check_int_eq(turbo_flow_security_sqlite_provider_publish(provider, &bundle), TURBO_OK);
    check_int_eq(turbo_flow_security_sqlite_provider_publish(provider, &bundle), TURBO_EBUSY);

    realm_config.resource_uid = "security:mqtt";
    realm_config.owner_name = "security.mqtt";
    realm_config.policy_source = "acl.sqlite";
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_int_eq(turbo_flow_security_realm_bind_policy_provider(
                     realm, turbo_flow_security_sqlite_provider_interface(provider)),
                 TURBO_OK);
    request.principal = &principal;
    request.root_group_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "root-a/telemetry/value";
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), TURBO_OK);

    bundle.policy_version = 2u;
    bundle.rules = &second;
    check_int_eq(turbo_flow_security_sqlite_provider_publish(provider, &bundle), TURBO_OK);
    principal.policy_version = 2u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                 TURBO_EPERM);
    check_uint_eq(decision.policy_version, 2u);
    request.resource = "root-a/new/value";
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision), TURBO_OK);

    turbo_flow_security_realm_destroy(realm);
    turbo_flow_security_sqlite_provider_destroy(provider);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("resolves provider metadata but rejects ACL bodies in YAML realms") {
    static const char yaml[] =
        "version: 1\nchannels:\n  acl.sqlite:\n    kind: acl_provider\n    config:\n"
        "      backend: sqlite\n      database_path: acl.sqlite3\n"
        "      namespace_name: mqtt\n  security.main:\n    kind: security_realm\n    config:\n"
        "      resource_uid: security:mqtt\n      owner_name: security.main\n"
        "      policy_source: acl.sqlite\nadapters: {}\n";
    static const char bad_yaml[] =
        "version: 1\nchannels:\n  security.main:\n    kind: security_realm\n    config:\n"
        "      resource_uid: security:mqtt\n      owner_name: security.main\n"
        "      policy_source: acl.sqlite\n      rules: []\nadapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_security_realm_create_resolved(resolved, "security.main", NULL, &realm, &error),
        TURBO_OK);
    check_str_eq(turbo_flow_security_realm_policy_source(realm), "acl.sqlite");
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
    check_str_contains(error.path, "rules");
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("isolates namespaces and rejects process-local memory stores") {
    char *path = tt_make_temp_file("flow-acl-isolation", ".sqlite3");
    turbo_flow_security_sqlite_config_t first_config = TURBO_FLOW_SECURITY_SQLITE_CONFIG_INIT;
    turbo_flow_security_sqlite_config_t second_config = TURBO_FLOW_SECURITY_SQLITE_CONFIG_INIT;
    turbo_flow_security_sqlite_provider_t *first_provider = NULL;
    turbo_flow_security_sqlite_provider_t *second_provider = NULL;
    turbo_flow_security_rule_t first_rule = acl_rule("root-a/first/");
    turbo_flow_security_rule_t second_rule = acl_rule("root-a/second/");
    turbo_flow_security_policy_bundle_t first_bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    turbo_flow_security_policy_bundle_t second_bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    turbo_flow_security_policy_bundle_t loaded = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    const turbo_flow_security_policy_provider_t *interface;

    check_not_null(path);
    first_config.database_path = path;
    first_config.namespace_name = "mqtt-first";
    second_config.database_path = path;
    second_config.namespace_name = "mqtt-second";
    check_int_eq(turbo_flow_security_sqlite_provider_create(&first_config, &first_provider),
                 TURBO_OK);
    check_int_eq(turbo_flow_security_sqlite_provider_create(&second_config, &second_provider),
                 TURBO_OK);

    first_bundle.policy_version = 1u;
    first_bundle.rules = &first_rule;
    first_bundle.rule_count = 1u;
    second_bundle.policy_version = 7u;
    second_bundle.rules = &second_rule;
    second_bundle.rule_count = 1u;
    check_int_eq(turbo_flow_security_sqlite_provider_publish(first_provider, &first_bundle),
                 TURBO_OK);
    check_int_eq(turbo_flow_security_sqlite_provider_publish(second_provider, &second_bundle),
                 TURBO_OK);

    interface = turbo_flow_security_sqlite_provider_interface(first_provider);
    check_not_null(interface);
    check_int_eq(interface->load(interface->ctx, 1u, &loaded), TURBO_OK);
    check_uint_eq(loaded.policy_version, 1u);
    check_uint_eq(loaded.rule_count, 1u);
    check_str_eq(loaded.rules[0].pattern, "root-a/first/");
    interface->release(interface->ctx, &loaded);

    interface = turbo_flow_security_sqlite_provider_interface(second_provider);
    loaded = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    check_int_eq(interface->load(interface->ctx, 7u, &loaded), TURBO_OK);
    check_uint_eq(loaded.policy_version, 7u);
    check_uint_eq(loaded.rule_count, 1u);
    check_str_eq(loaded.rules[0].pattern, "root-a/second/");
    interface->release(interface->ctx, &loaded);

    turbo_flow_security_sqlite_provider_destroy(second_provider);
    turbo_flow_security_sqlite_provider_destroy(first_provider);
    check_int_eq(tt_remove_file(path), 0);
    free(path);

    first_config.database_path = ":memory:";
    first_provider = NULL;
    check_int_eq(turbo_flow_security_sqlite_provider_create(&first_config, &first_provider),
                 TURBO_EINVAL);
    check_null(first_provider);
  }
}
