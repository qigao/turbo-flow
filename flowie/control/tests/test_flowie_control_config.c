#include "flowie_control_config_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

#define ADMIN_FINGERPRINT                                                                        \
  "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

static const char valid_config[] =
    "version: 1\n"
    "listener:\n"
    "  tls:\n"
    "    cert_file: certs/control.crt\n"
    "    key_file: certs/control.key\n"
    "    key_password_ref: env://FLOWIE_CONTROL_KEY_PASSWORD\n"
    "    client_ca_file: certs/control-ca.crt\n"
    "storage:\n"
    "  sqlite:\n"
    "    path: data/flowie-control.db\n"
    "management:\n"
    "  certificate_bindings:\n"
    "    - peer_certificate_sha256: " ADMIN_FINGERPRINT "\n"
    "      root_group: root-a\n"
    "      principal: admin-a\n"
    "dashboard:\n"
    "  enabled: true\n"
    "auth:\n"
    "  enabled: false\n";

static int parse_config(const char *yaml, flowie_control_config_t *config,
                        flowie_control_config_error_t *error) {
  *config = (flowie_control_config_t)FLOWIE_CONTROL_CONFIG_INIT;
  *error = (flowie_control_config_error_t)FLOWIE_CONTROL_CONFIG_ERROR_INIT;
  return flowie_control_config_parse_yaml(yaml, strlen(yaml), config, error);
}

spec("Flowie controller configuration") {
#ifdef FLOWIE_CONTROL_TEST_CONFIG_PATH
  it("keeps the shipped controller example inside the schema") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    flowie_control_config_error_t error = FLOWIE_CONTROL_CONFIG_ERROR_INIT;

    check_int_eq(flowie_control_config_load(FLOWIE_CONTROL_TEST_CONFIG_PATH, &config, &error),
                 TURBO_OK);
    check_str_eq(config.listener.host, "127.0.0.1");
    check_int_eq(config.management.admin_binding_count, 1);
  }
#endif

  it("loads a valid configuration with secure defaults") {
    flowie_control_config_t config;
    flowie_control_config_error_t error;

    check_int_eq(parse_config(valid_config, &config, &error), TURBO_OK);
    check_str_eq(config.listener.host, "127.0.0.1");
    check_int_eq(config.listener.port, 8443);
    check_str_eq(config.management.rpc_path, "/v1/management/rpc");
    check_int_eq(config.management.admin_binding_count, 1);
    check_true(config.dashboard_enabled);
    check_false(config.auth.enabled);
  }

  it("rejects unknown fields") {
    static const char yaml[] =
        "version: 1\n"
        "unexpected: true\n";
    flowie_control_config_t config;
    flowie_control_config_error_t error;

    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.unexpected");
  }

  it("rejects duplicate mapping fields") {
    static const char yaml[] =
        "version: 1\n"
        "version: 1\n";
    flowie_control_config_t config;
    flowie_control_config_error_t error;

    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$");
  }

  it("rejects literal TLS key passwords") {
    char yaml[sizeof(valid_config) + 32u];
    flowie_control_config_t config;
    flowie_control_config_error_t error;
    const char *reference = "env://FLOWIE_CONTROL_KEY_PASSWORD";
    char *position;

    memcpy(yaml, valid_config, sizeof(valid_config));
    position = strstr(yaml, reference);
    check_not_null(position);
    memcpy(position, "literal-secret", sizeof("literal-secret") - 1u);
    memmove(position + sizeof("literal-secret") - 1u, position + strlen(reference),
            strlen(position + strlen(reference)) + 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.listener.tls.key_password_ref");
  }

  it("rejects non-canonical certificate fingerprints") {
    char yaml[sizeof(valid_config)];
    flowie_control_config_t config;
    flowie_control_config_error_t error;
    char *fingerprint;

    memcpy(yaml, valid_config, sizeof(valid_config));
    fingerprint = strstr(yaml, ADMIN_FINGERPRINT);
    check_not_null(fingerprint);
    fingerprint[sizeof("sha256:") - 1u] = 'A';
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_true(strstr(error.path, "peer_certificate_sha256") != NULL);
  }

  it("rejects duplicate management certificate bindings") {
    static const char yaml[] =
        "version: 1\n"
        "listener:\n"
        "  tls:\n"
        "    cert_file: cert.pem\n"
        "    key_file: key.pem\n"
        "    client_ca_file: ca.pem\n"
        "storage:\n"
        "  sqlite:\n"
        "    path: control.db\n"
        "management:\n"
        "  certificate_bindings:\n"
        "    - peer_certificate_sha256: " ADMIN_FINGERPRINT "\n"
        "      root_group: root-a\n"
        "      principal: admin-a\n"
        "    - peer_certificate_sha256: " ADMIN_FINGERPRINT "\n"
        "      root_group: root-a\n"
        "      principal: admin-b\n";
    flowie_control_config_t config;
    flowie_control_config_error_t error;

    check_int_eq(parse_config(yaml, &config, &error), TURBO_EALREADY);
  }

  it("requires the listener body limit to cover the RPC limit") {
    static const char yaml[] =
        "version: 1\n"
        "listener:\n"
        "  tls:\n"
        "    cert_file: cert.pem\n"
        "    key_file: key.pem\n"
        "    client_ca_file: ca.pem\n"
        "  limits:\n"
        "    max_request_body_size: 4096\n"
        "storage:\n"
        "  sqlite:\n"
        "    path: control.db\n"
        "management:\n"
        "  rpc_max_request_size: 8192\n"
        "  certificate_bindings:\n"
        "    - peer_certificate_sha256: " ADMIN_FINGERPRINT "\n"
        "      root_group: root-a\n"
        "      principal: admin-a\n";
    flowie_control_config_t config;
    flowie_control_config_error_t error;

    check_int_eq(parse_config(yaml, &config, &error), TURBO_ERANGE);
    check_str_eq(error.path, "$.listener.limits.max_request_body_size");
  }

  it("requires enabled auth fields and accepts environment secret references") {
    static const char yaml[] =
        "version: 1\n"
        "listener:\n"
        "  tls:\n"
        "    cert_file: cert.pem\n"
        "    key_file: key.pem\n"
        "    client_ca_file: ca.pem\n"
        "storage:\n"
        "  sqlite:\n"
        "    path: control.db\n"
        "management:\n"
        "  certificate_bindings:\n"
        "    - peer_certificate_sha256: " ADMIN_FINGERPRINT "\n"
        "      root_group: root-a\n"
        "      principal: admin-a\n"
        "auth:\n"
        "  enabled: true\n"
        "  listener_id: flowie-control-auth\n"
        "  method: password\n"
        "  service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN\n"
        "  root_bindings:\n"
        "    - peer_certificate_sha256: " ADMIN_FINGERPRINT "\n"
        "      root_group: root-a\n";
    flowie_control_config_t config;
    flowie_control_config_error_t error;

    check_int_eq(parse_config(yaml, &config, &error), TURBO_OK);
    check_true(config.auth.enabled);
    check_int_eq(config.auth.binding_count, 1);
  }
}
