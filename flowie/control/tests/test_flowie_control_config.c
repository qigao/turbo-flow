#include "flowie_control_config_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <string.h>

#define ADMIN_FINGERPRINT "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

static const char valid_config[] = "version: 1\n"
                                   "listener:\n"
                                   "  tls:\n"
                                   "    cert_file: certs/control.crt\n"
                                   "    key_file: certs/control.key\n"
                                   "    key_password_ref: env://FLOWIE_CONTROL_KEY_PASSWORD\n"
                                   "storage:\n"
                                   "  sqlite:\n"
                                   "    path: data/flowie-control.db\n"
                                   "management:\n"
                                   "  session:\n"
                                   "    capacity: 512\n"
                                   "    max_sessions_per_principal: 7\n"
                                   "    ttl_seconds: 1800\n"
                                   "  login_executor:\n"
                                   "    workers: 3\n"
                                   "    queue_capacity: 64\n"
                                   "    deadline_ms: 8000\n"
                                   "dashboard:\n"
                                   "  enabled: true\n"
                                   "auth:\n"
                                   "  enabled: false\n";

static const char valid_external_https_config[] =
    "version: 1\n"
    "listener:\n"
    "  tls:\n"
    "    cert_file: cert.pem\n"
    "    key_file: key.pem\n"
    "storage:\n"
    "  sqlite:\n"
    "    path: control.db\n"
    "management:\n"
    "  session:\n"
    "    capacity: 1024\n"
    "auth:\n"
    "  enabled: true\n"
    "  listener_id: flowie-control-auth\n"
    "  method: bearer\n"
    "  service_bindings:\n"
    "    - service_id: broker-main\n"
    "      token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN\n"
    "      root_group: root-a\n"
    "  external_https:\n"
    "    url: https://auth.example/v1/assert\n"
    "    service_token_ref: env://FLOWIE_THIRD_PARTY_AUTH_TOKEN\n"
    "    trusted_issuer: https://identity.example\n"
    "    subject_type: device\n"
    "    timeout_ms: 2500\n"
    "    max_response_size: 8192\n"
    "    max_in_flight: 32\n"
    "    tls:\n"
    "      ca_file: third-party-ca.pem\n"
    "      client_cert_file: flowie-client.pem\n"
    "      client_key_file: flowie-client-key.pem\n"
    "      client_key_password_ref: env://FLOWIE_THIRD_PARTY_KEY_PASSWORD\n";

static const char valid_postgresql_config[] =
    "version: 1\n"
    "listener:\n"
    "  tls:\n"
    "    cert_file: cert.pem\n"
    "    key_file: key.pem\n"
    "storage:\n"
    "  control_store: postgresql\n"
    "  postgresql:\n"
    "    conninfo: host=db.internal dbname=flowie user=flowie sslmode=verify-full\n"
    "    password_ref: env://FLOWIE_CONTROL_PG_PASSWORD\n"
    "    schema_name: flowie_control\n"
    "    connect_timeout_seconds: 3\n"
    "    statement_timeout_ms: 4000\n"
    "    lock_timeout_ms: 2000\n"
    "    pool_capacity: 8\n"
    "    acquire_timeout_ms: 1500\n"
    "    schema_mode: migrate\n"
    "management:\n"
    "  session:\n"
    "    capacity: 1024\n"
    "    ttl_seconds: 3600\n";

static const char valid_bootstrap_config[] =
    "version: 1\n"
    "listener:\n"
    "  tls:\n"
    "    cert_file: cert.pem\n"
    "    key_file: key.pem\n"
    "storage:\n"
    "  sqlite:\n"
    "    path: control.db\n"
    "bootstrap:\n"
    "  username: platform-admin\n"
    "  password_ref: env://FLOWIE_BOOTSTRAP_PASSWORD\n"
    "management:\n"
    "  session:\n"
    "    capacity: 1024\n"
    "    ttl_seconds: 3600\n";

static int parse_config(const char *yaml, flowie_control_config_t *config,
                        flowie_control_config_error_t *error) {
  *config = (flowie_control_config_t)FLOWIE_CONTROL_CONFIG_INIT;
  *error = (flowie_control_config_error_t)FLOWIE_CONTROL_CONFIG_ERROR_INIT;
  return flowie_control_config_parse_yaml(yaml, strlen(yaml), config, error);
}

static int parse_session_principal_limit(const char *limit, flowie_control_config_t *config,
                                         flowie_control_config_error_t *error) {
  char yaml[512];
  int size = snprintf(yaml, sizeof(yaml),
                      "version: 1\n"
                      "listener:\n"
                      "  tls:\n"
                      "    cert_file: cert.pem\n"
                      "    key_file: key.pem\n"
                      "storage:\n"
                      "  sqlite:\n"
                      "    path: control.db\n"
                      "management:\n"
                      "  session:\n"
                      "    max_sessions_per_principal: %s\n",
                      limit);
  if (size < 0 || (size_t)size >= sizeof(yaml)) return TURBO_ERANGE;
  return parse_config(yaml, config, error);
}

spec("Flowie controller configuration") {
  static flowie_control_config_t config;
  static flowie_control_config_error_t error;

  before_each() {
    config = (flowie_control_config_t)FLOWIE_CONTROL_CONFIG_INIT;
    error = (flowie_control_config_error_t)FLOWIE_CONTROL_CONFIG_ERROR_INIT;
  }

#ifdef FLOWIE_CONTROL_TEST_CONFIG_PATH
  it("keeps the shipped controller example inside the schema") {
    check_int_eq(flowie_control_config_load(FLOWIE_CONTROL_TEST_CONFIG_PATH, &config, &error),
                 TURBO_OK);
    check_str_eq(config.listener.host, "127.0.0.1");
    check_size_eq(config.management.session_capacity, 1024u);
    check_size_eq(config.management.session_max_sessions_per_principal, 5u);
  }
#endif

  it("loads a valid configuration with secure defaults") {
    check_int_eq(parse_config(valid_config, &config, &error), TURBO_OK);
    check_str_eq(config.listener.host, "127.0.0.1");
    check_int_eq(config.listener.port, 8443);
    check_str_eq(config.management.rpc_path, "/v1/management/rpc");
    check_size_eq(config.management.session_capacity, 512u);
    check_size_eq(config.management.session_max_sessions_per_principal, 7u);
    check_uint_eq(config.management.session_ttl_seconds, 1800u);
    check_true(config.management.login_executor_configured);
    check_uint_eq(config.management.login_executor_workers, 3u);
    check_size_eq(config.management.login_executor_queue_capacity, 64u);
    check_uint_eq(config.management.login_executor_deadline_ms, 8000u);
    check_true(config.dashboard_enabled);
    check_false(config.auth.enabled);
    check_int_eq(config.store_provider, FLOWIE_CONTROL_CONFIG_STORE_SQLITE);
    check_false(config.auth.local_executor.configured);
    check_uint_eq(config.auth.local_executor.workers, 4u);
    check_size_eq(config.auth.local_executor.queue_capacity, 128u);
    check_uint_eq(config.auth.local_executor.deadline_ms, 10000u);
  }

  it("defaults each principal to five concurrent management sessions") {
    check_int_eq(parse_config(valid_postgresql_config, &config, &error), TURBO_OK);
    check_size_eq(config.management.session_max_sessions_per_principal, 5u);
  }

  it("rejects a zero per-principal management session limit") {
    check_int_eq(parse_session_principal_limit("0", &config, &error), TURBO_ERANGE);
    check_str_eq(error.path, "$.management.session.max_sessions_per_principal");
  }

  it("rejects a per-principal management session limit above 65536") {
    check_int_eq(parse_session_principal_limit("65537", &config, &error), TURBO_ERANGE);
    check_str_eq(error.path, "$.management.session.max_sessions_per_principal");
  }

  it("loads an explicit PostgreSQL control store without a literal password") {
    check_int_eq(parse_config(valid_postgresql_config, &config, &error), TURBO_OK);
    check_int_eq(config.store_provider, FLOWIE_CONTROL_CONFIG_STORE_POSTGRESQL);
    check_str_eq(config.postgresql.conninfo,
                 "host=db.internal dbname=flowie user=flowie sslmode=verify-full");
    check_str_eq(config.postgresql.password_ref, "env://FLOWIE_CONTROL_PG_PASSWORD");
    check_str_eq(config.postgresql.schema_name, "flowie_control");
    check_int_eq(config.postgresql.connect_timeout_seconds, 3);
    check_int_eq(config.postgresql.statement_timeout_ms, 4000);
    check_int_eq(config.postgresql.lock_timeout_ms, 2000);
    check_size_eq(config.postgresql.pool_capacity, 8u);
    check_int_eq(config.postgresql.acquire_timeout_ms, 1500);
    check_int_eq(config.postgresql.schema_mode, FLOWIE_CONTROL_CONFIG_PGSQL_SCHEMA_MIGRATE);
    check_true(config.sqlite_path[0] == '\0');
  }

  it("loads an initial administrator whose password is only an environment reference") {
    check_int_eq(parse_config(valid_bootstrap_config, &config, &error), TURBO_OK);
    check_true(config.bootstrap.enabled);
    check_str_eq(config.bootstrap.root_group_id, "system");
    check_str_eq(config.bootstrap.principal_id, "platform-admin");
    check_str_eq(config.bootstrap.principal_type, "human");
    check_str_eq(config.bootstrap.password_ref, "env://FLOWIE_BOOTSTRAP_PASSWORD");
  }

  it("defaults the bootstrap username and password environment reference") {
    static const char yaml[] =
        "version: 1\n"
        "listener:\n"
        "  tls:\n"
        "    cert_file: cert.pem\n"
        "    key_file: key.pem\n"
        "storage:\n"
        "  sqlite:\n"
        "    path: control.db\n"
        "bootstrap:\n"
        "  password_ref: env://FLOWIE_BOOTSTRAP_PASSWORD\n"
        "management:\n"
        "  session:\n"
        "    capacity: 1024\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_OK);
    check_true(config.bootstrap.enabled);
    check_str_eq(config.bootstrap.root_group_id, "system");
    check_str_eq(config.bootstrap.principal_id, "admin");
    check_str_eq(config.bootstrap.password_ref, "env://FLOWIE_BOOTSTRAP_PASSWORD");
  }

  it("defaults the bootstrap password environment reference") {
    static const char yaml[] =
        "version: 1\n"
        "listener:\n"
        "  tls:\n"
        "    cert_file: cert.pem\n"
        "    key_file: key.pem\n"
        "storage:\n"
        "  sqlite:\n"
        "    path: control.db\n"
        "bootstrap:\n"
        "  username: admin\n"
        "management:\n"
        "  session:\n"
        "    capacity: 1024\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_OK);
    check_str_eq(config.bootstrap.password_ref, "env://FLOWIE_BOOTSTRAP_PASSWORD");
  }

  it("rejects a literal bootstrap password") {
    char yaml[sizeof(valid_bootstrap_config)];
    char *reference;

    memcpy(yaml, valid_bootstrap_config, sizeof(valid_bootstrap_config));
    reference = strstr(yaml, "env://FLOWIE_BOOTSTRAP_PASSWORD");
    check_not_null(reference);
    memcpy(reference, "literal-password", sizeof("literal-password") - 1u);
    memmove(reference + sizeof("literal-password") - 1u,
            reference + sizeof("env://FLOWIE_BOOTSTRAP_PASSWORD") - 1u,
            strlen(reference + sizeof("env://FLOWIE_BOOTSTRAP_PASSWORD") - 1u) + 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.bootstrap.password_ref");
  }

  it("rejects simultaneous SQLite and PostgreSQL control store configuration") {
    static const char yaml[] =
        "version: 1\n"
        "listener:\n"
        "  tls:\n"
        "    cert_file: cert.pem\n"
        "    key_file: key.pem\n"
        "storage:\n"
        "  control_store: postgresql\n"
        "  sqlite:\n"
        "    path: control.db\n"
        "  postgresql:\n"
        "    conninfo: host=db.internal dbname=flowie user=flowie sslmode=verify-full\n"
        "    password_ref: env://FLOWIE_CONTROL_PG_PASSWORD\n"
        "management:\n"
        "  session:\n"
        "    capacity: 1024\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.storage.sqlite");
  }

  it("rejects a literal PostgreSQL password") {
    char yaml[sizeof(valid_postgresql_config)];
    char *reference;

    memcpy(yaml, valid_postgresql_config, sizeof(valid_postgresql_config));
    reference = strstr(yaml, "env://FLOWIE_CONTROL_PG_PASSWORD");
    check_not_null(reference);
    memcpy(reference, "literal-password", sizeof("literal-password") - 1u);
    memmove(reference + sizeof("literal-password") - 1u,
            reference + sizeof("env://FLOWIE_CONTROL_PG_PASSWORD") - 1u,
            strlen(reference + sizeof("env://FLOWIE_CONTROL_PG_PASSWORD") - 1u) + 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.storage.postgresql.password_ref");
  }

  it("rejects unknown fields") {
    static const char yaml[] = "version: 1\n"
                               "unexpected: true\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.unexpected");
  }

  it("rejects duplicate mapping fields") {
    static const char yaml[] = "version: 1\n"
                               "version: 1\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$");
  }

  it("rejects literal TLS key passwords") {
    char yaml[sizeof(valid_config) + 32u];
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

  it("rejects duplicate scoped service identifiers") {
    static const char yaml[] = "version: 1\n"
                               "listener:\n"
                               "  tls:\n"
                               "    cert_file: cert.pem\n"
                               "    key_file: key.pem\n"
                               "storage:\n"
                               "  sqlite:\n"
                               "    path: control.db\n"
                               "management:\n"
                               "  session:\n"
                               "    capacity: 1024\n"
                               "auth:\n"
                               "  enabled: true\n"
                               "  listener_id: auth-listener\n"
                               "  method: password\n"
                               "  service_bindings:\n"
                               "    - service_id: broker-a\n"
                               "      token_ref: env://FLOWIE_BROKER_A_TOKEN\n"
                               "      root_group: root-a\n"
                               "    - service_id: broker-a\n"
                               "      token_ref: env://FLOWIE_BROKER_B_TOKEN\n"
                               "      root_group: root-b\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EALREADY);
  }

  it("requires the listener body limit to cover the RPC limit") {
    static const char yaml[] = "version: 1\n"
                               "listener:\n"
                               "  tls:\n"
                               "    cert_file: cert.pem\n"
                               "    key_file: key.pem\n"
                               "  limits:\n"
                               "    max_request_body_size: 4096\n"
                               "storage:\n"
                               "  sqlite:\n"
                               "    path: control.db\n"
                               "management:\n"
                               "  rpc_max_request_size: 8192\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_ERANGE);
    check_str_eq(error.path, "$.listener.limits.max_request_body_size");
  }

  it("selects local authentication when no external HTTPS source is configured") {
    static const char yaml[] = "version: 1\n"
                               "listener:\n"
                               "  tls:\n"
                               "    cert_file: cert.pem\n"
                               "    key_file: key.pem\n"
                               "storage:\n"
                               "  sqlite:\n"
                               "    path: control.db\n"
                               "management:\n"
                               "  session:\n"
                               "    capacity: 1024\n"
                               "auth:\n"
                               "  enabled: true\n"
                               "  listener_id: flowie-control-auth\n"
                               "  method: password\n"
                               "  local_executor:\n"
                               "    workers: 6\n"
                               "    queue_capacity: 256\n"
                               "    deadline_ms: 12000\n"
                               "  service_bindings:\n"
                               "    - service_id: broker-main\n"
                               "      token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN\n"
                               "      root_group: root-a\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_OK);
    check_true(config.auth.enabled);
    check_false(config.auth.external_https.enabled);
    check_str_eq(config.auth.method, "password");
    check_true(config.auth.local_executor.configured);
    check_uint_eq(config.auth.local_executor.workers, 6u);
    check_size_eq(config.auth.local_executor.queue_capacity, 256u);
    check_uint_eq(config.auth.local_executor.deadline_ms, 12000u);
  }

  it("rejects local executor configuration with external HTTPS authentication") {
    char yaml[sizeof(valid_external_https_config) + 96u];
    char *external;
    static const char executor[] = "  local_executor:\n"
                                   "    workers: 2\n"
                                   "    queue_capacity: 16\n"
                                   "    deadline_ms: 5000\n";

    memcpy(yaml, valid_external_https_config, sizeof(valid_external_https_config));
    external = strstr(yaml, "  external_https:\n");
    check_not_null(external);
    memmove(external + sizeof(executor) - 1u, external, strlen(external) + 1u);
    memcpy(external, executor, sizeof(executor) - 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.auth.local_executor");
  }

  it("rejects a management login executor with external HTTPS authentication") {
    char yaml[sizeof(valid_external_https_config) + 128u];
    char *session;
    static const char executor[] = "  login_executor:\n"
                                   "    workers: 2\n"
                                   "    queue_capacity: 16\n"
                                   "    deadline_ms: 5000\n";

    memcpy(yaml, valid_external_https_config, sizeof(valid_external_https_config));
    session = strstr(yaml, "  session:\n");
    check_not_null(session);
    memmove(session + sizeof(executor) - 1u, session, strlen(session) + 1u);
    memcpy(session, executor, sizeof(executor) - 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.management.login_executor");
  }

  it("rejects a local executor deadline above the hard bound") {
    static const char yaml[] = "version: 1\n"
                               "listener:\n"
                               "  tls:\n"
                               "    cert_file: cert.pem\n"
                               "    key_file: key.pem\n"
                               "storage:\n"
                               "  sqlite:\n"
                               "    path: control.db\n"
                               "management:\n"
                               "  session:\n"
                               "    capacity: 1024\n"
                               "auth:\n"
                               "  enabled: true\n"
                               "  listener_id: flowie-control-auth\n"
                               "  method: password\n"
                               "  local_executor:\n"
                               "    deadline_ms: 60001\n"
                               "  service_bindings:\n"
                               "    - service_id: broker-main\n"
                               "      token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN\n"
                               "      root_group: root-a\n";
    check_int_eq(parse_config(yaml, &config, &error), TURBO_ERANGE);
    check_str_eq(error.path, "$.auth.local_executor.deadline_ms");
  }

  it("loads the bounded external HTTPS authentication configuration") {
    check_int_eq(parse_config(valid_external_https_config, &config, &error), TURBO_OK);
    check_true(config.auth.external_https.enabled);
    check_str_eq(config.auth.external_https.url, "https://auth.example/v1/assert");
    check_str_eq(config.auth.external_https.service_token_ref,
                 "env://FLOWIE_THIRD_PARTY_AUTH_TOKEN");
    check_str_eq(config.auth.external_https.trusted_issuer, "https://identity.example");
    check_str_eq(config.auth.external_https.subject_type, "device");
    check_uint_eq(config.auth.external_https.timeout_ms, 2500u);
    check_size_eq(config.auth.external_https.max_response_size, 8192u);
    check_uint_eq(config.auth.external_https.max_in_flight, 32u);
    check_str_eq(config.auth.external_https.tls.client_cert_file, "flowie-client.pem");
  }

  it("rejects an external HTTPS concurrency limit above the hard bound") {
    char yaml[sizeof(valid_external_https_config) + 2u];
    char *limit;

    memcpy(yaml, valid_external_https_config, sizeof(valid_external_https_config));
    limit = strstr(yaml, "max_in_flight: 32");
    check_not_null(limit);
    limit += sizeof("max_in_flight: ") - 1u;
    memmove(limit + sizeof("2048") - 1u, limit + sizeof("32") - 1u,
            strlen(limit + sizeof("32") - 1u) + 1u);
    memcpy(limit, "2048", sizeof("2048") - 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_ERANGE);
    check_str_eq(error.path, "$.auth.external_https.max_in_flight");
  }

  it("rejects insecure external authentication URLs") {
    char yaml[sizeof(valid_external_https_config)];
    char *scheme;

    memcpy(yaml, valid_external_https_config, sizeof(valid_external_https_config));
    scheme = strstr(yaml, "https://auth.example");
    check_not_null(scheme);
    memmove(scheme + sizeof("http://") - 1u, scheme + sizeof("https://") - 1u,
            strlen(scheme + sizeof("https://") - 1u) + 1u);
    memcpy(scheme, "http://", sizeof("http://") - 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.auth.external_https.url");
  }

  it("requires a complete external mTLS client identity") {
    char yaml[sizeof(valid_external_https_config)];
    char *key_line;
    char *line_end;

    memcpy(yaml, valid_external_https_config, sizeof(valid_external_https_config));
    key_line = strstr(yaml, "      client_key_file:");
    check_not_null(key_line);
    line_end = strchr(key_line, '\n');
    check_not_null(line_end);
    memmove(key_line, line_end + 1u, strlen(line_end + 1u) + 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.auth.external_https.tls");
  }

  it("rejects external authentication while the auth endpoint is disabled") {
    char yaml[sizeof(valid_external_https_config) + 1u];
    char *enabled;

    memcpy(yaml, valid_external_https_config, sizeof(valid_external_https_config));
    enabled = strstr(yaml, "  enabled: true");
    check_not_null(enabled);
    enabled += sizeof("  enabled: ") - 1u;
    memmove(enabled + sizeof("false") - 1u, enabled + sizeof("true") - 1u,
            strlen(enabled + sizeof("true") - 1u) + 1u);
    memcpy(enabled, "false", sizeof("false") - 1u);
    check_int_eq(parse_config(yaml, &config, &error), TURBO_EINVAL);
    check_str_eq(error.path, "$.auth.external_https");
  }
}
