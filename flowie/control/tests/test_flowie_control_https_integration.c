#include "flowie_control_runtime_internal.h"
#include "flowie_control_dashboard_internal.h"
#include "flowie_control_management_session_internal.h"

#include "platform.h"
#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "base64_utils.h"
#include "http_client.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_process.h"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
#endif

#ifndef FLOWIE_CONTROL_EXECUTABLE
  #error "FLOWIE_CONTROL_EXECUTABLE must point to the built controller"
#endif

#define CONTROL_INTEGRATION_TIMEOUT_MS 15000u
#define CONTROL_INTEGRATION_REQUEST_TIMEOUT_MS 1500
#define CONTROL_INTEGRATION_RSA_BITS 2048
#define CONTROL_INTEGRATION_STATUS_RPC_BODY                                                        \
  "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.system.status\",\"id\":1}"
#define CONTROL_INTEGRATION_AUTH_STATS_RPC_BODY                                                    \
  "{\"jsonrpc\":\"2.0\",\"method\":\"flowie.auth.external_https.stats\",\"params\":{},\"id\":2}"
#define CONTROL_INTEGRATION_SERVICE_TOKEN "integration-service-token"
#define CONTROL_INTEGRATION_SERVICE_TOKEN_ENV "FLOWIE_AUTH_SERVICE_TOKEN"
#define CONTROL_INTEGRATION_ADMIN_PASSWORD "integration-admin-password"
#define CONTROL_INTEGRATION_POLICY_EXPIRES_AT UINT64_C(4102444800)
#define CONTROL_INTEGRATION_SECRET_BASE64_CAPACITY 64u
#define CONTROL_INTEGRATION_FORM_CAPACITY 1024u
#define CONTROL_INTEGRATION_REVISION_CAPACITY 32u

typedef struct control_tls_material_s {
  EVP_PKEY *ca_key;
  X509 *ca_cert;
  EVP_PKEY *server_key;
  X509 *server_cert;
  EVP_PKEY *known_key;
  X509 *known_cert;
  EVP_PKEY *unknown_key;
  X509 *unknown_cert;
  char *ca_path;
  char *server_cert_path;
  char *server_key_path;
  char *known_cert_path;
  char *known_key_path;
  char *unknown_cert_path;
  char *unknown_key_path;
  char known_fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
} control_tls_material_t;

typedef struct control_http_state_s {
  coro_context_t *context;
  const char *base_url;
  const char *ca_path;
  const char *known_cert_path;
  const char *known_key_path;
  const char *unknown_cert_path;
  const char *unknown_key_path;
  const char *secret_base64;
  int ready;
  int unauthenticated_rpc_forbidden;
  int session_rpc_ok;
  int dashboard_htmx_ok;
  int dashboard_csrf_rejected;
  int dashboard_write_ok;
  int dashboard_logout_ok;
  int dashboard_role_revocation_ok;
  int local_auth_ok;
  int local_auth_bad_secret_forbidden;
  int acl_bundle_ok;
  int acl_version_miss_ok;
  int acl_bad_token_forbidden;
  int client_certificate_does_not_authenticate_rpc;
} control_http_state_t;

static int control_test_set_env(const char *name, const char *value) {
#ifdef _WIN32
  return _putenv_s(name, value ? value : "");
#else
  return value ? setenv(name, value, 1) : unsetenv(name);
#endif
}

static int control_test_socket_init(void) {
#ifdef _WIN32
  static int initialized = 0;
  WSADATA data;
  if (initialized) return 0;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return -1;
  initialized = 1;
#endif
  return 0;
}

static int control_test_reserve_port(unsigned short *port_out) {
  struct sockaddr_in address;
  socklen_t address_size = (socklen_t)sizeof(address);
#ifdef _WIN32
  SOCKET socket_handle;
  if (!port_out || control_test_socket_init() != 0) return -1;
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == INVALID_SOCKET) return -1;
#else
  int socket_handle;
  if (!port_out || control_test_socket_init() != 0) return -1;
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle < 0) return -1;
#endif
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  if (bind(socket_handle, (const struct sockaddr *)&address, sizeof(address)) != 0 ||
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) != 0) {
#ifdef _WIN32
    closesocket(socket_handle);
#else
    close(socket_handle);
#endif
    return -1;
  }
  *port_out = ntohs(address.sin_port);
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
  return 0;
}

static EVP_PKEY *control_test_generate_key(void) {
  EVP_PKEY_CTX *context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
  EVP_PKEY *key = NULL;
  if (!context || EVP_PKEY_keygen_init(context) <= 0 ||
      EVP_PKEY_CTX_set_rsa_keygen_bits(context, CONTROL_INTEGRATION_RSA_BITS) <= 0 ||
      EVP_PKEY_keygen(context, &key) <= 0) {
    EVP_PKEY_free(key);
    key = NULL;
  }
  EVP_PKEY_CTX_free(context);
  return key;
}

static int control_test_add_extension(X509 *certificate, X509 *issuer, int nid, const char *value) {
  X509V3_CTX extension_context;
  X509_EXTENSION *extension;
  if (!certificate || !issuer || !value) return -1;
  X509V3_set_ctx(&extension_context, issuer, certificate, NULL, NULL, 0);
  extension = X509V3_EXT_nconf_nid(NULL, &extension_context, nid, value);
  if (!extension) return -1;
  if (X509_add_ext(certificate, extension, -1) != 1) {
    X509_EXTENSION_free(extension);
    return -1;
  }
  X509_EXTENSION_free(extension);
  return 0;
}

static X509 *control_test_make_certificate(EVP_PKEY *key, X509 *issuer, EVP_PKEY *issuer_key,
                                           long serial, const char *common_name, int is_ca,
                                           const char *san, const char *extended_usage) {
  X509 *certificate = X509_new();
  X509_NAME *subject;
  if (!certificate || !key || !common_name) goto fail;
  if (X509_set_version(certificate, 2) != 1 ||
      ASN1_INTEGER_set(X509_get_serialNumber(certificate), serial) != 1 ||
      !X509_gmtime_adj(X509_get_notBefore(certificate), -300) ||
      !X509_gmtime_adj(X509_get_notAfter(certificate), 31536000L))
    goto fail;
  subject = X509_get_subject_name(certificate);
  if (!subject || X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                             (const unsigned char *)common_name, -1, -1, 0) != 1)
    goto fail;
  if (issuer) {
    if (X509_set_issuer_name(certificate, X509_get_subject_name(issuer)) != 1) goto fail;
  } else if (X509_set_issuer_name(certificate, subject) != 1) {
    goto fail;
  }
  if (X509_set_pubkey(certificate, key) != 1) goto fail;
  if (is_ca) {
    if (control_test_add_extension(certificate, certificate, NID_basic_constraints,
                                   "critical,CA:TRUE") != 0 ||
        control_test_add_extension(certificate, certificate, NID_key_usage,
                                   "critical,keyCertSign,cRLSign") != 0)
      goto fail;
  } else {
    if (control_test_add_extension(certificate, issuer, NID_basic_constraints,
                                   "critical,CA:FALSE") != 0 ||
        control_test_add_extension(certificate, issuer, NID_key_usage,
                                   "critical,digitalSignature,keyEncipherment") != 0 ||
        (san && control_test_add_extension(certificate, issuer, NID_subject_alt_name, san) != 0) ||
        (extended_usage &&
         control_test_add_extension(certificate, issuer, NID_ext_key_usage, extended_usage) != 0))
      goto fail;
  }
  if (!issuer_key) issuer_key = key;
  if (X509_sign(certificate, issuer_key, EVP_sha256()) <= 0) goto fail;
  return certificate;

fail:
  X509_free(certificate);
  return NULL;
}

static int control_test_write_certificate(const char *path, X509 *certificate) {
  BIO *file = NULL;
  int rc = -1;
  if (!path || !certificate) return -1;
  file = BIO_new_file(path, "wb");
  if (file && PEM_write_bio_X509(file, certificate) == 1) rc = 0;
  BIO_free(file);
  return rc;
}

static int control_test_write_key(const char *path, EVP_PKEY *key) {
  BIO *file = NULL;
  int rc = -1;
  if (!path || !key) return -1;
  file = BIO_new_file(path, "wb");
  if (file && PEM_write_bio_PrivateKey(file, key, NULL, NULL, 0, NULL, NULL) == 1) rc = 0;
  BIO_free(file);
  return rc;
}

static int control_test_fingerprint(X509 *certificate,
                                    char output[CORO_TLS_PEER_CERT_SHA256_CAPACITY]) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_size = 0u;
  static const char hex[] = "0123456789abcdef";
  if (!certificate || !output ||
      X509_digest(certificate, EVP_sha256(), digest, &digest_size) != 1 || digest_size != 32u)
    return -1;
  memcpy(output, "sha256:", sizeof("sha256:") - 1u);
  for (unsigned int index = 0u; index < digest_size; ++index) {
    output[sizeof("sha256:") - 1u + index * 2u] = hex[digest[index] >> 4u];
    output[sizeof("sha256:") + index * 2u] = hex[digest[index] & 0x0fu];
  }
  output[sizeof("sha256:") - 1u + digest_size * 2u] = '\0';
  return 0;
}

static int control_test_tls_material_open(control_tls_material_t *material) {
  if (!material) return -1;
  memset(material, 0, sizeof(*material));
  material->ca_path = tt_make_temp_file("flowie-control-it-ca", ".pem");
  material->server_cert_path = tt_make_temp_file("flowie-control-it-server", ".pem");
  material->server_key_path = tt_make_temp_file("flowie-control-it-server", ".key");
  material->known_cert_path = tt_make_temp_file("flowie-control-it-known", ".pem");
  material->known_key_path = tt_make_temp_file("flowie-control-it-known", ".key");
  material->unknown_cert_path = tt_make_temp_file("flowie-control-it-unknown", ".pem");
  material->unknown_key_path = tt_make_temp_file("flowie-control-it-unknown", ".key");
  if (!material->ca_path || !material->server_cert_path || !material->server_key_path ||
      !material->known_cert_path || !material->known_key_path || !material->unknown_cert_path ||
      !material->unknown_key_path)
    return -1;
  material->ca_key = control_test_generate_key();
  material->ca_cert = control_test_make_certificate(material->ca_key, NULL, NULL, 1,
                                                    "Flowie integration CA", 1, NULL, NULL);
  material->server_key = control_test_generate_key();
  material->server_cert =
      control_test_make_certificate(material->server_key, material->ca_cert, material->ca_key, 2,
                                    "localhost", 0, "DNS:localhost,IP:127.0.0.1", "serverAuth");
  material->known_key = control_test_generate_key();
  material->known_cert =
      control_test_make_certificate(material->known_key, material->ca_cert, material->ca_key, 3,
                                    "flowie-admin", 0, NULL, "clientAuth");
  material->unknown_key = control_test_generate_key();
  material->unknown_cert =
      control_test_make_certificate(material->unknown_key, material->ca_cert, material->ca_key, 4,
                                    "flowie-unknown", 0, NULL, "clientAuth");
  if (!material->ca_key || !material->ca_cert || !material->server_key || !material->server_cert ||
      !material->known_key || !material->known_cert || !material->unknown_key ||
      !material->unknown_cert ||
      control_test_fingerprint(material->known_cert, material->known_fingerprint) != 0 ||
      control_test_write_certificate(material->ca_path, material->ca_cert) != 0 ||
      control_test_write_certificate(material->server_cert_path, material->server_cert) != 0 ||
      control_test_write_key(material->server_key_path, material->server_key) != 0 ||
      control_test_write_certificate(material->known_cert_path, material->known_cert) != 0 ||
      control_test_write_key(material->known_key_path, material->known_key) != 0 ||
      control_test_write_certificate(material->unknown_cert_path, material->unknown_cert) != 0 ||
      control_test_write_key(material->unknown_key_path, material->unknown_key) != 0)
    return -1;
  return 0;
}

static void control_test_tls_material_close(control_tls_material_t *material) {
  char *paths[7];
  if (!material) return;
  paths[0] = material->ca_path;
  paths[1] = material->server_cert_path;
  paths[2] = material->server_key_path;
  paths[3] = material->known_cert_path;
  paths[4] = material->known_key_path;
  paths[5] = material->unknown_cert_path;
  paths[6] = material->unknown_key_path;
  for (size_t index = 0u; index < sizeof(paths) / sizeof(paths[0]); ++index) {
    if (paths[index]) {
      (void)tt_remove_file(paths[index]);
      free(paths[index]);
    }
  }
  X509_free(material->unknown_cert);
  EVP_PKEY_free(material->unknown_key);
  X509_free(material->known_cert);
  EVP_PKEY_free(material->known_key);
  X509_free(material->server_cert);
  EVP_PKEY_free(material->server_key);
  X509_free(material->ca_cert);
  EVP_PKEY_free(material->ca_key);
  memset(material, 0, sizeof(*material));
}

static int control_test_seed_store(const char *database_path, char *secret_base64,
                                   size_t secret_base64_capacity) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_store_t *store = NULL;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_policy_rule_put_command_t rule = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
  flowie_control_policy_publish_command_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
  flowie_control_policy_publish_result_t published = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  uint64_t revision = 0u;
  int rc;
  if (!secret_base64 || secret_base64_capacity == 0u) return TURBO_EINVAL;
  secret_base64[0] = '\0';
  store_config.database_path = database_path;
  rc = flowie_control_store_open(&store_config, &store);
  if (rc != TURBO_OK) return rc;

  root.root_group_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "integration-root";
  root.expected_revision = 0u;
  root.occurred_at = 1u;
  rc = flowie_control_store_root_group_create(store, &root, &result);
  revision = result.revision;
  if (rc == TURBO_OK) {
    user.root_group_id = "root-a";
    user.principal_id = "admin-a";
    user.principal_type = "operator";
    user.actor = "bootstrap";
    user.request_id = "integration-user";
    user.expected_revision = revision;
    user.occurred_at = 2u;
    result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    rc = flowie_control_store_user_create(store, &user, &result);
    revision = result.revision;
  }
  if (rc == TURBO_OK) {
    issue.root_group_id = "root-a";
    issue.principal_id = "admin-a";
    issue.actor = "bootstrap";
    issue.request_id = "integration-credential";
    issue.expected_revision = revision;
    issue.occurred_at = 3u;
    issue.initial_secret = CONTROL_INTEGRATION_ADMIN_PASSWORD;
    issue.initial_secret_size = sizeof(CONTROL_INTEGRATION_ADMIN_PASSWORD) - 1u;
    rc = flowie_control_store_credential_generate(store, &issue, &generated);
    revision = generated.revision;
  }
  if (rc == TURBO_OK &&
      tn_base64_encode_buf((const uint8_t *)CONTROL_INTEGRATION_ADMIN_PASSWORD,
                           sizeof(CONTROL_INTEGRATION_ADMIN_PASSWORD) - 1u, secret_base64,
                           secret_base64_capacity) != 0)
    rc = TURBO_ENOMEM;
  flowie_control_generated_credential_wipe(&generated);
  if (rc == TURBO_OK) {
    role.root_group_id = "root-a";
    role.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN;
    role.actor = "bootstrap";
    role.request_id = "integration-role";
    role.expected_revision = revision;
    role.occurred_at = 4u;
    result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    rc = flowie_control_store_role_create(store, &role, &result);
    revision = result.revision;
  }
  if (rc == TURBO_OK) {
    assignment.root_group_id = "root-a";
    assignment.principal_id = "admin-a";
    assignment.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN;
    assignment.actor = "bootstrap";
    assignment.request_id = "integration-assignment";
    assignment.expected_revision = revision;
    assignment.occurred_at = 5u;
    result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    rc = flowie_control_store_user_role_add(store, &assignment, &result);
    revision = result.revision;
  }
  if (rc == TURBO_OK) {
    rule.root_group_id = "root-a";
    rule.ordinal = 10u;
    rule.rule_line = "allow|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/events/#";
    rule.actor = "bootstrap";
    rule.request_id = "integration-policy-rule";
    rule.expected_revision = revision;
    rule.occurred_at = 6u;
    result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    rc = flowie_control_store_policy_rule_put(store, &rule, &result);
    revision = result.revision;
  }
  if (rc == TURBO_OK) {
    publish.root_group_id = "root-a";
    publish.actor = "bootstrap";
    publish.request_id = "integration-policy-publish";
    publish.expected_revision = revision;
    publish.occurred_at = 7u;
    publish.expires_at = CONTROL_INTEGRATION_POLICY_EXPIRES_AT;
    rc = flowie_control_store_policy_publish(store, &publish, &published);
  }
  flowie_control_store_destroy(store);
  return rc;
}

static int control_test_write_config(const char *path, const char *database_path,
                                     const control_tls_material_t *material, unsigned short port) {
  char yaml[8192];
  int size;
  if (!path || !database_path || !material) return -1;
  size = snprintf(yaml, sizeof(yaml),
                  "version: 1\n"
                  "listener:\n"
                  "  host: 127.0.0.1\n"
                  "  port: %u\n"
                  "  tls:\n"
                  "    cert_file: '%s'\n"
                  "    key_file: '%s'\n"
                  "    client_auth: none\n"
                  "storage:\n"
                  "  sqlite:\n"
                  "    path: '%s'\n"
                  "management:\n"
                  "  rpc_path: /v1/management/rpc\n"
                  "  session:\n"
                  "    capacity: 64\n"
                  "    ttl_seconds: 3600\n"
                  "dashboard:\n"
                  "  enabled: true\n"
                  "auth:\n"
                  "  enabled: true\n"
                  "  listener_id: flowie-control-auth\n"
                  "  method: password\n"
                  "  service_bindings:\n"
                  "    - service_id: integration-broker\n"
                  "      token_ref: env://" CONTROL_INTEGRATION_SERVICE_TOKEN_ENV "\n"
                  "      root_group: root-a\n",
                  (unsigned int)port, material->server_cert_path, material->server_key_path,
                  database_path);
  if (size <= 0 || (size_t)size >= sizeof(yaml)) return -1;
  return tt_write_file(path, yaml, (size_t)size);
}

static http_response_t *control_test_request(const control_http_state_t *state,
                                             const char *cert_path, const char *key_path,
                                             const char *body) {
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  turbo_tls_client_config_t tls = {0};
  if (!state || !state->base_url || !body) return NULL;
  client = http_client_create(state->base_url);
  if (!client) return NULL;
  http_client_set_timeout(client, CONTROL_INTEGRATION_REQUEST_TIMEOUT_MS);
  tls.verify_peer = 1;
  tls.ca_file = state->ca_path;
  tls.cert_file = cert_path;
  tls.key_file = key_path;
  if (http_client_set_tls_client_config(client, &tls) != TURBO_OK) goto done;
  response = http_post_json(client, "/v1/management/rpc", body);
done:
  http_client_destroy(client);
  return response;
}

static http_response_t *control_test_acl_request(const control_http_state_t *state,
                                                 const char *cert_path, const char *key_path,
                                                 const char *token, uint64_t required_version) {
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  turbo_tls_client_config_t tls = {0};
  const char *headers[2];
  char authorization[128];
  char version[64];
  int header_count = 0;
  if (!state || !state->base_url || !token ||
      snprintf(authorization, sizeof(authorization), "Authorization: Bearer %s", token) <= 0)
    return NULL;
  headers[header_count++] = authorization;
  if (required_version > 0u) {
    if (snprintf(version, sizeof(version), "X-TurboFlow-Policy-Version: %" PRIu64,
                 required_version) <= 0)
      return NULL;
    headers[header_count++] = version;
  }
  client = http_client_create(state->base_url);
  if (!client) return NULL;
  http_client_set_timeout(client, CONTROL_INTEGRATION_REQUEST_TIMEOUT_MS);
  tls.verify_peer = 1;
  tls.ca_file = state->ca_path;
  tls.cert_file = cert_path;
  tls.key_file = key_path;
  if (http_client_set_tls_client_config(client, &tls) != TURBO_OK) goto done;
  response = http_request(client, HTTP_GET, "/v3/acl", headers, header_count, NULL, 0u);
done:
  http_client_destroy(client);
  return response;
}

static http_response_t *control_test_auth_request(const control_http_state_t *state,
                                                  const char *secret_base64) {
  const char *headers[] = {"Content-Type: application/json",
                           "Authorization: Bearer " CONTROL_INTEGRATION_SERVICE_TOKEN};
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  turbo_tls_client_config_t tls = {0};
  char body[1024];
  int body_size;
  if (!state || !state->base_url || !secret_base64) return NULL;
  body_size = snprintf(body, sizeof(body),
                       "{\"version\":3,\"identity\":\"admin-a\",\"method\":\"password\","
                       "\"secret_base64\":\"%s\",\"protocol\":\"mqtt\","
                       "\"remote_address\":\"127.0.0.1:1883\","
                       "\"peer_certificate_sha256\":\"\"}",
                       secret_base64);
  if (body_size <= 0 || (size_t)body_size >= sizeof(body)) return NULL;
  client = http_client_create(state->base_url);
  if (!client) return NULL;
  http_client_set_timeout(client, CONTROL_INTEGRATION_REQUEST_TIMEOUT_MS);
  tls.verify_peer = 1;
  tls.ca_file = state->ca_path;
  if (http_client_set_tls_client_config(client, &tls) != TURBO_OK) goto done;
  response = http_request(client, HTTP_POST, "/v3/authenticate", headers,
                          (int)(sizeof(headers) / sizeof(headers[0])), body, (size_t)body_size);
done:
  memset(body, 0, sizeof(body));
  http_client_destroy(client);
  return response;
}

static int control_test_hidden_value(const char *html, const char *name, char *value_out,
                                     size_t value_capacity) {
  char marker[96];
  const char *value;
  const char *end;
  size_t value_size;
  int marker_size;
  if (value_out && value_capacity > 0u) value_out[0] = '\0';
  if (!html || !name || !value_out || value_capacity == 0u) return 0;
  marker_size = snprintf(marker, sizeof(marker), "name=\"%s\" value=\"", name);
  if (marker_size <= 0 || (size_t)marker_size >= sizeof(marker)) return 0;
  value = strstr(html, marker);
  if (!value) return 0;
  value += (size_t)marker_size;
  end = strchr(value, '"');
  if (!end) return 0;
  value_size = (size_t)(end - value);
  if (value_size == 0u || value_size >= value_capacity) return 0;
  memcpy(value_out, value, value_size);
  value_out[value_size] = '\0';
  return 1;
}

static int control_test_management_login(
    const control_http_state_t *state, http_client_t *client, http_cookie_jar_t *jar,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  http_response_t *response = NULL;
  char origin[160];
  char form[512];
  const char *headers[2];
  const char *token;
  int result = 0;
  if (token_out) token_out[0] = '\0';
  if (!state || !client || !jar || !token_out ||
      snprintf(origin, sizeof(origin), "Origin: %s", state->base_url) <= 0 ||
      snprintf(form, sizeof(form), "root_group=root-a&principal=admin-a&password=%s",
               CONTROL_INTEGRATION_ADMIN_PASSWORD) <= 0)
    goto done;
  headers[0] = "Content-Type: application/x-www-form-urlencoded";
  headers[1] = origin;
  response = http_request(client, HTTP_POST, "/v1/management/login", headers, 2, form,
                          strlen(form));
  token = http_cookie_jar_get(jar, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE);
  if (response && response->status_code == 303 && response->error_code == HTTP_ERROR_NONE &&
      token &&
      strnlen(token, FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u) ==
          FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE) {
    memcpy(token_out, token, FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u);
    result = 1;
  }

done:
  http_response_free(response);
  memset(form, 0, sizeof(form));
  return result;
}

static http_response_t *control_test_dashboard_content(http_client_t *client) {
  const char *headers[] = {"HX-Request: true"};
  if (!client) return NULL;
  return http_request(client, HTTP_GET, "/v1/management/dashboard/content?section=users", headers,
                      1, NULL, 0u);
}

static http_response_t *control_test_dashboard_action(http_client_t *client, const char *form) {
  const char *headers[] = {"Content-Type: application/x-www-form-urlencoded",
                           "HX-Request: true"};
  if (!client || !form) return NULL;
  return http_request(client, HTTP_POST, "/v1/management/dashboard/action?section=users", headers,
                      2, form, strlen(form));
}

static int control_test_management_workflow(control_http_state_t *state) {
  http_client_t *client = NULL;
  http_cookie_jar_t *jar = NULL;
  http_response_t *response = NULL;
  turbo_tls_client_config_t tls = {0};
  char first_token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  char second_token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char invalid_csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u];
  char revision[CONTROL_INTEGRATION_REVISION_CAPACITY] = {0};
  char origin[160];
  char cookie_header[128];
  char form[CONTROL_INTEGRATION_FORM_CAPACITY];
  const char *logout_headers[2];
  char *set_cookie = NULL;
  int form_size;
  int result = 0;
  if (!state) goto done;
  if (snprintf(origin, sizeof(origin), "Origin: %s", state->base_url) <= 0) goto done;
  client = http_client_create(state->base_url);
  jar = http_cookie_jar_create();
  if (!client || !jar) goto done;
  http_client_set_timeout(client, CONTROL_INTEGRATION_REQUEST_TIMEOUT_MS);
  http_client_follow_redirects(client, 0);
  http_client_set_cookie_jar(client, jar);
  tls.verify_peer = 1;
  tls.ca_file = state->ca_path;
  if (http_client_set_tls_client_config(client, &tls) != TURBO_OK) goto done;

  if (!control_test_management_login(state, client, jar, first_token)) goto done;
  response = http_post_json(client, "/v1/management/rpc", CONTROL_INTEGRATION_STATUS_RPC_BODY);
  state->session_rpc_ok =
      response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "\"result\"") != NULL;
  http_response_free(response);
  response = control_test_dashboard_content(client);
  state->dashboard_htmx_ok =
      response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "aria-current=\"page\">Users") != NULL &&
      strstr(response->body, "<section id=\"users\"") != NULL &&
      strstr(response->body, "<section id=\"groups\"") == NULL &&
      control_test_hidden_value(response->body, "csrf", csrf, sizeof(csrf)) &&
      strlen(csrf) == FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE &&
      control_test_hidden_value(response->body, "expected_revision", revision,
                                sizeof(revision));
  http_response_free(response);
  response = NULL;
  if (!state->session_rpc_ok || !state->dashboard_htmx_ok) goto done;

  memset(invalid_csrf, 'b', FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE);
  invalid_csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE] = '\0';
  form_size = snprintf(form, sizeof(form),
                       "csrf=%s&operation=user.create&principal_id=dashboard-denied&"
                       "principal_type=operator&request_id=integration-dashboard-csrf&"
                       "expected_revision=%s",
                       invalid_csrf, revision);
  if (form_size <= 0 || (size_t)form_size >= sizeof(form)) goto done;
  response = control_test_dashboard_action(client, form);
  state->dashboard_csrf_rejected =
      response && response->status_code == 403 && response->error_code == HTTP_ERROR_NONE;
  http_response_free(response);
  response = NULL;
  if (!state->dashboard_csrf_rejected) goto done;

  form_size = snprintf(form, sizeof(form),
                       "csrf=%s&operation=user.create&principal_id=dashboard-created&"
                       "principal_type=operator&request_id=integration-dashboard-create&"
                       "expected_revision=%s",
                       csrf, revision);
  if (form_size <= 0 || (size_t)form_size >= sizeof(form)) goto done;
  response = control_test_dashboard_action(client, form);
  state->dashboard_write_ok =
      response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "dashboard-created") != NULL &&
      control_test_hidden_value(response->body, "expected_revision", revision,
                                sizeof(revision));
  http_response_free(response);
  response = NULL;
  if (!state->dashboard_write_ok) goto done;

  if (snprintf(cookie_header, sizeof(cookie_header), "Cookie: %s=%s",
               FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE, first_token) <= 0)
    goto done;
  logout_headers[0] = origin;
  logout_headers[1] = cookie_header;
  http_client_set_cookie_jar(client, NULL);
  response =
      http_request(client, HTTP_POST, "/v1/management/logout", logout_headers, 2, NULL, 0u);
  set_cookie = response ? http_response_get_header(response, "Set-Cookie") : NULL;
  state->dashboard_logout_ok =
      response && response->status_code == 303 && response->error_code == HTTP_ERROR_NONE &&
      set_cookie && strstr(set_cookie, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE "=") != NULL &&
      strstr(set_cookie, "Max-Age=0") != NULL && strstr(set_cookie, "SameSite=Strict") != NULL &&
      strstr(set_cookie, "HttpOnly") != NULL && strstr(set_cookie, "Secure") != NULL;
  free(set_cookie);
  set_cookie = NULL;
  http_response_free(response);
  response = NULL;
  http_cookie_jar_remove(jar, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE);
  http_client_set_cookie_jar(client, jar);
  if (!state->dashboard_logout_ok ||
      !control_test_management_login(state, client, jar, second_token) ||
      strcmp(first_token, second_token) == 0)
    goto done;

  response = control_test_dashboard_content(client);
  if (!response || response->status_code != 200 || !response->body ||
      !control_test_hidden_value(response->body, "csrf", csrf, sizeof(csrf)) ||
      !control_test_hidden_value(response->body, "expected_revision", revision,
                                 sizeof(revision)))
    goto done;
  http_response_free(response);
  response = NULL;
  form_size = snprintf(form, sizeof(form),
                       "csrf=%s&operation=role.remove&principal_id=admin-a&role_id=%s&"
                       "request_id=integration-dashboard-role-remove&expected_revision=%s",
                       csrf, FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN, revision);
  if (form_size <= 0 || (size_t)form_size >= sizeof(form)) goto done;
  response = control_test_dashboard_action(client, form);
  if (!response || response->status_code != 200 || response->error_code != HTTP_ERROR_NONE)
    goto done;
  http_response_free(response);
  response = control_test_dashboard_content(client);
  state->dashboard_role_revocation_ok =
      response && response->status_code == 401 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "Authentication required") != NULL;
  result = state->dashboard_role_revocation_ok;

done:
  free(set_cookie);
  http_response_free(response);
  if (client) {
    http_client_set_cookie_jar(client, NULL);
    http_client_destroy(client);
  }
  http_cookie_jar_destroy(jar);
  memset(first_token, 0, sizeof(first_token));
  memset(second_token, 0, sizeof(second_token));
  memset(csrf, 0, sizeof(csrf));
  memset(invalid_csrf, 0, sizeof(invalid_csrf));
  memset(cookie_header, 0, sizeof(cookie_header));
  memset(form, 0, sizeof(form));
  return result;
}

static void control_test_http_task(coro_t *coroutine, void *arg) {
  control_http_state_t *state = (control_http_state_t *)arg;
  uint64_t deadline;
  http_response_t *response = NULL;
  (void)coroutine;
  if (!state || !state->context) return;
  deadline = turbo_monotonic_ms() + CONTROL_INTEGRATION_TIMEOUT_MS;
  while (turbo_monotonic_ms() < deadline) {
    response = control_test_auth_request(state, state->secret_base64);
    if (response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
        response->body && strstr(response->body, "\"authenticated\":true") != NULL) {
      state->ready = 1;
      state->local_auth_ok = strstr(response->body, "\"root_group\":\"root-a\"") != NULL &&
                             strstr(response->body, "\"policy_version\":1") != NULL;
      http_response_free(response);
      response = NULL;
      break;
    }
    http_response_free(response);
    response = NULL;
    coro_sleep(state->context, 25u);
  }
  if (!state->ready) return;

  (void)control_test_management_workflow(state);

  response = control_test_request(state, NULL, NULL, CONTROL_INTEGRATION_STATUS_RPC_BODY);
  state->unauthenticated_rpc_forbidden =
      response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "Authentication required") != NULL;
  http_response_free(response);

  response = control_test_auth_request(state, "d3Jvbmctc2VjcmV0");
  state->local_auth_bad_secret_forbidden =
      response && response->status_code == 403 && response->error_code == HTTP_ERROR_NONE;
  http_response_free(response);

  response = control_test_acl_request(state, NULL, NULL,
                                      CONTROL_INTEGRATION_SERVICE_TOKEN, 1u);
  state->acl_bundle_ok =
      response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "\"version\":3") != NULL &&
      strstr(response->body, "\"policy_version\":1") != NULL &&
      strstr(response->body, "allow|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/events/#") !=
          NULL;
  http_response_free(response);

  response = control_test_acl_request(state, NULL, NULL,
                                      CONTROL_INTEGRATION_SERVICE_TOKEN, 2u);
  state->acl_version_miss_ok =
      response && response->status_code == 404 && response->error_code == HTTP_ERROR_NONE;
  http_response_free(response);

  response =
      control_test_acl_request(state, NULL, NULL, "wrong", 1u);
  state->acl_bad_token_forbidden =
      response && response->status_code == 403 && response->error_code == HTTP_ERROR_NONE;
  http_response_free(response);

  response = control_test_request(state, state->unknown_cert_path, state->unknown_key_path,
                                  CONTROL_INTEGRATION_STATUS_RPC_BODY);
  state->client_certificate_does_not_authenticate_rpc =
      response && response->status_code == 200 && response->error_code == HTTP_ERROR_NONE &&
      response->body && strstr(response->body, "Authentication required") != NULL;
  http_response_free(response);
}

static int control_test_run_network_gate(void) {
  control_tls_material_t material;
  control_http_state_t http_state;
  turbo_process_options_t process_options;
  turbo_process_result_t process_result;
  turbo_process_t *process = NULL;
  coro_context_t *context = NULL;
  char *database_path = NULL;
  char *config_path = NULL;
  char base_url[128];
  char secret_base64[CONTROL_INTEGRATION_SECRET_BASE64_CAPACITY] = {0};
  const char *process_args[] = {"--config", NULL, NULL};
  unsigned short port = 0u;
  int rc = TURBO_EIO;
  int service_token_set = 0;
  const char *failure_stage = "initialization";

  memset(&material, 0, sizeof(material));
  memset(&http_state, 0, sizeof(http_state));
  database_path = tt_make_temp_file("flowie-control-it", ".sqlite3");
  config_path = tt_make_temp_file("flowie-control-it", ".yml");
  if (!database_path || !config_path) {
    failure_stage = "temporary paths";
    goto cleanup;
  }
  failure_stage = "reserve port";
  if (control_test_reserve_port(&port) != 0) goto cleanup;
  failure_stage = "generate TLS material";
  if (control_test_tls_material_open(&material) != 0) goto cleanup;
  failure_stage = "seed SQLite store";
  if (control_test_seed_store(database_path, secret_base64, sizeof(secret_base64)) != TURBO_OK)
    goto cleanup;
  failure_stage = "write controller configuration";
  if (control_test_write_config(config_path, database_path, &material, port) != 0) goto cleanup;
  failure_stage = "set service token";
  if (control_test_set_env(CONTROL_INTEGRATION_SERVICE_TOKEN_ENV,
                           CONTROL_INTEGRATION_SERVICE_TOKEN) != 0)
    goto cleanup;
  service_token_set = 1;
  failure_stage = "spawn controller";

  process_args[1] = config_path;
  turbo_process_options_init(&process_options);
  process_options.program = FLOWIE_CONTROL_EXECUTABLE;
  process_options.args = process_args;
  process_options.flags = TURBO_PROCESS_CAPTURE_STDERR;
  process_options.max_output_bytes = 65536u;
  if (turbo_process_spawn(&process_options, &process) != TURBO_OK) goto cleanup;

  failure_stage = "create client coroutine context";
  (void)snprintf(base_url, sizeof(base_url), "https://localhost:%u", (unsigned int)port);
  context = coro_context_create(NULL);
  if (!context) goto cleanup;
  http_state.context = context;
  http_state.base_url = base_url;
  http_state.ca_path = material.ca_path;
  http_state.known_cert_path = material.known_cert_path;
  http_state.known_key_path = material.known_key_path;
  http_state.unknown_cert_path = material.unknown_cert_path;
  http_state.unknown_key_path = material.unknown_key_path;
  http_state.secret_base64 = secret_base64;
  failure_stage = "spawn client coroutine";
  if (coro_context_spawn(context, control_test_http_task, &http_state) != TURBO_OK) goto cleanup;
  failure_stage = "complete HTTPS ACL requests";
  coro_context_run(context, TURBO_RUN_DEFAULT);
  if (http_state.ready && http_state.unauthenticated_rpc_forbidden &&
      http_state.session_rpc_ok && http_state.dashboard_htmx_ok &&
      http_state.dashboard_csrf_rejected && http_state.dashboard_write_ok &&
      http_state.dashboard_logout_ok && http_state.dashboard_role_revocation_ok &&
      http_state.local_auth_ok &&
      http_state.local_auth_bad_secret_forbidden && http_state.acl_bundle_ok &&
      http_state.acl_version_miss_ok && http_state.acl_bad_token_forbidden &&
      http_state.client_certificate_does_not_authenticate_rpc)
    rc = TURBO_OK;

cleanup:
  if (rc != TURBO_OK) {
    (void)fprintf(stderr,
                  "flowie-control integration failed at %s: ready=%d rpc-denied=%d session=%d "
                  "dashboard-htmx=%d dashboard-csrf=%d dashboard-write=%d "
                  "dashboard-logout=%d dashboard-role-revoke=%d "
                  "local-auth=%d local-auth-bad=%d "
                  "acl=%d acl-miss=%d acl-token=%d client-cert-rpc=%d\n",
                  failure_stage, http_state.ready, http_state.unauthenticated_rpc_forbidden,
                  http_state.session_rpc_ok,
                  http_state.dashboard_htmx_ok, http_state.dashboard_csrf_rejected,
                  http_state.dashboard_write_ok, http_state.dashboard_logout_ok,
                  http_state.dashboard_role_revocation_ok,
                  http_state.local_auth_ok, http_state.local_auth_bad_secret_forbidden,
                  http_state.acl_bundle_ok, http_state.acl_version_miss_ok,
                  http_state.acl_bad_token_forbidden,
                  http_state.client_certificate_does_not_authenticate_rpc);
  }
  if (context) coro_context_destroy(context);
  if (process) {
    if (turbo_process_poll(process, &process_result) == TURBO_EBUSY) {
      (void)turbo_process_terminate(process);
      (void)turbo_process_wait(process, &process_result);
    }
    if (rc != TURBO_OK) {
      char child_error[4096];
      size_t child_error_size = 0u;
      if (turbo_process_read_stderr(process, child_error, sizeof(child_error) - 1u,
                                    &child_error_size) == TURBO_OK &&
          child_error_size > 0u) {
        child_error[child_error_size] = '\0';
        (void)fprintf(stderr, "flowie-control integration child stderr: %s\n", child_error);
      }
    }
    turbo_process_destroy(process);
  }
  if (service_token_set) (void)control_test_set_env(CONTROL_INTEGRATION_SERVICE_TOKEN_ENV, NULL);
  memset(secret_base64, 0, sizeof(secret_base64));
  control_test_tls_material_close(&material);
  if (config_path) {
    (void)tt_remove_file(config_path);
    free(config_path);
  }
  if (database_path) {
    (void)tt_remove_file(database_path);
    free(database_path);
  }
  return rc;
}

spec("Flowie controller HTTPS integration") {
  it("serves scoped Broker Auth and ACL over TLS without authenticating RPC by certificate") {
    check_int_eq(control_test_run_network_gate(), TURBO_OK);
  }
}
