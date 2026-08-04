#include "flowie_control_external_https_authenticator_internal.h"

#include "CoroNet/turbo_coro_context.h"
#include "base64_utils.h"
#include "http_client.h"
#include "monocypher.h"
#include "turbo_coro.h"
#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str.h"

#include <openssl/ssl.h>

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_control_external_https_tls_s {
  tstr_t ca_file;
  tstr_t client_cert_file;
  tstr_t client_key_file;
  tstr_t client_key_password_ref;
} flowie_control_external_https_tls_t;

typedef enum external_https_outcome_e {
  EXTERNAL_HTTPS_OUTCOME_LOCAL_FAILURE = 0,
  EXTERNAL_HTTPS_OUTCOME_SUCCEEDED,
  EXTERNAL_HTTPS_OUTCOME_DENIED,
  EXTERNAL_HTTPS_OUTCOME_LOCAL_OVERLOAD,
  EXTERNAL_HTTPS_OUTCOME_REMOTE_OVERLOAD,
  EXTERNAL_HTTPS_OUTCOME_REMOTE_SERVER_FAILURE,
  EXTERNAL_HTTPS_OUTCOME_TRANSPORT_FAILURE,
  EXTERNAL_HTTPS_OUTCOME_PROTOCOL_FAILURE
} external_https_outcome_t;

struct flowie_control_external_https_authenticator_s {
  flowie_control_external_authenticator_t interface;
  tstr_t url;
  tstr_t host;
  tstr_t method;
  tstr_t service_token_ref;
  uint16_t port;
  uint32_t timeout_ms;
  size_t max_response_size;
  uint32_t max_in_flight;
  atomic_uint in_flight;
  atomic_uint_fast64_t started_requests;
  atomic_uint_fast64_t succeeded;
  atomic_uint_fast64_t denied;
  atomic_uint_fast64_t local_overload;
  atomic_uint_fast64_t remote_overload;
  atomic_uint_fast64_t remote_server_failures;
  atomic_uint_fast64_t transport_failures;
  atomic_uint_fast64_t protocol_failures;
  atomic_uint_fast64_t local_failures;
  turbo_flow_security_key_provider_t key_provider;
  flowie_control_external_https_tls_t tls;
};

static void external_https_counter_increment(atomic_uint_fast64_t *counter) {
  uint_fast64_t current;
  if (!counter) return;
  current = atomic_load_explicit(counter, memory_order_relaxed);
  while (current < UINT64_MAX &&
         !atomic_compare_exchange_weak_explicit(counter, &current, current + 1u,
                                                memory_order_relaxed, memory_order_relaxed)) {
  }
}

static void
external_https_record_outcome(flowie_control_external_https_authenticator_t *authenticator,
                              external_https_outcome_t outcome) {
  atomic_uint_fast64_t *counter;
  if (!authenticator) return;
  switch (outcome) {
  case EXTERNAL_HTTPS_OUTCOME_SUCCEEDED:
    counter = &authenticator->succeeded;
    break;
  case EXTERNAL_HTTPS_OUTCOME_DENIED:
    counter = &authenticator->denied;
    break;
  case EXTERNAL_HTTPS_OUTCOME_LOCAL_OVERLOAD:
    counter = &authenticator->local_overload;
    break;
  case EXTERNAL_HTTPS_OUTCOME_REMOTE_OVERLOAD:
    counter = &authenticator->remote_overload;
    break;
  case EXTERNAL_HTTPS_OUTCOME_REMOTE_SERVER_FAILURE:
    counter = &authenticator->remote_server_failures;
    break;
  case EXTERNAL_HTTPS_OUTCOME_TRANSPORT_FAILURE:
    counter = &authenticator->transport_failures;
    break;
  case EXTERNAL_HTTPS_OUTCOME_PROTOCOL_FAILURE:
    counter = &authenticator->protocol_failures;
    break;
  case EXTERNAL_HTTPS_OUTCOME_LOCAL_FAILURE:
  default:
    counter = &authenticator->local_failures;
    break;
  }
  external_https_counter_increment(counter);
}

static int external_https_text_valid(const char *value, size_t maximum, int required) {
  size_t size;
  if (!value) return !required;
  size = strnlen(value, maximum + 1u);
  if (size > maximum || (required && size == 0u)) return 0;
  for (size_t index = 0u; index < size; ++index) {
    unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

static int external_https_fingerprint_valid(const char *value) {
  static const char prefix[] = "sha256:";
  size_t size;
  if (!value) return 1;
  size = strlen(value);
  if (size != FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE ||
      memcmp(value, prefix, sizeof(prefix) - 1u) != 0)
    return 0;
  for (size_t i = sizeof(prefix) - 1u; i < size; ++i)
    if (!((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f'))) return 0;
  return 1;
}

static int external_https_optional_path_valid(const char *value) {
  if (!value) return 1;
  return external_https_text_valid(value, FLOWIE_CONTROL_EXTERNAL_HTTPS_TLS_PATH_MAX, 1);
}

static int external_https_ascii_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
  while (*left && *right) {
    if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) return 0;
    ++left;
    ++right;
  }
  return *left == '\0' && *right == '\0';
}

static int external_https_ascii_prefix(const char *value, size_t value_size, const char *prefix) {
  size_t prefix_size = strlen(prefix);
  if (!value || value_size < prefix_size) return 0;
  for (size_t index = 0u; index < prefix_size; ++index)
    if (tolower((unsigned char)value[index]) != tolower((unsigned char)prefix[index])) return 0;
  return 1;
}

static int external_https_json_fields_exact(const json_value_t *object, const char *const *allowed,
                                            size_t allowed_count) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT ||
      turbo_json_object_size(object) != allowed_count)
    return TURBO_EPROTO;
  for (size_t field_index = 0u; field_index < turbo_json_object_size(object); ++field_index) {
    const char *field = turbo_json_object_key(object, field_index);
    size_t matches = 0u;
    for (size_t allowed_index = 0u; allowed_index < allowed_count; ++allowed_index)
      if (field && strcmp(field, allowed[allowed_index]) == 0) ++matches;
    if (matches != 1u) return TURBO_EPROTO;
    for (size_t previous = 0u; previous < field_index; ++previous) {
      const char *previous_field = turbo_json_object_key(object, previous);
      if (previous_field && field && strcmp(previous_field, field) == 0) return TURBO_EPROTO;
    }
  }
  return TURBO_OK;
}

static int external_https_json_u64(const json_value_t *value, uint64_t *out) {
  const char *text;
  char buffer[32];
  char *end = NULL;
  size_t size = 0u;
  unsigned long long parsed;
  if (!value || turbo_json_type(value) != TURBO_JSON_NUMBER || !out) return TURBO_EPROTO;
  text = turbo_json_number_text(value, &size);
  if (!text || size == 0u || size >= sizeof(buffer)) return TURBO_EPROTO;
  memcpy(buffer, text, size);
  buffer[size] = '\0';
  if (buffer[0] == '-' || buffer[0] == '+' || (size > 1u && buffer[0] == '0')) return TURBO_EPROTO;
  errno = 0;
  parsed = strtoull(buffer, &end, 10);
  if (errno == ERANGE || !end || *end != '\0') return TURBO_EPROTO;
  *out = (uint64_t)parsed;
  return TURBO_OK;
}

static int external_https_copy_json_string(const json_value_t *object, const char *field, char *out,
                                           size_t capacity) {
  json_value_t *value;
  const char *text;
  size_t size;
  if (!object || !field || !out || capacity == 0u) return TURBO_EPROTO;
  value = turbo_json_object_get(object, field);
  if (!value || turbo_json_type(value) != TURBO_JSON_STRING) return TURBO_EPROTO;
  text = turbo_json_string(value);
  size = turbo_json_string_len(value);
  if (!text || size == 0u || size >= capacity || memchr(text, '\0', size)) return TURBO_EPROTO;
  memcpy(out, text, size);
  out[size] = '\0';
  return external_https_text_valid(out, capacity - 1u, 1) ? TURBO_OK : TURBO_EPROTO;
}

static int external_https_copy_groups(const json_value_t *object,
                                      flowie_control_external_auth_assertion_t *assertion) {
  json_value_t *groups = turbo_json_object_get(object, "groups");
  size_t count;
  if (!groups || turbo_json_type(groups) != TURBO_JSON_ARRAY || !assertion) return TURBO_EPROTO;
  count = turbo_json_array_size(groups);
  if (count > TURBO_FLOW_SECURITY_MAX_GROUPS) return TURBO_EPROTO;
  for (size_t index = 0u; index < count; ++index) {
    json_value_t *entry = turbo_json_array_get(groups, index);
    const char *text;
    size_t size;
    if (!entry || turbo_json_type(entry) != TURBO_JSON_STRING) return TURBO_EPROTO;
    text = turbo_json_string(entry);
    size = turbo_json_string_len(entry);
    if (!text || size == 0u || size > TURBO_FLOW_SECURITY_ID_MAX || memchr(text, '\0', size))
      return TURBO_EPROTO;
    memcpy(assertion->external_groups[index], text, size);
    assertion->external_groups[index][size] = '\0';
    if (!external_https_text_valid(assertion->external_groups[index], TURBO_FLOW_SECURITY_ID_MAX,
                                   1))
      return TURBO_EPROTO;
    for (size_t previous = 0u; previous < index; ++previous)
      if (strcmp(assertion->external_groups[previous], assertion->external_groups[index]) == 0)
        return TURBO_EPROTO;
  }
  assertion->external_group_count = (uint32_t)count;
  return TURBO_OK;
}

int flowie_control_external_https_decode_response(
    const char *body, size_t body_size, const char *method,
    flowie_control_external_auth_assertion_t *assertion_out) {
  static const char *const denied_fields[] = {"version", "authenticated"};
  static const char *const outer_fields[] = {"version", "authenticated", "assertion"};
  static const char *const assertion_fields[] = {
      "issuer",     "subject",  "subject_type",    "auth_method",     "issued_at",
      "expires_at", "revision", "assurance_level", "account_enabled", "groups"};
  flowie_control_external_auth_assertion_t assertion = FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *authenticated;
  json_value_t *assertion_object;
  json_value_t *account_enabled;
  uint64_t version = 0u;
  uint64_t assurance = 0u;
  int rc = TURBO_EPROTO;
  if (assertion_out && assertion_out->size >= sizeof(*assertion_out)) *assertion_out = assertion;
  if (!body || body_size == 0u || body_size > FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_RESPONSE_SIZE ||
      !external_https_text_valid(method, TURBO_FLOW_SECURITY_TYPE_MAX, 1) || !assertion_out ||
      assertion_out->size < sizeof(*assertion_out))
    return TURBO_EINVAL;
  if (turbo_parse_json((const uint8_t *)body, body_size, &document) != TURBO_OK || !document)
    return TURBO_EPROTO;
  authenticated = turbo_json_object_get(document, "authenticated");
  if (!authenticated || turbo_json_type(authenticated) != TURBO_JSON_BOOL ||
      external_https_json_u64(turbo_json_object_get(document, "version"), &version) != TURBO_OK ||
      version != FLOWIE_CONTROL_EXTERNAL_HTTPS_PROTOCOL_VERSION)
    goto done;
  if (!turbo_json_bool(authenticated)) {
    if (external_https_json_fields_exact(
            document, denied_fields, sizeof(denied_fields) / sizeof(denied_fields[0])) != TURBO_OK)
      goto done;
    rc = TURBO_EPERM;
    goto done;
  }
  if (external_https_json_fields_exact(document, outer_fields,
                                       sizeof(outer_fields) / sizeof(outer_fields[0])) != TURBO_OK)
    goto done;
  assertion_object = turbo_json_object_get(document, "assertion");
  if (external_https_json_fields_exact(assertion_object, assertion_fields,
                                       sizeof(assertion_fields) / sizeof(assertion_fields[0])) !=
      TURBO_OK)
    goto done;
  if (external_https_copy_json_string(assertion_object, "issuer", assertion.issuer,
                                      sizeof(assertion.issuer)) != TURBO_OK ||
      external_https_copy_json_string(assertion_object, "subject", assertion.subject,
                                      sizeof(assertion.subject)) != TURBO_OK ||
      external_https_copy_json_string(assertion_object, "subject_type", assertion.subject_type,
                                      sizeof(assertion.subject_type)) != TURBO_OK ||
      external_https_copy_json_string(assertion_object, "auth_method", assertion.auth_method,
                                      sizeof(assertion.auth_method)) != TURBO_OK ||
      strcmp(assertion.auth_method, method) != 0 ||
      external_https_json_u64(turbo_json_object_get(assertion_object, "issued_at"),
                              &assertion.issued_at) != TURBO_OK ||
      external_https_json_u64(turbo_json_object_get(assertion_object, "expires_at"),
                              &assertion.expires_at) != TURBO_OK ||
      external_https_json_u64(turbo_json_object_get(assertion_object, "revision"),
                              &assertion.revision) != TURBO_OK ||
      external_https_json_u64(turbo_json_object_get(assertion_object, "assurance_level"),
                              &assurance) != TURBO_OK ||
      assurance < FLOWIE_CONTROL_EXTERNAL_ASSURANCE_SINGLE_FACTOR ||
      assurance > FLOWIE_CONTROL_EXTERNAL_ASSURANCE_HARDWARE_BOUND || assertion.issued_at == 0u ||
      assertion.expires_at <= assertion.issued_at || assertion.revision == 0u ||
      external_https_copy_groups(assertion_object, &assertion) != TURBO_OK)
    goto done;
  account_enabled = turbo_json_object_get(assertion_object, "account_enabled");
  if (!account_enabled || turbo_json_type(account_enabled) != TURBO_JSON_BOOL) goto done;
  assertion.assurance_level = (uint32_t)assurance;
  assertion.account_enabled = turbo_json_bool(account_enabled) ? 1 : 0;
  *assertion_out = assertion;
  rc = TURBO_OK;

done:
  if (rc != TURBO_OK)
    *assertion_out =
        (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  turbo_free_json(&document);
  return rc;
}

static int external_https_json_add(json_value_t *object, const char *field, json_value_t *value) {
  if (value && turbo_json_object_add_checked(object, field, value)) return TURBO_OK;
  if (value) {
    turbo_json_doc_t *owned = (turbo_json_doc_t *)value;
    turbo_free_json(&owned);
  }
  return TURBO_ENOMEM;
}

static int external_https_request_valid(const flowie_control_external_auth_request_t *request) {
  return request && request->size >= sizeof(*request) &&
         external_https_text_valid(request->domain_id, TURBO_FLOW_SECURITY_ID_MAX, 1) &&
         external_https_text_valid(request->presented_identity, TURBO_FLOW_SECURITY_ID_MAX, 1) &&
         external_https_text_valid(request->method, TURBO_FLOW_SECURITY_TYPE_MAX, 1) &&
         request->secret && request->secret_size > 0u &&
         request->secret_size <= FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX &&
         external_https_text_valid(request->protocol, TURBO_FLOW_SECURITY_TYPE_MAX, 1) &&
         external_https_text_valid(request->remote_address, FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX,
                                   1) &&
         external_https_fingerprint_valid(request->peer_certificate_sha256);
}

int flowie_control_external_https_encode_request(
    const flowie_control_external_auth_request_t *request, char **body_out, size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  char *secret_base64 = NULL;
  int rc = TURBO_ENOMEM;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!external_https_request_valid(request) || !body_out || !body_size_out) return TURBO_EINVAL;
  if (tn_base64_encode(request->secret, request->secret_size, &secret_base64) != 0 ||
      !secret_base64)
    return TURBO_ENOMEM;
  document = (turbo_json_doc_t *)turbo_json_create_object();
  if (!document) goto done;
  if (external_https_json_add(
          document, "version",
          turbo_json_create_uint64(FLOWIE_CONTROL_EXTERNAL_HTTPS_PROTOCOL_VERSION)) != TURBO_OK ||
      external_https_json_add(document, "domain",
                              turbo_json_create_string(request->domain_id)) != TURBO_OK ||
      external_https_json_add(document, "identity",
                              turbo_json_create_string(request->presented_identity)) != TURBO_OK ||
      external_https_json_add(document, "method", turbo_json_create_string(request->method)) !=
          TURBO_OK ||
      external_https_json_add(document, "secret_base64", turbo_json_create_string(secret_base64)) !=
          TURBO_OK ||
      external_https_json_add(document, "protocol", turbo_json_create_string(request->protocol)) !=
          TURBO_OK ||
      external_https_json_add(document, "remote_address",
                              turbo_json_create_string(request->remote_address)) != TURBO_OK ||
      external_https_json_add(document, "peer_certificate_sha256",
                              turbo_json_create_string(request->peer_certificate_sha256
                                                           ? request->peer_certificate_sha256
                                                           : "")) != TURBO_OK)
    goto done;
  *body_out = turbo_json_serialize(document, body_size_out);
  if (!*body_out) goto done;
  rc = TURBO_OK;

done:
  if (secret_base64) {
    crypto_wipe(secret_base64, strlen(secret_base64));
    free(secret_base64);
  }
  turbo_free_json(&document);
  return rc;
}

static int external_https_content_type_json(const char *headers, size_t headers_size) {
  static const char name[] = "content-type:";
  size_t line_start = 0u;
  int matches = 0;
  if (!headers || headers_size == 0u) return 0;
  while (line_start < headers_size) {
    size_t line_end = line_start;
    size_t cursor;
    while (line_end < headers_size && headers[line_end] != '\n')
      ++line_end;
    cursor = line_start;
    if (line_end - cursor >= sizeof(name) - 1u) {
      size_t index;
      for (index = 0u; index < sizeof(name) - 1u; ++index)
        if (tolower((unsigned char)headers[cursor + index]) != name[index]) break;
      if (index == sizeof(name) - 1u) {
        cursor += sizeof(name) - 1u;
        while (cursor < line_end && (headers[cursor] == ' ' || headers[cursor] == '\t'))
          ++cursor;
        if (external_https_ascii_prefix(headers + cursor, line_end - cursor, "application/json") &&
            (cursor + 16u == line_end || headers[cursor + 16u] == ';' ||
             headers[cursor + 16u] == '\r'))
          ++matches;
        else return 0;
      }
    }
    line_start = line_end + 1u;
  }
  return matches == 1;
}

static int external_https_validate_url(const char *url, tstr_t *host_out, uint16_t *port_out) {
  uri_t *uri = NULL;
  const char *scheme;
  const char *host;
  const char *path;
  int port;
  int rc = TURBO_EINVAL;
  if (!url || !url[0] || !host_out || !port_out ||
      turbo_parse_uri((const uint8_t *)url, strlen(url), &uri) != TURBO_OK || !uri)
    return TURBO_EINVAL;
  scheme = turbo_uri_scheme(uri);
  host = turbo_uri_host(uri);
  path = turbo_uri_path(uri);
  port = turbo_uri_port(uri);
  if (!turbo_uri_is_valid(uri) || !scheme || strcmp(scheme, "https") != 0 || !host || !host[0] ||
      !path || path[0] != '/' || !path[1] ||
      (turbo_uri_userinfo(uri) && turbo_uri_userinfo(uri)[0]) ||
      (turbo_uri_query(uri) && turbo_uri_query(uri)[0]) ||
      (turbo_uri_fragment(uri) && turbo_uri_fragment(uri)[0]) || port < 0 || port > UINT16_MAX)
    goto done;
  *host_out = tstr_dup(host);
  if (!*host_out) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  *port_out = port == 0 ? 443u : (uint16_t)port;
  rc = TURBO_OK;

done:
  turbo_free_uri(&uri);
  return rc;
}

static int external_https_connect_policy(const char *scheme, const char *hostname, uint16_t port,
                                         const turbo_dns_result_t *results, size_t result_count,
                                         void *user_data) {
  const flowie_control_external_https_authenticator_t *authenticator =
      (const flowie_control_external_https_authenticator_t *)user_data;
  (void)results;
  if (!authenticator || !scheme || strcmp(scheme, "https") != 0 || !hostname ||
      !external_https_ascii_equal(hostname, authenticator->host) || port != authenticator->port ||
      result_count == 0u)
    return -1;
  return 0;
}

static int external_https_tls_config_valid(const flowie_control_external_https_tls_config_t *tls) {
  int has_cert;
  int has_key;
  if (!tls) return 1;
  has_cert = tls->client_cert_file != NULL;
  has_key = tls->client_key_file != NULL;
  return has_cert == has_key && (!tls->client_key_password_ref || has_key) &&
         external_https_optional_path_valid(tls->ca_file) &&
         external_https_optional_path_valid(tls->client_cert_file) &&
         external_https_optional_path_valid(tls->client_key_file) &&
         external_https_optional_path_valid(tls->client_key_password_ref);
}

static void external_https_tls_cleanup(flowie_control_external_https_tls_t *tls) {
  if (!tls) return;
  tstr_freep(&tls->ca_file);
  tstr_freep(&tls->client_cert_file);
  tstr_freep(&tls->client_key_file);
  tstr_freep(&tls->client_key_password_ref);
}

static int external_https_tls_init(flowie_control_external_https_tls_t *tls,
                                   const flowie_control_external_https_tls_config_t *config) {
  flowie_control_external_https_tls_t next = {0};
  if (!tls || !external_https_tls_config_valid(config)) return TURBO_EINVAL;
  if (!config) {
    *tls = next;
    return TURBO_OK;
  }
  next.ca_file = config->ca_file ? tstr_dup(config->ca_file) : NULL;
  next.client_cert_file = config->client_cert_file ? tstr_dup(config->client_cert_file) : NULL;
  next.client_key_file = config->client_key_file ? tstr_dup(config->client_key_file) : NULL;
  next.client_key_password_ref =
      config->client_key_password_ref ? tstr_dup(config->client_key_password_ref) : NULL;
  if ((config->ca_file && !next.ca_file) || (config->client_cert_file && !next.client_cert_file) ||
      (config->client_key_file && !next.client_key_file) ||
      (config->client_key_password_ref && !next.client_key_password_ref)) {
    external_https_tls_cleanup(&next);
    return TURBO_ENOMEM;
  }
  *tls = next;
  return TURBO_OK;
}

static int external_https_secret_valid(const turbo_flow_security_secret_lease_t *lease) {
  return lease && lease->bytes && lease->byte_count > 0u &&
         lease->byte_count <= FLOWIE_CONTROL_EXTERNAL_HTTPS_TOKEN_MAX &&
         !memchr(lease->bytes, '\0', lease->byte_count) &&
         !memchr(lease->bytes, '\r', lease->byte_count) &&
         !memchr(lease->bytes, '\n', lease->byte_count);
}

static int external_https_tls_apply(const flowie_control_external_https_tls_t *tls,
                                    const turbo_flow_security_key_provider_t *key_provider,
                                    http_client_t *client) {
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  turbo_tls_client_config_t config = {0};
  char *password = NULL;
  int rc = TURBO_OK;
  if (!tls || !key_provider || !client) return TURBO_EINVAL;
  if (!tls->ca_file && !tls->client_cert_file) return TURBO_OK;
  if (tls->client_key_password_ref) {
    rc = turbo_flow_security_secret_acquire(key_provider, tls->client_key_password_ref, &lease);
    if (rc != TURBO_OK) goto done;
    if (!external_https_secret_valid(&lease)) {
      rc = TURBO_EPERM;
      goto done;
    }
    password = (char *)malloc(lease.byte_count + 1u);
    if (!password) {
      rc = TURBO_ENOMEM;
      goto done;
    }
    memcpy(password, lease.bytes, lease.byte_count);
    password[lease.byte_count] = '\0';
  }
  config.ca_file = tls->ca_file;
  config.cert_file = tls->client_cert_file;
  config.key_file = tls->client_key_file;
  config.key_password = password;
  config.verify_peer = 1;
  if (http_client_set_tls_client_config(client, &config) != 0) rc = TURBO_EIO;

done:
  if (password) {
    crypto_wipe(password, lease.byte_count);
    free(password);
  }
  turbo_flow_security_secret_release(key_provider, &lease);
  return rc;
}

static int
external_https_tls_files_validate(const flowie_control_external_https_tls_t *tls,
                                  const turbo_flow_security_key_provider_t *key_provider) {
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  SSL_CTX *context = NULL;
  char *password = NULL;
  int rc = TURBO_EIO;
  if (!tls || !key_provider) return TURBO_EINVAL;
  if (!tls->ca_file && !tls->client_cert_file) return TURBO_OK;
  context = SSL_CTX_new(TLS_client_method());
  if (!context) goto done;
  if (tls->ca_file && SSL_CTX_load_verify_locations(context, tls->ca_file, NULL) != 1) goto done;
  if (!tls->client_cert_file) {
    rc = TURBO_OK;
    goto done;
  }
  if (tls->client_key_password_ref) {
    rc = turbo_flow_security_secret_acquire(key_provider, tls->client_key_password_ref, &lease);
    if (rc != TURBO_OK) goto done;
    if (!external_https_secret_valid(&lease)) {
      rc = TURBO_EPERM;
      goto done;
    }
    password = (char *)malloc(lease.byte_count + 1u);
    if (!password) {
      rc = TURBO_ENOMEM;
      goto done;
    }
    memcpy(password, lease.bytes, lease.byte_count);
    password[lease.byte_count] = '\0';
    SSL_CTX_set_default_passwd_cb_userdata(context, password);
  }
  rc = TURBO_EIO;
  if (SSL_CTX_use_certificate_chain_file(context, tls->client_cert_file) != 1 ||
      SSL_CTX_use_PrivateKey_file(context, tls->client_key_file, SSL_FILETYPE_PEM) != 1 ||
      SSL_CTX_check_private_key(context) != 1)
    goto done;
  rc = TURBO_OK;

done:
  SSL_CTX_free(context);
  if (password) {
    crypto_wipe(password, lease.byte_count);
    free(password);
  }
  turbo_flow_security_secret_release(key_provider, &lease);
  return rc;
}

static int external_https_verify(void *ctx, const flowie_control_external_auth_request_t *request,
                                 flowie_control_external_auth_assertion_t *assertion_out) {
  flowie_control_external_https_authenticator_t *authenticator =
      (flowie_control_external_https_authenticator_t *)ctx;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  char *authorization = NULL;
  char *body = NULL;
  size_t body_size = 0u;
  size_t token_size = 0u;
  const char *headers[3];
  unsigned int in_flight;
  external_https_outcome_t outcome = EXTERNAL_HTTPS_OUTCOME_LOCAL_FAILURE;
  int admitted = 0;
  int rc = TURBO_EIO;
  if (assertion_out && assertion_out->size >= sizeof(*assertion_out))
    *assertion_out =
        (flowie_control_external_auth_assertion_t)FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT;
  if (!authenticator || !external_https_request_valid(request) || !assertion_out ||
      assertion_out->size < sizeof(*assertion_out) ||
      strcmp(request->method, authenticator->method) != 0)
    return TURBO_EINVAL;
  if (!coro_running() || !coro_context_current()) return TURBO_ENOTSUP;
  external_https_counter_increment(&authenticator->started_requests);
  in_flight = atomic_load_explicit(&authenticator->in_flight, memory_order_relaxed);
  do {
    if (in_flight >= authenticator->max_in_flight) {
      external_https_record_outcome(authenticator, EXTERNAL_HTTPS_OUTCOME_LOCAL_OVERLOAD);
      return TURBO_EBUSY;
    }
  } while (!atomic_compare_exchange_weak_explicit(&authenticator->in_flight, &in_flight,
                                                  in_flight + 1u, memory_order_acq_rel,
                                                  memory_order_relaxed));
  admitted = 1;
  rc = turbo_flow_security_secret_acquire(&authenticator->key_provider,
                                          authenticator->service_token_ref, &lease);
  if (rc != TURBO_OK) goto done;
  token_size = lease.byte_count;
  if (!external_https_secret_valid(&lease)) {
    rc = TURBO_EPERM;
    goto done;
  }
  authorization = (char *)malloc(sizeof("Authorization: Bearer ") + token_size);
  if (!authorization) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  memcpy(authorization, "Authorization: Bearer ", sizeof("Authorization: Bearer ") - 1u);
  memcpy(authorization + sizeof("Authorization: Bearer ") - 1u, lease.bytes, token_size);
  authorization[sizeof("Authorization: Bearer ") - 1u + token_size] = '\0';
  rc = flowie_control_external_https_encode_request(request, &body, &body_size);
  if (rc != TURBO_OK) goto done;
  client = http_client_create(authenticator->url);
  if (!client) {
    rc = TURBO_EIO;
    goto done;
  }
  http_client_set_timeout(client, (int)authenticator->timeout_ms);
  http_client_set_connect_timeout(client, (int)authenticator->timeout_ms);
  http_client_set_read_timeout(client, (int)authenticator->timeout_ms);
  http_client_set_max_response_size(client, authenticator->max_response_size);
  http_client_set_max_response_header_size(client,
                                           FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_RESPONSE_SIZE);
  http_client_follow_redirects(client, 0);
  http_client_clear_retry_policy(client);
  rc = external_https_tls_apply(&authenticator->tls, &authenticator->key_provider, client);
  if (rc != TURBO_OK) goto done;
  if (http_client_set_connect_policy(client, external_https_connect_policy, authenticator) != 0) {
    rc = TURBO_EIO;
    goto done;
  }
  headers[0] = "Content-Type: application/json";
  headers[1] = "Accept: application/json";
  headers[2] = authorization;
  response = http_request(client, HTTP_POST, authenticator->url, headers, 3, body, body_size);
  if (!response) {
    outcome = EXTERNAL_HTTPS_OUTCOME_TRANSPORT_FAILURE;
    rc = TURBO_EIO;
    goto done;
  }
  if (response->status_code <= 0) {
    outcome = EXTERNAL_HTTPS_OUTCOME_TRANSPORT_FAILURE;
    rc = TURBO_EIO;
    goto done;
  }
  if (response->status_code == 401 || response->status_code == 403) {
    outcome = EXTERNAL_HTTPS_OUTCOME_DENIED;
    rc = TURBO_EPERM;
    goto done;
  }
  if (response->status_code == 429) {
    outcome = EXTERNAL_HTTPS_OUTCOME_REMOTE_OVERLOAD;
    rc = TURBO_EBUSY;
    goto done;
  }
  if (response->error_code != HTTP_ERROR_NONE) {
    outcome = EXTERNAL_HTTPS_OUTCOME_TRANSPORT_FAILURE;
    rc = TURBO_EIO;
    goto done;
  }
  if (response->status_code >= 500 && response->status_code <= 599) {
    outcome = EXTERNAL_HTTPS_OUTCOME_REMOTE_SERVER_FAILURE;
    rc = TURBO_EIO;
    goto done;
  }
  if (response->status_code != 200 ||
      !external_https_content_type_json(response->headers, response->headers_len)) {
    outcome = EXTERNAL_HTTPS_OUTCOME_PROTOCOL_FAILURE;
    rc = TURBO_EIO;
    goto done;
  }
  if (!response->body || response->body_len == 0u) {
    outcome = EXTERNAL_HTTPS_OUTCOME_PROTOCOL_FAILURE;
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_control_external_https_decode_response(response->body, response->body_len,
                                                     authenticator->method, assertion_out);
  if (rc == TURBO_OK) outcome = EXTERNAL_HTTPS_OUTCOME_SUCCEEDED;
  else if (rc == TURBO_EPERM) outcome = EXTERNAL_HTTPS_OUTCOME_DENIED;
  else outcome = EXTERNAL_HTTPS_OUTCOME_PROTOCOL_FAILURE;

done:
  if (response) http_response_free(response);
  if (client) http_client_destroy(client);
  if (body) {
    crypto_wipe(body, body_size);
    turbo_json_serialize_free(body);
  }
  if (authorization) {
    crypto_wipe(authorization, sizeof("Authorization: Bearer ") + token_size);
    free(authorization);
  }
  turbo_flow_security_secret_release(&authenticator->key_provider, &lease);
  if (admitted)
    (void)atomic_fetch_sub_explicit(&authenticator->in_flight, 1u, memory_order_release);
  external_https_record_outcome(authenticator, outcome);
  return rc;
}

int flowie_control_external_https_authenticator_create(
    const flowie_control_external_https_authenticator_config_t *config,
    flowie_control_external_https_authenticator_t **out) {
  flowie_control_external_https_authenticator_t *authenticator = NULL;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out ||
      !external_https_text_valid(config->url, FLOWIE_CONTROL_EXTERNAL_HTTPS_URL_MAX, 1) ||
      !external_https_text_valid(config->method, TURBO_FLOW_SECURITY_TYPE_MAX, 1) ||
      !external_https_text_valid(config->service_token_ref,
                                 FLOWIE_CONTROL_EXTERNAL_HTTPS_TLS_PATH_MAX, 1) ||
      config->timeout_ms == 0u ||
      config->timeout_ms > FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_TIMEOUT_MS ||
      config->max_response_size == 0u ||
      config->max_response_size > FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_RESPONSE_SIZE ||
      config->max_in_flight == 0u ||
      config->max_in_flight > FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_IN_FLIGHT ||
      config->key_provider.size < sizeof(config->key_provider) || !config->key_provider.acquire ||
      !config->key_provider.release || !external_https_tls_config_valid(&config->tls))
    return TURBO_EINVAL;
  authenticator =
      (flowie_control_external_https_authenticator_t *)calloc(1u, sizeof(*authenticator));
  if (!authenticator) return TURBO_ENOMEM;
  authenticator->url = config->url ? tstr_dup(config->url) : NULL;
  authenticator->method = tstr_dup(config->method);
  authenticator->service_token_ref = tstr_dup(config->service_token_ref);
  authenticator->timeout_ms = config->timeout_ms;
  authenticator->max_response_size = config->max_response_size;
  authenticator->max_in_flight = config->max_in_flight;
  atomic_init(&authenticator->in_flight, 0u);
  atomic_init(&authenticator->started_requests, 0u);
  atomic_init(&authenticator->succeeded, 0u);
  atomic_init(&authenticator->denied, 0u);
  atomic_init(&authenticator->local_overload, 0u);
  atomic_init(&authenticator->remote_overload, 0u);
  atomic_init(&authenticator->remote_server_failures, 0u);
  atomic_init(&authenticator->transport_failures, 0u);
  atomic_init(&authenticator->protocol_failures, 0u);
  atomic_init(&authenticator->local_failures, 0u);
  authenticator->key_provider = config->key_provider;
  if (!authenticator->url || !authenticator->method || !authenticator->service_token_ref) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = external_https_tls_init(&authenticator->tls, &config->tls);
  if (rc != TURBO_OK) goto fail;
  rc = external_https_validate_url(authenticator->url, &authenticator->host, &authenticator->port);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_flow_security_secret_acquire(&authenticator->key_provider,
                                          authenticator->service_token_ref, &lease);
  if (rc != TURBO_OK) goto fail;
  if (!external_https_secret_valid(&lease)) {
    rc = TURBO_EPERM;
    goto fail;
  }
  turbo_flow_security_secret_release(&authenticator->key_provider, &lease);
  rc = external_https_tls_files_validate(&authenticator->tls, &authenticator->key_provider);
  if (rc != TURBO_OK) goto fail;
  authenticator->interface =
      (flowie_control_external_authenticator_t)FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_INIT;
  authenticator->interface.capabilities = FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES |
                                          FLOWIE_CONTROL_EXTERNAL_AUTH_GROUP_CLAIMS;
  authenticator->interface.ctx = authenticator;
  authenticator->interface.method = authenticator->method;
  authenticator->interface.verify = external_https_verify;
  *out = authenticator;
  return TURBO_OK;

fail:
  turbo_flow_security_secret_release(&authenticator->key_provider, &lease);
  flowie_control_external_https_authenticator_destroy(authenticator);
  return rc;
}

void flowie_control_external_https_authenticator_destroy(
    flowie_control_external_https_authenticator_t *authenticator) {
  if (!authenticator) return;
  tstr_freep(&authenticator->url);
  tstr_freep(&authenticator->host);
  tstr_freep(&authenticator->method);
  tstr_freep(&authenticator->service_token_ref);
  external_https_tls_cleanup(&authenticator->tls);
  crypto_wipe(&authenticator->key_provider, sizeof(authenticator->key_provider));
  crypto_wipe(&authenticator->interface, sizeof(authenticator->interface));
  free(authenticator);
}

const flowie_control_external_authenticator_t *
flowie_control_external_https_authenticator_interface(
    const flowie_control_external_https_authenticator_t *authenticator) {
  return authenticator ? &authenticator->interface : NULL;
}

int flowie_control_external_https_authenticator_get_stats(
    const flowie_control_external_https_authenticator_t *authenticator,
    flowie_control_external_https_authenticator_stats_t *stats_out) {
  flowie_control_external_https_authenticator_stats_t stats =
      FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;
  if (stats_out && stats_out->size >= sizeof(*stats_out)) *stats_out = stats;
  if (!authenticator || !stats_out || stats_out->size < sizeof(*stats_out)) return TURBO_EINVAL;
  stats.started_requests =
      (uint64_t)atomic_load_explicit(&authenticator->started_requests, memory_order_relaxed);
  stats.in_flight = (uint64_t)atomic_load_explicit(&authenticator->in_flight, memory_order_relaxed);
  stats.succeeded = (uint64_t)atomic_load_explicit(&authenticator->succeeded, memory_order_relaxed);
  stats.denied = (uint64_t)atomic_load_explicit(&authenticator->denied, memory_order_relaxed);
  stats.local_overload =
      (uint64_t)atomic_load_explicit(&authenticator->local_overload, memory_order_relaxed);
  stats.remote_overload =
      (uint64_t)atomic_load_explicit(&authenticator->remote_overload, memory_order_relaxed);
  stats.remote_server_failures =
      (uint64_t)atomic_load_explicit(&authenticator->remote_server_failures, memory_order_relaxed);
  stats.transport_failures =
      (uint64_t)atomic_load_explicit(&authenticator->transport_failures, memory_order_relaxed);
  stats.protocol_failures =
      (uint64_t)atomic_load_explicit(&authenticator->protocol_failures, memory_order_relaxed);
  stats.local_failures =
      (uint64_t)atomic_load_explicit(&authenticator->local_failures, memory_order_relaxed);
  *stats_out = stats;
  return TURBO_OK;
}
