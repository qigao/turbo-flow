#include "flow_http_tls_client.h"
#include "turbo_flow_http_auth.h"

#include "flow_http_auth_internal.h"

#include "CoroNet/turbo_coro_context.h"
#include "base64_utils.h"
#include "http_client.h"
#include "monocypher.h"
#include "turbo_coro.h"
#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_HTTP_AUTH_PROTOCOL_VERSION 3u
#define FLOW_HTTP_AUTH_REMOTE_ADDRESS_MAX 255u

struct turbo_flow_http_auth_provider_s {
  turbo_flow_security_auth_provider_t interface;
  turbo_flow_security_enhanced_auth_provider_t enhanced_interface;
  tstr_t url;
  tstr_t host;
  tstr_t method;
  tstr_t service_token_ref;
  uint16_t port;
  uint32_t timeout_ms;
  size_t max_secret_size;
  turbo_flow_security_key_provider_t key_provider;
  flow_http_tls_client_t tls;
};

static int flow_http_enhanced_auth_begin(void *ctx,
                                         const turbo_flow_security_enhanced_auth_request_t *request,
                                         void **exchange_out,
                                         turbo_flow_security_enhanced_auth_result_t *result_out);
static int
flow_http_enhanced_auth_continue(void *ctx, void *exchange,
                                 const turbo_flow_security_enhanced_auth_request_t *request,
                                 turbo_flow_security_enhanced_auth_result_t *result_out);
static void flow_http_enhanced_auth_cancel(void *ctx, void *exchange);

static int flow_http_auth_config_error(turbo_flow_config_error_t *error, int status,
                                       const char *name, const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", name, field);
    else (void)snprintf(error->path, sizeof(error->path), "$.channels.%s", name ? name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_http_auth_ascii_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
  while (*left && *right) {
    if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) return 0;
    ++left;
    ++right;
  }
  return *left == '\0' && *right == '\0';
}

static int flow_http_auth_ascii_prefix(const char *value, size_t value_size, const char *prefix) {
  size_t prefix_size = strlen(prefix);
  if (!value || value_size < prefix_size) return 0;
  for (size_t i = 0u; i < prefix_size; ++i)
    if (tolower((unsigned char)value[i]) != tolower((unsigned char)prefix[i])) return 0;
  return 1;
}

static int flow_http_auth_connect_policy(const char *scheme, const char *hostname, uint16_t port,
                                         const turbo_dns_result_t *results, size_t result_count,
                                         void *user_data) {
  const turbo_flow_http_auth_provider_t *provider =
      (const turbo_flow_http_auth_provider_t *)user_data;
  (void)results;
  if (!provider || !scheme || strcmp(scheme, "https") != 0 || !hostname ||
      !flow_http_auth_ascii_equal(hostname, provider->host) || port != provider->port ||
      result_count == 0u)
    return -1;
  return 0;
}

static int flow_http_auth_string_valid(const char *value, size_t maximum, int required) {
  size_t size;
  if (!value) return !required;
  size = strnlen(value, maximum + 1u);
  return size <= maximum && (!required || size > 0u);
}

static int flow_http_auth_certificate_fingerprint_valid(const char *value) {
  static const char prefix[] = "sha256:";
  size_t size;
  if (!value) return 1;
  size = strlen(value);
  if (size != sizeof(prefix) - 1u + 64u || memcmp(value, prefix, sizeof(prefix) - 1u) != 0)
    return 0;
  for (size_t i = sizeof(prefix) - 1u; i < size; ++i)
    if (!((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f'))) return 0;
  return 1;
}

static const char *
flow_http_auth_peer_certificate(const turbo_flow_security_auth_request_t *request) {
  return request && request->size >= sizeof(*request) ? request->peer_certificate_sha256 : NULL;
}

static int flow_http_auth_json_fields(const json_value_t *object, const char *const *allowed,
                                      size_t allowed_count) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT) return TURBO_EPROTO;
  for (size_t i = 0u; i < turbo_json_object_size(object); ++i) {
    const char *field = turbo_json_object_key(object, i);
    int known = 0;
    for (size_t j = 0u; j < allowed_count; ++j) {
      if (field && strcmp(field, allowed[j]) == 0) {
        known = 1;
        break;
      }
    }
    if (!known) return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_http_auth_json_u64(const json_value_t *value, uint64_t *out) {
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
  parsed = strtoull(buffer, &end, 10);
  if (!end || *end != '\0') return TURBO_EPROTO;
  *out = (uint64_t)parsed;
  return TURBO_OK;
}

static int flow_http_auth_copy_json_string(const json_value_t *object, const char *field, char *out,
                                           size_t capacity, int required) {
  json_value_t *value;
  const char *text;
  size_t size;
  if (!object || !field || !out || capacity == 0u) return TURBO_EPROTO;
  value = turbo_json_object_get(object, field);
  if (!value) {
    out[0] = '\0';
    return required ? TURBO_EPROTO : TURBO_OK;
  }
  if (turbo_json_type(value) != TURBO_JSON_STRING) return TURBO_EPROTO;
  text = turbo_json_string(value);
  size = turbo_json_string_len(value);
  if (!text || size >= capacity || (required && size == 0u) || memchr(text, '\0', size))
    return TURBO_EPROTO;
  memcpy(out, text, size);
  out[size] = '\0';
  return TURBO_OK;
}

static int flow_http_auth_scope(const char *text, turbo_flow_security_scope_t *scope) {
  if (!text || !scope) return TURBO_EPROTO;
  if (strcmp(text, "self") == 0) *scope = TURBO_FLOW_SECURITY_SCOPE_SELF;
  else if (strcmp(text, "group") == 0) *scope = TURBO_FLOW_SECURITY_SCOPE_GROUP;
  else if (strcmp(text, "root_group") == 0) *scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  else if (strcmp(text, "system") == 0) *scope = TURBO_FLOW_SECURITY_SCOPE_SYSTEM;
  else return TURBO_EPROTO;
  return TURBO_OK;
}

static int flow_http_auth_string_array(const json_value_t *object, const char *field, char *values,
                                       size_t stride, uint32_t maximum, size_t value_maximum,
                                       uint32_t *count_out) {
  json_value_t *array = turbo_json_object_get(object, field);
  size_t count;
  if (!array || turbo_json_type(array) != TURBO_JSON_ARRAY || !values || !count_out)
    return TURBO_EPROTO;
  count = turbo_json_array_size(array);
  if (count > maximum) return TURBO_EPROTO;
  for (size_t i = 0u; i < count; ++i) {
    json_value_t *entry = turbo_json_array_get(array, i);
    const char *text;
    size_t size;
    if (!entry || turbo_json_type(entry) != TURBO_JSON_STRING) return TURBO_EPROTO;
    text = turbo_json_string(entry);
    size = turbo_json_string_len(entry);
    if (!text || size == 0u || size > value_maximum || memchr(text, '\0', size))
      return TURBO_EPROTO;
    memcpy(values + i * stride, text, size);
    values[i * stride + size] = '\0';
  }
  *count_out = (uint32_t)count;
  return TURBO_OK;
}

static int flow_http_auth_effective_groups_valid(const turbo_flow_security_principal_t *principal) {
  int contains_root = 0;
  if (!principal) return 0;
  for (uint32_t i = 0u; i < principal->group_count; ++i) {
    if (strcmp(principal->groups[i], principal->root_group_id) == 0) contains_root = 1;
    for (uint32_t j = 0u; j < i; ++j)
      if (strcmp(principal->groups[i], principal->groups[j]) == 0) return 0;
  }
  return principal->scope == TURBO_FLOW_SECURITY_SCOPE_SYSTEM ||
         (principal->root_group_id[0] != '\0' && principal->group_count != 0u && contains_root);
}

int flow_http_auth_decode_response(const char *body, size_t body_size, const char *method,
                                   turbo_flow_security_principal_t *principal_out) {
  static const char *const outer_allowed[] = {"version", "authenticated", "principal"};
  static const char *const principal_allowed[] = {"id",          "type",       "root_group",
                                                  "auth_method", "scope",      "roles",
                                                  "groups",      "expires_at", "policy_version"};
  turbo_json_doc_t *document = NULL;
  json_value_t *authenticated;
  json_value_t *principal;
  json_value_t *scope;
  json_value_t *expires;
  json_value_t *policy;
  uint64_t version = 0u;
  int rc = TURBO_EPROTO;
  if (!body || body_size == 0u || body_size > TURBO_FLOW_HTTP_AUTH_RESPONSE_LIMIT || !method ||
      !principal_out)
    return TURBO_EPROTO;
  if (turbo_parse_json((const uint8_t *)body, body_size, &document) != TURBO_OK || !document)
    return TURBO_EPROTO;
  if (flow_http_auth_json_fields(document, outer_allowed,
                                 sizeof(outer_allowed) / sizeof(outer_allowed[0])) != TURBO_OK ||
      flow_http_auth_json_u64(turbo_json_object_get(document, "version"), &version) != TURBO_OK ||
      version != FLOW_HTTP_AUTH_PROTOCOL_VERSION)
    goto done;
  authenticated = turbo_json_object_get(document, "authenticated");
  if (!authenticated || turbo_json_type(authenticated) != TURBO_JSON_BOOL) goto done;
  if (!turbo_json_bool(authenticated)) {
    rc = TURBO_EPERM;
    goto done;
  }
  principal = turbo_json_object_get(document, "principal");
  if (flow_http_auth_json_fields(principal, principal_allowed,
                                 sizeof(principal_allowed) / sizeof(principal_allowed[0])) !=
      TURBO_OK)
    goto done;
  *principal_out = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  if (flow_http_auth_copy_json_string(principal, "id", principal_out->principal_id,
                                      sizeof(principal_out->principal_id), 1) != TURBO_OK ||
      flow_http_auth_copy_json_string(principal, "type", principal_out->principal_type,
                                      sizeof(principal_out->principal_type), 1) != TURBO_OK ||
      flow_http_auth_copy_json_string(principal, "root_group", principal_out->root_group_id,
                                      sizeof(principal_out->root_group_id), 0) != TURBO_OK ||
      flow_http_auth_copy_json_string(principal, "auth_method", principal_out->auth_method,
                                      sizeof(principal_out->auth_method), 1) != TURBO_OK ||
      strcmp(principal_out->auth_method, method) != 0)
    goto done;
  scope = turbo_json_object_get(principal, "scope");
  if (!scope || turbo_json_type(scope) != TURBO_JSON_STRING ||
      flow_http_auth_scope(turbo_json_string(scope), &principal_out->scope) != TURBO_OK)
    goto done;
  if (flow_http_auth_string_array(principal, "roles", (char *)principal_out->roles,
                                  sizeof(principal_out->roles[0]), TURBO_FLOW_SECURITY_MAX_ROLES,
                                  TURBO_FLOW_SECURITY_TYPE_MAX,
                                  &principal_out->role_count) != TURBO_OK ||
      flow_http_auth_string_array(principal, "groups", (char *)principal_out->groups,
                                  sizeof(principal_out->groups[0]), TURBO_FLOW_SECURITY_MAX_GROUPS,
                                  TURBO_FLOW_SECURITY_ID_MAX,
                                  &principal_out->group_count) != TURBO_OK)
    goto done;
  expires = turbo_json_object_get(principal, "expires_at");
  policy = turbo_json_object_get(principal, "policy_version");
  if (flow_http_auth_json_u64(expires, &principal_out->expires_at) != TURBO_OK ||
      flow_http_auth_json_u64(policy, &principal_out->policy_version) != TURBO_OK ||
      principal_out->policy_version == 0u || !flow_http_auth_effective_groups_valid(principal_out))
    goto done;
  rc = TURBO_OK;

done:
  if (rc != TURBO_OK)
    *principal_out = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  turbo_free_json(&document);
  return rc;
}

static int flow_http_auth_add(json_value_t *object, const char *field, json_value_t *value) {
  if (value && turbo_json_object_add_checked(object, field, value)) return TURBO_OK;
  if (value) {
    turbo_json_doc_t *owned = (turbo_json_doc_t *)value;
    turbo_free_json(&owned);
  }
  return TURBO_ENOMEM;
}

static int flow_http_auth_encode_request(const turbo_flow_security_auth_request_t *request,
                                         char **body_out, size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  char *secret_base64 = NULL;
  const char *peer_certificate_sha256;
  int rc = TURBO_ENOMEM;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!request || request->size < TURBO_FLOW_SECURITY_AUTH_REQUEST_BASE_SIZE || !body_out ||
      !body_size_out || !request->identity || !request->method || !request->secret)
    return TURBO_EINVAL;
  peer_certificate_sha256 = flow_http_auth_peer_certificate(request);
  if (!flow_http_auth_certificate_fingerprint_valid(peer_certificate_sha256)) return TURBO_EPERM;
  if (tn_base64_encode(request->secret, request->secret_size, &secret_base64) != 0 ||
      !secret_base64)
    return TURBO_ENOMEM;
  document = (turbo_json_doc_t *)turbo_json_create_object();
  if (!document) goto done;
  if (flow_http_auth_add(document, "version",
                         turbo_json_create_uint64(FLOW_HTTP_AUTH_PROTOCOL_VERSION)) != TURBO_OK ||
      flow_http_auth_add(document, "identity", turbo_json_create_string(request->identity)) !=
          TURBO_OK ||
      flow_http_auth_add(document, "method", turbo_json_create_string(request->method)) !=
          TURBO_OK ||
      flow_http_auth_add(document, "secret_base64", turbo_json_create_string(secret_base64)) !=
          TURBO_OK ||
      flow_http_auth_add(document, "protocol",
                         turbo_json_create_string(request->protocol ? request->protocol : "")) !=
          TURBO_OK ||
      flow_http_auth_add(document, "remote_address",
                         turbo_json_create_string(request->remote_address ? request->remote_address
                                                                          : "")) != TURBO_OK ||
      flow_http_auth_add(document, "peer_certificate_sha256",
                         turbo_json_create_string(peer_certificate_sha256 ? peer_certificate_sha256
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

static int flow_http_auth_content_type_json(const char *headers, size_t headers_size) {
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
      size_t i;
      for (i = 0u; i < sizeof(name) - 1u; ++i) {
        if (tolower((unsigned char)headers[cursor + i]) != name[i]) break;
      }
      if (i == sizeof(name) - 1u) {
        cursor += sizeof(name) - 1u;
        while (cursor < line_end && (headers[cursor] == ' ' || headers[cursor] == '\t'))
          ++cursor;
        if (flow_http_auth_ascii_prefix(headers + cursor, line_end - cursor, "application/json") &&
            (cursor + 16u == line_end || headers[cursor + 16u] == ';' ||
             headers[cursor + 16u] == '\r')) {
          ++matches;
        } else {
          return 0;
        }
      }
    }
    line_start = line_end + 1u;
  }
  return matches == 1;
}

static int flow_http_authenticate(void *ctx, const turbo_flow_security_auth_request_t *request,
                                  turbo_flow_security_principal_t *principal_out) {
  turbo_flow_http_auth_provider_t *provider = (turbo_flow_http_auth_provider_t *)ctx;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  char *authorization = NULL;
  char *body = NULL;
  size_t body_size = 0u;
  const char *headers[3];
  const char *peer_certificate_sha256;
  size_t identity_size;
  size_t token_size;
  int rc = TURBO_EIO;
  if (!provider || !request || request->size < TURBO_FLOW_SECURITY_AUTH_REQUEST_BASE_SIZE ||
      !principal_out || principal_out->size < sizeof(*principal_out) || !request->identity ||
      !request->method || !request->secret)
    return TURBO_EINVAL;
  *principal_out = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  if (!coro_running() || !coro_context_current()) return TURBO_ENOTSUP;
  peer_certificate_sha256 = flow_http_auth_peer_certificate(request);
  identity_size = strnlen(request->identity, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  if (identity_size == 0u || identity_size > TURBO_FLOW_SECURITY_ID_MAX ||
      strcmp(request->method, provider->method) != 0 || request->secret_size == 0u ||
      request->secret_size > provider->max_secret_size ||
      !flow_http_auth_string_valid(request->protocol, TURBO_FLOW_SECURITY_TYPE_MAX, 0) ||
      !flow_http_auth_string_valid(request->remote_address, FLOW_HTTP_AUTH_REMOTE_ADDRESS_MAX, 0) ||
      !flow_http_auth_certificate_fingerprint_valid(peer_certificate_sha256))
    return TURBO_EPERM;
  rc = turbo_flow_security_secret_acquire(&provider->key_provider, provider->service_token_ref,
                                          &lease);
  if (rc != TURBO_OK) return rc;
  token_size = lease.byte_count;
  if (!lease.bytes || token_size == 0u || token_size > TURBO_FLOW_HTTP_AUTH_TOKEN_LIMIT ||
      memchr(lease.bytes, '\0', token_size) || memchr(lease.bytes, '\r', token_size) ||
      memchr(lease.bytes, '\n', token_size)) {
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
  rc = flow_http_auth_encode_request(request, &body, &body_size);
  if (rc != TURBO_OK) goto done;
  client = http_client_create(provider->url);
  if (!client) {
    rc = TURBO_EIO;
    goto done;
  }
  http_client_set_timeout(client, (int)provider->timeout_ms);
  http_client_set_connect_timeout(client, (int)provider->timeout_ms);
  http_client_set_read_timeout(client, (int)provider->timeout_ms);
  http_client_set_max_response_size(client, TURBO_FLOW_HTTP_AUTH_RESPONSE_LIMIT);
  http_client_set_max_response_header_size(client, TURBO_FLOW_HTTP_AUTH_RESPONSE_LIMIT);
  http_client_follow_redirects(client, 0);
  http_client_clear_retry_policy(client);
  rc = flow_http_tls_client_apply(&provider->tls, &provider->key_provider, client);
  if (rc != TURBO_OK) goto done;
  if (http_client_set_connect_policy(client, flow_http_auth_connect_policy, provider) != 0) {
    rc = TURBO_EIO;
    goto done;
  }
  headers[0] = "Content-Type: application/json";
  headers[1] = "Accept: application/json";
  headers[2] = authorization;
  response = http_request(client, HTTP_POST, provider->url, headers, 3, body, body_size);
  if (!response) {
    rc = TURBO_EIO;
    goto done;
  }
  if (response->status_code == 401 || response->status_code == 403) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (response->status_code == 429) {
    rc = TURBO_EBUSY;
    goto done;
  }
  if (response->status_code != 200 || response->error_code != HTTP_ERROR_NONE ||
      !flow_http_auth_content_type_json(response->headers, response->headers_len)) {
    rc = TURBO_EIO;
    goto done;
  }
  rc = flow_http_auth_decode_response(response->body, response->body_len, provider->method,
                                      principal_out);

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
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  return rc;
}

static int flow_http_enhanced_auth_begin(void *ctx,
                                         const turbo_flow_security_enhanced_auth_request_t *request,
                                         void **exchange_out,
                                         turbo_flow_security_enhanced_auth_result_t *result_out) {
  turbo_flow_security_auth_request_t basic = TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
  int rc;
  if (!request || !exchange_out || !result_out) return TURBO_EINVAL;
  *exchange_out = NULL;
  basic.identity = request->identity;
  basic.method = request->method;
  basic.secret = request->data;
  basic.secret_size = request->data_size;
  basic.remote_address = request->remote_address;
  basic.protocol = request->protocol;
  basic.peer_certificate_sha256 =
      request->size >= sizeof(*request) ? request->peer_certificate_sha256 : NULL;
  rc = flow_http_authenticate(ctx, &basic, &result_out->principal);
  if (rc != TURBO_OK) return rc;
  result_out->status = TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS;
  return TURBO_OK;
}

static int
flow_http_enhanced_auth_continue(void *ctx, void *exchange,
                                 const turbo_flow_security_enhanced_auth_request_t *request,
                                 turbo_flow_security_enhanced_auth_result_t *result_out) {
  (void)ctx;
  (void)exchange;
  (void)request;
  (void)result_out;
  return TURBO_ENOTSUP;
}

static void flow_http_enhanced_auth_cancel(void *ctx, void *exchange) {
  (void)ctx;
  (void)exchange;
}

static int flow_http_auth_validate_url(const char *url, tstr_t *host_out, uint16_t *port_out) {
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

int turbo_flow_http_auth_provider_create(const turbo_flow_http_auth_provider_config_t *config,
                                         turbo_flow_http_auth_provider_t **out) {
  const turbo_flow_http_tls_client_config_t *tls_config = NULL;
  turbo_flow_http_auth_provider_t *provider = NULL;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  size_t secret_limit;
  int rc;
  if (out) *out = NULL;
  if (!config ||
      !((config->api_version == TURBO_FLOW_HTTP_AUTH_API_VERSION_V1 &&
         config->size == offsetof(turbo_flow_http_auth_provider_config_t, tls)) ||
        (config->api_version == TURBO_FLOW_HTTP_AUTH_API_VERSION_V2 &&
         config->size >= sizeof(*config))) ||
      !out || !config->url || !config->method ||
      !flow_http_auth_string_valid(config->method, TURBO_FLOW_SECURITY_TYPE_MAX, 1) ||
      !config->service_token_ref || !config->service_token_ref[0] || config->timeout_ms == 0u ||
      config->timeout_ms > TURBO_FLOW_HTTP_AUTH_MAX_TIMEOUT_MS ||
      config->key_provider.size < sizeof(config->key_provider) || !config->key_provider.acquire ||
      !config->key_provider.release)
    return TURBO_EINVAL;
  if (config->api_version == TURBO_FLOW_HTTP_AUTH_API_VERSION_V2) tls_config = &config->tls;
  secret_limit = config->max_secret_size ? config->max_secret_size
                                         : TURBO_FLOW_HTTP_AUTH_DEFAULT_MAX_SECRET_SIZE;
  if (secret_limit == 0u || secret_limit > TURBO_FLOW_HTTP_AUTH_DEFAULT_MAX_SECRET_SIZE)
    return TURBO_ERANGE;
  provider = (turbo_flow_http_auth_provider_t *)calloc(1u, sizeof(*provider));
  if (!provider) return TURBO_ENOMEM;
  provider->url = tstr_dup(config->url);
  provider->method = tstr_dup(config->method);
  provider->service_token_ref = tstr_dup(config->service_token_ref);
  provider->timeout_ms = config->timeout_ms;
  provider->max_secret_size = secret_limit;
  provider->key_provider = config->key_provider;
  if (!provider->url || !provider->method || !provider->service_token_ref) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = flow_http_tls_client_init(&provider->tls, tls_config);
  if (rc != TURBO_OK) goto fail;
  rc = flow_http_auth_validate_url(provider->url, &provider->host, &provider->port);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_flow_security_secret_acquire(&provider->key_provider, provider->service_token_ref,
                                          &lease);
  if (rc != TURBO_OK) goto fail;
  if (!lease.bytes || lease.byte_count == 0u ||
      lease.byte_count > TURBO_FLOW_HTTP_AUTH_TOKEN_LIMIT ||
      memchr(lease.bytes, '\0', lease.byte_count) || memchr(lease.bytes, '\r', lease.byte_count) ||
      memchr(lease.bytes, '\n', lease.byte_count)) {
    rc = TURBO_EPERM;
    goto fail;
  }
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  rc = flow_http_tls_client_probe_secret(&provider->tls, &provider->key_provider);
  if (rc != TURBO_OK) goto fail;
  provider->interface = (turbo_flow_security_auth_provider_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_INIT;
  provider->interface.ctx = provider;
  provider->interface.authenticate = flow_http_authenticate;
  provider->enhanced_interface =
      (turbo_flow_security_enhanced_auth_provider_t)TURBO_FLOW_SECURITY_ENHANCED_AUTH_PROVIDER_INIT;
  provider->enhanced_interface.ctx = provider;
  provider->enhanced_interface.begin = flow_http_enhanced_auth_begin;
  provider->enhanced_interface.continue_exchange = flow_http_enhanced_auth_continue;
  provider->enhanced_interface.cancel = flow_http_enhanced_auth_cancel;
  *out = provider;
  return TURBO_OK;

fail:
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  turbo_flow_http_auth_provider_destroy(provider);
  return rc;
}

static int flow_http_auth_field_allowed(const char *field, const char *const *allowed,
                                        size_t allowed_count) {
  for (size_t i = 0u; i < allowed_count; ++i)
    if (field && strcmp(field, allowed[i]) == 0) return 1;
  return 0;
}

int turbo_flow_http_auth_provider_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider, turbo_flow_http_auth_provider_t **out,
    turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {
      "backend", "url", "method", "service_token_ref", "timeout_ms", "max_secret_size", "tls"};
  turbo_flow_http_auth_provider_config_t config = TURBO_FLOW_HTTP_AUTH_PROVIDER_CONFIG_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *value;
  const char *json;
  size_t json_size = 0u;
  uint64_t number;
  const char *tls_detail = NULL;
  int rc = TURBO_OK;
  if (out) *out = NULL;
  if (!resolved || !channel_name || !channel_name[0] || !key_provider || !out || !error ||
      error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document)
    return flow_http_auth_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                       "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_http_auth_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                     "authentication provider channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "auth_provider") != 0 || !fields ||
      turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_http_auth_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                     "channel must be kind auth_provider with a config mapping");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!flow_http_auth_field_allowed(field, allowed, sizeof(allowed) / sizeof(allowed[0]))) {
      rc = flow_http_auth_config_error(error, TURBO_EINVAL, channel_name, field,
                                       "unknown HTTPS authentication provider field");
      goto done;
    }
  }
  config.url = turbo_json_get_string(fields, "url");
  config.method = turbo_json_get_string(fields, "method");
  config.service_token_ref = turbo_json_get_string(fields, "service_token_ref");
  rc = flow_http_tls_client_parse_json(turbo_json_object_get(fields, "tls"), &config.tls,
                                       &tls_detail);
  if (rc != TURBO_OK) {
    rc = flow_http_auth_config_error(error, rc, channel_name, "tls", tls_detail);
    goto done;
  }
  if (!config.url || !config.url[0] || !config.method || !config.method[0] ||
      !config.service_token_ref || !config.service_token_ref[0]) {
    rc = flow_http_auth_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                     "url, method, and service_token_ref are required strings");
    goto done;
  }
  value = turbo_json_object_get(fields, "backend");
  if (!value || turbo_json_type(value) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(value), TURBO_FLOW_HTTP_AUTH_BACKEND) != 0) {
    rc = flow_http_auth_config_error(error, TURBO_EINVAL, channel_name, "backend",
                                     "backend must be exactly https");
    goto done;
  }
  value = turbo_json_object_get(fields, "timeout_ms");
  if (value) {
    if (flow_http_auth_json_u64(value, &number) != TURBO_OK || number == 0u ||
        number > TURBO_FLOW_HTTP_AUTH_MAX_TIMEOUT_MS) {
      rc = flow_http_auth_config_error(error, TURBO_ERANGE, channel_name, "timeout_ms",
                                       "timeout_ms must be between 1 and 30000");
      goto done;
    }
    config.timeout_ms = (uint32_t)number;
  }
  value = turbo_json_object_get(fields, "max_secret_size");
  if (value) {
    if (flow_http_auth_json_u64(value, &number) != TURBO_OK || number == 0u ||
        number > TURBO_FLOW_HTTP_AUTH_DEFAULT_MAX_SECRET_SIZE) {
      rc = flow_http_auth_config_error(error, TURBO_ERANGE, channel_name, "max_secret_size",
                                       "max_secret_size must be between 1 and 4096");
      goto done;
    }
    config.max_secret_size = (size_t)number;
  }
  config.key_provider = *key_provider;
  rc = turbo_flow_http_auth_provider_create(&config, out);
  if (rc != TURBO_OK)
    rc = flow_http_auth_config_error(error, rc, channel_name, NULL,
                                     "HTTPS authentication provider creation failed");

done:
  turbo_free_json(&document);
  return rc;
}

const turbo_flow_security_auth_provider_t *
turbo_flow_http_auth_provider_interface(const turbo_flow_http_auth_provider_t *provider) {
  return provider ? &provider->interface : NULL;
}

const turbo_flow_security_enhanced_auth_provider_t *
turbo_flow_http_enhanced_auth_provider_interface(const turbo_flow_http_auth_provider_t *provider) {
  return provider ? &provider->enhanced_interface : NULL;
}

const char *turbo_flow_http_auth_provider_method(const turbo_flow_http_auth_provider_t *provider) {
  return provider ? provider->method : NULL;
}

void turbo_flow_http_auth_provider_destroy(turbo_flow_http_auth_provider_t *provider) {
  if (!provider) return;
  tstr_freep(&provider->url);
  tstr_freep(&provider->host);
  tstr_freep(&provider->method);
  tstr_freep(&provider->service_token_ref);
  flow_http_tls_client_cleanup(&provider->tls);
  crypto_wipe(&provider->key_provider, sizeof(provider->key_provider));
  free(provider);
}

static void flow_http_auth_owner_destroy(void *owner) {
  turbo_flow_http_auth_provider_destroy((turbo_flow_http_auth_provider_t *)owner);
}

static int flow_http_auth_factory_create(void *ctx, const turbo_flow_resolved_config_t *resolved,
                                         const char *channel_name,
                                         const turbo_flow_security_key_provider_t *key_provider,
                                         turbo_flow_security_auth_provider_owner_t *owner_out,
                                         turbo_flow_config_error_t *error) {
  turbo_flow_http_auth_provider_t *provider = NULL;
  int rc;
  (void)ctx;
  if (!owner_out || owner_out->size < sizeof(*owner_out)) return TURBO_EINVAL;
  rc = turbo_flow_http_auth_provider_create_resolved(resolved, channel_name, key_provider,
                                                     &provider, error);
  if (rc != TURBO_OK) return rc;
  owner_out->backend = TURBO_FLOW_HTTP_AUTH_BACKEND;
  owner_out->method = provider->method;
  owner_out->provider = &provider->interface;
  owner_out->enhanced_provider = &provider->enhanced_interface;
  owner_out->owner = provider;
  owner_out->destroy = flow_http_auth_owner_destroy;
  return TURBO_OK;
}

const turbo_flow_security_auth_provider_factory_t *turbo_flow_http_auth_provider_factory(void) {
  static const turbo_flow_security_auth_provider_factory_t factory = {
      sizeof(turbo_flow_security_auth_provider_factory_t), TURBO_FLOW_SECURITY_ABI_V3,
      TURBO_FLOW_HTTP_AUTH_BACKEND, flow_http_auth_factory_create, NULL};
  return &factory;
}
