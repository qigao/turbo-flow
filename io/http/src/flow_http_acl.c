#include "flow_http_tls_client.h"
#include "turbo_flow_http_acl.h"

#include "flow_http_acl_internal.h"

#include "CoroNet/turbo_coro_context.h"
#include "http_client.h"
#include "turbo_crypto.h"
#include "turbo_coro.h"
#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_HTTP_ACL_PROTOCOL_VERSION 4u
#define FLOW_HTTP_ACL_TOKEN_LIMIT 4096u

struct turbo_flow_http_acl_provider_s {
  turbo_flow_security_authorization_provider_t interface;
  tstr url;
  tstr host;
  tstr service_id;
  tstr service_domain;
  tstr service_token_ref;
  uint16_t port;
  uint32_t timeout_ms;
  size_t max_response_size;
  turbo_flow_security_key_provider_t key_provider;
  flow_http_tls_client_t tls;
};

static int flow_http_acl_config_error(turbo_flow_config_error_t *error, int status,
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

static int flow_http_acl_ascii_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
  while (*left && *right) {
    if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) return 0;
    ++left;
    ++right;
  }
  return *left == '\0' && *right == '\0';
}

static int flow_http_acl_connect_policy(const char *scheme, const char *hostname, uint16_t port,
                                        const turbo_dns_result_t *results, size_t result_count,
                                        void *user_data) {
  const turbo_flow_http_acl_provider_t *provider =
      (const turbo_flow_http_acl_provider_t *)user_data;
  (void)results;
  return provider && scheme && strcmp(scheme, "https") == 0 && hostname &&
                 flow_http_acl_ascii_equal(hostname, provider->host) && port == provider->port &&
                 result_count != 0u
             ? 0
             : -1;
}

static int flow_http_acl_validate_url(const char *url, tstr *host_out, uint16_t *port_out) {
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

static int flow_http_acl_json_fields(const json_value_t *object, const char *const *allowed,
                                     size_t allowed_count) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT) return TURBO_EPROTO;
  for (size_t i = 0u; i < turbo_json_object_size(object); ++i) {
    const char *field = turbo_json_object_key(object, i);
    int known = 0;
    for (size_t j = 0u; j < allowed_count; ++j)
      if (field && strcmp(field, allowed[j]) == 0) known = 1;
    if (!known) return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_http_acl_json_u64(const json_value_t *value, uint64_t *out) {
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

static int flow_http_acl_content_type_json(const char *headers, size_t size) {
  static const char name[] = "content-type:";
  static const char media[] = "application/json";
  const char *cursor = headers;
  const char *end = headers ? headers + size : NULL;
  int matches = 0;
  while (cursor && cursor < end) {
    const char *line_end = memchr(cursor, '\n', (size_t)(end - cursor));
    size_t line_size = line_end ? (size_t)(line_end - cursor) : (size_t)(end - cursor);
    size_t matched_name = 0u;
    while (matched_name < sizeof(name) - 1u && matched_name < line_size &&
           tolower((unsigned char)cursor[matched_name]) == name[matched_name])
      ++matched_name;
    if (matched_name == sizeof(name) - 1u) {
      const char *value = cursor + sizeof(name) - 1u;
      size_t matched_media = 0u;
      while (value < cursor + line_size && (*value == ' ' || *value == '\t'))
        ++value;
      while (matched_media < sizeof(media) - 1u && value + matched_media < cursor + line_size &&
             tolower((unsigned char)value[matched_media]) == media[matched_media])
        ++matched_media;
      if (matched_media != sizeof(media) - 1u ||
          (value + matched_media < cursor + line_size && value[matched_media] != ';' &&
           value[matched_media] != '\r'))
        return 0;
      ++matches;
    }
    cursor = line_end ? line_end + 1 : end;
  }
  return matches == 1;
}

static void flow_http_acl_free_json_value(json_value_t *value) {
  turbo_json_doc_t *owned = (turbo_json_doc_t *)value;
  if (owned) turbo_free_json(&owned);
}

static int flow_http_acl_json_add(json_value_t *object, const char *field, json_value_t *value) {
  if (value && turbo_json_object_add_checked(object, field, value)) return TURBO_OK;
  flow_http_acl_free_json_value(value);
  return TURBO_ENOMEM;
}

static int flow_http_acl_json_array_add(json_value_t *array, json_value_t *value) {
  if (value && turbo_json_array_add_checked(array, value)) return TURBO_OK;
  flow_http_acl_free_json_value(value);
  return TURBO_ENOMEM;
}

static int flow_http_acl_add_string_array(json_value_t *object, const char *field,
                                          const char *values, size_t stride, uint32_t count) {
  json_value_t *array = turbo_json_create_array();
  int rc = array ? TURBO_OK : TURBO_ENOMEM;
  for (uint32_t index = 0u; rc == TURBO_OK && index < count; ++index)
    rc = flow_http_acl_json_array_add(array,
                                      turbo_json_create_string(values + (size_t)index * stride));
  if (rc == TURBO_OK) {
    rc = flow_http_acl_json_add(object, field, array);
    array = NULL;
  }
  if (rc != TURBO_OK) flow_http_acl_free_json_value(array);
  return rc;
}

static int flow_http_acl_bounded_string(const uint8_t *value, size_t size, size_t maximum,
                                        char **out) {
  char *copy;
  if (out) *out = NULL;
  if (!out || size > maximum || (size != 0u && !value) || (value && memchr(value, '\0', size)))
    return TURBO_EINVAL;
  copy = (char *)malloc(size + 1u);
  if (!copy) return TURBO_ENOMEM;
  if (size != 0u) memcpy(copy, value, size);
  copy[size] = '\0';
  *out = copy;
  return TURBO_OK;
}

int flow_http_acl_encode_check_request(const turbo_flow_security_request_t *request,
                                       char **body_out, size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  json_value_t *principal = NULL;
  char *username = NULL;
  char *client_id = NULL;
  const char *access;
  int rc = TURBO_ENOMEM;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!request || request->size < sizeof(*request) || !request->principal || !request->resource ||
      !body_out || !body_size_out ||
      request->principal->size < sizeof(*request->principal) ||
      request->principal->role_count > TURBO_FLOW_SECURITY_MAX_ROLES ||
      request->principal->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS ||
      (request->resource_type != TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC &&
       request->resource_type != TURBO_FLOW_SECURITY_RESOURCE_GENERIC))
    return TURBO_EINVAL;
  access = request->action == TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE
               ? "read"
               : (request->action == TURBO_FLOW_SECURITY_ACTION_PUBLISH
                      ? "write"
                      : (request->action == TURBO_FLOW_SECURITY_ACTION_CONNECT ? "connect" : NULL));
  if (!access) return TURBO_EINVAL;
  rc = flow_http_acl_bounded_string(request->username, request->username_size,
                                    TURBO_FLOW_SECURITY_ID_MAX, &username);
  if (rc == TURBO_OK)
    rc = flow_http_acl_bounded_string(request->client_id, request->client_id_size,
                                      TURBO_FLOW_SECURITY_ID_MAX, &client_id);
  if (rc != TURBO_OK) goto done;
  document = (turbo_json_doc_t *)turbo_json_create_object();
  principal = turbo_json_create_object();
  if (!document || !principal) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  if (flow_http_acl_json_add(principal, "id",
                             turbo_json_create_string(request->principal->principal_id)) !=
          TURBO_OK ||
      flow_http_acl_json_add(principal, "type",
                             turbo_json_create_string(request->principal->principal_type)) !=
          TURBO_OK ||
      flow_http_acl_json_add(principal, "domain",
                             turbo_json_create_string(request->principal->domain_id)) != TURBO_OK ||
      flow_http_acl_json_add(principal, "expires_at",
                             turbo_json_create_uint64(request->principal->expires_at)) != TURBO_OK ||
      flow_http_acl_json_add(principal, "policy_version",
                             turbo_json_create_uint64(request->principal->policy_version)) !=
          TURBO_OK ||
      flow_http_acl_add_string_array(principal, "roles", (const char *)request->principal->roles,
                                     sizeof(request->principal->roles[0]),
                                     request->principal->role_count) != TURBO_OK ||
      flow_http_acl_add_string_array(principal, "groups", (const char *)request->principal->groups,
                                     sizeof(request->principal->groups[0]),
                                     request->principal->group_count) != TURBO_OK ||
      flow_http_acl_json_add(document, "version",
                             turbo_json_create_uint64(FLOW_HTTP_ACL_PROTOCOL_VERSION)) != TURBO_OK ||
      flow_http_acl_json_add(document, "access", turbo_json_create_string(access)) != TURBO_OK ||
      flow_http_acl_json_add(document, "topic",
                             turbo_json_create_string(request->resource)) != TURBO_OK ||
      flow_http_acl_json_add(document, "username", turbo_json_create_string(username)) !=
          TURBO_OK ||
      flow_http_acl_json_add(document, "client_id", turbo_json_create_string(client_id)) !=
          TURBO_OK) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flow_http_acl_json_add(document, "principal", principal);
  principal = NULL;
  if (rc != TURBO_OK) goto done;
  *body_out = turbo_json_serialize(document, body_size_out);
  rc = *body_out ? TURBO_OK : TURBO_ENOMEM;

done:
  flow_http_acl_free_json_value(principal);
  turbo_free_json(&document);
  free(username);
  free(client_id);
  return rc;
}

static int flow_http_acl_reason(const char *value,
                                turbo_flow_security_decision_reason_t *reason_out) {
  if (!value || !reason_out) return TURBO_EPROTO;
  if (strcmp(value, "allow_rule") == 0) *reason_out = TURBO_FLOW_SECURITY_REASON_ALLOW_RULE;
  else if (strcmp(value, "deny_rule") == 0) *reason_out = TURBO_FLOW_SECURITY_REASON_DENY_RULE;
  else if (strcmp(value, "default_deny") == 0)
    *reason_out = TURBO_FLOW_SECURITY_REASON_DEFAULT_DENY;
  else if (strcmp(value, "domain_mismatch") == 0)
    *reason_out = TURBO_FLOW_SECURITY_REASON_DOMAIN_MISMATCH;
  else if (strcmp(value, "principal_expired") == 0)
    *reason_out = TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED;
  else if (strcmp(value, "policy_version_mismatch") == 0)
    *reason_out = TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH;
  else return TURBO_EPROTO;
  return TURBO_OK;
}

int flow_http_acl_decode_check_response(const char *body, size_t body_size,
                                        turbo_flow_security_decision_t *decision_out) {
  static const char *const allowed[] = {"version", "allowed", "reason", "policy_version"};
  turbo_json_doc_t *document = NULL;
  json_value_t *allowed_value;
  json_value_t *reason;
  uint64_t version = 0u;
  int rc = TURBO_EPROTO;
  if (!body || body_size == 0u || !decision_out || decision_out->size < sizeof(*decision_out))
    return TURBO_EINVAL;
  *decision_out = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
  if (turbo_parse_json((const uint8_t *)body, body_size, &document) != TURBO_OK || !document)
    return TURBO_EPROTO;
  allowed_value = turbo_json_object_get(document, "allowed");
  reason = turbo_json_object_get(document, "reason");
  if (turbo_json_object_size(document) != sizeof(allowed) / sizeof(allowed[0]) ||
      flow_http_acl_json_fields(document, allowed, sizeof(allowed) / sizeof(allowed[0])) !=
          TURBO_OK ||
      flow_http_acl_json_u64(turbo_json_object_get(document, "version"), &version) != TURBO_OK ||
      version != FLOW_HTTP_ACL_PROTOCOL_VERSION || !allowed_value ||
      turbo_json_type(allowed_value) != TURBO_JSON_BOOL || !reason ||
      turbo_json_type(reason) != TURBO_JSON_STRING ||
      flow_http_acl_reason(turbo_json_string(reason), &decision_out->reason) != TURBO_OK ||
      flow_http_acl_json_u64(turbo_json_object_get(document, "policy_version"),
                             &decision_out->policy_version) != TURBO_OK ||
      decision_out->policy_version == 0u)
    goto done;
  decision_out->effect = turbo_json_bool(allowed_value) ? TURBO_FLOW_SECURITY_ALLOW
                                                        : TURBO_FLOW_SECURITY_DENY;
  if ((decision_out->effect == TURBO_FLOW_SECURITY_ALLOW &&
       decision_out->reason != TURBO_FLOW_SECURITY_REASON_ALLOW_RULE) ||
      (decision_out->effect == TURBO_FLOW_SECURITY_DENY &&
       decision_out->reason == TURBO_FLOW_SECURITY_REASON_ALLOW_RULE))
    goto done;
  rc = TURBO_OK;
done:
  turbo_free_json(&document);
  if (rc != TURBO_OK)
    *decision_out = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
  return rc;
}

static int flow_http_acl_authorize(void *ctx, const turbo_flow_security_request_t *request,
                                   uint64_t now_epoch_seconds,
                                   turbo_flow_security_decision_t *decision_out) {
  turbo_flow_http_acl_provider_t *provider = (turbo_flow_http_acl_provider_t *)ctx;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  char *authorization = NULL;
  char *body = NULL;
  size_t body_size = 0u;
  char service_id_header[sizeof("X-TurboFlow-Service-Id: ") + TURBO_FLOW_SECURITY_ID_MAX];
  char service_domain_header[sizeof("X-TurboFlow-Service-Domain: ") + TURBO_FLOW_SECURITY_ID_MAX];
  const char *headers[5];
  size_t token_size = 0u;
  int rc;
  (void)now_epoch_seconds;
  if (!provider || !request || !decision_out || decision_out->size < sizeof(*decision_out))
    return TURBO_EINVAL;
  *decision_out = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
  if (!coro_running() || !coro_context_current()) return TURBO_ENOTSUP;
  rc = turbo_flow_security_secret_acquire(&provider->key_provider, provider->service_token_ref,
                                          &lease);
  if (rc != TURBO_OK) return rc;
  token_size = lease.byte_count;
  if (!lease.bytes || token_size == 0u || token_size > FLOW_HTTP_ACL_TOKEN_LIMIT ||
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
  if (snprintf(service_id_header, sizeof(service_id_header), "X-TurboFlow-Service-Id: %s",
               provider->service_id) <= 0 ||
      snprintf(service_domain_header, sizeof(service_domain_header),
               "X-TurboFlow-Service-Domain: %s", provider->service_domain) <= 0) {
    rc = TURBO_ERANGE;
    goto done;
  }
  rc = flow_http_acl_encode_check_request(request, &body, &body_size);
  if (rc != TURBO_OK) goto done;
  client = http_client_create(provider->url);
  if (!client) {
    rc = TURBO_EIO;
    goto done;
  }
  http_client_set_timeout(client, (int)provider->timeout_ms);
  http_client_set_connect_timeout(client, (int)provider->timeout_ms);
  http_client_set_read_timeout(client, (int)provider->timeout_ms);
  http_client_set_max_response_size(client, provider->max_response_size);
  http_client_set_max_response_header_size(client, 16384u);
  http_client_follow_redirects(client, 0);
  http_client_clear_retry_policy(client);
  rc = flow_http_tls_client_apply(&provider->tls, &provider->key_provider, client);
  if (rc != TURBO_OK) goto done;
  if (http_client_set_connect_policy(client, flow_http_acl_connect_policy, provider) != 0) {
    rc = TURBO_EIO;
    goto done;
  }
  headers[0] = "Content-Type: application/json";
  headers[1] = "Accept: application/json";
  headers[2] = authorization;
  headers[3] = service_id_header;
  headers[4] = service_domain_header;
  response = http_request(client, HTTP_POST, provider->url, headers, 5, body, body_size);
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
      !flow_http_acl_content_type_json(response->headers, response->headers_len)) {
    rc = TURBO_EIO;
    goto done;
  }
  rc = flow_http_acl_decode_check_response(response->body, response->body_len, decision_out);

done:
  if (response) http_response_free(response);
  if (client) http_client_destroy(client);
  if (body) turbo_json_serialize_free(body);
  if (authorization) {
    turbo_crypto_wipe(authorization, sizeof("Authorization: Bearer ") + token_size);
    free(authorization);
  }
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  return rc;
}

int turbo_flow_http_acl_provider_create(const turbo_flow_http_acl_provider_config_t *config,
                                        turbo_flow_http_acl_provider_t **out) {
  const turbo_flow_http_tls_client_config_t *tls_config = NULL;
  turbo_flow_http_acl_provider_t *provider;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  int rc;
  if (out) *out = NULL;
  if (!config || config->api_version != TURBO_FLOW_HTTP_ACL_API_VERSION_V4 ||
      config->size < sizeof(*config) || !out || !config->url || !config->service_id ||
      !config->service_id[0] || strlen(config->service_id) > TURBO_FLOW_SECURITY_ID_MAX ||
      !config->service_domain || !config->service_domain[0] ||
      strlen(config->service_domain) > TURBO_FLOW_SECURITY_ID_MAX ||
      !config->service_token_ref || !config->service_token_ref[0] ||
      config->timeout_ms == 0u || config->timeout_ms > TURBO_FLOW_HTTP_ACL_MAX_TIMEOUT_MS ||
      config->max_response_size == 0u ||
      config->max_response_size > TURBO_FLOW_HTTP_ACL_MAX_RESPONSE_LIMIT ||
      config->key_provider.size < sizeof(config->key_provider) || !config->key_provider.acquire ||
      !config->key_provider.release)
    return TURBO_EINVAL;
  tls_config = &config->tls;
  provider = (turbo_flow_http_acl_provider_t *)calloc(1u, sizeof(*provider));
  if (!provider) return TURBO_ENOMEM;
  provider->url = tstr_dup(config->url);
  provider->service_id = tstr_dup(config->service_id);
  provider->service_domain = tstr_dup(config->service_domain);
  provider->service_token_ref = tstr_dup(config->service_token_ref);
  provider->timeout_ms = config->timeout_ms;
  provider->max_response_size = config->max_response_size;
  provider->key_provider = config->key_provider;
  if (!provider->url || !provider->service_id || !provider->service_domain ||
      !provider->service_token_ref) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = flow_http_tls_client_init(&provider->tls, tls_config);
  if (rc != TURBO_OK) goto fail;
  rc = flow_http_acl_validate_url(provider->url, &provider->host, &provider->port);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_flow_security_secret_acquire(&provider->key_provider, provider->service_token_ref,
                                          &lease);
  if (rc != TURBO_OK) goto fail;
  if (!lease.bytes || lease.byte_count == 0u || lease.byte_count > FLOW_HTTP_ACL_TOKEN_LIMIT ||
      memchr(lease.bytes, '\0', lease.byte_count) || memchr(lease.bytes, '\r', lease.byte_count) ||
      memchr(lease.bytes, '\n', lease.byte_count)) {
    rc = TURBO_EPERM;
    goto fail;
  }
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  rc = flow_http_tls_client_probe_secret(&provider->tls, &provider->key_provider);
  if (rc != TURBO_OK) goto fail;
  provider->interface = (turbo_flow_security_authorization_provider_t)
      TURBO_FLOW_SECURITY_AUTHORIZATION_PROVIDER_INIT;
  provider->interface.ctx = provider;
  provider->interface.authorize = flow_http_acl_authorize;
  *out = provider;
  return TURBO_OK;

fail:
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  turbo_flow_http_acl_provider_destroy(provider);
  return rc;
}

static int flow_http_acl_field_allowed(const char *field, const char *const *allowed,
                                       size_t count) {
  for (size_t i = 0u; i < count; ++i)
    if (field && strcmp(field, allowed[i]) == 0) return 1;
  return 0;
}

int turbo_flow_http_acl_provider_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider, turbo_flow_http_acl_provider_t **out,
    turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {
      "backend", "url", "service_id", "service_domain", "service_token_ref", "timeout_ms",
      "max_response_size", "tls"};
  turbo_flow_http_acl_provider_config_t config = TURBO_FLOW_HTTP_ACL_PROVIDER_CONFIG_INIT;
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
    return flow_http_acl_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  kind = channel ? turbo_json_object_get(channel, "kind") : NULL;
  fields = channel ? turbo_json_object_get(channel, "config") : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT || !kind ||
      turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "acl_provider") != 0 || !fields ||
      turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_http_acl_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                    "channel must be kind acl_provider with config");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!flow_http_acl_field_allowed(field, allowed, sizeof(allowed) / sizeof(allowed[0]))) {
      rc = flow_http_acl_config_error(error, TURBO_EINVAL, channel_name, field,
                                      "unknown HTTPS ACL provider field");
      goto done;
    }
  }
  config.url = turbo_json_get_string(fields, "url");
  config.service_id = turbo_json_get_string(fields, "service_id");
  config.service_domain = turbo_json_get_string(fields, "service_domain");
  config.service_token_ref = turbo_json_get_string(fields, "service_token_ref");
  rc = flow_http_tls_client_parse_json(turbo_json_object_get(fields, "tls"), &config.tls,
                                       &tls_detail);
  if (rc != TURBO_OK) {
    rc = flow_http_acl_config_error(error, rc, channel_name, "tls", tls_detail);
    goto done;
  }
  value = turbo_json_object_get(fields, "backend");
  if (!config.url || !config.url[0] || !config.service_id || !config.service_id[0] ||
      !config.service_domain || !config.service_domain[0] || !config.service_token_ref ||
      !config.service_token_ref[0] ||
      !value || turbo_json_type(value) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(value), TURBO_FLOW_HTTP_ACL_BACKEND) != 0) {
    rc = flow_http_acl_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                    "backend=https, url, service_id, service_domain, and "
                                    "service_token_ref are required");
    goto done;
  }
  value = turbo_json_object_get(fields, "timeout_ms");
  if (value) {
    if (flow_http_acl_json_u64(value, &number) != TURBO_OK || number == 0u ||
        number > TURBO_FLOW_HTTP_ACL_MAX_TIMEOUT_MS) {
      rc = flow_http_acl_config_error(error, TURBO_ERANGE, channel_name, "timeout_ms",
                                      "timeout_ms must be between 1 and 30000");
      goto done;
    }
    config.timeout_ms = (uint32_t)number;
  }
  value = turbo_json_object_get(fields, "max_response_size");
  if (value) {
    if (flow_http_acl_json_u64(value, &number) != TURBO_OK || number == 0u ||
        number > TURBO_FLOW_HTTP_ACL_MAX_RESPONSE_LIMIT) {
      rc = flow_http_acl_config_error(error, TURBO_ERANGE, channel_name, "max_response_size",
                                      "max_response_size is out of range");
      goto done;
    }
    config.max_response_size = (size_t)number;
  }
  config.key_provider = *key_provider;
  rc = turbo_flow_http_acl_provider_create(&config, out);
  if (rc != TURBO_OK)
    rc = flow_http_acl_config_error(error, rc, channel_name, NULL,
                                    "HTTPS ACL provider creation failed");

done:
  turbo_free_json(&document);
  return rc;
}

const turbo_flow_security_authorization_provider_t *
turbo_flow_http_acl_provider_interface(const turbo_flow_http_acl_provider_t *provider) {
  return provider ? &provider->interface : NULL;
}

void turbo_flow_http_acl_provider_destroy(turbo_flow_http_acl_provider_t *provider) {
  if (!provider) return;
  tstr_freep(&provider->url);
  tstr_freep(&provider->host);
  tstr_freep(&provider->service_id);
  tstr_freep(&provider->service_domain);
  tstr_freep(&provider->service_token_ref);
  flow_http_tls_client_cleanup(&provider->tls);
  turbo_crypto_wipe(&provider->key_provider, sizeof(provider->key_provider));
  free(provider);
}

static void flow_http_acl_owner_destroy(void *owner) {
  turbo_flow_http_acl_provider_destroy((turbo_flow_http_acl_provider_t *)owner);
}

static int flow_http_acl_factory_create(void *ctx, const turbo_flow_resolved_config_t *resolved,
                                        const char *channel_name,
                                        const turbo_flow_security_key_provider_t *key_provider,
                                        turbo_flow_security_policy_provider_owner_t *owner_out,
                                        turbo_flow_config_error_t *error) {
  turbo_flow_http_acl_provider_t *provider = NULL;
  int rc;
  (void)ctx;
  if (!owner_out || owner_out->size < sizeof(*owner_out)) return TURBO_EINVAL;
  rc = turbo_flow_http_acl_provider_create_resolved(resolved, channel_name, key_provider, &provider,
                                                    error);
  if (rc != TURBO_OK) return rc;
  owner_out->backend = TURBO_FLOW_HTTP_ACL_BACKEND;
  owner_out->authorization_provider = &provider->interface;
  owner_out->owner = provider;
  owner_out->destroy = flow_http_acl_owner_destroy;
  return TURBO_OK;
}

const turbo_flow_security_policy_provider_factory_t *turbo_flow_http_acl_provider_factory(void) {
  static const turbo_flow_security_policy_provider_factory_t factory = {
      sizeof(turbo_flow_security_policy_provider_factory_t), TURBO_FLOW_SECURITY_ABI_V3,
      TURBO_FLOW_HTTP_ACL_BACKEND, flow_http_acl_factory_create, NULL};
  return &factory;
}
