#include "flow_http_tls_client.h"
#include "turbo_flow_http_acl.h"

#include "flow_http_acl_internal.h"

#include "CoroNet/turbo_coro_context.h"
#include "http_client.h"
#include "monocypher.h"
#include "turbo_coro.h"
#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str.h"
#include "turbo_vec.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_HTTP_ACL_PROTOCOL_VERSION 3u
#define FLOW_HTTP_ACL_TOKEN_LIMIT 4096u

typedef struct flow_http_acl_loaded_s {
  turbo_vec_t rules;
} flow_http_acl_loaded_t;

struct turbo_flow_http_acl_provider_s {
  turbo_flow_security_policy_provider_t interface;
  tstr_t url;
  tstr_t host;
  tstr_t service_token_ref;
  uint16_t port;
  uint32_t timeout_ms;
  size_t max_response_size;
  size_t max_rules;
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

static int flow_http_acl_validate_url(const char *url, tstr_t *host_out, uint16_t *port_out) {
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

static int flow_http_acl_decode_rule(const json_value_t *value, turbo_flow_security_rule_t *rule) {
  const char *line;
  if (!value || turbo_json_type(value) != TURBO_JSON_STRING || !rule) return TURBO_EPROTO;
  line = turbo_json_string(value);
  if (!line || turbo_json_string_len(value) == 0u ||
      turbo_json_string_len(value) > TURBO_FLOW_SECURITY_RULE_LINE_MAX)
    return TURBO_EPROTO;
  return turbo_flow_security_rule_parse_line(line, turbo_json_string_len(value), rule);
}

static void flow_http_acl_loaded_destroy(flow_http_acl_loaded_t *loaded) {
  if (!loaded) return;
  turbo_vec_destroy(&loaded->rules);
  free(loaded);
}

void flow_http_acl_decoded_cleanup(turbo_flow_security_policy_bundle_t *bundle) {
  if (!bundle) return;
  flow_http_acl_loaded_destroy((flow_http_acl_loaded_t *)bundle->provider_bundle);
  *bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
}

int flow_http_acl_decode_response(const char *body, size_t body_size, size_t max_rules,
                                  turbo_flow_security_policy_bundle_t *bundle_out) {
  static const char *const allowed[] = {"version", "policy_version", "expires_at", "rules"};
  turbo_json_doc_t *document = NULL;
  flow_http_acl_loaded_t *loaded = NULL;
  json_value_t *rules;
  uint64_t protocol_version;
  int rc = TURBO_EPROTO;
  if (!body || body_size == 0u || !bundle_out) return TURBO_EPROTO;
  *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  if (turbo_parse_json((const uint8_t *)body, body_size, &document) != TURBO_OK || !document)
    return TURBO_EPROTO;
  if (flow_http_acl_json_fields(document, allowed, sizeof(allowed) / sizeof(allowed[0])) !=
          TURBO_OK ||
      flow_http_acl_json_u64(turbo_json_object_get(document, "version"), &protocol_version) !=
          TURBO_OK ||
      protocol_version != FLOW_HTTP_ACL_PROTOCOL_VERSION ||
      flow_http_acl_json_u64(turbo_json_object_get(document, "policy_version"),
                             &bundle_out->policy_version) != TURBO_OK ||
      bundle_out->policy_version == 0u ||
      flow_http_acl_json_u64(turbo_json_object_get(document, "expires_at"),
                             &bundle_out->expires_at) != TURBO_OK)
    goto done;
  rules = turbo_json_object_get(document, "rules");
  if (!rules || turbo_json_type(rules) != TURBO_JSON_ARRAY || turbo_json_array_size(rules) == 0u ||
      turbo_json_array_size(rules) > max_rules)
    goto done;
  loaded = (flow_http_acl_loaded_t *)calloc(1u, sizeof(*loaded));
  if (!loaded) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = turbo_vec_init(&loaded->rules, sizeof(turbo_flow_security_rule_t));
  if (rc != TURBO_OK) goto done;
  rc = turbo_vec_reserve(&loaded->rules, turbo_json_array_size(rules));
  for (size_t i = 0u; rc == TURBO_OK && i < turbo_json_array_size(rules); ++i) {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    rc = flow_http_acl_decode_rule(turbo_json_array_get(rules, i), &rule);
    if (rc == TURBO_OK) rc = turbo_vec_push(&loaded->rules, &rule);
  }
  if (rc != TURBO_OK) goto done;
  bundle_out->rules = (const turbo_flow_security_rule_t *)loaded->rules.data;
  bundle_out->rule_count = turbo_vec_size(&loaded->rules);
  bundle_out->provider_bundle = loaded;
  loaded = NULL;
  rc = TURBO_OK;

done:
  flow_http_acl_loaded_destroy(loaded);
  turbo_free_json(&document);
  if (rc != TURBO_OK)
    *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  return rc;
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

static int flow_http_acl_load(void *ctx, uint64_t required_version,
                              turbo_flow_security_policy_bundle_t *bundle_out) {
  turbo_flow_http_acl_provider_t *provider = (turbo_flow_http_acl_provider_t *)ctx;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  http_client_t *client = NULL;
  http_response_t *response = NULL;
  char version_header[64];
  char *authorization = NULL;
  const char *headers[3];
  size_t token_size = 0u;
  int header_count = 2;
  int rc;
  if (!provider || !bundle_out || bundle_out->size < sizeof(*bundle_out)) return TURBO_EINVAL;
  *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
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
  headers[0] = "Accept: application/json";
  headers[1] = authorization;
  if (required_version != 0u) {
    int written =
        snprintf(version_header, sizeof(version_header), "X-TurboFlow-Policy-Version: %llu",
                 (unsigned long long)required_version);
    if (written < 0 || (size_t)written >= sizeof(version_header)) {
      rc = TURBO_ERANGE;
      goto done;
    }
    headers[2] = version_header;
    header_count = 3;
  }
  response = http_request(client, HTTP_GET, provider->url, headers, header_count, NULL, 0u);
  if (!response) {
    rc = TURBO_EIO;
    goto done;
  }
  if (response->status_code == 401 || response->status_code == 403) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (response->status_code == 404) {
    rc = TURBO_ENOENT;
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
  rc = flow_http_acl_decode_response(response->body, response->body_len, provider->max_rules,
                                     bundle_out);
  if (rc == TURBO_OK && required_version != 0u && bundle_out->policy_version != required_version) {
    flow_http_acl_decoded_cleanup(bundle_out);
    rc = TURBO_EPROTO;
  }

done:
  if (response) http_response_free(response);
  if (client) http_client_destroy(client);
  if (authorization) {
    crypto_wipe(authorization, sizeof("Authorization: Bearer ") + token_size);
    free(authorization);
  }
  turbo_flow_security_secret_release(&provider->key_provider, &lease);
  return rc;
}

static void flow_http_acl_release(void *ctx, turbo_flow_security_policy_bundle_t *bundle) {
  (void)ctx;
  flow_http_acl_decoded_cleanup(bundle);
}

int turbo_flow_http_acl_provider_create(const turbo_flow_http_acl_provider_config_t *config,
                                        turbo_flow_http_acl_provider_t **out) {
  const turbo_flow_http_tls_client_config_t *tls_config = NULL;
  turbo_flow_http_acl_provider_t *provider;
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  int rc;
  if (out) *out = NULL;
  if (!config ||
      !((config->api_version == TURBO_FLOW_HTTP_ACL_API_VERSION_V1 &&
         config->size == offsetof(turbo_flow_http_acl_provider_config_t, tls)) ||
        (config->api_version == TURBO_FLOW_HTTP_ACL_API_VERSION_V2 &&
         config->size >= sizeof(*config))) ||
      !out || !config->url || !config->service_token_ref || !config->service_token_ref[0] ||
      config->timeout_ms == 0u || config->timeout_ms > TURBO_FLOW_HTTP_ACL_MAX_TIMEOUT_MS ||
      config->max_response_size == 0u ||
      config->max_response_size > TURBO_FLOW_HTTP_ACL_MAX_RESPONSE_LIMIT ||
      config->max_rules == 0u || config->max_rules > TURBO_FLOW_SECURITY_MAX_RULES ||
      config->key_provider.size < sizeof(config->key_provider) || !config->key_provider.acquire ||
      !config->key_provider.release)
    return TURBO_EINVAL;
  if (config->api_version == TURBO_FLOW_HTTP_ACL_API_VERSION_V2) tls_config = &config->tls;
  provider = (turbo_flow_http_acl_provider_t *)calloc(1u, sizeof(*provider));
  if (!provider) return TURBO_ENOMEM;
  provider->url = tstr_dup(config->url);
  provider->service_token_ref = tstr_dup(config->service_token_ref);
  provider->timeout_ms = config->timeout_ms;
  provider->max_response_size = config->max_response_size;
  provider->max_rules = config->max_rules;
  provider->key_provider = config->key_provider;
  if (!provider->url || !provider->service_token_ref) {
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
  provider->interface =
      (turbo_flow_security_policy_provider_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_INIT;
  provider->interface.ctx = provider;
  provider->interface.load = flow_http_acl_load;
  provider->interface.release = flow_http_acl_release;
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
      "backend", "url", "service_token_ref", "timeout_ms", "max_response_size", "max_rules", "tls"};
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
  config.service_token_ref = turbo_json_get_string(fields, "service_token_ref");
  rc = flow_http_tls_client_parse_json(turbo_json_object_get(fields, "tls"), &config.tls,
                                       &tls_detail);
  if (rc != TURBO_OK) {
    rc = flow_http_acl_config_error(error, rc, channel_name, "tls", tls_detail);
    goto done;
  }
  value = turbo_json_object_get(fields, "backend");
  if (!config.url || !config.url[0] || !config.service_token_ref || !config.service_token_ref[0] ||
      !value || turbo_json_type(value) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(value), TURBO_FLOW_HTTP_ACL_BACKEND) != 0) {
    rc = flow_http_acl_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                    "backend=https, url, and service_token_ref are required");
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
  value = turbo_json_object_get(fields, "max_rules");
  if (value) {
    if (flow_http_acl_json_u64(value, &number) != TURBO_OK || number == 0u ||
        number > TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = flow_http_acl_config_error(error, TURBO_ERANGE, channel_name, "max_rules",
                                      "max_rules must be between 1 and 4096");
      goto done;
    }
    config.max_rules = (size_t)number;
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

const turbo_flow_security_policy_provider_t *
turbo_flow_http_acl_provider_interface(const turbo_flow_http_acl_provider_t *provider) {
  return provider ? &provider->interface : NULL;
}

void turbo_flow_http_acl_provider_destroy(turbo_flow_http_acl_provider_t *provider) {
  if (!provider) return;
  tstr_freep(&provider->url);
  tstr_freep(&provider->host);
  tstr_freep(&provider->service_token_ref);
  flow_http_tls_client_cleanup(&provider->tls);
  crypto_wipe(&provider->key_provider, sizeof(provider->key_provider));
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
  owner_out->provider = &provider->interface;
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
