#include "flowie_control_auth_iris_endpoint_internal.h"

#include "base64_utils.h"
#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_AUTH_HTTP_TOKEN_MAX 4096u
#define FLOWIE_CONTROL_AUTH_HTTP_DIGEST_SIZE 32u
#define FLOWIE_CONTROL_AUTH_HTTP_AUTHORIZATION_MAX \
  (sizeof("Bearer ") - 1u + FLOWIE_CONTROL_AUTH_HTTP_TOKEN_MAX)

struct flowie_control_auth_iris_endpoint_s {
  flowie_control_auth_iris_adapter_t *adapter;
  turbo_flow_security_key_provider_t key_provider;
  char *service_token_ref;
  size_t max_request_body_size;
  size_t max_secret_size;
  iris_app_t *bound_app;
};

static int flowie_control_auth_http_ascii_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
  while (*left && *right) {
    if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) return 0;
    ++left;
    ++right;
  }
  return *left == '\0' && *right == '\0';
}

static int flowie_control_auth_http_text_valid(const char *value, size_t maximum) {
  size_t size;
  if (!value || maximum == 0u) return 0;
  size = strnlen(value, maximum + 1u);
  if (size == 0u || size > maximum) return 0;
  for (size_t index = 0u; index < size; ++index) {
    unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

static void flowie_control_auth_http_free_value(json_value_t *value) {
  turbo_json_doc_t *owned = (turbo_json_doc_t *)value;
  if (owned) turbo_free_json(&owned);
}

static int flowie_control_auth_http_add(json_value_t *object, const char *field,
                                        json_value_t *value) {
  if (value && turbo_json_object_add_checked(object, field, value)) return TURBO_OK;
  flowie_control_auth_http_free_value(value);
  return TURBO_ENOMEM;
}

static int flowie_control_auth_http_array_add(json_value_t *array, json_value_t *value) {
  if (value && turbo_json_array_add_checked(array, value)) return TURBO_OK;
  flowie_control_auth_http_free_value(value);
  return TURBO_ENOMEM;
}

static int flowie_control_auth_http_fields_exact(const json_value_t *object,
                                                 const char *const *allowed,
                                                 size_t allowed_count) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT ||
      turbo_json_object_size(object) != allowed_count)
    return TURBO_EPROTO;
  for (size_t index = 0u; index < turbo_json_object_size(object); ++index) {
    const char *field = turbo_json_object_key(object, index);
    int known = 0;
    for (size_t allowed_index = 0u; allowed_index < allowed_count; ++allowed_index) {
      if (field && strcmp(field, allowed[allowed_index]) == 0) {
        known = 1;
        break;
      }
    }
    if (!known) return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flowie_control_auth_http_json_u64(const json_value_t *value, uint64_t *out) {
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
  if (buffer[0] == '-' || buffer[0] == '+' || (size > 1u && buffer[0] == '0'))
    return TURBO_EPROTO;
  parsed = strtoull(buffer, &end, 10);
  if (!end || *end != '\0') return TURBO_EPROTO;
  *out = (uint64_t)parsed;
  return TURBO_OK;
}

static int flowie_control_auth_http_copy_string(const json_value_t *object, const char *field,
                                                char *output, size_t capacity) {
  json_value_t *value;
  const char *text;
  size_t size;
  if (!object || !field || !output || capacity == 0u) return TURBO_EPROTO;
  value = turbo_json_object_get(object, field);
  if (!value || turbo_json_type(value) != TURBO_JSON_STRING) return TURBO_EPROTO;
  text = turbo_json_string(value);
  size = turbo_json_string_len(value);
  if (!text || size == 0u || size >= capacity || memchr(text, '\0', size)) return TURBO_EPROTO;
  memcpy(output, text, size);
  output[size] = '\0';
  return TURBO_OK;
}

void flowie_control_auth_http_request_clear(flowie_control_auth_http_request_t *request) {
  if (request) crypto_wipe(request, sizeof(*request));
}

int flowie_control_auth_http_decode_request(const char *body, size_t body_size,
                                            size_t max_secret_size,
                                            flowie_control_auth_http_request_t *request_out) {
  static const char *const allowed[] = {"version",       "identity", "method",
                                        "secret_base64", "protocol", "remote_address"};
  turbo_json_doc_t *document = NULL;
  json_value_t *encoded_value;
  const char *encoded;
  uint8_t *decoded = NULL;
  char *canonical = NULL;
  size_t encoded_size;
  size_t decoded_size = 0u;
  uint64_t version = 0u;
  int rc = TURBO_EPROTO;

  if (!request_out) return TURBO_EINVAL;
  memset(request_out, 0, sizeof(*request_out));
  if (!body || body_size == 0u ||
      body_size > FLOWIE_CONTROL_AUTH_HTTP_ABSOLUTE_REQUEST_BODY_MAX ||
      max_secret_size == 0u || max_secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX)
    return TURBO_EPROTO;
  if (turbo_parse_json((const uint8_t *)body, body_size, &document) != TURBO_OK || !document)
    return TURBO_EPROTO;
  if (flowie_control_auth_http_fields_exact(document, allowed,
                                            sizeof(allowed) / sizeof(allowed[0])) != TURBO_OK ||
      flowie_control_auth_http_json_u64(turbo_json_object_get(document, "version"), &version) !=
          TURBO_OK ||
      version != FLOWIE_CONTROL_AUTH_HTTP_PROTOCOL_VERSION ||
      flowie_control_auth_http_copy_string(document, "identity", request_out->identity,
                                           sizeof(request_out->identity)) != TURBO_OK ||
      flowie_control_auth_http_copy_string(document, "method", request_out->method,
                                           sizeof(request_out->method)) != TURBO_OK ||
      flowie_control_auth_http_copy_string(document, "protocol", request_out->protocol,
                                           sizeof(request_out->protocol)) != TURBO_OK ||
      flowie_control_auth_http_copy_string(document, "remote_address",
                                           request_out->remote_address,
                                           sizeof(request_out->remote_address)) != TURBO_OK)
    goto done;

  encoded_value = turbo_json_object_get(document, "secret_base64");
  if (!encoded_value || turbo_json_type(encoded_value) != TURBO_JSON_STRING) goto done;
  encoded = turbo_json_string(encoded_value);
  encoded_size = turbo_json_string_len(encoded_value);
  if (!encoded || encoded_size == 0u ||
      encoded_size > ((max_secret_size + 2u) / 3u) * 4u ||
      memchr(encoded, '\0', encoded_size) || strlen(encoded) != encoded_size ||
      tn_base64_decode(encoded, &decoded, &decoded_size) != 0 || !decoded || decoded_size == 0u ||
      decoded_size > max_secret_size)
    goto done;
  if (tn_base64_encode(decoded, decoded_size, &canonical) != 0 || !canonical) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  if (strlen(canonical) != encoded_size || memcmp(canonical, encoded, encoded_size) != 0)
    goto done;
  memcpy(request_out->secret, decoded, decoded_size);
  request_out->secret_size = decoded_size;
  rc = TURBO_OK;

done:
  if (canonical) {
    crypto_wipe(canonical, strlen(canonical));
    free(canonical);
  }
  if (decoded) {
    crypto_wipe(decoded, decoded_size);
    free(decoded);
  }
  turbo_free_json(&document);
  if (rc != TURBO_OK) flowie_control_auth_http_request_clear(request_out);
  return rc;
}

static const char *flowie_control_auth_http_scope_name(turbo_flow_security_scope_t scope) {
  switch (scope) {
  case TURBO_FLOW_SECURITY_SCOPE_SELF:
    return "self";
  case TURBO_FLOW_SECURITY_SCOPE_GROUP:
    return "group";
  case TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP:
    return "root_group";
  case TURBO_FLOW_SECURITY_SCOPE_SYSTEM:
    return "system";
  default:
    return NULL;
  }
}

int flowie_control_auth_http_encode_principal(const turbo_flow_security_principal_t *principal,
                                              char **body_out, size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  json_value_t *principal_json = NULL;
  json_value_t *roles = NULL;
  json_value_t *groups = NULL;
  const char *scope;
  int rc = TURBO_ENOMEM;

  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!principal || principal->size < sizeof(*principal) || !body_out || !body_size_out ||
      principal->role_count > TURBO_FLOW_SECURITY_MAX_ROLES ||
      principal->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS || principal->policy_version == 0u ||
      !(scope = flowie_control_auth_http_scope_name(principal->scope)) ||
      !flowie_control_auth_http_text_valid(principal->principal_id,
                                           TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_auth_http_text_valid(principal->principal_type,
                                           TURBO_FLOW_SECURITY_TYPE_MAX) ||
      !flowie_control_auth_http_text_valid(principal->root_group_id,
                                           TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_auth_http_text_valid(principal->auth_method,
                                           TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;

  document = (turbo_json_doc_t *)turbo_json_create_object();
  principal_json = turbo_json_create_object();
  roles = turbo_json_create_array();
  groups = turbo_json_create_array();
  if (!document || !principal_json || !roles || !groups) goto done;
  for (uint32_t index = 0u; index < principal->role_count; ++index) {
    if (!flowie_control_auth_http_text_valid(principal->roles[index],
                                             TURBO_FLOW_SECURITY_TYPE_MAX)) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (flowie_control_auth_http_array_add(
            roles, turbo_json_create_string(principal->roles[index])) != TURBO_OK)
      goto done;
  }
  for (uint32_t index = 0u; index < principal->group_count; ++index) {
    if (!flowie_control_auth_http_text_valid(principal->groups[index],
                                             TURBO_FLOW_SECURITY_ID_MAX)) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (flowie_control_auth_http_array_add(
            groups, turbo_json_create_string(principal->groups[index])) != TURBO_OK)
      goto done;
  }
  if (flowie_control_auth_http_add(document, "version",
                                   turbo_json_create_uint64(
                                       FLOWIE_CONTROL_AUTH_HTTP_PROTOCOL_VERSION)) != TURBO_OK ||
      flowie_control_auth_http_add(document, "authenticated", turbo_json_create_bool(true)) !=
          TURBO_OK ||
      flowie_control_auth_http_add(principal_json, "id",
                                   turbo_json_create_string(principal->principal_id)) != TURBO_OK ||
      flowie_control_auth_http_add(principal_json, "type",
                                   turbo_json_create_string(principal->principal_type)) !=
          TURBO_OK ||
      flowie_control_auth_http_add(principal_json, "root_group",
                                   turbo_json_create_string(principal->root_group_id)) !=
          TURBO_OK ||
      flowie_control_auth_http_add(principal_json, "auth_method",
                                   turbo_json_create_string(principal->auth_method)) != TURBO_OK ||
      flowie_control_auth_http_add(principal_json, "scope", turbo_json_create_string(scope)) !=
          TURBO_OK)
    goto done;
  if (flowie_control_auth_http_add(principal_json, "roles", roles) != TURBO_OK) {
    roles = NULL;
    goto done;
  }
  roles = NULL;
  if (flowie_control_auth_http_add(principal_json, "groups", groups) != TURBO_OK) {
    groups = NULL;
    goto done;
  }
  groups = NULL;
  if (flowie_control_auth_http_add(principal_json, "expires_at",
                                   turbo_json_create_uint64(principal->expires_at)) != TURBO_OK ||
      flowie_control_auth_http_add(
          principal_json, "policy_version",
          turbo_json_create_uint64(principal->policy_version)) != TURBO_OK)
    goto done;
  if (flowie_control_auth_http_add(document, "principal", principal_json) != TURBO_OK) {
    principal_json = NULL;
    goto done;
  }
  principal_json = NULL;
  *body_out = turbo_json_serialize(document, body_size_out);
  if (!*body_out) goto done;
  if (*body_size_out > FLOWIE_CONTROL_AUTH_HTTP_ABSOLUTE_REQUEST_BODY_MAX) {
    turbo_json_serialize_free(*body_out);
    *body_out = NULL;
    *body_size_out = 0u;
    rc = TURBO_EFBIG;
    goto done;
  }
  rc = TURBO_OK;

done:
  flowie_control_auth_http_free_value(roles);
  flowie_control_auth_http_free_value(groups);
  flowie_control_auth_http_free_value(principal_json);
  turbo_free_json(&document);
  return rc;
}

static int flowie_control_auth_http_encode_denied(char **body_out, size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  int rc = TURBO_ENOMEM;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!body_out || !body_size_out) return TURBO_EINVAL;
  document = (turbo_json_doc_t *)turbo_json_create_object();
  if (!document) return TURBO_ENOMEM;
  if (flowie_control_auth_http_add(document, "version",
                                   turbo_json_create_uint64(
                                       FLOWIE_CONTROL_AUTH_HTTP_PROTOCOL_VERSION)) == TURBO_OK &&
      flowie_control_auth_http_add(document, "authenticated", turbo_json_create_bool(false)) ==
          TURBO_OK) {
    *body_out = turbo_json_serialize(document, body_size_out);
    if (*body_out) rc = TURBO_OK;
  }
  turbo_free_json(&document);
  return rc;
}

static int flowie_control_auth_http_header(const Req *req, const char *name,
                                           const char **value_out) {
  const char *found = NULL;
  size_t matches = 0u;
  if (value_out) *value_out = NULL;
  if (!req || !name || !value_out) return TURBO_EINVAL;
  if (req->headers.count > 0 && !req->headers.items) return TURBO_EPROTO;
  for (int index = 0; index < req->headers.count; ++index) {
    const request_item_t *item = &req->headers.items[index];
    if (item->key && flowie_control_auth_http_ascii_equal(item->key, name)) {
      ++matches;
      found = item->value;
    }
  }
  if (matches != 1u || !found) return TURBO_EPROTO;
  *value_out = found;
  return TURBO_OK;
}

static void flowie_control_auth_http_wipe_header(Req *req, const char *name) {
  if (!req || !name || req->headers.count <= 0 || !req->headers.items) return;
  for (int index = 0; index < req->headers.count; ++index) {
    request_item_t *item = &req->headers.items[index];
    if (item->key && item->value && flowie_control_auth_http_ascii_equal(item->key, name)) {
      size_t size = strnlen(item->value, FLOWIE_CONTROL_AUTH_HTTP_AUTHORIZATION_MAX + 1u);
      if (size <= FLOWIE_CONTROL_AUTH_HTTP_AUTHORIZATION_MAX) crypto_wipe(item->value, size);
    }
  }
}

static int flowie_control_auth_http_verify_token(flowie_control_auth_iris_endpoint_t *endpoint,
                                                 const Req *req) {
  static const char prefix[] = "Bearer ";
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  const char *authorization = NULL;
  uint8_t expected[FLOWIE_CONTROL_AUTH_HTTP_DIGEST_SIZE];
  uint8_t actual[FLOWIE_CONTROL_AUTH_HTTP_DIGEST_SIZE];
  size_t authorization_size;
  int rc;

  memset(expected, 0, sizeof(expected));
  memset(actual, 0, sizeof(actual));
  rc = flowie_control_auth_http_header(req, "Authorization", &authorization);
  if (rc != TURBO_OK) return TURBO_EPERM;
  rc = turbo_flow_security_secret_acquire(&endpoint->key_provider,
                                          endpoint->service_token_ref, &lease);
  if (rc != TURBO_OK) return rc;
  authorization_size =
      strnlen(authorization, FLOWIE_CONTROL_AUTH_HTTP_AUTHORIZATION_MAX + 1u);
  if (authorization_size > FLOWIE_CONTROL_AUTH_HTTP_AUTHORIZATION_MAX) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (!lease.bytes || lease.byte_count == 0u ||
      lease.byte_count > FLOWIE_CONTROL_AUTH_HTTP_TOKEN_MAX ||
      memchr(lease.bytes, '\0', lease.byte_count) || memchr(lease.bytes, '\r', lease.byte_count) ||
      memchr(lease.bytes, '\n', lease.byte_count) ||
      authorization_size != sizeof(prefix) - 1u + lease.byte_count ||
      memcmp(authorization, prefix, sizeof(prefix) - 1u) != 0) {
    rc = TURBO_EPERM;
    goto done;
  }
  crypto_blake2b(expected, sizeof(expected), lease.bytes, lease.byte_count);
  crypto_blake2b(actual, sizeof(actual),
                 (const uint8_t *)authorization + sizeof(prefix) - 1u, lease.byte_count);
  rc = crypto_verify32(expected, actual) == 0 ? TURBO_OK : TURBO_EPERM;

done:
  crypto_wipe(actual, sizeof(actual));
  crypto_wipe(expected, sizeof(expected));
  turbo_flow_security_secret_release(&endpoint->key_provider, &lease);
  return rc;
}

static int flowie_control_auth_http_response_status(int rc) {
  if (rc == TURBO_EPERM) return FORBIDDEN;
  if (rc == TURBO_EBUSY) return TOO_MANY_REQUESTS;
  return SERVICE_UNAVAILABLE;
}

int flowie_control_auth_iris_endpoint_process(flowie_control_auth_iris_endpoint_t *endpoint,
                                              Req *req, int *status_out, char **body_out,
                                              size_t *body_size_out) {
  flowie_control_auth_http_request_t request;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  const char *content_type = NULL;
  int status = BAD_REQUEST;
  int rc = TURBO_EPROTO;

  memset(&request, 0, sizeof(request));
  if (status_out) *status_out = INTERNAL_SERVER_ERROR;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!req || !status_out || !body_out || !body_size_out) return TURBO_EINVAL;
  if (!endpoint) {
    if (req->body && req->body_len > 0u) crypto_wipe(req->body, req->body_len);
    flowie_control_auth_http_wipe_header(req, "Authorization");
    return TURBO_EINVAL;
  }
  if (!req->method || strcmp(req->method, "POST") != 0 || req->body_stream || !req->body ||
      req->body_len == 0u || req->body_len > endpoint->max_request_body_size ||
      flowie_control_auth_http_header(req, "Content-Type", &content_type) != TURBO_OK ||
      !flowie_control_auth_http_ascii_equal(content_type, "application/json"))
    goto done;
  rc = flowie_control_auth_http_decode_request(req->body, req->body_len,
                                               endpoint->max_secret_size, &request);
  if (rc != TURBO_OK) {
    if (rc == TURBO_ENOMEM) status = SERVICE_UNAVAILABLE;
    goto done;
  }
  rc = flowie_control_auth_http_verify_token(endpoint, req);
  if (rc != TURBO_OK) {
    status = flowie_control_auth_http_response_status(rc);
    goto done;
  }
  rc = flowie_control_auth_iris_adapter_authenticate(
      endpoint->adapter, req, request.identity, request.method, request.secret,
      request.secret_size, &principal, NULL);
  if (rc != TURBO_OK) {
    status = flowie_control_auth_http_response_status(rc);
    goto done;
  }
  rc = flowie_control_auth_http_encode_principal(&principal, body_out, body_size_out);
  status = rc == TURBO_OK ? OK : SERVICE_UNAVAILABLE;

done:
  flowie_control_auth_http_request_clear(&request);
  crypto_wipe(&principal, sizeof(principal));
  if (req->body && req->body_len > 0u) crypto_wipe(req->body, req->body_len);
  flowie_control_auth_http_wipe_header(req, "Authorization");
  if (rc != TURBO_OK && !*body_out) {
    int encode_rc = flowie_control_auth_http_encode_denied(body_out, body_size_out);
    if (encode_rc != TURBO_OK) return encode_rc;
  }
  *status_out = status;
  return TURBO_OK;
}

void flowie_control_auth_iris_endpoint_handle(flowie_control_auth_iris_endpoint_t *endpoint,
                                              Req *req, Res *res) {
  static const char internal_error[] =
      "{\"version\":2,\"authenticated\":false}";
  char *body = NULL;
  size_t body_size = 0u;
  int status = INTERNAL_SERVER_ERROR;
  if (!res) return;
  set_header(res, "Cache-Control", "no-store");
  set_header(res, "Pragma", "no-cache");
  if (flowie_control_auth_iris_endpoint_process(endpoint, req, &status, &body,
                                                &body_size) != TURBO_OK) {
    reply(res, INTERNAL_SERVER_ERROR, "application/json", internal_error,
          sizeof(internal_error) - 1u);
    return;
  }
  reply(res, status, "application/json", body, body_size);
  turbo_json_serialize_free(body);
}

static void flowie_control_auth_iris_endpoint_unbound(void *context, void *user_data) {
  flowie_control_auth_iris_endpoint_t *endpoint =
      (flowie_control_auth_iris_endpoint_t *)context;
  iris_app_t *app = (iris_app_t *)user_data;
  if (endpoint && (!app || endpoint->bound_app == app)) endpoint->bound_app = NULL;
}

static void flowie_control_auth_iris_registered_handler(Req *req, Res *res) {
  flowie_control_auth_iris_endpoint_t *endpoint = NULL;
  if (req && req->app && req->path && strcmp(req->path, FLOWIE_CONTROL_AUTH_HTTP_PATH) == 0)
    endpoint = (flowie_control_auth_iris_endpoint_t *)iris_app_lookup_rpc_context(
        req->app, FLOWIE_CONTROL_AUTH_HTTP_PATH);
  flowie_control_auth_iris_endpoint_handle(endpoint, req, res);
}

int flowie_control_auth_iris_endpoint_create(
    const flowie_control_auth_iris_endpoint_config_t *config,
    flowie_control_auth_iris_endpoint_t **out) {
  flowie_control_auth_iris_endpoint_t *endpoint;
  size_t reference_size;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || !config->adapter ||
      !flowie_control_auth_http_text_valid(config->service_token_ref,
                                           FLOWIE_CONTROL_AUTH_HTTP_TOKEN_REFERENCE_MAX) ||
      config->key_provider.size < sizeof(config->key_provider) ||
      !config->key_provider.acquire || !config->key_provider.release ||
      config->max_request_body_size == 0u ||
      config->max_request_body_size > FLOWIE_CONTROL_AUTH_HTTP_ABSOLUTE_REQUEST_BODY_MAX ||
      config->max_secret_size == 0u ||
      config->max_secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX)
    return TURBO_EINVAL;
  endpoint = (flowie_control_auth_iris_endpoint_t *)calloc(1u, sizeof(*endpoint));
  if (!endpoint) return TURBO_ENOMEM;
  reference_size = strlen(config->service_token_ref);
  endpoint->service_token_ref = (char *)malloc(reference_size + 1u);
  if (!endpoint->service_token_ref) {
    free(endpoint);
    return TURBO_ENOMEM;
  }
  memcpy(endpoint->service_token_ref, config->service_token_ref, reference_size + 1u);
  endpoint->adapter = config->adapter;
  endpoint->key_provider = config->key_provider;
  endpoint->max_request_body_size = config->max_request_body_size;
  endpoint->max_secret_size = config->max_secret_size;
  *out = endpoint;
  return TURBO_OK;
}

void flowie_control_auth_iris_endpoint_destroy(flowie_control_auth_iris_endpoint_t *endpoint) {
  if (!endpoint) return;
  if (endpoint->bound_app)
    (void)iris_app_unbind_rpc_context(endpoint->bound_app,
                                      FLOWIE_CONTROL_AUTH_HTTP_PATH, endpoint);
  if (endpoint->service_token_ref) {
    memset(endpoint->service_token_ref, 0, strlen(endpoint->service_token_ref));
    free(endpoint->service_token_ref);
  }
  crypto_wipe(endpoint, sizeof(*endpoint));
  free(endpoint);
}

int flowie_control_auth_iris_endpoint_register(flowie_control_auth_iris_endpoint_t *endpoint,
                                               iris_app_t *app) {
  if (!endpoint || !app || endpoint->bound_app) return TURBO_EINVAL;
  if (iris_app_bind_rpc_context_ex(app, FLOWIE_CONTROL_AUTH_HTTP_PATH, endpoint,
                                   flowie_control_auth_iris_endpoint_unbound, app) != 0)
    return TURBO_EBUSY;
  endpoint->bound_app = app;
  iris_app_post(app, FLOWIE_CONTROL_AUTH_HTTP_PATH,
                flowie_control_auth_iris_registered_handler);
  return TURBO_OK;
}
