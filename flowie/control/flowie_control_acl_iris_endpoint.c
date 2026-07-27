#include "flowie_control_acl_iris_endpoint_internal.h"

#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWIE_CONTROL_ACL_TOKEN_MAX = 4096,
  FLOWIE_CONTROL_ACL_DIGEST_SIZE = 32,
  FLOWIE_CONTROL_ACL_VERSION_HEADER_MAX = 32,
  FLOWIE_CONTROL_ACL_SECRET_REF_MAX = 1024
};

struct flowie_control_acl_iris_endpoint_s {
  const flowie_control_repository_t *repository;
  const flowie_control_auth_service_t *auth_service;
  turbo_flow_security_key_provider_t key_provider;
  char listener_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char *service_token_ref;
  size_t max_response_size;
  iris_app_t *bound_app;
};

static int flowie_control_acl_ascii_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
  while (*left && *right) {
    unsigned char a = (unsigned char)*left++;
    unsigned char b = (unsigned char)*right++;
    if (a >= 'A' && a <= 'Z') a = (unsigned char)(a - 'A' + 'a');
    if (b >= 'A' && b <= 'Z') b = (unsigned char)(b - 'A' + 'a');
    if (a != b) return 0;
  }
  return *left == '\0' && *right == '\0';
}

static int flowie_control_acl_header(const Req *req, const char *name, const char **value_out) {
  const char *found = NULL;
  size_t matches = 0u;
  if (value_out) *value_out = NULL;
  if (!req || !name || !value_out || req->headers.count < 0 ||
      (req->headers.count > 0 && !req->headers.items))
    return TURBO_EINVAL;
  for (int index = 0; index < req->headers.count; ++index) {
    const request_item_t *item = &req->headers.items[index];
    if (item->key && item->value && flowie_control_acl_ascii_equal(item->key, name)) {
      ++matches;
      found = item->value;
    }
  }
  if (matches == 0u) return TURBO_ENOENT;
  if (matches != 1u || !found) return TURBO_EPROTO;
  *value_out = found;
  return TURBO_OK;
}

static void flowie_control_acl_wipe_authorization(Req *req) {
  if (!req || req->headers.count <= 0 || !req->headers.items) return;
  for (int index = 0; index < req->headers.count; ++index) {
    request_item_t *item = &req->headers.items[index];
    if (item->key && item->value && flowie_control_acl_ascii_equal(item->key, "Authorization")) {
      size_t size = strnlen(item->value, sizeof("Bearer ") + FLOWIE_CONTROL_ACL_TOKEN_MAX);
      if (size < sizeof("Bearer ") + FLOWIE_CONTROL_ACL_TOKEN_MAX) crypto_wipe(item->value, size);
    }
  }
}

static int flowie_control_acl_verify_token(flowie_control_acl_iris_endpoint_t *endpoint,
                                           const Req *req) {
  static const char prefix[] = "Bearer ";
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  const char *authorization = NULL;
  uint8_t expected[FLOWIE_CONTROL_ACL_DIGEST_SIZE] = {0};
  uint8_t actual[FLOWIE_CONTROL_ACL_DIGEST_SIZE] = {0};
  size_t authorization_size;
  int rc = flowie_control_acl_header(req, "Authorization", &authorization);
  if (rc != TURBO_OK) return TURBO_EPERM;
  rc = turbo_flow_security_secret_acquire(&endpoint->key_provider, endpoint->service_token_ref,
                                          &lease);
  if (rc != TURBO_OK) return rc;
  authorization_size = strnlen(authorization, sizeof(prefix) + FLOWIE_CONTROL_ACL_TOKEN_MAX);
  if (!lease.bytes || lease.byte_count == 0u || lease.byte_count > FLOWIE_CONTROL_ACL_TOKEN_MAX ||
      authorization_size != sizeof(prefix) - 1u + lease.byte_count ||
      memcmp(authorization, prefix, sizeof(prefix) - 1u) != 0 ||
      memchr(lease.bytes, '\0', lease.byte_count) || memchr(lease.bytes, '\r', lease.byte_count) ||
      memchr(lease.bytes, '\n', lease.byte_count)) {
    rc = TURBO_EPERM;
    goto done;
  }
  crypto_blake2b(expected, sizeof(expected), lease.bytes, lease.byte_count);
  crypto_blake2b(actual, sizeof(actual), (const uint8_t *)authorization + sizeof(prefix) - 1u,
                 lease.byte_count);
  rc = crypto_verify32(expected, actual) == 0 ? TURBO_OK : TURBO_EPERM;

done:
  crypto_wipe(expected, sizeof(expected));
  crypto_wipe(actual, sizeof(actual));
  turbo_flow_security_secret_release(&endpoint->key_provider, &lease);
  return rc;
}

static int flowie_control_acl_required_version(const Req *req, uint64_t *version_out) {
  const char *value = NULL;
  char *end = NULL;
  unsigned long long parsed;
  size_t size;
  int rc;
  if (!version_out) return TURBO_EINVAL;
  *version_out = 0u;
  rc = flowie_control_acl_header(req, "X-TurboFlow-Policy-Version", &value);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc != TURBO_OK) return rc;
  size = strnlen(value, FLOWIE_CONTROL_ACL_VERSION_HEADER_MAX + 1u);
  if (size == 0u || size > FLOWIE_CONTROL_ACL_VERSION_HEADER_MAX || value[0] == '+' ||
      value[0] == '-' || (size > 1u && value[0] == '0'))
    return TURBO_EPROTO;
  errno = 0;
  parsed = strtoull(value, &end, 10);
  if (errno == ERANGE || !end || *end != '\0' || parsed == 0u) return TURBO_EPROTO;
  *version_out = (uint64_t)parsed;
  return TURBO_OK;
}

static void flowie_control_acl_free_value(json_value_t *value) {
  turbo_json_doc_t *owned = (turbo_json_doc_t *)value;
  if (owned) turbo_free_json(&owned);
}

static int flowie_control_acl_add(json_value_t *object, const char *field, json_value_t *value) {
  if (value && turbo_json_object_add_checked(object, field, value)) return TURBO_OK;
  flowie_control_acl_free_value(value);
  return TURBO_ENOMEM;
}

static int flowie_control_acl_array_add(json_value_t *array, json_value_t *value) {
  if (value && turbo_json_array_add_checked(array, value)) return TURBO_OK;
  flowie_control_acl_free_value(value);
  return TURBO_ENOMEM;
}

static int flowie_control_acl_encode_bundle(const turbo_flow_security_policy_bundle_t *bundle,
                                            size_t max_response_size, char **body_out,
                                            size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  json_value_t *rules = NULL;
  int rc = TURBO_ENOMEM;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!bundle || bundle->size < sizeof(*bundle) ||
      bundle->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || bundle->policy_version == 0u ||
      !bundle->rules || bundle->rule_count == 0u ||
      bundle->rule_count > TURBO_FLOW_SECURITY_MAX_RULES || max_response_size == 0u || !body_out ||
      !body_size_out)
    return TURBO_EINVAL;
  document = (turbo_json_doc_t *)turbo_json_create_object();
  rules = turbo_json_create_array();
  if (!document || !rules) goto done;
  for (size_t index = 0u; index < bundle->rule_count; ++index) {
    char line[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u];
    size_t line_size = 0u;
    rc =
        turbo_flow_security_rule_format_line(&bundle->rules[index], line, sizeof(line), &line_size);
    if (rc != TURBO_OK || line_size >= sizeof(line)) {
      if (rc == TURBO_OK) rc = TURBO_EPROTO;
      goto done;
    }
    line[line_size] = '\0';
    rc = flowie_control_acl_array_add(rules, turbo_json_create_string(line));
    if (rc != TURBO_OK) goto done;
  }
  if (flowie_control_acl_add(document, "version",
                             turbo_json_create_uint64(FLOWIE_CONTROL_ACL_HTTP_PROTOCOL_VERSION)) !=
          TURBO_OK ||
      flowie_control_acl_add(document, "policy_version",
                             turbo_json_create_uint64(bundle->policy_version)) != TURBO_OK ||
      flowie_control_acl_add(document, "expires_at",
                             turbo_json_create_uint64(bundle->expires_at)) != TURBO_OK)
    goto done;
  if (flowie_control_acl_add(document, "rules", rules) != TURBO_OK) {
    rules = NULL;
    goto done;
  }
  rules = NULL;
  *body_out = turbo_json_serialize(document, body_size_out);
  if (!*body_out) goto done;
  if (*body_size_out > max_response_size) {
    turbo_json_serialize_free(*body_out);
    *body_out = NULL;
    *body_size_out = 0u;
    rc = TURBO_EFBIG;
    goto done;
  }
  rc = TURBO_OK;

done:
  flowie_control_acl_free_value(rules);
  turbo_free_json(&document);
  return rc;
}

static int flowie_control_acl_status(int rc) {
  if (rc == TURBO_EPERM) return FORBIDDEN;
  if (rc == TURBO_ENOENT) return NOT_FOUND;
  if (rc == TURBO_EPROTO || rc == TURBO_EINVAL) return BAD_REQUEST;
  return SERVICE_UNAVAILABLE;
}

int flowie_control_acl_iris_endpoint_process(flowie_control_acl_iris_endpoint_t *endpoint, Req *req,
                                             int *status_out, char **body_out,
                                             size_t *body_size_out) {
  turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
  char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u] = {0};
  uint64_t required_version = 0u;
  int rc = TURBO_EPROTO;
  if (status_out) *status_out = INTERNAL_SERVER_ERROR;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!endpoint || !req || !status_out || !body_out || !body_size_out) return TURBO_EINVAL;
  if (!req->method || strcmp(req->method, "GET") != 0 || req->body_stream || req->body_len != 0u)
    goto done;
  rc = flowie_control_acl_verify_token(endpoint, req);
  if (rc != TURBO_OK) goto done;
  rc = req_get_verified_tls_peer_certificate_sha256(req, fingerprint);
  if (rc != TURBO_OK) {
    rc = TURBO_EPERM;
    goto done;
  }
  caller.listener_id = endpoint->listener_id;
  caller.peer_certificate_sha256 = fingerprint;
  caller.certificate_verified = 1;
  rc = flowie_control_auth_service_resolve_root_group(endpoint->auth_service, &caller,
                                                      root_group_id);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_acl_required_version(req, &required_version);
  if (rc != TURBO_OK) goto done;
  rc = endpoint->repository->policy->bundle_load(endpoint->repository->ctx, root_group_id,
                                                 required_version, &bundle);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_acl_encode_bundle(&bundle, endpoint->max_response_size, body_out,
                                        body_size_out);

done:
  if (bundle.provider_bundle)
    endpoint->repository->policy->bundle_release(endpoint->repository->ctx, &bundle);
  crypto_wipe(fingerprint, sizeof(fingerprint));
  crypto_wipe(root_group_id, sizeof(root_group_id));
  flowie_control_acl_wipe_authorization(req);
  *status_out = rc == TURBO_OK ? OK : flowie_control_acl_status(rc);
  return TURBO_OK;
}

static void flowie_control_acl_handle(flowie_control_acl_iris_endpoint_t *endpoint, Req *req,
                                      Res *res) {
  static const char unavailable[] = "{\"version\":3,\"error\":\"unavailable\"}";
  char *body = NULL;
  size_t body_size = 0u;
  int status = INTERNAL_SERVER_ERROR;
  if (!res) return;
  set_header(res, "Cache-Control", "no-store");
  if (flowie_control_acl_iris_endpoint_process(endpoint, req, &status, &body, &body_size) !=
      TURBO_OK) {
    reply(res, INTERNAL_SERVER_ERROR, "application/json", unavailable, sizeof(unavailable) - 1u);
    return;
  }
  if (!body) {
    reply(res, status, "application/json", unavailable, sizeof(unavailable) - 1u);
    return;
  }
  reply(res, status, "application/json", body, body_size);
  turbo_json_serialize_free(body);
}

static void flowie_control_acl_unbound(void *context, void *user_data) {
  flowie_control_acl_iris_endpoint_t *endpoint = (flowie_control_acl_iris_endpoint_t *)context;
  iris_app_t *app = (iris_app_t *)user_data;
  if (endpoint && (!app || endpoint->bound_app == app)) endpoint->bound_app = NULL;
}

static void flowie_control_acl_registered_handler(Req *req, Res *res) {
  flowie_control_acl_iris_endpoint_t *endpoint = NULL;
  if (req && req->app)
    endpoint = (flowie_control_acl_iris_endpoint_t *)iris_app_lookup_rpc_context(
        req->app, FLOWIE_CONTROL_ACL_HTTP_PATH);
  flowie_control_acl_handle(endpoint, req, res);
}

int flowie_control_acl_iris_endpoint_create(const flowie_control_acl_iris_endpoint_config_t *config,
                                            flowie_control_acl_iris_endpoint_t **out) {
  flowie_control_acl_iris_endpoint_t *endpoint;
  size_t listener_size;
  size_t reference_size;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      flowie_control_repository_validate(config->repository) != TURBO_OK || !config->auth_service ||
      !config->listener_id || !config->listener_id[0] ||
      (listener_size = strlen(config->listener_id)) > TURBO_FLOW_SECURITY_ID_MAX ||
      !config->service_token_ref || !config->service_token_ref[0] ||
      (reference_size = strlen(config->service_token_ref)) > FLOWIE_CONTROL_ACL_SECRET_REF_MAX ||
      config->key_provider.size < sizeof(config->key_provider) || !config->key_provider.acquire ||
      !config->key_provider.release || config->max_response_size == 0u || !out)
    return TURBO_EINVAL;
  endpoint = (flowie_control_acl_iris_endpoint_t *)calloc(1u, sizeof(*endpoint));
  if (!endpoint) return TURBO_ENOMEM;
  endpoint->service_token_ref = (char *)malloc(reference_size + 1u);
  if (!endpoint->service_token_ref) {
    free(endpoint);
    return TURBO_ENOMEM;
  }
  memcpy(endpoint->service_token_ref, config->service_token_ref, reference_size + 1u);
  memcpy(endpoint->listener_id, config->listener_id, listener_size + 1u);
  endpoint->repository = config->repository;
  endpoint->auth_service = config->auth_service;
  endpoint->key_provider = config->key_provider;
  endpoint->max_response_size = config->max_response_size;
  *out = endpoint;
  return TURBO_OK;
}

void flowie_control_acl_iris_endpoint_destroy(flowie_control_acl_iris_endpoint_t *endpoint) {
  if (!endpoint) return;
  if (endpoint->bound_app)
    (void)iris_app_unbind_rpc_context(endpoint->bound_app, FLOWIE_CONTROL_ACL_HTTP_PATH, endpoint);
  if (endpoint->service_token_ref) {
    crypto_wipe(endpoint->service_token_ref, strlen(endpoint->service_token_ref));
    free(endpoint->service_token_ref);
  }
  crypto_wipe(endpoint, sizeof(*endpoint));
  free(endpoint);
}

int flowie_control_acl_iris_endpoint_register(flowie_control_acl_iris_endpoint_t *endpoint,
                                              iris_app_t *app) {
  if (!endpoint || !app || endpoint->bound_app) return TURBO_EINVAL;
  if (iris_app_bind_rpc_context_ex(app, FLOWIE_CONTROL_ACL_HTTP_PATH, endpoint,
                                   flowie_control_acl_unbound, app) != 0)
    return TURBO_EBUSY;
  endpoint->bound_app = app;
  iris_app_get(app, FLOWIE_CONTROL_ACL_HTTP_PATH, flowie_control_acl_registered_handler);
  return TURBO_OK;
}
