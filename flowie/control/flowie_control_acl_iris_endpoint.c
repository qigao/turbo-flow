#include "flowie_control_acl_iris_endpoint_internal.h"

#include "flowie.h"
#include "monocypher.h"
#include "platform.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <stdlib.h>
#include <string.h>

enum { FLOWIE_CONTROL_ACL_TOKEN_MAX = 4096 };

struct flowie_control_acl_iris_endpoint_s {
  const flowie_control_repository_t *repository;
  flowie_control_service_credential_resolver_t *service_credentials;
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

static int flowie_control_acl_resolve_caller(
    flowie_control_acl_iris_endpoint_t *endpoint, const Req *req,
    flowie_control_verified_caller_t *caller_out) {
  static const char prefix[] = "Bearer ";
  const char *authorization = NULL;
  const char *service_domain = NULL;
  const char *service_id = NULL;
  size_t authorization_size;
  if (!endpoint || !caller_out || caller_out->size < sizeof(*caller_out)) return TURBO_EINVAL;
  if (flowie_control_acl_header(req, "Authorization", &authorization) != TURBO_OK ||
      flowie_control_acl_header(req, "X-Flowie-Service-Domain", &service_domain) != TURBO_OK ||
      flowie_control_acl_header(req, "X-Flowie-Service-Id", &service_id) != TURBO_OK)
    return TURBO_EPERM;
  authorization_size = strnlen(authorization, sizeof(prefix) + FLOWIE_CONTROL_ACL_TOKEN_MAX);
  if (authorization_size <= sizeof(prefix) - 1u ||
      authorization_size > sizeof(prefix) - 1u + FLOWIE_CONTROL_ACL_TOKEN_MAX ||
      memcmp(authorization, prefix, sizeof(prefix) - 1u) != 0)
    return TURBO_EPERM;
  return flowie_control_service_credential_resolve(
      endpoint->service_credentials, service_domain, service_id,
      (const uint8_t *)authorization + sizeof(prefix) - 1u,
      authorization_size - (sizeof(prefix) - 1u), FLOWIE_CONTROL_SERVICE_ACL_CHECK, caller_out);
}

static int flowie_control_acl_json_fields_exact(const json_value_t *object,
                                                const char *const *allowed,
                                                size_t allowed_count) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT ||
      turbo_json_object_size(object) != allowed_count)
    return TURBO_EPROTO;
  for (size_t index = 0u; index < turbo_json_object_size(object); ++index) {
    const char *field = turbo_json_object_key(object, index);
    int known = 0;
    for (size_t allowed_index = 0u; allowed_index < allowed_count; ++allowed_index)
      if (field && strcmp(field, allowed[allowed_index]) == 0) known = 1;
    if (!known) return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flowie_control_acl_json_u64(const json_value_t *value, uint64_t *out) {
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

static int flowie_control_acl_copy_string(const json_value_t *object, const char *field,
                                          char *output, size_t capacity) {
  json_value_t *value = turbo_json_object_get(object, field);
  const char *text;
  size_t size;
  if (!value || turbo_json_type(value) != TURBO_JSON_STRING || !output || capacity == 0u)
    return TURBO_EPROTO;
  text = turbo_json_string(value);
  size = turbo_json_string_len(value);
  if (!text || size == 0u || size >= capacity || memchr(text, '\0', size)) return TURBO_EPROTO;
  memcpy(output, text, size);
  output[size] = '\0';
  return TURBO_OK;
}

static int flowie_control_acl_copy_array(const json_value_t *object, const char *field,
                                         char *output, size_t stride, size_t maximum_size,
                                         uint32_t maximum_count, uint32_t *count_out) {
  json_value_t *array = turbo_json_object_get(object, field);
  size_t count;
  if (!array || turbo_json_type(array) != TURBO_JSON_ARRAY || !output || !count_out)
    return TURBO_EPROTO;
  count = turbo_json_array_size(array);
  if (count > maximum_count) return TURBO_EPROTO;
  for (size_t index = 0u; index < count; ++index) {
    json_value_t *item = turbo_json_array_get(array, index);
    const char *text;
    size_t size;
    if (!item || turbo_json_type(item) != TURBO_JSON_STRING || !(text = turbo_json_string(item)) ||
        (size = turbo_json_string_len(item)) == 0u || size > maximum_size ||
        memchr(text, '\0', size))
      return TURBO_EPROTO;
    memcpy(output + index * stride, text, size);
    output[index * stride + size] = '\0';
  }
  *count_out = (uint32_t)count;
  return TURBO_OK;
}

static int flowie_control_acl_decode_principal(const json_value_t *value,
                                               turbo_flow_security_principal_t *principal) {
  static const char *const fields[] = {"id",      "type",   "domain", "expires_at",
                                       "policy_version", "roles", "groups"};
  uint64_t expires_at = 0u;
  uint64_t policy_version = 0u;
  if (!principal || flowie_control_acl_json_fields_exact(
                        value, fields, sizeof(fields) / sizeof(fields[0])) != TURBO_OK)
    return TURBO_EPROTO;
  *principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  if (flowie_control_acl_copy_string(value, "id", principal->principal_id,
                                     sizeof(principal->principal_id)) != TURBO_OK ||
      flowie_control_acl_copy_string(value, "type", principal->principal_type,
                                     sizeof(principal->principal_type)) != TURBO_OK ||
      flowie_control_acl_copy_string(value, "domain", principal->domain_id,
                                     sizeof(principal->domain_id)) != TURBO_OK ||
      flowie_control_acl_json_u64(turbo_json_object_get(value, "expires_at"), &expires_at) !=
          TURBO_OK ||
      flowie_control_acl_json_u64(turbo_json_object_get(value, "policy_version"),
                                  &policy_version) != TURBO_OK ||
      policy_version == 0u ||
      flowie_control_acl_copy_array(value, "roles", (char *)principal->roles,
                                    sizeof(principal->roles[0]), TURBO_FLOW_SECURITY_TYPE_MAX,
                                    TURBO_FLOW_SECURITY_MAX_ROLES,
                                    &principal->role_count) != TURBO_OK ||
      flowie_control_acl_copy_array(value, "groups", (char *)principal->groups,
                                    sizeof(principal->groups[0]), TURBO_FLOW_SECURITY_ID_MAX,
                                    TURBO_FLOW_SECURITY_MAX_GROUPS,
                                    &principal->group_count) != TURBO_OK)
    return TURBO_EPROTO;
  memcpy(principal->auth_method, "password", sizeof("password"));
  principal->scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
  principal->expires_at = expires_at;
  principal->policy_version = policy_version;
  return TURBO_OK;
}

static int flowie_control_acl_decode_request(
    const char *body, size_t body_size, turbo_json_doc_t **document_out,
    turbo_flow_security_principal_t *principal_out, turbo_flow_security_request_t *request_out,
    flowie_mqtt_security_context_t *mqtt_out) {
  static const char *const fields[] = {"version", "access", "topic", "username", "client_id",
                                       "principal"};
  turbo_json_doc_t *document = NULL;
  json_value_t *access;
  json_value_t *topic;
  json_value_t *username;
  json_value_t *client_id;
  uint64_t version = 0u;
  int rc = TURBO_EPROTO;
  if (document_out) *document_out = NULL;
  if (!body || body_size == 0u || !document_out || !principal_out || !request_out || !mqtt_out)
    return TURBO_EINVAL;
  if (turbo_parse_json((const uint8_t *)body, body_size, &document) != TURBO_OK || !document)
    return TURBO_EPROTO;
  access = turbo_json_object_get(document, "access");
  topic = turbo_json_object_get(document, "topic");
  username = turbo_json_object_get(document, "username");
  client_id = turbo_json_object_get(document, "client_id");
  if (flowie_control_acl_json_fields_exact(document, fields,
                                           sizeof(fields) / sizeof(fields[0])) != TURBO_OK ||
      flowie_control_acl_json_u64(turbo_json_object_get(document, "version"), &version) !=
          TURBO_OK ||
      version != FLOWIE_CONTROL_ACL_HTTP_PROTOCOL_VERSION || !access ||
      turbo_json_type(access) != TURBO_JSON_STRING || !topic ||
      turbo_json_type(topic) != TURBO_JSON_STRING || turbo_json_string_len(topic) == 0u ||
      turbo_json_string_len(topic) > FLOWIE_CONTROL_ACL_HTTP_DEFAULT_RESPONSE_MAX || !username ||
      turbo_json_type(username) != TURBO_JSON_STRING ||
      turbo_json_string_len(username) > TURBO_FLOW_SECURITY_ID_MAX || !client_id ||
      turbo_json_type(client_id) != TURBO_JSON_STRING ||
      turbo_json_string_len(client_id) > TURBO_FLOW_SECURITY_ID_MAX ||
      flowie_control_acl_decode_principal(turbo_json_object_get(document, "principal"),
                                          principal_out) != TURBO_OK)
    goto done;
  *request_out = (turbo_flow_security_request_t)TURBO_FLOW_SECURITY_REQUEST_INIT;
  request_out->principal = principal_out;
  request_out->domain_id = principal_out->domain_id;
  request_out->resource = turbo_json_string(topic);
  if (strcmp(turbo_json_string(access), "connect") == 0) {
    request_out->action = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    request_out->resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
  } else if (strcmp(turbo_json_string(access), "read") == 0 ||
             strcmp(turbo_json_string(access), "write") == 0) {
    request_out->action = strcmp(turbo_json_string(access), "read") == 0
                              ? TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE
                              : TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request_out->resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    *mqtt_out = (flowie_mqtt_security_context_t)FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
    mqtt_out->kind = request_out->action == TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE
                         ? FLOWIE_MQTT_SECURITY_TOPIC_FILTER
                         : FLOWIE_MQTT_SECURITY_TOPIC;
    mqtt_out->username =
        (flowie_mqtt_span_t){(const uint8_t *)turbo_json_string(username),
                             turbo_json_string_len(username)};
    mqtt_out->client_id =
        (flowie_mqtt_span_t){(const uint8_t *)turbo_json_string(client_id),
                             turbo_json_string_len(client_id)};
    request_out->protocol_context = mqtt_out;
  } else {
    goto done;
  }
  rc = TURBO_OK;
  *document_out = document;
  document = NULL;

done:
  turbo_free_json(&document);
  return rc;
}

static const char *flowie_control_acl_reason(turbo_flow_security_decision_reason_t reason) {
  switch (reason) {
  case TURBO_FLOW_SECURITY_REASON_ALLOW_RULE: return "allow_rule";
  case TURBO_FLOW_SECURITY_REASON_DENY_RULE: return "deny_rule";
  case TURBO_FLOW_SECURITY_REASON_DOMAIN_MISMATCH: return "domain_mismatch";
  case TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED: return "principal_expired";
  case TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH: return "policy_version_mismatch";
  default: return "default_deny";
  }
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

static int flowie_control_acl_encode_decision(const turbo_flow_security_decision_t *decision,
                                              char **body_out, size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  int rc = TURBO_ENOMEM;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!decision || decision->size < sizeof(*decision) || decision->policy_version == 0u ||
      !body_out || !body_size_out)
    return TURBO_EINVAL;
  document = (turbo_json_doc_t *)turbo_json_create_object();
  if (!document) return TURBO_ENOMEM;
  if (flowie_control_acl_add(document, "version",
                             turbo_json_create_uint64(FLOWIE_CONTROL_ACL_HTTP_PROTOCOL_VERSION)) !=
          TURBO_OK ||
      flowie_control_acl_add(document, "allowed",
                             turbo_json_create_bool(decision->effect ==
                                                    TURBO_FLOW_SECURITY_ALLOW)) != TURBO_OK ||
      flowie_control_acl_add(document, "reason",
                             turbo_json_create_string(flowie_control_acl_reason(decision->reason))) !=
          TURBO_OK ||
      flowie_control_acl_add(document, "policy_version",
                             turbo_json_create_uint64(decision->policy_version)) != TURBO_OK)
    goto done;
  *body_out = turbo_json_serialize(document, body_size_out);
  if (!*body_out) goto done;
  rc = TURBO_OK;
done:
  turbo_free_json(&document);
  return rc;
}

static int flowie_control_acl_status(int rc) {
  if (rc == TURBO_EPERM) return FORBIDDEN;
  if (rc == TURBO_EPROTO || rc == TURBO_EINVAL) return BAD_REQUEST;
  return SERVICE_UNAVAILABLE;
}

int flowie_control_acl_iris_endpoint_process(flowie_control_acl_iris_endpoint_t *endpoint, Req *req,
                                             int *status_out, char **body_out,
                                             size_t *body_size_out) {
  turbo_json_doc_t *document = NULL;
  turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  flowie_control_policy_status_t policy = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
  flowie_mqtt_security_context_t mqtt = FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
  turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
  turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
  turbo_flow_security_realm_t *realm = NULL;
  turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
  flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
  const char *content_type = NULL;
  uint64_t now;
  int rc = TURBO_EPROTO;
  if (status_out) *status_out = INTERNAL_SERVER_ERROR;
  if (body_out) *body_out = NULL;
  if (body_size_out) *body_size_out = 0u;
  if (!endpoint || !req || !status_out || !body_out || !body_size_out) return TURBO_EINVAL;
  if (!req->method || strcmp(req->method, "POST") != 0 || req->body_stream || !req->body ||
      req->body_len == 0u || req->body_len > endpoint->max_response_size ||
      flowie_control_acl_header(req, "Content-Type", &content_type) != TURBO_OK ||
      !flowie_control_acl_ascii_equal(content_type, "application/json"))
    goto done;
  rc = flowie_control_acl_resolve_caller(endpoint, req, &caller);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_acl_decode_request(req->body, req->body_len, &document, &principal, &request,
                                         &mqtt);
  if (rc != TURBO_OK) goto done;
  now = turbo_realtime_ms() / 1000u;
  if (now == 0u) {
    rc = TURBO_EIO;
    goto done;
  }
  decision.policy_version = principal.policy_version;
  if (principal.expires_at != 0u && now >= principal.expires_at) {
    decision.reason = TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED;
    rc = flowie_control_acl_encode_decision(&decision, body_out, body_size_out);
    goto done;
  }
  rc = endpoint->repository->policy->status(endpoint->repository->ctx, principal.domain_id,
                                            &policy);
  if (rc != TURBO_OK) goto done;
  if (policy.policy_version != principal.policy_version ||
      (policy.expires_at != 0u && now >= policy.expires_at)) {
    decision.reason = TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH;
    rc = flowie_control_acl_encode_decision(&decision, body_out, body_size_out);
    goto done;
  }
  rc = endpoint->repository->policy->bundle_load(endpoint->repository->ctx, principal.domain_id,
                                                 principal.policy_version, &bundle);
  if (rc != TURBO_OK) goto done;
  rc = flowie_mqtt_security_matcher_init(&matcher);
  if (rc != TURBO_OK) goto done;
  realm_config.resource_uid = "flowie-control-acl-check";
  realm_config.owner_name = "flowie-control";
  realm_config.policy_version = bundle.policy_version;
  realm_config.rules = bundle.rules;
  realm_config.rule_count = bundle.rule_count;
  realm_config.matcher = matcher;
  rc = turbo_flow_security_realm_create(&realm_config, &realm);
  if (rc != TURBO_OK) goto done;
  rc = turbo_flow_security_realm_evaluate(realm, &request, now, &decision);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_acl_encode_decision(&decision, body_out, body_size_out);

done:
  turbo_flow_security_realm_destroy(realm);
  if (bundle.provider_bundle)
    endpoint->repository->policy->bundle_release(endpoint->repository->ctx, &bundle);
  turbo_free_json(&document);
  flowie_control_acl_wipe_authorization(req);
  *status_out = rc == TURBO_OK ? OK : flowie_control_acl_status(rc);
  return TURBO_OK;
}

static void flowie_control_acl_handle(flowie_control_acl_iris_endpoint_t *endpoint, Req *req,
                                      Res *res) {
  static const char unavailable[] = "{\"version\":4,\"error\":\"unavailable\"}";
  char *body = NULL;
  size_t body_size = 0u;
  int status = INTERNAL_SERVER_ERROR;
  if (!res) return;
  set_header(res, "Cache-Control", "no-store");
  if (flowie_control_acl_iris_endpoint_process(endpoint, req, &status, &body, &body_size) !=
      TURBO_OK || !body) {
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
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      flowie_control_repository_validate(config->repository) != TURBO_OK ||
      !config->service_credentials || config->max_response_size == 0u || !out)
    return TURBO_EINVAL;
  endpoint = (flowie_control_acl_iris_endpoint_t *)calloc(1u, sizeof(*endpoint));
  if (!endpoint) return TURBO_ENOMEM;
  endpoint->repository = config->repository;
  endpoint->service_credentials = config->service_credentials;
  endpoint->max_response_size = config->max_response_size;
  *out = endpoint;
  return TURBO_OK;
}

void flowie_control_acl_iris_endpoint_destroy(flowie_control_acl_iris_endpoint_t *endpoint) {
  if (!endpoint) return;
  if (endpoint->bound_app)
    (void)iris_app_unbind_rpc_context(endpoint->bound_app, FLOWIE_CONTROL_ACL_HTTP_PATH, endpoint);
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
  iris_app_post(app, FLOWIE_CONTROL_ACL_HTTP_PATH, flowie_control_acl_registered_handler);
  return TURBO_OK;
}
