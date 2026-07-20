#include "flowie_control_management_rpc_internal.h"

#include "base64_utils.h"
#include "flowie_control_credential_internal.h"
#include "turbo_error.h"
#include "turbo_parser.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWIE_CONTROL_RPC_AUTH_REQUIRED = -32001,
  FLOWIE_CONTROL_RPC_FORBIDDEN = -32003,
  FLOWIE_CONTROL_RPC_NOT_FOUND = -32004,
  FLOWIE_CONTROL_RPC_CONFLICT = -32009,
  FLOWIE_CONTROL_RPC_SECRET_UNAVAILABLE = -32010,
  FLOWIE_CONTROL_RPC_CREDENTIAL_BASE64_SIZE =
      4u * ((FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE + 2u) / 3u) + 1u,
  FLOWIE_CONTROL_RPC_DEFAULT_PAGE = 25
};

static const char *const FLOWIE_CONTROL_RPC_METHODS[] = {
    "flowie.system.status",       "flowie.user.get",        "flowie.user.list",
    "flowie.user.create",         "flowie.user.disable",    "flowie.group.list",
    "flowie.group.create",        "flowie.group.disable",   "flowie.group.member.add",
    "flowie.group.member.remove", "flowie.group.effective", "flowie.role.list",
    "flowie.role.create",         "flowie.role.disable",    "flowie.role.assign",
    "flowie.role.remove",         "flowie.role.effective",  "flowie.policy.status",
    "flowie.policy.rule.list",    "flowie.policy.rule.put", "flowie.policy.rule.delete",
    "flowie.policy.validate",     "flowie.policy.publish",  "flowie.audit.list",
    "flowie.credential.generate", "flowie.credential.rotate", "flowie.credential.revoke"};

struct flowie_control_management_rpc_server_s {
  flowie_control_management_service_t *service;
  rpc_context_t *rpc_context;
  flowie_control_management_rpc_resolve_caller_fn resolve_caller;
  void *resolve_caller_ctx;
  flowie_control_management_rpc_clock_fn clock;
  void *clock_ctx;
  iris_app_t *bound_app;
  size_t registered_method_count;
};

static void flowie_control_rpc_method(Req *request, Res *response);
static int flowie_control_rpc_registered_method(Req *request, Res *response,
                                                rpc_request_t *rpc_request,
                                                rpc_response_t *rpc_response);

static void flowie_control_rpc_free_json_value(json_value_t *value) {
  turbo_json_doc_t *owned = (turbo_json_doc_t *)value;
  if (owned) turbo_free_json(&owned);
}

static int flowie_control_rpc_add(json_value_t *object, const char *key, json_value_t *value) {
  if (value && turbo_json_object_add_checked(object, key, value)) return TURBO_OK;
  flowie_control_rpc_free_json_value(value);
  return TURBO_ENOMEM;
}

static int flowie_control_rpc_array_add(json_value_t *array, json_value_t *value) {
  if (value && turbo_json_array_add_checked(array, value)) return TURBO_OK;
  flowie_control_rpc_free_json_value(value);
  return TURBO_ENOMEM;
}

static int flowie_control_rpc_result(rpc_response_t *response, json_value_t *value) {
  char *json;
  size_t json_size = 0u;
  if (!response || !value) return TURBO_EINVAL;
  json = turbo_json_serialize(value, &json_size);
  flowie_control_rpc_free_json_value(value);
  if (!json || json_size == 0u) {
    turbo_json_serialize_free(json);
    return TURBO_ENOMEM;
  }
  rpc_set_result(response, json);
  turbo_json_serialize_free(json);
  return response->result ? TURBO_OK : TURBO_ENOMEM;
}

static int flowie_control_rpc_params(const rpc_request_t *request, const char *const *allowed,
                                     size_t allowed_count, turbo_json_doc_t **document_out) {
  turbo_json_doc_t *document = NULL;
  if (document_out) *document_out = NULL;
  if (!request || !document_out) return TURBO_EINVAL;
  if (!request->params) {
    document = (turbo_json_doc_t *)turbo_json_create_object();
  } else if (turbo_parse_json((const uint8_t *)request->params, strlen(request->params),
                              &document) != TURBO_OK ||
             !document || turbo_json_type(document) != TURBO_JSON_OBJECT) {
    turbo_free_json(&document);
    return TURBO_EPROTO;
  }
  if (!document) return TURBO_ENOMEM;
  for (size_t index = 0u; index < turbo_json_object_size(document); ++index) {
    const char *key = turbo_json_object_key(document, index);
    int known = 0;
    for (size_t allowed_index = 0u; allowed_index < allowed_count; ++allowed_index) {
      if (key && strcmp(key, allowed[allowed_index]) == 0) {
        known = 1;
        break;
      }
    }
    if (!known) {
      turbo_free_json(&document);
      return TURBO_EPROTO;
    }
  }
  *document_out = document;
  return TURBO_OK;
}

static int flowie_control_rpc_string(const json_value_t *object, const char *key, size_t maximum,
                                     int required, const char **out) {
  json_value_t *value;
  const char *text;
  size_t size;
  if (out) *out = NULL;
  if (!object || !key || maximum == 0u || !out) return TURBO_EINVAL;
  value = turbo_json_object_get(object, key);
  if (!value) return required ? TURBO_EPROTO : TURBO_OK;
  if (turbo_json_type(value) != TURBO_JSON_STRING) return TURBO_EPROTO;
  text = turbo_json_string(value);
  size = turbo_json_string_len(value);
  if (!text || size == 0u || size > maximum || memchr(text, '\0', size)) return TURBO_EPROTO;
  *out = text;
  return TURBO_OK;
}

static int flowie_control_rpc_u64(const json_value_t *object, const char *key, int required,
                                  uint64_t *out) {
  json_value_t *value;
  const char *text;
  char buffer[32];
  char *end = NULL;
  size_t size = 0u;
  unsigned long long parsed;
  if (!object || !key || !out) return TURBO_EINVAL;
  value = turbo_json_object_get(object, key);
  if (!value) return required ? TURBO_EPROTO : TURBO_OK;
  if (turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EPROTO;
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

static int flowie_control_rpc_page_limit(const json_value_t *params, size_t *limit_out) {
  uint64_t limit = FLOWIE_CONTROL_RPC_DEFAULT_PAGE;
  int rc = flowie_control_rpc_u64(params, "limit", 0, &limit);
  if (rc != TURBO_OK || limit == 0u || limit > FLOWIE_CONTROL_PAGE_MAX) return TURBO_EPROTO;
  *limit_out = (size_t)limit;
  return TURBO_OK;
}

static json_value_t *
flowie_control_rpc_command_result(const flowie_control_command_result_t *result) {
  json_value_t *object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "revision", turbo_json_create_uint64(result->revision)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "replayed", turbo_json_create_bool(result->replayed != 0)) !=
          TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return NULL;
  }
  return object;
}

static int flowie_control_rpc_error(rpc_response_t *response, int rc) {
  switch (rc) {
  case TURBO_EINVAL:
  case TURBO_EPROTO:
  case TURBO_ERANGE:
  case TURBO_ENOSPC:
    rpc_set_error(response, RPC_ERROR_INVALID_PARAMS, "Invalid params");
    break;
  case TURBO_EPERM:
    rpc_set_error(response, FLOWIE_CONTROL_RPC_FORBIDDEN, "Forbidden");
    break;
  case TURBO_ENOENT:
    rpc_set_error(response, FLOWIE_CONTROL_RPC_NOT_FOUND, "Not found");
    break;
  case TURBO_EBUSY:
    rpc_set_error(response, FLOWIE_CONTROL_RPC_CONFLICT, "Revision conflict");
    break;
  default:
    rpc_set_error(response, RPC_ERROR_INTERNAL, "Internal error");
    break;
  }
  return rc;
}

static json_value_t *flowie_control_rpc_user(const flowie_control_user_view_t *user) {
  json_value_t *object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "id", turbo_json_create_string(user->principal_id)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "type", turbo_json_create_string(user->principal_type)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "enabled", turbo_json_create_bool(user->enabled != 0)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "revision", turbo_json_create_uint64(user->revision)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "created_at", turbo_json_create_uint64(user->created_at)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "updated_at", turbo_json_create_uint64(user->updated_at)) !=
          TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return NULL;
  }
  return object;
}

static json_value_t *flowie_control_rpc_group(const flowie_control_group_view_t *group) {
  json_value_t *object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "id", turbo_json_create_string(group->group_id)) != TURBO_OK ||
      flowie_control_rpc_add(object, "parent_id",
                             turbo_json_create_string(group->parent_group_id)) != TURBO_OK ||
      flowie_control_rpc_add(object, "depth", turbo_json_create_uint64(group->depth)) != TURBO_OK ||
      flowie_control_rpc_add(object, "enabled", turbo_json_create_bool(group->enabled != 0)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "revision", turbo_json_create_uint64(group->revision)) !=
          TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return NULL;
  }
  return object;
}

static json_value_t *flowie_control_rpc_role(const flowie_control_role_view_t *role) {
  json_value_t *object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "id", turbo_json_create_string(role->role_id)) != TURBO_OK ||
      flowie_control_rpc_add(object, "enabled", turbo_json_create_bool(role->enabled != 0)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "revision", turbo_json_create_uint64(role->revision)) !=
          TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return NULL;
  }
  return object;
}

static int flowie_control_rpc_system_status(flowie_control_management_rpc_server_t *server,
                                            const flowie_control_management_caller_t *caller,
                                            const rpc_request_t *request,
                                            rpc_response_t *response) {
  flowie_control_management_status_t status = FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT;
  turbo_json_doc_t *params = NULL;
  json_value_t *object = NULL;
  int rc = flowie_control_rpc_params(request, NULL, 0u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_management_system_status(server->service, caller, &status);
  turbo_free_json(&params);
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "root_group",
                             turbo_json_create_string(caller->root_group_id)) != TURBO_OK ||
      flowie_control_rpc_add(object, "store_revision",
                             turbo_json_create_uint64(status.store_revision)) != TURBO_OK ||
      flowie_control_rpc_add(object, "policy_version",
                             turbo_json_create_uint64(status.policy.policy_version)) != TURBO_OK ||
      flowie_control_rpc_add(object, "draft_rules",
                             turbo_json_create_uint64(status.policy.draft_rule_count)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "published_rules",
                             turbo_json_create_uint64(status.policy.published_rule_count)) !=
          TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return flowie_control_rpc_error(response, TURBO_ENOMEM);
  }
  return flowie_control_rpc_result(response, object);
}

static int flowie_control_rpc_user_get(flowie_control_management_rpc_server_t *server,
                                       const flowie_control_management_caller_t *caller,
                                       const rpc_request_t *request, rpc_response_t *response) {
  static const char *const allowed[] = {"principal_id"};
  flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
  turbo_json_doc_t *params = NULL;
  const char *principal_id = NULL;
  int rc = flowie_control_rpc_params(request, allowed, 1u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK)
    rc = flowie_control_management_user_get(server->service, caller, principal_id, &user);
  turbo_free_json(&params);
  return rc == TURBO_OK ? flowie_control_rpc_result(response, flowie_control_rpc_user(&user))
                        : flowie_control_rpc_error(response, rc);
}

static int flowie_control_rpc_user_list(flowie_control_management_rpc_server_t *server,
                                        const flowie_control_management_caller_t *caller,
                                        const rpc_request_t *request, rpc_response_t *response) {
  static const char *const allowed[] = {"after", "limit"};
  turbo_json_doc_t *params = NULL;
  flowie_control_user_view_t *items = NULL;
  json_value_t *result = NULL;
  json_value_t *array = NULL;
  const char *after = NULL;
  size_t capacity = 0u;
  size_t count = 0u;
  int has_more = 0;
  int rc = flowie_control_rpc_params(request, allowed, 2u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "after", TURBO_FLOW_SECURITY_ID_MAX, 0, &after);
  if (rc == TURBO_OK) rc = flowie_control_rpc_page_limit(params, &capacity);
  if (rc == TURBO_OK) {
    items = (flowie_control_user_view_t *)calloc(capacity, sizeof(*items));
    if (!items) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < capacity; ++index)
    items[index] = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
  if (rc == TURBO_OK)
    rc = flowie_control_management_user_list(server->service, caller, after, items, capacity,
                                             &count, &has_more);
  if (rc == TURBO_OK) {
    result = turbo_json_create_object();
    array = turbo_json_create_array();
    if (!result || !array) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index)
    rc = flowie_control_rpc_array_add(array, flowie_control_rpc_user(&items[index]));
  if (rc == TURBO_OK) {
    rc = flowie_control_rpc_add(result, "items", array);
    if (rc == TURBO_OK) array = NULL;
  }
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_add(result, "has_more", turbo_json_create_bool(has_more != 0));
  free(items);
  turbo_free_json(&params);
  flowie_control_rpc_free_json_value(array);
  if (rc != TURBO_OK) {
    flowie_control_rpc_free_json_value(result);
    return flowie_control_rpc_error(response, rc);
  }
  return flowie_control_rpc_result(response, result);
}

static int flowie_control_rpc_user_write(flowie_control_management_rpc_server_t *server,
                                         const flowie_control_management_caller_t *caller,
                                         const rpc_request_t *request, rpc_response_t *response,
                                         int disable) {
  static const char *const create_allowed[] = {"principal_id", "principal_type", "request_id",
                                               "expected_revision"};
  static const char *const disable_allowed[] = {"principal_id", "request_id", "expected_revision"};
  turbo_json_doc_t *params = NULL;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *principal_id = NULL;
  const char *principal_type = NULL;
  const char *request_id = NULL;
  uint64_t expected_revision = 0u;
  uint64_t occurred_at;
  int rc = flowie_control_rpc_params(request, disable ? disable_allowed : create_allowed,
                                     disable ? 3u : 4u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK && !disable)
    rc = flowie_control_rpc_string(params, "principal_type", TURBO_FLOW_SECURITY_TYPE_MAX, 1,
                                   &principal_type);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &expected_revision);
  occurred_at = server->clock(server->clock_ctx);
  if (rc == TURBO_OK && occurred_at == 0u) rc = TURBO_EIO;
  if (rc == TURBO_OK && disable) {
    flowie_control_user_disable_command_t command = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_disable(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK) {
    flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.principal_type = principal_type;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_create(server->service, caller, &command, &result);
  }
  turbo_free_json(&params);
  return rc == TURBO_OK
             ? flowie_control_rpc_result(response, flowie_control_rpc_command_result(&result))
             : flowie_control_rpc_error(response, rc);
}

static int flowie_control_rpc_credential_issue(
    flowie_control_management_rpc_server_t *server,
    const flowie_control_management_caller_t *caller, const rpc_request_t *request,
    rpc_response_t *response, int rotate) {
  static const char *const allowed[] = {"principal_id", "request_id", "expected_revision"};
  turbo_json_doc_t *params = NULL;
  flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  json_value_t *object = NULL;
  const char *principal_id = NULL;
  const char *request_id = NULL;
  char secret_base64[FLOWIE_CONTROL_RPC_CREDENTIAL_BASE64_SIZE] = {0};
  uint64_t expected_revision = 0u;
  uint64_t occurred_at = 0u;
  int rc = flowie_control_rpc_params(request, allowed, 3u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &expected_revision);
  if (rc == TURBO_OK) {
    occurred_at = server->clock(server->clock_ctx);
    if (occurred_at == 0u) rc = TURBO_EIO;
  }
  if (rc == TURBO_OK) {
    flowie_control_credential_issue_command_t command =
        FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = rotate ? flowie_control_management_credential_rotate(server->service, caller, &command,
                                                               &generated)
                : flowie_control_management_credential_generate(server->service, caller, &command,
                                                                 &generated);
  }
  turbo_free_json(&params);
  if (rc == TURBO_EALREADY) {
    flowie_control_generated_credential_wipe(&generated);
    rpc_set_error(response, FLOWIE_CONTROL_RPC_SECRET_UNAVAILABLE,
                  "Credential secret is unavailable; use a new request_id");
    return rc;
  }
  if (rc != TURBO_OK) {
    flowie_control_generated_credential_wipe(&generated);
    return flowie_control_rpc_error(response, rc);
  }
  if (generated.secret_size != FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE ||
      tn_base64_encode_buf(generated.secret, generated.secret_size, secret_base64,
                           sizeof(secret_base64)) != 0) {
    rc = TURBO_EIO;
    goto done;
  }
  object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "revision", turbo_json_create_uint64(generated.revision)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "secret_base64", turbo_json_create_string(secret_base64)) !=
          TURBO_OK) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flowie_control_rpc_result(response, object);
  object = NULL;

done:
  flowie_control_rpc_free_json_value(object);
  flowie_control_generated_credential_wipe(&generated);
  flowie_control_credential_wipe(secret_base64, sizeof(secret_base64));
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  return TURBO_OK;
}

static int flowie_control_rpc_credential_revoke(
    flowie_control_management_rpc_server_t *server,
    const flowie_control_management_caller_t *caller, const rpc_request_t *request,
    rpc_response_t *response) {
  static const char *const allowed[] = {"principal_id", "request_id", "expected_revision"};
  turbo_json_doc_t *params = NULL;
  flowie_control_credential_revoke_command_t command =
      FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *principal_id = NULL;
  const char *request_id = NULL;
  uint64_t occurred_at = 0u;
  int rc = flowie_control_rpc_params(request, allowed, 3u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &command.expected_revision);
  if (rc == TURBO_OK) {
    occurred_at = server->clock(server->clock_ctx);
    if (occurred_at == 0u) rc = TURBO_EIO;
  }
  if (rc == TURBO_OK) {
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_credential_revoke(server->service, caller, &command, &result);
  }
  turbo_free_json(&params);
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  return flowie_control_rpc_result(response, flowie_control_rpc_command_result(&result));
}

static int flowie_control_rpc_named_list(flowie_control_management_rpc_server_t *server,
                                         const flowie_control_management_caller_t *caller,
                                         const rpc_request_t *request, rpc_response_t *response,
                                         int groups) {
  static const char *const allowed[] = {"after", "limit"};
  turbo_json_doc_t *params = NULL;
  void *items = NULL;
  json_value_t *result = NULL;
  json_value_t *array = NULL;
  const char *after = NULL;
  size_t capacity = 0u;
  size_t count = 0u;
  int has_more = 0;
  int rc = flowie_control_rpc_params(request, allowed, 2u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "after", TURBO_FLOW_SECURITY_ID_MAX, 0, &after);
  if (rc == TURBO_OK) rc = flowie_control_rpc_page_limit(params, &capacity);
  if (rc == TURBO_OK) {
    items = calloc(capacity, groups ? sizeof(flowie_control_group_view_t)
                                    : sizeof(flowie_control_role_view_t));
    if (!items) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < capacity; ++index) {
    if (groups)
      ((flowie_control_group_view_t *)items)[index] =
          (flowie_control_group_view_t)FLOWIE_CONTROL_GROUP_VIEW_INIT;
    else
      ((flowie_control_role_view_t *)items)[index] =
          (flowie_control_role_view_t)FLOWIE_CONTROL_ROLE_VIEW_INIT;
  }
  if (rc == TURBO_OK && groups)
    rc = flowie_control_management_group_list(server->service, caller, after, items, capacity,
                                              &count, &has_more);
  else if (rc == TURBO_OK)
    rc = flowie_control_management_role_list(server->service, caller, after, items, capacity,
                                             &count, &has_more);
  if (rc == TURBO_OK) {
    result = turbo_json_create_object();
    array = turbo_json_create_array();
    if (!result || !array) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item =
        groups ? flowie_control_rpc_group(&((flowie_control_group_view_t *)items)[index])
               : flowie_control_rpc_role(&((flowie_control_role_view_t *)items)[index]);
    rc = flowie_control_rpc_array_add(array, item);
  }
  if (rc == TURBO_OK) {
    rc = flowie_control_rpc_add(result, "items", array);
    if (rc == TURBO_OK) array = NULL;
  }
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_add(result, "has_more", turbo_json_create_bool(has_more != 0));
  free(items);
  turbo_free_json(&params);
  flowie_control_rpc_free_json_value(array);
  if (rc != TURBO_OK) {
    flowie_control_rpc_free_json_value(result);
    return flowie_control_rpc_error(response, rc);
  }
  return flowie_control_rpc_result(response, result);
}

static int flowie_control_rpc_group_write(flowie_control_management_rpc_server_t *server,
                                          const flowie_control_management_caller_t *caller,
                                          const rpc_request_t *request, rpc_response_t *response,
                                          int operation) {
  static const char *const create_allowed[] = {"group_id", "parent_group_id", "request_id",
                                               "expected_revision"};
  static const char *const disable_allowed[] = {"group_id", "request_id", "expected_revision"};
  static const char *const member_allowed[] = {"principal_id", "group_id", "request_id",
                                               "expected_revision"};
  const char *const *allowed = operation == 0   ? create_allowed
                               : operation == 1 ? disable_allowed
                                                : member_allowed;
  size_t allowed_count = operation == 1 ? 3u : 4u;
  turbo_json_doc_t *params = NULL;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *group_id = NULL;
  const char *parent_group_id = NULL;
  const char *principal_id = NULL;
  const char *request_id = NULL;
  uint64_t expected_revision = 0u;
  uint64_t occurred_at;
  int rc = flowie_control_rpc_params(request, allowed, allowed_count, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "group_id", TURBO_FLOW_SECURITY_ID_MAX, 1, &group_id);
  if (rc == TURBO_OK && operation == 0)
    rc = flowie_control_rpc_string(params, "parent_group_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &parent_group_id);
  if (rc == TURBO_OK && operation >= 2)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &expected_revision);
  occurred_at = server->clock(server->clock_ctx);
  if (rc == TURBO_OK && occurred_at == 0u) rc = TURBO_EIO;
  if (rc == TURBO_OK && operation == 0) {
    flowie_control_group_create_command_t command = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.group_id = group_id;
    command.parent_group_id = parent_group_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_group_create(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK && operation == 1) {
    flowie_control_group_disable_command_t command = FLOWIE_CONTROL_GROUP_DISABLE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.group_id = group_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_group_disable(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK && operation == 2) {
    flowie_control_membership_add_command_t command = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.group_id = group_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_membership_add(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK) {
    flowie_control_membership_remove_command_t command =
        FLOWIE_CONTROL_MEMBERSHIP_REMOVE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.group_id = group_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_membership_remove(server->service, caller, &command, &result);
  }
  turbo_free_json(&params);
  return rc == TURBO_OK
             ? flowie_control_rpc_result(response, flowie_control_rpc_command_result(&result))
             : flowie_control_rpc_error(response, rc);
}

static int flowie_control_rpc_role_write(flowie_control_management_rpc_server_t *server,
                                         const flowie_control_management_caller_t *caller,
                                         const rpc_request_t *request, rpc_response_t *response,
                                         int operation) {
  static const char *const role_allowed[] = {"role_id", "request_id", "expected_revision"};
  static const char *const assignment_allowed[] = {"principal_id", "role_id", "request_id",
                                                   "expected_revision"};
  turbo_json_doc_t *params = NULL;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *role_id = NULL;
  const char *principal_id = NULL;
  const char *request_id = NULL;
  uint64_t expected_revision = 0u;
  uint64_t occurred_at;
  int rc = flowie_control_rpc_params(request, operation < 2 ? role_allowed : assignment_allowed,
                                     operation < 2 ? 3u : 4u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "role_id", TURBO_FLOW_SECURITY_TYPE_MAX, 1, &role_id);
  if (rc == TURBO_OK && operation >= 2)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &expected_revision);
  occurred_at = server->clock(server->clock_ctx);
  if (rc == TURBO_OK && occurred_at == 0u) rc = TURBO_EIO;
  if (rc == TURBO_OK && operation == 0) {
    flowie_control_role_create_command_t command = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.role_id = role_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_role_create(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK && operation == 1) {
    flowie_control_role_disable_command_t command = FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.role_id = role_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_role_disable(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK && operation == 2) {
    flowie_control_user_role_add_command_t command = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.role_id = role_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_role_add(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK) {
    flowie_control_user_role_remove_command_t command =
        FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.principal_id = principal_id;
    command.role_id = role_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_role_remove(server->service, caller, &command, &result);
  }
  turbo_free_json(&params);
  return rc == TURBO_OK
             ? flowie_control_rpc_result(response, flowie_control_rpc_command_result(&result))
             : flowie_control_rpc_error(response, rc);
}

static int flowie_control_rpc_effective(flowie_control_management_rpc_server_t *server,
                                        const flowie_control_management_caller_t *caller,
                                        const rpc_request_t *request, rpc_response_t *response,
                                        int groups) {
  static const char *const allowed[] = {"principal_id"};
  turbo_json_doc_t *params = NULL;
  const char *principal_id = NULL;
  json_value_t *array = NULL;
  flowie_control_effective_groups_view_t group_view = FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  flowie_control_effective_roles_view_t role_view = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  int rc = flowie_control_rpc_params(request, allowed, 1u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "principal_id", TURBO_FLOW_SECURITY_ID_MAX, 1,
                                   &principal_id);
  if (rc == TURBO_OK && groups)
    rc = flowie_control_management_effective_groups(server->service, caller, principal_id,
                                                    &group_view);
  else if (rc == TURBO_OK)
    rc = flowie_control_management_effective_roles(server->service, caller, principal_id,
                                                   &role_view);
  turbo_free_json(&params);
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  array = turbo_json_create_array();
  if (!array) return flowie_control_rpc_error(response, TURBO_ENOMEM);
  for (uint32_t index = 0u;
       rc == TURBO_OK && index < (groups ? group_view.group_count : role_view.role_count);
       ++index) {
    const char *value = groups ? group_view.groups[index] : role_view.roles[index];
    rc = flowie_control_rpc_array_add(array, turbo_json_create_string(value));
  }
  if (rc != TURBO_OK) {
    flowie_control_rpc_free_json_value(array);
    return flowie_control_rpc_error(response, rc);
  }
  return flowie_control_rpc_result(response, array);
}

static int flowie_control_rpc_policy_status(flowie_control_management_rpc_server_t *server,
                                            const flowie_control_management_caller_t *caller,
                                            const rpc_request_t *request,
                                            rpc_response_t *response) {
  flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  turbo_json_doc_t *params = NULL;
  json_value_t *object = NULL;
  int rc = flowie_control_rpc_params(request, NULL, 0u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_management_policy_status(server->service, caller, &status);
  turbo_free_json(&params);
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "store_revision",
                             turbo_json_create_uint64(status.store_revision)) != TURBO_OK ||
      flowie_control_rpc_add(object, "policy_version",
                             turbo_json_create_uint64(status.policy_version)) != TURBO_OK ||
      flowie_control_rpc_add(object, "expires_at", turbo_json_create_uint64(status.expires_at)) !=
          TURBO_OK ||
      flowie_control_rpc_add(object, "draft_rules",
                             turbo_json_create_uint64(status.draft_rule_count)) != TURBO_OK ||
      flowie_control_rpc_add(object, "published_rules",
                             turbo_json_create_uint64(status.published_rule_count)) != TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return flowie_control_rpc_error(response, TURBO_ENOMEM);
  }
  return flowie_control_rpc_result(response, object);
}

static int flowie_control_rpc_policy_rule_list(flowie_control_management_rpc_server_t *server,
                                               const flowie_control_management_caller_t *caller,
                                               const rpc_request_t *request,
                                               rpc_response_t *response) {
  static const char *const allowed[] = {"after_ordinal", "limit"};
  turbo_json_doc_t *params = NULL;
  flowie_control_policy_rule_view_t *items = NULL;
  json_value_t *result = NULL;
  json_value_t *array = NULL;
  uint64_t after = 0u;
  size_t capacity = 0u;
  size_t count = 0u;
  int has_after = 0;
  int has_more = 0;
  int rc = flowie_control_rpc_params(request, allowed, 2u, &params);
  if (rc == TURBO_OK && turbo_json_object_get(params, "after_ordinal")) {
    has_after = 1;
    rc = flowie_control_rpc_u64(params, "after_ordinal", 1, &after);
    if (rc == TURBO_OK && after > UINT32_MAX) rc = TURBO_ERANGE;
  }
  if (rc == TURBO_OK) rc = flowie_control_rpc_page_limit(params, &capacity);
  if (rc == TURBO_OK) {
    items = (flowie_control_policy_rule_view_t *)calloc(capacity, sizeof(*items));
    if (!items) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < capacity; ++index)
    items[index] = (flowie_control_policy_rule_view_t)FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
  if (rc == TURBO_OK)
    rc = flowie_control_management_policy_rule_list(server->service, caller, (uint32_t)after,
                                                    has_after, items, capacity, &count, &has_more);
  if (rc == TURBO_OK) {
    result = turbo_json_create_object();
    array = turbo_json_create_array();
    if (!result || !array) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item ||
        flowie_control_rpc_add(item, "ordinal", turbo_json_create_uint64(items[index].ordinal)) !=
            TURBO_OK ||
        flowie_control_rpc_add(item, "rule_line",
                               turbo_json_create_string(items[index].rule_line)) != TURBO_OK ||
        flowie_control_rpc_add(item, "revision", turbo_json_create_uint64(items[index].revision)) !=
            TURBO_OK) {
      flowie_control_rpc_free_json_value(item);
      rc = TURBO_ENOMEM;
    } else {
      rc = flowie_control_rpc_array_add(array, item);
    }
  }
  if (rc == TURBO_OK) {
    rc = flowie_control_rpc_add(result, "items", array);
    if (rc == TURBO_OK) array = NULL;
  }
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_add(result, "has_more", turbo_json_create_bool(has_more != 0));
  free(items);
  turbo_free_json(&params);
  flowie_control_rpc_free_json_value(array);
  if (rc != TURBO_OK) {
    flowie_control_rpc_free_json_value(result);
    return flowie_control_rpc_error(response, rc);
  }
  return flowie_control_rpc_result(response, result);
}

static int flowie_control_rpc_policy_rule_write(flowie_control_management_rpc_server_t *server,
                                                const flowie_control_management_caller_t *caller,
                                                const rpc_request_t *request,
                                                rpc_response_t *response, int remove) {
  static const char *const put_allowed[] = {"ordinal", "rule_line", "request_id",
                                            "expected_revision"};
  static const char *const delete_allowed[] = {"ordinal", "request_id", "expected_revision"};
  turbo_json_doc_t *params = NULL;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *rule_line = NULL;
  const char *request_id = NULL;
  uint64_t ordinal = 0u;
  uint64_t expected_revision = 0u;
  uint64_t occurred_at;
  int rc = flowie_control_rpc_params(request, remove ? delete_allowed : put_allowed,
                                     remove ? 3u : 4u, &params);
  if (rc == TURBO_OK) rc = flowie_control_rpc_u64(params, "ordinal", 1, &ordinal);
  if (rc == TURBO_OK && ordinal >= TURBO_FLOW_SECURITY_MAX_RULES) rc = TURBO_ERANGE;
  if (rc == TURBO_OK && !remove)
    rc = flowie_control_rpc_string(params, "rule_line", TURBO_FLOW_SECURITY_RULE_LINE_MAX, 1,
                                   &rule_line);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &expected_revision);
  occurred_at = server->clock(server->clock_ctx);
  if (rc == TURBO_OK && occurred_at == 0u) rc = TURBO_EIO;
  if (rc == TURBO_OK && remove) {
    flowie_control_policy_rule_delete_command_t command =
        FLOWIE_CONTROL_POLICY_RULE_DELETE_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.ordinal = (uint32_t)ordinal;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_policy_rule_delete(server->service, caller, &command, &result);
  } else if (rc == TURBO_OK) {
    flowie_control_policy_rule_put_command_t command = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.ordinal = (uint32_t)ordinal;
    command.rule_line = rule_line;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_policy_rule_put(server->service, caller, &command, &result);
  }
  turbo_free_json(&params);
  return rc == TURBO_OK
             ? flowie_control_rpc_result(response, flowie_control_rpc_command_result(&result))
             : flowie_control_rpc_error(response, rc);
}

static int flowie_control_rpc_policy_validate(flowie_control_management_rpc_server_t *server,
                                              const flowie_control_management_caller_t *caller,
                                              const rpc_request_t *request,
                                              rpc_response_t *response) {
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  turbo_json_doc_t *params = NULL;
  json_value_t *object = NULL;
  int rc = flowie_control_rpc_params(request, NULL, 0u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_management_policy_validate(server->service, caller, &validation);
  turbo_free_json(&params);
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  object = turbo_json_create_object();
  if (!object ||
      flowie_control_rpc_add(object, "store_revision",
                             turbo_json_create_uint64(validation.store_revision)) != TURBO_OK ||
      flowie_control_rpc_add(object, "rule_count",
                             turbo_json_create_uint64(validation.rule_count)) != TURBO_OK ||
      flowie_control_rpc_add(object, "deny_rule_count",
                             turbo_json_create_uint64(validation.deny_rule_count)) != TURBO_OK) {
    flowie_control_rpc_free_json_value(object);
    return flowie_control_rpc_error(response, TURBO_ENOMEM);
  }
  return flowie_control_rpc_result(response, object);
}

static int flowie_control_rpc_policy_publish(flowie_control_management_rpc_server_t *server,
                                             const flowie_control_management_caller_t *caller,
                                             const rpc_request_t *request,
                                             rpc_response_t *response) {
  static const char *const allowed[] = {"request_id", "expected_revision", "expires_at"};
  turbo_json_doc_t *params = NULL;
  flowie_control_policy_publish_result_t result = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  const char *request_id = NULL;
  uint64_t expected_revision = 0u;
  uint64_t expires_at = 0u;
  uint64_t occurred_at;
  int rc = flowie_control_rpc_params(request, allowed, 3u, &params);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_string(params, "request_id", FLOWIE_CONTROL_REQUEST_ID_MAX, 1,
                                   &request_id);
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_u64(params, "expected_revision", 1, &expected_revision);
  if (rc == TURBO_OK) rc = flowie_control_rpc_u64(params, "expires_at", 0, &expires_at);
  occurred_at = server->clock(server->clock_ctx);
  if (rc == TURBO_OK && occurred_at == 0u) rc = TURBO_EIO;
  if (rc == TURBO_OK) {
    flowie_control_policy_publish_command_t command = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
    command.root_group_id = caller->root_group_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    command.expires_at = expires_at;
    rc = flowie_control_management_policy_publish(server->service, caller, &command, &result);
  }
  turbo_free_json(&params);
  if (rc != TURBO_OK) return flowie_control_rpc_error(response, rc);
  {
    json_value_t *object = turbo_json_create_object();
    if (!object ||
        flowie_control_rpc_add(object, "revision", turbo_json_create_uint64(result.revision)) !=
            TURBO_OK ||
        flowie_control_rpc_add(object, "policy_version",
                               turbo_json_create_uint64(result.policy_version)) != TURBO_OK ||
        flowie_control_rpc_add(object, "replayed", turbo_json_create_bool(result.replayed != 0)) !=
            TURBO_OK) {
      flowie_control_rpc_free_json_value(object);
      return flowie_control_rpc_error(response, TURBO_ENOMEM);
    }
    return flowie_control_rpc_result(response, object);
  }
}

static int flowie_control_rpc_audit_list(flowie_control_management_rpc_server_t *server,
                                         const flowie_control_management_caller_t *caller,
                                         const rpc_request_t *request, rpc_response_t *response) {
  static const char *const allowed[] = {"after_revision", "limit"};
  turbo_json_doc_t *params = NULL;
  flowie_control_audit_view_t *items = NULL;
  json_value_t *result = NULL;
  json_value_t *array = NULL;
  uint64_t after = 0u;
  size_t capacity = 0u;
  size_t count = 0u;
  int has_more = 0;
  int rc = flowie_control_rpc_params(request, allowed, 2u, &params);
  if (rc == TURBO_OK) rc = flowie_control_rpc_u64(params, "after_revision", 0, &after);
  if (rc == TURBO_OK) rc = flowie_control_rpc_page_limit(params, &capacity);
  if (rc == TURBO_OK) {
    items = (flowie_control_audit_view_t *)calloc(capacity, sizeof(*items));
    if (!items) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < capacity; ++index)
    items[index] = (flowie_control_audit_view_t)FLOWIE_CONTROL_AUDIT_VIEW_INIT;
  if (rc == TURBO_OK)
    rc = flowie_control_management_audit_list(server->service, caller, after, items, capacity,
                                              &count, &has_more);
  if (rc == TURBO_OK) {
    result = turbo_json_create_object();
    array = turbo_json_create_array();
    if (!result || !array) rc = TURBO_ENOMEM;
  }
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item ||
        flowie_control_rpc_add(item, "request_id",
                               turbo_json_create_string(items[index].request_id)) != TURBO_OK ||
        flowie_control_rpc_add(item, "actor", turbo_json_create_string(items[index].actor)) !=
            TURBO_OK ||
        flowie_control_rpc_add(item, "operation",
                               turbo_json_create_string(items[index].operation)) != TURBO_OK ||
        flowie_control_rpc_add(item, "target", turbo_json_create_string(items[index].target_id)) !=
            TURBO_OK ||
        flowie_control_rpc_add(item, "revision", turbo_json_create_uint64(items[index].revision)) !=
            TURBO_OK ||
        flowie_control_rpc_add(item, "occurred_at",
                               turbo_json_create_uint64(items[index].occurred_at)) != TURBO_OK) {
      flowie_control_rpc_free_json_value(item);
      rc = TURBO_ENOMEM;
    } else {
      rc = flowie_control_rpc_array_add(array, item);
    }
  }
  if (rc == TURBO_OK) {
    rc = flowie_control_rpc_add(result, "items", array);
    if (rc == TURBO_OK) array = NULL;
  }
  if (rc == TURBO_OK)
    rc = flowie_control_rpc_add(result, "has_more", turbo_json_create_bool(has_more != 0));
  free(items);
  turbo_free_json(&params);
  flowie_control_rpc_free_json_value(array);
  if (rc != TURBO_OK) {
    flowie_control_rpc_free_json_value(result);
    return flowie_control_rpc_error(response, rc);
  }
  return flowie_control_rpc_result(response, result);
}

static int flowie_control_rpc_dispatch(flowie_control_management_rpc_server_t *server,
                                       const flowie_control_management_caller_t *caller,
                                       const rpc_request_t *request, rpc_response_t *response) {
  const char *method = request->method;
  if (strcmp(method, "flowie.system.status") == 0)
    return flowie_control_rpc_system_status(server, caller, request, response);
  if (strcmp(method, "flowie.user.get") == 0)
    return flowie_control_rpc_user_get(server, caller, request, response);
  if (strcmp(method, "flowie.user.list") == 0)
    return flowie_control_rpc_user_list(server, caller, request, response);
  if (strcmp(method, "flowie.user.create") == 0)
    return flowie_control_rpc_user_write(server, caller, request, response, 0);
  if (strcmp(method, "flowie.user.disable") == 0)
    return flowie_control_rpc_user_write(server, caller, request, response, 1);
  if (strcmp(method, "flowie.credential.generate") == 0)
    return flowie_control_rpc_credential_issue(server, caller, request, response, 0);
  if (strcmp(method, "flowie.credential.rotate") == 0)
    return flowie_control_rpc_credential_issue(server, caller, request, response, 1);
  if (strcmp(method, "flowie.credential.revoke") == 0)
    return flowie_control_rpc_credential_revoke(server, caller, request, response);
  if (strcmp(method, "flowie.group.list") == 0)
    return flowie_control_rpc_named_list(server, caller, request, response, 1);
  if (strcmp(method, "flowie.group.create") == 0)
    return flowie_control_rpc_group_write(server, caller, request, response, 0);
  if (strcmp(method, "flowie.group.disable") == 0)
    return flowie_control_rpc_group_write(server, caller, request, response, 1);
  if (strcmp(method, "flowie.group.member.add") == 0)
    return flowie_control_rpc_group_write(server, caller, request, response, 2);
  if (strcmp(method, "flowie.group.member.remove") == 0)
    return flowie_control_rpc_group_write(server, caller, request, response, 3);
  if (strcmp(method, "flowie.group.effective") == 0)
    return flowie_control_rpc_effective(server, caller, request, response, 1);
  if (strcmp(method, "flowie.role.list") == 0)
    return flowie_control_rpc_named_list(server, caller, request, response, 0);
  if (strcmp(method, "flowie.role.create") == 0)
    return flowie_control_rpc_role_write(server, caller, request, response, 0);
  if (strcmp(method, "flowie.role.disable") == 0)
    return flowie_control_rpc_role_write(server, caller, request, response, 1);
  if (strcmp(method, "flowie.role.assign") == 0)
    return flowie_control_rpc_role_write(server, caller, request, response, 2);
  if (strcmp(method, "flowie.role.remove") == 0)
    return flowie_control_rpc_role_write(server, caller, request, response, 3);
  if (strcmp(method, "flowie.role.effective") == 0)
    return flowie_control_rpc_effective(server, caller, request, response, 0);
  if (strcmp(method, "flowie.policy.status") == 0)
    return flowie_control_rpc_policy_status(server, caller, request, response);
  if (strcmp(method, "flowie.policy.rule.list") == 0)
    return flowie_control_rpc_policy_rule_list(server, caller, request, response);
  if (strcmp(method, "flowie.policy.rule.put") == 0)
    return flowie_control_rpc_policy_rule_write(server, caller, request, response, 0);
  if (strcmp(method, "flowie.policy.rule.delete") == 0)
    return flowie_control_rpc_policy_rule_write(server, caller, request, response, 1);
  if (strcmp(method, "flowie.policy.validate") == 0)
    return flowie_control_rpc_policy_validate(server, caller, request, response);
  if (strcmp(method, "flowie.policy.publish") == 0)
    return flowie_control_rpc_policy_publish(server, caller, request, response);
  if (strcmp(method, "flowie.audit.list") == 0)
    return flowie_control_rpc_audit_list(server, caller, request, response);
  rpc_set_error(response, RPC_ERROR_METHOD_NOT_FOUND, "Method not found");
  return TURBO_ENOENT;
}

static void flowie_control_rpc_method(Req *request, Res *response) {
  flowie_control_management_rpc_server_t *server;
  rpc_response_t rpc_response;
  if (!request || !response || !request->app || !request->path) return;
  set_header(response, "Cache-Control", "no-store");
  set_header(response, "Pragma", "no-cache");
  set_header(response, "X-Content-Type-Options", "nosniff");
  server = (flowie_control_management_rpc_server_t *)iris_app_lookup_rpc_context(request->app,
                                                                                 request->path);
  if (!server) {
    memset(&rpc_response, 0, sizeof(rpc_response));
    rpc_response.arena = request->arena;
    rpc_response.jsonrpc = "2.0";
    rpc_set_error(&rpc_response, RPC_ERROR_INTERNAL, "RPC context unavailable");
  } else {
    (void)flowie_control_management_rpc_server_execute(server, request, &rpc_response);
  }
  rpc_send_response(response, &rpc_response);
  if (rpc_response.result)
    flowie_control_credential_wipe((void *)rpc_response.result, strlen(rpc_response.result));
}

int flowie_control_management_rpc_server_execute(flowie_control_management_rpc_server_t *server,
                                                 Req *request, rpc_response_t *response_out) {
  rpc_request_t rpc_request;
  rpc_method_handler_t handler = NULL;
  int rc;
  if (!server || !request || !response_out || !request->arena || !request->app || !request->path)
    return TURBO_EINVAL;
  memset(response_out, 0, sizeof(*response_out));
  response_out->arena = request->arena;
  response_out->jsonrpc = "2.0";
  response_out->protocol = RPC_PROTOCOL_JSON;
  if (!request->security || !request->security->authenticated) {
    rpc_set_error(response_out, FLOWIE_CONTROL_RPC_AUTH_REQUIRED, "Authentication required");
    return TURBO_EPERM;
  }
  if (request->body_len == 0u || request->body_len > server->rpc_context->config.max_request_size) {
    rpc_set_error(response_out, RPC_ERROR_INVALID_REQUEST, "Invalid request size");
    return TURBO_EPROTO;
  }
  for (size_t index = 0u; index < request->body_len; ++index) {
    unsigned char byte = (unsigned char)request->body[index];
    if (byte == ' ' || byte == '\t' || byte == '\r' || byte == '\n') continue;
    if (byte == '[') {
      rpc_set_error(response_out, RPC_ERROR_INVALID_REQUEST, "Batch requests are disabled");
      return TURBO_EPROTO;
    }
    break;
  }
  memset(&rpc_request, 0, sizeof(rpc_request));
  rc = rpc_parse_request(request, &rpc_request);
  if (rc != 0) {
    rpc_set_error(response_out, rc, "Invalid request");
    return TURBO_EPROTO;
  }
  response_out->id = rpc_request.id;
  if (!rpc_request.id) {
    rpc_set_error(response_out, RPC_ERROR_INVALID_REQUEST, "Notifications are disabled");
    return TURBO_EPROTO;
  }
  for (size_t index = 0u; index < server->rpc_context->method_count; ++index) {
    if (strcmp(server->rpc_context->methods[index].name, rpc_request.method) == 0) {
      handler = server->rpc_context->methods[index].handler;
      break;
    }
  }
  if (!handler) {
    rpc_set_error(response_out, RPC_ERROR_METHOD_NOT_FOUND, "Method not found");
    return TURBO_ENOENT;
  }
  rc = handler(request, NULL, &rpc_request, response_out);
  if (rc != TURBO_OK && response_out->error_code == 0)
    rpc_set_error(response_out, RPC_ERROR_INTERNAL, "Internal error");
  return rc;
}

static int flowie_control_rpc_registered_method(Req *request, Res *response,
                                                rpc_request_t *rpc_request,
                                                rpc_response_t *rpc_response) {
  flowie_control_management_rpc_server_t *server;
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  int rc;
  (void)response;
  if (!request || !request->app || !request->path || !rpc_request || !rpc_response) {
    if (rpc_response) rpc_set_error(rpc_response, RPC_ERROR_INTERNAL, "Internal error");
    return TURBO_EINVAL;
  }
  server = (flowie_control_management_rpc_server_t *)iris_app_lookup_rpc_context(request->app,
                                                                                 request->path);
  if (!server) {
    rpc_set_error(rpc_response, RPC_ERROR_INTERNAL, "RPC context unavailable");
    return TURBO_EINVAL;
  }
  rc = server->resolve_caller(server->resolve_caller_ctx, request, &caller);
  if (rc != TURBO_OK) {
    rpc_set_error(rpc_response,
                  rc == TURBO_EPERM ? FLOWIE_CONTROL_RPC_FORBIDDEN : RPC_ERROR_INTERNAL,
                  rc == TURBO_EPERM ? "Forbidden" : "Caller resolution failed");
    return rc;
  }
  return flowie_control_rpc_dispatch(server, &caller, rpc_request, rpc_response);
}

void flowie_control_management_rpc_server_handle(flowie_control_management_rpc_server_t *server,
                                                 Req *request, Res *response) {
  if (!server || !request || !response) return;
  if (!request->app && server->bound_app) request->app = server->bound_app;
  flowie_control_rpc_method(request, response);
}

int flowie_control_management_rpc_server_create(
    const flowie_control_management_rpc_server_config_t *config,
    flowie_control_management_rpc_server_t **out) {
  flowie_control_management_rpc_server_t *server;
  rpc_method_t method;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !config->service || !config->rpc_context ||
      !config->resolve_caller || !config->clock || !out ||
      config->rpc_context->method_count != 0u ||
      config->rpc_context->config.default_protocol != RPC_PROTOCOL_JSON ||
      config->rpc_context->config.enable_batch ||
      config->rpc_context->config.enable_introspection || !config->rpc_context->config.endpoint ||
      !config->rpc_context->config.endpoint[0] ||
      config->rpc_context->config.max_request_size == 0u ||
      config->rpc_context->config.max_request_size > FLOWIE_CONTROL_MANAGEMENT_RPC_REQUEST_MAX)
    return TURBO_EINVAL;
  server = (flowie_control_management_rpc_server_t *)calloc(1u, sizeof(*server));
  if (!server) return TURBO_ENOMEM;
  server->service = config->service;
  server->rpc_context = config->rpc_context;
  server->resolve_caller = config->resolve_caller;
  server->resolve_caller_ctx = config->resolve_caller_ctx;
  server->clock = config->clock;
  server->clock_ctx = config->clock_ctx;
  memset(&method, 0, sizeof(method));
  method.handler = flowie_control_rpc_registered_method;
  method.description = "Flowie management method";
  method.requires_auth = 1;
  for (size_t index = 0u;
       index < sizeof(FLOWIE_CONTROL_RPC_METHODS) / sizeof(FLOWIE_CONTROL_RPC_METHODS[0]);
       ++index) {
    method.name = FLOWIE_CONTROL_RPC_METHODS[index];
    if (rpc_register_method(server->rpc_context, &method) != 0) {
      flowie_control_management_rpc_server_destroy(server);
      return TURBO_ENOMEM;
    }
    ++server->registered_method_count;
  }
  *out = server;
  return TURBO_OK;
}

int flowie_control_management_rpc_server_bind(flowie_control_management_rpc_server_t *server,
                                              iris_app_t *app) {
  const char *endpoint;
  if (!server || !app || server->bound_app) return TURBO_EINVAL;
  endpoint = server->rpc_context->config.endpoint;
  if (iris_app_lookup_rpc_context(app, endpoint) ||
      iris_app_bind_rpc_context(app, endpoint, server) != 0)
    return TURBO_EBUSY;
  server->bound_app = app;
  iris_app_post(app, endpoint, flowie_control_rpc_method);
  return TURBO_OK;
}

void flowie_control_management_rpc_server_unbind(flowie_control_management_rpc_server_t *server) {
  if (!server || !server->bound_app) return;
  (void)iris_app_unbind_rpc_context(server->bound_app, server->rpc_context->config.endpoint,
                                    server);
  server->bound_app = NULL;
}

void flowie_control_management_rpc_server_destroy(flowie_control_management_rpc_server_t *server) {
  if (!server) return;
  flowie_control_management_rpc_server_unbind(server);
  while (server->registered_method_count > 0u) {
    --server->registered_method_count;
    (void)rpc_unregister_method(server->rpc_context,
                                FLOWIE_CONTROL_RPC_METHODS[server->registered_method_count]);
  }
  memset(server, 0, sizeof(*server));
  free(server);
}
