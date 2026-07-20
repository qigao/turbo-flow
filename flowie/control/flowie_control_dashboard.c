#include "flowie_control_dashboard_internal.h"
#include "flowie_control_dashboard_view_internal.h"

#include "http_common.h"
#include "monocypher.h"
#include "turbo_error.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWIE_CONTROL_DASHBOARD_FORM_FIELDS = 8 };

static const char FLOWIE_CONTROL_DASHBOARD_CSP[] =
    "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
    "img-src 'self'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'";

typedef struct flowie_control_dashboard_form_field_s {
  char *key;
  char *value;
} flowie_control_dashboard_form_field_t;

typedef struct flowie_control_dashboard_form_s {
  flowie_control_dashboard_form_field_t fields[FLOWIE_CONTROL_DASHBOARD_FORM_FIELDS];
  size_t count;
} flowie_control_dashboard_form_t;

struct flowie_control_dashboard_s {
  flowie_control_management_service_t *service;
  flowie_control_dashboard_resolve_session_fn resolve_session;
  void *resolve_session_ctx;
  flowie_control_dashboard_clock_fn clock;
  void *clock_ctx;
  flowie_control_dashboard_view_t *view;
  iris_app_t *bound_app;
};

static int flowie_control_dashboard_csrf_valid(const char *token) {
  if (!token ||
      strnlen(token, FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u) != FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE)
    return 0;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE; ++index) {
    unsigned char byte = (unsigned char)token[index];
    if (!isalnum(byte) && byte != '-' && byte != '_') return 0;
  }
  return 1;
}

static int flowie_control_dashboard_page_valid(const flowie_control_dashboard_page_t *page) {
  return page && page->size >= sizeof(*page) &&
         strnlen(page->users_after, sizeof(page->users_after)) < sizeof(page->users_after) &&
         strnlen(page->groups_after, sizeof(page->groups_after)) < sizeof(page->groups_after) &&
         strnlen(page->roles_after, sizeof(page->roles_after)) < sizeof(page->roles_after) &&
         (page->policy_has_after == 0 || page->policy_has_after == 1) &&
         (!page->policy_has_after || page->policy_after < TURBO_FLOW_SECURITY_MAX_RULES) &&
         (page->audit_has_after == 0 || page->audit_has_after == 1) &&
         (!page->audit_has_after || page->audit_after <= (uint64_t)INT64_MAX);
}

int flowie_control_dashboard_render_shell(flowie_control_dashboard_t *dashboard,
                                          const flowie_control_dashboard_page_t *page,
                                          char **html_out, size_t *html_size_out) {
  if (!dashboard || !flowie_control_dashboard_page_valid(page)) return TURBO_EINVAL;
  return flowie_control_dashboard_view_render_shell(dashboard->view, page, html_out,
                                                    html_size_out);
}

int flowie_control_dashboard_render_page(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page, char **html_out, size_t *html_size_out) {
  if (!dashboard || !caller || !flowie_control_dashboard_page_valid(page) ||
      !flowie_control_dashboard_csrf_valid(csrf_token))
    return TURBO_EINVAL;
  return flowie_control_dashboard_view_render_content(dashboard->view, dashboard->service, caller,
                                                      csrf_token, page, html_out, html_size_out);
}

int flowie_control_dashboard_render(flowie_control_dashboard_t *dashboard,
                                    const flowie_control_management_caller_t *caller,
                                    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
                                    char **html_out, size_t *html_size_out) {
  const flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  return flowie_control_dashboard_render_page(dashboard, caller, csrf_token, &page, html_out,
                                              html_size_out);
}

void flowie_control_dashboard_html_free(char *html) { free(html); }

static void flowie_control_dashboard_form_destroy(flowie_control_dashboard_form_t *form) {
  if (!form) return;
  for (size_t index = 0u; index < form->count; ++index) {
    free(form->fields[index].key);
    free(form->fields[index].value);
  }
  memset(form, 0, sizeof(*form));
}

static int flowie_control_dashboard_form_parse(const char *body, size_t body_size,
                                               flowie_control_dashboard_form_t *form) {
  char *copy;
  char *cursor;
  int rc = TURBO_EPROTO;
  if (!body || body_size == 0u || body_size > FLOWIE_CONTROL_DASHBOARD_BODY_MAX || !form ||
      memchr(body, '\0', body_size))
    return TURBO_EINVAL;
  memset(form, 0, sizeof(*form));
  copy = (char *)malloc(body_size + 1u);
  if (!copy) return TURBO_ENOMEM;
  memcpy(copy, body, body_size);
  copy[body_size] = '\0';
  cursor = copy;
  while (*cursor) {
    char *pair = cursor;
    char *separator = strchr(pair, '&');
    char *equals;
    char *key;
    char *value;
    if (separator) {
      *separator = '\0';
      cursor = separator + 1;
    } else {
      cursor += strlen(cursor);
    }
    if (form->count >= FLOWIE_CONTROL_DASHBOARD_FORM_FIELDS || !(equals = strchr(pair, '=')) ||
        strchr(equals + 1, '='))
      goto done;
    *equals = '\0';
    key = turbo_url_decode(pair);
    value = turbo_url_decode(equals + 1);
    if (!key || !value || !key[0]) {
      free(key);
      free(value);
      goto done;
    }
    for (size_t index = 0u; index < form->count; ++index) {
      if (strcmp(form->fields[index].key, key) == 0) {
        free(key);
        free(value);
        goto done;
      }
    }
    form->fields[form->count].key = key;
    form->fields[form->count].value = value;
    ++form->count;
  }
  rc = form->count > 0u ? TURBO_OK : TURBO_EPROTO;

done:
  free(copy);
  if (rc != TURBO_OK) flowie_control_dashboard_form_destroy(form);
  return rc;
}

static const char *flowie_control_dashboard_form_get(const flowie_control_dashboard_form_t *form,
                                                     const char *key) {
  if (!form || !key) return NULL;
  for (size_t index = 0u; index < form->count; ++index) {
    if (strcmp(form->fields[index].key, key) == 0) return form->fields[index].value;
  }
  return NULL;
}

static int flowie_control_dashboard_form_exact(const flowie_control_dashboard_form_t *form,
                                               const char *const *keys, size_t key_count) {
  if (!form || !keys || form->count != key_count) return 0;
  for (size_t index = 0u; index < key_count; ++index) {
    if (!flowie_control_dashboard_form_get(form, keys[index])) return 0;
  }
  return 1;
}

static int flowie_control_dashboard_u64(const char *text, int required, uint64_t *out) {
  char *end = NULL;
  unsigned long long value;
  if (!text || !text[0]) return required ? TURBO_EPROTO : TURBO_OK;
  if (*text == '+' || *text == '-' || (text[0] == '0' && text[1] != '\0')) return TURBO_EPROTO;
  value = strtoull(text, &end, 10);
  if (!end || *end != '\0') return TURBO_EPROTO;
  *out = (uint64_t)value;
  return TURBO_OK;
}

static int flowie_control_dashboard_page_text(const char *encoded, char *out, size_t capacity) {
  char *decoded;
  char *canonical;
  size_t size;
  int rc = TURBO_EPROTO;
  if (!encoded || !encoded[0] || !out || capacity < 2u) return TURBO_EPROTO;
  decoded = turbo_url_decode(encoded);
  if (!decoded) return TURBO_ENOMEM;
  canonical = turbo_url_encode(decoded);
  if (!canonical) {
    free(decoded);
    return TURBO_ENOMEM;
  }
  size = strnlen(decoded, capacity);
  if (size > 0u && size < capacity && strcmp(encoded, canonical) == 0) {
    memcpy(out, decoded, size + 1u);
    rc = TURBO_OK;
  }
  free(canonical);
  free(decoded);
  return rc;
}

int flowie_control_dashboard_page_parse(const Req *request, flowie_control_dashboard_page_t *out) {
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  if (out && out->size >= sizeof(*out)) *out = page;
  if (!request || !out || out->size < sizeof(*out) || request->query.count < 0 ||
      request->query.count > 5 || (request->query.count > 0 && !request->query.items))
    return TURBO_EINVAL;
  for (int index = 0; index < request->query.count; ++index) {
    const char *key = request->query.items[index].key;
    const char *value = request->query.items[index].value;
    uint64_t number = 0u;
    int rc;
    if (!key || !value) return TURBO_EPROTO;
    if (strcmp(key, "users_after") == 0) {
      if (page.users_after[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.users_after, sizeof(page.users_after));
    } else if (strcmp(key, "groups_after") == 0) {
      if (page.groups_after[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.groups_after, sizeof(page.groups_after));
    } else if (strcmp(key, "roles_after") == 0) {
      if (page.roles_after[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.roles_after, sizeof(page.roles_after));
    } else if (strcmp(key, "policy_after") == 0) {
      if (page.policy_has_after) return TURBO_EPROTO;
      rc = flowie_control_dashboard_u64(value, 1, &number);
      if (rc == TURBO_OK && number >= TURBO_FLOW_SECURITY_MAX_RULES) rc = TURBO_EPROTO;
      if (rc == TURBO_OK) {
        page.policy_after = (uint32_t)number;
        page.policy_has_after = 1;
      }
    } else if (strcmp(key, "audit_after") == 0) {
      if (page.audit_has_after) return TURBO_EPROTO;
      rc = flowie_control_dashboard_u64(value, 1, &number);
      if (rc == TURBO_OK && number > (uint64_t)INT64_MAX) rc = TURBO_EPROTO;
      if (rc == TURBO_OK) {
        page.audit_after = number;
        page.audit_has_after = 1;
      }
    } else {
      return TURBO_EPROTO;
    }
    if (rc != TURBO_OK) return rc;
  }
  *out = page;
  return TURBO_OK;
}

static int flowie_control_dashboard_command_text(const char *text, size_t maximum) {
  size_t size;
  if (!text) return 0;
  size = strnlen(text, maximum + 1u);
  return size > 0u && size <= maximum;
}

int flowie_control_dashboard_process_form(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], const char *body,
    size_t body_size) {
  static const char *const user_keys[] = {"csrf",           "operation",  "principal_id",
                                          "principal_type", "request_id", "expected_revision"};
  static const char *const user_disable_keys[] = {"csrf", "operation", "principal_id", "request_id",
                                                  "expected_revision"};
  static const char *const group_keys[] = {
      "csrf", "operation", "group_id", "parent_group_id", "request_id", "expected_revision"};
  static const char *const group_disable_keys[] = {"csrf", "operation", "group_id", "request_id",
                                                   "expected_revision"};
  static const char *const membership_keys[] = {"csrf",     "operation",  "principal_id",
                                                "group_id", "request_id", "expected_revision"};
  static const char *const role_keys[] = {"csrf", "operation", "role_id", "request_id",
                                          "expected_revision"};
  static const char *const assignment_keys[] = {"csrf",    "operation",  "principal_id",
                                                "role_id", "request_id", "expected_revision"};
  static const char *const rule_keys[] = {"csrf",      "operation",  "ordinal",
                                          "rule_line", "request_id", "expected_revision"};
  static const char *const rule_delete_keys[] = {"csrf", "operation", "ordinal", "request_id",
                                                 "expected_revision"};
  static const char *const publish_keys[] = {"csrf", "operation", "request_id", "expected_revision",
                                             "expires_at"};
  flowie_control_dashboard_form_t form;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *submitted_csrf;
  const char *operation;
  const char *request_id;
  uint64_t expected_revision = 0u;
  uint64_t occurred_at;
  int rc;
  if (!dashboard || !caller || !flowie_control_dashboard_csrf_valid(csrf_token))
    return TURBO_EINVAL;
  rc = flowie_control_dashboard_form_parse(body, body_size, &form);
  if (rc != TURBO_OK) return rc;
  submitted_csrf = flowie_control_dashboard_form_get(&form, "csrf");
  operation = flowie_control_dashboard_form_get(&form, "operation");
  request_id = flowie_control_dashboard_form_get(&form, "request_id");
  if (!submitted_csrf || strlen(submitted_csrf) != FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE ||
      crypto_verify64((const uint8_t *)submitted_csrf, (const uint8_t *)csrf_token) != 0 ||
      !operation ||
      !flowie_control_dashboard_command_text(request_id, FLOWIE_CONTROL_REQUEST_ID_MAX) ||
      flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "expected_revision"), 1,
                                   &expected_revision) != TURBO_OK) {
    rc = TURBO_EPERM;
    goto done;
  }
  occurred_at = dashboard->clock(dashboard->clock_ctx);
  if (occurred_at == 0u) {
    rc = TURBO_EIO;
    goto done;
  }
  if (strcmp(operation, "user.create") == 0) {
    flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, user_keys,
                                             sizeof(user_keys) / sizeof(user_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.principal_type = flowie_control_dashboard_form_get(&form, "principal_type");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "user.disable") == 0) {
    flowie_control_user_disable_command_t command = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, user_disable_keys, sizeof(user_disable_keys) / sizeof(user_disable_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_disable(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.create") == 0) {
    flowie_control_group_create_command_t command = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, group_keys,
                                             sizeof(group_keys) / sizeof(group_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.parent_group_id = flowie_control_dashboard_form_get(&form, "parent_group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_group_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.disable") == 0) {
    flowie_control_group_disable_command_t command = FLOWIE_CONTROL_GROUP_DISABLE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, group_disable_keys,
                                             sizeof(group_disable_keys) /
                                                 sizeof(group_disable_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_group_disable(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.member.add") == 0) {
    flowie_control_membership_add_command_t command = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, membership_keys, sizeof(membership_keys) / sizeof(membership_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_membership_add(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.member.remove") == 0) {
    flowie_control_membership_remove_command_t command =
        FLOWIE_CONTROL_MEMBERSHIP_REMOVE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, membership_keys, sizeof(membership_keys) / sizeof(membership_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_membership_remove(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.create") == 0) {
    flowie_control_role_create_command_t command = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, role_keys,
                                             sizeof(role_keys) / sizeof(role_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_role_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.disable") == 0) {
    flowie_control_role_disable_command_t command = FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, role_keys,
                                             sizeof(role_keys) / sizeof(role_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_role_disable(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.assign") == 0) {
    flowie_control_user_role_add_command_t command = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, assignment_keys, sizeof(assignment_keys) / sizeof(assignment_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_role_add(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.remove") == 0) {
    flowie_control_user_role_remove_command_t command =
        FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, assignment_keys, sizeof(assignment_keys) / sizeof(assignment_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_role_remove(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "policy.rule.put") == 0) {
    flowie_control_policy_rule_put_command_t command = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
    uint64_t ordinal = 0u;
    if (!flowie_control_dashboard_form_exact(&form, rule_keys,
                                             sizeof(rule_keys) / sizeof(rule_keys[0])) ||
        flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "ordinal"), 1,
                                     &ordinal) != TURBO_OK ||
        ordinal >= TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.ordinal = (uint32_t)ordinal;
    command.rule_line = flowie_control_dashboard_form_get(&form, "rule_line");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_policy_rule_put(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "policy.rule.delete") == 0) {
    flowie_control_policy_rule_delete_command_t command =
        FLOWIE_CONTROL_POLICY_RULE_DELETE_COMMAND_INIT;
    uint64_t ordinal = 0u;
    if (!flowie_control_dashboard_form_exact(
            &form, rule_delete_keys, sizeof(rule_delete_keys) / sizeof(rule_delete_keys[0])) ||
        flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "ordinal"), 1,
                                     &ordinal) != TURBO_OK ||
        ordinal >= TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.ordinal = (uint32_t)ordinal;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    rc =
        flowie_control_management_policy_rule_delete(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "policy.publish") == 0) {
    flowie_control_policy_publish_command_t command = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
    flowie_control_policy_publish_result_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
    uint64_t expires_at = 0u;
    if (!flowie_control_dashboard_form_exact(&form, publish_keys,
                                             sizeof(publish_keys) / sizeof(publish_keys[0])) ||
        flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "expires_at"), 0,
                                     &expires_at) != TURBO_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.root_group_id = caller->root_group_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.expected_revision = expected_revision;
    command.occurred_at = occurred_at;
    command.expires_at = expires_at;
    rc = flowie_control_management_policy_publish(dashboard->service, caller, &command, &publish);
  } else {
    rc = TURBO_EPROTO;
  }

done:
  flowie_control_dashboard_form_destroy(&form);
  return rc;
}

static const char *const FLOWIE_CONTROL_DASHBOARD_ROUTES[] = {
    FLOWIE_CONTROL_DASHBOARD_PATH,        FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH,
    FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, FLOWIE_CONTROL_DASHBOARD_CSS_PATH,
    FLOWIE_CONTROL_DASHBOARD_HTMX_PATH};

static void flowie_control_dashboard_headers(Res *response) {
  set_header(response, "Cache-Control", "no-store");
  set_header(response, "Content-Security-Policy", FLOWIE_CONTROL_DASHBOARD_CSP);
  set_header(response, "X-Content-Type-Options", "nosniff");
  set_header(response, "X-Frame-Options", "DENY");
  set_header(response, "Referrer-Policy", "no-referrer");
  set_header(response, "Cross-Origin-Opener-Policy", "same-origin");
  set_header(response, "Cross-Origin-Resource-Policy", "same-origin");
  set_header(response, "Permissions-Policy", "camera=(), microphone=(), geolocation=()");
}

int flowie_control_dashboard_request_is_htmx(const Req *request) {
  const char *header;
  if (!request) return 0;
  header = get_headers(request, "HX-Request");
  return header && strcmp(header, "true") == 0;
}

static int flowie_control_dashboard_status(int rc) {
  return rc == TURBO_EPERM                          ? FORBIDDEN
         : rc == TURBO_EBUSY                        ? CONFLICT
         : rc == TURBO_ENOENT                       ? NOT_FOUND
         : rc == TURBO_EPROTO || rc == TURBO_EINVAL ? BAD_REQUEST
                                                    : INTERNAL_SERVER_ERROR;
}

static const char *flowie_control_dashboard_error_message(int status) {
  return status == FORBIDDEN      ? "The caller is not allowed to perform this operation."
         : status == CONFLICT     ? "The control state changed. Reload and submit again."
         : status == NOT_FOUND    ? "The requested control object does not exist."
         : status == BAD_REQUEST  ? "The request did not match the dashboard contract."
                                  : "The management service could not complete the request.";
}

static flowie_control_dashboard_t *flowie_control_dashboard_from_request(const Req *request) {
  if (!request || !request->app || !request->path) return NULL;
  return (flowie_control_dashboard_t *)iris_app_lookup_rpc_context(request->app, request->path);
}

static int flowie_control_dashboard_resolve(
    flowie_control_dashboard_t *dashboard, const Req *request,
    flowie_control_management_caller_t *caller,
    char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]) {
  int rc;
  if (!dashboard || !request || !caller || !csrf) return TURBO_EINVAL;
  rc = dashboard->resolve_session(dashboard->resolve_session_ctx, request, caller, csrf);
  if (rc == TURBO_OK && !flowie_control_dashboard_csrf_valid(csrf)) rc = TURBO_EPERM;
  return rc;
}

static void flowie_control_dashboard_send_error(flowie_control_dashboard_t *dashboard,
                                                Res *response, int rc) {
  char *html = NULL;
  size_t html_size = 0u;
  int status = flowie_control_dashboard_status(rc);
  int render_rc = flowie_control_dashboard_view_render_error(
      dashboard->view, flowie_control_dashboard_error_message(status), &html, &html_size);
  if (render_rc != TURBO_OK) {
    send_text(response, INTERNAL_SERVER_ERROR, "Internal error");
    return;
  }
  set_header(response, "HX-Retarget", "#dashboard-feedback");
  set_header(response, "HX-Reswap", "innerHTML");
  reply(response, status, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_shell_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    send_text(response, UNAUTHORIZED, "Unauthorized");
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_page_parse(request, &page);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_shell(dashboard, &page, &html, &html_size);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    send_text(response, flowie_control_dashboard_status(rc), "Dashboard unavailable");
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_content_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    send_text(response, UNAUTHORIZED, "Unauthorized");
    return;
  }
  if (!flowie_control_dashboard_request_is_htmx(request)) {
    send_text(response, BAD_REQUEST, "HTMX request required");
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_page_parse(request, &page);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_page(dashboard, &caller, csrf, &page, &html, &html_size);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    flowie_control_dashboard_send_error(dashboard, response, rc);
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_post_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  const char *content_type;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    send_text(response, UNAUTHORIZED, "Unauthorized");
    return;
  }
  if (!flowie_control_dashboard_request_is_htmx(request)) {
    send_text(response, BAD_REQUEST, "HTMX request required");
    return;
  }
  content_type = get_headers(request, "Content-Type");
  if (!content_type || strcmp(content_type, "application/x-www-form-urlencoded") != 0 ||
      request->body_len == 0u || request->body_len > FLOWIE_CONTROL_DASHBOARD_BODY_MAX) {
    flowie_control_dashboard_send_error(dashboard, response, TURBO_EPROTO);
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_page_parse(request, &page);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_process_form(dashboard, &caller, csrf, request->body,
                                               request->body_len);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_page(dashboard, &caller, csrf, &page, &html, &html_size);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    flowie_control_dashboard_send_error(dashboard, response, rc);
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_asset_handler(Req *request, Res *response,
                                                   flowie_control_dashboard_asset_t asset,
                                                   const char *content_type) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  const void *data = NULL;
  size_t size = 0u;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    send_text(response, UNAUTHORIZED, "Unauthorized");
    return;
  }
  rc = flowie_control_dashboard_view_asset(dashboard->view, asset, &data, &size);
  if (rc != TURBO_OK) {
    send_text(response, INTERNAL_SERVER_ERROR, "Asset unavailable");
    return;
  }
  reply(response, OK, content_type, data, size);
}

static void flowie_control_dashboard_css_handler(Req *request, Res *response) {
  flowie_control_dashboard_asset_handler(request, response, FLOWIE_CONTROL_DASHBOARD_ASSET_CSS,
                                         "text/css; charset=utf-8");
}

static void flowie_control_dashboard_htmx_handler(Req *request, Res *response) {
  flowie_control_dashboard_asset_handler(request, response, FLOWIE_CONTROL_DASHBOARD_ASSET_HTMX,
                                         "text/javascript; charset=utf-8");
}

int flowie_control_dashboard_create(const flowie_control_dashboard_config_t *config,
                                    flowie_control_dashboard_t **out) {
  flowie_control_dashboard_t *dashboard;
  const char *resource_directory;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !config->service || !config->resolve_session ||
      !config->clock || !out)
    return TURBO_EINVAL;
  resource_directory = config->resource_directory;
#ifdef FLOWIE_CONTROL_DASHBOARD_RESOURCE_DIR
  if (!resource_directory) resource_directory = FLOWIE_CONTROL_DASHBOARD_RESOURCE_DIR;
#endif
  if (!resource_directory || resource_directory[0] == '\0') return TURBO_EINVAL;
  dashboard = (flowie_control_dashboard_t *)calloc(1u, sizeof(*dashboard));
  if (!dashboard) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_view_create(resource_directory, &dashboard->view);
  if (rc != TURBO_OK) {
    free(dashboard);
    return rc;
  }
  dashboard->service = config->service;
  dashboard->resolve_session = config->resolve_session;
  dashboard->resolve_session_ctx = config->resolve_session_ctx;
  dashboard->clock = config->clock;
  dashboard->clock_ctx = config->clock_ctx;
  *out = dashboard;
  return TURBO_OK;
}

int flowie_control_dashboard_bind(flowie_control_dashboard_t *dashboard, iris_app_t *app) {
  size_t bound = 0u;
  if (!dashboard || !app || dashboard->bound_app) return TURBO_EINVAL;
  for (size_t index = 0u;
       index < sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) / sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0]);
       ++index) {
    if (iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROUTES[index]))
      return TURBO_EBUSY;
  }
  for (; bound < sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) /
                         sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0]);
       ++bound) {
    if (iris_app_bind_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROUTES[bound], dashboard) != 0)
      break;
  }
  if (bound != sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) /
                   sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0])) {
    while (bound > 0u) {
      --bound;
      (void)iris_app_unbind_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROUTES[bound], dashboard);
    }
    return TURBO_EBUSY;
  }
  dashboard->bound_app = app;
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, flowie_control_dashboard_content_handler);
  iris_app_post(app, FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, flowie_control_dashboard_post_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_CSS_PATH, flowie_control_dashboard_css_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_HTMX_PATH, flowie_control_dashboard_htmx_handler);
  return TURBO_OK;
}

void flowie_control_dashboard_unbind(flowie_control_dashboard_t *dashboard) {
  if (!dashboard || !dashboard->bound_app) return;
  for (size_t index = sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) /
                          sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0]);
       index > 0u; --index) {
    (void)iris_app_unbind_rpc_context(dashboard->bound_app,
                                      FLOWIE_CONTROL_DASHBOARD_ROUTES[index - 1u], dashboard);
  }
  dashboard->bound_app = NULL;
}

void flowie_control_dashboard_destroy(flowie_control_dashboard_t *dashboard) {
  if (!dashboard) return;
  flowie_control_dashboard_unbind(dashboard);
  flowie_control_dashboard_view_destroy(dashboard->view);
  memset(dashboard, 0, sizeof(*dashboard));
  free(dashboard);
}
