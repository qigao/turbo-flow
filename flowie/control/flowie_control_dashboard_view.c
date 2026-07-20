#include "flowie_control_dashboard_view_internal.h"

#include "fmt.h"
#include "http_common.h"
#include "turbo_parser.h"
#include "mustache_json.h"
#include "turbo_error.h"
#include "turbo_fs.h"
#include "turbo_str.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

enum {
  FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE = 25,
  FLOWIE_CONTROL_DASHBOARD_RESOURCE_PATH_MAX = 1024,
  FLOWIE_CONTROL_DASHBOARD_TEMPLATE_MAX = 512 * 1024,
  FLOWIE_CONTROL_DASHBOARD_ASSET_MAX = 1024 * 1024
};

static const char FLOWIE_CONTROL_DASHBOARD_SHELL_TEMPLATE[] = "templates/dashboard.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_CONTENT_TEMPLATE[] =
    "templates/dashboard_content.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_ERROR_TEMPLATE[] =
    "templates/dashboard_error.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_CSS_ASSET[] = "assets/control.css";
static const char FLOWIE_CONTROL_DASHBOARD_HTMX_ASSET[] = "assets/htmx-2.0.9.min.js";

typedef enum flowie_control_dashboard_cursor_kind_e {
  FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR = 0,
  FLOWIE_CONTROL_DASHBOARD_GROUPS_CURSOR,
  FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR,
  FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR,
  FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR
} flowie_control_dashboard_cursor_kind_t;

struct flowie_control_dashboard_view_s {
  MUSTACHE_TEMPLATE *shell_template;
  MUSTACHE_TEMPLATE *content_template;
  MUSTACHE_TEMPLATE *error_template;
  turbo_fs_buf_t css;
  turbo_fs_buf_t htmx;
};

static void flowie_control_dashboard_json_free(json_value_t *value) {
  turbo_json_doc_t *document = value;
  turbo_free_json(&document);
}

static int flowie_control_dashboard_json_take(json_value_t *object, const char *key,
                                              json_value_t *value) {
  if (!object || !key || !value) {
    flowie_control_dashboard_json_free(value);
    return TURBO_ENOMEM;
  }
  if (!turbo_json_object_add_checked(object, key, value)) {
    flowie_control_dashboard_json_free(value);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flowie_control_dashboard_json_array_take(json_value_t *array, json_value_t *value) {
  if (!array || !value) {
    flowie_control_dashboard_json_free(value);
    return TURBO_ENOMEM;
  }
  if (!turbo_json_array_add_checked(array, value)) {
    flowie_control_dashboard_json_free(value);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flowie_control_dashboard_json_string(json_value_t *object, const char *key,
                                                const char *value) {
  if (!value) return TURBO_EINVAL;
  return flowie_control_dashboard_json_take(object, key, turbo_json_create_string(value));
}

static int flowie_control_dashboard_json_u64(json_value_t *object, const char *key,
                                             uint64_t value) {
  return flowie_control_dashboard_json_take(object, key, turbo_json_create_uint64(value));
}

static int flowie_control_dashboard_json_bool(json_value_t *object, const char *key, int value) {
  return flowie_control_dashboard_json_take(object, key, turbo_json_create_bool(value != 0));
}

static int flowie_control_dashboard_read(const char *resource_directory, const char *relative_path,
                                         size_t maximum, turbo_fs_buf_t *out) {
  char path[FLOWIE_CONTROL_DASHBOARD_RESOURCE_PATH_MAX];
  int rc;
  if (!resource_directory || !relative_path || !out) return TURBO_EINVAL;
  memset(out, 0, sizeof(*out));
  rc = turbo_fs_path_join(path, sizeof(path), resource_directory, relative_path);
  if (rc != TURBO_OK) return rc;
  rc = turbo_fs_read_file(path, out);
  if (rc != TURBO_OK) return rc;
  if (!out->base || out->len == 0u || out->len > maximum) {
    turbo_fs_buf_free(out);
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flowie_control_dashboard_compile(const char *resource_directory,
                                            const char *relative_path,
                                            MUSTACHE_TEMPLATE **template_out) {
  turbo_fs_buf_t source = {0};
  MUSTACHE_TEMPLATE *compiled;
  int rc;
  if (template_out) *template_out = NULL;
  if (!template_out) return TURBO_EINVAL;
  rc = flowie_control_dashboard_read(resource_directory, relative_path,
                                     FLOWIE_CONTROL_DASHBOARD_TEMPLATE_MAX, &source);
  if (rc != TURBO_OK) return rc;
  compiled = mustache_compile(source.base, source.len, NULL, NULL, 0u);
  turbo_fs_buf_free(&source);
  if (!compiled) return TURBO_EPROTO;
  *template_out = compiled;
  return TURBO_OK;
}

static int flowie_control_dashboard_render_template(const MUSTACHE_TEMPLATE *template_value,
                                                    json_value_t *model, char **html_out,
                                                    size_t *html_size_out) {
  MUSTACHE_STRING_RENDERER renderer = {0};
  char *html = NULL;
  int rc = TURBO_ENOMEM;
  if (html_out) *html_out = NULL;
  if (html_size_out) *html_size_out = 0u;
  if (!template_value || !model || !html_out || !html_size_out) return TURBO_EINVAL;
  if (mustache_string_renderer_init(&renderer) != 0) return TURBO_ENOMEM;
  if (mustache_render_json(template_value, model, &renderer.base, &renderer, NULL, NULL) != 0)
    goto done;
  html = mustache_string_renderer_get(&renderer);
  if (!html) goto done;
  *html_size_out = strlen(html);
  *html_out = html;
  rc = TURBO_OK;
done:
  mustache_string_renderer_free(&renderer);
  return rc;
}

static int flowie_control_dashboard_page_has_cursor(const flowie_control_dashboard_page_t *page,
                                                    flowie_control_dashboard_cursor_kind_t kind) {
  switch (kind) {
  case FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR:
    return page->users_after[0] != '\0';
  case FLOWIE_CONTROL_DASHBOARD_GROUPS_CURSOR:
    return page->groups_after[0] != '\0';
  case FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR:
    return page->roles_after[0] != '\0';
  case FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR:
    return page->policy_has_after;
  case FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR:
    return page->audit_has_after;
  default:
    return 0;
  }
}

static void flowie_control_dashboard_page_clear_cursor(flowie_control_dashboard_page_t *page,
                                                       flowie_control_dashboard_cursor_kind_t kind) {
  switch (kind) {
  case FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR:
    page->users_after[0] = '\0';
    break;
  case FLOWIE_CONTROL_DASHBOARD_GROUPS_CURSOR:
    page->groups_after[0] = '\0';
    break;
  case FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR:
    page->roles_after[0] = '\0';
    break;
  case FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR:
    page->policy_after = 0u;
    page->policy_has_after = 0;
    break;
  case FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR:
    page->audit_after = 0u;
    page->audit_has_after = 0;
    break;
  }
}

static int flowie_control_dashboard_page_set_cursor(flowie_control_dashboard_page_t *page,
                                                    flowie_control_dashboard_cursor_kind_t kind,
                                                    const char *next_text,
                                                    uint64_t next_number) {
  switch (kind) {
  case FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR:
    if (!next_text) return TURBO_EINVAL;
    (void)snprintf(page->users_after, sizeof(page->users_after), "%s", next_text);
    break;
  case FLOWIE_CONTROL_DASHBOARD_GROUPS_CURSOR:
    if (!next_text) return TURBO_EINVAL;
    (void)snprintf(page->groups_after, sizeof(page->groups_after), "%s", next_text);
    break;
  case FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR:
    if (!next_text) return TURBO_EINVAL;
    (void)snprintf(page->roles_after, sizeof(page->roles_after), "%s", next_text);
    break;
  case FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR:
    page->policy_after = (uint32_t)next_number;
    page->policy_has_after = 1;
    break;
  case FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR:
    page->audit_after = next_number;
    page->audit_has_after = 1;
    break;
  default:
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flowie_control_dashboard_url_pair(tstr_t *url, int *has_query, const char *key,
                                             const char *value) {
  char *encoded;
  tstr_t next;
  if (!url || !*url || !has_query || !key || !value) return TURBO_EINVAL;
  encoded = turbo_url_encode(value);
  if (!encoded) return TURBO_ENOMEM;
  next = tstr_append_format(*url, "{}{}={}", *has_query ? "&" : "?", key, encoded);
  free(encoded);
  if (!next) {
    *url = NULL;
    return TURBO_ENOMEM;
  }
  *url = next;
  *has_query = 1;
  return TURBO_OK;
}

static int flowie_control_dashboard_url(const char *base,
                                        const flowie_control_dashboard_page_t *page,
                                        char **url_out) {
  char number[32];
  tstr_t url;
  int has_query = 0;
  int rc = TURBO_OK;
  if (url_out) *url_out = NULL;
  if (!base || !page || !url_out) return TURBO_EINVAL;
  url = tstr_dup(base);
  if (!url) return TURBO_ENOMEM;
  if (page->users_after[0])
    rc = flowie_control_dashboard_url_pair(&url, &has_query, "users_after", page->users_after);
  if (rc == TURBO_OK && page->groups_after[0])
    rc = flowie_control_dashboard_url_pair(&url, &has_query, "groups_after", page->groups_after);
  if (rc == TURBO_OK && page->roles_after[0])
    rc = flowie_control_dashboard_url_pair(&url, &has_query, "roles_after", page->roles_after);
  if (rc == TURBO_OK && page->policy_has_after) {
    (void)snprintf(number, sizeof(number), "%u", page->policy_after);
    rc = flowie_control_dashboard_url_pair(&url, &has_query, "policy_after", number);
  }
  if (rc == TURBO_OK && page->audit_has_after) {
    (void)snprintf(number, sizeof(number), "%llu", (unsigned long long)page->audit_after);
    rc = flowie_control_dashboard_url_pair(&url, &has_query, "audit_after", number);
  }
  if (rc != TURBO_OK) {
    tstr_free(url);
    return rc;
  }
  *url_out = url;
  return TURBO_OK;
}

static int flowie_control_dashboard_add_pager(
    json_value_t *model, const char *key, const flowie_control_dashboard_page_t *page,
    flowie_control_dashboard_cursor_kind_t kind, int has_more, const char *next_text,
    uint64_t next_number) {
  flowie_control_dashboard_page_t target = *page;
  json_value_t *pager = turbo_json_create_object();
  char *url = NULL;
  int has_first = flowie_control_dashboard_page_has_cursor(page, kind);
  int rc;
  if (!pager) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_json_bool(pager, "first", has_first);
  if (rc == TURBO_OK && has_first) {
    flowie_control_dashboard_page_clear_cursor(&target, kind);
    rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, &target, &url);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(pager, "first_url", url);
    tstr_free(url);
    url = NULL;
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(pager, "more", has_more);
  if (rc == TURBO_OK && has_more) {
    target = *page;
    rc = flowie_control_dashboard_page_set_cursor(&target, kind, next_text, next_number);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, &target, &url);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(pager, "more_url", url);
    tstr_free(url);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, key, pager);
  else flowie_control_dashboard_json_free(pager);
  return rc;
}

static int flowie_control_dashboard_add_users(
    json_value_t *model, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_dashboard_page_t *page) {
  flowie_control_user_view_t users[FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE; ++index)
    users[index] = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
  rc = flowie_control_management_user_list(service, caller,
                                           page->users_after[0] ? page->users_after : NULL, users,
                                           FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, &count, &has_more);
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_string(item, "principal_id", users[index].principal_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "principal_type",
                                                users[index].principal_type);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", users[index].enabled);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_u64(item, "revision", users[index].revision);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "users", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "users_pager", page, FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR,
        has_more && count > 0u, count > 0u ? users[count - 1u].principal_id : NULL, 0u);
  return rc;
}

static int flowie_control_dashboard_add_groups(
    json_value_t *model, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_dashboard_page_t *page) {
  flowie_control_group_view_t groups[FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE; ++index)
    groups[index] = (flowie_control_group_view_t)FLOWIE_CONTROL_GROUP_VIEW_INIT;
  rc = flowie_control_management_group_list(service, caller,
                                            page->groups_after[0] ? page->groups_after : NULL,
                                            groups, FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, &count,
                                            &has_more);
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_string(item, "group_id", groups[index].group_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "parent_group_id",
                                                groups[index].parent_group_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_u64(item, "depth", groups[index].depth);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", groups[index].enabled);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "groups", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "groups_pager", page, FLOWIE_CONTROL_DASHBOARD_GROUPS_CURSOR,
        has_more && count > 0u, count > 0u ? groups[count - 1u].group_id : NULL, 0u);
  return rc;
}

static int flowie_control_dashboard_add_roles(
    json_value_t *model, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_dashboard_page_t *page) {
  flowie_control_role_view_t roles[FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE; ++index)
    roles[index] = (flowie_control_role_view_t)FLOWIE_CONTROL_ROLE_VIEW_INIT;
  rc = flowie_control_management_role_list(service, caller,
                                           page->roles_after[0] ? page->roles_after : NULL, roles,
                                           FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, &count, &has_more);
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_string(item, "role_id", roles[index].role_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", roles[index].enabled);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_u64(item, "revision", roles[index].revision);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "roles", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "roles_pager", page, FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR,
        has_more && count > 0u, count > 0u ? roles[count - 1u].role_id : NULL, 0u);
  return rc;
}

static int flowie_control_dashboard_add_rules(
    json_value_t *model, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_dashboard_page_t *page) {
  flowie_control_policy_rule_view_t rules[FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE; ++index)
    rules[index] = (flowie_control_policy_rule_view_t)FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
  rc = flowie_control_management_policy_rule_list(
      service, caller, page->policy_after, page->policy_has_after, rules,
      FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, &count, &has_more);
  if (rc == TURBO_ENOENT) rc = TURBO_OK;
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_u64(item, "ordinal", rules[index].ordinal);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "rule_line", rules[index].rule_line);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_u64(item, "revision", rules[index].revision);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "rules", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "policy_pager", page, FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR,
        has_more && count > 0u, NULL, count > 0u ? rules[count - 1u].ordinal : 0u);
  return rc;
}

static int flowie_control_dashboard_add_audits(
    json_value_t *model, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_dashboard_page_t *page) {
  flowie_control_audit_view_t *audits = NULL;
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int show = (caller->permissions & FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN) != 0u;
  int rc = TURBO_OK;
  if (!array) return TURBO_ENOMEM;
  if (show) {
    audits = (flowie_control_audit_view_t *)calloc(FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE,
                                                   sizeof(*audits));
    if (!audits) {
      flowie_control_dashboard_json_free(array);
      return TURBO_ENOMEM;
    }
    for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE; ++index)
      audits[index] = (flowie_control_audit_view_t)FLOWIE_CONTROL_AUDIT_VIEW_INIT;
    rc = flowie_control_management_audit_list(service, caller, page->audit_after, audits,
                                              FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, &count,
                                              &has_more);
  }
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_u64(item, "revision", audits[index].revision);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "actor", audits[index].actor);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "operation", audits[index].operation);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "target_id", audits[index].target_id);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "show_audit", show);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "audits", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "audit_pager", page, FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR,
        has_more && count > 0u, NULL, count > 0u ? audits[count - 1u].revision : 0u);
  free(audits);
  return rc;
}

int flowie_control_dashboard_view_create(const char *resource_directory,
                                         flowie_control_dashboard_view_t **out) {
  flowie_control_dashboard_view_t *view;
  int rc;
  if (out) *out = NULL;
  if (!resource_directory || !out) return TURBO_EINVAL;
  view = (flowie_control_dashboard_view_t *)calloc(1u, sizeof(*view));
  if (!view) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_compile(resource_directory,
                                        FLOWIE_CONTROL_DASHBOARD_SHELL_TEMPLATE,
                                        &view->shell_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_compile(resource_directory,
                                          FLOWIE_CONTROL_DASHBOARD_CONTENT_TEMPLATE,
                                          &view->content_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_compile(resource_directory,
                                          FLOWIE_CONTROL_DASHBOARD_ERROR_TEMPLATE,
                                          &view->error_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_read(resource_directory, FLOWIE_CONTROL_DASHBOARD_CSS_ASSET,
                                       FLOWIE_CONTROL_DASHBOARD_ASSET_MAX, &view->css);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_read(resource_directory, FLOWIE_CONTROL_DASHBOARD_HTMX_ASSET,
                                       FLOWIE_CONTROL_DASHBOARD_ASSET_MAX, &view->htmx);
  if (rc != TURBO_OK) {
    flowie_control_dashboard_view_destroy(view);
    return rc;
  }
  *out = view;
  return TURBO_OK;
}

void flowie_control_dashboard_view_destroy(flowie_control_dashboard_view_t *view) {
  if (!view) return;
  mustache_release(view->error_template);
  mustache_release(view->content_template);
  mustache_release(view->shell_template);
  turbo_fs_buf_free(&view->htmx);
  turbo_fs_buf_free(&view->css);
  memset(view, 0, sizeof(*view));
  free(view);
}

int flowie_control_dashboard_view_render_shell(
    flowie_control_dashboard_view_t *view, const flowie_control_dashboard_page_t *page,
    char **html_out, size_t *html_size_out) {
  json_value_t *model = NULL;
  char *content_url = NULL;
  int rc;
  if (!view || !page) return TURBO_EINVAL;
  rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, page, &content_url);
  if (rc != TURBO_OK) return rc;
  model = turbo_json_create_object();
  if (!model) rc = TURBO_ENOMEM;
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_string(model, "content_url", content_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->shell_template, model, html_out,
                                                  html_size_out);
  tstr_free(content_url);
  flowie_control_dashboard_json_free(model);
  return rc;
}

int flowie_control_dashboard_view_render_content(
    flowie_control_dashboard_view_t *view, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page, char **html_out, size_t *html_size_out) {
  flowie_control_management_status_t status = FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT;
  json_value_t *model = NULL;
  char *action_url = NULL;
  int rc;
  if (!view || !service || !caller || !caller->root_group_id || !csrf_token || !page)
    return TURBO_EINVAL;
  rc = flowie_control_management_system_status(service, caller, &status);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, page, &action_url);
  if (rc != TURBO_OK) return rc;
  model = turbo_json_create_object();
  if (!model) rc = TURBO_ENOMEM;
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_string(model, "root_group_id", caller->root_group_id);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "csrf", csrf_token);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "action_url", action_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "store_revision", status.store_revision);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "policy_version", status.policy.policy_version);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "draft_rule_count",
                                           status.policy.draft_rule_count);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "published_rule_count",
                                           status.policy.published_rule_count);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "can_user_admin",
        (caller->permissions & (FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN |
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN)) != 0u);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "can_security_admin",
        (caller->permissions & FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN) != 0u);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "can_policy_admin",
        (caller->permissions & (FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN |
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN)) != 0u);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_add_users(model, service, caller, page);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_add_groups(model, service, caller, page);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_add_roles(model, service, caller, page);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_add_rules(model, service, caller, page);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_add_audits(model, service, caller, page);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->content_template, model, html_out,
                                                  html_size_out);
  tstr_free(action_url);
  flowie_control_dashboard_json_free(model);
  return rc;
}

int flowie_control_dashboard_view_render_error(flowie_control_dashboard_view_t *view,
                                               const char *message, char **html_out,
                                               size_t *html_size_out) {
  json_value_t *model;
  int rc;
  if (!view || !message) return TURBO_EINVAL;
  model = turbo_json_create_object();
  if (!model) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_json_string(model, "message", message);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->error_template, model, html_out,
                                                  html_size_out);
  flowie_control_dashboard_json_free(model);
  return rc;
}

int flowie_control_dashboard_view_asset(const flowie_control_dashboard_view_t *view,
                                        flowie_control_dashboard_asset_t asset,
                                        const void **data_out, size_t *size_out) {
  const turbo_fs_buf_t *resource;
  if (data_out) *data_out = NULL;
  if (size_out) *size_out = 0u;
  if (!view || !data_out || !size_out) return TURBO_EINVAL;
  switch (asset) {
  case FLOWIE_CONTROL_DASHBOARD_ASSET_CSS:
    resource = &view->css;
    break;
  case FLOWIE_CONTROL_DASHBOARD_ASSET_HTMX:
    resource = &view->htmx;
    break;
  default:
    return TURBO_EINVAL;
  }
  if (!resource->base || resource->len == 0u) return TURBO_EPROTO;
  *data_out = resource->base;
  *size_out = resource->len;
  return TURBO_OK;
}
