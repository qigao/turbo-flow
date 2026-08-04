#include "flowie_control_dashboard_view_internal.h"

#include "fmt.h"
#include "http_common.h"
#include "mustache_json.h"
#include "turbo_error.h"
#include "turbo_fs.h"
#include "turbo_parser.h"
#include "turbo_str.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE = 25,
  FLOWIE_CONTROL_DASHBOARD_DOMAIN_LIMIT = 100,
  FLOWIE_CONTROL_DASHBOARD_GROUP_SELECTOR_LIMIT = FLOWIE_CONTROL_PAGE_MAX,
  FLOWIE_CONTROL_DASHBOARD_ROLE_SELECTOR_LIMIT = FLOWIE_CONTROL_PAGE_MAX,
  FLOWIE_CONTROL_DASHBOARD_GROUP_LABEL_MAX =
      TURBO_FLOW_SECURITY_ID_MAX + FLOWIE_CONTROL_GROUP_MAX_DEPTH * 2 + 2,
  FLOWIE_CONTROL_DASHBOARD_RESOURCE_LABEL_MAX = TURBO_FLOW_SECURITY_PATTERN_MAX * 3 + 1,
  FLOWIE_CONTROL_DASHBOARD_RESOURCE_PATH_MAX = 1024,
  FLOWIE_CONTROL_DASHBOARD_TEMPLATE_MAX = 512 * 1024,
  FLOWIE_CONTROL_DASHBOARD_ASSET_MAX = 1024 * 1024
};

static const char FLOWIE_CONTROL_DASHBOARD_SHELL_TEMPLATE[] = "templates/dashboard.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_CONTENT_TEMPLATE[] =
    "templates/dashboard_content.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_ERROR_TEMPLATE[] = "templates/dashboard_error.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_LOGIN_TEMPLATE[] = "templates/login.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_PASSWORD_TEMPLATE[] = "templates/password.mustache";
static const char FLOWIE_CONTROL_DASHBOARD_CSS_ASSET[] = "assets/control.css";
static const char FLOWIE_CONTROL_DASHBOARD_JS_ASSET[] = "assets/control.js";
static const char FLOWIE_CONTROL_DASHBOARD_HTMX_ASSET[] = "assets/htmx-2.0.9.min.js";

typedef enum flowie_control_dashboard_cursor_kind_e {
  FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR = 0,
  FLOWIE_CONTROL_DASHBOARD_GROUPS_CURSOR,
  FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR,
  FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR,
  FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR
} flowie_control_dashboard_cursor_kind_t;

static const char *
flowie_control_dashboard_section_name(flowie_control_dashboard_section_t section) {
  switch (section) {
  case FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW:
    return "overview";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_USERS:
    return "users";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS:
    return "groups";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES:
    return "roles";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS:
    return "acls";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT:
    return "audit";
  default:
    return NULL;
  }
}

static const char *
flowie_control_dashboard_section_title(flowie_control_dashboard_section_t section) {
  switch (section) {
  case FLOWIE_CONTROL_DASHBOARD_SECTION_USERS:
    return "Users | Flowie Control";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS:
    return "Groups | Flowie Control";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES:
    return "Roles | Flowie Control";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS:
    return "ACL rules | Flowie Control";
  case FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT:
    return "Audit | Flowie Control";
  default:
    return "Flowie Control";
  }
}

struct flowie_control_dashboard_view_s {
  MUSTACHE_TEMPLATE *shell_template;
  MUSTACHE_TEMPLATE *content_template;
  MUSTACHE_TEMPLATE *error_template;
  MUSTACHE_TEMPLATE *login_template;
  MUSTACHE_TEMPLATE *password_template;
  turbo_fs_buf_t css;
  turbo_fs_buf_t javascript;
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

static void
flowie_control_dashboard_page_clear_cursor(flowie_control_dashboard_page_t *page,
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
                                                    const char *next_text, uint64_t next_number) {
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
  if (page->domain_id[0])
    rc = flowie_control_dashboard_url_pair(&url, &has_query, "domain_id", page->domain_id);
  if (page->section != FLOWIE_CONTROL_DASHBOARD_SECTION_ALL) {
    const char *section = flowie_control_dashboard_section_name(page->section);
    if (!section) {
      tstr_free(url);
      return TURBO_EINVAL;
    }
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_url_pair(&url, &has_query, "section", section);
  }
  if (rc == TURBO_OK && page->users_after[0])
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

static int flowie_control_dashboard_navigation_url(const char *base,
                                                   const flowie_control_dashboard_page_t *page,
                                                   char **url_out) {
  flowie_control_dashboard_page_t target;
  if (!base || !page || !url_out) return TURBO_EINVAL;
  target = (flowie_control_dashboard_page_t)FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  memcpy(target.domain_id, page->domain_id, sizeof(target.domain_id));
  return flowie_control_dashboard_url(base, &target, url_out);
}

static int
flowie_control_dashboard_add_domains(json_value_t *model,
                                         flowie_control_management_service_t *service,
                                         const flowie_control_management_caller_t *authority_caller,
                                         const flowie_control_management_caller_t *scoped_caller) {
  flowie_control_domain_view_t roots[FLOWIE_CONTROL_DASHBOARD_DOMAIN_LIMIT];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_DOMAIN_LIMIT; ++index)
    roots[index] = (flowie_control_domain_view_t)FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  rc = flowie_control_management_domain_list(service, authority_caller, NULL, roots,
                                                 FLOWIE_CONTROL_DASHBOARD_DOMAIN_LIMIT, &count,
                                                 &has_more);
  (void)has_more;
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_string(item, "domain_id", roots[index].domain_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(
          item, "selected", strcmp(roots[index].domain_id, scoped_caller->domain_id) == 0);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "domains", array);
  else flowie_control_dashboard_json_free(array);
  return rc;
}

static int flowie_control_dashboard_add_pager(json_value_t *model, const char *key,
                                              const flowie_control_dashboard_page_t *page,
                                              flowie_control_dashboard_cursor_kind_t kind,
                                              size_t count, int has_more, const char *next_text,
                                              uint64_t next_number) {
  flowie_control_dashboard_page_t target = *page;
  json_value_t *pager = turbo_json_create_object();
  char *url = NULL;
  int has_first = flowie_control_dashboard_page_has_cursor(page, kind);
  int rc;
  if (!pager) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_json_u64(pager, "count", count);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, page, &url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(pager, "refresh_url", url);
  tstr_free(url);
  url = NULL;
  target = *page;
  flowie_control_dashboard_page_clear_cursor(&target, kind);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, &target, &url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(pager, "query_url", url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(pager, "first", has_first);
  if (rc == TURBO_OK && has_first)
    rc = flowie_control_dashboard_json_string(pager, "first_url", url);
  tstr_free(url);
  url = NULL;
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

static int
flowie_control_dashboard_group_label(const flowie_control_group_view_t *group,
                                     char label[FLOWIE_CONTROL_DASHBOARD_GROUP_LABEL_MAX]) {
  size_t group_id_size;
  size_t offset = 0u;
  if (!group || !label || group->depth > FLOWIE_CONTROL_GROUP_MAX_DEPTH) return TURBO_EINVAL;
  group_id_size = strnlen(group->group_id, sizeof(group->group_id));
  if (group_id_size == 0u || group_id_size >= sizeof(group->group_id) ||
      group->depth * 2u + (group->depth ? 1u : 0u) + group_id_size + 1u >
          FLOWIE_CONTROL_DASHBOARD_GROUP_LABEL_MAX)
    return TURBO_EPROTO;
  for (uint32_t depth = 0u; depth < group->depth; ++depth) {
    label[offset++] = '-';
    label[offset++] = '-';
  }
  if (group->depth) label[offset++] = ' ';
  memcpy(label + offset, group->group_id, group_id_size + 1u);
  return TURBO_OK;
}

static int flowie_control_dashboard_add_group_option(json_value_t *array,
                                                     const flowie_control_group_view_t *group,
                                                     size_t row_index, int has_children) {
  char label[FLOWIE_CONTROL_DASHBOARD_GROUP_LABEL_MAX];
  json_value_t *item;
  int rc;
  if (!array || !group) return TURBO_EINVAL;
  rc = flowie_control_dashboard_group_label(group, label);
  if (rc != TURBO_OK) return rc;
  item = turbo_json_create_object();
  if (!item) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_json_u64(item, "row_index", row_index);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_string(item, "group_id", group->group_id);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_string(item, "parent_group_id", group->parent_group_id);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(item, "tree_label", label);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_u64(item, "depth", group->depth);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(item, "aria_level", (uint64_t)group->depth + 1u);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(item, "enabled", group->enabled);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(item, "is_root", 0);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        item, "member_allowed", group->enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        item, "delete_candidate", !has_children);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(item, "add_disabled",
                                            !group->enabled);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(item, "remove_disabled", 0);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        item, "parent_disabled", !group->enabled || group->depth >= FLOWIE_CONTROL_GROUP_MAX_DEPTH);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
  else flowie_control_dashboard_json_free(item);
  return rc;
}

static int flowie_control_dashboard_group_has_children(
    const flowie_control_group_view_t *groups, size_t count, const char *group_id) {
  if (!groups || !group_id) return 0;
  for (size_t index = 0u; index < count; ++index) {
    if (strcmp(groups[index].parent_group_id, group_id) == 0) return 1;
  }
  return 0;
}

static int flowie_control_dashboard_add_group_children(json_value_t *array,
                                                       const flowie_control_group_view_t *groups,
                                                       size_t count, const char *parent_group_id,
                                                       uint32_t depth, size_t *emitted) {
  int rc = TURBO_OK;
  if (!array || !groups || !parent_group_id || !emitted || depth > FLOWIE_CONTROL_GROUP_MAX_DEPTH)
    return TURBO_EINVAL;
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    if (groups[index].depth != depth || strcmp(groups[index].parent_group_id, parent_group_id) != 0)
      continue;
    rc = flowie_control_dashboard_add_group_option(
        array, &groups[index], *emitted + 1u,
        flowie_control_dashboard_group_has_children(groups, count, groups[index].group_id));
    if (rc == TURBO_OK) ++*emitted;
    if (rc == TURBO_OK && depth < FLOWIE_CONTROL_GROUP_MAX_DEPTH)
      rc = flowie_control_dashboard_add_group_children(array, groups, count, groups[index].group_id,
                                                       depth + 1u, emitted);
  }
  return rc;
}

static int
flowie_control_dashboard_add_group_options(json_value_t *model,
                                           flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller) {
  flowie_control_group_view_t groups[FLOWIE_CONTROL_DASHBOARD_GROUP_SELECTOR_LIMIT];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  size_t emitted = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  memset(groups, 0, sizeof(groups));
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_GROUP_SELECTOR_LIMIT; ++index)
    groups[index].size = sizeof(groups[index]);
  rc = flowie_control_management_group_list(service, caller, NULL, groups,
                                            FLOWIE_CONTROL_DASHBOARD_GROUP_SELECTOR_LIMIT, &count,
                                            &has_more);
  if (rc == TURBO_OK && has_more) rc = TURBO_ENOSPC;
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    if (groups[index].depth != 0u || groups[index].parent_group_id[0]) continue;
    rc = flowie_control_dashboard_add_group_option(
        array, &groups[index], emitted + 1u,
        flowie_control_dashboard_group_has_children(groups, count, groups[index].group_id));
    if (rc == TURBO_OK) ++emitted;
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_add_group_children(array, groups, count, groups[index].group_id,
                                                       1u, &emitted);
  }
  if (rc == TURBO_OK && emitted != count) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "group_options", array);
  else flowie_control_dashboard_json_free(array);
  return rc;
}

static int
flowie_control_dashboard_add_user_options(json_value_t *model,
                                          flowie_control_management_service_t *service,
                                          const flowie_control_management_caller_t *caller) {
  flowie_control_user_view_t users[FLOWIE_CONTROL_PAGE_MAX];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_PAGE_MAX; ++index)
    users[index] = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
  rc = flowie_control_management_user_list(service, caller, NULL, users, FLOWIE_CONTROL_PAGE_MAX,
                                           &count, &has_more);
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_string(item, "principal_id", users[index].principal_id);
    if (rc == TURBO_OK)
      rc =
          flowie_control_dashboard_json_string(item, "principal_type", users[index].principal_type);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", users[index].enabled);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "user_options", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "user_options_truncated", has_more);
  return rc;
}

static int
flowie_control_dashboard_add_role_options(json_value_t *model,
                                          flowie_control_management_service_t *service,
                                          const flowie_control_management_caller_t *caller) {
  flowie_control_role_view_t roles[FLOWIE_CONTROL_DASHBOARD_ROLE_SELECTOR_LIMIT];
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc;
  if (!array) return TURBO_ENOMEM;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_ROLE_SELECTOR_LIMIT; ++index)
    roles[index] = (flowie_control_role_view_t)FLOWIE_CONTROL_ROLE_VIEW_INIT;
  rc = flowie_control_management_role_list(service, caller, NULL, roles,
                                           FLOWIE_CONTROL_DASHBOARD_ROLE_SELECTOR_LIMIT, &count,
                                           &has_more);
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_string(item, "role_id", roles[index].role_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", roles[index].enabled);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "role_options", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "role_options_truncated", has_more);
  return rc;
}

static int flowie_control_dashboard_add_users(json_value_t *model,
                                              flowie_control_management_service_t *service,
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
    rc = flowie_control_dashboard_json_u64(item, "row_index", index + 1u);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "principal_id", users[index].principal_id);
    if (rc == TURBO_OK)
      rc =
          flowie_control_dashboard_json_string(item, "principal_type", users[index].principal_type);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", users[index].enabled);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "users", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "users_pager", page, FLOWIE_CONTROL_DASHBOARD_USERS_CURSOR, count,
        has_more && count > 0u, count > 0u ? users[count - 1u].principal_id : NULL, 0u);
  return rc;
}

static int flowie_control_dashboard_add_roles(json_value_t *model,
                                              flowie_control_management_service_t *service,
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
    rc = flowie_control_dashboard_json_u64(item, "row_index", index + 1u);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "role_id", roles[index].role_id);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(item, "enabled", roles[index].enabled);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "roles", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "roles_pager", page, FLOWIE_CONTROL_DASHBOARD_ROLES_CURSOR, count,
        has_more && count > 0u, count > 0u ? roles[count - 1u].role_id : NULL, 0u);
  return rc;
}

static int flowie_control_dashboard_resource_label(
    const char *pattern, char label[FLOWIE_CONTROL_DASHBOARD_RESOURCE_LABEL_MAX]) {
  size_t offset = 0u;
  size_t pattern_size;
  if (!pattern || !label) return TURBO_EINVAL;
  pattern_size = strnlen(pattern, TURBO_FLOW_SECURITY_PATTERN_MAX + 1u);
  if (pattern_size == 0u || pattern_size > TURBO_FLOW_SECURITY_PATTERN_MAX) return TURBO_EPROTO;
  for (size_t index = 0u; index < pattern_size; ++index) {
    if (pattern[index] == '/') {
      if (offset + 3u >= FLOWIE_CONTROL_DASHBOARD_RESOURCE_LABEL_MAX) return TURBO_ENOSPC;
      label[offset++] = ' ';
      label[offset++] = '/';
      label[offset++] = ' ';
    } else {
      if (offset + 1u >= FLOWIE_CONTROL_DASHBOARD_RESOURCE_LABEL_MAX) return TURBO_ENOSPC;
      label[offset++] = pattern[index];
    }
  }
  label[offset] = '\0';
  return TURBO_OK;
}

static int flowie_control_dashboard_add_rules(json_value_t *model,
                                              flowie_control_management_service_t *service,
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
    turbo_flow_security_rule_t parsed = TURBO_FLOW_SECURITY_RULE_INIT;
    char resource_label[FLOWIE_CONTROL_DASHBOARD_RESOURCE_LABEL_MAX];
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = turbo_flow_security_rule_parse_line(rules[index].rule_line,
                                             strlen(rules[index].rule_line), &parsed);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_resource_label(parsed.pattern, resource_label);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_u64(item, "row_index", index + 1u);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_u64(item, "ordinal", rules[index].ordinal);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "rule_line", rules[index].rule_line);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "resource_label", resource_label);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_bool(
          item, "is_mqtt_topic",
          parsed.resource_type == TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "rules", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "policy_pager", page, FLOWIE_CONTROL_DASHBOARD_POLICY_CURSOR, count,
        has_more && count > 0u, NULL, count > 0u ? rules[count - 1u].ordinal : 0u);
  return rc;
}

static int flowie_control_dashboard_add_audits(json_value_t *model,
                                               flowie_control_management_service_t *service,
                                               const flowie_control_management_caller_t *caller,
                                               const flowie_control_dashboard_page_t *page) {
  flowie_control_audit_view_t *audits = NULL;
  json_value_t *array = turbo_json_create_array();
  size_t count = 0u;
  int has_more = 0;
  int rc = TURBO_OK;
  if (!array) return TURBO_ENOMEM;
  audits =
      (flowie_control_audit_view_t *)calloc(FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, sizeof(*audits));
  if (!audits) {
    flowie_control_dashboard_json_free(array);
    return TURBO_ENOMEM;
  }
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE; ++index)
    audits[index] = (flowie_control_audit_view_t)FLOWIE_CONTROL_AUDIT_VIEW_INIT;
  rc = flowie_control_management_audit_list(service, caller, page->audit_after, audits,
                                            FLOWIE_CONTROL_DASHBOARD_PAGE_SIZE, &count, &has_more);
  for (size_t index = 0u; rc == TURBO_OK && index < count; ++index) {
    json_value_t *item = turbo_json_create_object();
    if (!item) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_control_dashboard_json_u64(item, "cursor", audits[index].revision);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "actor", audits[index].actor);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "operation", audits[index].operation);
    if (rc == TURBO_OK)
      rc = flowie_control_dashboard_json_string(item, "target_id", audits[index].target_id);
    if (rc == TURBO_OK) rc = flowie_control_dashboard_json_array_take(array, item);
    else flowie_control_dashboard_json_free(item);
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_take(model, "audits", array);
  else flowie_control_dashboard_json_free(array);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_add_pager(
        model, "audit_pager", page, FLOWIE_CONTROL_DASHBOARD_AUDIT_CURSOR, count,
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
  rc = flowie_control_dashboard_compile(resource_directory, FLOWIE_CONTROL_DASHBOARD_SHELL_TEMPLATE,
                                        &view->shell_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_compile(
        resource_directory, FLOWIE_CONTROL_DASHBOARD_CONTENT_TEMPLATE, &view->content_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_compile(
        resource_directory, FLOWIE_CONTROL_DASHBOARD_ERROR_TEMPLATE, &view->error_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_compile(
        resource_directory, FLOWIE_CONTROL_DASHBOARD_LOGIN_TEMPLATE, &view->login_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_compile(
        resource_directory, FLOWIE_CONTROL_DASHBOARD_PASSWORD_TEMPLATE, &view->password_template);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_read(resource_directory, FLOWIE_CONTROL_DASHBOARD_CSS_ASSET,
                                       FLOWIE_CONTROL_DASHBOARD_ASSET_MAX, &view->css);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_read(resource_directory, FLOWIE_CONTROL_DASHBOARD_JS_ASSET,
                                       FLOWIE_CONTROL_DASHBOARD_ASSET_MAX, &view->javascript);
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
  mustache_release(view->password_template);
  mustache_release(view->login_template);
  mustache_release(view->content_template);
  mustache_release(view->shell_template);
  turbo_fs_buf_free(&view->htmx);
  turbo_fs_buf_free(&view->javascript);
  turbo_fs_buf_free(&view->css);
  memset(view, 0, sizeof(*view));
  free(view);
}

int flowie_control_dashboard_view_render_shell(flowie_control_dashboard_view_t *view,
                                               const flowie_control_dashboard_page_t *page,
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
    rc = flowie_control_dashboard_json_string(
        model, "page_title", flowie_control_dashboard_section_title(page->section));
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "content_url", content_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->shell_template, model, html_out,
                                                  html_size_out);
  tstr_free(content_url);
  flowie_control_dashboard_json_free(model);
  return rc;
}

int flowie_control_dashboard_view_render_login(flowie_control_dashboard_view_t *view,
                                               int group_mode, int show_error, char **html_out,
                                               size_t *html_size_out) {
  json_value_t *model;
  int rc;
  if (!view || (group_mode != 0 && group_mode != 1) || (show_error != 0 && show_error != 1))
    return TURBO_EINVAL;
  model = turbo_json_create_object();
  if (!model) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_json_bool(model, "system_mode", !group_mode);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "group_mode", group_mode);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "error", show_error);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->login_template, model, html_out,
                                                  html_size_out);
  flowie_control_dashboard_json_free(model);
  return rc;
}

int flowie_control_dashboard_view_render_password(
    flowie_control_dashboard_view_t *view,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], char **html_out,
    size_t *html_size_out) {
  json_value_t *model;
  int rc;
  if (!view || !csrf_token) return TURBO_EINVAL;
  model = turbo_json_create_object();
  if (!model) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_json_string(model, "csrf", csrf_token);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->password_template, model, html_out,
                                                  html_size_out);
  flowie_control_dashboard_json_free(model);
  return rc;
}

int flowie_control_dashboard_view_render_content(
    flowie_control_dashboard_view_t *view, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *authority_caller,
    const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page, char **html_out, size_t *html_size_out) {
  flowie_control_management_status_t status = FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT;
  json_value_t *model = NULL;
  char *action_url = NULL;
  char *overview_url = NULL;
  char *users_url = NULL;
  char *groups_url = NULL;
  char *roles_url = NULL;
  char *acls_url = NULL;
  char *audit_url = NULL;
  int can_select_root;
  int can_user_admin;
  int can_security_admin;
  int can_policy_admin;
  int can_audit_read;
  int show_overview;
  int show_users;
  int show_groups;
  int show_roles;
  int show_acls;
  int show_audit;
  int rc;
  if (!view || !service || !authority_caller || !authority_caller->domain_id || !caller ||
      !caller->domain_id || !csrf_token || !page)
    return TURBO_EINVAL;
  can_select_root =
      strcmp(authority_caller->domain_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN) == 0 &&
      (authority_caller->permissions & FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN) != 0u;
  can_user_admin = (caller->permissions & (FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN |
                                           FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN |
                                           FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN)) != 0u;
  can_security_admin = (caller->permissions & (FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN |
                                               FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN)) != 0u;
  can_policy_admin = (caller->permissions & (FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN |
                                             FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN |
                                             FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN)) != 0u;
  can_audit_read = can_security_admin;
  if (page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT && !can_audit_read)
    return TURBO_EPERM;
  show_overview = page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ALL ||
                  page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW;
  show_users = page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ALL ||
               page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_USERS;
  show_groups = page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ALL ||
                page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS;
  show_roles = page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ALL ||
               page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES;
  show_acls = page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ALL ||
              page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS;
  show_audit = can_audit_read && (page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ALL ||
                                  page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT);
  rc = flowie_control_management_system_status(service, caller, &status);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_dashboard_url(FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, page, &action_url);
  if (rc == TURBO_OK)
    rc =
        flowie_control_dashboard_navigation_url(FLOWIE_CONTROL_DASHBOARD_PATH, page, &overview_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_navigation_url(FLOWIE_CONTROL_DASHBOARD_USERS_PATH, page,
                                                 &users_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_navigation_url(FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH, page,
                                                 &groups_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_navigation_url(FLOWIE_CONTROL_DASHBOARD_ROLES_PATH, page,
                                                 &roles_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_navigation_url(FLOWIE_CONTROL_DASHBOARD_ACLS_PATH, page,
                                                 &acls_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_navigation_url(FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH, page,
                                                 &audit_url);
  if (rc != TURBO_OK) goto done;
  model = turbo_json_create_object();
  if (!model) rc = TURBO_ENOMEM;
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_string(model, "domain_id", caller->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "actor", caller->actor);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "csrf", csrf_token);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "action_url", action_url);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "policy_version", status.policy.policy_version);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "draft_rule_count",
                                           status.policy.draft_rule_count);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_u64(model, "published_rule_count",
                                           status.policy.published_rule_count);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "can_user_admin", can_user_admin);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "can_security_admin", can_security_admin);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "can_policy_admin", can_policy_admin);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "can_manage_access",
                                            can_user_admin || can_security_admin);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "can_audit_read", can_audit_read);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "can_select_root", can_select_root);
  if (rc == TURBO_OK && can_select_root)
    rc = flowie_control_dashboard_add_domains(model, service, authority_caller, caller);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "show_overview", show_overview);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "show_users", show_users);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "show_groups", show_groups);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "show_roles", show_roles);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "show_acls", show_acls);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_bool(model, "show_audit", show_audit);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "is_overview", page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "is_users", page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_USERS);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "is_groups", page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "is_roles", page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(model, "is_acls",
                                            page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_bool(
        model, "is_audit", page->section == FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_json_string(model, "overview_path", overview_url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "users_path", users_url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "groups_path", groups_url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "roles_path", roles_url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "acls_path", acls_url);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_json_string(model, "audit_path", audit_url);
  if (rc == TURBO_OK && (show_groups || show_acls || (show_users && can_user_admin)))
    rc = flowie_control_dashboard_add_group_options(model, service, caller);
  if (rc == TURBO_OK &&
      (show_acls || ((show_groups || show_roles) && (can_user_admin || can_security_admin))))
    rc = flowie_control_dashboard_add_user_options(model, service, caller);
  if (rc == TURBO_OK && ((show_users && can_security_admin) || show_acls))
    rc = flowie_control_dashboard_add_role_options(model, service, caller);
  if (rc == TURBO_OK && show_users)
    rc = flowie_control_dashboard_add_users(model, service, caller, page);
  if (rc == TURBO_OK && show_roles)
    rc = flowie_control_dashboard_add_roles(model, service, caller, page);
  if (rc == TURBO_OK && show_acls)
    rc = flowie_control_dashboard_add_rules(model, service, caller, page);
  if (rc == TURBO_OK && show_audit)
    rc = flowie_control_dashboard_add_audits(model, service, caller, page);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_template(view->content_template, model, html_out,
                                                  html_size_out);
done:
  tstr_free(action_url);
  tstr_free(overview_url);
  tstr_free(users_url);
  tstr_free(groups_url);
  tstr_free(roles_url);
  tstr_free(acls_url);
  tstr_free(audit_url);
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
  case FLOWIE_CONTROL_DASHBOARD_ASSET_JS:
    resource = &view->javascript;
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
