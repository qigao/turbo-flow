#include "flowie_control_dashboard_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char DASHBOARD_CSRF[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";

static int dashboard_resolve(void *ctx, const Req *request,
                             flowie_control_management_caller_t *caller_out,
                             char csrf_token_out[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]) {
  (void)request;
  *caller_out = *(flowie_control_management_caller_t *)ctx;
  memcpy(csrf_token_out, DASHBOARD_CSRF, sizeof(DASHBOARD_CSRF));
  return TURBO_OK;
}

static uint64_t dashboard_clock(void *ctx) {
  (void)ctx;
  return 5000u;
}

static flowie_control_dashboard_t *dashboard_open(char **path_out,
                                                  flowie_control_store_t **store_out,
                                                  flowie_control_management_service_t **service_out,
                                                  flowie_control_management_caller_t *caller) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t root_result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_management_service_config_t service_config =
      FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_dashboard_config_t dashboard_config = FLOWIE_CONTROL_DASHBOARD_CONFIG_INIT;
  flowie_control_dashboard_t *dashboard = NULL;

  *path_out = tt_make_temp_file("flowie-dashboard", ".sqlite3");
  check_not_null(*path_out);
  store_config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&store_config, store_out), TURBO_OK);
  root.root_group_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "request-root";
  root.occurred_at = 1000u;
  check_int_eq(flowie_control_store_root_group_create(*store_out, &root, &root_result), TURBO_OK);
  service_config.store = *store_out;
  check_int_eq(flowie_control_management_service_create(&service_config, service_out), TURBO_OK);
  dashboard_config.service = *service_out;
  dashboard_config.resolve_session = dashboard_resolve;
  dashboard_config.resolve_session_ctx = caller;
  dashboard_config.clock = dashboard_clock;
  check_int_eq(flowie_control_dashboard_create(&dashboard_config, &dashboard), TURBO_OK);
  return dashboard;
}

static void dashboard_close(flowie_control_dashboard_t *dashboard,
                            flowie_control_management_service_t *service,
                            flowie_control_store_t *store, char *path) {
  flowie_control_dashboard_destroy(dashboard);
  flowie_control_management_service_destroy(service);
  flowie_control_store_destroy(store);
  check_int_eq(tt_remove_file(path), 0);
  free(path);
}

spec("Flowie ACL dashboard") {
  it("parses only canonical independent keyset cursors") {
    request_item_t items[5] = {{"users_after", "device%3C1%3E"},
                               {"groups_after", "group-1"},
                               {"roles_after", "role-1"},
                               {"policy_after", "0"},
                               {"audit_after", "42"}};
    Req request;
    flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;

    memset(&request, 0, sizeof(request));
    request.query.items = items;
    request.query.count = 5;
    request.query.capacity = 5;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_OK);
    check_str_eq(page.users_after, "device<1>");
    check_str_eq(page.groups_after, "group-1");
    check_str_eq(page.roles_after, "role-1");
    check_true(page.policy_has_after);
    check_uint_eq(page.policy_after, 0u);
    check_true(page.audit_has_after);
    check_uint_eq(page.audit_after, 42u);
  }

  it("rejects duplicate unknown noncanonical and out-of-range cursors") {
    request_item_t items[2] = {{"users_after", "device-1"}, {"users_after", "device-2"}};
    Req request;
    flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;

    memset(&request, 0, sizeof(request));
    request.query.items = items;
    request.query.count = 2;
    request.query.capacity = 2;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_EPROTO);
    items[0].key = "unknown";
    request.query.count = 1;
    page = (flowie_control_dashboard_page_t)FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_EPROTO);
    items[0].key = "users_after";
    items[0].value = "device%3c1%3e";
    page = (flowie_control_dashboard_page_t)FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_EPROTO);
    items[0].key = "policy_after";
    items[0].value = "999999999999";
    page = (flowie_control_dashboard_page_t)FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_EPROTO);
  }

  it("requires the exact HTMX request header") {
    request_item_t header = {"HX-Request", "true"};
    Req request;

    memset(&request, 0, sizeof(request));
    check_false(flowie_control_dashboard_request_is_htmx(&request));
    request.headers.items = &header;
    request.headers.count = 1;
    request.headers.capacity = 1;
    check_true(flowie_control_dashboard_request_is_htmx(&request));
    header.value = "True";
    check_false(flowie_control_dashboard_request_is_htmx(&request));
    header.key = "HX-Boosted";
    header.value = "true";
    check_false(flowie_control_dashboard_request_is_htmx(&request));
  }

  it("renders escaped root-scoped state and unbinds every Iris path") {
    char *path = NULL;
    char *html = NULL;
    size_t html_size = 0u;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    iris_app_t *app = iris_app_create();

    check_not_null(app);
    caller.root_group_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    user.root_group_id = caller.root_group_id;
    user.principal_id = "device<script>";
    user.principal_type = "device";
    user.actor = caller.actor;
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 2000u;
    check_int_eq(flowie_control_management_user_create(service, &caller, &user, &result), TURBO_OK);

    check_int_eq(
        flowie_control_dashboard_render(dashboard, &caller, DASHBOARD_CSRF, &html, &html_size),
        TURBO_OK);
    check_not_null(html);
    check_size_gt(html_size, 0u);
    check_str_contains(html, "device&lt;script&gt;");
    check_false(strstr(html, "device<script>") != NULL);
    check_str_contains(html, "operation\" value=\"user.disable");
    check_str_contains(html, "operation\" value=\"group.member.add");
    check_str_contains(html, "operation\" value=\"role.assign");
    check_str_contains(html, "operation\" value=\"policy.rule.delete");
    check_str_contains(html, "hx-post=\"/v1/management/dashboard/action\"");
    check_str_contains(html, "hx-include=\"closest .command\"");
    check_str_contains(html, "hx-target=\"#dashboard\"");
    check_false(strstr(html, "<form") != NULL);
    check_false(strstr(html, "type=\"submit\"") != NULL);
    check_false(strstr(html, " method=\"") != NULL);
    check_false(strstr(html, " action=\"") != NULL);
    flowie_control_dashboard_html_free(html);
    html = NULL;
    html_size = 0u;

    {
      const flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
      check_int_eq(flowie_control_dashboard_render_shell(dashboard, &page, &html, &html_size),
                   TURBO_OK);
    }
    check_str_contains(html, "src=\"/v1/management/assets/htmx-2.0.9.min.js\"");
    check_str_contains(html, "hx-get=\"/v1/management/dashboard/content\"");
    check_str_contains(html, "hx-trigger=\"load\"");
    check_str_contains(html, "\"allowEval\":false");
    check_str_contains(html, "\"historyEnabled\":false");
    check_false(strstr(html, "https://") != NULL);
    flowie_control_dashboard_html_free(html);

    check_int_eq(flowie_control_dashboard_bind(dashboard, app), TURBO_OK);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH),
                 dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ACTION_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CSS_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_HTMX_PATH), dashboard);
    flowie_control_dashboard_unbind(dashboard);
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ACTION_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CSS_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_HTMX_PATH));

    iris_app_destroy(app);
    dashboard_close(dashboard, service, store, path);
  }

  it("renders forward pages while preserving the other list cursors") {
    char *path = NULL;
    char *html = NULL;
    size_t html_size = 0u;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;

    caller.root_group_id = "root-a";
    caller.actor = "user-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    for (uint64_t index = 0u; index < 26u; ++index) {
      char principal_id[32];
      char request_id[32];
      flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
      flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
      (void)snprintf(principal_id, sizeof(principal_id), "device-%03llu",
                     (unsigned long long)index);
      (void)snprintf(request_id, sizeof(request_id), "request-%03llu", (unsigned long long)index);
      command.root_group_id = caller.root_group_id;
      command.principal_id = principal_id;
      command.principal_type = "device";
      command.actor = caller.actor;
      command.request_id = request_id;
      command.expected_revision = index + 1u;
      command.occurred_at = 2000u + index;
      check_int_eq(flowie_control_management_user_create(service, &caller, &command, &result),
                   TURBO_OK);
    }

    (void)snprintf(page.groups_after, sizeof(page.groups_after), "%s", "root-a");
    check_int_eq(flowie_control_dashboard_render_page(dashboard, &caller, DASHBOARD_CSRF, &page,
                                                      &html, &html_size),
                 TURBO_OK);
    check_str_contains(html, "device-000");
    check_false(strstr(html, "device-025") != NULL);
    check_str_contains(
        html,
        "hx-get=\"/v1/management/dashboard/content?users_after=device-024&amp;groups_after=root-a\"");
    check_str_contains(
        html,
        "hx-post=\"/v1/management/dashboard/action?groups_after=root-a\"");
    flowie_control_dashboard_html_free(html);

    (void)snprintf(page.users_after, sizeof(page.users_after), "%s", "device-024");
    html = NULL;
    html_size = 0u;
    check_int_eq(flowie_control_dashboard_render_page(dashboard, &caller, DASHBOARD_CSRF, &page,
                                                      &html, &html_size),
                 TURBO_OK);
    check_str_contains(html, "device-025");
    check_false(strstr(html, "device-000") != NULL);
    check_str_contains(html,
                       "hx-get=\"/v1/management/dashboard/content?groups_after=root-a\"");
    flowie_control_dashboard_html_free(html);

    dashboard_close(dashboard, service, store, path);
  }

  it("rejects CSRF, unknown fields, and viewer writes without changing revision") {
    char *path = NULL;
    char body[1024];
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    uint64_t revision = 0u;

    caller.root_group_id = "root-a";
    caller.actor = "viewer";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user&expected_revision=1",
                   "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_EPERM);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user&expected_revision=1&root_group=root-b",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_EPROTO);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user&expected_revision=1",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_EPERM);
    check_int_eq(flowie_control_store_revision(store, &revision), TURBO_OK);
    check_uint_eq(revision, 1u);

    dashboard_close(dashboard, service, store, path);
  }

  it("submits an authorized form through the shared management service") {
    char *path = NULL;
    char body[1024];
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;

    caller.root_group_id = "root-a";
    caller.actor = "user-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user&expected_revision=1",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    check_int_eq(flowie_control_management_user_get(service, &caller, "device-1", &user), TURBO_OK);
    check_str_eq(user.principal_id, "device-1");
    check_uint_eq(user.revision, 2u);

    dashboard_close(dashboard, service, store, path);
  }

  it("exposes disable membership assignment and rule deletion through domain commands") {
    char *path = NULL;
    char body[1024];
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
    flowie_control_policy_rule_view_t rule = FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
    uint64_t revision = 0u;
    size_t count = 0u;
    int has_more = 0;

    caller.root_group_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);

    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user-create&expected_revision=1",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.create&group_id=operators&parent_group_id=root-a&"
                   "request_id=request-group-create&expected_revision=2",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.member.add&principal_id=device-1&group_id=operators&"
                   "request_id=request-member-add&expected_revision=3",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.member.remove&principal_id=device-1&group_id=operators&"
                   "request_id=request-member-remove&expected_revision=4",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.disable&group_id=operators&"
                   "request_id=request-group-disable&expected_revision=5",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.create&role_id=publisher&"
                   "request_id=request-role-create&expected_revision=6",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.assign&principal_id=device-1&role_id=publisher&"
                   "request_id=request-role-assign&expected_revision=7",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.remove&principal_id=device-1&role_id=publisher&"
                   "request_id=request-role-remove&expected_revision=8",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.disable&role_id=publisher&"
                   "request_id=request-role-disable&expected_revision=9",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=policy.rule.put&ordinal=10&"
                   "rule_line=allow|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/events/%%23&"
                   "request_id=request-rule-put&expected_revision=10",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=policy.rule.delete&ordinal=10&"
                   "request_id=request-rule-delete&expected_revision=11",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.disable&principal_id=device-1&"
                   "request_id=request-user-disable&expected_revision=12",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);

    check_int_eq(flowie_control_management_user_get(service, &caller, "device-1", &user), TURBO_OK);
    check_false(user.enabled);
    check_uint_eq(user.revision, 13u);
    check_int_eq(flowie_control_management_policy_rule_list(service, &caller, 0u, 0, &rule, 1u,
                                                            &count, &has_more),
                 TURBO_OK);
    check_size_eq(count, 0u);
    check_false(has_more);
    check_int_eq(flowie_control_store_revision(store, &revision), TURBO_OK);
    check_uint_eq(revision, 13u);

    dashboard_close(dashboard, service, store, path);
  }
}
