#include "flowie_control_dashboard_internal.h"
#include "flowie_control_http_request_internal.h"

#include "CoroNet.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
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

static int dashboard_login(void *ctx, const char *domain_id, const char *principal_id,
                           const uint8_t *secret, size_t secret_size, const char *remote_address,
                           char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  (void)ctx;
  (void)domain_id;
  (void)principal_id;
  (void)secret;
  (void)secret_size;
  (void)remote_address;
  if (token_out) token_out[0] = '\0';
  return TURBO_EPERM;
}

static int dashboard_logout(void *ctx, const char *token) {
  (void)ctx;
  (void)token;
  return TURBO_OK;
}

typedef struct dashboard_open_options_s {
  flowie_control_dashboard_login_fn login;
  flowie_control_dashboard_logout_fn logout;
  void *session_ctx;
  int executor_enabled;
  uint32_t executor_workers;
  size_t executor_queue_capacity;
  uint32_t executor_deadline_ms;
} dashboard_open_options_t;

typedef struct dashboard_executor_fixture_s {
  atomic_int release_login;
  atomic_int login_count;
  atomic_int logout_count;
} dashboard_executor_fixture_t;

typedef struct dashboard_executor_task_s {
  flowie_control_dashboard_t *dashboard;
  dashboard_executor_fixture_t *fixture;
  char token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u];
  int result;
} dashboard_executor_task_t;

static int
dashboard_executor_login(void *ctx, const char *domain_id, const char *principal_id,
                         const uint8_t *secret, size_t secret_size, const char *remote_address,
                         char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  dashboard_executor_fixture_t *fixture = (dashboard_executor_fixture_t *)ctx;
  if (!fixture || !domain_id || strcmp(domain_id, "root-a") != 0 || !principal_id ||
      strcmp(principal_id, "security-admin") != 0 || !secret || secret_size != 8u ||
      memcmp(secret, "password", 8u) != 0 || !remote_address ||
      strcmp(remote_address, "management-dashboard") != 0 || !token_out)
    return TURBO_EINVAL;
  atomic_fetch_add_explicit(&fixture->login_count, 1, memory_order_relaxed);
  while (!atomic_load_explicit(&fixture->release_login, memory_order_acquire))
    turbo_thread_yield();
  memset(token_out, 'a', FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE);
  token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE] = '\0';
  return TURBO_OK;
}

static int dashboard_executor_logout(void *ctx, const char *token) {
  dashboard_executor_fixture_t *fixture = (dashboard_executor_fixture_t *)ctx;
  if (!fixture || !token ||
      strnlen(token, FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u) !=
          FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE)
    return TURBO_EINVAL;
  atomic_fetch_add_explicit(&fixture->logout_count, 1, memory_order_relaxed);
  return TURBO_OK;
}

static void dashboard_executor_task_run(coro_t *co, void *arg) {
  static const uint8_t secret[] = "password";
  dashboard_executor_task_t *task = (dashboard_executor_task_t *)arg;
  (void)co;
  task->result = flowie_control_dashboard_execute_login(task->dashboard, "root-a", "security-admin",
                                                        secret, sizeof(secret) - 1u,
                                                        "management-dashboard", task->token);
  if (task->result == TURBO_EBUSY || task->result == TURBO_ETIMEDOUT)
    atomic_store_explicit(&task->fixture->release_login, 1, memory_order_release);
}

static flowie_control_dashboard_t *
dashboard_open_with_options(char **path_out, flowie_control_store_t **store_out,
                            flowie_control_management_service_t **service_out,
                            flowie_control_management_caller_t *caller,
                            const dashboard_open_options_t *options) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_command_result_t root_result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_management_service_config_t service_config =
      FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_dashboard_config_t dashboard_config = FLOWIE_CONTROL_DASHBOARD_CONFIG_INIT;
  flowie_control_dashboard_t *dashboard = NULL;

  *path_out = tt_make_temp_file("flowie-dashboard", ".sqlite3");
  check_not_null(*path_out);
  store_config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&store_config, store_out), TURBO_OK);
  root.domain_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "request-root";
  root.occurred_at = 1000u;
  check_int_eq(flowie_control_store_domain_create(*store_out, &root, &root_result), TURBO_OK);
  service_config.repository = flowie_control_store_repository(*store_out);
  check_int_eq(flowie_control_management_service_create(&service_config, service_out), TURBO_OK);
  dashboard_config.service = *service_out;
  dashboard_config.resolve_session = dashboard_resolve;
  dashboard_config.resolve_session_ctx = caller;
  dashboard_config.clock = dashboard_clock;
  dashboard_config.login = options->login;
  dashboard_config.logout = options->logout;
  dashboard_config.session_ctx = options->session_ctx;
  dashboard_config.session_ttl_seconds = 3600u;
  dashboard_config.login_executor_enabled = options->executor_enabled;
  dashboard_config.login_executor_workers = options->executor_workers;
  dashboard_config.login_executor_queue_capacity = options->executor_queue_capacity;
  dashboard_config.login_executor_deadline_ms = options->executor_deadline_ms;
  check_int_eq(flowie_control_dashboard_create(&dashboard_config, &dashboard), TURBO_OK);
  return dashboard;
}

static flowie_control_dashboard_t *dashboard_open(char **path_out,
                                                  flowie_control_store_t **store_out,
                                                  flowie_control_management_service_t **service_out,
                                                  flowie_control_management_caller_t *caller) {
  dashboard_open_options_t options = {
      dashboard_login,
      dashboard_logout,
      caller,
      0,
      FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_WORKERS,
      FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_QUEUE_CAPACITY,
      FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_DEADLINE_MS};
  return dashboard_open_with_options(path_out, store_out, service_out, caller, &options);
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
  it("rejects excess login work without blocking the CoroNet owner lane") {
    enum { TASK_COUNT = 6 };
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    dashboard_executor_fixture_t fixture;
    dashboard_executor_task_t tasks[TASK_COUNT];
    dashboard_open_options_t options;
    flowie_control_dashboard_t *dashboard;
    coro_context_t *context = coro_context_create(NULL);
    int succeeded = 0;
    int overloaded = 0;

    check_not_null(context);
    atomic_init(&fixture.release_login, 0);
    atomic_init(&fixture.login_count, 0);
    atomic_init(&fixture.logout_count, 0);
    memset(tasks, 0, sizeof(tasks));
    caller.domain_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    options = (dashboard_open_options_t){
        dashboard_executor_login, dashboard_executor_logout, &fixture, 1, 1u, 1u, 10000u};
    dashboard = dashboard_open_with_options(&path, &store, &service, &caller, &options);
    for (size_t index = 0u; index < TASK_COUNT; ++index) {
      tasks[index].dashboard = dashboard;
      tasks[index].fixture = &fixture;
      tasks[index].result = TURBO_EALREADY;
      check_int_eq(coro_context_spawn(context, dashboard_executor_task_run, &tasks[index]),
                   TURBO_OK);
    }
    check_int_eq(coro_context_run(context, TURBO_RUN_DEFAULT), TURBO_OK);
    for (size_t index = 0u; index < TASK_COUNT; ++index) {
      if (tasks[index].result == TURBO_OK) {
        ++succeeded;
        check_size_eq(strlen(tasks[index].token), FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE);
      } else if (tasks[index].result == TURBO_EBUSY) {
        ++overloaded;
        check_str_eq(tasks[index].token, "");
      } else {
        check_int_eq(tasks[index].result, TURBO_OK);
      }
    }
    check_int_gt(succeeded, 0);
    check_int_gt(overloaded, 0);
    check_int_eq(atomic_load_explicit(&fixture.login_count, memory_order_relaxed), succeeded);
    check_int_eq(atomic_load_explicit(&fixture.logout_count, memory_order_relaxed), 0);

    dashboard_close(dashboard, service, store, path);
    coro_context_destroy(context);
  }

  it("revokes a session issued after the login deadline") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    dashboard_executor_fixture_t fixture;
    dashboard_executor_task_t task;
    dashboard_open_options_t options;
    flowie_control_dashboard_t *dashboard;
    coro_context_t *context = coro_context_create(NULL);

    check_not_null(context);
    atomic_init(&fixture.release_login, 0);
    atomic_init(&fixture.login_count, 0);
    atomic_init(&fixture.logout_count, 0);
    memset(&task, 0, sizeof(task));
    caller.domain_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    options = (dashboard_open_options_t){
        dashboard_executor_login, dashboard_executor_logout, &fixture, 1, 1u, 1u, 1u};
    dashboard = dashboard_open_with_options(&path, &store, &service, &caller, &options);
    task.dashboard = dashboard;
    task.fixture = &fixture;
    task.result = TURBO_EALREADY;
    check_int_eq(coro_context_spawn(context, dashboard_executor_task_run, &task), TURBO_OK);
    check_int_eq(coro_context_run(context, TURBO_RUN_DEFAULT), TURBO_OK);
    check_int_eq(task.result, TURBO_ETIMEDOUT);
    check_str_eq(task.token, "");

    dashboard_close(dashboard, service, store, path);
    check_int_eq(atomic_load_explicit(&fixture.login_count, memory_order_relaxed), 1);
    check_int_eq(atomic_load_explicit(&fixture.logout_count, memory_order_relaxed), 1);
    coro_context_destroy(context);
  }

  it("parses only canonical independent keyset cursors") {
    request_item_t items[6] = {{"domain_id", "root-a"}, {"users_after", "device%3C1%3E"},
                               {"groups_after", "group-1"}, {"roles_after", "role-1"},
                               {"policy_after", "0"},       {"audit_after", "42"}};
    Req request;
    flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;

    memset(&request, 0, sizeof(request));
    request.query.items = items;
    request.query.count = 6;
    request.query.capacity = 6;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_OK);
    check_str_eq(page.domain_id, "root-a");
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

  it("accepts only the cursor owned by the selected dashboard page") {
    request_item_t items[2] = {{"section", "users"}, {"users_after", "device-1"}};
    Req request;
    flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;

    memset(&request, 0, sizeof(request));
    request.query.items = items;
    request.query.count = 2;
    request.query.capacity = 2;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_OK);
    check_int_eq(page.section, FLOWIE_CONTROL_DASHBOARD_SECTION_USERS);
    check_str_eq(page.users_after, "device-1");

    items[1] = (request_item_t){"groups_after", "group-1"};
    page = (flowie_control_dashboard_page_t)FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_EPROTO);

    items[0].value = "unknown";
    request.query.count = 1;
    page = (flowie_control_dashboard_page_t)FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
    check_int_eq(flowie_control_dashboard_page_parse(&request, &page), TURBO_EPROTO);

    items[0] = (request_item_t){"section", "users"};
    items[1] = (request_item_t){"section", "groups"};
    request.query.count = 2;
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
    header.key = "hx-request";
    check_true(flowie_control_dashboard_request_is_htmx(&request));
    header.value = "True";
    check_false(flowie_control_dashboard_request_is_htmx(&request));
    header.key = "HX-Boosted";
    header.value = "true";
    check_false(flowie_control_dashboard_request_is_htmx(&request));
  }

  it("accepts an exact HTTPS Origin for dashboard mutations") {
    request_item_t headers[2] = {{"Host", "mqtt.dev.my-photo.xyz"},
                                 {"Origin", "https://mqtt.dev.my-photo.xyz"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 2;
    request.headers.capacity = 2;
    check_true(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("accepts same-origin Fetch Metadata when Origin is absent") {
    request_item_t headers[2] = {{"host", "mqtt.dev.my-photo.xyz"},
                                 {"sec-fetch-site", "same-origin"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 2;
    request.headers.capacity = 2;
    check_true(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("accepts same-origin Fetch Metadata with an opaque Origin") {
    request_item_t headers[3] = {
        {"Host", "mqtt.dev.my-photo.xyz"}, {"Origin", "null"}, {"Sec-Fetch-Site", "same-origin"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 3;
    request.headers.capacity = 3;
    check_true(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("rejects an opaque Origin without same-origin Fetch Metadata") {
    request_item_t headers[3] = {
        {"Host", "mqtt.dev.my-photo.xyz"}, {"Origin", "null"}, {"Sec-Fetch-Site", "cross-site"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 3;
    request.headers.capacity = 3;
    check_false(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("does not let Fetch Metadata override a mismatched Origin") {
    request_item_t headers[3] = {{"Host", "mqtt.dev.my-photo.xyz"},
                                 {"Origin", "https://attacker.example"},
                                 {"Sec-Fetch-Site", "same-origin"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 3;
    request.headers.capacity = 3;
    check_false(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("rejects cross-site Fetch Metadata when Origin is absent") {
    request_item_t headers[2] = {{"Host", "mqtt.dev.my-photo.xyz"},
                                 {"Sec-Fetch-Site", "cross-site"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 2;
    request.headers.capacity = 2;
    check_false(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("rejects dashboard mutations without Origin or Fetch Metadata") {
    request_item_t header = {"Host", "mqtt.dev.my-photo.xyz"};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = &header;
    request.headers.count = 1;
    request.headers.capacity = 1;
    check_false(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("rejects duplicate Origin and Fetch Metadata headers") {
    request_item_t headers[3] = {{"Host", "mqtt.dev.my-photo.xyz"},
                                 {"Origin", "https://mqtt.dev.my-photo.xyz"},
                                 {"origin", "https://mqtt.dev.my-photo.xyz"}};
    Req request;

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 3;
    request.headers.capacity = 3;
    check_false(flowie_control_dashboard_request_is_same_origin(&request));

    headers[1] = (request_item_t){"Sec-Fetch-Site", "same-origin"};
    headers[2] = (request_item_t){"sec-fetch-site", "same-origin"};
    check_false(flowie_control_dashboard_request_is_same_origin(&request));
  }

  it("resolves lowercase HTTP/2-style cookie headers without accepting duplicates") {
    request_item_t headers[2] = {{"cookie", "theme=dark; flowie_management=token-value"},
                                 {"Cookie", "flowie_management=second"}};
    Req request;
    char value[32];

    memset(&request, 0, sizeof(request));
    request.headers.items = headers;
    request.headers.count = 1;
    request.headers.capacity = 2;
    check_int_eq(
        flowie_control_http_cookie_exact(&request, "flowie_management", value, sizeof(value)),
        TURBO_OK);
    check_str_eq(value, "token-value");
    request.headers.count = 2;
    check_int_eq(
        flowie_control_http_cookie_exact(&request, "flowie_management", value, sizeof(value)),
        TURBO_EPROTO);
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
    flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    flowie_control_policy_rule_put_command_t rule = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    iris_app_t *app = iris_app_create();

    check_not_null(app);
    caller.domain_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    user.domain_id = caller.domain_id;
    user.principal_id = "device<script>";
    user.principal_type = "device";
    user.actor = caller.actor;
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 2000u;
    check_int_eq(flowie_control_management_user_create(service, &caller, &user, &result), TURBO_OK);
    user.principal_id = "device-1";
    user.request_id = "request-user-acl";
    user.expected_revision = 2u;
    user.occurred_at = 2001u;
    check_int_eq(flowie_control_management_user_create(service, &caller, &user, &result), TURBO_OK);
    group.domain_id = caller.domain_id;
    group.group_id = "operators";
    group.parent_group_id = NULL;
    group.actor = caller.actor;
    group.request_id = "request-group";
    group.expected_revision = 3u;
    group.occurred_at = 2002u;
    check_int_eq(flowie_control_management_group_create(service, &caller, &group, &result),
                 TURBO_OK);
    group.group_id = "operators-east";
    group.parent_group_id = "operators";
    group.request_id = "request-group-child";
    group.expected_revision = 4u;
    group.occurred_at = 2003u;
    check_int_eq(flowie_control_management_group_create(service, &caller, &group, &result),
                 TURBO_OK);
    role.domain_id = caller.domain_id;
    role.role_id = "publisher";
    role.actor = caller.actor;
    role.request_id = "request-role";
    role.expected_revision = 5u;
    role.occurred_at = 2004u;
    check_int_eq(flowie_control_management_role_create(service, &caller, &role, &result), TURBO_OK);
    rule.domain_id = caller.domain_id;
    rule.ordinal = 10u;
    rule.rule_line =
        "user device-1 allow {\n"
        "  readwrite topic root-a/groups/operators/operators-east/devices/%u/events\n"
        "}";
    rule.actor = caller.actor;
    rule.request_id = "request-rule";
    rule.expected_revision = 6u;
    rule.occurred_at = 2005u;
    check_int_eq(flowie_control_management_policy_rule_put(service, &caller, &rule, &result),
                 TURBO_OK);

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
    check_str_contains(html, "hx-post=\"/v2/control/dashboard/action\"");
    check_str_contains(html, "hx-include=\"closest .command\"");
    check_str_contains(html, "hx-target=\"#dashboard\"");
    check_str_contains(html, "aria-label=\"Control sections\"");
    check_false(strstr(html, ">Manage</p>") != NULL);
    check_false(strstr(html, "href=\"#identity-management\"") != NULL);
    check_false(strstr(html, "href=\"#role-management\"") != NULL);
    check_false(strstr(html, "href=\"#acl-management\"") != NULL);
    check_str_contains(html, "href=\"/v2/control/dashboard/audit\"");
    check_str_contains(html, "id=\"users\"");
    check_str_contains(html, "id=\"groups\"");
    check_str_contains(html, "id=\"roles\"");
    check_str_contains(html, "id=\"acl-rules\"");
    check_str_contains(html, "aria-label=\"Users pagination\"");
    check_str_contains(html, "class=\"group-tree\" role=\"tree\"");
    check_str_contains(html, "class=\"group-tree__node group-tree__node--depth-0\" role=\"treeitem\"");
    check_str_contains(html, "class=\"group-tree__node group-tree__node--depth-1\" role=\"treeitem\"");
    check_str_contains(html, "aria-label=\"Roles pagination\"");
    check_str_contains(html, "aria-label=\"User ACL pagination\"");
    check_str_contains(html, "aria-label=\"Audit pagination\"");
    check_str_contains(html, "aria-label=\"Users tools\"");
    check_str_contains(html, "aria-label=\"Groups tools\"");
    check_str_contains(html, "aria-label=\"Roles tools\"");
    check_str_contains(html, "aria-label=\"User ACL tools\"");
    check_str_contains(html, "popovertarget=\"users-query\"");
    check_str_contains(html, "popovertarget=\"users-add\"");
    check_str_contains(html, "popovertarget=\"user-access-1\"");
    check_str_contains(html, "popovertarget=\"role-assignments-1\"");
    check_str_contains(html, "Manage user access");
    check_str_contains(html, "Manage role assignments");
    check_str_contains(html, "data-picker-options=\"group-member-picker-options\"");
    check_str_contains(html, "data-picker-options=\"group-parent-picker-options\"");
    check_str_contains(html, "data-picker-options=\"role-picker-options\"");
    check_str_contains(html, "role=\"option\" aria-selected=\"false\" tabindex=\"-1\"");
    check_str_contains(html, "data-value=\"operators\"");
    check_str_contains(html, ">operators</span>");
    check_str_contains(html, "data-value=\"operators-east\"");
    check_str_contains(html, ">-- operators-east</span>");
    check_str_contains(html, "data-value=\"publisher\"");
    check_str_contains(html, "name=\"parent_group_id\" data-picker-target");
    check_false(strstr(html, "<label>Parent group<input") != NULL);
    check_str_contains(html, "popovertarget=\"group-members-");
    check_str_contains(html, "Add a user to this group");
    check_str_contains(html, "popovertarget=\"group-members-1\"");
    check_str_contains(html, "popovertarget=\"group-members-2\"");
    check_false(strstr(html, "popovertarget=\"group-delete-1\"") != NULL);
    check_str_contains(html, "popovertarget=\"group-delete-2\"");
    check_str_contains(html, "data-entity-picker");
    check_str_contains(html, "data-picker-option");
    check_str_contains(html, "data-picker-target");
    check_str_contains(html, "data-request-id");
    check_false(strstr(html, "Request ID") != NULL);
    check_str_contains(html, "data-acl-builder-host");
    check_str_contains(html, "Topic permissions<textarea data-acl-entries");
    check_str_contains(html, "<span>Canonical document</span>");
    check_false(strstr(html, "data-acl-action") != NULL);
    check_str_contains(html, "<table class=\"acl-rule-table\"");
    check_str_contains(html, "<tr class=\"acl-rule-row\"><td class=\"acl-rule__ordinal\">10</td>");
    check_str_contains(html, "<td class=\"acl-rule__subject\"><code>device-1</code></td>");
    check_str_contains(html, "<td class=\"acl-rule__decision\">Allow</td>");
    check_str_contains(html, "1 statements · 1 rules");
    check_str_contains(html, "<code>%u</code> username");
    check_false(strstr(html, "class=\"resource-tree\"") != NULL);
    check_str_contains(html, ">View document</summary>");
    check_false(strstr(html, "user-edit-") != NULL);
    check_false(strstr(html, "group-edit-") != NULL);
    check_false(strstr(html, "role-edit-") != NULL);
    check_str_contains(html, "popovertarget=\"user-delete-1\"");
    check_str_contains(html, "popovertarget=\"acl-edit-1\"");
    check_str_contains(html, "Edit user ACL");
    check_str_contains(html, "popovertarget=\"acl-delete-1\"");
    check_str_contains(html, "class=\"actions-column\">Actions");
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
    check_str_contains(html, "src=\"/v2/control/assets/htmx-2.0.9.min.js\"");
    check_str_contains(html, "src=\"/v2/control/assets/control.js\"");
    check_str_contains(html, "class=\"skip-link\" href=\"#dashboard\"");
    check_str_contains(html, "hx-get=\"/v2/control/dashboard/content\"");
    check_str_contains(html, "hx-trigger=\"load\"");
    check_str_contains(html, "\"allowEval\":false");
    check_str_contains(html, "\"historyEnabled\":false");
    check_false(strstr(html, "https://") != NULL);
    flowie_control_dashboard_html_free(html);

    html = NULL;
    html_size = 0u;
    check_int_eq(flowie_control_dashboard_render_login(dashboard, 0, 0, &html, &html_size),
                 TURBO_OK);
    check_str_contains(html, "aria-current=\"page\">System administrator");
    check_str_contains(html, "name=\"domain\" value=\"system\"");
    check_false(strstr(html, "placeholder=\"root-a\"") != NULL);
    flowie_control_dashboard_html_free(html);

    html = NULL;
    html_size = 0u;
    check_int_eq(flowie_control_dashboard_render_login(dashboard, 1, 1, &html, &html_size),
                 TURBO_OK);
    check_str_contains(html, "aria-current=\"page\">Domain");
    check_str_contains(html, "name=\"domain\" required");
    check_str_contains(html, "role=\"alert\"");
    flowie_control_dashboard_html_free(html);

    html = NULL;
    html_size = 0u;
    check_int_eq(
        flowie_control_dashboard_render_password(dashboard, DASHBOARD_CSRF, &html, &html_size),
        TURBO_OK);
    check_str_contains(html, "name=\"csrf\" value=\"");
    check_str_contains(html, DASHBOARD_CSRF);
    check_str_contains(html, "minlength=\"16\"");
    flowie_control_dashboard_html_free(html);

    check_int_eq(flowie_control_dashboard_bind(dashboard, app), TURBO_OK);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_USERS_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROLES_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ACLS_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH),
                 dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ACTION_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CSS_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_JS_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_HTMX_PATH), dashboard);
    check_ptr_eq(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH),
                 dashboard);
    flowie_control_dashboard_unbind(dashboard);
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_USERS_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROLES_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ACLS_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ACTION_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_CSS_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_JS_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_HTMX_PATH));
    check_null(iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH));

    iris_app_destroy(app);
    dashboard_close(dashboard, service, store, path);
  }

  it("renders users groups roles ACL and audit as isolated pages") {
    static const flowie_control_dashboard_section_t sections[] = {
        FLOWIE_CONTROL_DASHBOARD_SECTION_USERS, FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS,
        FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES, FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS,
        FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT};
    static const char *const names[] = {"users", "groups", "roles", "acls", "audit"};
    static const char *const ids[] = {"users", "groups", "roles", "acl-rules", "audit"};
    static const char *const active_links[] = {
        "href=\"/v2/control/dashboard/users\" aria-current=\"page\"",
        "href=\"/v2/control/dashboard/groups\" aria-current=\"page\"",
        "href=\"/v2/control/dashboard/roles\" aria-current=\"page\"",
        "href=\"/v2/control/dashboard/acls\" aria-current=\"page\"",
        "href=\"/v2/control/dashboard/audit\" aria-current=\"page\""};
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;

    caller.domain_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    for (size_t index = 0u; index < sizeof(sections) / sizeof(sections[0]); ++index) {
      flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
      char expected_section[64];
      char expected_query[64];
      char *html = NULL;
      size_t html_size = 0u;

      page.section = sections[index];
      check_int_eq(flowie_control_dashboard_render_page(dashboard, &caller, DASHBOARD_CSRF, &page,
                                                        &html, &html_size),
                   TURBO_OK);
      (void)snprintf(expected_section, sizeof(expected_section), "<section id=\"%s\"", ids[index]);
      (void)snprintf(expected_query, sizeof(expected_query), "section=%s", names[index]);
      check_str_contains(html, expected_section);
      check_str_contains(html, expected_query);
      check_str_contains(html, active_links[index]);
      check_false(strstr(html, "<section id=\"overview\"") != NULL);
      if (index == 1u) {
        check_str_contains(html, "class=\"group-tree\" role=\"tree\"");
        check_false(strstr(html, "<table") != NULL);
      }
      for (size_t other = 0u; other < sizeof(ids) / sizeof(ids[0]); ++other) {
        if (other == index) continue;
        (void)snprintf(expected_section, sizeof(expected_section), "<section id=\"%s\"",
                       ids[other]);
        check_false(strstr(html, expected_section) != NULL);
      }
      flowie_control_dashboard_html_free(html);
    }
    {
      flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
      char *html = NULL;
      size_t html_size = 0u;
      page.section = FLOWIE_CONTROL_DASHBOARD_SECTION_USERS;
      check_int_eq(flowie_control_dashboard_render_shell(dashboard, &page, &html, &html_size),
                   TURBO_OK);
      check_str_contains(html, "<title>Users | Flowie Control</title>");
      check_str_contains(html, "hx-get=\"/v2/control/dashboard/content?section=users\"");
      flowie_control_dashboard_html_free(html);
    }
    dashboard_close(dashboard, service, store, path);
  }

  it("rejects the audit page when the caller cannot read audit records") {
    char *path = NULL;
    char *html = NULL;
    size_t html_size = 0u;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;

    caller.domain_id = "root-a";
    caller.actor = "user-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    page.section = FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT;
    check_int_eq(flowie_control_dashboard_render_page(dashboard, &caller, DASHBOARD_CSRF, &page,
                                                      &html, &html_size),
                 TURBO_EPERM);
    check_null(html);
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

    caller.domain_id = "root-a";
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
      command.domain_id = caller.domain_id;
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
    check_false(strstr(html, "<tr><td>device-025</td>") != NULL);
    check_str_contains(html, "25 shown");
    check_str_contains(html, "hx-get=\"/v2/control/dashboard/"
                             "content?users_after=device-024&amp;groups_after=root-a\"");
    check_str_contains(html, "hx-select=\"#users\"");
    check_str_contains(html, "hx-target=\"#users\"");
    check_str_contains(html, "hx-swap=\"outerHTML\">Next page");
    check_str_contains(html, "hx-get=\"/v2/control/dashboard/content?groups_after=root-a\" "
                             "hx-include=\"closest .query-panel\" hx-select=\"#users\"");
    check_str_contains(html, "hx-post=\"/v2/control/dashboard/action?groups_after=root-a\"");
    flowie_control_dashboard_html_free(html);

    (void)snprintf(page.users_after, sizeof(page.users_after), "%s", "device-024");
    html = NULL;
    html_size = 0u;
    check_int_eq(flowie_control_dashboard_render_page(dashboard, &caller, DASHBOARD_CSRF, &page,
                                                      &html, &html_size),
                 TURBO_OK);
    check_str_contains(html, "device-025");
    check_false(strstr(html, "<tr><td>device-000</td>") != NULL);
    check_str_contains(html, "1 shown");
    check_str_contains(html, "hx-get=\"/v2/control/dashboard/content?groups_after=root-a\"");
    check_str_contains(html, "hx-select=\"#users\"");
    check_str_contains(html, "hx-swap=\"outerHTML\">First page");
    flowie_control_dashboard_html_free(html);

    dashboard_close(dashboard, service, store, path);
  }

  it("renders HTMX CRUD controls for the system administrator") {
    char *path = NULL;
    char *html = NULL;
    size_t html_size = 0u;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_domain_create_command_t system_root =
        FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

    caller.domain_id = "root-a";
    caller.actor = "system-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    system_root.domain_id = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN;
    system_root.actor = "bootstrap";
    system_root.request_id = "request-system-root";
    system_root.expected_revision = 1u;
    system_root.occurred_at = 2000u;
    check_int_eq(flowie_control_store_domain_create(store, &system_root, &result), TURBO_OK);

    caller.domain_id = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN;
    check_int_eq(
        flowie_control_dashboard_render(dashboard, &caller, DASHBOARD_CSRF, &html, &html_size),
        TURBO_OK);
    check_false(strstr(html, ">Manage</p>") != NULL);
    check_false(strstr(html, "id=\"identity-management\"") != NULL);
    check_false(strstr(html, "id=\"role-management\"") != NULL);
    check_false(strstr(html, "id=\"acl-management\"") != NULL);
    check_str_contains(html, "href=\"/v2/control/dashboard/audit\"");
    check_str_contains(html, "popovertarget=\"users-add\"");
    check_str_contains(html, "popovertarget=\"groups-add\"");
    check_str_contains(html, "popovertarget=\"roles-add\"");
    check_str_contains(html, "popovertarget=\"acl-add\"");
    check_str_contains(html, "popovertarget=\"acl-publish\"");
    check_str_contains(html, "operation\" value=\"user.create");
    check_str_contains(html, "operation\" value=\"group.create");
    check_str_contains(html, "operation\" value=\"role.create");
    check_str_contains(html, "operation\" value=\"policy.rule.put");
    check_str_contains(html, "hx-post=\"/v2/control/dashboard/action\"");
    check_str_contains(html, "id=\"domain-scope\"");
    check_str_contains(html, "hx-trigger=\"change from:#domain-scope\"");
    check_false(strstr(html, ">Switch</button>") != NULL);
    flowie_control_dashboard_html_free(html);
    html = NULL;

    {
      flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
      memcpy(page.domain_id, "root-a", sizeof("root-a"));
      page.section = FLOWIE_CONTROL_DASHBOARD_SECTION_USERS;
      check_int_eq(flowie_control_dashboard_render_page(dashboard, &caller, DASHBOARD_CSRF, &page,
                                                        &html, &html_size),
                   TURBO_OK);
      check_str_contains(html, "Domain");
      check_str_contains(html, "value=\"root-a\"");
      check_str_contains(html, "href=\"/v2/control/dashboard/groups?domain_id=root-a\"");
      check_str_contains(html, "hx-post=\"/v2/control/dashboard/action?domain_id=root-a");
      flowie_control_dashboard_html_free(html);
    }

    dashboard_close(dashboard, service, store, path);
  }

  it("rejects CSRF, unknown fields, legacy revision fields, and viewer writes") {
    char *path = NULL;
    char body[1024];
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = NULL;
    flowie_control_dashboard_t *dashboard = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    uint64_t revision = 0u;

    caller.domain_id = "root-a";
    caller.actor = "viewer";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user",
                   "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_EPERM);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user&domain=root-b",
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
                 TURBO_EPROTO);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user",
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

    caller.domain_id = "root-a";
    caller.actor = "user-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    check_int_eq(flowie_control_management_user_get(service, &caller, "device-1", &user), TURBO_OK);
    check_str_eq(user.principal_id, "device-1");
    check_uint_eq(user.revision, 2u);

    dashboard_close(dashboard, service, store, path);
  }

  it("exposes group deletion membership assignment and rule deletion through domain commands") {
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

    caller.domain_id = "root-a";
    caller.actor = "security-admin";
    caller.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    dashboard = dashboard_open(&path, &store, &service, &caller);

    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.create&principal_id=device-1&principal_type=device&"
                   "request_id=request-user-create",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.create&group_id=operators&parent_group_id=root-a&"
                   "request_id=request-group-create",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.member.add&principal_id=device-1&group_id=operators&"
                   "request_id=request-member-add",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.member.remove&principal_id=device-1&group_id=operators&"
                   "request_id=request-member-remove",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=group.delete&group_id=operators&"
                   "request_id=request-group-delete",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.create&role_id=publisher&"
                   "request_id=request-role-create",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.assign&principal_id=device-1&role_id=publisher&"
                   "request_id=request-role-assign",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.remove&principal_id=device-1&role_id=publisher&"
                   "request_id=request-role-remove",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=role.disable&role_id=publisher&"
                   "request_id=request-role-disable",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                    "csrf=%s&operation=policy.rule.put&ordinal=10&"
                    "rule_line=user%%20device-1%%20allow&"
                    "request_id=request-rule-put",
                    DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=policy.rule.delete&ordinal=10&"
                   "request_id=request-rule-delete",
                   DASHBOARD_CSRF);
    check_int_eq(flowie_control_dashboard_process_form(dashboard, &caller, DASHBOARD_CSRF, body,
                                                       strlen(body)),
                 TURBO_OK);
    (void)snprintf(body, sizeof(body),
                   "csrf=%s&operation=user.disable&principal_id=device-1&"
                   "request_id=request-user-disable",
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
