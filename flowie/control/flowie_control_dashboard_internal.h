#ifndef FLOWIE_CONTROL_DASHBOARD_INTERNAL_H
#define FLOWIE_CONTROL_DASHBOARD_INTERNAL_H

#include "flowie_control_management_service_internal.h"
#include "flowie_control_management_session_internal.h"
#include "iris/iris_app.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_DASHBOARD_PATH "/v1/management/dashboard"
#define FLOWIE_CONTROL_DASHBOARD_USERS_PATH "/v1/management/dashboard/users"
#define FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH "/v1/management/dashboard/groups"
#define FLOWIE_CONTROL_DASHBOARD_ROLES_PATH "/v1/management/dashboard/roles"
#define FLOWIE_CONTROL_DASHBOARD_ACLS_PATH "/v1/management/dashboard/acls"
#define FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH "/v1/management/dashboard/audit"
#define FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH "/v1/management/dashboard/content"
#define FLOWIE_CONTROL_DASHBOARD_ACTION_PATH "/v1/management/dashboard/action"
#define FLOWIE_CONTROL_DASHBOARD_CSS_PATH "/v1/management/assets/control.css"
#define FLOWIE_CONTROL_DASHBOARD_JS_PATH "/v1/management/assets/control.js"
#define FLOWIE_CONTROL_DASHBOARD_HTMX_PATH "/v1/management/assets/htmx-2.0.9.min.js"
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH "/v1/management/login"
#define FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH "/v1/management/password"
#define FLOWIE_CONTROL_DASHBOARD_LOGOUT_PATH "/v1/management/logout"
#define FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE 64u
#define FLOWIE_CONTROL_DASHBOARD_BODY_MAX 16384u
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_WORKERS 4u
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_MAX_WORKERS 64u
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_QUEUE_CAPACITY 128u
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_MAX_QUEUE_CAPACITY 4096u
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_DEADLINE_MS 10000u
#define FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_MAX_DEADLINE_MS 60000u

typedef struct flowie_control_dashboard_s flowie_control_dashboard_t;

typedef enum flowie_control_dashboard_section_e {
  FLOWIE_CONTROL_DASHBOARD_SECTION_ALL = 0,
  FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW,
  FLOWIE_CONTROL_DASHBOARD_SECTION_USERS,
  FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS,
  FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES,
  FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS,
  FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT
} flowie_control_dashboard_section_t;

typedef struct flowie_control_dashboard_page_s {
  size_t size;
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char users_after[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char groups_after[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char roles_after[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint32_t policy_after;
  int policy_has_after;
  uint64_t audit_after;
  int audit_has_after;
  flowie_control_dashboard_section_t section;
} flowie_control_dashboard_page_t;

#define FLOWIE_CONTROL_DASHBOARD_PAGE_INIT                                                         \
  {sizeof(flowie_control_dashboard_page_t),                                                       \
   {0},                                                                                            \
   {0},                                                                                            \
   {0},                                                                                            \
   {0},                                                                                            \
   0u,                                                                                             \
   0,                                                                                              \
   0u,                                                                                             \
   0,                                                                                              \
   FLOWIE_CONTROL_DASHBOARD_SECTION_ALL}

typedef int (*flowie_control_dashboard_resolve_session_fn)(
    void *ctx, const Req *request, flowie_control_management_caller_t *caller_out,
    char csrf_token_out[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]);
typedef uint64_t (*flowie_control_dashboard_clock_fn)(void *ctx);
typedef int (*flowie_control_dashboard_login_fn)(
    void *ctx, const char *root_group_id, const char *principal_id, const uint8_t *secret,
    size_t secret_size, const char *remote_address,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]);
typedef int (*flowie_control_dashboard_logout_fn)(void *ctx, const char *token);

typedef struct flowie_control_dashboard_config_s {
  size_t size;
  flowie_control_management_service_t *service;
  flowie_control_dashboard_resolve_session_fn resolve_session;
  void *resolve_session_ctx;
  flowie_control_dashboard_clock_fn clock;
  void *clock_ctx;
  flowie_control_dashboard_login_fn login;
  flowie_control_dashboard_logout_fn logout;
  void *session_ctx;
  uint64_t session_ttl_seconds;
  int login_executor_enabled;
  uint32_t login_executor_workers;
  size_t login_executor_queue_capacity;
  uint32_t login_executor_deadline_ms;
  const char *resource_directory;
} flowie_control_dashboard_config_t;

#define FLOWIE_CONTROL_DASHBOARD_CONFIG_INIT                                                       \
  {sizeof(flowie_control_dashboard_config_t),                                                       \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0,                                                                                              \
   FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_WORKERS,                                        \
   FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_QUEUE_CAPACITY,                                 \
   FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_DEFAULT_DEADLINE_MS,                                    \
   NULL}

int flowie_control_dashboard_create(const flowie_control_dashboard_config_t *config,
                                    flowie_control_dashboard_t **out);
int flowie_control_dashboard_bind(flowie_control_dashboard_t *dashboard, iris_app_t *app);
void flowie_control_dashboard_unbind(flowie_control_dashboard_t *dashboard);
void flowie_control_dashboard_destroy(flowie_control_dashboard_t *dashboard);

/**
 * Execute a login through the configured bounded executor.
 *
 * When the executor is enabled, the caller must run on a CoroNet context owner lane. A deadline
 * abandons the result; a later successful result is revoked before its worker releases the job.
 */
int flowie_control_dashboard_execute_login(
    flowie_control_dashboard_t *dashboard, const char *root_group_id, const char *principal_id,
    const uint8_t *secret, size_t secret_size, const char *remote_address,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]);

/** Caller owns the returned malloc-compatible buffer and releases it with this API. */
int flowie_control_dashboard_render(flowie_control_dashboard_t *dashboard,
                                    const flowie_control_management_caller_t *caller,
                                    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
                                    char **html_out, size_t *html_size_out);
int flowie_control_dashboard_render_shell(flowie_control_dashboard_t *dashboard,
                                          const flowie_control_dashboard_page_t *page,
                                          char **html_out, size_t *html_size_out);
int flowie_control_dashboard_render_login(flowie_control_dashboard_t *dashboard, int group_mode,
                                          int show_error, char **html_out,
                                          size_t *html_size_out);
int flowie_control_dashboard_render_password(
    flowie_control_dashboard_t *dashboard,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], char **html_out,
    size_t *html_size_out);
int flowie_control_dashboard_render_page(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page, char **html_out, size_t *html_size_out);
void flowie_control_dashboard_html_free(char *html);

/** Parse the exact, canonical dashboard cursor query into caller-owned storage. */
int flowie_control_dashboard_page_parse(const Req *request, flowie_control_dashboard_page_t *out);

/** Dashboard content and action endpoints accept only strict HTMX requests. */
int flowie_control_dashboard_request_is_htmx(const Req *request);

/** Dashboard mutations accept an exact Origin, or same-origin Fetch Metadata for absent/opaque Origin. */
int flowie_control_dashboard_request_is_same_origin(const Req *request);

/** Execute one validated form body without performing socket I/O. */
int flowie_control_dashboard_process_form(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], const char *body,
    size_t body_size);

#ifdef __cplusplus
}
#endif

#endif
