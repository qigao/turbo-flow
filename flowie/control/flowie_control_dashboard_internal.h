#ifndef FLOWIE_CONTROL_DASHBOARD_INTERNAL_H
#define FLOWIE_CONTROL_DASHBOARD_INTERNAL_H

#include "flowie_control_management_service_internal.h"
#include "iris/iris_app.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_DASHBOARD_PATH "/v1/management/dashboard"
#define FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH "/v1/management/dashboard/content"
#define FLOWIE_CONTROL_DASHBOARD_ACTION_PATH "/v1/management/dashboard/action"
#define FLOWIE_CONTROL_DASHBOARD_CSS_PATH "/v1/management/assets/control.css"
#define FLOWIE_CONTROL_DASHBOARD_HTMX_PATH "/v1/management/assets/htmx-2.0.9.min.js"
#define FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE 64u
#define FLOWIE_CONTROL_DASHBOARD_BODY_MAX 16384u

typedef struct flowie_control_dashboard_s flowie_control_dashboard_t;

typedef struct flowie_control_dashboard_page_s {
  size_t size;
  char users_after[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char groups_after[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char roles_after[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint32_t policy_after;
  int policy_has_after;
  uint64_t audit_after;
  int audit_has_after;
} flowie_control_dashboard_page_t;

#define FLOWIE_CONTROL_DASHBOARD_PAGE_INIT                                                         \
  {sizeof(flowie_control_dashboard_page_t), {0}, {0}, {0}, 0u, 0, 0u, 0}

typedef int (*flowie_control_dashboard_resolve_session_fn)(
    void *ctx, const Req *request, flowie_control_management_caller_t *caller_out,
    char csrf_token_out[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]);
typedef uint64_t (*flowie_control_dashboard_clock_fn)(void *ctx);

typedef struct flowie_control_dashboard_config_s {
  size_t size;
  flowie_control_management_service_t *service;
  flowie_control_dashboard_resolve_session_fn resolve_session;
  void *resolve_session_ctx;
  flowie_control_dashboard_clock_fn clock;
  void *clock_ctx;
  const char *resource_directory;
} flowie_control_dashboard_config_t;

#define FLOWIE_CONTROL_DASHBOARD_CONFIG_INIT                                                       \
  {sizeof(flowie_control_dashboard_config_t), NULL, NULL, NULL, NULL, NULL, NULL}

int flowie_control_dashboard_create(const flowie_control_dashboard_config_t *config,
                                    flowie_control_dashboard_t **out);
int flowie_control_dashboard_bind(flowie_control_dashboard_t *dashboard, iris_app_t *app);
void flowie_control_dashboard_unbind(flowie_control_dashboard_t *dashboard);
void flowie_control_dashboard_destroy(flowie_control_dashboard_t *dashboard);

/** Caller owns the returned malloc-compatible buffer and releases it with this API. */
int flowie_control_dashboard_render(flowie_control_dashboard_t *dashboard,
                                    const flowie_control_management_caller_t *caller,
                                    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
                                    char **html_out, size_t *html_size_out);
int flowie_control_dashboard_render_shell(flowie_control_dashboard_t *dashboard,
                                          const flowie_control_dashboard_page_t *page,
                                          char **html_out, size_t *html_size_out);
int flowie_control_dashboard_render_page(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page, char **html_out, size_t *html_size_out);
void flowie_control_dashboard_html_free(char *html);

/** Parse the exact, canonical dashboard cursor query into caller-owned storage. */
int flowie_control_dashboard_page_parse(const Req *request, flowie_control_dashboard_page_t *out);

/** Dashboard content and action endpoints accept only strict HTMX requests. */
int flowie_control_dashboard_request_is_htmx(const Req *request);

/** Execute one validated form body without performing socket I/O. */
int flowie_control_dashboard_process_form(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], const char *body,
    size_t body_size);

#ifdef __cplusplus
}
#endif

#endif
