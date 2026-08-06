#ifndef FLOWIE_CONTROL_DASHBOARD_VIEW_INTERNAL_H
#define FLOWIE_CONTROL_DASHBOARD_VIEW_INTERNAL_H

#include "flowie_control_dashboard_internal.h"

typedef struct flowie_control_dashboard_view_s flowie_control_dashboard_view_t;

typedef enum flowie_control_dashboard_asset_e {
  FLOWIE_CONTROL_DASHBOARD_ASSET_CSS = 0,
  FLOWIE_CONTROL_DASHBOARD_ASSET_JS,
  FLOWIE_CONTROL_DASHBOARD_ASSET_HTMX
} flowie_control_dashboard_asset_t;

int flowie_control_dashboard_view_create(const char *resource_directory,
                                         flowie_control_dashboard_view_t **out);
void flowie_control_dashboard_view_destroy(flowie_control_dashboard_view_t *view);

int flowie_control_dashboard_view_render_shell(
    flowie_control_dashboard_view_t *view, const flowie_control_dashboard_page_t *page,
    char **html_out, size_t *html_size_out);
int flowie_control_dashboard_view_render_login(flowie_control_dashboard_view_t *view,
                                               int group_mode, int show_error, char **html_out,
                                               size_t *html_size_out);
int flowie_control_dashboard_view_render_password(
    flowie_control_dashboard_view_t *view,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], char **html_out,
    size_t *html_size_out);
int flowie_control_dashboard_view_render_content(
    flowie_control_dashboard_view_t *view, flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *authority_caller,
    const flowie_control_management_caller_t *scoped_caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page,
    const flowie_control_dashboard_action_result_t *action_result, char **html_out,
    size_t *html_size_out);
int flowie_control_dashboard_view_render_error(flowie_control_dashboard_view_t *view,
                                               const char *message, char **html_out,
                                               size_t *html_size_out);
int flowie_control_dashboard_view_asset(const flowie_control_dashboard_view_t *view,
                                        flowie_control_dashboard_asset_t asset,
                                        const void **data_out, size_t *size_out);

#endif
