#ifndef TURBO_FLOW_HTTP_SERVER_H
#define TURBO_FLOW_HTTP_SERVER_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_http_types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct coro_context_s coro_context_t;
typedef struct iris_app iris_app_t;

typedef struct turbo_flow_http_server_config_s {
  uint16_t port;
  const char *route;
  turbo_flow_http_method_t method;
  size_t max_body_size;
  int response_status;
  const char *response_content_type;
  coro_context_t *context;
  int take_context_ownership;
  iris_app_t *app;
  int take_app_ownership;
  /**
   * Optional host registry and trusted request-body schema selector.
   * The registry must outlive this adapter because request media is resolved at runtime.
   */
  const turbo_flow_content_binding_t *content_binding;
} turbo_flow_http_server_config_t;

/**
 * Register one HTTP request/reply boundary.
 *
 * Use the same adapter name on one source and one reachable terminal stage.
 * Request bodies enter through the source; the terminal stage payload becomes
 * the HTTP response body.
 */
TURBO_FLOW_C_API int
turbo_flow_http_register_server_adapter(turbo_flow_t *flow, const char *name,
                                        const turbo_flow_http_server_config_t *config);

/**
 * Register an owned Iris boundary from adapter kind `http` in a resolved YAML snapshot.
 * CoroNet context, Iris app, content registry, and ownership remain host-only state.
 */
TURBO_FLOW_C_API int turbo_flow_http_register_server_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_HTTP_SERVER_H */
