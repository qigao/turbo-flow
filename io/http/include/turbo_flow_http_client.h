#ifndef TURBO_FLOW_HTTP_CLIENT_H
#define TURBO_FLOW_HTTP_CLIENT_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_http_types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct http_client_s http_client_t;

typedef struct turbo_flow_http_client_config_s {
  /** Optional TurboHTTP client; when NULL the adapter creates one. */
  http_client_t *client;
  int take_client_ownership;
  const char *url;
  turbo_flow_http_method_t method;
  const char *const *headers;
  size_t header_count;
  const char *bearer_token;
  /** Request timeout in milliseconds; must be non-negative and 0 uses the client default. */
  int timeout_ms;
  size_t max_response_size;
  int success_status_min;
  int success_status_max;
  uint32_t max_pump_iterations;
  /** Non-zero registers this client as a periodic GET source. */
  uint32_t poll_interval_ms;
  /**
   * Optional host registry and trusted response schema selector.
   * The registry must outlive this adapter because response media is resolved at runtime.
   */
  const turbo_flow_content_binding_t *content_binding;
} turbo_flow_http_client_config_t;

/**
 * Register an HTTP client transform or periodic GET source.
 *
 * A supplied client must not be driven concurrently while this adapter is
 * running in polling mode. Ownership controls destruction only. Explicit flow
 * retry policy is supported for transform-mode GET, PUT, and DELETE requests;
 * only transport I/O and pump timeout failures are retried. POST, PATCH, HTTP
 * status failures, and polling-source retries remain adapter/config specific.
 */
TURBO_FLOW_C_API int
turbo_flow_http_register_client_adapter(turbo_flow_t *flow, const char *name,
                                        const turbo_flow_http_client_config_t *config);

/**
 * Register an owned HTTP client from adapter kind `http` in a resolved YAML snapshot.
 * Host objects (`client` and `content_binding`) are not accepted from YAML.
 */
TURBO_FLOW_C_API int turbo_flow_http_register_client_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_HTTP_CLIENT_H */
