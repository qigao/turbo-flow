#ifndef TURBO_FLOW_RPC_H
#define TURBO_FLOW_RPC_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct coro_context_s coro_context_t;
typedef struct iris_app iris_app_t;

typedef struct turbo_flow_rpc_server_config_s {
  uint16_t port;
  const char *endpoint;
  size_t max_request_size;
  coro_context_t *context;
  int take_context_ownership;
  iris_app_t *app;
  int take_app_ownership;
} turbo_flow_rpc_server_config_t;

typedef struct turbo_flow_rpc_client_config_s {
  const char *url;
  const char *method;
  const char *bearer_token;
  /** Request timeout in milliseconds; must be non-negative and 0 uses the client default. */
  int timeout_ms;
  /** Params JSON used by periodic source mode; defaults to null. */
  const char *poll_params;
  /** Non-zero registers this client as a periodic RPC source. */
  uint32_t poll_interval_ms;
} turbo_flow_rpc_client_config_t;

/** Register a JSON-RPC server boundary used by one source and one terminal reply stage. */
CXX_C_API int turbo_flow_rpc_register_server_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_rpc_server_config_t *config);

/** Register a JSON-RPC client transform: params JSON in, result JSON out. */
CXX_C_API int turbo_flow_rpc_register_client_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_rpc_client_config_t *config);

/** Register an owned RPC server from adapter kind `rpc` in a resolved YAML snapshot. */
CXX_C_API int turbo_flow_rpc_register_server_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

/** Register an owned RPC client from adapter kind `rpc` in a resolved YAML snapshot. */
CXX_C_API int turbo_flow_rpc_register_client_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

/** Return the current server request method, or NULL outside an RPC server dispatch. */
CXX_C_API const char *turbo_flow_rpc_request_method(const turbo_flow_msg_t *msg);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_RPC_H */
