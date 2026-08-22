#ifndef TURBO_FLOW_RPC_H
#define TURBO_FLOW_RPC_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_RPC_MODULE_VERSION 1u
#define TURBO_FLOW_RPC_CLIENT_MODULE "io.rpc.client"
#define TURBO_FLOW_RPC_CLIENT_CALL_OPERATION "rpc.client.call"
#define TURBO_FLOW_RPC_CLIENT_POLL_OPERATION "rpc.client.poll"
#define TURBO_FLOW_RPC_CLIENT_PRIMITIVE_TYPE "RpcClientConnection"
#define TURBO_FLOW_RPC_SERVER_MODULE "io.rpc.server"
#define TURBO_FLOW_RPC_SERVER_REQUEST_OPERATION "rpc.server.request"
#define TURBO_FLOW_RPC_SERVER_REPLY_OPERATION "rpc.server.reply"
#define TURBO_FLOW_RPC_SERVER_PRIMITIVE_TYPE "RpcServerEndpoint"

typedef struct coro_context_s coro_context_t;
typedef struct iris_app iris_app_t;
typedef struct turbo_http_s turbo_http_t;

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

typedef struct turbo_flow_rpc_http_client_binding_s {
  size_t size;
  /** Required injected standard TurboHTTP sync facade (turbo_http_create_sync). */
  turbo_http_t *client;
  /** Non-zero transfers destruction ownership to the RPC adapter. */
  int take_ownership;
} turbo_flow_rpc_http_client_binding_t;

#define TURBO_FLOW_RPC_HTTP_CLIENT_BINDING_INIT \
  {sizeof(turbo_flow_rpc_http_client_binding_t), NULL, 0}

/** Register a JSON-RPC server boundary used by one source and one terminal reply stage. */
TURBO_FLOW_C_API int turbo_flow_rpc_register_server_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_rpc_server_config_t *config);

/**
 * Register a JSON-RPC client transform or periodic source.
 *
 * This entry creates and owns a private synchronous TurboHTTP facade (H1).
 */
TURBO_FLOW_C_API int turbo_flow_rpc_register_client_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_rpc_client_config_t *config);

/**
 * Register with an injected synchronous TurboHTTP facade provider.
 *
 * The RPC wrapper borrows the provider. Adapter shutdown destroys the wrapper
 * first, then destroys the provider only when ownership was transferred.
 * A borrowed provider must outlive the adapter and may not be concurrently
 * driven by another adapter. The provider must be created with
 * turbo_http_create_sync() and keeps its own timeout; configured bearer
 * credentials are applied to it during registration.
 */
TURBO_FLOW_C_API int turbo_flow_rpc_register_client_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_rpc_client_config_t *config,
    const turbo_flow_rpc_http_client_binding_t *binding);

/** Register an owned RPC server from adapter kind `rpc` in a resolved YAML snapshot. */
TURBO_FLOW_C_API int turbo_flow_rpc_register_server_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

/**
 * Register an owned RPC client from adapter kind `rpc` in a resolved YAML snapshot.
 * Host object field http_client is not accepted from YAML.
 */
TURBO_FLOW_C_API int turbo_flow_rpc_register_client_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name);

/** Return the current server request method, or NULL outside an RPC server dispatch. */
TURBO_FLOW_C_API const char *turbo_flow_rpc_request_method(const turbo_flow_msg_t *msg);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_RPC_H */
