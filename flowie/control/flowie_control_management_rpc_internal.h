#ifndef FLOWIE_CONTROL_MANAGEMENT_RPC_INTERNAL_H
#define FLOWIE_CONTROL_MANAGEMENT_RPC_INTERNAL_H

#include "flowie_control_external_https_authenticator_internal.h"
#include "flowie_control_management_service_internal.h"
#include "iris/iris_app.h"
#include "iris/rpc_server.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_MANAGEMENT_RPC_REQUEST_MAX 65536u

typedef struct flowie_control_management_rpc_server_s flowie_control_management_rpc_server_t;

/** Resolve a transport-authenticated administrator. Returned strings are borrowed for one call. */
typedef int (*flowie_control_management_rpc_resolve_caller_fn)(
    void *ctx, const Req *request, flowie_control_management_caller_t *caller_out);
typedef uint64_t (*flowie_control_management_rpc_clock_fn)(void *ctx);
typedef int (*flowie_control_management_rpc_external_https_stats_fn)(
    void *ctx, flowie_control_external_https_authenticator_stats_t *stats_out);

typedef struct flowie_control_management_rpc_server_config_s {
  size_t size;
  flowie_control_management_service_t *service;
  rpc_context_t *rpc_context;
  flowie_control_management_rpc_resolve_caller_fn resolve_caller;
  void *resolve_caller_ctx;
  flowie_control_management_rpc_clock_fn clock;
  void *clock_ctx;
  flowie_control_management_rpc_external_https_stats_fn external_https_stats;
  void *external_https_stats_ctx;
} flowie_control_management_rpc_server_config_t;

#define FLOWIE_CONTROL_MANAGEMENT_RPC_SERVER_CONFIG_INIT                                           \
  {sizeof(flowie_control_management_rpc_server_config_t),                                          \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/** The RPC context and management service are borrowed and must outlive the server. */
int flowie_control_management_rpc_server_create(
    const flowie_control_management_rpc_server_config_t *config,
    flowie_control_management_rpc_server_t **out);

/** Bind to one explicit Iris app. The app must stop before server destruction. */
int flowie_control_management_rpc_server_bind(flowie_control_management_rpc_server_t *server,
                                              iris_app_t *app);
void flowie_control_management_rpc_server_unbind(flowie_control_management_rpc_server_t *server);
void flowie_control_management_rpc_server_destroy(flowie_control_management_rpc_server_t *server);

/** Internal deterministic entry used by the bound route and focused tests. */
void flowie_control_management_rpc_server_handle(flowie_control_management_rpc_server_t *server,
                                                 Req *request, Res *response);

/** Build one JSON-RPC response without performing socket I/O. */
int flowie_control_management_rpc_server_execute(flowie_control_management_rpc_server_t *server,
                                                 Req *request, rpc_response_t *response_out);

#ifdef __cplusplus
}
#endif

#endif
