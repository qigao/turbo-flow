#ifndef TURBO_FLOW_CHTTP_PROVIDER_ADAPTER_INTERNAL_H
#define TURBO_FLOW_CHTTP_PROVIDER_ADAPTER_INTERNAL_H

#include "turbo_flow_chttp.h"

#ifdef __cplusplus
extern "C" {
#endif

TURBO_FLOW_C_API int turbo_flow_chttp_client_register_provider(
    const turbo_flow_chttp_client_config_t *config,
    const char *provider_identity,
    const char *const *stage_names,
    size_t stage_count,
    turbo_flow_chttp_client_t **out_client);

TURBO_FLOW_C_API int turbo_flow_chttp_server_register_provider(
    const turbo_flow_chttp_server_config_t *config,
    const char *provider_identity,
    const char *const *stage_names,
    size_t stage_count,
    turbo_flow_chttp_server_t **out_server);

TURBO_FLOW_C_API int turbo_flow_chttp_websocket_server_register_provider(
    const turbo_flow_chttp_websocket_server_config_t *config,
    const char *provider_identity,
    const char *const *stage_names,
    size_t stage_count,
    turbo_flow_chttp_websocket_server_t **out_server);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CHTTP_PROVIDER_ADAPTER_INTERNAL_H */
