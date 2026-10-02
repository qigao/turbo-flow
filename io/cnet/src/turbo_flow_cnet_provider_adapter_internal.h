#ifndef TURBO_FLOW_CNET_PROVIDER_ADAPTER_INTERNAL_H
#define TURBO_FLOW_CNET_PROVIDER_ADAPTER_INTERNAL_H

#include "turbo_flow_cnet.h"

#ifdef __cplusplus
extern "C" {
#endif

TURBO_FLOW_C_API int turbo_flow_cnet_stream_sink_register_provider(
    const turbo_flow_cnet_stream_sink_config_t *config,
    const char *provider_identity,
    const char *const *stage_names,
    size_t stage_count,
    turbo_flow_cnet_stream_sink_t **sink_out);

TURBO_FLOW_C_API int turbo_flow_cnet_datagram_sink_register_provider(
    const turbo_flow_cnet_datagram_sink_config_t *config,
    const char *provider_identity,
    const char *const *stage_names,
    size_t stage_count,
    turbo_flow_cnet_datagram_sink_t **sink_out);

TURBO_FLOW_C_API int turbo_flow_cnet_packet_sink_register_provider(
    const turbo_flow_cnet_packet_sink_config_t *config,
    const char *provider_identity,
    const char *const *stage_names,
    size_t stage_count,
    turbo_flow_cnet_packet_sink_t **sink_out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CNET_PROVIDER_ADAPTER_INTERNAL_H */
