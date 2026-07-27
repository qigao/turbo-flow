#ifndef TURBO_FLOW_GATEWAY_PLUGIN_H
#define TURBO_FLOW_GATEWAY_PLUGIN_H

#include "turbo_flow_gateway.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Provider-neutral codec hook used only while the owning module is retained.
 *
 * inspect must fully validate the frame and fill message_type, sequence,
 * operation, optional correlation_id, and optional frame-derived device_id.
 * The host fills protocol, direction, and protocol_version.
 */
typedef int (*turbo_flow_gateway_inspect_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_frame_view_t *frame,
    turbo_flow_gateway_metadata_t *metadata);

typedef int (*turbo_flow_gateway_reply_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_frame_view_t *request, int status,
    turbo_flow_gateway_frame_output_t *output);

typedef int (*turbo_flow_gateway_encode_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_command_view_t *command,
    turbo_flow_gateway_frame_output_t *output);

typedef struct turbo_flow_gateway_codec_ops_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_gateway_inspect_fn inspect;
  turbo_flow_gateway_reply_fn reply;
  turbo_flow_gateway_encode_fn encode;
} turbo_flow_gateway_codec_ops_t;

#define TURBO_FLOW_GATEWAY_CODEC_OPS_INIT                                                      \
  {sizeof(turbo_flow_gateway_codec_ops_t), TURBO_FLOW_GATEWAY_ABI_VERSION, NULL, NULL, NULL}

/**
 * Create/destroy the opaque provider-neutral service used by plugin open/close.
 *
 * The implementation copies request strings and the fixed-size ops table, and
 * retains borrowed ctx until destroy. Concurrent ingress/egress is supported when inspect is
 * thread-safe. destroy must not race with a data-path call.
 */
CXX_C_API int turbo_flow_gateway_create(
    const turbo_flow_gateway_open_request_t *request, const char *gateway_name,
    const char *default_protocol_version,
    turbo_flow_gateway_capabilities_t capabilities,
    const turbo_flow_gateway_codec_ops_t *ops, void *ctx,
    turbo_flow_gateway_t **out);
CXX_C_API void turbo_flow_gateway_destroy(turbo_flow_gateway_t *gateway);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_GATEWAY_PLUGIN_H */
