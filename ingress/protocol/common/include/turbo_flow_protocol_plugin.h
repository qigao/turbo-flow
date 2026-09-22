#ifndef TURBO_FLOW_PROTOCOL_PLUGIN_H
#define TURBO_FLOW_PROTOCOL_PLUGIN_H

#include "turbo_flow_protocol.h"

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
typedef int (*turbo_flow_protocol_inspect_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata);

typedef int (*turbo_flow_protocol_reply_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_output_t *output);

typedef int (*turbo_flow_protocol_encode_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_command_view_t *command,
    turbo_flow_protocol_frame_output_t *output);


/**
 * Optional single-pass ingress decoder.
 *
 * The codec fully validates the frame, fills protocol-specific metadata fields,
 * and writes semantic/application bytes into the caller-owned output buffer.
 * The host finalizes protocol/direction/version/device identity exactly as it
 * does for inspect. The callback must not replace output->data or change
 * output->capacity and must not retain any input/output pointer.
 */
typedef int (*turbo_flow_protocol_decode_semantic_fn)(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata,
    turbo_flow_protocol_semantic_output_t *output);

typedef struct turbo_flow_protocol_codec_ops_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_inspect_fn inspect;
  turbo_flow_protocol_reply_fn reply;
  turbo_flow_protocol_encode_fn encode;
  /** Optional append-only ABI1 extension for semantic ingress. */
  turbo_flow_protocol_decode_semantic_fn decode_semantic;
} turbo_flow_protocol_codec_ops_t;

#define TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT                                                      \
  {sizeof(turbo_flow_protocol_codec_ops_t), TURBO_FLOW_PROTOCOL_ABI_VERSION, NULL, NULL, NULL, NULL}

/**
 * Create/destroy the opaque provider-neutral service used by plugin open/close.
 *
 * The implementation copies request strings and the fixed-size ops table, and
 * retains borrowed ctx until destroy. Concurrent ingress/egress is supported when inspect is
 * thread-safe. destroy must not race with a data-path call.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_create(
    const turbo_flow_protocol_open_request_t *request, const char *protocol_name,
    const char *default_protocol_version,
    turbo_flow_protocol_capabilities_t capabilities,
    const turbo_flow_protocol_codec_ops_t *ops, void *ctx,
    turbo_flow_protocol_t **out);
TURBO_FLOW_C_API void turbo_flow_protocol_destroy(turbo_flow_protocol_t *protocol);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_PLUGIN_H */
