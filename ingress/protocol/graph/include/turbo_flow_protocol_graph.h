#ifndef TURBO_FLOW_PROTOCOL_GRAPH_H
#define TURBO_FLOW_PROTOCOL_GRAPH_H

#include "turbo_flow.h"
#include "turbo_flow_protocol_runtime.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION 1u

/**
 * Borrowed binding from one protocol runtime to one STARTED TurboFlow source.
 * The flow and source name must outlive every callback using this binding.
 */
typedef struct turbo_flow_protocol_graph_sink_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_t *flow;
  const char *source_name;
} turbo_flow_protocol_graph_sink_t;

#define TURBO_FLOW_PROTOCOL_GRAPH_SINK_INIT                                                     \
  {sizeof(turbo_flow_protocol_graph_sink_t), TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION, NULL, NULL}

/**
 * Runtime publish callback that admits one normalized protocol message to Graph.
 *
 * The callback copies payload and protocol metadata into one message-owned
 * mem_buffer_t. On success it returns SETTLED because turbo_flow_publish() is a
 * synchronous graph boundary. Graph errors are returned unchanged.
 */
CXX_C_API int turbo_flow_protocol_graph_publish(
    void *ctx, const turbo_flow_protocol_publish_request_t *request,
    turbo_flow_protocol_publish_disposition_t *disposition);

/**
 * Return message-owned protocol metadata, or NULL for a non-protocol message.
 * The returned view has the same lifetime as msg->buffer and must not be freed.
 */
CXX_C_API const turbo_flow_protocol_metadata_t *
turbo_flow_protocol_graph_metadata(const turbo_flow_msg_t *msg);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_GRAPH_H */
