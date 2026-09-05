#ifndef TURBO_FLOW_PROTOCOL_GRAPH_H
#define TURBO_FLOW_PROTOCOL_GRAPH_H

#include "turbo_flow.h"
#include "turbo_flow_protocol_runtime.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION 1u

typedef struct turbo_flow_protocol_graph_completion_s {
  size_t size;
  uint32_t abi_version;
  uint64_t delivery_id;
  uint64_t session_id;
  uint64_t session_generation;
  int status;
} turbo_flow_protocol_graph_completion_t;

#define TURBO_FLOW_PROTOCOL_GRAPH_COMPLETION_INIT                                               \
  {sizeof(turbo_flow_protocol_graph_completion_t), TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION,       \
   0u, 0u, 0u, SALTS_OK}

/**
 * Completion for one successfully admitted async protocol publication.
 *
 * Runs exactly once on a Flow ingress worker and may race with the return from
 * the runtime publish callback. `completion` is borrowed for this call. The
 * callback must only enqueue a non-blocking notification; the serialized owner
 * processes it after publish() returns and installs PENDING, then calls
 * protocol_runtime_settle(). It must not re-enter the runtime or synchronously
 * stop, drain, reset, or destroy the Flow.
 */
typedef void (*turbo_flow_protocol_graph_completion_fn)(
    void *ctx, const turbo_flow_protocol_graph_completion_t *completion);

/**
 * Borrowed binding from one protocol runtime to one STARTED TurboFlow source.
 * The flow, source name, and completion context must outlive every accepted
 * async publication using this binding.
 */
typedef struct turbo_flow_protocol_graph_sink_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_t *flow;
  const char *source_name;
  turbo_flow_source_handoff_mode_t source_handoff;
  turbo_flow_protocol_graph_completion_fn completion;
  void *completion_ctx;
} turbo_flow_protocol_graph_sink_t;

#define TURBO_FLOW_PROTOCOL_GRAPH_SINK_V1_SIZE                                                 \
  offsetof(turbo_flow_protocol_graph_sink_t, source_handoff)

#define TURBO_FLOW_PROTOCOL_GRAPH_SINK_INIT                                                     \
  {sizeof(turbo_flow_protocol_graph_sink_t), TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION, NULL, NULL, \
   TURBO_FLOW_SOURCE_HANDOFF_INLINE, NULL, NULL}

/**
 * Runtime publish callback that admits one normalized protocol message to Graph.
 *
 * The callback copies payload and protocol metadata into one message-owned
 * mem_buffer_t. INLINE returns SETTLED after synchronous graph execution.
 * ASYNC_BOUNDED returns PENDING only after bounded ingress admission and later
 * invokes the configured completion once. Admission errors are returned
 * unchanged and do not produce a completion.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_graph_publish(
    void *ctx, const turbo_flow_protocol_publish_request_t *request,
    turbo_flow_protocol_publish_disposition_t *disposition);

/**
 * Return message-owned protocol metadata, or NULL for a non-protocol message.
 * The returned view has the same lifetime as msg->buffer and must not be freed.
 */
TURBO_FLOW_C_API const turbo_flow_protocol_metadata_t *
turbo_flow_protocol_graph_metadata(const turbo_flow_msg_t *msg);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_GRAPH_H */
