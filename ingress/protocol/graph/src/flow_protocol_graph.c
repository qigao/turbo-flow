#include "turbo_flow_protocol_graph.h"

#include "salts_error.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_protocol_graph_completion_ctx_s {
  turbo_flow_protocol_graph_completion_fn completion;
  void *completion_ctx;
  uint64_t delivery_id;
  uint64_t session_id;
  uint64_t session_generation;
} flow_protocol_graph_completion_ctx_t;

static void flow_protocol_graph_complete(void *ctx,
                                         const turbo_flow_publish_result_t *result) {
  flow_protocol_graph_completion_ctx_t *completion_ctx =
      (flow_protocol_graph_completion_ctx_t *)ctx;
  turbo_flow_protocol_graph_completion_t completion =
      TURBO_FLOW_PROTOCOL_GRAPH_COMPLETION_INIT;
  if (!completion_ctx) return;
  completion.delivery_id = completion_ctx->delivery_id;
  completion.session_id = completion_ctx->session_id;
  completion.session_generation = completion_ctx->session_generation;
  completion.status = result ? result->status : SALTS_EPROTO;
  completion_ctx->completion(completion_ctx->completion_ctx, &completion);
  free(completion_ctx);
}

static int flow_protocol_graph_layout(size_t payload_size, size_t *metadata_offset,
                                     size_t *total_size) {
  const size_t alignment = _Alignof(turbo_flow_protocol_metadata_t);
  size_t offset;
  if (!metadata_offset || !total_size || alignment == 0u) return SALTS_EINVAL;
  if (payload_size > SIZE_MAX - (alignment - 1u)) return SALTS_ERANGE;
  offset = (payload_size + alignment - 1u) & ~(alignment - 1u);
  if (offset > SIZE_MAX - sizeof(turbo_flow_protocol_metadata_t)) return SALTS_ERANGE;
  *metadata_offset = offset;
  *total_size = offset + sizeof(turbo_flow_protocol_metadata_t);
  return SALTS_OK;
}

int turbo_flow_protocol_graph_publish(
    void *ctx, const turbo_flow_protocol_publish_request_t *request,
    turbo_flow_protocol_publish_disposition_t *disposition) {
  turbo_flow_protocol_graph_sink_t *sink =
      (turbo_flow_protocol_graph_sink_t *)ctx;
  const turbo_flow_protocol_message_output_t *input;
  turbo_flow_protocol_metadata_t *metadata;
  turbo_flow_msg_t message;
  mem_buffer_t *buffer;
  size_t metadata_offset;
  size_t total_size;
  uint8_t *storage;
  turbo_flow_source_handoff_mode_t source_handoff =
      TURBO_FLOW_SOURCE_HANDOFF_INLINE;
  turbo_flow_protocol_graph_completion_fn completion = NULL;
  void *completion_user_ctx = NULL;
  flow_protocol_graph_completion_ctx_t *completion_ctx = NULL;
  int rc;

  if (!sink || sink->size < TURBO_FLOW_PROTOCOL_GRAPH_SINK_V1_SIZE ||
      (sink->size > TURBO_FLOW_PROTOCOL_GRAPH_SINK_V1_SIZE &&
       sink->size < sizeof(*sink)) ||
      sink->abi_version != TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION || !sink->flow ||
      !sink->source_name || !sink->source_name[0] || !request ||
      request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION ||
      request->delivery_id == 0u || !request->message || !disposition)
    return SALTS_EINVAL;
  input = request->message;
  if (input->size < sizeof(*input) ||
      input->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      (!input->payload && input->payload_size != 0u) ||
      input->payload_size > input->payload_capacity ||
      input->metadata.size < sizeof(input->metadata) ||
      input->metadata.abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION)
    return SALTS_EINVAL;
  if (sink->size >= sizeof(*sink)) {
    source_handoff = sink->source_handoff;
    completion = sink->completion;
    completion_user_ctx = sink->completion_ctx;
  }
  if (source_handoff != TURBO_FLOW_SOURCE_HANDOFF_INLINE &&
      source_handoff != TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED)
    return SALTS_EINVAL;
  if (source_handoff == TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED && !completion)
    return SALTS_EINVAL;
  rc = flow_protocol_graph_layout(input->payload_size, &metadata_offset,
                                 &total_size);
  if (rc != SALTS_OK) return rc;
  buffer = mem_get_buffer(mem_global(), total_size);
  if (!buffer) return SALTS_ENOMEM;
  storage = (uint8_t *)mem_buffer_data(buffer);
  if (input->payload_size > 0u)
    memcpy(storage, input->payload, input->payload_size);
  metadata = (turbo_flow_protocol_metadata_t *)(storage + metadata_offset);
  *metadata = input->metadata;
  mem_set_used(buffer, total_size);

  turbo_flow_msg_init(&message);
  message.id = request->delivery_id;
  message.type = input->metadata.message_type;
  message.buffer = buffer;
  message.payload = vstr_from_buf((const char *)storage, input->payload_size);
  message.transport_context = metadata;
  if (source_handoff == TURBO_FLOW_SOURCE_HANDOFF_INLINE) {
    rc = turbo_flow_publish(sink->flow, sink->source_name, &message);
  } else {
    completion_ctx = (flow_protocol_graph_completion_ctx_t *)calloc(1, sizeof(*completion_ctx));
    if (!completion_ctx) {
      turbo_flow_msg_cleanup(&message);
      return SALTS_ENOMEM;
    }
    completion_ctx->completion = completion;
    completion_ctx->completion_ctx = completion_user_ctx;
    completion_ctx->delivery_id = request->delivery_id;
    completion_ctx->session_id = request->session_id;
    completion_ctx->session_generation = request->session_generation;
    rc = turbo_flow_publish_async(sink->flow, sink->source_name, &message,
                                  flow_protocol_graph_complete, completion_ctx);
  }
  turbo_flow_msg_cleanup(&message);
  if (rc != SALTS_OK) {
    free(completion_ctx);
    return rc;
  }
  *disposition = source_handoff == TURBO_FLOW_SOURCE_HANDOFF_INLINE
                     ? TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED
                     : TURBO_FLOW_PROTOCOL_PUBLISH_PENDING;
  return SALTS_OK;
}

const turbo_flow_protocol_metadata_t *
turbo_flow_protocol_graph_metadata(const turbo_flow_msg_t *msg) {
  const turbo_flow_protocol_metadata_t *metadata;
  const uint8_t *begin;
  uintptr_t begin_address;
  uintptr_t end_address;
  uintptr_t metadata_address;
  size_t used;
  if (!msg || !msg->buffer || !msg->transport_context) return NULL;
  begin = (const uint8_t *)mem_buffer_const_data(msg->buffer);
  used = mem_buffer_used(msg->buffer);
  if (used < sizeof(turbo_flow_protocol_metadata_t)) return NULL;
  begin_address = (uintptr_t)begin;
  metadata_address = (uintptr_t)msg->transport_context;
  if (used > UINTPTR_MAX - begin_address) return NULL;
  end_address = begin_address + used;
  if (metadata_address < begin_address || metadata_address > end_address ||
      sizeof(*metadata) > end_address - metadata_address)
    return NULL;
  metadata = (const turbo_flow_protocol_metadata_t *)metadata_address;
  return metadata->size >= sizeof(*metadata) &&
                 metadata->abi_version == TURBO_FLOW_PROTOCOL_ABI_VERSION
             ? metadata
             : NULL;
}
