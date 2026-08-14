#include "turbo_flow_protocol_graph.h"

#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

static int flow_protocol_graph_layout(size_t payload_size, size_t *metadata_offset,
                                     size_t *total_size) {
  const size_t alignment = _Alignof(turbo_flow_protocol_metadata_t);
  size_t offset;
  if (!metadata_offset || !total_size || alignment == 0u) return TURBO_EINVAL;
  if (payload_size > SIZE_MAX - (alignment - 1u)) return TURBO_ERANGE;
  offset = (payload_size + alignment - 1u) & ~(alignment - 1u);
  if (offset > SIZE_MAX - sizeof(turbo_flow_protocol_metadata_t)) return TURBO_ERANGE;
  *metadata_offset = offset;
  *total_size = offset + sizeof(turbo_flow_protocol_metadata_t);
  return TURBO_OK;
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
  int rc;

  if (!sink || sink->size < sizeof(*sink) ||
      sink->abi_version != TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION || !sink->flow ||
      !sink->source_name || !sink->source_name[0] || !request ||
      request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION ||
      request->delivery_id == 0u || !request->message || !disposition)
    return TURBO_EINVAL;
  input = request->message;
  if (input->size < sizeof(*input) ||
      input->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      (!input->payload && input->payload_size != 0u) ||
      input->payload_size > input->payload_capacity ||
      input->metadata.size < sizeof(input->metadata) ||
      input->metadata.abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION)
    return TURBO_EINVAL;
  rc = flow_protocol_graph_layout(input->payload_size, &metadata_offset,
                                 &total_size);
  if (rc != TURBO_OK) return rc;
  buffer = mem_get_buffer(mem_global(), total_size);
  if (!buffer) return TURBO_ENOMEM;
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
  message.payload = tstr_v_from_buf((const char *)storage, input->payload_size);
  message.transport_context = metadata;
  rc = turbo_flow_publish(sink->flow, sink->source_name, &message);
  turbo_flow_msg_cleanup(&message);
  if (rc != TURBO_OK) return rc;
  *disposition = TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED;
  return TURBO_OK;
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
