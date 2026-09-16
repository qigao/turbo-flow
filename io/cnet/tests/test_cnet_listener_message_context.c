#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include <string.h>

static turbo_flow_msg_t listener_context_message(cnet_connection connection, const char *payload) {
  turbo_flow_msg_t message;
  const size_t payload_size = strlen(payload);
  const size_t storage_size = sizeof(turbo_flow_cnet_listener_message_context_t) + payload_size;
  mem_buffer_t *buffer = mem_get_buffer(mem_global(), storage_size);
  turbo_flow_cnet_listener_message_context_t *context;

  turbo_flow_msg_init(&message);
  check_not_null(buffer);
  context = (turbo_flow_cnet_listener_message_context_t *)mem_buffer_data(buffer);
  memset(context, 0, sizeof(*context));
  context->size = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_V1_SIZE;
  context->version = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION;
  context->connection = connection;
  memcpy(mem_buffer_data(buffer) + sizeof(*context), payload, payload_size);
  mem_set_used(buffer, storage_size);

  message.buffer = buffer;
  message.transport_context = context;
  message.payload =
      vstr_from_buf(mem_buffer_const_data(buffer) + sizeof(*context), payload_size);
  return message;
}

spec("CNet listener message context") {
  it("exposes message-owned connection identity without changing payload bytes") {
    const cnet_connection connection = {7u, 9u};
    turbo_flow_msg_t message = listener_context_message(connection, "wire");
    const turbo_flow_cnet_listener_message_context_t *context =
        turbo_flow_cnet_listener_message_context(&message);

    check_not_null(context);
    check_equal(context->size, TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_V1_SIZE);
    check_equal(context->version, TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION);
    check_equal(context->connection.slot, connection.slot);
    check_equal(context->connection.generation, connection.generation);
    check_equal(message.payload.len, 4u);
    check_equal(memcmp(message.payload.data, "wire", 4u), 0);

    turbo_flow_msg_cleanup(&message);
  }

  it("rejects context pointers outside message-owned storage") {
    turbo_flow_msg_t message;
    mem_buffer_t *buffer = mem_get_buffer(mem_global(), 8u);

    check_not_null(buffer);
    mem_set_used(buffer, 8u);
    turbo_flow_msg_init(&message);
    message.buffer = buffer;
    message.payload = vstr_from_buf(mem_buffer_const_data(buffer), 8u);
    message.transport_context = (void *)(mem_buffer_const_data(buffer) + 8u);

    check_null(turbo_flow_cnet_listener_message_context(&message));
    turbo_flow_msg_cleanup(&message);
  }

  it("rejects a wrong version and a payload that does not follow the context") {
    turbo_flow_msg_t message = listener_context_message((cnet_connection){1u, 2u}, "x");
    turbo_flow_cnet_listener_message_context_t *context =
        (turbo_flow_cnet_listener_message_context_t *)message.transport_context;
    const char *base = mem_buffer_const_data(message.buffer);

    context->version = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION + 1u;
    check_null(turbo_flow_cnet_listener_message_context(&message));

    context->version = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION;
    message.payload = vstr_from_buf(base, 1u);
    check_null(turbo_flow_cnet_listener_message_context(&message));

    turbo_flow_msg_cleanup(&message);
  }

  it("rejects zero generation because it is not a live CNet connection identity") {
    turbo_flow_msg_t message = listener_context_message((cnet_connection){1u, 0u}, "x");
    check_null(turbo_flow_cnet_listener_message_context(&message));
    turbo_flow_msg_cleanup(&message);
  }
}
