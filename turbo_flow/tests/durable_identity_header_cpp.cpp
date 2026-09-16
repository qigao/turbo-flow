#include "turbo_flow_durable_buffer.h"

static_assert(TURBO_FLOW_DURABLE_BUFFER_API_VERSION == 1u,
              "durable buffer API version must remain frozen");

int turbo_flow_durable_identity_header_cpp_probe() {
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  auto setter = &turbo_flow_msg_set_durable_identity;
  auto getter = &turbo_flow_msg_durable_identity;
  return identity.size == sizeof(identity) && identity.version == 1u && setter && getter ? 0 : 1;
}
