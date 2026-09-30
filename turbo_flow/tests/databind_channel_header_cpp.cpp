#include "turbo_flow_databind.h"

#include <type_traits>

static_assert(std::is_standard_layout<DataBindSocketPlan>::value,
              "SocketPlan must remain C-compatible");
static_assert(std::is_standard_layout<DataBindFlowMQChannelPlan>::value,
              "FlowMQ ChannelPlan must remain C-compatible");

extern "C" int turbo_flow_databind_header_cpp_probe(void) {
  DataBindSocketPlan socket = DATA_BIND_SOCKET_PLAN_INIT;
  DataBindFlowMQChannelPlan flowmq = DATA_BIND_FLOWMQ_CHANNEL_PLAN_INIT;
  return socket.abi_version == DATA_BIND_SOCKET_PLAN_ABI_VERSION &&
         flowmq.abi_version == DATA_BIND_FLOWMQ_CHANNEL_PLAN_ABI_VERSION;
}
