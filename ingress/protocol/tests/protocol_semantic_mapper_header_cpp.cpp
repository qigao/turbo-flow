#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_mapper.h"
#include "turbo_flow_protocol_plugin.h"

#include <type_traits>

static_assert(std::is_standard_layout<turbo_flow_protocol_semantic_output_t>::value,
              "semantic output must remain standard-layout C ABI");
static_assert(std::is_standard_layout<turbo_flow_protocol_mapper_request_t>::value,
              "mapper request must remain standard-layout C ABI");
static_assert(std::is_standard_layout<turbo_flow_protocol_mapper_output_t>::value,
              "mapper output must remain standard-layout C ABI");
static_assert(std::is_standard_layout<turbo_flow_protocol_mapper_v1_t>::value,
              "mapper vtable must remain standard-layout C ABI");
static_assert(std::is_standard_layout<turbo_flow_protocol_codec_ops_t>::value,
              "codec ops must remain standard-layout C ABI");
