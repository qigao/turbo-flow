#include "turbo_flow_protocol_source.h"

#include <cstddef>
#include <type_traits>

static_assert(TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION == 2u,
              "semantic admission requires ProtocolSource ABI2");
static_assert(std::is_standard_layout<turbo_flow_protocol_source_config_t>::value,
              "ProtocolSource config must remain standard-layout");
static_assert(std::is_standard_layout<turbo_flow_protocol_source_admit_request_t>::value,
              "ProtocolSource admit request must remain standard-layout");
static_assert(offsetof(turbo_flow_protocol_source_config_t, decode_mode) >
                  offsetof(turbo_flow_protocol_source_config_t, max_buffered_bytes),
              "semantic config must remain an append-only tail");
static_assert(offsetof(turbo_flow_protocol_source_admit_request_t, semantic) >
                  offsetof(turbo_flow_protocol_source_admit_request_t, message),
              "semantic output must remain an append-only admit tail");
