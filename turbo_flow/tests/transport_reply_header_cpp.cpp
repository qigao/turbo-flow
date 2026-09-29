#include "turbo_flow.h"

#include <cstddef>
#include <type_traits>

static_assert(std::is_standard_layout<turbo_flow_transport_reply_session_t>::value,
              "reply session must remain a C-compatible standard-layout value");
static_assert(std::is_standard_layout<turbo_flow_transport_reply_request_t>::value,
              "reply request must remain a C-compatible standard-layout value");
static_assert(std::is_standard_layout<turbo_flow_transport_reply_terminal_t>::value,
              "reply terminal must remain a C-compatible standard-layout value");
static_assert(offsetof(turbo_flow_transport_reply_request_t, session) >
                  offsetof(turbo_flow_transport_reply_request_t, version),
              "session token must remain explicit in request ABI");
static_assert(TURBO_FLOW_TRANSPORT_REPLY_SESSION_BYTES >= sizeof(uint64_t),
              "opaque reply token must hold generation-bearing handles");
