#include "flow_protocol_plugin_support.h"

#include "salts_error.h"

#include <stdatomic.h>

static atomic_uint FLOW_COAP_NEXT_MESSAGE_ID = 1u;

static int flow_coap_inspect(void *ctx, const char *configured_version,
                             const turbo_flow_protocol_frame_view_t *frame,
                             turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  return flow_protocol_coap_inspect(frame, metadata, 0);
}

static int flow_coap_reply(void *ctx, const char *configured_version,
                           const turbo_flow_protocol_frame_view_t *request, int status,
                           turbo_flow_protocol_frame_output_t *output) {
  unsigned next;
  (void)configured_version;
  if (!ctx) return SALTS_EINVAL;
  next = atomic_fetch_add_explicit((atomic_uint *)ctx, 1u, memory_order_relaxed) + 1u;
  if ((uint16_t)next == 0u)
    next = atomic_fetch_add_explicit((atomic_uint *)ctx, 1u, memory_order_relaxed) + 1u;
  return flow_protocol_coap_reply(request, status, output, 0, (uint16_t)next);
}

static int flow_coap_encode(void *ctx, const char *configured_version,
                            const turbo_flow_protocol_command_view_t *command,
                            turbo_flow_protocol_frame_output_t *output) {
  (void)ctx;
  (void)configured_version;
  return flow_protocol_coap_encode(command, output, 0);
}

static const char *const FLOW_COAP_VERSIONS[] = {"RFC7252"};
static const flow_protocol_plugin_descriptor_t FLOW_COAP_DESCRIPTOR = {
    "coap",
    TURBO_FLOW_PROTOCOL_COAP,
    "RFC7252",
    FLOW_COAP_VERSIONS,
    sizeof(FLOW_COAP_VERSIONS) / sizeof(FLOW_COAP_VERSIONS[0]),
    flow_coap_inspect,
    flow_coap_reply,
    flow_coap_encode,
    &FLOW_COAP_NEXT_MESSAGE_ID};

static const turbo_flow_protocol_plugin_api_t FLOW_COAP_API = {
    sizeof(turbo_flow_protocol_plugin_api_t),
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
    "coap",
    TURBO_FLOW_PROTOCOL_COAP,
    TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
        TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE,
    (void *)&FLOW_COAP_DESCRIPTOR,
    flow_protocol_plugin_open,
    flow_protocol_plugin_close};

FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT("coap", "1.0.0", FLOW_COAP_API)
