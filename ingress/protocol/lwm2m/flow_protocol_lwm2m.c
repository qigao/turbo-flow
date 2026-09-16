#include "flow_protocol_plugin_support.h"

#include "salts_error.h"

#include <stdatomic.h>

static atomic_uint FLOW_LWM2M_NEXT_MESSAGE_ID = 1u;

static int flow_lwm2m_inspect(void *ctx, const char *configured_version,
                              const turbo_flow_protocol_frame_view_t *frame,
                              turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  return flow_protocol_coap_inspect(frame, metadata, 1);
}

static int flow_lwm2m_reply(void *ctx, const char *configured_version,
                            const turbo_flow_protocol_frame_view_t *request, int status,
                            turbo_flow_protocol_frame_output_t *output) {
  unsigned next;
  (void)configured_version;
  if (!ctx) return SALTS_EINVAL;
  next = atomic_fetch_add_explicit((atomic_uint *)ctx, 1u, memory_order_relaxed) + 1u;
  if ((uint16_t)next == 0u)
    next = atomic_fetch_add_explicit((atomic_uint *)ctx, 1u, memory_order_relaxed) + 1u;
  return flow_protocol_coap_reply(request, status, output, 1, (uint16_t)next);
}

static int flow_lwm2m_encode(void *ctx, const char *configured_version,
                             const turbo_flow_protocol_command_view_t *command,
                             turbo_flow_protocol_frame_output_t *output) {
  (void)ctx;
  (void)configured_version;
  return flow_protocol_coap_encode(command, output, 1);
}

static const char *const FLOW_LWM2M_VERSIONS[] = {"1.2.2"};
static const flow_protocol_plugin_descriptor_t FLOW_LWM2M_DESCRIPTOR = {
    "lwm2m",
    TURBO_FLOW_PROTOCOL_LWM2M,
    "1.2.2",
    FLOW_LWM2M_VERSIONS,
    sizeof(FLOW_LWM2M_VERSIONS) / sizeof(FLOW_LWM2M_VERSIONS[0]),
    flow_lwm2m_inspect,
    flow_lwm2m_reply,
    flow_lwm2m_encode,
    &FLOW_LWM2M_NEXT_MESSAGE_ID};

static const turbo_flow_protocol_plugin_api_t FLOW_LWM2M_API = {
    sizeof(turbo_flow_protocol_plugin_api_t),
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
    "lwm2m",
    TURBO_FLOW_PROTOCOL_LWM2M,
    TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
        TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE,
    (void *)&FLOW_LWM2M_DESCRIPTOR,
    flow_protocol_plugin_open,
    flow_protocol_plugin_close};

FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT("lwm2m", "1.0.0", FLOW_LWM2M_API)
