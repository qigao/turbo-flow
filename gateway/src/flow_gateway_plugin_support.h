#ifndef FLOW_GATEWAY_PLUGIN_SUPPORT_H
#define FLOW_GATEWAY_PLUGIN_SUPPORT_H

#include "turbo_flow_gateway.h"
#include "turbo_flow_gateway_plugin.h"

typedef struct flow_gateway_plugin_descriptor_s {
  const char *name;
  turbo_flow_gateway_protocol_t protocol;
  const char *default_version;
  const char *const *versions;
  size_t version_count;
  turbo_flow_gateway_inspect_fn inspect;
  turbo_flow_gateway_reply_fn reply;
  turbo_flow_gateway_encode_fn encode;
  void *inspect_ctx;
} flow_gateway_plugin_descriptor_t;

int flow_gateway_plugin_open(
    void *ctx, const turbo_flow_gateway_open_request_t *request,
    turbo_flow_gateway_service_t *service);
void flow_gateway_plugin_close(void *ctx,
                               turbo_flow_gateway_service_t *service);

int flow_gateway_metadata_text(char *out, size_t capacity, const char *text);
int flow_gateway_metadata_text_n(char *out, size_t capacity, const char *text,
                                 size_t length);
int flow_gateway_metadata_format(char *out, size_t capacity,
                                 const char *format, unsigned value);
int flow_gateway_coap_inspect(const turbo_flow_gateway_frame_view_t *frame,
                              turbo_flow_gateway_metadata_t *metadata,
                              int lwm2m);
int flow_gateway_coap_reply(
    const turbo_flow_gateway_frame_view_t *request, int status,
    turbo_flow_gateway_frame_output_t *output, int lwm2m,
    uint16_t separate_message_id);
int flow_gateway_coap_encode(
    const turbo_flow_gateway_command_view_t *command,
    turbo_flow_gateway_frame_output_t *output, int lwm2m);

#endif /* FLOW_GATEWAY_PLUGIN_SUPPORT_H */
