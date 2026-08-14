#ifndef FLOW_PROTOCOL_PLUGIN_SUPPORT_H
#define FLOW_PROTOCOL_PLUGIN_SUPPORT_H

#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"

typedef struct flow_protocol_plugin_descriptor_s {
  const char *name;
  turbo_flow_protocol_kind_t protocol;
  const char *default_version;
  const char *const *versions;
  size_t version_count;
  turbo_flow_protocol_inspect_fn inspect;
  turbo_flow_protocol_reply_fn reply;
  turbo_flow_protocol_encode_fn encode;
  void *inspect_ctx;
} flow_protocol_plugin_descriptor_t;

int flow_protocol_plugin_open(
    void *ctx, const turbo_flow_protocol_open_request_t *request,
    turbo_flow_protocol_service_t *service);
void flow_protocol_plugin_close(void *ctx,
                               turbo_flow_protocol_service_t *service);

int flow_protocol_metadata_text(char *out, size_t capacity, const char *text);
int flow_protocol_metadata_text_n(char *out, size_t capacity, const char *text,
                                 size_t length);
int flow_protocol_metadata_format(char *out, size_t capacity,
                                 const char *format, unsigned value);
int flow_protocol_coap_inspect(const turbo_flow_protocol_frame_view_t *frame,
                              turbo_flow_protocol_metadata_t *metadata,
                              int lwm2m);
int flow_protocol_coap_reply(
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_output_t *output, int lwm2m,
    uint16_t separate_message_id);
int flow_protocol_coap_encode(
    const turbo_flow_protocol_command_view_t *command,
    turbo_flow_protocol_frame_output_t *output, int lwm2m);

#endif /* FLOW_PROTOCOL_PLUGIN_SUPPORT_H */
