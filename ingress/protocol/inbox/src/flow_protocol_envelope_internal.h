#ifndef FLOW_PROTOCOL_ENVELOPE_INTERNAL_H
#define FLOW_PROTOCOL_ENVELOPE_INTERNAL_H

#include "turbo_flow_protocol_inbox.h"

#ifdef __cplusplus
extern "C" {
#endif

int flow_protocol_envelope_encode(const turbo_flow_protocol_message_output_t *message,
                                  size_t max_payload_bytes, void *buffer, size_t capacity,
                                  size_t *written);

int flow_protocol_envelope_content_descriptor(turbo_flow_content_descriptor_t *content);

#ifdef __cplusplus
}
#endif

#endif /* FLOW_PROTOCOL_ENVELOPE_INTERNAL_H */
