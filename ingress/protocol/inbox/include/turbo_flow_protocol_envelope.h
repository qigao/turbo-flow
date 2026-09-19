#ifndef TURBO_FLOW_PROTOCOL_ENVELOPE_H
#define TURBO_FLOW_PROTOCOL_ENVELOPE_H

#include "turbo_flow_protocol_plugin.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Canonical pointer-free payload contract persisted by the generic durable
 * buffer for decoded protocol messages. The historical schema identity and
 * generated codec names stay unchanged so this migration changes ownership,
 * not the durable wire format.
 */
#define TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_NAME "turbo-flow.protocol.inbox"
#define TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_VERSION UINT32_C(1)
#define TURBO_FLOW_PROTOCOL_ENVELOPE_TYPE_NAME "ProtocolInboxEnvelope"
#define TURBO_FLOW_PROTOCOL_ENVELOPE_MEDIA_TYPE "application/vnd.tbe"
#define TURBO_FLOW_PROTOCOL_ENVELOPE_CONTENT_IDENTITY "protocol.ingress"

#define TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_FIXED_BYTES 24u
#define TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_VARIABLE_FIELDS 5u
#define TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_LENGTH_BYTES 4u
#define TURBO_FLOW_PROTOCOL_ENVELOPE_METADATA_BYTES                                             \
  (TURBO_FLOW_PROTOCOL_VERSION_MAX + TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX +                         \
   TURBO_FLOW_PROTOCOL_OPERATION_MAX + TURBO_FLOW_PROTOCOL_CORRELATION_MAX)
#define TURBO_FLOW_PROTOCOL_ENVELOPE_OVERHEAD                                                   \
  (TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_FIXED_BYTES +                                               \
   TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_VARIABLE_FIELDS *                                           \
       TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_LENGTH_BYTES +                                          \
   TURBO_FLOW_PROTOCOL_ENVELOPE_METADATA_BYTES)

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_ENVELOPE_H */
