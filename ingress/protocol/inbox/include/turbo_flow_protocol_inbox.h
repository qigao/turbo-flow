#ifndef TURBO_FLOW_PROTOCOL_INBOX_H
#define TURBO_FLOW_PROTOCOL_INBOX_H

#include "turbo_flow_inbox.h"
#include "turbo_flow_protocol_source.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION UINT32_C(1)
#define TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_NAME "turbo-flow.protocol.inbox"
#define TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_VERSION UINT32_C(1)
#define TURBO_FLOW_PROTOCOL_INBOX_TYPE_NAME "ProtocolInboxEnvelope"
#define TURBO_FLOW_PROTOCOL_INBOX_MEDIA_TYPE "application/vnd.tbe"
#define TURBO_FLOW_PROTOCOL_INBOX_CONTENT_IDENTITY "protocol.ingress"

#define TURBO_FLOW_PROTOCOL_INBOX_SOURCE_ID_MAX 127u
#define TURBO_FLOW_PROTOCOL_INBOX_ADMISSION_ID_MAX 255u
#define TURBO_FLOW_PROTOCOL_INBOX_CORRELATION_MAX 255u
#define TURBO_FLOW_PROTOCOL_INBOX_TBE_FIXED_BYTES 24u
#define TURBO_FLOW_PROTOCOL_INBOX_TBE_VARIABLE_FIELDS 5u
#define TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES 4u
#define TURBO_FLOW_PROTOCOL_INBOX_METADATA_BYTES                                                   \
  (TURBO_FLOW_PROTOCOL_VERSION_MAX + TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX +                           \
   TURBO_FLOW_PROTOCOL_OPERATION_MAX + TURBO_FLOW_PROTOCOL_CORRELATION_MAX)
#define TURBO_FLOW_PROTOCOL_INBOX_ENVELOPE_OVERHEAD                                                \
  (TURBO_FLOW_PROTOCOL_INBOX_TBE_FIXED_BYTES +                                                     \
   TURBO_FLOW_PROTOCOL_INBOX_TBE_VARIABLE_FIELDS * TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES +    \
   TURBO_FLOW_PROTOCOL_INBOX_METADATA_BYTES)
#define TURBO_FLOW_PROTOCOL_INBOX_DEFAULT_MAX_PAYLOAD_BYTES (64u * 1024u)
#define TURBO_FLOW_PROTOCOL_INBOX_DEFAULT_MAX_ENVELOPE_BYTES                                       \
  (TURBO_FLOW_PROTOCOL_INBOX_DEFAULT_MAX_PAYLOAD_BYTES +                                           \
   TURBO_FLOW_PROTOCOL_INBOX_ENVELOPE_OVERHEAD)

typedef struct turbo_flow_protocol_inbox_s turbo_flow_protocol_inbox_t;

/**
 * Borrowed decoded frame identity request.
 *
 * The transport-local delivery/session counters are intentionally absent: a
 * durable admission identity must be derived from configured Source state or
 * protocol/application data, never from one process lifetime.
 */
typedef struct turbo_flow_protocol_inbox_identity_request_s {
  size_t size;
  uint32_t abi_version;
  const turbo_flow_protocol_message_output_t *message;
} turbo_flow_protocol_inbox_identity_request_t;

#define TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_REQUEST_INIT                                            \
  {sizeof(turbo_flow_protocol_inbox_identity_request_t), TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION,    \
   NULL}

/**
 * Borrowed identities that must remain valid until the enclosing
 * turbo_flow_protocol_inbox_admit() call returns. The adapter synchronously
 * copies them into the configured Inbox during that call and never retains
 * the views afterward.
 */
typedef struct turbo_flow_protocol_inbox_identity_s {
  size_t size;
  uint32_t abi_version;
  vstr source_id;
  vstr admission_id;
  vstr correlation;
  uint64_t source_sequence;
  uint64_t timestamp_ns;
} turbo_flow_protocol_inbox_identity_t;

#define TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_INIT                                                    \
  {sizeof(turbo_flow_protocol_inbox_identity_t),                                                   \
   TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION,                                                          \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   0u,                                                                                             \
   0u}

typedef int (*turbo_flow_protocol_inbox_identity_resolve_fn)(
    void *ctx, const turbo_flow_protocol_inbox_identity_request_t *request,
    turbo_flow_protocol_inbox_identity_t *identity);

typedef struct turbo_flow_protocol_inbox_identity_ops_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_inbox_identity_resolve_fn resolve;
} turbo_flow_protocol_inbox_identity_ops_t;

#define TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_OPS_INIT                                                \
  {sizeof(turbo_flow_protocol_inbox_identity_ops_t), TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION, NULL}

typedef struct turbo_flow_protocol_inbox_config_s {
  size_t size;
  uint32_t abi_version;
  /** Borrowed and required to outlive the adapter. */
  turbo_flow_inbox_t *inbox;
  /** Exact ABI vtable copied by create(). */
  const turbo_flow_protocol_inbox_identity_ops_t *identity_ops;
  /** Borrowed callback context required to outlive the adapter. */
  void *identity_ctx;
  size_t max_payload_bytes;
  size_t max_envelope_bytes;
} turbo_flow_protocol_inbox_config_t;

#define TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT                                                      \
  {sizeof(turbo_flow_protocol_inbox_config_t),                                                     \
   TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION,                                                          \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_PROTOCOL_INBOX_DEFAULT_MAX_PAYLOAD_BYTES,                                            \
   TURBO_FLOW_PROTOCOL_INBOX_DEFAULT_MAX_ENVELOPE_BYTES}

/**
 * Create a caller-serialized Source-to-Inbox adapter.
 *
 * The adapter owns one bounded encoding scratch buffer. It borrows the Inbox
 * and identity context. Calls to admit are synchronous and must not overlap or
 * re-enter the adapter.
 */
TURBO_FLOW_C_API int
turbo_flow_protocol_inbox_create(const turbo_flow_protocol_inbox_config_t *config,
                                 turbo_flow_protocol_inbox_t **out);

TURBO_FLOW_C_API void turbo_flow_protocol_inbox_destroy(turbo_flow_protocol_inbox_t *adapter);

/**
 * `turbo_flow_protocol_source_ops_t.admit` callback.
 *
 * On SALTS_OK, the selected Inbox owns a complete independent copy. Identity
 * provider and Inbox errors are returned unchanged; malformed success outputs
 * and malformed decoded messages fail fast.
 */
TURBO_FLOW_C_API int
turbo_flow_protocol_inbox_admit(void *ctx,
                                const turbo_flow_protocol_source_admit_request_t *request);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_INBOX_H */
