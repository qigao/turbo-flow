#ifndef TURBO_FLOW_DURABLE_BUFFER_H
#define TURBO_FLOW_DURABLE_BUFFER_H

#include "turbo_flow.h"
#include "turbo_flow_inbox.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DURABLE_BUFFER_API_VERSION UINT32_C(1)
#define TURBO_FLOW_DURABLE_SOURCE_ID_MAX 127u
#define TURBO_FLOW_DURABLE_ADMISSION_ID_MAX 255u
#define TURBO_FLOW_DURABLE_CORRELATION_MAX 255u
#define TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES (2u * 1024u * 1024u)

typedef struct turbo_flow_durable_identity_s {
  size_t size;
  uint32_t version;
  vstr source_id;
  vstr admission_id;
  vstr correlation;
  uint64_t source_sequence;
} turbo_flow_durable_identity_t;

#define TURBO_FLOW_DURABLE_IDENTITY_INIT                                                   \
  {sizeof(turbo_flow_durable_identity_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION,           \
   {NULL, 0u}, {NULL, 0u}, {NULL, 0u}, 0u}

typedef struct turbo_flow_durable_buffer_binding_s turbo_flow_durable_buffer_binding_t;

typedef enum turbo_flow_durable_identity_mode_e {
  TURBO_FLOW_DURABLE_IDENTITY_GENERATED = 1,
  TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED = 2
} turbo_flow_durable_identity_mode_t;

typedef struct turbo_flow_durable_buffer_binding_config_s {
  size_t size;
  uint32_t version;
  const char *resource_name;
  turbo_flow_inbox_t *inbox;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
} turbo_flow_durable_buffer_binding_config_t;

#define TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT                                      \
  {sizeof(turbo_flow_durable_buffer_binding_config_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION, \
   NULL, NULL, TURBO_FLOW_DURABLE_IDENTITY_GENERATED,                                      \
   TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES}

TURBO_FLOW_C_API int turbo_flow_msg_set_durable_identity(
    turbo_flow_msg_t *message, const turbo_flow_durable_identity_t *identity);

TURBO_FLOW_C_API int turbo_flow_msg_durable_identity(
    const turbo_flow_msg_t *message, turbo_flow_durable_identity_t *out);

/**
 * Bind one configured Graph durable-buffer resource to a borrowed Inbox.
 *
 * The binding is owned by `flow`; the Inbox handle and provider remain owned by
 * the caller and must outlive the binding. Binding is materialization-time only
 * and is rejected after the Graph has been compiled or started.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_bind(
    turbo_flow_t *flow, const turbo_flow_durable_buffer_binding_config_t *config,
    turbo_flow_durable_buffer_binding_t **out);

/**
 * Remove a binding without closing or destroying its borrowed Inbox provider.
 * A started Graph must be stopped before unbinding.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_unbind(
    turbo_flow_durable_buffer_binding_t *binding);

#ifdef __cplusplus
}
#endif

#endif
