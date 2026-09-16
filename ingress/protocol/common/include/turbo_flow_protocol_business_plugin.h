#ifndef TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_H
#define TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_H

#include "turbo_flow_protocol_business.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*turbo_flow_protocol_business_consume_committed_fn)(
    void *ctx, const turbo_flow_protocol_business_event_view_t *event);
typedef int (*turbo_flow_protocol_business_prepare_command_fn)(
    void *ctx, const turbo_flow_protocol_business_command_request_t *request,
    turbo_flow_protocol_business_command_output_t *output);

typedef struct turbo_flow_protocol_business_ops_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_business_consume_committed_fn consume_committed;
  turbo_flow_protocol_business_prepare_command_fn prepare_command;
} turbo_flow_protocol_business_ops_t;

#define TURBO_FLOW_PROTOCOL_BUSINESS_OPS_INIT                                                       \
  {sizeof(turbo_flow_protocol_business_ops_t), TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION, NULL, NULL}

/**
 * Create/destroy the opaque service returned by a business plugin open/close.
 *
 * The implementation copies the function table and identity strings. ctx is
 * borrowed until destroy. Calls are synchronous and caller-serialized.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_business_create(
    const char *business_name, turbo_flow_protocol_kind_t protocol, const char *profile,
    size_t max_payload_size, turbo_flow_protocol_business_capabilities_t capabilities,
    const turbo_flow_protocol_business_ops_t *ops, void *ctx, turbo_flow_protocol_business_t **out);
TURBO_FLOW_C_API void turbo_flow_protocol_business_destroy(turbo_flow_protocol_business_t *business);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_H */
