#ifndef FLOW_PROTOCOL_NETWORK_INTAKE_INTERNAL_H
#define FLOW_PROTOCOL_NETWORK_INTAKE_INTERNAL_H

#include "turbo_flow_protocol_inbox.h"
#include "turbo_flow_resolved_config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum flow_protocol_network_transport_kind_e {
  FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP = 1,
  FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP = 2
} flow_protocol_network_transport_kind_t;

typedef struct flow_protocol_network_intake_settings_s {
  turbo_flow_protocol_kind_t protocol_kind;
  flow_protocol_network_transport_kind_t transport_kind;
  char protocol_provider[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
  char source_id[TURBO_FLOW_PROTOCOL_INBOX_SOURCE_ID_MAX + 1u];
  char source_adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char intake_adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  size_t max_sessions;
  size_t max_frame_size;
  size_t max_pending_claims;
  size_t max_pending_bytes;
  size_t source_scheduler_max_steps;
  size_t source_max_message_bytes;
} flow_protocol_network_intake_settings_t;

int flow_protocol_network_intake_preflight(
    const turbo_flow_resolved_config_t *resolved, const turbo_flow_t *flow,
    const char *source_adapter_name, const char *intake_adapter_name,
    flow_protocol_network_intake_settings_t *settings, turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* FLOW_PROTOCOL_NETWORK_INTAKE_INTERNAL_H */
