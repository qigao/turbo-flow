#ifndef TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_H
#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_H

#include "turbo_flow_inbox.h"
#include "turbo_flow_plugin.h"
#include "turbo_flow_resolved_config.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION UINT32_C(1)

typedef struct turbo_flow_protocol_network_intake_s turbo_flow_protocol_network_intake_t;

typedef struct turbo_flow_protocol_network_intake_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  const turbo_flow_resolved_config_t *resolved;
  turbo_flow_inbox_t *inbox;
  const char *source_adapter_name;
  const char *intake_adapter_name;
} turbo_flow_protocol_network_intake_config_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT                                             \
  {sizeof(turbo_flow_protocol_network_intake_config_t),                                            \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION, NULL, NULL, NULL, NULL, NULL}

typedef enum turbo_flow_protocol_network_intake_state_e {
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED = 1,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_BACKPRESSURED,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPING,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED
} turbo_flow_protocol_network_intake_state_t;

typedef struct turbo_flow_protocol_network_intake_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_protocol_network_intake_state_t state;
  int status;
  size_t active_sessions;
  size_t pending_claims;
  size_t pending_bytes;
  uint64_t frames_admitted;
  uint64_t source_polls;
  int backpressured;
  char source_endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
} turbo_flow_protocol_network_intake_snapshot_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT                                           \
  {sizeof(turbo_flow_protocol_network_intake_snapshot_t),                                          \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION,                                                 \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED, SALTS_OK, 0u, 0u, 0u, 0u, 0u, 0, {0}}

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_create(
    const turbo_flow_protocol_network_intake_config_t *config, turbo_flow_t **intake_flow_io,
    turbo_flow_protocol_network_intake_t **out, turbo_flow_config_error_t *error);
TURBO_FLOW_C_API int
turbo_flow_protocol_network_intake_start(turbo_flow_protocol_network_intake_t *intake);
TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_poll(
    turbo_flow_protocol_network_intake_t *intake, uint32_t timeout_ms,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot);
TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_snapshot(
    const turbo_flow_protocol_network_intake_t *intake,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot);
TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_stop(
    turbo_flow_protocol_network_intake_t *intake, uint64_t timeout_ms);
TURBO_FLOW_C_API int
turbo_flow_protocol_network_intake_destroy(turbo_flow_protocol_network_intake_t *intake);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_H */
