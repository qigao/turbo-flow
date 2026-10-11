#ifndef TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_H
#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_H

#include "turbo_flow_plugin.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resolved_config.h"
#include "turbo_flow_resource.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION UINT32_C(3)

typedef struct turbo_flow_protocol_network_intake_s turbo_flow_protocol_network_intake_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_REPLY_POLICY_API_VERSION 1u

typedef enum turbo_flow_protocol_network_reply_point_e {
  /** Preserve the existing explicit no-reply behavior. */
  TURBO_FLOW_PROTOCOL_NETWORK_REPLY_NONE = 0,
  /** Authorize codec reply only after canonical durable business admission succeeds. */
  TURBO_FLOW_PROTOCOL_NETWORK_REPLY_DURABLE_ADMISSION = 1
} turbo_flow_protocol_network_reply_point_t;

typedef struct turbo_flow_protocol_network_reply_policy_s {
  size_t size;
  uint32_t version;
  turbo_flow_protocol_network_reply_point_t point;
  /** Hard caller-selected bound for one codec-produced reply frame. */
  size_t max_encoded_bytes;
} turbo_flow_protocol_network_reply_policy_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_REPLY_POLICY_INIT                                              \
  {sizeof(turbo_flow_protocol_network_reply_policy_t),                                             \
   TURBO_FLOW_PROTOCOL_NETWORK_REPLY_POLICY_API_VERSION, TURBO_FLOW_PROTOCOL_NETWORK_REPLY_NONE,   \
   0u}

typedef struct turbo_flow_protocol_network_intake_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  const turbo_flow_resolved_config_t *resolved;
  /**
   * Borrowed STARTED Flow receiving normalized protocol messages. The named
   * decoded source must feed exactly one generic durable-buffer stage.
   */
  turbo_flow_t *downstream_flow;
  /** Exact parsed Source stage name; its adapter identity selects the factory. */
  const char *source_stage_name;
  const char *decoder_adapter_name;
  const char *decoded_source_name;
  /**
   * Optional reply policy. NULL selects no replies.
   */
  const turbo_flow_protocol_network_reply_policy_t *reply_policy;
  /** Borrowed runtime; must outlive intake, including failed cleanup retries. */
  salts_component_plugin_runtime *component_runtime;
  /** Resolvers are borrowed only during create; acquired bindings remain owned. */
  const turbo_flow_provider_resolver_v2_t *provider_resolver;
  const turbo_flow_resource_resolver_v1_t *resource_resolver;
} turbo_flow_protocol_network_intake_config_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT                                             \
  {sizeof(turbo_flow_protocol_network_intake_config_t),                                            \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION,                                                 \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

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
  /** Decoded frames whose downstream publish returned durable admission success. */
  uint64_t frames_admitted;
  uint64_t source_polls;
  int backpressured;
  char source_endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
} turbo_flow_protocol_network_intake_snapshot_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT                                           \
  {sizeof(turbo_flow_protocol_network_intake_snapshot_t),                                          \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION,                                                 \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED,                                                    \
   SALTS_OK,                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0,                                                                                              \
   {0}}

/**
 * Prepare a canonical Source from its .flow typed config and deployment resource,
 * then compile one Source -> decoder edge. No network I/O starts during create.
 * catalog/resolved describe only decoder/protocol/mapper inputs and are never
 * consulted for Source provider selection or CNet fields. v2 configs are rejected.
 *
 * Preflight failure leaves *intake_flow_io caller-owned. Once materialization
 * begins, intake takes the Flow and sets *intake_flow_io to NULL, even on failure.
 * Success returns a COMPILED intake. Ordinary failure leaves *out NULL; if cleanup
 * fails, *out is a retained retry owner. Call stop followed by destroy and check
 * their results; start/poll are unavailable on that failed-create owner.
 * All operations require exclusive control-thread access. downstream_flow and
 * component_runtime must stay alive through successful final destroy.
 * Invalid layout/arguments return SALTS_EINVAL; missing bindings return
 * SALTS_ENOENT; unsupported transport/capability returns SALTS_ENOTSUP; invalid
 * transport bounds return SALTS_ERANGE. Provider/codec errors are propagated.
 * The real TCP/UDP create and retry sequence is exercised in
 * ingress/protocol/tests/protocol_network_e2e_fixture.c.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_create(
    const turbo_flow_protocol_network_intake_config_t *config, turbo_flow_t **intake_flow_io,
    turbo_flow_protocol_network_intake_t **out, turbo_flow_config_error_t *error);
TURBO_FLOW_C_API int
turbo_flow_protocol_network_intake_start(turbo_flow_protocol_network_intake_t *intake);
TURBO_FLOW_C_API int
turbo_flow_protocol_network_intake_poll(turbo_flow_protocol_network_intake_t *intake,
                                        uint32_t timeout_ms,
                                        turbo_flow_protocol_network_intake_snapshot_t *snapshot);
TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_snapshot(
    const turbo_flow_protocol_network_intake_t *intake,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot);
TURBO_FLOW_C_API int
turbo_flow_protocol_network_intake_stop(turbo_flow_protocol_network_intake_t *intake,
                                        uint64_t timeout_ms);
/** Requires successful stop. Failure retains intake and its unfinished cleanup
 * obligations for retry; success invalidates intake. NULL is accepted. */
TURBO_FLOW_C_API int
turbo_flow_protocol_network_intake_destroy(turbo_flow_protocol_network_intake_t *intake);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_H */
