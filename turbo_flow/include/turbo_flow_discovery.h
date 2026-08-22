#ifndef TURBO_FLOW_DISCOVERY_H
#define TURBO_FLOW_DISCOVERY_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DISCOVERY_PEER_ID_MAX 127u
#define TURBO_FLOW_DISCOVERY_MAX_PEERS 256u

/** Pointer-free endpoint value copied from a discovery registry. */
typedef struct turbo_flow_discovery_peer_s {
  size_t size;
  char peer_id[TURBO_FLOW_DISCOVERY_PEER_ID_MAX + 1u];
  char host[TURBO_FLOW_ENDPOINT_MAX + 1u];
  char path[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int port;
} turbo_flow_discovery_peer_t;

#define TURBO_FLOW_DISCOVERY_PEER_INIT {sizeof(turbo_flow_discovery_peer_t), {0}, {0}, {0}, 0}

typedef struct turbo_flow_discovery_peer_list_s {
  size_t size;
  uint64_t registry_version;
  const turbo_flow_discovery_peer_t *peers;
  size_t peer_count;
} turbo_flow_discovery_peer_list_t;

#define TURBO_FLOW_DISCOVERY_PEER_LIST_INIT {sizeof(turbo_flow_discovery_peer_list_t), 0u, NULL, 0u}

typedef struct turbo_flow_discovery_controller_s turbo_flow_discovery_controller_t;

typedef struct turbo_flow_discovery_controller_config_s {
  size_t size;
  turbo_flow_t *flow;
  /** Fixed adapter slots managed by this controller; names are copied. */
  const char *const *adapter_names;
  size_t adapter_count;
} turbo_flow_discovery_controller_config_t;

#define TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT                                                \
  {sizeof(turbo_flow_discovery_controller_config_t), NULL, NULL, 0u}

typedef struct turbo_flow_discovery_replace_result_s {
  size_t size;
  int status;
  int rollback_status;
  uint64_t version_before;
  uint64_t version_after;
  size_t added;
  size_t updated;
  size_t removed;
  size_t unchanged;
} turbo_flow_discovery_replace_result_t;

#define TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT                                                   \
  {sizeof(turbo_flow_discovery_replace_result_t), TURBO_OK, TURBO_OK, 0u, 0u, 0u, 0u, 0u, 0u}

/**
 * Fill caller-owned peer storage and return one immutable registry version.
 * `capacity` is the controller slot count. On success `count` must not exceed it.
 */
typedef int (*turbo_flow_discovery_fetch_fn)(void *ctx, uint64_t *registry_version,
                                             turbo_flow_discovery_peer_t *peers, size_t capacity,
                                             size_t *count);

typedef struct turbo_flow_discovery_source_s {
  size_t size;
  turbo_flow_discovery_fetch_fn fetch;
  void *ctx;
} turbo_flow_discovery_source_t;

#define TURBO_FLOW_DISCOVERY_SOURCE_INIT {sizeof(turbo_flow_discovery_source_t), NULL, NULL}

/**
 * Create a controller for a STARTED flow and quiesce every configured adapter slot.
 * On failure, already-quiesced slots are resumed before returning.
 */
TURBO_FLOW_C_API int
turbo_flow_discovery_controller_create(const turbo_flow_discovery_controller_config_t *config,
                                       turbo_flow_discovery_controller_t **out);

/** Destroy controller metadata without changing the last committed adapter state. */
TURBO_FLOW_C_API void
turbo_flow_discovery_controller_destroy(turbo_flow_discovery_controller_t *controller);

/**
 * Replace the complete desired peer set.
 *
 * Versions must increase. Replaying an identical current version is an idempotent
 * success; conflicting data at the same version returns TURBO_EPROTO and older
 * versions return TURBO_EALREADY. All input is validated and copied before owner
 * commands begin. A failed command rolls applied slots back to the prior set.
 * Calls on one controller must be serialized by the host.
 */
TURBO_FLOW_C_API int
turbo_flow_discovery_replace_peer_list(turbo_flow_discovery_controller_t *controller,
                                       const turbo_flow_discovery_peer_list_t *peer_list,
                                       turbo_flow_discovery_replace_result_t *result);

/**
 * Fetch one registry snapshot and reconcile it; fetch failure leaves adapters unchanged.
 * Calls on one controller must be serialized by the host.
 */
TURBO_FLOW_C_API int turbo_flow_discovery_poll(turbo_flow_discovery_controller_t *controller,
                                        const turbo_flow_discovery_source_t *source,
                                        turbo_flow_discovery_replace_result_t *result);

TURBO_FLOW_C_API uint64_t
turbo_flow_discovery_registry_version(const turbo_flow_discovery_controller_t *controller);
TURBO_FLOW_C_API size_t
turbo_flow_discovery_active_peer_count(const turbo_flow_discovery_controller_t *controller);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DISCOVERY_H */
