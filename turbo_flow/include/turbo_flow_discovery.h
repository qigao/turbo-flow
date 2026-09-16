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
  {sizeof(turbo_flow_discovery_replace_result_t), SALTS_OK, SALTS_OK, 0u, 0u, 0u, 0u, 0u, 0u}

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
 * Each name must have exactly one command-capable CONNECTION resource provider:
 * missing providers return SALTS_ENOENT, ambiguous providers SALTS_EPROTO.
 * Names and stable UIDs are copied; the flow is borrowed until controller destroy.
 * Before effects, reserve 2*N history entries for N slots, including compensation.
 * The shared 256-entry history therefore admits at most 128 slots when empty;
 * insufficient remaining history returns SALTS_ENOSPC without owner effects.
 * Allocation failure returns SALTS_ENOMEM before owner effects.
 * On failure, attempt to resume every already-quiesced slot. If compensation
 * fails, return its first error with *out=NULL and record both error codes in
 * turbo_flow_last_error(flow); otherwise return the original failure.
 * Hosts serialize ALL calls on the flow (discovery, control, resource commands,
 * lifecycle). Callbacks must not reenter those operations or mutate registries.
 */
TURBO_FLOW_C_API int
turbo_flow_discovery_controller_create(const turbo_flow_discovery_controller_config_t *config,
                                       turbo_flow_discovery_controller_t **out);

/**
 * Release copied controller metadata without issuing owner commands.
 * Destroy before resetting/destroying the borrowed flow; serialize with all flow calls.
 */
TURBO_FLOW_C_API void
turbo_flow_discovery_controller_destroy(turbo_flow_discovery_controller_t *controller);

/**
 * Replace the complete desired peer set.
 *
 * Versions must increase. Replaying an identical current version is an idempotent
 * success; conflicting data at the same version returns SALTS_EPROTO and older
 * versions return SALTS_EALREADY. All input is validated and copied before owner
 * commands begin. Reserve 3*A+2*U+2*R history entries for actual added, updated,
 * and removed slots, including compensation; SALTS_ENOSPC/ENOMEM rejects before
 * owner effects. Unchanged sets consume no history. Each command reads the current
 * generation of the captured UID; a missing UID is never rebound by owner name.
 * A failed command compensates applied slots and preserves committed peers/version.
 * Newly added slots become inactive on rollback; their uncommitted endpoint is
 * not restored. Failed compensation is exposed in rollback_status, returned to
 * the caller, and makes subsequent replace/poll calls return SALTS_EINVAL.
 * Requires STARTED; hosts serialize all flow operations, and callbacks cannot reenter.
 */
TURBO_FLOW_C_API int
turbo_flow_discovery_replace_peer_list(turbo_flow_discovery_controller_t *controller,
                                       const turbo_flow_discovery_peer_list_t *peer_list,
                                       turbo_flow_discovery_replace_result_t *result);

/**
 * Fetch one registry snapshot and reconcile it; fetch failure leaves adapters unchanged.
 * Requires a consistent controller and STARTED flow, checked before fetch.
 * Hosts serialize all flow operations; callbacks cannot reenter or change lifecycle.
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
