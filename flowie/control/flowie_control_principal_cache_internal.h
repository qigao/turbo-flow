#ifndef FLOWIE_CONTROL_PRINCIPAL_CACHE_INTERNAL_H
#define FLOWIE_CONTROL_PRINCIPAL_CACHE_INTERNAL_H

#include "flowie_control_auth_cache_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_principal_cache_s flowie_control_principal_cache_t;

/** Create a bounded, thread-safe cache of immutable principal snapshots. */
int flowie_control_principal_cache_create(const flowie_control_auth_cache_config_t *config,
                                          flowie_control_principal_cache_t **out);

void flowie_control_principal_cache_destroy(flowie_control_principal_cache_t *cache);

/**
 * Look up a snapshot only when every fact-source revision still matches.
 *
 * A miss, expiry, or revision mismatch returns TURBO_ENOENT and never exposes stale data.
 */
int flowie_control_principal_cache_get(
    flowie_control_principal_cache_t *cache, const char *domain_id,
    const char *principal_id, uint64_t user_revision, uint64_t credential_revision,
    uint64_t store_revision, uint64_t policy_version, flowie_control_principal_snapshot_t *out,
    int *cache_hit_out);

/** Store one caller-owned immutable snapshot and its fact-source revisions. */
int flowie_control_principal_cache_put(flowie_control_principal_cache_t *cache,
                                       const flowie_control_principal_snapshot_t *snapshot,
                                       uint64_t store_revision, uint64_t policy_version);

size_t flowie_control_principal_cache_size(flowie_control_principal_cache_t *cache);

#ifdef __cplusplus
}
#endif

#endif
