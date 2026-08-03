#ifndef FLOWIE_CLUSTER_MEMBER_DIRECTORY_INTERNAL_H
#define FLOWIE_CLUSTER_MEMBER_DIRECTORY_INTERNAL_H

#include "flowie_cluster_pgsql_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_cluster_member_directory_s flowie_cluster_member_directory_t;
typedef struct flowie_cluster_member_directory_snapshot_s
    flowie_cluster_member_directory_snapshot_t;

/** Create one bounded, initially empty, thread-safe derived membership directory. */
int flowie_cluster_member_directory_create(size_t max_members,
                                           flowie_cluster_member_directory_t **out);

/**
 * Deep-copy and validate one PostgreSQL snapshot without changing the published directory.
 * The returned immutable candidate is owned by the caller until publish() consumes it.
 */
int flowie_cluster_member_directory_prepare(
    const flowie_cluster_member_directory_t *directory,
    const flowie_cluster_pgsql_membership_snapshot_t *source,
    flowie_cluster_member_directory_snapshot_t **out);

/** Atomically publish a prepared candidate and consume it on success only. */
int flowie_cluster_member_directory_publish(
    flowie_cluster_member_directory_t *directory,
    flowie_cluster_member_directory_snapshot_t *candidate);
void flowie_cluster_member_directory_snapshot_destroy(
    flowie_cluster_member_directory_snapshot_t *snapshot);

/** Route-projector-compatible exact node+boot lookup that copies its result. */
int flowie_cluster_member_directory_resolve(
    void *ctx, tstr_v node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    flowie_cluster_pgsql_member_t *out);

/** Requires all publishers and resolvers to be quiescent. */
void flowie_cluster_member_directory_destroy(flowie_cluster_member_directory_t *directory);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_MEMBER_DIRECTORY_INTERNAL_H */
