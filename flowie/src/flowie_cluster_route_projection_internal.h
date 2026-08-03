#ifndef FLOWIE_CLUSTER_ROUTE_PROJECTION_INTERNAL_H
#define FLOWIE_CLUSTER_ROUTE_PROJECTION_INTERNAL_H

#include "flowie_cluster_route_store_internal.h"
#include "flowie_cluster_session_bind_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_ROUTE_PROJECTION_ABI_V1 1u

typedef int (*flowie_cluster_route_fact_get_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_key_kind_t key_kind, const uint8_t *key, size_t key_size,
    flowie_cluster_pgsql_fact_record_t *out);

/** Resolve one exact node+boot membership row into copied endpoint data. */
typedef int (*flowie_cluster_route_member_resolve_fn)(
    void *ctx, tstr_v node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    flowie_cluster_pgsql_member_t *out);

typedef struct flowie_cluster_route_projector_config_s {
  size_t size;
  uint32_t abi_version;
  const flowie_cluster_session_bind_config_t *session;
  flowie_cluster_route_store_t *route_store;
  flowie_cluster_route_fact_get_fn fact_get;
  void *fact_get_ctx;
  flowie_cluster_route_member_resolve_fn member_resolve;
  void *member_resolve_ctx;
} flowie_cluster_route_projector_config_t;

#define FLOWIE_CLUSTER_ROUTE_PROJECTOR_CONFIG_INIT                                                 \
  {sizeof(flowie_cluster_route_projector_config_t), FLOWIE_CLUSTER_ROUTE_PROJECTION_ABI_V1,       \
   NULL, NULL, NULL, NULL, NULL, NULL}

typedef struct flowie_cluster_route_projector_s flowie_cluster_route_projector_t;

int flowie_cluster_route_projector_create(
    const flowie_cluster_route_projector_config_t *config,
    flowie_cluster_route_projector_t **out);
void flowie_cluster_route_projector_destroy(flowie_cluster_route_projector_t *projector);

/** Dispatcher-compatible hook. Success means the event may proceed to PG settlement. */
int flowie_cluster_route_projector_project(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event);

/**
 * Refresh member-derived endpoint and lease fields for every active route in one bounded snapshot.
 * Missing or non-routable members are left to expire; backend failures abort the cycle for retry.
 */
int flowie_cluster_route_projector_reconcile(flowie_cluster_route_projector_t *projector,
                                             size_t *out_refreshed);

/** Cluster-maintenance entry point; one caller scans the shared route namespace per cycle. */
int flowie_cluster_route_reconcile(flowie_cluster_route_store_t *route_store,
                                   flowie_cluster_route_member_resolve_fn member_resolve,
                                   void *member_resolve_ctx, size_t *out_refreshed);

/** Thin adapter for the production PG fact store. */
int flowie_cluster_route_projector_pgsql_fact_get(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_key_kind_t key_kind, const uint8_t *key, size_t key_size,
    flowie_cluster_pgsql_fact_record_t *out);

int flowie_cluster_route_projector_pgsql_member_resolve(
    void *ctx, tstr_v node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    flowie_cluster_pgsql_member_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_ROUTE_PROJECTION_INTERNAL_H */
