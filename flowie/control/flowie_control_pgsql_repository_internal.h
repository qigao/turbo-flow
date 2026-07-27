#ifndef FLOWIE_CONTROL_PGSQL_REPOSITORY_INTERNAL_H
#define FLOWIE_CONTROL_PGSQL_REPOSITORY_INTERNAL_H

#include "flowie_control_pgsql_database_internal.h"
#include "flowie_control_repository_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_pgsql_repository_provider_s
    flowie_control_pgsql_repository_provider_t;

/**
 * Create one PostgreSQL-backed control repository.
 *
 * The provider owns its pool and immutable command/query views. The returned repository pointer
 * remains valid until provider destruction and supports concurrent calls through bounded,
 * exclusive pool leases.
 */
int flowie_control_pgsql_repository_create(const flowie_control_pgsql_pool_config_t *config,
                                           flowie_control_pgsql_repository_provider_t **out);

const flowie_control_repository_t *
flowie_control_pgsql_repository_view(const flowie_control_pgsql_repository_provider_t *provider);

/**
 * Stop new leases, wait for in-flight repository calls, and release the provider.
 *
 * Callers must stop issuing new operations before destruction. On timeout the provider remains
 * allocated and may be passed to this function again; its pool stays closed to new work.
 */
int flowie_control_pgsql_repository_destroy(flowie_control_pgsql_repository_provider_t *provider,
                                            int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif
