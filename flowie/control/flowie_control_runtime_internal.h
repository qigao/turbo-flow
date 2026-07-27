#ifndef FLOWIE_CONTROL_RUNTIME_INTERNAL_H
#define FLOWIE_CONTROL_RUNTIME_INTERNAL_H

#include "flowie_control_config_internal.h"
#include "flowie_control_management_service_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER "viewer"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN "user_admin"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_POLICY_ADMIN "policy_admin"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN "security_admin"

typedef struct flowie_control_runtime_s flowie_control_runtime_t;

/** Validate TLS identity, secret references, and store selection without opening a listener/DB. */
int flowie_control_runtime_validate(const flowie_control_config_t *config);

/** Create the complete controller composition root and bind all enabled routes. */
int flowie_control_runtime_create(const flowie_control_config_t *config,
                                  flowie_control_runtime_t **out);

/** Run the configured HTTPS/mTLS listener until Iris receives a shutdown signal. */
int flowie_control_runtime_run(flowie_control_runtime_t *runtime);

/**
 * Stop request handling and destroy the selected repository.
 *
 * PostgreSQL returns a close error without freeing the runtime so the caller can retry after
 * outstanding leases are returned.
 */
int flowie_control_runtime_destroy(flowie_control_runtime_t *runtime);

/** Resolve an exact configured certificate fingerprint against current repository state. */
int flowie_control_runtime_resolve_management_fingerprint(
    flowie_control_runtime_t *runtime, const char *fingerprint,
    flowie_control_management_caller_t *caller_out);

/** Deterministic identity boundary used by the runtime callback and focused tests. */
int flowie_control_management_identity_resolve(
    const flowie_control_repository_t *repository,
    const flowie_control_config_admin_binding_t *bindings, size_t binding_count,
    const char *fingerprint, flowie_control_management_caller_t *caller_out);

#ifdef __cplusplus
}
#endif

#endif
