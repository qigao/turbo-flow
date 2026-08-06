#ifndef FLOWIE_CONTROL_SERVICE_CREDENTIAL_INTERNAL_H
#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_INTERNAL_H

#include "flowie_control_auth_service_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_TOKEN_MAX FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX
#define FLOWIE_CONTROL_SERVICE_ROLE_AUTH_CLIENT "flowie_auth_client"
#define FLOWIE_CONTROL_SERVICE_ROLE_ACL_CLIENT "flowie_acl_client"

typedef struct flowie_control_service_credential_resolver_s
    flowie_control_service_credential_resolver_t;

typedef struct flowie_control_service_credential_config_s {
  size_t size;
  const char *listener_id;
  const flowie_control_repository_t *repository;
} flowie_control_service_credential_config_t;

#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT                                             \
  {sizeof(flowie_control_service_credential_config_t), NULL, NULL}

int flowie_control_service_credential_resolver_create(
    const flowie_control_service_credential_config_t *config,
    flowie_control_service_credential_resolver_t **out);
void flowie_control_service_credential_resolver_destroy(
    flowie_control_service_credential_resolver_t *resolver);

/**
 * Verify one repository-backed service principal and derive its endpoint permissions.
 * service_id and service_domain are public selectors; token is the only secret.
 */
int flowie_control_service_credential_resolve(
    flowie_control_service_credential_resolver_t *resolver, const char *service_domain,
    const char *service_id, const uint8_t *token, size_t token_size,
    uint32_t required_permission, flowie_control_verified_caller_t *caller_out);

#ifdef __cplusplus
}
#endif

#endif
