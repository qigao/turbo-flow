#ifndef FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_INTERNAL_H

#include "flowie_control_auth_service_internal.h"
#include "iris/router.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_auth_iris_adapter_s flowie_control_auth_iris_adapter_t;

typedef struct flowie_control_auth_iris_adapter_config_s {
  size_t size;
  flowie_control_auth_service_t *service;
  const char *listener_id;
} flowie_control_auth_iris_adapter_config_t;

#define FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT                                               \
  {sizeof(flowie_control_auth_iris_adapter_config_t), NULL, NULL}

/**
 * Create a transport adapter that borrows the authentication service and copies
 * listener_id. Destroy it only after all request handlers using it have stopped.
 */
int flowie_control_auth_iris_adapter_create(
    const flowie_control_auth_iris_adapter_config_t *config,
    flowie_control_auth_iris_adapter_t **out);
void flowie_control_auth_iris_adapter_destroy(flowie_control_auth_iris_adapter_t *adapter);

/**
 * Authenticate decoded request fields using only Iris/CoroNet's verified mTLS
 * peer identity. HTTP headers and body fields cannot supply or override caller
 * identity. Transport identity failures are normalized to TURBO_EPERM.
 */
int flowie_control_auth_iris_adapter_authenticate(
    flowie_control_auth_iris_adapter_t *adapter, const Req *http_request,
    const char *identity, const char *method, const uint8_t *secret,
    size_t secret_size, turbo_flow_security_principal_t *principal_out,
    int *credential_cache_hit_out);

#ifdef __cplusplus
}
#endif

#endif
