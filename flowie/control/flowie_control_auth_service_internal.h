#ifndef FLOWIE_CONTROL_AUTH_SERVICE_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_SERVICE_INTERNAL_H

#include "flowie_control_auth_cache_internal.h"
#include "flowie_control_principal_cache_internal.h"
#include "flowie_control_auth_rate_limiter_internal.h"
#include "flowie_control_security_limits_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_auth_service_s flowie_control_auth_service_t;
typedef uint64_t (*flowie_control_auth_clock_fn)(void *ctx);

typedef struct flowie_control_auth_root_binding_s {
  size_t size;
  const char *listener_id;
  /** Canonical lowercase `sha256:` followed by exactly 64 hexadecimal digits. */
  const char *peer_certificate_sha256;
  const char *root_group_id;
} flowie_control_auth_root_binding_t;

#define FLOWIE_CONTROL_AUTH_ROOT_BINDING_INIT                                                      \
  {sizeof(flowie_control_auth_root_binding_t), NULL, NULL, NULL}

typedef int (*flowie_control_auth_policy_version_fn)(void *ctx, const char *root_group_id,
                                                     uint64_t *policy_version_out);

typedef struct flowie_control_auth_policy_version_provider_s {
  size_t size;
  void *ctx;
  flowie_control_auth_policy_version_fn current;
} flowie_control_auth_policy_version_provider_t;

#define FLOWIE_CONTROL_AUTH_POLICY_VERSION_PROVIDER_INIT                                           \
  {sizeof(flowie_control_auth_policy_version_provider_t), NULL, NULL}

typedef struct flowie_control_auth_service_config_s {
  size_t size;
  flowie_control_store_t *store;
  const flowie_control_auth_root_binding_t *bindings;
  size_t binding_count;
  const char *method;
  uint64_t principal_ttl_seconds;
  flowie_control_auth_cache_config_t credential_cache;
  /* The same bounded TTL/capacity policy is used for derived principal snapshots. */
  flowie_control_auth_cache_config_t principal_cache;
  flowie_control_auth_rate_limiter_config_t rate_limiter;
  flowie_control_auth_policy_version_provider_t policy_version;
  flowie_control_auth_clock_fn clock_seconds;
  void *clock_ctx;
} flowie_control_auth_service_config_t;

#define FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT                                                    \
  {sizeof(flowie_control_auth_service_config_t),                                                   \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   "password",                                                                                     \
   FLOWIE_CONTROL_AUTH_DEFAULT_PRINCIPAL_TTL_SECONDS,                                              \
   FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT,                                                          \
   FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT,                                                          \
   FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT,                                                   \
   FLOWIE_CONTROL_AUTH_POLICY_VERSION_PROVIDER_INIT,                                               \
   NULL,                                                                                           \
   NULL}

/**
 * Transport-owned, request-local caller identity.
 *
 * Only the TLS listener adapter may construct this value, after certificate-chain verification.
 * HTTP headers and request JSON are never valid sources for any field below.
 */
typedef struct flowie_control_verified_caller_s {
  size_t size;
  const char *listener_id;
  const char *peer_certificate_sha256;
  int certificate_verified;
} flowie_control_verified_caller_t;

#define FLOWIE_CONTROL_VERIFIED_CALLER_INIT                                                        \
  {sizeof(flowie_control_verified_caller_t), NULL, NULL, 0}

typedef struct flowie_control_authenticate_request_s {
  size_t size;
  const flowie_control_verified_caller_t *caller;
  const char *identity;
  const char *method;
  const uint8_t *secret;
  size_t secret_size;
} flowie_control_authenticate_request_t;

#define FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT                                                   \
  {sizeof(flowie_control_authenticate_request_t), NULL, NULL, NULL, NULL, 0u}

/**
 * Create an immutable, thread-safe authentication service core.
 *
 * The service owns copied bindings and its credential cache. It borrows store, policy provider,
 * and clock callback state until destroy. Destroy requires no concurrent authenticate calls.
 */
int flowie_control_auth_service_create(const flowie_control_auth_service_config_t *config,
                                       flowie_control_auth_service_t **out);
void flowie_control_auth_service_destroy(flowie_control_auth_service_t *service);

/**
 * Authenticate one request inside the Root Group selected only by verified transport identity.
 * Unknown callers and credential failures return TURBO_EPERM without cross-Root probing.
 */
int flowie_control_auth_service_authenticate(flowie_control_auth_service_t *service,
                                             const flowie_control_authenticate_request_t *request,
                                             turbo_flow_security_principal_t *principal_out,
                                             int *credential_cache_hit_out);

#ifdef __cplusplus
}
#endif

#endif
