#ifndef FLOWIE_CONTROL_AUTH_SERVICE_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_SERVICE_INTERNAL_H

#include "flowie_control_auth_cache_internal.h"
#include "flowie_control_auth_rate_limiter_internal.h"
#include "flowie_control_external_authenticator_internal.h"
#include "flowie_control_principal_cache_internal.h"
#include "flowie_control_security_limits_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_auth_service_s flowie_control_auth_service_t;
typedef uint64_t (*flowie_control_auth_clock_fn)(void *ctx);

typedef int (*flowie_control_auth_policy_version_fn)(void *ctx, const char *domain_id,
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
  const flowie_control_repository_t *repository;
  const char *method;
  uint64_t principal_ttl_seconds;
  flowie_control_auth_cache_config_t credential_cache;
  /* The same bounded TTL/capacity policy is used for derived principal snapshots. */
  flowie_control_auth_cache_config_t principal_cache;
  flowie_control_auth_rate_limiter_config_t rate_limiter;
  flowie_control_auth_policy_version_provider_t policy_version;
  /** Optional pair. Configure both to replace local credential verification for this method. */
  const flowie_control_external_authenticator_t *external_authenticator;
  const flowie_control_external_identity_mapper_t *external_identity_mapper;
  flowie_control_auth_clock_fn clock_seconds;
  void *clock_ctx;
} flowie_control_auth_service_config_t;

#define FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT                                                    \
  {sizeof(flowie_control_auth_service_config_t),                                                   \
   NULL,                                                                                           \
   "password",                                                                                     \
   FLOWIE_CONTROL_AUTH_DEFAULT_PRINCIPAL_TTL_SECONDS,                                              \
   FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT,                                                          \
   FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT,                                                          \
   FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT,                                                   \
   FLOWIE_CONTROL_AUTH_POLICY_VERSION_PROVIDER_INIT,                                               \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * Request-local Broker service identity produced by the service-credential resolver.
 *
 * The bearer token selects service_id and domain_id. peer_certificate_sha256 is present only
 * when that credential also requires a verified client certificate.
 */
typedef struct flowie_control_verified_caller_s {
  size_t size;
  const char *listener_id;
  const char *service_id;
  const char *domain_id;
  const char *peer_certificate_sha256;
  int authenticated;
} flowie_control_verified_caller_t;

#define FLOWIE_CONTROL_VERIFIED_CALLER_INIT                                                        \
  {sizeof(flowie_control_verified_caller_t), NULL, NULL, NULL, NULL, 0}

typedef struct flowie_control_authenticate_request_s {
  size_t size;
  const flowie_control_verified_caller_t *caller;
  const char *identity;
  const char *method;
  const uint8_t *secret;
  size_t secret_size;
  const char *protocol;
  const char *remote_address;
  /** MQTT TLS/WSS client identity asserted by the trusted Broker caller. */
  const char *peer_certificate_sha256;
} flowie_control_authenticate_request_t;

#define FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT                                                   \
  {sizeof(flowie_control_authenticate_request_t), NULL, NULL, NULL, NULL, 0u, NULL, NULL, NULL}

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
 * Authenticate one request inside the Domain selected by a verified service credential.
 * Unknown callers and credential failures return TURBO_EPERM without cross-Root probing.
 */
int flowie_control_auth_service_authenticate(flowie_control_auth_service_t *service,
                                             const flowie_control_authenticate_request_t *request,
                                             turbo_flow_security_principal_t *principal_out,
                                             int *credential_cache_hit_out);

/**
 * Authenticate a human login inside an explicitly presented Domain.
 *
 * The Domain is untrusted input and never grants authority by itself: the credential or
 * external assertion must resolve to an enabled local principal in that Domain. caller_scope
 * is used only for bounded rate limiting. When require_policy is zero, login does not depend on a
 * published MQTT ACL generation.
 */
int flowie_control_auth_service_authenticate_root(
    flowie_control_auth_service_t *service, const char *domain_id, const char *caller_scope,
    const flowie_control_authenticate_request_t *request, int require_policy,
    turbo_flow_security_principal_t *principal_out, int *credential_cache_hit_out);

/**
 * Resolve a verified service caller to its scoped Domain without authenticating a user
 * credential. Used by read-only broker-facing services such as ACL bundle distribution.
 */
int flowie_control_auth_service_resolve_domain(
    const flowie_control_auth_service_t *service, const flowie_control_verified_caller_t *caller,
    char domain_id_out[TURBO_FLOW_SECURITY_ID_MAX + 1u]);

#ifdef __cplusplus
}
#endif

#endif
