#ifndef FLOWIE_CONTROL_SERVICE_CREDENTIAL_INTERNAL_H
#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_INTERNAL_H

#include "flowie_control_auth_service_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_TOKEN_MAX 4096u
#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_REFERENCE_MAX 1024u

typedef struct flowie_control_service_credential_resolver_s
    flowie_control_service_credential_resolver_t;

typedef struct flowie_control_service_credential_binding_s {
  size_t size;
  const char *service_id;
  const char *token_ref;
  const char *root_group_id;
  /**
   * Optional second factor. When set, the HTTPS listener must require and verify client
   * certificates, and the request certificate must match this canonical SHA-256 fingerprint.
   */
  const char *peer_certificate_sha256;
} flowie_control_service_credential_binding_t;

#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_BINDING_INIT                                            \
  {sizeof(flowie_control_service_credential_binding_t), NULL, NULL, NULL, NULL}

typedef struct flowie_control_service_credential_config_s {
  size_t size;
  const char *listener_id;
  const flowie_control_service_credential_binding_t *bindings;
  size_t binding_count;
  turbo_flow_security_key_provider_t key_provider;
} flowie_control_service_credential_config_t;

#define FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT                                             \
  {sizeof(flowie_control_service_credential_config_t), NULL, NULL, 0u,                            \
   TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT}

/**
 * Create an immutable service-credential resolver.
 *
 * Binding identifiers and references are copied. Secret values remain owned by the key provider,
 * are validated at startup, and are acquired again for every request so env/provider rotation is
 * observed without duplicating secret state.
 */
int flowie_control_service_credential_resolver_create(
    const flowie_control_service_credential_config_t *config,
    flowie_control_service_credential_resolver_t **out);
void flowie_control_service_credential_resolver_destroy(
    flowie_control_service_credential_resolver_t *resolver);

/**
 * Resolve a bearer token to one scoped Broker service.
 *
 * The returned caller borrows immutable strings from resolver and is valid until resolver destroy.
 * If more than one current secret matches, resolution fails closed.
 */
int flowie_control_service_credential_resolve(
    flowie_control_service_credential_resolver_t *resolver, const uint8_t *token,
    size_t token_size, const char *verified_peer_certificate_sha256,
    flowie_control_verified_caller_t *caller_out);

#ifdef __cplusplus
}
#endif

#endif
