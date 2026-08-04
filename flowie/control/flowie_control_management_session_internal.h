#ifndef FLOWIE_CONTROL_MANAGEMENT_SESSION_INTERNAL_H
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_INTERNAL_H

#include "flowie_control_auth_service_internal.h"
#include "flowie_control_management_service_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE 64u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_CSRF_SIZE 64u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE "flowie_session"

typedef struct flowie_control_management_session_store_s
    flowie_control_management_session_store_t;
typedef uint64_t (*flowie_control_management_session_clock_fn)(void *ctx);

typedef struct flowie_control_management_session_identity_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  uint32_t permissions;
  char csrf[FLOWIE_CONTROL_MANAGEMENT_SESSION_CSRF_SIZE + 1u];
} flowie_control_management_session_identity_t;

#define FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT                                            \
  {sizeof(flowie_control_management_session_identity_t), {0}, {0}, 0u, {0}}

typedef struct flowie_control_management_session_config_s {
  size_t size;
  const flowie_control_repository_t *repository;
  flowie_control_auth_service_t *auth_service;
  const char *method;
  size_t capacity;
  size_t max_sessions_per_principal;
  uint64_t ttl_seconds;
  flowie_control_management_session_clock_fn clock;
  void *clock_ctx;
} flowie_control_management_session_config_t;

#define FLOWIE_CONTROL_MANAGEMENT_SESSION_CONFIG_INIT                                              \
  {sizeof(flowie_control_management_session_config_t), NULL, NULL, "password", 1024u, 5u, 3600u,  \
   NULL, NULL}

int flowie_control_management_session_store_create(
    const flowie_control_management_session_config_t *config,
    flowie_control_management_session_store_t **out);
void flowie_control_management_session_store_destroy(
    flowie_control_management_session_store_t *store);

/**
 * Authenticate one local or external account and issue an opaque bearer token.
 *
 * All input strings are borrowed for this call. token_out is always cleared on failure.
 */
int flowie_control_management_session_login(
    flowie_control_management_session_store_t *store, const char *domain_id,
    const char *presented_identity, const uint8_t *secret, size_t secret_size,
    const char *remote_address,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]);

/**
 * Resolve one opaque token and re-read the current local account/role state.
 *
 * The returned identity owns all strings and can be copied into request-local storage.
 */
int flowie_control_management_session_resolve(
    flowie_control_management_session_store_t *store, const char *token,
    flowie_control_management_session_identity_t *identity_out);

int flowie_control_management_session_revoke(
    flowie_control_management_session_store_t *store, const char *token);

#ifdef __cplusplus
}
#endif

#endif
