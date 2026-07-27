#ifndef FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_INTERNAL_H
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_INTERNAL_H

#include "flowie_control_external_authenticator_internal.h"

#include "turbo_flow_security.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_EXTERNAL_HTTPS_PROTOCOL_VERSION 2u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_URL_MAX 2047u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_TIMEOUT_MS 3000u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_TIMEOUT_MS 30000u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_RESPONSE_SIZE 16384u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_RESPONSE_SIZE 65536u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_MAX_IN_FLIGHT 64u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_MAX_IN_FLIGHT 1024u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_TOKEN_MAX 4096u
#define FLOWIE_CONTROL_EXTERNAL_HTTPS_TLS_PATH_MAX 4096u

typedef struct flowie_control_external_https_tls_config_s {
  const char *ca_file;
  const char *client_cert_file;
  const char *client_key_file;
  const char *client_key_password_ref;
} flowie_control_external_https_tls_config_t;

#define FLOWIE_CONTROL_EXTERNAL_HTTPS_TLS_CONFIG_INIT {NULL, NULL, NULL, NULL}

typedef struct flowie_control_external_https_authenticator_s
    flowie_control_external_https_authenticator_t;

typedef struct flowie_control_external_https_authenticator_stats_s {
  size_t size;
  uint64_t started_requests;
  uint64_t in_flight;
  uint64_t succeeded;
  uint64_t denied;
  uint64_t local_overload;
  uint64_t remote_overload;
  uint64_t remote_server_failures;
  uint64_t transport_failures;
  uint64_t protocol_failures;
  uint64_t local_failures;
} flowie_control_external_https_authenticator_stats_t;

#define FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT                                     \
  {sizeof(flowie_control_external_https_authenticator_stats_t),                                    \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

typedef struct flowie_control_external_https_authenticator_config_s {
  size_t size;
  const char *url;
  const char *method;
  const char *service_token_ref;
  uint32_t timeout_ms;
  size_t max_response_size;
  uint32_t max_in_flight;
  turbo_flow_security_key_provider_t key_provider;
  flowie_control_external_https_tls_config_t tls;
} flowie_control_external_https_authenticator_config_t;

#define FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_CONFIG_INIT                                    \
  {sizeof(flowie_control_external_https_authenticator_config_t),                                   \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_TIMEOUT_MS,                                               \
   FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_RESPONSE_SIZE,                                            \
   FLOWIE_CONTROL_EXTERNAL_HTTPS_DEFAULT_MAX_IN_FLIGHT,                                            \
   TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT,                                                          \
   FLOWIE_CONTROL_EXTERNAL_HTTPS_TLS_CONFIG_INIT}

/**
 * Create an immutable HTTPS authenticator.
 *
 * Configuration strings are copied. key_provider callback state is borrowed until destroy.
 * verify() must run inside a CoroNet coroutine; each request owns an independent HTTP client.
 * Concurrent verify() calls are bounded by max_in_flight. Destroy requires all calls to finish.
 */
int flowie_control_external_https_authenticator_create(
    const flowie_control_external_https_authenticator_config_t *config,
    flowie_control_external_https_authenticator_t **out);
void flowie_control_external_https_authenticator_destroy(
    flowie_control_external_https_authenticator_t *authenticator);

/** Borrowed descriptor valid until authenticator destruction. */
const flowie_control_external_authenticator_t *
flowie_control_external_https_authenticator_interface(
    const flowie_control_external_https_authenticator_t *authenticator);

/**
 * Copy a lock-free diagnostic snapshot without credentials or identity data.
 *
 * Counters saturate at UINT64_MAX. Concurrent snapshots may observe fields from slightly
 * different instants; each completed valid verify() attempt is assigned to exactly one outcome
 * counter. Contract errors rejected before coroutine execution are not counted.
 */
int flowie_control_external_https_authenticator_get_stats(
    const flowie_control_external_https_authenticator_t *authenticator,
    flowie_control_external_https_authenticator_stats_t *stats_out);

/** Protocol helpers exposed only for contract tests and alternate HTTP composition roots. */
int flowie_control_external_https_encode_request(
    const flowie_control_external_auth_request_t *request, char **body_out, size_t *body_size_out);
int flowie_control_external_https_decode_response(
    const char *body, size_t body_size, const char *method,
    flowie_control_external_auth_assertion_t *assertion_out);

#ifdef __cplusplus
}
#endif

#endif
