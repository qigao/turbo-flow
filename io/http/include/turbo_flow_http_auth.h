#ifndef TURBO_FLOW_HTTP_AUTH_H
#define TURBO_FLOW_HTTP_AUTH_H

#include "turbo_flow_config.h"
#include "turbo_flow_http_types.h"
#include "turbo_flow_security.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_HTTP_AUTH_API_VERSION_V1 1u
#define TURBO_FLOW_HTTP_AUTH_API_VERSION_V2 2u
#define TURBO_FLOW_HTTP_AUTH_API_VERSION_V3 3u
#define TURBO_FLOW_HTTP_AUTH_API_VERSION TURBO_FLOW_HTTP_AUTH_API_VERSION_V3
#define TURBO_FLOW_HTTP_AUTH_BACKEND "https"
#define TURBO_FLOW_HTTP_AUTH_DEFAULT_TIMEOUT_MS 3000u
#define TURBO_FLOW_HTTP_AUTH_MAX_TIMEOUT_MS 30000u
#define TURBO_FLOW_HTTP_AUTH_DEFAULT_MAX_SECRET_SIZE 4096u
#define TURBO_FLOW_HTTP_AUTH_RESPONSE_LIMIT 16384u
#define TURBO_FLOW_HTTP_AUTH_TOKEN_LIMIT 4096u

typedef struct turbo_flow_http_auth_provider_s turbo_flow_http_auth_provider_t;

/**
 * HTTPS authentication-service provider configuration.
 *
 * The service token is resolved through key_provider for every request so rotation does not
 * require rebuilding the graph. The provider never accepts database connection configuration.
 */
typedef struct turbo_flow_http_auth_provider_config_s {
  size_t size;
  uint32_t api_version;
  const char *url;
  const char *method;
  const char *service_id;
  const char *service_domain;
  const char *service_token_ref;
  uint32_t timeout_ms;
  size_t max_secret_size;
  turbo_flow_security_key_provider_t key_provider;
  turbo_flow_http_tls_client_config_t tls;
} turbo_flow_http_auth_provider_config_t;

#define TURBO_FLOW_HTTP_AUTH_PROVIDER_CONFIG_INIT                                                  \
  {sizeof(turbo_flow_http_auth_provider_config_t),                                                 \
   TURBO_FLOW_HTTP_AUTH_API_VERSION,                                                               \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_HTTP_AUTH_DEFAULT_TIMEOUT_MS,                                                        \
   TURBO_FLOW_HTTP_AUTH_DEFAULT_MAX_SECRET_SIZE,                                                   \
   TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT,                                                          \
   TURBO_FLOW_HTTP_TLS_CLIENT_CONFIG_INIT}

/**
 * Create an HTTPS-only provider. Configuration strings are copied.
 *
 * Authentication must be invoked from a running CoroNet coroutine. Network I/O yields to the
 * owner event loop; calling it outside a coroutine fails with TURBO_ENOTSUP.
 */
CXX_C_API int
turbo_flow_http_auth_provider_create(const turbo_flow_http_auth_provider_config_t *config,
                                     turbo_flow_http_auth_provider_t **out);

/** Create from one strict `kind: auth_provider`, `backend: https` channel. */
CXX_C_API int turbo_flow_http_auth_provider_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider, turbo_flow_http_auth_provider_t **out,
    turbo_flow_config_error_t *error);

/** Borrowed interface valid until provider destruction. */
CXX_C_API const turbo_flow_security_auth_provider_t *
turbo_flow_http_auth_provider_interface(const turbo_flow_http_auth_provider_t *provider);

/** One-step MQTT enhanced authentication; Authentication Data is the HTTPS credential. */
CXX_C_API const turbo_flow_security_enhanced_auth_provider_t *
turbo_flow_http_enhanced_auth_provider_interface(
    const turbo_flow_http_auth_provider_t *provider);

/** Borrowed configured authentication method. */
CXX_C_API const char *
turbo_flow_http_auth_provider_method(const turbo_flow_http_auth_provider_t *provider);

CXX_C_API void turbo_flow_http_auth_provider_destroy(turbo_flow_http_auth_provider_t *provider);

/** Factory registered by a product composition root under the exact `https` backend. */
CXX_C_API const turbo_flow_security_auth_provider_factory_t *
turbo_flow_http_auth_provider_factory(void);

#ifdef __cplusplus
}
#endif

#endif
