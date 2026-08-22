#ifndef TURBO_FLOW_HTTP_ACL_H
#define TURBO_FLOW_HTTP_ACL_H

#include "turbo_flow_http_types.h"
#include "turbo_flow_security.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_HTTP_ACL_API_VERSION_V1 1u
#define TURBO_FLOW_HTTP_ACL_API_VERSION_V2 2u
#define TURBO_FLOW_HTTP_ACL_API_VERSION_V3 3u
#define TURBO_FLOW_HTTP_ACL_API_VERSION_V4 4u
#define TURBO_FLOW_HTTP_ACL_API_VERSION TURBO_FLOW_HTTP_ACL_API_VERSION_V4
#define TURBO_FLOW_HTTP_ACL_BACKEND "https"
#define TURBO_FLOW_HTTP_ACL_DEFAULT_TIMEOUT_MS 3000u
#define TURBO_FLOW_HTTP_ACL_MAX_TIMEOUT_MS 30000u
#define TURBO_FLOW_HTTP_ACL_DEFAULT_RESPONSE_LIMIT (4u * 1024u * 1024u)
#define TURBO_FLOW_HTTP_ACL_MAX_RESPONSE_LIMIT (16u * 1024u * 1024u)

typedef struct turbo_flow_http_acl_provider_s turbo_flow_http_acl_provider_t;

typedef struct turbo_flow_http_acl_provider_config_s {
  size_t size;
  uint32_t api_version;
  const char *url;
  const char *service_id;
  const char *service_domain;
  const char *service_token_ref;
  uint32_t timeout_ms;
  size_t max_response_size;
  turbo_flow_security_key_provider_t key_provider;
  turbo_flow_http_tls_client_config_t tls;
} turbo_flow_http_acl_provider_config_t;

#define TURBO_FLOW_HTTP_ACL_PROVIDER_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_http_acl_provider_config_t),                                                  \
   TURBO_FLOW_HTTP_ACL_API_VERSION,                                                                \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_HTTP_ACL_DEFAULT_TIMEOUT_MS,                                                         \
   TURBO_FLOW_HTTP_ACL_DEFAULT_RESPONSE_LIMIT,                                                     \
   TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT,                                                          \
   TURBO_FLOW_HTTP_TLS_CLIENT_CONFIG_INIT}

/** Create an HTTPS-only, coroutine-driven per-request ACL decision provider. */
TURBO_FLOW_C_API int
turbo_flow_http_acl_provider_create(const turbo_flow_http_acl_provider_config_t *config,
                                    turbo_flow_http_acl_provider_t **out);

/** Create from one strict `kind: acl_provider`, `backend: https` channel. */
TURBO_FLOW_C_API int turbo_flow_http_acl_provider_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider, turbo_flow_http_acl_provider_t **out,
    turbo_flow_config_error_t *error);

TURBO_FLOW_C_API const turbo_flow_security_authorization_provider_t *
turbo_flow_http_acl_provider_interface(const turbo_flow_http_acl_provider_t *provider);

TURBO_FLOW_C_API void turbo_flow_http_acl_provider_destroy(turbo_flow_http_acl_provider_t *provider);

TURBO_FLOW_C_API const turbo_flow_security_policy_provider_factory_t *
turbo_flow_http_acl_provider_factory(void);

#ifdef __cplusplus
}
#endif

#endif
