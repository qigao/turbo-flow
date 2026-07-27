#ifndef FLOWIE_CONTROL_ACL_IRIS_ENDPOINT_INTERNAL_H
#define FLOWIE_CONTROL_ACL_IRIS_ENDPOINT_INTERNAL_H

#include "flowie_control_auth_service_internal.h"
#include "flowie_control_repository_internal.h"
#include "iris/iris_app.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_ACL_HTTP_PATH "/v3/acl"
#define FLOWIE_CONTROL_ACL_HTTP_PROTOCOL_VERSION 3u
#define FLOWIE_CONTROL_ACL_HTTP_DEFAULT_RESPONSE_MAX (16u * 1024u * 1024u)

typedef struct flowie_control_acl_iris_endpoint_s flowie_control_acl_iris_endpoint_t;

typedef struct flowie_control_acl_iris_endpoint_config_s {
  size_t size;
  const flowie_control_repository_t *repository;
  const flowie_control_auth_service_t *auth_service;
  const char *listener_id;
  const char *service_token_ref;
  turbo_flow_security_key_provider_t key_provider;
  size_t max_response_size;
} flowie_control_acl_iris_endpoint_config_t;

#define FLOWIE_CONTROL_ACL_IRIS_ENDPOINT_CONFIG_INIT                                               \
  {sizeof(flowie_control_acl_iris_endpoint_config_t),                                              \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT,                                                          \
   FLOWIE_CONTROL_ACL_HTTP_DEFAULT_RESPONSE_MAX}

int flowie_control_acl_iris_endpoint_create(const flowie_control_acl_iris_endpoint_config_t *config,
                                            flowie_control_acl_iris_endpoint_t **out);
void flowie_control_acl_iris_endpoint_destroy(flowie_control_acl_iris_endpoint_t *endpoint);
int flowie_control_acl_iris_endpoint_register(flowie_control_acl_iris_endpoint_t *endpoint,
                                              iris_app_t *app);
int flowie_control_acl_iris_endpoint_process(flowie_control_acl_iris_endpoint_t *endpoint, Req *req,
                                             int *status_out, char **body_out,
                                             size_t *body_size_out);

#ifdef __cplusplus
}
#endif

#endif
