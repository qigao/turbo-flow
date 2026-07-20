#ifndef FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_INTERNAL_H

#include "flowie_control_auth_iris_adapter_internal.h"
#include "iris/iris_app.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_AUTH_HTTP_PATH "/v2/authenticate"
#define FLOWIE_CONTROL_AUTH_HTTP_PROTOCOL_VERSION 2u
#define FLOWIE_CONTROL_AUTH_HTTP_DEFAULT_REQUEST_BODY_MAX 8192u
#define FLOWIE_CONTROL_AUTH_HTTP_ABSOLUTE_REQUEST_BODY_MAX 16384u
#define FLOWIE_CONTROL_AUTH_HTTP_REMOTE_ADDRESS_MAX 255u
#define FLOWIE_CONTROL_AUTH_HTTP_TOKEN_REFERENCE_MAX 1024u

typedef struct flowie_control_auth_iris_endpoint_s flowie_control_auth_iris_endpoint_t;

typedef struct flowie_control_auth_iris_endpoint_config_s {
  size_t size;
  flowie_control_auth_iris_adapter_t *adapter;
  const char *service_token_ref;
  turbo_flow_security_key_provider_t key_provider;
  size_t max_request_body_size;
  size_t max_secret_size;
} flowie_control_auth_iris_endpoint_config_t;

#define FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT                                             \
  {sizeof(flowie_control_auth_iris_endpoint_config_t),                                            \
   NULL,                                                                                          \
   NULL,                                                                                          \
   TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT,                                                         \
   FLOWIE_CONTROL_AUTH_HTTP_DEFAULT_REQUEST_BODY_MAX,                                             \
   FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX}

typedef struct flowie_control_auth_http_request_s {
  char identity[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char protocol[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char remote_address[FLOWIE_CONTROL_AUTH_HTTP_REMOTE_ADDRESS_MAX + 1u];
  uint8_t secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX];
  size_t secret_size;
} flowie_control_auth_http_request_t;

/**
 * Create one bounded HTTPS endpoint. The endpoint borrows adapter and key-provider
 * callback state, and copies service_token_ref. Stop request handling before destroy.
 */
int flowie_control_auth_iris_endpoint_create(
    const flowie_control_auth_iris_endpoint_config_t *config,
    flowie_control_auth_iris_endpoint_t **out);
void flowie_control_auth_iris_endpoint_destroy(flowie_control_auth_iris_endpoint_t *endpoint);

/**
 * Bind exactly POST /v2/authenticate on one Iris app. The caller keeps ownership
 * of both objects and must stop the app before destroying the endpoint.
 */
int flowie_control_auth_iris_endpoint_register(flowie_control_auth_iris_endpoint_t *endpoint,
                                               iris_app_t *app);

/** Direct handler form for composition roots that register routes themselves. */
void flowie_control_auth_iris_endpoint_handle(flowie_control_auth_iris_endpoint_t *endpoint,
                                              Req *req, Res *res);

/**
 * Produce one owned JSON response without sending it. The caller releases
 * body_out with turbo_json_serialize_free(). This seam is also used by tests.
 */
int flowie_control_auth_iris_endpoint_process(flowie_control_auth_iris_endpoint_t *endpoint,
                                              Req *req, int *status_out, char **body_out,
                                              size_t *body_size_out);

int flowie_control_auth_http_decode_request(const char *body, size_t body_size,
                                            size_t max_secret_size,
                                            flowie_control_auth_http_request_t *request_out);
void flowie_control_auth_http_request_clear(flowie_control_auth_http_request_t *request);
int flowie_control_auth_http_encode_principal(const turbo_flow_security_principal_t *principal,
                                              char **body_out, size_t *body_size_out);

#ifdef __cplusplus
}
#endif

#endif
