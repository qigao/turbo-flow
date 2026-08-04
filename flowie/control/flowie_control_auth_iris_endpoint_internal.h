#ifndef FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_INTERNAL_H

#include "flowie_control_auth_iris_adapter_internal.h"
#include "flowie_control_service_credential_internal.h"
#include "iris/iris_app.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_AUTH_HTTP_PATH "/v4/authenticate"
#define FLOWIE_CONTROL_AUTH_HTTP_PROTOCOL_VERSION 3u
#define FLOWIE_CONTROL_AUTH_HTTP_DEFAULT_REQUEST_BODY_MAX 8192u
#define FLOWIE_CONTROL_AUTH_HTTP_ABSOLUTE_REQUEST_BODY_MAX 16384u
#define FLOWIE_CONTROL_AUTH_HTTP_REMOTE_ADDRESS_MAX FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX
#define FLOWIE_CONTROL_AUTH_HTTP_TOKEN_REFERENCE_MAX 1024u
#define FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_DEFAULT_WORKERS 4u
#define FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_MAX_WORKERS 64u
#define FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_DEFAULT_QUEUE_CAPACITY 128u
#define FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_MAX_QUEUE_CAPACITY 4096u
#define FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_DEFAULT_DEADLINE_MS 10000u
#define FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_MAX_DEADLINE_MS 60000u

typedef struct flowie_control_auth_iris_endpoint_s flowie_control_auth_iris_endpoint_t;

typedef struct flowie_control_auth_iris_endpoint_config_s {
  size_t size;
  flowie_control_auth_iris_adapter_t *adapter;
  flowie_control_service_credential_resolver_t *service_credentials;
  size_t max_request_body_size;
  size_t max_secret_size;
  int local_executor_enabled;
  uint32_t local_executor_workers;
  size_t local_executor_queue_capacity;
  uint32_t local_executor_deadline_ms;
} flowie_control_auth_iris_endpoint_config_t;

#define FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT                                              \
  {sizeof(flowie_control_auth_iris_endpoint_config_t),                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   FLOWIE_CONTROL_AUTH_HTTP_DEFAULT_REQUEST_BODY_MAX,                                              \
   FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX,                                                           \
   0,                                                                                              \
   FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_DEFAULT_WORKERS,                                             \
   FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_DEFAULT_QUEUE_CAPACITY,                                      \
   FLOWIE_CONTROL_AUTH_LOCAL_EXECUTOR_DEFAULT_DEADLINE_MS}

typedef struct flowie_control_auth_http_request_s {
  char identity[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char protocol[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char remote_address[FLOWIE_CONTROL_AUTH_HTTP_REMOTE_ADDRESS_MAX + 1u];
  char peer_certificate_sha256[FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u];
  uint8_t secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX];
  size_t secret_size;
} flowie_control_auth_http_request_t;

/**
 * Create one bounded HTTPS endpoint. The endpoint borrows the adapter and immutable
 * service-credential resolver. Stop request handling before destroy.
 */
int flowie_control_auth_iris_endpoint_create(
    const flowie_control_auth_iris_endpoint_config_t *config,
    flowie_control_auth_iris_endpoint_t **out);
void flowie_control_auth_iris_endpoint_destroy(flowie_control_auth_iris_endpoint_t *endpoint);

/**
 * Bind exactly POST /v4/authenticate on one Iris app. The caller keeps ownership
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

/**
 * Execute one decoded request using an already verified transport identity.
 * With the local executor enabled this function must run inside a CoroNet
 * coroutine. A deadline only abandons the response; accepted synchronous work
 * remains owned by the executor and is drained during endpoint destruction.
 */
int flowie_control_auth_iris_endpoint_authenticate_verified(
    flowie_control_auth_iris_endpoint_t *endpoint, const flowie_control_verified_caller_t *caller,
    const flowie_control_auth_http_request_t *request,
    turbo_flow_security_principal_t *principal_out);

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
