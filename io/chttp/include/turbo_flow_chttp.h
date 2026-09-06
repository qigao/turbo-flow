#ifndef TURBO_FLOW_CHTTP_H
#define TURBO_FLOW_CHTTP_H

#include "turbo_flow.h"

#include <chttp/chttp.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CHTTP_CLIENT_API_VERSION 1u
#define TURBO_FLOW_CHTTP_SERVER_API_VERSION 1u
#define TURBO_FLOW_CHTTP_DEFAULT_STOP_TIMEOUT_MS 5000u
#define TURBO_FLOW_CHTTP_SERVER_DEFAULT_MAX_REQUEST_MESSAGE_BYTES (2u * 1024u * 1024u)
#define TURBO_FLOW_CHTTP_SERVER_DEFAULT_SUCCESS_STATUS 200u
#define TURBO_FLOW_CHTTP_SERVER_DEFAULT_OVERLOAD_STATUS 429u
#define TURBO_FLOW_CHTTP_SERVER_DEFAULT_UNAVAILABLE_STATUS 503u
#define TURBO_FLOW_CHTTP_SERVER_DEFAULT_GRAPH_ERROR_STATUS 500u

typedef struct turbo_flow_chttp_client_s turbo_flow_chttp_client_t;
typedef struct turbo_flow_chttp_server_s turbo_flow_chttp_server_t;

typedef enum turbo_flow_chttp_client_state_e {
  TURBO_FLOW_CHTTP_CLIENT_REGISTERED = 0,
  TURBO_FLOW_CHTTP_CLIENT_RUNNING,
  TURBO_FLOW_CHTTP_CLIENT_STOPPING,
  TURBO_FLOW_CHTTP_CLIENT_STOPPED,
  TURBO_FLOW_CHTTP_CLIENT_DETACHED,
  TURBO_FLOW_CHTTP_CLIENT_FAILED
} turbo_flow_chttp_client_state_t;

/**
 * Fixed request policy for one graph adapter.
 *
 * Registration copies the CHTTP client config, request strings and headers.
 * The TLS profile remains borrowed until the client is successfully destroyed. Every input
 * message payload is copied by CHTTP as the request body before owner polling
 * returns. `max_attempts` includes the initial request; zero is invalid.
 */
typedef struct turbo_flow_chttp_client_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *adapter_name;
  const chttp_client_config *client;
  const char *connection_uri;
  const char *authority;
  const char *target;
  chttp_method method;
  const chttp_header *headers;
  size_t header_count;
  const chttp_tls_profile *tls;
  chttp_protocol protocol;
  uint32_t overall_timeout_ms;
  uint32_t max_attempts;
  uint32_t retry_delay_ms;
  uint32_t stop_timeout_ms;
  /** Allows retries for methods that are not idempotent by definition. */
  int idempotent;
} turbo_flow_chttp_client_config_t;

#define TURBO_FLOW_CHTTP_CLIENT_CONFIG_V1_SIZE sizeof(turbo_flow_chttp_client_config_t)
#define TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT                                                        \
  {TURBO_FLOW_CHTTP_CLIENT_CONFIG_V1_SIZE,                                                         \
   TURBO_FLOW_CHTTP_CLIENT_API_VERSION,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   CHTTP_METHOD_GET,                                                                               \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   CHTTP_HTTP_1_1,                                                                                 \
   0u,                                                                                             \
   1u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_CHTTP_DEFAULT_STOP_TIMEOUT_MS,                                                       \
   0}

typedef struct turbo_flow_chttp_client_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_chttp_client_state_t state;
  size_t active_requests;
  size_t queued_requests;
  uint64_t submitted_requests;
  uint64_t completed_requests;
  uint64_t retried_requests;
  uint64_t canceled_requests;
  uint64_t response_bytes;
  int last_status;
  int last_native_status;
} turbo_flow_chttp_client_snapshot_t;

#define TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_V1_SIZE sizeof(turbo_flow_chttp_client_snapshot_t)
#define TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT                                                      \
  {TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_V1_SIZE,                                                       \
   TURBO_FLOW_CHTTP_CLIENT_API_VERSION,                                                            \
   TURBO_FLOW_CHTTP_CLIENT_REGISTERED,                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   SALTS_OK,                                                                                       \
   0}

/** Message-owned HTTP response metadata. String data is available through accessors. */
typedef struct turbo_flow_chttp_response_context_s {
  size_t size;
  uint32_t version;
  unsigned int http_major;
  unsigned int http_minor;
  unsigned int status_code;
  size_t header_count;
  int protocol_keep_alive;
  chttp_protocol protocol;
  uint32_t attempts;
} turbo_flow_chttp_response_context_t;

#define TURBO_FLOW_CHTTP_RESPONSE_CONTEXT_V1_SIZE sizeof(turbo_flow_chttp_response_context_t)

typedef enum turbo_flow_chttp_server_state_e {
  TURBO_FLOW_CHTTP_SERVER_REGISTERED = 0,
  TURBO_FLOW_CHTTP_SERVER_STARTING,
  TURBO_FLOW_CHTTP_SERVER_RUNNING,
  TURBO_FLOW_CHTTP_SERVER_STOPPING,
  TURBO_FLOW_CHTTP_SERVER_STOPPED,
  TURBO_FLOW_CHTTP_SERVER_DETACHED,
  TURBO_FLOW_CHTTP_SERVER_FAILED
} turbo_flow_chttp_server_state_t;

/**
 * One bounded HTTP/1.1 route exposed as a Flow source and terminal response sink.
 *
 * Registration copies the server configuration, host, session cookie name,
 * adapter/source/path strings, response content types, and graph-error body.
 * TLS configuration remains borrowed through registry detachment. Sessions must
 * be disabled because CHTTP deferred responses do not retain session requests.
 * `server->max_buffered_response_body_bytes` must be explicit and nonzero so
 * request admission and terminal response validation share one hard bound.
 */
typedef struct turbo_flow_chttp_server_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *adapter_name;
  const char *source_name;
  const chttp_server_config *server;
  const chttp_server_socket_options *socket_options;
  chttp_method method;
  const char *path;
  size_t max_request_message_bytes;
  unsigned int success_status;
  unsigned int overload_status;
  unsigned int unavailable_status;
  unsigned int graph_error_status;
  const char *response_content_type;
  const char *error_content_type;
  const void *graph_error_body;
  size_t graph_error_body_size;
  uint64_t first_message_id;
  uint32_t stop_timeout_ms;
} turbo_flow_chttp_server_config_t;

#define TURBO_FLOW_CHTTP_SERVER_CONFIG_V1_SIZE sizeof(turbo_flow_chttp_server_config_t)
#define TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT                                                        \
  {TURBO_FLOW_CHTTP_SERVER_CONFIG_V1_SIZE,                                                         \
   TURBO_FLOW_CHTTP_SERVER_API_VERSION,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   CHTTP_METHOD_GET,                                                                               \
   NULL,                                                                                           \
   TURBO_FLOW_CHTTP_SERVER_DEFAULT_MAX_REQUEST_MESSAGE_BYTES,                                      \
   TURBO_FLOW_CHTTP_SERVER_DEFAULT_SUCCESS_STATUS,                                                 \
   TURBO_FLOW_CHTTP_SERVER_DEFAULT_OVERLOAD_STATUS,                                                \
   TURBO_FLOW_CHTTP_SERVER_DEFAULT_UNAVAILABLE_STATUS,                                             \
   TURBO_FLOW_CHTTP_SERVER_DEFAULT_GRAPH_ERROR_STATUS,                                             \
   "application/octet-stream",                                                                     \
   "text/plain",                                                                                   \
   "flow execution failed",                                                                        \
   sizeof("flow execution failed") - 1u,                                                           \
   1u,                                                                                             \
   TURBO_FLOW_CHTTP_DEFAULT_STOP_TIMEOUT_MS}

/** Message-owned metadata copied from a CHTTP server request callback. */
typedef struct turbo_flow_chttp_server_request_context_s {
  size_t size;
  uint32_t version;
  unsigned int http_major;
  unsigned int http_minor;
  chttp_method method;
  size_t header_count;
  size_t param_count;
  int body_streamed;
  int protocol_keep_alive;
  int has_peer;
  cnet_stream_peer peer;
} turbo_flow_chttp_server_request_context_t;

#define TURBO_FLOW_CHTTP_SERVER_REQUEST_CONTEXT_V1_SIZE                                            \
  sizeof(turbo_flow_chttp_server_request_context_t)

typedef struct turbo_flow_chttp_server_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_chttp_server_state_t state;
  uint16_t bound_port;
  size_t active_requests;
  size_t request_capacity;
  uint64_t admitted_requests;
  uint64_t rejected_requests;
  uint64_t completed_requests;
  uint64_t response_bytes;
  int last_status;
} turbo_flow_chttp_server_snapshot_t;

#define TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_V1_SIZE sizeof(turbo_flow_chttp_server_snapshot_t)
#define TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT                                                      \
  {TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_V1_SIZE,                                                       \
   TURBO_FLOW_CHTTP_SERVER_API_VERSION,                                                            \
   TURBO_FLOW_CHTTP_SERVER_REGISTERED,                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   SALTS_OK}

/** Registers one asynchronous 0..1 response transform before graph compilation. */
TURBO_FLOW_C_API int
turbo_flow_chttp_client_register(const turbo_flow_chttp_client_config_t *config,
                                 turbo_flow_chttp_client_t **out_client);

/**
 * Atomically registers one SOURCE|SINK async server adapter before Flow compilation.
 *
 * Example DSL after registering `adapter_name="http.server"` and
 * `source_name="http_in"`:
 * @code
 * source http_in adapter http.server
 * stage response adapter http.server
 * stage main { http_in -> response }
 * @endcode
 *
 * @param config Borrowed configuration; documented strings are copied before return.
 * @param out_server Receives the adapter owner on success and NULL on failure.
 * @return SALTS_OK, SALTS_EINVAL for an invalid/unbounded contract, SALTS_ENOMEM,
 *         or the Flow registry error. HTTP/2 is accepted at registration but
 *         fails `turbo_flow_start()` with SALTS_ENOTSUP.
 */
TURBO_FLOW_C_API int
turbo_flow_chttp_server_register(const turbo_flow_chttp_server_config_t *config,
                                 turbo_flow_chttp_server_t **out_server);

/**
 * Copies lifecycle, capacity, and terminal counters without advancing either runtime.
 * @param server Borrowed registered server owner.
 * @param out_snapshot Caller-initialized size/versioned output.
 * @return SALTS_OK or SALTS_EINVAL.
 */
TURBO_FLOW_C_API int
turbo_flow_chttp_server_snapshot(const turbo_flow_chttp_server_t *server,
                                 turbo_flow_chttp_server_snapshot_t *out_snapshot);

/**
 * Releases a server only after Flow registry shutdown detached the adapter.
 * @param server Server owner returned by registration.
 * @return SALTS_OK, SALTS_EINVAL, or SALTS_EBUSY while attached/active.
 */
TURBO_FLOW_C_API int turbo_flow_chttp_server_destroy(turbo_flow_chttp_server_t *server);

/**
 * Returns request metadata only for a valid message-owned CHTTP server request.
 * The returned pointer remains valid while `message` retains its buffer.
 * @param message Borrowed Flow message.
 * @return Borrowed immutable context, or NULL for a non-CHTTP/stale shape.
 */
TURBO_FLOW_C_API const turbo_flow_chttp_server_request_context_t *
turbo_flow_chttp_server_request_context(const turbo_flow_msg_t *message);

/** Copies a borrowed target view; returns SALTS_OK, SALTS_EINVAL, or SALTS_EPROTO. */
TURBO_FLOW_C_API int turbo_flow_chttp_server_request_target(const turbo_flow_msg_t *message,
                                                            vstr *out_target);
/** Copies a borrowed normalized path view; returns SALTS_OK, SALTS_EINVAL, or SALTS_EPROTO. */
TURBO_FLOW_C_API int turbo_flow_chttp_server_request_path(const turbo_flow_msg_t *message,
                                                          vstr *out_path);
/** Copies borrowed header name/value views by index; returns SALTS_OK/EINVAL/EPROTO. */
TURBO_FLOW_C_API int turbo_flow_chttp_server_request_header_at(const turbo_flow_msg_t *message,
                                                               size_t index, vstr *out_name,
                                                               vstr *out_value);
/** Copies borrowed route-parameter name/value views by index; returns SALTS_OK/EINVAL/EPROTO. */
TURBO_FLOW_C_API int turbo_flow_chttp_server_request_param_at(const turbo_flow_msg_t *message,
                                                              size_t index, vstr *out_name,
                                                              vstr *out_value);
/** Copies the borrowed peer-certificate digest view; returns SALTS_OK/EINVAL/EPROTO. */
TURBO_FLOW_C_API int
turbo_flow_chttp_server_request_peer_certificate_sha256(const turbo_flow_msg_t *message,
                                                        vstr *out_digest);

/**
 * Drives queued admission, CHTTP progress, cancellation, deadlines and retries.
 * Exactly one thread may poll a client; concurrent poll fails with `SALTS_EBUSY`.
 */
TURBO_FLOW_C_API int turbo_flow_chttp_client_poll(turbo_flow_chttp_client_t *client,
                                                  uint32_t timeout_ms,
                                                  turbo_flow_chttp_client_snapshot_t *out_snapshot);

/** Requests cancellation by the unique message id of a queued or active request. */
TURBO_FLOW_C_API int turbo_flow_chttp_client_cancel(turbo_flow_chttp_client_t *client,
                                                    uint64_t message_id);

/** Copies a synchronized lifecycle and capacity snapshot. */
TURBO_FLOW_C_API int
turbo_flow_chttp_client_snapshot(const turbo_flow_chttp_client_t *client,
                                 turbo_flow_chttp_client_snapshot_t *out_snapshot);

/** Requires registry shutdown/detachment; repeated destruction is not supported. */
TURBO_FLOW_C_API int turbo_flow_chttp_client_destroy(turbo_flow_chttp_client_t *client);

/** Returns response metadata only when `message` owns a CHTTP response buffer. */
TURBO_FLOW_C_API const turbo_flow_chttp_response_context_t *
turbo_flow_chttp_response_context(const turbo_flow_msg_t *message);

/** Returns a message-owned reason phrase view. */
TURBO_FLOW_C_API int turbo_flow_chttp_response_reason(const turbo_flow_msg_t *message,
                                                      vstr *out_reason);

/** Returns message-owned name/value views for one response header. */
TURBO_FLOW_C_API int turbo_flow_chttp_response_header_at(const turbo_flow_msg_t *message,
                                                         size_t index, vstr *out_name,
                                                         vstr *out_value);

#ifdef __cplusplus
}
#endif

#endif
