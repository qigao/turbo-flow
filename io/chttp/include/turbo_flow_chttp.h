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
#define TURBO_FLOW_CHTTP_DEFAULT_STOP_TIMEOUT_MS 5000u

typedef struct turbo_flow_chttp_client_s turbo_flow_chttp_client_t;

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

/** Registers one asynchronous 0..1 response transform before graph compilation. */
TURBO_FLOW_C_API int
turbo_flow_chttp_client_register(const turbo_flow_chttp_client_config_t *config,
                                 turbo_flow_chttp_client_t **out_client);

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
