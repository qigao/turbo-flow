#ifndef TURBO_FLOW_PROTOCOL_RUNTIME_H
#define TURBO_FLOW_PROTOCOL_RUNTIME_H

#include "turbo_flow_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION 1u
#define TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_SESSIONS 64u
#define TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_FRAME_SIZE (64u * 1024u)
#define TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_BUFFERED_BYTES                                  \
  ((TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_SESSIONS + 1u) *                                   \
   TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_FRAME_SIZE)

typedef struct turbo_flow_protocol_runtime_s turbo_flow_protocol_runtime_t;

typedef enum turbo_flow_protocol_session_state_e {
  TURBO_FLOW_PROTOCOL_SESSION_OPEN = 1,
  TURBO_FLOW_PROTOCOL_SESSION_WAIT_SETTLEMENT,
  TURBO_FLOW_PROTOCOL_SESSION_DRAINING,
  TURBO_FLOW_PROTOCOL_SESSION_FAILED
} turbo_flow_protocol_session_state_t;

typedef enum turbo_flow_protocol_publish_disposition_e {
  /** The callback completed the required graph/sink admission boundary inline. */
  TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED = 1,
  /** The graph or selected sink owns the publication and will settle it later. */
  TURBO_FLOW_PROTOCOL_PUBLISH_PENDING
} turbo_flow_protocol_publish_disposition_t;

typedef struct turbo_flow_protocol_runtime_config_s {
  size_t size;
  uint32_t abi_version;
  size_t max_sessions;
  size_t max_frame_size;
  /**
   * Hard allocation budget for all session frame buffers plus one mapping
   * scratch buffer. create fails unless it covers
   * (max_sessions + 1) * max_frame_size.
   */
  size_t max_buffered_bytes;
} turbo_flow_protocol_runtime_config_t;

#define TURBO_FLOW_PROTOCOL_RUNTIME_CONFIG_INIT                                                 \
  {sizeof(turbo_flow_protocol_runtime_config_t),                                                \
   TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION,                                                     \
   TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_SESSIONS,                                            \
   TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_FRAME_SIZE,                                          \
   TURBO_FLOW_PROTOCOL_RUNTIME_DEFAULT_MAX_BUFFERED_BYTES}

typedef struct turbo_flow_protocol_session_open_request_s {
  size_t size;
  uint32_t abi_version;
  uint64_t session_id;
  uint64_t generation;
  const char *device_id;
  const char *protocol_version;
} turbo_flow_protocol_session_open_request_t;

#define TURBO_FLOW_PROTOCOL_SESSION_OPEN_REQUEST_INIT                                           \
  {sizeof(turbo_flow_protocol_session_open_request_t),                                          \
   TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION, 0u, 0u, NULL, NULL}

/**
 * Borrowed publication delivered synchronously to the runtime owner.
 *
 * message and its buffers are invalid after publish() returns. A callback that
 * selects PENDING must copy or otherwise admit all required data before return.
 */
typedef struct turbo_flow_protocol_publish_request_s {
  size_t size;
  uint32_t abi_version;
  uint64_t delivery_id;
  uint64_t session_id;
  uint64_t session_generation;
  const turbo_flow_protocol_message_output_t *message;
} turbo_flow_protocol_publish_request_t;

#define TURBO_FLOW_PROTOCOL_PUBLISH_REQUEST_INIT                                                \
  {sizeof(turbo_flow_protocol_publish_request_t),                                               \
   TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION, 0u, 0u, 0u, NULL}

typedef int (*turbo_flow_protocol_runtime_publish_fn)(
    void *ctx, const turbo_flow_protocol_publish_request_t *request,
    turbo_flow_protocol_publish_disposition_t *disposition);
typedef void (*turbo_flow_protocol_runtime_settled_fn)(
    void *ctx, uint64_t session_id, uint64_t generation, uint64_t delivery_id,
    const turbo_flow_protocol_metadata_t *metadata, int status);
typedef void (*turbo_flow_protocol_runtime_session_closed_fn)(
    void *ctx, uint64_t session_id, uint64_t generation, int status);
/**
 * Borrowed protocol response emitted only after the required graph/sink
 * settlement. The callback must synchronously copy or send frame->data before
 * returning and must not re-enter the runtime.
 */
typedef int (*turbo_flow_protocol_runtime_reply_fn)(
    void *ctx, uint64_t session_id, uint64_t generation,
    uint64_t delivery_id, const turbo_flow_protocol_frame_output_t *frame);

typedef struct turbo_flow_protocol_runtime_ops_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_runtime_publish_fn publish;
  turbo_flow_protocol_runtime_settled_fn settled;
  turbo_flow_protocol_runtime_session_closed_fn session_closed;
  turbo_flow_protocol_runtime_reply_fn reply;
} turbo_flow_protocol_runtime_ops_t;

#define TURBO_FLOW_PROTOCOL_RUNTIME_OPS_INIT                                                    \
  {sizeof(turbo_flow_protocol_runtime_ops_t),                                                   \
   TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION, NULL, NULL, NULL, NULL}

typedef struct turbo_flow_protocol_feed_result_s {
  size_t size;
  uint32_t abi_version;
  size_t accepted_size;
  size_t frames_dispatched;
  uint64_t pending_delivery_id;
  uint8_t backpressured;
} turbo_flow_protocol_feed_result_t;

#define TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT                                                    \
  {sizeof(turbo_flow_protocol_feed_result_t),                                                   \
   TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION, 0u, 0u, 0u, 0u}

typedef struct turbo_flow_protocol_runtime_snapshot_s {
  size_t size;
  uint32_t abi_version;
  size_t active_sessions;
  size_t pending_settlements;
  size_t buffered_bytes;
  uint8_t accepting;
} turbo_flow_protocol_runtime_snapshot_t;

#define TURBO_FLOW_PROTOCOL_RUNTIME_SNAPSHOT_INIT                                               \
  {sizeof(turbo_flow_protocol_runtime_snapshot_t),                                              \
   TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION, 0u, 0u, 0u, 0u}

/**
 * Create a caller-serialized, single-event-loop runtime.
 *
 * protocol is borrowed and must outlive the runtime. callbacks run inline and
 * must not re-enter this runtime. All mutating APIs must be called by the same
 * serialized transport/event-loop owner.
 */
CXX_C_API int turbo_flow_protocol_runtime_create(
    turbo_flow_protocol_t *protocol,
    const turbo_flow_protocol_runtime_config_t *config,
    const turbo_flow_protocol_runtime_ops_t *ops, void *ctx,
    turbo_flow_protocol_runtime_t **out);
CXX_C_API int turbo_flow_protocol_runtime_destroy(
    turbo_flow_protocol_runtime_t *runtime);

CXX_C_API int turbo_flow_protocol_runtime_session_open(
    turbo_flow_protocol_runtime_t *runtime,
    const turbo_flow_protocol_session_open_request_t *request);
CXX_C_API int turbo_flow_protocol_runtime_session_feed(
    turbo_flow_protocol_runtime_t *runtime, uint64_t session_id,
    uint64_t generation, const uint8_t *data, size_t size,
    turbo_flow_protocol_feed_result_t *result);
CXX_C_API int turbo_flow_protocol_runtime_settle(
    turbo_flow_protocol_runtime_t *runtime, uint64_t delivery_id, int status,
    turbo_flow_protocol_feed_result_t *result);
CXX_C_API int turbo_flow_protocol_runtime_session_close(
    turbo_flow_protocol_runtime_t *runtime, uint64_t session_id,
    uint64_t generation, int status);

/**
 * Close admission and drain already admitted graph/sink ownership.
 *
 * Sessions without pending settlement close immediately. Pending sessions
 * remain DRAINING until settle(). No new session/feed is admitted.
 */
CXX_C_API int turbo_flow_protocol_runtime_begin_shutdown(
    turbo_flow_protocol_runtime_t *runtime);

/** Cancel all pending ownership and close every session deterministically. */
CXX_C_API int turbo_flow_protocol_runtime_force_shutdown(
    turbo_flow_protocol_runtime_t *runtime, int status);

CXX_C_API int turbo_flow_protocol_runtime_snapshot(
    const turbo_flow_protocol_runtime_t *runtime,
    turbo_flow_protocol_runtime_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_RUNTIME_H */
