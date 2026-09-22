#ifndef TURBO_FLOW_PROTOCOL_SOURCE_H
#define TURBO_FLOW_PROTOCOL_SOURCE_H

#include "turbo_flow_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION 2u
#define TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_SESSIONS 64u
#define TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_FRAME_SIZE (64u * 1024u)
#define TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_BUFFERED_BYTES                                      \
  ((TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_SESSIONS + 1u) *                                        \
   TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_FRAME_SIZE)

typedef struct turbo_flow_protocol_source_s turbo_flow_protocol_source_t;

typedef enum turbo_flow_protocol_source_decode_mode_e {
  TURBO_FLOW_PROTOCOL_SOURCE_DECODE_RAW = 1,
  TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC = 2
} turbo_flow_protocol_source_decode_mode_t;

typedef struct turbo_flow_protocol_source_config_s {
  size_t size;
  uint32_t abi_version;
  size_t max_sessions;
  size_t max_frame_size;
  /**
   * Hard allocation budget for all session frame buffers plus one decode
   * scratch buffer. create fails unless it covers
   * (max_sessions + 1) * max_frame_size.
   */
  size_t max_buffered_bytes;
  /** RAW preserves ABI1 behavior; SEMANTIC additionally invokes the codec's
   * single-pass semantic decoder and exposes its borrowed output to admit(). */
  turbo_flow_protocol_source_decode_mode_t decode_mode;
  /** Required only for SEMANTIC mode and included in max_buffered_bytes. */
  size_t max_semantic_bytes;
} turbo_flow_protocol_source_config_t;

#define TURBO_FLOW_PROTOCOL_SOURCE_CONFIG_INIT                                                     \
  {sizeof(turbo_flow_protocol_source_config_t), TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION,            \
   TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_SESSIONS,                                                \
   TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_FRAME_SIZE,                                              \
   TURBO_FLOW_PROTOCOL_SOURCE_DEFAULT_MAX_BUFFERED_BYTES}

typedef struct turbo_flow_protocol_source_session_open_request_s {
  size_t size;
  uint32_t abi_version;
  uint64_t session_id;
  uint64_t generation;
  const char *device_id;
  const char *protocol_version;
} turbo_flow_protocol_source_session_open_request_t;

#define TURBO_FLOW_PROTOCOL_SOURCE_SESSION_OPEN_REQUEST_INIT                                       \
  {sizeof(turbo_flow_protocol_source_session_open_request_t),                                      \
   TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION,                                                         \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL}

/**
 * Borrowed decoded message offered synchronously to the configured Source
 * admission boundary. All pointers become invalid when admit() returns.
 * SALTS_OK means the provider owns a complete copy of the admitted message.
 */
typedef struct turbo_flow_protocol_source_admit_request_s {
  size_t size;
  uint32_t abi_version;
  uint64_t delivery_id;
  uint64_t session_id;
  uint64_t session_generation;
  const turbo_flow_protocol_message_output_t *message;
  /** Non-NULL only when the Source was created in SEMANTIC decode mode. */
  const turbo_flow_protocol_semantic_output_t *semantic;
} turbo_flow_protocol_source_admit_request_t;

#define TURBO_FLOW_PROTOCOL_SOURCE_ADMIT_REQUEST_INIT                                              \
  {sizeof(turbo_flow_protocol_source_admit_request_t),                                             \
   TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION,                                                         \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL}

typedef int (*turbo_flow_protocol_source_admit_fn)(
    void *ctx, const turbo_flow_protocol_source_admit_request_t *request);
typedef void (*turbo_flow_protocol_source_session_closed_fn)(void *ctx, uint64_t session_id,
                                                             uint64_t generation, int status);

typedef struct turbo_flow_protocol_source_ops_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_source_admit_fn admit;
  turbo_flow_protocol_source_session_closed_fn session_closed;
} turbo_flow_protocol_source_ops_t;

#define TURBO_FLOW_PROTOCOL_SOURCE_OPS_INIT                                                        \
  {sizeof(turbo_flow_protocol_source_ops_t), TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION, NULL, NULL}

typedef struct turbo_flow_protocol_source_feed_result_s {
  size_t size;
  uint32_t abi_version;
  /** Bytes copied from this feed into Source-owned bounded storage. */
  size_t accepted_size;
  size_t frames_admitted;
  uint8_t backpressured;
} turbo_flow_protocol_source_feed_result_t;

#define TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT                                                \
  {sizeof(turbo_flow_protocol_source_feed_result_t), TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION, 0u,   \
   0u, 0u}

typedef struct turbo_flow_protocol_source_snapshot_s {
  size_t size;
  uint32_t abi_version;
  size_t active_sessions;
  size_t buffered_bytes;
  uint8_t accepting;
} turbo_flow_protocol_source_snapshot_t;

#define TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT                                                   \
  {sizeof(turbo_flow_protocol_source_snapshot_t), TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION, 0u, 0u,  \
   0u}

/**
 * Create a caller-serialized protocol Source.
 *
 * protocol is borrowed and must outlive the Source. Callbacks run inline and
 * must not re-enter this Source. All mutating APIs must be called by the same
 * serialized transport/event-loop owner. Every size-versioned input requires
 * the exact ABI 2 layout; prefixes and oversized layouts are rejected. RAW mode
 * preserves the historical raw-message admission path. SEMANTIC mode performs
 * exactly one codec semantic decode and never reparses the raw frame in Source.
 */
TURBO_FLOW_C_API int turbo_flow_protocol_source_create(
    turbo_flow_protocol_t *protocol, const turbo_flow_protocol_source_config_t *config,
    const turbo_flow_protocol_source_ops_t *ops, void *ctx, turbo_flow_protocol_source_t **out);
TURBO_FLOW_C_API int turbo_flow_protocol_source_destroy(turbo_flow_protocol_source_t *source);

TURBO_FLOW_C_API int turbo_flow_protocol_source_session_open(
    turbo_flow_protocol_source_t *source,
    const turbo_flow_protocol_source_session_open_request_t *request);

/**
 * Copy feed bytes into bounded Source storage and synchronously admit complete
 * decoded frames. A capacity rejection retains the current frame, sets
 * backpressured, and returns SALTS_OK. While backpressured, call with NULL/0 to
 * retry the retained frame; non-empty feeds return SALTS_EBUSY.
 */
TURBO_FLOW_C_API int
turbo_flow_protocol_source_session_feed(turbo_flow_protocol_source_t *source, uint64_t session_id,
                                        uint64_t generation, const uint8_t *data, size_t size,
                                        turbo_flow_protocol_source_feed_result_t *result);
TURBO_FLOW_C_API int turbo_flow_protocol_source_session_close(turbo_flow_protocol_source_t *source,
                                                              uint64_t session_id,
                                                              uint64_t generation, int status);

/** Stop admission and synchronously close every Source session. */
TURBO_FLOW_C_API int
turbo_flow_protocol_source_begin_shutdown(turbo_flow_protocol_source_t *source);

/** Stop admission and synchronously fail every Source session. */
TURBO_FLOW_C_API int turbo_flow_protocol_source_force_shutdown(turbo_flow_protocol_source_t *source,
                                                               int status);

TURBO_FLOW_C_API int
turbo_flow_protocol_source_snapshot(const turbo_flow_protocol_source_t *source,
                                    turbo_flow_protocol_source_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_SOURCE_H */
