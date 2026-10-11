#ifndef TURBO_FLOW_CNET_H
#define TURBO_FLOW_CNET_H

#include "turbo_flow.h"

#include <cnet/cnet.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION 1u
#define TURBO_FLOW_CNET_STREAM_SOURCE_DEFAULT_MAX_MESSAGE_BYTES (1024u * 1024u)
#define TURBO_FLOW_CNET_STREAM_SOURCE_DEFAULT_SCHEDULER_CAPACITY 64u
#define TURBO_FLOW_CNET_STREAM_SOURCE_DEFAULT_MAX_STEPS 256u
#define TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY 64u

typedef struct turbo_flow_cnet_stream_source_s turbo_flow_cnet_stream_source_t;

typedef enum turbo_flow_cnet_stream_source_state_e {
  TURBO_FLOW_CNET_STREAM_SOURCE_NEW = 0,
  TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTING,
  TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED,
  TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED,
  TURBO_FLOW_CNET_STREAM_SOURCE_FAILED,
  TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING,
  TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED
} turbo_flow_cnet_stream_source_state_t;

/**
 * Configuration copied or synchronously consumed by open.
 *
 * The returned source, all of its API calls, and the referenced Flow have one
 * serialized owner thread. `content` is copied into each emitted message.
 *
 * @code{.c}
 * int open_tcp_source(turbo_flow_t *flow, const cnet_client_config *client,
 *                     const char *uri, turbo_flow_cnet_stream_source_t **out) {
 *   turbo_flow_cnet_stream_source_config_t config =
 *       TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
 *   config.flow = flow;
 *   config.source_name = "input";
 *   config.uri = uri;
 *   config.client = client;
 *   config.max_message_bytes = client->receive_buffer_bytes;
 *   // Forward both status and owner; the caller must clean up a non-NULL *out
 *   // even when open reports an error. See the open cleanup example below.
 *   return turbo_flow_cnet_stream_source_open(&config, out);
 * }
 * @endcode
 */
typedef struct turbo_flow_cnet_stream_source_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *source_name;
  const char *uri;
  const cnet_client_config *client;
  const cnet_stream_socket_options *socket_options;
  const cnet_tls_client_config *tls;
  const turbo_flow_content_descriptor_t *content;
  size_t max_message_bytes;
  size_t scheduler_capacity;
  size_t scheduler_max_steps_per_poll;
  uint64_t first_message_id;
} turbo_flow_cnet_stream_source_config_t;

#define TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_V1_SIZE sizeof(turbo_flow_cnet_stream_source_config_t)
#define TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT                                                  \
  {TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_V1_SIZE,                                                   \
   TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION,                                                      \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_CNET_STREAM_SOURCE_DEFAULT_MAX_MESSAGE_BYTES,                                        \
   TURBO_FLOW_CNET_STREAM_SOURCE_DEFAULT_SCHEDULER_CAPACITY,                                       \
   TURBO_FLOW_CNET_STREAM_SOURCE_DEFAULT_MAX_STEPS,                                                \
   1u}

/** Caller-owned portable snapshot. `error_stage` is copied from CNet. */
typedef struct turbo_flow_cnet_stream_source_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_cnet_stream_source_state_t state;
  int status;
  int native_status;
  cnet_connection connection;
  uint64_t messages_received;
  uint64_t bytes_received;
  size_t outstanding_demand;
  int receive_pending;
  char error_stage[TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY];
} turbo_flow_cnet_stream_source_snapshot_t;

#define TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_V1_SIZE                                             \
  sizeof(turbo_flow_cnet_stream_source_snapshot_t)
#define TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT                                                \
  {TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_V1_SIZE,                                                 \
   TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION,                                                      \
   TURBO_FLOW_CNET_STREAM_SOURCE_NEW,                                                              \
   SALTS_OK,                                                                                       \
   0,                                                                                              \
   {0u, 0u},                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0,                                                                                              \
   {0}}

/**
 * Open one CNet TCP, TLS, or Pipe source without polling the network.
 *
 * @param config Valid size/version configuration, consumed synchronously;
 * its Flow must remain alive until the returned owner's cleanup completes.
 * @param source_out Receives the owning handle on success. On failure it is
 * NULL if rollback completes, or a retryable owner if cleanup remains pending.
 * @return SALTS_OK on success; the validation/allocation/CNet/Scheduler/Graph
 * admission error after complete rollback; otherwise the cleanup error.
 *
 * A non-NULL owner returned on error is FAILED. Its snapshot status/error_stage
 * preserves the original open failure. Only snapshot, stop and destroy are
 * permitted: keep the owner and its Flow, explicitly retry stop, then destroy.
 * Do not discard or overwrite the output merely because open returned an error.
 *
 * Cleanup example for any non-NULL source, including one returned on failure:
 * @code{.c}
 * int cleanup_status = turbo_flow_cnet_stream_source_stop(source, timeout_ms);
 * turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
 * if (turbo_flow_cnet_stream_source_snapshot(source, &snapshot) == SALTS_OK &&
 *     snapshot.state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) {
 *   // Report cleanup_status separately: a progress error can follow full drain.
 *   if (turbo_flow_cnet_stream_source_destroy(source) == SALTS_OK) source = NULL;
 * }
 * // If source remains non-NULL, retain it and its Flow for a later stop retry.
 * @endcode
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_stream_source_open(const turbo_flow_cnet_stream_source_config_t *config,
                                   turbo_flow_cnet_stream_source_t **source_out);

/**
 * Add graph-output demand. Network receive admission remains demand-gated.
 *
 * @param source Live owner-thread source.
 * @param demand Positive downstream-value demand.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_ESHUTDOWN, the preserved terminal
 * source error, or a bounded Scheduler admission error.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_stream_source_request(turbo_flow_cnet_stream_source_t *source,
                                                           size_t demand);

/**
 * Drive bounded CFlow work, then CNet progress, then newly-woken CFlow work.
 *
 * @param source Live owner-thread source.
 * @param timeout_ms Maximum CNet wait for this pass; zero is nonblocking.
 * @param snapshot Optional initialized snapshot receiving state after progress.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_ESHUTDOWN, or the first preserved
 * CNet/Graph terminal error.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_stream_source_poll(turbo_flow_cnet_stream_source_t *source, uint32_t timeout_ms,
                                   turbo_flow_cnet_stream_source_snapshot_t *snapshot);

/**
 * Copy state without advancing CNet or CFlow.
 *
 * @param source Live source owned by the calling thread.
 * @param snapshot Initialized size/version destination.
 * @return SALTS_OK or SALTS_EINVAL.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_stream_source_snapshot(const turbo_flow_cnet_stream_source_t *source,
                                       turbo_flow_cnet_stream_source_snapshot_t *snapshot);

/**
 * Close admission and drive CNet to quiescence within `timeout_ms`.
 *
 * @param source Live source owned by the calling thread.
 * @param timeout_ms Maximum CNet drain duration.
 * @return SALTS_OK, SALTS_EALREADY, SALTS_ETIMEDOUT, SALTS_EBUSY, or a drain/
 * destroy error. Pending cleanup retains the owner in STOPPING for explicit
 * stop retry. A non-timeout progress error can accompany completed cleanup:
 * snapshot state STOPPED authorizes destroy even if stop returned an error.
 * Scheduler shutdown must complete before publishing STOPPED.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_stream_source_stop(turbo_flow_cnet_stream_source_t *source,
                                                        uint32_t timeout_ms);

/**
 * Release a source whose snapshot state is STOPPED.
 *
 * @param source Stopped source owned by the calling thread.
 * @return SALTS_OK, SALTS_EINVAL, or SALTS_EBUSY before STOPPED.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_stream_source_destroy(turbo_flow_cnet_stream_source_t *source);

#define TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION 1u
#define TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_MAX_CONNECTIONS 64u
#define TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_MAX_MESSAGE_BYTES (1024u * 1024u)
#define TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_SCHEDULER_CAPACITY 64u
#define TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_MAX_STEPS 256u

typedef struct turbo_flow_cnet_listener_source_s turbo_flow_cnet_listener_source_t;

typedef enum turbo_flow_cnet_listener_source_state_e {
  TURBO_FLOW_CNET_LISTENER_SOURCE_NEW = 0,
  TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING,
  TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED,
  TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING,
  TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED
} turbo_flow_cnet_listener_source_state_t;

/**
 * Configuration synchronously consumed by listener-source open.
 *
 * A non-NULL `tls` enables only TLS accepts and requires bounded TLS storage
 * in `client`. The returned owner, its Flow, and every public call belong to
 * one serialized thread.
 *
 * @code{.c}
 * int open_tcp_listener(turbo_flow_t *flow,
 *                       const cnet_listener_config *listener,
 *                       const cnet_client_config *client,
 *                       turbo_flow_cnet_listener_source_t **out) {
 *   turbo_flow_cnet_listener_source_config_t config =
 *       TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
 *   config.flow = flow;
 *   config.source_name = "input";
 *   config.listener = listener;
 *   config.client = client;
 *   config.max_connections = client->connection_capacity;
 *   config.max_message_bytes = client->receive_buffer_bytes;
 *   // Forward both status and owner; the caller must clean up a non-NULL *out
 *   // even when open reports an error. See the open cleanup example below.
 *   return turbo_flow_cnet_listener_source_open(&config, out);
 * }
 * @endcode
 */
typedef struct turbo_flow_cnet_listener_source_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *source_name;
  const cnet_listener_config *listener;
  const cnet_listener_options *listener_options;
  const cnet_client_config *client;
  const cnet_stream_socket_options *socket_options;
  const cnet_tls_server_config *tls;
  const turbo_flow_content_descriptor_t *content;
  size_t max_connections;
  size_t max_message_bytes;
  size_t scheduler_capacity;
  size_t scheduler_max_steps_per_poll;
  uint64_t first_message_id;
} turbo_flow_cnet_listener_source_config_t;

#define TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_V1_SIZE                                             \
  sizeof(turbo_flow_cnet_listener_source_config_t)
#define TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT                                                \
  {TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_V1_SIZE,                                                 \
   TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION,                                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_MAX_CONNECTIONS,                                        \
   TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_MAX_MESSAGE_BYTES,                                      \
   TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_SCHEDULER_CAPACITY,                                     \
   TURBO_FLOW_CNET_LISTENER_SOURCE_DEFAULT_MAX_STEPS,                                              \
   1u}

/**
 * Caller-owned snapshot copied without advancing either runtime.
 *
 * `status` and `error_stage` describe a terminal owner failure. A failed peer
 * is isolated and reported through `connections_failed` plus the
 * `last_connection_*` fields while the listener remains operational.
 */
typedef struct turbo_flow_cnet_listener_source_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_cnet_listener_source_state_t state;
  int status;
  int native_status;
  uint16_t bound_port;
  uint64_t connections_accepted;
  size_t active_connections;
  uint64_t connections_closed;
  uint64_t connections_failed;
  uint64_t messages_received;
  uint64_t bytes_received;
  size_t outstanding_demand;
  int receive_pending;
  int last_connection_status;
  int last_connection_native_status;
  char last_connection_error_stage[TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY];
  char error_stage[TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY];
} turbo_flow_cnet_listener_source_snapshot_t;

#define TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_V1_SIZE                                           \
  sizeof(turbo_flow_cnet_listener_source_snapshot_t)
#define TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT                                              \
  {TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_V1_SIZE,                                               \
   TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION,                                                    \
   TURBO_FLOW_CNET_LISTENER_SOURCE_NEW,                                                            \
   SALTS_OK,                                                                                       \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0,                                                                                              \
   SALTS_OK,                                                                                       \
   0,                                                                                              \
   {0},                                                                                            \
   {0}}

/**
 * Open a bounded TCP or TLS listener owner without waiting for a peer.
 *
 * @param config Valid size/version configuration, consumed synchronously;
 * its Flow must remain alive until the returned owner's cleanup completes.
 * @param source_out Receives the owning handle on success. On failure it is
 * NULL if rollback completes, or a retryable owner if cleanup remains pending.
 * @return SALTS_OK on success; the validation/allocation/CNet/Scheduler/Graph
 * admission error after complete rollback; otherwise the cleanup error.
 *
 * A non-NULL owner returned on error is FAILED. Its snapshot status/error_stage
 * preserves the original open failure. Only snapshot, stop and destroy are
 * permitted: keep the owner and its Flow, explicitly retry stop, then destroy.
 * Do not discard or overwrite the output merely because open returned an error.
 *
 * Cleanup example for any non-NULL source, including one returned on failure:
 * @code{.c}
 * int cleanup_status = turbo_flow_cnet_listener_source_stop(source, timeout_ms);
 * turbo_flow_cnet_listener_source_snapshot_t snapshot = TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
 * if (turbo_flow_cnet_listener_source_snapshot(source, &snapshot) == SALTS_OK &&
 *     snapshot.state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED) {
 *   // Report cleanup_status separately: a progress error can follow full drain.
 *   if (turbo_flow_cnet_listener_source_destroy(source) == SALTS_OK) source = NULL;
 * }
 * // If source remains non-NULL, retain it and its Flow for a later stop retry.
 * @endcode
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_listener_source_open(const turbo_flow_cnet_listener_source_config_t *config,
                                     turbo_flow_cnet_listener_source_t **source_out);

/** Add positive downstream-value demand; receive admission remains demand-gated. */
TURBO_FLOW_C_API int
turbo_flow_cnet_listener_source_request(turbo_flow_cnet_listener_source_t *source, size_t demand);

/**
 * Drive bounded Scheduler, listener, accepted-client, then woken Scheduler work.
 *
 * @param source Live owner-thread source.
 * @param timeout_ms Maximum wait for this progress pass; zero is nonblocking.
 * @param snapshot Optional initialized destination receiving post-poll state.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_ESHUTDOWN, or the first preserved error.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_listener_source_poll(turbo_flow_cnet_listener_source_t *source, uint32_t timeout_ms,
                                     turbo_flow_cnet_listener_source_snapshot_t *snapshot);

/** Copy portable state without advancing CNet or CFlow. */
TURBO_FLOW_C_API int
turbo_flow_cnet_listener_source_snapshot(const turbo_flow_cnet_listener_source_t *source,
                                         turbo_flow_cnet_listener_source_snapshot_t *snapshot);

/**
 * Close listener admission and drain accepted connections within `timeout_ms`.
 *
 * @param source Live source on its serialized owner thread, including failed open.
 * @param timeout_ms Maximum CNet client drain duration.
 * @return SALTS_OK, SALTS_EALREADY, or a close/drain/destroy error. Pending
 * cleanup retains the owner in STOPPING for explicit stop retry. A progress
 * error can accompany completed cleanup: snapshot state STOPPED authorizes
 * destroy even if stop returned an error. Scheduler shutdown must complete
 * before publishing STOPPED.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_listener_source_stop(turbo_flow_cnet_listener_source_t *source,
                                                          uint32_t timeout_ms);

/** Release a STOPPED source; pending cleanup returns SALTS_EBUSY. */
TURBO_FLOW_C_API int
turbo_flow_cnet_listener_source_destroy(turbo_flow_cnet_listener_source_t *source);

#define TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION 1u
#define TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_QUEUE_CAPACITY 64u
#define TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_MAX_MESSAGE_BYTES (1024u * 1024u)
#define TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_SCHEDULER_CAPACITY 64u
#define TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_MAX_STEPS 256u
#define TURBO_FLOW_CNET_PACKET_SOURCE_ERROR_STAGE_CAPACITY 64u

typedef struct turbo_flow_cnet_packet_source_s turbo_flow_cnet_packet_source_t;

typedef enum turbo_flow_cnet_packet_source_state_e {
  TURBO_FLOW_CNET_PACKET_SOURCE_NEW = 0,
  TURBO_FLOW_CNET_PACKET_SOURCE_RUNNING,
  TURBO_FLOW_CNET_PACKET_SOURCE_FAILED,
  TURBO_FLOW_CNET_PACKET_SOURCE_STOPPING,
  TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED
} turbo_flow_cnet_packet_source_state_t;

/**
 * Configuration synchronously consumed by packet-source open.
 *
 * `endpoint->observer` must be empty because the adapter owns that callback
 * boundary. CNet copies the endpoint configuration, including secure-KCP key
 * material. The returned owner, its Flow, and every public operation belong to
 * one serialized thread.
 */
typedef struct turbo_flow_cnet_packet_source_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *source_name;
  const cnet_packet_endpoint_config *endpoint;
  const turbo_flow_content_descriptor_t *content;
  size_t queue_capacity;
  size_t max_message_bytes;
  size_t scheduler_capacity;
  size_t scheduler_max_steps_per_poll;
  uint64_t first_message_id;
} turbo_flow_cnet_packet_source_config_t;

#define TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_V1_SIZE sizeof(turbo_flow_cnet_packet_source_config_t)
#define TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT                                                  \
  {TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_V1_SIZE,                                                   \
   TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION,                                                      \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_QUEUE_CAPACITY,                                           \
   TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_MAX_MESSAGE_BYTES,                                        \
   TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_SCHEDULER_CAPACITY,                                       \
   TURBO_FLOW_CNET_PACKET_SOURCE_DEFAULT_MAX_STEPS,                                                \
   1u}

/** Message-owned immutable packet identity stored inside `message->buffer`. */
typedef struct turbo_flow_cnet_packet_message_context_s {
  size_t size;
  uint32_t version;
  cnet_packet_session session;
  cnet_packet_session_info info;
} turbo_flow_cnet_packet_message_context_t;

#define TURBO_FLOW_CNET_PACKET_MESSAGE_CONTEXT_V1_SIZE                                             \
  sizeof(turbo_flow_cnet_packet_message_context_t)

/** Caller-owned portable snapshot copied without advancing CNet or CFlow. */
typedef struct turbo_flow_cnet_packet_source_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_cnet_packet_source_state_t state;
  int status;
  cnet_packet_protocol protocol;
  uint16_t bound_port;
  size_t queue_depth;
  size_t queue_capacity;
  uint64_t sessions_admitted;
  uint64_t sessions_opened;
  uint64_t sessions_closed;
  uint64_t messages_received;
  uint64_t bytes_received;
  size_t outstanding_demand;
  cnet_packet_session last_error_session;
  char error_stage[TURBO_FLOW_CNET_PACKET_SOURCE_ERROR_STAGE_CAPACITY];
} turbo_flow_cnet_packet_source_snapshot_t;

#define TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_V1_SIZE                                             \
  sizeof(turbo_flow_cnet_packet_source_snapshot_t)
#define TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT                                                \
  {TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_V1_SIZE,                                                 \
   TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION,                                                      \
   TURBO_FLOW_CNET_PACKET_SOURCE_NEW,                                                              \
   SALTS_OK,                                                                                       \
   (cnet_packet_protocol)0,                                                                        \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {0u, 0u},                                                                                       \
   {0}}

/**
 * Open a bounded UDP/KCP endpoint and Graph source without polling either runtime.
 *
 * @param config Borrowed configuration consumed synchronously; its Flow must
 *               remain alive until any returned source has been stopped.
 * @param source_out Receives the owning handle on success. On failure, NULL
 *                   means rollback completed; a non-NULL handle owns resources
 *                   whose cleanup failed and must be retained for explicit retry.
 * @return SALTS_OK on success, the admission error after successful rollback,
 *         or the cleanup error when rollback cannot finish. For a returned
 *         failed owner, snapshot.status/error_stage preserve the original open
 *         error. Only snapshot, stop and destroy are permitted on that owner.
 *
 * Callers must inspect *source_out even when open returns an error. Retry stop
 * with the returned handle; release it with destroy only after stop succeeds.
 * Do not discard the handle, reuse it for another open, or release its Flow
 * while cleanup is pending. Ordinary validation failures still return NULL.
 *
 * Cleanup example for any non-NULL source, including one returned on failure:
 * @code
 * int cleanup_status = turbo_flow_cnet_packet_source_stop(source, timeout_ms);
 * if (cleanup_status == SALTS_OK || cleanup_status == SALTS_EALREADY) {
 *   cleanup_status = turbo_flow_cnet_packet_source_destroy(source);
 *   if (cleanup_status == SALTS_OK) source = NULL;
 * }
 * // If source remains non-NULL, retain it and its Flow for a later stop retry.
 * @endcode
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_source_open(const turbo_flow_cnet_packet_source_config_t *config,
                                   turbo_flow_cnet_packet_source_t **source_out);

/** Add positive downstream-value demand; protocol progress is never demand-gated. */
TURBO_FLOW_C_API int turbo_flow_cnet_packet_source_request(turbo_flow_cnet_packet_source_t *source,
                                                           size_t demand);

/** Drive bounded Scheduler, CNet packet/timer progress, then woken Scheduler work. */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_source_poll(turbo_flow_cnet_packet_source_t *source, uint32_t timeout_ms,
                                   turbo_flow_cnet_packet_source_snapshot_t *snapshot);

/** Copy portable state without advancing CNet or CFlow. */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_source_snapshot(const turbo_flow_cnet_packet_source_t *source,
                                       turbo_flow_cnet_packet_source_snapshot_t *snapshot);

/** Open one generation-checked peer mapping through the owned CNet endpoint. */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_source_session_open(turbo_flow_cnet_packet_source_t *source,
                                           const cnet_datagram_peer *peer, uint32_t conversation,
                                           cnet_packet_session *session_out);

/** Copy CNet's immutable identity for a currently live generation handle. */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_source_session_get_info(const turbo_flow_cnet_packet_source_t *source,
                                               cnet_packet_session session,
                                               cnet_packet_session_info *info_out);

/** Close one generation-checked session; stale handles return CNet's status. */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_source_session_close(turbo_flow_cnet_packet_source_t *source,
                                            cnet_packet_session session);

/** Copy and admit one UDP datagram or reliable ordered KCP message. */
TURBO_FLOW_C_API int turbo_flow_cnet_packet_source_send(turbo_flow_cnet_packet_source_t *source,
                                                        cnet_packet_session session,
                                                        const void *data, size_t size);

/**
 * Return packet identity owned by `message->buffer`, or NULL for another
 * transport or a malformed/borrowed context. The pointer lives with `message`.
 */
TURBO_FLOW_C_API const turbo_flow_cnet_packet_message_context_t *
turbo_flow_cnet_packet_message_context(const turbo_flow_msg_t *message);

/** Stop session admission and protocol progress, then drain endpoint writes. */
TURBO_FLOW_C_API int turbo_flow_cnet_packet_source_stop(turbo_flow_cnet_packet_source_t *source,
                                                        uint32_t timeout_ms);

/** Release a successfully stopped source; a live owner returns SALTS_EBUSY. */
TURBO_FLOW_C_API int turbo_flow_cnet_packet_source_destroy(turbo_flow_cnet_packet_source_t *source);

#define TURBO_FLOW_CNET_STREAM_SINK_API_VERSION 1u
#define TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_MAX_MESSAGE_BYTES (1024u * 1024u)
#define TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_COMMAND_CAPACITY 2u
#define TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_MAX_STEPS 64u
#define TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_STOP_TIMEOUT_MS 30000u

typedef struct turbo_flow_cnet_stream_sink_s turbo_flow_cnet_stream_sink_t;

typedef enum turbo_flow_cnet_stream_sink_state_e {
  TURBO_FLOW_CNET_STREAM_SINK_REGISTERED = 0,
  TURBO_FLOW_CNET_STREAM_SINK_CONNECTING,
  TURBO_FLOW_CNET_STREAM_SINK_CONNECTED,
  TURBO_FLOW_CNET_STREAM_SINK_FAILED,
  TURBO_FLOW_CNET_STREAM_SINK_STOPPING,
  TURBO_FLOW_CNET_STREAM_SINK_STOPPED,
  TURBO_FLOW_CNET_STREAM_SINK_DETACHED
} turbo_flow_cnet_stream_sink_state_t;

typedef struct turbo_flow_cnet_stream_sink_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *adapter_name;
  const char *uri;
  const cnet_client_config *client;
  const cnet_stream_socket_options *socket_options;
  const cnet_tls_client_config *tls;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  /** Native stop wait per Flow stop attempt; a timeout requires explicit retry. */
  uint32_t stop_timeout_ms;
} turbo_flow_cnet_stream_sink_config_t;

#define TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT                                                    \
  {sizeof(turbo_flow_cnet_stream_sink_config_t),                                                   \
   TURBO_FLOW_CNET_STREAM_SINK_API_VERSION,                                                        \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_MAX_MESSAGE_BYTES,                                          \
   TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_COMMAND_CAPACITY,                                           \
   TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_MAX_STEPS,                                                  \
   TURBO_FLOW_CNET_STREAM_SINK_DEFAULT_STOP_TIMEOUT_MS}

typedef struct turbo_flow_cnet_stream_sink_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_cnet_stream_sink_state_t state;
  int status;
  int native_status;
  cnet_connection connection;
  size_t active_requests;
  uint64_t messages_sent;
  uint64_t bytes_sent;
} turbo_flow_cnet_stream_sink_snapshot_t;

#define TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT                                                  \
  {sizeof(turbo_flow_cnet_stream_sink_snapshot_t),                                                 \
   TURBO_FLOW_CNET_STREAM_SINK_API_VERSION,                                                        \
   TURBO_FLOW_CNET_STREAM_SINK_REGISTERED,                                                         \
   SALTS_OK,                                                                                       \
   0,                                                                                              \
   {0u, 0u},                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/**
 * Register one Flow-owned adapter binding; the returned progress handle is caller-driven.
 * Native stop/destroy errors propagate through turbo_flow_stop(). A failed stop
 * retains the binding and remaining native/Actor owners for explicit retry.
 * Pending Flow claims settle with SALTS_ESHUTDOWN when native drain fails;
 * active_requests still counts retained Actor tokens until real native terminal
 * delivery. Keep both Flow and this handle alive until stop succeeds.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_stream_sink_register(const turbo_flow_cnet_stream_sink_config_t *config,
                                     turbo_flow_cnet_stream_sink_t **sink_out);

/** Drive bounded IO Actor work, CNet progress, completion delivery, and acknowledgement. */
TURBO_FLOW_C_API int
turbo_flow_cnet_stream_sink_poll(turbo_flow_cnet_stream_sink_t *sink, uint32_t timeout_ms,
                                 turbo_flow_cnet_stream_sink_snapshot_t *snapshot);

/** Copy state without advancing the caller-owned progress lane. */
TURBO_FLOW_C_API int
turbo_flow_cnet_stream_sink_snapshot(const turbo_flow_cnet_stream_sink_t *sink,
                                     turbo_flow_cnet_stream_sink_snapshot_t *snapshot);

/** Release the progress handle after its Flow registry has been reset or destroyed. */
TURBO_FLOW_C_API int turbo_flow_cnet_stream_sink_destroy(turbo_flow_cnet_stream_sink_t *sink);

#define TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION 1u
#define TURBO_FLOW_CNET_DATAGRAM_SINK_DEFAULT_ACTOR_COMMAND_CAPACITY 64u
#define TURBO_FLOW_CNET_DATAGRAM_SINK_DEFAULT_ACTOR_MAX_STEPS_PER_POLL 64u
#define TURBO_FLOW_CNET_DATAGRAM_SINK_DEFAULT_STOP_TIMEOUT_MS 5000u

typedef struct turbo_flow_cnet_datagram_sink_s turbo_flow_cnet_datagram_sink_t;

typedef enum turbo_flow_cnet_datagram_sink_state_e {
  TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED = 1,
  TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING,
  TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED,
  TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING,
  TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED,
  TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED
} turbo_flow_cnet_datagram_sink_state_t;

typedef struct turbo_flow_cnet_datagram_sink_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *adapter_name;
  const cnet_datagram_config *datagram;
  cnet_datagram_peer peer;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  /** Native stop wait per Flow stop attempt; a timeout requires explicit retry. */
  uint32_t stop_timeout_ms;
} turbo_flow_cnet_datagram_sink_config_t;

#define TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT                                                  \
  {sizeof(turbo_flow_cnet_datagram_sink_config_t),                                                 \
   TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION,                                                      \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   {(cnet_datagram_address_family)0, 0u, 0u, {0}},                                                \
   CNET_DATAGRAM_MAX_PAYLOAD_BYTES,                                                                \
   TURBO_FLOW_CNET_DATAGRAM_SINK_DEFAULT_ACTOR_COMMAND_CAPACITY,                                   \
   TURBO_FLOW_CNET_DATAGRAM_SINK_DEFAULT_ACTOR_MAX_STEPS_PER_POLL,                                 \
   TURBO_FLOW_CNET_DATAGRAM_SINK_DEFAULT_STOP_TIMEOUT_MS}

typedef struct turbo_flow_cnet_datagram_sink_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_cnet_datagram_sink_state_t state;
  int status;
  uint16_t bound_port;
  size_t active_requests;
  uint64_t messages_sent;
  uint64_t bytes_sent;
} turbo_flow_cnet_datagram_sink_snapshot_t;

#define TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT                                                \
  {sizeof(turbo_flow_cnet_datagram_sink_snapshot_t),                                               \
   TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION,                                                      \
   TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED,                                                       \
   SALTS_OK,                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/**
 * Register one fixed-peer raw UDP terminal sink before graph compilation.
 * Each Flow stop attempts native drain once. Stop/destroy failure retains the
 * binding and remaining owners for explicit retry. Pending Flow claims settle
 * with SALTS_ESHUTDOWN on failed drain; active_requests includes retained Actor
 * tokens until real native terminals arrive. Keep Flow and this handle alive
 * until stop succeeds. Native progress remains on one non-overlapping owner.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_datagram_sink_register(const turbo_flow_cnet_datagram_sink_config_t *config,
                                       turbo_flow_cnet_datagram_sink_t **sink_out);

/** Advance owner-thread Actor and CNet datagram progress. */
TURBO_FLOW_C_API int
turbo_flow_cnet_datagram_sink_poll(turbo_flow_cnet_datagram_sink_t *sink, uint32_t timeout_ms,
                                   turbo_flow_cnet_datagram_sink_snapshot_t *snapshot);

/** Copy a synchronized snapshot without advancing the native progress owner. */
TURBO_FLOW_C_API int
turbo_flow_cnet_datagram_sink_snapshot(const turbo_flow_cnet_datagram_sink_t *sink,
                                       turbo_flow_cnet_datagram_sink_snapshot_t *snapshot);

/** Release the outer handle after flow reset/destroy detached the registration. */
TURBO_FLOW_C_API int turbo_flow_cnet_datagram_sink_destroy(turbo_flow_cnet_datagram_sink_t *sink);

#define TURBO_FLOW_CNET_PACKET_SINK_API_VERSION 1u
#define TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_SEND_CAPACITY 64u
#define TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_MAX_MESSAGE_BYTES CNET_DATAGRAM_MAX_PAYLOAD_BYTES
#define TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_ACTOR_COMMAND_CAPACITY 64u
#define TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_ACTOR_MAX_STEPS_PER_POLL 64u
#define TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_STOP_TIMEOUT_MS 5000u

typedef struct turbo_flow_cnet_packet_sink_s turbo_flow_cnet_packet_sink_t;

typedef enum turbo_flow_cnet_packet_sink_state_e {
  TURBO_FLOW_CNET_PACKET_SINK_REGISTERED = 1,
  TURBO_FLOW_CNET_PACKET_SINK_RUNNING,
  TURBO_FLOW_CNET_PACKET_SINK_FAILED,
  TURBO_FLOW_CNET_PACKET_SINK_STOPPING,
  TURBO_FLOW_CNET_PACKET_SINK_STOPPED,
  TURBO_FLOW_CNET_PACKET_SINK_DETACHED
} turbo_flow_cnet_packet_sink_state_t;

/**
 * Fixed-peer packet terminal configuration consumed synchronously by register.
 *
 * The sink copies `endpoint`, including its embedded security material, and
 * copies `endpoint->datagram.host`. The endpoint and observer callbacks in the
 * supplied configuration must be unowned and empty. Each accepted graph claim
 * is retained until the matching CNet tagged logical-send terminal arrives or
 * failed shutdown settles it with SALTS_ESHUTDOWN. `stop_timeout_ms` bounds one
 * native stop attempt per Flow stop. On failure the host retains the Flow and
 * sink and explicitly retries stop before reset/destroy. Native copied payloads,
 * operation slots and Actor tokens remain alive until their true terminal;
 * late terminals only retire already-settled claims, without counting success.
 */
typedef struct turbo_flow_cnet_packet_sink_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *adapter_name;
  const cnet_packet_endpoint_config *endpoint;
  cnet_datagram_peer peer;
  uint32_t conversation;
  size_t send_capacity;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  uint32_t stop_timeout_ms;
} turbo_flow_cnet_packet_sink_config_t;

#define TURBO_FLOW_CNET_PACKET_SINK_CONFIG_V1_SIZE sizeof(turbo_flow_cnet_packet_sink_config_t)
#define TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT                                                   \
  {TURBO_FLOW_CNET_PACKET_SINK_CONFIG_V1_SIZE,                                                    \
   TURBO_FLOW_CNET_PACKET_SINK_API_VERSION,                                                       \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL,                                                                                          \
   {(cnet_datagram_address_family)0, 0u, 0u, {0}},                                               \
   0u,                                                                                            \
   TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_SEND_CAPACITY,                                             \
   TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_MAX_MESSAGE_BYTES,                                         \
   TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_ACTOR_COMMAND_CAPACITY,                                    \
   TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_ACTOR_MAX_STEPS_PER_POLL,                                  \
   TURBO_FLOW_CNET_PACKET_SINK_DEFAULT_STOP_TIMEOUT_MS}

typedef struct turbo_flow_cnet_packet_sink_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_cnet_packet_sink_state_t state;
  int status;
  cnet_packet_protocol protocol;
  uint16_t bound_port;
  cnet_packet_session session;
  int session_open;
  /** Retained Actor tokens; may remain nonzero after shutdown settled claims. */
  size_t active_requests;
  uint64_t messages_sent;
  uint64_t bytes_sent;
  uint64_t terminals_failed;
} turbo_flow_cnet_packet_sink_snapshot_t;

#define TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_V1_SIZE                                              \
  sizeof(turbo_flow_cnet_packet_sink_snapshot_t)
#define TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT                                                 \
  {TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_V1_SIZE,                                                  \
   TURBO_FLOW_CNET_PACKET_SINK_API_VERSION,                                                       \
   TURBO_FLOW_CNET_PACKET_SINK_REGISTERED,                                                        \
   SALTS_OK,                                                                                      \
   (cnet_packet_protocol)0,                                                                       \
   0u,                                                                                            \
   {0u, 0u},                                                                                      \
   0,                                                                                             \
   0u,                                                                                            \
   0u,                                                                                            \
   0u,                                                                                            \
   0u}

/** Register one fixed-peer UDP, KCP, secure-KCP, or secure-KCP/FEC terminal sink. */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_sink_register(const turbo_flow_cnet_packet_sink_config_t *config,
                                     turbo_flow_cnet_packet_sink_t **sink_out);

/**
 * Advance the single-owner CFlow IO Actor and CNet packet endpoint.
 *
 * Concurrent submitters are supported. Poll calls are single-owner and return
 * `SALTS_EBUSY` when another poll owns the lane. Stop waits for that owner and
 * prevents new submissions before destroying Actor, Executor, and endpoint.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_sink_poll(turbo_flow_cnet_packet_sink_t *sink, uint32_t timeout_ms,
                                 turbo_flow_cnet_packet_sink_snapshot_t *snapshot);

/**
 * Copy state without advancing the owner lane.
 * Returns `SALTS_EBUSY` while poll, start, or stop owns that lane, including
 * reentrant calls from terminal completion callbacks.
 */
TURBO_FLOW_C_API int
turbo_flow_cnet_packet_sink_snapshot(const turbo_flow_cnet_packet_sink_t *sink,
                                     turbo_flow_cnet_packet_sink_snapshot_t *snapshot);

/**
 * Release the outer handle after Flow reset/destroy detached the registration.
 * The caller must serialize destroy with all other handle calls.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_packet_sink_destroy(turbo_flow_cnet_packet_sink_t *sink);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CNET_H */
