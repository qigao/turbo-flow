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
 * @param config Valid size/version configuration; borrowed only until return.
 * @param source_out Receives the owning opaque handle on success and NULL on failure.
 * @return SALTS_OK, a validation/configuration error, a bounded-resource error,
 * or the synchronous CNet connection-admission error.
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
 * @param timeout_ms Maximum CNet drain duration; retry after SALTS_ETIMEDOUT.
 * @return SALTS_OK, SALTS_EALREADY, SALTS_ETIMEDOUT, or a terminal drain error.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_stream_source_stop(turbo_flow_cnet_stream_source_t *source,
                                                        uint32_t timeout_ms);

/**
 * Release a successfully stopped source.
 *
 * @param source Stopped source owned by the calling thread.
 * @return SALTS_OK, SALTS_EINVAL, or SALTS_EBUSY before successful stop.
 */
TURBO_FLOW_C_API int turbo_flow_cnet_stream_source_destroy(turbo_flow_cnet_stream_source_t *source);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CNET_H */
