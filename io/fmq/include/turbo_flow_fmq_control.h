#ifndef TURBO_FLOW_FMQ_CONTROL_H
#define TURBO_FLOW_FMQ_CONTROL_H

#include "turbo_flow_config.h"
#include "turbo_flow_control.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION 1u
#define TURBO_FLOW_FMQ_CONTROL_TARGET_MAX 127u
#define TURBO_FLOW_FMQ_CONTROL_DEDUP_MAX 4096u
#define TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE 64u
#define TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE 88u
#define TURBO_FLOW_FMQ_CONTROL_REPLY_MESSAGE_MAX 159u
#define TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE                                                    \
  (TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE + TURBO_FLOW_FMQ_CONTROL_TARGET_MAX +                \
   TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX + TURBO_FLOW_CONTROL_NAME_MAX +                             \
   TURBO_FLOW_CONTROL_EXPR_MAX + 2u * TURBO_FLOW_ENDPOINT_MAX)
#define TURBO_FLOW_FMQ_CONTROL_REPLY_MAX_SIZE                                                      \
  (TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE + TURBO_FLOW_FMQ_CONTROL_REPLY_MESSAGE_MAX)

typedef enum turbo_flow_fmq_control_operation_e {
  TURBO_FLOW_FMQ_CONTROL_STATUS = 1,
  TURBO_FLOW_FMQ_CONTROL_EXECUTE
} turbo_flow_fmq_control_operation_t;

/** Canonical pointer-free projection of one local control command. */
typedef struct turbo_flow_fmq_control_command_s {
  size_t size;
  turbo_flow_control_kind_t kind;
  uint64_t timeout_ms;
  char target[TURBO_FLOW_CONTROL_NAME_MAX + 1u];
  turbo_flow_pool_kind_t pool_kind;
  uint32_t parallelism;
  turbo_flow_adapter_command_kind_t adapter_kind;
  char endpoint_host[TURBO_FLOW_ENDPOINT_MAX + 1u];
  char endpoint_path[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int endpoint_port;
  char condition[TURBO_FLOW_CONTROL_EXPR_MAX + 1u];
} turbo_flow_fmq_control_command_t;

#define TURBO_FLOW_FMQ_CONTROL_COMMAND_INIT                                                        \
  {sizeof(turbo_flow_fmq_control_command_t), 0, 0u, {0}, 0, 0u, 0, {0}, {0}, 0, {0}}

/** Pointer-free application request carried as one FMQ REQ DATA payload. */
typedef struct turbo_flow_fmq_control_request_s {
  size_t size;
  uint32_t version;
  turbo_flow_fmq_control_operation_t operation;
  uint64_t request_id;
  char target[TURBO_FLOW_FMQ_CONTROL_TARGET_MAX + 1u];
  char idempotency_key[TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX + 1u];
  turbo_flow_fmq_control_command_t command;
} turbo_flow_fmq_control_request_t;

#define TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT                                                        \
  {sizeof(turbo_flow_fmq_control_request_t),                                                       \
   TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION,                                                        \
   0,                                                                                              \
   0u,                                                                                             \
   {0},                                                                                            \
   {0},                                                                                            \
   TURBO_FLOW_FMQ_CONTROL_COMMAND_INIT}

/** Terminal application acknowledgement returned in one FMQ REP DATA payload. */
typedef struct turbo_flow_fmq_control_reply_s {
  size_t size;
  uint32_t version;
  uint64_t request_id;
  int status;
  int replayed;
  turbo_flow_runtime_snapshot_t runtime;
  turbo_flow_error_t error;
} turbo_flow_fmq_control_reply_t;

#define TURBO_FLOW_FMQ_CONTROL_REPLY_INIT                                                          \
  {sizeof(turbo_flow_fmq_control_reply_t),                                                         \
   TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION,                                                        \
   0u,                                                                                             \
   TURBO_OK,                                                                                       \
   0,                                                                                              \
   {0},                                                                                            \
   {0}}

/** Bounded server state resolved from a `kind: fmq_control` YAML channel. */
typedef struct turbo_flow_fmq_control_config_s {
  size_t size;
  uint32_t version;
  char target[TURBO_FLOW_FMQ_CONTROL_TARGET_MAX + 1u];
  size_t dedup_capacity;
  size_t max_request_bytes;
} turbo_flow_fmq_control_config_t;

#define TURBO_FLOW_FMQ_CONTROL_CONFIG_INIT                                                         \
  {sizeof(turbo_flow_fmq_control_config_t),                                                        \
   TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION,                                                        \
   {0},                                                                                            \
   TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX,                                                        \
   TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE}

typedef struct turbo_flow_fmq_control_service_s turbo_flow_fmq_control_service_t;

/**
 * Encode one Control V1 request in network byte order.
 *
 * @param request Valid caller-owned pointer-free request.
 * @param out Destination, or NULL to query the required size.
 * @param capacity Destination capacity.
 * @param out_len Receives the exact required size.
 * @return TURBO_OK, TURBO_EINVAL, or TURBO_ENOSPC. No partial envelope is emitted.
 * @code
 * turbo_flow_fmq_control_request_t request = TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
 * request.operation = TURBO_FLOW_FMQ_CONTROL_STATUS;
 * request.request_id = 1;
 * memcpy(request.target, "data-plane", sizeof("data-plane"));
 * size_t required = 0;
 * int rc = turbo_flow_fmq_control_request_encode(&request, NULL, 0, &required);
 * @endcode
 */
CXX_C_API int turbo_flow_fmq_control_request_encode(const turbo_flow_fmq_control_request_t *request,
                                                    uint8_t *out, size_t capacity, size_t *out_len);

/**
 * Decode exactly one Control V1 request.
 *
 * @param data Immutable envelope bytes.
 * @param data_len Exact byte count; trailing bytes are rejected.
 * @param out Caller-owned output initialized with TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT.
 * @return TURBO_OK, TURBO_EINVAL for invalid ABI/arguments, or TURBO_EPROTO for malformed,
 *         non-canonical, or unsupported bytes. `out` is unchanged on failure.
 */
CXX_C_API int turbo_flow_fmq_control_request_decode(const uint8_t *data, size_t data_len,
                                                    turbo_flow_fmq_control_request_t *out);

/**
 * Encode one terminal Control V1 application reply.
 *
 * @param reply Reply initialized with TURBO_FLOW_FMQ_CONTROL_REPLY_INIT.
 * @param out Destination, or NULL to query size.
 * @param capacity Destination capacity.
 * @param out_len Receives the exact required size.
 * @return TURBO_OK, TURBO_EINVAL, or TURBO_ENOSPC; no partial envelope is emitted.
 */
CXX_C_API int turbo_flow_fmq_control_reply_encode(const turbo_flow_fmq_control_reply_t *reply,
                                                  uint8_t *out, size_t capacity, size_t *out_len);

/**
 * Decode exactly one terminal Control V1 application reply.
 *
 * @param data Immutable reply bytes.
 * @param data_len Exact byte count.
 * @param out Caller-owned output initialized with TURBO_FLOW_FMQ_CONTROL_REPLY_INIT.
 * @return TURBO_OK, TURBO_EINVAL for invalid ABI/arguments, or TURBO_EPROTO for invalid bytes.
 */
CXX_C_API int turbo_flow_fmq_control_reply_decode(const uint8_t *data, size_t data_len,
                                                  turbo_flow_fmq_control_reply_t *out);

/**
 * Resolve a Control service channel from the immutable YAML projection.
 *
 * Required YAML fields are `protocol_version: 1` and non-empty `target`; optional bounded
 * fields are `dedup_capacity` and `max_request_bytes`.
 *
 * @param resolved Immutable result of turbo_flow_config_resolve_yaml().
 * @param channel_name Name below `channels`.
 * @param out Caller-owned config initialized with TURBO_FLOW_FMQ_CONTROL_CONFIG_INIT.
 * @param error Receives a stable YAML path and diagnostic on validation failure.
 * @return TURBO_OK or a Turbo error such as TURBO_ENOENT/TURBO_EINVAL/TURBO_ENOTSUP.
 */
CXX_C_API int turbo_flow_fmq_control_config_resolve(const turbo_flow_resolved_config_t *resolved,
                                                    const char *channel_name,
                                                    turbo_flow_fmq_control_config_t *out,
                                                    turbo_flow_config_error_t *error);

/**
 * Create the synchronous command owner for one target flow.
 *
 * The service borrows `target_flow`; destroy the service before the target. Calls are
 * serialized internally, but the host must also serialize any target lifecycle calls made
 * outside this service. The management REP graph must be a different flow from `target_flow`.
 *
 * @param target_flow Borrowed managed flow.
 * @param config Valid pointer-free service policy.
 * @param out Receives the owned service on success and NULL on failure.
 * @return TURBO_OK, TURBO_EINVAL, or TURBO_ENOMEM.
 */
CXX_C_API int turbo_flow_fmq_control_service_create(turbo_flow_t *target_flow,
                                                    const turbo_flow_fmq_control_config_t *config,
                                                    turbo_flow_fmq_control_service_t **out);
/** Destroy a service; NULL is accepted. No target flow ownership is transferred. */
CXX_C_API void turbo_flow_fmq_control_service_destroy(turbo_flow_fmq_control_service_t *service);

/**
 * Process one protocol request and always produce a terminal application reply.
 *
 * @return TURBO_OK when `reply` was produced, including malformed requests and failed commands;
 *         argument/state failures preventing protocol processing are returned directly.
 * Inspect `reply.status` for the management action result (the second ACK layer).
 * @param service Serialized owner created by turbo_flow_fmq_control_service_create().
 * @param request Immutable wire bytes.
 * @param request_len Request byte count, bounded by the resolved config.
 * @param reply Output initialized with TURBO_FLOW_FMQ_CONTROL_REPLY_INIT.
 */
CXX_C_API int turbo_flow_fmq_control_service_execute(turbo_flow_fmq_control_service_t *service,
                                                     const uint8_t *request, size_t request_len,
                                                     turbo_flow_fmq_control_reply_t *reply);

/**
 * Flow stage for `REP source -> control stage -> REP sink`.
 *
 * It replaces the message payload with a Control V1 reply and returns TURBO_OK whenever the
 * reply was generated. Allocation/invalid-context failures are transport-dispatch failures.
 * Do not attach a worker pool to this stage; the service is the target's serialized owner.
 *
 * Example DSL:
 * @code
 * source request adapter fmq.control.rep
 * stage control
 * stage reply adapter fmq.control.rep
 * stage main { request -> control -> reply }
 * @endcode
 * @param msg Mutable REP request message; protocol route/context are preserved.
 * @param ctx A turbo_flow_fmq_control_service_t pointer.
 * @return TURBO_OK when a reply payload is ready, or TURBO_EINVAL/TURBO_ENOMEM when no reply can
 *         be generated. Management failures are carried in the encoded reply, not returned here.
 */
CXX_C_API int turbo_flow_fmq_control_stage(turbo_flow_msg_t *msg, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_CONTROL_H */
