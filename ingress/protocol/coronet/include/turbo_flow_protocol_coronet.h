#ifndef TURBO_FLOW_PROTOCOL_CORONET_H
#define TURBO_FLOW_PROTOCOL_CORONET_H

#include "CoroNet/turbo_coro_socket.h"
#include "turbo_flow_coronet_execution.h"
#include "turbo_flow_protocol_runtime.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_CORONET_ABI_VERSION 1u
#define TURBO_FLOW_PROTOCOL_CORONET_HOST_MAX 255u
#define TURBO_FLOW_PROTOCOL_CORONET_PATH_MAX 255u
#define TURBO_FLOW_PROTOCOL_CORONET_SUBPROTOCOL_MAX 31u
#define TURBO_FLOW_PROTOCOL_CORONET_TLS_PATH_MAX 511u
#define TURBO_FLOW_PROTOCOL_CORONET_DEFAULT_TIMEOUT_MS 30000u
#define TURBO_FLOW_PROTOCOL_CORONET_CALL_TIMEOUT_NS UINT64_C(5000000000)

typedef struct turbo_flow_protocol_coronet_server_s turbo_flow_protocol_coronet_server_t;

typedef enum turbo_flow_protocol_coronet_transport_e {
  TURBO_FLOW_PROTOCOL_CORONET_UDP = 1,
  TURBO_FLOW_PROTOCOL_CORONET_TCP,
  TURBO_FLOW_PROTOCOL_CORONET_TLS,
  TURBO_FLOW_PROTOCOL_CORONET_WS,
  TURBO_FLOW_PROTOCOL_CORONET_WSS
} turbo_flow_protocol_coronet_transport_t;

typedef struct turbo_flow_protocol_coronet_identity_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_coronet_transport_t transport;
  uint64_t session_id;
  uint64_t generation;
  /**
   * Borrowed accepted socket. The resolver runs on its owning event-loop
   * thread and may query verified TLS identity. It must not retain the pointer.
   */
  const coro_socket_t *socket;
} turbo_flow_protocol_coronet_identity_request_t;

#define TURBO_FLOW_PROTOCOL_CORONET_IDENTITY_REQUEST_INIT                                           \
  {sizeof(turbo_flow_protocol_coronet_identity_request_t),                                          \
   TURBO_FLOW_PROTOCOL_CORONET_ABI_VERSION,                                                         \
   0,                                                                                              \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL}

/**
 * Resolve one authenticated transport identity into a stable device ID.
 *
 * The callback writes a NUL-terminated stable device ID. GB/T 32960 and JT/T 808
 * may omit the callback because their codec derives identity from each frame.
 */
typedef int (*turbo_flow_protocol_coronet_identity_fn)(
    void *ctx, const turbo_flow_protocol_coronet_identity_request_t *request, char *device_id,
    size_t capacity);

typedef struct turbo_flow_protocol_coronet_tls_config_s {
  size_t size;
  uint32_t abi_version;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
  const char *ca_file;
  const char *cipher_list;
  turbo_tls_client_auth_t client_auth;
} turbo_flow_protocol_coronet_tls_config_t;

#define TURBO_FLOW_PROTOCOL_CORONET_TLS_CONFIG_INIT                                                 \
  {sizeof(turbo_flow_protocol_coronet_tls_config_t),                                                \
   TURBO_FLOW_PROTOCOL_CORONET_ABI_VERSION,                                                         \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_TLS_CLIENT_AUTH_NONE}

typedef struct turbo_flow_protocol_coronet_config_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_coronet_execution_binding_t execution;
  turbo_flow_protocol_t *protocol;
  turbo_flow_protocol_coronet_transport_t transport;
  const char *host;
  int port;
  const char *path;
  const char *subprotocol;
  uint64_t socket_timeout_ms;
  /**
   * LwM2M normally requires DTLS. Set this only for an explicitly selected
   * NoSec/test deployment over UDP; no implicit downgrade is performed.
   */
  uint8_t allow_insecure_lwm2m;
  turbo_flow_protocol_coronet_tls_config_t tls;
  turbo_flow_protocol_runtime_config_t runtime;
  turbo_flow_protocol_runtime_ops_t runtime_ops;
  void *runtime_ctx;
  turbo_flow_protocol_coronet_identity_fn resolve_identity;
  void *identity_ctx;
} turbo_flow_protocol_coronet_config_t;

#define TURBO_FLOW_PROTOCOL_CORONET_CONFIG_INIT                                                     \
  {sizeof(turbo_flow_protocol_coronet_config_t),                                                    \
   TURBO_FLOW_PROTOCOL_CORONET_ABI_VERSION,                                                         \
   {sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE, NULL,    \
    NULL, 0u, 0u},                                                                                 \
   NULL,                                                                                           \
   TURBO_FLOW_PROTOCOL_CORONET_TCP,                                                                 \
   "127.0.0.1",                                                                                    \
   0,                                                                                              \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_PROTOCOL_CORONET_DEFAULT_TIMEOUT_MS,                                                  \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_CORONET_TLS_CONFIG_INIT,                                                     \
   TURBO_FLOW_PROTOCOL_RUNTIME_CONFIG_INIT,                                                         \
   TURBO_FLOW_PROTOCOL_RUNTIME_OPS_INIT,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef struct turbo_flow_protocol_coronet_snapshot_s {
  size_t size;
  uint32_t abi_version;
  uint8_t started;
  uint8_t stopping;
  uint8_t network_stopped;
  size_t active_handlers;
  turbo_flow_protocol_runtime_snapshot_t runtime;
} turbo_flow_protocol_coronet_snapshot_t;

#define TURBO_FLOW_PROTOCOL_CORONET_SNAPSHOT_INIT                                                   \
  {sizeof(turbo_flow_protocol_coronet_snapshot_t),                                                  \
   TURBO_FLOW_PROTOCOL_CORONET_ABI_VERSION,                                                         \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_RUNTIME_SNAPSHOT_INIT}

/**
 * Create a bounded transport owner. The protocol is borrowed and must outlive
 * the server. Configuration strings and callback tables are copied.
 *
 * @param config Validated transport/runtime configuration.
 * @param out Receives the owned opaque server on success.
 * @return TURBO_OK; TURBO_EINVAL for an invalid ABI/configuration;
 *         TURBO_ENOTSUP for an incompatible protocol/transport;
 *         TURBO_EPERM for an implicit LwM2M downgrade; or TURBO_ENOMEM.
 *
 * @code
 * turbo_flow_protocol_coronet_config_t cfg =
 *     TURBO_FLOW_PROTOCOL_CORONET_CONFIG_INIT;
 * cfg.protocol = opened_protocol;
 * cfg.transport = TURBO_FLOW_PROTOCOL_CORONET_UDP;
 * cfg.host = "0.0.0.0";
 * cfg.port = 1884;
 * cfg.runtime_ops.publish = publish_to_graph;
 * cfg.resolve_identity = resolve_authenticated_device;
 * turbo_flow_protocol_coronet_server_create(&cfg, &server);
 * turbo_flow_protocol_coronet_server_start(server);
 * @endcode
 */
CXX_C_API int
turbo_flow_protocol_coronet_server_create(const turbo_flow_protocol_coronet_config_t *config,
                                         turbo_flow_protocol_coronet_server_t **out);

/**
 * Start execution and bind the configured listener.
 *
 * @param server Owned server returned by create.
 * @return TURBO_OK, TURBO_EALREADY, or a bind/TLS/transport error.
 */
CXX_C_API int turbo_flow_protocol_coronet_server_start(turbo_flow_protocol_coronet_server_t *server);

/**
 * Close network admission while preserving admitted graph/sink settlement.
 *
 * @param server Started server.
 * @return TURBO_OK or an execution/transport error.
 */
CXX_C_API int
turbo_flow_protocol_coronet_server_begin_shutdown(turbo_flow_protocol_coronet_server_t *server);

/**
 * Wait for network handlers and admitted settlements.
 *
 * The CoroNet execution context remains live throughout this wait. Timeout
 * returns TURBO_ETIMEDOUT and does not destroy pending ownership.
 *
 * @param server Server after begin_shutdown.
 * @param timeout_ms Positive wait bound.
 * @return TURBO_OK, TURBO_ETIMEDOUT, or an execution/runtime error.
 */
CXX_C_API int
turbo_flow_protocol_coronet_server_wait_shutdown(turbo_flow_protocol_coronet_server_t *server,
                                                uint64_t timeout_ms);

/**
 * Cancel remaining ownership, then wait for the network close fence.
 *
 * @param server Started server.
 * @param status Nonzero cancellation/error status delivered to callbacks.
 * @param timeout_ms Positive wait bound.
 * @return TURBO_OK, TURBO_ETIMEDOUT, or an execution/runtime error.
 */
CXX_C_API int
turbo_flow_protocol_coronet_server_force_shutdown(turbo_flow_protocol_coronet_server_t *server,
                                                 int status, uint64_t timeout_ms);

/**
 * Marshal one asynchronous graph/sink settlement to the owning event loop.
 *
 * @param server Started server.
 * @param delivery_id ID supplied to the publish callback.
 * @param status Graph/sink settlement result.
 * @param result Receives any stream frames admitted after settlement.
 * @return TURBO_OK, TURBO_ENOENT for a stale delivery, or an execution/runtime
 *         error.
 */
CXX_C_API int turbo_flow_protocol_coronet_server_settle(turbo_flow_protocol_coronet_server_t *server,
                                                       uint64_t delivery_id, int status,
                                                       turbo_flow_protocol_feed_result_t *result);

/**
 * Copy a point-in-time transport/runtime snapshot.
 *
 * @param server Created server.
 * @param out Caller-initialized snapshot receiving the copy.
 * @return TURBO_OK or TURBO_EINVAL/execution error.
 */
CXX_C_API int
turbo_flow_protocol_coronet_server_snapshot(turbo_flow_protocol_coronet_server_t *server,
                                           turbo_flow_protocol_coronet_snapshot_t *out);

/**
 * Destroy only after a completed shutdown. An unstarted server may be
 * destroyed directly. Returns TURBO_EBUSY while ownership is still live.
 *
 * @param server Owned server, or NULL.
 * @return TURBO_OK or TURBO_EBUSY.
 */
CXX_C_API int
turbo_flow_protocol_coronet_server_destroy(turbo_flow_protocol_coronet_server_t *server);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_CORONET_H */
