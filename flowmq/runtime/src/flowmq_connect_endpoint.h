#ifndef FLOWMQ_CONNECT_ENDPOINT_H
#define FLOWMQ_CONNECT_ENDPOINT_H

#include "flow_coronet_runtime.h"
#include "flowmq_coronet_transport.h"
#include "flowmq_peer_session.h"
#include "flowmq_protocol.h"
#include "flowmq_security.h"

typedef struct flowmq_connect_endpoint_s flowmq_connect_endpoint_t;

typedef enum flowmq_connect_endpoint_connection_state_e {
  FLOWMQ_ENDPOINT_CONNECTION_STOPPED = 0,
  FLOWMQ_ENDPOINT_CONNECTION_CONNECTING,
  FLOWMQ_ENDPOINT_CONNECTION_READY,
  FLOWMQ_ENDPOINT_CONNECTION_BACKOFF,
  FLOWMQ_ENDPOINT_CONNECTION_FAILED
} flowmq_connect_endpoint_connection_state_t;

typedef enum flowmq_connect_endpoint_event_kind_e {
  FLOWMQ_ENDPOINT_EVENT_RECONNECT_SCHEDULED = 1,
  FLOWMQ_ENDPOINT_EVENT_RECONNECT_SUCCEEDED,
  FLOWMQ_ENDPOINT_EVENT_RECONNECT_FAILED,
  FLOWMQ_ENDPOINT_EVENT_HEARTBEAT_TIMEOUT
} flowmq_connect_endpoint_event_kind_t;

typedef struct flowmq_connect_endpoint_event_s {
  flowmq_connect_endpoint_event_kind_t kind;
  int status;
  uint64_t delay_ms;
  tstr_v peer_identity;
  tstr_v peer_topic;
} flowmq_connect_endpoint_event_t;

/* Decoded frames and their views remain valid only for the callback. */
typedef int (*flowmq_connect_endpoint_frame_fn)(void *ctx, const flowmq_protocol_frame_t *frame,
                                                uint64_t generation);
typedef int (*flowmq_connect_endpoint_receive_ready_fn)(void *ctx);
typedef void (*flowmq_connect_endpoint_state_fn)(void *ctx,
                                                 flowmq_connect_endpoint_connection_state_t state,
                                                 int status, size_t connections_current);
typedef void (*flowmq_connect_endpoint_event_fn)(void *ctx,
                                                 const flowmq_connect_endpoint_event_t *event);

typedef struct flowmq_connect_endpoint_config_s {
  flowmq_coronet_transport_t transport;
  flowmq_protocol_pattern_t pattern;
  const char *host;
  const char *path;
  const char *topic;
  const char *identity;
  int port;
  size_t max_frame_size;
  tf_coronet_socket_timeout_config_t timeouts;
  tf_coronet_socket_options_t socket_options;
  tf_coronet_udp_options_t udp_options;
  turbo_kcp_fec_config_t kcp_fec;
  int kcp_fec_configured;
  int initial_subscription_configured;
  uint64_t reconnect_initial_ms;
  uint64_t reconnect_max_ms;
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  coro_context_t *context;
  int drive_context;
  int own_context;
  flowmq_connect_endpoint_frame_fn on_frame;
  flowmq_connect_endpoint_receive_ready_fn receive_ready;
  flowmq_connect_endpoint_state_fn on_state;
  flowmq_connect_endpoint_event_fn on_event;
  void *callback_ctx;
  /** Borrowed adapter-owned immutable security runtime; NULL selects trusted v3 mode. */
  flowmq_security_binding_runtime_t *security;
} flowmq_connect_endpoint_config_t;

int flowmq_connect_endpoint_create(const flowmq_connect_endpoint_config_t *config,
                                   flowmq_connect_endpoint_t **out);
int flowmq_connect_endpoint_start(flowmq_connect_endpoint_t *endpoint, uint64_t timeout_ns);
void flowmq_connect_endpoint_stop(flowmq_connect_endpoint_t *endpoint);
void flowmq_connect_endpoint_destroy(flowmq_connect_endpoint_t *endpoint);

coro_context_t *flowmq_connect_endpoint_context(flowmq_connect_endpoint_t *endpoint);
int flowmq_connect_endpoint_send(flowmq_connect_endpoint_t *endpoint, const char *encoded,
                                 size_t encoded_size);
int flowmq_connect_endpoint_sendv(flowmq_connect_endpoint_t *endpoint, const turbo_iovec_t *iov,
                                  size_t iovcnt);
int flowmq_connect_endpoint_interrupt(flowmq_connect_endpoint_t *endpoint, int status);
int flowmq_connect_endpoint_update_endpoint(flowmq_connect_endpoint_t *endpoint, const char *host,
                                            int port, const char *path);
int flowmq_connect_endpoint_request_begin(flowmq_connect_endpoint_t *endpoint,
                                          uint64_t correlation_id, uint64_t *generation);
int flowmq_connect_endpoint_request_finish(flowmq_connect_endpoint_t *endpoint, uint64_t generation,
                                           uint64_t correlation_id,
                                           flowmq_peer_exchange_state_t terminal_state);
int flowmq_connect_endpoint_exchange_snapshot(const flowmq_connect_endpoint_t *endpoint,
                                              flowmq_peer_exchange_state_t *state,
                                              uint64_t *generation, uint64_t *correlation_id);

#endif /* FLOWMQ_CONNECT_ENDPOINT_H */
