#include "turbo_flow_cnet.h"

#include <cflow/executor.h>
#include <cflow/io_actor.h>

#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

enum { STREAM_SINK_SINGLE_REQUEST_LEASE = 1u };

typedef struct stream_sink_operation_s {
  turbo_flow_async_terminal_claim_t claim;
  size_t bytes;
} stream_sink_operation_t;

struct turbo_flow_cnet_stream_sink_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr uri;
  cnet_client_config client_config;
  cnet_stream_socket_options socket_options;
  const cnet_tls_client_config *tls;
  bool has_socket_options;
  cnet_client client;
  cnet_connection connection;
  cflow_executor executor;
  cflow_io_actor actor;
  atomic_bool client_initialized;
  bool executor_initialized;
  bool actor_initialized;
  atomic_int state;
  atomic_int status;
  atomic_int native_status;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  uint32_t stop_timeout_ms;
  atomic_uint_fast64_t next_lease;
  cflow_io_request_id native_request;
  cflow_io_request_id delivered_request;
  atomic_uint_fast64_t messages_sent;
  atomic_uint_fast64_t bytes_sent;
  atomic_bool detached;
};

static void stream_sink_stop_internal(turbo_flow_cnet_stream_sink_t *sink);

static turbo_flow_cnet_stream_sink_state_t
stream_sink_state(const turbo_flow_cnet_stream_sink_t *sink) {
  return (turbo_flow_cnet_stream_sink_state_t)atomic_load_explicit(&sink->state,
                                                                   memory_order_acquire);
}

static int stream_sink_connection_equal(cnet_connection left, cnet_connection right) {
  return left.slot == right.slot && left.generation == right.generation;
}

static int stream_sink_submit_status(cflow_io_submit_status status) {
  switch (status) {
  case CFLOW_IO_SUBMIT_ACCEPTED:
    return SALTS_OK;
  case CFLOW_IO_SUBMIT_FULL:
    return SALTS_ENOSPC;
  case CFLOW_IO_SUBMIT_CLOSED:
    return SALTS_ESHUTDOWN;
  case CFLOW_IO_SUBMIT_LEASE_IN_USE:
    return SALTS_EALREADY;
  case CFLOW_IO_SUBMIT_ID_EXHAUSTED:
    return SALTS_ERANGE;
  case CFLOW_IO_SUBMIT_INVALID_ARGUMENT:
  default:
    return SALTS_EINVAL;
  }
}

static int stream_sink_completion_status(const cflow_io_completion *completion) {
  if (!completion) return SALTS_EPROTO;
  switch (completion->kind) {
  case CFLOW_IO_COMPLETION_OK:
    return SALTS_OK;
  case CFLOW_IO_COMPLETION_CANCELLED:
    return SALTS_ECANCELED;
  case CFLOW_IO_COMPLETION_EOF:
    return SALTS_ESHUTDOWN;
  case CFLOW_IO_COMPLETION_FAILED:
  default:
    return completion->error != SALTS_OK ? completion->error : SALTS_EIO;
  }
}

static void stream_sink_operation_release(void *user) {
  stream_sink_operation_t *operation = (stream_sink_operation_t *)user;
  if (!operation) return;
  if (operation->claim._impl) {
    (void)turbo_flow_async_terminal_complete(&operation->claim, SALTS_ECANCELED, NULL);
  }
  free(operation);
}

static int stream_sink_backend_submit(void *user, cflow_io_actor *actor,
                                      cflow_io_request_id request_id, cflow_io_lease_id lease_id,
                                      void *operation_user) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)user;
  stream_sink_operation_t *operation = (stream_sink_operation_t *)operation_user;
  const turbo_flow_msg_t *message;
  int rc;
  (void)actor;
  (void)lease_id;
  if (!sink || !operation || sink->native_request != 0u) return SALTS_EBUSY;
  message = turbo_flow_async_terminal_claim_message(&operation->claim);
  if (!message || !message->payload.data || message->payload.len == 0u ||
      message->payload.len != operation->bytes) {
    return SALTS_EPROTO;
  }
  sink->native_request = request_id;
  rc = cnet_send(&sink->client, sink->connection, message->payload.data, message->payload.len);
  if (rc != SALTS_OK) sink->native_request = 0u;
  return rc;
}

static int stream_sink_backend_cancel(void *user, cflow_io_request_id request_id) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)user;
  if (!sink || request_id == 0u || request_id != sink->native_request) return SALTS_ENOENT;
  return cnet_close(&sink->client, sink->connection);
}

static void stream_sink_actor_completion(void *user, cflow_io_request_id request_id,
                                         cflow_io_lease_id lease_id, void *operation_user,
                                         const cflow_io_completion *completion) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)user;
  stream_sink_operation_t *operation = (stream_sink_operation_t *)operation_user;
  int status = stream_sink_completion_status(completion);
  (void)lease_id;
  if (!sink || !operation) return;
  if (status == SALTS_OK && completion->bytes != operation->bytes) status = SALTS_EPROTO;
  if (request_id == sink->native_request) sink->native_request = 0u;
  if (status == SALTS_OK) {
    uint64_t messages = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
    uint64_t bytes = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
    if (messages == UINT64_MAX || bytes > UINT64_MAX - (uint64_t)operation->bytes) {
      status = SALTS_ERANGE;
    } else {
      atomic_store_explicit(&sink->messages_sent, messages + 1u, memory_order_relaxed);
      atomic_store_explicit(&sink->bytes_sent, bytes + (uint64_t)operation->bytes,
                            memory_order_relaxed);
    }
  }
  sink->delivered_request = request_id;
  (void)turbo_flow_async_terminal_complete(&operation->claim, status, NULL);
}

static void stream_sink_actor_wake(void *user) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)user;
  if (sink && atomic_load_explicit(&sink->client_initialized, memory_order_acquire))
    (void)cnet_client_wake(&sink->client);
}

static void stream_sink_complete_native(turbo_flow_cnet_stream_sink_t *sink,
                                        cflow_io_completion_kind kind, size_t bytes, int status) {
  const cflow_io_completion completion = {kind, bytes, status};
  cflow_io_request_id request_id;
  if (!sink || (request_id = sink->native_request) == 0u) return;
  if (cflow_io_actor_complete(&sink->actor, request_id, &completion) ==
      CFLOW_IO_COMPLETE_ACCEPTED) {
    sink->native_request = 0u;
  }
}

static void stream_sink_on_state(void *user, cnet_connection connection,
                                 cnet_connection_state state, const cnet_error *error) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)user;
  if (!sink || !stream_sink_connection_equal(connection, sink->connection)) return;
  switch (state) {
  case CNET_CONNECTION_CONNECTED:
    if (stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_CONNECTING)
      atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTED,
                            memory_order_release);
    break;
  case CNET_CONNECTION_FAILED:
    atomic_store_explicit(&sink->status, error ? error->status : SALTS_EIO, memory_order_relaxed);
    atomic_store_explicit(&sink->native_status, error ? error->native_status : 0,
                          memory_order_relaxed);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
    stream_sink_complete_native(sink, CFLOW_IO_COMPLETION_FAILED, 0u,
                                atomic_load_explicit(&sink->status, memory_order_relaxed));
    break;
  case CNET_CONNECTION_CLOSED: {
    turbo_flow_cnet_stream_sink_state_t current = stream_sink_state(sink);
    if (current != TURBO_FLOW_CNET_STREAM_SINK_STOPPING) {
      atomic_store_explicit(&sink->status, SALTS_ESHUTDOWN, memory_order_relaxed);
      atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
    }
    stream_sink_complete_native(
        sink,
        current == TURBO_FLOW_CNET_STREAM_SINK_STOPPING ? CFLOW_IO_COMPLETION_CANCELLED
                                                        : CFLOW_IO_COMPLETION_FAILED,
        0u, current == TURBO_FLOW_CNET_STREAM_SINK_STOPPING ? SALTS_ECANCELED : SALTS_ESHUTDOWN);
  } break;
  case CNET_CONNECTION_CONNECTING:
  case CNET_CONNECTION_CLOSING:
    break;
  }
}

static void stream_sink_on_send(void *user, cnet_connection connection, size_t size) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)user;
  if (!sink || !stream_sink_connection_equal(connection, sink->connection)) return;
  stream_sink_complete_native(sink, CFLOW_IO_COMPLETION_OK, size, SALTS_OK);
}

static void stream_sink_acknowledge(turbo_flow_cnet_stream_sink_t *sink) {
  cflow_io_request_id request_id;
  if (!sink || (request_id = sink->delivered_request) == 0u) return;
  if (cflow_io_actor_acknowledge(&sink->actor, request_id) == CFLOW_IO_ACK_RELEASED) {
    sink->delivered_request = 0u;
  }
}

static void stream_sink_drive_actor(turbo_flow_cnet_stream_sink_t *sink) {
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
  (void)cflow_executor_run_ready(&sink->executor);
  stream_sink_acknowledge(sink);
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
}

static int stream_sink_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  cflow_io_actor_config actor_config = {0};
  cnet_connect_options options = {0};
  int rc;
  (void)stage;
  if (!sink || flow != sink->flow || atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      (stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_REGISTERED &&
       stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_STOPPED)) {
    return SALTS_EINVAL;
  }
  atomic_store_explicit(&sink->status, SALTS_OK, memory_order_relaxed);
  atomic_store_explicit(&sink->native_status, 0, memory_order_relaxed);
  rc = cnet_client_init(&sink->client, &sink->client_config);
  if (rc != SALTS_OK) goto fail;
  atomic_store_explicit(&sink->client_initialized, true, memory_order_release);
  if (sink->has_socket_options) {
    rc = cnet_client_set_stream_socket_options(&sink->client, &sink->socket_options);
    if (rc != SALTS_OK) goto fail;
  }
  if (!cflow_executor_manual_init_with_capacity(&sink->executor, 1u)) {
    rc = SALTS_ENOMEM;
    goto fail;
  }
  sink->executor_initialized = true;
  actor_config.request_capacity = 1u;
  actor_config.command_capacity = sink->actor_command_capacity;
  actor_config.executor = &sink->executor;
  actor_config.backend.submit = stream_sink_backend_submit;
  actor_config.backend.cancel = stream_sink_backend_cancel;
  actor_config.backend_user = sink;
  actor_config.completion = stream_sink_actor_completion;
  actor_config.completion_user = sink;
  actor_config.wake = stream_sink_actor_wake;
  actor_config.wake_user = sink;
  rc = cflow_io_actor_init(&sink->actor, &actor_config);
  if (rc != SALTS_OK) goto fail;
  sink->actor_initialized = true;
  options.uri = sink->uri;
  options.tls = sink->tls;
  options.observer.on_state = stream_sink_on_state;
  options.observer.on_send = stream_sink_on_send;
  options.observer.user = sink;
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTING, memory_order_release);
  rc = cnet_connect(&sink->client, &options, &sink->connection);
  if (rc == SALTS_OK) return SALTS_OK;

fail:
  atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
  stream_sink_stop_internal(sink);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
  return rc;
}

static void stream_sink_stop_internal(turbo_flow_cnet_stream_sink_t *sink) {
  int rc;
  if (!sink || stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_STOPPED ||
      stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_DETACHED) {
    return;
  }
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_STOPPING, memory_order_release);
  if (sink->actor_initialized) {
    (void)cflow_io_actor_close(&sink->actor);
    stream_sink_drive_actor(sink);
  }
  if (atomic_load_explicit(&sink->client_initialized, memory_order_acquire)) {
    do {
      rc = cnet_client_stop(&sink->client, sink->stop_timeout_ms);
    } while (rc == SALTS_ETIMEDOUT);
    if (rc != SALTS_OK && atomic_load_explicit(&sink->status, memory_order_relaxed) == SALTS_OK)
      atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
  }
  while (sink->actor_initialized && !cflow_io_actor_is_quiescent(&sink->actor)) {
    stream_sink_drive_actor(sink);
  }
  if (sink->actor_initialized) {
    (void)cflow_io_actor_destroy(&sink->actor);
    sink->actor_initialized = false;
  }
  if (sink->executor_initialized) {
    (void)cflow_executor_shutdown(&sink->executor);
    cflow_executor_destroy(&sink->executor);
    sink->executor_initialized = false;
  }
  if (atomic_load_explicit(&sink->client_initialized, memory_order_acquire)) {
    (void)cnet_client_destroy(&sink->client);
    atomic_store_explicit(&sink->client_initialized, false, memory_order_release);
  }
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_STOPPED, memory_order_release);
}

static void stream_sink_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  (void)flow;
  (void)stage;
  stream_sink_stop_internal(sink);
}

static void stream_sink_shutdown(void *ctx) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  if (!sink) return;
  stream_sink_stop_internal(sink);
  sink->flow = NULL;
  atomic_store_explicit(&sink->detached, true, memory_order_release);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_DETACHED, memory_order_release);
}

static int stream_sink_next_lease(turbo_flow_cnet_stream_sink_t *sink,
                                  cflow_io_lease_id *lease_out) {
  uint_fast64_t lease;
  uint_fast64_t next;
  if (!sink || !lease_out) return SALTS_EINVAL;
  lease = atomic_load_explicit(&sink->next_lease, memory_order_relaxed);
  do {
    if (lease == 0u || lease == UINT64_MAX) return SALTS_ERANGE;
    next = lease + 1u;
  } while (!atomic_compare_exchange_weak_explicit(&sink->next_lease, &lease, next,
                                                  memory_order_relaxed, memory_order_relaxed));
  *lease_out = (cflow_io_lease_id)lease;
  return SALTS_OK;
}

static int stream_sink_async_submit(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage,
                                    const turbo_flow_msg_t *message,
                                    turbo_flow_async_terminal_claim_t *claim) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  stream_sink_operation_t *operation;
  cflow_io_operation io_operation;
  cflow_io_submit_result submitted;
  cflow_io_lease_id lease_id;
  int rc;
  (void)stage;
  if (!sink || flow != sink->flow || !message || !claim) return SALTS_EINVAL;
  if (stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED) {
    return stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_FAILED
               ? atomic_load_explicit(&sink->status, memory_order_relaxed)
               : SALTS_EBUSY;
  }
  if (!message->payload.data || message->payload.len == 0u) return SALTS_EINVAL;
  if (message->payload.len > sink->max_message_bytes) return SALTS_EMSGSIZE;
  rc = stream_sink_next_lease(sink, &lease_id);
  if (rc != SALTS_OK) return rc;
  operation = (stream_sink_operation_t *)calloc(1u, sizeof(*operation));
  if (!operation) return SALTS_ENOMEM;
  operation->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  operation->bytes = message->payload.len;
  rc = turbo_flow_async_terminal_claim_move(&operation->claim, claim);
  if (rc != SALTS_OK) {
    free(operation);
    return rc;
  }
  io_operation.user = operation;
  io_operation.release = stream_sink_operation_release;
  submitted = cflow_io_actor_try_submit(&sink->actor, lease_id, &io_operation);
  rc = stream_sink_submit_status(submitted.status);
  if (rc != SALTS_OK) {
    (void)turbo_flow_async_terminal_claim_move(claim, &operation->claim);
    free(operation);
  }
  return rc;
}

static int stream_sink_config_validate(const turbo_flow_cnet_stream_sink_config_t *config) {
  bool is_tcp;
  bool is_tls;
  bool is_pipe;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->uri ||
      config->uri[0] == '\0' || !config->client || config->max_message_bytes == 0u ||
      config->max_message_bytes > config->client->max_send_bytes ||
      config->actor_command_capacity == 0u || config->actor_max_steps_per_poll == 0u ||
      config->stop_timeout_ms == 0u) {
    return SALTS_EINVAL;
  }
  if (turbo_flow_state(config->flow) == TURBO_FLOW_STATE_COMPILED ||
      turbo_flow_state(config->flow) == TURBO_FLOW_STATE_STARTED) {
    return SALTS_EBUSY;
  }
  is_tcp = strncmp(config->uri, "tcp://", sizeof("tcp://") - 1u) == 0;
  is_tls = strncmp(config->uri, "tls://", sizeof("tls://") - 1u) == 0;
  is_pipe = strncmp(config->uri, "pipe://", sizeof("pipe://") - 1u) == 0;
  if (!is_tcp && !is_tls && !is_pipe) return SALTS_ENOTSUP;
  if (config->tls && !is_tls) return SALTS_EINVAL;
  return config->socket_options ? cnet_stream_socket_options_validate(config->socket_options)
                                : SALTS_OK;
}

int turbo_flow_cnet_stream_sink_register(const turbo_flow_cnet_stream_sink_config_t *config,
                                         turbo_flow_cnet_stream_sink_t **sink_out) {
  turbo_flow_cnet_stream_sink_t *sink;
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  int rc;
  if (!sink_out) return SALTS_EINVAL;
  *sink_out = NULL;
  rc = stream_sink_config_validate(config);
  if (rc != SALTS_OK) return rc;
  sink = (turbo_flow_cnet_stream_sink_t *)calloc(1u, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  sink->flow = config->flow;
  sink->adapter_name = tstr_dup(config->adapter_name);
  sink->uri = tstr_dup(config->uri);
  sink->client_config = *config->client;
  sink->tls = config->tls;
  sink->max_message_bytes = config->max_message_bytes;
  sink->actor_command_capacity = config->actor_command_capacity;
  sink->actor_max_steps_per_poll = config->actor_max_steps_per_poll;
  sink->stop_timeout_ms = config->stop_timeout_ms;
  atomic_init(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_REGISTERED);
  atomic_init(&sink->next_lease, STREAM_SINK_SINGLE_REQUEST_LEASE);
  atomic_init(&sink->status, SALTS_OK);
  atomic_init(&sink->native_status, 0);
  atomic_init(&sink->messages_sent, 0u);
  atomic_init(&sink->bytes_sent, 0u);
  atomic_init(&sink->client_initialized, false);
  atomic_init(&sink->detached, false);
  if (!sink->adapter_name || !sink->uri) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->uri);
    free(sink);
    return SALTS_ENOMEM;
  }
  if (config->socket_options) {
    sink->socket_options = *config->socket_options;
    sink->has_socket_options = true;
  }
  adapter_ops.start = stream_sink_start;
  adapter_ops.stop = stream_sink_stop;
  adapter_ops.shutdown = stream_sink_shutdown;
  async_ops.submit = stream_sink_async_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  rc = turbo_flow_register_async_terminal_adapter_ex(config->flow, config->adapter_name,
                                                     &adapter_ops, &async_ops, sink, &schema);
  if (rc != SALTS_OK) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->uri);
    free(sink);
    return rc;
  }
  *sink_out = sink;
  return SALTS_OK;
}

int turbo_flow_cnet_stream_sink_snapshot(const turbo_flow_cnet_stream_sink_t *sink,
                                         turbo_flow_cnet_stream_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_stream_sink_snapshot_t current = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  if (!sink || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION) {
    return SALTS_EINVAL;
  }
  current.state = stream_sink_state(sink);
  current.status = atomic_load_explicit(&sink->status, memory_order_relaxed);
  current.native_status = atomic_load_explicit(&sink->native_status, memory_order_relaxed);
  current.connection = sink->connection;
  current.messages_sent = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
  current.bytes_sent = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
  if (sink->actor_initialized && cflow_io_actor_get_stats(&sink->actor, &stats))
    current.active_requests = stats.active_requests;
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_stream_sink_poll(turbo_flow_cnet_stream_sink_t *sink, uint32_t timeout_ms,
                                     turbo_flow_cnet_stream_sink_snapshot_t *snapshot) {
  size_t events = 0u;
  int rc;
  if (!sink || atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !atomic_load_explicit(&sink->client_initialized, memory_order_acquire) ||
      !sink->actor_initialized ||
      (snapshot && (snapshot->size < sizeof(*snapshot) ||
                    snapshot->version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION))) {
    return SALTS_EINVAL;
  }
  if (stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_STOPPED) return SALTS_ESHUTDOWN;
  stream_sink_drive_actor(sink);
  rc = cnet_client_poll(&sink->client, timeout_ms, &events);
  if (rc != SALTS_OK && stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_FAILED) {
    atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
  }
  stream_sink_drive_actor(sink);
  if (snapshot) (void)turbo_flow_cnet_stream_sink_snapshot(sink, snapshot);
  return stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_FAILED
             ? atomic_load_explicit(&sink->status, memory_order_relaxed)
             : rc;
}

int turbo_flow_cnet_stream_sink_destroy(turbo_flow_cnet_stream_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  if (!atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      atomic_load_explicit(&sink->client_initialized, memory_order_acquire) ||
      sink->actor_initialized || sink->executor_initialized) {
    return SALTS_EBUSY;
  }
  tstr_free(sink->adapter_name);
  tstr_free(sink->uri);
  free(sink);
  return SALTS_OK;
}
