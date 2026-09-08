#include "turbo_flow_cnet.h"

#include <cflow/executor.h>
#include <cflow/io_actor.h>
#include <salts/thread.h>
#define XXH_INLINE_ALL
#include <xxhash.h>

#include <inttypes.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  STREAM_SINK_SINGLE_REQUEST_LEASE = 1u,
  STREAM_SINK_REQUEST_CAPACITY = 1u,
  STREAM_SINK_RESOURCE_GENERATION = 1u
};

static const char STREAM_SINK_RESOURCE_UID_PREFIX[] = "cnet-stream-sink:";
static const char STREAM_SINK_HASHED_OWNER_PREFIX[] = "xxh3-128:";
static const char STREAM_SINK_SCHEMA_NAME[] = "CNetStream";
static const char STREAM_SINK_SCHEMA_TYPE[] = "NonEmptyBytes";
static const char STREAM_SINK_MEDIA_TYPE[] = "application/octet-stream";

typedef struct stream_sink_operation_s {
  struct turbo_flow_cnet_stream_sink_s *sink;
  turbo_flow_async_terminal_claim_t claim;
  size_t bytes;
} stream_sink_operation_t;

struct turbo_flow_cnet_stream_sink_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr uri;
  char managed_owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char resource_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
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
  atomic_uint_fast64_t accepted;
  atomic_uint_fast64_t completed;
  atomic_uint_fast64_t rejected;
  atomic_bool detached;
  salts_mutex_t lifecycle_mutex;
  salts_cond_t lifecycle_cond;
  bool lifecycle_initialized;
  bool owner_active;
  size_t submissions_active;
};

typedef enum stream_sink_cleanup_mode_e {
  STREAM_SINK_CLEANUP_STOP,
  STREAM_SINK_CLEANUP_FAILURE
} stream_sink_cleanup_mode_t;

static void stream_sink_cleanup_internal(turbo_flow_cnet_stream_sink_t *sink,
                                         stream_sink_cleanup_mode_t mode);

static int stream_sink_managed_identity_init(turbo_flow_cnet_stream_sink_t *sink) {
  const size_t adapter_name_len = tstr_len(sink->adapter_name);
  int written;
  if (adapter_name_len <= TURBO_FLOW_RESOURCE_OWNER_MAX) {
    memcpy(sink->managed_owner_name, sink->adapter_name, adapter_name_len + 1u);
  } else {
    const XXH128_hash_t hash = XXH3_128bits(sink->adapter_name, adapter_name_len);
    written = snprintf(sink->managed_owner_name, sizeof(sink->managed_owner_name),
                       "%s%016" PRIx64 "%016" PRIx64, STREAM_SINK_HASHED_OWNER_PREFIX, hash.high64,
                       hash.low64);
    if (written < 0 || (size_t)written >= sizeof(sink->managed_owner_name)) return SALTS_ERANGE;
  }
  written = snprintf(sink->resource_uid, sizeof(sink->resource_uid), "%s%s",
                     STREAM_SINK_RESOURCE_UID_PREFIX, sink->managed_owner_name);
  if (written < 0 || (size_t)written >= sizeof(sink->resource_uid)) return SALTS_ERANGE;
  return SALTS_OK;
}

static int stream_sink_lifecycle_init(turbo_flow_cnet_stream_sink_t *sink) {
  salts_mutex_init(&sink->lifecycle_mutex);
  if (!sink->lifecycle_mutex) return SALTS_ENOMEM;
  salts_cond_init(&sink->lifecycle_cond);
  if (!sink->lifecycle_cond) {
    salts_mutex_destroy(&sink->lifecycle_mutex);
    return SALTS_ENOMEM;
  }
  sink->lifecycle_initialized = true;
  return SALTS_OK;
}

static void stream_sink_lifecycle_destroy(turbo_flow_cnet_stream_sink_t *sink) {
  if (!sink || !sink->lifecycle_initialized) return;
  salts_cond_destroy(&sink->lifecycle_cond);
  salts_mutex_destroy(&sink->lifecycle_mutex);
  sink->lifecycle_initialized = false;
}

static turbo_flow_cnet_stream_sink_state_t
stream_sink_state(const turbo_flow_cnet_stream_sink_t *sink) {
  return (turbo_flow_cnet_stream_sink_state_t)atomic_load_explicit(&sink->state,
                                                                   memory_order_acquire);
}

static turbo_flow_managed_boundary_state_t
stream_sink_managed_state(turbo_flow_cnet_stream_sink_state_t state, size_t active_requests) {
  switch (state) {
  case TURBO_FLOW_CNET_STREAM_SINK_REGISTERED:
    return TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  case TURBO_FLOW_CNET_STREAM_SINK_CONNECTING:
    return TURBO_FLOW_MANAGED_BOUNDARY_STARTING;
  case TURBO_FLOW_CNET_STREAM_SINK_CONNECTED:
    return TURBO_FLOW_MANAGED_BOUNDARY_RUNNING;
  case TURBO_FLOW_CNET_STREAM_SINK_STOPPING:
    return active_requests > 0u ? TURBO_FLOW_MANAGED_BOUNDARY_DRAINING
                                : TURBO_FLOW_MANAGED_BOUNDARY_STOPPING;
  case TURBO_FLOW_CNET_STREAM_SINK_STOPPED:
  case TURBO_FLOW_CNET_STREAM_SINK_DETACHED:
    return TURBO_FLOW_MANAGED_BOUNDARY_STOPPED;
  case TURBO_FLOW_CNET_STREAM_SINK_FAILED:
  default:
    return TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  }
}

static int stream_sink_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(metadata.uid, sink->resource_uid, strlen(sink->resource_uid) + 1u);
  memcpy(metadata.owner_name, sink->managed_owner_name, strlen(sink->managed_owner_name) + 1u);
  metadata.generation = STREAM_SINK_RESOURCE_GENERATION;
  metadata.observed_generation = STREAM_SINK_RESOURCE_GENERATION;
  *out = metadata;
  return SALTS_OK;
}

static int stream_sink_managed_descriptor(void *ctx,
                                          turbo_flow_managed_boundary_descriptor_t *out) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  turbo_flow_managed_boundary_descriptor_t descriptor = TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  int rc;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  descriptor.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  descriptor.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(descriptor.uid, sink->resource_uid, strlen(sink->resource_uid) + 1u);
  memcpy(descriptor.owner_name, sink->managed_owner_name, strlen(sink->managed_owner_name) + 1u);
  descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT;
  descriptor.command_flags = 0u;
  rc = turbo_flow_content_descriptor_init(
      &descriptor.input, TURBO_FLOW_DOMAIN_IO_TRANSPORT, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
      TURBO_FLOW_DATA_ENCODING_OPAQUE, STREAM_SINK_MEDIA_TYPE, sink->managed_owner_name);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(&descriptor.input, STREAM_SINK_SCHEMA_NAME,
                                                    STREAM_SINK_SCHEMA_TYPE, 1u);
  if (rc != SALTS_OK) return rc;
  *out = descriptor;
  return SALTS_OK;
}

static int stream_sink_managed_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  size_t queue_depth = 0u;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (sink->submissions_active > 0u) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  stats.request_capacity = STREAM_SINK_REQUEST_CAPACITY;
  if (sink->actor_initialized && !cflow_io_actor_get_stats(&sink->actor, &stats)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EPROTO;
  }
  if (stats.admitted > SIZE_MAX - stats.ready) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EPROTO;
  }
  queue_depth = stats.admitted + stats.ready;
  if (queue_depth > stats.active_requests || stats.active_requests > stats.request_capacity) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EPROTO;
  }
  memcpy(snapshot.uid, sink->resource_uid, strlen(sink->resource_uid) + 1u);
  snapshot.generation = STREAM_SINK_RESOURCE_GENERATION;
  snapshot.observed_generation = STREAM_SINK_RESOURCE_GENERATION;
  snapshot.state = stream_sink_managed_state(stream_sink_state(sink), stats.active_requests);
  snapshot.queue_depth = (uint64_t)queue_depth;
  snapshot.queue_capacity = (uint64_t)stats.request_capacity;
  snapshot.in_flight = (uint64_t)(stats.active_requests - queue_depth);
  snapshot.accepted = atomic_load_explicit(&sink->accepted, memory_order_relaxed);
  snapshot.completed = atomic_load_explicit(&sink->completed, memory_order_relaxed);
  snapshot.rejected = atomic_load_explicit(&sink->rejected, memory_order_relaxed);
  if (snapshot.completed > snapshot.accepted) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EPROTO;
  }
  snapshot.backpressured = stats.active_requests >= stats.request_capacity;
  snapshot.last_status = atomic_load_explicit(&sink->status, memory_order_relaxed);
  *out = snapshot;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;
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

static void stream_sink_counter_increment(atomic_uint_fast64_t *counter) {
  uint_fast64_t current = atomic_load_explicit(counter, memory_order_relaxed);
  while (current != UINT64_MAX &&
         !atomic_compare_exchange_weak_explicit(counter, &current, current + 1u,
                                                memory_order_relaxed, memory_order_relaxed)) {
  }
}

static int stream_sink_reject(turbo_flow_cnet_stream_sink_t *sink, int status) {
  if (sink) stream_sink_counter_increment(&sink->rejected);
  return status;
}

static void stream_sink_submission_finish(turbo_flow_cnet_stream_sink_t *sink, bool accepted) {
  salts_mutex_lock(&sink->lifecycle_mutex);
  stream_sink_counter_increment(accepted ? &sink->accepted : &sink->rejected);
  --sink->submissions_active;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static void stream_sink_operation_complete(stream_sink_operation_t *operation, int status) {
  if (!operation || !operation->claim._impl) return;
  if (turbo_flow_async_terminal_complete(&operation->claim, status, NULL) == SALTS_OK)
    stream_sink_counter_increment(&operation->sink->completed);
}

static void stream_sink_operation_release(void *user) {
  stream_sink_operation_t *operation = (stream_sink_operation_t *)user;
  if (!operation) return;
  stream_sink_operation_complete(operation, SALTS_ECANCELED);
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
  stream_sink_operation_complete(operation, status);
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
        0u, current == TURBO_FLOW_CNET_STREAM_SINK_STOPPING ? SALTS_OK : SALTS_ESHUTDOWN);
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
  if (!sink || flow != sink->flow) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) || sink->owner_active ||
      sink->submissions_active > 0u ||
      (stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_REGISTERED &&
       stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_STOPPED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  sink->owner_active = true;
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTING, memory_order_release);
  atomic_store_explicit(&sink->status, SALTS_OK, memory_order_relaxed);
  atomic_store_explicit(&sink->native_status, 0, memory_order_relaxed);
  salts_mutex_unlock(&sink->lifecycle_mutex);
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
  actor_config.request_capacity = STREAM_SINK_REQUEST_CAPACITY;
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
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->actor_initialized = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  options.uri = sink->uri;
  options.tls = sink->tls;
  options.observer.on_state = stream_sink_on_state;
  options.observer.on_send = stream_sink_on_send;
  options.observer.user = sink;
  rc = cnet_connect(&sink->client, &options, &sink->connection);
  if (rc == SALTS_OK) {
    salts_mutex_lock(&sink->lifecycle_mutex);
    sink->owner_active = false;
    salts_cond_broadcast(&sink->lifecycle_cond);
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_OK;
  }

fail:
  atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
  salts_mutex_lock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  stream_sink_cleanup_internal(sink, STREAM_SINK_CLEANUP_FAILURE);
  return rc;
}

static void stream_sink_cleanup_internal(turbo_flow_cnet_stream_sink_t *sink,
                                         stream_sink_cleanup_mode_t mode) {
  const bool failure_cleanup = mode == STREAM_SINK_CLEANUP_FAILURE;
  int rc;
  if (!sink) return;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (!failure_cleanup && (stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_STOPPED ||
                           stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_DETACHED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return;
  }
  /* A concurrent stop waits for failure cleanup without masking its FAILED state. */
  if (!failure_cleanup &&
      !(stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_FAILED && sink->owner_active))
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_STOPPING, memory_order_release);
  while ((!failure_cleanup && sink->owner_active) || sink->submissions_active > 0u)
    salts_cond_wait(&sink->lifecycle_cond, &sink->lifecycle_mutex);
  if (!failure_cleanup && (stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_STOPPED ||
                           stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_DETACHED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return;
  }
  if (!failure_cleanup)
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_STOPPING, memory_order_release);
  sink->owner_active = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
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
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (sink->actor_initialized) {
    (void)cflow_io_actor_destroy(&sink->actor);
    sink->actor_initialized = false;
  }
  salts_mutex_unlock(&sink->lifecycle_mutex);
  if (sink->executor_initialized) {
    (void)cflow_executor_shutdown(&sink->executor);
    cflow_executor_destroy(&sink->executor);
    sink->executor_initialized = false;
  }
  if (atomic_load_explicit(&sink->client_initialized, memory_order_acquire)) {
    (void)cnet_client_destroy(&sink->client);
    atomic_store_explicit(&sink->client_initialized, false, memory_order_release);
  }
  salts_mutex_lock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->state,
                        failure_cleanup ? TURBO_FLOW_CNET_STREAM_SINK_FAILED
                                        : TURBO_FLOW_CNET_STREAM_SINK_STOPPED,
                        memory_order_release);
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static void stream_sink_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  (void)flow;
  (void)stage;
  stream_sink_cleanup_internal(sink, STREAM_SINK_CLEANUP_STOP);
}

static void stream_sink_shutdown(void *ctx) {
  turbo_flow_cnet_stream_sink_t *sink = (turbo_flow_cnet_stream_sink_t *)ctx;
  if (!sink) return;
  stream_sink_cleanup_internal(sink, STREAM_SINK_CLEANUP_STOP);
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->flow = NULL;
  atomic_store_explicit(&sink->detached, true, memory_order_release);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_DETACHED, memory_order_release);
  salts_mutex_unlock(&sink->lifecycle_mutex);
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
  turbo_flow_cnet_stream_sink_state_t state;
  int rc;
  (void)stage;
  if (!sink || !message || !claim) return stream_sink_reject(sink, SALTS_EINVAL);
  if (!message->payload.data || message->payload.len == 0u)
    return stream_sink_reject(sink, SALTS_EINVAL);
  if (message->payload.len > sink->max_message_bytes)
    return stream_sink_reject(sink, SALTS_EMSGSIZE);
  operation = (stream_sink_operation_t *)calloc(1u, sizeof(*operation));
  if (!operation) return stream_sink_reject(sink, SALTS_ENOMEM);
  operation->sink = sink;
  operation->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  operation->bytes = message->payload.len;
  salts_mutex_lock(&sink->lifecycle_mutex);
  state = stream_sink_state(sink);
  if (flow != sink->flow || atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !sink->actor_initialized || state != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED) {
    rc = state == TURBO_FLOW_CNET_STREAM_SINK_FAILED
             ? atomic_load_explicit(&sink->status, memory_order_relaxed)
             : SALTS_EBUSY;
    salts_mutex_unlock(&sink->lifecycle_mutex);
    free(operation);
    return stream_sink_reject(sink, rc);
  }
  ++sink->submissions_active;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  rc = stream_sink_next_lease(sink, &lease_id);
  if (rc != SALTS_OK) {
    free(operation);
    stream_sink_submission_finish(sink, false);
    return rc;
  }
  rc = turbo_flow_async_terminal_claim_move(&operation->claim, claim);
  if (rc != SALTS_OK) {
    free(operation);
    stream_sink_submission_finish(sink, false);
    return rc;
  }
  io_operation.user = operation;
  io_operation.release = stream_sink_operation_release;
  submitted = cflow_io_actor_try_submit(&sink->actor, lease_id, &io_operation);
  rc = stream_sink_submit_status(submitted.status);
  if (rc != SALTS_OK) {
    (void)turbo_flow_async_terminal_claim_move(claim, &operation->claim);
    free(operation);
    stream_sink_submission_finish(sink, false);
    return rc;
  }
  stream_sink_submission_finish(sink, true);
  return SALTS_OK;
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
  turbo_flow_managed_boundary_provider_ops_t boundary_ops =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_async_terminal_registration_t registration =
      TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  int rc;
  if (!sink_out) return SALTS_EINVAL;
  *sink_out = NULL;
  rc = stream_sink_config_validate(config);
  if (rc != SALTS_OK) return rc;
  sink = (turbo_flow_cnet_stream_sink_t *)calloc(1u, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  rc = stream_sink_lifecycle_init(sink);
  if (rc != SALTS_OK) {
    free(sink);
    return rc;
  }
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
  atomic_init(&sink->accepted, 0u);
  atomic_init(&sink->completed, 0u);
  atomic_init(&sink->rejected, 0u);
  atomic_init(&sink->client_initialized, false);
  atomic_init(&sink->detached, false);
  if (!sink->adapter_name || !sink->uri) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->uri);
    stream_sink_lifecycle_destroy(sink);
    free(sink);
    return SALTS_ENOMEM;
  }
  rc = stream_sink_managed_identity_init(sink);
  if (rc != SALTS_OK) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->uri);
    stream_sink_lifecycle_destroy(sink);
    free(sink);
    return rc;
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
  boundary_ops.resource.metadata = stream_sink_resource_metadata;
  boundary_ops.descriptor = stream_sink_managed_descriptor;
  boundary_ops.snapshot = stream_sink_managed_snapshot;
  registration.adapter_name = sink->adapter_name;
  registration.adapter_ops = &adapter_ops;
  registration.async_ops = &async_ops;
  registration.schema = &schema;
  registration.owner_name = sink->managed_owner_name;
  registration.boundary_ops = &boundary_ops;
  registration.ctx = sink;
  rc = turbo_flow_register_managed_async_terminal_adapter(config->flow, &registration);
  if (rc != SALTS_OK) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->uri);
    stream_sink_lifecycle_destroy(sink);
    free(sink);
    return rc;
  }
  *sink_out = sink;
  return SALTS_OK;
}

static int stream_sink_snapshot_locked(const turbo_flow_cnet_stream_sink_t *sink,
                                       turbo_flow_cnet_stream_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_stream_sink_snapshot_t current = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  current.state = stream_sink_state(sink);
  current.status = atomic_load_explicit(&sink->status, memory_order_relaxed);
  current.native_status = atomic_load_explicit(&sink->native_status, memory_order_relaxed);
  current.connection = sink->connection;
  current.messages_sent = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
  current.bytes_sent = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
  if (sink->actor_initialized) {
    if (!cflow_io_actor_get_stats(&sink->actor, &stats)) return SALTS_EPROTO;
    current.active_requests = stats.active_requests;
  }
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_stream_sink_snapshot(const turbo_flow_cnet_stream_sink_t *sink,
                                         turbo_flow_cnet_stream_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_stream_sink_t *mutable_sink = (turbo_flow_cnet_stream_sink_t *)sink;
  int rc;
  if (!sink || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock(&mutable_sink->lifecycle_mutex);
  rc = stream_sink_snapshot_locked(sink, snapshot);
  salts_mutex_unlock(&mutable_sink->lifecycle_mutex);
  return rc;
}

int turbo_flow_cnet_stream_sink_poll(turbo_flow_cnet_stream_sink_t *sink, uint32_t timeout_ms,
                                     turbo_flow_cnet_stream_sink_snapshot_t *snapshot) {
  size_t events = 0u;
  int rc;
  if (!sink || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                             snapshot->version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION))) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !atomic_load_explicit(&sink->client_initialized, memory_order_acquire) ||
      !sink->actor_initialized) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  if (stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_STOPPING ||
      stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_STOPPED) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_ESHUTDOWN;
  }
  if (sink->owner_active) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->owner_active = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  stream_sink_drive_actor(sink);
  rc = cnet_client_poll(&sink->client, timeout_ms, &events);
  if (rc != SALTS_OK && stream_sink_state(sink) != TURBO_FLOW_CNET_STREAM_SINK_FAILED) {
    atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_STREAM_SINK_FAILED, memory_order_release);
  }
  stream_sink_drive_actor(sink);
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (snapshot) {
    int snapshot_status = stream_sink_snapshot_locked(sink, snapshot);
    if (rc == SALTS_OK && snapshot_status != SALTS_OK) rc = snapshot_status;
  }
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  if (stream_sink_state(sink) == TURBO_FLOW_CNET_STREAM_SINK_FAILED)
    rc = atomic_load_explicit(&sink->status, memory_order_relaxed);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return rc;
}

int turbo_flow_cnet_stream_sink_destroy(turbo_flow_cnet_stream_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (!atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      atomic_load_explicit(&sink->client_initialized, memory_order_acquire) ||
      sink->actor_initialized || sink->executor_initialized || sink->owner_active ||
      sink->submissions_active > 0u) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  salts_mutex_unlock(&sink->lifecycle_mutex);
  tstr_free(sink->adapter_name);
  tstr_free(sink->uri);
  stream_sink_lifecycle_destroy(sink);
  free(sink);
  return SALTS_OK;
}
