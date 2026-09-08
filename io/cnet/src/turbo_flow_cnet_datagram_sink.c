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

enum { DATAGRAM_SINK_RESOURCE_GENERATION = 1u };

static const char DATAGRAM_SINK_RESOURCE_UID_PREFIX[] = "cnet-datagram-sink:";
static const char DATAGRAM_SINK_HASHED_OWNER_PREFIX[] = "xxh3-128:";
static const char DATAGRAM_SINK_SCHEMA_NAME[] = "CNetDatagram";
static const char DATAGRAM_SINK_SCHEMA_TYPE[] = "NonEmptyBytes";
static const char DATAGRAM_SINK_MEDIA_TYPE[] = "application/octet-stream";

typedef struct datagram_sink_operation_s {
  struct turbo_flow_cnet_datagram_sink_s *sink;
  turbo_flow_async_terminal_claim_t claim;
  size_t bytes;
} datagram_sink_operation_t;

struct turbo_flow_cnet_datagram_sink_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr host;
  char managed_owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char resource_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  cnet_datagram_config config;
  cnet_datagram_peer peer;
  cnet_datagram datagram;
  cflow_executor executor;
  cflow_io_actor actor;
  cflow_io_request_id *delivered_ids;
  size_t delivered_count;
  size_t delivered_capacity;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  uint32_t stop_timeout_ms;
  uint16_t bound_port;
  atomic_uint_fast64_t next_lease;
  atomic_uint_fast64_t messages_sent;
  atomic_uint_fast64_t bytes_sent;
  atomic_uint_fast64_t accepted;
  atomic_uint_fast64_t completed;
  atomic_uint_fast64_t rejected;
  atomic_int state;
  atomic_int status;
  atomic_bool datagram_initialized;
  bool executor_initialized;
  bool actor_initialized;
  atomic_bool detached;
  salts_mutex_t lifecycle_mutex;
  salts_cond_t lifecycle_cond;
  bool lifecycle_initialized;
  bool owner_active;
  bool starting;
  size_t submissions_active;
};

typedef enum datagram_sink_cleanup_mode_e {
  DATAGRAM_SINK_CLEANUP_STOP,
  DATAGRAM_SINK_CLEANUP_FAILURE
} datagram_sink_cleanup_mode_t;

static void datagram_sink_cleanup_internal(turbo_flow_cnet_datagram_sink_t *sink,
                                           datagram_sink_cleanup_mode_t mode);

static int datagram_sink_managed_identity_init(turbo_flow_cnet_datagram_sink_t *sink) {
  const size_t adapter_name_len = tstr_len(sink->adapter_name);
  int written;
  if (adapter_name_len <= TURBO_FLOW_RESOURCE_OWNER_MAX) {
    memcpy(sink->managed_owner_name, sink->adapter_name, adapter_name_len + 1u);
  } else {
    const XXH128_hash_t hash = XXH3_128bits(sink->adapter_name, adapter_name_len);
    written = snprintf(sink->managed_owner_name, sizeof(sink->managed_owner_name),
                       "%s%016" PRIx64 "%016" PRIx64, DATAGRAM_SINK_HASHED_OWNER_PREFIX,
                       hash.high64, hash.low64);
    if (written < 0 || (size_t)written >= sizeof(sink->managed_owner_name)) return SALTS_ERANGE;
  }
  written = snprintf(sink->resource_uid, sizeof(sink->resource_uid), "%s%s",
                     DATAGRAM_SINK_RESOURCE_UID_PREFIX, sink->managed_owner_name);
  if (written < 0 || (size_t)written >= sizeof(sink->resource_uid)) return SALTS_ERANGE;
  return SALTS_OK;
}

static int datagram_sink_lifecycle_init(turbo_flow_cnet_datagram_sink_t *sink) {
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

static void datagram_sink_lifecycle_destroy(turbo_flow_cnet_datagram_sink_t *sink) {
  if (!sink || !sink->lifecycle_initialized) return;
  salts_cond_destroy(&sink->lifecycle_cond);
  salts_mutex_destroy(&sink->lifecycle_mutex);
  sink->lifecycle_initialized = false;
}

static turbo_flow_cnet_datagram_sink_state_t
datagram_sink_state(const turbo_flow_cnet_datagram_sink_t *sink) {
  return (turbo_flow_cnet_datagram_sink_state_t)atomic_load_explicit(&sink->state,
                                                                     memory_order_acquire);
}

static turbo_flow_managed_boundary_state_t
datagram_sink_managed_state(turbo_flow_cnet_datagram_sink_state_t state, bool starting,
                            size_t active_requests) {
  if (starting) return TURBO_FLOW_MANAGED_BOUNDARY_STARTING;
  switch (state) {
  case TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED:
    return TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  case TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING:
    return TURBO_FLOW_MANAGED_BOUNDARY_RUNNING;
  case TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING:
    return active_requests > 0u ? TURBO_FLOW_MANAGED_BOUNDARY_DRAINING
                                : TURBO_FLOW_MANAGED_BOUNDARY_STOPPING;
  case TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED:
  case TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED:
    return TURBO_FLOW_MANAGED_BOUNDARY_STOPPED;
  case TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED:
  default:
    return TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  }
}

static int datagram_sink_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(metadata.uid, sink->resource_uid, strlen(sink->resource_uid) + 1u);
  memcpy(metadata.owner_name, sink->managed_owner_name, strlen(sink->managed_owner_name) + 1u);
  metadata.generation = DATAGRAM_SINK_RESOURCE_GENERATION;
  metadata.observed_generation = DATAGRAM_SINK_RESOURCE_GENERATION;
  *out = metadata;
  return SALTS_OK;
}

static int datagram_sink_managed_descriptor(void *ctx,
                                            turbo_flow_managed_boundary_descriptor_t *out) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
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
      TURBO_FLOW_DATA_ENCODING_OPAQUE, DATAGRAM_SINK_MEDIA_TYPE, sink->managed_owner_name);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(&descriptor.input, DATAGRAM_SINK_SCHEMA_NAME,
                                                    DATAGRAM_SINK_SCHEMA_TYPE, 1u);
  if (rc != SALTS_OK) return rc;
  *out = descriptor;
  return SALTS_OK;
}

static int datagram_sink_managed_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  size_t queue_depth;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (sink->submissions_active > 0u) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  stats.request_capacity = sink->config.send_capacity;
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
  snapshot.generation = DATAGRAM_SINK_RESOURCE_GENERATION;
  snapshot.observed_generation = DATAGRAM_SINK_RESOURCE_GENERATION;
  snapshot.state =
      datagram_sink_managed_state(datagram_sink_state(sink), sink->starting, stats.active_requests);
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

static void datagram_sink_counter_increment(atomic_uint_fast64_t *counter) {
  uint_fast64_t current = atomic_load_explicit(counter, memory_order_relaxed);
  while (current != UINT64_MAX &&
         !atomic_compare_exchange_weak_explicit(counter, &current, current + 1u,
                                                memory_order_relaxed, memory_order_relaxed)) {
  }
}

static int datagram_sink_reject(turbo_flow_cnet_datagram_sink_t *sink, int status) {
  if (sink) {
    salts_mutex_lock(&sink->lifecycle_mutex);
    datagram_sink_counter_increment(&sink->rejected);
    salts_mutex_unlock(&sink->lifecycle_mutex);
  }
  return status;
}

static void datagram_sink_submission_finish(turbo_flow_cnet_datagram_sink_t *sink, bool accepted) {
  salts_mutex_lock(&sink->lifecycle_mutex);
  datagram_sink_counter_increment(accepted ? &sink->accepted : &sink->rejected);
  --sink->submissions_active;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static void datagram_sink_operation_complete(datagram_sink_operation_t *operation, int status) {
  if (!operation || !operation->claim._impl) return;
  if (turbo_flow_async_terminal_complete(&operation->claim, status, NULL) == SALTS_OK) {
    salts_mutex_lock(&operation->sink->lifecycle_mutex);
    datagram_sink_counter_increment(&operation->sink->completed);
    salts_mutex_unlock(&operation->sink->lifecycle_mutex);
  }
}

static int datagram_sink_submit_status(cflow_io_submit_status status) {
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

static int datagram_sink_completion_status(const cflow_io_completion *completion) {
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

static void datagram_sink_operation_release(void *user) {
  datagram_sink_operation_t *operation = (datagram_sink_operation_t *)user;
  if (!operation) return;
  datagram_sink_operation_complete(operation, SALTS_ECANCELED);
  free(operation);
}

static int datagram_sink_backend_submit(void *user, cflow_io_actor *actor,
                                        cflow_io_request_id request_id, cflow_io_lease_id lease_id,
                                        void *operation_user) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  datagram_sink_operation_t *operation = (datagram_sink_operation_t *)operation_user;
  const turbo_flow_msg_t *message;
  (void)actor;
  (void)lease_id;
  if (!sink || !operation) return SALTS_EINVAL;
  message = turbo_flow_async_terminal_claim_message(&operation->claim);
  if (!message || !message->payload.data || message->payload.len == 0u ||
      message->payload.len != operation->bytes) {
    return SALTS_EPROTO;
  }
  return cnet_datagram_send(&sink->datagram, &sink->peer, message->payload.data,
                            message->payload.len, request_id);
}

static int datagram_sink_backend_cancel(void *user, cflow_io_request_id request_id) {
  (void)user;
  (void)request_id;
  return SALTS_ENOTSUP;
}

static void datagram_sink_actor_completion(void *user, cflow_io_request_id request_id,
                                           cflow_io_lease_id lease_id, void *operation_user,
                                           const cflow_io_completion *completion) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  datagram_sink_operation_t *operation = (datagram_sink_operation_t *)operation_user;
  int status = datagram_sink_completion_status(completion);
  (void)lease_id;
  if (!sink || !operation) return;
  if (status == SALTS_OK && completion->bytes != operation->bytes) status = SALTS_EPROTO;
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
  if (sink->delivered_count < sink->delivered_capacity)
    sink->delivered_ids[sink->delivered_count++] = request_id;
  else status = SALTS_EPROTO;
  datagram_sink_operation_complete(operation, status);
}

static void datagram_sink_actor_wake(void *user) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  if (sink && atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire))
    (void)cnet_datagram_wake(&sink->datagram);
}

static void datagram_sink_on_receive(void *user, cnet_datagram *datagram,
                                     const cnet_datagram_peer *peer,
                                     const cnet_receive_view *view) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  (void)datagram;
  (void)peer;
  (void)view;
  if (!sink) return;
  atomic_store_explicit(&sink->status, SALTS_EPROTO, memory_order_relaxed);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED, memory_order_release);
}

static void datagram_sink_on_send(void *user, cnet_datagram *datagram,
                                  const cnet_datagram_peer *peer, size_t size, int status,
                                  uint64_t tag) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)user;
  cflow_io_completion completion;
  (void)datagram;
  (void)peer;
  if (!sink || tag == 0u) return;
  if (status == SALTS_OK) {
    completion = (cflow_io_completion){CFLOW_IO_COMPLETION_OK, size, SALTS_OK};
  } else if (status == SALTS_ECANCELED) {
    completion = (cflow_io_completion){CFLOW_IO_COMPLETION_CANCELLED, 0u, SALTS_OK};
  } else {
    completion = (cflow_io_completion){CFLOW_IO_COMPLETION_FAILED, 0u, status};
  }
  (void)cflow_io_actor_complete(&sink->actor, (cflow_io_request_id)tag, &completion);
}

static void datagram_sink_acknowledge(turbo_flow_cnet_datagram_sink_t *sink) {
  size_t index;
  if (!sink) return;
  for (index = 0u; index < sink->delivered_count; ++index)
    (void)cflow_io_actor_acknowledge(&sink->actor, sink->delivered_ids[index]);
  sink->delivered_count = 0u;
}

static void datagram_sink_drive_actor(turbo_flow_cnet_datagram_sink_t *sink) {
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
  (void)cflow_executor_run_ready(&sink->executor);
  datagram_sink_acknowledge(sink);
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
}

static int datagram_sink_start(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  cflow_io_actor_config actor_config = {0};
  int rc;
  (void)stage;
  if (!sink || flow != sink->flow) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) || sink->owner_active ||
      sink->submissions_active > 0u ||
      (datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED &&
       datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  sink->owner_active = true;
  sink->starting = true;
  atomic_store_explicit(&sink->status, SALTS_OK, memory_order_relaxed);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  sink->config.host = sink->host;
  sink->config.observer.on_receive = datagram_sink_on_receive;
  sink->config.observer.on_send = datagram_sink_on_send;
  sink->config.observer.user = sink;
  rc = cnet_datagram_init(&sink->datagram, &sink->config);
  if (rc != SALTS_OK) goto fail;
  atomic_store_explicit(&sink->datagram_initialized, true, memory_order_release);
  rc = cnet_datagram_port(&sink->datagram, &sink->bound_port);
  if (rc != SALTS_OK) goto fail;
  if (!cflow_executor_manual_init_with_capacity(&sink->executor, sink->config.send_capacity)) {
    rc = SALTS_ENOMEM;
    goto fail;
  }
  sink->executor_initialized = true;
  actor_config.request_capacity = sink->config.send_capacity;
  actor_config.command_capacity = sink->actor_command_capacity;
  actor_config.executor = &sink->executor;
  actor_config.backend.submit = datagram_sink_backend_submit;
  actor_config.backend.cancel = datagram_sink_backend_cancel;
  actor_config.backend_user = sink;
  actor_config.completion = datagram_sink_actor_completion;
  actor_config.completion_user = sink;
  actor_config.wake = datagram_sink_actor_wake;
  actor_config.wake_user = sink;
  rc = cflow_io_actor_init(&sink->actor, &actor_config);
  if (rc != SALTS_OK) goto fail;
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->actor_initialized = true;
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING, memory_order_release);
  sink->starting = false;
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;

fail:
  atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->starting = false;
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED, memory_order_release);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  datagram_sink_cleanup_internal(sink, DATAGRAM_SINK_CLEANUP_FAILURE);
  return rc;
}

static void datagram_sink_cleanup_internal(turbo_flow_cnet_datagram_sink_t *sink,
                                           datagram_sink_cleanup_mode_t mode) {
  const bool failure_cleanup = mode == DATAGRAM_SINK_CLEANUP_FAILURE;
  int rc = SALTS_OK;
  if (!sink) return;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (!failure_cleanup && (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED ||
                           datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return;
  }
  if (!failure_cleanup &&
      !(datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED && sink->owner_active))
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING,
                          memory_order_release);
  while ((!failure_cleanup && sink->owner_active) || sink->submissions_active > 0u)
    salts_cond_wait(&sink->lifecycle_cond, &sink->lifecycle_mutex);
  if (!failure_cleanup && (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED ||
                           datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return;
  }
  if (!failure_cleanup)
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING,
                          memory_order_release);
  sink->owner_active = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  if (sink->actor_initialized) {
    (void)cflow_io_actor_close(&sink->actor);
    datagram_sink_drive_actor(sink);
  }
  if (atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) {
    do {
      rc = cnet_datagram_stop(&sink->datagram, sink->stop_timeout_ms);
    } while (rc == SALTS_ETIMEDOUT);
    if (rc != SALTS_OK && atomic_load_explicit(&sink->status, memory_order_relaxed) == SALTS_OK)
      atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
  }
  while (sink->actor_initialized && !cflow_io_actor_is_quiescent(&sink->actor))
    datagram_sink_drive_actor(sink);
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
  if (atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire)) {
    (void)cnet_datagram_destroy(&sink->datagram);
    atomic_store_explicit(&sink->datagram_initialized, false, memory_order_release);
  }
  sink->bound_port = 0u;
  salts_mutex_lock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->state,
                        failure_cleanup ? TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED
                                        : TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED,
                        memory_order_release);
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static void datagram_sink_stop(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  (void)flow;
  (void)stage;
  datagram_sink_cleanup_internal((turbo_flow_cnet_datagram_sink_t *)ctx,
                                 DATAGRAM_SINK_CLEANUP_STOP);
}

static void datagram_sink_shutdown(void *ctx) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  if (!sink) return;
  datagram_sink_cleanup_internal(sink, DATAGRAM_SINK_CLEANUP_STOP);
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->flow = NULL;
  atomic_store_explicit(&sink->detached, true, memory_order_release);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_DETACHED, memory_order_release);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static int datagram_sink_next_lease(turbo_flow_cnet_datagram_sink_t *sink,
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

static int datagram_sink_async_submit(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage,
                                      const turbo_flow_msg_t *message,
                                      turbo_flow_async_terminal_claim_t *claim) {
  turbo_flow_cnet_datagram_sink_t *sink = (turbo_flow_cnet_datagram_sink_t *)ctx;
  datagram_sink_operation_t *operation;
  cflow_io_operation io_operation;
  cflow_io_submit_result submitted;
  cflow_io_lease_id lease_id;
  turbo_flow_cnet_datagram_sink_state_t state;
  int rc;
  (void)stage;
  if (!sink || !message || !claim) return datagram_sink_reject(sink, SALTS_EINVAL);
  if (!message->payload.data || message->payload.len == 0u)
    return datagram_sink_reject(sink, SALTS_EINVAL);
  if (message->payload.len > sink->max_message_bytes)
    return datagram_sink_reject(sink, SALTS_EMSGSIZE);
  operation = (datagram_sink_operation_t *)calloc(1u, sizeof(*operation));
  if (!operation) return datagram_sink_reject(sink, SALTS_ENOMEM);
  operation->sink = sink;
  operation->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  operation->bytes = message->payload.len;
  salts_mutex_lock(&sink->lifecycle_mutex);
  state = datagram_sink_state(sink);
  if (flow != sink->flow || atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !sink->actor_initialized || state != TURBO_FLOW_CNET_DATAGRAM_SINK_RUNNING) {
    rc = state == TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED
             ? atomic_load_explicit(&sink->status, memory_order_relaxed)
             : SALTS_EBUSY;
    salts_mutex_unlock(&sink->lifecycle_mutex);
    free(operation);
    return datagram_sink_reject(sink, rc);
  }
  ++sink->submissions_active;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  rc = datagram_sink_next_lease(sink, &lease_id);
  if (rc != SALTS_OK) {
    free(operation);
    datagram_sink_submission_finish(sink, false);
    return rc;
  }
  rc = turbo_flow_async_terminal_claim_move(&operation->claim, claim);
  if (rc != SALTS_OK) {
    free(operation);
    datagram_sink_submission_finish(sink, false);
    return rc;
  }
  io_operation.user = operation;
  io_operation.release = datagram_sink_operation_release;
  submitted = cflow_io_actor_try_submit(&sink->actor, lease_id, &io_operation);
  rc = datagram_sink_submit_status(submitted.status);
  if (rc != SALTS_OK) {
    (void)turbo_flow_async_terminal_claim_move(claim, &operation->claim);
    free(operation);
    datagram_sink_submission_finish(sink, false);
    return rc;
  }
  datagram_sink_submission_finish(sink, true);
  return SALTS_OK;
}

static int datagram_sink_config_validate(const turbo_flow_cnet_datagram_sink_config_t *config) {
  const cnet_datagram_config *datagram;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->datagram ||
      config->max_message_bytes == 0u || config->actor_command_capacity == 0u ||
      config->actor_max_steps_per_poll == 0u || config->stop_timeout_ms == 0u) {
    return SALTS_EINVAL;
  }
  if (turbo_flow_state(config->flow) == TURBO_FLOW_STATE_COMPILED ||
      turbo_flow_state(config->flow) == TURBO_FLOW_STATE_STARTED)
    return SALTS_EBUSY;
  datagram = config->datagram;
  if (datagram->size != sizeof(*datagram) || !datagram->host || datagram->host[0] == '\0' ||
      datagram->send_capacity == 0u || datagram->request_capacity <= datagram->send_capacity ||
      datagram->completion_batch_capacity == 0u ||
      datagram->completion_batch_capacity > datagram->request_capacity ||
      datagram->max_datagram_bytes == 0u ||
      datagram->max_datagram_bytes > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      datagram->receive_buffer_bytes < datagram->max_datagram_bytes ||
      datagram->observer.on_receive || datagram->observer.on_send || datagram->observer.user ||
      (datagram->reuse_port != 0 && datagram->reuse_port != 1) ||
      !native_io_backend_kind_supported(datagram->backend) ||
      config->max_message_bytes > datagram->max_datagram_bytes || config->peer.port == 0u ||
      (config->peer.family != CNET_DATAGRAM_ADDRESS_IPV4 &&
       config->peer.family != CNET_DATAGRAM_ADDRESS_IPV6)) {
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

int turbo_flow_cnet_datagram_sink_register(const turbo_flow_cnet_datagram_sink_config_t *config,
                                           turbo_flow_cnet_datagram_sink_t **sink_out) {
  turbo_flow_cnet_datagram_sink_t *sink;
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
  rc = datagram_sink_config_validate(config);
  if (rc != SALTS_OK) return rc;
  sink = (turbo_flow_cnet_datagram_sink_t *)calloc(1u, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  rc = datagram_sink_lifecycle_init(sink);
  if (rc != SALTS_OK) {
    free(sink);
    return rc;
  }
  sink->flow = config->flow;
  sink->adapter_name = tstr_dup(config->adapter_name);
  sink->host = tstr_dup(config->datagram->host);
  sink->config = *config->datagram;
  sink->peer = config->peer;
  sink->max_message_bytes = config->max_message_bytes;
  sink->actor_command_capacity = config->actor_command_capacity;
  sink->actor_max_steps_per_poll = config->actor_max_steps_per_poll;
  sink->stop_timeout_ms = config->stop_timeout_ms;
  sink->delivered_capacity = config->datagram->send_capacity;
  sink->delivered_ids =
      (cflow_io_request_id *)calloc(sink->delivered_capacity, sizeof(*sink->delivered_ids));
  atomic_init(&sink->next_lease, 1u);
  atomic_init(&sink->messages_sent, 0u);
  atomic_init(&sink->bytes_sent, 0u);
  atomic_init(&sink->accepted, 0u);
  atomic_init(&sink->completed, 0u);
  atomic_init(&sink->rejected, 0u);
  atomic_init(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_REGISTERED);
  atomic_init(&sink->status, SALTS_OK);
  atomic_init(&sink->datagram_initialized, false);
  atomic_init(&sink->detached, false);
  if (!sink->adapter_name || !sink->host || !sink->delivered_ids) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->host);
    free(sink->delivered_ids);
    datagram_sink_lifecycle_destroy(sink);
    free(sink);
    return SALTS_ENOMEM;
  }
  rc = datagram_sink_managed_identity_init(sink);
  if (rc != SALTS_OK) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->host);
    free(sink->delivered_ids);
    datagram_sink_lifecycle_destroy(sink);
    free(sink);
    return rc;
  }
  adapter_ops.start = datagram_sink_start;
  adapter_ops.stop = datagram_sink_stop;
  adapter_ops.shutdown = datagram_sink_shutdown;
  async_ops.submit = datagram_sink_async_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  boundary_ops.resource.metadata = datagram_sink_resource_metadata;
  boundary_ops.descriptor = datagram_sink_managed_descriptor;
  boundary_ops.snapshot = datagram_sink_managed_snapshot;
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
    tstr_free(sink->host);
    free(sink->delivered_ids);
    datagram_sink_lifecycle_destroy(sink);
    free(sink);
    return rc;
  }
  *sink_out = sink;
  return SALTS_OK;
}

static int datagram_sink_snapshot_locked(const turbo_flow_cnet_datagram_sink_t *sink,
                                         turbo_flow_cnet_datagram_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_datagram_sink_snapshot_t current = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  current.state = datagram_sink_state(sink);
  current.status = atomic_load_explicit(&sink->status, memory_order_relaxed);
  current.bound_port = sink->bound_port;
  current.messages_sent = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
  current.bytes_sent = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
  if (sink->actor_initialized) {
    if (!cflow_io_actor_get_stats(&sink->actor, &stats)) return SALTS_EPROTO;
    current.active_requests = stats.active_requests;
  }
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_datagram_sink_snapshot(const turbo_flow_cnet_datagram_sink_t *sink,
                                           turbo_flow_cnet_datagram_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_datagram_sink_t *mutable_sink = (turbo_flow_cnet_datagram_sink_t *)sink;
  int rc;
  if (!sink || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION)
    return SALTS_EINVAL;
  salts_mutex_lock(&mutable_sink->lifecycle_mutex);
  if (mutable_sink->owner_active || mutable_sink->submissions_active > 0u) {
    salts_mutex_unlock(&mutable_sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  rc = datagram_sink_snapshot_locked(sink, snapshot);
  salts_mutex_unlock(&mutable_sink->lifecycle_mutex);
  return rc;
}

int turbo_flow_cnet_datagram_sink_poll(turbo_flow_cnet_datagram_sink_t *sink, uint32_t timeout_ms,
                                       turbo_flow_cnet_datagram_sink_snapshot_t *snapshot) {
  size_t events = 0u;
  int rc;
  if (!sink || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                             snapshot->version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION)))
    return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire) ||
      !sink->actor_initialized) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  if (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPING ||
      datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_ESHUTDOWN;
  }
  if (sink->owner_active) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->owner_active = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  datagram_sink_drive_actor(sink);
  rc = cnet_datagram_poll(&sink->datagram, timeout_ms, &events);
  if (rc != SALTS_OK && datagram_sink_state(sink) != TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED) {
    atomic_store_explicit(&sink->status, rc, memory_order_relaxed);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED, memory_order_release);
  }
  datagram_sink_drive_actor(sink);
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (snapshot) {
    int snapshot_status = datagram_sink_snapshot_locked(sink, snapshot);
    if (rc == SALTS_OK && snapshot_status != SALTS_OK) rc = snapshot_status;
  }
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  if (datagram_sink_state(sink) == TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED)
    rc = atomic_load_explicit(&sink->status, memory_order_relaxed);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return rc;
}

int turbo_flow_cnet_datagram_sink_destroy(turbo_flow_cnet_datagram_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (!atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      atomic_load_explicit(&sink->datagram_initialized, memory_order_acquire) ||
      sink->actor_initialized || sink->executor_initialized || sink->owner_active ||
      sink->submissions_active > 0u) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  salts_mutex_unlock(&sink->lifecycle_mutex);
  tstr_free(sink->adapter_name);
  tstr_free(sink->host);
  free(sink->delivered_ids);
  datagram_sink_lifecycle_destroy(sink);
  free(sink);
  return SALTS_OK;
}
