#include "turbo_flow_cnet_managed_sink.h"

#include <cflow/executor.h>
#include <cflow/io_actor.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#if !defined(CNET_STOP_DRAIN_CONTRACT_VERSION) || CNET_STOP_DRAIN_CONTRACT_VERSION < 1u
  #error "TurboFlow CNet packet sink requires stop-drain contract v1"
#endif

enum { PACKET_SINK_TAG_SLOT_BITS = 32u, PACKET_SINK_RESOURCE_GENERATION = 1u };

static const char PACKET_SINK_RESOURCE_UID_PREFIX[] = "cnet-packet-sink:";
static const char PACKET_SINK_MEDIA_TYPE[] = "application/octet-stream";
static const char PACKET_SINK_SCHEMA_NAME[] = "CNetPacket";
static const char PACKET_SINK_SCHEMA_TYPE[] = "NonEmptyBytes";

typedef struct packet_sink_operation_s packet_sink_operation_t;

struct packet_sink_operation_s {
  struct turbo_flow_cnet_packet_sink_s *sink;
  turbo_flow_async_terminal_claim_t claim;
  cnet_packet_session session;
  atomic_uint_fast64_t request_id;
  uint64_t tag;
  size_t bytes;
  uint32_t slot;
  atomic_uint_fast32_t generation;
  atomic_bool ready;
  atomic_bool occupied;
};

struct turbo_flow_cnet_packet_sink_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr host;
  turbo_flow_cnet_sink_identity_t identity;
  cnet_packet_endpoint_config config;
  cnet_datagram_peer peer;
  uint32_t conversation;
  cnet_packet_endpoint endpoint;
  cnet_packet_session session;
  cflow_executor executor;
  cflow_io_actor actor;
  packet_sink_operation_t *operations;
  cflow_io_request_id *delivered_ids;
  size_t delivered_count;
  size_t send_capacity;
  size_t max_message_bytes;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  uint32_t stop_timeout_ms;
  uint16_t bound_port;
  atomic_uint_fast64_t messages_sent;
  atomic_uint_fast64_t bytes_sent;
  atomic_uint_fast64_t terminals_failed;
  atomic_uint_fast64_t accepted;
  atomic_uint_fast64_t completed;
  atomic_uint_fast64_t rejected;
  atomic_int state;
  atomic_int status;
  atomic_bool session_open;
  atomic_bool endpoint_initialized;
  atomic_bool detached;
  salts_mutex_t lifecycle_mutex;
  salts_cond_t lifecycle_cond;
  bool lifecycle_initialized;
  bool owner_active;
  bool starting;
  size_t submissions_active;
  bool executor_initialized;
  bool actor_initialized;
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
  int test_stop_status;
  cnet_packet_session test_last_terminal_session;
  size_t test_last_terminal_size;
  int test_last_terminal_status;
  uint64_t test_last_terminal_tag;
  bool test_last_terminal_available;
#endif
};

static int packet_sink_stop_internal(turbo_flow_cnet_packet_sink_t *sink);

static int packet_sink_managed_identity_init(turbo_flow_cnet_packet_sink_t *sink) {
  return turbo_flow_cnet_sink_identity_init(&sink->identity, sink->adapter_name,
                                            PACKET_SINK_RESOURCE_UID_PREFIX);
}

static int packet_sink_lifecycle_init(turbo_flow_cnet_packet_sink_t *sink) {
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

static void packet_sink_secure_wipe(void *data, size_t size) {
  volatile unsigned char *cursor = (volatile unsigned char *)data;
  while (size-- != 0u)
    *cursor++ = 0u;
}

static void packet_sink_config_wipe(turbo_flow_cnet_packet_sink_t *sink) {
  if (!sink) return;
  packet_sink_secure_wipe(&sink->config.security, sizeof(sink->config.security));
}

static void packet_sink_lifecycle_destroy(turbo_flow_cnet_packet_sink_t *sink) {
  if (!sink || !sink->lifecycle_initialized) return;
  salts_cond_destroy(&sink->lifecycle_cond);
  salts_mutex_destroy(&sink->lifecycle_mutex);
  sink->lifecycle_initialized = false;
}

static void packet_sink_storage_destroy(turbo_flow_cnet_packet_sink_t *sink) {
  if (!sink) return;
  packet_sink_config_wipe(sink);
  tstr_free(sink->adapter_name);
  tstr_free(sink->host);
  free(sink->operations);
  free(sink->delivered_ids);
  sink->adapter_name = NULL;
  sink->host = NULL;
  sink->operations = NULL;
  sink->delivered_ids = NULL;
}

static turbo_flow_cnet_packet_sink_state_t
packet_sink_state(const turbo_flow_cnet_packet_sink_t *sink) {
  return (turbo_flow_cnet_packet_sink_state_t)atomic_load_explicit(&sink->state,
                                                                   memory_order_acquire);
}

static turbo_flow_managed_boundary_state_t
packet_sink_managed_state(turbo_flow_cnet_packet_sink_state_t state, bool starting,
                          size_t active_requests) {
  if (starting) return TURBO_FLOW_MANAGED_BOUNDARY_STARTING;
  switch (state) {
  case TURBO_FLOW_CNET_PACKET_SINK_REGISTERED:
    return TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  case TURBO_FLOW_CNET_PACKET_SINK_RUNNING:
    return TURBO_FLOW_MANAGED_BOUNDARY_RUNNING;
  case TURBO_FLOW_CNET_PACKET_SINK_STOPPING:
    return active_requests > 0u ? TURBO_FLOW_MANAGED_BOUNDARY_DRAINING
                                : TURBO_FLOW_MANAGED_BOUNDARY_STOPPING;
  case TURBO_FLOW_CNET_PACKET_SINK_STOPPED:
  case TURBO_FLOW_CNET_PACKET_SINK_DETACHED:
    return TURBO_FLOW_MANAGED_BOUNDARY_STOPPED;
  case TURBO_FLOW_CNET_PACKET_SINK_FAILED:
  default:
    return TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  }
}

static int packet_sink_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(metadata.uid, sink->identity.uid, strlen(sink->identity.uid) + 1u);
  memcpy(metadata.owner_name, sink->identity.owner, strlen(sink->identity.owner) + 1u);
  metadata.generation = PACKET_SINK_RESOURCE_GENERATION;
  metadata.observed_generation = PACKET_SINK_RESOURCE_GENERATION;
  *out = metadata;
  return SALTS_OK;
}

static int packet_sink_managed_descriptor(void *ctx,
                                          turbo_flow_managed_boundary_descriptor_t *out) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)ctx;
  turbo_flow_managed_boundary_descriptor_t descriptor = TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  int status;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  descriptor.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  descriptor.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(descriptor.uid, sink->identity.uid, strlen(sink->identity.uid) + 1u);
  memcpy(descriptor.owner_name, sink->identity.owner, strlen(sink->identity.owner) + 1u);
  descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT;
  descriptor.command_flags = 0u;
  status = turbo_flow_content_descriptor_init(
      &descriptor.input, TURBO_FLOW_DOMAIN_IO_TRANSPORT, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
      TURBO_FLOW_DATA_ENCODING_OPAQUE, PACKET_SINK_MEDIA_TYPE, sink->identity.owner);
  if (status != SALTS_OK) return status;
  status = turbo_flow_content_descriptor_declare_schema(&descriptor.input, PACKET_SINK_SCHEMA_NAME,
                                                        PACKET_SINK_SCHEMA_TYPE, 1u);
  if (status != SALTS_OK) return status;
  *out = descriptor;
  return SALTS_OK;
}

static int packet_sink_managed_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)ctx;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  size_t queue_depth;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (sink->submissions_active > 0u) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  stats.request_capacity = sink->send_capacity;
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
  memcpy(snapshot.uid, sink->identity.uid, strlen(sink->identity.uid) + 1u);
  snapshot.generation = PACKET_SINK_RESOURCE_GENERATION;
  snapshot.observed_generation = PACKET_SINK_RESOURCE_GENERATION;
  snapshot.state =
      packet_sink_managed_state(packet_sink_state(sink), sink->starting, stats.active_requests);
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

static bool packet_sink_session_equal(cnet_packet_session left, cnet_packet_session right) {
  return left.slot == right.slot && left.generation == right.generation;
}

static void packet_sink_fail(turbo_flow_cnet_packet_sink_t *sink, int status) {
  turbo_flow_cnet_packet_sink_state_t state;
  int expected = SALTS_OK;
  if (!sink) return;
  state = packet_sink_state(sink);
  if (state == TURBO_FLOW_CNET_PACKET_SINK_STOPPING ||
      state == TURBO_FLOW_CNET_PACKET_SINK_STOPPED || state == TURBO_FLOW_CNET_PACKET_SINK_DETACHED)
    return;
  if (status == SALTS_OK) status = SALTS_EIO;
  (void)atomic_compare_exchange_strong_explicit(&sink->status, &expected, status,
                                                memory_order_relaxed, memory_order_relaxed);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_FAILED, memory_order_release);
}

static int packet_sink_reject(turbo_flow_cnet_packet_sink_t *sink, int status) {
  if (sink) turbo_flow_cnet_sink_counter_increment(&sink->rejected);
  return status;
}

static void packet_sink_submission_finish(turbo_flow_cnet_packet_sink_t *sink, bool accepted) {
  salts_mutex_lock(&sink->lifecycle_mutex);
  turbo_flow_cnet_sink_counter_increment(accepted ? &sink->accepted : &sink->rejected);
  --sink->submissions_active;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static int packet_sink_submit_status(cflow_io_submit_status status) {
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

static int packet_sink_completion_status(const cflow_io_completion *completion) {
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

static void packet_sink_operation_reset(packet_sink_operation_t *operation) {
  if (!operation) return;
  atomic_store_explicit(&operation->ready, false, memory_order_release);
  operation->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  operation->session = (cnet_packet_session){0};
  atomic_store_explicit(&operation->request_id, 0u, memory_order_relaxed);
  operation->tag = 0u;
  operation->bytes = 0u;
  atomic_store_explicit(&operation->occupied, false, memory_order_release);
}

static void packet_sink_operation_release(void *user) {
  packet_sink_operation_t *operation = (packet_sink_operation_t *)user;
  if (!operation) return;
  if (operation->claim._impl) {
    if (turbo_flow_async_terminal_complete(&operation->claim, SALTS_ECANCELED, NULL) == SALTS_OK)
      turbo_flow_cnet_sink_counter_increment(&operation->sink->completed);
    else packet_sink_fail(operation->sink, SALTS_EPROTO);
  }
  packet_sink_operation_reset(operation);
}

static int packet_sink_operation_acquire(turbo_flow_cnet_packet_sink_t *sink,
                                         packet_sink_operation_t **operation_out) {
  size_t index;
  bool generation_exhausted = false;
  if (!sink || !operation_out) return SALTS_EINVAL;
  *operation_out = NULL;
  for (index = 0u; index < sink->send_capacity; ++index) {
    packet_sink_operation_t *operation = &sink->operations[index];
    uint_fast32_t generation;
    bool expected = false;
    if (!atomic_compare_exchange_strong_explicit(&operation->occupied, &expected, true,
                                                 memory_order_acq_rel, memory_order_relaxed))
      continue;
    generation = atomic_load_explicit(&operation->generation, memory_order_relaxed);
    if (generation == UINT32_MAX) {
      generation_exhausted = true;
      atomic_store_explicit(&operation->occupied, false, memory_order_release);
      continue;
    }
    ++generation;
    atomic_store_explicit(&operation->generation, generation, memory_order_relaxed);
    operation->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    operation->session = sink->session;
    atomic_store_explicit(&operation->request_id, 0u, memory_order_relaxed);
    operation->bytes = 0u;
    operation->tag = ((uint64_t)(uint32_t)generation << PACKET_SINK_TAG_SLOT_BITS) |
                     (uint64_t)(operation->slot + 1u);
    *operation_out = operation;
    return SALTS_OK;
  }
  return generation_exhausted ? SALTS_ERANGE : SALTS_ENOSPC;
}

static packet_sink_operation_t *packet_sink_operation_from_tag(turbo_flow_cnet_packet_sink_t *sink,
                                                               uint64_t tag) {
  uint32_t encoded_slot;
  uint32_t generation;
  packet_sink_operation_t *operation;
  if (!sink || tag == 0u) return NULL;
  encoded_slot = (uint32_t)(tag & UINT32_MAX);
  generation = (uint32_t)(tag >> PACKET_SINK_TAG_SLOT_BITS);
  if (encoded_slot == 0u || generation == 0u || encoded_slot > sink->send_capacity) return NULL;
  operation = &sink->operations[encoded_slot - 1u];
  if (!atomic_load_explicit(&operation->occupied, memory_order_acquire) ||
      !atomic_load_explicit(&operation->ready, memory_order_acquire) || operation->tag != tag ||
      atomic_load_explicit(&operation->generation, memory_order_relaxed) != generation)
    return NULL;
  return operation;
}

static int packet_sink_backend_submit(void *user, cflow_io_actor *actor,
                                      cflow_io_request_id request_id, cflow_io_lease_id lease_id,
                                      void *operation_user) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  packet_sink_operation_t *operation = (packet_sink_operation_t *)operation_user;
  const turbo_flow_msg_t *message;
  (void)actor;
  if (!sink || !operation || lease_id != operation->tag) return SALTS_EINVAL;
  atomic_store_explicit(&operation->request_id, request_id, memory_order_release);
  message = turbo_flow_async_terminal_claim_message(&operation->claim);
  if (!message || !message->payload.data || message->payload.len == 0u ||
      message->payload.len != operation->bytes)
    return SALTS_EPROTO;
  return cnet_packet_send_tagged(&sink->endpoint, operation->session, message->payload.data,
                                 message->payload.len, operation->tag);
}

static int packet_sink_backend_cancel(void *user, cflow_io_request_id request_id) {
  (void)user;
  (void)request_id;
  return SALTS_ENOTSUP;
}

static void packet_sink_actor_completion(void *user, cflow_io_request_id request_id,
                                         cflow_io_lease_id lease_id, void *operation_user,
                                         const cflow_io_completion *completion) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  packet_sink_operation_t *operation = (packet_sink_operation_t *)operation_user;
  int status = packet_sink_completion_status(completion);
  bool invariant_failed = false;
  uint64_t expected_request_id;
  if (!sink || !operation) return;
  expected_request_id = atomic_load_explicit(&operation->request_id, memory_order_acquire);
  if (expected_request_id == 0u) {
    (void)atomic_compare_exchange_strong_explicit(&operation->request_id, &expected_request_id,
                                                  request_id, memory_order_acq_rel,
                                                  memory_order_acquire);
    expected_request_id = atomic_load_explicit(&operation->request_id, memory_order_acquire);
  }
  if (request_id == 0u || request_id != expected_request_id || lease_id != operation->tag) {
    status = SALTS_EPROTO;
    invariant_failed = true;
  }
  if (status == SALTS_OK && completion->bytes != operation->bytes) {
    status = SALTS_EPROTO;
    invariant_failed = true;
  }
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
  if (status != SALTS_OK)
    (void)atomic_fetch_add_explicit(&sink->terminals_failed, 1u, memory_order_relaxed);
  if (invariant_failed) packet_sink_fail(sink, status);
  if (sink->delivered_count < sink->send_capacity)
    sink->delivered_ids[sink->delivered_count++] = request_id;
  else {
    status = SALTS_EPROTO;
    packet_sink_fail(sink, status);
  }
  if (turbo_flow_async_terminal_complete(&operation->claim, status, NULL) == SALTS_OK)
    turbo_flow_cnet_sink_counter_increment(&sink->completed);
  else packet_sink_fail(sink, SALTS_EPROTO);
}

static void packet_sink_actor_wake(void *user) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  if (sink && atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire))
    (void)cnet_packet_wake(&sink->endpoint);
}

static int packet_sink_on_admit(void *user, cnet_packet_endpoint *endpoint,
                                cnet_packet_protocol protocol, const cnet_datagram_peer *peer,
                                uint32_t conversation) {
  (void)user;
  (void)endpoint;
  (void)protocol;
  (void)peer;
  (void)conversation;
  return SALTS_EPERM;
}

static void packet_sink_on_state(void *user, cnet_packet_endpoint *endpoint,
                                 cnet_packet_session session, cnet_packet_session_state state,
                                 const cnet_datagram_peer *peer, uint32_t conversation) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  (void)endpoint;
  (void)peer;
  (void)conversation;
  if (!sink || !packet_sink_session_equal(session, sink->session)) return;
  if (state == CNET_PACKET_SESSION_OPEN) {
    atomic_store_explicit(&sink->session_open, true, memory_order_release);
  } else if (state == CNET_PACKET_SESSION_CLOSED) {
    atomic_store_explicit(&sink->session_open, false, memory_order_release);
    if (packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_RUNNING)
      packet_sink_fail(sink, SALTS_EPIPE);
  }
}

static void packet_sink_on_receive(void *user, cnet_packet_endpoint *endpoint,
                                   cnet_packet_session session, const cnet_receive_view *view) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  (void)endpoint;
  (void)session;
  (void)view;
  packet_sink_fail(sink, SALTS_EPROTO);
}

static void packet_sink_on_error(void *user, cnet_packet_endpoint *endpoint,
                                 cnet_packet_session session, int status) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  (void)endpoint;
  (void)session;
  packet_sink_fail(sink, status);
}

static void packet_sink_on_send_terminal(void *user, cnet_packet_endpoint *endpoint,
                                         cnet_packet_session session, size_t size, int status,
                                         uint64_t tag) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)user;
  packet_sink_operation_t *operation;
  cflow_io_completion completion;
  cflow_io_complete_status completed;
  (void)endpoint;
  if (!sink) return;
  operation = packet_sink_operation_from_tag(sink, tag);
  if (!operation) {
    packet_sink_fail(sink, SALTS_EPROTO);
    return;
  }
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
  sink->test_last_terminal_session = session;
  sink->test_last_terminal_size = size;
  sink->test_last_terminal_status = status;
  sink->test_last_terminal_tag = tag;
  sink->test_last_terminal_available = true;
#endif
  if (!packet_sink_session_equal(operation->session, session) ||
      (status == SALTS_OK && size != operation->bytes)) {
    status = SALTS_EPROTO;
    packet_sink_fail(sink, status);
  }
  completion.kind = status == SALTS_OK ? CFLOW_IO_COMPLETION_OK
                                       : (status == SALTS_ECANCELED ? CFLOW_IO_COMPLETION_CANCELLED
                                                                    : CFLOW_IO_COMPLETION_FAILED);
  completion.bytes = status == SALTS_OK ? size : 0u;
  completion.error = status != SALTS_OK && status != SALTS_ECANCELED ? status : SALTS_OK;
  completed = cflow_io_actor_complete(
      &sink->actor, atomic_load_explicit(&operation->request_id, memory_order_acquire),
      &completion);
  if (completed != CFLOW_IO_COMPLETE_ACCEPTED) packet_sink_fail(sink, SALTS_EPROTO);
}

static void packet_sink_acknowledge(turbo_flow_cnet_packet_sink_t *sink) {
  size_t index;
  if (!sink) return;
  for (index = 0u; index < sink->delivered_count; ++index) {
    if (cflow_io_actor_acknowledge(&sink->actor, sink->delivered_ids[index]) !=
        CFLOW_IO_ACK_RELEASED)
      packet_sink_fail(sink, SALTS_EPROTO);
  }
  sink->delivered_count = 0u;
}

static void packet_sink_drive_actor(turbo_flow_cnet_packet_sink_t *sink) {
  if (!sink || !sink->actor_initialized) return;
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
  (void)cflow_executor_run_ready(&sink->executor);
  packet_sink_acknowledge(sink);
  (void)cflow_io_actor_run_ready(&sink->actor, sink->actor_max_steps_per_poll);
}

static int packet_sink_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)ctx;
  cflow_io_actor_config actor_config = {0};
  cnet_packet_terminal_config terminal = CNET_PACKET_TERMINAL_CONFIG_INIT;
  int status;
  int cleanup_status;
  (void)stage;
  if (!sink || flow != sink->flow) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) || sink->owner_active ||
      sink->submissions_active > 0u ||
      (packet_sink_state(sink) != TURBO_FLOW_CNET_PACKET_SINK_REGISTERED &&
       packet_sink_state(sink) != TURBO_FLOW_CNET_PACKET_SINK_STOPPED)) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EINVAL;
  }
  sink->owner_active = true;
  sink->starting = true;
  atomic_store_explicit(&sink->status, SALTS_OK, memory_order_relaxed);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->session_open, false, memory_order_relaxed);
  sink->delivered_count = 0u;
  if (!cflow_executor_manual_init_with_capacity(&sink->executor, sink->send_capacity)) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  sink->executor_initialized = true;
  actor_config.request_capacity = sink->send_capacity;
  actor_config.command_capacity = sink->actor_command_capacity;
  actor_config.executor = &sink->executor;
  actor_config.backend.submit = packet_sink_backend_submit;
  actor_config.backend.cancel = packet_sink_backend_cancel;
  actor_config.backend_user = sink;
  actor_config.completion = packet_sink_actor_completion;
  actor_config.completion_user = sink;
  actor_config.wake = packet_sink_actor_wake;
  actor_config.wake_user = sink;
  status = cflow_io_actor_init(&sink->actor, &actor_config);
  if (status != SALTS_OK) goto fail;
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->actor_initialized = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  sink->config.datagram.host = sink->host;
  sink->config.observer = (cnet_packet_observer){.on_admit = packet_sink_on_admit,
                                                 .on_state = packet_sink_on_state,
                                                 .on_receive = packet_sink_on_receive,
                                                 .on_error = packet_sink_on_error,
                                                 .user = sink};
  terminal.send_capacity = sink->send_capacity;
  terminal.on_send = packet_sink_on_send_terminal;
  terminal.user = sink;
  status = cnet_packet_endpoint_init_ex(&sink->endpoint, &sink->config, &terminal);
  if (status != SALTS_OK) goto fail;
  atomic_store_explicit(&sink->endpoint_initialized, true, memory_order_release);
  status = cnet_packet_endpoint_port(&sink->endpoint, &sink->bound_port);
  if (status != SALTS_OK) goto fail;
  status =
      cnet_packet_session_open(&sink->endpoint, &sink->peer, sink->conversation, &sink->session);
  if (status != SALTS_OK) goto fail;
  salts_mutex_lock(&sink->lifecycle_mutex);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_RUNNING, memory_order_release);
  sink->starting = false;
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;

fail:
  atomic_store_explicit(&sink->status, status, memory_order_relaxed);
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->starting = false;
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_FAILED, memory_order_release);
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  cleanup_status = packet_sink_stop_internal(sink);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_FAILED, memory_order_release);
  return cleanup_status != SALTS_OK ? cleanup_status : status;
}

static int packet_sink_stop_internal(turbo_flow_cnet_packet_sink_t *sink) {
  int status = SALTS_OK;
  int destroy_status;
  bool endpoint_drained;
  if (!sink) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_STOPPED ||
      packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_DETACHED) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_OK;
  }
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_STOPPING, memory_order_release);
  while (sink->owner_active || sink->submissions_active > 0u)
    salts_cond_wait(&sink->lifecycle_cond, &sink->lifecycle_mutex);
  if (packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_STOPPED ||
      packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_DETACHED) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_OK;
  }
  sink->owner_active = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  endpoint_drained = !atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire);
  if (sink->actor_initialized) {
    status = cflow_io_actor_close(&sink->actor);
    if (status == SALTS_EALREADY) status = SALTS_OK;
    packet_sink_drive_actor(sink);
  }
  if (status == SALTS_OK &&
      atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire)) {
    do {
      status = cnet_packet_endpoint_stop(&sink->endpoint, sink->stop_timeout_ms);
      packet_sink_drive_actor(sink);
    } while (status == SALTS_ETIMEDOUT);
    endpoint_drained = true;
  }
#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
  if (endpoint_drained && status == SALTS_OK && sink->test_stop_status != SALTS_OK) {
    status = sink->test_stop_status;
    sink->test_stop_status = SALTS_OK;
  }
#endif
  while (endpoint_drained && sink->actor_initialized && !cflow_io_actor_is_quiescent(&sink->actor))
    packet_sink_drive_actor(sink);
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (status == SALTS_OK && sink->actor_initialized) {
    destroy_status = cflow_io_actor_destroy(&sink->actor);
    if (destroy_status == SALTS_OK) sink->actor_initialized = false;
    else status = destroy_status;
  }
  salts_mutex_unlock(&sink->lifecycle_mutex);
  if (status == SALTS_OK && !sink->actor_initialized && sink->executor_initialized) {
    if (!cflow_executor_shutdown(&sink->executor)) {
      status = SALTS_EBUSY;
    } else {
      cflow_executor_destroy(&sink->executor);
      sink->executor_initialized = false;
    }
  }
  if (status == SALTS_OK && !sink->actor_initialized && !sink->executor_initialized &&
      atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire)) {
    destroy_status = cnet_packet_endpoint_destroy(&sink->endpoint);
    if (destroy_status == SALTS_OK)
      atomic_store_explicit(&sink->endpoint_initialized, false, memory_order_release);
    else status = destroy_status;
  }
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (status == SALTS_OK && !sink->actor_initialized && !sink->executor_initialized &&
      !atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire)) {
    sink->session = (cnet_packet_session){0};
    sink->bound_port = 0u;
    atomic_store_explicit(&sink->session_open, false, memory_order_release);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_STOPPED, memory_order_release);
  } else {
    int expected = SALTS_OK;
    if (status == SALTS_OK) status = SALTS_EBUSY;
    (void)atomic_compare_exchange_strong_explicit(&sink->status, &expected, status,
                                                  memory_order_relaxed, memory_order_relaxed);
    atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_FAILED, memory_order_release);
  }
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return status;
}

#if defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
int turbo_flow_cnet_test_packet_sink_fail_next_stop(turbo_flow_cnet_packet_sink_t *sink,
                                                    int status) {
  if (!sink || status == SALTS_OK || status == SALTS_ETIMEDOUT) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (sink->owner_active || sink->submissions_active > 0u ||
      atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      packet_sink_state(sink) != TURBO_FLOW_CNET_PACKET_SINK_RUNNING) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  if (sink->test_stop_status != SALTS_OK) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EALREADY;
  }
  sink->test_stop_status = status;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return SALTS_OK;
}

int turbo_flow_cnet_test_packet_sink_replay_last_terminal(turbo_flow_cnet_packet_sink_t *sink) {
  cnet_packet_session session;
  size_t size;
  int status;
  uint64_t tag;
  if (!sink) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (sink->owner_active || sink->submissions_active > 0u ||
      atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      packet_sink_state(sink) != TURBO_FLOW_CNET_PACKET_SINK_RUNNING ||
      !sink->test_last_terminal_available) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->owner_active = true;
  session = sink->test_last_terminal_session;
  size = sink->test_last_terminal_size;
  status = sink->test_last_terminal_status;
  tag = sink->test_last_terminal_tag;
  salts_mutex_unlock(&sink->lifecycle_mutex);

  packet_sink_on_send_terminal(sink, &sink->endpoint, session, size, status, tag);

  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return atomic_load_explicit(&sink->status, memory_order_relaxed);
}
#endif

static void packet_sink_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  int status;
  (void)stage;
  status = packet_sink_stop_internal((turbo_flow_cnet_packet_sink_t *)ctx);
  if (status != SALTS_OK) (void)turbo_flow_adapter_report_stop_status(flow, status);
}

static void packet_sink_shutdown(void *ctx) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)ctx;
  if (!sink) return;
  if (packet_sink_stop_internal(sink) != SALTS_OK) return;
  salts_mutex_lock(&sink->lifecycle_mutex);
  sink->flow = NULL;
  atomic_store_explicit(&sink->detached, true, memory_order_release);
  atomic_store_explicit(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_DETACHED, memory_order_release);
  salts_mutex_unlock(&sink->lifecycle_mutex);
}

static int packet_sink_async_submit(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage,
                                    const turbo_flow_msg_t *message,
                                    turbo_flow_async_terminal_claim_t *claim) {
  turbo_flow_cnet_packet_sink_t *sink = (turbo_flow_cnet_packet_sink_t *)ctx;
  packet_sink_operation_t *operation;
  cflow_io_operation io_operation;
  cflow_io_submit_result submitted;
  turbo_flow_cnet_packet_sink_state_t state;
  int status;
  (void)stage;
  if (!sink || !message || !claim) return packet_sink_reject(sink, SALTS_EINVAL);
  if (!message->payload.data || message->payload.len == 0u)
    return packet_sink_reject(sink, SALTS_EINVAL);
  if (message->payload.len > sink->max_message_bytes)
    return packet_sink_reject(sink, SALTS_EMSGSIZE);
  salts_mutex_lock(&sink->lifecycle_mutex);
  state = packet_sink_state(sink);
  if (flow != sink->flow) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return packet_sink_reject(sink, SALTS_EINVAL);
  }
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) || !sink->actor_initialized ||
      state != TURBO_FLOW_CNET_PACKET_SINK_RUNNING) {
    status = state == TURBO_FLOW_CNET_PACKET_SINK_FAILED
                 ? atomic_load_explicit(&sink->status, memory_order_relaxed)
                 : SALTS_ESHUTDOWN;
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return packet_sink_reject(sink, status);
  }
  ++sink->submissions_active;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  status = packet_sink_operation_acquire(sink, &operation);
  if (status != SALTS_OK) {
    packet_sink_submission_finish(sink, false);
    return status;
  }
  operation->bytes = message->payload.len;
  status = turbo_flow_async_terminal_claim_move(&operation->claim, claim);
  if (status != SALTS_OK) {
    packet_sink_operation_reset(operation);
    packet_sink_submission_finish(sink, false);
    return status;
  }
  atomic_store_explicit(&operation->ready, true, memory_order_release);
  io_operation.user = operation;
  io_operation.release = packet_sink_operation_release;
  submitted = cflow_io_actor_try_submit(&sink->actor, operation->tag, &io_operation);
  status = packet_sink_submit_status(submitted.status);
  if (status != SALTS_OK) {
    (void)turbo_flow_async_terminal_claim_move(claim, &operation->claim);
    packet_sink_operation_reset(operation);
    packet_sink_submission_finish(sink, false);
    return status;
  }
  packet_sink_submission_finish(sink, true);
  return SALTS_OK;
}

static bool packet_sink_observer_empty(const cnet_packet_observer *observer) {
  return observer && !observer->on_admit && !observer->on_state && !observer->on_receive &&
         !observer->on_error && !observer->user;
}

static bool packet_sink_datagram_observer_empty(const cnet_datagram_observer *observer) {
  return observer && !observer->on_receive && !observer->on_send && !observer->user;
}

static bool packet_sink_kcp_observer_empty(const cnet_kcp_observer *observer) {
  return observer && !observer->output && !observer->on_receive && !observer->user;
}

static int packet_sink_config_validate(const turbo_flow_cnet_packet_sink_config_t *config) {
  const cnet_packet_endpoint_config *endpoint;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_PACKET_SINK_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->endpoint ||
      config->send_capacity == 0u || config->send_capacity > UINT32_MAX ||
      config->send_capacity > SIZE_MAX / sizeof(packet_sink_operation_t) ||
      config->send_capacity > SIZE_MAX / sizeof(cflow_io_request_id) ||
      config->max_message_bytes == 0u || config->actor_command_capacity == 0u ||
      config->actor_max_steps_per_poll == 0u || config->stop_timeout_ms == 0u ||
      config->peer.port == 0u ||
      (config->peer.family != CNET_DATAGRAM_ADDRESS_IPV4 &&
       config->peer.family != CNET_DATAGRAM_ADDRESS_IPV6))
    return SALTS_EINVAL;
  if (turbo_flow_state(config->flow) == TURBO_FLOW_STATE_COMPILED ||
      turbo_flow_state(config->flow) == TURBO_FLOW_STATE_STARTED)
    return SALTS_EBUSY;
  endpoint = config->endpoint;
  if (endpoint->size != sizeof(*endpoint) || endpoint->session_capacity == 0u ||
      endpoint->session_capacity > UINT32_MAX ||
      (endpoint->protocol != CNET_PACKET_UDP && endpoint->protocol != CNET_PACKET_KCP) ||
      !endpoint->datagram.host || endpoint->datagram.host[0] == '\0' ||
      !packet_sink_observer_empty(&endpoint->observer) ||
      !packet_sink_datagram_observer_empty(&endpoint->datagram.observer) ||
      !packet_sink_kcp_observer_empty(&endpoint->kcp.observer))
    return SALTS_EINVAL;
  if (endpoint->protocol == CNET_PACKET_UDP) {
    if (config->conversation != 0u || config->send_capacity > endpoint->datagram.send_capacity ||
        config->max_message_bytes > endpoint->datagram.max_datagram_bytes)
      return SALTS_EINVAL;
  } else {
    if (config->max_message_bytes > endpoint->kcp.max_message_bytes || endpoint->kcp.stream_mode)
      return SALTS_EINVAL;
    if ((endpoint->security.mode == CNET_KCP_SECURITY_NONE && config->conversation == 0u) ||
        (endpoint->security.mode != CNET_KCP_SECURITY_NONE && config->conversation != 0u))
      return SALTS_EINVAL;
  }
  return SALTS_OK;
}

int turbo_flow_cnet_packet_sink_register(const turbo_flow_cnet_packet_sink_config_t *config,
                                         turbo_flow_cnet_packet_sink_t **sink_out) {
  turbo_flow_cnet_packet_sink_t *sink;
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_managed_boundary_provider_ops_t boundary_ops =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_async_terminal_registration_t registration =
      TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  size_t index;
  int status;
  if (!sink_out) return SALTS_EINVAL;
  *sink_out = NULL;
  status = packet_sink_config_validate(config);
  if (status != SALTS_OK) return status;
  sink = (turbo_flow_cnet_packet_sink_t *)calloc(1u, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  status = packet_sink_lifecycle_init(sink);
  if (status != SALTS_OK) {
    free(sink);
    return status;
  }
  sink->flow = config->flow;
  sink->adapter_name = tstr_dup(config->adapter_name);
  sink->host = tstr_dup(config->endpoint->datagram.host);
  sink->config = *config->endpoint;
  sink->peer = config->peer;
  sink->conversation = config->conversation;
  sink->send_capacity = config->send_capacity;
  sink->max_message_bytes = config->max_message_bytes;
  sink->actor_command_capacity = config->actor_command_capacity;
  sink->actor_max_steps_per_poll = config->actor_max_steps_per_poll;
  sink->stop_timeout_ms = config->stop_timeout_ms;
  sink->operations =
      (packet_sink_operation_t *)calloc(sink->send_capacity, sizeof(*sink->operations));
  sink->delivered_ids =
      (cflow_io_request_id *)calloc(sink->send_capacity, sizeof(*sink->delivered_ids));
  if (!sink->adapter_name || !sink->host || !sink->operations || !sink->delivered_ids) {
    packet_sink_storage_destroy(sink);
    packet_sink_lifecycle_destroy(sink);
    free(sink);
    return SALTS_ENOMEM;
  }
  status = packet_sink_managed_identity_init(sink);
  if (status != SALTS_OK) {
    packet_sink_storage_destroy(sink);
    packet_sink_lifecycle_destroy(sink);
    free(sink);
    return status;
  }
  for (index = 0u; index < sink->send_capacity; ++index) {
    sink->operations[index].sink = sink;
    sink->operations[index].slot = (uint32_t)index;
    sink->operations[index].claim =
        (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    atomic_init(&sink->operations[index].request_id, 0u);
    atomic_init(&sink->operations[index].generation, 0u);
    atomic_init(&sink->operations[index].ready, false);
    atomic_init(&sink->operations[index].occupied, false);
  }
  atomic_init(&sink->messages_sent, 0u);
  atomic_init(&sink->bytes_sent, 0u);
  atomic_init(&sink->terminals_failed, 0u);
  atomic_init(&sink->accepted, 0u);
  atomic_init(&sink->completed, 0u);
  atomic_init(&sink->rejected, 0u);
  atomic_init(&sink->state, TURBO_FLOW_CNET_PACKET_SINK_REGISTERED);
  atomic_init(&sink->status, SALTS_OK);
  atomic_init(&sink->session_open, false);
  atomic_init(&sink->endpoint_initialized, false);
  atomic_init(&sink->detached, false);
  adapter_ops.start = packet_sink_start;
  adapter_ops.stop = packet_sink_stop;
  adapter_ops.shutdown = packet_sink_shutdown;
  async_ops.submit = packet_sink_async_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  boundary_ops.resource.metadata = packet_sink_resource_metadata;
  boundary_ops.descriptor = packet_sink_managed_descriptor;
  boundary_ops.snapshot = packet_sink_managed_snapshot;
  registration.adapter_name = sink->adapter_name;
  registration.adapter_ops = &adapter_ops;
  registration.async_ops = &async_ops;
  registration.schema = &schema;
  registration.owner_name = sink->identity.owner;
  registration.boundary_ops = &boundary_ops;
  registration.ctx = sink;
  status = turbo_flow_register_managed_async_terminal_adapter(config->flow, &registration);
  if (status != SALTS_OK) {
    packet_sink_storage_destroy(sink);
    packet_sink_lifecycle_destroy(sink);
    free(sink);
    return status;
  }
  *sink_out = sink;
  return SALTS_OK;
}

static int packet_sink_snapshot_locked(const turbo_flow_cnet_packet_sink_t *sink,
                                       turbo_flow_cnet_packet_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_packet_sink_snapshot_t current = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  cflow_io_actor_stats stats = {0};
  current.state = packet_sink_state(sink);
  current.status = atomic_load_explicit(&sink->status, memory_order_relaxed);
  current.protocol = sink->config.protocol;
  current.bound_port = sink->bound_port;
  current.session = sink->session;
  current.session_open = atomic_load_explicit(&sink->session_open, memory_order_acquire) ? 1 : 0;
  current.messages_sent = atomic_load_explicit(&sink->messages_sent, memory_order_relaxed);
  current.bytes_sent = atomic_load_explicit(&sink->bytes_sent, memory_order_relaxed);
  current.terminals_failed = atomic_load_explicit(&sink->terminals_failed, memory_order_relaxed);
  if (sink->actor_initialized && !cflow_io_actor_get_stats(&sink->actor, &stats))
    return SALTS_EPROTO;
  current.active_requests = stats.active_requests;
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_packet_sink_snapshot(const turbo_flow_cnet_packet_sink_t *sink,
                                         turbo_flow_cnet_packet_sink_snapshot_t *snapshot) {
  turbo_flow_cnet_packet_sink_t *mutable_sink = (turbo_flow_cnet_packet_sink_t *)sink;
  int status;
  if (!sink || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_PACKET_SINK_API_VERSION)
    return SALTS_EINVAL;
  salts_mutex_lock(&mutable_sink->lifecycle_mutex);
  while (mutable_sink->submissions_active > 0u)
    salts_cond_wait(&mutable_sink->lifecycle_cond, &mutable_sink->lifecycle_mutex);
  if (mutable_sink->owner_active) {
    salts_mutex_unlock(&mutable_sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  status = packet_sink_snapshot_locked(sink, snapshot);
  salts_mutex_unlock(&mutable_sink->lifecycle_mutex);
  return status;
}

int turbo_flow_cnet_packet_sink_poll(turbo_flow_cnet_packet_sink_t *sink, uint32_t timeout_ms,
                                     turbo_flow_cnet_packet_sink_snapshot_t *snapshot) {
  size_t events = 0u;
  int status;
  if (!sink || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                             snapshot->version != TURBO_FLOW_CNET_PACKET_SINK_API_VERSION)))
    return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      !atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire) ||
      !sink->actor_initialized || packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_STOPPING ||
      packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_STOPPED) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_ESHUTDOWN;
  }
  if (sink->owner_active) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  sink->owner_active = true;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  packet_sink_drive_actor(sink);
  status = cnet_packet_poll(&sink->endpoint, timeout_ms, &events);
  if (status != SALTS_OK) packet_sink_fail(sink, status);
  packet_sink_drive_actor(sink);
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (snapshot) {
    int snapshot_status = packet_sink_snapshot_locked(sink, snapshot);
    if (status == SALTS_OK) status = snapshot_status;
  }
  sink->owner_active = false;
  salts_cond_broadcast(&sink->lifecycle_cond);
  status = packet_sink_state(sink) == TURBO_FLOW_CNET_PACKET_SINK_FAILED
               ? atomic_load_explicit(&sink->status, memory_order_relaxed)
               : status;
  salts_mutex_unlock(&sink->lifecycle_mutex);
  return status;
}

int turbo_flow_cnet_packet_sink_destroy(turbo_flow_cnet_packet_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  salts_mutex_lock(&sink->lifecycle_mutex);
  if (!atomic_load_explicit(&sink->detached, memory_order_acquire) ||
      atomic_load_explicit(&sink->endpoint_initialized, memory_order_acquire) ||
      sink->actor_initialized || sink->executor_initialized || sink->owner_active ||
      sink->submissions_active > 0u) {
    salts_mutex_unlock(&sink->lifecycle_mutex);
    return SALTS_EBUSY;
  }
  salts_mutex_unlock(&sink->lifecycle_mutex);
  packet_sink_storage_destroy(sink);
  packet_sink_lifecycle_destroy(sink);
  free(sink);
  return SALTS_OK;
}
