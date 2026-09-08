#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include "stream_source_pipe_fixture.h"

#include <salts/clock.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { STREAM_SINK_TEST_TIMEOUT_MS = 5000, STREAM_SINK_NATIVE_PENDING_BYTES = 64u * 1024u };

static const char STREAM_SINK_GRAPH[] = "source input\n"
                                        "stage output adapter cnet.pipe.out\n"
                                        "stage main {\n"
                                        "  input -> output\n"
                                        "}\n";
static const char STREAM_SINK_RESOURCE_UID[] = "cnet-stream-sink:cnet.pipe.out";

typedef struct stream_sink_completion_s {
  atomic_size_t calls;
  atomic_int status;
} stream_sink_completion_t;

typedef struct stream_sink_snapshot_race_s {
  turbo_flow_t *flow;
  atomic_bool ready;
  atomic_bool done;
  atomic_size_t snapshots;
  atomic_uint_fast64_t states;
  atomic_int status;
} stream_sink_snapshot_race_t;

typedef struct stream_sink_boundary_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
} stream_sink_boundary_fixture_t;

static cnet_client_config stream_sink_client_config(void) {
  const cnet_client_config config = {.backend =
#if defined(_WIN32)
                                         NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
                                         NATIVE_IO_BACKEND_EPOLL,
#else
                                         NATIVE_IO_BACKEND_KQUEUE,
#endif
                                     .connection_capacity = 1u,
                                     .command_capacity = 8u,
                                     .request_capacity = 4u,
                                     .completion_batch_capacity = 4u,
                                     .event_capacity = 8u,
                                     .max_send_bytes = 256u,
                                     .receive_buffer_bytes = 256u};
  return config;
}

static void stream_sink_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  stream_sink_completion_t *completion = (stream_sink_completion_t *)ctx;
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

static void stream_sink_snapshot_race(void *ctx) {
  stream_sink_snapshot_race_t *race = (stream_sink_snapshot_race_t *)ctx;
  atomic_store_explicit(&race->ready, true, memory_order_release);
  while (!atomic_load_explicit(&race->done, memory_order_acquire)) {
    turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    int rc = turbo_flow_managed_boundary_snapshot_at(race->flow, 0u, &snapshot);
    if (rc == SALTS_OK) {
      if (snapshot.completed > snapshot.accepted ||
          snapshot.queue_depth > snapshot.queue_capacity ||
          snapshot.in_flight > snapshot.queue_capacity ||
          snapshot.queue_depth + snapshot.in_flight > snapshot.queue_capacity) {
        atomic_store_explicit(&race->status, SALTS_EPROTO, memory_order_release);
        break;
      }
      (void)atomic_fetch_or_explicit(&race->states, UINT64_C(1) << snapshot.state,
                                     memory_order_relaxed);
      (void)atomic_fetch_add_explicit(&race->snapshots, 1u, memory_order_relaxed);
    } else if (rc != SALTS_EBUSY) {
      atomic_store_explicit(&race->status, rc, memory_order_release);
      break;
    }
    salts_thread_yield();
  }
}

static int stream_sink_boundary_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  stream_sink_boundary_fixture_t *fixture = (stream_sink_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int stream_sink_boundary_descriptor(void *ctx,
                                           turbo_flow_managed_boundary_descriptor_t *out) {
  stream_sink_boundary_fixture_t *fixture = (stream_sink_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->descriptor;
  return SALTS_OK;
}

static int stream_sink_boundary_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  stream_sink_boundary_fixture_t *fixture = (stream_sink_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->snapshot;
  return SALTS_OK;
}

static void stream_sink_boundary_fixture_init(stream_sink_boundary_fixture_t *fixture) {
  memset(fixture, 0, sizeof(*fixture));
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(fixture->metadata.uid, STREAM_SINK_RESOURCE_UID, sizeof(STREAM_SINK_RESOURCE_UID));
  memcpy(fixture->metadata.owner_name, "existing-owner", sizeof("existing-owner"));
  fixture->metadata.generation = 1u;
  fixture->metadata.observed_generation = 1u;
  fixture->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  fixture->descriptor.domain = fixture->metadata.domain;
  fixture->descriptor.kind = fixture->metadata.kind;
  memcpy(fixture->descriptor.uid, fixture->metadata.uid, sizeof(STREAM_SINK_RESOURCE_UID));
  memcpy(fixture->descriptor.owner_name, fixture->metadata.owner_name, sizeof("existing-owner"));
  fixture->descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  fixture->descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT;
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.input, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                  TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE,
                  "application/octet-stream", "existing-owner"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&fixture->descriptor.input, "CNetStream",
                                                           "NonEmptyBytes", 1u),
              SALTS_OK);
  fixture->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  memcpy(fixture->snapshot.uid, fixture->metadata.uid, sizeof(STREAM_SINK_RESOURCE_UID));
  fixture->snapshot.generation = 1u;
  fixture->snapshot.observed_generation = 1u;
  fixture->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  fixture->snapshot.queue_capacity = 1u;
}

spec("TurboFlow CNet stream sink") {
  it("settles a Pipe terminal only after CNet reports the full write") {
    cnet_client_config client = stream_sink_client_config();
    stream_source_pipe_fixture_t pipe;
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    stream_sink_completion_t first;
    stream_sink_completion_t second;
    turbo_flow_msg_t first_message;
    turbo_flow_msg_t second_message;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[640];
    char received[32] = {0};
    uint64_t deadline;

    atomic_init(&first.calls, 0u);
    atomic_init(&first.status, SALTS_EALREADY);
    atomic_init(&second.calls, 0u);
    atomic_init(&second.status, SALTS_EALREADY);
    check_not_null(flow);
    check_equal(stream_source_pipe_fixture_start(&pipe), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "pipe://%s", pipe.name), 0);
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = uri;
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_equal(descriptor.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_equal(descriptor.uid, STREAM_SINK_RESOURCE_UID);
    check_equal(descriptor.owner_name, "cnet.pipe.out");
    check_equal(descriptor.role_flags, (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_SINK);
    check_equal(descriptor.capability_flags,
                (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT);
    check_equal(descriptor.command_flags, (uint32_t)0u);
    check_equal(descriptor.input.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_equal(descriptor.input.profile, TURBO_FLOW_CONTENT_PROFILE_GENERIC);
    check_equal(descriptor.input.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(descriptor.input.media_type, "application/octet-stream");
    check_equal(descriptor.input.schema_name, "CNetStream");
    check_equal(descriptor.input.type_name, "NonEmptyBytes");
    check_equal(descriptor.input.schema_version, (uint32_t)1u);
    check_equal(descriptor.input.identity, "cnet.pipe.out");
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.uid, STREAM_SINK_RESOURCE_UID);
    check_equal(managed.generation, (uint64_t)1u);
    check_equal(managed.observed_generation, (uint64_t)1u);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)0u);
    check_equal(managed.accepted, (uint64_t)0u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_equal(managed.backpressured, 0);
    check_equal(turbo_flow_parse_string(flow, STREAM_SINK_GRAPH, sizeof(STREAM_SINK_GRAPH) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_true(managed.state == TURBO_FLOW_MANAGED_BOUNDARY_STARTING ||
               managed.state == TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
    check_equal(stream_source_pipe_fixture_finish(&pipe), SALTS_OK);

    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED &&
           salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTED);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);

    turbo_flow_msg_init(&first_message);
    first_message.owned_payload = tstr_dup("pipe-terminal");
    first_message.payload = tstr_to_v(first_message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &first_message, stream_sink_complete, &first),
        SALTS_OK);
    turbo_flow_msg_cleanup(&first_message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_stream_sink_snapshot(sink, &snapshot), SALTS_OK);
      if (snapshot.active_requests == 0u) salts_sleep_ms(1u);
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_equal(managed.queue_depth + managed.in_flight, (uint64_t)1u);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.backpressured, 1);

    turbo_flow_msg_init(&second_message);
    second_message.owned_payload = tstr_dup("capacity-full");
    second_message.payload = tstr_to_v(second_message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &second_message, stream_sink_complete, &second),
        SALTS_OK);
    turbo_flow_msg_cleanup(&second_message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_ENOSPC);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)1u);

    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&first.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    }
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_OK);
    check_equal(snapshot.messages_sent, (uint64_t)1u);
    check_equal(snapshot.bytes_sent, (uint64_t)(sizeof("pipe-terminal") - 1u));
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      if (managed.completed == 0u) salts_sleep_ms(1u);
    } while (managed.completed == 0u && salts_monotonic_ms() < deadline);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)1u);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)0u);
    check_equal(managed.backpressured, 0);
    check_equal(stream_source_pipe_fixture_read(&pipe, received, sizeof("pipe-terminal") - 1u),
                SALTS_OK);
    check_equal(received, "pipe-terminal");

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_STOPPED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
    stream_source_pipe_fixture_close(&pipe);
  }

  it("reports client initialization failure through the managed lifecycle") {
    cnet_client_config client = stream_sink_client_config();
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    client.connection_capacity = 0u;
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = "pipe://managed-start-failure";
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_parse_string(flow, STREAM_SINK_GRAPH, sizeof(STREAM_SINK_GRAPH) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_EINVAL);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_FAILED);
    check_equal(managed.last_status, SALTS_EINVAL);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.accepted, (uint64_t)0u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)0u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
  }

  it("settles a pending claim exactly once while managed snapshots race stop") {
    cnet_client_config client = stream_sink_client_config();
    stream_source_pipe_fixture_t pipe;
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    stream_sink_completion_t completion;
    stream_sink_snapshot_race_t race;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    salts_thread_t observer = NULL;
    char uri[640];
    uint64_t deadline;

    memset(&race, 0, sizeof(race));
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    atomic_init(&race.ready, false);
    atomic_init(&race.done, false);
    atomic_init(&race.snapshots, 0u);
    atomic_init(&race.states, 0u);
    atomic_init(&race.status, SALTS_OK);
    race.flow = flow;
    check_not_null(flow);
    check_equal(stream_source_pipe_fixture_start(&pipe), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "pipe://%s", pipe.name), 0);
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = uri;
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, STREAM_SINK_GRAPH, sizeof(STREAM_SINK_GRAPH) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
    check_equal(stream_source_pipe_fixture_finish(&pipe), SALTS_OK);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTED);

    turbo_flow_msg_init(&message);
    message.owned_payload = tstr_dup("pending-stop");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &message, stream_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_stream_sink_snapshot(sink, &snapshot), SALTS_OK);
      if (snapshot.active_requests == 0u) salts_thread_yield();
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)1u);

    check_equal(salts_thread_create(&observer, stream_sink_snapshot_race, &race), SALTS_OK);
    while (!atomic_load_explicit(&race.ready, memory_order_acquire))
      salts_thread_yield();
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    atomic_store_explicit(&race.done, true, memory_order_release);
    check_equal(salts_thread_join(&observer), SALTS_OK);
    salts_thread_destroy(&observer);
    check_equal(atomic_load_explicit(&race.status, memory_order_acquire), SALTS_OK);
    check_true(atomic_load_explicit(&race.snapshots, memory_order_relaxed) > 0u);
    check_true((atomic_load_explicit(&race.states, memory_order_relaxed) &
                ((UINT64_C(1) << TURBO_FLOW_MANAGED_BOUNDARY_DRAINING) |
                 (UINT64_C(1) << TURBO_FLOW_MANAGED_BOUNDARY_STOPPING))) != 0u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_STOPPED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
    stream_source_pipe_fixture_close(&pipe);
  }

  it("settles a native pending send exactly once when the peer closes") {
    cnet_client_config client = stream_sink_client_config();
    stream_source_pipe_fixture_t pipe;
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    stream_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[640];
    uint64_t deadline;
    int poll_status = SALTS_OK;

    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    check_not_null(flow);
    check_equal(stream_source_pipe_fixture_start(&pipe), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "pipe://%s", pipe.name), 0);
    client.max_send_bytes = STREAM_SINK_NATIVE_PENDING_BYTES;
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = uri;
    config.client = &client;
    config.max_message_bytes = STREAM_SINK_NATIVE_PENDING_BYTES;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, STREAM_SINK_GRAPH, sizeof(STREAM_SINK_GRAPH) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
    check_equal(stream_source_pipe_fixture_finish(&pipe), SALTS_OK);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTED);

    turbo_flow_msg_init(&message);
    message.owned_payload = tstr_new_len(NULL, STREAM_SINK_NATIVE_PENDING_BYTES);
    check_not_null(message.owned_payload);
    memset(message.owned_payload, 'p', STREAM_SINK_NATIVE_PENDING_BYTES);
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &message, stream_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    do {
      int managed_status;
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
      managed_status = turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed);
      check_true(managed_status == SALTS_OK || managed_status == SALTS_EBUSY);
      if (managed_status == SALTS_EBUSY || managed.in_flight == 0u) salts_thread_yield();
    } while (managed.in_flight == 0u &&
             atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
             salts_monotonic_ms() < deadline);
    check_equal(managed.in_flight, (uint64_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);

    stream_source_pipe_fixture_close(&pipe);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      poll_status = turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot);
    check_not_equal(poll_status, SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_not_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_FAILED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_not_equal(managed.last_status, SALTS_OK);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
  }

  it("cancels a native pending send exactly once during stop") {
    cnet_client_config client = stream_sink_client_config();
    stream_source_pipe_fixture_t pipe;
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    stream_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[640];
    uint64_t deadline;

    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    check_not_null(flow);
    check_equal(stream_source_pipe_fixture_start(&pipe), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "pipe://%s", pipe.name), 0);
    client.max_send_bytes = STREAM_SINK_NATIVE_PENDING_BYTES;
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = uri;
    config.client = &client;
    config.max_message_bytes = STREAM_SINK_NATIVE_PENDING_BYTES;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, STREAM_SINK_GRAPH, sizeof(STREAM_SINK_GRAPH) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
    check_equal(stream_source_pipe_fixture_finish(&pipe), SALTS_OK);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTED);

    turbo_flow_msg_init(&message);
    message.owned_payload = tstr_new_len(NULL, STREAM_SINK_NATIVE_PENDING_BYTES);
    check_not_null(message.owned_payload);
    memset(message.owned_payload, 's', STREAM_SINK_NATIVE_PENDING_BYTES);
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &message, stream_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    do {
      int managed_status;
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
      managed_status = turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed);
      check_true(managed_status == SALTS_OK || managed_status == SALTS_EBUSY);
      if (managed_status == SALTS_EBUSY || managed.in_flight == 0u) salts_thread_yield();
    } while (managed.in_flight == 0u &&
             atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
             salts_monotonic_ms() < deadline);
    check_equal(managed.in_flight, (uint64_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_STOPPED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
    stream_source_pipe_fixture_close(&pipe);
  }

  it("preserves the full managed owner-name length") {
    cnet_client_config client = stream_sink_client_config();
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    char adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];

    memset(adapter_name, 'a', sizeof(adapter_name) - 1u);
    adapter_name[sizeof(adapter_name) - 1u] = '\0';
    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = adapter_name;
    config.uri = "pipe://managed-owner-limit";
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.owner_name, adapter_name);
    check_equal(descriptor.input.identity, adapter_name);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
  }

  it("derives the same bounded managed identity for an oversized adapter name") {
    cnet_client_config client = stream_sink_client_config();
    turbo_flow_cnet_stream_sink_t *first_sink = NULL;
    turbo_flow_cnet_stream_sink_t *second_sink = NULL;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_descriptor_t first = TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t second = TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *first_flow = turbo_flow_create();
    turbo_flow_t *second_flow = turbo_flow_create();
    char adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 129u];
    char expected_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];

    memset(adapter_name, 'b', sizeof(adapter_name) - 1u);
    adapter_name[sizeof(adapter_name) - 1u] = '\0';
    check_not_null(first_flow);
    check_not_null(second_flow);
    config.flow = first_flow;
    config.adapter_name = adapter_name;
    config.uri = "pipe://managed-owner-hash";
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &first_sink), SALTS_OK);
    config.flow = second_flow;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &second_sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(first_flow, 0u, &first), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(second_flow, 0u, &second), SALTS_OK);
    check_equal(first.owner_name, second.owner_name);
    check_equal(first.uid, second.uid);
    check_equal(first.input.identity, first.owner_name);
    check_equal(strncmp(first.owner_name, "xxh3-128:", sizeof("xxh3-128:") - 1u), 0);
    check_true(strlen(first.owner_name) <= TURBO_FLOW_RESOURCE_OWNER_MAX);
    check_greater(
        snprintf(expected_uid, sizeof(expected_uid), "cnet-stream-sink:%s", first.owner_name), 0);
    check_equal(first.uid, expected_uid);
    turbo_flow_destroy(first_flow);
    turbo_flow_destroy(second_flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(first_sink), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_sink_destroy(second_sink), SALTS_OK);
  }

  it("rolls back the staged adapter when the managed UID already exists") {
    cnet_client_config client = stream_sink_client_config();
    stream_sink_boundary_fixture_t fixture;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops =
        TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    stream_sink_boundary_fixture_init(&fixture);
    boundary_ops.resource.metadata = stream_sink_boundary_metadata;
    boundary_ops.descriptor = stream_sink_boundary_descriptor;
    boundary_ops.snapshot = stream_sink_boundary_snapshot;
    check_equal(turbo_flow_register_managed_boundary_provider(flow, "existing-owner", &boundary_ops,
                                                              &fixture),
                SALTS_OK);
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = "pipe://managed-duplicate-uid";
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_EALREADY);
    check_null(sink);
    check_null(turbo_flow_find_adapter_schema(flow, "cnet.pipe.out"));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);

    turbo_flow_destroy(flow);
  }
}
