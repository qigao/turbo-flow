#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include <salts/clock.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { DATAGRAM_SINK_TEST_TIMEOUT_MS = 5000 };

static const char DATAGRAM_SINK_RESOURCE_UID[] = "cnet-datagram-sink:cnet.udp.out";

typedef struct datagram_sink_receiver_s {
  size_t calls;
  size_t size;
  unsigned char data[64];
} datagram_sink_receiver_t;

typedef struct datagram_sink_completion_s {
  atomic_size_t calls;
  atomic_int status;
  turbo_flow_cnet_datagram_sink_t *sink;
  atomic_int snapshot_status;
} datagram_sink_completion_t;

typedef struct datagram_sink_boundary_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
} datagram_sink_boundary_fixture_t;

typedef struct datagram_sink_snapshot_race_s {
  turbo_flow_t *flow;
  atomic_bool ready;
  atomic_bool done;
  atomic_size_t snapshots;
  atomic_int status;
} datagram_sink_snapshot_race_t;

static native_io_backend_kind datagram_sink_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static void datagram_sink_receive(void *user, cnet_datagram *datagram,
                                  const cnet_datagram_peer *peer, const cnet_receive_view *view) {
  datagram_sink_receiver_t *receiver = (datagram_sink_receiver_t *)user;
  (void)datagram;
  (void)peer;
  if (!receiver || !view || view->size > sizeof(receiver->data)) return;
  memcpy(receiver->data, view->data, view->size);
  receiver->size = view->size;
  ++receiver->calls;
}

static void datagram_sink_ignore_send(void *user, cnet_datagram *datagram,
                                      const cnet_datagram_peer *peer, size_t size, int status,
                                      uint64_t tag) {
  (void)user;
  (void)datagram;
  (void)peer;
  (void)size;
  (void)status;
  (void)tag;
}

static cnet_datagram_config datagram_sink_cnet_config(size_t send_capacity) {
  cnet_datagram_config config = CNET_DATAGRAM_CONFIG_INIT;
  config.backend = datagram_sink_backend();
  config.host = "127.0.0.1";
  config.port = 0u;
  config.send_capacity = send_capacity;
  config.request_capacity = send_capacity + 1u;
  config.completion_batch_capacity = send_capacity + 1u;
  config.max_datagram_bytes = 256u;
  config.receive_buffer_bytes = 256u;
  return config;
}

static cnet_datagram_peer datagram_sink_peer(uint16_t port) {
  cnet_datagram_peer peer = {0};
  peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  peer.port = port;
  peer.address[0] = 127u;
  peer.address[3] = 1u;
  return peer;
}

static void datagram_sink_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  datagram_sink_completion_t *completion = (datagram_sink_completion_t *)ctx;
  if (completion->sink) {
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    atomic_store_explicit(&completion->snapshot_status,
                          turbo_flow_cnet_datagram_sink_snapshot(completion->sink, &snapshot),
                          memory_order_relaxed);
  }
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

static void datagram_sink_snapshot_race(void *ctx) {
  datagram_sink_snapshot_race_t *race = (datagram_sink_snapshot_race_t *)ctx;
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
      (void)atomic_fetch_add_explicit(&race->snapshots, 1u, memory_order_relaxed);
    } else if (rc != SALTS_EBUSY) {
      atomic_store_explicit(&race->status, rc, memory_order_release);
      break;
    }
    salts_thread_yield();
  }
}

static int datagram_sink_boundary_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  datagram_sink_boundary_fixture_t *fixture = (datagram_sink_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int datagram_sink_boundary_descriptor(void *ctx,
                                             turbo_flow_managed_boundary_descriptor_t *out) {
  datagram_sink_boundary_fixture_t *fixture = (datagram_sink_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->descriptor;
  return SALTS_OK;
}

static int datagram_sink_boundary_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  datagram_sink_boundary_fixture_t *fixture = (datagram_sink_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->snapshot;
  return SALTS_OK;
}

static void datagram_sink_boundary_fixture_init(datagram_sink_boundary_fixture_t *fixture) {
  memset(fixture, 0, sizeof(*fixture));
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(fixture->metadata.uid, DATAGRAM_SINK_RESOURCE_UID, sizeof(DATAGRAM_SINK_RESOURCE_UID));
  memcpy(fixture->metadata.owner_name, "existing-owner", sizeof("existing-owner"));
  fixture->metadata.generation = 1u;
  fixture->metadata.observed_generation = 1u;
  fixture->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  fixture->descriptor.domain = fixture->metadata.domain;
  fixture->descriptor.kind = fixture->metadata.kind;
  memcpy(fixture->descriptor.uid, fixture->metadata.uid, sizeof(DATAGRAM_SINK_RESOURCE_UID));
  memcpy(fixture->descriptor.owner_name, fixture->metadata.owner_name, sizeof("existing-owner"));
  fixture->descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  fixture->descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT;
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.input, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                  TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE,
                  "application/octet-stream", "existing-owner"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&fixture->descriptor.input,
                                                           "CNetDatagram", "NonEmptyBytes", 1u),
              SALTS_OK);
  fixture->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  memcpy(fixture->snapshot.uid, fixture->metadata.uid, sizeof(DATAGRAM_SINK_RESOURCE_UID));
  fixture->snapshot.generation = 1u;
  fixture->snapshot.observed_generation = 1u;
  fixture->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  fixture->snapshot.queue_capacity = 2u;
}

static turbo_flow_t *datagram_sink_flow(const cnet_datagram_config *datagram,
                                        cnet_datagram_peer peer,
                                        turbo_flow_cnet_datagram_sink_t **sink_out) {
  static const char graph[] = "source input\n"
                              "stage output adapter cnet.udp.out\n"
                              "stage main {\n"
                              "  input -> output\n"
                              "}\n";
  turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  turbo_flow_cnet_datagram_sink_config_t config = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  ingress.workers = 1u;
  config.flow = flow;
  config.adapter_name = "cnet.udp.out";
  config.datagram = datagram;
  config.peer = peer;
  config.max_message_bytes = 256u;
  if (!flow || turbo_flow_configure_async_ingress(flow, &ingress) != SALTS_OK ||
      turbo_flow_cnet_datagram_sink_register(&config, sink_out) != SALTS_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static void datagram_sink_message(turbo_flow_msg_t *message, const char *payload) {
  turbo_flow_msg_init(message);
  message->owned_payload = tstr_dup(payload);
  message->payload = tstr_to_v(message->owned_payload);
}

spec("TurboFlow CNet datagram sink") {
  it("registers one atomic managed UDP Sink contract") {
    cnet_datagram_config datagram = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_config_t config = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "cnet.udp.out";
    config.datagram = &datagram;
    config.peer = datagram_sink_peer(9u);
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = DATAGRAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_datagram_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_equal(descriptor.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_equal(descriptor.uid, DATAGRAM_SINK_RESOURCE_UID);
    check_equal(descriptor.owner_name, "cnet.udp.out");
    check_equal(descriptor.role_flags, (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_SINK);
    check_equal(descriptor.capability_flags,
                (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT);
    check_equal(descriptor.command_flags, (uint32_t)0u);
    check_equal(descriptor.input.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_equal(descriptor.input.profile, TURBO_FLOW_CONTENT_PROFILE_GENERIC);
    check_equal(descriptor.input.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(descriptor.input.media_type, "application/octet-stream");
    check_equal(descriptor.input.schema_name, "CNetDatagram");
    check_equal(descriptor.input.type_name, "NonEmptyBytes");
    check_equal(descriptor.input.schema_version, (uint32_t)1u);
    check_equal(descriptor.input.identity, "cnet.udp.out");
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.uid, DATAGRAM_SINK_RESOURCE_UID);
    check_equal(managed.generation, (uint64_t)1u);
    check_equal(managed.observed_generation, (uint64_t)1u);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED);
    check_equal(managed.queue_capacity, (uint64_t)2u);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)0u);
    check_equal(managed.accepted, (uint64_t)0u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_equal(managed.backpressured, 0);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("settles one raw UDP terminal only after the tagged send completes") {
    static const char payload[] = "udp-terminal";
    datagram_sink_receiver_t receiver = {0};
    cnet_datagram receiving_datagram = {0};
    cnet_datagram_config receiving_config = datagram_sink_cnet_config(1u);
    cnet_datagram_config sink_datagram = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    datagram_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow;
    cnet_datagram_peer peer;
    uint16_t port = 0u;
    uint64_t deadline;
    size_t events = 0u;

    receiving_config.observer.on_receive = datagram_sink_receive;
    receiving_config.observer.on_send = datagram_sink_ignore_send;
    receiving_config.observer.user = &receiver;
    check_equal(cnet_datagram_init(&receiving_datagram, &receiving_config), SALTS_OK);
    check_equal(cnet_datagram_port(&receiving_datagram, &port), SALTS_OK);
    check_equal(cnet_datagram_receive(&receiving_datagram, 1u), SALTS_OK);
    peer = datagram_sink_peer(port);
    flow = datagram_sink_flow(&sink_datagram, peer, &sink);
    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    completion.sink = sink;
    atomic_init(&completion.snapshot_status, SALTS_EALREADY);
    datagram_sink_message(&message, payload);

    check_equal(
        turbo_flow_publish_async(flow, "input", &message, datagram_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    deadline = salts_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_datagram_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.snapshot_status, memory_order_acquire),
                SALTS_EBUSY);
    check_equal(snapshot.messages_sent, (uint64_t)1u);
    check_equal(snapshot.bytes_sent, (uint64_t)(sizeof(payload) - 1u));
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
    check_equal(managed.queue_capacity, (uint64_t)2u);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)0u);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_equal(managed.backpressured, 0);
    check_equal(cnet_datagram_poll(&receiving_datagram, DATAGRAM_SINK_TEST_TIMEOUT_MS, &events),
                SALTS_OK);
    check_equal(receiver.calls, (size_t)1u);
    check_equal(receiver.size, sizeof(payload) - 1u);
    check_equal(receiver.data, payload, sizeof(payload) - 1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_STOPPED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
    check_equal(cnet_datagram_stop(&receiving_datagram, DATAGRAM_SINK_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_datagram_destroy(&receiving_datagram), SALTS_OK);
  }

  it("rejects beyond bounded Actor capacity and cancels the admitted claim on stop") {
    cnet_datagram_config sink_datagram = datagram_sink_cnet_config(1u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    datagram_sink_completion_t first;
    datagram_sink_completion_t second;
    turbo_flow_msg_t first_message;
    turbo_flow_msg_t second_message;
    turbo_flow_t *flow = datagram_sink_flow(&sink_datagram, datagram_sink_peer(9u), &sink);
    uint64_t deadline;

    check_not_null(flow);
    atomic_init(&first.calls, 0u);
    atomic_init(&first.status, SALTS_EALREADY);
    first.sink = NULL;
    atomic_init(&first.snapshot_status, SALTS_EALREADY);
    atomic_init(&second.calls, 0u);
    atomic_init(&second.status, SALTS_EALREADY);
    second.sink = NULL;
    atomic_init(&second.snapshot_status, SALTS_EALREADY);
    datagram_sink_message(&first_message, "first");
    datagram_sink_message(&second_message, "second");
    check_equal(
        turbo_flow_publish_async(flow, "input", &first_message, datagram_sink_complete, &first),
        SALTS_OK);
    check_equal(
        turbo_flow_publish_async(flow, "input", &second_message, datagram_sink_complete, &second),
        SALTS_OK);
    turbo_flow_msg_cleanup(&first_message);
    turbo_flow_msg_cleanup(&second_message);

    deadline = salts_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
      if (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u) salts_sleep_ms(1u);
    } while (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u &&
             salts_monotonic_ms() < deadline);
    check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_ENOSPC);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.queue_depth + managed.in_flight, (uint64_t)1u);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)1u);
    check_equal(managed.backpressured, 1);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_STOPPED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("reports datagram initialization failure through the managed lifecycle") {
    static const char graph[] = "source input\n"
                                "stage output adapter cnet.udp.out\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    cnet_datagram_config datagram = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_config_t config = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    datagram.host = "not-a-numeric-address";
    config.flow = flow;
    config.adapter_name = "cnet.udp.out";
    config.datagram = &datagram;
    config.peer = datagram_sink_peer(9u);
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = DATAGRAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_datagram_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_ENOENT);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_FAILED);
    check_equal(managed.last_status, SALTS_ENOENT);
    check_equal(managed.queue_capacity, (uint64_t)2u);
    check_equal(managed.accepted, (uint64_t)0u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.rejected, (uint64_t)0u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("settles an Actor-admitted address-family failure exactly once") {
    cnet_datagram_config datagram = datagram_sink_cnet_config(1u);
    cnet_datagram_peer peer = {0};
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    datagram_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow;
    uint64_t deadline;

    peer.family = CNET_DATAGRAM_ADDRESS_IPV6;
    peer.port = 9u;
    peer.address[15] = 1u;
    flow = datagram_sink_flow(&datagram, peer, &sink);
    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    completion.sink = NULL;
    atomic_init(&completion.snapshot_status, SALTS_EALREADY);
    datagram_sink_message(&message, "wrong-family");
    check_equal(
        turbo_flow_publish_async(flow, "input", &message, datagram_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    deadline = salts_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_datagram_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_not_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("keeps managed snapshots coherent while stop settles one admitted claim") {
    cnet_datagram_config datagram = datagram_sink_cnet_config(1u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    datagram_sink_completion_t completion;
    datagram_sink_snapshot_race_t race;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = datagram_sink_flow(&datagram, datagram_sink_peer(9u), &sink);
    salts_thread_t observer = NULL;
    uint64_t deadline;
    int snapshot_status;

    memset(&race, 0, sizeof(race));
    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    completion.sink = NULL;
    atomic_init(&completion.snapshot_status, SALTS_EALREADY);
    atomic_init(&race.ready, false);
    atomic_init(&race.done, false);
    atomic_init(&race.snapshots, 0u);
    atomic_init(&race.status, SALTS_OK);
    race.flow = flow;
    datagram_sink_message(&message, "pending-stop-race");
    check_equal(
        turbo_flow_publish_async(flow, "input", &message, datagram_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    do {
      snapshot_status = turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot);
      check_true(snapshot_status == SALTS_OK || snapshot_status == SALTS_EBUSY);
      if (snapshot_status == SALTS_EBUSY || snapshot.active_requests == 0u) salts_thread_yield();
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)1u);

    check_equal(salts_thread_create(&observer, datagram_sink_snapshot_race, &race), SALTS_OK);
    while (!atomic_load_explicit(&race.ready, memory_order_acquire))
      salts_thread_yield();
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    atomic_store_explicit(&race.done, true, memory_order_release);
    check_equal(salts_thread_join(&observer), SALTS_OK);
    salts_thread_destroy(&observer);
    check_equal(atomic_load_explicit(&race.status, memory_order_acquire), SALTS_OK);
    check_true(atomic_load_explicit(&race.snapshots, memory_order_relaxed) > 0u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_STOPPED);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("preserves the full managed owner-name length") {
    cnet_datagram_config datagram = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_config_t config = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    char adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];

    memset(adapter_name, 'a', sizeof(adapter_name) - 1u);
    adapter_name[sizeof(adapter_name) - 1u] = '\0';
    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = adapter_name;
    config.datagram = &datagram;
    config.peer = datagram_sink_peer(9u);
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = DATAGRAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_datagram_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.owner_name, adapter_name);
    check_equal(descriptor.input.identity, adapter_name);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("derives the same bounded managed identity for an oversized adapter name") {
    cnet_datagram_config datagram = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *first_sink = NULL;
    turbo_flow_cnet_datagram_sink_t *second_sink = NULL;
    turbo_flow_cnet_datagram_sink_config_t config = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
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
    config.datagram = &datagram;
    config.peer = datagram_sink_peer(9u);
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = DATAGRAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_datagram_sink_register(&config, &first_sink), SALTS_OK);
    config.flow = second_flow;
    check_equal(turbo_flow_cnet_datagram_sink_register(&config, &second_sink), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(first_flow, 0u, &first), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(second_flow, 0u, &second), SALTS_OK);
    check_equal(first.owner_name, second.owner_name);
    check_equal(first.uid, second.uid);
    check_equal(first.input.identity, first.owner_name);
    check_equal(strncmp(first.owner_name, "xxh3-128:", sizeof("xxh3-128:") - 1u), 0);
    check_true(strlen(first.owner_name) <= TURBO_FLOW_RESOURCE_OWNER_MAX);
    check_greater(
        snprintf(expected_uid, sizeof(expected_uid), "cnet-datagram-sink:%s", first.owner_name), 0);
    check_equal(first.uid, expected_uid);

    turbo_flow_destroy(first_flow);
    turbo_flow_destroy(second_flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(first_sink), SALTS_OK);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(second_sink), SALTS_OK);
  }

  it("rolls back the staged adapter when the managed UID already exists") {
    cnet_datagram_config datagram = datagram_sink_cnet_config(2u);
    datagram_sink_boundary_fixture_t fixture;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops =
        TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_config_t config = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    datagram_sink_boundary_fixture_init(&fixture);
    boundary_ops.resource.metadata = datagram_sink_boundary_metadata;
    boundary_ops.descriptor = datagram_sink_boundary_descriptor;
    boundary_ops.snapshot = datagram_sink_boundary_snapshot;
    check_equal(turbo_flow_register_managed_boundary_provider(flow, "existing-owner", &boundary_ops,
                                                              &fixture),
                SALTS_OK);
    config.flow = flow;
    config.adapter_name = "cnet.udp.out";
    config.datagram = &datagram;
    config.peer = datagram_sink_peer(9u);
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = DATAGRAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_datagram_sink_register(&config, &sink), SALTS_EALREADY);
    check_null(sink);
    check_null(turbo_flow_find_adapter_schema(flow, "cnet.udp.out"));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);

    turbo_flow_destroy(flow);
  }
}
