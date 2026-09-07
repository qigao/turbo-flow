#include "tinytest.h"
#include "turbo_flow_cnet.h"
#include "turbo_flow_cnet_test_internal.h"

#include <salts/clock.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <string.h>

extern int cnet_terminal_sink_header_cpp_probe(void);

enum { PACKET_SINK_TEST_TIMEOUT_MS = 5000, PACKET_SINK_RACE_ITERATIONS = 32 };

typedef struct packet_sink_completion_s {
  atomic_size_t calls;
  atomic_int status;
} packet_sink_completion_t;

typedef struct packet_sink_snapshot_completion_s {
  turbo_flow_cnet_packet_sink_t *sink;
  atomic_size_t calls;
  atomic_int status;
  atomic_int snapshot_status;
} packet_sink_snapshot_completion_t;

typedef struct packet_sink_peer_s {
  cnet_packet_endpoint endpoint;
  cnet_packet_session session;
  size_t receives;
  size_t opens;
  size_t closes;
  int status;
  unsigned char payload[512];
  size_t payload_size;
} packet_sink_peer_t;

typedef struct packet_sink_race_s {
  turbo_flow_t *flow;
  turbo_flow_msg_t message;
  packet_sink_completion_t completion;
  atomic_bool publisher_ready;
  atomic_bool stopper_ready;
  atomic_bool go;
  int publish_status;
  int stop_status;
} packet_sink_race_t;

static native_io_backend_kind packet_sink_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static cnet_packet_endpoint_config packet_sink_endpoint_config(cnet_packet_protocol protocol) {
  cnet_packet_endpoint_config config = CNET_PACKET_ENDPOINT_CONFIG_INIT;
  config.protocol = protocol;
  config.session_capacity = 2u;
  config.datagram.backend = packet_sink_backend();
  config.datagram.host = "127.0.0.1";
  config.datagram.port = 0u;
  config.datagram.send_capacity = 8u;
  config.datagram.request_capacity = 9u;
  config.datagram.completion_batch_capacity = 4u;
  config.datagram.max_datagram_bytes = 1500u;
  config.datagram.receive_buffer_bytes = 1500u;
  config.kcp.mtu = 512u;
  config.kcp.send_window = 8u;
  config.kcp.receive_window = 8u;
  config.kcp.interval_ms = 10u;
  config.kcp.send_segment_capacity = 16u;
  config.kcp.max_message_bytes = 256u;
  return config;
}

static void packet_sink_secure_config(cnet_packet_endpoint_config *config) {
  memset(config->security.pre_shared_key, 0x5au, sizeof(config->security.pre_shared_key));
  config->security.mode = CNET_KCP_SECURITY_PSK_V1;
  config->security.handshake_retry_ms = 10u;
  config->security.fec.backend = CNET_KCP_FEC_REED_SOLOMON;
  config->security.fec.data_shards = 2u;
  config->security.fec.parity_shards = 1u;
  config->security.fec.max_payload_bytes = 624u;
  config->security.fec.receive_group_count = 4u;
  config->kcp.mtu = 576u;
}

static cnet_datagram_peer packet_sink_peer_address(uint16_t port) {
  cnet_datagram_peer peer = {0};
  peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  peer.port = port;
  peer.address[0] = 127u;
  peer.address[3] = 1u;
  return peer;
}

static int packet_sink_peer_admit(void *user, cnet_packet_endpoint *endpoint,
                                  cnet_packet_protocol protocol, const cnet_datagram_peer *peer,
                                  uint32_t conversation) {
  (void)user;
  (void)endpoint;
  (void)protocol;
  (void)peer;
  (void)conversation;
  return SALTS_OK;
}

static void packet_sink_peer_state(void *user, cnet_packet_endpoint *endpoint,
                                   cnet_packet_session session, cnet_packet_session_state state,
                                   const cnet_datagram_peer *peer, uint32_t conversation) {
  packet_sink_peer_t *owner = (packet_sink_peer_t *)user;
  (void)endpoint;
  (void)session;
  (void)peer;
  (void)conversation;
  if (state == CNET_PACKET_SESSION_OPEN) ++owner->opens;
  if (state == CNET_PACKET_SESSION_OPEN) owner->session = session;
  if (state == CNET_PACKET_SESSION_CLOSED) ++owner->closes;
}

static void packet_sink_peer_receive(void *user, cnet_packet_endpoint *endpoint,
                                     cnet_packet_session session, const cnet_receive_view *view) {
  packet_sink_peer_t *owner = (packet_sink_peer_t *)user;
  (void)endpoint;
  (void)session;
  if (!owner || !view || view->size > sizeof(owner->payload)) return;
  memcpy(owner->payload, view->data, view->size);
  owner->payload_size = view->size;
  ++owner->receives;
}

static void packet_sink_peer_error(void *user, cnet_packet_endpoint *endpoint,
                                   cnet_packet_session session, int status) {
  packet_sink_peer_t *owner = (packet_sink_peer_t *)user;
  (void)endpoint;
  (void)session;
  owner->status = status;
}

static int packet_sink_peer_init(packet_sink_peer_t *peer,
                                 cnet_packet_endpoint_config *config, uint16_t *port_out) {
  memset(peer, 0, sizeof(*peer));
  peer->status = SALTS_OK;
  config->observer.on_admit = packet_sink_peer_admit;
  config->observer.on_state = packet_sink_peer_state;
  config->observer.on_receive = packet_sink_peer_receive;
  config->observer.on_error = packet_sink_peer_error;
  config->observer.user = peer;
  if (cnet_packet_endpoint_init(&peer->endpoint, config) != SALTS_OK) return SALTS_EIO;
  return cnet_packet_endpoint_port(&peer->endpoint, port_out);
}

static void packet_sink_peer_stop(packet_sink_peer_t *peer) {
  check_equal(cnet_packet_endpoint_stop(&peer->endpoint, PACKET_SINK_TEST_TIMEOUT_MS), SALTS_OK);
  check_equal(cnet_packet_endpoint_destroy(&peer->endpoint), SALTS_OK);
}

static void packet_sink_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  packet_sink_completion_t *completion = (packet_sink_completion_t *)ctx;
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

static void packet_sink_complete_with_snapshot(void *ctx,
                                               const turbo_flow_publish_result_t *result) {
  packet_sink_snapshot_completion_t *completion =
      (packet_sink_snapshot_completion_t *)ctx;
  turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  int snapshot_status = turbo_flow_cnet_packet_sink_snapshot(completion->sink, &snapshot);
  atomic_store_explicit(&completion->snapshot_status, snapshot_status, memory_order_relaxed);
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

static void packet_sink_race_publish(void *ctx) {
  packet_sink_race_t *race = (packet_sink_race_t *)ctx;
  atomic_store_explicit(&race->publisher_ready, true, memory_order_release);
  while (!atomic_load_explicit(&race->go, memory_order_acquire)) salts_thread_yield();
  race->publish_status = turbo_flow_publish_async(race->flow, "input", &race->message,
                                                   packet_sink_complete, &race->completion);
}

static void packet_sink_race_stop(void *ctx) {
  packet_sink_race_t *race = (packet_sink_race_t *)ctx;
  atomic_store_explicit(&race->stopper_ready, true, memory_order_release);
  while (!atomic_load_explicit(&race->go, memory_order_acquire)) salts_thread_yield();
  race->stop_status = turbo_flow_stop(race->flow);
}

static void packet_sink_datagram_ignore_receive(void *user, cnet_datagram *datagram,
                                                const cnet_datagram_peer *peer,
                                                const cnet_receive_view *view) {
  (void)user;
  (void)datagram;
  (void)peer;
  (void)view;
}

static void packet_sink_datagram_ignore_send(void *user, cnet_datagram *datagram,
                                             const cnet_datagram_peer *peer, size_t size, int status,
                                             uint64_t tag) {
  (void)user;
  (void)datagram;
  (void)peer;
  (void)size;
  (void)status;
  (void)tag;
}

static void packet_sink_message(turbo_flow_msg_t *message, const char *payload) {
  turbo_flow_msg_init(message);
  message->owned_payload = tstr_dup(payload);
  message->payload = tstr_to_v(message->owned_payload);
}

static turbo_flow_t *packet_sink_flow(const cnet_packet_endpoint_config *endpoint,
                                      cnet_datagram_peer peer, uint32_t conversation,
                                      size_t send_capacity,
                                      turbo_flow_cnet_packet_sink_t **sink_out) {
  static const char graph[] = "source input\n"
                              "stage output adapter cnet.packet.out\n"
                              "stage main {\n"
                              "  input -> output\n"
                              "}\n";
  turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_config_t config = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  ingress.workers = 1u;
  config.flow = flow;
  config.adapter_name = "cnet.packet.out";
  config.endpoint = endpoint;
  config.peer = peer;
  config.conversation = conversation;
  config.send_capacity = send_capacity;
  config.max_message_bytes = 256u;
  if (!flow || turbo_flow_configure_async_ingress(flow, &ingress) != SALTS_OK ||
      turbo_flow_cnet_packet_sink_register(&config, sink_out) != SALTS_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static void packet_sink_round_trip(cnet_packet_protocol protocol, int secure, size_t cycles) {
  static const char payload[] = "packet-terminal";
  cnet_packet_endpoint_config peer_config = packet_sink_endpoint_config(protocol);
  cnet_packet_endpoint_config sink_config = packet_sink_endpoint_config(protocol);
  packet_sink_peer_t peer_owner;
  turbo_flow_cnet_packet_sink_t *sink = NULL;
  turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  packet_sink_completion_t completion;
  turbo_flow_msg_t message;
  turbo_flow_t *flow;
  cnet_datagram_peer peer;
  uint32_t conversation = protocol == CNET_PACKET_KCP && !secure ? UINT32_C(0x12345678) : 0u;
  uint16_t port = 0u;
  uint64_t deadline;
  size_t events = 0u;
  size_t cycle;

  if (secure) {
    packet_sink_secure_config(&peer_config);
    packet_sink_secure_config(&sink_config);
  }
  check_equal(packet_sink_peer_init(&peer_owner, &peer_config, &port), SALTS_OK);
  peer = packet_sink_peer_address(port);
  flow = packet_sink_flow(&sink_config, peer, conversation, 2u, &sink);
  check_not_null(flow);
  for (cycle = 0u; cycle < cycles; ++cycle) {
    if (cycle != 0u) check_equal(turbo_flow_start(flow), SALTS_OK);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
      check_equal(cnet_packet_poll(&peer_owner.endpoint, 1u, &events), SALTS_OK);
    } while ((!snapshot.session_open || (secure && peer_owner.opens <= cycle)) &&
             salts_monotonic_ms() < deadline);
    check_equal(snapshot.session_open, 1);

    packet_sink_message(&message, payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &message, packet_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    if (protocol == CNET_PACKET_KCP) {
      size_t attempt;
      for (attempt = 0u; attempt < 20u; ++attempt)
        check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
      check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    }

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline) {
      check_equal(cnet_packet_poll(&peer_owner.endpoint, 1u, &events), SALTS_OK);
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    }
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(snapshot.messages_sent, (uint64_t)(cycle + 1u));
    check_equal(snapshot.bytes_sent, (uint64_t)((cycle + 1u) * (sizeof(payload) - 1u)));
    check_equal(peer_owner.receives, cycle + 1u);
    check_equal(peer_owner.payload_size, sizeof(payload) - 1u);
    check_equal(peer_owner.payload, payload, sizeof(payload) - 1u);
    if (protocol == CNET_PACKET_KCP)
      check_equal(cnet_packet_session_close(&peer_owner.endpoint, peer_owner.session), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
  }
  turbo_flow_destroy(flow);
  check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
  packet_sink_peer_stop(&peer_owner);
}

spec("TurboFlow CNet packet terminal sink") {
  it("exposes a size-versioned C and C++ opaque contract") {
    turbo_flow_cnet_packet_sink_config_t config = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
    turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
    check_equal(config.size, TURBO_FLOW_CNET_PACKET_SINK_CONFIG_V1_SIZE);
    check_equal(config.version, TURBO_FLOW_CNET_PACKET_SINK_API_VERSION);
    check_equal(snapshot.size, TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_V1_SIZE);
    check_equal(snapshot.version, TURBO_FLOW_CNET_PACKET_SINK_API_VERSION);
    check_equal(cnet_terminal_sink_header_cpp_probe(), 0);
  }

  it("rejects invalid bounds, observers and packet session identity before publication") {
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_UDP);
    turbo_flow_cnet_packet_sink_config_t config = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    config.flow = flow;
    config.adapter_name = "cnet.packet.out";
    config.endpoint = &endpoint;
    config.peer = packet_sink_peer_address(9u);
    config.max_message_bytes = 256u;

    check_not_null(flow);
    check_equal(turbo_flow_cnet_packet_sink_register(NULL, &sink), SALTS_EINVAL);
    check_null(sink);
    check_equal(turbo_flow_cnet_packet_sink_register(&config, NULL), SALTS_EINVAL);
    config.size = 0u;
    check_equal(turbo_flow_cnet_packet_sink_register(&config, &sink), SALTS_EINVAL);
    config = (turbo_flow_cnet_packet_sink_config_t)TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
    config.flow = flow;
    config.adapter_name = "cnet.packet.out";
    config.endpoint = &endpoint;
    config.peer = packet_sink_peer_address(9u);
    config.max_message_bytes = 256u;
    config.send_capacity = 0u;
    check_equal(turbo_flow_cnet_packet_sink_register(&config, &sink), SALTS_EINVAL);
    config.send_capacity = 1u;
    config.conversation = 1u;
    check_equal(turbo_flow_cnet_packet_sink_register(&config, &sink), SALTS_EINVAL);
    config.conversation = 0u;
    endpoint.observer.user = flow;
    check_equal(turbo_flow_cnet_packet_sink_register(&config, &sink), SALTS_EINVAL);
    endpoint.observer.user = NULL;
    endpoint.protocol = CNET_PACKET_KCP;
    endpoint.kcp.stream_mode = true;
    config.conversation = 1u;
    check_equal(turbo_flow_cnet_packet_sink_register(&config, &sink), SALTS_EINVAL);
    check_null(sink);
    turbo_flow_destroy(flow);
  }

  it("settles raw UDP only from the authoritative tagged native terminal") {
    packet_sink_round_trip(CNET_PACKET_UDP, 0, 1u);
  }

  it("fails snapshot fast when called reentrantly from terminal completion") {
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_UDP);
    cnet_packet_endpoint_config peer_config = packet_sink_endpoint_config(CNET_PACKET_UDP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    packet_sink_peer_t peer_owner;
    packet_sink_snapshot_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow;
    cnet_datagram_peer peer;
    uint16_t port = 0u;
    size_t events = 0u;
    uint64_t deadline;

    check_equal(packet_sink_peer_init(&peer_owner, &peer_config, &port), SALTS_OK);
    peer = packet_sink_peer_address(port);
    flow = packet_sink_flow(&endpoint, peer, 0u, 1u, &sink);
    check_not_null(flow);
    completion.sink = sink;
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    atomic_init(&completion.snapshot_status, SALTS_EALREADY);
    packet_sink_message(&message, "snapshot-from-completion");
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         packet_sink_complete_with_snapshot, &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline) {
      check_equal(cnet_packet_poll(&peer_owner.endpoint, 1u, &events), SALTS_OK);
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, NULL), SALTS_OK);
    }
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.snapshot_status, memory_order_acquire),
                SALTS_EBUSY);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
    packet_sink_peer_stop(&peer_owner);
  }

  it("retains plain KCP claims until the peer acknowledges the logical message") {
    packet_sink_round_trip(CNET_PACKET_KCP, 0, 1u);
  }

  it("preserves secure KCP/FEC terminal semantics across Flow restart") {
    packet_sink_round_trip(CNET_PACKET_KCP, 1, 2u);
  }

  it("rejects a delayed terminal from the previous session generation") {
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_UDP);
    cnet_packet_endpoint_config peer_config = packet_sink_endpoint_config(CNET_PACKET_UDP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
    packet_sink_peer_t peer_owner;
    packet_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow;
    cnet_datagram_peer peer;
    uint16_t port = 0u;
    size_t events = 0u;
    uint64_t deadline;
    int snapshot_status;

    check_equal(packet_sink_peer_init(&peer_owner, &peer_config, &port), SALTS_OK);
    peer = packet_sink_peer_address(port);
    flow = packet_sink_flow(&endpoint, peer, 0u, 1u, &sink);
    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
      check_equal(cnet_packet_poll(&peer_owner.endpoint, 1u, &events), SALTS_OK);
    } while (!snapshot.session_open && salts_monotonic_ms() < deadline);
    check_equal(snapshot.session_open, 1);

    packet_sink_message(&message, "previous-generation-terminal");
    check_equal(turbo_flow_publish_async(flow, "input", &message, packet_sink_complete,
                                         &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline) {
      check_equal(cnet_packet_poll(&peer_owner.endpoint, 1u, &events), SALTS_OK);
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    }
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    atomic_store_explicit(&completion.calls, 0u, memory_order_release);
    atomic_store_explicit(&completion.status, SALTS_EALREADY, memory_order_relaxed);
    packet_sink_message(&message, "current-generation-claim");
    check_equal(turbo_flow_publish_async(flow, "input", &message, packet_sink_complete,
                                         &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    do {
      snapshot_status = turbo_flow_cnet_packet_sink_snapshot(sink, &snapshot);
      if (snapshot_status != SALTS_OK) break;
      salts_thread_yield();
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot_status, SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)1u);

    check_equal(turbo_flow_cnet_test_packet_sink_replay_last_terminal(sink), SALTS_EPROTO);
    check_equal(turbo_flow_cnet_packet_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_PACKET_SINK_FAILED);
    check_equal(snapshot.status, SALTS_EPROTO);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ECANCELED);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
    packet_sink_peer_stop(&peer_owner);
  }

  it("preserves CNet send admission errors instead of converting them to sink capacity") {
    static const char fragmented[] =
        "this message requires several KCP fragments at the configured minimum-sized test MTU";
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_KCP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    packet_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow;
    uint64_t deadline;

    endpoint.kcp.mtu = 64u;
    endpoint.kcp.send_segment_capacity = 1u;
    flow = packet_sink_flow(&endpoint, packet_sink_peer_address(9u), UINT32_C(0x12345678), 2u,
                            &sink);
    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    packet_sink_message(&message, fragmented);
    check_equal(turbo_flow_publish_async(flow, "input", &message, packet_sink_complete, &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, NULL), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ENOBUFS);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
  }

  it("cancels a backend-pending KCP claim exactly once during stop") {
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_KCP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
    packet_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = packet_sink_flow(&endpoint, packet_sink_peer_address(9u),
                                          UINT32_C(0x12345678), 1u, &sink);
    uint64_t deadline;

    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    packet_sink_message(&message, "pending-kcp");
    check_equal(turbo_flow_publish_async(flow, "input", &message, packet_sink_complete, &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(turbo_flow_cnet_packet_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_PACKET_SINK_STOPPED);
    check_equal(snapshot.active_requests, (size_t)0u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
  }

  it("drains an admitted claim before reporting a concrete stop error and retries cleanup") {
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_KCP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
    packet_sink_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = packet_sink_flow(&endpoint, packet_sink_peer_address(9u),
                                          UINT32_C(0x12345678), 1u, &sink);
    uint64_t deadline;

    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    packet_sink_message(&message, "stop-error-retry");
    check_equal(turbo_flow_publish_async(flow, "input", &message, packet_sink_complete,
                                         &completion),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_cnet_test_packet_sink_fail_next_stop(sink, SALTS_EIO), SALTS_OK);

    check_equal(turbo_flow_stop(flow), SALTS_EIO);
    check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_FAILED);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(turbo_flow_cnet_packet_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_PACKET_SINK_FAILED);
    check_equal(snapshot.active_requests, (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_STOPPED);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(turbo_flow_cnet_packet_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_PACKET_SINK_STOPPED);
    check_equal(snapshot.active_requests, (size_t)0u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
  }

  it("preserves a malformed KCP protocol failure as the terminal owner status") {
    static const unsigned char malformed[] = {0u, 1u, 2u, 3u};
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_KCP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    turbo_flow_cnet_packet_sink_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
    cnet_datagram_config attacker_config = CNET_DATAGRAM_CONFIG_INIT;
    cnet_datagram attacker = {0};
    turbo_flow_t *flow = packet_sink_flow(&endpoint, packet_sink_peer_address(9u),
                                          UINT32_C(0x12345678), 1u, &sink);
    cnet_datagram_peer target;
    size_t events = 0u;
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(flow);
    check_equal(turbo_flow_cnet_packet_sink_snapshot(sink, &snapshot), SALTS_OK);
    attacker_config.backend = packet_sink_backend();
    attacker_config.host = "127.0.0.1";
    attacker_config.port = 0u;
    attacker_config.send_capacity = 1u;
    attacker_config.request_capacity = 2u;
    attacker_config.completion_batch_capacity = 2u;
    attacker_config.max_datagram_bytes = 256u;
    attacker_config.receive_buffer_bytes = 256u;
    attacker_config.observer.on_receive = packet_sink_datagram_ignore_receive;
    attacker_config.observer.on_send = packet_sink_datagram_ignore_send;
    check_equal(cnet_datagram_init(&attacker, &attacker_config), SALTS_OK);
    target = packet_sink_peer_address(snapshot.bound_port);
    check_equal(cnet_datagram_send(&attacker, &target, malformed, sizeof(malformed), 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    while (poll_status == SALTS_OK && salts_monotonic_ms() < deadline) {
      check_equal(cnet_datagram_poll(&attacker, 1u, &events), SALTS_OK);
      poll_status = turbo_flow_cnet_packet_sink_poll(sink, 1u, &snapshot);
    }
    check_equal(poll_status, SALTS_EPROTO);
    check_equal(snapshot.state, TURBO_FLOW_CNET_PACKET_SINK_FAILED);
    check_equal(snapshot.status, SALTS_EPROTO);

    check_equal(cnet_datagram_stop(&attacker, PACKET_SINK_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_datagram_destroy(&attacker), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
  }

  it("returns SALTS_ENOSPC at its own bound and drains cancellation exactly once") {
    cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_UDP);
    turbo_flow_cnet_packet_sink_t *sink = NULL;
    packet_sink_completion_t first;
    packet_sink_completion_t second;
    turbo_flow_msg_t first_message;
    turbo_flow_msg_t second_message;
    turbo_flow_t *flow = packet_sink_flow(&endpoint, packet_sink_peer_address(9u), 0u, 1u, &sink);
    uint64_t deadline;

    check_not_null(flow);
    atomic_init(&first.calls, 0u);
    atomic_init(&first.status, SALTS_EALREADY);
    atomic_init(&second.calls, 0u);
    atomic_init(&second.status, SALTS_EALREADY);
    packet_sink_message(&first_message, "first");
    packet_sink_message(&second_message, "second");
    check_equal(turbo_flow_publish_async(flow, "input", &first_message, packet_sink_complete, &first),
                SALTS_OK);
    check_equal(
        turbo_flow_publish_async(flow, "input", &second_message, packet_sink_complete, &second),
        SALTS_OK);
    turbo_flow_msg_cleanup(&first_message);
    turbo_flow_msg_cleanup(&second_message);

    deadline = salts_monotonic_ms() + PACKET_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_ENOSPC);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
  }

  it("linearizes concurrent publication and stop without losing or duplicating a claim") {
    size_t iteration;
    for (iteration = 0u; iteration < PACKET_SINK_RACE_ITERATIONS; ++iteration) {
      cnet_packet_endpoint_config endpoint = packet_sink_endpoint_config(CNET_PACKET_UDP);
      turbo_flow_cnet_packet_sink_t *sink = NULL;
      packet_sink_race_t race;
      salts_thread_t publisher = NULL;
      salts_thread_t stopper = NULL;

      memset(&race, 0, sizeof(race));
      race.flow =
          packet_sink_flow(&endpoint, packet_sink_peer_address(9u), 0u, 1u, &sink);
      check_not_null(race.flow);
      packet_sink_message(&race.message, "publish-stop-race");
      atomic_init(&race.completion.calls, 0u);
      atomic_init(&race.completion.status, SALTS_EALREADY);
      atomic_init(&race.publisher_ready, false);
      atomic_init(&race.stopper_ready, false);
      atomic_init(&race.go, false);
      race.publish_status = SALTS_EALREADY;
      race.stop_status = SALTS_EALREADY;

      check_equal(salts_thread_create(&publisher, packet_sink_race_publish, &race), SALTS_OK);
      check_equal(salts_thread_create(&stopper, packet_sink_race_stop, &race), SALTS_OK);
      while (!atomic_load_explicit(&race.publisher_ready, memory_order_acquire) ||
             !atomic_load_explicit(&race.stopper_ready, memory_order_acquire))
        salts_thread_yield();
      atomic_store_explicit(&race.go, true, memory_order_release);
      check_equal(salts_thread_join(&publisher), SALTS_OK);
      check_equal(salts_thread_join(&stopper), SALTS_OK);
      salts_thread_destroy(&publisher);
      salts_thread_destroy(&stopper);

      check_equal(race.stop_status, SALTS_OK);
      capture(race.publish_status, "%d");
      check_true(race.publish_status == SALTS_OK || race.publish_status == SALTS_ESHUTDOWN ||
                 race.publish_status == SALTS_EINVAL);
      if (race.publish_status == SALTS_OK) {
        check_equal(atomic_load_explicit(&race.completion.calls, memory_order_acquire), (size_t)1u);
        capture(race.completion.status, "%d");
        check_true(atomic_load_explicit(&race.completion.status, memory_order_acquire) == SALTS_OK ||
                   atomic_load_explicit(&race.completion.status, memory_order_acquire) ==
                       SALTS_ECANCELED ||
                   atomic_load_explicit(&race.completion.status, memory_order_acquire) ==
                       SALTS_ESHUTDOWN);
      } else {
        check_equal(atomic_load_explicit(&race.completion.calls, memory_order_acquire), (size_t)0u);
      }
      turbo_flow_msg_cleanup(&race.message);
      turbo_flow_destroy(race.flow);
      check_equal(turbo_flow_cnet_packet_sink_destroy(sink), SALTS_OK);
    }
  }
}
