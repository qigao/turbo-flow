#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include <salts/clock.h>

#include <string.h>

extern int cnet_packet_source_header_cpp_probe(void);

enum { PACKET_SOURCE_TEST_TIMEOUT_MS = 5000 };

static native_io_backend_kind packet_source_test_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static cnet_packet_endpoint_config packet_source_endpoint_config(cnet_packet_protocol protocol) {
  cnet_packet_endpoint_config config = CNET_PACKET_ENDPOINT_CONFIG_INIT;
  config.protocol = protocol;
  config.session_capacity = 2u;
  config.datagram.backend = packet_source_test_backend();
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

typedef struct packet_source_graph_probe_s {
  size_t count;
  uint64_t ids[8];
  char payloads[8][64];
  turbo_flow_cnet_packet_message_context_t contexts[8];
} packet_source_graph_probe_t;

static int packet_source_graph_sink(turbo_flow_msg_t *message, void *ctx) {
  packet_source_graph_probe_t *probe = (packet_source_graph_probe_t *)ctx;
  const turbo_flow_cnet_packet_message_context_t *packet =
      turbo_flow_cnet_packet_message_context(message);
  if (!probe || !packet || packet->size < sizeof(*packet) ||
      packet->version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION ||
      !cnet_packet_session_valid(packet->session) || probe->count >= 8u ||
      message->payload.len >= sizeof(probe->payloads[0]))
    return SALTS_EPROTO;
  probe->ids[probe->count] = message->id;
  probe->contexts[probe->count] = *packet;
  memcpy(probe->payloads[probe->count], message->payload.data, message->payload.len);
  probe->payloads[probe->count][message->payload.len] = '\0';
  ++probe->count;
  return SALTS_OK;
}

static turbo_flow_t *packet_source_started_flow(packet_source_graph_probe_t *probe) {
  static const char dsl[] = "source input\n"
                            "stage sink\n"
                            "stage main {\n"
                            "  input -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_parse_string(flow, dsl, strlen(dsl)) != SALTS_OK ||
      turbo_flow_register_stage_ex(flow, "sink", packet_source_graph_sink, probe, NULL) !=
          SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_cnet_packet_source_config_t
packet_source_config(turbo_flow_t *flow, const cnet_packet_endpoint_config *endpoint) {
  turbo_flow_cnet_packet_source_config_t config = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
  config.flow = flow;
  config.source_name = "input";
  config.endpoint = endpoint;
  config.queue_capacity = 2u;
  config.max_message_bytes = 256u;
  config.scheduler_capacity = 8u;
  config.scheduler_max_steps_per_poll = 32u;
  config.first_message_id = 1u;
  return config;
}

static cnet_datagram_peer packet_source_peer(uint16_t port) {
  cnet_datagram_peer peer = {0};
  peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  peer.port = port;
  peer.address[0] = 127u;
  peer.address[3] = 1u;
  return peer;
}

static void packet_source_stop_destroy(turbo_flow_cnet_packet_source_t *source) {
  check_equal(turbo_flow_cnet_packet_source_stop(source, PACKET_SOURCE_TEST_TIMEOUT_MS), SALTS_OK);
  check_equal(turbo_flow_cnet_packet_source_destroy(source), SALTS_OK);
}

static void packet_source_round_trip(cnet_packet_protocol protocol, uint32_t conversation) {
  static const char message[] = "packet-round-trip";
  cnet_packet_endpoint_config left_endpoint = packet_source_endpoint_config(protocol);
  cnet_packet_endpoint_config right_endpoint = packet_source_endpoint_config(protocol);
  packet_source_graph_probe_t left_probe = {0};
  packet_source_graph_probe_t right_probe = {0};
  turbo_flow_t *left_flow = packet_source_started_flow(&left_probe);
  turbo_flow_t *right_flow = packet_source_started_flow(&right_probe);
  turbo_flow_cnet_packet_source_config_t left_config =
      packet_source_config(left_flow, &left_endpoint);
  turbo_flow_cnet_packet_source_config_t right_config =
      packet_source_config(right_flow, &right_endpoint);
  turbo_flow_cnet_packet_source_t *left = NULL;
  turbo_flow_cnet_packet_source_t *right = NULL;
  turbo_flow_cnet_packet_source_snapshot_t right_snapshot =
      TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
  cnet_packet_session left_session = {0};
  cnet_packet_session_info left_info = {0};
  cnet_packet_session_info right_info = {0};
  cnet_datagram_peer right_peer;
  uint64_t deadline;

  check_not_null(left_flow);
  check_not_null(right_flow);
  check_equal(turbo_flow_cnet_packet_source_open(&left_config, &left), SALTS_OK);
  check_equal(turbo_flow_cnet_packet_source_open(&right_config, &right), SALTS_OK);
  check_equal(turbo_flow_cnet_packet_source_snapshot(right, &right_snapshot), SALTS_OK);
  right_peer = packet_source_peer(right_snapshot.bound_port);
  check_equal(
      turbo_flow_cnet_packet_source_session_open(left, &right_peer, conversation, &left_session),
      SALTS_OK);
  check_equal(turbo_flow_cnet_packet_source_session_get_info(left, left_session, &left_info),
              SALTS_OK);
  check_equal(left_info.protocol, protocol);
  check_equal(left_info.peer.port, right_snapshot.bound_port);
  check_equal(left_info.conversation, conversation);
  check_equal(turbo_flow_cnet_packet_source_send(left, left_session, message, sizeof(message)),
              SALTS_OK);
  deadline = salts_monotonic_ms() + PACKET_SOURCE_TEST_TIMEOUT_MS;
  while (right_snapshot.messages_received == 0u && salts_monotonic_ms() < deadline) {
    check_equal(turbo_flow_cnet_packet_source_poll(left, 1u, NULL), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_poll(right, 1u, &right_snapshot), SALTS_OK);
  }
  check_equal(right_snapshot.messages_received, 1u);
  check_equal(right_snapshot.queue_depth, 1u);
  check_equal(right_probe.count, 0u);
  check_equal(turbo_flow_cnet_packet_source_request(right, 1u), SALTS_OK);
  check_equal(turbo_flow_cnet_packet_source_poll(right, 0u, &right_snapshot), SALTS_OK);
  check_equal(right_snapshot.queue_depth, 0u);
  check_equal(right_probe.count, 1u);
  check_equal(right_probe.ids[0], 1u);
  check_equal(right_probe.payloads[0], message, sizeof(message));
  check_equal(right_probe.contexts[0].info.protocol, protocol);
  check_equal(right_probe.contexts[0].info.conversation, conversation);
  check_equal(turbo_flow_cnet_packet_source_session_get_info(right, right_probe.contexts[0].session,
                                                             &right_info),
              SALTS_OK);
  check_equal(right_info.peer.port, right_probe.contexts[0].info.peer.port);
  check_equal(right_info.conversation, right_probe.contexts[0].info.conversation);

  packet_source_stop_destroy(right);
  packet_source_stop_destroy(left);
  check_equal(turbo_flow_stop(right_flow), SALTS_OK);
  check_equal(turbo_flow_stop(left_flow), SALTS_OK);
  turbo_flow_destroy(right_flow);
  turbo_flow_destroy(left_flow);
}

static void packet_source_secure_config(cnet_packet_endpoint_config *config,
                                        unsigned char psk_byte) {
  memset(config->security.pre_shared_key, psk_byte, sizeof(config->security.pre_shared_key));
  config->security.mode = CNET_KCP_SECURITY_PSK_V1;
  config->security.handshake_retry_ms = 10u;
  config->security.fec.backend = CNET_KCP_FEC_REED_SOLOMON;
  config->security.fec.data_shards = 2u;
  config->security.fec.parity_shards = 1u;
  config->security.fec.max_payload_bytes = 624u;
  config->security.fec.receive_group_count = 4u;
  config->kcp.mtu = 576u;
}

typedef struct packet_source_datagram_probe_s {
  size_t sends;
  int status;
} packet_source_datagram_probe_t;

static void packet_source_datagram_receive(void *user, cnet_datagram *datagram,
                                           const cnet_datagram_peer *peer,
                                           const cnet_receive_view *view) {
  (void)user;
  (void)datagram;
  (void)peer;
  (void)view;
}

static void packet_source_datagram_send(void *user, cnet_datagram *datagram,
                                        const cnet_datagram_peer *peer, size_t size, int status,
                                        uint64_t tag) {
  packet_source_datagram_probe_t *probe = (packet_source_datagram_probe_t *)user;
  (void)datagram;
  (void)peer;
  (void)size;
  (void)tag;
  probe->status = status;
  ++probe->sends;
}

spec("CNet packet source owner") {
  it("exposes a size-versioned C and C++ opaque contract") {
    turbo_flow_cnet_packet_source_config_t config = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
    turbo_flow_cnet_packet_source_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    check_equal(config.size, TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_V1_SIZE);
    check_equal(config.version, TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION);
    check_equal(snapshot.size, TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_V1_SIZE);
    check_equal(snapshot.version, TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION);
    check_equal(cnet_packet_source_header_cpp_probe(), 0);
  }

  it("rejects invalid endpoint, queue, message and session bounds before publishing") {
    cnet_packet_endpoint_config endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    packet_source_graph_probe_t probe = {0};
    turbo_flow_t *flow = packet_source_started_flow(&probe);
    turbo_flow_cnet_packet_source_t *source = NULL;
    turbo_flow_cnet_packet_source_config_t config = packet_source_config(flow, &endpoint);

    check_not_null(flow);
    check_equal(turbo_flow_cnet_packet_source_open(NULL, &source), SALTS_EINVAL);
    check_null(source);
    check_equal(turbo_flow_cnet_packet_source_open(&config, NULL), SALTS_EINVAL);
    config.size = 0u;
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);
    config = packet_source_config(flow, &endpoint);
    config.queue_capacity = 0u;
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);
    config = packet_source_config(flow, &endpoint);
    config.max_message_bytes = 0u;
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);
    config = packet_source_config(flow, &endpoint);
    endpoint.session_capacity = 0u;
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("preserves UDP session identity and gates graph delivery on demand") {
    packet_source_round_trip(CNET_PACKET_UDP, 0u);
  }

  it("preserves plain KCP identity while protocol progress remains demand-independent") {
    packet_source_round_trip(CNET_PACKET_KCP, UINT32_C(0x12345678));
  }

  it("rejects stale sessions after bounded slot reuse") {
    cnet_packet_endpoint_config endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    packet_source_graph_probe_t probe = {0};
    turbo_flow_t *flow = packet_source_started_flow(&probe);
    turbo_flow_cnet_packet_source_config_t config = packet_source_config(flow, &endpoint);
    turbo_flow_cnet_packet_source_t *source = NULL;
    cnet_datagram_peer first_peer = packet_source_peer(10001u);
    cnet_datagram_peer second_peer = packet_source_peer(10002u);
    cnet_packet_session first = {0};
    cnet_packet_session second = {0};
    cnet_packet_session_info info = {0};

    check_not_null(flow);
    endpoint.session_capacity = 1u;
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_session_open(source, &first_peer, 0u, &first),
                SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_session_close(source, first), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_session_open(source, &second_peer, 0u, &second),
                SALTS_OK);
    check_equal(first.slot, second.slot);
    check_not_equal(first.generation, second.generation);
    check_equal(turbo_flow_cnet_packet_source_send(source, first, "x", 1u), SALTS_ENOENT);
    check_equal(turbo_flow_cnet_packet_source_session_get_info(source, first, &info), SALTS_ENOENT);
    check_equal(turbo_flow_cnet_packet_source_session_get_info(source, second, &info), SALTS_OK);
    check_equal(info.peer.port, second_peer.port);

    packet_source_stop_destroy(source);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails explicitly when the owning queue is full without publishing a partial message") {
    cnet_packet_endpoint_config left_endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    cnet_packet_endpoint_config right_endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    packet_source_graph_probe_t left_probe = {0};
    packet_source_graph_probe_t right_probe = {0};
    turbo_flow_t *left_flow = packet_source_started_flow(&left_probe);
    turbo_flow_t *right_flow = packet_source_started_flow(&right_probe);
    turbo_flow_cnet_packet_source_config_t left_config =
        packet_source_config(left_flow, &left_endpoint);
    turbo_flow_cnet_packet_source_config_t right_config =
        packet_source_config(right_flow, &right_endpoint);
    turbo_flow_cnet_packet_source_t *left = NULL;
    turbo_flow_cnet_packet_source_t *right = NULL;
    turbo_flow_cnet_packet_source_snapshot_t right_snapshot =
        TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    cnet_packet_session session = {0};
    cnet_datagram_peer peer;
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(left_flow);
    check_not_null(right_flow);
    right_config.queue_capacity = 1u;
    check_equal(turbo_flow_cnet_packet_source_open(&left_config, &left), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_open(&right_config, &right), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_snapshot(right, &right_snapshot), SALTS_OK);
    peer = packet_source_peer(right_snapshot.bound_port);
    check_equal(turbo_flow_cnet_packet_source_session_open(left, &peer, 0u, &session), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_send(left, session, "one", 3u), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_send(left, session, "two", 3u), SALTS_OK);
    deadline = salts_monotonic_ms() + PACKET_SOURCE_TEST_TIMEOUT_MS;
    while (poll_status == SALTS_OK && salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_packet_source_poll(left, 1u, NULL), SALTS_OK);
      poll_status = turbo_flow_cnet_packet_source_poll(right, 1u, &right_snapshot);
    }
    check_equal(poll_status, SALTS_ENOBUFS);
    check_equal(right_snapshot.state, TURBO_FLOW_CNET_PACKET_SOURCE_FAILED);
    check_equal(right_snapshot.status, SALTS_ENOBUFS);
    check_equal(right_snapshot.queue_depth, 1u);
    check_equal(right_snapshot.messages_received, 1u);
    check_equal(right_probe.count, 0u);

    packet_source_stop_destroy(right);
    packet_source_stop_destroy(left);
    check_equal(turbo_flow_stop(right_flow), SALTS_OK);
    check_equal(turbo_flow_stop(left_flow), SALTS_OK);
    turbo_flow_destroy(right_flow);
    turbo_flow_destroy(left_flow);
  }

  it("fails an oversize callback before queue or graph publication") {
    cnet_packet_endpoint_config left_endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    cnet_packet_endpoint_config right_endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    packet_source_graph_probe_t left_probe = {0};
    packet_source_graph_probe_t right_probe = {0};
    turbo_flow_t *left_flow = packet_source_started_flow(&left_probe);
    turbo_flow_t *right_flow = packet_source_started_flow(&right_probe);
    turbo_flow_cnet_packet_source_config_t left_config =
        packet_source_config(left_flow, &left_endpoint);
    turbo_flow_cnet_packet_source_config_t right_config =
        packet_source_config(right_flow, &right_endpoint);
    turbo_flow_cnet_packet_source_t *left = NULL;
    turbo_flow_cnet_packet_source_t *right = NULL;
    turbo_flow_cnet_packet_source_snapshot_t right_snapshot =
        TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    cnet_packet_session session = {0};
    cnet_datagram_peer peer;
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(left_flow);
    check_not_null(right_flow);
    right_config.max_message_bytes = 4u;
    check_equal(turbo_flow_cnet_packet_source_open(&left_config, &left), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_open(&right_config, &right), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_snapshot(right, &right_snapshot), SALTS_OK);
    peer = packet_source_peer(right_snapshot.bound_port);
    check_equal(turbo_flow_cnet_packet_source_session_open(left, &peer, 0u, &session), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_send(left, session, "large", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + PACKET_SOURCE_TEST_TIMEOUT_MS;
    while (poll_status == SALTS_OK && salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_packet_source_poll(left, 1u, NULL), SALTS_OK);
      poll_status = turbo_flow_cnet_packet_source_poll(right, 1u, &right_snapshot);
    }
    check_equal(poll_status, SALTS_EMSGSIZE);
    check_equal(right_snapshot.queue_depth, 0u);
    check_equal(right_snapshot.messages_received, 0u);
    check_equal(right_snapshot.bytes_received, 0u);
    check_equal(right_probe.count, 0u);

    packet_source_stop_destroy(right);
    packet_source_stop_destroy(left);
    check_equal(turbo_flow_stop(right_flow), SALTS_OK);
    check_equal(turbo_flow_stop(left_flow), SALTS_OK);
    turbo_flow_destroy(right_flow);
    turbo_flow_destroy(left_flow);
  }

  it("accepts matching authenticated KCP with valid Reed-Solomon FEC bounds") {
    static const char message[] = "secure-packet";
    cnet_packet_endpoint_config left_endpoint = packet_source_endpoint_config(CNET_PACKET_KCP);
    cnet_packet_endpoint_config right_endpoint = packet_source_endpoint_config(CNET_PACKET_KCP);
    packet_source_graph_probe_t left_probe = {0};
    packet_source_graph_probe_t right_probe = {0};
    turbo_flow_t *left_flow = packet_source_started_flow(&left_probe);
    turbo_flow_t *right_flow = packet_source_started_flow(&right_probe);
    turbo_flow_cnet_packet_source_config_t left_config;
    turbo_flow_cnet_packet_source_config_t right_config;
    turbo_flow_cnet_packet_source_t *left = NULL;
    turbo_flow_cnet_packet_source_t *right = NULL;
    turbo_flow_cnet_packet_source_snapshot_t left_snapshot =
        TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_packet_source_snapshot_t right_snapshot =
        TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    cnet_packet_session session = {0};
    cnet_datagram_peer peer;
    uint64_t deadline;

    packet_source_secure_config(&left_endpoint, 0x5au);
    packet_source_secure_config(&right_endpoint, 0x5au);
    left_config = packet_source_config(left_flow, &left_endpoint);
    right_config = packet_source_config(right_flow, &right_endpoint);
    check_not_null(left_flow);
    check_not_null(right_flow);
    check_equal(turbo_flow_cnet_packet_source_open(&left_config, &left), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_open(&right_config, &right), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_snapshot(right, &right_snapshot), SALTS_OK);
    peer = packet_source_peer(right_snapshot.bound_port);
    check_equal(turbo_flow_cnet_packet_source_session_open(left, &peer, 0u, &session), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_send(left, session, message, sizeof(message)),
                SALTS_EBUSY);
    deadline = salts_monotonic_ms() + PACKET_SOURCE_TEST_TIMEOUT_MS;
    while ((left_snapshot.sessions_opened == 0u || right_snapshot.sessions_opened == 0u) &&
           salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_packet_source_poll(left, 1u, &left_snapshot), SALTS_OK);
      check_equal(turbo_flow_cnet_packet_source_poll(right, 1u, &right_snapshot), SALTS_OK);
    }
    check_equal(left_snapshot.sessions_opened, 1u);
    check_equal(right_snapshot.sessions_admitted, 1u);
    check_equal(right_snapshot.sessions_opened, 1u);
    check_equal(turbo_flow_cnet_packet_source_send(left, session, message, sizeof(message)),
                SALTS_OK);
    deadline = salts_monotonic_ms() + PACKET_SOURCE_TEST_TIMEOUT_MS;
    while (right_snapshot.messages_received == 0u && salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_packet_source_poll(left, 1u, &left_snapshot), SALTS_OK);
      check_equal(turbo_flow_cnet_packet_source_poll(right, 1u, &right_snapshot), SALTS_OK);
    }
    check_equal(right_snapshot.queue_depth, 1u);
    check_equal(right_probe.count, 0u);
    check_equal(turbo_flow_cnet_packet_source_request(right, 1u), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_poll(right, 0u, &right_snapshot), SALTS_OK);
    check_equal(right_probe.count, 1u);
    check_equal(right_probe.payloads[0], message, sizeof(message));
    check_not_equal(right_probe.contexts[0].info.conversation, 0u);

    packet_source_stop_destroy(right);
    packet_source_stop_destroy(left);
    check_equal(turbo_flow_stop(right_flow), SALTS_OK);
    check_equal(turbo_flow_stop(left_flow), SALTS_OK);
    turbo_flow_destroy(right_flow);
    turbo_flow_destroy(left_flow);
  }

  it("rejects mismatched authenticated KCP before admission or publication") {
    cnet_packet_endpoint_config left_endpoint = packet_source_endpoint_config(CNET_PACKET_KCP);
    cnet_packet_endpoint_config right_endpoint = packet_source_endpoint_config(CNET_PACKET_KCP);
    packet_source_graph_probe_t left_probe = {0};
    packet_source_graph_probe_t right_probe = {0};
    turbo_flow_t *left_flow = packet_source_started_flow(&left_probe);
    turbo_flow_t *right_flow = packet_source_started_flow(&right_probe);
    turbo_flow_cnet_packet_source_config_t left_config;
    turbo_flow_cnet_packet_source_config_t right_config;
    turbo_flow_cnet_packet_source_t *left = NULL;
    turbo_flow_cnet_packet_source_t *right = NULL;
    turbo_flow_cnet_packet_source_snapshot_t left_snapshot =
        TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_packet_source_snapshot_t right_snapshot =
        TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    cnet_packet_session session = {0};
    cnet_datagram_peer peer;
    size_t attempt;

    packet_source_secure_config(&left_endpoint, 0x11u);
    packet_source_secure_config(&right_endpoint, 0x22u);
    left_config = packet_source_config(left_flow, &left_endpoint);
    right_config = packet_source_config(right_flow, &right_endpoint);
    check_not_null(left_flow);
    check_not_null(right_flow);
    check_equal(turbo_flow_cnet_packet_source_open(&left_config, &left), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_open(&right_config, &right), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_snapshot(right, &right_snapshot), SALTS_OK);
    peer = packet_source_peer(right_snapshot.bound_port);
    check_equal(turbo_flow_cnet_packet_source_session_open(left, &peer, 0u, &session), SALTS_OK);
    for (attempt = 0u; attempt < 50u; ++attempt) {
      check_equal(turbo_flow_cnet_packet_source_poll(left, 1u, &left_snapshot), SALTS_OK);
      check_equal(turbo_flow_cnet_packet_source_poll(right, 1u, &right_snapshot), SALTS_OK);
    }
    check_equal(left_snapshot.sessions_opened, 0u);
    check_equal(right_snapshot.sessions_admitted, 0u);
    check_equal(right_snapshot.sessions_opened, 0u);
    check_equal(right_snapshot.messages_received, 0u);
    check_equal(right_snapshot.queue_depth, 0u);
    check_equal(right_probe.count, 0u);

    packet_source_stop_destroy(right);
    packet_source_stop_destroy(left);
    check_equal(turbo_flow_stop(right_flow), SALTS_OK);
    check_equal(turbo_flow_stop(left_flow), SALTS_OK);
    turbo_flow_destroy(right_flow);
    turbo_flow_destroy(left_flow);
  }

  it("rejects invalid FEC transport bounds before publishing an owner") {
    cnet_packet_endpoint_config endpoint = packet_source_endpoint_config(CNET_PACKET_KCP);
    packet_source_graph_probe_t probe = {0};
    turbo_flow_t *flow = packet_source_started_flow(&probe);
    turbo_flow_cnet_packet_source_config_t config;
    turbo_flow_cnet_packet_source_t *source = NULL;

    packet_source_secure_config(&endpoint, 0x5au);
    endpoint.datagram.max_datagram_bytes = 640u;
    endpoint.datagram.receive_buffer_bytes = 640u;
    config = packet_source_config(flow, &endpoint);
    check_not_null(flow);
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("preserves malformed plain-KCP status as the terminal owner error") {
    static const unsigned char malformed[] = {0u, 1u, 2u, 3u};
    cnet_packet_endpoint_config endpoint = packet_source_endpoint_config(CNET_PACKET_KCP);
    packet_source_graph_probe_t graph_probe = {0};
    packet_source_datagram_probe_t datagram_probe = {0};
    turbo_flow_t *flow = packet_source_started_flow(&graph_probe);
    turbo_flow_cnet_packet_source_config_t config = packet_source_config(flow, &endpoint);
    turbo_flow_cnet_packet_source_t *source = NULL;
    turbo_flow_cnet_packet_source_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    cnet_datagram_config datagram_config = CNET_DATAGRAM_CONFIG_INIT;
    cnet_datagram datagram = {0};
    cnet_datagram_peer peer;
    uint64_t deadline;
    int poll_status = SALTS_OK;
    size_t events = 0u;

    check_not_null(flow);
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_snapshot(source, &snapshot), SALTS_OK);
    datagram_config.backend = packet_source_test_backend();
    datagram_config.host = "127.0.0.1";
    datagram_config.port = 0u;
    datagram_config.send_capacity = 1u;
    datagram_config.request_capacity = 2u;
    datagram_config.completion_batch_capacity = 2u;
    datagram_config.max_datagram_bytes = 256u;
    datagram_config.receive_buffer_bytes = 256u;
    datagram_config.observer.on_receive = packet_source_datagram_receive;
    datagram_config.observer.on_send = packet_source_datagram_send;
    datagram_config.observer.user = &datagram_probe;
    check_equal(cnet_datagram_init(&datagram, &datagram_config), SALTS_OK);
    peer = packet_source_peer(snapshot.bound_port);
    check_equal(cnet_datagram_send(&datagram, &peer, malformed, sizeof(malformed), 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + PACKET_SOURCE_TEST_TIMEOUT_MS;
    while (poll_status == SALTS_OK && salts_monotonic_ms() < deadline) {
      check_equal(cnet_datagram_poll(&datagram, 1u, &events), SALTS_OK);
      poll_status = turbo_flow_cnet_packet_source_poll(source, 1u, &snapshot);
    }
    check_equal(poll_status, SALTS_EPROTO);
    check_equal(snapshot.status, SALTS_EPROTO);
    check_equal(snapshot.queue_depth, 0u);
    check_equal(snapshot.messages_received, 0u);
    check_equal(graph_probe.count, 0u);
    check_equal(datagram_probe.sends, 1u);
    check_equal(datagram_probe.status, SALTS_OK);

    check_equal(cnet_datagram_stop(&datagram, PACKET_SOURCE_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_datagram_destroy(&datagram), SALTS_OK);
    packet_source_stop_destroy(source);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("enforces stop before destroy and rejects repeated or post-stop work") {
    cnet_packet_endpoint_config endpoint = packet_source_endpoint_config(CNET_PACKET_UDP);
    packet_source_graph_probe_t probe = {0};
    turbo_flow_t *flow = packet_source_started_flow(&probe);
    turbo_flow_cnet_packet_source_config_t config = packet_source_config(flow, &endpoint);
    turbo_flow_cnet_packet_source_t *source = NULL;
    turbo_flow_cnet_packet_source_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    cnet_packet_session session = {0};
    cnet_datagram_peer peer = packet_source_peer(10001u);

    check_not_null(flow);
    check_equal(turbo_flow_cnet_packet_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_destroy(source), SALTS_EBUSY);
    check_equal(turbo_flow_cnet_packet_source_request(source, 0u), SALTS_EINVAL);
    check_equal(turbo_flow_cnet_packet_source_stop(source, PACKET_SOURCE_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_cnet_packet_source_stop(source, PACKET_SOURCE_TEST_TIMEOUT_MS),
                SALTS_EALREADY);
    check_equal(turbo_flow_cnet_packet_source_request(source, 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_packet_source_poll(source, 0u, &snapshot), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_packet_source_session_open(source, &peer, 0u, &session),
                SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_packet_source_send(source, session, "x", 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_packet_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
