#include "tinytest.h"
#include "turbo_flow_cnet.h"
#include "turbo_flow_cnet_test_internal.h"

#include <salts/clock.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <string.h>

enum { DATAGRAM_SINK_TEST_TIMEOUT_MS = 5000 };

typedef struct datagram_sink_receiver_s {
  size_t calls;
  size_t size;
  unsigned char data[64];
} datagram_sink_receiver_t;

typedef struct datagram_sink_completion_s {
  atomic_size_t calls;
  atomic_int status;
} datagram_sink_completion_t;

typedef struct datagram_sink_snapshot_race_s {
  turbo_flow_cnet_datagram_sink_t *sink;
  atomic_bool ready;
  atomic_bool done;
  atomic_size_t snapshots;
  atomic_int status;
} datagram_sink_snapshot_race_t;

static void datagram_sink_snapshot_race(void *ctx) {
  datagram_sink_snapshot_race_t *race = (datagram_sink_snapshot_race_t *)ctx;
  while (!atomic_load_explicit(&race->done, memory_order_acquire)) {
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    int status = turbo_flow_cnet_datagram_sink_snapshot(race->sink, &snapshot);
    if (status != SALTS_OK || snapshot.active_requests > 2u ||
        (snapshot.state == TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED && snapshot.bound_port != 0u)) {
      atomic_store_explicit(&race->status, status != SALTS_OK ? status : SALTS_EPROTO,
                            memory_order_release);
      break;
    }
    (void)atomic_fetch_add_explicit(&race->snapshots, 1u, memory_order_relaxed);
    atomic_store_explicit(&race->ready, true, memory_order_release);
    cmeta_thread_yield();
  }
}

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
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
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
  static datagram_sink_snapshot_race_t race;
  static cmeta_thread_t reader = NULL;
  after_each() {
    if (reader) {
      atomic_store_explicit(&race.done, true, memory_order_release);
      check_equal(cmeta_thread_join(&reader), SALTS_OK);
    }
  }

  it("retains native owners after stop or destroy fails and retries only explicitly") {
    const int statuses[] = {SALTS_EIO, SALTS_ETIMEDOUT, SALTS_EBUSY};
    for (size_t index = 0u; index < sizeof(statuses) / sizeof(statuses[0]); ++index) {
      cnet_datagram_config native = datagram_sink_cnet_config(2u);
      turbo_flow_cnet_datagram_sink_t *sink = NULL;
      turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
      turbo_flow_t *flow = datagram_sink_flow(&native, datagram_sink_peer(9u), &sink);
      check_not_null(flow);
      if (index < 2u)
        check_equal(turbo_flow_cnet_test_datagram_sink_fail_next_stop(sink, statuses[index], false),
                    SALTS_OK);
      else
        check_equal(turbo_flow_cnet_test_datagram_sink_fail_next_destroy(sink, statuses[index]),
                    SALTS_OK);
      check_equal(turbo_flow_stop(flow), statuses[index]);
      check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED);
      check_equal(snapshot.status, statuses[index]);
      check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_EBUSY);
      /* Fault admission requires the original native owner to remain initialized. */
      check_equal(turbo_flow_cnet_test_datagram_sink_fail_next_destroy(sink, SALTS_EBUSY), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_stop(flow), SALTS_EBUSY);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
    }
  }

  it("settles two native-pending claims once while failed stops retain their Actor tokens") {
    cnet_datagram_config native = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    datagram_sink_completion_t completion[2];
    turbo_flow_t *flow = datagram_sink_flow(&native, datagram_sink_peer(9u), &sink);
    uint64_t deadline;
    check_not_null(flow);
    for (size_t index = 0u; index < 2u; ++index) {
      turbo_flow_msg_t message;
      atomic_init(&completion[index].calls, 0u);
      atomic_init(&completion[index].status, SALTS_EALREADY);
      datagram_sink_message(&message, index == 0u ? "first" : "second");
      check_equal(turbo_flow_publish_async(flow, "input", &message, datagram_sink_complete,
                                           &completion[index]), SALTS_OK);
      turbo_flow_msg_cleanup(&message);
    }
    deadline = cmeta_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
      if (snapshot.active_requests < 2u) cmeta_thread_yield();
    } while (snapshot.active_requests < 2u && cmeta_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)2u);
    /* Drive real native send admission without observing its terminal batch. */
    check_equal(turbo_flow_cnet_test_datagram_sink_submit_ready(sink), SALTS_OK);
    for (size_t index = 0u; index < 2u; ++index)
      check_equal(atomic_load_explicit(&completion[index].calls, memory_order_acquire), (size_t)0u);

    race.sink = sink;
    atomic_init(&race.ready, false);
    atomic_init(&race.done, false);
    atomic_init(&race.snapshots, 0u);
    atomic_init(&race.status, SALTS_OK);
    check_equal(cmeta_thread_create(&reader, datagram_sink_snapshot_race, &race), SALTS_OK);
    deadline = cmeta_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    while (!atomic_load_explicit(&race.ready, memory_order_acquire) &&
           cmeta_monotonic_ms() < deadline) cmeta_thread_yield();
    check_true(atomic_load_explicit(&race.ready, memory_order_acquire));
    check_equal(turbo_flow_cnet_test_datagram_sink_fail_next_stop(sink, SALTS_ETIMEDOUT, false),
                SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_ETIMEDOUT);
    for (size_t index = 0u; index < 2u; ++index) {
      check_equal(atomic_load_explicit(&completion[index].calls, memory_order_acquire), (size_t)1u);
      check_equal(atomic_load_explicit(&completion[index].status, memory_order_acquire),
                  SALTS_ESHUTDOWN);
    }
    check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED);
    check_equal(snapshot.active_requests, (size_t)2u);
    check_equal(turbo_flow_cnet_test_datagram_sink_fail_next_stop(sink, SALTS_EIO, false), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_EBUSY);
    check_equal(turbo_flow_stop(flow), SALTS_EIO);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    atomic_store_explicit(&race.done, true, memory_order_release);
    check_equal(cmeta_thread_join(&reader), SALTS_OK);
    check_equal(atomic_load_explicit(&race.status, memory_order_acquire), SALTS_OK);
    check_greater(atomic_load_explicit(&race.snapshots, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_DATAGRAM_SINK_STOPPED);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.messages_sent, (uint64_t)0u);
    for (size_t index = 0u; index < 2u; ++index) {
      check_equal(atomic_load_explicit(&completion[index].calls, memory_order_acquire), (size_t)1u);
      check_equal(atomic_load_explicit(&completion[index].status, memory_order_acquire),
                  SALTS_ESHUTDOWN);
    }
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }

  it("reports a native drain error and permits explicit cleanup retry") {
    cnet_datagram_config native = datagram_sink_cnet_config(2u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    turbo_flow_t *flow = datagram_sink_flow(&native, datagram_sink_peer(9u), &sink);
    check_not_null(flow);
    check_equal(turbo_flow_cnet_test_datagram_sink_fail_next_stop(sink, SALTS_EIO, true), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_EIO);
    check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_DATAGRAM_SINK_FAILED);
    check_equal(snapshot.status, SALTS_EIO);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_EBUSY);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
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
    datagram_sink_message(&message, payload);

    check_equal(
        turbo_flow_publish_async(flow, "input", &message, datagram_sink_complete, &completion),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    deadline = cmeta_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&completion.calls, memory_order_acquire) == 0u &&
           cmeta_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_datagram_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(snapshot.messages_sent, (uint64_t)1u);
    check_equal(snapshot.bytes_sent, (uint64_t)(sizeof(payload) - 1u));
    check_equal(cnet_datagram_poll(&receiving_datagram, DATAGRAM_SINK_TEST_TIMEOUT_MS, &events),
                SALTS_OK);
    check_equal(receiver.calls, (size_t)1u);
    check_equal(receiver.size, sizeof(payload) - 1u);
    check_equal(receiver.data, payload, sizeof(payload) - 1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
    check_equal(cnet_datagram_stop(&receiving_datagram, DATAGRAM_SINK_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_datagram_destroy(&receiving_datagram), SALTS_OK);
  }

  it("rejects beyond bounded Actor capacity and cancels the admitted claim on stop") {
    cnet_datagram_config sink_datagram = datagram_sink_cnet_config(1u);
    turbo_flow_cnet_datagram_sink_t *sink = NULL;
    turbo_flow_cnet_datagram_sink_snapshot_t snapshot = TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
    datagram_sink_completion_t first;
    datagram_sink_completion_t second;
    turbo_flow_msg_t first_message;
    turbo_flow_msg_t second_message;
    turbo_flow_t *flow = datagram_sink_flow(&sink_datagram, datagram_sink_peer(9u), &sink);
    uint64_t deadline;

    check_not_null(flow);
    atomic_init(&first.calls, 0u);
    atomic_init(&first.status, SALTS_EALREADY);
    atomic_init(&second.calls, 0u);
    atomic_init(&second.status, SALTS_EALREADY);
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

    deadline = cmeta_monotonic_ms() + DATAGRAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
      if (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u) cmeta_sleep_ms(1u);
    } while (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u &&
             cmeta_monotonic_ms() < deadline);
    check_equal(turbo_flow_cnet_datagram_sink_snapshot(sink, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_ENOSPC);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_datagram_sink_destroy(sink), SALTS_OK);
  }
}
