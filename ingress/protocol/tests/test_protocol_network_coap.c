#include "protocol_network_e2e_fixture.h"
#include "tinytest.h"

#include <salts/clock.h>

#include <stdio.h>

#ifndef FLOW_CNET_PLUGIN_MODULE
  #error FLOW_CNET_PLUGIN_MODULE is required
#endif
#ifndef FLOW_PROTOCOL_COAP_MODULE
  #error FLOW_PROTOCOL_COAP_MODULE is required
#endif
#ifndef FLOW_DURABLE_MEMORY_PLUGIN_MODULE
  #error FLOW_DURABLE_MEMORY_PLUGIN_MODULE is required
#endif

enum { PROTOCOL_NETWORK_COAP_TIMEOUT_MS = 5000 };

static void protocol_network_coap_wait_admitted(
    protocol_network_e2e_fixture_t *fixture,
    turbo_flow_protocol_network_intake_snapshot_t *intake) {
  uint64_t deadline = salts_monotonic_ms() + PROTOCOL_NETWORK_COAP_TIMEOUT_MS;
  while (intake->frames_admitted == 0u && salts_monotonic_ms() < deadline)
    check_equal(protocol_network_e2e_poll(fixture, 1u, intake), SALTS_OK);
  check_equal(intake->frames_admitted, (uint64_t)1u);
}

static void protocol_network_coap_admit(protocol_network_e2e_fixture_t *fixture,
                                        const uint8_t frame[4]) {
  turbo_flow_protocol_network_intake_snapshot_t intake =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  unsigned port = 0u;
  const size_t sent_before = fixture->udp.sent;

  check_equal(protocol_network_e2e_start(fixture, &intake), SALTS_OK);
  check_equal(intake.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING);
  check_equal(sscanf(intake.source_endpoint, "udp://127.0.0.1:%u", &port), 1);
  check_true(port > 0u && port <= UINT16_MAX);
  check_equal(protocol_network_e2e_udp_send_frame(fixture, intake.source_endpoint, frame, 4u),
              SALTS_OK);
  protocol_network_coap_wait_admitted(fixture, &intake);
  check_true(fixture->udp.sent > sent_before);
  check_equal(fixture->udp.last_send_status, SALTS_OK);
  check_equal(fixture->business.stage_completions, 0u);
  check_equal(fixture->udp.received, 0u);
}

static void protocol_network_coap_deliver_business(protocol_network_e2e_fixture_t *fixture) {
  check_equal(protocol_network_e2e_business_request_and_drive(
                  fixture, PROTOCOL_NETWORK_COAP_TIMEOUT_MS),
              SALTS_OK);
  check_equal(fixture->business.stage_completions, 1u);
  check_equal(fixture->business.failed_completions, 0u);
  check_equal(fixture->udp.received, 1u);
  check_true(fixture->udp.last_payload_size > 0u);
}

spec("real CoAP UDP protocol intake") {
  it("admits a real UDP packet before business execution and sends only after explicit durable progress") {
    static const uint8_t coap_get[] = {0x40u, 0x01u, 0x12u, 0x34u};
    protocol_network_e2e_fixture_t fixture;
    check_equal(protocol_network_e2e_coap_init(&fixture, FLOW_CNET_PLUGIN_MODULE,
                                               FLOW_PROTOCOL_COAP_MODULE,
                                               FLOW_DURABLE_MEMORY_PLUGIN_MODULE),
                SALTS_OK);
    protocol_network_coap_admit(&fixture, coap_get);
    check_equal(fixture.business.stage_completions, (size_t)0u);
    protocol_network_coap_deliver_business(&fixture);

    protocol_network_e2e_destroy(&fixture);
  }

  it("keeps the same Sink provider type independent across two explicit destination peers") {
    static const uint8_t coap_a[] = {0x40u, 0x01u, 0x00u, 0x11u};
    static const uint8_t coap_b[] = {0x40u, 0x01u, 0x00u, 0x22u};
    protocol_network_e2e_fixture_t first;
    protocol_network_e2e_fixture_t second;

    check_equal(protocol_network_e2e_coap_init(&first, FLOW_CNET_PLUGIN_MODULE,
                                               FLOW_PROTOCOL_COAP_MODULE,
                                               FLOW_DURABLE_MEMORY_PLUGIN_MODULE),
                SALTS_OK);
    check_equal(protocol_network_e2e_coap_init(&second, FLOW_CNET_PLUGIN_MODULE,
                                               FLOW_PROTOCOL_COAP_MODULE,
                                               FLOW_DURABLE_MEMORY_PLUGIN_MODULE),
                SALTS_OK);
    check_true(first.receiver_port != 0u);
    check_true(second.receiver_port != 0u);
    check_not_equal(first.receiver_port, second.receiver_port);

    protocol_network_coap_admit(&first, coap_a);
    check_equal(second.udp.received, 0u);
    protocol_network_coap_deliver_business(&first);
    check_equal(first.udp.received, 1u);
    check_equal(second.udp.received, 0u);

    protocol_network_coap_admit(&second, coap_b);
    check_equal(first.udp.received, 1u);
    protocol_network_coap_deliver_business(&second);
    check_equal(first.udp.received, 1u);
    check_equal(second.udp.received, 1u);

    protocol_network_e2e_destroy(&second);
    protocol_network_e2e_destroy(&first);
  }
}
