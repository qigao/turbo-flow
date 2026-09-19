#include "protocol_network_e2e_fixture.h"
#include "tinytest.h"

#include <salts/clock.h>

#include <stdio.h>
#include <string.h>

#ifndef FLOW_CNET_PLUGIN_MODULE
  #error FLOW_CNET_PLUGIN_MODULE is required
#endif
#ifndef FLOW_PROTOCOL_JTT808_MODULE
  #error FLOW_PROTOCOL_JTT808_MODULE is required
#endif
#if defined(FLOW_E2E_DURABLE_TURBODB)
  #ifndef FLOW_TURBODB_PLUGIN_MODULE
    #error FLOW_TURBODB_PLUGIN_MODULE is required
  #endif
  #define FLOW_E2E_DURABLE_PLUGIN_MODULE FLOW_TURBODB_PLUGIN_MODULE
  #define FLOW_E2E_STORAGE_KIND PROTOCOL_NETWORK_E2E_STORAGE_TURBODB
#else
  #ifndef FLOW_DURABLE_MEMORY_PLUGIN_MODULE
    #error FLOW_DURABLE_MEMORY_PLUGIN_MODULE is required
  #endif
  #define FLOW_E2E_DURABLE_PLUGIN_MODULE FLOW_DURABLE_MEMORY_PLUGIN_MODULE
  #define FLOW_E2E_STORAGE_KIND PROTOCOL_NETWORK_E2E_STORAGE_MEMORY
#endif

enum { PROTOCOL_NETWORK_E2E_TIMEOUT_MS = 5000 };

static void protocol_network_wait_first_fragment(protocol_network_e2e_fixture_t *fixture,
                                                 size_t sent_before,
                                                 turbo_flow_protocol_network_intake_snapshot_t *snap) {
  uint64_t deadline = salts_monotonic_ms() + PROTOCOL_NETWORK_E2E_TIMEOUT_MS;
  while ((fixture->tcp.sent == sent_before || snap->active_sessions == 0u) &&
         salts_monotonic_ms() < deadline) {
    check_equal(protocol_network_e2e_poll(fixture, 1u, snap), SALTS_OK);
  }
  check_true(fixture->tcp.sent > sent_before);
  check_equal(snap->active_sessions, 1u);
}

static void protocol_network_wait_admitted_frame(
    protocol_network_e2e_fixture_t *fixture,
    turbo_flow_protocol_network_intake_snapshot_t *snap) {
  uint64_t deadline = salts_monotonic_ms() + PROTOCOL_NETWORK_E2E_TIMEOUT_MS;
  while (snap->frames_admitted == 0u && salts_monotonic_ms() < deadline)
    check_equal(protocol_network_e2e_poll(fixture, 1u, snap), SALTS_OK);
  check_equal(snap->frames_admitted, (uint64_t)1u);
}

spec("real JT/T808 TCP protocol intake") {
  it("stores a fragmented real TCP frame before any business Graph or real UDP Sink execution") {
    protocol_network_e2e_fixture_t fixture;
    turbo_flow_protocol_network_intake_snapshot_t intake =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    uint8_t frame[64];
    size_t frame_size;
    size_t split;
    size_t sent_before;
    unsigned port = 0u;

    check_equal(protocol_network_e2e_jtt808_init(&fixture, FLOW_CNET_PLUGIN_MODULE,
                                                 FLOW_PROTOCOL_JTT808_MODULE,
                                                 FLOW_E2E_DURABLE_PLUGIN_MODULE,
                                                 FLOW_E2E_STORAGE_KIND),
                SALTS_OK);
    check_equal(protocol_network_e2e_start(&fixture, &intake), SALTS_OK);
    check_equal(intake.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING);
    check_equal(sscanf(intake.source_endpoint, "tcp://127.0.0.1:%u", &port), 1);
    check_true(port > 0u && port <= UINT16_MAX);
    check_equal(protocol_network_e2e_tcp_connect(&fixture, intake.source_endpoint,
                                                 PROTOCOL_NETWORK_E2E_TIMEOUT_MS),
                SALTS_OK);

    frame_size = protocol_network_e2e_jtt808_frame(frame, sizeof(frame));
    check_true(frame_size > 4u);
    split = frame_size / 2u;
    check_true(split > 0u && split < frame_size);

    sent_before = fixture.tcp.sent;
    check_equal(protocol_network_e2e_tcp_send(&fixture, frame, split), SALTS_OK);
    protocol_network_wait_first_fragment(&fixture, sent_before, &intake);
    check_equal(intake.frames_admitted, (uint64_t)0u);
    check_equal(fixture.business.stage_completions, 0u);
    check_equal(fixture.udp.received, 0u);

    check_equal(protocol_network_e2e_tcp_send(&fixture, frame + split, frame_size - split), SALTS_OK);
    protocol_network_wait_admitted_frame(&fixture, &intake);
    check_equal(fixture.business.stage_completions, 0u);
    check_equal(fixture.udp.received, 0u);

    check_equal(protocol_network_e2e_business_request_and_drive(
                    &fixture, PROTOCOL_NETWORK_E2E_TIMEOUT_MS),
                SALTS_OK);
    check_equal(fixture.business.stage_completions, 1u);
    check_equal(fixture.business.failed_completions, 0u);
    check_equal(fixture.udp.received, 1u);
    check_true(fixture.udp.last_payload_size > 0u);
    check_equal(intake.frames_admitted, (uint64_t)1u);

    protocol_network_e2e_destroy(&fixture);
  }

  it("drops a closed TCP generation partial parser before admitting a complete frame on reuse") {
    protocol_network_e2e_fixture_t fixture;
    turbo_flow_protocol_network_intake_snapshot_t intake =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    uint8_t frame[64];
    size_t frame_size;
    size_t split;
    size_t sent_before;

    check_equal(protocol_network_e2e_jtt808_init(&fixture, FLOW_CNET_PLUGIN_MODULE,
                                                 FLOW_PROTOCOL_JTT808_MODULE,
                                                 FLOW_E2E_DURABLE_PLUGIN_MODULE,
                                                 FLOW_E2E_STORAGE_KIND),
                SALTS_OK);
    check_equal(protocol_network_e2e_start(&fixture, &intake), SALTS_OK);
    check_equal(protocol_network_e2e_tcp_connect(&fixture, intake.source_endpoint,
                                                 PROTOCOL_NETWORK_E2E_TIMEOUT_MS),
                SALTS_OK);
    frame_size = protocol_network_e2e_jtt808_frame(frame, sizeof(frame));
    check_true(frame_size > 4u);
    split = frame_size / 2u;

    sent_before = fixture.tcp.sent;
    check_equal(protocol_network_e2e_tcp_send(&fixture, frame, split), SALTS_OK);
    protocol_network_wait_first_fragment(&fixture, sent_before, &intake);
    check_equal(intake.frames_admitted, (uint64_t)0u);

    check_equal(protocol_network_e2e_tcp_close(&fixture, PROTOCOL_NETWORK_E2E_TIMEOUT_MS), SALTS_OK);
    check_equal(protocol_network_e2e_tcp_connect(&fixture, intake.source_endpoint,
                                                 PROTOCOL_NETWORK_E2E_TIMEOUT_MS),
                SALTS_OK);
    sent_before = fixture.tcp.sent;
    check_equal(protocol_network_e2e_tcp_send(&fixture, frame, frame_size), SALTS_OK);
    protocol_network_wait_admitted_frame(&fixture, &intake);
    check_true(fixture.tcp.sent > sent_before);
    check_equal(intake.frames_admitted, (uint64_t)1u);
    check_equal(fixture.business.stage_completions, 0u);
    check_equal(fixture.udp.received, 0u);

    check_equal(protocol_network_e2e_business_request_and_drive(
                    &fixture, PROTOCOL_NETWORK_E2E_TIMEOUT_MS),
                SALTS_OK);
    check_equal(intake.frames_admitted, (uint64_t)1u);

    protocol_network_e2e_destroy(&fixture);
  }
}
