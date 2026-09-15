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

static void protocol_network_wait_pending_record(protocol_network_e2e_fixture_t *fixture,
                                                 turbo_flow_protocol_network_intake_snapshot_t *snap,
                                                 turbo_flow_inbox_snapshot_t *inbox) {
  uint64_t deadline = salts_monotonic_ms() + PROTOCOL_NETWORK_E2E_TIMEOUT_MS;
  while (inbox->pending_records == 0u && salts_monotonic_ms() < deadline) {
    check_equal(protocol_network_e2e_poll(fixture, 1u, snap), SALTS_OK);
    *inbox = (turbo_flow_inbox_snapshot_t)TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    check_equal(turbo_flow_inbox_snapshot(&fixture->inbox, inbox), SALTS_OK);
  }
  check_equal(inbox->pending_records, 1u);
}

spec("real JT/T808 TCP protocol intake") {
  it("stores a fragmented real TCP frame before any business Graph or real UDP Sink execution") {
    protocol_network_e2e_fixture_t fixture;
    turbo_flow_protocol_network_intake_snapshot_t intake =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_inbox_snapshot_t inbox = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t business = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    uint8_t frame[64];
    size_t frame_size;
    size_t split;
    size_t sent_before;
    unsigned port = 0u;

    check_equal(protocol_network_e2e_jtt808_init(&fixture, FLOW_CNET_PLUGIN_MODULE,
                                                 FLOW_PROTOCOL_JTT808_MODULE),
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
    check_equal(turbo_flow_inbox_snapshot(&fixture.inbox, &inbox), SALTS_OK);
    check_equal(inbox.pending_records, 0u);
    check_equal(fixture.business.stage_completions, 0u);
    check_equal(fixture.udp.received, 0u);

    check_equal(protocol_network_e2e_tcp_send(&fixture, frame + split, frame_size - split), SALTS_OK);
    protocol_network_wait_pending_record(&fixture, &intake, &inbox);
    check_equal(fixture.business.stage_completions, 0u);
    check_equal(fixture.udp.received, 0u);

    check_equal(protocol_network_e2e_business_request_and_drive(
                    &fixture, PROTOCOL_NETWORK_E2E_TIMEOUT_MS, &business),
                SALTS_OK);
    check_equal(business.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(business.graph_status, SALTS_OK);
    check_equal(business.settlement_status, SALTS_OK);
    check_equal(fixture.business.stage_completions, 1u);
    check_equal(fixture.business.failed_completions, 0u);
    check_equal(fixture.udp.received, 1u);
    check_true(fixture.udp.last_payload_size > 0u);
    check_equal(turbo_flow_inbox_snapshot(&fixture.inbox, &inbox), SALTS_OK);
    check_equal(inbox.pending_records, 0u);
    check_equal(inbox.completed, (uint64_t)1u);

    protocol_network_e2e_destroy(&fixture);
  }

  it("drops a closed TCP generation partial parser before admitting a complete frame on reuse") {
    protocol_network_e2e_fixture_t fixture;
    turbo_flow_protocol_network_intake_snapshot_t intake =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_inbox_snapshot_t inbox = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    uint8_t frame[64];
    size_t frame_size;
    size_t split;
    size_t sent_before;

    check_equal(protocol_network_e2e_jtt808_init(&fixture, FLOW_CNET_PLUGIN_MODULE,
                                                 FLOW_PROTOCOL_JTT808_MODULE),
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
    check_equal(turbo_flow_inbox_snapshot(&fixture.inbox, &inbox), SALTS_OK);
    check_equal(inbox.pending_records, 0u);

    check_equal(protocol_network_e2e_tcp_close(&fixture, PROTOCOL_NETWORK_E2E_TIMEOUT_MS), SALTS_OK);
    check_equal(protocol_network_e2e_tcp_connect(&fixture, intake.source_endpoint,
                                                 PROTOCOL_NETWORK_E2E_TIMEOUT_MS),
                SALTS_OK);
    sent_before = fixture.tcp.sent;
    check_equal(protocol_network_e2e_tcp_send(&fixture, frame, frame_size), SALTS_OK);
    protocol_network_wait_pending_record(&fixture, &intake, &inbox);
    check_true(fixture.tcp.sent > sent_before);
    check_equal(inbox.admitted, (uint64_t)1u);
    check_equal(inbox.pending_records, 1u);
    check_equal(fixture.business.stage_completions, 0u);
    check_equal(fixture.udp.received, 0u);

    protocol_network_e2e_destroy(&fixture);
  }
}
