#include "flowmq_protocol.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowmq_protocol") {
  it("round trips binary data without a TurboFlow dependency") {
    static const char payload[] = {'a', '\0', 'b'};
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0u;

    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.message_id = UINT64_C(42);
    input.identity = tstr_v_from_cstr("publisher");
    input.topic = tstr_v_from_cstr("orders.created");
    input.payload = tstr_v_from_buf(payload, sizeof(payload));

    check_int_eq(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_int_eq(flowmq_protocol_decode_frame(encoded, FLOWMQ_PROTOCOL_HEADER_SIZE - 1u, 1024u,
                                              &output, &consumed),
                 FLOWMQ_PROTOCOL_INCOMPLETE);
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &output,
                                              &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOWMQ_PROTOCOL_FRAME_DATA);
    check_int_eq(output.pattern, FLOWMQ_PROTOCOL_PUB);
    check_size_eq(output.payload.len, sizeof(payload));
    check_mem_eq(output.payload.data, payload, sizeof(payload));
    check_size_eq(consumed, tstr_len(encoded));

    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("reassembles a fragmented v2 payload into owned storage") {
    static char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 17u];
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0u;

    for (size_t i = 0u; i < sizeof(payload); ++i) payload[i] = (char)(i % 251u);
    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = UINT64_C(0x1020304050607080);
    input.payload = tstr_v_from_buf(payload, sizeof(payload));

    check_int_eq(flowmq_protocol_encode_frame(&input, sizeof(payload), &encoded), TURBO_OK);
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), sizeof(payload), &output,
                                              &consumed),
                 TURBO_OK);
    check_not_null(output.owned_payload);
    check_size_eq(output.payload.len, sizeof(payload));
    check_mem_eq(output.payload.data, payload, sizeof(payload));

    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("rejects invalid control-frame metadata") {
    flowmq_protocol_frame_t frame;
    tstr_t encoded = NULL;

    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    frame.identity = tstr_v_from_cstr("peer");
    check_int_eq(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EPROTO);
    check_null(encoded);
  }

  it("keeps receive and heartbeat deadlines independent") {
    flowmq_protocol_heartbeat_deadlines_t heartbeat;
    uint64_t wait_deadline_ns = 0u;
    const uint64_t start_ns = UINT64_C(1000000000);

    flowmq_protocol_heartbeat_deadlines_init(&heartbeat, start_ns, 20u, 500u, 55u);
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(20000000));
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(40000000));
    check_int_eq(flowmq_protocol_heartbeat_deadlines_next(
                     &heartbeat, start_ns + UINT64_C(55000000), &wait_deadline_ns),
                 FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED);
  }
}
