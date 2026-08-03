#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

spec("flowie cluster peer edge action codec") {
  it("round trips one sequenced PUBLISH socket action and its acknowledgement") {
    static const uint8_t publish[] = {0x30u, 0x06u, 0x00u, 0x01u,
                                      'a',   0x00u, 'o',   'k'};
    flowie_cluster_peer_edge_action_t decoded = FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT;
    tstr_t action = NULL;
    tstr_t ack = NULL;
    uint64_t acknowledged = 0u;
    check_int_eq(flowie_cluster_peer_edge_action_encode(
                     7u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 0,
                     TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 256u, &action),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_edge_action_decode(action, tstr_len(action), 256u, &decoded),
                 TURBO_OK);
    check_uint_eq(decoded.action_sequence, 7u);
    check_int_eq(decoded.action.mqtt_version, FLOWIE_MQTT_VERSION_5);
    check_int_eq(decoded.action.packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
    check_int_eq(decoded.action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(flowie_cluster_peer_edge_action_ack_encode(7u, &ack), TURBO_OK);
    check_int_eq(flowie_cluster_peer_edge_action_ack_decode(ack, tstr_len(ack), &acknowledged),
                 TURBO_OK);
    check_uint_eq(acknowledged, 7u);
    tstr_free(ack);
    tstr_free(action);
  }

  it("rejects no-op actions, zero sequences and malformed acknowledgements") {
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    tstr_t encoded = NULL;
    uint64_t sequence = 1u;
    check_int_eq(flowie_cluster_peer_edge_action_encode(
                     0u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){pingreq, sizeof(pingreq)}, 0,
                     (turbo_flow_protocol_settlement_point_t)0, 128u, &encoded),
                 TURBO_EINVAL);
    check_int_eq(flowie_cluster_peer_edge_action_encode(
                     1u, FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){NULL, 0u}, 0,
                     (turbo_flow_protocol_settlement_point_t)0, 128u, &encoded),
                 TURBO_EINVAL);
    check_int_eq(flowie_cluster_peer_edge_action_ack_encode(1u, &encoded), TURBO_OK);
    encoded[12] = 1;
    check_int_eq(flowie_cluster_peer_edge_action_ack_decode(encoded, tstr_len(encoded), &sequence),
                 TURBO_EPROTO);
    check_uint_eq(sequence, 0u);
    tstr_free(encoded);
  }
}
