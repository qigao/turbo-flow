#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowie cluster peer MQTT reply action codec") {
  it("round trips packet close and owner settlement as one typed action") {
    static const uint8_t disconnect[] = {0xe0u, 0x00u};
    static const uint8_t expected_header[FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE] = {
        'T',   'F',   'R',   'P',   0x00u, 0x01u, 0x00u, 0x14u, 0x00u, 0x00u,
        0x00u, 0x16u, 0x00u, 0x00u, 0x00u, 0x02u, 0x05u, 0x07u, 0x04u, 0x00u};
    flowie_cluster_peer_mqtt_reply_action_t decoded = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_reply_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){disconnect, sizeof(disconnect)}, 1,
                     TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 128u, &encoded),
                 TURBO_OK);
    check_size_eq(tstr_len(encoded), sizeof(expected_header) + sizeof(disconnect));
    check_mem_eq(encoded, expected_header, sizeof(expected_header));
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(encoded, tstr_len(encoded), 128u, &decoded),
                 TURBO_OK);
    check_int_eq(decoded.mqtt_version, FLOWIE_MQTT_VERSION_5);
    check_int_eq(decoded.close_after_send, 1);
    check_int_eq(decoded.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(decoded.packet.type, FLOWIE_MQTT_PACKET_DISCONNECT);
    check_mem_eq(decoded.packet.packet.data, disconnect, decoded.packet.packet.size);
    tstr_free(encoded);
  }

  it("uses a versioned payload for a successful no-op") {
    flowie_cluster_peer_mqtt_reply_action_t decoded = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_reply_encode(
                     FLOWIE_MQTT_VERSION_3_1_1, (flowie_mqtt_span_t){NULL, 0u}, 0,
                     (turbo_flow_protocol_settlement_point_t)0, 128u, &encoded),
                 TURBO_OK);
    check_size_eq(tstr_len(encoded), FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE);
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(encoded, tstr_len(encoded), 128u, &decoded),
                 TURBO_OK);
    check_int_eq(decoded.mqtt_version, FLOWIE_MQTT_VERSION_3_1_1);
    check_int_eq(decoded.packet.type, 0);
    check_int_eq(decoded.close_after_send, 0);
    check_int_eq(decoded.settlement_point, 0);
    tstr_free(encoded);
  }

  it("rejects inbound-only packets malformed flags and packet limits") {
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    flowie_cluster_peer_mqtt_reply_action_t decoded = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_reply_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){pingreq, sizeof(pingreq)}, 0,
                     (turbo_flow_protocol_settlement_point_t)0, 128u, &encoded),
                 TURBO_EPROTO);
    check_null(encoded);
    check_int_eq(flowie_cluster_peer_mqtt_reply_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){pingresp, sizeof(pingresp)}, 0,
                     TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED, 128u, &encoded),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(encoded, tstr_len(encoded), 1u, &decoded),
                 TURBO_EMSGSIZE);
    encoded[17] |= 0x80u;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(encoded, tstr_len(encoded), 128u, &decoded),
                 TURBO_EPROTO);
    encoded[17] &= 0x7fu;
    encoded[18] = 0u;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(encoded, tstr_len(encoded), 128u, &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }
}
