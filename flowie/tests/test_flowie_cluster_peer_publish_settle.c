#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static turbo_flow_protocol_settlement_request_t flowie_publish_settlement(void) {
  turbo_flow_protocol_settlement_request_t settlement =
      TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
  settlement.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  settlement.message.protocol_version = FLOWIE_MQTT_VERSION_5;
  settlement.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
  settlement.message.qos = 1u;
  settlement.message.packet_id = 42u;
  settlement.message.session_generation = 19u;
  settlement.message.duplicate = 1u;
  settlement.message.retain = 1u;
  settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
  settlement.status = TURBO_OK;
  settlement.message_id = 71u;
  settlement.attempt = 2u;
  return settlement;
}

spec("flowie cluster peer PUBLISH_SETTLE codec") {
  it("round trips a graph settlement without serializing process-local capabilities") {
    static const uint8_t client_id[] = "device/settle";
    flowie_cluster_peer_publish_settle_view_t decoded =
        FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VIEW_INIT;
    turbo_flow_protocol_settlement_request_t settlement = flowie_publish_settlement();
    tstr_t encoded = NULL;

    check_int_eq(flowie_cluster_peer_publish_settle_encode(
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &settlement, 256u,
                     &encoded),
                 TURBO_OK);
    check_size_eq(tstr_len(encoded),
                  FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_HEADER_SIZE + sizeof(client_id) - 1u);
    check_mem_eq(encoded, "TFPS", 4u);
    check_int_eq(flowie_cluster_peer_publish_settle_decode(encoded, tstr_len(encoded), 256u,
                                                           &decoded),
                 TURBO_OK);
    check_mem_eq(decoded.client_id.data, client_id, decoded.client_id.size);
    check_int_eq(decoded.settlement.message.protocol, TURBO_FLOW_PROTOCOL_MQTT);
    check_uint_eq(decoded.settlement.message.protocol_version, FLOWIE_MQTT_VERSION_5);
    check_uint_eq(decoded.settlement.message.qos, 1u);
    check_uint_eq(decoded.settlement.message.packet_id, 42u);
    check_uint_eq(decoded.settlement.message.session_generation, 19u);
    check_true(decoded.settlement.message.duplicate);
    check_true(decoded.settlement.message.retain);
    check_int_eq(decoded.settlement.point, TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED);
    check_uint_eq(decoded.settlement.message_id, 71u);
    check_uint_eq(decoded.settlement.attempt, 2u);
    tstr_free(encoded);
  }

  it("rejects non-graph boundaries malformed flags and stale payload limits") {
    static const uint8_t client_id[] = "device-a";
    flowie_cluster_peer_publish_settle_view_t decoded =
        FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VIEW_INIT;
    turbo_flow_protocol_settlement_request_t settlement = flowie_publish_settlement();
    tstr_t encoded = NULL;

    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED;
    check_int_eq(flowie_cluster_peer_publish_settle_encode(
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &settlement, 256u,
                     &encoded),
                 TURBO_EPROTO);
    settlement = flowie_publish_settlement();
    settlement.attempt = 0u;
    check_int_eq(flowie_cluster_peer_publish_settle_encode(
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &settlement, 256u,
                     &encoded),
                 TURBO_EPROTO);
    settlement = flowie_publish_settlement();
    check_int_eq(flowie_cluster_peer_publish_settle_encode(
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &settlement, 256u,
                     &encoded),
                 TURBO_OK);
    encoded[59] = (char)0x80u;
    check_int_eq(flowie_cluster_peer_publish_settle_decode(encoded, tstr_len(encoded), 256u,
                                                           &decoded),
                 TURBO_EPROTO);
    encoded[59] = 0;
    check_int_eq(flowie_cluster_peer_publish_settle_decode(
                     encoded, tstr_len(encoded), tstr_len(encoded) - 1u, &decoded),
                 TURBO_EMSGSIZE);
    tstr_free(encoded);
  }
}
