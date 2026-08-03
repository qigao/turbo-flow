#include "flowie_cluster_publish_event_internal.h"

#include "flowie_cluster_peer_wire_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

spec("flowie cluster durable PUBLISH event codec") {
  it("round trips exact publisher edge and MQTT identity") {
    static const uint8_t client_id[] = "publisher-a";
    static const uint8_t publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                      0x00u, 0x07u, 0x00u, 'o',   'k'};
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    flowie_cluster_publish_event_view_t decoded = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    tstr_t encoded = NULL;
    edge_boot[0] = 0x11u;
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 3u, 4u, 5u, 6u,
                     1000u, tstr_v_from_cstr("edge-a"), edge_boot,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, &encoded),
                 TURBO_OK);
    check_int_eq(flowie_cluster_publish_event_decode(encoded, tstr_len(encoded), 1024u, &decoded),
                 TURBO_OK);
    check_int_eq(decoded.requested_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_uint_eq(decoded.connection_id, 3u);
    check_uint_eq(decoded.connection_generation, 4u);
    check_uint_eq(decoded.session_id, 5u);
    check_uint_eq(decoded.session_generation, 6u);
    check_uint_eq(decoded.accepted_at_epoch_seconds, 1000u);
    check_int_eq(decoded.edge_boot_id[0], 0x11u);
    check_size_eq(decoded.edge_node_id.len, 6u);
    check_mem_eq(decoded.edge_node_id.data, "edge-a", 6u);
    check_int_eq(decoded.publish.mqtt_version, FLOWIE_MQTT_VERSION_5);
    check_mem_eq(decoded.publish.client_id.data, client_id, sizeof(client_id) - 1u);
    check_int_eq(decoded.publish.packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
    tstr_free(encoded);
  }

  it("decodes legacy events without inventing an acceptance timestamp") {
    static const uint8_t client_id[] = "publisher-a";
    static const uint8_t publish[] = {0x30u, 0x06u, 0x00u, 0x01u,
                                      'a',   0x00u, 'o',   'k'};
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    flowie_cluster_publish_event_view_t decoded = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    tstr_t encoded = NULL;
    tstr_t legacy = NULL;
    edge_boot[0] = 0x33u;
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, 3u, 4u, 5u, 6u,
                     1000u, tstr_v_from_cstr("edge-a"), edge_boot,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, &encoded),
                 TURBO_OK);
    legacy = tstr_new_len(NULL, tstr_len(encoded) - 8u);
    check_not_null(legacy);
    memcpy(legacy, encoded, 72u);
    memcpy(legacy + 72u, encoded + 80u, tstr_len(encoded) - 80u);
    flowie_cluster_peer_wire_write_u16((uint8_t *)legacy + 4u, 1u);
    flowie_cluster_peer_wire_write_u16((uint8_t *)legacy + 6u,
                                       FLOWIE_CLUSTER_PUBLISH_EVENT_V1_HEADER_SIZE);
    flowie_cluster_peer_wire_write_u32((uint8_t *)legacy + 8u, (uint32_t)tstr_len(legacy));
    check_int_eq(flowie_cluster_publish_event_decode(legacy, tstr_len(legacy), 1024u, &decoded),
                 TURBO_OK);
    check_uint_eq(decoded.accepted_at_epoch_seconds, 0u);
    check_mem_eq(decoded.publish.client_id.data, client_id, sizeof(client_id) - 1u);
    tstr_free(legacy);
    tstr_free(encoded);
  }

  it("rejects invalid QoS zero settlement and reserved bytes") {
    static const uint8_t client_id[] = "publisher-a";
    static const uint8_t publish[] = {0x30u, 0x06u, 0x00u, 0x01u,
                                      'a',   0x00u, 'o',   'k'};
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    flowie_cluster_publish_event_view_t decoded = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    tstr_t encoded = NULL;
    edge_boot[0] = 0x22u;
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, 3u, 4u, 5u, 6u,
                     0u, tstr_v_from_cstr("edge-a"), edge_boot,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, &encoded),
                 TURBO_EINVAL);
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 3u, 4u, 5u, 6u,
                     1000u, tstr_v_from_cstr("edge-a"), edge_boot,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, &encoded),
                 TURBO_EPROTO);
    check_null(encoded);
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, 3u, 4u, 5u, 6u,
                     1000u, tstr_v_from_cstr("edge-a"), edge_boot,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, &encoded),
                 TURBO_OK);
    encoded[17] = 1;
    check_int_eq(flowie_cluster_publish_event_decode(encoded, tstr_len(encoded), 1024u, &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }
}
