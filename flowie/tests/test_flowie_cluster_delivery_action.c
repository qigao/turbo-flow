#include "flowie_cluster_delivery_action_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

spec("flowie cluster durable delivery action") {
  it("rewrites one target PUBLISH and preserves the original expiry deadline") {
    static const uint8_t client_id[] = "publisher-a";
    static const uint8_t source_packet[] = {0x32u, 0x0du, 0x00u, 0x01u, 'a', 0x00u, 0x07u, 0x05u,
                                            FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
                                            0x00u, 0x00u, 0x00u, 0x0au, 'o', 'k'};
    uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    uint32_t identifiers[] = {9u};
    flowie_cluster_publish_event_view_t event = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    tstr_t source = NULL;
    tstr_t encoded = NULL;
    size_t consumed = 0u;
    uint64_t expiry_at = 0u;
    int expired = 0;
    int seen_expiry = 0;
    int seen_identifier = 0;
    edge_boot_id[0] = 1u;
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 2u, 3u, 4u, 5u,
                     1000u, tstr_v_from_cstr("edge-a"), edge_boot_id,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){source_packet, sizeof(source_packet)}, 1024u, &source),
                 TURBO_OK);
    check_int_eq(flowie_cluster_publish_event_decode(source, tstr_len(source), 1024u, &event),
                 TURBO_OK);
    check_int_eq(flowie_cluster_delivery_packet_encode(
                     &event, FLOWIE_MQTT_VERSION_5, 1u, 0u, 23u, identifiers, 1u, 1004u, 1024u,
                     &encoded, &expiry_at, &expired),
                 TURBO_OK);
    check_false(expired);
    check_uint_eq(expiry_at, 1010u);
    options.version = FLOWIE_MQTT_VERSION_5;
    options.max_packet_size = 1024u;
    check_int_eq(flowie_mqtt_packet_parse((const uint8_t *)encoded, tstr_len(encoded), &options,
                                          &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_publish_parse(&packet, &publish), FLOWIE_MQTT_PARSE_OK);
    check_uint_eq(publish.packet_id, 23u);
    check_false(publish.retain);
    check_int_eq(flowie_mqtt_property_iterator_init(&publish.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    while (flowie_mqtt_property_iterator_next(&iterator, &property) == FLOWIE_MQTT_PARSE_OK) {
      if (property.identifier == FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL) {
        check_uint_eq(property.integer, 6u);
        seen_expiry = 1;
      }
      if (property.identifier == FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER) {
        check_uint_eq(property.integer, 9u);
        seen_identifier = 1;
      }
    }
    check_true(seen_expiry);
    check_true(seen_identifier);
    tstr_freep(&encoded);
    check_int_eq(flowie_cluster_delivery_packet_encode(
                     &event, FLOWIE_MQTT_VERSION_5, 1u, 0u, 23u, identifiers, 1u, 1010u, 1024u,
                     &encoded, &expiry_at, &expired),
                 TURBO_OK);
    check_true(expired);
    check_null(encoded);
    check_uint_eq(expiry_at, 1010u);
    tstr_free(source);
  }

  it("carries one exact edge connection and sequenced MQTT action") {
    static const uint8_t packet[] = {0x30u, 0x04u, 0x00u, 0x01u, 'a', 0x00u};
    uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    flowie_cluster_delivery_action_view_t decoded = FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
    tstr_t encoded = NULL;
    edge_boot_id[0] = 1u;
    check_int_eq(flowie_cluster_delivery_action_encode(
                     tstr_v_from_cstr("edge-a"), edge_boot_id, 2u, 3u, 4u, 5u, 6u,
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){packet, sizeof(packet)}, 1024u,
                     &encoded),
                 TURBO_OK);
    check_int_eq(flowie_cluster_delivery_action_decode(encoded, tstr_len(encoded), 1024u, &decoded),
                 TURBO_OK);
    check_size_eq(decoded.edge_node_id.len, sizeof("edge-a") - 1u);
    check_mem_eq(decoded.edge_node_id.data, "edge-a", sizeof("edge-a") - 1u);
    check_mem_eq(decoded.edge_boot_id, edge_boot_id, sizeof(edge_boot_id));
    check_uint_eq(decoded.connection_id, 2u);
    check_uint_eq(decoded.connection_generation, 3u);
    check_uint_eq(decoded.session_id, 4u);
    check_uint_eq(decoded.session_generation, 5u);
    check_uint_eq(decoded.edge_action.action_sequence, 6u);
    check_size_eq(decoded.edge_action.action.packet.packet.size, sizeof(packet));
    check_mem_eq(decoded.edge_action.action.packet.packet.data, packet, sizeof(packet));
    tstr_free(encoded);
  }

  it("rejects reserved fields and missing edge incarnation") {
    static const uint8_t packet[] = {0x30u, 0x04u, 0x00u, 0x01u, 'a', 0x00u};
    uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    flowie_cluster_delivery_action_view_t decoded = FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_delivery_action_encode(
                     tstr_v_from_cstr("edge-a"), edge_boot_id, 2u, 3u, 4u, 5u, 6u,
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){packet, sizeof(packet)}, 1024u,
                     &encoded),
                 TURBO_EINVAL);
    edge_boot_id[0] = 1u;
    check_int_eq(flowie_cluster_delivery_action_encode(
                     tstr_v_from_cstr("edge-a"), edge_boot_id, 2u, 3u, 4u, 5u, 6u,
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){packet, sizeof(packet)}, 1024u,
                     &encoded),
                 TURBO_OK);
    encoded[68] = 1;
    check_int_eq(flowie_cluster_delivery_action_decode(encoded, tstr_len(encoded), 1024u, &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }
}
