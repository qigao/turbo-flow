#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowie cluster peer TAKEOVER_CLOSE codec") {
  it("round trips the exact old socket client identity") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t expected_header[FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_HEADER_SIZE] = {
        'T',   'F',   'T',   'C',   0x00u, 0x01u, 0x00u, 0x10u,
        0x00u, 0x00u, 0x00u, 0x18u, 0x00u, 0x08u, 0x05u, 0x00u};
    flowie_cluster_peer_takeover_close_view_t decoded =
        FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_VIEW_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_takeover_close_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     128u, &encoded),
                 TURBO_OK);
    check_size_eq(tstr_len(encoded), sizeof(expected_header) + sizeof(client_id) - 1u);
    check_mem_eq(encoded, expected_header, sizeof(expected_header));
    check_int_eq(
        flowie_cluster_peer_takeover_close_decode(encoded, tstr_len(encoded), 128u, &decoded),
        TURBO_OK);
    check_int_eq(decoded.mqtt_version, FLOWIE_MQTT_VERSION_5);
    check_mem_eq(decoded.client_id.data, client_id, decoded.client_id.size);
    tstr_free(encoded);
  }

  it("rejects malformed identity reserved bytes and payload limits") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t invalid_utf8[] = {0xc0u, 0x80u};
    flowie_cluster_peer_takeover_close_view_t decoded =
        FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_VIEW_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_takeover_close_encode(
                     FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){invalid_utf8, sizeof(invalid_utf8)}, 128u, &encoded),
                 TURBO_EPROTO);
    check_null(encoded);
    check_int_eq(flowie_cluster_peer_takeover_close_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_HEADER_SIZE, &encoded),
                 TURBO_EMSGSIZE);
    check_null(encoded);
    check_int_eq(flowie_cluster_peer_takeover_close_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     128u, &encoded),
                 TURBO_OK);
    encoded[15] = 1u;
    check_int_eq(
        flowie_cluster_peer_takeover_close_decode(encoded, tstr_len(encoded), 128u, &decoded),
        TURBO_EPROTO);
    encoded[15] = 0u;
    check_int_eq(flowie_cluster_peer_takeover_close_decode(encoded, tstr_len(encoded),
                                                           tstr_len(encoded) - 1u, &decoded),
                 TURBO_EMSGSIZE);
    tstr_free(encoded);
  }
}
