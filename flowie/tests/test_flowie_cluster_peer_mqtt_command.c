#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH[] = {0x30u, 0x05u, 0x00u, 0x01u,
                                                                'a',   0x00u, 'x'};

spec("flowie cluster peer MQTT command codec") {
  it("round trips a versioned binary-safe packet command") {
    static const uint8_t client_id[] = {'c', 'l', 'i', 'e', 'n', 't', '-', 'a'};
    static const uint8_t expected_header[FLOWIE_CLUSTER_PEER_MQTT_COMMAND_HEADER_SIZE] = {
        'T',   'F',   'M',   'Q',   0x00u, 0x01u, 0x00u, 0x14u, 0x00u, 0x00u,
        0x00u, 0x23u, 0x00u, 0x00u, 0x00u, 0x07u, 0x00u, 0x08u, 0x05u, 0x00u};
    flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH)},
                     128u, &encoded),
                 TURBO_OK);
    check_not_null(encoded);
    check_size_eq(tstr_len(encoded), 35u);
    check_mem_eq(encoded, expected_header, sizeof(expected_header));
    check_mem_eq(encoded + sizeof(expected_header), client_id, sizeof(client_id));
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH,
                                                         encoded, tstr_len(encoded), 128u,
                                                         &decoded),
                 TURBO_OK);
    check_int_eq(decoded.operation, FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH);
    check_int_eq(decoded.mqtt_version, FLOWIE_MQTT_VERSION_5);
    check_mem_eq(decoded.client_id.data, client_id, decoded.client_id.size);
    check_int_eq(decoded.packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
    check_mem_eq(decoded.packet.packet.data, FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH,
                 decoded.packet.packet.size);
    tstr_free(encoded);
  }

  it("accepts exactly the five post-CONNECT MQTT operation families") {
    static const uint8_t client_id[] = {'c'};
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x07u, 0x00u, 0x00u,
                                        0x03u, 'a',   '/',   '#',   0x01u};
    static const uint8_t unsubscribe[] = {0xa2u, 0x08u, 0x00u, 0x08u, 0x00u,
                                          0x00u, 0x03u, 'a',   '/',   '#'};
    static const uint8_t ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t disconnect[] = {0xe0u, 0x00u};
    static const struct {
      flowie_cluster_peer_operation_t operation;
      const uint8_t *packet;
      size_t packet_size;
      flowie_mqtt_packet_type_t packet_type;
    } cases[] = {
        {FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH,
         sizeof(FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH), FLOWIE_MQTT_PACKET_PUBLISH},
        {FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, subscribe, sizeof(subscribe),
         FLOWIE_MQTT_PACKET_SUBSCRIBE},
        {FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE, unsubscribe, sizeof(unsubscribe),
         FLOWIE_MQTT_PACKET_UNSUBSCRIBE},
        {FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK, ack, sizeof(ack), FLOWIE_MQTT_PACKET_PUBACK},
        {FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT, disconnect, sizeof(disconnect),
         FLOWIE_MQTT_PACKET_DISCONNECT}};
    size_t index;
    for (index = 0u; index < sizeof(cases) / sizeof(cases[0]); ++index) {
      flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
      tstr_t encoded = NULL;
      check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                       cases[index].operation, FLOWIE_MQTT_VERSION_5,
                       (flowie_mqtt_span_t){client_id, sizeof(client_id)},
                       (flowie_mqtt_span_t){cases[index].packet, cases[index].packet_size}, 128u,
                       &encoded),
                   TURBO_OK);
      check_int_eq(flowie_cluster_peer_mqtt_command_decode(cases[index].operation, encoded,
                                                           tstr_len(encoded), 128u, &decoded),
                   TURBO_OK);
      check_int_eq(decoded.packet.type, cases[index].packet_type);
      check_size_eq(decoded.packet.packet.size, cases[index].packet_size);
      check_mem_eq(decoded.packet.packet.data, cases[index].packet, cases[index].packet_size);
      tstr_free(encoded);
    }
  }

  it("rejects malformed headers operation confusion and packet limits") {
    static const uint8_t client_id[] = {'c'};
    flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_MQTT_TEST_PUBLISH)},
                     128u, &encoded),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE,
                                                encoded, tstr_len(encoded), 128u, &decoded),
        TURBO_EPROTO);
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND,
                                                         encoded, tstr_len(encoded), 128u,
                                                         &decoded),
                 TURBO_ENOTSUP);
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH,
                                                         encoded, tstr_len(encoded), 4u, &decoded),
                 TURBO_EMSGSIZE);
    encoded[0] = 'X';
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH,
                                                         encoded, tstr_len(encoded), 128u,
                                                         &decoded),
                 TURBO_EPROTO);
    encoded[0] = 'T';
    encoded[19] = 1;
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH,
                                                         encoded, tstr_len(encoded), 128u,
                                                         &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }

  it("validates typed ACK and rejects invalid client identifiers") {
    static const uint8_t ack[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t client_id[] = {'c'};
    static const uint8_t invalid_client_id[] = {0xc0u, 0x80u};
    flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
    tstr_t encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id)},
                     (flowie_mqtt_span_t){ack, sizeof(ack)}, 128u, &encoded),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK,
                                                         encoded, tstr_len(encoded), 128u,
                                                         &decoded),
                 TURBO_OK);
    check_int_eq(decoded.packet.type, FLOWIE_MQTT_PACKET_PUBACK);
    tstr_free(encoded);
    encoded = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){invalid_client_id, sizeof(invalid_client_id)},
                     (flowie_mqtt_span_t){ack, sizeof(ack)}, 128u, &encoded),
                 TURBO_EPROTO);
    check_null(encoded);
  }
}
