#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

#define FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD 2048u

static int flowie_cluster_peer_connect_bind_test_contains(const uint8_t *data, size_t data_size,
                                                          const void *needle, size_t needle_size) {
  if (!data || !needle || needle_size == 0u || needle_size > data_size) return 0;
  for (size_t offset = 0u; offset <= data_size - needle_size; ++offset)
    if (memcmp(data + offset, needle, needle_size) == 0) return 1;
  return 0;
}

static uint32_t flowie_cluster_peer_connect_bind_test_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static turbo_flow_security_principal_t flowie_cluster_peer_connect_bind_test_principal(void) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)strcpy(principal.principal_id, "writer");
  (void)strcpy(principal.principal_type, "device");
  (void)strcpy(principal.root_group_id, "root-a");
  (void)strcpy(principal.auth_method, "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  principal.role_count = 1u;
  (void)strcpy(principal.roles[0], "writer");
  principal.group_count = 2u;
  (void)strcpy(principal.groups[0], "root-a");
  (void)strcpy(principal.groups[1], "team-a");
  principal.expires_at = 1000u;
  principal.policy_version = 7u;
  return principal;
}

static int flowie_cluster_peer_connect_bind_test_parse(
    const flowie_mqtt_connect_packet_t *description, uint8_t *wire, size_t capacity,
    flowie_mqtt_packet_view_t *packet, flowie_mqtt_connect_view_t *connect, size_t *written) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  size_t consumed = 0u;
  int rc;
  rc = flowie_mqtt_connect_packet_encode(description, wire, capacity, written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  options.max_packet_size = capacity;
  rc = flowie_mqtt_packet_parse(wire, *written, &options, packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != *written) return FLOWIE_MQTT_PARSE_MALFORMED;
  return flowie_mqtt_connect_parse(packet, connect);
}

spec("flowie cluster peer CONNECT_BIND codec") {
  it("round trips authenticated session state without edge credentials") {
    static const uint8_t connect_properties[] = {
        0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x21u, 0x00u, 0x0au, 0x15u, 0x00u, 0x05u,
        't',   'o',   'k',   'e',   'n',   0x16u, 0x00u, 0x0eu, 'r',   'a',   'w',
        '-',   'c',   'r',   'e',   'd',   'e',   'n',   't',   'i',   'a',   'l'};
    static const uint8_t will_properties[] = {0x18u, 0x00u, 0x00u, 0x00u, 0x05u, 0x03u,
                                              0x00u, 0x04u, 't',   'e',   'x',   't'};
    static const uint8_t client_id[] = "device-a";
    static const uint8_t will_topic[] = "status/device-a";
    static const uint8_t will_payload[] = {0x00u, 0xffu, 'x'};
    static const uint8_t username[] = "login-name";
    static const uint8_t password[] = "socket-secret";
    static const uint8_t expected_header[] = {'T', 'F', 'C', 'B', 0x00u, 0x02u, 0x00u, 0x20u};
    static const uint8_t expected_session_expiry[] = {0x11u, 0x00u, 0x00u, 0x00u, 0x3cu};
    flowie_mqtt_connect_packet_t description = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    turbo_flow_security_principal_t principal = flowie_cluster_peer_connect_bind_test_principal();
    uint8_t wire[512];
    size_t wire_size = 0u;
    tstr_t encoded = NULL;
    description.version = FLOWIE_MQTT_VERSION_5;
    description.clean_start = 0u;
    description.has_will = 1u;
    description.will_qos = 1u;
    description.will_retain = 1u;
    description.has_username = 1u;
    description.has_password = 1u;
    description.keep_alive = 45u;
    description.properties = (flowie_mqtt_span_t){connect_properties, sizeof(connect_properties)};
    description.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    description.will_properties = (flowie_mqtt_span_t){will_properties, sizeof(will_properties)};
    description.will_topic = (flowie_mqtt_span_t){will_topic, sizeof(will_topic) - 1u};
    description.will_payload = (flowie_mqtt_span_t){will_payload, sizeof(will_payload)};
    description.username = (flowie_mqtt_span_t){username, sizeof(username) - 1u};
    description.password = (flowie_mqtt_span_t){password, sizeof(password) - 1u};
    check_int_eq(flowie_cluster_peer_connect_bind_test_parse(&description, wire, sizeof(wire),
                                                             &packet, &connect, &wire_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(
        flowie_cluster_peer_connect_bind_encode(
            &connect, &principal, FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
        TURBO_OK);
    check_mem_eq(encoded, expected_header, sizeof(expected_header));
    check_false(flowie_cluster_peer_connect_bind_test_contains(
        (const uint8_t *)encoded, tstr_len(encoded), username, sizeof(username) - 1u));
    check_false(flowie_cluster_peer_connect_bind_test_contains(
        (const uint8_t *)encoded, tstr_len(encoded), password, sizeof(password) - 1u));
    check_false(flowie_cluster_peer_connect_bind_test_contains((const uint8_t *)encoded,
                                                               tstr_len(encoded), "raw-credential",
                                                               sizeof("raw-credential") - 1u));
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     encoded, tstr_len(encoded), FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD,
                     &decoded),
                 TURBO_OK);
    check_true(decoded.security_enabled);
    check_str_eq(decoded.principal.principal_id, "writer");
    check_str_eq(decoded.principal.root_group_id, "root-a");
    check_uint_eq(decoded.principal.policy_version, 7u);
    check_int_eq(decoded.connect.version, FLOWIE_MQTT_VERSION_5);
    check_uint_eq(decoded.connect.keep_alive, 45u);
    check_size_eq(decoded.connect.username.size, 0u);
    check_size_eq(decoded.connect.password.size, 0u);
    check_mem_eq(decoded.connect.client_id.data, client_id, sizeof(client_id) - 1u);
    check_size_eq(decoded.connect.properties.values.size, sizeof(expected_session_expiry));
    check_mem_eq(decoded.connect.properties.values.data, expected_session_expiry,
                 sizeof(expected_session_expiry));
    check_mem_eq(decoded.connect.will_properties.values.data, will_properties,
                 sizeof(will_properties));
    check_mem_eq(decoded.connect.will_payload.data, will_payload, sizeof(will_payload));
    tstr_free(encoded);
  }

  it("supports an insecure MQTT 3.1.1 bind while still stripping credentials") {
    static const uint8_t client_id[] = "legacy-a";
    static const uint8_t username[] = "legacy-login";
    static const uint8_t password[] = "legacy-secret";
    flowie_mqtt_connect_packet_t description = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    uint8_t wire[256];
    size_t wire_size = 0u;
    tstr_t encoded = NULL;
    description.version = FLOWIE_MQTT_VERSION_3_1_1;
    description.has_username = 1u;
    description.has_password = 1u;
    description.keep_alive = 30u;
    description.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    description.username = (flowie_mqtt_span_t){username, sizeof(username) - 1u};
    description.password = (flowie_mqtt_span_t){password, sizeof(password) - 1u};
    check_int_eq(flowie_cluster_peer_connect_bind_test_parse(&description, wire, sizeof(wire),
                                                             &packet, &connect, &wire_size),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_cluster_peer_connect_bind_encode(
                     &connect, NULL, FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
                 TURBO_OK);
    check_uint_eq(flowie_cluster_peer_connect_bind_test_read_u32((const uint8_t *)encoded + 16u),
                  0u);
    check_uint_eq(flowie_cluster_peer_connect_bind_test_read_u32((const uint8_t *)encoded + 20u),
                  0u);
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     encoded, tstr_len(encoded), FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD,
                     &decoded),
                 TURBO_OK);
    check_false(decoded.security_enabled);
    check_uint_eq(decoded.principal.policy_version, 0u);
    check_int_eq(decoded.connect.version, FLOWIE_MQTT_VERSION_3_1_1);
    check_size_eq(decoded.connect.username.size, 0u);
    check_size_eq(decoded.connect.password.size, 0u);
    tstr_free(encoded);
  }

  it("round trips bounded ingress addresses and opaque PROXY TLVs as advisory metadata") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t proxy_tlvs[] = {0xe0u, 0x00u, 0x02u, 'a', 'b'};
    static const char remote_address[] = "203.0.113.9:45678";
    static const char transport_peer_address[] = "127.0.0.1:443";
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_ingress_metadata_t metadata = {0};
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    tstr_t encoded = NULL;
    connect.version = FLOWIE_MQTT_VERSION_3_1_1;
    connect.clean_start = 1u;
    connect.keep_alive = 30u;
    connect.properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    metadata.remote_address =
        (tstr_v){remote_address, sizeof(remote_address) - 1u};
    metadata.transport_peer_address =
        (tstr_v){transport_peer_address, sizeof(transport_peer_address) - 1u};
    metadata.proxy_tlvs = (tstr_v){(const char *)proxy_tlvs, sizeof(proxy_tlvs)};
    check_int_eq(flowie_cluster_peer_connect_bind_encode_with_metadata(
                     &connect, NULL, &metadata,
                     FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     encoded, tstr_len(encoded), FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD,
                     &decoded),
                 TURBO_OK);
    check_size_eq(decoded.remote_address.len, sizeof(remote_address) - 1u);
    check_mem_eq(decoded.remote_address.data, remote_address, sizeof(remote_address) - 1u);
    check_size_eq(decoded.transport_peer_address.len, sizeof(transport_peer_address) - 1u);
    check_mem_eq(decoded.transport_peer_address.data, transport_peer_address,
                 sizeof(transport_peer_address) - 1u);
    check_size_eq(decoded.proxy_tlvs.len, sizeof(proxy_tlvs));
    check_mem_eq(decoded.proxy_tlvs.data, proxy_tlvs, sizeof(proxy_tlvs));
    check_mem_eq(decoded.connect.client_id.data, client_id, sizeof(client_id) - 1u);
    tstr_free(encoded);
  }

  it("rejects incomplete or non-text ingress addresses before encoding") {
    static const uint8_t client_id[] = "device-a";
    static const char remote_address[] = "203.0.113.9:45678";
    char address_with_nul[] = {'1', '2', '7', '\0', 'x'};
    char overlong_address[FLOWIE_CLUSTER_PEER_ADDRESS_MAX + 1u];
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_ingress_metadata_t metadata = {0};
    tstr_t encoded = NULL;
    memset(overlong_address, 'x', sizeof(overlong_address));
    connect.version = FLOWIE_MQTT_VERSION_3_1_1;
    connect.clean_start = 1u;
    connect.properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    metadata.remote_address =
        (tstr_v){remote_address, sizeof(remote_address) - 1u};
    check_int_eq(flowie_cluster_peer_connect_bind_encode_with_metadata(
                     &connect, NULL, &metadata,
                     FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
                 TURBO_EINVAL);
    check_null(encoded);
    metadata.transport_peer_address = (tstr_v){address_with_nul, sizeof(address_with_nul)};
    check_int_eq(flowie_cluster_peer_connect_bind_encode_with_metadata(
                     &connect, NULL, &metadata,
                     FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
                 TURBO_EINVAL);
    check_null(encoded);
    metadata.remote_address = (tstr_v){overlong_address, sizeof(overlong_address)};
    metadata.transport_peer_address =
        (tstr_v){remote_address, sizeof(remote_address) - 1u};
    check_int_eq(flowie_cluster_peer_connect_bind_encode_with_metadata(
                     &connect, NULL, &metadata,
                     FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
                 TURBO_EINVAL);
    check_null(encoded);
  }

  it("rejects invalid principals malformed headers and payload overflow") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t empty_property = 0u;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    turbo_flow_security_principal_t principal = flowie_cluster_peer_connect_bind_test_principal();
    tstr_t encoded = NULL;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 30u;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.properties.values.data = &empty_property;
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties.values.data = &empty_property;
    principal.policy_version = 0u;
    check_int_eq(
        flowie_cluster_peer_connect_bind_encode(
            &connect, &principal, FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
        TURBO_EPROTO);
    check_null(encoded);
    principal = flowie_cluster_peer_connect_bind_test_principal();
    check_int_eq(flowie_cluster_peer_connect_bind_encode(
                     &connect, &principal, FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE, &encoded),
                 TURBO_EMSGSIZE);
    check_null(encoded);
    check_int_eq(
        flowie_cluster_peer_connect_bind_encode(
            &connect, &principal, FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
        TURBO_OK);
    check_int_eq(flowie_cluster_peer_connect_bind_decode(encoded, tstr_len(encoded),
                                                         tstr_len(encoded) - 1u, &decoded),
                 TURBO_EMSGSIZE);
    encoded[24] = 1;
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     encoded, tstr_len(encoded), FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD,
                     &decoded),
                 TURBO_EPROTO);
    encoded[24] = 0;
    encoded[23] = 2;
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     encoded, tstr_len(encoded), FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD,
                     &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }

  it("revalidates principal group invariants after wire decode") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t empty_property = 0u;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    turbo_flow_security_principal_t principal = flowie_cluster_peer_connect_bind_test_principal();
    uint8_t *second_group;
    tstr_t encoded = NULL;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.properties.values.data = &empty_property;
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties.values.data = &empty_property;
    check_int_eq(
        flowie_cluster_peer_connect_bind_encode(
            &connect, &principal, FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD, &encoded),
        TURBO_OK);
    second_group = NULL;
    for (size_t offset = FLOWIE_CLUSTER_PEER_CONNECT_BIND_HEADER_SIZE;
         offset + sizeof("team-a") - 1u <= tstr_len(encoded); ++offset) {
      if (memcmp(encoded + offset, "team-a", sizeof("team-a") - 1u) == 0) {
        second_group = (uint8_t *)encoded + offset;
        break;
      }
    }
    check_not_null(second_group);
    memcpy(second_group, "root-a", sizeof("root-a") - 1u);
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     encoded, tstr_len(encoded), FLOWIE_CLUSTER_PEER_CONNECT_BIND_TEST_MAX_PAYLOAD,
                     &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }
}
