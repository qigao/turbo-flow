#include "flowie_proxy_protocol_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static const uint8_t FLOWIE_PROXY_TEST_SIGNATURE[12] = {
    0x0du, 0x0au, 0x0du, 0x0au, 0x00u, 0x0du, 0x0au, 0x51u, 0x55u, 0x49u, 0x54u, 0x0au};

static void flowie_proxy_test_header(uint8_t *header, uint8_t command, uint8_t family,
                                     uint16_t payload_size) {
  memcpy(header, FLOWIE_PROXY_TEST_SIGNATURE, sizeof(FLOWIE_PROXY_TEST_SIGNATURE));
  header[12] = (uint8_t)(0x20u | command);
  header[13] = family;
  header[14] = (uint8_t)(payload_size >> 8u);
  header[15] = (uint8_t)payload_size;
}

spec("flowie trusted PROXY protocol parser") {
  it("parses a fragmented HAProxy-style v1 TCP4 header without consuming TLS") {
    static const char bytes[] =
        "PROXY TCP4 192.0.2.10 10.0.0.8 8080 8883\r\n\x16\x03\x01";
    static const size_t header_size =
        sizeof("PROXY TCP4 192.0.2.10 10.0.0.8 8080 8883\r\n") - 1u;
    flowie_proxy_protocol_v1_view_t view = FLOWIE_PROXY_PROTOCOL_V1_VIEW_INIT;
    size_t consumed = 99u;

    check_int_eq(flowie_proxy_protocol_v1_parse(bytes, header_size - 1u, 256u, &view,
                                                &consumed),
                 FLOWIE_PROXY_PROTOCOL_INCOMPLETE);
    check_size_eq(consumed, 0u);
    check_int_eq(flowie_proxy_protocol_v1_parse(bytes, sizeof(bytes) - 1u, 256u, &view,
                                                &consumed),
                 TURBO_OK);
    check_size_eq(consumed, header_size);
    check_size_eq(view.header_size, header_size);
    check_int_eq(view.command, FLOWIE_PROXY_PROTOCOL_COMMAND_PROXY);
    check_int_eq(view.address_family, FLOWIE_PROXY_PROTOCOL_ADDRESS_IPV4);
    check_str_eq(view.source_address, "192.0.2.10");
    check_str_eq(view.destination_address, "10.0.0.8");
    check_uint_eq(view.source_port, 8080u);
    check_uint_eq(view.destination_port, 8883u);
    check_uint_eq((uint8_t)bytes[consumed], 0x16u);
  }

  it("parses and canonicalizes a v1 TCP6 header") {
    static const char bytes[] =
        "PROXY TCP6 2001:0db8:0:0:0:0:0:1 2001:db8::2 1883 8883\r\n";
    flowie_proxy_protocol_v1_view_t view = FLOWIE_PROXY_PROTOCOL_V1_VIEW_INIT;
    size_t consumed = 0u;

    check_int_eq(flowie_proxy_protocol_v1_parse(bytes, sizeof(bytes) - 1u, 256u, &view,
                                                &consumed),
                 TURBO_OK);
    check_int_eq(view.address_family, FLOWIE_PROXY_PROTOCOL_ADDRESS_IPV6);
    check_str_eq(view.source_address, "2001:db8::1");
    check_str_eq(view.destination_address, "2001:db8::2");
    check_uint_eq(view.source_port, 1883u);
    check_uint_eq(view.destination_port, 8883u);
    check_size_eq(consumed, sizeof(bytes) - 1u);
  }

  it("treats v1 UNKNOWN as the authenticated direct transport peer") {
    static const char bytes[] = "PROXY UNKNOWN ignored-by-receiver\r\n";
    flowie_proxy_protocol_v1_view_t view = FLOWIE_PROXY_PROTOCOL_V1_VIEW_INIT;
    size_t consumed = 0u;

    check_int_eq(flowie_proxy_protocol_v1_parse(bytes, sizeof(bytes) - 1u, 256u, &view,
                                                &consumed),
                 TURBO_OK);
    check_int_eq(view.command, FLOWIE_PROXY_PROTOCOL_COMMAND_LOCAL);
    check_int_eq(view.address_family, FLOWIE_PROXY_PROTOCOL_ADDRESS_UNSPECIFIED);
    check_str_eq(view.source_address, "");
    check_size_eq(consumed, sizeof(bytes) - 1u);
  }

  it("rejects non-canonical v1 tokens, addresses and ports") {
    static const char extra_space[] =
        "PROXY  TCP4 192.0.2.10 10.0.0.8 8080 8883\r\n";
    static const char invalid_address[] =
        "PROXY TCP4 300.0.2.10 10.0.0.8 8080 8883\r\n";
    static const char oversized_port[] =
        "PROXY TCP4 192.0.2.10 10.0.0.8 65536 8883\r\n";
    static const char padded_port[] =
        "PROXY TCP4 192.0.2.10 10.0.0.8 01883 8883\r\n";
    flowie_proxy_protocol_v1_view_t view = FLOWIE_PROXY_PROTOCOL_V1_VIEW_INIT;
    size_t consumed = 0u;

    check_int_eq(flowie_proxy_protocol_v1_parse(extra_space, sizeof(extra_space) - 1u, 256u,
                                                &view, &consumed),
                 TURBO_EPROTO);
    check_int_eq(flowie_proxy_protocol_v1_parse(invalid_address,
                                                sizeof(invalid_address) - 1u, 256u, &view,
                                                &consumed),
                 TURBO_EPROTO);
    check_int_eq(flowie_proxy_protocol_v1_parse(oversized_port,
                                                sizeof(oversized_port) - 1u, 256u, &view,
                                                &consumed),
                 TURBO_EPROTO);
    check_int_eq(flowie_proxy_protocol_v1_parse(padded_port, sizeof(padded_port) - 1u, 256u,
                                                &view, &consumed),
                 TURBO_EPROTO);
    check_size_eq(consumed, 0u);
  }

  it("fails closed when a v1 line ending or size boundary is invalid") {
    static const char naked_lf[] =
        "PROXY TCP4 192.0.2.10 10.0.0.8 8080 8883\n";
    static const char complete[] =
        "PROXY TCP4 192.0.2.10 10.0.0.8 8080 8883\r\n";
    uint8_t no_line_end[FLOWIE_PROXY_PROTOCOL_V1_MAX_WIRE_SIZE];
    flowie_proxy_protocol_v1_view_t view = FLOWIE_PROXY_PROTOCOL_V1_VIEW_INIT;
    size_t consumed = 0u;

    memset(no_line_end, 'x', sizeof(no_line_end));
    memcpy(no_line_end, "PROXY ", sizeof("PROXY ") - 1u);
    check_int_eq(flowie_proxy_protocol_v1_parse(naked_lf, sizeof(naked_lf) - 1u, 256u, &view,
                                                &consumed),
                 TURBO_EPROTO);
    check_int_eq(flowie_proxy_protocol_v1_parse(no_line_end, sizeof(no_line_end), 256u, &view,
                                                &consumed),
                 TURBO_EPROTO);
    check_int_eq(flowie_proxy_protocol_v1_parse(complete, sizeof(complete) - 1u, 32u, &view,
                                                &consumed),
                 TURBO_EMSGSIZE);
    check_size_eq(consumed, 0u);
  }

  it("parses a fragmented TCP4 header and leaves following TLS bytes untouched") {
    uint8_t bytes[35] = {0u};
    static const uint8_t source[4] = {192u, 0u, 2u, 10u};
    static const uint8_t destination[4] = {10u, 0u, 0u, 8u};
    flowie_proxy_protocol_v2_view_t view = FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT;
    size_t consumed = 99u;
    flowie_proxy_test_header(bytes, 1u, 0x11u, 16u);
    memcpy(bytes + 16u, source, sizeof(source));
    memcpy(bytes + 20u, destination, sizeof(destination));
    bytes[24] = 0x1fu;
    bytes[25] = 0x90u;
    bytes[26] = 0x23u;
    bytes[27] = 0x28u;
    bytes[28] = 0x04u;
    bytes[29] = 0x00u;
    bytes[30] = 0x01u;
    bytes[31] = 0xa5u;
    bytes[32] = 0x16u;
    bytes[33] = 0x03u;
    bytes[34] = 0x01u;
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 7u, 256u, &view, &consumed),
                 FLOWIE_PROXY_PROTOCOL_INCOMPLETE);
    check_size_eq(consumed, 0u);
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 31u, 256u, &view, &consumed),
                 FLOWIE_PROXY_PROTOCOL_INCOMPLETE);
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, sizeof(bytes), 256u, &view, &consumed),
                 TURBO_OK);
    check_size_eq(consumed, 32u);
    check_int_eq(view.command, FLOWIE_PROXY_PROTOCOL_COMMAND_PROXY);
    check_int_eq(view.address_family, FLOWIE_PROXY_PROTOCOL_ADDRESS_IPV4);
    check_mem_eq(view.source_address, source, sizeof(source));
    check_mem_eq(view.destination_address, destination, sizeof(destination));
    check_uint_eq(view.source_port, 8080u);
    check_uint_eq(view.destination_port, 9000u);
    check_size_eq(view.tlvs_size, 4u);
    check_uint_eq(bytes[consumed], 0x16u);
  }

  it("parses TCP6 addresses without formatting or allocation") {
    uint8_t bytes[52] = {0u};
    flowie_proxy_protocol_v2_view_t view = FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT;
    size_t consumed = 0u;
    size_t index;
    flowie_proxy_test_header(bytes, 1u, 0x21u, 36u);
    for (index = 0u; index < 16u; ++index) {
      bytes[16u + index] = (uint8_t)index;
      bytes[32u + index] = (uint8_t)(0x80u + index);
    }
    bytes[48] = 0x07u;
    bytes[49] = 0x5bu;
    bytes[50] = 0x22u;
    bytes[51] = 0xb3u;
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, sizeof(bytes), sizeof(bytes), &view,
                                                &consumed),
                 TURBO_OK);
    check_int_eq(view.address_family, FLOWIE_PROXY_PROTOCOL_ADDRESS_IPV6);
    check_size_eq(view.address_size, 16u);
    check_uint_eq(view.source_port, 1883u);
    check_uint_eq(view.destination_port, 8883u);
    check_size_eq(consumed, sizeof(bytes));
  }

  it("iterates validated TLVs without copying or assigning application semantics") {
    uint8_t bytes[39] = {0u};
    flowie_proxy_protocol_v2_view_t view = FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT;
    flowie_proxy_protocol_v2_tlv_cursor_t cursor =
        FLOWIE_PROXY_PROTOCOL_V2_TLV_CURSOR_INIT;
    flowie_proxy_protocol_v2_tlv_t tlv = FLOWIE_PROXY_PROTOCOL_V2_TLV_INIT;
    size_t consumed = 0u;
    flowie_proxy_test_header(bytes, 1u, 0x11u, 23u);
    bytes[28] = 0xe0u;
    bytes[29] = 0x00u;
    bytes[30] = 0x02u;
    bytes[31] = 0x12u;
    bytes[32] = 0x34u;
    bytes[33] = 0xe0u;
    bytes[34] = 0x00u;
    bytes[35] = 0x00u;
    bytes[36] = 0xe1u;
    bytes[37] = 0x00u;
    bytes[38] = 0x00u;
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, sizeof(bytes), sizeof(bytes), &view,
                                                &consumed),
                 TURBO_OK);
    check_int_eq(flowie_proxy_protocol_v2_tlv_cursor_init(&view, &cursor), TURBO_OK);
    check_int_eq(flowie_proxy_protocol_v2_tlv_next(&cursor, &tlv), TURBO_OK);
    check_uint_eq(tlv.type, 0xe0u);
    check_size_eq(tlv.value_size, 2u);
    check_mem_eq(tlv.value, bytes + 31u, 2u);
    check_int_eq(flowie_proxy_protocol_v2_tlv_next(&cursor, &tlv), TURBO_OK);
    check_uint_eq(tlv.type, 0xe0u);
    check_size_eq(tlv.value_size, 0u);
    check_int_eq(flowie_proxy_protocol_v2_tlv_next(&cursor, &tlv), TURBO_OK);
    check_uint_eq(tlv.type, 0xe1u);
    check_int_eq(flowie_proxy_protocol_v2_tlv_next(&cursor, &tlv), TURBO_ENOENT);
  }

  it("rejects a TLV cursor for LOCAL or structurally invalid borrowed views") {
    uint8_t bytes[16] = {0u};
    flowie_proxy_protocol_v2_view_t view = FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT;
    flowie_proxy_protocol_v2_tlv_cursor_t cursor =
        FLOWIE_PROXY_PROTOCOL_V2_TLV_CURSOR_INIT;
    flowie_proxy_protocol_v2_tlv_t tlv = FLOWIE_PROXY_PROTOCOL_V2_TLV_INIT;
    size_t consumed = 0u;
    flowie_proxy_test_header(bytes, 0u, 0x00u, 0u);
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, sizeof(bytes), sizeof(bytes), &view,
                                                &consumed),
                 TURBO_OK);
    check_int_eq(flowie_proxy_protocol_v2_tlv_cursor_init(&view, &cursor), TURBO_EINVAL);
    cursor.data = bytes;
    cursor.data_size = 1u;
    cursor.offset = 2u;
    check_int_eq(flowie_proxy_protocol_v2_tlv_next(&cursor, &tlv), TURBO_EINVAL);
  }

  it("accepts LOCAL while refusing to trust its ignored address block") {
    uint8_t bytes[20] = {0u};
    flowie_proxy_protocol_v2_view_t view = FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT;
    size_t consumed = 0u;
    flowie_proxy_test_header(bytes, 0u, 0xffu, 4u);
    memset(bytes + 16u, 0xa5, 4u);
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, sizeof(bytes), sizeof(bytes), &view,
                                                &consumed),
                 TURBO_OK);
    check_int_eq(view.command, FLOWIE_PROXY_PROTOCOL_COMMAND_LOCAL);
    check_int_eq(view.address_family, FLOWIE_PROXY_PROTOCOL_ADDRESS_UNSPECIFIED);
    check_null(view.source_address);
    check_size_eq(view.header_size, sizeof(bytes));
  }

  it("fails closed on missing, invalid, oversized or malformed headers") {
    uint8_t bytes[32] = {0u};
    flowie_proxy_protocol_v2_view_t view = FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT;
    size_t consumed = 0u;
    check_int_eq(flowie_proxy_protocol_v2_parse("MQTT", 4u, 64u, &view, &consumed),
                 TURBO_EPROTO);
    flowie_proxy_test_header(bytes, 1u, 0x11u, 12u);
    bytes[12] = 0x31u;
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 28u, 64u, &view, &consumed),
                 TURBO_EPROTO);
    bytes[12] = 0x22u;
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 28u, 64u, &view, &consumed),
                 TURBO_EPROTO);
    bytes[12] = 0x21u;
    bytes[13] = 0x12u;
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 28u, 64u, &view, &consumed),
                 TURBO_EPROTO);
    flowie_proxy_test_header(bytes, 1u, 0x11u, 13u);
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 29u, 64u, &view, &consumed),
                 TURBO_EPROTO);
    flowie_proxy_test_header(bytes, 1u, 0x11u, 100u);
    check_int_eq(flowie_proxy_protocol_v2_parse(bytes, 16u, 64u, &view, &consumed),
                 TURBO_EMSGSIZE);
  }

  it("copies only canonical bounded trusted CIDRs into pre-TLS admission") {
    static const char *const trusted[] = {"127.0.0.1/32", "2001:db8::/32"};
    static const char *const host_bits_set[] = {"10.1.0.1/16"};
    flowie_endpoint_proxy_binding_t binding = FLOWIE_ENDPOINT_PROXY_BINDING_INIT;
    flowie_proxy_protocol_policy_t *policy = NULL;
    coro_server_pre_tls_admission_config_t admission =
        CORO_SERVER_PRE_TLS_ADMISSION_CONFIG_DEFAULT;

    binding.trusted_peer_cidrs = trusted;
    binding.trusted_peer_count = sizeof(trusted) / sizeof(trusted[0]);
    binding.max_header_bytes = 512u;
    binding.header_timeout_ms = 250u;
    check_int_eq(flowie_proxy_protocol_policy_create(&binding, &policy), TURBO_OK);
    check_not_null(policy);
    check_int_eq(flowie_proxy_protocol_policy_coronet_config(policy, &admission), TURBO_OK);
    check_size_eq(admission.max_prefix_bytes, 512u);
    check_uint_eq(admission.timeout_ms, 250u);
    check_not_null(admission.callback);
    check_not_null(admission.release);
    flowie_proxy_protocol_policy_destroy(policy);

    policy = NULL;
    binding.trusted_peer_cidrs = host_bits_set;
    binding.trusted_peer_count = 1u;
    check_int_eq(flowie_proxy_protocol_policy_create(&binding, &policy), TURBO_EINVAL);
    check_null(policy);
    binding.trusted_peer_cidrs = trusted;
    binding.trusted_peer_count = FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS + 1u;
    check_int_eq(flowie_proxy_protocol_policy_create(&binding, &policy), TURBO_EINVAL);
  }
}
