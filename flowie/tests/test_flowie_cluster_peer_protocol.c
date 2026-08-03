#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

static flowie_cluster_peer_frame_t flowie_cluster_peer_test_command(tstr_v payload) {
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  unsigned int index;
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
  frame.shard_id = UINT32_C(0x01020304);
  frame.owner_epoch = UINT64_C(0x0102030405060708);
  frame.connection_id = UINT64_C(0x1112131415161718);
  frame.connection_generation = UINT64_C(0x2122232425262728);
  frame.cluster_id = tstr_v_from_cstr("c");
  frame.listener_id = tstr_v_from_cstr("l");
  frame.source_node_id = tstr_v_from_cstr("s");
  frame.target_node_id = tstr_v_from_cstr("t");
  frame.payload = payload;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index) {
    frame.source_boot_id[index] = (uint8_t)(index + 1u);
    frame.target_boot_id[index] = (uint8_t)(index + 17u);
    frame.correlation_id[index] = (uint8_t)(index + 0xa0u);
  }
  return frame;
}

static flowie_cluster_peer_frame_t flowie_cluster_peer_test_ping(void) {
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  unsigned int index;
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_PING;
  frame.cluster_id = tstr_v_from_cstr("cluster-a");
  frame.source_node_id = tstr_v_from_cstr("node-a");
  frame.target_node_id = tstr_v_from_cstr("node-b");
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index) {
    frame.source_boot_id[index] = (uint8_t)(index + 1u);
    frame.target_boot_id[index] = (uint8_t)(index + 17u);
  }
  return frame;
}

spec("flowie cluster peer wire protocol") {
  group("golden v1 encoding") {
    it("uses one stable network-order representation") {
      static const uint8_t payload[] = {0xaau, 0x00u};
      static const uint8_t expected[] = {
          0x54, 0x46, 0x43, 0x4c, 0x00, 0x01, 0x00, 0x70, 0x00, 0x03, 0x00, 0x02, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x76, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01,
          0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
          0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
          0x1d, 0x1e, 0x1f, 0x20, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03,
          0x04, 0x05, 0x06, 0x07, 0x08, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9,
          0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21,
          0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x63, 0x6c, 0x73, 0x74, 0xaa, 0x00};
      flowie_cluster_peer_frame_t frame =
          flowie_cluster_peer_test_command(tstr_v_from_buf((const char *)payload, sizeof(payload)));
      tstr_t encoded = NULL;
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, 1024u, &encoded), TURBO_OK);
      check_size_eq(tstr_len(encoded), sizeof(expected));
      check_mem_eq(encoded, expected, sizeof(expected));
      tstr_free(encoded);
    }
  }

  group("owned decoding") {
    it("round trips binary command payload and owns its backing storage") {
      static const uint8_t payload[] = {0x00u, 0x7fu, 0x80u, 0xffu};
      flowie_cluster_peer_frame_t input =
          flowie_cluster_peer_test_command(tstr_v_from_buf((const char *)payload, sizeof(payload)));
      flowie_cluster_peer_frame_t output = FLOWIE_CLUSTER_PEER_FRAME_INIT;
      tstr_t encoded = NULL;
      size_t consumed = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&input, sizeof(payload), &encoded), TURBO_OK);
      check_int_eq(flowie_cluster_peer_frame_decode(encoded, tstr_len(encoded), sizeof(payload),
                                                    &output, &consumed),
                   TURBO_OK);
      check_size_eq(consumed, tstr_len(encoded));
      check_not_null(output.storage);
      check_int_eq(output.kind, FLOWIE_CLUSTER_PEER_FRAME_COMMAND);
      check_int_eq(output.operation, FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH);
      check_uint_eq(output.shard_id, input.shard_id);
      check_hex64_eq(output.owner_epoch, input.owner_epoch);
      check_hex64_eq(output.connection_id, input.connection_id);
      check_hex64_eq(output.connection_generation, input.connection_generation);
      check_mem_eq(output.payload.data, payload, sizeof(payload));
      tstr_free(encoded);
      check_mem_eq(output.payload.data, payload, sizeof(payload));
      flowie_cluster_peer_frame_cleanup(&output);
      flowie_cluster_peer_frame_cleanup(&output);
      check_null(output.storage);
    }

    it("reports incomplete frames without allocation or consumption") {
      flowie_cluster_peer_frame_t input = flowie_cluster_peer_test_ping();
      flowie_cluster_peer_frame_t output = FLOWIE_CLUSTER_PEER_FRAME_INIT;
      tstr_t encoded = NULL;
      size_t consumed = 99u;
      check_int_eq(flowie_cluster_peer_frame_encode(&input, 64u, &encoded), TURBO_OK);
      check_int_eq(flowie_cluster_peer_frame_decode(encoded, FLOWIE_CLUSTER_PEER_HEADER_SIZE - 1u,
                                                    64u, &output, &consumed),
                   FLOWIE_CLUSTER_PEER_INCOMPLETE);
      check_size_eq(consumed, 0u);
      check_null(output.storage);
      check_int_eq(flowie_cluster_peer_frame_decode(encoded, tstr_len(encoded) - 1u, 64u, &output,
                                                    &consumed),
                   FLOWIE_CLUSTER_PEER_INCOMPLETE);
      check_null(output.storage);
      tstr_free(encoded);
    }

    it("consumes only the first frame from a concatenated receive buffer") {
      flowie_cluster_peer_frame_t input = flowie_cluster_peer_test_ping();
      flowie_cluster_peer_frame_t output = FLOWIE_CLUSTER_PEER_FRAME_INIT;
      tstr_t encoded = NULL;
      tstr_t pair = NULL;
      size_t consumed = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&input, 64u, &encoded), TURBO_OK);
      pair = tstr_new_len(NULL, tstr_len(encoded) * 2u);
      check_not_null(pair);
      memcpy(pair, encoded, tstr_len(encoded));
      memcpy(pair + tstr_len(encoded), encoded, tstr_len(encoded));
      check_int_eq(flowie_cluster_peer_frame_decode(pair, tstr_len(pair), 64u, &output, &consumed),
                   TURBO_OK);
      check_size_eq(consumed, tstr_len(encoded));
      flowie_cluster_peer_frame_cleanup(&output);
      tstr_free(pair);
      tstr_free(encoded);
    }
  }

  group("strict boundaries and semantics") {
    it("rejects oversized payload from the fixed header before body arrival") {
      static const uint8_t payload[] = {1u, 2u};
      flowie_cluster_peer_frame_t input =
          flowie_cluster_peer_test_command(tstr_v_from_buf((const char *)payload, sizeof(payload)));
      flowie_cluster_peer_frame_t output = FLOWIE_CLUSTER_PEER_FRAME_INIT;
      tstr_t encoded = NULL;
      size_t consumed = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&input, sizeof(payload), &encoded), TURBO_OK);
      encoded[20] = 0u;
      encoded[21] = 0u;
      encoded[22] = 0u;
      encoded[23] = 3u;
      check_int_eq(flowie_cluster_peer_frame_decode(encoded, FLOWIE_CLUSTER_PEER_HEADER_SIZE, 2u,
                                                    &output, &consumed),
                   TURBO_EMSGSIZE);
      check_null(output.storage);
      tstr_free(encoded);
    }

    it("rejects unknown version flags and inconsistent total length") {
      flowie_cluster_peer_frame_t input = flowie_cluster_peer_test_ping();
      flowie_cluster_peer_frame_t output = FLOWIE_CLUSTER_PEER_FRAME_INIT;
      tstr_t encoded = NULL;
      size_t consumed = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&input, 64u, &encoded), TURBO_OK);
      encoded[5] = 2;
      check_int_eq(
          flowie_cluster_peer_frame_decode(encoded, tstr_len(encoded), 64u, &output, &consumed),
          TURBO_EPROTO);
      encoded[5] = FLOWIE_CLUSTER_PEER_WIRE_VERSION;
      encoded[15] = 1;
      check_int_eq(
          flowie_cluster_peer_frame_decode(encoded, tstr_len(encoded), 64u, &output, &consumed),
          TURBO_EPROTO);
      encoded[15] = 0;
      encoded[19] = (uint8_t)(encoded[19] - 1u);
      check_int_eq(
          flowie_cluster_peer_frame_decode(encoded, tstr_len(encoded), 64u, &output, &consumed),
          TURBO_EPROTO);
      tstr_free(encoded);
    }

    it("rejects state frames without fencing or connection generation") {
      static const uint8_t payload[] = {1u};
      flowie_cluster_peer_frame_t frame =
          flowie_cluster_peer_test_command(tstr_v_from_buf((const char *)payload, sizeof(payload)));
      tstr_t encoded = NULL;
      frame.owner_epoch = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, sizeof(payload), &encoded),
                   TURBO_EPROTO);
      frame =
          flowie_cluster_peer_test_command(tstr_v_from_buf((const char *)payload, sizeof(payload)));
      frame.connection_generation = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, sizeof(payload), &encoded),
                   TURBO_EPROTO);
      check_null(encoded);
    }

    it("allows connection-free events but rejects ambiguous connection fields") {
      static const uint8_t payload[] = {1u};
      flowie_cluster_peer_frame_t frame =
          flowie_cluster_peer_test_command(tstr_v_from_buf((const char *)payload, sizeof(payload)));
      tstr_t encoded = NULL;
      frame.kind = FLOWIE_CLUSTER_PEER_FRAME_EVENT;
      frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_EVENT_DELIVER;
      frame.connection_id = 0u;
      frame.connection_generation = 0u;
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, sizeof(payload), &encoded), TURBO_OK);
      tstr_free(encoded);
      encoded = NULL;
      frame.connection_id = 1u;
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, sizeof(payload), &encoded),
                   TURBO_EPROTO);
      check_null(encoded);
    }

    it("rejects control frames carrying MQTT state") {
      static const uint8_t payload[] = {1u};
      flowie_cluster_peer_frame_t frame = flowie_cluster_peer_test_ping();
      tstr_t encoded = NULL;
      frame.payload = tstr_v_from_buf((const char *)payload, sizeof(payload));
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, sizeof(payload), &encoded),
                   TURBO_EPROTO);
      frame = flowie_cluster_peer_test_ping();
      frame.owner_epoch = 1u;
      check_int_eq(flowie_cluster_peer_frame_encode(&frame, sizeof(payload), &encoded),
                   TURBO_EPROTO);
      check_null(encoded);
    }

    it("requires the exact target process incarnation") {
      flowie_cluster_peer_frame_t frame = flowie_cluster_peer_test_ping();
      uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
      memcpy(boot_id, frame.target_boot_id, sizeof(boot_id));
      check_int_eq(flowie_cluster_peer_frame_require_target(&frame, tstr_v_from_cstr("cluster-a"),
                                                            tstr_v_from_cstr("node-b"), boot_id),
                   TURBO_OK);
      boot_id[0] ^= 0xffu;
      check_int_eq(flowie_cluster_peer_frame_require_target(&frame, tstr_v_from_cstr("cluster-a"),
                                                            tstr_v_from_cstr("node-b"), boot_id),
                   TURBO_EPROTO);
      check_int_eq(flowie_cluster_peer_frame_require_target(&frame, tstr_v_from_cstr("other"),
                                                            tstr_v_from_cstr("node-b"),
                                                            frame.target_boot_id),
                   TURBO_EPROTO);
    }
  }
}
