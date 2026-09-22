#include "salts_error.h"
#include "tinytest.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_mapper.h"
#include "turbo_flow_protocol_plugin.h"

#include <string.h>

typedef struct semantic_probe_s {
  int inspect_calls;
  int semantic_calls;
  int replace_buffer;
} semantic_probe_t;

static int fixture_inspect(void *ctx, const char *configured_version,
                           const turbo_flow_protocol_frame_view_t *frame,
                           turbo_flow_protocol_metadata_t *metadata) {
  semantic_probe_t *probe = (semantic_probe_t *)ctx;
  (void)configured_version;
  (void)frame;
  if (!probe || !metadata) return SALTS_EINVAL;
  probe->inspect_calls++;
  memcpy(metadata->operation, "legacy", sizeof("legacy"));
  return SALTS_OK;
}

static int fixture_semantic(void *ctx, const char *configured_version,
                            const turbo_flow_protocol_frame_view_t *frame,
                            turbo_flow_protocol_metadata_t *metadata,
                            turbo_flow_protocol_semantic_output_t *output) {
  static const uint8_t business[] = "{\"age\":21}";
  semantic_probe_t *probe = (semantic_probe_t *)ctx;
  (void)configured_version;
  if (!probe || !frame || !metadata || !output) return SALTS_EINVAL;
  probe->semantic_calls++;
  if (probe->replace_buffer) {
    output->data = (uint8_t *)(uintptr_t)1u;
    return SALTS_OK;
  }
  if (!output->data || sizeof(business) - 1u > output->capacity) return SALTS_EMSGSIZE;
  memcpy(output->data, business, sizeof(business) - 1u);
  output->data_size = sizeof(business) - 1u;
  output->semantic_type = 50u;
  memcpy(output->media_type, "application/json", sizeof("application/json"));
  memcpy(metadata->operation, "post", sizeof("post"));
  metadata->message_type = 2u;
  metadata->sequence = 17u;
  return SALTS_OK;
}

static int create_fixture(semantic_probe_t *probe, int semantic,
                          turbo_flow_protocol_t **out) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  request.protocol = TURBO_FLOW_PROTOCOL_COAP;
  request.protocol_version = "fixture-v1";
  request.max_frame_size = 256u;
  ops.inspect = fixture_inspect;
  if (semantic) ops.decode_semantic = fixture_semantic;
  return turbo_flow_protocol_create(
      &request, "fixture", "fixture-v1",
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
          TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
      &ops, probe, out);
}

static void setup_outputs(turbo_flow_protocol_frame_view_t *frame,
                          turbo_flow_protocol_message_output_t *raw,
                          turbo_flow_protocol_semantic_output_t *semantic,
                          uint8_t *raw_bytes, size_t raw_capacity,
                          uint8_t *semantic_bytes, size_t semantic_capacity) {
  static const uint8_t wire[] = {0x40u, 0x02u, 0x00u, 0x11u, 0xffu, 'x'};
  *frame = (turbo_flow_protocol_frame_view_t)TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  frame->data = wire;
  frame->data_size = sizeof(wire);
  frame->device_id = "device-17";
  frame->protocol_version = "fixture-v1";
  *raw = (turbo_flow_protocol_message_output_t)TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  raw->payload = raw_bytes;
  raw->payload_capacity = raw_capacity;
  *semantic =
      (turbo_flow_protocol_semantic_output_t)TURBO_FLOW_PROTOCOL_SEMANTIC_OUTPUT_INIT;
  semantic->data = semantic_bytes;
  semantic->capacity = semantic_capacity;
}

spec("protocol semantic decode and mapper ABI") {
  it("keeps legacy raw-only codecs explicit") {
    semantic_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_frame_view_t frame;
    turbo_flow_protocol_message_output_t raw;
    turbo_flow_protocol_semantic_output_t semantic;
    uint8_t raw_bytes[32] = {0};
    uint8_t semantic_bytes[32] = {0};
    check_equal(create_fixture(&probe, 0, &protocol), SALTS_OK);
    setup_outputs(&frame, &raw, &semantic, raw_bytes, sizeof(raw_bytes),
                  semantic_bytes, sizeof(semantic_bytes));
    check_equal(turbo_flow_protocol_decode_semantic(protocol, &frame, &raw, &semantic),
                SALTS_ENOTSUP);
    check_equal(raw.payload_size, (size_t)0u);
    check_equal(semantic.data_size, (size_t)0u);
    check_equal(probe.inspect_calls, 0);
    check_equal(probe.semantic_calls, 0);
    turbo_flow_protocol_destroy(protocol);
  }

  it("decodes raw preserve and semantic content through one codec callback") {
    static const uint8_t expected[] = "{\"age\":21}";
    semantic_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_frame_view_t frame;
    turbo_flow_protocol_message_output_t raw;
    turbo_flow_protocol_semantic_output_t semantic;
    uint8_t raw_bytes[32] = {0};
    uint8_t semantic_bytes[32] = {0};
    check_equal(create_fixture(&probe, 1, &protocol), SALTS_OK);
    setup_outputs(&frame, &raw, &semantic, raw_bytes, sizeof(raw_bytes),
                  semantic_bytes, sizeof(semantic_bytes));
    check_equal(turbo_flow_protocol_decode_semantic(protocol, &frame, &raw, &semantic),
                SALTS_OK);
    check_equal(probe.semantic_calls, 1);
    check_equal(probe.inspect_calls, 0);
    check_equal(raw.payload_size, frame.data_size);
    check_equal(memcmp(raw.payload, frame.data, frame.data_size), 0);
    check_equal(raw.metadata.protocol, TURBO_FLOW_PROTOCOL_COAP);
    check_equal(raw.metadata.direction, TURBO_FLOW_PROTOCOL_DIRECTION_UP);
    check_equal(raw.metadata.operation, "post");
    check_equal(raw.metadata.device_id, "device-17");
    check_equal(raw.metadata.protocol_version, "fixture-v1");
    check_equal(semantic.data_size, sizeof(expected) - 1u);
    check_equal(memcmp(semantic.data, expected, sizeof(expected) - 1u), 0);
    check_equal(semantic.semantic_type, 50u);
    check_equal(semantic.media_type, "application/json");
    turbo_flow_protocol_destroy(protocol);
  }

  it("rejects a semantic codec that replaces caller-owned storage") {
    semantic_probe_t probe = {.replace_buffer = 1};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_frame_view_t frame;
    turbo_flow_protocol_message_output_t raw;
    turbo_flow_protocol_semantic_output_t semantic;
    uint8_t raw_bytes[32] = {0};
    uint8_t semantic_bytes[32] = {0};
    check_equal(create_fixture(&probe, 1, &protocol), SALTS_OK);
    setup_outputs(&frame, &raw, &semantic, raw_bytes, sizeof(raw_bytes),
                  semantic_bytes, sizeof(semantic_bytes));
    check_equal(turbo_flow_protocol_decode_semantic(protocol, &frame, &raw, &semantic),
                SALTS_EPROTO);
    check_equal(raw.payload_size, (size_t)0u);
    check_equal(semantic.data_size, (size_t)0u);
    check_equal(semantic.data, semantic_bytes);
    turbo_flow_protocol_destroy(protocol);
  }

  it("defines preflight plus bounded pre-durable mapping without raw-frame access") {
    turbo_flow_protocol_mapper_preflight_request_t preflight =
        TURBO_FLOW_PROTOCOL_MAPPER_PREFLIGHT_REQUEST_INIT;
    turbo_flow_protocol_mapper_contract_t contract =
        TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
    turbo_flow_protocol_mapper_request_t request =
        TURBO_FLOW_PROTOCOL_MAPPER_REQUEST_INIT;
    turbo_flow_protocol_mapper_output_t output =
        TURBO_FLOW_PROTOCOL_MAPPER_OUTPUT_INIT;
    turbo_flow_protocol_mapper_v1_t mapper = TURBO_FLOW_PROTOCOL_MAPPER_V1_INIT;
    check_equal(preflight.size, sizeof(preflight));
    check_equal(contract.size, sizeof(contract));
    check_equal(request.size, sizeof(request));
    check_equal(output.size, sizeof(output));
    check_equal(mapper.size, sizeof(mapper));
    check_equal(preflight.semantic_type, TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE);
    check_equal(contract.semantic_type, TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE);
    check_equal(request.semantic_type, TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE);
    check_equal(contract.content.size, sizeof(contract.content));
    check_equal(contract.content.domain, TURBO_FLOW_DOMAIN_DATA);
    check_equal(output.content.size, sizeof(output.content));
    check_equal(output.payload_size, (size_t)0u);
    check_null(mapper.preflight);
    check_null(mapper.map);
  }
}
