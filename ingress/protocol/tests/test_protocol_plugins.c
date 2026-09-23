#include "salts_error.h"
#include "tinytest.h"
#include "turbo_flow_plugin_protocol.h"
#include "turbo_flow_protocol.h"

#include <stdio.h>
#include <string.h>

typedef struct protocol_case_s {
  const char *module;
  const char *name;
  turbo_flow_protocol_kind_t protocol;
  const char *version;
  const char *device_id;
  const char *operation;
  const uint8_t *frame;
  size_t frame_size;
} protocol_case_t;

static int protocol_open_one(const char *module, const char *name,
                             turbo_flow_protocol_kind_t protocol, const char *version,
                             turbo_flow_plugin_host_t **host_out,
                             turbo_flow_protocol_registry_t **registry_out,
                             turbo_flow_protocol_owner_t **owner_out,
                             turbo_flow_protocol_t **protocol_out);
static void protocol_close_one(turbo_flow_plugin_host_t *host,
                               turbo_flow_protocol_registry_t *registry,
                               turbo_flow_protocol_owner_t *owner);

static size_t protocol_gbt32960_frame(uint8_t *out, size_t capacity) {
  static const char vin[] = "L1234567890123456";
  uint8_t checksum = 0u;
  if (!out || capacity < 25u) return 0u;
  memset(out, 0, 25u);
  out[0] = 0x23u;
  out[1] = 0x23u;
  out[2] = 0x02u;
  out[3] = 0xfeu;
  memcpy(out + 4u, vin, sizeof(vin) - 1u);
  out[21] = 0x01u;
  for (size_t i = 2u; i < 24u; ++i)
    checksum ^= out[i];
  out[24] = checksum;
  return 25u;
}

static size_t protocol_jtt808_frame(uint8_t *out, size_t capacity) {
  const uint8_t unescaped[] = {0x02u, 0x00u, 0x40u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u,
                               0x00u, 0x00u, 0x00u, 0x01u, 0x23u, 0x45u, 0x00u, 0x01u};
  uint8_t checksum = 0u;
  size_t written = 0u;
  if (!out || capacity < sizeof(unescaped) * 2u + 4u) return 0u;
  out[written++] = 0x7eu;
  for (size_t i = 0u; i < sizeof(unescaped); ++i) {
    checksum ^= unescaped[i];
    if (unescaped[i] == 0x7du || unescaped[i] == 0x7eu) return 0u;
    out[written++] = unescaped[i];
  }
  if (checksum == 0x7du) {
    out[written++] = 0x7du;
    out[written++] = 0x01u;
  } else if (checksum == 0x7eu) {
    out[written++] = 0x7du;
    out[written++] = 0x02u;
  } else {
    out[written++] = checksum;
  }
  out[written++] = 0x7eu;
  return written;
}

static size_t protocol_jtt808_transparent_json_frame(
    uint8_t *out, size_t capacity, const uint8_t *json, size_t json_size) {
  uint8_t header[17] = {0x09u, 0x00u, 0x00u, 0x00u, 0x01u,
                        0x00u, 0x00u, 0x00u, 0x00u, 0x00u,
                        0x00u, 0x00u, 0x01u, 0x23u, 0x45u,
                        0x00u, 0x21u};
  uint8_t checksum = 0u;
  size_t written = 0u;
  size_t body_size;
  if (!out || (!json && json_size != 0u) || json_size > 0x03feu) return 0u;
  body_size = json_size + 1u;
  header[2] = (uint8_t)(0x40u | ((body_size >> 8u) & 0x03u));
  header[3] = (uint8_t)body_size;
  if (capacity < (sizeof(header) + body_size + 1u) * 2u + 2u) return 0u;

  out[written++] = 0x7eu;
  for (size_t i = 0u; i < sizeof(header) + body_size; ++i) {
    uint8_t value;
    if (i < sizeof(header))
      value = header[i];
    else if (i == sizeof(header))
      value = 0x01u;
    else
      value = json[i - sizeof(header) - 1u];
    checksum ^= value;
    if (value == 0x7du) {
      out[written++] = 0x7du;
      out[written++] = 0x01u;
    } else if (value == 0x7eu) {
      out[written++] = 0x7du;
      out[written++] = 0x02u;
    } else {
      out[written++] = value;
    }
  }
  if (checksum == 0x7du) {
    out[written++] = 0x7du;
    out[written++] = 0x01u;
  } else if (checksum == 0x7eu) {
    out[written++] = 0x7du;
    out[written++] = 0x02u;
  } else {
    out[written++] = checksum;
  }
  out[written++] = 0x7eu;
  return written;
}

static int protocol_semantic_decode_one(
    const char *module, const char *name, turbo_flow_protocol_kind_t protocol_kind,
    const char *version, const char *device_id,
    const uint8_t *frame_data, size_t frame_size,
    uint32_t expected_message_type, uint32_t expected_semantic_type,
    const char *expected_media_type, const uint8_t *expected_semantic,
    size_t expected_semantic_size) {
  turbo_flow_plugin_host_t *host = NULL;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_info_t info = TURBO_FLOW_PROTOCOL_INFO_INIT;
  turbo_flow_protocol_frame_view_t frame = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  turbo_flow_protocol_message_output_t raw = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  turbo_flow_protocol_semantic_output_t semantic = TURBO_FLOW_PROTOCOL_SEMANTIC_OUTPUT_INIT;
  uint8_t raw_bytes[512];
  uint8_t semantic_bytes[256];
  int rc = protocol_open_one(module, name, protocol_kind, version,
                             &host, &registry, &owner, &protocol);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_protocol_get_info(protocol, &info);
  if (rc != SALTS_OK) goto done;
  if ((info.capabilities & TURBO_FLOW_PROTOCOL_CAP_SEMANTIC_DECODE) == 0u) {
    rc = SALTS_EPROTO;
    goto done;
  }
  frame.data = frame_data;
  frame.data_size = frame_size;
  frame.device_id = device_id;
  frame.protocol_version = version;
  raw.payload = raw_bytes;
  raw.payload_capacity = sizeof(raw_bytes);
  semantic.data = semantic_bytes;
  semantic.capacity = sizeof(semantic_bytes);
  rc = turbo_flow_protocol_decode_semantic(protocol, &frame, &raw, &semantic);
  if (rc != SALTS_OK) goto done;
  if (raw.payload_size != frame_size ||
      memcmp(raw.payload, frame_data, frame_size) != 0 ||
      raw.metadata.message_type != expected_message_type ||
      semantic.semantic_type != expected_semantic_type ||
      strcmp(semantic.media_type, expected_media_type) != 0 ||
      semantic.data_size != expected_semantic_size ||
      memcmp(semantic.data, expected_semantic, expected_semantic_size) != 0)
    rc = SALTS_EPROTO;

done:
  protocol_close_one(host, registry, owner);
  return rc;
}

static int protocol_roundtrip(turbo_flow_protocol_registry_t *registry,
                              const protocol_case_t *test_case) {
  uint8_t payload[256];
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_frame_view_t frame = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  turbo_flow_protocol_message_output_t decoded = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  int rc;
  request.protocol = test_case->protocol;
  request.protocol_version = test_case->version;
  rc = turbo_flow_protocol_owner_create_registered(registry, test_case->name, &request, &owner);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_protocol_owner_instance(owner, test_case->protocol, &protocol);
  if (rc != SALTS_OK) goto done;
  frame.data = test_case->frame;
  frame.data_size = test_case->frame_size;
  frame.device_id = test_case->device_id;
  frame.protocol_version = test_case->version;
  decoded.payload = payload;
  decoded.payload_capacity = sizeof(payload);
  rc = turbo_flow_protocol_decode(protocol, &frame, &decoded);
  if (rc != SALTS_OK) goto done;
  if (decoded.payload_size != test_case->frame_size ||
      memcmp(decoded.payload, test_case->frame, test_case->frame_size) != 0 ||
      strcmp(decoded.metadata.operation, test_case->operation) != 0 ||
      (test_case->device_id && strcmp(decoded.metadata.device_id, test_case->device_id) != 0))
    rc = SALTS_EPROTO;

done:
  turbo_flow_protocol_owner_destroy(owner);
  return rc;
}

static int protocol_reject_frame(turbo_flow_protocol_registry_t *registry,
                                 const protocol_case_t *test_case, const uint8_t *frame_data,
                                 size_t frame_size) {
  uint8_t payload[256];
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_frame_view_t frame = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  turbo_flow_protocol_message_output_t decoded = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  int rc;
  request.protocol = test_case->protocol;
  request.protocol_version = test_case->version;
  rc = turbo_flow_protocol_owner_create_registered(registry, test_case->name, &request, &owner);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_protocol_owner_instance(owner, test_case->protocol, &protocol);
  if (rc != SALTS_OK) goto done;
  frame.data = frame_data;
  frame.data_size = frame_size;
  frame.device_id = test_case->device_id;
  frame.protocol_version = test_case->version;
  decoded.payload = payload;
  decoded.payload_capacity = sizeof(payload);
  rc = turbo_flow_protocol_decode(protocol, &frame, &decoded);

done:
  turbo_flow_protocol_owner_destroy(owner);
  return rc;
}

static int protocol_registry_load_modules(const char *const *modules, size_t module_count,
                                          turbo_flow_plugin_host_t **host_out,
                                          turbo_flow_protocol_registry_t **registry_out) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_plugin_host_t *host = NULL;
  turbo_flow_protocol_registry_t *registry = NULL;
  int rc;
  if (!modules || module_count == 0u || !host_out || !registry_out) return SALTS_EINVAL;
  *host_out = NULL;
  *registry_out = NULL;
  config.module_capacity = module_count;
  config.adapter_provider_capacity = 0u;
  config.resource_provider_capacity = 0u;
  config.protocol_provider_capacity = module_count;
  config.business_provider_capacity = 0u;
  rc = turbo_flow_plugin_host_create(&config, &host, &error);
  for (size_t i = 0u; rc == SALTS_OK && i < module_count; ++i)
    rc = turbo_flow_plugin_host_load(host, modules[i], &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error);
  if (rc == SALTS_OK) rc = turbo_flow_protocol_registry_create(snapshot, &registry);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  if (rc != SALTS_OK) {
    turbo_flow_plugin_error_t shutdown_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    if (registry) (void)turbo_flow_protocol_registry_destroy(registry);
    if (host) (void)turbo_flow_plugin_host_destroy(host, 1000u, &shutdown_error);
    return rc;
  }
  *host_out = host;
  *registry_out = registry;
  return SALTS_OK;
}

static int protocol_open_one(const char *module, const char *name,
                             turbo_flow_protocol_kind_t protocol, const char *version,
                             turbo_flow_plugin_host_t **host_out,
                             turbo_flow_protocol_registry_t **registry_out,
                             turbo_flow_protocol_owner_t **owner_out,
                             turbo_flow_protocol_t **protocol_out) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_plugin_host_t *host = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  int rc;
  *host_out = NULL;
  *registry_out = NULL;
  *owner_out = NULL;
  *protocol_out = NULL;
  request.protocol = protocol;
  request.protocol_version = version;
  rc = protocol_registry_load_modules(&module, 1u, &host, &registry);
  if (rc == SALTS_OK)
    rc = turbo_flow_protocol_owner_create_registered(registry, name, &request, &owner);
  if (rc == SALTS_OK) rc = turbo_flow_protocol_owner_instance(owner, protocol, protocol_out);
  if (rc != SALTS_OK) {
    turbo_flow_protocol_owner_destroy(owner);
    if (registry) (void)turbo_flow_protocol_registry_destroy(registry);
    if (host) {
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      (void)turbo_flow_plugin_host_destroy(host, 1000u, &error);
    }
    return rc;
  }
  *host_out = host;
  *registry_out = registry;
  *owner_out = owner;
  return SALTS_OK;
}

static void protocol_close_one(turbo_flow_plugin_host_t *host,
                               turbo_flow_protocol_registry_t *registry,
                               turbo_flow_protocol_owner_t *owner) {
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_protocol_owner_destroy(owner);
  if (registry) (void)turbo_flow_protocol_registry_destroy(registry);
  if (host) (void)turbo_flow_plugin_host_destroy(host, 1000u, &error);
}

spec("protocol plugin conformance") {
  it("loads six equal DLLs and raw-preserves each protocol frame") {
    static const uint8_t mqtt_sn[] = {10u, 0x04u, 0x00u, 0x01u, 0x00u, 30u, 'd', 'e', 'v', '1'};
    static const uint8_t coap[] = {0x40u, 0x01u, 0x12u, 0x34u};
    static const uint8_t lwm2m[] = {0x40u, 0x03u, 0x00u, 0x01u};
    static const uint8_t ocpp[] = "[2,\"42\",\"BootNotification\",{}]";
    uint8_t gbt32960[25];
    uint8_t jtt808[64];
    const size_t gbt32960_size = protocol_gbt32960_frame(gbt32960, sizeof(gbt32960));
    const size_t jtt808_size = protocol_jtt808_frame(jtt808, sizeof(jtt808));
    protocol_case_t cases[] = {
        {FLOW_PROTOCOL_MQTT_SN_MODULE, "mqtt-sn", TURBO_FLOW_PROTOCOL_MQTT_SN, "1.2", "sensor-1",
         "connect", mqtt_sn, sizeof(mqtt_sn)},
        {FLOW_PROTOCOL_COAP_MODULE, "coap", TURBO_FLOW_PROTOCOL_COAP, "RFC7252", "sensor-2", "get",
         coap, sizeof(coap)},
        {FLOW_PROTOCOL_LWM2M_MODULE, "lwm2m", TURBO_FLOW_PROTOCOL_LWM2M, "1.2.2", "device-3",
         "write", lwm2m, sizeof(lwm2m)},
        {FLOW_PROTOCOL_OCPP_MODULE, "ocpp", TURBO_FLOW_PROTOCOL_OCPP, "1.6J", "charger-4",
         "BootNotification", ocpp, sizeof(ocpp) - 1u},
        {FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960", TURBO_FLOW_PROTOCOL_GBT_32960, "2025", NULL,
         "realtime-data", gbt32960, gbt32960_size},
        {FLOW_PROTOCOL_JTT808_MODULE, "jtt808", TURBO_FLOW_PROTOCOL_JTT_808, "2019-A1", NULL,
         "location", jtt808, jtt808_size}};
    const char *modules[] = {FLOW_PROTOCOL_MQTT_SN_MODULE,  FLOW_PROTOCOL_COAP_MODULE,
                             FLOW_PROTOCOL_LWM2M_MODULE,    FLOW_PROTOCOL_OCPP_MODULE,
                             FLOW_PROTOCOL_GBT32960_MODULE, FLOW_PROTOCOL_JTT808_MODULE};
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_protocol_registry_t *registry = NULL;
    check_equal(gbt32960_size, 25u);
    check_true(jtt808_size > 0u);
    check_equal(protocol_registry_load_modules(modules, sizeof(modules) / sizeof(modules[0]), &host,
                                               &registry),
                SALTS_OK);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      check_equal(protocol_roundtrip(registry, &cases[i]), SALTS_OK);
    }
    protocol_close_one(host, registry, NULL);
  }

  it("rejects an unsupported negotiated protocol version before opening") {
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.protocol_version = "2.1";
    {
      const char *module = FLOW_PROTOCOL_OCPP_MODULE;
      check_equal(protocol_registry_load_modules(&module, 1u, &host, &registry), SALTS_OK);
    }
    check_equal(turbo_flow_protocol_owner_create_registered(registry, "ocpp", &request, &owner),
                SALTS_ENOTSUP);
    check_null(owner);
    protocol_close_one(host, registry, NULL);
  }

  it("rejects malformed frames at every protocol boundary") {
    static const uint8_t bad_mqtt_sn[] = {9u, 0x04u, 0x00u, 0x01u, 0x00u, 30u, 'd', 'e', 'v', '1'};
    static const uint8_t bad_coap[] = {0x80u, 0x01u, 0x12u, 0x34u};
    static const uint8_t bad_lwm2m[] = {0x49u, 0x03u, 0x00u, 0x01u};
    static const uint8_t bad_ocpp[] = "[5,\"42\",{}]";
    uint8_t bad_gbt32960[25];
    uint8_t bad_jtt808[64];
    protocol_case_t cases[] = {
        {FLOW_PROTOCOL_MQTT_SN_MODULE, "mqtt-sn", TURBO_FLOW_PROTOCOL_MQTT_SN, "1.2", "sensor-1",
         NULL, bad_mqtt_sn, sizeof(bad_mqtt_sn)},
        {FLOW_PROTOCOL_COAP_MODULE, "coap", TURBO_FLOW_PROTOCOL_COAP, "RFC7252", "sensor-2", NULL,
         bad_coap, sizeof(bad_coap)},
        {FLOW_PROTOCOL_LWM2M_MODULE, "lwm2m", TURBO_FLOW_PROTOCOL_LWM2M, "1.2.2", "device-3", NULL,
         bad_lwm2m, sizeof(bad_lwm2m)},
        {FLOW_PROTOCOL_OCPP_MODULE, "ocpp", TURBO_FLOW_PROTOCOL_OCPP, "2.0.1", "charger-4", NULL,
         bad_ocpp, sizeof(bad_ocpp) - 1u},
        {FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960", TURBO_FLOW_PROTOCOL_GBT_32960, "2025", NULL,
         NULL, bad_gbt32960, sizeof(bad_gbt32960)},
        {FLOW_PROTOCOL_JTT808_MODULE, "jtt808", TURBO_FLOW_PROTOCOL_JTT_808, "2019-A1", NULL, NULL,
         bad_jtt808, 0u}};
    const char *modules[] = {FLOW_PROTOCOL_MQTT_SN_MODULE,  FLOW_PROTOCOL_COAP_MODULE,
                             FLOW_PROTOCOL_LWM2M_MODULE,    FLOW_PROTOCOL_OCPP_MODULE,
                             FLOW_PROTOCOL_GBT32960_MODULE, FLOW_PROTOCOL_JTT808_MODULE};
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_protocol_registry_t *registry = NULL;
    check_equal(protocol_gbt32960_frame(bad_gbt32960, sizeof(bad_gbt32960)), sizeof(bad_gbt32960));
    bad_gbt32960[24] ^= 0x01u;
    cases[5].frame_size = protocol_jtt808_frame(bad_jtt808, sizeof(bad_jtt808));
    check_true(cases[5].frame_size > 3u);
    bad_jtt808[cases[5].frame_size - 2u] ^= 0x01u;
    check_equal(protocol_registry_load_modules(modules, sizeof(modules) / sizeof(modules[0]), &host,
                                               &registry),
                SALTS_OK);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      check_equal(protocol_reject_frame(registry, &cases[i], cases[i].frame, cases[i].frame_size),
                  SALTS_EPROTO);
    }
    protocol_close_one(host, registry, NULL);
  }

  it("emits protocol responses only after an explicit settlement result") {
    static const uint8_t mqtt_sn[] = {7u, 0x0cu, 0x20u, 0x00u, 0x01u, 0x00u, 0x09u};
    static const uint8_t mqtt_sn_ack[] = {7u, 0x0du, 0x00u, 0x01u, 0x00u, 0x09u, 0x00u};
    static const uint8_t coap[] = {0x41u, 0x01u, 0x12u, 0x34u, 0xaau};
    static const uint8_t coap_ack[] = {0x61u, 0x45u, 0x12u, 0x34u, 0xaau};
    static const uint8_t lwm2m[] = {0x40u, 0x03u, 0x00u, 0x01u};
    static const uint8_t lwm2m_ack[] = {0x60u, 0x44u, 0x00u, 0x01u};
    static const uint8_t ocpp[] = "[2,\"42\",\"BootNotification\",{}]";
    static const uint8_t ocpp_result[] = "[3,\"42\",{}]";
    uint8_t gbt32960[25];
    uint8_t jtt808[64];
    uint8_t response[256];
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_frame_view_t request = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    turbo_flow_protocol_frame_output_t output = TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);

    check_equal(protocol_open_one(FLOW_PROTOCOL_MQTT_SN_MODULE, "mqtt-sn",
                                  TURBO_FLOW_PROTOCOL_MQTT_SN, "1.2", &host, &registry, &owner,
                                  &protocol),
                SALTS_OK);
    request.data = mqtt_sn;
    request.data_size = sizeof(mqtt_sn);
    request.device_id = "sensor-1";
    request.protocol_version = "1.2";
    check_equal(turbo_flow_protocol_reply(protocol, &request, SALTS_OK, &output), SALTS_OK);
    check_equal(output.data_size, sizeof(mqtt_sn_ack));
    check_equal(output.data, mqtt_sn_ack, sizeof(mqtt_sn_ack));
    protocol_close_one(host, registry, owner);

    host = NULL;
    registry = NULL;
    owner = NULL;
    protocol = NULL;
    output = (turbo_flow_protocol_frame_output_t)TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_equal(protocol_open_one(FLOW_PROTOCOL_COAP_MODULE, "coap", TURBO_FLOW_PROTOCOL_COAP,
                                  "RFC7252", &host, &registry, &owner, &protocol),
                SALTS_OK);
    request = (turbo_flow_protocol_frame_view_t)TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    request.data = coap;
    request.data_size = sizeof(coap);
    request.device_id = "sensor-2";
    request.protocol_version = "RFC7252";
    check_equal(turbo_flow_protocol_reply(protocol, &request, SALTS_OK, &output), SALTS_OK);
    check_equal(output.data_size, sizeof(coap_ack));
    check_equal(output.data, coap_ack, sizeof(coap_ack));
    protocol_close_one(host, registry, owner);

    host = NULL;
    registry = NULL;
    owner = NULL;
    protocol = NULL;
    output = (turbo_flow_protocol_frame_output_t)TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_equal(protocol_open_one(FLOW_PROTOCOL_LWM2M_MODULE, "lwm2m", TURBO_FLOW_PROTOCOL_LWM2M,
                                  "1.2.2", &host, &registry, &owner, &protocol),
                SALTS_OK);
    request = (turbo_flow_protocol_frame_view_t)TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    request.data = lwm2m;
    request.data_size = sizeof(lwm2m);
    request.device_id = "device-3";
    request.protocol_version = "1.2.2";
    check_equal(turbo_flow_protocol_reply(protocol, &request, SALTS_OK, &output), SALTS_OK);
    check_equal(output.data_size, sizeof(lwm2m_ack));
    check_equal(output.data, lwm2m_ack, sizeof(lwm2m_ack));
    protocol_close_one(host, registry, owner);

    host = NULL;
    registry = NULL;
    owner = NULL;
    protocol = NULL;
    output = (turbo_flow_protocol_frame_output_t)TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_equal(protocol_open_one(FLOW_PROTOCOL_OCPP_MODULE, "ocpp", TURBO_FLOW_PROTOCOL_OCPP,
                                  "1.6J", &host, &registry, &owner, &protocol),
                SALTS_OK);
    request = (turbo_flow_protocol_frame_view_t)TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    request.data = ocpp;
    request.data_size = sizeof(ocpp) - 1u;
    request.device_id = "charger-4";
    request.protocol_version = "1.6J";
    check_equal(turbo_flow_protocol_reply(protocol, &request, SALTS_OK, &output), SALTS_OK);
    check_equal(output.data_size, sizeof(ocpp_result) - 1u);
    check_equal(output.data, ocpp_result, sizeof(ocpp_result) - 1u);
    protocol_close_one(host, registry, owner);

    check_equal(protocol_gbt32960_frame(gbt32960, sizeof(gbt32960)), sizeof(gbt32960));
    host = NULL;
    registry = NULL;
    owner = NULL;
    protocol = NULL;
    output = (turbo_flow_protocol_frame_output_t)TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_equal(protocol_open_one(FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960",
                                  TURBO_FLOW_PROTOCOL_GBT_32960, "2025", &host, &registry, &owner,
                                  &protocol),
                SALTS_OK);
    request = (turbo_flow_protocol_frame_view_t)TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    request.data = gbt32960;
    request.data_size = sizeof(gbt32960);
    request.protocol_version = "2025";
    check_equal(turbo_flow_protocol_reply(protocol, &request, SALTS_OK, &output), SALTS_OK);
    check_equal(output.data_size, 25u);
    check_equal(output.data[3], 0x01u);
    protocol_close_one(host, registry, owner);

    check_true(protocol_jtt808_frame(jtt808, sizeof(jtt808)) > 0u);
    host = NULL;
    registry = NULL;
    owner = NULL;
    protocol = NULL;
    output = (turbo_flow_protocol_frame_output_t)TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_equal(protocol_open_one(FLOW_PROTOCOL_JTT808_MODULE, "jtt808",
                                  TURBO_FLOW_PROTOCOL_JTT_808, "2019-A1", &host, &registry, &owner,
                                  &protocol),
                SALTS_OK);
    request = (turbo_flow_protocol_frame_view_t)TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    request.data = jtt808;
    request.data_size = protocol_jtt808_frame(jtt808, sizeof(jtt808));
    request.protocol_version = "2019-A1";
    check_equal(turbo_flow_protocol_reply(protocol, &request, SALTS_OK, &output), SALTS_OK);
    check_true(output.data_size > 0u);
    check_equal(output.metadata.operation, "platform-ack");
    protocol_close_one(host, registry, owner);
  }

  it("extracts identical application JSON through real CoAP and JT/T808 codecs") {
    static const uint8_t json[] = "{\"age\":21}";
    uint8_t coap[64];
    uint8_t jtt808[128];
    size_t coap_size = 0u;
    size_t jtt808_size;

    coap[coap_size++] = 0x40u;
    coap[coap_size++] = 0x02u;
    coap[coap_size++] = 0x12u;
    coap[coap_size++] = 0x34u;
    coap[coap_size++] = 0xc1u; /* Content-Format, one-byte value */
    coap[coap_size++] = 50u;   /* application/json */
    coap[coap_size++] = 0xffu;
    memcpy(coap + coap_size, json, sizeof(json) - 1u);
    coap_size += sizeof(json) - 1u;

    jtt808_size = protocol_jtt808_transparent_json_frame(
        jtt808, sizeof(jtt808), json, sizeof(json) - 1u);
    check_true(jtt808_size > 0u);

    check_equal(protocol_semantic_decode_one(
                    FLOW_PROTOCOL_COAP_MODULE, "coap", TURBO_FLOW_PROTOCOL_COAP,
                    "RFC7252", "sensor-2", coap, coap_size,
                    2u, 50u, "application/json", json, sizeof(json) - 1u),
                SALTS_OK);
    check_equal(protocol_semantic_decode_one(
                    FLOW_PROTOCOL_JTT808_MODULE, "jtt808", TURBO_FLOW_PROTOCOL_JTT_808,
                    "2019-A1", NULL, jtt808, jtt808_size,
                    UINT32_C(0x0900), 1u, "application/json",
                    json, sizeof(json) - 1u),
                SALTS_OK);
  }

  it("encodes bounded semantic downlink commands for every plugin") {
    static const uint8_t empty_object[] = "{}";
    static const uint8_t value[] = {0x41u, 0x42u};
    struct command_case_s {
      const char *module;
      const char *name;
      turbo_flow_protocol_kind_t protocol;
      const char *version;
      const char *device_id;
      const char *operation;
      const char *resource;
      const char *correlation;
      uint64_t sequence;
      const uint8_t *payload;
      size_t payload_size;
    } cases[] = {{FLOW_PROTOCOL_MQTT_SN_MODULE, "mqtt-sn", TURBO_FLOW_PROTOCOL_MQTT_SN, "1.2",
                  "sensor-1", "publish", "7", NULL, 9u, value, sizeof(value)},
                 {FLOW_PROTOCOL_COAP_MODULE, "coap", TURBO_FLOW_PROTOCOL_COAP, "RFC7252",
                  "sensor-2", "get", "/sensors/1", "tk", 0x1234u, NULL, 0u},
                 {FLOW_PROTOCOL_LWM2M_MODULE, "lwm2m", TURBO_FLOW_PROTOCOL_LWM2M, "1.2.2",
                  "device-3", "read", "/3/0/13", "lm", 0x1235u, NULL, 0u},
                 {FLOW_PROTOCOL_OCPP_MODULE, "ocpp", TURBO_FLOW_PROTOCOL_OCPP, "2.0.1", "charger-4",
                  "BootNotification", NULL, "call-1", 1u, empty_object, sizeof(empty_object) - 1u},
                 {FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960", TURBO_FLOW_PROTOCOL_GBT_32960, "2025",
                  "L1234567890123456", "heartbeat", NULL, NULL, 1u, NULL, 0u},
                 {FLOW_PROTOCOL_JTT808_MODULE, "jtt808", TURBO_FLOW_PROTOCOL_JTT_808, "2019-A1",
                  "00000000000123450001", "location-query", NULL, NULL, 7u, NULL, 0u}};
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_protocol_registry_t *registry = NULL;
      turbo_flow_plugin_host_t *host = NULL;
      turbo_flow_protocol_owner_t *owner = NULL;
      turbo_flow_protocol_t *protocol = NULL;
      turbo_flow_protocol_command_view_t command = TURBO_FLOW_PROTOCOL_COMMAND_VIEW_INIT;
      turbo_flow_protocol_frame_output_t output = TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
      uint8_t frame[512];
      check_equal(protocol_open_one(cases[i].module, cases[i].name, cases[i].protocol,
                                    cases[i].version, &host, &registry, &owner, &protocol),
                  SALTS_OK);
      command.device_id = cases[i].device_id;
      command.operation = cases[i].operation;
      command.resource = cases[i].resource;
      command.correlation_id = cases[i].correlation;
      command.sequence = cases[i].sequence;
      command.payload = cases[i].payload;
      command.payload_size = cases[i].payload_size;
      output.data = frame;
      output.capacity = sizeof(frame);
      check_equal(turbo_flow_protocol_encode(protocol, &command, &output), SALTS_OK);
      check_true(output.data_size > 0u);
      check_equal(output.metadata.operation, cases[i].operation);
      check_equal(output.metadata.device_id, cases[i].device_id);
      protocol_close_one(host, registry, owner);
    }
  }
}
