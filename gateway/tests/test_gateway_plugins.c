#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_gateway.h"

#include <stdio.h>
#include <string.h>

typedef struct gateway_case_s {
  const char *module;
  const char *name;
  turbo_flow_gateway_protocol_t protocol;
  const char *version;
  const char *device_id;
  const char *operation;
  const uint8_t *frame;
  size_t frame_size;
} gateway_case_t;

static size_t gateway_gbt32960_frame(uint8_t *out, size_t capacity) {
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
  for (size_t i = 2u; i < 24u; ++i) checksum ^= out[i];
  out[24] = checksum;
  return 25u;
}

static size_t gateway_jtt808_frame(uint8_t *out, size_t capacity) {
  const uint8_t unescaped[] = {
      0x02u, 0x00u, 0x40u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u,
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

static int gateway_roundtrip(turbo_flow_gateway_registry_t *registry,
                             const gateway_case_t *test_case) {
  char topic[256];
  char down_topic[256];
  uint8_t mqtt_payload[256];
  uint8_t restored[256];
  turbo_flow_gateway_open_request_t request =
      TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
  turbo_flow_gateway_owner_t *owner = NULL;
  turbo_flow_gateway_t *gateway = NULL;
  turbo_flow_gateway_frame_view_t frame =
      TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
  turbo_flow_gateway_mqtt_output_t mapped =
      TURBO_FLOW_GATEWAY_MQTT_OUTPUT_INIT;
  turbo_flow_gateway_mqtt_view_t down =
      TURBO_FLOW_GATEWAY_MQTT_VIEW_INIT;
  turbo_flow_gateway_frame_output_t output =
      TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
  int written;
  int rc;
  request.protocol = test_case->protocol;
  request.protocol_version = test_case->version;
  request.tenant = "fleet-a";
  rc = turbo_flow_gateway_owner_create_registered(
      registry, test_case->name, &request, &owner);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_gateway_owner_instance(owner, test_case->protocol, &gateway);
  if (rc != TURBO_OK) goto done;
  frame.data = test_case->frame;
  frame.data_size = test_case->frame_size;
  frame.device_id = test_case->device_id;
  frame.protocol_version = test_case->version;
  mapped.topic = topic;
  mapped.topic_capacity = sizeof(topic);
  mapped.payload = mqtt_payload;
  mapped.payload_capacity = sizeof(mqtt_payload);
  rc = turbo_flow_gateway_ingress(gateway, &frame, &mapped);
  if (rc != TURBO_OK) goto done;
  written = snprintf(down_topic, sizeof(down_topic),
                     "gateway/%s/fleet-a/%s/down/%s", test_case->name,
                     mapped.metadata.device_id, test_case->operation);
  if (written <= 0 || (size_t)written >= sizeof(down_topic)) {
    rc = TURBO_EMSGSIZE;
    goto done;
  }
  down.topic = down_topic;
  down.topic_size = (size_t)written;
  down.payload = mapped.payload;
  down.payload_size = mapped.payload_size;
  output.data = restored;
  output.capacity = sizeof(restored);
  rc = turbo_flow_gateway_egress(gateway, &down, &output);
  if (rc != TURBO_OK) goto done;
  if (output.data_size != test_case->frame_size ||
      memcmp(output.data, test_case->frame, test_case->frame_size) != 0 ||
      strcmp(mapped.metadata.operation, test_case->operation) != 0 ||
      strcmp(output.metadata.device_id, mapped.metadata.device_id) != 0)
    rc = TURBO_EPROTO;

done:
  turbo_flow_gateway_owner_destroy(owner);
  return rc;
}

static int gateway_reject_frame(turbo_flow_gateway_registry_t *registry,
                                const gateway_case_t *test_case,
                                const uint8_t *frame_data,
                                size_t frame_size) {
  char topic[256];
  uint8_t payload[256];
  turbo_flow_gateway_open_request_t request =
      TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
  turbo_flow_gateway_owner_t *owner = NULL;
  turbo_flow_gateway_t *gateway = NULL;
  turbo_flow_gateway_frame_view_t frame =
      TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
  turbo_flow_gateway_mqtt_output_t mapped =
      TURBO_FLOW_GATEWAY_MQTT_OUTPUT_INIT;
  int rc;
  request.protocol = test_case->protocol;
  request.protocol_version = test_case->version;
  request.tenant = "fleet-a";
  rc = turbo_flow_gateway_owner_create_registered(
      registry, test_case->name, &request, &owner);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_gateway_owner_instance(owner, test_case->protocol, &gateway);
  if (rc != TURBO_OK) goto done;
  frame.data = frame_data;
  frame.data_size = frame_size;
  frame.device_id = test_case->device_id;
  frame.protocol_version = test_case->version;
  mapped.topic = topic;
  mapped.topic_capacity = sizeof(topic);
  mapped.payload = payload;
  mapped.payload_capacity = sizeof(payload);
  rc = turbo_flow_gateway_ingress(gateway, &frame, &mapped);

done:
  turbo_flow_gateway_owner_destroy(owner);
  return rc;
}

static int gateway_open_one(
    const char *module, const char *name,
    turbo_flow_gateway_protocol_t protocol, const char *version,
    turbo_flow_gateway_registry_t **registry_out,
    turbo_flow_gateway_owner_t **owner_out,
    turbo_flow_gateway_t **gateway_out) {
  turbo_flow_gateway_open_request_t request =
      TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
  turbo_flow_gateway_registry_t *registry = NULL;
  turbo_flow_gateway_owner_t *owner = NULL;
  char reason[256];
  int rc;
  *registry_out = NULL;
  *owner_out = NULL;
  *gateway_out = NULL;
  request.protocol = protocol;
  request.protocol_version = version;
  rc = turbo_flow_gateway_registry_create(1u, &registry);
  if (rc == TURBO_OK)
    rc = turbo_flow_gateway_registry_load(registry, module, reason,
                                          sizeof(reason));
  if (rc == TURBO_OK)
    rc = turbo_flow_gateway_owner_create_registered(
        registry, name, &request, &owner);
  if (rc == TURBO_OK)
    rc = turbo_flow_gateway_owner_instance(owner, protocol, gateway_out);
  if (rc != TURBO_OK) {
    turbo_flow_gateway_owner_destroy(owner);
    if (registry) (void)turbo_flow_gateway_registry_destroy(registry);
    return rc;
  }
  *registry_out = registry;
  *owner_out = owner;
  return TURBO_OK;
}

static void gateway_close_one(turbo_flow_gateway_registry_t *registry,
                              turbo_flow_gateway_owner_t *owner) {
  turbo_flow_gateway_owner_destroy(owner);
  if (registry) (void)turbo_flow_gateway_registry_destroy(registry);
}

spec("gateway plugin conformance") {
  it("loads six equal DLLs and raw-preserves each protocol frame") {
    static const uint8_t mqtt_sn[] = {
        10u, 0x04u, 0x00u, 0x01u, 0x00u, 30u, 'd', 'e', 'v', '1'};
    static const uint8_t coap[] = {0x40u, 0x01u, 0x12u, 0x34u};
    static const uint8_t lwm2m[] = {0x40u, 0x03u, 0x00u, 0x01u};
    static const uint8_t ocpp[] =
        "[2,\"42\",\"BootNotification\",{}]";
    uint8_t gbt32960[25];
    uint8_t jtt808[64];
    const size_t gbt32960_size =
        gateway_gbt32960_frame(gbt32960, sizeof(gbt32960));
    const size_t jtt808_size =
        gateway_jtt808_frame(jtt808, sizeof(jtt808));
    gateway_case_t cases[] = {
        {FLOW_GATEWAY_MQTT_SN_MODULE, "mqtt-sn",
         TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN, "1.2", "sensor-1",
         "connect", mqtt_sn, sizeof(mqtt_sn)},
        {FLOW_GATEWAY_COAP_MODULE, "coap", TURBO_FLOW_GATEWAY_PROTOCOL_COAP,
         "RFC7252", "sensor-2", "get", coap, sizeof(coap)},
        {FLOW_GATEWAY_LWM2M_MODULE, "lwm2m",
         TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M, "1.2.2", "device-3", "write",
         lwm2m, sizeof(lwm2m)},
        {FLOW_GATEWAY_OCPP_MODULE, "ocpp", TURBO_FLOW_GATEWAY_PROTOCOL_OCPP,
         "1.6J", "charger-4", "BootNotification", ocpp,
         sizeof(ocpp) - 1u},
        {FLOW_GATEWAY_GBT32960_MODULE, "gbt32960",
         TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960, "2025", NULL,
         "realtime-data", gbt32960, gbt32960_size},
        {FLOW_GATEWAY_JTT808_MODULE, "jtt808",
         TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808, "2019-A1", NULL, "location",
         jtt808, jtt808_size}};
    turbo_flow_gateway_registry_t *registry = NULL;
    char reason[256];
    check_size_eq(gbt32960_size, 25u);
    check_true(jtt808_size > 0u);
    check_int_eq(turbo_flow_gateway_registry_create(
                     sizeof(cases) / sizeof(cases[0]), &registry),
                 TURBO_OK);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      check_int_eq(turbo_flow_gateway_registry_load(
                       registry, cases[i].module, reason, sizeof(reason)),
                   TURBO_OK);
      check_int_eq(gateway_roundtrip(registry, &cases[i]), TURBO_OK);
    }
    check_int_eq(turbo_flow_gateway_registry_destroy(registry), TURBO_OK);
  }

  it("rejects an unsupported negotiated protocol version before opening") {
    turbo_flow_gateway_registry_t *registry = NULL;
    turbo_flow_gateway_owner_t *owner = NULL;
    turbo_flow_gateway_open_request_t request =
        TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
    char reason[256];
    request.protocol = TURBO_FLOW_GATEWAY_PROTOCOL_OCPP;
    request.protocol_version = "2.1";
    check_int_eq(turbo_flow_gateway_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_gateway_registry_load(
                     registry, FLOW_GATEWAY_OCPP_MODULE, reason,
                     sizeof(reason)),
                 TURBO_OK);
    check_int_eq(turbo_flow_gateway_owner_create_registered(
                     registry, "ocpp", &request, &owner),
                 TURBO_ENOTSUP);
    check_null(owner);
    check_int_eq(turbo_flow_gateway_registry_destroy(registry), TURBO_OK);
  }

  it("rejects malformed frames at every protocol boundary") {
    static const uint8_t bad_mqtt_sn[] = {
        9u, 0x04u, 0x00u, 0x01u, 0x00u, 30u, 'd', 'e', 'v', '1'};
    static const uint8_t bad_coap[] = {0x80u, 0x01u, 0x12u, 0x34u};
    static const uint8_t bad_lwm2m[] = {0x49u, 0x03u, 0x00u, 0x01u};
    static const uint8_t bad_ocpp[] = "[5,\"42\",{}]";
    uint8_t bad_gbt32960[25];
    uint8_t bad_jtt808[64];
    gateway_case_t cases[] = {
        {FLOW_GATEWAY_MQTT_SN_MODULE, "mqtt-sn",
         TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN, "1.2", "sensor-1", NULL,
         bad_mqtt_sn, sizeof(bad_mqtt_sn)},
        {FLOW_GATEWAY_COAP_MODULE, "coap", TURBO_FLOW_GATEWAY_PROTOCOL_COAP,
         "RFC7252", "sensor-2", NULL, bad_coap, sizeof(bad_coap)},
        {FLOW_GATEWAY_LWM2M_MODULE, "lwm2m",
         TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M, "1.2.2", "device-3", NULL,
         bad_lwm2m, sizeof(bad_lwm2m)},
        {FLOW_GATEWAY_OCPP_MODULE, "ocpp", TURBO_FLOW_GATEWAY_PROTOCOL_OCPP,
         "2.0.1", "charger-4", NULL, bad_ocpp, sizeof(bad_ocpp) - 1u},
        {FLOW_GATEWAY_GBT32960_MODULE, "gbt32960",
         TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960, "2025", NULL, NULL,
         bad_gbt32960, sizeof(bad_gbt32960)},
        {FLOW_GATEWAY_JTT808_MODULE, "jtt808",
         TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808, "2019-A1", NULL, NULL,
         bad_jtt808, 0u}};
    turbo_flow_gateway_registry_t *registry = NULL;
    char reason[256];
    check_size_eq(gateway_gbt32960_frame(bad_gbt32960,
                                         sizeof(bad_gbt32960)),
                  sizeof(bad_gbt32960));
    bad_gbt32960[24] ^= 0x01u;
    cases[5].frame_size =
        gateway_jtt808_frame(bad_jtt808, sizeof(bad_jtt808));
    check_true(cases[5].frame_size > 3u);
    bad_jtt808[cases[5].frame_size - 2u] ^= 0x01u;
    check_int_eq(turbo_flow_gateway_registry_create(
                     sizeof(cases) / sizeof(cases[0]), &registry),
                 TURBO_OK);
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      check_int_eq(turbo_flow_gateway_registry_load(
                       registry, cases[i].module, reason, sizeof(reason)),
                   TURBO_OK);
      check_int_eq(gateway_reject_frame(registry, &cases[i], cases[i].frame,
                                        cases[i].frame_size),
                   TURBO_EPROTO);
    }
    check_int_eq(turbo_flow_gateway_registry_destroy(registry), TURBO_OK);
  }

  it("rejects a downlink topic whose operation disagrees with the frame") {
    static const uint8_t coap[] = {0x40u, 0x01u, 0x12u, 0x34u};
    turbo_flow_gateway_registry_t *registry = NULL;
    turbo_flow_gateway_owner_t *owner = NULL;
    turbo_flow_gateway_t *gateway = NULL;
    turbo_flow_gateway_open_request_t request =
        TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
    turbo_flow_gateway_mqtt_view_t message =
        TURBO_FLOW_GATEWAY_MQTT_VIEW_INIT;
    turbo_flow_gateway_frame_output_t output =
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    uint8_t restored[sizeof(coap)];
    char reason[256];
    request.protocol = TURBO_FLOW_GATEWAY_PROTOCOL_COAP;
    request.protocol_version = "RFC7252";
    request.tenant = "fleet-a";
    message.topic =
        "gateway/coap/fleet-a/sensor-2/down/delete";
    message.topic_size = strlen(message.topic);
    message.payload = coap;
    message.payload_size = sizeof(coap);
    output.data = restored;
    output.capacity = sizeof(restored);
    check_int_eq(turbo_flow_gateway_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_gateway_registry_load(
                     registry, FLOW_GATEWAY_COAP_MODULE, reason,
                     sizeof(reason)),
                 TURBO_OK);
    check_int_eq(turbo_flow_gateway_owner_create_registered(
                     registry, "coap", &request, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_gateway_owner_instance(
                     owner, TURBO_FLOW_GATEWAY_PROTOCOL_COAP, &gateway),
                 TURBO_OK);
    check_int_eq(turbo_flow_gateway_egress(gateway, &message, &output),
                 TURBO_EPROTO);
    check_size_eq(output.data_size, 0u);
    turbo_flow_gateway_owner_destroy(owner);
    check_int_eq(turbo_flow_gateway_registry_destroy(registry), TURBO_OK);
  }

  it("emits protocol responses only after an explicit settlement result") {
    static const uint8_t mqtt_sn[] = {
        7u, 0x0cu, 0x20u, 0x00u, 0x01u, 0x00u, 0x09u};
    static const uint8_t mqtt_sn_ack[] = {
        7u, 0x0du, 0x00u, 0x01u, 0x00u, 0x09u, 0x00u};
    static const uint8_t coap[] = {
        0x41u, 0x01u, 0x12u, 0x34u, 0xaau};
    static const uint8_t coap_ack[] = {
        0x61u, 0x45u, 0x12u, 0x34u, 0xaau};
    static const uint8_t lwm2m[] = {0x40u, 0x03u, 0x00u, 0x01u};
    static const uint8_t lwm2m_ack[] = {0x60u, 0x44u, 0x00u, 0x01u};
    static const uint8_t ocpp[] =
        "[2,\"42\",\"BootNotification\",{}]";
    static const uint8_t ocpp_result[] = "[3,\"42\",{}]";
    uint8_t gbt32960[25];
    uint8_t jtt808[64];
    uint8_t response[256];
    turbo_flow_gateway_registry_t *registry = NULL;
    turbo_flow_gateway_owner_t *owner = NULL;
    turbo_flow_gateway_t *gateway = NULL;
    turbo_flow_gateway_frame_view_t request =
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    turbo_flow_gateway_frame_output_t output =
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);

    check_int_eq(gateway_open_one(
                     FLOW_GATEWAY_MQTT_SN_MODULE, "mqtt-sn",
                     TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN, "1.2", &registry,
                     &owner, &gateway),
                 TURBO_OK);
    request.data = mqtt_sn;
    request.data_size = sizeof(mqtt_sn);
    request.device_id = "sensor-1";
    request.protocol_version = "1.2";
    check_int_eq(turbo_flow_gateway_reply(gateway, &request, TURBO_OK,
                                          &output),
                 TURBO_OK);
    check_size_eq(output.data_size, sizeof(mqtt_sn_ack));
    check_mem_eq(output.data, mqtt_sn_ack, sizeof(mqtt_sn_ack));
    gateway_close_one(registry, owner);

    registry = NULL;
    owner = NULL;
    gateway = NULL;
    output = (turbo_flow_gateway_frame_output_t)
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_int_eq(gateway_open_one(
                     FLOW_GATEWAY_COAP_MODULE, "coap",
                     TURBO_FLOW_GATEWAY_PROTOCOL_COAP, "RFC7252", &registry,
                     &owner, &gateway),
                 TURBO_OK);
    request = (turbo_flow_gateway_frame_view_t)
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    request.data = coap;
    request.data_size = sizeof(coap);
    request.device_id = "sensor-2";
    request.protocol_version = "RFC7252";
    check_int_eq(turbo_flow_gateway_reply(gateway, &request, TURBO_OK,
                                          &output),
                 TURBO_OK);
    check_size_eq(output.data_size, sizeof(coap_ack));
    check_mem_eq(output.data, coap_ack, sizeof(coap_ack));
    gateway_close_one(registry, owner);

    registry = NULL;
    owner = NULL;
    gateway = NULL;
    output = (turbo_flow_gateway_frame_output_t)
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_int_eq(gateway_open_one(
                     FLOW_GATEWAY_LWM2M_MODULE, "lwm2m",
                     TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M, "1.2.2", &registry,
                     &owner, &gateway),
                 TURBO_OK);
    request = (turbo_flow_gateway_frame_view_t)
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    request.data = lwm2m;
    request.data_size = sizeof(lwm2m);
    request.device_id = "device-3";
    request.protocol_version = "1.2.2";
    check_int_eq(turbo_flow_gateway_reply(gateway, &request, TURBO_OK,
                                          &output),
                 TURBO_OK);
    check_size_eq(output.data_size, sizeof(lwm2m_ack));
    check_mem_eq(output.data, lwm2m_ack, sizeof(lwm2m_ack));
    gateway_close_one(registry, owner);

    registry = NULL;
    owner = NULL;
    gateway = NULL;
    output = (turbo_flow_gateway_frame_output_t)
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_int_eq(gateway_open_one(
                     FLOW_GATEWAY_OCPP_MODULE, "ocpp",
                     TURBO_FLOW_GATEWAY_PROTOCOL_OCPP, "1.6J", &registry,
                     &owner, &gateway),
                 TURBO_OK);
    request = (turbo_flow_gateway_frame_view_t)
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    request.data = ocpp;
    request.data_size = sizeof(ocpp) - 1u;
    request.device_id = "charger-4";
    request.protocol_version = "1.6J";
    check_int_eq(turbo_flow_gateway_reply(gateway, &request, TURBO_OK,
                                          &output),
                 TURBO_OK);
    check_size_eq(output.data_size, sizeof(ocpp_result) - 1u);
    check_mem_eq(output.data, ocpp_result, sizeof(ocpp_result) - 1u);
    gateway_close_one(registry, owner);

    check_size_eq(gateway_gbt32960_frame(gbt32960, sizeof(gbt32960)),
                  sizeof(gbt32960));
    registry = NULL;
    owner = NULL;
    gateway = NULL;
    output = (turbo_flow_gateway_frame_output_t)
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_int_eq(gateway_open_one(
                     FLOW_GATEWAY_GBT32960_MODULE, "gbt32960",
                     TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960, "2025",
                     &registry, &owner, &gateway),
                 TURBO_OK);
    request = (turbo_flow_gateway_frame_view_t)
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    request.data = gbt32960;
    request.data_size = sizeof(gbt32960);
    request.protocol_version = "2025";
    check_int_eq(turbo_flow_gateway_reply(gateway, &request, TURBO_OK,
                                          &output),
                 TURBO_OK);
    check_size_eq(output.data_size, 25u);
    check_uint_eq(output.data[3], 0x01u);
    gateway_close_one(registry, owner);

    check_true(gateway_jtt808_frame(jtt808, sizeof(jtt808)) > 0u);
    registry = NULL;
    owner = NULL;
    gateway = NULL;
    output = (turbo_flow_gateway_frame_output_t)
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    output.data = response;
    output.capacity = sizeof(response);
    check_int_eq(gateway_open_one(
                     FLOW_GATEWAY_JTT808_MODULE, "jtt808",
                     TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808, "2019-A1",
                     &registry, &owner, &gateway),
                 TURBO_OK);
    request = (turbo_flow_gateway_frame_view_t)
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    request.data = jtt808;
    request.data_size = gateway_jtt808_frame(jtt808, sizeof(jtt808));
    request.protocol_version = "2019-A1";
    check_int_eq(turbo_flow_gateway_reply(gateway, &request, TURBO_OK,
                                          &output),
                 TURBO_OK);
    check_true(output.data_size > 0u);
    check_str_eq(output.metadata.operation, "platform-ack");
    gateway_close_one(registry, owner);
  }

  it("encodes bounded semantic downlink commands for every plugin") {
    static const uint8_t empty_object[] = "{}";
    static const uint8_t value[] = {0x41u, 0x42u};
    struct command_case_s {
      const char *module;
      const char *name;
      turbo_flow_gateway_protocol_t protocol;
      const char *version;
      const char *device_id;
      const char *operation;
      const char *resource;
      const char *correlation;
      uint64_t sequence;
      const uint8_t *payload;
      size_t payload_size;
    } cases[] = {
        {FLOW_GATEWAY_MQTT_SN_MODULE, "mqtt-sn",
         TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN, "1.2", "sensor-1", "publish",
         "7", NULL, 9u, value, sizeof(value)},
        {FLOW_GATEWAY_COAP_MODULE, "coap",
         TURBO_FLOW_GATEWAY_PROTOCOL_COAP, "RFC7252", "sensor-2", "get",
         "/sensors/1", "tk", 0x1234u, NULL, 0u},
        {FLOW_GATEWAY_LWM2M_MODULE, "lwm2m",
         TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M, "1.2.2", "device-3", "read",
         "/3/0/13", "lm", 0x1235u, NULL, 0u},
        {FLOW_GATEWAY_OCPP_MODULE, "ocpp",
         TURBO_FLOW_GATEWAY_PROTOCOL_OCPP, "2.0.1", "charger-4",
         "BootNotification", NULL, "call-1", 1u, empty_object,
         sizeof(empty_object) - 1u},
        {FLOW_GATEWAY_GBT32960_MODULE, "gbt32960",
         TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960, "2025",
         "L1234567890123456", "heartbeat", NULL, NULL, 1u, NULL, 0u},
        {FLOW_GATEWAY_JTT808_MODULE, "jtt808",
         TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808, "2019-A1",
         "00000000000123450001", "location-query", NULL, NULL, 7u, NULL,
         0u}};
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_gateway_registry_t *registry = NULL;
      turbo_flow_gateway_owner_t *owner = NULL;
      turbo_flow_gateway_t *gateway = NULL;
      turbo_flow_gateway_command_view_t command =
          TURBO_FLOW_GATEWAY_COMMAND_VIEW_INIT;
      turbo_flow_gateway_frame_output_t output =
          TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
      uint8_t frame[512];
      check_int_eq(gateway_open_one(
                       cases[i].module, cases[i].name, cases[i].protocol,
                       cases[i].version, &registry, &owner, &gateway),
                   TURBO_OK);
      command.device_id = cases[i].device_id;
      command.operation = cases[i].operation;
      command.resource = cases[i].resource;
      command.correlation_id = cases[i].correlation;
      command.sequence = cases[i].sequence;
      command.payload = cases[i].payload;
      command.payload_size = cases[i].payload_size;
      output.data = frame;
      output.capacity = sizeof(frame);
      check_int_eq(turbo_flow_gateway_encode(gateway, &command, &output),
                   TURBO_OK);
      check_true(output.data_size > 0u);
      check_str_eq(output.metadata.operation, cases[i].operation);
      check_str_eq(output.metadata.device_id, cases[i].device_id);
      gateway_close_one(registry, owner);
    }
  }
}
