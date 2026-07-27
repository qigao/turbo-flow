#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_gateway.h"
#include "turbo_flow_gateway_plugin.h"

#include <string.h>

typedef struct gateway_probe_s {
  size_t opens;
  size_t closes;
} gateway_probe_t;

static int gateway_probe_inspect(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_frame_view_t *frame,
    turbo_flow_gateway_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || frame->data_size != 1u || frame->data[0] != 0x42u)
    return TURBO_EPROTO;
  metadata->message_type = frame->data[0];
  memcpy(metadata->operation, "probe", sizeof("probe"));
  return TURBO_OK;
}

static int gateway_probe_open(
    void *ctx, const turbo_flow_gateway_open_request_t *request,
    turbo_flow_gateway_service_t *service) {
  turbo_flow_gateway_codec_ops_t ops = TURBO_FLOW_GATEWAY_CODEC_OPS_INIT;
  gateway_probe_t *probe = (gateway_probe_t *)ctx;
  turbo_flow_gateway_t *gateway = NULL;
  int rc;
  ops.inspect = gateway_probe_inspect;
  rc = turbo_flow_gateway_create(
      request, "probe", "1", TURBO_FLOW_GATEWAY_CAP_INGRESS |
                                     TURBO_FLOW_GATEWAY_CAP_EGRESS |
                                     TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE,
      &ops, NULL, &gateway);
  if (rc != TURBO_OK) return rc;
  probe->opens++;
  service->protocol = request->protocol;
  service->instance = gateway;
  service->owner = gateway;
  return TURBO_OK;
}

static void gateway_probe_close(void *ctx,
                                turbo_flow_gateway_service_t *service) {
  gateway_probe_t *probe = (gateway_probe_t *)ctx;
  turbo_flow_gateway_destroy((turbo_flow_gateway_t *)service->owner);
  service->instance = NULL;
  service->owner = NULL;
  probe->closes++;
}

spec("gateway registry") {
  it("retains a registered module contract while its owner is alive") {
    gateway_probe_t probe = {0};
    turbo_flow_gateway_plugin_api_t api = {
        sizeof(turbo_flow_gateway_plugin_api_t),
        TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MAJOR,
        TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MINOR,
        "probe",
        TURBO_FLOW_GATEWAY_PROTOCOL_COAP,
        TURBO_FLOW_GATEWAY_CAP_INGRESS | TURBO_FLOW_GATEWAY_CAP_EGRESS |
            TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE,
        &probe,
        gateway_probe_open,
        gateway_probe_close};
    turbo_flow_gateway_registry_t *registry = NULL;
    turbo_flow_gateway_owner_t *owner = NULL;
    turbo_flow_gateway_t *gateway = NULL;
    turbo_flow_gateway_open_request_t request =
        TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
    request.protocol = TURBO_FLOW_GATEWAY_PROTOCOL_COAP;
    request.protocol_version = "1";

    check_int_eq(turbo_flow_gateway_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_gateway_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_gateway_registry_register(registry, &api),
                 TURBO_EALREADY);
    check_int_eq(turbo_flow_gateway_owner_create_registered(
                     registry, "probe", &request, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_gateway_owner_instance(
                     owner, TURBO_FLOW_GATEWAY_PROTOCOL_COAP, &gateway),
                 TURBO_OK);
    check_not_null(gateway);
    check_int_eq(turbo_flow_gateway_registry_destroy(registry), TURBO_EBUSY);
    turbo_flow_gateway_owner_destroy(owner);
    check_size_eq(probe.opens, 1u);
    check_size_eq(probe.closes, 1u);
    check_int_eq(turbo_flow_gateway_registry_destroy(registry), TURBO_OK);
  }

  it("maps into caller-owned bounded buffers without partial output") {
    const uint8_t frame_data[] = {0x42u};
    char topic[128];
    uint8_t payload[1];
    gateway_probe_t probe = {0};
    turbo_flow_gateway_codec_ops_t ops = TURBO_FLOW_GATEWAY_CODEC_OPS_INIT;
    turbo_flow_gateway_open_request_t request =
        TURBO_FLOW_GATEWAY_OPEN_REQUEST_INIT;
    turbo_flow_gateway_frame_view_t frame =
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    turbo_flow_gateway_mqtt_output_t output =
        TURBO_FLOW_GATEWAY_MQTT_OUTPUT_INIT;
    turbo_flow_gateway_t *gateway = NULL;
    request.protocol = TURBO_FLOW_GATEWAY_PROTOCOL_COAP;
    request.protocol_version = "1";
    ops.inspect = gateway_probe_inspect;
    check_int_eq(turbo_flow_gateway_create(
                     &request, "probe", "1",
                     TURBO_FLOW_GATEWAY_CAP_INGRESS |
                         TURBO_FLOW_GATEWAY_CAP_EGRESS |
                         TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE,
                     &ops, &probe, &gateway),
                 TURBO_OK);
    frame.data = frame_data;
    frame.data_size = sizeof(frame_data);
    frame.device_id = "device-1";
    output.topic = topic;
    output.topic_capacity = sizeof(topic);
    output.payload = payload;
    output.payload_capacity = 0u;
    check_int_eq(turbo_flow_gateway_ingress(gateway, &frame, &output),
                 TURBO_EMSGSIZE);
    check_size_eq(output.topic_size, 0u);
    check_size_eq(output.payload_size, 0u);
    output.payload_capacity = sizeof(payload);
    check_int_eq(turbo_flow_gateway_ingress(gateway, &frame, &output),
                 TURBO_OK);
    check_str_eq(topic, "gateway/coap/default/device-1/up/probe");
    check_size_eq(output.payload_size, sizeof(frame_data));
    check_int_eq(payload[0], 0x42u);
    turbo_flow_gateway_destroy(gateway);
  }
}
