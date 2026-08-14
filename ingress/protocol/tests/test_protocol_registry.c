#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"

#include <string.h>

typedef struct protocol_probe_s {
  size_t opens;
  size_t closes;
} protocol_probe_t;

static int protocol_probe_inspect(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || frame->data_size != 1u || frame->data[0] != 0x42u)
    return TURBO_EPROTO;
  metadata->message_type = frame->data[0];
  memcpy(metadata->operation, "probe", sizeof("probe"));
  return TURBO_OK;
}

static int protocol_probe_open(
    void *ctx, const turbo_flow_protocol_open_request_t *request,
    turbo_flow_protocol_service_t *service) {
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  protocol_probe_t *probe = (protocol_probe_t *)ctx;
  turbo_flow_protocol_t *protocol = NULL;
  int rc;
  ops.inspect = protocol_probe_inspect;
  rc = turbo_flow_protocol_create(
      request, "probe", "1", TURBO_FLOW_PROTOCOL_CAP_INGRESS |
                                     TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                                     TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
      &ops, NULL, &protocol);
  if (rc != TURBO_OK) return rc;
  probe->opens++;
  service->protocol = request->protocol;
  service->instance = protocol;
  service->owner = protocol;
  return TURBO_OK;
}

static void protocol_probe_close(void *ctx,
                                turbo_flow_protocol_service_t *service) {
  protocol_probe_t *probe = (protocol_probe_t *)ctx;
  turbo_flow_protocol_destroy((turbo_flow_protocol_t *)service->owner);
  service->instance = NULL;
  service->owner = NULL;
  probe->closes++;
}

spec("protocol registry") {
  it("retains a registered module contract while its owner is alive") {
    protocol_probe_t probe = {0};
    turbo_flow_protocol_plugin_api_t api = {
        sizeof(turbo_flow_protocol_plugin_api_t),
        TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
        TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
        "probe",
        TURBO_FLOW_PROTOCOL_COAP,
        TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
            TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
        &probe,
        protocol_probe_open,
        protocol_probe_close};
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_open_request_t request =
        TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
    request.protocol = TURBO_FLOW_PROTOCOL_COAP;
    request.protocol_version = "1";

    check_int_eq(turbo_flow_protocol_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_protocol_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_protocol_registry_register(registry, &api),
                 TURBO_EALREADY);
    check_int_eq(turbo_flow_protocol_owner_create_registered(
                     registry, "probe", &request, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_protocol_owner_instance(
                     owner, TURBO_FLOW_PROTOCOL_COAP, &protocol),
                 TURBO_OK);
    check_not_null(protocol);
    check_int_eq(turbo_flow_protocol_registry_destroy(registry), TURBO_EBUSY);
    turbo_flow_protocol_owner_destroy(owner);
    check_size_eq(probe.opens, 1u);
    check_size_eq(probe.closes, 1u);
    check_int_eq(turbo_flow_protocol_registry_destroy(registry), TURBO_OK);
  }

  it("decodes into caller-owned bounded buffers without partial output") {
    const uint8_t frame_data[] = {0x42u};
    uint8_t payload[1];
    protocol_probe_t probe = {0};
    turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
    turbo_flow_protocol_open_request_t request =
        TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
    turbo_flow_protocol_frame_view_t frame =
        TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    turbo_flow_protocol_message_output_t output =
        TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_protocol_t *protocol = NULL;
    request.protocol = TURBO_FLOW_PROTOCOL_COAP;
    request.protocol_version = "1";
    ops.inspect = protocol_probe_inspect;
    check_int_eq(turbo_flow_protocol_create(
                     &request, "probe", "1",
                     TURBO_FLOW_PROTOCOL_CAP_INGRESS |
                         TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                         TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
                     &ops, &probe, &protocol),
                 TURBO_OK);
    frame.data = frame_data;
    frame.data_size = sizeof(frame_data);
    frame.device_id = "device-1";
    output.payload = payload;
    output.payload_capacity = 0u;
    check_int_eq(turbo_flow_protocol_decode(protocol, &frame, &output),
                 TURBO_EMSGSIZE);
    check_size_eq(output.payload_size, 0u);
    output.payload_capacity = sizeof(payload);
    check_int_eq(turbo_flow_protocol_decode(protocol, &frame, &output),
                 TURBO_OK);
    check_str_eq(output.metadata.device_id, "device-1");
    check_str_eq(output.metadata.operation, "probe");
    check_size_eq(output.payload_size, sizeof(frame_data));
    check_int_eq(payload[0], 0x42u);
    turbo_flow_protocol_destroy(protocol);
  }
}
