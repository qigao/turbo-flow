#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_protocol_business.h"
#include "turbo_flow_protocol_business_plugin.h"

#include <string.h>

typedef struct business_probe_s {
  size_t opens;
  size_t closes;
  size_t events;
  size_t commands;
  int event_status;
  uint64_t last_delivery_id;
  char last_action[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
} business_probe_t;

static int business_probe_consume(void *ctx,
                                  const turbo_flow_protocol_business_event_view_t *event) {
  business_probe_t *probe = (business_probe_t *)ctx;
  probe->events++;
  probe->last_delivery_id = event->delivery_id;
  return probe->event_status;
}

static int business_probe_prepare(void *ctx,
                                  const turbo_flow_protocol_business_command_request_t *request,
                                  turbo_flow_protocol_business_command_output_t *output) {
  business_probe_t *probe = (business_probe_t *)ctx;
  if (request->content.data_size > output->payload_capacity) return TURBO_EMSGSIZE;
  probe->commands++;
  memcpy(probe->last_action, request->action, strlen(request->action) + 1u);
  memcpy(output->device_id, request->device_id, strlen(request->device_id) + 1u);
  memcpy(output->operation, "write", sizeof("write"));
  if (request->resource)
    memcpy(output->resource, request->resource, strlen(request->resource) + 1u);
  if (request->correlation_id)
    memcpy(output->correlation_id, request->correlation_id, strlen(request->correlation_id) + 1u);
  output->sequence = request->sequence;
  if (request->content.data_size > 0u)
    memcpy(output->payload, request->content.data, request->content.data_size);
  output->payload_size = request->content.data_size;
  return TURBO_OK;
}

static int business_probe_open(void *ctx, const turbo_flow_protocol_business_open_request_t *request,
                               turbo_flow_protocol_business_service_t *service) {
  turbo_flow_protocol_business_ops_t ops = TURBO_FLOW_PROTOCOL_BUSINESS_OPS_INIT;
  business_probe_t *probe = (business_probe_t *)ctx;
  turbo_flow_protocol_business_t *business = NULL;
  int rc;
  ops.consume_committed = business_probe_consume;
  ops.prepare_command = business_probe_prepare;
  rc = turbo_flow_protocol_business_create("probe-biz", request->protocol, request->profile,
                                          request->max_payload_size,
                                          TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT |
                                              TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND,
                                          &ops, probe, &business);
  if (rc != TURBO_OK) return rc;
  probe->opens++;
  service->protocol = request->protocol;
  service->instance = business;
  service->owner = business;
  return TURBO_OK;
}

static void business_probe_close(void *ctx, turbo_flow_protocol_business_service_t *service) {
  business_probe_t *probe = (business_probe_t *)ctx;
  turbo_flow_protocol_business_destroy((turbo_flow_protocol_business_t *)service->owner);
  service->instance = NULL;
  service->owner = NULL;
  probe->closes++;
}

static turbo_flow_protocol_business_plugin_api_t business_probe_api(business_probe_t *probe) {
  turbo_flow_protocol_business_plugin_api_t api = {
      sizeof(turbo_flow_protocol_business_plugin_api_t),
      TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MAJOR,
      TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MINOR,
      "probe-biz",
      TURBO_FLOW_PROTOCOL_OCPP,
      TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT |
          TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND,
      probe,
      business_probe_open,
      business_probe_close};
  return api;
}

spec("protocol business service") {
  it("keeps the plugin export limited while an opaque owner is alive") {
    business_probe_t probe = {0};
    turbo_flow_protocol_business_plugin_api_t api = business_probe_api(&probe);
    turbo_flow_protocol_business_open_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_OPEN_REQUEST_INIT;
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_info_t info = TURBO_FLOW_PROTOCOL_BUSINESS_INFO_INIT;
    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.profile = "ocpp-2.0.1-core";
    request.max_payload_size = 128u;
    check_int_eq(turbo_flow_protocol_business_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_registry_register(registry, &api), TURBO_EALREADY);
    check_int_eq(turbo_flow_protocol_business_owner_create_registered(registry, "probe-biz",
                                                                     &request, &owner),
                 TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_owner_instance(owner, TURBO_FLOW_PROTOCOL_OCPP,
                                                            &business),
                 TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_get_info(business, &info), TURBO_OK);
    check_str_eq(info.business, "probe-biz");
    check_str_eq(info.profile, "ocpp-2.0.1-core");
    check_int_eq(turbo_flow_protocol_business_registry_destroy(registry), TURBO_EBUSY);
    turbo_flow_protocol_business_owner_destroy(owner);
    check_size_eq(probe.opens, 1u);
    check_size_eq(probe.closes, 1u);
    check_int_eq(turbo_flow_protocol_business_registry_destroy(registry), TURBO_OK);
  }

  it("delivers only a borrowed post-commit schema identity") {
    const uint8_t payload[] = {'{', '}'};
    business_probe_t probe = {0};
    turbo_flow_protocol_business_ops_t ops = TURBO_FLOW_PROTOCOL_BUSINESS_OPS_INIT;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event = TURBO_FLOW_PROTOCOL_BUSINESS_EVENT_VIEW_INIT;
    ops.consume_committed = business_probe_consume;
    check_int_eq(turbo_flow_protocol_business_create(
                     "event-probe", TURBO_FLOW_PROTOCOL_OCPP, "ocpp-2.0.1-core", 128u,
                     TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT, &ops, &probe, &business),
                 TURBO_OK);
    event.delivery_id = 41u;
    event.session_id = 7u;
    event.session_generation = 2u;
    event.route = "protocol/ocpp/charger-1/Heartbeat";
    event.route_size = strlen(event.route);
    event.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    event.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
    memcpy(event.metadata.device_id, "charger-1", sizeof("charger-1"));
    memcpy(event.metadata.operation, "Heartbeat", sizeof("Heartbeat"));
    event.content.data = payload;
    event.content.data_size = sizeof(payload);
    event.content.media_type = "application/json";
    event.content.schema_id = "ocpp-2.0.1";
    event.content.type_name = "HeartbeatRequest";
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    check_size_eq(probe.events, 1u);
    check_uint_eq(probe.last_delivery_id, 41u);
    event.content.type_name = NULL;
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EINVAL);
    check_size_eq(probe.events, 1u);
    event.content.type_name = "HeartbeatRequest";
    probe.event_status = TURBO_EBUSY;
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EBUSY);
    check_size_eq(probe.events, 2u);
    turbo_flow_protocol_business_destroy(business);
  }

  it("maps a schema-bound business action into caller-owned command data") {
    const uint8_t payload[] = {0x01u, 0x02u, 0x03u};
    uint8_t output_payload[sizeof(payload)];
    business_probe_t probe = {0};
    turbo_flow_protocol_business_ops_t ops = TURBO_FLOW_PROTOCOL_BUSINESS_OPS_INIT;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_command_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_REQUEST_INIT;
    turbo_flow_protocol_business_command_output_t output =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_OUTPUT_INIT;
    turbo_flow_protocol_command_view_t command = TURBO_FLOW_PROTOCOL_COMMAND_VIEW_INIT;
    ops.prepare_command = business_probe_prepare;
    check_int_eq(turbo_flow_protocol_business_create(
                     "command-probe", TURBO_FLOW_PROTOCOL_JTT_808, "jtt808-2019", 64u,
                     TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND, &ops, &probe, &business),
                 TURBO_OK);
    request.command_id = 9u;
    request.protocol = TURBO_FLOW_PROTOCOL_JTT_808;
    request.tenant = "fleet-a";
    request.device_id = "013800138000";
    request.action = "set-terminal-parameters";
    request.resource = "0x8103";
    request.correlation_id = "cmd-9";
    request.sequence = 17u;
    request.content.data = payload;
    request.content.data_size = sizeof(payload);
    request.content.media_type = "application/octet-stream";
    output.payload = output_payload;
    output.payload_capacity = sizeof(output_payload);
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_OK);
    check_size_eq(probe.commands, 1u);
    check_str_eq(probe.last_action, "set-terminal-parameters");
    check_str_eq(output.device_id, "013800138000");
    check_str_eq(output.operation, "write");
    check_size_eq(output.payload_size, sizeof(payload));
    check_int_eq(turbo_flow_protocol_business_command_view(&output, &command), TURBO_OK);
    check_str_eq(command.device_id, "013800138000");
    check_str_eq(command.operation, "write");
    check_str_eq(command.resource, "0x8103");
    check_str_eq(command.correlation_id, "cmd-9");
    check_uint_eq(command.sequence, 17u);
    check_size_eq(command.payload_size, sizeof(payload));
    check_int_eq(command.payload[2], 0x03u);
    output.payload_capacity = sizeof(payload) - 1u;
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EMSGSIZE);
    check_size_eq(output.payload_size, 0u);
    turbo_flow_protocol_business_destroy(business);
  }
}
