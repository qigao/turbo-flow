#include "salts_error.h"
#include "tinytest.h"
#include "turbo_flow_plugin_protocol.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"

#include <string.h>

#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_ONE
  #error FLOW_PROTOCOL_CATALOG_FIXTURE_ONE is required
#endif
#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_TWO
  #error FLOW_PROTOCOL_CATALOG_FIXTURE_TWO is required
#endif
#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_DUPLICATE
  #error FLOW_PROTOCOL_CATALOG_FIXTURE_DUPLICATE is required
#endif
#ifndef FLOW_PROTOCOL_CATALOG_FIXTURE_MISMATCH
  #error FLOW_PROTOCOL_CATALOG_FIXTURE_MISMATCH is required
#endif

static int protocol_probe_inspect(void *ctx, const char *configured_version,
                                  const turbo_flow_protocol_frame_view_t *frame,
                                  turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || frame->data_size != 1u || frame->data[0] != 0x42u) return SALTS_EPROTO;
  metadata->message_type = frame->data[0];
  memcpy(metadata->operation, "probe", sizeof("probe"));
  return SALTS_OK;
}

spec("protocol registry") {
  it("treats ABI 1.0 host config as a bounded prefix without protocol fallback") {
    turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_protocol_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    config.size = TURBO_FLOW_PLUGIN_HOST_CONFIG_V1_0_SIZE;
    config.abi_minor = 0u;
    config.module_capacity = 1u;
    config.adapter_provider_capacity = 0u;
    config.resource_provider_capacity = 0u;
    config.protocol_provider_capacity = 99u;
    config.business_provider_capacity = 99u;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_COAP_MODULE, &error), SALTS_ENOSPC);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.protocol_provider_count, 0u);
    check_equal(catalog.business_provider_count, 0u);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("retains the unified module snapshot while its owner is alive") {
    turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
    request.protocol = TURBO_FLOW_PROTOCOL_COAP;
    request.protocol_version = "RFC7252";
    config.module_capacity = 1u;
    config.adapter_provider_capacity = 0u;
    config.resource_provider_capacity = 0u;
    config.protocol_provider_capacity = 1u;
    config.business_provider_capacity = 0u;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_COAP_MODULE, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_protocol_registry_create(snapshot, &registry), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_protocol_owner_create_registered(registry, "coap", &request, &owner),
                SALTS_OK);
    check_equal(turbo_flow_protocol_owner_instance(owner, TURBO_FLOW_PROTOCOL_COAP, &protocol),
                SALTS_OK);
    check_not_null(protocol);
    check_equal(turbo_flow_protocol_registry_destroy(registry), SALTS_EBUSY);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_EBUSY);
    turbo_flow_protocol_owner_destroy(owner);
    check_equal(turbo_flow_protocol_registry_destroy(registry), SALTS_OK);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("rolls back multi-category providers on capacity duplicate and capability mismatch") {
    turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_protocol_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    config.module_capacity = 4u;
    config.adapter_provider_capacity = 0u;
    config.resource_provider_capacity = 0u;
    config.protocol_provider_capacity = 1u;
    config.business_provider_capacity = 1u;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_CATALOG_FIXTURE_ONE, &error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_CATALOG_FIXTURE_DUPLICATE, &error),
                SALTS_EALREADY);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_CATALOG_FIXTURE_TWO, &error),
                SALTS_ENOSPC);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.protocol_provider_count, 1u);
    check_equal(catalog.business_provider_count, 1u);
    check_equal(catalog.protocol_providers[0].name, "catalog-protocol-one");
    check_equal(catalog.business_providers[0].business, "catalog-business-one");
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);

    host = NULL;
    snapshot = NULL;
    config.module_capacity = 1u;
    config.business_provider_capacity = 0u;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    catalog = (turbo_flow_plugin_protocol_catalog_v1_t)TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_CATALOG_FIXTURE_ONE, &error),
                SALTS_ENOSPC);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.protocol_provider_count, 0u);
    check_equal(catalog.business_provider_count, 0u);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);

    host = NULL;
    snapshot = NULL;
    config.business_provider_capacity = 1u;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    catalog = (turbo_flow_plugin_protocol_catalog_v1_t)TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_CATALOG_FIXTURE_MISMATCH, &error),
                SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.protocol_provider_count, 0u);
    check_equal(catalog.business_provider_count, 0u);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("decodes into caller-owned bounded buffers without partial output") {
    const uint8_t frame_data[] = {0x42u};
    uint8_t payload[1];
    turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
    turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
    turbo_flow_protocol_frame_view_t frame = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    turbo_flow_protocol_message_output_t output = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_protocol_t *protocol = NULL;
    request.protocol = TURBO_FLOW_PROTOCOL_COAP;
    request.protocol_version = "1";
    ops.inspect = protocol_probe_inspect;
    check_equal(turbo_flow_protocol_create(&request, "probe", "1",
                                           TURBO_FLOW_PROTOCOL_CAP_INGRESS |
                                               TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                                               TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
                                           &ops, NULL, &protocol),
                SALTS_OK);
    frame.data = frame_data;
    frame.data_size = sizeof(frame_data);
    frame.device_id = "device-1";
    output.payload = payload;
    output.payload_capacity = 0u;
    check_equal(turbo_flow_protocol_decode(protocol, &frame, &output), SALTS_EMSGSIZE);
    check_equal(output.payload_size, 0u);
    output.payload_capacity = sizeof(payload);
    check_equal(turbo_flow_protocol_decode(protocol, &frame, &output), SALTS_OK);
    check_equal(output.metadata.device_id, "device-1");
    check_equal(output.metadata.operation, "probe");
    check_equal(output.payload_size, sizeof(frame_data));
    check_equal(payload[0], 0x42u);
    turbo_flow_protocol_destroy(protocol);
  }
}
