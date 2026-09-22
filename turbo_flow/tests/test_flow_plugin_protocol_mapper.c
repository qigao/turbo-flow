#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_protocol_mapper.h"

#include <stddef.h>
#include <string.h>
#include <tinytest.h>

static turbo_flow_plugin_host_config_t mapper_config(size_t capacity) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  config.module_capacity = 4u;
  config.protocol_mapper_capacity = capacity;
  return config;
}

spec("plugin protocol mapper catalog") {
  it("loads one ABI3.2 mapper, snapshots its DLL lease, and preflights the borrowed vtable") {
    turbo_flow_plugin_host_config_t config = mapper_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_protocol_mapper_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_CATALOG_V1_INIT;
    turbo_flow_protocol_mapper_preflight_request_t request =
        TURBO_FLOW_PROTOCOL_MAPPER_PREFLIGHT_REQUEST_INIT;
    turbo_flow_protocol_mapper_contract_t contract =
        TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_MAPPER_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_protocol_mapper_count(host), (size_t)1u);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_mapper_catalog(snapshot, &catalog),
                SALTS_OK);
    check_equal(catalog.count, (size_t)1u);
    check_equal(strcmp(catalog.entries[0].plugin_id, "fixture.protocol-mapper.good"), 0);
    check_equal(strcmp(catalog.entries[0].mapper.name, "fixture.mapper"), 0);
    check_equal(strcmp(catalog.entries[0].mapper.profile, "applicant-json"), 0);
    check_equal(catalog.entries[0].mapper.protocol, TURBO_FLOW_PROTOCOL_COAP);

    request.protocol = TURBO_FLOW_PROTOCOL_COAP;
    request.profile = "applicant-json";
    request.message_type = 2u;
    request.semantic_type = 50u;
    request.semantic_media_type = "application/json";
    request.max_semantic_bytes = 128u;
    request.max_output_bytes = 128u;
    check_equal(catalog.entries[0].mapper.preflight(
                    catalog.entries[0].mapper.ctx, &request, &contract),
                SALTS_OK);
    check_equal(contract.protocol, TURBO_FLOW_PROTOCOL_COAP);
    check_equal(contract.message_type, 2u);
    check_equal(contract.semantic_type, 50u);
    check_equal(strcmp(contract.profile, "applicant-json"), 0);
    check_equal(strcmp(contract.content.schema_name, "rulesforge.Applicant.data"), 0);
    check_equal(strcmp(contract.content.type_name, "Applicant"), 0);
    check_equal(contract.content.schema_version, 1u);

    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_EBUSY);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("enforces mapper capacity at zero and N plus one without partial registration") {
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_host_config_t zero = mapper_config(0u);
    check_equal(turbo_flow_plugin_host_create(&zero, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_MAPPER_GOOD, &error), SALTS_ENOSPC);
    check_equal(turbo_flow_plugin_host_protocol_mapper_count(host), (size_t)0u);
    check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0u);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);

    turbo_flow_plugin_host_config_t one = mapper_config(1u);
    host = NULL;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_create(&one, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_MAPPER_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PROTOCOL_MAPPER_SECOND, &error), SALTS_ENOSPC);
    check_equal(turbo_flow_plugin_host_protocol_mapper_count(host), (size_t)1u);
    check_equal(turbo_flow_plugin_host_module_count(host), (size_t)1u);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rolls back duplicate and malformed mapper registration") {
    const char *paths[] = {FLOW_PROTOCOL_MAPPER_DUPLICATE, FLOW_PROTOCOL_MAPPER_MISSING_MAP};
    const int statuses[] = {SALTS_EALREADY, SALTS_EINVAL};
    for (size_t i = 0u; i < sizeof(paths) / sizeof(paths[0]); ++i) {
      turbo_flow_plugin_host_config_t config = mapper_config(2u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, paths[i], &error), statuses[i]);
      check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
      check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0u);
      check_equal(turbo_flow_plugin_host_protocol_mapper_count(host), (size_t)0u);
      check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
    }
  }

  it("rejects the ABI3.1 host-config prefix instead of zero-extending mapper capacity") {
    turbo_flow_plugin_host_config_t config = mapper_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    config.size = offsetof(turbo_flow_plugin_host_config_t, protocol_mapper_capacity);
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_EINVAL);
    check_null(host);
  }
}
