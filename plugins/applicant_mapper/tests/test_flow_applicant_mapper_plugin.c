#include "tinytest.h"
#include "turbo_flow_applicant_mapper_plugin.h"
#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_protocol_mapper.h"

#include <string.h>

#ifndef FLOW_APPLICANT_MAPPER_PLUGIN
  #error FLOW_APPLICANT_MAPPER_PLUGIN is required
#endif

static const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *
find_mapper(const turbo_flow_plugin_protocol_mapper_catalog_v1_t *catalog,
            turbo_flow_protocol_kind_t protocol) {
  if (!catalog) return NULL;
  for (size_t i = 0u; i < catalog->count; ++i) {
    const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *entry =
        &catalog->entries[i];
    if (entry->mapper.protocol == protocol) return entry;
  }
  return NULL;
}

static void prove_mapper(
    const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *entry,
    uint32_t message_type, uint32_t semantic_type) {
  static const uint8_t json[] = "{\"age\":21}";
  turbo_flow_protocol_mapper_preflight_request_t preflight =
      TURBO_FLOW_PROTOCOL_MAPPER_PREFLIGHT_REQUEST_INIT;
  turbo_flow_protocol_mapper_contract_t contract =
      TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
  turbo_flow_protocol_mapper_request_t request =
      TURBO_FLOW_PROTOCOL_MAPPER_REQUEST_INIT;
  turbo_flow_protocol_mapper_output_t output =
      TURBO_FLOW_PROTOCOL_MAPPER_OUTPUT_INIT;
  uint8_t payload[64];

  check_not_null(entry);
  check_equal(entry->plugin_id, TURBO_FLOW_APPLICANT_MAPPER_PLUGIN_ID);
  check_equal(entry->mapper.name, TURBO_FLOW_APPLICANT_MAPPER_NAME);
  check_equal(entry->mapper.profile, TURBO_FLOW_APPLICANT_MAPPER_PROFILE);

  preflight.protocol = entry->mapper.protocol;
  preflight.profile = TURBO_FLOW_APPLICANT_MAPPER_PROFILE;
  preflight.message_type = message_type;
  preflight.semantic_type = semantic_type;
  preflight.semantic_media_type = "application/json";
  preflight.max_semantic_bytes = sizeof(payload);
  preflight.max_output_bytes = sizeof(payload);
  check_equal(entry->mapper.preflight(entry->mapper.ctx, &preflight, &contract), SALTS_OK);
  check_equal(contract.content.schema_name, TURBO_FLOW_APPLICANT_SCHEMA_ID);
  check_equal(contract.content.type_name, TURBO_FLOW_APPLICANT_TYPE_NAME);
  check_equal(contract.content.schema_version, TURBO_FLOW_APPLICANT_SCHEMA_VERSION);
  check_equal(contract.content.encoding, TURBO_FLOW_DATA_ENCODING_JSON);

  request.protocol = entry->mapper.protocol;
  request.profile = TURBO_FLOW_APPLICANT_MAPPER_PROFILE;
  request.metadata.message_type = message_type;
  request.semantic_data = json;
  request.semantic_size = sizeof(json) - 1u;
  request.semantic_type = semantic_type;
  request.semantic_media_type = "application/json";
  output.payload = payload;
  output.payload_capacity = sizeof(payload);
  check_equal(entry->mapper.map(entry->mapper.ctx, &request, &output), SALTS_OK);
  check_equal(output.payload_size, sizeof(json) - 1u);
  check_equal(output.payload, json, sizeof(json) - 1u);
  check_equal(turbo_flow_content_descriptor_validate(&contract.content, &output.content),
              SALTS_OK);
}

spec("Applicant JSON protocol mapper plugin") {
  it("registers CoAP and JT808 profiles with one canonical Applicant schema") {
    turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_protocol_mapper_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_CATALOG_V1_INIT;
    const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *coap;
    const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *jtt808;

    config.module_capacity = 1u;
    config.protocol_mapper_capacity = 2u;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_APPLICANT_MAPPER_PLUGIN, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_protocol_mapper_count(host), (size_t)2u);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_mapper_catalog(snapshot, &catalog),
                SALTS_OK);
    check_equal(catalog.count, (size_t)2u);

    coap = find_mapper(&catalog, TURBO_FLOW_PROTOCOL_COAP);
    jtt808 = find_mapper(&catalog, TURBO_FLOW_PROTOCOL_JTT_808);
    prove_mapper(coap, TURBO_FLOW_APPLICANT_COAP_MESSAGE_TYPE,
                 TURBO_FLOW_APPLICANT_COAP_SEMANTIC_TYPE);
    prove_mapper(jtt808, TURBO_FLOW_APPLICANT_JTT808_MESSAGE_TYPE,
                 TURBO_FLOW_APPLICANT_JTT808_SEMANTIC_TYPE);

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("rejects malformed or non-Applicant JSON before canonical publication") {
    static const uint8_t bad_json[] = "{\"age\":}";
    static const uint8_t wrong_shape[] = "{\"name\":\"x\"}";
    static const uint8_t fractional_age[] = "{\"age\":21.5}";
    const uint8_t *cases[] = {bad_json, wrong_shape, fractional_age};
    const size_t sizes[] = {sizeof(bad_json) - 1u, sizeof(wrong_shape) - 1u,
                            sizeof(fractional_age) - 1u};
    turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_protocol_mapper_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_CATALOG_V1_INIT;
    const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *entry;
    uint8_t payload[64];

    config.module_capacity = 1u;
    config.protocol_mapper_capacity = 2u;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_APPLICANT_MAPPER_PLUGIN, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_protocol_mapper_catalog(snapshot, &catalog),
                SALTS_OK);
    entry = find_mapper(&catalog, TURBO_FLOW_PROTOCOL_COAP);
    check_not_null(entry);

    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_protocol_mapper_request_t request =
          TURBO_FLOW_PROTOCOL_MAPPER_REQUEST_INIT;
      turbo_flow_protocol_mapper_output_t output =
          TURBO_FLOW_PROTOCOL_MAPPER_OUTPUT_INIT;
      request.protocol = TURBO_FLOW_PROTOCOL_COAP;
      request.profile = TURBO_FLOW_APPLICANT_MAPPER_PROFILE;
      request.metadata.message_type = TURBO_FLOW_APPLICANT_COAP_MESSAGE_TYPE;
      request.semantic_data = cases[i];
      request.semantic_size = sizes[i];
      request.semantic_type = TURBO_FLOW_APPLICANT_COAP_SEMANTIC_TYPE;
      request.semantic_media_type = "application/json";
      output.payload = payload;
      output.payload_capacity = sizeof(payload);
      check_equal(entry->mapper.map(entry->mapper.ctx, &request, &output), SALTS_EPROTO);
      check_equal(output.payload_size, (size_t)0u);
    }

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }
}
