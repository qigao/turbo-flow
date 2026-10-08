#include "turbo_flow_chttp_resource.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int check_provider(
    const cmeta_plugin_manifest *manifest,
    const char *export_id,
    const char *expected_type,
    const char *expected_resource_contract,
    uint64_t expected_resource_capabilities) {
  const cmeta_plugin_export *entry = NULL;
  turbo_flow_provider_factory *factory;
  turbo_flow_provider_contract_v1_t contract =
      TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  cmeta_plugin_status status;
  int rc;

  status = cmeta_plugin_manifest_find_export(
      manifest, export_id, &entry);
  if (status != CMETA_PLUGIN_OK || !entry) return 10;

  status = cmeta_plugin_export_require_interface(
      entry,
      TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
      0u,
      turbo_flow_provider_factory_interface());
  if (status != CMETA_PLUGIN_OK) return 11;

  factory =
      (turbo_flow_provider_factory *)entry->value.interface.value;
  if (!turbo_flow_provider_factory_valid(factory)) return 12;

  rc = turbo_flow_provider_factory_contract(factory, &contract);
  if (rc != SALTS_OK ||
      !turbo_flow_provider_contract_valid(&contract))
    return 13;

  if (!contract.config.codec_factory ||
      !contract.config.message_artifact ||
      !contract.config.message_artifact->type_name ||
      strcmp(contract.config.message_artifact->type_name, expected_type) != 0)
    return 14;

  if (!contract.resource.contract_id ||
      strcmp(
          contract.resource.contract_id,
          expected_resource_contract) != 0 ||
      contract.resource.contract_version !=
          TURBO_FLOW_CHTTP_RESOURCE_CONTRACT_VERSION ||
      contract.resource.required_capabilities !=
          expected_resource_capabilities ||
      !cmeta_interface_desc_equal(
          contract.resource.expected_interface,
          turbo_flow_chttp_deployment_resource_interface()))
    return 15;

  return 0;
}

int main(int argc, char **argv) {
  cmeta_plugin_registry registry = {0};
  cmeta_plugin_registry_config config = {1u};
  cmeta_plugin_ref plugin = {0};
  cmeta_plugin_lease lease = {0};
  const cmeta_plugin_manifest *manifest = NULL;
  bool quiescent = true;
  int registry_live = 0;
  int plugin_loaded = 0;
  int plugin_started = 0;
  int lease_live = 0;
  int rc = 0;

  if (argc != 2 || !argv[1] || !argv[1][0]) return 1;

  if (cmeta_plugin_registry_init(&registry, &config) !=
      CMETA_PLUGIN_OK) {
    fprintf(stderr, "CHTTP consumer failed at registry init\n");
    return 2;
  }
  registry_live = 1;

  if (cmeta_plugin_registry_load(&registry, argv[1], &plugin) !=
      CMETA_PLUGIN_OK) {
    fprintf(stderr, "CHTTP consumer failed at plugin DLL load: %s\n", argv[1]);
    rc = 3;
    goto cleanup;
  }
  plugin_loaded = 1;

  if (cmeta_plugin_registry_start(&registry, plugin) !=
      CMETA_PLUGIN_OK) {
    rc = 4;
    goto cleanup;
  }
  plugin_started = 1;

  if (cmeta_plugin_registry_acquire(
          &registry, plugin, &lease, &manifest) !=
      CMETA_PLUGIN_OK) {
    rc = 5;
    goto cleanup;
  }
  lease_live = 1;

  if (!manifest || manifest->struct_size != CMETA_PLUGIN_MANIFEST_SIZE ||
      manifest->abi_version != CMETA_PLUGIN_ABI_VERSION ||
      !manifest->plugin_id ||
      strcmp(manifest->plugin_id, "turbo-flow.chttp") != 0 ||
      manifest->export_count != 3u) {
    rc = 6;
    goto cleanup;
  }

  rc = check_provider(
      manifest, "chttp.client", "CHttpClientConfig",
      TURBO_FLOW_CHTTP_CLIENT_RESOURCE_CONTRACT_ID,
      TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
          TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT);
  if (rc != 0) goto cleanup;
  rc = check_provider(
      manifest, "chttp.server", "CHttpServerConfig",
      TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID,
      TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
          TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND);
  if (rc != 0) goto cleanup;
  rc = check_provider(
      manifest, "chttp.websocket_server",
      "CHttpWebSocketServerConfig",
      TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID,
      TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
          TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND);
  if (rc != 0) goto cleanup;

  if (cmeta_plugin_registry_request_stop(&registry, plugin) !=
      CMETA_PLUGIN_OK) {
    rc = 7;
    goto cleanup;
  }
  plugin_started = 0;

  if (cmeta_plugin_registry_poll_quiescent(
          &registry, plugin, &quiescent) != CMETA_PLUGIN_OK ||
      quiescent) {
    rc = 8;
    goto cleanup;
  }

  if (cmeta_plugin_registry_release(&registry, &lease) !=
      CMETA_PLUGIN_OK) {
    rc = 9;
    goto cleanup;
  }
  lease_live = 0;

  if (cmeta_plugin_registry_poll_quiescent(
          &registry, plugin, &quiescent) != CMETA_PLUGIN_OK ||
      !quiescent) {
    rc = 16;
    goto cleanup;
  }

  if (cmeta_plugin_registry_unload(&registry, plugin) !=
      CMETA_PLUGIN_OK) {
    rc = 17;
    goto cleanup;
  }
  plugin_loaded = 0;

cleanup:
  if (lease_live)
    (void)cmeta_plugin_registry_release(&registry, &lease);
  if (plugin_started)
    (void)cmeta_plugin_registry_request_stop(&registry, plugin);
  if (plugin_loaded) {
    quiescent = false;
    if (cmeta_plugin_registry_poll_quiescent(
            &registry, plugin, &quiescent) == CMETA_PLUGIN_OK &&
        quiescent)
      (void)cmeta_plugin_registry_unload(&registry, plugin);
  }
  if (registry_live)
    (void)cmeta_plugin_registry_destroy(&registry);

  if (rc != 0)
    fprintf(stderr, "CHTTP installed provider contract failed: %d\n", rc);
  return rc == 0 ? 0 : 1;
}
