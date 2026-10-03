#include "turbo_flow_cnet_resource.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

typedef struct expected_provider_s {
  const char *export_id;
  const char *type_name;
  uint64_t resource_capabilities;
} expected_provider_t;

static const expected_provider_t providers[] = {
    {"cnet.stream_source", "CNetStreamSourceConfig",
     TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
         TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT},
    {"cnet.listener_source", "CNetListenerSourceConfig",
     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND},
    {"cnet.packet_source", "CNetPacketSourceConfig",
     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND},
    {"cnet.stream_sink", "CNetStreamSinkConfig",
     TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
         TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT},
    {"cnet.datagram_sink", "CNetDatagramSinkConfig",
     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND},
    {"cnet.packet_sink", "CNetPacketSinkConfig",
     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
         TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND},
};

static int check_provider(
    const salts_plugin_manifest *manifest,
    const expected_provider_t *expected) {
  const salts_plugin_export *entry = NULL;
  turbo_flow_provider_factory *factory;
  turbo_flow_provider_contract_v1_t contract =
      TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  salts_plugin_status status;
  int rc;

  status = salts_plugin_manifest_find_export(
      manifest, expected->export_id, &entry);
  if (status != SALTS_PLUGIN_OK || !entry) return 10;

  status = salts_plugin_export_require_interface(
      entry,
      TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
      0u,
      turbo_flow_provider_factory_interface());
  if (status != SALTS_PLUGIN_OK) return 11;

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
      strcmp(
          contract.config.message_artifact->type_name,
          expected->type_name) != 0)
    return 14;

  if (!contract.resource.contract_id ||
      strcmp(
          contract.resource.contract_id,
          TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID) != 0 ||
      contract.resource.contract_version !=
          TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION ||
      contract.resource.required_capabilities !=
          expected->resource_capabilities ||
      !cmeta_interface_desc_equal(
          contract.resource.expected_interface,
          turbo_flow_cnet_deployment_resource_interface()))
    return 15;

  return 0;
}

int main(int argc, char **argv) {
  salts_plugin_registry registry = {0};
  salts_plugin_registry_config config = {1u};
  salts_plugin_ref plugin = {0};
  salts_plugin_lease lease = {0};
  const salts_plugin_manifest *manifest = NULL;
  bool quiescent = true;
  int registry_live = 0;
  int plugin_loaded = 0;
  int plugin_started = 0;
  int lease_live = 0;
  int rc = 0;

  if (argc != 2 || !argv[1] || !argv[1][0]) return 1;

  if (salts_plugin_registry_init(&registry, &config) !=
      SALTS_PLUGIN_OK) {
    fprintf(stderr, "CNet consumer failed at registry init\n");
    return 2;
  }
  registry_live = 1;

  if (salts_plugin_registry_load(&registry, argv[1], &plugin) !=
      SALTS_PLUGIN_OK) {
    fprintf(stderr, "CNet consumer failed at plugin DLL load: %s\n", argv[1]);
    rc = 3;
    goto cleanup;
  }
  plugin_loaded = 1;

  if (salts_plugin_registry_start(&registry, plugin) !=
      SALTS_PLUGIN_OK) {
    rc = 4;
    goto cleanup;
  }
  plugin_started = 1;

  if (salts_plugin_registry_acquire(
          &registry, plugin, &lease, &manifest) !=
      SALTS_PLUGIN_OK) {
    rc = 5;
    goto cleanup;
  }
  lease_live = 1;

  if (!manifest || manifest->struct_size != SALTS_PLUGIN_MANIFEST_SIZE ||
      manifest->abi_version != SALTS_PLUGIN_ABI_VERSION ||
      !manifest->plugin_id ||
      strcmp(manifest->plugin_id, "turbo-flow.cnet") != 0 ||
      manifest->export_count !=
          sizeof(providers) / sizeof(providers[0])) {
    rc = 6;
    goto cleanup;
  }

  for (size_t i = 0u; i < sizeof(providers) / sizeof(providers[0]); ++i) {
    rc = check_provider(manifest, &providers[i]);
    if (rc != 0) goto cleanup;
  }

  if (salts_plugin_registry_request_stop(&registry, plugin) !=
      SALTS_PLUGIN_OK) {
    rc = 7;
    goto cleanup;
  }
  plugin_started = 0;

  if (salts_plugin_registry_poll_quiescent(
          &registry, plugin, &quiescent) != SALTS_PLUGIN_OK ||
      quiescent) {
    rc = 8;
    goto cleanup;
  }

  if (salts_plugin_registry_release(&registry, &lease) !=
      SALTS_PLUGIN_OK) {
    rc = 9;
    goto cleanup;
  }
  lease_live = 0;

  if (salts_plugin_registry_poll_quiescent(
          &registry, plugin, &quiescent) != SALTS_PLUGIN_OK ||
      !quiescent) {
    rc = 16;
    goto cleanup;
  }

  if (salts_plugin_registry_unload(&registry, plugin) !=
      SALTS_PLUGIN_OK) {
    rc = 17;
    goto cleanup;
  }
  plugin_loaded = 0;

cleanup:
  if (lease_live)
    (void)salts_plugin_registry_release(&registry, &lease);
  if (plugin_started)
    (void)salts_plugin_registry_request_stop(&registry, plugin);
  if (plugin_loaded) {
    quiescent = false;
    if (salts_plugin_registry_poll_quiescent(
            &registry, plugin, &quiescent) == SALTS_PLUGIN_OK &&
        quiescent)
      (void)salts_plugin_registry_unload(&registry, plugin);
  }
  if (registry_live)
    (void)salts_plugin_registry_destroy(&registry);

  if (rc != 0)
    fprintf(stderr, "CNet installed provider contract failed: %d\n", rc);
  return rc == 0 ? 0 : 1;
}
