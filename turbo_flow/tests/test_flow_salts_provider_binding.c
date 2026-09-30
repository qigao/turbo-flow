#include "tinytest.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif

spec("Salts Plugin provider binding") {
  it("resolves the exact provider Interface under one live module lease") {
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config config = {1u};
    salts_plugin_ref ref = {0};
    salts_plugin_lease lease = {0};
    const salts_plugin_manifest *manifest = NULL;
    const salts_plugin_export *entry = NULL;
    turbo_flow_provider_factory *factory;
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    bool quiescent = false;

    check_equal(salts_plugin_registry_init(&registry, &config),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &ref),
                SALTS_PLUGIN_OK);
    check_true(salts_plugin_ref_valid(ref));
    check_equal(salts_plugin_registry_start(&registry, ref),
                SALTS_PLUGIN_OK);

    check_equal(salts_plugin_registry_acquire(
                    &registry, ref, &lease, &manifest),
                SALTS_PLUGIN_OK);
    check_true(salts_plugin_lease_valid(lease));
    check_not_null(manifest);

    check_equal(salts_plugin_manifest_find_export(
                    manifest, "fixture.provider", &entry),
                SALTS_PLUGIN_OK);
    check_not_null(entry);
    check_equal(
        salts_plugin_export_require_interface(
            entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION, 0u,
            turbo_flow_provider_factory_interface()),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_export_require_interface(
            entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION + 1u, 0u,
            turbo_flow_provider_factory_interface()),
        SALTS_PLUGIN_INCOMPATIBLE_CONTRACT);

    factory = (turbo_flow_provider_factory *)entry->value.interface.value;
    check_not_null(factory);
    check_true(turbo_flow_provider_factory_valid(factory));
    check_equal(turbo_flow_provider_factory_contract(factory, &contract),
                SALTS_OK);
    check_true(turbo_flow_provider_contract_valid(&contract));
    check_equal(contract.config.message_artifact->type_name,
                "FixtureProviderConfig");
    check_null(contract.resource.contract_id);

    check_equal(salts_plugin_registry_request_stop(&registry, ref),
                SALTS_PLUGIN_OK);

    /* Stop closes new admission, but the live generation/provider lease still
       pins the module and prevents registry quiescence/unload. */
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, ref, &quiescent),
                SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(salts_plugin_registry_unload(&registry, ref),
                SALTS_PLUGIN_BUSY);

    check_equal(salts_plugin_registry_release(&registry, &lease),
                SALTS_PLUGIN_OK);
    check_false(salts_plugin_lease_valid(lease));

    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, ref, &quiescent),
                SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(salts_plugin_registry_unload(&registry, ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_count(&registry), (size_t)0u);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
  }
}
