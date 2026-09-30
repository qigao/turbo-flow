#include "tinytest.h"
#include "turbo_flow_provider.h"
#include "salts_resource_fixture.h"

#include <salts/plugin.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_SALTS_RESOURCE_FIXTURE
#error "FLOW_SALTS_RESOURCE_FIXTURE is required"
#endif

spec("Salts Plugin provider/resource binding") {
  it("binds exact provider and resource Interfaces under independent leases") {
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config registry_config = {2u};
    salts_plugin_ref provider_ref = {0};
    salts_plugin_ref resource_ref = {0};
    salts_plugin_lease provider_lease = {0};
    salts_plugin_lease resource_lease = {0};
    const salts_plugin_manifest *provider_manifest = NULL;
    const salts_plugin_manifest *resource_manifest = NULL;
    const salts_plugin_export *provider_entry = NULL;
    const salts_plugin_export *resource_entry = NULL;
    turbo_flow_provider_factory *factory;
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    turbo_flow_provider_resource_view_v1_t resource_view =
        TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
    turbo_flow_provider_instance_v1_t instance =
        TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    bool provider_quiescent = true;
    bool resource_quiescent = true;

    check_equal(salts_plugin_registry_init(&registry, &registry_config),
                SALTS_PLUGIN_OK);

    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_RESOURCE_FIXTURE, &resource_ref),
                SALTS_PLUGIN_OK);
    check_true(salts_plugin_ref_valid(provider_ref));
    check_true(salts_plugin_ref_valid(resource_ref));
    check_true(provider_ref.slot != resource_ref.slot);

    check_equal(salts_plugin_registry_start(&registry, provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, resource_ref),
                SALTS_PLUGIN_OK);

    check_equal(salts_plugin_registry_acquire(
                    &registry, provider_ref, &provider_lease,
                    &provider_manifest),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_acquire(
                    &registry, resource_ref, &resource_lease,
                    &resource_manifest),
                SALTS_PLUGIN_OK);
    check_true(salts_plugin_lease_valid(provider_lease));
    check_true(salts_plugin_lease_valid(resource_lease));

    check_equal(salts_plugin_manifest_find_export(
                    provider_manifest, "fixture.provider", &provider_entry),
                SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_export_require_interface(
            provider_entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION, 0u,
            turbo_flow_provider_factory_interface()),
        SALTS_PLUGIN_OK);

    factory =
        (turbo_flow_provider_factory *)provider_entry->value.interface.value;
    check_not_null(factory);
    check_true(turbo_flow_provider_factory_valid(factory));
    check_equal(turbo_flow_provider_factory_contract(factory, &contract),
                SALTS_OK);
    check_true(turbo_flow_provider_contract_valid(&contract));
    check_true(contract.config.message_artifact != NULL);
    check_true(contract.resource.contract_id != NULL);
    check_true(strcmp(contract.resource.contract_id,
                      FLOW_TEST_RESOURCE_CONTRACT_ID) == 0);
    check_equal(contract.resource.contract_version,
                FLOW_TEST_RESOURCE_CONTRACT_VERSION);
    check_equal(contract.resource.required_capabilities,
                (uint64_t)FLOW_TEST_RESOURCE_CAP_READ);
    check_true(cmeta_interface_desc_equal(
        contract.resource.expected_interface,
        flow_test_resource_interface()));

    check_equal(salts_plugin_manifest_find_export(
                    resource_manifest, "fixture.resource", &resource_entry),
                SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_export_require_interface(
            resource_entry, contract.resource.contract_id,
            contract.resource.contract_version,
            contract.resource.required_capabilities,
            contract.resource.expected_interface),
        SALTS_PLUGIN_OK);

    resource_view.identity = "db-main";
    resource_view.export_id = resource_entry->export_id;
    resource_view.interface_desc = resource_entry->value.interface.desc;
    resource_view.interface_value = resource_entry->value.interface.value;

    instance.instance_name = "stage_a";
    instance.config.type_name = "FixtureProviderConfig";
    instance.resource = &resource_view;
    check_equal(
        turbo_flow_provider_factory_preflight(factory, &instance, &error),
        SALTS_OK);

    check_equal(salts_plugin_registry_request_stop(&registry, provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_request_stop(&registry, resource_ref),
                SALTS_PLUGIN_OK);

    /* Each live lease independently blocks registry quiescence and unload. */
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, provider_ref, &provider_quiescent),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, resource_ref, &resource_quiescent),
                SALTS_PLUGIN_OK);
    check_false(provider_quiescent);
    check_false(resource_quiescent);
    check_equal(salts_plugin_registry_unload(&registry, provider_ref),
                SALTS_PLUGIN_BUSY);
    check_equal(salts_plugin_registry_unload(&registry, resource_ref),
                SALTS_PLUGIN_BUSY);

    check_equal(salts_plugin_registry_release(&registry, &provider_lease),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, provider_ref, &provider_quiescent),
                SALTS_PLUGIN_OK);
    check_true(provider_quiescent);
    check_equal(salts_plugin_registry_unload(&registry, provider_ref),
                SALTS_PLUGIN_OK);

    /* Resource remains pinned after provider module has already unloaded. */
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, resource_ref, &resource_quiescent),
                SALTS_PLUGIN_OK);
    check_false(resource_quiescent);
    check_equal(salts_plugin_registry_unload(&registry, resource_ref),
                SALTS_PLUGIN_BUSY);

    check_equal(salts_plugin_registry_release(&registry, &resource_lease),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, resource_ref, &resource_quiescent),
                SALTS_PLUGIN_OK);
    check_true(resource_quiescent);
    check_equal(salts_plugin_registry_unload(&registry, resource_ref),
                SALTS_PLUGIN_OK);

    check_equal(salts_plugin_registry_count(&registry), (size_t)0u);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
  }
}
