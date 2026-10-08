#ifndef FLOW_ADAPTER_COMPONENT_FIXTURE_H
#define FLOW_ADAPTER_COMPONENT_FIXTURE_H

#include "component_scope_fixture.h"
#include "turbo_flow_provider.h"

/* Existing IO plugins export factory Interfaces. This local deployment adapts
 * one such export without changing the production plugin ABI. Its outer lease
 * covers all projected callbacks until the Component generation is drained. */
cmeta_component(AdapterProviderFixture,
    cmeta_provides(turbo_flow_provider_factory));

typedef struct adapter_component_fixture_s {
  component_fixture_t component;
  cmeta_plugin_registry *registry;
  cmeta_plugin_lease lease;
  turbo_flow_provider_factory factory;
  int anchor;
  cmeta_object_interface_provider interfaces;
  salts_component_provider_binding binding;
} adapter_component_fixture_t;

static cmeta_status adapter_component_project(
    void *context, const cmeta_object_ref *object,
    const cmeta_interface_desc *expected, cmeta_interface_projection *out) {
  adapter_component_fixture_t *fixture = context;
  if (!fixture || !object || !out) return CMETA_INVALID_ARGUMENT;
  if (!cmeta_interface_desc_equal(expected,
          turbo_flow_provider_factory_interface())) return CMETA_TRAIT_MISSING;
  *out = (cmeta_interface_projection){sizeof(*out),
      turbo_flow_provider_factory_interface(),
      fixture->factory.self, fixture->factory.vtable};
  return CMETA_OK;
}

static cmeta_status SALTS_COMPONENT_CALL adapter_component_create(
    void *context, const cmeta_data_desc *config_data, const void *config_value,
    const salts_component_dependency *dependencies, size_t dependency_count,
    cmeta_object_ref *out) {
  adapter_component_fixture_t *fixture = context;
  (void)dependencies;
  if (!fixture || config_data || config_value || dependency_count)
    return CMETA_INVALID_ARGUMENT;
  return cmeta_object_borrow(out, &fixture->anchor, &cmeta_data_int, NULL);
}

static int adapter_component_open(adapter_component_fixture_t *fixture,
    cmeta_plugin_registry *registry, cmeta_plugin_ref plugin,
    const char *export_id) {
  const cmeta_plugin_manifest *manifest = NULL;
  const cmeta_plugin_export *entry = NULL;
  component_fixture_t *component = &fixture->component;
  const salts_component_plugin_generation_storage storage = {
      component->deployments, 1u, component->instances, 1u,
      component->dependencies, 1u, component->activation_order, 1u,
      component->modules, 1u};
  salts_component_plugin_generation *previous = NULL;
  salts_component_deployment deployment;

  fixture->registry = registry;
  if (cmeta_plugin_registry_acquire(registry, plugin, &fixture->lease,
          &manifest) != CMETA_PLUGIN_OK ||
      cmeta_plugin_manifest_find_export(manifest, export_id,
          &entry) != CMETA_PLUGIN_OK ||
      cmeta_plugin_export_require_interface(entry,
          TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
          TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION, 0u,
          turbo_flow_provider_factory_interface()) != CMETA_PLUGIN_OK)
    return SALTS_EPROTO;
  fixture->factory = *(const turbo_flow_provider_factory *)
      entry->value.interface.value;
  fixture->interfaces = (cmeta_object_interface_provider){
      sizeof(fixture->interfaces), fixture, adapter_component_project};
  fixture->binding = (salts_component_provider_binding){
      sizeof(fixture->binding), SALTS_COMPONENT_PROVIDER_BINDING_ABI_VERSION,
      cmeta_component_meta(AdapterProviderFixture), fixture,
      &fixture->interfaces, adapter_component_create, NULL, NULL};
  deployment = (salts_component_deployment){&fixture->binding, NULL, NULL};
  if (salts_component_plugin_generation_build(&component->generation,
          UINT64_C(1), registry, &storage, &deployment, 1u, NULL, 0u,
          NULL, 0u) != SALTS_COMPONENT_PLUGIN_OK ||
      salts_component_plugin_runtime_init(&component->runtime) !=
          SALTS_COMPONENT_PLUGIN_OK ||
      salts_component_plugin_runtime_publish(&component->runtime,
          &component->generation, &previous) != SALTS_COMPONENT_PLUGIN_OK ||
      previous != NULL ||
      salts_component_plugin_scope_acquire(&component->runtime,
          &component->scope) != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EPROTO;
  return SALTS_OK;
}

static int adapter_component_close(adapter_component_fixture_t *fixture) {
  if (component_fixture_close(&fixture->component) != SALTS_OK)
    return SALTS_EPROTO;
  if (fixture->component.generation.state ==
          SALTS_COMPONENT_PLUGIN_GENERATION_BUILT &&
      salts_component_plugin_generation_discard(&fixture->component.generation)
          != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EPROTO;
  if (cmeta_plugin_lease_valid(fixture->lease) &&
      cmeta_plugin_registry_release(fixture->registry, &fixture->lease) !=
          CMETA_PLUGIN_OK)
    return SALTS_EPROTO;
  return SALTS_OK;
}

#endif
