#include "tinytest.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"
#include "salts_resource_fixture.h"
#include "provider_config_native.h"

#include <salts/component_plugin.h>
#include <salts/plugin.h>

#include <string.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_SALTS_RESOURCE_FIXTURE
#error "FLOW_SALTS_RESOURCE_FIXTURE is required"
#endif

#define FLOW_TEST_PROVIDER_COMPONENT_ID "TurboFlowFixtureProvider"
#define FLOW_TEST_PROVIDER_EXPORT_ID "fixture.provider"

typedef struct provider_resolver_fixture_s {
  unsigned calls;
  int accept_any_identity;
  const char *component_identity;
} provider_resolver_fixture_t;

typedef struct resource_resolver_fixture_s {
  cmeta_plugin_registry *registry;
  cmeta_plugin_ref plugin;
  unsigned calls;
} resource_resolver_fixture_t;

#include "component_scope_fixture.h"

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resource_resolver_fixture_t *fixture =
      (resource_resolver_fixture_t *)ctx;
  (void)requirement;
  if (!fixture || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->calls;
  if (!resource_name || strcmp(resource_name, FLOW_TEST_RESOURCE_IDENTITY) != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->identity = "deployment.db_main";
  out->registry = fixture->registry;
  out->plugin = fixture->plugin;
  out->export_id = "fixture.resource";
  return SALTS_OK;
}

static int resolve_provider(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v2_t *out,
    turbo_flow_config_error_t *error) {
  provider_resolver_fixture_t *fixture =
      (provider_resolver_fixture_t *)ctx;
  if (!fixture || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->calls;
  if (!provider_identity ||
      (!fixture->accept_any_identity &&
       strcmp(provider_identity, "fixture.provider") != 0)) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->module_identity = "test.turboflow.provider";
  out->component_identity = fixture->component_identity;
  return SALTS_OK;
}

static void stop_unload(
    cmeta_plugin_registry *registry,
    cmeta_plugin_ref ref) {
  bool quiescent = false;
  check_equal(
      cmeta_plugin_registry_request_stop(registry, ref),
      CMETA_PLUGIN_OK);
  check_equal(
      cmeta_plugin_registry_poll_quiescent(
          registry, ref, &quiescent),
      CMETA_PLUGIN_OK);
  check_true(quiescent);
  check_equal(
      cmeta_plugin_registry_unload(registry, ref),
      CMETA_PLUGIN_OK);
}

spec("TurboFlow Component provider binding") {
  it("borrows the provider factory from one pinned Component generation") {
    cmeta_plugin_registry registry = {0};
    const cmeta_plugin_registry_config registry_config = {2u};
    cmeta_plugin_ref provider_ref = {0};
    cmeta_plugin_ref resource_ref = {0};
    component_fixture_t component = {0};
    provider_resolver_fixture_t fixture = {0};
    resource_resolver_fixture_t resource_fixture = {0};
    turbo_flow_provider_resolver_v2_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V2_INIT;
    turbo_flow_resource_resolver_v1_t resource_resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    turbo_flow_provider_binding_t *binding = NULL;
    turbo_flow_resource_binding_t *resource_binding = NULL;
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    turbo_flow_provider_resource_view_v1_t resource_view =
        TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
    turbo_flow_provider_instance_v1_t instance =
        TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    DataBindNativeTypeBinding native = {0};
    DataBindError databind_error = DATA_BIND_ERROR_INIT;
    BatchConfig_t config;
    const DataBindMessageNativeArtifact *artifact;
    cmeta_plugin_lifecycle_info info;
    bool quiescent = true;

    check_equal(cmeta_plugin_registry_init(&registry, &registry_config),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_load(
                    &registry, FLOW_SALTS_RESOURCE_FIXTURE, &resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_start(&registry, provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_start(&registry, resource_ref),
                CMETA_PLUGIN_OK);

    check_equal(component_fixture_open(
                    &component, &registry, provider_ref),
                SALTS_OK);
    check_equal(
        salts_component_plugin_scope_generation_id(&component.scope),
        UINT64_C(1));

    fixture.component_identity = FLOW_TEST_PROVIDER_COMPONENT_ID;
    resolver.ctx = &fixture;
    resolver.resolve = resolve_provider;

    check_equal(turbo_flow_provider_binding_acquire(
                    &component.scope, &resolver,
                    "fixture.provider", &binding, &error),
                SALTS_OK);
    check_not_null(binding);
    check_equal(fixture.calls, 1u);

    check_equal(turbo_flow_provider_binding_contract(binding, &contract),
                SALTS_OK);
    check_true(turbo_flow_provider_contract_valid(&contract));
    check_true(strcmp(contract.config.message_artifact->type_name,
                      "BatchConfig") == 0);

    resource_fixture.registry = &registry;
    resource_fixture.plugin = resource_ref;
    resource_resolver.ctx = &resource_fixture;
    resource_resolver.resolve = resolve_resource;
    check_equal(turbo_flow_resource_binding_acquire(
                    &resource_resolver, FLOW_TEST_RESOURCE_IDENTITY,
                    &contract.resource, &resource_binding, &error),
                SALTS_OK);
    check_not_null(resource_binding);
    check_equal(turbo_flow_resource_binding_view(
                    resource_binding, &resource_view),
                SALTS_OK);

    artifact = contract.config.message_artifact;
    check_not_null(artifact);
    check_equal(
        artifact->native_binding(&native, &databind_error),
        DATA_BIND_OK);
    BatchConfig_init(&config);
    config.batch = 7u;
    config.concurrency = 2u;

    instance.instance_name = "stage_a";
    instance.config.type_name = artifact->type_name;
    instance.config.data = native.data;
    instance.config.value = &config;
    instance.config.value_bytes = sizeof(config);
    instance.resource = &resource_view;
    check_equal(turbo_flow_provider_binding_preflight(
                    binding, &instance, &error),
                SALTS_OK);

    check_equal(cmeta_plugin_registry_get_lifecycle(
                    &registry, provider_ref, &info),
                CMETA_PLUGIN_OK);
    check_equal(info.active_leases, (size_t)1u);

    check_equal(cmeta_plugin_registry_request_stop(
                    &registry, provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_poll_quiescent(
                    &registry, provider_ref, &quiescent),
                CMETA_PLUGIN_OK);
    check_false(quiescent);
    check_equal(cmeta_plugin_registry_unload(
                    &registry, provider_ref),
                CMETA_PLUGIN_BUSY);

    check_equal(turbo_flow_provider_binding_release(&binding), SALTS_OK);
    check_null(binding);

    /* Binding owns no module lease; Component generation still pins the DSO. */
    check_equal(cmeta_plugin_registry_get_lifecycle(
                    &registry, provider_ref, &info),
                CMETA_PLUGIN_OK);
    check_equal(info.active_leases, (size_t)1u);

    check_equal(turbo_flow_resource_binding_release(&resource_binding), SALTS_OK);
    check_null(resource_binding);

    check_equal(component_fixture_close(&component), SALTS_OK);
    check_equal(cmeta_plugin_registry_get_lifecycle(
                    &registry, provider_ref, &info),
                CMETA_PLUGIN_OK);
    check_equal(info.active_leases, (size_t)0u);

    check_equal(cmeta_plugin_registry_poll_quiescent(
                    &registry, provider_ref, &quiescent),
                CMETA_PLUGIN_OK);
    check_true(quiescent);
    check_equal(cmeta_plugin_registry_unload(
                    &registry, provider_ref),
                CMETA_PLUGIN_OK);

    stop_unload(&registry, resource_ref);
    BatchConfig_clear(&config);
    check_equal(cmeta_plugin_registry_destroy(&registry), CMETA_PLUGIN_OK);
  }

  it("rejects an explicit component alias that is not in the pinned generation") {
    cmeta_plugin_registry registry = {0};
    const cmeta_plugin_registry_config registry_config = {1u};
    cmeta_plugin_ref provider_ref = {0};
    component_fixture_t component = {0};
    provider_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v2_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V2_INIT;
    turbo_flow_provider_binding_t *binding = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(cmeta_plugin_registry_init(&registry, &registry_config),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_start(&registry, provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(component_fixture_open(
                    &component, &registry, provider_ref),
                SALTS_OK);

    fixture.accept_any_identity = 1;
    fixture.component_identity = "MissingProviderComponent";
    resolver.ctx = &fixture;
    resolver.resolve = resolve_provider;

    check_equal(turbo_flow_provider_binding_acquire(
                    &component.scope, &resolver,
                    "alternate.provider", &binding, &error),
                SALTS_ENOENT);
    check_null(binding);

    check_equal(component_fixture_close(&component), SALTS_OK);
    stop_unload(&registry, provider_ref);
    check_equal(cmeta_plugin_registry_destroy(&registry), CMETA_PLUGIN_OK);
  }

  it("propagates deployment alias failure before Component lookup") {
    cmeta_plugin_registry registry = {0};
    const cmeta_plugin_registry_config registry_config = {1u};
    cmeta_plugin_ref provider_ref = {0};
    component_fixture_t component = {0};
    provider_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v2_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V2_INIT;
    turbo_flow_provider_binding_t *binding = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(cmeta_plugin_registry_init(&registry, &registry_config),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_start(&registry, provider_ref),
                CMETA_PLUGIN_OK);
    check_equal(component_fixture_open(
                    &component, &registry, provider_ref),
                SALTS_OK);

    fixture.component_identity = FLOW_TEST_PROVIDER_COMPONENT_ID;
    resolver.ctx = &fixture;
    resolver.resolve = resolve_provider;

    check_equal(turbo_flow_provider_binding_acquire(
                    &component.scope, &resolver,
                    "missing.provider", &binding, &error),
                SALTS_ENOENT);
    check_null(binding);
    check_equal(fixture.calls, 1u);
    check_equal(error.status, SALTS_ENOENT);

    check_equal(component_fixture_close(&component), SALTS_OK);
    stop_unload(&registry, provider_ref);
    check_equal(cmeta_plugin_registry_destroy(&registry), CMETA_PLUGIN_OK);
  }
}
