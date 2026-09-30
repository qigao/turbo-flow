#include "tinytest.h"
#include "turbo_flow_provider_binding.h"

#include <salts/plugin.h>

#include <string.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif

typedef struct provider_resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  unsigned calls;
  int accept_any_identity;
} provider_resolver_fixture_t;

static int resolve_provider(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v1_t *out,
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
  out->registry = fixture->registry;
  out->plugin = fixture->plugin;
  return SALTS_OK;
}

spec("TurboFlow Salts provider binding") {
  it("retains the exact provider factory Interface under one module lease") {
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config registry_config = {1u};
    salts_plugin_ref ref = {0};
    provider_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_provider_binding_t *binding = NULL;
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    turbo_flow_provider_instance_v1_t instance =
        TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    bool quiescent = true;

    check_equal(salts_plugin_registry_init(&registry, &registry_config),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, ref),
                SALTS_PLUGIN_OK);

    fixture.registry = &registry;
    fixture.plugin = ref;
    resolver.ctx = &fixture;
    resolver.resolve = resolve_provider;

    check_equal(turbo_flow_provider_binding_acquire(
                    &resolver, "fixture.provider", &binding, &error),
                SALTS_OK);
    check_not_null(binding);
    check_equal(fixture.calls, 1u);

    check_equal(turbo_flow_provider_binding_contract(binding, &contract),
                SALTS_OK);
    check_true(turbo_flow_provider_contract_valid(&contract));
    check_true(strcmp(contract.config.message_artifact->type_name,
                      "FixtureProviderConfig") == 0);

    instance.instance_name = "stage_a";
    instance.config.type_name = "FixtureProviderConfig";
    check_equal(turbo_flow_provider_binding_preflight(
                    binding, &instance, &error),
                SALTS_OK);

    check_equal(salts_plugin_registry_request_stop(&registry, ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, ref, &quiescent),
                SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(salts_plugin_registry_unload(&registry, ref),
                SALTS_PLUGIN_BUSY);

    check_equal(turbo_flow_provider_binding_release(&binding), SALTS_OK);
    check_null(binding);

    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, ref, &quiescent),
                SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(salts_plugin_registry_unload(&registry, ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
  }

  it("uses the .flow provider identity as the exact Salts export id") {
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config registry_config = {1u};
    salts_plugin_ref ref = {0};
    provider_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_provider_binding_t *binding = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    bool quiescent = false;

    check_equal(salts_plugin_registry_init(&registry, &registry_config),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, ref),
                SALTS_PLUGIN_OK);

    fixture.registry = &registry;
    fixture.plugin = ref;
    fixture.accept_any_identity = 1;
    resolver.ctx = &fixture;
    resolver.resolve = resolve_provider;

    /* Resolver may select a module, but may not alias semantic export identity. */
    check_equal(turbo_flow_provider_binding_acquire(
                    &resolver, "alternate.provider", &binding, &error),
                SALTS_ENOENT);
    check_null(binding);

    check_equal(salts_plugin_registry_request_stop(&registry, ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_poll_quiescent(
                    &registry, ref, &quiescent),
                SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(salts_plugin_registry_unload(&registry, ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
  }

  it("propagates explicit missing-provider failure without registry discovery") {
    provider_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_provider_binding_t *binding = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    resolver.ctx = &fixture;
    resolver.resolve = resolve_provider;

    check_equal(turbo_flow_provider_binding_acquire(
                    &resolver, "missing.provider", &binding, &error),
                SALTS_ENOENT);
    check_null(binding);
    check_equal(fixture.calls, 1u);
    check_equal(error.status, SALTS_ENOENT);
  }
}
