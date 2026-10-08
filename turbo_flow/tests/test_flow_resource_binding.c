#include "tinytest.h"
#include "turbo_flow_resource.h"
#include "salts_resource_fixture.h"

#include <salts/plugin.h>

#include <string.h>

#ifndef FLOW_SALTS_RESOURCE_FIXTURE
#error "FLOW_SALTS_RESOURCE_FIXTURE is required"
#endif

typedef struct resource_resolver_fixture_s {
  cmeta_plugin_registry *registry;
  cmeta_plugin_ref plugin;
  unsigned calls;
} resource_resolver_fixture_t;

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
  if (!resource_name || strcmp(resource_name, "db_main") != 0) {
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

static turbo_flow_provider_resource_requirement_v1_t
resource_requirement(void) {
  turbo_flow_provider_resource_requirement_v1_t requirement =
      TURBO_FLOW_PROVIDER_RESOURCE_REQUIREMENT_V1_INIT;
  requirement.contract_id = FLOW_TEST_RESOURCE_CONTRACT_ID;
  requirement.contract_version = FLOW_TEST_RESOURCE_CONTRACT_VERSION;
  requirement.required_capabilities = FLOW_TEST_RESOURCE_CAP_READ;
  requirement.expected_interface = flow_test_resource_interface();
  return requirement;
}

spec("TurboFlow deployment resource binding") {
  it("acquires exact Interface and keeps the resource module leased") {
    cmeta_plugin_registry registry = {0};
    cmeta_plugin_registry_config registry_config = {1u};
    cmeta_plugin_ref resource_ref = {0};
    resource_resolver_fixture_t fixture = {0};
    turbo_flow_resource_resolver_v1_t resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    turbo_flow_provider_resource_requirement_v1_t requirement =
        resource_requirement();
    turbo_flow_resource_binding_t *binding = NULL;
    turbo_flow_provider_resource_view_v1_t view =
        TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    flow_test_resource *resource;
    bool quiescent = false;

    check_equal(cmeta_plugin_registry_init(&registry, &registry_config),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_load(
                    &registry, FLOW_SALTS_RESOURCE_FIXTURE, &resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_start(&registry, resource_ref),
                CMETA_PLUGIN_OK);

    fixture.registry = &registry;
    fixture.plugin = resource_ref;
    resolver.ctx = &fixture;
    resolver.resolve = resolve_resource;

    check_equal(turbo_flow_resource_binding_acquire(
                    &resolver, "db_main", &requirement, &binding, &error),
                SALTS_OK);
    check_not_null(binding);
    check_equal(fixture.calls, 1u);

    check_equal(turbo_flow_resource_binding_view(binding, &view), SALTS_OK);
    check_true(strcmp(view.reference_name, "db_main") == 0);
    check_true(strcmp(view.identity, "deployment.db_main") == 0);
    check_true(strcmp(view.export_id, "fixture.resource") == 0);
    check_true(cmeta_interface_desc_equal(
        view.interface_desc, flow_test_resource_interface()));
    resource = (flow_test_resource *)view.interface_value;
    check_not_null(resource);
    check_true(flow_test_resource_valid(resource));
    check_equal(flow_test_resource_ping(resource), 7);

    check_equal(cmeta_plugin_registry_request_stop(&registry, resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_poll_quiescent(
                    &registry, resource_ref, &quiescent),
                CMETA_PLUGIN_OK);
    check_false(quiescent);
    check_equal(cmeta_plugin_registry_unload(&registry, resource_ref),
                CMETA_PLUGIN_BUSY);

    check_equal(turbo_flow_resource_binding_release(&binding), SALTS_OK);
    check_null(binding);
    check_equal(cmeta_plugin_registry_poll_quiescent(
                    &registry, resource_ref, &quiescent),
                CMETA_PLUGIN_OK);
    check_true(quiescent);
    check_equal(cmeta_plugin_registry_unload(&registry, resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_destroy(&registry), CMETA_PLUGIN_OK);
  }

  it("rejects an incompatible exact resource contract and releases its lease") {
    cmeta_plugin_registry registry = {0};
    cmeta_plugin_registry_config registry_config = {1u};
    cmeta_plugin_ref resource_ref = {0};
    resource_resolver_fixture_t fixture = {0};
    turbo_flow_resource_resolver_v1_t resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    turbo_flow_provider_resource_requirement_v1_t requirement =
        resource_requirement();
    turbo_flow_resource_binding_t *binding = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    bool quiescent = false;

    check_equal(cmeta_plugin_registry_init(&registry, &registry_config),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_load(
                    &registry, FLOW_SALTS_RESOURCE_FIXTURE, &resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_start(&registry, resource_ref),
                CMETA_PLUGIN_OK);

    fixture.registry = &registry;
    fixture.plugin = resource_ref;
    resolver.ctx = &fixture;
    resolver.resolve = resolve_resource;
    ++requirement.contract_version;

    check_equal(turbo_flow_resource_binding_acquire(
                    &resolver, "db_main", &requirement, &binding, &error),
                SALTS_EPROTO);
    check_null(binding);
    check_equal(error.status, SALTS_EPROTO);

    check_equal(cmeta_plugin_registry_request_stop(&registry, resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_poll_quiescent(
                    &registry, resource_ref, &quiescent),
                CMETA_PLUGIN_OK);
    check_true(quiescent);
    check_equal(cmeta_plugin_registry_unload(&registry, resource_ref),
                CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_destroy(&registry), CMETA_PLUGIN_OK);
  }

  it("propagates explicit missing-resource failure without plugin discovery") {
    resource_resolver_fixture_t fixture = {0};
    turbo_flow_resource_resolver_v1_t resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    turbo_flow_provider_resource_requirement_v1_t requirement =
        resource_requirement();
    turbo_flow_resource_binding_t *binding = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    resolver.ctx = &fixture;
    resolver.resolve = resolve_resource;
    check_equal(turbo_flow_resource_binding_acquire(
                    &resolver, "missing", &requirement, &binding, &error),
                SALTS_ENOENT);
    check_null(binding);
    check_equal(fixture.calls, 1u);
    check_equal(error.status, SALTS_ENOENT);
  }
}
