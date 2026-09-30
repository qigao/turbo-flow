#include "tinytest.h"
#include "turbo_flow_deployment_resource.h"

#include <string.h>

#define FIXTURE_RESOURCE_METHODS(X, I) \
  X(I, R0, int, health, _)

CMETA_INTERFACE(fixture_resource, FIXTURE_RESOURCE_METHODS);

typedef struct resolver_state_s {
  salts_plugin_registry registry;
  unsigned calls;
} resolver_state_t;

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_deployment_resource_requirement_v1_t *requirement,
    turbo_flow_deployment_resource_reference_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_state_t *state = (resolver_state_t *)ctx;
  static const char identity[] = "deployment.telemetry-db";
  static const char export_id[] = "telemetry.db";

  if (!state || !resource_name || !out ||
      !turbo_flow_deployment_resource_requirement_valid(requirement))
    return SALTS_EINVAL;
  if (strcmp(resource_name, "telemetry_db") != 0) return SALTS_ENOENT;
  if (out->size != sizeof(*out)) return SALTS_EINVAL;

  ++state->calls;
  memset(out->identity, 0, sizeof(out->identity));
  memcpy(out->identity, identity, sizeof(identity));
  out->registry = &state->registry;
  out->plugin = (salts_plugin_ref){1u, 7u};
  memset(out->export_id, 0, sizeof(out->export_id));
  memcpy(out->export_id, export_id, sizeof(export_id));
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

spec("TurboFlow deployment resource resolver") {
  it("returns only bounded non-secret Salts Plugin coordinates") {
    resolver_state_t state = {0};
    turbo_flow_deployment_resource_resolver_v1_t resolver =
        TURBO_FLOW_DEPLOYMENT_RESOURCE_RESOLVER_V1_INIT;
    turbo_flow_deployment_resource_requirement_v1_t requirement =
        TURBO_FLOW_DEPLOYMENT_RESOURCE_REQUIREMENT_V1_INIT;
    turbo_flow_deployment_resource_reference_v1_t reference =
        TURBO_FLOW_DEPLOYMENT_RESOURCE_REFERENCE_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    resolver.ctx = &state;
    resolver.resolve = resolve_resource;
    requirement.contract_id = "turbo_flow.fixture.resource";
    requirement.contract_version = 1u;
    requirement.required_capabilities = UINT64_C(4);
    requirement.expected_interface = fixture_resource_interface();

    check_true(turbo_flow_deployment_resource_resolver_valid(&resolver));
    check_true(turbo_flow_deployment_resource_requirement_valid(&requirement));
    check_equal(
        resolver.resolve(resolver.ctx, "telemetry_db", &requirement,
                         &reference, &error),
        SALTS_OK);
    check_true(turbo_flow_deployment_resource_reference_valid(&reference));
    check_equal(reference.identity, "deployment.telemetry-db");
    check_equal(reference.export_id, "telemetry.db");
    check_equal(reference.plugin.slot, 1u);
    check_equal(reference.plugin.generation, 7u);
    check_equal(state.calls, 1u);

    /* Resolver returns coordinates only; a lease is acquired by generation. */
    check_equal(salts_plugin_registry_count(&state.registry), (size_t)0u);
  }

  it("rejects incomplete exact resource requirements") {
    turbo_flow_deployment_resource_requirement_v1_t requirement =
        TURBO_FLOW_DEPLOYMENT_RESOURCE_REQUIREMENT_V1_INIT;

    check_false(turbo_flow_deployment_resource_requirement_valid(&requirement));
    requirement.contract_id = "turbo_flow.fixture.resource";
    requirement.contract_version = 1u;
    check_false(turbo_flow_deployment_resource_requirement_valid(&requirement));
    requirement.expected_interface = fixture_resource_interface();
    check_true(turbo_flow_deployment_resource_requirement_valid(&requirement));
  }

  it("rejects references without stable identity or Plugin coordinates") {
    turbo_flow_deployment_resource_reference_v1_t reference =
        TURBO_FLOW_DEPLOYMENT_RESOURCE_REFERENCE_V1_INIT;
    salts_plugin_registry registry = {0};

    check_false(turbo_flow_deployment_resource_reference_valid(&reference));
    memcpy(reference.identity, "resource", sizeof("resource"));
    reference.registry = &registry;
    reference.plugin = (salts_plugin_ref){1u, 1u};
    memcpy(reference.export_id, "resource.export", sizeof("resource.export"));
    check_true(turbo_flow_deployment_resource_reference_valid(&reference));
  }
}
