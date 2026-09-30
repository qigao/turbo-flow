#include "tinytest.h"
#include "turbo_flow_provider_resolver.h"

#include <string.h>

typedef struct resolver_fixture_s {
  salts_plugin_registry registry;
  unsigned provider_calls;
  unsigned resource_calls;
} resolver_fixture_t;

static int resolve_provider(void *ctx, const char *provider_identity,
                            turbo_flow_provider_export_ref_v1_t *out,
                            turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !provider_identity || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  if (strcmp(provider_identity, "fixture.provider") != 0)
    return SALTS_ENOENT;

  ++fixture->provider_calls;
  out->registry = &fixture->registry;
  out->plugin = (salts_plugin_ref){1u, 7u};
  out->export_id = "fixture.provider";
  out->identity = "provider/fixture.provider";
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_provider_export_ref_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !resource_name || !requirement || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  if (strcmp(resource_name, "db-main") != 0 ||
      !requirement->contract_id ||
      strcmp(requirement->contract_id, "test.resource") != 0 ||
      requirement->contract_version != 3u ||
      requirement->required_capabilities != UINT64_C(4) ||
      requirement->expected_interface != turbo_flow_runtime_owner_interface())
    return SALTS_EPROTO;

  ++fixture->resource_calls;
  out->registry = &fixture->registry;
  out->plugin = (salts_plugin_ref){2u, 9u};
  out->export_id = "resource.db";
  out->identity = "prod/db-main";
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

spec("TurboFlow provider/resource resolver contract") {
  it("returns explicit already-loaded Salts Plugin export references") {
    resolver_fixture_t fixture = {{(void *)1}, 0u, 0u};
    turbo_flow_provider_resolver_v1_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_provider_export_ref_v1_t provider =
        TURBO_FLOW_PROVIDER_EXPORT_REF_V1_INIT;
    turbo_flow_provider_export_ref_v1_t resource =
        TURBO_FLOW_PROVIDER_EXPORT_REF_V1_INIT;
    turbo_flow_provider_resource_requirement_v1_t requirement =
        TURBO_FLOW_PROVIDER_RESOURCE_REQUIREMENT_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    resolver.ctx = &fixture;
    resolver.resolve_provider = resolve_provider;
    resolver.resolve_resource = resolve_resource;
    check_true(turbo_flow_provider_resolver_valid(&resolver));

    check_equal(resolver.resolve_provider(
                    resolver.ctx, "fixture.provider", &provider, &error),
                SALTS_OK);
    check_true(turbo_flow_provider_export_ref_valid(&provider));
    check_true(provider.registry == &fixture.registry);
    check_equal(provider.plugin.slot, 1u);
    check_equal(provider.plugin.generation, 7u);
    check_equal(provider.export_id, "fixture.provider");
    check_equal(provider.identity, "provider/fixture.provider");

    requirement.contract_id = "test.resource";
    requirement.contract_version = 3u;
    requirement.required_capabilities = UINT64_C(4);
    requirement.expected_interface = turbo_flow_runtime_owner_interface();
    check_true(turbo_flow_provider_resource_requirement_valid(&requirement));

    check_equal(resolver.resolve_resource(
                    resolver.ctx, "db-main", &requirement, &resource, &error),
                SALTS_OK);
    check_true(turbo_flow_provider_export_ref_valid(&resource));
    check_true(resource.registry == &fixture.registry);
    check_equal(resource.plugin.slot, 2u);
    check_equal(resource.plugin.generation, 9u);
    check_equal(resource.export_id, "resource.db");
    check_equal(resource.identity, "prod/db-main");

    check_equal(fixture.provider_calls, 1u);
    check_equal(fixture.resource_calls, 1u);
  }

  it("requires explicit provider resolution and never implies resource fallback") {
    turbo_flow_provider_resolver_v1_t resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_provider_export_ref_v1_t ref =
        TURBO_FLOW_PROVIDER_EXPORT_REF_V1_INIT;

    check_false(turbo_flow_provider_resolver_valid(&resolver));
    check_false(turbo_flow_provider_export_ref_valid(&ref));

    resolver.resolve_provider = resolve_provider;
    check_true(turbo_flow_provider_resolver_valid(&resolver));
    check_null(resolver.resolve_resource);
  }
}
