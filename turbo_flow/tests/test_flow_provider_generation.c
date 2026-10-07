#include "flow_provider_generation_internal.h"
#include "salts_resource_fixture.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <salts/component_plugin.h>
#include <salts/plugin.h>

#include <stdbool.h>
#include <string.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_SALTS_RESOURCE_FIXTURE
#error "FLOW_SALTS_RESOURCE_FIXTURE is required"
#endif

#define FLOW_TEST_PROVIDER_COMPONENT_ID "TurboFlowFixtureProvider"
#define FLOW_TEST_PROVIDER_EXPORT_ID "fixture.provider"

typedef struct generation_resolver_fixture_s {
  cmeta_plugin_registry *registry;
  cmeta_plugin_ref resource;
  unsigned provider_calls;
  unsigned resource_calls;
} generation_resolver_fixture_t;

typedef struct component_generation_fixture_s {
  salts_component_plugin_generation generation;
  salts_component_deployment deployments[1];
  salts_component_instance instances[1];
  salts_component_dependency dependencies[1];
  size_t activation_order[1];
  salts_component_plugin_module modules[1];
  salts_component_plugin_runtime runtime;
} component_generation_fixture_t;

static int component_generation_open(
    component_generation_fixture_t *fixture,
    cmeta_plugin_registry *registry,
    cmeta_plugin_ref provider_ref,
    uint64_t generation_id) {
  const salts_component_plugin_generation_storage storage = {
      fixture->deployments, 1u,
      fixture->instances, 1u,
      fixture->dependencies, 1u,
      fixture->activation_order, 1u,
      fixture->modules, 1u,
  };
  const salts_component_plugin_source source = {
      provider_ref,
      FLOW_TEST_PROVIDER_EXPORT_ID,
      NULL,
      NULL,
  };
  salts_component_plugin_generation *previous = NULL;

  memset(fixture, 0, sizeof(*fixture));
  if (salts_component_plugin_generation_build(
          &fixture->generation,
          generation_id,
          registry,
          &storage,
          NULL, 0u,
          &source, 1u,
          NULL, 0u) != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EPROTO;

  if (salts_component_plugin_runtime_init(
          &fixture->runtime) != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EPROTO;

  if (salts_component_plugin_runtime_publish(
          &fixture->runtime,
          &fixture->generation,
          &previous) != SALTS_COMPONENT_PLUGIN_OK ||
      previous != NULL)
    return SALTS_EPROTO;

  return SALTS_OK;
}

static int component_generation_close(
    component_generation_fixture_t *fixture) {
  salts_component_plugin_generation *previous = NULL;

  if (fixture->runtime.initialized) {
    if (fixture->runtime.current != NULL) {
      if (salts_component_plugin_runtime_close(
              &fixture->runtime,
              &previous) != SALTS_COMPONENT_PLUGIN_OK ||
          previous != &fixture->generation)
        return SALTS_EPROTO;
    }
    if (fixture->generation.state ==
        SALTS_COMPONENT_PLUGIN_GENERATION_DRAINING) {
      if (salts_component_plugin_generation_drain(
              &fixture->runtime,
              &fixture->generation) != SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
    }
    if (salts_component_plugin_runtime_destroy(
            &fixture->runtime) != SALTS_COMPONENT_PLUGIN_OK)
      return SALTS_EPROTO;
  }

  return SALTS_OK;
}

static int resolve_provider(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  generation_resolver_fixture_t *fixture =
      (generation_resolver_fixture_t *)ctx;
  if (!fixture || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->provider_calls;
  if (!provider_identity ||
      strcmp(provider_identity, "fixture.provider") != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->module_identity = "test.turboflow.provider";
  out->component_identity = FLOW_TEST_PROVIDER_COMPONENT_ID;
  return SALTS_OK;
}

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  generation_resolver_fixture_t *fixture =
      (generation_resolver_fixture_t *)ctx;
  if (!fixture || !requirement || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->resource_calls;
  if (!resource_name ||
      strcmp(resource_name, FLOW_TEST_RESOURCE_IDENTITY) != 0 ||
      !requirement->contract_id ||
      strcmp(requirement->contract_id,
             FLOW_TEST_RESOURCE_CONTRACT_ID) != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->identity = "deployment.db_main";
  out->registry = fixture->registry;
  out->plugin = fixture->resource;
  out->export_id = "fixture.resource";
  return SALTS_OK;
}

static void setup_resolvers(
    generation_resolver_fixture_t *fixture,
    turbo_flow_provider_resolver_v1_t *provider_resolver,
    turbo_flow_resource_resolver_v1_t *resource_resolver) {
  *provider_resolver =
      (turbo_flow_provider_resolver_v1_t)
          TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  provider_resolver->ctx = fixture;
  provider_resolver->resolve = resolve_provider;

  *resource_resolver =
      (turbo_flow_resource_resolver_v1_t)
          TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  resource_resolver->ctx = fixture;
  resource_resolver->resolve = resolve_resource;
}

static void start_registry(
    cmeta_plugin_registry *registry,
    cmeta_plugin_ref *provider,
    cmeta_plugin_ref *resource) {
  const cmeta_plugin_registry_config config = {2u};

  check_equal(
      cmeta_plugin_registry_init(registry, &config),
      CMETA_PLUGIN_OK);
  check_equal(
      cmeta_plugin_registry_load(
          registry, FLOW_SALTS_PROVIDER_FIXTURE, provider),
      CMETA_PLUGIN_OK);
  check_equal(
      cmeta_plugin_registry_load(
          registry, FLOW_SALTS_RESOURCE_FIXTURE, resource),
      CMETA_PLUGIN_OK);
  check_equal(
      cmeta_plugin_registry_start(registry, *provider),
      CMETA_PLUGIN_OK);
  check_equal(
      cmeta_plugin_registry_start(registry, *resource),
      CMETA_PLUGIN_OK);
}

static void stop_unload(
    cmeta_plugin_registry *registry, cmeta_plugin_ref ref) {
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

static size_t active_leases(
    cmeta_plugin_registry *registry, cmeta_plugin_ref ref) {
  cmeta_plugin_lifecycle_info info;
  check_equal(
      cmeta_plugin_registry_get_lifecycle(registry, ref, &info),
      CMETA_PLUGIN_OK);
  return info.active_leases;
}

spec("Component-backed provider generation aggregate") {
  it("accepts an empty provider set without a Component runtime") {
    static const char *src = "source input\n";
    flow_provider_generation_t *generation = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(
        turbo_flow_parse_string(flow, src, strlen(src)),
        SALTS_OK);
    check_equal(
        flow_provider_generation_prepare(
            flow, NULL, NULL, NULL,
            0u, &generation, &error),
        SALTS_OK);
    check_not_null(generation);
    check_equal(flow_provider_generation_count(generation), (size_t)0u);
    check_equal(
        flow_provider_generation_component_generation_id(generation),
        UINT64_C(0));
    check_equal(
        flow_provider_generation_materialize(
            generation, flow, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_poll(generation, 5u, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_quiesce(generation, 5u, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_drain(generation, 5u, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_shutdown(generation, &error),
        SALTS_OK);

    turbo_flow_destroy(flow);
    check_equal(
        flow_provider_generation_owner_destroy(
            generation, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_release(
            &generation, &error),
        SALTS_OK);
    check_null(generation);
  }

  it("pins one Component generation across three provider roots") {
    static const char *src =
        "source ingress adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "}\n"
        "stage response adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n"
        "stage first adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n"
        "stage second adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n"
        "stage main {\n"
        "  ingress -> response\n"
        "}\n";
    cmeta_plugin_registry registry = {0};
    cmeta_plugin_ref provider_ref = {0};
    cmeta_plugin_ref resource_ref = {0};
    component_generation_fixture_t component = {0};
    generation_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    flow_provider_generation_t *generation = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    bool quiescent = true;

    check_not_null(flow);
    check_equal(
        turbo_flow_parse_string(flow, src, strlen(src)),
        SALTS_OK);

    start_registry(&registry, &provider_ref, &resource_ref);
    check_equal(component_generation_open(
                    &component, &registry, provider_ref, UINT64_C(41)),
                SALTS_OK);

    fixture.registry = &registry;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_equal(active_leases(&registry, provider_ref), (size_t)1u);

    check_equal(
        flow_provider_generation_prepare(
            flow, &component.runtime,
            &provider_resolver, &resource_resolver,
            3u, &generation, &error),
        SALTS_OK);
    check_not_null(generation);
    check_equal(flow_provider_generation_count(generation), (size_t)3u);
    check_equal(
        flow_provider_generation_component_generation_id(generation),
        UINT64_C(41));
    check_equal(fixture.provider_calls, 3u);
    check_equal(fixture.resource_calls, 3u);

    /* Three provider roots share one Component/module lease. */
    check_equal(active_leases(&registry, provider_ref), (size_t)1u);
    check_equal(active_leases(&registry, resource_ref), (size_t)3u);

    check_equal(
        flow_provider_generation_materialize(
            generation, flow, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_poll(generation, 9u, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_quiesce(generation, 10u, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_drain(generation, 10u, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_shutdown(generation, &error),
        SALTS_OK);

    check_equal(
        cmeta_plugin_registry_request_stop(
            &registry, provider_ref),
        CMETA_PLUGIN_OK);
    check_equal(
        cmeta_plugin_registry_request_stop(
            &registry, resource_ref),
        CMETA_PLUGIN_OK);

    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &registry, resource_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_false(quiescent);

    turbo_flow_destroy(flow);
    flow = NULL;

    check_equal(
        flow_provider_generation_owner_destroy(
            generation, &error),
        SALTS_OK);
    check_equal(
        flow_provider_generation_release(
            &generation, &error),
        SALTS_OK);
    check_null(generation);

    /* TurboFlow released its one scope and all resource bindings, but the
       published Component generation still owns the provider module lease. */
    check_equal(active_leases(&registry, provider_ref), (size_t)1u);
    check_equal(active_leases(&registry, resource_ref), (size_t)0u);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &registry, resource_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_true(quiescent);

    check_equal(component_generation_close(&component), SALTS_OK);
    check_equal(active_leases(&registry, provider_ref), (size_t)0u);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_true(quiescent);

    check_equal(
        cmeta_plugin_registry_unload(&registry, provider_ref),
        CMETA_PLUGIN_OK);
    check_equal(
        cmeta_plugin_registry_unload(&registry, resource_ref),
        CMETA_PLUGIN_OK);
    check_equal(
        cmeta_plugin_registry_destroy(&registry),
        CMETA_PLUGIN_OK);
  }

  it("counts a source-only provider as its own materialization root") {
    static const char *src =
        "source ingress adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n";
    cmeta_plugin_registry registry = {0};
    cmeta_plugin_ref provider_ref = {0};
    cmeta_plugin_ref resource_ref = {0};
    component_generation_fixture_t component = {0};
    generation_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    flow_provider_generation_t *generation = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(
        turbo_flow_parse_string(flow, src, strlen(src)),
        SALTS_OK);
    start_registry(&registry, &provider_ref, &resource_ref);
    check_equal(component_generation_open(
                    &component, &registry, provider_ref, UINT64_C(42)),
                SALTS_OK);

    fixture.registry = &registry;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_equal(
        flow_provider_generation_prepare(
            flow, &component.runtime,
            &provider_resolver, &resource_resolver,
            1u, &generation, &error),
        SALTS_OK);
    check_equal(flow_provider_generation_count(generation), (size_t)1u);
    check_equal(
        flow_provider_generation_component_generation_id(generation),
        UINT64_C(42));
    check_equal(fixture.provider_calls, 1u);
    check_equal(fixture.resource_calls, 1u);
    check_equal(active_leases(&registry, provider_ref), (size_t)1u);

    check_equal(
        flow_provider_generation_release(
            &generation, &error),
        SALTS_OK);
    turbo_flow_destroy(flow);

    check_equal(component_generation_close(&component), SALTS_OK);
    stop_unload(&registry, provider_ref);
    stop_unload(&registry, resource_ref);
    check_equal(
        cmeta_plugin_registry_destroy(&registry),
        CMETA_PLUGIN_OK);
  }

  it("rejects owner capacity before acquiring a Component scope or resource lease") {
    static const char *src =
        "stage first adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n"
        "stage second adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n";
    cmeta_plugin_registry registry = {0};
    cmeta_plugin_ref provider_ref = {0};
    cmeta_plugin_ref resource_ref = {0};
    component_generation_fixture_t component = {0};
    generation_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    flow_provider_generation_t *generation = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(
        turbo_flow_parse_string(flow, src, strlen(src)),
        SALTS_OK);
    start_registry(&registry, &provider_ref, &resource_ref);
    check_equal(component_generation_open(
                    &component, &registry, provider_ref, UINT64_C(43)),
                SALTS_OK);

    fixture.registry = &registry;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_equal(
        flow_provider_generation_prepare(
            flow, &component.runtime,
            &provider_resolver, &resource_resolver,
            1u, &generation, &error),
        SALTS_ENOSPC);
    check_null(generation);
    check_equal(fixture.provider_calls, 0u);
    check_equal(fixture.resource_calls, 0u);
    check_equal(active_leases(&registry, provider_ref), (size_t)1u);
    check_equal(active_leases(&registry, resource_ref), (size_t)0u);

    turbo_flow_destroy(flow);
    check_equal(component_generation_close(&component), SALTS_OK);
    stop_unload(&registry, provider_ref);
    stop_unload(&registry, resource_ref);
    check_equal(
        cmeta_plugin_registry_destroy(&registry),
        CMETA_PLUGIN_OK);
  }

  it("releases the Component scope and earlier resources when typed config fails") {
    static const char *src =
        "stage first adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n"
        "stage invalid adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 0\n"
        "  concurrency 2\n"
        "}\n";
    cmeta_plugin_registry registry = {0};
    cmeta_plugin_ref provider_ref = {0};
    cmeta_plugin_ref resource_ref = {0};
    component_generation_fixture_t component = {0};
    generation_resolver_fixture_t fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    flow_provider_generation_t *generation = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(
        turbo_flow_parse_string(flow, src, strlen(src)),
        SALTS_OK);
    start_registry(&registry, &provider_ref, &resource_ref);
    check_equal(component_generation_open(
                    &component, &registry, provider_ref, UINT64_C(44)),
                SALTS_OK);

    fixture.registry = &registry;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_not_equal(
        flow_provider_generation_prepare(
            flow, &component.runtime,
            &provider_resolver, &resource_resolver,
            2u, &generation, &error),
        SALTS_OK);
    check_null(generation);
    check_equal(fixture.provider_calls, 2u);
    check_equal(fixture.resource_calls, 1u);
    check_equal(active_leases(&registry, provider_ref), (size_t)1u);
    check_equal(active_leases(&registry, resource_ref), (size_t)0u);

    turbo_flow_destroy(flow);
    check_equal(component_generation_close(&component), SALTS_OK);
    stop_unload(&registry, provider_ref);
    stop_unload(&registry, resource_ref);
    check_equal(
        cmeta_plugin_registry_destroy(&registry),
        CMETA_PLUGIN_OK);
  }
}
