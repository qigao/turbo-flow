#include "flow_provider_generation_internal.h"
#include "salts_resource_fixture.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <salts/plugin.h>

#include <stdbool.h>
#include <string.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_SALTS_RESOURCE_FIXTURE
#error "FLOW_SALTS_RESOURCE_FIXTURE is required"
#endif

typedef struct generation_resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref provider;
  salts_plugin_ref resource;
  unsigned provider_calls;
  unsigned resource_calls;
} generation_resolver_fixture_t;

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
  out->registry = fixture->registry;
  out->plugin = fixture->provider;
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
    salts_plugin_registry *registry,
    salts_plugin_ref *provider,
    salts_plugin_ref *resource) {
  salts_plugin_registry_config config = {2u};
  check_equal(
      salts_plugin_registry_init(registry, &config),
      SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_load(
          registry, FLOW_SALTS_PROVIDER_FIXTURE, provider),
      SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_load(
          registry, FLOW_SALTS_RESOURCE_FIXTURE, resource),
      SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_start(registry, *provider),
      SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_start(registry, *resource),
      SALTS_PLUGIN_OK);
}

static void stop_unload(
    salts_plugin_registry *registry, salts_plugin_ref ref) {
  bool quiescent = false;
  check_equal(
      salts_plugin_registry_request_stop(registry, ref),
      SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_poll_quiescent(
          registry, ref, &quiescent),
      SALTS_PLUGIN_OK);
  check_true(quiescent);
  check_equal(
      salts_plugin_registry_unload(registry, ref),
      SALTS_PLUGIN_OK);
}

spec("canonical provider generation aggregate") {
  it("accepts an empty provider set without a dummy resolver") {
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
            flow, NULL, NULL, 0u, &generation, &error),
        SALTS_OK);
    check_not_null(generation);
    check_equal(flow_provider_generation_count(generation), (size_t)0u);
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

  it("prepares exact roots and lets a terminal claim its paired source") {
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
    salts_plugin_registry registry = {0};
    salts_plugin_ref provider_ref = {0};
    salts_plugin_ref resource_ref = {0};
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
    fixture.registry = &registry;
    fixture.provider = provider_ref;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_equal(
        flow_provider_generation_prepare(
            flow, &provider_resolver, &resource_resolver,
            3u, &generation, &error),
        SALTS_OK);
    check_not_null(generation);
    check_equal(flow_provider_generation_count(generation), (size_t)3u);
    check_equal(fixture.provider_calls, 3u);
    check_equal(fixture.resource_calls, 3u);

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
        salts_plugin_registry_request_stop(
            &registry, provider_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_request_stop(
            &registry, resource_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, resource_ref, &quiescent),
        SALTS_PLUGIN_OK);
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

    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, resource_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, provider_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_unload(&registry, resource_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_destroy(&registry),
        SALTS_PLUGIN_OK);
  }

  it("counts a source-only provider as its own materialization root") {
    static const char *src =
        "source ingress adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n";
    salts_plugin_registry registry = {0};
    salts_plugin_ref provider_ref = {0};
    salts_plugin_ref resource_ref = {0};
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
    fixture.registry = &registry;
    fixture.provider = provider_ref;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_equal(
        flow_provider_generation_prepare(
            flow, &provider_resolver, &resource_resolver,
            1u, &generation, &error),
        SALTS_OK);
    check_equal(flow_provider_generation_count(generation), (size_t)1u);
    check_equal(fixture.provider_calls, 1u);
    check_equal(fixture.resource_calls, 1u);

    check_equal(
        flow_provider_generation_release(
            &generation, &error),
        SALTS_OK);
    turbo_flow_destroy(flow);
    stop_unload(&registry, provider_ref);
    stop_unload(&registry, resource_ref);
    check_equal(
        salts_plugin_registry_destroy(&registry),
        SALTS_PLUGIN_OK);
  }

  it("rejects owner capacity before acquiring any provider or resource lease") {
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
    salts_plugin_registry registry = {0};
    salts_plugin_ref provider_ref = {0};
    salts_plugin_ref resource_ref = {0};
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
    fixture.registry = &registry;
    fixture.provider = provider_ref;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_equal(
        flow_provider_generation_prepare(
            flow, &provider_resolver, &resource_resolver,
            1u, &generation, &error),
        SALTS_ENOSPC);
    check_null(generation);
    check_equal(fixture.provider_calls, 0u);
    check_equal(fixture.resource_calls, 0u);

    turbo_flow_destroy(flow);
    stop_unload(&registry, provider_ref);
    stop_unload(&registry, resource_ref);
    check_equal(
        salts_plugin_registry_destroy(&registry),
        SALTS_PLUGIN_OK);
  }

  it("releases earlier leases when a later typed config fails preflight") {
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
    salts_plugin_registry registry = {0};
    salts_plugin_ref provider_ref = {0};
    salts_plugin_ref resource_ref = {0};
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
    fixture.registry = &registry;
    fixture.provider = provider_ref;
    fixture.resource = resource_ref;
    setup_resolvers(&fixture, &provider_resolver, &resource_resolver);

    check_not_equal(
        flow_provider_generation_prepare(
            flow, &provider_resolver, &resource_resolver,
            2u, &generation, &error),
        SALTS_OK);
    check_null(generation);
    check_equal(fixture.provider_calls, 2u);
    check_equal(fixture.resource_calls, 1u);

    turbo_flow_destroy(flow);
    stop_unload(&registry, provider_ref);
    stop_unload(&registry, resource_ref);
    check_equal(
        salts_plugin_registry_destroy(&registry),
        SALTS_PLUGIN_OK);
  }
}
