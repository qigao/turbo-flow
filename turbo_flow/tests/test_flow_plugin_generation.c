#include "salts_resource_fixture.h"
#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resolved_config.h"
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
  cmeta_plugin_registry *registry;
  cmeta_plugin_ref provider;
  cmeta_plugin_ref resource;
  unsigned provider_calls;
  unsigned resource_calls;
} generation_resolver_fixture_t;

typedef struct generation_test_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_t *flow;
  cmeta_plugin_registry registry;
  cmeta_plugin_ref provider_ref;
  cmeta_plugin_ref resource_ref;
  generation_resolver_fixture_t resolver;
  turbo_flow_provider_resolver_v1_t provider_resolver;
  turbo_flow_resource_resolver_v1_t resource_resolver;
} generation_test_t;

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

static int test_open(
    generation_test_t *test, const char *graph,
    turbo_flow_plugin_error_t *plugin_error,
    turbo_flow_config_error_t *config_error) {
  static const char yaml[] =
      "version: 1\n"
      "adapters: {}\n";
  turbo_flow_plugin_host_config_t host_config =
      TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  cmeta_plugin_registry_config registry_config = {2u};
  int rc;

  memset(test, 0, sizeof(*test));

  rc = turbo_flow_plugin_host_create(
      &host_config, &test->host, plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(
      test->host, &test->snapshot, plugin_error);
  if (rc != SALTS_OK) return rc;

  rc = turbo_flow_config_resolve_yaml(
      yaml, sizeof(yaml) - 1u, &test->resolved, config_error);
  if (rc != SALTS_OK) return rc;

  test->flow = turbo_flow_create();
  if (!test->flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(test->flow, graph, strlen(graph));
  if (rc != SALTS_OK) return rc;

  if (cmeta_plugin_registry_init(
          &test->registry, &registry_config) != CMETA_PLUGIN_OK)
    return SALTS_EIO;
  if (cmeta_plugin_registry_load(
          &test->registry, FLOW_SALTS_PROVIDER_FIXTURE,
          &test->provider_ref) != CMETA_PLUGIN_OK)
    return SALTS_EIO;
  if (cmeta_plugin_registry_load(
          &test->registry, FLOW_SALTS_RESOURCE_FIXTURE,
          &test->resource_ref) != CMETA_PLUGIN_OK)
    return SALTS_EIO;
  if (cmeta_plugin_registry_start(
          &test->registry, test->provider_ref) != CMETA_PLUGIN_OK)
    return SALTS_EIO;
  if (cmeta_plugin_registry_start(
          &test->registry, test->resource_ref) != CMETA_PLUGIN_OK)
    return SALTS_EIO;

  test->resolver.registry = &test->registry;
  test->resolver.provider = test->provider_ref;
  test->resolver.resource = test->resource_ref;
  test->provider_resolver =
      (turbo_flow_provider_resolver_v1_t)
          TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  test->provider_resolver.ctx = &test->resolver;
  test->provider_resolver.resolve = resolve_provider;
  test->resource_resolver =
      (turbo_flow_resource_resolver_v1_t)
          TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  test->resource_resolver.ctx = &test->resolver;
  test->resource_resolver.resolve = resolve_resource;
  return SALTS_OK;
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

static void test_close(
    generation_test_t *test,
    turbo_flow_plugin_error_t *plugin_error) {
  if (test->flow) turbo_flow_destroy(test->flow);
  if (test->resolved)
    turbo_flow_resolved_config_destroy(test->resolved);
  if (test->snapshot)
    turbo_flow_plugin_catalog_snapshot_destroy(test->snapshot);
  if (test->host)
    check_equal(
        turbo_flow_plugin_host_destroy(
            test->host, 10u, plugin_error),
        SALTS_OK);
  if (cmeta_plugin_ref_valid(test->provider_ref))
    stop_unload(&test->registry, test->provider_ref);
  if (cmeta_plugin_ref_valid(test->resource_ref))
    stop_unload(&test->registry, test->resource_ref);
  check_equal(
      cmeta_plugin_registry_destroy(&test->registry),
      CMETA_PLUGIN_OK);
  memset(test, 0, sizeof(*test));
}

spec("canonical provider-backed Graph generation") {
  it("creates, compiles, polls and retires through canonical provider instances") {
    static const char graph[] =
        "source input\n"
        "stage provider_stage adapter fixture.provider {\n"
        "  resource " FLOW_TEST_RESOURCE_IDENTITY "\n"
        "  batch 7\n"
        "  concurrency 2\n"
        "}\n"
        "stage main {\n"
        "  input -> provider_stage\n"
        "}\n";
    generation_test_t test;
    turbo_flow_plugin_generation_config_t config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error =
        TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_t *cleanup = NULL;
    turbo_flow_msg_t message;
    bool quiescent = true;

    check_equal(
        test_open(&test, graph, &plugin_error, &error),
        SALTS_OK);

    config.owner_capacity = 1u;
    config.provider_resolver = &test.provider_resolver;
    config.resource_resolver = &test.resource_resolver;

    check_equal(
        turbo_flow_plugin_generation_create(
            test.snapshot, test.resolved, &test.flow, &config,
            NULL, &generation, &cleanup, &error),
        SALTS_OK);
    check_not_null(generation);
    check_null(cleanup);
    check_null(test.flow);
    check_equal(
        turbo_flow_plugin_generation_owner_count(generation),
        (size_t)1u);
    check_equal(test.resolver.provider_calls, 1u);
    check_equal(test.resolver.resource_calls, 1u);

    check_equal(
        turbo_flow_start(
            turbo_flow_plugin_generation_flow(generation)),
        SALTS_OK);
    check_equal(
        turbo_flow_plugin_generation_poll(
            generation, 7u, &error),
        SALTS_OK);

    turbo_flow_msg_init(&message);
    check_equal(
        turbo_flow_publish(
            turbo_flow_plugin_generation_flow(generation),
            "input", &message),
        SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    check_equal(
        cmeta_plugin_registry_request_stop(
            &test.registry, test.provider_ref),
        CMETA_PLUGIN_OK);
    check_equal(
        cmeta_plugin_registry_request_stop(
            &test.registry, test.resource_ref),
        CMETA_PLUGIN_OK);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &test.registry, test.provider_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &test.registry, test.resource_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_false(quiescent);

    check_equal(
        turbo_flow_plugin_generation_destroy(
            generation, 10u, &error),
        SALTS_OK);
    generation = NULL;

    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &test.registry, test.provider_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        cmeta_plugin_registry_poll_quiescent(
            &test.registry, test.resource_ref, &quiescent),
        CMETA_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        cmeta_plugin_registry_unload(
            &test.registry, test.provider_ref),
        CMETA_PLUGIN_OK);
    check_equal(
        cmeta_plugin_registry_unload(
            &test.registry, test.resource_ref),
        CMETA_PLUGIN_OK);
    test.provider_ref = (cmeta_plugin_ref){0};
    test.resource_ref = (cmeta_plugin_ref){0};

    test_close(&test, &plugin_error);
  }

  it("rejects provider owner capacity before resolver side effects") {
    static const char graph[] =
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
    generation_test_t test;
    turbo_flow_plugin_generation_config_t config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error =
        TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_t *cleanup = NULL;
    turbo_flow_t *original_flow;

    check_equal(
        test_open(&test, graph, &plugin_error, &error),
        SALTS_OK);
    original_flow = test.flow;

    config.owner_capacity = 1u;
    config.provider_resolver = &test.provider_resolver;
    config.resource_resolver = &test.resource_resolver;

    check_equal(
        turbo_flow_plugin_generation_create(
            test.snapshot, test.resolved, &test.flow, &config,
            NULL, &generation, &cleanup, &error),
        SALTS_ENOSPC);
    check_null(generation);
    check_null(cleanup);
    check_true(test.flow == original_flow);
    check_equal(test.resolver.provider_calls, 0u);
    check_equal(test.resolver.resource_calls, 0u);

    test_close(&test, &plugin_error);
  }
}
