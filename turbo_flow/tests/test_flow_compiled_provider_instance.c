#include "flow_provider_instance_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_provider.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <salts/plugin.h>

#include <string.h>

#ifndef FLOW_SALTS_PROVIDER_FIXTURE
#error "FLOW_SALTS_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_SALTS_RESOURCE_FIXTURE
#error "FLOW_SALTS_RESOURCE_FIXTURE is required"
#endif

typedef struct provider_resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  unsigned calls;
} provider_resolver_fixture_t;

typedef struct resource_resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  unsigned calls;
} resource_resolver_fixture_t;

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
      strcmp(provider_identity, "fixture.provider") != 0) {
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

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resource_resolver_fixture_t *fixture =
      (resource_resolver_fixture_t *)ctx;
  if (!fixture || !requirement || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->calls;
  if (!resource_name || strcmp(resource_name, "db-main") != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->identity = "db-main";
  out->registry = fixture->registry;
  out->plugin = fixture->plugin;
  out->export_id = "fixture.resource";
  return SALTS_OK;
}

static void stop_and_unload(
    salts_plugin_registry *registry, salts_plugin_ref ref) {
  bool quiescent = false;
  check_equal(
      salts_plugin_registry_request_stop(registry, ref),
      SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_poll_quiescent(registry, ref, &quiescent),
      SALTS_PLUGIN_OK);
  check_true(quiescent);
  check_equal(
      salts_plugin_registry_unload(registry, ref),
      SALTS_PLUGIN_OK);
}

spec("compiled provider instance") {
  it("binds provider config and resource before provider preflight") {
    static const char *src =
        "stage worker adapter fixture.provider {\n"
        "  resource db-main\n"
        "  batch 7\n"
        "  durable true\n"
        "}\n";
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config registry_config = {2u};
    salts_plugin_ref provider_ref = {0};
    salts_plugin_ref resource_ref = {0};
    provider_resolver_fixture_t provider_fixture = {0};
    resource_resolver_fixture_t resource_fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_resource_resolver_v1_t resource_resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    flow_compiled_provider_instance_t *compiled = NULL;
    const turbo_flow_provider_instance_v1_t *view = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    int stage_index;
    bool quiescent = true;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    stage_index = turbo_flow_find_stage(flow, "worker");
    check_true(stage_index >= 0);

    check_equal(salts_plugin_registry_init(&registry, &registry_config),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_RESOURCE_FIXTURE, &resource_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, resource_ref),
                SALTS_PLUGIN_OK);

    provider_fixture.registry = &registry;
    provider_fixture.plugin = provider_ref;
    provider_resolver.ctx = &provider_fixture;
    provider_resolver.resolve = resolve_provider;
    resource_fixture.registry = &registry;
    resource_fixture.plugin = resource_ref;
    resource_resolver.ctx = &resource_fixture;
    resource_resolver.resolve = resolve_resource;

    check_equal(
        flow_compiled_provider_instance_prepare(
            flow, (size_t)stage_index, &provider_resolver,
            &resource_resolver, &compiled, &error),
        SALTS_OK);
    check_not_null(compiled);
    check_equal(provider_fixture.calls, 1u);
    check_equal(resource_fixture.calls, 1u);

    check_equal(
        flow_compiled_provider_instance_view(compiled, &view),
        SALTS_OK);
    check_not_null(view);
    check_equal(view->instance_name, "worker");
    check_equal(view->config.type_name, "BatchConfig");
    check_not_null(view->config.data);
    check_not_null(view->config.value);
    check_true(view->config.value_bytes > 0u);
    check_not_null(view->resource);
    check_equal(view->resource->identity, "db-main");
    check_equal(view->resource->export_id, "fixture.resource");

    check_equal(
        salts_plugin_registry_request_stop(&registry, provider_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, provider_ref),
        SALTS_PLUGIN_BUSY);

    check_equal(
        salts_plugin_registry_request_stop(&registry, resource_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, resource_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, resource_ref),
        SALTS_PLUGIN_BUSY);

    check_equal(
        flow_compiled_provider_instance_release(&compiled),
        SALTS_OK);
    check_null(compiled);

    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, provider_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, provider_ref),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &registry, resource_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, resource_ref),
        SALTS_PLUGIN_OK);

    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects invalid typed config before resource resolution") {
    static const char *src =
        "stage worker adapter fixture.provider {\n"
        "  resource db-main\n"
        "  batch 0\n"
        "  durable true\n"
        "}\n";
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config registry_config = {1u};
    salts_plugin_ref provider_ref = {0};
    provider_resolver_fixture_t provider_fixture = {0};
    resource_resolver_fixture_t resource_fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_resource_resolver_v1_t resource_resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    flow_compiled_provider_instance_t *compiled = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    int stage_index;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    stage_index = turbo_flow_find_stage(flow, "worker");
    check_true(stage_index >= 0);

    check_equal(salts_plugin_registry_init(&registry, &registry_config),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, provider_ref),
                SALTS_PLUGIN_OK);

    provider_fixture.registry = &registry;
    provider_fixture.plugin = provider_ref;
    provider_resolver.ctx = &provider_fixture;
    provider_resolver.resolve = resolve_provider;
    resource_resolver.ctx = &resource_fixture;
    resource_resolver.resolve = resolve_resource;

    check_equal(
        flow_compiled_provider_instance_prepare(
            flow, (size_t)stage_index, &provider_resolver,
            &resource_resolver, &compiled, &error),
        SALTS_EPROTO);
    check_null(compiled);
    check_equal(provider_fixture.calls, 1u);
    check_equal(resource_fixture.calls, 0u);
    check_contains(error.path, "batch");

    stop_and_unload(&registry, provider_ref);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects a missing required resource before provider preflight") {
    static const char *src =
        "stage worker adapter fixture.provider {\n"
        "  batch 7\n"
        "  durable true\n"
        "}\n";
    salts_plugin_registry registry = {0};
    salts_plugin_registry_config registry_config = {1u};
    salts_plugin_ref provider_ref = {0};
    provider_resolver_fixture_t provider_fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    flow_compiled_provider_instance_t *compiled = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    int stage_index;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    stage_index = turbo_flow_find_stage(flow, "worker");
    check_true(stage_index >= 0);

    check_equal(salts_plugin_registry_init(&registry, &registry_config),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_load(
                    &registry, FLOW_SALTS_PROVIDER_FIXTURE, &provider_ref),
                SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_start(&registry, provider_ref),
                SALTS_PLUGIN_OK);

    provider_fixture.registry = &registry;
    provider_fixture.plugin = provider_ref;
    provider_resolver.ctx = &provider_fixture;
    provider_resolver.resolve = resolve_provider;

    check_equal(
        flow_compiled_provider_instance_prepare(
            flow, (size_t)stage_index, &provider_resolver,
            NULL, &compiled, &error),
        SALTS_EPROTO);
    check_null(compiled);
    check_contains(error.message, "resource");

    stop_and_unload(&registry, provider_ref);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
    turbo_flow_destroy(flow);
  }
}
