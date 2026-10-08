#ifndef FLOW_COMPONENT_SCOPE_FIXTURE_H
#define FLOW_COMPONENT_SCOPE_FIXTURE_H
#include <salts/component_plugin.h>
#include <salts/error_codes.h>
#include <string.h>

/* Reuse the provider binding fixture's address-stable generation storage. */
#ifndef FLOW_TEST_PROVIDER_EXPORT_ID
#define FLOW_TEST_PROVIDER_EXPORT_ID "fixture.provider"
#endif
typedef struct component_fixture_s {
  salts_component_plugin_generation generation;
  salts_component_deployment deployments[1];
  salts_component_instance instances[1];
  salts_component_dependency dependencies[1];
  size_t activation_order[1];
  salts_component_plugin_module modules[1];
  salts_component_plugin_runtime runtime;
  salts_component_plugin_scope scope;
} component_fixture_t;

static int component_fixture_open(
    component_fixture_t *fixture,
    cmeta_plugin_registry *registry,
    cmeta_plugin_ref provider_ref) {
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
          UINT64_C(1),
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

  if (salts_component_plugin_scope_acquire(
          &fixture->runtime,
          &fixture->scope) != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EPROTO;

  return SALTS_OK;
}

static int component_fixture_close(component_fixture_t *fixture) {
  salts_component_plugin_generation *previous = NULL;

  if (fixture->scope.live &&
      salts_component_plugin_scope_release(
          &fixture->scope) != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EPROTO;

  if (fixture->runtime.initialized) {
    if (fixture->runtime.current != NULL) {
      if (salts_component_plugin_runtime_close(
              &fixture->runtime,
              &previous) != SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
      if (previous != &fixture->generation)
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

#endif
