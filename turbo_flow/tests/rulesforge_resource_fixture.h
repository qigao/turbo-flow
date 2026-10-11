#ifndef RULESFORGE_RESOURCE_FIXTURE_H
#define RULESFORGE_RESOURCE_FIXTURE_H
#include "turbo_flow_resource.h"
#include "turbo_flow_rulesforge_resource.h"

#define RULESFORGE_FIXTURE_CONTROL_METHODS(X, I) X(I, R1, int, configure, const char *, rfl_path)
CMETA_INTERFACE(rulesforge_fixture_control, RULESFORGE_FIXTURE_CONTROL_METHODS);

typedef struct rulesforge_resource_fixture_s {
  cmeta_plugin_registry registry;
  cmeta_plugin_ref plugin;
  int live;
  turbo_flow_resource_resolver_v1_t resolver;
} rulesforge_resource_fixture_t;

int rulesforge_resource_fixture_open(rulesforge_resource_fixture_t *fixture, const char *path);
int rulesforge_resource_fixture_close(rulesforge_resource_fixture_t *fixture);
#endif
