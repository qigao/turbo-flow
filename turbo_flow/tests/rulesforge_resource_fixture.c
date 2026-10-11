#include "rulesforge_resource_fixture.h"
#include <salts/thread.h>
#include <stdlib.h>

typedef struct fixture_s {
  ruleforge_knowledge_base_t kb;
  int initialized, stopping;
} fixture_t;
static fixture_t fixture;

static int configure(void *self, const char *path) {
  fixture_t *f = self;
  if (!f || !path || !path[0] || f->kb || f->stopping) return SALTS_EINVAL;
  if (ruleforge_init() != RULES_FORGE_OK) return SALTS_EIO;
  f->initialized = 1;
  if (ruleforge_kb_create(&f->kb) != RULES_FORGE_OK) return SALTS_EIO;
  return ruleforge_kb_load_drl_file(f->kb, path, NULL, 0) == RULES_FORGE_OK ? SALTS_OK : SALTS_EIO;
}
static int snapshot(void *self, turbo_flow_rulesforge_resource_view_t *out) {
  fixture_t *f = self;
  if (!f || !f->kb || f->stopping || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  *out = (turbo_flow_rulesforge_resource_view_t){sizeof(*out), f->kb, "Applicant"};
  return SALTS_OK;
}
CMETA_IMPLEMENTS(rulesforge_fixture_control, control_impl, 0u, .configure = configure);
CMETA_IMPLEMENTS(turbo_flow_rulesforge_resource, resource_impl,
                 TURBO_FLOW_RULESFORGE_RESOURCE_IMMUTABLE, .snapshot = snapshot);

static cmeta_plugin_status CMETA_PLUGIN_CALL start(void *self) {
  ((fixture_t *)self)->stopping = 0;
  return CMETA_PLUGIN_OK;
}
static cmeta_plugin_status CMETA_PLUGIN_CALL stop(void *self) {
  ((fixture_t *)self)->stopping = 1;
  return CMETA_PLUGIN_OK;
}
/* Registry invokes quiescence only after every resource/control lease is gone. */
static bool CMETA_PLUGIN_CALL quiescent(const void *self) {
  fixture_t *f = (fixture_t *)self;
  if (!f->stopping) return false;
  if (f->kb) {
    if (ruleforge_kb_destroy(f->kb) != RULES_FORGE_OK) return false;
    f->kb = NULL;
  }
  if (f->initialized) {
    if (ruleforge_cleanup() != RULES_FORGE_OK) return false;
    f->initialized = 0;
  }
  return true;
}
static void CMETA_PLUGIN_CALL destroy(void *self) {
  fixture_t *f = self;
  /* The fixture must never unload engine state before registry quiescence. */
  if (f->kb || f->initialized) abort();
}
static rulesforge_fixture_control control;
static turbo_flow_rulesforge_resource resource;
static cmeta_plugin_export exports[2];
static cmeta_plugin_manifest manifest = {.struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
                                         .abi_version = CMETA_PLUGIN_ABI_VERSION,
                                         .plugin_id = "test.rulesforge.resource",
                                         .version = {1u, 0u, 0u},
                                         .self = &fixture,
                                         .start = start,
                                         .request_stop = stop,
                                         .is_quiescent = quiescent,
                                         .destroy = destroy};
static cmeta_once_t once = SALTS_ONCE_INIT;
static void init(void) {
  control = control_impl_as_rulesforge_fixture_control(&fixture);
  resource = resource_impl_as_turbo_flow_rulesforge_resource(&fixture);
  exports[0] =
      (cmeta_plugin_export){.struct_size = CMETA_PLUGIN_EXPORT_SIZE,
                            .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
                            .contract_version = 1u,
                            .export_id = "control",
                            .contract_id = "test.rulesforge.control",
                            .value.interface = {rulesforge_fixture_control_interface(), &control}};
  exports[1] = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .contract_version = TURBO_FLOW_RULESFORGE_RESOURCE_CONTRACT_VERSION,
      .capabilities = TURBO_FLOW_RULESFORGE_RESOURCE_IMMUTABLE,
      .export_id = "knowledge_base",
      .contract_id = TURBO_FLOW_RULESFORGE_RESOURCE_CONTRACT_ID,
      .value.interface = {turbo_flow_rulesforge_resource_interface(), &resource}};
  manifest.exports = exports;
  manifest.export_count = 2u;
}
CMETA_PLUGIN_QUERY_EXPORT const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t abi) {
  if (abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  cmeta_once(&once, init);
  return &manifest;
}
