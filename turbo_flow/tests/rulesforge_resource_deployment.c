#include "rulesforge_resource_fixture.h"
#include <string.h>

static int resolve(void *ctx, const char *name,
                   const turbo_flow_provider_resource_requirement_v1_t *requirement,
                   turbo_flow_resource_candidate_v1_t *out, turbo_flow_config_error_t *error) {
  rulesforge_resource_fixture_t *f = ctx;
  (void)requirement;
  (void)error;
  if (!f || !name || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  if (strcmp(name, "rules_adult")) return SALTS_ENOENT;
  out->identity = "deployment.rules.adult";
  out->registry = &f->registry;
  out->plugin = f->plugin;
  out->export_id = "knowledge_base";
  return SALTS_OK;
}

int rulesforge_resource_fixture_open(rulesforge_resource_fixture_t *f, const char *path) {
  cmeta_plugin_registry_config config = {1u};
  cmeta_plugin_lease lease = {0};
  const cmeta_plugin_manifest *manifest = NULL;
  const cmeta_plugin_export *entry = NULL;
  int rc;
  if (!f || !path) return SALTS_EINVAL;
  if (cmeta_plugin_registry_init(&f->registry, &config) != CMETA_PLUGIN_OK) return SALTS_EIO;
  f->live = 1;
  if (cmeta_plugin_registry_load(&f->registry, FLOW_RULESFORGE_RESOURCE_FIXTURE, &f->plugin) !=
      CMETA_PLUGIN_OK)
    return SALTS_EIO;
  if (cmeta_plugin_registry_start(&f->registry, f->plugin) != CMETA_PLUGIN_OK) return SALTS_EIO;
  if (cmeta_plugin_registry_acquire(&f->registry, f->plugin, &lease, &manifest) != CMETA_PLUGIN_OK)
    return SALTS_EIO;
  rc = SALTS_EPROTO;
  if (cmeta_plugin_manifest_find_export(manifest, "control", &entry) == CMETA_PLUGIN_OK &&
      cmeta_plugin_export_require_interface(entry, "test.rulesforge.control", 1u, 0u,
                                            rulesforge_fixture_control_interface()) ==
          CMETA_PLUGIN_OK)
    rc = rulesforge_fixture_control_configure(entry->value.interface.value, path);
  if (cmeta_plugin_registry_release(&f->registry, &lease) != CMETA_PLUGIN_OK) return SALTS_EIO;
  f->resolver = (turbo_flow_resource_resolver_v1_t){sizeof(f->resolver), f, resolve};
  return rc;
}

int rulesforge_resource_fixture_close(rulesforge_resource_fixture_t *f) {
  bool ready = false;
  if (!f || !f->live) return SALTS_OK;
  if (cmeta_plugin_ref_valid(f->plugin)) {
    cmeta_plugin_status status = cmeta_plugin_registry_request_stop(&f->registry, f->plugin);
    if (status != CMETA_PLUGIN_OK && status != CMETA_PLUGIN_ALREADY) return SALTS_EIO;
    if (cmeta_plugin_registry_poll_quiescent(&f->registry, f->plugin, &ready) != CMETA_PLUGIN_OK ||
        !ready)
      return SALTS_EBUSY;
    if (cmeta_plugin_registry_unload(&f->registry, f->plugin) != CMETA_PLUGIN_OK) return SALTS_EIO;
    f->plugin = (cmeta_plugin_ref){0};
  }
  if (cmeta_plugin_registry_destroy(&f->registry) != CMETA_PLUGIN_OK) return SALTS_EBUSY;
  f->live = 0;
  return SALTS_OK;
}
