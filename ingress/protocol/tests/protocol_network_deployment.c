#include "protocol_network_deployment.h"
#include "protocol_network_e2e_fixture.h"
#include <salts/plugin.h>
#include <stdlib.h>
#include <string.h>

/* Deployment adapts the real factory exports, without adding compatibility
 * exports to any provider. These leases cover all Component projections. */
cmeta_component(NetworkIntakeListener, cmeta_provides(turbo_flow_provider_factory));
cmeta_component(NetworkIntakePacket, cmeta_provides(turbo_flow_provider_factory));
cmeta_component(NetworkIntakeSink, cmeta_provides(turbo_flow_provider_factory));
cmeta_component(NetworkIntakeStore, cmeta_provides(turbo_flow_provider_factory));
enum { FACTORY_COUNT = 4, MODULE_COUNT = 3 };
typedef struct factory_s {
  cmeta_plugin_lease lease;
  turbo_flow_provider_factory factory;
  cmeta_object_interface_provider interfaces;
  salts_component_provider_binding binding;
  const char *identity;
  int anchor;
} factory_t;
struct protocol_network_deployment_s {
  cmeta_plugin_registry registry;
  int registry_live;
  cmeta_plugin_ref modules[MODULE_COUNT];
  cmeta_plugin_lease control_lease;
  size_t module_count;
  factory_t factories[FACTORY_COUNT];
  salts_component_plugin_generation generation;
  salts_component_plugin_runtime runtime;
  salts_component_deployment deployments[FACTORY_COUNT];
  salts_component_instance instances[FACTORY_COUNT];
  salts_component_dependency dependencies[1];
  size_t activation_order[FACTORY_COUNT];
  salts_component_plugin_module component_modules[1];
  int database;
  unsigned references, next_slot;
};
typedef struct protocol_network_deployment_s deployment_t;

static cmeta_status project(void *context, const cmeta_object_ref *object,
                            const cmeta_interface_desc *expected, cmeta_interface_projection *out) {
  factory_t *factory = context;
  if (!factory || !object || !out) return CMETA_INVALID_ARGUMENT;
  if (!cmeta_interface_desc_equal(expected, turbo_flow_provider_factory_interface()))
    return CMETA_TRAIT_MISSING;
  *out = (cmeta_interface_projection){sizeof(*out), turbo_flow_provider_factory_interface(),
                                      factory->factory.self, factory->factory.vtable};
  return CMETA_OK;
}
static cmeta_status SALTS_COMPONENT_CALL create(void *context, const cmeta_data_desc *config_data,
                                                const void *config_value,
                                                const salts_component_dependency *dependencies,
                                                size_t count, cmeta_object_ref *out) {
  factory_t *factory = context;
  (void)dependencies;
  if (!factory || config_data || config_value || count) return CMETA_INVALID_ARGUMENT;
  return cmeta_object_borrow(out, &factory->anchor, &cmeta_data_int, NULL);
}
static int resolve_provider(void *ctx, const char *identity,
                            turbo_flow_provider_candidate_v2_t *out,
                            turbo_flow_config_error_t *error) {
  deployment_t *state = ctx;
  (void)error;
  if (!state || !identity || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  for (size_t i = 0; i < FACTORY_COUNT; ++i) {
    if (strcmp(identity, state->factories[i].identity) != 0) continue;
    out->component_identity = state->factories[i].binding.component->stable_id;
    return SALTS_OK;
  }
  return SALTS_ENOENT;
}
static int resolve_resource(void *ctx, const char *name,
                            const turbo_flow_provider_resource_requirement_v1_t *requirement,
                            turbo_flow_resource_candidate_v1_t *out,
                            turbo_flow_config_error_t *error) {
  protocol_network_e2e_fixture_t *fixture = ctx;
  deployment_t *state = fixture ? fixture->deployment : NULL;
  static const char *const names[2][4] = {
      {"fixture.listener.0", "fixture.packet.0", "fixture.output.0", "fixture.memory.0"},
      {"fixture.listener.1", "fixture.packet.1", "fixture.output.1", "fixture.memory.1"}};
  const unsigned slot = fixture ? fixture->deployment_slot : 2u;
  if (slot >= 2u) return SALTS_EINVAL;
  (void)requirement;
  (void)error;
  if (!state || !name || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  if (strcmp(name, "wire_net") == 0) out->export_id = names[slot][fixture->packet_source ? 1 : 0];
  else if (strcmp(name, "listener_net") == 0) out->export_id = names[slot][0];
  else if (strcmp(name, "packet_net") == 0) out->export_id = names[slot][1];
  else if (strcmp(name, "output_net") == 0) out->export_id = names[slot][2];
  else if (strcmp(name, "intake_store") == 0)
    out->export_id = state->database ? (slot == 0u ? "fixture.database.0" : "fixture.database.1")
                                     : names[slot][3];
  else return SALTS_ENOENT;
  out->identity = out->export_id;
  out->registry = &state->registry;
  out->plugin = state->modules[2];
  return SALTS_OK;
}
static int configure_resources(deployment_t *state, unsigned slot, unsigned port,
                               const char *filename) {
  const cmeta_plugin_manifest *manifest = NULL;
  const cmeta_plugin_export *entry = NULL;
  int rc = SALTS_EPROTO;
  if (cmeta_plugin_lease_valid(state->control_lease)) return SALTS_EBUSY;
  if (cmeta_plugin_registry_acquire(&state->registry, state->modules[2], &state->control_lease,
                                    &manifest) != CMETA_PLUGIN_OK)
    return rc;
  if (cmeta_plugin_manifest_find_export(manifest, "fixture.configure", &entry) == CMETA_PLUGIN_OK &&
      cmeta_plugin_export_require_interface(entry, PROTOCOL_NETWORK_DEPLOYMENT_CONTROL_ID, 1u, 0u,
                                            protocol_network_deployment_control_interface()) ==
          CMETA_PLUGIN_OK) {
    protocol_network_deployment_control *control = entry->value.interface.value;
    rc = protocol_network_deployment_control_configure(control, slot, port, filename);
  }
  if (cmeta_plugin_registry_release(&state->registry, &state->control_lease) != CMETA_PLUGIN_OK)
    return SALTS_EPROTO;
  return rc;
}
int protocol_network_deployment_open(protocol_network_e2e_fixture_t *fixture,
                                     const char *cnet_module, const char *durable_module,
                                     int packet_source) {
  deployment_t *state;
  const cmeta_component_desc *metadata[FACTORY_COUNT] = {
      cmeta_component_meta(NetworkIntakeListener), cmeta_component_meta(NetworkIntakePacket),
      cmeta_component_meta(NetworkIntakeSink), cmeta_component_meta(NetworkIntakeStore)};
  const char *paths[MODULE_COUNT] = {cnet_module, durable_module, FLOW_NETWORK_DEPLOYMENT_MODULE};
  cmeta_plugin_registry_config config = {MODULE_COUNT};
  salts_component_plugin_generation *previous = NULL;
  salts_component_deployment requested[FACTORY_COUNT];
  int rc;
  if (!fixture) return SALTS_EINVAL;
  fixture->packet_source = packet_source;
  if (fixture->deployment) {
    state = fixture->deployment;
    if (state->next_slot >= 2u ||
        state->database != (fixture->storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB)) {
      fixture->deployment = NULL;
      return SALTS_EINVAL;
    }
    ++state->references;
    fixture->deployment_slot = state->next_slot++;
    rc = configure_resources(state, fixture->deployment_slot, fixture->receiver_port,
                             fixture->turbodb_path);
    if (rc != SALTS_OK) return rc;
    goto bind_resolvers;
  }
  state = calloc(1u, sizeof(*state));
  if (!state) return SALTS_ENOMEM;
  fixture->deployment = state;
  state->references = 1u;
  state->next_slot = 1u;
  state->database = fixture->storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB;
  state->factories[0].identity = "cnet.listener_source";
  state->factories[1].identity = "cnet.packet_source";
  state->factories[2].identity = "cnet.datagram_sink";
  state->factories[3].identity = state->database ? "flow.durable.turbodb" : "flow.durable.memory";
  if (cmeta_plugin_registry_init(&state->registry, &config) != CMETA_PLUGIN_OK) return SALTS_EPROTO;
  state->registry_live = 1;
  for (size_t i = 0; i < MODULE_COUNT; ++i) {
    if (cmeta_plugin_registry_load(&state->registry, paths[i], &state->modules[i]) !=
        CMETA_PLUGIN_OK)
      return SALTS_EPROTO;
    ++state->module_count;
    if (cmeta_plugin_registry_start(&state->registry, state->modules[i]) != CMETA_PLUGIN_OK)
      return SALTS_EPROTO;
  }
  rc = configure_resources(state, 0u, fixture->receiver_port, fixture->turbodb_path);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < FACTORY_COUNT; ++i) {
    factory_t *factory = &state->factories[i];
    const cmeta_plugin_manifest *manifest = NULL;
    const cmeta_plugin_export *entry = NULL;
    if (cmeta_plugin_registry_acquire(&state->registry, state->modules[i == 3 ? 1 : 0],
                                      &factory->lease, &manifest) != CMETA_PLUGIN_OK ||
        cmeta_plugin_manifest_find_export(manifest, factory->identity, &entry) != CMETA_PLUGIN_OK ||
        cmeta_plugin_export_require_interface(entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
                                              TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION, 0u,
                                              turbo_flow_provider_factory_interface()) !=
            CMETA_PLUGIN_OK)
      return SALTS_EPROTO;
    factory->factory = *(const turbo_flow_provider_factory *)entry->value.interface.value;
    factory->interfaces =
        (cmeta_object_interface_provider){sizeof(factory->interfaces), factory, project};
    factory->binding =
        (salts_component_provider_binding){sizeof(factory->binding),
                                           SALTS_COMPONENT_PROVIDER_BINDING_ABI_VERSION,
                                           metadata[i],
                                           factory,
                                           &factory->interfaces,
                                           create,
                                           NULL,
                                           NULL};
    requested[i] = (salts_component_deployment){&factory->binding, NULL, NULL};
  }
  {
    const salts_component_plugin_generation_storage storage = {
        state->deployments,       FACTORY_COUNT,
        state->instances,         FACTORY_COUNT,
        state->dependencies,      1u,
        state->activation_order,  FACTORY_COUNT,
        state->component_modules, 1u};
    if (salts_component_plugin_generation_build(&state->generation, 1u, &state->registry, &storage,
                                                requested, FACTORY_COUNT, NULL, 0u, NULL,
                                                0u) != SALTS_COMPONENT_PLUGIN_OK ||
        salts_component_plugin_runtime_init(&state->runtime) != SALTS_COMPONENT_PLUGIN_OK ||
        salts_component_plugin_runtime_publish(&state->runtime, &state->generation, &previous) !=
            SALTS_COMPONENT_PLUGIN_OK ||
        previous)
      return SALTS_EPROTO;
  }
bind_resolvers:
  fixture->component_runtime = &state->runtime;
  fixture->provider_resolver = (turbo_flow_provider_resolver_v2_t){
      sizeof(fixture->provider_resolver), state, resolve_provider};
  fixture->resource_resolver = (turbo_flow_resource_resolver_v1_t){
      sizeof(fixture->resource_resolver), fixture, resolve_resource};
  return SALTS_OK;
}
int protocol_network_deployment_lifetime(const protocol_network_e2e_fixture_t *fixture,
                                         protocol_network_deployment_lifetime_t *out) {
  deployment_t *state = fixture ? fixture->deployment : NULL;
  cmeta_plugin_lifecycle_info info[MODULE_COUNT];
  if (!state || !out || state->module_count != MODULE_COUNT) return SALTS_EINVAL;
  for (size_t i = 0; i < MODULE_COUNT; ++i) {
    if (cmeta_plugin_registry_get_lifecycle(&state->registry, state->modules[i], &info[i]) !=
        CMETA_PLUGIN_OK)
      return SALTS_EPROTO;
  }
  /* This fixture drives lifecycle and polling exclusively on its test thread. */
  *out = (protocol_network_deployment_lifetime_t){info[0].active_leases, info[1].active_leases,
                                                  info[2].active_leases,
                                                  state->generation.active_scopes};
  return SALTS_OK;
}
int protocol_network_deployment_close(protocol_network_e2e_fixture_t *fixture) {
  deployment_t *state = fixture->deployment;
  salts_component_plugin_generation *previous = NULL;
  if (!state) return SALTS_OK;
  if (state->references > 1u) {
    --state->references;
    fixture->deployment = NULL;
    fixture->component_runtime = NULL;
    return SALTS_OK;
  }
  if (state->runtime.initialized) {
    if (state->runtime.current && salts_component_plugin_runtime_close(
                                      &state->runtime, &previous) != SALTS_COMPONENT_PLUGIN_OK)
      return SALTS_EBUSY;
    if (state->generation.state == SALTS_COMPONENT_PLUGIN_GENERATION_DRAINING &&
        salts_component_plugin_generation_drain(&state->runtime, &state->generation) !=
            SALTS_COMPONENT_PLUGIN_OK)
      return SALTS_EBUSY;
    if (salts_component_plugin_runtime_destroy(&state->runtime) != SALTS_COMPONENT_PLUGIN_OK)
      return SALTS_EBUSY;
  }
  if (state->generation.state == SALTS_COMPONENT_PLUGIN_GENERATION_BUILT &&
      salts_component_plugin_generation_discard(&state->generation) != SALTS_COMPONENT_PLUGIN_OK)
    return SALTS_EBUSY;
  if (cmeta_plugin_lease_valid(state->control_lease) &&
      cmeta_plugin_registry_release(&state->registry, &state->control_lease) != CMETA_PLUGIN_OK)
    return SALTS_EBUSY;
  for (size_t i = FACTORY_COUNT; i > 0; --i) {
    if (cmeta_plugin_lease_valid(state->factories[i - 1].lease) &&
        cmeta_plugin_registry_release(&state->registry, &state->factories[i - 1].lease) !=
            CMETA_PLUGIN_OK)
      return SALTS_EBUSY;
  }
  while (state->module_count) {
    cmeta_plugin_ref module = state->modules[state->module_count - 1];
    cmeta_plugin_lifecycle_info info;
    bool quiet = false;
    if (cmeta_plugin_registry_get_lifecycle(&state->registry, module, &info) != CMETA_PLUGIN_OK)
      return SALTS_EPROTO;
    if (info.state == CMETA_PLUGIN_LIFECYCLE_STARTED) {
      if (cmeta_plugin_registry_request_stop(&state->registry, module) != CMETA_PLUGIN_OK)
        return SALTS_EBUSY;
      info.state = CMETA_PLUGIN_LIFECYCLE_STOPPING;
    }
    if (info.state == CMETA_PLUGIN_LIFECYCLE_STOPPING &&
        (cmeta_plugin_registry_poll_quiescent(&state->registry, module, &quiet) !=
             CMETA_PLUGIN_OK ||
         !quiet))
      return SALTS_EBUSY;
    if (cmeta_plugin_registry_unload(&state->registry, module) != CMETA_PLUGIN_OK)
      return SALTS_EBUSY;
    --state->module_count;
  }
  if (state->registry_live && cmeta_plugin_registry_destroy(&state->registry) != CMETA_PLUGIN_OK)
    return SALTS_EBUSY;
  free(state);
  fixture->deployment = NULL;
  fixture->component_runtime = NULL;
  return SALTS_OK;
}
