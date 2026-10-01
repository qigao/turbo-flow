#include "turbo_flow_provider.h"
#include "salts_resource_fixture.h"
#include "provider_config_native.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <string.h>

typedef struct fixture_provider_state_s {
  bool started;
  bool stopping;
} fixture_provider_state_t;

static fixture_provider_state_t fixture_state;

static int fixture_describe(void *self, turbo_flow_provider_contract_v1_t *out) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  *out = (turbo_flow_provider_contract_v1_t)TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory = ProviderConfig_codec_create;
  out->config.message_artifact = BatchConfig_native_artifact();
  out->resource.contract_id = FLOW_TEST_RESOURCE_CONTRACT_ID;
  out->resource.contract_version = FLOW_TEST_RESOURCE_CONTRACT_VERSION;
  out->resource.required_capabilities = FLOW_TEST_RESOURCE_CAP_READ;
  out->resource.expected_interface = flow_test_resource_interface();
  return SALTS_OK;
}

static int fixture_preflight(void *self,
                             const turbo_flow_provider_instance_v1_t *instance,
                             turbo_flow_config_error_t *error) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  const turbo_flow_provider_resource_view_v1_t *resource;
  flow_test_resource *resource_value;

  if (!state || !state->started || state->stopping || !instance ||
      instance->size != sizeof(*instance) || !instance->instance_name ||
      !instance->instance_name[0])
    return SALTS_EINVAL;

  if (instance->config.size != sizeof(instance->config) ||
      !instance->config.type_name ||
      strcmp(instance->config.type_name, "BatchConfig") != 0 ||
      !instance->config.data || !instance->config.value ||
      instance->config.value_bytes != sizeof(BatchConfig_t))
    return SALTS_EPROTO;
  {
    const BatchConfig_t *config =
        (const BatchConfig_t *)instance->config.value;
    if (config->batch != 7u || !config->durable)
      return SALTS_EPROTO;
  }

  resource = instance->resource;
  if (!resource || resource->size != sizeof(*resource) ||
      !resource->identity ||
      strcmp(resource->identity, FLOW_TEST_RESOURCE_IDENTITY) != 0 ||
      !resource->export_id ||
      strcmp(resource->export_id, "fixture.resource") != 0 ||
      !resource->interface_desc ||
      !cmeta_interface_desc_equal(
          resource->interface_desc, flow_test_resource_interface()) ||
      !resource->interface_value)
    return SALTS_EPROTO;

  resource_value = (flow_test_resource *)resource->interface_value;
  if (!flow_test_resource_valid(resource_value) ||
      flow_test_resource_ping(resource_value) != 7)
    return SALTS_EPROTO;

  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

static int fixture_materialize(void *self, turbo_flow_t *flow,
                               const turbo_flow_provider_instance_v1_t *instance,
                               turbo_flow_runtime_owner *owner_out,
                               turbo_flow_config_error_t *error) {
  (void)self;
  (void)flow;
  (void)instance;
  (void)owner_out;
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_ENOTSUP;
}

CMETA_IMPLEMENTS(turbo_flow_provider_factory, fixture_provider_factory, 0u,
                 .contract = fixture_describe,
                 .preflight = fixture_preflight,
                 .materialize = fixture_materialize);

static turbo_flow_provider_factory fixture_factory;
static salts_plugin_export fixture_export;
static salts_plugin_manifest fixture_manifest = {
    .struct_size = SALTS_PLUGIN_MANIFEST_SIZE,
    .abi_version = SALTS_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.provider",
    .version = {1u, 0u, 0u},
    .self = &fixture_state,
};
static salts_once_t fixture_once = SALTS_ONCE_INIT;

static void fixture_init(void) {
  fixture_factory =
      fixture_provider_factory_as_turbo_flow_provider_factory(&fixture_state);
  fixture_export = (salts_plugin_export){
      .struct_size = SALTS_PLUGIN_EXPORT_SIZE,
      .kind = SALTS_PLUGIN_EXPORT_INTERFACE,
      .contract_version = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
      .capabilities = 0u,
      .export_id = "fixture.provider",
      .contract_id = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_provider_factory_interface(),
          .value = &fixture_factory,
      },
  };
  fixture_manifest.exports = &fixture_export;
  fixture_manifest.export_count = 1u;
}

static salts_plugin_status SALTS_PLUGIN_CALL fixture_start(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state) return SALTS_PLUGIN_INVALID_ARGUMENT;
  state->stopping = false;
  state->started = true;
  return SALTS_PLUGIN_OK;
}

static salts_plugin_status SALTS_PLUGIN_CALL fixture_request_stop(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state) return SALTS_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return SALTS_PLUGIN_OK;
}

static bool SALTS_PLUGIN_CALL fixture_is_quiescent(const void *self) {
  const fixture_provider_state_t *state =
      (const fixture_provider_state_t *)self;
  return state && state->stopping;
}

static void SALTS_PLUGIN_CALL fixture_destroy(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state) return;
  state->started = false;
  state->stopping = true;
}

SALTS_PLUGIN_QUERY_EXPORT
const salts_plugin_manifest *SALTS_PLUGIN_CALL
salts_plugin_query(uint32_t host_abi) {
  if (host_abi != SALTS_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&fixture_once, fixture_init);
  fixture_manifest.start = fixture_start;
  fixture_manifest.request_stop = fixture_request_stop;
  fixture_manifest.is_quiescent = fixture_is_quiescent;
  fixture_manifest.destroy = fixture_destroy;
  return &fixture_manifest;
}
