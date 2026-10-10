#include "turbo_flow_provider.h"
#include "turbo_flow_provider_adapter.h"
#include "salts_resource_fixture.h"
#include "provider_config_native.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <string.h>

typedef struct fixture_provider_state_s {
  bool started;
  bool stopping;
  unsigned owner_quiesce_calls;
  unsigned owner_drain_calls;
  unsigned owner_shutdown_calls;
  unsigned owner_poll_calls;
  unsigned owner_destroy_calls;
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
    if (config->batch != 7u || config->concurrency != 2u)
      return SALTS_EPROTO;
  }

  resource = instance->resource;
  if (!resource || resource->size != sizeof(*resource) ||
      !resource->reference_name ||
      strcmp(resource->reference_name, FLOW_TEST_RESOURCE_IDENTITY) != 0 ||
      !resource->identity ||
      strcmp(resource->identity, "deployment.db_main") != 0 ||
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

static int fixture_owner_quiesce(void *self, uint64_t timeout_ms) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || timeout_ms == 0u) return SALTS_EINVAL;
  ++state->owner_quiesce_calls;
  return SALTS_OK;
}

static int fixture_owner_drain(void *self, uint64_t timeout_ms) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || timeout_ms == 0u || state->owner_quiesce_calls == 0u)
    return SALTS_EINVAL;
  ++state->owner_drain_calls;
  return SALTS_OK;
}

static int fixture_owner_shutdown(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || state->owner_drain_calls == 0u) return SALTS_EINVAL;
  ++state->owner_shutdown_calls;
  return SALTS_OK;
}

static int fixture_owner_poll(void *self, uint32_t timeout_ms) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  (void)timeout_ms;
  if (!state) return SALTS_EINVAL;
  ++state->owner_poll_calls;
  return SALTS_OK;
}

static void fixture_owner_destroy(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (state) ++state->owner_destroy_calls;
}

static int fixture_adapter_start(
    void *self, turbo_flow_t *flow,
    const turbo_flow_stage_plan_t *stage) {
  (void)self;
  (void)flow;
  return stage ? SALTS_OK : SALTS_EINVAL;
}

static int fixture_adapter_consume(
    void *self, turbo_flow_t *flow,
    const turbo_flow_stage_plan_t *stage,
    turbo_flow_msg_t *message) {
  (void)self;
  (void)flow;
  return stage && message ? SALTS_OK : SALTS_EINVAL;
}

static int fixture_register_exact_stage_adapter(
    fixture_provider_state_t *state, turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance) {
  turbo_flow_provider_adapter_registration_v1_t registration =
      TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT;
  turbo_flow_adapter_ops_t ops = {0};
  turbo_flow_adapter_schema_t schema = {0};
  const turbo_flow_stage_plan_t *stage;
  const char *stage_names[1];
  int stage_index;

  if (!state || !flow || !instance || !instance->instance_name)
    return SALTS_EINVAL;

  stage_index = turbo_flow_find_stage(flow, instance->instance_name);
  if (stage_index < 0) return SALTS_ENOENT;
  stage = turbo_flow_stage_at(flow, (size_t)stage_index);
  if (!stage) return SALTS_EPROTO;

  /*
   * Buffer providers own a separate buffer binding contract. This generic
   * fixture only publishes exact adapter-stage registrations so the canonical
   * generation integration test can reach Graph compile.
   */
  if (stage->is_buffer) return SALTS_OK;
  if (!stage->adapter_name ||
      strcmp(stage->adapter_name, "fixture.provider") != 0)
    return SALTS_EPROTO;

  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  if (stage->is_source) {
    schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
    schema.direction = TURBO_FLOW_ADAPTER_INPUT;
    ops.start = fixture_adapter_start;
  } else {
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    ops.consume = fixture_adapter_consume;
  }

  stage_names[0] = instance->instance_name;
  registration.provider_identity = "fixture.provider";
  registration.stage_names = stage_names;
  registration.stage_count = 1u;
  registration.adapter_ops = &ops;
  registration.schema = &schema;
  registration.ctx = state;
  return turbo_flow_provider_adapter_register(flow, &registration);
}

CMETA_IMPLEMENTS(
    turbo_flow_runtime_owner, fixture_runtime_owner,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
        TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL,
    .quiesce = fixture_owner_quiesce,
    .drain = fixture_owner_drain,
    .shutdown = fixture_owner_shutdown,
    .poll = fixture_owner_poll,
    .destroy = fixture_owner_destroy);

static int fixture_materialize(void *self, turbo_flow_t *flow,
                               const turbo_flow_provider_instance_v1_t *instance,
                               turbo_flow_runtime_owner *owner_out,
                               turbo_flow_config_error_t *error) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  int rc;

  if (!state || !flow || !owner_out) return SALTS_EINVAL;
  rc = fixture_preflight(self, instance, error);
  if (rc != SALTS_OK) return rc;

  rc = fixture_register_exact_stage_adapter(state, flow, instance);
  if (rc != SALTS_OK) return rc;

  *owner_out =
      fixture_runtime_owner_as_turbo_flow_runtime_owner(state);
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(turbo_flow_provider_factory, fixture_provider_factory, 0u,
                 .contract = fixture_describe,
                 .preflight = fixture_preflight,
                 .materialize = fixture_materialize);

static turbo_flow_provider_factory fixture_factory;
static cmeta_plugin_export fixture_export;
static cmeta_plugin_manifest fixture_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.provider",
    .version = {1u, 0u, 0u},
    .self = &fixture_state,
};
static cmeta_once_t fixture_once = SALTS_ONCE_INIT;

static void fixture_init(void) {
  fixture_factory =
      fixture_provider_factory_as_turbo_flow_provider_factory(&fixture_state);
  fixture_export = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
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

static cmeta_plugin_status CMETA_PLUGIN_CALL fixture_start(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  state->stopping = false;
  state->started = true;
  state->owner_quiesce_calls = 0u;
  state->owner_drain_calls = 0u;
  state->owner_shutdown_calls = 0u;
  state->owner_poll_calls = 0u;
  state->owner_destroy_calls = 0u;
  return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL fixture_request_stop(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL fixture_is_quiescent(const void *self) {
  const fixture_provider_state_t *state =
      (const fixture_provider_state_t *)self;
  return state && state->stopping;
}

static void CMETA_PLUGIN_CALL fixture_destroy(void *self) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state) return;
  state->started = false;
  state->stopping = true;
}

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  cmeta_once(&fixture_once, fixture_init);
  fixture_manifest.start = fixture_start;
  fixture_manifest.request_stop = fixture_request_stop;
  fixture_manifest.is_quiescent = fixture_is_quiescent;
  fixture_manifest.destroy = fixture_destroy;
  return &fixture_manifest;
}
