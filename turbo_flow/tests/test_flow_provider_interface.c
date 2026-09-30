#include "tinytest.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#include <string.h>

typedef struct fixture_owner_state_s {
  unsigned quiesce_calls;
  unsigned drain_calls;
  unsigned shutdown_calls;
  unsigned poll_calls;
  unsigned destroy_calls;
} fixture_owner_state_t;

static int fixture_owner_quiesce(void *self, uint64_t timeout_ms) {
  fixture_owner_state_t *state = (fixture_owner_state_t *)self;
  if (!state || timeout_ms == 0u) return SALTS_EINVAL;
  ++state->quiesce_calls;
  return SALTS_OK;
}

static int fixture_owner_drain(void *self, uint64_t timeout_ms) {
  fixture_owner_state_t *state = (fixture_owner_state_t *)self;
  if (!state || timeout_ms == 0u) return SALTS_EINVAL;
  ++state->drain_calls;
  return SALTS_OK;
}

static int fixture_owner_shutdown(void *self) {
  fixture_owner_state_t *state = (fixture_owner_state_t *)self;
  if (!state) return SALTS_EINVAL;
  ++state->shutdown_calls;
  return SALTS_OK;
}

static int fixture_owner_poll(void *self, uint32_t timeout_ms) {
  fixture_owner_state_t *state = (fixture_owner_state_t *)self;
  (void)timeout_ms;
  if (!state) return SALTS_EINVAL;
  ++state->poll_calls;
  return SALTS_OK;
}

static void fixture_owner_destroy(void *self) {
  fixture_owner_state_t *state = (fixture_owner_state_t *)self;
  if (state) ++state->destroy_calls;
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

static DataBindStatus fixture_codec_factory(DataBind **out, DataBindError *error) {
  if (out) *out = NULL;
  if (error) {
    *error = (DataBindError){sizeof(*error), DATA_BIND_ERR_SCHEMA, 0, 0, {0}, {0}};
  }
  return DATA_BIND_ERR_SCHEMA;
}

static DataBindStatus fixture_native_binding(DataBindNativeTypeBinding *out,
                                             DataBindError *error) {
  if (out) memset(out, 0, sizeof(*out));
  if (error) {
    *error = (DataBindError){sizeof(*error), DATA_BIND_ERR_SCHEMA, 0, 0, {0}, {0}};
  }
  return DATA_BIND_ERR_SCHEMA;
}

static const DataBindMessageNativeArtifact FIXTURE_ARTIFACT = {
    sizeof(DataBindMessageNativeArtifact),
    DATA_BIND_MESSAGE_NATIVE_ARTIFACT_ABI_VERSION,
    "FixtureConfig",
    fixture_native_binding};

typedef struct fixture_provider_state_s {
  fixture_owner_state_t owner;
  unsigned describe_calls;
  unsigned preflight_calls;
  unsigned materialize_calls;
} fixture_provider_state_t;

static int fixture_provider_describe(void *self,
                                     turbo_flow_provider_contract_v1_t *out) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  ++state->describe_calls;
  *out = (turbo_flow_provider_contract_v1_t)TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory = fixture_codec_factory;
  out->config.message_artifact = &FIXTURE_ARTIFACT;
  return SALTS_OK;
}

static int fixture_provider_preflight(
    void *self, const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || !instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0])
    return SALTS_EINVAL;
  ++state->preflight_calls;
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

static int fixture_provider_materialize(
    void *self, turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  fixture_provider_state_t *state = (fixture_provider_state_t *)self;
  if (!state || !flow || !instance || !owner_out) return SALTS_EINVAL;
  ++state->materialize_calls;
  *owner_out = fixture_runtime_owner_as_turbo_flow_runtime_owner(&state->owner);
  if (error) *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_provider_factory, fixture_provider, 0u,
    .describe = fixture_provider_describe,
    .preflight = fixture_provider_preflight,
    .materialize = fixture_provider_materialize);

spec("canonical TurboFlow provider Interface") {
  it("admits provider factories through exact Salts Plugin Interface identity") {
    fixture_provider_state_t state = {0};
    turbo_flow_provider_factory factory =
        fixture_provider_as_turbo_flow_provider_factory(&state);
    salts_plugin_export entry = {
        SALTS_PLUGIN_EXPORT_SIZE,
        SALTS_PLUGIN_EXPORT_INTERFACE,
        TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
        0u,
        "fixture.provider",
        TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
        {.interface = {turbo_flow_provider_factory_interface(), &factory}}};

    check_true(turbo_flow_provider_factory_valid(&factory));
    check_true(cmeta_interface_desc_valid(turbo_flow_provider_factory_interface()));
    check_equal(
        salts_plugin_export_require_interface(
            &entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION, 0u,
            turbo_flow_provider_factory_interface()),
        SALTS_PLUGIN_OK);
    check_equal(
        salts_plugin_export_require_interface(
            &entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION + 1u, 0u,
            turbo_flow_provider_factory_interface()),
        SALTS_PLUGIN_INCOMPATIBLE_CONTRACT);
  }

  it("describes typed config and materializes one CMeta runtime owner") {
    fixture_provider_state_t state = {0};
    turbo_flow_provider_factory factory =
        fixture_provider_as_turbo_flow_provider_factory(&state);
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    turbo_flow_provider_instance_v1_t instance =
        TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
    turbo_flow_runtime_owner owner = {0};
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(turbo_flow_provider_factory_describe(&factory, &contract), SALTS_OK);
    check_true(turbo_flow_provider_contract_valid(&contract));
    check_equal(contract.config.message_artifact, &FIXTURE_ARTIFACT);
    check_equal(contract.resource.contract_id, NULL);

    instance.instance_name = "stage_a";
    instance.config.type_name = "FixtureConfig";
    check_equal(
        turbo_flow_provider_factory_preflight(&factory, &instance, &error),
        SALTS_OK);
    check_equal(
        turbo_flow_provider_factory_materialize(
            &factory, flow, &instance, &owner, &error),
        SALTS_OK);
    check_true(turbo_flow_runtime_owner_contract_valid(&owner));
    check_true(turbo_flow_runtime_owner_has(
        &owner, TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL));

    check_equal(turbo_flow_runtime_owner_quiesce(&owner, 10u), SALTS_OK);
    check_equal(turbo_flow_runtime_owner_drain(&owner, 10u), SALTS_OK);
    check_equal(turbo_flow_runtime_owner_poll(&owner, 0u), SALTS_OK);
    check_equal(turbo_flow_runtime_owner_shutdown(&owner), SALTS_OK);
    turbo_flow_runtime_owner_destroy(&owner);

    check_equal(state.describe_calls, 1u);
    check_equal(state.preflight_calls, 1u);
    check_equal(state.materialize_calls, 1u);
    check_equal(state.owner.quiesce_calls, 1u);
    check_equal(state.owner.drain_calls, 1u);
    check_equal(state.owner.poll_calls, 1u);
    check_equal(state.owner.shutdown_calls, 1u);
    check_equal(state.owner.destroy_calls, 1u);
    check_false(turbo_flow_runtime_owner_valid(&owner));

    turbo_flow_destroy(flow);
  }
}
