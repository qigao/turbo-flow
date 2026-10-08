#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdatomic.h>

typedef struct plugin_cflow_fixture_state_s {
  atomic_bool started;
  atomic_bool stopping;
  atomic_uint invokes;
} plugin_cflow_fixture_state_t;

static plugin_cflow_fixture_state_t fixture_state;

FunctionDecl(value, int, plugin_cflow_double,
    (int, value, CMETA_PARAM_IN));

int plugin_cflow_double(int value) {
  return value * 2;
}

static bool CMETA_PLUGIN_CALL plugin_cflow_double_invoke(
    void *context, void *return_storage, void *const *params,
    size_t param_count) {
  plugin_cflow_fixture_state_t *state =
      (plugin_cflow_fixture_state_t *)context;
  if (!state || !return_storage || !params || param_count != 1u ||
      !params[0] || !atomic_load(&state->started) ||
      atomic_load(&state->stopping))
    return false;
  *(int *)return_storage = plugin_cflow_double(*(const int *)params[0]);
  atomic_fetch_add(&state->invokes, 1u);
  return true;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL
plugin_cflow_start(void *self) {
  plugin_cflow_fixture_state_t *state =
      (plugin_cflow_fixture_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  atomic_store(&state->stopping, false);
  atomic_store(&state->started, true);
  return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL
plugin_cflow_request_stop(void *self) {
  plugin_cflow_fixture_state_t *state =
      (plugin_cflow_fixture_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  atomic_store(&state->stopping, true);
  atomic_store(&state->started, false);
  return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL
plugin_cflow_is_quiescent(const void *self) {
  const plugin_cflow_fixture_state_t *state =
      (const plugin_cflow_fixture_state_t *)self;
  return state && atomic_load(&state->stopping);
}

static void CMETA_PLUGIN_CALL
plugin_cflow_destroy(void *self) {
  plugin_cflow_fixture_state_t *state =
      (plugin_cflow_fixture_state_t *)self;
  if (!state) return;
  atomic_store(&state->started, false);
  atomic_store(&state->stopping, true);
}

static cmeta_plugin_export fixture_export;
static salts_once_t fixture_once = SALTS_ONCE_INIT;

static void plugin_cflow_init_export(void) {
  fixture_export = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_FUNCTION,
      .contract_version = 1u,
      .capabilities = 1u,
      .export_id = "test.turboflow.cflow.double",
      .contract_id = "test.turboflow.cflow",
      .value.function = {
          .desc = FunctionMeta(plugin_cflow_double),
          .abi = FunctionAbi(plugin_cflow_double),
          .context = &fixture_state,
          .invoke = plugin_cflow_double_invoke,
      },
  };
}

static cmeta_plugin_manifest fixture_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.cflow.plugin",
    .version = {1u, 0u, 0u},
    .exports = &fixture_export,
    .export_count = 1u,
    .self = &fixture_state,
    .start = plugin_cflow_start,
    .request_stop = plugin_cflow_request_stop,
    .is_quiescent = plugin_cflow_is_quiescent,
    .destroy = plugin_cflow_destroy,
};

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&fixture_once, plugin_cflow_init_export);
  return &fixture_manifest;
}
