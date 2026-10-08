#include "salts_resource_fixture.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>

typedef struct resource_state_s {
  bool started;
  bool stopping;
} resource_state_t;

static resource_state_t resource_state;

static int resource_ping(void *self) {
  resource_state_t *state = (resource_state_t *)self;
  return state && state->started && !state->stopping ? 7 : -1;
}

CMETA_IMPLEMENTS(flow_test_resource, resource_impl,
                 FLOW_TEST_RESOURCE_CAP_READ,
                 .ping = resource_ping);

static flow_test_resource resource_handle;
static cmeta_plugin_export resource_export;
static cmeta_plugin_manifest resource_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.resource",
    .version = {1u, 0u, 0u},
    .self = &resource_state,
};
static cmeta_once_t resource_once = SALTS_ONCE_INIT;

static void resource_init(void) {
  resource_handle = resource_impl_as_flow_test_resource(&resource_state);
  resource_export = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .contract_version = FLOW_TEST_RESOURCE_CONTRACT_VERSION,
      .capabilities = FLOW_TEST_RESOURCE_CAP_READ,
      .export_id = "fixture.resource",
      .contract_id = FLOW_TEST_RESOURCE_CONTRACT_ID,
      .value.interface = {
          .desc = flow_test_resource_interface(),
          .value = &resource_handle,
      },
  };
  resource_manifest.exports = &resource_export;
  resource_manifest.export_count = 1u;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL resource_start(void *self) {
  resource_state_t *state = (resource_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  state->stopping = false;
  state->started = true;
  return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL resource_request_stop(void *self) {
  resource_state_t *state = (resource_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL resource_is_quiescent(const void *self) {
  const resource_state_t *state = (const resource_state_t *)self;
  return state && state->stopping;
}

static void CMETA_PLUGIN_CALL resource_destroy(void *self) {
  resource_state_t *state = (resource_state_t *)self;
  if (!state) return;
  state->started = false;
  state->stopping = true;
}

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  cmeta_once(&resource_once, resource_init);
  resource_manifest.start = resource_start;
  resource_manifest.request_stop = resource_request_stop;
  resource_manifest.is_quiescent = resource_is_quiescent;
  resource_manifest.destroy = resource_destroy;
  return &resource_manifest;
}
