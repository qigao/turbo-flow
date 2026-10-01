#include "turbo_flow_turbodb_resource.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>

#ifndef FLOW_TURBODB_RESOURCE_FIXTURE_DB
#error "FLOW_TURBODB_RESOURCE_FIXTURE_DB is required"
#endif

typedef struct database_resource_state_s {
  bool started;
  bool stopping;
  orm_option_t filename;
  orm_config_t database;
} database_resource_state_t;

static database_resource_state_t resource_state;

static int resource_snapshot(
    void *self, turbo_flow_turbodb_database_view_t *view_out) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state || !state->started || state->stopping || !view_out ||
      view_out->size != sizeof(*view_out))
    return SALTS_EINVAL;

  *view_out =
      (turbo_flow_turbodb_database_view_t)TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT;
  view_out->database = &state->database;
  view_out->namespace_name = "orders";
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_turbodb_database_resource, database_resource_impl,
    TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT,
    .snapshot = resource_snapshot);

static turbo_flow_turbodb_database_resource resource_handle;
static salts_plugin_export resource_export;
static salts_plugin_manifest resource_manifest = {
    .struct_size = SALTS_PLUGIN_MANIFEST_SIZE,
    .abi_version = SALTS_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.turbodb.database",
    .version = {1u, 0u, 0u},
    .self = &resource_state,
};
static salts_once_t resource_once = SALTS_ONCE_INIT;

static void resource_init(void) {
  orm_config(&resource_state.database);
  resource_state.filename.keyword = orm_view("filename");
  resource_state.filename.value = orm_view(FLOW_TURBODB_RESOURCE_FIXTURE_DB);
  resource_state.database.driver = orm_view("sqlite");
  resource_state.database.options = &resource_state.filename;
  resource_state.database.option_count = 1u;

  resource_handle =
      database_resource_impl_as_turbo_flow_turbodb_database_resource(
          &resource_state);
  resource_export = (salts_plugin_export){
      .struct_size = SALTS_PLUGIN_EXPORT_SIZE,
      .kind = SALTS_PLUGIN_EXPORT_INTERFACE,
      .contract_version =
          TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_VERSION,
      .capabilities = TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT,
      .export_id = "fixture.turbodb.database",
      .contract_id = TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_turbodb_database_resource_interface(),
          .value = &resource_handle,
      },
  };
  resource_manifest.exports = &resource_export;
  resource_manifest.export_count = 1u;
}

static salts_plugin_status SALTS_PLUGIN_CALL resource_start(void *self) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state) return SALTS_PLUGIN_INVALID_ARGUMENT;
  state->stopping = false;
  state->started = true;
  return SALTS_PLUGIN_OK;
}

static salts_plugin_status SALTS_PLUGIN_CALL resource_request_stop(void *self) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state) return SALTS_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return SALTS_PLUGIN_OK;
}

static bool SALTS_PLUGIN_CALL resource_is_quiescent(const void *self) {
  const database_resource_state_t *state =
      (const database_resource_state_t *)self;
  return state && state->stopping;
}

static void SALTS_PLUGIN_CALL resource_destroy(void *self) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state) return;
  state->started = false;
  state->stopping = true;
}

SALTS_PLUGIN_QUERY_EXPORT
const salts_plugin_manifest *SALTS_PLUGIN_CALL
salts_plugin_query(uint32_t host_abi) {
  if (host_abi != SALTS_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&resource_once, resource_init);
  resource_manifest.start = resource_start;
  resource_manifest.request_stop = resource_request_stop;
  resource_manifest.is_quiescent = resource_is_quiescent;
  resource_manifest.destroy = resource_destroy;
  return &resource_manifest;
}
