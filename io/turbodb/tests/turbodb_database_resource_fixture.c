#include "turbo_flow_turbodb_resource.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <string.h>

#ifndef FLOW_TURBODB_RESOURCE_FIXTURE_DB
#error "FLOW_TURBODB_RESOURCE_FIXTURE_DB is required"
#endif
#ifndef FLOW_TURBODB_SQLITE_DRIVER
#error "FLOW_TURBODB_SQLITE_DRIVER is required"
#endif

typedef struct database_resource_state_s {
  bool started;
  bool stopping;
  orm_runtime_t *runtime;
  orm_option_t filename;
  orm_config_t database;
} database_resource_state_t;

static database_resource_state_t resource_state;

static int resource_snapshot(
    void *self, turbo_flow_turbodb_database_view_t *view_out) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state || !state->started || state->stopping || !state->runtime ||
      !view_out || view_out->size != sizeof(*view_out))
    return SALTS_EINVAL;

  *view_out =
      (turbo_flow_turbodb_database_view_t)TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT;
  view_out->runtime = state->runtime;
  view_out->database = &state->database;
  view_out->namespace_name = "orders";
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_turbodb_database_resource, database_resource_impl,
    TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT,
    .snapshot = resource_snapshot);

static turbo_flow_turbodb_database_resource resource_handle;
static cmeta_plugin_export resource_export;
static cmeta_plugin_manifest resource_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.turbodb.database",
    .version = {1u, 0u, 0u},
    .self = &resource_state,
};
static cmeta_once_t resource_once = SALTS_ONCE_INIT;

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
  resource_export = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
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

static cmeta_plugin_status CMETA_PLUGIN_CALL resource_start(void *self) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  orm_runtime_config_t runtime_config;
  orm_driver_load_config_t load = {0};
  orm_error_t error;

  if (!state || state->runtime) return CMETA_PLUGIN_INVALID_ARGUMENT;

  orm_runtime_config_init(&runtime_config);
  orm_error_init(&error);
  if (orm_runtime_create(&runtime_config, &state->runtime, &error) !=
      ORM_STATUS_OK)
    return CMETA_PLUGIN_LOAD_FAILED;

  load.struct_size = (uint32_t)sizeof(load);
  load.abi_version = ORM_RUNTIME_ABI_VERSION;
  load.module_path = orm_view(FLOW_TURBODB_SQLITE_DRIVER);
  load.expected_driver_id = orm_view("sqlite");
  if (orm_runtime_load_driver(state->runtime, &load, &error) !=
      ORM_STATUS_OK) {
    (void)orm_runtime_close(state->runtime, &error);
    orm_runtime_release(state->runtime);
    state->runtime = NULL;
    return CMETA_PLUGIN_LOAD_FAILED;
  }

  state->stopping = false;
  state->started = true;
  return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL resource_request_stop(void *self) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state) return CMETA_PLUGIN_INVALID_ARGUMENT;
  state->stopping = true;
  state->started = false;
  return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL resource_is_quiescent(const void *self) {
  database_resource_state_t *state =
      (database_resource_state_t *)(uintptr_t)self;
  orm_error_t error;
  orm_status_t status;

  if (!state || !state->stopping) return false;
  if (!state->runtime) return true;

  orm_error_init(&error);
  status = orm_runtime_close(state->runtime, &error);
  if (status != ORM_STATUS_OK) return false;
  orm_runtime_release(state->runtime);
  state->runtime = NULL;
  return true;
}

static void CMETA_PLUGIN_CALL resource_destroy(void *self) {
  database_resource_state_t *state = (database_resource_state_t *)self;
  if (!state) return;
  state->started = false;
  state->stopping = true;
}

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&resource_once, resource_init);
  resource_manifest.start = resource_start;
  resource_manifest.request_stop = resource_request_stop;
  resource_manifest.is_quiescent = resource_is_quiescent;
  resource_manifest.destroy = resource_destroy;
  return &resource_manifest;
}
