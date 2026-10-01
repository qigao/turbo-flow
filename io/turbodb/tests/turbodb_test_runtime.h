#ifndef TURBO_FLOW_TURBODB_TEST_RUNTIME_H
#define TURBO_FLOW_TURBODB_TEST_RUNTIME_H

#include <orm_runtime.h>

#include <string.h>

#ifndef FLOW_TURBODB_SQLITE_DRIVER
#error "FLOW_TURBODB_SQLITE_DRIVER is required"
#endif

static inline orm_status_t turbodb_test_sqlite_runtime_open(
    orm_runtime_t **runtime_out, orm_error_t *error) {
  orm_runtime_config_t runtime_config;
  orm_driver_load_config_t load = {0};
  orm_runtime_t *runtime = NULL;
  orm_status_t status;

  if (runtime_out) *runtime_out = NULL;
  if (!runtime_out || !error) return ORM_STATUS_INVALID_ARGUMENT;

  orm_runtime_config_init(&runtime_config);
  status = orm_runtime_create(&runtime_config, &runtime, error);
  if (status != ORM_STATUS_OK) return status;

  load.struct_size = (uint32_t)sizeof(load);
  load.abi_version = ORM_RUNTIME_ABI_VERSION;
  load.module_path = orm_view(FLOW_TURBODB_SQLITE_DRIVER);
  load.expected_driver_id = orm_view("sqlite");
  status = orm_runtime_load_driver(runtime, &load, error);
  if (status != ORM_STATUS_OK) {
    (void)orm_runtime_close(runtime, error);
    orm_runtime_release(runtime);
    return status;
  }

  *runtime_out = runtime;
  return ORM_STATUS_OK;
}

static inline orm_status_t turbodb_test_runtime_close(
    orm_runtime_t **runtime_io, orm_error_t *error) {
  orm_status_t status;
  if (!runtime_io || !error) return ORM_STATUS_INVALID_ARGUMENT;
  if (!*runtime_io) return ORM_STATUS_OK;
  status = orm_runtime_close(*runtime_io, error);
  if (status != ORM_STATUS_OK) return status;
  orm_runtime_release(*runtime_io);
  *runtime_io = NULL;
  return ORM_STATUS_OK;
}

#endif /* TURBO_FLOW_TURBODB_TEST_RUNTIME_H */
