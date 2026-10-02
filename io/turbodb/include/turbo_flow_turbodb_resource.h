#ifndef TURBO_FLOW_TURBODB_RESOURCE_H
#define TURBO_FLOW_TURBODB_RESOURCE_H

#include "turbo_flow_turbodb.h"

#include <orm_runtime.h>

#include <cmeta/interface.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_ID \
  "turbo_flow.turbodb.database"

enum {
  TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_VERSION = 1u
};

enum {
  TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT = UINT64_C(1) << 0
};

/**
 * Borrowed deployment database view.
 *
 * database may carry driver/session/credential options owned by the deployment
 * resource. TurboFlow Core never copies or logs those values. runtime owns the
 * exact dynamically loaded TurboDB driver registry; database selects one driver
 * already admitted by that runtime. namespace_name is the physical durable-inbox
 * namespace. All three are borrowed under the same resource Salts Plugin lease. Only the TurboDB provider interprets this provider-specific
 * view; the generic resource binder treats the Interface value as opaque.
 */
typedef struct turbo_flow_turbodb_database_view_s {
  size_t size;
  orm_runtime_t *runtime;
  const orm_config_t *database;
  const char *namespace_name;
} turbo_flow_turbodb_database_view_t;

#define TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT \
  {sizeof(turbo_flow_turbodb_database_view_t), NULL, NULL, NULL}

#define TURBO_FLOW_TURBODB_DATABASE_RESOURCE_METHODS(X, I) \
  X(I, R1, int, snapshot, turbo_flow_turbodb_database_view_t *, view_out)

CMETA_INTERFACE(
    turbo_flow_turbodb_database_resource,
    TURBO_FLOW_TURBODB_DATABASE_RESOURCE_METHODS);

static inline int turbo_flow_turbodb_database_view_valid(
    const turbo_flow_turbodb_database_view_t *view) {
  return view != NULL && view->size == sizeof(*view) &&
         view->runtime != NULL && view->database != NULL &&
         view->namespace_name != NULL &&
         view->namespace_name[0] != '\0';
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_TURBODB_RESOURCE_H */
