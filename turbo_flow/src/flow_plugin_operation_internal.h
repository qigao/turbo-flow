#ifndef FLOW_PLUGIN_OPERATION_INTERNAL_H
#define FLOW_PLUGIN_OPERATION_INTERNAL_H
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_materializer.h"
#include <cstl/vec.h>
#include <salts/thread.h>
#include <stdatomic.h>
enum {
  FLOW_PLUGIN_OPERATION_MAX_INFLIGHT = 1048576u,
  FLOW_PLUGIN_OPERATION_MAX_BYTES = 1073741824u
};
int flow_plugin_operation_callback_result(turbo_flow_plugin_operation_error_v3_t *error,
                                          uint32_t phase, int status);

/* Entries are preallocated by the control-thread domain and never moved while callbacks live. */
typedef struct flow_plugin_result_entry_s {
  turbo_flow_projection_owner_t *owner;
  void *context;
  turbo_flow_plugin_operation_vtable_v3_t vtable;
} flow_plugin_result_entry_t;
struct turbo_flow_plugin_result_domain_s {
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  vec_t entries;
  size_t count;
  uint32_t state;
  int last_cleanup_status;
};
int flow_plugin_result_domain_admit(const turbo_flow_plugin_result_domain_t *domain,
                                    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                    size_t count);
int flow_plugin_result_domain_attach(turbo_flow_plugin_result_domain_t *domain);
void flow_plugin_result_domain_detach(turbo_flow_plugin_result_domain_t *domain);
int flow_plugin_result_domain_materialize(turbo_flow_plugin_result_domain_t *domain,
                                          const turbo_flow_plugin_operation_v3_t *op,
                                          const turbo_flow_plugin_operation_request_v3_t *request,
                                          flow_plugin_result_entry_t **out,
                                          turbo_flow_plugin_operation_error_v3_t *error);

typedef struct flow_plugin_operation_binding_s {
  turbo_flow_plugin_operation_v3_t operation;
  turbo_flow_plugin_operation_request_v3_t request;
  flow_plugin_result_entry_t *result;
  void *session;
  atomic_uint admission;
  salts_mutex_t error_mutex;
  turbo_flow_plugin_operation_error_v3_t error;
  int has_materializer;
  turbo_flow_plugin_materializer_v1_t materializer;
  turbo_flow_data_schema_t materializer_native_schema;
  size_t materializer_max_encoded_bytes;
} flow_plugin_operation_binding_t;
int flow_plugin_operations_prepare(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                   const turbo_flow_resolved_config_t *resolved, turbo_flow_t *flow,
                                   turbo_flow_plugin_result_domain_t *domain, size_t budget,
                                   vec_t *bindings, turbo_flow_config_error_t *error);
int flow_plugin_operations_preflight(vec_t *bindings, turbo_flow_config_error_t *error);
int flow_plugin_materializers_prepare(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                      const turbo_flow_resolved_config_t *resolved,
                                      vec_t *bindings, turbo_flow_config_error_t *error);
size_t flow_plugin_materializer_binding_count(const vec_t *bindings);
int flow_plugin_operations_materialize(vec_t *bindings, turbo_flow_plugin_result_domain_t *domain,
                                       turbo_flow_t *flow, turbo_flow_config_error_t *error);
int flow_plugin_operations_close(vec_t *bindings);
int flow_plugin_operations_release(vec_t *bindings, turbo_flow_config_error_t *error);
void flow_plugin_operations_free(vec_t *bindings);
#endif
