#include "plugin_operation_fixture.h"
#include <stdlib.h>
#include <string.h>

#ifndef FLOW_OPERATION_MODE
  #define FLOW_OPERATION_MODE OP_FIXTURE_OK
#endif
#ifndef FLOW_OPERATION_ID
  #define FLOW_OPERATION_ID "fixture.operation"
#endif
typedef struct operation_context_s {
  operation_fixture_t *factory;
  int releases;
} operation_context_t;
static void fixture_barrier(operation_fixture_t *f, int point) {
  salts_mutex_lock(&f->mutex);
  if (f->barrier == point) {
    f->entered = 1;
    salts_cond_broadcast(&f->cond);
    while (!f->proceed)
      salts_cond_wait(&f->cond, &f->mutex);
  }
  salts_mutex_unlock(&f->mutex);
}
static const turbo_flow_data_schema_t record_schema = {sizeof(turbo_flow_data_schema_t),
                                                       TURBO_FLOW_DOMAIN_DATA,
                                                       TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                       "operation.Record.data",
                                                       "Record",
                                                       "operation_record",
                                                       8u,
                                                       1u,
                                                       NULL};
static const turbo_flow_data_schema_t fixture_schema = {sizeof(turbo_flow_data_schema_t),
                                                        TURBO_FLOW_DOMAIN_DATA,
                                                        TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                        "cmeta.int.data",
                                                        "Integer",
                                                        "int",
                                                        7u,
                                                        1u,
                                                        NULL};
static int operation_preflight_impl(void *ctx, const turbo_flow_plugin_operation_request_v3_t *r,
                                    turbo_flow_plugin_operation_error_v3_t *error) {
  (void)error;
  if (!ctx || !r || r->size != sizeof(*r)) return SALTS_EINVAL;
  atomic_fetch_add(&((operation_fixture_t *)ctx)->preflights, 1);
  return ((operation_fixture_t *)ctx)->mode == OP_FIXTURE_PREFLIGHT_FAIL ? SALTS_EIO : SALTS_OK;
}
static int context_create_impl(void *ctx, const turbo_flow_plugin_operation_request_v3_t *r,
                               void **out, turbo_flow_plugin_operation_error_v3_t *error) {
  operation_context_t *c;
  int mode = ((operation_fixture_t *)ctx)->mode;
  atomic_fetch_add(&((operation_fixture_t *)ctx)->context_creates, 1);
  (void)r;
  (void)error;
  *out = NULL;
  if (mode == OP_FIXTURE_CONTEXT_FAIL) return SALTS_EIO;
  if (mode == OP_FIXTURE_CONTEXT_FACTORY_ALIAS) {
    *out = ctx;
    return SALTS_OK;
  }
  c = calloc(1, sizeof(*c));
  if (!c) return SALTS_ENOMEM;
  c->factory = ctx;
  ++c->factory->contexts;
  *out = c;
  /* Deliberate serialized contract violation to exercise real owner_create rejection after
   * allocation. */
  if (mode == OP_FIXTURE_OWNER_FAIL) c->factory->fault_schema.size = 0;
  return mode == OP_FIXTURE_CONTEXT_FAIL_VALUE ? SALTS_EIO : SALTS_OK;
}
static int session_create_impl(void *ctx, const turbo_flow_plugin_operation_request_v3_t *r,
                               void *result_ctx, void **out,
                               turbo_flow_plugin_operation_error_v3_t *error) {
  operation_context_t *s;
  int mode = ((operation_fixture_t *)ctx)->mode;
  atomic_fetch_add(&((operation_fixture_t *)ctx)->session_creates, 1);
  if (mode == OP_FIXTURE_SECOND_SESSION_FAIL &&
      atomic_load(&((operation_fixture_t *)ctx)->session_creates) == 2)
    return SALTS_EIO;
  (void)r;
  (void)error;
  *out = NULL;
  if (mode == OP_FIXTURE_SESSION_FAIL) return SALTS_EIO;
  if (mode == OP_FIXTURE_SESSION_FACTORY_ALIAS) {
    *out = ctx;
    return SALTS_OK;
  }
  if (mode == OP_FIXTURE_SESSION_RESULT_ALIAS) {
    *out = result_ctx;
    return SALTS_OK;
  }
  s = calloc(1, sizeof(*s));
  if (!s) return SALTS_ENOMEM;
  s->factory = ctx;
  ++s->factory->sessions;
  *out = s;
  return mode == OP_FIXTURE_SESSION_FAIL_VALUE ? SALTS_EIO : SALTS_OK;
}
static int execute_impl(void *ctx, const turbo_flow_plugin_operation_input_v3_t *input,
                        const turbo_flow_plugin_operation_budget_v3_t *budget, void **out,
                        turbo_flow_plugin_operation_error_v3_t *error) {
  int rc;
  operation_fixture_t *f = ((operation_context_t *)ctx)->factory;
  int mode = f->mode;
  *out = NULL;
  atomic_fetch_add(&f->executes, 1);
  fixture_barrier(f, OP_BARRIER_EXECUTE);
  if (mode == OP_FIXTURE_ZERO_CHARGE) return budget->charge(budget->ctx, 0);
  rc = budget->charge(budget->ctx, 1);
  if (rc == SALTS_OK) rc = budget->charge(budget->ctx, 1);
  if (rc != SALTS_OK && mode != OP_FIXTURE_SWALLOW_CHARGE) return rc;
  if (mode == OP_FIXTURE_EXECUTE_NULL) return SALTS_OK;
  if (mode == OP_FIXTURE_EXECUTE_FAIL_NULL) {
    error->phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE;
    error->engine_status = SALTS_EIO;
    return SALTS_EIO;
  }
  if (mode == OP_FIXTURE_EXECUTE_ALIAS || mode == OP_FIXTURE_EXECUTE_FAIL_ALIAS) {
    *out = (char *)input->value + sizeof(int);
    return mode == OP_FIXTURE_EXECUTE_ALIAS ? SALTS_OK : SALTS_EIO;
  }
  *out = malloc(sizeof(int));
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)input->value * 2;
  return mode == OP_FIXTURE_EXECUTE_FAIL_VALUE ? SALTS_EIO : SALTS_OK;
}
/* Fault the real DLL's returned shared error only after its normal output/ownership work. */
static int fixture_error_return(operation_fixture_t *f, uint32_t phase,
                                turbo_flow_plugin_operation_error_v3_t *error, int rc) {
  if (f->error_phase != phase) return rc;
  turbo_flow_plugin_operation_error_v3_init(error);
  error->status = SALTS_EPROTO;
  error->engine_status = SALTS_EIO;
  error->phase = TURBO_FLOW_PLUGIN_OPERATION_PHASE_RELEASE;
  strcpy(error->message, "fixture diagnostic");
  if (f->error_fault != OP_ERROR_VALID) {
    error->phase = UINT32_MAX;
    memset(error->message, 'X', sizeof(error->message));
  }
  switch (f->error_fault) {
  case OP_ERROR_SHORT:
    error->size = sizeof(size_t);
    break;
  case OP_ERROR_TRUNCATED:
    --error->size;
    break;
  case OP_ERROR_LONG:
    ++error->size;
    break;
  case OP_ERROR_OLD_MAJOR:
    error->abi_major = 2u;
    break;
  case OP_ERROR_NEW_MAJOR:
    error->abi_major = 4u;
    break;
  case OP_ERROR_NEW_MINOR:
    error->abi_minor = 1u;
    break;
  default:
    break;
  }
  return f->error_status;
}
static int operation_preflight(void *ctx, const turbo_flow_plugin_operation_request_v3_t *r,
                               turbo_flow_plugin_operation_error_v3_t *error) {
  int rc = operation_preflight_impl(ctx, r, error);
  return fixture_error_return(ctx, TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT, error, rc);
}
static int context_create(void *ctx, const turbo_flow_plugin_operation_request_v3_t *r, void **out,
                          turbo_flow_plugin_operation_error_v3_t *error) {
  int rc = context_create_impl(ctx, r, out, error);
  return fixture_error_return(ctx, TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT, error, rc);
}
static int session_create(void *ctx, const turbo_flow_plugin_operation_request_v3_t *r,
                          void *result_ctx, void **out,
                          turbo_flow_plugin_operation_error_v3_t *error) {
  int rc = session_create_impl(ctx, r, result_ctx, out, error);
  return fixture_error_return(ctx, TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION, error, rc);
}
static int execute(void *ctx, const turbo_flow_plugin_operation_input_v3_t *input,
                   const turbo_flow_plugin_operation_budget_v3_t *budget, void **out,
                   turbo_flow_plugin_operation_error_v3_t *error) {
  int rc = execute_impl(ctx, input, budget, out, error);
  return fixture_error_return(((operation_context_t *)ctx)->factory,
                              TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE, error, rc);
}
static int clone_result(const void *value, void *ctx, void **out) {
  operation_fixture_t *f = ((operation_context_t *)ctx)->factory;
  atomic_fetch_add(&f->clones, 1);
  fixture_barrier(f, OP_BARRIER_CLONE);
  *out = malloc(sizeof(int));
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)value;
  return SALTS_OK;
}
static void destroy_result(void *value, void *ctx) {
  operation_fixture_t *f = ((operation_context_t *)ctx)->factory;
  atomic_fetch_add(&f->destroys, 1);
  fixture_barrier(f, OP_BARRIER_DESTROY);
  free(value);
}
static int session_release(void *ctx) {
  operation_context_t *s = ctx;
  int mode = s->factory->mode;
  atomic_fetch_add(&s->factory->session_releases, 1);
  if ((mode == OP_FIXTURE_RELEASE_SESSION_ONCE || s->factory->fail_session_release) &&
      !s->releases++)
    return SALTS_EIO;
  --s->factory->sessions;
  free(s);
  return SALTS_OK;
}
static int context_release(void *ctx) {
  operation_context_t *c = ctx;
  int mode = c->factory->mode;
  atomic_fetch_add(&c->factory->context_releases, 1);
  if ((mode == OP_FIXTURE_RELEASE_CONTEXT_ONCE || c->factory->fail_context_release) &&
      !c->releases++)
    return SALTS_EIO;
  --c->factory->contexts;
  free(c);
  return SALTS_OK;
}
static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  if (!out) return SALTS_EINVAL;
  *out = NULL;
  if (!host || host->size != sizeof(*host) || host->abi_major != 3u || host->abi_minor != 0u)
    return SALTS_EINVAL;
  operation_fixture_t *f = calloc(1, sizeof(*f));
  if (!f) return SALTS_ENOMEM;
  f->mode = FLOW_OPERATION_MODE;
  salts_mutex_init(&f->mutex);
  salts_cond_init(&f->cond);
  if (!f->mutex || !f->cond) {
    if (f->mutex) salts_mutex_destroy(&f->mutex);
    if (f->cond) salts_cond_destroy(&f->cond);
    free(f);
    return SALTS_ENOMEM;
  }
  *out = f;
  return SALTS_OK;
}
static int fixture_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                           turbo_flow_msg_t *message) {
  return ctx && flow && stage && message ? SALTS_OK : SALTS_EINVAL;
}
static int fixture_adapter(void *ctx, turbo_flow_t *flow,
                           const turbo_flow_resolved_config_t *resolved, const char *name,
                           turbo_flow_config_error_t *error) {
  turbo_flow_adapter_ops_t ops = {0};
  (void)resolved;
  (void)error;
  ops.consume = fixture_consume;
  return turbo_flow_register_adapter(flow, name, &ops, ctx);
}
static int fixture_register(void *ctx, const turbo_flow_plugin_registration_v1_t *r) {
  turbo_flow_plugin_schema_v1_t schema = TURBO_FLOW_PLUGIN_SCHEMA_V1_INIT;
  turbo_flow_plugin_operation_v3_t op;
  int rc;
  int mode = ((operation_fixture_t *)ctx)->mode;
  if (!r || r->size != sizeof(*r) || r->abi_major != 3u || r->abi_minor != 0u ||
      !r->add_operation || !r->add_schema)
    return SALTS_EINVAL;
  turbo_flow_plugin_product_adapter_provider_v1_t adapter =
      TURBO_FLOW_PLUGIN_PRODUCT_ADAPTER_PROVIDER_V1_INIT;
  adapter.provider.kind = FLOW_OPERATION_ID ".adapter";
  adapter.provider.register_adapter = fixture_adapter;
  adapter.provider.ctx = ctx;
  rc = r->add_adapter_provider(r->ctx, &adapter);
  if (rc != SALTS_OK && mode != OP_FIXTURE_SWALLOW) return rc;
  turbo_flow_plugin_operation_v3_init(&op);
  op.operation_name = "fixture.double";
  op.operation_version = 1;
  op.input.schema_version = op.output.schema_version = 1;
  op.input.data = op.output.data = &cmeta_data_int;
  op.input.projection = op.output.projection = &fixture_schema;
  op.execution = TURBO_FLOW_PLUGIN_OPERATION_SYNC;
  op.threading = TURBO_FLOW_PLUGIN_OPERATION_THREAD_SAFE;
  op.effects = TURBO_FLOW_PLUGIN_OPERATION_EFFECT_RESULT;
  op.guarantees = TURBO_FLOW_PLUGIN_OPERATION_STEPS_CHARGED;
  op.limits.max_inflight = 8;
  op.limits.max_input_bytes = 64;
  op.limits.max_result_bytes = 64;
  op.limits.max_retained_bytes = 512;
  op.limits.max_steps = 8;
  op.max_session_bytes = op.max_result_context_bytes = sizeof(operation_context_t);
  op.factory_ctx = ctx;
  op.preflight = operation_preflight;
  op.create_result_context = context_create;
  op.create_session = session_create;
  op.vtable.execute = execute;
  op.vtable.clone_result = clone_result;
  op.vtable.destroy_result = destroy_result;
  op.vtable.release_session = session_release;
  op.vtable.release_result_context = context_release;
  schema.schema_version = 1;
  schema.data = &cmeta_data_int;
  static const char *permission[] = {"network"};
  static const char *bad_permission[] = {"bad permission"};
  static const char *duplicate_permissions[] = {"network", "network"};
  static cmeta_data_desc bad_data;
  bad_data = cmeta_data_int;
  bad_data.shape = NULL;
  switch (mode) {
  case OP_FIXTURE_BAD_SHAPE:
    op.input.data = &bad_data;
    break;
  case OP_FIXTURE_BAD_ABI:
    op.abi_major = 2;
    break;
  case OP_FIXTURE_SHORT:
    --op.size;
    break;
  case OP_FIXTURE_LONG:
    ++op.size;
    break;
  case OP_FIXTURE_BAD_VTABLE:
    --op.vtable.size;
    break;
  case OP_FIXTURE_NO_GUARANTEE:
    op.guarantees = 0;
    break;
  case OP_FIXTURE_BAD_EFFECT:
    op.effects = UINT32_MAX;
    break;
  case OP_FIXTURE_BAD_PERMISSION:
    op.permissions = bad_permission;
    op.permission_count = 1;
    break;
  case OP_FIXTURE_DUP_PERMISSION:
    op.permissions = duplicate_permissions;
    op.permission_count = 2;
    break;
  case OP_FIXTURE_PERMISSION:
    op.permissions = permission;
    op.permission_count = 1;
    break;
  case OP_FIXTURE_BAD_LIMIT:
    op.limits.max_inflight = 0;
    break;
  case OP_FIXTURE_MISSING_OUTPUT:
    op.output.data = &operation_record_data;
    op.output.projection = &record_schema;
    break;
  case OP_FIXTURE_MEMORY_OVERFLOW:
    op.max_session_bytes = SIZE_MAX;
    break;
  case OP_FIXTURE_OWNER_FAIL:
    ((operation_fixture_t *)ctx)->fault_schema = fixture_schema;
    op.output.projection = &((operation_fixture_t *)ctx)->fault_schema;
    break;
  default:
    break;
  }
  if (mode == OP_FIXTURE_EXECUTE_ALIAS || mode == OP_FIXTURE_EXECUTE_FAIL_ALIAS) {
    op.input.data = &operation_record_data;
    op.input.projection = &record_schema;
    turbo_flow_plugin_schema_v1_t record = TURBO_FLOW_PLUGIN_SCHEMA_V1_INIT;
    record.schema_version = 1;
    record.data = &operation_record_data;
    rc = r->add_schema(r->ctx, &record);
    if (rc != SALTS_OK) return rc;
  }
  if (mode == OP_FIXTURE_SWALLOW) (void)r->add_schema(r->ctx, &schema);
  rc = r->add_operation(r->ctx, &op);
  if (mode == OP_FIXTURE_DUPLICATE_CHANGED) op.limits.max_steps++;
  if (mode == OP_FIXTURE_DUPLICATE || mode == OP_FIXTURE_DUPLICATE_CHANGED ||
      mode == OP_FIXTURE_SWALLOW)
    rc = r->add_operation(r->ctx, &op);
  if (mode == OP_FIXTURE_TWO_VERSIONS) {
    op.operation_version++;
    rc = r->add_operation(r->ctx, &op);
  }
  if (mode == OP_FIXTURE_TWO_NAMES) {
    op.operation_name = "fixture.other";
    rc = r->add_operation(r->ctx, &op);
  }
  if (mode != OP_FIXTURE_NO_SCHEMA && mode != OP_FIXTURE_SWALLOW) {
    int schema_rc = r->add_schema(r->ctx, &schema);
    if (rc == SALTS_OK) rc = schema_rc;
  }
  return mode == OP_FIXTURE_SWALLOW ? SALTS_OK : rc;
}
static int fixture_quiesce(void *ctx, uint64_t timeout) {
  (void)ctx;
  (void)timeout;
  return SALTS_OK;
}
static int fixture_shutdown(void *ctx) {
  operation_fixture_t *f = ctx;
  return f->sessions || f->contexts ? SALTS_EBUSY : SALTS_OK;
}
static void fixture_destroy(void *ctx) {
  operation_fixture_t *f = ctx;
  salts_cond_destroy(&f->cond);
  salts_mutex_destroy(&f->mutex);
  free(f);
}
static const turbo_flow_plugin_api_v1_t fixture_api = {sizeof(turbo_flow_plugin_api_v1_t),
                                                       3u,
                                                       0u,
                                                       FLOW_OPERATION_ID,
                                                       "1.0.0",
                                                       (uint32_t)TURBO_FLOW_PLUGIN_CAP_OPERATION |
                                                           TURBO_FLOW_PLUGIN_CAP_SCHEMA |
                                                           TURBO_FLOW_PLUGIN_CAP_PRODUCT_ADAPTER,
                                                       fixture_load,
                                                       fixture_register,
                                                       fixture_quiesce,
                                                       fixture_shutdown,
                                                       fixture_destroy};
TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &fixture_api;
}
