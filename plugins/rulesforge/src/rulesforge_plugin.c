#include "turbo_flow_rulesforge_plugin.h"

#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_materializer.h"
#include "turbo_flow_provider.h"
#include "turbo_flow_rulesforge_resource.h"

#include <data_bind_format_provider.h>
#include <data_bind_json_provider.h>
#include <data_bind_native.h>
#include <rules_forge.h>
#include <cmeta_error.h>

#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct rulesforge_plugin_s {
  atomic_size_t contexts;
  atomic_size_t sessions;
  turbo_flow_provider_resource_requirement_v1_t resource_requirement;
} rulesforge_plugin_t;

typedef struct rulesforge_result_context_s {
  rulesforge_plugin_t *plugin;
  ruleforge_knowledge_base_t knowledge_base;
  char fact_type[128];
  int max_rules;
} rulesforge_result_context_t;

typedef struct rulesforge_session_s {
  rulesforge_result_context_t *context;
} rulesforge_session_t;

enum {
  RULESFORGE_MATERIALIZER_MAX_ENCODED_BYTES = 4096u,
  RULESFORGE_MATERIALIZER_WORKSPACE_BYTES = 4096u,
  RULESFORGE_MATERIALIZER_MAX_DEPTH = 8u,
  RULESFORGE_MATERIALIZER_MAX_ITEMS = 32u
};

static int rulesforge_databind_status(DataBindStatus status) {
  switch (status) {
  case DATA_BIND_OK:
    return SALTS_OK;
  case DATA_BIND_ERR_INVALID_ARG:
    return SALTS_EINVAL;
  case DATA_BIND_ERR_IO:
    return SALTS_EIO;
  case DATA_BIND_ERR_OOM:
    return SALTS_ENOMEM;
  case DATA_BIND_ERR_LIMIT:
  case DATA_BIND_ERR_BUFFER_TOO_SMALL:
    return SALTS_ENOSPC;
  case DATA_BIND_ERR_CANCELED:
    return SALTS_ECANCELED;
  case DATA_BIND_ERR_PARSE:
  case DATA_BIND_ERR_SCHEMA:
  case DATA_BIND_ERR_TYPE_NOT_FOUND:
  case DATA_BIND_ERR_TYPE_MISMATCH:
  case DATA_BIND_ERR_RUNTIME:
  default:
    return SALTS_EPROTO;
  }
}

static int rulesforge_materialize_applicant_json(
    void *ctx, const turbo_flow_plugin_materializer_input_v1_t *input,
    void *native_out, size_t native_capacity) {
  unsigned char workspace[RULESFORGE_MATERIALIZER_WORKSPACE_BYTES];
  DataBindNativeOptions options = DATA_BIND_NATIVE_OPTIONS_INIT;
  DataBindNativeDiagnostic diagnostic = DATA_BIND_NATIVE_DIAGNOSTIC_INIT;
  DataBindFormatReader reader = DATA_BIND_FORMAT_READER_INIT;
  DataBindError reader_error = DATA_BIND_ERROR_INIT;
  const DataBindFormatProvider *provider;
  DataBindStatus status;
  DataBindStatus close_status;
  (void)ctx;

  if (!input || input->size != sizeof(*input) ||
      input->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      input->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !input->data || input->data_size == 0u ||
      input->data_size > RULESFORGE_MATERIALIZER_MAX_ENCODED_BYTES ||
      !native_out || native_capacity != sizeof(turbo_flow_rulesforge_applicant))
    return input && input->data_size > RULESFORGE_MATERIALIZER_MAX_ENCODED_BYTES
               ? SALTS_ENOSPC
               : SALTS_EINVAL;

  options.workspace = workspace;
  options.workspace_bytes = sizeof(workspace);
  options.max_depth = RULESFORGE_MATERIALIZER_MAX_DEPTH;
  options.max_items = RULESFORGE_MATERIALIZER_MAX_ITEMS;
  options.max_owned_bytes = 0u;

  status = data_bind_native_init(
      &options, &turbo_flow_rulesforge_applicant_data,
      native_out, native_capacity, &diagnostic);
  if (status != DATA_BIND_OK) return rulesforge_databind_status(status);

  provider = data_bind_json_format_provider();
  if (!provider) return SALTS_ENOTSUP;
  status = data_bind_format_reader_open(
      provider, (const char *)input->data, input->data_size,
      options.max_depth, &reader, &reader_error);
  if (status != DATA_BIND_OK) return rulesforge_databind_status(status);

  status = data_bind_native_decode(
      &options, &turbo_flow_rulesforge_applicant_data, reader.reader,
      native_out, native_capacity, &diagnostic);
  close_status = data_bind_format_reader_close(&reader);
  if (status != DATA_BIND_OK) return rulesforge_databind_status(status);
  return rulesforge_databind_status(close_status);
}

static int rulesforge_status(ruleforge_status_t status) {
  switch (status) {
  case RULES_FORGE_OK:
    return SALTS_OK;
  case RULES_FORGE_ERROR_INVALID_ARGUMENT:
    return SALTS_EINVAL;
  case RULES_FORGE_ERROR_MEMORY_ALLOCATION:
    return SALTS_ENOMEM;
  case RULES_FORGE_ERROR_RESOURCE_LIMIT:
    return SALTS_ENOSPC;
  case RULES_FORGE_ERROR_COMPILATION_FAILED:
  case RULES_FORGE_ERROR_FACT_INSERTION_FAILED:
  case RULES_FORGE_ERROR_SESSION_INCONSISTENT:
  case RULES_FORGE_STATUS_END_OF_STREAM:
    return SALTS_EPROTO;
  default:
    return SALTS_EIO;
  }
}

static int rulesforge_error(turbo_flow_plugin_operation_error_v3_t *error, uint32_t phase,
                            ruleforge_status_t engine_status, const char *message) {
  int status = rulesforge_status(engine_status);
  if (error && error->size == sizeof(*error) &&
      error->abi_major == TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR &&
      error->abi_minor == TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) {
    error->status = status;
    error->engine_status = (int64_t)engine_status;
    error->phase = phase;
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "RulesForge operation failed");
  }
  return status;
}

static int rulesforge_resource(const turbo_flow_plugin_operation_request_v3_t *request,
                               turbo_flow_rulesforge_resource_view_t *view) {
  const turbo_flow_provider_resource_view_v1_t *resource;
  turbo_flow_rulesforge_resource *handle;
  if (!request || request->size != sizeof(*request) ||
      request->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      request->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !(resource = request->resource) || resource->size != sizeof(*resource) ||
      !cmeta_interface_desc_equal(resource->interface_desc,
                                  turbo_flow_rulesforge_resource_interface()) ||
      !resource->interface_value || !view)
    return SALTS_EINVAL;
  handle = resource->interface_value;
  if (!turbo_flow_rulesforge_resource_valid(handle) ||
      !turbo_flow_rulesforge_resource_has(handle, TURBO_FLOW_RULESFORGE_RESOURCE_IMMUTABLE))
    return SALTS_ENOTSUP;
  int rc = turbo_flow_rulesforge_resource_snapshot(handle, view);
  if (rc != SALTS_OK) return rc;
  if (view->size != sizeof(*view) || !view->knowledge_base || !view->fact_type ||
      !view->fact_type[0] || strlen(view->fact_type) >= 128u)
    return SALTS_EINVAL;
  int exists = 0;
  ruleforge_status_t status =
      ruleforge_kb_has_schema_type(view->knowledge_base, view->fact_type, &exists);
  if (status != RULES_FORGE_OK) return rulesforge_status(status);
  return exists ? SALTS_OK : SALTS_EPROTO;
}

static int rulesforge_preflight(void *factory_ctx,
                                const turbo_flow_plugin_operation_request_v3_t *request,
                                turbo_flow_plugin_operation_error_v3_t *error) {
  turbo_flow_rulesforge_resource_view_t view = TURBO_FLOW_RULESFORGE_RESOURCE_VIEW_INIT;
  (void)error;
  if (!factory_ctx || !request || !request->operation_name ||
      strcmp(request->operation_name, TURBO_FLOW_RULESFORGE_OPERATION) != 0)
    return SALTS_EINVAL;
  if (request->limits.max_steps < 2u || request->limits.max_steps > (uint32_t)INT_MAX)
    return SALTS_ENOSPC;
  return rulesforge_resource(request, &view);
}

static int rulesforge_result_context_create(
    void *factory_ctx, const turbo_flow_plugin_operation_request_v3_t *request, void **context_out,
    turbo_flow_plugin_operation_error_v3_t *error) {
  rulesforge_plugin_t *plugin = factory_ctx;
  rulesforge_result_context_t *context;
  turbo_flow_rulesforge_resource_view_t view = TURBO_FLOW_RULESFORGE_RESOURCE_VIEW_INIT;
  (void)error;
  if (context_out) *context_out = NULL;
  if (!plugin || !context_out) return SALTS_EINVAL;
  int rc = rulesforge_resource(request, &view);
  if (rc != SALTS_OK) return rc;
  context = calloc(1u, sizeof(*context));
  if (!context) return SALTS_ENOMEM;
  context->plugin = plugin;
  context->knowledge_base = view.knowledge_base;
  context->max_rules = (int)request->limits.max_steps - 1;
  memcpy(context->fact_type, view.fact_type, strlen(view.fact_type) + 1u);
  atomic_fetch_add_explicit(&plugin->contexts, (size_t)1u, memory_order_relaxed);
  *context_out = context;
  return SALTS_OK;
}

static int rulesforge_session_create(
    void *factory_ctx, const turbo_flow_plugin_operation_request_v3_t *request,
    void *result_context, void **session_out, turbo_flow_plugin_operation_error_v3_t *error) {
  rulesforge_session_t *session;
  rulesforge_result_context_t *context = (rulesforge_result_context_t *)result_context;
  (void)factory_ctx;
  (void)request;
  (void)error;
  if (session_out) *session_out = NULL;
  if (!context || !context->plugin || !context->knowledge_base || !session_out)
    return SALTS_EINVAL;
  session = (rulesforge_session_t *)calloc(1u, sizeof(*session));
  if (!session) return SALTS_ENOMEM;
  session->context = context;
  atomic_fetch_add_explicit(&context->plugin->sessions, (size_t)1u, memory_order_relaxed);
  *session_out = session;
  return SALTS_OK;
}

static int rulesforge_execute(void *session_ptr,
                              const turbo_flow_plugin_operation_input_v3_t *input,
                              const turbo_flow_plugin_operation_budget_v3_t *budget,
                              void **result_out,
                              turbo_flow_plugin_operation_error_v3_t *error) {
  rulesforge_session_t *binding = (rulesforge_session_t *)session_ptr;
  rulesforge_result_context_t *context;
  const turbo_flow_rulesforge_applicant *applicant;
  turbo_flow_rulesforge_decision *decision = NULL;
  ruleforge_stateful_session_t session = NULL;
  ruleforge_status_t status;
  char json[96];
  int fired = 0;
  int count;
  int rc;

  if (result_out) *result_out = NULL;
  if (!binding || !(context = binding->context) || !input || !budget || !budget->charge ||
      !result_out || input->size != sizeof(*input) ||
      input->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      input->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      input->bytes != sizeof(turbo_flow_rulesforge_applicant) || !input->value)
    return SALTS_EINVAL;

  rc = budget->charge(budget->ctx, 1u);
  if (rc != SALTS_OK) return rc;

  applicant = (const turbo_flow_rulesforge_applicant *)input->value;
  count = snprintf(json, sizeof(json), "{\"age\":%d}", applicant->age);
  if (count < 0 || (size_t)count >= sizeof(json)) return SALTS_ERANGE;

  status = ruleforge_session_create(context->knowledge_base, &session);
  if (status == RULES_FORGE_OK)
    status = ruleforge_session_add_fact_json(session, context->fact_type, json, NULL);
  if (status == RULES_FORGE_OK)
    status = ruleforge_session_fire_all_rules(session, context->max_rules, &fired);

  if (status == RULES_FORGE_OK && fired > 0) {
    rc = budget->charge(budget->ctx, (uint32_t)fired);
    if (rc != SALTS_OK) status = RULES_FORGE_ERROR_RESOURCE_LIMIT;
  }

  if (session) {
    ruleforge_status_t release_status = ruleforge_session_destroy(session);
    if (status == RULES_FORGE_OK && release_status != RULES_FORGE_OK) status = release_status;
  }
  if (status != RULES_FORGE_OK)
    return rulesforge_error(error, TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE, status,
                            ruleforge_get_last_error_message());

  decision = (turbo_flow_rulesforge_decision *)malloc(sizeof(*decision));
  if (!decision) return SALTS_ENOMEM;
  decision->matched = fired > 0 ? 1 : 0;
  decision->fired = fired;
  *result_out = decision;
  return SALTS_OK;
}

static int rulesforge_clone_result(const void *value, void *ctx, void **out) {
  turbo_flow_rulesforge_decision *copy;
  (void)ctx;
  if (!value || !out) return SALTS_EINVAL;
  *out = NULL;
  copy = (turbo_flow_rulesforge_decision *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const turbo_flow_rulesforge_decision *)value;
  *out = copy;
  return SALTS_OK;
}

static void rulesforge_destroy_result(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static int rulesforge_release_session(void *session_ptr) {
  rulesforge_session_t *session = (rulesforge_session_t *)session_ptr;
  if (!session || !session->context || !session->context->plugin) return SALTS_EINVAL;
  atomic_fetch_sub_explicit(&session->context->plugin->sessions, (size_t)1u, memory_order_relaxed);
  free(session);
  return SALTS_OK;
}

static int rulesforge_release_result_context(void *context_ptr) {
  rulesforge_result_context_t *context = (rulesforge_result_context_t *)context_ptr;
  if (!context || !context->plugin || !context->knowledge_base) return SALTS_EINVAL;
  atomic_fetch_sub_explicit(&context->plugin->contexts, (size_t)1u, memory_order_relaxed);
  free(context);
  return SALTS_OK;
}

static int rulesforge_plugin_load(const turbo_flow_plugin_host_v1_t *host, void **plugin_out) {
  rulesforge_plugin_t *plugin;
  if (plugin_out) *plugin_out = NULL;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !plugin_out)
    return SALTS_EINVAL;
  plugin = (rulesforge_plugin_t *)calloc(1u, sizeof(*plugin));
  if (!plugin) return SALTS_ENOMEM;
  atomic_init(&plugin->contexts, 0u);
  atomic_init(&plugin->sessions, 0u);
  plugin->resource_requirement = (turbo_flow_provider_resource_requirement_v1_t){
      sizeof(turbo_flow_provider_resource_requirement_v1_t),
      TURBO_FLOW_RULESFORGE_RESOURCE_CONTRACT_ID, TURBO_FLOW_RULESFORGE_RESOURCE_CONTRACT_VERSION,
      TURBO_FLOW_RULESFORGE_RESOURCE_IMMUTABLE, turbo_flow_rulesforge_resource_interface()};
  *plugin_out = plugin;
  return SALTS_OK;
}

static int rulesforge_plugin_register(
    void *plugin_ptr, const turbo_flow_plugin_registration_v1_t *registration) {
  rulesforge_plugin_t *plugin = (rulesforge_plugin_t *)plugin_ptr;
  turbo_flow_plugin_schema_v1_t input_schema = TURBO_FLOW_PLUGIN_SCHEMA_V1_INIT;
  turbo_flow_plugin_schema_v1_t output_schema = TURBO_FLOW_PLUGIN_SCHEMA_V1_INIT;
  turbo_flow_plugin_materializer_v1_t materializer =
      TURBO_FLOW_PLUGIN_MATERIALIZER_V1_INIT;
  turbo_flow_plugin_operation_v3_t operation;
  int rc;
  if (!plugin || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_schema || !registration->add_materializer ||
      !registration->add_operation)
    return SALTS_EINVAL;

  input_schema.schema_version = TURBO_FLOW_RULESFORGE_SCHEMA_VERSION;
  input_schema.data = &turbo_flow_rulesforge_applicant_data;
  output_schema.schema_version = TURBO_FLOW_RULESFORGE_SCHEMA_VERSION;
  output_schema.data = &turbo_flow_rulesforge_decision_data;
  rc = registration->add_schema(registration->ctx, &input_schema);
  if (rc != SALTS_OK) return rc;
  rc = registration->add_schema(registration->ctx, &output_schema);
  if (rc != SALTS_OK) return rc;

  materializer.schema = turbo_flow_rulesforge_applicant_schema;
  materializer.data = &turbo_flow_rulesforge_applicant_data;
  materializer.max_encoded_bytes = RULESFORGE_MATERIALIZER_MAX_ENCODED_BYTES;
  materializer.native_bytes = sizeof(turbo_flow_rulesforge_applicant);
  materializer.threading = TURBO_FLOW_PLUGIN_MATERIALIZER_THREAD_SAFE;
  materializer.ownership = TURBO_FLOW_PLUGIN_MATERIALIZER_CALLER_BUFFER;
  materializer.ctx = plugin;
  materializer.materialize = rulesforge_materialize_applicant_json;
  rc = registration->add_materializer(registration->ctx, &materializer);
  if (rc != SALTS_OK) return rc;

  turbo_flow_plugin_operation_v3_init(&operation);
  operation.resource_requirement = &plugin->resource_requirement;
  operation.operation_name = TURBO_FLOW_RULESFORGE_OPERATION;
  operation.operation_version = 1u;
  operation.input.schema_version = TURBO_FLOW_RULESFORGE_SCHEMA_VERSION;
  operation.input.data = &turbo_flow_rulesforge_applicant_data;
  operation.input.projection = &turbo_flow_rulesforge_applicant_schema;
  operation.output.schema_version = TURBO_FLOW_RULESFORGE_SCHEMA_VERSION;
  operation.output.data = &turbo_flow_rulesforge_decision_data;
  operation.output.projection = &turbo_flow_rulesforge_decision_schema;
  operation.execution = TURBO_FLOW_PLUGIN_OPERATION_SYNC;
  operation.threading = TURBO_FLOW_PLUGIN_OPERATION_THREAD_SAFE;
  operation.cancellation = TURBO_FLOW_PLUGIN_OPERATION_CANCEL_NONE;
  operation.effects = TURBO_FLOW_PLUGIN_OPERATION_EFFECT_RESULT;
  operation.guarantees = TURBO_FLOW_PLUGIN_OPERATION_STEPS_CHARGED;
  operation.limits.max_inflight = 64u;
  operation.limits.max_input_bytes = sizeof(turbo_flow_rulesforge_applicant);
  operation.limits.max_result_bytes = sizeof(turbo_flow_rulesforge_decision);
  operation.limits.max_retained_bytes =
      64u * sizeof(turbo_flow_rulesforge_decision);
  operation.limits.max_steps = 1024u;
  operation.max_session_bytes = sizeof(rulesforge_session_t);
  operation.max_result_context_bytes = sizeof(rulesforge_result_context_t);
  operation.factory_ctx = plugin;
  operation.preflight = rulesforge_preflight;
  operation.create_result_context = rulesforge_result_context_create;
  operation.create_session = rulesforge_session_create;
  operation.vtable.execute = rulesforge_execute;
  operation.vtable.clone_result = rulesforge_clone_result;
  operation.vtable.destroy_result = rulesforge_destroy_result;
  operation.vtable.release_session = rulesforge_release_session;
  operation.vtable.release_result_context = rulesforge_release_result_context;
  return registration->add_operation(registration->ctx, &operation);
}

static int rulesforge_plugin_quiesce(void *plugin_ptr, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin_ptr ? SALTS_OK : SALTS_EINVAL;
}

static int rulesforge_plugin_shutdown(void *plugin_ptr) {
  rulesforge_plugin_t *plugin = (rulesforge_plugin_t *)plugin_ptr;
  if (!plugin) return SALTS_EINVAL;
  return atomic_load_explicit(&plugin->contexts, memory_order_relaxed) == 0u &&
                 atomic_load_explicit(&plugin->sessions, memory_order_relaxed) == 0u
             ? SALTS_OK
             : SALTS_EBUSY;
}

static void rulesforge_plugin_destroy(void *plugin_ptr) {
  rulesforge_plugin_t *plugin = (rulesforge_plugin_t *)plugin_ptr;
  if (!plugin) return;
  free(plugin);
}

static const turbo_flow_plugin_api_v1_t rulesforge_plugin_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    TURBO_FLOW_RULESFORGE_PLUGIN_ID,
    TURBO_FLOW_RULESFORGE_PLUGIN_VERSION,
    (turbo_flow_plugin_capabilities_t)(TURBO_FLOW_PLUGIN_CAP_SCHEMA |
                                       TURBO_FLOW_PLUGIN_CAP_OPERATION |
                                       TURBO_FLOW_PLUGIN_CAP_MATERIALIZER),
    rulesforge_plugin_load,
    rulesforge_plugin_register,
    rulesforge_plugin_quiesce,
    rulesforge_plugin_shutdown,
    rulesforge_plugin_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &rulesforge_plugin_api;
}
