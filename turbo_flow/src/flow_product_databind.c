#include "turbo_flow_databind.h"

#include "flow_databind_internal.h"

#include "salts_error.h"

static int flow_product_databind_status(DataBindStatus status) {
  switch (status) {
    case DATA_BIND_OK: return SALTS_OK;
    case DATA_BIND_ERR_INVALID_ARG: return SALTS_EINVAL;
    case DATA_BIND_ERR_OOM: return SALTS_ENOMEM;
    case DATA_BIND_ERR_LIMIT:
    case DATA_BIND_ERR_BUFFER_TOO_SMALL:
      return SALTS_ENOSPC;
    case DATA_BIND_ERR_CANCELED:
      return SALTS_ECANCELED;
    case DATA_BIND_ERR_IO:
    case DATA_BIND_ERR_PARSE:
    case DATA_BIND_ERR_SCHEMA:
    case DATA_BIND_ERR_TYPE_NOT_FOUND:
    case DATA_BIND_ERR_TYPE_MISMATCH:
    case DATA_BIND_ERR_RUNTIME:
    case DATA_BIND_ERR_VALIDATION:
      return SALTS_EPROTO;
  }
  return SALTS_EPROTO;
}

typedef DataBindStatus (*flow_product_databind_resolver_fn)(
    DataBindNativeTypeBinding *out, DataBindError *error);

static int flow_product_databind_bind(
    turbo_flow_t *flow, const char *stage_name,
    flow_databind_transport_kind_t transport,
    DataBindFormat format, const void *transport_plan,
    const char *channel_name, const char *message_type,
    flow_product_databind_resolver_fn resolver) {
  DataBindNativeTypeBinding native = {0};
  DataBindError error = DATA_BIND_ERROR_INIT;
  flow_databind_source_binding_t binding = {0};
  DataBindStatus status;

  if (!resolver) return SALTS_EINVAL;
  status = resolver(&native, &error);
  if (status != DATA_BIND_OK)
    return flow_product_databind_status(status);

  binding.bound = 1;
  binding.transport = transport;
  binding.format = format;
  binding.transport_plan = transport_plan;
  binding.channel_name = (tstr)channel_name;
  binding.message_type = (tstr)message_type;
  binding.native = native;
  return flow_databind_source_bind(flow, stage_name, &binding);
}

int turbo_flow_product_bind_databind_socket_source(
    turbo_flow_t *flow, const char *stage_name,
    const DataBindSocketPlan *plan) {
  if (!plan || plan->size != sizeof(*plan) ||
      plan->abi_version != DATA_BIND_SOCKET_PLAN_ABI_VERSION ||
      !plan->channel_name || !plan->channel_name[0] ||
      !plan->message_type || !plan->message_type[0] ||
      !plan->native_binding)
    return SALTS_EINVAL;

  return flow_product_databind_bind(
      flow, stage_name, FLOW_DATABIND_TRANSPORT_SOCKET,
      plan->format, plan, plan->channel_name, plan->message_type,
      plan->native_binding);
}

int turbo_flow_product_bind_databind_flowmq_source(
    turbo_flow_t *flow, const char *stage_name,
    const DataBindFlowMQChannelPlan *plan) {
  if (!plan || plan->size != sizeof(*plan) ||
      plan->abi_version != DATA_BIND_FLOWMQ_CHANNEL_PLAN_ABI_VERSION ||
      !plan->channel_name || !plan->channel_name[0] ||
      !plan->message_type || !plan->message_type[0] ||
      !plan->native_binding)
    return SALTS_EINVAL;

  /*
   * Socket and FlowMQ generated resolvers have the same public
   * DataBindNativeTypeBinding ABI. Keep the generic bridge local to Product;
   * Graph sees only the resolved transport-neutral binding.
   */
  return flow_product_databind_bind(
      flow, stage_name, FLOW_DATABIND_TRANSPORT_FLOWMQ,
      plan->format, plan, plan->channel_name, plan->message_type,
      plan->native_binding);
}


static int flow_product_databind_service_bind(
    turbo_flow_t *flow, const char *stage_name,
    const char *service_name, const char *operation_name,
    DataBindTransportKind transport,
    flow_databind_codec_factory_fn codec_factory,
    flow_databind_service_native_resolver_fn native_resolver,
    const void *projection) {
  DataBindNativeTypeBinding request = {0};
  DataBindNativeTypeBinding response = {0};
  DataBindServiceNativeBinding native = {0};
  DataBindError error = DATA_BIND_ERROR_INIT;
  flow_databind_service_binding_t binding = {0};
  DataBindStatus status;

  if (!flow || !stage_name || !stage_name[0] ||
      !service_name || !service_name[0] ||
      !operation_name || !operation_name[0] ||
      !codec_factory || !native_resolver || !projection)
    return SALTS_EINVAL;

  status = native_resolver(&request, &response, &native, &error);
  if (status != DATA_BIND_OK)
    return flow_product_databind_status(status);

  binding.bound = 1;
  binding.transport = transport;
  binding.service_name = (tstr)service_name;
  binding.operation_name = (tstr)operation_name;
  binding.codec_factory = codec_factory;
  binding.request = request;
  binding.response = response;
  binding.native = native;
  binding.native.request = &binding.request;
  binding.native.response = &binding.response;
  if (transport == DATA_BIND_TRANSPORT_HTTP)
    binding.projection.http =
        (const DataBindHttpProjectionConfig *)projection;
  else
    binding.projection.rpc =
        (const DataBindRpcProjectionConfig *)projection;
  return flow_databind_service_bind(flow, stage_name, &binding);
}

int turbo_flow_product_bind_databind_http_service(
    turbo_flow_t *flow, const char *stage_name,
    const char *service_name, const char *operation_name,
    turbo_flow_databind_codec_factory_fn codec_factory,
    turbo_flow_databind_service_native_resolver_fn native_resolver,
    const DataBindHttpProjectionArtifact *projection_artifact) {
  const DataBindHttpProjectionConfig *projection;
  if (!projection_artifact ||
      projection_artifact->size != sizeof(*projection_artifact) ||
      projection_artifact->abi_version != DATA_BIND_METHOD_PLAN_ABI_VERSION)
    return SALTS_EINVAL;
  projection = data_bind_http_projection_artifact_find(
      projection_artifact, service_name, operation_name);
  if (!projection) return SALTS_ENOENT;
  return flow_product_databind_service_bind(
      flow, stage_name, service_name, operation_name,
      DATA_BIND_TRANSPORT_HTTP, codec_factory,
      native_resolver, projection);
}

int turbo_flow_product_bind_databind_rpc_service(
    turbo_flow_t *flow, const char *stage_name,
    const char *service_name, const char *operation_name,
    turbo_flow_databind_codec_factory_fn codec_factory,
    turbo_flow_databind_service_native_resolver_fn native_resolver,
    const DataBindRpcProjectionArtifact *projection_artifact) {
  const DataBindRpcProjectionConfig *projection;
  if (!projection_artifact ||
      projection_artifact->size != sizeof(*projection_artifact) ||
      projection_artifact->abi_version != DATA_BIND_METHOD_PLAN_ABI_VERSION)
    return SALTS_EINVAL;
  projection = data_bind_rpc_projection_artifact_find(
      projection_artifact, service_name, operation_name);
  if (!projection) return SALTS_ENOENT;
  return flow_product_databind_service_bind(
      flow, stage_name, service_name, operation_name,
      DATA_BIND_TRANSPORT_RPC, codec_factory,
      native_resolver, projection);
}


static int flow_product_databind_execution_plan_available(
    const turbo_flow_t *flow) {
  return flow && flow->compiled_plan.sealed &&
         (flow->state == TURBO_FLOW_STATE_COMPILED ||
          flow->state == TURBO_FLOW_STATE_STARTED ||
          flow->state == TURBO_FLOW_STATE_STOPPED);
}

size_t turbo_flow_execution_plan_databind_service_count(
    const turbo_flow_t *flow) {
  return flow_product_databind_execution_plan_available(flow)
             ? vec_size(&flow->compiled_plan.databind_services)
             : 0u;
}

int turbo_flow_execution_plan_databind_service_at(
    const turbo_flow_t *flow, size_t index,
    turbo_flow_databind_service_plan_view_t *out) {
  const flow_databind_service_plan_t *service;
  const cmeta_function_desc *function;

  if (!flow_product_databind_execution_plan_available(flow) || !out ||
      out->size != sizeof(*out) ||
      out->version != TURBO_FLOW_DATABIND_SERVICE_PLAN_API_VERSION)
    return SALTS_EINVAL;

  service = (const flow_databind_service_plan_t *)vec_at_const(
      &flow->compiled_plan.databind_services, index);
  if (!service || !service->binding_plan) return SALTS_ENOENT;

  function = data_bind_binding_plan_function(service->binding_plan);
  if (!function || !service->function ||
      !cmeta_function_desc_equal(function, service->function))
    return SALTS_EPROTO;

  *out = (turbo_flow_databind_service_plan_view_t)
      TURBO_FLOW_DATABIND_SERVICE_PLAN_VIEW_INIT;
  out->stage_index = service->stage_index;
  out->transport = service->transport;
  out->service_name = service->service_name;
  out->operation_name = service->operation_name;
  out->function_name = function->name;
  out->ingress_count =
      data_bind_binding_plan_ingress_count(service->binding_plan);
  out->egress_count =
      data_bind_binding_plan_egress_count(service->binding_plan);
  out->error_count =
      data_bind_binding_plan_error_count(service->binding_plan);
  return SALTS_OK;
}
