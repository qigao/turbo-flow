#include "turbo_flow_databind.h"

#include "flow_internal.h"

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

static int flow_product_databind_bind(
    turbo_flow_t *flow, const char *stage_name,
    flow_databind_transport_kind_t transport,
    DataBindFormat format, const void *transport_plan,
    const char *channel_name, const char *message_type,
    DataBindSocketNativeBindingResolverFn resolver) {
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
      (DataBindSocketNativeBindingResolverFn)plan->native_binding);
}
