#ifndef FLOW_DATABIND_INTERNAL_H
#define FLOW_DATABIND_INTERNAL_H

#include "flow_internal.h"

#include <data_bind_method_plan.h>
#include <data_bind_native_binding.h>

struct flow_databind_source_binding_s {
  int bound;
  flow_databind_transport_kind_t transport;
  DataBindFormat format;
  const void *transport_plan;
  tstr channel_name;
  tstr message_type;
  DataBindNativeTypeBinding native;
};

typedef struct flow_databind_channel_plan_s {
  const char *channel_name;
  const char *message_type;
  const char *data_stable_id;
  DataBindNativeTypeBinding native;
} flow_databind_channel_plan_t;

typedef DataBindStatus (*flow_databind_codec_factory_fn)(
    DataBind **out, DataBindError *error);
typedef DataBindStatus (*flow_databind_service_native_resolver_fn)(
    DataBindNativeTypeBinding *request_out,
    DataBindNativeTypeBinding *response_out,
    DataBindServiceNativeBinding *service_out,
    DataBindError *error);

struct flow_databind_service_binding_s {
  int bound;
  DataBindTransportKind transport;
  tstr service_name;
  tstr operation_name;
  flow_databind_codec_factory_fn codec_factory;
  DataBindNativeTypeBinding request;
  DataBindNativeTypeBinding response;
  DataBindServiceNativeBinding native;
  union {
    const DataBindHttpProjectionConfig *http;
    const DataBindRpcProjectionConfig *rpc;
  } projection;
};

typedef struct flow_databind_service_plan_s {
  uint32_t stage_index;
  DataBindTransportKind transport;
  const char *service_name;
  const char *operation_name;
  const cmeta_function_desc *function;
  const DataBindBindingPlan *binding_plan;
  union {
    DataBindHttpMethodPlan *http;
    DataBindRpcMethodPlan *rpc;
  } method;
} flow_databind_service_plan_t;

int flow_databind_service_bind(
    turbo_flow_t *flow, const char *stage_name,
    const flow_databind_service_binding_t *binding);

#endif /* FLOW_DATABIND_INTERNAL_H */
