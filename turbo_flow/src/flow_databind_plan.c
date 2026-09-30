#include "flow_databind_internal.h"

#include <stdlib.h>
#include <string.h>

void flow_databind_stage_binding_clear(flow_stage_plan_impl_t *stage) {
  if (!stage) return;
  if (stage->databind_source) {
    flow_databind_source_binding_t *binding = stage->databind_source;
    tstr_freep(&binding->channel_name);
    tstr_freep(&binding->message_type);
    memset(binding, 0, sizeof(*binding));
    free(binding);
    stage->databind_source = NULL;
  }
  if (stage->databind_service) {
    flow_databind_service_binding_t *binding = stage->databind_service;
    tstr_freep(&binding->service_name);
    tstr_freep(&binding->operation_name);
    memset(binding, 0, sizeof(*binding));
    free(binding);
    stage->databind_service = NULL;
  }
}

int flow_databind_channels_init(vec_t *channels) {
  if (!channels) return SALTS_EINVAL;
  return turbo_flow_stl_error(
      vec_init_bytes(channels, sizeof(flow_databind_channel_plan_t),
                     _Alignof(turbo_flow_max_align_t), SIZE_MAX));
}

void flow_databind_channels_destroy(vec_t *channels) {
  if (!channels) return;
  vec_destroy(channels);
}

int flow_databind_channels_verify(const vec_t *channels) {
  if (!channels) return SALTS_EINVAL;
  for (size_t channel_index = 0u;
       channel_index < vec_size(channels); ++channel_index) {
    const flow_databind_channel_plan_t *channel =
        (const flow_databind_channel_plan_t *)vec_at_const(
            channels, channel_index);
    if (!channel || !channel->channel_name || !channel->message_type ||
        !channel->data_stable_id || !channel->native.idl_type_name ||
        !channel->native.data ||
        strcmp(channel->message_type, channel->native.idl_type_name) != 0 ||
        !cmeta_data_desc_valid(channel->native.data)) {
      return SALTS_EPROTO;
    }
  }
  return SALTS_OK;
}

static int flow_databind_text_equal(const char *left, const char *right) {
  return left && right && strcmp(left, right) == 0;
}

static int flow_databind_state_list_valid(
    const DataBindNativeStateBinding *items, size_t count) {
  if ((count != 0u) != (items != NULL)) return 0;
  for (size_t i = 0u; i < count; ++i) {
    const DataBindNativeStateBinding *item = &items[i];
    if (item->size != sizeof(*item) || !item->field_name ||
        !item->field_name[0] || item->bit >= 8u)
      return 0;
    for (size_t j = 0u; j < i; ++j) {
      if (strcmp(item->field_name, items[j].field_name) == 0)
        return 0;
    }
  }
  return 1;
}

static int flow_databind_native_valid(
    const DataBindNativeTypeBinding *native) {
  return native &&
         native->size == sizeof(*native) &&
         native->abi_version == DATA_BIND_NATIVE_BINDING_ABI_VERSION &&
         native->idl_type_name && native->idl_type_name[0] &&
         native->data && cmeta_data_desc_valid(native->data) &&
         native->data->stable_id && native->data->stable_id[0] &&
         native->data->storage_type &&
         cmeta_type_desc_valid(native->data->storage_type) &&
         flow_databind_state_list_valid(
             native->presence, native->presence_count) &&
         flow_databind_state_list_valid(
             native->nulls, native->null_count);
}

static int flow_databind_state_equal(
    const DataBindNativeStateBinding *left,
    const DataBindNativeStateBinding *right) {
  return left && right &&
         left->size == sizeof(*left) &&
         right->size == sizeof(*right) &&
         flow_databind_text_equal(left->field_name, right->field_name) &&
         left->byte_offset == right->byte_offset &&
         left->bit == right->bit;
}

static int flow_databind_native_equal(
    const DataBindNativeTypeBinding *left,
    const DataBindNativeTypeBinding *right) {
  if (!flow_databind_native_valid(left) ||
      !flow_databind_native_valid(right) ||
      !flow_databind_text_equal(left->idl_type_name, right->idl_type_name) ||
      !cmeta_data_desc_equal(left->data, right->data) ||
      left->presence_count != right->presence_count ||
      left->null_count != right->null_count)
    return 0;

  for (size_t i = 0u; i < left->presence_count; ++i) {
    if (!flow_databind_state_equal(&left->presence[i], &right->presence[i]))
      return 0;
  }
  for (size_t i = 0u; i < left->null_count; ++i) {
    if (!flow_databind_state_equal(&left->nulls[i], &right->nulls[i]))
      return 0;
  }
  return 1;
}

static int flow_databind_source_binding_valid(
    const flow_databind_source_binding_t *binding) {
  return binding && binding->bound &&
         (binding->transport == FLOW_DATABIND_TRANSPORT_SOCKET ||
          binding->transport == FLOW_DATABIND_TRANSPORT_FLOWMQ) &&
         binding->format >= DATA_BIND_FORMAT_BINARY &&
         binding->format <= DATA_BIND_FORMAT_XML &&
         binding->transport_plan &&
         binding->channel_name && binding->channel_name[0] &&
         binding->message_type && binding->message_type[0] &&
         flow_databind_native_valid(&binding->native) &&
         strcmp(binding->message_type, binding->native.idl_type_name) == 0;
}

static int flow_databind_binding_contract_equal(
    const flow_databind_source_binding_t *left,
    const flow_databind_source_binding_t *right) {
  return flow_databind_source_binding_valid(left) &&
         flow_databind_source_binding_valid(right) &&
         flow_databind_text_equal(left->channel_name, right->channel_name) &&
         flow_databind_text_equal(left->message_type, right->message_type) &&
         flow_databind_native_equal(&left->native, &right->native);
}

int flow_databind_source_bind(
    turbo_flow_t *flow, const char *stage_name,
    const flow_databind_source_binding_t *binding) {
  flow_stage_plan_impl_t *stage;
  tstr channel_name = NULL;
  tstr message_type = NULL;
  int stage_index;

  if (!flow || !stage_name || !stage_name[0] || !binding ||
      !binding->bound ||
      (binding->transport != FLOW_DATABIND_TRANSPORT_SOCKET &&
       binding->transport != FLOW_DATABIND_TRANSPORT_FLOWMQ) ||
      !binding->transport_plan ||
      !binding->channel_name || !binding->channel_name[0] ||
      !binding->message_type || !binding->message_type[0] ||
      turbo_flow_state(flow) != TURBO_FLOW_STATE_PARSED)
    return SALTS_EINVAL;
  if (!flow_databind_source_binding_valid(binding))
    return SALTS_EPROTO;

  stage_index = turbo_flow_find_stage(flow, stage_name);
  if (stage_index < 0) return SALTS_ENOENT;
  stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, (size_t)stage_index);
  if (!stage || !stage->is_source || stage->is_port || stage->is_buffer)
    return SALTS_ENOTSUP;
  if (stage->databind_source) return SALTS_EALREADY;

  /*
   * Multiple generated transport projections for one logical Channel must
   * resolve exactly the same format-neutral native binding. Transport format,
   * framing/pattern and delivery limits are intentionally not part of this
   * comparison.
   */
  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *other =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (!other || !other->databind_source ||
        strcmp(other->databind_source->channel_name,
               binding->channel_name) != 0)
      continue;
    if (!flow_databind_binding_contract_equal(
            other->databind_source, binding))
      return SALTS_EPROTO;
  }

  channel_name = tstr_dup(binding->channel_name);
  message_type = tstr_dup(binding->message_type);
  if (!channel_name || !message_type) {
    tstr_freep(&channel_name);
    tstr_freep(&message_type);
    return SALTS_ENOMEM;
  }

  {
    flow_databind_source_binding_t *owned =
        (flow_databind_source_binding_t *)calloc(1, sizeof(*owned));
    if (!owned) {
      tstr_freep(&channel_name);
      tstr_freep(&message_type);
      return SALTS_ENOMEM;
    }
    *owned = *binding;
    owned->channel_name = channel_name;
    owned->message_type = message_type;
    stage->databind_source = owned;
  }
  return SALTS_OK;
}

static int flow_databind_channel_plan_matches(
    const flow_databind_channel_plan_t *channel,
    const flow_databind_source_binding_t *binding) {
  if (!channel || !binding ||
      !flow_databind_text_equal(
          channel->channel_name, binding->channel_name))
    return 0;
  return flow_databind_text_equal(
             channel->message_type, binding->message_type) &&
         flow_databind_text_equal(
             channel->data_stable_id, binding->native.data->stable_id) &&
         flow_databind_native_equal(&channel->native, &binding->native);
}

int flow_plan_build_databind_channels(
    const turbo_flow_t *flow, flow_compiled_plan_t *plan) {
  const size_t stage_count = flow ? vec_size(&flow->stages) : 0u;
  int rc;

  if (!flow || !plan || plan->sealed ||
      vec_size(&plan->nodes) != stage_count ||
      !vec_empty(&plan->databind_channels))
    return SALTS_EINVAL;

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(
            &flow->stages, stage_index);
    flow_runtime_node_plan_t *node =
        (flow_runtime_node_plan_t *)vec_at(&plan->nodes, stage_index);
    uint32_t contract_index = FLOW_PLAN_INDEX_NONE;

    const flow_databind_source_binding_t *binding;
    if (!stage || !node) return SALTS_EPROTO;
    binding = stage->databind_source;
    if (!binding) continue;
    if (!stage->is_source ||
        !flow_databind_source_binding_valid(binding))
      return SALTS_EPROTO;

    for (size_t i = 0u; i < vec_size(&plan->databind_channels); ++i) {
      const flow_databind_channel_plan_t *existing =
          (const flow_databind_channel_plan_t *)vec_at_const(
              &plan->databind_channels, i);
      if (!existing ||
          !flow_databind_text_equal(
              existing->channel_name, binding->channel_name))
        continue;
      if (!flow_databind_channel_plan_matches(existing, binding))
        return SALTS_EPROTO;
      if (i > UINT32_MAX) return SALTS_ERANGE;
      contract_index = (uint32_t)i;
      break;
    }

    if (contract_index == FLOW_PLAN_INDEX_NONE) {
      flow_databind_channel_plan_t channel = {0};
      size_t next = vec_size(&plan->databind_channels);
      if (next > UINT32_MAX) return SALTS_ERANGE;
      channel.channel_name = binding->channel_name;
      channel.message_type = binding->message_type;
      channel.data_stable_id = binding->native.data->stable_id;
      channel.native = binding->native;
      rc = turbo_flow_stl_error(
          vec_push(&plan->databind_channels, &channel));
      if (rc != SALTS_OK) return rc;
      contract_index = (uint32_t)next;
    }

    node->databind_channel_index = contract_index;
    node->databind_transport = binding->transport;
    node->databind_format = (uint32_t)binding->format;
    node->databind_transport_plan = binding->transport_plan;
  }

  return SALTS_OK;
}


static int flow_databind_service_native_valid(
    const flow_databind_service_binding_t *binding) {
  const DataBindServiceNativeBinding *native;
  if (!binding) return 0;
  native = &binding->native;
  return binding->bound &&
         (binding->transport == DATA_BIND_TRANSPORT_HTTP ||
          binding->transport == DATA_BIND_TRANSPORT_RPC) &&
         binding->service_name && binding->service_name[0] &&
         binding->operation_name && binding->operation_name[0] &&
         binding->codec_factory &&
         flow_databind_native_valid(&binding->request) &&
         flow_databind_native_valid(&binding->response) &&
         native->size == sizeof(*native) &&
         native->abi_version == DATA_BIND_BINDING_PLAN_ABI_VERSION &&
         native->function && cmeta_function_desc_valid(native->function) &&
         native->request && native->response &&
         flow_databind_native_equal(native->request, &binding->request) &&
         flow_databind_native_equal(native->response, &binding->response) &&
         ((binding->transport == DATA_BIND_TRANSPORT_HTTP &&
           binding->projection.http) ||
          (binding->transport == DATA_BIND_TRANSPORT_RPC &&
           binding->projection.rpc));
}

int flow_databind_service_bind(
    turbo_flow_t *flow, const char *stage_name,
    const flow_databind_service_binding_t *binding) {
  flow_stage_plan_impl_t *stage;
  flow_databind_service_binding_t *owned;
  int stage_index;

  if (!flow || !stage_name || !stage_name[0] || !binding ||
      turbo_flow_state(flow) != TURBO_FLOW_STATE_PARSED)
    return SALTS_EINVAL;
  if (!flow_databind_service_native_valid(binding)) return SALTS_EPROTO;

  stage_index = turbo_flow_find_stage(flow, stage_name);
  if (stage_index < 0) return SALTS_ENOENT;
  stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, (size_t)stage_index);
  if (!stage || stage->is_source || stage->is_port || stage->is_buffer ||
      !stage->operation_name || !stage->operation_name[0])
    return SALTS_ENOTSUP;
  if (!flow_find_operation_registration(flow, stage->operation_name))
    return SALTS_ENOENT;
  if (stage->databind_service) return SALTS_EALREADY;

  owned = (flow_databind_service_binding_t *)calloc(1, sizeof(*owned));
  if (!owned) return SALTS_ENOMEM;
  *owned = *binding;
  owned->service_name = tstr_dup(binding->service_name);
  owned->operation_name = tstr_dup(binding->operation_name);
  if (!owned->service_name || !owned->operation_name) {
    tstr_freep(&owned->service_name);
    tstr_freep(&owned->operation_name);
    free(owned);
    return SALTS_ENOMEM;
  }

  /*
   * Generated resolver output points at the caller-owned request/response
   * structs. Rebase those two pointers after copying the immutable overlay into
   * the parsed stage.
   */
  owned->native.request = &owned->request;
  owned->native.response = &owned->response;
  stage->databind_service = owned;
  return SALTS_OK;
}

int flow_databind_services_init(vec_t *services) {
  if (!services) return SALTS_EINVAL;
  return turbo_flow_stl_error(
      vec_init_bytes(services, sizeof(flow_databind_service_plan_t),
                     _Alignof(turbo_flow_max_align_t), SIZE_MAX));
}

void flow_databind_services_destroy(vec_t *services) {
  if (!services) return;
  for (size_t i = 0u; i < vec_size(services); ++i) {
    flow_databind_service_plan_t *service =
        (flow_databind_service_plan_t *)vec_at(services, i);
    if (!service) continue;
    if (service->transport == DATA_BIND_TRANSPORT_HTTP) {
      data_bind_http_method_plan_free(service->method.http);
    } else if (service->transport == DATA_BIND_TRANSPORT_RPC) {
      data_bind_rpc_method_plan_free(service->method.rpc);
    }
    memset(service, 0, sizeof(*service));
  }
  vec_destroy(services);
}

int flow_databind_services_verify(
    const vec_t *services, size_t stage_count) {
  if (!services) return SALTS_EINVAL;
  for (size_t i = 0u; i < vec_size(services); ++i) {
    const flow_databind_service_plan_t *service =
        (const flow_databind_service_plan_t *)vec_at_const(services, i);
    const DataBindBindingPlan *binding = NULL;
    const DataBindTransportPlan *transport_plan = NULL;
    DataBindTransportPlanInfo info = DATA_BIND_TRANSPORT_PLAN_INFO_INIT;
    if (!service || service->stage_index >= stage_count ||
        !service->service_name || !service->operation_name ||
        !service->function || !cmeta_function_desc_valid(service->function))
      return SALTS_EPROTO;

    if (service->transport == DATA_BIND_TRANSPORT_HTTP &&
        service->method.http) {
      binding = data_bind_http_method_plan_binding(service->method.http);
      transport_plan = data_bind_http_method_plan_transport(service->method.http);
    } else if (service->transport == DATA_BIND_TRANSPORT_RPC &&
               service->method.rpc) {
      binding = data_bind_rpc_method_plan_binding(service->method.rpc);
      transport_plan = data_bind_rpc_method_plan_transport(service->method.rpc);
    } else {
      return SALTS_EPROTO;
    }

    if (!binding || binding != service->binding_plan ||
        data_bind_binding_plan_function(binding) != service->function ||
        !transport_plan ||
        !data_bind_transport_plan_info(transport_plan, &info) ||
        !flow_databind_text_equal(info.service_name, service->service_name) ||
        !flow_databind_text_equal(info.operation_name, service->operation_name))
      return SALTS_EPROTO;
    if ((service->transport == DATA_BIND_TRANSPORT_HTTP &&
         info.kind != DATA_BIND_TRANSPORT_HTTP) ||
        (service->transport == DATA_BIND_TRANSPORT_RPC &&
         info.kind != DATA_BIND_TRANSPORT_RPC))
      return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int flow_plan_build_databind_services(
    const turbo_flow_t *flow, flow_compiled_plan_t *plan) {
  const size_t stage_count = flow ? vec_size(&flow->stages) : 0u;

  if (!flow || !plan || plan->sealed ||
      vec_size(&plan->nodes) != stage_count ||
      !vec_empty(&plan->databind_services))
    return SALTS_EINVAL;

  for (size_t stage_index = 0u; stage_index < stage_count; ++stage_index) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, stage_index);
    flow_runtime_node_plan_t *node =
        (flow_runtime_node_plan_t *)vec_at(&plan->nodes, stage_index);
    const flow_databind_service_binding_t *source;
    flow_databind_service_plan_t compiled = {0};
    DataBindBindingPlanDiagnostic diagnostic =
        DATA_BIND_BINDING_PLAN_DIAGNOSTIC_INIT;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = NULL;
    DataBindStatus status;

    if (!stage || !node) return SALTS_EPROTO;
    source = stage->databind_service;
    if (!source) continue;
    if (!flow_databind_service_native_valid(source) ||
        stage->is_source || stage->is_port || stage->is_buffer ||
        !stage->operation_resolved)
      return SALTS_EPROTO;
    if (stage_index > UINT32_MAX) return SALTS_ERANGE;

    status = source->codec_factory(&codec, &error);
    if (status != DATA_BIND_OK || !codec)
      return status == DATA_BIND_ERR_OOM ? SALTS_ENOMEM : SALTS_EPROTO;

    compiled.stage_index = (uint32_t)stage_index;
    compiled.transport = source->transport;
    compiled.service_name = source->service_name;
    compiled.operation_name = source->operation_name;
    compiled.function = source->native.function;

    if (source->transport == DATA_BIND_TRANSPORT_HTTP) {
      status = data_bind_http_method_plan_compile_service(
          codec, source->service_name, source->operation_name,
          source->projection.http, &source->native,
          &compiled.method.http, &diagnostic);
      if (status == DATA_BIND_OK && compiled.method.http)
        compiled.binding_plan =
            data_bind_http_method_plan_binding(compiled.method.http);
    } else {
      status = data_bind_rpc_method_plan_compile_service(
          codec, source->service_name, source->operation_name,
          source->projection.rpc, &source->native,
          &compiled.method.rpc, &diagnostic);
      if (status == DATA_BIND_OK && compiled.method.rpc)
        compiled.binding_plan =
            data_bind_rpc_method_plan_binding(compiled.method.rpc);
    }

    data_bind_free(codec);
    codec = NULL;
    if (status != DATA_BIND_OK || !compiled.binding_plan ||
        data_bind_binding_plan_function(compiled.binding_plan) !=
            source->native.function) {
      if (compiled.transport == DATA_BIND_TRANSPORT_HTTP)
        data_bind_http_method_plan_free(compiled.method.http);
      else
        data_bind_rpc_method_plan_free(compiled.method.rpc);
      return status == DATA_BIND_ERR_OOM ? SALTS_ENOMEM : SALTS_EPROTO;
    }

    {
      int rc = turbo_flow_stl_error(
          vec_push(&plan->databind_services, &compiled));
      if (rc != SALTS_OK) {
        if (compiled.transport == DATA_BIND_TRANSPORT_HTTP)
          data_bind_http_method_plan_free(compiled.method.http);
        else
          data_bind_rpc_method_plan_free(compiled.method.rpc);
        return rc;
      }
    }
    node->databind_service_index =
        (uint32_t)(vec_size(&plan->databind_services) - 1u);
  }
  return SALTS_OK;
}
