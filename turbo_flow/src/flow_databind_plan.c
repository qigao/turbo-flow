#include "flow_internal.h"

#include <string.h>

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
  if (stage->databind_source.bound) return SALTS_EALREADY;

  /*
   * Multiple generated transport projections for one logical Channel must
   * resolve exactly the same format-neutral native binding. Transport format,
   * framing/pattern and delivery limits are intentionally not part of this
   * comparison.
   */
  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *other =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    if (!other || !other->databind_source.bound ||
        strcmp(other->databind_source.channel_name,
               binding->channel_name) != 0)
      continue;
    if (!flow_databind_binding_contract_equal(
            &other->databind_source, binding))
      return SALTS_EPROTO;
  }

  channel_name = tstr_dup(binding->channel_name);
  message_type = tstr_dup(binding->message_type);
  if (!channel_name || !message_type) {
    tstr_freep(&channel_name);
    tstr_freep(&message_type);
    return SALTS_ENOMEM;
  }

  stage->databind_source = *binding;
  stage->databind_source.channel_name = channel_name;
  stage->databind_source.message_type = message_type;
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

    if (!stage || !node) return SALTS_EPROTO;
    if (!stage->databind_source.bound) continue;
    if (!stage->is_source ||
        !flow_databind_source_binding_valid(&stage->databind_source))
      return SALTS_EPROTO;

    for (size_t i = 0u; i < vec_size(&plan->databind_channels); ++i) {
      const flow_databind_channel_plan_t *existing =
          (const flow_databind_channel_plan_t *)vec_at_const(
              &plan->databind_channels, i);
      if (!existing ||
          !flow_databind_text_equal(
              existing->channel_name,
              stage->databind_source.channel_name))
        continue;
      if (!flow_databind_channel_plan_matches(
              existing, &stage->databind_source))
        return SALTS_EPROTO;
      if (i > UINT32_MAX) return SALTS_ERANGE;
      contract_index = (uint32_t)i;
      break;
    }

    if (contract_index == FLOW_PLAN_INDEX_NONE) {
      flow_databind_channel_plan_t channel = {0};
      size_t next = vec_size(&plan->databind_channels);
      if (next > UINT32_MAX) return SALTS_ERANGE;
      channel.channel_name = stage->databind_source.channel_name;
      channel.message_type = stage->databind_source.message_type;
      channel.data_stable_id = stage->databind_source.native.data->stable_id;
      channel.native = stage->databind_source.native;
      rc = turbo_flow_stl_error(
          vec_push(&plan->databind_channels, &channel));
      if (rc != SALTS_OK) return rc;
      contract_index = (uint32_t)next;
    }

    node->databind_channel_index = contract_index;
    node->databind_transport = stage->databind_source.transport;
    node->databind_format = stage->databind_source.format;
    node->databind_transport_plan = stage->databind_source.transport_plan;
  }

  return SALTS_OK;
}
