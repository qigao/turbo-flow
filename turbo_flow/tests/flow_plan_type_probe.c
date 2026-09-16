#include <cmeta/cmeta.h>

#include "turbo_flow.h"

static const cmeta_type_identity FLOW_PROBE_MESSAGE_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turbo.flow.Message");
static const cmeta_type_desc FLOW_PROBE_MESSAGE_TYPE = {
    .name = "turbo_flow_msg_t",
    .size = sizeof(turbo_flow_msg_t),
    .align = _Alignof(turbo_flow_msg_t),
    .kind = CMETA_T_OBJECT,
    .identity = &FLOW_PROBE_MESSAGE_IDENTITY};

static const cmeta_type_identity FLOW_PROBE_OPERATION_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turbo.flow.Operation");
static const cmeta_type_desc FLOW_PROBE_OPERATION_TYPE = {
    .name = "turbo_flow_operation_descriptor_t",
    .size = sizeof(turbo_flow_operation_descriptor_t),
    .align = _Alignof(turbo_flow_operation_descriptor_t),
    .kind = CMETA_T_OBJECT,
    .identity = &FLOW_PROBE_OPERATION_IDENTITY};

const cmeta_type_desc *flow_plan_probe_message_type(void) {
  return &FLOW_PROBE_MESSAGE_TYPE;
}

const cmeta_type_desc *flow_plan_probe_operation_type(void) {
  return &FLOW_PROBE_OPERATION_TYPE;
}

int flow_plan_probe_stage_is_buffer_field(void) {
  turbo_flow_stage_plan_t stage = {0};
  stage.is_buffer = 1;
  return stage.is_buffer;
}

static const cmeta_type_identity FLOW_PROBE_DATA_MESSAGE_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turbo.flow.type.1.Message");
static const cmeta_type_desc FLOW_PROBE_DATA_MESSAGE_TYPE = {
    .name = "independent_data_message_descriptor",
    .size = sizeof(turbo_flow_msg_t),
    .align = _Alignof(turbo_flow_msg_t),
    .kind = CMETA_T_OBJECT,
    .identity = &FLOW_PROBE_DATA_MESSAGE_IDENTITY};

const cmeta_type_desc *flow_plan_probe_data_message_type(void) {
  return &FLOW_PROBE_DATA_MESSAGE_TYPE;
}
