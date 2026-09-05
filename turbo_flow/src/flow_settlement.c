#include "flow_internal.h"

static SALTS_THREAD_LOCAL flow_stage_completion_t *flow_current_settlement;

static uint32_t flow_settlement_action_flag(turbo_flow_settlement_action_t action) {
  switch (action) {
  case TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE: return TURBO_FLOW_SETTLEMENT_COMPLETE;
  case TURBO_FLOW_SETTLEMENT_ACTION_RETRYABLE_FAILURE: return TURBO_FLOW_SETTLEMENT_RETRY;
  case TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE: return TURBO_FLOW_SETTLEMENT_REQUEUE;
  case TURBO_FLOW_SETTLEMENT_ACTION_DEAD_LETTER: return TURBO_FLOW_SETTLEMENT_DEAD_LETTER;
  case TURBO_FLOW_SETTLEMENT_ACTION_CANCELED: return TURBO_FLOW_SETTLEMENT_CANCELED;
  case TURBO_FLOW_SETTLEMENT_ACTION_ACKNOWLEDGE: return TURBO_FLOW_SETTLEMENT_ACKNOWLEDGE;
  default: return 0u;
  }
}

static int flow_settlement_result_valid(const turbo_flow_settlement_result_t *result) {
  if (!result || result->size < sizeof(*result) || flow_settlement_action_flag(result->action) == 0u)
    return 0;
  if ((result->action == TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE ||
       result->action == TURBO_FLOW_SETTLEMENT_ACTION_ACKNOWLEDGE) &&
      result->status != SALTS_OK) {
    return 0;
  }
  if (result->action != TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE &&
      result->action != TURBO_FLOW_SETTLEMENT_ACTION_ACKNOWLEDGE &&
      result->status == SALTS_OK) {
    return 0;
  }
  return 1;
}

flow_stage_completion_t *flow_settlement_scope_enter(flow_stage_completion_t *completion) {
  flow_stage_completion_t *previous = flow_current_settlement;
  flow_current_settlement = completion;
  return previous;
}

void flow_settlement_scope_leave(flow_stage_completion_t *previous) {
  flow_current_settlement = previous;
}

int turbo_flow_settlement_report(const turbo_flow_settlement_result_t *result) {
  if (!flow_current_settlement || !flow_settlement_result_valid(result)) return SALTS_EINVAL;
  if (flow_current_settlement->settlement_reported) {
    flow_current_settlement->settlement_duplicate = 1;
    return SALTS_EALREADY;
  }
  flow_current_settlement->settlement = *result;
  flow_current_settlement->settlement.size = sizeof(flow_current_settlement->settlement);
  flow_current_settlement->settlement_reported = 1;
  return SALTS_OK;
}

int flow_adapter_apply_settlement(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                                  turbo_flow_msg_t *msg, flow_stage_completion_t *completion,
                                  int callback_status) {
  const turbo_flow_operation_runtime_contract_t *runtime;
  const flow_adapter_registration_t *adapter;
  turbo_flow_settlement_result_t automatic = TURBO_FLOW_SETTLEMENT_RESULT_INIT;
  turbo_flow_settlement_result_t *result;
  uint32_t required;
  turbo_flow_stage_plan_t view;
  int rc;

  if (!flow || !stage || !msg || !completion) return SALTS_EINVAL;
  runtime = flow_stage_operation_runtime(flow, stage);
  if (!runtime || runtime->settlement == 0u) return callback_status;
  if ((runtime->settlement & ~TURBO_FLOW_SETTLEMENT_RETRY) == 0u) return callback_status;
  if (completion->settlement_duplicate) {
    return flow_set_error_keep_state(flow, SALTS_EALREADY, stage->line, stage->column,
                                     "stage reported settlement more than once");
  }
  if (!completion->settlement_reported) {
    if (callback_status != SALTS_OK ||
        (runtime->settlement & TURBO_FLOW_SETTLEMENT_COMPLETE) == 0u) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                       "stage did not report a required settlement result");
    }
    automatic.action = TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE;
    automatic.status = SALTS_OK;
    automatic.attempt = msg->execution_attempt ? msg->execution_attempt : 1u;
    automatic.message_id = msg->id;
    automatic.sequence = completion->entry.sequence;
    result = &automatic;
  } else {
    result = &completion->settlement;
    if (result->attempt == 0u) result->attempt = msg->execution_attempt ? msg->execution_attempt : 1u;
    if (result->message_id == 0u) result->message_id = msg->id;
    if (result->sequence == 0u) result->sequence = completion->entry.sequence;
  }
  required = flow_settlement_action_flag(result->action);
  if ((runtime->settlement & required) == 0u) {
    return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                     "stage reported a settlement action outside its contract");
  }
  if (callback_status != SALTS_OK && result->status != callback_status) {
    return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                     "settlement status does not match stage status");
  }
  adapter = flow_adapter_for_stage(flow, stage);
  if (!adapter || !adapter->settlement_ops.apply) {
    return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                     "operation settlement owner is not configured");
  }
  flow_make_stage_view(stage, &view);
  rc = adapter->settlement_ops.apply(adapter->settlement_ctx, flow, &view, msg, result);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, stage->line, stage->column,
                                     "settlement owner rejected result");
  }
  completion->settlement = *result;
  completion->settlement_reported = 1;
  completion->terminal = result->action != TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE &&
                         result->action != TURBO_FLOW_SETTLEMENT_ACTION_ACKNOWLEDGE;
  return callback_status;
}
