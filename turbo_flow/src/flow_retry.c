#include "flow_internal.h"

int turbo_flow_retry_execute(const turbo_flow_retry_policy_t *policy, turbo_flow_msg_t *msg,
                             const turbo_flow_retry_ops_t *ops, void *ctx) {
  if (!policy || !msg || !ops || ops->size < sizeof(*ops) || !ops->attempt || !ops->retryable) {
    return SALTS_EINVAL;
  }
  if (policy->max_attempts < 2u || policy->max_attempts > TURBO_FLOW_RETRY_MAX_ATTEMPTS ||
      policy->delay_ms > TURBO_FLOW_RETRY_MAX_DELAY_MS || (policy->delay_ms > 0u && !ops->wait)) {
    return SALTS_EINVAL;
  }

  for (uint32_t attempt = 1u; attempt <= policy->max_attempts; ++attempt) {
    turbo_flow_msg_t attempt_msg;
    int status;

    turbo_flow_msg_init(&attempt_msg);
    msg->execution_attempt = attempt;
    status = turbo_flow_msg_clone(&attempt_msg, msg);
    if (status != SALTS_OK) return status;
    attempt_msg.execution_attempt = attempt;
    status = ops->attempt(ctx, &attempt_msg, attempt);
    if (status == SALTS_OK) {
      turbo_flow_msg_cleanup(msg);
      turbo_flow_msg_move(msg, &attempt_msg);
      return SALTS_OK;
    }
    turbo_flow_msg_cleanup(&attempt_msg);
    if (attempt == policy->max_attempts || !ops->retryable(ctx, status)) return status;
    if (policy->delay_ms > 0u) {
      int wait_status = ops->wait(ctx, policy->delay_ms);
      if (wait_status != SALTS_OK) return wait_status;
    }
  }
  return SALTS_EINVAL;
}
