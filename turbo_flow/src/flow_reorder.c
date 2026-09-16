#include "flow_internal.h"

#include <errno.h>
#include <string.h>

static flow_reorder_state_t *flow_reorder_for_stage(turbo_flow_t *flow, uint32_t stage_index) {
  if (!flow) return NULL;
  for (size_t i = 0; i < vec_size(&flow->reorder_states); ++i) {
    flow_reorder_state_t *state = (flow_reorder_state_t *)vec_at(&flow->reorder_states, i);
    if (state && state->stage_index == stage_index) return state;
  }
  return NULL;
}

static void flow_reorder_advance_canceled(flow_reorder_state_t *state) {
  while (!state->active && hash_set_contains(&state->canceled_sequences, &state->next_sequence)) {
    (void)turbo_flow_stl_error(hash_set_remove(&state->canceled_sequences, &state->next_sequence));
    ++state->next_sequence;
  }
}

void flow_clear_reorder_states(turbo_flow_t *flow) {
  if (!flow) return;
  for (size_t i = 0; i < vec_size(&flow->reorder_states); ++i) {
    flow_reorder_state_t *state = (flow_reorder_state_t *)vec_at(&flow->reorder_states, i);
    if (!state) continue;
    if (state->canceled_sequences_initialized) {
      hash_set_destroy(&state->canceled_sequences);
      state->canceled_sequences_initialized = 0;
    }
    salts_cond_destroy(&state->cond);
    salts_mutex_destroy(&state->mutex);
  }
  turbo_flow_stl_error(vec_clear(&flow->reorder_states));
}

int flow_start_reorder_states(turbo_flow_t *flow) {
  if (!flow) return SALTS_EINVAL;
  flow_clear_reorder_states(flow);

  for (size_t i = 0; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage =
        (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, i);
    flow_reorder_state_t state;
    if (!stage || stage->reorder.capacity == 0) continue;

    memset(&state, 0, sizeof(state));
    state.stage_index = (uint32_t)i;
    state.capacity = stage->reorder.capacity;
    state.timeout_ms = stage->reorder.timeout_ms;
    state.next_sequence = 1u;
    if (turbo_flow_stl_error(hash_set_init_bytes(&state.canceled_sequences, sizeof(uint64_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX, ((NULL) ? (NULL) : hash_bytes), ((NULL) ? (NULL) : hash_key_equal), NULL)) != SALTS_OK ||
        turbo_flow_stl_error(hash_set_reserve(&state.canceled_sequences, (size_t)state.capacity + 1u)) != SALTS_OK) {
      hash_set_destroy(&state.canceled_sequences);
      flow_clear_reorder_states(flow);
      return flow_set_error_keep_state(flow, SALTS_ENOMEM, stage->line, stage->column,
                                       "cannot create reorder cancellation state");
    }
    state.canceled_sequences_initialized = 1;
    salts_mutex_init(&state.mutex);
    salts_cond_init(&state.cond);
    if (!state.mutex || !state.cond || turbo_flow_stl_error(vec_push(&flow->reorder_states, &state)) != SALTS_OK) {
      salts_cond_destroy(&state.cond);
      salts_mutex_destroy(&state.mutex);
      hash_set_destroy(&state.canceled_sequences);
      flow_clear_reorder_states(flow);
      return flow_set_error_keep_state(flow, SALTS_ENOMEM, stage->line, stage->column,
                                       "cannot create reorder state");
    }
  }
  return SALTS_OK;
}

int flow_reorder_reserve(turbo_flow_t *flow, uint32_t stage_index, uint64_t *sequence) {
  flow_reorder_state_t *state = flow_reorder_for_stage(flow, stage_index);
  if (!sequence) return SALTS_EINVAL;
  if (!state) return SALTS_ENOTSUP;

  salts_mutex_lock(&state->mutex);
  if (state->stopping) {
    salts_mutex_unlock(&state->mutex);
    return SALTS_ESHUTDOWN;
  }
  if (state->issued_sequence >= state->next_sequence &&
      state->issued_sequence - state->next_sequence >= state->capacity) {
    salts_mutex_unlock(&state->mutex);
    return SALTS_ENOSPC;
  }
  *sequence = ++state->issued_sequence;
  salts_mutex_unlock(&state->mutex);
  return SALTS_OK;
}

int flow_reorder_cancel(turbo_flow_t *flow, uint32_t stage_index, uint64_t sequence) {
  flow_reorder_state_t *state = flow_reorder_for_stage(flow, stage_index);
  int rc = SALTS_OK;
  if (!state || sequence == 0u) return SALTS_OK;

  salts_mutex_lock(&state->mutex);
  if (sequence >= state->next_sequence) {
    rc = turbo_flow_stl_error(hash_set_add(&state->canceled_sequences, &sequence));
    if (rc == SALTS_EALREADY) rc = SALTS_OK;
    if (rc == SALTS_OK) {
      flow_reorder_advance_canceled(state);
      salts_cond_broadcast(&state->cond);
    }
  }
  salts_mutex_unlock(&state->mutex);
  return rc;
}

void flow_stop_reorder_states(turbo_flow_t *flow) {
  if (!flow) return;
  for (size_t i = 0; i < vec_size(&flow->reorder_states); ++i) {
    flow_reorder_state_t *state = (flow_reorder_state_t *)vec_at(&flow->reorder_states, i);
    if (!state) continue;
    salts_mutex_lock(&state->mutex);
    state->stopping = 1;
    salts_cond_broadcast(&state->cond);
    salts_mutex_unlock(&state->mutex);
  }
}

int flow_reorder_enter(turbo_flow_t *flow, uint32_t stage_index, uint64_t sequence) {
  flow_reorder_state_t *state = flow_reorder_for_stage(flow, stage_index);
  int rc = SALTS_OK;
  if (!state) return SALTS_OK;

  salts_mutex_lock(&state->mutex);
  if (state->stopping) {
    rc = SALTS_ESHUTDOWN;
    goto done;
  }
  if (sequence < state->next_sequence) {
    rc = SALTS_EALREADY;
    goto done;
  }
  if (sequence != state->next_sequence || state->active) {
    if (state->waiting >= state->capacity) {
      rc = SALTS_ENOSPC;
      goto done;
    }
    ++state->waiting;
    while (!state->stopping && (sequence != state->next_sequence || state->active)) {
      int wait_rc =
          salts_cond_timedwait(&state->cond, &state->mutex, (uint64_t)state->timeout_ms * 1000000u);
      if (wait_rc == -ETIMEDOUT) {
        rc = SALTS_ETIMEDOUT;
        break;
      }
    }
    --state->waiting;
  }
  if (state->stopping) rc = SALTS_ESHUTDOWN;
  if (rc == SALTS_OK) state->active = 1;

done:
  salts_mutex_unlock(&state->mutex);
  return rc;
}

void flow_reorder_leave(turbo_flow_t *flow, uint32_t stage_index, uint64_t sequence) {
  flow_reorder_state_t *state = flow_reorder_for_stage(flow, stage_index);
  if (!state) return;

  salts_mutex_lock(&state->mutex);
  if (state->active && sequence == state->next_sequence) {
    state->active = 0;
    ++state->next_sequence;
    flow_reorder_advance_canceled(state);
    salts_cond_broadcast(&state->cond);
  }
  salts_mutex_unlock(&state->mutex);
}
