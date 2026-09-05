#include "flow_internal.h"

#include <string.h>

static int flow_emitter_output_validate(const turbo_flow_msg_t *output) {
  if (!output) return SALTS_EINVAL;
  if (flow_msg_payload_validate(output) != SALTS_OK) return SALTS_EINVAL;
  if (flow_msg_transport_context_is_borrowed(output)) {
    return SALTS_ENOTSUP;
  }
  return SALTS_OK;
}

int flow_emitter_init(turbo_flow_emitter_t *emitter, uint32_t max_outputs) {
  int rc;
  if (!emitter || max_outputs == 0u || max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS) {
    return SALTS_EINVAL;
  }
  memset(emitter, 0, sizeof(*emitter));
  rc = turbo_flow_stl_error(vec_init_bytes(&emitter->outputs, sizeof(turbo_flow_msg_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX));
  if (rc != SALTS_OK) return rc;
  emitter->max_outputs = max_outputs;
  emitter->active = 1;
  emitter->status = SALTS_OK;
  return SALTS_OK;
}

void flow_emitter_close(turbo_flow_emitter_t *emitter) {
  if (emitter) emitter->active = 0;
}

void flow_emitter_cleanup(turbo_flow_emitter_t *emitter) {
  if (!emitter) return;
  for (size_t i = 0; i < vec_size(&emitter->outputs); ++i) {
    turbo_flow_msg_t *output = (turbo_flow_msg_t *)vec_at(&emitter->outputs, i);
    turbo_flow_msg_cleanup(output);
  }
  vec_destroy(&emitter->outputs);
  memset(emitter, 0, sizeof(*emitter));
}

int turbo_flow_emitter_emit_clone(turbo_flow_emitter_t *emitter,
                                  const turbo_flow_msg_t *output) {
  turbo_flow_msg_t clone;
  int rc;
  if (!emitter || !emitter->active) return SALTS_EBUSY;
  if (emitter->status != SALTS_OK) return emitter->status;
  rc = flow_emitter_output_validate(output);
  if (rc != SALTS_OK) return emitter->status = rc;
  if (vec_size(&emitter->outputs) >= emitter->max_outputs) {
    return emitter->status = SALTS_ENOSPC;
  }

  rc = turbo_flow_msg_clone(&clone, output);
  if (rc != SALTS_OK) return emitter->status = rc;
  rc = turbo_flow_stl_error(vec_push(&emitter->outputs, &clone));
  if (rc != SALTS_OK) turbo_flow_msg_cleanup(&clone);
  if (rc != SALTS_OK) emitter->status = rc;
  return emitter->status;
}

int turbo_flow_emitter_emit_move(turbo_flow_emitter_t *emitter, turbo_flow_msg_t *output) {
  int rc;
  if (!emitter || !emitter->active) return SALTS_EBUSY;
  if (emitter->status != SALTS_OK) return emitter->status;
  rc = flow_emitter_output_validate(output);
  if (rc != SALTS_OK) return emitter->status = rc;
  if (vec_size(&emitter->outputs) >= emitter->max_outputs) {
    return emitter->status = SALTS_ENOSPC;
  }

  rc = turbo_flow_stl_error(vec_push(&emitter->outputs, output));
  if (rc == SALTS_OK) turbo_flow_msg_init(output);
  if (rc != SALTS_OK) emitter->status = rc;
  return emitter->status;
}

size_t turbo_flow_emitter_count(const turbo_flow_emitter_t *emitter) {
  return emitter && emitter->active ? vec_size(&emitter->outputs) : 0u;
}
