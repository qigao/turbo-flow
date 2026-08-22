#include "flow_internal.h"

#include <string.h>

static int flow_emitter_view_within(const vstr *view, const void *base, size_t size) {
  uintptr_t data_address;
  uintptr_t base_address;
  size_t offset;

  if (!view || !base || !view->data) return 0;
  data_address = (uintptr_t)view->data;
  base_address = (uintptr_t)base;
  if (data_address < base_address) return 0;
  offset = (size_t)(data_address - base_address);
  return offset <= size && view->len <= size - offset;
}

static int flow_emitter_output_validate(const turbo_flow_msg_t *output) {
  if (!output) return TURBO_EINVAL;
  if ((!output->payload.data && output->payload.len != 0u) ||
      (output->payload.data &&
       !(output->owned_payload &&
         flow_emitter_view_within(&output->payload, output->owned_payload,
                                  tstr_len(output->owned_payload))) &&
       !(output->buffer &&
         flow_emitter_view_within(&output->payload, mem_buffer_const_data(output->buffer),
                                  mem_buffer_used(output->buffer))))) {
    return TURBO_EINVAL;
  }
  if (flow_msg_transport_context_is_borrowed(output)) {
    return TURBO_ENOTSUP;
  }
  return TURBO_OK;
}

int flow_emitter_init(turbo_flow_emitter_t *emitter, uint32_t max_outputs) {
  int rc;
  if (!emitter || max_outputs == 0u || max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS) {
    return TURBO_EINVAL;
  }
  memset(emitter, 0, sizeof(*emitter));
  rc = turbo_vec_init(&emitter->outputs, sizeof(turbo_flow_msg_t));
  if (rc != TURBO_OK) return rc;
  emitter->max_outputs = max_outputs;
  emitter->active = 1;
  emitter->status = TURBO_OK;
  return TURBO_OK;
}

void flow_emitter_close(turbo_flow_emitter_t *emitter) {
  if (emitter) emitter->active = 0;
}

void flow_emitter_cleanup(turbo_flow_emitter_t *emitter) {
  if (!emitter) return;
  for (size_t i = 0; i < turbo_vec_size(&emitter->outputs); ++i) {
    turbo_flow_msg_t *output = (turbo_flow_msg_t *)turbo_vec_at(&emitter->outputs, i);
    turbo_flow_msg_cleanup(output);
  }
  turbo_vec_destroy(&emitter->outputs);
  memset(emitter, 0, sizeof(*emitter));
}

int turbo_flow_emitter_emit_clone(turbo_flow_emitter_t *emitter,
                                  const turbo_flow_msg_t *output) {
  turbo_flow_msg_t clone;
  int rc;
  if (!emitter || !emitter->active) return TURBO_EBUSY;
  if (emitter->status != TURBO_OK) return emitter->status;
  rc = flow_emitter_output_validate(output);
  if (rc != TURBO_OK) return emitter->status = rc;
  if (turbo_vec_size(&emitter->outputs) >= emitter->max_outputs) {
    return emitter->status = TURBO_ENOSPC;
  }

  rc = turbo_flow_msg_clone(&clone, output);
  if (rc != TURBO_OK) return emitter->status = rc;
  rc = turbo_vec_push(&emitter->outputs, &clone);
  if (rc != TURBO_OK) turbo_flow_msg_cleanup(&clone);
  if (rc != TURBO_OK) emitter->status = rc;
  return emitter->status;
}

int turbo_flow_emitter_emit_move(turbo_flow_emitter_t *emitter, turbo_flow_msg_t *output) {
  int rc;
  if (!emitter || !emitter->active) return TURBO_EBUSY;
  if (emitter->status != TURBO_OK) return emitter->status;
  rc = flow_emitter_output_validate(output);
  if (rc != TURBO_OK) return emitter->status = rc;
  if (turbo_vec_size(&emitter->outputs) >= emitter->max_outputs) {
    return emitter->status = TURBO_ENOSPC;
  }

  rc = turbo_vec_push(&emitter->outputs, output);
  if (rc == TURBO_OK) turbo_flow_msg_init(output);
  if (rc != TURBO_OK) emitter->status = rc;
  return emitter->status;
}

size_t turbo_flow_emitter_count(const turbo_flow_emitter_t *emitter) {
  return emitter && emitter->active ? turbo_vec_size(&emitter->outputs) : 0u;
}
