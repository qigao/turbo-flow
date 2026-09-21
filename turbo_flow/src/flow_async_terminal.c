#include "flow_internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_async_terminal_claim_impl_s {
  turbo_flow_t *flow;
  flow_async_publication_t *publication;
  uint32_t stage_index;
  turbo_flow_msg_t message;
  flow_stage_completion_t completion;
  flow_sink_completion_observer_t sink_observer;
} flow_async_terminal_claim_impl_t;

typedef struct flow_async_emit_claim_impl_s {
  turbo_flow_t *flow;
  flow_async_publication_t *publication;
  uint32_t stage_index;
  turbo_flow_msg_t message;
  flow_stage_completion_t completion;
} flow_async_emit_claim_impl_t;

struct flow_async_publication_s {
  turbo_flow_t *flow;
  const char *source_name;
  turbo_flow_msg_t message;
  uint64_t observe_start;
  salts_mutex_t mutex;
  size_t pending;
  int sealed;
  int owner_active;
  int status;
  flow_async_publication_finish_fn finish;
  void *finish_ctx;
  flow_async_publication_t *previous_scope;
};

static SALTS_THREAD_LOCAL flow_async_publication_t *flow_current_async_publication;

static int flow_async_terminal_claim_shape_valid(const turbo_flow_async_terminal_claim_t *claim) {
  return claim && claim->size >= sizeof(*claim) &&
         claim->version == TURBO_FLOW_ASYNC_TERMINAL_API_VERSION;
}

static int flow_async_emit_claim_shape_valid(const turbo_flow_async_emit_claim_t *claim) {
  return claim && claim->size >= sizeof(*claim) &&
         claim->version == TURBO_FLOW_ASYNC_EMIT_API_VERSION;
}

static void flow_async_publication_finalize(flow_async_publication_t *publication) {
  turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
  turbo_flow_observe_event_t event;
  turbo_flow_t *flow = publication->flow;
  result.status = publication->status;
  if (flow->observer_ops.message_complete) {
    flow->observer_ops.message_complete(
        flow->observer_ctx, publication->source_name, &publication->message,
        publication->observe_start != 0u ? salts_hrtime() - publication->observe_start : 0u,
        result.status);
  }
  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_FLOW_COMPLETE;
  event.source_name = publication->source_name;
  event.msg = &publication->message;
  event.status = result.status;
  event.selected = -1;
  event.edge_kind = -1;
  event.duration_ns =
      publication->observe_start != 0u ? salts_hrtime() - publication->observe_start : 0u;
  flow_observer_emit(flow, &event);
  if (publication->finish) publication->finish(publication->finish_ctx, &result);
  turbo_flow_msg_cleanup(&publication->message);
  salts_mutex_destroy(&publication->mutex);
  free(publication);
  flow_publish_leave(flow);
}

static void flow_async_publication_release(flow_async_publication_t *publication, int status) {
  int finalize = 0;
  salts_mutex_lock(&publication->mutex);
  if (publication->status == SALTS_OK && status != SALTS_OK) publication->status = status;
  if (publication->pending > 0u) --publication->pending;
  if (publication->sealed && !publication->owner_active && publication->pending == 0u) finalize = 1;
  salts_mutex_unlock(&publication->mutex);
  if (finalize) flow_async_publication_finalize(publication);
}

flow_async_publication_t *flow_async_publication_create(turbo_flow_t *flow, const char *source_name,
                                                        const turbo_flow_msg_t *message,
                                                        uint64_t observe_start,
                                                        flow_async_publication_finish_fn finish,
                                                        void *ctx) {
  flow_async_publication_t *publication;
  int rc;
  if (!flow || !source_name || !message) return NULL;
  publication = (flow_async_publication_t *)calloc(1u, sizeof(*publication));
  if (!publication) return NULL;
  turbo_flow_msg_init(&publication->message);
  rc = turbo_flow_msg_clone(&publication->message, message);
  if (rc != SALTS_OK) {
    free(publication);
    return NULL;
  }
  publication->flow = flow;
  publication->source_name = source_name;
  publication->observe_start = observe_start;
  publication->status = SALTS_OK;
  publication->owner_active = 1;
  publication->finish = finish;
  publication->finish_ctx = ctx;
  publication->previous_scope = flow_current_async_publication;
  salts_mutex_init(&publication->mutex);
  salts_mutex_lock(&flow->runtime_mutex);
  ++flow->active_publishes;
  salts_mutex_unlock(&flow->runtime_mutex);
  flow_current_async_publication = publication;
  return publication;
}

void flow_async_publication_seal(flow_async_publication_t *publication, int status) {
  int finalize = 0;
  if (!publication) return;
  if (flow_current_async_publication == publication) {
    flow_current_async_publication = publication->previous_scope;
  }
  salts_mutex_lock(&publication->mutex);
  if (publication->status == SALTS_OK && status != SALTS_OK) publication->status = status;
  publication->sealed = 1;
  if (!publication->owner_active && publication->pending == 0u) finalize = 1;
  salts_mutex_unlock(&publication->mutex);
  if (finalize) flow_async_publication_finalize(publication);
}

void flow_async_publication_owner_leave(flow_async_publication_t *publication) {
  int finalize = 0;
  if (!publication) return;
  salts_mutex_lock(&publication->mutex);
  if (publication->owner_active) publication->owner_active = 0;
  if (publication->sealed && publication->pending == 0u) finalize = 1;
  salts_mutex_unlock(&publication->mutex);
  if (finalize) flow_async_publication_finalize(publication);
}

int turbo_flow_async_terminal_claim_move(turbo_flow_async_terminal_claim_t *destination,
                                         turbo_flow_async_terminal_claim_t *source) {
  if (!flow_async_terminal_claim_shape_valid(destination) ||
      !flow_async_terminal_claim_shape_valid(source) || !source->_impl || destination->_impl) {
    return SALTS_EINVAL;
  }
  destination->_impl = source->_impl;
  source->_impl = NULL;
  return SALTS_OK;
}

const turbo_flow_msg_t *
turbo_flow_async_terminal_claim_message(const turbo_flow_async_terminal_claim_t *claim) {
  const flow_async_terminal_claim_impl_t *impl;
  if (!flow_async_terminal_claim_shape_valid(claim) || !claim->_impl) return NULL;
  impl = (const flow_async_terminal_claim_impl_t *)claim->_impl;
  return &impl->message;
}

static void flow_async_terminal_observe(flow_async_terminal_claim_impl_t *impl, int status) {
  const flow_stage_plan_impl_t *stage =
      (const flow_stage_plan_impl_t *)vec_at_const(&impl->flow->stages, impl->stage_index);
  turbo_flow_observe_event_t event;
  uint64_t duration = impl->completion.async_started_at != 0u
                          ? salts_hrtime() - impl->completion.async_started_at
                          : 0u;
  if (impl->flow->observer_ops.stage_complete) {
    impl->flow->observer_ops.stage_complete(impl->flow->observer_ctx, stage ? stage->name : NULL,
                                            stage ? stage->adapter_name : NULL, &impl->message,
                                            duration, status);
  }
  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_STAGE_END;
  event.stage_name = stage ? stage->name : NULL;
  event.adapter_name = stage ? stage->adapter_name : NULL;
  event.operation_name = stage ? stage->operation_name : NULL;
  event.msg = &impl->message;
  event.status = status;
  event.selected = -1;
  event.edge_kind = -1;
  event.attempt = impl->message.execution_attempt;
  event.duration_ns = duration;
  flow_observer_emit(impl->flow, &event);
  event.kind = TURBO_FLOW_OBSERVE_SINK_COMPLETE;
  event.duration_ns = 0u;
  if (impl->sink_observer.fn) {
    const flow_sink_completion_observer_t previous =
        flow_sink_completion_scope_enter(
            impl->sink_observer.fn, impl->sink_observer.ctx);
    flow_observer_emit(impl->flow, &event);
    flow_sink_completion_scope_leave(previous);
  } else {
    flow_observer_emit(impl->flow, &event);
  }
}

int turbo_flow_async_terminal_complete(turbo_flow_async_terminal_claim_t *claim, int status,
                                       const turbo_flow_settlement_result_t *settlement) {
  flow_async_terminal_claim_impl_t *impl;
  const flow_stage_plan_impl_t *stage;
  flow_async_publication_t *publication;
  int terminal_status = status;
  if (!flow_async_terminal_claim_shape_valid(claim)) return SALTS_EINVAL;
  if (!claim->_impl) return SALTS_EALREADY;
  if (settlement && settlement->size < sizeof(*settlement)) return SALTS_EINVAL;
  impl = (flow_async_terminal_claim_impl_t *)claim->_impl;
  claim->_impl = NULL;
  publication = impl->publication;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&impl->flow->stages, impl->stage_index);
  if (!stage) {
    terminal_status = SALTS_EPROTO;
  } else {
    impl->completion.status = status;
    if (settlement) {
      impl->completion.settlement = *settlement;
      impl->completion.settlement.size = sizeof(impl->completion.settlement);
      impl->completion.settlement_reported = 1;
    }
    terminal_status = flow_adapter_apply_settlement(impl->flow, stage, impl->stage_index,
                                                    &impl->message, &impl->completion, status);
  }
  flow_async_terminal_observe(impl, terminal_status);
  turbo_flow_msg_cleanup(&impl->message);
  free(impl);
  flow_async_publication_release(publication, terminal_status);
  return SALTS_OK;
}

int turbo_flow_async_emit_claim_move(turbo_flow_async_emit_claim_t *destination,
                                     turbo_flow_async_emit_claim_t *source) {
  if (!flow_async_emit_claim_shape_valid(destination) ||
      !flow_async_emit_claim_shape_valid(source) || !source->_impl || destination->_impl) {
    return SALTS_EINVAL;
  }
  destination->_impl = source->_impl;
  source->_impl = NULL;
  return SALTS_OK;
}

const turbo_flow_msg_t *
turbo_flow_async_emit_claim_message(const turbo_flow_async_emit_claim_t *claim) {
  const flow_async_emit_claim_impl_t *impl;
  if (!flow_async_emit_claim_shape_valid(claim) || !claim->_impl) return NULL;
  impl = (const flow_async_emit_claim_impl_t *)claim->_impl;
  return &impl->message;
}

static void flow_async_emit_observe(flow_async_emit_claim_impl_t *impl, int status) {
  const flow_stage_plan_impl_t *stage =
      (const flow_stage_plan_impl_t *)vec_at_const(&impl->flow->stages, impl->stage_index);
  turbo_flow_observe_event_t event;
  uint64_t duration = impl->completion.async_started_at != 0u
                          ? salts_hrtime() - impl->completion.async_started_at
                          : 0u;
  if (impl->flow->observer_ops.stage_complete) {
    impl->flow->observer_ops.stage_complete(impl->flow->observer_ctx, stage ? stage->name : NULL,
                                            stage ? stage->adapter_name : NULL, &impl->message,
                                            duration, status);
  }
  memset(&event, 0, sizeof(event));
  event.kind = TURBO_FLOW_OBSERVE_STAGE_END;
  event.stage_name = stage ? stage->name : NULL;
  event.adapter_name = stage ? stage->adapter_name : NULL;
  event.operation_name = stage ? stage->operation_name : NULL;
  event.msg = &impl->message;
  event.status = status;
  event.selected = -1;
  event.edge_kind = -1;
  event.attempt = impl->message.execution_attempt;
  event.duration_ns = duration;
  flow_observer_emit(impl->flow, &event);
}

int turbo_flow_async_emit_complete(turbo_flow_async_emit_claim_t *claim, int status,
                                   turbo_flow_msg_t *output) {
  flow_async_emit_claim_impl_t *impl;
  flow_async_publication_t *publication;
  flow_async_publication_t *previous_scope;
  turbo_flow_msg_t local;
  int terminal_status = status;
  int has_output = output != NULL;

  if (!flow_async_emit_claim_shape_valid(claim)) return SALTS_EINVAL;
  if (!claim->_impl) return SALTS_EALREADY;
  if ((status != SALTS_OK && output) ||
      (output && (flow_msg_payload_validate(output) != SALTS_OK ||
                  flow_msg_transport_context_is_borrowed(output)))) {
    return SALTS_EINVAL;
  }

  impl = (flow_async_emit_claim_impl_t *)claim->_impl;
  claim->_impl = NULL;
  publication = impl->publication;
  turbo_flow_msg_init(&local);
  if (has_output) {
    (void)turbo_flow_msg_move(&local, output);
    previous_scope = flow_current_async_publication;
    flow_current_async_publication = publication;
    terminal_status = flow_run_message_from_stage(impl->flow, impl->stage_index, &local);
    flow_current_async_publication = previous_scope;
    turbo_flow_msg_cleanup(&local);
  }
  flow_async_emit_observe(impl, status);
  turbo_flow_msg_cleanup(&impl->message);
  free(impl);
  flow_async_publication_release(publication, terminal_status);
  return SALTS_OK;
}

static void flow_async_terminal_abandon(flow_async_terminal_claim_impl_t *impl) {
  flow_async_publication_t *publication = impl->publication;
  turbo_flow_msg_cleanup(&impl->message);
  free(impl);
  flow_async_publication_release(publication, SALTS_OK);
}

int flow_async_terminal_submit_stage(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                                     const flow_adapter_registration_t *adapter,
                                     turbo_flow_msg_t *msg, flow_stage_completion_t *completion) {
  flow_async_terminal_claim_impl_t *impl;
  turbo_flow_async_terminal_claim_t claim = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  turbo_flow_stage_plan_t view;
  int rc;
  if (!flow_current_async_publication) {
    return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                     "async terminal stage requires asynchronous publication");
  }
  impl = (flow_async_terminal_claim_impl_t *)calloc(1u, sizeof(*impl));
  if (!impl) return SALTS_ENOMEM;
  turbo_flow_msg_init(&impl->message);
  rc = turbo_flow_msg_clone(&impl->message, msg);
  if (rc != SALTS_OK) {
    free(impl);
    return rc;
  }
  impl->flow = flow;
  impl->publication = flow_current_async_publication;
  impl->stage_index = completion->entry.stage_index;
  impl->completion = *completion;
  impl->sink_observer = flow_sink_completion_scope_current();
  salts_mutex_lock(&impl->publication->mutex);
  ++impl->publication->pending;
  salts_mutex_unlock(&impl->publication->mutex);
  claim._impl = impl;
  flow_make_stage_view(stage, &view);
  rc = adapter->async_terminal_ops.submit(adapter->ctx, flow, &view, msg, &claim);
  if (rc == SALTS_OK && claim._impl) {
    flow_async_terminal_abandon(impl);
    return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                     "async terminal adapter did not move its accepted claim");
  }
  if (rc != SALTS_OK) {
    if (!claim._impl) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                       "async terminal adapter moved a rejected claim");
    }
    flow_async_terminal_abandon(impl);
    return rc;
  }
  completion->async_pending = 1;
  return SALTS_OK;
}

static void flow_async_emit_abandon(flow_async_emit_claim_impl_t *impl) {
  flow_async_publication_t *publication = impl->publication;
  turbo_flow_msg_cleanup(&impl->message);
  free(impl);
  flow_async_publication_release(publication, SALTS_OK);
}

int flow_async_emit_submit_stage(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                                 const flow_adapter_registration_t *adapter, turbo_flow_msg_t *msg,
                                 flow_stage_completion_t *completion) {
  flow_async_emit_claim_impl_t *impl;
  turbo_flow_async_emit_claim_t claim = TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
  turbo_flow_stage_plan_t view;
  int rc;
  if (!flow_current_async_publication) {
    return flow_set_error_keep_state(flow, SALTS_ENOTSUP, stage->line, stage->column,
                                     "async emitting stage requires asynchronous publication");
  }
  impl = (flow_async_emit_claim_impl_t *)calloc(1u, sizeof(*impl));
  if (!impl) return SALTS_ENOMEM;
  turbo_flow_msg_init(&impl->message);
  rc = turbo_flow_msg_clone(&impl->message, msg);
  if (rc != SALTS_OK) {
    free(impl);
    return rc;
  }
  impl->flow = flow;
  impl->publication = flow_current_async_publication;
  impl->stage_index = completion->entry.stage_index;
  impl->completion = *completion;
  salts_mutex_lock(&impl->publication->mutex);
  ++impl->publication->pending;
  salts_mutex_unlock(&impl->publication->mutex);
  claim._impl = impl;
  flow_make_stage_view(stage, &view);
  rc = adapter->async_emit_ops.submit(adapter->ctx, flow, &view, msg, &claim);
  if (rc == SALTS_OK && claim._impl) {
    flow_async_emit_abandon(impl);
    return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                     "async emitting adapter did not move its accepted claim");
  }
  if (rc != SALTS_OK) {
    if (!claim._impl) {
      return flow_set_error_keep_state(flow, SALTS_EPROTO, stage->line, stage->column,
                                       "async emitting adapter moved a rejected claim");
    }
    flow_async_emit_abandon(impl);
    return rc;
  }
  completion->async_pending = 1;
  return SALTS_OK;
}
