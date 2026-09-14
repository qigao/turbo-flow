#include "turbo_flow_inbox_source.h"

#include <cflow/publishers.h>
#include <salts_error.h>
#include <tstr.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum flow_inbox_source_phase_e {
  FLOW_INBOX_SOURCE_IDLE = 0,
  FLOW_INBOX_SOURCE_GRAPH_RUNNING,
  FLOW_INBOX_SOURCE_SETTLE_COMPLETE,
  FLOW_INBOX_SOURCE_SETTLE_FAIL,
  FLOW_INBOX_SOURCE_SETTLE_UNKNOWN
} flow_inbox_source_phase_t;

struct turbo_flow_inbox_source_s {
  turbo_flow_inbox_source_config_t config;
  tstr graph_source_name;
  flow_inbox_source_phase_t phase;
  turbo_flow_inbox_claim_t claim;
  turbo_flow_msg_t message;
  turbo_flow_run_t *run;
  uint64_t record_id;
  int graph_status;
  int settlement_status;
  int request_failure_status;
  bool message_live;
};

static int flow_inbox_source_add_size(size_t left, size_t right, size_t *out) {
  if (!out || right > SIZE_MAX - left) return SALTS_ERANGE;
  *out = left + right;
  return SALTS_OK;
}

static int flow_inbox_source_result_prepare(turbo_flow_inbox_source_result_t *result) {
  if (!result || result->size != sizeof(*result) ||
      result->version != TURBO_FLOW_INBOX_SOURCE_API_VERSION) {
    return SALTS_EINVAL;
  }
  *result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
  return SALTS_OK;
}

static void flow_inbox_source_result_set(const turbo_flow_inbox_source_t *source,
                                         turbo_flow_inbox_source_result_state_t state,
                                         turbo_flow_inbox_source_result_t *result) {
  result->state = state;
  result->record_id = source->record_id;
  result->graph_status = source->graph_status;
  result->settlement_status = source->settlement_status;
}

static void flow_inbox_source_release(turbo_flow_inbox_source_t *source) {
  if (source->run) turbo_flow_run_close(source->run);
  if (source->message_live) turbo_flow_msg_cleanup(&source->message);
  source->phase = FLOW_INBOX_SOURCE_IDLE;
  source->claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  memset(&source->message, 0, sizeof(source->message));
  source->run = NULL;
  source->record_id = 0u;
  source->graph_status = SALTS_OK;
  source->settlement_status = SALTS_OK;
  source->request_failure_status = SALTS_OK;
  source->message_live = false;
}

static int flow_inbox_source_config_check(const turbo_flow_inbox_source_config_t *config) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  const turbo_flow_stage_plan_t *stage;
  int stage_index;
  int status;
  if (!config || config->size != sizeof(*config) ||
      config->version != TURBO_FLOW_INBOX_SOURCE_API_VERSION || !config->inbox || !config->flow ||
      !config->graph_source_name || config->graph_source_name[0] == '\0' ||
      strlen(config->graph_source_name) > TURBO_FLOW_CONTROL_NAME_MAX ||
      config->max_message_bytes < sizeof(turbo_flow_inbox_source_context_t) + 1u ||
      turbo_flow_state(config->flow) != TURBO_FLOW_STATE_STARTED ||
      (config->scheduler && !cflow_scheduler_valid(config->scheduler)))
    return SALTS_EINVAL;
  status = turbo_flow_inbox_snapshot(config->inbox, &snapshot);
  if (status != SALTS_OK) return status;
  stage_index = turbo_flow_find_stage(config->flow, config->graph_source_name);
  if (stage_index < 0) return SALTS_EINVAL;
  stage = turbo_flow_stage_at(config->flow, (size_t)stage_index);
  return stage && stage->is_source && (!stage->adapter_name || stage->adapter_name[0] == '\0')
             ? SALTS_OK
             : SALTS_EINVAL;
}

int turbo_flow_inbox_source_create(const turbo_flow_inbox_source_config_t *config,
                                   turbo_flow_inbox_source_t **source_out) {
  turbo_flow_inbox_source_t *source;
  int status;
  if (!source_out || *source_out) return SALTS_EINVAL;
  status = flow_inbox_source_config_check(config);
  if (status != SALTS_OK) return status;
  source = (turbo_flow_inbox_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  source->graph_source_name = tstr_dup(config->graph_source_name);
  if (!source->graph_source_name) {
    free(source);
    return SALTS_ENOMEM;
  }
  source->config = *config;
  source->config.graph_source_name = source->graph_source_name;
  source->claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  source->phase = FLOW_INBOX_SOURCE_IDLE;
  *source_out = source;
  return SALTS_OK;
}

static int flow_inbox_source_message_build(turbo_flow_inbox_source_t *source) {
  const turbo_flow_inbox_record_t *record = &source->claim.record;
  turbo_flow_inbox_source_context_t *context;
  mem_buffer_t *buffer;
  char *base;
  size_t retained_bytes = sizeof(*context);
  size_t source_id_offset;
  size_t admission_id_offset;
  size_t correlation_offset;
  size_t payload_offset;
  int status;

  source_id_offset = retained_bytes;
  status = flow_inbox_source_add_size(retained_bytes, record->source_id.len, &retained_bytes);
  if (status != SALTS_OK) return status;
  admission_id_offset = retained_bytes;
  status = flow_inbox_source_add_size(retained_bytes, record->admission_id.len, &retained_bytes);
  if (status != SALTS_OK) return status;
  correlation_offset = retained_bytes;
  status = flow_inbox_source_add_size(retained_bytes, record->correlation.len, &retained_bytes);
  if (status != SALTS_OK) return status;
  payload_offset = retained_bytes;
  status = flow_inbox_source_add_size(retained_bytes, record->payload.len, &retained_bytes);
  if (status != SALTS_OK) return status;
  if (retained_bytes > source->config.max_message_bytes) return SALTS_ENOSPC;

  buffer = mem_get_buffer(mem_global(), retained_bytes);
  if (!buffer) return SALTS_ENOMEM;
  base = mem_buffer_data(buffer);
  context = (turbo_flow_inbox_source_context_t *)base;
  *context = (turbo_flow_inbox_source_context_t){sizeof(*context),
                                                 TURBO_FLOW_INBOX_SOURCE_API_VERSION,
                                                 0u,
                                                 source->claim.record_id,
                                                 record->source_sequence,
                                                 source_id_offset,
                                                 record->source_id.len,
                                                 admission_id_offset,
                                                 record->admission_id.len,
                                                 correlation_offset,
                                                 record->correlation.len,
                                                 payload_offset,
                                                 record->payload.len,
                                                 retained_bytes};
  memcpy(base + source_id_offset, record->source_id.data, record->source_id.len);
  memcpy(base + admission_id_offset, record->admission_id.data, record->admission_id.len);
  if (record->correlation.len != 0u)
    memcpy(base + correlation_offset, record->correlation.data, record->correlation.len);
  if (record->payload.len != 0u)
    memcpy(base + payload_offset, record->payload.data, record->payload.len);
  mem_set_used(buffer, retained_bytes);

  turbo_flow_msg_init(&source->message);
  source->message_live = true;
  source->message.id = source->claim.record_id;
  source->message.ts_ns = record->timestamp_ns;
  source->message.type = record->message_type;
  source->message.flags = record->message_flags;
  source->message.buffer = buffer;
  source->message.payload = vstr_from_buf(base + payload_offset, record->payload.len);
  source->message.transport_context = context;
  status = turbo_flow_msg_copy_content_descriptor(&source->message, &record->content);
  if (status != SALTS_OK) {
    turbo_flow_msg_cleanup(&source->message);
    memset(&source->message, 0, sizeof(source->message));
    source->message_live = false;
  }
  return status;
}

static int flow_inbox_source_settle(turbo_flow_inbox_source_t *source,
                                    turbo_flow_inbox_source_result_t *result) {
  const bool completing = source->phase == FLOW_INBOX_SOURCE_SETTLE_COMPLETE;
  int status;
  if (!completing && source->phase != FLOW_INBOX_SOURCE_SETTLE_FAIL) return SALTS_EINVAL;
  status = completing
               ? turbo_flow_inbox_complete(source->config.inbox, &source->claim)
               : turbo_flow_inbox_fail(source->config.inbox, &source->claim, source->graph_status);
  source->settlement_status = status;
  if (status == SALTS_OK) {
    const uint64_t record_id = source->record_id;
    const int graph_status = source->graph_status;
    flow_inbox_source_result_set(
        source, completing ? TURBO_FLOW_INBOX_SOURCE_COMPLETED : TURBO_FLOW_INBOX_SOURCE_FAILED,
        result);
    flow_inbox_source_release(source);
    result->record_id = record_id;
    result->graph_status = graph_status;
    result->settlement_status = SALTS_OK;
    return graph_status;
  }
  if (status == SALTS_ECANCELED) {
    const uint64_t record_id = source->record_id;
    const int graph_status = source->graph_status;
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_OWNER_LOST_UNKNOWN, result);
    flow_inbox_source_release(source);
    result->record_id = record_id;
    result->graph_status = graph_status;
    result->settlement_status = SALTS_ECANCELED;
    return SALTS_ECANCELED;
  }
  if (status == SALTS_EALREADY) {
    source->phase = FLOW_INBOX_SOURCE_SETTLE_UNKNOWN;
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN, result);
    return status;
  }
  flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING, result);
  return status;
}

static int flow_inbox_source_fail_claim(turbo_flow_inbox_source_t *source, int graph_status) {
  turbo_flow_inbox_source_result_t ignored = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
  const int failure_status = graph_status == SALTS_OK ? SALTS_EPROTO : graph_status;
  int settlement_status;
  source->graph_status = failure_status;
  source->phase = FLOW_INBOX_SOURCE_SETTLE_FAIL;
  settlement_status = flow_inbox_source_settle(source, &ignored);
  return ignored.settlement_status == SALTS_OK ? failure_status : settlement_status;
}

int turbo_flow_inbox_source_request(turbo_flow_inbox_source_t *source) {
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t snapshot = TURBO_FLOW_RUN_RESULT_INIT;
  turbo_flow_inbox_source_result_t ignored = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
  cflow_publisher publisher = {0};
  int status;
  int cancel_status;
  if (!source) return SALTS_EINVAL;
  if (source->phase != FLOW_INBOX_SOURCE_IDLE) return SALTS_EBUSY;
  if (turbo_flow_state(source->config.flow) != TURBO_FLOW_STATE_STARTED) return SALTS_ESHUTDOWN;
  status = turbo_flow_inbox_claim(source->config.inbox, &source->claim);
  if (status != SALTS_OK) return status;
  source->record_id = source->claim.record_id;
  status = flow_inbox_source_message_build(source);
  if (status != SALTS_OK) return flow_inbox_source_fail_claim(source, status);
  if (!cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &source->message, 1u)) {
    turbo_flow_msg_cleanup(&source->message);
    memset(&source->message, 0, sizeof(source->message));
    source->message_live = false;
    return flow_inbox_source_fail_claim(source, SALTS_ENOMEM);
  }
  run_config.scheduler = source->config.scheduler;
  status = turbo_flow_run_open(source->config.flow, source->graph_source_name, &publisher,
                               &run_config, &source->run);
  if (status != SALTS_OK) {
    cflow_publisher_destroy(&publisher);
    turbo_flow_msg_cleanup(&source->message);
    memset(&source->message, 0, sizeof(source->message));
    source->message_live = false;
    return flow_inbox_source_fail_claim(source, status);
  }
  source->phase = FLOW_INBOX_SOURCE_GRAPH_RUNNING;
  status = turbo_flow_run_request(source->run, 1u);
  if (status == SALTS_OK) return SALTS_OK;

  if (turbo_flow_run_snapshot(source->run, &snapshot) == SALTS_OK &&
      snapshot.state != TURBO_FLOW_RUN_OPEN && snapshot.state != TURBO_FLOW_RUN_ACTIVE) {
    return SALTS_OK;
  }
  cancel_status = turbo_flow_run_cancel(source->run);
  if (cancel_status != SALTS_OK && cancel_status != SALTS_EALREADY) return cancel_status;
  if (cancel_status == SALTS_OK) source->request_failure_status = status;
  return turbo_flow_inbox_source_poll(source, &ignored);
}

int turbo_flow_inbox_source_poll(turbo_flow_inbox_source_t *source,
                                 turbo_flow_inbox_source_result_t *result) {
  turbo_flow_run_result_t snapshot = TURBO_FLOW_RUN_RESULT_INIT;
  int status;
  if (flow_inbox_source_result_prepare(result) != SALTS_OK || !source) return SALTS_EINVAL;
  if (source->phase == FLOW_INBOX_SOURCE_IDLE) return SALTS_ENOENT;
  if (source->phase == FLOW_INBOX_SOURCE_SETTLE_COMPLETE ||
      source->phase == FLOW_INBOX_SOURCE_SETTLE_FAIL) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING, result);
    return SALTS_EBUSY;
  }
  if (source->phase == FLOW_INBOX_SOURCE_SETTLE_UNKNOWN) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN, result);
    return SALTS_EBUSY;
  }
  status = turbo_flow_run_snapshot(source->run, &snapshot);
  if (status != SALTS_OK) {
    source->graph_status = status;
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE, result);
    return status;
  }
  if (snapshot.state == TURBO_FLOW_RUN_OPEN || snapshot.state == TURBO_FLOW_RUN_ACTIVE) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE, result);
    return SALTS_OK;
  }
  source->graph_status =
      snapshot.state == TURBO_FLOW_RUN_CANCELED && source->request_failure_status != SALTS_OK
          ? source->request_failure_status
          : snapshot.status;
  if (snapshot.state == TURBO_FLOW_RUN_COMPLETED && source->graph_status == SALTS_OK) {
    source->phase = FLOW_INBOX_SOURCE_SETTLE_COMPLETE;
  } else {
    if (source->graph_status == SALTS_OK) source->graph_status = SALTS_EPROTO;
    source->phase = FLOW_INBOX_SOURCE_SETTLE_FAIL;
  }
  return flow_inbox_source_settle(source, result);
}

int turbo_flow_inbox_source_cancel(turbo_flow_inbox_source_t *source,
                                   turbo_flow_inbox_source_result_t *result) {
  int status;
  if (flow_inbox_source_result_prepare(result) != SALTS_OK || !source) return SALTS_EINVAL;
  if (source->phase == FLOW_INBOX_SOURCE_IDLE) return SALTS_EALREADY;
  if (source->phase != FLOW_INBOX_SOURCE_GRAPH_RUNNING) {
    flow_inbox_source_result_set(source,
                                 source->phase == FLOW_INBOX_SOURCE_SETTLE_UNKNOWN
                                     ? TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN
                                     : TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING,
                                 result);
    return SALTS_EBUSY;
  }
  status = turbo_flow_run_cancel(source->run);
  if (status != SALTS_OK && status != SALTS_EALREADY) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE, result);
    return status;
  }
  if (status == SALTS_OK) source->request_failure_status = SALTS_ECANCELED;
  return turbo_flow_inbox_source_poll(source, result);
}

int turbo_flow_inbox_source_retry_settlement(turbo_flow_inbox_source_t *source,
                                             turbo_flow_inbox_source_result_t *result) {
  if (flow_inbox_source_result_prepare(result) != SALTS_OK || !source) return SALTS_EINVAL;
  if (source->phase == FLOW_INBOX_SOURCE_SETTLE_UNKNOWN) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN, result);
    return SALTS_EALREADY;
  }
  if (source->phase != FLOW_INBOX_SOURCE_SETTLE_COMPLETE &&
      source->phase != FLOW_INBOX_SOURCE_SETTLE_FAIL) {
    return SALTS_EINVAL;
  }
  return flow_inbox_source_settle(source, result);
}

int turbo_flow_inbox_source_reconcile_settlement(turbo_flow_inbox_source_t *source,
                                                 turbo_flow_inbox_source_result_t *result) {
  turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
  turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
  size_t failed_count = 0u;
  size_t history_count = 0u;
  bool failed_match;
  bool history_match;
  bool expected_complete;
  int status;
  if (flow_inbox_source_result_prepare(result) != SALTS_OK || !source) return SALTS_EINVAL;
  if (source->phase != FLOW_INBOX_SOURCE_SETTLE_UNKNOWN) return SALTS_EINVAL;
  flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN, result);
  expected_complete = source->graph_status == SALTS_OK;
  status = turbo_flow_inbox_scan_failed(source->config.inbox, source->record_id - 1u, &failed, 1u,
                                        &failed_count);
  if (status != SALTS_OK) return status;
  status = turbo_flow_inbox_scan_history(source->config.inbox, source->record_id - 1u, &history, 1u,
                                         &history_count);
  if (status != SALTS_OK) return status;
  failed_match = failed_count == 1u && failed.record_id == source->record_id;
  history_match = history_count == 1u && history.record_id == source->record_id;
  if (failed_match && history_match) return SALTS_EPROTO;
  if (!failed_match && !history_match) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN, result);
    return SALTS_EBUSY;
  }
  if (failed_match && failed.kind == TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN) {
    const uint64_t record_id = source->record_id;
    const int graph_status = source->graph_status;
    source->claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    flow_inbox_source_release(source);
    result->state = TURBO_FLOW_INBOX_SOURCE_OWNER_LOST_UNKNOWN;
    result->record_id = record_id;
    result->graph_status = graph_status;
    result->settlement_status = SALTS_ECANCELED;
    return SALTS_ECANCELED;
  }
  if ((expected_complete &&
       (!history_match || history.kind != TURBO_FLOW_INBOX_TERMINAL_COMPLETED)) ||
      (!expected_complete &&
       ((!failed_match || failed.kind != TURBO_FLOW_INBOX_FAILURE_PROCESSING ||
         failed.status != source->graph_status) &&
        (!history_match || history.kind != TURBO_FLOW_INBOX_TERMINAL_DISCARDED)))) {
    flow_inbox_source_result_set(source, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN, result);
    return SALTS_EPROTO;
  }
  {
    const uint64_t record_id = source->record_id;
    const int graph_status = source->graph_status;
    source->claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    flow_inbox_source_release(source);
    result->state =
        expected_complete ? TURBO_FLOW_INBOX_SOURCE_COMPLETED : TURBO_FLOW_INBOX_SOURCE_FAILED;
    result->record_id = record_id;
    result->graph_status = graph_status;
    result->settlement_status = SALTS_OK;
    return graph_status;
  }
}

int turbo_flow_inbox_source_destroy(turbo_flow_inbox_source_t *source) {
  if (!source) return SALTS_EINVAL;
  if (source->phase != FLOW_INBOX_SOURCE_IDLE) return SALTS_EBUSY;
  tstr_free(source->graph_source_name);
  free(source);
  return SALTS_OK;
}

const turbo_flow_inbox_source_context_t *
turbo_flow_inbox_source_context(const turbo_flow_msg_t *message) {
  const turbo_flow_inbox_source_context_t *context;
  const char *base;
  size_t used;
  size_t expected;
  if (!message || !message->buffer || !message->transport_context) return NULL;
  base = mem_buffer_const_data(message->buffer);
  used = mem_buffer_used(message->buffer);
  if (!base || used < sizeof(*context) || message->transport_context != base ||
      (uintptr_t)base % _Alignof(turbo_flow_inbox_source_context_t) != 0u)
    return NULL;
  context = (const turbo_flow_inbox_source_context_t *)base;
  if (context->size != sizeof(*context) ||
      context->version != TURBO_FLOW_INBOX_SOURCE_API_VERSION || context->reserved != 0u ||
      context->record_id == 0u || context->source_id_size == 0u ||
      context->admission_id_size == 0u || context->source_id_offset != sizeof(*context)) {
    return NULL;
  }
  if (flow_inbox_source_add_size(context->source_id_offset, context->source_id_size, &expected) !=
          SALTS_OK ||
      context->admission_id_offset != expected ||
      flow_inbox_source_add_size(context->admission_id_offset, context->admission_id_size,
                                 &expected) != SALTS_OK ||
      context->correlation_offset != expected ||
      flow_inbox_source_add_size(context->correlation_offset, context->correlation_size,
                                 &expected) != SALTS_OK ||
      context->payload_offset != expected ||
      flow_inbox_source_add_size(context->payload_offset, context->payload_size, &expected) !=
          SALTS_OK ||
      context->buffer_size != expected || expected != used || message->id != context->record_id) {
    return NULL;
  }
  return context;
}

vstr turbo_flow_inbox_source_source_id(const turbo_flow_msg_t *message) {
  const turbo_flow_inbox_source_context_t *context = turbo_flow_inbox_source_context(message);
  if (!context) return vstr_from_buf(NULL, 0u);
  return vstr_from_buf(mem_buffer_const_data(message->buffer) + context->source_id_offset,
                       context->source_id_size);
}

vstr turbo_flow_inbox_source_admission_id(const turbo_flow_msg_t *message) {
  const turbo_flow_inbox_source_context_t *context = turbo_flow_inbox_source_context(message);
  if (!context) return vstr_from_buf(NULL, 0u);
  return vstr_from_buf(mem_buffer_const_data(message->buffer) + context->admission_id_offset,
                       context->admission_id_size);
}

vstr turbo_flow_inbox_source_correlation(const turbo_flow_msg_t *message) {
  const turbo_flow_inbox_source_context_t *context = turbo_flow_inbox_source_context(message);
  if (!context) return vstr_from_buf(NULL, 0u);
  return vstr_from_buf(mem_buffer_const_data(message->buffer) + context->correlation_offset,
                       context->correlation_size);
}
