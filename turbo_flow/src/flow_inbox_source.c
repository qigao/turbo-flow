#include "turbo_flow_inbox_source.h"
#include "flow_inbox_driver_internal.h"

#include <salts_error.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_inbox_source_s {
  flow_inbox_driver_t *driver;
};

static int flow_inbox_source_add_size(size_t left, size_t right, size_t *out) {
  if (!out || right > SIZE_MAX - left) return SALTS_ERANGE;
  *out = left + right;
  return SALTS_OK;
}

int turbo_flow_inbox_source_create(const turbo_flow_inbox_source_config_t *config,
                                   turbo_flow_inbox_source_t **source_out) {
  flow_inbox_driver_config_t driver_config;
  turbo_flow_inbox_source_t *source;
  int stage_index;
  int rc;
  if (!source_out || *source_out || !config || config->size != sizeof(*config) ||
      config->version != TURBO_FLOW_INBOX_SOURCE_API_VERSION || !config->graph_source_name ||
      config->graph_source_name[0] == '\0' ||
      strlen(config->graph_source_name) > TURBO_FLOW_CONTROL_NAME_MAX ||
      config->max_message_bytes < sizeof(turbo_flow_inbox_source_context_t) + 1u)
    return SALTS_EINVAL;
  stage_index = config->flow ? turbo_flow_find_stage(config->flow, config->graph_source_name) : -1;
  if (stage_index < 0) return SALTS_EINVAL;
  source = (turbo_flow_inbox_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  driver_config = (flow_inbox_driver_config_t){
      config->inbox, config->flow, FLOW_INBOX_DRIVER_SOURCE,
      (uint32_t)stage_index, config->scheduler, config->max_message_bytes,
      NULL, NULL, NULL};
  rc = flow_inbox_driver_create(&driver_config, &source->driver);
  if (rc != SALTS_OK) { free(source); return rc; }
  *source_out = source;
  return SALTS_OK;
}

int turbo_flow_inbox_source_request(turbo_flow_inbox_source_t *source) {
  return source ? flow_inbox_driver_request(source->driver) : SALTS_EINVAL;
}
int turbo_flow_inbox_source_poll(turbo_flow_inbox_source_t *source,
                                 turbo_flow_inbox_source_result_t *result) {
  return source ? flow_inbox_driver_poll(source->driver, result) : SALTS_EINVAL;
}
int turbo_flow_inbox_source_cancel(turbo_flow_inbox_source_t *source,
                                   turbo_flow_inbox_source_result_t *result) {
  return source ? flow_inbox_driver_cancel(source->driver, result) : SALTS_EINVAL;
}
int turbo_flow_inbox_source_retry_settlement(turbo_flow_inbox_source_t *source,
                                             turbo_flow_inbox_source_result_t *result) {
  return source ? flow_inbox_driver_retry_settlement(source->driver, result) : SALTS_EINVAL;
}
int turbo_flow_inbox_source_reconcile_settlement(turbo_flow_inbox_source_t *source,
                                                 turbo_flow_inbox_source_result_t *result) {
  return source ? flow_inbox_driver_reconcile_settlement(source->driver, result) : SALTS_EINVAL;
}
int turbo_flow_inbox_source_destroy(turbo_flow_inbox_source_t *source) {
  int rc;
  if (!source) return SALTS_EINVAL;
  rc = flow_inbox_driver_destroy(source->driver);
  if (rc == SALTS_OK) free(source);
  return rc;
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
