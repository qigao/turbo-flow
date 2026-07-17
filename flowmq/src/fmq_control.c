#include "turbo_flow_fmq_control.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_fmq_control_record_s {
  turbo_flow_fmq_control_request_t request;
  turbo_flow_fmq_control_reply_t reply;
} flow_fmq_control_record_t;

struct turbo_flow_fmq_control_service_s {
  turbo_flow_t *target_flow;
  turbo_flow_fmq_control_config_t config;
  flow_fmq_control_record_t *history;
  size_t history_count;
  turbo_mutex_t mutex;
};

static void flow_fmq_control_error(turbo_flow_fmq_control_reply_t *reply, int status,
                                   const char *message) {
  reply->status = status;
  reply->error.code = status;
  (void)snprintf(reply->error.message, sizeof(reply->error.message), "%s",
                 message ? message : "control request failed");
}

static int flow_fmq_control_text_same(const char *left, const char *right) {
  return strcmp(left, right) == 0;
}

static int flow_fmq_control_request_same(const turbo_flow_fmq_control_request_t *left,
                                         const turbo_flow_fmq_control_request_t *right) {
  const turbo_flow_fmq_control_command_t *a = &left->command;
  const turbo_flow_fmq_control_command_t *b = &right->command;
  return left->version == right->version && left->operation == right->operation &&
         left->request_id == right->request_id &&
         flow_fmq_control_text_same(left->target, right->target) &&
         flow_fmq_control_text_same(left->idempotency_key, right->idempotency_key) &&
         a->kind == b->kind && a->timeout_ms == b->timeout_ms && a->pool_kind == b->pool_kind &&
         a->parallelism == b->parallelism && a->adapter_kind == b->adapter_kind &&
         a->endpoint_port == b->endpoint_port && flow_fmq_control_text_same(a->target, b->target) &&
         flow_fmq_control_text_same(a->condition, b->condition) &&
         flow_fmq_control_text_same(a->endpoint_host, b->endpoint_host) &&
         flow_fmq_control_text_same(a->endpoint_path, b->endpoint_path);
}

static int flow_fmq_control_history_lookup(turbo_flow_fmq_control_service_t *service,
                                           const turbo_flow_fmq_control_request_t *request,
                                           turbo_flow_fmq_control_reply_t *reply) {
  for (size_t i = 0u; i < service->history_count; ++i) {
    flow_fmq_control_record_t *record = &service->history[i];
    if (strcmp(record->request.idempotency_key, request->idempotency_key) != 0) continue;
    if (!flow_fmq_control_request_same(&record->request, request)) return TURBO_EPROTO;
    *reply = record->reply;
    reply->replayed = 1;
    return TURBO_OK;
  }
  return TURBO_ENOENT;
}

static int flow_fmq_control_history_record(turbo_flow_fmq_control_service_t *service,
                                           const turbo_flow_fmq_control_request_t *request,
                                           const turbo_flow_fmq_control_reply_t *reply) {
  flow_fmq_control_record_t *record;
  if (service->history_count >= service->config.dedup_capacity) return TURBO_ENOSPC;
  record = &service->history[service->history_count++];
  record->request = *request;
  record->reply = *reply;
  return TURBO_OK;
}

int turbo_flow_fmq_control_service_create(turbo_flow_t *target_flow,
                                          const turbo_flow_fmq_control_config_t *config,
                                          turbo_flow_fmq_control_service_t **out) {
  turbo_flow_fmq_control_service_t *service;
  if (out) *out = NULL;
  if (!target_flow || !config || config->size < sizeof(*config) || !out ||
      config->version != TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION || config->target[0] == '\0' ||
      !memchr(config->target, '\0', sizeof(config->target)) || config->dedup_capacity == 0u ||
      config->dedup_capacity > TURBO_FLOW_FMQ_CONTROL_DEDUP_MAX ||
      config->max_request_bytes < TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE ||
      config->max_request_bytes > TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE) {
    return TURBO_EINVAL;
  }
  service = (turbo_flow_fmq_control_service_t *)calloc(1u, sizeof(*service));
  if (!service) return TURBO_ENOMEM;
  service->history =
      (flow_fmq_control_record_t *)calloc(config->dedup_capacity, sizeof(*service->history));
  if (!service->history) {
    free(service);
    return TURBO_ENOMEM;
  }
  service->target_flow = target_flow;
  service->config = *config;
  turbo_mutex_init(&service->mutex);
  *out = service;
  return TURBO_OK;
}

void turbo_flow_fmq_control_service_destroy(turbo_flow_fmq_control_service_t *service) {
  if (!service) return;
  turbo_mutex_destroy(&service->mutex);
  free(service->history);
  free(service);
}

int turbo_flow_fmq_control_service_execute(turbo_flow_fmq_control_service_t *service,
                                           const uint8_t *request_bytes, size_t request_len,
                                           turbo_flow_fmq_control_reply_t *reply) {
  turbo_flow_fmq_control_request_t request = TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
  turbo_flow_fmq_control_reply_t result = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
  turbo_flow_control_command_t local_command;
  int rc;
  if (!service || (!request_bytes && request_len > 0u) || !reply || reply->size < sizeof(*reply))
    return TURBO_EINVAL;
  if (request_len > service->config.max_request_bytes) {
    flow_fmq_control_error(&result, TURBO_EMSGSIZE, "control request exceeds configured limit");
    *reply = result;
    return TURBO_OK;
  }
  rc = request_len == 0u
           ? TURBO_EPROTO
           : turbo_flow_fmq_control_request_decode(request_bytes, request_len, &request);
  if (rc != TURBO_OK) {
    flow_fmq_control_error(&result, TURBO_EPROTO, "malformed or unsupported Control V1 request");
    *reply = result;
    return TURBO_OK;
  }
  result.request_id = request.request_id;
  turbo_mutex_lock(&service->mutex);
  if (strcmp(request.target, service->config.target) != 0) {
    flow_fmq_control_error(&result, TURBO_ENOENT, "control target is not owned by this service");
    goto done;
  }
  if (request.operation == TURBO_FLOW_FMQ_CONTROL_STATUS) {
    rc = turbo_flow_runtime_snapshot(service->target_flow, &result.runtime);
    if (rc != TURBO_OK) flow_fmq_control_error(&result, rc, "failed to read target status");
    goto done;
  }
  rc = flow_fmq_control_history_lookup(service, &request, &result);
  if (rc == TURBO_OK) goto done;
  if (rc == TURBO_EPROTO) {
    result.request_id = request.request_id;
    flow_fmq_control_error(&result, rc, "idempotency key was reused for a different request");
    goto done;
  }
  if (service->history_count >= service->config.dedup_capacity) {
    flow_fmq_control_error(&result, TURBO_ENOSPC, "control idempotency history is full");
    goto done;
  }
  memset(&local_command, 0, sizeof(local_command));
  local_command.size = sizeof(local_command);
  local_command.kind = request.command.kind;
  local_command.timeout_ms = request.command.timeout_ms;
  memcpy(local_command.target, request.command.target, sizeof(local_command.target));
  local_command.pool_kind = request.command.pool_kind;
  local_command.parallelism = request.command.parallelism;
  local_command.adapter.size = sizeof(local_command.adapter);
  local_command.adapter.kind = request.command.adapter_kind;
  memcpy(local_command.endpoint_host, request.command.endpoint_host,
         sizeof(local_command.endpoint_host));
  memcpy(local_command.endpoint_path, request.command.endpoint_path,
         sizeof(local_command.endpoint_path));
  local_command.adapter.endpoint.port = request.command.endpoint_port;
  memcpy(local_command.condition, request.command.condition, sizeof(local_command.condition));
  rc = turbo_flow_control_execute_ex(service->target_flow, &local_command, NULL, &result.error);
  result.status = rc;
  (void)turbo_flow_runtime_snapshot(service->target_flow, &result.runtime);
  if (flow_fmq_control_history_record(service, &request, &result) != TURBO_OK) {
    flow_fmq_control_error(&result, TURBO_ENOSPC, "control idempotency history is full");
  }

done:
  turbo_mutex_unlock(&service->mutex);
  *reply = result;
  return TURBO_OK;
}

int turbo_flow_fmq_control_stage(turbo_flow_msg_t *msg, void *ctx) {
  turbo_flow_fmq_control_service_t *service = (turbo_flow_fmq_control_service_t *)ctx;
  turbo_flow_fmq_control_reply_t reply = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
  uint8_t encoded[TURBO_FLOW_FMQ_CONTROL_REPLY_MAX_SIZE];
  tstr_t payload;
  size_t encoded_len = 0u;
  int rc;
  if (!msg || !service || (!msg->payload.data && msg->payload.len > 0u)) return TURBO_EINVAL;
  rc = turbo_flow_fmq_control_service_execute(service, (const uint8_t *)msg->payload.data,
                                              msg->payload.len, &reply);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_fmq_control_reply_encode(&reply, encoded, sizeof(encoded), &encoded_len);
  if (rc != TURBO_OK) return rc;
  payload = tstr_new_len(encoded, encoded_len);
  if (!payload) return TURBO_ENOMEM;
  turbo_flow_msg_clear_content(msg);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
  return TURBO_OK;
}
