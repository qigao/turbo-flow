#include "turbo_flow_turbodb.h"

#include <cflow/publishers.h>
#include <cstl/vec.h>
#include <salts_error.h>
#include <salts_str.h>

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum outbox_slot_phase_e {
  OUTBOX_SLOT_EMPTY = 0,
  OUTBOX_SLOT_GRAPH_RUNNING,
  OUTBOX_SLOT_DEAD_LETTER_PENDING,
  OUTBOX_SLOT_ACK_PENDING,
  OUTBOX_SLOT_REQUEUE_PENDING
} outbox_slot_phase_t;

typedef struct outbox_slot_s {
  outbox_slot_phase_t phase;
  uint64_t token;
  size_t retained_bytes;
  int graph_status;
  int source_failure_status;
  const char *source_failure_stage;
  turbo_flow_msg_t message;
  turbo_flow_run_t *run;
} outbox_slot_t;

struct turbo_flow_turbodb_outbox_source_s {
  turbo_flow_turbodb_outbox_source_config_t config;
  tstr source_name;
  turbo_flow_turbodb_outbox_source_state_t state;
  int status;
  vec_t slots;
  size_t outstanding_demand;
  size_t in_flight_messages;
  size_t in_flight_bytes;
  uint64_t next_message_id;
  bool message_id_exhausted;
  uint64_t fetched;
  uint64_t acknowledged;
  uint64_t requeued;
  uint64_t dead_lettered;
  uint64_t data_loss_events;
  char error_stage[48];
  cflow_waitable fetch_waitable;
  bool fetch_wait_armed;
  atomic_bool fetch_wake_pending;
};

turbo_flow_turbodb_outbox_source_config_t turbo_flow_turbodb_outbox_source_config_default(void) {
  turbo_flow_turbodb_outbox_source_config_t config;
  memset(&config, 0, sizeof(config));
  config.size = sizeof(config);
  config.version = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION;
  config.provider =
      (turbo_flow_turbodb_outbox_provider_ops_t)TURBO_FLOW_TURBODB_OUTBOX_PROVIDER_OPS_INIT;
  config.permanent_failure_policy = TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_FAIL_SOURCE;
  config.shutdown_policy = TURBO_FLOW_TURBODB_OUTBOX_SHUTDOWN_REQUEUE;
  config.fetch_count = TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_FETCH_COUNT;
  config.in_flight_messages = TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_IN_FLIGHT_MESSAGES;
  config.in_flight_bytes = TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_IN_FLIGHT_BYTES;
  config.max_identity_bytes = TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_MAX_IDENTITY_BYTES;
  config.max_payload_bytes = TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_MAX_PAYLOAD_BYTES;
  config.max_delivery_attempts = TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_MAX_DELIVERY_ATTEMPTS;
  config.first_message_id = 1u;
  return config;
}

static int outbox_source_config_valid(const turbo_flow_turbodb_outbox_source_config_t *config) {
  size_t max_record_bytes;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION || !config->flow ||
      !config->source_name || config->source_name[0] == '\0' ||
      config->provider.size < sizeof(config->provider) || !config->provider.fetch ||
      !config->provider.cancel_fetch || !config->provider.acknowledge ||
      !config->provider.requeue || !config->classify_failure || config->fetch_count == 0u ||
      config->in_flight_messages == 0u || config->in_flight_bytes == 0u ||
      config->in_flight_messages > SIZE_MAX / sizeof(outbox_slot_t) ||
      config->max_identity_bytes == 0u || config->max_payload_bytes == 0u ||
      config->max_delivery_attempts == 0u || config->first_message_id == 0u ||
      config->shutdown_policy != TURBO_FLOW_TURBODB_OUTBOX_SHUTDOWN_REQUEUE ||
      (config->permanent_failure_policy != TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_FAIL_SOURCE &&
       config->permanent_failure_policy != TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER) ||
      (config->permanent_failure_policy == TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER &&
       !config->provider.dead_letter)) {
    return 0;
  }
  if (config->max_identity_bytes > SIZE_MAX - config->max_payload_bytes) return 0;
  max_record_bytes = config->max_identity_bytes + config->max_payload_bytes;
  if (max_record_bytes > SIZE_MAX - sizeof(turbo_flow_turbodb_outbox_message_context_t)) return 0;
  if (config->in_flight_bytes < sizeof(turbo_flow_turbodb_outbox_message_context_t) + 1u) return 0;
  return turbo_flow_state(config->flow) == TURBO_FLOW_STATE_STARTED;
}

int turbo_flow_turbodb_outbox_source_open(const turbo_flow_turbodb_outbox_source_config_t *config,
                                          turbo_flow_turbodb_outbox_source_t **source_out) {
  turbo_flow_turbodb_outbox_source_t *source;
  if (!source_out) return SALTS_EINVAL;
  *source_out = NULL;
  if (!outbox_source_config_valid(config)) return SALTS_EINVAL;
  source = (turbo_flow_turbodb_outbox_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  source->source_name = tstr_dup(config->source_name);
  if (!source->source_name) {
    free(source);
    return SALTS_ENOMEM;
  }
  source->config = *config;
  source->config.source_name = source->source_name;
  source->state = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING;
  source->status = SALTS_OK;
  source->next_message_id = config->first_message_id;
  atomic_init(&source->fetch_wake_pending, false);
  if (vec_init_bytes(&source->slots, sizeof(outbox_slot_t), _Alignof(outbox_slot_t),
                     config->in_flight_messages) != STL_OK ||
      vec_resize(&source->slots, config->in_flight_messages) != STL_OK) {
    vec_destroy(&source->slots);
    tstr_free(source->source_name);
    free(source);
    return SALTS_ENOMEM;
  }
  for (size_t index = 0u; index < config->in_flight_messages; ++index) {
    outbox_slot_t *slot = (outbox_slot_t *)vec_at(&source->slots, index);
    if (slot) memset(slot, 0, sizeof(*slot));
  }
  *source_out = source;
  return SALTS_OK;
}

static outbox_slot_t *outbox_source_slot_at(turbo_flow_turbodb_outbox_source_t *source,
                                            size_t index) {
  return (outbox_slot_t *)vec_at(&source->slots, index);
}

static void outbox_source_error(turbo_flow_turbodb_outbox_source_t *source, int status,
                                const char *stage, bool terminal) {
  if (!source) return;
  source->status = status;
  (void)snprintf(source->error_stage, sizeof(source->error_stage), "%s", stage ? stage : "");
  if (terminal) source->state = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED;
}

static int outbox_record_validate(const turbo_flow_turbodb_outbox_source_t *source,
                                  const turbo_flow_turbodb_outbox_record_t *record,
                                  size_t retained_budget, size_t *retained_bytes) {
  size_t variable_bytes;
  if (!source || !record || !retained_bytes || record->size < sizeof(*record) ||
      record->version != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION || record->token == 0u ||
      record->raft_index == 0u || record->term == 0u || record->delivery_attempt == 0u ||
      record->delivery_attempt > source->config.max_delivery_attempts ||
      record->identity.len == 0u || !record->identity.data ||
      record->identity.len > source->config.max_identity_bytes ||
      record->payload.len > source->config.max_payload_bytes ||
      (record->payload.len != 0u && !record->payload.data)) {
    return SALTS_EINVAL;
  }
  if (record->identity.len > SIZE_MAX - record->payload.len) return SALTS_ERANGE;
  variable_bytes = record->identity.len + record->payload.len;
  if (variable_bytes > SIZE_MAX - sizeof(turbo_flow_turbodb_outbox_message_context_t))
    return SALTS_ERANGE;
  *retained_bytes = sizeof(turbo_flow_turbodb_outbox_message_context_t) + variable_bytes;
  if (*retained_bytes > retained_budget) return SALTS_ENOSPC;
  return SALTS_OK;
}

static int outbox_message_build(turbo_flow_turbodb_outbox_source_t *source,
                                const turbo_flow_turbodb_outbox_record_t *record,
                                size_t retained_bytes, turbo_flow_msg_t *message) {
  turbo_flow_turbodb_outbox_message_context_t *context;
  char *base;
  mem_buffer_t *buffer;
  if (!source || !record || !message) return SALTS_EINVAL;
  buffer = mem_get_buffer(mem_global(), retained_bytes);
  if (!buffer) return SALTS_ENOMEM;
  base = mem_buffer_data(buffer);
  context = (turbo_flow_turbodb_outbox_message_context_t *)base;
  memset(context, 0, sizeof(*context));
  context->size = sizeof(*context);
  context->version = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION;
  context->raft_index = record->raft_index;
  context->term = record->term;
  context->delivery_attempt = record->delivery_attempt;
  context->identity_size = record->identity.len;
  memcpy(base + sizeof(*context), record->identity.data, record->identity.len);
  if (record->payload.len != 0u) {
    memcpy(base + sizeof(*context) + record->identity.len, record->payload.data,
           record->payload.len);
  }
  mem_set_used(buffer, retained_bytes);
  turbo_flow_msg_init(message);
  message->id = source->next_message_id;
  message->type = source->config.message_type;
  message->flags = source->config.message_flags;
  message->buffer = buffer;
  message->payload =
      vstr_from_buf(base + sizeof(*context) + record->identity.len, record->payload.len);
  message->transport_context = context;
  return SALTS_OK;
}

static void outbox_slot_release(turbo_flow_turbodb_outbox_source_t *source, outbox_slot_t *slot) {
  if (!source || !slot || slot->phase == OUTBOX_SLOT_EMPTY) return;
  turbo_flow_run_close(slot->run);
  turbo_flow_msg_cleanup(&slot->message);
  assert(source->in_flight_messages != 0u);
  assert(source->in_flight_bytes >= slot->retained_bytes);
  --source->in_flight_messages;
  source->in_flight_bytes -= slot->retained_bytes;
  memset(slot, 0, sizeof(*slot));
}

static int outbox_source_settle_one(turbo_flow_turbodb_outbox_source_t *source, bool *progressed) {
  size_t count = vec_size(&source->slots);
  if (progressed) *progressed = false;
  for (size_t index = 0u; index < count; ++index) {
    outbox_slot_t *slot = outbox_source_slot_at(source, index);
    if (!slot || slot->phase == OUTBOX_SLOT_EMPTY) continue;
    if (slot->phase == OUTBOX_SLOT_GRAPH_RUNNING) {
      turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
      int status = turbo_flow_run_snapshot(slot->run, &result);
      if (status != SALTS_OK) {
        outbox_source_error(source, status, "graph_snapshot", true);
        return status;
      }
      if (result.state == TURBO_FLOW_RUN_OPEN || result.state == TURBO_FLOW_RUN_ACTIVE) continue;
      slot->graph_status = result.status;
      if (result.state == TURBO_FLOW_RUN_COMPLETED && result.status == SALTS_OK) {
        slot->phase = OUTBOX_SLOT_ACK_PENDING;
      } else {
        turbo_flow_turbodb_outbox_failure_disposition_t disposition =
            source->config.classify_failure(
                source->config.policy_ctx,
                turbo_flow_turbodb_outbox_message_context(&slot->message), result.status);
        if (disposition != TURBO_FLOW_TURBODB_OUTBOX_FAILURE_RETRYABLE &&
            disposition != TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT) {
          slot->phase = OUTBOX_SLOT_REQUEUE_PENDING;
          slot->source_failure_status = SALTS_EPROTO;
          slot->source_failure_stage = "classify_failure";
        } else if (disposition == TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT &&
                   source->config.permanent_failure_policy ==
                       TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER) {
          slot->phase = OUTBOX_SLOT_DEAD_LETTER_PENDING;
        } else if (disposition == TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT) {
          slot->phase = OUTBOX_SLOT_REQUEUE_PENDING;
          slot->source_failure_status = result.status == SALTS_OK ? SALTS_EPROTO : result.status;
          slot->source_failure_stage = "permanent_failure";
        } else {
          slot->phase = OUTBOX_SLOT_REQUEUE_PENDING;
        }
      }
    }
    if (slot->phase == OUTBOX_SLOT_DEAD_LETTER_PENDING) {
      int status = source->config.provider.dead_letter(
          source->config.provider_ctx, slot->token,
          turbo_flow_turbodb_outbox_message_context(&slot->message), slot->graph_status);
      if (status != SALTS_OK) {
        outbox_source_error(source, status, "dead_letter", false);
        return status;
      }
      ++source->dead_lettered;
      slot->phase = OUTBOX_SLOT_ACK_PENDING;
      if (progressed) *progressed = true;
      source->status = SALTS_OK;
      source->error_stage[0] = '\0';
      return SALTS_OK;
    }
    if (slot->phase == OUTBOX_SLOT_ACK_PENDING) {
      int status = source->config.provider.acknowledge(source->config.provider_ctx, slot->token);
      if (status != SALTS_OK) {
        outbox_source_error(source, status, "acknowledge", false);
        return status;
      }
      ++source->acknowledged;
      outbox_slot_release(source, slot);
    } else if (slot->phase == OUTBOX_SLOT_REQUEUE_PENDING) {
      int source_failure_status = slot->source_failure_status;
      const char *source_failure_stage = slot->source_failure_stage;
      int status = source->config.provider.requeue(source->config.provider_ctx, slot->token);
      if (status != SALTS_OK) {
        outbox_source_error(source, status, "requeue", false);
        return status;
      }
      ++source->requeued;
      outbox_slot_release(source, slot);
      if (source_failure_status != SALTS_OK) {
        outbox_source_error(source, source_failure_status, source_failure_stage, true);
      }
    }
    if (progressed) *progressed = true;
    if (source->state != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED) {
      source->status = SALTS_OK;
      source->error_stage[0] = '\0';
    }
    return SALTS_OK;
  }
  return SALTS_OK;
}

static outbox_slot_t *outbox_source_free_slot(turbo_flow_turbodb_outbox_source_t *source,
                                              size_t *free_slots) {
  outbox_slot_t *result = NULL;
  size_t count = vec_size(&source->slots);
  *free_slots = 0u;
  for (size_t index = 0u; index < count; ++index) {
    outbox_slot_t *slot = outbox_source_slot_at(source, index);
    if (slot && slot->phase == OUTBOX_SLOT_EMPTY) {
      if (!result) result = slot;
      ++*free_slots;
    }
  }
  return result;
}

static bool outbox_source_token_active(turbo_flow_turbodb_outbox_source_t *source, uint64_t token) {
  for (size_t index = 0u; index < vec_size(&source->slots); ++index) {
    outbox_slot_t *slot = outbox_source_slot_at(source, index);
    if (slot && slot->phase != OUTBOX_SLOT_EMPTY && slot->token == token) return true;
  }
  return false;
}

static int outbox_source_requeue_rejected(turbo_flow_turbodb_outbox_source_t *source,
                                          outbox_slot_t *slot, uint64_t token, int failure_status,
                                          const char *stage) {
  int status;
  if (!slot || token == 0u) {
    outbox_source_error(source, failure_status, stage, true);
    return failure_status;
  }
  slot->phase = OUTBOX_SLOT_REQUEUE_PENDING;
  slot->token = token;
  slot->source_failure_status = failure_status;
  slot->source_failure_stage = stage;
  ++source->in_flight_messages;
  status = source->config.provider.requeue(source->config.provider_ctx, token);
  if (status != SALTS_OK) {
    outbox_source_error(source, status, "requeue_rejected", false);
    return status;
  }
  ++source->requeued;
  outbox_slot_release(source, slot);
  outbox_source_error(source, failure_status, stage, true);
  return failure_status;
}

static void outbox_source_fetch_wake(void *ctx) {
  turbo_flow_turbodb_outbox_source_t *source = (turbo_flow_turbodb_outbox_source_t *)ctx;
  if (source) atomic_store_explicit(&source->fetch_wake_pending, true, memory_order_release);
}

static void outbox_source_unarm_fetch(turbo_flow_turbodb_outbox_source_t *source) {
  cflow_waitable waitable;
  if (!source || !source->fetch_wait_armed) return;
  waitable = source->fetch_waitable;
  source->fetch_waitable = (cflow_waitable){0};
  source->fetch_wait_armed = false;
  cflow_waitable_cancel(&waitable);
}

static int outbox_source_fetch_one(turbo_flow_turbodb_outbox_source_t *source, bool *progressed) {
  turbo_flow_turbodb_outbox_fetch_budget_t budget;
  turbo_flow_turbodb_outbox_fetch_step_t step;
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  cflow_publisher publisher = {0};
  outbox_slot_t *slot;
  size_t free_slots;
  size_t retained_bytes = 0u;
  size_t remaining_bytes;
  int status;
  if (progressed) *progressed = false;
  slot = outbox_source_free_slot(source, &free_slots);
  if (!slot || source->outstanding_demand == 0u ||
      source->in_flight_bytes >= source->config.in_flight_bytes)
    return SALTS_OK;
  if (source->message_id_exhausted || source->fetched == UINT64_MAX)
    return outbox_source_requeue_rejected(source, NULL, 0u, SALTS_ERANGE, "message_id");
  remaining_bytes = source->config.in_flight_bytes - source->in_flight_bytes;
  budget = (turbo_flow_turbodb_outbox_fetch_budget_t){
      sizeof(budget),
      TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION,
      source->outstanding_demand < source->config.fetch_count ? source->outstanding_demand
                                                              : source->config.fetch_count,
      remaining_bytes,
      source->config.max_identity_bytes,
      source->config.max_payload_bytes};
  if (budget.max_records > free_slots) budget.max_records = free_slots;
  step = source->config.provider.fetch(source->config.provider_ctx, &budget);
  if (step.size < sizeof(step) || step.version != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION)
    return outbox_source_requeue_rejected(source, NULL, 0u, SALTS_EPROTO, "fetch_step");
  if (step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD && step.record.token != 0u &&
      outbox_source_token_active(source, step.record.token)) {
    outbox_source_error(source, SALTS_EPROTO, "duplicate_token", true);
    return SALTS_EPROTO;
  }
  if ((step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_IDLE ||
       step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_WAIT ||
       step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD) &&
      step.status != SALTS_OK) {
    return outbox_source_requeue_rejected(
        source, step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD ? slot : NULL,
        step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD ? step.record.token : 0u, SALTS_EPROTO,
        "fetch_status");
  }
  if (step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_IDLE) return SALTS_OK;
  if (step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_DATA_LOSS) {
    ++source->data_loss_events;
    outbox_source_error(source, step.status == SALTS_OK ? SALTS_ENOENT : step.status, "data_loss",
                        true);
    return source->status;
  }
  if (step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_ERROR) {
    outbox_source_error(source, step.status == SALTS_OK ? SALTS_EIO : step.status, "fetch", true);
    return source->status;
  }
  if (step.kind == TURBO_FLOW_TURBODB_OUTBOX_FETCH_WAIT) {
    cflow_waker waker = {outbox_source_fetch_wake, source};
    if (step.status != SALTS_OK || !cflow_waitable_valid(&step.waitable) ||
        source->fetch_wait_armed) {
      return outbox_source_requeue_rejected(source, NULL, 0u, SALTS_EPROTO, "fetch_wait");
    }
    atomic_store_explicit(&source->fetch_wake_pending, false, memory_order_release);
    source->fetch_waitable = step.waitable;
    source->fetch_wait_armed = true;
    if (!cflow_waitable_arm(&source->fetch_waitable, waker)) {
      source->fetch_waitable = (cflow_waitable){0};
      source->fetch_wait_armed = false;
      return outbox_source_requeue_rejected(source, NULL, 0u, SALTS_EPROTO, "fetch_wait_arm");
    }
    source->state = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_WAITING;
    if (progressed) *progressed = true;
    return SALTS_OK;
  }
  if (step.kind != TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD)
    return outbox_source_requeue_rejected(source, NULL, 0u, SALTS_EPROTO, "fetch_kind");
  status = outbox_record_validate(source, &step.record, remaining_bytes, &retained_bytes);
  if (status != SALTS_OK)
    return outbox_source_requeue_rejected(source, slot, step.record.token, status, "record");
  status = outbox_message_build(source, &step.record, retained_bytes, &slot->message);
  if (status != SALTS_OK)
    return outbox_source_requeue_rejected(source, slot, step.record.token, status, "message_copy");
  if (!cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &slot->message, 1u)) {
    turbo_flow_msg_cleanup(&slot->message);
    return outbox_source_requeue_rejected(source, slot, step.record.token, SALTS_ENOMEM,
                                          "publisher");
  }
  run_config.scheduler = source->config.scheduler;
  status = turbo_flow_run_open(source->config.flow, source->source_name, &publisher, &run_config,
                               &slot->run);
  if (status != SALTS_OK) {
    cflow_publisher_destroy(&publisher);
    turbo_flow_msg_cleanup(&slot->message);
    return outbox_source_requeue_rejected(source, slot, step.record.token, status, "graph_open");
  }
  slot->phase = OUTBOX_SLOT_GRAPH_RUNNING;
  slot->token = step.record.token;
  slot->retained_bytes = retained_bytes;
  ++source->in_flight_messages;
  source->in_flight_bytes += retained_bytes;
  ++source->fetched;
  source->message_id_exhausted = source->next_message_id == UINT64_MAX;
  if (!source->message_id_exhausted) ++source->next_message_id;
  status = turbo_flow_run_request(slot->run, 1u);
  if (status != SALTS_OK) {
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    int snapshot_status = turbo_flow_run_snapshot(slot->run, &result);
    if (snapshot_status == SALTS_OK && result.state != TURBO_FLOW_RUN_OPEN &&
        result.state != TURBO_FLOW_RUN_ACTIVE) {
      --source->outstanding_demand;
      if (progressed) *progressed = true;
      return SALTS_OK;
    }
    {
      int cancel_status = turbo_flow_run_cancel(slot->run);
      if (cancel_status != SALTS_OK && cancel_status != SALTS_EALREADY) {
        outbox_source_error(source, cancel_status, "graph_cancel", true);
        return cancel_status;
      }
    }
    slot->phase = OUTBOX_SLOT_REQUEUE_PENDING;
    slot->graph_status = status;
    if (status != SALTS_ENOSPC) {
      slot->source_failure_status = status;
      slot->source_failure_stage = "graph_request";
    }
    outbox_source_error(source, status, "graph_request", false);
    if (progressed) *progressed = true;
    return status;
  }
  --source->outstanding_demand;
  if (progressed) *progressed = true;
  return SALTS_OK;
}

int turbo_flow_turbodb_outbox_source_request(turbo_flow_turbodb_outbox_source_t *source,
                                             size_t demand) {
  if (!source || demand == 0u) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED) return source->status;
  if (source->state != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING &&
      source->state != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_WAITING)
    return SALTS_ESHUTDOWN;
  if (demand > SIZE_MAX - source->outstanding_demand) source->outstanding_demand = SIZE_MAX;
  else source->outstanding_demand += demand;
  return SALTS_OK;
}

int turbo_flow_turbodb_outbox_source_poll(turbo_flow_turbodb_outbox_source_t *source,
                                          size_t max_steps) {
  if (!source || max_steps == 0u) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED) return SALTS_ESHUTDOWN;
  if (source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED) return source->status;
  if (source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_WAITING) {
    if (!atomic_exchange_explicit(&source->fetch_wake_pending, false, memory_order_acq_rel))
      return SALTS_OK;
    outbox_source_unarm_fetch(source);
    source->state = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING;
  }
  for (size_t step_index = 0u; step_index < max_steps; ++step_index) {
    bool progressed = false;
    int status = outbox_source_settle_one(source, &progressed);
    if (status != SALTS_OK) return status;
    if (source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED) return source->status;
    if (!progressed && source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING) {
      status = outbox_source_fetch_one(source, &progressed);
      if (status != SALTS_OK) return status;
    }
    if (!progressed) break;
  }
  return SALTS_OK;
}

int turbo_flow_turbodb_outbox_source_snapshot(
    const turbo_flow_turbodb_outbox_source_t *source,
    turbo_flow_turbodb_outbox_source_snapshot_t *snapshot) {
  turbo_flow_turbodb_outbox_source_snapshot_t current =
      TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
  if (!source || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION)
    return SALTS_EINVAL;
  current.state = source->state;
  current.status = source->status;
  current.outstanding_demand = source->outstanding_demand;
  current.in_flight_messages = source->in_flight_messages;
  current.in_flight_bytes = source->in_flight_bytes;
  current.fetched = source->fetched;
  current.acknowledged = source->acknowledged;
  current.requeued = source->requeued;
  current.dead_lettered = source->dead_lettered;
  current.data_loss_events = source->data_loss_events;
  (void)snprintf(current.error_stage, sizeof(current.error_stage), "%s", source->error_stage);
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_turbodb_outbox_source_stop(turbo_flow_turbodb_outbox_source_t *source) {
  int status;
  if (!source) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED) return SALTS_EALREADY;
  source->state = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPING;
  outbox_source_unarm_fetch(source);
  status = source->config.provider.cancel_fetch(source->config.provider_ctx);
  if (status != SALTS_OK) {
    outbox_source_error(source, status, "cancel_fetch", false);
    return status;
  }
  for (size_t index = 0u; index < vec_size(&source->slots); ++index) {
    outbox_slot_t *slot = outbox_source_slot_at(source, index);
    if (!slot || slot->phase == OUTBOX_SLOT_EMPTY) continue;
    if (slot->phase == OUTBOX_SLOT_GRAPH_RUNNING) {
      status = turbo_flow_run_cancel(slot->run);
      if (status != SALTS_OK && status != SALTS_EALREADY) {
        outbox_source_error(source, status, "graph_cancel", false);
        return status;
      }
    }
    slot->phase = OUTBOX_SLOT_REQUEUE_PENDING;
    slot->source_failure_status = SALTS_OK;
    slot->source_failure_stage = NULL;
  }
  while (source->in_flight_messages != 0u) {
    bool progressed = false;
    status = outbox_source_settle_one(source, &progressed);
    if (status != SALTS_OK) return status;
    if (!progressed) {
      outbox_source_error(source, SALTS_EBUSY, "graph_cancel", false);
      return SALTS_EBUSY;
    }
  }
  source->state = TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED;
  source->status = SALTS_OK;
  source->error_stage[0] = '\0';
  return SALTS_OK;
}

int turbo_flow_turbodb_outbox_source_destroy(turbo_flow_turbodb_outbox_source_t *source) {
  if (!source) return SALTS_EINVAL;
  if (source->state != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED) return SALTS_EBUSY;
  vec_destroy(&source->slots);
  tstr_free(source->source_name);
  free(source);
  return SALTS_OK;
}

const turbo_flow_turbodb_outbox_message_context_t *
turbo_flow_turbodb_outbox_message_context(const turbo_flow_msg_t *message) {
  const turbo_flow_turbodb_outbox_message_context_t *context;
  const char *base;
  size_t used;
  if (!message || !message->buffer || !message->transport_context) return NULL;
  base = mem_buffer_const_data(message->buffer);
  used = mem_buffer_used(message->buffer);
  if (!base || used < sizeof(*context) || message->transport_context != base) return NULL;
  context = (const turbo_flow_turbodb_outbox_message_context_t *)base;
  if (context->size != sizeof(*context) ||
      context->version != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION ||
      context->identity_size > used - sizeof(*context) ||
      message->payload.data != base + sizeof(*context) + context->identity_size ||
      message->payload.len != used - sizeof(*context) - context->identity_size)
    return NULL;
  return context;
}

vstr turbo_flow_turbodb_outbox_message_identity(const turbo_flow_msg_t *message) {
  const turbo_flow_turbodb_outbox_message_context_t *context =
      turbo_flow_turbodb_outbox_message_context(message);
  if (!context) return (vstr){NULL, 0u};
  return vstr_from_buf(mem_buffer_const_data(message->buffer) + sizeof(*context),
                       context->identity_size);
}
