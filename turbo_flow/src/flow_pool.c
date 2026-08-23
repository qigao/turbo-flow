#include "flow_internal.h"

#include <stdio.h>
#include <string.h>

#define FLOW_POOL_STATUS_SCHEMA_ID 1u
#define FLOW_POOL_STATUS_SCHEMA_VERSION 1u

static const char FLOW_POOL_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowResource [id(1), version(1)];\n"
    "message PoolStatus {\n"
    "  uint32 kind;\n"
    "  uint32 state;\n"
    "  uint32 stage_index;\n"
    "  uint32 parallelism;\n"
    "  string queue_capacity;\n"
    "  string resource_capacity;\n"
    "  string submitted;\n"
    "  string started;\n"
    "  string completed;\n"
    "  string failed;\n"
    "  string canceled;\n"
    "  string rejected;\n"
    "  string queued;\n"
    "  string active;\n"
    "  bool accepting;\n"
    "  bool drained;\n"
    "  bool saturated;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_POOL_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_EXECUTION,
    TURBO_FLOW_RESOURCE_POOL,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowResource",
    "PoolStatus",
    FLOW_POOL_STATUS_SCHEMA_ID,
    FLOW_POOL_STATUS_SCHEMA_VERSION,
    FLOW_POOL_STATUS_SCHEMA_TEXT};

static uint64_t flow_pool_load(const atomic_uint_fast64_t *value) {
  return atomic_load_explicit(value, memory_order_acquire);
}

int flow_pool_record_add(turbo_flow_t *flow, turbo_flow_pool_kind_t kind, uint32_t stage_index,
                         uint32_t parallelism, uint64_t queue_capacity,
                         uint64_t resource_capacity, size_t *index) {
  flow_pool_record_t record;
  int rc;

  if (!flow || !index || stage_index >= vec_size(&flow->stages) || parallelism == 0u) {
    return TURBO_EINVAL;
  }
  memset(&record, 0, sizeof(record));
  record.kind = kind;
  record.stage_index = stage_index;
  if (flow->runtime_generation == UINT64_MAX) return TURBO_ERANGE;
  record.generation = flow->runtime_generation + 1u;
  record.parallelism = parallelism;
  record.queue_capacity = queue_capacity;
  record.resource_capacity = resource_capacity;
  atomic_init(&record.state, TURBO_FLOW_POOL_STARTING);
  atomic_init(&record.submitted, 0u);
  atomic_init(&record.started, 0u);
  atomic_init(&record.completed, 0u);
  atomic_init(&record.failed, 0u);
  atomic_init(&record.canceled, 0u);
  atomic_init(&record.rejected, 0u);
  atomic_init(&record.queued, 0u);
  atomic_init(&record.active, 0u);
  rc = turbo_flow_stl_error(vec_push(&flow->pool_records, &record));
  if (rc != TURBO_OK) return rc;
  *index = vec_size(&flow->pool_records) - 1u;
  return TURBO_OK;
}

int flow_runtime_generation_can_advance(const turbo_flow_t *flow) {
  return flow && flow->runtime_generation != UINT64_MAX ? TURBO_OK : TURBO_ERANGE;
}

void flow_runtime_generation_commit(turbo_flow_t *flow) {
  if (!flow || flow->runtime_generation == UINT64_MAX) return;
  for (size_t index = 0u; index < vec_size(&flow->executor_plans); ++index) {
    const flow_executor_plan_t *executor =
        (const flow_executor_plan_t *)vec_at_const(&flow->executor_plans, index);
    if (executor && executor->keyed_store) flow_keyed_state_store_reset(executor->keyed_store);
  }
  ++flow->runtime_generation;
}

flow_pool_record_t *flow_pool_record_at(turbo_flow_t *flow, size_t index) {
  return flow ? (flow_pool_record_t *)vec_at(&flow->pool_records, index) : NULL;
}

void flow_pool_record_set_state(flow_pool_record_t *record, turbo_flow_pool_state_t state) {
  if (record) atomic_store_explicit(&record->state, state, memory_order_release);
}

void flow_pool_record_submitted(flow_pool_record_t *record) {
  if (!record) return;
  atomic_fetch_add_explicit(&record->submitted, 1u, memory_order_acq_rel);
  atomic_fetch_add_explicit(&record->queued, 1u, memory_order_acq_rel);
}

void flow_pool_record_attempted(flow_pool_record_t *record) {
  if (record) atomic_fetch_add_explicit(&record->submitted, 1u, memory_order_acq_rel);
}

void flow_pool_record_queued(flow_pool_record_t *record) {
  if (record) atomic_fetch_add_explicit(&record->queued, 1u, memory_order_acq_rel);
}

void flow_pool_record_started(flow_pool_record_t *record) {
  if (!record) return;
  atomic_fetch_sub_explicit(&record->queued, 1u, memory_order_acq_rel);
  atomic_fetch_add_explicit(&record->started, 1u, memory_order_acq_rel);
  atomic_fetch_add_explicit(&record->active, 1u, memory_order_acq_rel);
}

void flow_pool_record_finished(flow_pool_record_t *record, int status) {
  if (!record) return;
  atomic_fetch_sub_explicit(&record->active, 1u, memory_order_acq_rel);
  if (status == TURBO_ECANCELED || status == TURBO_ESHUTDOWN) {
    atomic_fetch_add_explicit(&record->canceled, 1u, memory_order_acq_rel);
  } else if (status == TURBO_OK) {
    atomic_fetch_add_explicit(&record->completed, 1u, memory_order_acq_rel);
  } else {
    atomic_fetch_add_explicit(&record->failed, 1u, memory_order_acq_rel);
  }
}

void flow_pool_record_rejected(flow_pool_record_t *record) {
  if (!record) return;
  atomic_fetch_sub_explicit(&record->queued, 1u, memory_order_acq_rel);
  atomic_fetch_add_explicit(&record->rejected, 1u, memory_order_acq_rel);
}

void flow_pool_record_rejected_unqueued(flow_pool_record_t *record) {
  if (!record) return;
  atomic_fetch_add_explicit(&record->rejected, 1u, memory_order_acq_rel);
}

void flow_pool_record_canceled_unqueued(flow_pool_record_t *record) {
  if (!record) return;
  atomic_fetch_add_explicit(&record->canceled, 1u, memory_order_acq_rel);
}

size_t turbo_flow_pool_count(const turbo_flow_t *flow) {
  return flow ? vec_size(&flow->pool_records) : 0u;
}

int turbo_flow_pool_snapshot_at(const turbo_flow_t *flow, size_t index,
                                turbo_flow_pool_snapshot_t *out) {
  const flow_pool_record_t *record;
  const flow_stage_plan_impl_t *stage;

  if (!flow || !out) return TURBO_EINVAL;
  record = (const flow_pool_record_t *)vec_at_const(&flow->pool_records, index);
  if (!record) return TURBO_EINVAL;
  stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, record->stage_index);
  if (!stage) return TURBO_EINVAL;

  memset(out, 0, sizeof(*out));
  out->kind = record->kind;
  out->state =
      (turbo_flow_pool_state_t)atomic_load_explicit(&record->state, memory_order_acquire);
  out->stage_index = record->stage_index;
  out->stage_name = stage->name;
  out->parallelism = record->parallelism;
  out->queue_capacity = record->queue_capacity;
  out->resource_capacity = record->resource_capacity;
  out->submitted = flow_pool_load(&record->submitted);
  out->started = flow_pool_load(&record->started);
  out->completed = flow_pool_load(&record->completed);
  out->failed = flow_pool_load(&record->failed);
  out->canceled = flow_pool_load(&record->canceled);
  out->rejected = flow_pool_load(&record->rejected);
  out->queued = flow_pool_load(&record->queued);
  out->active = flow_pool_load(&record->active);
  if (record->kind == TURBO_FLOW_POOL_THREAD && out->state == TURBO_FLOW_POOL_RUNNING) {
    const flow_threadpool_adapter_t *adapter =
        flow_threadpool_adapter_for_stage(flow, record->stage_index);
    if (adapter && adapter->pool) {
      turbo_threadpool_stats_t stats;
      memset(&stats, 0, sizeof(stats));
      turbo_threadpool_get_stats(adapter->pool, &stats);
      out->queue_capacity = stats.queue_capacity;
      out->queued = stats.queued_tasks > 0 ? (uint64_t)stats.queued_tasks : 0u;
      out->active = stats.active_tasks > 0 ? (uint64_t)stats.active_tasks : 0u;
    }
  }
  return TURBO_OK;
}

static void flow_pool_condition(turbo_flow_resource_condition_t *condition,
                                turbo_flow_resource_condition_kind_t kind, int value,
                                turbo_flow_resource_condition_reason_t true_reason,
                                turbo_flow_resource_condition_reason_t false_reason) {
  condition->kind = kind;
  condition->status = value ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE;
  condition->reason = value ? true_reason : false_reason;
}

int turbo_flow_pool_resource_status_at(const turbo_flow_t *flow, size_t index,
                                       turbo_flow_pool_resource_status_t *out) {
  const flow_pool_record_t *record;
  turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
  int written;
  int rc;

  if (!flow || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  record = (const flow_pool_record_t *)vec_at_const(&flow->pool_records, index);
  if (!record) return TURBO_ENOENT;
  rc = turbo_flow_pool_snapshot_at(flow, index, &status.snapshot);
  if (rc != TURBO_OK) return rc;

  status.resource_kind = TURBO_FLOW_RESOURCE_POOL;
  written = snprintf(status.uid, sizeof(status.uid), "pool:%u:%s", (unsigned)record->kind,
                     status.snapshot.stage_name);
  if (written < 0 || (size_t)written >= sizeof(status.uid)) return TURBO_ENAMETOOLONG;
  written =
      snprintf(status.owner_name, sizeof(status.owner_name), "%s", status.snapshot.stage_name);
  if (written < 0 || (size_t)written >= sizeof(status.owner_name)) return TURBO_ENAMETOOLONG;
  status.generation = record->generation;
  status.observed_generation = record->generation;
  status.condition_count = TURBO_FLOW_RESOURCE_CONDITION_MAX;
  flow_pool_condition(&status.conditions[0], TURBO_FLOW_RESOURCE_CONDITION_READY,
                      status.snapshot.state == TURBO_FLOW_POOL_RUNNING,
                      TURBO_FLOW_RESOURCE_REASON_RUNNING, TURBO_FLOW_RESOURCE_REASON_NOT_RUNNING);
  flow_pool_condition(&status.conditions[1], TURBO_FLOW_RESOURCE_CONDITION_ACCEPTING,
                      turbo_flow_pool_accepting(&status.snapshot),
                      TURBO_FLOW_RESOURCE_REASON_ACCEPTING,
                      TURBO_FLOW_RESOURCE_REASON_NOT_ACCEPTING);
  flow_pool_condition(&status.conditions[2], TURBO_FLOW_RESOURCE_CONDITION_DRAINED,
                      turbo_flow_pool_drained(&status.snapshot), TURBO_FLOW_RESOURCE_REASON_DRAINED,
                      TURBO_FLOW_RESOURCE_REASON_WORK_PENDING);
  flow_pool_condition(&status.conditions[3], TURBO_FLOW_RESOURCE_CONDITION_SATURATED,
                      turbo_flow_pool_saturated(&status.snapshot),
                      TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED,
                      TURBO_FLOW_RESOURCE_REASON_CAPACITY_AVAILABLE);
  memcpy(out, &status, sizeof(status));
  return TURBO_OK;
}

const turbo_flow_resource_schema_t *turbo_flow_pool_status_schema(void) {
  return &FLOW_POOL_STATUS_SCHEMA;
}

int turbo_flow_pool_status_document_at(const turbo_flow_t *flow, size_t index,
                                       turbo_flow_resource_document_t *out) {
  turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
  turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
  tstr payload = NULL;
  tstr formatted;
  int rc;

  if (!flow || !out || out->size < sizeof(*out) || out->payload) return TURBO_EINVAL;
  rc = turbo_flow_pool_resource_status_at(flow, index, &status);
  if (rc != TURBO_OK) return rc;

  payload = tstr_new();
  if (!payload) return TURBO_ENOMEM;
  formatted = tstr_cat_fmt(
      payload,
      "{\"kind\":%u,\"state\":%u,\"stage_index\":%u,\"parallelism\":%u,"
      "\"queue_capacity\":\"%llu\",\"resource_capacity\":\"%llu\","
      "\"submitted\":\"%llu\",\"started\":\"%llu\",\"completed\":\"%llu\","
      "\"failed\":\"%llu\",\"canceled\":\"%llu\",\"rejected\":\"%llu\","
      "\"queued\":\"%llu\",\"active\":\"%llu\",\"accepting\":%s,"
      "\"drained\":%s,\"saturated\":%s}",
      (unsigned)status.snapshot.kind, (unsigned)status.snapshot.state,
      (unsigned)status.snapshot.stage_index, (unsigned)status.snapshot.parallelism,
      (unsigned long long)status.snapshot.queue_capacity,
      (unsigned long long)status.snapshot.resource_capacity,
      (unsigned long long)status.snapshot.submitted, (unsigned long long)status.snapshot.started,
      (unsigned long long)status.snapshot.completed, (unsigned long long)status.snapshot.failed,
      (unsigned long long)status.snapshot.canceled, (unsigned long long)status.snapshot.rejected,
      (unsigned long long)status.snapshot.queued, (unsigned long long)status.snapshot.active,
      turbo_flow_pool_accepting(&status.snapshot) ? "true" : "false",
      turbo_flow_pool_drained(&status.snapshot) ? "true" : "false",
      turbo_flow_pool_saturated(&status.snapshot) ? "true" : "false");
  if (!formatted) {
    tstr_free(payload);
    return TURBO_ENOMEM;
  }
  payload = formatted;
  {
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    metadata.domain = TURBO_FLOW_DOMAIN_EXECUTION;
    metadata.kind = TURBO_FLOW_RESOURCE_POOL;
    metadata.generation = status.generation;
    metadata.observed_generation = status.observed_generation;
    rc = snprintf(metadata.uid, sizeof(metadata.uid), "%s", status.uid);
    if (rc < 0 || (size_t)rc >= sizeof(metadata.uid)) {
      rc = TURBO_ENAMETOOLONG;
      goto cleanup;
    }
    rc = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", status.owner_name);
    if (rc < 0 || (size_t)rc >= sizeof(metadata.owner_name)) {
      rc = TURBO_ENAMETOOLONG;
      goto cleanup;
    }
    rc = turbo_flow_resource_document_set_payload_copy(
        &document, &metadata, &FLOW_POOL_STATUS_SCHEMA, payload, tstr_len(payload));
    if (rc != TURBO_OK) goto cleanup;
  }
  memcpy(out, &document, sizeof(document));
  tstr_free(payload);
  return TURBO_OK;

cleanup:
  tstr_free(payload);
  turbo_flow_resource_document_cleanup(&document);
  return rc;
}

int turbo_flow_pool_accepting(const turbo_flow_pool_snapshot_t *snapshot) {
  return snapshot && snapshot->state == TURBO_FLOW_POOL_RUNNING;
}

int turbo_flow_pool_drained(const turbo_flow_pool_snapshot_t *snapshot) {
  return snapshot && snapshot->queued == 0u && snapshot->active == 0u;
}

int turbo_flow_pool_saturated(const turbo_flow_pool_snapshot_t *snapshot) {
  if (!snapshot || snapshot->state != TURBO_FLOW_POOL_RUNNING) return 0;
  if (snapshot->queue_capacity > 0u && snapshot->queued >= snapshot->queue_capacity) return 1;
  return snapshot->resource_capacity > 0u &&
         snapshot->active + snapshot->queued >= snapshot->resource_capacity;
}
