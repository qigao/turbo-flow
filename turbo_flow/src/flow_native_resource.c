#include "flow_internal.h"

#include "fmt.h"

#include <stdio.h>
#include <string.h>

static const char FLOW_RUNTIME_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowCoreResource [id(2), version(1)];\n"
    "message RuntimeStatus {\n"
    "  uint32 state;\n"
    "  bool accepting;\n"
    "  uint32 active_publishes;\n"
    "  string stages;\n"
    "  string edges;\n"
    "  string adapters;\n"
    "  string pools;\n"
    "}\n";
static const char FLOW_SEGMENT_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowCoreResource [id(3), version(1)];\n"
    "message SegmentStatus {\n"
    "  uint32 kind;\n"
    "  uint32 stage_index;\n"
    "  uint32 edge_index;\n"
    "  uint32 width;\n"
    "  uint32 capacity;\n"
    "  string load;\n"
    "  bool saturated;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_RUNTIME_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t), TURBO_FLOW_DOMAIN_MANAGEMENT,
    TURBO_FLOW_RESOURCE_RUNTIME, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON, "TurboFlowCoreResource", "RuntimeStatus", 2u, 1u,
    FLOW_RUNTIME_STATUS_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOW_SEGMENT_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t), TURBO_FLOW_DOMAIN_EXECUTION,
    TURBO_FLOW_RESOURCE_SEGMENT, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON, "TurboFlowCoreResource", "SegmentStatus", 3u, 1u,
    FLOW_SEGMENT_STATUS_SCHEMA_TEXT};

static int flow_native_available(const turbo_flow_t *flow) {
  turbo_flow_runtime_snapshot_t snapshot;
  return flow && turbo_flow_runtime_snapshot(flow, &snapshot) == TURBO_OK &&
         (snapshot.state == TURBO_FLOW_STATE_COMPILED ||
          snapshot.state == TURBO_FLOW_STATE_STARTED ||
          snapshot.state == TURBO_FLOW_STATE_STOPPED);
}

static uint64_t flow_native_generation(const turbo_flow_t *flow) {
  uint64_t generation;
  turbo_mutex_lock((turbo_mutex_t *)&flow->runtime_mutex);
  generation = flow->runtime_generation;
  turbo_mutex_unlock((turbo_mutex_t *)&flow->runtime_mutex);
  return generation == 0u ? 1u : generation;
}

static int flow_native_identity(turbo_flow_resource_metadata_t *metadata, const char *uid,
                                const char *owner_name) {
  int written = snprintf(metadata->uid, sizeof(metadata->uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(metadata->uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(metadata->owner_name, sizeof(metadata->owner_name), "%s", owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata->owner_name)) return TURBO_ENAMETOOLONG;
  return TURBO_OK;
}

size_t flow_native_resource_count(const turbo_flow_t *flow) {
  return flow_native_available(flow) ? 1u + turbo_vec_size(&flow->data_segments) : 0u;
}

int flow_native_resource_metadata_at(const turbo_flow_t *flow, size_t index,
                                     turbo_flow_resource_metadata_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  uint64_t generation;
  int rc;
  if (!flow_native_available(flow) || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  generation = flow_native_generation(flow);
  metadata.generation = generation;
  metadata.observed_generation = generation;
  if (index == 0u) {
    metadata.domain = TURBO_FLOW_DOMAIN_MANAGEMENT;
    metadata.kind = TURBO_FLOW_RESOURCE_RUNTIME;
    rc = flow_native_identity(&metadata, "runtime:graph", "graph");
  } else {
    const flow_data_segment_plan_t *segment;
    const flow_stage_plan_impl_t *stage;
    char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
    int written;
    --index;
    segment = (const flow_data_segment_plan_t *)turbo_vec_at_const(&flow->data_segments, index);
    if (!segment) return TURBO_ENOENT;
    stage = (const flow_stage_plan_impl_t *)turbo_vec_at_const(&flow->stages,
                                                               segment->stage_index);
    if (!stage || !stage->name) return TURBO_EPROTO;
    metadata.domain = TURBO_FLOW_DOMAIN_EXECUTION;
    metadata.kind = TURBO_FLOW_RESOURCE_SEGMENT;
    written = snprintf(uid, sizeof(uid), "segment:%llu:%u:%u:%u",
                       (unsigned long long)index, (unsigned)segment->kind,
                       (unsigned)segment->stage_index, (unsigned)segment->edge_index);
    if (written < 0 || (size_t)written >= sizeof(uid)) return TURBO_ENAMETOOLONG;
    rc = flow_native_identity(&metadata, uid, stage->name);
  }
  if (rc != TURBO_OK) return rc;
  *out = metadata;
  return TURBO_OK;
}

static int flow_segment_load(const turbo_flow_t *flow, const flow_data_segment_plan_t *segment,
                             uint64_t *load, uint64_t *capacity, int *saturated) {
  *load = 0u;
  *capacity = segment->capacity;
  *saturated = 0;
  if (segment->kind != FLOW_DATA_SEGMENT_WORKER_POOL) return TURBO_OK;
  for (size_t i = 0; i < turbo_flow_pool_count(flow); ++i) {
    turbo_flow_pool_snapshot_t pool;
    if (turbo_flow_pool_snapshot_at(flow, i, &pool) != TURBO_OK ||
        pool.stage_index != segment->stage_index || pool.kind != TURBO_FLOW_POOL_DISRUPTOR) {
      continue;
    }
    *load = pool.active > UINT64_MAX - pool.queued ? UINT64_MAX : pool.active + pool.queued;
    *capacity = pool.queue_capacity;
    *saturated = turbo_flow_pool_saturated(&pool);
    return TURBO_OK;
  }
  return TURBO_OK;
}

int flow_native_resource_snapshot_at(const turbo_flow_t *flow, size_t index,
                                     turbo_flow_resource_snapshot_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flow_native_resource_metadata_at(flow, index, &metadata);
  if (rc != TURBO_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  if (index == 0u) {
    turbo_flow_runtime_snapshot_t runtime;
    rc = turbo_flow_runtime_snapshot(flow, &runtime);
    if (rc != TURBO_OK) return rc;
    out->load = runtime.active_publishes;
    out->capacity = 0u;
    out->saturated = 0;
    out->last_status = flow_error_code(flow);
    return TURBO_OK;
  }
  {
    const flow_data_segment_plan_t *segment =
        (const flow_data_segment_plan_t *)turbo_vec_at_const(&flow->data_segments, index - 1u);
    if (!segment) return TURBO_ENOENT;
    rc = flow_segment_load(flow, segment, &out->load, &out->capacity, &out->saturated);
    out->last_status = rc;
    return rc;
  }
}

int flow_native_resource_document_at(const turbo_flow_t *flow, size_t index,
                                     turbo_flow_resource_document_kind_t document_kind,
                                     turbo_flow_resource_document_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  const turbo_flow_resource_schema_t *schema;
  tstr payload;
  int rc;
  if (!out || out->size < sizeof(*out) || out->payload) return TURBO_EINVAL;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  rc = flow_native_resource_metadata_at(flow, index, &metadata);
  if (rc != TURBO_OK) return rc;
  if (index == 0u) {
    turbo_flow_runtime_snapshot_t runtime;
    rc = turbo_flow_runtime_snapshot(flow, &runtime);
    if (rc != TURBO_OK) return rc;
    schema = &FLOW_RUNTIME_STATUS_SCHEMA;
    payload = tstr_format(
        "{\"state\":{},\"accepting\":{},\"active_publishes\":{},"
        "\"stages\":\"{}\",\"edges\":\"{}\",\"adapters\":\"{}\","
        "\"pools\":\"{}\"}",
        (unsigned)runtime.state, runtime.accepting_publishes, runtime.active_publishes,
        runtime.stage_count, runtime.edge_count, runtime.adapter_count, runtime.pool_count);
  } else {
    const flow_data_segment_plan_t *segment =
        (const flow_data_segment_plan_t *)turbo_vec_at_const(&flow->data_segments, index - 1u);
    uint64_t load;
    uint64_t capacity;
    int saturated;
    if (!segment) return TURBO_ENOENT;
    rc = flow_segment_load(flow, segment, &load, &capacity, &saturated);
    if (rc != TURBO_OK) return rc;
    schema = &FLOW_SEGMENT_STATUS_SCHEMA;
    payload = tstr_format(
        "{\"kind\":{},\"stage_index\":{},\"edge_index\":{},\"width\":{},"
        "\"capacity\":{},\"load\":\"{}\",\"saturated\":{}}",
        (unsigned)segment->kind, (unsigned)segment->stage_index, (unsigned)segment->edge_index,
        (unsigned)segment->width, (unsigned)segment->capacity, load, saturated);
  }
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, schema, payload,
                                                     tstr_len(payload));
  tstr_free(payload);
  return rc;
}

int flow_native_resource_command(turbo_flow_t *flow, size_t index,
                                 const turbo_flow_resource_command_t *command) {
  turbo_flow_runtime_snapshot_t before;
  turbo_flow_runtime_snapshot_t after;
  int rc;
  if (!flow || !command) return TURBO_EINVAL;
  if (index != 0u) return TURBO_ENOTSUP;
  rc = turbo_flow_runtime_snapshot(flow, &before);
  if (rc != TURBO_OK) return rc;
  if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_QUIESCE) {
    rc = turbo_flow_pause(flow);
  } else if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_RESUME) {
    rc = turbo_flow_resume(flow);
  } else {
    return TURBO_ENOTSUP;
  }
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_runtime_snapshot(flow, &after);
  if (rc != TURBO_OK) return rc;
  if (before.accepting_publishes != after.accepting_publishes) {
    turbo_mutex_lock(&flow->runtime_mutex);
    if (flow->runtime_generation == UINT64_MAX) {
      turbo_mutex_unlock(&flow->runtime_mutex);
      return TURBO_ERANGE;
    }
    ++flow->runtime_generation;
    turbo_mutex_unlock(&flow->runtime_mutex);
  }
  return TURBO_OK;
}
