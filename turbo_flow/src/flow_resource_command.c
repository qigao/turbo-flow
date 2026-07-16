#include "flow_internal.h"

#include <string.h>

int flow_resource_command_valid(const turbo_flow_resource_command_t *command) {
  if (!command || command->size < sizeof(*command) ||
      command->kind < TURBO_FLOW_RESOURCE_COMMAND_QUIESCE ||
      command->kind > TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL || command->target_uid[0] == '\0' ||
      memchr(command->target_uid, '\0', sizeof(command->target_uid)) == NULL ||
      command->idempotency_key[0] == '\0' ||
      memchr(command->idempotency_key, '\0', sizeof(command->idempotency_key)) == NULL ||
      memchr(command->endpoint_host, '\0', sizeof(command->endpoint_host)) == NULL ||
      memchr(command->endpoint_path, '\0', sizeof(command->endpoint_path)) == NULL ||
      command->expected_generation == 0u ||
      command->deadline_ns == 0u) {
    return 0;
  }
  if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL) {
    return command->parallelism > 0u && command->endpoint_host[0] == '\0' &&
           command->endpoint_path[0] == '\0' && command->endpoint_port == 0;
  }
  if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT) {
    return command->parallelism == 0u && command->endpoint_port >= 0 &&
           (command->endpoint_host[0] != '\0' || command->endpoint_path[0] != '\0');
  }
  return command->parallelism == 0u && command->endpoint_host[0] == '\0' &&
         command->endpoint_path[0] == '\0' && command->endpoint_port == 0;
}

static int flow_resource_command_same(const turbo_flow_resource_command_t *left,
                                      const turbo_flow_resource_command_t *right) {
  return left->kind == right->kind && left->expected_generation == right->expected_generation &&
         left->deadline_ns == right->deadline_ns && left->parallelism == right->parallelism &&
         left->drain_timeout_ms == right->drain_timeout_ms &&
         left->endpoint_port == right->endpoint_port &&
         strcmp(left->target_uid, right->target_uid) == 0 &&
         strcmp(left->idempotency_key, right->idempotency_key) == 0 &&
         strcmp(left->endpoint_host, right->endpoint_host) == 0 &&
         strcmp(left->endpoint_path, right->endpoint_path) == 0;
}

static int flow_resource_command_lookup(turbo_flow_t *flow,
                                        const turbo_flow_resource_command_t *command,
                                        turbo_flow_resource_command_result_t *result) {
  for (size_t i = 0; i < turbo_vec_size(&flow->resource_command_history); ++i) {
    const flow_resource_command_record_t *record =
        (const flow_resource_command_record_t *)turbo_vec_at_const(&flow->resource_command_history,
                                                                   i);
    if (!record || strcmp(record->command.idempotency_key, command->idempotency_key) != 0) continue;
    if (!flow_resource_command_same(&record->command, command)) return TURBO_EPROTO;
    *result = record->result;
    result->replayed = 1;
    return result->status;
  }
  return TURBO_ENOENT;
}

static int flow_resource_command_record(turbo_flow_t *flow,
                                        const turbo_flow_resource_command_t *command,
                                        const turbo_flow_resource_command_result_t *result) {
  flow_resource_command_record_t record;
  if (turbo_vec_size(&flow->resource_command_history) >=
      TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX) {
    return TURBO_ENOSPC;
  }
  memset(&record, 0, sizeof(record));
  record.command = *command;
  record.command.size = sizeof(record.command);
  record.result = *result;
  record.result.size = sizeof(record.result);
  return turbo_vec_push(&flow->resource_command_history, &record);
}

static int flow_resource_metadata_find(const turbo_flow_t *flow, const char *uid, size_t *index,
                                       turbo_flow_resource_metadata_t *metadata) {
  size_t count = turbo_flow_resource_metadata_count(flow);
  for (size_t i = 0; i < count; ++i) {
    turbo_flow_resource_metadata_t current = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc = turbo_flow_resource_metadata_at(flow, i, &current);
    if (rc != TURBO_OK) return rc;
    if (strcmp(current.uid, uid) == 0) {
      *index = i;
      *metadata = current;
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

static int flow_resource_apply_registered(turbo_flow_t *flow, size_t metadata_index,
                                          const turbo_flow_resource_command_t *command) {
  flow_resource_registration_t *resource =
      (flow_resource_registration_t *)turbo_vec_at(&flow->resources, metadata_index);
  if (!resource) return TURBO_ENOENT;
  if (!resource->ops.command) return TURBO_ENOTSUP;
  return resource->ops.command(resource->ctx, flow, command);
}

static int flow_resource_apply_pool(turbo_flow_t *flow, size_t metadata_index,
                                    const turbo_flow_resource_metadata_t *metadata,
                                    const turbo_flow_resource_command_t *command) {
  turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
  turbo_flow_pool_resize_command_t resize;
  uint64_t remaining_ms;
  uint64_t now;
  int rc;
  size_t provider_count = turbo_vec_size(&flow->resources);
  size_t native_count = flow_native_resource_count(flow);
  if (metadata_index < provider_count + native_count ||
      command->kind != TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL)
    return TURBO_ENOTSUP;
  metadata_index -= provider_count + native_count;
  rc = turbo_flow_pool_resource_status_at(flow, metadata_index, &status);
  if (rc != TURBO_OK) return rc;
  now = turbo_hrtime();
  if (command->deadline_ns == UINT64_MAX) {
    remaining_ms = command->drain_timeout_ms;
  } else {
    if (now >= command->deadline_ns) return TURBO_ETIMEDOUT;
    remaining_ms = (command->deadline_ns - now + UINT64_C(999999)) / UINT64_C(1000000);
    if (command->drain_timeout_ms < remaining_ms) remaining_ms = command->drain_timeout_ms;
  }
  memset(&resize, 0, sizeof(resize));
  resize.size = sizeof(resize);
  resize.stage_name = status.snapshot.stage_name;
  resize.kind = status.snapshot.kind;
  resize.parallelism = command->parallelism;
  resize.drain_timeout_ms = remaining_ms;
  resize.expected_generation = metadata->generation;
  return turbo_flow_resize_pool(flow, &resize);
}

int turbo_flow_resource_command(turbo_flow_t *flow,
                                const turbo_flow_resource_command_t *command,
                                turbo_flow_resource_command_result_t *out) {
  turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_resource_metadata_t after = TURBO_FLOW_RESOURCE_METADATA_INIT;
  size_t metadata_index = 0u;
  int rc;

  if (!flow || !out || out->size < sizeof(*out) || !flow_resource_command_valid(command)) {
    return TURBO_EINVAL;
  }
  rc = flow_resource_command_lookup(flow, command, &result);
  if (rc != TURBO_ENOENT) {
    if (rc == TURBO_EPROTO) return rc;
    *out = result;
    return rc;
  }
  if (turbo_vec_size(&flow->resource_command_history) >= TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX)
    return TURBO_ENOSPC;
  if (command->deadline_ns != UINT64_MAX && turbo_hrtime() >= command->deadline_ns) {
    rc = TURBO_ETIMEDOUT;
    goto record;
  }
  rc = flow_resource_metadata_find(flow, command->target_uid, &metadata_index, &metadata);
  if (rc != TURBO_OK) goto record;
  result.generation_before = metadata.generation;
  result.generation_after = metadata.generation;
  result.observed_generation = metadata.observed_generation;
  if (metadata.generation != command->expected_generation) {
    rc = TURBO_EBUSY;
    goto record;
  }
  if (metadata_index < turbo_vec_size(&flow->resources)) {
    rc = flow_resource_apply_registered(flow, metadata_index, command);
  } else if (metadata_index < turbo_vec_size(&flow->resources) +
                                  flow_native_resource_count(flow)) {
    rc = flow_native_resource_command(flow, metadata_index - turbo_vec_size(&flow->resources),
                                      command);
  } else if (metadata.kind == TURBO_FLOW_RESOURCE_POOL) {
    rc = flow_resource_apply_pool(flow, metadata_index, &metadata, command);
  } else {
    rc = TURBO_ENOTSUP;
  }
  if (flow_resource_metadata_find(flow, command->target_uid, &metadata_index, &after) == TURBO_OK) {
    result.generation_after = after.generation;
    result.observed_generation = after.observed_generation;
  }

record:
  result.status = rc;
  if (flow_resource_command_record(flow, command, &result) != TURBO_OK) return TURBO_ENOSPC;
  *out = result;
  return rc;
}
