#include "flow_internal.h"

#include <stdio.h>
#include <string.h>

int flow_resource_metadata_valid(const turbo_flow_resource_metadata_t *metadata) {
  return metadata && metadata->size >= sizeof(*metadata) &&
         metadata->domain > TURBO_FLOW_DOMAIN_NONE &&
         metadata->domain <= TURBO_FLOW_DOMAIN_MANAGEMENT &&
         metadata->kind >= TURBO_FLOW_RESOURCE_CONNECTION &&
         metadata->kind <= TURBO_FLOW_RESOURCE_SECURITY_REALM && metadata->uid[0] != '\0' &&
         memchr(metadata->uid, '\0', sizeof(metadata->uid)) != NULL &&
         metadata->owner_name[0] != '\0' &&
         memchr(metadata->owner_name, '\0', sizeof(metadata->owner_name)) != NULL &&
         metadata->generation != 0u && metadata->observed_generation <= metadata->generation;
}

static atomic_uint_fast64_t flow_resource_command_sequence = 0u;

static int flow_resource_next_command_key(char *key, size_t key_size) {
  uint_fast64_t current;
  int written;
  if (!key || key_size == 0u) return SALTS_EINVAL;
  current = atomic_load_explicit(&flow_resource_command_sequence, memory_order_relaxed);
  for (;;) {
    if (current == UINT_FAST64_MAX) return SALTS_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&flow_resource_command_sequence, &current, current + 1u,
                                              memory_order_relaxed,
                                              memory_order_relaxed)) {
      break;
    }
  }
  written = snprintf(key, key_size, "internal:%llu", (unsigned long long)(current + 1u));
  return written < 0 || (size_t)written >= key_size ? SALTS_ENAMETOOLONG : SALTS_OK;
}

int flow_resource_command_init(turbo_flow_resource_command_t *resource_command,
                                              turbo_flow_resource_command_kind_t kind,
                                              const char *target_uid, uint64_t generation) {
  int written;
  int rc;
  if (!resource_command || !target_uid || target_uid[0] == '\0' || generation == 0u) {
    return SALTS_EINVAL;
  }
  *resource_command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
  resource_command->kind = kind;
  resource_command->expected_generation = generation;
  written = snprintf(resource_command->target_uid, sizeof(resource_command->target_uid), "%s",
                     target_uid);
  if (written < 0 || (size_t)written >= sizeof(resource_command->target_uid)) {
    return SALTS_ENAMETOOLONG;
  }
  rc = flow_resource_next_command_key(resource_command->idempotency_key,
                                     sizeof(resource_command->idempotency_key));
  return rc;
}

int flow_find_adapter_command_resource(turbo_flow_t *flow, const char *adapter_name,
                                              turbo_flow_resource_metadata_t *metadata) {
  int found = 0;
  if (!flow || !adapter_name || !metadata) return SALTS_EINVAL;
  for (size_t i = 0u; i < vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *resource =
        (const flow_resource_registration_t *)vec_at_const(&flow->resources, i);
    turbo_flow_resource_metadata_t current = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc;
    if (!resource || !resource->ops.command) continue;
    rc = resource->ops.metadata(resource->ctx, &current);
    if (rc != SALTS_OK) return rc;
    if (!flow_resource_metadata_valid(&current) ||
        strcmp(current.owner_name, resource->owner_name) != 0) {
      return SALTS_EPROTO;
    }
    if (current.kind != TURBO_FLOW_RESOURCE_CONNECTION ||
        strcmp(current.owner_name, adapter_name) != 0) {
      continue;
    }
    if (found) return SALTS_EPROTO;
    *metadata = current;
    found = 1;
  }
  return found ? SALTS_OK : SALTS_ENOENT;
}

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
  for (size_t i = 0; i < vec_size(&flow->resource_command_history); ++i) {
    const flow_resource_command_record_t *record =
        (const flow_resource_command_record_t *)vec_at_const(&flow->resource_command_history,
                                                                   i);
    if (!record || strcmp(record->command.idempotency_key, command->idempotency_key) != 0) continue;
    if (!flow_resource_command_same(&record->command, command)) return SALTS_EPROTO;
    *result = record->result;
    result->replayed = 1;
    return result->status;
  }
  return SALTS_ENOENT;
}

static int flow_resource_metadata_find(const turbo_flow_t *flow, const char *uid, size_t *index,
                                       turbo_flow_resource_metadata_t *metadata) {
  size_t count = turbo_flow_resource_metadata_count(flow);
  for (size_t i = 0; i < count; ++i) {
    turbo_flow_resource_metadata_t current = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc = turbo_flow_resource_metadata_at(flow, i, &current);
    if (rc != SALTS_OK) return rc;
    if (strcmp(current.uid, uid) == 0) {
      *index = i;
      *metadata = current;
      return SALTS_OK;
    }
  }
  return SALTS_ENOENT;
}

static int flow_resource_apply_registered(turbo_flow_t *flow, size_t metadata_index,
                                          const turbo_flow_resource_command_t *command) {
  flow_resource_registration_t *resource =
      (flow_resource_registration_t *)vec_at(&flow->resources, metadata_index);
  uint32_t boundary_command = 0u;
  if (!resource) return SALTS_ENOENT;
  if (!resource->ops.command) return SALTS_ENOTSUP;
  if (resource->has_managed_boundary) {
    switch (command->kind) {
      case TURBO_FLOW_RESOURCE_COMMAND_QUIESCE:
        boundary_command = TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE;
        break;
      case TURBO_FLOW_RESOURCE_COMMAND_RESUME:
        boundary_command = TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME;
        break;
      case TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT:
        boundary_command = TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_REPLACE_ENDPOINT;
        break;
      case TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL:
        break;
    }
    if ((resource->managed_boundary.command_flags & boundary_command) == 0u) {
      return SALTS_ENOTSUP;
    }
  }
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
  size_t provider_count = vec_size(&flow->resources);
  size_t native_count = flow_native_resource_count(flow);
  if (metadata_index < provider_count + native_count ||
      command->kind != TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL)
    return SALTS_ENOTSUP;
  metadata_index -= provider_count + native_count;
  rc = turbo_flow_pool_resource_status_at(flow, metadata_index, &status);
  if (rc != SALTS_OK) return rc;
  now = salts_hrtime();
  if (command->deadline_ns == UINT64_MAX) {
    remaining_ms = command->drain_timeout_ms;
  } else {
    if (now >= command->deadline_ns) return SALTS_ETIMEDOUT;
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

static int flow_resource_command_execute(turbo_flow_t *flow,
                                flow_resource_command_scope_t *scope,
                                const turbo_flow_resource_command_t *command,
                                turbo_flow_resource_command_result_t *out) {
  turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_resource_metadata_t after = TURBO_FLOW_RESOURCE_METADATA_INIT;
  size_t metadata_index = 0u;
  size_t record_index;
  flow_resource_command_record_t *record_slot;
  int rc;

  if (!flow || !out || out->size < sizeof(*out) || !flow_resource_command_valid(command)) {
    return SALTS_EINVAL;
  }
  if (flow->command_in_progress || flow->active_command_scope != scope) return SALTS_EBUSY;
  rc = flow_resource_command_lookup(flow, command, &result);
  if (result.replayed || rc != SALTS_ENOENT) {
    if (!result.replayed && rc == SALTS_EPROTO) return rc;
    *out = result;
    return rc;
  }
  if (scope && scope->remaining == 0u) return SALTS_ENOSPC;
  record_index = vec_size(&flow->resource_command_history);
  if (record_index >= TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX) return SALTS_ENOSPC;
  flow->command_in_progress = 1;
  /* Claim raw storage before callbacks; vec_push would allocate a temporary even after reserve. */
  rc = turbo_flow_stl_error(vec_resize(&flow->resource_command_history, record_index + 1u));
  if (rc != SALTS_OK) {
    flow->command_in_progress = 0;
    return rc;
  }
  record_slot = (flow_resource_command_record_t *)vec_at(&flow->resource_command_history, record_index);
  if (scope) --scope->remaining;
  if (command->deadline_ns != UINT64_MAX && salts_hrtime() >= command->deadline_ns) {
    rc = SALTS_ETIMEDOUT;
    goto record;
  }
  rc = flow_resource_metadata_find(flow, command->target_uid, &metadata_index, &metadata);
  if (rc != SALTS_OK) goto record;
  result.generation_before = metadata.generation;
  result.generation_after = metadata.generation;
  result.observed_generation = metadata.observed_generation;
  if (metadata.generation != command->expected_generation) {
    rc = SALTS_EBUSY;
    goto record;
  }
  if (metadata_index < vec_size(&flow->resources)) {
    rc = flow_resource_apply_registered(flow, metadata_index, command);
  } else if (metadata_index < vec_size(&flow->resources) +
                                  flow_native_resource_count(flow)) {
    rc = flow_native_resource_command(flow, metadata_index - vec_size(&flow->resources),
                                      command);
  } else if (metadata.kind == TURBO_FLOW_RESOURCE_POOL) {
    rc = flow_resource_apply_pool(flow, metadata_index, &metadata, command);
  } else {
    rc = SALTS_ENOTSUP;
  }
  if (flow_resource_metadata_find(flow, command->target_uid, &metadata_index, &after) == SALTS_OK) {
    result.generation_after = after.generation;
    result.observed_generation = after.observed_generation;
  }

record:
  result.status = rc;
  record_slot->command = *command;
  record_slot->command.size = sizeof(record_slot->command);
  record_slot->result = result;
  flow->command_in_progress = 0;
  *out = result;
  return rc;
}

int turbo_flow_resource_command(turbo_flow_t *flow,
                                const turbo_flow_resource_command_t *command,
                                turbo_flow_resource_command_result_t *out) {
  return flow_resource_command_execute(flow, NULL, command, out);
}

int flow_resource_command_scope_begin(turbo_flow_t *flow, size_t budget,
                                      flow_resource_command_scope_t *scope) {
  size_t used;
  int rc;
  if (!flow || !scope || scope->flow || scope->remaining || budget == 0u) return SALTS_EINVAL;
  if (flow->active_command_scope || flow->command_in_progress) return SALTS_EBUSY;
  used = vec_size(&flow->resource_command_history);
  if (budget > TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX - used) return SALTS_ENOSPC;
  rc = turbo_flow_stl_error(vec_reserve(&flow->resource_command_history, used + budget));
  if (rc != SALTS_OK) return rc;
  scope->flow = flow;
  scope->remaining = budget;
  flow->active_command_scope = scope;
  return SALTS_OK;
}

int flow_resource_command_scope_execute(flow_resource_command_scope_t *scope,
                                        const turbo_flow_resource_command_t *command,
                                        turbo_flow_resource_command_result_t *out) {
  if (!scope || !scope->flow || scope->flow->active_command_scope != scope) return SALTS_EINVAL;
  return flow_resource_command_execute(scope->flow, scope, command, out);
}

void flow_resource_command_scope_end(flow_resource_command_scope_t *scope) {
  if (!scope || !scope->flow || scope->flow->active_command_scope != scope) return;
  scope->flow->active_command_scope = NULL;
  scope->flow = NULL;
  scope->remaining = 0u;
}
