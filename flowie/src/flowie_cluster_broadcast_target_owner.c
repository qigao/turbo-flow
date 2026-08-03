#include "flowie_cluster_broadcast_target_owner_internal.h"

#include "flow_coronet_execution.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_broadcast_target_owned_s {
  flowie_cluster_session_publish_target_t view;
  tstr_t client_id;
  tstr_t edge_node_id;
  tstr_t shared_filter;
  uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  turbo_vec_t subscription_identifiers;
  int identifiers_initialized;
} flowie_cluster_broadcast_target_owned_t;

typedef struct flowie_cluster_broadcast_target_operation_s {
  struct flowie_cluster_broadcast_target_owner_s *owner;
  tstr_t payload;
  flowie_cluster_broadcast_event_view_t event;
  uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
  flowie_cluster_owner_token_t owner_token;
  turbo_vec_t targets;
  int targets_initialized;
  size_t target_index;
  flowie_cluster_session_delivery_plan_t *plan;
  flowie_cluster_pgsql_event_dedupe_t shard_ack_dedupe;
  flowie_cluster_pgsql_fact_command_t shard_ack;
  flowie_cluster_broadcast_target_apply_complete_fn complete;
  void *completion_ctx;
} flowie_cluster_broadcast_target_operation_t;

typedef enum flowie_cluster_broadcast_target_request_kind_e {
  FLOWIE_CLUSTER_BROADCAST_TARGET_REQUEST_DELIVERY = 1,
  FLOWIE_CLUSTER_BROADCAST_TARGET_REQUEST_SHARD_ACK
} flowie_cluster_broadcast_target_request_kind_t;

typedef struct flowie_cluster_broadcast_target_request_s {
  flowie_cluster_broadcast_target_operation_t *operation;
  flowie_cluster_broadcast_target_request_kind_t kind;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  int status;
  atomic_int completed;
} flowie_cluster_broadcast_target_request_t;

struct flowie_cluster_broadcast_target_owner_s {
  flowie_cluster_broadcast_target_owner_config_t config;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int accepting;
  int active;
  int last_status;
  uint64_t admitted_events;
  uint64_t completed_events;
  uint64_t target_transactions;
  uint64_t shard_ack_transactions;
  size_t current_target_count;
};

int flowie_cluster_broadcast_target_owner_config_validate(
    const flowie_cluster_broadcast_target_owner_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_ABI_V1 ||
      config->max_payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      config->max_payload_size > UINT32_MAX || config->max_targets == 0u ||
      config->max_targets > SIZE_MAX / sizeof(flowie_cluster_broadcast_target_owned_t) ||
      !config->execution || !config->execution->context || !config->session_bind ||
      !config->resolve || !config->submit || !config->self_fence)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_broadcast_target_owner_counter(uint64_t *counter) {
  if (!counter || *counter == UINT64_MAX) return TURBO_ERANGE;
  ++*counter;
  return TURBO_OK;
}

static void flowie_cluster_broadcast_target_owned_destroy(
    flowie_cluster_broadcast_target_owned_t *target) {
  if (!target) return;
  if (target->identifiers_initialized) turbo_vec_destroy(&target->subscription_identifiers);
  tstr_freep(&target->edge_node_id);
  tstr_freep(&target->shared_filter);
  tstr_freep(&target->client_id);
  memset(target, 0, sizeof(*target));
}

static void flowie_cluster_broadcast_target_operation_destroy(
    flowie_cluster_broadcast_target_operation_t *operation) {
  if (!operation) return;
  flowie_cluster_session_delivery_plan_destroy(&operation->plan);
  if (operation->targets_initialized) {
    for (size_t slot = 0u; slot < turbo_vec_size(&operation->targets); ++slot) {
      flowie_cluster_broadcast_target_owned_t *target =
          (flowie_cluster_broadcast_target_owned_t *)turbo_vec_at(&operation->targets, slot);
      flowie_cluster_broadcast_target_owned_destroy(target);
    }
    turbo_vec_destroy(&operation->targets);
  }
  tstr_freep(&operation->payload);
  free(operation);
}

static void flowie_cluster_broadcast_target_owner_finish(
    flowie_cluster_broadcast_target_operation_t *operation, int status) {
  flowie_cluster_broadcast_target_owner_t *owner;
  flowie_cluster_broadcast_target_apply_complete_fn complete;
  void *completion_ctx;
  if (!operation) return;
  owner = operation->owner;
  complete = operation->complete;
  completion_ctx = operation->completion_ctx;
  flowie_cluster_broadcast_target_operation_destroy(operation);
  turbo_mutex_lock(&owner->mutex);
  owner->last_status = status;
  if (status == TURBO_OK) {
    int counter_rc = flowie_cluster_broadcast_target_owner_counter(&owner->completed_events);
    if (counter_rc != TURBO_OK) {
      owner->last_status = counter_rc;
      status = counter_rc;
    }
  }
  owner->active = 0;
  owner->current_target_count = 0u;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
  complete(completion_ctx, status);
}

static int flowie_cluster_broadcast_target_copy(
    void *ctx, const flowie_cluster_session_publish_target_t *source) {
  flowie_cluster_broadcast_target_operation_t *operation =
      (flowie_cluster_broadcast_target_operation_t *)ctx;
  flowie_cluster_broadcast_target_owned_t target;
  int rc;
  if (!operation || !source || source->size != sizeof(*source) ||
      source->abi_version != FLOWIE_CLUSTER_SESSION_BIND_ABI_V1 ||
      (source->shared_filter.size != 0u && !source->shared_filter.data) ||
      turbo_vec_size(&operation->targets) >= operation->owner->config.max_targets)
    return TURBO_ENOSPC;
  memset(&target, 0, sizeof(target));
  target.view = *source;
  target.client_id = tstr_new_len(source->client_id.data, source->client_id.size);
  if (!target.client_id) return TURBO_ENOMEM;
  if (source->edge_node_id.len != 0u) {
    target.edge_node_id = tstr_from_v(source->edge_node_id);
    if (!target.edge_node_id) {
      flowie_cluster_broadcast_target_owned_destroy(&target);
      return TURBO_ENOMEM;
    }
  }
  if (source->shared_filter.size != 0u) {
    target.shared_filter =
        tstr_new_len(source->shared_filter.data, source->shared_filter.size);
    if (!target.shared_filter) {
      flowie_cluster_broadcast_target_owned_destroy(&target);
      return TURBO_ENOMEM;
    }
  }
  if (source->edge_boot_id)
    memcpy(target.edge_boot_id, source->edge_boot_id, sizeof(target.edge_boot_id));
  rc = turbo_vec_init(&target.subscription_identifiers, sizeof(uint32_t));
  if (rc != TURBO_OK) {
    flowie_cluster_broadcast_target_owned_destroy(&target);
    return rc;
  }
  target.identifiers_initialized = 1;
  if (source->subscription_identifier_count != 0u) {
    rc = turbo_vec_reserve(&target.subscription_identifiers,
                           source->subscription_identifier_count);
    for (size_t slot = 0u; rc == TURBO_OK && slot < source->subscription_identifier_count;
         ++slot)
      rc = turbo_vec_push(&target.subscription_identifiers,
                          &source->subscription_identifiers[slot]);
    if (rc != TURBO_OK) {
      flowie_cluster_broadcast_target_owned_destroy(&target);
      return rc;
    }
  }
  rc = turbo_vec_push(&operation->targets, &target);
  if (rc != TURBO_OK) flowie_cluster_broadcast_target_owned_destroy(&target);
  return rc;
}

static void flowie_cluster_broadcast_target_view(
    flowie_cluster_broadcast_target_owned_t *target,
    flowie_cluster_session_publish_target_t *out) {
  *out = target->view;
  out->client_id = (flowie_mqtt_span_t){(const uint8_t *)target->client_id,
                                        tstr_len(target->client_id)};
  out->edge_node_id = target->edge_node_id ? tstr_to_v(target->edge_node_id)
                                           : (tstr_v){NULL, 0u};
  out->edge_boot_id = target->view.edge_boot_id ? target->edge_boot_id : NULL;
  out->shared_filter =
      (flowie_mqtt_span_t){(const uint8_t *)target->shared_filter, tstr_len(target->shared_filter)};
  out->subscription_identifier_count = turbo_vec_size(&target->subscription_identifiers);
  out->subscription_identifiers =
      out->subscription_identifier_count != 0u
          ? (const uint32_t *)turbo_vec_at_const(&target->subscription_identifiers, 0u)
          : NULL;
}

static void flowie_cluster_broadcast_target_step(
    flowie_cluster_broadcast_target_operation_t *operation);

static void flowie_cluster_broadcast_target_request_finish(void *arg1, void *arg2) {
  flowie_cluster_broadcast_target_request_t *request =
      (flowie_cluster_broadcast_target_request_t *)arg1;
  flowie_cluster_broadcast_target_operation_t *operation;
  int rc;
  (void)arg2;
  if (!request || !(operation = request->operation)) {
    free(request);
    return;
  }
  if (request->kind == FLOWIE_CLUSTER_BROADCAST_TARGET_REQUEST_DELIVERY) {
    const flowie_cluster_pgsql_fact_command_t *command =
        flowie_cluster_session_delivery_plan_command(operation->plan);
    if (!command || memcmp(command->command_id, request->command_id,
                           FLOWIE_CLUSTER_COMMAND_ID_SIZE) != 0) {
      rc = TURBO_EPROTO;
      flowie_cluster_session_delivery_plan_destroy(&operation->plan);
    } else {
      rc = flowie_cluster_session_delivery_plan_finalize(&operation->plan, request->status);
    }
    free(request);
    if (rc != TURBO_OK) {
      flowie_cluster_broadcast_target_owner_finish(operation, rc);
      return;
    }
    turbo_mutex_lock(&operation->owner->mutex);
    rc = flowie_cluster_broadcast_target_owner_counter(
        &operation->owner->target_transactions);
    turbo_mutex_unlock(&operation->owner->mutex);
    if (rc != TURBO_OK) {
      flowie_cluster_broadcast_target_owner_finish(operation, rc);
      return;
    }
    operation->target_index += 1u;
    flowie_cluster_broadcast_target_step(operation);
    return;
  }
  rc = memcmp(operation->shard_ack.command_id, request->command_id,
              FLOWIE_CLUSTER_COMMAND_ID_SIZE) == 0
           ? request->status
           : TURBO_EPROTO;
  free(request);
  if (rc == TURBO_EALREADY) rc = TURBO_OK;
  if (rc == TURBO_OK) {
    turbo_mutex_lock(&operation->owner->mutex);
    rc = flowie_cluster_broadcast_target_owner_counter(
        &operation->owner->shard_ack_transactions);
    turbo_mutex_unlock(&operation->owner->mutex);
  }
  flowie_cluster_broadcast_target_owner_finish(operation, rc);
}

static void flowie_cluster_broadcast_target_request_complete(
    void *ctx, const uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE], int status) {
  flowie_cluster_broadcast_target_request_t *request =
      (flowie_cluster_broadcast_target_request_t *)ctx;
  int rc;
  if (!request || !command_id ||
      atomic_exchange_explicit(&request->completed, 1, memory_order_acq_rel) != 0)
    return;
  if (memcmp(request->command_id, command_id, FLOWIE_CLUSTER_COMMAND_ID_SIZE) != 0)
    status = TURBO_EPROTO;
  request->status = status;
  rc = tf_coronet_execution_post(request->operation->owner->config.execution,
                                 flowie_cluster_broadcast_target_request_finish, request, NULL);
  if (rc != TURBO_OK) {
    flowie_cluster_broadcast_target_operation_t *operation = request->operation;
    operation->owner->config.self_fence(operation->owner->config.self_fence_ctx, rc);
    free(request);
    flowie_cluster_broadcast_target_owner_finish(operation, TURBO_EPROTO);
  }
}

static int flowie_cluster_broadcast_target_submit(
    flowie_cluster_broadcast_target_operation_t *operation,
    flowie_cluster_broadcast_target_request_kind_t kind,
    const flowie_cluster_pgsql_fact_command_t *command) {
  flowie_cluster_broadcast_target_request_t *request;
  int rc;
  if (!operation || !command) return TURBO_EINVAL;
  request = (flowie_cluster_broadcast_target_request_t *)calloc(1u, sizeof(*request));
  if (!request) return TURBO_ENOMEM;
  request->operation = operation;
  request->kind = kind;
  memcpy(request->command_id, command->command_id, sizeof(request->command_id));
  atomic_init(&request->completed, 0);
  rc = operation->owner->config.submit(
      operation->owner->config.submit_ctx, command,
      flowie_cluster_broadcast_target_request_complete, request);
  if (rc != TURBO_OK) free(request);
  return rc;
}

static void flowie_cluster_broadcast_target_step(
    flowie_cluster_broadcast_target_operation_t *operation) {
  flowie_cluster_broadcast_target_owner_t *owner = operation->owner;
  int rc;
  if (operation->target_index < turbo_vec_size(&operation->targets)) {
    flowie_cluster_broadcast_target_owned_t *target =
        (flowie_cluster_broadcast_target_owned_t *)turbo_vec_at(
            &operation->targets, operation->target_index);
    flowie_cluster_session_publish_target_t view = FLOWIE_CLUSTER_SESSION_PUBLISH_TARGET_INIT;
    if (!target) {
      flowie_cluster_broadcast_target_owner_finish(operation, TURBO_EPROTO);
      return;
    }
    flowie_cluster_broadcast_target_view(target, &view);
    rc = flowie_cluster_session_delivery_plan_create(
        owner->config.session_bind, &operation->owner_token, &operation->event,
        operation->digest, &view, &operation->plan);
    if (rc == TURBO_OK)
      rc = flowie_cluster_broadcast_target_submit(
          operation, FLOWIE_CLUSTER_BROADCAST_TARGET_REQUEST_DELIVERY,
          flowie_cluster_session_delivery_plan_command(operation->plan));
    if (rc != TURBO_OK) {
      flowie_cluster_session_delivery_plan_destroy(&operation->plan);
      flowie_cluster_broadcast_target_owner_finish(operation, rc);
    }
    return;
  }
  rc = owner->config.resolve(owner->config.resolve_ctx, owner->config.shard_id,
                             &operation->owner_token);
  if (rc == TURBO_OK && operation->owner_token.shard_id != owner->config.shard_id)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_broadcast_shard_ack_command(
        &operation->owner_token, &operation->event, operation->digest,
        &operation->shard_ack_dedupe, &operation->shard_ack);
  if (rc == TURBO_OK)
    rc = flowie_cluster_broadcast_target_submit(
        operation, FLOWIE_CLUSTER_BROADCAST_TARGET_REQUEST_SHARD_ACK,
        &operation->shard_ack);
  if (rc != TURBO_OK) flowie_cluster_broadcast_target_owner_finish(operation, rc);
}

static void flowie_cluster_broadcast_target_start(void *arg1, void *arg2) {
  flowie_cluster_broadcast_target_operation_t *operation =
      (flowie_cluster_broadcast_target_operation_t *)arg1;
  flowie_cluster_broadcast_target_owner_t *owner;
  size_t target_count = 0u;
  int rc;
  (void)arg2;
  if (!operation || !(owner = operation->owner)) return;
  rc = flowie_cluster_broadcast_event_decode(operation->payload, tstr_len(operation->payload),
                                             owner->config.max_payload_size, &operation->event);
  if (rc == TURBO_OK)
    rc = flowie_cluster_broadcast_event_digest(
        operation->payload, tstr_len(operation->payload), owner->config.max_payload_size,
        operation->digest);
  if (rc == TURBO_OK)
    rc = owner->config.resolve(owner->config.resolve_ctx, owner->config.shard_id,
                               &operation->owner_token);
  if (rc == TURBO_OK && operation->owner_token.shard_id != owner->config.shard_id)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = turbo_vec_init(&operation->targets,
                        sizeof(flowie_cluster_broadcast_target_owned_t));
  if (rc == TURBO_OK) operation->targets_initialized = 1;
  if (rc == TURBO_OK) rc = turbo_vec_reserve(&operation->targets, owner->config.max_targets);
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_bind_match_publish(
        owner->config.session_bind, &operation->event.publish,
        flowie_cluster_broadcast_target_copy, operation, &target_count);
  if (rc == TURBO_OK && target_count != turbo_vec_size(&operation->targets)) rc = TURBO_EPROTO;
  if (rc != TURBO_OK) {
    flowie_cluster_broadcast_target_owner_finish(operation, rc);
    return;
  }
  turbo_mutex_lock(&owner->mutex);
  owner->current_target_count = target_count;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
  flowie_cluster_broadcast_target_step(operation);
}

int flowie_cluster_broadcast_target_owner_create(
    const flowie_cluster_broadcast_target_owner_config_t *config,
    flowie_cluster_broadcast_target_owner_t **out) {
  flowie_cluster_broadcast_target_owner_t *owner;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_broadcast_target_owner_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  owner = (flowie_cluster_broadcast_target_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  owner->config = *config;
  owner->accepting = 1;
  owner->last_status = TURBO_OK;
  turbo_mutex_init(&owner->mutex);
  turbo_cond_init(&owner->changed);
  *out = owner;
  return TURBO_OK;
}

int flowie_cluster_broadcast_target_owner_apply(
    void *ctx, const void *payload, size_t payload_size,
    flowie_cluster_broadcast_target_apply_complete_fn complete, void *completion_ctx) {
  flowie_cluster_broadcast_target_owner_t *owner =
      (flowie_cluster_broadcast_target_owner_t *)ctx;
  flowie_cluster_broadcast_target_operation_t *operation;
  int rc;
  if (!owner || !payload || payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      !complete)
    return TURBO_EINVAL;
  if (payload_size > owner->config.max_payload_size) return TURBO_EMSGSIZE;
  operation = (flowie_cluster_broadcast_target_operation_t *)calloc(1u, sizeof(*operation));
  if (!operation) return TURBO_ENOMEM;
  operation->payload = tstr_new_len(payload, payload_size);
  if (!operation->payload) {
    free(operation);
    return TURBO_ENOMEM;
  }
  operation->owner = owner;
  operation->event = (flowie_cluster_broadcast_event_view_t)FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
  operation->shard_ack_dedupe =
      (flowie_cluster_pgsql_event_dedupe_t)FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
  operation->shard_ack =
      (flowie_cluster_pgsql_fact_command_t)FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  operation->complete = complete;
  operation->completion_ctx = completion_ctx;
  turbo_mutex_lock(&owner->mutex);
  if (!owner->accepting) rc = TURBO_ESHUTDOWN;
  else if (owner->active) rc = TURBO_EBUSY;
  else {
    rc = flowie_cluster_broadcast_target_owner_counter(&owner->admitted_events);
    if (rc == TURBO_OK) owner->active = 1;
  }
  turbo_mutex_unlock(&owner->mutex);
  if (rc != TURBO_OK) {
    flowie_cluster_broadcast_target_operation_destroy(operation);
    return rc;
  }
  rc = tf_coronet_execution_post(owner->config.execution,
                                 flowie_cluster_broadcast_target_start, operation, NULL);
  if (rc != TURBO_OK) {
    flowie_cluster_broadcast_target_operation_destroy(operation);
    turbo_mutex_lock(&owner->mutex);
    owner->active = 0;
    owner->last_status = rc;
    turbo_cond_broadcast(&owner->changed);
    turbo_mutex_unlock(&owner->mutex);
  }
  return rc;
}

int flowie_cluster_broadcast_target_owner_snapshot(
    flowie_cluster_broadcast_target_owner_t *owner,
    flowie_cluster_broadcast_target_owner_snapshot_t *out) {
  if (!owner || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&owner->mutex);
  out->last_status = owner->last_status;
  out->admitted_events = owner->admitted_events;
  out->completed_events = owner->completed_events;
  out->target_transactions = owner->target_transactions;
  out->shard_ack_transactions = owner->shard_ack_transactions;
  out->current_target_count = owner->current_target_count;
  out->accepting = owner->accepting;
  out->active = owner->active;
  turbo_mutex_unlock(&owner->mutex);
  return TURBO_OK;
}

int flowie_cluster_broadcast_target_owner_close(flowie_cluster_broadcast_target_owner_t *owner) {
  int rc = TURBO_OK;
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->mutex);
  if (!owner->accepting) rc = TURBO_EALREADY;
  else owner->accepting = 0;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
  return rc;
}

int flowie_cluster_broadcast_target_owner_drain(flowie_cluster_broadcast_target_owner_t *owner,
                                                uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc = TURBO_OK;
  if (!owner) return TURBO_EINVAL;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  turbo_mutex_lock(&owner->mutex);
  if (owner->accepting) rc = TURBO_EBUSY;
  while (rc == TURBO_OK && owner->active) {
    uint64_t now_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&owner->changed, &owner->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) {
      rc = timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&owner->changed, &owner->mutex, deadline_ns - now_ns);
  }
  turbo_mutex_unlock(&owner->mutex);
  return rc;
}

int flowie_cluster_broadcast_target_owner_destroy(flowie_cluster_broadcast_target_owner_t *owner) {
  int ready;
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->mutex);
  ready = !owner->accepting && !owner->active;
  turbo_mutex_unlock(&owner->mutex);
  if (!ready) return TURBO_EBUSY;
  turbo_cond_destroy(&owner->changed);
  turbo_mutex_destroy(&owner->mutex);
  free(owner);
  return TURBO_OK;
}
