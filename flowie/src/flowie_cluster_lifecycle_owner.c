#include "flowie_cluster_lifecycle_owner_internal.h"

#include "flow_coronet_execution.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_lifecycle_operation_s {
  struct flowie_cluster_lifecycle_owner_s *owner;
  flowie_cluster_owner_token_t owner_token;
  flowie_cluster_lifecycle_event_view_t event;
  tstr_t client_id;
  tstr_t edge_node_id;
  uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_session_lifecycle_plan_t *plan;
  flowie_cluster_lifecycle_apply_complete_fn complete;
  void *completion_ctx;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  int durable_status;
  atomic_int durable_completed;
} flowie_cluster_lifecycle_operation_t;

struct flowie_cluster_lifecycle_owner_s {
  flowie_cluster_lifecycle_owner_config_t config;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int accepting;
  int active;
  int last_status;
  uint64_t admitted_events;
  uint64_t completed_events;
  uint64_t durable_transactions;
};

int flowie_cluster_lifecycle_owner_config_validate(
    const flowie_cluster_lifecycle_owner_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_LIFECYCLE_OWNER_ABI_V1 || !config->execution ||
      !config->execution->context || !config->session_bind || !config->submit || !config->now ||
      !config->self_fence)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_lifecycle_owner_counter(uint64_t *counter) {
  if (!counter || *counter == UINT64_MAX) return TURBO_ERANGE;
  ++*counter;
  return TURBO_OK;
}

static void
flowie_cluster_lifecycle_operation_destroy(flowie_cluster_lifecycle_operation_t *operation) {
  if (!operation) return;
  flowie_cluster_session_lifecycle_plan_destroy(&operation->plan);
  tstr_freep(&operation->edge_node_id);
  tstr_freep(&operation->client_id);
  free(operation);
}

static void flowie_cluster_lifecycle_owner_finish(flowie_cluster_lifecycle_operation_t *operation,
                                                  int status) {
  flowie_cluster_lifecycle_owner_t *owner;
  flowie_cluster_lifecycle_apply_complete_fn complete;
  void *completion_ctx;
  if (!operation) return;
  owner = operation->owner;
  complete = operation->complete;
  completion_ctx = operation->completion_ctx;
  flowie_cluster_lifecycle_operation_destroy(operation);
  turbo_mutex_lock(&owner->mutex);
  owner->last_status = status;
  if (status == TURBO_OK) {
    int counter_rc = flowie_cluster_lifecycle_owner_counter(&owner->completed_events);
    if (counter_rc != TURBO_OK) {
      owner->last_status = counter_rc;
      status = counter_rc;
    }
  }
  owner->active = 0;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
  complete(completion_ctx, status);
}

static void flowie_cluster_lifecycle_durable_finish(void *arg1, void *arg2) {
  flowie_cluster_lifecycle_operation_t *operation = (flowie_cluster_lifecycle_operation_t *)arg1;
  const flowie_cluster_pgsql_fact_command_t *command;
  int rc;
  (void)arg2;
  if (!operation) return;
  command = flowie_cluster_session_lifecycle_plan_command(operation->plan);
  if (!command ||
      memcmp(command->command_id, operation->command_id, FLOWIE_CLUSTER_COMMAND_ID_SIZE) != 0) {
    flowie_cluster_session_lifecycle_plan_destroy(&operation->plan);
    flowie_cluster_lifecycle_owner_finish(operation, TURBO_EPROTO);
    return;
  }
  rc = flowie_cluster_session_lifecycle_plan_finalize(&operation->plan, operation->durable_status);
  if (rc == TURBO_OK || rc == TURBO_EBUSY) {
    turbo_mutex_lock(&operation->owner->mutex);
    {
      int counter_rc =
          flowie_cluster_lifecycle_owner_counter(&operation->owner->durable_transactions);
      if (counter_rc != TURBO_OK) rc = counter_rc;
    }
    turbo_mutex_unlock(&operation->owner->mutex);
  }
  flowie_cluster_lifecycle_owner_finish(operation, rc);
}

static void flowie_cluster_lifecycle_durable_complete(
    void *ctx, const uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE], int status) {
  flowie_cluster_lifecycle_operation_t *operation = (flowie_cluster_lifecycle_operation_t *)ctx;
  int rc;
  if (!operation || !command_id ||
      atomic_exchange_explicit(&operation->durable_completed, 1, memory_order_acq_rel) != 0)
    return;
  if (memcmp(operation->command_id, command_id, FLOWIE_CLUSTER_COMMAND_ID_SIZE) != 0)
    status = TURBO_EPROTO;
  operation->durable_status = status;
  rc = tf_coronet_execution_post(operation->owner->config.execution,
                                 flowie_cluster_lifecycle_durable_finish, operation, NULL);
  if (rc != TURBO_OK) {
    flowie_cluster_lifecycle_owner_t *owner = operation->owner;
    owner->config.self_fence(owner->config.self_fence_ctx, rc);
    flowie_cluster_session_lifecycle_plan_destroy(&operation->plan);
    flowie_cluster_lifecycle_owner_finish(operation, TURBO_EPROTO);
  }
}

static void flowie_cluster_lifecycle_start(void *arg1, void *arg2) {
  flowie_cluster_lifecycle_operation_t *operation = (flowie_cluster_lifecycle_operation_t *)arg1;
  const flowie_cluster_pgsql_fact_command_t *command;
  uint64_t now_epoch_seconds;
  int rc;
  (void)arg2;
  if (!operation) return;
  now_epoch_seconds = operation->owner->config.now(operation->owner->config.now_ctx);
  if (now_epoch_seconds == 0u) {
    flowie_cluster_lifecycle_owner_finish(operation, TURBO_EIO);
    return;
  }
  rc = flowie_cluster_session_lifecycle_plan_create(operation->owner->config.session_bind,
                                                    &operation->owner_token, &operation->event,
                                                    now_epoch_seconds, &operation->plan);
  if (rc != TURBO_OK) {
    flowie_cluster_lifecycle_owner_finish(operation, rc);
    return;
  }
  command = flowie_cluster_session_lifecycle_plan_command(operation->plan);
  if (!command) {
    rc = flowie_cluster_session_lifecycle_plan_finalize(&operation->plan, TURBO_OK);
    flowie_cluster_lifecycle_owner_finish(operation, rc);
    return;
  }
  memcpy(operation->command_id, command->command_id, sizeof(operation->command_id));
  atomic_init(&operation->durable_completed, 0);
  rc = operation->owner->config.submit(operation->owner->config.submit_ctx, command,
                                       flowie_cluster_lifecycle_durable_complete, operation);
  if (rc != TURBO_OK) {
    flowie_cluster_session_lifecycle_plan_destroy(&operation->plan);
    flowie_cluster_lifecycle_owner_finish(operation, rc);
  }
}

int flowie_cluster_lifecycle_owner_create(const flowie_cluster_lifecycle_owner_config_t *config,
                                          flowie_cluster_lifecycle_owner_t **out) {
  flowie_cluster_lifecycle_owner_t *owner;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_lifecycle_owner_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  owner = (flowie_cluster_lifecycle_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  owner->config = *config;
  owner->accepting = 1;
  owner->last_status = TURBO_OK;
  turbo_mutex_init(&owner->mutex);
  turbo_cond_init(&owner->changed);
  *out = owner;
  return TURBO_OK;
}

int flowie_cluster_lifecycle_owner_apply(void *ctx,
                                         const flowie_cluster_owner_token_t *current_owner,
                                         const flowie_cluster_lifecycle_event_view_t *event,
                                         flowie_cluster_lifecycle_apply_complete_fn complete,
                                         void *completion_ctx) {
  flowie_cluster_lifecycle_owner_t *owner = (flowie_cluster_lifecycle_owner_t *)ctx;
  flowie_cluster_lifecycle_operation_t *operation;
  int rc = TURBO_OK;
  if (!owner || !current_owner || current_owner->size != sizeof(*current_owner) ||
      current_owner->abi_version != FLOWIE_CLUSTER_INTERNAL_ABI_V1 || !event ||
      event->size != sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1 ||
      event->shard_id != owner->config.shard_id || !event->client_id.data ||
      event->client_id.size == 0u || event->client_id.size > UINT16_MAX ||
      !event->edge_node_id.data || event->edge_node_id.size == 0u ||
      event->edge_node_id.size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      memchr(event->edge_node_id.data, '\0', event->edge_node_id.size) || !event->edge_boot_id ||
      !complete)
    return TURBO_EINVAL;
  operation = (flowie_cluster_lifecycle_operation_t *)calloc(1u, sizeof(*operation));
  if (!operation) return TURBO_ENOMEM;
  operation->owner = owner;
  operation->owner_token = *current_owner;
  operation->event = *event;
  operation->client_id = tstr_new_len(event->client_id.data, event->client_id.size);
  operation->edge_node_id = tstr_new_len(event->edge_node_id.data, event->edge_node_id.size);
  if (!operation->client_id || !operation->edge_node_id) {
    flowie_cluster_lifecycle_operation_destroy(operation);
    return TURBO_ENOMEM;
  }
  memcpy(operation->edge_boot_id, event->edge_boot_id, sizeof(operation->edge_boot_id));
  operation->event.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)operation->client_id, tstr_len(operation->client_id)};
  operation->event.edge_node_id = (flowie_mqtt_span_t){(const uint8_t *)operation->edge_node_id,
                                                       tstr_len(operation->edge_node_id)};
  operation->event.edge_boot_id = operation->edge_boot_id;
  operation->complete = complete;
  operation->completion_ctx = completion_ctx;
  turbo_mutex_lock(&owner->mutex);
  if (!owner->accepting) rc = TURBO_ESHUTDOWN;
  else if (owner->active) rc = TURBO_EBUSY;
  else {
    rc = flowie_cluster_lifecycle_owner_counter(&owner->admitted_events);
    if (rc == TURBO_OK) owner->active = 1;
  }
  turbo_mutex_unlock(&owner->mutex);
  if (rc != TURBO_OK) {
    flowie_cluster_lifecycle_operation_destroy(operation);
    return rc;
  }
  rc = tf_coronet_execution_post(owner->config.execution, flowie_cluster_lifecycle_start, operation,
                                 NULL);
  if (rc != TURBO_OK) {
    flowie_cluster_lifecycle_operation_destroy(operation);
    turbo_mutex_lock(&owner->mutex);
    owner->active = 0;
    owner->last_status = rc;
    turbo_cond_broadcast(&owner->changed);
    turbo_mutex_unlock(&owner->mutex);
  }
  return rc;
}

int flowie_cluster_lifecycle_owner_snapshot(flowie_cluster_lifecycle_owner_t *owner,
                                            flowie_cluster_lifecycle_owner_snapshot_t *out) {
  if (!owner || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_LIFECYCLE_OWNER_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&owner->mutex);
  out->last_status = owner->last_status;
  out->admitted_events = owner->admitted_events;
  out->completed_events = owner->completed_events;
  out->durable_transactions = owner->durable_transactions;
  out->accepting = owner->accepting;
  out->active = owner->active;
  turbo_mutex_unlock(&owner->mutex);
  return TURBO_OK;
}

int flowie_cluster_lifecycle_owner_close(flowie_cluster_lifecycle_owner_t *owner) {
  int rc = TURBO_OK;
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->mutex);
  if (!owner->accepting) rc = TURBO_EALREADY;
  else owner->accepting = 0;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
  return rc;
}

int flowie_cluster_lifecycle_owner_drain(flowie_cluster_lifecycle_owner_t *owner,
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

int flowie_cluster_lifecycle_owner_destroy(flowie_cluster_lifecycle_owner_t *owner) {
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
