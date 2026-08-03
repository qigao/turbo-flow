#include "flowie_cluster_peer_internal.h"

#include "flow_coronet_actor.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_peer_owner_async_completion_s {
  struct flowie_cluster_peer_owner_s *owner;
  flowie_cluster_peer_frame_t command;
  tstr_t source_node_id;
  tstr_t reply_payload;
  size_t command_bytes;
  int durable_status;
  flowie_cluster_peer_owner_finalize_fn finalize;
  void *finalize_ctx;
  atomic_uint refs;
  atomic_int claimed;
} flowie_cluster_peer_owner_async_completion_t;

struct flowie_cluster_peer_owner_s {
  tf_coronet_actor_t actor;
  tf_coronet_execution_t *execution;
  size_t max_payload_size;
  size_t queue_bytes_limit;
  size_t pending_bytes;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int accepting;
  int async_inflight;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_peer_owner_resolve_fn resolve;
  flowie_cluster_peer_owner_execute_fn execute;
  flowie_cluster_peer_owner_execute_async_fn execute_async;
  flowie_cluster_peer_owner_reply_fn reply;
  void *user_data;
};

static int flowie_cluster_peer_owner_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_peer_owner_text_validate(tstr_v value, size_t maximum) {
  if (!value.data || value.len == 0u) return TURBO_EINVAL;
  if (value.len > maximum) return TURBO_EMSGSIZE;
  return memchr(value.data, '\0', value.len) ? TURBO_EPROTO : TURBO_OK;
}

static int
flowie_cluster_peer_owner_config_validate(const flowie_cluster_peer_owner_config_t *config,
                                          size_t *max_frame_size) {
  size_t identity_overhead =
      FLOWIE_CLUSTER_ID_MAX + FLOWIE_CLUSTER_LISTENER_ID_MAX + FLOWIE_CLUSTER_NODE_ID_MAX * 2u;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_OWNER_ABI_V1 || !config->execution ||
      !config->execution->context || config->max_payload_size == 0u ||
      config->max_payload_size > UINT32_MAX || config->queue_entries == 0u ||
      config->queue_bytes == 0u || !config->resolve ||
      ((!config->execute) == (!config->execute_async)) || !config->reply || !max_frame_size) {
    return TURBO_EINVAL;
  }
  if (config->max_payload_size > SIZE_MAX - identity_overhead - FLOWIE_CLUSTER_PEER_HEADER_SIZE)
    return TURBO_ERANGE;
  *max_frame_size = FLOWIE_CLUSTER_PEER_HEADER_SIZE + identity_overhead + config->max_payload_size;
  if (*max_frame_size > UINT32_MAX || config->queue_bytes < *max_frame_size) return TURBO_EINVAL;
  rc = flowie_cluster_peer_owner_text_validate(config->cluster_id, FLOWIE_CLUSTER_ID_MAX);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_owner_text_validate(config->listener_id,
                                                 FLOWIE_CLUSTER_LISTENER_ID_MAX);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_owner_text_validate(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX);
  if (rc != TURBO_OK) return rc;
  return flowie_cluster_peer_owner_nonzero(config->local_boot_id, sizeof(config->local_boot_id))
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flowie_cluster_peer_owner_route_require(flowie_cluster_peer_owner_t *owner,
                                                   const flowie_cluster_peer_frame_t *command) {
  if (command->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      !tstr_v_eq(command->cluster_id, tstr_to_v(owner->cluster_id)) ||
      !tstr_v_eq(command->listener_id, tstr_to_v(owner->listener_id)) ||
      !tstr_v_eq(command->target_node_id, tstr_to_v(owner->local_node_id)) ||
      memcmp(command->target_boot_id, owner->local_boot_id, sizeof(owner->local_boot_id)) != 0) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static void flowie_cluster_peer_owner_account(flowie_cluster_peer_owner_t *owner, size_t bytes) {
  turbo_mutex_lock(&owner->mutex);
  if (bytes <= owner->pending_bytes) owner->pending_bytes -= bytes;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
}

static int flowie_cluster_peer_owner_async_reserve(flowie_cluster_peer_owner_t *owner) {
  int rc;
  turbo_mutex_lock(&owner->mutex);
  if (owner->async_inflight) {
    rc = TURBO_EBUSY;
  } else {
    owner->async_inflight = 1;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&owner->mutex);
  return rc;
}

static void flowie_cluster_peer_owner_async_release(flowie_cluster_peer_owner_t *owner) {
  turbo_mutex_lock(&owner->mutex);
  owner->async_inflight = 0;
  turbo_cond_broadcast(&owner->changed);
  turbo_mutex_unlock(&owner->mutex);
}

static void flowie_cluster_peer_owner_reply_init(flowie_cluster_peer_owner_t *owner,
                                                 const flowie_cluster_peer_frame_t *command,
                                                 int status, tstr_t payload,
                                                 flowie_cluster_peer_frame_t *reply) {
  *reply = (flowie_cluster_peer_frame_t)FLOWIE_CLUSTER_PEER_FRAME_INIT;
  reply->kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply->operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
  reply->shard_id = command->shard_id;
  reply->status = status;
  reply->owner_epoch = command->owner_epoch;
  reply->connection_id = command->connection_id;
  reply->connection_generation = command->connection_generation;
  reply->cluster_id = tstr_to_v(owner->cluster_id);
  reply->listener_id = tstr_to_v(owner->listener_id);
  reply->source_node_id = tstr_to_v(owner->local_node_id);
  reply->target_node_id = command->source_node_id;
  reply->payload = payload ? tstr_to_v(payload) : tstr_v_from_buf(NULL, 0u);
  memcpy(reply->source_boot_id, owner->local_boot_id, sizeof(reply->source_boot_id));
  memcpy(reply->target_boot_id, command->source_boot_id, sizeof(reply->target_boot_id));
  memcpy(reply->correlation_id, command->correlation_id, sizeof(reply->correlation_id));
}

static void flowie_cluster_peer_owner_async_completion_release(
    flowie_cluster_peer_owner_async_completion_t *completion) {
  if (!completion || atomic_fetch_sub_explicit(&completion->refs, 1u, memory_order_acq_rel) != 1u)
    return;
  tstr_freep(&completion->reply_payload);
  tstr_freep(&completion->source_node_id);
  free(completion);
}

static flowie_cluster_peer_owner_async_completion_t *
flowie_cluster_peer_owner_async_completion_create(flowie_cluster_peer_owner_t *owner,
                                                  const flowie_cluster_peer_frame_t *command,
                                                  size_t command_bytes) {
  flowie_cluster_peer_owner_async_completion_t *completion;
  if (!owner || !command || command_bytes == 0u) return NULL;
  completion = (flowie_cluster_peer_owner_async_completion_t *)calloc(1u, sizeof(*completion));
  if (!completion) return NULL;
  completion->source_node_id = tstr_from_v(command->source_node_id);
  if (!completion->source_node_id) {
    free(completion);
    return NULL;
  }
  completion->owner = owner;
  completion->command = (flowie_cluster_peer_frame_t)FLOWIE_CLUSTER_PEER_FRAME_INIT;
  completion->command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  completion->command.operation = command->operation;
  completion->command.shard_id = command->shard_id;
  completion->command.owner_epoch = command->owner_epoch;
  completion->command.connection_id = command->connection_id;
  completion->command.connection_generation = command->connection_generation;
  completion->command.source_node_id = tstr_to_v(completion->source_node_id);
  memcpy(completion->command.source_boot_id, command->source_boot_id,
         sizeof(completion->command.source_boot_id));
  memcpy(completion->command.correlation_id, command->correlation_id,
         sizeof(completion->command.correlation_id));
  completion->command_bytes = command_bytes;
  atomic_init(&completion->refs, 2u);
  atomic_init(&completion->claimed, 0);
  return completion;
}

static void flowie_cluster_peer_owner_async_finish(void *arg1, void *arg2) {
  flowie_cluster_peer_owner_async_completion_t *completion =
      (flowie_cluster_peer_owner_async_completion_t *)arg1;
  flowie_cluster_peer_frame_t reply;
  int status;
  (void)arg2;
  if (!completion) return;
  status = completion->finalize(completion->finalize_ctx, completion->durable_status,
                                &completion->reply_payload);
  completion->finalize_ctx = NULL;
  if ((status != TURBO_OK && completion->reply_payload) ||
      (completion->reply_payload &&
       tstr_len(completion->reply_payload) > completion->owner->max_payload_size)) {
    tstr_freep(&completion->reply_payload);
    status = status == TURBO_OK ? TURBO_EMSGSIZE : TURBO_EPROTO;
  }
  flowie_cluster_peer_owner_reply_init(completion->owner, &completion->command, status,
                                       completion->reply_payload, &reply);
  (void)completion->owner->reply(completion->owner->user_data, &reply);
  flowie_cluster_peer_owner_async_release(completion->owner);
  flowie_cluster_peer_owner_account(completion->owner, completion->command_bytes);
  flowie_cluster_peer_owner_async_completion_release(completion);
}

static int flowie_cluster_peer_owner_async_complete(void *completion_ctx, int durable_status,
                                                    flowie_cluster_peer_owner_finalize_fn finalize,
                                                    void *finalize_ctx) {
  flowie_cluster_peer_owner_async_completion_t *completion =
      (flowie_cluster_peer_owner_async_completion_t *)completion_ctx;
  int rc;
  if (!completion || !finalize) return TURBO_EINVAL;
  if (atomic_exchange_explicit(&completion->claimed, 1, memory_order_acq_rel) != 0) {
    return TURBO_EALREADY;
  }
  completion->durable_status = durable_status;
  completion->finalize = finalize;
  completion->finalize_ctx = finalize_ctx;
  rc = tf_coronet_execution_post(completion->owner->execution,
                                 flowie_cluster_peer_owner_async_finish, completion, NULL);
  if (rc != TURBO_OK) {
    flowie_cluster_peer_owner_async_release(completion->owner);
    flowie_cluster_peer_owner_account(completion->owner, completion->command_bytes);
    flowie_cluster_peer_owner_async_completion_release(completion);
  }
  return rc;
}

static int flowie_cluster_peer_owner_execute(void *ctx, uint64_t command_id, const void *bytes,
                                             size_t size) {
  flowie_cluster_peer_owner_t *owner = (flowie_cluster_peer_owner_t *)ctx;
  flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  flowie_cluster_peer_frame_t reply;
  flowie_cluster_owner_token_t expected = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  flowie_cluster_owner_token_t presented = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  tstr_t reply_payload = NULL;
  size_t consumed = 0u;
  int rc;
  int reply_rc;
  int async_accepted = 0;
  (void)command_id;

  rc = flowie_cluster_peer_frame_decode(bytes, size, owner->max_payload_size, &command, &consumed);
  if (rc == TURBO_OK && consumed != size) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_cluster_peer_owner_route_require(owner, &command);
  if (rc == TURBO_OK) rc = owner->resolve(owner->user_data, command.shard_id, &expected);
  if (rc == TURBO_OK)
    rc = flowie_cluster_owner_token_init(&presented, command.shard_id, command.owner_epoch,
                                         owner->local_node_id, tstr_len(owner->local_node_id),
                                         owner->local_boot_id);
  if (rc == TURBO_OK) rc = flowie_cluster_owner_token_require(&expected, &presented);
  if (rc == TURBO_OK && owner->execute_async) {
    flowie_cluster_peer_owner_async_completion_t *completion = NULL;
    rc = flowie_cluster_peer_owner_async_reserve(owner);
    if (rc == TURBO_OK)
      completion = flowie_cluster_peer_owner_async_completion_create(owner, &command, size);
    if (rc == TURBO_OK && !completion) rc = TURBO_ENOMEM;
    if (completion) {
      rc = owner->execute_async(owner->user_data, &command,
                                flowie_cluster_peer_owner_async_complete, completion);
      if (rc == TURBO_OK) {
        async_accepted = 1;
      } else if (atomic_load_explicit(&completion->claimed, memory_order_acquire) != 0) {
        rc = TURBO_EPROTO;
        async_accepted = 1;
      } else {
        flowie_cluster_peer_owner_async_release(owner);
        flowie_cluster_peer_owner_async_completion_release(completion);
      }
      flowie_cluster_peer_owner_async_completion_release(completion);
    } else if (rc != TURBO_EBUSY) {
      flowie_cluster_peer_owner_async_release(owner);
    }
  } else if (rc == TURBO_OK) {
    rc = owner->execute(owner->user_data, &command, &reply_payload);
  }

  if (!async_accepted && command.size == sizeof(command) &&
      command.kind == FLOWIE_CLUSTER_PEER_FRAME_COMMAND) {
    flowie_cluster_peer_owner_reply_init(owner, &command, rc, reply_payload, &reply);
    reply_rc = owner->reply(owner->user_data, &reply);
    if (rc == TURBO_OK && reply_rc != TURBO_OK) rc = reply_rc;
  }
  tstr_free(reply_payload);
  flowie_cluster_peer_frame_cleanup(&command);
  if (!async_accepted) flowie_cluster_peer_owner_account(owner, size);
  return rc;
}

static void flowie_cluster_peer_owner_storage_destroy(flowie_cluster_peer_owner_t *owner) {
  if (!owner) return;
  tstr_freep(&owner->cluster_id);
  tstr_freep(&owner->listener_id);
  tstr_freep(&owner->local_node_id);
  turbo_cond_destroy(&owner->changed);
  turbo_mutex_destroy(&owner->mutex);
  free(owner);
}

int flowie_cluster_peer_owner_create(const flowie_cluster_peer_owner_config_t *config,
                                     flowie_cluster_peer_owner_t **out) {
  flowie_cluster_peer_owner_t *owner;
  size_t max_frame_size = 0u;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = flowie_cluster_peer_owner_config_validate(config, &max_frame_size);
  if (rc != TURBO_OK) return rc;
  owner = (flowie_cluster_peer_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  turbo_mutex_init(&owner->mutex);
  turbo_cond_init(&owner->changed);
  owner->execution = config->execution;
  owner->max_payload_size = config->max_payload_size;
  owner->queue_bytes_limit = config->queue_bytes;
  owner->cluster_id = tstr_from_v(config->cluster_id);
  owner->listener_id = tstr_from_v(config->listener_id);
  owner->local_node_id = tstr_from_v(config->local_node_id);
  if (!owner->cluster_id || !owner->listener_id || !owner->local_node_id) {
    flowie_cluster_peer_owner_storage_destroy(owner);
    return TURBO_ENOMEM;
  }
  memcpy(owner->local_boot_id, config->local_boot_id, sizeof(owner->local_boot_id));
  owner->resolve = config->resolve;
  owner->execute = config->execute;
  owner->execute_async = config->execute_async;
  owner->reply = config->reply;
  owner->user_data = config->user_data;
  rc = tf_coronet_actor_init(&owner->actor, config->execution, flowie_cluster_peer_owner_execute,
                             owner, config->queue_entries, max_frame_size);
  if (rc != TURBO_OK) {
    flowie_cluster_peer_owner_storage_destroy(owner);
    return rc;
  }
  owner->accepting = 1;
  *out = owner;
  return TURBO_OK;
}

int flowie_cluster_peer_owner_submit(flowie_cluster_peer_owner_t *owner,
                                     const flowie_cluster_peer_frame_t *command) {
  tf_coronet_actor_reply_t *actor_reply = NULL;
  tstr_t encoded = NULL;
  size_t bytes;
  int rc;
  if (!owner || !command) return TURBO_EINVAL;
  rc = flowie_cluster_peer_owner_route_require(owner, command);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_frame_encode(command, owner->max_payload_size, &encoded);
  if (rc != TURBO_OK) return rc;
  bytes = tstr_len(encoded);
  turbo_mutex_lock(&owner->mutex);
  if (!owner->accepting) rc = TURBO_ESHUTDOWN;
  else if (bytes > owner->queue_bytes_limit - owner->pending_bytes) rc = TURBO_ENOSPC;
  else {
    owner->pending_bytes += bytes;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&owner->mutex);
  if (rc == TURBO_OK) {
    rc = tf_coronet_actor_submit(&owner->actor, encoded, bytes, UINT64_MAX, &actor_reply);
    if (rc != TURBO_OK) flowie_cluster_peer_owner_account(owner, bytes);
  }
  tf_coronet_actor_reply_destroy(actor_reply);
  tstr_free(encoded);
  return rc;
}

int flowie_cluster_peer_owner_close(flowie_cluster_peer_owner_t *owner) {
  if (!owner) return TURBO_EINVAL;
  turbo_mutex_lock(&owner->mutex);
  if (!owner->accepting) {
    turbo_mutex_unlock(&owner->mutex);
    return TURBO_EALREADY;
  }
  owner->accepting = 0;
  turbo_mutex_unlock(&owner->mutex);
  tf_coronet_actor_close(&owner->actor);
  return TURBO_OK;
}

int flowie_cluster_peer_owner_drain(flowie_cluster_peer_owner_t *owner, uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc;
  if (!owner) return TURBO_EINVAL;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  rc = tf_coronet_actor_drain(&owner->actor, timeout_ns);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&owner->mutex);
  while (owner->pending_bytes != 0u) {
    uint64_t now_ns;
    uint64_t remaining_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&owner->changed, &owner->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    remaining_ns = deadline_ns - now_ns;
    if (turbo_cond_timedwait(&owner->changed, &owner->mutex, remaining_ns) != TURBO_OK) break;
  }
  rc = owner->pending_bytes == 0u ? TURBO_OK : timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
  turbo_mutex_unlock(&owner->mutex);
  return rc;
}

int flowie_cluster_peer_owner_destroy(flowie_cluster_peer_owner_t *owner) {
  int rc;
  if (!owner) return TURBO_EINVAL;
  rc = tf_coronet_actor_destroy(&owner->actor);
  if (rc != TURBO_OK) return rc;
  flowie_cluster_peer_owner_storage_destroy(owner);
  return TURBO_OK;
}
