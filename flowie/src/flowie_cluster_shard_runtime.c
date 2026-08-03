#include "flowie_cluster_shard_runtime_internal.h"

#include "flow_coronet_execution.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct flowie_cluster_shard_runtime_s {
  flowie_cluster_pgsql_lease_worker_t *lease_worker;
  flowie_cluster_pgsql_fact_store_t *fact_store;
  flowie_cluster_pgsql_fact_worker_t *fact_worker;
  flowie_cluster_session_bind_t *session_bind;
  flowie_cluster_peer_owner_t *peer_owner;
  flowie_cluster_broadcast_pgsql_source_t *broadcast_source;
  flowie_cluster_broadcast_dispatcher_t *broadcast_dispatcher;
  flowie_cluster_broadcast_target_owner_t *broadcast_target_owner;
  flowie_cluster_broadcast_target_dispatcher_t *broadcast_target_dispatcher;
  flowie_cluster_takeover_pgsql_source_t *takeover_source;
  flowie_cluster_takeover_dispatcher_t *takeover_dispatcher;
  flowie_cluster_delivery_pgsql_source_t *delivery_source;
  flowie_cluster_delivery_dispatcher_t *delivery_dispatcher;
  flowie_cluster_lifecycle_pgsql_source_t *lifecycle_source;
  flowie_cluster_lifecycle_owner_t *lifecycle_owner;
  flowie_cluster_lifecycle_dispatcher_t *lifecycle_dispatcher;
  flowie_cluster_route_projector_t *route_projector;
  flowie_cluster_route_pgsql_source_t *route_source;
  flowie_cluster_route_dispatcher_t *route_dispatcher;
  flowie_cluster_peer_owner_reply_fn reply;
  void *reply_ctx;
  flowie_cluster_session_self_fence_fn self_fence;
  void *self_fence_ctx;
  atomic_int fenced;
  int owner_closed;
  int lease_closed;
  int fact_closed;
  int broadcast_closed;
  int broadcast_target_closed;
  int broadcast_target_owner_closed;
  int takeover_closed;
  int delivery_closed;
  int lifecycle_closed;
  int lifecycle_owner_closed;
  int route_closed;
  int closed;
};

static int flowie_cluster_shard_runtime_validation_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  (void)ctx;
  (void)command;
  (void)completion;
  (void)completion_ctx;
  return TURBO_EINVAL;
}

static void flowie_cluster_shard_runtime_validation_fence(void *ctx, int reason) {
  (void)ctx;
  (void)reason;
}

static int
flowie_cluster_shard_runtime_session_config(const flowie_cluster_shard_runtime_config_t *config,
                                            flowie_cluster_session_bind_config_t *out) {
  if (!config || !config->session_bind || !out) return TURBO_EINVAL;
  if (config->session_bind->submit || config->session_bind->submit_ctx ||
      config->session_bind->self_fence || config->session_bind->self_fence_ctx)
    return TURBO_EINVAL;
  *out = *config->session_bind;
  out->submit = flowie_cluster_shard_runtime_validation_submit;
  out->self_fence = flowie_cluster_shard_runtime_validation_fence;
  return flowie_cluster_session_bind_config_validate(out);
}

int flowie_cluster_shard_runtime_config_validate(
    const flowie_cluster_shard_runtime_config_t *config) {
  flowie_cluster_session_bind_config_t session;
  const flowie_cluster_pgsql_fact_config_t *fact;
  const flowie_cluster_pgsql_config_t *coordinator;
  size_t identity_overhead;
  size_t maximum_frame_size;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_SHARD_RUNTIME_ABI_V1 || !config->execution ||
      !config->execution->context || !config->fact_worker || !config->session_bind ||
      !config->reply || config->owner_max_payload_size == 0u ||
      config->owner_max_payload_size > UINT32_MAX || config->owner_queue_entries == 0u ||
      config->owner_queue_bytes == 0u)
    return TURBO_EINVAL;
  if ((!config->takeover_send &&
       (config->takeover_send_ctx || config->takeover_poll_interval_ns != 0u ||
        config->takeover_retry_interval_ns != 0u || config->takeover_reply_timeout_ns != 0u)) ||
      (config->takeover_send &&
       (config->takeover_poll_interval_ns == 0u || config->takeover_retry_interval_ns == 0u ||
        config->takeover_reply_timeout_ns == 0u)))
    return TURBO_EINVAL;
  if ((!config->delivery_send &&
       (config->delivery_send_ctx || config->delivery_poll_interval_ns != 0u ||
        config->delivery_retry_interval_ns != 0u || config->delivery_reply_timeout_ns != 0u)) ||
      (config->delivery_send &&
       (config->delivery_poll_interval_ns == 0u || config->delivery_retry_interval_ns == 0u ||
        config->delivery_reply_timeout_ns == 0u)))
    return TURBO_EINVAL;
  if ((!config->broadcast_publish &&
       (config->broadcast_max_payload_size != 0u || config->broadcast_publish_ctx ||
        config->broadcast_poll_interval_ns != 0u ||
        config->broadcast_ack_poll_interval_ns != 0u ||
        config->broadcast_retry_interval_ns != 0u ||
        config->broadcast_republish_interval_ns != 0u)) ||
      (config->broadcast_publish &&
       (config->broadcast_max_payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
        config->broadcast_max_payload_size > UINT32_MAX ||
        config->broadcast_poll_interval_ns == 0u ||
        config->broadcast_ack_poll_interval_ns == 0u ||
        config->broadcast_retry_interval_ns == 0u ||
        config->broadcast_republish_interval_ns == 0u)))
    return TURBO_EINVAL;
  if ((!config->broadcast_target_claim &&
       (config->broadcast_target_max_payload_size != 0u || config->broadcast_target_ack ||
        config->broadcast_target_requeue || config->broadcast_target_transport_ctx ||
        config->broadcast_target_poll_interval_ns != 0u ||
        config->broadcast_target_retry_interval_ns != 0u ||
        config->broadcast_target_ack_retry_interval_ns != 0u)) ||
      (config->broadcast_target_claim &&
       (!config->broadcast_target_ack || !config->broadcast_target_requeue ||
        config->broadcast_target_max_payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
        config->broadcast_target_max_payload_size > UINT32_MAX ||
        config->broadcast_target_poll_interval_ns == 0u ||
        config->broadcast_target_retry_interval_ns == 0u ||
        config->broadcast_target_ack_retry_interval_ns == 0u)))
    return TURBO_EINVAL;
  if ((config->lifecycle_apply_ctx && !config->lifecycle_apply) ||
      (config->route_before_settle_ctx && !config->route_before_settle) ||
      ((config->route_store || config->route_member_resolve || config->route_member_resolve_ctx) &&
       (!config->route_store || !config->route_member_resolve)) ||
      ((config->route_poll_interval_ns != 0u || config->route_retry_interval_ns != 0u) &&
       (!config->route_store || !config->route_member_resolve ||
        config->route_poll_interval_ns == 0u || config->route_retry_interval_ns == 0u)) ||
      ((config->lifecycle_apply || config->lifecycle_poll_interval_ns != 0u ||
        config->lifecycle_retry_interval_ns != 0u) &&
       (config->lifecycle_poll_interval_ns == 0u ||
        config->lifecycle_retry_interval_ns == 0u)))
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_worker_config_validate(config->fact_worker);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_shard_runtime_session_config(config, &session);
  if (rc != TURBO_OK) return rc;
  fact = config->fact_worker->fact;
  coordinator = fact->coordinator;
  if (config->shard_id >= coordinator->shard_count ||
      fact->max_fact_records < session.max_sessions ||
      fact->max_value_size < session.max_fact_value_size ||
      fact->max_event_payload_size < session.max_event_payload_size ||
      config->owner_max_payload_size < session.max_bind_payload_size)
    return TURBO_EINVAL;
  if (config->broadcast_publish &&
      (session.max_event_payload_size >
           SIZE_MAX - FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
       config->broadcast_max_payload_size <
           session.max_event_payload_size + FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE))
    return TURBO_EINVAL;
  if (config->delivery_send &&
      config->owner_max_payload_size < session.max_event_payload_size)
    return TURBO_EINVAL;
  identity_overhead =
      FLOWIE_CLUSTER_ID_MAX + FLOWIE_CLUSTER_LISTENER_ID_MAX + FLOWIE_CLUSTER_NODE_ID_MAX * 2u;
  if (config->owner_max_payload_size >
      SIZE_MAX - FLOWIE_CLUSTER_PEER_HEADER_SIZE - identity_overhead)
    return TURBO_ERANGE;
  maximum_frame_size =
      FLOWIE_CLUSTER_PEER_HEADER_SIZE + identity_overhead + config->owner_max_payload_size;
  if (maximum_frame_size > UINT32_MAX || config->owner_queue_bytes < maximum_frame_size)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static void flowie_cluster_shard_runtime_self_fence(void *ctx, int reason) {
  flowie_cluster_shard_runtime_t *runtime = (flowie_cluster_shard_runtime_t *)ctx;
  if (!runtime) return;
  atomic_store_explicit(&runtime->fenced, 1, memory_order_release);
  if (runtime->self_fence) runtime->self_fence(runtime->self_fence_ctx, reason);
}

static int flowie_cluster_shard_runtime_resolve(void *ctx, uint32_t shard_id,
                                                flowie_cluster_owner_token_t *out) {
  flowie_cluster_shard_runtime_t *runtime = (flowie_cluster_shard_runtime_t *)ctx;
  flowie_cluster_pgsql_lease_snapshot_t lease = FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
  int rc;
  if (!runtime || !out) return TURBO_EINVAL;
  if (atomic_load_explicit(&runtime->fenced, memory_order_acquire)) return TURBO_EBUSY;
  rc = flowie_cluster_pgsql_lease_worker_snapshot(runtime->lease_worker, &lease);
  if (rc != TURBO_OK) return rc;
  if (lease.state != FLOWIE_CLUSTER_SHARD_ACTIVE || lease.owner.shard_id != shard_id)
    return TURBO_EBUSY;
  *out = lease.owner;
  return TURBO_OK;
}

static int flowie_cluster_shard_runtime_reply(void *ctx, const flowie_cluster_peer_frame_t *reply) {
  flowie_cluster_shard_runtime_t *runtime = (flowie_cluster_shard_runtime_t *)ctx;
  if (!runtime || !runtime->reply) return TURBO_EINVAL;
  return runtime->reply(runtime->reply_ctx, reply);
}

static int
flowie_cluster_shard_runtime_execute_async(void *ctx, const flowie_cluster_peer_frame_t *command,
                                           flowie_cluster_peer_owner_complete_fn complete,
                                           void *completion_ctx) {
  flowie_cluster_shard_runtime_t *runtime = (flowie_cluster_shard_runtime_t *)ctx;
  if (!runtime) return TURBO_EINVAL;
  return flowie_cluster_session_bind_execute_async(runtime->session_bind, command, complete,
                                                   completion_ctx);
}

static void flowie_cluster_shard_runtime_storage_destroy(flowie_cluster_shard_runtime_t *runtime) {
  if (!runtime) return;
  if (runtime->broadcast_target_dispatcher) {
    (void)flowie_cluster_broadcast_target_dispatcher_close(
        runtime->broadcast_target_dispatcher);
    (void)flowie_cluster_broadcast_target_dispatcher_drain(
        runtime->broadcast_target_dispatcher, UINT64_MAX);
    (void)flowie_cluster_broadcast_target_dispatcher_destroy(
        runtime->broadcast_target_dispatcher);
  }
  if (runtime->broadcast_target_owner) {
    (void)flowie_cluster_broadcast_target_owner_close(runtime->broadcast_target_owner);
    (void)flowie_cluster_broadcast_target_owner_drain(runtime->broadcast_target_owner,
                                                      UINT64_MAX);
    (void)flowie_cluster_broadcast_target_owner_destroy(runtime->broadcast_target_owner);
  }
  if (runtime->broadcast_dispatcher) {
    (void)flowie_cluster_broadcast_dispatcher_close(runtime->broadcast_dispatcher);
    (void)flowie_cluster_broadcast_dispatcher_drain(runtime->broadcast_dispatcher, UINT64_MAX);
    (void)flowie_cluster_broadcast_dispatcher_destroy(runtime->broadcast_dispatcher);
  }
  flowie_cluster_broadcast_pgsql_source_destroy(runtime->broadcast_source);
  if (runtime->lifecycle_dispatcher) {
    (void)flowie_cluster_lifecycle_dispatcher_close(runtime->lifecycle_dispatcher);
    (void)flowie_cluster_lifecycle_dispatcher_drain(runtime->lifecycle_dispatcher, UINT64_MAX);
    (void)flowie_cluster_lifecycle_dispatcher_destroy(runtime->lifecycle_dispatcher);
  }
  if (runtime->route_dispatcher) {
    (void)flowie_cluster_route_dispatcher_close(runtime->route_dispatcher);
    (void)flowie_cluster_route_dispatcher_drain(runtime->route_dispatcher, UINT64_MAX);
    (void)flowie_cluster_route_dispatcher_destroy(runtime->route_dispatcher);
  }
  flowie_cluster_route_pgsql_source_destroy(runtime->route_source);
  if (runtime->lifecycle_owner) {
    (void)flowie_cluster_lifecycle_owner_close(runtime->lifecycle_owner);
    (void)flowie_cluster_lifecycle_owner_drain(runtime->lifecycle_owner, UINT64_MAX);
    (void)flowie_cluster_lifecycle_owner_destroy(runtime->lifecycle_owner);
  }
  flowie_cluster_lifecycle_pgsql_source_destroy(runtime->lifecycle_source);
  if (runtime->takeover_dispatcher) {
    (void)flowie_cluster_takeover_dispatcher_close(runtime->takeover_dispatcher);
    (void)flowie_cluster_takeover_dispatcher_drain(runtime->takeover_dispatcher, UINT64_MAX);
    (void)flowie_cluster_takeover_dispatcher_destroy(runtime->takeover_dispatcher);
  }
  flowie_cluster_takeover_pgsql_source_destroy(runtime->takeover_source);
  if (runtime->delivery_dispatcher) {
    (void)flowie_cluster_delivery_dispatcher_close(runtime->delivery_dispatcher);
    (void)flowie_cluster_delivery_dispatcher_drain(runtime->delivery_dispatcher, UINT64_MAX);
    (void)flowie_cluster_delivery_dispatcher_destroy(runtime->delivery_dispatcher);
  }
  flowie_cluster_delivery_pgsql_source_destroy(runtime->delivery_source);
  if (runtime->peer_owner) (void)flowie_cluster_peer_owner_destroy(runtime->peer_owner);
  if (runtime->session_bind) (void)flowie_cluster_session_bind_destroy(runtime->session_bind);
  flowie_cluster_route_projector_destroy(runtime->route_projector);
  flowie_cluster_pgsql_fact_worker_destroy(runtime->fact_worker);
  flowie_cluster_pgsql_fact_store_destroy(runtime->fact_store);
  flowie_cluster_pgsql_lease_worker_destroy(runtime->lease_worker);
  free(runtime);
}

int flowie_cluster_shard_runtime_create(const flowie_cluster_shard_runtime_config_t *config,
                                        flowie_cluster_shard_runtime_t **out) {
  flowie_cluster_session_bind_config_t session;
  flowie_cluster_peer_owner_config_t owner = FLOWIE_CLUSTER_PEER_OWNER_CONFIG_INIT;
  flowie_cluster_takeover_dispatcher_config_t takeover =
      FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CONFIG_INIT;
  flowie_cluster_delivery_dispatcher_config_t delivery =
      FLOWIE_CLUSTER_DELIVERY_DISPATCHER_CONFIG_INIT;
  flowie_cluster_lifecycle_dispatcher_config_t lifecycle =
      FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CONFIG_INIT;
  flowie_cluster_lifecycle_owner_config_t lifecycle_owner =
      FLOWIE_CLUSTER_LIFECYCLE_OWNER_CONFIG_INIT;
  flowie_cluster_broadcast_dispatcher_config_t broadcast =
      FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CONFIG_INIT;
  flowie_cluster_broadcast_target_owner_config_t broadcast_target_owner =
      FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_CONFIG_INIT;
  flowie_cluster_broadcast_target_dispatcher_config_t broadcast_target =
      FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CONFIG_INIT;
  const flowie_cluster_pgsql_config_t *coordinator;
  flowie_cluster_shard_runtime_t *runtime;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_shard_runtime_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  coordinator = config->fact_worker->fact->coordinator;
  runtime = (flowie_cluster_shard_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  runtime->reply = config->reply;
  runtime->reply_ctx = config->reply_ctx;
  runtime->self_fence = config->self_fence;
  runtime->self_fence_ctx = config->self_fence_ctx;
  atomic_init(&runtime->fenced, 0);
  rc = flowie_cluster_pgsql_lease_worker_create(coordinator, config->shard_id,
                                                &runtime->lease_worker);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_pgsql_fact_store_open(config->fact_worker->fact, &runtime->fact_store);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_pgsql_fact_worker_create(config->fact_worker, &runtime->fact_worker);
  if (rc != TURBO_OK) goto fail;
  session = *config->session_bind;
  session.submit = flowie_cluster_session_bind_pgsql_submit;
  session.submit_ctx = runtime->fact_worker;
  session.self_fence = flowie_cluster_shard_runtime_self_fence;
  session.self_fence_ctx = runtime;
  rc = flowie_cluster_session_bind_create(&session, &runtime->session_bind);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_session_bind_recover_pgsql(runtime->session_bind, runtime->fact_store,
                                                 runtime->lease_worker);
  if (rc != TURBO_OK) goto fail;
  if (config->route_store) {
    flowie_cluster_route_projector_config_t projection =
        FLOWIE_CLUSTER_ROUTE_PROJECTOR_CONFIG_INIT;
    projection.session = &session;
    projection.route_store = config->route_store;
    projection.fact_get = flowie_cluster_route_projector_pgsql_fact_get;
    projection.fact_get_ctx = runtime->fact_store;
    projection.member_resolve = config->route_member_resolve;
    projection.member_resolve_ctx = config->route_member_resolve_ctx;
    rc = flowie_cluster_route_projector_create(&projection, &runtime->route_projector);
    if (rc != TURBO_OK) goto fail;
    if (config->route_poll_interval_ns != 0u) {
      flowie_cluster_route_dispatcher_config_t route =
          FLOWIE_CLUSTER_ROUTE_DISPATCHER_CONFIG_INIT;
      rc = flowie_cluster_route_pgsql_source_create(config->fact_worker->fact,
                                                     &runtime->route_source);
      if (rc != TURBO_OK) goto fail;
      route.shard_id = config->shard_id;
      route.max_payload_size = session.max_event_payload_size;
      route.poll_interval_ns = config->route_poll_interval_ns;
      route.retry_interval_ns = config->route_retry_interval_ns;
      route.resolve = flowie_cluster_shard_runtime_resolve;
      route.resolve_ctx = runtime;
      route.fetch = flowie_cluster_route_pgsql_source_fetch;
      route.settle = flowie_cluster_route_pgsql_source_settle;
      route.recover = flowie_cluster_route_pgsql_source_recover;
      route.source_ctx = runtime->route_source;
      route.project = flowie_cluster_route_projector_project;
      route.project_ctx = runtime->route_projector;
      rc = flowie_cluster_route_dispatcher_create(&route, &runtime->route_dispatcher);
      if (rc != TURBO_OK) goto fail;
    }
  }
  owner.execution = config->execution;
  owner.max_payload_size = config->owner_max_payload_size;
  owner.queue_entries = config->owner_queue_entries;
  owner.queue_bytes = config->owner_queue_bytes;
  owner.cluster_id = tstr_v_from_cstr(coordinator->cluster_id);
  owner.listener_id = tstr_v_from_cstr(coordinator->listener_id);
  owner.local_node_id = tstr_v_from_cstr(coordinator->node_id);
  memcpy(owner.local_boot_id, coordinator->boot_id, sizeof(owner.local_boot_id));
  owner.resolve = flowie_cluster_shard_runtime_resolve;
  owner.execute_async = flowie_cluster_shard_runtime_execute_async;
  owner.reply = flowie_cluster_shard_runtime_reply;
  owner.user_data = runtime;
  rc = flowie_cluster_peer_owner_create(&owner, &runtime->peer_owner);
  if (rc != TURBO_OK) goto fail;
  if (config->broadcast_target_claim) {
    broadcast_target_owner.shard_id = config->shard_id;
    broadcast_target_owner.max_payload_size = config->broadcast_target_max_payload_size;
    broadcast_target_owner.max_targets = session.max_sessions;
    broadcast_target_owner.execution = config->execution;
    broadcast_target_owner.session_bind = runtime->session_bind;
    broadcast_target_owner.resolve = flowie_cluster_shard_runtime_resolve;
    broadcast_target_owner.resolve_ctx = runtime;
    broadcast_target_owner.submit = flowie_cluster_session_bind_pgsql_submit;
    broadcast_target_owner.submit_ctx = runtime->fact_worker;
    broadcast_target_owner.self_fence = flowie_cluster_shard_runtime_self_fence;
    broadcast_target_owner.self_fence_ctx = runtime;
    rc = flowie_cluster_broadcast_target_owner_create(
        &broadcast_target_owner, &runtime->broadcast_target_owner);
    if (rc != TURBO_OK) goto fail;
    broadcast_target.max_payload_size = config->broadcast_target_max_payload_size;
    broadcast_target.poll_interval_ns = config->broadcast_target_poll_interval_ns;
    broadcast_target.retry_interval_ns = config->broadcast_target_retry_interval_ns;
    broadcast_target.ack_retry_interval_ns = config->broadcast_target_ack_retry_interval_ns;
    broadcast_target.claim = config->broadcast_target_claim;
    broadcast_target.ack = config->broadcast_target_ack;
    broadcast_target.requeue = config->broadcast_target_requeue;
    broadcast_target.transport_ctx = config->broadcast_target_transport_ctx;
    broadcast_target.apply = flowie_cluster_broadcast_target_owner_apply;
    broadcast_target.apply_ctx = runtime->broadcast_target_owner;
    rc = flowie_cluster_broadcast_target_dispatcher_create(
        &broadcast_target, &runtime->broadcast_target_dispatcher);
    if (rc != TURBO_OK) goto fail;
  }
  if (config->broadcast_publish) {
    rc = flowie_cluster_broadcast_pgsql_source_create(config->fact_worker->fact,
                                                      &runtime->broadcast_source);
    if (rc != TURBO_OK) goto fail;
    broadcast.shard_id = config->shard_id;
    broadcast.shard_count = coordinator->shard_count;
    broadcast.max_payload_size = config->broadcast_max_payload_size;
    broadcast.poll_interval_ns = config->broadcast_poll_interval_ns;
    broadcast.ack_poll_interval_ns = config->broadcast_ack_poll_interval_ns;
    broadcast.retry_interval_ns = config->broadcast_retry_interval_ns;
    broadcast.republish_interval_ns = config->broadcast_republish_interval_ns;
    broadcast.resolve = flowie_cluster_shard_runtime_resolve;
    broadcast.resolve_ctx = runtime;
    broadcast.fetch = flowie_cluster_broadcast_pgsql_source_fetch;
    broadcast.settle = flowie_cluster_broadcast_pgsql_source_settle;
    broadcast.ack_count = flowie_cluster_broadcast_pgsql_source_ack_count;
    broadcast.recover = flowie_cluster_broadcast_pgsql_source_recover;
    broadcast.source_ctx = runtime->broadcast_source;
    broadcast.publish = config->broadcast_publish;
    broadcast.publish_ctx = config->broadcast_publish_ctx;
    rc = flowie_cluster_broadcast_dispatcher_create(&broadcast,
                                                    &runtime->broadcast_dispatcher);
    if (rc != TURBO_OK) goto fail;
  }
  if (config->takeover_send) {
    rc = flowie_cluster_takeover_pgsql_source_create(config->fact_worker->fact,
                                                     &runtime->takeover_source);
    if (rc != TURBO_OK) goto fail;
    takeover.shard_id = config->shard_id;
    takeover.max_payload_size = config->owner_max_payload_size;
    takeover.poll_interval_ns = config->takeover_poll_interval_ns;
    takeover.retry_interval_ns = config->takeover_retry_interval_ns;
    takeover.reply_timeout_ns = config->takeover_reply_timeout_ns;
    takeover.cluster_id = tstr_v_from_cstr(coordinator->cluster_id);
    takeover.listener_id = tstr_v_from_cstr(coordinator->listener_id);
    takeover.resolve = flowie_cluster_shard_runtime_resolve;
    takeover.resolve_ctx = runtime;
    takeover.fetch = flowie_cluster_takeover_pgsql_source_fetch;
    takeover.settle = flowie_cluster_takeover_pgsql_source_settle;
    takeover.before_settle = runtime->route_projector
                                 ? flowie_cluster_route_projector_project
                                 : config->route_before_settle;
    takeover.before_settle_ctx = runtime->route_projector ? runtime->route_projector
                                                           : config->route_before_settle_ctx;
    takeover.recover = flowie_cluster_takeover_pgsql_source_recover;
    takeover.source_ctx = runtime->takeover_source;
    takeover.send = config->takeover_send;
    takeover.send_ctx = config->takeover_send_ctx;
    rc = flowie_cluster_takeover_dispatcher_create(&takeover, &runtime->takeover_dispatcher);
    if (rc != TURBO_OK) goto fail;
  }
  if (config->delivery_send) {
    rc = flowie_cluster_delivery_pgsql_source_create(config->fact_worker->fact,
                                                     &runtime->delivery_source);
    if (rc != TURBO_OK) goto fail;
    delivery.shard_id = config->shard_id;
    delivery.max_payload_size = config->owner_max_payload_size;
    delivery.poll_interval_ns = config->delivery_poll_interval_ns;
    delivery.retry_interval_ns = config->delivery_retry_interval_ns;
    delivery.reply_timeout_ns = config->delivery_reply_timeout_ns;
    delivery.cluster_id = tstr_v_from_cstr(coordinator->cluster_id);
    delivery.listener_id = tstr_v_from_cstr(coordinator->listener_id);
    delivery.resolve = flowie_cluster_shard_runtime_resolve;
    delivery.resolve_ctx = runtime;
    delivery.fetch = flowie_cluster_delivery_pgsql_source_fetch;
    delivery.settle = flowie_cluster_delivery_pgsql_source_settle;
    delivery.recover = flowie_cluster_delivery_pgsql_source_recover;
    delivery.source_ctx = runtime->delivery_source;
    delivery.send = config->delivery_send;
    delivery.send_ctx = config->delivery_send_ctx;
    rc = flowie_cluster_delivery_dispatcher_create(&delivery, &runtime->delivery_dispatcher);
    if (rc != TURBO_OK) goto fail;
  }
  if (config->lifecycle_poll_interval_ns != 0u) {
    rc = flowie_cluster_lifecycle_pgsql_source_create(config->fact_worker->fact,
                                                      &runtime->lifecycle_source);
    if (rc != TURBO_OK) goto fail;
    if (!config->lifecycle_apply) {
      lifecycle_owner.shard_id = config->shard_id;
      lifecycle_owner.execution = config->execution;
      lifecycle_owner.session_bind = runtime->session_bind;
      lifecycle_owner.submit = flowie_cluster_session_bind_pgsql_submit;
      lifecycle_owner.submit_ctx = runtime->fact_worker;
      lifecycle_owner.now = session.now;
      lifecycle_owner.now_ctx = session.now_ctx;
      lifecycle_owner.self_fence = flowie_cluster_shard_runtime_self_fence;
      lifecycle_owner.self_fence_ctx = runtime;
      rc = flowie_cluster_lifecycle_owner_create(&lifecycle_owner,
                                                 &runtime->lifecycle_owner);
      if (rc != TURBO_OK) goto fail;
    }
    lifecycle.shard_id = config->shard_id;
    lifecycle.max_payload_size = session.max_event_payload_size;
    lifecycle.poll_interval_ns = config->lifecycle_poll_interval_ns;
    lifecycle.retry_interval_ns = config->lifecycle_retry_interval_ns;
    lifecycle.resolve = flowie_cluster_shard_runtime_resolve;
    lifecycle.resolve_ctx = runtime;
    lifecycle.fetch = flowie_cluster_lifecycle_pgsql_source_fetch;
    lifecycle.settle = flowie_cluster_lifecycle_pgsql_source_settle;
    lifecycle.before_settle = runtime->route_projector
                                  ? flowie_cluster_route_projector_project
                                  : config->route_before_settle;
    lifecycle.before_settle_ctx = runtime->route_projector ? runtime->route_projector
                                                            : config->route_before_settle_ctx;
    lifecycle.recover = flowie_cluster_lifecycle_pgsql_source_recover;
    lifecycle.source_ctx = runtime->lifecycle_source;
    lifecycle.apply = config->lifecycle_apply ? config->lifecycle_apply
                                              : flowie_cluster_lifecycle_owner_apply;
    lifecycle.apply_ctx = config->lifecycle_apply ? config->lifecycle_apply_ctx
                                                  : runtime->lifecycle_owner;
    rc = flowie_cluster_lifecycle_dispatcher_create(&lifecycle, &runtime->lifecycle_dispatcher);
    if (rc != TURBO_OK) goto fail;
  }
  *out = runtime;
  return TURBO_OK;

fail:
  atomic_store_explicit(&runtime->fenced, 1, memory_order_release);
  flowie_cluster_shard_runtime_storage_destroy(runtime);
  return rc;
}

int flowie_cluster_shard_runtime_submit(flowie_cluster_shard_runtime_t *runtime,
                                        const flowie_cluster_peer_frame_t *command) {
  if (!runtime || !command) return TURBO_EINVAL;
  if (atomic_load_explicit(&runtime->fenced, memory_order_acquire)) return TURBO_EBUSY;
  return flowie_cluster_peer_owner_submit(runtime->peer_owner, command);
}

int flowie_cluster_shard_runtime_receive(flowie_cluster_shard_runtime_t *runtime,
                                         const flowie_cluster_peer_frame_t *frame) {
  if (!runtime || !frame) return TURBO_EINVAL;
  if (frame->kind == FLOWIE_CLUSTER_PEER_FRAME_COMMAND)
    return flowie_cluster_shard_runtime_submit(runtime, frame);
  if (frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY &&
      frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK &&
      runtime->delivery_dispatcher)
    return flowie_cluster_delivery_dispatcher_reply(runtime->delivery_dispatcher, frame);
  if (frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY &&
      frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY &&
      runtime->takeover_dispatcher)
    return flowie_cluster_takeover_dispatcher_reply(runtime->takeover_dispatcher, frame);
  return frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY ? TURBO_ENOENT : TURBO_EPROTO;
}

int flowie_cluster_shard_runtime_snapshot(flowie_cluster_shard_runtime_t *runtime,
                                          flowie_cluster_pgsql_lease_snapshot_t *out) {
  int rc;
  if (!runtime || !out) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_lease_worker_snapshot(runtime->lease_worker, out);
  if (rc == TURBO_OK && atomic_load_explicit(&runtime->fenced, memory_order_acquire))
    out->state = FLOWIE_CLUSTER_SHARD_FENCED;
  return rc;
}

int flowie_cluster_shard_runtime_session_snapshot(const flowie_cluster_shard_runtime_t *runtime,
                                                  flowie_mqtt_span_t client_id,
                                                  flowie_session_snapshot_t *snapshot,
                                                  turbo_flow_security_principal_t *principal) {
  if (!runtime) return TURBO_EINVAL;
  return flowie_cluster_session_bind_snapshot(runtime->session_bind, client_id, snapshot,
                                              principal);
}

int flowie_cluster_shard_runtime_lifecycle_snapshot(
    flowie_cluster_shard_runtime_t *runtime, flowie_cluster_lifecycle_dispatcher_snapshot_t *out) {
  if (!runtime || !out) return TURBO_EINVAL;
  if (!runtime->lifecycle_dispatcher) return TURBO_ENOENT;
  return flowie_cluster_lifecycle_dispatcher_snapshot(runtime->lifecycle_dispatcher, out);
}

int flowie_cluster_shard_runtime_broadcast_snapshot(
    flowie_cluster_shard_runtime_t *runtime, flowie_cluster_broadcast_dispatcher_snapshot_t *out) {
  if (!runtime || !out) return TURBO_EINVAL;
  if (!runtime->broadcast_dispatcher) return TURBO_ENOENT;
  return flowie_cluster_broadcast_dispatcher_snapshot(runtime->broadcast_dispatcher, out);
}

int flowie_cluster_shard_runtime_broadcast_target_snapshot(
    flowie_cluster_shard_runtime_t *runtime,
    flowie_cluster_broadcast_target_dispatcher_snapshot_t *out) {
  if (!runtime || !out) return TURBO_EINVAL;
  if (!runtime->broadcast_target_dispatcher) return TURBO_ENOENT;
  return flowie_cluster_broadcast_target_dispatcher_snapshot(
      runtime->broadcast_target_dispatcher, out);
}

int flowie_cluster_shard_runtime_broadcast_target_owner_snapshot(
    flowie_cluster_shard_runtime_t *runtime,
    flowie_cluster_broadcast_target_owner_snapshot_t *out) {
  if (!runtime || !out) return TURBO_EINVAL;
  if (!runtime->broadcast_target_owner) return TURBO_ENOENT;
  return flowie_cluster_broadcast_target_owner_snapshot(runtime->broadcast_target_owner, out);
}

int flowie_cluster_shard_runtime_delivery_snapshot(
    flowie_cluster_shard_runtime_t *runtime,
    flowie_cluster_delivery_dispatcher_snapshot_t *out) {
  if (!runtime || !out) return TURBO_EINVAL;
  if (!runtime->delivery_dispatcher) return TURBO_ENOENT;
  return flowie_cluster_delivery_dispatcher_snapshot(runtime->delivery_dispatcher, out);
}

int flowie_cluster_shard_runtime_close(flowie_cluster_shard_runtime_t *runtime,
                                       uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int result = TURBO_OK;
  int rc;
  if (!runtime) return TURBO_EINVAL;
  if (runtime->closed) return TURBO_OK;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  atomic_store_explicit(&runtime->fenced, 1, memory_order_release);
  if (runtime->broadcast_target_dispatcher && !runtime->broadcast_target_closed) {
    rc = flowie_cluster_broadcast_target_dispatcher_close(
        runtime->broadcast_target_dispatcher);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->broadcast_target_closed = 1;
  }
  if (runtime->broadcast_target_owner && !runtime->broadcast_target_owner_closed) {
    rc = flowie_cluster_broadcast_target_owner_close(runtime->broadcast_target_owner);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->broadcast_target_owner_closed = 1;
  }
  if (runtime->broadcast_dispatcher && !runtime->broadcast_closed) {
    rc = flowie_cluster_broadcast_dispatcher_close(runtime->broadcast_dispatcher);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->broadcast_closed = 1;
  }
  if (runtime->lifecycle_dispatcher && !runtime->lifecycle_closed) {
    rc = flowie_cluster_lifecycle_dispatcher_close(runtime->lifecycle_dispatcher);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->lifecycle_closed = 1;
  }
  if (runtime->route_dispatcher && !runtime->route_closed) {
    rc = flowie_cluster_route_dispatcher_close(runtime->route_dispatcher);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->route_closed = 1;
  }
  if (runtime->lifecycle_owner && !runtime->lifecycle_owner_closed) {
    rc = flowie_cluster_lifecycle_owner_close(runtime->lifecycle_owner);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->lifecycle_owner_closed = 1;
  }
  if (runtime->takeover_dispatcher && !runtime->takeover_closed) {
    rc = flowie_cluster_takeover_dispatcher_close(runtime->takeover_dispatcher);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->takeover_closed = 1;
  }
  if (runtime->delivery_dispatcher && !runtime->delivery_closed) {
    rc = flowie_cluster_delivery_dispatcher_close(runtime->delivery_dispatcher);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->delivery_closed = 1;
  }
  if (!runtime->owner_closed) {
    rc = flowie_cluster_peer_owner_close(runtime->peer_owner);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    runtime->owner_closed = 1;
  }
  if (!runtime->lease_closed) {
    rc = flowie_cluster_pgsql_lease_worker_close(runtime->lease_worker);
    runtime->lease_closed = 1;
    if (rc != TURBO_OK && rc != TURBO_EALREADY) result = rc;
  }
  if (runtime->broadcast_dispatcher) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_broadcast_dispatcher_drain(runtime->broadcast_dispatcher, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->broadcast_target_dispatcher) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_broadcast_target_dispatcher_drain(
        runtime->broadcast_target_dispatcher, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->broadcast_target_owner) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_broadcast_target_owner_drain(runtime->broadcast_target_owner,
                                                      timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->lifecycle_dispatcher) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_lifecycle_dispatcher_drain(runtime->lifecycle_dispatcher, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->lifecycle_owner) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_lifecycle_owner_drain(runtime->lifecycle_owner, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->route_dispatcher) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_route_dispatcher_drain(runtime->route_dispatcher, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->takeover_dispatcher) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_takeover_dispatcher_drain(runtime->takeover_dispatcher, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (runtime->delivery_dispatcher) {
    if (deadline_ns != UINT64_MAX) {
      uint64_t now_ns = turbo_hrtime();
      timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
    }
    rc = flowie_cluster_delivery_dispatcher_drain(runtime->delivery_dispatcher, timeout_ns);
    if (rc != TURBO_OK) return rc;
  }
  if (deadline_ns != UINT64_MAX) {
    uint64_t now_ns = turbo_hrtime();
    timeout_ns = now_ns >= deadline_ns ? 0u : deadline_ns - now_ns;
  }
  rc = flowie_cluster_peer_owner_drain(runtime->peer_owner, timeout_ns);
  if (rc != TURBO_OK) return rc;
  if (!runtime->fact_closed) {
    rc = flowie_cluster_pgsql_fact_worker_close(runtime->fact_worker);
    runtime->fact_closed = 1;
    if (rc != TURBO_OK && rc != TURBO_EALREADY && result == TURBO_OK) result = rc;
  }
  runtime->closed = 1;
  return result;
}

int flowie_cluster_shard_runtime_destroy(flowie_cluster_shard_runtime_t *runtime) {
  int rc;
  if (!runtime) return TURBO_OK;
  rc = flowie_cluster_shard_runtime_close(runtime, UINT64_MAX);
  if (!runtime->closed) return rc;
  flowie_cluster_shard_runtime_storage_destroy(runtime);
  return rc;
}
