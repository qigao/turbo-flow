#include "flowie_cluster_node_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <stdlib.h>
#include <string.h>

enum { FLOWIE_CLUSTER_NODE_CREATE_CLEANUP_ATTEMPTS = 2u };

typedef enum flowie_cluster_node_lifecycle_state_e {
  FLOWIE_CLUSTER_NODE_LIFECYCLE_CREATED = 0,
  FLOWIE_CLUSTER_NODE_LIFECYCLE_RUNNING,
  FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSING,
  FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED
} flowie_cluster_node_lifecycle_state_t;

typedef struct flowie_cluster_node_shard_s {
  flowie_cluster_node_t *node;
  flowie_cluster_shard_runtime_config_t config;
  flowie_cluster_shard_runtime_t *runtime;
  flowie_cluster_redis_target_t *target;
  int registered;
  int closed;
} flowie_cluster_node_shard_t;

typedef struct flowie_cluster_node_connector_s {
  flowie_cluster_peer_connector_config_t config;
  flowie_cluster_peer_connector_t *connector;
  tstr_t node_id;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t advertised_endpoint;
  int active;
  int closed;
  int drained;
} flowie_cluster_node_connector_t;

typedef struct flowie_cluster_node_deadline_s {
  uint64_t deadline_ns;
  int infinite;
} flowie_cluster_node_deadline_t;

struct flowie_cluster_node_s {
  const flowie_cluster_node_api_t *api;
  flowie_cluster_node_router_t *router;
  flowie_cluster_redis_bus_t *redis;
  flowie_cluster_peer_listener_config_t listener_config;
  flowie_cluster_peer_listener_t *listener;
  flowie_cluster_peer_authority_t *authority;
  flowie_cluster_member_directory_t *member_directory;
  turbo_vec_t authority_views;
  int authority_views_initialized;
  flowie_cluster_membership_runtime_t *membership;
  turbo_vec_t shards;
  turbo_vec_t connectors;
  size_t max_connectors;
  int shards_initialized;
  int connectors_initialized;
  int listener_closed;
  int listener_drained;
  int router_closed;
  int router_drained;
  int membership_closed;
  uint64_t topology_revision;
  flowie_cluster_node_connector_configure_fn connector_configure;
  void *connector_configure_ctx;
  flowie_cluster_node_lifecycle_state_t state;
};

static const flowie_cluster_node_api_t FLOWIE_CLUSTER_NODE_DEFAULT_API = {
    sizeof(flowie_cluster_node_api_t),
    FLOWIE_CLUSTER_NODE_ABI_V1,
    {flowie_cluster_node_router_create, flowie_cluster_node_router_register_runtime,
     flowie_cluster_node_router_unregister_runtime, flowie_cluster_node_router_register_edge,
     flowie_cluster_node_router_unregister_edge, flowie_cluster_node_router_send,
     flowie_cluster_node_router_close, flowie_cluster_node_router_drain,
     flowie_cluster_node_router_destroy},
    {flowie_cluster_redis_bus_create, flowie_cluster_redis_target_create,
     flowie_cluster_redis_bus_publish, flowie_cluster_redis_target_claim,
     flowie_cluster_redis_target_ack, flowie_cluster_redis_target_requeue,
     flowie_cluster_redis_target_destroy, flowie_cluster_redis_bus_destroy,
     flowie_cluster_redis_bus_route_store},
    {flowie_cluster_shard_runtime_create, flowie_cluster_shard_runtime_close,
     flowie_cluster_shard_runtime_destroy},
    {flowie_cluster_peer_listener_create, flowie_cluster_peer_listener_start,
     flowie_cluster_peer_listener_close, flowie_cluster_peer_listener_drain,
     flowie_cluster_peer_listener_destroy},
    {flowie_cluster_peer_connector_create, flowie_cluster_peer_connector_start,
     flowie_cluster_peer_connector_close, flowie_cluster_peer_connector_drain,
     flowie_cluster_peer_connector_destroy},
    {flowie_cluster_membership_runtime_create, flowie_cluster_membership_runtime_start,
     flowie_cluster_membership_runtime_close, flowie_cluster_membership_runtime_destroy}};

static int flowie_cluster_node_api_validate(const flowie_cluster_node_api_t *api) {
  return api && api->size == sizeof(*api) && api->abi_version == FLOWIE_CLUSTER_NODE_ABI_V1 &&
                 api->router.create && api->router.register_runtime &&
                 api->router.unregister_runtime && api->router.register_edge &&
                 api->router.unregister_edge && api->router.send && api->router.close &&
                 api->router.drain && api->router.destroy && api->redis.bus_create &&
                 api->redis.target_create && api->redis.publish && api->redis.target_claim &&
                 api->redis.target_ack && api->redis.target_requeue && api->redis.target_destroy &&
                 api->redis.bus_destroy && api->redis.route_store && api->shard.create &&
                 api->shard.close &&
                 api->shard.destroy && api->listener.create && api->listener.start &&
                 api->listener.close && api->listener.drain && api->listener.destroy &&
                 api->connector.create && api->connector.start && api->connector.close &&
                 api->connector.drain && api->connector.destroy && api->membership.create &&
                 api->membership.start && api->membership.close && api->membership.destroy
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flowie_cluster_node_membership_current(void *ctx,
                                                  flowie_cluster_topology_peer_t *storage,
                                                  size_t capacity, size_t *out_count,
                                                  uint64_t *out_revision) {
  return flowie_cluster_node_topology_snapshot((const flowie_cluster_node_t *)ctx, storage,
                                               capacity, out_count, out_revision);
}

static int flowie_cluster_node_membership_apply(void *ctx,
                                                const flowie_cluster_topology_plan_t *plan,
                                                uint64_t timeout_ns) {
  return flowie_cluster_node_apply_topology((flowie_cluster_node_t *)ctx, plan, timeout_ns);
}

static int flowie_cluster_node_route_maintenance(void *ctx, size_t *out_changed) {
  flowie_cluster_node_t *node = (flowie_cluster_node_t *)ctx;
  flowie_cluster_route_store_t *route_store;
  if (out_changed) *out_changed = 0u;
  if (!node || !node->redis || !node->member_directory || !out_changed) return TURBO_EINVAL;
  route_store = node->api->redis.route_store(node->redis);
  if (!route_store) return TURBO_EPROTO;
  return flowie_cluster_route_reconcile(route_store, flowie_cluster_member_directory_resolve,
                                        node->member_directory, out_changed);
}

static int flowie_cluster_node_view_equal(tstr_v left, tstr_v right) {
  return left.data && right.data && left.len == right.len &&
         memcmp(left.data, right.data, left.len) == 0;
}

static int flowie_cluster_node_view_equal_cstr(tstr_v left, const char *right) {
  return right && flowie_cluster_node_view_equal(left, tstr_v_from_cstr(right));
}

static int flowie_cluster_node_view_compare(tstr_v left, tstr_v right) {
  size_t common = left.len < right.len ? left.len : right.len;
  int order = common == 0u ? 0 : memcmp(left.data, right.data, common);
  if (order != 0) return order;
  return left.len < right.len ? -1 : left.len > right.len ? 1 : 0;
}

static int flowie_cluster_node_text_valid(tstr_v value, size_t maximum) {
  return value.data && value.len != 0u && value.len <= maximum &&
         memchr(value.data, '\0', value.len) == NULL;
}

static int flowie_cluster_node_bytes_nonzero(const uint8_t *value, size_t size) {
  uint8_t combined = 0u;
  size_t index;
  if (!value) return 0;
  for (index = 0u; index < size; ++index)
    combined |= value[index];
  return combined != 0u;
}

static int flowie_cluster_node_peer_equal(const flowie_cluster_node_connector_t *connector,
                                          const flowie_cluster_topology_peer_t *peer) {
  return connector && peer &&
         flowie_cluster_node_view_equal(tstr_to_v(connector->node_id), peer->node_id) &&
         memcmp(connector->boot_id, peer->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0 &&
         flowie_cluster_node_view_equal(tstr_to_v(connector->advertised_endpoint),
                                        peer->advertised_endpoint);
}

static int flowie_cluster_node_config_validate(const flowie_cluster_node_config_t *config) {
  flowie_cluster_membership_runtime_config_t membership;
  size_t index;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_NODE_ABI_V1 ||
      config->router.size != sizeof(config->router) ||
      config->router.abi_version != FLOWIE_CLUSTER_NODE_ROUTER_ABI_V1 ||
      config->redis.size != sizeof(config->redis) ||
      config->redis.abi_version != FLOWIE_CLUSTER_REDIS_BUS_ABI_V1 ||
      config->listener.size != sizeof(config->listener) ||
      config->listener.abi_version != FLOWIE_CLUSTER_PEER_LISTENER_ABI_V1 ||
      config->router.shard_count == 0u || config->router.max_links == 0u ||
      config->redis.shard_count != config->router.shard_count ||
      config->local_shard_count > config->router.shard_count ||
      config->connector_count > config->router.max_links ||
      (config->local_shard_count != 0u && !config->local_shards) ||
      (config->connector_count != 0u &&
       (!config->connectors || !config->connector_peers || config->topology_revision == 0u)) ||
      (config->connector_configure_ctx && !config->connector_configure) ||
      (config->peer_authority &&
       (config->peer_authority->max_peers != config->router.max_links ||
        config->listener.authorize || config->listener.authorize_ctx)) ||
      config->listener.router ||
      !flowie_cluster_node_view_equal_cstr(config->router.cluster_id, config->redis.cluster_id) ||
      !flowie_cluster_node_view_equal_cstr(config->router.listener_id, config->redis.listener_id) ||
      !flowie_cluster_node_view_equal(config->router.cluster_id, config->listener.cluster_id) ||
      !flowie_cluster_node_view_equal(config->router.local_node_id,
                                      config->listener.local_node_id) ||
      memcmp(config->router.local_boot_id, config->listener.local_boot_id,
             FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
    return TURBO_EINVAL;

  if (config->redis.route_enabled && !config->membership) return TURBO_EINVAL;

  if (config->membership) {
    membership = *config->membership;
    if (membership.current || membership.apply || membership.topology_ctx ||
        membership.member_directory || membership.maintenance || membership.maintenance_ctx ||
        membership.topology.max_nodes > config->router.max_links + 1u)
      return TURBO_EINVAL;
    membership.current = flowie_cluster_node_membership_current;
    membership.apply = flowie_cluster_node_membership_apply;
    membership.topology_ctx = (void *)config;
    if (flowie_cluster_membership_runtime_config_validate(&membership) != TURBO_OK)
      return TURBO_EINVAL;
  }

  for (index = 0u; index < config->local_shard_count; ++index) {
    const flowie_cluster_shard_runtime_config_t *shard = &config->local_shards[index];
    const flowie_cluster_pgsql_config_t *coordinator;
    if (shard->size != sizeof(*shard) ||
        shard->abi_version != FLOWIE_CLUSTER_SHARD_RUNTIME_ABI_V1 || !shard->fact_worker ||
        !shard->fact_worker->fact || !shard->fact_worker->fact->coordinator || shard->reply ||
        shard->reply_ctx || shard->broadcast_publish || shard->broadcast_publish_ctx ||
        shard->broadcast_target_claim || shard->broadcast_target_ack ||
        shard->broadcast_target_requeue || shard->broadcast_target_transport_ctx ||
        shard->takeover_send || shard->takeover_send_ctx || shard->delivery_send ||
        shard->delivery_send_ctx || shard->route_store || shard->route_member_resolve ||
        shard->route_member_resolve_ctx || shard->shard_id >= config->router.shard_count ||
        (config->redis.route_enabled &&
         (shard->route_poll_interval_ns == 0u || shard->route_retry_interval_ns == 0u)) ||
        (!config->redis.route_enabled &&
         (shard->route_poll_interval_ns != 0u || shard->route_retry_interval_ns != 0u)))
      return TURBO_EINVAL;
    coordinator = shard->fact_worker->fact->coordinator;
    if (coordinator->shard_count != config->router.shard_count ||
        !flowie_cluster_node_view_equal_cstr(config->router.cluster_id, coordinator->cluster_id) ||
        !flowie_cluster_node_view_equal_cstr(config->router.listener_id,
                                             coordinator->listener_id) ||
        !flowie_cluster_node_view_equal_cstr(config->router.local_node_id, coordinator->node_id) ||
        memcmp(config->router.local_boot_id, coordinator->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) !=
            0)
      return TURBO_EINVAL;
  }

  for (index = 0u; index < config->connector_count; ++index) {
    const flowie_cluster_peer_connector_config_t *connector = &config->connectors[index];
    const flowie_cluster_topology_peer_t *peer = &config->connector_peers[index];
    if (connector->size != sizeof(*connector) ||
        connector->abi_version != FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1 || connector->router ||
        !connector->remote_node_id.data || connector->remote_node_id.len == 0u ||
        peer->size != sizeof(*peer) || peer->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 ||
        !flowie_cluster_node_text_valid(peer->node_id, FLOWIE_CLUSTER_NODE_ID_MAX) ||
        !flowie_cluster_node_text_valid(peer->advertised_endpoint,
                                        FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX) ||
        !flowie_cluster_node_bytes_nonzero(peer->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
        !flowie_cluster_node_view_equal(connector->remote_node_id, peer->node_id) ||
        memcmp(connector->remote_boot_id, peer->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0 ||
        !flowie_cluster_node_view_equal(config->router.cluster_id, connector->cluster_id) ||
        !flowie_cluster_node_view_equal(config->router.local_node_id, connector->local_node_id) ||
        memcmp(config->router.local_boot_id, connector->local_boot_id,
               FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
      return TURBO_EINVAL;
    if (config->peer_authority && (connector->authorize || connector->authorize_ctx))
      return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flowie_cluster_node_shard_compare(const void *left, const void *right) {
  const flowie_cluster_node_shard_t *lhs = (const flowie_cluster_node_shard_t *)left;
  const flowie_cluster_node_shard_t *rhs = (const flowie_cluster_node_shard_t *)right;
  return lhs->config.shard_id < rhs->config.shard_id   ? -1
         : lhs->config.shard_id > rhs->config.shard_id ? 1
                                                       : 0;
}

static int flowie_cluster_node_connector_compare(const void *left, const void *right) {
  const flowie_cluster_node_connector_t *lhs = (const flowie_cluster_node_connector_t *)left;
  const flowie_cluster_node_connector_t *rhs = (const flowie_cluster_node_connector_t *)right;
  tstr_v lhs_id = tstr_to_v(lhs->node_id);
  tstr_v rhs_id = tstr_to_v(rhs->node_id);
  size_t common = lhs_id.len < rhs_id.len ? lhs_id.len : rhs_id.len;
  int order = memcmp(lhs_id.data, rhs_id.data, common);
  if (order != 0) return order;
  return lhs_id.len < rhs_id.len ? -1 : lhs_id.len > rhs_id.len ? 1 : 0;
}

static void
flowie_cluster_node_connector_record_cleanup(flowie_cluster_node_connector_t *connector) {
  if (!connector) return;
  tstr_free(connector->node_id);
  tstr_free(connector->advertised_endpoint);
  connector->node_id = NULL;
  connector->advertised_endpoint = NULL;
}

static int
flowie_cluster_node_connector_record_init(flowie_cluster_node_connector_t *connector,
                                          const flowie_cluster_peer_connector_config_t *config,
                                          const flowie_cluster_topology_peer_t *peer) {
  if (!connector || !config || !peer) return TURBO_EINVAL;
  memset(connector, 0, sizeof(*connector));
  connector->config = *config;
  connector->node_id = tstr_from_v(peer->node_id);
  connector->advertised_endpoint = tstr_from_v(peer->advertised_endpoint);
  if (!connector->node_id || !connector->advertised_endpoint) {
    flowie_cluster_node_connector_record_cleanup(connector);
    return TURBO_ENOMEM;
  }
  memcpy(connector->boot_id, peer->boot_id, sizeof(connector->boot_id));
  connector->config.remote_node_id = tstr_to_v(connector->node_id);
  memcpy(connector->config.remote_boot_id, connector->boot_id,
         sizeof(connector->config.remote_boot_id));
  return TURBO_OK;
}

static void flowie_cluster_node_connector_records_cleanup(flowie_cluster_node_t *node) {
  size_t index;
  if (!node || !node->connectors_initialized) return;
  for (index = 0u; index < turbo_vec_size(&node->connectors); ++index)
    flowie_cluster_node_connector_record_cleanup(
        (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index));
}

static int flowie_cluster_node_prepare_records(flowie_cluster_node_t *node,
                                               const flowie_cluster_node_config_t *config) {
  size_t index;
  int rc = turbo_vec_init(&node->shards, sizeof(flowie_cluster_node_shard_t));
  if (rc != TURBO_OK) return rc;
  node->shards_initialized = 1;
  rc = turbo_vec_init(&node->connectors, sizeof(flowie_cluster_node_connector_t));
  if (rc != TURBO_OK) return rc;
  node->connectors_initialized = 1;
  for (index = 0u; index < config->local_shard_count; ++index) {
    flowie_cluster_node_shard_t shard = {0};
    shard.config = config->local_shards[index];
    rc = turbo_vec_push(&node->shards, &shard);
    if (rc != TURBO_OK) return rc;
  }
  for (index = 0u; index < config->connector_count; ++index) {
    flowie_cluster_node_connector_t connector = {0};
    rc = flowie_cluster_node_connector_record_init(&connector, &config->connectors[index],
                                                   &config->connector_peers[index]);
    if (rc != TURBO_OK) return rc;
    rc = turbo_vec_push(&node->connectors, &connector);
    if (rc != TURBO_OK) {
      flowie_cluster_node_connector_record_cleanup(&connector);
      return rc;
    }
  }
  if (turbo_vec_size(&node->shards) > 1u)
    qsort(turbo_vec_at(&node->shards, 0u), turbo_vec_size(&node->shards),
          sizeof(flowie_cluster_node_shard_t), flowie_cluster_node_shard_compare);
  if (turbo_vec_size(&node->connectors) > 1u)
    qsort(turbo_vec_at(&node->connectors, 0u), turbo_vec_size(&node->connectors),
          sizeof(flowie_cluster_node_connector_t), flowie_cluster_node_connector_compare);
  for (index = 1u; index < turbo_vec_size(&node->shards); ++index) {
    const flowie_cluster_node_shard_t *previous =
        (const flowie_cluster_node_shard_t *)turbo_vec_at_const(&node->shards, index - 1u);
    const flowie_cluster_node_shard_t *current =
        (const flowie_cluster_node_shard_t *)turbo_vec_at_const(&node->shards, index);
    if (previous->config.shard_id == current->config.shard_id) return TURBO_EINVAL;
  }
  for (index = 1u; index < turbo_vec_size(&node->connectors); ++index) {
    const flowie_cluster_node_connector_t *previous =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, index - 1u);
    const flowie_cluster_node_connector_t *current =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, index);
    if (flowie_cluster_node_view_equal(tstr_to_v(previous->node_id), tstr_to_v(current->node_id)))
      return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flowie_cluster_node_publish_authority(flowie_cluster_node_t *node) {
  size_t index;
  int rc;
  if (!node || !node->authority) return TURBO_OK;
  turbo_vec_clear(&node->authority_views);
  for (index = 0u; index < turbo_vec_size(&node->connectors); ++index) {
    const flowie_cluster_node_connector_t *connector =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, index);
    flowie_cluster_topology_peer_t peer = FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT;
    peer.node_id = tstr_to_v(connector->node_id);
    memcpy(peer.boot_id, connector->boot_id, sizeof(peer.boot_id));
    peer.advertised_endpoint = tstr_to_v(connector->advertised_endpoint);
    rc = turbo_vec_push(&node->authority_views, &peer);
    if (rc != TURBO_OK) return rc;
  }
  return flowie_cluster_peer_authority_replace(
      node->authority,
      (const flowie_cluster_topology_peer_t *)turbo_vec_data(&node->authority_views),
      turbo_vec_size(&node->authority_views), node->topology_revision);
}

static int flowie_cluster_node_send(void *ctx, const flowie_cluster_peer_frame_t *frame,
                                    flowie_cluster_peer_send_complete_fn complete,
                                    void *complete_ctx) {
  flowie_cluster_node_t *node = (flowie_cluster_node_t *)ctx;
  return !node || !node->router
             ? TURBO_EINVAL
             : node->api->router.send(node->router, frame, complete, complete_ctx);
}

static int flowie_cluster_node_reply(void *ctx, const flowie_cluster_peer_frame_t *frame) {
  return flowie_cluster_node_send(ctx, frame, NULL, NULL);
}

static int flowie_cluster_node_publish(void *ctx, const void *payload, size_t payload_size) {
  flowie_cluster_node_t *node = (flowie_cluster_node_t *)ctx;
  return !node || !node->redis ? TURBO_EINVAL
                               : node->api->redis.publish(node->redis, payload, payload_size);
}

static int flowie_cluster_node_target_claim(void *ctx,
                                            flowie_cluster_broadcast_target_claim_t *out) {
  flowie_cluster_node_shard_t *shard = (flowie_cluster_node_shard_t *)ctx;
  if (!shard || !shard->node || !shard->target) return TURBO_EINVAL;
  return shard->node->api->redis.target_claim(shard->target, out);
}

static int flowie_cluster_node_target_ack(void *ctx, uint64_t token) {
  flowie_cluster_node_shard_t *shard = (flowie_cluster_node_shard_t *)ctx;
  if (!shard || !shard->node || !shard->target) return TURBO_EINVAL;
  return shard->node->api->redis.target_ack(shard->target, token);
}

static int flowie_cluster_node_target_requeue(void *ctx, uint64_t token) {
  flowie_cluster_node_shard_t *shard = (flowie_cluster_node_shard_t *)ctx;
  if (!shard || !shard->node || !shard->target) return TURBO_EINVAL;
  return shard->node->api->redis.target_requeue(shard->target, token);
}

static int flowie_cluster_node_wire_shard(flowie_cluster_node_t *node,
                                          flowie_cluster_node_shard_t *shard) {
  shard->node = node;
  if (node->member_directory) {
    shard->config.route_store = node->api->redis.route_store(node->redis);
    if (!shard->config.route_store) return TURBO_EPROTO;
    shard->config.route_member_resolve = flowie_cluster_member_directory_resolve;
    shard->config.route_member_resolve_ctx = node->member_directory;
  }
  shard->config.reply = flowie_cluster_node_reply;
  shard->config.reply_ctx = node;
  if (shard->config.broadcast_poll_interval_ns != 0u) {
    shard->config.broadcast_publish = flowie_cluster_node_publish;
    shard->config.broadcast_publish_ctx = node;
  }
  if (shard->config.broadcast_target_poll_interval_ns != 0u) {
    shard->config.broadcast_target_claim = flowie_cluster_node_target_claim;
    shard->config.broadcast_target_ack = flowie_cluster_node_target_ack;
    shard->config.broadcast_target_requeue = flowie_cluster_node_target_requeue;
    shard->config.broadcast_target_transport_ctx = shard;
  }
  if (shard->config.takeover_poll_interval_ns != 0u) {
    shard->config.takeover_send = flowie_cluster_node_send;
    shard->config.takeover_send_ctx = node;
  }
  if (shard->config.delivery_poll_interval_ns != 0u) {
    shard->config.delivery_send = flowie_cluster_node_send;
    shard->config.delivery_send_ctx = node;
  }
  return TURBO_OK;
}

static flowie_cluster_node_deadline_t flowie_cluster_node_deadline(uint64_t timeout_ns) {
  flowie_cluster_node_deadline_t deadline;
  uint64_t now_ns = turbo_hrtime();
  deadline.infinite = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - now_ns;
  deadline.deadline_ns = deadline.infinite ? UINT64_MAX : now_ns + timeout_ns;
  return deadline;
}

static int flowie_cluster_node_remaining(const flowie_cluster_node_deadline_t *deadline,
                                         uint64_t *out) {
  uint64_t now_ns;
  if (deadline->infinite) {
    *out = UINT64_MAX;
    return TURBO_OK;
  }
  now_ns = turbo_hrtime();
  if (now_ns >= deadline->deadline_ns) return TURBO_ETIMEDOUT;
  *out = deadline->deadline_ns - now_ns;
  return TURBO_OK;
}

static void flowie_cluster_node_record_error(int rc, int *result) {
  if (rc != TURBO_OK && *result == TURBO_OK) *result = rc;
}

static int flowie_cluster_node_all_closed(const flowie_cluster_node_t *node) {
  size_t index;
  if (node->membership && !node->membership_closed) return 0;
  if ((node->listener && !node->listener_drained) || (node->router && !node->router_drained))
    return 0;
  for (index = 0u; index < turbo_vec_size(&node->connectors); ++index) {
    const flowie_cluster_node_connector_t *connector =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, index);
    if (connector->connector && !connector->drained) return 0;
  }
  for (index = 0u; index < turbo_vec_size(&node->shards); ++index) {
    const flowie_cluster_node_shard_t *shard =
        (const flowie_cluster_node_shard_t *)turbo_vec_at_const(&node->shards, index);
    if (shard->registered || (shard->runtime && !shard->closed)) return 0;
  }
  return 1;
}

static int flowie_cluster_node_storage_destroy(flowie_cluster_node_t *node) {
  size_t index;
  int rc;
  if (node->membership) {
    node->api->membership.destroy(node->membership);
    node->membership = NULL;
  }
  for (index = turbo_vec_size(&node->connectors); index > 0u; --index) {
    flowie_cluster_node_connector_t *connector =
        (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index - 1u);
    if (!connector->connector) continue;
    rc = node->api->connector.destroy(connector->connector);
    if (rc != TURBO_OK) return rc;
    connector->connector = NULL;
    flowie_cluster_node_connector_record_cleanup(connector);
  }
  if (node->listener) {
    rc = node->api->listener.destroy(node->listener);
    if (rc != TURBO_OK) return rc;
    node->listener = NULL;
  }
  for (index = turbo_vec_size(&node->shards); index > 0u; --index) {
    flowie_cluster_node_shard_t *shard =
        (flowie_cluster_node_shard_t *)turbo_vec_at(&node->shards, index - 1u);
    if (shard->runtime) {
      rc = node->api->shard.destroy(shard->runtime);
      if (rc != TURBO_OK) return rc;
      shard->runtime = NULL;
    }
    if (shard->target) {
      node->api->redis.target_destroy(shard->target);
      shard->target = NULL;
    }
  }
  if (node->router) {
    rc = node->api->router.destroy(node->router);
    if (rc != TURBO_OK) return rc;
    node->router = NULL;
  }
  if (node->redis) {
    rc = node->api->redis.bus_destroy(node->redis);
    if (rc != TURBO_OK) return rc;
    node->redis = NULL;
  }
  flowie_cluster_member_directory_destroy(node->member_directory);
  node->member_directory = NULL;
  flowie_cluster_peer_authority_destroy(node->authority);
  node->authority = NULL;
  if (node->authority_views_initialized) {
    turbo_vec_destroy(&node->authority_views);
    node->authority_views_initialized = 0;
  }
  if (node->connectors_initialized) {
    flowie_cluster_node_connector_records_cleanup(node);
    turbo_vec_destroy(&node->connectors);
  }
  if (node->shards_initialized) turbo_vec_destroy(&node->shards);
  free(node);
  return TURBO_OK;
}

static void flowie_cluster_node_create_cleanup(flowie_cluster_node_t *node) {
  unsigned attempt;
  if (!node) return;
  for (attempt = 0u; attempt < FLOWIE_CLUSTER_NODE_CREATE_CLEANUP_ATTEMPTS &&
                     node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED;
       ++attempt)
    (void)flowie_cluster_node_close(node, UINT64_MAX);
  if (node->state == FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED)
    (void)flowie_cluster_node_storage_destroy(node);
}

int flowie_cluster_node_create_with_api(const flowie_cluster_node_config_t *config,
                                        const flowie_cluster_node_api_t *api,
                                        flowie_cluster_node_t **out) {
  flowie_cluster_node_t *node;
  size_t index;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_node_api_validate(api);
  if (rc == TURBO_OK) rc = flowie_cluster_node_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  node = (flowie_cluster_node_t *)calloc(1u, sizeof(*node));
  if (!node) return TURBO_ENOMEM;
  node->api = api;
  node->state = FLOWIE_CLUSTER_NODE_LIFECYCLE_CREATED;
  node->max_connectors = config->router.max_links;
  node->topology_revision = config->topology_revision;
  node->connector_configure = config->connector_configure;
  node->connector_configure_ctx = config->connector_configure_ctx;
  node->listener_config = config->listener;
  if (config->peer_authority) {
    rc = flowie_cluster_peer_authority_create(config->peer_authority, &node->authority);
    if (rc != TURBO_OK) goto fail_unopened;
    rc = turbo_vec_init(&node->authority_views, sizeof(flowie_cluster_topology_peer_t));
    if (rc != TURBO_OK) goto fail_unopened;
    node->authority_views_initialized = 1;
    rc = turbo_vec_reserve(&node->authority_views, config->router.max_links);
    if (rc != TURBO_OK) goto fail_unopened;
  }
  rc = flowie_cluster_node_prepare_records(node, config);
  if (rc != TURBO_OK) goto fail_unopened;
  rc = flowie_cluster_node_publish_authority(node);
  if (rc != TURBO_OK) goto fail_unopened;
  rc = api->router.create(&config->router, &node->router);
  if (rc != TURBO_OK) goto fail;
  rc = api->redis.bus_create(&config->redis, &node->redis);
  if (rc != TURBO_OK) goto fail;
  if (config->redis.route_enabled) {
    rc = flowie_cluster_member_directory_create(config->membership->topology.max_nodes,
                                                &node->member_directory);
    if (rc != TURBO_OK) goto fail;
  }
  for (index = 0u; index < turbo_vec_size(&node->shards); ++index) {
    flowie_cluster_node_shard_t *shard =
        (flowie_cluster_node_shard_t *)turbo_vec_at(&node->shards, index);
    rc = flowie_cluster_node_wire_shard(node, shard);
    if (rc != TURBO_OK) goto fail;
    if (shard->config.broadcast_target_poll_interval_ns != 0u) {
      rc = api->redis.target_create(node->redis, shard->config.shard_id, &shard->target);
      if (rc != TURBO_OK) goto fail;
    }
    rc = api->shard.create(&shard->config, &shard->runtime);
    if (rc != TURBO_OK) goto fail;
    rc = api->router.register_runtime(node->router, shard->config.shard_id, shard->runtime);
    if (rc != TURBO_OK) goto fail;
    shard->registered = 1;
  }
  node->listener_config.router = node->router;
  if (node->authority) {
    node->listener_config.authorize = flowie_cluster_peer_authority_authorize;
    node->listener_config.authorize_ctx = node->authority;
  }
  rc = api->listener.create(&node->listener_config, &node->listener);
  if (rc != TURBO_OK) goto fail;
  for (index = 0u; index < turbo_vec_size(&node->connectors); ++index) {
    flowie_cluster_node_connector_t *connector =
        (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index);
    connector->config.router = node->router;
    if (node->authority) {
      connector->config.authorize = flowie_cluster_peer_authority_authorize;
      connector->config.authorize_ctx = node->authority;
    }
    rc = api->connector.create(&connector->config, &connector->connector);
    if (rc != TURBO_OK) goto fail;
  }
  if (config->membership) {
    flowie_cluster_membership_runtime_config_t membership = *config->membership;
    membership.current = flowie_cluster_node_membership_current;
    membership.apply = flowie_cluster_node_membership_apply;
    membership.topology_ctx = node;
    membership.member_directory = node->member_directory;
    if (node->member_directory) {
      membership.maintenance = flowie_cluster_node_route_maintenance;
      membership.maintenance_ctx = node;
    }
    rc = api->membership.create(&membership, &node->membership);
    if (rc != TURBO_OK) goto fail;
  }
  *out = node;
  return TURBO_OK;

fail:
  flowie_cluster_node_create_cleanup(node);
  return rc;
fail_unopened:
  if (node->connectors_initialized) {
    flowie_cluster_node_connector_records_cleanup(node);
    turbo_vec_destroy(&node->connectors);
  }
  if (node->shards_initialized) turbo_vec_destroy(&node->shards);
  if (node->authority_views_initialized) turbo_vec_destroy(&node->authority_views);
  flowie_cluster_peer_authority_destroy(node->authority);
  free(node);
  return rc;
}

int flowie_cluster_node_create(const flowie_cluster_node_config_t *config,
                               flowie_cluster_node_t **out) {
  return flowie_cluster_node_create_with_api(config, &FLOWIE_CLUSTER_NODE_DEFAULT_API, out);
}

int flowie_cluster_node_start(flowie_cluster_node_t *node, uint64_t timeout_ns) {
  flowie_cluster_node_deadline_t deadline;
  uint64_t remaining_ns;
  size_t index;
  int rc;
  if (!node || timeout_ns == 0u || node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_CREATED ||
      !node->listener)
    return TURBO_EINVAL;
  deadline = flowie_cluster_node_deadline(timeout_ns);
  rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
  if (rc == TURBO_OK) rc = node->api->listener.start(node->listener, remaining_ns);
  if (rc != TURBO_OK) goto fail;
  for (index = 0u; index < turbo_vec_size(&node->connectors); ++index) {
    flowie_cluster_node_connector_t *connector =
        (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index);
    rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->connector.start(connector->connector, remaining_ns);
    if (rc != TURBO_OK) goto fail;
    connector->active = 1;
  }
  node->state = FLOWIE_CLUSTER_NODE_LIFECYCLE_RUNNING;
  if (node->membership) {
    rc = node->api->membership.start(node->membership);
    if (rc != TURBO_OK) goto fail;
  }
  return TURBO_OK;

fail:
  node->state = FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSING;
  (void)flowie_cluster_node_close(node, timeout_ns);
  return rc;
}

int flowie_cluster_node_register_edge(flowie_cluster_node_t *node,
                                      flowie_cluster_node_edge_receive_fn receive,
                                      void *receive_ctx) {
  if (!node || !node->router || !receive ||
      node->state == FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSING ||
      node->state == FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED)
    return TURBO_EINVAL;
  return node->api->router.register_edge(node->router, receive, receive_ctx);
}

int flowie_cluster_node_unregister_edge(flowie_cluster_node_t *node,
                                        flowie_cluster_node_edge_receive_fn receive,
                                        void *receive_ctx) {
  if (!node || !node->router || !receive) return TURBO_EINVAL;
  return node->api->router.unregister_edge(node->router, receive, receive_ctx);
}

int flowie_cluster_node_submit(void *ctx, const flowie_cluster_peer_frame_t *frame) {
  return flowie_cluster_node_send(ctx, frame, NULL, NULL);
}

static size_t flowie_cluster_node_connector_lower_bound(const flowie_cluster_node_t *node,
                                                        tstr_v node_id, int *found) {
  size_t first = 0u;
  size_t count = turbo_vec_size(&node->connectors);
  while (count != 0u) {
    size_t step = count / 2u;
    size_t index = first + step;
    const flowie_cluster_node_connector_t *connector =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, index);
    if (flowie_cluster_node_view_compare(tstr_to_v(connector->node_id), node_id) < 0) {
      first = index + 1u;
      count -= step + 1u;
    } else {
      count = step;
    }
  }
  if (found) {
    const flowie_cluster_node_connector_t *connector =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, first);
    *found = connector && flowie_cluster_node_view_equal(tstr_to_v(connector->node_id), node_id);
  }
  return first;
}

int flowie_cluster_node_topology_snapshot(const flowie_cluster_node_t *node,
                                          flowie_cluster_topology_peer_t *storage, size_t capacity,
                                          size_t *out_count, uint64_t *out_revision) {
  size_t count;
  size_t index;
  if (out_count) *out_count = 0u;
  if (out_revision) *out_revision = 0u;
  if (!node || !out_count || !out_revision) return TURBO_EINVAL;
  if (node && node->authority)
    return flowie_cluster_peer_authority_snapshot(node->authority, storage, capacity, out_count,
                                                  out_revision);
  count = turbo_vec_size(&node->connectors);
  if (count != 0u && !storage) return TURBO_EINVAL;
  if (capacity < count) return TURBO_ENOSPC;
  for (index = 0u; index < count; ++index) {
    const flowie_cluster_node_connector_t *connector =
        (const flowie_cluster_node_connector_t *)turbo_vec_at_const(&node->connectors, index);
    flowie_cluster_topology_peer_t peer = FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT;
    peer.node_id = tstr_to_v(connector->node_id);
    memcpy(peer.boot_id, connector->boot_id, sizeof(peer.boot_id));
    peer.advertised_endpoint = tstr_to_v(connector->advertised_endpoint);
    storage[index] = peer;
  }
  *out_count = count;
  *out_revision = node->topology_revision;
  return TURBO_OK;
}

static int flowie_cluster_node_resolved_connector_validate(
    const flowie_cluster_node_t *node, const flowie_cluster_topology_peer_t *peer,
    const flowie_cluster_peer_connector_config_t *config) {
  if (!node || !peer || !config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1 || config->router ||
      !flowie_cluster_node_view_equal(config->cluster_id, node->listener_config.cluster_id) ||
      !flowie_cluster_node_view_equal(config->local_node_id, node->listener_config.local_node_id) ||
      memcmp(config->local_boot_id, node->listener_config.local_boot_id,
             FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0 ||
      !flowie_cluster_node_view_equal(config->remote_node_id, peer->node_id) ||
      memcmp(config->remote_boot_id, peer->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_cluster_node_topology_remove(flowie_cluster_node_t *node,
                                               const flowie_cluster_topology_peer_t *peer,
                                               flowie_cluster_node_deadline_t *deadline) {
  flowie_cluster_node_connector_t removed = {0};
  flowie_cluster_node_connector_t *connector;
  uint64_t remaining_ns;
  size_t index;
  int found;
  int rc;
  index = flowie_cluster_node_connector_lower_bound(node, peer->node_id, &found);
  if (!found) return TURBO_OK;
  connector = (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index);
  if (!flowie_cluster_node_peer_equal(connector, peer)) return TURBO_EPROTO;
  if (!connector->closed) {
    rc = flowie_cluster_node_remaining(deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->connector.close(connector->connector, remaining_ns);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    connector->closed = 1;
    connector->active = 0;
  }
  if (!connector->drained) {
    rc = flowie_cluster_node_remaining(deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->connector.drain(connector->connector, remaining_ns);
    if (rc != TURBO_OK) return rc;
    connector->drained = 1;
  }
  rc = node->api->connector.destroy(connector->connector);
  if (rc != TURBO_OK) return rc;
  connector->connector = NULL;
  rc = turbo_vec_erase(&node->connectors, index, &removed);
  if (rc != TURBO_OK) return rc;
  flowie_cluster_node_connector_record_cleanup(&removed);
  return TURBO_OK;
}

static int flowie_cluster_node_topology_start_connector(flowie_cluster_node_t *node,
                                                        flowie_cluster_node_connector_t *connector,
                                                        flowie_cluster_node_deadline_t *deadline) {
  uint64_t remaining_ns;
  int rc;
  if (node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_RUNNING || connector->active) return TURBO_OK;
  rc = flowie_cluster_node_remaining(deadline, &remaining_ns);
  if (rc == TURBO_OK) rc = node->api->connector.start(connector->connector, remaining_ns);
  if (rc == TURBO_OK) connector->active = 1;
  return rc;
}

static int flowie_cluster_node_topology_add(flowie_cluster_node_t *node,
                                            const flowie_cluster_topology_peer_t *peer,
                                            flowie_cluster_node_deadline_t *deadline) {
  flowie_cluster_peer_connector_config_t config = FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT;
  flowie_cluster_node_connector_t created = {0};
  flowie_cluster_node_connector_t *stored;
  size_t count;
  size_t index;
  int found;
  int rc;
  index = flowie_cluster_node_connector_lower_bound(node, peer->node_id, &found);
  if (found) {
    stored = (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index);
    if (!flowie_cluster_node_peer_equal(stored, peer)) return TURBO_EPROTO;
    return flowie_cluster_node_topology_start_connector(node, stored, deadline);
  }
  if (!node->connector_configure) return TURBO_ENOTSUP;
  rc = node->connector_configure(node->connector_configure_ctx, peer, &config);
  if (rc != TURBO_OK) return rc;
  if (node->authority) {
    if (config.authorize || config.authorize_ctx) return TURBO_EPROTO;
    config.authorize = flowie_cluster_peer_authority_authorize;
    config.authorize_ctx = node->authority;
  }
  rc = flowie_cluster_node_resolved_connector_validate(node, peer, &config);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_node_connector_record_init(&created, &config, peer);
  if (rc != TURBO_OK) return rc;
  created.config.router = node->router;
  count = turbo_vec_size(&node->connectors);
  if (count >= node->max_connectors) {
    flowie_cluster_node_connector_record_cleanup(&created);
    return TURBO_ENOSPC;
  }
  if (count == SIZE_MAX) {
    flowie_cluster_node_connector_record_cleanup(&created);
    return TURBO_ERANGE;
  }
  rc = turbo_vec_reserve(&node->connectors, count + 1u);
  if (rc != TURBO_OK) {
    flowie_cluster_node_connector_record_cleanup(&created);
    return rc;
  }
  rc = turbo_vec_insert(&node->connectors, index, &created);
  if (rc != TURBO_OK) {
    flowie_cluster_node_connector_record_cleanup(&created);
    return rc;
  }
  stored = (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index);
  rc = node->api->connector.create(&stored->config, &stored->connector);
  if (rc != TURBO_OK) {
    flowie_cluster_node_connector_t removed = {0};
    int erase_rc = turbo_vec_erase(&node->connectors, index, &removed);
    if (erase_rc == TURBO_OK) flowie_cluster_node_connector_record_cleanup(&removed);
    return rc;
  }
  return flowie_cluster_node_topology_start_connector(node, stored, deadline);
}

int flowie_cluster_node_apply_topology(flowie_cluster_node_t *node,
                                       const flowie_cluster_topology_plan_t *plan,
                                       uint64_t timeout_ns) {
  flowie_cluster_node_deadline_t deadline;
  flowie_cluster_topology_operation_t operation = FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT;
  uint64_t revision;
  size_t count;
  size_t index;
  int additions_seen = 0;
  int rc;
  if (!node || !plan || timeout_ns == 0u ||
      (node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_CREATED &&
       node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_RUNNING))
    return TURBO_EINVAL;
  revision = flowie_cluster_topology_plan_revision(plan);
  count = flowie_cluster_topology_plan_operation_count(plan);
  if (revision < node->topology_revision) return TURBO_EBUSY;
  if (revision == node->topology_revision) return count == 0u ? TURBO_OK : TURBO_EPROTO;
  for (index = 0u; index < count; ++index) {
    operation = (flowie_cluster_topology_operation_t)FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT;
    rc = flowie_cluster_topology_plan_operation_at(plan, index, &operation);
    if (rc != TURBO_OK) return rc;
    if (operation.kind == FLOWIE_CLUSTER_TOPOLOGY_ADD) additions_seen = 1;
    else if (operation.kind != FLOWIE_CLUSTER_TOPOLOGY_REMOVE || additions_seen)
      return TURBO_EPROTO;
  }
  deadline = flowie_cluster_node_deadline(timeout_ns);
  for (index = 0u; index < count; ++index) {
    operation = (flowie_cluster_topology_operation_t)FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT;
    rc = flowie_cluster_topology_plan_operation_at(plan, index, &operation);
    if (rc != TURBO_OK) return rc;
    rc = operation.kind == FLOWIE_CLUSTER_TOPOLOGY_REMOVE
             ? flowie_cluster_node_topology_remove(node, &operation.peer, &deadline)
             : flowie_cluster_node_topology_add(node, &operation.peer, &deadline);
    if (rc != TURBO_OK) return rc;
    rc = flowie_cluster_node_publish_authority(node);
    if (rc != TURBO_OK) return rc;
  }
  node->topology_revision = revision;
  return flowie_cluster_node_publish_authority(node);
}

int flowie_cluster_node_close(flowie_cluster_node_t *node, uint64_t timeout_ns) {
  flowie_cluster_node_deadline_t deadline;
  uint64_t remaining_ns;
  size_t index;
  int peers_drained = 1;
  int result = TURBO_OK;
  int rc;
  if (!node || timeout_ns == 0u) return TURBO_EINVAL;
  if (node->state == FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED) return TURBO_OK;
  node->state = FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSING;
  deadline = flowie_cluster_node_deadline(timeout_ns);
  if (node->membership && !node->membership_closed) {
    rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->membership.close(node->membership, remaining_ns);
    if (rc != TURBO_OK && rc != TURBO_EALREADY) return rc;
    node->membership_closed = 1;
  }

  for (index = turbo_vec_size(&node->connectors); index > 0u; --index) {
    flowie_cluster_node_connector_t *connector =
        (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index - 1u);
    if (!connector->connector || connector->closed) continue;
    rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->connector.close(connector->connector, remaining_ns);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) connector->closed = 1;
    else flowie_cluster_node_record_error(rc, &result);
  }
  if (node->listener && !node->listener_closed) {
    rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->listener.close(node->listener, remaining_ns);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) node->listener_closed = 1;
    else flowie_cluster_node_record_error(rc, &result);
  }
  for (index = turbo_vec_size(&node->connectors); index > 0u; --index) {
    flowie_cluster_node_connector_t *connector =
        (flowie_cluster_node_connector_t *)turbo_vec_at(&node->connectors, index - 1u);
    if (!connector->connector || connector->drained) continue;
    if (!connector->closed) {
      peers_drained = 0;
      continue;
    }
    rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->connector.drain(connector->connector, remaining_ns);
    if (rc == TURBO_OK) connector->drained = 1;
    else {
      peers_drained = 0;
      flowie_cluster_node_record_error(rc, &result);
    }
  }
  if (node->listener && !node->listener_drained) {
    if (!node->listener_closed) peers_drained = 0;
    else {
      rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
      if (rc == TURBO_OK) rc = node->api->listener.drain(node->listener, remaining_ns);
      if (rc == TURBO_OK) node->listener_drained = 1;
      else {
        peers_drained = 0;
        flowie_cluster_node_record_error(rc, &result);
      }
    }
  }
  if (!peers_drained) return result == TURBO_OK ? TURBO_EBUSY : result;

  if (node->router && !node->router_closed) {
    rc = node->api->router.close(node->router);
    if (rc == TURBO_OK || rc == TURBO_EALREADY) node->router_closed = 1;
    else flowie_cluster_node_record_error(rc, &result);
  }
  if (node->router && node->router_closed && !node->router_drained) {
    rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
    if (rc == TURBO_OK) rc = node->api->router.drain(node->router, remaining_ns);
    if (rc == TURBO_OK) node->router_drained = 1;
    else flowie_cluster_node_record_error(rc, &result);
  }
  if (node->router && !node->router_drained) return result == TURBO_OK ? TURBO_EBUSY : result;

  for (index = 0u; index < turbo_vec_size(&node->shards); ++index) {
    flowie_cluster_node_shard_t *shard =
        (flowie_cluster_node_shard_t *)turbo_vec_at(&node->shards, index);
    if (shard->registered) {
      rc = node->api->router.unregister_runtime(node->router, shard->config.shard_id,
                                                shard->runtime);
      if (rc == TURBO_OK || rc == TURBO_ENOENT) shard->registered = 0;
      else flowie_cluster_node_record_error(rc, &result);
    }
    if (!shard->registered && shard->runtime && !shard->closed) {
      rc = flowie_cluster_node_remaining(&deadline, &remaining_ns);
      if (rc == TURBO_OK) rc = node->api->shard.close(shard->runtime, remaining_ns);
      if (rc == TURBO_OK || rc == TURBO_EALREADY) shard->closed = 1;
      else flowie_cluster_node_record_error(rc, &result);
    }
  }
  if (flowie_cluster_node_all_closed(node)) node->state = FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED;
  return result == TURBO_OK && node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED ? TURBO_EBUSY
                                                                                   : result;
}

int flowie_cluster_node_destroy(flowie_cluster_node_t *node) {
  int rc;
  if (!node) return TURBO_OK;
  rc = flowie_cluster_node_close(node, UINT64_MAX);
  if (rc != TURBO_OK || node->state != FLOWIE_CLUSTER_NODE_LIFECYCLE_CLOSED) return rc;
  return flowie_cluster_node_storage_destroy(node);
}
