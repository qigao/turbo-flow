#include "flowie_cluster_node_router_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef enum flowie_cluster_node_shard_state_e {
  FLOWIE_CLUSTER_NODE_SHARD_EMPTY = 0,
  FLOWIE_CLUSTER_NODE_SHARD_ACTIVE,
  FLOWIE_CLUSTER_NODE_SHARD_DRAINING
} flowie_cluster_node_shard_state_t;

typedef struct flowie_cluster_node_shard_s {
  flowie_cluster_node_shard_receive_fn receive;
  void *receive_ctx;
  size_t inflight_routes;
  flowie_cluster_node_shard_state_t state;
} flowie_cluster_node_shard_t;

typedef struct flowie_cluster_node_edge_s {
  flowie_cluster_node_edge_receive_fn receive;
  void *receive_ctx;
  size_t inflight_routes;
  flowie_cluster_node_shard_state_t state;
} flowie_cluster_node_edge_t;

typedef struct flowie_cluster_node_route_lease_s {
  flowie_cluster_node_shard_receive_fn receive;
  void *receive_ctx;
  flowie_cluster_node_shard_t *shard;
  int edge;
} flowie_cluster_node_route_lease_t;

struct flowie_cluster_node_router_s {
  flowie_cluster_node_shard_t *shards;
  uint32_t shard_count;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_peer_registry_t *peers;
  flowie_cluster_node_edge_t edge;
  size_t registered_shards;
  size_t inflight_routes;
  int closing;
  int drained;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
};

static int flowie_cluster_node_router_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_node_router_text_validate(tstr_v value, size_t maximum) {
  if (!value.data || value.len == 0u || value.len > maximum || memchr(value.data, '\0', value.len))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_node_router_view_equal(tstr_v left, tstr_t right) {
  return left.data && right && left.len == tstr_len(right) &&
         memcmp(left.data, right, left.len) == 0;
}

static int
flowie_cluster_node_router_config_validate(const flowie_cluster_node_router_config_t *config) {
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_NODE_ROUTER_ABI_V1 || config->shard_count == 0u ||
      config->max_links == 0u || config->max_inflight_sends == 0u ||
      config->shard_count > SIZE_MAX / sizeof(flowie_cluster_node_shard_t) ||
      !flowie_cluster_node_router_nonzero(config->local_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EINVAL;
  rc = flowie_cluster_node_router_text_validate(config->cluster_id, FLOWIE_CLUSTER_ID_MAX);
  if (rc == TURBO_OK)
    rc = flowie_cluster_node_router_text_validate(config->listener_id,
                                                  FLOWIE_CLUSTER_LISTENER_ID_MAX);
  if (rc == TURBO_OK)
    rc =
        flowie_cluster_node_router_text_validate(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX);
  return rc;
}

int flowie_cluster_node_router_create(const flowie_cluster_node_router_config_t *config,
                                      flowie_cluster_node_router_t **out) {
  flowie_cluster_peer_registry_config_t peers = FLOWIE_CLUSTER_PEER_REGISTRY_CONFIG_INIT;
  flowie_cluster_node_router_t *router;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_node_router_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  router = (flowie_cluster_node_router_t *)calloc(1u, sizeof(*router));
  if (!router) return TURBO_ENOMEM;
  router->shards =
      (flowie_cluster_node_shard_t *)calloc(config->shard_count, sizeof(*router->shards));
  router->cluster_id = tstr_from_v(config->cluster_id);
  router->listener_id = tstr_from_v(config->listener_id);
  router->local_node_id = tstr_from_v(config->local_node_id);
  if (!router->shards || !router->cluster_id || !router->listener_id || !router->local_node_id) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  router->shard_count = config->shard_count;
  memcpy(router->local_boot_id, config->local_boot_id, sizeof(router->local_boot_id));
  turbo_mutex_init(&router->mutex);
  turbo_cond_init(&router->changed);
  peers.max_links = config->max_links;
  peers.max_inflight_sends = config->max_inflight_sends;
  rc = flowie_cluster_peer_registry_create(&peers, &router->peers);
  if (rc != TURBO_OK) goto fail_sync;
  *out = router;
  return TURBO_OK;

fail_sync:
  turbo_cond_destroy(&router->changed);
  turbo_mutex_destroy(&router->mutex);
fail:
  tstr_free(router->cluster_id);
  tstr_free(router->listener_id);
  tstr_free(router->local_node_id);
  free(router->shards);
  free(router);
  return rc;
}

int flowie_cluster_node_router_register_shard(flowie_cluster_node_router_t *router,
                                              uint32_t shard_id,
                                              flowie_cluster_node_shard_receive_fn receive,
                                              void *receive_ctx) {
  flowie_cluster_node_shard_t *shard;
  int rc;
  if (!router || shard_id >= router->shard_count || !receive) return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  shard = &router->shards[shard_id];
  if (router->closing) rc = TURBO_ESHUTDOWN;
  else if (shard->state != FLOWIE_CLUSTER_NODE_SHARD_EMPTY)
    rc = shard->receive == receive && shard->receive_ctx == receive_ctx ? TURBO_EALREADY
                                                                        : TURBO_EBUSY;
  else {
    shard->receive = receive;
    shard->receive_ctx = receive_ctx;
    shard->state = FLOWIE_CLUSTER_NODE_SHARD_ACTIVE;
    ++router->registered_shards;
    router->drained = 0;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&router->mutex);
  return rc;
}

int flowie_cluster_node_router_unregister_shard(flowie_cluster_node_router_t *router,
                                                uint32_t shard_id,
                                                flowie_cluster_node_shard_receive_fn receive,
                                                void *receive_ctx) {
  flowie_cluster_node_shard_t *shard;
  int rc;
  if (!router || shard_id >= router->shard_count || !receive) return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  shard = &router->shards[shard_id];
  if (shard->state == FLOWIE_CLUSTER_NODE_SHARD_EMPTY) rc = TURBO_ENOENT;
  else if (shard->receive != receive || shard->receive_ctx != receive_ctx) rc = TURBO_EBUSY;
  else if (shard->inflight_routes != 0u) {
    shard->state = FLOWIE_CLUSTER_NODE_SHARD_DRAINING;
    rc = TURBO_EBUSY;
  } else {
    memset(shard, 0, sizeof(*shard));
    --router->registered_shards;
    turbo_cond_broadcast(&router->changed);
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&router->mutex);
  return rc;
}

static int flowie_cluster_node_router_runtime_receive(void *ctx,
                                                      const flowie_cluster_peer_frame_t *frame) {
  return flowie_cluster_shard_runtime_receive((flowie_cluster_shard_runtime_t *)ctx, frame);
}

int flowie_cluster_node_router_register_runtime(flowie_cluster_node_router_t *router,
                                                uint32_t shard_id,
                                                flowie_cluster_shard_runtime_t *runtime) {
  if (!runtime) return TURBO_EINVAL;
  return flowie_cluster_node_router_register_shard(
      router, shard_id, flowie_cluster_node_router_runtime_receive, runtime);
}

int flowie_cluster_node_router_unregister_runtime(flowie_cluster_node_router_t *router,
                                                  uint32_t shard_id,
                                                  flowie_cluster_shard_runtime_t *runtime) {
  if (!runtime) return TURBO_EINVAL;
  return flowie_cluster_node_router_unregister_shard(
      router, shard_id, flowie_cluster_node_router_runtime_receive, runtime);
}

int flowie_cluster_node_router_register_edge(flowie_cluster_node_router_t *router,
                                             flowie_cluster_node_edge_receive_fn receive,
                                             void *receive_ctx) {
  int rc;
  if (!router || !receive) return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  if (router->closing) rc = TURBO_ESHUTDOWN;
  else if (router->edge.state != FLOWIE_CLUSTER_NODE_SHARD_EMPTY)
    rc = router->edge.receive == receive && router->edge.receive_ctx == receive_ctx
             ? TURBO_EALREADY
             : TURBO_EBUSY;
  else {
    router->edge.receive = receive;
    router->edge.receive_ctx = receive_ctx;
    router->edge.state = FLOWIE_CLUSTER_NODE_SHARD_ACTIVE;
    router->drained = 0;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&router->mutex);
  return rc;
}

int flowie_cluster_node_router_unregister_edge(flowie_cluster_node_router_t *router,
                                               flowie_cluster_node_edge_receive_fn receive,
                                               void *receive_ctx) {
  int rc;
  if (!router || !receive) return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  if (router->edge.state == FLOWIE_CLUSTER_NODE_SHARD_EMPTY) rc = TURBO_ENOENT;
  else if (router->edge.receive != receive || router->edge.receive_ctx != receive_ctx)
    rc = TURBO_EBUSY;
  else if (router->edge.inflight_routes != 0u) {
    router->edge.state = FLOWIE_CLUSTER_NODE_SHARD_DRAINING;
    rc = TURBO_EBUSY;
  } else {
    memset(&router->edge, 0, sizeof(router->edge));
    turbo_cond_broadcast(&router->changed);
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&router->mutex);
  return rc;
}

int flowie_cluster_node_router_register_link(
    flowie_cluster_node_router_t *router, tstr_v remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_peer_link_t *link) {
  return !router ? TURBO_EINVAL
                 : flowie_cluster_peer_registry_register(router->peers, remote_node_id,
                                                         remote_boot_id, link);
}

int flowie_cluster_node_router_unregister_link(
    flowie_cluster_node_router_t *router, tstr_v remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_peer_link_t *link) {
  return !router ? TURBO_EINVAL
                 : flowie_cluster_peer_registry_unregister(router->peers, remote_node_id,
                                                           remote_boot_id, link);
}

static int flowie_cluster_node_router_edge_first(const flowie_cluster_peer_frame_t *frame) {
  if (frame->kind == FLOWIE_CLUSTER_PEER_FRAME_COMMAND)
    return frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE ||
           frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION;
  return frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY &&
         frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
}

static int flowie_cluster_node_router_route_acquire(
    flowie_cluster_node_router_t *router, const flowie_cluster_peer_frame_t *frame, int edge,
    flowie_cluster_node_route_lease_t *lease) {
  int rc;
  memset(lease, 0, sizeof(*lease));
  turbo_mutex_lock(&router->mutex);
  if (router->closing) rc = TURBO_ESHUTDOWN;
  else if (edge && router->edge.state != FLOWIE_CLUSTER_NODE_SHARD_ACTIVE) rc = TURBO_ENOENT;
  else if (!edge && router->shards[frame->shard_id].state != FLOWIE_CLUSTER_NODE_SHARD_ACTIVE)
    rc = TURBO_ENOENT;
  else {
    if (edge) {
      ++router->edge.inflight_routes;
      lease->receive = router->edge.receive;
      lease->receive_ctx = router->edge.receive_ctx;
      lease->edge = 1;
    } else {
      lease->shard = &router->shards[frame->shard_id];
      ++lease->shard->inflight_routes;
      lease->receive = lease->shard->receive;
      lease->receive_ctx = lease->shard->receive_ctx;
    }
    ++router->inflight_routes;
    router->drained = 0;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&router->mutex);
  return rc;
}

static void flowie_cluster_node_router_route_release(
    flowie_cluster_node_router_t *router, const flowie_cluster_node_route_lease_t *lease) {
  turbo_mutex_lock(&router->mutex);
  if (lease->edge) --router->edge.inflight_routes;
  else --lease->shard->inflight_routes;
  --router->inflight_routes;
  if (router->closing && router->inflight_routes == 0u) router->drained = 1;
  turbo_cond_broadcast(&router->changed);
  turbo_mutex_unlock(&router->mutex);
}

int flowie_cluster_node_router_receive(void *ctx, const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_node_router_t *router = (flowie_cluster_node_router_t *)ctx;
  flowie_cluster_node_route_lease_t lease;
  int edge_first;
  int rc;
  if (!router || !frame) return TURBO_EINVAL;
  if ((frame->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND &&
       frame->kind != FLOWIE_CLUSTER_PEER_FRAME_REPLY) ||
      frame->shard_id >= router->shard_count ||
      !flowie_cluster_node_router_view_equal(frame->cluster_id, router->cluster_id) ||
      !flowie_cluster_node_router_view_equal(frame->listener_id, router->listener_id) ||
      !flowie_cluster_node_router_view_equal(frame->target_node_id, router->local_node_id) ||
      memcmp(frame->target_boot_id, router->local_boot_id, sizeof(router->local_boot_id)) != 0)
    return TURBO_EPROTO;
  edge_first = flowie_cluster_node_router_edge_first(frame);
  rc = flowie_cluster_node_router_route_acquire(router, frame, edge_first, &lease);
  if (rc == TURBO_ENOENT && edge_first && frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY)
    rc = flowie_cluster_node_router_route_acquire(router, frame, 0, &lease);
  if (rc != TURBO_OK) return rc;
  rc = lease.receive(lease.receive_ctx, frame);
  flowie_cluster_node_router_route_release(router, &lease);
  if (rc == TURBO_ENOENT && lease.edge && frame->kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY) {
    rc = flowie_cluster_node_router_route_acquire(router, frame, 0, &lease);
    if (rc != TURBO_OK) return rc;
    rc = lease.receive(lease.receive_ctx, frame);
    flowie_cluster_node_router_route_release(router, &lease);
  }
  return rc;
}

int flowie_cluster_node_router_send(void *ctx, const flowie_cluster_peer_frame_t *frame,
                                    flowie_cluster_peer_send_complete_fn complete,
                                    void *complete_ctx) {
  flowie_cluster_node_router_t *router = (flowie_cluster_node_router_t *)ctx;
  int rc;
  if (!router || !frame) return TURBO_EINVAL;
  if (flowie_cluster_node_router_view_equal(frame->target_node_id, router->local_node_id) &&
      memcmp(frame->target_boot_id, router->local_boot_id, sizeof(router->local_boot_id)) == 0) {
    rc = flowie_cluster_node_router_receive(router, frame);
    if (rc == TURBO_OK && complete) complete(complete_ctx, TURBO_OK);
    return rc;
  }
  return flowie_cluster_peer_registry_send(router->peers, frame, complete, complete_ctx);
}

int flowie_cluster_node_router_snapshot(flowie_cluster_node_router_t *router,
                                        flowie_cluster_node_router_snapshot_t *out) {
  uint32_t shard_id;
  int rc;
  if (!router || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_NODE_ROUTER_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  out->registered_shards = router->registered_shards;
  out->registered_edges = router->edge.state == FLOWIE_CLUSTER_NODE_SHARD_EMPTY ? 0u : 1u;
  out->draining_shards = 0u;
  for (shard_id = 0u; shard_id < router->shard_count; ++shard_id)
    if (router->shards[shard_id].state == FLOWIE_CLUSTER_NODE_SHARD_DRAINING)
      ++out->draining_shards;
  out->inflight_routes = router->inflight_routes;
  out->closing = router->closing;
  turbo_mutex_unlock(&router->mutex);
  rc = flowie_cluster_peer_registry_snapshot(router->peers, &out->peers);
  return rc;
}

int flowie_cluster_node_router_close(flowie_cluster_node_router_t *router) {
  int peer_rc;
  int rc = TURBO_OK;
  if (!router) return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  if (router->closing) rc = TURBO_EALREADY;
  else {
    router->closing = 1;
    router->drained = router->inflight_routes == 0u;
    turbo_cond_broadcast(&router->changed);
  }
  turbo_mutex_unlock(&router->mutex);
  peer_rc = flowie_cluster_peer_registry_close(router->peers);
  if (peer_rc != TURBO_OK && peer_rc != TURBO_EALREADY) return peer_rc;
  return rc;
}

int flowie_cluster_node_router_drain(flowie_cluster_node_router_t *router, uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc;
  if (!router) return TURBO_EINVAL;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  rc = flowie_cluster_peer_registry_drain(router->peers, timeout_ns);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&router->mutex);
  if (!router->closing) rc = TURBO_EBUSY;
  else rc = TURBO_OK;
  while (rc == TURBO_OK && router->inflight_routes != 0u) {
    uint64_t now_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&router->changed, &router->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) {
      rc = timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&router->changed, &router->mutex, deadline_ns - now_ns);
  }
  if (rc == TURBO_OK) router->drained = 1;
  turbo_mutex_unlock(&router->mutex);
  return rc;
}

int flowie_cluster_node_router_destroy(flowie_cluster_node_router_t *router) {
  int ready;
  int rc;
  if (!router) return TURBO_EINVAL;
  turbo_mutex_lock(&router->mutex);
  ready = router->closing && router->drained && router->inflight_routes == 0u &&
          router->registered_shards == 0u &&
          router->edge.state == FLOWIE_CLUSTER_NODE_SHARD_EMPTY;
  turbo_mutex_unlock(&router->mutex);
  if (!ready) return TURBO_EBUSY;
  rc = flowie_cluster_peer_registry_destroy(router->peers);
  if (rc != TURBO_OK) return rc;
  turbo_cond_destroy(&router->changed);
  turbo_mutex_destroy(&router->mutex);
  tstr_free(router->cluster_id);
  tstr_free(router->listener_id);
  tstr_free(router->local_node_id);
  free(router->shards);
  free(router);
  return TURBO_OK;
}
