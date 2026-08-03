#include "flowie_cluster_node_router_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

typedef struct flowie_cluster_node_router_test_s {
  flowie_cluster_node_router_t *router;
  int receive_count;
  int unregister_result;
} flowie_cluster_node_router_test_t;

typedef struct flowie_cluster_node_router_loopback_test_s {
  int shard_receives;
  int edge_receives;
  int edge_status;
  int completions;
} flowie_cluster_node_router_loopback_test_t;

static void flowie_cluster_node_router_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                                 uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(seed + index);
}

static flowie_cluster_node_router_config_t flowie_cluster_node_router_test_config(void) {
  flowie_cluster_node_router_config_t config = FLOWIE_CLUSTER_NODE_ROUTER_CONFIG_INIT;
  config.shard_count = 4u;
  config.max_links = 1u;
  config.max_inflight_sends = 1u;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.listener_id = tstr_v_from_cstr("mqtt-main");
  config.local_node_id = tstr_v_from_cstr("node-a");
  flowie_cluster_node_router_test_boot(config.local_boot_id, 1u);
  return config;
}

static flowie_cluster_peer_frame_t
flowie_cluster_node_router_test_frame(const flowie_cluster_node_router_config_t *config) {
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
  frame.shard_id = 2u;
  frame.owner_epoch = 9u;
  frame.connection_id = 11u;
  frame.connection_generation = 2u;
  frame.cluster_id = config->cluster_id;
  frame.listener_id = config->listener_id;
  frame.source_node_id = tstr_v_from_cstr("node-b");
  frame.target_node_id = config->local_node_id;
  flowie_cluster_node_router_test_boot(frame.source_boot_id, 33u);
  memcpy(frame.target_boot_id, config->local_boot_id, sizeof(frame.target_boot_id));
  frame.correlation_id[0] = 1u;
  return frame;
}

static int flowie_cluster_node_router_test_receive(void *ctx,
                                                   const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_node_router_test_t *test = (flowie_cluster_node_router_test_t *)ctx;
  if (!frame) return TURBO_EINVAL;
  ++test->receive_count;
  test->unregister_result = flowie_cluster_node_router_unregister_shard(
      test->router, frame->shard_id, flowie_cluster_node_router_test_receive, test);
  return TURBO_OK;
}

static int flowie_cluster_node_router_test_authorize(void *ctx, tstr_v peer_node_id,
                                                     const uint8_t *peer_boot_id,
                                                     const char *certificate_sha256) {
  (void)ctx;
  (void)peer_node_id;
  (void)peer_boot_id;
  (void)certificate_sha256;
  return TURBO_OK;
}

static int flowie_cluster_node_router_test_peer_receive(void *ctx,
                                                        const flowie_cluster_peer_frame_t *frame) {
  (void)ctx;
  return frame ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_cluster_node_router_loopback_shard(
    void *ctx, const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_node_router_loopback_test_t *test =
      (flowie_cluster_node_router_loopback_test_t *)ctx;
  if (!test || !frame) return TURBO_EINVAL;
  ++test->shard_receives;
  return TURBO_OK;
}

static int flowie_cluster_node_router_loopback_edge(
    void *ctx, const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_node_router_loopback_test_t *test =
      (flowie_cluster_node_router_loopback_test_t *)ctx;
  if (!test || !frame) return TURBO_EINVAL;
  ++test->edge_receives;
  return test->edge_status;
}

static void flowie_cluster_node_router_loopback_complete(void *ctx, int status) {
  flowie_cluster_node_router_loopback_test_t *test =
      (flowie_cluster_node_router_loopback_test_t *)ctx;
  if (test && status == TURBO_OK) ++test->completions;
}

spec("flowie cluster node router") {
  it("routes exact local identities and drains borrowed shards") {
    flowie_cluster_node_router_config_t config = flowie_cluster_node_router_test_config();
    flowie_cluster_node_router_snapshot_t snapshot = FLOWIE_CLUSTER_NODE_ROUTER_SNAPSHOT_INIT;
    flowie_cluster_node_router_test_t test = {0};
    flowie_cluster_peer_frame_t frame = flowie_cluster_node_router_test_frame(&config);
    check_int_eq(flowie_cluster_node_router_create(&config, &test.router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_register_shard(
                     test.router, 2u, flowie_cluster_node_router_test_receive, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_register_shard(
                     test.router, 2u, flowie_cluster_node_router_test_receive, &test),
                 TURBO_EALREADY);
    check_int_eq(flowie_cluster_node_router_receive(test.router, &frame), TURBO_OK);
    check_int_eq(test.receive_count, 1);
    check_int_eq(test.unregister_result, TURBO_EBUSY);
    check_int_eq(flowie_cluster_node_router_receive(test.router, &frame), TURBO_ENOENT);
    check_int_eq(flowie_cluster_node_router_unregister_shard(
                     test.router, 2u, flowie_cluster_node_router_test_receive, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_snapshot(test.router, &snapshot), TURBO_OK);
    check_size_eq(snapshot.registered_shards, 0u);
    check_size_eq(snapshot.inflight_routes, 0u);

    check_int_eq(flowie_cluster_node_router_register_shard(
                     test.router, 2u, flowie_cluster_node_router_test_receive, &test),
                 TURBO_OK);
    frame.target_node_id = tstr_v_from_cstr("node-c");
    check_int_eq(flowie_cluster_node_router_receive(test.router, &frame), TURBO_EPROTO);
    frame.target_node_id = config.local_node_id;
    frame.listener_id = tstr_v_from_cstr("mqtt-alt");
    check_int_eq(flowie_cluster_node_router_receive(test.router, &frame), TURBO_EPROTO);
    frame.listener_id = config.listener_id;
    frame.shard_id = config.shard_count;
    check_int_eq(flowie_cluster_node_router_receive(test.router, &frame), TURBO_EPROTO);
    frame.shard_id = 2u;
    check_int_eq(flowie_cluster_node_router_close(test.router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_receive(test.router, &frame), TURBO_ESHUTDOWN);
    check_int_eq(flowie_cluster_node_router_unregister_shard(
                     test.router, 2u, flowie_cluster_node_router_test_receive, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_drain(test.router, 0u), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(test.router), TURBO_OK);
  }

  it("composes exact peer sends without owning links") {
    flowie_cluster_node_router_config_t config = flowie_cluster_node_router_test_config();
    flowie_cluster_node_router_t *router = NULL;
    flowie_cluster_peer_link_t *link = NULL;
    flowie_cluster_peer_link_config_t link_config = FLOWIE_CLUSTER_PEER_LINK_CONFIG_INIT;
    flowie_cluster_peer_frame_t frame;
    uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_node_router_test_boot(remote_boot_id, 33u);
    check_int_eq(flowie_cluster_node_router_create(&config, &router), TURBO_OK);
    link_config.role = FLOWIE_CLUSTER_PEER_ROLE_INITIATOR;
    link_config.max_payload_size = 256u;
    link_config.queue_entries = 1u;
    link_config.queue_bytes = 2048u;
    link_config.cluster_id = config.cluster_id;
    link_config.local_node_id = config.local_node_id;
    link_config.remote_node_id = tstr_v_from_cstr("node-b");
    memcpy(link_config.local_boot_id, config.local_boot_id, sizeof(link_config.local_boot_id));
    memcpy(link_config.remote_boot_id, remote_boot_id, sizeof(link_config.remote_boot_id));
    link_config.authorize = flowie_cluster_node_router_test_authorize;
    link_config.receive = flowie_cluster_node_router_test_peer_receive;
    check_int_eq(flowie_cluster_peer_link_create(&link_config, &link), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_register_link(router, link_config.remote_node_id,
                                                          remote_boot_id, link),
                 TURBO_OK);
    frame = flowie_cluster_node_router_test_frame(&config);
    frame.source_node_id = config.local_node_id;
    memcpy(frame.source_boot_id, config.local_boot_id, sizeof(frame.source_boot_id));
    frame.target_node_id = link_config.remote_node_id;
    memcpy(frame.target_boot_id, remote_boot_id, sizeof(frame.target_boot_id));
    check_int_eq(flowie_cluster_node_router_send(router, &frame, NULL, NULL), TURBO_EBUSY);
    check_int_eq(flowie_cluster_node_router_unregister_link(router, link_config.remote_node_id,
                                                            remote_boot_id, link),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_close(router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_drain(router, 0u), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router), TURBO_OK);
    check_int_eq(flowie_cluster_peer_link_destroy(link), TURBO_OK);
  }

  it("loops local sends through edge and shard routes without a self peer link") {
    flowie_cluster_node_router_config_t config = flowie_cluster_node_router_test_config();
    flowie_cluster_node_router_snapshot_t snapshot = FLOWIE_CLUSTER_NODE_ROUTER_SNAPSHOT_INIT;
    flowie_cluster_node_router_loopback_test_t test = {0};
    flowie_cluster_node_router_t *router = NULL;
    flowie_cluster_peer_frame_t frame = flowie_cluster_node_router_test_frame(&config);
    frame.source_node_id = config.local_node_id;
    memcpy(frame.source_boot_id, config.local_boot_id, sizeof(frame.source_boot_id));
    check_int_eq(flowie_cluster_node_router_create(&config, &router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_register_shard(
                     router, frame.shard_id, flowie_cluster_node_router_loopback_shard, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_register_edge(
                     router, flowie_cluster_node_router_loopback_edge, &test),
                 TURBO_OK);

    check_int_eq(flowie_cluster_node_router_send(
                     router, &frame, flowie_cluster_node_router_loopback_complete, &test),
                 TURBO_OK);
    check_int_eq(test.shard_receives, 1);
    check_int_eq(test.edge_receives, 0);
    check_int_eq(test.completions, 1);

    frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE;
    check_int_eq(flowie_cluster_node_router_send(router, &frame, NULL, NULL), TURBO_OK);
    check_int_eq(test.edge_receives, 1);
    check_int_eq(test.shard_receives, 1);

    frame.kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
    frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
    test.edge_status = TURBO_ENOENT;
    check_int_eq(flowie_cluster_node_router_send(router, &frame, NULL, NULL), TURBO_OK);
    check_int_eq(test.edge_receives, 2);
    check_int_eq(test.shard_receives, 2);
    test.edge_status = TURBO_OK;
    check_int_eq(flowie_cluster_node_router_send(router, &frame, NULL, NULL), TURBO_OK);
    check_int_eq(test.edge_receives, 3);
    check_int_eq(test.shard_receives, 2);

    check_int_eq(flowie_cluster_node_router_snapshot(router, &snapshot), TURBO_OK);
    check_size_eq(snapshot.registered_edges, 1u);
    check_size_eq(snapshot.registered_shards, 1u);
    check_int_eq(flowie_cluster_node_router_unregister_edge(
                     router, flowie_cluster_node_router_loopback_edge, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_unregister_shard(
                     router, frame.shard_id, flowie_cluster_node_router_loopback_shard, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_close(router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_drain(router, 0u), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router), TURBO_OK);
  }

  it("rejects unbounded configuration and premature destroy") {
    flowie_cluster_node_router_config_t config = FLOWIE_CLUSTER_NODE_ROUTER_CONFIG_INIT;
    flowie_cluster_node_router_t *router = NULL;
    check_int_eq(flowie_cluster_node_router_create(&config, &router), TURBO_EINVAL);
    check_null(router);
    config = flowie_cluster_node_router_test_config();
    check_int_eq(flowie_cluster_node_router_create(&config, &router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router), TURBO_EBUSY);
    check_int_eq(flowie_cluster_node_router_close(router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_drain(router, 0u), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router), TURBO_OK);
  }
}
