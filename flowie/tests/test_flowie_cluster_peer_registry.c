#include "flowie_cluster_peer_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static void flowie_cluster_peer_registry_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                                   uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(seed + index);
}

static int flowie_cluster_peer_registry_test_authorize(void *ctx, tstr_v peer_node_id,
                                                       const uint8_t *peer_boot_id,
                                                       const char *certificate_sha256) {
  (void)ctx;
  (void)peer_node_id;
  (void)peer_boot_id;
  (void)certificate_sha256;
  return TURBO_OK;
}

static int flowie_cluster_peer_registry_test_receive(void *ctx,
                                                     const flowie_cluster_peer_frame_t *frame) {
  (void)ctx;
  return frame ? TURBO_OK : TURBO_EINVAL;
}

static flowie_cluster_peer_link_config_t flowie_cluster_peer_registry_test_link_config(
    const uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], const char *remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  flowie_cluster_peer_link_config_t config = FLOWIE_CLUSTER_PEER_LINK_CONFIG_INIT;
  config.role = FLOWIE_CLUSTER_PEER_ROLE_INITIATOR;
  config.max_payload_size = 256u;
  config.queue_entries = 1u;
  config.queue_bytes = 2048u;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.local_node_id = tstr_v_from_cstr("node-a");
  config.remote_node_id = tstr_v_from_cstr(remote_node_id);
  memcpy(config.local_boot_id, local_boot_id, sizeof(config.local_boot_id));
  memcpy(config.remote_boot_id, remote_boot_id, sizeof(config.remote_boot_id));
  config.authorize = flowie_cluster_peer_registry_test_authorize;
  config.receive = flowie_cluster_peer_registry_test_receive;
  return config;
}

static flowie_cluster_peer_frame_t
flowie_cluster_peer_registry_test_frame(const uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                        const char *remote_node_id,
                                        const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE;
  frame.shard_id = 7u;
  frame.owner_epoch = 9u;
  frame.connection_id = 11u;
  frame.connection_generation = 2u;
  frame.cluster_id = tstr_v_from_cstr("cluster-a");
  frame.listener_id = tstr_v_from_cstr("mqtt-main");
  frame.source_node_id = tstr_v_from_cstr("node-a");
  frame.target_node_id = tstr_v_from_cstr(remote_node_id);
  frame.payload = tstr_v_from_cstr("payload");
  memcpy(frame.source_boot_id, local_boot_id, sizeof(frame.source_boot_id));
  memcpy(frame.target_boot_id, remote_boot_id, sizeof(frame.target_boot_id));
  frame.correlation_id[0] = 1u;
  return frame;
}

spec("flowie cluster peer link registry") {
  it("routes by exact boot identity and rolls back rejected send leases") {
    flowie_cluster_peer_registry_config_t registry_config =
        FLOWIE_CLUSTER_PEER_REGISTRY_CONFIG_INIT;
    flowie_cluster_peer_registry_snapshot_t snapshot = FLOWIE_CLUSTER_PEER_REGISTRY_SNAPSHOT_INIT;
    flowie_cluster_peer_registry_t *registry = NULL;
    flowie_cluster_peer_link_t *first = NULL;
    flowie_cluster_peer_link_t *second = NULL;
    flowie_cluster_peer_link_config_t first_config;
    flowie_cluster_peer_link_config_t second_config;
    flowie_cluster_peer_frame_t frame;
    uint8_t local_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t first_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t second_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_peer_registry_test_boot(local_boot, 1u);
    flowie_cluster_peer_registry_test_boot(first_boot, 33u);
    flowie_cluster_peer_registry_test_boot(second_boot, 65u);
    first_config = flowie_cluster_peer_registry_test_link_config(local_boot, "node-b", first_boot);
    second_config =
        flowie_cluster_peer_registry_test_link_config(local_boot, "node-c", second_boot);
    registry_config.max_links = 1u;
    registry_config.max_inflight_sends = 1u;
    check_int_eq(flowie_cluster_peer_link_create(&first_config, &first), TURBO_OK);
    check_int_eq(flowie_cluster_peer_link_create(&second_config, &second), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_create(&registry_config, &registry), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_register(registry, tstr_v_from_cstr("node-b"),
                                                       first_boot, first),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_register(registry, tstr_v_from_cstr("node-b"),
                                                       first_boot, first),
                 TURBO_EALREADY);
    check_int_eq(flowie_cluster_peer_registry_register(registry, tstr_v_from_cstr("node-b"),
                                                       first_boot, second),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_peer_registry_register(registry, tstr_v_from_cstr("node-c"),
                                                       second_boot, second),
                 TURBO_ENOSPC);
    frame = flowie_cluster_peer_registry_test_frame(local_boot, "node-b", first_boot);
    check_int_eq(flowie_cluster_peer_registry_send(registry, &frame, NULL, NULL), TURBO_EBUSY);
    check_int_eq(flowie_cluster_peer_registry_snapshot(registry, &snapshot), TURBO_OK);
    check_size_eq(snapshot.registered_links, 1u);
    check_size_eq(snapshot.inflight_sends, 0u);
    check_int_eq(flowie_cluster_peer_registry_unregister(registry, tstr_v_from_cstr("node-b"),
                                                         first_boot, second),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_peer_registry_unregister(registry, tstr_v_from_cstr("node-b"),
                                                         first_boot, first),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_close(registry), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_drain(registry, 0u), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_destroy(registry), TURBO_OK);
    check_int_eq(flowie_cluster_peer_link_destroy(first), TURBO_OK);
    check_int_eq(flowie_cluster_peer_link_destroy(second), TURBO_OK);
  }

  it("rejects unbounded capacity and destruction before quiescence") {
    flowie_cluster_peer_registry_config_t config = FLOWIE_CLUSTER_PEER_REGISTRY_CONFIG_INIT;
    flowie_cluster_peer_registry_t *registry = NULL;
    check_int_eq(flowie_cluster_peer_registry_create(&config, &registry), TURBO_EINVAL);
    check_null(registry);
    config.max_links = 1u;
    config.max_inflight_sends = 1u;
    check_int_eq(flowie_cluster_peer_registry_create(&config, &registry), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_destroy(registry), TURBO_EBUSY);
    check_int_eq(flowie_cluster_peer_registry_close(registry), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_drain(registry, 0u), TURBO_OK);
    check_int_eq(flowie_cluster_peer_registry_destroy(registry), TURBO_OK);
  }
}
