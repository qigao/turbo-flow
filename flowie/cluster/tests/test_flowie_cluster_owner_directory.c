#include "flowie_cluster_owner_directory_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdio.h>
#include <string.h>

static void owner_directory_entries(flowie_cluster_owner_directory_entry_t *entries,
                                    size_t count, uint64_t deadline_ns) {
  for (size_t index = 0u; index < count; ++index) {
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0};
    char node_id[32];
    int length;
    entries[index] =
        (flowie_cluster_owner_directory_entry_t)FLOWIE_CLUSTER_OWNER_DIRECTORY_ENTRY_INIT;
    entries[index].shard_id = (uint32_t)index;
    entries[index].local_deadline_ns = deadline_ns;
    boot_id[0] = (uint8_t)(index + 1u);
    length = snprintf(node_id, sizeof(node_id), "node-%u", (unsigned)index);
    check_int_gt(length, 0);
    check_int_eq(flowie_cluster_owner_token_init(&entries[index].owner, (uint32_t)index,
                                                 (uint64_t)index + 10u, node_id,
                                                 (size_t)length, boot_id),
                 TURBO_OK);
  }
}

static flowie_cluster_owner_directory_t *owner_directory_create(uint32_t shard_count) {
  flowie_cluster_owner_directory_config_t config = FLOWIE_CLUSTER_OWNER_DIRECTORY_CONFIG_INIT;
  flowie_cluster_owner_directory_t *directory = NULL;
  config.shard_count = shard_count;
  config.cluster_id = tstr_v_from_cstr("alpha");
  config.listener_id = tstr_v_from_cstr("mqtt");
  check_int_eq(flowie_cluster_owner_directory_create(&config, &directory), TURBO_OK);
  check_not_null(directory);
  return directory;
}

spec("Flowie cluster owner directory") {
  it("resolves a client through the versioned shard hash without database IO") {
    flowie_cluster_owner_directory_entry_t entries[4];
    flowie_cluster_owner_directory_t *directory = owner_directory_create(4u);
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_mqtt_span_t client_id = {(const uint8_t *)"client-a", strlen("client-a")};
    uint32_t expected_shard = 0u;
    owner_directory_entries(entries, 4u, turbo_hrtime() + UINT64_C(1000000000));
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 4u, 7u), TURBO_OK);
    check_int_eq(flowie_cluster_shard_for_key(
                     FLOWIE_CLUSTER_HASH_VERSION_1, FLOWIE_CLUSTER_KEY_SESSION,
                     (const uint8_t *)"alpha", strlen("alpha"), (const uint8_t *)"mqtt",
                     strlen("mqtt"), client_id.data, client_id.size, 4u, &expected_shard),
                 TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_resolve(directory, client_id, &owner), TURBO_OK);
    check_uint_eq(owner.shard_id, expected_shard);
    check_uint_eq(owner.owner_epoch, (uint64_t)expected_shard + 10u);
    flowie_cluster_owner_directory_destroy(directory);
  }

  it("fails closed for expired and unassigned shards") {
    flowie_cluster_owner_directory_entry_t entries[2];
    flowie_cluster_owner_directory_t *directory = owner_directory_create(2u);
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    owner_directory_entries(entries, 2u, 1u);
    entries[1].local_deadline_ns = 0u;
    entries[1].owner = (flowie_cluster_owner_token_t)FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    entries[1].owner.shard_id = 1u;
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 1u), TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_resolve_shard(directory, 0u, &owner),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_owner_directory_resolve_shard(directory, 1u, &owner),
                 TURBO_EBUSY);
    check_uint_eq(owner.owner_epoch, 0u);
    flowie_cluster_owner_directory_destroy(directory);
  }

  it("rejects stale and same-revision conflicting snapshots") {
    flowie_cluster_owner_directory_entry_t entries[2];
    flowie_cluster_owner_directory_t *directory = owner_directory_create(2u);
    uint64_t revision = 0u;
    owner_directory_entries(entries, 2u, turbo_hrtime() + UINT64_C(1000000000));
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 5u), TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 5u), TURBO_OK);
    entries[0].owner.owner_epoch++;
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 5u),
                 TURBO_EPROTO);
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 4u),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_owner_directory_revision(directory, &revision), TURBO_OK);
    check_uint_eq(revision, 5u);
    flowie_cluster_owner_directory_destroy(directory);
  }

  it("requires a complete index-aligned bounded shard view") {
    flowie_cluster_owner_directory_entry_t entries[2];
    flowie_cluster_owner_directory_t *directory = owner_directory_create(2u);
    owner_directory_entries(entries, 2u, turbo_hrtime() + UINT64_C(1000000000));
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 1u, 1u),
                 TURBO_EINVAL);
    entries[1].shard_id = 0u;
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 1u),
                 TURBO_EINVAL);
    entries[1] =
        (flowie_cluster_owner_directory_entry_t)FLOWIE_CLUSTER_OWNER_DIRECTORY_ENTRY_INIT;
    entries[1].shard_id = 1u;
    entries[1].owner.shard_id = 1u;
    entries[1].owner.boot_id[0] = 1u;
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 2u, 1u),
                 TURBO_EINVAL);
    flowie_cluster_owner_directory_destroy(directory);
  }
}
