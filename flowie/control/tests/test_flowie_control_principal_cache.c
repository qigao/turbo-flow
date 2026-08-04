#include "flowie_control_principal_cache_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

static uint64_t principal_cache_test_clock(void *ctx) { return *(const uint64_t *)ctx; }

static flowie_control_principal_snapshot_t principal_cache_snapshot(const char *root,
                                                                    const char *principal,
                                                                    uint64_t user_revision) {
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  memcpy(snapshot.domain_id, root, strlen(root) + 1u);
  memcpy(snapshot.principal_id, principal, strlen(principal) + 1u);
  memcpy(snapshot.principal_type, "device", sizeof("device"));
  snapshot.user_revision = user_revision;
  snapshot.credential_revision = user_revision + 1u;
  snapshot.effective_groups.group_count = 1u;
  memcpy(snapshot.effective_groups.groups[0], root, strlen(root) + 1u);
  return snapshot;
}

typedef struct principal_cache_reader_s {
  flowie_control_principal_cache_t *cache;
  atomic_int failures;
} principal_cache_reader_t;

static void principal_cache_reader(void *arg) {
  principal_cache_reader_t *reader = (principal_cache_reader_t *)arg;
  for (size_t index = 0u; index < 100u; ++index) {
    flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    int hit = 0;
    if (flowie_control_principal_cache_get(reader->cache, "root-a", "device-a", 1u, 2u, 3u,
                                           7u, &snapshot, &hit) != TURBO_OK || !hit ||
        strcmp(snapshot.principal_id, "device-a") != 0)
      atomic_fetch_add_explicit(&reader->failures, 1, memory_order_relaxed);
  }
}

spec("Flowie control principal snapshot cache") {
  it("enforces revisions, TTL, LRU capacity and concurrent reads") {
    uint64_t now_ms = 100u;
    flowie_control_auth_cache_config_t config = FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT;
    flowie_control_principal_cache_t *cache = NULL;
    flowie_control_principal_snapshot_t first =
        principal_cache_snapshot("root-a", "device-a", 1u);
    flowie_control_principal_snapshot_t second =
        principal_cache_snapshot("root-a", "device-b", 3u);
    flowie_control_principal_snapshot_t third =
        principal_cache_snapshot("root-a", "device-c", 5u);
    flowie_control_principal_snapshot_t out = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    principal_cache_reader_t reader;
    turbo_thread_t threads[4];
    int hit = 0;

    config.capacity = 2u;
    config.ttl_ms = 10u;
    config.clock_ms = principal_cache_test_clock;
    config.clock_ctx = &now_ms;
    check_int_eq(flowie_control_principal_cache_create(&config, &cache), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_put(cache, &first, 3u, 7u), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 1u, 2u, 3u,
                                                     7u, &out, &hit), TURBO_OK);
    check_true(hit);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 2u, 2u, 3u,
                                                     7u, &out, &hit), TURBO_ENOENT);
    check_false(hit);
    check_int_eq(flowie_control_principal_cache_put(cache, &first, 3u, 7u), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 1u, 2u, 3u,
                                                     8u, &out, &hit), TURBO_ENOENT);
    check_false(hit);
    check_int_eq(flowie_control_principal_cache_put(cache, &first, 3u, 7u), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 1u, 2u, 4u,
                                                     7u, &out, &hit), TURBO_ENOENT);
    check_false(hit);
    check_int_eq(flowie_control_principal_cache_put(cache, &first, 3u, 7u), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_put(cache, &second, 3u, 7u), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 1u, 2u, 3u,
                                                     7u, &out, &hit), TURBO_OK);
    check_true(hit);
    check_int_eq(flowie_control_principal_cache_put(cache, &third, 3u, 7u), TURBO_OK);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-b", 3u, 4u, 3u,
                                                     7u, &out, &hit), TURBO_ENOENT);
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 1u, 2u, 3u,
                                                     7u, &out, &hit), TURBO_OK);
    check_true(hit);
    now_ms = 110u;
    check_int_eq(flowie_control_principal_cache_get(cache, "root-a", "device-a", 1u, 2u, 3u,
                                                     7u, &out, &hit), TURBO_ENOENT);
    check_false(hit);
    now_ms = 100u;
    check_int_eq(flowie_control_principal_cache_put(cache, &first, 3u, 7u), TURBO_OK);
    reader.cache = cache;
    atomic_init(&reader.failures, 0);
    for (size_t index = 0u; index < 4u; ++index)
      check_int_eq(turbo_thread_create(&threads[index], principal_cache_reader, &reader), 0);
    for (size_t index = 0u; index < 4u; ++index) turbo_thread_join(&threads[index]);
    check_int_eq(atomic_load_explicit(&reader.failures, memory_order_relaxed), 0);
    flowie_control_principal_cache_destroy(cache);
  }
}
