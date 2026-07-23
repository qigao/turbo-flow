#include "tinytest.h"
#include "turbo_flow_bitmap_index.h"
#include "turbo_flow_index_store.h"

#include <stdint.h>

enum {
  FLOW_STORE_BENCH_MEMBERS = 10000,
  FLOW_STORE_BENCH_QUERIES = 4096,
  FLOW_STORE_BENCH_SAMPLES = 100
};

static turbo_flow_store_bytes_t bench_bytes(const void *data, size_t size) {
  turbo_flow_store_bytes_t bytes;
  bytes.data = (const uint8_t *)data;
  bytes.size = size;
  return bytes;
}

spec("flowstore index baseline") {
  bench("compares HashMap membership with the former roaring selector") {
    static const char index_name[] = "members";
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    turbo_flow_index_store_t *store = NULL;
    turbo_flow_bitmap_index_t *bitmap = NULL;
    volatile size_t hash_hits = 0u;
    volatile size_t bitmap_hits = 0u;

    limits.initial_records = FLOW_STORE_BENCH_MEMBERS;
    limits.max_records = FLOW_STORE_BENCH_MEMBERS;
    limits.max_bytes = FLOW_STORE_BENCH_MEMBERS * sizeof(uint64_t) + sizeof(index_name);
    limits.max_item_bytes = sizeof(index_name) + sizeof(uint64_t);
    limits.full_policy = TURBO_FLOW_STORE_FULL_REJECT;
    check_int_eq(turbo_flow_index_store_create_memory(&limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_bitmap_index_create(FLOW_STORE_BENCH_MEMBERS, &bitmap), TURBO_OK);
    for (uint64_t member = 0u; member < FLOW_STORE_BENCH_MEMBERS; ++member) {
      check_int_eq(turbo_flow_index_store_add(store,
                                              bench_bytes(index_name, sizeof(index_name) - 1u),
                                              bench_bytes(&member, sizeof(member))),
                   TURBO_OK);
      check_int_eq(turbo_flow_bitmap_index_add(bitmap, member), TURBO_OK);
    }

    benchmark_ops("FlowStore HashMap contains", FLOW_STORE_BENCH_SAMPLES,
                  FLOW_STORE_BENCH_QUERIES) {
      for (uint64_t member = 0u; member < FLOW_STORE_BENCH_QUERIES; ++member) {
        int present = 0;
        if (turbo_flow_index_store_contains(store, bench_bytes(index_name, sizeof(index_name) - 1u),
                                            bench_bytes(&member, sizeof(member)),
                                            &present) == TURBO_OK) {
          hash_hits += (size_t)present;
        }
      }
    }

    benchmark_ops("CRoaring64 contains", FLOW_STORE_BENCH_SAMPLES, FLOW_STORE_BENCH_QUERIES) {
      for (uint64_t member = 0u; member < FLOW_STORE_BENCH_QUERIES; ++member) {
        int present = 0;
        check_int_eq(turbo_flow_bitmap_index_contains(bitmap, member, &present), TURBO_OK);
        bitmap_hits += (size_t)present;
      }
    }

    check_size_eq(hash_hits, bitmap_hits);
    turbo_flow_bitmap_index_destroy(bitmap);
    turbo_flow_index_store_destroy(store);
  }
}
