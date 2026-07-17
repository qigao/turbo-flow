#ifndef TURBO_FLOW_FMQ_TEST_BENCH_STATS_H
#define TURBO_FLOW_FMQ_TEST_BENCH_STATS_H

#include <stddef.h>
#include <stdint.h>

static inline int fmq_bench_u64_compare(const void *lhs, const void *rhs) {
  uint64_t left = *(const uint64_t *)lhs;
  uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

/** Nearest-rank percentile over a sorted bounded sample set: O(1) time and O(1) space. */
static inline uint64_t fmq_bench_percentile(const uint64_t *sorted, size_t count,
                                            size_t percent) {
  size_t index;
  if (!sorted || count == 0u || percent > 100u) return 0u;
  index = ((count - 1u) * percent + 99u) / 100u;
  return sorted[index];
}

#endif /* TURBO_FLOW_FMQ_TEST_BENCH_STATS_H */
