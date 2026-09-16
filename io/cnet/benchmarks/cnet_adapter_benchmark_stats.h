#ifndef TURBO_FLOW_CNET_ADAPTER_BENCHMARK_STATS_H
#define TURBO_FLOW_CNET_ADAPTER_BENCHMARK_STATS_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct tf_cnet_benchmark_summary_s {
  double median;
  double mad;
} tf_cnet_benchmark_summary_t;

int tf_cnet_benchmark_percentile_u64(const uint64_t *values, size_t count,
                                     unsigned int percentile, uint64_t *out);
int tf_cnet_benchmark_summarize(const double *values, size_t count,
                                tf_cnet_benchmark_summary_t *out);
int tf_cnet_benchmark_validate_provenance(const char *expected_commit,
                                          const char *actual_commit,
                                          bool source_dirty);

#endif /* TURBO_FLOW_CNET_ADAPTER_BENCHMARK_STATS_H */
