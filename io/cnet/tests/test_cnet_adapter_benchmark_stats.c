#include "cnet_adapter_benchmark_stats.h"
#include "tinytest.h"

#include <salts/error_codes.h>

#include <math.h>
#include <stdint.h>

spec("CNet adapter benchmark statistics") {
  it("uses deterministic nearest-rank percentiles") {
    const uint64_t values[] = {50u, 10u, 40u, 30u, 20u};
    uint64_t result = 0u;

    check_equal(tf_cnet_benchmark_percentile_u64(values, 5u, 1u, &result), SALTS_OK);
    check_equal(result, UINT64_C(10));
    check_equal(tf_cnet_benchmark_percentile_u64(values, 5u, 50u, &result), SALTS_OK);
    check_equal(result, UINT64_C(30));
    check_equal(tf_cnet_benchmark_percentile_u64(values, 5u, 95u, &result), SALTS_OK);
    check_equal(result, UINT64_C(50));
    check_equal(tf_cnet_benchmark_percentile_u64(values, 5u, 100u, &result), SALTS_OK);
    check_equal(result, UINT64_C(50));
  }

  it("summarizes independent replicates with median and MAD") {
    const double values[] = {30.0, 10.0, 200.0, 20.0, 5.0};
    tf_cnet_benchmark_summary_t result = {0};

    check_equal(tf_cnet_benchmark_summarize(values, 5u, &result), SALTS_OK);
    check_equal(result.median, 20.0);
    check_equal(result.mad, 10.0);
  }

  it("rejects invalid, overflowed, and non-finite samples") {
    const uint64_t latency = 1u;
    const double non_positive[] = {1.0, 0.0};
    const double non_finite[] = {1.0, INFINITY};
    tf_cnet_benchmark_summary_t summary = {0};
    uint64_t percentile = 0u;

    check_equal(tf_cnet_benchmark_percentile_u64(NULL, 1u, 50u, &percentile), SALTS_EINVAL);
    check_equal(tf_cnet_benchmark_percentile_u64(&latency, 0u, 50u, &percentile), SALTS_EINVAL);
    check_equal(tf_cnet_benchmark_percentile_u64(&latency, 1u, 0u, &percentile), SALTS_ERANGE);
    check_equal(tf_cnet_benchmark_percentile_u64(&latency, 1u, 101u, &percentile), SALTS_ERANGE);
    check_equal(tf_cnet_benchmark_percentile_u64(&latency, SIZE_MAX, 50u, &percentile),
                SALTS_ERANGE);
    check_equal(tf_cnet_benchmark_summarize(NULL, 1u, &summary), SALTS_EINVAL);
    check_equal(tf_cnet_benchmark_summarize(non_positive, SIZE_MAX, &summary), SALTS_ERANGE);
    check_equal(tf_cnet_benchmark_summarize(non_positive, 2u, &summary), SALTS_ERANGE);
    check_equal(tf_cnet_benchmark_summarize(non_finite, 2u, &summary), SALTS_ERANGE);
  }
}
