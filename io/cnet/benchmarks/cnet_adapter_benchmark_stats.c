#include "cnet_adapter_benchmark_stats.h"

#include <salts/error_codes.h>

#include <math.h>
#include <stdlib.h>

static int compare_u64(const void *left, const void *right) {
  const uint64_t lhs = *(const uint64_t *)left;
  const uint64_t rhs = *(const uint64_t *)right;
  return lhs < rhs ? -1 : lhs > rhs;
}

static int compare_double(const void *left, const void *right) {
  const double lhs = *(const double *)left;
  const double rhs = *(const double *)right;
  return lhs < rhs ? -1 : lhs > rhs;
}

static double median_sorted(const double *values, size_t count) {
  const size_t middle = count / 2u;
  return count % 2u != 0u
             ? values[middle]
             : values[middle - 1u] + (values[middle] - values[middle - 1u]) / 2.0;
}

int tf_cnet_benchmark_percentile_u64(const uint64_t *values, size_t count,
                                     unsigned int percentile, uint64_t *out) {
  uint64_t *scratch;
  size_t rank;

  if (!values || !out || count == 0u) return SALTS_EINVAL;
  if (percentile == 0u || percentile > 100u) return SALTS_ERANGE;
  if (count > SIZE_MAX / sizeof(*scratch)) return SALTS_ERANGE;
  scratch = (uint64_t *)malloc(count * sizeof(*scratch));
  if (!scratch) return SALTS_ENOMEM;
  for (size_t index = 0u; index < count; ++index)
    scratch[index] = values[index];
  qsort(scratch, count, sizeof(*scratch), compare_u64);
  rank = (count / 100u) * percentile +
         ((count % 100u) * percentile + 99u) / 100u;
  *out = scratch[rank - 1u];
  free(scratch);
  return SALTS_OK;
}

int tf_cnet_benchmark_summarize(const double *values, size_t count,
                                tf_cnet_benchmark_summary_t *out) {
  double *scratch;
  double median;

  if (!values || !out || count == 0u) return SALTS_EINVAL;
  if (count > SIZE_MAX / sizeof(*scratch)) return SALTS_ERANGE;
  for (size_t index = 0u; index < count; ++index) {
    if (!isfinite(values[index]) || values[index] <= 0.0) return SALTS_ERANGE;
  }
  scratch = (double *)malloc(count * sizeof(*scratch));
  if (!scratch) return SALTS_ENOMEM;
  for (size_t index = 0u; index < count; ++index)
    scratch[index] = values[index];
  qsort(scratch, count, sizeof(*scratch), compare_double);
  median = median_sorted(scratch, count);
  for (size_t index = 0u; index < count; ++index)
    scratch[index] = fabs(values[index] - median);
  qsort(scratch, count, sizeof(*scratch), compare_double);
  out->median = median;
  out->mad = median_sorted(scratch, count);
  free(scratch);
  return SALTS_OK;
}
