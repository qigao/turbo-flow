#ifndef FLOW_IO_POLICY_H
#define FLOW_IO_POLICY_H

#include "turbo_thread.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum tf_io_admission_policy_e {
  TF_IO_ADMISSION_FAIL = 0,
  TF_IO_ADMISSION_BLOCK
} tf_io_admission_policy_t;

typedef struct tf_io_budget_config_s {
  size_t max_messages;
  size_t max_bytes;
  tf_io_admission_policy_t admission;
  /** BLOCK only. Zero checks immediately; UINT64_MAX waits without a deadline. */
  uint64_t wait_timeout_ms;
} tf_io_budget_config_t;

typedef struct tf_io_budget_snapshot_s {
  size_t messages;
  size_t bytes;
  size_t max_messages;
  size_t max_bytes;
  int accepting;
} tf_io_budget_snapshot_t;

/**
 * Composite message/byte admission budget.
 *
 * One mutex owns both counters so a reservation is committed atomically across
 * the two limits. The owner must close admission before destroying the budget.
 */
typedef struct tf_io_budget_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  size_t messages;
  size_t bytes;
  size_t max_messages;
  size_t max_bytes;
  uint64_t wait_timeout_ms;
  tf_io_admission_policy_t admission;
  int accepting;
  int initialized;
} tf_io_budget_t;

int tf_io_budget_init(tf_io_budget_t *budget, const tf_io_budget_config_t *config);
void tf_io_budget_destroy(tf_io_budget_t *budget);
int tf_io_budget_open(tf_io_budget_t *budget);
void tf_io_budget_close(tf_io_budget_t *budget);
int tf_io_budget_acquire(tf_io_budget_t *budget, size_t bytes);
int tf_io_budget_release(tf_io_budget_t *budget, size_t bytes);
int tf_io_budget_drain(tf_io_budget_t *budget, uint64_t timeout_ms);
int tf_io_budget_snapshot(tf_io_budget_t *budget, tf_io_budget_snapshot_t *out);

typedef struct tf_round_robin_s {
  atomic_uint_fast64_t cursor;
} tf_round_robin_t;

void tf_round_robin_init(tf_round_robin_t *selector);
int tf_round_robin_next(tf_round_robin_t *selector, size_t candidate_count, size_t *index);

#ifdef __cplusplus
}
#endif

#endif
