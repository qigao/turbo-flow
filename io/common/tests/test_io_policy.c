#include "flow_io_policy.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>

typedef struct budget_wait_s {
  tf_io_budget_t *budget;
  atomic_int entered;
  atomic_int result;
} budget_wait_t;

static void acquire_waiter(void *arg) {
  budget_wait_t *wait = (budget_wait_t *)arg;
  atomic_store_explicit(&wait->entered, 1, memory_order_release);
  atomic_store_explicit(&wait->result, tf_io_budget_acquire(wait->budget, 4), memory_order_release);
}

spec("io policy primitives") {
  it("rejects timeout configuration for non-blocking admission") {
    tf_io_budget_t budget;
    const tf_io_budget_config_t invalid_timeout = {1, 0, TF_IO_ADMISSION_FAIL, 1};
    const tf_io_budget_config_t invalid_policy = {1, 0, (tf_io_admission_policy_t)99, 0};

    check_int_eq(tf_io_budget_init(&budget, &invalid_timeout), TURBO_EINVAL);
    check_int_eq(tf_io_budget_init(&budget, &invalid_policy), TURBO_EINVAL);
  }

  it("commits message and byte admission as one bounded reservation") {
    tf_io_budget_t budget;
    tf_io_budget_snapshot_t snapshot;
    const tf_io_budget_config_t config = {2, 6, TF_IO_ADMISSION_FAIL, 0};

    check_int_eq(tf_io_budget_init(&budget, &config), TURBO_OK);
    check_int_eq(tf_io_budget_open(&budget), TURBO_OK);
    check_int_eq(tf_io_budget_acquire(&budget, 4), TURBO_OK);
    check_int_eq(tf_io_budget_acquire(&budget, 4), TURBO_ENOSPC);
    check_int_eq(tf_io_budget_snapshot(&budget, &snapshot), TURBO_OK);
    check_size_eq(snapshot.messages, 1);
    check_size_eq(snapshot.bytes, 4);
    check_int_eq(tf_io_budget_release(&budget, 4), TURBO_OK);
    tf_io_budget_destroy(&budget);
  }

  it("interrupts blocked admission when its owner closes") {
    tf_io_budget_t budget;
    budget_wait_t wait;
    turbo_thread_t thread;
    const tf_io_budget_config_t config = {1, 0, TF_IO_ADMISSION_BLOCK, UINT64_MAX};

    check_int_eq(tf_io_budget_init(&budget, &config), TURBO_OK);
    check_int_eq(tf_io_budget_open(&budget), TURBO_OK);
    check_int_eq(tf_io_budget_acquire(&budget, 1), TURBO_OK);
    wait.budget = &budget;
    atomic_init(&wait.entered, 0);
    atomic_init(&wait.result, TURBO_EALREADY);
    check_int_eq(turbo_thread_create(&thread, acquire_waiter, &wait), TURBO_OK);
    while (!atomic_load_explicit(&wait.entered, memory_order_acquire))
      turbo_thread_yield();
    tf_io_budget_close(&budget);
    check_int_eq(turbo_thread_join(&thread), TURBO_OK);
    turbo_thread_destroy(&thread);
    check_int_eq(atomic_load_explicit(&wait.result, memory_order_acquire), TURBO_ESHUTDOWN);
    check_int_eq(tf_io_budget_release(&budget, 1), TURBO_OK);
    tf_io_budget_destroy(&budget);
  }

  it("selects candidates in round robin order without owning the candidate set") {
    tf_round_robin_t selector;
    size_t index = SIZE_MAX;
    tf_round_robin_init(&selector);
    check_int_eq(tf_round_robin_next(&selector, 3, &index), TURBO_OK);
    check_size_eq(index, 0);
    check_int_eq(tf_round_robin_next(&selector, 3, &index), TURBO_OK);
    check_size_eq(index, 1);
    check_int_eq(tf_round_robin_next(&selector, 3, &index), TURBO_OK);
    check_size_eq(index, 2);
    check_int_eq(tf_round_robin_next(&selector, 3, &index), TURBO_OK);
    check_size_eq(index, 0);
  }
}
