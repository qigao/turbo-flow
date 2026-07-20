#include "flowie_control_auth_rate_limiter_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>

#define RATE_LIMITER_CERT_A                                                                        \
  "sha256:"                                                                                        \
  "aaaaaaaaaaaaaaaa"                                                                               \
  "aaaaaaaaaaaaaaaa"                                                                               \
  "aaaaaaaaaaaaaaaa"                                                                               \
  "aaaaaaaaaaaaaaaa"

static uint64_t rate_limiter_test_clock(void *ctx) { return *(const uint64_t *)ctx; }

typedef struct rate_limiter_concurrent_task_s {
  flowie_control_auth_rate_limiter_t *limiter;
  atomic_int accepted;
  atomic_int rejected;
  atomic_int errors;
} rate_limiter_concurrent_task_t;

static void rate_limiter_concurrent_acquire(void *arg) {
  rate_limiter_concurrent_task_t *task = (rate_limiter_concurrent_task_t *)arg;
  for (size_t index = 0u; index < 50u; ++index) {
    int rc = flowie_control_auth_rate_limiter_acquire(task->limiter, RATE_LIMITER_CERT_A, "root-a",
                                                       "device-a");
    if (rc == TURBO_OK)
      atomic_fetch_add_explicit(&task->accepted, 1, memory_order_relaxed);
    else if (rc == TURBO_EBUSY)
      atomic_fetch_add_explicit(&task->rejected, 1, memory_order_relaxed);
    else
      atomic_fetch_add_explicit(&task->errors, 1, memory_order_relaxed);
  }
}

spec("Flowie control auth rate limiter") {
  it("applies caller and identity buckets with refill and success reset") {
    uint64_t now_ms = 100u;
    flowie_control_auth_rate_limiter_config_t config =
        FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT;
    flowie_control_auth_rate_limiter_t *limiter = NULL;
    config.caller_capacity = 2u;
    config.identity_capacity = 2u;
    config.caller_per_second = 1u;
    config.caller_burst = 10u;
    config.identity_per_second = 1u;
    config.identity_burst = 2u;
    config.clock_ms = rate_limiter_test_clock;
    config.clock_ctx = &now_ms;
    check_int_eq(flowie_control_auth_rate_limiter_create(&config, &limiter), TURBO_OK);

    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-a"),
                 TURBO_OK);
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-a"),
                 TURBO_OK);
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-a"),
                 TURBO_EBUSY);
    flowie_control_auth_rate_limiter_record_success(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                    "device-a");
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-a"),
                 TURBO_OK);

    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-b"),
                 TURBO_OK);
    check_size_eq(flowie_control_auth_rate_limiter_caller_size(limiter), 1u);
    check_size_eq(flowie_control_auth_rate_limiter_identity_size(limiter), 2u);
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-c"),
                 TURBO_OK);
    check_size_eq(flowie_control_auth_rate_limiter_identity_size(limiter), 2u);
    flowie_control_auth_rate_limiter_destroy(limiter);
  }

  it("limits the verified caller across rotating identities") {
    uint64_t now_ms = 100u;
    flowie_control_auth_rate_limiter_config_t config =
        FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT;
    flowie_control_auth_rate_limiter_t *limiter = NULL;
    config.caller_capacity = 1u;
    config.identity_capacity = 4u;
    config.caller_per_second = 1u;
    config.caller_burst = 2u;
    config.identity_per_second = 10u;
    config.identity_burst = 10u;
    config.clock_ms = rate_limiter_test_clock;
    config.clock_ctx = &now_ms;
    check_int_eq(flowie_control_auth_rate_limiter_create(&config, &limiter), TURBO_OK);
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-a"),
                 TURBO_OK);
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-b"),
                 TURBO_OK);
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-c"),
                 TURBO_EBUSY);
    now_ms = 1100u;
    check_int_eq(flowie_control_auth_rate_limiter_acquire(limiter, RATE_LIMITER_CERT_A, "root-a",
                                                          "device-c"),
                 TURBO_OK);
    flowie_control_auth_rate_limiter_destroy(limiter);
  }

  it("rejects unsafe configuration") {
    flowie_control_auth_rate_limiter_config_t config =
        FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT;
    flowie_control_auth_rate_limiter_t *limiter = NULL;
    config.caller_burst = 0u;
    check_int_eq(flowie_control_auth_rate_limiter_create(&config, &limiter), TURBO_EINVAL);
    check_null(limiter);
  }

  it("atomically enforces a shared burst under concurrent callers") {
    uint64_t now_ms = 100u;
    flowie_control_auth_rate_limiter_config_t config =
        FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT;
    flowie_control_auth_rate_limiter_t *limiter = NULL;
    rate_limiter_concurrent_task_t task;
    turbo_thread_t threads[4];
    config.caller_capacity = 1u;
    config.identity_capacity = 1u;
    config.caller_per_second = 1u;
    config.caller_burst = 100u;
    config.identity_per_second = 1u;
    config.identity_burst = 100u;
    config.clock_ms = rate_limiter_test_clock;
    config.clock_ctx = &now_ms;
    check_int_eq(flowie_control_auth_rate_limiter_create(&config, &limiter), TURBO_OK);
    task.limiter = limiter;
    atomic_init(&task.accepted, 0);
    atomic_init(&task.rejected, 0);
    atomic_init(&task.errors, 0);
    for (size_t index = 0u; index < 4u; ++index)
      check_int_eq(turbo_thread_create(&threads[index], rate_limiter_concurrent_acquire, &task), 0);
    for (size_t index = 0u; index < 4u; ++index) turbo_thread_join(&threads[index]);
    check_int_eq(atomic_load_explicit(&task.accepted, memory_order_relaxed), 100);
    check_int_eq(atomic_load_explicit(&task.rejected, memory_order_relaxed), 100);
    check_int_eq(atomic_load_explicit(&task.errors, memory_order_relaxed), 0);
    flowie_control_auth_rate_limiter_destroy(limiter);
  }
}
