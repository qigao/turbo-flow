#include "flow_timer.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>

typedef struct flow_timer_thread_ctx_s {
  tf_timer_t *timer;
  uint32_t delay_ms;
  int stop;
} flow_timer_thread_ctx_t;

static void flow_timer_test_thread(void *arg) {
  flow_timer_thread_ctx_t *ctx = (flow_timer_thread_ctx_t *)arg;
  if (!ctx || !ctx->timer) return;
  turbo_sleep_ms(ctx->delay_ms);
  if (ctx->stop) {
    tf_timer_stop(ctx->timer);
  } else {
    tf_timer_signal(ctx->timer);
  }
}

spec("flow_timer") {
  it("returns immediately on stop request") {
    tf_timer_t timer;
    check_int_eq(tf_timer_init(&timer), TURBO_OK);
    tf_timer_stop(&timer);
    check_int_eq(tf_timer_wait_for_ms(&timer, 1000), TURBO_ESHUTDOWN);
    tf_timer_destroy(&timer);
  }

  it("can be reset for a new lifecycle after stop") {
    tf_timer_t timer;
    check_int_eq(tf_timer_init(&timer), TURBO_OK);
    tf_timer_stop(&timer);
    check_int_eq(tf_timer_wait_for_ms(&timer, 1), TURBO_ESHUTDOWN);
    tf_timer_reset(&timer);
    check_int_eq(tf_timer_wait_for_ms(&timer, 1), TURBO_ETIMEDOUT);
    tf_timer_destroy(&timer);
  }

  it("notifies waiting callers before timeout") {
    tf_timer_t timer;
    turbo_thread_t thread;
    flow_timer_thread_ctx_t ctx;
    uint64_t started = turbo_hrtime();
    int rc;

    check_int_eq(tf_timer_init(&timer), TURBO_OK);

    ctx.timer = &timer;
    ctx.delay_ms = 20;
    ctx.stop = 0;

    check_int_eq(turbo_thread_create(&thread, flow_timer_test_thread, &ctx), TURBO_OK);
    rc = tf_timer_wait_for_ms(&timer, 500);
    (void)turbo_thread_join(&thread);
    check_int_eq(rc, TURBO_OK);
    check_true((turbo_hrtime() - started) < UINT64_C(500000000));
    tf_timer_destroy(&timer);
  }

  it("times out without notification") {
    tf_timer_t timer;
    uint64_t started = turbo_hrtime();
    check_int_eq(tf_timer_init(&timer), TURBO_OK);
    check_int_eq(tf_timer_wait_for_ms(&timer, 20), TURBO_ETIMEDOUT);
    check_true((turbo_hrtime() - started) >= UINT64_C(20000000));
    tf_timer_destroy(&timer);
  }

  it("waits until deadline") {
    tf_timer_t timer;
    uint64_t deadline = turbo_hrtime() + UINT64_C(30000000);
    check_int_eq(tf_timer_init(&timer), TURBO_OK);
    check_int_eq(tf_timer_wait_until_ns(&timer, deadline), TURBO_ETIMEDOUT);
    tf_timer_destroy(&timer);
  }

  it("interrupts an unbounded absolute deadline with shutdown") {
    tf_timer_t timer;
    turbo_thread_t thread;
    flow_timer_thread_ctx_t ctx;
    uint64_t started = turbo_hrtime();
    int rc;

    check_int_eq(tf_timer_init(&timer), TURBO_OK);
    ctx.timer = &timer;
    ctx.delay_ms = 20;
    ctx.stop = 1;
    check_int_eq(turbo_thread_create(&thread, flow_timer_test_thread, &ctx), TURBO_OK);
    rc = tf_timer_wait_until_ns(&timer, UINT64_MAX);
    check_int_eq(turbo_thread_join(&thread), TURBO_OK);
    check_int_eq(rc, TURBO_ESHUTDOWN);
    check_true((turbo_hrtime() - started) < UINT64_C(500000000));
    tf_timer_destroy(&timer);
  }
}
