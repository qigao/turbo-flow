#ifndef FLOW_TIMER_H
#define FLOW_TIMER_H

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tf_timer_s {
  turbo_mutex_t mutex;
  turbo_cond_t cond;
  atomic_int stopped;
} tf_timer_t;

int tf_timer_init(tf_timer_t *timer);
void tf_timer_destroy(tf_timer_t *timer);

void tf_timer_signal(tf_timer_t *timer);
void tf_timer_stop(tf_timer_t *timer);
/** Reset a stopped timer before reuse. The caller must ensure no waiters are active. */
void tf_timer_reset(tf_timer_t *timer);

/**
 * Wait up to timeout_ms for either a signal or stop event.
 * Return TURBO_OK for signal/notify, TURBO_ESHUTDOWN if stopped, TURBO_ETIMEDOUT on timeout.
 */
int tf_timer_wait_for_ms(tf_timer_t *timer, uint64_t timeout_ms);

/**
 * Wait until monotonic deadline_ns for either a signal or stop event.
 * Return TURBO_OK for signal/notify, TURBO_ESHUTDOWN if stopped, TURBO_ETIMEDOUT on timeout.
 */
int tf_timer_wait_until_ns(tf_timer_t *timer, uint64_t deadline_ns);

#ifdef __cplusplus
}
#endif

#endif
