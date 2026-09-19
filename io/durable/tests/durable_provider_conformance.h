#ifndef TURBO_FLOW_DURABLE_PROVIDER_CONFORMANCE_H
#define TURBO_FLOW_DURABLE_PROVIDER_CONFORMANCE_H

#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include <salts/clock.h>
#include <stddef.h>

/*
 * Provider-neutral Task 9 contract.
 *
 * Provider fixtures own configuration, storage provisioning and progress.
 * This harness deliberately observes only the public Graph durable-buffer ABI.
 */
typedef struct turbo_flow_durable_provider_conformance_v1_s {
  void *ctx;
  turbo_flow_t *flow;
  int (*publish_stable)(void *ctx, const char *admission_id);
  int (*progress)(void *ctx);
  size_t (*delivered)(void *ctx);
} turbo_flow_durable_provider_conformance_v1_t;

static void turbo_flow_durable_provider_conformance_capacity_and_replay(
    turbo_flow_durable_provider_conformance_v1_t *p) {
  turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
  size_t count = 0u;

  check_not_null(p);
  check_not_null(p->flow);
  check_not_null(p->publish_stable);
  check_not_null(p->progress);
  check_not_null(p->delivered);

  /* Stable replay is one logical admission and must not consume capacity twice. */
  check_equal(p->publish_stable(p->ctx, "parity-one"), SALTS_OK);
  check_equal(p->publish_stable(p->ctx, "parity-one"), SALTS_OK);
  check_equal(p->publish_stable(p->ctx, "parity-two"), SALTS_OK);
  check_equal(p->publish_stable(p->ctx, "parity-three"), SALTS_ENOSPC);

  /* Admission ends at the durable cut; downstream requires explicit progress. */
  check_equal(p->delivered(p->ctx), (size_t)0u);

  for (size_t i = 0u; i < 1000u && p->delivered(p->ctx) < 2u; ++i) {
    check_equal(p->progress(p->ctx), SALTS_OK);
    salts_sleep_ms(1u);
  }
  check_equal(p->delivered(p->ctx), (size_t)2u);

  /*
   * Completed history remains capacity-accounted until an explicit operator
   * action releases it. No implicit retry, reconciliation or fallback.
   */
  check_equal(p->publish_stable(p->ctx, "parity-three"), SALTS_ENOSPC);
  check_equal(turbo_flow_durable_buffer_scan_history(
                  p->flow, "intake.store", 0u, &history, 1u, &count),
              SALTS_OK);
  check_equal(count, (size_t)1u);
  check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);
  check_equal(turbo_flow_durable_buffer_forget(
                  p->flow, "intake.store", history.record_id),
              SALTS_OK);
  check_equal(p->publish_stable(p->ctx, "parity-three"), SALTS_OK);
}

#endif
