#include "flow_event_time_watermark.h"

#include "tinytest.h"
#include "turbo_flow.h"

#include <stdint.h>

#define WATERMARK_OBSERVE_BENCH_ITERS 1000000u

spec("event-time watermark owner benchmark") {
  bench("atomic observation hot path") {
    turbo_flow_event_time_window_store_config_t store_config =
        TURBO_FLOW_EVENT_TIME_WINDOW_STORE_CONFIG_INIT;
    tf_event_time_watermark_config_t owner_config = TF_EVENT_TIME_WATERMARK_CONFIG_INIT;
    tf_event_time_watermark_snapshot_t snapshot = TF_EVENT_TIME_WATERMARK_SNAPSHOT_INIT;
    turbo_flow_event_time_window_store_t *store;
    turbo_flow_t *flow;
    tf_event_time_watermark_owner_t *owner;
    uint64_t event_time_ns = 0u;
    int status = TURBO_OK;

    store_config.max_windows = 1u;
    store_config.max_key_size = sizeof(uint64_t);
    store_config.max_value_size = sizeof(uint64_t);
    store_config.max_total_bytes = sizeof(uint64_t) * 3u;
    store_config.window_size_ns = UINT64_C(1000000000);
    store = turbo_flow_event_time_window_store_create(&store_config);
    flow = turbo_flow_create();
    check_not_null(store);
    check_not_null(flow);
    owner_config.flow = flow;
    owner_config.store = store;
    owner = tf_event_time_watermark_owner_create(&owner_config);
    check_not_null(owner);

    benchmark_ops("owner=event-time-watermark operation=observe atomic=max",
                  WATERMARK_OBSERVE_BENCH_ITERS, 1u) {
      status = tf_event_time_watermark_owner_observe(owner, ++event_time_ns);
    }
    check_int_eq(status, TURBO_OK);
    check_int_eq(tf_event_time_watermark_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.max_observed_event_time_ns, WATERMARK_OBSERVE_BENCH_ITERS);
    check_uint_eq(snapshot.observed_event_count, WATERMARK_OBSERVE_BENCH_ITERS);

    tf_event_time_watermark_owner_destroy(owner);
    turbo_flow_destroy(flow);
    turbo_flow_event_time_window_store_destroy(store);
  }
}
