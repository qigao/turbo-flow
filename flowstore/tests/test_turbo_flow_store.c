#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_bitmap_index.h"
#include "turbo_flow_index_store.h"
#include "turbo_flow_index_store_provider.h"
#include "turbo_flow_log_store.h"
#include "turbo_flow_log_store_provider.h"
#include "turbo_flow_mqtt_store.h"
#include "turbo_flow_series_store.h"
#include "turbo_flow_series_store_provider.h"
#include "turbo_flow_state_store.h"
#include "turbo_flow_store_policy.h"

#include <string.h>

static turbo_flow_store_bytes_t test_bytes(const char *text) {
  turbo_flow_store_bytes_t bytes;
  bytes.data = (const uint8_t *)text;
  bytes.size = strlen(text);
  return bytes;
}

typedef struct mqtt_store_test_backend_s {
  int scan_calls;
  int commit_calls;
  turbo_flow_record_mutation_t last_mutation;
} mqtt_store_test_backend_t;

static int mqtt_store_test_scan(void *ctx, turbo_flow_record_visit_fn visit, void *visit_ctx) {
  mqtt_store_test_backend_t *backend = (mqtt_store_test_backend_t *)ctx;
  turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
  if (!backend || !visit) return TURBO_EINVAL;
  backend->scan_calls += 1;
  record.key = (const uint8_t *)"session";
  record.key_size = 7u;
  record.revision = 1u;
  record.value = (const uint8_t *)"fact";
  record.value_size = 4u;
  return visit(visit_ctx, &record);
}

static int mqtt_store_test_commit(void *ctx, const turbo_flow_record_mutation_t *mutations,
                                  size_t mutation_count) {
  mqtt_store_test_backend_t *backend = (mqtt_store_test_backend_t *)ctx;
  if (!backend || !mutations || mutation_count != 1u) return TURBO_EINVAL;
  backend->commit_calls += 1;
  backend->last_mutation = mutations[0];
  return TURBO_OK;
}

static int mqtt_store_test_visit(void *ctx, const turbo_flow_record_view_t *record) {
  size_t *count = (size_t *)ctx;
  if (!record || !count) return TURBO_EINVAL;
  *count += 1u;
  return TURBO_OK;
}

spec("turbo_flow_mqtt_store") {
  it("owns the MQTT fact boundary while borrowing backend storage") {
    mqtt_store_test_backend_t backend_context = {0};
    turbo_flow_record_store_t backend = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_mqtt_store_t *store = NULL;
    turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
    size_t visited = 0u;

    backend.capabilities = TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
    backend.max_key_size = 128u;
    backend.max_value_size = 1024u;
    backend.max_batch_size = 4u;
    backend.max_records = 8u;
    backend.ctx = &backend_context;
    backend.scan = mqtt_store_test_scan;
    backend.commit = mqtt_store_test_commit;
    check_int_eq(turbo_flow_mqtt_store_create(&backend, &store), TURBO_OK);
    check_not_null(store);
    check_size_eq(turbo_flow_mqtt_store_max_key_size(store), 128u);
    check_uint_eq(turbo_flow_mqtt_store_capabilities(store),
                  TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH);
    check_int_eq(turbo_flow_mqtt_store_scan(store, mqtt_store_test_visit, &visited), TURBO_OK);
    check_size_eq(visited, 1u);
    check_int_eq(backend_context.scan_calls, 1);
    mutation.key = (const uint8_t *)"session";
    mutation.key_size = 7u;
    mutation.next_revision = 2u;
    mutation.value = (const uint8_t *)"next";
    mutation.value_size = 4u;
    check_int_eq(turbo_flow_mqtt_store_commit(store, &mutation, 1u), TURBO_OK);
    check_int_eq(backend_context.commit_calls, 1);
    check_uint_eq(backend_context.last_mutation.next_revision, 2u);
    turbo_flow_mqtt_store_destroy(store);
  }
}

spec("turbo_flow_store_provider_abi") {
  it("provides complete zeroed operation table initializers") {
    turbo_flow_index_store_provider_ops_t index_ops = TURBO_FLOW_INDEX_STORE_PROVIDER_OPS_INIT;
    turbo_flow_log_store_provider_ops_t log_ops = TURBO_FLOW_LOG_STORE_PROVIDER_OPS_INIT;
    turbo_flow_series_store_provider_ops_t series_ops = TURBO_FLOW_SERIES_STORE_PROVIDER_OPS_INIT;

    check_size_eq(index_ops.size, sizeof(index_ops));
    check_uint_eq(index_ops.api_version, TURBO_FLOW_INDEX_STORE_PROVIDER_API_VERSION);
    check_true(index_ops.close == NULL && index_ops.destroy == NULL && index_ops.stats == NULL);
    check_size_eq(log_ops.size, sizeof(log_ops));
    check_uint_eq(log_ops.api_version, TURBO_FLOW_LOG_STORE_PROVIDER_API_VERSION);
    check_true(log_ops.close == NULL && log_ops.destroy == NULL && log_ops.stats == NULL);
    check_size_eq(series_ops.size, sizeof(series_ops));
    check_uint_eq(series_ops.api_version, TURBO_FLOW_SERIES_STORE_PROVIDER_API_VERSION);
    check_true(series_ops.close == NULL && series_ops.destroy == NULL && series_ops.stats == NULL);
  }
}

spec("turbo_flow_bitmap_index") {
  it("bounds integer membership and selects by rank") {
    turbo_flow_bitmap_index_t *index = NULL;
    size_t count = 0u;
    uint64_t member = 0u;
    int present = 0;

    check_int_eq(turbo_flow_bitmap_index_create(2u, &index), TURBO_OK);
    check_int_eq(turbo_flow_bitmap_index_add(index, 42u), TURBO_OK);
    check_int_eq(turbo_flow_bitmap_index_add(index, 7u), TURBO_OK);
    check_int_eq(turbo_flow_bitmap_index_add(index, 42u), TURBO_OK);
    check_int_eq(turbo_flow_bitmap_index_add(index, 99u), TURBO_ENOSPC);
    check_int_eq(turbo_flow_bitmap_index_contains(index, 42u, &present), TURBO_OK);
    check_int_eq(present, 1);
    check_int_eq(turbo_flow_bitmap_index_count(index, &count), TURBO_OK);
    check_size_eq(count, 2u);
    check_int_eq(turbo_flow_bitmap_index_select(index, 0u, &member), TURBO_OK);
    check_uint_eq(member, 7u);
    check_int_eq(turbo_flow_bitmap_index_select(index, 2u, &member), TURBO_ERANGE);
    check_int_eq(turbo_flow_bitmap_index_remove(index, 7u), TURBO_OK);
    check_int_eq(turbo_flow_bitmap_index_add(index, 99u), TURBO_OK);
    turbo_flow_bitmap_index_destroy(index);
  }
}

static turbo_flow_store_limits_t test_limits(size_t records, size_t bytes,
                                             turbo_flow_store_full_policy_t policy) {
  turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
  limits.initial_records = records < 4u ? records : 4u;
  limits.max_records = records;
  limits.max_bytes = bytes;
  limits.max_item_bytes = bytes;
  limits.full_policy = policy;
  return limits;
}

spec("turbo_flow_store_policy") {
  it("routes by data semantics and configured local resource limits") {
    turbo_flow_store_policy_t policy = TURBO_FLOW_STORE_POLICY_INIT;
    turbo_flow_store_requirements_t requirements = TURBO_FLOW_STORE_REQUIREMENTS_INIT;
    turbo_flow_store_decision_t decision = TURBO_FLOW_STORE_DECISION_INIT;

    policy.local_max_records = 1000u;
    policy.local_max_bytes = 1024u * 1024u;
    policy.local_max_item_bytes = 4096u;
    policy.local_max_writes_per_second = 1000u;
    policy.local_max_retention_ms = 60000u;
    policy.remote_capabilities =
        TURBO_FLOW_STORAGE_CAP_STATE | TURBO_FLOW_STORAGE_CAP_INDEX |
        TURBO_FLOW_STORAGE_CAP_LOG;
    requirements.data_class = TURBO_FLOW_STORE_DATA_STATE;
    requirements.expected_records = 100u;
    requirements.expected_bytes = 4096u;
    requirements.max_item_bytes = 64u;
    requirements.writes_per_second = 10u;
    requirements.retention_ms = 1000u;
    check_int_eq(turbo_flow_store_route(&policy, &requirements, &decision), TURBO_OK);
    check_int_eq(decision.model, TURBO_FLOW_STORE_MODEL_STATE);
    check_int_eq(decision.placement, TURBO_FLOW_STORE_PLACEMENT_MEMORY);

    requirements.data_class = TURBO_FLOW_STORE_DATA_EVENT;
    requirements.writes_per_second = 1001u;
    check_int_eq(turbo_flow_store_route(&policy, &requirements, &decision), TURBO_OK);
    check_int_eq(decision.model, TURBO_FLOW_STORE_MODEL_LOG);
    check_int_eq(decision.placement, TURBO_FLOW_STORE_PLACEMENT_REDIS);

    requirements.data_class = TURBO_FLOW_STORE_DATA_METRIC;
    requirements.writes_per_second = 10u;
    requirements.durable = 1;
    check_int_eq(turbo_flow_store_route(&policy, &requirements, &decision), TURBO_ENOTSUP);
  }
}

spec("turbo_flow_state_store") {
  it("copies state and enforces revision conflicts") {
    turbo_flow_store_limits_t limits = test_limits(2u, 64u, TURBO_FLOW_STORE_FULL_REJECT);
    turbo_flow_state_store_t *store = NULL;
    turbo_flow_state_record_t record = TURBO_FLOW_STATE_RECORD_INIT;
    turbo_flow_store_stats_t stats = TURBO_FLOW_STORE_STATS_INIT;
    uint64_t revision = 0u;
    char mutable_value[] = "on";

    check_int_eq(turbo_flow_state_store_create_memory(&limits, &store), TURBO_OK);
    check_not_null(store);
    check_int_eq(turbo_flow_state_store_put(store, test_bytes("device:1"),
                                            test_bytes(mutable_value), 0u, &revision),
                 TURBO_OK);
    check_uint_eq(revision, 1u);
    mutable_value[0] = 'x';
    check_int_eq(turbo_flow_state_store_get(store, test_bytes("device:1"), &record), TURBO_OK);
    check_uint_eq(record.revision, 1u);
    check_mem_eq(mem_buffer_const_data(record.value), "on", 2u);
    turbo_flow_state_record_cleanup(&record);

    check_int_eq(
        turbo_flow_state_store_put(store, test_bytes("device:1"), test_bytes("off"), 9u, &revision),
        TURBO_EBUSY);
    check_int_eq(
        turbo_flow_state_store_put(store, test_bytes("device:1"), test_bytes("off"), 1u, &revision),
        TURBO_OK);
    check_uint_eq(revision, 2u);
    check_int_eq(turbo_flow_state_store_stats(store, &stats), TURBO_OK);
    check_size_eq(stats.records, 1u);
    check_size_eq(stats.bytes, strlen("device:1") + strlen("off"));
    check_uint_eq(stats.conflicts, 1u);
    check_int_eq(turbo_flow_state_store_close(store), TURBO_OK);
    check_int_eq(turbo_flow_state_store_get(store, test_bytes("device:1"), &record),
                 TURBO_ESHUTDOWN);
    check_int_eq(turbo_flow_state_store_stats(store, &stats), TURBO_ESHUTDOWN);
    check_int_eq(
        turbo_flow_state_store_put(store, test_bytes("device:2"), test_bytes("on"), 0u, &revision),
        TURBO_ESHUTDOWN);
    turbo_flow_state_store_destroy(store);
  }
}

spec("turbo_flow_index_store") {
  it("stores set membership and intersects named indices") {
    turbo_flow_store_limits_t limits = test_limits(8u, 128u, TURBO_FLOW_STORE_FULL_REJECT);
    turbo_flow_index_store_t *store = NULL;
    turbo_flow_store_bytes_t names[] = {test_bytes("online"), test_bytes("room:a")};
    size_t count = 0u;
    int present = 0;

    check_int_eq(turbo_flow_index_store_create_memory(&limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, names[0], test_bytes("device:1")), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, names[0], test_bytes("device:2")), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, names[1], test_bytes("device:2")), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, names[1], test_bytes("device:3")), TURBO_OK);
    check_int_eq(turbo_flow_index_store_add(store, names[0], test_bytes("device:1")),
                 TURBO_EALREADY);
    check_int_eq(turbo_flow_index_store_contains(store, names[0], test_bytes("device:2"), &present),
                 TURBO_OK);
    check_int_eq(present, 1);
    check_int_eq(turbo_flow_index_store_intersection_count(store, names, 2u, &count), TURBO_OK);
    check_size_eq(count, 1u);
    check_int_eq(turbo_flow_index_store_remove(store, names[0], test_bytes("device:2")), TURBO_OK);
    check_int_eq(turbo_flow_index_store_intersection_count(store, names, 2u, &count), TURBO_OK);
    check_size_eq(count, 0u);
    check_int_eq(turbo_flow_index_store_close(store), TURBO_OK);
    check_int_eq(turbo_flow_index_store_contains(store, names[0], test_bytes("device:1"),
                                                 &present),
                 TURBO_ESHUTDOWN);
    turbo_flow_index_store_destroy(store);
  }
}

spec("turbo_flow_log_store") {
  it("uses monotonic cursors and trims oldest records at the hard limit") {
    turbo_flow_store_limits_t limits = test_limits(2u, 16u, TURBO_FLOW_STORE_FULL_TRIM_OLDEST);
    turbo_flow_log_store_t *store = NULL;
    turbo_flow_log_record_t records[2] = {TURBO_FLOW_LOG_RECORD_INIT, TURBO_FLOW_LOG_RECORD_INIT};
    size_t count = 0u;
    uint64_t cursor = 0u;
    uint64_t head = 0u;
    uint64_t tail = 0u;

    check_int_eq(turbo_flow_log_store_create_memory(&limits, &store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 10u, test_bytes("a"), &cursor), TURBO_OK);
    check_uint_eq(cursor, 1u);
    check_int_eq(turbo_flow_log_store_append(store, 20u, test_bytes("bb"), &cursor), TURBO_OK);
    check_int_eq(turbo_flow_log_store_append(store, 30u, test_bytes("ccc"), &cursor), TURBO_OK);
    check_int_eq(turbo_flow_log_store_bounds(store, &head, &tail), TURBO_OK);
    check_uint_eq(head, 2u);
    check_uint_eq(tail, 3u);
    check_int_eq(turbo_flow_log_store_read(store, 1u, records, 2u, &count), TURBO_ERANGE);
    check_int_eq(turbo_flow_log_store_read(store, 0u, records, 2u, &count), TURBO_OK);
    check_size_eq(count, 2u);
    check_uint_eq(records[0].cursor, 2u);
    check_mem_eq(mem_buffer_const_data(records[1].payload), "ccc", 3u);
    turbo_flow_log_record_cleanup(&records[0]);
    turbo_flow_log_record_cleanup(&records[1]);
    check_int_eq(turbo_flow_log_store_append(store, 29u, test_bytes("late"), &cursor),
                 TURBO_ERANGE);
    check_int_eq(turbo_flow_log_store_trim_before_cursor(store, 4u, &count), TURBO_OK);
    check_size_eq(count, 2u);
    check_int_eq(turbo_flow_log_store_append(store, 29u, test_bytes("late"), &cursor),
                 TURBO_ERANGE);
    check_int_eq(turbo_flow_log_store_close(store), TURBO_OK);
    check_int_eq(turbo_flow_log_store_read(store, 0u, records, 2u, &count),
                 TURBO_ESHUTDOWN);
    turbo_flow_log_store_destroy(store);
  }
}

spec("turbo_flow_series_store") {
  it("keeps ordered typed samples and computes range aggregates") {
    turbo_flow_series_config_t config = TURBO_FLOW_SERIES_CONFIG_INIT;
    turbo_flow_series_store_t *store = NULL;
    turbo_flow_series_sample_t sample;
    turbo_flow_series_sample_t range[3];
    size_t count = 0u;
    double value = 0.0;

    config.limits = test_limits(4u, 256u, TURBO_FLOW_STORE_FULL_REJECT);
    config.duplicate_policy = TURBO_FLOW_SERIES_DUPLICATE_KEEP_LAST;
    check_int_eq(turbo_flow_series_store_create_memory(&config, &store), TURBO_OK);
    sample.value.kind = TURBO_FLOW_SERIES_INT64;
    sample.timestamp_ms = 100u;
    sample.value.as.integer = 2;
    check_int_eq(turbo_flow_series_store_append(store, test_bytes("temp"), &sample), TURBO_OK);
    sample.timestamp_ms = 200u;
    sample.value.as.integer = 4;
    check_int_eq(turbo_flow_series_store_append(store, test_bytes("temp"), &sample), TURBO_OK);
    sample.value.as.integer = 6;
    check_int_eq(turbo_flow_series_store_append(store, test_bytes("temp"), &sample), TURBO_OK);
    sample.timestamp_ms = 300u;
    sample.value.as.integer = 8;
    check_int_eq(turbo_flow_series_store_append(store, test_bytes("temp"), &sample), TURBO_OK);
    check_int_eq(
        turbo_flow_series_store_range(store, test_bytes("temp"), 150u, 300u, range, 3u, &count),
        TURBO_OK);
    check_size_eq(count, 2u);
    check_uint_eq(range[0].value.as.integer, 6u);
    check_int_eq(turbo_flow_series_store_aggregate(store, test_bytes("temp"), 100u, 300u,
                                                   TURBO_FLOW_SERIES_AGGREGATE_AVG, &value, &count),
                 TURBO_OK);
    check_size_eq(count, 3u);
    check_double_eq(value, 16.0 / 3.0, 0.000001);
    sample.timestamp_ms = 250u;
    check_int_eq(turbo_flow_series_store_append(store, test_bytes("temp"), &sample), TURBO_ERANGE);
    check_int_eq(turbo_flow_series_store_close(store), TURBO_OK);
    check_int_eq(
        turbo_flow_series_store_range(store, test_bytes("temp"), 100u, 300u, range, 3u, &count),
        TURBO_ESHUTDOWN);
    turbo_flow_series_store_destroy(store);
  }
}
