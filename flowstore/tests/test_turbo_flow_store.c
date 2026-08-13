#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_bitmap_index.h"
#include "turbo_flow_index_store.h"
#include "turbo_flow_index_store_provider.h"
#include "turbo_flow_log_store.h"
#include "turbo_flow_log_store_provider.h"
#include "turbo_flow_databind_store.h"
#include "turbo_flow_series_store.h"
#include "turbo_flow_series_store_provider.h"
#include "turbo_flow_state_store.h"
#include "turbo_flow_store_policy.h"

#include <stdio.h>
#include <string.h>

static turbo_flow_store_bytes_t test_bytes(const char *text) {
  turbo_flow_store_bytes_t bytes;
  bytes.data = (const uint8_t *)text;
  bytes.size = strlen(text);
  return bytes;
}

typedef struct binding_test_entry_s {
  uint8_t key[32];
  size_t key_size;
  uint8_t value[256];
  size_t value_size;
  uint64_t revision;
  int present;
} binding_test_entry_t;

typedef struct binding_test_backend_s {
  binding_test_entry_t entries[4];
  size_t scan_calls;
} binding_test_backend_t;

static int binding_test_scan(void *ctx, turbo_flow_record_visit_fn visit, void *visit_ctx) {
  binding_test_backend_t *backend = (binding_test_backend_t *)ctx;
  if (!backend || !visit) return TURBO_EINVAL;
  backend->scan_calls += 1u;
  for (size_t i = 0u; i < 4u; ++i) {
    turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
    binding_test_entry_t *entry = &backend->entries[i];
    int rc;
    if (!entry->present) continue;
    record.key = entry->key;
    record.key_size = entry->key_size;
    record.revision = entry->revision;
    record.value = entry->value;
    record.value_size = entry->value_size;
    rc = visit(visit_ctx, &record);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int binding_test_commit(void *ctx, const turbo_flow_record_mutation_t *mutations,
                                  size_t mutation_count) {
  binding_test_backend_t *backend = (binding_test_backend_t *)ctx;
  binding_test_entry_t *entry = NULL;
  if (!backend || !mutations || mutation_count != 1u) return TURBO_EINVAL;
  for (size_t i = 0u; i < 4u; ++i) {
    if (backend->entries[i].present && backend->entries[i].key_size == mutations[0].key_size &&
        memcmp(backend->entries[i].key, mutations[0].key, mutations[0].key_size) == 0) {
      entry = &backend->entries[i];
      break;
    }
  }
  if (!entry) {
    for (size_t i = 0u; i < 4u; ++i) {
      if (!backend->entries[i].present) {
        entry = &backend->entries[i];
        break;
      }
    }
  }
  if (!entry || mutations[0].expected_revision !=
                    (entry->present ? entry->revision : TURBO_FLOW_RECORD_REVISION_ABSENT))
    return TURBO_EBUSY;
  if (mutations[0].kind == TURBO_FLOW_RECORD_DELETE) {
    if (!entry->present) return TURBO_EBUSY;
    memset(entry, 0, sizeof(*entry));
    return TURBO_OK;
  }
  if (mutations[0].kind != TURBO_FLOW_RECORD_PUT || mutations[0].key_size > sizeof(entry->key) ||
      mutations[0].value_size > sizeof(entry->value))
    return TURBO_EINVAL;
  memcpy(entry->key, mutations[0].key, mutations[0].key_size);
  if (mutations[0].value_size != 0u)
    memcpy(entry->value, mutations[0].value, mutations[0].value_size);
  entry->key_size = mutations[0].key_size;
  entry->value_size = mutations[0].value_size;
  entry->revision = mutations[0].next_revision;
  entry->present = 1;
  return TURBO_OK;
}

static turbo_flow_record_store_t binding_test_store(binding_test_backend_t *backend) {
  turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
  store.capabilities = TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  store.max_key_size = 32u;
  store.max_value_size = 256u;
  store.max_batch_size = 1u;
  store.max_records = 4u;
  store.ctx = backend;
  store.scan = binding_test_scan;
  store.commit = binding_test_commit;
  return store;
}

static DataBindRecord *binding_test_record(DataBind *codec, uint64_t id, const char *status,
                                              uint32_t amount) {
  char json[256];
  DataBindRecord *record = NULL;
  DataBindError error = DATA_BIND_ERROR_INIT;
  int written = snprintf(json, sizeof(json), "{\"id\":%llu,\"status\":\"%s\",\"amount\":%u}",
                         (unsigned long long)id, status, (unsigned int)amount);
  if (written < 0 || (size_t)written >= sizeof(json)) return NULL;
  if (data_bind_record_from_json(codec, "Order", json, (size_t)written, &record, &error) !=
      DATA_BIND_OK)
    return NULL;
  return record;
}

typedef struct binding_test_capture_s {
  size_t count;
  uint64_t last_id;
} binding_test_capture_t;

static int binding_test_capture(void *ctx, const turbo_flow_databind_view_t *view) {
  binding_test_capture_t *capture = (binding_test_capture_t *)ctx;
  DataBindError error = DATA_BIND_ERROR_INIT;
  if (!capture || !view || !view->record) return TURBO_EINVAL;
  if (data_bind_record_get_u64(view->record, "id", &capture->last_id, &error) != DATA_BIND_OK)
    return TURBO_EPROTO;
  ++capture->count;
  return TURBO_OK;
}

spec("turbo_flow_store_record") {
  it("owns opaque binary Record values without a codec") {
    static const uint8_t key[] = {'r', 'a', 'w'};
    static const uint8_t value[] = {0u, 0xffu, 0x7fu};
    binding_test_backend_t backend = {0};
    turbo_flow_record_store_t backend_store = binding_test_store(&backend);
    turbo_flow_store_config_t config = TURBO_FLOW_STORE_CONFIG_INIT;
    turbo_flow_store_t *store = NULL;
    turbo_flow_store_record_t record = TURBO_FLOW_STORE_RECORD_INIT;

    config.backend = &backend_store;
    check_int_eq(turbo_flow_store_create(&config, &store), TURBO_OK);
    if (store) {
      check_int_eq(turbo_flow_store_put(store, key, sizeof(key), 0u, 1u, value, sizeof(value)),
                   TURBO_OK);
      check_int_eq(turbo_flow_store_get(store, key, sizeof(key), &record), TURBO_OK);
      check_uint_eq(record.revision, 1u);
      check_size_eq(record.value_size, sizeof(value));
      check_mem_eq(record.value, value, sizeof(value));
      turbo_flow_store_record_clear(&record);
      check_int_eq(turbo_flow_store_delete(store, key, sizeof(key), 1u), TURBO_OK);
      check_int_eq(turbo_flow_store_get(store, key, sizeof(key), &record), TURBO_ENOENT);
    }
    turbo_flow_store_record_clear(&record);
    turbo_flow_store_destroy(store);
  }
}

spec("turbo_flow_databind") {
  it("serializes DataBind records through RecordStore and returns an owned typed record") {
    static const char schema[] =
        "message Order { uint64 id; string status; uint32 amount; }";
    static const uint8_t key[] = {'o', 'r', 'd', 'e', 'r', '-', '1'};
    binding_test_backend_t backend = {0};
    turbo_flow_record_store_t store = binding_test_store(&backend);
    turbo_flow_store_config_t store_config = TURBO_FLOW_STORE_CONFIG_INIT;
    turbo_flow_store_t *flow_store = NULL;
    turbo_flow_databind_binding_config_t config = TURBO_FLOW_DATABIND_BINDING_CONFIG_INIT;
    turbo_flow_databind_binding_t *binding = NULL;
    DataBind *codec = NULL;
    DataBindRecord *source = NULL;
    DataBindRecord *decoded = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;
    uint64_t id = 0u;
    uint64_t revision = 0u;

    store_config.backend = &store;
    check_int_eq(turbo_flow_store_create(&store_config, &flow_store), TURBO_OK);
    config.schema_text = schema;
    config.schema_size = sizeof(schema) - 1u;
    config.type_name = "Order";
    check_int_eq(turbo_flow_databind_binding_create(&config, &binding), TURBO_OK);
    check_not_null(binding);
    check_int_eq(data_bind_create_from_text(schema, sizeof(schema) - 1u, &codec, &error),
                 DATA_BIND_OK);
    source = binding_test_record(codec, UINT64_C(9007199254740993), "open", 125u);
    check_not_null(source);
    if (flow_store && binding && source) {
      check_int_eq(turbo_flow_databind_put(flow_store, binding, key, sizeof(key),
                                                     TURBO_FLOW_RECORD_REVISION_ABSENT, 1u, source),
                   TURBO_OK);
      check_int_eq(turbo_flow_databind_get(flow_store, binding, key, sizeof(key), &decoded,
                                                     &revision),
                   TURBO_OK);
      check_uint_eq(revision, 1u);
      check_int_eq(data_bind_record_get_u64(decoded, "id", &id, &error), DATA_BIND_OK);
      check_hex64_eq(id, UINT64_C(9007199254740993));
      data_bind_record_free(decoded);
      decoded = NULL;
      check_int_eq(turbo_flow_store_delete(flow_store, key, sizeof(key), 1u),
                   TURBO_OK);
      check_int_eq(turbo_flow_databind_get(flow_store, binding, key, sizeof(key), &decoded,
                                                     &revision),
                   TURBO_ENOENT);
      check_null(decoded);
      check_uint_eq(revision, TURBO_FLOW_RECORD_REVISION_ABSENT);
    }
    data_bind_record_free(decoded);
    data_bind_record_free(source);
    data_bind_free(codec);
    turbo_flow_databind_binding_destroy(binding);
    turbo_flow_store_destroy(flow_store);
  }

  it("filters DataBind records with exact uint64 QueryVM comparisons") {
    static const char schema[] =
        "message Order { uint64 id; string status; uint32 amount; }";
    static const uint8_t first_key[] = {'o', 'r', 'd', 'e', 'r', '-', '1'};
    static const uint8_t second_key[] = {'o', 'r', 'd', 'e', 'r', '-', '2'};
    binding_test_backend_t backend = {0};
    turbo_flow_record_store_t store = binding_test_store(&backend);
    turbo_flow_store_config_t store_config = TURBO_FLOW_STORE_CONFIG_INIT;
    turbo_flow_store_t *flow_store = NULL;
    turbo_flow_databind_binding_config_t config = TURBO_FLOW_DATABIND_BINDING_CONFIG_INIT;
    turbo_flow_databind_binding_t *binding = NULL;
    turbo_flow_databind_operand_t operands[2] = {
        TURBO_FLOW_DATABIND_OPERAND_INIT,
        TURBO_FLOW_DATABIND_OPERAND_INIT};
    qvm_instruction_t instructions[] = {
        {QVM_OP_LOAD_PATH, 0u, 1u, 0u, 0u, 0u},
        {QVM_OP_LOAD_CONST, 0u, 2u, 0u, 1u, 0u},
        {QVM_OP_CMP, 0u, 0u, TURBO_FLOW_DATABIND_COMPARE_GE, 1u, 2u}};
    turbo_flow_databind_query_t query = TURBO_FLOW_DATABIND_QUERY_INIT;
    binding_test_capture_t capture = {0};
    DataBind *codec = NULL;
    DataBindRecord *first = NULL;
    DataBindRecord *second = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;
    size_t matched = 0u;

    store_config.backend = &store;
    check_int_eq(turbo_flow_store_create(&store_config, &flow_store), TURBO_OK);
    config.schema_text = schema;
    config.schema_size = sizeof(schema) - 1u;
    config.type_name = "Order";
    check_int_eq(turbo_flow_databind_binding_create(&config, &binding), TURBO_OK);
    check_int_eq(data_bind_create_from_text(schema, sizeof(schema) - 1u, &codec, &error),
                 DATA_BIND_OK);
    first = binding_test_record(codec, UINT64_C(9007199254740993), "open", 125u);
    second = binding_test_record(codec, UINT64_C(9007199254740994), "closed", 50u);
    check_not_null(first);
    check_not_null(second);
    if (flow_store && binding && first && second) {
      check_int_eq(turbo_flow_databind_put(flow_store, binding, first_key, sizeof(first_key), 0u,
                                                     1u, first),
                   TURBO_OK);
      check_int_eq(turbo_flow_databind_put(flow_store, binding, second_key, sizeof(second_key),
                                                     0u, 1u, second),
                   TURBO_OK);
      operands[0].kind = TURBO_FLOW_DATABIND_OPERAND_FIELD;
      operands[0].value.field_name = "id";
      operands[1].kind = TURBO_FLOW_DATABIND_OPERAND_UINT64;
      operands[1].value.uinteger = UINT64_C(9007199254740994);
      query.instructions = instructions;
      query.instruction_count = sizeof(instructions) / sizeof(instructions[0]);
      query.length = query.instruction_count;
      query.operands = operands;
      query.operand_count = sizeof(operands) / sizeof(operands[0]);
      check_int_eq(turbo_flow_databind_query(flow_store, binding, &query, binding_test_capture,
                                                       &capture, &matched),
                   TURBO_OK);
      check_size_eq(matched, 1u);
      check_size_eq(capture.count, 1u);
      check_hex64_eq(capture.last_id, UINT64_C(9007199254740994));
    }
    data_bind_record_free(second);
    data_bind_record_free(first);
    data_bind_free(codec);
    turbo_flow_databind_binding_destroy(binding);
    turbo_flow_store_destroy(flow_store);
  }

  it("round-trips an empty DataBind binary message") {
    static const char schema[] = "message Empty {}";
    static const uint8_t key[] = {'e', 'm', 'p', 't', 'y'};
    binding_test_backend_t backend = {0};
    turbo_flow_record_store_t store = binding_test_store(&backend);
    turbo_flow_store_config_t store_config = TURBO_FLOW_STORE_CONFIG_INIT;
    turbo_flow_store_t *flow_store = NULL;
    turbo_flow_databind_binding_config_t config = TURBO_FLOW_DATABIND_BINDING_CONFIG_INIT;
    turbo_flow_databind_binding_t *binding = NULL;
    DataBind *codec = NULL;
    DataBindRecord *source = NULL;
    DataBindRecord *decoded = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;
    uint64_t revision = 0u;

    store_config.backend = &store;
    check_int_eq(turbo_flow_store_create(&store_config, &flow_store), TURBO_OK);
    config.schema_text = schema;
    config.schema_size = sizeof(schema) - 1u;
    config.type_name = "Empty";
    check_int_eq(turbo_flow_databind_binding_create(&config, &binding), TURBO_OK);
    check_int_eq(data_bind_create_from_text(schema, sizeof(schema) - 1u, &codec, &error),
                 DATA_BIND_OK);
    if (codec)
      check_int_eq(data_bind_record_from_json(codec, "Empty", "{}", 2u, &source, &error),
                   DATA_BIND_OK);
    if (flow_store && binding && source) {
      check_int_eq(turbo_flow_databind_put(flow_store, binding, key, sizeof(key), 0u, 1u, source),
                   TURBO_OK);
      check_int_eq(turbo_flow_databind_get(flow_store, binding, key, sizeof(key), &decoded,
                                                     &revision),
                   TURBO_OK);
      check_uint_eq(revision, 1u);
      check_str_eq(data_bind_record_type_name(decoded), "Empty");
    }
    data_bind_record_free(decoded);
    data_bind_record_free(source);
    data_bind_free(codec);
    turbo_flow_databind_binding_destroy(binding);
    turbo_flow_store_destroy(flow_store);
  }

  it("rejects unsupported verified QueryVM opcodes before scanning") {
    static const char schema[] = "message Order { uint64 id; }";
    binding_test_backend_t backend = {0};
    turbo_flow_record_store_t store = binding_test_store(&backend);
    turbo_flow_store_config_t store_config = TURBO_FLOW_STORE_CONFIG_INIT;
    turbo_flow_store_t *flow_store = NULL;
    turbo_flow_databind_binding_config_t config = TURBO_FLOW_DATABIND_BINDING_CONFIG_INIT;
    turbo_flow_databind_binding_t *binding = NULL;
    turbo_flow_databind_operand_t operands[2] = {
        TURBO_FLOW_DATABIND_OPERAND_INIT,
        TURBO_FLOW_DATABIND_OPERAND_INIT};
    qvm_instruction_t instructions[] = {
        {QVM_OP_LOAD_CONST, 0u, 1u, 0u, 0u, 0u},
        {QVM_OP_LOAD_CONST, 0u, 2u, 0u, 1u, 0u},
        {QVM_OP_ADD, 0u, 0u, 0u, 1u, 2u}};
    turbo_flow_databind_query_t query = TURBO_FLOW_DATABIND_QUERY_INIT;
    binding_test_capture_t capture = {0};

    store_config.backend = &store;
    check_int_eq(turbo_flow_store_create(&store_config, &flow_store), TURBO_OK);
    config.schema_text = schema;
    config.schema_size = sizeof(schema) - 1u;
    config.type_name = "Order";
    check_int_eq(turbo_flow_databind_binding_create(&config, &binding), TURBO_OK);
    operands[0].kind = TURBO_FLOW_DATABIND_OPERAND_UINT64;
    operands[0].value.uinteger = 1u;
    operands[1].kind = TURBO_FLOW_DATABIND_OPERAND_UINT64;
    operands[1].value.uinteger = 2u;
    query.instructions = instructions;
    query.instruction_count = sizeof(instructions) / sizeof(instructions[0]);
    query.length = query.instruction_count;
    query.operands = operands;
    query.operand_count = sizeof(operands) / sizeof(operands[0]);
    if (flow_store && binding) {
      check_int_eq(turbo_flow_databind_query(flow_store, binding, &query, binding_test_capture,
                                                       &capture, NULL),
                   TURBO_ENOTSUP);
      check_size_eq(backend.scan_calls, 0u);
    }
    turbo_flow_databind_binding_destroy(binding);
    turbo_flow_store_destroy(flow_store);
  }

  it("requires atomic batch commits at FlowStore creation") {
    static const char schema[] = "message Order { uint64 id; }";
    binding_test_backend_t backend = {0};
    turbo_flow_record_store_t store = binding_test_store(&backend);
    turbo_flow_store_config_t config = TURBO_FLOW_STORE_CONFIG_INIT;
    turbo_flow_store_t *flow_store = NULL;

    store.capabilities = 0u;
    config.backend = &store;
    check_int_eq(turbo_flow_store_create(&config, &flow_store), TURBO_EINVAL);
    check_null(flow_store);
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
