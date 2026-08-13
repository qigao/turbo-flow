#include "record_store_contract.h"
#include "tinytest.h"
#include "turbo_flow_config.h"
#include "turbo_flow_sqlite_storage_backend.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct sqlite_record_capture_s {
  const uint8_t *key;
  size_t key_size;
  const uint8_t *value;
  size_t value_size;
  uint64_t revision;
  size_t count;
} sqlite_record_capture_t;

typedef struct sqlite_snapshot_writer_s {
  turbo_flow_record_store_t *writer;
  size_t count;
  int write_status;
} sqlite_snapshot_writer_t;

static turbo_flow_sqlite_record_store_config_t sqlite_test_config(const char *path,
                                                                  const char *namespace_name) {
  turbo_flow_sqlite_record_store_config_t config = TURBO_FLOW_SQLITE_RECORD_STORE_CONFIG_INIT;
  config.database_path = path;
  config.namespace_name = namespace_name;
  config.busy_timeout_ms = 1000;
  config.max_records = 8u;
  config.max_bytes = 1024u;
  config.max_item_bytes = 128u;
  config.max_key_size = 32u;
  config.max_value_size = 96u;
  config.max_batch_size = 4u;
  return config;
}

static int sqlite_record_count(void *ctx, const turbo_flow_record_view_t *record) {
  size_t *count = (size_t *)ctx;
  if (!count || !record) return TURBO_EINVAL;
  ++*count;
  return TURBO_OK;
}

static int sqlite_record_capture(void *ctx, const turbo_flow_record_view_t *record) {
  sqlite_record_capture_t *capture = (sqlite_record_capture_t *)ctx;
  if (!capture || !record || record->key_size != capture->key_size ||
      memcmp(record->key, capture->key, capture->key_size) != 0 ||
      record->value_size != capture->value_size ||
      (capture->value_size != 0u &&
       memcmp(record->value, capture->value, capture->value_size) != 0) ||
      record->revision != capture->revision)
    return TURBO_EPROTO;
  ++capture->count;
  return TURBO_OK;
}

static int sqlite_snapshot_visit(void *ctx, const turbo_flow_record_view_t *record) {
  static const uint8_t key[] = {'b', 0u, 2u};
  static const uint8_t value[] = {'n', 0u, 'w'};
  sqlite_snapshot_writer_t *snapshot = (sqlite_snapshot_writer_t *)ctx;
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  if (!snapshot || !snapshot->writer || !record) return TURBO_EINVAL;
  ++snapshot->count;
  if (snapshot->count != 1u) return TURBO_OK;
  mutation.key = key;
  mutation.key_size = sizeof(key);
  mutation.next_revision = 1u;
  mutation.value = value;
  mutation.value_size = sizeof(value);
  snapshot->write_status = snapshot->writer->commit(snapshot->writer->ctx, &mutation, 1u);
  return snapshot->write_status;
}

static int sqlite_test_put(turbo_flow_record_store_t *store, const uint8_t *key, size_t key_size,
                           const uint8_t *value, size_t value_size) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.next_revision = 1u;
  mutation.value = value;
  mutation.value_size = value_size;
  return store->commit(store->ctx, &mutation, 1u);
}

spec("turbo_flow_sqlite_storage_backend") {
  it("runs the durable binary RecordStore contract through direct backend options") {
    char *path = tt_make_temp_file("flow-record-sqlite", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(path, "contract");
    turbo_flow_sqlite_storage_backend_options_t options =
        TURBO_FLOW_SQLITE_STORAGE_BACKEND_OPTIONS_INIT;
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_record_store_t *store = NULL;
    turbo_flow_test_record_store_contract_result_t result;

    check_not_null(path);
    options.config = &config;
    request.options = &options;
    request.options_size = sizeof(options);
    check_str_eq(turbo_flow_sqlite_storage_backend_api()->backend, "sqlite");
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(
                     registry, turbo_flow_sqlite_storage_backend_api()),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(registry, "sqlite", &request,
                                                                    &owner, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_service(owner, TURBO_FLOW_STORAGE_MODEL_RECORD,
                                                          (void **)&store),
                 TURBO_OK);
    check_true((store->capabilities & TURBO_FLOW_RECORD_STORE_DURABLE) != 0u);
    check_true((store->capabilities & TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH) != 0u);
    check_int_eq(turbo_flow_test_record_store_contract_run(store, &result), TURBO_OK);
    check_true(result.binary_wire_equal);
    turbo_flow_storage_backend_owner_destroy(owner);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("keeps process-local records only for the lifetime of one memory store owner") {
    static const uint8_t key[] = {'m', 'e', 'm'};
    static const uint8_t value[] = {'v', 'a', 'l', 'u', 'e'};
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(":memory:", "protocol");
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    sqlite_record_capture_t capture = {key, sizeof(key), value, sizeof(value), 1u, 0u};
    size_t count = 0u;

    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    check_true((store.capabilities & TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH) != 0u);
    check_true((store.capabilities & TURBO_FLOW_RECORD_STORE_DURABLE) == 0u);
    check_int_eq(sqlite_test_put(&store, key, sizeof(key), value, sizeof(value)), TURBO_OK);
    check_int_eq(store.scan(store.ctx, sqlite_record_capture, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);

    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.scan(store.ctx, sqlite_record_count, &count), TURBO_OK);
    check_size_eq(count, 0u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);
  }

  it("persists a binary record across explicit close and reopen") {
    static const uint8_t key[] = {0u, 'k', 0xffu};
    static const uint8_t value[] = {'v', 0u, 7u, 0xfeu};
    char *path = tt_make_temp_file("flow-record-sqlite-reopen", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(path, "persistent");
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    sqlite_record_capture_t capture = {key, sizeof(key), value, sizeof(value), 1u, 0u};

    check_not_null(path);
    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    check_int_eq(sqlite_test_put(&store, key, sizeof(key), value, sizeof(value)), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);
    check_null(store.ctx);
    check_null(store.scan);
    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    check_int_eq(store.scan(store.ctx, sqlite_record_capture, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_EINVAL);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("round trips an empty value as a non-null binary record") {
    static const uint8_t key[] = {'e', 'm', 'p', 't', 'y'};
    char *path = tt_make_temp_file("flow-record-sqlite-empty", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(path, "empty");
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    sqlite_record_capture_t capture = {key, sizeof(key), NULL, 0u, 1u, 0u};

    check_not_null(path);
    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    check_int_eq(sqlite_test_put(&store, key, sizeof(key), NULL, 0u), TURBO_OK);
    check_int_eq(store.scan(store.ctx, sqlite_record_capture, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("rejects a corrupt database instead of replacing it with an empty store") {
    static const char invalid_database[] = "not-a-sqlite-database";
    char *path = tt_make_temp_file("flow-record-sqlite-corrupt", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(path, "corrupt");
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;

    check_not_null(path);
    check_int_eq(tt_write_file(path, invalid_database, sizeof(invalid_database) - 1u), 0);
    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_EIO);
    check_null(store.ctx);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("rolls back every mutation when one CAS precondition is stale") {
    static const uint8_t key_a[] = {'a'};
    static const uint8_t key_b[] = {'b'};
    static const uint8_t value_one[] = {'1'};
    static const uint8_t value_two[] = {'2'};
    char *path = tt_make_temp_file("flow-record-sqlite-cas", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(path, "cas");
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_mutation_t mutations[2] = {TURBO_FLOW_RECORD_MUTATION_INIT,
                                                 TURBO_FLOW_RECORD_MUTATION_INIT};
    sqlite_record_capture_t capture = {key_a, sizeof(key_a), value_one, sizeof(value_one), 1u, 0u};

    check_not_null(path);
    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    check_int_eq(sqlite_test_put(&store, key_a, sizeof(key_a), value_one, sizeof(value_one)),
                 TURBO_OK);
    mutations[0].key = key_a;
    mutations[0].key_size = sizeof(key_a);
    mutations[0].expected_revision = 1u;
    mutations[0].next_revision = 2u;
    mutations[0].value = value_two;
    mutations[0].value_size = sizeof(value_two);
    mutations[1].kind = TURBO_FLOW_RECORD_DELETE;
    mutations[1].key = key_b;
    mutations[1].key_size = sizeof(key_b);
    mutations[1].expected_revision = 1u;
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_EBUSY);
    check_int_eq(store.scan(store.ctx, sqlite_record_capture, &capture), TURBO_OK);
    check_size_eq(capture.count, 1u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("rolls back a batch that exceeds record, byte, item, or API bounds") {
    static const uint8_t key_a[] = {'a'};
    static const uint8_t key_b[] = {'b'};
    static const uint8_t key_long[] = {'l', 'o', 'n', 'g'};
    static const uint8_t value[] = {'v'};
    static const uint8_t value_long[] = {'v', 'a', 'l', 'u', 'e'};
    char *path = tt_make_temp_file("flow-record-sqlite-limits", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t config = sqlite_test_config(path, "limits");
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_mutation_t mutations[3] = {TURBO_FLOW_RECORD_MUTATION_INIT,
                                                 TURBO_FLOW_RECORD_MUTATION_INIT,
                                                 TURBO_FLOW_RECORD_MUTATION_INIT};
    size_t count = 0u;

    check_not_null(path);
    config.max_records = 1u;
    config.max_bytes = 3u;
    config.max_item_bytes = 2u;
    config.max_key_size = 2u;
    config.max_value_size = 2u;
    config.max_batch_size = 2u;
    check_int_eq(turbo_flow_sqlite_record_store_create(&config, &store), TURBO_OK);
    for (size_t i = 0u; i < 2u; ++i) {
      mutations[i].key = i == 0u ? key_a : key_b;
      mutations[i].key_size = 1u;
      mutations[i].next_revision = 1u;
      mutations[i].value = value;
      mutations[i].value_size = sizeof(value);
    }
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_ENOSPC);
    check_int_eq(store.scan(store.ctx, sqlite_record_count, &count), TURBO_OK);
    check_size_eq(count, 0u);
    mutations[0].key = key_long;
    mutations[0].key_size = sizeof(key_long);
    check_int_eq(store.commit(store.ctx, mutations, 1u), TURBO_EINVAL);
    mutations[0].key = key_a;
    mutations[0].key_size = sizeof(key_a);
    mutations[0].value = value_long;
    mutations[0].value_size = sizeof(value_long);
    check_int_eq(store.commit(store.ctx, mutations, 1u), TURBO_EINVAL);
    mutations[0].value = value;
    mutations[0].value_size = sizeof(value);
    mutations[2] = mutations[0];
    mutations[2].key = key_b;
    check_int_eq(store.commit(store.ctx, mutations, 3u), TURBO_EINVAL);
    mutations[1] = mutations[0];
    check_int_eq(store.commit(store.ctx, mutations, 2u), TURBO_EINVAL);
    check_int_eq(turbo_flow_sqlite_record_store_close(&store), TURBO_OK);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("keeps scan on a stable snapshot while another store commits") {
    static const uint8_t key[] = {'a', 0u, 1u};
    static const uint8_t value[] = {'o', 0u, 'd'};
    char *path = tt_make_temp_file("flow-record-sqlite-snapshot", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t first_config = sqlite_test_config(path, "snapshot");
    turbo_flow_sqlite_record_store_config_t second_config = first_config;
    turbo_flow_record_store_t first = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_store_t second = TURBO_FLOW_RECORD_STORE_INIT;
    sqlite_snapshot_writer_t snapshot = {&second, 0u, TURBO_EIO};
    size_t final_count = 0u;

    check_not_null(path);
    check_int_eq(turbo_flow_sqlite_record_store_create(&first_config, &first), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_create(&second_config, &second), TURBO_OK);
    check_int_eq(sqlite_test_put(&first, key, sizeof(key), value, sizeof(value)), TURBO_OK);
    check_int_eq(first.scan(first.ctx, sqlite_snapshot_visit, &snapshot), TURBO_OK);
    check_int_eq(snapshot.write_status, TURBO_OK);
    check_size_eq(snapshot.count, 1u);
    check_int_eq(first.scan(first.ctx, sqlite_record_count, &final_count), TURBO_OK);
    check_size_eq(final_count, 2u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&second), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_close(&first), TURBO_OK);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("isolates protocol and business namespaces in one physical database") {
    static const uint8_t key[] = {'s', 'a', 'm', 'e'};
    static const uint8_t protocol_value[] = {'m', 'q', 't', 't'};
    static const uint8_t business_value[] = {'b', 'i', 'z'};
    char *path = tt_make_temp_file("flow-record-sqlite-namespaces", ".sqlite3");
    turbo_flow_sqlite_record_store_config_t protocol_config = sqlite_test_config(path, "protocol");
    turbo_flow_sqlite_record_store_config_t business_config = sqlite_test_config(path, "business");
    turbo_flow_record_store_t protocol_store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_record_store_t business_store = TURBO_FLOW_RECORD_STORE_INIT;
    sqlite_record_capture_t protocol_capture = {
        key, sizeof(key), protocol_value, sizeof(protocol_value), 1u, 0u};
    sqlite_record_capture_t business_capture = {
        key, sizeof(key), business_value, sizeof(business_value), 1u, 0u};

    check_not_null(path);
    check_int_eq(turbo_flow_sqlite_record_store_create(&protocol_config, &protocol_store),
                 TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_create(&business_config, &business_store),
                 TURBO_OK);
    check_int_eq(
        sqlite_test_put(&protocol_store, key, sizeof(key), protocol_value, sizeof(protocol_value)),
        TURBO_OK);
    check_int_eq(
        sqlite_test_put(&business_store, key, sizeof(key), business_value, sizeof(business_value)),
        TURBO_OK);
    check_int_eq(protocol_store.scan(protocol_store.ctx, sqlite_record_capture, &protocol_capture),
                 TURBO_OK);
    check_int_eq(business_store.scan(business_store.ctx, sqlite_record_capture, &business_capture),
                 TURBO_OK);
    check_size_eq(protocol_capture.count, 1u);
    check_size_eq(business_capture.count, 1u);
    check_int_eq(turbo_flow_sqlite_record_store_close(&business_store), TURBO_OK);
    check_int_eq(turbo_flow_sqlite_record_store_close(&protocol_store), TURBO_OK);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("opens a strict resolved YAML channel through the common owner ABI") {
    char *path = tt_make_temp_file("flow-record-sqlite-yaml", ".sqlite3");
    char yaml[2048];
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
    turbo_flow_record_store_t *store = NULL;
    turbo_flow_test_record_store_contract_result_t result;
    int yaml_size;

    check_not_null(path);
    yaml_size =
        snprintf(yaml, sizeof(yaml),
                 "version: 1\nchannels:\n  mqtt.sessions:\n    kind: record_store\n    config:\n"
                 "      backend: sqlite\n      database_path: '%s'\n"
                 "      namespace_name: mqtt.sessions\n      busy_timeout_ms: 1000\n"
                 "      max_records: 8\n      max_bytes: 1024\n      max_item_bytes: 128\n"
                 "      max_key_size: 32\n      max_value_size: 96\n      max_batch_size: 4\n"
                 "adapters: {}\n",
                 path);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(
                     registry, turbo_flow_sqlite_storage_backend_api()),
                 TURBO_OK);
    request.resolved = resolved;
    request.channel_name = "mqtt.sessions";
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(registry, "sqlite", &request,
                                                                    &owner, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_service(owner, TURBO_FLOW_STORAGE_MODEL_RECORD,
                                                          (void **)&store),
                 TURBO_OK);
    check_int_eq(turbo_flow_test_record_store_contract_run(store, &result), TURBO_OK);
    turbo_flow_storage_backend_owner_destroy(owner);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }
}
