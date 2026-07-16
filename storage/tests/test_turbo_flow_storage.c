#include "turbo_flow_storage.h"

#include "tinytest.h"
#include "turbo_fs.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include "sqlite3.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
  #include <direct.h>
#else
  #include <sys/stat.h>
  #include <unistd.h>
#endif

typedef struct storage_capture_ctx_s {
  atomic_int count;
  tstr_t payload;
} storage_capture_ctx_t;

static int storage_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  storage_capture_ctx_t *capture = (storage_capture_ctx_t *)ctx;

  if (!capture || !msg) return TURBO_EINVAL;
  tstr_freep(&capture->payload);
  capture->payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  if (!capture->payload) return TURBO_ENOMEM;
  atomic_fetch_add_explicit(&capture->count, 1, memory_order_release);
  return TURBO_OK;
}

static void storage_make_path(char *out, size_t out_size, const char *suffix) {
  static atomic_int seq = 0;
  char tmpdir[TURBO_FS_MAX_PATH];
  char name[128];
  int id;

  check_int_eq(turbo_fs_get_tmpdir(tmpdir, sizeof(tmpdir)), 0);
  id = atomic_fetch_add_explicit(&seq, 1, memory_order_relaxed);
  snprintf(name, sizeof(name), "turbo_flow_storage_%d_%d_%s", turbo_getpid(), id, suffix);
  check_int_eq(turbo_fs_path_join(out, out_size, tmpdir, name), 0);
  if (turbo_fs_access(out, TURBO_FS_ACCESS_EXISTS) == 0) {
    (void)turbo_fs_unlink(out);
  }
}

static void storage_remove_if_exists(const char *path) {
  if (path && turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) == 0) {
    (void)turbo_fs_unlink(path);
  }
}

static void storage_make_dir(const char *path) {
#ifdef _WIN32
  check_int_eq(_mkdir(path), 0);
#else
  check_int_eq(mkdir(path, 0700), 0);
#endif
}

static void storage_remove_dir_if_exists(const char *path) {
  if (!path || turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) != 0) return;
#ifdef _WIN32
  (void)_rmdir(path);
#else
  (void)rmdir(path);
#endif
}

static void storage_write_test_file(const char *path, const char *data, size_t len) {
  turbo_fs_buf_t buf = turbo_fs_buf_init((char *)(data ? data : ""), len);

  check_int_eq(turbo_fs_write_file(path, &buf), 0);
}

static void storage_check_file_bytes(const char *path, const char *data, size_t len) {
  turbo_fs_buf_t buf;

  memset(&buf, 0, sizeof(buf));
  check_int_eq(turbo_fs_read_file(path, &buf), 0);
  check_size_eq(buf.len, len);
  check_mem_eq(buf.base, data, len);
  turbo_fs_buf_free(&buf);
}

static void storage_publish_payload(turbo_flow_t *flow, const char *source_name, const char *data,
                                    size_t len, int expected_rc) {
  turbo_flow_msg_t msg;
  mem_buffer_t *buffer = mem_wrap_external((void *)data, len, NULL, NULL);

  check_not_null(buffer);
  turbo_flow_msg_init(&msg);
  msg.buffer = buffer;
  msg.payload = tstr_v_from_buf(data, len);
  check_int_eq(turbo_flow_publish(flow, source_name, &msg), expected_rc);
  turbo_flow_msg_cleanup(&msg);
}

static void storage_sqlite_exec(const char *path, const char *sql) {
  sqlite3 *db = NULL;
  char *error = NULL;

  check_int_eq(sqlite3_open(path, &db), SQLITE_OK);
  check_not_null(db);
  check_int_eq(sqlite3_exec(db, sql, NULL, NULL, &error), SQLITE_OK);
  sqlite3_free(error);
  check_int_eq(sqlite3_close(db), SQLITE_OK);
}

static int storage_sqlite_count_payloads(const char *path) {
  sqlite3 *db = NULL;
  sqlite3_stmt *stmt = NULL;
  int count = -1;

  check_int_eq(sqlite3_open(path, &db), SQLITE_OK);
  check_not_null(db);
  check_int_eq(sqlite3_prepare_v2(db, "select count(*) from events", -1, &stmt, NULL), SQLITE_OK);
  check_int_eq(sqlite3_step(stmt), SQLITE_ROW);
  count = sqlite3_column_int(stmt, 0);
  check_int_eq(sqlite3_finalize(stmt), SQLITE_OK);
  check_int_eq(sqlite3_close(db), SQLITE_OK);
  return count;
}

spec("turbo_flow_storage") {
  it("rejects negative SQLite busy timeouts") {
    turbo_flow_storage_sqlite_sink_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:sqlite-invalid";
    config.owner_name = "sqlite.invalid";
    config.path = "unused.db";
    config.statement = "insert into events(payload) values (?)";
    config.busy_timeout_ms = -1;
    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_sqlite_sink_adapter(flow, "sqlite.invalid", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("rejects storage adapters without explicit stable resource identity") {
    turbo_flow_storage_file_sink_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    config.path = "unused.dat";
    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_sink_adapter(flow, "file.write", &config),
                 TURBO_EINVAL);
    config.resource_uid = "storage:file-write";
    check_int_eq(turbo_flow_storage_register_file_sink_adapter(flow, "file.write", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("writes binary payloads through a file sink") {
    static const char *src = "source input\n"
                             "stage archive adapter \"file.write\"\n"
                             "stage main {\n"
                             "  input -> archive\n"
                             "}\n";
    const char payload[] = {'A', '\0', 'B'};
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_storage_file_sink_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    storage_make_path(path, sizeof(path), "binary.dat");
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:file-write";
    config.owner_name = "file.write";
    config.path = path;
    config.max_payload_size = sizeof(payload);
    config.encoding = TURBO_FLOW_STORAGE_ENCODING_BINARY;
    config.write_mode = TURBO_FLOW_STORAGE_WRITE_TRUNCATE;

    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_sink_adapter(flow, "file.write", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    storage_publish_payload(flow, "input", payload, sizeof(payload), TURBO_OK);
    {
      turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
      turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
      turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
      check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &metadata), TURBO_OK);
      check_int_eq(metadata.domain, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      check_int_eq(metadata.kind, TURBO_FLOW_RESOURCE_STORAGE);
      check_str_eq(metadata.uid, config.resource_uid);
      check_str_eq(metadata.owner_name, config.owner_name);
      check_int_eq(turbo_flow_resource_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
      check_int_eq(snapshot.last_status, TURBO_OK);
      check_int_eq(turbo_flow_resource_document_at(
                       flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
                   TURBO_OK);
      check_str_eq(document.schema->type_name, "StorageStatus");
      check_true(mem_buffer_used(document.payload) > 0u);
      {
        tstr_t status_json = tstr_new_len(mem_buffer_const_data(document.payload),
                                         mem_buffer_used(document.payload));
        check_not_null(status_json);
        check_null(strstr(status_json, path));
        check_null(strstr(status_json, "payload"));
        check_not_null(strstr(status_json, "\"operations\":\"1\""));
        check_not_null(strstr(status_json, "\"bytes\":\"3\""));
        tstr_free(status_json);
      }
      turbo_flow_resource_document_cleanup(&document);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);

    storage_check_file_bytes(path, payload, sizeof(payload));
    turbo_flow_destroy(flow);
    storage_remove_if_exists(path);
  }

  it("appends file sink payloads when configured for append mode") {
    static const char *src = "source input\n"
                             "stage archive adapter \"file.write\"\n"
                             "stage main {\n"
                             "  input -> archive\n"
                             "}\n";
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_storage_file_sink_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    storage_make_path(path, sizeof(path), "append.dat");
    storage_write_test_file(path, "old", 3u);
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:file-append";
    config.owner_name = "file.write";
    config.path = path;
    config.write_mode = TURBO_FLOW_STORAGE_WRITE_APPEND;

    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_sink_adapter(flow, "file.write", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    storage_publish_payload(flow, "input", "new", 3u, TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);

    storage_check_file_bytes(path, "oldnew", 6u);
    turbo_flow_destroy(flow);
    storage_remove_if_exists(path);
  }

  it("rejects oversized sink payloads before writing") {
    static const char *src = "source input\n"
                             "stage archive adapter \"file.write\"\n"
                             "stage main {\n"
                             "  input -> archive\n"
                             "}\n";
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_storage_file_sink_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    storage_make_path(path, sizeof(path), "oversized-sink.dat");
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:file-limit";
    config.owner_name = "file.write";
    config.path = path;
    config.max_payload_size = 2u;

    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_sink_adapter(flow, "file.write", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    storage_publish_payload(flow, "input", "abc", 3u, TURBO_EFBIG);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    check_true(turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) != 0);

    turbo_flow_destroy(flow);
    storage_remove_if_exists(path);
  }

  it("appends line-delimited records through an append-log sink") {
    static const char *src = "source input\n"
                             "stage log adapter \"file.append_log\"\n"
                             "stage main {\n"
                             "  input -> log\n"
                             "}\n";
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_storage_append_log_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    storage_make_path(path, sizeof(path), "records.log");
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:append-log";
    config.owner_name = "file.append_log";
    config.path = path;
    config.record_mode = TURBO_FLOW_STORAGE_RECORD_LINE;

    check_not_null(flow);
    check_int_eq(
        turbo_flow_storage_register_append_log_sink_adapter(flow, "file.append_log", &config),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    storage_publish_payload(flow, "input", "one", 3u, TURBO_OK);
    storage_publish_payload(flow, "input", "two", 3u, TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);

    storage_check_file_bytes(path, "one\ntwo\n", 8u);
    turbo_flow_destroy(flow);
    storage_remove_if_exists(path);
  }

  it("publishes an empty file as one source message") {
    static const char *src = "source file_in adapter \"file.read\"\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  file_in -> capture\n"
                             "}\n";
    char path[TURBO_FS_MAX_PATH];
    storage_capture_ctx_t capture;
    turbo_flow_storage_file_source_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    storage_make_path(path, sizeof(path), "empty.dat");
    storage_write_test_file(path, "", 0u);
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:file-read";
    config.owner_name = "file.read";
    config.path = path;
    config.max_payload_size = 1u;

    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_source_adapter(flow, "file.read", &config),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(flow, "capture", storage_capture_stage, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    for (int i = 0; i < 1000 && atomic_load_explicit(&capture.count, memory_order_acquire) == 0;
         ++i) {
      turbo_sleep_ms(1);
    }
    check_int_eq(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    check_not_null(capture.payload);
    check_size_eq(tstr_len(capture.payload), 0u);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);

    tstr_freep(&capture.payload);
    turbo_flow_destroy(flow);
    storage_remove_if_exists(path);
  }

  it("rejects missing and oversized source files during start") {
    static const char *src = "source file_in adapter \"file.read\"\n"
                             "stage main {\n"
                             "}\n";
    char missing_path[TURBO_FS_MAX_PATH];
    char large_path[TURBO_FS_MAX_PATH];
    turbo_flow_storage_file_source_config_t config;
    turbo_flow_t *flow = NULL;

    storage_make_path(missing_path, sizeof(missing_path), "missing.dat");
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:file-missing";
    config.owner_name = "file.read";
    config.path = missing_path;

    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_source_adapter(flow, "file.read", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_ENOENT);
    turbo_flow_destroy(flow);

    storage_make_path(large_path, sizeof(large_path), "large.dat");
    storage_write_test_file(large_path, "abcd", 4u);
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:file-large";
    config.owner_name = "file.read";
    config.path = large_path;
    config.max_payload_size = 3u;

    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_file_source_adapter(flow, "file.read", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_EFBIG);
    turbo_flow_destroy(flow);

    storage_remove_if_exists(missing_path);
    storage_remove_if_exists(large_path);
  }

  it("publishes regular files from a directory source") {
    static const char *src = "source dir_in adapter \"dir.read\"\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  dir_in -> capture\n"
                             "}\n";
    char dir_path[TURBO_FS_MAX_PATH];
    char first_path[TURBO_FS_MAX_PATH];
    char second_path[TURBO_FS_MAX_PATH];
    storage_capture_ctx_t capture;
    turbo_flow_storage_directory_source_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;

    storage_make_path(dir_path, sizeof(dir_path), "dir");
    storage_make_dir(dir_path);
    check_int_eq(turbo_fs_path_join(first_path, sizeof(first_path), dir_path, "a.txt"), 0);
    check_int_eq(turbo_fs_path_join(second_path, sizeof(second_path), dir_path, "b.txt"), 0);
    storage_write_test_file(first_path, "one", 3u);
    storage_write_test_file(second_path, "two", 3u);
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:directory-read";
    config.owner_name = "dir.read";
    config.path = dir_path;
    config.max_files = 2u;

    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_directory_source_adapter(flow, "dir.read", &config),
                 TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "dir.read");
    check_not_null(schema);
    check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_FILE);
    check_uint_eq(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    check_int_eq(
        turbo_flow_register_stage_ex(flow, "capture", storage_capture_stage, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    for (int i = 0; i < 1000 && atomic_load_explicit(&capture.count, memory_order_acquire) < 2;
         ++i) {
      turbo_sleep_ms(1);
    }
    check_int_eq(atomic_load_explicit(&capture.count, memory_order_acquire), 2);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);

    tstr_freep(&capture.payload);
    turbo_flow_destroy(flow);
    storage_remove_if_exists(first_path);
    storage_remove_if_exists(second_path);
    storage_remove_dir_if_exists(dir_path);
  }

  it("binds payloads into a sqlite sink statement") {
    static const char *src = "source input\n"
                             "stage db adapter \"sqlite.write\"\n"
                             "stage main {\n"
                             "  input -> db\n"
                             "}\n";
    char path[TURBO_FS_MAX_PATH];
    turbo_flow_storage_sqlite_sink_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;

    storage_make_path(path, sizeof(path), "events.sqlite");
    storage_sqlite_exec(path, "create table events(payload text not null)");
    memset(&config, 0, sizeof(config));
    config.resource_uid = "storage:sqlite-write";
    config.owner_name = "sqlite.write";
    config.path = path;
    config.statement = "insert into events(payload) values (?)";
    config.busy_timeout_ms = 1000;

    check_not_null(flow);
    check_int_eq(turbo_flow_storage_register_sqlite_sink_adapter(flow, "sqlite.write", &config),
                 TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "sqlite.write");
    check_not_null(schema);
    check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_SQLITE);
    check_uint_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    storage_publish_payload(flow, "input", "alpha", 5u, TURBO_OK);
    storage_publish_payload(flow, "input", "beta", 4u, TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);

    check_int_eq(storage_sqlite_count_payloads(path), 2);
    turbo_flow_destroy(flow);
    storage_remove_if_exists(path);
  }
}
