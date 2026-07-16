#include "turbo_flow_storage.h"

#include "fmt.h"
#include "turbo_error.h"
#include "turbo_fs.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3.h"

#ifdef _WIN32
  #include <windows.h>
#else
  #include <dirent.h>
#endif

#ifndef EOVERFLOW
  #define EOVERFLOW ERANGE
#endif

typedef enum flow_storage_adapter_kind_e {
  FLOW_STORAGE_FILE_SOURCE = 0,
  FLOW_STORAGE_FILE_SINK,
  FLOW_STORAGE_APPEND_LOG_SINK,
  FLOW_STORAGE_DIRECTORY_SOURCE,
  FLOW_STORAGE_SQLITE_SINK
} flow_storage_adapter_kind_t;

typedef struct flow_storage_adapter_s {
  turbo_flow_t *flow;
  tstr_t path;
  tstr_t source_name;
  tstr_t statement;
  tstr_t resource_uid;
  tstr_t owner_name;
  size_t max_payload_size;
  size_t max_files;
  int encoding;
  int write_mode;
  int record_mode;
  int fsync;
  int binary_payload;
  int busy_timeout_ms;
  sqlite3 *sqlite_db;
  sqlite3_stmt *sqlite_stmt;
  turbo_mutex_t sqlite_lock;
  int sqlite_lock_initialized;
  flow_storage_adapter_kind_t kind;
  atomic_int started;
  atomic_uint_fast64_t operations;
  atomic_uint_fast64_t bytes;
  atomic_uint_fast64_t failures;
  atomic_int last_status;
  int thread_started;
  turbo_thread_t thread;
} flow_storage_adapter_t;

static const char FLOW_STORAGE_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowStorageResource [id(1), version(1)];\n"
    "message StorageStatus {\n"
    "  uint32 kind;\n"
    "  bool started;\n"
    "  string operations;\n"
    "  string bytes;\n"
    "  string failures;\n"
    "  int32 last_status;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_STORAGE_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t), TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_STORAGE, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON, "TurboFlowStorageResource", "StorageStatus", 1u, 1u,
    FLOW_STORAGE_STATUS_SCHEMA_TEXT};

static const char *const FLOW_STORAGE_ENCODING_VALUES[] = {"bin", "utf8"};
static const char *const FLOW_STORAGE_WRITE_MODE_VALUES[] = {"truncate", "append", "create_new"};
static const char *const FLOW_STORAGE_RECORD_MODE_VALUES[] = {"line", "length_prefixed_le64"};

static const turbo_flow_option_field_t FLOW_STORAGE_FILE_SOURCE_FIELDS[] = {
    {"path", TURBO_FLOW_OPTION_PATH, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"encoding", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_STORAGE_ENCODING_VALUES, 2}};

static const turbo_flow_option_field_t FLOW_STORAGE_DIRECTORY_SOURCE_FIELDS[] = {
    {"path", TURBO_FLOW_OPTION_PATH, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"max_files", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"encoding", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_STORAGE_ENCODING_VALUES, 2}};

static const turbo_flow_option_field_t FLOW_STORAGE_FILE_SINK_FIELDS[] = {
    {"path", TURBO_FLOW_OPTION_PATH, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"encoding", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_STORAGE_ENCODING_VALUES, 2},
    {"write_mode", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_STORAGE_WRITE_MODE_VALUES, 3},
    {"fsync", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

static const turbo_flow_option_field_t FLOW_STORAGE_APPEND_LOG_FIELDS[] = {
    {"path", TURBO_FLOW_OPTION_PATH, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"encoding", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_STORAGE_ENCODING_VALUES, 2},
    {"record_mode", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_STORAGE_RECORD_MODE_VALUES, 2},
    {"fsync", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

static const turbo_flow_option_field_t FLOW_STORAGE_SQLITE_FIELDS[] = {
    {"path", TURBO_FLOW_OPTION_PATH, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"statement", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"binary_payload", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"busy_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0}};

static const turbo_flow_adapter_schema_t FLOW_STORAGE_FILE_SOURCE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_FILE,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_STORAGE_FILE_SOURCE_FIELDS,
    sizeof(FLOW_STORAGE_FILE_SOURCE_FIELDS) / sizeof(FLOW_STORAGE_FILE_SOURCE_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_STORAGE_DIRECTORY_SOURCE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_FILE,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_STORAGE_DIRECTORY_SOURCE_FIELDS,
    sizeof(FLOW_STORAGE_DIRECTORY_SOURCE_FIELDS) / sizeof(FLOW_STORAGE_DIRECTORY_SOURCE_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_STORAGE_FILE_SINK_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_FILE,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_STORAGE_FILE_SINK_FIELDS,
    sizeof(FLOW_STORAGE_FILE_SINK_FIELDS) / sizeof(FLOW_STORAGE_FILE_SINK_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_STORAGE_APPEND_LOG_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_FILE,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_STORAGE_APPEND_LOG_FIELDS,
    sizeof(FLOW_STORAGE_APPEND_LOG_FIELDS) / sizeof(FLOW_STORAGE_APPEND_LOG_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_STORAGE_SQLITE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_SQLITE,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_STORAGE_SQLITE_FIELDS,
    sizeof(FLOW_STORAGE_SQLITE_FIELDS) / sizeof(FLOW_STORAGE_SQLITE_FIELDS[0])};

static int flow_storage_sqlite_open(flow_storage_adapter_t *adapter);
static void flow_storage_sqlite_close(flow_storage_adapter_t *adapter);
static int flow_storage_sqlite_consume(flow_storage_adapter_t *adapter, const char *data,
                                       size_t len);

static int flow_storage_errno_to_turbo(int rc) {
  int e;

  if (rc >= 0) return rc;
  e = -rc;
  switch (e) {
  case EACCES:
    return TURBO_EPERM;
  case EEXIST:
    return TURBO_EALREADY;
  case EFBIG:
    return TURBO_EFBIG;
  case EINVAL:
    return TURBO_EINVAL;
  case EISDIR:
    return TURBO_EISDIR;
  case ENAMETOOLONG:
    return TURBO_ENAMETOOLONG;
  case ENOENT:
    return TURBO_ENOENT;
  case ENOMEM:
    return TURBO_ENOMEM;
  case ENOSPC:
    return TURBO_ENOSPC;
  case ENOTDIR:
    return TURBO_ENOTDIR;
  case ERANGE:
    return TURBO_ERANGE;
#if EOVERFLOW != ERANGE
  case EOVERFLOW:
    return TURBO_ERANGE;
#endif
#ifdef EROFS
  case EROFS:
    return TURBO_EROFS;
#endif
  default:
    return TURBO_EIO;
  }
}

static int flow_storage_dup_opt(tstr_t *dst, const char *src) {
  if (!dst) return TURBO_EINVAL;
  if (!src) return TURBO_OK;
  *dst = tstr_dup(src);
  return *dst ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_storage_identity_valid(const char *resource_uid, const char *owner_name) {
  size_t uid_len;
  size_t owner_len;
  if (!resource_uid || !*resource_uid || !owner_name || !*owner_name) return 0;
  uid_len = strlen(resource_uid);
  owner_len = strlen(owner_name);
  return uid_len <= TURBO_FLOW_RESOURCE_UID_MAX &&
         owner_len <= TURBO_FLOW_RESOURCE_OWNER_MAX;
}

static int flow_storage_set_identity(flow_storage_adapter_t *adapter,
                                     const char *resource_uid,
                                     const char *owner_name) {
  int rc;
  if (!adapter || !flow_storage_identity_valid(resource_uid, owner_name)) return TURBO_EINVAL;
  rc = flow_storage_dup_opt(&adapter->resource_uid, resource_uid);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->owner_name, owner_name);
  return rc;
}

static void flow_storage_record(flow_storage_adapter_t *adapter, size_t bytes, int status) {
  if (!adapter) return;
  atomic_fetch_add_explicit(&adapter->operations, 1u, memory_order_relaxed);
  if (status == TURBO_OK) {
    atomic_fetch_add_explicit(&adapter->bytes, bytes, memory_order_relaxed);
  } else {
    atomic_fetch_add_explicit(&adapter->failures, 1u, memory_order_relaxed);
  }
  atomic_store_explicit(&adapter->last_status, status, memory_order_release);
}

static int flow_storage_record_result(flow_storage_adapter_t *adapter, size_t bytes, int status) {
  flow_storage_record(adapter, bytes, status);
  return status;
}

static void flow_storage_init_state(flow_storage_adapter_t *adapter) {
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->operations, 0u);
  atomic_init(&adapter->bytes, 0u);
  atomic_init(&adapter->failures, 0u);
  atomic_init(&adapter->last_status, TURBO_OK);
}

static int flow_storage_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int written;
  if (!adapter || !out || out->size < sizeof(*out) || !adapter->resource_uid ||
      !adapter->owner_name) {
    return TURBO_EINVAL;
  }
  metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  metadata.kind = TURBO_FLOW_RESOURCE_STORAGE;
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", adapter->resource_uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", adapter->owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_storage_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flow_storage_resource_metadata(ctx, &metadata);
  if (rc != TURBO_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  out->last_status = atomic_load_explicit(&adapter->last_status, memory_order_acquire);
  return TURBO_OK;
}

static int flow_storage_resource_document(void *ctx,
                                          turbo_flow_resource_document_kind_t document_kind,
                                          turbo_flow_resource_document_t *out) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  tstr_t payload;
  int rc;
  if (!adapter || !out || document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS)
    return document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_STATUS ? TURBO_EINVAL : TURBO_ENOTSUP;
  rc = flow_storage_resource_metadata(adapter, &metadata);
  if (rc != TURBO_OK) return rc;
  payload = tstr_format(
      "{\"kind\":{},\"started\":{},\"operations\":\"{}\",\"bytes\":\"{}\","
      "\"failures\":\"{}\",\"last_status\":{}}",
      (unsigned)adapter->kind,
      atomic_load_explicit(&adapter->started, memory_order_acquire),
      atomic_load_explicit(&adapter->operations, memory_order_relaxed),
      atomic_load_explicit(&adapter->bytes, memory_order_relaxed),
      atomic_load_explicit(&adapter->failures, memory_order_relaxed),
      atomic_load_explicit(&adapter->last_status, memory_order_acquire));
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(
      out, &metadata, &FLOW_STORAGE_STATUS_SCHEMA, payload, tstr_len(payload));
  tstr_free(payload);
  return rc;
}

static int flow_storage_path_configured(const flow_storage_adapter_t *adapter) {
  return adapter && adapter->path && adapter->path[0] != '\0';
}

static int flow_storage_valid_encoding(int encoding) {
  return encoding == TURBO_FLOW_STORAGE_ENCODING_BINARY ||
         encoding == TURBO_FLOW_STORAGE_ENCODING_UTF8;
}

static int flow_storage_valid_write_mode(int write_mode) {
  return write_mode == TURBO_FLOW_STORAGE_WRITE_TRUNCATE ||
         write_mode == TURBO_FLOW_STORAGE_WRITE_APPEND ||
         write_mode == TURBO_FLOW_STORAGE_WRITE_CREATE_NEW;
}

static int flow_storage_valid_record_mode(int record_mode) {
  return record_mode == TURBO_FLOW_STORAGE_RECORD_LINE ||
         record_mode == TURBO_FLOW_STORAGE_RECORD_LENGTH_PREFIXED_LE64;
}

static int flow_storage_check_payload_size(const flow_storage_adapter_t *adapter, size_t len) {
  if (!adapter) return TURBO_EINVAL;
  if (adapter->max_payload_size > 0 && len > adapter->max_payload_size) return TURBO_EFBIG;
  return TURBO_OK;
}

static int flow_storage_validate_source_file_path(const flow_storage_adapter_t *adapter,
                                                  const char *path) {
  turbo_fs_stat_t stat_info;
  int rc;

  if (!adapter || !path || path[0] == '\0') return TURBO_ENOTSUP;
  memset(&stat_info, 0, sizeof(stat_info));
  rc = flow_storage_errno_to_turbo(turbo_fs_stat(path, &stat_info));
  if (rc != TURBO_OK) return rc;
  if (!stat_info.is_file) return stat_info.is_directory ? TURBO_EISDIR : TURBO_EINVAL;
  if (adapter->max_payload_size > 0 && stat_info.size > adapter->max_payload_size) {
    return TURBO_EFBIG;
  }
  if (stat_info.size > (uint64_t)((size_t)-1)) return TURBO_ERANGE;
  return TURBO_OK;
}

static int flow_storage_validate_source_file_config(const flow_storage_adapter_t *adapter) {
  return flow_storage_validate_source_file_path(adapter, adapter ? adapter->path : NULL);
}

static int flow_storage_source_file_size(const flow_storage_adapter_t *adapter, const char *path,
                                         size_t *out_size) {
  turbo_fs_stat_t stat_info;
  int rc;

  if (!out_size) return TURBO_EINVAL;
  rc = flow_storage_validate_source_file_path(adapter, path);
  if (rc != TURBO_OK) return rc;

  memset(&stat_info, 0, sizeof(stat_info));
  rc = flow_storage_errno_to_turbo(turbo_fs_stat(path, &stat_info));
  if (rc != TURBO_OK) return rc;
  *out_size = (size_t)stat_info.size;
  return TURBO_OK;
}

static int flow_storage_validate_utf8(const char *data, size_t len) {
  tstr_t tmp;
  int ok;

  if (len > 0 && !data) return TURBO_EINVAL;
  tmp = tstr_new_len(data ? data : "", len);
  if (!tmp) return TURBO_ENOMEM;
  ok = tstr_utf8_valid(tmp);
  tstr_freep(&tmp);
  return ok ? TURBO_OK : TURBO_EPROTO;
}

static int flow_storage_validate_payload(const flow_storage_adapter_t *adapter, const char *data,
                                         size_t len) {
  int rc;

  if (!adapter || (len > 0 && !data)) return TURBO_EINVAL;
  rc = flow_storage_check_payload_size(adapter, len);
  if (rc != TURBO_OK) return rc;
  if (adapter->encoding == TURBO_FLOW_STORAGE_ENCODING_UTF8) {
    return flow_storage_validate_utf8(data, len);
  }
  return TURBO_OK;
}

static int flow_storage_source_publish_path(flow_storage_adapter_t *adapter, const char *path) {
  turbo_flow_msg_t msg;
  turbo_file_t fd;
  size_t file_size = 0;
  size_t total = 0;
  int rc;

  if (!adapter || !adapter->flow || !adapter->source_name) return TURBO_EINVAL;
  rc = flow_storage_source_file_size(adapter, path, &file_size);
  if (rc != TURBO_OK) return flow_storage_record_result(adapter, 0u, rc);

  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(file_size > 0 ? NULL : "", file_size);
  if (!msg.owned_payload) return flow_storage_record_result(adapter, 0u, TURBO_ENOMEM);
  msg.payload = tstr_to_v(msg.owned_payload);

  fd = turbo_fs_open(path, TURBO_FS_O_RDONLY, 0);
  if (fd == TURBO_INVALID_FILE) {
    turbo_flow_msg_cleanup(&msg);
    return flow_storage_record_result(adapter, 0u, TURBO_EIO);
  }
  while (total < file_size) {
    int n = turbo_fs_read(fd, msg.owned_payload + total, file_size - total);
    if (n < 0) {
      rc = flow_storage_errno_to_turbo(n);
      break;
    }
    if (n == 0) {
      rc = TURBO_EIO;
      break;
    }
    total += (size_t)n;
  }
  {
    int close_rc = flow_storage_errno_to_turbo(turbo_fs_close(fd));
    if (rc == TURBO_OK) rc = close_rc;
  }
  if (rc == TURBO_OK)
    rc = flow_storage_validate_payload(adapter, msg.payload.data, msg.payload.len);
  if (rc != TURBO_OK) {
    turbo_flow_msg_cleanup(&msg);
    return flow_storage_record_result(adapter, 0u, rc);
  }

  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  return flow_storage_record_result(adapter, file_size, rc);
}

static int flow_storage_source_publish_file(flow_storage_adapter_t *adapter) {
  return flow_storage_source_publish_path(adapter, adapter ? adapter->path : NULL);
}

static int flow_storage_source_publish_directory_entry(flow_storage_adapter_t *adapter,
                                                       const char *name, int *published) {
  char full_path[TURBO_FS_MAX_PATH];
  int rc;

  if (published) *published = 0;
  if (!adapter || !name || name[0] == '\0') return TURBO_EINVAL;
  if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return TURBO_OK;
  rc = turbo_fs_path_join(full_path, sizeof(full_path), adapter->path, name);
  if (rc != 0) return flow_storage_errno_to_turbo(rc);
  rc = flow_storage_validate_source_file_path(adapter, full_path);
  if (rc == TURBO_EISDIR) return TURBO_OK;
  if (rc != TURBO_OK) return rc;
  rc = flow_storage_source_publish_path(adapter, full_path);
  if (rc == TURBO_OK && published) *published = 1;
  return rc;
}

static int flow_storage_source_publish_directory(flow_storage_adapter_t *adapter) {
  turbo_fs_stat_t stat_info;
  size_t published = 0;
  int rc;

  if (!adapter || !adapter->path || adapter->path[0] == '\0') return TURBO_ENOTSUP;
  memset(&stat_info, 0, sizeof(stat_info));
  rc = flow_storage_errno_to_turbo(turbo_fs_stat(adapter->path, &stat_info));
  if (rc != TURBO_OK) return rc;
  if (!stat_info.is_directory) return TURBO_ENOTDIR;

#ifdef _WIN32
  {
    char pattern[TURBO_FS_MAX_PATH];
    WIN32_FIND_DATAA data;
    HANDLE find;
    rc = turbo_fs_path_join(pattern, sizeof(pattern), adapter->path, "*");
    if (rc != 0) return flow_storage_errno_to_turbo(rc);
    find = FindFirstFileA(pattern, &data);
    if (find == INVALID_HANDLE_VALUE) {
      return GetLastError() == ERROR_FILE_NOT_FOUND ? TURBO_OK : TURBO_EIO;
    }
    do {
      int did_publish = 0;
      if (adapter->max_files > 0 && published >= adapter->max_files) break;
      if (data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) continue;
      rc = flow_storage_source_publish_directory_entry(adapter, data.cFileName, &did_publish);
      if (rc != TURBO_OK) {
        FindClose(find);
        return rc;
      }
      if (did_publish) ++published;
    } while (FindNextFileA(find, &data));
    FindClose(find);
  }
#else
  {
    DIR *dir = opendir(adapter->path);
    struct dirent *entry;
    if (!dir) return TURBO_EIO;
    while ((entry = readdir(dir)) != NULL) {
      int did_publish = 0;
      if (adapter->max_files > 0 && published >= adapter->max_files) break;
      rc = flow_storage_source_publish_directory_entry(adapter, entry->d_name, &did_publish);
      if (rc != TURBO_OK) {
        closedir(dir);
        return rc;
      }
      if (did_publish) ++published;
    }
    closedir(dir);
  }
#endif

  return TURBO_OK;
}

static void flow_storage_source_thread(void *arg) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)arg;

  if (!adapter) return;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return;
  if (adapter->kind == FLOW_STORAGE_DIRECTORY_SOURCE) {
    int rc = flow_storage_source_publish_directory(adapter);
    if (rc != TURBO_OK) atomic_store_explicit(&adapter->last_status, rc, memory_order_release);
  } else {
    (void)flow_storage_source_publish_file(adapter);
  }
}

static int flow_storage_file_source_start(void *ctx, turbo_flow_t *flow,
                                          const turbo_flow_stage_plan_t *stage) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;

  if (!adapter || !stage) return TURBO_EINVAL;
  if (!stage->is_source) return TURBO_EINVAL;
  if (!flow_storage_path_configured(adapter)) return TURBO_ENOTSUP;
  if (!flow_storage_valid_encoding(adapter->encoding)) return TURBO_EINVAL;
  if (adapter->thread_started) return TURBO_EALREADY;
  if (adapter->kind == FLOW_STORAGE_FILE_SOURCE) {
    int rc = flow_storage_validate_source_file_config(adapter);
    if (rc != TURBO_OK) return rc;
  } else {
    turbo_fs_stat_t stat_info;
    int rc = flow_storage_errno_to_turbo(turbo_fs_stat(adapter->path, &stat_info));
    if (rc != TURBO_OK) return rc;
    if (!stat_info.is_directory) return TURBO_ENOTDIR;
  }

  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) return TURBO_ENOMEM;
  adapter->flow = flow;
  atomic_store_explicit(&adapter->started, 1, memory_order_release);

  if (turbo_thread_create(&adapter->thread, flow_storage_source_thread, adapter) != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    atomic_store_explicit(&adapter->last_status, TURBO_EINVAL, memory_order_release);
    return TURBO_EINVAL;
  }
  adapter->thread_started = 1;
  return TURBO_OK;
}

static void flow_storage_join_source_thread(flow_storage_adapter_t *adapter) {
  if (!adapter || !adapter->thread_started) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  (void)turbo_thread_join(&adapter->thread);
  adapter->thread_started = 0;
}

static int flow_storage_sink_start(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;

  (void)flow;
  if (!adapter || !stage) return TURBO_EINVAL;
  if (stage->is_source) return TURBO_EINVAL;
  if (adapter->kind == FLOW_STORAGE_SQLITE_SINK) {
    int rc;
    turbo_mutex_lock(&adapter->sqlite_lock);
    rc = flow_storage_sqlite_open(adapter);
    if (rc == TURBO_OK) atomic_store_explicit(&adapter->started, 1, memory_order_release);
    atomic_store_explicit(&adapter->last_status, rc, memory_order_release);
    turbo_mutex_unlock(&adapter->sqlite_lock);
    return rc;
  }
  if (!flow_storage_path_configured(adapter)) return TURBO_ENOTSUP;
  if (!flow_storage_valid_encoding(adapter->encoding)) return TURBO_EINVAL;
  if (adapter->kind == FLOW_STORAGE_FILE_SINK &&
      !flow_storage_valid_write_mode(adapter->write_mode)) {
    return TURBO_EINVAL;
  }
  if (adapter->kind == FLOW_STORAGE_APPEND_LOG_SINK &&
      !flow_storage_valid_record_mode(adapter->record_mode)) {
    return TURBO_EINVAL;
  }
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  atomic_store_explicit(&adapter->last_status, TURBO_OK, memory_order_release);
  return TURBO_OK;
}

static int flow_storage_open_for_sink(const flow_storage_adapter_t *adapter) {
  int flags = TURBO_FS_O_WRONLY | TURBO_FS_O_CREAT;

  if (!adapter) return TURBO_INVALID_FILE;
  switch (adapter->write_mode) {
  case TURBO_FLOW_STORAGE_WRITE_TRUNCATE:
    flags |= TURBO_FS_O_TRUNC;
    break;
  case TURBO_FLOW_STORAGE_WRITE_APPEND:
    flags |= TURBO_FS_O_APPEND;
    break;
  case TURBO_FLOW_STORAGE_WRITE_CREATE_NEW:
    if (turbo_fs_access(adapter->path, TURBO_FS_ACCESS_EXISTS) == 0) {
      return TURBO_INVALID_FILE;
    }
    flags |= TURBO_FS_O_TRUNC;
    break;
  default:
    return TURBO_INVALID_FILE;
  }
  return turbo_fs_open(adapter->path, flags, TURBO_FS_DEFAULT_MODE);
}

static int flow_storage_write_all_to_fd(turbo_file_t fd, const char *data, size_t len) {
  size_t total = 0;

  while (total < len) {
    int n = turbo_fs_write(fd, data + total, len - total);
    if (n < 0) return flow_storage_errno_to_turbo(n);
    if (n == 0) return TURBO_EIO;
    total += (size_t)n;
  }
  return TURBO_OK;
}

static int flow_storage_write_payload(flow_storage_adapter_t *adapter, const char *data,
                                      size_t len) {
  turbo_file_t fd;
  int rc;
  int close_rc;

  if (!adapter) return TURBO_EINVAL;
  fd = flow_storage_open_for_sink(adapter);
  if (fd == TURBO_INVALID_FILE) {
    if (adapter->write_mode == TURBO_FLOW_STORAGE_WRITE_CREATE_NEW &&
        turbo_fs_access(adapter->path, TURBO_FS_ACCESS_EXISTS) == 0) {
      return TURBO_EALREADY;
    }
    return TURBO_EIO;
  }

  rc = flow_storage_write_all_to_fd(fd, data ? data : "", len);
  if (rc == TURBO_OK && adapter->fsync) rc = flow_storage_errno_to_turbo(turbo_fs_fsync(fd));
  close_rc = flow_storage_errno_to_turbo(turbo_fs_close(fd));
  return rc != TURBO_OK ? rc : close_rc;
}

static void flow_storage_store_le64(char out[8], uint64_t value) {
  for (size_t i = 0; i < 8u; ++i) {
    out[i] = (char)((value >> (i * 8u)) & 0xffu);
  }
}

static int flow_storage_append_log_record(flow_storage_adapter_t *adapter, const char *data,
                                          size_t len) {
  turbo_file_t fd;
  int rc = TURBO_OK;
  int close_rc;

  if (!adapter) return TURBO_EINVAL;
  fd = turbo_fs_open(adapter->path, TURBO_FS_O_WRONLY | TURBO_FS_O_CREAT | TURBO_FS_O_APPEND,
                     TURBO_FS_DEFAULT_MODE);
  if (fd == TURBO_INVALID_FILE) return TURBO_EIO;

  switch (adapter->record_mode) {
  case TURBO_FLOW_STORAGE_RECORD_LINE:
    rc = flow_storage_write_all_to_fd(fd, data ? data : "", len);
    if (rc == TURBO_OK) rc = flow_storage_write_all_to_fd(fd, "\n", 1u);
    break;
  case TURBO_FLOW_STORAGE_RECORD_LENGTH_PREFIXED_LE64: {
    char prefix[8];
    flow_storage_store_le64(prefix, (uint64_t)len);
    rc = flow_storage_write_all_to_fd(fd, prefix, sizeof(prefix));
    if (rc == TURBO_OK) rc = flow_storage_write_all_to_fd(fd, data ? data : "", len);
    break;
  }
  default:
    rc = TURBO_EINVAL;
    break;
  }

  if (rc == TURBO_OK && adapter->fsync) rc = flow_storage_errno_to_turbo(turbo_fs_fsync(fd));
  close_rc = flow_storage_errno_to_turbo(turbo_fs_close(fd));
  return rc != TURBO_OK ? rc : close_rc;
}

static int flow_storage_sqlite_open(flow_storage_adapter_t *adapter) {
  int rc;

  if (!adapter || !adapter->path || adapter->path[0] == '\0' || !adapter->statement ||
      adapter->statement[0] == '\0')
    return TURBO_ENOTSUP;

  rc = sqlite3_open(adapter->path, &adapter->sqlite_db);
  if (rc != SQLITE_OK || !adapter->sqlite_db) {
    if (adapter->sqlite_db) sqlite3_close(adapter->sqlite_db);
    adapter->sqlite_db = NULL;
    return TURBO_EIO;
  }
  if (adapter->busy_timeout_ms > 0) {
    sqlite3_busy_timeout(adapter->sqlite_db, adapter->busy_timeout_ms);
  }
  rc = sqlite3_prepare_v2(adapter->sqlite_db, adapter->statement, -1, &adapter->sqlite_stmt, NULL);
  if (rc != SQLITE_OK || !adapter->sqlite_stmt) {
    sqlite3_close(adapter->sqlite_db);
    adapter->sqlite_db = NULL;
    adapter->sqlite_stmt = NULL;
    return TURBO_EINVAL;
  }
  if (sqlite3_bind_parameter_count(adapter->sqlite_stmt) != 1) {
    sqlite3_finalize(adapter->sqlite_stmt);
    sqlite3_close(adapter->sqlite_db);
    adapter->sqlite_stmt = NULL;
    adapter->sqlite_db = NULL;
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static void flow_storage_sqlite_close(flow_storage_adapter_t *adapter) {
  if (!adapter) return;
  if (adapter->sqlite_stmt) {
    sqlite3_finalize(adapter->sqlite_stmt);
    adapter->sqlite_stmt = NULL;
  }
  if (adapter->sqlite_db) {
    sqlite3_close(adapter->sqlite_db);
    adapter->sqlite_db = NULL;
  }
}

static int flow_storage_sqlite_consume(flow_storage_adapter_t *adapter, const char *data,
                                       size_t len) {
  int rc;

  if (!adapter || !adapter->sqlite_stmt) return TURBO_ESHUTDOWN;
  if (len > (size_t)INT_MAX) return TURBO_EINVAL;

  sqlite3_reset(adapter->sqlite_stmt);
  sqlite3_clear_bindings(adapter->sqlite_stmt);
  if (adapter->binary_payload) {
    rc = sqlite3_bind_blob(adapter->sqlite_stmt, 1, data ? data : "", (int)len, SQLITE_TRANSIENT);
  } else {
    rc = sqlite3_bind_text(adapter->sqlite_stmt, 1, data ? data : "", (int)len, SQLITE_TRANSIENT);
  }
  if (rc != SQLITE_OK) return TURBO_EIO;
  rc = sqlite3_step(adapter->sqlite_stmt);
  if (rc != SQLITE_DONE && rc != SQLITE_ROW) return TURBO_EIO;
  sqlite3_reset(adapter->sqlite_stmt);
  return TURBO_OK;
}

static int flow_storage_sink_consume(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;
  int rc;

  (void)flow;
  (void)stage;
  if (!adapter) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire))
    return flow_storage_record_result(adapter, 0u, TURBO_EINVAL);
  if (!flow_storage_path_configured(adapter))
    return flow_storage_record_result(adapter, 0u, TURBO_ENOTSUP);
  if (!msg || (msg->payload.len > 0 && !msg->payload.data))
    return flow_storage_record_result(adapter, 0u, TURBO_EINVAL);

  rc = flow_storage_validate_payload(adapter, msg->payload.data, msg->payload.len);
  if (rc != TURBO_OK) return flow_storage_record_result(adapter, 0u, rc);

  if (adapter->kind == FLOW_STORAGE_APPEND_LOG_SINK) {
    rc = flow_storage_append_log_record(adapter, msg->payload.data, msg->payload.len);
    return flow_storage_record_result(adapter, msg->payload.len, rc);
  }
  if (adapter->kind == FLOW_STORAGE_SQLITE_SINK) {
    turbo_mutex_lock(&adapter->sqlite_lock);
    rc = flow_storage_sqlite_consume(adapter, msg->payload.data, msg->payload.len);
    turbo_mutex_unlock(&adapter->sqlite_lock);
    return flow_storage_record_result(adapter, msg->payload.len, rc);
  }
  rc = flow_storage_write_payload(adapter, msg->payload.data, msg->payload.len);
  return flow_storage_record_result(adapter, msg->payload.len, rc);
}

static void flow_storage_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;

  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  flow_storage_join_source_thread(adapter);
  if (adapter->kind == FLOW_STORAGE_SQLITE_SINK && adapter->sqlite_lock_initialized) {
    turbo_mutex_lock(&adapter->sqlite_lock);
    flow_storage_sqlite_close(adapter);
    turbo_mutex_unlock(&adapter->sqlite_lock);
  }
}

static void flow_storage_shutdown(void *ctx) {
  flow_storage_adapter_t *adapter = (flow_storage_adapter_t *)ctx;

  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  flow_storage_join_source_thread(adapter);
  if (adapter->sqlite_lock_initialized) {
    turbo_mutex_lock(&adapter->sqlite_lock);
    flow_storage_sqlite_close(adapter);
    turbo_mutex_unlock(&adapter->sqlite_lock);
    turbo_mutex_destroy(&adapter->sqlite_lock);
  }
  tstr_freep(&adapter->path);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->statement);
  tstr_freep(&adapter->resource_uid);
  tstr_freep(&adapter->owner_name);
  free(adapter);
}

static int flow_storage_register_adapter(turbo_flow_t *flow, const char *name,
                                         flow_storage_adapter_t *adapter, int is_source,
                                         const turbo_flow_adapter_schema_t *schema) {
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  int rc;

  if (!flow || !name || name[0] == '\0' || !adapter) return TURBO_EINVAL;
  memset(&ops, 0, sizeof(ops));
  ops.start = is_source ? flow_storage_file_source_start : flow_storage_sink_start;
  ops.consume = is_source ? NULL : flow_storage_sink_consume;
  ops.stop = flow_storage_stop;
  ops.shutdown = flow_storage_shutdown;

  resource.owner_name = adapter->owner_name;
  resource.ops.metadata = flow_storage_resource_metadata;
  resource.ops.snapshot = flow_storage_resource_snapshot;
  resource.ops.document = flow_storage_resource_document;
  resource.ctx = adapter;
  rc = turbo_flow_register_adapter_with_resources(flow, name, &ops, adapter, schema, &resource, 1u);
  return rc;
}

int turbo_flow_storage_register_file_source_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_file_source_config_t *config) {
  flow_storage_adapter_t *adapter;
  int rc;

  if (!flow || !name || name[0] == '\0' || !config ||
      !flow_storage_identity_valid(config->resource_uid, config->owner_name)) return TURBO_EINVAL;
  adapter = (flow_storage_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  flow_storage_init_state(adapter);
  adapter->kind = FLOW_STORAGE_FILE_SOURCE;
  adapter->encoding = TURBO_FLOW_STORAGE_ENCODING_BINARY;

  adapter->max_payload_size = config->max_payload_size;
  adapter->encoding = config->encoding;
  rc = flow_storage_set_identity(adapter, config->resource_uid, config->owner_name);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->path, config->path);
  if (rc != TURBO_OK) {
    flow_storage_shutdown(adapter);
    return rc;
  }

  return flow_storage_register_adapter(flow, name, adapter, 1, &FLOW_STORAGE_FILE_SOURCE_SCHEMA);
}

int turbo_flow_storage_register_directory_source_adapter(
    turbo_flow_t *flow, const char *name,
    const turbo_flow_storage_directory_source_config_t *config) {
  flow_storage_adapter_t *adapter;
  int rc;

  if (!flow || !name || name[0] == '\0' || !config ||
      !flow_storage_identity_valid(config->resource_uid, config->owner_name)) return TURBO_EINVAL;
  adapter = (flow_storage_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  flow_storage_init_state(adapter);
  adapter->kind = FLOW_STORAGE_DIRECTORY_SOURCE;
  adapter->encoding = TURBO_FLOW_STORAGE_ENCODING_BINARY;

  adapter->max_payload_size = config->max_payload_size;
  adapter->max_files = config->max_files;
  adapter->encoding = config->encoding;
  rc = flow_storage_set_identity(adapter, config->resource_uid, config->owner_name);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->path, config->path);
  if (rc != TURBO_OK) {
    flow_storage_shutdown(adapter);
    return rc;
  }

  return flow_storage_register_adapter(flow, name, adapter, 1,
                                       &FLOW_STORAGE_DIRECTORY_SOURCE_SCHEMA);
}

int turbo_flow_storage_register_file_sink_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_file_sink_config_t *config) {
  flow_storage_adapter_t *adapter;
  int rc;

  if (!flow || !name || name[0] == '\0' || !config ||
      !flow_storage_identity_valid(config->resource_uid, config->owner_name)) return TURBO_EINVAL;
  adapter = (flow_storage_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  flow_storage_init_state(adapter);
  adapter->kind = FLOW_STORAGE_FILE_SINK;
  adapter->encoding = TURBO_FLOW_STORAGE_ENCODING_BINARY;
  adapter->write_mode = TURBO_FLOW_STORAGE_WRITE_TRUNCATE;

  adapter->max_payload_size = config->max_payload_size;
  adapter->encoding = config->encoding;
  adapter->write_mode = config->write_mode;
  adapter->fsync = config->fsync ? 1 : 0;
  rc = flow_storage_set_identity(adapter, config->resource_uid, config->owner_name);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->path, config->path);
  if (rc != TURBO_OK) {
    flow_storage_shutdown(adapter);
    return rc;
  }

  return flow_storage_register_adapter(flow, name, adapter, 0, &FLOW_STORAGE_FILE_SINK_SCHEMA);
}

int turbo_flow_storage_register_append_log_sink_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_append_log_config_t *config) {
  flow_storage_adapter_t *adapter;
  int rc;

  if (!flow || !name || name[0] == '\0' || !config ||
      !flow_storage_identity_valid(config->resource_uid, config->owner_name)) return TURBO_EINVAL;
  adapter = (flow_storage_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  flow_storage_init_state(adapter);
  adapter->kind = FLOW_STORAGE_APPEND_LOG_SINK;
  adapter->encoding = TURBO_FLOW_STORAGE_ENCODING_BINARY;
  adapter->record_mode = TURBO_FLOW_STORAGE_RECORD_LINE;

  adapter->max_payload_size = config->max_payload_size;
  adapter->encoding = config->encoding;
  adapter->record_mode = config->record_mode;
  adapter->fsync = config->fsync ? 1 : 0;
  rc = flow_storage_set_identity(adapter, config->resource_uid, config->owner_name);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->path, config->path);
  if (rc != TURBO_OK) {
    flow_storage_shutdown(adapter);
    return rc;
  }

  return flow_storage_register_adapter(flow, name, adapter, 0, &FLOW_STORAGE_APPEND_LOG_SCHEMA);
}

int turbo_flow_storage_register_sqlite_sink_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_sqlite_sink_config_t *config) {
  flow_storage_adapter_t *adapter;
  int rc;

  if (!flow || !name || name[0] == '\0' || !config ||
      !flow_storage_identity_valid(config->resource_uid, config->owner_name) || !config->path ||
      config->path[0] == '\0' || !config->statement || config->statement[0] == '\0' ||
      config->busy_timeout_ms < 0) {
    return TURBO_EINVAL;
  }
  adapter = (flow_storage_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  flow_storage_init_state(adapter);
  adapter->kind = FLOW_STORAGE_SQLITE_SINK;
  adapter->encoding = TURBO_FLOW_STORAGE_ENCODING_BINARY;
  adapter->binary_payload = config->binary_payload ? 1 : 0;
  adapter->busy_timeout_ms = config->busy_timeout_ms;
  turbo_mutex_init(&adapter->sqlite_lock);
  adapter->sqlite_lock_initialized = 1;

  rc = flow_storage_set_identity(adapter, config->resource_uid, config->owner_name);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->path, config->path);
  if (rc == TURBO_OK) rc = flow_storage_dup_opt(&adapter->statement, config->statement);
  if (rc != TURBO_OK) {
    flow_storage_shutdown(adapter);
    return rc;
  }

  return flow_storage_register_adapter(flow, name, adapter, 0, &FLOW_STORAGE_SQLITE_SCHEMA);
}
