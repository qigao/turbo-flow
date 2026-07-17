#ifndef TURBO_FLOW_STORAGE_H
#define TURBO_FLOW_STORAGE_H

#include "turbo_flow.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_STORAGE_MODULE "buffer.storage"
#define TURBO_FLOW_STORAGE_FILE_READ_OPERATION "storage.file.read"
#define TURBO_FLOW_STORAGE_DIRECTORY_READ_OPERATION "storage.directory.read"
#define TURBO_FLOW_STORAGE_FILE_WRITE_OPERATION "storage.file.write"
#define TURBO_FLOW_STORAGE_APPEND_WRITE_OPERATION "storage.append.write"
#define TURBO_FLOW_STORAGE_SQLITE_EXECUTE_OPERATION "storage.sqlite.execute"
#define TURBO_FLOW_STORAGE_PRIMITIVE_TYPE "StorageResource"

typedef enum turbo_flow_storage_encoding_e {
  TURBO_FLOW_STORAGE_ENCODING_BINARY = 0,
  TURBO_FLOW_STORAGE_ENCODING_UTF8
} turbo_flow_storage_encoding_t;

typedef enum turbo_flow_storage_write_mode_e {
  TURBO_FLOW_STORAGE_WRITE_TRUNCATE = 0,
  TURBO_FLOW_STORAGE_WRITE_APPEND,
  TURBO_FLOW_STORAGE_WRITE_CREATE_NEW
} turbo_flow_storage_write_mode_t;

typedef enum turbo_flow_storage_record_mode_e {
  TURBO_FLOW_STORAGE_RECORD_LINE = 0,
  TURBO_FLOW_STORAGE_RECORD_LENGTH_PREFIXED_LE64
} turbo_flow_storage_record_mode_t;

typedef struct turbo_flow_storage_file_source_config_s {
  /** Stable resource identity. Required and unique within one flow. */
  const char *resource_uid;
  /** Resource owner name. Required and must name this storage owner. */
  const char *owner_name;
  /** Path to read once when the flow starts. Required for configured sources. */
  const char *path;
  /** Maximum accepted file size in bytes; 0 leaves the size unbounded. */
  size_t max_payload_size;
  /** TURBO_FLOW_STORAGE_ENCODING_BINARY or TURBO_FLOW_STORAGE_ENCODING_UTF8. */
  int encoding;
} turbo_flow_storage_file_source_config_t;

typedef struct turbo_flow_storage_directory_source_config_s {
  /** Stable resource identity. Required and unique within one flow. */
  const char *resource_uid;
  /** Resource owner name. Required and must name this storage owner. */
  const char *owner_name;
  /** Directory path scanned once when the flow starts. Required for configured sources. */
  const char *path;
  /** Maximum accepted file payload size in bytes; 0 leaves each file size unbounded. */
  size_t max_payload_size;
  /** Maximum regular files published from one scan; 0 leaves file count unbounded. */
  size_t max_files;
  /** TURBO_FLOW_STORAGE_ENCODING_BINARY or TURBO_FLOW_STORAGE_ENCODING_UTF8. */
  int encoding;
} turbo_flow_storage_directory_source_config_t;

typedef struct turbo_flow_storage_file_sink_config_s {
  /** Stable resource identity. Required and unique within one flow. */
  const char *resource_uid;
  /** Resource owner name. Required and must name this storage owner. */
  const char *owner_name;
  /** Path to write each consumed message payload. Required for configured sinks. */
  const char *path;
  /** Maximum accepted message payload size in bytes; 0 leaves the size unbounded. */
  size_t max_payload_size;
  /** TURBO_FLOW_STORAGE_ENCODING_BINARY or TURBO_FLOW_STORAGE_ENCODING_UTF8. */
  int encoding;
  /** TURBO_FLOW_STORAGE_WRITE_TRUNCATE, APPEND, or CREATE_NEW. */
  int write_mode;
  /** Non-zero flushes the file descriptor before closing. */
  int fsync;
} turbo_flow_storage_file_sink_config_t;

typedef struct turbo_flow_storage_append_log_config_s {
  /** Stable resource identity. Required and unique within one flow. */
  const char *resource_uid;
  /** Resource owner name. Required and must name this storage owner. */
  const char *owner_name;
  /** Path to append records to. Required for configured append-log sinks. */
  const char *path;
  /** Maximum accepted message payload size in bytes; 0 leaves the size unbounded. */
  size_t max_payload_size;
  /** TURBO_FLOW_STORAGE_ENCODING_BINARY or TURBO_FLOW_STORAGE_ENCODING_UTF8. */
  int encoding;
  /** TURBO_FLOW_STORAGE_RECORD_LINE or LENGTH_PREFIXED_LE64. */
  int record_mode;
  /** Non-zero flushes the file descriptor before closing. */
  int fsync;
} turbo_flow_storage_append_log_config_t;

typedef struct turbo_flow_storage_sqlite_sink_config_s {
  /** Stable resource identity. Required and unique within one flow. */
  const char *resource_uid;
  /** Resource owner name. Required and must name this storage owner. */
  const char *owner_name;
  /** SQLite database path. Required for configured sinks. */
  const char *path;
  /** Fixed parameterized statement. The message payload is bound as parameter 1. */
  const char *statement;
  /** Non-zero binds payload as BLOB; zero binds payload as TEXT. */
  int binary_payload;
  /** sqlite3 busy timeout in milliseconds; must be non-negative and 0 leaves the default. */
  int busy_timeout_ms;
} turbo_flow_storage_sqlite_sink_config_t;

/**
 * Register a file source adapter for DSL `source <name> adapter "<adapter>"`.
 *
 * The adapter reads exactly one configured local file after the flow reaches
 * STARTED and publishes the file contents as one message. It does not watch
 * directories, poll for changes, index files, or add persistence semantics to
 * turbo_flow core.
 */
CXX_C_API int turbo_flow_storage_register_file_source_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_file_source_config_t *config);

/**
 * Register a directory source adapter for DSL `source <name> adapter "<adapter>"`.
 *
 * The adapter scans one configured local directory once after STARTED and
 * publishes each regular file as one message. It does not recurse, watch for
 * changes, persist cursors, or delete files.
 */
CXX_C_API int turbo_flow_storage_register_directory_source_adapter(
    turbo_flow_t *flow, const char *name,
    const turbo_flow_storage_directory_source_config_t *config);

/**
 * Register a file sink adapter for DSL `stage <name> adapter "<adapter>"`.
 *
 * The adapter writes each consumed message payload to the configured path using
 * the configured write mode. It returns write errors to the current publish
 * call and does not retry or queue failed writes.
 */
CXX_C_API int
turbo_flow_storage_register_file_sink_adapter(turbo_flow_t *flow, const char *name,
                                              const turbo_flow_storage_file_sink_config_t *config);

/**
 * Register an append-log sink adapter for DSL `stage <name> adapter "<adapter>"`.
 *
 * Each consumed message is appended as either payload plus newline or an
 * unsigned little-endian 64-bit length prefix followed by the payload.
 */
CXX_C_API int turbo_flow_storage_register_append_log_sink_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_append_log_config_t *config);

/**
 * Register a SQLite sink adapter for DSL `stage <name> adapter "<adapter>"`.
 *
 * The adapter opens one SQLite database during start and executes the configured
 * statement for each message, binding the payload to parameter 1. It does not
 * construct SQL from message data or infer columns from parsed payloads.
 */
CXX_C_API int turbo_flow_storage_register_sqlite_sink_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_storage_sqlite_sink_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_STORAGE_H */
