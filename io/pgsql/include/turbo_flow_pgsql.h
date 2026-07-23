#ifndef TURBO_FLOW_PGSQL_H
#define TURBO_FLOW_PGSQL_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_pgsql_sink_config_s {
  const char *conninfo;
  /** Fixed parameterized statement. The message payload is bound as $1. */
  const char *statement;
  int binary_payload;
  /** Optional parameter payload media type and trusted schema binding. */
  const char *parameter_content_type;
  const turbo_flow_content_binding_t *parameter_content_binding;
} turbo_flow_pgsql_sink_config_t;

CXX_C_API int turbo_flow_pgsql_register_sink_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_pgsql_sink_config_t *config);

#define TURBO_FLOW_PGSQL_DEFAULT_MAX_ROWS 10000u
#define TURBO_FLOW_PGSQL_DEFAULT_MAX_RESULT_BYTES (16u * 1024u * 1024u)

typedef enum turbo_flow_pgsql_result_format_e {
  TURBO_FLOW_PGSQL_RESULT_ROWSET_JSON = 0,
  /** Strict text CSV intended for a downstream DataBind CSV bind_all stage. */
  TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV
} turbo_flow_pgsql_result_format_t;

typedef struct turbo_flow_pgsql_query_config_s {
  const char *conninfo;
  /** Fixed SELECT or CRUD/RETURNING statement. */
  const char *statement;
  /** Non-secret stable identity used by content descriptors; never use raw SQL here. */
  const char *statement_name;
  /** Zero executes without parameters; non-zero binds the message payload as $1. */
  int bind_payload;
  int binary_payload;
  turbo_flow_pgsql_result_format_t result_format;
  size_t max_rows;
  size_t max_result_bytes;
  /** Optional trusted row object schema. All fields and schema_registry are required together. */
  const char *result_schema_name;
  const char *result_type_name;
  uint32_t result_schema_version;
  const turbo_flow_schema_registry_t *schema_registry;
  /** Optional typed row mapper; required to attach Batch<Object> projection. */
  const struct turbo_flow_pgsql_row_mapper_s *row_mapper;
} turbo_flow_pgsql_query_config_t;

typedef struct turbo_flow_pgsql_rowset_view_s turbo_flow_pgsql_rowset_view_t;

typedef int (*turbo_flow_pgsql_map_rows_fn)(void *ctx, const turbo_flow_pgsql_rowset_view_t *rows,
                                            const turbo_flow_data_schema_t *schema,
                                            void **projection_out);

typedef struct turbo_flow_pgsql_row_mapper_s {
  size_t size;
  turbo_flow_pgsql_map_rows_fn map;
  turbo_flow_projection_clone_fn clone;
  turbo_flow_destroy_fn destroy;
  void *ctx;
} turbo_flow_pgsql_row_mapper_t;

CXX_C_API size_t turbo_flow_pgsql_row_count(const turbo_flow_pgsql_rowset_view_t *rows);
CXX_C_API size_t turbo_flow_pgsql_column_count(const turbo_flow_pgsql_rowset_view_t *rows);
CXX_C_API const char *turbo_flow_pgsql_column_name(const turbo_flow_pgsql_rowset_view_t *rows,
                                                   size_t column);
CXX_C_API uint32_t turbo_flow_pgsql_column_oid(const turbo_flow_pgsql_rowset_view_t *rows,
                                               size_t column);
CXX_C_API int turbo_flow_pgsql_column_format(const turbo_flow_pgsql_rowset_view_t *rows,
                                             size_t column);
/** Borrowed cell bytes valid only during the mapper callback. */
CXX_C_API int turbo_flow_pgsql_cell(const turbo_flow_pgsql_rowset_view_t *rows, size_t row,
                                    size_t column, const char **data, size_t *len, int *is_null);

/**
 * Register a PostgreSQL transform for SELECT and CRUD/RETURNING.
 *
 * Tuple results replace the input with a bounded canonical JSON rowset or
 * strict DataBind CSV, according to `result_format`. Command-only results become a
 * `DatabaseCommandResult`. PGresult never crosses the adapter boundary.
 */
CXX_C_API int
turbo_flow_pgsql_register_query_adapter(turbo_flow_t *flow, const char *name,
                                        const turbo_flow_pgsql_query_config_t *config);

#define TURBO_FLOW_PGSQL_OUTBOX_API_VERSION 1u
#define TURBO_FLOW_PGSQL_OUTBOX_NAME_MAX 255u
#define TURBO_FLOW_PGSQL_OUTBOX_MAX_CAPACITY 1000000u
#define TURBO_FLOW_PGSQL_OUTBOX_MAX_PAYLOAD_SIZE (64u * 1024u * 1024u)
#define TURBO_FLOW_PGSQL_OUTBOX_MAX_CLAIM_SCAN 1024u

typedef enum turbo_flow_pgsql_outbox_role_e {
  TURBO_FLOW_PGSQL_OUTBOX_SINK = 1,
  TURBO_FLOW_PGSQL_OUTBOX_SOURCE = 2
} turbo_flow_pgsql_outbox_role_t;

/**
 * One PostgreSQL-backed durable outbox graph binding.
 *
 * Sink acceptance is reported only after COMMIT. Payload, message type/flags,
 * and an optional serializable protocol origin are committed together; live
 * process-local routes are never serialized. A source holds a PostgreSQL
 * session advisory lock while synchronously publishing one owned message and
 * deletes the row only after graph success. Connection loss releases the lock;
 * a crash after graph side effects and before DELETE can redeliver the row.
 */
typedef struct turbo_flow_pgsql_outbox_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_pgsql_outbox_role_t role;
  /** libpq connection string; copied and never exposed by status documents. */
  const char *conninfo;
  /** Stable logical queue name stored in the fixed TurboFlow outbox table. */
  const char *outbox_name;
  size_t capacity;
  size_t max_payload_size;
  /** Source poll interval; required for both roles so one channel has one contract. */
  uint32_t poll_interval_ms;
  /** Maximum candidate rows inspected per source poll. */
  size_t claim_scan_limit;
  /** Non-zero creates the fixed table/index; zero validates an existing schema. */
  int create_table;
} turbo_flow_pgsql_outbox_config_t;

#define TURBO_FLOW_PGSQL_OUTBOX_CONFIG_INIT                                                        \
  {sizeof(turbo_flow_pgsql_outbox_config_t),                                                       \
   TURBO_FLOW_PGSQL_OUTBOX_API_VERSION,                                                            \
   TURBO_FLOW_PGSQL_OUTBOX_SINK,                                                                   \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0}

/** Register one source or sink against the fixed PostgreSQL outbox schema. */
CXX_C_API int
turbo_flow_pgsql_register_outbox_adapter(turbo_flow_t *flow, const char *name,
                                         const turbo_flow_pgsql_outbox_config_t *config);

/**
 * Register an outbox adapter from strict YAML.
 *
 * The adapter must be `kind: pgsql_outbox` with `{channel, role}`. The channel
 * must be `kind: outbox`, `backend: postgresql`; unknown or cross-role fields
 * fail before adapter registration.
 */
CXX_C_API int
turbo_flow_pgsql_register_resolved_outbox_adapter(turbo_flow_t *flow, const char *name,
                                                  const turbo_flow_resolved_config_t *resolved,
                                                  turbo_flow_config_error_t *error);

#define TURBO_FLOW_PGSQL_RECORD_STORE_API_VERSION 1u
#define TURBO_FLOW_PGSQL_RECORD_STORE_NAMESPACE_MAX 255u
#define TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_KEY_SIZE 65538u
#define TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE (16u * 1024u * 1024u)
#define TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE 4096u
#define TURBO_FLOW_PGSQL_RECORD_STORE_MAX_VALUE_SIZE (64u * 1024u * 1024u)
#define TURBO_FLOW_PGSQL_RECORD_STORE_MAX_RECORDS 1000000u

/**
 * One PostgreSQL-backed record namespace for durable protocol state.
 *
 * The provider serializes commits for one namespace with a transaction advisory
 * lock. Revision checks, capacity validation, and all mutations occur in the
 * same transaction. The caller serializes scan/commit calls.
 */
typedef struct turbo_flow_pgsql_record_store_config_s {
  size_t size;
  uint32_t version;
  /** libpq connection string; copied by the provider. */
  const char *conninfo;
  /** Stable namespace stored in the fixed TurboFlow record table. */
  const char *namespace_name;
  /** Zero selects TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_KEY_SIZE. */
  size_t max_key_size;
  /** Zero selects TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE. */
  size_t max_value_size;
  /** Zero selects TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE. */
  size_t max_batch_size;
  /** Required maximum record count for bounded scans and commits. */
  size_t max_records;
  /** Non-zero creates the fixed table; zero validates an existing schema. */
  int create_table;
} turbo_flow_pgsql_record_store_config_t;

#define TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT                                                  \
  {sizeof(turbo_flow_pgsql_record_store_config_t),                                                 \
   TURBO_FLOW_PGSQL_RECORD_STORE_API_VERSION,                                                      \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PGSQL_H */
