#ifndef FLOWIE_CONTROL_PGSQL_DATABASE_INTERNAL_H
#define FLOWIE_CONTROL_PGSQL_DATABASE_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;

#define FLOWIE_CONTROL_PGSQL_DATABASE_VERSION 1u
#define FLOWIE_CONTROL_PGSQL_SCHEMA_VERSION 3u
#define FLOWIE_CONTROL_PGSQL_SCHEMA_NAME_MAX 63u
#define FLOWIE_CONTROL_PGSQL_CONNINFO_MAX 4096u
#define FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS 60000
#define FLOWIE_CONTROL_PGSQL_POOL_VERSION 1u
#define FLOWIE_CONTROL_PGSQL_POOL_CAPACITY_MAX 64u

typedef struct flowie_control_pgsql_database_s flowie_control_pgsql_database_t;
typedef struct flowie_control_pgsql_pool_s flowie_control_pgsql_pool_t;

typedef enum flowie_control_pgsql_schema_mode_e {
  FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE = 0,
  FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE = 1
} flowie_control_pgsql_schema_mode_t;

typedef struct flowie_control_pgsql_database_config_s {
  size_t size;
  uint32_t version;
  const char *conninfo;
  /** Optional separately supplied password; overrides conninfo and is copied/wiped by the pool. */
  const char *password;
  const char *schema_name;
  int connect_timeout_seconds;
  int statement_timeout_ms;
  int lock_timeout_ms;
  /** Require an effective libpq sslmode of verify-full and an active TLS session. */
  int require_tls;
  flowie_control_pgsql_schema_mode_t schema_mode;
} flowie_control_pgsql_database_config_t;

#define FLOWIE_CONTROL_PGSQL_DATABASE_CONFIG_INIT                                                  \
  {sizeof(flowie_control_pgsql_database_config_t),                                                 \
   FLOWIE_CONTROL_PGSQL_DATABASE_VERSION,                                                          \
   NULL,                                                                                           \
   NULL,                                                                                           \
   "flowie_control",                                                                               \
   5,                                                                                              \
   5000,                                                                                           \
   5000,                                                                                           \
   1,                                                                                              \
   FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE}

typedef struct flowie_control_pgsql_pool_config_s {
  size_t size;
  uint32_t version;
  flowie_control_pgsql_database_config_t database;
  size_t capacity;
  int acquire_timeout_ms;
} flowie_control_pgsql_pool_config_t;

#define FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT                                                      \
  {sizeof(flowie_control_pgsql_pool_config_t), FLOWIE_CONTROL_PGSQL_POOL_VERSION,                  \
   FLOWIE_CONTROL_PGSQL_DATABASE_CONFIG_INIT, 4u, 5000}

/**
 * One exclusive pool lease.
 *
 * The fields are an internal ownership token and must not be copied or modified. A successful
 * acquire must reach exactly one release. The PGconn borrowed from the lease becomes invalid at
 * release, pool close, or pool destruction.
 */
typedef struct flowie_control_pgsql_pool_lease_s {
  flowie_control_pgsql_pool_t *owner;
  PGconn *connection;
  size_t slot;
  uint64_t generation;
} flowie_control_pgsql_pool_lease_t;

#define FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT {NULL, NULL, 0u, 0u}

typedef struct flowie_control_pgsql_pool_stats_s {
  size_t size;
  size_t capacity;
  size_t healthy;
  size_t available;
  size_t leased;
  size_t waiters;
  uint64_t acquisition_timeouts;
  uint64_t cleanup_failures;
  int closing;
} flowie_control_pgsql_pool_stats_t;

#define FLOWIE_CONTROL_PGSQL_POOL_STATS_INIT                                                       \
  {sizeof(flowie_control_pgsql_pool_stats_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0}

/**
 * Open one startup/migration session.
 *
 * The object owns its libpq connection and copied configuration. It is not thread-safe and must
 * not be used as the future request connection pool. `MIGRATE` takes a transaction-scoped advisory
 * lock and applies monotonic schema migrations; `VALIDATE` performs no DDL.
 */
int flowie_control_pgsql_database_open(const flowie_control_pgsql_database_config_t *config,
                                       flowie_control_pgsql_database_t **out);

void flowie_control_pgsql_database_destroy(flowie_control_pgsql_database_t *database);

/** Return the validated schema version observed during open. */
uint32_t
flowie_control_pgsql_database_schema_version(const flowie_control_pgsql_database_t *database);

/** Map a five-byte PostgreSQL SQLSTATE to the repository error vocabulary. */
int flowie_control_pgsql_sqlstate_status(const char *sqlstate);

/**
 * Validate a public non-secret conninfo.
 *
 * Requires an explicit sslmode=verify-full and rejects password, passfile, sslpassword, service,
 * and servicefile so authentication material can only enter through the separate password field.
 */
int flowie_control_pgsql_public_conninfo_validate(const char *conninfo);

/** Validate one libpq result status and map its SQLSTATE on failure. */
int flowie_control_pgsql_result_status(PGresult *result, int expected_status);

/**
 * Create a bounded MPMC connection pool.
 *
 * Every slot owns one independently validated libpq connection. `MIGRATE` is applied only by the
 * first slot; the remaining slots validate that same schema. Creation is atomic: failure to open
 * any configured slot destroys the whole pool.
 */
int flowie_control_pgsql_pool_create(const flowie_control_pgsql_pool_config_t *config,
                                     flowie_control_pgsql_pool_t **out);

/**
 * Acquire one exclusive connection, waiting at most the configured acquire timeout.
 *
 * When no slot is available and dead capacity exists, acquire coalesces one asynchronous reopen
 * request and waits within the same deadline. Returns TURBO_ETIMEDOUT when capacity remains busy
 * or recovery exceeds that deadline, TURBO_EIO when the requested recovery fails and no healthy
 * slot remains, and TURBO_ESHUTDOWN after close begins.
 */
int flowie_control_pgsql_pool_acquire(flowie_control_pgsql_pool_t *pool,
                                      flowie_control_pgsql_pool_lease_t *lease);

PGconn *flowie_control_pgsql_pool_lease_connection(const flowie_control_pgsql_pool_lease_t *lease);

/** Copy a lock-consistent bounded-resource snapshot into caller-owned storage. */
int flowie_control_pgsql_pool_stats(flowie_control_pgsql_pool_t *pool,
                                    flowie_control_pgsql_pool_stats_t *out);

/** Borrow the validated schema name until pool destruction. */
const char *flowie_control_pgsql_pool_schema_name(const flowie_control_pgsql_pool_t *pool);

/**
 * Return a lease and clean the session before making its slot available.
 *
 * Open or failed transactions are rolled back. A connection that cannot return to the idle state
 * is reopened and schema-validated before reuse. Reopen failure removes that slot from the healthy
 * capacity and returns the cleanup error.
 */
int flowie_control_pgsql_pool_release(flowie_control_pgsql_pool_lease_t *lease);

/**
 * Stop new acquisitions and wait for waiters and leases to leave the pool.
 *
 * Timeout leaves the pool in closing state; callers may release outstanding leases and retry.
 */
int flowie_control_pgsql_pool_close(flowie_control_pgsql_pool_t *pool, int timeout_ms);

/** Destroy a closed, quiescent pool; returns TURBO_EBUSY if close has not completed. */
int flowie_control_pgsql_pool_destroy(flowie_control_pgsql_pool_t *pool);

#ifdef __cplusplus
}
#endif

#endif
