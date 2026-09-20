#ifndef TURBO_FLOW_TURBODB_H
#define TURBO_FLOW_TURBODB_H

#include "turbo_flow.h"
#include "turbo_flow_inbox.h"

#include <orm.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_TURBODB_API_VERSION UINT32_C(1)

/**
 * Message metadata and trusted schema for one typed TurboDb Publisher.
 *
 * The projection schema, its strings, and the CMeta descriptor named by
 * projection_type are borrowed and must outlive the Publisher and every
 * emitted message or clone. The schema must describe DATA-domain values and
 * projection_type must equal the wrapped Publisher's CMeta type name.
 *
 * @code{.c}
 * static int open_rows(orm_query_t *query, const orm_flow_config_t *flow,
 *                      const turbo_flow_data_schema_t *schema,
 *                      orm_error_t *error, cflow_publisher *out) {
 *   turbo_flow_turbodb_source_config_t source =
 *       turbo_flow_turbodb_source_config_default();
 *   source.projection_schema = schema;
 *   return turbo_flow_turbodb_query_open(query, flow, &source, out, error);
 * }
 * @endcode
 */
typedef struct turbo_flow_turbodb_source_config_s {
  size_t size;
  uint32_t version;
  const turbo_flow_data_schema_t *projection_schema;
  uint64_t first_message_id;
  uint32_t message_type;
  uint32_t message_flags;
} turbo_flow_turbodb_source_config_t;

/**
 * Return a complete versioned configuration with message IDs starting at 1.
 * projection_schema remains NULL and must be set before open/wrap.
 */
TURBO_FLOW_C_API turbo_flow_turbodb_source_config_t turbo_flow_turbodb_source_config_default(void);

/**
 * Move a typed, constructing CFlow Publisher into a TurboFlow message Publisher.
 *
 * On success typed_publisher is cleared and message_publisher owns the adapter.
 * A validation or allocation failure leaves typed_publisher live and
 * message_publisher empty. Each row is owned by its emitted message projection.
 * WAIT, wakers, cancellation, and terminal state are forwarded without polling
 * or materializing the full result set.
 *
 * @param typed_publisher Live constructing Publisher; moved only on success.
 * @param config Borrowed, validated source configuration.
 * @param message_publisher Zero-initialized output Publisher.
 * @return SALTS_OK, SALTS_EINVAL for an invalid contract, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_publisher_wrap(cflow_publisher *typed_publisher,
                                  const turbo_flow_turbodb_source_config_t *config,
                                  cflow_publisher *message_publisher);

/**
 * Open a native ORM row Publisher and move it into the message adapter.
 *
 * query, its connection, orm_config->row_shape, reachable CMeta metadata, and
 * source_config remain borrowed for the returned Publisher/message lifetimes.
 * Adapter validation does not modify orm_error; an ORM open failure does.
 *
 * @param query Live ORM query.
 * @param orm_config Borrowed native flow configuration and row shape.
 * @param source_config Borrowed message/projection configuration.
 * @param message_publisher Zero-initialized output Publisher.
 * @param orm_error Caller-initialized detailed ORM error storage.
 * @return SALTS_OK; SALTS_EINVAL/SALTS_ENOMEM for adapter failures; or a
 * mapped Salts category for the detailed ORM error.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_query_open(orm_query_t *query, const orm_flow_config_t *orm_config,
                              const turbo_flow_turbodb_source_config_t *source_config,
                              cflow_publisher *message_publisher, orm_error_t *orm_error);

/**
 * Transaction-scoped row form with the same errors and ownership as query_open.
 * transaction must also outlive the returned Publisher.
 */
TURBO_FLOW_C_API int turbo_flow_turbodb_query_open_in_transaction(
    orm_query_t *query, orm_transaction_t *transaction, const orm_flow_config_t *orm_config,
    const turbo_flow_turbodb_source_config_t *source_config, cflow_publisher *message_publisher,
    orm_error_t *orm_error);

/**
 * Open a native ORM command-result Publisher and move it into the adapter.
 * Command execution remains deferred until downstream demand resumes it.
 * query/source_config stay borrowed; orm_error receives ORM open diagnostics.
 *
 * @return SALTS_OK; SALTS_EINVAL/SALTS_ENOMEM for adapter failures; or a
 * mapped Salts category for the detailed ORM error.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_command_open(orm_query_t *query,
                                const turbo_flow_turbodb_source_config_t *source_config,
                                cflow_publisher *message_publisher, orm_error_t *orm_error);

/**
 * Transaction-scoped command form with the same errors and ownership as
 * command_open. transaction must outlive the returned Publisher.
 */
TURBO_FLOW_C_API int turbo_flow_turbodb_command_open_in_transaction(
    orm_query_t *query, orm_transaction_t *transaction,
    const turbo_flow_turbodb_source_config_t *source_config, cflow_publisher *message_publisher,
    orm_error_t *orm_error);

#define TURBO_FLOW_TURBODB_INBOX_API_VERSION UINT32_C(1)
#define TURBO_FLOW_TURBODB_INBOX_SCHEMA_VERSION UINT32_C(3)
#define TURBO_FLOW_TURBODB_INBOX_DEFAULT_CONNECTIONS 4u
#define TURBO_FLOW_TURBODB_INBOX_MAX_CONNECTIONS 64u
#define TURBO_FLOW_TURBODB_INBOX_NAMESPACE_MAX 63u

/**
 * Exact v3 durable inbox configuration.
 *
 * `database` and every view reachable from it are borrowed only during create;
 * successful create owns all opened ORM connections. `namespace_name` is also
 * borrowed only during create and must match `[A-Za-z_][A-Za-z0-9_]*`. It maps
 * to `<namespace_name>_inbox_meta_v3` and
 * `<namespace_name>_inbox_records_v3`.
 *
 * Version 3 supports only TurboDB's file-backed SQLite driver with a durable
 * rollback/WAL journal and FULL-or-stronger synchronization. Other drivers,
 * `:memory:`, journal OFF/MEMORY, and weaker synchronization are rejected with
 * `SALTS_ENOTSUP`; each additional backend requires its own transaction and
 * failure verification before admission.
 *
 * The two tables and exactly one initialized metadata row must already exist.
 * Create validates the current schema and data invariants but never creates,
 * migrates, repairs, or deletes database objects. Record and byte limits count
 * both live rows and terminal idempotency tombstones. Tombstones remain until
 * explicit `turbo_flow_inbox_forget()`; exact replay still resolves after
 * close. Limits are hard admission and lease-cache bounds. There is no
 * alternate provider or memory fallback.
 */
typedef enum turbo_flow_turbodb_inbox_open_mode_e {
  /** Acquire only a cleanly CLOSED namespace; an ACTIVE owner returns busy. */
  TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE = 0,
  /** Fence exactly expected_generation after an upper coordinator authorizes takeover. */
  TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER = 1
} turbo_flow_turbodb_inbox_open_mode_t;

typedef struct turbo_flow_turbodb_inbox_config_s {
  size_t size;
  uint32_t version;
  const orm_config_t *database;
  const char *namespace_name;
  size_t max_records;
  size_t max_total_bytes;
  size_t max_record_bytes;
  size_t max_claims;
  uint32_t connection_count;
  turbo_flow_turbodb_inbox_open_mode_t open_mode;
  /** Required and non-zero only for OPEN_TAKEOVER. */
  uint64_t expected_generation;
} turbo_flow_turbodb_inbox_config_t;

/**
 * Return finite defaults with database/namespace left unset.
 *
 * @return Exact-version configuration using the unified memory-provider
 * capacity defaults and four ORM connections.
 */
TURBO_FLOW_C_API turbo_flow_turbodb_inbox_config_t turbo_flow_turbodb_inbox_config_default(void);

/**
 * Open a durable provider into an empty unified inbox handle.
 *
 * Exclusive open advances generation only from a cleanly CLOSED namespace.
 * Explicit takeover requires an exact coordinator-provided generation, fences
 * that owner, and marks its `CLAIMED` rows FAILED with
 * `TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN`; it never requeues them.
 * Callers discover those IDs with `turbo_flow_inbox_scan_failed()` and choose
 * retry/discard explicitly. A stale provider returns `SALTS_EBUSY` for reads
 * and ordinary mutations. Settling one of its old leases returns
 * `SALTS_ECANCELED`, invalidates that lease, and never reports business
 * completion. Calls may be concurrent; an exhausted finite connection pool or
 * SQLite lock contention returns `SALTS_EBUSY`.
 *
 * @param config Exact-version configuration with a valid ORM config and
 * pre-provisioned v2 namespace.
 * @param out Exact-version empty `TURBO_FLOW_INBOX_INIT` handle receiving the
 * provider vtable and ownership.
 * @param orm_error Detailed error for an ORM boundary failure; initialized by
 * this function before first use. It is not used to hide the returned Salts
 * status.
 * @return SALTS_OK; SALTS_EINVAL for invalid ABI/config; SALTS_EPROTO for a
 * missing, old, or inconsistent schema/data contract; SALTS_ENOSPC when stored
 * state exceeds configured bounds; SALTS_ENOMEM for owner allocation; or the
 * mapped ORM failure. No failure selects another provider.
 *
 * @code{.c}
 * turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
 * turbo_flow_turbodb_inbox_config_t cfg =
 *     turbo_flow_turbodb_inbox_config_default();
 * cfg.database = &database;
 * cfg.namespace_name = "orders";
 * int rc = turbo_flow_turbodb_inbox_create(&cfg, &inbox, &error);
 * @endcode
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_inbox_create(const turbo_flow_turbodb_inbox_config_t *config,
                                turbo_flow_inbox_t *out, orm_error_t *orm_error);

#define TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION UINT32_C(1)
#define TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_FETCH_COUNT 16u
#define TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_IN_FLIGHT_MESSAGES 64u
#define TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_IN_FLIGHT_BYTES (16u * 1024u * 1024u)
#define TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_MAX_IDENTITY_BYTES 256u
#define TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_MAX_PAYLOAD_BYTES (1024u * 1024u)
#define TURBO_FLOW_TURBODB_OUTBOX_DEFAULT_MAX_DELIVERY_ATTEMPTS 32u

typedef struct turbo_flow_turbodb_outbox_source_s turbo_flow_turbodb_outbox_source_t;

/**
 * Borrowed receipt returned by one provider fetch step.
 *
 * Every byte view remains valid only until fetch returns. A RECORD transfers
 * one active token to the adapter; the provider must keep that token valid
 * until acknowledge, requeue, or dead_letter followed by acknowledge succeeds.
 * A WAIT waitable's cancel operation is a quiescent boundary: after cancel
 * returns, it must neither retain nor invoke the previously armed waker.
 */
typedef struct turbo_flow_turbodb_outbox_record_s {
  size_t size;
  uint32_t version;
  uint64_t token;
  uint64_t raft_index;
  uint64_t term;
  uint32_t delivery_attempt;
  vstr identity;
  vstr payload;
} turbo_flow_turbodb_outbox_record_t;

#define TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT                                                      \
  {sizeof(turbo_flow_turbodb_outbox_record_t),                                                     \
   TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION,                                                   \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u}}

/**
 * Exact remaining admission budget observed by a non-blocking provider fetch.
 *
 * A provider may internally obtain at most max_records, but returns one record
 * per callback and drains that private batch before opening another fetch.
 */
typedef struct turbo_flow_turbodb_outbox_fetch_budget_s {
  size_t size;
  uint32_t version;
  size_t max_records;
  size_t max_retained_bytes;
  size_t max_identity_bytes;
  size_t max_payload_bytes;
} turbo_flow_turbodb_outbox_fetch_budget_t;

typedef enum turbo_flow_turbodb_outbox_fetch_kind_e {
  TURBO_FLOW_TURBODB_OUTBOX_FETCH_IDLE = 0,
  TURBO_FLOW_TURBODB_OUTBOX_FETCH_WAIT,
  TURBO_FLOW_TURBODB_OUTBOX_FETCH_RECORD,
  TURBO_FLOW_TURBODB_OUTBOX_FETCH_DATA_LOSS,
  TURBO_FLOW_TURBODB_OUTBOX_FETCH_ERROR
} turbo_flow_turbodb_outbox_fetch_kind_t;

typedef struct turbo_flow_turbodb_outbox_fetch_step_s {
  size_t size;
  uint32_t version;
  turbo_flow_turbodb_outbox_fetch_kind_t kind;
  /** IDLE, WAIT, and RECORD require SALTS_OK; error kinds carry their cause. */
  int status;
  cflow_waitable waitable;
  turbo_flow_turbodb_outbox_record_t record;
} turbo_flow_turbodb_outbox_fetch_step_t;

#define TURBO_FLOW_TURBODB_OUTBOX_FETCH_STEP_INIT                                                  \
  {sizeof(turbo_flow_turbodb_outbox_fetch_step_t),                                                 \
   TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION,                                                   \
   TURBO_FLOW_TURBODB_OUTBOX_FETCH_IDLE,                                                           \
   SALTS_OK,                                                                                       \
   {0},                                                                                            \
   TURBO_FLOW_TURBODB_OUTBOX_RECORD_INIT}

typedef turbo_flow_turbodb_outbox_fetch_step_t (*turbo_flow_turbodb_outbox_fetch_fn)(
    void *ctx, const turbo_flow_turbodb_outbox_fetch_budget_t *budget);
typedef int (*turbo_flow_turbodb_outbox_cancel_fetch_fn)(void *ctx);
typedef int (*turbo_flow_turbodb_outbox_settle_fn)(void *ctx, uint64_t token);

/** Message-owned receipt metadata visible to Graph stages. */
typedef struct turbo_flow_turbodb_outbox_message_context_s {
  size_t size;
  uint32_t version;
  uint64_t raft_index;
  uint64_t term;
  uint32_t delivery_attempt;
  size_t identity_size;
} turbo_flow_turbodb_outbox_message_context_t;

typedef int (*turbo_flow_turbodb_outbox_dead_letter_fn)(
    void *ctx, uint64_t token, const turbo_flow_turbodb_outbox_message_context_t *receipt,
    int graph_status);

typedef struct turbo_flow_turbodb_outbox_provider_ops_s {
  size_t size;
  turbo_flow_turbodb_outbox_fetch_fn fetch;
  turbo_flow_turbodb_outbox_cancel_fetch_fn cancel_fetch;
  turbo_flow_turbodb_outbox_settle_fn acknowledge;
  turbo_flow_turbodb_outbox_settle_fn requeue;
  turbo_flow_turbodb_outbox_dead_letter_fn dead_letter;
} turbo_flow_turbodb_outbox_provider_ops_t;

#define TURBO_FLOW_TURBODB_OUTBOX_PROVIDER_OPS_INIT                                                \
  {sizeof(turbo_flow_turbodb_outbox_provider_ops_t), NULL, NULL, NULL, NULL, NULL}

typedef enum turbo_flow_turbodb_outbox_failure_disposition_e {
  TURBO_FLOW_TURBODB_OUTBOX_FAILURE_RETRYABLE = 0,
  TURBO_FLOW_TURBODB_OUTBOX_FAILURE_PERMANENT
} turbo_flow_turbodb_outbox_failure_disposition_t;

typedef turbo_flow_turbodb_outbox_failure_disposition_t (
    *turbo_flow_turbodb_outbox_failure_classify_fn)(
    void *ctx, const turbo_flow_turbodb_outbox_message_context_t *receipt, int graph_status);

typedef enum turbo_flow_turbodb_outbox_permanent_failure_policy_e {
  TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_FAIL_SOURCE = 0,
  TURBO_FLOW_TURBODB_OUTBOX_PERMANENT_DEAD_LETTER
} turbo_flow_turbodb_outbox_permanent_failure_policy_t;

typedef enum turbo_flow_turbodb_outbox_shutdown_policy_e {
  TURBO_FLOW_TURBODB_OUTBOX_SHUTDOWN_REQUEUE = 0
} turbo_flow_turbodb_outbox_shutdown_policy_t;

typedef struct turbo_flow_turbodb_outbox_source_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_t *flow;
  const char *source_name;
  /** Borrowed by every in-flight Graph run; NULL selects the Flow-owned scheduler. */
  cflow_scheduler *scheduler;
  void *provider_ctx;
  turbo_flow_turbodb_outbox_provider_ops_t provider;
  turbo_flow_turbodb_outbox_failure_classify_fn classify_failure;
  void *policy_ctx;
  turbo_flow_turbodb_outbox_permanent_failure_policy_t permanent_failure_policy;
  turbo_flow_turbodb_outbox_shutdown_policy_t shutdown_policy;
  size_t fetch_count;
  size_t in_flight_messages;
  size_t in_flight_bytes;
  size_t max_identity_bytes;
  size_t max_payload_bytes;
  uint32_t max_delivery_attempts;
  uint64_t first_message_id;
  uint32_t message_type;
  uint32_t message_flags;
} turbo_flow_turbodb_outbox_source_config_t;

typedef enum turbo_flow_turbodb_outbox_source_state_e {
  TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING = 0,
  TURBO_FLOW_TURBODB_OUTBOX_SOURCE_WAITING,
  TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPING,
  TURBO_FLOW_TURBODB_OUTBOX_SOURCE_STOPPED,
  TURBO_FLOW_TURBODB_OUTBOX_SOURCE_FAILED
} turbo_flow_turbodb_outbox_source_state_t;

typedef struct turbo_flow_turbodb_outbox_source_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_turbodb_outbox_source_state_t state;
  int status;
  size_t outstanding_demand;
  size_t in_flight_messages;
  size_t in_flight_bytes;
  uint64_t fetched;
  uint64_t acknowledged;
  uint64_t requeued;
  uint64_t dead_lettered;
  uint64_t data_loss_events;
  char error_stage[48];
} turbo_flow_turbodb_outbox_source_snapshot_t;

#define TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT                                             \
  {sizeof(turbo_flow_turbodb_outbox_source_snapshot_t),                                            \
   TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION,                                                   \
   TURBO_FLOW_TURBODB_OUTBOX_SOURCE_RUNNING,                                                       \
   SALTS_OK,                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {0}}

/** Return finite defaults; flow, source_name, callbacks, and contexts remain unset. */
TURBO_FLOW_C_API turbo_flow_turbodb_outbox_source_config_t
turbo_flow_turbodb_outbox_source_config_default(void);

/**
 * Open one owner-thread-affine, non-blocking outbox Source.
 *
 * The configuration and provider vtable are copied; flow, scheduler,
 * provider_ctx, and policy_ctx are borrowed through successful destroy.
 * No provider callback runs during open. The Flow must already be STARTED.
 *
 * @param config Immutable bounded source contract.
 * @param source_out Receives the owned source and is cleared before validation.
 * @return SALTS_OK, SALTS_EINVAL for invalid ABI/config/lifecycle, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_outbox_source_open(const turbo_flow_turbodb_outbox_source_config_t *config,
                                      turbo_flow_turbodb_outbox_source_t **source_out);

/** Add finite downstream demand with saturating arithmetic; zero is invalid. */
TURBO_FLOW_C_API int
turbo_flow_turbodb_outbox_source_request(turbo_flow_turbodb_outbox_source_t *source, size_t demand);

/**
 * Perform at most max_steps owner-context fetch or slot-settlement transitions.
 * Provider callback failures preserve the active token and may be retried by
 * a later poll with the same idempotent operation. Non-terminal Graph request
 * rejection preserves the token for requeue and returns the rejection status.
 * Dead-letter and its following acknowledgement are always separate steps.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_outbox_source_poll(turbo_flow_turbodb_outbox_source_t *source, size_t max_steps);
/** Copy counters and the first current error stage without advancing work. */
TURBO_FLOW_C_API int
turbo_flow_turbodb_outbox_source_snapshot(const turbo_flow_turbodb_outbox_source_t *source,
                                          turbo_flow_turbodb_outbox_source_snapshot_t *snapshot);
/**
 * Stop admission, cancel an armed fetch and every live Graph run, then requeue
 * all unsettled claims. A provider error leaves STOPPING state for an exact retry.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_outbox_source_stop(turbo_flow_turbodb_outbox_source_t *source);
/** Release a successfully stopped source; a live owner returns SALTS_EBUSY. */
TURBO_FLOW_C_API int
turbo_flow_turbodb_outbox_source_destroy(turbo_flow_turbodb_outbox_source_t *source);
/** Validate and borrow message-owned receipt metadata, or return NULL. */
TURBO_FLOW_C_API const turbo_flow_turbodb_outbox_message_context_t *
turbo_flow_turbodb_outbox_message_context(const turbo_flow_msg_t *message);
/** Validate and borrow the stable outbox identity, or return an empty view. */
TURBO_FLOW_C_API vstr turbo_flow_turbodb_outbox_message_identity(const turbo_flow_msg_t *message);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_TURBODB_H */
