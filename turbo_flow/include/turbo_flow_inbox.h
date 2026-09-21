#ifndef TURBO_FLOW_INBOX_H
#define TURBO_FLOW_INBOX_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_INBOX_API_VERSION UINT32_C(4)
#define TURBO_FLOW_INBOX_RECORD_SCHEMA "turbo-flow.inbox.record"
#define TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION UINT32_C(3)

#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_RECORDS 1024u
#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_TOTAL_BYTES (64u * 1024u * 1024u)
#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_RECORD_BYTES (1024u * 1024u)
#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_CLAIMS 64u
#define TURBO_FLOW_INBOX_MEMORY_MAX_RECORDS 1048576u
#define TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES (SIZE_MAX / 2u)

typedef struct turbo_flow_inbox_s turbo_flow_inbox_t;
typedef struct turbo_flow_inbox_ops_v2_s turbo_flow_inbox_ops_v2_t;

/**
 * Copy-only Source admission value.
 *
 * All pointers are borrowed for the call. A successful admit makes an
 * independent provider-owned copy; every failure leaves caller ownership
 * unchanged. Only the exact current envelope schema is accepted.
 */
typedef struct turbo_flow_inbox_record_s {
  size_t size;
  uint32_t version;
  const char *envelope_schema;
  uint32_t envelope_schema_version;
  /** Stable configured Source identity; non-empty and included in admission identity. */
  vstr source_id;
  /** Canonical persisted partition key used by all partitioned claim modes. */
  vstr partition_key;
  /** Stable per-Source admission identity; non-empty and safe to replay verbatim. */
  vstr admission_id;
  uint64_t source_sequence;
  uint64_t timestamp_ns;
  uint32_t message_type;
  uint32_t message_flags;
  turbo_flow_content_descriptor_t content;
  vstr correlation;
  vstr payload;
} turbo_flow_inbox_record_t;

/** Initialize a record with the only accepted envelope schema and version. */
TURBO_FLOW_C_API void turbo_flow_inbox_record_init(turbo_flow_inbox_record_t *record);

typedef struct turbo_flow_inbox_receipt_s {
  size_t size;
  uint32_t version;
  uint64_t record_id;
} turbo_flow_inbox_receipt_t;

#define TURBO_FLOW_INBOX_RECEIPT_INIT                                                              \
  {sizeof(turbo_flow_inbox_receipt_t), TURBO_FLOW_INBOX_API_VERSION, 0u}

/**
 * Move-only claim lease.
 *
 * `record` and its byte views are immutable and borrowed from the provider
 * until complete/fail returns either SALTS_OK or SALTS_ECANCELED. Cancellation
 * means a durable owner takeover moved the record to OWNER_LOST_UNKNOWN; both
 * outcomes invalidate the lease. Copying this value does not create another
 * claim; stale or concurrently settling copies are rejected by record ID and
 * claim token.
 */
typedef struct turbo_flow_inbox_claim_s {
  size_t size;
  uint32_t version;
  uint64_t record_id;
  uint64_t claim_token;
  turbo_flow_inbox_record_t record;
} turbo_flow_inbox_claim_t;

#define TURBO_FLOW_INBOX_CLAIM_INIT                                                                \
  {sizeof(turbo_flow_inbox_claim_t), TURBO_FLOW_INBOX_API_VERSION, 0u, 0u, {0}}

#define TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_PARTITIONS 64u
#define TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_BYTES (64u * 1024u)

typedef enum turbo_flow_inbox_claim_ordering_e {
  /** Preserve the existing total FIFO order. No partition exclusions are allowed. */
  TURBO_FLOW_INBOX_CLAIM_ORDER_GLOBAL = 0,
  /**
   * Skip pending records whose persisted canonical partition_key appears in
   * excluded_partitions. The oldest eligible record wins.
   */
  TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY = 1,
  /** Compatibility name for the original source_id-only coordinator slice. */
  TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_SOURCE_ID = TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY
} turbo_flow_inbox_claim_ordering_t;

/**
 * Exact-version, caller-owned selector for one non-blocking claim.
 *
 * excluded_partitions is borrowed for the call. It is valid only for
 * PARTITION_KEY ordering, is explicitly bounded, and may not contain
 * empty or duplicate keys. An excluded record remains PENDING; no provider
 * counter or claim token advances for a skipped partition.
 */
typedef struct turbo_flow_inbox_claim_request_s {
  size_t size;
  uint32_t version;
  turbo_flow_inbox_claim_ordering_t ordering;
  const vstr *excluded_partitions;
  size_t excluded_partition_count;
} turbo_flow_inbox_claim_request_t;

#define TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT                                                        \
  {sizeof(turbo_flow_inbox_claim_request_t), TURBO_FLOW_INBOX_API_VERSION,                         \
   TURBO_FLOW_INBOX_CLAIM_ORDER_GLOBAL, NULL, 0u}

typedef struct turbo_flow_inbox_snapshot_s {
  size_t size;
  uint32_t version;
  /** Provider ownership generation; memory providers use generation one. */
  uint64_t generation;
  int accepting;
  size_t records;
  /** Terminal idempotency tombstones retained until explicit forget/destroy. */
  size_t history_records;
  size_t pending_records;
  size_t failed_records;
  size_t in_flight_claims;
  size_t retained_bytes;
  uint64_t admitted;
  uint64_t completed;
  uint64_t failed;
  uint64_t retried;
  uint64_t discarded;
} turbo_flow_inbox_snapshot_t;

#define TURBO_FLOW_INBOX_SNAPSHOT_INIT                                                             \
  {sizeof(turbo_flow_inbox_snapshot_t), TURBO_FLOW_INBOX_API_VERSION, 0u, 0, 0u}

typedef enum turbo_flow_inbox_failure_kind_e {
  TURBO_FLOW_INBOX_FAILURE_PROCESSING = 1,
  /** The previous durable owner ended without a known Graph outcome. */
  TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN = 2
} turbo_flow_inbox_failure_kind_t;

/** Caller-owned, read-only failed-record index entry. */
typedef struct turbo_flow_inbox_failed_entry_s {
  size_t size;
  uint32_t version;
  uint64_t record_id;
  int status;
  turbo_flow_inbox_failure_kind_t kind;
} turbo_flow_inbox_failed_entry_t;

#define TURBO_FLOW_INBOX_FAILED_ENTRY_INIT                                                         \
  {sizeof(turbo_flow_inbox_failed_entry_t), TURBO_FLOW_INBOX_API_VERSION, 0u, SALTS_OK,            \
   TURBO_FLOW_INBOX_FAILURE_PROCESSING}

typedef enum turbo_flow_inbox_terminal_kind_e {
  TURBO_FLOW_INBOX_TERMINAL_COMPLETED = 1,
  TURBO_FLOW_INBOX_TERMINAL_DISCARDED = 2
} turbo_flow_inbox_terminal_kind_t;

/** Caller-owned, read-only terminal idempotency-history entry. */
typedef struct turbo_flow_inbox_history_entry_s {
  size_t size;
  uint32_t version;
  uint64_t record_id;
  turbo_flow_inbox_terminal_kind_t kind;
} turbo_flow_inbox_history_entry_t;

#define TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT                                                        \
  {sizeof(turbo_flow_inbox_history_entry_t), TURBO_FLOW_INBOX_API_VERSION, 0u,                     \
   TURBO_FLOW_INBOX_TERMINAL_COMPLETED}

/**
 * Versioned provider vtable used by in-process and configured DLL providers.
 *
 * The host owns the move-only `turbo_flow_inbox_t`; `ctx` and this vtable must
 * remain valid until destroy succeeds. Provider-owned allocation is released
 * only by its destroy callback. No callback may select another provider.
 *
 * Calls other than destroy may arrive concurrently and the provider must
 * serialize its own mutable state. The host calls destroy only after excluding
 * all concurrent calls. admit/claim/snapshot receive initialized exact-version
 * outputs and must preserve size/version. A failed callback must not transfer
 * ownership or publish a partial output. Provider callbacks never call back
 * into the host while holding their state lock.
 */
struct turbo_flow_inbox_ops_v2_s {
  size_t size;
  uint32_t version;
  int (*admit)(void *ctx, const turbo_flow_inbox_record_t *record,
               turbo_flow_inbox_receipt_t *receipt);
  int (*claim)(void *ctx, turbo_flow_inbox_claim_t *claim);
  int (*claim_ex)(void *ctx, const turbo_flow_inbox_claim_request_t *request,
                  turbo_flow_inbox_claim_t *claim);
  int (*complete)(void *ctx, uint64_t record_id, uint64_t claim_token);
  int (*fail)(void *ctx, uint64_t record_id, uint64_t claim_token, int status);
  int (*retry)(void *ctx, uint64_t record_id);
  int (*discard)(void *ctx, uint64_t record_id);
  int (*forget)(void *ctx, uint64_t record_id);
  int (*scan_failed)(void *ctx, uint64_t after_record_id, turbo_flow_inbox_failed_entry_t *entries,
                     size_t capacity, size_t *out_count);
  int (*scan_history)(void *ctx, uint64_t after_record_id,
                      turbo_flow_inbox_history_entry_t *entries, size_t capacity,
                      size_t *out_count);
  int (*close)(void *ctx);
  int (*snapshot)(void *ctx, turbo_flow_inbox_snapshot_t *snapshot);
  int (*destroy)(void *ctx);
};

struct turbo_flow_inbox_s {
  size_t size;
  uint32_t version;
  const turbo_flow_inbox_ops_v2_t *ops;
  void *ctx;
};

#define TURBO_FLOW_INBOX_INIT {sizeof(turbo_flow_inbox_t), TURBO_FLOW_INBOX_API_VERSION, NULL, NULL}

typedef struct turbo_flow_inbox_memory_config_s {
  size_t size;
  uint32_t version;
  /** Maximum live + terminal-history records; in-progress reservations also consume slots. */
  size_t max_records;
  /** Sum of live and terminal-history variable bytes retained. */
  size_t max_total_bytes;
  /** Maximum total variable bytes for one record. */
  size_t max_record_bytes;
  size_t max_claims;
} turbo_flow_inbox_memory_config_t;

/** Return finite defaults; every limit remains explicit in the returned value. */
TURBO_FLOW_C_API turbo_flow_inbox_memory_config_t turbo_flow_inbox_memory_config_default(void);

/**
 * Create the built-in bounded memory provider in a zero-state output handle.
 *
 * The provider preallocates its record slots and serializes concurrent Source
 * admission and claim state with one mutex. It copies correlation and payload
 * bytes on admission. Replaying the same source_id + admission_id with the
 * same complete record returns the original receipt, including after terminal
 * completion/discard; different content is a protocol error. Terminal records
 * become bounded idempotency tombstones until explicit forget or destroy. It is
 * not durable and never substitutes for a configured database provider.
 *
 * @param config Exact-version, non-zero finite capacity configuration.
 * @param out Exact-version `TURBO_FLOW_INBOX_INIT` handle receiving ownership.
 * @return SALTS_OK, SALTS_EINVAL for an invalid ABI/config/output state, or
 *         SALTS_ENOMEM when bounded owner allocation fails.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_memory_create(const turbo_flow_inbox_memory_config_t *config,
                                                    turbo_flow_inbox_t *out);

/**
 * Copy one validated current-schema record into the selected provider.
 * Exact replay is resolved before lifecycle/capacity checks and therefore
 * returns the original receipt even after close or terminal settlement.
 * @return SALTS_OK; SALTS_EINVAL for malformed ABI/data; SALTS_EPROTO for an
 *         unsupported envelope schema; SALTS_ENOSPC for a configured bound;
 *         SALTS_EBUSY for transient provider contention; SALTS_ESHUTDOWN for a
 *         new identity after close; or the exact provider error.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_admit(turbo_flow_inbox_t *inbox,
                                            const turbo_flow_inbox_record_t *record,
                                            turbo_flow_inbox_receipt_t *receipt);
/**
 * Non-blocking FIFO claim into a zero-state `TURBO_FLOW_INBOX_CLAIM_INIT` value.
 * Empty returns SALTS_ENOENT; claim saturation returns SALTS_ENOSPC. Reusing a
 * live claim returns SALTS_EBUSY without changing its record ID or token.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_claim(turbo_flow_inbox_t *inbox,
                                            turbo_flow_inbox_claim_t *claim);
/**
 * Non-blocking claim with an explicit ordering/partition selector.
 *
 * GLOBAL is identical to turbo_flow_inbox_claim(). PARTITION_SOURCE_ID selects
 * the oldest pending record whose source_id is not in the bounded exclusion
 * set. If pending backlog exists only in excluded partitions, SALTS_ENOENT is
 * returned without changing provider state.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_claim_ex(
    turbo_flow_inbox_t *inbox, const turbo_flow_inbox_claim_request_t *request,
    turbo_flow_inbox_claim_t *claim);
/**
 * Complete one claim and retain its idempotency tombstone.
 * @return SALTS_OK on completion; SALTS_ECANCELED when owner takeover moved
 *         the durable record to OWNER_LOST_UNKNOWN; SALTS_EALREADY for a stale
 *         or concurrently settling copy; or the exact provider error. Both
 *         SALTS_OK and SALTS_ECANCELED invalidate `claim`.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_complete(turbo_flow_inbox_t *inbox,
                                               turbo_flow_inbox_claim_t *claim);
/**
 * Move one claim to FAILED with `status`.
 * @return SALTS_OK on failure recording; SALTS_ECANCELED when owner takeover
 *         already moved the record to OWNER_LOST_UNKNOWN; SALTS_EALREADY for a
 *         stale or concurrently settling copy; or the exact provider error.
 *         Both SALTS_OK and SALTS_ECANCELED invalidate `claim`.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_fail(turbo_flow_inbox_t *inbox,
                                           turbo_flow_inbox_claim_t *claim, int status);
/** Move one FAILED record back to PENDING; retry is never implicit. */
TURBO_FLOW_C_API int turbo_flow_inbox_retry(turbo_flow_inbox_t *inbox, uint64_t record_id);
/** Mark one FAILED record discarded and retain its idempotency tombstone. */
TURBO_FLOW_C_API int turbo_flow_inbox_discard(turbo_flow_inbox_t *inbox, uint64_t record_id);
/**
 * Remove one terminal idempotency tombstone and release its retained quota.
 * Live PENDING/CLAIMED/FAILED records are rejected. Once forgotten, replaying
 * the same admission identity is a new admission by explicit caller choice.
 * @param inbox Exact-version provider handle.
 * @param record_id Non-zero terminal record ID.
 * @return SALTS_OK, SALTS_EINVAL for an invalid handle/ID, SALTS_ENOENT when
 *         absent, SALTS_EBUSY while live, or the exact provider error.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_forget(turbo_flow_inbox_t *inbox, uint64_t record_id);
/**
 * Read FAILED entries ordered by record ID without advancing state.
 *
 * Each input slot must be `TURBO_FLOW_INBOX_FAILED_ENTRY_INIT`. At most
 * `capacity` entries with `record_id > after_record_id` are copied. Zero
 * capacity is valid and returns zero. Crash recovery uses
 * `TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN`; only explicit retry/discard
 * may advance such a record.
 * @param inbox Exact-version provider handle.
 * @param after_record_id Exclusive pagination cursor; zero starts the scan.
 * @param entries Caller-owned initialized slots, or NULL when capacity is zero.
 * @param capacity Number of entries available.
 * @param out_count Receives the number of entries written.
 * @return SALTS_OK, SALTS_EINVAL for invalid arguments, SALTS_EPROTO for a
 *         malformed provider result, or the exact provider error.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_scan_failed(const turbo_flow_inbox_t *inbox,
                                                  uint64_t after_record_id,
                                                  turbo_flow_inbox_failed_entry_t *entries,
                                                  size_t capacity, size_t *out_count);
/**
 * Read terminal tombstones ordered by record ID without advancing state.
 *
 * This is the durable recovery path for discovering completed/discarded IDs
 * after process state is lost. Each input slot must be
 * `TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT`; zero capacity is valid.
 * @param inbox Exact-version provider handle.
 * @param after_record_id Exclusive pagination cursor; zero starts the scan.
 * @param entries Caller-owned initialized slots, or NULL when capacity is zero.
 * @param capacity Number of entries available.
 * @param out_count Receives the number of entries written.
 * @return SALTS_OK, SALTS_EINVAL for invalid arguments, SALTS_EPROTO for a
 *         malformed provider result, or the exact provider error.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_scan_history(const turbo_flow_inbox_t *inbox,
                                                   uint64_t after_record_id,
                                                   turbo_flow_inbox_history_entry_t *entries,
                                                   size_t capacity, size_t *out_count);
/**
 * Stop new admission while leaving accepted records claimable for drain.
 * A provider may return SALTS_EBUSY while an already-reserved admission is
 * finishing its owned copy/transaction; close then leaves the inbox open and
 * is safe to retry.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_close(turbo_flow_inbox_t *inbox);
/** Copy current counters without mutating or advancing provider state. */
TURBO_FLOW_C_API int turbo_flow_inbox_snapshot(const turbo_flow_inbox_t *inbox,
                                               turbo_flow_inbox_snapshot_t *snapshot);
/**
 * Destroy only a closed inbox with no live record/claim; terminal history is
 * released by the non-durable memory provider and remains durable in TurboDB.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_destroy(turbo_flow_inbox_t *inbox);

/**
 * Minimal memory-provider lifecycle:
 *
 * @code
 * turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
 * turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
 * int rc = turbo_flow_inbox_memory_create(&config, &inbox);
 * if (rc == SALTS_OK) rc = turbo_flow_inbox_close(&inbox);
 * if (rc == SALTS_OK) rc = turbo_flow_inbox_destroy(&inbox);
 * @endcode
 */

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_INBOX_H */
