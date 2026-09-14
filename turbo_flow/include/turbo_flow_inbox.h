#ifndef TURBO_FLOW_INBOX_H
#define TURBO_FLOW_INBOX_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_INBOX_API_VERSION UINT32_C(1)
#define TURBO_FLOW_INBOX_RECORD_SCHEMA "turbo-flow.inbox.record"
#define TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION UINT32_C(1)

#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_RECORDS 1024u
#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_TOTAL_BYTES (64u * 1024u * 1024u)
#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_RECORD_BYTES (1024u * 1024u)
#define TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_CLAIMS 64u
#define TURBO_FLOW_INBOX_MEMORY_MAX_RECORDS 1048576u
#define TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES (SIZE_MAX / 2u)

typedef struct turbo_flow_inbox_s turbo_flow_inbox_t;
typedef struct turbo_flow_inbox_ops_v1_s turbo_flow_inbox_ops_v1_t;

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

#define TURBO_FLOW_INBOX_RECEIPT_INIT                                                             \
  {sizeof(turbo_flow_inbox_receipt_t), TURBO_FLOW_INBOX_API_VERSION, 0u}

/**
 * Move-only claim lease.
 *
 * `record` and its byte views are immutable and borrowed from the provider
 * until the first successful complete/fail. Copying this value does not create
 * another claim; stale copies are rejected by record_id + claim_token.
 */
typedef struct turbo_flow_inbox_claim_s {
  size_t size;
  uint32_t version;
  uint64_t record_id;
  uint64_t claim_token;
  turbo_flow_inbox_record_t record;
} turbo_flow_inbox_claim_t;

#define TURBO_FLOW_INBOX_CLAIM_INIT                                                               \
  {sizeof(turbo_flow_inbox_claim_t), TURBO_FLOW_INBOX_API_VERSION, 0u, 0u, {0}}

typedef struct turbo_flow_inbox_snapshot_s {
  size_t size;
  uint32_t version;
  int accepting;
  size_t records;
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

#define TURBO_FLOW_INBOX_SNAPSHOT_INIT                                                            \
  {sizeof(turbo_flow_inbox_snapshot_t), TURBO_FLOW_INBOX_API_VERSION, 0}

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
struct turbo_flow_inbox_ops_v1_s {
  size_t size;
  uint32_t version;
  int (*admit)(void *ctx, const turbo_flow_inbox_record_t *record,
               turbo_flow_inbox_receipt_t *receipt);
  int (*claim)(void *ctx, turbo_flow_inbox_claim_t *claim);
  int (*complete)(void *ctx, uint64_t record_id, uint64_t claim_token);
  int (*fail)(void *ctx, uint64_t record_id, uint64_t claim_token, int status);
  int (*retry)(void *ctx, uint64_t record_id);
  int (*discard)(void *ctx, uint64_t record_id);
  int (*close)(void *ctx);
  int (*snapshot)(void *ctx, turbo_flow_inbox_snapshot_t *snapshot);
  int (*destroy)(void *ctx);
};

struct turbo_flow_inbox_s {
  size_t size;
  uint32_t version;
  const turbo_flow_inbox_ops_v1_t *ops;
  void *ctx;
};

#define TURBO_FLOW_INBOX_INIT                                                                     \
  {sizeof(turbo_flow_inbox_t), TURBO_FLOW_INBOX_API_VERSION, NULL, NULL}

typedef struct turbo_flow_inbox_memory_config_s {
  size_t size;
  uint32_t version;
  size_t max_records;
  /** Sum of correlation and payload bytes retained by accepted records. */
  size_t max_total_bytes;
  /** Maximum correlation + payload bytes for one record. */
  size_t max_record_bytes;
  size_t max_claims;
} turbo_flow_inbox_memory_config_t;

/** Return finite defaults; every limit remains explicit in the returned value. */
TURBO_FLOW_C_API turbo_flow_inbox_memory_config_t
turbo_flow_inbox_memory_config_default(void);

/**
 * Create the built-in bounded memory provider in a zero-state output handle.
 *
 * The provider preallocates its record slots and serializes concurrent Source
 * admission and claim state with one mutex. It copies correlation and payload
 * bytes on admission. It is not durable and never substitutes for a configured
 * database provider.
 *
 * @param config Exact-version, non-zero finite capacity configuration.
 * @param out Exact-version `TURBO_FLOW_INBOX_INIT` handle receiving ownership.
 * @return SALTS_OK, SALTS_EINVAL for an invalid ABI/config/output state, or
 *         SALTS_ENOMEM when bounded owner allocation fails.
 */
TURBO_FLOW_C_API int
turbo_flow_inbox_memory_create(const turbo_flow_inbox_memory_config_t *config,
                               turbo_flow_inbox_t *out);

/**
 * Copy one validated current-schema record into the selected provider.
 * @return SALTS_OK; SALTS_EINVAL for malformed ABI/data; SALTS_EPROTO for an
 *         unsupported envelope schema; SALTS_ENOSPC for a configured bound;
 *         SALTS_ESHUTDOWN after close; or the exact provider error.
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
/** Remove one successfully processed claim and invalidate the supplied lease. */
TURBO_FLOW_C_API int turbo_flow_inbox_complete(turbo_flow_inbox_t *inbox,
                                                turbo_flow_inbox_claim_t *claim);
/** Retain a failed record in FAILED state and invalidate the supplied lease. */
TURBO_FLOW_C_API int turbo_flow_inbox_fail(turbo_flow_inbox_t *inbox,
                                            turbo_flow_inbox_claim_t *claim, int status);
/** Move one FAILED record back to PENDING; retry is never implicit. */
TURBO_FLOW_C_API int turbo_flow_inbox_retry(turbo_flow_inbox_t *inbox, uint64_t record_id);
/** Explicitly remove one FAILED record. PENDING or CLAIMED records are rejected. */
TURBO_FLOW_C_API int turbo_flow_inbox_discard(turbo_flow_inbox_t *inbox, uint64_t record_id);
/** Stop new admission while leaving accepted records claimable for drain. */
TURBO_FLOW_C_API int turbo_flow_inbox_close(turbo_flow_inbox_t *inbox);
/** Copy current counters without mutating or advancing provider state. */
TURBO_FLOW_C_API int turbo_flow_inbox_snapshot(const turbo_flow_inbox_t *inbox,
                                                turbo_flow_inbox_snapshot_t *snapshot);
/** Destroy only a closed, fully drained inbox; success clears the move-only handle. */
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
