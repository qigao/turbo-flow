#ifndef TURBO_FLOW_INBOX_SOURCE_H
#define TURBO_FLOW_INBOX_SOURCE_H

#include "turbo_flow_inbox.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_INBOX_SOURCE_API_VERSION UINT32_C(1)
#define TURBO_FLOW_INBOX_SOURCE_DEFAULT_MAX_MESSAGE_BYTES (2u * 1024u * 1024u)

typedef struct turbo_flow_inbox_source_s turbo_flow_inbox_source_t;

/**
 * Message-owned Inbox identity.
 *
 * Offsets are relative to the beginning of `message->buffer`. The structure contains no
 * process-local pointer and remains valid while TurboFlow retains the shared message buffer across
 * clone/move boundaries. Use the accessors below instead of interpreting offsets directly.
 */
typedef struct turbo_flow_inbox_source_context_s {
  size_t size;
  uint32_t version;
  uint32_t reserved;
  uint64_t record_id;
  uint64_t source_sequence;
  size_t source_id_offset;
  size_t source_id_size;
  size_t admission_id_offset;
  size_t admission_id_size;
  size_t correlation_offset;
  size_t correlation_size;
  size_t payload_offset;
  size_t payload_size;
  size_t buffer_size;
} turbo_flow_inbox_source_context_t;

typedef struct turbo_flow_inbox_source_config_s {
  size_t size;
  uint32_t version;
  /** Borrowed; the caller must keep the provider alive until source destruction succeeds. */
  turbo_flow_inbox_t *inbox;
  /** Borrowed STARTED Flow; the caller must drain/destroy this source before stopping the Flow. */
  turbo_flow_t *flow;
  /** Borrowed configured name, copied during create; it must identify an adapter-free Source. */
  const char *graph_source_name;
  /** Borrowed Scheduler; NULL selects the Flow-owned bounded Scheduler. */
  cflow_scheduler *scheduler;
  /** Finite upper bound for context, identity, correlation, and payload bytes retained per run. */
  size_t max_message_bytes;
} turbo_flow_inbox_source_config_t;

#define TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT                                                        \
  {sizeof(turbo_flow_inbox_source_config_t),                                                       \
   TURBO_FLOW_INBOX_SOURCE_API_VERSION,                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_INBOX_SOURCE_DEFAULT_MAX_MESSAGE_BYTES}

typedef enum turbo_flow_inbox_source_result_state_e {
  TURBO_FLOW_INBOX_SOURCE_EMPTY = 0,
  TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE,
  TURBO_FLOW_INBOX_SOURCE_COMPLETED,
  TURBO_FLOW_INBOX_SOURCE_FAILED,
  TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING,
  TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN,
  TURBO_FLOW_INBOX_SOURCE_OWNER_LOST_UNKNOWN
} turbo_flow_inbox_source_result_state_t;

typedef struct turbo_flow_inbox_source_result_s {
  size_t size;
  uint32_t version;
  turbo_flow_inbox_source_result_state_t state;
  uint64_t record_id;
  int graph_status;
  int settlement_status;
} turbo_flow_inbox_source_result_t;

#define TURBO_FLOW_INBOX_SOURCE_RESULT_INIT                                                        \
  {sizeof(turbo_flow_inbox_source_result_t),                                                       \
   TURBO_FLOW_INBOX_SOURCE_API_VERSION,                                                            \
   TURBO_FLOW_INBOX_SOURCE_EMPTY,                                                                  \
   0u,                                                                                             \
   SALTS_OK,                                                                                       \
   SALTS_OK}

/**
 * Create a single-owner, single-record Inbox-to-Graph driver.
 *
 * The exact current config ABI is required. `graph_source_name` must resolve to an adapter-free
 * logical Source on a STARTED Flow. Creation never claims a record and never starts execution.
 * Calls on the returned object must be serialized by its owner.
 *
 * @param config Borrowed exact-version configuration; dependencies outlive the driver.
 * @param source_out Zero-state output receiving ownership on success.
 * @return SALTS_OK, SALTS_EINVAL for an invalid contract, SALTS_ENOMEM, or the exact Inbox
 *         snapshot error.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_source_create(const turbo_flow_inbox_source_config_t *config,
                                                    turbo_flow_inbox_source_t **source_out);

/**
 * Claim at most one FIFO record and request one non-blocking Graph run.
 *
 * An empty Inbox returns SALTS_ENOENT. While a run or unresolved settlement is retained, this
 * returns SALTS_EBUSY. Any failure after claim is recorded through Inbox fail; if that settlement
 * cannot be confirmed, the claim remains owned and Graph execution is never retried implicitly.
 * This function never waits for terminal state. A caller-selected inline Scheduler may execute
 * Graph work before request returns; owners that must continue polling external I/O use the
 * Flow-owned worker Scheduler or another deferred Scheduler.
 * @return SALTS_OK once requested; SALTS_ENOENT when empty; SALTS_EBUSY while owning another
 *         record; or the exact Flow/Inbox/allocation failure.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_source_request(turbo_flow_inbox_source_t *source);

/**
 * Snapshot the active Graph run without waiting and settle it once it becomes terminal.
 *
 * GRAPH_ACTIVE returns SALTS_OK. Confirmed completion returns SALTS_OK; confirmed Graph failure
 * returns its exact status. A failed settlement returns its exact provider status and is not
 * attempted again by poll. The caller must use retry_settlement or, for EALREADY, reconcile.
 * @param result Exact-version caller-owned output reset before observation.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_source_poll(turbo_flow_inbox_source_t *source,
                                                  turbo_flow_inbox_source_result_t *result);

/**
 * Cancel only the active Graph run and settle the claim as failed with SALTS_ECANCELED.
 *
 * An already terminal run is polled and settled from its actual outcome. Pending or unknown
 * settlement returns SALTS_EBUSY and is never changed by cancellation.
 * @param result Exact-version caller-owned output.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_source_cancel(turbo_flow_inbox_source_t *source,
                                                    turbo_flow_inbox_source_result_t *result);

/**
 * Retry only a known failed settlement; this never creates or requests another Graph run.
 * @return The confirmed Graph status, the exact provider error, or SALTS_EINVAL/EALREADY for an
 *         invalid phase.
 */
TURBO_FLOW_C_API int
turbo_flow_inbox_source_retry_settlement(turbo_flow_inbox_source_t *source,
                                         turbo_flow_inbox_source_result_t *result);

/**
 * Reconcile an EALREADY settlement against the provider's failed/history indexes.
 *
 * A matching exact terminal record releases the stale local claim. Absence or an incompatible
 * terminal state returns SALTS_EBUSY/SALTS_EPROTO and retains ownership for operator action.
 * @return The confirmed Graph status, SALTS_ECANCELED for owner loss, or the exact scan/error
 *         status while the ambiguous claim remains retained.
 */
TURBO_FLOW_C_API int
turbo_flow_inbox_source_reconcile_settlement(turbo_flow_inbox_source_t *source,
                                             turbo_flow_inbox_source_result_t *result);

/**
 * Destroy only an idle source. Active or unresolved ownership returns SALTS_EBUSY.
 * @return SALTS_OK, SALTS_EBUSY, or SALTS_EINVAL for NULL.
 */
TURBO_FLOW_C_API int turbo_flow_inbox_source_destroy(turbo_flow_inbox_source_t *source);

/** Return a validated message-owned Inbox context, or NULL for a non-Inbox/malformed message. */
TURBO_FLOW_C_API const turbo_flow_inbox_source_context_t *
turbo_flow_inbox_source_context(const turbo_flow_msg_t *message);
/** Return immutable message-owned identity views; invalid messages produce an empty view. */
TURBO_FLOW_C_API vstr turbo_flow_inbox_source_source_id(const turbo_flow_msg_t *message);
TURBO_FLOW_C_API vstr turbo_flow_inbox_source_admission_id(const turbo_flow_msg_t *message);
TURBO_FLOW_C_API vstr turbo_flow_inbox_source_correlation(const turbo_flow_msg_t *message);

#ifdef __cplusplus
}
#endif

#endif
