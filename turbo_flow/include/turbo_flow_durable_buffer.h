#ifndef TURBO_FLOW_DURABLE_BUFFER_H
#define TURBO_FLOW_DURABLE_BUFFER_H

#include "turbo_flow.h"
#include "turbo_flow_inbox.h"
#include "turbo_flow_inbox_source.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DURABLE_BUFFER_API_VERSION UINT32_C(1)
#define TURBO_FLOW_DURABLE_SOURCE_ID_MAX 127u
#define TURBO_FLOW_DURABLE_ADMISSION_ID_MAX 255u
#define TURBO_FLOW_DURABLE_CORRELATION_MAX 255u
#define TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES (2u * 1024u * 1024u)

typedef struct turbo_flow_durable_identity_s {
  size_t size;
  uint32_t version;
  vstr source_id;
  vstr admission_id;
  vstr correlation;
  uint64_t source_sequence;
} turbo_flow_durable_identity_t;

#define TURBO_FLOW_DURABLE_IDENTITY_INIT                                                   \
  {sizeof(turbo_flow_durable_identity_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION,           \
   {NULL, 0u}, {NULL, 0u}, {NULL, 0u}, 0u}

TURBO_FLOW_C_API int turbo_flow_msg_set_durable_identity(
    turbo_flow_msg_t *message, const turbo_flow_durable_identity_t *identity);

TURBO_FLOW_C_API int turbo_flow_msg_durable_identity(
    const turbo_flow_msg_t *message, turbo_flow_durable_identity_t *out);

typedef struct turbo_flow_durable_buffer_binding_s turbo_flow_durable_buffer_binding_t;

typedef enum turbo_flow_durable_identity_mode_e {
  TURBO_FLOW_DURABLE_IDENTITY_GENERATED = 1,
  TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED = 2
} turbo_flow_durable_identity_mode_t;

typedef struct turbo_flow_durable_buffer_binding_config_s {
  size_t size;
  uint32_t version;
  const char *resource_name;
  turbo_flow_inbox_t *inbox;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
} turbo_flow_durable_buffer_binding_config_t;

#define TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT                                             \
  {sizeof(turbo_flow_durable_buffer_binding_config_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION,       \
   NULL, NULL, TURBO_FLOW_DURABLE_IDENTITY_GENERATED,                                               \
   TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES}

/**
 * Provider-neutral observation for one named durable-buffer resource.
 *
 * Storage counters are copied from the selected Inbox provider. Driver fields
 * describe only TurboFlow-owned downstream progress and never expose provider
 * handles/cursors. The snapshot is bounded and read-only.
 */
typedef struct turbo_flow_durable_buffer_snapshot_s {
  size_t size;
  uint32_t version;
  uint64_t provider_generation;
  int accepting;
  int drain_paused;
  size_t records;
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
  turbo_flow_inbox_source_result_state_t driver_state;
  uint64_t active_record_id;
  int graph_status;
  int settlement_status;
} turbo_flow_durable_buffer_snapshot_t;

#define TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT                                                   \
  {sizeof(turbo_flow_durable_buffer_snapshot_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION,            \
   0u, 0, 0, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,                                       \
   TURBO_FLOW_INBOX_SOURCE_EMPTY, 0u, SALTS_OK, SALTS_OK}

/**
 * Bind one configured durable-buffer resource to a caller-owned Inbox provider.
 * The binding borrows `inbox`; provider ownership and lifetime remain with the caller.
 * Keep the provider alive and its handle immutable until unbind or Flow destruction/
 * reset without keep_registry. Compile/start/admission reject a changed handle or
 * generation with ECANCELED; snapshot errors propagate unchanged. Providers remain
 * responsible for atomically fencing takeover against admission.
 * Aliases of the same provider (ops/ctx) within one Flow are rejected with EALREADY.
 * Generated IDs use a fresh binding UUID namespace plus generation and sequence;
 * entropy failure rejects bind. Stable upstream identities are unchanged.
 * Configure before compile. Each buffer must resolve to a bound resource, and two
 * buffers cannot share a binding; compile/start reject these cases with ENOENT/EINVAL.
 * Serialize bind/unbind with Flow lifecycle operations. Successful bind returns a
 * Flow-owned handle in `out`; failure clears `out` without transferring Inbox ownership.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_bind(
    turbo_flow_t *flow, const turbo_flow_durable_buffer_binding_config_t *config,
    turbo_flow_durable_buffer_binding_t **out);

/**
 * Snapshot one named durable-buffer resource without advancing provider or Graph
 * state. The output must be initialized with TURBO_FLOW_DURABLE_BUFFER_SNAPSHOT_INIT.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_snapshot(
    turbo_flow_t *flow, const char *resource_name,
    turbo_flow_durable_buffer_snapshot_t *snapshot);

/**
 * Pause new ordinary downstream claims for one named durable buffer. Admission
 * remains unchanged and an already-owned run may still be polled/settled.
 * Explicit turbo_flow_durable_buffer_drain() is an operator action and may still
 * drain accepted backlog while this control is paused.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_pause_drain(
    turbo_flow_t *flow, const char *resource_name);

/** Resume ordinary downstream claims after pause_drain(). */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_resume_drain(
    turbo_flow_t *flow, const char *resource_name);

/**
 * Progress at most one claim without blocking. Empty storage is success. Calls on
 * a binding are caller-serialized with Flow lifecycle and provider mutation.
 * A new claim requires STARTED/open ordinary admission; paused admission returns
 * ESHUTDOWN without claiming. An owned run may be polled/settled while paused or
 * stopped. Provider, Graph and settlement errors propagate unchanged; progress
 * never implicitly retries or reconciles a pending/unknown settlement.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_progress(
    turbo_flow_durable_buffer_binding_t *binding);

/** Close the borrowed Inbox to new admissions; accepted records remain retained. */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_quiesce(
    turbo_flow_durable_buffer_binding_t *binding);

/**
 * Drain accepted records with a single owner. STARTED permits buffer-origin runs
 * even while ordinary Graph admission is paused. STOPPED/FAILED never claim new
 * records: only an already-owned terminal/canceled run may settle. Failed records
 * and unresolved settlement return their exact error, without retry/drop/replay.
 * Pending stopped backlog returns EBUSY; a running backlog exceeding timeout_ms
 * returns ETIMEDOUT. Zero is non-blocking; UINT64_MAX waits without a deadline.
 * The owner must continue servicing any external I/O required by async Sinks.
 * Example: quiesce(binding), then drain(binding, 1000), stop(flow), unbind(binding),
 * checking each status before proceeding. For chained buffers use generation
 * retirement, which closes and drains cuts in upstream-to-downstream order.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_drain(
    turbo_flow_durable_buffer_binding_t *binding, uint64_t timeout_ms);

/**
 * Retry only the currently retained settlement attempt for this binding.
 * This never claims another record and never re-executes Graph work. It is valid
 * only while the driver retains a known failed complete/fail settlement attempt;
 * an already-unknown EALREADY outcome remains unknown and returns EALREADY.
 * The result output must be an exact-version TURBO_FLOW_INBOX_SOURCE_RESULT_INIT value.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_retry_settlement(
    turbo_flow_durable_buffer_binding_t *binding,
    turbo_flow_inbox_source_result_t *result);

/**
 * Reconcile only an EALREADY/unknown settlement against provider failed/history
 * indexes. Reconciliation does not retry Graph work or mutate provider record state.
 * A durable owner-lost record resolves to OWNER_LOST_UNKNOWN/ECANCELED; an
 * unobservable outcome remains fail-closed.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_reconcile_settlement(
    turbo_flow_durable_buffer_binding_t *binding,
    turbo_flow_inbox_source_result_t *result);

/**
 * Scan FAILED records for one named durable-buffer resource without advancing
 * provider state. Entries preserve PROCESSING versus OWNER_LOST_UNKNOWN so an
 * operator can make an explicit retry or discard decision.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_scan_failed(
    turbo_flow_t *flow, const char *resource_name, uint64_t after_record_id,
    turbo_flow_inbox_failed_entry_t *entries, size_t capacity, size_t *out_count);

/**
 * Explicitly move one FAILED record back to PENDING. This does not request or
 * execute downstream Graph work; later progress/drain owns that decision.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_retry_failed(
    turbo_flow_t *flow, const char *resource_name, uint64_t record_id);

/**
 * Explicitly discard one FAILED record and retain its idempotency tombstone.
 * This never retries, reconciles, or executes downstream Graph work.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_discard_failed(
    turbo_flow_t *flow, const char *resource_name, uint64_t record_id);

/**
 * Scan terminal completion/discard history for one named durable-buffer resource.
 * This explicit operator observation never retries, reconciles, forgets, or otherwise
 * advances provider state. Calls are caller-serialized with lifecycle and progress.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_scan_history(
    turbo_flow_t *flow, const char *resource_name, uint64_t after_record_id,
    turbo_flow_inbox_history_entry_t *entries, size_t capacity, size_t *out_count);

/**
 * Explicitly forget one terminal completion/discard tombstone for a named resource.
 * Success releases provider quota and permits the same admission identity to become
 * new work. Live, failed, pending, and unknown records are unchanged and rejected;
 * no retry or reconciliation is implied.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_forget(
    turbo_flow_t *flow, const char *resource_name, uint64_t record_id);

/**
 * Remove one binding while the Flow is not started and its driver is idle
 * (EBUSY otherwise). Reset also rejects an unresolved driver. The void Flow
 * destroy operation preserves the complete Flow with EBUSY in its error state
 * until the caller explicitly settles/drains the claim and retries destruction.
 * Success invalidates the handle; the Inbox remains caller-owned and is not closed.
 * Start revalidates bindings, including when a binding was removed after compile.
 */
TURBO_FLOW_C_API int turbo_flow_durable_buffer_unbind(
    turbo_flow_durable_buffer_binding_t *binding);

#ifdef __cplusplus
}
#endif

#endif
