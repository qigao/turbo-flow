#ifndef TURBO_FLOW_QUEUE_H
#define TURBO_FLOW_QUEUE_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_QUEUE_MAX_CAPACITY 65536u
#define TURBO_FLOW_QUEUE_MAX_PAYLOAD_SIZE (64u * 1024u * 1024u)
#define TURBO_FLOW_QUEUE_MAX_TIMEOUT_MS (UINT32_MAX - 1u)
#define TURBO_FLOW_QUEUE_NAME_MAX 127u
#define TURBO_FLOW_SQLITE_BLOB_STORE_KEY_MAX 1024u
#define TURBO_FLOW_SQLITE_BLOB_STORE_DEFAULT_MAX_VALUE_SIZE (16u * 1024u * 1024u)
#define TURBO_FLOW_SQLITE_QUEUE_DEFAULT_MAX_STATE_SIZE (16u * 1024u * 1024u)
#define TURBO_FLOW_SQLITE_RECORD_STORE_NAMESPACE_MAX 255u
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_KEY_SIZE 65535u
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE (16u * 1024u * 1024u)
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE 4096u
#define TURBO_FLOW_QUEUE_CLAIM_OWNER_API_VERSION 1u

typedef struct turbo_flow_queue_s turbo_flow_queue_t;

typedef enum turbo_flow_queue_full_policy_e {
  TURBO_FLOW_QUEUE_FULL_FAIL = 0,
  TURBO_FLOW_QUEUE_FULL_BLOCK,
  TURBO_FLOW_QUEUE_FULL_DROP_OLDEST
} turbo_flow_queue_full_policy_t;

typedef struct turbo_flow_queue_config_s {
  /** Required stable management UID, for example `queue:orders`. */
  const char *resource_uid;
  /** Required stable owner reference, independent of source/sink binding names. */
  const char *owner_name;
  /** Required fixed message capacity. */
  size_t capacity;
  /** Required payload bound applied before message cloning. */
  size_t max_payload_size;
  turbo_flow_queue_full_policy_t full_policy;
  /** BLOCK only: finite wait for capacity. */
  uint64_t enqueue_timeout_ms;
} turbo_flow_queue_config_t;

typedef struct turbo_flow_sqlite_queue_config_s {
  /** Queue behavior and management identity shared with the in-memory implementation. */
  turbo_flow_queue_config_t queue;
  /** Required SQLite database path. The queue opens and owns one connection. */
  const char *database_path;
  /** Required stable key when a database stores more than one logical queue. */
  const char *queue_name;
  /** Non-negative SQLite busy timeout; zero keeps SQLite's immediate busy behavior. */
  int busy_timeout_ms;
  /** Zero uses TURBO_FLOW_SQLITE_QUEUE_DEFAULT_MAX_STATE_SIZE. */
  size_t max_state_size;
} turbo_flow_sqlite_queue_config_t;

typedef struct turbo_flow_sqlite_blob_store_config_s {
  /** Required SQLite database path. */
  const char *database_path;
  /** Required stable row key. */
  const char *key;
  /** Non-negative SQLite busy timeout. */
  int busy_timeout_ms;
  /** Zero uses TURBO_FLOW_SQLITE_BLOB_STORE_DEFAULT_MAX_VALUE_SIZE. */
  size_t max_value_size;
} turbo_flow_sqlite_blob_store_config_t;

typedef struct turbo_flow_sqlite_record_store_config_s {
  /** Required SQLite database path. */
  const char *database_path;
  /** Required namespace isolating one logical state owner. */
  const char *namespace_name;
  /** Non-negative SQLite busy timeout. */
  int busy_timeout_ms;
  /** Zero uses TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_KEY_SIZE. */
  size_t max_key_size;
  /** Zero uses TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE. */
  size_t max_value_size;
  /** Zero uses TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE. */
  size_t max_batch_size;
  /** Required maximum record count for bounded startup scans. */
  size_t max_records;
} turbo_flow_sqlite_record_store_config_t;

/** Create an atomic SQLite blob store in the caller-owned view. */
CXX_C_API int
turbo_flow_sqlite_blob_store_create(const turbo_flow_sqlite_blob_store_config_t *config,
                                    turbo_flow_blob_store_t *out);

/**
 * Create a SQLite blob store from a `kind: blob_store`, `backend: sqlite`
 * channel in an immutable YAML projection. The configured key is copied into
 * `key`; callers pass that key when binding the store to an owner.
 */
CXX_C_API int turbo_flow_sqlite_blob_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_blob_store_t *out, char *key, size_t key_capacity, turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_sqlite_blob_store_destroy(turbo_flow_blob_store_t *store);

/** Create a namespaced, revision-checked, atomic SQLite record store. */
CXX_C_API int turbo_flow_sqlite_record_store_create(
    const turbo_flow_sqlite_record_store_config_t *config, turbo_flow_record_store_t *out);

/** Create a SQLite record store from a `kind: record_store` YAML channel. */
CXX_C_API int turbo_flow_sqlite_record_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_sqlite_record_store_destroy(turbo_flow_record_store_t *store);

typedef struct turbo_flow_queue_snapshot_s {
  /** Caller sets this to sizeof(turbo_flow_queue_snapshot_t). */
  size_t size;
  size_t depth;
  size_t capacity;
  uint64_t enqueued;
  uint64_t delivered;
  uint64_t dropped_oldest;
  uint64_t enqueue_failures;
  uint64_t publish_failures;
} turbo_flow_queue_snapshot_t;

#define TURBO_FLOW_QUEUE_SNAPSHOT_INIT {sizeof(turbo_flow_queue_snapshot_t)}

/**
 * Runtime ACK counters for the two distinct queue commit boundaries.
 *
 * An accept ACK means the memory enqueue or durable transaction committed.
 * A delivery ACK means downstream publication and backend finalization both
 * succeeded. These counters are observational and reset when the queue object
 * is recreated; the SQLite rows remain the durable fact source.
 */
typedef struct turbo_flow_queue_ack_snapshot_s {
  /** Caller sets this to sizeof(turbo_flow_queue_ack_snapshot_t). */
  size_t size;
  uint64_t accept_acks;
  uint64_t delivery_acks;
  uint64_t delivery_requeues;
  uint64_t delivery_requeue_failures;
} turbo_flow_queue_ack_snapshot_t;

#define TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT {sizeof(turbo_flow_queue_ack_snapshot_t)}

/** One host-owned asynchronous delivery claim. */
typedef struct turbo_flow_queue_claim_s {
  /** Caller sets this to sizeof(turbo_flow_queue_claim_t). */
  size_t size;
  /** Opaque queue-local generation. Only the matching active claim may settle it. */
  uint64_t token;
  /** Borrowed immutable message, valid until this token is acknowledged or requeued. */
  const turbo_flow_msg_t *message;
} turbo_flow_queue_claim_t;

#define TURBO_FLOW_QUEUE_CLAIM_INIT {sizeof(turbo_flow_queue_claim_t), 0u, NULL}

/** Additive bounded direct-claim owner configuration; legacy/default behavior is one claim. */
typedef struct turbo_flow_queue_claim_owner_config_s {
  size_t size;
  uint32_t version;
  size_t max_active_claims;
} turbo_flow_queue_claim_owner_config_t;

#define TURBO_FLOW_QUEUE_CLAIM_OWNER_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_queue_claim_owner_config_t), TURBO_FLOW_QUEUE_CLAIM_OWNER_API_VERSION, 1u}

/** Create a host-owned queue. Configuration is validated and copied. */
CXX_C_API turbo_flow_queue_t *turbo_flow_queue_create(const turbo_flow_queue_config_t *config);

/**
 * Create one named Queue channel from an immutable resolved YAML snapshot.
 *
 * The channel must have kind `queue`, pattern `push_pull`, and backend
 * `memory` or `sqlite`. Unknown fields and unsupported capabilities fail fast.
 */
CXX_C_API int turbo_flow_queue_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                               const char *channel_name, turbo_flow_queue_t **out,
                                               turbo_flow_config_error_t *error);

/**
 * Create a durable SQLite-backed queue.
 *
 * Sink acceptance commits the row before returning success. A source claims one
 * row and deletes it only after graph publication succeeds. Failed publication
 * and process restart make the row pending again, yielding explicit at-least-once
 * delivery: messages are not silently lost, but a crash after an external side
 * effect and before deletion can cause redelivery.
 */
CXX_C_API turbo_flow_queue_t *
turbo_flow_sqlite_queue_create(const turbo_flow_sqlite_queue_config_t *config);

/** Destroy an unreferenced queue; returns TURBO_EBUSY while adapters retain it. */
CXX_C_API int turbo_flow_queue_destroy(turbo_flow_queue_t *queue);

/** Register a source that publishes queued messages and acknowledges on success. */
CXX_C_API int turbo_flow_queue_register_source_adapter(turbo_flow_t *flow, const char *name,
                                                       turbo_flow_queue_t *queue);

/** Register a sink that clones accepted messages into the bounded queue. */
CXX_C_API int turbo_flow_queue_register_sink_adapter(turbo_flow_t *flow, const char *name,
                                                     turbo_flow_queue_t *queue);

/** Register one `{channel, role}` Queue adapter from the resolved YAML snapshot. */
CXX_C_API int turbo_flow_queue_register_resolved_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    turbo_flow_queue_t *queue, turbo_flow_config_error_t *error);

/**
 * Configure bounded direct claims before a source adapter or claim becomes active.
 * Memory and SQLite queues support a finite bound up to queue capacity.
 */
CXX_C_API int
turbo_flow_queue_configure_claims(turbo_flow_queue_t *queue,
                                  const turbo_flow_queue_claim_owner_config_t *config);

/**
 * Non-blocking asynchronous claim for delayed-reply consumers.
 *
 * The queue retains the authoritative item until ack or requeue succeeds. Direct claims are
 * bounded by the configured owner limit and remain mutually exclusive with a source adapter.
 * `claim->message` is a stable borrowed immutable view until that token is settled.
 * TURBO_ENOENT means no pending item; TURBO_EBUSY means the claim bound or source is active.
 */
CXX_C_API int turbo_flow_queue_claim(turbo_flow_queue_t *queue, turbo_flow_queue_claim_t *claim);

/** Commit successful delayed delivery and release queue capacity. */
CXX_C_API int turbo_flow_queue_claim_ack(turbo_flow_queue_t *queue, uint64_t token);

/** Return a failed delayed delivery to the front without changing its payload. */
CXX_C_API int turbo_flow_queue_claim_requeue(turbo_flow_queue_t *queue, uint64_t token);

/** Terminally remove a failed delayed delivery without reporting a delivery ACK. */
CXX_C_API int turbo_flow_queue_claim_drop(turbo_flow_queue_t *queue, uint64_t token);

/** Export a borrowed thin settler binding for a credit/delayed-reply coordinator. */
CXX_C_API int turbo_flow_queue_claim_settler(turbo_flow_queue_t *queue,
                                             turbo_flow_claim_settler_t *out);

CXX_C_API int turbo_flow_queue_snapshot(const turbo_flow_queue_t *queue,
                                        turbo_flow_queue_snapshot_t *out);

CXX_C_API int turbo_flow_queue_ack_snapshot(const turbo_flow_queue_t *queue,
                                            turbo_flow_queue_ack_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_QUEUE_H */
