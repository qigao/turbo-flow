#ifndef FLOWIE_CLUSTER_PGSQL_INTERNAL_H
#define FLOWIE_CLUSTER_PGSQL_INTERNAL_H

#include "flowie_cluster_internal.h"
#include "turbo_error.h"
#include "turbo_flow_record_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_PGSQL_ABI_V1 1u
#define FLOWIE_CLUSTER_PGSQL_CONNINFO_MAX 4096u
#define FLOWIE_CLUSTER_PGSQL_SCHEMA_NAME_MAX 63u
#define FLOWIE_CLUSTER_COMMAND_ID_SIZE 16u
#define FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE 32u
#define FLOWIE_CLUSTER_PGSQL_FACT_BATCH_MAX 256u
#define FLOWIE_CLUSTER_PGSQL_MS_PER_SECOND UINT64_C(1000)

typedef struct flowie_cluster_pgsql_config_s {
  size_t size;
  uint32_t abi_version;
  const char *conninfo;
  const char *schema_name;
  const char *cluster_id;
  const char *listener_id;
  const char *node_id;
  const char *advertised_endpoint;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  uint32_t hash_version;
  uint32_t shard_count;
  uint64_t lease_ttl_ms;
  uint64_t renew_interval_ms;
  uint64_t retry_interval_ms;
  uint64_t worst_case_db_latency_ms;
  uint64_t safety_margin_ms;
  int create_schema;
} flowie_cluster_pgsql_config_t;

#define FLOWIE_CLUSTER_PGSQL_CONFIG_INIT                                                           \
  {sizeof(flowie_cluster_pgsql_config_t),                                                          \
   FLOWIE_CLUSTER_PGSQL_ABI_V1,                                                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   {0},                                                                                            \
   FLOWIE_CLUSTER_HASH_VERSION_1,                                                                  \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0}

typedef struct flowie_cluster_pgsql_coordinator_s flowie_cluster_pgsql_coordinator_t;

/** One copied membership row. Storage remains valid until snapshot_cleanup(). */
typedef struct flowie_cluster_pgsql_member_s {
  size_t size;
  uint32_t abi_version;
  size_t node_id_size;
  char node_id[FLOWIE_CLUSTER_NODE_ID_MAX + 1u];
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_node_state_t state;
  size_t advertised_endpoint_size;
  char advertised_endpoint[FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX + 1u];
  uint64_t lease_deadline_epoch_ms;
  uint64_t revision;
} flowie_cluster_pgsql_member_t;

#define FLOWIE_CLUSTER_PGSQL_MEMBER_INIT                                                          \
  {sizeof(flowie_cluster_pgsql_member_t), FLOWIE_CLUSTER_PGSQL_ABI_V1, 0u, {0}, {0},              \
   FLOWIE_CLUSTER_NODE_STARTING, 0u, {0}, 0u, 0u}

/** Owned immutable result of one repeatable-read PostgreSQL snapshot. */
typedef struct flowie_cluster_pgsql_membership_snapshot_s {
  size_t size;
  uint32_t abi_version;
  uint64_t membership_revision;
  flowie_cluster_pgsql_member_t *members;
  size_t member_count;
} flowie_cluster_pgsql_membership_snapshot_t;

#define FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT                                             \
  {sizeof(flowie_cluster_pgsql_membership_snapshot_t), FLOWIE_CLUSTER_PGSQL_ABI_V1, 0u, NULL, 0u}

/** Validate bounded identifiers and lease invariants without performing I/O. */
int flowie_cluster_pgsql_config_validate(const flowie_cluster_pgsql_config_t *config);

/**
 * Open one synchronous coordinator connection and validate or create its schema.
 * The coordinator copies configuration and is not thread-safe; its caller must
 * serialize all operations and keep blocking libpq calls off CoroNet owner lanes.
 */
int flowie_cluster_pgsql_coordinator_open(const flowie_cluster_pgsql_config_t *config,
                                          flowie_cluster_pgsql_coordinator_t **out);
void flowie_cluster_pgsql_coordinator_destroy(flowie_cluster_pgsql_coordinator_t *coordinator);

/** Mark all lease-expired live rows EXPIRED under one membership revision. */
int flowie_cluster_pgsql_membership_expire(flowie_cluster_pgsql_coordinator_t *coordinator,
                                           uint64_t *out_membership_revision);

/**
 * Copy one strictly node-id ordered repeatable-read snapshot. max_nodes is a
 * hard allocation/query bound; ENOSPC means PostgreSQL contains more rows.
 */
int flowie_cluster_pgsql_membership_snapshot(
    flowie_cluster_pgsql_coordinator_t *coordinator, size_t max_nodes,
    flowie_cluster_pgsql_membership_snapshot_t *out);
void flowie_cluster_pgsql_membership_snapshot_cleanup(
    flowie_cluster_pgsql_membership_snapshot_t *snapshot);

/** Resolve one exact node+boot membership row into a copied route endpoint. */
int flowie_cluster_pgsql_member_resolve(
    flowie_cluster_pgsql_coordinator_t *coordinator, tstr_v node_id,
    const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_pgsql_member_t *out);

/** One PostgreSQL-derived owner view with a conservative local deadline. */
typedef struct flowie_cluster_pgsql_shard_owner_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  uint64_t local_deadline_ns;
  flowie_cluster_owner_token_t owner;
} flowie_cluster_pgsql_shard_owner_t;

#define FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_INIT                                                     \
  {sizeof(flowie_cluster_pgsql_shard_owner_t), FLOWIE_CLUSTER_PGSQL_ABI_V1, 0u, 0u,               \
   FLOWIE_CLUSTER_OWNER_TOKEN_INIT}

typedef struct flowie_cluster_pgsql_shard_owner_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_pgsql_shard_owner_t *owners;
  size_t owner_count;
} flowie_cluster_pgsql_shard_owner_snapshot_t;

#define FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT                                            \
  {sizeof(flowie_cluster_pgsql_shard_owner_snapshot_t), FLOWIE_CLUSTER_PGSQL_ABI_V1, NULL, 0u}

/**
 * Read exactly shard_count index-aligned ownership rows in one SQL statement.
 * Missing, expired, or non-live owners are returned as unassigned entries.
 */
int flowie_cluster_pgsql_shard_owner_snapshot(
    flowie_cluster_pgsql_coordinator_t *coordinator,
    flowie_cluster_pgsql_shard_owner_snapshot_t *out);
void flowie_cluster_pgsql_shard_owner_snapshot_cleanup(
    flowie_cluster_pgsql_shard_owner_snapshot_t *snapshot);

/** Claim advances the epoch and returns a conservative local monotonic deadline. */
int flowie_cluster_pgsql_shard_claim(flowie_cluster_pgsql_coordinator_t *coordinator,
                                     uint32_t shard_id, uint64_t request_start_ns,
                                     flowie_cluster_owner_token_t *out_token,
                                     uint64_t *out_deadline_ns);

/** Renew/require/release accept only the exact node, boot, shard and epoch token. */
int flowie_cluster_pgsql_shard_renew(flowie_cluster_pgsql_coordinator_t *coordinator,
                                     const flowie_cluster_owner_token_t *token,
                                     uint64_t request_start_ns, uint64_t *out_deadline_ns);
int flowie_cluster_pgsql_shard_require(flowie_cluster_pgsql_coordinator_t *coordinator,
                                       const flowie_cluster_owner_token_t *token,
                                       uint64_t request_start_ns, uint64_t *out_deadline_ns);
int flowie_cluster_pgsql_shard_release(flowie_cluster_pgsql_coordinator_t *coordinator,
                                       const flowie_cluster_owner_token_t *token);

typedef enum flowie_cluster_pgsql_fact_write_kind_e {
  FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT = 1,
  FLOWIE_CLUSTER_PGSQL_EVENT_ONLY,
  FLOWIE_CLUSTER_PGSQL_FACT_ONLY
} flowie_cluster_pgsql_fact_write_kind_t;

/**
 * One fenced MQTT write and optional durable intent. EVENT_ONLY retains
 * record.key as the outbox identity without modifying cluster_fact. FACT_ONLY
 * updates cluster_fact without creating an outbox row. The default kind does
 * both in one transaction.
 */
typedef struct flowie_cluster_pgsql_fact_mutation_s {
  size_t size;
  flowie_cluster_pgsql_fact_write_kind_t write_kind;
  flowie_cluster_key_kind_t key_kind;
  const uint8_t *shard_key;
  size_t shard_key_size;
  turbo_flow_record_mutation_t record;
  uint32_t event_type;
  const uint8_t *event_payload;
  size_t event_payload_size;
} flowie_cluster_pgsql_fact_mutation_t;

#define FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT                                                    \
  {sizeof(flowie_cluster_pgsql_fact_mutation_t),                                                   \
   FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT,                                                            \
   FLOWIE_CLUSTER_KEY_SESSION,                                                                     \
   NULL,                                                                                           \
   0u,                                                                                             \
   TURBO_FLOW_RECORD_MUTATION_INIT,                                                                \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u}

/**
 * Optional immutable source-event identity for an idempotent target delivery.
 * Ordinary targets write cluster_event_dedupe. A non-empty shared_filter instead
 * writes the globally unique cluster_shared_selection winner. Either row is
 * inserted in the same fenced transaction as target facts and outbox actions.
 * A matching existing row means the target already applied the event; an
 * identity or digest mismatch is a protocol conflict.
 */
typedef struct flowie_cluster_pgsql_event_dedupe_s {
  size_t size;
  uint32_t abi_version;
  uint8_t source_command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  uint32_t event_index;
  uint32_t source_shard_id;
  uint64_t source_owner_epoch;
  uint64_t source_fact_revision;
  /** Zero denotes the shard-complete ACK; positive values denote target sessions. */
  uint64_t target_session_id;
  uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
  /** Non-empty only for a PostgreSQL-arbitrated shared-subscription candidate. */
  const uint8_t *shared_filter;
  size_t shared_filter_size;
} flowie_cluster_pgsql_event_dedupe_t;

#define FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT                                                    \
  {sizeof(flowie_cluster_pgsql_event_dedupe_t), FLOWIE_CLUSTER_PGSQL_ABI_V1, {0}, 0u, 0u, 0u,   \
   0u, 0u, {0}, NULL, 0u}

/** Caller-chosen stable ID makes a lost COMMIT reply independently confirmable. */
typedef struct flowie_cluster_pgsql_fact_command_s {
  size_t size;
  uint32_t abi_version;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  flowie_cluster_owner_token_t owner;
  const flowie_cluster_pgsql_fact_mutation_t *mutations;
  size_t mutation_count;
  const flowie_cluster_pgsql_event_dedupe_t *dedupe;
} flowie_cluster_pgsql_fact_command_t;

#define FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT                                                     \
  {sizeof(flowie_cluster_pgsql_fact_command_t),                                                    \
   FLOWIE_CLUSTER_PGSQL_ABI_V1,                                                                    \
   {0},                                                                                            \
   FLOWIE_CLUSTER_OWNER_TOKEN_INIT,                                                                \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL}

/** Explicit bounds for facts, receipts and unpublished outbox data. */
typedef struct flowie_cluster_pgsql_fact_config_s {
  size_t size;
  uint32_t abi_version;
  const flowie_cluster_pgsql_config_t *coordinator;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_fact_records;
  size_t max_receipts;
  size_t max_dedupe_records;
  size_t max_outbox_records;
  uint64_t max_outbox_bytes;
  size_t max_event_payload_size;
} flowie_cluster_pgsql_fact_config_t;

#define FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT                                                      \
  {sizeof(flowie_cluster_pgsql_fact_config_t),                                                     \
   FLOWIE_CLUSTER_PGSQL_ABI_V1,                                                                    \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

typedef struct flowie_cluster_pgsql_fact_store_s flowie_cluster_pgsql_fact_store_t;

int flowie_cluster_pgsql_fact_config_validate(const flowie_cluster_pgsql_fact_config_t *config);
int flowie_cluster_pgsql_fact_command_validate(const flowie_cluster_pgsql_fact_config_t *config,
                                               const flowie_cluster_pgsql_fact_command_t *command);
int flowie_cluster_pgsql_fact_store_open(const flowie_cluster_pgsql_fact_config_t *config,
                                         flowie_cluster_pgsql_fact_store_t **out);
void flowie_cluster_pgsql_fact_store_destroy(flowie_cluster_pgsql_fact_store_t *store);

/**
 * Commit returns EALREADY when the exact command or optional target-delivery
 * source event is already durable. EBUSY denotes fencing, revision, command-ID,
 * or source-event identity conflict; ENOSPC denotes a configured bound. Any
 * COMMIT I/O error is uncertain and must be confirmed.
 */
int flowie_cluster_pgsql_fact_commit(flowie_cluster_pgsql_fact_store_t *store,
                                     const flowie_cluster_pgsql_fact_command_t *command);

/** OK means the exact command or target-delivery source event is durable. */
int flowie_cluster_pgsql_fact_confirm(flowie_cluster_pgsql_fact_store_t *store,
                                      const flowie_cluster_pgsql_fact_command_t *command);

/**
 * Count exact shard-complete markers for one source event. EBUSY means at least
 * one shard marker reused the source key with conflicting immutable identity.
 */
int flowie_cluster_pgsql_event_ack_count(
    flowie_cluster_pgsql_fact_store_t *store,
    const flowie_cluster_pgsql_event_dedupe_t *source_event, size_t *out_count);

/** One bounded, ownership-fenced fact scan request for shard recovery. */
typedef struct flowie_cluster_pgsql_fact_scan_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_owner_token_t owner;
  flowie_cluster_key_kind_t key_kind;
  size_t max_records;
} flowie_cluster_pgsql_fact_scan_t;

#define FLOWIE_CLUSTER_PGSQL_FACT_SCAN_INIT                                                        \
  {sizeof(flowie_cluster_pgsql_fact_scan_t), FLOWIE_CLUSTER_PGSQL_ABI_V1,                          \
   FLOWIE_CLUSTER_OWNER_TOKEN_INIT, FLOWIE_CLUSTER_KEY_SESSION, 0u}

int flowie_cluster_pgsql_fact_scan_validate(const flowie_cluster_pgsql_fact_config_t *config,
                                            const flowie_cluster_pgsql_fact_scan_t *scan);
/**
 * visit receives a borrowed row view valid only until it returns. The SQL
 * snapshot must contain the exact live owner token; an empty owned shard still
 * succeeds, while a missing/stale owner returns TURBO_EBUSY.
 */
int flowie_cluster_pgsql_fact_scan(flowie_cluster_pgsql_fact_store_t *store,
                                   const flowie_cluster_pgsql_fact_scan_t *scan,
                                   turbo_flow_record_visit_fn visit, void *visit_ctx);

/** One owned, exact-key fact read under the caller's current shard fence. */
typedef struct flowie_cluster_pgsql_fact_record_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_key_kind_t key_kind;
  uint32_t shard_id;
  uint64_t owner_epoch;
  uint64_t revision;
  tstr_t key;
  tstr_t value;
} flowie_cluster_pgsql_fact_record_t;

#define FLOWIE_CLUSTER_PGSQL_FACT_RECORD_INIT                                                      \
  {sizeof(flowie_cluster_pgsql_fact_record_t), FLOWIE_CLUSTER_PGSQL_ABI_V1,                       \
   FLOWIE_CLUSTER_KEY_SESSION, 0u, 0u, 0u, NULL, NULL}

int flowie_cluster_pgsql_fact_get(flowie_cluster_pgsql_fact_store_t *store,
                                  const flowie_cluster_owner_token_t *current_owner,
                                  flowie_cluster_key_kind_t key_kind, const uint8_t *key,
                                  size_t key_size, flowie_cluster_pgsql_fact_record_t *out);
void flowie_cluster_pgsql_fact_record_cleanup(flowie_cluster_pgsql_fact_record_t *record);

/** One owned immutable cluster_outbox row. Strings are released by cleanup(). */
typedef struct flowie_cluster_pgsql_outbox_event_s {
  size_t size;
  uint32_t abi_version;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  uint32_t event_index;
  uint32_t shard_id;
  uint64_t event_owner_epoch;
  uint64_t fact_revision;
  uint64_t event_type;
  flowie_cluster_key_kind_t record_kind;
  tstr_t record_key;
  tstr_t payload;
  /** PostgreSQL outbox insertion time, truncated to authoritative epoch seconds. */
  uint64_t created_at_epoch_seconds;
  uint64_t attempt_count;
} flowie_cluster_pgsql_outbox_event_t;

#define FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT                                                     \
  {sizeof(flowie_cluster_pgsql_outbox_event_t),                                                    \
   FLOWIE_CLUSTER_PGSQL_ABI_V1,                                                                    \
   {0},                                                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   FLOWIE_CLUSTER_KEY_SESSION,                                                                     \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u}

typedef int (*flowie_cluster_outbox_before_settle_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event);

/**
 * Fetch and increment the attempt counter of the oldest matching unpublished
 * event under the exact current owner lock. ENOENT means no matching event;
 * EBUSY means the supplied owner no longer owns the shard.
 */
int flowie_cluster_pgsql_outbox_next(flowie_cluster_pgsql_fact_store_t *store,
                                     const flowie_cluster_owner_token_t *current_owner,
                                     uint64_t event_type, flowie_cluster_pgsql_outbox_event_t *out);

/**
 * Mark an exact fetched event published under the current owner lock. A lost
 * COMMIT may return EIO; retrying the same event returns EALREADY once durable.
 */
int flowie_cluster_pgsql_outbox_settle(flowie_cluster_pgsql_fact_store_t *store,
                                       const flowie_cluster_owner_token_t *current_owner,
                                       const flowie_cluster_pgsql_outbox_event_t *event);

void flowie_cluster_pgsql_outbox_event_cleanup(flowie_cluster_pgsql_outbox_event_t *event);

/** Open an equivalent connection before atomically replacing the old store. */
int flowie_cluster_pgsql_fact_store_reopen(flowie_cluster_pgsql_fact_store_t **store);

typedef void (*flowie_cluster_pgsql_fact_completion_fn)(
    void *ctx, const uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE], int status);

typedef struct flowie_cluster_pgsql_fact_worker_config_s {
  size_t size;
  uint32_t abi_version;
  const flowie_cluster_pgsql_fact_config_t *fact;
  size_t max_queue_entries;
  size_t max_queue_bytes;
} flowie_cluster_pgsql_fact_worker_config_t;

#define FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT                                               \
  {sizeof(flowie_cluster_pgsql_fact_worker_config_t), FLOWIE_CLUSTER_PGSQL_ABI_V1, NULL, 0u, 0u}

typedef struct flowie_cluster_pgsql_fact_worker_s flowie_cluster_pgsql_fact_worker_t;

/**
 * The worker owns one blocking libpq connection and a bounded deep-copy queue.
 * Completion runs on the worker thread and must post back to an owner lane; it
 * must not close or destroy the worker reentrantly.
 */
int flowie_cluster_pgsql_fact_worker_config_validate(
    const flowie_cluster_pgsql_fact_worker_config_t *config);
int flowie_cluster_pgsql_fact_worker_create(const flowie_cluster_pgsql_fact_worker_config_t *config,
                                            flowie_cluster_pgsql_fact_worker_t **out);
int flowie_cluster_pgsql_fact_worker_submit(flowie_cluster_pgsql_fact_worker_t *worker,
                                            const flowie_cluster_pgsql_fact_command_t *command,
                                            flowie_cluster_pgsql_fact_completion_fn completion,
                                            void *completion_ctx);
/** Stop admission, drain accepted commands and join the worker thread. */
int flowie_cluster_pgsql_fact_worker_close(flowie_cluster_pgsql_fact_worker_t *worker);
void flowie_cluster_pgsql_fact_worker_destroy(flowie_cluster_pgsql_fact_worker_t *worker);

typedef struct flowie_cluster_pgsql_lease_worker_s flowie_cluster_pgsql_lease_worker_t;

typedef struct flowie_cluster_pgsql_lease_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_shard_state_t state;
  flowie_cluster_owner_token_t owner;
  uint64_t local_deadline_ns;
  int last_coordinator_status;
} flowie_cluster_pgsql_lease_snapshot_t;

#define FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT                                                   \
  {sizeof(flowie_cluster_pgsql_lease_snapshot_t),                                                  \
   FLOWIE_CLUSTER_PGSQL_ABI_V1,                                                                    \
   FLOWIE_CLUSTER_SHARD_UNASSIGNED,                                                                \
   FLOWIE_CLUSTER_OWNER_TOKEN_INIT,                                                                \
   0u,                                                                                             \
   TURBO_OK}

/**
 * Claim one shard before starting a dedicated renew/reconnect worker. The
 * worker owns all subsequent libpq access; readers receive only copied state.
 */
int flowie_cluster_pgsql_lease_worker_create(const flowie_cluster_pgsql_config_t *config,
                                             uint32_t shard_id,
                                             flowie_cluster_pgsql_lease_worker_t **out);
int flowie_cluster_pgsql_lease_worker_snapshot(flowie_cluster_pgsql_lease_worker_t *worker,
                                               flowie_cluster_pgsql_lease_snapshot_t *out);
int flowie_cluster_pgsql_lease_worker_activate(flowie_cluster_pgsql_lease_worker_t *worker,
                                               const flowie_cluster_owner_token_t *recovered_owner);
int flowie_cluster_pgsql_lease_worker_close(flowie_cluster_pgsql_lease_worker_t *worker);
void flowie_cluster_pgsql_lease_worker_destroy(flowie_cluster_pgsql_lease_worker_t *worker);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_PGSQL_INTERNAL_H */
