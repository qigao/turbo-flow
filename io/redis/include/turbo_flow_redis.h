#ifndef TURBO_FLOW_REDIS_H
#define TURBO_FLOW_REDIS_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_state_store.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_REDIS_MAX_BLOCK_MS 60000u
#define TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_API_VERSION 1u
#define TURBO_FLOW_REDIS_STREAM_MAX_ACTIVE_CLAIMS 65536u

typedef struct turbo_flow_redis_stream_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  const char *stream;
  const char *field;
  size_t maxlen;
  /** Non-zero registers an XREADGROUP source; zero registers an XADD sink. */
  uint32_t poll_interval_ms;
  const char *group;
  const char *consumer;
  const char *group_start_id;
  size_t read_count;
  uint32_t block_ms;
  int create_group;
} turbo_flow_redis_stream_config_t;

typedef struct turbo_flow_redis_stream_owner_s turbo_flow_redis_stream_owner_t;
typedef struct turbo_flow_redis_stream_publisher_s turbo_flow_redis_stream_publisher_t;

#define TURBO_FLOW_REDIS_STREAM_PUBLISHER_API_VERSION 1u

typedef struct turbo_flow_redis_stream_publisher_config_s {
  size_t size;
  uint32_t version;
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  const char *stream;
  const char *field;
  size_t maxlen;
  size_t max_payload_size;
} turbo_flow_redis_stream_publisher_config_t;

#define TURBO_FLOW_REDIS_STREAM_PUBLISHER_CONFIG_INIT                                              \
  {sizeof(turbo_flow_redis_stream_publisher_config_t),                                             \
   TURBO_FLOW_REDIS_STREAM_PUBLISHER_API_VERSION,                                                  \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0,                                                                                              \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u}

/** Create one mutex-serialized, binary-safe XADD publisher with explicit hard bounds. */
CXX_C_API int
turbo_flow_redis_stream_publisher_create(const turbo_flow_redis_stream_publisher_config_t *config,
                                         turbo_flow_redis_stream_publisher_t **out);
CXX_C_API void
turbo_flow_redis_stream_publisher_destroy(turbo_flow_redis_stream_publisher_t *publisher);
/** OK means Redis returned the new stream ID; the ID remains transport metadata and is discarded.
 */
CXX_C_API int
turbo_flow_redis_stream_publisher_append(turbo_flow_redis_stream_publisher_t *publisher,
                                         const void *payload, size_t payload_size);

/** Type-erased append port for embedding the publisher in a transport-neutral dispatcher. */
CXX_C_API int turbo_flow_redis_stream_publisher_publish(void *publisher, const void *payload,
                                                        size_t payload_size);

typedef struct turbo_flow_redis_stream_claim_s {
  size_t size;
  uint64_t token;
  /** Borrowed Redis entry ID, valid until this token is acked/requeued or owner is destroyed. */
  const char *entry_id;
  /** Borrowed binary payload with the same lifetime as entry_id. */
  tstr_v payload;
} turbo_flow_redis_stream_claim_t;

#define TURBO_FLOW_REDIS_STREAM_CLAIM_INIT                                                         \
  {sizeof(turbo_flow_redis_stream_claim_t), 0u, NULL, {NULL, 0u}}

typedef struct turbo_flow_redis_stream_claim_owner_config_s {
  size_t size;
  uint32_t version;
  size_t max_active_claims;
} turbo_flow_redis_stream_claim_owner_config_t;

#define TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_CONFIG_INIT                                            \
  {sizeof(turbo_flow_redis_stream_claim_owner_config_t),                                           \
   TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_API_VERSION, 1u}

/**
 * Register a Redis Streams input or output adapter backed by TurboNet::Redis.
 * XREADGROUP source entries are acknowledged only after successful publication.
 */
CXX_C_API int
turbo_flow_redis_register_stream_adapter(turbo_flow_t *flow, const char *name,
                                         const turbo_flow_redis_stream_config_t *config);

/**
 * Create a host-serialized Redis Stream owner for asynchronous delayed replies.
 *
 * `stream`, `group`, and stable `consumer` are required. If `create_group` is set, BUSYGROUP is
 * idempotent. The owner first replays this consumer's PEL before reading new `>` entries.
 */
CXX_C_API int turbo_flow_redis_stream_owner_create(const turbo_flow_redis_stream_config_t *config,
                                                   turbo_flow_redis_stream_owner_t **out);

/** Additive bounded multi-claim constructor; the legacy constructor keeps a bound of one. */
CXX_C_API int turbo_flow_redis_stream_owner_create_ex(
    const turbo_flow_redis_stream_config_t *config,
    const turbo_flow_redis_stream_claim_owner_config_t *claim_config,
    turbo_flow_redis_stream_owner_t **out);

/** Destroy the owner. An active unacked entry remains in the Redis PEL. */
CXX_C_API void turbo_flow_redis_stream_owner_destroy(turbo_flow_redis_stream_owner_t *owner);

/**
 * Claim one entry with XREADGROUP. TURBO_ENOENT means no entry was available during block_ms;
 * TURBO_EBUSY means the configured active-claim bound has been reached.
 */
CXX_C_API int turbo_flow_redis_stream_owner_claim(turbo_flow_redis_stream_owner_t *owner,
                                                  turbo_flow_redis_stream_claim_t *claim);

/**
 * XACK the matching active entry.
 *
 * An uncertain transport result keeps the claim active. A later retry checks the exact ID in the
 * group PEL: absence completes the prior XACK, same-consumer ownership retries it, and ownership
 * by another consumer returns TURBO_EBUSY without acknowledging that consumer's claim.
 */
CXX_C_API int turbo_flow_redis_stream_owner_ack(turbo_flow_redis_stream_owner_t *owner,
                                                uint64_t token);

/** Leave the entry in the PEL and make it the next pending replay candidate. */
CXX_C_API int turbo_flow_redis_stream_owner_requeue(turbo_flow_redis_stream_owner_t *owner,
                                                    uint64_t token);

/** Terminally XACK a failed entry without classifying it as worker completion. */
CXX_C_API int turbo_flow_redis_stream_owner_drop(turbo_flow_redis_stream_owner_t *owner,
                                                 uint64_t token);

/** Export a borrowed thin settler binding for a credit/delayed-reply coordinator. */
CXX_C_API int turbo_flow_redis_stream_owner_settler(turbo_flow_redis_stream_owner_t *owner,
                                                    turbo_flow_claim_settler_t *out);

#define TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE (16u * 1024u * 1024u)
#define TURBO_FLOW_REDIS_MAX_KEY_SIZE 1024u
#define TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE 65538u
#define TURBO_FLOW_REDIS_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE 4096u

#define TURBO_FLOW_REDIS_CONNECTION_CONFIG_VERSION 1u

typedef enum turbo_flow_redis_deployment_e {
  TURBO_FLOW_REDIS_DEPLOYMENT_STANDALONE = 0,
  TURBO_FLOW_REDIS_DEPLOYMENT_CLUSTER = 1,
  TURBO_FLOW_REDIS_DEPLOYMENT_SENTINEL = 2
} turbo_flow_redis_deployment_t;

/** Optional deployment-aware connection settings. Zero-initialized means legacy standalone. */
typedef struct turbo_flow_redis_connection_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_redis_deployment_t deployment;
  const char *host;
  uint16_t port;
  const char **seed_hosts;
  uint16_t *seed_ports;
  size_t seed_count;
  const char *service_name;
  const char *sentinel_username;
  const char *sentinel_password;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  size_t connections_per_node;
  uint32_t topology_refresh_ms;
  int max_redirections;
} turbo_flow_redis_connection_config_t;

#define TURBO_FLOW_REDIS_CONNECTION_CONFIG_INIT                                                    \
  {sizeof(turbo_flow_redis_connection_config_t),                                                   \
   TURBO_FLOW_REDIS_CONNECTION_CONFIG_VERSION,                                                     \
   TURBO_FLOW_REDIS_DEPLOYMENT_STANDALONE,                                                         \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

typedef enum turbo_flow_redis_data_operation_e {
  /** Store the input payload at the configured key using binary-safe SET. */
  TURBO_FLOW_REDIS_DATA_SET = 1,
  /** Replace the input payload with the configured key's binary-safe GET value. */
  TURBO_FLOW_REDIS_DATA_GET
} turbo_flow_redis_data_operation_t;

typedef struct turbo_flow_redis_data_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  const char *key;
  turbo_flow_redis_data_operation_t operation;
  /** Hard bound for SET input and GET output; zero uses the documented default. */
  size_t max_value_size;
} turbo_flow_redis_data_config_t;

/** Fixed-key atomic blob store for durable owner snapshots. */
typedef struct turbo_flow_redis_blob_store_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  const char *key;
  /** Zero uses TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE. */
  size_t max_value_size;
} turbo_flow_redis_blob_store_config_t;

typedef struct turbo_flow_redis_record_store_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  /** Required Redis hash key isolating one logical record namespace. */
  const char *key;
  /** Zero uses TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE. */
  size_t max_record_key_size;
  /** Zero uses TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE. */
  size_t max_value_size;
  /** Zero uses TURBO_FLOW_REDIS_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE. */
  size_t max_batch_size;
  /** Required maximum record count for bounded startup scans. */
  size_t max_records;
  turbo_flow_redis_connection_config_t connection;
} turbo_flow_redis_record_store_config_t;

/** Create a mutex-serialized Redis state store for rebuildable projections. */
CXX_C_API int turbo_flow_redis_state_store_create(
    const turbo_flow_redis_record_store_config_t *config,
    const turbo_flow_store_limits_t *limits, turbo_flow_state_store_t **out);

/**
 * Create a binary-safe Redis SET/GET store view.
 *
 * The returned view owns an internal TurboNet::Redis client. The caller must
 * keep it alive while an owner borrows it and later call
 * turbo_flow_redis_blob_store_destroy().
 */
CXX_C_API int turbo_flow_redis_blob_store_create(const turbo_flow_redis_blob_store_config_t *config,
                                                 turbo_flow_blob_store_t *out);

/**
 * Create a Redis blob store from a `kind: blob_store`, `backend: redis`
 * channel in an immutable YAML projection. The configured key is copied into
 * `key`; callers pass that key when binding the store to an owner.
 */
CXX_C_API int turbo_flow_redis_blob_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_blob_store_t *out, char *key, size_t key_capacity, turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_redis_blob_store_destroy(turbo_flow_blob_store_t *store);

/**
 * Register a fixed-key Redis data adapter backed by TurboNet::Redis.
 * SET is a sink. GET is a transform and returns TURBO_ENOENT for a missing key
 * without changing the input message. Values are binary-safe and bounded.
 */
CXX_C_API int turbo_flow_redis_register_data_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_redis_data_config_t *config);

/**
 * Register one Redis adapter from an immutable resolved YAML snapshot.
 *
 * `pattern: stream` requires an explicit `role: source|sink`; `pattern: data`
 * requires an explicit `operation: set|get`. Pattern-specific fields are
 * rejected outside their owning contract.
 */
CXX_C_API int
turbo_flow_redis_register_resolved_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_resolved_config_t *resolved,
                                           turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_REDIS_H */
